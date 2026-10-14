"""SQL authority for immutable official-Recipe control bindings."""

from __future__ import annotations

import hmac
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    ApiIdempotencyRecord,
    JudgeExecutionProfile,
    PipelineRunControlBinding,
    ProviderConnection,
    RecipeProviderBinding,
)
from loom.pipeline.control_bindings import (
    JudgeExecutionProfileApplyV1,
    JudgeExecutionProfileV1,
    RecipeProviderBindingApply,
    RecipeProviderBindingApplyV1,
    RecipeProviderBindingSnapshot,
    RecipeProviderBindingV1,
    TerminalGenProviderBindingApplyV2,
    TerminalGenProviderBindingV2,
    control_snapshot_digest,
    snapshot_bytes,
    validate_registered_judge_adapter,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.public_api import (
    ResolvedRecipeControlBindingsV1,
    ResolvedRecipeControlBindingV1,
)
from loom.pipeline.spec import ProviderAttemptLimitsV1, RecipeIdentityV1
from loom_service.pipeline_api_service import PipelineApiError

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_PROFILE_NAME_RE = re.compile(r"^(?:[a-z][a-z0-9_]{0,62}|behavior-judge-codex-gpt-5\.6-sol-v1)$")
_TERMINALGEN_CONTROL_SLOTS = tuple(f"generate_card_{ordinal:02d}" for ordinal in range(18))


def _profile_snapshot(value: dict[str, Any]) -> JudgeExecutionProfileV1:
    return JudgeExecutionProfileV1.model_validate_json(json.dumps(value))


def _provider_snapshot(value: dict[str, Any]) -> RecipeProviderBindingSnapshot:
    if value.get("schema_version") == "loom.recipe-provider-binding.v2":
        return TerminalGenProviderBindingV2.model_validate_json(json.dumps(value))
    return RecipeProviderBindingV1.model_validate_json(json.dumps(value))


def _provider_matches(connection: ProviderConnection, *, provider: str, model: str) -> bool:
    expected_type = {
        "openai": "openai-compatible",
        "anthropic": "anthropic",
    }.get(provider)
    return bool(
        expected_type == connection.provider_type
        and connection.status == "valid"
        and connection.deleted_at is None
        and (connection.allowed_models is None or model in connection.allowed_models)
    )


async def _global_idempotency(
    session: AsyncSession,
    *,
    endpoint: Literal["judge_profile_apply", "provider_binding_apply"],
    key: str,
    digest: str,
) -> tuple[ApiIdempotencyRecord, bool]:
    now = datetime.now(UTC)
    inserted_id = (
        await session.execute(
            text("""
                INSERT INTO api_idempotency_records (
                    team_id, endpoint, idempotency_key, request_digest, expires_at
                ) VALUES (NULL, :endpoint, :key, :digest, :expires_at)
                ON CONFLICT (endpoint, idempotency_key)
                    WHERE team_id IS NULL DO NOTHING
                RETURNING id
            """),
            {
                "endpoint": endpoint,
                "key": key,
                "digest": digest,
                "expires_at": now + timedelta(days=3650),
            },
        )
    ).scalar_one_or_none()
    record = (
        await session.execute(
            select(ApiIdempotencyRecord)
            .where(
                ApiIdempotencyRecord.team_id.is_(None),
                ApiIdempotencyRecord.endpoint == endpoint,
                ApiIdempotencyRecord.idempotency_key == key,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if record is None:
        raise RuntimeError("global idempotency insert was not readable in its transaction")
    if inserted_id is None:
        if not hmac.compare_digest(record.request_digest, digest):
            raise PipelineApiError(409, "idempotency_conflict", "Idempotency key body differs")
        if record.state != "completed":
            raise PipelineApiError(409, "idempotency_in_progress", "Identical apply is in progress")
        return record, True
    return record, False


def _complete(record: ApiIdempotencyRecord, *, resource_id: UUID, response: dict[str, Any]) -> None:
    record.state = "completed"
    record.resource_type = "pipeline_control_binding"
    record.resource_id = resource_id
    record.response_status = 200
    record.response_json = response
    record.completed_at = datetime.now(UTC)


def _redacted_profile(snapshot: JudgeExecutionProfileV1) -> dict[str, Any]:
    return {
        "schema_version": snapshot.schema_version,
        "profile_id": str(snapshot.profile_id),
        "profile_name": snapshot.profile_name,
        "version": snapshot.version,
        "status": snapshot.status,
        "recipe_name": snapshot.recipe_name,
        "recipe_version": snapshot.recipe_version,
        "recipe_digest": snapshot.recipe_digest,
        "node_key": snapshot.node_key,
        "environment": snapshot.environment,
        "agent_name": snapshot.agent_name,
        "agent_version": snapshot.agent_version,
        "agent_adapter": snapshot.agent_adapter,
        "agent_adapter_digest": snapshot.agent_adapter_digest,
        "provider": snapshot.provider,
        "model": snapshot.model,
        "wire_api": snapshot.wire_api,
        "runner_lock_sha256": snapshot.runner_lock_sha256,
        "provider_asset_manifest_sha256": snapshot.provider_asset_manifest_sha256,
        "provider_asset_locks": [
            item.model_dump(mode="json") for item in snapshot.provider_asset_locks
        ],
        "mcp_server_locks": [item.model_dump(mode="json") for item in snapshot.mcp_server_locks],
        "provider_request_limit_per_attempt": snapshot.provider_request_limit_per_attempt,
        "provider_cost_limit_microusd_per_attempt": snapshot.provider_cost_limit_microusd_per_attempt,
        "per_call_timeout_seconds": snapshot.per_call_timeout_seconds,
        "allowed_team_ids": [str(item) for item in snapshot.allowed_team_ids],
        "created_by": str(snapshot.created_by),
        "created_at": snapshot.created_at.isoformat(),
        "updated_by": str(snapshot.updated_by),
        "updated_at": snapshot.updated_at.isoformat(),
        "snapshot_sha256": control_snapshot_digest(snapshot),
    }


def _admin_profile(snapshot: JudgeExecutionProfileV1) -> dict[str, Any]:
    value = snapshot.model_dump(mode="json")
    value["snapshot_sha256"] = control_snapshot_digest(snapshot)
    return value


def _admin_binding(snapshot: RecipeProviderBindingSnapshot) -> dict[str, Any]:
    value = snapshot.model_dump(mode="json")
    value["snapshot_sha256"] = control_snapshot_digest(snapshot)
    return value


async def apply_judge_profile(
    session: AsyncSession,
    *,
    actor_id: UUID,
    recipe_name: str,
    recipe_version: int,
    profile_name: str,
    payload: JudgeExecutionProfileApplyV1,
    idempotency_key: str,
    create_only: bool,
    expected_version: int | None,
) -> tuple[dict[str, Any], bool]:
    if (recipe_name, recipe_version) != ("behavior-recovery", 1):
        raise PipelineApiError(422, "judge_profile_incompatible", "Unsupported Recipe profile")
    if _PROFILE_NAME_RE.fullmatch(profile_name) is None:
        raise PipelineApiError(422, "judge_profile_incompatible", "Invalid profile name")
    try:
        validate_registered_judge_adapter(payload)
    except ValueError as exc:
        raise PipelineApiError(
            422, "judge_profile_incompatible", "Judge adapter registration is incompatible"
        ) from exc
    connection = await session.get(ProviderConnection, payload.provider_connection_id)
    if connection is None or not _provider_matches(
        connection, provider=payload.provider, model=payload.model
    ):
        raise PipelineApiError(
            422, "provider_connection_unavailable", "Provider connection is unavailable"
        )
    digest = canonical_digest(
        {
            "endpoint": "judge_profile_apply",
            "path": [recipe_name, recipe_version, profile_name],
            "request": payload.model_dump(mode="json"),
        }
    )
    replay_record, replay = await _global_idempotency(
        session, endpoint="judge_profile_apply", key=idempotency_key, digest=digest
    )
    if replay:
        return dict(replay_record.response_json or {}), True
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
        {"identity": f"judge-profile:{recipe_name}:{recipe_version}:{profile_name}"},
    )
    current = (
        await session.execute(
            select(JudgeExecutionProfile)
            .where(
                JudgeExecutionProfile.recipe_name == recipe_name,
                JudgeExecutionProfile.recipe_version == recipe_version,
                JudgeExecutionProfile.profile_name == profile_name,
                JudgeExecutionProfile.is_current.is_(True),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if create_only and current is not None:
        await session.delete(replay_record)
        await session.flush()
        raise PipelineApiError(412, "precondition_failed", "Profile already exists")
    if not create_only and (current is None or current.version != expected_version):
        await session.delete(replay_record)
        await session.flush()
        raise PipelineApiError(412, "version_drift", "If-Match-Version does not match")
    now = datetime.now(UTC)
    profile_id = current.profile_id if current is not None else uuid4()
    version = current.version + 1 if current is not None else 1
    created_by = current.created_by if current is not None else actor_id
    created_at = current.created_at if current is not None else now
    snapshot = JudgeExecutionProfileV1(
        schema_version="loom.judge-execution-profile.v1",
        profile_id=profile_id,
        profile_name=profile_name,
        version=version,
        recipe_name="behavior-recovery",
        recipe_version=1,
        node_key="offline_judge",
        created_by=created_by,
        created_at=created_at,
        updated_by=actor_id,
        updated_at=now,
        **payload.model_dump(mode="python"),
    )
    if current is not None:
        current.is_current = False
    row = JudgeExecutionProfile(
        profile_id=profile_id,
        recipe_name=recipe_name,
        recipe_version=recipe_version,
        profile_name=profile_name,
        version=version,
        status=snapshot.status,
        environment=snapshot.environment,
        provider_connection_id=snapshot.provider_connection_id,
        agent_adapter=snapshot.agent_adapter,
        recipe_digest=snapshot.recipe_digest,
        snapshot_json=snapshot.model_dump(mode="json"),
        snapshot_bytes=snapshot_bytes(snapshot),
        snapshot_sha256=control_snapshot_digest(snapshot),
        allowed_team_ids=snapshot.allowed_team_ids,
        created_by=created_by,
        created_at=created_at,
        updated_by=actor_id,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    response = _admin_profile(snapshot)
    _complete(replay_record, resource_id=profile_id, response=response)
    return response, False


async def apply_provider_binding(
    session: AsyncSession,
    *,
    actor_id: UUID,
    recipe_name: str,
    recipe_version: int,
    logical_name: str,
    payload: RecipeProviderBindingApply,
    idempotency_key: str,
    create_only: bool,
    expected_version: int | None,
) -> tuple[dict[str, Any], bool]:
    is_behavior = isinstance(payload, RecipeProviderBindingApplyV1) and (
        recipe_name,
        recipe_version,
        logical_name,
    ) == ("behavior-recovery", 1, "behavior_recovery_primitive")
    is_terminalgen = isinstance(payload, TerminalGenProviderBindingApplyV2) and (
        recipe_name == "terminalgen-authoring"
        and recipe_version == 1
        and re.fullmatch(r"generate_card_(?:0[0-9]|1[0-7])", logical_name) is not None
    )
    if not (is_behavior or is_terminalgen):
        raise PipelineApiError(422, "control_binding_incompatible", "Unsupported Recipe binding")
    connection = await session.get(ProviderConnection, payload.provider_connection_id)
    if connection is None or not _provider_matches(
        connection, provider=payload.provider, model=payload.model
    ):
        raise PipelineApiError(
            422, "provider_connection_unavailable", "Provider connection is unavailable"
        )
    pricing_ready = connection.pricing_source == "rate-card" and connection.rate_card_provider == "openai"
    if is_terminalgen and getattr(connection, "pricing_config", None) is not None:
        from loom_llm_gateway.dialect import TokenUsage
        from loom_llm_gateway.provider_pricing import configured_cost
        estimate = await configured_cost(session, connection, payload.model, TokenUsage(1, 1))
        pricing_ready = estimate.confidence == "configured" and estimate.currency == "USD"
    if is_terminalgen and (
        not pricing_ready
        or connection.responses_api_supported is not True
        or connection.responses_api_probed_at is None
        or datetime.now(UTC) - connection.responses_api_probed_at >= timedelta(hours=24)
    ):
        raise PipelineApiError(
            422,
            "provider_connection_unavailable",
            "TerminalGen requires fresh native Responses support and configured pricing",
        )
    digest = canonical_digest(
        {
            "endpoint": "provider_binding_apply",
            "path": [recipe_name, recipe_version, logical_name],
            "request": payload.model_dump(mode="json"),
        }
    )
    replay_record, replay = await _global_idempotency(
        session, endpoint="provider_binding_apply", key=idempotency_key, digest=digest
    )
    if replay:
        return dict(replay_record.response_json or {}), True
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
        {"identity": f"provider-binding:{recipe_name}:{recipe_version}:{logical_name}"},
    )
    current = (
        await session.execute(
            select(RecipeProviderBinding)
            .where(
                RecipeProviderBinding.recipe_name == recipe_name,
                RecipeProviderBinding.recipe_version == recipe_version,
                RecipeProviderBinding.logical_name == logical_name,
                RecipeProviderBinding.is_current.is_(True),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if create_only and current is not None:
        await session.delete(replay_record)
        await session.flush()
        raise PipelineApiError(412, "precondition_failed", "Provider binding already exists")
    if not create_only and (current is None or current.version != expected_version):
        await session.delete(replay_record)
        await session.flush()
        raise PipelineApiError(412, "version_drift", "If-Match-Version does not match")
    now = datetime.now(UTC)
    binding_id = current.binding_id if current is not None else uuid4()
    version = current.version + 1 if current is not None else 1
    created_by = current.created_by if current is not None else actor_id
    created_at = current.created_at if current is not None else now
    if is_terminalgen:
        assert isinstance(payload, TerminalGenProviderBindingApplyV2)
        snapshot: RecipeProviderBindingSnapshot = TerminalGenProviderBindingV2(
            schema_version="loom.recipe-provider-binding.v2",
            binding_id=binding_id,
            logical_name=logical_name,
            version=version,
            recipe_name="terminalgen-authoring",
            recipe_version=1,
            node_key=logical_name,
            created_by=created_by,
            created_at=created_at,
            updated_by=actor_id,
            updated_at=now,
            **payload.model_dump(mode="python"),
        )
    else:
        assert isinstance(payload, RecipeProviderBindingApplyV1)
        snapshot = RecipeProviderBindingV1(
            schema_version="loom.recipe-provider-binding.v1",
            binding_id=binding_id,
            logical_name="behavior_recovery_primitive",
            version=version,
            recipe_name="behavior-recovery",
            recipe_version=1,
            node_key="recovery_primitive",
            created_by=created_by,
            created_at=created_at,
            updated_by=actor_id,
            updated_at=now,
            **payload.model_dump(mode="python"),
        )
    if current is not None:
        current.is_current = False
    row = RecipeProviderBinding(
        binding_id=binding_id,
        recipe_name=recipe_name,
        recipe_version=recipe_version,
        logical_name=logical_name,
        version=version,
        status=snapshot.status,
        environment=snapshot.environment,
        provider_connection_id=snapshot.provider_connection_id,
        recipe_digest=snapshot.recipe_digest,
        snapshot_json=snapshot.model_dump(mode="json"),
        snapshot_bytes=snapshot_bytes(snapshot),
        snapshot_sha256=control_snapshot_digest(snapshot),
        allowed_team_ids=snapshot.allowed_team_ids,
        created_by=created_by,
        created_at=created_at,
        updated_by=actor_id,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    response = _admin_binding(snapshot)
    _complete(replay_record, resource_id=binding_id, response=response)
    return response, False


class SqlPipelineRecipeBindingResolver:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession] | None = None) -> None:
        self._session_factory = session_factory

    async def list(
        self, *, team_id: UUID, recipe_name: str, recipe_version: int
    ) -> list[dict[str, Any]]:
        if self._session_factory is None:
            raise RuntimeError("judge profile reader requires a session factory")
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(JudgeExecutionProfile).where(
                            JudgeExecutionProfile.recipe_name == recipe_name,
                            JudgeExecutionProfile.recipe_version == recipe_version,
                            JudgeExecutionProfile.status == "active",
                            JudgeExecutionProfile.is_current.is_(True),
                        )
                    )
                ).scalars()
            )
        result = []
        for row in rows:
            snapshot = _profile_snapshot(row.snapshot_json)
            if snapshot.allowed_team_ids and team_id not in snapshot.allowed_team_ids:
                continue
            result.append(_redacted_profile(snapshot))
        return sorted(result, key=lambda item: item["profile_name"].encode())

    async def resolve(
        self,
        team_id: UUID,
        recipe_identity: RecipeIdentityV1,
        judge_profile_id: UUID | None,
        logical_slots: tuple[str, ...],
        *,
        session: AsyncSession | None = None,
    ) -> ResolvedRecipeControlBindingsV1:
        if session is None:
            raise PipelineApiError(422, "binding_unavailable", "Transactional resolver required")
        if (recipe_identity.name, recipe_identity.version) == ("terminalgen-authoring", 1):
            if judge_profile_id is not None:
                raise PipelineApiError(
                    422,
                    "judge_profile_incompatible",
                    "TerminalGen does not accept a judge profile",
                )
            if logical_slots != _TERMINALGEN_CONTROL_SLOTS:
                raise PipelineApiError(
                    422,
                    "control_binding_missing",
                    "TerminalGen binding slots drifted",
                )
            rows = list(
                (
                    await session.execute(
                        select(RecipeProviderBinding)
                        .where(
                            RecipeProviderBinding.recipe_name == recipe_identity.name,
                            RecipeProviderBinding.recipe_version == recipe_identity.version,
                            RecipeProviderBinding.logical_name.in_(logical_slots),
                            RecipeProviderBinding.is_current.is_(True),
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            by_name = {row.logical_name: row for row in rows}
            if len(by_name) != len(logical_slots):
                raise PipelineApiError(
                    422,
                    "control_binding_missing",
                    "TerminalGen provider bindings are incomplete",
                )
            items: list[ResolvedRecipeControlBindingV1] = []
            for logical_name in logical_slots:
                row = by_name[logical_name]
                snapshot = _provider_snapshot(row.snapshot_json)
                if (
                    not isinstance(snapshot, TerminalGenProviderBindingV2)
                    or row.status != "active"
                    or snapshot.recipe_digest != recipe_identity.digest
                    or (snapshot.allowed_team_ids and team_id not in snapshot.allowed_team_ids)
                    or row.snapshot_sha256 != control_snapshot_digest(snapshot)
                ):
                    raise PipelineApiError(
                        422,
                        "control_binding_incompatible",
                        "TerminalGen provider binding is incompatible",
                    )
                items.append(
                    ResolvedRecipeControlBindingV1(
                        logical_name=logical_name,
                        kind="provider",
                        node_key=logical_name,
                        object_id=snapshot.binding_id,
                        version=snapshot.version,
                        snapshot_sha256=row.snapshot_sha256,
                        provider_limits=ProviderAttemptLimitsV1(
                            provider_request_limit_per_attempt=(
                                snapshot.provider_request_limit_per_attempt
                            ),
                            provider_cost_limit_microusd_per_attempt=(
                                snapshot.provider_cost_limit_microusd_per_attempt
                            ),
                            per_call_timeout_seconds=snapshot.per_call_timeout_seconds,
                        ),
                    )
                )
            return ResolvedRecipeControlBindingsV1(items=items)
        if judge_profile_id is None:
            raise PipelineApiError(422, "judge_profile_missing", "Judge profile is required")
        if logical_slots != ("behavior_offline_judge", "behavior_recovery_primitive"):
            raise PipelineApiError(422, "control_binding_missing", "Recipe binding slots drifted")
        profile = (
            await session.execute(
                select(JudgeExecutionProfile)
                .where(
                    JudgeExecutionProfile.profile_id == judge_profile_id,
                    JudgeExecutionProfile.is_current.is_(True),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        binding = (
            await session.execute(
                select(RecipeProviderBinding)
                .where(
                    RecipeProviderBinding.recipe_name == recipe_identity.name,
                    RecipeProviderBinding.recipe_version == recipe_identity.version,
                    RecipeProviderBinding.logical_name == "behavior_recovery_primitive",
                    RecipeProviderBinding.is_current.is_(True),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if profile is None:
            raise PipelineApiError(422, "judge_profile_missing", "Judge profile is unavailable")
        if profile.status != "active":
            raise PipelineApiError(422, "judge_profile_disabled", "Judge profile is disabled")
        if binding is None or binding.status != "active":
            raise PipelineApiError(422, "control_binding_missing", "Primitive binding unavailable")
        p = _profile_snapshot(profile.snapshot_json)
        b = _provider_snapshot(binding.snapshot_json)
        if (
            p.recipe_digest != recipe_identity.digest
            or b.recipe_digest != recipe_identity.digest
            or (p.allowed_team_ids and team_id not in p.allowed_team_ids)
            or (b.allowed_team_ids and team_id not in b.allowed_team_ids)
        ):
            raise PipelineApiError(422, "judge_profile_incompatible", "Binding is incompatible")
        items = [
            ResolvedRecipeControlBindingV1(
                logical_name="behavior_offline_judge",
                kind="judge_profile",
                node_key="offline_judge",
                object_id=p.profile_id,
                version=p.version,
                snapshot_sha256=profile.snapshot_sha256,
                provider_limits=ProviderAttemptLimitsV1(
                    provider_request_limit_per_attempt=p.provider_request_limit_per_attempt,
                    provider_cost_limit_microusd_per_attempt=p.provider_cost_limit_microusd_per_attempt,
                    per_call_timeout_seconds=p.per_call_timeout_seconds,
                ),
            ),
            ResolvedRecipeControlBindingV1(
                logical_name="behavior_recovery_primitive",
                kind="provider",
                node_key="recovery_primitive",
                object_id=b.binding_id,
                version=b.version,
                snapshot_sha256=binding.snapshot_sha256,
                provider_limits=ProviderAttemptLimitsV1(
                    provider_request_limit_per_attempt=b.provider_request_limit_per_attempt,
                    provider_cost_limit_microusd_per_attempt=b.provider_cost_limit_microusd_per_attempt,
                    per_call_timeout_seconds=b.per_call_timeout_seconds,
                ),
            ),
        ]
        return ResolvedRecipeControlBindingsV1(items=items)

    async def persist_run_bindings(
        self,
        session: AsyncSession,
        *,
        pipeline_run_id: UUID,
        items: ResolvedRecipeControlBindingsV1,
    ) -> None:
        for item in items.items:
            if item.kind == "judge_profile":
                profile_source = (
                    await session.execute(
                        select(JudgeExecutionProfile).where(
                            JudgeExecutionProfile.profile_id == item.object_id,
                            JudgeExecutionProfile.version == item.version,
                        )
                    )
                ).scalar_one()
                source_snapshot_sha256 = profile_source.snapshot_sha256
                source_snapshot_json = profile_source.snapshot_json
                source_snapshot_bytes = profile_source.snapshot_bytes
                source_provider_connection_id = profile_source.provider_connection_id
            else:
                binding_source = (
                    await session.execute(
                        select(RecipeProviderBinding).where(
                            RecipeProviderBinding.binding_id == item.object_id,
                            RecipeProviderBinding.version == item.version,
                        )
                    )
                ).scalar_one()
                source_snapshot_sha256 = binding_source.snapshot_sha256
                source_snapshot_json = binding_source.snapshot_json
                source_snapshot_bytes = binding_source.snapshot_bytes
                source_provider_connection_id = binding_source.provider_connection_id
            if source_snapshot_sha256 != item.snapshot_sha256:
                raise PipelineApiError(409, "control_binding_drift", "Source snapshot drifted")
            limits = item.provider_limits
            session.add(
                PipelineRunControlBinding(
                    pipeline_run_id=pipeline_run_id,
                    logical_name=item.logical_name,
                    kind=item.kind,
                    node_key=item.node_key,
                    source_object_id=item.object_id,
                    source_version=item.version,
                    snapshot_json=source_snapshot_json,
                    snapshot_bytes=source_snapshot_bytes,
                    snapshot_sha256=source_snapshot_sha256,
                    provider_connection_id=source_provider_connection_id,
                    provider_request_limit=limits.provider_request_limit_per_attempt,
                    provider_cost_limit_microusd=limits.provider_cost_limit_microusd_per_attempt,
                    per_call_timeout_seconds=limits.per_call_timeout_seconds,
                )
            )


async def read_current_profile(
    session: AsyncSession, *, recipe_name: str, recipe_version: int, profile_name: str
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(JudgeExecutionProfile).where(
                JudgeExecutionProfile.recipe_name == recipe_name,
                JudgeExecutionProfile.recipe_version == recipe_version,
                JudgeExecutionProfile.profile_name == profile_name,
                JudgeExecutionProfile.is_current.is_(True),
            )
        )
    ).scalar_one_or_none()
    return None if row is None else _admin_profile(_profile_snapshot(row.snapshot_json))


async def read_current_binding(
    session: AsyncSession, *, recipe_name: str, recipe_version: int, logical_name: str
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(RecipeProviderBinding).where(
                RecipeProviderBinding.recipe_name == recipe_name,
                RecipeProviderBinding.recipe_version == recipe_version,
                RecipeProviderBinding.logical_name == logical_name,
                RecipeProviderBinding.is_current.is_(True),
            )
        )
    ).scalar_one_or_none()
    return None if row is None else _admin_binding(_provider_snapshot(row.snapshot_json))


__all__ = [
    "SqlPipelineRecipeBindingResolver",
    "apply_judge_profile",
    "apply_provider_binding",
    "read_current_binding",
    "read_current_profile",
]
