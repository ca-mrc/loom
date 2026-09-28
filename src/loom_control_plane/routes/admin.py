"""Admin token endpoints."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import insert, text

from loom.agent_runtime import AgentRuntimeReleaseV1
from loom.agent_runtime_registry import AgentRuntimeConflictError, register_agent_runtime
from loom.auth import FAMILY_ORCHESTRATOR_SCOPES, AuthContext, verify_bearer_token
from loom.db.schema import (
    AdminAuditEvent,
    ExecutionCapacityPolicy,
    ServiceExecutionTarget,
    Token,
)
from loom.execution_image_admission import ImageAdmissionError, ImageAdmissionKeyring
from loom.service_execution_backend import local_execution_enabled
from loom_control_plane.execution_admission import (
    fetch_execution_admission_status,
    upsert_execution_admission_policy,
)
from loom_control_plane.execution_capacity import (
    create_execution_capacity_observation,
    fetch_execution_capacity_status,
    upsert_execution_capacity_policy,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.execution_finance import (
    create_execution_price_snapshot,
    fetch_execution_finance_status,
    record_execution_node_cost,
    settle_execution_cost_reservation,
    upsert_execution_budget_policy,
    upsert_target_price_binding,
)
from loom_control_plane.execution_resource_calibration import (
    create_execution_resource_calibration,
    fetch_execution_resource_profile_status,
    upsert_execution_resource_profile_binding,
)
from loom_control_plane.task_image_materializations import (
    TaskImageRetryConflictError,
    retry_task_image_materialization,
    task_image_materialization_payload,
)

router = APIRouter(prefix="/admin")

# Bug 3 fix: revoke prefix must be hex (the same charset `encode(.., 'hex')`
# emits) and at least 4 chars long, otherwise `%` or `` would revoke every
# token in the table.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")


















class _ExecutionAdmissionPolicyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    max_concurrent: int = Field(gt=0)
    enabled: bool = False
    reason: str | None = Field(default=None, max_length=500)


class _ExecutionPriceSnapshotPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    provider: str = Field(min_length=1, max_length=80)
    region: str = Field(min_length=1, max_length=120)
    sku: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1, max_length=120)
    source_version: str = Field(min_length=1, max_length=160)
    source_uri: str = Field(min_length=1, max_length=2048)
    effective_at: datetime
    observed_at: datetime
    base_microusd_per_hour: int = Field(default=0, ge=0)
    vcpu_microusd_per_hour: int = Field(default=0, ge=0)
    memory_gib_microusd_per_hour: int = Field(default=0, ge=0)
    ephemeral_storage_gib_microusd_per_hour: int = Field(default=0, ge=0)

    @field_validator("effective_at", "observed_at", mode="before")
    @classmethod
    def _parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value


class _ExecutionTargetPriceBindingPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    price_snapshot_id: UUID
    enabled: bool = False
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("price_snapshot_id", mode="before")
    @classmethod
    def _parse_uuid(cls, value: object) -> object:
        return UUID(value) if isinstance(value, str) else value


class _ExecutionBudgetPolicyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    daily_limit_microusd: int = Field(gt=0)
    monthly_limit_microusd: int = Field(gt=0)
    per_attempt_limit_microusd: int = Field(gt=0)
    max_estimate_duration_seconds: int = Field(gt=0, le=604_800)
    emergency_stop: bool = False
    enabled: bool = False
    reason: str | None = Field(default=None, max_length=500)


class _ExecutionNodeCostPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    price_snapshot_id: UUID
    provider_record_id: str = Field(min_length=1, max_length=240)
    node_name: str = Field(min_length=1, max_length=253)
    interval_started_at: datetime
    interval_stopped_at: datetime
    node_cpu_millis: int = Field(gt=0)
    node_memory_mib: int = Field(gt=0)
    node_ephemeral_storage_mib: int = Field(gt=0)
    provider_billed_microusd: int = Field(ge=0)
    billing_source: str = Field(min_length=1, max_length=120)
    billing_source_version: str = Field(min_length=1, max_length=160)
    observed_at: datetime

    @field_validator("price_snapshot_id", mode="before")
    @classmethod
    def _parse_uuid(cls, value: object) -> object:
        return UUID(value) if isinstance(value, str) else value

    @field_validator(
        "interval_started_at",
        "interval_stopped_at",
        "observed_at",
        mode="before",
    )
    @classmethod
    def _parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value


class _ExecutionCostSettlementPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    billing_complete_through: datetime

    @field_validator("billing_complete_through", mode="before")
    @classmethod
    def _parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value


class _ExecutionCapacityPolicyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    max_nodes: int = Field(gt=0)
    max_vcpu_millis: int = Field(gt=0)
    max_memory_mib: int = Field(gt=0)
    max_storage_mib: int = Field(gt=0)
    node_cpu_millis: int = Field(gt=0)
    node_memory_mib: int = Field(gt=0)
    node_storage_mib: int = Field(gt=0)
    max_pending_jobs: int = Field(gt=0)
    max_unschedulable_jobs: int = Field(ge=0)
    max_image_pull_backoff_jobs: int = Field(ge=0)
    max_create_per_minute: int = Field(gt=0)
    observation_max_age_seconds: int = Field(ge=10, le=900)
    reason: str | None = Field(default=None, max_length=500)


class _ExecutionCapacityObservationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1, max_length=120)
    source_version: str = Field(min_length=1, max_length=160)
    observed_at: datetime
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    provider_capacity_reason: str | None = Field(default=None, max_length=500)
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    autoscaler_reason: str | None = Field(default=None, max_length=500)
    provider_quota_nodes: int = Field(ge=0)
    provider_quota_vcpu_millis: int = Field(ge=0)
    provider_quota_memory_mib: int = Field(ge=0)
    provider_quota_storage_mib: int = Field(ge=0)
    provider_used_nodes: int = Field(ge=0)
    provider_used_vcpu_millis: int = Field(ge=0)
    provider_used_memory_mib: int = Field(ge=0)
    provider_used_storage_mib: int = Field(ge=0)
    active_nodes: int = Field(ge=0)
    node_states: dict[str, int] | None = None
    placement: dict[str, Any] | None = None
    provisioned_vcpu_millis: int = Field(ge=0)
    provisioned_memory_mib: int = Field(ge=0)
    provisioned_storage_mib: int = Field(ge=0)
    allocatable_cpu_millis: int = Field(ge=0)
    allocatable_memory_mib: int = Field(ge=0)
    allocatable_storage_mib: int = Field(ge=0)
    requested_cpu_millis: int = Field(ge=0)
    requested_memory_mib: int = Field(ge=0)
    requested_storage_mib: int = Field(ge=0)
    pending_jobs: int = Field(ge=0)
    unschedulable_jobs: int = Field(ge=0)
    image_pull_backoff_jobs: int = Field(ge=0)
    pending_reasons: dict[str, int]

    @field_validator("observed_at", mode="before")
    @classmethod
    def _parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value


class _ExecutionResourceCalibrationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    source_pool_id: str = Field(min_length=1, max_length=80)
    source_architecture: Literal["x86_64", "arm64"]
    resource_profile: str = Field(min_length=1, max_length=120)
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_version: str = Field(min_length=1, max_length=160)
    window_started_at: datetime
    window_stopped_at: datetime

    @field_validator("window_started_at", "window_stopped_at", mode="before")
    @classmethod
    def _parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value


class _ExecutionResourceProfileBindingPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    calibration_id: UUID
    enabled: bool = False
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("calibration_id", mode="before")
    @classmethod
    def _parse_uuid(cls, value: object) -> object:
        return UUID(value) if isinstance(value, str) else value


class _TaskImageServiceTokenPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    expires_in_days: int = Field(default=90, ge=1, le=365)


class _ExecutionCapacityCollectorTokenPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    # Capacity collection is a continuously scheduled control-plane identity.
    # An omitted lifetime produces a non-expiring, single-scope token so the
    # collector cannot silently stop after a calendar deadline. Operators may
    # still request a bounded token for temporary collectors.
    expires_in_days: int | None = Field(default=None, ge=1, le=365)


async def _require_admin_scope(
    request: Request,
    authorization: str | None,
    scope: str,
) -> AuthContext:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state,
                "admin_secret_verifier",
                None,
            ),
        )
    if ctx is None or scope not in ctx.scopes:
        raise HTTPException(status_code=403, detail=f"missing scope {scope}")
    return ctx


async def _require_any_scope(
    request: Request,
    authorization: str | None,
    *scopes: str,
) -> AuthContext:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(request.app.state, "admin_secret_verifier", None),
        )
    if ctx is None or not set(scopes).intersection(ctx.scopes):
        raise HTTPException(status_code=403, detail=f"missing one of scopes {sorted(scopes)}")
    return ctx


async def _require_capacity_observer(
    request: Request,
    authorization: str | None,
) -> AuthContext:
    ctx = await _require_any_scope(
        request,
        authorization,
        "admin:worker_pools",
        "execution:capacity:observe",
    )
    if "admin:worker_pools" not in ctx.scopes and (
        ctx.type != "worker"
        or ctx.team_id is not None
        or set(ctx.scopes) != {"execution:capacity:observe"}
    ):
        raise HTTPException(status_code=403, detail="invalid capacity observer identity")
    return ctx


@router.post("/worker-tokens", status_code=201)
async def issue_worker_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")

    if not local_execution_enabled():
        raise HTTPException(
            status_code=409,
            detail="worker credentials are available only for explicit local development",
        )

    raw_bytes = secrets.token_bytes(32)
    raw = "loom_w_" + raw_bytes.hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["worker:claim", "worker:report", "worker:index"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.post("/batch-runner-tokens", status_code=201)
async def issue_batch_runner_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")

    raw = "loom_br_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["submit:batch"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.post("/task-image-builder-tokens", status_code=201)
async def issue_task_image_builder_token(
    request: Request,
    payload: _TaskImageServiceTokenPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """Mint a least-privilege credential for exclusive image builders."""
    await _require_admin_scope(request, authorization, "admin:tokens")

    if not local_execution_enabled():
        raise HTTPException(
            status_code=409,
            detail="local task-image credentials require explicit local development",
        )

    raw = "loom_tib_" + secrets.token_bytes(32).hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at = datetime.now(UTC) + timedelta(days=payload.expires_in_days)

    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["task-image:build"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.post("/execution-capacity-collector-tokens", status_code=201)
async def issue_execution_capacity_collector_token(
    request: Request,
    payload: _ExecutionCapacityCollectorTokenPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """Mint a token that can only read its policy and publish observations."""
    await _require_admin_scope(request, authorization, "admin:tokens")

    raw = "loom_ecc_" + secrets.token_bytes(32).hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at = (
        datetime.now(UTC) + timedelta(days=payload.expires_in_days)
        if payload.expires_in_days is not None
        else None
    )
    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["execution:capacity:observe"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()
    return {"token": raw, "token_hash_prefix": token_hash.hex()[:8]}


@router.post("/task-image-registry-gc-tokens", status_code=201)
async def issue_task_image_registry_gc_token(
    request: Request,
    payload: _TaskImageServiceTokenPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """Mint a least-privilege credential for the registry-retention controller."""
    await _require_admin_scope(request, authorization, "admin:tokens")

    if not local_execution_enabled():
        raise HTTPException(
            status_code=409,
            detail="local task-image credentials require explicit local development",
        )

    raw = "loom_tigc_" + secrets.token_bytes(32).hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at = datetime.now(UTC) + timedelta(days=payload.expires_in_days)

    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["task-image:gc"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.put("/agents/{name}/versions/{version}")
async def register_agent_runtime_route(
    name: str, version: str, payload: AgentRuntimeReleaseV1, request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _require_admin_scope(request, authorization, "admin:tokens")
    if (name, version) != (payload.agent_name, payload.agent_version):
        raise HTTPException(status_code=400, detail="agent release identity differs from request path")
    try:
        keyring = ImageAdmissionKeyring.from_json(
            request.app.state.settings.execution_image_admission_public_keys_json,
        )
        async with request.app.state.session_factory() as session, session.begin():
            await register_agent_runtime(session, payload, keyring=keyring)
    except AgentRuntimeConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ImageAdmissionError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return payload.public_metadata() | {"agent_name": name}


@router.post("/task-image-materializations/{materialization_id}/retry")
async def retry_task_image_materialization_route(
    materialization_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Requeue an exhausted or registry-inconsistent task image."""
    await _require_admin_scope(request, authorization, "admin:tokens")
    try:
        async with request.app.state.session_factory() as session, session.begin():
            row = await retry_task_image_materialization(
                session,
                materialization_id=materialization_id,
            )
    except TaskImageRetryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return task_image_materialization_payload(row)


@router.post("/family-orchestrator-tokens", status_code=201)
async def issue_family_orchestrator_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """Issue the teamless credential for family cancellation and evolution.

    The credential cannot call the Gateway directly. It may cancel only
    family-linked trials through the Control Plane or ask the Control Plane to
    mint a JWT for an existing trial and a provider owned by or shared with
    that trial's team.
    """
    await _require_admin_scope(request, authorization, "admin:tokens")

    raw = "loom_fo_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="family_orchestrator",
                scopes=list(FAMILY_ORCHESTRATOR_SCOPES),
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.delete("/worker-tokens/{prefix}")
async def revoke_token(
    prefix: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")
    if not _HEX_PREFIX_RE.fullmatch(prefix):
        raise HTTPException(
            status_code=400,
            detail="prefix must be 4-64 hex characters",
        )
    async with request.app.state.session_factory() as session:
        await session.execute(
            text("""
                UPDATE tokens SET revoked_at = NOW()
                 WHERE encode(token_hash, 'hex') LIKE :prefix
            """),
            {"prefix": prefix + "%"},
        )
        await session.commit()
    return {"status": "revoked"}


@router.put("/execution-admission-policies/{scope_kind}/{scope_key}")
async def put_execution_admission_policy(
    scope_kind: Literal[
        "global", "environment", "region", "team", "batch", "execution_class", "pool"
    ],
    scope_key: str,
    request: Request,
    payload: _ExecutionAdmissionPolicyPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_execution_admission_policy(
                session,
                scope_kind=scope_kind,
                scope_key=scope_key,
                max_concurrent=payload.max_concurrent,
                enabled=payload.enabled,
                reason=payload.reason,
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.admission_policy.upserted",
                    target_type="execution_admission_policy",
                    target_id=f"{row.scope_kind}/{row.scope_key}",
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "max_concurrent": row.max_concurrent,
                        "enabled": row.enabled,
                        "reason": row.reason,
                        "version": row.version,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "scope_kind": row.scope_kind,
                "scope_key": row.scope_key,
                "max_concurrent": row.max_concurrent,
                "enabled": row.enabled,
                "reason": row.reason,
                "version": row.version,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/execution-admission/status")
async def get_execution_admission_status(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, list[dict[str, object]]]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        return await fetch_execution_admission_status(session)


@router.post("/execution-price-snapshots")
async def post_execution_price_snapshot(
    request: Request,
    payload: _ExecutionPriceSnapshotPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row, created = await create_execution_price_snapshot(
                session,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.price_snapshot.recorded",
                    target_type="execution_price_snapshot",
                    target_id=str(row.id),
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "created": created,
                        "provider": row.provider,
                        "region": row.region,
                        "sku": row.sku,
                        "source": row.source,
                        "source_version": row.source_version,
                        "rate_card_sha256": row.rate_card_sha256,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "created": created,
                "provider": row.provider,
                "region": row.region,
                "sku": row.sku,
                "source": row.source,
                "source_version": row.source_version,
                "effective_at": row.effective_at.isoformat(),
                "observed_at": row.observed_at.isoformat(),
                "rate_card_sha256": row.rate_card_sha256,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/execution-target-price-bindings/{target_id}")
async def put_execution_target_price_binding(
    target_id: str,
    request: Request,
    payload: _ExecutionTargetPriceBindingPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_target_price_binding(
                session,
                target_id=target_id,
                price_snapshot_id=payload.price_snapshot_id,
                enabled=payload.enabled,
                reason=payload.reason,
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.target_price_binding.upserted",
                    target_type="execution_target_price_binding",
                    target_id=row.target_id,
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "price_snapshot_id": str(row.price_snapshot_id),
                        "enabled": row.enabled,
                        "reason": row.reason,
                        "version": row.version,
                    },
                )
            )
            await session.commit()
            return {
                "target_id": row.target_id,
                "price_snapshot_id": str(row.price_snapshot_id),
                "enabled": row.enabled,
                "reason": row.reason,
                "version": row.version,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/execution-budget-policies/{scope_kind}/{scope_key}")
async def put_execution_budget_policy(
    scope_kind: Literal["pool", "target"],
    scope_key: str,
    request: Request,
    payload: _ExecutionBudgetPolicyPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_execution_budget_policy(
                session,
                scope_kind=scope_kind,
                scope_key=scope_key,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.budget_policy.upserted",
                    target_type="execution_budget_policy",
                    target_id=f"{row.scope_kind}/{row.scope_key}",
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "daily_limit_microusd": row.daily_limit_microusd,
                        "monthly_limit_microusd": row.monthly_limit_microusd,
                        "per_attempt_limit_microusd": row.per_attempt_limit_microusd,
                        "max_estimate_duration_seconds": (row.max_estimate_duration_seconds),
                        "emergency_stop": row.emergency_stop,
                        "enabled": row.enabled,
                        "version": row.version,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "scope_kind": row.scope_kind,
                "scope_key": row.scope_key,
                "daily_limit_microusd": row.daily_limit_microusd,
                "monthly_limit_microusd": row.monthly_limit_microusd,
                "per_attempt_limit_microusd": row.per_attempt_limit_microusd,
                "max_estimate_duration_seconds": row.max_estimate_duration_seconds,
                "emergency_stop": row.emergency_stop,
                "enabled": row.enabled,
                "reason": row.reason,
                "version": row.version,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/execution-node-cost-records")
async def post_execution_node_cost_record(
    request: Request,
    payload: _ExecutionNodeCostPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row, created = await record_execution_node_cost(
                session,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.node_cost.recorded",
                    target_type="execution_node_cost_record",
                    target_id=str(row.id),
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "created": created,
                        "provider": row.provider,
                        "provider_record_id": row.provider_record_id,
                        "target_id": row.target_id,
                        "provider_billed_microusd": row.provider_billed_microusd,
                        "allocated_microusd": row.allocated_microusd,
                        "idle_system_fragmentation_microusd": (
                            row.idle_system_fragmentation_microusd
                        ),
                        "evidence_sha256": row.evidence_sha256,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "created": created,
                "provider": row.provider,
                "provider_record_id": row.provider_record_id,
                "target_id": row.target_id,
                "node_identity_sha256": row.node_identity_sha256,
                "provider_billed_microusd": row.provider_billed_microusd,
                "allocated_microusd": row.allocated_microusd,
                "idle_system_fragmentation_microusd": (row.idle_system_fragmentation_microusd),
                "evidence_sha256": row.evidence_sha256,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/execution-cost-reservations/{reservation_id}/settle")
async def post_execution_cost_settlement(
    reservation_id: UUID,
    request: Request,
    payload: _ExecutionCostSettlementPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await settle_execution_cost_reservation(
                session,
                reservation_id=reservation_id,
                billing_complete_through=payload.billing_complete_through,
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.cost_reservation.settled",
                    target_type="execution_cost_reservation",
                    target_id=str(row.id),
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "lease_id": str(row.lease_id),
                        "price_snapshot_id": str(row.price_snapshot_id),
                        "estimated_cost_microusd": row.estimated_cost_microusd,
                        "actual_allocated_microusd": row.actual_allocated_microusd,
                        "billing_complete_through": (
                            row.billing_complete_through.isoformat()
                            if row.billing_complete_through
                            else None
                        ),
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "state": row.state,
                "estimated_cost_microusd": row.estimated_cost_microusd,
                "actual_allocated_microusd": row.actual_allocated_microusd,
                "billing_complete_through": (
                    row.billing_complete_through.isoformat()
                    if row.billing_complete_through
                    else None
                ),
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/execution-finance/status")
async def get_execution_finance_status(
    request: Request,
    pool_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        return await fetch_execution_finance_status(session, pool_id=pool_id)


@router.put("/execution-capacity-policies/{target_id}")
async def put_execution_capacity_policy(
    target_id: str,
    request: Request,
    payload: _ExecutionCapacityPolicyPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_execution_capacity_policy(
                session,
                target_id=target_id,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.capacity_policy.upserted",
                    target_type="execution_capacity_policy",
                    target_id=row.target_id,
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "enabled": row.enabled,
                        "max_nodes": row.max_nodes,
                        "max_vcpu_millis": row.max_vcpu_millis,
                        "max_memory_mib": row.max_memory_mib,
                        "max_storage_mib": row.max_storage_mib,
                        "max_pending_jobs": row.max_pending_jobs,
                        "max_unschedulable_jobs": row.max_unschedulable_jobs,
                        "max_image_pull_backoff_jobs": row.max_image_pull_backoff_jobs,
                        "max_create_per_minute": row.max_create_per_minute,
                        "observation_max_age_seconds": row.observation_max_age_seconds,
                        "version": row.version,
                    },
                )
            )
            await session.commit()
            return {
                "target_id": row.target_id,
                "enabled": row.enabled,
                "max_nodes": row.max_nodes,
                "max_vcpu_millis": row.max_vcpu_millis,
                "max_memory_mib": row.max_memory_mib,
                "max_storage_mib": row.max_storage_mib,
                "node_cpu_millis": row.node_cpu_millis,
                "node_memory_mib": row.node_memory_mib,
                "node_storage_mib": row.node_storage_mib,
                "max_pending_jobs": row.max_pending_jobs,
                "max_unschedulable_jobs": row.max_unschedulable_jobs,
                "max_image_pull_backoff_jobs": row.max_image_pull_backoff_jobs,
                "max_create_per_minute": row.max_create_per_minute,
                "observation_max_age_seconds": row.observation_max_age_seconds,
                "reason": row.reason,
                "version": row.version,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/execution-capacity-observations")
async def post_execution_capacity_observation(
    request: Request,
    payload: _ExecutionCapacityObservationPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_capacity_observer(request, authorization)
    try:
        async with request.app.state.session_factory() as session:
            row, created = await create_execution_capacity_observation(
                session,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.capacity_observation.recorded",
                    target_type="execution_capacity_observation",
                    target_id=str(row.id),
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "created": created,
                        "target_id": row.target_id,
                        "source": row.source,
                        "source_version": row.source_version,
                        "provider_capacity_state": row.provider_capacity_state,
                        "autoscaler_state": row.autoscaler_state,
                        "observation_sha256": row.observation_sha256,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "created": created,
                "target_id": row.target_id,
                "source": row.source,
                "source_version": row.source_version,
                "observed_at": row.observed_at.isoformat(),
                "provider_capacity_state": row.provider_capacity_state,
                "autoscaler_state": row.autoscaler_state,
                "observation_sha256": row.observation_sha256,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/execution-capacity-collector-policy/{target_id}")
async def get_execution_capacity_collector_policy(
    target_id: str,
    request: Request,
    pool_id: str,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_capacity_observer(request, authorization)
    async with request.app.state.session_factory() as session:
        target = await session.get(ServiceExecutionTarget, target_id)
        if target is None or target.provider != "nebius" or target.logical_pool_id != pool_id:
            raise HTTPException(status_code=404, detail="capacity policy target is unavailable")
        group = await resolve_capacity_targets(session, target_id)
        if group.owner.id != target_id:
            raise HTTPException(status_code=409, detail="capacity collector requires the capacity owner target")
        policy = await session.get(ExecutionCapacityPolicy, target_id)
    if policy is None:
        raise HTTPException(status_code=409, detail="capacity policy is unavailable")
    return {
        "target_id": target.id,
        **({"target_scope": group.scope.model_dump(mode="json")} if group.scope is not None else {}),
        "pool_id": target.logical_pool_id,
        "enabled": policy.enabled,
        "max_nodes": policy.max_nodes,
        "node_cpu_millis": policy.node_cpu_millis,
        "node_memory_mib": policy.node_memory_mib,
        "node_storage_mib": policy.node_storage_mib,
        "version": policy.version,
    }


@router.get("/execution-capacity/status")
async def get_execution_capacity_status(
    request: Request,
    pool_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        return await fetch_execution_capacity_status(session, pool_id=pool_id)


@router.post("/execution-resource-calibrations")
async def post_execution_resource_calibration(
    request: Request,
    payload: _ExecutionResourceCalibrationPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row, created = await create_execution_resource_calibration(
                session,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.resource_calibration.recorded",
                    target_type="execution_resource_calibration",
                    target_id=str(row.id),
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "created": created,
                        "target_id": row.target_id,
                        "source_pool_id": row.source_pool_id,
                        "resource_profile": row.resource_profile,
                        "source_version": row.source_version,
                        "eligible": row.eligible,
                        "trial_attempts": row.trial_attempts,
                        "peak_batch_concurrency": row.peak_batch_concurrency,
                        "evidence_sha256": row.evidence_sha256,
                    },
                )
            )
            await session.commit()
            return {
                "id": str(row.id),
                "created": created,
                "target_id": row.target_id,
                "source_pool_id": row.source_pool_id,
                "source_architecture": row.source_architecture,
                "resource_profile": row.resource_profile,
                "candidate_sha": row.candidate_sha,
                "source_version": row.source_version,
                "trial_attempts": row.trial_attempts,
                "distinct_tasks": row.distinct_tasks,
                "usage_records": row.usage_records,
                "incomplete_attempts": row.incomplete_attempts,
                "evidence_duration_seconds": row.evidence_duration_seconds,
                "peak_batch_concurrency": row.peak_batch_concurrency,
                "throttled_attempts": row.throttled_attempts,
                "oom_attempts": row.oom_attempts,
                "memory_limit_attempts": row.memory_limit_attempts,
                "eligible": row.eligible,
                "blockers": row.blockers_json,
                "percentiles": row.percentiles_json,
                "recommendation": {
                    "cpu_millis": row.recommended_cpu_millis,
                    "memory_mib": row.recommended_memory_mib,
                    "ephemeral_storage_mib": row.recommended_ephemeral_storage_mib,
                    "pids": row.recommended_pids,
                },
                "evidence_sha256": row.evidence_sha256,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/execution-resource-profile-bindings/{target_id}")
async def put_execution_resource_profile_binding(
    target_id: str,
    request: Request,
    payload: _ExecutionResourceProfileBindingPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    auth = await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_execution_resource_profile_binding(
                session,
                target_id=target_id,
                **payload.model_dump(),
            )
            session.add(
                AdminAuditEvent(
                    actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
                    action="execution.resource_profile_binding.upserted",
                    target_type="execution_resource_profile_binding",
                    target_id=row.target_id,
                    request_id=request.headers.get("x-request-id"),
                    event_metadata={
                        "calibration_id": str(row.calibration_id),
                        "enabled": row.enabled,
                        "reason": row.reason,
                        "version": row.version,
                    },
                )
            )
            await session.commit()
            return {
                "target_id": row.target_id,
                "calibration_id": str(row.calibration_id),
                "enabled": row.enabled,
                "reason": row.reason,
                "version": row.version,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/execution-resource-profile/status")
async def get_execution_resource_profile_status(
    request: Request,
    pool_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        return await fetch_execution_resource_profile_status(session, pool_id=pool_id)
