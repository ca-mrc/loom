"""Trials read routes + write forwarders (spec §5.1).

Read surface:
- GET /api/v1/trials             — list with cursor pagination + filters
- GET /api/v1/trials/{id}        — detail + service download URLs
- GET /api/v1/trials/{id}/artifacts/download — authenticated artifact proxy

Write forwarders (Task 8):
- POST /api/v1/trials            — proxies to Control Plane /trials
- POST /api/v1/trials/{id}/cancel — proxies to Control Plane /trials/{id}/cancel

Field extraction notes: reward is extracted from `Trial.result` (the
JSONB the worker writes at finalize). `batch_id` is projected when set.
LLM usage is aggregated from `llm_calls` so stale rate-card snapshots do
not leak into trial read responses. Agent name + model are pulled from
current top-level `Trial.config["agent_name"]` / `Trial.config["agent_model"]`,
with legacy `Trial.config["agent"]` fallback for older rows.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    ExecutionCostReservation,
    LlmCall,
    ServiceExecutionLease,
    Task,
    Team,
    Trial,
    TrialResourceUsage,
    User,
)
from loom.execution_diagnosis_store import execution_failure_groups, read_trial_execution_failure
from loom.execution_resource_allocation import resource_allocation_summary
from loom.execution_selection import execution_selection_readback
from loom.model_switch_store import load_model_switch_plan, plan_snapshot_from_row
from loom.models.types import ModelSpec
from loom.resource_usage_store import resource_usage_response
from loom.service_execution_backend import NEBIUS_BACKEND, local_execution_enabled
from loom.service_execution_phases import PhaseCost, PhaseLease, execution_phases
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
)
from loom_service import wire_responses as wire
from loom_service.agent_catalog import (
    known_names,
    native_selections_error,
    selection_agents,
    validate_agent_model_compat,
)
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)
from loom_service.debug_evidence import build_trial_debug_evidence
from loom_service.delivery_export import ArchiveBuildResult, build_canonical_trial_bundle_archive
from loom_service.delivery_export_errors import DeliveryExportError
from loom_service.dependencies import SessionAndCtx
from loom_service.diagnosis import build_trial_diagnosis
from loom_service.effective_combination import ProviderRouteError, resolve_provider_route
from loom_service.forwarders import forward, propagate
from loom_service.monitor_filters import (
    apply_trial_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.multi_model import usage_by_role
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.pool_submission import prepare_trial_submission
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.public_links import public_url_for
from loom_service.routes.object_downloads import stream_object_response
from loom_service.service_execution_status import service_execution_lifecycle_stage
from loom_service.stale_running_debug import trial_stale_running_debug_context
from loom_service.submission_compat import validate_submission_agent_task_compatibility
from loom_service.task_image_preparation import task_image_preparation_for_trial
from loom_service.trial_bundles import canonical_bundle_for_trial
from loom_service.trial_execution_provenance import (
    trial_execution_provenance,
    validated_runtime_plan,
)
from loom_service.trial_progress import load_trial_progress
from loom_service.trial_timing import trial_started_at
from loom_service.usage_accounting import (
    cost_meta_filter as _cost_meta_filter,
)
from loom_service.usage_accounting import (
    empty_usage_projection,
    price_snapshots_for_trials,
    project_trial_llm_evidence,
    summarize_usage_counts,
    usage_status_filter,
)

router = APIRouter()


def _extract_reward(result: dict[str, Any] | None) -> float | None:
    """Pull aggregate reward out of the worker-written result JSONB.
    The worker stores the multi-step combined reward as
    `result["aggregate_reward"]` (Plan 3 contract); fall through to
    `result["reward"]` for single-step trials that predate that key.

    Defensive cast — `result` is JSONB whose shape is enforced by the
    worker; a malformed value should not crash the read API."""
    if not result:
        return None
    val = result.get("aggregate_reward")
    if val is None:
        val = result.get("reward")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _empty_usage_projection() -> dict[str, Any]:
    return empty_usage_projection()


def _priced_call_filter() -> Any:
    return (
        ~LlmCall.rate_card_hash.like("facade:tokens-only%")
        & ~_price_unknown_call_filter()
        & (LlmCall.rate_card_hash != "failed-upstream")
    )


def _price_unknown_call_filter() -> Any:
    return LlmCall.rate_card_hash.like("facade:rate-card:missing%") | _cost_meta_filter(
        COST_META_SOURCE_KEY, "unpriced"
    )


def _cost_source_counts(row: Any) -> dict[str, int]:
    return {
        "operator-supplied": int(row.cost_source_operator_supplied_count or 0),
        "rate-card": int(row.cost_source_rate_card_count or 0),
        "tokens-only": int(row.cost_source_tokens_only_count or 0),
        "unpriced": int(row.cost_source_unpriced_count or 0),
    }


def _cost_confidence_counts(row: Any) -> dict[str, int]:
    return {
        "configured": int(row.cost_confidence_configured_count or 0),
        "not_applicable": int(row.cost_confidence_not_applicable_count or 0),
        "unavailable": int(row.cost_confidence_unavailable_count or 0),
    }


async def _usage_by_trial_ids(
    session: Any,
    trial_ids: Sequence[UUID],
) -> dict[UUID, dict[str, Any]]:
    if not trial_ids:
        return {}
    rows = (
        await session.execute(
            select(
                LlmCall.trial_id.label("trial_id"),
                func.coalesce(
                    func.sum(LlmCall.input_tokens),
                    0,
                ).label("total_prompt_tokens"),
                func.coalesce(
                    func.sum(LlmCall.output_tokens),
                    0,
                ).label("total_completion_tokens"),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(
                    func.sum(LlmCall.cost_usd),
                    0,
                ).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(
                    _cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"),
                )
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            )
            .where(LlmCall.trial_id.in_(trial_ids))
            .group_by(LlmCall.trial_id),
        )
    ).all()
    return {
        row.trial_id: summarize_usage_counts(
            llm_calls_count=int(row.llm_calls_count or 0),
            total_prompt_tokens=int(row.total_prompt_tokens or 0),
            total_completion_tokens=int(row.total_completion_tokens or 0),
            total_cost_usd=row.total_cost_usd,
            priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(
                row.token_only_llm_calls_count or 0,
            ),
            price_unknown_llm_calls_count=int(
                row.price_unknown_llm_calls_count or 0,
            ),
            failed_upstream_llm_calls_count=int(
                row.failed_upstream_llm_calls_count or 0,
            ),
            partial_usage_llm_calls_count=int(
                row.partial_usage_llm_calls_count or 0,
            ),
            missing_usage_llm_calls_count=int(
                row.missing_usage_llm_calls_count or 0,
            ),
            cost_source_counts=_cost_source_counts(row),
            cost_confidence_counts=_cost_confidence_counts(row),
        )
        for row in rows
    }


def _extract_agent_projection(
    config: dict[str, Any] | None,
) -> tuple[str | None, dict[str, Any] | None]:
    if not isinstance(config, dict):
        return None, None

    if "agent_name" in config or "agent_model" in config:
        name = config.get("agent_name")
        model = config.get("agent_model")
        return (
            name if isinstance(name, str) and name else None,
            model if isinstance(model, dict) else None,
        )

    agent = config.get("agent")
    if not isinstance(agent, dict):
        return None, None
    name = agent.get("name")
    model = agent.get("model")
    return (
        name if isinstance(name, str) and name else None,
        model if isinstance(model, dict) else None,
    )


def _trial_row(
    t: Trial,
    *,
    usage: dict[str, Any] | None = None,
    owner_team: Team | None = None,
    submitted_by_user: User | None = None,
) -> dict[str, Any]:
    agent_name, model = _extract_agent_projection(t.config)
    started_at = trial_started_at(t.started_at, t.result)
    usage_projection = usage or _empty_usage_projection()
    llm_evidence = project_trial_llm_evidence(
        t,
        llm_calls_count=int(usage_projection["llm_calls_count"]),
    )
    out: dict[str, Any] = {
        "id": str(t.id),
        "task_id": t.task_id,
        "team_id": str(t.team_id),
        "batch_id": str(t.batch_id) if t.batch_id is not None else None,
        "state": t.state,
        "failure_reason": t.failure_reason,
        "failure_message": t.failure_message,
        "submitted_at": t.submitted_at.isoformat(),
        "started_at": started_at.isoformat() if started_at else None,
        "finished_at": (t.finished_at.isoformat() if t.finished_at else None),
        "attempt_count": t.attempt_count,
        "aggregate_reward": _extract_reward(t.result),
        "total_prompt_tokens": usage_projection["total_prompt_tokens"],
        "total_completion_tokens": usage_projection["total_completion_tokens"],
        "total_tokens": usage_projection["total_tokens"],
        "llm_calls_count": usage_projection["llm_calls_count"],
        "estimated_cost_usd": usage_projection["estimated_cost_usd"],
        "cost_currency": usage_projection["cost_currency"],
        "cost_status": usage_projection["cost_status"],
        "cost_estimate_source": usage_projection["cost_estimate_source"],
        "cost_estimate_confidence": usage_projection["cost_estimate_confidence"],
        "pricing_modes": usage_projection["pricing_modes"],
        "priced_llm_calls_count": usage_projection["priced_llm_calls_count"],
        "token_only_llm_calls_count": usage_projection["token_only_llm_calls_count"],
        "price_unknown_llm_calls_count": usage_projection["price_unknown_llm_calls_count"],
        "failed_upstream_llm_calls_count": usage_projection["failed_upstream_llm_calls_count"],
        "partial_usage_llm_calls_count": usage_projection["partial_usage_llm_calls_count"],
        "missing_usage_llm_calls_count": usage_projection["missing_usage_llm_calls_count"],
        "usage_reporting_status": usage_projection["usage_reporting_status"],
        "usage_estimate_confidence": usage_projection["usage_estimate_confidence"],
        "llm_evidence_status": llm_evidence["llm_evidence_status"],
        "no_call": llm_evidence["no_call"],
        "no_call_reason": llm_evidence["no_call_reason"],
        "no_call_message": llm_evidence["no_call_message"],
        "no_call_retryable": llm_evidence["no_call_retryable"],
        "agent_name": agent_name,
        "agent_version": t.config.get("agent_version"),
        "model": model,
        "visibility": t.visibility,
        "share_status": t.share_status,
        "source_provenance": t.source_provenance,
        "submitted_by_user": (
            {
                "id": str(submitted_by_user.id),
                "username": submitted_by_user.username,
                "team_id": str(t.team_id),
                "team_name": owner_team.name if owner_team else None,
            }
            if submitted_by_user is not None
            else None
        ),
    }
    if owner_team is not None:
        out["team_name"] = owner_team.name
        out["owner_team"] = {
            "id": str(owner_team.id),
            "name": owner_team.name,
        }
    return out


@router.get("/trials", response_model=wire.TrialList, response_model_exclude_unset=True)
async def list_trials(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query(description="Search trial ID, task ID, or owner")] = None,
    task_id: Annotated[str | None, Query()] = None,
    batch_id: Annotated[UUID | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")

    target_team = resolve_monitor_team_filter(ctx, team_id)

    stmt = select(Trial).order_by(
        Trial.submitted_at.desc(),
        Trial.id.desc(),
    )
    stmt = apply_trial_monitor_filters(
        stmt,
        target_team=target_team,
        trial_q=q,
        task_id=task_id,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_name,
        agent=agent,
        model_provider=model_provider,
        model_name=model_name,
        model=model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
    if cursor:
        try:
            c = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc
        # Composite (submitted_at, id) key: rows strictly LESS than
        # the cursor (DESC ordering).
        stmt = stmt.where(
            or_(
                Trial.submitted_at < c.submitted_at,
                and_(
                    Trial.submitted_at == c.submitted_at,
                    Trial.id < c.id,
                ),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows = list((await s.execute(stmt)).scalars().all())

    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_c = encode_cursor(
            Cursor(submitted_at=last.submitted_at, id=last.id),
        )
    else:
        next_c = None
    usage_by_trial = await _usage_by_trial_ids(s, [r.id for r in rows])
    teams_by_id: dict[UUID, Team] = {}
    users_by_id: dict[UUID, User] = {}
    if rows:
        team_rows = (
            (
                await s.execute(
                    select(Team).where(Team.id.in_({r.team_id for r in rows})),
                )
            )
            .scalars()
            .all()
        )
        teams_by_id = {team.id: team for team in team_rows}
        user_ids = {r.submitted_by_user_id for r in rows if r.submitted_by_user_id is not None}
        if user_ids:
            user_rows = (
                (
                    await s.execute(
                        select(User).where(User.id.in_(user_ids)),
                    )
                )
                .scalars()
                .all()
            )
            users_by_id = {user.id: user for user in user_rows}
    progress = await load_trial_progress(s, rows, admin=is_admin(ctx))
    items = [
        _trial_row(
            r,
            usage=usage_by_trial.get(r.id),
            owner_team=teams_by_id.get(r.team_id),
            submitted_by_user=(
                users_by_id.get(r.submitted_by_user_id)
                if r.submitted_by_user_id is not None
                else None
            ),
        )
        for r in rows
    ]
    for item, row in zip(items, rows, strict=True):
        item["progress"] = progress[row.id]
    return {"items": items, "next_cursor": next_c}


def _artifact_bucket(item: dict[str, Any], default_bucket: str) -> str:
    bucket = item.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return default_bucket
    return bucket


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _artifact_size(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        size = int(value)
    except ValueError:
        return None
    return size if size >= 0 else None


def _merge_projected_artifacts(
    indexed: list[dict[str, Any]], canonical: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    # The download API identifies files by key within the authorized Trial.
    # Canonical artifact records own size and sharing metadata; index entries
    # can add step information but cannot override canonical sharing policy.
    merged: dict[str, dict[str, Any]] = {}
    for entry in [*indexed, *canonical]:
        key = entry["key"]
        merged[key] = {**merged.get(key, {}), **entry}
    return list(merged.values())


def _projected_artifacts(
    request: Request,
    *,
    trajectory_index: dict[str, Any] | None,
    trial_id: UUID,
) -> list[dict[str, Any]]:
    if not trajectory_index:
        return []
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return []

    out: list[dict[str, Any]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or not key:
            continue
        size_int = _artifact_size(item.get("size", item.get("size_bytes")))
        entry: dict[str, Any] = {
            "key": key,
            "size": size_int,
            "download_url": str(
                public_url_for(
                    request,
                    "download_artifact",
                    trial_id=str(trial_id),
                ).include_query_params(key=key),
            ),
        }
        share_status = item.get("share_status")
        if share_status in {"pending_scan", "shared", "blocked"}:
            entry["share_status"] = share_status
        else:
            entry["share_status"] = "pending_scan"
        blocked_reason = item.get("blocked_reason")
        if isinstance(blocked_reason, str):
            entry["blocked_reason"] = blocked_reason
        elif entry["share_status"] == "blocked":
            entry["blocked_reason"] = "blocked by artifact sharing policy"
        else:
            entry["blocked_reason"] = None
        step_name = item.get("step_name")
        if isinstance(step_name, str):
            entry["step_name"] = step_name
        out.append(entry)
    return out


def _find_projected_artifact(
    trajectory_index: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    if not trajectory_index:
        return None
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return None
    for item in artifacts:
        if isinstance(item, dict) and item.get("key") == key:
            return item
    return None


def _service_execution_file_key(artifact: Artifact, file_item: dict[str, Any]) -> str | None:
    explicit_key = file_item.get("key")
    explicit_bucket = file_item.get("bucket")
    if isinstance(explicit_key, str) and explicit_key and isinstance(explicit_bucket, str):
        return explicit_key
    provenance = artifact.provenance if isinstance(artifact.provenance, dict) else {}
    lease_id = provenance.get("lease_id")
    generation = provenance.get("generation")
    relative_path = file_item.get("relative_path")
    if (
        artifact.control_producer_kind != "service_execution"
        or not isinstance(lease_id, str)
        or not lease_id
        or not isinstance(generation, int)
        or isinstance(generation, bool)
        or generation < 1
        or not isinstance(relative_path, str)
        or not relative_path
    ):
        return None
    return (
        f"service-executions/{artifact.team_id}/{lease_id}/{generation}/output/"
        f"artifacts/{artifact.id}/{relative_path}"
    )


def _projected_service_execution_artifacts(
    request: Request,
    *,
    trial_id: UUID,
    artifacts: Sequence[Artifact],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for artifact in artifacts:
        storage = artifact.storage if isinstance(artifact.storage, dict) else {}
        files = storage.get("files")
        source_evidence = storage.get("source_evidence", [])
        if not isinstance(files, list) or not isinstance(source_evidence, list):
            continue
        for file_item in (*files, *source_evidence):
            if not isinstance(file_item, dict):
                continue
            key = _service_execution_file_key(artifact, file_item)
            if key is None:
                continue
            raw_size = file_item.get("size_bytes")
            size = _artifact_size(raw_size)
            out.append(
                {
                    "key": key,
                    "size": size,
                    "sha256": file_item.get("sha256"),
                    "media_type": file_item.get("media_type") or "application/octet-stream",
                    "share_status": artifact.share_status,
                    "blocked_reason": artifact.blocked_reason,
                    "step_name": artifact.name,
                    "download_url": str(
                        public_url_for(
                            request,
                            "download_artifact",
                            trial_id=str(trial_id),
                        ).include_query_params(key=key)
                    ),
                }
            )
    return out


async def _execution_phases(
    s: AsyncSession, *, trial: Trial, attempt: int, admin: bool,
) -> dict[str, Any] | None:
    leases = (await s.execute(
        select(ServiceExecutionLease).where(
            ServiceExecutionLease.trial_id == trial.id,
            ServiceExecutionLease.attempt == attempt,
        )
    )).scalars().all()
    reservations = (await s.execute(
        select(ExecutionCostReservation).where(
            ExecutionCostReservation.trial_id == trial.id,
            ExecutionCostReservation.attempt == attempt,
        )
    )).scalars().all()
    result = trial.result if isinstance(trial.result, dict) else {}
    handoff = result.get("verifier_execution")
    phases = execution_phases(
        [
            PhaseLease(
                lease_id=str(lease.id),
                execution_role=lease.execution_role,
                observed_state=lease.observed_state,
                created_at=lease.created_at,
                pod_started_at=lease.pod_started_at,
                pod_terminated_at=lease.pod_terminated_at,
                deleted_at=lease.deleted_at,
                runtime_contract=lease.runtime_contract_json,
                verifier_retry=lease.verifier_retry,
            )
            for lease in leases
        ],
        verifier_execution=handoff if isinstance(handoff, dict) else None,
        costs={
            str(item.lease_id): PhaseCost(
                requested_cpu_millis=item.requested_cpu_millis,
                requested_memory_mib=item.requested_memory_mib,
                requested_ephemeral_storage_mib=item.requested_ephemeral_storage_mib,
                estimated_cost_microusd=item.estimated_cost_microusd,
                actual_allocated_microusd=item.actual_allocated_microusd,
                state=item.state,
            )
            for item in reservations
        },
        now=datetime.now(UTC),
    )
    if phases is not None and not admin:
        # Execution prices are operator finance data; teams see resources and time.
        for phase in phases["phases"]:
            phase["estimated_cost_microusd"] = None
            phase["allocated_cost_microusd"] = None
    return phases


@router.get("/trials/{trial_id}", response_model=wire.TrialDetail, response_model_exclude_unset=True)
async def get_trial(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    usage_by_trial = await _usage_by_trial_ids(s, [trial.id])
    owner_team = (
        await s.execute(
            select(Team).where(Team.id == trial.team_id),
        )
    ).scalar_one_or_none()
    submitted_by_user = None
    if trial.submitted_by_user_id is not None:
        submitted_by_user = (
            await s.execute(
                select(User).where(User.id == trial.submitted_by_user_id),
            )
        ).scalar_one_or_none()
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    base = _trial_row(
        trial,
        usage=usage_by_trial.get(trial.id),
        owner_team=owner_team,
        submitted_by_user=submitted_by_user,
    )
    base["progress"] = (await load_trial_progress(s, [trial], admin=is_admin(ctx)))[trial.id]
    base["result"] = trial.result
    base["task_environment_preparation"] = await task_image_preparation_for_trial(s, trial)
    base["price_snapshots"] = await price_snapshots_for_trials(s, [trial.id])
    trajectory_index = trial.trajectory_index or {}
    materialization = (
        await s.execute(
            select(ServiceExecutionLease)
            .where(
                ServiceExecutionLease.trial_id == trial.id,
                ServiceExecutionLease.execution_role == "attempt",
            )
            .order_by(ServiceExecutionLease.attempt.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    runtime_plan = validated_runtime_plan(materialization)
    task_environment = (
        task.config.get("environment", {})
        if task is not None and isinstance(task.config, dict)
        else {}
    )
    task_default_network_policy = (
        task_environment.get("baseline_network_policy")
        if isinstance(task_environment, dict)
        else None
    )
    requested_network_policy = (
        trial.config.get("baseline_network_policy_override")
        if isinstance(trial.config, dict)
        else None
    )
    trial_result = trial.result if isinstance(trial.result, dict) else {}
    raw_runtime_result = trial_result.get("runtime_result")
    runtime_result = raw_runtime_result if isinstance(raw_runtime_result, dict) else {}
    canonical_bundle = None
    canonical_bundle_error: DeliveryExportError | None = None
    try:
        canonical_bundle = await canonical_bundle_for_trial(s, trial=trial)
    except DeliveryExportError as exc:
        canonical_bundle_error = exc
    source_bundle = None
    if materialization is not None and materialization.output_upload_session_id is not None:
        upload = await s.get(
            ArtifactUploadSession,
            materialization.output_upload_session_id,
        )
        if upload is not None and upload.team_id == trial.team_id:
            source_counts = (
                await s.execute(
                    select(
                        func.count(ArtifactUploadFile.file_index).label("required_file_count"),
                        func.coalesce(
                            func.sum(
                                func.coalesce(
                                    ArtifactUploadFile.expected_size,
                                    ArtifactUploadFile.expected_max_bytes,
                                )
                            ),
                            0,
                        ).label("required_size_bytes"),
                        func.coalesce(
                            func.sum(
                                case((ArtifactUploadFile.state == "verified", 1), else_=0)
                            ),
                            0,
                        ).label("committed_file_count"),
                        func.coalesce(
                            func.sum(
                                case(
                                    (
                                        ArtifactUploadFile.state == "verified",
                                        ArtifactUploadFile.actual_size,
                                    ),
                                    else_=0,
                                )
                            ),
                            0,
                        ).label("committed_size_bytes"),
                    ).where(ArtifactUploadFile.session_id == upload.id)
                )
            ).one()
            source_bundle = {
                "state": upload.state,
                "required_file_count": int(source_counts.required_file_count or 0),
                "required_size_bytes": int(source_counts.required_size_bytes or 0),
                "committed_file_count": int(source_counts.committed_file_count or 0),
                "committed_size_bytes": int(source_counts.committed_size_bytes or 0),
            }
    base["execution_selection"] = execution_selection_readback(trial.config, runtime_plan)
    base["materialization"] = (
        {
            "resource_allocation": (
                resource_allocation_summary(runtime_plan) if runtime_plan is not None else None
            ),
            "network_policy": {
                "task_default": task_default_network_policy,
                "requested_override": requested_network_policy,
                "effective": (
                    runtime_plan.effective_network_policy.model_dump(mode="json")
                    if runtime_plan is not None
                    and runtime_plan.effective_network_policy is not None
                    else None
                ),
            },
            "state": materialization.materialization_state,
            "lifecycle_stage": service_execution_lifecycle_stage(
                trial_state=trial.state,
                observed_state=materialization.observed_state,
                output_commit_state=materialization.output_commit_state,
                materialization_state=materialization.materialization_state,
                error_code=materialization.error_code,
                error_class=materialization.error_class,
            ),
            "compute_state": runtime_result.get("status"),
            "output_commit_state": materialization.output_commit_state,
            "canonical_ready": (
                materialization.materialization_state == "committed"
                and canonical_bundle is not None
            ),
            "backend": "nebius",
            "pool_id": materialization.selected_pool_id,
            "target_id": materialization.target_id if is_admin(ctx) else None,
            "execution_state": materialization.observed_state,
            "submitted_at": trial.submitted_at.isoformat(),
            "pod_scheduled_at": (
                materialization.pod_scheduled_at.isoformat()
                if materialization.pod_scheduled_at
                else None
            ),
            "pod_started_at": (
                materialization.pod_started_at.isoformat()
                if materialization.pod_started_at
                else None
            ),
            "pod_terminated_at": (
                materialization.pod_terminated_at.isoformat()
                if materialization.pod_terminated_at
                else None
            ),
            "output_committed_at": (
                materialization.output_committed_at.isoformat()
                if materialization.output_committed_at
                else None
            ),
            "source_bundle": source_bundle,
            "attempts": materialization.materialization_attempts,
            "next_attempt_at": (
                materialization.materialization_next_attempt_at.isoformat()
                if materialization.materialization_next_attempt_at
                else None
            ),
            "started_at": (
                materialization.materialization_started_at.isoformat()
                if materialization.materialization_started_at
                else None
            ),
            "committed_at": (
                materialization.materialization_committed_at.isoformat()
                if materialization.materialization_committed_at
                else None
            ),
            "error": (
                {
                    "code": materialization.materialization_error_code,
                    "message": (
                        materialization.materialization_error_message
                        if is_admin(ctx)
                        else "Canonical transfer did not complete; Loom will retry automatically."
                    ),
                }
                if materialization.materialization_error_code
                else (
                    {
                        "code": canonical_bundle_error.code,
                        "message": "The canonical Trial bundle failed integrity validation.",
                    }
                    if canonical_bundle_error is not None
                    else {
                        "code": materialization.output_unavailable_reason or "output_unavailable",
                        "message": "Execution ended before output could be retained; no canonical bundle is available.",
                    } if materialization.output_commit_state == "unavailable" else None
                )
            ),
            "trajectory_sha256": materialization.canonical_trajectory_sha256,
            "atif_sha256": materialization.canonical_atif_sha256,
            "source_cleanup_state": materialization.source_cleanup_state,
            "source_cleanup_attempts": materialization.source_cleanup_attempts,
            "source_cleanup_error_message": (
                (
                    materialization.source_cleanup_error_message
                    if is_admin(ctx)
                    else "Temporary source cleanup will retry automatically."
                )
                if materialization.source_cleanup_error_message
                else None
            ),
            "source_retain_until": (
                materialization.source_retain_until.isoformat()
                if materialization.source_retain_until
                else None
            ),
            "bundle": (
                {
                    "schema_version": "loom.canonical-trial-bundle-export.v1",
                    "artifact_id": str(canonical_bundle.artifact_id),
                    "file_count": len(canonical_bundle.files),
                    "size_bytes": sum(file.size_bytes for file in canonical_bundle.files),
                    "manifest_sha256": canonical_bundle.manifest_sha256,
                    "content_sha256": canonical_bundle.content_sha256,
                    "download_url": str(
                        public_url_for(
                            request,
                            "download_trial_bundle",
                            trial_id=str(trial.id),
                        )
                    ),
                }
                if canonical_bundle is not None
                and materialization.materialization_state == "committed"
                else None
            ),
        }
        if materialization is not None
        else None
    )
    base["execution_phases"] = (
        await _execution_phases(s, trial=trial, attempt=materialization.attempt, admin=is_admin(ctx))
        if materialization is not None
        else None
    )
    service_execution_artifacts = list(
        (
            await s.execute(
                select(Artifact).where(
                    Artifact.trial_id == trial.id,
                    Artifact.team_id == trial.team_id,
                    Artifact.control_producer_kind == "service_execution",
                )
            )
        )
        .scalars()
        .all()
    )
    base["execution_provenance"] = trial_execution_provenance(
        trial, materialization, artifacts=service_execution_artifacts,
    ).model_dump(mode="json")
    # The worker's TrajectoryWriter writes events.jsonl under
    # `<trajectories_bucket>/<team_id>/<trial_id>/events.jsonl`;
    # finalize.py writes ATIF to the same bucket at `atif.json`.
    # Both user-facing URLs stay on loom_service so remote browser
    # clients do not need direct MinIO reachability.
    base["atif_url"] = str(
        public_url_for(request, "download_atif", trial_id=str(trial.id)),
    )
    base["trajectory_url"] = str(
        public_url_for(request, "download_trajectory", trial_id=str(trial.id)),
    )
    # `*_ready` flags so the SPA can avoid rendering a download link
    # that's going to 404. The trajectory exists as soon as the worker
    # starts the trial (first event flushed); ATIF only after finalize.
    # A build failure or cancellation before execution never reaches finalize.
    is_terminal = trial.state in {"succeeded", "failed", "cancelled"}
    base["atif_ready"] = bool(trajectory_index.get("atif_uri")) or (
        is_terminal and trial.started_at is not None and trial.finished_at is not None
    )
    base["trajectory_ready"] = bool(trajectory_index.get("trajectory_uri")) or (
        trial.started_at is not None
    )
    base["artifacts"] = _merge_projected_artifacts(
        _projected_artifacts(
            request,
            trajectory_index=trajectory_index,
            trial_id=trial.id,
        ),
        _projected_service_execution_artifacts(
            request,
            trial_id=trial.id,
            artifacts=service_execution_artifacts,
        ),
    )
    debug_evidence = build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        execution_failure=await read_trial_execution_failure(s, trial),
        **debug_context,
    )
    if trial.state == "failed" and debug_evidence["failure"].get("reason") == "oom_killed":
        base["failure_reason"] = "oom_killed"
        base["failure_message"] = debug_evidence["failure"]["message"]
    base["debug_evidence"] = debug_evidence
    base["diagnosis"] = build_trial_diagnosis(debug_evidence)
    plan_row = await load_model_switch_plan(s, trial.id)
    if plan_row is not None:
        base["model_switch_plan"] = plan_snapshot_from_row(plan_row).model_dump(
            mode="json",
        )
        base["usage_by_role"] = usage_by_role(llm_calls)
    return base


def _trial_bundle_body(archive: ArchiveBuildResult) -> Iterator[bytes]:
    try:
        while chunk := archive.body.read(1024 * 1024):
            yield chunk
    finally:
        close = getattr(archive.body, "close", None)
        if callable(close):
            close()


@router.get("/trials/{trial_id}/bundle/download")
async def download_trial_bundle(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> StreamingResponse:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(select(Trial).where(Trial.id == trial_id))
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    try:
        bundle = await canonical_bundle_for_trial(s, trial=trial)
        if bundle is None:
            raise HTTPException(status_code=409, detail="canonical Trial bundle is not ready")
        archive = build_canonical_trial_bundle_archive(
            client=request.app.state.minio_client,
            bundle=bundle,
        )
    except DeliveryExportError as exc:
        detail = (
            exc.detail
            if is_admin(ctx)
            else {
                "code": exc.code,
                "message": "The canonical Trial bundle failed integrity validation.",
            }
        )
        raise HTTPException(status_code=exc.status_code, detail=detail) from exc
    filename = f"{trial.id}-complete-trial-bundle.tar.gz"
    return StreamingResponse(
        _trial_bundle_body(archive),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(archive.size_bytes),
            "X-Content-SHA256": f"sha256:{archive.sha256}",
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/trials/{trial_id}/resource-usage")
async def get_trial_resource_usage(
    sc: SessionAndCtx,
    trial_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (await s.execute(select(Trial).where(Trial.id == trial_id))).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    rows = list(
        (
            await s.execute(
                select(TrialResourceUsage)
                .where(TrialResourceUsage.trial_id == trial_id)
                .order_by(
                    TrialResourceUsage.attempt_count.asc(),
                    TrialResourceUsage.container_role.asc(),
                    TrialResourceUsage.role_name.asc(),
                    TrialResourceUsage.execution_key.asc(),
                )
            )
        )
        .scalars()
        .all()
    )
    response = resource_usage_response(rows)
    response["termination_failures"] = await execution_failure_groups(s, trial_id=trial_id)
    return response


@router.get("/trials/{trial_id}/debug", response_model=wire.DebugEvidence, response_model_exclude_unset=True)
async def get_trial_debug(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    return build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        execution_failure=await read_trial_execution_failure(s, trial),
        **debug_context,
    )


@router.get("/trials/{trial_id}/diagnosis", response_model=wire.DiagnosisReport, response_model_exclude_unset=True)
async def get_trial_diagnosis(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    debug_evidence = build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        execution_failure=await read_trial_execution_failure(s, trial),
        **debug_context,
    )
    return build_trial_diagnosis(debug_evidence)


@router.get("/trials/{trial_id}/artifacts/download")
async def download_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    key: Annotated[str, Query(min_length=1)],
) -> StreamingResponse:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    artifact = _find_projected_artifact(trial.trajectory_index, key)
    if artifact is not None:
        return stream_object_response(
            client=request.app.state.minio_client,
            bucket=_artifact_bucket(artifact, settings.artifacts_bucket),
            key=key,
            filename=_artifact_filename(key),
            artifact_kind="artifact",
        )

    service_artifacts = list(
        (
            await s.execute(
                select(Artifact).where(
                    Artifact.trial_id == trial.id,
                    Artifact.team_id == trial.team_id,
                    Artifact.control_producer_kind == "service_execution",
                )
            )
        )
        .scalars()
        .all()
    )
    for service_artifact in service_artifacts:
        storage = (
            service_artifact.storage if isinstance(service_artifact.storage, dict) else {}
        )
        files = storage.get("files")
        source_evidence = storage.get("source_evidence", [])
        if not isinstance(files, list) or not isinstance(source_evidence, list):
            continue
        for file_item in (*files, *source_evidence):
            if not isinstance(file_item, dict):
                continue
            if _service_execution_file_key(service_artifact, file_item) != key:
                continue
            media_type = file_item.get("media_type")
            return stream_object_response(
                client=request.app.state.minio_client,
                bucket=(
                    file_item["bucket"]
                    if isinstance(file_item.get("bucket"), str) and file_item["bucket"]
                    else settings.artifacts_bucket
                ),
                key=key,
                filename=_artifact_filename(key),
                artifact_kind="service_execution_artifact",
                media_type=(
                    media_type if isinstance(media_type, str) else "application/octet-stream"
                ),
            )

    raise HTTPException(status_code=404, detail="artifact not found")


class _SubmitReq(BaseModel):
    task_id: str
    config: dict[str, Any]
    # Direct release smokes may pin one trial to an exact worker pool and
    # replay safely. The Control Plane owns validation and persistence for
    # both fields; the Service must preserve them at the proxy boundary.
    required_worker_pool: str | None = None
    idempotency_key: str | None = None
    # cluster-deploy.md §Schema additions: optional per-trial provider
    # override. When set, gateway uses this connection's decrypted
    # api_key + base_url; otherwise platform default. UUID is
    # validated against the caller's team before forwarding.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


def _validate_agent_name(config: dict[str, Any]) -> None:
    """Reject a trial_config whose `agent_name` isn't in the catalog.
    Server-side defense in depth — the SPA already restricts the
    dropdown to known names, but a direct API caller (curl, sdk) can
    still send anything. Validating here keeps a typo'd or hostile
    name from reaching the worker."""
    agent_name = config.get("agent_name")
    if not isinstance(agent_name, str) or not agent_name:
        # The Control Plane's TrialConfig.model_validate will 422 this
        # — propagate cleanly rather than 400 here so the error shape
        # stays consistent.
        return
    if agent_name not in known_names():
        raise HTTPException(
            status_code=400,
            detail=(f"unknown agent_name {agent_name!r}. GET /api/v1/agents for the catalog."),
        )
    model_raw = config.get("agent_model")
    model: ModelSpec | None
    if model_raw is None:
        model = None
    else:
        try:
            model = ModelSpec.model_validate(model_raw)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"agent_model failed to validate: {exc}",
            ) from exc
    err = validate_agent_model_compat(agent_name, model)
    if err is not None:
        raise HTTPException(status_code=400, detail=err)


def _resolve_trial_provider_route(payload: _SubmitReq) -> _SubmitReq:
    """Apply the batch submission's connection/model rules (#2054) to one
    trial: a matching resolved pair for model-backed agents, none for
    no-model agents."""
    agent_name = payload.config.get("agent_name")
    if not isinstance(agent_name, str) or not agent_name:
        return payload
    model_raw = payload.config.get("agent_model")
    try:
        route = resolve_provider_route(
            context="trial",
            agent_model=None if model_raw is None else ModelSpec.model_validate(model_raw),
            connection_id=payload.provider_connection_id,
            model_id=payload.provider_model_id,
        )
    except ProviderRouteError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return payload.model_copy(
        update={
            "provider_connection_id": route.connection_id,
            "provider_model_id": route.model_id,
        },
    )


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    sc: SessionAndCtx,
    payload: _SubmitReq,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Authenticate locally, check `submit` scope, then proxy to
    Control Plane's POST /trials. The CP runs the canonical license-
    allowlist + team-quota checks; this route just keeps unauthorized
    requests from touching the upstream at all."""
    s, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    _validate_agent_name(payload.config)
    payload = _resolve_trial_provider_route(payload)
    # Hosted single trials run natively on Nebius (#2054).
    if not local_execution_enabled():
        native_err = native_selections_error(
            NEBIUS_BACKEND, selection_agents(payload.config, []),
        )
        if native_err is not None:
            raise HTTPException(status_code=400, detail=native_err)
    await validate_submission_agent_task_compatibility(
        s,
        team_id=ctx.team_id,
        task_ids=[payload.task_id],
        trial_config=payload.config,
    )

    # Validate the optional provider_connection_id before forwarding.
    # Doing this in loom_service (not control-plane) avoids a
    # round-trip + keeps the user-facing error close to the user-
    # supplied input.
    if payload.provider_connection_id is not None and ctx.team_id is not None:
        await validate_provider_connection(
            s,
            payload.provider_connection_id,
            team_id=ctx.team_id,
            agent_submission=True,
        )

    try:
        body, submission_id = await prepare_trial_submission(s, request.app.state.settings,
            team_id=ctx.team_id, user_id=ctx.user_id, payload=payload.model_dump(mode="json"))
    except ValueError:
        raise HTTPException(status_code=400, detail="submission provenance unavailable") from None
    # The CP independently verifies the caller. Commit the origin and release
    # browser-auth locks before that network hop, including legacy configuration.
    await s.commit()
    resp = await forward(
        request.app.state.http_client,
        method="POST",
        path="/trials",
        authorization=authorization,
        json_body=body,
        submission_request=request,
        submission_id=submission_id,
    )
    return propagate(resp)


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Same-team check happens BEFORE the forward so we don't burn
    a CP round-trip when the caller is unauthorized."""
    s, ctx = sc
    require_scope(ctx, "submit")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    if ctx.auth_kind == "session":
        # Release the session-auth row lock before the CP revalidates that row.
        await s.commit()
    resp = await forward(
        request.app.state.http_client,
        method="POST",
        path=f"/trials/{trial_id}/cancel",
        authorization=authorization,
        cancellation_request=request,
    )
    return propagate(resp)
