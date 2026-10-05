"""Batches CRUD (spec §5.3 / Plan 19, renamed in Plan 28).

Routes:
- POST /api/v1/batches          — create + immediately materialize
                                  expected_trial_count from task_filter
- GET  /api/v1/batches          — list with cursor pagination
- GET  /api/v1/batches/{id}     — detail + trial roll-up (state summary,
                                  aggregate reward from Trial.result JSONB,
                                  and token totals from llm_calls)
- POST /api/v1/batches/{id}/cancel — terminate the batch + cascade-cancel
                                  its still-active trials
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

from loom_service.pool_submission import submission_origin

if TYPE_CHECKING:
    from loom.family_run.spec import FamilyRunSpec
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, TypeAdapter, ValidationError
from sqlalchemy import and_, func, or_, select, update

from loom.auth import AuthContext
from loom.data_lifecycle_registry import ensure_batch_lifecycle_authority
from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    ProviderConnection,
    ProviderModelCache,
    ServiceExecutionLease,
    Task,
    Team,
    TeamMembership,
    Trial,
    TrialResourceUsage,
    User,
    Worker,
)
from loom.execution_diagnosis_store import execution_failure_groups
from loom.hosted_harness import harnesses_supporting, hosted_harness
from loom.models.batch import Combination
from loom.models.networking import (
    NetworkPolicy,
    UnsupportedNetworkPolicyOverrideError,
    hosted_http_egress,
    resolve_effective_network_policy,
)
from loom.models.task import TaskConfig
from loom.models.types import ModelSpec
from loom.pipeline.keys import canonical_digest
from loom.request_params import sanitize_request_extras
from loom.resource_usage_store import resource_usage_response
from loom.security.redaction import redact_mapping, redact_text
from loom.service_execution_backend import (
    NEBIUS_BACKEND,
    local_execution_enabled,
)
from loom.service_execution_materialization import (
    TaskExecutionResourceRequestsV1,
    load_service_execution_runtime_profile,
)
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
)
from loom_service import wire_responses as wire
from loom_service.admin_audit import (
    actor_from_context,
    write_admin_audit_event,
)
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
from loom_service.batch_identity import build_batch_identity
from loom_service.batch_purpose import (
    BatchPurpose,
    validate_purpose_resolved_tasks,
    validate_purpose_task_filter,
    validate_purpose_trial_config,
)
from loom_service.combination_summary import combination_summary_for_batch
from loom_service.debug_evidence import build_batch_debug_evidence
from loom_service.dependencies import AdminSessionAndCtx, SessionAndCtx
from loom_service.diagnosis import build_batch_diagnosis, trial_failure_records
from loom_service.effective_combination import (
    ProviderRouteError,
    derive_stored_routes,
    resolve_provider_route,
)
from loom_service.execution_admission import (
    admit_execution_backend,
    freeze_task_resource_requests,
    preview_execution_selection,
    reject_submission,
    reject_unsupported_hosted_backend,
)
from loom_service.failure_taxonomy import (
    build_supplemental_rerun_plan,
    is_auto_safe_rerun,
    is_replaceable_by_successful_supplemental,
)
from loom_service.family_run_seed import prepare_family_run_state
from loom_service.forwarders import forward, propagate
from loom_service.metrics import SUBMISSION_REJECTS_TOTAL
from loom_service.monitor_filters import (
    apply_batch_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.multi_model import (
    apply_plan_mode,
    parse_multi_model,
    validate_multi_model_for_batch,
)
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.provider_connections_service import preflight_failure_kind
from loom_service.service_execution_status import (
    SERVICE_EXECUTION_LIFECYCLE_STAGES,
    service_execution_lifecycle_case,
)
from loom_service.stale_running_debug import batch_stale_running_decisions
from loom_service.submission_compat import (
    AgentTaskIncompatibilityError,
    validate_submission_agent_task_compatibility,
    validate_submission_agent_task_pairs,
)
from loom_service.task_config_validation import (
    expected_trial_count,
    invalid_task_config_detail,
    split_valid_task_configs,
)
from loom_service.task_filter import resolve_task_filter_with_diagnostics
from loom_service.trial_progress import latest_execution_id, progress_summary
from loom_service.trial_timing import trial_started_at
from loom_service.usage_accounting import (
    PreRunBudgetEstimate,
    _cost_confidence_counts,
    _cost_source_counts,
    _price_unknown_call_filter,
    _priced_call_filter,
    empty_usage_projection,
    estimate_pre_run_batch_budget,
    price_snapshots_for_trials,
    project_batch_budget,
    summarize_llm_evidence_for_trials,
    summarize_usage_counts,
    usage_status_filter,
)
from loom_service.usage_accounting import (
    cost_meta_filter as _cost_meta_filter,
)
from loom_service.usage_accounting import (
    usage_by_batch_ids as _usage_by_batch_ids,
)

router = APIRouter()


def _catalog_family_run_defaults(benchmark_id: str | None) -> FamilyRunSpec | None:
    """Fetch the ``family_run_defaults`` block from the benchmark
    catalog (JSON shipped in ``loom_benchmarks``). None when the
    benchmark has no defaults or when the entry is missing.
    """
    from loom.family_run.spec import FamilyRunSpec

    if benchmark_id is None:
        return None
    try:
        from loom_benchmarks.catalog import CATALOG
    except ImportError:
        return None
    entry = CATALOG.get(benchmark_id)
    if entry is None or entry.family_run_defaults is None:
        return None
    return FamilyRunSpec.model_validate(entry.family_run_defaults)


def _build_service_state_backend(request: Request) -> Any:
    """Materialise a state backend on the service side.

    Wraps the boto3 MinIO client into a :class:`MinioObjectStore` +
    :class:`S3ArtifactsStateBackend`. Kept as a helper so tests can
    monkey-patch it with a fake without touching the route body.
    """
    from loom.family_run.state_backends import S3ArtifactsStateBackend
    from loom.trajectory.storage import MinioObjectStore

    settings = request.app.state.settings
    store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    return S3ArtifactsStateBackend(store=store, bucket=settings.artifacts_bucket)


_RERUNNABLE_FAILURE_REASONS: frozenset[str] = frozenset(
    {
        "gateway_error",
        "provider_transport_disconnect",
        "retry_exhausted",
        "exhausted_retries",
    }
)


@dataclass(frozen=True)
class _BatchTrialProjection:
    id: UUID
    batch_id: UUID | None
    team_id: UUID
    task_id: str
    config: dict[str, Any]
    state: str
    failure_reason: str | None
    failure_message: str | None
    result: dict[str, Any] | None
    claimed_at: datetime | None
    pre_start_heartbeat_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    sample_idx: int
    combination_idx: int
    provider_connection_id: UUID | None
    provider_model_id: str | None
    worker_id: UUID | None


class _CreateBatch(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    name_suffix: str | None = Field(default=None, max_length=80)
    description: str | None = None
    # evaluation = native benchmarks only, verification required;
    # trajectory_generation = TaskSets and/or benchmarks (transition).
    purpose: BatchPurpose
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    # Plan 23: n-sampling for single-combination batches. Ignored
    # when `combinations` is non-empty (each Combination carries
    # its own n_per_task).
    n_per_task: int = Field(default=1, ge=1, le=100)
    # Hosted submissions default to Nebius, so callers should omit this. An
    # explicit "nebius" is compatibility input; any other hosted value is
    # rejected by reject_unsupported_hosted_backend before any work.
    # Disposable local stacks (LOOM_LOCAL_EXECUTION=1) default to Docker.
    backend: str = Field(
        default_factory=lambda: "docker" if local_execution_enabled() else NEBIUS_BACKEND,
        # Not `deprecated=True`: pydantic warns on every attribute read.
        json_schema_extra={"deprecated": True},
        description=(
            "Deprecated. Hosted execution is Nebius-only: omit this field. "
            "An explicit 'nebius' is accepted for compatibility; other hosted "
            "values are rejected."
        ),
    )
    # Plan 28 PR-3: multi-(agent, model) combinations. Empty list
    # ⇒ single-combination behavior (agent + model come from
    # trial_config).
    combinations: list[Combination] = Field(default_factory=list)
    # cluster-deploy.md §Schema additions: batch-level provider
    # override. Carries through fan-out into Trial.provider_connection_id
    # for every trial spawned from this batch (unless overridden on
    # a per-trial basis at submission). Validated against the team
    # before insertion.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None
    # Explicit on-behalf-of team for platform admins. Non-admin callers
    # may omit this or pass their own team id; cross-team values require
    # admin scope and are used for provider validation + Batch.team_id.
    team_id: UUID | None = None
    budget_usd: float | None = Field(default=None, ge=0)
    budget_policy: Literal["none", "soft", "hard"] = "none"
    budget_confirmed: bool = False
    model_switch_plan_mode: Literal["inherit", "resample"] | None = None
    task_resource_requests: dict[str, TaskExecutionResourceRequestsV1] = Field(
        default_factory=dict, max_length=5000,
    )


class _AdminCreateBatchOnBehalf(_CreateBatch):
    represented_user_id: UUID | None = None
    represented_username: str | None = Field(default=None, max_length=64)
    # Issue #188 / #1109: operator/release canaries only. User
    # POST /batches must not admit this field — coverage trials belong
    # on a separate admin on-behalf batch identity.
    required_worker_pools: list[str] = Field(default_factory=list, max_length=20)


class _NetworkPolicyPreviewRequest(BaseModel):
    task_filter: dict[str, Any]
    baseline_network_policy_override: NetworkPolicy | None = None
    agent_names: list[str] = Field(default_factory=list, max_length=50)
    team_id: UUID | None = None


class _NetworkPolicyPreviewChoice(BaseModel):
    kind: Literal["gateway-only", "web-allowlist", "public-web"]
    available: bool
    incompatible_task_ids: list[str]
    reasons: list[str]


class _NetworkPolicyDefaultGroup(BaseModel):
    policy: dict[str, Any]
    task_ids: list[str]


class _NetworkPolicyPreviewResponse(BaseModel):
    task_count: int
    authored_defaults: list[_NetworkPolicyDefaultGroup]
    choices: list[_NetworkPolicyPreviewChoice]
    selected_incompatible_task_ids: list[str]
    selected_rejection_reasons: dict[str, list[str]]
    widens_task_ids: list[str]


_NETWORK_POLICY_ADAPTER: TypeAdapter[NetworkPolicy] = TypeAdapter(NetworkPolicy)


def _network_policy_groups(
    policies: Sequence[tuple[str, NetworkPolicy]],
) -> list[dict[str, Any]]:
    groups: dict[str, tuple[dict[str, Any], list[str]]] = {}
    for task_id, policy in policies:
        payload = policy.model_dump(mode="json")
        key = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        groups.setdefault(key, (payload, []))[1].append(task_id)
    return [
        {"policy": payload, "task_ids": sorted(task_ids)}
        for _, (payload, task_ids) in sorted(groups.items())
    ]


async def _batch_network_policy_evidence(
    session: Any,
    *,
    task_ids: Sequence[str],
    trial_config: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate authored and resolved policy without per-Trial UI requests."""
    task_ids = sorted(set(task_ids))
    rows = (
        await session.execute(select(Task.id, Task.config).where(Task.id.in_(task_ids)))
    ).all()
    requested_raw = trial_config.get("baseline_network_policy_override")
    try:
        requested = (
            _NETWORK_POLICY_ADAPTER.validate_python(requested_raw)
            if requested_raw is not None
            else None
        )
    except ValidationError:
        requested = None
    authored: list[tuple[str, NetworkPolicy]] = []
    resolved: list[tuple[str, NetworkPolicy]] = []
    unavailable: list[str] = []
    # Batch detail must stay readable for historical or partial task configs;
    # those tasks are reported rather than failing the whole response.
    for task_id, raw_config in rows:
        try:
            task = TaskConfig.model_validate(raw_config)
        except ValidationError:
            unavailable.append(str(task_id))
            continue
        authored.append((str(task_id), task.environment.baseline_network_policy))
        try:
            effective = resolve_effective_network_policy(
                baseline=task.environment.baseline_network_policy,
                supported=task.environment.network_policies_supported,
                override=requested,
            )
        except UnsupportedNetworkPolicyOverrideError:
            unavailable.append(str(task_id))
            continue
        resolved.append((str(task_id), effective))
    return {
        "authored_defaults": _network_policy_groups(authored),
        "requested_override": (
            requested.model_dump(mode="json") if requested is not None else None
        ),
        "resolved_effective": _network_policy_groups(resolved),
        "unavailable_task_ids": sorted(unavailable),
    }


class _RerunFailedBatch(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=5000)
    include_operator_approval: bool = False
    use_current_runtime: bool = False
    model_switch_plan_mode: Literal["inherit", "resample"] = "inherit"


def _sanitize_trial_config(config: dict[str, Any]) -> dict[str, Any]:
    out = dict(config)
    if "request_params" in out:
        out["request_params"] = sanitize_request_extras(out.get("request_params"))
    return out


def _multi_model_block(trial_config: dict[str, Any] | None) -> dict[str, Any]:
    raw = (trial_config or {}).get("multi_model")
    return raw if isinstance(raw, dict) else {}


def _multi_model_teacher_id(trial_config: dict[str, Any] | None) -> str | None:
    mm = _multi_model_block(trial_config)
    if not mm.get("enabled"):
        return None
    secondary = mm.get("secondary_model") or {}
    if isinstance(secondary, dict):
        name = secondary.get("name")
        return str(name) if name else None
    return None


def _multi_model_teacher_episodes(trial_config: dict[str, Any] | None) -> int | None:
    mm = _multi_model_block(trial_config)
    if not mm.get("enabled"):
        return None
    try:
        return int(mm.get("teacher_episodes") or 2)
    except (TypeError, ValueError):
        return 2


def _multi_model_episode_ceiling(trial_config: dict[str, Any] | None) -> int | None:
    mm = _multi_model_block(trial_config)
    if not mm.get("enabled"):
        return None
    try:
        return int(mm.get("episode_ceiling") or 50)
    except (TypeError, ValueError):
        return 50


def _reject_invalid_workspace_staging_policy_name(
    trial_config: dict[str, Any],
) -> None:
    """#1263: early reject unknown named staging policies at batch accept."""
    policy_name = trial_config.get("workspace_staging_policy_name")
    if policy_name is None:
        return
    if not isinstance(policy_name, str) or policy_name not in {"tb21", "none"}:
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail=("trial_config.workspace_staging_policy_name must be 'tb21' or 'none'"),
        )


def _extract_family_run_override(
    trial_config: dict[str, Any],
) -> FamilyRunSpec | None:
    """Read the optional ``family_run`` block off the trial_config.

    Returns ``None`` when the field is absent (classic mode) or the
    value is ``None``. Any parse error is surfaced to the caller as a
    :class:`ValueError` — the batches route translates that into a 400.
    """
    from loom.family_run.spec import FamilyRunSpec

    raw = trial_config.get("family_run")
    if raw is None:
        return None
    if isinstance(raw, FamilyRunSpec):
        return raw
    return FamilyRunSpec.model_validate(raw)


def _normalize_required_worker_pools(values: Sequence[str]) -> list[str]:
    pools: list[str] = []
    seen: set[str] = set()
    for raw in values:
        pool = str(raw).strip()
        if not pool:
            reject_submission(
                reason="invalid_input",
                status_code=400,
                detail="required_worker_pools entries must be non-empty strings",
            )
        if len(pool) > 80 or any(ch.isspace() for ch in pool):
            reject_submission(
                reason="invalid_input",
                status_code=400,
                detail=(
                    "required_worker_pools entries must be 1-80 characters "
                    "and contain no whitespace"
                ),
            )
        if pool not in seen:
            seen.add(pool)
            pools.append(pool)
    return pools


def _reject_if_k8s_worker_unavailable(
    request: Request,
    required_worker_pools: Sequence[str],
) -> None:
    """Fail-loudly gate for #383. When the cluster's
    `k8s_worker.enabled` render toggle is false, the k8s loom-worker
    Deployment is not rendered, so `k8s-worker` pool coverage cannot
    be satisfied. Reject up front instead of accepting a submission
    whose coverage trial will queue forever.
    """
    if "k8s-worker" not in required_worker_pools:
        return
    settings = request.app.state.settings
    if settings.k8s_worker_enabled:
        return
    reject_submission(
        reason="k8s_worker_unavailable",
        status_code=400,
        detail=(
            "required_worker_pool 'k8s-worker' is not available on this "
            "cluster: k8s_worker.enabled=false in the deployed profile "
            "(#383). Choose an available x86_64 execution pool. "
            "ARM execution is no longer supported."
        ),
    )


async def _reject_if_team_paused(session: Any, team_id: UUID) -> None:
    paused_at = (
        await session.execute(
            select(Team.submissions_paused_at).where(Team.id == team_id),
        )
    ).scalar_one_or_none()
    if paused_at is not None:
        reject_submission(
            reason="team_paused",
            status_code=403,
            detail="team submissions are paused",
        )


async def _resolve_on_behalf_submitter(
    session: Any,
    payload: _AdminCreateBatchOnBehalf,
) -> User:
    if payload.team_id is None:
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="team_id is required for admin on-behalf batch submission",
        )
    represented_user_id = payload.represented_user_id
    represented_username = (
        payload.represented_username.strip() if payload.represented_username is not None else None
    )
    if bool(represented_user_id) == bool(represented_username):
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail=("set exactly one of represented_user_id or represented_username"),
        )

    team = (
        await session.execute(
            select(Team).where(Team.id == payload.team_id),
        )
    ).scalar_one_or_none()
    if team is None:
        reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="team not found",
        )
    if team.disabled_at is not None:
        reject_submission(
            reason="permission",
            status_code=403,
            detail="represented team is disabled",
        )

    if represented_user_id is not None:
        stmt = select(User).where(User.id == represented_user_id)
    else:
        assert represented_username is not None
        stmt = select(User).where(
            User.username_normalized == represented_username.casefold(),
        )
    user = (await session.execute(stmt)).scalar_one_or_none()
    if user is None:
        reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="represented user not found",
        )
    if user.status != "active" or user.disabled_at is not None:
        reject_submission(
            reason="permission",
            status_code=403,
            detail="represented user is not active",
        )

    membership = (
        await session.execute(
            select(TeamMembership).where(
                TeamMembership.team_id == payload.team_id,
                TeamMembership.user_id == user.id,
            ),
        )
    ).scalar_one_or_none()
    if membership is None:
        reject_submission(
            reason="permission",
            status_code=403,
            detail="represented user is not a member of the represented team",
        )
    return cast(User, user)


async def _resolve_submission_team_id(
    session: Any,
    ctx: AuthContext,
    requested_team_id: UUID | None,
) -> UUID:
    if requested_team_id is None:
        if ctx.team_id is None:
            reject_submission(
                reason="invalid_input",
                status_code=400,
                detail="admin tokens must scope batches to a team — "
                "set team_id or use a team-scoped user token",
            )
        assert ctx.team_id is not None
        return ctx.team_id

    if not is_admin(ctx):
        if ctx.team_id != requested_team_id:
            reject_submission(
                reason="permission",
                status_code=403,
                detail="cross-team batch submission requires admin scope",
            )
        return requested_team_id

    exists = (
        await session.execute(
            select(Team.id).where(Team.id == requested_team_id),
        )
    ).scalar_one_or_none()
    if exists is None:
        reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="team not found",
        )
    return requested_team_id


async def _reject_if_known_failed_provider_model(
    session: Any,
    *,
    provider_connection_id: UUID | None,
    provider_model_id: str | None,
    context: str | None = None,
) -> None:
    if provider_connection_id is None or not provider_model_id:
        return
    row = (
        await session.execute(
            select(ProviderModelCache).where(
                ProviderModelCache.provider_connection_id == provider_connection_id,
                ProviderModelCache.model_id == provider_model_id,
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        prefix = f"{context}: " if context else ""
        reject_submission(
            reason="provider_model_cache",
            status_code=400,
            detail=(
                f"{prefix}provider model {provider_model_id!r} is not in the model cache "
                "for this provider connection; run `loom providers models "
                "NAME --refresh` or choose a cached model"
            ),
        )
    # Only a definitive rejection blocks (#948). A timed-out or otherwise
    # inconclusive probe is not evidence the model is unusable; the run
    # itself surfaces a real failure if there is one.
    if preflight_failure_kind(
        row.last_preflight_status,
        row.last_preflight_error_code,
        row.last_preflight_http_status,
    ) != "rejected":
        return
    prefix = f"{context}: " if context else ""
    detail = f"{prefix}provider model {provider_model_id!r} last preflight failed for this provider connection"
    if row.last_preflight_error_code:
        detail += f" ({row.last_preflight_error_code})"
    if row.last_preflight_at is not None:
        detail += f" at {row.last_preflight_at.isoformat()}"
    detail += "; run provider model preflight again or choose another model"
    reject_submission(
        reason="provider_model_preflight",
        status_code=400,
        detail=detail,
    )


def _effective_provider_fields(
    payload: _CreateBatch,
    combo: Combination | None,
) -> tuple[UUID | None, str | None]:
    """Provider route of a payload already passed through
    `_resolve_payload_provider_routes`: each combination carries its own
    resolved pair, so nothing is inherited field by field."""
    if combo is None:
        return payload.provider_connection_id, payload.provider_model_id
    return combo.provider_connection_id, combo.provider_model_id


def _resolve_payload_provider_routes(
    payload: _CreateBatch,
    trial_config: dict[str, Any],
) -> _CreateBatch:
    """Return `payload` with every selection's connection/model resolved.

    Combinations get their own resolved pair (inheriting the batch-level
    defaults as a pair); a single-combination batch gets resolved
    batch-level fields. Rejects conflicting model identities and stale
    provider fields on no-model agents.
    """
    try:
        if payload.combinations:
            resolved: list[Combination] = []
            for i, combo in enumerate(payload.combinations):
                route = resolve_provider_route(
                    context=f"combinations[{i}]",
                    agent_model=combo.agent_model,
                    connection_id=combo.provider_connection_id,
                    model_id=combo.provider_model_id,
                    default_connection_id=payload.provider_connection_id,
                    default_model_id=payload.provider_model_id,
                )
                resolved.append(
                    combo.model_copy(
                        update={
                            "provider_connection_id": route.connection_id,
                            "provider_model_id": route.model_id,
                        },
                    ),
                )
            return payload.model_copy(update={"combinations": resolved})
        agent_name = trial_config.get("agent_name")
        if not isinstance(agent_name, str) or not agent_name:
            return payload
        model_raw = trial_config.get("agent_model")
        route = resolve_provider_route(
            context="trial_config",
            agent_model=None if model_raw is None else ModelSpec.model_validate(model_raw),
            connection_id=payload.provider_connection_id,
            model_id=payload.provider_model_id,
        )
    except ProviderRouteError as exc:
        reject_submission(reason="invalid_input", status_code=400, detail=str(exc))
    return payload.model_copy(
        update={
            "provider_connection_id": route.connection_id,
            "provider_model_id": route.model_id,
        },
    )


def _reject_if_not_natively_runnable(
    backend: str,
    selections: list[tuple[str, str]],
    *,
    action: str = "",
) -> None:
    err = native_selections_error(backend, selections)
    if err is not None:
        reject_submission(
            reason="agent_not_natively_runnable",
            status_code=400,
            detail=f"{action}{err}",
        )


def _disambiguate_derived_labels(
    combos: list[Combination],
    connection_names: dict[UUID, str],
) -> list[Combination]:
    """Name the connection in derived labels that would otherwise collide,
    so the same model can be compared across connections (#2054).
    Explicit labels are left alone; duplicates among them stay an error."""
    derived = [_derive_combination_label(c) for c in combos]
    counts = Counter(label for c, label in zip(combos, derived, strict=True) if not c.label)
    out: list[Combination] = []
    for combo, label in zip(combos, derived, strict=True):
        if not combo.label and counts[label] > 1 and combo.provider_connection_id is not None:
            name = connection_names.get(combo.provider_connection_id) or str(
                combo.provider_connection_id,
            )[:8]
            combo = combo.model_copy(update={"label": f"{label}@{name}"[:200]})
        out.append(combo)
    return out


def _combination_context(index: int, combo: Combination) -> str:
    label = combo.label or _derive_combination_label(combo)
    return f"combinations[{index}] {label!r}"


def _merge_budget_source(values: set[str]) -> str:
    if not values:
        return "none"
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


def _merge_budget_confidence(values: set[str]) -> str:
    if not values:
        return "unavailable"
    if "unavailable" in values:
        return "unavailable"
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


async def _estimate_pre_run_budget_for_payload(
    session: Any,
    *,
    payload: _CreateBatch,
    provider_connections_by_id: dict[UUID, ProviderConnection],
    task_count: int,
    expected_trial_count: int,
    required_worker_pool_count: int,
    settings: Any,
    budget_usd: float | None,
    budget_policy: str,
    provider_connection: ProviderConnection | None,
) -> tuple[PreRunBudgetEstimate, list[dict[str, Any]]]:
    if not payload.combinations:
        estimate = await estimate_pre_run_batch_budget(
            session,
            provider_connection=provider_connection,
            provider_model_id=payload.provider_model_id,
            expected_trial_count=expected_trial_count,
            settings=settings,
            budget_usd=budget_usd,
            budget_policy=budget_policy,
            secondary_model_id=_multi_model_teacher_id(payload.trial_config),
            teacher_episodes=_multi_model_teacher_episodes(payload.trial_config),
            episode_ceiling=_multi_model_episode_ceiling(payload.trial_config),
        )
        return estimate, []

    estimates: list[PreRunBudgetEstimate] = []
    items: list[dict[str, Any]] = []
    for i, combo in enumerate(payload.combinations):
        conn_id, model_id = _effective_provider_fields(payload, combo)
        combo_expected = task_count * int(combo.n_per_task)
        if i == 0:
            combo_expected += required_worker_pool_count
        estimate = await estimate_pre_run_batch_budget(
            session,
            provider_connection=(
                provider_connections_by_id.get(conn_id) if conn_id is not None else None
            ),
            provider_model_id=model_id,
            expected_trial_count=combo_expected,
            settings=settings,
            budget_usd=budget_usd,
            budget_policy=budget_policy,
            secondary_model_id=_multi_model_teacher_id(payload.trial_config),
            teacher_episodes=_multi_model_teacher_episodes(payload.trial_config),
            episode_ceiling=_multi_model_episode_ceiling(payload.trial_config),
        )
        estimates.append(estimate)
        items.append(
            {
                "combination_idx": i,
                "label": combo.label or _derive_combination_label(combo),
                "provider_connection_id": str(conn_id) if conn_id else None,
                "provider_model_id": model_id,
                "expected_trial_count": combo_expected,
                "pre_run_estimated_cost_usd": (estimate.pre_run_estimated_cost_usd),
                "cost_estimate_source": estimate.cost_estimate_source,
                "cost_estimate_confidence": estimate.cost_estimate_confidence,
                "unpriced_reason": estimate.unpriced_reason,
                "pre_run_estimated_llm_calls_count": (estimate.pre_run_estimated_llm_calls_count),
                "pre_run_estimated_prompt_tokens": (estimate.pre_run_estimated_prompt_tokens),
                "pre_run_estimated_completion_tokens": (
                    estimate.pre_run_estimated_completion_tokens
                ),
            }
        )

    costs = [estimate.pre_run_estimated_cost_usd for estimate in estimates]
    total_cost = (
        sum(cast(float, cost) for cost in costs)
        if all(cost is not None for cost in costs)
        else None
    )
    aggregate = PreRunBudgetEstimate(
        budget_usd=estimates[0].budget_usd if estimates else budget_usd,
        budget_policy=budget_policy,
        pre_run_estimated_cost_usd=total_cost,
        cost_estimate_source=_merge_budget_source(
            {estimate.cost_estimate_source for estimate in estimates}
        ),
        cost_estimate_confidence=_merge_budget_confidence(
            {estimate.cost_estimate_confidence for estimate in estimates}
        ),
        pre_run_estimated_llm_calls_count=sum(
            estimate.pre_run_estimated_llm_calls_count for estimate in estimates
        ),
        pre_run_estimated_prompt_tokens=sum(
            estimate.pre_run_estimated_prompt_tokens for estimate in estimates
        ),
        pre_run_estimated_completion_tokens=sum(
            estimate.pre_run_estimated_completion_tokens for estimate in estimates
        ),
        unpriced_reason=(None if total_cost is not None else "one_or_more_combinations_unpriced"),
    )
    return aggregate, [
        {
            "reason": "combination_budget_estimate",
            "items": items,
        }
    ]


def _serialize(
    b: Batch,
    *,
    summary: dict[str, int] | None = None,
    aggregate_reward: float | None = None,
    usage: dict[str, int] | None = None,
    owner_team: Team | None = None,
    submitted_by_user: User | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_profile = b.service_execution_runtime_profile
    out: dict[str, Any] = {
        "id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "purpose": b.purpose,
        "task_filter": b.task_filter,
        "trial_config": b.trial_config,
        "state": b.state,
        "result_status": b.result_status,
        "failure_reason": b.failure_reason,
        "failure_message": (redact_text(b.failure_message) if b.failure_message else None),
        "fanout_errors": redact_mapping(b.fanout_errors),
        "rerun_of_batch_id": (str(b.rerun_of_batch_id) if b.rerun_of_batch_id else None),
        "rerun_targets": b.rerun_targets,
        "created_at": b.created_at.isoformat(),
        "finished_at": (b.finished_at.isoformat() if b.finished_at else None),
        "created_by_token_prefix": b.created_by_token_prefix,
        "expected_trial_count": b.expected_trial_count,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "task_resource_requests": (runtime_profile or {}).get("task_resource_requests", {}),
        "service_execution_runtime_profile": (
            {
                "candidate_sha": runtime_profile["candidate_sha"],
                "sha256": canonical_digest(runtime_profile),
            }
            if runtime_profile is not None
            else None
        ),
        "combinations": b.combinations,
        "required_worker_pools": b.required_worker_pools,
        "visibility": b.visibility,
        "share_status": b.share_status,
        "source_provenance": b.source_provenance,
        "resolved_task_ids": b.resolved_task_ids,
        "submitted_by_user": (
            {
                "id": str(submitted_by_user.id),
                "username": submitted_by_user.username,
                "team_id": str(b.team_id),
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
    usage_projection = dict(usage or _empty_usage_projection())
    usage_projection.pop("total_cost_usd", None)
    out.update(usage_projection)
    out.update(project_batch_budget(b, usage_projection))
    if summary is not None:
        out["trial_summary"] = summary
        out["aggregate_reward"] = aggregate_reward
    if extra:
        out.update(extra)
    return out


async def _create_batch_record(
    request: Request,
    s: Any,
    ctx: AuthContext,
    payload: _CreateBatch,
    *,
    submitted_by_user_id: UUID | None,
    usage_attributed_user_id: UUID | None,
    usage_attributed_actor: str | None,
    dry_run: bool = False,
) -> dict[str, Any]:
    reject_unsupported_hosted_backend(payload.backend)
    submission_team_id = await _resolve_submission_team_id(
        s,
        ctx,
        payload.team_id,
    )
    await _reject_if_team_paused(s, submission_team_id)

    catalog = known_names()
    trial_config = _sanitize_trial_config(payload.trial_config)
    _reject_invalid_workspace_staging_policy_name(trial_config)
    purpose_trial_err = validate_purpose_trial_config(payload.purpose, trial_config)
    if purpose_trial_err is not None:
        reject_submission(
            reason=purpose_trial_err,
            status_code=400,
            detail=purpose_trial_err,
        )
    purpose_filter_err = validate_purpose_task_filter(
        payload.purpose,
        payload.task_filter,
    )
    if purpose_filter_err is not None:
        reject_submission(
            reason=purpose_filter_err,
            status_code=400,
            detail=purpose_filter_err,
        )
    required_worker_pools = _normalize_required_worker_pools(
        getattr(payload, "required_worker_pools", []) or [],
    )
    _reject_if_k8s_worker_unavailable(request, required_worker_pools)

    if payload.combinations:
        # Multi-combination batch. trial_config MUST NOT carry
        # agent_name / agent_model / n_per_task in this shape —
        # those live on each Combination.
        for forbidden in ("agent_name", "agent_model", "agent_version"):
            if forbidden in trial_config:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"trial_config.{forbidden} must be absent when "
                        "`combinations` is supplied — each combination "
                        "carries its own agent/model"
                    ),
                )
        # Catalog membership + agent⇄model compatibility per
        # Combination.
        for i, combo in enumerate(payload.combinations):
            if combo.agent_name not in catalog:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"combinations[{i}].agent_name "
                        f"{combo.agent_name!r} is not in the agent "
                        "catalog. GET /api/v1/agents."
                    ),
                )
            err = validate_agent_model_compat(
                combo.agent_name,
                combo.agent_model,
            )
            if err is not None:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"combinations[{i}]: {err}",
                )
    else:
        # Single-combination batch. Catalog check on the agent
        # embedded in trial_config + agent⇄model compatibility.
        agent_name = trial_config.get("agent_name")
        if isinstance(agent_name, str) and agent_name:
            if agent_name not in catalog:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"unknown agent_name {agent_name!r} in trial_config. GET /api/v1/agents."
                    ),
                )
            if "agent_model" not in trial_config:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        "trial_config.agent_model is required when "
                        "trial_config.agent_name is supplied; use null "
                        "for agents that do not call a model"
                    ),
                )
            model_raw = trial_config["agent_model"]
            model: ModelSpec | None
            if model_raw is None:
                model = None
            else:
                try:
                    model = ModelSpec.model_validate(model_raw)
                except Exception as exc:
                    reject_submission(
                        reason="invalid_input",
                        status_code=400,
                        detail=(f"trial_config.agent_model failed to validate: {exc}"),
                    )
            err = validate_agent_model_compat(agent_name, model)
            if err is not None:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"trial_config: {err}",
                )

    # #2054: resolve every selection's connection/model once. From here on,
    # preflight, budget, persistence and fan-out all read these values.
    payload = _resolve_payload_provider_routes(payload, trial_config)
    _reject_if_not_natively_runnable(
        payload.backend,
        selection_agents(trial_config, payload.combinations),
    )

    # Validate provider_connection_id before task materialization/fan-out
    # work so known bad provider/model input returns a direct actionable error.
    provider_connection: ProviderConnection | None = None
    provider_connection_ids: set[UUID] = set()
    provider_model_checks: list[tuple[UUID | None, str | None, str | None]] = []
    if payload.combinations:
        for i, combo in enumerate(payload.combinations):
            conn_id, model_id = _effective_provider_fields(payload, combo)
            if conn_id is not None:
                provider_connection_ids.add(conn_id)
            provider_model_checks.append((conn_id, model_id, _combination_context(i, combo)))
    else:
        conn_id, model_id = _effective_provider_fields(payload, None)
        if conn_id is not None:
            provider_connection_ids.add(conn_id)
        provider_model_checks.append((conn_id, model_id, None))

    for conn_id in sorted(provider_connection_ids, key=str):
        try:
            await validate_provider_connection(
                s,
                conn_id,
                team_id=submission_team_id,
                agent_submission=True,
            )
        except HTTPException:
            SUBMISSION_REJECTS_TOTAL.labels(
                reason="provider_connection",
            ).inc()
            raise

    for conn_id, model_id, context in provider_model_checks:
        await _reject_if_known_failed_provider_model(
            s,
            provider_connection_id=conn_id,
            provider_model_id=model_id,
            context=context,
        )

    if payload.provider_connection_id is not None:
        provider_connection = (
            await s.execute(
                select(ProviderConnection).where(
                    ProviderConnection.id == payload.provider_connection_id,
                    ProviderConnection.deleted_at.is_(None),
                ),
            )
        ).scalar_one_or_none()
    provider_connections_by_id: dict[UUID, ProviderConnection] = {}
    if provider_connection_ids:
        provider_rows = (
            (
                await s.execute(
                    select(ProviderConnection).where(
                        ProviderConnection.id.in_(list(provider_connection_ids)),
                        ProviderConnection.deleted_at.is_(None),
                    ),
                )
            )
            .scalars()
            .all()
        )
        provider_connections_by_id = {row.id: row for row in provider_rows}

    if payload.combinations:
        payload = payload.model_copy(
            update={
                "combinations": _disambiguate_derived_labels(
                    payload.combinations,
                    {cid: row.display_name for cid, row in provider_connections_by_id.items()},
                ),
            },
        )
        # Labels unique within the batch (after computing the
        # derived label for those without one).
        seen_labels: set[str] = set()
        for i, combo in enumerate(payload.combinations):
            label = combo.label or _derive_combination_label(combo)
            if label in seen_labels:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(f"combinations[{i}] label {label!r} is duplicated within the batch"),
                )
            seen_labels.add(label)

    # #1380: mid-trajectory model switch validation (after provider rows load).
    multi_model_spec = None
    try:
        multi_model_spec = parse_multi_model(trial_config)
    except Exception as exc:
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail=f"trial_config.multi_model is invalid: {exc}",
        )
    if multi_model_spec is not None and multi_model_spec.enabled:
        if payload.combinations:
            for i, combo in enumerate(payload.combinations):
                conn_id, _model_id = _effective_provider_fields(payload, combo)
                conn_row = provider_connections_by_id.get(conn_id) if conn_id is not None else None
                err = validate_multi_model_for_batch(
                    trial_config=trial_config,
                    agent_name=combo.agent_name,
                    agent_model=combo.agent_model,
                    provider_connection_id=conn_id,
                    provider_connection=conn_row,
                    context=f"combinations[{i}]",
                )
                if err is not None:
                    reject_submission(
                        reason="invalid_input",
                        status_code=400,
                        detail=err,
                    )
                await _reject_if_known_failed_provider_model(
                    s,
                    provider_connection_id=conn_id,
                    provider_model_id=multi_model_spec.secondary_model.name
                    if multi_model_spec.secondary_model is not None
                    else None,
                    context=f"combinations[{i}].multi_model.secondary_model",
                )
        else:
            agent_name = trial_config.get("agent_name")
            model_raw = trial_config.get("agent_model")
            model = None
            if model_raw is not None:
                try:
                    model = ModelSpec.model_validate(model_raw)
                except Exception as exc:
                    reject_submission(
                        reason="invalid_input",
                        status_code=400,
                        detail=f"trial_config.agent_model failed to validate: {exc}",
                    )
            conn_id, _model_id = _effective_provider_fields(payload, None)
            conn_row = (
                provider_connections_by_id.get(conn_id)
                if conn_id is not None
                else provider_connection
            )
            err = validate_multi_model_for_batch(
                trial_config=trial_config,
                agent_name=agent_name if isinstance(agent_name, str) else None,
                agent_model=model,
                provider_connection_id=conn_id,
                provider_connection=conn_row,
                context="trial_config",
            )
            if err is not None:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=err,
                )
            await _reject_if_known_failed_provider_model(
                s,
                provider_connection_id=conn_id,
                provider_model_id=(
                    multi_model_spec.secondary_model.name
                    if multi_model_spec.secondary_model is not None
                    else None
                ),
                context="trial_config.multi_model.secondary_model",
            )
        # Persist a concrete switch_episode on the batch trial_config so
        # retries / fan-out reuse the same K when not overridden per trial.
        trial_config = apply_plan_mode(
            trial_config,
            mode=payload.model_switch_plan_mode,
        )

    task_result = await resolve_task_filter_with_diagnostics(
        s,
        payload.task_filter,
        team_id=submission_team_id,
    )
    task_ids = task_result.task_ids
    # Audit M2: a filter materializing to zero tasks creates a
    # batch stuck in `submitted` forever — reject up front.
    if not task_ids:
        explicit_task_ids = payload.task_filter.get("task_ids")
        if isinstance(explicit_task_ids, (list, tuple)) and all(
            isinstance(task_id, str) for task_id in explicit_task_ids
        ):
            await validate_submission_agent_task_compatibility(
                s,
                team_id=submission_team_id,
                task_ids=explicit_task_ids,
                combinations=payload.combinations,
                trial_config=trial_config,
            )
        reject_submission(
            reason="empty_filter",
            status_code=400,
            detail=(
                f"task_filter {payload.task_filter} matched zero "
                "tasks; refusing to create empty batch"
            ),
        )

    valid_task_ids, invalid_tasks = await split_valid_task_configs(
        s,
        task_ids,
    )
    if invalid_tasks:
        reject_submission(
            reason="invalid_task_config",
            status_code=400,
            detail=invalid_task_config_detail(invalid_tasks),
        )

    if payload.purpose == "evaluation":
        purpose_task_rows = (
            (
                await s.execute(
                    select(Task.task_set_id).where(Task.id.in_(list(valid_task_ids))),
                )
            )
            .scalars()
            .all()
        )
        purpose_resolved_err = validate_purpose_resolved_tasks(
            payload.purpose,
            task_set_ids=purpose_task_rows,
        )
        if purpose_resolved_err is not None:
            reject_submission(
                reason=purpose_resolved_err,
                status_code=400,
                detail=purpose_resolved_err,
            )

    # A live worker is immediately executable. A fresh compatible pool policy
    # is only cold-start authority, but it must be allowed to observe the queued
    # trials created below; otherwise min_slots=0 can never scale up.
    service_execution_runtime_profile = await admit_execution_backend(
        s,
        backend=payload.backend,
        task_ids=valid_task_ids,
        trial_config=trial_config,
        combinations=payload.combinations,
        runtime_profile_json=request.app.state.settings.service_execution_runtime_profile_json,
    )
    service_execution_runtime_profile = await freeze_task_resource_requests(
        s, backend=payload.backend, task_ids=valid_task_ids,
        trial_config=trial_config, combinations=payload.combinations,
        profile=service_execution_runtime_profile, overrides=payload.task_resource_requests,
    )

    # Reject structurally incompatible agent/task pairs instead of fanning
    # out trials that cannot satisfy the task's execution contract.
    try:
        await validate_submission_agent_task_compatibility(
            s,
            team_id=submission_team_id,
            task_ids=valid_task_ids,
            combinations=payload.combinations,
            trial_config=trial_config,
        )
    except AgentTaskIncompatibilityError as exc:
        reject_submission(
            reason="agent_task_incompat",
            status_code=exc.status_code,
            detail=str(exc.detail),
        )

    if dry_run:
        return {
            "dry_run": True,
            "accepted": True,
            "backend": payload.backend,
            "rejected_task_ids": [],
            "rejection_reasons": {},
            "tasks": await preview_execution_selection(
                s, task_ids=valid_task_ids, trial_config=trial_config, combinations=payload.combinations,
            ),
        }

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"

    # expected_trial_count = sum over combinations × tasks.
    if payload.combinations:
        combinations_jsonb = [c.model_dump(mode="json") for c in payload.combinations]
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=combinations_jsonb,
        ) + len(required_worker_pools)
    else:
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=None,
        ) + len(required_worker_pools)
        combinations_jsonb = []

    budget_policy = payload.budget_policy
    if payload.budget_usd is None:
        budget_policy = "none"
    elif budget_policy == "none":
        budget_policy = "hard"

    budget_estimate, budget_diagnostics = await _estimate_pre_run_budget_for_payload(
        s,
        payload=payload,
        provider_connections_by_id=provider_connections_by_id,
        task_count=len(valid_task_ids),
        expected_trial_count=expected,
        required_worker_pool_count=len(required_worker_pools),
        settings=request.app.state.settings,
        budget_usd=payload.budget_usd,
        budget_policy=budget_policy,
        provider_connection=provider_connection,
    )
    if budget_policy != "none":
        pre_run_cost = budget_estimate.pre_run_estimated_cost_usd
        budget_value = budget_estimate.budget_usd
        if pre_run_cost is None:
            reason = (
                "batch_budget_unpriced"
                if budget_policy == "hard"
                else "batch_budget_unpriced_confirmation_required"
            )
            if budget_policy == "hard" or not payload.budget_confirmed:
                reject_submission(
                    reason=reason,
                    status_code=400 if budget_policy == "hard" else 409,
                    detail={
                        "reason": reason,
                        "budget": budget_estimate.as_api_dict(),
                    },
                )
        elif budget_value is not None and pre_run_cost > budget_value:
            if budget_policy == "hard":
                reject_submission(
                    reason="batch_budget_exceeded",
                    status_code=400,
                    detail={
                        "reason": "batch_budget_exceeded",
                        "budget": budget_estimate.as_api_dict(),
                    },
                )
            if not payload.budget_confirmed:
                reject_submission(
                    reason="batch_budget_confirmation_required",
                    status_code=409,
                    detail={
                        "reason": "batch_budget_confirmation_required",
                        "budget": budget_estimate.as_api_dict(),
                    },
                )

    explicit_name = payload.name.strip() if payload.name else ""
    explicit_description = payload.description.strip() if payload.description else ""
    generated_identity = build_batch_identity(
        task_filter=dict(payload.task_filter),
        trial_config=trial_config,
        combinations=combinations_jsonb or payload.combinations,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        suffix=payload.name_suffix,
        provider_model_id=payload.provider_model_id,
    )
    batch_name = explicit_name or generated_identity.name
    batch_description = explicit_description or generated_identity.description

    batch_id = uuid4()
    batch_created_at = datetime.now(UTC)
    lifecycle_authority_id = await ensure_batch_lifecycle_authority(
        s,
        batch_id=batch_id,
        team_id=submission_team_id,
        created_at=batch_created_at,
    )
    b = Batch(
        id=batch_id,
        pool_origin=submission_origin(request.app.state.settings, batch_id),
        team_id=submission_team_id,
        name=batch_name,
        description=batch_description,
        purpose=payload.purpose,
        task_filter=dict(payload.task_filter),
        resolved_task_ids=list(task_result.task_ids),
        source_provenance=list(task_result.benchmark_selection_provenance),
        trial_config=trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=submitted_by_user_id,
        usage_attributed_user_id=usage_attributed_user_id,
        usage_attributed_actor=usage_attributed_actor,
        expected_trial_count=expected,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        combinations=combinations_jsonb,
        service_execution_runtime_profile=(
            service_execution_runtime_profile.model_dump(mode="json")
            if service_execution_runtime_profile is not None
            else None
        ),
        required_worker_pools=required_worker_pools,
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id,
        budget_usd=payload.budget_usd if budget_policy != "none" else None,
        budget_policy=budget_policy,
        budget_confirmed_at=(
            datetime.now(UTC) if budget_policy == "soft" and payload.budget_confirmed else None
        ),
        pre_run_estimated_cost_usd=budget_estimate.pre_run_estimated_cost_usd,
        pre_run_cost_estimate_source=budget_estimate.cost_estimate_source,
        pre_run_cost_estimate_confidence=(budget_estimate.cost_estimate_confidence),
        budget_diagnostics=budget_diagnostics,
        created_at=batch_created_at,
        lifecycle_authority_id=lifecycle_authority_id,
    )
    s.add(b)
    await s.flush()
    await s.refresh(b)

    # #672 PR-3 hot-path: if the trial_config opted the batch into
    # family-run mode (or the benchmark catalog carries defaults),
    # resolve the spec, seed per-family state, and record the resolved
    # spec on the batch. The batch_runner reads batch_family_state to
    # stamp trials.family_key at CP-submit time; the scheduler's claim
    # query then gates trial claims by family sequence position.
    override_family_run = _extract_family_run_override(trial_config)
    # Resolve the catalog default off the tasks' shared benchmark_id.
    # Family-run is rejected for multi-benchmark batches unless the
    # trial_config carries a fully-formed override — mixing benchmark
    # defaults across tasks would silently pick one arbitrary benchmark
    # and apply it to foreign tasks.
    task_rows = (
        (
            await s.execute(
                select(Task).where(Task.id.in_(list(valid_task_ids))),
            )
        )
        .scalars()
        .all()
    )
    distinct_benchmarks = {t.benchmark_id for t in task_rows if t.benchmark_id is not None}
    catalog_default = None
    if len(distinct_benchmarks) == 1:
        catalog_default = _catalog_family_run_defaults(
            next(iter(distinct_benchmarks)),
        )
    if override_family_run is not None or catalog_default is not None:
        try:
            state_backend = _build_service_state_backend(request)
            seeded = await prepare_family_run_state(
                session=s,
                batch_id=b.id,
                submission_team_id=submission_team_id,
                tasks=task_rows,
                catalog_default=catalog_default,
                override=override_family_run,
                state_backend=state_backend,
            )
        except ValueError as exc:
            reject_submission(
                reason="invalid_family_run_spec",
                status_code=400,
                detail=f"family_run spec resolution failed: {exc}",
            )
        if seeded is not None:
            b.family_run_spec = seeded.resolved_spec.model_dump(mode="json")
            await s.flush()

    usage_projection = _empty_usage_projection()
    budget_projection = project_batch_budget(b, usage_projection)
    return {
        "batch_id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "purpose": b.purpose,
        "expected_trial_count": expected,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "required_worker_pools": b.required_worker_pools,
        "state": b.state,
        "created_at": b.created_at.isoformat(),
        **budget_projection,
    }


def _reject_required_worker_pools_on_user_batch(raw_body: object) -> None:
    """#1109: coverage pools are operator-only (admin on-behalf)."""
    if not isinstance(raw_body, dict):
        return
    pools = raw_body.get("required_worker_pools")
    if pools is None:
        return
    if pools == [] or pools == ():
        return
    reject_submission(
        reason="required_worker_pools_not_allowed_on_user_batches",
        status_code=400,
        detail=(
            "required_worker_pools is operator-only (#1109). Use "
            "POST /api/v1/admin/batches/on-behalf for release coverage "
            "canaries. User evaluation batches must create exactly N "
            "trials for N tasks with no injected pool-coverage trials."
        ),
    )


@router.post(
    "/network-policy-preview",
    response_model=_NetworkPolicyPreviewResponse,
)
async def preview_batch_network_policy(
    request: Request,
    sc: SessionAndCtx,
    payload: _NetworkPolicyPreviewRequest,
) -> _NetworkPolicyPreviewResponse:
    """Preview the network dimension without creating a Batch or Trial."""
    s, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    team_id = await _resolve_submission_team_id(s, ctx, payload.team_id)
    task_result = await resolve_task_filter_with_diagnostics(
        s, payload.task_filter, team_id=team_id,
    )
    rows = (
        await s.execute(select(Task.id, Task.config).where(Task.id.in_(task_result.task_ids)))
    ).all()
    tasks: dict[str, TaskConfig] = {}
    for task_id, raw_config in rows:
        try:
            tasks[str(task_id)] = TaskConfig.model_validate(raw_config)
        except ValueError:
            continue

    profile = load_service_execution_runtime_profile(
        request.app.state.settings.service_execution_runtime_profile_json,
    )
    task_ids = [task_id for task_id in task_result.task_ids if task_id in tasks]
    agents_have_consumer = bool(payload.agent_names) and all(
        (spec := hosted_harness(agent_name)) is not None and spec.workspace
        for agent_name in payload.agent_names
    )

    defaults: dict[str, tuple[dict[str, Any], list[str]]] = {}
    for task_id in task_ids:
        policy = tasks[task_id].environment.baseline_network_policy.model_dump(mode="json")
        key = json.dumps(policy, sort_keys=True, separators=(",", ":"))
        defaults.setdefault(key, (policy, []))[1].append(task_id)

    choices: list[_NetworkPolicyPreviewChoice] = []
    for kind in ("gateway-only", "web-allowlist", "public-web"):
        incompatible = [
            task_id for task_id in task_ids
            if kind not in tasks[task_id].environment.network_policies_supported
        ]
        reasons: list[str] = []
        if incompatible:
            reasons.append("network_policy_override_not_supported")
        if kind in {"web-allowlist", "public-web"}:
            if profile is None or not profile.supports_task_web_egress:
                reasons.append("task_egress_runtime_unavailable")
                incompatible = list(task_ids)
            if not agents_have_consumer:
                reasons.append("task_network_consumer_unavailable")
                incompatible = list(task_ids)
        choices.append(_NetworkPolicyPreviewChoice(
            kind=kind,
            available=not incompatible and not reasons and bool(task_ids),
            incompatible_task_ids=list(dict.fromkeys(incompatible)),
            reasons=list(dict.fromkeys(reasons)),
        ))

    selected_reasons: dict[str, list[str]] = {}
    widens: list[str] = []
    rank = {"gateway-only": 0, "web-allowlist": 1, "public-web": 2}
    for task_id in task_ids:
        task = tasks[task_id]
        try:
            effective = resolve_effective_network_policy(
                baseline=task.environment.baseline_network_policy,
                supported=task.environment.network_policies_supported,
                override=payload.baseline_network_policy_override,
            )
        except ValueError:
            selected_reasons.setdefault(task_id, []).append(
                "network_policy_override_not_supported",
            )
            continue
        if effective.kind not in rank:
            selected_reasons.setdefault(task_id, []).append(
                "hosted_network_policy_unsupported",
            )
        if hosted_http_egress(effective) is not None:
            if profile is None or not profile.supports_task_web_egress:
                selected_reasons.setdefault(task_id, []).append(
                    "task_egress_runtime_unavailable",
                )
            if not agents_have_consumer:
                selected_reasons.setdefault(task_id, []).append(
                    "task_network_consumer_unavailable",
                )
        if (
            payload.baseline_network_policy_override is not None
            and rank.get(effective.kind, -1)
            > rank.get(task.environment.baseline_network_policy.kind, -1)
        ):
            widens.append(task_id)

    return _NetworkPolicyPreviewResponse(
        task_count=len(task_ids),
        authored_defaults=[
            _NetworkPolicyDefaultGroup(policy=policy, task_ids=ids)
            for policy, ids in defaults.values()
        ],
        choices=choices,
        selected_incompatible_task_ids=list(selected_reasons),
        selected_rejection_reasons=selected_reasons,
        widens_task_ids=widens,
    )


@router.post("/batches", status_code=201, response_model=wire.PostBatchesResponse, response_model_exclude_unset=True)
async def create_batch(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateBatch,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
        require_submitting_user(ctx)
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    try:
        raw_body = await request.json()
    except Exception:
        raw_body = None
    _reject_required_worker_pools_on_user_batch(raw_body)
    response = await _create_batch_record(
        request,
        s,
        ctx,
        payload,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
    )
    await s.commit()
    return response


@router.post("/batches/dry-run", response_model=wire.PostBatchesDryRunResponse)
async def dry_run_batch(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateBatch,
) -> dict[str, Any]:
    """Run batch admission and resolve every axis without writing anything."""
    s, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    try:
        raw_body = await request.json()
    except Exception:
        raw_body = None
    _reject_required_worker_pools_on_user_batch(raw_body)
    try:
        return await _create_batch_record(
            request,
            s,
            ctx,
            payload,
            submitted_by_user_id=ctx.user_id,
            usage_attributed_user_id=ctx.user_id,
            usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
            dry_run=True,
        )
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else None
        if detail is None or detail.get("reason") != "nebius_task_incompatible":
            raise
        return {
            "dry_run": True,
            "accepted": False,
            "backend": payload.backend,
            "rejected_task_ids": list(detail.get("task_ids") or []),
            "rejection_reasons": dict(detail.get("rejection_reasons") or {}),
            "tasks": [],
        }
    finally:
        await s.rollback()


@router.post("/admin/batches/on-behalf", status_code=201)
async def admin_create_batch_on_behalf(
    request: Request,
    sc: AdminSessionAndCtx,
    payload: _AdminCreateBatchOnBehalf,
    x_loom_admin_actor: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    s, ctx = sc
    actor = await actor_from_context(s, ctx, x_loom_admin_actor)
    represented_user = await _resolve_on_behalf_submitter(s, payload)
    response = await _create_batch_record(
        request,
        s,
        ctx,
        payload,
        submitted_by_user_id=represented_user.id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=actor,
    )
    await write_admin_audit_event(
        s,
        actor=actor,
        action="batch.submit_on_behalf",
        target_type="batch",
        target_id=response["batch_id"],
        request=request,
        metadata={
            "represented_user_id": str(represented_user.id),
            "represented_username": represented_user.username,
            "represented_team_id": str(payload.team_id),
            "expected_trial_count": response["expected_trial_count"],
            "backend": response["backend"],
        },
    )
    await s.commit()
    return response


def _derive_combination_label(combo: Combination) -> str:
    """Default label `"{agent_name}"` or
    `"{agent_name}/{provider}/{name}"` when a model is set."""
    name = combo.agent_name + (f"@{combo.agent_version}" if combo.agent_version else "")
    if combo.agent_model is None:
        return name
    return f"{name}/{combo.agent_model.provider}/{combo.agent_model.name}"


@router.get("/batches", response_model=wire.BatchList, response_model_exclude_unset=True)
async def list_batches(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query()] = None,
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

    stmt = select(Batch).order_by(
        Batch.created_at.desc(),
        Batch.id.desc(),
    )
    stmt = apply_batch_monitor_filters(
        stmt,
        target_team=target_team,
        q=q,
        benchmark_id=benchmark_id,
        agent_name=agent_name or agent,
        model_provider=model_provider,
        model_name=model_name or model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(
                    Batch.created_at == cur.submitted_at,
                    Batch.id < cur.id,
                ),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows: Sequence[Batch] = (await s.execute(stmt)).scalars().all()

    items = list(rows)
    if len(items) > limit:
        items = items[:limit]
        last = items[-1]
        next_cursor: str | None = encode_cursor(
            Cursor(submitted_at=last.created_at, id=last.id),
        )
    else:
        next_cursor = None
    usage_by_batch = await _usage_by_batch_ids(s, [r.id for r in items])
    teams_by_id: dict[UUID, Team] = {}
    users_by_id: dict[UUID, User] = {}
    if items:
        team_rows = (
            (
                await s.execute(
                    select(Team).where(Team.id.in_({r.team_id for r in items})),
                )
            )
            .scalars()
            .all()
        )
        teams_by_id = {team.id: team for team in team_rows}
        user_ids = {r.submitted_by_user_id for r in items if r.submitted_by_user_id is not None}
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
    return {
        "items": [
            _serialize(
                r,
                usage=usage_by_batch.get(r.id),
                owner_team=teams_by_id.get(r.team_id),
                submitted_by_user=(
                    users_by_id.get(r.submitted_by_user_id)
                    if r.submitted_by_user_id is not None
                    else None
                ),
            )
            for r in items
        ],
        "next_cursor": next_cursor,
    }


def _rollup_from_result(result: dict[str, Any] | None) -> float | None:
    """Pull reward out of a Trial.result JSONB. Same logic as
    routes/trials.py — kept inline here so the rollup query can flow
    naturally."""
    if not result:
        return None
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        return float(reward) if reward is not None else None
    except (TypeError, ValueError):
        return None


def _empty_usage_projection() -> dict[str, Any]:
    return empty_usage_projection()


async def _usage_totals_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[str, Any]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return _empty_usage_projection()
    row = (
        await session.execute(
            select(
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
            ).where(LlmCall.trial_id.in_(trial_ids)),
        )
    ).one()
    return summarize_usage_counts(
        llm_calls_count=int(row.llm_calls_count or 0),
        total_prompt_tokens=int(row.total_prompt_tokens or 0),
        total_completion_tokens=int(row.total_completion_tokens or 0),
        total_cost_usd=row.total_cost_usd,
        priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
        token_only_llm_calls_count=int(row.token_only_llm_calls_count or 0),
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


async def _llm_calls_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> list[LlmCall]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return []
    return list(
        (
            await session.execute(
                select(LlmCall)
                .where(LlmCall.trial_id.in_(trial_ids))
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )


async def _worker_pool_names_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, str]:
    worker_ids = sorted(
        {trial.worker_id for trial in trials if trial.worker_id is not None},
        key=str,
    )
    if not worker_ids:
        return {}
    result = await session.execute(
        select(Worker.id, Worker.pool_name).where(Worker.id.in_(worker_ids)),
    )
    return {worker_id: pool_name for worker_id, pool_name in result.all()}


async def _llm_call_counts_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, int]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return {}
    rows = (
        await session.execute(
            select(LlmCall.trial_id, func.count(LlmCall.id))
            .where(LlmCall.trial_id.in_(trial_ids))
            .group_by(LlmCall.trial_id),
        )
    ).all()
    return {trial_id: int(count or 0) for trial_id, count in rows if trial_id is not None}


async def _trial_projections_for_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> list[_BatchTrialProjection]:
    if not batch_ids:
        return []
    rows = (
        await session.execute(
            select(
                Trial.id,
                Trial.batch_id,
                Trial.team_id,
                Trial.task_id,
                Trial.config,
                Trial.state,
                Trial.failure_reason,
                Trial.failure_message,
                Trial.result,
                Trial.claimed_at,
                Trial.pre_start_heartbeat_at,
                Trial.started_at,
                Trial.finished_at,
                Trial.sample_idx,
                Trial.combination_idx,
                Trial.provider_connection_id,
                Trial.provider_model_id,
                Trial.worker_id,
            ).where(Trial.batch_id.in_(list(batch_ids))),
        )
    ).all()
    return [
        _BatchTrialProjection(
            id=row.id,
            batch_id=row.batch_id,
            team_id=row.team_id,
            task_id=row.task_id,
            config=row.config,
            state=row.state,
            failure_reason=row.failure_reason,
            failure_message=row.failure_message,
            result=row.result,
            claimed_at=row.claimed_at,
            pre_start_heartbeat_at=row.pre_start_heartbeat_at,
            started_at=trial_started_at(row.started_at, row.result),
            finished_at=row.finished_at,
            sample_idx=row.sample_idx,
            combination_idx=row.combination_idx,
            provider_connection_id=row.provider_connection_id,
            provider_model_id=row.provider_model_id,
            worker_id=row.worker_id,
        )
        for row in rows
    ]


async def _trial_projections_for_batch(
    session: Any,
    batch_id: UUID,
) -> list[_BatchTrialProjection]:
    return await _trial_projections_for_batch_ids(session, [batch_id])



def _empty_trial_summary() -> dict[str, int]:
    return {
        k: 0
        for k in (
            "queued",
            "protected-pending",
            "claimed",
            "running",
            "materializing",
            "succeeded",
            "failed",
            "cancelled",
        )
    }


async def _batch_service_execution_summary(
    session: Any,
    trial_ids: Sequence[UUID],
) -> dict[str, Any] | None:
    if not trial_ids:
        return None
    lifecycle_expr = service_execution_lifecycle_case()
    rows = (
        await session.execute(
            select(
                lifecycle_expr.label("lifecycle_stage"),
                ServiceExecutionLease.output_commit_state,
                ServiceExecutionLease.materialization_state,
                ServiceExecutionLease.observed_state,
                func.count(),
            )
            .select_from(ServiceExecutionLease)
            .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
            .where(
                ServiceExecutionLease.trial_id.in_(trial_ids),
                ServiceExecutionLease.execution_role == "attempt",
                ServiceExecutionLease.id == latest_execution_id(),
            )
            .group_by(
                lifecycle_expr,
                ServiceExecutionLease.output_commit_state,
                ServiceExecutionLease.materialization_state,
                ServiceExecutionLease.observed_state,
            )
        )
    ).all()
    if not rows:
        return None
    lifecycle = {state: 0 for state in SERVICE_EXECUTION_LIFECYCLE_STAGES}
    output_commit: dict[str, int] = {}
    materialization: dict[str, int] = {}
    execution: dict[str, int] = {}
    lease_count = 0
    for lifecycle_stage, output_state, materialization_state, observed_state, count in rows:
        value = int(count)
        lease_count += value
        lifecycle[str(lifecycle_stage)] = lifecycle.get(str(lifecycle_stage), 0) + value
        output_commit[str(output_state)] = output_commit.get(str(output_state), 0) + value
        execution[str(observed_state)] = execution.get(str(observed_state), 0) + value
        materialization[str(materialization_state)] = (
            materialization.get(str(materialization_state), 0) + value
        )
    return {
        "lease_count": lease_count,
        "lifecycle_stages": lifecycle,
        "output_commit_states": output_commit,
        "materialization_states": materialization,
        "execution_states": execution,
        "canonical_ready_count": materialization.get("committed", 0),
    }


def _summary_from_trials(trials: Sequence[Any]) -> dict[str, int]:
    summary = _empty_trial_summary()
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _rollup_from_trials(trials: Sequence[Any]) -> float | None:
    reward_sum = 0.0
    reward_n = 0
    for trial in trials:
        if str(trial.state) not in {"succeeded", "failed"}:
            continue
        rew = _rollup_from_result(trial.result)
        if rew is not None:
            reward_sum += rew
            reward_n += 1
    return (reward_sum / reward_n) if reward_n > 0 else None


async def _benchmark_summary_from_trials(
    session: Any,
    trials: Sequence[Any],
) -> list[dict[str, Any]]:
    task_ids = sorted({trial.task_id for trial in trials})
    if not task_ids:
        return []

    rows = (
        await session.execute(
            select(
                Task.id,
                Task.benchmark_id,
                Benchmark.display_name,
            )
            .outerjoin(Benchmark, Benchmark.id == Task.benchmark_id)
            .where(Task.id.in_(task_ids)),
        )
    ).all()
    task_lookup = {
        str(row.id): {
            "benchmark_id": row.benchmark_id,
            "display_name": row.display_name,
        }
        for row in rows
    }

    grouped: dict[str, list[Any]] = defaultdict(list)
    labels: dict[str, tuple[str | None, str]] = {}
    for trial in trials:
        meta = task_lookup.get(str(trial.task_id), {})
        benchmark_id = meta.get("benchmark_id")
        group_key = str(benchmark_id) if benchmark_id else "__unbenchmarked__"
        display_name = (
            str(meta.get("display_name"))
            if meta.get("display_name")
            else (str(benchmark_id) if benchmark_id else "Unbenchmarked tasks")
        )
        grouped[group_key].append(trial)
        labels[group_key] = (
            str(benchmark_id) if benchmark_id else None,
            display_name,
        )

    summaries: list[dict[str, Any]] = []
    for group_key, group_trials in grouped.items():
        summary = _summary_from_trials(group_trials)
        benchmark_id, display_name = labels[group_key]
        summaries.append(
            {
                "benchmark_id": benchmark_id,
                "display_name": display_name,
                "metric_name": "score",
                "expected_trial_count": len(group_trials),
                "completed_trial_count": sum(
                    summary.get(state, 0) for state in ("succeeded", "failed", "cancelled")
                ),
                "platform_failed_count": summary.get("failed", 0),
                "trial_summary": summary,
                "aggregate_reward": _rollup_from_trials(group_trials),
            }
        )

    return sorted(
        summaries,
        key=lambda row: (
            str(row["display_name"]).casefold(),
            str(row["benchmark_id"] or ""),
        ),
    )


def _trial_key(trial: Any) -> tuple[str, int, int]:
    return (trial.task_id, int(trial.sample_idx), int(trial.combination_idx))


def _is_rerunnable_failure(trial: Any) -> bool:
    return is_auto_safe_rerun(trial)


def _effective_trials(
    original_trials: Sequence[Any],
    rerun_trials: Sequence[Any],
) -> list[Any]:
    original_by_key = {_trial_key(trial): trial for trial in original_trials}
    effective = dict(original_by_key)
    for trial in rerun_trials:
        if str(trial.state) != "succeeded":
            continue
        key = _trial_key(trial)
        original = original_by_key.get(key)
        if original is None or not is_replaceable_by_successful_supplemental(original):
            continue
        effective[key] = trial
    return list(effective.values())


def _result_status_from_trials(trials: Sequence[Any]) -> str | None:
    if not trials:
        return None
    states = [str(trial.state) for trial in trials]
    if any(
        state in {"queued", "protected-pending", "claimed", "running"}
        for state in states
    ):
        return None
    succeeded = sum(1 for state in states if state == "succeeded")
    failed = len(states) - succeeded
    if failed == 0 and succeeded > 0:
        return "succeeded"
    if succeeded == 0:
        return "all_failed"
    return "partial_failed"


@router.get("/batches/{batch_id}", response_model=wire.BatchDetail, response_model_exclude_unset=True)
async def get_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    include_debug: Annotated[
        bool,
        Query(
            description=(
                "Include heavyweight debug_evidence and diagnosis payloads. "
                "Defaults to false so large batch detail reads stay bounded; "
                "use /debug or /diagnosis for targeted diagnostics."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    owner_team = (
        await s.execute(
            select(Team).where(Team.id == b.team_id),
        )
    ).scalar_one_or_none()
    submitted_by_user = None
    if b.submitted_by_user_id is not None:
        submitted_by_user = (
            await s.execute(
                select(User).where(User.id == b.submitted_by_user_id),
            )
        ).scalar_one_or_none()

    original_trials: list[Any] = list(await _trial_projections_for_batch(s, batch_id))
    summary = _summary_from_trials(original_trials)
    avg_reward = _rollup_from_trials(original_trials)
    usage = await _usage_totals_for_trials(s, original_trials)
    price_snapshots = await price_snapshots_for_trials(
        s,
        {trial.id for trial in original_trials},
    )
    llm_call_counts = await _llm_call_counts_for_trials(s, original_trials)
    llm_evidence = summarize_llm_evidence_for_trials(
        original_trials,
        llm_call_counts=llm_call_counts,
    )
    benchmark_summary = await _benchmark_summary_from_trials(
        s,
        original_trials,
    )
    combination_summary = await combination_summary_for_batch(
        s,
        combinations=b.combinations,
        trials=original_trials,
        expected_trial_count=b.expected_trial_count,
        required_worker_pool_count=len(b.required_worker_pools or []),
        fanout_errors=b.fanout_errors,
    )

    rerun_batches = (
        (
            await s.execute(
                select(Batch)
                .where(Batch.rerun_of_batch_id == batch_id)
                .order_by(Batch.created_at.asc(), Batch.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    rerun_batch_ids = [child.id for child in rerun_batches]
    rerun_trials: list[Any] = []
    if rerun_batch_ids:
        rerun_trials = await _trial_projections_for_batch_ids(
            s,
            rerun_batch_ids,
        )
    rerun_plan = build_supplemental_rerun_plan(
        b,
        original_trials,
        supplemental_trials=rerun_trials,
    )
    effective_trials = _effective_trials(original_trials, rerun_trials)
    effective_summary = _summary_from_trials(effective_trials)
    effective_reward = _rollup_from_trials(effective_trials)
    effective_usage = await _usage_totals_for_trials(s, effective_trials)
    effective_combination_summary = await combination_summary_for_batch(
        s,
        combinations=b.combinations,
        trials=effective_trials,
        expected_trial_count=b.expected_trial_count,
        required_worker_pool_count=len(b.required_worker_pools or []),
        fanout_errors=b.fanout_errors,
    )
    effective_llm_call_counts = await _llm_call_counts_for_trials(
        s,
        effective_trials,
    )
    effective_llm_evidence = summarize_llm_evidence_for_trials(
        effective_trials,
        llm_call_counts=effective_llm_call_counts,
    )
    effective_price_snapshots = await price_snapshots_for_trials(
        s,
        {trial.id for trial in effective_trials},
    )
    service_execution_summary = (
        await _batch_service_execution_summary(
            s,
            [trial.id for trial in original_trials],
        )
        if b.backend == NEBIUS_BACKEND
        else None
    )
    network_policy = await _batch_network_policy_evidence(
        s,
        task_ids=(
            b.resolved_task_ids
            or [trial.task_id for trial in original_trials]
        ),
        trial_config=b.trial_config,
    )
    rerunnable_failed_count = sum(1 for trial in original_trials if _is_rerunnable_failure(trial))
    extra = {
        "service_execution_summary": service_execution_summary,
        "network_policy": network_policy,
        "progress": await progress_summary(s, select(Trial.id).where(Trial.batch_id == b.id)),
        "rerun_batches": [
            {
                "id": str(child.id),
                "name": child.name,
                "state": child.state,
                "result_status": child.result_status,
                "expected_trial_count": child.expected_trial_count,
                "created_at": child.created_at.isoformat(),
                "finished_at": (child.finished_at.isoformat() if child.finished_at else None),
            }
            for child in rerun_batches
        ],
        "rerunnable_failed_count": rerunnable_failed_count,
        "rerun_plan": rerun_plan,
        "final_trial_selection": rerun_plan["final_trial_selection"],
        "effective_trial_summary": effective_summary,
        "effective_result_status": _result_status_from_trials(effective_trials),
        "effective_aggregate_reward": effective_reward,
        "effective_total_prompt_tokens": effective_usage["total_prompt_tokens"],
        "effective_total_completion_tokens": effective_usage["total_completion_tokens"],
        "effective_total_tokens": effective_usage["total_tokens"],
        "effective_llm_calls_count": effective_usage["llm_calls_count"],
        "effective_estimated_cost_usd": effective_usage["estimated_cost_usd"],
        "effective_cost_currency": effective_usage["cost_currency"],
        "effective_cost_status": effective_usage["cost_status"],
        "effective_pricing_modes": effective_usage["pricing_modes"],
        "effective_priced_llm_calls_count": effective_usage["priced_llm_calls_count"],
        "effective_token_only_llm_calls_count": effective_usage["token_only_llm_calls_count"],
        "effective_price_unknown_llm_calls_count": effective_usage["price_unknown_llm_calls_count"],
        "effective_failed_upstream_llm_calls_count": effective_usage[
            "failed_upstream_llm_calls_count"
        ],
        "effective_partial_usage_llm_calls_count": effective_usage["partial_usage_llm_calls_count"],
        "effective_missing_usage_llm_calls_count": effective_usage["missing_usage_llm_calls_count"],
        "effective_usage_reporting_status": effective_usage["usage_reporting_status"],
        "effective_usage_estimate_confidence": effective_usage["usage_estimate_confidence"],
        "no_call_trial_count": llm_evidence["no_call_trial_count"],
        "no_call_reason_counts": llm_evidence["no_call_reason_counts"],
        "llm_evidence_status": llm_evidence["llm_evidence_status"],
        "model_backed_terminal_trial_count": llm_evidence["model_backed_terminal_trial_count"],
        "effective_no_call_trial_count": effective_llm_evidence["no_call_trial_count"],
        "effective_no_call_reason_counts": effective_llm_evidence["no_call_reason_counts"],
        "effective_llm_evidence_status": effective_llm_evidence["llm_evidence_status"],
        "effective_model_backed_terminal_trial_count": effective_llm_evidence[
            "model_backed_terminal_trial_count"
        ],
        "price_snapshots": price_snapshots,
        "effective_price_snapshots": effective_price_snapshots,
        "benchmark_summary": benchmark_summary,
        "combination_summary": combination_summary,
        "effective_combination_summary": effective_combination_summary,
    }
    if include_debug:
        llm_calls = await _llm_calls_for_trials(s, original_trials)
        worker_pool_names = await _worker_pool_names_for_trials(s, original_trials)
        stale_running_decisions = await batch_stale_running_decisions(
            s,
            original_trials,
            llm_calls=llm_calls,
            settings=request.app.state.settings,
        )
        debug_evidence = build_batch_debug_evidence(
            b,
            trials=original_trials,
            llm_calls=llm_calls,
            worker_pool_names_by_id=worker_pool_names,
            stale_running_decisions_by_trial_id=stale_running_decisions,
        )
        extra["debug_evidence"] = debug_evidence
        extra["diagnosis"] = build_batch_diagnosis(
            debug_evidence,
            trial_failures=trial_failure_records(original_trials),
        )

    return _serialize(
        b,
        summary=summary,
        aggregate_reward=avg_reward,
        usage=usage,
        owner_team=owner_team,
        submitted_by_user=submitted_by_user,
        extra=extra,
    )


@router.get("/batches/{batch_id}/resource-usage")
async def get_batch_resource_usage(
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    batch = (await s.execute(select(Batch).where(Batch.id == batch_id))).scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    require_team_or_admin(ctx, batch.team_id)
    rows = list(
        (
            await s.execute(
                select(TrialResourceUsage)
                .join(Trial, Trial.id == TrialResourceUsage.trial_id)
                .where(Trial.batch_id == batch_id)
                .order_by(
                    TrialResourceUsage.trial_id.asc(),
                    TrialResourceUsage.attempt_count.asc(),
                    TrialResourceUsage.container_role.asc(),
                    TrialResourceUsage.role_name.asc(),
                )
            )
        )
        .scalars()
        .all()
    )
    response = resource_usage_response(rows)
    response["batch_id"] = str(batch_id)
    response["termination_failures"] = await execution_failure_groups(s, batch_id=batch_id)
    response["trials_with_telemetry"] = len({row.trial_id for row in rows})
    return response


@router.get("/batches/{batch_id}/debug", response_model=wire.DebugEvidence, response_model_exclude_unset=True)
async def get_batch_debug(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    trials = await _trial_projections_for_batch(s, batch_id)
    llm_calls = await _llm_calls_for_trials(s, trials)
    worker_pool_names = await _worker_pool_names_for_trials(s, trials)
    stale_running_decisions = await batch_stale_running_decisions(
        s,
        trials,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    return build_batch_debug_evidence(
        b,
        trials=trials,
        llm_calls=llm_calls,
        worker_pool_names_by_id=worker_pool_names,
        stale_running_decisions_by_trial_id=stale_running_decisions,
    )


@router.get("/batches/{batch_id}/diagnosis", response_model=wire.DiagnosisReport, response_model_exclude_unset=True)
async def get_batch_diagnosis(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    trials = await _trial_projections_for_batch(s, batch_id)
    llm_calls = await _llm_calls_for_trials(s, trials)
    worker_pool_names = await _worker_pool_names_for_trials(s, trials)
    stale_running_decisions = await batch_stale_running_decisions(
        s,
        trials,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    debug_evidence = build_batch_debug_evidence(
        b,
        trials=trials,
        llm_calls=llm_calls,
        worker_pool_names_by_id=worker_pool_names,
        stale_running_decisions_by_trial_id=stale_running_decisions,
    )
    return build_batch_diagnosis(
        debug_evidence,
        trial_failures=trial_failure_records(trials),
    )


@router.get("/batches/{batch_id}/rerun-plan", response_model=wire.RerunPlan, response_model_exclude_unset=True)
async def get_batch_rerun_plan(
    sc: SessionAndCtx,
    batch_id: UUID,
    task_id: Annotated[
        list[str] | None,
        Query(
            description=(
                "Optional task id filter. Repeat task_id to generate a plan "
                "for an explicit supplemental task list."
            )
        ),
    ] = None,
    include_operator_approval: Annotated[
        bool,
        Query(
            description=(
                "Include operator-approval rows in supplemental_task_ids. "
                "Defaults to false so only auto-safe rows are emitted."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    original_trials = await _trial_projections_for_batch(s, batch_id)
    child_batch_ids = (
        (
            await s.execute(
                select(Batch.id).where(Batch.rerun_of_batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )
    supplemental_trials: list[Any] = []
    if child_batch_ids:
        supplemental_trials = await _trial_projections_for_batch_ids(
            s,
            child_batch_ids,
        )
    return build_supplemental_rerun_plan(
        b,
        original_trials,
        task_ids=task_id,
        supplemental_trials=supplemental_trials,
        include_operator_approval=include_operator_approval,
    )


@router.post("/batches/{batch_id}/rerun-failed", status_code=201, response_model=wire.PostBatchesIdRerunFailedResponse, response_model_exclude_unset=True)
async def rerun_failed_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _RerunFailedBatch | None = None,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
        require_submitting_user(ctx)
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    if b.backend != NEBIUS_BACKEND and not local_execution_enabled():
        # Historical batch: readable, but a rerun would inherit its backend.
        reject_submission(
            reason="unsupported_hosted_backend",
            status_code=400,
            detail={
                "reason": "unsupported_hosted_backend",
                "backend": b.backend,
                "message": (
                    f"Batch {batch_id} ran on backend {b.backend!r}, which hosted "
                    "execution no longer supports. Submit a new batch instead of "
                    "rerunning failed cases."
                ),
            },
        )
    await _reject_if_team_paused(s, b.team_id)
    _reject_if_k8s_worker_unavailable(request, b.required_worker_pools or [])

    original_trials = await _trial_projections_for_batch(s, batch_id)
    child_batch_ids = (
        (
            await s.execute(
                select(Batch.id).where(Batch.rerun_of_batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )
    supplemental_trials: list[Any] = []
    if child_batch_ids:
        supplemental_trials = await _trial_projections_for_batch_ids(
            s,
            child_batch_ids,
        )
    request_payload = payload or _RerunFailedBatch()
    plan = build_supplemental_rerun_plan(
        b,
        original_trials,
        task_ids=(request_payload.task_ids or None),
        supplemental_trials=supplemental_trials,
        include_operator_approval=request_payload.include_operator_approval,
    )
    selected_targets = list(plan["auto_safe"])
    if request_payload.include_operator_approval:
        selected_targets.extend(plan["operator_approval"])
    if not selected_targets:
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="batch has no rerunnable failed trials",
        )

    targets = [
        {
            "task_id": target["task_id"],
            "sample_idx": int(target["sample_idx"]),
            "combination_idx": int(target["combination_idx"]),
            "original_trial_id": target["original_trial_id"],
            "failure_reason": target["failure_reason"],
        }
        for target in selected_targets
    ]
    task_ids = sorted({target["task_id"] for target in selected_targets})
    rerun_task_result = await resolve_task_filter_with_diagnostics(
        s,
        {"subset_kind": "explicit", "task_ids": task_ids},
        team_id=b.team_id,
        require_runnable=True,
    )
    if set(rerun_task_result.task_ids) != set(task_ids):
        await validate_submission_agent_task_compatibility(
            s,
            team_id=b.team_id,
            task_ids=task_ids,
            combinations=b.combinations or [],
            trial_config=b.trial_config,
        )
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="rerun target tasks are missing or no longer runnable",
        )
    valid_rerun_task_ids, invalid_rerun_tasks = await split_valid_task_configs(
        s,
        rerun_task_result.task_ids,
    )
    if invalid_rerun_tasks:
        reject_submission(
            reason="invalid_task_config",
            status_code=400,
            detail=invalid_task_config_detail(invalid_rerun_tasks),
        )
    # Serialize all selected inputs with expiry, including legacy selections
    # without agent metadata. Exact failed agent/task pairs are checked below.
    await validate_submission_agent_task_compatibility(
        s, team_id=b.team_id, task_ids=valid_rerun_task_ids, trial_config={},
    )
    rerun_trial_config = dict(b.trial_config)
    combinations = [dict(item) for item in b.combinations or []]
    _reject_if_not_natively_runnable(
        b.backend,
        selection_agents(
            rerun_trial_config,
            combinations,
            {int(t["combination_idx"]) for t in targets},
        ),
        action="cannot rerun ",
    )
    runtime_profile_json = json.dumps(b.service_execution_runtime_profile or {})
    if request_payload.use_current_runtime:
        selections = combinations or [rerun_trial_config]
        # Re-resolving the deployed controller is the pinned-versions feature.
        if b.backend != NEBIUS_BACKEND or any(
            (spec := hosted_harness(item.get("agent_name"))) is None
            or not spec.supports("pinned_versions")
            for item in selections
        ):
            reject_submission(
                reason="invalid_input", status_code=400,
                detail="current runtime rerun supports only native Nebius "
                + ", ".join(harnesses_supporting("pinned_versions")),
            )
        # A missing explicit version selects the deployment-owned controller,
        # exactly as an ordinary new submission does. Never mutate the parent.
        rerun_trial_config.pop("agent_version", None)
        for item in combinations:
            item.pop("agent_version", None)
        runtime_profile_json = request.app.state.settings.service_execution_runtime_profile_json
    service_execution_runtime_profile = await admit_execution_backend(
        s,
        backend=b.backend,
        task_ids=valid_rerun_task_ids,
        trial_config=rerun_trial_config,
        combinations=combinations,
        runtime_profile_json=runtime_profile_json,
        resolve_versions=request_payload.use_current_runtime,
        automatic_only=request_payload.use_current_runtime,
    )
    inherited_requests = (b.service_execution_runtime_profile or {}).get("task_resource_requests", {})
    service_execution_runtime_profile = await freeze_task_resource_requests(
        s, backend=b.backend, task_ids=valid_rerun_task_ids,
        trial_config=rerun_trial_config, combinations=combinations,
        profile=service_execution_runtime_profile,
        overrides={
            task_id: TaskExecutionResourceRequestsV1.model_validate(value)
            for task_id, value in inherited_requests.items() if task_id in valid_rerun_task_ids
        },
    )
    agent_task_pairs: list[tuple[str, str]] = []
    for target in targets:
        task_id = str(target["task_id"])
        combination_idx = int(target["combination_idx"])
        if combination_idx < 0 or (combinations and combination_idx >= len(combinations)):
            reject_submission(
                reason="invalid_input",
                status_code=400,
                detail=f"rerun target combination_idx {combination_idx} is invalid",
            )
        if combinations:
            agent_name = combinations[combination_idx].get("agent_name")
            if not isinstance(agent_name, str) or not agent_name:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"combinations[{combination_idx}].agent_name must be a non-empty string"
                    ),
                )
        else:
            if combination_idx != 0:
                reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"rerun target combination_idx {combination_idx} is invalid",
                )
            raw_agent_name = b.trial_config.get("agent_name")
            if not isinstance(raw_agent_name, str) or not raw_agent_name:
                continue
            agent_name = raw_agent_name
        agent_task_pairs.append((task_id, agent_name))
    # #2054: a rerun submits new trials, so it follows current submission
    # policy. The parent batch and its accepted trials are left untouched.
    # Only the re-dispatched combinations are resolved; the rerun persists
    # their resolved routes so what is stored matches what runs.
    try:
        rerun_routes = derive_stored_routes(
            trial_config=rerun_trial_config,
            combinations=combinations,
            source_connection_id=b.provider_connection_id,
            source_model_id=b.provider_model_id,
            indices={int(t["combination_idx"]) for t in targets},
        )
    except ProviderRouteError as exc:
        reject_submission(
            reason="invalid_input",
            status_code=400,
            detail=f"cannot rerun: {exc}",
        )
    combinations = rerun_routes.combinations
    for conn_id in sorted(rerun_routes.connection_ids, key=str):
        try:
            await validate_provider_connection(
                s,
                conn_id,
                team_id=b.team_id,
                agent_submission=True,
            )
        except HTTPException:
            SUBMISSION_REJECTS_TOTAL.labels(reason="provider_connection").inc()
            raise
    await validate_submission_agent_task_pairs(
        s,
        team_id=b.team_id,
        agent_task_pairs=agent_task_pairs,
    )
    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    rerun_id = uuid4()
    rerun_created_at = datetime.now(UTC)
    rerun_lifecycle_authority_id = await ensure_batch_lifecycle_authority(
        s,
        batch_id=rerun_id,
        team_id=b.team_id,
        created_at=rerun_created_at,
    )
    rerun = Batch(
        id=rerun_id,
        pool_origin=submission_origin(request.app.state.settings, rerun_id),
        team_id=b.team_id,
        name=f"{b.name} failed-case rerun",
        description=(f"Reruns {len(targets)} transient failed case(s) from batch {b.id}."),
        purpose=b.purpose,
        task_filter={"subset_kind": "explicit", "task_ids": task_ids},
        resolved_task_ids=list(rerun_task_result.task_ids),
        trial_config=apply_plan_mode(
            rerun_trial_config,
            mode=request_payload.model_switch_plan_mode,
        ),
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
        expected_trial_count=len(targets),
        n_per_task=1,
        backend=b.backend,
        combinations=combinations,
        service_execution_runtime_profile=(
            service_execution_runtime_profile.model_dump(mode="json")
            if service_execution_runtime_profile is not None
            else None
        ),
        # Combinations carry their own resolved routes, so the parent's
        # batch-level defaults stay as they were; a single selection's route
        # lives in the batch-level fields.
        provider_connection_id=(
            b.provider_connection_id if combinations else rerun_routes.batch_connection_id
        ),
        provider_model_id=b.provider_model_id if combinations else rerun_routes.batch_model_id,
        rerun_of_batch_id=b.id,
        rerun_targets=targets,
        source_provenance=[
            {
                "kind": "supplemental_rerun",
                "source_batch_id": str(b.id),
                **({"use_current_runtime": True} if request_payload.use_current_runtime else {}),
            },
            *rerun_task_result.benchmark_selection_provenance,
        ],
        created_at=rerun_created_at,
        lifecycle_authority_id=rerun_lifecycle_authority_id,
    )
    s.add(rerun)
    await s.commit()
    await s.refresh(rerun)
    return {
        "batch_id": str(rerun.id),
        "rerun_of_batch_id": str(b.id),
        "expected_trial_count": rerun.expected_trial_count,
        "state": rerun.state,
        "created_at": rerun.created_at.isoformat(),
        "rerun_target_count": len(targets),
        "rerun_plan": plan,
    }


@router.post("/batches/{batch_id}/cancel", response_model=wire.PostBatchesIdCancelResponse, response_model_exclude_unset=True)
async def cancel_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> Any:
    s, ctx = sc
    require_scope(ctx, "submit")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    now = datetime.now(UTC)
    active_trial_ids = (
        await s.execute(
            select(Trial.id).where(
                Trial.batch_id == batch_id,
                Trial.state.in_(["queued", "protected-pending", "claimed", "running"]),
            )
        )
    ).scalars().all()
    # The Control Plane owns the trial cancellation transition for every
    # backend. In protected staging this is also the only path that can move a
    # live claim to cancel-pending without releasing its concurrency lease.
    if ctx.auth_kind == "session":
        # CP session revalidation must not wait on this request's auth-row lock.
        await s.commit()
    for trial_id in active_trial_ids:
        response = await forward(
            request.app.state.http_client,
            method="POST",
            path=f"/trials/{trial_id}/cancel",
            authorization=authorization,
            cancellation_request=request,
        )
        if response.status_code not in {200, 409}:
            return propagate(response)

    await s.execute(
        update(Batch).where(Batch.id == batch_id).values(state="cancelled", finished_at=now),
    )
    await s.commit()
    return {"batch_id": str(batch_id), "state": "cancelled"}
