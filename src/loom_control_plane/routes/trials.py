"""Trial submission + fetch endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.application_session import ApplicationSessionAudienceV1
from loom.auth import AuthContext, is_admin, verify_bearer_token
from loom.benchmark_profiles import reject_non_runnable_benchmark_profiles
from loom.data_lifecycle_registry import ensure_trial_lifecycle_authority
from loom.db.schema import (
    Batch,
    LlmCall,
    ProviderConnection,
    Team,
    TeamQuota,
    TrialTaskImageMaterialization,
)
from loom.db.schema import Task as TaskRow
from loom.db.schema import Trial as TrialRow
from loom.db.task_set_visibility import visible_tasks
from loom.execution_architecture import execution_cpu_arch
from loom.llm_call_ledger import serialize_llm_call
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.nebius_pool_priority import PoolWorkOriginV1
from loom.nebius_submission import qualify_trial_submission
from loom.service_execution_backend import (
    NEBIUS_BACKEND,
    NEBIUS_LOGICAL_POOL_ID,
    local_execution_enabled,
)
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    execution_selection_rejections,
)
from loom.submission_identity import require_submitting_user
from loom.task_image_materialization import (
    TaskImageSnapshotConflictError,
    ensure_task_image_materializations,
)
from loom.verifier_runtime import apply_legacy_verifier_default
from loom_control_plane.request_auth import RequestPrincipal
from loom_control_plane.scheduler.requires_caps import derive_requires_caps
from loom_control_plane.trial_cancellation import cancel_trial_under_authority
from loom_service.auth_guards import require_human_or_admin, require_scope
from loom_service.session_auth import (
    verify_csrf,
    verify_session_cookie,
)
from loom_service.submission_compat import validate_submission_agent_task_compatibility

router = APIRouter()


def _resolve_required_worker_pool_for_backend(
    *,
    batch_backend: str,
    requested_pool: str | None,
    task_service_pool: str | None,
    automatic_compatible: bool = False,
) -> str | None:
    """Map an explicit user backend to exactly one execution mechanism."""

    if batch_backend != NEBIUS_BACKEND:
        if not local_execution_enabled():
            raise HTTPException(status_code=400, detail="Hosted execution supports Nebius only")
        if requested_pool == NEBIUS_LOGICAL_POOL_ID:
            raise HTTPException(
                status_code=400,
                detail=(
                    "required_worker_pool 'nebius-cpu' requires "
                    "backend 'nebius'"
                ),
            )
        return requested_pool
    if task_service_pool != NEBIUS_LOGICAL_POOL_ID and not automatic_compatible:
        raise HTTPException(
            status_code=400,
            detail=(
                "backend 'nebius' requires a task revision bound to the "
                "nebius-cpu service-execution pool"
            ),
        )
    resolved_pool = task_service_pool or NEBIUS_LOGICAL_POOL_ID
    if requested_pool is not None and requested_pool != resolved_pool:
        raise HTTPException(
            status_code=400,
            detail=(
                "required_worker_pool conflicts with the task's immutable "
                "service-execution binding"
            ),
        )
    return resolved_pool


async def _ensure_trial_task_image_links(
    session: AsyncSession,
    *,
    trial_id: UUID,
    task_row: TaskRow,
) -> None:
    # The prerequisite set is the submitted revision. Serialize first publication
    # and replays on the Trial so a later TaskSet rebuild cannot append a newer
    # revision to an existing Trial's frozen image links.
    await session.execute(
        select(TrialRow.id).where(TrialRow.id == trial_id).with_for_update()
    )
    if await session.scalar(
        select(TrialTaskImageMaterialization.materialization_id)
        .where(TrialTaskImageMaterialization.trial_id == trial_id)
        .limit(1)
    ) is not None:
        return
    try:
        materializations = await ensure_task_image_materializations(
            session,
            task_row=task_row,
        )
    except TaskImageSnapshotConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "task_image_snapshot_conflict",
                "task_id": task_row.id,
                "message": str(exc),
            },
        ) from exc
    if not materializations:
        return
    await session.execute(
        pg_insert(TrialTaskImageMaterialization)
        .values(
            [
                {
                    "trial_id": trial_id,
                    "materialization_id": materialization.id,
                }
                for materialization in materializations
            ]
        )
        .on_conflict_do_nothing(index_elements=["trial_id", "materialization_id"])
    )


def _required_worker_pool(payload: dict[str, Any]) -> str | None:
    raw = payload.get("required_worker_pool")
    if raw is None:
        return None
    pool = str(raw).strip()
    if not pool:
        raise HTTPException(
            status_code=400,
            detail="required_worker_pool must be a non-empty string",
        )
    if len(pool) > 80 or any(ch.isspace() for ch in pool):
        raise HTTPException(
            status_code=400,
            detail=("required_worker_pool must be 1-80 characters and contain no whitespace"),
        )
    return pool




async def _browser_submitter(session: AsyncSession, request: Request) -> AuthContext:
    """The shared CP verifies the bound session and CSRF, never just its audience."""
    audience = None
    raw_audience = request.headers.get("X-Loom-Session-Audience")
    if raw_audience is not None:
        if len(raw_audience) > 4096:
            raise HTTPException(status_code=401, detail="invalid session audience")
        try:
            audience = ApplicationSessionAudienceV1.model_validate_json(raw_audience)
        except ValueError:
            raise HTTPException(status_code=401, detail="invalid session audience") from None
    ctx = require_human_or_admin(await verify_session_cookie(session, request.cookies.get("loom_session"), audience=audience))
    verify_csrf(ctx, request.headers.get("X-Loom-CSRF"))
    require_scope(ctx, "submit")
    if ctx.team_id is not None and not is_admin(ctx):
        disabled_at = (await session.execute(
            select(Team.disabled_at).where(Team.id == ctx.team_id),
        )).scalar_one_or_none()
        if disabled_at is not None:
            raise HTTPException(status_code=403, detail="team is disabled")
    await session.commit()
    return ctx


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
        if not authorization:
            ctx = await _browser_submitter(session, request)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized to submit")

    task_id = payload.get("task_id")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id required")
    idempotency_key = payload.get("idempotency_key")

    # Plan 19 validated only batch existence. Multi-team deployments need
    # ownership validation too: normal team tokens may only submit into their
    # own batches, while the internal batch-runner token derives the target
    # tenant from the parent batch row.
    batch_id = payload.get("batch_id")
    batch_team_id: UUID | None = None
    batch_backend = "docker" if local_execution_enabled() else NEBIUS_BACKEND
    batch_submitter_user_id: UUID | None = None
    batch_usage_user_id: UUID | None = None
    batch_usage_actor: str | None = None
    batch_runtime_profile: dict[str, Any] | None = None
    batch_pool_origin: dict[str, Any] | None = None
    pool_origin: dict[str, Any] | None = None
    if batch_id is not None:
        async with request.app.state.session_factory() as session:
            batch_row = (
                await session.execute(
                    select(
                        Batch.team_id,
                        Batch.backend,
                        Batch.submitted_by_user_id,
                        Batch.usage_attributed_user_id,
                        Batch.usage_attributed_actor,
                        Batch.service_execution_runtime_profile,
                        Batch.pool_origin,
                    ).where(Batch.id == batch_id),
                )
            ).first()
        if batch_row is None:
            raise HTTPException(
                status_code=400,
                detail=f"unknown batch {batch_id}",
            )
        batch_team_id = batch_row.team_id
        batch_backend = batch_row.backend
        batch_submitter_user_id = batch_row.submitted_by_user_id
        batch_usage_user_id = batch_row.usage_attributed_user_id
        batch_usage_actor = batch_row.usage_attributed_actor
        batch_runtime_profile = batch_row.service_execution_runtime_profile
        batch_pool_origin = batch_row.pool_origin

    if ctx.team_id is not None:
        if "submit" not in ctx.scopes:
            raise HTTPException(
                status_code=401,
                detail="not authorized to submit",
            )
        if batch_team_id is not None and batch_team_id != ctx.team_id:
            raise HTTPException(
                status_code=403,
                detail="batch belongs to another team",
            )
        require_submitting_user(ctx)
        submit_team_id = ctx.team_id
        submitter_user_id = ctx.user_id
        usage_user_id = ctx.user_id
        usage_actor = f"user:{ctx.user_id}" if ctx.user_id is not None else None
    elif "submit:batch" in ctx.scopes and batch_team_id is not None:
        submit_team_id = batch_team_id
        submitter_user_id = batch_submitter_user_id
        usage_user_id = batch_usage_user_id
        usage_actor = batch_usage_actor
        # Only authenticated internal fan-out inherits its immutable parent.
        # A user naming a shared batch, or supplying a body/header origin, is
        # not provenance. Direct public submissions need their own handoff.
        if batch_pool_origin is not None:
            try:
                origin = PoolWorkOriginV1.model_validate(batch_pool_origin)
                if str(origin.submission_id) != str(batch_id) or origin.kind == "personal_build":
                    raise ValueError("invalid batch origin")
                pool_origin = origin.model_dump(mode="json")
            except ValueError:
                raise HTTPException(status_code=409, detail="batch submission origin unavailable") from None
    else:
        raise HTTPException(status_code=401, detail="not authorized to submit")

    raw_submission_id = request.headers.get("X-Loom-Submission-ID")
    if raw_submission_id is not None:
        # Qualify before even the idempotency fast path. A body/header assertion
        # cannot promote work or borrow another user's retained service handoff.
        async with request.app.state.session_factory() as session:
            try:
                direct_origin = await qualify_trial_submission(session, raw_submission_id,
                    team_id=ctx.team_id, user_id=ctx.user_id, payload=payload)
            except ValueError:
                raise HTTPException(status_code=409, detail="submission provenance unavailable") from None
        pool_origin = direct_origin.model_dump(mode="json")

    # Plan 19: if `idempotency_key` was supplied and a trial with that
    # key already exists FOR THIS TEAM, return its trial_id without
    # minting a new row. The team-scoping is important — without it,
    # a cross-team idempotency_key collision would leak the existence
    # of the other team's trial (audit H1). The follow-up INSERT below
    # has `ON CONFLICT DO NOTHING` which closes the race window; the
    # early read just skips the (heavier) license + quota work on the
    # common "runner re-submits an already-emitted trial" path.
    if idempotency_key is not None:
        async with request.app.state.session_factory() as session:
            existing_row = (
                await session.execute(
                    select(TrialRow, TaskRow)
                    .join(TaskRow, TaskRow.id == TrialRow.task_id)
                    .where(
                        TrialRow.idempotency_key == idempotency_key,
                        TrialRow.team_id == submit_team_id,
                    ),
                )
            ).one_or_none()
            if existing_row is not None:
                existing, existing_task = existing_row
                # A replay reads the original Trial; do not recreate retired
                # ARM build prerequisites for a historical execution.
                if existing.requires_caps.get("cpu_arch") != "arm64":
                    await _ensure_trial_task_image_links(
                        session,
                        trial_id=existing.id,
                        task_row=existing_task,
                    )
                await session.commit()
                return {
                    "trial_id": str(existing.id),
                    "state": existing.state,
                    "submitted_at": existing.submitted_at.isoformat(),
                }

    async with request.app.state.session_factory() as session:
        task_row = (
            await session.execute(
                visible_tasks(team_id=submit_team_id).where(TaskRow.id == task_id),
            )
        ).scalar_one_or_none()
        if task_row is not None and task_row.benchmark_id is not None:
            await reject_non_runnable_benchmark_profiles(
                session,
                [task_row.benchmark_id],
            )
    if task_row is None:
        raise HTTPException(status_code=404, detail="task not found")

    try:
        task_config = normalize_steps(
            TaskConfig.model_validate(task_row.config),
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid task config for {task_id}: {exc}",
        ) from exc
    try:
        execution_cpu_arch(task_config.environment.cpu_arch)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    # Snapshot deployment RetryPolicy defaults into the trial payload at submit
    # time when the submitter didn't set an explicit `retry` block (#401). Persisting
    # the resolved policy keeps clone/re-run reproducible after an operator retunes
    # the deployment defaults.
    settings = request.app.state.settings
    raw_config = dict(payload.get("config") or {})
    if "retry" not in raw_config:
        raw_config["retry"] = {
            "max_attempts": settings.trial_retry_default_max_attempts,
            "retry_on": list(settings.trial_retry_default_retry_on),
            "backoff": {
                "base_sec": settings.trial_retry_default_backoff_base_sec,
                "max_sec": settings.trial_retry_default_backoff_max_sec,
                "multiplier": settings.trial_retry_default_backoff_multiplier,
                "jitter": settings.trial_retry_default_backoff_jitter,
            },
        }
    try:
        trial_config = TrialConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid trial config: {exc}",
        ) from exc
    trial_config = apply_legacy_verifier_default(
        task_config,
        trial_config,
        task_checksum=task_row.checksum,
        legacy_separate_verifier_checksum=task_row.legacy_separate_verifier_checksum,
        source_provenance=task_row.source_provenance or {},
    )
    if trial_config.multi_model is not None and trial_config.multi_model.enabled:
        from loom.models.trial import (
            MultiModelSwitchSpec,
            materialize_multi_model_switch_episode,
        )

        materialized = materialize_multi_model_switch_episode(trial_config.multi_model)
        trial_config = trial_config.model_copy(
            update={
                "multi_model": MultiModelSwitchSpec.model_validate(materialized),
            },
        )
    async with request.app.state.session_factory() as session:
        await validate_submission_agent_task_compatibility(
            session,
            team_id=submit_team_id,
            task_ids=[task_id],
            trial_config=trial_config.model_dump(mode="json"),
        )
    if trial_config.agent_version is not None and (
        batch_backend != NEBIUS_BACKEND or task_config.service_execution is not None
    ):
        raise HTTPException(status_code=400, detail="agent_version requires automatic native Nebius execution")
    requires_caps = derive_requires_caps(task_config)
    requires_caps_json = requires_caps.model_dump(mode="json")
    requires_caps_json["backend"] = batch_backend
    if trial_config.multi_model is not None and trial_config.multi_model.enabled:
        requires_caps_json["terminus2_model_switch"] = True
    required_worker_pool = _required_worker_pool(payload)
    task_service_execution = task_config.service_execution
    automatic_compatible = False
    if task_service_execution is None and batch_backend == NEBIUS_BACKEND:
        profile = (
            ServiceExecutionRuntimeProfileV1.model_validate(batch_runtime_profile)
            if batch_runtime_profile is not None
            else None
        )
        automatic_compatible = profile is not None and not execution_selection_rejections(
            task_config,
            trial_config,
            profile,
            source_provenance=dict(task_row.source_provenance or {}),
            allow_task_image_preparation=True,
        )
    required_worker_pool = _resolve_required_worker_pool_for_backend(
        batch_backend=batch_backend,
        requested_pool=required_worker_pool,
        task_service_pool=(
            task_service_execution.logical_pool_id
            if task_service_execution is not None
            else None
        ),
        automatic_compatible=automatic_compatible,
    )
    if required_worker_pool is not None:
        requires_caps_json["worker_pool"] = required_worker_pool

    trial_id = uuid4()
    async with request.app.state.session_factory() as session:
        # Defensive: a Team can exist without a TeamQuota row. If so, the
        # §2.6 DRF claim query's JOIN team_quotas would silently exclude
        # every trial we insert, and the trial would languish in queued
        # forever. Upsert a default quota row idempotently before the
        # trial INSERT so submission and scheduling stay aligned.
        await session.execute(
            pg_insert(TeamQuota)
            .values(team_id=submit_team_id)
            .on_conflict_do_nothing(index_elements=["team_id"]),
        )
        # Clamp requested max_attempts to the team's admin-set ceiling (#401).
        # The scheduler's claim query enforces this too, but persisting the
        # clamped value keeps the snapshot honest: what the trial payload says
        # matches what will actually run.
        ceiling = (
            await session.execute(
                select(TeamQuota.max_attempts_ceiling).where(
                    TeamQuota.team_id == submit_team_id,
                ),
            )
        ).scalar_one()
        if trial_config.retry.max_attempts > ceiling:
            trial_config = trial_config.model_copy(
                update={
                    "retry": trial_config.retry.model_copy(
                        update={
                            "max_attempts": ceiling,
                        }
                    ),
                }
            )
        # Plan 19: batch_id + idempotency_key are optional. When
        # `idempotency_key` is set we use pg_insert + ON CONFLICT DO
        # NOTHING so a concurrent race (two runner instances picking
        # the same batch row before the SELECT-skip-locked is held)
        # doesn't produce duplicate trial rows. `batch_id` was
        # already FK-validated upstream.
        # Plan 23: sample_idx is the n-sampling index. Defaults to 0 so
        # hand-submitted trials and pre-migration callers Just Work.
        sample_idx = int(payload.get("sample_idx") or 0)
        # Plan 28 PR-3: combination_idx is the multi-combination index.
        # Defaults to 0 (single-combination batches + hand-submitted
        # trials).
        combination_idx = int(payload.get("combination_idx") or 0)
        # cluster-deploy.md §Schema additions: per-trial provider
        # override. loom_service validated team-scope before forwarding;
        # control-plane trusts the validated payload (it doesn't have
        # a session_factory wired to provider_connections specifically,
        # and the FK enforces existence at INSERT time).
        provider_connection_id = payload.get("provider_connection_id")
        provider_model_id = payload.get("provider_model_id")
        # #672 PR-3: family_key is set by the batch_runner when the
        # parent batch opted into family-run mode. NULL for classic
        # batches; the scheduler's claim query gates trials whose
        # family_key is set on the matching batch_family_state row.
        family_key = payload.get("family_key")
        if family_key is not None and not isinstance(family_key, str):
            raise HTTPException(
                status_code=400,
                detail="family_key must be a string when supplied",
            )
        insert_values: dict[str, Any] = {
            "id": trial_id,
            "pool_origin": pool_origin,
            "team_id": submit_team_id,
            "task_id": task_id,
            "config": trial_config.model_dump(mode="json"),
            "requires_caps": requires_caps_json,
            "state": "queued",
            "submit_priority": trial_config.submit_priority,
            "batch_id": batch_id,
            "submitted_by_user_id": submitter_user_id,
            "usage_attributed_user_id": usage_user_id,
            "usage_attributed_actor": usage_actor,
            "idempotency_key": idempotency_key,
            "sample_idx": sample_idx,
            "combination_idx": combination_idx,
            "provider_connection_id": provider_connection_id,
            "provider_model_id": provider_model_id,
            "family_key": family_key,
        }
        if idempotency_key is not None:
            # The partial unique index is `WHERE idempotency_key IS NOT
            # NULL`; the ON CONFLICT predicate must match it for
            # Postgres to use the index as a conflict target.
            stmt = (
                pg_insert(TrialRow)
                .values(**insert_values)
                .on_conflict_do_nothing(
                    index_elements=["idempotency_key"],
                    index_where=text(
                        "idempotency_key IS NOT NULL",
                    ),
                )
                .returning(TrialRow.id, TrialRow.submitted_at)
            )
            result = await session.execute(stmt)
            row = result.one_or_none()
            if row is None:
                # ON CONFLICT fired — another caller won the race.
                # Re-read the canonical row scoped to this team (a
                # cross-team idempotency-key collision should never
                # reach here because the partial unique index is
                # global, but we never expose another team's trial:
                # if the canonical row belongs to a different team
                # we 409 the caller).
                existing = (
                    await session.execute(
                        select(TrialRow).where(
                            TrialRow.idempotency_key == idempotency_key,
                        ),
                    )
                ).scalar_one()
                if existing.team_id != submit_team_id:
                    raise HTTPException(
                        status_code=409,
                        detail=("idempotency_key collision with another team's trial"),
                    )
                existing_task = (
                    await session.execute(
                        select(TaskRow).where(TaskRow.id == existing.task_id),
                    )
                ).scalar_one()
                await _ensure_trial_task_image_links(
                    session,
                    trial_id=existing.id,
                    task_row=existing_task,
                )
                await session.commit()
                return {
                    "trial_id": str(existing.id),
                    "state": existing.state,
                    "submitted_at": existing.submitted_at.isoformat(),
                }
            trial_id = row.id
            submitted_at = row.submitted_at
        else:
            result = await session.execute(
                insert(TrialRow).values(**insert_values).returning(TrialRow.submitted_at),
            )
            submitted_at = result.scalar_one()
        lifecycle_authority_id = await ensure_trial_lifecycle_authority(
            session,
            trial_id=trial_id,
            team_id=submit_team_id,
            created_at=submitted_at,
        )
        lifecycle_result = await session.execute(
            update(TrialRow)
            .where(
                TrialRow.id == trial_id,
                TrialRow.lifecycle_authority_id.is_(None),
            )
            .values(lifecycle_authority_id=lifecycle_authority_id)
        )
        if lifecycle_result.rowcount != 1:
            raise RuntimeError("trial lifecycle authority binding is stale")
        await _ensure_trial_task_image_links(
            session,
            trial_id=trial_id,
            task_row=task_row,
        )
        if trial_config.multi_model is not None and trial_config.multi_model.enabled:
            from loom.model_switch_store import persist_model_switch_plan

            conn_id = None
            if provider_connection_id:
                conn_id = UUID(str(provider_connection_id))
            inherit_raw = payload.get("inherit_model_switch_plan_from_trial_id")
            inherit_from = UUID(str(inherit_raw)) if inherit_raw else None
            conn_row = None
            if conn_id is not None:
                conn_row = (
                    await session.execute(
                        select(ProviderConnection).where(
                            ProviderConnection.id == conn_id,
                        ),
                    )
                ).scalar_one_or_none()
            dumped = trial_config.model_dump(mode="json")
            await persist_model_switch_plan(
                session,
                trial_id=trial_id,
                trial_config=dumped,
                agent_model=trial_config.agent_model,
                provider_connection_id=conn_id,
                combination_idx=combination_idx,
                inherit_from_trial_id=inherit_from,
                provider_connection=conn_row,
            )
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "state": "queued",
        "submitted_at": submitted_at.isoformat(),
    }


_FAMILY_CANCEL_SCOPE_SQL = text("""
SELECT team_id
  FROM trials
 WHERE id = (:trial_id)::uuid
   AND batch_id IS NOT NULL
   AND family_key IS NOT NULL;
""")

_BATCH_CANCEL_SCOPE_SQL = text("""
SELECT team_id
  FROM trials
 WHERE id = (:trial_id)::uuid
   AND batch_id IS NOT NULL;
""")


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(request.app.state, "admin_secret_verifier", None),
            allow_family_orchestrator=True,
        )
        if not authorization:
            ctx = await _browser_submitter(session, request)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    caller_is_admin = is_admin(ctx)
    if ctx.type == "family_orchestrator":
        async with request.app.state.session_factory() as session:
            scoped_team_id = (
                await session.execute(
                    _FAMILY_CANCEL_SCOPE_SQL,
                    {"trial_id": trial_id},
                )
            ).scalar_one_or_none()
        if scoped_team_id is None:
            raise HTTPException(status_code=401, detail="not authorized")
    elif (
        ctx.type == "worker"
        and ctx.team_id is None
        and ctx.user_id is None
        and ctx.scopes == ["submit:batch"]
    ):
        async with request.app.state.session_factory() as session:
            scoped_team_id = (
                await session.execute(
                    _BATCH_CANCEL_SCOPE_SQL,
                    {"trial_id": trial_id},
                )
            ).scalar_one_or_none()
        if scoped_team_id is None:
            raise HTTPException(status_code=401, detail="not authorized")
    else:
        # A platform admin may cancel any trial; a team caller is scoped to its
        # own team. A non-admin token with no team has nothing it may act on.
        if not caller_is_admin and ctx.team_id is None:
            raise HTTPException(status_code=401, detail="not authorized")
        scoped_team_id = None if caller_is_admin else ctx.team_id

    row: Any | None = None
    row = await cancel_trial_under_authority(
        session_factory=request.app.state.session_factory,

        trial_id=trial_id,
        team_id=scoped_team_id,
    )

    if row is None:
        raise HTTPException(
            status_code=409,
            detail="trial is in a terminal state (succeeded/failed/cancelled)",
        )
    response_state = (
        "cancellation-requested" if row["state"] in {"claimed", "running"} else "cancelled"
    )
    return {"trial_id": str(row["id"]), "state": response_state}


@router.get("/trials/{trial_id}")
async def get_trial(
    trial_id: UUID,
    request: Request,
    principal: RequestPrincipal,
) -> dict[str, Any]:
    ctx = principal
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                select(TrialRow).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(status_code=403, detail="trial belongs to another team")

    return {
        "id": str(row.id),
        "team_id": str(row.team_id),
        "task_id": row.task_id,
        "state": row.state,
        "worker_id": str(row.worker_id) if row.worker_id is not None else None,
        "failure_reason": row.failure_reason,
        "submitted_at": row.submitted_at.isoformat(),
        "claimed_at": row.claimed_at.isoformat() if row.claimed_at else None,
        "pre_start_heartbeat_at": (
            row.pre_start_heartbeat_at.isoformat() if row.pre_start_heartbeat_at else None
        ),
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "cancellation_requested_at": (
            row.cancellation_requested_at.isoformat()
            if row.cancellation_requested_at
            else None
        ),
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "attempt_count": row.attempt_count,
        "result": row.result,
        "trajectory_index": row.trajectory_index,
    }


@router.get("/trials/{trial_id}/llm-calls")
async def get_trial_llm_calls(
    trial_id: UUID,
    request: Request,
    principal: RequestPrincipal,
) -> dict[str, Any]:
    """List every llm_calls row for this trial, ordered by capture time.
    Read by the worker at finalize to project LLMCallEvents into the
    trajectory before ATIF projection runs (Plan 9 amendment A9.2)."""
    ctx = principal
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")

    # Worker scope OR same-team team-token can read.
    async with request.app.state.session_factory() as session:
        trial_row = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
    if trial_row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and trial_row != ctx.team_id:
        raise HTTPException(
            status_code=403,
            detail="trial belongs to another team",
        )

    async with request.app.state.session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(LlmCall)
                    .where(LlmCall.trial_id == trial_id)
                    .order_by(LlmCall.captured_at),
                )
            )
            .scalars()
            .all()
        )
    return {"items": [serialize_llm_call(row) for row in rows]}


class _TerminusReclaimBody(BaseModel):
    step_id: str = Field(min_length=1)
    worker_id: UUID | None = None


class _EpisodeCheckpointBody(BaseModel):
    execution_id: UUID
    run_attempt_id: UUID
    episode: int = Field(ge=1)
    active_role: str
    last_call_ordinal: int = Field(ge=0)
    last_seq: int = Field(ge=0)
    tmux_session_id: str | None = None
    attempt_deadline_wall_clock: datetime | None = None


async def _terminus_principal(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AuthContext | None:
    async with request.app.state.session_factory() as session:
        return await verify_bearer_token(session, authorization)


TerminusPrincipal = Annotated[AuthContext | None, Depends(_terminus_principal)]






@router.post("/trials/{trial_id}/terminus/reclaim")
async def reclaim_terminus(
    trial_id: UUID,
    payload: _TerminusReclaimBody,
    request: Request,
    principal: TerminusPrincipal,
) -> dict[str, Any]:
    ctx = principal
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    from loom_control_plane.terminus_recovery import reclaim_terminus_execution

    async with request.app.state.session_factory() as session:
        trial_team = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
        if trial_team is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if ctx.team_id is not None and trial_team != ctx.team_id:
            raise HTTPException(status_code=403, detail="trial belongs to another team")
        state = await reclaim_terminus_execution(
            session,
            trial_id=trial_id,
            step_id=payload.step_id,
            worker_id=payload.worker_id,
        )
        await session.commit()
    return state.model_dump(mode="json")


@router.post("/trials/{trial_id}/terminus/episode-checkpoints")
async def post_episode_checkpoint(
    trial_id: UUID,
    payload: _EpisodeCheckpointBody,
    request: Request,
    principal: TerminusPrincipal,
) -> dict[str, Any]:
    ctx = principal
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    from loom_control_plane.terminus_recovery import write_episode_checkpoint

    async with request.app.state.session_factory() as session:
        trial_row = (
            await session.execute(
                select(TrialRow).where(TrialRow.id == trial_id).with_for_update(),
            )
        ).scalar_one_or_none()
        if trial_row is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if ctx.team_id is not None and trial_row.team_id != ctx.team_id:
            raise HTTPException(status_code=403, detail="trial belongs to another team")
        if trial_row.state not in {"claimed", "running"}:
            raise HTTPException(status_code=409, detail="trial_terminal")
        attempt_deadline = payload.attempt_deadline_wall_clock
        if attempt_deadline is not None:
            if attempt_deadline.tzinfo is None or attempt_deadline.utcoffset() is None:
                raise HTTPException(status_code=422, detail="attempt_deadline_timezone_required")
            attempt_deadline = attempt_deadline.astimezone(UTC)
            if datetime.now(UTC) >= attempt_deadline:
                raise HTTPException(status_code=409, detail="attempt_deadline_elapsed")
        row = await write_episode_checkpoint(
            session,
            execution_id=payload.execution_id,
            run_attempt_id=payload.run_attempt_id,
            episode=payload.episode,
            active_role=payload.active_role,
            last_call_ordinal=payload.last_call_ordinal,
            last_seq=payload.last_seq,
            tmux_session_id=payload.tmux_session_id,
        )
        if attempt_deadline is not None and datetime.now(UTC) >= attempt_deadline:
            await session.rollback()
            raise HTTPException(status_code=409, detail="attempt_deadline_elapsed")
        await session.commit()
    return {"id": str(row.id), "version": row.version, "checksum": row.checksum}
