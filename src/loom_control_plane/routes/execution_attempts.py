"""Claim-fenced worker reports and control for Pipeline ExecutionAttempts."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext, verify_bearer_token
from loom.db.schema import (
    ArtifactUploadSession,
    ExecutionAttempt,
    ExecutionAttemptControlCommand,
    ExecutionAttemptRequest,
    ExecutionAttemptWorkerEvent,
    PipelineBudgetLedger,
    PipelineBudgetReservation,
    PipelineCancellationOutbox,
    PipelineEvent,
    PipelineLivePreviewFrame,
    PipelineLivePreviewGeneration,
    PipelineProviderDispatch,
    PipelineRun,
    PipelineStageRun,
    Worker,
)
from loom.pipeline.artifact_commit import ArtifactCommitError
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.pipeline.live_preview import (
    PREVIEW_GLOBAL_MAX_BYTES,
    PREVIEW_GLOBAL_MAX_GENERATIONS,
    PREVIEW_TEAM_MAX_BYTES,
    PREVIEW_TEAM_MAX_GENERATIONS,
    LivePreviewContractError,
    LivePreviewRecordV1,
    is_stage1_live_preview_eligible,
    validate_preview_jpeg,
)
from loom.pipeline.retry import retry_decision
from loom.pipeline.state import RetryClass
from loom.pipeline.work_protocol import (
    CheckpointPrepareRequestV1,
    ExecutionCancelAckV1,
    ExecutionCompleteV1,
    ExecutionControlCommandV1,
    ExecutionControlResponseV1,
    ExecutionEventsV1,
    ExecutionFailedV1,
    ExecutionHeartbeatV1,
    ExecutionStartedV1,
    FinalOutputAbortV1,
    FinalOutputFileCompleteV1,
    FinalOutputPrepareRequestV1,
    FinalOutputSessionCommitV1,
    UploadTokenRenewV1,
    WorkerLostCleanupAckV1,
)
from loom.security.redaction import redact_text
from loom_control_plane.execution_attempt_fencing import (
    AttemptFenceError,
    idempotency_values,
    replay_or_conflict,
    verify_attempt_claim,
)
from loom_control_plane.live_preview import (
    acquire_preview_capacity_locks,
    active_global_preview_totals,
    active_team_preview_totals,
    enforce_generation_bounds,
    generation_expiry,
    global_preview_bound_exceeded,
    publish_due,
    purge_live_preview,
)
from loom_control_plane.metrics import (
    PIPELINE_ARTIFACT_BYTES_TOTAL,
    PIPELINE_ARTIFACT_COMMIT_FAILURES_TOTAL,
    PIPELINE_CANCEL_LATENCY_SECONDS,
    PIPELINE_LIVE_PREVIEW_BYTES_TOTAL,
    PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL,
    PIPELINE_STAGE_DURATION_SECONDS,
)

router = APIRouter()

ClaimIdHeader = Annotated[UUID, Header(alias="X-Loom-Claim-Id")]
LeaseEpochHeader = Annotated[int, Header(alias="X-Loom-Lease-Epoch", ge=1)]
LeaseTokenHeader = Annotated[str, Header(alias="X-Loom-Lease-Token", min_length=32)]
RequestIdHeader = Annotated[UUID, Header(alias="X-Loom-Request-Id")]
PreviewIdempotencyHeader = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=128)
]
PreviewStepHeader = Annotated[
    int, Header(alias="X-Loom-Preview-Step", ge=0, le=18_446_744_073_709_551_615)
]


class ArtifactReadServiceV1(Protocol):
    async def read_manifest(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        if_match: str,
    ) -> Response: ...

    async def read_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        if_match: str,
        range_header: str | None,
    ) -> Response: ...


class FinalOutputServiceV1(Protocol):
    async def prepare(self, **kwargs: Any) -> dict[str, Any]: ...
    async def renew(self, **kwargs: Any) -> dict[str, Any]: ...
    async def put_part(self, **kwargs: Any) -> dict[str, Any]: ...
    async def complete_file(self, **kwargs: Any) -> dict[str, Any]: ...
    async def commit(self, **kwargs: Any) -> dict[str, Any]: ...
    async def abort(self, **kwargs: Any) -> dict[str, Any]: ...


class CheckpointServiceV1(FinalOutputServiceV1, Protocol):
    pass


def _stage_resource_class(stage: PipelineStageRun) -> str:
    if stage.node_kind == "gate":
        return "controller"
    profile = stage.resource_profile_json or {}
    variants = profile.get("execution_variants", [])
    return (
        "gpu"
        if any(
            isinstance(item, dict) and int(item.get("gpu_count_exact", 0)) > 0 for item in variants
        )
        else "cpu"
    )


def _artifact_failure_reason(error: Exception) -> str:
    if isinstance(error, TimeoutError | ConnectionError):
        return "transport"
    reason = error.reason if isinstance(error, ArtifactCommitError) else ""
    if "fenced" in reason or "claim" in reason:
        return "fenced"
    if any(value in reason for value in ("digest", "hash", "integrity", "readback")):
        return "integrity"
    if any(value in reason for value in ("budget", "quota", "too_large", "exceeded")):
        return "quota"
    if any(
        value in reason
        for value in ("contract", "invalid", "mismatch", "conflict", "not_found", "session")
    ):
        return "contract"
    return "internal"


async def _commit_artifact_operation(
    service: FinalOutputServiceV1,
    *,
    commit_kind: str,
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        return await service.commit(**kwargs)
    except Exception as exc:
        PIPELINE_ARTIFACT_COMMIT_FAILURES_TOTAL.labels(
            commit_kind=commit_kind,
            reason=_artifact_failure_reason(exc),
        ).inc()
        raise


async def _ack_cancellation_outbox(
    session: AsyncSession,
    *,
    attempt: ExecutionAttempt,
    observed_at: datetime,
    outcome: str,
    cleanup_proof: dict[str, Any],
) -> None:
    outbox = (
        await session.execute(
            select(PipelineCancellationOutbox)
            .where(PipelineCancellationOutbox.execution_attempt_id == attempt.id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if outbox is None:
        return
    ack = {
        "cleanup_proof": cleanup_proof,
        "execution_attempt_id": str(attempt.id),
        "observed_at": observed_at.isoformat(),
        "outcome": outcome,
    }
    digest = canonical_digest(ack)
    if outbox.state == "acked":
        if outbox.ack_digest != digest:
            raise HTTPException(status_code=409, detail="cancellation_ack_conflict")
        return
    outbox.state = "acked"
    outbox.ack_json = ack
    outbox.ack_digest = digest
    outbox.acked_at = observed_at
    outbox.version += 1


async def _append_terminal_event(
    session: AsyncSession,
    *,
    run: PipelineRun,
    stage: PipelineStageRun,
    attempt: ExecutionAttempt,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    """Append one event while holding the run's sequence fence."""

    locked_run = (
        await session.execute(select(PipelineRun).where(PipelineRun.id == run.id).with_for_update())
    ).scalar_one_or_none()
    if locked_run is None:
        raise HTTPException(status_code=409, detail="pipeline_run_unavailable")
    seq = locked_run.next_event_seq
    locked_run.next_event_seq += 1
    locked_run.version += 1
    session.add(
        PipelineEvent(
            pipeline_run_id=run.id,
            seq=seq,
            stage_run_id=stage.id,
            execution_attempt_id=attempt.id,
            event_type=event_type,
            actor_kind="worker",
            actor_id=str(attempt.worker_id) if attempt.worker_id is not None else None,
            payload_json=payload,
        )
    )


def _attempt_gpu_seconds(attempt: ExecutionAttempt, *, observed_at: datetime) -> int:
    """Return conservative integral GPU-seconds from the frozen StageRequest."""

    request = attempt.stage_request_json or {}
    budget = request.get("budget")
    if not isinstance(budget, dict):
        raise HTTPException(status_code=409, detail="attempt_budget_unavailable")
    gpu_limit = budget.get("gpu_seconds_limit")
    timeout = budget.get("timeout_seconds")
    if not isinstance(gpu_limit, int) or not isinstance(timeout, int) or timeout <= 0:
        raise HTTPException(status_code=409, detail="attempt_budget_unavailable")
    if gpu_limit == 0:
        return 0
    gpu_count, remainder = divmod(gpu_limit, timeout + 35)
    if gpu_count <= 0 or remainder:
        raise HTTPException(status_code=409, detail="attempt_gpu_budget_invalid")
    runtime = attempt.heartbeat_runtime_seconds
    if runtime is None:
        if attempt.runtime_started_at is None:
            raise HTTPException(status_code=409, detail="attempt_runtime_unavailable")
        runtime = max((observed_at - attempt.runtime_started_at).total_seconds(), 0.0)
    return gpu_count * math.ceil(max(runtime, 0.0))


async def _settle_attempt_reservations(
    session: AsyncSession,
    *,
    stage: PipelineStageRun,
    attempt: ExecutionAttempt,
    observed_at: datetime,
    final_output_bytes: int | None,
) -> int:
    """Settle success usage or release failed-attempt capacity exactly once."""

    reservations = list(
        (
            await session.execute(
                select(PipelineBudgetReservation)
                .where(
                    PipelineBudgetReservation.execution_attempt_id == attempt.id,
                    PipelineBudgetReservation.state == "active",
                )
                .order_by(PipelineBudgetReservation.kind, PipelineBudgetReservation.reservation_key)
                .with_for_update()
            )
        ).scalars()
    )
    if any(item.kind == "provider" for item in reservations):
        raise HTTPException(status_code=409, detail="provider_reservation_active")
    ledger = await session.get(PipelineBudgetLedger, stage.pipeline_run_id, with_for_update=True)
    if ledger is None:
        raise HTTPException(status_code=409, detail="pipeline_budget_unavailable")

    final_key = f"artifact:final:{attempt.id}"
    final_reservations = [
        item
        for item in reservations
        if item.kind == "artifact" and item.reservation_key == final_key
    ]
    if final_output_bytes is not None and len(final_reservations) != 1:
        raise HTTPException(status_code=409, detail="final_output_reservation_missing")
    if final_output_bytes is not None and any(
        item.kind == "artifact" and item.reservation_key != final_key for item in reservations
    ):
        raise HTTPException(status_code=409, detail="artifact_reservation_active")

    gpu_actual = _attempt_gpu_seconds(attempt, observed_at=observed_at)
    gpu_reservations = [item for item in reservations if item.kind == "gpu"]
    if (gpu_actual > 0) != (len(gpu_reservations) == 1):
        raise HTTPException(status_code=409, detail="gpu_reservation_mismatch")
    if len(gpu_reservations) > 1:
        raise HTTPException(status_code=409, detail="gpu_reservation_mismatch")

    for reservation in reservations:
        if reservation.kind == "provider":  # guarded above; keeps type narrowing explicit
            continue
        actual: int | None
        if reservation.kind == "gpu":
            actual = gpu_actual
        else:
            actual = final_output_bytes
        if actual is not None and actual > reservation.reserved_amount:
            raise HTTPException(status_code=409, detail="budget_settlement_exceeded")

        if reservation.kind == "gpu":
            ledger.gpu_reserved_seconds -= reservation.reserved_amount
            if actual is not None:
                ledger.gpu_settled_seconds += actual
        else:
            ledger.artifact_reserved_bytes -= reservation.reserved_amount
            if actual is not None:
                ledger.artifact_settled_bytes += actual
        reservation.state = "released" if actual is None else "settled"
        reservation.settled_amount = actual
        reservation.settled_at = observed_at
    ledger.version += 1
    ledger.updated_at = observed_at
    return gpu_actual


async def _require_no_active_uploads(session: AsyncSession, *, attempt_id: UUID) -> None:
    active = (
        await session.execute(
            select(func.count())
            .select_from(ArtifactUploadSession)
            .where(
                ArtifactUploadSession.execution_attempt_id == attempt_id,
                ArtifactUploadSession.state.in_(
                    ["preparing", "uploading", "uploaded", "committing", "committed_ready"]
                ),
            )
        )
    ).scalar_one()
    if active:
        raise HTTPException(status_code=409, detail="cleanup_incomplete")


async def _worker_auth(
    request: Request,
    authorization: str | None,
    *,
    scope: str,
) -> AuthContext:
    async with request.app.state.session_factory() as auth_session:
        ctx = await verify_bearer_token(auth_session, authorization)
    if ctx is None or scope not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")
    return ctx


def _raise_fence(exc: AttemptFenceError) -> None:
    raise HTTPException(
        status_code=404 if exc.not_found else 409,
        detail="not_found" if exc.not_found else exc.reason,
    ) from exc


async def _begin_mutation(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    ctx: AuthContext,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    request_id: UUID,
    route: str,
    payload: Any,
) -> tuple[ExecutionAttempt, dict[str, Any] | None]:
    try:
        attempt = await verify_attempt_claim(
            session,
            attempt_id=attempt_id,
            auth=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            require_live_lease=False,
        )
        replay = await replay_or_conflict(
            session,
            attempt_id=attempt_id,
            route=route,
            request_id=request_id,
            payload=payload,
        )
        if replay is None and (
            attempt.state not in {"claimed", "running"}
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= datetime.now(UTC)
        ):
            raise AttemptFenceError("claim_fenced")
    except AttemptFenceError as exc:
        _raise_fence(exc)
    return attempt, replay


async def _journal_response(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    route: str,
    request_id: UUID,
    payload: Any,
    response: dict[str, Any],
) -> None:
    await session.execute(
        insert(ExecutionAttemptRequest).values(
            **idempotency_values(
                attempt_id=attempt_id,
                route=route,
                request_id=request_id,
                payload=payload,
                response=response,
            )
        )
    )


async def _bounded_preview_body(request: Request) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            declared_size = int(declared)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="preview_size_invalid") from exc
        if not 1 <= declared_size <= 524_288:
            raise HTTPException(status_code=413, detail="preview_size_invalid")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > 524_288:
            raise HTTPException(status_code=413, detail="preview_size_invalid")
        chunks.append(chunk)
    if total == 0:
        raise HTTPException(status_code=422, detail="preview_size_invalid")
    return b"".join(chunks)


@router.put("/api/v1/execution-attempts/{attempt_id}/live-preview/frames/{sequence}")
async def publish_live_preview_frame(
    attempt_id: UUID,
    sequence: Annotated[int, Path(ge=0)],
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    idempotency_key: PreviewIdempotencyHeader,
    step_idx: PreviewStepHeader,
    if_match: Annotated[str, Header(alias="If-Match")],
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    if request.headers.get("content-type") != "image/jpeg":
        raise HTTPException(status_code=415, detail="preview_content_type_invalid")
    value = await _bounded_preview_body(request)
    digest = digest_bytes(value)
    if if_match != f'"{digest}"':
        raise HTTPException(status_code=412, detail="preview_digest_mismatch")
    try:
        validate_preview_jpeg(value)
    except LivePreviewContractError as exc:
        PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL.labels(result="rejected", reason=exc.reason).inc()
        raise HTTPException(status_code=422, detail=exc.reason) from exc

    observed = datetime.now(UTC)
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
            )
        except AttemptFenceError as exc:
            PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL.labels(
                result="rejected", reason="claim_fenced"
            ).inc()
            _raise_fence(exc)
        stage = await session.get(PipelineStageRun, attempt.stage_run_id)
        if stage is None:
            raise HTTPException(status_code=404, detail="not_found")
        run = await session.get(PipelineRun, stage.pipeline_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="not_found")
        if (
            attempt.state != "running"
            or attempt.started_at is None
            or stage.state != "running"
            or attempt.cancellation_requested_at is not None
            or run.cancellation_requested_at is not None
        ):
            raise HTTPException(status_code=409, detail="preview_lifecycle_fenced")
        if not is_stage1_live_preview_eligible(stage.resolved_execution_spec_json):
            raise HTTPException(status_code=409, detail="preview_contract_ineligible")

        await acquire_preview_capacity_locks(session, team_id=run.team_id)
        generation = await session.get(
            PipelineLivePreviewGeneration, attempt_id, with_for_update=True
        )
        if generation is None:
            team_generations, _team_bytes = await active_team_preview_totals(
                session, team_id=run.team_id
            )
            global_generations, _global_bytes = await active_global_preview_totals(session)
            if team_generations >= PREVIEW_TEAM_MAX_GENERATIONS:
                raise HTTPException(status_code=429, detail="preview_team_bound_exceeded")
            if global_generations >= PREVIEW_GLOBAL_MAX_GENERATIONS:
                raise HTTPException(status_code=429, detail="preview_global_bound_exceeded")
            generation = PipelineLivePreviewGeneration(
                execution_attempt_id=attempt_id,
                generation=attempt_id,
                team_id=run.team_id,
                pipeline_run_id=run.id,
                pipeline_stage_run_id=stage.id,
                worker_id=attempt.worker_id,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                state="waiting",
                expires_at=generation_expiry(observed),
            )
            session.add(generation)
            await session.flush()
        elif (
            generation.worker_id != attempt.worker_id
            or generation.claim_id != claim_id
            or generation.lease_epoch != lease_epoch
            or generation.purged_at is not None
        ):
            raise HTTPException(status_code=409, detail="preview_generation_fenced")

        prior = await session.get(PipelineLivePreviewFrame, (attempt_id, sequence))
        if prior is not None:
            if (
                prior.idempotency_key != idempotency_key
                or int(prior.step_idx) != step_idx
                or prior.jpeg_sha256 != digest
                or prior.jpeg_bytes != value
            ):
                raise HTTPException(status_code=409, detail="preview_replay_conflict")
            return JSONResponse(
                status_code=200,
                content={
                    "schema_version": "loom.behavior-stage1-live-preview.v1",
                    "attempt_id": str(attempt_id),
                    "generation": str(generation.generation),
                    "sequence": sequence,
                    "accepted": True,
                    "idempotent_replay": True,
                },
            )

        # Capacity is charged only for new frames. Identical replays above are
        # allowed at a full team/global budget and never double-charge bytes.
        idempotency_prior = (
            await session.execute(
                select(PipelineLivePreviewFrame).where(
                    PipelineLivePreviewFrame.execution_attempt_id == attempt_id,
                    PipelineLivePreviewFrame.idempotency_key == idempotency_key,
                )
            )
        ).scalar_one_or_none()
        if idempotency_prior is not None:
            raise HTTPException(status_code=409, detail="preview_idempotency_conflict")
        expected_sequence = (
            0 if generation.latest_sequence is None else generation.latest_sequence + 1
        )
        if sequence != expected_sequence:
            raise HTTPException(status_code=409, detail="preview_sequence_gap")
        if generation.latest_step_idx is not None and step_idx < int(generation.latest_step_idx):
            raise HTTPException(status_code=409, detail="preview_step_regressed")
        if not publish_due(last_received_at=generation.received_at, now=observed):
            raise HTTPException(status_code=429, detail="preview_cadence_exceeded")

        record = LivePreviewRecordV1(
            schema_version="loom.behavior-stage1-live-preview.v1",
            sequence=sequence,
            step_idx=step_idx,
            jpeg_sha256=digest,
            jpeg_size_bytes=len(value),
        )
        await enforce_generation_bounds(
            session,
            generation=generation,
            incoming_bytes=len(value),
        )
        await session.flush()
        _team_generations, team_bytes = await active_team_preview_totals(
            session, team_id=run.team_id
        )
        _global_generations, global_bytes = await active_global_preview_totals(session)
        if team_bytes + len(value) > PREVIEW_TEAM_MAX_BYTES:
            raise HTTPException(status_code=429, detail="preview_team_bound_exceeded")
        if global_bytes + len(value) > PREVIEW_GLOBAL_MAX_BYTES:
            raise HTTPException(status_code=429, detail="preview_global_bound_exceeded")
        session.add(
            PipelineLivePreviewFrame(
                execution_attempt_id=attempt_id,
                sequence=record.sequence,
                step_idx=record.step_idx,
                jpeg_sha256=record.jpeg_sha256,
                jpeg_size_bytes=record.jpeg_size_bytes,
                jpeg_bytes=value,
                idempotency_key=idempotency_key,
                received_at=observed,
            )
        )
        generation.state = "live"
        generation.latest_sequence = sequence
        generation.latest_step_idx = step_idx
        generation.received_at = observed
        generation.frame_count += 1
        generation.total_bytes += len(value)
        generation.expires_at = generation_expiry(observed)
        generation.updated_at = observed
        await session.commit()
    PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL.labels(result="accepted", reason="ok").inc()
    PIPELINE_LIVE_PREVIEW_BYTES_TOTAL.inc(len(value))
    return JSONResponse(
        status_code=201,
        content={
            "schema_version": "loom.behavior-stage1-live-preview.v1",
            "attempt_id": str(attempt_id),
            "generation": str(attempt_id),
            "sequence": sequence,
            "accepted": True,
            "idempotent_replay": False,
        },
    )


@router.post("/execution-attempts/{attempt_id}/heartbeats")
async def heartbeat_attempt(
    attempt_id: UUID,
    payload: ExecutionHeartbeatV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="heartbeats",
            payload=payload,
        )
        if replay is not None:
            return replay
        expires_at = datetime.now(UTC) + timedelta(seconds=60)
        attempt.heartbeat_phase = payload.phase
        attempt.heartbeat_runtime_seconds = payload.monotonic_runtime_seconds
        attempt.last_heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = expires_at
        response = {"lease_expires_at": expires_at.isoformat(), "state": attempt.state}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="heartbeats",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.get("/execution-attempts/{attempt_id}/control")
async def get_attempt_control(
    attempt_id: UUID,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    after_seq: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        try:
            await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        rows = (
            (
                await session.execute(
                    select(ExecutionAttemptControlCommand)
                    .where(
                        ExecutionAttemptControlCommand.execution_attempt_id == attempt_id,
                        ExecutionAttemptControlCommand.seq > after_seq,
                    )
                    .order_by(ExecutionAttemptControlCommand.seq)
                )
            )
            .scalars()
            .all()
        )
        current_seq = (
            await session.execute(
                select(func.coalesce(func.max(ExecutionAttemptControlCommand.seq), 0)).where(
                    ExecutionAttemptControlCommand.execution_attempt_id == attempt_id
                )
            )
        ).scalar_one()
    response = ExecutionControlResponseV1(
        commands=[ExecutionControlCommandV1(seq=row.seq, command=row.command) for row in rows],
        current_seq=current_seq,
    )
    return response.model_dump(mode="json")


@router.post("/execution-attempts/{attempt_id}/events")
async def append_attempt_events(
    attempt_id: UUID,
    payload: ExecutionEventsV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        _, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="events",
            payload=payload,
        )
        if replay is not None:
            return replay
        accepted_through = -1
        current_bytes = (
            await session.execute(
                select(func.coalesce(func.sum(ExecutionAttemptWorkerEvent.message_bytes), 0)).where(
                    ExecutionAttemptWorkerEvent.execution_attempt_id == attempt_id
                )
            )
        ).scalar_one()
        for event in payload.events:
            message = redact_text(event.message)
            message_bytes = len(message.encode("utf-8"))
            if int(current_bytes) + message_bytes > 16 * 1024 * 1024:
                await session.execute(
                    pg_insert(ExecutionAttemptWorkerEvent)
                    .values(
                        execution_attempt_id=attempt_id,
                        local_seq=9_223_372_036_854_775_807,
                        occurred_at=datetime.now(UTC),
                        stream="worker",
                        level="warning",
                        message="log_truncated",
                        message_bytes=len(b"log_truncated"),
                    )
                    .on_conflict_do_nothing(index_elements=["execution_attempt_id", "local_seq"])
                )
                break
            inserted = (
                await session.execute(
                    pg_insert(ExecutionAttemptWorkerEvent)
                    .values(
                        execution_attempt_id=attempt_id,
                        local_seq=event.local_seq,
                        occurred_at=event.timestamp,
                        stream=event.stream,
                        level=event.level,
                        message=message,
                        message_bytes=message_bytes,
                    )
                    .on_conflict_do_nothing(index_elements=["execution_attempt_id", "local_seq"])
                    .returning(ExecutionAttemptWorkerEvent.id)
                )
            ).scalar_one_or_none()
            if inserted is not None:
                current_bytes = int(current_bytes) + message_bytes
            accepted_through = event.local_seq
        response = {"accepted_through_local_seq": accepted_through}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="events",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.post("/execution-attempts/{attempt_id}/started")
async def report_attempt_started(
    attempt_id: UUID,
    payload: ExecutionStartedV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="started",
            payload=payload,
        )
        if replay is not None:
            return replay
        if attempt.started_at is not None:
            raise HTTPException(status_code=409, detail="attempt_already_started")
        stage = (
            await session.execute(
                select(PipelineStageRun).where(PipelineStageRun.id == attempt.stage_run_id)
            )
        ).scalar_one()
        attempt.state = "running"
        attempt.started_at = datetime.now(UTC)
        attempt.container_id = payload.container_id
        attempt.runtime_started_at = payload.runtime_started_at
        attempt.input_view_digest = payload.input_view_digest
        # The step-token exchange durably rotates execution-attempt JWTs and
        # writes their jti before the container reports started.  Older workers
        # report no jti; do not let that compatibility shape erase the live
        # Gateway revocation fence.
        if attempt.step_jwt_id is None:
            attempt.step_jwt_id = payload.step_jwt_id
        elif payload.step_jwt_id not in {None, attempt.step_jwt_id}:
            raise HTTPException(status_code=409, detail="step_jwt_rotated")
        if is_stage1_live_preview_eligible(stage.resolved_execution_spec_json):
            run = await session.get(PipelineRun, stage.pipeline_run_id)
            if run is None or attempt.worker_id is None:
                raise HTTPException(status_code=409, detail="preview_identity_unavailable")
            await acquire_preview_capacity_locks(session, team_id=run.team_id)
            generation = await session.get(
                PipelineLivePreviewGeneration, attempt.id, with_for_update=True
            )
            if generation is None:
                team_generations, _team_bytes = await active_team_preview_totals(
                    session, team_id=run.team_id
                )
                global_generations, global_bytes = await active_global_preview_totals(session)
                if (
                    team_generations < PREVIEW_TEAM_MAX_GENERATIONS
                    and not global_preview_bound_exceeded(
                        generations=global_generations,
                        bytes_used=global_bytes,
                        incoming=0,
                    )
                ):
                    session.add(
                        PipelineLivePreviewGeneration(
                            execution_attempt_id=attempt.id,
                            generation=attempt.id,
                            team_id=run.team_id,
                            pipeline_run_id=run.id,
                            pipeline_stage_run_id=stage.id,
                            worker_id=attempt.worker_id,
                            claim_id=claim_id,
                            lease_epoch=lease_epoch,
                            state="waiting",
                            expires_at=generation_expiry(datetime.now(UTC)),
                        )
                    )
        await session.execute(
            update(PipelineStageRun)
            .where(PipelineStageRun.id == attempt.stage_run_id)
            .values(
                state="running", started_at=datetime.now(UTC), version=PipelineStageRun.version + 1
            )
        )
        response = {"execution_attempt_id": str(attempt_id), "state": "running"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="started",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


async def _terminal_report(
    *,
    attempt_id: UUID,
    payload: ExecutionFailedV1 | ExecutionCancelAckV1,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    request_id: UUID,
    route: str,
    state: str,
    authorization: str | None,
) -> dict[str, Any]:
    if not isinstance(payload, ExecutionFailedV1) or state != "failed":
        raise HTTPException(status_code=409, detail="terminal_report_invalid")
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route=route,
            payload=payload,
        )
        if replay is not None:
            return replay
        observed_at = datetime.now(UTC)
        await _require_no_active_uploads(session, attempt_id=attempt.id)
        stage = await session.get(PipelineStageRun, attempt.stage_run_id, with_for_update=True)
        if stage is None:
            raise HTTPException(status_code=404, detail="not_found")
        run = await session.get(PipelineRun, stage.pipeline_run_id)
        if run is None:
            raise HTTPException(status_code=409, detail="pipeline_run_unavailable")
        ledger = await session.get(
            PipelineBudgetLedger, stage.pipeline_run_id, with_for_update=True
        )
        if ledger is None:
            raise HTTPException(status_code=409, detail="pipeline_budget_unavailable")
        await _settle_attempt_reservations(
            session,
            stage=stage,
            attempt=attempt,
            observed_at=observed_at,
            final_output_bytes=None,
        )
        request_budget = (attempt.stage_request_json or {}).get("budget")
        max_attempts = (
            request_budget.get("max_attempts") if isinstance(request_budget, dict) else None
        )
        if not isinstance(max_attempts, int) or max_attempts not in {1, 2, 3}:
            raise HTTPException(status_code=409, detail="attempt_budget_unavailable")
        decision = retry_decision(
            completed_attempt_number=attempt.attempt_number,
            max_attempts=max_attempts,
            retry_class=RetryClass(payload.retry_class.value),
            reason_code=payload.reason_code,
            terminal_cause=ledger.terminal_cause,
            cleanup_acknowledged=True,
        )
        cleanup_proof = payload.resources.model_dump(mode="json")
        attempt.state = "failed"
        attempt.finished_at = observed_at
        attempt.exit_code = payload.exit_code
        attempt.retry_class = payload.retry_class.value
        attempt.reason_code = payload.reason_code
        attempt.cleanup_acknowledged_at = observed_at
        attempt.cleanup_proof_json = cleanup_proof
        attempt.cleanup_proof_digest = canonical_digest(cleanup_proof)
        attempt.version += 1
        if payload.stage_result is not None:
            attempt.result_manifest_json = payload.stage_result.model_dump(mode="json")
            attempt.result_manifest_digest = payload.stage_result_sha256
        await purge_live_preview(
            session,
            attempt_id=attempt.id,
            reason="attempt_failed",
        )
        if decision.retry:
            assert decision.delay_seconds is not None
            stage.state = "retry_wait"
            stage.next_attempt_at = observed_at + timedelta(seconds=decision.delay_seconds)
            stage.domain_outcome = None
            stage.reason_code = None
            stage.finished_at = None
        else:
            stage.state = "failed"
            stage.next_attempt_at = None
            stage.domain_outcome = None
            stage.reason_code = payload.reason_code
            stage.finished_at = observed_at
        stage.version += 1
        await _append_terminal_event(
            session,
            run=run,
            stage=stage,
            attempt=attempt,
            event_type="execution_attempt_failed",
            payload={
                "attempt_number": attempt.attempt_number,
                "reason_code": payload.reason_code,
                "retry": decision.retry,
                "retry_delay_seconds": decision.delay_seconds,
                "stage_state": stage.state,
            },
        )
        response = {"execution_attempt_id": str(attempt_id), "state": "failed"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route=route,
            request_id=request_id,
            payload=payload,
            response=response,
        )
        stage_duration = max((observed_at - stage.created_at).total_seconds(), 0)
        resource_class = _stage_resource_class(stage)
        await session.commit()
        PIPELINE_STAGE_DURATION_SECONDS.labels(
            resource_class=resource_class,
            result="retry_wait" if decision.retry else "failed",
        ).observe(stage_duration)
        return response


@router.post("/execution-attempts/{attempt_id}/failed")
async def report_attempt_failed(
    attempt_id: UUID,
    payload: ExecutionFailedV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _terminal_report(
        attempt_id=attempt_id,
        payload=payload,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        request_id=request_id,
        route="failed",
        state="failed",
        authorization=authorization,
    )


@router.post("/execution-attempts/{attempt_id}/cancel-ack")
async def report_attempt_cancelled(
    attempt_id: UUID,
    payload: ExecutionCancelAckV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if payload.teardown_observed is not True:
        raise HTTPException(status_code=409, detail="cleanup_not_observed")
    journal_payload = payload.model_dump(mode="json")
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="cancel-ack",
            payload=journal_payload,
        )
        if replay is not None:
            return replay
        stage = await session.get(PipelineStageRun, attempt.stage_run_id)
        if stage is None:
            raise HTTPException(status_code=404, detail="not_found")
        terminal_cause = (
            await session.execute(
                select(PipelineBudgetLedger.terminal_cause).where(
                    PipelineBudgetLedger.pipeline_run_id == stage.pipeline_run_id
                )
            )
        ).scalar_one_or_none()
        if terminal_cause is None or attempt.cancellation_requested_at is None:
            raise HTTPException(status_code=409, detail="cancellation_not_requested")
        active_uploads = (
            await session.execute(
                select(func.count())
                .select_from(ArtifactUploadSession)
                .where(
                    ArtifactUploadSession.execution_attempt_id == attempt.id,
                    ArtifactUploadSession.state.in_(
                        ["preparing", "uploading", "uploaded", "committing", "committed_ready"]
                    ),
                )
            )
        ).scalar_one()
        active_reservations = (
            await session.execute(
                select(func.count())
                .select_from(PipelineBudgetReservation)
                .where(
                    PipelineBudgetReservation.execution_attempt_id == attempt.id,
                    PipelineBudgetReservation.state == "active",
                    ~select(PipelineProviderDispatch.id).where(
                        PipelineProviderDispatch.reservation_id == PipelineBudgetReservation.id
                    ).exists(),
                )
            )
        ).scalar_one()
        if active_uploads:
            raise HTTPException(status_code=409, detail="cleanup_incomplete")
        if active_reservations:
            released = list(
                (
                    await session.execute(
                        select(PipelineBudgetReservation)
                        .where(
                            PipelineBudgetReservation.execution_attempt_id == attempt.id,
                            PipelineBudgetReservation.state == "active",
                            ~select(PipelineProviderDispatch.id).where(
                                PipelineProviderDispatch.reservation_id == PipelineBudgetReservation.id
                            ).exists(),
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            ledger = await session.get(
                PipelineBudgetLedger, stage.pipeline_run_id, with_for_update=True
            )
            if ledger is None:
                raise HTTPException(status_code=409, detail="cleanup_incomplete")
            for reservation in released:
                if reservation.kind == "provider":
                    ledger.provider_reserved_microusd -= reservation.reserved_amount
                elif reservation.kind == "gpu":
                    ledger.gpu_reserved_seconds -= reservation.reserved_amount
                else:
                    ledger.artifact_reserved_bytes -= reservation.reserved_amount
                reservation.state = "released"
                reservation.settled_at = datetime.now(UTC)
        attempt.state = "cancelled"
        attempt.finished_at = datetime.now(UTC)
        attempt.retry_class = "cancelled"
        attempt.reason_code = str(terminal_cause)
        attempt.cancellation_observed_at = payload.observed_at
        attempt.cancellation_outcome = payload.outcome
        cleanup_proof = payload.resources.model_dump(mode="json")
        attempt.cleanup_acknowledged_at = payload.observed_at
        attempt.cleanup_proof_json = cleanup_proof
        attempt.cleanup_proof_digest = canonical_digest(cleanup_proof)
        await purge_live_preview(session, attempt_id=attempt.id, reason="cancelled")
        stage.state = "cancelled"
        stage.reason_code = str(terminal_cause)
        stage.finished_at = payload.observed_at
        stage.version += 1
        await _ack_cancellation_outbox(
            session,
            attempt=attempt,
            observed_at=payload.observed_at,
            outcome=payload.outcome,
            cleanup_proof=cleanup_proof,
        )
        response = {"execution_attempt_id": str(attempt_id), "state": "cancelled"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="cancel-ack",
            request_id=request_id,
            payload=journal_payload,
            response=response,
        )
        cancellation_latency = max(
            (payload.observed_at - attempt.cancellation_requested_at).total_seconds(), 0
        )
        stage_duration = max((payload.observed_at - stage.created_at).total_seconds(), 0)
        resource_class = _stage_resource_class(stage)
        await session.commit()
        PIPELINE_CANCEL_LATENCY_SECONDS.labels(outcome=payload.outcome).observe(
            cancellation_latency
        )
        PIPELINE_STAGE_DURATION_SECONDS.labels(
            resource_class=resource_class,
            result="cancelled",
        ).observe(stage_duration)
        return response


@router.post("/execution-attempts/{attempt_id}/complete")
async def report_attempt_complete(
    attempt_id: UUID,
    payload: ExecutionCompleteV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="complete",
            payload=payload,
        )
        if replay is not None:
            return replay
        service = getattr(request.app.state, "execution_attempt_completion_service", None)
        if service is None:
            raise HTTPException(status_code=503, detail="artifact_committer_unavailable")
        committed_bytes = await service.complete(attempt=attempt, report=payload, session=session)
        observed_at = datetime.now(UTC)
        await _require_no_active_uploads(session, attempt_id=attempt.id)
        stage = await session.get(PipelineStageRun, attempt.stage_run_id, with_for_update=True)
        if stage is None:
            raise HTTPException(status_code=404, detail="not_found")
        run = await session.get(PipelineRun, stage.pipeline_run_id)
        if run is None:
            raise HTTPException(status_code=409, detail="pipeline_run_unavailable")
        await _settle_attempt_reservations(
            session,
            stage=stage,
            attempt=attempt,
            observed_at=observed_at,
            final_output_bytes=sum((committed_bytes or {}).values()),
        )
        attempt.state = "succeeded"
        attempt.exit_code = 0
        attempt.result_manifest_json = payload.stage_result.model_dump(mode="json")
        attempt.result_manifest_digest = payload.stage_result_sha256
        attempt.finished_at = observed_at
        attempt.version += 1
        stage.state = "succeeded"
        stage.domain_outcome = payload.stage_result.domain_outcome
        stage.reason_code = payload.stage_result.reason_code
        stage.next_attempt_at = None
        stage.finished_at = observed_at
        stage.version += 1
        generation = await session.get(
            PipelineLivePreviewGeneration, attempt.id, with_for_update=True
        )
        if generation is not None and generation.purged_at is None:
            generation.state = "handoff"
            generation.expires_at = generation_expiry(observed_at)
            generation.updated_at = observed_at
        await _append_terminal_event(
            session,
            run=run,
            stage=stage,
            attempt=attempt,
            event_type="execution_attempt_succeeded",
            payload={
                "attempt_number": attempt.attempt_number,
                "domain_outcome": payload.stage_result.domain_outcome,
                "reason_code": payload.stage_result.reason_code,
                "stage_result_sha256": payload.stage_result_sha256,
            },
        )
        response = {"execution_attempt_id": str(attempt_id), "state": "succeeded"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="complete",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        stage_duration = max((observed_at - stage.created_at).total_seconds(), 0)
        resource_class = _stage_resource_class(stage)
        await session.commit()
        for artifact_class, byte_count in (committed_bytes or {}).items():
            if byte_count:
                PIPELINE_ARTIFACT_BYTES_TOTAL.labels(artifact_class=artifact_class).inc(byte_count)
        PIPELINE_STAGE_DURATION_SECONDS.labels(
            resource_class=resource_class,
            result="succeeded",
        ).observe(stage_duration)
        return response


@router.post("/execution-attempts/{attempt_id}/worker-lost-cleanup-ack")
async def report_worker_lost_cleanup(
    attempt_id: UUID,
    payload: WorkerLostCleanupAckV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    journal_payload = payload.model_dump(mode="json")
    ctx = await _worker_auth(request, authorization, scope="worker:reap")
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=None,
                require_live_lease=False,
            )
            replay = await replay_or_conflict(
                session,
                attempt_id=attempt_id,
                route="worker-lost-cleanup-ack",
                request_id=request_id,
                payload=journal_payload,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        if replay is not None:
            return replay
        if attempt.lease_expires_at is None or attempt.lease_expires_at > datetime.now(UTC):
            raise HTTPException(status_code=409, detail="claim_not_expired")
        worker = await session.get(Worker, attempt.worker_id)
        if worker is None:
            raise HTTPException(status_code=409, detail="cleanup_authority_invalid")
        # verify_attempt_claim binds the worker journal to this expired claim.
        terminal_cause = (
            await session.execute(
                select(PipelineBudgetLedger.terminal_cause)
                .join(
                    PipelineStageRun,
                    PipelineStageRun.pipeline_run_id == PipelineBudgetLedger.pipeline_run_id,
                )
                .where(PipelineStageRun.id == attempt.stage_run_id)
            )
        ).scalar_one_or_none()
        attempt.state = "cancelled" if terminal_cause is not None else "lost"
        attempt.retry_class = (
            "cancelled" if terminal_cause is not None else "infrastructure_transient"
        )
        attempt.reason_code = "worker_lost_cleanup" if terminal_cause is not None else "worker_lost"
        attempt.finished_at = datetime.now(UTC)
        proof = payload.resources.model_dump(mode="json")
        attempt.cleanup_acknowledged_at = payload.observed_at
        attempt.cleanup_proof_json = proof
        attempt.cleanup_proof_digest = canonical_digest(proof)
        await purge_live_preview(session, attempt_id=attempt.id, reason="worker_lost")
        if terminal_cause is not None:
            attempt.cancellation_observed_at = payload.observed_at
            attempt.cancellation_outcome = "worker_lost_cleanup"
        active_uploads = (
            await session.execute(
                select(func.count())
                .select_from(ArtifactUploadSession)
                .where(
                    ArtifactUploadSession.execution_attempt_id == attempt.id,
                    ArtifactUploadSession.state.in_(
                        ["preparing", "uploading", "uploaded", "committing", "committed_ready"]
                    ),
                )
            )
        ).scalar_one()
        active_reservations = (
            await session.execute(
                select(func.count())
                .select_from(PipelineBudgetReservation)
                .where(
                    PipelineBudgetReservation.execution_attempt_id == attempt.id,
                    PipelineBudgetReservation.state == "active",
                    ~select(PipelineProviderDispatch.id).where(
                        PipelineProviderDispatch.reservation_id == PipelineBudgetReservation.id
                    ).exists(),
                )
            )
        ).scalar_one()
        if active_uploads:
            raise HTTPException(status_code=409, detail="cleanup_incomplete")
        if active_reservations:
            stage = await session.get(PipelineStageRun, attempt.stage_run_id)
            if stage is None:
                raise HTTPException(status_code=409, detail="cleanup_incomplete")
            ledger = await session.get(
                PipelineBudgetLedger, stage.pipeline_run_id, with_for_update=True
            )
            reservations = list(
                (
                    await session.execute(
                        select(PipelineBudgetReservation)
                        .where(
                            PipelineBudgetReservation.execution_attempt_id == attempt.id,
                            PipelineBudgetReservation.state == "active",
                            ~select(PipelineProviderDispatch.id).where(
                                PipelineProviderDispatch.reservation_id == PipelineBudgetReservation.id
                            ).exists(),
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            if ledger is None:
                raise HTTPException(status_code=409, detail="cleanup_incomplete")
            for reservation in reservations:
                if reservation.kind == "provider":
                    ledger.provider_reserved_microusd -= reservation.reserved_amount
                elif reservation.kind == "gpu":
                    ledger.gpu_reserved_seconds -= reservation.reserved_amount
                else:
                    ledger.artifact_reserved_bytes -= reservation.reserved_amount
                reservation.state = "released"
                reservation.settled_at = payload.observed_at
        if terminal_cause is not None:
            stage = await session.get(PipelineStageRun, attempt.stage_run_id)
            if stage is None:
                raise HTTPException(status_code=409, detail="cleanup_incomplete")
            stage.state = "cancelled"
            stage.reason_code = str(terminal_cause)
            stage.finished_at = payload.observed_at
            stage.version += 1
            await _ack_cancellation_outbox(
                session,
                attempt=attempt,
                observed_at=payload.observed_at,
                outcome="worker_lost_cleanup",
                cleanup_proof=proof,
            )
            cancellation_requested_at = attempt.cancellation_requested_at
            if cancellation_requested_at is None:
                raise HTTPException(status_code=409, detail="cancellation_not_requested")
            cancellation_latency = max(
                (payload.observed_at - cancellation_requested_at).total_seconds(), 0
            )
            stage_duration = max((payload.observed_at - stage.created_at).total_seconds(), 0)
            resource_class = _stage_resource_class(stage)
        response = {"execution_attempt_id": str(attempt_id), "state": attempt.state}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="worker-lost-cleanup-ack",
            request_id=request_id,
            payload=journal_payload,
            response=response,
        )
        await session.commit()
        if terminal_cause is not None:
            PIPELINE_CANCEL_LATENCY_SECONDS.labels(outcome="worker_lost_cleanup").observe(
                cancellation_latency
            )
            PIPELINE_STAGE_DURATION_SECONDS.labels(
                resource_class=resource_class,
                result="cancelled",
            ).observe(stage_duration)
        return response


async def _read_input(
    *,
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    file_index: int | None,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    if_match: str,
    range_header: str | None,
    authorization: str | None,
) -> Response:
    ctx = await _worker_auth(request, authorization, scope="worker:read_inputs")
    async with request.app.state.session_factory() as session:
        try:
            await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
    service: ArtifactReadServiceV1 | None = getattr(
        request.app.state, "artifact_read_service", None
    )
    if service is None:
        raise HTTPException(status_code=503, detail="artifact_read_unavailable")
    if file_index is None:
        return await service.read_manifest(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
            if_match=if_match,
        )
    return await service.read_file(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=file_index,
        if_match=if_match,
        range_header=range_header,
    )


@router.get(
    "/api/v1/internal/execution-attempts/{attempt_id}/input-bindings/"
    "{binding_name}/items/{item_key}/manifest"
)
async def read_input_manifest(
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    if_match: Annotated[str, Header(alias="If-Match")],
    authorization: str | None = Header(default=None),
) -> Response:
    return await _read_input(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=None,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        if_match=if_match,
        range_header=None,
        authorization=authorization,
    )


@router.get(
    "/api/v1/internal/execution-attempts/{attempt_id}/input-bindings/"
    "{binding_name}/items/{item_key}/files/{file_index}"
)
async def read_input_file(
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    file_index: int,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    if_match: Annotated[str, Header(alias="If-Match")],
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    authorization: str | None = Header(default=None),
) -> Response:
    if file_index < 0:
        raise HTTPException(status_code=404, detail="not_found")
    return await _read_input(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=file_index,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        if_match=if_match,
        range_header=range_header,
        authorization=authorization,
    )


async def _output_context(
    *,
    attempt_id: UUID,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    authorization: str | None,
) -> tuple[ExecutionAttempt, FinalOutputServiceV1]:
    ctx = await _worker_auth(request, authorization, scope="worker:write_outputs")
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        if attempt.cancellation_requested_at is not None:
            raise HTTPException(status_code=409, detail="cancellation_requested")
    service: FinalOutputServiceV1 | None = getattr(request.app.state, "final_output_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="artifact_committer_unavailable")
    return attempt, service


async def _checkpoint_context(
    *,
    attempt_id: UUID,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    authorization: str | None,
    allow_cancelling: bool = False,
) -> tuple[ExecutionAttempt, CheckpointServiceV1]:
    ctx = await _worker_auth(request, authorization, scope="worker:write_outputs")
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        if attempt.cancellation_requested_at is not None and not allow_cancelling:
            raise HTTPException(status_code=409, detail="cancellation_requested")
    service: CheckpointServiceV1 | None = getattr(request.app.state, "checkpoint_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="artifact_committer_unavailable")
    return attempt, service


@router.post("/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions")
async def prepare_checkpoint(
    attempt_id: UUID,
    payload: CheckpointPrepareRequestV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=payload.cancel_drain,
    )
    if payload.cancel_drain:
        async with request.app.state.session_factory() as session:
            stage = await session.get(PipelineStageRun, attempt.stage_run_id)
            terminal_cause = None
            if stage is not None:
                terminal_cause = await session.scalar(
                    select(PipelineBudgetLedger.terminal_cause).where(
                        PipelineBudgetLedger.pipeline_run_id == stage.pipeline_run_id
                    )
                )
        if attempt.cancellation_requested_at is None or terminal_cause != "user_cancel":
            raise HTTPException(status_code=409, detail="checkpoint_cancel_drain_forbidden")
    return await service.prepare(attempt=attempt, request_id=request_id, request=payload)


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions/{session_id}/commit"
)
async def commit_checkpoint_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputSessionCommitV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=True,
    )
    return await _commit_artifact_operation(
        service,
        commit_kind="checkpoint",
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions/{session_id}/renew"
)
async def renew_checkpoint_token(
    attempt_id: UUID,
    session_id: UUID,
    payload: UploadTokenRenewV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=True,
    )
    return await service.renew(attempt=attempt, session_id=session_id, request=payload)


@router.put(
    "/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions/"
    "{session_id}/files/{file_index}/parts/{part_number}"
)
async def put_checkpoint_part(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    part_number: int,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    content_sha256: Annotated[str, Header(alias="X-Loom-Content-Sha256")],
    content_length: Annotated[int, Header(alias="Content-Length", ge=0)],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if file_index < 0 or part_number < 1:
        raise HTTPException(status_code=400, detail="invalid_part")
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=True,
    )
    return await service.put_part(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        part_number=part_number,
        request_id=request_id,
        upload_token=upload_token,
        content_sha256=content_sha256,
        content_length=content_length,
        body=request.stream(),
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions/"
    "{session_id}/files/{file_index}/complete"
)
async def complete_checkpoint_file(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    payload: FinalOutputFileCompleteV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=True,
    )
    return await service.complete_file(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions/{session_id}/abort"
)
async def abort_checkpoint_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputAbortV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _checkpoint_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
        allow_cancelling=True,
    )
    return await service.abort(
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        request=payload,
    )


@router.post("/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions")
async def prepare_final_output(
    attempt_id: UUID,
    payload: FinalOutputPrepareRequestV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.prepare(attempt=attempt, request_id=request_id, request=payload)


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/renew"
)
async def renew_final_output(
    attempt_id: UUID,
    session_id: UUID,
    payload: UploadTokenRenewV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.renew(attempt=attempt, session_id=session_id, request=payload)


@router.put(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/"
    "{session_id}/files/{file_index}/parts/{part_number}"
)
async def put_final_output_part(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    part_number: int,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    content_sha256: Annotated[str, Header(alias="X-Loom-Content-Sha256")],
    content_length: Annotated[int, Header(alias="Content-Length", ge=0)],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if file_index < 0 or part_number < 1:
        raise HTTPException(status_code=400, detail="invalid_part")
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.put_part(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        part_number=part_number,
        request_id=request_id,
        upload_token=upload_token,
        content_sha256=content_sha256,
        content_length=content_length,
        body=request.stream(),
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/"
    "{session_id}/files/{file_index}/complete"
)
async def complete_final_output_file(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    payload: FinalOutputFileCompleteV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.complete_file(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/commit"
)
async def commit_final_output_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputSessionCommitV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await _commit_artifact_operation(
        service,
        commit_kind="final_output",
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/abort"
)
async def abort_final_output_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputAbortV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.abort(
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        request=payload,
    )
