"""Credential-free Pod broker and durable output upload routes."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import HTTPConnection
from starlette.responses import Response, StreamingResponse

from loom.db.schema import (
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TerminusAgentExecution,
    TerminusAgentRunAttempt,
    Trial,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.llm_call_ledger import read_service_execution_llm_calls as read_lease_calls
from loom.pipeline.artifact_commit import ArtifactCommitError
from loom.service_execution_live import LiveTrajectoryError, append_live_events
from loom.task_image_archives import PreparedImageArchiveV1
from loom.trajectory.upload_observation import UploadSpan, observe_upload
from loom_control_plane.service_execution_output import (
    ResolvedServiceExecutionHandoff,
    ServiceExecutionBrokerError,
    ServiceExecutionFileCompleteV1,
    ServiceExecutionOutputCommitV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionPeerV1,
    ServiceExecutionSmallFilesV1,
    ServiceExecutionTokenRequestV1,
    VerifiedExecutionPod,
    authorize_service_execution_peer,
    mint_service_execution_peer_token,
    resolve_service_execution_handoff,
    resolve_service_execution_input,
)
from loom_llm_gateway.harness_cache import (
    HarnessCacheEntry,
    HarnessCacheError,
    harness_cache_entry,
    read_entry,
    store_entry,
)

router = APIRouter(prefix="/internal/service-execution", tags=["service-execution"])
LeaseIdHeader = Annotated[UUID, Header(alias="X-Loom-Execution-Lease-Id")]
GenerationHeader = Annotated[int, Header(alias="X-Loom-Execution-Generation", gt=0)]
RoleHeader = Annotated[str, Header(alias="X-Loom-Execution-Role")]
UploadTokenHeader = Annotated[str, Header(alias="X-Loom-Upload-Token", min_length=32)]
ContentSha256Header = Annotated[
    str, Header(alias="X-Loom-Content-SHA256", pattern=r"^sha256:[0-9a-f]{64}$")
]


class TerminusReclaimRequestV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-terminus-reclaim.v1"] = (
        "loom.service-execution-terminus-reclaim.v1"
    )


class TrajectoryEventsRequestV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-trajectory-events.v1"] = (
        "loom.service-execution-trajectory-events.v1"
    )
    events: list[dict[str, Any]] = Field(min_length=1, max_length=500)


class TerminusCheckpointRequestV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-terminus-checkpoint.v1"] = (
        "loom.service-execution-terminus-checkpoint.v1"
    )
    execution_id: UUID
    run_attempt_id: UUID
    episode: int = Field(ge=1)
    active_role: Literal["student", "teacher"]
    last_call_ordinal: int = Field(ge=0)
    last_seq: int = Field(ge=0)
    tmux_session_id: str | None = Field(default=None, max_length=255)
    attempt_deadline_wall_clock: datetime


def _peer_ip(request: HTTPConnection) -> str | None:
    return request.client.host if request.client else None


def _peer(lease_id: UUID, generation: int, role: str) -> ServiceExecutionPeerV1:
    try:
        return ServiceExecutionPeerV1(
            lease_id=lease_id,
            generation=generation,
            execution_role=role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="execution_identity_invalid") from exc


def _broker_http(exc: ServiceExecutionBrokerError) -> HTTPException:
    status = (
        503
        if exc.reason
        in {
            "workload_identity_not_observed",
            "execution_target_unavailable",
            "execution_pod_review_unavailable",
        }
        else 409
    )
    if exc.reason in {"peer_ip_invalid", "execution_pod_identity_invalid"}:
        status = 403
    return HTTPException(status_code=status, detail=exc.reason)


def _artifact_http(exc: ArtifactCommitError) -> HTTPException:
    if exc.reason in {"upload_token_invalid", "upload_token_expired"}:
        return HTTPException(status_code=403, detail=exc.reason)
    if "object" in exc.reason or "multipart" in exc.reason:
        return HTTPException(status_code=503, detail=exc.reason)
    return HTTPException(status_code=409, detail=exc.reason)


async def _verified_pod(
    request: HTTPConnection, identity: ServiceExecutionPeerV1
) -> VerifiedExecutionPod | None:
    # Release the DB connection before waiting on a regional API or IAM refresh.
    # Final authorization uses a separate transaction and current lease fences.
    async with request.app.state.session_factory() as session:
        lease = await session.get(ServiceExecutionLease, identity.lease_id)
        target = await session.get(ServiceExecutionTarget, lease.target_id) if lease else None
        if target is None or not target.spec_json.get("pod_identity_audience"):
            return None
        spec = dict(target.spec_json)
    authorization = request.headers.get("authorization", "")
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token:
        raise ServiceExecutionBrokerError("execution_pod_identity_invalid")
    reviewer = getattr(request.app.state, "execution_pod_reviewer", None)
    if reviewer is None:
        raise ServiceExecutionBrokerError("execution_pod_review_unavailable")
    return cast(
        VerifiedExecutionPod,
        await reviewer.review(
            cluster_scope_id=spec["cluster_scope_id"],
            namespace=spec["namespace_name"],
            service_account=spec.get("service_account_name", "loom-execution-attempt"),
            audience=spec["pod_identity_audience"],
            token=token,
        ),
    )


async def _authorize(
    request: HTTPConnection,
    identity: ServiceExecutionPeerV1,
    *,
    purpose: Literal["token", "input", "output"] = "output",
) -> ServiceExecutionLease:
    try:
        verified_pod = await _verified_pod(request, identity)
        async with request.app.state.session_factory() as session:
            lease = await authorize_service_execution_peer(
                session,
                peer_ip=_peer_ip(request),
                verified_pod=verified_pod,
                identity=identity,
                purpose=purpose,
            )
            session.expunge(lease)
            return lease
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc


@router.get("/llm-calls")
async def read_service_execution_llm_calls(
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    execution_role: RoleHeader,
) -> dict[str, Any]:
    """Read only this Pod's active attempt, excluding previous Trial retries."""
    identity = _peer(lease_id, generation, execution_role)
    if identity.execution_role != "attempt" or request.query_params:
        raise HTTPException(status_code=403, detail="execution_ledger_scope_invalid")
    try:
        verified_pod = await _verified_pod(request, identity)
        async with request.app.state.session_factory() as session:
            lease = await authorize_service_execution_peer(
                session, peer_ip=_peer_ip(request), verified_pod=verified_pod,
                identity=identity, purpose="token", lock=True,
            )
            trial = await session.get(Trial, lease.trial_id)
            if trial is None or trial.team_id != lease.team_id or trial.attempt_count != lease.attempt:
                raise ServiceExecutionBrokerError("execution_generation_fenced")
            rows = await read_lease_calls(session, lease)
            return {
                "trial_id": str(lease.trial_id), "team_id": str(lease.team_id),
                "step_id": "agent", "task_id": trial.task_id,
                "agent_name": (trial.config or {}).get("agent_name"),
                "items": rows,
            }
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc


async def _authorized_attempt_lease(
    request: Request,
    identity: ServiceExecutionPeerV1,
    session: AsyncSession,
) -> tuple[ServiceExecutionLease, Trial]:
    if identity.execution_role != "attempt":
        raise HTTPException(status_code=403, detail="terminus_execution_scope_invalid")
    try:
        verified_pod = await _verified_pod(request, identity)
        lease = await authorize_service_execution_peer(
            session,
            peer_ip=_peer_ip(request),
            verified_pod=verified_pod,
            identity=identity,
            purpose="token",
            lock=True,
        )
        trial = await session.get(Trial, lease.trial_id, with_for_update=True)
        if (
            trial is None
            or trial.team_id != lease.team_id
            or trial.attempt_count != lease.attempt
            or trial.state not in {"claimed", "running"}
        ):
            raise ServiceExecutionBrokerError("execution_generation_fenced")
        return lease, trial
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc


@router.post("/trajectory-events")
async def append_service_execution_trajectory_events(
    payload: TrajectoryEventsRequestV1, request: Request,
) -> dict[str, Any]:
    """Pod-bound, current-attempt preview; canonical events remain materializer-owned."""
    async with request.app.state.session_factory() as session:
        lease, trial = await _authorized_attempt_lease(request, payload, session)
        try:
            result = await append_live_events(session, lease=lease, trial=trial, events=payload.events)
        except LiveTrajectoryError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.code) from exc
        await session.commit()
        return result


@router.post("/terminus/reclaim")
async def reclaim_service_execution_terminus(
    payload: TerminusReclaimRequestV1,
    request: Request,
) -> dict[str, Any]:
    from loom_control_plane.terminus_recovery import reclaim_terminus_execution

    async with request.app.state.session_factory() as session:
        lease, _trial = await _authorized_attempt_lease(request, payload, session)
        state = await reclaim_terminus_execution(
            session,
            trial_id=lease.trial_id,
            step_id="agent",
        )
        await session.commit()
        return state.model_dump(mode="json")


@router.post("/terminus/episode-checkpoints")
async def checkpoint_service_execution_terminus(
    payload: TerminusCheckpointRequestV1,
    request: Request,
) -> dict[str, Any]:
    from loom_control_plane.terminus_recovery import write_episode_checkpoint

    deadline = payload.attempt_deadline_wall_clock
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise HTTPException(status_code=422, detail="attempt_deadline_timezone_required")
    deadline = deadline.astimezone(UTC)
    if datetime.now(UTC) >= deadline:
        raise HTTPException(status_code=409, detail="attempt_deadline_elapsed")
    async with request.app.state.session_factory() as session:
        lease, _trial = await _authorized_attempt_lease(request, payload, session)
        execution = await session.get(TerminusAgentExecution, payload.execution_id)
        attempt = await session.get(TerminusAgentRunAttempt, payload.run_attempt_id)
        if (
            execution is None
            or execution.trial_id != lease.trial_id
            or execution.step_id != "agent"
            or attempt is None
            or attempt.execution_id != execution.id
            or attempt.state != "running"
        ):
            raise HTTPException(status_code=409, detail="terminus_execution_identity_invalid")
        row = await write_episode_checkpoint(
            session,
            execution_id=execution.id,
            run_attempt_id=attempt.id,
            episode=payload.episode,
            active_role=payload.active_role,
            last_call_ordinal=payload.last_call_ordinal,
            last_seq=payload.last_seq,
            tmux_session_id=payload.tmux_session_id,
        )
        if datetime.now(UTC) >= deadline:
            await session.rollback()
            raise HTTPException(status_code=409, detail="attempt_deadline_elapsed")
        await session.commit()
        return {"id": str(row.id), "version": row.version, "checksum": row.checksum}


@router.post("/token")
async def issue_service_execution_token(
    payload: ServiceExecutionTokenRequestV1,
    request: Request,
) -> dict[str, Any]:
    try:
        verified_pod = await _verified_pod(request, payload)
        async with request.app.state.session_factory() as session:
            lease = await authorize_service_execution_peer(
                session,
                peer_ip=_peer_ip(request),
                verified_pod=verified_pod,
                identity=payload,
                lock=True,
            )
            token, expires_at, step_jwt_id = await mint_service_execution_peer_token(
                session,
                lease=lease,
                ttl_seconds=payload.ttl_seconds,
                attempt_deadline_wall_clock=payload.attempt_deadline_wall_clock,
                signing_key=request.app.state.settings.step_jwt_signing_key.get_secret_value(),
            )
            await session.commit()
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc
    return {
        "schema_version": "loom.service-execution-token.v1",
        "token": token,
        "expires_at": expires_at.isoformat(),
        "step_jwt_id": str(step_jwt_id),
    }


@router.get("/inputs/manifest")
async def get_service_execution_input_manifest(
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> Response:
    identity = _peer(lease_id, generation, role)
    lease = await _authorize(request, identity, purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            resolved = await resolve_service_execution_input(
                session,
                lease=lease,
                store=request.app.state.artifact_store,
                artifacts_bucket=request.app.state.settings.artifacts_bucket,
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
    return Response(
        content=resolved.manifest_body,
        media_type="application/json",
        headers={
            "X-Loom-Content-SHA256": "sha256:" + hashlib.sha256(resolved.manifest_body).hexdigest()
        },
    )


@router.get("/inputs/files/{file_index}")
async def get_service_execution_input_file(
    file_index: int,
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> StreamingResponse:
    identity = _peer(lease_id, generation, role)
    lease = await _authorize(request, identity, purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            resolved = await resolve_service_execution_input(
                session,
                lease=lease,
                store=request.app.state.artifact_store,
                artifacts_bucket=request.app.state.settings.artifacts_bucket,
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
    if file_index < 0 or file_index >= len(resolved.manifest.files):
        raise HTTPException(status_code=404, detail="task_input_file_not_found")
    item = resolved.manifest.files[file_index]
    key = resolved.prefix + item.relative_path
    try:
        facts = await request.app.state.artifact_store.stat_object(
            bucket=resolved.bucket,
            key=key,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="task_input_object_unavailable") from exc
    if facts.content_length != item.size_bytes:
        raise HTTPException(status_code=409, detail="task_input_object_drift")
    return StreamingResponse(
        request.app.state.artifact_store.stream_object(
            bucket=resolved.bucket,
            key=key,
            chunk_size=1024 * 1024,
        ),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(item.size_bytes),
            "X-Loom-Content-SHA256": item.sha256,
        },
    )


async def _prepared_image_archives(request: Request, lease_id: UUID, generation: int, role: str) -> dict[str, PreparedImageArchiveV1]:
    from loom_control_plane.service_execution_output import resolve_service_execution_image_archives
    lease = await _authorize(request, _peer(lease_id, generation, role), purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            return await resolve_service_execution_image_archives(session, lease=lease,
                artifacts_bucket=request.app.state.settings.artifacts_bucket)
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc


@router.get("/inputs/images/manifest")
async def get_prepared_image_manifest(request: Request, lease_id: LeaseIdHeader,
                                      generation: GenerationHeader, role: RoleHeader) -> dict[str, dict[str, str | int]]:
    archives = await _prepared_image_archives(request, lease_id, generation, role)
    # Object locations and version tokens remain on the trusted storage side.
    return {name: {"image_ref": item.image_ref, "size_bytes": item.object.size_bytes,
                   "sha256": "sha256:" + item.object.content_sha256}
            for name, item in archives.items()}


@router.get("/inputs/images/{service}")
async def get_prepared_image_archive(service: str, request: Request, lease_id: LeaseIdHeader,
                                     generation: GenerationHeader, role: RoleHeader) -> StreamingResponse:
    archives = await _prepared_image_archives(request, lease_id, generation, role)
    if service not in archives:
        raise HTTPException(status_code=404, detail="prepared_image_service_not_found")
    descriptor = archives[service].object
    store = request.app.state.artifact_store
    try:
        facts = await store.stat_object(bucket=descriptor.bucket, key=descriptor.object_key,
                                        version_id=descriptor.version_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="prepared_image_object_unavailable") from exc
    if (facts.version_id != descriptor.version_id or facts.content_length != descriptor.size_bytes
            or facts.checksum_sha256 != "sha256:" + descriptor.content_sha256):
        raise HTTPException(status_code=409, detail="prepared_image_object_drift")
    return StreamingResponse(store.stream_object(bucket=descriptor.bucket, key=descriptor.object_key,
        version_id=descriptor.version_id, chunk_size=1024 * 1024), media_type="application/octet-stream",
        headers={"Content-Length": str(descriptor.size_bytes),
                 "X-Loom-Content-SHA256": "sha256:" + descriptor.content_sha256})


async def _resolved_handoff(
    request: Request, lease_id: UUID, generation: int, role: str,
) -> ResolvedServiceExecutionHandoff:
    identity = _peer(lease_id, generation, role)
    if identity.execution_role != "verifier":
        raise HTTPException(status_code=403, detail="verifier_handoff_scope_invalid")
    lease = await _authorize(request, identity, purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            return await resolve_service_execution_handoff(session, lease=lease)
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc


@router.get("/inputs/handoff/manifest")
async def get_service_execution_handoff_manifest(
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> Response:
    resolved = await _resolved_handoff(request, lease_id, generation, role)
    return Response(
        content=resolved.manifest_body,
        media_type="application/json",
        headers={
            "X-Loom-Content-SHA256": "sha256:" + hashlib.sha256(resolved.manifest_body).hexdigest()
        },
    )


@router.get("/inputs/handoff/files/{file_index}")
async def get_service_execution_handoff_file(
    file_index: int,
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> StreamingResponse:
    resolved = await _resolved_handoff(request, lease_id, generation, role)
    if file_index < 0 or file_index >= len(resolved.manifest.files):
        raise HTTPException(status_code=404, detail="verifier_handoff_file_not_found")
    item = resolved.manifest.files[file_index]
    store = request.app.state.output_source_store
    bucket = request.app.state.output_source_bucket
    key = resolved.source_keys[file_index]
    try:
        facts = await store.stat_object(bucket=bucket, key=key)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="verifier_handoff_object_unavailable") from exc
    if facts.content_length != item.size_bytes:
        raise HTTPException(status_code=409, detail="verifier_handoff_object_drift")
    return StreamingResponse(
        store.stream_object(bucket=bucket, key=key, chunk_size=1024 * 1024),
        media_type="application/octet-stream",
        headers={"Content-Length": str(item.size_bytes), "X-Loom-Content-SHA256": item.sha256},
    )


@asynccontextmanager
async def _observed_output(
    request: Request, identity: ServiceExecutionPeerV1, operation: str, *,
    session_id: UUID | None = None, file_index: int | None = None, part_number: int | None = None,
) -> AsyncIterator[ServiceExecutionLease]:
    # A caller trace is correlation only; our own request ID is always fresh.
    raw_id = request.headers.get("X-Loom-Upload-Request-Id", "")
    try:
        broker_id = UUID(raw_id) if len(raw_id) == 36 else None
    except ValueError:
        broker_id = None
    with observe_upload(
        "source", lease_id=identity.lease_id, generation=identity.generation,
        session_id=session_id, request_id=uuid4(), broker_request_id=broker_id,
        file_index=file_index, part_number=part_number,
    ), UploadSpan("gateway", operation) as span:
        try:
            with UploadSpan("authorization", operation):
                lease = await _authorize(request, identity)
            with UploadSpan("processing", operation):
                yield lease
            span.status_code = 201 if operation == "prepare" else 200
        except HTTPException as exc:
            span.status_code = exc.status_code
            raise


@router.post("/outputs/prepare", status_code=201)
async def prepare_service_execution_output(
    payload: ServiceExecutionOutputPrepareV1,
    request: Request,
) -> dict[str, Any]:
    async with _observed_output(request, payload, "prepare") as lease:
        try:
            return cast(
                dict[str, Any],
                await request.app.state.service_execution_output_service.prepare(
                    lease=lease,
                    request=payload,
                ),
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
        except ArtifactCommitError as exc:
            raise _artifact_http(exc) from exc


@router.put("/outputs/{session_id}/files/{file_index}/parts/{part_number}")
async def upload_service_execution_output_part(
    session_id: UUID,
    file_index: int,
    part_number: int,
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
    upload_token: UploadTokenHeader,
    content_sha256: ContentSha256Header,
    content_length: int = Header(alias="Content-Length", ge=0),
) -> dict[str, Any]:
    identity = _peer(lease_id, generation, role)
    async with _observed_output(request, identity, "part", session_id=session_id, file_index=file_index, part_number=part_number) as lease:
        try:
            return cast(
                dict[str, Any],
                await request.app.state.service_execution_output_service.put_part(
                    lease=lease,
                    session_id=session_id,
                    file_index=file_index,
                    part_number=part_number,
                    content_length=content_length,
                    content_sha256=content_sha256,
                    upload_token=upload_token,
                    body=request.stream(),
                ),
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
        except ArtifactCommitError as exc:
            raise _artifact_http(exc) from exc


@router.post("/outputs/{session_id}/files/{file_index}/complete")
async def complete_service_execution_output_file(
    session_id: UUID,
    file_index: int,
    payload: ServiceExecutionFileCompleteV1,
    request: Request,
    upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    async with _observed_output(request, payload, "complete", session_id=session_id, file_index=file_index) as lease:
        try:
            return cast(
                dict[str, Any],
                await request.app.state.service_execution_output_service.complete_file(
                    lease=lease,
                    session_id=session_id,
                    file_index=file_index,
                    ordered_parts=payload.ordered_parts,
                    upload_token=upload_token,
                ),
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
        except ArtifactCommitError as exc:
            raise _artifact_http(exc) from exc


@router.post("/outputs/{session_id}/small-files")
async def upload_service_execution_small_files(
    session_id: UUID, request: Request, upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    # Bound bytes before JSON/base64 parsing, including chunked requests.
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 2 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="small_file_batch_too_large")
        body.extend(chunk)
    try:
        payload = ServiceExecutionSmallFilesV1.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail="small_file_batch_invalid") from exc
    async with _observed_output(request, payload, "small_files", session_id=session_id) as lease:
        try:
            return cast(dict[str, Any], await request.app.state.service_execution_output_service.small_files(
                lease=lease, session_id=session_id, request=payload, upload_token=upload_token,
            ))
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
        except ArtifactCommitError as exc:
            raise _artifact_http(exc) from exc


@router.post("/outputs/{session_id}/commit")
async def commit_service_execution_output(
    session_id: UUID,
    payload: ServiceExecutionOutputCommitV1,
    request: Request,
    upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    async with _observed_output(request, payload, "commit", session_id=session_id) as lease:
        try:
            return cast(
                dict[str, Any],
                await request.app.state.service_execution_output_service.commit(
                    lease=lease,
                    session_id=session_id,
                    upload_token=upload_token,
                ),
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
        except ArtifactCommitError as exc:
            raise _artifact_http(exc) from exc


__all__ = ["router"]


async def _harness_cache_entry(request: Request, identity: ServiceExecutionPeerV1) -> HarnessCacheEntry:
    """The lease and its frozen plan alone determine the entry (#2310)."""
    lease = await _authorize(request, identity, purpose="input")
    try:
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        return harness_cache_entry(team_id=lease.team_id, plan=plan)
    except (HarnessCacheError, ValueError) as exc:
        raise HTTPException(status_code=403, detail="harness_cache_not_declared") from exc


@router.get("/harness-cache")
async def get_harness_cache(
    request: Request, lease_id: LeaseIdHeader, generation: GenerationHeader, role: RoleHeader,
) -> StreamingResponse:
    entry = await _harness_cache_entry(request, _peer(lease_id, generation, role))
    store, bucket = request.app.state.artifact_store, request.app.state.settings.artifacts_bucket
    published = await read_entry(store, bucket=bucket, entry=entry)
    if published is None:
        raise HTTPException(status_code=404, detail="harness_cache_miss")
    try:
        facts = await store.stat_object(bucket=bucket, key=published.archive_key)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="harness_cache_miss") from exc
    if not 0 < facts.content_length <= entry.max_bytes:
        raise HTTPException(status_code=404, detail="harness_cache_miss")
    return StreamingResponse(
        store.stream_object(bucket=bucket, key=published.archive_key, chunk_size=1024 * 1024),
        media_type="application/gzip",
        headers={"Content-Length": str(facts.content_length), "X-Loom-Content-SHA256": published.digest},
    )


@router.put("/harness-cache", status_code=204)
async def put_harness_cache(
    request: Request, lease_id: LeaseIdHeader, generation: GenerationHeader, role: RoleHeader,
    content_sha256: ContentSha256Header,
) -> Response:
    entry = await _harness_cache_entry(request, _peer(lease_id, generation, role))
    try:
        size = int(request.headers.get("content-length", ""))
    except ValueError as exc:
        raise HTTPException(status_code=411, detail="harness_cache_size_invalid") from exc
    try:
        await store_entry(
            request.app.state.artifact_store, bucket=request.app.state.settings.artifacts_bucket,
            entry=entry, body=request.stream(), declared_size=size, declared_digest=content_sha256,
        )
    except HarnessCacheError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(status_code=204)
