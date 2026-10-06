"""Credential-free Pod broker and durable output upload routes."""

from __future__ import annotations

import hashlib
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from starlette.requests import HTTPConnection
from starlette.responses import Response, StreamingResponse

from loom.db.schema import ServiceExecutionLease, ServiceExecutionTarget, Trial
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.llm_call_ledger import read_service_execution_llm_calls as read_lease_calls
from loom.pipeline.artifact_commit import ArtifactCommitError
from loom_control_plane.service_execution_output import (
    ResolvedServiceExecutionHandoff,
    ServiceExecutionBrokerError,
    ServiceExecutionFileCompleteV1,
    ServiceExecutionOutputCommitV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionPeerV1,
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
    read_entry_digest,
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
                "step_id": "agent",
                "items": rows,
            }
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc


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


@router.post("/outputs/prepare", status_code=201)
async def prepare_service_execution_output(
    payload: ServiceExecutionOutputPrepareV1,
    request: Request,
) -> dict[str, Any]:
    lease = await _authorize(request, payload)
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
    lease = await _authorize(request, identity)
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
    lease = await _authorize(request, payload)
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


@router.post("/outputs/{session_id}/commit")
async def commit_service_execution_output(
    session_id: UUID,
    payload: ServiceExecutionOutputCommitV1,
    request: Request,
    upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    lease = await _authorize(request, payload)
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
    digest = await read_entry_digest(store, bucket=bucket, entry=entry)
    if digest is None:
        raise HTTPException(status_code=404, detail="harness_cache_miss")
    try:
        facts = await store.stat_object(bucket=bucket, key=entry.archive_key)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="harness_cache_miss") from exc
    if not 0 < facts.content_length <= entry.max_bytes:
        raise HTTPException(status_code=404, detail="harness_cache_miss")
    return StreamingResponse(
        store.stream_object(bucket=bucket, key=entry.archive_key, chunk_size=1024 * 1024),
        media_type="application/gzip",
        headers={"Content-Length": str(facts.content_length), "X-Loom-Content-SHA256": digest},
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
