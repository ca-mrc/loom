"""Credential-free workload broker and durable Pod-native output commit."""

from __future__ import annotations

import hashlib
import ipaddress
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1, ExecutionRuntimeResultV1
from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
    PartReceiptV1,
    ServiceExecutionOutputProducerV1,
    UploadAuthV1,
    UploadFilePlanV1,
    confined_relative_path,
)
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    MAX_INPUT_MANIFEST_BYTES,
    ServiceExecutionInputManifestV1,
    build_verifier_handoff_manifest,
    service_execution_input_binding,
    verifier_handoff_input,
    verifier_handoff_path,
)
from loom.trajectory.storage import ObjectStore
from loom_control_plane.service_execution import (
    VerifierHandoffUnavailableError,
    committed_handoff_files,
    record_committed_runtime_result,
)
from loom_control_plane.service_execution_task_snapshot import (
    ServiceExecutionTaskSnapshotError,
    resolve_service_execution_task_snapshot,
)

_MAX_FILES = 10_133
_MAX_TOKEN_TTL_SECONDS = 600
_RESULT_MAX_BYTES = 1024 * 1024


class ServiceExecutionBrokerError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ServiceExecutionPeerV1(_Strict):
    lease_id: UUID
    generation: int = Field(gt=0)
    execution_role: Literal["attempt", "verifier"]


class ServiceExecutionTokenRequestV1(ServiceExecutionPeerV1):
    ttl_seconds: int = Field(default=480, gt=0, le=_MAX_TOKEN_TTL_SECONDS)
    attempt_deadline_wall_clock: AwareDatetime | None = None


class ServiceExecutionOutputFileV1(_Strict):
    relative_path: str = Field(min_length=1, max_length=4096)
    media_type: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("relative_path")
    @classmethod
    def confined_path(cls, value: str) -> str:
        return confined_relative_path(value)


class ServiceExecutionOutputPrepareV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-output-prepare.v1"]
    request_id: UUID
    files: tuple[ServiceExecutionOutputFileV1, ...] = Field(min_length=1, max_length=_MAX_FILES)

    @model_validator(mode="after")
    def exact_inventory(self) -> ServiceExecutionOutputPrepareV1:
        paths = [item.relative_path for item in self.files]
        if paths[0] != "result.json" or len(paths) != len(set(paths)):
            raise ValueError("service execution output inventory is invalid")
        if paths != sorted(paths, key=lambda item: (item != "result.json", item.encode())):
            raise ValueError("service execution output inventory is not canonical")
        return self


class ServiceExecutionOutputCommitV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-output-commit.v1"]


class ServiceExecutionFileCompleteV1(ServiceExecutionPeerV1):
    schema_version: Literal["loom.service-execution-file-complete.v1"]
    ordered_parts: tuple[PartReceiptV1, ...]

    @field_validator("ordered_parts")
    @classmethod
    def contiguous_parts(cls, values: tuple[PartReceiptV1, ...]) -> tuple[PartReceiptV1, ...]:
        if [item.part_number for item in values] != list(range(1, len(values) + 1)):
            raise ValueError("service execution output parts are not contiguous")
        return values


@dataclass(frozen=True)
class ResolvedServiceExecutionInput:
    manifest: ServiceExecutionInputManifestV1
    manifest_body: bytes
    bucket: str
    prefix: str


def _s3_location(value: str) -> tuple[str, str]:
    if not value.startswith("s3://"):
        raise ServiceExecutionBrokerError("task_input_source_invalid")
    bucket, separator, key = value.removeprefix("s3://").partition("/")
    if not separator or not bucket or not key:
        raise ServiceExecutionBrokerError("task_input_source_invalid")
    return bucket, key


async def resolve_service_execution_input(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    store: ObjectStore,
    artifacts_bucket: str,
) -> ResolvedServiceExecutionInput:
    plan = _runtime_plan(lease)
    if plan.task_input is None:
        raise ServiceExecutionBrokerError("task_input_unavailable")
    try:
        row = await resolve_service_execution_task_snapshot(session, lease=lease)
    except ServiceExecutionTaskSnapshotError as exc:
        raise ServiceExecutionBrokerError(str(exc)) from exc
    if row.source is None:
        raise ServiceExecutionBrokerError("task_input_unavailable")
    try:
        binding = service_execution_input_binding(row.source_provenance)
    except ValueError as exc:
        raise ServiceExecutionBrokerError("task_input_identity_invalid") from exc
    if binding is None:
        raise ServiceExecutionBrokerError("task_input_unavailable")
    manifest_bucket, manifest_key = _s3_location(binding.manifest_uri)
    source_bucket, source_prefix = _s3_location(row.source)
    if (
        manifest_bucket != artifacts_bucket
        or source_bucket != artifacts_bucket
        or not source_prefix.endswith("/")
        or binding.manifest_sha256 != plan.task_input.manifest_sha256
        or binding.file_count != plan.task_input.file_count
        or binding.total_bytes != plan.task_input.total_bytes
    ):
        raise ServiceExecutionBrokerError("task_input_identity_drift")
    body = await store.get_object(bucket=manifest_bucket, key=manifest_key)
    if len(body) > MAX_INPUT_MANIFEST_BYTES:
        raise ServiceExecutionBrokerError("task_input_manifest_too_large")
    if "sha256:" + hashlib.sha256(body).hexdigest() != binding.manifest_sha256:
        raise ServiceExecutionBrokerError("task_input_manifest_drift")
    try:
        manifest = ServiceExecutionInputManifestV1.model_validate_json(body)
    except ValueError as exc:
        raise ServiceExecutionBrokerError("task_input_manifest_invalid") from exc
    if (
        manifest.canonical_bytes() != body
        or manifest.task_revision_sha256 != plan.task_revision_sha256
        or len(manifest.files) != binding.file_count
        or sum(item.size_bytes for item in manifest.files) != binding.total_bytes
    ):
        raise ServiceExecutionBrokerError("task_input_manifest_drift")
    return ResolvedServiceExecutionInput(
        manifest=manifest,
        manifest_body=body,
        bucket=source_bucket,
        prefix=source_prefix,
    )


@dataclass(frozen=True)
class ResolvedServiceExecutionHandoff:
    manifest: ServiceExecutionInputManifestV1
    manifest_body: bytes
    source_keys: tuple[str, ...]


async def resolve_service_execution_handoff(
    session: AsyncSession, *, lease: ServiceExecutionLease,
) -> ResolvedServiceExecutionHandoff:
    plan = _runtime_plan(lease)
    if plan.handoff_input is None or lease.parent_lease_id is None:
        raise ServiceExecutionBrokerError("verifier_handoff_unavailable")
    parent = await session.get(ServiceExecutionLease, lease.parent_lease_id)
    if parent is None or parent.trial_id != lease.trial_id or parent.attempt != lease.attempt:
        raise ServiceExecutionBrokerError("verifier_handoff_unavailable")
    try:
        files, keys = await committed_handoff_files(session, parent=parent)
    except VerifierHandoffUnavailableError as exc:
        raise ServiceExecutionBrokerError("verifier_handoff_unavailable") from exc
    try:
        manifest = build_verifier_handoff_manifest(
            task_revision_sha256=plan.task_revision_sha256, committed_files=files,
        )
    except ValueError as exc:
        raise ServiceExecutionBrokerError("verifier_handoff_unavailable") from exc
    if verifier_handoff_input(manifest) != plan.handoff_input:
        raise ServiceExecutionBrokerError("verifier_handoff_identity_drift")
    by_target = {verifier_handoff_path(path): key for (path, _, _), key in zip(files, keys, strict=True)}
    return ResolvedServiceExecutionHandoff(
        manifest=manifest,
        manifest_body=manifest.canonical_bytes(),
        source_keys=tuple(by_target[item.relative_path] for item in manifest.files),
    )


def _runtime_plan(lease: ServiceExecutionLease) -> ExecutionRuntimePlanV1:
    if lease.runtime_contract_json is None or lease.runtime_contract_sha256 is None:
        raise ServiceExecutionBrokerError("runtime_identity_unavailable")
    if canonical_digest(lease.runtime_contract_json) != lease.runtime_contract_sha256:
        raise ServiceExecutionBrokerError("runtime_identity_drift")
    try:
        return ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    except ValueError as exc:
        raise ServiceExecutionBrokerError("runtime_identity_invalid") from exc


def _defers_verification(plan: ExecutionRuntimePlanV1) -> bool:
    """A separate-mode attempt is graded later by its own verifier lease."""
    return plan.execution_role == "attempt" and plan.verifier_execution == "separate_execution"


def _validate_runtime_result(
    lease: ServiceExecutionLease,
    plan: ExecutionRuntimePlanV1,
    payload: bytes,
) -> ExecutionRuntimeResultV1:
    try:
        result = ExecutionRuntimeResultV1.model_validate(json.loads(payload))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ServiceExecutionBrokerError("runtime_result_invalid") from exc
    expected_roles = ["execution", plan.main.role]
    expected_roles.extend(item.role_name for item in plan.sidecars)
    if plan.verifier is not None:
        expected_roles.append("verifier")
    expected_phases = [*[item.role for item in plan.setup], plan.main.role]
    if plan.verifier is not None:
        expected_phases.append(plan.verifier.role)
    actual_phases = [item.role for item in result.phases]
    phases_match = actual_phases == expected_phases[: len(actual_phases)]
    declared_outputs = [
        (item.source_path, item.relative_path, item.kind, item.required)
        for item in plan.output_declarations
    ]
    reported_outputs = [
        (item.source_path, item.relative_path, item.kind, item.required) for item in result.outputs
    ]
    if result.status == "succeeded":
        phases_match = phases_match and len(actual_phases) == len(expected_phases)
    if (
        result.runtime_contract_sha256 != lease.runtime_contract_sha256
        or result.candidate_sha != plan.candidate_sha
        or result.task_revision_sha256 != plan.task_revision_sha256
        or result.command_identity_sha256 != plan.command_identity_sha256
        or result.execution_role != lease.execution_role
        or result.execution_class_id != lease.execution_class_id
        or result.task_image_ref != plan.task_image_ref
        or result.runtime_image_ref != plan.runtime_image_ref
        or result.runtime_binary_sha256 != plan.runtime_binary_sha256
        or list(result.container_roles) != expected_roles
        or not phases_match
        or reported_outputs != declared_outputs
        or (
            result.status == "succeeded"
            and not _defers_verification(plan)
            and any(item.kind == "verifier" for item in plan.output_declarations)
            and result.verifier_rewards is None
        )
        or (_defers_verification(plan) and result.verifier_rewards is not None)
        or any(
            stream.bytes_saved > plan.max_log_bytes_per_stream
            for phase in result.phases
            for stream in (phase.stdout, phase.stderr)
        )
    ):
        raise ServiceExecutionBrokerError("runtime_result_identity_drift")
    return result


@dataclass(frozen=True)
class VerifiedExecutionPod:
    """Gateway-local result of the bound cluster's native TokenReview."""

    cluster_scope_id: str
    namespace: str
    service_account: str
    pod_uid: str
    audience: str


async def authorize_service_execution_peer(
    session: AsyncSession,
    *,
    peer_ip: str | None = None,
    verified_pod: VerifiedExecutionPod | None = None,
    identity: ServiceExecutionPeerV1,
    now: datetime | None = None,
    lock: bool = False,
    purpose: Literal["token", "input", "output"] = "token",
) -> ServiceExecutionLease:
    """Bind native Pod identity or the legacy direct peer to the current lease."""

    # Authorize against the current lease even when an internal caller loaded
    # it earlier in the transaction. Never reuse stale identity-map fences.
    statement = (
        select(ServiceExecutionLease)
        .where(ServiceExecutionLease.id == identity.lease_id)
        .execution_options(populate_existing=True)
    )
    if lock:
        statement = statement.with_for_update()
    lease = (await session.execute(statement)).scalar_one_or_none()
    if lease is None:
        raise ServiceExecutionBrokerError("workload_identity_not_observed")
    target = await session.get(ServiceExecutionTarget, lease.target_id, populate_existing=True)
    audience = target.spec_json.get("pod_identity_audience") if target is not None else None
    if audience is not None:
        if lease.pod_uid is None:
            raise ServiceExecutionBrokerError("workload_identity_not_observed")
        if (
            verified_pod is None
            or target is None
            or verified_pod.cluster_scope_id != target.spec_json.get("cluster_scope_id")
            or verified_pod.namespace != lease.namespace_name
            or verified_pod.namespace != target.spec_json.get("namespace_name")
            or verified_pod.service_account
            != target.spec_json.get("service_account_name", "loom-execution-attempt")
            or verified_pod.audience != audience
            or verified_pod.pod_uid != lease.pod_uid
        ):
            raise ServiceExecutionBrokerError("execution_pod_identity_invalid")
    else:
        try:
            normalized_ip = str(ipaddress.ip_address(peer_ip or ""))
        except ValueError as exc:
            raise ServiceExecutionBrokerError("peer_ip_invalid") from exc
        if str(lease.pod_ip) != normalized_ip:
            raise ServiceExecutionBrokerError("workload_identity_not_observed")
    current_time = now or datetime.now(UTC)
    resource_identity_matches = (
        lease.resource_generation == identity.generation
        and lease.execution_role == identity.execution_role
        and lease.deleted_at is None
        and lease.pod_uid is not None
    )
    if not resource_identity_matches:
        raise ServiceExecutionBrokerError("execution_generation_fenced")
    if purpose in {"token", "input"} and (
        lease.generation != identity.generation
        or lease.deadline_at <= current_time
        or lease.revoked_at is not None
        or lease.observed_state not in {"creating", "running", "finalizing"}
    ):
        raise ServiceExecutionBrokerError("execution_generation_fenced")
    if (
        purpose == "output"
        and lease.revoked_at is not None
        and (
            lease.cleanup_state != "pending"
            or lease.cleanup_deadline_at is None
            or lease.cleanup_deadline_at <= current_time
        )
    ):
        raise ServiceExecutionBrokerError("execution_output_window_closed")
    if purpose in {"token", "input"} and (
        target is None
        or target.desired_state != "active"
        or target.observed_state != "ready"
        or target.health_status != "healthy"
        or target.health_observed_at is None
        or target.health_observed_at
        + timedelta(seconds=int(target.spec_json["health_stale_after_seconds"]))
        <= current_time
    ):
        raise ServiceExecutionBrokerError("execution_target_unavailable")
    return lease


async def mint_service_execution_peer_token(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    ttl_seconds: int,
    signing_key: str,
    attempt_deadline_wall_clock: datetime | None = None,
    now: datetime | None = None,
) -> tuple[str, datetime, UUID]:
    current_time = now or datetime.now(UTC)
    if ttl_seconds <= 0 or ttl_seconds > _MAX_TOKEN_TTL_SECONDS:
        raise ServiceExecutionBrokerError("service_execution_ttl_exceeded")
    plan = _runtime_plan(lease)
    trial = await session.get(Trial, lease.trial_id)
    if trial is None or trial.team_id != lease.team_id:
        raise ServiceExecutionBrokerError("trial_identity_drift")
    # A rolling old runtime gets the existing lease cutoff; new runtimes pass
    # their actual phase deadline. Neither can extend the admitted lease.
    deadline = lease.deadline_at
    if attempt_deadline_wall_clock is not None:
        if (
            attempt_deadline_wall_clock.tzinfo is None
            or attempt_deadline_wall_clock.utcoffset() is None
        ):
            raise ServiceExecutionBrokerError("execution_deadline_invalid")
        deadline = min(deadline, attempt_deadline_wall_clock)
    if deadline <= current_time:
        raise ServiceExecutionBrokerError("execution_deadline_elapsed")
    step_jwt_id = uuid4()
    token = mint_step_jwt(
        team_id=lease.team_id,
        trial_id=lease.trial_id,
        step_id="agent" if lease.execution_role == "attempt" else "verifier",
        ttl_sec=ttl_seconds,
        signing_key=signing_key,
        provider_connection_id=trial.provider_connection_id,
        provider_connection_id_bound=True,
        step_jwt_id=step_jwt_id,
        issued_at=current_time,
        attempt_deadline_wall_clock=deadline,
        service_execution_lease_id=lease.id,
        service_execution_generation=lease.generation,
        service_execution_role=cast(Any, lease.execution_role),
        service_execution_runtime_contract_sha256=lease.runtime_contract_sha256,
        service_execution_candidate_sha=plan.candidate_sha,
        service_execution_task_revision_sha256=plan.task_revision_sha256,
        service_execution_command_identity_sha256=plan.command_identity_sha256,
    )
    expires_at = current_time + timedelta(seconds=ttl_seconds)
    session.add(
        AdminAuditEvent(
            actor=f"service-execution-pod:{lease.pod_uid}",
            action="service_execution.step_token.minted",
            target_type="execution_lease",
            target_id=str(lease.id),
            event_metadata={
                "step_jwt_id": str(step_jwt_id),
                "trial_id": str(lease.trial_id),
                "team_id": str(lease.team_id),
                "generation": lease.generation,
                "execution_role": lease.execution_role,
                "runtime_contract_sha256": lease.runtime_contract_sha256,
                "expires_in_seconds": ttl_seconds,
                "attempt_deadline_wall_clock": deadline.isoformat(),
                "provider_connection_id": (
                    str(trial.provider_connection_id) if trial.provider_connection_id else None
                ),
                "credential_delivery": "observed_pod_peer",
            },
        )
    )
    await session.flush()
    return token, expires_at, step_jwt_id


class ServiceExecutionOutputRouteService:
    def __init__(
        self,
        *,
        service: ArtifactCommitService,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._service = service
        self._session_factory = session_factory

    @staticmethod
    def _producer(lease: ServiceExecutionLease) -> ServiceExecutionOutputProducerV1:
        plan = _runtime_plan(lease)
        assert lease.runtime_contract_sha256 is not None
        if lease.resource_generation is None:
            raise ServiceExecutionBrokerError("runtime_identity_unavailable")
        return ServiceExecutionOutputProducerV1(
            commit_kind="service_execution_output",
            team_id=lease.team_id,
            service_execution_lease_id=lease.id,
            service_execution_generation=lease.resource_generation,
            service_execution_role=cast(Any, lease.execution_role),
            runtime_contract_sha256=lease.runtime_contract_sha256,
            candidate_sha=plan.candidate_sha,
            task_revision_sha256=plan.task_revision_sha256,
            command_identity_sha256=plan.command_identity_sha256,
        )

    @staticmethod
    def _file_plans(
        lease: ServiceExecutionLease,
        request: ServiceExecutionOutputPrepareV1,
    ) -> list[UploadFilePlanV1]:
        plan = _runtime_plan(lease)
        total = sum(item.size_bytes for item in request.files)
        stream_count = sum(
            item.relative_path.endswith((".stdout", ".stderr")) for item in request.files
        )
        if stream_count > 2 * 64 or total > (
            _RESULT_MAX_BYTES + plan.max_artifact_bytes + 2 * 64 * plan.max_log_bytes_per_stream
        ):
            raise ServiceExecutionBrokerError("output_inventory_exceeds_runtime_bounds")
        artifact_id = uuid4()
        result: list[UploadFilePlanV1] = []
        for index, item in enumerate(request.files):
            maximum = (
                _RESULT_MAX_BYTES
                if item.relative_path == "result.json"
                else plan.max_log_bytes_per_stream
                if item.relative_path.endswith((".stdout", ".stderr"))
                else plan.max_artifact_bytes
            )
            if item.size_bytes > maximum:
                raise ServiceExecutionBrokerError("output_file_exceeds_runtime_bounds")
            result.append(
                UploadFilePlanV1(
                    file_index=index,
                    preallocated_artifact_id=artifact_id,
                    relative_path=item.relative_path,
                    artifact_name="trial_bundle",
                    artifact_type="loom.trial-artifact-bundle.v1",
                    producer="service",
                    media_type=item.media_type,
                    role="semantic_document" if item.relative_path == "result.json" else "payload",
                    archive_format="none",
                    expected_max_bytes=max(1, maximum),
                    expected_sha256=item.sha256,
                    expected_size=item.size_bytes,
                )
            )
        return result

    async def prepare(
        self,
        *,
        lease: ServiceExecutionLease,
        request: ServiceExecutionOutputPrepareV1,
    ) -> dict[str, Any]:
        producer = self._producer(lease)
        request_digest = canonical_digest(request)
        grant = await self._service.prepare_session(
            producer=producer,
            files=self._file_plans(lease, request),
            idempotency_key=str(request.request_id),
            request_digest=request_digest,
        )
        async with self._session_factory() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            if (
                current is None
                or current.resource_generation != lease.resource_generation
                or current.deleted_at is not None
                or current.output_commit_state == "unavailable"
            ):
                raise ServiceExecutionBrokerError("execution_generation_fenced")
            if current.output_commit_state == "committed":
                if current.output_upload_session_id != grant.upload_session_id:
                    raise ServiceExecutionBrokerError("output_commit_identity_drift")
            elif current.output_commit_state == "uploading":
                if current.output_upload_session_id != grant.upload_session_id:
                    raise ServiceExecutionBrokerError("output_upload_already_active")
            elif current.output_commit_state != "not_started":
                raise ServiceExecutionBrokerError("output_commit_unavailable")
            else:
                current.output_commit_state = "uploading"
                current.output_upload_session_id = grant.upload_session_id
                current.output_generation = current.resource_generation
                current.updated_at = datetime.now(UTC)
            await session.commit()
        return grant.model_dump(mode="json")

    async def assert_session(
        self, *, lease: ServiceExecutionLease, session_id: UUID
    ) -> ArtifactUploadSession:
        async with self._session_factory() as session:
            upload = await session.get(ArtifactUploadSession, session_id)
            if (
                upload is None
                or upload.commit_kind != "service_execution_output"
                or upload.service_execution_lease_id != lease.id
                or upload.service_execution_generation != lease.resource_generation
                or upload.team_id != lease.team_id
            ):
                raise ServiceExecutionBrokerError("output_upload_identity_drift")
            session.expunge(upload)
            return upload

    async def session_files(
        self, *, lease: ServiceExecutionLease, session_id: UUID
    ) -> list[ArtifactUploadFile]:
        await self.assert_session(lease=lease, session_id=session_id)
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(ArtifactUploadFile)
                        .where(ArtifactUploadFile.session_id == session_id)
                        .order_by(ArtifactUploadFile.file_index)
                    )
                ).scalars()
            )
            for row in rows:
                session.expunge(row)
            return rows

    async def put_part(
        self,
        *,
        lease: ServiceExecutionLease,
        session_id: UUID,
        file_index: int,
        part_number: int,
        content_length: int,
        content_sha256: str,
        upload_token: str,
        body: AsyncIterator[bytes],
    ) -> dict[str, Any]:
        await self.assert_session(lease=lease, session_id=session_id)
        result = await self._service.write_part(
            session_id=session_id,
            file_index=file_index,
            part_number=part_number,
            content_length=content_length,
            content_sha256=content_sha256,
            body=body,
            auth=UploadAuthV1(upload_token=upload_token),
        )
        return result.model_dump(mode="json")

    async def complete_file(
        self,
        *,
        lease: ServiceExecutionLease,
        session_id: UUID,
        file_index: int,
        ordered_parts: tuple[PartReceiptV1, ...],
        upload_token: str,
    ) -> dict[str, Any]:
        await self.assert_session(lease=lease, session_id=session_id)
        result = await self._service.complete_file(
            session_id=session_id,
            file_index=file_index,
            ordered_parts=list(ordered_parts),
            auth=UploadAuthV1(upload_token=upload_token),
        )
        return result.model_dump(mode="json")

    async def commit(
        self,
        *,
        lease: ServiceExecutionLease,
        session_id: UUID,
        upload_token: str,
    ) -> dict[str, Any]:
        await self.assert_session(lease=lease, session_id=session_id)
        auth = UploadAuthV1(upload_token=upload_token)
        result_payload = await self._service.read_verified_file(
            session_id=session_id,
            file_index=0,
            auth=auth,
            max_bytes=_RESULT_MAX_BYTES,
        )
        runtime_result = _validate_runtime_result(lease, _runtime_plan(lease), result_payload)
        upload_files = await self.session_files(lease=lease, session_id=session_id)
        paths = [item.relative_path for item in upload_files]
        required_paths = {
            "result.json",
            *(
                stream.path
                for phase in runtime_result.phases
                for stream in (phase.stdout, phase.stderr)
            ),
            *(
                output.relative_path
                for output in runtime_result.outputs
                if output.state == "captured"
            ),
        }
        if set(paths) != required_paths:
            raise ServiceExecutionBrokerError("runtime_output_inventory_drift")
        uploaded_by_path = {item.relative_path: item for item in upload_files}
        if any(
            output.state == "captured"
            and (
                uploaded_by_path[output.relative_path].expected_size != output.size_bytes
                or uploaded_by_path[output.relative_path].expected_sha256 != output.sha256
            )
            for output in runtime_result.outputs
        ):
            raise ServiceExecutionBrokerError("runtime_output_evidence_drift")
        result = await self._service.commit_session(
            session_id=session_id,
            auth=auth,
        )
        manifest, manifest_sha256, marker_sha256 = await self._service.committed_session_evidence(
            session_id
        )
        if len(manifest.artifacts) != 1:
            raise ServiceExecutionBrokerError("output_commit_manifest_drift")
        record = manifest.artifacts[0]
        async with self._session_factory() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            upload = await session.get(ArtifactUploadSession, session_id, with_for_update=True)
            if (
                current is None
                or upload is None
                or current.resource_generation != lease.resource_generation
                or current.deleted_at is not None
                or current.output_commit_state == "unavailable"
                or current.output_upload_session_id != session_id
                or current.output_generation != lease.resource_generation
                or upload.state != "committed"
            ):
                raise ServiceExecutionBrokerError("execution_generation_fenced")
            output_already_committed = current.output_commit_state == "committed"
            artifact = await session.get(Artifact, record.artifact_id)
            if artifact is None:
                session.add(
                    Artifact(
                        id=record.artifact_id,
                        artifact_type=record.artifact_type,
                        name=record.artifact_name,
                        team_id=current.team_id,
                        trial_id=current.trial_id,
                        producer_kind=None,
                        control_producer_kind="service_execution",
                        control_producer_id=current.id,
                        content_hash=record.content_sha256,
                        storage={
                            "session_id": str(session_id),
                            "files": [item.model_dump(mode="json") for item in record.stored_files],
                        },
                        visibility="team",
                        share_status="pending_scan",
                        safety_state="verified_internal",
                        access_class="team_runtime",
                        artifact_upload_session_id=session_id,
                        manifest_sha256=record.manifest_sha256,
                        stored_size_bytes=sum(item.size_bytes for item in record.stored_files),
                        unpacked_size_bytes=sum(item.size_bytes for item in record.stored_files),
                        file_count=max(1, len(record.stored_files)),
                        provenance={
                            "schema_version": "loom.service-execution-trial-bundle-provenance.v1",
                            "lease_id": str(current.id),
                            "generation": current.resource_generation,
                            "runtime_contract_sha256": current.runtime_contract_sha256,
                            "candidate_sha": runtime_result.candidate_sha,
                            "task_revision_sha256": runtime_result.task_revision_sha256,
                            "command_identity_sha256": runtime_result.command_identity_sha256,
                        },
                    )
                )
            elif (
                artifact.artifact_upload_session_id != session_id
                or artifact.manifest_sha256 != record.manifest_sha256
            ):
                raise ServiceExecutionBrokerError("output_artifact_identity_drift")
            if not output_already_committed:
                current.output_commit_state = "committed"
                current.output_manifest_sha256 = manifest_sha256
                current.output_marker_sha256 = marker_sha256
                current.output_committed_at = datetime.now(UTC)
                current.updated_at = current.output_committed_at
                if current.materialization_state != "not_started":
                    raise ServiceExecutionBrokerError("materialization_identity_drift")
                current.materialization_state = "pending"
                current.materialization_next_attempt_at = current.output_committed_at
            elif (
                current.output_manifest_sha256 != manifest_sha256
                or current.output_marker_sha256 != marker_sha256
                or current.materialization_state not in {"pending", "running", "committed"}
            ):
                raise ServiceExecutionBrokerError("materialization_identity_drift")
            if current.output_committed_at is None:
                raise ServiceExecutionBrokerError("output_commit_identity_drift")
            trial = await session.get(Trial, current.trial_id, with_for_update=True)
            if trial is None:
                raise ServiceExecutionBrokerError("trial_identity_drift")
            trajectory_index = {
                "schema_version": "loom.service-execution-materialization-source.v1",
                "artifact_bundle_id": str(record.artifact_id),
                "upload_session_id": str(session_id),
                "manifest_sha256": manifest_sha256,
                "outputs": [item.model_dump(mode="json") for item in runtime_result.outputs],
                "verifier_rewards": runtime_result.verifier_rewards,
            }
            # The attempt's bundle remains the trial's source; a deferred verifier
            # is merged into it when the attempt materializes.
            if not output_already_committed and current.execution_role == "attempt":
                if trial.trajectory_index is not None:
                    raise ServiceExecutionBrokerError("trajectory_index_identity_drift")
                trial.trajectory_index = trajectory_index
            await record_committed_runtime_result(
                session,
                lease_id=current.id,
                generation=current.generation,
                runtime_result=runtime_result,
                observed_at=current.output_committed_at,
            )
            await session.commit()
        return {
            **result.model_dump(mode="json"),
            "manifest_sha256": manifest_sha256,
            "committed_marker_sha256": marker_sha256,
        }


__all__ = [
    "ResolvedServiceExecutionHandoff",
    "ServiceExecutionBrokerError",
    "ServiceExecutionFileCompleteV1",
    "ServiceExecutionOutputCommitV1",
    "ServiceExecutionOutputPrepareV1",
    "ServiceExecutionOutputRouteService",
    "ServiceExecutionPeerV1",
    "ServiceExecutionTokenRequestV1",
    "authorize_service_execution_peer",
    "committed_handoff_files",
    "mint_service_execution_peer_token",
    "resolve_service_execution_handoff",
]
