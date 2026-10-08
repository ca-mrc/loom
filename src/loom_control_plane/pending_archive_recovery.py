"""One bounded archival worker for a qualified, already completed execution.

This entry point never starts the application, scheduler or cleanup loops. The
operator launches it only from a published, immutable Control Plane image.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Artifact, ArtifactUploadSession, LlmCall, ServiceExecutionLease, Trial
from loom.db.schema_startup import service_schema_head
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.service_execution import defers_verification
from loom_control_plane.service_execution_materializer import (
    MaterializationClaim,
    MaterializationIntegrityError,
    MaterializationResult,
    ServiceExecutionMaterializer,
    _canonical_jsonl,
    _load_source,
    build_canonical_atif,
    build_canonical_events,
    validate_usage_accounting,
)
from loom_control_plane.service_execution_task_snapshot import (
    resolve_service_execution_task_snapshot,
)

SOFT_TIMEOUT = 1500
HARD_TIMEOUT = 1700
JOB_TIMEOUT = 1800
CLAIM_SECONDS = 3600
AUDIT_KEY = "pending_archive_recovery"
Sha256 = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
GitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]


class RecoveryRefusedError(ValueError):
    """Safe closed diagnostic; never includes source data or credentials."""


class ArchiveRecoveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    team_id: UUID
    trial_id: UUID
    lease_id: UUID
    artifact_id: UUID
    upload_session_id: UUID
    attempt: int = Field(ge=1, strict=True)
    generation: int = Field(ge=1, strict=True)
    output_manifest_sha256: Sha256
    output_marker_sha256: Sha256
    runtime_result_sha256: Sha256
    cluster_id: str = Field(pattern=r"^mk8s-[a-z0-9-]{1,100}$")
    namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    installed_candidate: GitSha
    candidate_sha: GitSha
    image_ref: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$")
    installed_image_ref: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$")
    schema_head: str = Field(pattern=r"^[0-9]{4}$")
    trial_config_sha256: Sha256
    task_config_sha256: Sha256
    derivation_sha256: Sha256
    events_sha256: Sha256
    atif_sha256: Sha256

    @property
    def digest(self) -> str:
        return digest_bytes(canonical_document(self.model_dump(mode="json")))


def qualify_pending(
    request: ArchiveRecoveryRequest, lease: ServiceExecutionLease, trial: Trial,
    artifact: Artifact, *, now: datetime,
) -> None:
    """Recheck immutable identities and fresh eligibility under database locks."""
    expected = {
        "id": request.lease_id, "team_id": request.team_id, "trial_id": request.trial_id,
        "attempt": request.attempt, "resource_generation": request.generation,
        "output_generation": request.generation, "output_upload_session_id": request.upload_session_id,
        "output_manifest_sha256": request.output_manifest_sha256,
        "output_marker_sha256": request.output_marker_sha256,
        "execution_role": "attempt", "parent_lease_id": None,
        "desired_state": "deleted", "observed_state": "deleted", "cleanup_state": "complete",
        "output_commit_state": "committed", "materialization_state": "pending",
        "materialization_claim_id": None, "materialization_claim_expires_at": None,
        "materialization_error_code": "transient_materialization_error",
        "materialization_error_message": "multipart object readback identity mismatch",
        "canonical_trajectory_sha256": None, "canonical_atif_sha256": None,
        "source_cleanup_state": "not_ready", "materialization_recovery_requested_at": None,
    }
    runtime = (trial.result or {}).get("runtime_result")
    if (
        any(getattr(lease, key) != value for key, value in expected.items())
        or defers_verification(lease)
        or any(getattr(lease, key) is None for key in ("finalized_at", "revoked_at", "deleted_at"))
        or lease.materialization_attempts < 1
        or lease.materialization_next_attempt_at is None
        or trial.id != request.trial_id or trial.team_id != request.team_id
        or trial.attempt_count != request.attempt or trial.state != "materializing"
        or trial.config.get("agent_name") != "oracle" or trial.config.get("agent_model") is not None
        or digest_bytes(canonical_document(trial.config)) != request.trial_config_sha256
        or not isinstance(runtime, dict) or runtime.get("status") != "succeeded"
        or digest_bytes(canonical_document(runtime)) != request.runtime_result_sha256
        or (trial.result or {}).get("verifier_execution") is not None
        or artifact.id != request.artifact_id or artifact.team_id != request.team_id
        or artifact.trial_id != request.trial_id or artifact.control_producer_kind != "service_execution"
        or artifact.control_producer_id != request.lease_id
        or AUDIT_KEY in (artifact.artifact_metadata or {})
    ):
        raise RecoveryRefusedError("pending_archive_not_eligible")


async def claim_pending_archive(
    session: AsyncSession, request: ArchiveRecoveryRequest, *, now: datetime | None = None,
) -> MaterializationClaim:
    """Caller commits one audited claim; no other Trial or lease is changed."""
    current = now or datetime.now(UTC)
    await session.execute(text("SET LOCAL lock_timeout='5s'"))
    await session.execute(text("SET LOCAL statement_timeout='10s'"))
    if not await admission_open(session):
        raise RecoveryRefusedError("rollout_guard_held")
    if list(await session.scalars(text("SELECT version_num FROM alembic_version"))) != [request.schema_head]:
        raise RecoveryRefusedError("schema_binding_changed")
    lease = await session.get(ServiceExecutionLease, request.lease_id, with_for_update=True)
    trial = await session.get(Trial, request.trial_id, with_for_update=True)
    artifact = await session.get(Artifact, request.artifact_id, with_for_update=True)
    if lease is None or trial is None or artifact is None:
        raise RecoveryRefusedError("source_identity_missing")
    qualify_pending(request, lease, trial, artifact, now=current)
    await qualify_inputs(session, request, lease, trial)
    child = await session.scalar(select(ServiceExecutionLease.id).where(
        ServiceExecutionLease.parent_lease_id == lease.id).limit(1))
    upload = await session.get(ArtifactUploadSession, request.upload_session_id)
    if child is not None or upload is None:
        raise RecoveryRefusedError("source_shape_unsupported")
    records = (upload.canonical_manifest_json or {}).get("artifacts", [])
    if len(records) != 1 or records[0].get("artifact_id") != str(artifact.id):
        raise RecoveryRefusedError("source_shape_unsupported")
    files = records[0].get("stored_files", [])
    sizes = [item.get("size_bytes") for item in files]
    if (not 1 <= len(files) <= 1024 or any(type(size) is not int or size < 0 for size in sizes)
            or sum(sizes) > 512 * 1024 * 1024):
        raise RecoveryRefusedError("source_size_unsupported")
    claim = MaterializationClaim(lease_id=lease.id, claim_id=uuid4())
    artifact.artifact_metadata = {**(artifact.artifact_metadata or {}), AUDIT_KEY: {
        "request": request.model_dump(mode="json"), "request_sha256": request.digest,
        "claim_id": str(claim.claim_id), "started_at": current.isoformat(),
        "previous_materialization_attempts": lease.materialization_attempts,
        "previous_error_code": lease.materialization_error_code,
        "previous_error_message": lease.materialization_error_message,
        "previous_next_attempt_at": str(lease.materialization_next_attempt_at),
        "source_files": len(files), "source_bytes": sum(sizes),
    }}
    lease.materialization_state = "running"
    lease.materialization_attempts += 1
    lease.materialization_claim_id = claim.claim_id
    lease.materialization_claim_expires_at = current + timedelta(seconds=CLAIM_SECONDS)
    lease.materialization_started_at = current
    lease.materialization_next_attempt_at = None
    lease.updated_at = current
    await session.flush()
    return claim


async def committed_readback(
    session: AsyncSession, request: ArchiveRecoveryRequest, claim: MaterializationClaim,
) -> dict[str, Any]:
    lease = await session.get(ServiceExecutionLease, request.lease_id)
    trial = await session.get(Trial, request.trial_id)
    artifact = await session.get(Artifact, request.artifact_id)
    if lease is None or trial is None or artifact is None:
        raise RecoveryRefusedError("completion_identity_missing")
    audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY, {})
    index = trial.trajectory_index or {}
    storage = artifact.storage or {}
    files = [*storage.get("files", []), *storage.get("source_evidence", [])]
    if (
        audit.get("request_sha256") != request.digest or audit.get("claim_id") != str(claim.claim_id)
        or lease.materialization_state != "committed" or trial.state != "succeeded"
        or lease.materialization_attempts != audit.get("previous_materialization_attempts", -1) + 1
        or lease.attempt != request.attempt or trial.attempt_count != request.attempt
        or lease.output_manifest_sha256 != request.output_manifest_sha256
        or lease.output_marker_sha256 != request.output_marker_sha256
        or lease.output_upload_session_id != request.upload_session_id
        or lease.output_generation != request.generation or lease.resource_generation != request.generation
        or digest_bytes(canonical_document((trial.result or {}).get("runtime_result"))) != request.runtime_result_sha256
        or not lease.canonical_trajectory_sha256 or not lease.canonical_atif_sha256
        or lease.source_cleanup_state != "retained" or lease.source_retain_until is None
        or not files or any(not item.get("version_id") for item in files)
        or not index.get("trajectory_version_id") or not index.get("atif_version_id")
    ):
        raise RecoveryRefusedError("canonical_commit_not_qualified")
    return {"status": "committed", "request_sha256": request.digest,
            "materialization_attempts": lease.materialization_attempts,
            "execution_attempts": trial.attempt_count, "canonical_files": len(files),
            "source_retain_until": lease.source_retain_until.isoformat()}


PROJECTION_FIELDS = ("trial_config_sha256", "task_config_sha256", "runtime_result_sha256",
                     "derivation_sha256", "events_sha256", "atif_sha256")


async def qualify_inputs(
    session: AsyncSession, request: ArchiveRecoveryRequest, lease: ServiceExecutionLease, trial: Trial,
) -> None:
    snapshot = await resolve_service_execution_task_snapshot(session, lease=lease, trial=trial)
    call = await session.scalar(select(LlmCall.id).where(
        LlmCall.team_id == request.team_id, LlmCall.trial_id == request.trial_id).limit(1))
    if (call is not None or defers_verification(lease) or trial.config.get("agent_name") != "oracle"
            or trial.config.get("agent_model") is not None or (trial.result or {}).get("cancelled")
            or (trial.result or {}).get("verifier_execution") is not None
            or digest_bytes(canonical_document(trial.config)) != request.trial_config_sha256
            or digest_bytes(canonical_document(snapshot.config)) != request.task_config_sha256
            or digest_bytes(canonical_document((trial.result or {}).get("runtime_result"))) != request.runtime_result_sha256
            or lease.output_manifest_sha256 != request.output_manifest_sha256
            or lease.output_marker_sha256 != request.output_marker_sha256
            or lease.output_upload_session_id != request.upload_session_id):
        raise RecoveryRefusedError("projection_inputs_changed")


async def project_oracle(
    session: AsyncSession, materializer: ServiceExecutionMaterializer, *, team_id: UUID, lease_id: UUID,
) -> dict[str, str]:
    """Read-only exact Oracle projection, also executable against installed code.

    It reads only the manifests and four derivation files. No claim, destination
    object write, event or source cleanup is performed.
    """
    lease = await session.get(ServiceExecutionLease, lease_id)
    if lease is None or lease.team_id != team_id:
        raise RecoveryRefusedError("source_identity_missing")
    trial = await session.get(Trial, lease.trial_id)
    if (trial is None or defers_verification(lease) or trial.team_id != team_id or trial.config.get("agent_name") != "oracle"
            or trial.config.get("agent_model") is not None or (trial.result or {}).get("cancelled")
            or (trial.result or {}).get("verifier_execution") is not None
            or await session.scalar(select(LlmCall.id).where(
                LlmCall.team_id == team_id, LlmCall.trial_id == trial.id).limit(1)) is not None):
        raise RecoveryRefusedError("oracle_projection_only")
    snapshot = await resolve_service_execution_task_snapshot(session, lease=lease, trial=trial)
    source = await materializer._verify_source(await _load_source(session, lease))
    inputs = {}
    for item in source.record.stored_files:
        if item.relative_path in {"result.json", "trajectory/events.jsonl", "accounting/usage.json", "verifier/output.json"}:
            inputs[item.relative_path] = await materializer._read_exact(
                key=f"{source.prefix}artifacts/{source.artifact_id}/{item.relative_path}",
                expected=item.sha256, size=item.size_bytes)
    if set(inputs) != {"result.json", "trajectory/events.jsonl", "accounting/usage.json", "verifier/output.json"}:
        raise RecoveryRefusedError("derivation_input_missing")
    runtime = ExecutionRuntimeResultV1.model_validate_json(inputs["result.json"])
    if runtime.status != "succeeded" or runtime.model_dump(mode="json") != (trial.result or {}).get("runtime_result"):
        raise RecoveryRefusedError("runtime_result_projection_drift")
    task_config = TaskConfig.model_validate(snapshot.config)
    trial_config = TrialConfig.model_validate(trial.config)
    validate_usage_accounting(trace_body=inputs["trajectory/events.jsonl"],
        usage_body=inputs["accounting/usage.json"], trial_config=trial_config)
    events = build_canonical_events(trial_id=trial.id, task_id=trial.task_id, task_config=task_config,
        trial_config=trial_config, runtime_result=runtime, trace_body=inputs["trajectory/events.jsonl"],
        verifier_body=inputs["verifier/output.json"], gateway_calls=[], exception_info=None)
    atif = build_canonical_atif(events, task_id=trial.task_id, agent_name=trial_config.agent_name,
        agent_version=trial_config.agent_version or task_config.agent.version or "service-execution-v1")
    return {"trial_config_sha256": digest_bytes(canonical_document(trial.config)),
            "task_config_sha256": digest_bytes(canonical_document(snapshot.config)),
            "runtime_result_sha256": digest_bytes(canonical_document(runtime.model_dump(mode="json"))),
            "derivation_sha256": digest_bytes(canonical_document({key: digest_bytes(body) for key, body in inputs.items()})),
            "events_sha256": digest_bytes(_canonical_jsonl(events)), "atif_sha256": digest_bytes(atif)}


class OracleRecoveryMaterializer(ServiceExecutionMaterializer):
    def __init__(self, *, request: ArchiveRecoveryRequest, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.request = request

    async def _park_failure(self, claim: MaterializationClaim, *, code: str) -> None:
        """Fence a caught failure without rewriting the original Trial outcome.

        Unavailable archives are not automatically reclaimed or source-cleaned.
        Cancellation does not take this path: ownership remains until expiry so
        SDK threads have time to stop under the process/Job hard deadlines.
        """
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (lease is None or lease.id != self.request.lease_id
                    or lease.materialization_state != "running"
                    or lease.materialization_claim_id != claim.claim_id):
                return
            artifact = await session.get(Artifact, self.request.artifact_id, with_for_update=True)
            audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY, {}) if artifact else {}
            if (artifact is None or audit.get("request_sha256") != self.request.digest
                    or audit.get("claim_id") != str(claim.claim_id)):
                raise RecoveryRefusedError("recovery_failure_audit_changed")
            artifact.artifact_metadata = {**(artifact.artifact_metadata or {}), AUDIT_KEY: {
                **audit, "failure": {"code": code[:120], "claim_id": str(claim.claim_id),
                                     "observed_at": now.isoformat()}}}
            lease.materialization_state = "unavailable"
            lease.materialization_claim_id = None
            lease.materialization_claim_expires_at = None
            lease.materialization_next_attempt_at = None
            lease.materialization_error_code = code[:120]
            lease.materialization_error_message = "bounded archive recovery incomplete; inspect recovery audit"
            lease.updated_at = now
            await session.commit()

    async def _unavailable(self, claim: MaterializationClaim, exc: MaterializationIntegrityError) -> None:
        await self._park_failure(claim, code=exc.code)

    async def _retry(self, claim: MaterializationClaim, exc: Exception) -> None:
        # A one-shot operator must not hand a failed recovery back to the faulty
        # installed worker. The audit records no exception text or credentials.
        await self._park_failure(claim, code="recovery_incomplete")

    async def _qualify_commit(
        self, session: AsyncSession, lease: ServiceExecutionLease, trial: Trial,
        artifact: Artifact, result: MaterializationResult,
    ) -> None:
        await qualify_inputs(session, self.request, lease, trial)
        audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY, {})
        if (audit.get("request_sha256") != self.request.digest
                or audit.get("claim_id") != str(lease.materialization_claim_id)
                or result.events_sha256 != self.request.events_sha256
                or result.atif_sha256 != self.request.atif_sha256):
            raise RecoveryRefusedError("recovery_projection_changed")


def qualify_platform(request: ArchiveRecoveryRequest, directory: Path) -> None:
    environment = json.loads((directory / "environment.json").read_bytes())
    profile = json.loads((directory / "profile.json").read_bytes())
    if (environment.get("cluster_id") != request.cluster_id
            or environment.get("namespace") != request.namespace
            or profile.get("candidate_sha") != request.installed_candidate
            or service_schema_head() != request.schema_head):
        raise RecoveryRefusedError("platform_binding_changed")


async def run_recovery(request: ArchiveRecoveryRequest, settings: ControlPlaneSettings) -> dict[str, Any]:
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_config = ServiceExecutionSourceConfig.from_settings(settings)
    if source_config is None:
        await engine.dispose()
        raise RecoveryRefusedError("independent_source_store_required")
    canonical = MinioObjectStore(endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(), secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region)
    source = cast(MinioObjectStore, source_config.build_store(MinioObjectStore))
    try:
        async with asyncio.timeout(SOFT_TIMEOUT):
            materializer = OracleRecoveryMaterializer(request=request, session_factory=sessions, source_store=source,
                source_bucket=source_config.bucket, canonical_store=canonical,
                artifacts_bucket=settings.artifacts_bucket, trajectories_bucket=settings.trajectories_bucket,
                source_retention_seconds=settings.service_execution_source_retention_sec)
            async with sessions.begin() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                projected = await project_oracle(session, materializer, team_id=request.team_id, lease_id=request.lease_id)
            if any(projected[key] != getattr(request, key) for key in PROJECTION_FIELDS):
                raise RecoveryRefusedError("installed_projection_differs")
            async with sessions.begin() as session:
                claim = await claim_pending_archive(session, request)
            await materializer.materialize_claim(claim, require_versions=True)
            async with sessions() as session:
                return await committed_readback(session, request, claim)
    finally:
        source.close()
        canonical.close()
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-json", required=True)
    parser.add_argument("--platform", type=Path, required=True)
    args = parser.parse_args()
    # This dedicated process cannot survive past claim expiry even if SDK thread
    # cancellation is delayed. It is never used inside the long-lived service.
    signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.alarm(HARD_TIMEOUT)
    try:
        raw = args.request_json.encode()
        if len(raw) > 16384:
            raise RecoveryRefusedError("request_size")
        request = ArchiveRecoveryRequest.model_validate_json(raw)
        qualify_platform(request, args.platform)
        result = asyncio.run(run_recovery(request, ControlPlaneSettings()))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        # Exceptions may include storage URLs/credentials; emit only closed codes.
        code = str(error) if isinstance(error, RecoveryRefusedError) else "recovery_incomplete"
        print(json.dumps({"status": "blocked", "reason": code}))
        return 1
    finally:
        signal.alarm(0)


if __name__ == "__main__":
    raise SystemExit(main())
