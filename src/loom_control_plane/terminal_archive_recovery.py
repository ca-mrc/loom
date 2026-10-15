"""One storage-only archival pass for an exact, deleted attempt and its verifier.

No scheduler, execution, source-GC or schema-upgrade loop is started. Ordinary
materialization owns the claim and canonical commit; rollout stays fail closed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Select, event, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import ORMExecuteState, defer

from loom.db.schema import Artifact, ArtifactUploadSession, LlmCall, ServiceExecutionLease, Trial
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.pending_archive_recovery import GitSha, RecoveryRefusedError, Sha256
from loom_control_plane.service_execution_materializer import (
    MaterializationClaim,
    MaterializationIntegrityError,
    MaterializationResult,
    ServiceExecutionMaterializer,
)
from loom_control_plane.service_execution_task_snapshot import (
    resolve_service_execution_task_snapshot,
)

AUDIT_KEY = "terminal_archive_recovery"


class TerminalArchiveBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    team_id: UUID
    trial_id: UUID
    lease_id: UUID
    child_lease_id: UUID
    artifact_id: UUID
    upload_session_id: UUID
    child_upload_session_id: UUID
    attempt: int = Field(gt=0)
    generation: int = Field(gt=0)
    child_generation: int = Field(gt=0)
    output_manifest_sha256: Sha256
    output_marker_sha256: Sha256
    child_output_manifest_sha256: Sha256
    child_output_marker_sha256: Sha256
    trial_state: str = Field(pattern=r"^(materializing|failed|cancelled)$")
    trial_result_sha256: Sha256
    trial_config_sha256: Sha256
    task_config_sha256: Sha256
    failure_reason: str | None
    failure_message_sha256: Sha256
    finished_at: datetime | None


class TerminalArchiveRecoveryRequest(TerminalArchiveBinding):
    operation_id: UUID
    cluster_id: str = Field(pattern=r"^mk8scluster-[a-z0-9]+$")
    namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    installed_candidate: GitSha
    candidate_sha: GitSha
    image_ref: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$")
    installed_image_ref: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$")
    schema_head: str = Field(pattern=r"^[0-9]{4}$")

    @property
    def digest(self) -> str:
        # Reuse the existing Job submission-intent identity, not a new gate.
        return digest_bytes(canonical_document(self.model_dump(mode="json")))


def qualify_deleted(lease: Any, trial: Any, child: Any, *, allow_running: bool = False) -> None:
    """Only the current, committed parent/child pair with observed deletion."""
    if (lease.execution_role != "attempt" or lease.parent_lease_id is not None
            or trial.id != lease.trial_id or trial.team_id != lease.team_id
            or trial.attempt_count != lease.attempt
            or trial.state not in {"materializing", "failed", "cancelled"}
            or child.execution_role != "verifier" or child.parent_lease_id != lease.id
            or child.team_id != lease.team_id or child.trial_id != lease.trial_id
            or child.attempt != lease.attempt):
        raise RecoveryRefusedError("terminal_archive_identity_changed")
    for row in (lease, child):
        if (row.desired_state != "deleted" or row.observed_state != "deleted"
                or row.cleanup_state != "complete" or row.finalized_at is None
                or row.deleted_at is None or row.revoked_at is None
                or row.output_commit_state != "committed"
                or row.output_generation != row.resource_generation
                or row.source_cleanup_state != "not_ready"
                or row.canonical_trajectory_sha256 is not None or row.canonical_atif_sha256 is not None):
            raise RecoveryRefusedError("terminal_archive_source_not_eligible")
    if (child.materialization_state != "pending" or child.materialization_claim_id is not None
            or child.materialization_claim_expires_at is not None):
        raise RecoveryRefusedError("verifier_archive_claim_active")
    if not allow_running and (lease.materialization_state != "pending"
            or lease.materialization_claim_id is not None or lease.materialization_claim_expires_at is not None):
        # Expired running claims are also refused; this operator never steals one.
        raise RecoveryRefusedError("parent_archive_claim_active")
    if allow_running and (lease.materialization_state != "running"
            or lease.materialization_claim_id is None or lease.materialization_claim_expires_at is None):
        raise RecoveryRefusedError("archive_claim_lost")
    handoff = (trial.result or {}).get("verifier_execution")
    if (not isinstance(handoff, dict) or handoff.get("state") != "committed"
            or handoff.get("lease_id") != str(child.id)
            or handoff.get("output_manifest_sha256") != child.output_manifest_sha256
            or handoff.get("output_marker_sha256") != child.output_marker_sha256):
        raise RecoveryRefusedError("verifier_projection_changed")


async def capture_binding(session: AsyncSession, *, team_id: UUID, lease_id: UUID,
                          allow_running: bool = False, lock: bool = False) -> TerminalArchiveBinding:
    lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=lock)
    if lease is None or lease.team_id != team_id:
        raise RecoveryRefusedError("source_identity_missing")
    trial = await session.get(Trial, lease.trial_id, with_for_update=lock)
    child_id = ((trial.result or {}).get("verifier_execution") or {}).get("lease_id") if trial else None
    child = await session.get(ServiceExecutionLease, UUID(child_id), with_for_update=lock) if child_id else None
    if trial is None or child is None:
        raise RecoveryRefusedError("verifier_identity_missing")
    qualify_deleted(lease, trial, child, allow_running=allow_running)
    query = select(Artifact).where(Artifact.control_producer_kind == "service_execution", Artifact.control_producer_id == lease.id)
    artifact = await session.scalar(query.with_for_update() if lock else query)
    if artifact is None or artifact.team_id != team_id or artifact.trial_id != trial.id:
        raise RecoveryRefusedError("artifact_identity_changed")
    for row in (lease, child):
        upload = await session.get(ArtifactUploadSession, row.output_upload_session_id)
        if upload is None or upload.team_id != team_id or upload.state != "committed":
            raise RecoveryRefusedError("source_commit_changed")
        manifest = upload.canonical_manifest_json or {}
        records = manifest.get("artifacts", [])
        files = [item for record in records for item in record.get("stored_files", [])]
        sizes = [item.get("size_bytes") for item in files]
        if (len(records) != 1 or not 1 <= len(files) <= 1024
                or any(type(size) is not int or size < 0 for size in sizes)
                or sum(sizes) > 512 * 1024**2):
            raise RecoveryRefusedError("archive_source_bounds_exceeded")
    snapshot = await resolve_service_execution_task_snapshot(session, lease=lease, trial=trial)
    return TerminalArchiveBinding(
        team_id=team_id, trial_id=trial.id, lease_id=lease.id, child_lease_id=child.id,
        artifact_id=artifact.id, upload_session_id=lease.output_upload_session_id,
        child_upload_session_id=child.output_upload_session_id, attempt=lease.attempt,
        generation=lease.output_generation, child_generation=child.output_generation,
        output_manifest_sha256=lease.output_manifest_sha256,
        output_marker_sha256=lease.output_marker_sha256,
        child_output_manifest_sha256=child.output_manifest_sha256,
        child_output_marker_sha256=child.output_marker_sha256,
        trial_state=trial.state, trial_result_sha256=digest_bytes(canonical_document(trial.result)),
        trial_config_sha256=digest_bytes(canonical_document(trial.config)),
        task_config_sha256=digest_bytes(canonical_document(snapshot.config)),
        failure_reason=trial.failure_reason,
        failure_message_sha256=digest_bytes(canonical_document(trial.failure_message)), finished_at=trial.finished_at,
    )


class TerminalArchiveMaterializer(ServiceExecutionMaterializer):
    def __init__(self, *, request: TerminalArchiveRecoveryRequest, **kwargs: Any):
        super().__init__(**kwargs)
        self.request = request

    async def _qualify(self, session: AsyncSession, lease: ServiceExecutionLease, artifact: Artifact) -> None:
        current = await capture_binding(session, team_id=self.request.team_id,
                                        lease_id=self.request.lease_id, allow_running=True)
        expected = TerminalArchiveBinding.model_validate(self.request.model_dump(include=set(TerminalArchiveBinding.model_fields)))
        audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY, {})
        if (current != expected or audit.get("operation_id") != str(self.request.operation_id)
                or audit.get("materialization_attempt") != lease.materialization_attempts):
            raise MaterializationIntegrityError("terminal_archive_projection_changed")

    async def _qualify_before_copy(
        self, session: AsyncSession, lease: ServiceExecutionLease, trial: Trial, artifact: Artifact,
    ) -> None:
        await self._qualify(session, lease, artifact)

    async def _qualify_commit(
        self, session: AsyncSession, lease: ServiceExecutionLease, trial: Trial,
        artifact: Artifact, result: MaterializationResult,
    ) -> None:
        await self._qualify(session, lease, artifact)
        if self.request.trial_state in {"failed", "cancelled"} and result.final_trial_state != self.request.trial_state:
            raise MaterializationIntegrityError("terminal_archive_outcome_changed")

    async def _load_and_materialize(self, claim: MaterializationClaim) -> MaterializationResult:
        result = await super()._load_and_materialize(claim)
        # Failed/cancelled executions retain their existing outcome and message;
        # canonical exception evidence is still archived by the ordinary worker.
        return replace(result, preserve_trial_outcome=True) if self.request.trial_state != "materializing" else result

    async def _unavailable(self, claim: MaterializationClaim, exc: MaterializationIntegrityError) -> None:
        # Preserve the original Trial outcome, evidence and ordinary retry audit.
        # A failed one-shot does not synthesize an output_unavailable Trial.
        await self._retry(claim, exc)

    async def _qualify_claim(self, session: AsyncSession, lease: ServiceExecutionLease) -> None:
        if lease.id != self.request.lease_id:
            raise RecoveryRefusedError("exact_parent_required")
        if not await admission_open(session):
            raise RecoveryRefusedError("rollout_guard_held")
        current = await capture_binding(session, team_id=self.request.team_id, lease_id=lease.id, lock=True)
        expected = TerminalArchiveBinding.model_validate(self.request.model_dump(include=set(TerminalArchiveBinding.model_fields)))
        if current != expected:
            raise RecoveryRefusedError("terminal_archive_projection_changed")
        artifact = await session.get(Artifact, self.request.artifact_id, with_for_update=True)
        if artifact is None or AUDIT_KEY in (artifact.artifact_metadata or {}):
            raise RecoveryRefusedError("archive_recovery_already_requested")
        from loom_control_plane.pending_archive_recovery import qualify_lifecycle_scope
        await qualify_lifecycle_scope(session, self.request)
        # This audit and ordinary claim are committed in the same transaction.
        artifact.artifact_metadata = {**(artifact.artifact_metadata or {}), AUDIT_KEY: {
            "operation_id": str(self.request.operation_id), "materialization_attempt": lease.materialization_attempts + 1,
            "candidate_sha": self.request.candidate_sha, "requested_at": datetime.now(UTC).isoformat(),
        }}


def archive_sessions(engine: AsyncEngine, *, legacy_baseline: bool) -> async_sessionmaker[AsyncSession]:
    """0175 lacks only the unrelated 0176 baseline linkage used by new runs."""
    class ArchiveSession(AsyncSession):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            if legacy_baseline:
                @event.listens_for(self.sync_session, "do_orm_execute")
                def projected_calls(state: ORMExecuteState) -> None:
                    if (state.is_select and isinstance(state.statement, Select)
                            and any(row.get("expr") is LlmCall for row in state.statement.column_descriptions)):
                        state.statement = state.statement.options(defer(LlmCall.baseline_session_id, raiseload=True))
    return async_sessionmaker(engine, class_=ArchiveSession, expire_on_commit=False)


async def recover(request: TerminalArchiveRecoveryRequest, settings: ControlPlaneSettings) -> dict[str, Any]:
    config = ServiceExecutionSourceConfig.from_settings(settings)
    if config is None:
        raise RecoveryRefusedError("independent_source_store_required")
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    canonical = MinioObjectStore(endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(), secret_key=settings.minio_secret_key.get_secret_value(), region=settings.minio_region)
    source = cast(MinioObjectStore, config.build_store(MinioObjectStore))
    try:
        async with engine.connect() as connection:
            head = (await connection.execute(text("SELECT version_num FROM alembic_version"))).scalars().all()
            if head != [request.schema_head] or request.schema_head not in {"0175", "0176"}:
                raise RecoveryRefusedError("schema_binding_changed")
        sessions = archive_sessions(engine, legacy_baseline=request.schema_head == "0175")
        worker = TerminalArchiveMaterializer(request=request, session_factory=sessions, source_store=source,
            source_bucket=config.bucket, canonical_store=canonical, artifacts_bucket=settings.artifacts_bucket,
            trajectories_bucket=settings.trajectories_bucket, source_retention_seconds=settings.service_execution_source_retention_sec,
            claim_ttl_seconds=3600)
        async with asyncio.timeout(1500):
            # Keep the ordinary backoff. Wait only for this exact parent, never
            # steal an active/expired running claim or rewrite its retry time.
            deadline = asyncio.get_running_loop().time() + 360
            claim = None
            while asyncio.get_running_loop().time() < deadline:
                async with sessions() as session:
                    lease = await session.get(ServiceExecutionLease, request.lease_id)
                    if lease is None:
                        raise RecoveryRefusedError("source_identity_missing")
                    if lease.materialization_state == "running":
                        if (lease.materialization_claim_expires_at is None
                                or lease.materialization_claim_expires_at <= datetime.now(UTC)):
                            raise RecoveryRefusedError("parent_archive_claim_active")
                        delay = 1.0
                    else:
                        await capture_binding(session, team_id=request.team_id, lease_id=request.lease_id)
                        retry_at = lease.materialization_next_attempt_at
                        delay = max(0.0, (retry_at - datetime.now(UTC)).total_seconds()) if retry_at else 0.0
                if delay > 0:
                    await asyncio.sleep(min(delay, max(0.0, deadline - asyncio.get_running_loop().time())))
                    continue
                claim = await worker.claim_one(lease_id=request.lease_id)
                if claim is not None:
                    break
                await asyncio.sleep(0.1)
            if claim is None:
                raise RecoveryRefusedError("archive_claim_not_available")
            await worker.materialize_claim(claim, require_versions=True)
            async with sessions() as session:
                parent = await session.get(ServiceExecutionLease, request.lease_id)
                child = await session.get(ServiceExecutionLease, request.child_lease_id)
                trial = await session.get(Trial, request.trial_id)
                expected_state = "succeeded" if request.trial_state == "materializing" else request.trial_state
                if parent is not None and parent.materialization_state != "committed":
                    return {"status": "deferred", "reason": "archive_materialization_deferred",
                            "lease_id": str(parent.id), "materialization_state": parent.materialization_state,
                            "error_code": parent.materialization_error_code,
                            "constraint": "execution_leases_materialization_group_check" if
                            "execution_leases_materialization_group_check" in (parent.materialization_error_message or "") else None}
                if (parent is None or child is None or trial is None or trial.state != expected_state
                        or trial.attempt_count != request.attempt
                        or parent.materialization_state != "committed" or child.materialization_state != "committed"
                        or parent.source_cleanup_state != "retained" or child.source_cleanup_state != "retained"
                        or parent.source_retain_until is None
                        or not parent.canonical_trajectory_sha256 or not parent.canonical_atif_sha256
                        or child.materialization_attempts < 1
                        or parent.canonical_trajectory_sha256 != child.canonical_trajectory_sha256
                        or parent.canonical_atif_sha256 != child.canonical_atif_sha256
                        or digest_bytes(canonical_document(trial.result)) != request.trial_result_sha256):
                    raise RecoveryRefusedError("canonical_commit_incomplete")
                if request.trial_state != "materializing" and (trial.failure_reason != request.failure_reason
                        or digest_bytes(canonical_document(trial.failure_message)) != request.failure_message_sha256
                        or trial.finished_at != request.finished_at):
                    raise RecoveryRefusedError("terminal_trial_outcome_changed")
                return {"status": "committed", "lease_id": str(parent.id), "child_lease_id": str(child.id),
                        "trial_state": trial.state, "execution_attempts": trial.attempt_count,
                        "materialization_attempts": parent.materialization_attempts,
                        "source_retain_until": parent.source_retain_until.isoformat()}
    finally:
        source.close()
        canonical.close()
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-json", required=True)
    parser.add_argument("--platform", type=Path, required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.alarm(1700)
    try:
        if len(args.request_json.encode()) > 16384:
            raise RecoveryRefusedError("request_size")
        request = TerminalArchiveRecoveryRequest.model_validate_json(args.request_json)
        profile = json.loads((args.platform / "profile.json").read_bytes())
        environment = json.loads((args.platform / "environment.json").read_bytes())
        if (profile.get("candidate_sha") != request.installed_candidate
                or environment.get("namespace") != request.namespace or environment.get("cluster_id") != request.cluster_id):
            raise RecoveryRefusedError("platform_binding_changed")
        result = asyncio.run(recover(request, ControlPlaneSettings()))
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "committed" else 1
    except Exception as error:
        reason = str(error) if isinstance(error, RecoveryRefusedError) else "terminal_archive_recovery_incomplete"
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,119}", reason):
            reason = "terminal_archive_recovery_incomplete"
        print(json.dumps({"status": "blocked", "reason": reason, "error_class": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
