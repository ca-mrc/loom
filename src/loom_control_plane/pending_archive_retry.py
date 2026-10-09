"""One audited requeue of a parked Oracle archive; never re-execute its Trial."""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.data_lifecycle_registry import RuntimeLifecycleScope
from loom.db.schema import Artifact, ServiceExecutionLease, Trial
from loom.db.schema_startup import service_schema_head
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.pending_archive_recovery import (
    AUDIT_KEY,
    PROJECTION_FIELDS,
    ArchiveRecoveryRequest,
    GitSha,
    RecoveryRefusedError,
    Sha256,
    project_oracle,
    qualify_inputs,
)
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    MaterializationResult,
    ServiceExecutionMaterializer,
    _legacy_verifier_reward_projection,
)

RETRY_AUDIT_KEY = "pending_archive_retry"


def _legacy_retry_intact(lease: ServiceExecutionLease, trial: Trial, artifact: Artifact) -> bool:
    metadata = artifact.artifact_metadata or {}
    for key, code in (("legacy_verifier_archival_recovery", "verifier_reward_drift"),
                      ("usage_roundoff_archival_recovery", "usage_output_identity_drift")):
        audit = metadata.get(key)
        if not isinstance(audit, dict):
            continue
        try:
            runtime = ExecutionRuntimeResultV1.model_validate((trial.result or {}).get("runtime_result"))
            outcome_matches = (_legacy_verifier_reward_projection(runtime) if code == "verifier_reward_drift"
                               else runtime.status == "succeeded" and trial.config.get("agent_name") == "terminus-2")
            if (audit.get("error_code") == code and audit.get("output_manifest_sha256") == lease.output_manifest_sha256
                    and datetime.fromisoformat(audit["requested_at"]) == lease.materialization_recovery_requested_at
                    and trial.state == "failed" and trial.failure_reason == "output_unavailable"
                    and outcome_matches):
                return True
        except (ValueError, KeyError, TypeError):
            pass
    return False


class ArchiveRetryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    team_id: UUID
    lease_id: UUID
    previous_request_sha256: Sha256
    previous_claim_id: UUID
    candidate_sha: GitSha
    schema_head: str = Field(pattern=r"^[0-9]{4}$")

    @property
    def digest(self) -> str:
        return digest_bytes(canonical_document(self.model_dump(mode="json")))


def _original_request(artifact: Artifact, request: ArchiveRetryRequest) -> ArchiveRecoveryRequest:
    audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY)
    if not isinstance(audit, dict):
        raise RecoveryRefusedError("original_recovery_audit_missing")
    original = ArchiveRecoveryRequest.model_validate(audit.get("request"))
    failure = audit.get("failure")
    if (original.digest != request.previous_request_sha256
            or audit.get("request_sha256") != original.digest
            or audit.get("claim_id") != str(request.previous_claim_id)
            or not isinstance(failure, dict) or failure.get("code") != "recovery_incomplete"
            or failure.get("claim_id") != audit.get("claim_id")
            or original.team_id != request.team_id or original.lease_id != request.lease_id
            or original.artifact_id != artifact.id):
        raise RecoveryRefusedError("original_recovery_audit_changed")
    return original


async def _qualify(
    session: AsyncSession, materializer: ServiceExecutionMaterializer,
    request: ArchiveRetryRequest, lease: ServiceExecutionLease, trial: Trial, artifact: Artifact,
) -> ArchiveRecoveryRequest:
    original = _original_request(artifact, request)
    if (lease.team_id != request.team_id or trial.team_id != request.team_id
            or trial.id != original.trial_id or artifact.team_id != request.team_id
            or artifact.trial_id != trial.id or artifact.control_producer_kind != "service_execution"
            or artifact.control_producer_id != lease.id or lease.trial_id != trial.id
            or lease.attempt != original.attempt or trial.attempt_count != original.attempt
            or original.attempt != 1 or trial.state != "materializing"
            or (trial.result or {}).get("runtime_result", {}).get("status") != "succeeded"
            or lease.execution_role != "attempt" or lease.parent_lease_id is not None
            or lease.desired_state != "deleted" or lease.observed_state != "deleted"
            or lease.cleanup_state != "complete" or lease.output_commit_state != "committed"
            or lease.resource_generation != original.generation or lease.output_generation != original.generation
            or any(getattr(lease, key) is None for key in ("finalized_at", "revoked_at", "deleted_at"))
            or lease.canonical_trajectory_sha256 is not None or lease.canonical_atif_sha256 is not None
            or lease.source_cleanup_state != "not_ready" or lease.source_retain_until is not None
            or await session.scalar(select(ServiceExecutionLease.id).where(
                ServiceExecutionLease.parent_lease_id == lease.id).limit(1)) is not None):
        raise RecoveryRefusedError("parked_archive_identity_changed")
    await qualify_inputs(session, original, lease, trial)
    projected = await project_oracle(session, materializer, team_id=request.team_id, lease_id=request.lease_id)
    if any(projected[key] != getattr(original, key) for key in PROJECTION_FIELDS):
        raise RecoveryRefusedError("original_projection_changed")
    return original


async def requeue_parked_archive(
    sessions: async_sessionmaker[AsyncSession], materializer: ServiceExecutionMaterializer,
    request: ArchiveRetryRequest, *, apply: bool = False,
) -> dict[str, Any]:
    """Preview without writes or atomically append an audit and reopen storage work.

    An ambiguous apply must be resolved through audit readback, never replayed.
    The installed caller must bind candidate_sha to its verified platform profile.
    """
    async with sessions.begin() as session:
        if not apply:
            await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text("SET LOCAL lock_timeout='5s'"))
        await session.execute(text("SET LOCAL statement_timeout='10s'"))
        if not await admission_open(session):
            raise RecoveryRefusedError("rollout_guard_held")
        if (request.schema_head != service_schema_head()
                or list(await session.scalars(text("SELECT version_num FROM alembic_version"))) != [request.schema_head]):
            raise RecoveryRefusedError("schema_binding_changed")
        lease = await session.get(ServiceExecutionLease, request.lease_id, with_for_update=apply)
        if lease is None or lease.team_id != request.team_id:
            raise RecoveryRefusedError("source_identity_missing")
        trial = await session.get(Trial, lease.trial_id, with_for_update=apply)
        artifact_query = select(Artifact).where(
            Artifact.team_id == request.team_id, Artifact.control_producer_kind == "service_execution",
            Artifact.control_producer_id == lease.id)
        artifact = (await session.scalars(artifact_query.with_for_update() if apply else artifact_query)).one_or_none()
        if trial is None or artifact is None:
            raise RecoveryRefusedError("source_identity_missing")
        audit = (artifact.artifact_metadata or {}).get(AUDIT_KEY, {})
        if (RETRY_AUDIT_KEY in (artifact.artifact_metadata or {})
                or lease.materialization_state != "unavailable"
                or lease.materialization_error_code != "recovery_incomplete"
                or lease.materialization_recovery_requested_at is not None
                or lease.materialization_claim_id is not None or lease.materialization_claim_expires_at is not None
                or lease.materialization_next_attempt_at is not None
                or type(audit.get("previous_materialization_attempts")) is not int
                or lease.materialization_attempts != audit["previous_materialization_attempts"] + 1):
            raise RecoveryRefusedError("parked_archive_not_eligible")
        await _qualify(session, materializer, request, lease, trial, artifact)
        now = datetime.now(UTC)
        report = {"status": "requeued" if apply else "preview", "request_sha256": request.digest,
                  "request": request.model_dump(mode="json"),
                  "original_audit_sha256": digest_bytes(canonical_document(audit)),
                  "previous_materialization_attempts": lease.materialization_attempts}
        if apply:
            artifact.artifact_metadata = {**(artifact.artifact_metadata or {}), RETRY_AUDIT_KEY: {
                **report, "requested_at": now.isoformat()}}
            # The terminal-state guard requires the distinct audit to exist first.
            await session.flush([artifact])
            lease.materialization_state = "pending"
            lease.materialization_next_attempt_at = now
            lease.materialization_recovery_requested_at = now
            lease.updated_at = now
            await session.flush()
        return report


async def qualify_requeued_archive(
    session: AsyncSession, materializer: ServiceExecutionMaterializer,
    lease: ServiceExecutionLease, trial: Trial, artifact: Artifact,
    result: MaterializationResult | None = None,
) -> None:
    """Recheck recovery bindings before copies and under the canonical commit locks."""
    audit = (artifact.artifact_metadata or {}).get(RETRY_AUDIT_KEY)
    if audit is None:
        if (lease.materialization_recovery_requested_at is not None
                and (AUDIT_KEY in (artifact.artifact_metadata or {}) or not _legacy_retry_intact(lease, trial, artifact))):
            raise MaterializationIntegrityError("archive_retry_qualification_failed")
        return
    try:
        request = ArchiveRetryRequest.model_validate(audit["request"])
        if (audit.get("request_sha256") != request.digest or request.lease_id != lease.id
                or audit.get("original_audit_sha256") != digest_bytes(canonical_document(
                    (artifact.artifact_metadata or {}).get(AUDIT_KEY)))
                or lease.materialization_recovery_requested_at is None
                or datetime.fromisoformat(audit["requested_at"]) != lease.materialization_recovery_requested_at):
            raise RecoveryRefusedError("archive_retry_audit_changed")
        original = await _qualify(session, materializer, request, lease, trial, artifact)
        if result is not None and (
            result.events_sha256 != original.events_sha256 or result.atif_sha256 != original.atif_sha256
            or result.events_version_id in {None, "", "null"} or result.atif_version_id in {None, "", "null"}
            or any(row.version_id in {None, "", "null"} for row in (*result.files, *result.source_evidence))
        ):
            raise RecoveryRefusedError("archive_retry_canonical_identity_changed")
    except (ValueError, KeyError, TypeError, RuntimeError) as exc:
        raise MaterializationIntegrityError("archive_retry_qualification_failed") from exc


def qualify_retry_platform(request: ArchiveRetryRequest, platform: Path, *, readback: bool = False) -> None:
    profile = json.loads((platform / "profile.json").read_bytes())
    environment = json.loads((platform / "environment.json").read_bytes())
    if (environment.get("namespace") != RuntimeLifecycleScope.from_environ().namespace
            or (not readback and (profile.get("candidate_sha") != request.candidate_sha
                                  or service_schema_head() != request.schema_head))):
        raise RecoveryRefusedError("platform_binding_changed")


async def _run(request: ArchiveRetryRequest, *, apply: bool, readback: bool) -> dict[str, Any]:
    settings = ControlPlaneSettings()
    source_config = ServiceExecutionSourceConfig.from_settings(settings)
    if source_config is None:
        raise RecoveryRefusedError("independent_source_store_required")
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    canonical = MinioObjectStore(endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(), secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region)
    source = cast(MinioObjectStore, source_config.build_store(MinioObjectStore))
    try:
        async with asyncio.timeout(300):
            if readback:
                async with sessions.begin() as session:
                    await session.execute(text("SET TRANSACTION READ ONLY"))
                    lease = await session.get(ServiceExecutionLease, request.lease_id)
                    artifact = (await session.scalars(select(Artifact).where(
                        Artifact.team_id == request.team_id, Artifact.control_producer_kind == "service_execution",
                        Artifact.control_producer_id == request.lease_id))).one_or_none()
                    audit = (artifact.artifact_metadata or {}).get(RETRY_AUDIT_KEY) if artifact else None
                    if (lease is None or lease.team_id != request.team_id or not isinstance(audit, dict)
                            or audit.get("request_sha256") != request.digest):
                        raise RecoveryRefusedError("retry_audit_not_found")
                    if artifact is None or _original_request(artifact, request).namespace != RuntimeLifecycleScope.from_environ().namespace:
                        raise RecoveryRefusedError("lifecycle_namespace_changed")
                    return {"status": "observed", "audit": audit, "archive_state": lease.materialization_state,
                            "materialization_attempts": lease.materialization_attempts}
            materializer = ServiceExecutionMaterializer(session_factory=sessions, source_store=source,
                source_bucket=source_config.bucket, canonical_store=canonical,
                artifacts_bucket=settings.artifacts_bucket, trajectories_bucket=settings.trajectories_bucket,
                source_retention_seconds=settings.service_execution_source_retention_sec)
            return await requeue_parked_archive(sessions, materializer, request, apply=apply)
    finally:
        source.close()
        canonical.close()
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-json", required=True)
    parser.add_argument("--platform", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--readback", action="store_true")
    args = parser.parse_args()
    try:
        if len(args.request_json.encode()) > 4096:
            raise RecoveryRefusedError("request_size")
        request = ArchiveRetryRequest.model_validate_json(args.request_json)
        qualify_retry_platform(request, args.platform, readback=args.readback)
        print(json.dumps(asyncio.run(_run(request, apply=args.apply, readback=args.readback)), sort_keys=True))
        return 0
    except Exception as error:
        code = str(error) if isinstance(error, RecoveryRefusedError) else "archive_retry_incomplete"
        print(json.dumps({"status": "blocked", "reason": code}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
