"""Persist parent/verifier archive ACKs through the real PostgreSQL constraints."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Artifact, ArtifactUploadSession, ServiceExecutionLease, Trial
from loom.execution_contract import VerifierTopology
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    MaterializationResult,
    ServiceExecutionMaterializer,
)
from tests.integration.test_service_execution_leases import (
    _deferred_verifier_contract,
    _requirements,
    _reserve,
    _separate_attempt_awaiting_verifier,
)


@pytest.mark.parametrize("outcome", ["materializing", "failed"])
@pytest.mark.parametrize("committed", [True, False])
async def test_parent_archive_settles_deferred_verifier_atomically(
    postgres_url: str, outcome: str, committed: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    store = FakeObjectStore()
    materializer = ServiceExecutionMaterializer(
        session_factory=sessions, source_store=store, source_bucket="artifacts",
        canonical_store=store, artifacts_bucket="artifacts", trajectories_bucket="trajectories",
    )
    try:
        trial_id, parent, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session:
            child = await _reserve(
                session, trial_id=trial_id, target=target, now=now,
                requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=_deferred_verifier_contract(),
                parent_lease_id=parent.id,
            )
            upload = ArtifactUploadSession(
                id=uuid4(), team_id=child.team_id, commit_kind="service_execution_output",
                service_execution_lease_id=child.id,
                service_execution_generation=child.resource_generation,
                service_execution_role="verifier",
                service_execution_runtime_contract_sha256=child.runtime_contract_sha256,
                service_execution_candidate_sha="1" * 40,
                service_execution_task_revision_sha256="sha256:" + "2" * 64,
                service_execution_command_identity_sha256="sha256:" + "3" * 64,
                idempotency_key=uuid4().hex, request_digest="sha256:" + "6" * 64,
                prefix=f"service-execution/{child.id}/{uuid4().hex}/", state="committed",
                expected_total_max_bytes=1024, canonical_manifest_json={},
                manifest_sha256="sha256:" + "7" * 64,
                committed_marker_sha256="sha256:" + "8" * 64,
                expires_at=now + timedelta(hours=1), committed_at=now,
            )
            session.add(upload)
            await session.flush()
            child.output_commit_state = "committed"
            child.output_generation = child.resource_generation
            child.output_upload_session_id = upload.id
            child.output_manifest_sha256 = upload.manifest_sha256
            child.output_marker_sha256 = upload.committed_marker_sha256
            child.output_committed_at = now
            child.materialization_state = "pending"
            child.materialization_next_attempt_at = now
            for row in (child,):
                row.revoked_at = now
                row.desired_state = row.observed_state = "deleted"
                row.cleanup_state = "complete"
                row.cleanup_requested_at = now
                row.cleanup_deadline_at = now + timedelta(seconds=30)
                row.deleted_at = row.finalized_at = now
            parent_row = await session.get(ServiceExecutionLease, parent.id)
            parent_row.materialization_state = "pending"
            parent_row.materialization_next_attempt_at = now
            trial = await session.get(Trial, trial_id)
            trial.state = outcome
            trial.result = {"reward": 0.0, "verifier_execution": {"lease_id": str(child.id)}}
            if outcome == "failed":
                trial.failure_reason = "execution_failed"
                trial.failure_message = "original failure"
            artifact = Artifact(
                id=uuid4(), artifact_type="loom.trial-artifact-bundle.v1", name="trial_bundle",
                team_id=parent.team_id, trial_id=trial_id,
                control_producer_kind="service_execution", control_producer_id=parent.id,
                content_hash="sha256:" + "1" * 64,
                artifact_upload_session_id=parent_row.output_upload_session_id,
                manifest_sha256=parent_row.output_manifest_sha256,
                file_count=1, stored_size_bytes=1, unpacked_size_bytes=1,
                provenance={"lease_id": str(parent.id), "generation": parent.resource_generation,
                            "runtime_contract_sha256": parent.runtime_contract_sha256},
            )
            session.add(artifact)
            await session.commit()
            child_id, artifact_id, original_result = child.id, artifact.id, trial.result
        claim = await materializer.claim_one(lease_id=parent.id)
        assert claim is not None
        result = MaterializationResult(
            artifact_id=artifact_id, files=(), source_evidence=(), events=(),
            events_body=b"", atif_body=b'{"schema_version":"1"}',
            events_uri=f"s3://trajectories/{trial_id}/events.jsonl",
            atif_uri=f"s3://trajectories/{trial_id}/atif.json",
            events_sha256="sha256:" + "9" * 64, atif_sha256="sha256:" + "a" * 64,
            final_trial_state="succeeded" if outcome == "materializing" else "failed",
            failure_reason=None, preserve_trial_outcome=True,
        )
        if committed:
            assert await materializer._commit(claim, result)
            # A late worker cannot ACK or mutate either archive again.
            assert not await materializer._commit(claim, result)
        else:
            await materializer._unavailable(claim, MaterializationIntegrityError("source_corrupt"))
        async with sessions() as session:
            parent_row = await session.get(ServiceExecutionLease, parent.id)
            child = await session.get(ServiceExecutionLease, child_id)
            trial = await session.get(Trial, trial_id)
            assert parent_row.materialization_state == child.materialization_state == (
                "committed" if committed else "unavailable")
            assert child.materialization_next_attempt_at is None
            assert child.materialization_claim_id is None
            assert trial.result == original_result and trial.attempt_count == 1
            if committed:
                assert child.materialization_attempts == 1
                assert child.canonical_trajectory_sha256 == parent_row.canonical_trajectory_sha256
                assert child.canonical_atif_sha256 == parent_row.canonical_atif_sha256
                assert child.materialization_error_code is child.materialization_error_message is None
                assert child.source_cleanup_state == "retained"
                assert child.source_retain_until == parent_row.source_retain_until
                assert trial.state == ("succeeded" if outcome == "materializing" else "failed")
                if outcome == "failed":
                    assert (trial.failure_reason, trial.failure_message) == (
                        "execution_failed", "original failure")
            else:
                assert child.materialization_error_code == "source_corrupt"
                assert child.materialization_error_message
                assert child.canonical_trajectory_sha256 is child.canonical_atif_sha256 is None
                assert child.source_cleanup_state == "not_ready"
    finally:
        await engine.dispose()
