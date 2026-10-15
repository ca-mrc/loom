"""Qualified one-shot recovery keeps real archive fences and the rollout guard."""
from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from minio.versioningconfig import ENABLED, VersioningConfig
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.data_lifecycle_registry import bind_existing_trial_lifecycle_authority
from loom.db.schema import Artifact, LlmCall, ServiceExecutionLease, Trial
from loom.pipeline.keys import digest_bytes
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.pending_archive_recovery import RecoveryRefusedError
from loom_control_plane.terminal_archive_recovery import (
    AUDIT_KEY,
    TerminalArchiveMaterializer,
    TerminalArchiveRecoveryRequest,
    archive_sessions,
    capture_binding,
    recover,
)
from tests.integration.test_deferred_verifier_archive import _seed_deferred_archive
from tests.integration.test_service_execution_spool_materialization import (
    _config,
    _store,
    independent_minio_endpoints,  # noqa: F401 -- expose shared fixture
)


@pytest.mark.parametrize("failed", [False, True], ids=["success", "failed-outcome-preserved"])
async def test_qualified_archive_recovery_on_installed_0175(
    isolated_migration_postgres_url, independent_minio_endpoints, monkeypatch, failed,  # noqa: F811
):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-test")
    source_container, canonical_container = independent_minio_endpoints
    for container, buckets in ((source_container, ("artifacts",)), (canonical_container, ("artifacts", "trajectories"))):
        for bucket in buckets:
            container.get_client().set_bucket_versioning(bucket, VersioningConfig(ENABLED))
    source, canonical = _store(source_container), _store(canonical_container)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        trial_id, parent, child = await _seed_deferred_archive(sessions, source, now=now, failed=failed)
        # Seed the existing submission lifecycle owner; the recovery itself must
        # refuse to manufacture a missing/mismatched parent authority.
        async with sessions.begin() as session:
            await bind_existing_trial_lifecycle_authority(session, trial_id=trial_id, expected_team_id=parent.team_id)
        # The published repair must archive without installing migrations first.
        await __import__("asyncio").to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0175")
        sessions = archive_sessions(engine, legacy_baseline=True)
        async with sessions() as session:
            # Exercise the actual historical SELECT; the 0176-only baseline
            # column must not leak into the installed-schema read path.
            assert not list(await session.scalars(select(LlmCall)))
            binding = await capture_binding(session, team_id=parent.team_id, lease_id=parent.id)
            trial = await session.get(Trial, trial_id)
            before = copy.deepcopy((trial.result, trial.failure_reason, trial.failure_message, trial.finished_at, trial.attempt_count))
        request = TerminalArchiveRecoveryRequest(**binding.model_dump(), operation_id=uuid4(),
            cluster_id="mk8scluster-test", namespace="loom-test", installed_candidate="a" * 40,
            candidate_sha="b" * 40, image_ref="cr.eu-north1.nebius.cloud/test/loom-control-plane@sha256:" + "b" * 64,
            installed_image_ref="cr.eu-north1.nebius.cloud/test/loom-control-plane@sha256:" + "a" * 64,
            schema_head="0175")
        worker = TerminalArchiveMaterializer(request=request, session_factory=sessions,
            source_store=source, source_bucket="artifacts", canonical_store=canonical,
            artifacts_bucket="artifacts", trajectories_bucket="trajectories", source_retention_seconds=3600)
        async with sessions.begin() as session:
            await session.execute(text("INSERT INTO nebius_rollout_guard (id,owner,candidate_sha) VALUES (1,'test-terminal-archive',:sha)"), {"sha": "a" * 40})
        try:
            with pytest.raises(RecoveryRefusedError, match="rollout_guard_held"):
                await worker.claim_one(lease_id=parent.id)
        finally:
            async with sessions.begin() as session:
                await session.execute(text("DELETE FROM nebius_rollout_guard WHERE owner='test-terminal-archive'"))
        # Changing a review-bound projection refuses work before owning a claim.
        async with sessions.begin() as session:
            trial = await session.get(Trial, trial_id)
            trial.config = {**trial.config, "agent_version": "changed-after-inspection"}
        with pytest.raises(RecoveryRefusedError, match="terminal_archive_projection_changed"):
            await worker.claim_one(lease_id=parent.id)
        async with sessions.begin() as session:
            trial = await session.get(Trial, trial_id)
            config = dict(trial.config)
            config.pop("agent_version")
            trial.config = config
        # Never take over an expired running claim either.
        async with sessions.begin() as session:
            current = await session.get(ServiceExecutionLease, parent.id)
            current.materialization_state = "running"
            current.materialization_attempts += 1
            current.materialization_claim_id = uuid4()
            current.materialization_claim_expires_at = now - timedelta(seconds=1)
            current.materialization_started_at = now
            current.materialization_next_attempt_at = None
        with pytest.raises(RecoveryRefusedError, match="parent_archive_claim_active"):
            await worker.claim_one(lease_id=parent.id)
        async with sessions.begin() as session:
            current = await session.get(ServiceExecutionLease, parent.id)
            current.materialization_state = "pending"
            current.materialization_claim_id = current.materialization_claim_expires_at = None
            current.materialization_next_attempt_at = now
        sources = {row.object_name: row.version_id for row in source_container.get_client().list_objects(
            "artifacts", prefix=f"service-executions/{parent.team_id}/", recursive=True, include_version=True)}
        if failed:
            claim = await worker.claim_one(lease_id=parent.id)
            assert claim is not None
            result = await worker._load_and_materialize(claim)
            assert result.preserve_trial_outcome
            assert await worker._commit(claim, result)
            assert not await worker._commit(claim, result)
        else:
            source_config, canonical_config = source_container.get_config(), canonical_container.get_config()
            settings = ControlPlaneSettings(db_url=isolated_migration_postgres_url,
                minio_endpoint="http://" + canonical_config["endpoint"], minio_access_key=canonical_config["access_key"],
                minio_secret_key=canonical_config["secret_key"], minio_region="us-east-1",
                service_execution_source_endpoint="http://" + source_config["endpoint"],
                service_execution_source_access_key=source_config["access_key"], service_execution_source_secret_key=source_config["secret_key"],
                service_execution_source_region="us-east-1", service_execution_source_bucket="artifacts",
                service_execution_source_retention_sec=3600,
                step_jwt_signing_key="test-terminal-archive-signing-key")
            report = await recover(request, settings)
            assert report["status"] == "committed" and report["execution_attempts"] == 1
        assert await worker.claim_one(lease_id=parent.id) is None
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.result == before[0] and trial.attempt_count == before[4] == 1
            if failed:
                assert (trial.failure_reason, trial.failure_message, trial.finished_at) == before[1:4]
            else:
                assert trial.state == "succeeded"
            archived = await session.get(ServiceExecutionLease, parent.id)
            for lease_id in (parent.id, child.id):
                current = await session.get(ServiceExecutionLease, lease_id)
                assert current.materialization_state == "committed" and current.materialization_attempts > 0
                assert current.canonical_trajectory_sha256 == archived.canonical_trajectory_sha256
                assert current.canonical_atif_sha256 == archived.canonical_atif_sha256
                assert current.source_cleanup_state == "retained" and current.source_retain_until > now
            artifact = await session.get(Artifact, request.artifact_id)
            assert artifact.artifact_metadata[AUDIT_KEY]["operation_id"] == str(request.operation_id)
            for row in artifact.storage["files"] + artifact.storage["source_evidence"]:
                assert row["version_id"]
                response = canonical_container.get_client().get_object(row["bucket"], row["key"], version_id=row["version_id"])
                try:
                    assert digest_bytes(response.read()) == row["sha256"]
                finally:
                    response.close()
                    response.release_conn()
        assert sources == {row.object_name: row.version_id for row in source_container.get_client().list_objects(
            "artifacts", prefix=f"service-executions/{parent.team_id}/", recursive=True, include_version=True)}
    finally:
        await engine.dispose()
