"""Exercise committed verifier handoff with the actual bootstrap actuator login."""

from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import Artifact, ArtifactUploadFile, ServiceExecutionLease, Team, Trial
from loom.execution_runtime_contract import ExecutionRuntimeResultV1, VerifierTopology
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    committed_handoff_files,
    finalize_committed_service_execution,
    record_committed_runtime_result,
)
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401
from tests.integration.test_service_execution_leases import (
    _hand_off_to_verifier,
    _requirements,
    _reserve,
    _runtime_contract,
    _runtime_result_payload,
    _seed_ready_trial,
)


async def test_bootstrap_actuator_reads_committed_handoff_without_upload_write_authority(
    platform_database: str,  # noqa: F811 -- shared disposable PostgreSQL fixture
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    password = "restricted-handoff-role-" + "a" * 32
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", password)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    url = make_url(platform_database).set(drivername="postgresql+psycopg")
    owner_engine = create_async_engine(url)
    actuator_engine = create_async_engine(url.set(username="loom_actuator", password=password))
    owners = async_sessionmaker(owner_engine, expire_on_commit=False)
    restricted = async_sessionmaker(actuator_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    upload_tables = ("artifact_upload_sessions", "artifact_upload_files")
    try:
        async with owners.begin() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now,
                requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=_runtime_contract(now=now, verifier_execution="separate_execution"))
            await _hand_off_to_verifier(session, parent_id=lease.id, now=now)
            lease.finalized_at = None
            lease.observed_state = "finalizing"
            artifact = Artifact(
                id=uuid4(), artifact_type="loom.trial-artifact-bundle.v1", name="trial_bundle",
                team_id=lease.team_id, trial_id=trial_id,
                control_producer_kind="service_execution", control_producer_id=lease.id,
                content_hash="sha256:" + "1" * 64,
                artifact_upload_session_id=lease.output_upload_session_id,
                manifest_sha256=lease.output_manifest_sha256,
                file_count=1, stored_size_bytes=4, unpacked_size_bytes=4,
                provenance={"lease_id": str(lease.id), "generation": lease.resource_generation,
                            "runtime_contract_sha256": lease.runtime_contract_sha256},
            )
            session.add(artifact)
            file = ArtifactUploadFile(
                session_id=lease.output_upload_session_id, file_index=0,
                preallocated_artifact_id=artifact.id, relative_path="artifacts/workspace.tar",
                artifact_name="workspace", artifact_type="loom.trial-artifact-bundle.v1",
                producer="container", media_type="application/x-tar", role="payload",
                archive_format="none", expected_max_bytes=4, expected_size=4,
                expected_sha256="sha256:" + "3" * 64,
                computed_sha256="sha256:" + "3" * 64, actual_size=4, state="verified",
            )
            session.add(file)
            document = _runtime_result_payload(lease, started_at=now)
            document.update(status="timed_out", partial_evidence=True, container_roles=["execution", "agent"])
            document["phases"] = document["phases"][:1]
            document["phases"][0]["timed_out"] = True
            # Use the real artifact-backed payload path, including its existing
            # tenant, producer and generation validation, before handoff reads.
            document["outputs"] = [{
                "source_path": f"out/part-{i:04}.json", "relative_path": f"artifacts/part-{i:04}.json",
                "kind": "task_artifact", "required": False, "state": "captured", "size_bytes": 4,
                "sha256": "sha256:" + "3" * 64,
            } for i in range(512)]
            result = ExecutionRuntimeResultV1.model_validate(document)
            reported = await record_committed_runtime_result(session, lease_id=lease.id,
                generation=lease.generation, runtime_result=result, observed_at=now)
            assert reported.payload_json["schema_version"] == "loom.execution-event-payload-reference.v1"
            lease_id, artifact_id, upload_id = lease.id, artifact.id, file.session_id
            foreign_team_id = uuid4()
            session.add(Team(id=foreign_team_id, name="foreign-handoff-role"))

        async with restricted.begin() as session:
            assert await session.scalar(text("SELECT current_user")) == "loom_actuator"
            parent = await session.get(ServiceExecutionLease, lease_id)
            files, keys = await committed_handoff_files(session, parent=parent)
            assert files == [("artifacts/workspace.tar", 4, "sha256:" + "3" * 64)]
            assert len(keys) == 1 and keys[0].endswith(f"artifacts/{artifact_id}/artifacts/workspace.tar")
            parent.output_commit_state = "uploading"
            with pytest.raises(ValueError, match="verifier_handoff_unavailable"):
                await committed_handoff_files(session, parent=parent)
            parent.output_commit_state = "committed"

        # Fresh sessions/transactions exercise rejection after each tamper; the
        # owner restores facts before the next case or successful finalization.
        for field, invalid in (("team_id", foreign_team_id), ("control_producer_id", uuid4()),
                               ("provenance", {"lease_id": str(lease_id), "generation": 999})):
            async with owners.begin() as session:
                artifact = await session.get(Artifact, artifact_id)
                original = getattr(artifact, field)
                setattr(artifact, field, invalid)
            try:
                async with restricted.begin() as session:
                    with pytest.raises(ServiceExecutionConflict, match="artifact identity drift"):
                        await finalize_committed_service_execution(session, lease_id=lease_id, observed_at=now)
            finally:
                async with owners.begin() as session:
                    artifact = await session.get(Artifact, artifact_id)
                    setattr(artifact, field, original)

        async with owners.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            original_manifest = parent.output_manifest_sha256
            parent.output_manifest_sha256 = "sha256:" + "9" * 64
        async with restricted.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            with pytest.raises(ValueError, match="verifier_handoff_unavailable"):
                await committed_handoff_files(session, parent=parent)
        async with owners.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            parent.output_manifest_sha256 = original_manifest
        async with owners.begin() as session:
            source = await session.get(ArtifactUploadFile, (upload_id, 0))
            source.state = "planned"
        async with restricted.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            with pytest.raises(ValueError, match="verifier_handoff_unavailable"):
                await committed_handoff_files(session, parent=parent)
        async with owners.begin() as session:
            source = await session.get(ArtifactUploadFile, (upload_id, 0))
            source.state = "verified"
        async with restricted.begin() as session:
            assert await finalize_committed_service_execution(session, lease_id=lease_id, observed_at=now)
            trial = await session.get(Trial, trial_id)
            assert trial.state == "running"
            assert trial.result["runtime_result"]["status"] == "timed_out"
            assert trial.result["verifier_execution"] == {"state": "pending", "parent_lease_id": str(lease_id)}

        # Replay bootstrap must retain SELECT without broadening upload or
        # unrelated administration authority. These are real SQL ACL checks.
        bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
        login = url.set(drivername="postgresql", username="loom_actuator", password=password)
        with psycopg.connect(login.render_as_string(hide_password=False), autocommit=True) as db:
            for table in upload_tables:
                assert db.execute("SELECT has_table_privilege(current_user,%s,'SELECT')", (table,)).fetchone() == (True,)
                assert db.execute("SELECT has_table_privilege(current_user,%s,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')", (table,)).fetchone() == (False,)
                for statement in (f"INSERT INTO {table} DEFAULT VALUES", f"UPDATE {table} SET state=state WHERE false", f"DELETE FROM {table} WHERE false"):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege) as denied:
                        db.execute(statement)
                    assert denied.value.sqlstate == "42501"
            for statement in ("SELECT * FROM secrets LIMIT 0", "UPDATE artifacts SET team_id=team_id WHERE false", "DELETE FROM users WHERE false"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    db.execute(statement)
        # Revoke only the new SELECTs in this disposable DB: the same actual
        # handoff helper must fail again, then normal bootstrap restores it.
        with psycopg.connect(platform_database, autocommit=True) as db:
            db.execute("REVOKE SELECT ON artifact_upload_sessions, artifact_upload_files FROM loom_actuator")
        async with restricted.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            with pytest.raises(ProgrammingError) as denied:
                await committed_handoff_files(session, parent=parent)
            assert denied.value.orig.sqlstate == "42501"
            await session.rollback()
        bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
        async with restricted.begin() as session:
            parent = await session.get(ServiceExecutionLease, lease_id)
            assert (await committed_handoff_files(session, parent=parent))[0] == files
            assert parent.output_upload_session_id == upload_id
    finally:
        await actuator_engine.dispose()
        await owner_engine.dispose()
