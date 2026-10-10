"""Real migrated DB / ACL-bearing dump / isolated socket restore / S3 records."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import boto3
import docker
import httpx
import psycopg
import pytest
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import Artifact, LlmCall, Task, Team, TeamMembership, TeamQuota, Trial, User
from loom.nebius_restore import (
    RESTORE_SCRIPT,
    RestoreError,
    download_backup,
    snapshot_sql,
    verify_restored_records,
)
from loom.security.secret_store import DecryptError, LocalEncryptedSecretStore
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password
from tests.support.minio import MINIO_TEST_IMAGE
from tests.support.minio_images import prepare_test_image
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.docker


def archive(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.uid, info.gid, info.mode = len(data), 999, 999, 0o600
            tar.addfile(info, io.BytesIO(data))
    return output.getvalue()


def test_real_acl_dump_restores_without_source_roles_and_verifies_s3(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    with (
        PostgresContainer("postgres:16") as source,
        MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).with_kwargs(tmpfs={"/data": "rw,size=536870912"}).waiting_for(
            HttpWaitStrategy(9000, "/minio/health/cluster")
        ) as storage,
    ):
        url = make_url(source.get_connection_url()).set(drivername="postgresql+psycopg")
        monkeypatch.setenv("LOOM_DB_URL", url.render_as_string(hide_password=False))
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "head"],
            cwd=root,
            env=os.environ.copy(),
            check=True,
            capture_output=True,
        )
        cfg = storage.get_config()
        s3 = boto3.client(
            "s3",
            endpoint_url="http://" + cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name="us-east-1",
        )
        buckets = {name: "restore-" + name for name in ("backup", "artifacts", "trajectories")}
        for bucket in buckets.values():
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        refs = {}
        for name, bucket in (
            ("output", buckets["artifacts"]),
            ("trajectory", buckets["trajectories"]),
            ("atif", buckets["trajectories"]),
        ):
            data = (name + "-saved-data").encode()
            version = s3.put_object(Bucket=bucket, Key=name, Body=data)["VersionId"]
            refs[name] = {
                "bucket": bucket,
                "key": name,
                "size_bytes": len(data),
                "version_id": version,
            }
        trajectory = {}
        for name in ("trajectory", "atif"):
            ref = refs[name]
            trajectory.update(
                {
                    name + "_uri": "s3://" + ref["bucket"] + "/" + name,
                    name + "_size_bytes": ref["size_bytes"],
                    name + "_version_id": ref["version_id"],
                }
            )
        engine = create_engine(url)
        trial, team = uuid4(), uuid4()
        try:
            with engine.begin() as db:
                db.execute(insert(Team).values(id=team, name="restore-local-test"))
                db.execute(
                    insert(Task).values(
                        id="restore-test", checksum="a" * 64, config={}, source="test"
                    )
                )
                db.execute(
                    insert(Trial).values(
                        id=trial,
                        team_id=team,
                        task_id="restore-test",
                        config={},
                        requires_caps={},
                        state="succeeded",
                        submitted_at=datetime.now(UTC),
                        result={"aggregate_reward": 0},
                        trajectory_index=trajectory,
                    )
                )
                db.execute(
                    insert(LlmCall).values(
                        id=uuid4(),
                        team_id=team,
                        trial_id=trial,
                        step_id="restore",
                        model="fixture",
                        dialect="openai",
                        input_tokens=5,
                        output_tokens=2,
                        cost_usd=0,
                        rate_card_hash="fixture",
                    )
                )
                db.execute(
                    insert(Artifact).values(
                        id=uuid4(),
                        trial_id=trial,
                        team_id=team,
                        artifact_type="file",
                        name="output",
                        content_hash="b" * 64,
                        storage={"files": [refs["output"]]},
                    )
                )
                # --no-owner alone still dumps ACLs referencing these source-only roles.
                for role in ("loom_service", "loom_control_plane", "loom_gateway", "loom_actuator"):
                    db.execute(text(f"CREATE ROLE {role} NOLOGIN"))
                    db.execute(text(f"GRANT SELECT ON public.trials TO {role}"))
                snapshot = db.execute(text(snapshot_sql([str(trial)]))).scalar_one()
                generated = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "loom.nebius_restore",
                        "baseline-sql",
                        "--trial-id",
                        str(trial),
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                cli_baseline = db.execute(text(generated)).scalar_one()
        finally:
            engine.dispose()
        baseline = {
            "schema_revision": snapshot["schema_revision"],
            "trials": [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"objects", "trajectory_index"}
                }
                for row in snapshot["trials"]
            ],
        }
        assert baseline == cli_baseline
        pg = source.get_wrapped_container()
        dump = pg.exec_run(
            ["pg_dump", "-U", source.username, "-d", source.dbname, "-Fc", "--no-owner"]
        )
        assert dump.exit_code == 0
        key = "loom-nebius-platform/test.dump"
        s3.put_object(
            Bucket=buckets["backup"],
            Key=key,
            Body=dump.output,
            Metadata={"sha256": hashlib.sha256(dump.output).hexdigest()},
        )
        request = {
            "namespace": "loom-nebius-platform",
            "buckets": buckets,
            "backup_key": key,
            "max_backup_bytes": 16 * 1024 * 1024,
            "baseline": baseline,
        }
        download_backup(request, s3, tmp_path)
        client = docker.from_env()
        restored = client.containers.run(
            "postgres:16",
            command=["sleep", "300"],
            entrypoint=[],
            detach=True,
            user="999:999",
            read_only=True,
            network_mode="none",
            tmpfs={
                "/restore": "rw,uid=999,gid=999,size=536870912",
                "/code": "rw,uid=999,gid=999,size=1048576",
            },
        )
        try:
            for directory, files in (
                ("/restore", {"loom.dump": (tmp_path / "loom.dump").read_bytes()}),
                (
                    "/code",
                    {
                        "restore.sh": RESTORE_SCRIPT.encode(),
                        "records.sql": snapshot_sql([str(trial)]).encode(),
                    },
                ),
            ):
                subprocess.run(
                    ["docker", "exec", "-i", restored.id, "tar", "-x", "-C", directory],
                    input=archive(files),
                    check=True,
                    capture_output=True,
                )
            result = restored.exec_run(["sh", "/code/restore.sh"])
            if result.exit_code:
                # Private test temp files retain diagnostics, never dump rows into CI logs.
                for name in ("pg_restore", "records", "initdb", "start"):
                    log = restored.exec_run(["cat", "/restore/" + name + ".log"])
                    (tmp_path / (name + ".log")).write_bytes(log.output)
            assert result.exit_code == 0, result.output.decode()
            assert restored.exec_run(["test", "-f", "/restore/database-stopped"]).exit_code == 0
            actual = json.loads(restored.exec_run(["cat", "/restore/records.json"]).output)
            assert actual == snapshot
            summary = verify_restored_records(request, actual, s3)
            assert (
                summary["trial_count"]
                == summary["llm_call_count"]
                == summary["artifact_count"]
                == 1
            )
            assert summary["canonical_object_count"] == 3
            # Changed canonical contents fail without GET or alternative endpoints.
            changed = json.loads(json.dumps(actual))
            changed["trials"][0]["objects"][0]["files"][0]["size_bytes"] += 1
            with pytest.raises(RestoreError, match="canonical-reference-size-mismatch"):
                verify_restored_records(request, changed, s3)
        finally:
            restored.remove(force=True)
            client.close()


def test_management_backup_restores_two_owner_identities_and_environment_registry(tmp_path, monkeypatch, platform_inputs):
    from scripts.ops.nebius_management_proofs import verify_backup_object

    from loom import nebius_platform_bootstrap as bootstrap
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget
    from loom.db.schema import TeamMembership, User
    from loom.nebius_environment_contract import new_environment_registration
    from loom_service.password_auth import hash_password, verify_password
    from tests.unit.test_nebius_environment_contract import foundation_from

    tables = ("users", "teams", "team_memberships", "nebius_environments", "nebius_platform_budgets")
    snapshot = "SELECT jsonb_build_object('schema', (SELECT version_num FROM alembic_version)," + ",".join(
        f"'{name}', (SELECT jsonb_agg(row ORDER BY row::text) FROM (SELECT to_jsonb(t) row FROM {name} t) q)"
        for name in tables) + ");"
    with (PostgresContainer("postgres:16", dbname="loom") as source,
          MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).with_kwargs(tmpfs={"/data": "rw,size=536870912"}).waiting_for(
              HttpWaitStrategy(9000, "/minio/health/cluster")) as storage):
        url = make_url(source.get_connection_url()).set(drivername="postgresql+psycopg")
        connection = url.set(drivername="postgresql").render_as_string(hide_password=False)
        monkeypatch.setattr(bootstrap, "database_url", lambda *_args: connection)
        monkeypatch.setenv("LOOM_DB_URL", connection)
        monkeypatch.setenv("LOOM_DB_SERVICE_PASSWORD", "management-restore-test-only-" + "x" * 24)
        bootstrap.bootstrap_management_database({"namespace": "loom-nebius-management"})
        foundation = foundation_from(platform_inputs[0])
        engine = create_engine(url)
        try:
            with engine.begin() as db:
                for name in ("alice", "bob"):
                    owner, team = uuid4(), uuid4()
                    db.execute(insert(Team).values(id=team, name=name))
                    db.execute(insert(User).values(id=owner, username=name, username_normalized=name, status="active",
                                                  password_hash=hash_password("restore-test-" + name)))
                    db.execute(insert(TeamMembership).values(team_id=team, user_id=owner, role="owner"))
                    registration = new_environment_registration(foundation, environment_id=uuid4(), incarnation=uuid4(),
                        owner_user_id=owner, owner_team_id=team, slug=name)
                    row = registration.model_dump()
                    db.execute(insert(NebiusEnvironment).values(**{key: row[key] for key in row if key in NebiusEnvironment.__table__.columns}))
                db.execute(insert(NebiusPlatformBudget).values(cluster_id=foundation.platform_config["cluster_id"],
                    cpu_millis=3000, memory_mib=8192, storage_mib=20480, ephemeral_storage_mib=32768))
                baseline = db.execute(text(snapshot)).scalar_one()
        finally:
            engine.dispose()
        dump = source.get_wrapped_container().exec_run(["pg_dump", "-U", source.username, "-d", source.dbname, "-Fc", "--no-owner"])
        assert dump.exit_code == 0
        digest = hashlib.sha256(dump.output).hexdigest()
        cfg = storage.get_config()
        s3 = boto3.client("s3", endpoint_url="http://" + cfg["endpoint"], aws_access_key_id=cfg["access_key"],
                          aws_secret_access_key=cfg["secret_key"], region_name="us-east-1")
        bucket, namespace = "management-recovery-test", "loom-nebius-management"
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        key = namespace + "/2026/09/24/000000-" + digest[:12] + ".dump"
        s3.put_object(Bucket=bucket, Key=key, Body=dump.output, Metadata={"sha256": digest})
        proof = verify_backup_object(client=s3, bucket=bucket, namespace=namespace, job_uid=str(uuid4()),
            report={"backup_key": key, "sha256": digest, "bytes": len(dump.output)}, max_bytes=16 * 1024**2)
        request = {"namespace": namespace, "buckets": {"backup": bucket}, "backup_key": proof["key"], "max_backup_bytes": 16 * 1024**2}
        download_backup(request, s3, tmp_path)
        client = docker.from_env()
        restored = client.containers.run("postgres:16", command=["sleep", "300"], entrypoint=[], detach=True,
            user="999:999", read_only=True, network_mode="none",
            tmpfs={"/restore": "rw,uid=999,gid=999,size=536870912", "/code": "rw,uid=999,gid=999,size=1048576"})
        try:
            for directory, files in (("/restore", {"loom.dump": (tmp_path / "loom.dump").read_bytes()}),
                                     ("/code", {"restore.sh": RESTORE_SCRIPT.encode(), "records.sql": snapshot.encode()})):
                subprocess.run(["docker", "exec", "-i", restored.id, "tar", "-x", "-C", directory],
                               input=archive(files), check=True, capture_output=True)
            result = restored.exec_run(["sh", "/code/restore.sh"])
            assert result.exit_code == 0
            actual = json.loads(restored.exec_run(["cat", "/restore/records.json"]).output)
            assert actual == baseline
            assert {row["application_namespace"] for row in actual["nebius_environments"]} == {"loom-dev-alice", "loom-dev-bob"}
            assert len({row["owner_user_id"] for row in actual["nebius_environments"]}) == 2
            owners = {row["username"]: row for row in actual["users"] if row["username"] in {"alice", "bob"}}
            assert set(owners) == {"alice", "bob"}
            assert all(verify_password("restore-test-" + name, row["password_hash"]) for name, row in owners.items())
            assert restored.exec_run(["test", "-f", "/restore/database-stopped"]).exit_code == 0
        finally:
            restored.remove(force=True)
            client.close()
            s3.close()


_KEY = bytes(range(32))
_ROLE_PASSWORD = "application-restore-test-role-password"


def _bootstrap(monkeypatch, pg):
    url = make_url(pg.get_connection_url()).set(drivername="postgresql+psycopg")
    connection = url.set(drivername="postgresql").render_as_string(hide_password=False)
    # Only replace the production namespace/hostname validation at this local
    # fixture boundary. Actual role creation, migrations and grants run intact.
    monkeypatch.setattr(bootstrap, "database_url", lambda *_args: connection)
    monkeypatch.setenv("LOOM_DB_URL", connection)
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", _ROLE_PASSWORD)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + uuid4().hex * 2)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + uuid4().hex * 2)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    return url


def _restore_dump(pg, dump):
    container = pg.get_wrapped_container()
    assert container.put_archive("/tmp", archive({"application.dump": dump}))
    restored = container.exec_run([
        "pg_restore", "-U", pg.username, "-d", pg.dbname,
        "--no-owner", "--no-privileges", "--exit-on-error", "--jobs=1",
        "/tmp/application.dump",
    ])
    # Do not print dump contents, password hashes or database diagnostics.
    assert restored.exit_code == 0


@pytest.mark.timeout(120)
async def test_restored_application_bootstrap_login_isolation_and_artifact_access(monkeypatch):
    with (
        PostgresContainer("postgres:16", dbname="loom") as source,
        PostgresContainer("postgres:16", dbname="loom") as restored,
        MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).with_kwargs(
            tmpfs={"/data": "rw,size=536870912"},
        ).waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster")) as storage,
        ExitStack() as clients,
    ):
        source_url = _bootstrap(monkeypatch, source)
        cfg = storage.get_config()
        endpoint = "http://" + cfg["endpoint"]
        s3 = clients.enter_context(closing(boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1",
            aws_access_key_id=cfg["access_key"], aws_secret_access_key=cfg["secret_key"],
        )))
        bucket = "restored-application-artifacts"
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        owners = {}
        engine = create_engine(source_url)
        try:
            with engine.begin() as db:
                db.execute(insert(Task).values(id="restore-application", checksum="a" * 64, config={}, source="test"))
                for name in ("alice", "bob"):
                    team, user, trial, artifact, lease = (uuid4() for _ in range(5))
                    payload = ("restored result for " + name).encode()
                    key = f"trials/{team}/{trial}/attempts/1/bundles/{artifact}/files/result.txt"
                    version = s3.put_object(Bucket=bucket, Key=key, Body=payload)["VersionId"]
                    owners[name] = {"team": team, "trial": trial, "key": key, "payload": payload, "version": version}
                    db.execute(insert(Team).values(id=team, name="restore-" + name))
                    db.execute(insert(TeamQuota).values(team_id=team))
                    db.execute(insert(User).values(id=user, username=name, username_normalized=name,
                        status="active", is_platform_admin=False, password_hash=hash_password("restore-test-" + name),
                        password_set_at=datetime.now(UTC)))
                    db.execute(insert(TeamMembership).values(team_id=team, user_id=user, role="owner"))
                    db.execute(insert(Trial).values(id=trial, team_id=team, task_id="restore-application",
                        state="succeeded", config={}, requires_caps={}, submitted_at=datetime.now(UTC),
                        result={"aggregate_reward": 1}, attempt_count=1))
                    db.execute(insert(Artifact).values(id=artifact, team_id=team, trial_id=trial,
                        artifact_type="loom.execution-runtime-evidence.v1", name="runtime_evidence",
                        producer_kind=None, control_producer_kind="service_execution", control_producer_id=lease,
                        content_hash="sha256:" + hashlib.sha256(payload).hexdigest(),
                        storage={"files": [{"relative_path": "result.txt", "bucket": bucket, "key": key,
                            "size_bytes": len(payload), "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                            "media_type": "text/plain", "version_id": version}]},
                        visibility="team", share_status="pending_scan", safety_state="verified_internal",
                        access_class="team_runtime", provenance={"lease_id": str(lease), "generation": 1}))
        finally:
            engine.dispose()

        source_async = create_async_engine(source_url)
        try:
            async with async_sessionmaker(source_async)() as session:
                store = LocalEncryptedSecretStore(session, master_key=_KEY)
                await store.put(namespace="restored-test", value="disposable-test-value")
                await session.commit()
        finally:
            await source_async.dispose()
        dump = source.get_wrapped_container().exec_run([
            "pg_dump", "-U", source.username, "-d", source.dbname, "-Fc", "--no-owner",
        ])
        assert dump.exit_code == 0
        source.get_wrapped_container().stop()
        _restore_dump(restored, dump.output)

        restored_url = make_url(restored.get_connection_url()).set(drivername="postgresql+psycopg")
        service_url = restored_url.set(username="loom_service", password=_ROLE_PASSWORD)
        with pytest.raises(psycopg.OperationalError):
            with psycopg.connect(service_url.set(drivername="postgresql").render_as_string(hide_password=False)):
                pytest.fail("A database dump must not recreate source login roles")
        _bootstrap(monkeypatch, restored)
        role_engine = create_engine(service_url)
        try:
            with role_engine.connect() as db:
                assert db.execute(text("SELECT current_user")).scalar_one() == "loom_service"
                assert db.execute(text("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls FROM pg_roles WHERE rolname=current_user")).scalar_one() is False
        finally:
            role_engine.dispose()

        monkeypatch.setenv("LOOM_ENV", "development")
        monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
        settings = LoomServiceSettings(_env_file=None, db_url=service_url.render_as_string(hide_password=False),
            db_url_pool=None, service_mode="api_only", minio_endpoint=endpoint,
            minio_access_key=cfg["access_key"], minio_secret_key=cfg["secret_key"], minio_region="us-east-1",
            artifacts_bucket=bucket, control_plane_url="http://unused-control-plane/", gateway_url="http://unused-gateway/")
        monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(bytes(reversed(_KEY))).decode())
        wrong_key_app = create_app(settings)
        with pytest.raises(DecryptError, match="startup validation failed"):
            async with wrong_key_app.router.lifespan_context(wrong_key_app):
                pytest.fail("An incompatible secret-store key must reject startup")
        assert not hasattr(wrong_key_app.state, "session_factory")

        monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(_KEY).decode())
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            for name, owner in owners.items():
                async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
                    path = f"/api/v1/trials/{owner['trial']}"
                    assert (await client.get(path)).status_code == 401
                    wrong_password = await client.post("/api/v1/auth/login", json={"username": name, "password": "incorrect"})
                    assert wrong_password.status_code == 401
                    login = await client.post("/api/v1/auth/login", json={"username": name, "password": "restore-test-" + name})
                    assert login.status_code == 200
                    assert login.json()["current_team"]["id"] == str(owner["team"])
                    detail = await client.get(path)
                    assert detail.status_code == 200
                    assert detail.json()["state"] == "succeeded"
                    download = await client.get(path + "/artifacts/download", params={"key": owner["key"]})
                    assert download.status_code == 200 and download.content == owner["payload"]
                    foreign = owners["bob" if name == "alice" else "alice"]
                    foreign_path = f"/api/v1/trials/{foreign['trial']}"
                    assert (await client.get(foreign_path)).status_code == 403
                    assert (await client.get(foreign_path + "/artifacts/download", params={"key": foreign["key"]})).status_code == 403
                    if name == "alice":
                        # A genuinely missing disposable object fails its own
                        # download; Bob's next login/download must still work.
                        s3.delete_object(Bucket=bucket, Key=owner["key"], VersionId=owner["version"])
                        missing = await client.get(path + "/artifacts/download", params={"key": owner["key"]})
                        assert missing.status_code == 404
                        assert (await client.get(path)).status_code == 200
        assert not hasattr(app.state, "session_factory")
