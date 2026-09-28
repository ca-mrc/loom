"""Actual empty PostgreSQL bootstrap, repeated migration and role boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import io
import ipaddress
import json
import os
import sys
import tarfile
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom import nebius_platform_bootstrap as bootstrap
from tests.unit.test_nebius_platform_render import platform_inputs, regional_inputs  # noqa: F401


async def _exercise_gateway_dispatch_role(database: str) -> None:
    """The deployed Gateway role must admit and observe an upstream dispatch."""
    import httpx
    from sqlalchemy import insert, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.auth import AuthContext
    from loom.db.schema import GatewayDispatchReceipt, Team
    from loom_llm_gateway.dispatch_audit import DispatchAudit, _RequestAudit
    from tests.integration.gateway_db import delete_gateway_trial, insert_gateway_trial

    owner_engine = create_async_engine(make_url(database).set(drivername="postgresql+psycopg"))
    gateway_engine = create_async_engine(make_url(database).set(
        drivername="postgresql+psycopg", username="loom_gateway",
        password=os.environ["LOOM_DB_GATEWAY_PASSWORD"],
    ))
    team_id, trial_id = uuid4(), uuid4()
    task_id = None
    try:
        async with owner_engine.begin() as connection:
            await connection.execute(insert(Team).values(id=team_id, name=f"dispatch-role-{team_id}"))
            task_id = await connection.run_sync(
                lambda conn: insert_gateway_trial(conn, team_id=team_id, trial_id=trial_id),
            )
        factory = async_sessionmaker(gateway_engine, expire_on_commit=False)
        audit = DispatchAudit(
            _RequestAudit(session_factory=factory),
            AuthContext(token_hash=b"", type="step", scopes=["llm:call"],
                        team_id=team_id, expires_at=None, trial_id=trial_id,
                        step_id="agent", step_jwt_id=uuid4()),
            "facade_openai", None, "model_call",
        )
        upstream_called = False

        async def upstream() -> httpx.Response:
            nonlocal upstream_called
            # Admission must already be durable before transport is allowed.
            async with factory() as session:
                receipt = await session.scalar(select(GatewayDispatchReceipt).where(
                    GatewayDispatchReceipt.trial_id == trial_id,
                ))
                assert receipt is not None and receipt.provider_outcome == "admitted"
            upstream_called = True
            return httpx.Response(200)

        response = await audit.send(upstream, deadline=None)
        assert upstream_called and response.status_code == 200
        async with factory() as session:
            receipt = await session.scalar(select(GatewayDispatchReceipt).where(
                GatewayDispatchReceipt.trial_id == trial_id,
            ))
            assert receipt is not None
            assert receipt.provider_outcome == "response_received"
            assert receipt.provider_http_status == 200
            assert receipt.provider_observed_at is not None
    finally:
        if task_id is not None:
            async with owner_engine.begin() as connection:
                await connection.run_sync(
                    lambda conn: delete_gateway_trial(conn, trial_id=trial_id, task_id=task_id),
                )
                await connection.execute(Team.__table__.delete().where(Team.id == team_id))
        await gateway_engine.dispose()
        await owner_engine.dispose()


async def _exercise_native_builder_role(database: str) -> None:
    """Run the native queue/capacity/publication path as the deployed DB role."""
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.db.schema import (
        Batch,
        TaskImageCapacityWait,
        TaskImageMaterialization,
        TaskImageMaterializationAttempt,
        TaskImagePublicationEvidence,
        Trial,
        TrialTaskImageMaterialization,
    )
    from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
    from loom_control_plane.task_image_capacity_wait import remember_capacity_wait
    from loom_control_plane.task_image_materializations import (
        claim_task_image_materialization,
        complete_task_image_materialization,
        fail_task_image_materialization,
        has_nebius_task_image_demand,
        heartbeat_task_image_materialization,
        start_task_image_materialization,
    )
    from loom_execution_capacity_collector.contracts import ResourceTotals
    from tests.integration.test_service_execution_leases import _seed_ready_trial

    owner_engine = create_async_engine(make_url(database).set(drivername="postgresql+psycopg"))
    actuator_engine = create_async_engine(make_url(database).set(
        drivername="postgresql+psycopg", username="loom_actuator",
        password=os.environ["LOOM_DB_ACTUATOR_PASSWORD"],
    ))
    owners = async_sessionmaker(owner_engine, expire_on_commit=False)
    actuators = async_sessionmaker(actuator_engine, expire_on_commit=False)
    try:
        for outcome in ("ready", "failed"):
            now = datetime.now(UTC)
            async with owners() as session, session.begin():
                trial_id, target = await _seed_ready_trial(session, now=now)
                trial = await session.get(Trial, trial_id)
                batch = Batch(id=uuid4(), team_id=trial.team_id, name="native-role-test", task_filter={},
                              trial_config={}, created_by_token_prefix="test", backend="nebius",
                              service_execution_runtime_profile={"logical_pool_id": target.logical_pool_id})
                session.add(batch)
                await session.flush()
                trial.batch_id = batch.id
                trial.requires_caps = {"worker_pool": target.logical_pool_id}
                image = TaskImageMaterialization(
                    id=uuid4(), materialization_key=uuid4().hex * 2, task_id=trial.task_id,
                    task_checksum="b" * 64, cpu_arch="x86_64",
                    task_config={
                        "schema_version": "1", "task": {"id": "native-role", "name": "native-role"},
                        "environment": {"os": "linux", "cpu_arch": "x86_64", "dockerfile": "Dockerfile"},
                        "agent": {"name": "oracle"}, "verifier": {"name": "pytest"},
                    },
                )
                session.add(image)
                await session.flush()
                session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image.id))
                image_id = image.id
            async with actuators() as session, session.begin():
                for _ in range(2):
                    await remember_capacity_wait(
                        session, target_id=target.target_id, materialization_id=image_id,
                        lease_epoch=0, pool_id=target.logical_pool_id,
                        resources=ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048),
                        now=datetime.now(UTC),
                    )
                wait = await session.get(TaskImageCapacityWait, target.target_id)
                assert wait is not None and wait.materialization_id == image_id
            async with actuators() as session, session.begin():
                row = await claim_task_image_materialization(
                    session, builder_id="native-role", cpu_arch="x86_64", nebius_pool_id=target.logical_pool_id,
                )
                assert row is not None and row.id == image_id
                assert await has_nebius_task_image_demand(session, materialization_id=image_id,
                                                         pool_id=target.logical_pool_id)
                attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
                    TaskImageMaterializationAttempt.materialization_id == image_id,
                ))
                attempt.native_build = {
                    "target_id": target.target_id, "namespace": "builds", "job_name": "role-test",
                    "state": "reserved", "reserved_at": now.isoformat(),
                    "resources": {"vcpu_millis": 1000, "memory_mib": 1024, "storage_mib": 2048},
                }
                await session.flush()
                reserved = await reserve_native_task_image_capacity(session, attempt_id=attempt.id)
                assert reserved["capacity_reserved_at"]
                wait = await session.get(TaskImageCapacityWait, target.target_id)
                assert wait is not None
                await session.delete(wait)
                ownership = {"materialization_id": image_id, "builder_id": "native-role", "lease_epoch": row.lease_epoch}
                await start_task_image_materialization(session, **ownership)
                await heartbeat_task_image_materialization(session, **ownership)
                images = {"task": "registry.example/tasks@sha256:" + "a" * 64}
                if outcome == "ready":
                    await complete_task_image_materialization(session, **ownership, registry_images=images)
                else:
                    await fail_task_image_materialization(session, **ownership, registry_images=images,
                                                         retryable=False, failure_reason="build_failed",
                                                         failure_message="bounded test failure")
                # The same actor persists cleanup acknowledgement and can read
                # publication evidence without mutable task/catalog privileges.
                attempt.native_build = {**attempt.native_build, "capacity_released_at": datetime.now(UTC).isoformat()}
                assert row.state == outcome
                evidence = await session.scalar(select(TaskImagePublicationEvidence).where(
                    TaskImagePublicationEvidence.materialization_id == image_id,
                ))
                assert evidence is not None and evidence.registry_image == images["task"]
    finally:
        await actuator_engine.dispose()
        await owner_engine.dispose()


@pytest.fixture(scope="module")
def platform_database(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    # Exercise the same verify-full TLS URL as the live bootstrap. A plaintext
    # fixture hides ConfigParser failures on the percent-encoded CA path.
    with PostgresContainer("postgres:16", dbname="loom") as postgres:
        url = make_url(postgres.get_connection_url()).set(drivername="postgresql")
        host = url.host or "localhost"
        try:
            san: x509.GeneralName = x509.IPAddress(ipaddress.ip_address(host))
        except ValueError:
            san = x509.DNSName(host)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
            .not_valid_after(datetime.now(UTC) + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([san]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256())
        )
        certificate = cert.public_bytes(serialization.Encoding.PEM)
        ca_path = tmp_path_factory.mktemp("platform-tls") / "ca.crt"
        ca_path.write_bytes(certificate)
        private_key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for filename, contents in (
                ("platform.crt", certificate),
                ("platform.key", private_key),
            ):
                entry = tarfile.TarInfo(filename)
                entry.size = len(contents)
                entry.uid = entry.gid = 999
                entry.mode = 0o600
                tar.addfile(entry, io.BytesIO(contents))
        postgres.get_wrapped_container().put_archive("/tmp", archive.getvalue())
        with psycopg.connect(url.render_as_string(hide_password=False), autocommit=True) as db:
            db.execute("ALTER SYSTEM SET ssl_cert_file = '/tmp/platform.crt'")
            db.execute("ALTER SYSTEM SET ssl_key_file = '/tmp/platform.key'")
            db.execute("ALTER SYSTEM SET ssl = 'on'")
            db.execute("SELECT pg_reload_conf()")
        tls_url = url.update_query_dict(
            {"sslmode": "verify-full", "sslrootcert": str(ca_path)}
        ).render_as_string(hide_password=False)
        deadline = time.monotonic() + 10
        while True:
            try:
                with psycopg.connect(tls_url, connect_timeout=2) as db:
                    assert db.execute(
                        "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
                    ).fetchone() == (True,)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        yield tls_url


def test_fresh_bootstrap_repeat_and_database_privileges(
    platform_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    token = "loom_ecc_" + "f" * 64
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", token)
    batch_token = "loom_br_" + "b" * 64
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", batch_token)
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", "test-password-" + role + "-" * 30)
    config = {"namespace": "loom-nebius-platform"}
    bootstrap.bootstrap_database(config)
    bootstrap.bootstrap_database(config)
    with psycopg.connect(platform_database) as connection:
        row = connection.execute(
            "SELECT count(*) FROM tokens WHERE token_hash=%s",
            (hashlib.sha256(token.encode()).digest(),),
        ).fetchone()
        assert row == (1,)
        batch_hash = hashlib.sha256(batch_token.encode()).digest()
        assert connection.execute(
            "SELECT type, scopes, team_id, expires_at, revoked_at FROM tokens WHERE token_hash=%s",
            (batch_hash,),
        ).fetchall() == [("worker", ["submit:batch"], None, None, None)]
        roles = connection.execute(
            "SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolbypassrls FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
            (["loom_service", "loom_control_plane", "loom_gateway", "loom_actuator"],),
        ).fetchall()
        assert len(roles) == 4 and all(not any(row[1:]) for row in roles)
    for role in ("gateway", "actuator"):
        url = make_url(platform_database).set(
            username="loom_" + role, password=os.environ["LOOM_DB_" + role.upper() + "_PASSWORD"]
        )
        with psycopg.connect(url.render_as_string(hide_password=False)) as connection:
            assert connection.execute("SELECT count(*) FROM execution_targets").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("DELETE FROM users")
            connection.rollback()
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("CREATE ROLE bootstrap_escape")
            connection.rollback()
            for table in ("task_image_materializations", "trial_task_image_materializations"):
                connection.execute("SELECT 1 FROM " + table + " LIMIT 0")
            assert connection.execute(
                "SELECT has_table_privilege(current_user, 'nebius_rollout_guard', 'SELECT')",
            ).fetchone() == (role == "actuator",)
            assert connection.execute(
                "SELECT has_table_privilege(current_user, 'nebius_rollout_guard', 'INSERT,UPDATE,DELETE')",
            ).fetchone() == (False,)
            if role == "actuator":
                for privilege in ("SELECT", "INSERT", "UPDATE"):
                    assert connection.execute(
                        "SELECT has_table_privilege(current_user, 'trial_resource_usage', %s)", (privilege,),
                    ).fetchone() == (True,)
            assert connection.execute(
                "SELECT has_table_privilege(current_user, 'trial_resource_usage', 'DELETE')",
            ).fetchone() == (False,)
            if role == "gateway":
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    connection.execute("DELETE FROM gateway_dispatch_receipts")
                connection.rollback()
                for table in ("batches", "task_image_materialization_attempts", "task_image_publication_evidence"):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        connection.execute("SELECT 1 FROM " + table + " LIMIT 0")
                    connection.rollback()
                for table in ("task_image_materializations", "trial_task_image_materializations"):
                    assert connection.execute(
                        "SELECT has_table_privilege(current_user, %s, 'INSERT,UPDATE,DELETE')", (table,),
                    ).fetchone() == (False,)
            else:
                for table in ("batches", "trial_task_image_materializations"):
                    assert connection.execute(
                        "SELECT has_table_privilege(current_user, %s, 'INSERT,UPDATE,DELETE')", (table,),
                    ).fetchone() == (False,)
                for table, denied in (
                    ("task_image_materializations", "INSERT,DELETE"),
                    ("task_image_materialization_attempts", "DELETE"),
                    ("task_image_publication_evidence", "UPDATE,DELETE"),
                    ("tasks", "SELECT,INSERT,UPDATE,DELETE"),
                    ("gateway_dispatch_receipts", "SELECT,INSERT,UPDATE,DELETE"),
                ):
                    assert connection.execute(
                        "SELECT has_table_privilege(current_user, %s, %s)", (table, denied),
                    ).fetchone() == (False,)
    asyncio.run(_exercise_gateway_dispatch_role(platform_database))
    # Exercise actual CP HTTP handlers and actual database policy writes. Only
    # the transport is adapted from in-cluster HTTP to an in-process ASGI app.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.admin_secret import AdminSecretVerifier
    from loom.nebius_platform_render import build_platform
    from loom_control_plane.routes import admin, service_executions

    environment, candidate, profile = request.getfixturevalue("platform_inputs")
    files = build_platform(
        environment, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2]
    )
    data = files["10-config-network.yaml"][0]["data"]
    (tmp_path / "catalog.json").write_text(data["catalog.json"])
    admin_token = "loom_admin_" + "e" * 64
    admin_path = tmp_path / "secrets.toml"
    admin_path.write_text('[admin]\ntoken = "' + admin_token + '"\n')
    app = FastAPI()
    engine = create_async_engine(make_url(platform_database).set(drivername="postgresql+psycopg"))
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(admin_token)
    app.include_router(admin.router)
    app.include_router(service_executions.router)
    with TestClient(app) as client:

        def open_request(request: object, timeout: int) -> io.BytesIO:
            from urllib.parse import urlsplit

            response = client.request(
                request.method,
                urlsplit(request.full_url).path,
                content=request.data,
                headers=dict(request.header_items()),
            )
            assert response.status_code == 200, response.text
            return io.BytesIO(response.content)

        monkeypatch.setattr(bootstrap.urllib.request, "urlopen", open_request)
        bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
        bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
        headers = {"Authorization": "Bearer " + admin_token}

        def api(method: str, path: str, body: dict | None = None) -> dict:
            response = client.request(method, "/admin/" + path, headers=headers, json=body)
            assert response.status_code == 200, response.text
            return response.json()

        assert api("GET", "execution-admission/status")["policies"] == []
        capacity_path = "execution-capacity-policies/" + environment["target_id"]
        historical = {
            "enabled": True,
            "max_nodes": 2,
            "max_vcpu_millis": 32000,
            "max_memory_mib": 131072,
            "max_storage_mib": 131072,
            "node_cpu_millis": 16000,
            "node_memory_mib": 65536,
            "node_storage_mib": 65536,
            "max_pending_jobs": 4,
            "max_unschedulable_jobs": 4,
            "max_image_pull_backoff_jobs": 0,
            "max_create_per_minute": 4,
            "observation_max_age_seconds": 300,
            "reason": "Dedicated bounded Nebius integration pool; validate current quota before activation.",
        }
        api("PUT", capacity_path, historical)
        api(
            "PUT",
            "execution-admission-policies/global/*",
            {
                "max_concurrent": 4,
                "enabled": True,
                "reason": "Nebius integration environment capacity",
            },
        )
        api(
            "PUT",
            "execution-admission-policies/pool/nebius-cpu",
            {
                "max_concurrent": 3,
                "enabled": True,
                "reason": "Deliberate operator pool limit",
            },
        )
        bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
        status = api("GET", "execution-capacity/status")
        policy = next(
            row["policy"]
            for row in status["targets"]
            if row["target_id"] == environment["target_id"]
        )
        assert all(policy[key] == value for key, value in environment["capacity_policy"].items())
        policies = api("GET", "execution-admission/status")["policies"]
        assert next(row for row in policies if row["scope_kind"] == "global")["enabled"] is False
        pool = next(row for row in policies if row["scope_kind"] == "pool")
        assert pool["enabled"] is True and pool["max_concurrent"] == 3
        assert pool["reason"] == "Deliberate operator pool limit"

        # Matching a historical reason alone cannot widen an operator edit.
        # Different reason, disabled state and a changed create rate all remain.
        for changes in (
            {"max_nodes": 1},
            {"reason": "Reviewed operator capacity"},
            {"enabled": False},
            {"max_create_per_minute": 1},
        ):
            explicit = dict(historical, **changes)
            api("PUT", capacity_path, explicit)
            api(
                "PUT",
                "execution-admission-policies/global/*",
                {
                    "max_concurrent": 4,
                    "enabled": True,
                    "reason": "Deliberate operator global limit",
                },
            )
            api(
                "PUT",
                "execution-admission-policies/pool/nebius-cpu",
                {
                    "max_concurrent": 2,
                    "enabled": True,
                    "reason": "Nebius integration environment capacity",
                },
            )
            capsys.readouterr()
            bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
            assert json.loads(capsys.readouterr().out)["retained_operator_capacity_policy"] is True
            status = api("GET", "execution-capacity/status")
            retained = next(
                row["policy"]
                for row in status["targets"]
                if row["target_id"] == environment["target_id"]
            )
            assert all(retained[key] == value for key, value in explicit.items())
            policies = api("GET", "execution-admission/status")["policies"]
            assert all(row["enabled"] for row in policies)
            assert (
                next(row for row in policies if row["scope_kind"] == "global")["max_concurrent"]
                == 4
            )
            assert (
                next(row for row in policies if row["scope_kind"] == "pool")["max_concurrent"] == 2
            )

        # Reviewed explicit concurrency remains available; null never means zero.
        bootstrap.configure_platform(
            dict(environment, max_concurrent=7), config_dir=tmp_path, admin_secret=admin_path
        )
        assert all(
            row["enabled"] and row["max_concurrent"] == 7
            for row in api("GET", "execution-admission/status")["policies"]
        )
        # Converting a protected standalone binding must close execution even
        # when the previous operator customized its numeric capacity limits.
        from loom.nebius_environment_contract import EnvironmentRegistrationV1, FoundationBinding
        from loom.nebius_environment_render import render_environment
        from tests.unit.test_nebius_environment_contract import foundation_from, registration_for

        foundation = foundation_from(environment)
        foundation = FoundationBinding.model_validate({
            **foundation.model_dump(), "public_dns_zone": environment["public_host"].split(".", 1)[1],
        })
        row = registration_for(foundation, "dev", scope="shared")
        row = EnvironmentRegistrationV1.model_validate({
            **row.model_dump(), "binding_mode": "imported",
            "application_namespace": environment["namespace"],
            "execution_namespace": environment["execution_namespace"],
            "build_namespace": environment["execution_namespace"] + "-build",
            "target_id": environment["target_id"], "public_host": environment["public_host"],
        })
        child = render_environment(row, candidate, foundation, profile=profile, keyring={},
                                   repo_root=Path(__file__).resolve().parents[2])
        (tmp_path / "catalog.json").write_text(child.files["10-config-network.yaml"][0]["data"]["catalog.json"])
        explicit = {**environment["capacity_policy"], "max_create_per_minute": 1,
                    "reason": "Custom operator limit retained across import"}
        api("PUT", capacity_path, explicit)
        for _ in range(2):
            bootstrap.configure_platform(child.config, config_dir=tmp_path, admin_secret=admin_path)
            policies = api("GET", "execution-capacity/status")["targets"]
            closed = next(item["policy"] for item in policies if item["target_id"] == row.target_id)
            assert closed["enabled"] is False
            assert all(closed[key] == value for key, value in explicit.items() if key != "enabled")
        regional_environment, regional_candidate, regional_profile = request.getfixturevalue(
            "regional_inputs"
        )
        regional_files = build_platform(
            regional_environment,
            regional_candidate,
            regional_profile,
            {},
            repo_root=Path(__file__).resolve().parents[2],
        )
        (tmp_path / "catalog.json").write_text(
            regional_files["10-config-network.yaml"][0]["data"]["catalog.json"]
        )
        bootstrap.configure_platform(
            regional_environment, config_dir=tmp_path, admin_secret=admin_path
        )
        bootstrap.configure_platform(
            regional_environment, config_dir=tmp_path, admin_secret=admin_path
        )
        secondary_id = regional_environment["regional_execution_targets"][0]["target_id"]
        regional_status = api("GET", "execution-capacity/status")["targets"]
        assert {row["target_id"] for row in regional_status} == {
            environment["target_id"],
            secondary_id,
        }
        secondary = next(row for row in regional_status if row["target_id"] == secondary_id)
        assert secondary["policy"]["max_nodes"] == 100
        assert secondary["desired_state"] == "active" and secondary["health_status"] == "unknown"
        # A guest sibling is registered and priced through the same API, but
        # remains disabled until a separate reviewed activation. It must never
        # create another physical capacity policy or duplicate the price.
        from tests.unit.test_nebius_guest_deployment import guest_inputs

        guest_environment, guest_candidate, guest_profile = guest_inputs((environment, candidate, profile))
        guest_files = build_platform(guest_environment, guest_candidate, guest_profile, {},
                                     repo_root=Path(__file__).resolve().parents[2])
        guest_data = guest_files["10-config-network.yaml"][0]["data"]
        for name in ("catalog.json", "guest-catalog.json"):
            (tmp_path / name).write_text(guest_data[name])
        for _ in range(2):
            bootstrap.configure_platform(guest_environment, config_dir=tmp_path, admin_secret=admin_path)
            rows = api("GET", "execution-capacity/status")["targets"]
            guest_row = next(item for item in rows if item["target_id"] == "nebius-guest-fixture")
            assert guest_row["desired_state"] == "disabled"
            assert guest_row["health_status"] == "unknown"
        client.portal.call(engine.dispose)
    with psycopg.connect(platform_database) as connection:
        assert connection.execute(
            "SELECT desired_state, health_status FROM execution_targets WHERE id=%s",
            (environment["target_id"],),
        ).fetchone() == ("active", "unknown")
        assert connection.execute("SELECT count(*) FROM execution_price_snapshots").fetchone() == (
            2,
        )
        assert connection.execute(
            "SELECT enabled FROM execution_target_price_bindings WHERE target_id=%s",
            (environment["target_id"],),
        ).fetchone() == (True,)
    # Replaying bootstrap must not rebind, broaden, extend or revive a token.
    asyncio.run(_exercise_native_builder_role(platform_database))
    with psycopg.connect(platform_database) as connection:
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert connection.execute(
                "SELECT has_table_privilege('loom_gateway', 'task_image_capacity_waits', %s)",
                (privilege,),
            ).fetchone() == (False,)
        connection.execute(
            "INSERT INTO teams (id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'other-authority')"
        )
    for update in (
        "scopes=ARRAY['submit:batch', 'admin:tokens']",
        "type='team'",
        "expires_at=now() + interval '1 day'",
        "revoked_at=now()",
        "team_id='11111111-1111-1111-1111-111111111111'",
    ):
        with psycopg.connect(platform_database) as connection:
            connection.execute(
                "UPDATE tokens SET " + update + " WHERE token_hash=%s", (batch_hash,)
            )
            previous = connection.execute(
                "SELECT * FROM tokens WHERE token_hash=%s", (batch_hash,)
            ).fetchone()
        with pytest.raises(ValueError, match=r"batch runner token.*another authority"):
            bootstrap.bootstrap_database(config)
        with psycopg.connect(platform_database) as connection:
            assert (
                connection.execute(
                    "SELECT * FROM tokens WHERE token_hash=%s", (batch_hash,)
                ).fetchone()
                == previous
            )
            connection.execute(
                "UPDATE tokens SET type='worker', scopes=ARRAY['submit:batch'], team_id=NULL, expires_at=NULL, revoked_at=NULL WHERE token_hash=%s",
                (batch_hash,),
            )
    # A revoked collector identity must not be silently revived on upgrade.
    with psycopg.connect(platform_database) as connection:
        connection.execute(
            "UPDATE tokens SET revoked_at=now() WHERE token_hash=%s",
            (hashlib.sha256(token.encode()).digest(),),
        )
    with pytest.raises(ValueError, match="revoked"):
        bootstrap.bootstrap_database(config)
    # Real runtime SQL failure remains diagnosable without emitting URLs,
    # credential values, SQL parameters or driver exception bodies.
    with psycopg.connect(platform_database) as connection:
        connection.execute("DROP TABLE execution_events")
    config_path = tmp_path / "environment.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setenv("LOOM_PLATFORM_CONFIG", str(config_path))
    monkeypatch.setattr(sys, "argv", ["bootstrap", "database"])
    assert bootstrap.main() == 1
    diagnostic = json.loads(capsys.readouterr().err)
    assert diagnostic == {"phase": "database", "error_type": "UndefinedTable", "sqlstate": "42P01"}


@pytest.mark.parametrize(
    ("metadata", "size_delta", "accepted"),
    [
        ({"sha256": "match"}, 0, True),
        ({"Sha256": "match"}, 0, True),
        ({"SHA256": "match"}, 0, True),
        ({"Sha256": "mismatch"}, 0, False),
        ({}, 0, False),
        ({"Sha256": "match"}, 1, False),
        ({"sha256": "match", "Sha256": "mismatch"}, 0, False),
    ],
)
def test_backup_native_s3_metadata_case_preserves_hash_and_size_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    metadata: dict[str, str],
    size_delta: int,
    accepted: bool,
) -> None:
    dump = tmp_path / "loom.dump"
    dump.write_bytes(b"bounded backup payload")
    checksum = hashlib.sha256(dump.read_bytes()).hexdigest()

    class Storage:
        def upload_file(self, filename: str, bucket: str, key: str, **kwargs: dict) -> None:
            assert filename == str(dump)
            assert bucket == "dedicated-backups"
            assert key.startswith("loom-nebius-platform/")
            assert kwargs["ExtraArgs"] == {"Metadata": {"sha256": checksum}}

        def head_object(self, **_kwargs: str) -> dict:
            return {
                "ContentLength": dump.stat().st_size + size_delta,
                "Metadata": {
                    key: checksum if value == "match" else "wrong"
                    for key, value in metadata.items()
                },
            }

    monkeypatch.setattr(bootstrap, "Path", lambda _path: dump)
    monkeypatch.setattr(bootstrap.boto3, "client", lambda *_args, **_kwargs: Storage())
    monkeypatch.setenv("LOOM_BACKUP_ACCESS_KEY", "fixture-access-key")
    monkeypatch.setenv("LOOM_BACKUP_SECRET_KEY", "fixture-secret-key")
    config = {
        "namespace": "loom-nebius-platform",
        "storage_endpoint": "https://storage.eu-north1.nebius.cloud",
        "region": "eu-north1",
        "buckets": {"backup": "dedicated-backups"},
    }
    if accepted:
        bootstrap.upload_backup(config)
        assert json.loads(capsys.readouterr().out)["sha256"] == checksum
    else:
        with pytest.raises(ValueError, match="readback mismatch"):
            bootstrap.upload_backup(config)
        assert not capsys.readouterr().out
