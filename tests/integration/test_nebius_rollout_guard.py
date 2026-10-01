"""Exercise the real Postgres boundary between dispatch and an idle rollout."""
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.nebius_rollout_guard import acquire, admission_open, release
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.task_image_materializations import claim_task_image_materialization
from tests.integration import test_service_execution_leases as execution
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def bound_pool_guard(database_guard, isolated_migration_postgres_url, monkeypatch, tmp_path):
    """Double Kubernetes only; execute the installed guard command against SQL."""
    import base64
    import json
    import os
    import subprocess
    import sys
    from uuid import uuid4

    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    url = isolated_migration_postgres_url
    state.secret["data"]["control-plane-url"] = base64.b64encode(url.encode()).decode()
    runtime = {"metadata": {"name": "loom-control-plane-test", "uid": str(uuid4())}}
    # The owning ops/cluster tests prove the backend and workload graph. Here
    # only those remote reads are doubled, not settings, the command or SQL.
    monkeypatch.setattr(api, "_database", lambda target: state.pod)
    monkeypatch.setattr(api, "_runtime", lambda target: runtime)
    read = api._run
    environment = {**{key: value for key, value in os.environ.items() if not key.startswith("LOOM_CP_")}, "LOOM_CP_DB_URL": url,
        "LOOM_CP_MINIO_ACCESS_KEY": "test-access", "LOOM_CP_MINIO_SECRET_KEY": "test-secret",
        "LOOM_CP_STEP_JWT_SIGNING_KEY": "bound-guard-test-signing-key-000000"}
    state.runtime_environment, state.directory, state.commands = environment, tmp_path, []

    def transport(args):
        if args[:7] != ["exec", "-n", state.target.namespace, "pod/loom-control-plane-test",
                "-c", "loom-control-plane", "--"]:
            return read(args)
        state.commands.append(args)
        assert args[7] == "python"
        result = subprocess.run([sys.executable, *args[8:]], capture_output=True, check=False,
            timeout=30, cwd=state.directory, env=state.runtime_environment)
        state.process = result
        if result.returncode:
            raise PoolMigrationError("guard_acquire")
        return json.loads(result.stdout)

    monkeypatch.setattr(api, "_run", transport)
    return api, state, url


@pytest.mark.parametrize("source", ["direct", "pooled", "dotenv"])
@pytest.mark.asyncio
async def test_bound_acquire_rejects_actual_runtime_database_drift_before_sql(bound_pool_guard, source):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state, url = bound_pool_guard
    # Both URLs connect successfully to real SQL, so connection failure cannot
    # make this test pass. Exact loaded credential/config correspondence is
    # required even when the two URLs currently happen to reach the same DB.
    alternate = url + ("&" if "?" in url else "?") + "application_name=private-runtime-marker"
    if source == "direct":
        state.runtime_environment["LOOM_CP_DB_URL"] = alternate
    elif source == "pooled":
        state.runtime_environment["LOOM_CP_DB_URL_POOL"] = alternate
    else:
        (state.directory / ".env").write_text("LOOM_CP_DB_URL_POOL=" + alternate + "\n")
    engine = create_async_engine(url)
    try:
        with pytest.raises(PoolMigrationError):
            api.guard(state.target, "acquire")
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT count(*) FROM nebius_rollout_guard")) == 0
        assert len(state.commands) == 1
        assert b"private-runtime-marker" not in state.process.stdout + state.process.stderr
        assert url.encode() not in state.process.stdout + state.process.stderr
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM nebius_rollout_guard"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_bound_acquire_keeps_real_guard_ownership_and_admission_semantics(bound_pool_guard):
    api, state, url = bound_pool_guard
    engine = create_async_engine(url)
    owner, candidate = str(api.request.registration.spec.operation_id), api.request.registration.candidate["candidate_sha"]
    try:
        assert api.guard(state.target, "acquire") == {"status": "acquired"}
        async with AsyncSession(engine) as session, session.begin():
            assert not await admission_open(session)
            row = (await session.execute(text("SELECT owner, candidate_sha FROM nebius_rollout_guard"))).one()
            assert tuple(row) == (owner, candidate)
        assert api.guard(state.target, "acquire") == {"status": "skipped_locked"}
        assert len(state.commands) == 2
        assert url.encode() not in state.process.stdout + state.process.stderr
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM nebius_rollout_guard"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_idle_check_excludes_inflight_admission_and_persists_across_connections(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine) as scheduler, AsyncSession(engine) as deploy:
            async with scheduler.begin():
                assert await admission_open(scheduler)
                async with deploy.begin():
                    result = await acquire(deploy, owner="test-rollout", candidate="a" * 40)
                    assert result == {"status": "skipped_busy", "reason": "admission_in_progress"}
            async with deploy.begin():
                assert (await acquire(deploy, owner="test-rollout", candidate="a" * 40))["status"] == "acquired"
                async with scheduler.begin():
                    assert not await admission_open(scheduler)
        # The deploy connection is gone; rollout or runner restarts cannot lift the pause.
        async with AsyncSession(engine) as session:
            async with session.begin():
                assert not await admission_open(session)
                assert await claim_task_image_materialization(session, builder_id="test", cpu_arch="x86_64") is None
                assert (await acquire(session, owner="other", candidate="b" * 40))["status"] == "skipped_locked"
            with pytest.raises(ValueError, match="owner"):
                async with session.begin():
                    await release(session, owner="other", candidate="a" * 40)
            async with session.begin():
                await release(session, owner="test-rollout", candidate="a" * 40)
            async with session.begin():
                assert await admission_open(session)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM nebius_rollout_guard WHERE owner = 'test-rollout'"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_queued_work_does_not_block_but_reservations_do(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    now = datetime.now(UTC)
    try:
        # Roll this fixture back; it must not leave active work in the shared DB.
        async with AsyncSession(engine) as session:
            trial_id, target = await execution._seed_ready_trial(session, now=now)
            assert (await acquire(session, owner="test-queued", candidate="a" * 40))["status"] == "acquired"
            with pytest.raises(ExecutionProvisioningBlockedError, match="platform_deploying"):
                await execution._reserve(session, trial_id=trial_id, target=target, now=now)
            await release(session, owner="test-queued", candidate="a" * 40)
            await execution._reserve(session, trial_id=trial_id, target=target, now=now)
            result = await acquire(session, owner="test-queued", candidate="a" * 40)
            assert result["status"] == "skipped_busy"
            assert result["active"]["executions"] == 1
            await session.rollback()
    finally:
        await engine.dispose()


def test_operator_cli_acquires_and_releases(isolated_migration_postgres_url, monkeypatch):
    import json
    import subprocess
    import sys

    monkeypatch.setenv("LOOM_CP_DB_URL", isolated_migration_postgres_url)
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "test-access")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "test-secret")
    command = [sys.executable, "-m", "loom.nebius_rollout_guard"]
    result = subprocess.run([*command, "acquire", "--owner", "cli-test", "--candidate", "a" * 40],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "acquired"
    result = subprocess.run([*command, "release", "--owner", "cli-test", "--candidate", "b" * 40],
                            capture_output=True, text=True)
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "Rollout guard unavailable; no automatic deployment or resume\n"
    result = subprocess.run([*command, "observe", "--owner", "cli-test", "--candidate", "a" * 40],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "held"}
    result = subprocess.run([*command, "release", "--owner", "cli-test", "--candidate", "a" * 40],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "released"


@pytest.mark.asyncio
async def test_recovery_observation_matches_both_owner_and_candidate_without_writing(isolated_migration_postgres_url):
    from loom import nebius_rollout_guard as guard

    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            assert await guard.observe(session, owner="recovery", candidate="a" * 40) == {"status": "open"}
            await acquire(session, owner="recovery", candidate="a" * 40)
            assert await guard.observe(session, owner="recovery", candidate="a" * 40) == {"status": "held"}
            assert await guard.observe(session, owner="foreign", candidate="a" * 40) == {"status": "skipped_locked"}
            assert await guard.observe(session, owner="recovery", candidate="b" * 40) == {"status": "skipped_locked"}
            assert not await admission_open(session)
            await release(session, owner="recovery", candidate="a" * 40)
            assert await guard.observe(session, owner="recovery", candidate="a" * 40) == {"status": "open"}
    finally:
        await engine.dispose()


def test_operator_cli_observes_pause_without_releasing(isolated_migration_postgres_url, monkeypatch):
    import json
    import subprocess
    import sys

    monkeypatch.setenv("LOOM_CP_DB_URL", isolated_migration_postgres_url)
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "test-access")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "test-secret")
    command = [sys.executable, "-m", "loom.nebius_rollout_guard"]
    try:
        result = subprocess.run([*command, "acquire", "--owner", "observe-cli", "--candidate", "a" * 40],
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        for owner, candidate, expected in (("observe-cli", "a" * 40, "held"),
                                           ("observe-cli", "b" * 40, "skipped_locked"),
                                           ("foreign", "a" * 40, "skipped_locked")):
            result = subprocess.run([*command, "observe", "--owner", owner, "--candidate", candidate],
                                    capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout) == {"status": expected}
    finally:
        result = subprocess.run([*command, "release", "--owner", "observe-cli", "--candidate", "a" * 40],
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_release_rejects_wrong_candidate_and_retains_admission_pause(isolated_migration_postgres_url):
    from loom.nebius_rollout_guard import observe

    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine) as session:
            async with session.begin():
                assert (await acquire(session, owner="same-owner", candidate="a" * 40))["status"] == "acquired"
            with pytest.raises(ValueError, match="candidate"):
                async with session.begin():
                    await release(session, owner="same-owner", candidate="b" * 40)
            # A failed recovery for another candidate must retain the persisted
            # pause and its original owner, including across transactions.
            async with session.begin():
                assert not await admission_open(session)
                assert await observe(session, owner="same-owner", candidate="a" * 40) == {"status": "held"}
                assert await release(session, owner="same-owner", candidate="a" * 40) == {"status": "released"}
            async with session.begin():
                assert await admission_open(session)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("action", ["acquire", "observe", "release"])
@pytest.mark.parametrize("candidate_args", [[], ["--candidate", ""]])
def test_operator_cli_requires_candidate_for_every_action(action, candidate_args):
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "loom.nebius_rollout_guard", action, "--owner", "cli-test", *candidate_args],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 2
    assert "--candidate" in result.stderr


def test_operator_observation_reads_real_persisted_guard_without_service(isolated_migration_postgres_url):
    import json

    import psycopg
    from scripts.ops.deploy_nebius_platform import rollout_guard
    from sqlalchemy.engine import make_url

    url = make_url(isolated_migration_postgres_url).set(drivername="postgresql")
    with psycopg.connect(url.render_as_string(hide_password=False), autocommit=True) as database:
        database.execute("INSERT INTO nebius_rollout_guard(id, owner, candidate_sha) VALUES (1, %s, %s)",
                         ("test-persisted-owner", "a" * 40))

        class DatabaseKubectl:
            def run(self, *command):
                assert "statefulset/loom-postgres" in command and "psql" in command
                cursor = database.execute(command[-1])
                result = None
                while True:
                    if cursor.description:
                        result = cursor.fetchone()[0]
                    if not cursor.nextset():
                        break
                return json.dumps(result)

        kube = DatabaseKubectl()
        assert rollout_guard(kube, "test-platform", "observe", "test-persisted-owner", "a" * 40) == {"status": "held"}
        assert rollout_guard(kube, "test-platform", "observe", "test-persisted-owner", "b" * 40) == {"status": "skipped_locked"}
        assert rollout_guard(kube, "test-platform", "observe", "another-owner", "a" * 40) == {"status": "skipped_locked"}
        database.execute("DELETE FROM nebius_rollout_guard")
        assert rollout_guard(kube, "test-platform", "observe", "test-persisted-owner", "a" * 40) == {"status": "open"}


def test_periodic_read_only_probe_tracks_busy_idle_and_held_guard(isolated_migration_postgres_url):
    """The exact operator SQL sees reservations and never creates/releases a guard."""
    import asyncio
    import json
    from types import SimpleNamespace

    from scripts.ops.nebius_idle_rollout import idle_snapshot

    async def scenario():
        engine = create_async_engine(isolated_migration_postgres_url)
        try:
            async with AsyncSession(engine) as session:
                trial_id, target = await execution._seed_ready_trial(session, now=datetime.now(UTC))
                await execution._reserve(session, trial_id=trial_id, target=target, now=datetime.now(UTC))
                # Capture the exact query emitted by the operator transport.
                queries = []
                idle_snapshot(SimpleNamespace(run=lambda *args: queries.append(args[-1]) or json.dumps({
                    'locked': False, 'active': {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0},
                })), 'platform')
                sql = queries[0].split('; ')[2]
                busy = await session.scalar(text(sql))
                assert busy['active']['executions'] == 1
                assert busy['locked'] is False
                assert (await acquire(session, owner='periodic-check', candidate='a' * 40))['status'] == 'skipped_busy'
                await session.rollback()
                idle = await session.scalar(text(sql))
                assert not any(idle['active'].values()) and idle['locked'] is False
                assert (await acquire(session, owner='periodic-check', candidate='a' * 40))['status'] == 'acquired'
                held = await session.scalar(text(sql))
                assert held['locked'] is True
                assert not await admission_open(session)
                await release(session, owner='periodic-check', candidate='a' * 40)
                assert (await session.scalar(text(sql)))['locked'] is False
                await session.rollback()
        finally:
            await engine.dispose()
    asyncio.run(scenario())
