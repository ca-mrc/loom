"""Reference generation must agree independently and clean up on refusal."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import scripts.build_application_schema_reference as builder

from loom.application_schema_inventory import ApplicationSchemaInventory, ApplicationSchemaObject
from loom.application_schema_provisioning import derive_identity
from loom.application_schema_reference import (
    APPLICATION_SCHEMA_REVISIONS,
    application_schema_reference,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disagreement", "observation_error", "cancelled"])
async def test_reference_builder_cleans_its_container_on_failure(monkeypatch, failure) -> None:
    container = MagicMock()
    container.with_bind_ports.return_value = container
    container.__enter__.return_value = container
    container.get_connection_url.return_value = "postgresql+psycopg://localhost/isolated"
    factory = MagicMock(return_value=container)
    monkeypatch.setattr(builder, "PostgresContainer", factory)
    first = ApplicationSchemaInventory(16, ())
    second = ApplicationSchemaInventory(
        16, (ApplicationSchemaObject("routine", "unexpected", "f" * 64),)
    )
    if failure == "disagreement":
        effects = [first, second]
        error = RuntimeError
    elif failure == "observation_error":
        effects = [RuntimeError("observation refused")]
        error = RuntimeError
    else:
        effects = [asyncio.CancelledError()]
        error = asyncio.CancelledError
    observer = AsyncMock(side_effect=effects)
    monkeypatch.setattr(builder, "_observe_fresh_database", observer)
    with pytest.raises(error):
        await builder.build_application_schema_reference()
    container.with_bind_ports.assert_called_once_with(5432, ("127.0.0.1", None))
    container.__exit__.assert_called_once()
    assert factory.call_args.args == (builder.application_reference_postgres_image(),)
    assert factory.call_args.kwargs["driver"] == "psycopg"
    if failure == "disagreement":
        first_identity = observer.call_args_list[0].args[1]
        second_identity = observer.call_args_list[1].args[1]
        assert first_identity.database != second_identity.database
        assert first_identity.db_role != second_identity.db_role


@pytest.mark.asyncio
async def test_reference_builder_rejects_wrong_actual_server_major(monkeypatch) -> None:
    container = MagicMock()
    container.with_bind_ports.return_value = container
    container.__enter__.return_value = container
    container.get_connection_url.return_value = "postgresql+psycopg://localhost/isolated"
    monkeypatch.setattr(builder, "PostgresContainer", MagicMock(return_value=container))
    monkeypatch.setattr(
        builder, "_observe_fresh_database", AsyncMock(return_value=ApplicationSchemaInventory(16, ()))
    )
    with pytest.raises(RuntimeError, match="PostgreSQL major"):
        await builder.build_application_schema_reference(postgres_major=17)
    container.__exit__.assert_called_once()


def test_reference_builder_rejects_a_database_address(monkeypatch, capsys) -> None:
    monkeypatch.setattr(builder.sys, "argv", ["builder", "postgresql://live.example/application"])
    with pytest.raises(SystemExit, match="accepts no arguments"):
        builder.main()
    assert capsys.readouterr().out == ""


@pytest.mark.asyncio
async def test_builder_emits_all_fixed_major_profile_pairs(monkeypatch) -> None:
    build = AsyncMock(side_effect=application_schema_reference)
    monkeypatch.setattr(builder, "build_application_schema_reference", build)
    result = await builder._build_profiles()
    assert set(result) == {"16", "17"}
    assert build.await_count == 2 * 6 * len(APPLICATION_SCHEMA_REVISIONS)
    for major in (16, 17):
        profiles = {"legacy-owner", "sealed-owner", "staging-readonly-legacy-owner", "staging-readonly-sealed-owner", "cnpg-staging-legacy-owner", "cnpg-staging-sealed-owner"}
        assert set(result[str(major)]) == set(APPLICATION_SCHEMA_REVISIONS)
        for revision in APPLICATION_SCHEMA_REVISIONS:
            assert set(result[str(major)][revision]) == profiles
            for profile in profiles:
                build.assert_any_await(postgres_major=major, profile=profile, revision=revision)
                assert result[str(major)][revision][profile]["postgres_major"] == major
                assert result[str(major)][revision][profile]["profile"] == profile


@pytest.mark.asyncio
async def test_reference_cancellation_reaps_real_migration_child_before_database_cleanup(
    monkeypatch,
) -> None:
    bootstrap = MagicMock()
    bootstrap.apply_role_and_database = AsyncMock()
    database = MagicMock()
    database._converge_roles = AsyncMock()
    database.destroy = AsyncMock()
    monkeypatch.setattr(builder, "PsycopgSharedFixtureSqlExecutor", lambda _url: bootstrap)
    monkeypatch.setattr(builder, "ReferenceDatabase", lambda _url: database)
    create_process = asyncio.create_subprocess_exec
    child = None
    created = asyncio.Event()

    async def spawn(*args, **kwargs):
        nonlocal child
        assert args[1:3] == ("-m", "alembic")
        assert kwargs["stdin"] == asyncio.subprocess.DEVNULL
        child = await create_process(
            builder.sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        assert child.pid > 0 and child.returncode is None
        # Return ownership without awaiting interpreter output/startup. The
        # caller resumes through process assignment before the event waiter can
        # run, so cancellation tests the production kill/reap boundary.
        created.set()
        return child

    async def destroy(_identity):
        assert child is not None and child.returncode is not None

    database.destroy.side_effect = destroy
    monkeypatch.setattr(builder.asyncio, "create_subprocess_exec", spawn)
    identity = derive_identity("reference-cancel")
    task = asyncio.create_task(
        builder._observe_fresh_database("postgresql+psycopg://unused/isolated", identity)
    )
    try:
        await asyncio.wait_for(created.wait(), timeout=10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        assert child is not None and child.returncode is not None
        database.destroy.assert_awaited_once_with(identity)
        database._converge_roles.assert_not_awaited()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if child is not None and child.returncode is None:
            child.kill()
            await child.wait()
        if child is not None:
            assert child.returncode is not None


def test_script_path_entrypoint_rejects_arguments_without_provisioning():
    import subprocess
    result = subprocess.run(
        [builder.sys.executable, str(builder._ROOT / "scripts/build_application_schema_reference.py"), "invalid"],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode != 0
    assert result.stdout == ""
    assert result.stderr.strip() == "reference builder accepts no arguments or database address"
