"""A live-registry diagnostic must never claim an operation or release capacity."""
from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy import select

from loom.db.nebius_environment_schema import (
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
    NebiusPlatformReservation,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_environment_retirement import prepare_retirement
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize("change,failed_check", [
    (None, None), ("material", "material_undelivered"), ("source", "source_fenced"),
    ("namespace", "namespace_identities"), ("plan", "operation_plan"),
])
async def test_read_only_probe_reports_qualification_without_claiming(environment_registry, change, failed_check):
    from scripts.ops.nebius_retirement_registry_probe import observe

    _, factory, _, _ = environment_registry
    target = await prepare_retirement(environment_registry)
    async with factory.begin() as session:
        if change == "material":
            material = await session.get(NebiusEnvironmentResource, (target["source_operation_id"], "credentials:material"))
            material.phase, material.provider_identity = "applied", "private-material"
        elif change == "source":
            source = await session.get(NebiusEnvironmentOperation, target["source_operation_id"])
            source.error_code = "private-error"
        elif change == "namespace":
            target["namespace_uids"][next(iter(target["namespace_uids"]))] = "10000000-0000-4000-8000-000000000001"
        elif change == "plan":
            operation = await session.get(NebiusEnvironmentOperation, target["operation_id"])
            operation.plan_json = operation.plan_json | {"source_operation_id": "private-source"}
    async with factory() as session:
        original = await session.get(NebiusPlatformReservation, target["registration"]["environment_id"])
        before = (original.cpu_millis, original.memory_mib, original.storage_mib, original.ephemeral_storage_mib)
    url = factory.kw["bind"].url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)
    report = await asyncio.to_thread(observe, url, [target])
    assert report["status"] == "observed" and report["read_only"] is True
    row, = report["targets"]
    assert row["operation_id"] == target["operation_id"]
    assert {key for key, value in row["checks"].items() if value is False} == ({failed_check} if failed_check else set())
    assert "private-" not in json.dumps(report)
    async with factory() as session:
        operation = await session.get(NebiusEnvironmentOperation, target["operation_id"])
        assert operation.phase == "pending" and operation.runner_epoch == 0 and operation.lease_token is None
        after = await session.get(NebiusPlatformReservation, target["registration"]["environment_id"])
        assert (after.cpu_millis, after.memory_mib, after.storage_mib, after.ephemeral_storage_mib) == before
        rows = (await session.scalars(select(NebiusEnvironmentResource).where(
            NebiusEnvironmentResource.operation_id == target["operation_id"]))).all()
        assert rows and all(row.phase == "planned" for row in rows)


async def test_fixed_probe_source_executes_without_installed_probe_module(environment_registry, monkeypatch, capsys):
    from pathlib import Path

    import sqlalchemy
    from scripts.ops import nebius_retirement_registry_probe as probe
    from sqlalchemy.engine import URL, make_url

    _, factory, _, _ = environment_registry
    target = await prepare_retirement(environment_registry)
    namespace = "loom-nebius-management"
    protected_url = URL.create("postgresql", username="loom_service", password="private-probe-credential",
        host=f"loom-postgres.{namespace}.svc", port=5432, database="loom",
        query={"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"})
    fixture_url = factory.kw["bind"].url.set(drivername="postgresql+psycopg")
    original = sqlalchemy.create_engine

    def connect_to_fixture(url, **kwargs):
        # Only substitute the disposable DB address/auth; retain real driver,
        # transaction options, queries and all CLI validation/serialization.
        assert make_url(url) == protected_url.set(drivername="postgresql+psycopg")
        return original(fixture_url, **kwargs)

    monkeypatch.setattr(sqlalchemy, "create_engine", connect_to_fixture)
    monkeypatch.setenv("LOOM_SVC_DB_URL", protected_url.render_as_string(hide_password=False))
    monkeypatch.setattr("sys.argv", ["-c", namespace, json.dumps([target])])
    with pytest.raises(SystemExit) as stopped:
        exec(compile(Path(probe.__file__).read_text(), "<protected-probe>", "exec"), {"__name__": "__main__"})
    assert stopped.value.code == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["status"] == "observed" and report["read_only"] is True
    assert all(report["targets"][0]["checks"].values())
    assert "private-" not in captured.out + captured.err
    async with factory() as session:
        operation = await session.get(NebiusEnvironmentOperation, target["operation_id"])
        assert operation.phase == "pending" and operation.runner_epoch == 0


async def test_probe_refuses_a_connection_without_server_read_only_enforcement(environment_registry, monkeypatch):
    from scripts.ops import nebius_retirement_registry_probe as probe

    _, factory, _, _ = environment_registry
    target = await prepare_retirement(environment_registry)
    original = probe.create_engine

    def unenforced(url, **kwargs):
        kwargs["connect_args"]["options"] = "-c default_transaction_read_only=off"
        return original(url, **kwargs)

    monkeypatch.setattr(probe, "create_engine", unenforced)
    url = factory.kw["bind"].url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)
    with pytest.raises(ValueError, match="read_only_required"):
        await asyncio.to_thread(probe.observe, url, [target])
