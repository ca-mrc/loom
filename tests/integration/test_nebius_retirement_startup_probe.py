"""PostgreSQL, not a mock, enforces the startup probe's read-only boundary."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import event, select, text
from sqlalchemy.exc import InternalError

from loom.db.nebius_environment_schema import (
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
    NebiusPlatformReservation,
)
from loom_service.environment_management.retirement import RetirementTarget
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_environment_retirement import prepare_retirement
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize("changed", [False, True])
async def test_startup_snapshot_reads_progress_without_claiming_or_releasing(environment_registry, monkeypatch, changed):
    from scripts.ops import nebius_retirement_startup_probe as probe

    registry, factory, _, _ = environment_registry
    target = RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    if changed:
        lease = await registry.claim(target.operation_id)
        step = await registry.next_step(lease)
        await registry.confirm_step(lease, step.key, provider_identity="private-provider-id")
    async with factory() as session:
        reservation = await session.get(NebiusPlatformReservation, target.registration.environment_id)
        before = (reservation.cpu_millis, reservation.memory_mib, reservation.storage_mib, reservation.ephemeral_storage_mib)
        resources = (await session.scalars(select(NebiusEnvironmentResource).where(
            NebiusEnvironmentResource.operation_id == target.operation_id))).all()
        expected_count = len(resources)
    original = probe.create_async_engine
    statements = []

    def instrumented(url, **kwargs):
        engine = original(url, **kwargs)
        event.listen(engine.sync_engine, "before_cursor_execute",
            lambda conn, cursor, statement, parameters, context, executemany: statements.append(statement))
        return engine

    monkeypatch.setattr(probe, "create_async_engine", instrumented)
    result = await probe.database_snapshot(factory.kw["bind"].url, (target,))
    assert result == [{"operation_id": str(target.operation_id), "phase": "running" if changed else "pending",
        "runner_epoch": 1 if changed else 0, "lease_present": changed, "error_present": False,
        "resource_count": expected_count, "effects_started": changed}]
    assert statements and all(statement.lstrip().upper().startswith(("SELECT", "SHOW")) for statement in statements)
    assert not any("FOR UPDATE" in statement.upper() for statement in statements)
    async with factory() as session:
        operation = await session.get(NebiusEnvironmentOperation, target.operation_id)
        assert operation.runner_epoch == (1 if changed else 0)
        after = await session.get(NebiusPlatformReservation, target.registration.environment_id)
        assert (after.cpu_millis, after.memory_mib, after.storage_mib, after.ephemeral_storage_mib) == before


async def test_probe_connection_cannot_write_to_operation(environment_registry, monkeypatch):
    from scripts.ops import nebius_retirement_startup_probe as probe

    _, factory, _, _ = environment_registry
    target = RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    original = probe.create_async_engine
    attempts = []

    def inject_write(url, **kwargs):
        engine = original(url, **kwargs)

        def before(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("SHOW transaction_read_only"):
                attempts.append(True)
                cursor.execute("UPDATE nebius_environment_operations SET runner_epoch=runner_epoch+1")

        event.listen(engine.sync_engine, "before_cursor_execute", before)
        return engine

    monkeypatch.setattr(probe, "create_async_engine", inject_write)
    with pytest.raises(InternalError, match="read-only transaction"):
        await probe.database_snapshot(factory.kw["bind"].url, (target,))
    assert attempts == [True]
    async with factory() as session:
        assert (await session.get(NebiusEnvironmentOperation, target.operation_id)).runner_epoch == 0


async def test_missing_or_foreign_target_fails_without_claiming(environment_registry):
    from scripts.ops import nebius_retirement_startup_probe as probe

    _, factory, _, _ = environment_registry
    target = RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    for bad in (target.model_copy(update={"operation_id": uuid4()}), target.model_copy(update={"source_operation_id": uuid4()})):
        with pytest.raises(ValueError):
            await probe.database_snapshot(factory.kw["bind"].url, (bad,))
    async with factory() as session:
        assert (await session.get(NebiusEnvironmentOperation, target.operation_id)).runner_epoch == 0
        assert await session.scalar(text("SHOW transaction_read_only")) == "off"
