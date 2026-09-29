"""Recovery cannot start from previously claimed state or release storage."""
from __future__ import annotations

from uuid import uuid4

import pytest
from psycopg.errors import ReadOnlySqlTransaction
from sqlalchemy import event

from loom.db.nebius_environment_schema import NebiusEnvironmentOperation, NebiusPlatformReservation
from loom_service.environment_management.retirement import RetirementSettings, RetirementTarget
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_environment_retirement import prepare_retirement
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def targets_and_settings(environment_registry):
    registry, factory, (alice, bob), prepare = environment_registry
    first = RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    second = RetirementTarget.model_validate(await prepare_retirement(
        (registry, factory, (bob, alice), lambda: prepare("bob", bob))))
    settings = RetirementSettings(schema_version="loom.nebius-retirement.v1", namespace="loom-nebius-management",
        kubernetes={"kind": "projected_service_account", "endpoint": "https://kubernetes.test",
            "ca_file": "/var/run/loom-retirement-kubernetes/ca.crt", "token_file": "/var/run/loom-retirement-kubernetes/token"},
        targets=(first, second))
    return settings


@pytest.mark.parametrize("change", ["claimed", "prior_attempt", "error", "effects", "completed", "foreign"])
async def test_one_ineligible_target_prevents_all_retirement(environment_registry, monkeypatch, change):
    from scripts.ops import nebius_retirement_recovery_runner as runner
    from scripts.ops import nebius_retirement_startup_probe as startup

    registry, factory, _, _ = environment_registry
    settings = await targets_and_settings(environment_registry)
    second = settings.targets[1]
    if change in {"claimed", "prior_attempt", "effects"}:
        lease = await registry.claim(second.operation_id)
        if change == "effects":
            step = await registry.next_step(lease)
            await registry.confirm_step(lease, step.key, provider_identity="fixture-resource")
        if change != "claimed":
            await registry.finish_attempt(lease, error_code="nebius_operation_failed", retry=True)
    elif change in {"error", "completed"}:
        async with factory.begin() as session:
            row = await session.get(NebiusEnvironmentOperation, second.operation_id)
            if change == "error":
                row.error_code = "nebius_operation_failed"
            else:
                row.phase = "completed"
    else:
        settings = settings.model_copy(update={"targets": (settings.targets[0], second.model_copy(update={"operation_id": uuid4()}))})
    url = factory.kw["bind"].url

    async def observe(selected, database_url):
        # Replace only unavailable host TLS/projected-Kubernetes transport;
        # operation qualification still reads the actual PostgreSQL rows.
        return {"schema": startup.SCHEMA, "status": "observed", "stage": "complete",
            "checks": ["database_binding", "kubernetes_ca", "kubernetes_token", "database", "kubernetes"],
            "operations": await startup.database_snapshot(url, selected.targets)}

    monkeypatch.setattr(runner._startup, "observe_startup", observe)
    monkeypatch.setattr(runner, "retirement_database_url", lambda value, namespace: url)
    result = await runner.run_recovery(settings, "fixture-database")
    assert result["status"] == "blocked" and result["retirement_started"] is False
    assert result["stage"] == ("startup" if change == "foreign" else "operation_state")
    async with factory() as session:
        first = await session.get(NebiusEnvironmentOperation, settings.targets[0].operation_id)
        reservation = await session.get(NebiusPlatformReservation, settings.targets[0].registration.environment_id)
        assert first.phase == "pending" and first.runner_epoch == 0 and first.lease_token is None
        assert reservation.cpu_millis > 0 and reservation.memory_mib > 0 and reservation.storage_mib > 0


async def test_reservation_snapshot_is_server_enforced_read_only(environment_registry, monkeypatch):
    from scripts.ops import nebius_retirement_recovery_runner as runner

    _, factory, _, _ = environment_registry
    settings = await targets_and_settings(environment_registry)
    original = runner.create_async_engine

    def inject_write(url, **kwargs):
        engine = original(url, **kwargs)

        def before(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("SHOW transaction_read_only"):
                cursor.execute("UPDATE nebius_platform_reservations SET storage_mib=0")

        event.listen(engine.sync_engine, "before_cursor_execute", before)
        return engine

    monkeypatch.setattr(runner, "create_async_engine", inject_write)
    with pytest.raises(ReadOnlySqlTransaction):
        await runner.reservation_snapshot(factory.kw["bind"].url, settings.targets)
    async with factory() as session:
        for target in settings.targets:
            assert (await session.get(NebiusPlatformReservation, target.registration.environment_id)).storage_mib > 0
