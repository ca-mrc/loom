"""Crash-stable generation material uses real PostgreSQL and authenticated encryption."""
from __future__ import annotations

import asyncio
import base64
import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.schema import Secret
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, started
from tests.integration.test_nebius_application_operations import _next_plan, _observed_complete
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture(autouse=True)
def management_key(monkeypatch):
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())


def material(plan):
    row = plan["registration"]
    suffix = f"{row['incarnation'].replace('-', '')}-g{row['access_generation']}"
    return {
        f"loom-application-db-{suffix}": {"DATABASE_URL": "test-only-managed-password", "ca.crt": "test-only-ca"},
        f"loom-application-storage-{suffix}": {"key": "test-only-object-key"},
        f"loom-application-auth-{suffix}": {"keyring": "test-only-shared-keyring"},
    }


async def test_concurrent_generation_is_once_encrypted_recoverable_and_not_public(applications):
    registry, factory, alice, _, operation, lease = await started(applications)
    original = await registry.frozen_plan(lease)
    expected = material(original)
    calls = []

    def generate(plan):
        calls.append(copy.deepcopy(plan))
        value = material(plan)
        plan["registration"]["slug"] = "must-not-change-frozen-plan"
        return value

    values = await asyncio.gather(*[registry.ensure_material(lease, generate) for _ in range(3)])
    assert values == [expected] * 3 and calls == [original]
    assert await registry.load_material(lease) == expected
    assert await registry.frozen_plan(lease) == original
    async with factory() as session:
        secret = (await session.scalars(select(Secret))).one()
        ref = await session.scalar(text("SELECT secret_ref FROM nebius_application_material"))
        assert ref == secret.ref
        assert b"test-only-managed-password" not in secret.ciphertext
        public = (await registry.get_operation(operation.operation_id, principal=alice)).model_dump_json()
        assert ref not in public and "test-only-managed-password" not in public
    # Caller mutation must not change durable replay or the other callers' values.
    values[0].clear()
    assert values[1] == expected and await registry.load_material(lease) == expected


async def test_takeover_recovers_original_and_stale_lease_cannot_read_or_generate(applications):
    registry, factory, _, _, operation, lease = await started(applications)
    first = await registry.ensure_material(lease, material)
    await expire(factory, lease)
    current = await registry.claim(operation.operation_id)
    assert current.runner_epoch > lease.runner_epoch

    def forbidden(_):
        pytest.fail("a retry must not regenerate credentials")

    assert await registry.ensure_material(current, forbidden) == first
    for action in (registry.ensure_material(lease, forbidden), registry.load_material(lease)):
        with pytest.raises(ManagementError, match="stale_operation_lease"):
            await action


@pytest.mark.parametrize("action", ["suspend", "destroy_retained"])
async def test_stop_reads_own_predecessor_only_and_cannot_generate(applications, action):
    registry, _, alice, _, first, lease = await started(applications)
    expected = await registry.ensure_material(lease, material)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action=action, expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    assert await registry.load_material(current, operation_id=first.operation_id) == expected
    with pytest.raises(ManagementError, match="application_material_missing"):
        await registry.load_material(current)
    with pytest.raises(ManagementError, match="invalid_application_material_operation"):
        await registry.ensure_material(current, lambda _: pytest.fail("stop must not generate"))
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.load_material(lease)


async def test_update_material_is_distinct_but_history_is_available(applications, platform_inputs):
    registry, factory, alice, plan, first, lease = await started(applications)
    old = await registry.ensure_material(lease, material)
    await _observed_complete(factory, first.operation_id)
    newer = _next_plan(plan, platform_inputs)
    changed = await registry.transition(first.application_id, principal=alice, idempotency_key="update",
        action="update", expected_generation=1, release_id=newer["release"].release_id, **newer)
    current = await registry.claim(changed.operation_id)
    new = await registry.ensure_material(current, material)
    assert set(old).isdisjoint(new)
    assert await registry.load_material(current, operation_id=first.operation_id) == old
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Secret)) == 2


async def test_sibling_unknown_and_future_material_are_denied(applications):
    registry, factory, _, _, _, lease = await started(applications)
    _, _, (_, bob), prepare, _, _ = applications
    sibling = await registry.create(principal=bob, idempotency_key="bob", **prepare("bob", bob))
    other = await registry.claim(sibling.operation_id)
    await registry.ensure_material(other, material)
    for operation_id in (sibling.operation_id, uuid4()):
        with pytest.raises(ManagementError, match="application_material_forbidden"):
            await registry.load_material(lease, operation_id=operation_id)
    # A future plan in history is not readable by a current older lease.
    async with factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, sibling.operation_id)
        row.application_id, row.deployment_generation = lease.application_id, 99
    with pytest.raises(ManagementError, match="application_material_forbidden"):
        await registry.load_material(lease, operation_id=sibling.operation_id)


@pytest.mark.parametrize("change", ["empty", "extra", "missing", "fixed", "empty-value", "nonstr", "bad-key", "large"])
async def test_invalid_material_never_persists(applications, change):
    registry, factory, _, _, _, lease = await started(applications)

    def invalid(plan):
        value = material(plan)
        first = next(iter(value))
        if change == "empty":
            return {}
        if change == "extra":
            value["loom-admin"] = {"password": "must-not-store"}
        elif change == "missing":
            value.pop(first)
        elif change == "fixed":
            value["loom-application-db"] = value.pop(first)
        else:
            value[first] = {"key": ""} if change == "empty-value" else {"key": 123} if change == "nonstr" else (
                {"bad/key": "value"} if change == "bad-key" else {"key": "x" * 1_048_577})
        return value

    with pytest.raises(ManagementError, match="invalid_application_material"):
        await registry.ensure_material(lease, invalid)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Secret)) == 0
        assert await session.scalar(text("SELECT count(*) FROM nebius_application_material")) == 0


async def test_old_fixed_name_plan_cannot_generate_new_credentials(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    plan = prepare()
    suffix = f"-{plan['prepared'].registration.incarnation.hex}-g1"
    files = json.loads(json.dumps(plan["prepared"].files).replace(suffix, ""))
    plan["prepared"] = replace(plan["prepared"], files=files)
    first = await registry.create(principal=alice, idempotency_key="historical", **plan)
    lease = await registry.claim(first.operation_id)
    with pytest.raises(ManagementError, match="invalid_application_material_operation"):
        await registry.ensure_material(lease, lambda _: pytest.fail("historical activation is forbidden"))
