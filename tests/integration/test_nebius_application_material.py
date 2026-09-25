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
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.schema import Secret
from loom.security.secret_store import LocalEncryptedSecretStore, parse_ref
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
    for action in (lambda: registry.ensure_material(lease, forbidden), lambda: registry.load_material(lease)):
        with pytest.raises(ManagementError, match="stale_operation_lease"):
            await action()


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


@pytest.mark.parametrize("action", ["update", "resume"])
async def test_successor_material_is_distinct_but_history_is_available(applications, platform_inputs, action):
    registry, factory, alice, plan, first, lease = await started(applications)
    old = await registry.ensure_material(lease, material)
    await _observed_complete(factory, first.operation_id)
    if action == "resume":
        stop = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
            action="suspend", expected_generation=1)
        await _observed_complete(factory, stop.operation_id)
    generation = 3 if action == "resume" else 2
    newer = _next_plan(plan, platform_inputs, new_release=action == "update",
                       deployment_generation=generation, access_generation=generation)
    changed = await registry.transition(first.application_id, principal=alice, idempotency_key="update",
        action=action, expected_generation=generation - 1,
        release_id=newer["release"].release_id if action == "update" else None, **newer)
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


@pytest.mark.parametrize("damage", ["ciphertext", "namespace", "identity", "identity-type", "shape", "json", "key"])
async def test_unreadable_material_is_bounded_and_never_regenerated(applications, monkeypatch, damage):
    registry, factory, _, _, operation, lease = await started(applications)
    await registry.ensure_material(lease, material)
    async with factory.begin() as session:
        secret = (await session.scalars(select(Secret))).one()
        if damage == "ciphertext":
            secret.ciphertext = b"deliberately-invalid-test-ciphertext"
        elif damage == "key":
            monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"k" * 32).decode())
        else:
            store = LocalEncryptedSecretStore(session)
            plaintext = await store.get(secret.ref)
            envelope = json.loads(plaintext)
            if damage == "identity":
                envelope["identity"]["data_environment_id"] = str(uuid4())
            elif damage == "identity-type":
                envelope["identity"]["access_generation"] = True
            elif damage == "shape":
                envelope["material"] = {"loom-admin": {"key": "test-only"}}
            new_ref = await store.put(
                namespace="wrong-namespace" if damage == "namespace" else parse_ref(secret.ref).namespace,
                value="not-json" if damage == "json" else json.dumps(envelope))
            await session.execute(text("UPDATE nebius_application_material SET secret_ref=:ref WHERE operation_id=:op"),
                                  {"ref": new_ref, "op": operation.operation_id})
    for attempt in (lambda: registry.load_material(lease),
                    lambda: registry.ensure_material(lease, lambda _: pytest.fail("corruption must not regenerate"))):
        with pytest.raises(ManagementError) as error:
            await attempt()
        assert error.value.code == "application_material_unavailable"
        assert str(error.value) == "application_material_unavailable"
        assert error.value.status_code == 503


async def test_reference_insert_failure_rolls_back_ciphertext_too(applications):
    registry, factory, _, _, _, lease = await started(applications)
    async with factory.begin() as session:
        await session.execute(text("""
            CREATE FUNCTION reject_test_material() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic material commit failure'; END $$
        """))
        await session.execute(text("""
            CREATE TRIGGER reject_test_material BEFORE INSERT ON nebius_application_material
            FOR EACH ROW EXECUTE FUNCTION reject_test_material()
        """))
    with pytest.raises(DBAPIError, match="synthetic material commit failure"):
        await registry.ensure_material(lease, material)
    async with factory.begin() as session:
        assert await session.scalar(select(func.count()).select_from(Secret)) == 0
        assert await session.scalar(text("SELECT count(*) FROM nebius_application_material")) == 0
        await session.execute(text("DROP TRIGGER reject_test_material ON nebius_application_material"))
        await session.execute(text("DROP FUNCTION reject_test_material()"))
    expected = material(await registry.frozen_plan(lease))
    assert await registry.ensure_material(lease, material) == expected


async def test_factory_failure_is_bounded_and_commits_nothing(applications):
    registry, factory, _, _, _, lease = await started(applications)

    def broken(_):
        raise RuntimeError("must-not-expose-sensitive-material")

    with pytest.raises(ManagementError) as error:
        await registry.ensure_material(lease, broken)
    assert str(error.value) == "application_material_generation_failed"
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Secret)) == 0
    assert await registry.ensure_material(lease, material) == material(await registry.frozen_plan(lease))
