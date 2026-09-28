"""Durable startup phase exclusion using real application/effect journals."""
from __future__ import annotations

import asyncio
import hashlib

import pytest

from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, intent, started
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def unfence(plan, *, uid="fence-uid", rv="9"):
    key = "activate:unfence:" + hashlib.sha256((uid + ":" + rv).encode()).hexdigest()
    return key, intent(plan, api_version="v1", kind="ResourceQuota", name="loom-application-retired",
                       action="delete", uid=uid, resource_version=rv)


@pytest.mark.parametrize("phase", ["prepared", "dispatched", "rejected", "observed"])
async def test_activation_boundary_survives_every_request_phase_and_lease_takeover(applications, phase):
    registry, factory, _, plan, operation, lease = await started(applications)
    assert not await registry.activation_started(lease)
    key, value = unfence(plan)
    await registry.prepare_effect(lease, key, value)
    if phase != "prepared":
        assert await registry.dispatch_effect(lease, key)
    if phase == "rejected":
        await registry.reject_effect(lease, key, status_code=409)
    elif phase == "observed":
        await registry.observe_effect(lease, key, uid="fence-uid", resource_version=None)
    assert await registry.activation_started(lease)
    for retirement_key, retirement in (
        ("retire:scale:old", intent(plan, action="patch", uid="old-api", resource_version="7")),
        ("pod-fence:create", intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired")),
        ("pod-fence:advance:old", intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired", action="patch", uid="fence-uid", resource_version="8")),
    ):
        with pytest.raises(ManagementError, match="application_activation_started"):
            await registry.prepare_effect(lease, retirement_key, retirement)
    assert len(await registry.effect_history(lease)) == 1
    await expire(factory, lease)
    replacement = await registry.claim(operation.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.activation_started(lease)
    assert await registry.activation_started(replacement)


@pytest.mark.parametrize("change", ["target", "action", "kind", "key", "unqualified_delete"])
async def test_activation_marker_cannot_name_another_mutation(applications, change):
    registry, _, _, plan, _, lease = await started(applications)
    key, value = unfence(plan)
    if change == "target":
        value["name"] = "other-quota"
    elif change == "action":
        value["action"] = "patch"
    elif change == "kind":
        value.update(kind="Service", name="loom-service")
    elif change == "key":
        key = "activate:unfence:" + "0" * 64
    else:
        key = "arbitrary-delete"
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(lease, key, value)
    assert await registry.effect_history(lease) == []


async def test_definitive_rejection_allows_fresh_unfence_but_not_return_to_retirement(applications):
    registry, _, _, plan, _, lease = await started(applications)
    first, first_value = unfence(plan)
    await registry.prepare_effect(lease, first, first_value)
    await registry.dispatch_effect(lease, first)
    second, second_value = unfence(plan, rv="10")
    with pytest.raises(ManagementError):
        await registry.prepare_effect(lease, second, second_value)
    await registry.reject_effect(lease, first, status_code=409)
    assert (await registry.prepare_effect(lease, second, second_value)).phase == "prepared"
    await registry.dispatch_effect(lease, second)
    await registry.observe_effect(lease, second, uid="fence-uid", resource_version=None)
    third, third_value = unfence(plan, rv="11")
    with pytest.raises(ManagementError, match="application_activation_started"):
        await registry.prepare_effect(lease, third, third_value)
    assert (await registry.prepare_effect(lease, second, second_value)).phase == "observed"
    assert not await registry.dispatch_effect(lease, second)
    assert (await registry.prepare_effect(lease, "start:api", intent(plan))).phase == "prepared"


async def test_stop_successor_can_close_admission_without_inheriting_active_phase(applications):
    registry, _, alice, plan, operation, lease = await started(applications)
    key, value = unfence(plan)
    await registry.prepare_effect(lease, key, value)
    await registry.dispatch_effect(lease, key)
    stopped = await registry.transition(operation.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    successor = await registry.claim(stopped.operation_id)
    assert not await registry.activation_started(successor)
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(successor, key, value)
    closed = intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired")
    assert (await registry.prepare_effect(successor, "pod-fence:create", closed)).phase == "prepared"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.dispatch_effect(lease, key)


async def test_same_lease_preparation_race_has_one_durable_phase_winner(applications):
    registry, _, _, plan, _, lease = await started(applications)
    key, value = unfence(plan)
    retirement = intent(plan, action="patch", uid="old-api", resource_version="8")
    results = await asyncio.gather(registry.prepare_effect(lease, key, value),
        registry.prepare_effect(lease, "retire:scale:old", retirement), return_exceptions=True)
    assert sum(isinstance(result, ManagementError) for result in results) == 1
    effects = await registry.effect_history(lease)
    assert len(effects) == 1
    winner = effects[0]
    assert await registry.dispatch_effect(lease, winner.key)
    if winner.key == key:
        await registry.observe_effect(lease, key, uid="fence-uid", resource_version=None)
        with pytest.raises(ManagementError, match="application_activation_started"):
            await registry.prepare_effect(lease, "retire:scale:old", retirement)
    else:
        with pytest.raises(ManagementError, match="application_effect_unresolved"):
            await registry.prepare_effect(lease, key, value)
        await registry.observe_effect(lease, winner.key, uid="old-api", resource_version="9")
        assert (await registry.prepare_effect(lease, key, value)).phase == "prepared"


async def test_one_application_activation_never_blocks_another_owner(applications):
    registry, _, _, plan, _, lease = await started(applications)
    await registry.prepare_effect(lease, *unfence(plan))
    _, _, (_, bob), prepare, _, _ = applications
    other = prepare("bob", principal=bob)
    operation = await registry.create(principal=bob, idempotency_key="bob", **other)
    sibling = await registry.claim(operation.operation_id)
    assert not await registry.activation_started(sibling)
    assert (await registry.prepare_effect(sibling, "retire:scale:old",
        intent(other, action="patch", uid="bobs-api", resource_version="8"))).phase == "prepared"
