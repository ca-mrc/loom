"""Real durable IAM intent; no cloud writes are performed by the journal."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, started
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_material import material
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def binding(plan, **changes):
    return dict(data_environment_id=str(plan["prepared"].registration.data_environment_id),
                project_id="dedicated-application-project", data_group_id="shared-data-group",
                source_group_id="shared-source-group") | changes


async def observe(registry, lease, plan, key, identity):
    effect = await registry.prepare_cloud_create(lease, key, binding(plan))
    assert await registry.dispatch_cloud_effect(lease, effect.key) is True
    await registry.observe_cloud_effect(lease, effect.operation_id, effect.key, resource_id=identity)
    return (await registry.cloud_history(lease))[-1]


async def test_one_cloud_prepare_and_dispatch_winner_freezes_scoped_intent(applications):
    registry, _, _, plan, operation, lease = await started(applications)
    effects = await asyncio.gather(*[
        registry.prepare_cloud_create(lease, "account", binding(plan)) for _ in range(3)
    ])
    assert effects == [effects[0]] * 3
    first = effects[0]
    assert first.operation_id == operation.operation_id and first.kind == "service_account"
    assert first.action == "create" and first.phase == "prepared"
    expected = first.expected
    assert expected["metadata"]["parent_id"] == "dedicated-application-project"
    assert expected["metadata"]["name"] == f"loom-app-{lease.incarnation.hex}-g1"
    assert expected["metadata"]["labels"]["loom-application-id"] == str(lease.application_id)
    assert expected["metadata"]["labels"]["loom-operation-id"] == str(operation.operation_id)
    assert expected["metadata"]["labels"]["loom-data-environment-id"] == binding(plan)["data_environment_id"]
    assert sorted(await asyncio.gather(*[
        registry.dispatch_cloud_effect(lease, "account") for _ in range(3)
    ])) == [False, False, True]
    with pytest.raises(ManagementError, match="application_cloud_binding_conflict"):
        await registry.prepare_cloud_create(lease, "account", binding(plan, project_id="different-project"))


async def test_membership_requires_recorded_key_and_committed_material(applications):
    registry, _, _, plan, _, lease = await started(applications)
    for key in ("key", "data", "source"):
        with pytest.raises(ManagementError, match="application_cloud_dependency_missing"):
            await registry.prepare_cloud_create(lease, key, binding(plan))
    await observe(registry, lease, plan, "account", "account-owned")
    key = await observe(registry, lease, plan, "key", "key-owned")
    assert key.expected["spec"]["account"] == {"service_account": {"id": "account-owned"}}
    assert key.expected["spec"]["secret_delivery_mode"] == "EXPLICIT"
    with pytest.raises(ManagementError, match="application_material_missing"):
        await registry.prepare_cloud_create(lease, "data", binding(plan))
    await registry.ensure_material(lease, material)
    data = await observe(registry, lease, plan, "data", "data-member-owned")
    source = await observe(registry, lease, plan, "source", "source-member-owned")
    assert data.kind == source.kind == "membership"
    assert data.expected["metadata"]["parent_id"] == "shared-data-group"
    assert source.expected["metadata"]["parent_id"] == "shared-source-group"
    assert data.expected["spec"] == source.expected["spec"] == {"member_id": "account-owned"}
    assert {effect.kind for effect in await registry.cloud_history(lease)} == {"service_account", "access_key", "membership"}


async def test_uncertain_dispatch_survives_takeover_without_new_authorization(applications):
    registry, factory, _, plan, operation, lease = await started(applications)
    await registry.prepare_cloud_create(lease, "account", binding(plan))
    assert await registry.dispatch_cloud_effect(lease, "account") is True
    await expire(factory, lease)
    current = await registry.claim(operation.operation_id)
    recovered = await registry.prepare_cloud_create(current, "account", binding(plan))
    assert recovered.phase == "dispatched" and recovered.dispatch_epoch == lease.runner_epoch
    assert await registry.dispatch_cloud_effect(current, "account") is False
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.observe_cloud_effect(lease, operation.operation_id, "account", resource_id="late-account")
    await registry.observe_cloud_effect(current, operation.operation_id, "account", resource_id="late-account")
    with pytest.raises(ManagementError, match="application_cloud_observation_conflict"):
        await registry.observe_cloud_effect(current, operation.operation_id, "account", resource_id="replacement")


async def test_stop_reconciles_and_retires_only_own_recorded_history(applications):
    registry, _, alice, plan, first, lease = await started(applications)
    await registry.prepare_cloud_create(lease, "account", binding(plan))
    await registry.dispatch_cloud_effect(lease, "account")
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ManagementError, match="application_cloud_forbidden"):
        await registry.dispatch_cloud_effect(current, "account", operation_id=first.operation_id)
    with pytest.raises(ManagementError, match="invalid_application_cloud_operation"):
        await registry.prepare_cloud_create(current, "account", binding(plan))
    with pytest.raises(ManagementError, match="application_cloud_dependency_missing"):
        await registry.prepare_cloud_delete(current, first.operation_id, "account")
    await registry.observe_cloud_effect(current, first.operation_id, "account", resource_id="late-owned-account")
    deletion = await registry.prepare_cloud_delete(current, first.operation_id, "account")
    assert deletion.action == "delete" and deletion.resource_id == "late-owned-account"
    assert deletion.expected == (await registry.cloud_history(current))[0].expected
    assert await registry.dispatch_cloud_effect(current, deletion.key) is True
    with pytest.raises(ManagementError, match="application_cloud_observation_conflict"):
        await registry.observe_cloud_effect(current, stopped.operation_id, deletion.key, resource_id="replacement")
    await registry.observe_cloud_effect(current, stopped.operation_id, deletion.key, resource_id="late-owned-account")
    with pytest.raises(ManagementError, match="application_cloud_forbidden"):
        await registry.prepare_cloud_delete(current, uuid4(), "account")


async def test_same_owner_sibling_cannot_observe_or_retire_cloud_history(applications):
    registry, _, alice, plan, first, lease = await started(applications)
    await observe(registry, lease, plan, "account", "first-account")
    _, _, _, prepare, _, _ = applications
    sibling = await registry.create(principal=alice, idempotency_key="sibling", **prepare("alice-next", alice))
    other = await registry.claim(sibling.operation_id)
    assert await registry.cloud_history(other) == []
    with pytest.raises(ManagementError, match="application_cloud_forbidden"):
        await registry.prepare_cloud_delete(other, first.operation_id, "account")
    with pytest.raises(ManagementError, match="application_cloud_forbidden"):
        await registry.observe_cloud_effect(other, first.operation_id, "account", resource_id="first-account")
    with pytest.raises(ManagementError, match="application_cloud_forbidden"):
        await registry.dispatch_cloud_effect(other, "account", operation_id=first.operation_id)


@pytest.mark.parametrize("key,change", [
    ("bucket", {}), ("group", {}), ("backup", {}), ("account", {"data_environment_id": str(uuid4())}),
    ("account", {"source_group_id": "shared-data-group"}), ("account", {"project_id": ""}),
    ("account", {"secret": "never-in-intent"}),
])
async def test_invalid_cloud_scope_never_records_intent(applications, key, change):
    registry, _, _, plan, _, lease = await started(applications)
    with pytest.raises(ManagementError, match="invalid_application_cloud"):
        await registry.prepare_cloud_create(lease, key, binding(plan, **change))
    assert await registry.cloud_history(lease) == []


async def test_observation_requires_dispatch_and_blocks_conflicting_retirement(applications):
    registry, _, _, plan, first, lease = await started(applications)
    await registry.prepare_cloud_create(lease, "account", binding(plan))
    with pytest.raises(ManagementError, match="application_cloud_not_dispatched"):
        await registry.observe_cloud_effect(lease, first.operation_id, "account", resource_id="never-sent")
    with pytest.raises(ManagementError, match="application_cloud_dependency_missing"):
        await registry.prepare_cloud_create(lease, "key", binding(plan))
