"""Real PostgreSQL journal with a controlled cloud network boundary."""
from __future__ import annotations

import asyncio
import copy

import pytest

from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderRetryError,
    ProviderWaitingError,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_cloud_effects import binding
from tests.integration.test_nebius_application_effects import expire, started
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_material import material
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class Cloud:
    def __init__(self):
        self.resources = {}
        self.mutations = []
        self.pending = None
        self.delay_create = self.delay_delete = False

    async def find(self, kind, expected):
        for actual_kind, value in self.resources.values():
            if actual_kind != kind or value["metadata"]["parent_id"] != expected["metadata"]["parent_id"]:
                continue
            if ((kind == "membership" and value["spec"]["member_id"] == expected["spec"]["member_id"])
                    or (kind != "membership" and value["metadata"]["name"] == expected["metadata"]["name"])):
                return copy.deepcopy(value)
        return None

    async def create(self, kind, expected, *, idempotency_key):
        self.mutations.append(("create", kind, idempotency_key))
        value = copy.deepcopy(expected)
        value["metadata"]["id"] = f"resource-{len(self.mutations)}"
        if self.delay_create:
            self.pending = kind, value
            raise ProviderRetryError("cloud_response_lost")
        self.resources[value["metadata"]["id"]] = kind, value
        return copy.deepcopy(value)

    async def get_resource(self, kind, identity):
        found = self.resources.get(identity)
        assert found is None or found[0] == kind
        return copy.deepcopy(found[1]) if found else None

    async def delete_resource(self, kind, identity, *, idempotency_key):
        self.mutations.append(("delete", kind, identity, idempotency_key))
        if self.delay_delete:
            raise ProviderRetryError("cloud_delete_lost")
        self.resources.pop(identity, None)

    async def access_key_secret(self, identity):
        assert self.resources[identity][0] == "access_key"
        return {"access-key": "test-access-key", "secret-key": "test-private-key"}


async def provider(applications):
    from loom_service.application_management.cloud_provider import ApplicationCloudProvider

    registry, factory, alice, plan, operation, lease = await started(applications)
    cloud = Cloud()
    return ApplicationCloudProvider(registry, cloud), cloud, registry, factory, alice, plan, operation, lease


async def test_cloud_provider_grants_only_shared_group_memberships_and_keeps_secrets_private(applications):
    worker, cloud, registry, _, alice, plan, operation, lease = await provider(applications)
    account = await worker.create(lease, "account", binding(plan))
    await worker.create(lease, "key", binding(plan))
    credentials = await worker.key_material(lease)
    assert credentials == {"access-key": "test-access-key", "secret-key": "test-private-key"}

    def generated(frozen):
        bundles = material(frozen)
        name = next(name for name in bundles if name.startswith("loom-application-storage-"))
        bundles[name] = credentials
        return bundles

    await registry.ensure_material(lease, generated)
    await worker.create(lease, "data", binding(plan))
    await worker.create(lease, "source", binding(plan))
    assert await worker.create(lease, "account", binding(plan)) == account
    assert [call[:2] for call in cloud.mutations] == [
        ("create", "service_account"), ("create", "access_key"), ("create", "membership"), ("create", "membership")]
    assert len({call[2] for call in cloud.mutations}) == 4
    assert "test-private-key" not in repr(await registry.cloud_history(lease))
    assert "test-private-key" not in (await registry.get_operation(operation.operation_id, principal=alice)).model_dump_json()


async def test_concurrent_provider_calls_send_only_once(applications):
    worker, cloud, _, _, _, plan, _, lease = await provider(applications)
    values = await asyncio.gather(*[worker.create(lease, "account", binding(plan)) for _ in range(3)],
                                  return_exceptions=True)
    assert all(not isinstance(value, Exception) or isinstance(value, ProviderWaitingError) for value in values)
    assert any(not isinstance(value, Exception) for value in values)
    assert len(cloud.mutations) == 1
    assert (await worker.create(lease, "account", binding(plan))).observed_resource_id == "resource-1"


async def test_unknown_create_never_resends_and_late_success_is_reconciled_after_takeover(applications):
    worker, cloud, registry, factory, _, plan, operation, lease = await provider(applications)
    cloud.delay_create = True
    with pytest.raises(ProviderRetryError):
        await worker.create(lease, "account", binding(plan))
    await expire(factory, lease)
    current = await registry.claim(operation.operation_id)
    with pytest.raises(ProviderWaitingError, match="application_cloud_unconfirmed"):
        await worker.create(current, "account", binding(plan))
    assert len(cloud.mutations) == 1
    kind, value = cloud.pending
    cloud.resources[value["metadata"]["id"]] = kind, value
    assert (await worker.create(current, "account", binding(plan))).observed_resource_id == "resource-1"
    assert len(cloud.mutations) == 1


@pytest.mark.parametrize("damage", ["missing", "labels", "parent", "id"])
async def test_observed_resource_drift_blocks_replay_and_never_recreates(applications, damage):
    worker, cloud, _, _, _, plan, _, lease = await provider(applications)
    await worker.create(lease, "account", binding(plan))
    if damage == "missing":
        cloud.resources.clear()
    else:
        row = cloud.resources["resource-1"][1]
        if damage == "labels":
            row["metadata"]["labels"]["loom-application-id"] = "foreign"
        else:
            row["metadata"]["parent_id" if damage == "parent" else "id"] = "replacement"
    with pytest.raises(ProviderBlockedError):
        await worker.create(lease, "account", binding(plan))
    assert len(cloud.mutations) == 1


async def test_late_old_create_can_be_reconciled_under_stop_without_resending(applications):
    worker, cloud, registry, _, alice, plan, first, lease = await provider(applications)
    cloud.delay_create = True
    with pytest.raises(ProviderRetryError):
        await worker.create(lease, "account", binding(plan))
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ProviderWaitingError):
        await worker.reconcile(current, first.operation_id, "account")
    kind, value = cloud.pending
    cloud.resources[value["metadata"]["id"]] = kind, value
    assert (await worker.reconcile(current, first.operation_id, "account")).observed_resource_id == "resource-1"
    assert len(cloud.mutations) == 1


async def test_uncertain_delete_only_reconciles_exact_retained_identity(applications):
    worker, cloud, registry, _, alice, plan, first, lease = await provider(applications)
    await worker.create(lease, "account", binding(plan))
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    cloud.delay_delete = True
    with pytest.raises(ProviderRetryError):
        await worker.delete(current, first.operation_id, "account")
    with pytest.raises(ProviderWaitingError):
        await worker.delete(current, first.operation_id, "account")
    assert len(cloud.mutations) == 2
    cloud.resources.pop("resource-1")
    deleted = await worker.delete(current, first.operation_id, "account")
    assert deleted.phase == "observed" and deleted.observed_resource_id == "resource-1"
    assert len(cloud.mutations) == 2


@pytest.mark.parametrize("first_phase", ["prepared", "dispatched", "observed"])
async def test_superseding_stop_keeps_one_retirement_intent(applications, first_phase):
    worker, cloud, registry, _, alice, plan, first, lease = await provider(applications)
    await worker.create(lease, "account", binding(plan))
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="suspend",
        action="suspend", expected_generation=1)
    stopping = await registry.claim(stopped.operation_id)
    deletion = await registry.prepare_cloud_delete(stopping, first.operation_id, "account")
    if first_phase == "dispatched":
        cloud.delay_delete = True
        with pytest.raises(ProviderRetryError):
            await worker.delete(stopping, first.operation_id, "account")
    elif first_phase == "observed":
        await worker.delete(stopping, first.operation_id, "account")
    destroyed = await registry.transition(first.application_id, principal=alice, idempotency_key="destroy",
        action="destroy_retained", expected_generation=2)
    current = await registry.claim(destroyed.operation_id)
    retained = await registry.prepare_cloud_delete(current, first.operation_id, "account")
    assert (retained.operation_id, retained.key) == (deletion.operation_id, deletion.key)
    if first_phase == "dispatched":
        with pytest.raises(ProviderWaitingError):
            await worker.delete(current, first.operation_id, "account")
        assert len(cloud.mutations) == 2
        cloud.resources.pop("resource-1")  # Original delayed DELETE finally arrives.
    result = await worker.delete(current, first.operation_id, "account")
    assert result.phase == "observed"
    assert result.operation_id == stopped.operation_id
    assert len(cloud.mutations) == 2
    history = await registry.cloud_history(current)
    assert len([effect for effect in history if effect.action == "delete"]) == 1


async def test_matching_but_unrecorded_resource_is_not_adopted(applications):
    worker, cloud, registry, _, _, plan, _, lease = await provider(applications)
    planned = await registry.prepare_cloud_create(lease, "account", binding(plan))
    unrecorded = copy.deepcopy(planned.expected)
    unrecorded["metadata"]["id"] = "not-dispatched-by-this-journal"
    cloud.resources[unrecorded["metadata"]["id"]] = "service_account", unrecorded
    with pytest.raises(ProviderBlockedError, match="application_cloud_unrecorded_resource"):
        await worker.create(lease, "account", binding(plan))
    assert cloud.mutations == []
    assert (await registry.cloud_history(lease))[0].phase == "prepared"


async def test_stale_dispatch_response_keeps_uncertainty_for_successor(applications):
    worker, cloud, registry, factory, _, plan, first, lease = await provider(applications)
    create = cloud.create

    async def expire_before_reply(*args, **kwargs):
        value = await create(*args, **kwargs)
        await expire(factory, lease)
        return value

    cloud.create = expire_before_reply
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await worker.create(lease, "account", binding(plan))
    current = await registry.claim(first.operation_id)
    assert (await registry.cloud_history(current))[0].phase == "dispatched"
    assert (await worker.reconcile(current, first.operation_id, "account")).observed_resource_id == "resource-1"
    assert len(cloud.mutations) == 1


@pytest.mark.parametrize("damage", ["labels", "id", "stale-lease"])
async def test_delete_checks_ownership_and_current_lease_before_dispatch(applications, damage):
    worker, cloud, registry, factory, alice, plan, first, lease = await provider(applications)
    await worker.create(lease, "account", binding(plan))
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    read = cloud.get_resource

    async def corrupted_read(*args):
        value = await read(*args)
        if damage == "stale-lease":
            await expire(factory, current)
        elif damage == "labels":
            value["metadata"]["labels"]["loom-incarnation"] = "not-ours"
        else:
            value["metadata"]["id"] = "replacement"
        return value

    cloud.get_resource = corrupted_read
    with pytest.raises(ManagementError if damage == "stale-lease" else ProviderBlockedError):
        await worker.delete(current, first.operation_id, "account")
    assert len(cloud.mutations) == 1
    assert "resource-1" in cloud.resources


@pytest.mark.parametrize("value", [{}, {"access-key": "one"}, {"access-key": "one", "secret-key": ""},
                                  {"access-key": "one", "secret-key": "x" * 4097}])
async def test_invalid_cloud_material_cannot_escape_provider(applications, value):
    worker, cloud, _, _, _, plan, _, lease = await provider(applications)
    await worker.create(lease, "account", binding(plan))
    await worker.create(lease, "key", binding(plan))

    async def invalid(_):
        return value

    cloud.access_key_secret = invalid
    with pytest.raises(ProviderBlockedError, match="application_cloud_material_invalid"):
        await worker.key_material(lease)
