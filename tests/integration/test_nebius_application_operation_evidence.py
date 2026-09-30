"""Owner-visible journal evidence is read-only, bounded, and not live readiness."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_cloud_effects import binding, observe
from tests.integration.test_nebius_application_effects import expire, intent, started
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def test_evidence_projects_real_journals_without_plan_identity_material_or_mutation(applications):
    from loom_service.application_management.operation_evidence import read_operation_evidence

    registry, factory, alice, plan, operation, lease = await started(applications)
    await registry.prepare_effect(lease, "api", intent(plan))
    await registry.dispatch_effect(lease, "api")
    await registry.observe_effect(lease, "api", uid="private-resource-uid", resource_version="2")
    await registry.prepare_effect(lease, "web", intent(plan, name="loom-web"))
    await observe(registry, lease, plan, "account", "private-account-id")
    await registry.prepare_cloud_create(lease, "key", binding(plan))
    await registry.dispatch_cloud_effect(lease, "key")
    before = await registry.get_operation(operation.operation_id, principal=alice)
    kube_before, cloud_before = await registry.effect_history(lease), await registry.cloud_history(lease)

    report = await read_operation_evidence(factory, operation.operation_id, principal=alice)

    assert report.operation == before
    assert report.runner_epoch == lease.runner_epoch and report.lease_active is True
    assert report.completion_recorded is False  # Observed effects are not completion.
    assert [row.model_dump() for row in report.kubernetes] == [
        {"kind": "Deployment", "action": "create", "phase": "observed", "count": 1},
        {"kind": "Deployment", "action": "create", "phase": "prepared", "count": 1},
    ]
    assert [row.model_dump() for row in report.cloud] == [
        {"kind": "access_key", "action": "create", "phase": "dispatched", "count": 1},
        {"kind": "service_account", "action": "create", "phase": "observed", "count": 1},
    ]
    assert set(report.model_dump()) == {
        "schema_version", "operation", "runner_epoch", "lease_active", "completion_recorded", "kubernetes", "cloud",
    }
    serialized = report.model_dump_json()
    assert all(value not in serialized for value in (
        "private-resource-uid", "private-account-id", str(lease.lease_token),
        "plan_json", "intent_json", "secret-key", "dedicated-application-project",
    ))
    assert await registry.get_operation(operation.operation_id, principal=alice) == before
    assert await registry.effect_history(lease) == kube_before
    assert await registry.cloud_history(lease) == cloud_before


async def test_evidence_requires_both_owner_and_team_and_hides_missing_identity(applications):
    from loom_service.application_management.operation_evidence import read_operation_evidence

    registry, factory, alice, _, operation, _ = await started(applications)
    bob = applications[2][1]
    for identity, principal in ((operation.operation_id, bob),
                                (operation.operation_id, replace(alice, team_id=uuid4())), (uuid4(), alice)):
        with pytest.raises(ManagementError, match="application_forbidden") as error:
            await read_operation_evidence(factory, identity, principal=principal)
        assert error.value.status_code == 403
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "running"


async def test_evidence_counts_only_selected_operation_not_earlier_generation(applications):
    from loom_service.application_management.operation_evidence import read_operation_evidence

    registry, factory, alice, plan, first, lease = await started(applications)
    await observe(registry, lease, plan, "account", "account-owned")
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    deletion = await registry.prepare_cloud_delete(current, first.operation_id, "account")
    await registry.dispatch_cloud_effect(current, deletion.key)
    await registry.observe_cloud_effect(current, stopped.operation_id, deletion.key, resource_id="account-owned")

    report = await read_operation_evidence(factory, stopped.operation_id, principal=alice)
    assert report.operation.action == "suspend" and report.operation.phase == "running"
    assert report.completion_recorded is False
    assert [row.model_dump() for row in report.cloud] == [
        {"kind": "service_account", "action": "delete", "phase": "observed", "count": 1},
    ]
    assert not report.kubernetes
    old = await read_operation_evidence(factory, first.operation_id, principal=alice)
    assert old.operation.phase == "superseded" and old.lease_active is False
    assert old.cloud[0].action == "create"


async def test_expired_lease_is_reported_without_claiming_or_releasing_it(applications):
    from loom_service.application_management.operation_evidence import read_operation_evidence

    _, factory, alice, _, operation, lease = await started(applications)
    await expire(factory, lease)
    report = await read_operation_evidence(factory, operation.operation_id, principal=alice)
    assert report.lease_active is False and report.operation.phase == "running"
    assert report.runner_epoch == lease.runner_epoch
    async with factory() as session:
        row = await session.scalar(select(NebiusApplicationOperation).where(
            NebiusApplicationOperation.operation_id == operation.operation_id))
        assert row.lease_token == lease.lease_token and row.runner_epoch == lease.runner_epoch


async def test_operation_and_effect_counts_share_one_read_snapshot(applications, monkeypatch):
    from loom_service.application_management import operation_evidence

    registry, factory, alice, plan, operation, lease = await started(applications)
    original = operation_evidence._counts
    changed = False

    async def concurrent_writer_after_operation_read(*args, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            await registry.prepare_cloud_create(lease, "account", binding(plan))
        return await original(*args, **kwargs)

    monkeypatch.setattr(operation_evidence, "_counts", concurrent_writer_after_operation_read)
    first = await operation_evidence.read_operation_evidence(factory, operation.operation_id, principal=alice)
    assert not first.cloud  # The concurrent commit is outside this snapshot.
    following = await operation_evidence.read_operation_evidence(factory, operation.operation_id, principal=alice)
    assert [row.model_dump() for row in following.cloud] == [
        {"kind": "service_account", "action": "create", "phase": "prepared", "count": 1},
    ]


async def test_unqualified_journal_labels_never_become_public_payload(applications):
    from loom.db.nebius_application_effect_schema import NebiusApplicationEffect
    from loom_service.application_management.operation_evidence import read_operation_evidence

    _, factory, alice, _, operation, _ = await started(applications)
    async with factory.begin() as session:
        session.add(NebiusApplicationEffect(operation_id=operation.operation_id, effect_key="malformed", sequence=1,
            intent_json={"kind": "private-malformed-label", "action": "create"}, phase="prepared"))
    with pytest.raises(ManagementError, match="application_evidence_unqualified") as error:
        await read_operation_evidence(factory, operation.operation_id, principal=alice)
    assert error.value.status_code == 503 and "private-malformed-label" not in str(error.value)
