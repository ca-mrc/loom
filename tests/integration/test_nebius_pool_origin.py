"""Priority uses protected registration and retained app versions, not body rank."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text, update

from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_auth import credential
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_contract import participant


async def setup_origin(applications, *, environment_class="development", kind="application", cluster_id=None):
    from loom_service.pool_management.auth import resolve_pool_machine

    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    operation = await registry.create(principal=alice, idempotency_key="origin", **plan)
    app = plan["prepared"].registration
    binding = PoolParticipantV1.model_validate(participant(
        participant_id=uuid4(), pool_id=uuid4(), environment_id=app.data_environment_id,
        environment_class=environment_class))
    raw, _, _, _ = await credential(factory, participant_config=binding, cluster_id=cluster_id or app.cluster_id)
    async with factory() as session:
        principal = await resolve_pool_machine(session, "Bearer " + raw)
    origin = PoolWorkOriginV1.model_validate({
        "data_environment_id": app.data_environment_id, "submission_id": uuid4(),
        "kind": kind, "application": ({
            "application_id": app.application_id, "incarnation": app.incarnation,
            "deployment_generation": 1, "release_id": app.release_id,
            "source_digest": plan["release"].source_digest,
        } if kind == "application" else None),
    })
    return factory, principal, binding, origin, registry, alice, operation, plan


@pytest.mark.parametrize("environment_class,expected", [("production", 0), ("staging", 1), ("development", 2)])
async def test_environment_priority_comes_from_current_protected_participant(applications, environment_class, expected):
    from loom_service.pool_management.origin import qualify_pool_origin

    factory, principal, _, origin, *_ = await setup_origin(applications, environment_class=environment_class, kind="environment")
    async with factory() as session:
        assert await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="trial") == expected
        assert not session.new and not session.dirty and not session.deleted


async def test_personal_priority_keeps_original_version_after_suspend_without_mutation(applications):
    from loom_service.pool_management.origin import qualify_pool_origin

    factory, principal, _, origin, registry, alice, operation, _ = await setup_origin(applications)
    stopped = await registry.transition(operation.application_id, principal=alice, idempotency_key="suspend",
        action="suspend", expected_generation=1)
    before = await registry.get_operation(operation.operation_id, principal=alice)
    assert before.phase == "superseded"
    async with factory() as session:
        assert await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="task_image_build") == 3
    assert await registry.get_operation(operation.operation_id, principal=alice) == before
    assert (await registry.get_operation(stopped.operation_id, principal=alice)).phase == "pending"


async def test_cutover_can_qualify_retained_application_history_without_opening_the_pool(applications):
    from loom.db.nebius_pool_schema import NebiusPoolBinding
    from loom_service.pool_management.origin import qualify_retained_pool_origin

    factory, principal, binding, origin, registry, alice, operation, plan = await setup_origin(applications)
    await registry.transition(operation.application_id, principal=alice, idempotency_key='origin-retirement',
        action='suspend', expected_generation=1)
    async with factory.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == binding.pool_id).values(mode='closed'))
    async with factory() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        assert await qualify_retained_pool_origin(session, origin, participant=binding,
            cluster_id=plan['prepared'].registration.cluster_id, workload_kind='trial') == 3
        assert (await session.get(NebiusPoolBinding, binding.pool_id)).mode == 'closed'
        assert not session.new and not session.dirty and not session.deleted
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == 'superseded'


@pytest.mark.parametrize('damage', ['source', 'cluster', 'environment', 'generation'])
async def test_cutover_origin_history_denies_unregistered_source_and_wrong_binding(applications, damage):
    from loom_service.pool_management.origin import PoolOriginError, qualify_retained_pool_origin

    factory, _, binding, origin, _, _, _, plan = await setup_origin(applications)
    cluster = plan['prepared'].registration.cluster_id
    if damage == 'source':
        origin = origin.model_copy(update={'application': origin.application.model_copy(update={'source_digest': 'sha256:' + 'f' * 64})})
    elif damage == 'cluster':
        cluster = 'mk8scluster-foreign'
    elif damage == 'environment':
        binding = binding.model_copy(update={'environment_id': uuid4()})
    else:
        origin = origin.model_copy(update={'application': origin.application.model_copy(update={'deployment_generation': 50})})
    async with factory() as session:
        with pytest.raises(PoolOriginError):
            await qualify_retained_pool_origin(session, origin, participant=binding, cluster_id=cluster, workload_kind='trial')


@pytest.mark.parametrize("field,value", [
    ("application_id", uuid4()), ("incarnation", uuid4()), ("deployment_generation", 20),
    ("release_id", uuid4()), ("source_digest", "sha256:" + "f" * 64),
])
async def test_unregistered_or_changed_personal_source_cannot_get_a_priority(applications, field, value):
    from loom_service.pool_management.origin import PoolOriginError, qualify_pool_origin

    factory, principal, _, origin, *_ = await setup_origin(applications)
    origin = origin.model_copy(update={"application": origin.application.model_copy(update={field: value})})
    async with factory() as session:
        with pytest.raises(PoolOriginError):
            await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="trial")


@pytest.mark.parametrize("damage", ["target", "data", "class", "fenced", "closed", "binding_hash", "binding_identity", "build_unavailable"])
async def test_unqualified_scope_or_registration_fails_closed(applications, damage):
    from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
    from loom.pipeline.keys import canonical_digest
    from loom_service.pool_management.origin import qualify_pool_origin

    factory, principal, binding, origin, *_ = await setup_origin(
        applications, environment_class="production" if damage == "class" else "development")
    target, workload = "nebius-default", "trial"
    if damage == "target":
        target = "foreign"
    elif damage == "data":
        origin = origin.model_copy(update={"data_environment_id": uuid4()})
    elif damage == "build_unavailable":
        origin = origin.model_copy(update={"kind": "personal_build", "application": None})
        workload = "application_image_build"
    elif damage in {"fenced", "closed", "binding_hash", "binding_identity"}:
        async with factory.begin() as session:
            if damage == "closed":
                await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == binding.pool_id).values(mode="closed"))
                principal = replace(principal, pool_mode="closed")
            elif damage == "fenced":
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == binding.participant_id).values(phase="fenced"))
                principal = replace(principal, participant_phase="fenced")
            else:
                payload = binding.model_dump(mode="json") | {"binding_revision": 2}
                if damage == "binding_identity":
                    payload["environment_id"] = str(uuid4())
                digest = canonical_digest(payload).removeprefix("sha256:") if damage == "binding_identity" else "f" * 64
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == binding.participant_id).values(
                    binding_revision=2, binding_json=payload, binding_sha256=digest))
                principal = replace(principal, participant_revision=2, participant_binding_sha256=digest)
    async with factory() as session:
        with pytest.raises(ValueError):
            await qualify_pool_origin(session, principal, origin, target_id=target, workload_kind=workload)


async def test_suspension_intent_is_not_a_deployed_source_generation(applications):
    from loom_service.pool_management.origin import PoolOriginError, qualify_pool_origin

    factory, principal, _, origin, registry, alice, operation, _ = await setup_origin(applications)
    await registry.transition(operation.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    stopped = origin.model_copy(update={"application": origin.application.model_copy(update={"deployment_generation": 2})})
    async with factory() as session:
        with pytest.raises(PoolOriginError):
            await qualify_pool_origin(session, principal, stopped, target_id="nebius-default", workload_kind="trial")


async def test_origin_lookup_preserves_pending_caller_owned_application_changes(applications):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom_service.pool_management.origin import PoolOriginError, qualify_pool_origin

    factory, principal, _, origin, *_ = await setup_origin(applications)
    async with factory() as session:
        row = await session.get(NebiusApplication, origin.application.application_id)
        row.desired_state = "suspended"
        with pytest.raises(PoolOriginError):
            await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="trial")
        assert row in session.dirty and row.desired_state == "suspended"
        async with factory() as observer:
            assert (await observer.get(NebiusApplication, row.application_id)).desired_state == "active"


async def test_same_data_identity_does_not_authorize_a_different_physical_cluster(applications):
    from loom_service.pool_management.origin import PoolOriginError, qualify_pool_origin

    factory, principal, _, origin, *_ = await setup_origin(applications, cluster_id="foreign-cluster")
    async with factory() as session:
        with pytest.raises(PoolOriginError):
            await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="trial")


async def test_update_retains_old_source_instead_of_relabeling_queued_work(applications, platform_inputs):
    from loom_service.pool_management.origin import qualify_pool_origin
    from tests.integration.test_nebius_application_operations import _next_plan, _observed_complete

    factory, principal, _, origin, registry, alice, operation, plan = await setup_origin(applications)
    await _observed_complete(factory, operation.operation_id)
    next_plan = _next_plan(plan, platform_inputs)
    await registry.transition(operation.application_id, principal=alice, idempotency_key="update",
        action="update", expected_generation=1, release_id=next_plan["release"].release_id, **next_plan)
    updated = origin.model_copy(update={"application": origin.application.model_copy(update={
        "deployment_generation": 2, "release_id": next_plan["release"].release_id,
        "source_digest": next_plan["release"].source_digest,
    })})
    assert updated.application.release_id != origin.application.release_id
    assert updated.application.source_digest != origin.application.source_digest
    async with factory() as session:
        for recorded in (origin, updated):
            assert await qualify_pool_origin(session, principal, recorded,
                target_id="nebius-default", workload_kind="trial") == 3
