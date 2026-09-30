"""Priority uses protected registration and retained app versions, not body rank."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import update

from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.integration.test_nebius_pool_auth import credential
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_contract import participant


async def setup_origin(applications, *, environment_class="development", kind="application"):
    from loom_service.pool_management.auth import resolve_pool_machine

    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    operation = await registry.create(principal=alice, idempotency_key="origin", **plan)
    app = plan["prepared"].registration
    binding = PoolParticipantV1.model_validate(participant(
        participant_id=uuid4(), pool_id=uuid4(), environment_id=app.data_environment_id,
        environment_class=environment_class))
    raw, _, _, _ = await credential(factory, participant_config=binding, cluster_id=app.cluster_id)
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
    return factory, principal, binding, origin, registry, alice, operation


@pytest.mark.parametrize("environment_class,expected", [("production", 0), ("staging", 1), ("development", 2)])
async def test_environment_priority_comes_from_current_protected_participant(applications, environment_class, expected):
    from loom_service.pool_management.origin import qualify_pool_origin

    factory, principal, _, origin, *_ = await setup_origin(applications, environment_class=environment_class, kind="environment")
    async with factory() as session:
        assert await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="trial") == expected
        assert not session.new and not session.dirty and not session.deleted


async def test_personal_priority_keeps_original_version_after_suspend_without_mutation(applications):
    from loom_service.pool_management.origin import qualify_pool_origin

    factory, principal, _, origin, registry, alice, operation = await setup_origin(applications)
    stopped = await registry.transition(operation.application_id, principal=alice, idempotency_key="suspend",
        action="suspend", expected_generation=1)
    before = await registry.get_operation(operation.operation_id, principal=alice)
    assert before.phase == "superseded"
    async with factory() as session:
        assert await qualify_pool_origin(session, principal, origin, target_id="nebius-default", workload_kind="task_image_build") == 3
    assert await registry.get_operation(operation.operation_id, principal=alice) == before
    assert (await registry.get_operation(stopped.operation_id, principal=alice)).phase == "pending"


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


@pytest.mark.parametrize("damage", ["target", "data", "class", "fenced", "closed", "binding_hash", "build_unavailable"])
async def test_unqualified_scope_or_registration_fails_closed(applications, damage):
    from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
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
    elif damage in {"fenced", "closed", "binding_hash"}:
        async with factory.begin() as session:
            if damage == "closed":
                await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == binding.pool_id).values(mode="closed"))
                principal = replace(principal, pool_mode="closed")
            elif damage == "fenced":
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == binding.participant_id).values(phase="fenced"))
                principal = replace(principal, participant_phase="fenced")
            else:
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == binding.participant_id).values(
                    binding_revision=2, binding_sha256="f" * 64))
                principal = replace(principal, participant_revision=2, participant_binding_sha256="f" * 64)
    async with factory() as session:
        with pytest.raises(ValueError):
            await qualify_pool_origin(session, principal, origin, target_id=target, workload_kind=workload)
