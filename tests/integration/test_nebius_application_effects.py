"""Durable application write intent is distinct from provider/runtime fencing."""
from __future__ import annotations

import asyncio
import copy
from dataclasses import replace

import pytest
from sqlalchemy import text

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_operations import _next_plan, _observed_complete
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs


def intent(plan, **changes):
    return dict(api_version="apps/v1", kind="Deployment", namespace=plan["prepared"].registration.application_namespace,
                name="loom-service", action="create", request_sha256="a" * 64) | changes


async def started(applications):
    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    operation = await registry.create(principal=alice, idempotency_key="create", **plan)
    return registry, factory, alice, plan, operation, await registry.claim(operation.operation_id)


async def expire(factory, lease):
    async with factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, lease.operation_id)
        row.lease_expires_at = await session.scalar(text("SELECT clock_timestamp() - interval '1 second'"))


async def test_prepare_is_atomic_immutable_and_never_sends_a_write(applications):
    registry, _, _, plan, operation, lease = await started(applications)
    values = await asyncio.gather(*[
        registry.prepare_effect(lease, "api-create", intent(plan)) for _ in range(3)
    ])
    assert all(value == values[0] for value in values)
    first = values[0]
    assert first.phase == "prepared" and first.dispatch_epoch is None
    assert first.sequence == 1 and first.operation_id == operation.operation_id
    with pytest.raises(ManagementError, match="application_effect_conflict"):
        await registry.prepare_effect(lease, "api-create", intent(plan, request_sha256="b" * 64))
    with pytest.raises(ManagementError, match="application_effect_unresolved"):
        await registry.prepare_effect(lease, "web-create", intent(plan, name="loom-web"))
    assert len(await registry.effect_history(lease)) == 1


@pytest.mark.parametrize("change", [
    {"namespace": "loom-shared"}, {"name": "foreign"}, {"kind": "Job", "api_version": "batch/v1"},
    {"kind": "StatefulSet"}, {"api_version": "v1"}, {"action": "delete"},
    {"action": "patch", "uid": "exact-uid"}, {"action": "create", "uid": "exact-uid", "resource_version": "4"},
    {"request_sha256": "not-a-digest"}, {"body": {"password": "must-not-be-stored"}},
    {"kind": "Secret", "api_version": "v1", "name": "management-key"},
    {"kind": "ResourceQuota", "api_version": "v1", "name": "arbitrary"},
    {"kind": "Pod", "api_version": "v1", "name": "api-pod"},
])
async def test_invalid_or_foreign_write_intent_is_rejected_without_journal_row(applications, change):
    registry, _, _, plan, _, lease = await started(applications)
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(lease, "invalid", intent(plan, **change))
    assert await registry.effect_history(lease) == []


@pytest.mark.parametrize("kind,api,name", [
    ("Namespace", "v1", None), ("RoleBinding", "rbac.authorization.k8s.io/v1", "authority"),
])
async def test_bootstrap_identity_cannot_be_deleted_or_patched(applications, kind, api, name):
    registry, _, _, plan, _, lease = await started(applications)
    for action in ("patch", "delete"):
        with pytest.raises(ManagementError, match="invalid_application_effect"):
            await registry.prepare_effect(lease, "identity", intent(plan, kind=kind, api_version=api,
                namespace=None if kind == "Namespace" else plan["prepared"].registration.application_namespace,
                name=name or plan["prepared"].registration.application_namespace,
                action=action, uid="owned-uid", resource_version="4"))


async def test_dispatch_has_one_winner_and_expiry_does_not_authorize_resend(applications):
    registry, factory, _, plan, operation, lease = await started(applications)
    await registry.prepare_effect(lease, "api", intent(plan))
    winners = await asyncio.gather(*[registry.dispatch_effect(lease, "api") for _ in range(3)])
    assert winners.count(True) == 1 and winners.count(False) == 2
    await expire(factory, lease)
    newer = await registry.claim(operation.operation_id)
    assert await registry.dispatch_effect(newer, "api") is False
    effect = (await registry.effect_history(newer))[0]
    assert effect.phase == "dispatched" and effect.dispatch_epoch == lease.runner_epoch
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.observe_effect(lease, "api", uid="created-uid", resource_version="9")
    await registry.observe_effect(newer, "api", uid="created-uid", resource_version="9")
    assert (await registry.effect_history(newer))[0].phase == "observed"


async def test_prepared_effect_can_be_dispatched_after_takeover_but_not_observed_before_dispatch(applications):
    registry, factory, _, plan, operation, lease = await started(applications)
    await registry.prepare_effect(lease, "api", intent(plan))
    with pytest.raises(ManagementError, match="application_effect_not_dispatched"):
        await registry.observe_effect(lease, "api", uid="created-uid", resource_version="9")
    await expire(factory, lease)
    newer = await registry.claim(operation.operation_id)
    assert await registry.dispatch_effect(newer, "api") is True
    assert (await registry.effect_history(newer))[0].dispatch_epoch == newer.runner_epoch


async def test_observation_is_immutable_and_must_match_exact_mutation_target(applications):
    registry, _, _, plan, _, lease = await started(applications)
    await registry.prepare_effect(lease, "stop", intent(plan, action="patch", uid="owned-uid", resource_version="4"))
    await registry.dispatch_effect(lease, "stop")
    for uid, rv in (("replacement-uid", "5"), ("owned-uid", None), ("owned-uid", "bad\nversion")):
        with pytest.raises(ManagementError, match="invalid_application_effect_observation"):
            await registry.observe_effect(lease, "stop", uid=uid, resource_version=rv)
    await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="5")
    await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="5")
    with pytest.raises(ManagementError, match="application_effect_observation_conflict"):
        await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="6")
    assert await registry.dispatch_effect(lease, "stop") is False
    second = await registry.prepare_effect(lease, "delete-pod", intent(plan, api_version="v1", kind="Pod",
        name="api-old-pod", action="delete", uid="pod-uid", resource_version="7"))
    assert second.sequence == 2
    await registry.dispatch_effect(lease, "delete-pod")
    with pytest.raises(ManagementError, match="invalid_application_effect_observation"):
        await registry.observe_effect(lease, "delete-pod", uid="pod-uid", resource_version="8")
    await registry.observe_effect(lease, "delete-pod", uid="pod-uid", resource_version=None)
    assert [item.phase for item in await registry.effect_history(lease)] == ["observed", "observed"]


async def test_stop_retains_uncertain_predecessor_and_capacity_without_foreign_history(applications):
    registry, factory, alice, plan, first, lease = await started(applications)
    await registry.prepare_effect(lease, "late-create", intent(plan))
    await registry.dispatch_effect(lease, "late-create")
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    stop_lease = await registry.claim(stopped.operation_id)
    history = await registry.effect_history(stop_lease)
    assert len(history) == 1 and history[0].operation_id == first.operation_id
    assert history[0].phase == "dispatched"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.effect_history(lease)
    with pytest.raises(ManagementError, match="application_effect_missing"):
        await registry.observe_effect(stop_lease, "late-create", uid="late-uid", resource_version="9")
    _, _, (_, bob), prepare, _, _ = applications
    other = await registry.create(principal=bob, idempotency_key="bob", **prepare("bob", bob))
    bob_lease = await registry.claim(other.operation_id)
    assert await registry.effect_history(bob_lease) == []
    sibling = await registry.create(principal=alice, idempotency_key="alice-feature", **prepare("alice-feature", alice))
    sibling_lease = await registry.claim(sibling.operation_id)
    assert await registry.effect_history(sibling_lease) == []
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.effect_history(replace(bob_lease, application_id=first.application_id))
    async with factory() as session:
        hold = await session.get(NebiusApplicationReservation, first.application_id)
        assert hold.cpu_millis == plan["prepared"].platform_envelope.cpu_millis
        assert (await session.get(NebiusApplicationOperation, stopped.operation_id)).phase == "running"


async def test_retry_preserves_dispatched_effect_and_missing_keys_fail_closed(applications):
    registry, _, alice, plan, first, lease = await started(applications)
    for call in (registry.dispatch_effect(lease, "missing"),
                 registry.observe_effect(lease, "missing", uid="uid", resource_version="1")):
        with pytest.raises(ManagementError, match="application_effect_missing"):
            await call
    await registry.prepare_effect(lease, "api", intent(plan))
    await registry.dispatch_effect(lease, "api")
    await registry.finish_attempt(lease, error_code="uncertain_write", retry=False)
    await registry.retry(first.operation_id, principal=alice)
    newer = await registry.claim(first.operation_id)
    assert await registry.dispatch_effect(newer, "api") is False
    assert (await registry.effect_history(newer))[0].phase == "dispatched"


@pytest.mark.parametrize("kind,name", [
    ("Namespace", None), ("Secret", "loom-application-db"), ("Secret", "loom-application-storage"),
    ("Secret", "loom-application-auth"), ("ResourceQuota", "loom-application-retired"),
])
async def test_exact_bootstrap_and_runtime_material_targets_are_recorded_without_contents(applications, kind, name):
    registry, _, _, plan, _, lease = await started(applications)
    ns = plan["prepared"].registration.application_namespace
    if kind == "Secret":
        name += f"-{plan['prepared'].registration.incarnation.hex}-g1"
    effect = await registry.prepare_effect(lease, "setup", intent(plan, kind=kind, api_version="v1",
        name=name or ns, namespace=None if kind == "Namespace" else ns))
    assert effect.intent.name == (name or ns)
    assert set(effect.intent.model_dump()) == {
        "api_version", "kind", "namespace", "name", "action", "request_sha256", "uid", "resource_version",
    }
    assert await registry.dispatch_effect(lease, "setup") is True


async def test_credential_effects_follow_frozen_source_not_new_stop_intent_generation(applications):
    registry, _, alice, plan, first, lease = await started(applications)
    prefix = f"loom-application-db-{plan['prepared'].registration.incarnation.hex}"
    for name in ("loom-application-db", prefix + "-g2", prefix + "-g0"):
        with pytest.raises(ManagementError, match="invalid_application_effect"):
            await registry.prepare_effect(lease, "wrong", intent(plan, api_version="v1", kind="Secret", name=name))
    await registry.prepare_effect(lease, "db", intent(plan, api_version="v1", kind="Secret", name=prefix + "-g1"))
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    stop_lease = await registry.claim(stopped.operation_id)
    old_material = await registry.prepare_effect(stop_lease, "retire-db", intent(plan, api_version="v1", kind="Secret",
        name=prefix + "-g1", action="delete", uid="old-secret-uid", resource_version="2"))
    assert old_material.intent.name == prefix + "-g1" and stop_lease.access_generation == 2


async def test_retained_historical_fixed_name_plan_is_not_silently_rewritten(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    plan = prepare()
    files = copy.deepcopy(plan["prepared"].files)
    suffix = f"-{plan['prepared'].registration.incarnation.hex}-g1"
    # Model a plan frozen by the older renderer, without changing any live row.
    for docs in files.values():
        for doc in docs:
            if doc["kind"] == "Deployment":
                pod = doc["spec"]["template"]["spec"]
                for container in pod["containers"]:
                    for env in container.get("env", []):
                        if "valueFrom" in env:
                            ref = env["valueFrom"]["secretKeyRef"]
                            ref["name"] = ref["name"].removesuffix(suffix)
                for volume in pod.get("volumes", []):
                    if "secret" in volume:
                        secret = volume["secret"]
                        secret["secretName"] = secret["secretName"].removesuffix(suffix)
    plan["prepared"] = replace(plan["prepared"], files=files)
    first = await registry.create(principal=alice, idempotency_key="historical", **plan)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="destroy_retained", expected_generation=1)
    lease = await registry.claim(stopped.operation_id)
    assert (await registry.frozen_plan(lease))["files"] == files
    effect = await registry.prepare_effect(lease, "old-db", intent(plan, api_version="v1", kind="Secret",
        name="loom-application-db", action="delete", uid="historical-uid", resource_version="4"))
    assert effect.intent.name == "loom-application-db"


@pytest.mark.parametrize("action", ["update", "resume", "suspend", "destroy_retained"])
async def test_current_lease_can_only_delete_own_prior_generation_material(applications, platform_inputs, action):
    registry, factory, alice, plan, first, first_lease = await started(applications)
    original_files = copy.deepcopy((await registry.frozen_plan(first_lease))["files"])
    _, _, _, prepare, _, _ = applications
    sibling_plan = prepare("alice-feature", alice)
    sibling = await registry.create(principal=alice, idempotency_key="sibling", **sibling_plan)
    await _observed_complete(factory, sibling.operation_id)
    old_generations = [1]
    if action == "resume":
        stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="suspend",
            action="suspend", expected_generation=1)
        await _observed_complete(factory, stopped.operation_id)
        newer = _next_plan(plan, platform_inputs, new_release=False, deployment_generation=3, access_generation=3)
        current = await registry.transition(first.application_id, principal=alice, idempotency_key="resume",
            action="resume", expected_generation=2, **newer)
    else:
        await _observed_complete(factory, first.operation_id)
        newer = _next_plan(plan, platform_inputs)
        current = await registry.transition(first.application_id, principal=alice, idempotency_key="update",
            action="update", expected_generation=1, release_id=newer["release"].release_id, **newer)
        if action != "update":
            # Stop interrupts g2 before it has retired g1. Neither old lease can
            # perform cleanup, so g3 must cover BOTH retained material generations.
            current = await registry.transition(first.application_id, principal=alice, idempotency_key="stop-update",
                action=action, expected_generation=2)
            old_generations.append(2)
    lease = await registry.claim(current.operation_id)
    prefix = f"loom-application-db-{plan['prepared'].registration.incarnation.hex}"
    old = intent(plan, api_version="v1", kind="Secret", name=prefix + "-g1", action="delete",
                 uid="old-secret-uid", resource_version="2")
    sibling_name = f"loom-application-db-{sibling_plan['prepared'].registration.incarnation.hex}-g1"
    for change in (
        {"action": "create", "uid": None, "resource_version": None}, {"action": "patch"},
        {"uid": None}, {"resource_version": None}, {"namespace": "loom-shared"},
        {"name": sibling_name}, {"name": prefix + "-g99"},
    ):
        with pytest.raises(ManagementError, match="invalid_application_effect"):
            await registry.prepare_effect(lease, "invalid", old | change)
    assert await registry.effect_history(lease) == []
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.prepare_effect(first_lease, "stale-delete", old)
    for generation in old_generations:
        for purpose in ("db", "storage", "auth"):
            name = f"loom-application-{purpose}-{plan['prepared'].registration.incarnation.hex}-g{generation}"
            key = f"retire-{purpose}-{generation}"
            effect = await registry.prepare_effect(lease, key, old | {"name": name})
            assert effect.intent.name == name and effect.intent.action == "delete"
            assert await registry.dispatch_effect(lease, key) is True
            await registry.observe_effect(lease, key, uid="old-secret-uid", resource_version=None)
    history = await registry.effect_history(lease)
    assert len(history) == 3 * len(old_generations)
    assert all(effect.phase == "observed" for effect in history)
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, first.operation_id)).plan_json["files"] == original_files


async def test_concurrent_distinct_intents_cannot_both_prepare_before_reconciliation(applications):
    registry, _, _, plan, _, lease = await started(applications)
    results = await asyncio.gather(*[
        registry.prepare_effect(lease, key, intent(plan, name=key)) for key in ("loom-service", "loom-web")
    ], return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    failure = next(result for result in results if isinstance(result, Exception))
    assert isinstance(failure, ManagementError) and failure.code == "application_effect_unresolved"
    assert len(await registry.effect_history(lease)) == 1


async def test_definitively_rejected_effect_is_retained_but_allows_a_fresh_preconditioned_attempt(applications):
    registry, _, _, plan, _, lease = await started(applications)
    request = intent(plan, action="delete", uid="old-uid", resource_version="1")
    await registry.prepare_effect(lease, "stale-delete", request)
    with pytest.raises(ManagementError, match="application_effect_not_dispatched"):
        await registry.reject_effect(lease, "stale-delete", status_code=409)
    await registry.dispatch_effect(lease, "stale-delete")
    for status in (200, 408, 429, 500):
        with pytest.raises(ManagementError, match="invalid_application_effect_rejection"):
            await registry.reject_effect(lease, "stale-delete", status_code=status)
    await registry.reject_effect(lease, "stale-delete", status_code=409)
    await registry.reject_effect(lease, "stale-delete", status_code=409)
    rejected = await registry.prepare_effect(lease, "stale-delete", request)
    assert rejected.phase == "rejected" and rejected.rejection_status == 409
    assert rejected.observed_uid is None and rejected.observed_resource_version is None
    assert await registry.dispatch_effect(lease, "stale-delete") is False
    with pytest.raises(ManagementError, match="application_effect_observation_conflict"):
        await registry.reject_effect(lease, "stale-delete", status_code=422)
    with pytest.raises(ManagementError, match="application_effect_observation_conflict"):
        await registry.observe_effect(lease, "stale-delete", uid="old-uid", resource_version=None)
    fresh = await registry.prepare_effect(lease, "fresh-delete", request | {"resource_version": "2"})
    assert fresh.sequence == 2 and fresh.phase == "prepared"
    assert await registry.dispatch_effect(lease, "fresh-delete") is True
