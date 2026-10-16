"""Real PostgreSQL application intent, replay, mixed admission and lease fencing."""
from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_render import render_application
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_render import inputs


@pytest.fixture
def applications(environment_registry, platform_inputs):
    from loom_service.application_management.registry import ApplicationRegistry

    legacy, factory, principals, prepare_legacy = environment_registry
    release_id = uuid4()

    def prepare(slug="alice", principal=principals[0], **changes):
        row, release, shared, foundation = inputs(platform_inputs, slug)
        release = release.model_copy(update={"release_id": release_id})
        row = ApplicationRegistrationV1.model_validate(row.model_dump() | {
            "owner_user_id": principal.user_id, "owner_team_id": principal.team_id,
            "release_id": release_id,
        } | changes)
        return dict(prepared=render_application(row, release, shared, foundation), release=release, shared=shared)

    return ApplicationRegistry(factory), factory, principals, prepare, legacy, prepare_legacy


async def test_concurrent_create_replay_freezes_one_plan_and_one_reservation(applications):
    from loom.db.nebius_application_operation_schema import (
        NebiusApplicationOperation,
        NebiusApplicationReservation,
    )
    from loom.db.nebius_application_schema import NebiusApplication

    registry, factory, (alice, _), prepare, _, _ = applications
    results = await asyncio.gather(*[
        registry.create(principal=alice, idempotency_key="same-request", **prepare()) for _ in range(3)
    ])
    assert all(result == results[0] for result in results)
    assert results[0].phase == "pending"
    assert results[0].deployment_generation == results[0].access_generation == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplication)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationOperation)) == 1
        reservation = (await session.scalars(select(NebiusApplicationReservation))).one()
        assert reservation.storage_mib == 0
        assert reservation.cpu_millis > 0
        operation = await session.get(NebiusApplicationOperation, results[0].operation_id)
        original = operation.plan_json
    changed = prepare()
    changed["prepared"].files["10-network.yaml"][0]["metadata"]["annotations"] = {"changed-default": "later"}
    assert await registry.create(principal=alice, idempotency_key="same-request", **changed) == results[0]
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, results[0].operation_id)
        assert operation.plan_json == original


async def test_replay_needs_no_publication_or_render_inputs_and_conflicting_intent_fails(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="replay", **plan)
    assert await registry.replay_create(principal=alice, idempotency_key="replay", slug="alice",
                                       release_id=plan["release"].release_id) == first
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.replay_create(principal=alice, idempotency_key="replay", slug="alice", release_id=uuid4())
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, idempotency_key="replay", **prepare("another"))


async def test_owner_and_team_isolation_and_scopes_apply_to_reads_and_writes(applications):
    registry, _, (alice, bob), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="owned", **prepare())
    assert await registry.get_operation(first.operation_id, principal=alice) == first
    for foreign in (bob, replace(alice, team_id=uuid4())):
        with pytest.raises(ManagementError, match="application_forbidden"):
            await registry.get_operation(first.operation_id, principal=foreign)
        assert await registry.list_applications(principal=foreign) == []
    with pytest.raises(ManagementError, match="application_owner_mismatch"):
        await registry.create(principal=bob, idempotency_key="stolen", **prepare())
    with pytest.raises(ManagementError) as denied:
        await registry.create(principal=replace(alice, scopes=["read:own"]), idempotency_key="weak", **prepare())
    assert denied.value.status_code == 403


@pytest.mark.parametrize("change", ["data", "release", "storage", "envelope", "generation"])
async def test_invalid_prepared_binding_never_commits(applications, change):
    from loom.db.nebius_application_schema import NebiusApplication

    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    if change == "data":
        plan["shared"] = plan["shared"].model_copy(update={"data_environment_id": uuid4()})
    elif change == "release":
        plan["release"] = plan["release"].model_copy(update={"release_id": uuid4()})
    elif change in {"storage", "envelope"}:
        costs = replace(plan["prepared"].platform_envelope, **({"storage_mib": 1} if change == "storage" else {"cpu_millis": 0}))
        plan["prepared"] = replace(plan["prepared"], platform_envelope=costs)
    else:
        row = plan["prepared"].registration.model_copy(update={"access_generation": 2})
        plan["prepared"] = replace(plan["prepared"], registration=row)
    with pytest.raises(ManagementError, match="invalid_application_plan"):
        await registry.create(principal=alice, idempotency_key="invalid", **plan)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplication)) == 0


@pytest.mark.parametrize("first", ["application", "legacy", "concurrent"])
async def test_application_and_legacy_share_one_capacity_allowance(applications, first):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget

    registry, factory, (alice, _), prepare, legacy, prepare_legacy = applications
    app, old = prepare(), prepare_legacy("old")
    async with factory.begin() as session:
        budget = await session.get(NebiusPlatformBudget, old.registration.cluster_id)
        budget.cpu_millis = max(old.platform_envelope.cpu_millis, app["prepared"].platform_envelope.cpu_millis)
    async def create_app():
        return await registry.create(principal=alice, idempotency_key="app", **app)
    async def create_old():
        return await legacy.create(principal=alice, idempotency_key="old", prepared=old)
    if first == "concurrent":
        results = await asyncio.gather(create_app(), create_old(), return_exceptions=True)
        assert sum(not isinstance(result, Exception) for result in results) == 1
        errors = [result for result in results if isinstance(result, Exception)]
        assert isinstance(errors[0], ManagementError) and errors[0].code == "platform_capacity_exhausted"
    else:
        winner, loser = (create_app, create_old) if first == "application" else (create_old, create_app)
        await winner()
        with pytest.raises(ManagementError, match="platform_capacity_exhausted"):
            await loser()
    async with factory() as session:
        assert (await session.scalar(select(func.count()).select_from(NebiusApplication))
                + await session.scalar(select(func.count()).select_from(NebiusEnvironment))) == 1


async def test_name_conflict_rolls_back_operation_and_reservation(applications):
    from loom.db.nebius_application_operation_schema import (
        NebiusApplicationOperation,
        NebiusApplicationReservation,
    )

    registry, factory, (alice, _), prepare, legacy, prepare_legacy = applications
    await legacy.create(principal=alice, idempotency_key="legacy-name", prepared=prepare_legacy())
    with pytest.raises(ManagementError, match="application_name_conflict"):
        await registry.create(principal=alice, idempotency_key="colliding-name", **prepare())
    async with factory() as session:
        for model in (NebiusApplicationOperation, NebiusApplicationReservation):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def _observed_complete(factory, operation_id):
    """Test-only provider boundary; no production completion API exists yet."""
    from loom.db.nebius_application_operation_schema import NebiusApplicationOperation

    async with factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, operation_id)
        row.phase, row.lease_token, row.lease_expires_at = "completed", None, None


def _next_plan(plan, platform_inputs, *, new_release=True, **changes):
    original = plan["prepared"].registration
    release = plan["release"]
    if new_release:
        release = release.model_copy(update={"release_id": uuid4(), "source_digest": "sha256:" + "c" * 64})
    row = original.model_copy(update={
        "deployment_generation": original.deployment_generation + 1,
        "access_generation": original.access_generation + 1,
        "release_id": release.release_id, "desired_state": "active",
    } | changes)
    return dict(prepared=render_application(row, release, plan["shared"], inputs(platform_inputs)[3]),
                release=release, shared=plan["shared"])


@pytest.mark.parametrize("action", ["suspend", "destroy_retained"])
async def test_stop_invalidates_inflight_lease_but_retains_names_capacity_and_frozen_source(applications, action):
    from loom.db.nebius_application_operation_schema import (
        NebiusApplicationOperation,
        NebiusApplicationReservation,
    )
    from loom.db.nebius_application_schema import NebiusDeploymentNameClaim

    registry, factory, (alice, _), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="start", **prepare())
    lease = await registry.claim(first.operation_id)
    old = await registry.frozen_plan(lease)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
                                         expected_generation=1, action=action)
    assert stopped.deployment_generation == stopped.access_generation == 2
    assert stopped.phase == "pending"  # no claim of stopped processes
    for check in (registry.renew(lease), registry.frozen_plan(lease),
                  registry.finish_attempt(lease, error_code="old_effect", retry=True)):
        with pytest.raises(ManagementError, match="stale_operation_lease"):
            await check
    assert await registry.claim(first.operation_id) is None
    with pytest.raises(ManagementError, match="stale_operation_generation"):
        await registry.retry(first.operation_id, principal=alice)
    replay = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
                                      expected_generation=1, action=action)
    assert replay == stopped
    async with factory() as session:
        reservation = await session.get(NebiusApplicationReservation, first.application_id)
        assert reservation.cpu_millis == old["platform_envelope"]["cpu_millis"]
        assert await session.scalar(select(func.count()).select_from(NebiusDeploymentNameClaim)) == 3
        assert (await session.get(NebiusApplicationOperation, first.operation_id)).plan_json == old
        current = await session.get(NebiusApplicationOperation, stopped.operation_id)
        assert current.plan_json["source_operation_id"] == str(first.operation_id)
    stop_lease = await registry.claim(stopped.operation_id)
    assert (await registry.frozen_plan(stop_lease))["shared"] == old["shared"]


async def test_expected_generation_concurrency_has_exactly_one_transition(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="create", **prepare())
    results = await asyncio.gather(*[
        registry.transition(first.application_id, principal=alice, idempotency_key=action,
                            expected_generation=1, action=action) for action in ("suspend", "destroy_retained")
    ], return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    error = next(result for result in results if isinstance(result, Exception))
    assert isinstance(error, ManagementError) and error.code == "application_generation_conflict"


async def test_update_requires_completed_predecessor_and_freezes_new_version(applications, platform_inputs):
    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="create", **plan)
    newer = _next_plan(plan, platform_inputs)
    change = dict(principal=alice, idempotency_key="update", action="update", expected_generation=1,
                  release_id=newer["release"].release_id)
    with pytest.raises(ManagementError, match="application_transition_not_ready"):
        await registry.transition(first.application_id, **change, **newer)
    await _observed_complete(factory, first.operation_id)
    updated = await registry.transition(first.application_id, **change, **newer)
    assert updated.action == "update" and updated.access_generation == 2
    # Replay requires neither the publication reader nor prepared plan.
    assert await registry.transition(first.application_id, **change) == updated
    lease = await registry.claim(updated.operation_id)
    frozen = await registry.frozen_plan(lease)
    assert frozen["release"]["source_digest"] == "sha256:" + "c" * 64
    assert frozen["shared"] == plan["shared"].model_dump(mode="json")
    assert frozen["source_operation_id"] == str(first.operation_id)
    assert frozen["registration"]["application_id"] == str(first.application_id)


@pytest.mark.parametrize("field", ["incarnation", "owner_user_id", "owner_team_id", "data_environment_id", "slug", "access_generation"])
async def test_update_cannot_replace_immutable_binding_or_skip_access_generation(applications, platform_inputs, field):
    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="create", **plan)
    await _observed_complete(factory, first.operation_id)
    newer = _next_plan(plan, platform_inputs)
    row = newer["prepared"].registration.model_copy(update={field: "another" if field == "slug" else 3 if field == "access_generation" else uuid4()})
    newer["prepared"] = replace(newer["prepared"], registration=row)
    with pytest.raises(ManagementError, match="invalid_application_plan"):
        await registry.transition(first.application_id, principal=alice, idempotency_key="invalid-update",
                                  action="update", expected_generation=1, release_id=newer["release"].release_id, **newer)
    assert (await registry.list_applications(principal=alice))[0].deployment_generation == 1


async def test_resume_waits_for_observed_suspend_and_destroy_is_terminal(applications, platform_inputs):
    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="create", **plan)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="suspend", action="suspend", expected_generation=1)
    resumed_plan = _next_plan(plan, platform_inputs, new_release=False, deployment_generation=3, access_generation=3)
    with pytest.raises(ManagementError, match="application_transition_not_ready"):
        await registry.transition(first.application_id, principal=alice, idempotency_key="resume", action="resume", expected_generation=2, **resumed_plan)
    await _observed_complete(factory, stopped.operation_id)
    resumed = await registry.transition(first.application_id, principal=alice, idempotency_key="resume", action="resume", expected_generation=2, **resumed_plan)
    assert resumed.deployment_generation == 3
    destroyed = await registry.transition(first.application_id, principal=alice, idempotency_key="destroy", action="destroy_retained", expected_generation=3)
    await _observed_complete(factory, destroyed.operation_id)
    with pytest.raises(ManagementError, match="application_transition_not_supported"):
        await registry.transition(first.application_id, principal=alice, idempotency_key="revive", action="resume", expected_generation=4, **resumed_plan)


async def test_leases_use_database_expiry_epochs_and_all_identity_fields(applications):
    from loom.db.nebius_application_operation_schema import NebiusApplicationOperation

    registry, factory, (alice, _), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="create", **prepare())
    leases = await asyncio.gather(registry.claim(first.operation_id), registry.claim(first.operation_id))
    assert sum(lease is not None for lease in leases) == 1
    lease = next(lease for lease in leases if lease is not None)
    await registry.renew(lease)
    for field in ("application_id", "incarnation", "deployment_generation", "access_generation", "runner_epoch", "lease_token"):
        corrupt = replace(lease, **{field: 999 if field.endswith("generation") or field == "runner_epoch" else uuid4()})
        with pytest.raises(ManagementError, match="stale_operation_lease"):
            await registry.frozen_plan(corrupt)
    async with factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, first.operation_id)
        row.lease_expires_at = await session.scalar(text("SELECT clock_timestamp() - interval '1 second'"))
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.renew(lease)
    second = await registry.claim(first.operation_id)
    assert second.runner_epoch == lease.runner_epoch + 1 and second.lease_token != lease.lease_token
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.finish_attempt(lease, error_code="stale", retry=False)
    await registry.finish_attempt(second, error_code="provider_unavailable", retry=False)
    assert await registry.claim(first.operation_id) is None
    assert (await registry.retry(first.operation_id, principal=alice)).phase == "pending"
    third = await registry.claim(first.operation_id)
    assert third.runner_epoch == second.runner_epoch + 1


async def test_foreign_transition_and_retry_cannot_mutate_an_application(applications):
    registry, _, (alice, bob), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="create", **prepare())
    for foreign in (bob, replace(alice, team_id=uuid4())):
        with pytest.raises(ManagementError, match="application_forbidden"):
            await registry.transition(first.application_id, principal=foreign, idempotency_key="foreign-stop",
                                      action="destroy_retained", expected_generation=1)
        with pytest.raises(ManagementError, match="application_forbidden"):
            await registry.retry(first.operation_id, principal=foreign)
    assert (await registry.list_applications(principal=alice))[0].deployment_generation == 1


async def test_duplicate_transition_replays_and_key_reuse_conflicts(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="create", **prepare())
    changes = await asyncio.gather(*[
        registry.transition(first.application_id, principal=alice, idempotency_key="same-stop", action="suspend", expected_generation=1)
        for _ in range(3)
    ])
    assert all(change == changes[0] for change in changes)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.transition(first.application_id, principal=alice, idempotency_key="same-stop", action="destroy_retained", expected_generation=2)


async def test_update_capacity_failure_preserves_current_generation_and_larger_hold_is_retained(applications, platform_inputs):
    from loom.db.nebius_application_operation_schema import NebiusApplicationReservation
    from loom.db.nebius_environment_schema import NebiusPlatformBudget
    from loom.nebius_environment_render import _envelope

    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="create", **plan)
    await _observed_complete(factory, first.operation_id)
    bigger = _next_plan(plan, platform_inputs)
    files = copy.deepcopy(bigger["prepared"].files)
    for docs in files.values():
        for doc in docs:
            if doc["kind"] == "Deployment":
                doc["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "500m"
    bigger["prepared"] = replace(bigger["prepared"], files=files, platform_envelope=_envelope(files))
    cluster = plan["prepared"].registration.cluster_id
    async with factory.begin() as session:
        (await session.get(NebiusPlatformBudget, cluster)).cpu_millis = plan["prepared"].platform_envelope.cpu_millis
    args = dict(principal=alice, idempotency_key="grow", action="update", expected_generation=1, release_id=bigger["release"].release_id)
    with pytest.raises(ManagementError, match="platform_capacity_exhausted"):
        await registry.transition(first.application_id, **args, **bigger)
    assert (await registry.list_applications(principal=alice))[0].deployment_generation == 1
    assert (await registry.get_operation(first.operation_id, principal=alice)).phase == "completed"
    async with factory.begin() as session:
        (await session.get(NebiusPlatformBudget, cluster)).cpu_millis = bigger["prepared"].platform_envelope.cpu_millis
    updated = await registry.transition(first.application_id, **args, **bigger)
    await _observed_complete(factory, updated.operation_id)
    smaller = _next_plan(bigger, platform_inputs)
    await registry.transition(first.application_id, principal=alice, idempotency_key="shrink", action="update", expected_generation=2,
                              release_id=smaller["release"].release_id, **smaller)
    async with factory() as session:
        hold = await session.get(NebiusApplicationReservation, first.application_id)
        assert hold.cpu_millis == bigger["prepared"].platform_envelope.cpu_millis


@pytest.mark.parametrize("seconds", [0, -1, 301, True, 1.5])
async def test_invalid_lease_duration_is_rejected_before_claim(applications, seconds):
    registry, _, (alice, _), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="create", **prepare())
    with pytest.raises(ValueError, match="between 1 and 300"):
        await registry.claim(first.operation_id, lease_seconds=seconds)
    assert (await registry.get_operation(first.operation_id, principal=alice)).phase == "pending"
