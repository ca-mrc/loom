"""Shared physical inventory never grants independent guest admission authority."""

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ExecutionTargetPriceBinding,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom.execution_contract import nebius_guest_execution_class, workload_requirements_from_task
from loom.execution_runtime_contract import runtime_pod_resources
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    create_execution_capacity_observation,
    fetch_execution_capacity_status,
    upsert_execution_capacity_policy,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.execution_finance import (
    upsert_execution_budget_policy,
    upsert_target_price_binding,
)
from loom_control_plane.service_execution import (
    persist_execution_catalog,
    set_execution_target_health,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_owner import _alias
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_nebius_task_image_capacity import (
    _native,
)
from tests.integration.test_nebius_task_image_capacity import (
    native_setup as _native_setup,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)
from tests.unit.test_guest_execution_materialization import _compile, _guest_inputs

native_setup = _native_setup


async def _family(session, now, *, publish=True, **placement_args):
    pair = await _seed_ready_trial(session, now=now)
    alias = _alias(pair[1])
    await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(alias,))
    await set_execution_target_health(session, target_id=alias.target_id, desired_state="active",
        observed_state="ready", health_status="healthy", observed_at=now)
    price = await session.get(ExecutionTargetPriceBinding, pair[1].target_id)
    await upsert_target_price_binding(session, target_id=alias.target_id,
        price_snapshot_id=price.price_snapshot_id, enabled=True, reason="guest test", now=now)
    await upsert_execution_budget_policy(session, scope_kind="target", scope_key=alias.target_id,
        daily_limit_microusd=100_000_000, monthly_limit_microusd=1_000_000_000,
        per_attempt_limit_microusd=10_000_000, max_estimate_duration_seconds=7200,
        emergency_stop=False, enabled=True, reason="guest test", now=now)
    owner_trial = await session.get(Trial, pair[0])
    guest_id = uuid4()
    session.add(Trial(id=guest_id, team_id=owner_trial.team_id, task_id=owner_trial.task_id,
        config=owner_trial.config, requires_caps=owner_trial.requires_caps, state="queued", attempt_count=0))
    group = await resolve_capacity_targets(session, alias.target_id)
    placement = placement_fixture(target_id=pair[1].target_id, **placement_args)
    placement["target_scope"] = group.scope.model_dump(mode="json")
    if publish:
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
    return pair, (guest_id, alias), placement


async def _guest_reserve(session, pair, now):
    task, trial, profile = _guest_inputs()
    return await _reserve(session, trial_id=pair[0], target=pair[1], now=now,
        requirements=workload_requirements_from_task(task), runtime_contract=_compile(task, trial, profile))


@pytest.mark.parametrize("competitor", ["ordinary", "native"])
async def test_guest_races_ordinary_or_native_for_one_physical_node(native_setup, competitor):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    task, trial, profile = _guest_inputs()
    total = runtime_pod_resources(_compile(task, trial, profile))
    async with sessions() as session, session.begin():
        owner, guest, _ = await _family(session, now, node_cpu=total.cpu_millis,
            node_memory=total.memory_mib, node_storage=total.ephemeral_storage_mib,
            quota_nodes=1, used_nodes=0, nodes=0)
        attempt, _ = await _native(session, owned, owner, now, cpu=1000)

    async def claim(is_guest):
        try:
            async with sessions() as session, session.begin():
                if is_guest:
                    return await _guest_reserve(session, guest, now + timedelta(seconds=2))
                if competitor == "native":
                    return await reserve_native_task_image_capacity(session, attempt_id=attempt, now=now + timedelta(seconds=2))
                return await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))
        except ExecutionProvisioningBlockedError as error:
            return error.reason

    results = await asyncio.gather(claim(True), claim(False))
    assert sum(isinstance(row, (dict, ServiceExecutionLease)) for row in results) == 1
    assert "execution_capacity_provider_quota_nodes_exceeded" in results


@pytest.mark.parametrize("limit,reason", [("max_pending_jobs", "pending_limit"), ("max_create_per_minute", "create_rate")])
@pytest.mark.parametrize("first", ["ordinary", "guest", "native"])
async def test_pending_and_rate_limits_cover_whole_family(native_setup, limit, reason, first):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, _ = await _family(session, now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        setattr(policy, limit, 1)
        if first == "native":
            attempt, _ = await _native(session, owned, owner, now, cpu=1000)
            await reserve_native_task_image_capacity(session, attempt_id=attempt, now=now + timedelta(seconds=2))
        elif first == "ordinary":
            await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))
        else:
            await _guest_reserve(session, guest, now + timedelta(seconds=2))
    with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
        async with sessions() as session, session.begin():
            if first == "guest":
                await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=3))
            else:
                await _guest_reserve(session, guest, now + timedelta(seconds=3))


@pytest.mark.parametrize("observed", [False, True])
async def test_disabled_guest_keeps_occupancy_and_observed_lease_is_not_double_charged(native_setup, observed):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, placement = await _family(session, now, quota_nodes=1)
        lease = await _guest_reserve(session, guest, now + timedelta(seconds=2))
        auth = await session.scalar(select(ExecutionProvisioningAuthorization).where(
            ExecutionProvisioningAuthorization.lease_id == lease.id))
        total = {"cpu_millis": auth.requested_cpu_millis, "memory_mib": auth.requested_memory_mib,
                 "storage_mib": auth.requested_storage_mib}
        # Leave only one ordinary request after the guest's committed demand.
        shape = {"cpu_millis": total["cpu_millis"] + 1000,
                 "memory_mib": total["memory_mib"] + 1024,
                 "storage_mib": total["storage_mib"] + 2048}
        placement["nodes"][0]["allocatable"] = shape
        placement["template_samples"][0]["allocatable"] = shape
        if observed:
            placement["nodes"][0].update(requested=total, used_pod_slots=1, managed_pods=[{
                "uid": "guest-pod", "lease_id": str(lease.id), "generation": lease.resource_generation,
                "requests": total,
            }])
        guest_row = await session.get(ServiceExecutionTarget, guest[1].target_id)
        guest_row.desired_state = "disabled"
        await _record(session, owner[1].target_id, now + timedelta(seconds=3), placement)
        ordinary = await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=4))
        assert ordinary.target_id == owner[1].target_id
        # Both demand identities must be retained even though guest intent is disabled.
        from loom_control_plane.execution_capacity import admit_capacity_resources
        from loom_execution_capacity_collector.contracts import ResourceTotals
        with pytest.raises(ExecutionProvisioningBlockedError, match="quota_nodes_exceeded"):
            await admit_capacity_resources(session, target=await session.get(ServiceExecutionTarget, owner[1].target_id),
                demand_id="next", resources=ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048),
                current_time=now + timedelta(seconds=5))


@pytest.mark.parametrize("guest", [False, True])
async def test_registration_invalidates_old_observation_for_whole_family(native_setup, guest):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, alias, _ = await _family(session, now, publish=False)
    with pytest.raises(ExecutionProvisioningBlockedError, match="target_scope"):
        async with sessions() as session, session.begin():
            if guest:
                await _guest_reserve(session, alias, now + timedelta(seconds=2))
            else:
                await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))


@pytest.mark.parametrize("damage", ["owner_disabled", "guest_unhealthy", "owner_policy_disabled", "stale"])
async def test_guest_requires_own_health_and_owner_authority(native_setup, damage):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, _ = await _family(session, now)
        if damage == "owner_disabled":
            (await session.get(ServiceExecutionTarget, owner[1].target_id)).desired_state = "disabled"
        elif damage == "guest_unhealthy":
            await set_execution_target_health(session, target_id=guest[1].target_id,
                desired_state="active", observed_state="degraded", health_status="unhealthy", observed_at=now)
        elif damage == "owner_policy_disabled":
            (await session.get(ExecutionCapacityPolicy, owner[1].target_id)).enabled = False
        reason = {"owner_disabled": "owner_not_active", "guest_unhealthy": "target_unhealthy",
                  "owner_policy_disabled": "policy_unavailable", "stale": "observation_stale"}[damage]
        from loom_control_plane.execution_capacity import admit_capacity_resources
        from loom_execution_capacity_collector.contracts import ResourceTotals
        with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
            await admit_capacity_resources(session, target=await session.get(ServiceExecutionTarget, guest[1].target_id),
                demand_id="health-test", resources=ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048),
                current_time=now + timedelta(seconds=902 if damage == "stale" else 2))


async def test_only_owner_can_publish_policy_and_exact_membership_observation(native_setup):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, placement = await _family(session, now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        names = inspect.signature(upsert_execution_capacity_policy).parameters
        args = {key: getattr(policy, key) for key in names if key not in {"session", "now"}}
        args["target_id"] = guest[1].target_id
        with pytest.raises(ValueError, match="owner"):
            await upsert_execution_capacity_policy(session, **args)
        observation = await session.scalar(select(ExecutionCapacityObservation).where(
            ExecutionCapacityObservation.target_id == owner[1].target_id).order_by(ExecutionCapacityObservation.observed_at.desc()).limit(1))
        names = inspect.signature(create_execution_capacity_observation).parameters
        args = {key: deepcopy(value) for key, value in observation.observation_json.items() if key in names}
        args.update(target_id=guest[1].target_id, observed_at=now + timedelta(seconds=3), source_version=str(uuid4()))
        with pytest.raises(ValueError, match="owner"):
            await create_execution_capacity_observation(session, **args)
        for damage in (None, {**placement["target_scope"], "target_ids": sorted([owner[1].target_id, "foreign-target"])}):
            args.update(target_id=owner[1].target_id, placement={**placement, "target_scope": damage})
            with pytest.raises(ValueError, match="scope"):
                await create_execution_capacity_observation(session, **args)


async def test_guest_status_and_resource_allocation_resolve_owner_without_rebinding_identity(native_setup):
    from loom_control_plane.execution_resource_allocation import allocate_target_resources

    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, placement = await _family(session, now)
        task, trial, profile = _guest_inputs()
        plan = await allocate_target_resources(session, _compile(task, trial, profile),
            target_id=guest[1].target_id, now=now + timedelta(seconds=2))
        assert plan.node_resource_allocation.target_id == guest[1].target_id
        status = await fetch_execution_capacity_status(session, now=now + timedelta(seconds=2))
        rows = {row["target_id"]: row for row in status["targets"]}
        assert rows[guest[1].target_id]["capacity_owner_target_id"] == owner[1].target_id
        assert rows[guest[1].target_id]["target_scope"] == placement["target_scope"]
        assert rows[guest[1].target_id]["observation"]["id"] == rows[owner[1].target_id]["observation"]["id"]
