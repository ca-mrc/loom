"""Native builds and execution Pods share one PostgreSQL capacity authority."""

from __future__ import annotations

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ServiceExecutionLease,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
)
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    create_execution_capacity_observation,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


@pytest.fixture
async def native_setup(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owned = []
    try:
        yield sessions, owned
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.materialization_id.in_(owned),
            ))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id.in_(owned)))
        await engine.dispose()


async def _native(session, owned, pair, now, *, cpu=64000, checksum="2" * 64):
    trial = await session.get(Trial, pair[0])
    row = TaskImageMaterialization(
        id=uuid4(), materialization_key=uuid4().hex * 2, task_id=trial.task_id, task_checksum=checksum,
        cpu_arch="x86_64", task_config={}, state="claimed", claimed_by="native", lease_epoch=1,
        attempt_count=1, lease_expires_at=now + timedelta(minutes=5),
    )
    owned.append(row.id)
    session.add(row)
    await session.flush()
    attempt = TaskImageMaterializationAttempt(
        id=uuid4(), materialization_id=row.id, attempt_number=1, lease_epoch=1,
        builder_id="native", claimed_at=now,
        native_build={"target_id": pair[1].target_id, "namespace": "builds", "job_name": "test-build",
                      "state": "reserved", "reserved_at": now.isoformat(),
                      "resources": {"vcpu_millis": cpu, "memory_mib": 1024, "storage_mib": 2048}},
    )
    session.add(attempt)
    await session.flush()
    return attempt.id, row.id


def _io_placement(pair, now, *, pressure=0.0, quota_nodes=1, requested_cpu=0):
    placement = placement_fixture(target_id=pair[1].target_id, nodes=1, used_nodes=1,
                                  quota_nodes=quota_nodes, requested_cpu=requested_cpu)
    node = placement["nodes"][0]
    node.update(name="worker-1", draining=False,
                io_pressure={"sampled_at": now.isoformat(), "full_avg10": pressure})
    return placement


async def _io_native(session, owned, pair, now, *, cpu=1000):
    attempt_id, materialization_id = await _native(session, owned, pair, now, cpu=cpu, checksum=uuid4().hex * 2)
    attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
    attempt.native_build = {
        **attempt.native_build, "io_full_avg10_max": 50.0,
        "job": {"metadata": {"name": "test-build"}, "spec": {"template": {"spec": {
            "nodeSelector": {"pool": "shared"}, "tolerations": [],
        }}}},
    }
    return attempt_id, materialization_id


async def test_io_hot_node_cannot_authorize_native_build_without_spare_quota(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        at = now + timedelta(seconds=1)
        await _record(session, pair[1].target_id, at, _io_placement(pair, at, pressure=65.0))
        attempt_id, _ = await _native(session, owned, pair, now, cpu=1000)
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "io_full_avg10_max": 50.0}
    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
    async with sessions() as session:
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        assert "capacity_reserved_at" not in attempt.native_build


async def test_io_feedback_permits_second_active_build_after_actual_ten_second_window(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        at = now + timedelta(seconds=1)
        placement = _io_placement(pair, at)
        await _record(session, pair[1].target_id, at, placement)
        first, _ = await _io_native(session, owned, pair, now)
        original = await reserve_native_task_image_capacity(session, attempt_id=first, now=at)
        second, _ = await _io_native(session, owned, pair, now)
        assert original["io_binding"]["mode"] == "node"
        pod = original["job"]["spec"]["template"]["spec"]
        assert pod["nodeSelector"] == {"pool": "shared"}
        fields = pod["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchFields"]
        assert fields == [{"key": "metadata.name", "operator": "In", "values": ["worker-1"]}]
    with pytest.raises(ExecutionProvisioningBlockedError, match="feedback_pending"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=second, now=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        first_attempt = await session.get(TaskImageMaterializationAttempt, first)
        first_attempt.native_build = {
            **first_attempt.native_build, "state": "running", "pod_uid": "observed-first-pod",
            "node_name": "worker-1", "phases": [{"name": "build", "state": {
                "running": {"startedAt": (now + timedelta(seconds=3)).isoformat()},
            }}],
        }
        at = now + timedelta(seconds=12)
        await _record(session, pair[1].target_id, at, _io_placement(pair, at))
    with pytest.raises(ExecutionProvisioningBlockedError, match="feedback_pending"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=second, now=now + timedelta(seconds=12))
    async with sessions() as session, session.begin():
        at = now + timedelta(seconds=14)
        await _record(session, pair[1].target_id, at, _io_placement(pair, at))
        result = await reserve_native_task_image_capacity(session, attempt_id=second, now=at)
        assert result["io_binding"]["node_uid"] == original["io_binding"]["node_uid"]
        assert (await session.get(TaskImageMaterializationAttempt, first)).native_build.get("capacity_released_at") is None
        assert result.get("capacity_released_at") is None


@pytest.mark.parametrize("damage", ["missing", "stale", "future", "61s"])
async def test_io_unknown_metric_does_not_authorize_or_consume_a_reservation(native_setup, damage):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    at = now + timedelta(seconds=1)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        policy.observation_max_age_seconds = 300
        placement = _io_placement(pair, at, quota_nodes=10)
        if damage == "missing":
            placement["nodes"][0].pop("io_pressure")
        else:
            delta = (timedelta(seconds=-61) if damage == "61s" else
                     timedelta(minutes=-30 if damage == "stale" else 10))
            placement["nodes"][0]["io_pressure"]["sampled_at"] = (at + delta).isoformat()
        await _record(session, pair[1].target_id, at, placement)
        attempt_id, _ = await _io_native(session, owned, pair, now)
    with pytest.raises(ExecutionProvisioningBlockedError, match="observation_unavailable"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at)
    async with sessions() as session:
        assert "capacity_reserved_at" not in (await session.get(TaskImageMaterializationAttempt, attempt_id)).native_build


@pytest.mark.parametrize("existing_nodes", [0, 1])
async def test_io_healthy_but_resource_full_node_uses_one_unobserved_cold_bootstrap(native_setup, existing_nodes):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    at = now + timedelta(seconds=1)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        placement = (_io_placement(pair, at, quota_nodes=2, requested_cpu=64000) if existing_nodes else
                     placement_fixture(target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=2))
        await _record(session, pair[1].target_id, at, placement)
        first, _ = await _io_native(session, owned, pair, now)
        result = await reserve_native_task_image_capacity(session, attempt_id=first, now=at)
        assert result["io_binding"] == {"mode": "bootstrap", "excluded_node_names": ["worker-1"] if existing_nodes else []}
        second, _ = await _io_native(session, owned, pair, now)
    with pytest.raises(ExecutionProvisioningBlockedError, match="feedback_pending"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=second, now=at)


async def test_io_frozen_node_revalidation_cannot_move_to_new_healthy_node(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    at = now + timedelta(seconds=1)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        placement = _io_placement(pair, at, quota_nodes=2)
        await _record(session, pair[1].target_id, at, placement)
        attempt_id, _ = await _io_native(session, owned, pair, now)
        original = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at)
    async with sessions() as session, session.begin():
        placement["nodes"][0]["uid"] = "replaced-node-uid"
        await _record(session, pair[1].target_id, at + timedelta(seconds=1), placement)
    with pytest.raises(ExecutionProvisioningBlockedError, match="node_identity_changed"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at + timedelta(seconds=2), revalidate_existing=True)
    async with sessions() as session:
        assert (await session.get(TaskImageMaterializationAttempt, attempt_id)).native_build == original


async def test_io_revalidation_cannot_create_a_missing_frozen_binding(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    at = now + timedelta(seconds=1)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        await _record(session, pair[1].target_id, at, _io_placement(pair, at))
        attempt_id, _ = await _io_native(session, owned, pair, now)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at)
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        damaged = dict(attempt.native_build)
        damaged.pop("io_binding")
        attempt.native_build = damaged
    with pytest.raises(ExecutionProvisioningBlockedError, match="binding_invalid"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at, revalidate_existing=True)


async def test_io_bootstrap_binding_remains_charged_in_shared_peer_quota_projection(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    at = now + timedelta(seconds=1)
    async with sessions() as session, session.begin():
        a, b = await _seed_ready_trial(session, now=now), await _seed_ready_trial(session, now=now)
        first = placement_fixture(target_id=a[1].target_id, nodes=0, used_nodes=0,
                                  quota_nodes=2, parent_id="shared-io-tenant")
        second = _io_placement(b, at, pressure=65.0, quota_nodes=2)
        for quota in second["quota_resources"].values():
            quota["parent_id"] = "shared-io-tenant"
        await _record(session, a[1].target_id, at, first)
        await _record(session, b[1].target_id, at, second)
        attempt_id, _ = await _io_native(session, owned, b, now)
        reserved = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=at)
        assert reserved["io_binding"]["mode"] == "bootstrap"
    with pytest.raises(ExecutionProvisioningBlockedError, match="provider_quota_nodes_exceeded"):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=a[0], target=a[1], now=at + timedelta(seconds=1))


@pytest.mark.parametrize("competitor", ["execution", "native"])
async def test_execution_and_native_builds_race_for_one_shared_node(native_setup, competitor):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        a, b = await _seed_ready_trial(session, now=now), await _seed_ready_trial(session, now=now)
        for pair in (a, b):
            await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement_fixture(
                target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1, parent_id="shared-tenant",
            ))
        first, _ = await _native(session, owned, a, now)
        second, _ = await _native(session, owned, b, now)

    async def reserve(attempt_id=None):
        try:
            async with sessions() as session, session.begin():
                if attempt_id is not None:
                    return await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
                return await _reserve(session, trial_id=b[0], target=b[1], now=now + timedelta(seconds=2))
        except ExecutionProvisioningBlockedError as error:
            return error.reason

    results = await asyncio.gather(reserve(first), reserve(second if competitor == "native" else None))
    assert sum(isinstance(result, (dict, ServiceExecutionLease)) for result in results) == 1
    assert "execution_capacity_provider_quota_nodes_exceeded" in results
    async with sessions() as session:
        saved = (await session.scalars(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.id.in_([first, second]),
        ))).all()
        assert sum("capacity_reserved_at" in row.native_build for row in saved) <= 1


async def test_observed_native_pod_is_not_charged_twice(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        placement = placement_fixture(target_id=pair[1].target_id, nodes=1, used_nodes=1, quota_nodes=1)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, materialization_id = await _native(session, owned, pair, now, cpu=63000)
        original = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        placement["nodes"][0]["requested"] = {"cpu_millis": 63000, "memory_mib": 1024, "storage_mib": 2048}
        placement["nodes"][0]["used_pod_slots"] = 1
        placement["nodes"][0]["managed_pods"] = [{
            "uid": "native-pod", "lease_id": f"task-image:{materialization_id}", "generation": 1,
            "requests": placement["nodes"][0]["requested"],
        }]
        await _record(session, pair[1].target_id, now + timedelta(seconds=3), placement)
        assert await reserve_native_task_image_capacity(
            session, attempt_id=attempt_id, now=now + timedelta(seconds=4), revalidate_existing=True,
        ) == original
        lease = await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=4))
        assert isinstance(lease, ServiceExecutionLease)


async def test_revalidating_observed_pending_build_does_not_count_another_pending_job(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        policy.max_pending_jobs = 1
        placement = placement_fixture(target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, materialization_id = await _native(session, owned, pair, now)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        placement["pending_pods"] = [{
            "uid": "pending-build-pod", "lease_id": f"task-image:{materialization_id}", "generation": 1,
            "requests": {"cpu_millis": 64000, "memory_mib": 1024, "storage_mib": 2048},
        }]
        observation, _ = await _record(session, pair[1].target_id, now + timedelta(seconds=3), placement)
        names = inspect.signature(create_execution_capacity_observation).parameters
        payload = {key: value for key, value in observation.observation_json.items() if key in names}
        payload.update(pending_jobs=1, observed_at=now + timedelta(seconds=4), source_version=str(uuid4()))
        await create_execution_capacity_observation(session, **payload)
        assert await reserve_native_task_image_capacity(
            session, attempt_id=attempt_id, now=now + timedelta(seconds=5), revalidate_existing=True,
        )


async def test_terminal_or_expired_native_attempt_holds_capacity_until_cleanup(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement_fixture(
            target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1,
        ))
        attempt_id, materialization_id = await _native(session, owned, pair, now)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "state": "released"}
        materialization = await session.get(TaskImageMaterialization, materialization_id)
        materialization.state = "failed"
        materialization.lease_expires_at = now - timedelta(seconds=1)
    with pytest.raises(ExecutionProvisioningBlockedError, match="quota_nodes_exceeded"):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=3))
    async with sessions() as session, session.begin():
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "capacity_released_at": (now + timedelta(seconds=4)).isoformat()}
    async with sessions() as session, session.begin():
        assert isinstance(await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=5)), ServiceExecutionLease)


@pytest.mark.parametrize("limit,reason", [
    ("max_pending_jobs", "pending_limit"), ("max_create_per_minute", "create_rate"),
])
async def test_execution_limits_include_native_reservations(native_setup, limit, reason):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt_id, _ = await _native(session, owned, pair, now, cpu=1000)
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        setattr(policy, limit, 1)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now)
    with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=1))


async def test_native_admission_accounts_execution_in_an_independent_cpu_but_shared_disk_domain(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        a, b = await _seed_ready_trial(session, now=now), await _seed_ready_trial(session, now=now)
        for index, pair in enumerate((a, b)):
            placement = placement_fixture(target_id=pair[1].target_id, nodes=0, used_nodes=0,
                                          quota_nodes=1, parent_id=f"cpu-{index}")
            placement["quota_resources"]["storage"]["parent_id"] = "shared-ssd"
            await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, _ = await _native(session, owned, b, now)
        await _reserve(session, trial_id=a[0], target=a[1], now=now + timedelta(seconds=2))
    with pytest.raises(ExecutionProvisioningBlockedError, match="quota_storage_exceeded"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))


@pytest.mark.parametrize("invalid", ["oversize", "stale_observation", "stale_lease"])
async def test_native_admission_preserves_existing_resource_and_freshness_boundaries(native_setup, invalid):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt_id, materialization_id = await _native(session, owned, pair, now, cpu=64001 if invalid == "oversize" else 1000)
        row = await session.get(TaskImageMaterialization, materialization_id)
        if invalid == "stale_lease":
            row.lease_expires_at = now - timedelta(seconds=1)
        elif invalid == "stale_observation":
            row.lease_expires_at = now + timedelta(days=2)
    reason = {"oversize": "node_shape", "stale_observation": "observation_stale", "stale_lease": "native_lease_stale"}[invalid]
    with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id,
                                                    now=now + timedelta(days=1) if invalid == "stale_observation" else now)


@pytest.mark.parametrize("uses_added_label", [False, True])
async def test_native_cold_admission_reuses_history_only_for_unused_added_labels(native_setup, uses_added_label):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        historical = placement_fixture(target_id=pair[1].target_id, nodes=1)
        historical["node_group"]["template"]["labels"] = {}
        historical["daemonsets"] = [{
            "uid": "resident", "generation": 1,
            "requests": {"cpu_millis": 100, "memory_mib": 100, "storage_mib": 0},
            "scheduling": {"node_selector": {
                "loom.nebius/node-os" if uses_added_label else "kubernetes.io/os": "linux",
            }},
        }]
        historical["template_samples"][0].update(
            daemonsets={"resident": 1}, daemonset_slots=1,
            daemonset_requests=historical["daemonsets"][0]["requests"],
        )
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), historical)
        current = deepcopy(historical)
        current["nodes"] = []
        current["template_samples"] = []
        current["node_group"]["node_count"] = 0
        current["node_group"]["template"]["labels"] = {
            "loom.nebius/node-os": "linux", "loom.nebius/node-arch": "amd64",
        }
        for quota in current["quota_resources"].values():
            quota["used"] = 0
        await _record(session, pair[1].target_id, now + timedelta(seconds=2), current)
        attempt_id, _ = await _native(session, owned, pair, now, cpu=1000)
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "resources": {
            "vcpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384,
        }}
    if uses_added_label:
        with pytest.raises(ExecutionProvisioningBlockedError, match="node_allocatable_unknown"):
            async with sessions() as session, session.begin():
                await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))
        async with sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            assert not attempt.native_build.get("capacity_reserved_at")
    else:
        async with sessions() as session, session.begin():
            decision = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))
            assert decision["capacity_reserved_at"]
            assert decision["resources"] == {"vcpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384}
        async with sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            assert attempt.native_build["capacity_reserved_at"] == decision["capacity_reserved_at"]
