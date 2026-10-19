"""Optional operator build counts do not replace physical placement/quota guards."""
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom_control_plane.execution_placement import PlacementUnavailableError
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.pool_management.capacity import PoolCapacityPolicyV1, require_fit
from tests.execution_placement_fixtures import placement_fixture


def policy(limit):
    return PoolCapacityPolicyV1(observation_max_age_seconds=60, max_create_per_minute=100,
        max_pending_jobs=100, max_unschedulable_jobs=0, max_image_pull_backoff_jobs=0,
        build_concurrency_limit=limit)


def request(pool_id, *, phase="reserved"):
    return NebiusPoolRequest(request_id=uuid4(), pool_id=pool_id, workload_kind="task_image_build",
        phase=phase, cpu_millis=1000, memory_mib=2048, ephemeral_storage_mib=16384, pod_slots=1)


def capacity(limit, *, node_cpu=64000, quota_nodes=20, max_nodes=100):
    placement = CapacityPlacement.model_validate(placement_fixture(
        target_id="pool", node_cpu=node_cpu, quota_nodes=quota_nodes))
    placement = placement.model_copy(update={"node_group": placement.node_group.model_copy(update={"max_nodes": max_nodes})})
    return SimpleNamespace(policy=policy(limit), placement=placement, sample=placement.template_samples[0],
        provider=SimpleNamespace(provider_capacity_state="available", autoscaler_state="ready"),
        kubernetes=SimpleNamespace(pending_jobs=0, unschedulable_jobs=0, image_pull_backoff_jobs=0))


@pytest.mark.parametrize("limit", [None, 17, 64])
def test_resource_capacity_accepts_seventeenth_build_without_implicit_sixteen(limit):
    pool_id = uuid4()
    active = [request(pool_id, phase="cleanup_intent") for _ in range(16)]
    require_fit({pool_id: capacity(limit)}, candidate=request(pool_id), active=active,
        protected_waits=[], recent_grants={})
    serialized = policy(limit).model_dump(mode="json")
    assert PoolCapacityPolicyV1.model_validate(serialized).model_dump(mode="json") == serialized


def test_explicit_operator_limit_counts_terminal_cleanup_reservations():
    pool_id = uuid4()
    with pytest.raises(PlacementUnavailableError, match="pool_build_concurrency_exceeded"):
        require_fit({pool_id: capacity(16)}, candidate=request(pool_id),
            active=[request(pool_id, phase="cleanup_intent") for _ in range(16)],
            protected_waits=[], recent_grants={})


@pytest.mark.parametrize("quota_nodes,max_nodes,reason", [(1, 100, "quota"), (20, 1, "pool_max_nodes_exceeded")])
def test_resource_only_mode_still_rejects_actual_quota_or_node_exhaustion(quota_nodes, max_nodes, reason):
    pool_id = uuid4()
    with pytest.raises(PlacementUnavailableError, match=reason):
        require_fit({pool_id: capacity(None, node_cpu=16000, quota_nodes=quota_nodes, max_nodes=max_nodes)},
            candidate=request(pool_id), active=[request(pool_id) for _ in range(16)],
            protected_waits=[], recent_grants={})


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "17"])
def test_operator_limit_requires_a_positive_integer_or_null(value):
    with pytest.raises(ValueError):
        policy(value)
