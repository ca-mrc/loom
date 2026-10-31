"""A sampler transition is not another physical container execution."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from loom.models.resource_usage import (
    ResourceCounters,
    TrialResourceUsageReport,
    aggregate_resource_usage,
)

NOW = datetime.now(UTC)
TRIAL, LEASE, POD = uuid4(), uuid4(), str(uuid4())


def report(source, key, **changes):
    data = dict(trial_id=TRIAL, attempt_count=1, execution_lease_id=LEASE,
        resource_generation=1, target_id="target", pod_uid=POD,
        execution_key=key * 64, container_role="controller", role_name="execution",
        backend="nebius_kubernetes", source=source,
        container_started_at=NOW - timedelta(minutes=1), observation_seq=1,
        first_observed_at=NOW - timedelta(seconds=10), last_observed_at=NOW,
        finalized_at=NOW, completeness="partial",
        counters=ResourceCounters(cpu_usage_usec=3000, memory_sampled_max_bytes=200))
    data.update(changes)
    return TrialResourceUsageReport(**data)


def test_source_transition_does_not_add_the_same_cumulative_container_twice():
    old = report("kubelet_summary", "a", terminal_reason="observation_source_changed")
    new = report("native_node_observer", "b", counters=ResourceCounters(
        cpu_usage_usec=4000, memory_sampled_max_bytes=100))
    result = aggregate_resource_usage([old, new])
    assert result["cpu_usage_usec"] == 4000
    assert result["memory_sampled_max_sum_bytes"] == 200
    assert result["records"] == 2  # Preserve the original provenance records.


def test_real_restart_and_lease_boundaries_are_still_added():
    old = report("kubelet_summary", "a")
    restart = report("native_node_observer", "b", container_started_at=NOW - timedelta(seconds=20))
    independent = report("native_node_observer", "c", execution_lease_id=uuid4())
    assert aggregate_resource_usage([old, restart, independent])["cpu_usage_usec"] == 9000


def test_pod_storage_source_transition_is_not_a_second_pod():
    old = report("kubelet_summary", "a", container_role="pod", role_name="pod",
        container_started_at=None, counters=ResourceCounters(ephemeral_storage_sampled_max_bytes=50))
    new = report("native_node_observer", "b", container_role="pod", role_name="pod",
        container_started_at=None, counters=ResourceCounters(ephemeral_storage_sampled_max_bytes=100))
    assert aggregate_resource_usage([old, new])["pod_ephemeral_storage_sampled_max_sum_bytes"] == 100


def test_unknown_incarnations_are_not_assumed_to_be_the_same_container():
    old = report("kubelet_summary", "a", container_started_at=None)
    new = report("native_node_observer", "b", container_started_at=None)
    assert aggregate_resource_usage([old, new])["cpu_usage_usec"] == 6000
