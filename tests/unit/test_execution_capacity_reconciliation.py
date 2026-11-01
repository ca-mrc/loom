from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from loom_control_plane.execution_capacity_reconciliation import (
    ReleasedPod,
    reconcile_released_pods,
)
from loom_control_plane.service_execution import _equivalent_cleanup_receipt_replay
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture


def inventory():
    raw = placement_fixture(target_id="owner", requested_cpu=8000,
                            requested_memory=8192, requested_storage=65536)
    raw["nodes"][0].update(used_pod_slots=3, managed_pods=[{
        "uid": "pod-old", "lease_id": "lease", "generation": 1,
        "requests": {"cpu_millis": 8000, "memory_mib": 8192, "storage_mib": 65536},
    }])
    return CapacityPlacement.model_validate(raw)


@pytest.mark.parametrize("mismatch", ["uid", "generation", "lease", "future", "duplicate_uid", "duplicate_identity"])
def test_missing_or_ambiguous_exact_proof_keeps_occupancy(mismatch):
    at = datetime.now(UTC)
    placement = inventory()
    receipt = ReleasedPod("lease", 1, "pod-old", at)
    if mismatch in {"uid", "generation", "lease", "future"}:
        receipt = ReleasedPod("other" if mismatch == "lease" else "lease",
                              2 if mismatch == "generation" else 1,
                              "replacement" if mismatch == "uid" else "pod-old",
                              at + timedelta(seconds=1) if mismatch == "future" else at)
    else:
        original = placement.nodes[0].managed_pods[0]
        duplicate = original.model_copy(update=(
            {"lease_id": "different"} if mismatch == "duplicate_uid" else {"uid": "replacement"}
        ))
        placement = placement.model_copy(update={"pending_pods": [duplicate]})
    assert reconcile_released_pods(placement, [receipt], now=at) == placement


def test_confirmed_release_subtracts_only_observed_requests_once_without_mutating_observation():
    at = datetime.now(UTC)
    placement = inventory()
    receipt = ReleasedPod("lease", 1, "pod-old", at)
    corrected = reconcile_released_pods(placement, [receipt, receipt], now=at)
    node = corrected.nodes[0]
    assert (node.requested.cpu_millis, node.requested.memory_mib, node.requested.storage_mib) == (0, 0, 0)
    assert node.used_pod_slots == 2 and not node.managed_pods
    assert corrected.node_group == placement.node_group
    assert corrected.quota_resources == placement.quota_resources
    assert placement.nodes[0].requested.cpu_millis == 8000
    assert reconcile_released_pods(corrected, [receipt], now=at) == corrected


def test_invalid_observed_request_totals_are_not_clamped_into_fake_free_capacity():
    at = datetime.now(UTC)
    placement = inventory()
    node = placement.nodes[0]
    node = node.model_copy(update={"requested": node.requested.model_copy(update={"cpu_millis": 1})})
    placement = placement.model_copy(update={"nodes": [node]})
    assert reconcile_released_pods(placement, [ReleasedPod("lease", 1, "pod-old", at)], now=at) == placement


def test_pending_release_removes_only_exact_old_pod_and_retains_new_generation():
    at = datetime.now(UTC)
    placement = inventory()
    old = placement.nodes[0].managed_pods[0]
    new = old.model_copy(update={"uid": "pod-new", "generation": 2})
    placement = placement.model_copy(update={"nodes": [], "pending_pods": [old, new]})
    corrected = reconcile_released_pods(placement, [ReleasedPod("lease", 1, "pod-old", at)], now=at)
    assert corrected.pending_pods == [new]


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("change", ["timestamp", "earlier_probe", "uid", "generation", "outer_payload", "unknown_field", "invalid_time"])
def test_cleanup_replay_only_normalizes_proven_same_identity_and_keeps_legacy_conservative(legacy, change):
    at = datetime.now(UTC)
    lease = SimpleNamespace(pod_uid="pod", job_uid="job", resource_generation=1)
    receipt = {"pod_uid": "pod", "job_uid": "job", "resource_generation": 1,
               "pods_absent": True, "confirmed_at": at.isoformat()}
    old = {"normalized_state": "deleted", "job_uid": "job", "resource_generation": 1}
    if not legacy:
        old["cleanup_receipt"] = receipt
    new = {**old, "cleanup_receipt": {**receipt, "confirmed_at": (at + timedelta(seconds=1)).isoformat()}}
    if change == "earlier_probe":
        new["cleanup_receipt"]["confirmed_at"] = (at - timedelta(seconds=1)).isoformat()
    elif change == "uid":
        new["cleanup_receipt"]["pod_uid"] = "replacement"
    elif change == "generation":
        new["cleanup_receipt"]["resource_generation"] = 2
    elif change == "outer_payload":
        new["reason"] = "changed"
    elif change == "unknown_field":
        new["cleanup_receipt"]["unknown"] = True
    elif change == "invalid_time":
        new["cleanup_receipt"]["confirmed_at"] = "invalid"
    assert _equivalent_cleanup_receipt_replay(old, new, lease=lease) is (change in {"timestamp", "earlier_probe"})
    assert ("cleanup_receipt" in old) is not legacy


@pytest.mark.parametrize("change", [None, "uid", "generation", "outer_payload", "invalid_time"])
def test_legacy_actor_replays_new_cleanup_event_without_discarding_its_receipt(change):
    lease = SimpleNamespace(pod_uid="pod", job_uid="job", resource_generation=1)
    receipt = {"pod_uid": "pod", "job_uid": "job", "resource_generation": 1,
               "pods_absent": True, "confirmed_at": datetime.now(UTC).isoformat()}
    new = {"normalized_state": "deleted", "job_uid": "job", "resource_generation": 1}
    old = {**new, "cleanup_receipt": receipt}
    if change == "uid":
        receipt["pod_uid"] = "replacement"
    elif change == "generation":
        receipt["resource_generation"] = 2
    elif change == "outer_payload":
        new["reason"] = "changed"
    elif change == "invalid_time":
        receipt["confirmed_at"] = "invalid"
    assert _equivalent_cleanup_receipt_replay(old, new, lease=lease) is (change is None)
    assert old["cleanup_receipt"] == receipt
