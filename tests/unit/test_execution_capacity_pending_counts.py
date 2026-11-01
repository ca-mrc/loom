"""Per-Pod counter classification and historical observation wire compatibility."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity_reconciliation import (
    ReleasedPod,
    reconcile_released_pods,
    released_pending_counts,
)
from loom_execution_capacity_collector import kubernetes
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    ManagedPodPlacement,
    PodPendingCounts,
)
from tests.execution_placement_fixtures import placement_fixture
from tests.unit.test_execution_capacity_collector import _capture_nodes, _node, _pod


def legacy_pod():
    return {"uid": "old-pod", "lease_id": "lease", "generation": 1,
            "requests": {"cpu_millis": 1000, "memory_mib": 1024, "storage_mib": 2048}}


def test_default_classification_preserves_historical_canonical_payload_and_digest():
    old = legacy_pod()
    parsed = ManagedPodPlacement.model_validate(old)
    assert parsed.pending_counts is None
    assert parsed.model_dump() == old
    assert json.loads(parsed.model_dump_json()) == old
    assert canonical_digest(parsed.model_dump(mode="json")) == canonical_digest(old)
    assert ManagedPodPlacement.model_validate_json(parsed.model_dump_json()).model_dump() == old
    # Mixed old/new Pods retain exactly the metadata they originally carried.
    new = {**old, "uid": "new-pod", "generation": 2,
           "pending_counts": {"pending": True, "unschedulable": True, "image_pull_backoff": False}}
    raw = placement_fixture(target_id="wire")
    raw["pending_pods"] = [old, new]
    placement = CapacityPlacement.model_validate(raw)
    wire = placement.model_dump(mode="json")
    assert wire["pending_pods"] == [old, new]
    assert CapacityPlacement.model_validate(wire).model_dump(mode="json") == wire
    assert canonical_digest(CapacityPlacement.model_validate_json(placement.model_dump_json()).model_dump(mode="json")) == canonical_digest(wire)


@pytest.mark.parametrize("value", [1, 0, "true", None])
def test_classification_requires_strict_booleans(value):
    with pytest.raises(ValidationError):
        PodPendingCounts(pending=value, unschedulable=False, image_pull_backoff=False)


@pytest.mark.parametrize("subcategory", ["unschedulable", "image_pull_backoff"])
def test_subcategories_require_a_pending_pod(subcategory):
    values = {"pending": False, "unschedulable": False, "image_pull_backoff": False}
    values[subcategory] = True
    with pytest.raises(ValidationError, match="require a pending Pod"):
        PodPendingCounts(**values)


@pytest.mark.parametrize("bound,classification", [
    (bound, classification) for bound in (False, True)
    for classification in ("running", "pending", "unschedulable", "image_pull_backoff", "both")
    if bound or classification != "running"
])
async def test_collector_records_same_classification_as_aggregates_once_for_bound_and_unbound_pods(
    monkeypatch, bound, classification,
):
    # Running Pods necessarily have a node. Pending classification can occur
    # while bound (init/image pull) or still awaiting a scheduling assignment.
    pod = _pod(name="managed", namespace="loom-nebius-staging", target=True,
               node_name="node-1" if bound else None, pending=classification != "running")
    unschedulable = classification in {"unschedulable", "both"}
    image_pull = classification in {"image_pull_backoff", "both"}
    if not unschedulable:
        pod.status.conditions = []
    if image_pull:
        pod.status.container_statuses = [{"name": "container", "image": "test", "imageID": "test-id",
            "ready": False, "restartCount": 0, "state": {"waiting": {"reason": "ImagePullBackOff"}}}]
    foreign = _pod(name="foreign", namespace="other", target=False, node_name="node-1", pending=True)
    seen = []
    classify = kubernetes._pending_state

    def counted(value):
        seen.append(value.metadata.uid)
        return classify(value)

    monkeypatch.setattr(kubernetes, "_pending_state", counted)
    snapshot = await _capture_nodes([_node("node-1")], [pod, foreign])
    placement = snapshot.nodes[0].managed_pods[0] if bound else snapshot.pending_pods[0]
    counts = placement.pending_counts
    assert counts is not None
    assert (counts.pending, counts.unschedulable, counts.image_pull_backoff) == (
        classification != "running", unschedulable, image_pull)
    assert (snapshot.pending_jobs, snapshot.unschedulable_jobs, snapshot.image_pull_backoff_jobs) == (
        int(counts.pending), int(counts.unschedulable), int(counts.image_pull_backoff))
    assert sorted(seen) == ["foreign-uid", "managed-uid"]


@pytest.mark.parametrize("bound", [False, True])
def test_released_counter_delta_uses_only_exact_removed_classified_pods(bound):
    at = datetime.now(UTC)
    old = {**legacy_pod(), "pending_counts": {
        "pending": True, "unschedulable": True, "image_pull_backoff": True}}
    survivor = {**legacy_pod(), "uid": "survivor", "lease_id": "other"}
    raw = placement_fixture(target_id="owner", requested_cpu=2000, requested_memory=2048, requested_storage=4096)
    if bound:
        raw["nodes"][0].update(used_pod_slots=2, managed_pods=[old, survivor])
    else:
        raw["pending_pods"] = [old, survivor]
    original = CapacityPlacement.model_validate(raw)
    corrected = reconcile_released_pods(original, [ReleasedPod("lease", 1, "old-pod", at)], now=at)
    assert released_pending_counts(original, corrected) == {
        "pending_jobs": 1, "unschedulable_jobs": 1, "image_pull_backoff_jobs": 1}
    assert released_pending_counts(original, original) == {
        "pending_jobs": 0, "unschedulable_jobs": 0, "image_pull_backoff_jobs": 0}
    old.pop("pending_counts")
    legacy = CapacityPlacement.model_validate(raw)
    assert released_pending_counts(legacy, reconcile_released_pods(
        legacy, [ReleasedPod("lease", 1, "old-pod", at)], now=at)) == {
        "pending_jobs": 0, "unschedulable_jobs": 0, "image_pull_backoff_jobs": 0}
