from copy import deepcopy

import pytest

from loom_control_plane.execution_placement import (
    PlacementUnavailableError,
    cold_sample,
    plan_placement,
    quota_identity,
)
from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals
from tests.execution_placement_fixtures import placement_fixture


def _resources(cpu: int = 2_000, memory: int = 1_024, storage: int = 1_024):
    return ResourceTotals(cpu_millis=cpu, memory_mib=memory, storage_mib=storage)


@pytest.mark.parametrize("dimension,limit", [("nodes", 4), ("vcpu", 12000), ("memory", 6144), ("storage", 12288)])
def test_provider_quota_projection_charges_distinct_pool_shapes_and_native_floor(dimension, limit):
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = placement_fixture(target_id="first", parent_id="account", used_nodes=0,
                                node_cpu=2000, node_memory=1024, node_storage=2048)
    other = placement_fixture(target_id="second", parent_id="account", used_nodes=0,
                              node_cpu=4000, node_memory=2048, node_storage=4096)
    current["quota_resources"][dimension]["limit"] = limit
    other["quota_resources"][dimension]["limit"] = limit
    require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=1,
                                   peers=[(CapacityPlacement.model_validate(other), 1)])
    other["quota_resources"][dimension]["limit"] = limit - 1
    with pytest.raises(PlacementUnavailableError, match=f"provider_quota_{dimension}_exceeded"):
        require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=1,
                                       peers=[(CapacityPlacement.model_validate(other), 1)])


def test_provider_quota_projection_preserves_highest_external_usage_and_lowest_limit():
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = placement_fixture(target_id="first", parent_id="account", quota_nodes=20, used_nodes=1)
    other = placement_fixture(target_id="second", parent_id="account", quota_nodes=8, used_nodes=6)
    require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=1,
                                   peers=[(CapacityPlacement.model_validate(other), 1)])
    with pytest.raises(PlacementUnavailableError, match="provider_quota_nodes_exceeded"):
        require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=2,
                                       peers=[(CapacityPlacement.model_validate(other), 1)])


@pytest.mark.parametrize("identity_field", ["parent_id", "region", "service", "name", "unit"])
def test_provider_quota_projection_does_not_combine_unrelated_quota_identities(identity_field):
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = placement_fixture(target_id="first", parent_id="account", quota_nodes=2)
    other = placement_fixture(target_id="second", parent_id="account", quota_nodes=1)
    for quota in other["quota_resources"].values():
        quota[identity_field] = "foreign"
    require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=1,
                                   peers=[(CapacityPlacement.model_validate(other), 10)])


@pytest.mark.parametrize("overlap", ["group", "node_uid", "provider_id", "repeated_peer"])
def test_provider_quota_projection_rejects_duplicate_physical_inventory(overlap):
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = placement_fixture(target_id="first", parent_id="account")
    other = placement_fixture(target_id="second", parent_id="account")
    if overlap == "group":
        other["node_group"]["id"] = "first"
    elif overlap == "node_uid":
        other["nodes"][0]["uid"] = "first-node-0"
    elif overlap == "provider_id":
        other["nodes"][0]["provider_id"] = "test://first/0"
    peer = (CapacityPlacement.model_validate(other), 0)
    with pytest.raises(PlacementUnavailableError, match="overlapping_targets"):
        require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=0,
                                       peers=[peer, peer] if overlap == "repeated_peer" else [peer])


@pytest.mark.parametrize("count", [-1, True])
def test_provider_quota_projection_rejects_invalid_unobserved_node_charges(count):
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = CapacityPlacement.model_validate(placement_fixture(target_id="first"))
    with pytest.raises(ValueError, match="additional_nodes"):
        require_provider_quota_headroom(current, additional_nodes=count, peers=[])


def test_provider_quota_projection_keeps_stable_failure_order_independent_of_json_key_order():
    from loom_control_plane.execution_placement import require_provider_quota_headroom

    current = placement_fixture(target_id="first", quota_nodes=0, used_nodes=0)
    current["quota_resources"] = dict(reversed(list(current["quota_resources"].items())))
    with pytest.raises(PlacementUnavailableError, match="provider_quota_nodes_exceeded"):
        require_provider_quota_headroom(CapacityPlacement.model_validate(current), additional_nodes=1, peers=[])


def test_fragmented_nodes_cannot_combine_cpu_and_memory_holes():
    data = placement_fixture(target_id="a", nodes=2, node_cpu=4_000, node_memory=4_096)
    data["nodes"][0]["requested"].update(cpu_millis=3_500)
    data["nodes"][1]["requested"].update(memory_mib=3_500)
    placement = CapacityPlacement.model_validate(data)
    result = plan_placement(
        placement, [("new:1", _resources(memory=2_048))], sample=cold_sample(placement)
    )
    assert result.additional_nodes == 1


def test_actual_allocatable_daemonsets_and_slots_bound_cold_packing():
    data = placement_fixture(
        target_id="a",
        nodes=0,
        used_nodes=0,
        node_cpu=15_900,
        node_memory=63_688,
        node_storage=69_431,
        raw_storage=81_920,
    )
    sample = data["template_samples"][0]
    sample["daemonset_requests"].update(cpu_millis=220, memory_mib=408)
    sample["daemonset_slots"] = 5
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [(f"{i}:1", _resources()) for i in range(7)], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )
    assert (
        plan_placement(
            placement, [(f"{i}:1", _resources()) for i in range(8)], sample=cold_sample(placement)
        ).additional_nodes
        == 2
    )
    sample["pod_slots"] = 6
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement,
            [(f"{i}:1", _resources(100)) for i in range(2)],
            sample=cold_sample(placement),
        ).additional_nodes
        == 2
    )


def test_pod_identity_absorbs_authorization_once_and_stale_generation_does_not():
    data = placement_fixture(target_id="a", node_cpu=2_000, requested_cpu=2_000)
    data["nodes"][0]["managed_pods"] = [
        {
            "uid": "pod",
            "lease_id": "lease",
            "generation": 1,
            "requests": _resources().model_dump(),
        }
    ]
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("lease:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 0
    )
    assert (
        plan_placement(
            placement, [("lease:2", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )


def test_pending_pod_and_unobserved_ledger_are_one_demand():
    data = placement_fixture(target_id="a", nodes=0, node_cpu=2_000)
    data["pending_pods"] = [
        {"uid": "pod", "lease_id": "lease", "generation": 1, "requests": _resources().model_dump()}
    ]
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("lease:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )


def test_zero_nodes_can_reuse_matching_history_but_changed_template_cannot():
    data = placement_fixture(target_id="a")
    old = CapacityPlacement.model_validate(data)
    data["nodes"] = []
    data["node_group"]["node_count"] = 0
    data["template_samples"] = []
    current = CapacityPlacement.model_validate(data)
    assert cold_sample(current, [old]) is not None
    data["node_group"]["template"]["kubernetes_version"] = "1.34"
    current = CapacityPlacement.model_validate(data)
    assert cold_sample(current, [old]) is None
    with pytest.raises(PlacementUnavailableError, match="node_allocatable_unknown"):
        plan_placement(current, [("new:1", _resources())], sample=None)


def test_oversized_task_and_full_hot_pod_slots_need_real_cold_fit():
    data = placement_fixture(target_id="a", node_cpu=2_000)
    data["nodes"][0]["used_pod_slots"] = 64
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("new:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )
    with pytest.raises(PlacementUnavailableError, match="exceeds_node_allocatable"):
        plan_placement(placement, [("new:1", _resources(2_001))], sample=cold_sample(placement))


def test_provider_charge_is_distinct_from_allocatable_and_domain_is_native():
    data = placement_fixture(target_id="a", node_storage=65_536, raw_storage=81_920)
    placement = CapacityPlacement.model_validate(data)
    assert placement.node_group.raw_node.storage_mib == 81_920
    assert placement.nodes[0].allocatable.storage_mib == 65_536
    quota = placement.quota_resources["storage"]
    newer = quota.model_copy(update={"used": 0, "limit": 1_000_000})
    assert quota_identity(quota) == quota_identity(newer)
    assert quota_identity(quota) != quota_identity(quota.model_copy(update={"region": "other"}))


def test_daemonset_generation_only_still_reuses_historical_sample():
    data = placement_fixture(target_id="a")
    data["daemonsets"] = [
        {"uid": "ds", "generation": 1, "requests": _resources(100).model_dump(), "scheduling": {}}
    ]
    old = CapacityPlacement.model_validate(data)
    changed = deepcopy(data)
    changed["nodes"] = []
    changed["node_group"]["node_count"] = 0
    changed["template_samples"] = []
    changed["daemonsets"][0]["generation"] = 2
    sample = cold_sample(CapacityPlacement.model_validate(changed), [old])
    assert sample is not None
    assert (
        plan_placement(
            CapacityPlacement.model_validate(changed),
            [("new:1", _resources())],
            sample=sample,
        ).additional_nodes
        == 1
    )


@pytest.mark.parametrize("field", ["requests", "scheduling"])
def test_daemonset_request_or_scheduling_change_invalidates_historical_sample(field):
    data = placement_fixture(target_id="a")
    data["daemonsets"] = [
        {
            "uid": "ds",
            "generation": 1,
            "requests": _resources(100).model_dump(),
            "scheduling": {"node_selector": {"kubernetes.io/os": "linux"}},
        }
    ]
    old = CapacityPlacement.model_validate(data)
    changed = deepcopy(data)
    changed["nodes"] = []
    changed["node_group"]["node_count"] = 0
    changed["template_samples"] = []
    if field == "requests":
        changed["daemonsets"][0]["requests"]["cpu_millis"] += 50
    else:
        changed["daemonsets"][0]["scheduling"] = {}
    assert cold_sample(CapacityPlacement.model_validate(changed), [old]) is None


def test_registered_not_ready_node_is_already_charged_to_provider():
    data = placement_fixture(target_id="a")
    data["nodes"][0]["ready"] = False
    placement = CapacityPlacement.model_validate(data)
    result = plan_placement(placement, [("new:1", _resources())], sample=cold_sample(placement))
    assert result.cold_nodes == 1 and result.additional_nodes == 0


def test_duplicate_pending_pod_authority_is_visible_instead_of_silently_deduplicated():
    data = placement_fixture(target_id="a", nodes=0)
    data["pending_pods"] = [
        {"uid": uid, "lease_id": "lease", "generation": 1, "requests": _resources().model_dump()}
        for uid in ("pod-a", "pod-b")
    ]
    placement = CapacityPlacement.model_validate(data)
    with pytest.raises(PlacementUnavailableError, match="duplicate_pod_identity"):
        plan_placement(placement, [("lease:1", _resources())], sample=cold_sample(placement))


_ADDED_LABELS = {"loom.nebius/node-os": "linux", "loom.nebius/node-arch": "amd64"}


def _label_history(scheduling=None):
    data = placement_fixture(target_id="label-history")
    data["node_group"]["template"]["labels"] = {"existing": "kept"}
    data["daemonsets"] = [{
        "uid": "resident", "generation": 1,
        "requests": _resources(100, 100, 0).model_dump(),
        "scheduling": scheduling or {"node_selector": {"kubernetes.io/os": "linux"}},
    }]
    data["template_samples"][0].update(
        daemonsets={"resident": 1}, daemonset_slots=1,
        daemonset_requests=_resources(100, 100, 0).model_dump(),
    )
    old = CapacityPlacement.model_validate(data)
    data = deepcopy(data)
    data["nodes"] = []
    data["node_group"]["node_count"] = 0
    data["template_samples"] = []
    data["node_group"]["template"]["labels"].update(_ADDED_LABELS)
    return old, data


def test_cold_native_build_reuses_measured_sample_after_unused_label_additions():
    old, data = _label_history()
    current = CapacityPlacement.model_validate(data)
    sample = cold_sample(current, [old])
    assert sample is not None
    assert sample.allocatable == old.template_samples[0].allocatable
    assert sample.daemonset_requests == old.template_samples[0].daemonset_requests
    plan = plan_placement(current, [("task-image:build:2", _resources(1000, 2048, 16384))], sample=sample)
    assert plan.additional_nodes == 1


@pytest.mark.parametrize("scheduling", [
    {"node_selector": {"loom.nebius/node-os": "linux"}},
    *({"affinity": {"node_affinity": {"required_during_scheduling_ignored_during_execution": {
        "node_selector_terms": [{"match_expressions": [{"key": "loom.nebius/node-arch", "operator": operator, "values": []}]}],
    }}}} for operator in ("In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt")),
    {"affinity": {"pod_affinity": {"required_during_scheduling_ignored_during_execution": [
        {"topology_key": "loom.nebius/node-os"},
    ]}}},
    {"topology_spread_constraints": [{"topology_key": "loom.nebius/node-arch"}]},
    {"topology_spread_constraints": [{"match_label_keys": ["loom.nebius/node-os"]}]},
])
def test_added_labels_referenced_by_daemonset_scheduling_require_new_samples(scheduling):
    old, data = _label_history(scheduling)
    assert cold_sample(CapacityPlacement.model_validate(data), [old]) is None


@pytest.mark.parametrize("labels", [None, [], "invalid", {"existing": "kept", "new": None},
                                   {"existing": "changed"}, {}, {"new": "value"}])
def test_label_compatibility_rejects_malformed_changed_or_removed_labels(labels):
    old, data = _label_history()
    data["node_group"]["template"]["labels"] = labels
    assert cold_sample(CapacityPlacement.model_validate(data), [old]) is None
    malformed_old = old.model_copy(update={"node_group": old.node_group.model_copy(update={
        "template": {**old.node_group.template, "labels": labels},
    })})
    if not isinstance(labels, dict) or any(not isinstance(v, str) for v in labels.values()):
        _, valid = _label_history()
        assert cold_sample(CapacityPlacement.model_validate(valid), [malformed_old]) is None
        assert cold_sample(CapacityPlacement.model_validate(data), [malformed_old]) is None


@pytest.mark.parametrize("field,value", [
    ("preset", "other"), ("platform", "other"), ("os", "other"),
    ("kubernetes_version", "1.34"), ("max_pods", 128), ("boot_disk_type", "other"),
    ("boot_disk_mib", 2048), ("taints", [{"key": "dedicated", "effect": "NO_SCHEDULE"}]),
])
def test_label_additions_do_not_hide_material_template_changes(field, value):
    old, data = _label_history()
    data["node_group"]["template"][field] = value
    assert cold_sample(CapacityPlacement.model_validate(data), [old]) is None


@pytest.mark.parametrize("change", ["group", "raw_node", "daemon_generation", "daemon_requests", "daemon_scheduling"])
def test_label_additions_preserve_group_resource_and_daemonset_boundaries(change):
    old, data = _label_history()
    if change == "group":
        data["node_group"]["id"] = "different-group"
    elif change == "raw_node":
        data["node_group"]["raw_node"]["cpu_millis"] += 1000
    elif change == "daemon_generation":
        data["daemonsets"][0]["generation"] += 1
    elif change == "daemon_requests":
        data["daemonsets"][0]["requests"]["cpu_millis"] += 100
    else:
        data["daemonsets"][0]["scheduling"] = {}
    sample = cold_sample(CapacityPlacement.model_validate(data), [old])
    if change == "daemon_generation":
        assert sample is not None
    else:
        assert sample is None
