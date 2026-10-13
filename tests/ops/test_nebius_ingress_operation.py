"""Live installation preflight counts full inventory, never sanitized snapshots."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_operation")


@pytest.fixture
def inventory():
    node = {"metadata": {"name": "computeinstance-test", "uid": str(uuid4()), "labels": {
        "loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}},
        "spec": {"providerID": "nebius://computeinstance-test", "taints": [
            {"key": "loom.nebius/platform", "value": "integration", "effect": "NoSchedule"}]},
        "status": {"conditions": [{"type": "Ready", "status": "True"}],
                   "allocatable": {"cpu": "1000m", "memory": "1Gi", "ephemeral-storage": "2Gi", "pods": "10"}}}
    pod = {"metadata": {"name": "foreign", "namespace": "foreign", "uid": str(uuid4())},
           "spec": {"nodeName": "computeinstance-test", "containers": [{"name": "app", "resources": {
               "requests": {"cpu": "800m", "memory": "768Mi", "ephemeral-storage": "1920Mi"}}}]},
           "status": {"phase": "Running"}}
    return {"nodes": [node], "pods": [pod]}


def test_exact_two_pod_ingress_envelope_fits_after_foreign_load(inventory):
    result = module().qualify_capacity(**inventory)
    assert result == {"node_uid": inventory["nodes"][0]["metadata"]["uid"], "reserved_pods": 2,
                      "cpu_millis": 200, "memory_mib": 256, "storage_mib": 128}


@pytest.mark.parametrize("observed", [{}, {"requests": {"memory": "768Mi"}}])
def test_partial_kubelet_summary_keeps_declared_ingress_load(inventory, observed):
    # Live kubelets can omit a zero/rounded CPU value from status.resources.
    # The shared raw decoder must still charge the full declared requests.
    inventory["pods"][0]["status"]["resources"] = observed
    assert module().qualify_capacity(**inventory)["reserved_pods"] == 2
    inventory["nodes"][0]["status"]["allocatable"]["cpu"] = "999m"
    with pytest.raises(module().OperationError, match="cannot fit the ingress envelope"):
        module().qualify_capacity(**inventory)


@pytest.mark.parametrize("field", ["allocatedResources", "resources"])
@pytest.mark.parametrize("excess", [False, True])
def test_pod_level_observed_allocation_is_compared_with_container_requests(inventory, field, excess):
    pod = inventory["pods"][0]
    requests = {"cpu": "801m" if excess else "800m", "memory": "768Mi", "ephemeral-storage": "1920Mi"}
    pod["status"][field] = requests if field == "allocatedResources" else {"requests": requests}
    if excess:
        with pytest.raises(module().OperationError):
            module().qualify_capacity(**inventory)
    else:
        assert module().qualify_capacity(**inventory)["reserved_pods"] == 2


@pytest.mark.parametrize("resource,value", [("cpu", "801m"), ("memory", "769Mi"), ("ephemeral-storage", "1921Mi")])
def test_any_resource_exhaustion_blocks_without_borrowing_legacy_node(inventory, resource, value):
    legacy = copy.deepcopy(inventory["nodes"][0])
    legacy["metadata"].update(name="legacy", uid=str(uuid4()), labels={"loom.nebius/node-role": "system"})
    legacy["status"]["allocatable"] = {"cpu": "100", "memory": "1Ti", "ephemeral-storage": "1Ti", "pods": "1000"}
    inventory["nodes"].append(legacy)
    inventory["pods"][0]["spec"]["containers"][0]["resources"]["requests"][resource] = value
    with pytest.raises(module().OperationError):
        module().qualify_capacity(**inventory)


@pytest.mark.parametrize("change", ["cordon", "not-ready", "deleting", "draining", "taint", "pressure", "missing-capacity", "pod-slots"])
def test_ineligible_or_incomplete_node_cannot_authorize_staging(inventory, change):
    node = inventory["nodes"][0]
    if change == "cordon":
        node["spec"]["unschedulable"] = True
    elif change == "not-ready":
        node["status"]["conditions"][0]["status"] = "False"
    elif change == "deleting":
        node["metadata"]["deletionTimestamp"] = "2026-09-23T22:00:00Z"
    elif change in {"taint", "draining"}:
        node["spec"]["taints"].append({"key": "ToBeDeletedByClusterAutoscaler" if change == "draining" else "foreign",
                                        "effect": "PreferNoSchedule" if change == "draining" else "NoExecute"})
    elif change == "pressure":
        node["status"]["conditions"].append({"type": "DiskPressure", "status": "True"})
    elif change == "missing-capacity":
        del node["status"]["allocatable"]["ephemeral-storage"]
    else:
        node["status"]["allocatable"]["pods"] = "2"
    with pytest.raises(module().OperationError):
        module().qualify_capacity(**inventory)


@pytest.mark.parametrize("change", ["init", "sidecar", "overhead", "pod-level", "resize", "allocated", "status-requests", "terminating"])
def test_full_foreign_pod_accounting_is_conservative(inventory, change):
    pod = inventory["pods"][0]
    extra = {"name": "setup", "resources": {"requests": {"cpu": "801m"}}}
    if change in {"init", "sidecar"}:
        if change == "sidecar":
            extra["restartPolicy"] = "Always"
        pod["spec"]["initContainers"] = [extra]
    elif change == "overhead":
        pod["spec"]["overhead"] = {"cpu": "1m"}
    elif change == "pod-level":
        pod["spec"]["resources"] = {"requests": {"cpu": "801m"}}
    elif change == "resize":
        pod["status"]["conditions"] = [{"type": "PodResizeInProgress", "status": "True"}]
    elif change in {"allocated", "status-requests"}:
        state = {"allocatedResources": {"cpu": "801m"}} if change == "allocated" else {"resources": {"requests": {"cpu": "801m"}}}
        pod["status"]["containerStatuses"] = [{"name": "app", **state}]
    else:
        pod["metadata"]["deletionTimestamp"] = "2026-09-23T22:00:00Z"
        pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "801m"
    with pytest.raises(module().OperationError):
        module().qualify_capacity(**inventory)


@pytest.mark.parametrize("tolerates", [True, False])
def test_unscheduled_pod_is_charged_only_if_it_can_compete_for_system_node(inventory, tolerates):
    pod = copy.deepcopy(inventory["pods"][0])
    pod["metadata"].update(name="pending", uid=str(uuid4()))
    pod["spec"].pop("nodeName")
    if tolerates:
        pod["spec"]["tolerations"] = [{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}]
    pod["status"]["phase"] = "Pending"
    inventory["pods"].append(pod)
    if tolerates:
        with pytest.raises(module().OperationError):
            module().qualify_capacity(**inventory)
    else:
        assert module().qualify_capacity(**inventory)["reserved_pods"] == 2


def test_completed_foreign_pods_do_not_consume_live_capacity(inventory):
    inventory["pods"][0]["status"]["phase"] = "Succeeded"
    inventory["pods"][0]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "100"
    assert module().qualify_capacity(**inventory)["reserved_pods"] == 2


def test_duplicate_inventory_never_becomes_capacity_authority(inventory):
    inventory["pods"].append(copy.deepcopy(inventory["pods"][0]))
    with pytest.raises(module().OperationError):
        module().qualify_capacity(**inventory)
