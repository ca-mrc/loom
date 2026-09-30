"""Platform fit includes inactive maintenance and rollout, not only today's Pods."""
from __future__ import annotations

import copy
from uuid import uuid4

import pytest
from tests.ops.test_nebius_ingress_operation import inventory as inventory

from loom.nebius_environment_render import PlatformEnvelope


def workload(kind, name, cpu="100m", memory="128Mi", scratch="1Gi"):
    template = {"metadata": {"labels": {"app": name}}, "spec": {
        "nodeSelector": {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"},
        "tolerations": [{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}],
        "containers": [{"name": "main", "image": "test@sha256:" + "a" * 64, "resources": {
            "requests": {"cpu": cpu, "memory": memory, "ephemeral-storage": scratch}}}]}}
    spec = {"template": template}
    if kind == "Deployment":
        spec.update(replicas=1, strategy={"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1}})
    elif kind == "CronJob":
        spec = {"jobTemplate": {"spec": spec}, "concurrencyPolicy": "Forbid", "schedule": "0 2 * * *"}
    return {"apiVersion": "batch/v1" if kind in {"Job", "CronJob"} else "apps/v1", "kind": kind,
            "metadata": {"name": name, "namespace": "loom-platform", "uid": str(uuid4())}, "spec": spec}


@pytest.fixture
def platform(inventory):
    inventory["nodes"][0]["status"]["allocatable"] = {"cpu": "3", "memory": "8Gi", "ephemeral-storage": "20Gi", "pods": "20"}
    inventory["pods"] = []
    inventory["controllers"] = [workload("Deployment", "api"), workload("CronJob", "backup", scratch="10Gi")]
    return inventory


def qualify(platform, planned=()):
    from scripts.ops.nebius_management_capacity import qualify_platform_capacity

    return qualify_platform_capacity(**platform, planned=list(planned),
        reserve=PlatformEnvelope(500, 1024, 10 * 1024, 4 * 1024), reserve_pods=4)


def test_fit_reserves_unstarted_backup_and_deployment_surge(platform):
    result = qualify(platform)
    assert result["required"] == {"cpu_millis": 800, "memory_mib": 1408, "ephemeral_storage_mib": 16384, "pods": 7}
    assert result["node_uid"] == platform["nodes"][0]["metadata"]["uid"]


def test_running_owned_pod_is_not_charged_twice_with_its_controller(platform):
    dep = platform["controllers"][0]
    rs = workload("ReplicaSet", "api-hash")
    rs["spec"]["replicas"] = 1
    rs["metadata"]["ownerReferences"] = [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "api", "uid": dep["metadata"]["uid"], "controller": True}]
    platform["controllers"].append(rs)
    pod = copy.deepcopy(dep["spec"]["template"])
    pod.update(apiVersion="v1", kind="Pod", status={"phase": "Running"})
    pod["metadata"].update(name="api-hash-0", namespace="loom-platform", uid=str(uuid4()), ownerReferences=[{
        "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "api-hash", "uid": rs["metadata"]["uid"], "controller": True}])
    pod["spec"]["nodeName"] = "computeinstance-test"
    platform["pods"].append(pod)
    assert qualify(platform)["required"]["cpu_millis"] == 800
    assert qualify(platform)["required"]["pods"] == 7


def test_pending_foreign_pod_and_terminating_pod_both_consume_capacity(platform):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    for index in range(2):
        pod = copy.deepcopy(platform["controllers"][0]["spec"]["template"])
        pod.update(apiVersion="v1", kind="Pod", status={"phase": "Pending"})
        pod["metadata"].update(name="foreign-" + str(index), namespace="foreign", uid=str(uuid4()))
        pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "1500m"
        if index:
            pod["metadata"]["deletionTimestamp"] = "2026-09-24T00:00:00Z"
            pod["spec"]["nodeName"] = "computeinstance-test"
        platform["pods"].append(pod)
    with pytest.raises(ManagementCapacityError):
        qualify(platform)


@pytest.mark.parametrize("resource,value", [("cpu", "799m"), ("memory", "1407Mi"), ("ephemeral-storage", "16383Mi"), ("pods", "6")])
def test_any_shortfall_rejects_without_borrowing_foreign_system_node(platform, resource, value):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    foreign = copy.deepcopy(platform["nodes"][0])
    foreign["metadata"].update(name="computeinstance-foreign", uid=str(uuid4()), labels={"loom.nebius/node-role": "system"})
    foreign["spec"]["providerID"] = "nebius://computeinstance-foreign"
    platform["nodes"].append(foreign)
    platform["nodes"][0]["status"]["allocatable"][resource] = value
    with pytest.raises(ManagementCapacityError):
        qualify(platform)


def test_planned_management_controllers_add_only_missing_headroom_on_replay(platform):
    planned = workload("Deployment", "manager", cpu="200m", scratch="256Mi")
    planned["metadata"].pop("uid")
    initial = qualify(platform, [planned])
    assert initial["required"]["cpu_millis"] == 1200
    actual = copy.deepcopy(planned)
    actual["metadata"]["uid"] = str(uuid4())
    platform["controllers"].append(actual)
    assert qualify(platform, [planned]) == initial


@pytest.mark.parametrize("change", ["pressure", "cordon", "unbounded_cron", "duplicate_uid", "unknown_kind", "planned_elsewhere"])
def test_unsafe_or_incomplete_inventory_never_qualifies(platform, change):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    planned = []
    if change == "pressure":
        platform["nodes"][0]["status"]["conditions"].append({"type": "DiskPressure", "status": "True"})
    elif change == "cordon":
        platform["nodes"][0]["spec"]["unschedulable"] = True
    elif change == "unbounded_cron":
        platform["controllers"][1]["spec"]["concurrencyPolicy"] = "Allow"
    elif change == "duplicate_uid":
        platform["controllers"].append(copy.deepcopy(platform["controllers"][0]))
    elif change == "unknown_kind":
        platform["controllers"][0]["kind"] = "MysteryController"
    else:
        planned = [workload("Deployment", "manager")]
        planned[0]["spec"]["template"]["spec"]["nodeSelector"] = {"foreign": "true"}
    with pytest.raises(ManagementCapacityError):
        qualify(platform, planned)


def test_heavier_old_replicaset_is_reserved_during_rollout(platform):
    dep = platform["controllers"][0]
    rs = workload("ReplicaSet", "api-old", cpu="800m")
    rs["spec"]["replicas"] = 1
    rs["metadata"]["ownerReferences"] = [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "api", "uid": dep["metadata"]["uid"], "controller": True}]
    platform["controllers"].append(rs)
    assert qualify(platform)["required"]["cpu_millis"] == 2200


@pytest.mark.parametrize("surge", [1, "10%", 5, "100%"])
def test_daemonset_surge_reserves_one_extra_pod_per_matching_node(platform, surge):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    ds = workload("DaemonSet", "agent", cpu="1000m")
    ds["spec"]["updateStrategy"] = {"type": "RollingUpdate", "rollingUpdate": {
        "maxUnavailable": 0, "maxSurge": surge}}
    platform["controllers"] = [ds]
    # The 500m child reserve plus two 1000m daemon Pods fits exactly.
    assert qualify(platform)["required"]["cpu_millis"] == 2500
    assert qualify(platform)["required"]["pods"] == 6
    platform["nodes"][0]["status"]["allocatable"]["cpu"] = "2499m"
    with pytest.raises(ManagementCapacityError):
        qualify(platform)


@pytest.mark.parametrize("strategy", [{}, {"type": "OnDelete"}, {
    "type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0}}, {
    "type": "RollingUpdate", "rollingUpdate": {"maxSurge": "0%"}}])
def test_daemonset_without_surge_needs_one_pod_per_node(platform, strategy):
    ds = workload("DaemonSet", "agent", cpu="1000m")
    ds["spec"]["updateStrategy"] = strategy
    platform["controllers"] = [ds]
    assert qualify(platform)["required"]["cpu_millis"] == 1500


@pytest.mark.parametrize("strategy", [{"type": "Mystery"}, {
    "type": "RollingUpdate", "rollingUpdate": {"maxSurge": -1}}])
def test_unknown_daemonset_strategy_never_qualifies(platform, strategy):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    ds = workload("DaemonSet", "agent")
    ds["spec"]["updateStrategy"] = strategy
    platform["controllers"] = [ds]
    with pytest.raises(ManagementCapacityError):
        qualify(platform)


@pytest.mark.parametrize('resource,value', [('cpu', '799m'), ('memory', '1407Mi'),
    ('ephemeral-storage', '16383Mi'), ('pods', '6')])
def test_capacity_failure_retains_numeric_fit_without_workload_details(platform, resource, value):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError, qualify_platform_capacity

    platform['nodes'][0]['status']['allocatable'][resource] = value
    diagnostic = {}
    with pytest.raises(ManagementCapacityError):
        qualify_platform_capacity(**platform, planned=[], reserve=PlatformEnvelope(500, 1024, 0, 4096),
            reserve_pods=4, diagnostic=diagnostic)
    assert diagnostic['stage'] == 'capacity'
    node, = diagnostic['nodes']
    assert node['required'] == {'cpu_millis': 800, 'memory_mib': 1408,
        'ephemeral_storage_mib': 16384, 'pods': 7}
    assert node['placement_matches'] is True
    assert set(diagnostic) == {'schema', 'stage', 'kind', 'error_type', 'nodes'}
    assert 'containers' not in str(diagnostic) and 'backup' not in str(diagnostic)


def test_inventory_decode_and_placement_have_distinct_closed_diagnostics(platform):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError, qualify_platform_capacity

    def fail():
        diagnostic = {}
        with pytest.raises(ManagementCapacityError):
            qualify_platform_capacity(**platform, planned=[], reserve=PlatformEnvelope(0, 0, 0, 0),
                reserve_pods=0, diagnostic=diagnostic)
        return diagnostic

    platform['controllers'][1]['spec']['concurrencyPolicy'] = 'Allow'
    report = fail()
    assert report['stage'] == 'controller_count' and report['kind'] == 'CronJob'
    assert report['error_type'] == 'ValueError' and report['nodes'] == []
    platform['controllers'][1]['spec']['concurrencyPolicy'] = 'Forbid'
    platform['nodes'][0]['metadata']['labels']['loom.nebius/platform'] = 'foreign'
    report = fail()
    assert report['stage'] == 'node_eligibility' and report['nodes'] == []


def test_diagnostic_collection_does_not_change_successful_fit(platform):
    from scripts.ops.nebius_management_capacity import qualify_platform_capacity

    diagnostic = {}
    result = qualify_platform_capacity(**platform, planned=[], reserve=PlatformEnvelope(500, 1024, 0, 4096),
        reserve_pods=4, diagnostic=diagnostic)
    assert result == qualify(platform)
    assert diagnostic['stage'] == 'complete'
    assert diagnostic['error_type'] is None
