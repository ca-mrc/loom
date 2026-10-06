"""Pool accounting must not trust child labels or sum per-owner capacity."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import pytest
from kubernetes import client as k8s
from pydantic import ValidationError
from urllib3.response import HTTPResponse

from loom_control_plane.execution_placement import PlacementUnavailableError, plan_placement
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeGroupPlacement,
    ResourceTotals,
)
from loom_execution_capacity_collector.kubernetes import (
    InClusterKubernetesCapacityReader,
    KubernetesObservationError,
)


def _scope():
    # Import inside the helper: RED names the missing pool interface, without
    # preventing the independent legacy collector regression lane from loading.
    from loom_execution_capacity_collector.pool import PoolObservationScope

    return PoolObservationScope.model_validate({
        "node_selector": {"loom.nebius/role": "execution"},
        "environments": [
            {"environment_id": UUID(int=n), "incarnation": UUID(int=n + 10),
             "execution_namespace": f"run-{n}", "build_namespace": f"run-{n}-build",
             "target_ids": [f"target-{n}"]}
            for n in (1, 2)
        ],
        "jobs": [
            {"reservation_id": UUID(int=n + 20), "environment_id": UUID(int=n),
             "incarnation": UUID(int=n + 10), "namespace": f"run-{n}",
             "job_name": f"job-{n}", "job_uid": f"job-uid-{n}",
             "target_id": f"target-{n}",
             "workload_kind": "trial", "lease_id": "same-local-claim", "generation": 1}
            for n in (1, 2)
        ],
    })


def _node(name="node-1"):
    return k8s.V1Node(
        metadata=k8s.V1ObjectMeta(name=name, uid=f"uid-{name}", labels={"loom.nebius/role": "execution"}),
        spec=k8s.V1NodeSpec(provider_id=f"nebius://{name}"),
        status=k8s.V1NodeStatus(
            capacity={"cpu": "4", "memory": "8Gi", "ephemeral-storage": "100Gi"},
            allocatable={"cpu": "3500m", "memory": "7Gi", "ephemeral-storage": "90Gi", "pods": "64"},
            conditions=[k8s.V1NodeCondition(type="Ready", status="True")],
        ),
    )


def _pod(owner=1, *, name=None, pending=False):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name or f"pod-{owner}", uid=f"uid-{name or owner}", namespace=f"run-{owner}",
            labels={"app.kubernetes.io/managed-by": "loom-execution-actuator",
                    "loom.openai.com/lease-id": "same-local-claim", "loom.openai.com/generation": "1"},
            annotations={"loom.openai.com/target-id": f"target-{owner}",
                         "loom.openai.com/execution-role": "attempt"},
            owner_references=[k8s.V1OwnerReference(
                api_version="batch/v1", kind="Job", name=f"job-{owner}", uid=f"job-uid-{owner}", controller=True,
            )],
        ),
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="work", resources=k8s.V1ResourceRequirements(
                requests={"cpu": "1", "memory": "1Gi", "ephemeral-storage": "2Gi"},
            ))],
            node_name=None if pending else "node-1",
        ),
        status=k8s.V1PodStatus(phase="Pending" if pending else "Running"),
    )


async def _capture(nodes, pods, scope=None, *, raw_pods=None):
    calls = []

    def listing(kind, items, **kwargs):
        calls.append(kind)
        if kind == "nodes":
            assert kwargs["label_selector"] == "loom.nebius/role=execution"
        if kind == "pods" and kwargs.get("_preload_content") is False:
            with k8s.ApiClient() as api:
                data = raw_pods if raw_pods is not None else api.sanitize_for_serialization(items)
            return SimpleNamespace(data=json.dumps({
                "apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "pods-1"},
                "items": data,
            }).encode())
        return SimpleNamespace(items=items, metadata=SimpleNamespace(resource_version=kind + "-1"))

    reader = InClusterKubernetesCapacityReader(
        core_api=SimpleNamespace(
            list_node=lambda **kw: listing("nodes", nodes, **kw),
            list_pod_for_all_namespaces=lambda **kw: listing("pods", pods, **kw),
        ),
        apps_api=SimpleNamespace(list_daemon_set_for_all_namespaces=lambda **kw: listing("daemons", [], **kw)),
    )
    snapshot = await reader.capture_pool(scope=scope or _scope())
    assert calls == ["nodes", "pods", "daemons"]
    return snapshot


def _placement(snapshot):
    return CapacityPlacement(
        node_group=NodeGroupPlacement(id="pool", max_nodes=1, node_count=1, template={},
                                     raw_node=ResourceTotals(cpu_millis=4000, memory_mib=8192, storage_mib=102400)),
        nodes=snapshot.nodes, pending_pods=snapshot.pending_pods, quota_resources={},
        daemonsets=snapshot.daemonsets, template_samples=snapshot.template_samples,
    )


@pytest.mark.asyncio
async def test_two_environments_observe_one_pool_and_distinct_global_grants():
    snapshot = await _capture([_node()], [_pod(1), _pod(2)])
    assert snapshot.active_nodes == 1
    assert snapshot.allocatable.cpu_millis == 3500
    assert snapshot.requested.cpu_millis == 2000
    managed = snapshot.nodes[0].managed_pods
    assert {p.lease_id for p in managed} == {
        "reservation:00000000-0000-0000-0000-000000000015",
        "reservation:00000000-0000-0000-0000-000000000016",
    }
    # The physical node has 1500m left. Discounting the two proven observed
    # grants permits exactly one additional 1000m grant; summing child nodes or
    # recharging observed grants breaks this hand-derived packing result.
    demand = ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048)
    result = plan_placement(_placement(snapshot), [
        (p.lease_id + ":1", demand) for p in managed
    ] + [("unobserved:1", demand)], sample=None)
    assert result.additional_nodes == 0
    assert len(result.observed_lease_ids) == 2
    with pytest.raises(PlacementUnavailableError):
        plan_placement(_placement(snapshot), [("new-a:1", demand), ("new-b:1", demand)], sample=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["uid", "namespace", "name", "controller", "target", "lease", "generation", "no_owner"])
async def test_forged_labels_cannot_discount_another_environment_grant(change):
    pod = _pod(1)
    if change == "namespace":
        pod.metadata.namespace = "run-2"
    elif change == "target":
        pod.metadata.annotations["loom.openai.com/target-id"] = "target-2"
    elif change == "lease":
        pod.metadata.labels["loom.openai.com/lease-id"] = "different"
    elif change == "generation":
        pod.metadata.labels["loom.openai.com/generation"] = "2"
    elif change == "no_owner":
        pod.metadata.owner_references = []
    else:
        setattr(pod.metadata.owner_references[0], change, False if change == "controller" else "foreign")
    snapshot = await _capture([_node()], [pod])
    assert snapshot.nodes[0].managed_pods == []
    assert snapshot.nodes[0].requested.cpu_millis == 1000
    assert snapshot.nodes[0].used_pod_slots == 1


@pytest.mark.asyncio
async def test_unproven_pending_pods_keep_their_own_charge_even_outside_registered_namespaces():
    pod = _pod(1, pending=True)
    pod.metadata.namespace = "foreign"
    snapshot = await _capture([], [pod])
    assert snapshot.active_nodes == 0
    assert snapshot.requested.cpu_millis == 1000
    assert snapshot.pending_pods[0].lease_id == "foreign-pod:uid-1"
    assert snapshot.template_samples == []  # No invented cold-node capacity.


@pytest.mark.asyncio
async def test_proven_incompatible_pending_pods_do_not_charge_this_pool():
    pod = _pod(1, pending=True)
    pod.metadata.namespace = "foreign"
    pod.spec.node_selector = {"loom.nebius/role": "different-pool"}
    snapshot = await _capture([], [pod])
    assert snapshot.pending_pods == []
    assert snapshot.requested.cpu_millis == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["node_uid", "provider_id", "pod_uid", "reservation"])
async def test_ambiguous_physical_or_grant_identity_fails_closed(identity):
    nodes, pods = [_node()], [_pod()]
    if identity in {"node_uid", "provider_id"}:
        second = _node("node-2")
        if identity == "node_uid":
            second.metadata.uid = nodes[0].metadata.uid
        else:
            second.spec.provider_id = nodes[0].spec.provider_id
        nodes.append(second)
    else:
        second = _pod(name="second")
        if identity == "pod_uid":
            second.metadata.uid = pods[0].metadata.uid
        pods.append(second)
    with pytest.raises(KubernetesObservationError):
        await _capture(nodes, pods)


@pytest.mark.asyncio
async def test_registered_work_scheduled_outside_pool_is_not_invented_free_capacity():
    pod = _pod()
    pod.spec.node_name = "different-pool"
    with pytest.raises(KubernetesObservationError, match="outside"):
        await _capture([_node()], [pod])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["ReplicaSet", "Job"])
@pytest.mark.parametrize("namespace", ["run-1", "run-1-build"])
@pytest.mark.parametrize("pending", [False, True])
async def test_platform_controllers_in_registered_namespaces_do_not_charge_execution_pool(kind, namespace, pending):
    pod = _pod(name="platform-controller", pending=pending)
    pod.metadata.namespace = namespace
    pod.metadata.labels = {"app.kubernetes.io/name": "platform-controller"}
    pod.metadata.annotations = {}
    pod.metadata.owner_references = [k8s.V1OwnerReference(
        api_version="apps/v1" if kind == "ReplicaSet" else "batch/v1", kind=kind,
        name="platform-controller", uid="platform-controller-uid", controller=True)]
    pod.spec.node_selector = {"loom.nebius/role": "system"}
    pod.spec.node_name = None if pending else "platform-node"
    snapshot = await _capture([], [pod])
    assert snapshot.active_nodes == 0
    assert snapshot.requested.cpu_millis == 0
    assert snapshot.pending_pods == []
    assert snapshot.pending_jobs == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "generation", "owner_name", "owner_controller"])
async def test_gateway_job_cannot_hide_off_pool_execution_with_a_conflicting_selector(damage):
    pod = _pod()
    pod.spec.node_selector = {"loom.nebius/role": "system"}
    pod.spec.node_name = "platform-node"
    if damage == "generation":
        pod.metadata.labels["loom.openai.com/generation"] = "wrong"
    elif damage == "owner_name":
        pod.metadata.owner_references[0].name = "wrong"
    elif damage == "owner_controller":
        pod.metadata.owner_references[0].controller = False
    with pytest.raises(KubernetesObservationError, match="outside"):
        await _capture([], [pod])


@pytest.mark.asyncio
async def test_unknown_registered_pod_without_exclusion_remains_fail_closed_off_pool():
    pod = _pod()
    pod.metadata.owner_references = []
    pod.spec.node_name = "platform-node"
    with pytest.raises(KubernetesObservationError, match="outside"):
        await _capture([], [pod])


@pytest.mark.asyncio
async def test_excluded_registered_controller_still_charges_actual_pool_occupancy():
    pod = _pod()
    pod.metadata.owner_references = []
    pod.spec.node_selector = {"loom.nebius/role": "system"}
    snapshot = await _capture([_node()], [pod])
    assert snapshot.nodes[0].managed_pods == []
    assert snapshot.nodes[0].requested.cpu_millis == 1000
    assert snapshot.nodes[0].used_pod_slots == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["verifier", "task_image_build"])
async def test_builds_and_verifiers_share_the_same_physical_accounting(kind):
    from loom_execution_capacity_collector.pool import PoolObservationScope

    data = _scope().model_dump()
    data["jobs"][0]["workload_kind"] = kind
    pod = _pod(1, pending=True)
    if kind == "task_image_build":
        data["jobs"][0].update(namespace="run-1-build", lease_id="task-image:materialization")
        pod.metadata.namespace = "run-1-build"
        pod.metadata.labels = {"app.kubernetes.io/component": "task-image-builder",
                               "loom.materialization-id": "materialization", "loom.lease-epoch": "1"}
    else:
        pod.metadata.annotations["loom.openai.com/execution-role"] = "verifier"
    scope = PoolObservationScope.model_validate(data)
    snapshot = await _capture([], [pod], scope)
    assert snapshot.pending_pods[0].lease_id == "reservation:00000000-0000-0000-0000-000000000015"
    assert snapshot.pending_pods[0].requests.cpu_millis == 1000
    assert snapshot.pending_jobs == 1


@pytest.mark.asyncio
async def test_terminating_work_sidecar_init_peak_and_foreign_resident_pods_are_charged():
    pod = _pod(1)
    pod.metadata.deletion_timestamp = datetime.now(UTC)
    pod.spec.init_containers = [
        k8s.V1Container(name="sidecar", restart_policy="Always",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "100m"})),
        k8s.V1Container(name="init", resources=k8s.V1ResourceRequirements(requests={"cpu": "2"})),
    ]
    pod.spec.overhead = {"cpu": "50m"}
    pod.spec.containers[0].env = [k8s.V1EnvVar(name="SECRET", value="never-export")]
    pod.spec.containers[0].command = ["never-export-command"]
    foreign = _pod(2)
    foreign.metadata.namespace = "outside"
    # A running foreign Pod counts even if its selector no longer matches.
    foreign.spec.node_selector = {"loom.nebius/role": "different-pool"}
    snapshot = await _capture([_node()], [pod, foreign])
    assert snapshot.nodes[0].requested.cpu_millis == 3150
    assert snapshot.nodes[0].used_pod_slots == 2
    assert snapshot.nodes[0].managed_pods[0].requests.cpu_millis == 2150
    assert "never-export" not in snapshot.model_dump_json()


@pytest.mark.asyncio
async def test_lost_job_receipt_keeps_pod_charge_without_claiming_observation():
    from loom_execution_capacity_collector.pool import PoolObservationScope

    data = _scope().model_dump()
    data["jobs"] = []
    snapshot = await _capture([_node()], [_pod(1)], PoolObservationScope.model_validate(data))
    assert snapshot.nodes[0].managed_pods == []
    assert snapshot.nodes[0].requested.cpu_millis == 1000


@pytest.mark.asyncio
async def test_pool_scope_fingerprint_is_order_independent_and_binds_gateway_receipts():
    from loom_execution_capacity_collector.pool import PoolObservationScope

    scope = _scope()
    first = await _capture([], [], scope)
    reordered = scope.model_dump()
    reordered["environments"] = list(reversed(reordered["environments"]))
    reordered["jobs"] = list(reversed(reordered["jobs"]))
    second = await _capture([], [], PoolObservationScope.model_validate(reordered))
    assert first.source_versions["pool_scope"] == second.source_versions["pool_scope"]
    reordered["jobs"][0]["job_uid"] = "new-job-uid"
    third = await _capture([], [], PoolObservationScope.model_validate(reordered))
    assert first.source_versions["pool_scope"] != third.source_versions["pool_scope"]


@pytest.mark.parametrize("change", [
    "environment", "incarnation", "namespace", "target", "job_uid", "job_name", "reservation",
    "wrong_incarnation", "wrong_namespace", "nil", "empty_selector", "selector_injection", "unknown_target",
])
def test_ambiguous_or_unbound_management_scope_is_rejected(change):
    from loom_execution_capacity_collector.pool import PoolObservationScope

    data = _scope().model_dump()
    if change == "target":
        data["environments"][0]["target_ids"] = ["target-1", "target-1"]
    elif change == "unknown_target":
        data["jobs"][0]["target_id"] = "not-registered"
    elif change in {"environment", "incarnation", "namespace"}:
        key = {"environment": "environment_id", "incarnation": "incarnation",
               "namespace": "execution_namespace"}[change]
        data["environments"][1][key] = data["environments"][0][key]
    elif change in {"job_uid", "job_name", "reservation"}:
        key = "reservation_id" if change == "reservation" else change
        data["jobs"][1][key] = data["jobs"][0][key]
        if change == "job_name":
            # Same names in different namespaces are legitimate; duplicate the
            # exact namespace/name pair within a single registered environment.
            data["jobs"][1].update(environment_id=UUID(int=1), incarnation=UUID(int=11), namespace="run-1")
    elif change == "wrong_incarnation":
        data["jobs"][0]["incarnation"] = UUID(int=99)
    elif change == "wrong_namespace":
        data["jobs"][0]["namespace"] = "run-1-build"
    elif change == "nil":
        data["jobs"][0]["reservation_id"] = UUID(int=0)
    elif change == "empty_selector":
        data["node_selector"] = {}
    else:
        data["node_selector"] = {"pool": "x,other=true"}
    with pytest.raises(ValidationError):
        PoolObservationScope.model_validate(data)


@pytest.mark.asyncio
async def test_local_target_aliases_share_one_inventory_and_remain_job_bound():
    from loom_execution_capacity_collector.pool import PoolObservationScope

    data = _scope().model_dump()
    for environment in data["environments"]:
        environment["target_ids"] = ["native", "native-alias"]
    for job in data["jobs"]:
        job["target_id"] = "native-alias"
    pods = [_pod(1), _pod(2)]
    for pod in pods:
        pod.metadata.annotations["loom.openai.com/target-id"] = "native-alias"
    scope = PoolObservationScope.model_validate(data)
    result = await _capture([_node()], pods, scope)
    assert result.active_nodes == 1
    assert result.requested.cpu_millis == 2000
    assert len(result.nodes[0].managed_pods) == 2
    # Another registered alias is not evidence for this particular receipt.
    pods[0].metadata.annotations["loom.openai.com/target-id"] = "native"
    changed = await _capture([_node()], pods, scope)
    assert changed.requested.cpu_millis == 2000
    assert [pod.lease_id for pod in changed.nodes[0].managed_pods] == [
        "reservation:00000000-0000-0000-0000-000000000016",
    ]


@pytest.mark.asyncio
async def test_pool_fingerprint_sorts_local_aliases_but_binds_receipt_target():
    from loom_execution_capacity_collector.pool import PoolObservationScope

    data = _scope().model_dump()
    data["environments"][0]["target_ids"] = ["target-1", "native-alias"]
    first = await _capture([], [], PoolObservationScope.model_validate(data))
    data["environments"][0]["target_ids"].reverse()
    reordered = await _capture([], [], PoolObservationScope.model_validate(data))
    assert first.source_versions["pool_scope"] == reordered.source_versions["pool_scope"]
    data["jobs"][0]["target_id"] = "native-alias"
    changed = await _capture([], [], PoolObservationScope.model_validate(data))
    assert first.source_versions["pool_scope"] != changed.source_versions["pool_scope"]


@pytest.mark.asyncio
async def test_pool_reads_pod_level_requests_not_lost_by_older_kubernetes_sdk():
    pod = _pod(1)
    with k8s.ApiClient() as api:
        raw = api.sanitize_for_serialization(pod)
    # The pinned SDK does not declare V1PodSpec.resources, but the API can
    # return PodLevelResources. The container's 1 CPU / 1 GiB is not its total.
    raw["spec"]["resources"] = {"requests": {"cpu": "3", "memory": "4Gi"}}
    raw["spec"]["overhead"] = {"cpu": "50m", "memory": "16Mi"}
    snapshot = await _capture([_node()], [pod], raw_pods=[raw])
    assert snapshot.requested.cpu_millis == 3050
    assert snapshot.requested.memory_mib == 4112
    assert snapshot.requested.storage_mib == 2048
    assert snapshot.nodes[0].managed_pods[0].requests.cpu_millis == 3050


@pytest.mark.asyncio
async def test_pool_rejects_incomplete_pod_list_instead_of_reporting_empty_capacity():
    reader = InClusterKubernetesCapacityReader(
        core_api=SimpleNamespace(
            list_node=lambda **_: SimpleNamespace(items=[], metadata=SimpleNamespace(resource_version="nodes-1")),
            list_pod_for_all_namespaces=lambda **_: SimpleNamespace(data=b'{"metadata":{"resourceVersion":"1"}}'),
        ),
        apps_api=SimpleNamespace(list_daemon_set_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[], metadata=SimpleNamespace(resource_version="ds-1"),
        )),
    )
    with pytest.raises(KubernetesObservationError):
        await reader.capture_pool(scope=_scope())


@pytest.mark.asyncio
@pytest.mark.parametrize("resize", [
    "legacy_status", "condition", "allocated", "status_requests", "pod_level", "init_allocated",
])
async def test_pool_never_frees_capacity_during_unqualified_in_place_resize(resize):
    pod = _pod(1)
    pod.metadata.namespace = "foreign"
    with k8s.ApiClient() as api:
        raw = api.sanitize_for_serialization(pod)
    if resize == "legacy_status":
        raw["status"]["resize"] = "InProgress"
    elif resize == "condition":
        raw["status"]["conditions"] = [{"type": "PodResizeInProgress", "status": "True"}]
    elif resize == "pod_level":
        raw["status"]["resources"] = {"requests": {"cpu": "3", "memory": "3Gi"}}
    else:
        values = {"cpu": "3", "memory": "3Gi"}
        status = {"name": "work", "ready": True, "restartCount": 0, "image": "test", "imageID": "test"}
        status["allocatedResources" if resize in {"allocated", "init_allocated"} else "resources"] = (
            values if resize in {"allocated", "init_allocated"} else {"requests": values}
        )
        if resize == "init_allocated":
            raw["spec"]["initContainers"] = [{
                "name": "sidecar", "restartPolicy": "Always", "image": "test",
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
            }]
            status["name"] = "sidecar"
        raw["status"]["initContainerStatuses" if resize == "init_allocated" else "containerStatuses"] = [status]
    with pytest.raises(KubernetesObservationError, match="resize"):
        await _capture([_node()], [pod], raw_pods=[raw])


@pytest.mark.asyncio
async def test_equal_allocated_status_and_unrelated_pool_resize_do_not_block_inventory():
    first, other = _pod(1), _pod(2)
    other.metadata.namespace = "foreign"
    other.spec.node_name = "other-node"
    with k8s.ApiClient() as api:
        raw = api.sanitize_for_serialization([first, other])
    raw[0]["status"]["containerStatuses"] = [{
        "name": "work", "ready": True, "restartCount": 0, "image": "test", "imageID": "test",
        "allocatedResources": {"cpu": "1", "memory": "1Gi"},
    }]
    raw[1]["status"]["resize"] = "InProgress"
    snapshot = await _capture([_node()], [first, other], raw_pods=raw)
    assert snapshot.requested.cpu_millis == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["allocatedResources", "resources"])
@pytest.mark.parametrize("shape,want_cpu", [("containers", 1000), ("init_sidecar", 1500), ("pod_level", 3000)])
async def test_pod_status_summary_matches_effective_request_without_false_resize(field, shape, want_cpu):
    pod = _pod(1)
    with k8s.ApiClient() as api:
        raw = api.sanitize_for_serialization(pod)
    if shape == "init_sidecar":
        raw["spec"]["initContainers"] = [{"name": "sidecar", "restartPolicy": "Always", "image": "test",
                                          "resources": {"requests": {"cpu": "500m"}}}]
    elif shape == "pod_level":
        raw["spec"]["resources"] = {"requests": {"cpu": "3"}}
    observed = {"cpu": str(want_cpu) + "m", "memory": "1Gi"}
    raw["status"][field] = observed if field == "allocatedResources" else {"requests": observed}
    snapshot = await _capture([_node()], [pod], raw_pods=[raw])
    assert snapshot.requested.cpu_millis == want_cpu
    assert snapshot.requested.memory_mib == 1024


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["verifier", None])
async def test_wrong_or_missing_execution_role_cannot_discount_trial_grant(role):
    pod = _pod()
    if role is None:
        pod.metadata.annotations.pop("loom.openai.com/execution-role")
    else:
        pod.metadata.annotations["loom.openai.com/execution-role"] = role
    snapshot = await _capture([_node()], [pod])
    assert snapshot.nodes[0].managed_pods == []
    assert snapshot.nodes[0].requested.cpu_millis == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "changed_version", "repeated_token"])
async def test_generated_kubernetes_client_preserves_pool_pagination_and_snapshot_fences(monkeypatch, mode):
    pod_pages = []
    with k8s.ApiClient(configuration=k8s.Configuration(host="https://cluster.test")) as api:
        def request(method, url, **kwargs):
            assert method == "GET"
            parsed = urlsplit(url)
            query = parse_qs(parsed.query)
            query.update({key: [value] for key, value in kwargs.get("fields", [])})
            if parsed.path == "/api/v1/nodes":
                kind, version, items, continuation = "NodeList", "n1", [_node()], None
            elif parsed.path == "/apis/apps/v1/daemonsets":
                kind, version, items, continuation = "DaemonSetList", "d1", [], None
            else:
                assert parsed.path == "/api/v1/pods"
                pod_pages.append(query)
                page = len(pod_pages)
                assert query.get("continue") == (None if page == 1 else ["page-2"])
                kind, version, items = "PodList", "p1", [_pod(page)]
                continuation = "page-2" if page == 1 or mode == "repeated_token" else None
                if page == 2 and mode == "changed_version":
                    version = "p2"
            body = {
                "apiVersion": "apps/v1" if kind == "DaemonSetList" else "v1", "kind": kind,
                "metadata": {"resourceVersion": version, **({"continue": continuation} if continuation else {})},
                "items": api.sanitize_for_serialization(items),
            }
            return HTTPResponse(body=json.dumps(body).encode(), status=200,
                                headers={"Content-Type": "application/json"}, preload_content=False)

        monkeypatch.setattr(api.rest_client.pool_manager, "request", request)
        reader = InClusterKubernetesCapacityReader(core_api=k8s.CoreV1Api(api), apps_api=k8s.AppsV1Api(api))
        if mode == "complete":
            snapshot = await reader.capture_pool(scope=_scope())
            assert snapshot.active_nodes == 1
            assert snapshot.requested.cpu_millis == 2000
            assert len(snapshot.nodes[0].managed_pods) == 2
        else:
            reason = "resource version" if mode == "changed_version" else "repeated a token"
            with pytest.raises(KubernetesObservationError, match=reason):
                await reader.capture_pool(scope=_scope())
        assert len(pod_pages) == 2
