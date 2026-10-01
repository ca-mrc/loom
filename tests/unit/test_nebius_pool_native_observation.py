"""Pool native Kubernetes reads qualify identity before accessing publisher/logs."""
from __future__ import annotations

import copy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom_execution_actuator.task_image_controller import NativeBuildKubernetesApi
from tests.unit.test_nebius_pool_client import inputs


def fixture():
    request, receipt = inputs()
    receipt.update(phase="observed", plan_sha256="a" * 64, job_uid=str(uuid4()))
    runtime = PoolNativeRuntimeV1.model_validate({"receipt": receipt, "target_id": request.target_id,
        "namespace": {"name": "loom-build", "uid": str(uuid4())},
        "job_name": "loom-pool-" + receipt["reservation_id"].replace("-", ""),
        "lease_epoch": request.build.expected_lease_epoch + 1, "deadline_at": request.deadline_at,
        "registry_repository": "registry.example/tasks", "job_effect_id": uuid4()})
    metadata = {"namespace": runtime.namespace.name, "labels": {
        "app.kubernetes.io/managed-by": "loom-pool-gateway", "app.kubernetes.io/component": "task-image-builder",
        "loom.materialization-id": str(request.key.local_work_id), "loom.lease-epoch": str(runtime.lease_epoch)},
        "annotations": {"loom.openai.com/target-id": runtime.target_id,
            "loom.nebius/pool-reservation-id": str(runtime.receipt.reservation_id),
            "loom.nebius/pool-plan-sha256": runtime.receipt.plan_sha256,
            "loom.nebius/pool-effect-id": str(runtime.job_effect_id)}}
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {**copy.deepcopy(metadata),
        "name": runtime.job_name, "uid": str(runtime.receipt.job_uid), "resourceVersion": "1"}, "status": {"succeeded": 1}}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {**copy.deepcopy(metadata),
        "name": runtime.job_name + "-abcde", "uid": str(uuid4()), "resourceVersion": "2",
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "controller": True,
            "uid": str(runtime.receipt.job_uid), "name": runtime.job_name}]}}
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": runtime.namespace.name, "uid": str(runtime.namespace.uid)}}
    return runtime, job, pod, namespace


@pytest.mark.parametrize("damage", [None, "omitted_item_types", "wrong_list_kind", "wrong_pod_kind",
    "namespace_before", "namespace_after", "job", "pod", "pod_during_log", "partial_list"])
async def test_real_native_reader_binds_namespace_job_and_pod_before_returning_results(damage):
    runtime, job, pod, namespace = fixture()
    calls = []

    def read_namespace(name, **kwargs):
        assert name == runtime.namespace.name
        calls.append("namespace")
        value = copy.deepcopy(namespace)
        if damage == "namespace_before" or (damage == "namespace_after" and calls.count("namespace") > 1):
            value["metadata"]["uid"] = str(uuid4())
        return value

    def read_job(name, namespace, **kwargs):
        assert (name, namespace) == (runtime.job_name, runtime.namespace.name)
        calls.append("job")
        value = copy.deepcopy(job)
        if damage == "job":
            value["metadata"]["uid"] = str(uuid4())
        return value

    def list_pods(namespace, **kwargs):
        calls.append("pods")
        value = copy.deepcopy(pod)
        if damage == "pod":
            value["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
        elif damage == "omitted_item_types":
            value.pop("apiVersion")
            value.pop("kind")
        elif damage == "wrong_pod_kind":
            value["kind"] = "Secret"
        return {"apiVersion": "v1", "kind": "SecretList" if damage == "wrong_list_kind" else "PodList",
            "items": [value], "metadata": {"continue": "more" if damage == "partial_list" else ""}}

    def read_pod(name, namespace, **kwargs):
        calls.append("pod")
        value = copy.deepcopy(pod)
        if damage == "pod_during_log" and "log" in calls:
            value["metadata"]["uid"] = str(uuid4())
        return value

    def read_log(name, namespace, **kwargs):
        calls.append("log")
        assert kwargs["limit_bytes"] == 16384 and kwargs["tail_lines"] == 100
        return "compile output token=must-not-persist"

    api = NativeBuildKubernetesApi.__new__(NativeBuildKubernetesApi)
    api._json = lambda value: value
    api._batch = SimpleNamespace(read_namespaced_job=read_job)
    api._core = SimpleNamespace(read_namespace=read_namespace, list_namespaced_pod=list_pods,
        read_namespaced_pod=read_pod, read_namespaced_pod_log=read_log)
    if damage in {None, "omitted_item_types"}:
        result = await api.observe_pool(runtime)
        assert result["metadata"]["uid"] == str(runtime.receipt.job_uid)
        assert result["pods"] == [pod] and "compile output" in result["builder_log"]
        assert "must-not-persist" not in result["builder_log"] and calls.count("namespace") == 2
    else:
        with pytest.raises(ValueError):
            await api.observe_pool(runtime)
        if damage in {"namespace_before", "job", "pod", "partial_list"}:
            assert "log" not in calls
