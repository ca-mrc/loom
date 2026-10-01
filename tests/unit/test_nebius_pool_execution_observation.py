"""The real execution reader qualifies gateway-owned Job/Pod/namespace identity."""
from __future__ import annotations

import asyncio
import copy
import threading
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from loom_execution_actuator.contracts import KubernetesApiError
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from tests.unit.test_execution_actuator_kubernetes import _job, _pod
from tests.unit.test_nebius_pool_execution_render import inputs


def fixture():
    from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1

    participant, body = inputs()
    reservation = uuid4()
    runtime = PoolExecutionRuntimeV1(receipt={"reservation_id": reservation, "pool_id": participant.pool_id,
        "request_key": body["key"], "request_sha256": "a" * 64, "admission_epoch": participant.admission_epoch,
        "phase": "observed", "plan_sha256": "b" * 64, "job_uid": uuid4()},
        target_id=body["target_id"], namespace=participant.execution_namespace,
        job_name="loom-pool-" + reservation.hex, resource_generation=1, lease_generation=1,
        execution_unit_key=body["execution"]["execution_unit_key"], deadline_at=body["deadline_at"], job_effect_id=uuid4())
    job, pod = _job(), _pod(phase="Running")
    job.api_version, job.kind = "batch/v1", "Job"
    job.metadata.name, job.metadata.namespace = runtime.job_name, runtime.namespace.name
    job.metadata.uid = str(runtime.receipt.job_uid)
    job.metadata.labels.update({"app.kubernetes.io/managed-by": "loom-execution-actuator",
        "app.kubernetes.io/component": "execution-unit",
        "loom.openai.com/lease-id": str(runtime.receipt.request_key.local_work_id)})
    job.metadata.annotations.update({"loom.openai.com/target-id": runtime.target_id,
        "loom.openai.com/execution-unit-key": str(runtime.execution_unit_key),
        "loom.nebius/pool-reservation-id": str(runtime.receipt.reservation_id),
        "loom.nebius/pool-plan-sha256": runtime.receipt.plan_sha256,
        "loom.nebius/pool-effect-id": str(runtime.job_effect_id)})
    pod.api_version, pod.kind = "v1", "Pod"
    pod.metadata.name, pod.metadata.namespace = runtime.job_name + "-abcde", runtime.namespace.name
    pod.metadata.uid, pod.metadata.resource_version = str(uuid4()), "43"
    pod.metadata.labels, pod.metadata.annotations = copy.deepcopy(job.metadata.labels), copy.deepcopy(job.metadata.annotations)
    pod.metadata.owner_references = [SimpleNamespace(api_version="batch/v1", kind="Job", name=runtime.job_name,
        uid=str(runtime.receipt.job_uid), controller=True)]
    namespace = SimpleNamespace(api_version="v1", kind="Namespace", metadata=SimpleNamespace(
        name=runtime.namespace.name, uid=str(runtime.namespace.uid), deletion_timestamp=None))
    return runtime, job, pod, namespace


@pytest.mark.parametrize("damage", [None, "namespace-before", "namespace-after", "namespace-deleting",
    "job-uid", "job-reservation", "job-effect", "pod-owner", "pod-plan", "extra-pod", "partial-list",
    "wrong-kind", "pod-controller-label", "job-generation", "job-lease", "pod-target"])
async def test_execution_reader_returns_only_qualified_owned_observation(damage):
    runtime, job, pod, namespace = fixture()
    calls = []

    def read_namespace(name, **kwargs):
        assert name == runtime.namespace.name
        calls.append("namespace")
        value = copy.deepcopy(namespace)
        if damage == "namespace-before" or (damage == "namespace-after" and calls.count("namespace") == 2):
            value.metadata.uid = str(uuid4())
        if damage == "namespace-deleting":
            value.metadata.deletion_timestamp = datetime.now(UTC)
        return value

    def read_job(name, namespace, **kwargs):
        assert (name, namespace) == (runtime.job_name, runtime.namespace.name)
        calls.append("job")
        value = copy.deepcopy(job)
        if damage == "job-uid":
            value.metadata.uid = str(uuid4())
        elif damage in {"job-reservation", "job-effect"}:
            value.metadata.annotations["loom.nebius/pool-" + ("reservation-id" if damage == "job-reservation" else "effect-id")] = str(uuid4())
        elif damage == "job-generation":
            value.metadata.labels["loom.openai.com/generation"] = "2"
        elif damage == "job-lease":
            value.metadata.labels["loom.openai.com/lease-id"] = str(uuid4())
        return value

    def pods(namespace, **kwargs):
        assert namespace == runtime.namespace.name and kwargs["label_selector"] == "job-name=" + runtime.job_name
        calls.append("pods")
        value = copy.deepcopy(pod)
        if damage == "pod-owner":
            value.metadata.owner_references[0].uid = str(uuid4())
        elif damage == "pod-plan":
            value.metadata.annotations["loom.nebius/pool-plan-sha256"] = "c" * 64
        elif damage == "wrong-kind":
            value.kind = "Secret"
        elif damage == "pod-controller-label":
            value.metadata.labels["batch.kubernetes.io/controller-uid"] = str(uuid4())
        elif damage == "pod-target":
            value.metadata.annotations["loom.openai.com/target-id"] = "another-target"
        return SimpleNamespace(api_version="v1", kind="PodList", items=[value] * (2 if damage == "extra-pod" else 1),
            metadata=SimpleNamespace(_continue="more" if damage == "partial-list" else ""))

    api = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    api._batch = SimpleNamespace(read_namespaced_job=read_job)
    api._core = SimpleNamespace(read_namespace=read_namespace, list_namespaced_pod=pods)
    if damage:
        with pytest.raises(KubernetesApiError):
            await api.observe_pool(runtime)
    else:
        result = await api.observe_pool(runtime)
        assert result.job_uid == str(runtime.receipt.job_uid) and result.pod_uid == pod.metadata.uid
        assert result.normalized_state == "running" and result.node_name == "node-a"
        assert calls == ["namespace", "job", "pods", "namespace"]


async def test_missing_job_is_only_absence_not_a_deletion_observation_or_retry():
    runtime, _, _, namespace = fixture()
    calls = []

    def missing(*args, **kwargs):
        calls.append("job")
        error = RuntimeError("gone")
        error.status = 404
        raise error

    def read_namespace(*args, **kwargs):
        calls.append("namespace")
        return namespace

    api = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    api._batch = SimpleNamespace(read_namespaced_job=missing)
    api._core = SimpleNamespace(read_namespace=read_namespace)
    assert await api.observe_pool(runtime) is None
    assert calls == ["namespace", "job", "namespace"]


@pytest.mark.parametrize("operation", ["probe", "observe", "usage"])
@pytest.mark.parametrize("sdk_failure", [False, True])
async def test_cancelled_pool_read_finishes_before_its_client_closes(operation, sdk_failure):
    runtime, _, _, namespace = fixture()
    started, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    resource = httpx.Client()

    def bounded_read(*args, **kwargs):
        loop.call_soon_threadsafe(started.set)
        assert release.wait(5), "test did not release its SDK read"
        assert not resource.is_closed, "client closed underneath an active SDK read"
        if sdk_failure:
            raise RuntimeError("SDK read failed during shutdown")
        if operation == "usage":
            return SimpleNamespace(data=b"{}", release_conn=lambda: None)
        return namespace

    def missing_job(*args, **kwargs):
        error = RuntimeError("gone")
        error.status = 404
        raise error

    api = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    api._api_client, api._credentials = resource, None
    api._core = SimpleNamespace(read_namespace=bounded_read, connect_get_node_proxy_with_path=bounded_read)
    api._batch = SimpleNamespace(read_namespaced_job=missing_job)

    async def owning_loop():
        try:
            if operation == "probe":
                await api.probe_pool_namespace(runtime.namespace)
            elif operation == "observe":
                await api.observe_pool(runtime)
            else:
                await api.resource_summary(node_name="node-a")
        finally:
            await api.close()

    task = asyncio.create_task(owning_loop())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        closed_while_reading = resource.is_closed
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        resource.close()
    assert not done and not closed_while_reading
