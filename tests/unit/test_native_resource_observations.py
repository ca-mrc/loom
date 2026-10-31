"""Verified API observations retain node, producer and Pod identity."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import httpx
import pytest

from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.contracts import (
    KubernetesApiError,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.resource_observations import (
    read_resource_observation,
    validate_cached_resource_observation,
)

NODE_UID = str(uuid4())
PRODUCER_UID = str(uuid4())
NOW = datetime.now(UTC)


def _core():
    owner = SimpleNamespace(api_version="v1", kind="Node", name="node-1", uid=NODE_UID)
    node = SimpleNamespace(
        metadata=SimpleNamespace(name="node-1", uid=NODE_UID, deletion_timestamp=None)
    )
    producer = SimpleNamespace(
        metadata=SimpleNamespace(
            name="observer-1",
            namespace="observations",
            uid=PRODUCER_UID,
            deletion_timestamp=None,
            owner_references=[
                SimpleNamespace(
                    api_version="apps/v1",
                    kind="DaemonSet",
                    name="loom-native-resource-observer",
                    uid=str(uuid4()),
                    controller=True,
                )
            ],
        ),
        spec=SimpleNamespace(node_name="node-1"),
    )
    snapshot = {
        "schema_version": "loom.native-resource-observation.v1",
        "node_name": "node-1",
        "node_uid": NODE_UID,
        "producer_pod_name": "observer-1",
        "producer_pod_uid": PRODUCER_UID,
        "observed_at": NOW.isoformat(),
        "summary": {
            "node": {"nodeName": "node-1"},
            "pods": [
                {
                    "podRef": {"namespace": "execution", "uid": str(uuid4()), "name": "job-pod"},
                    "containers": [
                        {
                            "name": "execution",
                            "startTime": (NOW - timedelta(seconds=60)).isoformat(),
                            "cpu": {"time": NOW.isoformat(), "usageCoreNanoSeconds": 4000000},
                            "memory": {"time": NOW.isoformat(), "usageBytes": 100},
                        }
                    ],
                }
            ],
        },
    }
    cm = SimpleNamespace(
        metadata=SimpleNamespace(
            name="node-" + NODE_UID,
            namespace="observations",
            deletion_timestamp=None,
            owner_references=[owner],
        ),
        data={"snapshot.json": json.dumps(snapshot)},
    )
    core = SimpleNamespace(
        api_client=SimpleNamespace(
            configuration=SimpleNamespace(verify_ssl=True, ssl_ca_cert="api-ca")
        ),
        read_node=Mock(return_value=node),
        read_namespaced_config_map=Mock(return_value=cm),
        read_namespaced_pod=Mock(return_value=producer),
    )
    return core, snapshot, cm, producer


def _read(core):
    return read_resource_observation(
        core, node_name="node-1", namespace="observations", execution_namespace="execution", now=NOW
    )


def test_verified_observation_uses_exact_node_configmap_and_producer_gets():
    core, snapshot, _, _ = _core()
    assert _read(core) == {
        **snapshot["summary"],
        "_native_observation": {"observed_at": NOW.isoformat(), "max_age_seconds": 60},
    }
    assert core.read_node.call_args.kwargs["name"] == "node-1"
    assert core.read_namespaced_config_map.call_args.kwargs["name"] == "node-" + NODE_UID
    assert core.read_namespaced_pod.call_args.kwargs["name"] == "observer-1"
    assert all(
        0 < call.call_args.kwargs["_request_timeout"] <= 10
        for call in (
            core.read_node,
            core.read_namespaced_config_map,
            core.read_namespaced_pod,
        )
    )


@pytest.mark.parametrize(
    "damage",
    [
        "node_uid",
        "cm_owner",
        "producer_uid",
        "producer_node",
        "producer_owner",
        "summary_node",
        "foreign_namespace",
        "deleted_node",
        "insecure_api",
    ],
)
def test_observation_rejects_identity_changes(damage):
    core, snapshot, cm, producer = _core()
    if damage == "node_uid":
        snapshot["node_uid"] = str(uuid4())
    if damage == "cm_owner":
        cm.metadata.owner_references[0].uid = str(uuid4())
    if damage == "producer_uid":
        producer.metadata.uid = str(uuid4())
    if damage == "producer_node":
        producer.spec.node_name = "node-other"
    if damage == "producer_owner":
        producer.metadata.owner_references[0].name = "untrusted"
    if damage == "summary_node":
        snapshot["summary"]["node"]["nodeName"] = "node-other"
    if damage == "foreign_namespace":
        snapshot["summary"]["pods"][0]["podRef"]["namespace"] = "foreign"
    if damage == "deleted_node":
        core.read_node.return_value.metadata.deletion_timestamp = NOW
    if damage == "insecure_api":
        core.api_client.configuration.verify_ssl = False
    cm.data["snapshot.json"] = json.dumps(snapshot)
    with pytest.raises(ValueError, match=r"^native_observation_identity$"):
        _read(core)


@pytest.mark.parametrize("damage", ["stale", "future", "before_start"])
def test_observation_rejects_snapshot_times_and_incarnation_mismatch(damage):
    core, snapshot, cm, _ = _core()
    stamp = (
        NOW + timedelta(seconds=10) if damage.startswith("future") else NOW - timedelta(seconds=61)
    )
    if damage in {"stale", "future"}:
        snapshot["observed_at"] = stamp.isoformat()
    else:
        key = "cpu" if damage == "stale_cpu" else "memory"
        if damage == "before_start":
            snapshot["summary"]["pods"][0]["containers"][0]["startTime"] = NOW.isoformat()
            stamp = NOW - timedelta(seconds=1)
        snapshot["summary"]["pods"][0]["containers"][0][key]["time"] = stamp.isoformat()
    cm.data["snapshot.json"] = json.dumps(snapshot)
    with pytest.raises(ValueError, match=r"^native_observation_stale$"):
        _read(core)


@pytest.mark.parametrize(
    "damage", ["duplicate", "size", "bad_json", "pods_type", "nan", "counter_type"]
)
def test_observation_rejects_bounded_payload_damage(damage):
    core, snapshot, cm, _ = _core()
    if damage == "pods_type":
        snapshot["summary"]["pods"] = {}
    if damage == "counter_type":
        snapshot["summary"]["pods"][0]["containers"][0]["cpu"]["usageCoreNanoSeconds"] = True
    value = json.dumps(snapshot)
    if damage == "duplicate":
        value = value.replace('"schema_version":', '"schema_version":"bad","schema_version":', 1)
    if damage == "size":
        value = " " * (700 * 1024 + 1)
    if damage == "bad_json":
        value = "{invalid"
    if damage == "nan":
        value = value.replace('"usageBytes": 100', '"usageBytes": NaN')
    cm.data["snapshot.json"] = value
    with pytest.raises(ValueError, match=r"^native_observation_malformed$"):
        _read(core)


def test_node_uid_pin_rejects_replaced_node_before_reading_snapshot():
    core, _, _, _ = _core()
    with pytest.raises(ValueError, match=r"^native_observation_identity$"):
        read_resource_observation(
            core,
            node_name="node-1",
            namespace="observations",
            execution_namespace="execution",
            expected_node_uid=str(uuid4()),
            now=NOW,
        )
    core.read_namespaced_config_map.assert_not_called()


def test_api_reads_share_total_budget(monkeypatch):
    core, _, _, _ = _core()
    clock = iter((0, 1, 11))
    monkeypatch.setattr(
        "loom_execution_actuator.resource_observations.monotonic", lambda: next(clock)
    )
    with pytest.raises(TimeoutError):
        _read(core)
    core.read_namespaced_config_map.assert_not_called()


@pytest.mark.parametrize("fail", [False, True])
async def test_api_adapter_selects_observer_without_direct_kubelet_fallback(monkeypatch, fail):
    core, snapshot, cm, _ = _core()
    # Collection may precede execution by minutes in the complete unit suite.
    current = datetime.now(UTC)
    snapshot["observed_at"] = current.isoformat()
    container = snapshot["summary"]["pods"][0]["containers"][0]
    container["startTime"] = (current - timedelta(seconds=60)).isoformat()
    container["cpu"]["time"] = container["memory"]["time"] = current.isoformat()
    cm.data["snapshot.json"] = json.dumps(snapshot)
    monkeypatch.setattr(
        "loom_execution_actuator.kubernetes_api.read_kubelet_summary",
        lambda *args, **kwargs: pytest.fail("observer cannot fall back to kubelet"),
    )
    api = InClusterKubernetesJobApi(
        client_module=SimpleNamespace(),
        batch_api=object(),
        core_api=core,
        resource_observation_namespace="observations",
        resource_observation_execution_namespace="execution",
    )
    if fail:
        core.read_namespaced_config_map.side_effect = httpx.ConnectTimeout(
            "private token and endpoint"
        )
        with pytest.raises(KubernetesApiError, match=r"^Kubernetes resource_summary failed$"):
            await api.resource_summary(node_name="node-1")
    else:
        result = await api.resource_summary(node_name="node-1", expected_node_uid=NODE_UID)
        assert result["node"]["nodeName"] == "node-1"
        assert result["_native_observation"]["max_age_seconds"] == 60


async def test_cached_observation_cannot_extend_original_timestamp_freshness():
    core, snapshot, cm, _ = _core()
    older = (NOW - timedelta(seconds=50)).isoformat()
    snapshot["observed_at"] = older
    container = snapshot["summary"]["pods"][0]["containers"][0]
    container["cpu"]["time"] = container["memory"]["time"] = older
    cm.data["snapshot.json"] = json.dumps(snapshot)
    summary = _read(core)
    reader = Mock()

    async def resource_summary(**kwargs):
        reader()
        return summary

    actuator = ExecutionActuator(
        sessions=Mock(),
        kubernetes=SimpleNamespace(resource_summary=resource_summary),
        target=ExecutionTargetRuntime(target_id="target", namespace="execution"),
        controller_id="test",
        resource_usage_source="native_node_observer",
    )
    observation = KubernetesJobObservation(
        namespace="execution",
        job_name="job",
        lease_id=str(uuid4()),
        resource_generation=1,
        target_id="target",
        execution_unit_key=str(uuid4()),
        normalized_state=NormalizedJobState.RUNNING,
        node_name="node-1",
        pod_uid=snapshot["summary"]["pods"][0]["podRef"]["uid"],
    )
    samples, diagnostic = await actuator._usage_samples(observation, now=NOW)
    assert samples and diagnostic is None
    assert samples["execution"].observed_at == NOW - timedelta(seconds=50)
    assert await actuator._usage_samples(observation, now=NOW + timedelta(seconds=11)) == (
        {},
        "native_observation_stale",
    )
    assert reader.call_count == 1


@pytest.mark.parametrize("age", [14, 121, float("nan")])
def test_observation_settings_bound_maximum_age(age):
    with pytest.raises(ValueError):
        ExecutionActuatorSettings(
            db_url="postgresql://test",
            controller_id="test",
            target_id="target",
            namespace="execution",
            resource_observation_namespace="observations",
            resource_observation_max_age_seconds=age,
        )


@pytest.mark.parametrize("known_zero", [False, True])
async def test_observer_distinguishes_missing_counters_from_measured_zero(known_zero):
    core, snapshot, cm, _ = _core()
    pod = snapshot["summary"]["pods"][0]
    pod["containers"] = []
    pod["_diagnostic_codes"] = ["container_status_unavailable"]
    if known_zero:
        pod["ephemeral-storage"] = {"usedBytes": 0, "time": NOW.isoformat()}
    cm.data["snapshot.json"] = json.dumps(snapshot)
    summary = _read(core)

    async def resource_summary(**kwargs):
        return summary

    actuator = ExecutionActuator(
        sessions=Mock(),
        kubernetes=SimpleNamespace(resource_summary=resource_summary),
        target=ExecutionTargetRuntime(target_id="target", namespace="execution"),
        controller_id="test",
        resource_usage_source="native_node_observer",
    )
    observation = KubernetesJobObservation(
        namespace="execution",
        job_name="job",
        lease_id=str(uuid4()),
        resource_generation=1,
        target_id="target",
        execution_unit_key=str(uuid4()),
        normalized_state=NormalizedJobState.RUNNING,
        node_name="node-1",
        pod_uid=pod["podRef"]["uid"],
    )
    samples, diagnostic = await actuator._usage_samples(observation, now=NOW)
    if known_zero:
        assert samples["pod"].counters.ephemeral_storage_sampled_max_bytes == 0
        assert diagnostic is None
    else:
        assert samples == {}
        assert diagnostic == "native_observation_counters_unavailable"


def _two_pod_snapshot():
    core, snapshot, cm, _ = _core()
    first = snapshot["summary"]["pods"][0]
    container = first["containers"][0]
    container["startTime"] = (NOW - timedelta(seconds=120)).isoformat()
    container["rootfs"] = {"time": NOW.isoformat(), "usedBytes": 4096}
    container["logs"] = {"time": NOW.isoformat(), "usedBytes": 512}
    first["ephemeral-storage"] = {"time": NOW.isoformat(), "usedBytes": 8192}
    second = deepcopy(first)
    second["podRef"]["uid"] = str(uuid4())
    second["podRef"]["name"] = "other-pod"
    snapshot["summary"]["pods"].append(second)
    return core, snapshot, cm


@pytest.mark.parametrize("kind", ["cpu", "memory", "rootfs", "logs"])
@pytest.mark.parametrize("offset", [-61, 10])
def test_expired_field_preserves_other_fresh_counters_and_pods(kind, offset):
    core, snapshot, cm = _two_pod_snapshot()
    snapshot["summary"]["pods"][0]["containers"][0][kind]["time"] = (
        NOW + timedelta(seconds=offset)
    ).isoformat()
    cm.data["snapshot.json"] = json.dumps(snapshot)
    original = cm.data["snapshot.json"]
    result = _read(core)
    first, second = result["pods"]
    assert kind not in first["containers"][0]
    assert set(first["containers"][0]) >= ({"cpu", "memory", "rootfs", "logs"} - {kind})
    assert ("ephemeral-storage" in first) is (kind not in {"rootfs", "logs"})
    assert second == snapshot["summary"]["pods"][1]
    assert cm.data["snapshot.json"] == original
    assert result["_native_observation"]["observed_at"] == NOW.isoformat()


async def test_cached_disk_expiry_preserves_fresh_cpu_memory_and_other_pod():
    core, snapshot, cm = _two_pod_snapshot()
    snapshot["summary"]["pods"][0]["containers"][0]["rootfs"]["time"] = (
        NOW - timedelta(seconds=55)
    ).isoformat()
    cm.data["snapshot.json"] = json.dumps(snapshot)
    summary = _read(core)
    reader = Mock()

    async def resource_summary(**kwargs):
        reader()
        return summary

    actuator = ExecutionActuator(
        sessions=Mock(),
        kubernetes=SimpleNamespace(resource_summary=resource_summary),
        target=ExecutionTargetRuntime(target_id="target", namespace="execution"),
        controller_id="test",
        resource_usage_source="native_node_observer",
    )
    observation = KubernetesJobObservation(
        namespace="execution",
        job_name="job",
        lease_id=str(uuid4()),
        resource_generation=1,
        target_id="target",
        execution_unit_key=str(uuid4()),
        normalized_state=NormalizedJobState.RUNNING,
        node_name="node-1",
        pod_uid=snapshot["summary"]["pods"][0]["podRef"]["uid"],
    )
    initial, diagnostic = await actuator._usage_samples(observation, now=NOW)
    assert (
        diagnostic is None and initial["pod"].counters.ephemeral_storage_sampled_max_bytes == 8192
    )
    samples, diagnostic = await actuator._usage_samples(
        observation, now=NOW + timedelta(seconds=10)
    )
    assert diagnostic is None
    assert samples["execution"].counters.cpu_usage_usec == 4000
    assert samples["execution"].counters.memory_current_bytes == 100
    assert samples["execution"].counters.filesystem_sampled_max_bytes is None
    assert samples["pod"].counters.ephemeral_storage_sampled_max_bytes is None
    other = observation.model_copy(
        update={"pod_uid": snapshot["summary"]["pods"][1]["podRef"]["uid"]}
    )
    other_samples, other_diagnostic = await actuator._usage_samples(
        other, now=NOW + timedelta(seconds=10)
    )
    assert other_diagnostic is None
    assert other_samples["execution"].counters.filesystem_sampled_max_bytes == 4608
    assert other_samples["pod"].counters.ephemeral_storage_sampled_max_bytes == 8192
    assert reader.call_count == 1
    assert summary["pods"][0]["containers"][0]["cpu"]["time"] == NOW.isoformat()
    assert summary["_native_observation"]["observed_at"] == NOW.isoformat()
    assert await actuator._usage_samples(observation, now=NOW + timedelta(seconds=61)) == (
        {},
        "native_observation_stale",
    )


@pytest.mark.parametrize("offset", [-61, 10])
def test_expired_volume_aggregate_preserves_fresh_container_and_other_pod(offset):
    core, snapshot, cm = _two_pod_snapshot()
    snapshot["summary"]["pods"][0]["ephemeral-storage"]["time"] = (
        NOW + timedelta(seconds=offset)
    ).isoformat()
    cm.data["snapshot.json"] = json.dumps(snapshot)
    result = _read(core)
    assert "ephemeral-storage" not in result["pods"][0]
    assert result["pods"][0]["containers"] == snapshot["summary"]["pods"][0]["containers"]
    assert result["pods"][1] == snapshot["summary"]["pods"][1]


@pytest.mark.parametrize("damage", ["counter", "timestamp", "namespace"])
def test_field_expiry_does_not_hide_invalid_payload_or_mutate_rejected_cache(damage):
    _, snapshot, _ = _two_pod_snapshot()
    summary = snapshot["summary"]
    summary["_native_observation"] = {"observed_at": NOW.isoformat(), "max_age_seconds": 60}
    summary["pods"][0]["containers"][0]["rootfs"]["time"] = (
        NOW - timedelta(seconds=61)
    ).isoformat()
    second = summary["pods"][1]
    if damage == "counter":
        second["containers"][0]["memory"] = {
            "time": (NOW - timedelta(seconds=61)).isoformat(),
            "usageBytes": False,
        }
    elif damage == "timestamp":
        second["containers"][0]["cpu"]["time"] = "invalid-private-timestamp"
    else:
        second["podRef"]["namespace"] = "foreign"
    original = deepcopy(summary)
    with pytest.raises(ValueError, match=r"native_observation_(malformed|identity)$"):
        validate_cached_resource_observation(
            summary, node_name="node-1", execution_namespace="execution", now=NOW
        )
    assert summary == original
