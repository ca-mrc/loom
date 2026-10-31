"""The observer publishes only identity-qualified, bounded real samples."""

import copy
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace as S
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from loom_execution_actuator.node_observer import (
    NodeResourceObserver,
    ObservationError,
    ObserverSettings,
    _stamp,
)

NOW = datetime(2026, 10, 31, 4, 50, tzinfo=UTC)
NODE_UID, POD_UID, PRODUCER_UID = (str(uuid4()) for _ in range(3))
CID = "containerd://" + "a" * 64


def settings(**changes):
    return ObserverSettings(namespace="observations", execution_namespace="execution",
        node_name="node-1", pod_name="observer-1", pod_uid=PRODUCER_UID, **changes)


def workload():
    return S(metadata=S(namespace="execution", name="task", uid=POD_UID,
                        labels={"app.kubernetes.io/managed-by": "loom-execution-actuator"}),
             spec=S(node_name="node-1", volumes=[S(name="workspace", empty_dir=S(medium="")),
                                                S(name="memory", empty_dir=S(medium="Memory")),
                                                S(name="private", empty_dir=None)]),
             status=S(container_statuses=[S(name="execution", container_id=CID,
                        state=S(running=S(started_at=NOW - timedelta(minutes=1)), terminated=None))],
                      init_container_statuses=[]))


class Core:
    def __init__(self):
        self.pods = [workload()]
        self.records = []
        self.node = S(metadata=S(name="node-1", uid=NODE_UID, deletion_timestamp=None))
        self.producer = S(metadata=S(uid=PRODUCER_UID, namespace="observations", deletion_timestamp=None,
            owner_references=[S(kind="DaemonSet", name="loom-native-resource-observer", controller=True)]),
            spec=S(node_name="node-1"))

    def read_node(self, **kwargs):
        return self.node

    def read_namespaced_pod(self, **kwargs):
        assert kwargs["namespace"] == "observations"
        return self.producer

    def list_namespaced_pod(self, **kwargs):
        assert kwargs["namespace"] == "execution"
        assert kwargs["field_selector"] == "spec.nodeName=node-1"
        assert kwargs["label_selector"] == "app.kubernetes.io/managed-by=loom-execution-actuator"
        return S(items=self.pods, metadata=S(_continue=None))

    def read_namespaced_config_map(self, **kwargs):
        if not self.records:
            raise ApiException(status=404)
        return S(metadata=S(resource_version="1"))

    def create_namespaced_config_map(self, **kwargs):
        self.records.append(copy.deepcopy(kwargs["body"]))

    def replace_namespaced_config_map(self, **kwargs):
        assert kwargs["body"]["metadata"]["resourceVersion"] == "1"
        self.records.append(copy.deepcopy(kwargs["body"]))


def sample(at=NOW, cpu=100_000_000):
    return {"pods": [{"podRef": {"uid": POD_UID, "namespace": "execution", "name": "task"},
        "containers": [{"name": "execution", "startTime": (NOW - timedelta(seconds=62)).isoformat().replace("+00:00", "Z"),
            "_started_at": (NOW - timedelta(minutes=1)).isoformat(),
            "_container_id": CID,
            "cpu": {"time": at.isoformat(), "usageCoreNanoSeconds": cpu},
            "memory": {"time": at.isoformat(), "usageBytes": 1234},
            "rootfs": {"time": at.isoformat(), "usedBytes": 4096},
            "logs": {"time": at.isoformat(), "usedBytes": 4096}}],
        "ephemeral-storage": {"time": at.isoformat(), "usedBytes": 16384}}]}


def published(core):
    return json.loads(core.records[-1]["data"]["snapshot.json"])


def test_observer_api_identity_snapshot_and_real_cpu_delta():
    core = Core()
    observer = NodeResourceObserver(settings(), core)
    seen = []

    def collect(request, timeout):
        seen.append(request)
        assert 0 < timeout <= 10
        return sample(NOW + timedelta(seconds=15 * (len(seen) - 1)),
                      100_000_000 + 30_000_000 * (len(seen) - 1))

    observer._sample = collect
    observer.collect_once(now=NOW)
    first = published(core)
    assert first["node_uid"] == NODE_UID
    assert first["producer_pod_uid"] == PRODUCER_UID
    assert first["producer_pod_name"] == "observer-1"
    assert first["summary"]["node"] == {"nodeName": "node-1"}
    assert core.records[-1]["metadata"]["ownerReferences"][0]["uid"] == NODE_UID
    assert seen[0]["pods"][0]["volumes"] == [
        {"name": "workspace", "kind": "empty_dir", "medium": ""},
        {"name": "memory", "kind": "empty_dir", "medium": "Memory"}]
    assert "usageNanoCores" not in first["summary"]["pods"][0]["containers"][0]["cpu"]
    observer.collect_once(now=NOW + timedelta(seconds=15))
    container = published(core)["summary"]["pods"][0]["containers"][0]
    assert container["cpu"]["usageNanoCores"] == 2_000_000
    assert container["memory"]["usageBytes"] == 1234
    assert container["rootfs"]["time"] == (NOW + timedelta(seconds=15)).isoformat()


@pytest.mark.parametrize("fault", ["foreign_pod", "foreign_namespace", "container_id", "started_at", "duplicate"])
def test_observer_rejects_helper_identity_drift(fault):
    core, body = Core(), sample()
    pod = body["pods"][0]
    if fault == "foreign_pod":
        pod["podRef"]["uid"] = str(uuid4())
    elif fault == "foreign_namespace":
        pod["podRef"]["namespace"] = "foreign"
    elif fault == "container_id":
        pod["containers"][0]["_container_id"] = "containerd://" + "b" * 64
    elif fault == "started_at":
        pod["containers"][0]["_started_at"] = NOW.isoformat()
    else:
        body["pods"].append(copy.deepcopy(pod))
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    with pytest.raises(ObservationError, match="identity"):
        observer.collect_once(now=NOW)
    assert not core.records


def test_original_times_survive_capture_window_and_stale_values_remain_unknown():
    observer = NodeResourceObserver(settings(), Core())
    body = sample(NOW + timedelta(seconds=2))
    observer._qualify(body, observer._request([workload()]), NOW, NOW + timedelta(seconds=3))
    assert body["pods"][0]["containers"][0]["cpu"]["time"] == (NOW + timedelta(seconds=2)).isoformat()
    stale = sample(NOW - timedelta(seconds=61))
    observer._qualify(stale, observer._request([workload()]), NOW, NOW)
    assert "cpu" not in stale["pods"][0]["containers"][0]
    assert "memory" not in stale["pods"][0]["containers"][0]


def test_counter_reset_does_not_create_negative_or_fabricated_cpu_rate():
    observer = NodeResourceObserver(settings(), Core())
    request = observer._request([workload()])
    observer._qualify(sample(), request, NOW, NOW)
    reset = sample(NOW + timedelta(seconds=15), 1)
    observer._qualify(reset, request, NOW + timedelta(seconds=15), NOW + timedelta(seconds=15))
    assert "usageNanoCores" not in reset["pods"][0]["containers"][0]["cpu"]


@pytest.mark.parametrize("fault", ["namespace", "node", "producer_uid", "size"])
def test_observer_refuses_inventory_scope_or_producer_mismatch(fault):
    core = Core()
    if fault == "namespace":
        core.pods[0].metadata.namespace = "foreign"
    elif fault == "node":
        core.pods[0].spec.node_name = "foreign"
    elif fault == "producer_uid":
        core.producer.metadata.uid = str(uuid4())
    else:
        core.pods = [workload()] * 129
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: pytest.fail("unqualified inventory reached runtime")
    with pytest.raises(ObservationError):
        observer.collect_once(now=NOW)


def test_helper_process_boundary_is_bounded_and_has_no_shell(tmp_path):
    helper = tmp_path / "helper"
    helper.write_text("#!/usr/bin/env python3\nimport sys,json\nrequest=json.load(sys.stdin)\nassert request['schema_version']=='loom.node-resource-request.v1'\nprint(json.dumps({'pods':[]}))\n")
    helper.chmod(0o700)
    observer = NodeResourceObserver(settings(helper_path=str(helper)), Core())
    assert observer._sample(observer._request([]), 2) == {"pods": []}


@pytest.mark.parametrize("endpoint", ["http://api.test", "https://u:p@api.test", "https://api.test/path", "https://api.test?secret=x"])
def test_api_endpoint_override_preserves_https_origin_boundary(endpoint):
    with pytest.raises(ValueError):
        settings(kubernetes_endpoint=endpoint)


def test_cpu_time_keeps_original_nanosecond_precision():
    assert _stamp("2026-10-31T04:50:00.123456789Z") % 10**9 == 123456789


def test_cri_paths_and_arbitrary_metadata_are_not_persisted():
    core = Core()
    body = sample()
    body["foreign_node_inventory"] = "must-not-be-persisted"
    body["pods"][0]["private_metadata"] = "must-not-be-persisted"
    body["pods"][0]["podRef"]["private_metadata"] = "must-not-be-persisted"
    body["pods"][0]["containers"][0]["log_path"] = "/private/host/path"
    body["pods"][0]["containers"][0]["rootfs"]["mountpoint"] = "/private/rootfs"
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    observer.collect_once(now=NOW)
    raw = core.records[-1]["data"]["snapshot.json"]
    assert "must-not-be-persisted" not in raw
    assert "/private/" not in raw


def test_configmap_and_git_repo_are_counted_as_local_ephemeral_but_projected_is_not():
    pod = workload()
    pod.spec.volumes.extend([S(name="config", empty_dir=None, config_map=S()),
                            S(name="repo", empty_dir=None, config_map=None, git_repo=S()),
                            S(name="token", empty_dir=None, config_map=None, git_repo=None)])
    observer = NodeResourceObserver(settings(), Core())
    volumes = observer._request([pod])["pods"][0]["volumes"]
    assert {volume["name"]: volume["kind"] for volume in volumes} == {
        "workspace": "empty_dir", "memory": "empty_dir", "config": "config_map", "repo": "git_repo"}


@pytest.mark.parametrize(("field", "key"), [("cpu", "usageCoreNanoSeconds"), ("memory", "usageBytes"), ("rootfs", "usedBytes"), ("logs", "usedBytes")])
def test_nested_private_values_in_counter_fields_are_rejected_before_publication(field, key):
    core, body = Core(), sample()
    body["pods"][0]["containers"][0][field][key] = {"secret": "must-not-persist"}
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    with pytest.raises(ObservationError, match="payload"):
        observer.collect_once(now=NOW)
    assert not core.records


def test_settings_rejection_does_not_echo_credentials():
    with pytest.raises(ValueError) as error:
        settings(kubernetes_endpoint="https://user:private-secret@api.test")
    assert "private-secret" not in str(error.value)


def test_original_filesystem_timestamp_is_not_refreshed_by_publication():
    core, body = Core(), sample()
    original = (NOW - timedelta(seconds=25)).isoformat()
    body["pods"][0]["containers"][0]["rootfs"]["time"] = original
    body["pods"][0]["ephemeral-storage"]["time"] = original
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    observer.collect_once(now=NOW)
    pod = published(core)["summary"]["pods"][0]
    assert pod["containers"][0]["rootfs"]["time"] == original
    assert pod["ephemeral-storage"]["time"] == original


def test_transport_retry_warnings_cannot_echo_endpoint_or_credentials(monkeypatch, caplog):
    import logging

    from loom_execution_actuator.node_observer import _quiet_transport_logs

    for name in ("urllib3", "kubernetes.client"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "handlers", list(logger.handlers))
        monkeypatch.setattr(logger, "propagate", logger.propagate)
    _quiet_transport_logs()
    logging.getLogger("urllib3.connectionpool").warning("private-endpoint-and-token")
    logging.getLogger("kubernetes.client.rest").warning("private-endpoint-and-token")
    assert "private-endpoint-and-token" not in caplog.text


@pytest.mark.parametrize("field", ["cpu", "memory", "rootfs", "logs", "ephemeral-storage"])
def test_missing_source_time_cannot_be_fabricated_from_publication_time(field):
    core, body = Core(), sample()
    pod = body["pods"][0]
    group = pod[field] if field == "ephemeral-storage" else pod["containers"][0][field]
    del group["time"]
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    with pytest.raises(ObservationError, match="payload"):
        observer.collect_once(now=NOW)
    assert not core.records


def test_creation_epoch_matches_old_kubelet_even_when_actual_start_is_later():
    created = (NOW - timedelta(seconds=70)).replace(microsecond=0)
    actual_started = NOW - timedelta(minutes=1)
    body = sample()
    container = body["pods"][0]["containers"][0]
    container["startTime"] = created.isoformat().replace("+00:00", "Z")
    container["_started_at"] = actual_started.isoformat()
    observer = NodeResourceObserver(settings(), Core())
    observer._qualify(body, observer._request([workload()]), NOW, NOW)
    assert container["startTime"] == created.isoformat().replace("+00:00", "Z")
    assert "_started_at" not in container


def test_old_and_observer_sources_share_creation_epoch_without_double_cpu():
    from loom.models.resource_usage import (
        ResourceCounters,
        TrialResourceUsageReport,
        aggregate_resource_usage,
    )
    from loom_execution_actuator.resource_usage import pod_samples

    created = (NOW - timedelta(seconds=70)).replace(microsecond=0)
    body = sample(cpu=200_000)
    container = body["pods"][0]["containers"][0]
    container["startTime"] = created.isoformat().replace("+00:00", "Z")
    container["_started_at"] = (NOW - timedelta(minutes=1)).isoformat()
    observer = NodeResourceObserver(settings(), Core())
    observer._qualify(body, observer._request([workload()]), NOW, NOW)
    current = pod_samples(body, namespace="execution", pod_uid=POD_UID)["execution"]
    old = TrialResourceUsageReport(trial_id=uuid4(), attempt_count=1,
        execution_lease_id=uuid4(), resource_generation=1, target_id="fixture", pod_uid=POD_UID,
        execution_key="a" * 64, container_role="controller", role_name="execution", backend="nebius_kubernetes",
        source="kubelet_summary", observation_seq=1, container_started_at=created,
        first_observed_at=NOW, last_observed_at=NOW, completeness="partial",
        counters=ResourceCounters(cpu_usage_usec=100))
    new = old.model_copy(update={"source": "native_node_observer", "execution_key": "b" * 64,
                                "container_started_at": current.started_at, "counters": current.counters})
    assert aggregate_resource_usage([old, new])["cpu_usage_usec"] == 200


@pytest.mark.parametrize("fault", ["missing_actual_start", "creation_after_start", "creation_subsecond", "zero_creation"])
def test_creation_epoch_and_actual_start_are_independently_qualified(fault):
    body = sample()
    container = body["pods"][0]["containers"][0]
    if fault == "missing_actual_start":
        del container["_started_at"]
    elif fault == "creation_after_start":
        container["startTime"] = NOW.isoformat()
    elif fault == "creation_subsecond":
        container["startTime"] = (NOW - timedelta(seconds=62, microseconds=1)).isoformat()
    else:
        container["startTime"] = "1970-01-01T00:00:00Z"
    core = Core()
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda *_: body
    with pytest.raises(ObservationError, match="identity"):
        observer.collect_once(now=NOW)
    assert not core.records


@pytest.mark.parametrize("explicit_origin", [None, "https://observer-api.test:6443"])
def test_incluster_api_requests_keep_the_selected_origin_through_repeated_token_refresh(
    tmp_path, monkeypatch, explicit_origin,
):
    from kubernetes import client, config
    from kubernetes.config.incluster_config import InClusterConfigLoader

    from loom_execution_actuator import node_observer

    token, ca = tmp_path / "token", tmp_path / "ca.crt"
    token.write_text("fixture-token-0")
    ca.write_text("fixture-ca")
    environ = {"KUBERNETES_SERVICE_HOST": "127.0.0.2", "KUBERNETES_SERVICE_PORT": "443"}
    loader = InClusterConfigLoader(str(token), str(ca), environ=environ)
    monkeypatch.setattr(client.Configuration, "_default", None)
    monkeypatch.setattr(config, "load_incluster_config", loader.load_and_set)
    monkeypatch.setattr(node_observer, "ObserverSettings", lambda: settings(kubernetes_endpoint=explicit_origin))
    monkeypatch.setattr(node_observer, "_quiet_transport_logs", lambda: None)
    requests = []

    def request(api, method, url, **kwargs):
        requests.append((url, kwargs["headers"]["authorization"],
            api.configuration.ssl_ca_cert, api.configuration.verify_ssl))
        return S(status=200, getheaders=lambda: {})

    monkeypatch.setattr(client.ApiClient, "request", request)

    def collect(observer):
        api = observer.core.api_client
        for index in range(3):
            if index:
                token.write_text("fixture-token-" + str(index))
                loader.token_expires_at = datetime.now() - timedelta(minutes=1)
            api.call_api("/api/v1/nodes/node-1", "GET", auth_settings=["BearerToken"],
                _preload_content=False, _return_http_data_only=True)
            # The real loader replaces its hook even without a token rotation.
            api.configuration.auth_settings()
        raise SystemExit

    monkeypatch.setattr(node_observer.NodeResourceObserver, "collect_once", collect)
    with pytest.raises(SystemExit):
        node_observer.main()
    expected = explicit_origin or "https://127.0.0.2:443"
    assert requests == [(expected + "/api/v1/nodes/node-1", "bearer fixture-token-" + str(index),
        str(ca), True) for index in range(3)]
    assert environ == {"KUBERNETES_SERVICE_HOST": "127.0.0.2", "KUBERNETES_SERVICE_PORT": "443"}


def test_incluster_refresh_errors_propagate_and_the_origin_hook_survives(tmp_path):
    from kubernetes import client
    from kubernetes.config.config_exception import ConfigException
    from kubernetes.config.incluster_config import InClusterConfigLoader

    from loom_execution_actuator.node_observer import _preserve_api_origin

    token, ca = tmp_path / "token", tmp_path / "ca.crt"
    token.write_text("fixture-token")
    ca.write_text("fixture-ca")
    loader = InClusterConfigLoader(str(token), str(ca), environ={
        "KUBERNETES_SERVICE_HOST": "127.0.0.2", "KUBERNETES_SERVICE_PORT": "443"})
    configuration = client.Configuration()
    loader.load_and_set(configuration)
    endpoint = settings(kubernetes_endpoint="https://observer-api.test:6443").kubernetes_endpoint
    _preserve_api_origin(configuration, endpoint)
    wrapped_refresh = configuration.refresh_api_key_hook
    token.write_text("")
    loader.token_expires_at = datetime.now() - timedelta(minutes=1)
    with pytest.raises(ConfigException):
        configuration.auth_settings()
    assert configuration.host == endpoint
    assert configuration.refresh_api_key_hook is wrapped_refresh
    assert configuration.ssl_ca_cert == str(ca) and configuration.verify_ssl is True
    token.write_text("fixture-refreshed-token")
    assert configuration.auth_settings()["BearerToken"]["value"] == "bearer fixture-refreshed-token"
    assert configuration.host == endpoint
    assert configuration.refresh_api_key_hook is wrapped_refresh
