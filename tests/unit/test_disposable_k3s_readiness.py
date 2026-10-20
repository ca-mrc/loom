"""Disposable-cluster setup observes actual prerequisites, not discovery alone."""

import json
import ssl
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, ProtocolError, SSLError

from tests.integration import test_execution_actuator_k3s as fixture
from tests.integration.test_execution_actuator_k3s import _wait_for_dns_pods


def test_addon_failure_diagnostics_distinguish_sandbox_readiness_and_image_pull():
    from kubernetes import client

    pod = client.V1Pod(metadata=client.V1ObjectMeta(name="coredns-fixture"),
        status=client.V1PodStatus(phase="Pending", pod_ip="10.42.0.2",
            conditions=[client.V1PodCondition(type="PodReadyToStartContainers", status="True")]))
    timestamp = datetime(2026, 10, 20, tzinfo=UTC)
    event = client.CoreV1Event(metadata=client.V1ObjectMeta(),
        involved_object=client.V1ObjectReference(name="coredns-fixture", kind="Pod"),
        type="Normal", reason="Pulling", message="Pulling addon image", count=1,
        first_timestamp=timestamp, last_timestamp=timestamp)

    def events(namespace, **kwargs):
        assert namespace == "kube-system" and kwargs["limit"] <= 64
        assert kwargs["_request_timeout"] == (2, 3)
        return client.CoreV1EventList(items=[] if kwargs.get("field_selector") == "type=Warning" else [event])

    core = SimpleNamespace(list_namespaced_pod=lambda *args, **kwargs: client.V1PodList(items=[pod]),
        list_namespaced_event=events)
    result = json.loads(fixture._native_addon_diagnostics(core, ("dns",)))
    assert result["pods"][0]["pod_ip"] == "10.42.0.2"
    assert result["pods"][0]["conditions"] == [{"type": "PodReadyToStartContainers", "status": "True"}]
    assert result["events"][0]["reason"] == "Pulling"
    assert result["events"][0]["last_timestamp"] == timestamp.isoformat()


@pytest.fixture
def loading_client(monkeypatch):
    from kubernetes import client, config

    clock = [0.0]
    monkeypatch.setattr(fixture.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(fixture.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(config, "load_kube_config_from_dict", lambda payload: None)
    core = SimpleNamespace(get_api_resources=lambda **kwargs: object())
    core.read_namespaced_service = lambda *args, **kwargs: SimpleNamespace(spec=SimpleNamespace(cluster_ip="10.43.0.1"))
    batch = object()
    monkeypatch.setattr(client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(client, "BatchV1Api", lambda: batch)
    container = SimpleNamespace(
        exec=lambda command: SimpleNamespace(exit_code=0, output=b"{}"),
        get_logs=lambda: (b"", b'Starting k3s v1.36.1+k3s1'),
        get_exposed_port=lambda port: "16443",
    )
    return container, core, batch, clock


def test_client_does_not_exec_during_cgroup_initialization(loading_client):
    container, core, batch, clock = loading_client
    core.read_namespace = lambda *args, **kwargs: object()
    initialized = False

    def logs():
        nonlocal initialized
        initialized = clock[0] > 0
        return b"", b"Starting k3s v1.36.1+k3s1" if initialized else b""

    def execute(command):
        # An exec process in the root cgroup prevents K3s from enabling the
        # subtree controllers. Reading Docker logs has no such side effect.
        assert initialized, "exec would race root cgroup initialization"
        return SimpleNamespace(exit_code=0, output=b"{}")

    container.get_logs, container.exec = logs, execute
    assert fixture._load_client(container)[1:] == (core, batch)


def test_client_bootstrap_wait_has_a_deadline_without_exec(loading_client):
    container, core, _, clock = loading_client
    core.read_namespace = lambda *args, **kwargs: object()
    container.get_logs = lambda: (b"", b"not initialized")
    executions = []
    container.exec = lambda command: executions.append(command) or SimpleNamespace(exit_code=0, output=b"{}")
    with pytest.raises(AssertionError, match="bootstrap"):
        fixture._load_client(container)
    assert not executions and clock[0] == 90


def test_client_discovery_waits_for_system_namespace(loading_client):
    container, core, batch, clock = loading_client
    reads = []

    def read(name, *, _request_timeout):
        assert name == "kube-system" and 0 < _request_timeout <= 5
        reads.append(name)
        if len(reads) == 1:
            raise ApiException(status=404, reason="namespace bootstrap pending")
        return SimpleNamespace(metadata=SimpleNamespace(uid="cluster-identity"))

    core.read_namespace = read
    assert fixture._load_client(container)[1:] == (core, batch)
    assert len(reads) == 2 and clock[0] > 0


def test_client_missing_system_namespace_has_bounded_wait(loading_client):
    container, core, _, clock = loading_client

    def read(*args, **kwargs):
        raise ApiException(status=404, reason="namespace bootstrap pending")

    core.read_namespace = read
    with pytest.raises(AssertionError, match="kube-system"):
        fixture._load_client(container)
    assert clock[0] == 90


@pytest.mark.parametrize("pending", ["absent", "unallocated"])
def test_client_waits_for_bootstrap_service_allocation(loading_client, pending):
    container, core, batch, clock = loading_client
    core.read_namespace = lambda *args, **kwargs: object()
    reads = []

    def service(name, namespace, *, _request_timeout):
        assert (name, namespace) == ("kubernetes", "default") and 0 < _request_timeout <= 5
        reads.append(1)
        if len(reads) == 1:
            if pending == "absent":
                raise ApiException(status=404, reason="service allocator bootstrap pending")
            return SimpleNamespace(spec=SimpleNamespace(cluster_ip=None))
        return SimpleNamespace(spec=SimpleNamespace(cluster_ip="10.43.0.1"))

    core.read_namespaced_service = service
    assert fixture._load_client(container)[1:] == (core, batch)
    assert len(reads) == 2 and clock[0] > 0


def test_client_missing_bootstrap_service_has_bounded_wait(loading_client):
    container, core, _, clock = loading_client
    core.read_namespace = lambda *args, **kwargs: object()

    def missing(*args, **kwargs):
        raise ApiException(status=404, reason="service allocator bootstrap pending")

    core.read_namespaced_service = missing
    with pytest.raises(AssertionError, match="Service"):
        fixture._load_client(container)
    assert clock[0] == 90


@pytest.mark.parametrize("status", [403, 500])
def test_bootstrap_service_observation_does_not_retry_other_errors(loading_client, status):
    container, core, _, clock = loading_client
    core.read_namespace = lambda *args, **kwargs: object()
    error = ApiException(status=status, reason="not a missing service")

    def failed(*args, **kwargs):
        raise error

    core.read_namespaced_service = failed
    with pytest.raises(ApiException) as caught:
        fixture._load_client(container)
    assert caught.value is error and clock[0] == 0


@pytest.mark.parametrize("status", [403, 500])
def test_client_system_namespace_does_not_retry_other_errors(loading_client, status):
    container, core, _, clock = loading_client
    error = ApiException(status=status, reason="not a missing namespace")

    def read(*args, **kwargs):
        raise error

    core.read_namespace = read
    with pytest.raises(ApiException) as raised:
        fixture._load_client(container)
    assert raised.value is error and clock[0] == 0


def _tls_eof():
    return MaxRetryError(None, "/api/v1/namespaces/kube-system/pods", SSLError(
        ssl.SSLEOFError(8, "EOF occurred in violation of protocol"),
    ))


async def test_dns_readiness_survives_tls_eof_then_waits_for_a_real_pod():
    pod = object()
    responses = iter((_tls_eof(), [], [pod]))
    calls = []

    def read(namespace, *, label_selector, **kwargs):
        calls.append((namespace, label_selector))
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return SimpleNamespace(items=response)

    assert await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read)) == [pod]
    assert calls == [("kube-system", "k8s-app=kube-dns")] * 3


async def test_persistent_tls_eof_fails_with_original_cause_by_setup_deadline():
    error = _tls_eof()

    def read(*args, **kwargs):
        raise error

    with pytest.raises(AssertionError, match="CoreDNS") as raised:
        await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read), timeout=0.01)
    assert raised.value.__cause__ is error


@pytest.mark.parametrize("error", [
    ApiException(status=403, reason="Forbidden"),
    MaxRetryError(None, "/pods", SSLError(ssl.SSLCertVerificationError("bad certificate"))),
    MaxRetryError(None, "/pods", ProtocolError("connection lost")),
])
async def test_dns_readiness_does_not_retry_authority_or_other_transport_failures(error):
    calls = []

    def read(*args, **kwargs):
        calls.append(1)
        raise error

    with pytest.raises(type(error)) as raised:
        await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read))
    assert raised.value is error
    assert len(calls) == 1
