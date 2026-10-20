"""Disposable-cluster setup observes actual prerequisites, not discovery alone."""

import json
import ssl
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kubernetes import client
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
    core_api = client.CoreV1Api
    monkeypatch.setattr(client, "CoreV1Api", lambda api_client=None: core if api_client is None else core_api(api_client))
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


def test_pod_timeout_reports_container_states_without_dumping_workload_data(loading_client):
    _, core, _, clock = loading_client
    terminated = client.V1ContainerStateTerminated(
        exit_code=137, reason="OOMKilled", signal=9, message="private termination output",
    )
    pod = client.V1Pod(
        spec=client.V1PodSpec(containers=[client.V1Container(
            name="tls", env=[client.V1EnvVar(name="TOKEN", value="private env value")],
        )]),
        status=client.V1PodStatus(phase="Pending", container_statuses=[client.V1ContainerStatus(
            name="tls", image="fixture", image_id="fixture", ready=False, restart_count=2,
            state=client.V1ContainerState(waiting=client.V1ContainerStateWaiting(
                reason="ImagePullBackOff", message="private registry detail",
            )),
            last_state=client.V1ContainerState(terminated=terminated),
        )], init_container_statuses=[client.V1ContainerStatus(
            name="init", image="fixture", image_id="fixture", ready=False, restart_count=1,
            state=client.V1ContainerState(terminated=terminated),
        )]),
    )
    core.read_namespaced_pod = lambda name, namespace: pod
    with pytest.raises(AssertionError, match="did not become ready") as caught:
        fixture._wait_for_pod(core, "test", "ingress")
    diagnostic = str(caught.value)
    for detail in ("tls", "init", "ImagePullBackOff", "OOMKilled", "137", "restart_count", "last_state"):
        assert detail in diagnostic
    assert "private" not in diagnostic
    assert clock[0] == 60
    terminated.reason = "x" * 10000
    pod.status.container_statuses[0].state.waiting.reason = "x" * 10000
    pod.status.container_statuses *= 100
    pod.status.init_container_statuses *= 100
    bounded = fixture._pod_container_details(pod)
    assert len(bounded) <= 4096 and bounded.endswith("... (truncated)")


@pytest.mark.parametrize("events_unavailable", [False, True])
def test_pod_timeout_retains_bounded_sandbox_and_pull_events(loading_client, events_unavailable):
    _, core, _, clock = loading_client
    pod = client.V1Pod(metadata=client.V1ObjectMeta(name="broker", uid="current-pod"),
        status=client.V1PodStatus(phase="Pending"))
    core.read_namespaced_pod = lambda name, namespace: pod
    timestamp = datetime(2026, 10, 20, tzinfo=UTC)

    def events(namespace, **kwargs):
        assert namespace == "test"
        assert kwargs == {"limit": 32, "_request_timeout": (2, 3),
            "field_selector": "involvedObject.kind=Pod,involvedObject.uid=current-pod"}
        if events_unavailable:
            raise ApiException(status=500, reason="private response body")
        return client.CoreV1EventList(items=[client.CoreV1Event(
            metadata=client.V1ObjectMeta(annotations={"private": "private annotation"}),
            involved_object=client.V1ObjectReference(kind="Pod", name="broker", uid="current-pod"),
            type=event_type, reason=reason, message=message, count=3,
            first_timestamp=timestamp, last_timestamp=timestamp,
        ) for event_type, reason, message in (
            ("Warning", "FailedCreatePodSandBox", "flannel subnet.env unavailable"),
            ("Normal", "Pulling", "Pulling sandbox image"),
            ("Normal", "Pulled", "x" * 100000),
        )])

    core.list_namespaced_event = events
    with pytest.raises(AssertionError, match="did not become ready: Pending") as caught:
        fixture._wait_for_pod(core, "test", "broker")
    notes = "\n".join(caught.value.__notes__)
    assert clock[0] == 60 and len(notes) <= 16400
    assert "private" not in notes
    if events_unavailable:
        assert "ApiException" in notes
    else:
        for detail in ("FailedCreatePodSandBox", "flannel subnet.env unavailable", "Pulling sandbox image",
                       timestamp.isoformat(), '"count": 3'):
            assert detail in notes


def test_ready_pod_does_not_build_failure_diagnostics(loading_client, monkeypatch):
    _, core, _, clock = loading_client
    pod = client.V1Pod(status=client.V1PodStatus(phase="Running", conditions=[
        client.V1PodCondition(type="Ready", status="True"),
    ]))
    core.read_namespaced_pod = lambda name, namespace: pod

    def unexpected(*args, **kwargs):
        pytest.fail("successful readiness must not collect failure diagnostics")

    monkeypatch.setattr(fixture, "_pod_container_details", unexpected)
    core.list_namespaced_event = unexpected
    assert fixture._wait_for_pod(core, "test", "ingress") is pod
    assert clock[0] == 0


def test_pod_failure_diagnostics_do_not_honor_retry_after_or_change_readiness_client(loading_client, monkeypatch):
    from urllib3.connectionpool import HTTPConnectionPool
    from urllib3.response import HTTPResponse

    _, core, _, clock = loading_client
    pod = client.V1Pod(metadata=client.V1ObjectMeta(name="broker", uid="current-pod"),
        status=client.V1PodStatus(phase="Pending"))
    core.read_namespaced_pod = lambda name, namespace: pod
    requests = []

    def throttled(pool, connection, method, url, **kwargs):
        requests.append(url)
        return HTTPResponse(body=b"{}", status=429, headers={"Retry-After": "600"})

    monkeypatch.setattr(HTTPConnectionPool, "_make_request", throttled)
    configuration = client.Configuration(host="http://127.0.0.1:1")
    with client.ApiClient(configuration=configuration) as api_client:
        core.api_client = api_client
        core.list_namespaced_event = client.CoreV1Api(api_client).list_namespaced_event
        with pytest.raises(AssertionError, match="did not become ready: Pending"):
            fixture._wait_for_pod(core, "test", "broker")
        assert len(requests) == 1 and clock[0] == 60
        assert configuration.retries is None


def test_policy_diagnostics_identify_missing_status_and_stale_generation():
    from tests.cluster import test_nebius_application_authority as authority

    policies = [
        client.V1ValidatingAdmissionPolicy(metadata=client.V1ObjectMeta(name="missing", generation=1)),
        client.V1ValidatingAdmissionPolicy(
            metadata=client.V1ObjectMeta(name="stale", generation=2),
            status=client.V1ValidatingAdmissionPolicyStatus(
                observed_generation=1,
                conditions=[client.V1Condition(type="Ready", status="False", reason="TypeChecking", message="private",
                                               last_transition_time="2026-10-20T17:00:00Z")],
                type_checking=client.V1TypeChecking(expression_warnings=[
                    client.V1ExpressionWarning(field_ref="spec.validations[0].expression", warning="private expression"),
                ]),
            ),
        ),
    ]
    diagnostic = authority._policy_typecheck_details(policies)
    for detail in ("missing", "stale", "generation", "observed_generation", "TypeChecking", "spec.validations[0]"):
        assert detail in diagnostic
    assert "'status_present': False" in diagnostic
    assert "'generation': 2" in diagnostic and "'observed_generation': 1" in diagnostic
    assert "private" not in diagnostic
    policies[1].status.type_checking.expression_warnings[0].field_ref = "x" * 10000
    bounded = authority._policy_typecheck_details(policies * 100)
    assert len(bounded) <= 4096 and bounded.endswith("... (truncated)")
