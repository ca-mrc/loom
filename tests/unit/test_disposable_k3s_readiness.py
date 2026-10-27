"""Disposable-cluster setup observes actual prerequisites, not discovery alone."""

import json
import ssl
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, ProtocolError, ReadTimeoutError, SSLError

from tests.integration import test_execution_actuator_k3s as fixture
from tests.integration.test_execution_actuator_k3s import _wait_for_dns_pods


@pytest.fixture
def pod_http_api():
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    state = SimpleNamespace(delay=0, status=200, requests=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state.requests.append((self.path, self.headers.get("Impersonate-User"),
                                   self.headers.get("Cookie")))
            if "/events" in self.path:
                status, body = 200, {"apiVersion": "v1", "kind": "EventList", "items": []}
            else:
                time.sleep(state.delay)
                status = state.status
                body = {"apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": "dns-fixture", "namespace": "test"},
                        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            if status == 429:
                self.send_header("Retry-After", "600")
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    configuration = client.Configuration(host=f"http://127.0.0.1:{server.server_port}")
    with client.ApiClient(configuration=configuration) as api_client:
        try:
            yield client.CoreV1Api(api_client), state
        finally:
            api_client.rest_client.pool_manager.pools.dispose_func = lambda pool: pool.close()
            api_client.rest_client.pool_manager.clear()
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)


@pytest.mark.timeout(3)
def test_pod_sdk_read_cannot_outlive_remaining_readiness_budget(pod_http_api, monkeypatch):
    import time

    core, state = pod_http_api
    state.delay = 0.4
    started = time.monotonic()
    first = True

    def clock():
        nonlocal first
        if first:
            first = False
            return 0.0
        return 59.9 + time.monotonic() - started

    # Only the fixture clock moves near its original 60s deadline. The SDK's
    # socket clock stays real, so this exercises an actual stalled HTTP read.
    monkeypatch.setattr(fixture, "time", SimpleNamespace(monotonic=clock, sleep=time.sleep))
    with pytest.raises(AssertionError, match="did not become ready"):
        fixture._wait_for_pod(core, "test", "dns-fixture")
    assert time.monotonic() - started < 0.3
    assert core.api_client.configuration.retries is None


def test_pod_readiness_does_not_sleep_for_sdk_retry_after(pod_http_api, monkeypatch):
    from urllib3.util.retry import Retry

    core, state = pod_http_api
    state.status = 429

    def unexpected_retry(*args, **kwargs):
        pytest.fail("SDK retry would escape the readiness deadline")

    monkeypatch.setattr(Retry, "sleep", unexpected_retry)
    with pytest.raises(ApiException) as raised:
        fixture._wait_for_pod(core, "test", "dns-fixture")
    assert raised.value.status == 429
    assert len(state.requests) == 1
    assert core.api_client.configuration.retries is None


def test_pod_readiness_preserves_scoped_headers_and_ready_assertion(pod_http_api):
    core, state = pod_http_api
    core.api_client.default_headers["Impersonate-User"] = "fixture-reader"
    core.api_client.cookie = "session=fixture-only"
    assert fixture._wait_for_pod(core, "test", "dns-fixture").status.phase == "Running"
    assert state.requests == [("/api/v1/namespaces/test/pods/dns-fixture",
                               "fixture-reader", "session=fixture-only")]
    assert core.api_client.configuration.retries is None


def test_network_fixture_builds_only_its_consumed_image_and_reuses_each_once(monkeypatch):
    builds, removed = [], []
    monkeypatch.setattr(fixture, "_docker_platform", lambda: "linux/amd64")
    monkeypatch.setattr(fixture, "_build_image", lambda **kwargs: builds.append(kwargs))
    monkeypatch.setattr(fixture, "subprocess", SimpleNamespace(
        run=lambda args, **kwargs: removed.append(args[-1])))
    prepared = fixture.built_images.__wrapped__()
    try:
        images = next(prepared)
        network_tag = images.fixture
        assert [b["dockerfile"] for b in builds] == ["tests/fixtures/execution_runtime_fixture/Dockerfile"]
        assert images.fixture == network_tag and len(builds) == 1
        runtime_tag = images.runtime
        assert [b["dockerfile"] for b in builds] == [
            "tests/fixtures/execution_runtime_fixture/Dockerfile", "deploy/Dockerfile.execution-runtime"]
        assert images.runtime == runtime_tag and len(builds) == 2
        assert all(b["platform"] == "linux/amd64" for b in builds)
    finally:
        prepared.close()
    assert set(removed) == {network_tag, runtime_tag}


def test_failed_image_build_is_not_retried_and_owned_tag_is_cleaned(monkeypatch):
    original = RuntimeError("fixture build failed")
    builds, removed = [], []
    monkeypatch.setattr(fixture, "_docker_platform", lambda: "linux/amd64")

    def fail(**kwargs):
        builds.append(kwargs["tag"])
        raise original

    monkeypatch.setattr(fixture, "_build_image", fail)
    monkeypatch.setattr(fixture, "subprocess", SimpleNamespace(run=lambda args, **kwargs: removed.append(args[-1])))
    prepared = fixture.built_images.__wrapped__()
    images = next(prepared)
    try:
        for _ in range(2):
            with pytest.raises(RuntimeError) as caught:
                _ = images.runtime
            assert caught.value is original
    finally:
        prepared.close()
    assert len(builds) == 1 and removed == builds


@pytest.mark.parametrize("transient", [
    MaxRetryError(None, "/pods", SSLError(ssl.SSLEOFError("fixture EOF"))),
    ReadTimeoutError(None, "/pods", "fixture stalled read"),
])
def test_pod_setup_retries_only_observed_transient_reads_on_private_transport(monkeypatch, transient):
    calls = []
    pod = client.V1Pod(status=client.V1PodStatus(phase="Running", conditions=[
        client.V1PodCondition(type="Ready", status="True")]))

    def read(observer, name, namespace, **kwargs):
        assert observer.api_client.configuration.retries == 0
        assert name == "dns-fixture" and namespace == "test"
        assert all(0 < value <= 2.5 for value in kwargs["_request_timeout"])
        calls.append(1)
        if len(calls) == 1:
            raise transient
        return pod

    monkeypatch.setattr(client.CoreV1Api, "read_namespaced_pod", read)
    configuration = client.Configuration(host="http://127.0.0.1:1")
    with client.ApiClient(configuration=configuration) as api_client:
        core = client.CoreV1Api(api_client)
        assert fixture._wait_for_pod(core, "test", "dns-fixture") is pod
        assert configuration.retries is None and len(calls) == 2


@pytest.mark.parametrize("error", [
    ApiException(status=403, reason="Forbidden"),
    MaxRetryError(None, "/pods", SSLError(ssl.SSLCertVerificationError("bad certificate"))),
    MaxRetryError(None, "/pods", ProtocolError("connection lost")),
])
def test_pod_setup_does_not_retry_authority_certificate_or_unknown_transport_failures(monkeypatch, error):
    calls = []

    def read(observer, *args, **kwargs):
        calls.append(1)
        raise error

    monkeypatch.setattr(client.CoreV1Api, "read_namespaced_pod", read)
    configuration = client.Configuration(host="http://127.0.0.1:1")
    with client.ApiClient(configuration=configuration) as api_client:
        with pytest.raises(type(error)) as caught:
            fixture._wait_for_pod(client.CoreV1Api(api_client), "test", "dns-fixture")
        assert caught.value is error and calls == [1] and configuration.retries is None


def test_build_timings_keep_stage_cache_and_cpu_scope_without_raw_output(monkeypatch, tmp_path):
    secret = "private-build-credential"
    log = (f"#1 [build  9/11] RUN {secret}\n#1 DONE 2.5s\n"
           f"#2 [guest-build 4/4] RUN https://{secret}/archive\n#2 CACHED\n{secret}\n"
           f"#3 [internal] load metadata for https://{secret}/image\n#3 DONE 120.0s\n"
           "#4 exporting to image\n#4 DONE 0.2s\n")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setattr(fixture, "subprocess", SimpleNamespace(
        run=lambda *args, **kwargs: SimpleNamespace(stderr=log)))
    fixture._build_image(tag="fixture-only", dockerfile="deploy/Dockerfile.execution-runtime", platform="linux/amd64")
    text = (tmp_path / "loom-k3s-image-build-timings.jsonl").read_text()
    assert secret not in text and "https://" not in text
    start, end = map(json.loads, text.splitlines())
    assert start["event"] == "start" and end["status"] == "success"
    assert end["stages"] == [
        {"stage": "build", "index": 9, "instruction": "RUN", "cached": False, "wall_seconds": 2.5},
        {"stage": "guest-build", "index": 4, "instruction": "RUN", "cached": True},
        {"phase": "load metadata", "cached": False, "wall_seconds": 120.0},
        {"phase": "exporting to image", "cached": False, "wall_seconds": 0.2}]
    assert "excludes build daemon" in end["cpu_scope"]


def test_database_startup_failure_keeps_evidence_before_node_cleanup(monkeypatch, tmp_path):
    from tests.cluster import test_nebius_pool_database_guard as database

    original = AssertionError("database did not become ready")
    events = []
    container = SimpleNamespace(
        get_wrapped_container=lambda: SimpleNamespace(id="disposable-database"),
        stop=lambda: events.append("stopped"),
    )
    monkeypatch.setattr(database, "_docker", lambda *args: "")
    monkeypatch.setattr(database, "_start_k3s", lambda **kwargs: container)

    def fail_startup(*args, **kwargs):
        raise original

    def diagnostic(args, **kwargs):
        if args[:3] == ["docker", "image", "rm"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        assert "stopped" not in events, "startup evidence was read after node teardown"
        events.append(args)
        return SimpleNamespace(returncode=0, stdout="database startup evidence", stderr="")

    monkeypatch.setattr(database, "_load_client", fail_startup)
    monkeypatch.setattr(database.subprocess, "run", diagnostic)
    request = SimpleNamespace(guards=[SimpleNamespace(namespace="disposable-database")])
    with pytest.raises(AssertionError) as caught:
        database.test_actual_database_observer_works_without_control_plane(
            (request, None, None, None), None, tmp_path, monkeypatch)
    assert caught.value is original
    assert "database startup evidence" in "\n".join(getattr(original, "__notes__", ()))
    assert "stopped" in events


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
    core = SimpleNamespace(get_api_resources=lambda **kwargs: object(),
        api_client=SimpleNamespace(configuration=client.Configuration(host="http://127.0.0.1:1")))
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
    core.read_namespaced_pod = lambda name, namespace, **kwargs: pod
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
def test_pod_timeout_retains_bounded_sandbox_and_pull_events(loading_client, monkeypatch, events_unavailable):
    from kubernetes.client.api.core_v1_api import CoreV1Api

    _, core, _, clock = loading_client
    core.api_client = SimpleNamespace(configuration=client.Configuration())
    pod = client.V1Pod(metadata=client.V1ObjectMeta(name="broker", uid="current-pod"),
        status=client.V1PodStatus(phase="Pending"))
    core.read_namespaced_pod = lambda name, namespace, **kwargs: pod
    timestamp = datetime(2026, 10, 20, tzinfo=UTC)

    def events(api, namespace, **kwargs):
        assert namespace == "test"
        assert api.api_client.configuration.retries == 0
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

    monkeypatch.setattr(CoreV1Api, "list_namespaced_event", events)
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
    core.read_namespaced_pod = lambda name, namespace, **kwargs: pod

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
    core.read_namespaced_pod = lambda name, namespace, **kwargs: pod
    requests = []

    def throttled(pool, connection, method, url, **kwargs):
        requests.append((pool, url))
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
        assert requests[0][0].pool is None, "failed diagnostics retained their HTTP connection pool"


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
