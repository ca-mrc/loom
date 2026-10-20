"""Remote clients preserve target/CA and delegate renewal to the native SDK."""

import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    NebiusKubernetesCredentials,
    connection_from_fields,
    create_api_client,
    read_kubelet_summary,
)
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader


@pytest.fixture
def connection(tmp_path: Path) -> NebiusKubernetesConnection:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-cluster-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    ca = tmp_path / "ca.crt"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}")
    credentials.chmod(0o440)
    return NebiusKubernetesConnection(
        endpoint="https://regional-api.example:443", ca_file=ca, credentials_file=credentials
    )


class FakeSDK:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False
        self.expiration = datetime.now(UTC) + timedelta(hours=1)
        self.failure = False

    def get_token_sync(self, *, timeout: float) -> Any:
        assert timeout == 30
        self.calls += 1
        if self.failure:
            raise TimeoutError("renewal unavailable")
        return SimpleNamespace(token=f"ephemeral-{self.calls}", expiration=self.expiration)

    async def get_token(self, *, timeout: float) -> Any:
        return self.get_token_sync(timeout=timeout)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> FakeSDK:
    import nebius.sdk

    instance = FakeSDK()
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: instance)
    return instance


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "https://",
        "http://regional-api.example",
        "https://u:p@host",
        "https://host/path",
        "https://host?x=1",
        "https://host#fragment",
        "https://host:bad",
        "https://ho st",
        "https://host\\evil",
    ],
)
def test_remote_origin_rejects_ambiguous_or_insecure_target(endpoint: str) -> None:
    with pytest.raises(ValueError, match="HTTPS origin"):
        connection_from_fields(endpoint, Path("ca"), Path("credential"))


@pytest.mark.parametrize(
    "present",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
    ],
)
def test_partial_configuration_cannot_select_local_cluster(present: tuple[bool, ...]) -> None:
    with pytest.raises(ValueError, match="set together"):
        connection_from_fields(
            "https://host" if present[0] else None,
            Path("ca") if present[1] else None,
            Path("credential") if present[2] else None,
        )
    assert connection_from_fields(None, None, None) is None


@pytest.mark.asyncio
async def test_native_hook_refresh_and_async_gateway_interface(
    connection: NebiusKubernetesConnection, sdk: FakeSDK
) -> None:
    api, credentials = create_api_client(connection)
    try:
        cfg = api.configuration
        assert cfg.host == connection.endpoint
        assert cfg.ssl_ca_cert == str(connection.ca_file)
        assert cfg.verify_ssl is True
        assert cfg.get_api_key_with_prefix("authorization") == "Bearer ephemeral-1"
        assert cfg.get_api_key_with_prefix("authorization") == "Bearer ephemeral-2"
        assert await credentials.get_token() == "ephemeral-3"
        sdk.failure = True
        with pytest.raises(TimeoutError):
            cfg.get_api_key_with_prefix("authorization")
    finally:
        api.close()
        await credentials.close()
    assert sdk.closed


@pytest.mark.asyncio
async def test_expired_native_token_never_enters_request(
    connection: NebiusKubernetesConnection, sdk: FakeSDK
) -> None:
    credentials = NebiusKubernetesCredentials(connection)
    sdk.expiration = datetime.now(UTC) - timedelta(seconds=1)
    try:
        with pytest.raises(RuntimeError, match="expired"):
            credentials.get_token_sync()
        with pytest.raises(RuntimeError, match="expired"):
            await credentials.get_token()
    finally:
        await credentials.close()


@pytest.mark.parametrize(
    "reader_type", [InClusterKubernetesJobApi, InClusterKubernetesCapacityReader]
)
@pytest.mark.asyncio
async def test_remote_clients_share_explicit_target_without_local_loader(
    connection: NebiusKubernetesConnection,
    sdk: FakeSDK,
    reader_type: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kubernetes import config

    def forbidden() -> None:
        pytest.fail("remote mode must never load local credentials")

    monkeypatch.setattr(config, "load_incluster_config", forbidden)
    reader = reader_type(connection=connection)
    try:
        core = reader._core.api_client
        other = (
            reader._batch.api_client
            if isinstance(reader, InClusterKubernetesJobApi)
            else reader._apps.api_client
        )
        assert core is other
        assert core.configuration.host == connection.endpoint
        assert core.configuration.get_api_key_with_prefix("authorization") == "Bearer ephemeral-1"
    finally:
        await reader.close()
    assert sdk.closed


def _kubelet_core(connection, read, auth=None):
    node = SimpleNamespace(
        metadata=SimpleNamespace(name="node-1", uid="aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa",
                                 deletion_timestamp=None),
        status=SimpleNamespace(addresses=[SimpleNamespace(type="InternalIP", address="10.23.0.4")]),
    )
    configuration = SimpleNamespace(
        verify_ssl=True, ssl_ca_cert=str(connection.ca_file),
        auth_settings=auth or (lambda: {"BearerToken": {"value": "Bearer fixture-runtime-token"}}),
    )

    def read_node(**kwargs):
        read(kwargs["_request_timeout"])
        return node

    return SimpleNamespace(read_node=read_node, api_client=SimpleNamespace(configuration=configuration))


def test_kubelet_optional_deadline_shares_remaining_time_across_sdk_auth_and_http(
    connection, monkeypatch,
):
    import httpx

    import loom.nebius_kubernetes as module

    clock, sdk_timeouts, http_timeouts = [0.0], [], []

    def read(timeout):
        sdk_timeouts.append(timeout)
        clock[0] += 2

    def auth():
        clock[0] += 1
        return {"BearerToken": {"value": "Bearer fixture-runtime-token"}}

    def request(value):
        assert str(value.url) == "https://10.23.0.4:10250/stats/summary"
        assert value.headers["Authorization"] == "Bearer fixture-runtime-token"
        http_timeouts.append(value.extensions["timeout"]["read"])
        clock[0] += 1
        return httpx.Response(200, json={"node": {"nodeName": "node-1"}})

    original = httpx.Client
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(request), **kwargs,
    ))
    result = read_kubelet_summary(_kubelet_core(connection, read, auth), node_name="node-1",
                                  timeout_seconds=5)
    assert result["node"]["nodeName"] == "node-1"
    assert sdk_timeouts == [5] and http_timeouts == [2]


@pytest.mark.parametrize("timeout", [0, -1, 10.01, float("nan"), float("inf"), True])
def test_kubelet_optional_deadline_rejects_invalid_or_extended_timeouts(timeout):
    with pytest.raises(ValueError, match="positive and at most ten seconds"):
        read_kubelet_summary(SimpleNamespace(), node_name="node-1", timeout_seconds=timeout)


@pytest.mark.parametrize("stage", ["node", "auth"])
def test_kubelet_expired_deadline_never_starts_another_network_stage(connection, monkeypatch, stage):
    import httpx

    import loom.nebius_kubernetes as module

    clock = [0.0]

    def read(_timeout):
        if stage == "node":
            clock[0] = 6

    def auth():
        clock[0] = 6
        return {"BearerToken": {"value": "Bearer fixture-runtime-token"}}

    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(httpx, "Client", lambda **_: pytest.fail("deadline reached kubelet"))
    with pytest.raises(TimeoutError, match="observation deadline exceeded"):
        read_kubelet_summary(_kubelet_core(connection, read, auth), node_name="node-1",
                              timeout_seconds=5)


def test_kubelet_stream_trickle_cannot_renew_deadline_and_response_is_closed(connection, monkeypatch):
    import httpx

    import loom.nebius_kubernetes as module

    clock, chunks, closed = [0.0], [], []

    class Trickle(httpx.SyncByteStream):
        def __iter__(self):
            for _ in range(20):
                clock[0] += 3
                chunks.append(1)
                yield b" "

        def close(self):
            closed.append(True)

    original = httpx.Client
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Trickle())), **kwargs,
    ))
    with pytest.raises(TimeoutError, match="observation deadline exceeded"):
        read_kubelet_summary(_kubelet_core(connection, lambda _: None), node_name="node-1",
                              timeout_seconds=5)
    assert len(chunks) == 2 and closed == [True]


@pytest.mark.parametrize("extra_byte", [False, True])
def test_kubelet_stream_enforces_exact_sixteen_mib_boundary(connection, monkeypatch, extra_byte):
    import httpx

    prefix = b'{"node":{"nodeName":"node-1"}}'
    payload = prefix + b" " * (16 * 1024**2 - len(prefix) + int(extra_byte))
    original = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=payload)), **kwargs,
    ))
    core = _kubelet_core(connection, lambda _: None)
    if extra_byte:
        with pytest.raises(ValueError, match="summary exceeds its bound"):
            read_kubelet_summary(core, node_name="node-1")
    else:
        assert read_kubelet_summary(core, node_name="node-1")["node"]["nodeName"] == "node-1"
@pytest.mark.parametrize(
    "reader_type", [InClusterKubernetesJobApi, InClusterKubernetesCapacityReader]
)
def test_invalid_remote_ca_fails_without_local_fallback(
    connection: NebiusKubernetesConnection, reader_type: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kubernetes import config

    monkeypatch.setattr(config, "load_incluster_config", lambda: pytest.fail("local fallback"))
    connection.ca_file.write_text("not a certificate")
    with pytest.raises(ssl.SSLError, match=r"certificate|PEM"):
        reader_type(connection=connection)


def test_world_readable_credential_rejected(connection: NebiusKubernetesConnection) -> None:
    connection.credentials_file.chmod(0o444)
    with pytest.raises(ValueError, match="private bounded"):
        NebiusKubernetesCredentials(connection, sdk_factory=FakeSDK)


def test_actuator_environment_requires_complete_remote_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_KUBERNETES_ENDPOINT", "https://regional-api.example"
    )
    with pytest.raises(ValueError, match="set together"):
        ExecutionActuatorSettings(
            db_url="postgresql://unused",
            controller_id="test",
            target_id="regional",
            namespace="execution",
        )


@pytest.mark.asyncio
async def test_remote_request_uses_bound_origin_namespace_and_fresh_authorization(
    connection: NebiusKubernetesConnection, sdk: FakeSDK, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urllib3 import HTTPResponse

    reader = InClusterKubernetesJobApi(connection=connection)
    calls = []

    def get(url: str, **kwargs: Any) -> HTTPResponse:
        calls.append((url, kwargs["headers"]["authorization"]))
        return HTTPResponse(
            body=b'{"apiVersion":"v1","kind":"PodList","items":[]}',
            status=200,
            headers={"Content-Type": "application/json"},
        )

    monkeypatch.setattr(reader._core.api_client.rest_client, "GET", get)
    try:
        reader._core.list_namespaced_pod(namespace="region-two-execution")
        reader._core.list_namespaced_pod(namespace="region-two-execution")
        assert calls == [
            (
                connection.endpoint + "/api/v1/namespaces/region-two-execution/pods",
                "Bearer ephemeral-1",
            ),
            (
                connection.endpoint + "/api/v1/namespaces/region-two-execution/pods",
                "Bearer ephemeral-2",
            ),
        ]
    finally:
        await reader.close()
