"""TokenReview auth transport pressure, using isolated HTTPS and fixture IAM."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_llm_gateway import pod_identity
from loom_llm_gateway.pod_identity import ExecutionPodReviewer


def _server_tls(tmp_path: Path) -> tuple[ssl.SSLContext, Path]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tokenreview-fixture")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    ca = tmp_path / "tokenreview-ca.pem"
    private_key = tmp_path / "tokenreview-key.pem"
    ca.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    private_key.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    private_key.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(ca, private_key)
    return context, ca


async def test_101_tokenreviews_reach_https_and_cancel_without_queue_or_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Component auth proof, not production native model-load acceptance."""
    count = 101
    context, ca = _server_tls(tmp_path)
    # TokenReview uses explicit cluster trust and never ambient proxy settings.
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    arrived, all_closed = asyncio.Event(), asyncio.Event()
    release = asyncio.Event()
    observed: list[str] = []
    closed = 0
    recovery = False
    handlers: set[asyncio.Task] = set()
    credentials = []

    class FixtureCredentials:
        def __init__(self, connection: NebiusKubernetesConnection) -> None:
            self.ssl_context = ssl.create_default_context(cafile=str(connection.ca_file))
            self.closed = False
            credentials.append(self)

        async def get_token(self) -> str:
            # Only IAM exchange is a fixture; no live IAM/Kubernetes is used.
            return "fixture-iam-only"

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(pod_identity, "NebiusKubernetesCredentials", FixtureCredentials)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal closed
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        eof = released = None
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            lines = head.split("\r\n")
            assert lines[0] == "POST /apis/authentication.k8s.io/v1/tokenreviews HTTP/1.1"
            headers = {
                line.split(": ", 1)[0].lower(): line.split(": ", 1)[1]
                for line in lines[1:]
                if ": " in line
            }
            assert headers["authorization"] == "Bearer fixture-iam-only"
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            assert body["apiVersion"] == "authentication.k8s.io/v1"
            assert body["kind"] == "TokenReview"
            assert body["spec"]["audiences"] == ["loom-execution"]
            token = body["spec"]["token"]
            observed.append(token)
            if len(observed) == count:
                arrived.set()
            if not recovery:
                eof = asyncio.create_task(reader.read())
                released = asyncio.create_task(release.wait())
                done, _ = await asyncio.wait([eof, released], return_when=asyncio.FIRST_COMPLETED)
                if eof in done:
                    assert eof.result() == b""
                    closed += 1
                    if closed == count:
                        all_closed.set()
                return
            payload = json.dumps(
                {
                    "status": {
                        "authenticated": True,
                        "audiences": ["loom-execution"],
                        "user": {
                            "username": "system:serviceaccount:execution:attempt",
                            "extra": {
                                "authentication.kubernetes.io/pod-uid": ["pod-recovered"],
                            },
                        },
                    }
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n"
                b"Connection: close\r\n"
                + f"Content-Length: {len(payload)}\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
        finally:
            for pending in [eof, released]:
                if pending is not None:
                    pending.cancel()
            await asyncio.gather(*(t for t in [eof, released] if t), return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context, backlog=count * 2)
    origin = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    reviewer = ExecutionPodReviewer(
        {
            "fixture-cluster": NebiusKubernetesConnection(
                endpoint=origin,
                ca_file=ca,
                credentials_file=tmp_path / "unused-iam-fixture",
            )
        }
    )

    async def review(token: str):
        return await reviewer.review(
            cluster_scope_id="fixture-cluster",
            namespace="execution",
            service_account="attempt",
            audience="loom-execution",
            token=token,
        )

    tasks = [asyncio.create_task(review(f"fixture-pod-{i}")) for i in range(count)]
    client = None
    try:
        try:
            await asyncio.wait_for(arrived.wait(), 3)
        except TimeoutError:
            pytest.fail(f"only {len(observed)}/{count} TokenReviews reached held HTTPS upstream")
        assert set(observed) == {f"fixture-pod-{i}" for i in range(count)}
        assert len(credentials) == 1
        client = reviewer._clients["fixture-cluster"]
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        await asyncio.wait_for(all_closed.wait(), 1)
        recovery = True
        result = await review("fixture-recovery-pod")
        assert result.pod_uid == "pod-recovered"
        assert result.cluster_scope_id == "fixture-cluster"
        assert result.namespace == "execution" and result.service_account == "attempt"
        assert result.audience == "loom-execution"
        assert not client.is_closed
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        release.set()
        await reviewer.close()
        server.close()
        await server.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
    assert client is not None and client.is_closed
    assert all(credential.closed for credential in credentials)
