from __future__ import annotations

import asyncio
import json
import socket
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT
from loom.pipeline.keys import canonical_digest
from loom_llm_gateway.drain import ensure_drain_state
from loom_llm_gateway.routes import task_egress
from loom_llm_gateway.task_egress import EgressDeniedError, TaskEgressConfig, TaskEgressRuntime
from tests.unit.test_execution_runtime_contract import _plan
from tests.unit.test_task_web_egress import policy


def gateway(monkeypatch):
    app = FastAPI()
    app.state.task_egress = TaskEgressRuntime(TaskEgressConfig(protected_cidrs=("8.8.8.8/32",)))
    app.include_router(task_egress.router)
    plan = _plan(task_egress=policy(), output_declarations=(TASK_EGRESS_OUTPUT,))
    lease = SimpleNamespace(id=uuid4(), runtime_contract_json=plan.canonical_payload(),
        runtime_contract_sha256=canonical_digest(plan.canonical_payload()),
        deadline_at=datetime.now(UTC) + timedelta(minutes=1))
    authorize = AsyncMock(return_value=lease)
    monkeypatch.setattr(task_egress, "_authorize", authorize)
    headers = {"X-Loom-Execution-Lease-Id": str(lease.id), "X-Loom-Execution-Generation": "1",
        "X-Loom-Execution-Role": "attempt", "X-Loom-Runtime-Contract-SHA256": lease.runtime_contract_sha256,
        "X-Loom-Phase-Deadline": lease.deadline_at.isoformat()}
    return app, headers, authorize


def test_gateway_relays_real_socket_bytes_and_releases_drain(monkeypatch) -> None:
    app, headers, authorize = gateway(monkeypatch)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    def upstream():
        connection, _ = listener.accept()
        with connection:
            assert connection.recv(100) == b"request-bytes"
            connection.sendall(b"package-bytes")
    thread = threading.Thread(target=upstream)
    thread.start()
    async def connect(destination, protected):
        assert destination == policy().destinations[0]
        assert protected == ("8.8.8.8/32",)
        return await asyncio.open_connection(*listener.getsockname())
    monkeypatch.setattr(task_egress, "connect_destination", connect)
    with TestClient(app) as client, client.websocket_connect("/internal/service-execution/task-egress", headers=headers) as ws:
        ws.send_json(policy().destinations[0].model_dump())
        assert ws.receive_json() == {"status": "ready"}
        ws.send_bytes(b"request-bytes")
        assert ws.receive_bytes() == b"package-bytes"
    thread.join(timeout=2)
    listener.close()
    assert not thread.is_alive()
    assert not app.state.task_egress.connections
    assert ensure_drain_state(app).in_flight == 0
    assert authorize.await_args.kwargs["purpose"] == "token"


def test_public_web_dials_a_host_that_is_not_on_an_allowlist(monkeypatch) -> None:
    from loom.models.networking import PublicWeb, WebDestination

    app, headers, _authorize = gateway(monkeypatch)
    plan = _plan(task_egress=PublicWeb(), output_declarations=(TASK_EGRESS_OUTPUT,))
    lease = _authorize.return_value
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(plan.canonical_payload())
    headers["X-Loom-Runtime-Contract-SHA256"] = lease.runtime_contract_sha256
    destination = WebDestination(host="pypi.org", protocol="https")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()

    def upstream():
        connection, _ = listener.accept()
        with connection:
            connection.recv(16)

    thread = threading.Thread(target=upstream)
    thread.start()

    async def connect(requested, protected):
        assert requested == destination
        assert protected == ("8.8.8.8/32",)
        return await asyncio.open_connection(*listener.getsockname())

    monkeypatch.setattr(task_egress, "connect_destination", connect)
    with TestClient(app) as client, client.websocket_connect(
        "/internal/service-execution/task-egress", headers=headers,
    ) as ws:
        ws.send_json(destination.model_dump())
        assert ws.receive_json() == {"status": "ready"}
    thread.join(timeout=2)
    listener.close()


@pytest.mark.parametrize("mode,reason", [
    ("identity", "task_egress_identity_rejected"), ("digest", "task_egress_contract_mismatch"),
    ("host", "task_egress_destination_denied"), ("disabled", "task_egress_unavailable"),
    ("expired", "task_egress_deadline"), ("drain", "task_egress_unavailable"),
])
def test_gateway_rejects_before_any_dial(monkeypatch, mode, reason) -> None:
    app, headers, authorize = gateway(monkeypatch)
    destination = policy().destinations[0].model_dump()
    if mode == "identity":
        authorize.side_effect = HTTPException(status_code=403)
    elif mode == "digest":
        headers["X-Loom-Runtime-Contract-SHA256"] = "sha256:" + "0" * 64
    elif mode == "host":
        destination["host"] = "other.example.org"
    elif mode == "disabled":
        app.state.task_egress = None
    elif mode == "expired":
        headers["X-Loom-Phase-Deadline"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    elif mode == "drain":
        ensure_drain_state(app).draining = True
    dial = AsyncMock()
    monkeypatch.setattr(task_egress, "connect_destination", dial)
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/internal/service-execution/task-egress", headers=headers) as ws:
            ws.send_text(json.dumps(destination))
            ws.receive_json()
    assert exc.value.reason == reason
    dial.assert_not_called()
    assert ensure_drain_state(app).in_flight == 0


def test_gateway_rechecks_lease_and_closes_active_tunnel(monkeypatch) -> None:
    from unittest.mock import MagicMock

    import loom_llm_gateway.task_egress as transport

    app, headers, authorize = gateway(monkeypatch)
    lease = authorize.return_value
    authorize.side_effect = [lease, HTTPException(status_code=409)]
    monkeypatch.setattr(transport, "_FENCE_SECONDS", 0.01)
    writer = MagicMock()
    writer.wait_closed = AsyncMock()
    async def connect(destination, protected):
        return asyncio.StreamReader(), writer
    monkeypatch.setattr(task_egress, "connect_destination", connect)
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/internal/service-execution/task-egress", headers=headers) as ws:
            ws.send_json(policy().destinations[0].model_dump())
            assert ws.receive_json() == {"status": "ready"}
            ws.receive_bytes()
    assert exc.value.reason == "task_egress_identity_rejected"
    assert authorize.await_count == 2
    writer.close.assert_called()
    assert not app.state.task_egress.connections
    assert ensure_drain_state(app).in_flight == 0


def test_gateway_limits_connections_per_lease(monkeypatch) -> None:
    app, headers, _ = gateway(monkeypatch)
    app.state.task_egress = TaskEgressRuntime(TaskEgressConfig(
        protected_cidrs=("8.8.8.8/32",), maximum_connections_per_lease=1,
    ))
    app.state.task_egress.acquire(headers["X-Loom-Execution-Lease-Id"])
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/internal/service-execution/task-egress", headers=headers) as ws:
            ws.send_json(policy().destinations[0].model_dump())
            ws.receive_json()
    assert exc.value.reason == "task_egress_capacity_exceeded"
    assert ensure_drain_state(app).in_flight == 0


def test_package_connections_retain_per_lease_and_global_capacity_bounds() -> None:
    runtime = TaskEgressRuntime(TaskEgressConfig(protected_cidrs=("8.8.8.8/32",)))
    # Metadata, package requests and redirects retain separate TCP tunnels.
    for lease in ("package-task", "other-task"):
        for _ in range(32):
            runtime.acquire(lease)
        with pytest.raises(EgressDeniedError, match="task_egress_capacity_exceeded"):
            runtime.acquire(lease)
    with pytest.raises(EgressDeniedError, match="task_egress_capacity_exceeded"):
        runtime.acquire("third-task")
    assert runtime.connections == {"package-task": 32, "other-task": 32}

    runtime.release("package-task")
    runtime.acquire("third-task")
    with pytest.raises(EgressDeniedError, match="task_egress_capacity_exceeded"):
        runtime.acquire("package-task")
    runtime.release("third-task")
    runtime.acquire("package-task")
    for lease in ("package-task", "other-task"):
        for _ in range(32):
            runtime.release(lease)
    assert not runtime.connections


async def test_download_activity_keeps_tunnel_alive_without_upload_activity(monkeypatch) -> None:
    from unittest.mock import MagicMock

    import loom_llm_gateway.task_egress as transport

    monkeypatch.setattr(transport, "_IDLE_SECONDS", 0.05)
    monkeypatch.setattr(transport, "_FENCE_SECONDS", 0.01)
    reader = asyncio.StreamReader()
    async def waiting_upload():
        await asyncio.Future()
    websocket = SimpleNamespace(receive_bytes=waiting_upload, send_bytes=AsyncMock())
    writer = MagicMock()
    writer.wait_closed = AsyncMock()
    async def produce():
        for _ in range(6):
            await asyncio.sleep(0.02)
            reader.feed_data(b"chunk")
        reader.feed_eof()
    producer = asyncio.create_task(produce())
    await transport.relay(websocket, reader, writer,
        deadline=datetime.now(UTC) + timedelta(seconds=1), reauthorize=AsyncMock())
    await producer
    assert websocket.send_bytes.await_count == 6
    writer.close.assert_called_once()
