"""#2310: the Gateway admits setup-only install sources only for setup phases,
and never lets them widen the task's own egress."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT
from loom.models.networking import WebAllowlist, WebDestination
from loom.pipeline.keys import canonical_digest
from loom_llm_gateway.routes import task_egress
from loom_llm_gateway.task_egress import EgressDeniedError
from tests.unit.test_execution_runtime_contract import _plan
from tests.unit.test_task_egress_gateway import gateway

_INSTALL = WebDestination(host="registry.npmjs.org", protocol="https")
_DATA = WebDestination(host="data.example.org", protocol="https")


def _setup_plan(task_egress: WebAllowlist | None = None):
    return _plan(
        setup_egress=WebAllowlist(destinations=(_INSTALL,)),
        task_egress=task_egress,
        output_declarations=(TASK_EGRESS_OUTPUT,),
    )


def _bind(monkeypatch, plan):
    app, headers, authorize = gateway(monkeypatch)
    lease = authorize.return_value
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(plan.canonical_payload())
    headers["X-Loom-Runtime-Contract-SHA256"] = lease.runtime_contract_sha256
    return app, headers


def _connect(monkeypatch, app, headers, destination: WebDestination, phase: str | None):
    """Return "dialed" if the Gateway authorized and dialed it, else the close reason."""
    if phase is not None:
        headers = {**headers, "X-Loom-Execution-Phase": phase}
    # An authorized dial fails like an unreachable host; nothing leaves the test.
    connect = AsyncMock(side_effect=EgressDeniedError("destination_connect_failed"))
    monkeypatch.setattr(task_egress, "connect_destination", connect)
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/internal/service-execution/task-egress", headers=headers) as ws:
            ws.send_text(json.dumps(destination.model_dump()))
            ws.receive_json()
    if connect.await_count:
        assert connect.await_args.args[0] == destination
        return "dialed"
    return exc.value.reason


def test_plan_contract_binds_setup_egress_to_a_setup_phase() -> None:
    plan = _setup_plan()
    assert plan.canonical_payload()["setup_egress"]["destinations"][0]["host"] == "registry.npmjs.org"
    assert "setup_egress" not in _plan().canonical_payload()  # legacy plans keep their bytes
    with pytest.raises(ValidationError, match="setup phase"):
        _plan(setup=(), setup_egress=WebAllowlist(destinations=(_INSTALL,)),
              output_declarations=(TASK_EGRESS_OUTPUT,))
    with pytest.raises(ValidationError, match="diagnostic output"):
        _plan(setup_egress=WebAllowlist(destinations=(_INSTALL,)))


def test_install_source_is_dialed_only_during_setup(monkeypatch) -> None:
    app, headers = _bind(monkeypatch, _setup_plan())

    assert _connect(monkeypatch, app, headers, _INSTALL, "setup") == "dialed"
    for phase in ("agent", "verifier", None):
        # Gateway-only task: outside setup there is no egress at all.
        assert _connect(monkeypatch, app, headers, _INSTALL, phase) == "task_egress_not_declared"
    assert _connect(monkeypatch, app, headers, _DATA, "setup") == "task_egress_destination_denied"


def test_setup_and_task_egress_never_widen_each_other(monkeypatch) -> None:
    app, headers = _bind(monkeypatch, _setup_plan(task_egress=WebAllowlist(destinations=(_DATA,))))

    assert _connect(monkeypatch, app, headers, _DATA, "agent") == "dialed"
    assert _connect(monkeypatch, app, headers, _INSTALL, "agent") == "task_egress_destination_denied"
    assert _connect(monkeypatch, app, headers, _DATA, "setup") == "task_egress_destination_denied"
    assert _connect(monkeypatch, app, headers, _INSTALL, "setup") == "dialed"
