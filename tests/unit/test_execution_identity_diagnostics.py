"""Startup failures identify the internal operation without exposing HTTP data."""
from __future__ import annotations

import httpx
import pytest

from loom import service_execution_sandbox_task as module


@pytest.mark.parametrize("kind", ["read_timeout", "connect_timeout", "http_503", "invalid_response"])
async def test_identity_failure_has_safe_operation_and_phase(monkeypatch, kind):
    def transport(request):
        if kind == "read_timeout":
            raise httpx.ReadTimeout("private-upstream-token", request=request)
        if kind == "connect_timeout":
            raise httpx.ConnectTimeout("private-upstream-token", request=request)
        if kind == "http_503":
            return httpx.Response(503, text="private-upstream-token")
        return httpx.Response(200, json={"trial_id": "private-upstream-token", "step_id": "agent"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    monkeypatch.setattr(module.httpx, "AsyncClient", lambda **kwargs: client)
    with pytest.raises(module.ServiceExecutionTaskError, match="execution_identity ledger GET") as caught:
        await module._execution_identity("http://127.0.0.1:1234")
    message = str(caught.value)
    assert kind in message
    assert "private-upstream-token" not in message
    assert "127.0.0.1" not in message
