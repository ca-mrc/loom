"""Real provider SDK error propagation over a controlled Gateway transport."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest


@pytest.mark.parametrize("trusted_stall", [True, False])
def test_gateway_pool_stall_reaches_native_retry_marker(tmp_path, monkeypatch, trusted_stall):
    # LiteLLM can import provider metadata before issuing the mocked model call.
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    monkeypatch.setenv("LITELLM_TELEMETRY", "False")
    import httpx
    import litellm
    import openai
    import tomli_w
    from fastapi import FastAPI, HTTPException

    from loom import service_execution_sandbox_task as controller
    from loom.errors import AgentError
    from loom_llm_gateway.dispatch_audit import QueueStalledError
    from loom_llm_gateway.request_pool import PoolDispatchAudit
    from tests.unit.test_service_execution_terminus_plan import _inputs

    task, trial, _ = _inputs()
    (tmp_path / "task.toml").write_text(tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)))
    monkeypatch.setenv("LOOM_TASK_TRIAL_JSON", trial.model_dump_json())
    monkeypatch.setattr("sys.argv", ["sandbox-task", "terminus-2", "--workspace", str(tmp_path)])

    audit = object.__new__(PoolDispatchAudit)
    audit._admit = AsyncMock(side_effect=QueueStalledError("test-group"))
    audit._close_queue = AsyncMock()
    gateway = FastAPI()

    @gateway.post("/v1/chat/completions")
    async def model_call():
        if trusted_stall:
            await audit._admit_with_capacity(None, 1)
        # Identical error text is insufficient without Gateway provenance.
        raise HTTPException(429, "provider_queue_stalled")

    observed = []

    async def run_agent(*args):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as source:
            async def dispatch(request):
                observed.append(request)
                return await source.post("/v1/chat/completions", content=request.content)

            async with openai.AsyncOpenAI(
                api_key="offline-fixture", base_url="http://gateway/v1", max_retries=0,
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(dispatch)),
            ) as sdk:
                try:
                    await litellm.acompletion(
                        model="openai/glm-5.3", messages=[{"role": "user", "content": "offline fixture"}],
                        client=sdk, num_retries=0, max_retries=0,
                    )
                except litellm.RateLimitError as exc:
                    raise AgentError("model request failed") from exc
        pytest.fail("controlled rate-limit response unexpectedly succeeded")

    monkeypatch.setattr(controller, "run_agent", run_agent)
    with pytest.raises(AgentError):
        controller.main()
    assert len(observed) == 1
    marker = tmp_path / ".loom/agent/transport-failure.json"
    if trusted_stall:
        assert json.loads(marker.read_bytes()) == {"failure_reason": "gateway_error"}
        audit._close_queue.assert_awaited_once_with("stalled")
    else:
        assert not marker.exists()
        audit._close_queue.assert_not_awaited()
