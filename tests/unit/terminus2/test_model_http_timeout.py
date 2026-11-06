"""Effective HTTP request bounds through the hosted Terminus SDK boundary."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.unit.terminus2.test_runtime_loop import _AttemptTokenCP, _patch_harbor

from loom.agent.terminus2 import runtime as runtime_module
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.attempt_deadline import AttemptDeadline
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


@pytest.mark.parametrize(
    "agent_seconds,pause_extension", [(30000.0, 0.0), (120.0, 0.0), (1800.0, 7200.0)]
)
@pytest.mark.parametrize("status", [200, 503])
async def test_signed_timeout_reaches_litellm_openai_request_without_nested_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    agent_seconds: float,
    pause_extension: float,
    status: int,
) -> None:
    # Harbor is optional in the library test environment. Its existing runtime
    # interface fixture expands the live call kwargs into the real LiteLLM /
    # OpenAI request, intercepted at HTTPX: no provider or discovery traffic.
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    monkeypatch.setenv("LITELLM_TELEMETRY", "False")
    import litellm

    requests: list[httpx.Request] = []
    constructor_kwargs: list[dict[str, object]] = []
    reasoning_seen: list[object] = []
    _patch_harbor(
        monkeypatch,
        tokens_seen=[],
        llm_kwargs_seen=constructor_kwargs,
        reasoning_seen=reasoning_seen,
    )
    terminus, _ = runtime_module._import_terminus2()
    original_run = terminus.run

    async def http_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if status != 200:
            return httpx.Response(
                status, json={"error": {"message": "offline unavailable", "type": "server_error"}}
            )
        return httpx.Response(
            200,
            json={
                "id": "offline-completion",
                "object": "chat.completion",
                "created": 0,
                "model": "glm-5.3",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "offline"},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    def http_client(*, timeout_sec: float, **kwargs: object) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=timeout_sec, transport=httpx.MockTransport(http_handler), **kwargs
        )

    monkeypatch.setattr("loom.llm_sdk_clients.create_model_http_client", http_client)

    async def model_run(self, *args: object, **kwargs: object) -> None:
        await original_run(self, *args, **kwargs)
        await litellm.acompletion(
            model="openai/glm-5.3",
            api_base="http://127.0.0.1:9/openai/v1",
            messages=[{"role": "user", "content": "offline fixture"}],
            drop_params=True,
            **{**constructor_kwargs[-1], **self._llm_call_kwargs},
        )

    monkeypatch.setattr(terminus, "run", model_run)
    clock = [30.0]
    deadline = AttemptDeadline.after(
        agent_seconds,
        clock=lambda: clock[0],
        wall_clock=lambda: 1000.0,
    )
    # Only the already-authorized phase extension is supplied here; its quota
    # ceiling and outer lease authorization belong to the existing issuer.
    if pause_extension:
        assert deadline.extend_to(deadline.wall_deadline.timestamp() + pause_extension)
    clock[0] += 30.0
    expected = agent_seconds + pause_extension - 30.0
    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="glm-5.3"),
        team_id=str(uuid4()),
        trial_id=uuid4(),
        cp_client=_AttemptTokenCP(),
        gateway_url="http://127.0.0.1:9",
        request_params={"temperature": 0.2, "max_tokens": 32, "reasoning_effort": "high"},
    )
    runtime.begin_attempt(deadline)
    driver = FakeDriver()
    await driver.start()
    try:
        async with TrajectoryWriter(
            local_path=tmp_path / "events.jsonl",
            store=FakeObjectStore(),
            bucket="test",
            key="events.jsonl",
            min_part_bytes=0,
        ) as trajectory:
            if status == 200:
                await runtime.run(
                    instruction="task",
                    env=driver,
                    trajectory=trajectory,
                    mcp=[],
                    skills_dir=None,
                    step_id="agent",
                )
            else:
                with pytest.raises(AgentError):
                    await runtime.run(
                        instruction="task",
                        env=driver,
                        trajectory=trajectory,
                        mcp=[],
                        skills_dir=None,
                        step_id="agent",
                    )
    finally:
        await driver.stop()

    assert len(requests) == 1, "the Gateway owns retries, including after a retryable SDK response"
    assert requests[0].extensions["timeout"] == dict.fromkeys(
        ("connect", "read", "write", "pool"), expected
    )
    assert float(requests[0].headers["x-stainless-read-timeout"]) == expected
    body = json.loads(requests[0].content)
    assert body["model"] == "glm-5.3"
    assert body["temperature"] == 0.2
    assert body["max_tokens"] == 32
    assert reasoning_seen == ["high"]
