"""Pinned Harbor and native LiteLLM transport qualification, with no provider I/O.

Harbor is optional in the ordinary development environment. Run this lane with
Python 3.12 and the exact source revision from config/harbor-runtime.json.
Only terminal execution and HTTP transports are simulated; Harbor's constructor,
agent loop, parser, usage extraction, and trajectory serialization are authentic.
"""
from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
import socket
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import httpx
import pytest

if sys.version_info < (3, 12):
    pytest.skip("pinned Harbor requires Python 3.12", allow_module_level=True)
pytest.importorskip("harbor")

# LiteLLM imports its model metadata when Harbor is imported. Keep that offline.
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
os.environ["LITELLM_TELEMETRY"] = "False"

from loom.agent.direct_provider import DirectProviderConnection
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom_cli.agent_connection import resolve_agent_connection
from loom_cli.rate_cards import RateCardEntry, RateCardTable
from loom_cli.terminus_ledger import LocalHarborLedger
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient

_KEY = "synthetic-direct-runtime-qualification-key"
_CONTENT = json.dumps({
    "analysis": "The requested task is complete.", "plan": "Finish now.",
    "commands": [], "task_complete": True,
})
_CASES = [
    ("openai", "gpt-4o-mini", "https://openai.test/v1", 100, 20, 0, 7),
    ("anthropic", "claude-sonnet-4-20250514", "https://anthropic.test", 95, 20, 5, 0),
    ("google", "gemini-2.5-flash", "https://generativelanguage.googleapis.com", 100, 27, 0, 7),
    ("openai", "local-qualification", "http://localhost:8765/v1", 100, 20, 0, 7),
]


def _response(request: httpx.Request, *, model: str) -> httpx.Response:
    if request.url.host == "anthropic.test":
        body = {
            "id": "msg_qualification", "type": "message", "role": "assistant", "model": model,
            "content": [{"type": "text", "text": _CONTENT}], "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 80, "output_tokens": 20, "cache_read_input_tokens": 10,
                      "cache_creation_input_tokens": 5},
        }
    elif request.url.host == "generativelanguage.googleapis.com":
        body = {
            "candidates": [{"index": 0, "content": {"role": "model", "parts": [{"text": _CONTENT}]},
                            "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 20,
                              "totalTokenCount": 127, "cachedContentTokenCount": 10,
                              "thoughtsTokenCount": 7},
            "modelVersion": model,
        }
    else:
        body = {
            "id": "chatcmpl-qualification", "object": "chat.completion", "created": 1720000000,
            "model": model, "choices": [{"index": 0, "message": {"role": "assistant", "content": _CONTENT},
                                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120,
                      "prompt_tokens_details": {"cached_tokens": 10},
                      "completion_tokens_details": {"reasoning_tokens": 7}},
        }
    return httpx.Response(200, json=body, request=request)


def _install_transport(monkeypatch: pytest.MonkeyPatch, handler: Any) -> None:
    transport = httpx.MockTransport(handler)
    original_async_init = httpx.AsyncClient.__init__
    original_sync_init = httpx.Client.__init__

    def async_init(self: httpx.AsyncClient, *args: Any, **kwargs: Any) -> None:
        kwargs.update(transport=transport, trust_env=False)
        original_async_init(self, *args, **kwargs)

    def sync_init(self: httpx.Client, *args: Any, **kwargs: Any) -> None:
        kwargs.update(transport=transport, trust_env=False)
        original_sync_init(self, *args, **kwargs)

    def deny_connect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Network sockets are forbidden in Harbor qualification")

    monkeypatch.setattr(httpx.AsyncClient, "__init__", async_init)
    monkeypatch.setattr(httpx.Client, "__init__", sync_init)
    monkeypatch.setattr(socket.socket, "connect", deny_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", deny_connect)


def _runtime(tmp_path: Path, provider: str, model: str, base: str):
    trial_id = uuid4()
    selected = ModelSpec(provider=provider, name=model)
    connection = DirectProviderConnection(model=selected, api_key=_KEY, base_url=base)
    if provider == "google":
        connection = resolve_agent_connection(UpstreamDirectGatewayClient(
            anthropic_client=None, openai_client=None, google_client=None,
            tokens={"google": _KEY},
        ), selected)
    tracked = selected if base.startswith("https:") else ModelSpec(provider="local", name=f"qualification/{model}")
    rates = RateCardTable(entries=(RateCardEntry(provider=provider, model=model,
        input_per_mtok=1, output_per_mtok=2, cache_read_per_mtok=0.1, cache_write_per_mtok=1.25),))
    ledger = LocalHarborLedger(trial_id=trial_id, model=tracked, connection=connection, rate_cards=rates)
    runtime = LoomTerminus2Runtime(model=tracked, team_id=str(uuid4()), trial_id=trial_id,
        cp_client=None, gateway_url="", direct_provider=connection, direct_ledger=ledger, max_turns=2)
    driver = FakeDriver(exec_handler=lambda *args: ExecResult(return_code=0,
        stdout=b"qualification terminal\n", stderr=b"", duration_sec=0))
    writer = TrajectoryWriter(local_path=tmp_path / "events.jsonl", store=FakeObjectStore(),
        bucket="qualification", key=f"{trial_id}/events.jsonl", min_part_bytes=0)
    return runtime, ledger, driver, writer


@pytest.mark.asyncio
@pytest.mark.parametrize("provider,model,base,input_tokens,output_tokens,cache_write,thinking", _CASES)
async def test_real_harbor_direct_provider_and_usage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str, model: str, base: str,
    input_tokens: int, output_tokens: int, cache_write: int, thinking: int,
) -> None:
    from harbor.agents.terminus_2.terminus_2 import Terminus2
    from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

    assert Terminus2.__module__ == "harbor.agents.terminus_2.terminus_2"
    distribution = importlib.metadata.distribution("harbor")
    assert distribution.version == "0.18.0"
    direct_url = distribution.read_text("direct_url.json")
    if direct_url is not None:
        assert json.loads(direct_url).get("vcs_info", {}).get("commit_id") == HARBOR_COMPAT_SHA
    temporaries = tmp_path / "runtime-temporaries"
    temporaries.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporaries))
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith(base)
        body = json.loads(request.content)
        assert "session_id" not in body
        assert "x-session-id" not in request.headers
        assert any((request.headers.get("authorization") == f"Bearer {_KEY}",
                    request.headers.get("x-api-key") == _KEY,
                    request.headers.get("x-goog-api-key") == _KEY,
                    request.url.params.get("key") == _KEY))
        if provider == "google":
            assert request.url.path == f"/v1beta/models/{model}:generateContent"
            assert body["generationConfig"]["temperature"] == 0.2
        else:
            assert body["model"] == model
            assert body["temperature"] == 0.2
            assert request.url.path.endswith("/messages" if provider == "anthropic" else "/chat/completions")
        requests.append(request)
        return _response(request, model=model)

    _install_transport(monkeypatch, respond)
    runtime, ledger, driver, writer = _runtime(tmp_path, provider, model, base)
    runtime.request_params = {"temperature": 0.2}
    await driver.start()
    tasks_before = set(asyncio.all_tasks())
    try:
        async with writer:
            await runtime.run(instruction="Task is already complete; mark complete and confirm.",
                env=driver, trajectory=writer, mcp=[], skills_dir=None, step_id="agent")
        events_text = (tmp_path / "events.jsonl").read_text()
        events = [json.loads(line) for line in events_text.splitlines()]
        calls = [event for event in events if event["kind"] == "llm_call"]
        turns = [event for event in events if event["kind"] == "terminus2_turn"]
        assert len(calls) == len(turns) == len(requests) == 2
        call_ids = {call["gateway_request_id"] for call in calls}
        assert len(call_ids) == 2
        assert call_ids == {turn["gateway_request_id"] for turn in turns}
        for call in calls:
            assert call["usage_status"] is None
            assert call["request_params"]["parameters"]["temperature"] == 0.2
            assert (call["input_tokens"], call["output_tokens"], call["cached_input_tokens"],
                    call["cache_write_tokens"], call["thinking_tokens"]) == (
                        input_tokens, output_tokens, 10, cache_write, thinking)
            assert call["cost_usd_snapshot"] == pytest.approx(
                0 if base.startswith("http:") else
                ((80 if provider == "anthropic" else input_tokens - 10) + output_tokens * 2 + 1 + cache_write * 1.25) / 1_000_000)
        assert len(await ledger.get_trial_llm_calls(runtime.trial_id)) == 2
        assert _KEY not in events_text
        assert PurePosixPath("/workspace/.loom/agent/trajectory.json") in driver.filesystem
        assert all(_KEY.encode() not in body for body in driver.filesystem.values())
        assert runtime._active_env is None
        assert list(temporaries.iterdir()) == []
    finally:
        await driver.stop()
        # LiteLLM owns one process-global logging worker, separate from the
        # attempt's supervisor/checkpoint tasks. Shut it down for test isolation.
        await GLOBAL_LOGGING_WORKER.stop()
        assert not [task for task in asyncio.all_tasks() - tasks_before if not task.done()]


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_mode", ["cancel", "provider_error"])
async def test_real_harbor_direct_failure_stops_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, stop_mode: str,
) -> None:
    from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

    from loom.errors import AgentError

    temporaries = tmp_path / "runtime-temporaries"
    temporaries.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporaries))
    started = asyncio.Event()
    never_release = asyncio.Event()
    request_count = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        assert request.url.host == "openai.test"
        request_count += 1
        started.set()
        if stop_mode == "cancel":
            await never_release.wait()
            raise AssertionError("cancelled request must not continue")
        return httpx.Response(401, request=request, json={"error": {
            "message": f"Incorrect API key provided: {_KEY}", "type": "invalid_request_error",
            "code": "invalid_api_key", "param": None,
        }})

    _install_transport(monkeypatch, respond)
    runtime, ledger, driver, writer = _runtime(tmp_path, "openai", "gpt-4o-mini", "https://openai.test/v1")
    await driver.start()
    tasks_before = set(asyncio.all_tasks())
    try:
        async with writer:
            task = asyncio.create_task(runtime.run(instruction="Finish now.", env=driver,
                trajectory=writer, mcp=[], skills_dir=None, step_id="agent"))
            await asyncio.wait_for(started.wait(), timeout=10)
            if stop_mode == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(AgentError) as failure:
                    await task
                assert _KEY not in str(failure.value)
        text = (tmp_path / "events.jsonl").read_text()
        events = [json.loads(line) for line in text.splitlines()]
        calls = [event for event in events if event["kind"] == "llm_call"]
        assert request_count == len(calls) == (1 if stop_mode == "cancel" else 3)
        assert all(call["call_status"] == "failed" for call in calls)
        assert all(call["usage_status"] == "missing" for call in calls)
        assert not [event for event in events if event["kind"] == "terminus2_turn"]
        assert await ledger.get_trial_llm_calls(runtime.trial_id) == []
        assert runtime._active_env is None
        assert list(temporaries.iterdir()) == []
        assert _KEY not in text
        assert _KEY not in caplog.text
        assert all(_KEY.encode() not in body for body in driver.filesystem.values())
    finally:
        await driver.stop()
        await GLOBAL_LOGGING_WORKER.stop()
        assert not [task for task in asyncio.all_tasks() - tasks_before if not task.done()]


@pytest.mark.asyncio
@pytest.mark.parametrize("provider,model,base,usage_mode", [
    (*case[:3], mode) for case in _CASES for mode in ("missing", "partial")
    # LiteLLM rejects absent native Anthropic/Google usage before returning a
    # response. OpenAI-compatible omission and all partial usage normalize.
    if mode == "partial" or case[0] == "openai"
])
async def test_real_harbor_marks_provider_usage_omissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, usage_mode: str,
    provider: str, model: str, base: str,
) -> None:
    from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

    def respond(request: httpx.Request) -> httpx.Response:
        body = json.loads(_response(request, model=model).content)
        usage_key = "usageMetadata" if provider == "google" else "usage"
        output_key = {"google": "candidatesTokenCount", "anthropic": "output_tokens"}.get(
            provider, "completion_tokens"
        )
        if usage_mode == "missing":
            body.pop(usage_key)
        else:
            body[usage_key].pop(output_key)
        return httpx.Response(200, request=request, json=body)

    _install_transport(monkeypatch, respond)
    runtime, _, driver, writer = _runtime(tmp_path, provider, model, base)
    await driver.start()
    try:
        async with writer:
            await runtime.run(instruction="Finish now.", env=driver, trajectory=writer,
                              mcp=[], skills_dir=None, step_id="agent")
        events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
        calls = [event for event in events if event["kind"] == "llm_call"]
        turns = [event for event in events if event["kind"] == "terminus2_turn"]
        assert len(calls) == len(turns) == 2
        assert all(call["usage_status"] == usage_mode for call in calls)
        assert {call["gateway_request_id"] for call in calls} == {
            turn["gateway_request_id"] for turn in turns
        }
    finally:
        await driver.stop()
        await GLOBAL_LOGGING_WORKER.stop()


@pytest.mark.asyncio
async def test_real_harbor_retry_joins_only_current_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(request, model="gpt-4o-mini")

    _install_transport(monkeypatch, respond)
    runtime, ledger, driver, writer = _runtime(tmp_path, "openai", "gpt-4o-mini", "https://openai.test/v1")
    await driver.start()
    try:
        async with writer:
            for _ in range(2):
                await runtime.run(instruction="Finish now.", env=driver, trajectory=writer,
                                  mcp=[], skills_dir=None, step_id="agent")
        events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
        calls = [event for event in events if event["kind"] == "llm_call"]
        turns = [event for event in events if event["kind"] == "terminus2_turn"]
        assert len(calls) == len(turns) == len(requests) == 4
        assert [call["gateway_request_id"] for call in calls] == [
            turn["gateway_request_id"] for turn in turns
        ]
        assert len(await ledger.get_trial_llm_calls(runtime.trial_id)) == 2
    finally:
        await driver.stop()
        await GLOBAL_LOGGING_WORKER.stop()
