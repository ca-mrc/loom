"""Local relay tests use real listening sockets and deterministic upstreams."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest

from loom.agent.direct_provider import DirectProviderConnection
from loom.models.types import ModelSpec
from loom_cli.claude_gateway import LocalClaudeRelay
from loom_cli.rate_cards import RateCardEntry, RateCardTable
from tests.unit.test_anthropic_chat_stream import _wire
from tests.unit.test_claude_gateway_lifecycle import _SlowNativeStream


def _relay(dialect, handler, rates=()):
    model = ModelSpec(provider="local", name="endpoint/code-model")
    return LocalClaudeRelay(trial_id=uuid4(), model=model,
        connection=DirectProviderConnection(model=ModelSpec(provider="anthropic" if dialect == "anthropic" else "openai", name="code-model"),
            base_url="https://upstream.test/prefix/v1", api_key="selected-upstream-secret", api_dialect=dialect),
        transport=httpx.MockTransport(handler), rate_cards=RateCardTable(entries=rates))


def _payload(**extra):
    return {"model": "code-model", "max_tokens": 100, "messages": [{"role": "user", "content": "hello"}], **extra}


@pytest.mark.parametrize("dialect", ["anthropic", "openai_chat"])
async def test_relay_binds_model_auth_route_and_usage_without_exposing_provider_key(dialect):
    calls = []
    def upstream(request):
        calls.append(request)
        if dialect == "anthropic":
            return httpx.Response(200, json={"id": "m", "type": "message", "role": "assistant", "model": "provider-alias",
                "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                "usage": {"input_tokens": 12, "output_tokens": 4}})
        return httpx.Response(200, json={"id": "m", "model": "provider-alias", "choices": [{"index": 0,
            "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 4}})
    relay = _relay(dialect, upstream)
    await relay.start(host="127.0.0.1")
    url = relay.base_url
    try:
        async with httpx.AsyncClient(base_url=url, trust_env=False) as client:
            assert (await client.post("/v1/messages", json=_payload())).status_code == 401
            headers = {"Authorization": f"Bearer {relay.token}"}
            assert (await client.post("/v1/messages", headers=headers, json=_payload())).status_code == 403
            relay.activate(step_id="main")
            assert (await client.post("/v1/messages", headers=headers, json=_payload(model="other"))).status_code == 403
            result = await client.post("/v1/messages", headers=headers, json=_payload())
            assert result.status_code == 200 and result.json()["content"][0]["text"] == "ok"
            assert "selected-upstream-secret" not in result.text
            await relay.deactivate()
            assert (await client.post("/v1/messages", headers=headers, json=_payload())).status_code == 403
        assert len(calls) == 1
        assert calls[0].url.path == ("/prefix/v1/messages" if dialect == "anthropic" else "/prefix/v1/chat/completions")
        assert calls[0].headers.get("x-api-key" if dialect == "anthropic" else "authorization") == (
            "selected-upstream-secret" if dialect == "anthropic" else "Bearer selected-upstream-secret")
        rows = await relay.get_trial_llm_calls(relay.trial_id)
        assert len(rows) == 1 and rows[0]["model"] == "endpoint/code-model" and rows[0]["input_tokens"] == 12
        assert rows[0]["provider_extras"]["_loom_model_identity"]["provider"] == "local"
        assert rows[0]["provider_extras"]["_loom_cost_confidence"] == "unavailable"
        assert rows[0]["step_id"] == "main" and "selected-upstream-secret" not in str(rows)
    finally:
        await relay.aclose()
    async with httpx.AsyncClient(trust_env=False) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get(url + "/health")


async def test_translated_stream_preserves_tools_and_distinguishes_configured_zero_price():
    relay = _relay("openai_chat", lambda _: httpx.Response(200, content=_wire(), headers={"content-type": "text/event-stream"}),
        rates=(RateCardEntry("local:endpoint", "code-model", 0, 0, 0, 0),))
    await relay.start(host="127.0.0.1")
    try:
        relay.activate(step_id="main")
        async with httpx.AsyncClient(base_url=relay.base_url, trust_env=False) as client:
            result = await client.post("/v1/messages", headers={"Authorization": f"Bearer {relay.token}"}, json=_payload(stream=True))
            assert result.status_code == 200 and '"tool_use"' in result.text and "message_stop" in result.text
        rows = await relay.get_trial_llm_calls(relay.trial_id)
        assert len(rows) == 1 and rows[0]["input_tokens"] == 70
        assert rows[0]["provider_extras"]["cache_read_input_tokens"] == 30
        assert rows[0]["provider_extras"]["_loom_cost_confidence"] == "configured"
    finally:
        await relay.aclose()


async def test_disconnect_closes_upstream_and_retains_partial_observed_usage():
    slow = _SlowNativeStream()
    relay = _relay("anthropic", lambda _: httpx.Response(200, stream=slow, headers={"content-type": "text/event-stream"}))
    await relay.start(host="127.0.0.1")
    try:
        relay.activate(step_id="main")
        async with httpx.AsyncClient(base_url=relay.base_url, trust_env=False) as client:
            async with client.stream("POST", "/v1/messages", headers={"Authorization": f"Bearer {relay.token}"}, json=_payload(stream=True)) as response:
                await anext(response.aiter_bytes())
                await asyncio.wait_for(slow.waiting.wait(), 2)
            for _ in range(100):
                if slow.closed:
                    break
                await asyncio.sleep(.01)
        await relay.deactivate()
        rows = await relay.get_trial_llm_calls(relay.trial_id)
        assert slow.closed and len(rows) == 1 and rows[0]["input_tokens"] == 12
        assert rows[0]["provider_extras"]["_loom_call_status"] == "failed"
        assert rows[0]["provider_extras"]["_loom_usage_status"] == "partial"
    finally:
        await relay.aclose()


async def test_concurrent_trials_have_independent_tokens_models_and_phase_authority():
    first, second = [_relay("anthropic", lambda _: httpx.Response(401, text="selected-upstream-secret")) for _ in range(2)]
    await asyncio.gather(first.start(host="127.0.0.1"), second.start(host="127.0.0.1"))
    try:
        first.activate(step_id="main")
        second.activate(step_id="main")
        await first.deactivate()
        async with httpx.AsyncClient(base_url=second.base_url, trust_env=False) as client:
            assert (await client.post("/v1/messages", headers={"Authorization": f"Bearer {first.token}"}, json=_payload())).status_code == 401
            failure = await client.post("/v1/messages", headers={"Authorization": f"Bearer {second.token}"}, json=_payload())
            assert failure.status_code == 401 and "selected-upstream-secret" not in failure.text
        assert await first.get_trial_llm_calls(first.trial_id) == []
        rows = await second.get_trial_llm_calls(second.trial_id)
        assert len(rows) == 1 and rows[0]["provider_extras"]["_loom_failure_category"] == "upstream_http_4xx"
    finally:
        await asyncio.gather(first.aclose(), second.aclose())
