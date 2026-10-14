"""UpstreamDirectGatewayClient calls provider SDKs directly and computes
cost from the local rate-card. The SDKs themselves are stubbed; this
test only verifies the call shape + cost math."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pytest

from loom.agent.gateway_client import GatewayCallRequest
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_cli.rate_cards import seed_default_if_missing
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient


@dataclass
class _FakeAnthropic:
    """Minimal stand-in for anthropic.AsyncAnthropic."""

    last_kwargs: dict[str, Any] | None = None

    class _Messages:
        def __init__(self, outer: _FakeAnthropic) -> None:
            self._outer = outer

        async def create(self, **kwargs: Any) -> Any:
            self._outer.last_kwargs = kwargs
            return _FakeAnthropicResponse()

    @property
    def messages(self) -> _FakeAnthropic._Messages:
        return self._Messages(self)


class _FakeAnthropicResponse:
    stop_reason = "end_turn"

    class _Content:
        type = "text"
        text = "Hello"

    content: ClassVar[list[_Content]] = [_Content()]

    class _Usage:
        input_tokens = 12
        output_tokens = 5
        cache_creation_input_tokens = 0
        cache_read_input_tokens = 0

    usage = _Usage()


@pytest.mark.asyncio
async def test_anthropic_call_records_tokens_and_cost(
    tmp_xdg_home: Path,
) -> None:
    seed_default_if_missing()
    fake = _FakeAnthropic()
    client = UpstreamDirectGatewayClient(
        anthropic_client=fake,  # type: ignore[arg-type]
        openai_client=None,
        google_client=None,
        tokens={"anthropic": "sk-ant-xxx"},
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt="You are helpful.",
        tools=None, tool_choice=None,
        team_id="00000000-0000-0000-0000-000000000000",
        trial_id="11111111-1111-1111-1111-111111111111",
        step_id="step-1",
    )
    resp = await client.call(req)
    assert resp.response.role == "assistant"
    assert resp.response.content == "Hello"
    assert resp.input_tokens == 12
    assert resp.output_tokens == 5
    assert resp.cost_usd == pytest.approx(0.000555, rel=1e-6)
    assert resp.finish_reason == "stop"
    assert fake.last_kwargs is not None
    assert fake.last_kwargs["model"] == "claude-opus-4-7"
    assert fake.last_kwargs["system"] == "You are helpful."


@pytest.mark.asyncio
async def test_missing_token_raises(tmp_xdg_home: Path) -> None:
    seed_default_if_missing()
    client = UpstreamDirectGatewayClient(
        anthropic_client=_FakeAnthropic(),  # type: ignore[arg-type]
        openai_client=None, google_client=None, tokens={},
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None, tools=None, tool_choice=None,
        team_id="t", trial_id="x", step_id="s",
    )
    with pytest.raises(ValueError, match="anthropic API key"):
        await client.call(req)


@dataclass
class _FakeOpenAIChatChoice:
    class _Msg:
        role = "assistant"
        content = "openai reply"
        tool_calls = None
    finish_reason = "stop"
    message: ClassVar[_Msg] = _Msg()


@dataclass
class _FakeOpenAIResponse:
    class _Usage:
        prompt_tokens = 7
        completion_tokens = 3
        prompt_tokens_details = type("D", (), {"cached_tokens": 0})()
    usage: ClassVar[_Usage] = _Usage()
    choices: ClassVar[list[_FakeOpenAIChatChoice]] = [_FakeOpenAIChatChoice()]


@dataclass
class _FakeOpenAI:
    last_kwargs: dict[str, Any] | None = None

    class _Completions:
        def __init__(self, outer: _FakeOpenAI) -> None:
            self._outer = outer

        async def create(self, **kwargs: Any) -> _FakeOpenAIResponse:
            self._outer.last_kwargs = kwargs
            return _FakeOpenAIResponse()

    class _Chat:
        def __init__(self, outer: _FakeOpenAI) -> None:
            self.completions = _FakeOpenAI._Completions(outer)

    @property
    def chat(self) -> _FakeOpenAI._Chat:
        return self._Chat(self)


@pytest.mark.asyncio
async def test_openai_call_records_tokens_and_cost(
    tmp_xdg_home: Path,
) -> None:
    seed_default_if_missing()
    fake = _FakeOpenAI()
    client = UpstreamDirectGatewayClient(
        anthropic_client=None, openai_client=fake,  # type: ignore[arg-type]
        google_client=None, tokens={"openai": "sk-oai-xxx"},
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="openai", name="gpt-4o-mini"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt="be helpful",
        tools=None, tool_choice=None,
        team_id="t", trial_id="x", step_id="s",
    )
    resp = await client.call(req)
    assert resp.response.content == "openai reply"
    assert resp.input_tokens == 7
    assert resp.output_tokens == 3
    assert resp.cost_usd == pytest.approx(
        7 * 0.15 / 1_000_000 + 3 * 0.6 / 1_000_000, rel=1e-6,
    )
    assert fake.last_kwargs is not None
    assert fake.last_kwargs["model"] == "gpt-4o-mini"
    assert fake.last_kwargs["messages"][0] == {
        "role": "system", "content": "be helpful",
    }
