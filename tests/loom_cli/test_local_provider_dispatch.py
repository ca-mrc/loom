"""UpstreamDirectGatewayClient._call_local — dispatch a chat-completion
request against a configured local server. We mock the openai
AsyncOpenAI client so no real network or local server is required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from loom.agent.gateway_client import GatewayCallRequest
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_cli.config import LocalProvider
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient


def _fake_openai_response(text: str, in_tok: int, out_tok: int) -> Any:
    """Construct a minimal object that quacks like
    openai.types.chat.ChatCompletion."""
    msg = type("Msg", (), {"content": text})()
    choice = type("Choice", (), {"message": msg, "finish_reason": "stop"})()
    usage = type("Usage", (), {
        "prompt_tokens": in_tok, "completion_tokens": out_tok,
    })()
    return type("Resp", (), {"choices": [choice], "usage": usage})()


@pytest.fixture
def gateway_with_vllm(tmp_xdg_home) -> UpstreamDirectGatewayClient:  # type: ignore[no-untyped-def]
    return UpstreamDirectGatewayClient(
        anthropic_client=None,
        openai_client=None,
        google_client=None,
        tokens={},
        local_providers={
            "vllm": LocalProvider(base_url="http://localhost:8000/v1"),
        },
    )


async def test_call_local_dispatches_with_correct_base_url(
    gateway_with_vllm: UpstreamDirectGatewayClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the local-provider's base_url makes it into the openai
    AsyncOpenAI client + the model_id is the third path component."""
    captured: dict[str, Any] = {}

    class FakeChatCompletions:
        async def create(self, **kwargs: Any) -> Any:
            captured["model"] = kwargs["model"]
            captured["messages"] = kwargs["messages"]
            return _fake_openai_response("hi from vllm", 10, 5)

    class FakeChat:
        completions = FakeChatCompletions()

    class FakeAsyncOpenAI:
        def __init__(self, *, base_url: str, api_key: str, http_client: Any) -> None:
            captured["base_url"] = base_url
            captured["api_key"] = api_key
            self.chat = FakeChat()

    monkeypatch.setattr(
        "openai.AsyncOpenAI", FakeAsyncOpenAI,
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="vllm/llama-3.1-8b"),
        messages=[ChatMessage(role="user", content="hello")],
        system_prompt="be brief",
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    resp = await gateway_with_vllm.call(req)
    assert resp.response.content == "hi from vllm"
    assert resp.input_tokens == 10
    assert resp.output_tokens == 5
    assert resp.cost_usd == 0.0  # default for local (no rate-card entry)
    # The local server got the third part of the model spec, not the
    # whole `vllm/llama-3.1-8b`.
    assert captured["model"] == "llama-3.1-8b"
    # base_url came from the registered local provider config.
    assert captured["base_url"] == "http://localhost:8000/v1"
    # No api_key was set → "EMPTY" placeholder (vLLM/ollama accept).
    assert captured["api_key"] == "EMPTY"
    # System prompt was prepended as a `system` role message.
    assert captured["messages"][0] == {
        "role": "system", "content": "be brief",
    }


async def test_call_local_unknown_provider_raises_with_fix_hint(
    gateway_with_vllm: UpstreamDirectGatewayClient,
) -> None:
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="ollama/llama-3.1-8b"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    with pytest.raises(ValueError) as exc:
        await gateway_with_vllm.call(req)
    msg = str(exc.value)
    assert "ollama" in msg
    assert "loom config set local.ollama.base_url" in msg
    # Helpful list of what IS registered.
    assert "['vllm']" in msg


async def test_call_local_rejects_bare_model_id_without_server_name(
    gateway_with_vllm: UpstreamDirectGatewayClient,
) -> None:
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="llama-3.1-8b"),  # no slash
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    with pytest.raises(ValueError, match="local/<server>/<model>"):
        await gateway_with_vllm.call(req)


async def test_call_local_uses_configured_api_key(
    monkeypatch: pytest.MonkeyPatch, tmp_xdg_home,  # type: ignore[no-untyped-def]
) -> None:
    captured: dict[str, str] = {}

    class FakeAsyncOpenAI:
        def __init__(self, *, base_url: str, api_key: str, http_client: Any) -> None:
            captured["api_key"] = api_key
            self.chat = type("C", (), {
                "completions": type("CC", (), {
                    "create": AsyncMock(
                        return_value=_fake_openai_response("ok", 1, 1),
                    ),
                })(),
            })()

    monkeypatch.setattr("openai.AsyncOpenAI", FakeAsyncOpenAI)
    gateway = UpstreamDirectGatewayClient(
        anthropic_client=None, openai_client=None, google_client=None,
        tokens={},
        local_providers={
            "vllm": LocalProvider(
                base_url="http://localhost:8000/v1",
                api_key="sk-private-cluster-key",
            ),
        },
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="vllm/llama-3.1-8b"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    await gateway.call(req)
    assert captured["api_key"] == "sk-private-cluster-key"


async def test_local_client_cached_across_calls(
    gateway_with_vllm: UpstreamDirectGatewayClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-turn agent loop should reuse the same openai AsyncOpenAI
    instance per local server, not reconstruct one per call."""
    construct_count = 0

    class FakeAsyncOpenAI:
        def __init__(self, **_: Any) -> None:
            nonlocal construct_count
            construct_count += 1
            self.chat = type("C", (), {
                "completions": type("CC", (), {
                    "create": AsyncMock(
                        return_value=_fake_openai_response("x", 1, 1),
                    ),
                })(),
            })()

    monkeypatch.setattr("openai.AsyncOpenAI", FakeAsyncOpenAI)
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="vllm/llama-3.1-8b"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    await gateway_with_vllm.call(req)
    await gateway_with_vllm.call(req)
    await gateway_with_vllm.call(req)
    assert construct_count == 1


async def test_call_local_uses_rate_card_override_when_present(
    monkeypatch: pytest.MonkeyPatch, tmp_xdg_home,  # type: ignore[no-untyped-def]
) -> None:
    """Operators who want internal cost accounting against their own
    GPU rates add a `[[entries]]` row with provider="local:<server>".
    Confirm that path produces a non-zero cost instead of the default
    $0."""
    # Write a rate-cards.toml with a row for our local server
    import tomli_w

    from loom_cli.rate_cards import rate_cards_path
    rc_path = rate_cards_path()
    rc_path.parent.mkdir(parents=True, exist_ok=True)
    rc_path.write_text(tomli_w.dumps({
        "entries": [{
            "provider": "local:vllm",
            "model": "llama-3.1-8b",
            "input_per_mtok": 0.10,
            "output_per_mtok": 0.30,
            "cache_read_per_mtok": 0.0,
            "cache_write_per_mtok": 0.0,
        }],
    }))

    class FakeAsyncOpenAI:
        def __init__(self, **_: Any) -> None:
            self.chat = type("C", (), {
                "completions": type("CC", (), {
                    "create": AsyncMock(
                        # 1M input, 1M output → $0.10 + $0.30 = $0.40
                        return_value=_fake_openai_response("ok", 1_000_000, 1_000_000),
                    ),
                })(),
            })()

    monkeypatch.setattr("openai.AsyncOpenAI", FakeAsyncOpenAI)
    gateway = UpstreamDirectGatewayClient(
        anthropic_client=None, openai_client=None, google_client=None,
        tokens={},
        local_providers={
            "vllm": LocalProvider(base_url="http://localhost:8000/v1"),
        },
    )
    req = GatewayCallRequest(
        model=ModelSpec(provider="local", name="vllm/llama-3.1-8b"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None, tool_choice=None,
        team_id="t1", trial_id="t2", step_id="s1",
    )
    resp = await gateway.call(req)
    # 1M input × $0.10/Mtok + 1M output × $0.30/Mtok = $0.40
    assert abs(resp.cost_usd - 0.40) < 1e-9


def test_run_cmd_parses_local_model_spec(tmp_xdg_home) -> None:  # type: ignore[no-untyped-def]
    """End-to-end shape: `--model local/vllm/llama-3.1-8b` parses into
    a ModelSpec(provider='local', name='vllm/llama-3.1-8b'). Catches
    regressions in the model-spec parser when adding new providers."""
    from loom_cli.model_spec import parse_model

    spec = parse_model("local/vllm/llama-3.1-8b")
    assert spec.provider == "local"
    assert spec.name == "vllm/llama-3.1-8b"


def test_run_cmd_parses_local_model_spec_with_slashes_in_id(
    tmp_xdg_home,  # type: ignore[no-untyped-def]
) -> None:
    """Hugging Face model ids contain slashes (org/name). Make sure
    they survive the spec parser, e.g.
    `local/vllm/meta-llama/Llama-3.1-8B-Instruct` →
    name = `vllm/meta-llama/Llama-3.1-8B-Instruct`. _call_local then
    splits on the FIRST `/` so server='vllm',
    model_id='meta-llama/Llama-3.1-8B-Instruct'."""
    from loom_cli.model_spec import parse_model

    spec = parse_model("local/vllm/meta-llama/Llama-3.1-8B-Instruct")
    assert spec.provider == "local"
    assert spec.name == "vllm/meta-llama/Llama-3.1-8B-Instruct"
