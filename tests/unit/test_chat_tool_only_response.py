"""Tool-only upstream messages must reach callers and successful call accounting."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom_llm_gateway.rate_card import RateCardEntry, RateCardTable
from loom_llm_gateway.routes import chat


@pytest.mark.parametrize("include_content", [True, False], ids=["null", "missing"])
async def test_chat_tool_only_response_is_returned_and_accounted_once(
    monkeypatch: pytest.MonkeyPatch, include_content: bool,
) -> None:
    team_id, trial_id = uuid4(), uuid4()
    ctx = AuthContext(
        token_hash=b"", type="step", scopes=["llm:call"], team_id=team_id,
        expires_at=None, trial_id=trial_id, step_id="main",
    )
    upstream = {
        "id": "chatcmpl-tool-only",
        "object": "chat.completion",
        "created": 1717000000,
        "model": "tool-model",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "tool_calls": [{
                    "id": "call_1", "type": "function",
                    "function": {"name": "lookup", "arguments": '{"key":"value"}'},
                }],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {
            "prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14,
            "cache_read_input_tokens": 3, "cache_creation_input_tokens": 2,
        },
    }
    if include_content:
        upstream["choices"][0]["message"]["content"] = None
    original = deepcopy(upstream)
    table = RateCardTable(
        id="tool-response-card", captured_at=datetime(2026, 1, 1, tzinfo=UTC),
        entries=[RateCardEntry(
            provider="openai", model="tool-model", input_per_mtok=2,
            output_per_mtok=5, cache_read_per_mtok=0.5, cache_write_per_mtok=3,
        )],
    )
    session = AsyncMock(spec=AsyncSession)
    session.__aenter__.return_value = session
    app = FastAPI()
    app.include_router(chat.router)
    app.state.settings = SimpleNamespace(
        step_jwt_signing_key=SecretStr("local-test-key"), upstream_timeout_sec=10,
        openai_api_key=SecretStr("stub"),
    )
    app.state.session_factory = lambda: session
    app.state.rate_card_cache = SimpleNamespace(get=AsyncMock(return_value=table))
    # Stub only provider/database boundaries; parsing, pricing and record_call run normally.
    monkeypatch.setattr(chat, "require_llm_call_bearer", AsyncMock(return_value=ctx))
    monkeypatch.setattr(chat, "authorize_trial_execution_dispatch", AsyncMock())
    monkeypatch.setattr(chat, "authorize_trial_model", AsyncMock())
    monkeypatch.setattr(chat.litellm_wrapper, "acompletion", AsyncMock(return_value=upstream))
    monkeypatch.setattr(
        "loom_llm_gateway.llm_calls.ensure_trial_event_lifecycle_authority",
        AsyncMock(return_value=uuid4()),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer stub"},
            json={
                "model": "openai/tool-model",
                "messages": [{"role": "user", "content": "Look up the value"}],
                "tools": [{
                    "type": "function",
                    "function": {"name": "lookup", "parameters": {"type": "object"}},
                }],
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    usage = body.pop("loom")
    assert body == original
    assert upstream == original
    assert usage["finish_reason"] == "tool_calls"
    assert usage["input_tokens"] == 10
    assert usage["output_tokens"] == 4
    assert usage["cached_input_tokens"] == 3
    assert usage["cache_write_tokens"] == 2
    # Existing additive pricing: (10*2 + 4*5 + 3*0.5 + 2*3) / 1_000_000.
    assert usage["cost_usd"] == pytest.approx(0.0000475)
    session.execute.assert_awaited_once()
    stored = session.execute.await_args.args[0].compile().params
    assert stored["trial_id"] == trial_id
    assert stored["team_id"] == team_id
    assert stored["step_id"] == "main"
    assert stored["dialect"] == "openai_chat"
    assert stored["model"] == "tool-model"
    assert stored["input_tokens"] == 10
    assert stored["output_tokens"] == 4
    assert stored["provider_extras"] == {
        "cache_read_input_tokens": 3, "cache_creation_input_tokens": 2,
    }
    assert stored["cost_usd"] == pytest.approx(0.0000475)
    assert stored["rate_card_hash"] == usage["rate_card_hash"]
    assert stored["attempt"] == 1
    session.commit.assert_awaited_once()
