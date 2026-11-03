"""Protocol selection must not replace the admitted provider/model identity."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from loom.db.schema import LlmCall
from loom.llm_call_ledger import serialize_execution_accounting_call
from loom.trajectory.llm_call_events import llm_call_row_to_event
from loom_llm_gateway.execution_attempt_dispatch import authorize_anthropic_trial_model
from loom_service.provider_connections_service import (
    _preflight_base_path_headers_body,
    _probe_url_and_headers,
)


@pytest.mark.parametrize("suffix", ["", "/", "/v1", "/v1/"])
def test_anthropic_discovery_preflight_and_runtime_share_the_same_prefix(suffix):
    base = "https://relay.example/proxy" + suffix
    url, headers = _probe_url_and_headers("anthropic", base, "key")
    preflight_base, path, _, _ = _preflight_base_path_headers_body("anthropic", base, "key", "m")
    assert url == "https://relay.example/proxy/v1/models"
    assert preflight_base + path == "https://relay.example/proxy/v1/messages"
    assert headers["x-api-key"] == "key"


def _authority(provider="vendor", name="code-model", mode="chat"):
    config = {"agent_name": "claude-code", "agent_model": {"provider": provider, "name": name},
              "claude_code_mode": mode}
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: config)))

    @asynccontextmanager
    async def factory():
        yield session

    request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace(session_factory=factory))})
    ctx = SimpleNamespace(service_execution_lease_id=uuid4(), trial_id=uuid4())
    return request, ctx


@pytest.mark.parametrize("provider", ["anthropic", "openai", "local", "vendor"])
@pytest.mark.parametrize("mode,connection_type", [("native", "anthropic"), ("chat", "openai-compatible"), ("chat", "custom")])
async def test_authorization_preserves_the_frozen_namespace(provider, mode, connection_type):
    request, ctx = _authority(provider=provider, mode=mode)
    identity = await authorize_anthropic_trial_model(request, ctx, model="code-model", connection_type=connection_type)
    assert identity.provider == provider and identity.name == "code-model"


@pytest.mark.parametrize("model,connection_type", [("other", "openai-compatible"), ("code-model", "anthropic")])
async def test_wrong_model_or_connection_mode_is_forbidden(model, connection_type):
    request, ctx = _authority()
    with pytest.raises(HTTPException) as exc:
        await authorize_anthropic_trial_model(request, ctx, model=model, connection_type=connection_type)
    assert exc.value.status_code == 403


def test_trusted_identity_and_route_survive_safe_ledger_and_typed_export():
    row = LlmCall(id=uuid4(), trial_id=uuid4(), step_id="agent", model="code-model",
                  dialect="anthropic_facade", input_tokens=100, output_tokens=10,
                  cost_usd=Decimal("0"), rate_card_hash="unknown", captured_at=datetime.now(UTC), attempt=1,
                  provider_extras={"_loom_model_identity": {"provider": "vendor", "name": "code-model"},
                                   "_loom_gateway_route": "anthropic_to_chat",
                                   "_loom_compatibility_profile": "claude-code-chat-v1",
                                   "private": "secret"})
    safe = serialize_execution_accounting_call(row)
    event = llm_call_row_to_event(safe, trial_id=row.trial_id, seq=0)
    assert event.model.provider == "vendor" and event.model.name == "code-model"
    assert event.gateway_route == "anthropic_to_chat"
    assert event.compatibility_profile == "claude-code-chat-v1"
    assert "secret" not in event.model_dump_json()
    legacy = llm_call_row_to_event({"model": "old", "dialect": "anthropic_facade"}, trial_id=uuid4(), seq=0)
    assert legacy.model.provider == "anthropic"
    assert "gateway_route" not in legacy.model_dump()


def test_chat_reasoning_tokens_remain_observed_counters():
    from loom_llm_gateway.dialect import DIALECTS
    usage = DIALECTS["openai_chat"].extract_tokens({"usage": {"prompt_tokens": 10, "completion_tokens": 8,
        "completion_tokens_details": {"reasoning_tokens": 5}}})
    assert usage.output_tokens == 8 and usage.provider_extras["reasoning_tokens"] == 5


@pytest.mark.parametrize("confidence,expected", [("unavailable", "price_unknown"), ("not_applicable", "price_unknown"), ("configured", "priced")])
def test_export_distinguishes_unknown_price_from_a_configured_zero(confidence, expected):
    row = LlmCall(id=uuid4(), trial_id=uuid4(), step_id="agent", model="m",
                  dialect="anthropic_facade", input_tokens=10, output_tokens=1,
                  cost_usd=Decimal("0"), rate_card_hash="rate", captured_at=datetime.now(UTC), attempt=1,
                  provider_extras={"_loom_cost_confidence": confidence})
    safe = serialize_execution_accounting_call(row)
    event = llm_call_row_to_event(safe, trial_id=row.trial_id, seq=0)
    assert event.cost_status == expected and event.cost_usd_snapshot == 0


@pytest.mark.parametrize("failure", ["upstream_protocol", "client_cancelled"])
def test_stream_failure_status_survives_typed_export(failure):
    event = llm_call_row_to_event({"model": "m", "dialect": "anthropic_facade", "provider_extras": {
        "_loom_call_status": "failed", "_loom_usage_status": "partial", "_loom_failure_category": failure,
    }}, trial_id=uuid4(), seq=0)
    assert event.failure_category == failure
