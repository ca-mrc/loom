"""Exercise Google requests and responses through the installed SDK, offline."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from google import genai
from google.genai import errors, types

from loom.agent.gateway_client import GatewayCallRequest
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_cli.rate_cards import RateCardEntry, RateCardTable
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient


def _request(system_prompt: str | None = None) -> GatewayCallRequest:
    return GatewayCallRequest(
        model=ModelSpec(provider="google", name="gemini-2.5-flash"),
        messages=[
            ChatMessage(role="user", content="First question"),
            ChatMessage(role="assistant", content="Earlier answer"),
            ChatMessage(role="user", content="Follow-up"),
        ],
        system_prompt=system_prompt,
        tools=None,
        tool_choice=None,
        team_id="t",
        trial_id="x",
        step_id="s",
    )


@asynccontextmanager
async def _gateway(
    response: dict[str, Any],
    *,
    status_code: int = 200,
    transport_error: BaseException | None = None,
) -> AsyncIterator[tuple[UpstreamDirectGatewayClient, list[httpx.Request]]]:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if transport_error is not None:
            raise transport_error
        return httpx.Response(status_code, json=response)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        sdk = genai.Client(
            api_key="offline-google-test-key",
            vertexai=False,
            enterprise=False,
            http_options=types.HttpOptions(
                base_url="https://google-sdk.invalid",
                httpx_async_client=http_client,
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )
        gateway = UpstreamDirectGatewayClient(
            anthropic_client=None,
            openai_client=None,
            google_client=sdk,
            tokens={"google": "offline-google-test-key"},
            _rate_cards=RateCardTable(entries=(RateCardEntry(
                provider="google",
                model="gemini-2.5-flash",
                input_per_mtok=2.0,
                output_per_mtok=5.0,
                cache_read_per_mtok=0.5,
                cache_write_per_mtok=0.0,
            ),)),
        )
        try:
            yield gateway, requests
        finally:
            await sdk.aio.aclose()
            sdk.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("system_prompt", [None, "Be helpful.", ""])
async def test_google_serializes_model_messages_and_system_instruction(
    system_prompt: str | None,
) -> None:
    payload = {
        "candidates": [{
            "content": {"role": "model", "parts": [{"text": "Hello"}]},
            "finishReason": "STOP",
        }],
        "usageMetadata": {"promptTokenCount": 9, "candidatesTokenCount": 4},
    }
    async with _gateway(payload) as (gateway, requests):
        response = await gateway.call(_request(system_prompt))

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url.path == "/v1beta/models/gemini-2.5-flash:generateContent"
    assert request.headers["x-goog-api-key"] == "offline-google-test-key"
    body = json.loads(request.content)
    assert body["contents"] == [
        {"role": "user", "parts": [{"text": "First question"}]},
        {"role": "model", "parts": [{"text": "Earlier answer"}]},
        {"role": "user", "parts": [{"text": "Follow-up"}]},
    ]
    if system_prompt is None:
        assert "systemInstruction" not in body
    else:
        assert body["systemInstruction"] == {
            "role": "user", "parts": [{"text": system_prompt}],
        }
    assert response.response.role == "assistant"
    assert response.response.content == "Hello"
    assert response.finish_reason == "stop"
    assert response.input_tokens == 9
    assert response.output_tokens == 4
    assert response.cost_usd == pytest.approx(0.000038)
    assert response.duration_sec >= 0
    assert response.streamed is False
    assert response.time_to_first_token_sec is None
    assert len(response.rate_card_hash) == 64
    assert response.gateway_request_id.startswith("upstream-direct-")


@pytest.mark.asyncio
async def test_google_filters_nontext_and_thought_parts_and_records_usage() -> None:
    payload = {
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"text": "Do not expose this reasoning", "thought": True},
                {"text": "Visible "},
                {"inlineData": {"mimeType": "image/png", "data": "aW1hZ2U="}},
                {"functionCall": {"name": "unused", "args": {}}},
                {"text": None},
                {"text": "answer", "thought": False},
            ]},
            "finishReason": "STOP",
        }],
        "usageMetadata": {
            "promptTokenCount": 100,
            "candidatesTokenCount": 20,
            "cachedContentTokenCount": 40,
            "thoughtsTokenCount": 7,
            "totalTokenCount": 127,
        },
    }
    async with _gateway(payload) as (gateway, _requests):
        response = await gateway.call(_request())

    assert response.response.content == "Visible answer"
    assert response.input_tokens == 100
    assert response.output_tokens == 20
    assert response.cached_input_tokens == 40
    assert response.cache_write_tokens == 0
    assert response.thinking_tokens == 7
    assert response.provider_extras == {
        "cachedContentTokenCount": 40,
        "thoughtsTokenCount": 7,
    }
    # Preserve local rate-card semantics: prompt + candidate + cache charges.
    assert response.cost_usd == pytest.approx(0.00032)


@pytest.mark.asyncio
@pytest.mark.parametrize(("reason", "expected"), [
    ("STOP", "stop"),
    ("MAX_TOKENS", "length"),
    ("SAFETY", "content_filter"),
    ("RECITATION", "content_filter"),
    ("BLOCKLIST", "content_filter"),
    ("PROHIBITED_CONTENT", "content_filter"),
    ("SPII", "content_filter"),
    ("IMAGE_SAFETY", "content_filter"),
    ("IMAGE_PROHIBITED_CONTENT", "content_filter"),
    ("IMAGE_RECITATION", "content_filter"),
    ("MALFORMED_FUNCTION_CALL", "malformed_function_call"),
    ("OTHER", "other"),
    ("FINISH_REASON_UNSPECIFIED", "unknown"),
    (None, "unknown"),
])
async def test_google_normalizes_sdk_finish_reasons(
    reason: str | None, expected: str,
) -> None:
    payload = {"candidates": [{
        "content": {"parts": [{"text": "Reply"}]},
        "finishReason": reason,
    }]}
    async with _gateway(payload) as (gateway, _requests):
        response = await gateway.call(_request())

    assert response.finish_reason == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {},
    {"candidates": []},
    {"candidates": None},
    {"candidates": [{"finishReason": "SAFETY"}]},
    {"candidates": [{"content": None, "finishReason": "SAFETY"}]},
    {"candidates": [{"content": {}, "finishReason": "SAFETY"}]},
    {"candidates": [{"content": {"parts": []}, "finishReason": "SAFETY"}]},
    {"candidates": [{"content": {"parts": None}, "finishReason": "SAFETY"}]},
    {"usageMetadata": {}},
    {"usageMetadata": {
        "promptTokenCount": None,
        "candidatesTokenCount": None,
        "cachedContentTokenCount": None,
        "thoughtsTokenCount": None,
    }},
])
async def test_google_handles_empty_content_and_absent_usage(
    payload: dict[str, Any],
) -> None:
    async with _gateway(payload) as (gateway, _requests):
        response = await gateway.call(_request())

    assert response.response.content == ""
    assert response.input_tokens == 0
    assert response.output_tokens == 0
    assert response.cached_input_tokens == 0
    assert response.cache_write_tokens == 0
    assert response.thinking_tokens == 0
    assert response.cost_usd == 0
    assert response.finish_reason in {"unknown", "content_filter"}


@pytest.mark.asyncio
async def test_google_preserves_prompt_block_reason_without_candidates() -> None:
    payload = {"promptFeedback": {"blockReason": "SAFETY"}}
    async with _gateway(payload) as (gateway, _requests):
        response = await gateway.call(_request())

    assert response.response.content == ""
    assert response.finish_reason == "content_filter"


@pytest.mark.asyncio
@pytest.mark.parametrize(("status_code", "error_class"), [
    (400, errors.ClientError),
    (503, errors.ServerError),
])
async def test_google_propagates_sdk_errors(
    status_code: int, error_class: type[errors.APIError],
) -> None:
    payload = {"error": {"code": status_code, "message": "Upstream failed"}}
    async with _gateway(payload, status_code=status_code) as (gateway, requests):
        with pytest.raises(error_class, match="Upstream failed") as error:
            await gateway.call(_request())
    assert error.value.code == status_code
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_google_propagates_cancellation() -> None:
    cancelled = asyncio.CancelledError("cancelled by caller")
    async with _gateway({}, transport_error=cancelled) as (gateway, requests):
        with pytest.raises(asyncio.CancelledError) as error:
            await gateway.call(_request())
    assert error.value is cancelled
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_google_rejects_missing_credentials_before_sending_request() -> None:
    async with _gateway({}) as (gateway, requests):
        gateway.tokens = {}
        with pytest.raises(ValueError, match="no google API key"):
            await gateway.call(_request())
    assert requests == []
