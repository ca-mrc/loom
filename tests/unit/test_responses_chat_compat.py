from __future__ import annotations

import httpx
import pytest

from loom_llm_gateway.routes.responses import _extract_responses_usage
from loom_llm_gateway.routes.responses_chat_compat import (
    chat_completion_to_responses,
    should_fallback_to_chat_completions,
)


def test_detects_chat_only_missing_messages_error() -> None:
    response = httpx.Response(400, json={
        "error": {
            "message": "you must provide a messages parameter",
            "type": "invalid_request_error",
            "param": "message",
            "code": "missing_required_parameter",
        },
    })

    assert should_fallback_to_chat_completions(
        response, {"model": "qwen", "input": "hi"},
    )


def test_does_not_fallback_on_unrelated_missing_required_parameter() -> None:
    response = httpx.Response(400, json={
        "error": {
            "message": "model is required",
            "type": "invalid_request_error",
            "param": "model",
            "code": "missing_required_parameter",
        },
    })

    assert not should_fallback_to_chat_completions(
        response, {"model": "qwen", "input": "hi"},
    )


def _chat_body(usage: dict[str, object]) -> dict[str, object]:
    return {
        "id": "chatcmpl-1",
        "model": "gpt-4.1-mini",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}],
        "usage": usage,
    }


@pytest.mark.parametrize("stream", [False, True])
def test_translated_usage_keeps_cache_and_reasoning_counts(stream: bool) -> None:
    # Dropping cached_tokens left every translated Codex call `price_unknown`
    # (`missing_cache_usage`) while Terminus on the same model priced (#2054).
    body = chat_completion_to_responses(_chat_body({
        "prompt_tokens": 2000, "completion_tokens": 40, "total_tokens": 2040,
        "prompt_tokens_details": {"cached_tokens": 1536, "audio_tokens": 0},
        "completion_tokens_details": {"reasoning_tokens": 12, "accepted_prediction_tokens": 0},
    }), model_name="gpt-4.1-mini", stream=stream)

    usage = _extract_responses_usage(body)

    assert (usage.input_tokens, usage.output_tokens) == (2000, 40)
    assert usage.provider_extras["_loom_cache_usage_known"] is True
    assert usage.provider_extras["cache_read_input_tokens"] == 1536
    assert usage.provider_extras["reasoning_tokens"] == 12
    assert usage.provider_extras["_loom_unsupported_billing"] is False


@pytest.mark.parametrize("prompt_details", [None, {}, {"cached_tokens": None}, {"cached_tokens": -1}])
def test_translated_usage_leaves_unreported_cache_unknown(prompt_details: object) -> None:
    chat_usage: dict[str, object] = {"prompt_tokens": 10, "completion_tokens": 2}
    if prompt_details is not None:
        chat_usage["prompt_tokens_details"] = prompt_details

    body = chat_completion_to_responses(
        _chat_body(chat_usage), model_name="gpt-4.1-mini", stream=False,
    )

    assert isinstance(body, dict)
    assert "input_tokens_details" not in body["usage"]
    assert _extract_responses_usage(body).provider_extras["_loom_cache_usage_known"] is False
