from copy import deepcopy

import pytest

from loom_llm_gateway.litellm_wrapper import ParsedResponse, parse_litellm_response

_FAKE_LITELLM_RESPONSE = {
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1717000000,
    "model": "claude-opus-4-7",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "hello"},
            "finish_reason": "stop",
        },
    ],
    "usage": {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
        "cache_creation_input_tokens": 100,
        "cache_read_input_tokens": 50,
    },
}


def test_parses_basic_response():
    parsed = parse_litellm_response(_FAKE_LITELLM_RESPONSE, provider="anthropic")
    assert isinstance(parsed, ParsedResponse)
    assert parsed.response_content == "hello"
    assert parsed.finish_reason == "stop"
    assert parsed.input_tokens == 10
    assert parsed.output_tokens == 5
    assert parsed.cached_input_tokens == 50
    assert parsed.cache_write_tokens == 100
    assert parsed.provider_extras == {
        "cache_creation_input_tokens": 100,
        "cache_read_input_tokens": 50,
    }


def test_handles_missing_cache_fields():
    resp = dict(_FAKE_LITELLM_RESPONSE)
    resp["usage"] = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    parsed = parse_litellm_response(resp, provider="openai")
    assert parsed.cached_input_tokens == 0
    assert parsed.cache_write_tokens == 0
    assert parsed.provider_extras == {}


def test_handles_thinking_tokens():
    resp = dict(_FAKE_LITELLM_RESPONSE)
    resp["usage"] = {
        "prompt_tokens": 10, "completion_tokens": 5,
        "thinking_tokens": 200,
    }
    parsed = parse_litellm_response(resp, provider="anthropic")
    assert parsed.thinking_tokens == 200


def test_collapses_multimodal_content():
    """Multimodal content as a list of {type, text} parts collapses to text."""
    resp = dict(_FAKE_LITELLM_RESPONSE)
    resp["choices"] = [{
        "index": 0,
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "hello "},
                {"type": "image_url", "image_url": "..."},  # ignored
                {"type": "text", "text": "world"},
            ],
        },
        "finish_reason": "stop",
    }]
    parsed = parse_litellm_response(resp, provider="anthropic")
    assert parsed.response_content == "hello world"


@pytest.mark.parametrize("include_content", [True, False], ids=["null", "missing"])
def test_tool_only_response_preserves_raw_message_and_usage(include_content: bool) -> None:
    response = deepcopy(_FAKE_LITELLM_RESPONSE)
    message = response["choices"][0]["message"]
    if include_content:
        message["content"] = None
    else:
        del message["content"]
    message["tool_calls"] = [{
        "id": "call_1",
        "type": "function",
        "function": {"name": "lookup", "arguments": '{"key":"value"}'},
    }]
    response["choices"][0]["finish_reason"] = "tool_calls"
    original = deepcopy(response)

    parsed = parse_litellm_response(response, provider="openai")

    assert parsed.response_content == ""
    assert parsed.finish_reason == "tool_calls"
    assert parsed.raw_response == original
    assert parsed.input_tokens == 10
    assert parsed.output_tokens == 5
    assert parsed.cached_input_tokens == 50
    assert parsed.cache_write_tokens == 100
    assert parsed.provider_extras == {
        "cache_creation_input_tokens": 100,
        "cache_read_input_tokens": 50,
    }


def test_unknown_provider_counters_land_in_extras():
    """Provider-specific int counters not in the known set show up in
    provider_extras."""
    resp = dict(_FAKE_LITELLM_RESPONSE)
    resp["usage"] = {
        "prompt_tokens": 10, "completion_tokens": 5,
        "custom_provider_counter": 42,
    }
    parsed = parse_litellm_response(resp, provider="anthropic")
    assert parsed.provider_extras == {"custom_provider_counter": 42}
