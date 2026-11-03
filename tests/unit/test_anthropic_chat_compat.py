"""Preserve Claude's tool conversation when the selected upstream speaks Chat."""

from __future__ import annotations

import copy
import json

import pytest

from loom.anthropic_chat_compat import (
    AnthropicCompatibilityError,
    anthropic_to_chat,
    chat_to_anthropic,
)
from loom.anthropic_protocol import anthropic_api_base_url


@pytest.mark.parametrize("base,expected", [
    ("https://api.anthropic.com", "https://api.anthropic.com/v1"),
    ("https://relay.example/v1/", "https://relay.example/v1"),
    ("https://relay.example/anthropic", "https://relay.example/anthropic/v1"),
    ("https://relay.example/anthropic/v1", "https://relay.example/anthropic/v1"),
    ("http://127.0.0.1:43123/proxy/", "http://127.0.0.1:43123/proxy/v1"),
])
def test_native_operations_share_one_versioned_api_root(base: str, expected: str) -> None:
    assert anthropic_api_base_url(base) == expected


@pytest.mark.parametrize("base", [
    "file:///tmp/provider", "https://key@relay.example", "https://relay.example?key=secret",
    "https://relay.example/#fragment", "https://relay.example/a/../b", "https://relay.example//v1",
])
def test_native_root_rejects_ambiguous_or_credential_bearing_urls(base: str) -> None:
    with pytest.raises(ValueError):
        anthropic_api_base_url(base)


def _request() -> dict:
    return {
        "model": "vendor/code-model", "max_tokens": 2048, "stream": True,
        "system": [{"type": "text", "text": "Solve the task."}, {"type": "text", "text": "Keep files local."}],
        "messages": [
            {"role": "user", "content": "Read both files."},
            {"role": "assistant", "content": [
                {"type": "text", "text": "I will read them."},
                {"type": "tool_use", "id": "call_a", "name": "Read", "input": {"file_path": "a.py"}},
                {"type": "tool_use", "id": "call_b", "name": "Read", "input": {"file_path": "b.py"}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_b", "content": "print('b')"},
                {"type": "tool_result", "tool_use_id": "call_a", "content": [{"type": "text", "text": "print('a')"}]},
                {"type": "text", "text": "Now fix the bug."},
            ]},
        ],
        "tools": [{"name": "Read", "description": "Read a file", "input_schema": {
            "type": "object", "properties": {"file_path": {"type": "string"}}, "required": ["file_path"],
        }}],
        "tool_choice": {"type": "auto", "disable_parallel_tool_use": True},
        "thinking": {"type": "disabled"}, "output_config": {"effort": "high"},
        "metadata": {"user_id": "not-a-chat-user-setting"},
    }


def test_two_turn_tool_conversation_preserves_ids_results_and_selected_model() -> None:
    request = _request()
    original = copy.deepcopy(request)
    chat = anthropic_to_chat(request)
    assert request == original
    assert chat["model"] == "vendor/code-model"
    assert chat["messages"][:2] == [
        {"role": "system", "content": "Solve the task.\n\nKeep files local."},
        {"role": "user", "content": "Read both files."},
    ]
    assistant = chat["messages"][2]
    assert assistant["content"] == "I will read them."
    assert [(call["id"], call["function"]["name"], json.loads(call["function"]["arguments"]))
            for call in assistant["tool_calls"]] == [
        ("call_a", "Read", {"file_path": "a.py"}), ("call_b", "Read", {"file_path": "b.py"}),
    ]
    assert chat["messages"][3:] == [
        {"role": "tool", "tool_call_id": "call_b", "content": "print('b')"},
        {"role": "tool", "tool_call_id": "call_a", "content": "print('a')"},
        {"role": "user", "content": "Now fix the bug."},
    ]
    assert chat["tools"][0]["function"]["parameters"] == request["tools"][0]["input_schema"]
    assert chat["tool_choice"] == "auto" and chat["parallel_tool_calls"] is False
    assert chat["stream_options"] == {"include_usage": True}
    assert chat["max_tokens"] == 2048
    assert not ({"thinking", "output_config", "metadata", "reasoning_effort"} & chat.keys())


def test_tool_error_is_explicit_in_the_upstream_observation() -> None:
    request = _request()
    request["messages"][2]["content"][0].update(is_error=True, content="Permission denied")
    chat = anthropic_to_chat(request)
    assert chat["messages"][3]["content"] == "[tool_error]\nPermission denied"


@pytest.mark.parametrize("changes", [
    {"thinking": {"type": "adaptive"}},
    {"context_management": {"edits": [{"type": "clear_tool_uses_20250919"}]}},
    {"output_config": {"format": {"type": "json_schema"}}},
    {"tools": [{"type": "web_search_20250305", "name": "web_search"}]},
    {"messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}}]}]},
    {"service_tier": "priority"},
    {"max_tokens": 0},
])
def test_unrepresentable_semantics_fail_before_an_upstream_request(changes: dict) -> None:
    with pytest.raises(AnthropicCompatibilityError):
        anthropic_to_chat({**_request(), **changes})


@pytest.mark.parametrize("tool_id", ["unknown", "call_b"])
def test_orphan_or_repeated_tool_results_are_rejected(tool_id: str) -> None:
    request = _request()
    request["messages"][2]["content"][1]["tool_use_id"] = tool_id
    with pytest.raises(AnthropicCompatibilityError, match="tool"):
        anthropic_to_chat(request)


def _response() -> dict:
    return {
        "id": "chatcmpl_1", "model": "provider-alias",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "Writing now.", "tool_calls": [
            {"type": "function", "id": "write_1", "function": {"name": "Write", "arguments": '{"file_path":"proof.txt","content":"ok"}'}},
        ]}, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 20,
                  "prompt_tokens_details": {"cached_tokens": 30}, "completion_tokens_details": {"reasoning_tokens": 7}},
    }


def test_chat_response_preserves_tools_and_does_not_double_count_cached_input() -> None:
    result = chat_to_anthropic(_response(), model="vendor/code-model")
    assert result["model"] == "vendor/code-model"
    assert result["content"] == [
        {"type": "text", "text": "Writing now."},
        {"type": "tool_use", "id": "write_1", "name": "Write", "input": {"file_path": "proof.txt", "content": "ok"}},
    ]
    assert result["stop_reason"] == "tool_use"
    assert result["usage"] == {"input_tokens": 70, "output_tokens": 20, "cache_read_input_tokens": 30}


@pytest.mark.parametrize("field,value", [("arguments", "{broken"), ("arguments", "[]"), ("name", "")])
def test_invalid_chat_tool_call_cannot_become_an_empty_success(field: str, value: str) -> None:
    body = _response()
    body["choices"][0]["message"]["tool_calls"][0]["function"][field] = value
    with pytest.raises(AnthropicCompatibilityError):
        chat_to_anthropic(body, model="selected-model")


@pytest.mark.parametrize("usage", [None, {}, {"prompt_tokens": 10}, {"prompt_tokens": -1, "completion_tokens": 2},
                                    {"prompt_tokens": 10, "completion_tokens": 2, "prompt_tokens_details": {"cached_tokens": 11}}])
def test_missing_or_impossible_usage_does_not_become_complete_accounting(usage: object) -> None:
    with pytest.raises(AnthropicCompatibilityError, match="usage"):
        chat_to_anthropic({**_response(), "usage": usage}, model="selected-model")
