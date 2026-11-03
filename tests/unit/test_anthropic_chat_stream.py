"""The Anthropic client must see exactly the tool arguments Chat streamed."""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx
import pytest

from loom.anthropic_chat_compat import AnthropicCompatibilityError
from loom.anthropic_chat_stream import ChatToAnthropicStream


def _chunk(delta: dict[str, Any], finish: str | None = None) -> bytes:
    return _event({"id": "chatcmpl_a", "model": "upstream-alias", "choices": [
        {"index": 0, "delta": delta, "finish_reason": finish},
    ]})


def _event(body: dict[str, Any]) -> bytes:
    return ("data: " + json.dumps(body, ensure_ascii=False) + "\r\n\r\n").encode()


def _wire() -> bytes:
    return b"".join([
        _chunk({"role": "assistant", "content": "Reading café."}),
        _chunk({"tool_calls": [
            {"index": 0, "id": "read_a", "type": "function", "function": {"name": "Read", "arguments": '{"file_'}},
            {"index": 1, "id": "read_b", "type": "function", "function": {"name": "Read", "arguments": '{"file_path":"b.py"}'}},
        ]}),
        _chunk({"tool_calls": [{"index": 0, "function": {"arguments": 'path":"café.py"}'}}]}),
        _chunk({"content": "Two files."}),
        _chunk({}, "tool_calls"),
        _event({"id": "chatcmpl_a", "model": "upstream-alias", "choices": [], "usage": {
            "prompt_tokens": 100, "completion_tokens": 20,
            "prompt_tokens_details": {"cached_tokens": 30},
            "completion_tokens_details": {"reasoning_tokens": 7},
        }}),
        b"data: [DONE]\r\n\r\n",
    ])


def _parse_with_sdk(wire: bytes) -> Any:
    with anthropic.Anthropic(api_key="test", http_client=httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(
            200, content=wire, headers={"content-type": "text/event-stream"},
        )),
    )) as client, client.messages.stream(model="selected", max_tokens=100,
                                        messages=[{"role": "user", "content": "read"}]) as stream:
        return stream.get_final_message()


@pytest.mark.parametrize("width", [1, 7, 1024, 65536])
def test_fragmented_parallel_tools_and_usage_are_readable_by_anthropic_sdk(width: int) -> None:
    converter = ChatToAnthropicStream(model="selected")
    wire = _wire()
    converted = b"".join(converter.feed(wire[n:n + width]) for n in range(0, len(wire), width))
    converted += converter.finish()
    message = _parse_with_sdk(converted)
    assert message.model == "selected"
    assert [(block.type, getattr(block, "text", None), getattr(block, "id", None),
             getattr(block, "input", None)) for block in message.content] == [
        ("text", "Reading café.", None, None),
        ("tool_use", None, "read_a", {"file_path": "café.py"}),
        ("tool_use", None, "read_b", {"file_path": "b.py"}),
        ("text", "Two files.", None, None),
    ]
    assert message.stop_reason == "tool_use"
    assert message.usage.input_tokens == 70
    assert message.usage.cache_read_input_tokens == 30
    assert message.usage.output_tokens == 20
    assert converter.usage == {"prompt_tokens": 100, "completion_tokens": 20,
                               "prompt_tokens_details": {"cached_tokens": 30},
                               "completion_tokens_details": {"reasoning_tokens": 7}}
    assert converter.response_model == "upstream-alias"
    assert converter.completed
    assert converted.count(b"event: message_stop\n") == 1


def test_text_is_forwarded_before_the_upstream_finishes() -> None:
    converter = ChatToAnthropicStream(model="selected")
    first = converter.feed(_chunk({"role": "assistant", "content": "Hello"}))
    assert b'"text":"Hello"' in first
    assert b"event: message_stop" not in first
    assert not converter.completed and converter.usage is None


@pytest.mark.parametrize("suffix", [b"", b"data: [DONE]\n\n"])
def test_incomplete_stream_never_fabricates_a_successful_stop(suffix: bytes) -> None:
    converter = ChatToAnthropicStream(model="selected")
    converter.feed(_chunk({"content": "Partial"}))
    with pytest.raises(AnthropicCompatibilityError, match=r"finish|incomplete"):
        converter.feed(suffix)
        converter.finish()
    assert not converter.completed


def test_disconnect_retains_usage_seen_before_completion() -> None:
    converter = ChatToAnthropicStream(model="selected")
    converter.feed(_chunk({"content": "Partial"}))
    converter.feed(_event({"choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 3}}))
    with pytest.raises(AnthropicCompatibilityError, match="incomplete"):
        converter.finish()
    assert converter.usage == {"prompt_tokens": 9, "completion_tokens": 3}


@pytest.mark.parametrize("arguments", ['{broken', '[]', '{"x":NaN}'])
def test_malformed_tool_arguments_are_not_closed_as_successful_tools(arguments: str) -> None:
    converter = ChatToAnthropicStream(model="selected")
    output = converter.feed(_chunk({"tool_calls": [
        {"index": 0, "id": "a", "type": "function", "function": {"name": "Write", "arguments": arguments}},
    ]}))
    with pytest.raises(AnthropicCompatibilityError, match=r"arguments|JSON"):
        converter.feed(_chunk({}, "tool_calls"))
    assert b"event: content_block_stop" not in output


@pytest.mark.parametrize("wire", [b"data: {broken\n\n", b"data: []\n\n", b"data: \xff\n\n"])
def test_malformed_sse_fails_explicitly(wire: bytes) -> None:
    with pytest.raises(AnthropicCompatibilityError):
        ChatToAnthropicStream(model="selected").feed(wire)


def test_bounds_unterminated_events_and_accumulated_tool_arguments() -> None:
    with pytest.raises(AnthropicCompatibilityError, match="limit"):
        ChatToAnthropicStream(model="selected", max_event_bytes=32).feed(b"data: " + b"x" * 33)
    converter = ChatToAnthropicStream(model="selected", max_tool_bytes=10)
    converter.feed(_chunk({"tool_calls": [
        {"index": 0, "id": "a", "type": "function", "function": {"name": "Write", "arguments": '{"x":"'}},
    ]}))
    with pytest.raises(AnthropicCompatibilityError, match="limit"):
        converter.feed(_chunk({"tool_calls": [{"index": 0, "function": {"arguments": "a" * 11}}]}))


def test_response_identity_cannot_change_mid_stream() -> None:
    converter = ChatToAnthropicStream(model="selected")
    converter.feed(_chunk({"content": "First"}))
    with pytest.raises(AnthropicCompatibilityError, match="identity"):
        converter.feed(_event({"id": "other", "choices": []}))
