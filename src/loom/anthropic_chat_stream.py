"""Bounded incremental Chat SSE → Anthropic SSE, independent of transport.

No terminal event is invented on EOF. Callers retain ``usage`` even on error
or cancellation and own closing the HTTP stream and recording that outcome.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from loom.anthropic_chat_compat import (
    AnthropicCompatibilityError,
    chat_finish_reason,
    chat_to_anthropic,
    chat_usage_to_anthropic,
)


def _event(kind: str, **body: Any) -> bytes:
    return (f"event: {kind}\ndata: " + json.dumps(
        {"type": kind, **body}, ensure_ascii=False, allow_nan=False, separators=(",", ":"),
    ) + "\n\n").encode()


def _observed_usage(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep independently valid counters even when the complete usage is invalid."""
    def counters(value: dict[str, Any], keys: tuple[str, ...]) -> dict[str, int]:
        return {key: value[key] for key in keys if isinstance(value.get(key), int)
                and not isinstance(value[key], bool) and value[key] >= 0}

    result: dict[str, Any] = counters(raw, ("prompt_tokens", "completion_tokens", "total_tokens"))
    for field, keys in (("prompt_tokens_details", ("cached_tokens", "audio_tokens")),
                        ("completion_tokens_details", ("reasoning_tokens", "audio_tokens"))):
        if isinstance(raw.get(field), dict):
            result[field] = counters(raw[field], keys)
    return result


@dataclass
class _Block:
    kind: str
    content: str = ""
    tool_id: str = ""
    name: str = ""
    started: bool = False
    sent: int = 0
    size: int = 0


class ChatToAnthropicStream:
    """Convert a single choice with text and parallel function tools.

    Tools start in order of first appearance. A fragmented function header is
    buffered until its arguments begin. Stops wait for validated arguments and
    the provider's terminal marker, so truncated tools never look successful.
    Initial zero usage is a required wire placeholder, not observed accounting.
    """

    def __init__(
        self, *, model: str, max_event_bytes: int = 2 * 1024 * 1024,
        max_tool_bytes: int = 1024 * 1024, max_response_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self.model = model
        self.usage: dict[str, Any] | None = None
        self.response_model: str | None = None
        self.completed = False
        self._id: str | None = None
        self._started = False
        self._reason: str | None = None
        self._buffer = bytearray()
        self._data: list[bytes] = []
        self._event_size = 0
        self._size = 0
        self._max_event = max_event_bytes
        self._max_tool = max_tool_bytes
        self._max_response = max_response_bytes
        self._blocks: list[_Block] = []
        self._tools: dict[int, _Block] = {}
        self._text: _Block | None = None

    def feed(self, chunk: bytes) -> bytes:
        self._size += len(chunk)
        if self._size > self._max_response:
            raise AnthropicCompatibilityError("Chat response exceeds size limit")
        self._buffer.extend(chunk)
        output: list[bytes] = []
        while (end := self._buffer.find(b"\n")) >= 0:
            line = bytes(self._buffer[:end]).removesuffix(b"\r")
            del self._buffer[:end + 1]
            self._event_size += len(line)
            if self._event_size > self._max_event:
                raise AnthropicCompatibilityError("Chat SSE event exceeds size limit")
            if not line:
                if self._data:
                    output.append(self._consume(b"\n".join(self._data)))
                self._data.clear()
                self._event_size = 0
            elif line.startswith(b"data:"):
                self._data.append(line[5:].removeprefix(b" "))
        if self._event_size + len(self._buffer) > self._max_event:
            raise AnthropicCompatibilityError("Chat SSE event exceeds size limit")
        return b"".join(output)

    def finish(self) -> bytes:
        """Validate EOF without turning a disconnection into completion."""
        if not self.completed or self._buffer.strip() or self._data:
            raise AnthropicCompatibilityError("incomplete Chat stream")
        return b""

    def _consume(self, data: bytes) -> bytes:
        if self.completed:
            raise AnthropicCompatibilityError("Chat data after completion")
        if data == b"[DONE]":
            return self._complete()
        try:
            body = json.loads(data)
        except (ValueError, UnicodeError) as exc:
            raise AnthropicCompatibilityError("malformed Chat SSE JSON") from exc
        if not isinstance(body, dict) or "error" in body:
            raise AnthropicCompatibilityError("invalid Chat stream response")
        for field, previous in (("id", self._id), ("model", self.response_model)):
            value = body.get(field)
            if value is not None:
                if not isinstance(value, str) or not value or (previous is not None and value != previous):
                    raise AnthropicCompatibilityError("Chat stream identity changed")
                if field == "id":
                    self._id = value
                else:
                    self.response_model = value
        if body.get("usage") is not None:
            # Preserve observations before strict completion validation, even if
            # another counter is missing or malformed and this stream fails.
            if isinstance(body["usage"], dict):
                self.usage = _observed_usage(body["usage"])
            chat_usage_to_anthropic(body["usage"])
        choices = body.get("choices")
        if not isinstance(choices, list) or len(choices) > 1:
            raise AnthropicCompatibilityError("expected one Chat stream choice")
        if not choices:
            return b""
        choice = choices[0]
        if not isinstance(choice, dict) or choice.get("index") != 0:
            raise AnthropicCompatibilityError("invalid Chat choice index")
        if self._reason is not None:
            raise AnthropicCompatibilityError("Chat choice after finish reason")
        delta = choice.get("delta")
        if not isinstance(delta, dict) or delta.get("role") not in {None, "assistant"}:
            raise AnthropicCompatibilityError("invalid Chat stream delta")
        if any(delta.get(key) for key in ("function_call", "audio", "refusal")):
            raise AnthropicCompatibilityError("unsupported Chat stream content")
        content = delta.get("content")
        if content is not None:
            if not isinstance(content, str):
                raise AnthropicCompatibilityError("Chat content must be text")
            if content:
                if self._text is None:
                    self._text = _Block("text")
                    self._blocks.append(self._text)
                self._text.content += content
        calls = delta.get("tool_calls")
        if calls is not None:
            if not isinstance(calls, list):
                raise AnthropicCompatibilityError("Chat tool calls must be an array")
            for call in calls:
                self._tool_delta(call)
        if choice.get("finish_reason") is not None:
            self._reason = chat_finish_reason(choice["finish_reason"])
            self._validate_tools(choice["finish_reason"])
        output = b""
        if not self._started:
            if not self._id:
                raise AnthropicCompatibilityError("missing Chat response identity")
            self._started = True
            output = _event("message_start", message={
                "id": self._id, "type": "message", "role": "assistant", "model": self.model,
                "content": [], "stop_reason": None, "stop_sequence": None,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            })
        return output + self._flush()

    def _tool_delta(self, call: Any) -> None:
        if not isinstance(call, dict):
            raise AnthropicCompatibilityError("invalid Chat tool delta")
        index = call.get("index")
        if not isinstance(index, int) or isinstance(index, bool) or index < 0:
            raise AnthropicCompatibilityError("invalid Chat tool index")
        if call.get("type") not in {None, "function"}:
            raise AnthropicCompatibilityError("unsupported Chat tool type")
        function = call.get("function", {})
        if not isinstance(function, dict):
            raise AnthropicCompatibilityError("invalid Chat tool function")
        self._text = None
        block = self._tools.get(index)
        if block is None:
            block = _Block("tool_use")
            self._tools[index] = block
            self._blocks.append(block)
        for key, attr in (("id", "tool_id"), ("name", "name"), ("arguments", "content")):
            value = call.get(key) if key == "id" else function.get(key)
            if value is None:
                continue
            if not isinstance(value, str):
                raise AnthropicCompatibilityError("Chat tool fields must be strings")
            if block.started and key != "arguments" and value:
                raise AnthropicCompatibilityError("Chat tool identity changed after arguments")
            if key == "arguments":
                block.size += len(value.encode())
            if block.size > self._max_tool:
                raise AnthropicCompatibilityError("Chat tool arguments exceed size limit")
            setattr(block, attr, getattr(block, attr) + value)

    def _validate_tools(self, reason: str) -> None:
        # Reuse complete-response validation for IDs, names and object arguments.
        chat_to_anthropic({
            "id": self._id, "choices": [{"finish_reason": reason, "message": {
                "role": "assistant", "content": None, "tool_calls": [
                    {"id": b.tool_id, "type": "function", "function": {"name": b.name, "arguments": b.content}}
                    for b in self._tools.values()
                ],
            }}], "usage": {"prompt_tokens": 0, "completion_tokens": 0},
        }, model=self.model)

    def _flush(self) -> bytes:
        output: list[bytes] = []
        for index, block in enumerate(self._blocks):
            if not block.started:
                if block.kind == "tool_use" and not (block.tool_id and block.name and block.content):
                    break
                content: dict[str, Any] = {"type": block.kind}
                if block.kind == "text":
                    content["text"] = ""
                else:
                    content.update(id=block.tool_id, name=block.name, input={})
                output.append(_event("content_block_start", index=index, content_block=content))
                block.started = True
            if block.sent < len(block.content):
                delta = ({"type": "text_delta", "text": block.content[block.sent:]}
                         if block.kind == "text" else
                         {"type": "input_json_delta", "partial_json": block.content[block.sent:]})
                output.append(_event("content_block_delta", index=index, delta=delta))
                block.sent = len(block.content)
        return b"".join(output)

    def _complete(self) -> bytes:
        if not self._started or self._reason is None:
            raise AnthropicCompatibilityError("Chat stream ended without a finish reason")
        if self.usage is None:
            raise AnthropicCompatibilityError("Chat stream completed without observed usage")
        output = self._flush()
        for index in range(len(self._blocks)):
            output += _event("content_block_stop", index=index)
        output += _event("message_delta", delta={"stop_reason": self._reason, "stop_sequence": None},
                         usage=chat_usage_to_anthropic(self.usage))
        output += _event("message_stop")
        self.completed = True
        return output
