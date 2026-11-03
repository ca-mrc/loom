"""Observe native Messages SSE without altering its content or semantics."""

from __future__ import annotations

import json
from typing import Any

from loom.anthropic_chat_compat import AnthropicCompatibilityError


class NativeMessagesStream:
    """Bounded native stream observer shared by hosted and local execution."""

    def __init__(self) -> None:
        self.usage: dict[str, Any] | None = None
        self.response_model: str | None = None
        self.completed = False
        self._buffer = bytearray()
        self._data: list[bytes] = []
        self._event_size = 0
        self._size = 0
        self._started = False
        self._terminal_usage = False

    def feed(self, chunk: bytes) -> bytes:
        self._size += len(chunk)
        if self._size > 16 * 1024 * 1024:
            raise AnthropicCompatibilityError("native Messages stream exceeds size limit")
        self._buffer.extend(chunk)
        while (end := self._buffer.find(b"\n")) >= 0:
            line = bytes(self._buffer[:end]).removesuffix(b"\r")
            del self._buffer[:end + 1]
            self._event_size += len(line)
            if self._event_size > 2 * 1024 * 1024:
                raise AnthropicCompatibilityError("native Messages SSE event exceeds size limit")
            if not line:
                if self._data:
                    self._consume(b"\n".join(self._data))
                self._data.clear()
                self._event_size = 0
            elif line.startswith(b"data:"):
                self._data.append(line[5:].removeprefix(b" "))
        if self._event_size + len(self._buffer) > 2 * 1024 * 1024:
            raise AnthropicCompatibilityError("native Messages SSE event exceeds size limit")
        return chunk

    def _consume(self, data: bytes) -> None:
        try:
            event = json.loads(data)
        except (ValueError, UnicodeError) as exc:
            raise AnthropicCompatibilityError("malformed native Messages SSE JSON") from exc
        if not isinstance(event, dict) or event.get("type") == "error":
            raise AnthropicCompatibilityError("native Messages stream error")
        kind = event.get("type")
        if kind == "ping":
            return
        if self.completed:
            raise AnthropicCompatibilityError("native Messages event after completion")
        if kind == "message_start":
            if self._started:
                raise AnthropicCompatibilityError("repeated native Messages start")
            self._started = True
            message = event.get("message")
            if not isinstance(message, dict):
                raise AnthropicCompatibilityError("missing native Messages message")
            self.response_model = message.get("model") if isinstance(message.get("model"), str) else None
            self._merge_usage(message.get("usage"))
        elif not self._started:
            raise AnthropicCompatibilityError("native Messages event before start")
        self._merge_usage(event.get("usage"))
        if kind == "message_delta":
            delta, usage = event.get("delta"), event.get("usage")
            if (isinstance(delta, dict) and delta.get("stop_reason") is not None
                    and isinstance(usage, dict) and "output_tokens" in usage):
                self._terminal_usage = True
        if kind == "message_stop":
            if not self._terminal_usage:
                raise AnthropicCompatibilityError("native Messages stream missing terminal usage")
            if self.usage is None or not {"input_tokens", "output_tokens"} <= self.usage.keys():
                raise AnthropicCompatibilityError("native Messages stream missing usage block")
            self.completed = True

    def _merge_usage(self, raw: Any) -> None:
        if raw is None:
            return
        if not isinstance(raw, dict):
            raise AnthropicCompatibilityError("invalid native Messages usage")
        if self.usage is None:
            self.usage = {}
        for key in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
            if key in raw:
                value = raw[key]
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise AnthropicCompatibilityError("invalid native Messages token counter")
                self.usage[key] = value
        # The pricing owner needs native cache-lifetime evidence too.
        if "cache_creation" in raw:
            self.usage["cache_creation"] = raw["cache_creation"]

    def finish(self) -> bytes:
        if not self.completed or self._buffer.strip() or self._data:
            raise AnthropicCompatibilityError("incomplete native Messages stream")
        return b""
