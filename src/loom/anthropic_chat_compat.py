"""Bounded Anthropic Messages ↔ Chat compatibility for Claude workspace tools.

    This is an explicit compatibility profile, not an arbitrary Anthropic API
    emulator. Active native thinking, context management and server tools have
    no equivalent here and fail before dispatch. The profile disables native
    effort/cache behavior; cache hints and client metadata are advisory only.
    HTTP, credentials, model authorization and accounting belong to the caller.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from typing import Any

CHAT_PROFILE = "claude-code-chat-v1"
_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_REQUEST_FIELDS = frozenset({
    "model", "messages", "system", "max_tokens", "stream", "tools", "tool_choice",
    "temperature", "top_p", "stop_sequences", "metadata", "thinking", "output_config",
    "context_management",
})


class AnthropicCompatibilityError(ValueError):
    """The supplied request/response cannot retain its meaning in this profile."""


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AnthropicCompatibilityError(f"{label} must be an object")
    return value


def _string(value: Any, label: str, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not empty and not value):
        raise AnthropicCompatibilityError(f"{label} must be a non-empty string")
    return value


def _name(value: Any) -> str:
    name = _string(value, "tool name")
    if not _NAME.fullmatch(name):
        raise AnthropicCompatibilityError("tool name cannot be represented by Chat")
    return name


def _counter(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AnthropicCompatibilityError(f"{label} must be a nonnegative integer")
    return value


def _json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (ValueError, TypeError) as exc:
        raise AnthropicCompatibilityError("tool input is not valid JSON") from exc


def _text_blocks(value: Any, *, separator: str = "") -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        raise AnthropicCompatibilityError("message content must be text or a text-block array")
    texts: list[str] = []
    for raw in value:
        block = _object(raw, "content block")
        if block.get("type") != "text" or set(block) - {"type", "text", "cache_control"}:
            raise AnthropicCompatibilityError("unsupported message content block")
        texts.append(_string(block.get("text"), "text content", empty=True))
    return separator.join(texts)


def _messages(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise AnthropicCompatibilityError("messages must be a non-empty array")
    output: list[dict[str, Any]] = []
    pending: set[str] = set()
    seen: set[str] = set()
    for item in raw:
        message = _object(item, "message")
        if set(message) - {"role", "content"}:
            raise AnthropicCompatibilityError("unsupported message field")
        role, content = message.get("role"), message.get("content")
        if role not in {"user", "assistant"}:
            raise AnthropicCompatibilityError("only user and assistant messages are supported")
        if role == "assistant":
            if pending:
                raise AnthropicCompatibilityError("tool calls are missing their results")
            texts: list[str] = []
            calls: list[dict[str, Any]] = []
            blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
            if not isinstance(blocks, list):
                raise AnthropicCompatibilityError("assistant content must be text or blocks")
            for raw_block in blocks:
                block = _object(raw_block, "assistant block")
                if block.get("type") == "text":
                    texts.append(_text_blocks([block]))
                elif block.get("type") == "tool_use":
                    if set(block) - {"type", "id", "name", "input", "cache_control"}:
                        raise AnthropicCompatibilityError("unsupported tool-use field")
                    tool_id = _string(block.get("id"), "tool ID")
                    if tool_id in seen:
                        raise AnthropicCompatibilityError("duplicate tool ID")
                    seen.add(tool_id)
                    pending.add(tool_id)
                    calls.append({"type": "function", "id": tool_id, "function": {
                        "name": _name(block.get("name")), "arguments": _json(dict(_object(block.get("input"), "tool input"))),
                    }})
                else:
                    raise AnthropicCompatibilityError("unsupported assistant content block")
            output.append({"role": "assistant", "content": "".join(texts) or None,
                           **({"tool_calls": calls} if calls else {})})
            continue
        if isinstance(content, str):
            if pending:
                raise AnthropicCompatibilityError("tool calls are missing their results")
            output.append({"role": "user", "content": content})
            continue
        if not isinstance(content, list):
            raise AnthropicCompatibilityError("user content must be text or blocks")
        user_text: list[str] = []
        for raw_block in content:
            block = _object(raw_block, "user block")
            if block.get("type") == "text":
                if pending:
                    raise AnthropicCompatibilityError("tool results must precede the next user text")
                user_text.append(_text_blocks([block]))
                continue
            if block.get("type") != "tool_result" or set(block) - {"type", "tool_use_id", "content", "is_error", "cache_control"}:
                raise AnthropicCompatibilityError("unsupported user content block")
            tool_id = _string(block.get("tool_use_id"), "tool result ID")
            if tool_id not in pending:
                raise AnthropicCompatibilityError("tool result has an unknown or repeated tool ID")
            if user_text:
                output.append({"role": "user", "content": "".join(user_text)})
                user_text.clear()
            pending.remove(tool_id)
            text = _text_blocks(block.get("content", ""))
            if block.get("is_error"):
                text = "[tool_error]\n" + text
            output.append({"role": "tool", "tool_call_id": tool_id, "content": text})
        if user_text:
            output.append({"role": "user", "content": "".join(user_text)})
    if pending:
        raise AnthropicCompatibilityError("tool calls are missing their results")
    return output


def anthropic_to_chat(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Translate the explicit text/workspace-tool compatibility profile."""
    if set(payload) - _REQUEST_FIELDS:
        raise AnthropicCompatibilityError("unsupported Anthropic request field")
    if payload.get("thinking") not in (None, {"type": "disabled"}):
        raise AnthropicCompatibilityError("native thinking is unsupported by the Chat profile")
    if payload.get("context_management") is not None:
        raise AnthropicCompatibilityError("native context management is unsupported by the Chat profile")
    config = payload.get("output_config")
    if config is not None:
        config = _object(config, "output_config")
        if set(config) - {"effort"} or config.get("effort") not in {None, "low", "medium", "high", "max"}:
            raise AnthropicCompatibilityError("unsupported native output_config semantics")
    tokens = _counter(payload.get("max_tokens"), "max_tokens")
    if not tokens:
        raise AnthropicCompatibilityError("max_tokens must be positive")
    stream = payload.get("stream", False)
    if not isinstance(stream, bool):
        raise AnthropicCompatibilityError("stream must be boolean")
    messages = _messages(payload.get("messages"))
    if payload.get("system") is not None:
        messages.insert(0, {"role": "system", "content": _text_blocks(payload["system"], separator="\n\n")})
    result: dict[str, Any] = {
        "model": _string(payload.get("model"), "model"), "messages": messages,
        "max_tokens": tokens, "stream": stream,
    }
    if stream:
        result["stream_options"] = {"include_usage": True}
    for key in ("temperature", "top_p"):
        if key in payload:
            value = payload[key]
            if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
                raise AnthropicCompatibilityError(f"{key} must be finite")
            result[key] = value
    if "stop_sequences" in payload:
        stops = payload["stop_sequences"]
        if not isinstance(stops, list) or not all(isinstance(stop, str) for stop in stops):
            raise AnthropicCompatibilityError("stop_sequences must be a string array")
        result["stop"] = list(stops)
    tools = payload.get("tools")
    if tools is not None:
        if not isinstance(tools, list):
            raise AnthropicCompatibilityError("tools must be an array")
        names: set[str] = set()
        converted: list[dict[str, Any]] = []
        for raw in tools:
            tool = _object(raw, "tool")
            if (set(tool) - {"name", "description", "input_schema", "type", "cache_control", "strict", "defer_loading"}
                    or tool.get("type") not in {None, "custom"} or tool.get("defer_loading")):
                raise AnthropicCompatibilityError("unsupported tool semantics")
            name = _name(tool.get("name"))
            if name in names:
                raise AnthropicCompatibilityError("duplicate tool name")
            names.add(name)
            function: dict[str, Any] = {"name": name, "parameters": dict(_object(tool.get("input_schema"), "tool schema"))}
            if "description" in tool:
                function["description"] = _string(tool["description"], "tool description", empty=True)
            if "strict" in tool:
                if not isinstance(tool["strict"], bool):
                    raise AnthropicCompatibilityError("tool strict must be boolean")
                function["strict"] = tool["strict"]
            converted.append({"type": "function", "function": function})
        if converted:
            result["tools"] = converted
    if payload.get("tool_choice") is not None:
        choice = _object(payload["tool_choice"], "tool_choice")
        if set(choice) - {"type", "name", "disable_parallel_tool_use"}:
            raise AnthropicCompatibilityError("unsupported tool_choice field")
        kind = choice.get("type")
        if kind in {"auto", "none", "any"}:
            result["tool_choice"] = "required" if kind == "any" else kind
        elif kind == "tool":
            result["tool_choice"] = {"type": "function", "function": {"name": _name(choice.get("name"))}}
        else:
            raise AnthropicCompatibilityError("unsupported tool_choice")
        if "disable_parallel_tool_use" in choice:
            if not isinstance(choice["disable_parallel_tool_use"], bool):
                raise AnthropicCompatibilityError("disable_parallel_tool_use must be boolean")
            result["parallel_tool_calls"] = not choice["disable_parallel_tool_use"]
    return result


def chat_usage_to_anthropic(value: Any) -> dict[str, int]:
    """Chat prompt totals include cache hits; Anthropic input excludes them."""
    usage = _object(value, "usage")
    prompt = _counter(usage.get("prompt_tokens"), "usage.prompt_tokens")
    output = _counter(usage.get("completion_tokens"), "usage.completion_tokens")
    details = usage.get("prompt_tokens_details") or {}
    cached = _counter(_object(details, "usage.prompt_tokens_details").get("cached_tokens", 0), "usage.cached_tokens")
    if cached > prompt:
        raise AnthropicCompatibilityError("usage cached tokens exceed prompt tokens")
    return {"input_tokens": prompt - cached, "output_tokens": output,
            **({"cache_read_input_tokens": cached} if cached else {})}


def chat_finish_reason(value: Any) -> str:
    reasons = {"stop": "end_turn", "length": "max_tokens", "tool_calls": "tool_use", "content_filter": "refusal"}
    if not isinstance(value, str) or value not in reasons:
        raise AnthropicCompatibilityError("unsupported or missing Chat finish reason")
    return reasons[value]


def chat_to_anthropic(body: Mapping[str, Any], *, model: str) -> dict[str, Any]:
    """Normalize one complete Chat response, without inventing usage or tools."""
    choices = body.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise AnthropicCompatibilityError("expected exactly one Chat choice")
    choice = _object(choices[0], "Chat choice")
    message = _object(choice.get("message"), "Chat message")
    if message.get("role") != "assistant":
        raise AnthropicCompatibilityError("Chat response must be an assistant message")
    blocks: list[dict[str, Any]] = []
    if message.get("content") is not None:
        text = _string(message["content"], "Chat content", empty=True)
        if text:
            blocks.append({"type": "text", "text": text})
    calls = message.get("tool_calls") or []
    if not isinstance(calls, list):
        raise AnthropicCompatibilityError("Chat tool calls must be an array")
    seen: set[str] = set()
    for raw in calls:
        call = _object(raw, "Chat tool call")
        if call.get("type") != "function":
            raise AnthropicCompatibilityError("unsupported Chat tool type")
        tool_id = _string(call.get("id"), "Chat tool ID")
        if tool_id in seen:
            raise AnthropicCompatibilityError("duplicate Chat tool ID")
        seen.add(tool_id)
        function = _object(call.get("function"), "Chat function")
        try:
            arguments = json.loads(_string(function.get("arguments"), "Chat tool arguments"))
        except (ValueError, TypeError) as exc:
            raise AnthropicCompatibilityError("Chat tool arguments are not valid JSON") from exc
        arguments = dict(_object(arguments, "Chat tool arguments"))
        _json(arguments)
        blocks.append({"type": "tool_use", "id": tool_id, "name": _name(function.get("name")), "input": arguments})
    reason = chat_finish_reason(choice.get("finish_reason"))
    if reason == "tool_use" and not calls:
        raise AnthropicCompatibilityError("Chat tool finish reason has no tool call")
    return {
        "id": _string(body.get("id"), "Chat response ID"), "type": "message", "role": "assistant",
        "model": model, "content": blocks, "stop_reason": reason, "stop_sequence": None,
        "usage": chat_usage_to_anthropic(body.get("usage")),
    }
