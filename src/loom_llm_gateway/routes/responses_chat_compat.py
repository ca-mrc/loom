"""Responses-to-Chat fallback helpers for OpenAI-shaped provider facades.

Some OpenAI-compatible providers expose `/v1/chat/completions` but either
omit `/v1/responses` or route it to a chat handler. Codex 0.141+ requires
Responses wire format, so the gateway can recover from that specific
chat-only signature by making one non-streaming chat call and synthesizing
the Responses body or SSE stream Codex expects.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
from fastapi import HTTPException


def should_fallback_to_chat_completions(
    response: httpx.Response,
    payload: dict[str, Any],
) -> bool:
    if response.status_code != 400:
        return False
    if "input" not in payload and "instructions" not in payload:
        return False
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            message = str(error.get("message") or "").lower()
            param = str(error.get("param") or "").lower()
            code = str(error.get("code") or "").lower()
            message_mentions_messages = "messages" in message
            param_is_message = param in {"message", "messages"}
            return (
                (message_mentions_messages or param_is_message)
                and (
                    "missing_required_parameter" in code
                    or "must provide" in message
                    or "required" in message
                )
            )

    text = response.text.lower()
    return (
        (
            "messages parameter" in text
            or "messages required" in text
            or "missing messages" in text
        )
        and (
            "missing_required_parameter" in text
            or "must provide" in text
            or "required" in text
        )
    )


def decode_chat_completion_body(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"chat-completions fallback returned non-JSON body: {exc}",
        ) from exc
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=502,
            detail="chat-completions fallback returned non-object JSON body",
        )
    return body


def responses_payload_to_chat_completion(
    payload: dict[str, Any],
) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    instructions = payload.get("instructions")
    if isinstance(instructions, str) and instructions:
        messages.append({"role": "system", "content": instructions})

    input_value = payload.get("input")
    if isinstance(input_value, str):
        messages.append({"role": "user", "content": input_value})
    elif isinstance(input_value, list):
        for item in input_value:
            if isinstance(item, dict):
                _append_responses_input_item(messages, item)

    if not messages:
        raise HTTPException(
            status_code=502,
            detail=(
                "chat-completions fallback cannot convert Responses "
                "payload without string input or message items"
            ),
        )

    chat_payload: dict[str, Any] = {
        "model": payload["model"],
        "messages": messages,
        # Codex always asks for Responses SSE. The compat layer does one
        # complete chat call, then synthesizes the Responses stream itself.
        "stream": False,
    }
    tools = _responses_tools_to_chat_tools(payload.get("tools"))
    if tools:
        chat_payload["tools"] = tools
    tool_choice = _responses_tool_choice_to_chat(payload.get("tool_choice"))
    if tool_choice is not None:
        chat_payload["tool_choice"] = tool_choice
    if isinstance(payload.get("parallel_tool_calls"), bool):
        chat_payload["parallel_tool_calls"] = payload["parallel_tool_calls"]
    if isinstance(payload.get("temperature"), int | float):
        chat_payload["temperature"] = payload["temperature"]
    if isinstance(payload.get("top_p"), int | float):
        chat_payload["top_p"] = payload["top_p"]
    if isinstance(payload.get("max_output_tokens"), int):
        chat_payload["max_tokens"] = payload["max_output_tokens"]
    return chat_payload


def chat_completion_to_responses(
    chat_body: dict[str, Any],
    *,
    model_name: str,
    stream: bool,
) -> dict[str, Any] | str:
    response_body = _chat_completion_to_response_body(
        chat_body, model_name=model_name,
    )
    if not stream:
        return response_body
    return _response_body_to_sse(response_body)


def synthetic_responses_http_response(
    body_or_stream: dict[str, Any] | str,
) -> httpx.Response:
    if isinstance(body_or_stream, dict):
        return httpx.Response(200, json=body_or_stream)
    return httpx.Response(
        200,
        content=body_or_stream.encode(),
        headers={"content-type": "text/event-stream"},
    )


def _append_responses_input_item(
    messages: list[dict[str, Any]],
    item: dict[str, Any],
) -> None:
    item_type = item.get("type")
    if item_type == "message":
        role = item.get("role")
        if role == "developer":
            role = "system"
        if role not in {"system", "user", "assistant", "tool"}:
            role = "user"
        message: dict[str, Any] = {
            "role": role,
            "content": _responses_content_to_text(item.get("content")),
        }
        if role == "tool" and isinstance(item.get("call_id"), str):
            message["tool_call_id"] = item["call_id"]
        messages.append(message)
        return

    if item_type == "function_call":
        call_id = _string_or_default(item.get("call_id"), "call_compat")
        name = _string_or_default(item.get("name"), "unknown")
        arguments = item.get("arguments")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments or {}, separators=(",", ":"))
        messages.append({
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": call_id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            }],
        })
        return

    if item_type == "function_call_output":
        call_id = _string_or_default(item.get("call_id"), "call_compat")
        output = item.get("output", item.get("content", ""))
        messages.append({
            "role": "tool",
            "tool_call_id": call_id,
            "content": _responses_content_to_text(output),
        })


def _responses_content_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for part in value:
            if isinstance(part, str):
                parts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            for key in ("text", "input_text", "output_text"):
                text = part.get(key)
                if isinstance(text, str):
                    parts.append(text)
                    break
        return "\n".join(parts)
    if isinstance(value, dict):
        for key in ("text", "input_text", "output_text"):
            text = value.get(key)
            if isinstance(text, str):
                return text
    return str(value)


def _responses_tools_to_chat_tools(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tools: list[dict[str, Any]] = []
    for tool in value:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        name = tool.get("name")
        if not isinstance(name, str) or not name:
            continue
        function: dict[str, Any] = {
            "name": name,
            "parameters": tool.get("parameters") or {
                "type": "object",
                "properties": {},
            },
        }
        description = tool.get("description")
        if isinstance(description, str):
            function["description"] = description
        tools.append({"type": "function", "function": function})
    return tools


def _responses_tool_choice_to_chat(value: Any) -> Any:
    if value in {"auto", "none", "required"}:
        return value
    if isinstance(value, dict) and value.get("type") == "function":
        name = value.get("name")
        if isinstance(name, str) and name:
            return {"type": "function", "function": {"name": name}}
    return None


def _chat_completion_to_response_body(
    chat_body: dict[str, Any],
    *,
    model_name: str,
) -> dict[str, Any]:
    choice = _first_chat_choice(chat_body)
    message = choice.get("message") if isinstance(choice, dict) else {}
    if not isinstance(message, dict):
        message = {}

    output: list[dict[str, Any]] = []
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list) and tool_calls:
        for index, tool_call in enumerate(tool_calls):
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function")
            if not isinstance(function, dict):
                continue
            call_id = _string_or_default(
                tool_call.get("id"), f"call_compat_{index}",
            )
            name = _string_or_default(function.get("name"), "unknown")
            arguments = function.get("arguments")
            if not isinstance(arguments, str):
                arguments = json.dumps(
                    arguments or {}, separators=(",", ":"),
                )
            output.append({
                "id": f"fc_{index}",
                "type": "function_call",
                "status": "completed",
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            })

    if not output:
        text = _responses_content_to_text(message.get("content", ""))
        output.append({
            "id": "msg_0",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": [],
            }],
        })

    return {
        "id": _string_or_default(
            chat_body.get("id"), f"resp_chat_compat_{int(time.time())}",
        ),
        "object": "response",
        "created_at": int(chat_body.get("created") or time.time()),
        "status": "completed",
        "model": _string_or_default(chat_body.get("model"), model_name),
        "output": output,
        "parallel_tool_calls": False,
        "tool_choice": "auto",
        "tools": [],
        "usage": _chat_usage_to_responses_usage(chat_body.get("usage")),
    }


def _first_chat_choice(chat_body: dict[str, Any]) -> dict[str, Any]:
    choices = chat_body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise HTTPException(
            status_code=502,
            detail="chat-completions fallback returned no choices",
        )
    choice = choices[0]
    if not isinstance(choice, dict):
        raise HTTPException(
            status_code=502,
            detail="chat-completions fallback returned malformed choice",
        )
    return choice


def _chat_usage_to_responses_usage(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    input_tokens = _int_or_zero(value.get("prompt_tokens"))
    output_tokens = _int_or_zero(value.get("completion_tokens"))
    total_tokens = _int_or_zero(value.get("total_tokens"))
    if total_tokens == 0:
        total_tokens = input_tokens + output_tokens
    usage: dict[str, Any] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }
    # Keep the token details Responses usage has a place for. Without the
    # cached count, pricing cannot tell cache reads from fresh input and every
    # translated call was `price_unknown` (`missing_cache_usage`), although
    # the same model priced fine through Chat (#2054).
    for chat_key, responses_key, fields in (
        ("prompt_tokens_details", "input_tokens_details", ("cached_tokens", "audio_tokens")),
        ("completion_tokens_details", "output_tokens_details", ("reasoning_tokens", "audio_tokens")),
    ):
        details = _token_counts(value.get(chat_key), fields)
        if details:
            usage[responses_key] = details
    return usage


def _token_counts(value: Any, fields: tuple[str, ...]) -> dict[str, int]:
    """Only real non-negative counts: Codex parses these as integers, and an
    absent count must stay unknown rather than become zero."""
    if not isinstance(value, dict):
        return {}
    return {
        field: value[field]
        for field in fields
        if isinstance(value.get(field), int)
        and not isinstance(value[field], bool)
        and value[field] >= 0
    }


def _response_body_to_sse(response_body: dict[str, Any]) -> str:
    sequence = 0
    in_progress = {
        **response_body,
        "status": "in_progress",
        "output": [],
        "usage": None,
    }
    events: list[str] = [
        _sse_event("response.created", {
            "type": "response.created",
            "sequence_number": sequence,
            "response": in_progress,
        }),
    ]
    sequence += 1

    output = response_body.get("output")
    if isinstance(output, list):
        for output_index, item in enumerate(output):
            if not isinstance(item, dict):
                continue
            sequence = _append_response_item_sse_events(
                events, sequence, output_index, item,
            )

    events.append(_sse_event("response.completed", {
        "type": "response.completed",
        "sequence_number": sequence,
        "response": response_body,
    }))
    return "".join(events)


def _append_response_item_sse_events(
    events: list[str],
    sequence: int,
    output_index: int,
    item: dict[str, Any],
) -> int:
    if item.get("type") == "function_call":
        item_added = {**item, "status": "in_progress", "arguments": ""}
        events.append(_sse_event("response.output_item.added", {
            "type": "response.output_item.added",
            "sequence_number": sequence,
            "output_index": output_index,
            "item": item_added,
        }))
        sequence += 1
        arguments = _string_or_default(item.get("arguments"), "")
        events.append(_sse_event("response.function_call_arguments.delta", {
            "type": "response.function_call_arguments.delta",
            "sequence_number": sequence,
            "item_id": item["id"],
            "output_index": output_index,
            "delta": arguments,
        }))
        sequence += 1
        events.append(_sse_event("response.function_call_arguments.done", {
            "type": "response.function_call_arguments.done",
            "sequence_number": sequence,
            "item_id": item["id"],
            "output_index": output_index,
            "arguments": arguments,
        }))
        sequence += 1
        events.append(_sse_event("response.output_item.done", {
            "type": "response.output_item.done",
            "sequence_number": sequence,
            "output_index": output_index,
            "item": item,
        }))
        return sequence + 1

    content = item.get("content")
    if not isinstance(content, list) or not content:
        content = [{"type": "output_text", "text": "", "annotations": []}]
    part = content[0] if isinstance(content[0], dict) else {
        "type": "output_text",
        "text": str(content[0]),
        "annotations": [],
    }
    text = _responses_content_to_text(part)
    item_added = {**item, "status": "in_progress", "content": []}
    events.append(_sse_event("response.output_item.added", {
        "type": "response.output_item.added",
        "sequence_number": sequence,
        "output_index": output_index,
        "item": item_added,
    }))
    sequence += 1
    empty_part = {**part, "text": ""}
    events.append(_sse_event("response.content_part.added", {
        "type": "response.content_part.added",
        "sequence_number": sequence,
        "item_id": item["id"],
        "output_index": output_index,
        "content_index": 0,
        "part": empty_part,
    }))
    sequence += 1
    events.append(_sse_event("response.output_text.delta", {
        "type": "response.output_text.delta",
        "sequence_number": sequence,
        "item_id": item["id"],
        "output_index": output_index,
        "content_index": 0,
        "delta": text,
    }))
    sequence += 1
    events.append(_sse_event("response.output_text.done", {
        "type": "response.output_text.done",
        "sequence_number": sequence,
        "item_id": item["id"],
        "output_index": output_index,
        "content_index": 0,
        "text": text,
    }))
    sequence += 1
    events.append(_sse_event("response.content_part.done", {
        "type": "response.content_part.done",
        "sequence_number": sequence,
        "item_id": item["id"],
        "output_index": output_index,
        "content_index": 0,
        "part": part,
    }))
    sequence += 1
    events.append(_sse_event("response.output_item.done", {
        "type": "response.output_item.done",
        "sequence_number": sequence,
        "output_index": output_index,
        "item": item,
    }))
    return sequence + 1


def _sse_event(event: str, data: dict[str, Any]) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _string_or_default(value: Any, default: str) -> str:
    return value if isinstance(value, str) and value else default
