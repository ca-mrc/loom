"""Codex session log to an ATIF trajectory.

Codex records every model request in its session log ("rollout",
`CODEX_HOME/sessions/**/rollout-*.jsonl`): messages, reasoning summaries, full
tool-call arguments (including `apply_patch` text) and tool outputs. Its
`exec --json` stream omits most of that.

`harbor_trajectory` ports Harbor's Codex converter
(`harbor/agents/installed/codex.py`, the commit pinned for the harness
controller) so a hosted Codex trajectory matches what Harbor produces, one ATIF
step per model request. Harbor's LiteLLM cost estimate is left out; Loom's
cost comes from the Gateway ledger. `clean_trajectory` then drops what is not
the agent's own work: Codex's injected context messages, per-step `extra`
metadata and tool-output headers.
"""

from __future__ import annotations

import json
import re
from typing import Any

# Codex injects these as user/developer messages ahead of the task.
_CONTEXT_PREFIXES = (
    "<environment_context>", "<permissions instructions>", "<user_instructions>",
    "# AGENTS.md instructions", "<turn_aborted>",
)
# `exec_command` and `apply_patch` outputs carry a header before the output.
_OUTPUT_HEADER = re.compile(
    r"\A(?:Chunk ID: [^\n]*\n)?Wall time: [^\n]*\n"
    r"(?:Process exited with code (?P<process>-?\d+)\n)?"
    r"(?:Original token count: [^\n]*\n)?Output:\n",
)
_EXIT_CODE = re.compile(r"\AExit code: (?P<exit>-?\d+)\n")


def _message_text(content: list[Any]) -> str:
    return "".join(block["text"] for block in content
                   if isinstance(block, dict) and isinstance(block.get("text"), str))


def _output_blob(raw: Any) -> tuple[str | None, dict[str, Any] | None]:
    if raw is None:
        return None, None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw, None
    else:
        parsed = raw
    if isinstance(parsed, dict):
        output = parsed.get("output")
        if output is None and parsed:
            output = json.dumps(parsed, ensure_ascii=False)
        metadata = parsed.get("metadata")
        return output, metadata if isinstance(metadata, dict) else None
    return str(parsed), None


def _token_metrics(payload: dict[str, Any]) -> dict[str, Any] | None:
    info = payload.get("info")
    last = info.get("last_token_usage") if isinstance(info, dict) else None
    if not isinstance(last, dict):
        return None
    return {
        "prompt_tokens": last.get("input_tokens") or None,
        "completion_tokens": last.get("output_tokens") or None,
        "cached_tokens": last.get("cached_input_tokens") or None,
        "extra": {"reasoning_output_tokens": last.get("reasoning_output_tokens"),
                  "total_tokens": last.get("total_tokens")},
    }


def _group(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge assistant events from one model request into a single step."""
    result: list[dict[str, Any]] = []
    groups: dict[str, dict[str, Any]] = {}

    def flush() -> None:
        for group in groups.values():
            group["tool_calls"].sort(key=lambda call: call.get("tool_order", 0))
            group["text"] = "\n\n".join(part for part in group.pop("message_parts") if part)
            result.append(group)
        groups.clear()

    for event in events:
        call_id = event.get("api_call_id")
        if (event["kind"] == "message" and event.get("role") != "assistant") or not isinstance(call_id, str):
            flush()
            result.append(event)
            continue
        group = groups.setdefault(call_id, {
            "kind": "bundled", "api_call_id": call_id, "codex_turn_id": event.get("codex_turn_id"),
            "timestamp": event.get("timestamp"), "message_parts": [], "reasoning": None,
            "tool_calls": [], "metrics": event.get("metrics"),
        })
        if event["kind"] == "message":
            if event.get("text"):
                group["message_parts"].append(event["text"])
            if event.get("reasoning"):
                group["reasoning"] = event["reasoning"]
            if event.get("timestamp"):
                group["timestamp"] = event["timestamp"]
        else:
            group["tool_calls"].append(event)
            if not group["reasoning"] and event.get("reasoning"):
                group["reasoning"] = event["reasoning"]
            if not group.get("metrics") and event.get("metrics"):
                group["metrics"] = event["metrics"]
    flush()
    return result


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _drop_none(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_drop_none(item) for item in value]
    return value


def _step(event: dict[str, Any], step_id: int, model_name: str | None) -> dict[str, Any]:
    if event["kind"] == "message":
        role = event.get("role", "user")
        source = "agent" if role == "assistant" else "user" if role == "user" else "system"
        agent = source == "agent"
        return {
            "step_id": step_id, "timestamp": event.get("timestamp"), "source": source,
            "model_name": model_name if agent else None, "message": event.get("text", ""),
            "reasoning_content": event.get("reasoning") if agent and event.get("reasoning") else None,
            "llm_call_count": 1 if agent else None,
        }
    if event["kind"] == "tool_call":
        call_id = event.get("call_id", "")
        arguments = event.get("arguments") or {}
        extra: dict[str, Any] = {}
        if event.get("metadata"):
            extra["tool_metadata"] = event["metadata"]
        for key in ("raw_arguments", "status", "api_call_id", "codex_turn_id"):
            if event.get(key):
                extra[key] = event[key]
        output = event.get("output")
        return {
            "step_id": step_id, "timestamp": event.get("timestamp"), "source": "agent",
            "model_name": model_name, "message": event.get("message") or "",
            "reasoning_content": event.get("reasoning") or None,
            "tool_calls": [{"tool_call_id": call_id, "function_name": event.get("tool_name", ""),
                            "arguments": arguments if isinstance(arguments, dict) else {"value": arguments}}],
            "observation": ({"results": [{"source_call_id": call_id or None, "content": output}]}
                            if output is not None else None),
            "metrics": event.get("metrics") if isinstance(event.get("metrics"), dict) else None,
            "llm_call_count": 1, "extra": extra or None,
        }
    calls = event.get("tool_calls", [])
    extra = {key: event[key] for key in ("api_call_id", "codex_turn_id") if event.get(key)}
    details = {}
    for call in calls:
        detail = {key: call[key] for key in ("metadata", "raw_arguments", "status") if call.get(key)}
        if detail:
            details[call.get("call_id", "")] = detail
    if details:
        extra["tool_call_details"] = details
    return {
        "step_id": step_id, "timestamp": event.get("timestamp"), "source": "agent",
        "model_name": model_name, "message": event.get("text", ""),
        "reasoning_content": event.get("reasoning") or None,
        "tool_calls": [{"tool_call_id": call.get("call_id", ""), "function_name": call.get("tool_name", ""),
                        "arguments": (call.get("arguments") or {}) if isinstance(call.get("arguments") or {}, dict)
                        else {"value": call.get("arguments")}} for call in calls] or None,
        "observation": ({"results": [{"source_call_id": call.get("call_id") or None, "content": call.get("output")}
                                     for call in calls]} if calls else None),
        "metrics": event.get("metrics") or None, "llm_call_count": 1, "extra": extra or None,
    }


def harbor_trajectory(session: bytes, *, model_name: str | None = None) -> dict[str, Any] | None:
    """Harbor's ATIF for one Codex session log, or None when it has no steps."""
    raw: list[dict[str, Any]] = []
    for line in session.splitlines():
        if line.strip():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                raw.append(item)
    if not raw:
        return None
    meta = next((e for e in raw if e.get("type") == "session_meta"), None)
    meta_payload = meta.get("payload", {}) if meta else {}
    agent_extra = {key: meta_payload[key] for key in ("originator", "cwd", "git", "instructions")
                   if meta_payload.get(key) is not None}
    default_model = next((e["payload"]["model"] for e in raw if e.get("type") == "turn_context"
                          and isinstance(e.get("payload", {}).get("model"), str)), model_name)

    events: list[dict[str, Any]] = []
    pending: dict[str, dict[str, Any]] = {}
    reasoning: str | None = None
    turn_id: str | None = None
    index, saw_output, order = 1, False, 0
    metrics_by_call: dict[str, dict[str, Any]] = {}

    def call_id_now() -> str:
        return f"api_call_{index}"

    for item in raw:
        kind, payload, timestamp = item.get("type"), item.get("payload", {}), item.get("timestamp")
        if kind == "event_msg" and isinstance(payload, dict):
            event_type = payload.get("type")
            if event_type in {"task_started", "turn_started"}:
                turn_id = payload.get("turn_id") if isinstance(payload.get("turn_id"), str) else None
            elif event_type in {"task_complete", "turn_complete", "turn_aborted"}:
                turn_id = None
            elif event_type == "token_count" and saw_output:
                metrics = _token_metrics(payload)
                if metrics:
                    metrics_by_call[call_id_now()] = metrics
                index, saw_output, order = index + 1, False, 0
            continue
        if kind == "turn_context":
            if isinstance(payload, dict) and isinstance(payload.get("turn_id"), str) and turn_id is None:
                turn_id = payload["turn_id"]
            continue
        if kind != "response_item" or not isinstance(payload, dict):
            continue
        payload_type = payload.get("type")
        base = {"api_call_id": call_id_now(), "codex_turn_id": turn_id, "timestamp": timestamp}
        if payload_type == "reasoning":
            summary = payload.get("summary")
            parts: list[str] = []
            for part in summary if isinstance(summary, list) else []:
                text = part if isinstance(part, str) else part.get("text") if isinstance(part, dict) else None
                if isinstance(text, str):
                    parts.append(text)
            reasoning = "\n".join(parts) if parts else None
        elif payload_type == "message":
            content = payload.get("content", [])
            assistant = payload.get("role") == "assistant"
            events.append({**base, "kind": "message", "role": payload.get("role", "user"),
                           "text": _message_text(content) if isinstance(content, list) else "",
                           "reasoning": reasoning if assistant else None})
            saw_output = saw_output or assistant
            reasoning = None
        elif payload_type == "web_search_call":
            action = payload.get("action") or {}
            arguments = {"action_type": action.get("type", ""),
                         **{key: action[key] for key in ("query", "queries", "url") if key in action}}
            events.append({**base, "kind": "tool_call", "tool_order": order, "call_id": "",
                           "tool_name": "web_search_call", "arguments": arguments, "raw_arguments": None,
                           "reasoning": reasoning, "status": payload.get("status"), "message": None})
            order, saw_output, reasoning = order + 1, True, None
        elif payload_type in {"function_call", "custom_tool_call"}:
            call_id = payload.get("call_id")
            if not call_id:
                continue
            raw_arguments = payload.get("arguments" if payload_type == "function_call" else "input")
            try:
                arguments = json.loads(raw_arguments)  # type: ignore[arg-type]  # non-strings fall through
            except (json.JSONDecodeError, TypeError):
                arguments = ({"input": raw_arguments} if isinstance(raw_arguments, str)
                             else {} if raw_arguments is None else {"value": raw_arguments})
            pending[call_id] = {**base, "kind": "tool_call", "tool_order": order, "call_id": call_id,
                                "tool_name": payload.get("name") or "", "arguments": arguments,
                                "raw_arguments": raw_arguments, "reasoning": reasoning,
                                "status": payload.get("status"), "message": None}
            order, saw_output, reasoning = order + 1, True, None
        elif payload_type in {"function_call_output", "custom_tool_call_output"}:
            call_id = payload.get("call_id")
            output, metadata = _output_blob(payload.get("output"))
            call = pending.pop(call_id, None) if call_id else None
            if call is None:
                call = {**base, "kind": "tool_call", "tool_order": order, "call_id": call_id or "",
                        "tool_name": payload.get("name", "") or "", "arguments": {}, "raw_arguments": None,
                        "reasoning": reasoning, "status": None, "message": None}
                order += 1
            call.update(output=output, metadata=metadata, timestamp=call.get("timestamp") or timestamp)
            events.append(call)
            reasoning = None

    for event in events:
        if event.get("api_call_id") in metrics_by_call:
            event["metrics"] = metrics_by_call[event["api_call_id"]]
    steps = [_step(event, step_id, default_model) for step_id, event in enumerate(_group(events), start=1)]
    if not steps:
        return None

    final_metrics = None
    for item in reversed(raw):
        info = item.get("payload", {}).get("info") if item.get("type") == "event_msg" else None
        if item.get("payload", {}).get("type") != "token_count" or not isinstance(info, dict):
            continue
        total = info.get("total_token_usage")
        if not isinstance(total, dict):
            continue
        cost = info.get("total_cost") if info.get("total_cost") is not None else info.get("cost_usd")
        final_metrics = {
            "total_prompt_tokens": total.get("input_tokens") or None,
            "total_completion_tokens": total.get("output_tokens") or None,
            "total_cached_tokens": total.get("cached_input_tokens") or None,
            "total_cost_usd": cost, "total_steps": len(steps),
            "extra": {"reasoning_output_tokens": total.get("reasoning_output_tokens"),
                      "total_tokens": total.get("total_tokens"),
                      "last_token_usage": info.get("last_token_usage")},
        }
        break
    document: dict[str, Any] = _drop_none({
        "schema_version": "ATIF-v1.7",
        "session_id": meta_payload.get("id") or "codex-session",
        "agent": {"name": "codex", "version": meta_payload.get("cli_version") or "unknown",
                  "model_name": default_model, "extra": agent_extra or None},
        "steps": steps,
        "final_metrics": final_metrics,
    })
    return document


def _observation(result: dict[str, Any]) -> dict[str, Any]:
    content = result.get("content")
    if not isinstance(content, str):
        return result
    header = _OUTPUT_HEADER.match(content)
    exit_match = _EXIT_CODE.match(content)
    if exit_match is not None:
        header = _OUTPUT_HEADER.match(content[exit_match.end():])
        if header is not None:
            return {**result, "content": content[exit_match.end() + header.end():],
                    "exit_code": int(exit_match["exit"])}
    if header is not None:
        cleaned = {**result, "content": content[header.end():]}
        if header["process"] is not None:
            cleaned["exit_code"] = int(header["process"])
        return cleaned
    return result


def clean_trajectory(document: dict[str, Any]) -> dict[str, Any]:
    """Keep the agent's work: the task, messages, reasoning, tool calls, observations.

    Codex's injected context messages (sandbox permissions, environment and
    AGENTS.md context) and per-step `extra` metadata are dropped, tool outputs
    lose Codex's timing headers (the exit code is kept), and steps are
    renumbered. Session-level totals are dropped; the caller attaches
    authoritative accounting.
    """
    steps: list[dict[str, Any]] = []
    for step in document.get("steps", []):
        message = step.get("message") or ""
        if step.get("source") == "system" or (
            step.get("source") == "user" and message.lstrip().startswith(_CONTEXT_PREFIXES)
        ):
            continue
        cleaned = {key: value for key, value in step.items() if key != "extra"}
        if isinstance(step.get("observation"), dict):
            cleaned["observation"] = {"results": [
                _observation(result) for result in step["observation"].get("results", [])
                if isinstance(result, dict)
            ]}
        steps.append({**cleaned, "step_id": len(steps) + 1})
    agent = {key: value for key, value in document.get("agent", {}).items() if key != "extra"}
    return {"schema_version": document.get("schema_version", "ATIF-v1.7"), "agent": agent, "steps": steps}


__all__ = ["clean_trajectory", "harbor_trajectory"]
