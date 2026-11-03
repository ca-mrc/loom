"""Per-message Claude ATIF from native evidence, with checked canonical joins."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from loom.claude_code_capture import ClaudeCapture
from loom.errors import AgentError
from loom.models.trajectory import AgentThoughtEvent, ToolUseEvent, TrajectoryEvent


def claude_atif_steps(
    native: bytes, instruction: str, *, trial_id: UUID, events: Sequence[TrajectoryEvent],
    model_name: str | None, succeeded: bool,
) -> list[dict[str, Any]]:
    capture = ClaudeCapture(trial_id=trial_id, instruction=instruction)
    capture.feed(native)
    capture.finish(exit_code=0, require_success=succeeded)
    semantic_types = (AgentThoughtEvent, ToolUseEvent)
    canonical = [event.model_dump(exclude={"emitted_at", "seq"}) for event in events if isinstance(event, semantic_types)]
    native_events = [event.model_dump(exclude={"emitted_at", "seq"}) for event in capture.events]
    if canonical != native_events:
        raise AgentError("Claude native/canonical tool or message identity drift")
    steps: list[dict[str, Any]] = [{"step_id": 1, "source": "user", "message": instruction}]
    for turn in capture.turns:
        step: dict[str, Any] = {"step_id": len(steps) + 1, "source": "agent", "message": "".join(turn["text"]),
                                "llm_call_count": 1, "extra": {"message_id": turn["id"]}}
        if model_name is not None:
            step["model_name"] = model_name
        if turn["thinking"]:
            step["reasoning_content"] = "\n\n".join(turn["thinking"])
        if turn["tools"]:
            step["tool_calls"] = [{"tool_call_id": tool["id"], "function_name": tool["name"], "arguments": tool["input"]}
                                  for tool in turn["tools"]]
            results = [{"source_call_id": tool["id"], "content": tool["result"]["content"],
                        "extra": {"is_error": tool["result"]["is_error"]}}
                       for tool in turn["tools"] if "result" in tool]
            if results:
                step["observation"] = {"results": results}
        steps.append(step)
    return steps
