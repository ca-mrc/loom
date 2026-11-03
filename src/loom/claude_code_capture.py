"""Shared bounded capture of Claude Code's native stream-json evidence.

CLI usage/cost summaries are diagnostic only. Model accounting comes from the
Gateway or local relay ledger. Tool observations join by tool_use_id, never by
array position; blocks carrying the same assistant message ID form one turn.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from loom.errors import AgentError
from loom.models.trajectory import AgentThoughtEvent, ToolUseEvent, TrajectoryEvent

MAX_CLAUDE_NATIVE_BYTES = 16 * 1024 * 1024


class ClaudeCapture:
    def __init__(self, *, trial_id: UUID, instruction: str, max_bytes: int = MAX_CLAUDE_NATIVE_BYTES) -> None:
        if not instruction.strip():
            raise AgentError("Claude requires a nonempty task instruction")
        self.trial_id = trial_id
        self.instruction = instruction
        self.events: list[TrajectoryEvent] = []
        self.turns: list[dict[str, Any]] = []
        self.result: dict[str, Any] | None = None
        self._pending: dict[str, dict[str, Any]] = {}
        self._tool_ids: set[str] = set()
        self._message_ids: set[str] = set()
        self._buffer = bytearray()
        self._bytes = 0
        self._max_bytes = max_bytes

    def feed(self, chunk: bytes) -> None:
        self._bytes += len(chunk)
        if self._bytes > self._max_bytes:
            raise AgentError("Claude native evidence exceeds size limit")
        self._buffer.extend(chunk)
        while (end := self._buffer.find(b"\n")) >= 0:
            line = bytes(self._buffer[:end])
            del self._buffer[:end + 1]
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise AgentError("Claude produced malformed native JSON") from exc
            if not isinstance(payload, dict):
                raise AgentError("Claude native event must be an object")
            self._consume(payload)

    def _base(self) -> dict[str, Any]:
        return {"trial_id": self.trial_id, "step_id": "agent", "seq": len(self.events), "emitted_at": datetime.now(UTC)}

    def _consume(self, payload: dict[str, Any]) -> None:
        kind = payload.get("type")
        # Native lifecycle/rate-limit/progress events remain in the raw artifact.
        if kind not in {"assistant", "user", "result"}:
            return
        # A second terminal result or semantic output after it is not a single
        # complete CLI execution, even if the process exits zero.
        if self.result is not None:
            raise AgentError("Claude native events continue after terminal result")
        if payload.get("parent_tool_use_id") is not None:
            raise AgentError("Claude subagent output is outside the workspace-tool profile")
        if kind == "result":
            self.result = payload
            return
        message = payload.get("message")
        if not isinstance(message, dict) or not isinstance(message.get("content"), list):
            raise AgentError("Claude native message has invalid content")
        content = message["content"]
        if kind == "assistant":
            self._assistant(message, content)
        else:
            for block in content:
                if not isinstance(block, dict):
                    raise AgentError("Claude native user block is invalid")
                if block.get("type") == "tool_result":
                    tool_id = block.get("tool_use_id")
                    if not isinstance(tool_id, str) or tool_id not in self._pending:
                        raise AgentError("Claude tool result has an orphan or repeated ID")
                    tool = self._pending.pop(tool_id)
                    result = {"content": block.get("content", ""), "is_error": bool(block.get("is_error", False)),
                              "tool_use_id": tool_id}
                    tool["result"] = result
                    self.events.append(ToolUseEvent(**self._base(), tool_name=tool["name"], args=tool["input"],
                                                    result=result, duration_sec=0))
                elif block.get("type") == "text" and isinstance(block.get("text"), str):
                    # Preserve CLI-injected user context as native evidence, not
                    # as the operator's trusted initial task instruction.
                    continue
                else:
                    raise AgentError("unsupported Claude native user content")

    def _assistant(self, message: dict[str, Any], content: list[Any]) -> None:
        message_id = message.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise AgentError("Claude assistant message is missing its ID")
        if not self.turns or self.turns[-1]["id"] != message_id:
            if message_id in self._message_ids:
                raise AgentError("Claude assistant message ID reappeared out of order")
            if self._pending:
                raise AgentError("Claude started another turn before tool results")
            self._message_ids.add(message_id)
            self.turns.append({"id": message_id, "text": [], "thinking": [], "tools": []})
        turn = self.turns[-1]
        for block in content:
            if not isinstance(block, dict):
                raise AgentError("invalid Claude assistant block")
            kind = block.get("type")
            if kind in {"text", "thinking"}:
                text = block.get("text" if kind == "text" else "thinking")
                if not isinstance(text, str):
                    raise AgentError("invalid Claude assistant text")
                turn["text" if kind == "text" else "thinking"].append(text)
                self.events.append(AgentThoughtEvent(**self._base(), content=text, sdk_event_type=f"claude.{kind}",
                                                    reasoning_content=text if kind == "thinking" else None))
            elif kind == "tool_use":
                tool_id, name, arguments = block.get("id"), block.get("name"), block.get("input")
                if (not isinstance(tool_id, str) or not tool_id or tool_id in self._tool_ids
                        or not isinstance(name, str) or not name or not isinstance(arguments, dict)):
                    raise AgentError("invalid or duplicate Claude tool call")
                self._tool_ids.add(tool_id)
                tool = {"id": tool_id, "name": name, "input": arguments}
                self._pending[tool_id] = tool
                turn["tools"].append(tool)
            elif kind == "redacted_thinking":
                # Opaque provider evidence stays native; it cannot be represented
                # as a supplied textual thinking summary.
                continue
            else:
                raise AgentError("unsupported Claude assistant content")

    def finish(self, *, exit_code: int | None, require_success: bool = True) -> None:
        if not require_success:
            return
        if self._buffer.strip():
            raise AgentError("Claude native evidence has a truncated final line")
        if exit_code != 0:
            raise AgentError(f"claude-code exited rc={exit_code}")
        if (self.result is None or self.result.get("subtype") != "success"
                or self.result.get("is_error") is not False or self.result.get("permission_denials")):
            raise AgentError("Claude terminal result is missing, failed, or reports denied tools")
        if self._pending:
            raise AgentError("Claude terminal result is missing tool observations")
        if not self.turns:
            raise AgentError("Claude produced no assistant turn")
