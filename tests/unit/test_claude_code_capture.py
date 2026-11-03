"""Real Claude stream-json shapes, tool-result joins, and honest terminal state."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from loom.claude_code_capture import ClaudeCapture
from loom.errors import AgentError
from loom.models.trajectory import AgentThoughtEvent, ToolUseEvent


def _lines():
    # Pinned CLI emits assistant messages, user tool results, then a result.
    return [
        {"type": "system", "subtype": "init", "session_id": "session", "tools": ["Read", "Write"]},
        {"type": "assistant", "message": {"id": "msg_1", "role": "assistant", "content": [
            {"type": "text", "text": "Read both files."},
            {"type": "tool_use", "id": "a", "name": "Read", "input": {"file_path": "a.py"}},
            {"type": "tool_use", "id": "b", "name": "Read", "input": {"file_path": "b.py"}},
        ]}},
        {"type": "user", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "b", "content": "café"},
            {"type": "tool_result", "tool_use_id": "a", "content": "not found", "is_error": True},
        ]}},
        {"type": "assistant", "message": {"id": "msg_2", "role": "assistant", "content": [{"type": "text", "text": "Done."}]}},
        {"type": "result", "subtype": "success", "is_error": False, "permission_denials": [], "total_cost_usd": 999},
    ]


def test_fragmented_capture_joins_tool_results_by_id_and_keeps_native_order():
    capture = ClaudeCapture(trial_id=uuid4(), instruction="Fix the task.")
    wire = b"".join(json.dumps(item, ensure_ascii=False).encode() + b"\n" for item in _lines())
    for byte in wire:
        capture.feed(bytes([byte]))
    capture.finish(exit_code=0)
    tools = [event for event in capture.events if isinstance(event, ToolUseEvent)]
    assert [(event.args["file_path"], event.result["content"], event.result["is_error"]) for event in tools] == [
        ("b.py", "café", False), ("a.py", "not found", True),
    ]
    assert [event.content for event in capture.events if isinstance(event, AgentThoughtEvent)] == ["Read both files.", "Done."]
    assert len(capture.turns) == 2
    assert [tool["id"] for tool in capture.turns[0]["tools"]] == ["a", "b"]
    assert capture.turns[0]["tools"][0]["result"]["is_error"]
    assert [event.seq for event in capture.events] == list(range(len(capture.events)))


@pytest.mark.parametrize("result", [
    {"type": "result", "subtype": "error_during_execution", "is_error": True},
    {"type": "result", "subtype": "error_max_turns", "is_error": False},
    {"type": "result", "subtype": "success", "is_error": False, "permission_denials": [{"tool_name": "Write"}]},
])
def test_zero_process_exit_does_not_hide_terminal_error_or_tool_denial(result):
    capture = ClaudeCapture(trial_id=uuid4(), instruction="Fix it.")
    capture.feed(json.dumps(result).encode() + b"\n")
    with pytest.raises(AgentError):
        capture.finish(exit_code=0)


@pytest.mark.parametrize("damage", ["orphan", "duplicate", "missing_result", "missing_terminal", "truncated"])
def test_invalid_joins_or_incomplete_capture_never_become_success(damage):
    lines = _lines()
    if damage == "orphan":
        lines[2]["message"]["content"][0]["tool_use_id"] = "foreign"
    elif damage == "duplicate":
        lines[2]["message"]["content"].append(lines[2]["message"]["content"][0])
    elif damage == "missing_result":
        del lines[2]
    elif damage == "missing_terminal":
        lines.pop()
    wire = b"".join(json.dumps(item).encode() + b"\n" for item in lines)
    if damage == "truncated":
        wire = wire[:-3]
    capture = ClaudeCapture(trial_id=uuid4(), instruction="Fix it.")
    with pytest.raises(AgentError):
        capture.feed(wire)
        capture.finish(exit_code=0)


def test_capture_bounds_an_unterminated_json_line():
    capture = ClaudeCapture(trial_id=uuid4(), instruction="Fix it.", max_bytes=32)
    with pytest.raises(AgentError, match="limit"):
        capture.feed(b"x" * 33)


def test_assistant_blocks_with_one_message_id_form_one_turn():
    capture = ClaudeCapture(trial_id=uuid4(), instruction="Fix it.")
    lines = _lines()
    first = lines[1]
    text, *tools = first["message"]["content"]
    lines[1:2] = [
        {**first, "message": {**first["message"], "content": [text]}},
        {**first, "message": {**first["message"], "content": tools}},
    ]
    capture.feed(b"".join(json.dumps(item).encode() + b"\n" for item in lines))
    capture.finish(exit_code=0)
    assert len(capture.turns) == 2 and len(capture.turns[0]["tools"]) == 2
