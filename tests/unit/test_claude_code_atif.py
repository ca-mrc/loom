"""Canonical Claude trajectories preserve real turns and verifier/accounting owners."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from loom.claude_code_capture import ClaudeCapture
from loom.models.trajectory import TrialEndEvent, TrialStartEvent
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_atif,
)
from tests.unit.test_claude_code_capture import _lines


def _case(lines=None, final_state="succeeded"):
    trial_id = uuid4()
    instruction = "Fix the task, preserving its tests."
    raw = b"".join(json.dumps(item).encode() + b"\n" for item in (_lines() if lines is None else lines))
    capture = ClaudeCapture(trial_id=trial_id, instruction=instruction)
    capture.feed(raw)
    events = [TrialStartEvent(trial_id=trial_id, step_id="agent", seq=0, emitted_at=datetime.now(UTC),
                             task_id="task", agent_name="claude-code", agent_mode="out-of-box"),
              *capture.events,
              TrialEndEvent(trial_id=trial_id, step_id="agent", seq=len(capture.events) + 1,
                            emitted_at=datetime.now(UTC), final_state=final_state)]
    return raw, instruction, events


def test_native_turns_keep_tool_ids_and_original_instruction_without_cli_costs():
    raw, instruction, events = _case()
    document = json.loads(build_canonical_atif(events, task_id="task", agent_name="claude-code", agent_version="2.1.183",
        claude_native=raw, claude_instruction=instruction.encode()))
    assert len(document["steps"]) == 3
    assert document["steps"][0]["message"] == instruction
    assert document["steps"][1]["message"] == "Read both files."
    assert [call["tool_call_id"] for call in document["steps"][1]["tool_calls"]] == ["a", "b"]
    assert [(result["source_call_id"], result["content"]) for result in document["steps"][1]["observation"]["results"]] == [
        ("a", "not found"), ("b", "café"),
    ]
    assert document["metadata"]["final_state"] == "succeeded"
    assert document["accounting"]["totals"]["cost_usd"] == 0  # No ledger events in this fixture.
    assert "999" not in json.dumps(document)


def test_successful_materialization_requires_native_evidence():
    _, _, events = _case()
    with pytest.raises(MaterializationIntegrityError, match="claude_native"):
        build_canonical_atif(events, task_id="task", agent_name="claude-code", agent_version="2.1.183")


def test_cancelled_native_turn_keeps_pending_tool_without_fabricating_an_observation():
    raw, instruction, events = _case(_lines()[:2], final_state="cancelled")
    document = json.loads(build_canonical_atif(events, task_id="task", agent_name="claude-code", agent_version="2.1.183",
        claude_native=raw, claude_instruction=instruction.encode()))
    assert len(document["steps"]) == 2
    assert len(document["steps"][1]["tool_calls"]) == 2
    assert "observation" not in document["steps"][1]
    assert document["metadata"]["final_state"] == "cancelled"


def test_materializer_rejects_native_and_canonical_tool_drift():
    raw, instruction, events = _case()
    events[2] = events[2].model_copy(update={"args": {"file_path": "forged.py"}})
    with pytest.raises(MaterializationIntegrityError, match="claude_native"):
        build_canonical_atif(events, task_id="task", agent_name="claude-code", agent_version="2.1.183",
                            claude_native=raw, claude_instruction=instruction.encode())


async def test_source_copy_retains_both_claude_inputs_for_canonical_derivation():
    from loom.trajectory.storage import FakeObjectStore
    from tests.unit.test_harbor_subagent_materialization import _copy_bodies, _worker

    raw, instruction, _ = _case()
    bodies = {"artifacts/claude-code/events.jsonl": raw, "artifacts/claude-code/instruction.txt": instruction.encode()}
    source, canonical = FakeObjectStore(), FakeObjectStore()
    files, _, inputs = await _copy_bodies(_worker(source, canonical), source, bodies)
    assert inputs == bodies
    assert {file.relative_path for file in files} == bodies.keys()


def test_materialized_claude_usage_and_verifier_come_from_canonical_owners():
    from loom.service_execution_claude_code import claude_usage
    from loom.trajectory.llm_call_events import llm_call_row_to_event
    from loom_control_plane.service_execution_materializer import build_canonical_events, validate_usage_accounting
    from tests.unit.test_native_claude_code import _trial
    from tests.unit.test_native_codex import _result
    from tests.unit.test_service_execution_terminus_plan import _inputs

    trial_id = uuid4()
    task, _, _ = _inputs()
    row = {"id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent", "model": "code-model",
           "dialect": "anthropic_facade", "input_tokens": 20, "output_tokens": 5, "cost_usd": 0.01,
           "provider_extras": {"_loom_model_identity": {"provider": "vendor", "name": "code-model"}}}
    call = llm_call_row_to_event(row, trial_id=trial_id, seq=0)
    trace = call.model_dump_json().encode() + b"\n"
    usage = claude_usage([call], _trial())
    validate_usage_accounting(usage_body=json.dumps(usage).encode(), trace_body=trace, trial_config=_trial())
    events = build_canonical_events(trial_id=trial_id, task_id="task", task_config=task, trial_config=_trial(),
        runtime_result=_result(), trace_body=trace, verifier_body=b'{"rewards":{"passed":1}}', gateway_calls=[row])
    assert any(event.kind == "llm_call" and event.model.provider == "vendor" for event in events)
    assert any(event.kind == "verifier_end" and event.result.rewards == {"passed": 1} for event in events)
    with pytest.raises(MaterializationIntegrityError, match="trajectory_invalid"):
        build_canonical_events(trial_id=trial_id, task_id="task", task_config=task, trial_config=_trial(),
            runtime_result=_result(), trace_body=trace, verifier_body=b'{"rewards":{"passed":1}}', gateway_calls=[])
