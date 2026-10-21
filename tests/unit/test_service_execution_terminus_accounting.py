"""A discarded length response is still a real, billable Gateway request."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from loom.db.schema import LlmCall
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.llm_call_ledger import serialize_execution_accounting_call
from loom.models.trajectory import (
    LLMCallEvent,
    Terminus2CommandEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
)
from loom.models.trial import TrialConfig
from loom.service_execution_terminus_trace import (
    parse_terminus_events,
    reconcile_terminus_ledger,
    terminus_usage,
)
from loom.trajectory.llm_call_events import llm_call_row_to_event
from loom_control_plane.service_execution_materializer import (
    build_canonical_atif,
    build_canonical_events,
)
from tests.unit.test_service_execution_materialization import _REVISION, _RUNTIME_IMAGE, _TASK_IMAGE
from tests.unit.test_service_execution_terminus_plan import _inputs


def _case():
    trial_id = uuid4()
    trial = TrialConfig(
        agent_name="terminus-2", agent_model={"provider": "openai", "name": "glm-5.2"}
    )
    now = datetime(2026, 9, 10, tzinfo=UTC)
    base = {"trial_id": trial_id, "step_id": "agent", "seq": 0, "emitted_at": now}
    events = [Terminus2UserPromptEvent(**base, prompt_id="p", harbor_step_id=1, message="Solve")]
    rows = []
    for index, (prompt, completion) in enumerate(
        [
            (906, 8192),
            (968, 183),
            (1483, 1477),
            (2230, 1330),
            (3842, 238),
            (4064, 468),
        ]
    ):
        rows.append(
            {
                "id": str(uuid4()),
                "trial_id": str(trial_id),
                "step_id": "agent",
                "dialect": "openai_facade",
                "model": "glm-5.2",
                "input_tokens": prompt,
                "output_tokens": completion,
                "cost_usd": 0.01,
                "rate_card_hash": "test-rate",
                "provider_extras": {},
                "captured_at": (now + timedelta(seconds=index)).isoformat(),
                "finish_reason": "length" if index == 0 else "stop",
                "attempt": 1,
            }
        )
        if index == 0:
            continue  # Harbor discarded this response; there is no native step.
        events.extend(
            [
                llm_call_row_to_event(rows[-1], trial_id=trial_id, seq=0),
                Terminus2TurnEvent(
                    **base,
                    turn_id=f"t{index}",
                    turn_index=index - 1,
                    gateway_request_id=rows[-1]["id"],
                    parse_state="ok",
                    completion_state="continue",
                    harbor_step_id=index + 1,
                    analysis="inspect",
                    plan="list",
                ),
                Terminus2CommandEvent(
                    **base,
                    turn_id=f"t{index}",
                    command_batch_id=f"b{index}",
                    command_id=f"c{index}",
                    index=0,
                    keystrokes="ls\n",
                    duration_sec=0.1,
                ),
                Terminus2TerminalObservationEvent(
                    **base,
                    turn_id=f"t{index}",
                    command_batch_id=f"b{index}",
                    observation_id=f"o{index}",
                    text="ok",
                    capture_source="incremental",
                    byte_len=2,
                    truncated=False,
                    completeness="full",
                    content_hash="fixture",
                    redaction_applied=False,
                    is_aggregate=False,
                ),
            ]
        )
    return (
        trial,
        trial_id,
        [event.model_copy(update={"seq": i}) for i, event in enumerate(events)],
        rows,
    )


@pytest.mark.parametrize("limited", [False, True])
def test_truncated_request_counts_without_inventing_a_harbor_turn(limited):
    trial, trial_id, native, rows = _case()
    if limited:
        trial = trial.model_copy(update={"agent_model": trial.agent_model.model_copy(update={
            "max_input_tokens": 16384, "max_output_tokens": 256,
        })})
    assert terminus_usage(native, trial)["call_count"] == 5
    events = reconcile_terminus_ledger(native, rows, trial, trial_id)
    usage = terminus_usage(events, trial)
    assert usage["call_count"] == 6
    assert usage["totals"]["input_tokens"] == 13493
    assert usage["totals"]["output_tokens"] == 11888
    assert usage["totals"]["cost_usd"] == pytest.approx(0.06)
    assert usage["gateway_request_ids"] == [row["id"] for row in rows]
    assert events[0].finish_reason == "length"
    assert events[0].messages == []  # No invented content for the discarded response.
    assert [
        event.model_dump(exclude={"seq"}) for event in events if not isinstance(event, LLMCallEvent)
    ] == [
        event.model_dump(exclude={"seq"}) for event in native if not isinstance(event, LLMCallEvent)
    ]
    runtime = ExecutionRuntimeResultV1.model_validate(
        {
            "schema_version": "loom.execution-runtime-result.v1",
            "runtime_contract_sha256": "sha256:" + "1" * 64,
            "candidate_sha": "1" * 40,
            "task_revision_sha256": _REVISION,
            "command_identity_sha256": "sha256:" + "2" * 64,
            "execution_role": "attempt",
            "container_roles": ["execution", "agent", "verifier"],
            "task_image_ref": _TASK_IMAGE,
            "runtime_image_ref": _RUNTIME_IMAGE,
            "runtime_binary_sha256": "sha256:" + "3" * 64,
            "execution_class_id": "linux-amd64-cpu-pod-v1",
            "status": "succeeded",
            "started_at": native[0].emitted_at,
            "finished_at": native[-1].emitted_at,
            "phases": [],
            "outputs": [],
            "verifier_rewards": {"passed": 1},
            "partial_evidence": False,
        }
    )
    canonical = build_canonical_events(
        trial_id=trial_id,
        task_id="task",
        task_config=_inputs()[0],
        trial_config=trial,
        runtime_result=runtime,
        gateway_calls=rows,
        trace_body=b"\n".join(event.model_dump_json().encode() for event in native),
        verifier_body=b'{"rewards":{"passed":1}}',
    )
    assert terminus_usage(list(canonical), trial) == usage
    assert canonical[-1].final_state == "succeeded"
    atif = json.loads(
        build_canonical_atif(canonical, task_id="task", agent_name="terminus-2", agent_version="2")
    )
    assert atif["accounting"] == usage
    assert len(atif["steps"]) == 6  # Initial user + five actual agent turns.
    assert sum(len(step.get("tool_calls", [])) for step in atif["steps"]) == 5
    assert reconcile_terminus_ledger(events, rows, trial, trial_id) == events


@pytest.mark.parametrize("damage", ["missing", "duplicate", "trial", "step", "model", "tokens"])
def test_ledger_reconciliation_rejects_cross_identity_and_incomplete_or_false_usage(damage):
    trial, trial_id, events, rows = _case()
    if damage == "missing":
        rows.pop()
    elif damage == "duplicate":
        rows.append(rows[-1])
    elif damage == "trial":
        rows[-1]["trial_id"] = str(uuid4())
    elif damage == "step":
        rows[-1]["step_id"] = "verifier"
    elif damage == "model":
        rows[-1]["model"] = "other"
    else:
        rows[-1]["input_tokens"] += 1
    with pytest.raises(ValueError):
        reconcile_terminus_ledger(events, rows, trial, trial_id)


def test_safe_export_preserves_failed_retry_usage_without_provider_logs():
    row = LlmCall(
        id=uuid4(),
        trial_id=uuid4(),
        step_id="agent",
        model="glm-5.2",
        dialect="openai_facade",
        input_tokens=4,
        output_tokens=2,
        cost_usd=Decimal("0.01"),
        rate_card_hash="rate",
        captured_at=datetime.now(UTC),
        attempt=2,
        provider_extras={
            "reasoning_tokens": 2,
            "_loom_call_status": "failed",
            "private_debug": "secret",
            "nested": {"credential": "secret"},
            "_loom_raw_provider_log": {
                "request": {"headers": {"Authorization": "secret"}},
                "response": {"body": {"choices": [{"finish_reason": "length"}]}},
            },
        },
    )
    safe = serialize_execution_accounting_call(row)
    assert "secret" not in json.dumps(safe)
    assert safe["finish_reason"] == "length" and safe["call_status"] == "failed"
    event = llm_call_row_to_event(safe, trial_id=row.trial_id, seq=0)
    assert event.attempt == 2 and event.output_tokens == 2
    assert event.thinking_tokens == 2


@pytest.mark.parametrize("route,exported", [
    ("native_responses", True), ("chat_translation", True), ("private-debug", False),
])
def test_safe_export_keeps_only_the_bounded_gateway_route(route, exported):
    # Codex evidence must say which Responses route served each call (#2054).
    row = LlmCall(
        id=uuid4(), trial_id=uuid4(), step_id="agent", model="gpt-4.1-mini",
        dialect="openai_responses", input_tokens=4, output_tokens=2, cost_usd=Decimal("0"),
        rate_card_hash="rate", captured_at=datetime.now(UTC), attempt=1,
        provider_extras={"_loom_gateway_route": route},
    )
    safe = serialize_execution_accounting_call(row)
    assert ("_loom_gateway_route" in safe["provider_extras"]) is exported
    # Typed events keep only numeric counters; the route stays ledger evidence.
    assert llm_call_row_to_event(safe, trial_id=row.trial_id, seq=0).provider_extras == {}


def test_unknown_usage_survives_typed_events_usage_and_atif_without_private_extras():
    trial, trial_id, _native, rows = _case()
    failed = {
        **rows[0], "input_tokens": 0, "output_tokens": 0, "cost_usd": 0,
        "provider_extras": {
            "_loom_call_status": "failed", "_loom_usage_status": "missing",
            "_loom_failure_category": "upstream_timeout", "private": "secret",
        },
    }
    partial = {**rows[1], "provider_extras": {"_loom_usage_status": "partial"}}
    events = reconcile_terminus_ledger([], [failed, partial], trial, trial_id)
    # Exercise the durable JSON boundary, not just live Python attributes.
    restored = [LLMCallEvent.model_validate_json(event.model_dump_json()) for event in events]
    assert restored[0].call_status == "failed"
    assert restored[0].usage_status == "missing"
    assert restored[0].failure_category == "upstream_timeout"
    assert restored[0].provider_extras == {}
    assert restored[1].call_status == "completed" and restored[1].usage_status == "partial"
    usage = terminus_usage(restored, trial)
    assert usage["failed_call_count"] == usage["missing_usage_call_count"] == 1
    assert usage["partial_usage_call_count"] == 1
    assert usage["totals"]["input_tokens"] == partial["input_tokens"]
    from loom.models.trajectory import TrialEndEvent, TrialStartEvent
    canonical = [
        TrialStartEvent(trial_id=trial_id, step_id="agent", seq=0, emitted_at=datetime.now(UTC),
                        task_id="task", agent_name="terminus-2", agent_mode="in-box"),
        *restored,
        TrialEndEvent(trial_id=trial_id, step_id="agent", seq=3, emitted_at=datetime.now(UTC),
                      final_state="failed"),
    ]
    for agent_name in ("terminus-2", "generic"):
        atif = json.loads(build_canonical_atif(canonical, task_id="task",
                                               agent_name=agent_name, agent_version="2"))
        diagnostics = atif["accounting"] if agent_name == "terminus-2" else atif["steps"][0]["metrics"]
        assert diagnostics["failed_call_count"] == diagnostics["missing_usage_call_count"] == 1
        assert diagnostics["partial_usage_call_count"] == 1
        assert "secret" not in json.dumps(atif)


def test_projection_rejects_unbounded_status_strings_without_relaxing_counter_contract():
    trial, trial_id, _events, rows = _case()
    row = {**rows[0], "provider_extras": {
        "_loom_usage_status": "private-debug", "_loom_failure_category": "private-debug",
        "reasoning_tokens": 2,
    }}
    event = llm_call_row_to_event(row, trial_id=trial_id, seq=0)
    assert event.call_status == "completed" and event.usage_status is None
    assert event.failure_category is None and event.provider_extras == {"reasoning_tokens": 2}
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LLMCallEvent.model_validate({**event.model_dump(), "usage_status": "private-debug"})
    with pytest.raises(ValidationError):
        LLMCallEvent.model_validate({**event.model_dump(), "provider_extras": {"string": "secret"}})
    # Existing healthy source usage documents retain the old shape.
    assert "missing_usage_call_count" not in terminus_usage([event], trial)


@pytest.mark.parametrize("identity", [
    {"provider": "anthropic"}, {"name": "another-model"}, {"source": "local-server"},
])
def test_terminus_model_identity_still_rejects_other_provider_model_or_source(identity):
    trial, trial_id, native, rows = _case()
    trial = trial.model_copy(update={"agent_model": trial.agent_model.model_copy(update={
        **identity, "max_input_tokens": 16384, "max_output_tokens": 256,
    })})
    with pytest.raises(ValueError, match="another model identity"):
        parse_terminus_events(
            b"\n".join(event.model_dump_json().encode() for event in native),
            trial=trial, trial_id=trial_id,
        )
    with pytest.raises(ValueError, match="another model identity"):
        reconcile_terminus_ledger(native, rows, trial, trial_id)


@pytest.mark.parametrize("multi_model", [
    {
        "enabled": True,
        "policy": "student_teacher_student",
        "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
        "switch_episode": 2,
        "return_switch_episode": 3,
        "teacher_episodes": 1,
    },
    {
        "enabled": True,
        "policy": "beta_mixture",
        "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
        "beta": 0.6,
        "mix_seed": "seed",
    },
    {
        "enabled": True,
        "policy": "student_to_teacher_turns",
        "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
        "step_start": 2,
        "step_end": 9,
        "mix_seed": "seed",
    },
])
def test_terminus_mix_accepts_teacher_ledger_identity(multi_model: dict) -> None:
    trial, trial_id, native, rows = _case()
    trial = TrialConfig.model_validate({
        **trial.model_dump(mode="json"),
        "multi_model": multi_model,
    })
    teacher_row = {
        **rows[1],
        "id": str(uuid4()),
        "model": "teacher",
    }
    teacher_event = llm_call_row_to_event(teacher_row, trial_id=trial_id, seq=1)
    events = parse_terminus_events(
        b"\n".join(event.model_dump_json().encode() for event in (*native[:1], teacher_event)),
        trial=trial,
        trial_id=trial_id,
    )
    assert events[1].model.name == "teacher"
    reconciled = reconcile_terminus_ledger([events[0]], [teacher_row], trial, trial_id)
    assert any(isinstance(event, LLMCallEvent) and event.model.name == "teacher" for event in reconciled)
