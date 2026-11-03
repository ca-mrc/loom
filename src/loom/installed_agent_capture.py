"""Ledger validation shared by installed CLI harnesses; CLI totals are never authoritative."""
from __future__ import annotations

import math
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.models.trajectory import AgentThoughtEvent, LLMCallEvent, ToolUseEvent, TrajectoryEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.llm_call_events import llm_call_diagnostic_counts, llm_call_row_to_event

_EVENT: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
_COUNTERS = ("input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "thinking_tokens")
_CANONICAL_KINDS = (ToolUseEvent, AgentThoughtEvent, LLMCallEvent)


def _same_model(actual: ModelSpec, expected: ModelSpec | None) -> bool:
    limits = {"max_input_tokens", "max_output_tokens"}
    return expected is not None and actual.model_dump(exclude=limits) == expected.model_dump(exclude=limits)


def ledger_calls(rows: list[dict[str, Any]], *, trial: TrialConfig, trial_id: UUID) -> list[LLMCallEvent]:
    """Gateway ledger rows as call events; another model fails the Trial."""
    calls: list[LLMCallEvent] = []
    seen: set[str] = set()
    for row in rows:
        call_id = str(row.get("id") or "")
        if not call_id or call_id in seen or row.get("trial_id") != str(trial_id) or row.get("step_id") != "agent":
            raise ValueError("Gateway ledger has invalid or duplicate call identity")
        seen.add(call_id)
        call = llm_call_row_to_event(row, trial_id=trial_id, seq=0)
        if not _same_model(call.model, trial.agent_model):
            raise ValueError("Gateway ledger has another model identity")
        calls.append(call)
    return calls


def installed_agent_usage(events: list[TrajectoryEvent], trial: TrialConfig, *, schema: str) -> dict[str, Any]:
    calls = [event for event in events if isinstance(event, LLMCallEvent)]
    return {
        "schema_version": schema,
        "model": trial.agent_model.to_gateway_model_string() if trial.agent_model else None,
        "call_count": len(calls),
        **llm_call_diagnostic_counts(calls),
        "gateway_request_ids": [call.gateway_request_id for call in calls],
        "totals": {
            **{name: sum(getattr(call, name) for call in calls) for name in _COUNTERS},
            "cost_usd": math.fsum(call.cost_usd_snapshot for call in calls),
            "duration_sec": math.fsum(call.duration_sec for call in calls),
        },
    }


def parse_installed_agent_events(
    body: bytes | None, *, trial: TrialConfig, trial_id: UUID | None = None, label: str = "Installed agent",
) -> list[TrajectoryEvent]:
    """Validate an installed-agent trace: ordered, one Trial, own model only."""
    events: list[TrajectoryEvent] = []
    call_ids: set[str] = set()
    for line in (body or b"").splitlines():
        event = _EVENT.validate_json(line)
        if not isinstance(event, _CANONICAL_KINDS):
            raise ValueError(f"{label} trace may only record tool use, thoughts and model calls")
        if event.seq != len(events) or event.step_id != "agent":
            raise ValueError(f"{label} trace order or step identity is invalid")
        if trial_id is None:
            trial_id = event.trial_id
        if event.trial_id != trial_id:
            raise ValueError(f"{label} trace has another Trial identity")
        if isinstance(event, LLMCallEvent):
            if not _same_model(event.model, trial.agent_model):
                raise ValueError(f"{label} trace has another model identity")
            if not event.gateway_request_id or event.gateway_request_id in call_ids:
                raise ValueError(f"{label} trace has missing or duplicate Gateway calls")
            call_ids.add(event.gateway_request_id)
        events.append(event)
    return events
