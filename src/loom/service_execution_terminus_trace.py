"""Typed Harbor trace accounting shared by execution and canonical projection."""

from __future__ import annotations

import math
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.models.trajectory import LLMCallEvent, TrajectoryEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.llm_call_events import llm_call_diagnostic_counts, llm_call_row_to_event

_EVENT: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
_COUNTERS = (
    "input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "thinking_tokens",
)


def _same_model_identity(actual: ModelSpec, expected: ModelSpec | None) -> bool:
    # Gateway accounting records model identity, not the Trial's generation
    # limits. Retain source/routing checks without requiring those limits to
    # round-trip through every native trace and ledger projection.
    limits = {"max_input_tokens", "max_output_tokens"}
    return expected is not None and actual.model_dump(exclude=limits) == expected.model_dump(exclude=limits)


def parse_terminus_events(
    body: bytes | None, *, trial: TrialConfig, trial_id: UUID | None = None,
) -> list[TrajectoryEvent]:
    events: list[TrajectoryEvent] = []
    call_ids: set[str] = set()
    for line in (body or b"").splitlines():
        event = _EVENT.validate_json(line)
        if event.seq != len(events) or event.step_id != "agent":
            raise ValueError("Terminus trace order or step identity is invalid")
        if trial_id is None:
            trial_id = event.trial_id
        if event.trial_id != trial_id:
            raise ValueError("Terminus trace has another Trial identity")
        if not (event.kind.startswith("terminus2_") or isinstance(event, LLMCallEvent)):
            raise ValueError("Terminus source cannot author lifecycle events")
        if isinstance(event, LLMCallEvent):
            if not _same_model_identity(event.model, trial.agent_model):
                raise ValueError("Terminus trace has another model identity")
            if not event.gateway_request_id or event.gateway_request_id in call_ids:
                raise ValueError("Terminus trace has missing or duplicate Gateway calls")
            call_ids.add(event.gateway_request_id)
        events.append(event)
    return events


def terminus_usage(events: list[TrajectoryEvent], trial: TrialConfig) -> dict[str, Any]:
    calls = [event for event in events if isinstance(event, LLMCallEvent)]
    return {
        "schema_version": "loom.service-execution-terminus-usage.v1",
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


def matches_terminus_usage(document: Any, expected: dict[str, Any]) -> bool:
    """Accept historical float summation roundoff, never identity/token drift.

    Older runtimes used builtin sum, whose algorithm differs across Python
    versions. Bound compatibility by one ULP per addition, with no fixed dollar
    tolerance. New producers use fsum; retained source documents stay untouched.
    """
    if not isinstance(document, dict) or not isinstance(document.get("totals"), dict):
        return False
    totals = dict(document["totals"])
    for name in ("cost_usd", "duration_sec"):
        actual, target = totals.get(name), expected["totals"][name]
        if (isinstance(actual, bool) or not isinstance(actual, (int, float))
                or not math.isfinite(actual) or not math.isfinite(target)
                or actual < 0 or target < 0):
            return False
        bound = max(math.ulp(actual), math.ulp(target)) * max(1, expected["call_count"])
        if abs(actual - target) > bound:
            return False
        totals[name] = target
    return {**document, "totals": totals} == expected


def reconcile_terminus_ledger(
    events: list[TrajectoryEvent], rows: list[dict[str, Any]],
    trial: TrialConfig, trial_id: UUID,
) -> list[TrajectoryEvent]:
    """Complete request accounting without manufacturing Harbor turns/commands.

    Harbor may discard a truncated response or retry internally. The Gateway
    ledger owns request count and usage; native events own the accepted turns.
    Unmatched requests remain synthetic LLMCallEvents with no invented content.
    Callers must read rows through the tenant/lease/generation-scoped query.
    """
    calls: dict[str, LLMCallEvent] = {}
    for row in rows:
        call_id = str(row.get("id") or "")
        if (not call_id or call_id in calls or row.get("trial_id") != str(trial_id)
                or row.get("step_id") != "agent"):
            raise ValueError("Gateway ledger has invalid or duplicate call identity")
        call = llm_call_row_to_event(row, trial_id=trial_id, seq=0)
        if not _same_model_identity(call.model, trial.agent_model):
            raise ValueError("Gateway ledger has another model identity")
        if row.get("finish_reason"):
            call = call.model_copy(update={"finish_reason": row["finish_reason"]})
        calls[call_id] = call
    linked: set[str] = set()
    for event in events:
        if event.trial_id != trial_id or event.step_id != "agent":
            raise ValueError("Terminus trace has another Trial or step identity")
        if isinstance(event, LLMCallEvent):
            matched = calls.get(event.gateway_request_id)
            if matched is None or event.gateway_request_id in linked:
                raise ValueError("Terminus trace call is absent or duplicated in Gateway ledger")
            if (event.input_tokens, event.output_tokens) != (matched.input_tokens, matched.output_tokens):
                raise ValueError("Terminus trace tokens differ from Gateway ledger")
            linked.add(event.gateway_request_id)
    # Accounting events stand independently of Harbor's accepted-step sequence.
    # Keep all native messages, turns, commands and observations untouched.
    result: list[TrajectoryEvent] = list(calls.values())
    result.extend(event for event in events if not isinstance(event, LLMCallEvent))
    return [event.model_copy(update={"seq": seq}) for seq, event in enumerate(result)]
