"""Per-phase view of one hosted attempt: agent, awaiting verifier, verifier.

A separate-mode attempt runs as two execution leases. Accounting, readback and
the UI read this one projection so they agree on phase boundaries, the handoff
gap, any reservation overlap and the size of the handed-off workspace.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

PhaseName = Literal["agent", "awaiting_verifier", "verifier"]


@dataclass(frozen=True)
class PhaseLease:
    lease_id: str
    execution_role: str
    observed_state: str
    created_at: datetime
    pod_started_at: datetime | None
    pod_terminated_at: datetime | None
    deleted_at: datetime | None
    runtime_contract: Mapping[str, Any] | None
    verifier_retry: int = 0


@dataclass(frozen=True)
class PhaseCost:
    requested_cpu_millis: int
    requested_memory_mib: int
    requested_ephemeral_storage_mib: int
    estimated_cost_microusd: int
    actual_allocated_microusd: int | None
    state: str


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds())


def _reserved_until(lease: PhaseLease, now: datetime) -> datetime:
    # Capacity is held from reservation until the pod's resources are deleted.
    return lease.deleted_at or now


def _lease_phase(name: PhaseName, lease: PhaseLease, cost: PhaseCost | None, now: datetime) -> dict[str, Any]:
    return {
        "phase": name,
        "retry": lease.verifier_retry,
        "lease_id": lease.lease_id,
        "state": lease.observed_state,
        "reserved_at": lease.created_at.isoformat(),
        "started_at": _iso(lease.pod_started_at),
        "finished_at": _iso(lease.pod_terminated_at),
        "released_at": _iso(lease.deleted_at),
        "reserved_seconds": _seconds(lease.created_at, _reserved_until(lease, now)),
        "requested": (
            {
                "cpu_millis": cost.requested_cpu_millis,
                "memory_mib": cost.requested_memory_mib,
                "ephemeral_storage_mib": cost.requested_ephemeral_storage_mib,
            }
            if cost is not None
            else None
        ),
        "estimated_cost_microusd": cost.estimated_cost_microusd if cost is not None else None,
        "allocated_cost_microusd": cost.actual_allocated_microusd if cost is not None else None,
        "cost_state": cost.state if cost is not None else None,
    }


def reserved_cpu_seconds(phases: Mapping[str, Any]) -> float:
    """Reserved CPU-seconds across phases; the measure #2212 reduces."""

    return sum(
        float(item["reserved_seconds"] or 0.0) * int(item["requested"]["cpu_millis"]) / 1000
        for item in phases["phases"]
        if item["requested"] is not None
    )


def execution_phases(
    leases: Sequence[PhaseLease],
    *,
    verifier_execution: Mapping[str, Any] | None,
    costs: Mapping[str, PhaseCost],
    now: datetime,
) -> dict[str, Any] | None:
    """Project the latest attempt's leases into ordered phases.

    ``leases`` must belong to one attempt. Returns ``None`` when there is no
    attempt lease, so trials that never reached hosted execution omit it.
    """

    agent = next((item for item in leases if item.execution_role == "attempt"), None)
    if agent is None:
        return None
    # A natively failed verifier is retried on a new lease; every try held capacity.
    verifiers = sorted(
        (item for item in leases if item.execution_role == "verifier"),
        key=lambda item: item.verifier_retry,
    )
    first = verifiers[0] if verifiers else None
    verifier = verifiers[-1] if verifiers else None
    mode = (agent.runtime_contract or {}).get("verifier_execution")
    phases = [_lease_phase("agent", agent, costs.get(agent.lease_id), now)]
    handoff = verifier_execution if mode == "separate_execution" else None
    if handoff is not None:
        waiting_since = agent.pod_terminated_at or agent.deleted_at
        waiting_until = first.created_at if first is not None else None
        phases.append({
            "phase": "awaiting_verifier",
            "retry": 0,
            "lease_id": None,
            "state": "complete" if first is not None else str(handoff.get("state") or "pending"),
            "reserved_at": None,
            "started_at": _iso(waiting_since),
            "finished_at": _iso(waiting_until),
            "released_at": None,
            # Nothing is reserved while waiting; this is the price of on-demand grading.
            "reserved_seconds": 0.0,
            "requested": None,
            "estimated_cost_microusd": None,
            "allocated_cost_microusd": None,
            "cost_state": None,
        })
    phases.extend(_lease_phase("verifier", item, costs.get(item.lease_id), now) for item in verifiers)

    handoff_bytes = None
    if verifier is not None:
        handoff_input = (verifier.runtime_contract or {}).get("handoff_input")
        if isinstance(handoff_input, Mapping):
            handoff_bytes = handoff_input.get("total_bytes")
    overlap = None
    gap = None
    if first is not None:
        overlap_start = max(agent.created_at, first.created_at)
        overlap_end = min(_reserved_until(agent, now), _reserved_until(first, now))
        overlap = max(0.0, (overlap_end - overlap_start).total_seconds())
        gap = _seconds(agent.pod_terminated_at, first.pod_started_at)
    return {
        "schema_version": "loom.service-execution-phases.v1",
        "verifier_execution": mode,
        "verifier_state": (handoff or {}).get("state") if handoff is not None else None,
        "phases": phases,
        "handoff_gap_seconds": gap,
        "reservation_overlap_seconds": overlap,
        "handoff_storage_bytes": handoff_bytes,
        "reserved_seconds": sum(item["reserved_seconds"] or 0.0 for item in phases),
    }
