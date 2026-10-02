from __future__ import annotations

from datetime import UTC, datetime, timedelta

from loom.service_execution_phases import (
    PhaseCost,
    PhaseLease,
    execution_phases,
    reserved_cpu_seconds,
)

_T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _lease(role: str, *, start: int, deleted: int | None, mode: str = "separate_execution", **contract: object) -> PhaseLease:
    return PhaseLease(
        lease_id=f"{role}-lease",
        execution_role=role,
        observed_state="deleted" if deleted is not None else "running",
        created_at=_T0 + timedelta(seconds=start),
        pod_started_at=_T0 + timedelta(seconds=start + 5),
        pod_terminated_at=_T0 + timedelta(seconds=deleted - 2) if deleted is not None else None,
        deleted_at=_T0 + timedelta(seconds=deleted) if deleted is not None else None,
        runtime_contract={"verifier_execution": mode, **contract},
    )


def _cost(cpu: int) -> PhaseCost:
    return PhaseCost(
        requested_cpu_millis=cpu, requested_memory_mib=1024, requested_ephemeral_storage_mib=2048,
        estimated_cost_microusd=100, actual_allocated_microusd=None, state="reserved",
    )


def test_shared_attempt_is_a_single_agent_phase() -> None:
    phases = execution_phases(
        [_lease("attempt", start=0, deleted=100, mode="in_attempt")],
        verifier_execution=None, costs={"attempt-lease": _cost(2000)}, now=_T0 + timedelta(hours=1),
    )
    assert phases is not None
    assert [item["phase"] for item in phases["phases"]] == ["agent"]
    assert phases["phases"][0]["reserved_seconds"] == 100.0
    assert phases["phases"][0]["requested"]["cpu_millis"] == 2000
    assert phases["reservation_overlap_seconds"] is None
    assert phases["reserved_seconds"] == 100.0


def test_separate_attempt_shows_the_unreserved_wait_before_its_verifier() -> None:
    phases = execution_phases(
        [_lease("attempt", start=0, deleted=100)],
        verifier_execution={"state": "pending", "parent_lease_id": "attempt-lease"},
        costs={}, now=_T0 + timedelta(seconds=130),
    )
    assert phases is not None
    assert [item["phase"] for item in phases["phases"]] == ["agent", "awaiting_verifier"]
    waiting = phases["phases"][1]
    assert waiting["state"] == "pending"
    assert waiting["reserved_seconds"] == 0.0
    assert waiting["finished_at"] is None
    assert phases["verifier_state"] == "pending"


def test_separate_attempt_reports_gap_overlap_and_handoff_storage() -> None:
    verifier = _lease(
        "verifier", start=110, deleted=170,
        handoff_input={"manifest_sha256": "sha256:" + "a" * 64, "file_count": 3, "total_bytes": 4096},
    )
    phases = execution_phases(
        [verifier, _lease("attempt", start=0, deleted=100)],
        verifier_execution={"state": "committed", "lease_id": "verifier-lease"},
        costs={"attempt-lease": _cost(2000), "verifier-lease": _cost(500)},
        now=_T0 + timedelta(hours=1),
    )
    assert phases is not None
    assert [item["phase"] for item in phases["phases"]] == ["agent", "awaiting_verifier", "verifier"]
    assert phases["phases"][1]["state"] == "complete"
    assert phases["phases"][2]["requested"]["cpu_millis"] == 500
    # Agent pod ended at 98s; verifier pod started at 115s.
    assert phases["handoff_gap_seconds"] == 17.0
    assert phases["reservation_overlap_seconds"] == 0.0
    assert phases["handoff_storage_bytes"] == 4096
    assert phases["reserved_seconds"] == 160.0


def test_on_demand_verifier_reserves_less_than_holding_the_agent_pod_through_grading() -> None:
    """Before/after measurement for one modeled trial.

    Agent runs 600s; grading runs 120s; the verifier lease waits 30s for capacity.
    A colocated pod requests 2.2 CPU (controller + task + verifier sandboxes) for
    the whole trial. On demand, the agent pod drops the verifier sandbox
    (1.2 CPU) and the verifier pod requests 1.2 CPU only while grading.
    """

    now = _T0 + timedelta(hours=2)
    holding = execution_phases(
        [_lease("attempt", start=0, deleted=600 + 120, mode="in_attempt")],
        verifier_execution=None, costs={"attempt-lease": _cost(2200)}, now=now,
    )
    on_demand = execution_phases(
        [_lease("attempt", start=0, deleted=600), _lease("verifier", start=630, deleted=630 + 120)],
        verifier_execution={"state": "committed"},
        costs={"attempt-lease": _cost(1200), "verifier-lease": _cost(1200)},
        now=now,
    )
    assert holding is not None and on_demand is not None
    assert reserved_cpu_seconds(holding) == 1584.0
    assert reserved_cpu_seconds(on_demand) == 864.0
    assert on_demand["reservation_overlap_seconds"] == 0.0
    # Wall time grows by the wait; reserved capacity shrinks by the grading delta.
    assert on_demand["phases"][1]["reserved_seconds"] == 0.0


def test_retried_verifier_reports_every_try_and_counts_its_capacity() -> None:
    first = _lease("verifier", start=110, deleted=140)
    retry = PhaseLease(**{
        **first.__dict__, "lease_id": "verifier-retry", "verifier_retry": 1,
        "created_at": _T0 + timedelta(seconds=150), "pod_started_at": _T0 + timedelta(seconds=155),
        "pod_terminated_at": _T0 + timedelta(seconds=208), "deleted_at": _T0 + timedelta(seconds=210),
    })
    phases = execution_phases(
        [retry, _lease("attempt", start=0, deleted=100), first],
        verifier_execution={"state": "committed", "lease_id": "verifier-retry", "retries": 1},
        costs={"attempt-lease": _cost(2000), "verifier-lease": _cost(500), "verifier-retry": _cost(500)},
        now=_T0 + timedelta(hours=1),
    )
    assert phases is not None
    assert [(item["phase"], item["retry"]) for item in phases["phases"]] == [
        ("agent", 0), ("awaiting_verifier", 0), ("verifier", 0), ("verifier", 1),
    ]
    # The wait ends when the first verifier is reserved; both tries held capacity.
    assert phases["phases"][1]["finished_at"] == (_T0 + timedelta(seconds=110)).isoformat()
    assert phases["handoff_gap_seconds"] == 17.0
    assert reserved_cpu_seconds(phases) == 200.0 + 15.0 + 30.0


def test_no_attempt_lease_omits_phases() -> None:
    assert execution_phases([], verifier_execution=None, costs={}, now=_T0) is None
