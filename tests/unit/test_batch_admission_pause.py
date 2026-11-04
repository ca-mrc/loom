"""Compiled admission-pause gates and the never-started drain selection (#2777)."""

from __future__ import annotations

from sqlalchemy.dialects import postgresql

from loom_control_plane.batch_admission import paused_unstarted_attempts
from loom_control_plane.execution_capacity_wait import _candidate_rows
from loom_control_plane.retry_exhausted_sweeper import _SWEEP_SQL
from loom_control_plane.service_execution_scheduler import _SERVICE_TRIAL_SQL


def _sql(statement: object) -> str:
    return str(statement.compile(  # type: ignore[attr-defined]
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True},
    ))


def test_attempt_candidates_exclude_paused_batches_and_refund_released_attempts() -> None:
    assert "AND b.admission_paused_at IS NULL" in _SERVICE_TRIAL_SQL
    assert "t.attempt_count - t.admission_pause_releases < q.max_attempts_ceiling" in _SERVICE_TRIAL_SQL


def test_capacity_wait_heads_exclude_paused_batches() -> None:
    compiled = _sql(_candidate_rows())
    assert "batches.admission_paused_at IS NULL" in compiled
    assert "trials.attempt_count - trials.admission_pause_releases" in compiled


def test_retry_exhausted_sweeper_excludes_pause_releases() -> None:
    assert "t.attempt_count - t.admission_pause_releases >= q.max_attempts_ceiling" in str(_SWEEP_SQL)


def test_drain_selects_only_never_scheduled_unrevoked_local_attempts() -> None:
    compiled = _sql(paused_unstarted_attempts(limit=7))
    for fragment in (
        "batches.admission_paused_at IS NOT NULL",
        "batches.state IN ('submitted', 'running')",
        "execution_leases.execution_role = 'attempt'",
        "execution_leases.desired_state = 'create'",
        "execution_leases.revoked_at IS NULL",
        "execution_leases.pod_scheduled_at IS NULL",
        "execution_leases.pod_started_at IS NULL",
        "execution_leases.node_name IS NULL",
        "execution_leases.output_commit_state = 'not_started'",
        "trials.state = 'claimed'",
        "trials.started_at IS NULL",
        "trials.cancellation_requested_at IS NULL",
        "trials.attempt_count = execution_leases.attempt",
        "nebius_pool_execution_outbox.lease_id = execution_leases.id",
        "LIMIT 7",
    ):
        assert fragment in compiled, fragment
