"""Pure rules for drain cancellation, retention, and verifier admission."""

from __future__ import annotations

from sqlalchemy.dialects import postgresql

from loom.batch_cancellation import (
    reservation_allowed,
    revocation_desired_state,
    trial_is_retained,
)


def test_running_batch_admits_attempts_and_verifiers() -> None:
    assert reservation_allowed(
        "running", "attempt", retained=False, cancellation_requested=False, handoff_pending=False,
    )
    assert reservation_allowed(
        "submitted", "verifier", retained=False, cancellation_requested=False, handoff_pending=False,
    )


def test_cancelling_batch_admits_only_a_retained_pending_verifier() -> None:
    assert reservation_allowed(
        "cancelling", "verifier", retained=True, cancellation_requested=False, handoff_pending=True,
    )
    assert not reservation_allowed(
        "cancelling", "attempt", retained=True, cancellation_requested=False, handoff_pending=True,
    )
    assert not reservation_allowed(
        "cancelling", "verifier", retained=False, cancellation_requested=False, handoff_pending=True,
    )
    assert not reservation_allowed(
        "cancelling", "verifier", retained=True, cancellation_requested=True, handoff_pending=True,
    )
    assert not reservation_allowed(
        "cancelling", "verifier", retained=True, cancellation_requested=False, handoff_pending=False,
    )


def test_cancelled_batch_admits_nothing() -> None:
    assert not reservation_allowed(
        "cancelled", "verifier", retained=True, cancellation_requested=False, handoff_pending=True,
    )


def test_uncommitted_finalize_keeps_the_cancellation_request_durable() -> None:
    assert revocation_desired_state("finalize", "uploading") is None
    assert revocation_desired_state("finalize", "committed") == "delete_pending"
    assert revocation_desired_state("start", "not_started") == "cancel"
    assert revocation_desired_state("delete_pending", "committed") is None


def test_retention_predicate_compiles() -> None:
    compiled = str(
        trial_is_retained().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "verifier_execution" in compiled
    assert "uploading" in compiled
    assert "execution_role" in compiled
