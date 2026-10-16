"""Separate-verifier handoff is required only after a succeeded attempt (#2521)."""

from __future__ import annotations

from types import SimpleNamespace

from loom_control_plane.service_execution_materializer import requires_deferred_verifier_handoff


def _lease(*, role: str = "attempt", verifier_execution: str = "separate_execution") -> SimpleNamespace:
    return SimpleNamespace(
        execution_role=role,
        runtime_contract_json={
            "execution_role": role,
            "verifier_execution": verifier_execution,
        },
    )


def test_succeeded_separate_attempt_without_handoff_is_missing() -> None:
    assert requires_deferred_verifier_handoff(
        _lease(),
        {"runtime_result": {"status": "succeeded"}},
    )


def test_timed_out_separate_attempt_without_handoff_is_causal() -> None:
    assert not requires_deferred_verifier_handoff(
        _lease(),
        {"runtime_result": {"status": "timed_out"}},
    )


def test_failed_and_cancelled_separate_attempts_without_handoff_are_causal() -> None:
    assert not requires_deferred_verifier_handoff(
        _lease(),
        {"runtime_result": {"status": "failed"}},
    )
    assert not requires_deferred_verifier_handoff(
        _lease(),
        {"runtime_result": {"status": "cancelled"}},
    )


def test_existing_handoff_dict_is_not_treated_as_missing() -> None:
    assert not requires_deferred_verifier_handoff(
        _lease(),
        {
            "runtime_result": {"status": "succeeded"},
            "verifier_execution": {"state": "pending"},
        },
    )


def test_in_attempt_never_requires_deferred_handoff() -> None:
    assert not requires_deferred_verifier_handoff(
        _lease(verifier_execution="in_attempt"),
        {"runtime_result": {"status": "succeeded"}},
    )
