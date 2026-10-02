"""Stable user-facing lifecycle projection for durable service execution."""

from __future__ import annotations

from typing import Any

from sqlalchemy import case

from loom.db.schema import ServiceExecutionLease, Trial

SERVICE_EXECUTION_LIFECYCLE_STAGES = (
    "queued",
    "admission_blocked",
    "provisioning",
    "running",
    "verifying",
    "materializing",
    "succeeded",
    "failed",
    "cancelled",
    "output_unavailable",
)


def service_execution_lifecycle_stage(
    *,
    trial_state: str,
    observed_state: str,
    output_commit_state: str,
    materialization_state: str,
    error_code: str | None,
    error_class: str | None,
) -> str:
    # Cancellation can finish before any output exists; availability is a separate axis.
    if trial_state == "cancelled":
        return "cancelled"
    if output_commit_state == "unavailable" or materialization_state == "unavailable":
        return "output_unavailable"
    if materialization_state in {"pending", "running"}:
        return "materializing"
    if trial_state == "succeeded":
        return "succeeded"
    if trial_state == "failed":
        return "failed"
    if error_code == "unschedulable" or error_class == "policy":
        return "admission_blocked"
    if output_commit_state == "uploading" or observed_state in {"finalizing", "finalized"}:
        return "verifying"
    # A separate-mode attempt released its pod and awaits or runs its verifier lease.
    if output_commit_state == "committed" and observed_state in {"delete_pending", "deleted"}:
        return "verifying"
    if observed_state == "running":
        return "running"
    if observed_state in {"creating", "created", "starting"}:
        return "provisioning"
    if observed_state in {"cancelled", "timed_out", "failed"}:
        return "failed"
    return "queued"


def service_execution_lifecycle_case() -> Any:
    """SQL equivalent of :func:`service_execution_lifecycle_stage`."""
    return case(
        (Trial.state == "cancelled", "cancelled"),
        (
            (ServiceExecutionLease.output_commit_state == "unavailable")
            | (ServiceExecutionLease.materialization_state == "unavailable"),
            "output_unavailable",
        ),
        (
            ServiceExecutionLease.materialization_state.in_(("pending", "running")),
            "materializing",
        ),
        (Trial.state == "succeeded", "succeeded"),
        (Trial.state == "failed", "failed"),
        (
            (ServiceExecutionLease.error_code == "unschedulable")
            | (ServiceExecutionLease.error_class == "policy"),
            "admission_blocked",
        ),
        (
            (ServiceExecutionLease.output_commit_state == "uploading")
            | ServiceExecutionLease.observed_state.in_(("finalizing", "finalized")),
            "verifying",
        ),
        (
            (ServiceExecutionLease.output_commit_state == "committed")
            & ServiceExecutionLease.observed_state.in_(("delete_pending", "deleted")),
            "verifying",
        ),
        (ServiceExecutionLease.observed_state == "running", "running"),
        (
            ServiceExecutionLease.observed_state.in_(("creating", "created", "starting")),
            "provisioning",
        ),
        (
            ServiceExecutionLease.observed_state.in_(("cancelled", "timed_out", "failed")),
            "failed",
        ),
        else_="queued",
    )


__all__ = [
    "SERVICE_EXECUTION_LIFECYCLE_STAGES",
    "service_execution_lifecycle_case",
    "service_execution_lifecycle_stage",
]
