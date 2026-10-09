"""Bounded image identity from an authorized trial's current execution attempt.

Image hashes always come from a digest-verified frozen plan. A start observation
or matching committed result describes execution evidence, not independent
container imageID inspection. No current application/profile defaults are used.
"""

from collections.abc import Sequence
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError

from loom.db.schema import Artifact, ServiceExecutionLease, Trial
from loom.execution_runtime_contract import ExecutionRuntimePlanV1, ExecutionRuntimeResultV1
from loom.pipeline.keys import canonical_digest


class TrialExecutionProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    state: Literal["unavailable", "planned", "execution_started", "runtime_reported"] = "unavailable"
    image_source: Literal["frozen_runtime_plan"] | None = None
    lease_id: UUID | None = None
    attempt: int | None = None
    resource_generation: int | None = None
    started_at: str | None = None
    candidate_sha: str | None = None
    runtime_contract_sha256: str | None = None
    task_image_digest: str | None = None
    agent_image_digest: str | None = None
    runtime_image_digest: str | None = None
    runtime_binary_sha256: str | None = None


def validated_runtime_plan(lease: ServiceExecutionLease | None) -> ExecutionRuntimePlanV1 | None:
    """Missing, old or damaged evidence stays unknown instead of using defaults."""
    if lease is None or not lease.runtime_contract_json or not lease.runtime_contract_sha256:
        return None
    try:
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    except ValidationError:
        return None
    if (
        canonical_digest(plan.canonical_payload()) != lease.runtime_contract_sha256
        or plan.execution_role != lease.execution_role
        or plan.execution_class_id != lease.execution_class_id
    ):
        return None
    return plan


def _matching_runtime_result(
    trial: Trial,
    lease: ServiceExecutionLease,
    plan: ExecutionRuntimePlanV1,
    artifacts: Sequence[Artifact],
) -> ExecutionRuntimeResultV1 | None:
    result = trial.result
    if (
        not isinstance(result, dict)
        or result.get("schema_version") != "loom.service-execution-trial-result.v1"
        or lease.output_commit_state != "committed"
        or lease.output_generation != lease.resource_generation
        or lease.output_upload_session_id is None
        or not lease.output_manifest_sha256
        or not lease.output_marker_sha256
        or result.get("output_manifest_sha256") != lease.output_manifest_sha256
        or result.get("output_marker_sha256") != lease.output_marker_sha256
    ):
        return None
    # The trial-level result has no attempt identity. Require the immutable
    # output artifact's lease/generation/upload binding before using that result.
    identity = {
        "schema_version": "loom.service-execution-trial-bundle-provenance.v1",
        "lease_id": str(lease.id),
        "generation": lease.resource_generation,
        "runtime_contract_sha256": lease.runtime_contract_sha256,
        "candidate_sha": plan.candidate_sha,
        "task_revision_sha256": plan.task_revision_sha256,
        "command_identity_sha256": plan.command_identity_sha256,
    }
    if not any(
        artifact.trial_id == trial.id
        and artifact.team_id == trial.team_id
        and artifact.control_producer_kind == "service_execution"
        and artifact.control_producer_id == lease.id
        and artifact.artifact_upload_session_id == lease.output_upload_session_id
        and isinstance(artifact.provenance, dict)
        and all(artifact.provenance.get(key) == value for key, value in identity.items())
        for artifact in artifacts
    ):
        return None
    try:
        reported = ExecutionRuntimeResultV1.model_validate(result.get("runtime_result"))
    except ValidationError:
        return None
    if reported.runtime_contract_sha256 != lease.runtime_contract_sha256 or any(
        getattr(reported, field) != getattr(plan, field)
        for field in (
            "candidate_sha", "task_revision_sha256", "command_identity_sha256",
            "execution_role", "execution_class_id", "task_image_ref", "runtime_image_ref",
            "runtime_binary_sha256",
        )
    ):
        return None
    return reported


def trial_execution_provenance(
    trial: Trial,
    lease: ServiceExecutionLease | None,
    *,
    artifacts: Sequence[Artifact] = (),
) -> TrialExecutionProvenance:
    """Caller must authorize the trial; never combine attempts or verifier leases."""
    if (
        lease is None
        or lease.trial_id != trial.id
        or lease.team_id != trial.team_id
        or lease.attempt != trial.attempt_count
        or lease.execution_role != "attempt"
    ):
        return TrialExecutionProvenance()
    plan = validated_runtime_plan(lease)
    if plan is None:
        return TrialExecutionProvenance()
    state: Literal["planned", "execution_started", "runtime_reported"] = "planned"
    started_at = lease.pod_started_at
    if started_at is not None:
        state = "execution_started"
    reported = _matching_runtime_result(trial, lease, plan, artifacts)
    if reported is not None:
        state, started_at = "runtime_reported", reported.started_at
    # Plan validation guarantees immutable digest suffixes. Exclude registry
    # locations entirely: historical image-ref prefixes are not secret-safe.
    return TrialExecutionProvenance(
        state=state, image_source="frozen_runtime_plan", lease_id=lease.id,
        attempt=lease.attempt, resource_generation=lease.resource_generation,
        started_at=started_at.isoformat() if started_at is not None else None,
        candidate_sha=plan.candidate_sha, runtime_contract_sha256=lease.runtime_contract_sha256,
        task_image_digest=plan.task_image_ref.rsplit("@", 1)[1],
        agent_image_digest=plan.agent_image_ref.rsplit("@", 1)[1] if plan.agent_image_ref else None,
        runtime_image_digest=plan.runtime_image_ref.rsplit("@", 1)[1],
        runtime_binary_sha256=plan.runtime_binary_sha256,
    )
