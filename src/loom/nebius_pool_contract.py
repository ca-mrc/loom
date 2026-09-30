"""Global pool identity contracts; validation alone confers no runtime authority.

Protected registration, credentials and current admission epochs must qualify
these values before use. Target IDs are local to a participant, never global
reservation keys. Runtime profiles and namespace ownership are registry-bound.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

PoolWorkloadKind = Literal["trial", "verifier", "task_image_build", "application_image_build"]
PoolEnvironmentClass = Literal["production", "staging", "development"]
_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]
_NAMESPACE = r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$"
_TARGET = r"^[a-z0-9][a-z0-9-]{0,79}$"
_Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PoolRequestPhase = Literal[
    "reserved", "create_intent", "observed", "cleanup_intent", "released", "cancelled_unstarted",
]
MAX_POOL_REQUEST_BYTES = 1024 * 1024


def _non_nil(*identities: UUID) -> None:
    if any(not identity.int for identity in identities):
        raise ValueError("nil_pool_identity")


class _PoolContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PoolNamespaceBindingV1(_PoolContract):
    name: str = Field(pattern=_NAMESPACE)
    uid: UUID

    @model_validator(mode="after")
    def identity(self) -> PoolNamespaceBindingV1:
        _non_nil(self.uid)
        return self


class PoolTargetBindingV1(_PoolContract):
    target_id: str = Field(pattern=_TARGET)
    profile_id: UUID
    workload_kinds: tuple[PoolWorkloadKind, ...] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def identity(self) -> PoolTargetBindingV1:
        _non_nil(self.profile_id)
        if len(set(self.workload_kinds)) != len(self.workload_kinds):
            raise ValueError("duplicate_pool_workload_kind")
        return self


class PoolParticipantV1(_PoolContract):
    schema_version: Literal["loom.pool-participant.v1"] = "loom.pool-participant.v1"
    participant_id: UUID
    installation_id: UUID
    environment_id: UUID
    environment_class: PoolEnvironmentClass
    incarnation: UUID
    pool_id: UUID
    binding_revision: _Generation
    admission_epoch: _Generation
    execution_namespace: PoolNamespaceBindingV1
    build_namespace: PoolNamespaceBindingV1
    targets: tuple[PoolTargetBindingV1, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def unambiguous_identity(self) -> PoolParticipantV1:
        _non_nil(self.participant_id, self.installation_id, self.environment_id, self.incarnation, self.pool_id)
        if (self.execution_namespace.name == self.build_namespace.name
                or self.execution_namespace.uid == self.build_namespace.uid):
            raise ValueError("duplicate_pool_namespace")
        if len({target.target_id for target in self.targets}) != len(self.targets):
            raise ValueError("duplicate_pool_target")
        return self

    def target(self, target_id: str, workload_kind: PoolWorkloadKind) -> PoolTargetBindingV1:
        for target in self.targets:
            if target.target_id == target_id and workload_kind in target.workload_kinds:
                return target
        raise ValueError("target_workload_unavailable")


class PoolRequestKeyV1(_PoolContract):
    schema_version: Literal["loom.pool-request-key.v1"] = "loom.pool-request-key.v1"
    participant_id: UUID
    workload_kind: PoolWorkloadKind
    local_work_id: UUID
    generation: _Generation

    @model_validator(mode="after")
    def identity(self) -> PoolRequestKeyV1:
        _non_nil(self.participant_id, self.local_work_id)
        return self

    def storage_key(self) -> tuple[UUID, PoolWorkloadKind, UUID, int]:
        return self.participant_id, self.workload_kind, self.local_work_id, self.generation


class PoolRequestActionV1(_PoolContract):
    """Reference an existing immutable request; never accept workload/phase authority."""

    schema_version: Literal["loom.pool-request-action.v1"] = "loom.pool-request-action.v1"
    pool_id: UUID
    request_key: PoolRequestKeyV1
    admission_epoch: _Generation
    request_sha256: _Digest

    @model_validator(mode="after")
    def identity(self) -> PoolRequestActionV1:
        _non_nil(self.pool_id)
        return self


class PoolActivationV1(_PoolContract):
    """One retained claim consent; expiry prevents first activation, not release."""

    schema_version: Literal["loom.pool-activation.v1"] = "loom.pool-activation.v1"
    action: PoolRequestActionV1
    not_after: datetime

    @model_validator(mode="after")
    def aware_deadline(self) -> PoolActivationV1:
        if self.not_after.utcoffset() is None:
            raise ValueError("pool_activation_requires_absolute_deadline")
        object.__setattr__(self, "not_after", self.not_after.astimezone(UTC))
        return self


class PoolWaitingV1(_PoolContract):
    """No reservation identity: this demand has acquired no capacity."""

    schema_version: Literal["loom.pool-waiting.v1"] = "loom.pool-waiting.v1"
    phase: Literal["waiting"] = "waiting"
    request_key: PoolRequestKeyV1
    pool_id: UUID
    request_sha256: _Digest
    reason: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def identity(self) -> PoolWaitingV1:
        _non_nil(self.pool_id)
        return self


class PoolReceiptV1(_PoolContract):
    """A durable reservation snapshot, never caller-provided release authority.

    Waiting requests have no reservation receipt. The registry must independently
    qualify cleanup_observation_id against this request, frozen plan, namespace
    and fenced writer before persisting a released receipt. This shape validates
    references and accounting state; it cannot prove actual Kubernetes absence.
    Blocked/unknown transport outcomes do not replace the durable phase.
    """

    schema_version: Literal["loom.pool-receipt.v1"] = "loom.pool-receipt.v1"
    reservation_id: UUID
    pool_id: UUID
    request_key: PoolRequestKeyV1
    admission_epoch: _Generation
    request_sha256: _Digest
    phase: PoolRequestPhase
    plan_sha256: _Digest | None = None
    job_uid: UUID | None = None
    cleanup_observation_id: UUID | None = None

    @model_validator(mode="after")
    def durable_phase(self) -> PoolReceiptV1:
        _non_nil(self.reservation_id, self.pool_id)
        _non_nil(*(value for value in (self.job_uid, self.cleanup_observation_id) if value is not None))
        unstarted = self.phase in {"reserved", "cancelled_unstarted"}
        if unstarted != (self.plan_sha256 is None):
            raise ValueError("pool_intent_phase_mismatch")
        if ((self.phase in {"reserved", "cancelled_unstarted", "create_intent"} and self.job_uid is not None)
                or (self.phase == "observed" and self.job_uid is None)):
            raise ValueError("pool_job_phase_mismatch")
        if (self.phase == "released") != (self.cleanup_observation_id is not None):
            raise ValueError("pool_cleanup_phase_mismatch")
        return self

    @property
    def capacity_charged(self) -> bool:
        return self.phase not in {"released", "cancelled_unstarted"}


def validate_pool_receipt_transition(previous: PoolReceiptV1, following: PoolReceiptV1) -> None:
    """Reject history loss; caller still owns transactional CAS and proof checks."""
    for field in ("reservation_id", "pool_id", "request_key", "admission_epoch", "request_sha256"):
        if getattr(previous, field) != getattr(following, field):
            raise ValueError("pool_receipt_identity_changed")
    for field in ("plan_sha256", "job_uid", "cleanup_observation_id"):
        value = getattr(previous, field)
        if value is not None and value != getattr(following, field):
            raise ValueError("pool_receipt_evidence_changed")
    if previous == following:
        return
    # Cancellation may precede the readback of an uncertain create. Retain the
    # discovered UID while staying charged; it cannot then disappear or change.
    if (previous.phase == following.phase == "cleanup_intent"
            and previous.job_uid is None and following.job_uid is not None):
        return
    edges = {
        ("reserved", "create_intent"), ("reserved", "cancelled_unstarted"),
        ("create_intent", "observed"), ("create_intent", "cleanup_intent"),
        ("observed", "cleanup_intent"), ("cleanup_intent", "released"),
    }
    if (previous.phase, following.phase) not in edges:
        raise ValueError("pool_receipt_transition_forbidden")
