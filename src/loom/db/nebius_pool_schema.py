"""Durable global resource journals; protected registration is separate authority."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    LargeBinary,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusPoolBinding(Base):
    __tablename__ = "nebius_pool_bindings"
    __table_args__ = (
        UniqueConstraint("cluster_id", "node_group_id", name="nebius_pool_physical_identity_key"),
        CheckConstraint("policy_revision > 0 AND admission_epoch > 0 AND mode IN ('legacy','closed','global')",
                        name="nebius_pool_binding_state_check"),
        CheckConstraint("pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "installation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "length(cluster_id) BETWEEN 1 AND 253 AND length(node_group_id) BETWEEN 1 AND 253",
                        name="nebius_pool_binding_identity_check"),
        CheckConstraint("jsonb_typeof(binding_json) = 'object' AND binding_sha256 ~ '^[0-9a-f]{64}$'",
                        name="nebius_pool_binding_payload_check"),
    )
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    installation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    node_group_id: Mapped[str] = mapped_column(Text, nullable=False)
    policy_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    binding_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    binding_sha256: Mapped[str] = mapped_column(Text, nullable=False)


class NebiusPoolParticipant(Base):
    __tablename__ = "nebius_pool_participants"
    __table_args__ = (
        UniqueConstraint("participant_id", "pool_id", name="nebius_pool_participant_pool_key"),
        UniqueConstraint("environment_id", "incarnation", name="nebius_pool_participant_incarnation_key"),
        CheckConstraint("binding_revision > 0 AND admission_epoch > 0 AND phase IN ('active','fenced')",
                        name="nebius_pool_participant_state_check"),
        CheckConstraint("participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "environment_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "incarnation <> '00000000-0000-0000-0000-000000000000'::uuid",
                        name="nebius_pool_participant_identity_check"),
        CheckConstraint("jsonb_typeof(binding_json) = 'object' AND binding_sha256 ~ '^[0-9a-f]{64}$'",
                        name="nebius_pool_participant_payload_check"),
    )
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_pool_bindings.pool_id", ondelete="RESTRICT"), nullable=False)
    environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    binding_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    binding_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    binding_sha256: Mapped[str] = mapped_column(Text, nullable=False)


class NebiusPoolMachine(Base):
    """Protected stable identity; credential rotation cannot widen its scope."""

    __tablename__ = "nebius_pool_machines"
    __table_args__ = (
        ForeignKeyConstraint(["participant_id", "pool_id"],
                             ["nebius_pool_participants.participant_id", "nebius_pool_participants.pool_id"],
                             ondelete="RESTRICT", name="nebius_pool_machine_participant_fk"),
        CheckConstraint("machine_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "credential_epoch > 0 AND phase IN ('active','revoked')",
                        name="nebius_pool_machine_state_check"),
        CheckConstraint("(role = 'participant' AND participant_id IS NOT NULL) OR "
                        "(role IN ('observer','gateway') AND participant_id IS NULL)",
                        name="nebius_pool_machine_role_check"),
        CheckConstraint("workload_scope IN ('environment','application_builder') AND "
                        "(workload_scope = 'environment' OR role = 'participant')",
                        name="nebius_pool_machine_workload_scope_check"),
    )
    machine_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_pool_bindings.pool_id", ondelete="RESTRICT"), nullable=False)
    participant_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    role: Mapped[str] = mapped_column(Text, nullable=False)
    workload_scope: Mapped[str] = mapped_column(Text, nullable=False, server_default="environment")
    credential_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)


class NebiusPoolMachineCredential(Base):
    """Hash-only token binding; old epochs cannot regain machine authority."""

    __tablename__ = "nebius_pool_machine_credentials"
    __table_args__ = (
        CheckConstraint("octet_length(token_hash) = 32 AND credential_epoch > 0",
                        name="nebius_pool_machine_credential_shape_check"),
    )
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, ForeignKey("tokens.token_hash", ondelete="RESTRICT"), primary_key=True)
    machine_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_pool_machines.machine_id", ondelete="RESTRICT"), nullable=False)
    credential_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


class NebiusPoolCancellation(Base):
    """Terminal identity for cancellation before prepare, never a resource claim."""

    __tablename__ = "nebius_pool_cancellations"
    __table_args__ = (
        ForeignKeyConstraint(["participant_id", "pool_id"],
            ["nebius_pool_participants.participant_id", "nebius_pool_participants.pool_id"],
            ondelete="RESTRICT", name="nebius_pool_cancellation_participant_fk"),
        UniqueConstraint("participant_id", "workload_kind", "local_work_id", "generation",
                         name="nebius_pool_cancellation_replay_key"),
        CheckConstraint("generation > 0 AND admission_epoch > 0 AND "
            "cancellation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "local_work_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "workload_kind IN ('trial','verifier','task_image_build','application_image_build') AND "
            "request_sha256 ~ '^[0-9a-f]{64}$'", name="nebius_pool_cancellation_identity_check"),
    )
    cancellation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    workload_kind: Mapped[str] = mapped_column(Text, nullable=False)
    local_work_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NebiusPoolRequest(Base):
    __tablename__ = "nebius_pool_requests"
    __table_args__ = (
        ForeignKeyConstraint(["participant_id", "pool_id"],
                             ["nebius_pool_participants.participant_id", "nebius_pool_participants.pool_id"],
                             ondelete="RESTRICT", name="nebius_pool_request_participant_fk"),
        ForeignKeyConstraint(["cleanup_observation_id", "request_id", "plan_sha256", "namespace_uid"],
                             ["nebius_pool_cleanup_observations.observation_id", "nebius_pool_cleanup_observations.request_id", "nebius_pool_cleanup_observations.plan_sha256", "nebius_pool_cleanup_observations.namespace_uid"],
                             ondelete="RESTRICT", use_alter=True, name="nebius_pool_request_cleanup_fk"),
        UniqueConstraint("participant_id", "workload_kind", "local_work_id", "generation", name="nebius_pool_request_replay_key"),
        UniqueConstraint("request_id", "plan_sha256", "namespace_uid", name="nebius_pool_request_plan_key"),
        CheckConstraint("generation > 0 AND admission_epoch > 0 AND "
                        "request_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "namespace_uid <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "local_work_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "workload_kind IN ('trial','verifier','task_image_build','application_image_build') AND "
                        "target_id ~ '^[a-z0-9][a-z0-9-]{0,79}$'",
                        name="nebius_pool_request_identity_check"),
        CheckConstraint("cpu_millis > 0 AND memory_mib > 0 AND ephemeral_storage_mib >= 0 AND pod_slots > 0",
                        name="nebius_pool_request_envelope_check"),
        CheckConstraint("priority BETWEEN 0 AND 3 AND renewed_at >= created_at AND "
                        "(granted_at IS NULL OR granted_at >= created_at) AND "
                        "(phase <> 'waiting' OR granted_at IS NULL) AND "
                        "(phase IN ('waiting','cancelled_unstarted') OR granted_at IS NOT NULL)",
                        name="nebius_pool_request_admission_check"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object'",
                        name="nebius_pool_request_payload_check"),
        CheckConstraint("phase IN ('waiting','reserved','create_intent','observed','cleanup_intent','released','cancelled_unstarted') AND "
                        "((phase IN ('waiting','reserved','cancelled_unstarted')) = (plan_sha256 IS NULL)) AND "
                        "((plan_sha256 IS NULL) = (plan_json IS NULL)) AND "
                        "(plan_sha256 IS NULL OR (plan_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(plan_json) = 'object'))",
                        name="nebius_pool_request_plan_check"),
        CheckConstraint("(phase NOT IN ('waiting','reserved','cancelled_unstarted','create_intent') OR job_uid IS NULL) AND "
                        "(phase <> 'observed' OR job_uid IS NOT NULL) AND "
                        "(job_uid IS NULL OR job_uid <> '00000000-0000-0000-0000-000000000000'::uuid) AND "
                        "((phase = 'released') = (cleanup_observation_id IS NOT NULL))",
                        name="nebius_pool_request_evidence_check"),
        CheckConstraint("(stop_json IS NULL OR (jsonb_typeof(stop_json) = 'object' AND phase IN ('cleanup_intent','released'))) AND "
                        "(drain_json IS NULL OR (stop_json IS NOT NULL AND jsonb_typeof(drain_json) = 'object'))",
                        name="nebius_pool_request_lifecycle_check"),
    )
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    namespace_uid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    workload_kind: Mapped[str] = mapped_column(Text, nullable=False)
    local_work_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    target_id: Mapped[str] = mapped_column(Text, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ephemeral_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pod_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    plan_sha256: Mapped[str | None] = mapped_column(Text)
    plan_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    stop_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    drain_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    job_uid: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    cleanup_observation_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    renewed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    granted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    priority: Mapped[int] = mapped_column(BigInteger, nullable=False)


class NebiusPoolEffect(Base):
    """Append-once external write authority, bound to an immutable workload plan."""

    __tablename__ = "nebius_pool_effects"
    __table_args__ = (
        ForeignKeyConstraint(["request_id", "plan_sha256", "namespace_uid"],
            ["nebius_pool_requests.request_id", "nebius_pool_requests.plan_sha256", "nebius_pool_requests.namespace_uid"],
            ondelete="RESTRICT", name="nebius_pool_effect_plan_fk"),
        UniqueConstraint("request_id", "effect_key", name="nebius_pool_effect_replay_key"),
        UniqueConstraint("request_id", "sequence", name="nebius_pool_effect_sequence_key"),
        CheckConstraint("effect_id <> '00000000-0000-0000-0000-000000000000'::uuid AND sequence > 0 AND "
                        "effect_key ~ '^[a-zA-Z0-9._:-]{1,128}$'", name="nebius_pool_effect_identity_check"),
        CheckConstraint("phase IN ('prepared','dispatched','observed','rejected') AND jsonb_typeof(intent_json) = 'object' AND "
                        "((intent_json->>'kind' IN ('Job','ConfigMap','Pod')) AND "
                        "(intent_json->>'action' IN ('create','delete')) AND "
                        "(intent_json->>'kind' <> 'Pod' OR intent_json->>'action' = 'delete')) IS TRUE",
                        name="nebius_pool_effect_shape_check"),
        CheckConstraint("(phase = 'prepared') = (dispatch_id IS NULL) AND "
                        "(dispatch_id IS NULL) = (dispatch_machine_id IS NULL) AND "
                        "(dispatch_id IS NULL) = (dispatch_epoch IS NULL) AND "
                        "(dispatch_epoch IS NULL OR dispatch_epoch > 0) AND "
                        "(dispatch_id IS NULL OR dispatch_id <> '00000000-0000-0000-0000-000000000000'::uuid)",
                        name="nebius_pool_effect_dispatch_check"),
        CheckConstraint("(phase = 'observed') = (observed_uid IS NOT NULL) AND "
                        "(observed_uid IS NULL OR observed_uid <> '00000000-0000-0000-0000-000000000000'::uuid) AND "
                        "(observed_resource_version IS NULL OR length(observed_resource_version) BETWEEN 1 AND 253) AND "
                        "(observed_resource_version IS NOT NULL) = (phase = 'observed' AND intent_json->>'action' = 'create')",
                        name="nebius_pool_effect_observation_check"),
        CheckConstraint("(phase = 'rejected') = (rejection_status IS NOT NULL) AND "
                        "(rejection_status IS NULL OR rejection_status IN (409,422))",
                        name="nebius_pool_effect_rejection_check"),
    )
    effect_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    plan_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_uid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    effect_key: Mapped[str] = mapped_column(Text, nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    dispatch_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    dispatch_machine_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_pool_machines.machine_id", ondelete="RESTRICT"))
    dispatch_epoch: Mapped[int | None] = mapped_column(BigInteger)
    observed_uid: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    observed_resource_version: Mapped[str | None] = mapped_column(Text)
    rejection_status: Mapped[int | None] = mapped_column(SmallInteger)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NebiusPoolCleanupObservation(Base):
    """Gateway-qualified absence references, not caller assertions of cleanup."""

    __tablename__ = "nebius_pool_cleanup_observations"
    __table_args__ = (
        UniqueConstraint("observation_id", "request_id", "plan_sha256", "namespace_uid", name="nebius_pool_cleanup_binding_key"),
        ForeignKeyConstraint(["request_id", "plan_sha256", "namespace_uid"],
                             ["nebius_pool_requests.request_id", "nebius_pool_requests.plan_sha256", "nebius_pool_requests.namespace_uid"],
                             ondelete="RESTRICT", name="nebius_pool_cleanup_request_fk"),
        CheckConstraint("writer_epoch > 0 AND observation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "namespace_uid <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "plan_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(evidence_json) = 'object'",
                        name="nebius_pool_cleanup_shape_check"),
    )
    observation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    plan_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_uid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class NebiusPoolCapture(Base):
    """Server-issued physical inventory scope, separate from later reservations."""

    __tablename__ = "nebius_pool_captures"
    __table_args__ = (
        UniqueConstraint("capture_id", "pool_id", name="nebius_pool_capture_pool_key"),
        CheckConstraint("capture_id <> '00000000-0000-0000-0000-000000000000'::uuid AND admission_epoch > 0 AND "
                        "registration_sha256 ~ '^[0-9a-f]{64}$' AND scope_sha256 ~ '^[0-9a-f]{64}$' AND "
                        "jsonb_typeof(scope_json) = 'object'", name="nebius_pool_capture_shape_check"),
    )
    capture_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_pool_bindings.pool_id", ondelete="RESTRICT"), nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    registration_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    scope_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    scope_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.clock_timestamp())


class NebiusPoolObservation(Base):
    """One immutable provider/cluster snapshot for an issued capture scope."""

    __tablename__ = "nebius_pool_observations"
    __table_args__ = (
        ForeignKeyConstraint(["capture_id", "pool_id"], ["nebius_pool_captures.capture_id", "nebius_pool_captures.pool_id"],
                             ondelete="RESTRICT", name="nebius_pool_observation_capture_fk"),
        UniqueConstraint("capture_id", name="nebius_pool_observation_replay_key"),
        CheckConstraint("observation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
                        "observation_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(observation_json) = 'object'",
                        name="nebius_pool_observation_shape_check"),
    )
    observation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    capture_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    observation_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    observation_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
