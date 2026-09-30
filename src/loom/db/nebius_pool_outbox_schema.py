"""Environment-local handoff journals, without foreign keys to management SQL."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusPoolSubmission(Base):
    """Server-written direct-trial provenance; the HTTP ID alone authorizes nothing."""

    __tablename__ = "nebius_pool_submissions"
    __table_args__ = (
        CheckConstraint("id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(pool_origin) = 'object' AND "
            "(pool_origin->>'submission_id' = id::text AND "
            "pool_origin->>'kind' IN ('environment','application')) IS TRUE",
            name="nebius_pool_submissions_payload_check"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False)
    user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    pool_origin: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NebiusPoolExecutionOutbox(Base):
    """Pre-claim execution identity and the exact remote physical grant."""

    __tablename__ = "nebius_pool_execution_outbox"
    __table_args__ = (
        UniqueConstraint("reservation_id", name="nebius_pool_execution_outbox_grant_key"),
        Index("nebius_pool_execution_outbox_live_key", "trial_id", unique=True,
              postgresql_where=text("phase NOT IN ('cancelled','released')")),
        CheckConstraint("lease_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json)='object' AND "
            "jsonb_typeof(selection_json)='object'", name="nebius_pool_execution_outbox_identity_check"),
        CheckConstraint("phase IN ('selected','grant_pending','attached','activation_pending','active','stop_pending','cancel_pending','cancelled','released') AND "
            "(phase <> 'selected' OR reservation_id IS NULL) AND "
            "(phase NOT IN ('grant_pending','attached','cancelled') OR reservation_id IS NOT NULL) AND "
            "(reservation_id IS NULL) = (receipt_json IS NULL) AND "
            "(reservation_id IS NULL OR reservation_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND "
            "(receipt_json IS NULL OR jsonb_typeof(receipt_json)='object') AND "
            "(attached_lease_id IS NULL OR attached_lease_id=lease_id) AND "
            "(phase NOT IN ('attached','activation_pending','active','stop_pending','released') OR attached_lease_id IS NOT NULL) AND "
            "(phase NOT IN ('selected','grant_pending') OR attached_lease_id IS NULL) AND "
            "(attached_lease_id IS NULL OR reservation_id IS NOT NULL) AND "
            "(activation_json IS NULL OR (jsonb_typeof(activation_json)='object' AND attached_lease_id IS NOT NULL AND "
            "phase NOT IN ('selected','grant_pending','attached'))) AND "
            "(phase NOT IN ('activation_pending','active','stop_pending','released') OR activation_json IS NOT NULL) AND "
            "(phase IN ('active','stop_pending','released')) = (activated_json IS NOT NULL) AND "
            "(activated_json IS NULL OR jsonb_typeof(activated_json)='object') AND "
            "(phase='cancelled') = (cancelled_json IS NOT NULL) AND "
            "(cancelled_json IS NULL OR jsonb_typeof(cancelled_json)='object') AND "
            "(stop_json IS NULL OR (phase IN ('stop_pending','released') AND jsonb_typeof(stop_json)='object')) AND "
            "(drain_json IS NULL) = (output_json IS NULL) AND "
            "(drain_json IS NULL OR (stop_json IS NOT NULL AND jsonb_typeof(drain_json)='object' AND "
            "jsonb_typeof(output_json)='object')) AND "
            "(phase='released') = (released_json IS NOT NULL) AND "
            "(released_json IS NULL OR (drain_json IS NOT NULL AND jsonb_typeof(released_json)='object'))",
            name="nebius_pool_execution_outbox_phase_check"),
    )
    lease_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    trial_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("trials.id", ondelete="RESTRICT"), nullable=False)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    selection_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    reservation_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    receipt_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    attached_lease_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("execution_leases.id", ondelete="RESTRICT"))
    cancelled_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    activation_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    activated_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    stop_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    drain_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    output_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    released_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NebiusPoolBuildOutbox(Base):
    """A durable selection is not a claimed attempt or permission to write a Job."""

    __tablename__ = "nebius_pool_build_outbox"
    __table_args__ = (
        UniqueConstraint("participant_id", "materialization_id", "generation", name="nebius_pool_build_outbox_replay_key"),
        UniqueConstraint("reservation_id", name="nebius_pool_build_outbox_grant_key"),
        Index("nebius_pool_build_outbox_live_key", "materialization_id", unique=True,
              postgresql_where=text("phase NOT IN ('cancelled','released')")),
        ForeignKeyConstraint(["materialization_id"], ["task_image_materializations.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(["attempt_id", "materialization_id", "attempt_number", "lease_epoch", "builder_id"],
            ["task_image_materialization_attempts.id", "task_image_materialization_attempts.materialization_id",
             "task_image_materialization_attempts.attempt_number", "task_image_materialization_attempts.lease_epoch",
             "task_image_materialization_attempts.builder_id"], ondelete="RESTRICT", name="nebius_pool_build_outbox_claim_fk"),
        CheckConstraint("generation > 0 AND admission_epoch > 0 AND "
            "outbox_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "length(builder_id) BETWEEN 1 AND 128 AND length(logical_pool_id) BETWEEN 1 AND 80",
            name="nebius_pool_build_outbox_identity_check"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object' AND "
            "jsonb_typeof(selection_json) = 'object'", name="nebius_pool_build_outbox_payload_check"),
        CheckConstraint("phase IN ('selected','attached','activation_pending','active','cancel_pending','stop_pending','cancelled','released') AND "
            "(phase NOT IN ('attached','activation_pending','active','stop_pending','released') OR num_nonnulls(attempt_id, attempt_number, lease_epoch) = 3) AND "
            "(phase <> 'selected' OR attempt_id IS NULL) AND "
            "(attempt_id IS NULL OR reservation_id IS NOT NULL) AND "
            "num_nonnulls(attempt_id, attempt_number, lease_epoch) IN (0,3) AND "
            "(phase NOT IN ('attached','cancelled') OR reservation_id IS NOT NULL) AND "
            "(phase <> 'selected' OR reservation_id IS NULL) AND "
            "(reservation_id IS NULL) = (receipt_json IS NULL) AND "
            "(reservation_id IS NULL OR reservation_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND "
            "(receipt_json IS NULL OR jsonb_typeof(receipt_json) = 'object') AND "
            "(phase = 'cancelled') = (cancelled_json IS NOT NULL) AND "
            "(cancelled_json IS NULL OR jsonb_typeof(cancelled_json) = 'object') AND "
            "(activation_json IS NULL OR (jsonb_typeof(activation_json) = 'object' AND attempt_id IS NOT NULL AND "
            "phase NOT IN ('selected','attached'))) AND "
            "(phase NOT IN ('activation_pending','active','stop_pending','released') OR activation_json IS NOT NULL) AND "
            "(phase IN ('active','stop_pending','released')) = (activated_json IS NOT NULL) AND "
            "(activated_json IS NULL OR jsonb_typeof(activated_json) = 'object') AND "
            "(phase = 'released') = (released_json IS NOT NULL) AND "
            "(released_json IS NULL OR jsonb_typeof(released_json) = 'object')",
            name="nebius_pool_build_outbox_phase_check"),
    )
    outbox_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    admission_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(Text, nullable=False)
    logical_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    selection_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    reservation_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    receipt_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    cancelled_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    activation_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    activated_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    released_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    attempt_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    attempt_number: Mapped[int | None] = mapped_column(Integer)
    lease_epoch: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
