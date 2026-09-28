"""Application intent journals and app-only platform reservations."""
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
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplicationReservation(Base):
    __tablename__ = "nebius_application_reservations"
    __table_args__ = (
        ForeignKeyConstraint(["application_id", "cluster_id"],
                             ["nebius_applications.application_id", "nebius_applications.cluster_id"],
                             ondelete="RESTRICT", name="nebius_application_reservation_owner_fk"),
        CheckConstraint("cpu_millis >= 0 AND memory_mib >= 0 AND storage_mib = 0 AND ephemeral_storage_mib >= 0",
                        name="nebius_application_reservation_envelope_check"),
    )
    application_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(Text, ForeignKey("nebius_platform_budgets.cluster_id", ondelete="RESTRICT"), nullable=False)
    cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ephemeral_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)


class NebiusApplicationOperation(Base):
    __tablename__ = "nebius_application_operations"
    __table_args__ = (
        UniqueConstraint("owner_user_id", "idempotency_key", name="nebius_application_operation_replay_key"),
        UniqueConstraint("application_id", "deployment_generation", name="nebius_application_operation_generation_key"),
        Index("nebius_application_operation_runnable_idx", "phase", "created_at"),
        CheckConstraint("action IN ('create','update','suspend','resume','destroy_retained')", name="nebius_application_operation_action_check"),
        CheckConstraint("phase IN ('pending','running','blocked','completed','superseded')", name="nebius_application_operation_phase_check"),
        CheckConstraint("deployment_generation > 0 AND access_generation > 0 AND runner_epoch >= 0",
                        name="nebius_application_operation_generation_check"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(plan_json) = 'object'",
                        name="nebius_application_operation_plan_check"),
        CheckConstraint("(phase = 'running') = (lease_token IS NOT NULL) AND (lease_token IS NULL) = (lease_expires_at IS NULL)",
                        name="nebius_application_operation_lease_check"),
        CheckConstraint("(completion_json IS NULL) = (completed_at IS NULL) AND "
                        "(completion_json IS NULL OR (jsonb_typeof(completion_json) = 'object' "
                        "AND phase IN ('completed','superseded')))",
                        name="nebius_application_operation_completion_check"),
    )
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    application_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_applications.application_id", ondelete="RESTRICT"), nullable=False)
    owner_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    access_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[str | None] = mapped_column(Text)
    plan_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    completion_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    runner_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    lease_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
