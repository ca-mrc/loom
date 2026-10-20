"""Scoped admission state and immutable protected operation receipts."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, ForeignKeyConstraint, Text, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusPoolLifecycleOperation(Base):
    __tablename__ = 'nebius_pool_lifecycle_operations'
    __table_args__ = (
        CheckConstraint("operation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object' AND "
            "jsonb_typeof(receipt_json) = 'object'", name='nebius_pool_lifecycle_operation_shape_check'),
    )
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey('nebius_pool_bindings.pool_id', ondelete='RESTRICT'), nullable=False)
    participant_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    receipt_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class NebiusPoolParticipantLifecycle(Base):
    __tablename__ = 'nebius_pool_participant_lifecycle'
    __table_args__ = (
        ForeignKeyConstraint(['participant_id', 'pool_id'],
            ['nebius_pool_participants.participant_id', 'nebius_pool_participants.pool_id'], ondelete='RESTRICT'),
        CheckConstraint("generation > 0 AND state IN ('enrolled','ready','active','suspended','retiring','retired') "
            "AND binding_sha256 ~ '^[0-9a-f]{64}$'", name='nebius_pool_participant_lifecycle_shape_check'),
    )
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    head_operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey('nebius_pool_lifecycle_operations.operation_id', ondelete='RESTRICT'), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    binding_sha256: Mapped[str] = mapped_column(Text, nullable=False)
