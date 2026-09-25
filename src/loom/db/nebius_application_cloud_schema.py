"""Immutable IAM request identity, not cloud credentials or runtime readiness."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplicationCloudEffect(Base):
    __tablename__ = "nebius_application_cloud_effects"
    __table_args__ = (
        UniqueConstraint("operation_id", "sequence", name="nebius_application_cloud_sequence_key"),
        CheckConstraint("sequence > 0 AND effect_key ~ '^[a-z][a-z0-9:-]{0,127}$'",
                        name="nebius_application_cloud_key_check"),
        CheckConstraint("phase IN ('prepared','dispatched','observed') AND jsonb_typeof(intent_json) = 'object'",
                        name="nebius_application_cloud_shape_check"),
        CheckConstraint("(phase = 'prepared') = (dispatch_epoch IS NULL) AND (dispatch_epoch IS NULL OR dispatch_epoch > 0)",
                        name="nebius_application_cloud_dispatch_check"),
        CheckConstraint("(phase = 'observed') = (observed_resource_id IS NOT NULL) AND "
                        "(observed_resource_id IS NULL OR observed_resource_id ~ '^[a-zA-Z0-9_-]{1,128}$')",
                        name="nebius_application_cloud_observation_check"),
    )
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey(
        "nebius_application_operations.operation_id", ondelete="RESTRICT"), primary_key=True)
    effect_key: Mapped[str] = mapped_column(Text, primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    dispatch_epoch: Mapped[int | None] = mapped_column(BigInteger)
    observed_resource_id: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
