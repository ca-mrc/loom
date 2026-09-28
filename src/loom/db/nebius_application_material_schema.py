"""Atomic operation-to-ciphertext references; never plaintext credentials."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplicationMaterial(Base):
    __tablename__ = "nebius_application_material"
    __table_args__ = (UniqueConstraint("secret_ref", name="nebius_application_material_ref_key"),)

    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey(
        "nebius_application_operations.operation_id", ondelete="RESTRICT"), primary_key=True)
    secret_ref: Mapped[str] = mapped_column(Text, ForeignKey("secrets.ref", ondelete="RESTRICT"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
