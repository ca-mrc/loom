"""Immutable participant releases, separate from physical platform authority."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, ForeignKeyConstraint, Text, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusPoolParticipantCatalog(Base):
    __tablename__ = 'nebius_pool_participant_catalogs'
    __table_args__ = (
        ForeignKeyConstraint(['participant_id', 'pool_id'],
            ['nebius_pool_participants.participant_id', 'nebius_pool_participants.pool_id'], ondelete='RESTRICT'),
        CheckConstraint("binding_revision > 0 AND jsonb_typeof(document_json) = 'object' AND "
            "document_sha256 ~ '^[0-9a-f]{64}$'", name='nebius_pool_participant_catalog_shape_check'),
    )
    participant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    binding_revision: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    document_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    document_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class NebiusPoolProfileDocument(Base):
    __tablename__ = 'nebius_pool_profile_documents'
    __table_args__ = (
        CheckConstraint("family IN ('execution','task_images','application_images') AND "
            "profile_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "jsonb_typeof(document_json) = 'object' AND document_sha256 ~ '^[0-9a-f]{64}$'",
            name='nebius_pool_profile_document_shape_check'),
    )
    pool_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey('nebius_pool_bindings.pool_id', ondelete='RESTRICT'), primary_key=True)
    profile_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    family: Mapped[str] = mapped_column(Text, primary_key=True)
    document_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    document_sha256: Mapped[str] = mapped_column(Text, nullable=False)

