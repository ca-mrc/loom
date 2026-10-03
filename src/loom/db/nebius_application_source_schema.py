"""Durable owner grants to verified shared source bytes, not application builds."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, Index, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplicationSourceUpload(Base):
    __tablename__ = "nebius_application_source_uploads"
    __table_args__ = (
        UniqueConstraint("owner_user_id", "idempotency_key", name="nebius_application_source_replay_key"),
        Index("nebius_application_source_owner_idx", "owner_user_id", "owner_team_id"),
        CheckConstraint(" AND ".join(f"{name} <> '00000000-0000-0000-0000-000000000000'::uuid" for name in (
            "upload_id", "owner_user_id", "owner_team_id", "installation_id", "data_environment_id",
        )), name="nebius_application_source_identity_check"),
        CheckConstraint("cluster_id ~ '^[a-zA-Z0-9_-]{1,128}$' AND "
            "source_bucket ~ '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$' AND "
            "idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$' AND request_sha256 ~ '^[0-9a-f]{64}$'",
            name="nebius_application_source_binding_check"),
        CheckConstraint("source_digest ~ '^sha256:[0-9a-f]{64}$' AND archive_sha256 ~ '^[0-9a-f]{64}$' AND "
            "archive_size_bytes BETWEEN 10240 AND 570870784 AND archive_size_bytes % 10240 = 0 AND "
            "(base_commit IS NULL OR base_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$') AND "
            "object_key = 'application-sources/v1/sha256/' || archive_sha256 || '.tar'",
            name="nebius_application_source_content_check"),
        CheckConstraint("phase IN ('awaiting_source','source_verified') AND expires_at > created_at AND "
            "expires_at <= created_at + interval '1 hour' AND "
            "((phase = 'source_verified') = (verified_at IS NOT NULL)) AND "
            "(verified_at IS NULL OR (verified_at >= created_at AND verified_at < expires_at))",
            name="nebius_application_source_phase_check"),
    )
    upload_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    owner_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    owner_team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False)
    installation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    data_environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    source_digest: Mapped[str] = mapped_column(Text, nullable=False)
    archive_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    archive_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    base_commit: Mapped[str | None] = mapped_column(Text)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
