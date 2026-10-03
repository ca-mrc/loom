"""Management-owned build intent and retained per-attempt source/recipe inputs."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, Index, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplicationBuild(Base):
    __tablename__ = "nebius_application_builds"
    __table_args__ = (
        UniqueConstraint("owner_user_id", "idempotency_key", name="nebius_application_build_replay_key"),
        Index("nebius_application_build_owner_idx", "owner_user_id", "owner_team_id"),
        CheckConstraint("current_attempt > 0 AND desired_state IN ('running','cancelled')",
            name="nebius_application_build_state_check"),
        CheckConstraint("idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$' AND request_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(binding_json) = 'object'", name="nebius_application_build_request_check"),
    )
    build_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    upload_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_application_source_uploads.upload_id", ondelete="RESTRICT"), nullable=False)
    owner_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    owner_team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False)
    installation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    data_environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    binding_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    current_attempt: Mapped[int] = mapped_column(BigInteger, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NebiusApplicationBuildAttempt(Base):
    __tablename__ = "nebius_application_build_attempts"
    __table_args__ = (
        CheckConstraint("attempt > 0 AND phase IN ('queued','running','settling','ready','failed','cancelled')",
            name="nebius_application_build_attempt_state_check"),
        CheckConstraint("runner_epoch >= 0 AND ((lease_token IS NULL) = (lease_expires_at IS NULL)) AND "
            "(lease_token IS NULL OR runner_epoch > 0)", name="nebius_application_build_lease_check"),
        CheckConstraint("(phase <> 'running' OR activated_json IS NOT NULL) AND "
            "(phase <> 'settling' OR settlement_json IS NOT NULL) AND "
            "(phase NOT IN ('ready','failed','cancelled') OR lease_token IS NULL) AND "
            "(phase NOT IN ('ready','failed','cancelled') OR pool_request_json IS NULL OR terminal_receipt_json IS NOT NULL) AND "
            "(phase <> 'ready' OR COALESCE(terminal_receipt_json->>'phase' = 'released' AND "
            "settlement_json->>'outcome' = 'ready' AND jsonb_typeof(settlement_json->'publication') = 'object', false))",
            name="nebius_application_build_completion_check"),
        CheckConstraint("claim_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(claim_json) = 'object' AND "
            "COALESCE(claim_json->>'build_id' = build_id::text AND (claim_json->>'attempt')::bigint = attempt, false)",
            name="nebius_application_build_attempt_input_check"),
        CheckConstraint("(pool_request_json IS NULL AND pool_request_sha256 IS NULL) OR "
            "(pool_request_json IS NOT NULL AND pool_request_sha256 IS NOT NULL AND "
            "jsonb_typeof(pool_request_json) = 'object' AND pool_request_sha256 ~ '^[0-9a-f]{64}$')",
            name="nebius_application_build_pool_request_check"),
    )
    build_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_application_builds.build_id", ondelete="RESTRICT"), primary_key=True)
    attempt: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    claim_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    claim_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    pool_request_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    pool_request_sha256: Mapped[str | None] = mapped_column(Text)
    runner_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    lease_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    grant_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    activation_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    activated_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    settlement_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    terminal_receipt_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
