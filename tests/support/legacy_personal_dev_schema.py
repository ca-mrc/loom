"""Test-only models for personal-dev records at published pre-retirement revisions.

They use separate metadata and must never enter the application's ORM registry.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import (
    UUID as PgUUID,  # noqa: N811  (UUID is a type, not a constant)
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from loom.db.schema import Team, User


class Base(DeclarativeBase):
    metadata = MetaData()


Team.__table__.to_metadata(Base.metadata)
User.__table__.to_metadata(Base.metadata)


def _personal_storage_binding_check(environment_name: str) -> str:
    return (
        "(storage_binding IS NULL AND storage_binding_sha256 IS NULL) OR (("
        "storage_binding IS NOT NULL AND storage_binding_sha256 ~ '^[0-9a-f]{64}$' "
        "AND storage_binding_sha256 <> repeat('0', 64) "
        "AND storage_binding->>'schema_version' = '1' "
        "AND subject_id <> '00000000-0000-0000-0000-000000000000'::uuid "
        "AND subject_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid "
        "AND owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid "
        "AND owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid "
        f"AND {environment_name} NOT IN ('dev', 'development', 'staging', 'production', "
        "'prod', 'local', 'loom', 'shared', 'default') "
        "AND storage_binding = jsonb_build_object('schema_version', 1, "
        f"'layout', 'incarnation-v1', 'environment_name', {environment_name}, "
        "'subject_id', subject_id::text, 'subject_incarnation', subject_incarnation::text, "
        "'owner_user_id', owner_user_id::text, 'owner_team_id', owner_team_id::text)) IS TRUE)"
    )


class DevInstance(Base):
    """Durable lifecycle authority for one derived ``dev-<name>`` environment."""

    __tablename__ = "dev_instances"
    __table_args__ = (
        CheckConstraint(
            _personal_storage_binding_check("name"), name="dev_instances_storage_binding_check"
        ),
        CheckConstraint(
            "name ~ '^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$'",
            name="dev_instances_name_check",
        ),
        CheckConstraint(
            "status IN ('provisioning', 'ready', 'updating', 'activating', "
            "'deleting', 'draining', 'failed', 'deleted')",
            name="dev_instances_status_check",
        ),
        CheckConstraint(
            "min_slots >= 0 AND max_slots >= min_slots AND max_slots <= 8",
            name="dev_instances_slots_check",
        ),
        CheckConstraint(
            "deployment_generation > 0",
            name="dev_instances_deployment_generation_check",
        ),
        CheckConstraint(
            "(candidate_id IS NULL AND candidate_sha ~ '^[0-9a-f]{40}$') OR "
            "(candidate_id IS NOT NULL AND candidate_sha ~ '^[0-9a-f]{64}$')",
            name="dev_instances_candidate_sha_check",
        ),
        CheckConstraint(
            "operation_epoch > 0",
            name="dev_instances_operation_epoch_check",
        ),
        CheckConstraint(
            "accepted_capacity_mode IN ('shadow-v1', 'membership-v1') AND ("
            "(accepted_capacity_mode = 'shadow-v1' "
            "AND accepted_capacity_membership_checkpoint IS NULL) OR (("
            "accepted_capacity_mode = 'membership-v1' "
            "AND accepted_capacity_membership_checkpoint IS NOT NULL "
            "AND jsonb_typeof(accepted_capacity_membership_checkpoint) = 'object' "
            "AND accepted_capacity_membership_checkpoint->>'schema_version' = '1' "
            "AND jsonb_typeof(accepted_capacity_membership_checkpoint->'execution') = 'object' "
            "AND accepted_capacity_membership_checkpoint->>'namespace_id' IS NOT NULL "
            "AND jsonb_typeof(accepted_capacity_membership_checkpoint->'revision') = 'number' "
            "AND accepted_capacity_membership_checkpoint->>'head_sha256' "
            "~ '^[0-9a-f]{64}$' "
            "AND capacity_reporter_incarnation IS NOT NULL "
            "AND capacity_reporter_token_sha256 IS NOT NULL "
            "AND local_activation_sha256 IS NOT NULL "
            "AND protected_admission_sha256 IS NOT NULL "
            "AND capacity_agent_installation_sha256 IS NOT NULL "
            "AND capacity_supported_pool_ids IS NOT NULL "
            "AND capacity_supported_architectures IS NOT NULL) IS TRUE))",
            name="dev_instances_accepted_capacity_mode_check",
        ),
        CheckConstraint(
            "(capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL "
            "AND capacity_reporter_incarnation IS NULL "
            "AND capacity_reporter_token_sha256 IS NULL "
            "AND local_activation_sha256 IS NULL "
            "AND protected_admission_sha256 IS NULL "
            "AND capacity_agent_installation_sha256 IS NULL "
            "AND capacity_supported_pool_ids IS NULL "
            "AND capacity_supported_architectures IS NULL) OR ("
            "capacity_reporter_incarnation IS NOT NULL "
            "AND capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
            "AND capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
            "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
            "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
            "AND jsonb_array_length(capacity_supported_architectures) > 0 "
            "AND ((accepted_capacity_mode = 'shadow-v1' "
            "AND capacity_configuration_epoch > 0 "
            "AND capacity_configuration_sha256 ~ '^[0-9a-f]{64}$') OR ("
            "accepted_capacity_mode = 'membership-v1' "
            "AND capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL)))",
            name="dev_instances_capacity_projection_check",
        ),
        CheckConstraint(
            "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
            "OR (candidate_id IS NOT NULL AND capacity_namespace IS NOT NULL "
            "AND capacity_database IS NOT NULL "
            "AND capacity_namespace = 'loom-dev-' || name "
            "AND capacity_database = CASE WHEN storage_binding IS NULL THEN "
            "'loom_dev_' || replace(name, '-', '_') ELSE "
            "'ld_' || replace(name, '-', '_') || '_' || replace(subject_incarnation::text, '-', '') END)",
            name="dev_instances_personal_capacity_identity_check",
        ),
        CheckConstraint(
            "status <> 'ready' OR candidate_id IS NULL OR ("
            "(accepted_capacity_mode = 'shadow-v1' "
            "AND capacity_configuration_epoch IS NOT NULL) OR ("
            "accepted_capacity_mode = 'membership-v1' "
            "AND accepted_capacity_membership_checkpoint IS NOT NULL))",
            name="dev_instances_personal_readiness_capacity_check",
        ),
        UniqueConstraint("subject_id", name="dev_instances_subject_id_uidx"),
        Index("dev_instances_owner_status_idx", "owner_user_id", "status"),
        Index("dev_instances_team_status_idx", "owner_team_id", "status"),
    )

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    subject_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
        server_default=text("gen_random_uuid()"),
    )
    subject_incarnation: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
        server_default=text("gen_random_uuid()"),
    )
    owner_user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    owner_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="RESTRICT"),
        nullable=False,
    )
    min_slots: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'provisioning'"),
    )
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    candidate_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("personal_dev_candidates.id", ondelete="RESTRICT"),
        nullable=True,
    )
    candidate_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    capacity_namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    capacity_database: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_binding: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    storage_binding_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("1"),
    )
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_step: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text("'claimed'"),
    )
    accepted_capacity_mode: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shadow-v1'"),
        default="shadow-v1",
    )
    accepted_capacity_membership_checkpoint: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    capacity_configuration_epoch: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    capacity_configuration_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    capacity_reporter_incarnation: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=True,
    )
    capacity_reporter_token_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    local_activation_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    protected_admission_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    capacity_agent_installation_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    capacity_supported_pool_ids: Mapped[list[str] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    capacity_supported_architectures: Mapped[list[str] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    keep_data: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    failure_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    ready_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class PersonalDevCandidate(Base):
    """Immutable, owner-scoped source identity and build status for personal dev."""

    __tablename__ = "personal_dev_candidates"
    __table_args__ = (
        CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{64}$' AND "
            "source_sha256 ~ '^[0-9a-f]{64}$' AND "
            "archive_sha256 ~ '^[0-9a-f]{64}$' AND "
            "build_contract_sha256 ~ '^[0-9a-f]{64}$'",
            name="personal_dev_candidates_digests_check",
        ),
        CheckConstraint(
            "source_commit ~ '^[0-9a-f]{40}$'",
            name="personal_dev_candidates_source_commit_check",
        ),
        CheckConstraint(
            "archive_size_bytes > 0",
            name="personal_dev_candidates_archive_size_check",
        ),
        CheckConstraint(
            "object_bucket <> '' AND object_bucket = btrim(object_bucket) "
            "AND position('/' in object_bucket) = 0 AND "
            "((source_generation_id = id AND "
            "object_key = 'personal-dev/sources/' || owner_team_id::text || '/' || "
            "owner_user_id::text || '/' || candidate_sha || '/' || "
            "archive_sha256 || '.tar') OR "
            "object_key = 'personal-dev/sources/' || owner_team_id::text || '/' || "
            "owner_user_id::text || '/' || candidate_sha || '/' || "
            "source_generation_id::text || '/' || archive_sha256 || '.tar')",
            name="personal_dev_candidates_object_binding_check",
        ),
        CheckConstraint(
            "status IN ('uploaded', 'queued', 'building', 'ready', 'failed')",
            name="personal_dev_candidates_status_check",
        ),
        CheckConstraint(
            "registry_prefix IS NULL OR ("
            "length(registry_prefix) BETWEEN 1 AND 309 "
            "AND registry_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*$' "
            "AND right(registry_prefix, 1) NOT IN ('/', ':') "
            "AND position('://' in registry_prefix) = 0 "
            "AND position('@' in registry_prefix) = 0)",
            name="personal_dev_candidates_registry_prefix_check",
        ),
        CheckConstraint(
            "artifact_gc_lease_epoch >= 0 "
            "AND (artifact_gc_blocked_reason IS NULL OR "
            "artifact_gc_blocked_reason IN ("
            "'manifest_authority_invalid', 'registry_authority_unavailable')) AND ("
            "(artifact_state = 'retained' "
            "AND artifact_gc_claimed_by IS NULL "
            "AND artifact_gc_lease_expires_at IS NULL "
            "AND artifact_gc_manifest_json IS NULL "
            "AND artifact_gc_manifest_sha256 IS NULL "
            "AND artifact_collected_at IS NULL) OR ("
            "artifact_state = 'collecting' "
            "AND artifact_gc_blocked_reason IS NULL "
            "AND artifact_gc_unreferenced_at IS NOT NULL "
            "AND artifact_gc_claimed_by IS NOT NULL "
            "AND artifact_gc_lease_expires_at IS NOT NULL "
            "AND artifact_gc_manifest_json IS NOT NULL "
            "AND jsonb_typeof(artifact_gc_manifest_json) = 'object' "
            "AND artifact_gc_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_collected_at IS NULL) OR ("
            "artifact_state = 'collected' "
            "AND artifact_gc_blocked_reason IS NULL "
            "AND artifact_gc_unreferenced_at IS NOT NULL "
            "AND artifact_gc_claimed_by IS NULL "
            "AND artifact_gc_lease_expires_at IS NULL "
            "AND artifact_gc_manifest_json IS NOT NULL "
            "AND jsonb_typeof(artifact_gc_manifest_json) = 'object' "
            "AND artifact_gc_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_collected_at IS NOT NULL))",
            name="personal_dev_candidates_artifact_gc_check",
        ),
        CheckConstraint(
            "artifact_gc_manifest_json IS NULL OR (("
            "jsonb_typeof(artifact_gc_manifest_json) = 'object' "
            "AND artifact_gc_manifest_json->>'schema_version' = '1' "
            "AND artifact_gc_manifest_json->>'candidate_id' = id::text "
            "AND artifact_gc_manifest_json->>'owner_user_id' = owner_user_id::text "
            "AND artifact_gc_manifest_json->>'owner_team_id' = owner_team_id::text "
            "AND artifact_gc_manifest_json->>'candidate_sha' = candidate_sha "
            "AND artifact_gc_manifest_json->>'object_bucket' = object_bucket "
            "AND artifact_gc_manifest_json->>'source_generation_id' = "
            "source_generation_id::text "
            "AND artifact_gc_manifest_json->>'source_object_key' = object_key) IS TRUE)",
            name="personal_dev_candidates_artifact_manifest_binding_check",
        ),
        CheckConstraint(
            "(status IN ('uploaded', 'queued', 'building') "
            "AND image_manifest_digest IS NULL "
            "AND publication_json IS NULL AND publication_sha256 IS NULL "
            "AND failure_reason IS NULL AND ready_at IS NULL) OR "
            "(status = 'ready' AND image_manifest_digest IS NOT NULL "
            "AND image_manifest_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND publication_json IS NOT NULL "
            "AND publication_sha256 IS NOT NULL "
            "AND publication_sha256 ~ '^[0-9a-f]{64}$' "
            "AND failure_reason IS NULL AND ready_at IS NOT NULL) OR "
            "(status = 'failed' AND image_manifest_digest IS NULL "
            "AND publication_json IS NULL AND publication_sha256 IS NULL "
            "AND failure_reason IS NOT NULL AND ready_at IS NULL)",
            name="personal_dev_candidates_terminal_fields_check",
        ),
        UniqueConstraint(
            "owner_user_id",
            "owner_team_id",
            "source_sha256",
            "archive_sha256",
            "build_contract_sha256",
            name="personal_dev_candidates_owner_source_uidx",
        ),
        Index(
            "personal_dev_candidates_owner_created_idx",
            "owner_user_id",
            "created_at",
            "id",
        ),
        Index(
            "personal_dev_candidates_status_created_idx",
            "status",
            "created_at",
            "id",
        ),
        Index(
            "personal_dev_candidates_artifact_gc_idx",
            "artifact_state",
            "artifact_gc_unreferenced_at",
            "artifact_gc_lease_expires_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    owner_user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    owner_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="RESTRICT"),
        nullable=False,
    )
    candidate_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    archive_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    build_contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_commit: Mapped[str] = mapped_column(String(40), nullable=False)
    dirty: Mapped[bool] = mapped_column(Boolean, nullable=False)
    manifest_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    object_bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    source_generation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    archive_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'uploaded'"),
    )
    image_manifest_digest: Mapped[str | None] = mapped_column(String(71), nullable=True)
    publication_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    publication_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    registry_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    artifact_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        server_default=text("'retained'"),
    )
    artifact_gc_lease_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    artifact_gc_unreferenced_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    artifact_gc_claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    artifact_gc_blocked_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    artifact_gc_lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    artifact_gc_manifest_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    artifact_gc_manifest_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    artifact_collected_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    ready_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
