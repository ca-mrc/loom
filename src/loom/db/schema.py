"""SQLAlchemy ORM models for Loom's Postgres state (spec §4.7).

JSONB-typed columns hold Pydantic-serialized payloads; the ORM doesn't
validate the inner shape — that's the responsibility of the application code
that writes the row (which already validates against the Pydantic models).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import INET, JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import (
    UUID as PgUUID,  # noqa: N811  (UUID is a type, not a constant)
)
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base
from loom.db.nebius_application_effect_schema import (
    NebiusApplicationEffect as NebiusApplicationEffect,
)
from loom.db.nebius_application_material_schema import (
    NebiusApplicationMaterial as NebiusApplicationMaterial,
)
from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation as NebiusApplicationOperation,
)
from loom.db.nebius_application_operation_schema import (
    NebiusApplicationReservation as NebiusApplicationReservation,
)
from loom.db.nebius_application_schema import NebiusApplication as NebiusApplication
from loom.db.nebius_application_schema import NebiusDeploymentNameClaim as NebiusDeploymentNameClaim
from loom.db.nebius_environment_schema import NebiusEnvironment as NebiusEnvironment
from loom.db.nebius_environment_schema import (
    NebiusEnvironmentNamespace as NebiusEnvironmentNamespace,
)


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


class Team(Base):
    __tablename__ = "teams"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    disabled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    disabled_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    submissions_paused_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    submissions_paused_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    # Explicit opt-in for public account-request discovery/submission (#775).
    # Default false: internal and smoke Teams stay private unless an admin
    # enables the policy. Never auto-enable via migration or bootstrap.
    public_registration_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )


class TeamQuota(Base):
    __tablename__ = "team_quotas"
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        primary_key=True,
    )
    fair_share_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    # Admin's ceiling: the scheduler stops re-claiming a trial once
    # `attempt_count >= max_attempts_ceiling`. Semantically distinct from
    # TrialConfig.retry.max_attempts (submitter's requested count). See #401.
    max_attempts_ceiling: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
    )
    in_flight_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Legacy per-team SPDX metadata. This field is retained for API/backfill
    # compatibility; license values no longer gate task selection or submit.
    license_allowlist: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        server_default=text(
            "ARRAY['MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0']::text[]",
        ),
    )
    # TaskSet quota columns (#242 sub-plan 7). NULL means "use global
    # default from loom-schema.toml"; non-NULL overrides per-team.
    taskset_max_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    taskset_max_storage_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    # SSRF defense layer 3 opt-in (cluster-deploy.md §Secrets/SSRF).
    # When False (default for `loom cluster`), `POST /provider-connections`
    # rejects RFC1918 / IPv6 ULA / loopback / link-local IPs. When True
    # (default for `loom service` single-box mode, set at bootstrap),
    # RFC1918 + ULA are permitted; loopback + link-local stay rejected
    # unconditionally.
    allow_private_endpoints: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )


class StagingMutationEpoch(Base):
    """Monotonic authority for protected staging mutations."""

    __tablename__ = "staging_mutation_epochs"
    __table_args__ = (
        CheckConstraint(
            "environment = 'staging'",
            name="staging_mutation_epochs_env_check",
        ),
        CheckConstraint(
            "namespace <> ''",
            name="staging_mutation_epochs_namespace_check",
        ),
        CheckConstraint("epoch >= 0", name="staging_mutation_epochs_epoch_check"),
        CheckConstraint(
            "reason IN ('bootstrap','rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epochs_reason_check",
        ),
        CheckConstraint(
            "(reason = 'bootstrap' AND request_id IS NULL AND evidence_sha256 IS NULL) OR "
            "(reason <> 'bootstrap' AND request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' "
            "AND evidence_sha256 ~ '^[0-9a-f]{64}$')",
            name="staging_mutation_epochs_evidence_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    reason: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'bootstrap'"),
        default="bootstrap",
    )
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class StagingMutationEpochEvent(Base):
    """Append-only evidence for one successful protected mutation CAS."""

    __tablename__ = "staging_mutation_epoch_events"
    __table_args__ = (
        CheckConstraint(
            "environment = 'staging'",
            name="staging_mutation_epoch_events_env_check",
        ),
        CheckConstraint(
            "namespace <> '' AND epoch > 0",
            name="staging_mutation_epoch_events_identity_check",
        ),
        CheckConstraint(
            "mutation_class IN ('rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epoch_events_class_check",
        ),
        CheckConstraint(
            "request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_mutation_epoch_events_evidence_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, primary_key=True)
    epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    mutation_class: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )


class StagingLifecycleCapacity(Base):
    """Fresh exact object/filesystem capacity used by staging admission."""

    __tablename__ = "staging_lifecycle_capacity"
    __table_args__ = (
        CheckConstraint("environment = 'staging'", name="staging_lifecycle_capacity_env_check"),
        CheckConstraint(
            "namespace <> '' AND source = 'exact-object-inventory-v1'",
            name="staging_lifecycle_capacity_identity_check",
        ),
        CheckConstraint(
            "object_count >= 0 AND bytes_used >= 0",
            name="staging_lifecycle_capacity_counters_check",
        ),
        CheckConstraint(
            "disk_free_percent BETWEEN 0 AND 100 AND inode_free_percent BETWEEN 0 AND 100",
            name="staging_lifecycle_capacity_percent_check",
        ),
        CheckConstraint(
            "policy_sha256 ~ '^[0-9a-f]{64}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_lifecycle_capacity_digest_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    object_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bytes_used: Mapped[int] = mapped_column(BigInteger, nullable=False)
    disk_free_percent: Mapped[int] = mapped_column(Integer, nullable=False)
    inode_free_percent: Mapped[int] = mapped_column(Integer, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class DataLifecycleAuthority(Base):
    """Typed environment/owner/retention authority for execution data."""

    __tablename__ = "data_lifecycle_authorities"
    __table_args__ = (
        CheckConstraint("environment <> ''", name="data_lifecycle_authorities_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_authorities_namespace_check"),
        CheckConstraint(
            "data_class IN ('run','trial','event','artifact','benchmark','catalog','system')",
            name="data_lifecycle_authorities_class_check",
        ),
        CheckConstraint(
            "owner_kind IN ('batch','trial','artifact','benchmark','system','orphan')",
            name="data_lifecycle_authorities_owner_kind_check",
        ),
        CheckConstraint(
            "state IN ('active','deleting','quarantined')",
            name="data_lifecycle_authorities_state_check",
        ),
        CheckConstraint(
            "(pinned AND expires_at IS NULL) OR "
            "(NOT pinned AND expires_at IS NOT NULL AND expires_at > created_at)",
            name="data_lifecycle_authorities_retention_check",
        ),
        CheckConstraint(
            "data_class NOT IN ('catalog','system') OR pinned",
            name="data_lifecycle_authorities_pinned_class_check",
        ),
        UniqueConstraint(
            "environment",
            "namespace",
            "data_class",
            "owner_kind",
            "owner_id",
            name="data_lifecycle_authorities_owner_uidx",
        ),
        Index(
            "data_lifecycle_authorities_gc_idx",
            "environment",
            "namespace",
            "state",
            "expires_at",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="RESTRICT"),
        nullable=True,
    )
    data_class: Mapped[str] = mapped_column(Text, nullable=False)
    owner_kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_id: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    pinned: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,
    )
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    deletion_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    lifecycle_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class DataLifecycleObject(Base):
    """Exact object-store identity owned by one lifecycle authority."""

    __tablename__ = "data_lifecycle_objects"
    __table_args__ = (
        CheckConstraint("environment <> ''", name="data_lifecycle_objects_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_objects_namespace_check"),
        CheckConstraint(
            "bucket <> '' AND object_key <> ''",
            name="data_lifecycle_objects_key_check",
        ),
        CheckConstraint("size_bytes >= 0", name="data_lifecycle_objects_size_check"),
        CheckConstraint(
            "state IN ('active','delete_pending','deleted','quarantined')",
            name="data_lifecycle_objects_state_check",
        ),
        CheckConstraint(
            "(state = 'deleted') = (verified_deleted_at IS NOT NULL)",
            name="data_lifecycle_objects_deleted_check",
        ),
        Index("data_lifecycle_objects_authority_state_idx", "authority_id", "state"),
        Index(
            "data_lifecycle_objects_identity_uidx",
            "environment",
            "namespace",
            "bucket",
            "object_key",
            text("COALESCE(version_id, '')"),
            unique=True,
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    authority_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="CASCADE"),
        nullable=False,
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    version_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    deletion_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    verified_deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )


class DataLifecycleGcRun(Base):
    """Append-only top-level journal for one staging-only GC transaction."""

    __tablename__ = "data_lifecycle_gc_runs"
    __table_args__ = (
        CheckConstraint("environment = 'staging'", name="data_lifecycle_gc_runs_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_gc_runs_namespace_check"),
        CheckConstraint(
            "mutation_epoch_before >= 0",
            name="data_lifecycle_gc_runs_epoch_before_check",
        ),
        CheckConstraint(
            "mutation_epoch_after IS NULL OR mutation_epoch_after > mutation_epoch_before",
            name="data_lifecycle_gc_runs_epoch_after_check",
        ),
        CheckConstraint(
            "state IN ('planned','applying','verifying','completed','failed')",
            name="data_lifecycle_gc_runs_state_check",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    mutation_epoch_before: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mutation_epoch_after: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'planned'"),
        default="planned",
    )
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False)
    requested_by: Mapped[str] = mapped_column(Text, nullable=False)
    policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    inventory: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class DataLifecycleGcItem(Base):
    """Retryable exact-object progress in a two-phase GC run."""

    __tablename__ = "data_lifecycle_gc_items"
    __table_args__ = (
        CheckConstraint(
            "state IN ('marked','object_deleted','verified','metadata_deleted','failed')",
            name="data_lifecycle_gc_items_state_check",
        ),
    )
    gc_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_gc_runs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    object_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
    )
    deletion_token: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'marked'"),
        default="marked",
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class DataLifecycleGcAuthority(Base):
    """Exact lifecycle-authority membership retained for resumable GC."""

    __tablename__ = "data_lifecycle_gc_authorities"
    gc_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_gc_runs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    authority_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
    )
    deletion_token: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)


class User(Base):
    __tablename__ = "users"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    email: Mapped[str | None] = mapped_column(Text, nullable=True)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    password_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    password_set_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending_setup'"),
        default="pending_setup",
    )
    disabled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    is_platform_admin: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )


class DevInstance(Base):
    """Durable lifecycle authority for one derived ``dev-<name>`` environment."""

    __tablename__ = "dev_instances"
    __table_args__ = (
        CheckConstraint(_personal_storage_binding_check("name"), name="dev_instances_storage_binding_check"),
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
    storage_binding: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
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


class PersonalDevCandidateArtifactCollection(Base):
    """Append-only evidence for one completed personal candidate collection."""

    __tablename__ = "personal_dev_candidate_artifact_collections"
    __table_args__ = (
        CheckConstraint(
            "collection_sequence > 0 AND gc_lease_epoch > 0 "
            "AND collector_id <> '' AND collector_id = btrim(collector_id) "
            "AND manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(manifest_json) = 'object' "
            "AND ((manifest_json->>'schema_version' = '1' "
            "AND manifest_json->>'candidate_id' = candidate_id::text) IS TRUE) "
            "AND collected_at >= unreferenced_at",
            name="personal_dev_candidate_artifact_collections_check",
        ),
        UniqueConstraint(
            "candidate_id",
            "collection_sequence",
            name="personal_dev_candidate_artifact_collections_sequence_uidx",
        ),
        Index(
            "personal_dev_candidate_artifact_collections_candidate_idx",
            "candidate_id",
            "collected_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    candidate_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("personal_dev_candidates.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collection_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    collector_id: Mapped[str] = mapped_column(String(128), nullable=False)
    gc_lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    manifest_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    unreferenced_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    collected_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )


class PersonalDevCandidateBuildAttempt(Base):
    """Lease-fenced handoff from trusted intake to an isolated image builder."""

    __tablename__ = "personal_dev_candidate_build_attempts"
    __table_args__ = (
        CheckConstraint(
            "attempt_sequence >= 0 AND operation_epoch > 0 AND lease_epoch >= 0",
            name="personal_dev_candidate_build_attempts_counters_check",
        ),
        CheckConstraint(
            "state IN ('queued', 'claimed', 'running', 'succeeded', 'failed')",
            name="personal_dev_candidate_build_attempts_state_check",
        ),
        CheckConstraint(
            "(state = 'queued' AND claimed_by IS NULL AND lease_expires_at IS NULL "
            "AND started_at IS NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'claimed' AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND started_at IS NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'running' AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND started_at IS NOT NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'succeeded' AND claimed_by IS NOT NULL AND lease_expires_at IS NULL "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL AND failure_reason IS NULL) OR "
            "(state = 'failed' AND claimed_by IS NOT NULL AND lease_expires_at IS NULL "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL AND failure_reason IS NOT NULL)",
            name="personal_dev_candidate_build_attempts_state_fields_check",
        ),
        UniqueConstraint(
            "candidate_id",
            "attempt_sequence",
            name="personal_dev_candidate_build_attempts_sequence_uidx",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "operation_epoch",
            "attempt_sequence",
            name="personal_dev_candidate_build_attempts_operation_uidx",
        ),
        Index(
            "personal_dev_candidate_build_attempts_picker_idx",
            "state",
            "lease_expires_at",
            "created_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    candidate_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("personal_dev_candidates.id", ondelete="CASCADE"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'queued'"))
    lease_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    failure_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
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
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class PersonalDevBuildPlatformRequest(Base):
    """Retained cold platform demand; not an assignment or physical grant."""

    __tablename__ = "personal_dev_build_platform_requests"
    __table_args__ = (
        CheckConstraint("platform IN ('linux/amd64', 'linux/arm64') AND attempt_lease_epoch > 0 "
            "AND deployment_generation > 0", name="personal_build_request_identity_check"),
        CheckConstraint("bucket_id ~ '^build-[0-9a-f]{64}$' "
            "AND source_binding_sha256 ~ '^[0-9a-f]{64}$' AND source_binding_sha256 <> repeat('0',64) "
            "AND runtime_installation_sha256 ~ '^[0-9a-f]{64}$' AND runtime_installation_sha256 <> repeat('0',64)",
            name="personal_build_request_digest_check"),
        CheckConstraint("cancelled_at IS NULL OR cancelled_at >= created_at", name="personal_build_request_time_check"),
        UniqueConstraint("attempt_id", "attempt_lease_epoch", "platform", name="personal_build_request_attempt_uidx"),
        Index("personal_build_request_owner_pending_idx", "owner_user_id", "subject_id", "subject_incarnation", "cancelled_at"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    owner_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    candidate_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("personal_dev_candidates.id", ondelete="RESTRICT"), nullable=False)
    attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("personal_dev_candidate_build_attempts.id", ondelete="RESTRICT"), nullable=False)
    attempt_lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bucket_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_binding_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_installation_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    cancelled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class PersonalDevNativeBuilderAgent(Base):
    """Durable signed identity and secret-free inventory for one native agent."""

    __tablename__ = "personal_dev_native_builder_agents"
    __table_args__ = (
        CheckConstraint(
            "key_id ~ '^[a-z][a-z0-9._-]{0,63}$' "
            "AND provider = 'gb10-gvisor-docker-v1' "
            "AND platform = 'linux/arm64' "
            "AND protocol_version = 1 "
            "AND host_architecture = 'aarch64' "
            "AND host_name <> '' AND host_name = btrim(host_name) "
            "AND octet_length(agent_image) BETWEEN 73 AND 584 "
            "AND agent_image ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
            "AND octet_length(builder_image) BETWEEN 73 AND 584 "
            "AND builder_image ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
            "AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$' "
            "AND readiness_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND status_sha256 ~ '^[0-9a-f]{64}$' "
            "AND max_concurrency = 2",
            name="personal_dev_native_builder_agents_identity_check",
        ),
        CheckConstraint(
            "jsonb_typeof(managed_grant_ids_json) = 'array' "
            "AND jsonb_array_length(managed_grant_ids_json) <= 64 "
            "AND jsonb_typeof(active_grant_ids_json) = 'array' "
            "AND jsonb_array_length(active_grant_ids_json) <= 2",
            name="personal_dev_native_builder_agents_inventory_check",
        ),
        CheckConstraint(
            "available = (unavailable_reason IS NULL) "
            "AND (unavailable_reason IS NULL OR "
            "unavailable_reason ~ '^[a-z][a-z0-9_]{0,127}$') "
            "AND jsonb_typeof(status_json) = 'object' "
            "AND ((status_json->>'agent_instance_id' = instance_id::text "
            "AND status_json->>'agent_key_id' = key_id "
            "AND status_json->>'provider' = provider "
            "AND status_json->>'platform' = platform "
            "AND (status_json->>'protocol_version')::integer = protocol_version "
            "AND status_json->>'host_name' = host_name "
            "AND status_json->>'host_architecture' = host_architecture "
            "AND status_json->>'host_boot_id' = host_boot_id::text "
            "AND status_json->>'agent_image' = agent_image "
            "AND status_json->>'builder_image' = builder_image "
            "AND status_json->>'runtime_profile_sha256' = runtime_profile_sha256 "
            "AND (status_json->>'max_concurrency')::integer = max_concurrency "
            "AND status_json->'managed_grant_ids' = managed_grant_ids_json "
            "AND status_json->'active_grant_ids' = active_grant_ids_json "
            "AND (status_json->>'available')::boolean = available "
            "AND status_json->>'unavailable_reason' "
            "IS NOT DISTINCT FROM unavailable_reason "
            "AND status_json->>'readiness_evidence_sha256' = "
            "readiness_evidence_sha256) IS TRUE) "
            "AND first_seen_at <= last_seen_at AND last_seen_at <= updated_at",
            name="personal_dev_native_builder_agents_status_check",
        ),
        UniqueConstraint(
            "key_id",
            name="personal_dev_native_builder_agents_key_uidx",
        ),
        Index(
            "personal_dev_native_builder_agents_freshness_idx",
            "available",
            "last_seen_at",
            "instance_id",
        ),
    )

    instance_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    key_id: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    protocol_version: Mapped[int] = mapped_column(Integer, nullable=False)
    host_name: Mapped[str] = mapped_column(String(253), nullable=False)
    host_architecture: Mapped[str] = mapped_column(String(32), nullable=False)
    host_boot_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    agent_image: Mapped[str] = mapped_column(Text, nullable=False)
    builder_image: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_profile_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    max_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    managed_grant_ids_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    active_grant_ids_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    available: Mapped[bool] = mapped_column(Boolean, nullable=False)
    unavailable_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    readiness_evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    last_poll_requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    last_poll_nonce: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class PersonalDevNativeBuildGrant(Base):
    """One agent-bound arm64 build under a whole-attempt lease epoch."""

    __tablename__ = "personal_dev_native_build_grants"
    __table_args__ = (
        CheckConstraint(
            "attempt_lease_epoch > 0 "
            "AND platform = 'linux/arm64' "
            "AND provider = 'gb10-gvisor-docker-v1' "
            "AND required_agent_key_id ~ '^[a-z][a-z0-9._-]{0,63}$' "
            "AND (running_agent_instance_id IS NULL "
            "OR running_agent_instance_id = required_agent_instance_id) "
            "AND octet_length(agent_image) BETWEEN 73 AND 584 "
            "AND agent_image ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
            "AND octet_length(builder_image) BETWEEN 73 AND 584 "
            "AND builder_image ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
            "AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$' "
            "AND contract_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(contract_json) BETWEEN 2 AND 65536 "
            "AND artifact_max_bytes BETWEEN 1 AND 17179869184 "
            "AND active_deadline_seconds BETWEEN 300 AND 7200",
            name="personal_dev_native_build_grants_identity_check",
        ),
        CheckConstraint(
            "source_bucket <> '' AND source_bucket = btrim(source_bucket) "
            "AND position('/' in source_bucket) = 0 "
            "AND artifact_bucket = source_bucket "
            "AND source_object_key <> '' AND artifact_object_key <> '' "
            "AND octet_length(source_object_key) <= 2048 "
            "AND octet_length(artifact_object_key) <= 2048 "
            "AND source_object_key !~ '[[:cntrl:]]' "
            "AND artifact_object_key !~ '[[:cntrl:]]' "
            "AND artifact_object_key LIKE 'personal-dev/builds/%/artifacts.tar'",
            name="personal_dev_native_build_grants_object_binding_check",
        ),
        CheckConstraint(
            "state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="personal_dev_native_build_grants_state_check",
        ),
        CheckConstraint(
            "((last_request_at IS NULL AND last_request_nonce IS NULL) OR "
            "(last_request_at IS NOT NULL AND last_request_nonce IS NOT NULL)) AND ("
            "(state = 'queued' AND running_agent_instance_id IS NULL "
            "AND started_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL "
            "AND failure_reason IS NULL AND completion_json IS NULL "
            "AND completion_sha256 IS NULL AND runtime_evidence_json IS NULL "
            "AND runtime_evidence_sha256 IS NULL AND artifact_head_json IS NULL "
            "AND artifact_head_sha256 IS NULL) OR ("
            "state = 'running' AND running_agent_instance_id IS NOT NULL "
            "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND finished_at IS NULL AND failure_reason IS NULL "
            "AND completion_json IS NULL AND completion_sha256 IS NULL "
            "AND runtime_evidence_json IS NULL AND runtime_evidence_sha256 IS NULL "
            "AND artifact_head_json IS NULL AND artifact_head_sha256 IS NULL) OR ("
            "state = 'succeeded' AND running_agent_instance_id IS NOT NULL "
            "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND finished_at IS NOT NULL AND failure_reason IS NULL "
            "AND jsonb_typeof(completion_json) = 'object' "
            "AND completion_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(runtime_evidence_json) = 'object' "
            "AND runtime_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(artifact_head_json) = 'object' "
            "AND artifact_head_sha256 ~ '^[0-9a-f]{64}$') OR ("
            "state = 'failed' AND running_agent_instance_id IS NOT NULL "
            "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND finished_at IS NOT NULL "
            "AND failure_reason ~ '^[a-z][a-z0-9_]{0,127}$' "
            "AND jsonb_typeof(completion_json) = 'object' "
            "AND completion_sha256 ~ '^[0-9a-f]{64}$' "
            "AND runtime_evidence_json IS NULL AND runtime_evidence_sha256 IS NULL "
            "AND artifact_head_json IS NULL AND artifact_head_sha256 IS NULL) OR ("
            "state = 'cancelled' AND finished_at IS NOT NULL "
            "AND failure_reason ~ '^[a-z][a-z0-9_]{0,127}$' "
            "AND ((running_agent_instance_id IS NULL AND started_at IS NULL "
            "AND heartbeat_at IS NULL) OR (running_agent_instance_id IS NOT NULL "
            "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL)) "
            "AND completion_json IS NULL AND completion_sha256 IS NULL "
            "AND runtime_evidence_json IS NULL AND runtime_evidence_sha256 IS NULL "
            "AND artifact_head_json IS NULL AND artifact_head_sha256 IS NULL)) "
            "AND queued_at <= updated_at "
            "AND (started_at IS NULL OR queued_at <= started_at) "
            "AND (heartbeat_at IS NULL OR started_at <= heartbeat_at) "
            "AND (finished_at IS NULL OR COALESCE(heartbeat_at, queued_at) <= finished_at)",
            name="personal_dev_native_build_grants_terminal_check",
        ),
        UniqueConstraint(
            "attempt_id",
            "attempt_lease_epoch",
            "platform",
            name="personal_dev_native_build_grants_attempt_platform_uidx",
        ),
        Index(
            "personal_dev_native_build_grants_picker_idx",
            "state",
            "queued_at",
            "id",
        ),
        Index(
            "personal_dev_native_build_grants_agent_state_idx",
            "required_agent_instance_id",
            "state",
            "updated_at",
            "id",
        ),
        Index(
            "personal_dev_native_build_grants_attempt_idx",
            "attempt_id",
            "attempt_lease_epoch",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    candidate_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "personal_dev_candidates.id",
            name="personal_dev_native_build_grants_candidate_fkey",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "personal_dev_candidate_build_attempts.id",
            name="personal_dev_native_build_grants_attempt_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    attempt_lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    required_agent_instance_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "personal_dev_native_builder_agents.instance_id",
            name="personal_dev_native_build_grants_required_agent_fkey",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    required_agent_key_id: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_image: Mapped[str] = mapped_column(Text, nullable=False)
    builder_image: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_profile_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    contract_json: Mapped[str] = mapped_column(Text, nullable=False)
    contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_bucket: Mapped[str] = mapped_column(Text, nullable=False)
    source_object_key: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_bucket: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_object_key: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_max_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    active_deadline_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    running_agent_instance_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "personal_dev_native_builder_agents.instance_id",
            name="personal_dev_native_build_grants_running_agent_fkey",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    last_request_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_request_nonce: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    completion_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    completion_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    runtime_evidence_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    runtime_evidence_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    artifact_head_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    artifact_head_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    queued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class DevLifecycleOperation(Base):
    """Owner-bound, epoch-fenced apply request for one personal environment."""

    __tablename__ = "dev_lifecycle_operations"
    __table_args__ = (
        CheckConstraint(_personal_storage_binding_check("environment_name"), name="dev_lifecycle_operations_storage_binding_check"),
        CheckConstraint(
            "operation_epoch >= expected_operation_epoch "
            "AND operation_epoch <= expected_operation_epoch + 1 "
            "AND expected_operation_epoch >= 0 AND attempt_sequence >= 0",
            name="dev_lifecycle_operations_epochs_check",
        ),
        CheckConstraint(
            "kind IN ('create', 'update', 'capacity', 'destroy', 'noop')",
            name="dev_lifecycle_operations_kind_check",
        ),
        CheckConstraint(
            "state IN ('requested', 'running', 'activating', 'succeeded', "
            "'failed', 'cancelling', 'cancelled', 'superseded')",
            name="dev_lifecycle_operations_state_check",
        ),
        CheckConstraint(
            "capacity_mode IN ('shadow-v1', 'membership-v1') AND ("
            "(capacity_mode = 'shadow-v1' AND capacity_membership_envelope IS NULL) OR ("
            "capacity_mode = 'membership-v1' "
            "AND capacity_expected_configuration_epoch IS NULL "
            "AND capacity_projection_request_sha256 IS NULL "
            "AND capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL "
            "AND (capacity_membership_envelope IS NULL OR "
            "(jsonb_typeof(capacity_membership_envelope) = 'object' "
            "AND capacity_membership_envelope->>'schema_version' = '1' "
            "AND capacity_membership_envelope->>'mode' = 'membership-v1' "
            "AND jsonb_typeof(capacity_membership_envelope->'request') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope->'observation') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope->'expected_checkpoint') = 'object' "
            "AND capacity_membership_envelope->>'request_sha256' ~ '^[0-9a-f]{64}$' "
            "AND capacity_membership_envelope->>'idempotency_key' IS NOT NULL "
            "AND jsonb_typeof(capacity_membership_envelope->'result') "
            "IN ('null', 'object') "
            "AND jsonb_typeof(capacity_membership_envelope->'historical_outcome') "
            "IN ('null', 'object') "
            "AND (jsonb_typeof(capacity_membership_envelope->'release') = 'null' OR ("
            "jsonb_typeof(capacity_membership_envelope->'release') = 'object' "
            "AND capacity_membership_envelope->'release'->>'schema_version' = '1' "
            "AND capacity_membership_envelope->'release'->>'outcome' = 'verified' "
            "AND capacity_membership_envelope->'release'->>'query_sha256' "
            "~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(capacity_membership_envelope->'release' "
            "->'membership_receipt') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope->'release'->'current') = 'object' "
            "AND capacity_membership_envelope->'release'->>'historical' = 'true' "
            "AND capacity_membership_envelope->'release'->>'worker_available' = 'false' "
            "AND jsonb_typeof(capacity_membership_envelope->'release' "
            "->'incarnation_work') = 'object' "
            "AND capacity_membership_envelope->'release'->>'release_set_sha256' "
            "~ '^[0-9a-f]{64}$')) IS TRUE) IS TRUE)) AND ("
            "kind = 'noop' OR checkpoint NOT IN ("
            "'capacity_projection_pending', 'capacity_projected', 'cleanup_pending', "
            "'membership_outcome_resolved', 'release_verified', "
            "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
            "'buckets_deleted', 'tenant_deleted', 'complete') OR ("
            "capacity_membership_envelope IS NOT NULL "
            "AND capacity_reporter_incarnation IS NOT NULL "
            "AND capacity_reporter_token_sha256 IS NOT NULL "
            "AND local_activation_sha256 IS NOT NULL "
            "AND protected_admission_sha256 IS NOT NULL "
            "AND capacity_agent_installation_sha256 IS NOT NULL "
            "AND capacity_supported_pool_ids IS NOT NULL "
            "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
            "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
            "AND capacity_supported_architectures IS NOT NULL "
            "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
            "AND jsonb_array_length(capacity_supported_architectures) > 0)))",
            name="dev_lifecycle_operations_capacity_mode_check",
        ),
        CheckConstraint(
            "request_sha256 ~ '^[0-9a-f]{64}$' "
            "AND candidate_sha ~ '^[0-9a-f]{64}$' "
            "AND (readiness_evidence_sha256 IS NULL OR "
            "readiness_evidence_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (activation_acknowledgement_sha256 IS NULL OR "
            "activation_acknowledgement_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (local_activation_sha256 IS NULL OR "
            "local_activation_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (capacity_projection_request_sha256 IS NULL OR "
            "capacity_projection_request_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (capacity_configuration_sha256 IS NULL OR "
            "capacity_configuration_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (capacity_reporter_token_sha256 IS NULL OR "
            "capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (protected_admission_sha256 IS NULL OR "
            "protected_admission_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (capacity_agent_installation_sha256 IS NULL OR "
            "capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$')",
            name="dev_lifecycle_operations_digests_check",
        ),
        CheckConstraint(
            "capacity_mode = 'membership-v1' OR ((capacity_expected_configuration_epoch IS NULL "
            "AND capacity_projection_request_sha256 IS NULL "
            "AND capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL "
            "AND capacity_reporter_incarnation IS NULL "
            "AND capacity_reporter_token_sha256 IS NULL "
            "AND protected_admission_sha256 IS NULL "
            "AND capacity_agent_installation_sha256 IS NULL "
            "AND capacity_supported_pool_ids IS NULL "
            "AND capacity_supported_architectures IS NULL) OR (("
            "kind = 'destroy' "
            "AND capacity_expected_configuration_epoch IS NULL "
            "AND capacity_projection_request_sha256 IS NULL "
            "AND capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL "
            "AND local_activation_sha256 IS NOT NULL "
            "AND capacity_reporter_incarnation IS NOT NULL "
            "AND capacity_reporter_token_sha256 IS NOT NULL "
            "AND protected_admission_sha256 IS NOT NULL "
            "AND capacity_agent_installation_sha256 IS NOT NULL "
            "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
            "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
            "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
            "AND jsonb_array_length(capacity_supported_architectures) > 0) OR ("
            "capacity_expected_configuration_epoch > 0 "
            "AND local_activation_sha256 IS NOT NULL "
            "AND capacity_projection_request_sha256 IS NOT NULL "
            "AND capacity_reporter_incarnation IS NOT NULL "
            "AND capacity_reporter_token_sha256 IS NOT NULL "
            "AND protected_admission_sha256 IS NOT NULL "
            "AND capacity_agent_installation_sha256 IS NOT NULL "
            "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
            "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
            "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
            "AND jsonb_array_length(capacity_supported_architectures) > 0 "
            "AND ((capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL) OR ("
            "capacity_configuration_epoch = capacity_expected_configuration_epoch + 1 "
            "AND capacity_configuration_sha256 IS NOT NULL)))))",
            name="dev_lifecycle_operations_capacity_projection_check",
        ),
        CheckConstraint(
            "(checkpoint <> 'pre_activation_abandoned' OR ("
            "kind = 'destroy' AND state = 'succeeded' "
            "AND readiness_evidence_sha256 IS NULL "
            "AND activation_acknowledgement_sha256 IS NULL "
            "AND local_activation_sha256 IS NULL "
            "AND capacity_expected_configuration_epoch IS NULL "
            "AND capacity_projection_request_sha256 IS NULL "
            "AND capacity_configuration_epoch IS NULL "
            "AND capacity_configuration_sha256 IS NULL "
            "AND capacity_reporter_incarnation IS NULL "
            "AND capacity_reporter_token_sha256 IS NULL "
            "AND protected_admission_sha256 IS NULL "
            "AND capacity_agent_installation_sha256 IS NULL "
            "AND capacity_supported_pool_ids IS NULL "
            "AND capacity_supported_architectures IS NULL)) AND ("
            "state <> 'succeeded' OR kind = 'noop' "
            "OR checkpoint = 'pre_activation_abandoned' "
            "OR (capacity_mode = 'shadow-v1' "
            "AND capacity_configuration_epoch IS NOT NULL) OR ("
            "capacity_mode = 'membership-v1' "
            "AND ((capacity_membership_envelope -> 'result' IS NOT NULL "
            "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object') OR ("
            "kind = 'destroy' AND checkpoint = 'complete' "
            "AND capacity_membership_envelope -> 'historical_outcome' ->> 'outcome' "
            "= 'committed' "
            "AND jsonb_typeof(capacity_membership_envelope "
            "-> 'historical_outcome' -> 'receipt') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'object'))))",
            name="dev_lifecycle_operations_capacity_completion_check",
        ),
        CheckConstraint(
            "(capacity_mode = 'shadow-v1' OR kind = 'noop' "
            "OR checkpoint = 'pre_activation_abandoned' "
            "OR (checkpoint NOT IN ('capacity_projection_pending', 'capacity_projected', "
            "'cleanup_pending', 'membership_outcome_resolved', 'release_verified', "
            "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
            "'buckets_deleted', 'tenant_deleted', 'complete') AND ("
            "capacity_membership_envelope IS NULL OR "
            "jsonb_typeof(capacity_membership_envelope -> 'release') = 'null')) OR ("
            "checkpoint = 'capacity_projection_pending' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
            "OR (checkpoint = 'membership_outcome_resolved' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
            "OR (checkpoint = 'capacity_projected' AND "
            "(capacity_membership_envelope -> 'result' IS NOT NULL AND "
            "jsonb_typeof(capacity_membership_envelope -> 'result') = 'object') AND "
            "jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
            "OR (checkpoint = 'cleanup_pending' AND kind = 'destroy' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
            "OR (kind = 'destroy' AND checkpoint IN ('release_verified', "
            "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
            "'buckets_deleted', 'tenant_deleted', 'complete') "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'object' AND (("
            "jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null') "
            "OR (jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
            "AND capacity_membership_envelope -> 'historical_outcome' ->> 'outcome' "
            "= 'committed' AND jsonb_typeof(capacity_membership_envelope "
            "-> 'historical_outcome' -> 'receipt') = 'object'))) "
            "OR (checkpoint = 'complete' AND kind <> 'destroy' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null')) "
            "IS TRUE",
            name="dev_lifecycle_operations_membership_completion_check",
        ),
        CheckConstraint(
            "min_slots >= 0 AND max_slots >= min_slots AND max_slots <= 8 "
            "AND deployment_generation > 0",
            name="dev_lifecycle_operations_target_check",
        ),
        CheckConstraint(
            "(kind = 'noop' AND operation_epoch = expected_operation_epoch "
            "AND state = 'succeeded') OR "
            "(kind <> 'noop' AND operation_epoch = expected_operation_epoch + 1)",
            name="dev_lifecycle_operations_transition_check",
        ),
        CheckConstraint(
            "(state IN ('requested', 'running', 'activating', 'cancelling') "
            "AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state IN ('succeeded', 'superseded') AND finished_at IS NOT NULL "
            "AND failure_reason IS NULL) OR "
            "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)",
            name="dev_lifecycle_operations_terminal_fields_check",
        ),
        CheckConstraint(
            "(kind IN ('capacity', 'destroy', 'noop') "
            "AND readiness_evidence_sha256 IS NULL "
            "AND activation_acknowledgement_sha256 IS NULL) OR "
            "(kind IN ('create', 'update') AND ("
            "(state IN ('requested', 'running', 'failed', 'cancelling', 'cancelled') "
            "AND readiness_evidence_sha256 IS NULL "
            "AND activation_acknowledgement_sha256 IS NULL) OR "
            "(state = 'activating' AND readiness_evidence_sha256 IS NOT NULL) OR "
            "(state IN ('succeeded', 'superseded') AND readiness_evidence_sha256 IS NOT NULL "
            "AND activation_acknowledgement_sha256 IS NOT NULL)))",
            name="dev_lifecycle_operations_activation_evidence_check",
        ),
        CheckConstraint(
            "(membership_predecessor_operation_id IS NULL "
            "AND membership_accepted_operation_id IS NULL "
            "AND membership_predecessor_envelope_sha256 IS NULL "
            "AND membership_successor_binding IS NULL "
            "AND membership_successor_binding_sha256 IS NULL "
            "AND membership_continuation_kind IS NULL) OR (("
            "membership_predecessor_operation_id IS NOT NULL "
            "AND membership_predecessor_operation_id <> id "
            "AND capacity_mode = 'membership-v1' AND kind IN ('create', 'update', 'destroy') "
            "AND membership_predecessor_envelope_sha256 ~ '^[0-9a-f]{64}$' "
            "AND membership_predecessor_envelope_sha256 <> repeat('0', 64) "
            "AND membership_successor_binding_sha256 ~ '^[0-9a-f]{64}$' "
            "AND membership_successor_binding_sha256 <> repeat('0', 64) "
            "AND membership_continuation_kind IN ('create', 'update', 'capacity', 'destroy') "
            "AND jsonb_typeof(membership_successor_binding) = 'object' "
            "AND membership_successor_binding->>'schema_version' = '1' "
            "AND membership_successor_binding->>'predecessor_operation_id' "
            "= membership_predecessor_operation_id::text "
            "AND membership_successor_binding->>'predecessor_envelope_sha256' "
            "= membership_predecessor_envelope_sha256 "
            "AND (membership_successor_binding->>'accepted_operation_id') "
            "IS NOT DISTINCT FROM membership_accepted_operation_id::text "
            "AND membership_successor_binding->>'owner_team_id' = owner_team_id::text) IS TRUE)",
            name="dev_lifecycle_operations_successor_fields_check",
        ),
        CheckConstraint(
            "(state = 'superseded') = (checkpoint = 'membership_successor_created') "
            "AND (state <> 'superseded' OR ((capacity_mode = 'membership-v1' "
            "AND jsonb_typeof(capacity_membership_envelope->'result') = 'null' "
            "AND jsonb_typeof(capacity_membership_envelope->'historical_outcome') = 'object' "
            "AND jsonb_typeof(capacity_membership_envelope->'release') = 'null' "
            "AND capacity_membership_envelope->'historical_outcome'->>'outcome' "
            "IN ('committed', 'terminal-not-committed') "
            "AND NOT (kind = 'destroy' AND capacity_membership_envelope "
            "->'historical_outcome'->>'outcome' = 'committed')) IS TRUE))",
            name="dev_lifecycle_operations_superseded_check",
        ),
        UniqueConstraint(
            "membership_predecessor_operation_id",
            name="dev_lifecycle_operations_successor_predecessor_uidx",
        ),
        UniqueConstraint(
            "owner_user_id",
            "idempotency_key",
            name="dev_lifecycle_operations_owner_idempotency_uidx",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "expected_operation_epoch",
            "request_sha256",
            "capacity_mode",
            name="dev_lifecycle_operations_request_uidx",
        ),
        UniqueConstraint(
            "attempt_id",
            name="dev_lifecycle_operations_attempt_id_uidx",
        ),
        Index(
            "dev_lifecycle_operations_environment_created_idx",
            "environment_name",
            "created_at",
            "id",
        ),
        Index(
            "dev_lifecycle_operations_active_environment_uidx",
            "environment_name",
            unique=True,
            postgresql_where=text(
                "state IN ('requested', 'running', 'activating', 'cancelling')",
            ),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    environment_name: Mapped[str] = mapped_column(
        Text,
        ForeignKey("dev_instances.name", ondelete="RESTRICT"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
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
    operation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expected_operation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    capacity_mode: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shadow-v1'"),
        default="shadow-v1",
    )
    capacity_membership_envelope: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    storage_binding: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    storage_binding_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    membership_predecessor_operation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("dev_lifecycle_operations.id", ondelete="RESTRICT"),
        nullable=True,
    )
    membership_predecessor_envelope_sha256: Mapped[str | None] = mapped_column(
        String(64), nullable=True,
    )
    membership_accepted_operation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("dev_lifecycle_operations.id", ondelete="RESTRICT"),
        nullable=True,
    )
    membership_successor_binding: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True,
    )
    membership_successor_binding_sha256: Mapped[str | None] = mapped_column(
        String(64), nullable=True,
    )
    membership_continuation_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    candidate_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("personal_dev_candidates.id", ondelete="RESTRICT"),
        nullable=False,
    )
    candidate_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    min_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    max_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    readiness_evidence_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    activation_acknowledgement_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    local_activation_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    capacity_expected_configuration_epoch: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    capacity_projection_request_sha256: Mapped[str | None] = mapped_column(
        String(64),
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
    keep_data: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    checkpoint: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text("'claimed'"),
    )
    failure_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
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
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class DevLifecycleOperationAttempt(Base):
    """Immutable attempt identity and checkpoint history for a logical operation."""

    __tablename__ = "dev_lifecycle_operation_attempts"
    __table_args__ = (
        CheckConstraint(
            "operation_epoch > 0 AND attempt_sequence >= 0 AND lease_epoch >= 0",
            name="dev_lifecycle_operation_attempts_counters_check",
        ),
        CheckConstraint(
            "credential_binding_version = 1 "
            "AND bootstrap_auth_kind IN ('bearer', 'session') "
            "AND octet_length(bootstrap_credential_hash) = 32",
            name="dev_lifecycle_operation_attempts_credential_check",
        ),
        CheckConstraint(
            "(claimed_by IS NULL AND lease_expires_at IS NULL) OR "
            "(claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="dev_lifecycle_operation_attempts_lease_check",
        ),
        CheckConstraint(
            "state IN ('running', 'activating', 'succeeded', 'failed', 'cancelled', 'superseded')",
            name="dev_lifecycle_operation_attempts_state_check",
        ),
        CheckConstraint(
            "(state IN ('running', 'activating') AND finished_at IS NULL "
            "AND failure_reason IS NULL) OR "
            "(state IN ('succeeded', 'superseded') AND finished_at IS NOT NULL "
            "AND failure_reason IS NULL) OR "
            "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)",
            name="dev_lifecycle_operation_attempts_terminal_fields_check",
        ),
        UniqueConstraint(
            "operation_id",
            "attempt_sequence",
            name="dev_lifecycle_operation_attempts_sequence_uidx",
        ),
        Index(
            "dev_lifecycle_operation_attempts_active_operation_uidx",
            "operation_id",
            unique=True,
            postgresql_where=text("state IN ('running', 'activating')"),
        ),
        Index(
            "dev_lifecycle_operation_attempts_picker_idx",
            "state",
            "checkpoint",
            "lease_expires_at",
            "created_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    operation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("dev_lifecycle_operations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    checkpoint: Mapped[str] = mapped_column(String(32), nullable=False)
    credential_binding_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    bootstrap_auth_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    bootstrap_credential_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        nullable=False,
    )
    lease_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    failure_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
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
    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class DevLifecycleActivationAcknowledgement(Base):
    """Append-only trusted environment acknowledgement for one activation."""

    __tablename__ = "dev_lifecycle_activation_acknowledgements"
    __table_args__ = (
        CheckConstraint(
            "operation_epoch > 0 AND deployment_generation > 0",
            name="dev_lifecycle_activation_acknowledgements_counters_check",
        ),
        CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{64}$' "
            "AND readiness_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND signature_sha256 ~ '^[0-9a-f]{64}$'",
            name="dev_lifecycle_activation_acknowledgements_digests_check",
        ),
        CheckConstraint(
            "agent_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'",
            name="dev_lifecycle_activation_acknowledgements_key_check",
        ),
        UniqueConstraint(
            "payload_sha256",
            name="dev_lifecycle_activation_acknowledgements_payload_uidx",
        ),
    )

    operation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("dev_lifecycle_operations.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    environment_name: Mapped[str] = mapped_column(Text, nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    candidate_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    candidate_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    readiness_evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    local_activation_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    signature_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_key_id: Mapped[str] = mapped_column(String(64), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TeamMembership(Base):
    __tablename__ = "team_memberships"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_memberships_role_check",
        ),
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TeamInvite(Base):
    __tablename__ = "team_invites"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_invites_role_check",
        ),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'revoked', 'expired')",
            name="team_invites_status_check",
        ),
        CheckConstraint(
            "max_uses IS NULL OR max_uses > 0",
            name="team_invites_max_uses_positive_check",
        ),
        CheckConstraint(
            "accepted_uses >= 0",
            name="team_invites_accepted_uses_nonnegative_check",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    email: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    code_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    code_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_uses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_by_actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_sent_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class UserSession(Base):
    __tablename__ = "user_sessions"
    session_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    current_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="SET NULL"),
        nullable=True,
    )
    csrf_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class LoginChallenge(Base):
    __tablename__ = "login_challenges"
    challenge_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)


class PendingTeamRegistration(Base):
    __tablename__ = "pending_team_registrations"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    contact_email: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=True,
    )
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class UserRegistrationRequest(Base):
    __tablename__ = "user_registration_requests"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="user_registration_requests_role_check",
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="user_registration_requests_status_check",
        ),
        Index(
            "user_registration_requests_active_username_uidx",
            "username_normalized",
            unique=True,
            postgresql_where=text("status IN ('pending', 'approved')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False, default="member")
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    reviewed_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    setup_token_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class AccountActionToken(Base):
    __tablename__ = "account_action_tokens"
    __table_args__ = (
        CheckConstraint(
            "purpose IN ('setup_password', 'reset_password')",
            name="account_action_tokens_purpose_check",
        ),
        Index("account_action_tokens_user_purpose_idx", "user_id", "purpose"),
        Index("account_action_tokens_prefix_idx", "token_prefix"),
    )

    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    purpose: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    registration_request_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("user_registration_requests.id", ondelete="SET NULL"),
        nullable=True,
    )
    password_reset_request_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("password_reset_requests.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class PasswordResetRequest(Base):
    __tablename__ = "password_reset_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="password_reset_requests_status_check",
        ),
        Index(
            "password_reset_requests_active_username_uidx",
            "username_normalized",
            unique=True,
            postgresql_where=text("status IN ('pending', 'approved')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    reviewed_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    reset_token_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)


class AdminAuditEvent(Base):
    __tablename__ = "admin_audit_events"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    target_type: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("jsonb_build_object()"),
        default=dict,
    )


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(
            "task_set_id IS NULL OR benchmark_id IS NULL",
            name="tasks_benchmark_or_taskset_check",
        ),
        Index("tasks_task_set_id_idx", "task_set_id"),
    )
    id: Mapped[str] = mapped_column(String, primary_key=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    # Per-task SPDX license tag (Plan 13). NULL on hand-authored tasks;
    # benchmark-imported tasks always carry it.
    license: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Parent benchmark, NULL for hand-authored tasks.
    benchmark_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("benchmarks.id"),
        nullable=True,
    )
    # Parent user TaskSet (#242 sub-plan 3). NULL for benchmark/hand-authored.
    task_set_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="SET NULL"),
        nullable=True,
    )
    # PR-1 (benchmark series): open-ended key→value metadata. Adapters
    # populate from upstream (year/exam/difficulty/topic/…). The SPA
    # uses these for the tag filter UI; the backend exposes a
    # discovery endpoint that walks distinct values per benchmark.
    tags: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default="{}",
    )
    # Immutable upstream identity for benchmark-imported tasks. Legacy tasks
    # default to an empty object; profile-specific importers populate it.
    source_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    registered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskBundleSource(Base):
    """Immutable logical identity and shared source admission/retirement lock."""

    __tablename__ = "task_bundle_sources"
    __table_args__ = (CheckConstraint("id ~ '^[0-9a-f]{64}$'", name="task_bundle_sources_id_check"),)
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_uri: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    spec_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskBundleSourceIncarnation(Base):
    __tablename__ = "task_bundle_source_incarnations"
    __table_args__ = (
        CheckConstraint("state IN ('uploading','available','deleting','retired')", name="task_bundle_incarnation_state_check"),
        CheckConstraint("expires_at > created_at", name="task_bundle_incarnation_deadline_check"),
        Index("task_bundle_incarnation_source_idx", "source_id", "state"),
        Index("task_bundle_incarnation_available_uidx", "source_id", unique=True, postgresql_where=text("state = 'available'")),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(64), ForeignKey("task_bundle_sources.id"), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskBundleSourceWrite(Base):
    __tablename__ = "task_bundle_source_writes"
    __table_args__ = (
        UniqueConstraint("incarnation_id", "object_key", name="task_bundle_write_object_uidx"),
        UniqueConstraint("id", "bucket", "object_key", name="task_bundle_write_storage_uidx"),
        CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$' AND size_bytes >= 0", name="task_bundle_write_content_check"),
        CheckConstraint("inventory_epoch >= 0", name="task_bundle_write_epoch_check"),
        Index("task_bundle_write_reconcile_idx", "last_observed_end_at", "id"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    incarnation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("task_bundle_source_incarnations.id"), nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    issued_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    inventory_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    inventory_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    inventory_cursor: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    last_observed_end_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class TaskBundleSourceVersion(Base):
    __tablename__ = "task_bundle_source_versions"
    __table_args__ = (
        ForeignKeyConstraint(["write_id", "bucket", "object_key"], ["task_bundle_source_writes.id", "task_bundle_source_writes.bucket", "task_bundle_source_writes.object_key"], name="task_bundle_version_write_fkey"),
        UniqueConstraint("bucket", "object_key", "version_id", name="task_bundle_version_identity_uidx"),
        CheckConstraint("version_id <> 'null' AND version_id <> '' AND version_id = btrim(version_id)", name="task_bundle_version_immutable_check"),
        CheckConstraint("state IN ('available','deleting','deleted')", name="task_bundle_version_state_check"),
        CheckConstraint("(state = 'deleted') = (deleted_at IS NOT NULL)", name="task_bundle_version_deleted_check"),
        Index("task_bundle_version_write_idx", "write_id", "state"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    write_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    version_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class TaskBundleSourceReference(Base):
    __tablename__ = "task_bundle_source_references"
    __table_args__ = (CheckConstraint("kind IN ('catalog','materialization','trial')", name="task_bundle_reference_kind_check"),)
    source_id: Mapped[str] = mapped_column(String(64), ForeignKey("task_bundle_sources.id"), primary_key=True)
    kind: Mapped[str] = mapped_column(String, primary_key=True)
    owner_id: Mapped[str] = mapped_column(Text, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class TaskImageMaterialization(Base):
    """Per-architecture prerequisite, optionally qualified by a strong manifest."""

    __tablename__ = "task_image_materializations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ready_publication_operation_id", "id"],
            [
                "task_image_publication_jobs.operation_id",
                "task_image_publication_jobs.materialization_id",
            ],
            name="task_image_materializations_ready_publication_fkey",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        CheckConstraint(
            "ready_publication_operation_id IS NULL OR "
            "(state = 'ready' AND jsonb_typeof(registry_images) = 'object' "
            "AND registry_images <> '{}'::jsonb AND ready_at IS NOT NULL)",
            name="task_image_materializations_ready_publication_check",
        ),
        CheckConstraint(
            "materialization_key ~ '^[0-9a-f]{64}$'",
            name="task_image_materializations_key_check",
        ),
        CheckConstraint(
            "task_checksum ~ '^[0-9a-f]{64}$'",
            name="task_image_materializations_checksum_check",
        ),
        CheckConstraint(
            "(bundle_content_manifest_sha256 = '' AND NOT "
            "(task_source_provenance ? 'bundle_content_manifest_sha256')) OR "
            "(bundle_content_manifest_sha256 ~ '^[0-9a-f]{64}$' AND COALESCE("
            "jsonb_typeof(task_source_provenance) = 'object' AND "
            "jsonb_typeof(task_source_provenance -> 'bundle_content_manifest_sha256') = 'string' "
            "AND task_source_provenance ->> 'bundle_content_manifest_sha256' = "
            "bundle_content_manifest_sha256, false))",
            name="task_image_materializations_manifest_binding_check",
        ),
        CheckConstraint(
            "bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256("
            "convert_to('task-image-materialization-v2', 'UTF8') || decode('00', 'hex') || "
            "convert_to(task_id, 'UTF8') || decode('00', 'hex') || "
            "convert_to(task_checksum, 'UTF8') || decode('00', 'hex') || "
            "convert_to(cpu_arch, 'UTF8') || decode('00', 'hex') || "
            "convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex')",
            name="task_image_materializations_manifest_key_check",
        ),
        CheckConstraint(
            "cpu_arch IN ('x86_64', 'arm64')",
            name="task_image_materializations_cpu_arch_check",
        ),
        CheckConstraint(
            "state IN ('queued', 'claimed', 'running', 'ready', 'failed', 'retiring', 'retired')",
            name="task_image_materializations_state_check",
        ),
        CheckConstraint(
            "attempt_count >= 0 AND max_attempts > 0 AND lease_epoch >= 0",
            name="task_image_materializations_counters_check",
        ),
        UniqueConstraint(
            "materialization_key",
            name="task_image_materializations_key_uidx",
        ),
        UniqueConstraint(
            "task_id",
            "task_checksum",
            "cpu_arch",
            "bundle_content_manifest_sha256",
            name="task_image_materializations_task_arch_uidx",
        ),
        Index(
            "task_image_materializations_queue_idx",
            "cpu_arch",
            "state",
            "next_attempt_at",
            "created_at",
        ),
        Index(
            "task_image_materializations_reference_idx",
            "task_id",
            "task_checksum",
        ),
        Index(
            "task_image_materializations_registry_gc_idx",
            "state",
            "unreferenced_at",
            "lease_expires_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    materialization_key: Mapped[str] = mapped_column(String(64), nullable=False)
    task_id: Mapped[str] = mapped_column(Text, nullable=False)
    task_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    bundle_content_manifest_sha256: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default=text("''"), default="",
    )
    cpu_arch: Mapped[str] = mapped_column(String(16), nullable=False)
    task_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    task_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_source_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'queued'"),
        default="queued",
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("3"),
        default=3,
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    registry_images: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    ready_publication_operation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=True,
    )
    registry_image_history: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    claimed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    ready_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_referenced_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    unreferenced_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )


class TaskImageCapacityWait(Base):
    """Renewable waiting head, never an attempt or permission to create a Pod."""

    __tablename__ = "task_image_capacity_waits"
    __table_args__ = (
        UniqueConstraint("materialization_id", name="task_image_capacity_waits_materialization_key"),
        CheckConstraint("lease_epoch >= 0", name="task_image_capacity_waits_epoch_check"),
        CheckConstraint("cpu_millis > 0 AND memory_mib > 0 AND storage_mib > 0",
                        name="task_image_capacity_waits_resources_check"),
        CheckConstraint("expires_at > renewed_at AND renewed_at >= first_waited_at "
                        "AND expires_at <= renewed_at + interval '120 seconds'",
                        name="task_image_capacity_waits_lifetime_check"),
    )
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="CASCADE"), primary_key=True,
    )
    materialization_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("task_image_materializations.id", ondelete="CASCADE"),
        nullable=False,
    )
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_waited_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    renewed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImageBuildGrant(Base):
    """One recoverable held-job authority with one submission invocation."""

    __tablename__ = "task_image_build_grants"
    __table_args__ = (
        CheckConstraint(
            "environment ~ '^[A-Za-z0-9_.-]+$'",
            name="task_image_build_grants_environment_check",
        ),
        CheckConstraint(
            "provider = 'slurm-rootless-v1'",
            name="task_image_build_grants_provider_check",
        ),
        CheckConstraint(
            "slurm_cluster_id IN ('oldlab','gb10')",
            name="task_image_build_grants_cluster_check",
        ),
        CheckConstraint(
            "cpu_arch IN ('x86_64','arm64')",
            name="task_image_build_grants_arch_check",
        ),
        CheckConstraint(
            "(slurm_cluster_id = 'oldlab' AND cpu_arch = 'x86_64' "
            "AND slurm_qos = 'loom-task-image-builder-rootless-oldlab') OR "
            "(slurm_cluster_id = 'gb10' AND cpu_arch = 'arm64' "
            "AND slurm_qos = 'loom-task-image-builder-rootless-gb10')",
            name="task_image_build_grants_native_check",
        ),
        CheckConstraint(
            "state IN ('issued','submitting','bound','released','revoked')",
            name="task_image_build_grants_state_check",
        ),
        CheckConstraint(
            "submitting_identity = 'loom-builder' "
            "AND slurm_account = 'loom-task-builder' "
            "AND slurm_partition = 'loom-task-builder'",
            name="task_image_build_grants_identity_check",
        ),
        CheckConstraint(
            "request_sha256 ~ '^[0-9a-f]{64}$'",
            name="task_image_build_grants_request_digest_check",
        ),
        CheckConstraint(
            "jsonb_typeof(authority_spec) = 'object' "
            "AND authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND authority_sha256 <> repeat('0', 64) "
            "AND isfinite(grant_expires_at)",
            name="task_image_build_grants_authority_check",
        ),
        CheckConstraint(
            "slurm_comment = 'loom-task-builder-v1:grant=' || id::text",
            name="task_image_build_grants_comment_check",
        ),
        CheckConstraint(
            "slurm_job_id IS NULL OR slurm_job_id ~ '^[0-9]+$'",
            name="task_image_build_grants_job_id_check",
        ),
        CheckConstraint(
            "journal_sequence >= 0",
            name="task_image_build_grants_journal_check",
        ),
        CheckConstraint(
            "ambiguity_settle_seconds > 0",
            name="task_image_build_grants_settle_check",
        ),
        CheckConstraint(
            "(state = 'issued' AND invocation_started_at IS NULL "
            "AND slurm_job_id IS NULL AND ambiguity_settle_until IS NULL "
            "AND bound_at IS NULL "
            "AND released_at IS NULL AND revoked_at IS NULL "
            "AND revoke_reason IS NULL) OR "
            "(state = 'submitting' AND invocation_started_at IS NOT NULL "
            "AND slurm_job_id IS NULL AND ambiguity_settle_until IS NOT NULL "
            "AND bound_at IS NULL "
            "AND released_at IS NULL AND revoked_at IS NULL "
            "AND revoke_reason IS NULL) OR "
            "(state = 'bound' AND invocation_started_at IS NOT NULL "
            "AND slurm_job_id IS NOT NULL AND ambiguity_settle_until IS NOT NULL "
            "AND bound_at IS NOT NULL "
            "AND released_at IS NULL AND revoked_at IS NULL "
            "AND revoke_reason IS NULL) OR "
            "(state = 'released' AND invocation_started_at IS NOT NULL "
            "AND slurm_job_id IS NOT NULL AND ambiguity_settle_until IS NOT NULL "
            "AND bound_at IS NOT NULL "
            "AND released_at IS NOT NULL AND revoked_at IS NULL "
            "AND revoke_reason IS NULL) OR "
            "(state = 'revoked' AND ambiguity_settle_until IS NOT NULL "
            "AND (released_at IS NULL OR (invocation_started_at IS NOT NULL "
            "AND slurm_job_id IS NOT NULL AND bound_at IS NOT NULL)) "
            "AND revoked_at IS NOT NULL AND revoke_reason IS NOT NULL)",
            name="task_image_build_grants_state_fields_check",
        ),
        Index(
            "task_image_build_grants_comment_uidx",
            "slurm_comment",
            unique=True,
        ),
        Index(
            "task_image_build_grants_job_uidx",
            "slurm_cluster_id",
            "slurm_job_id",
            unique=True,
            postgresql_where=text("slurm_job_id IS NOT NULL"),
        ),
        Index(
            "task_image_build_grants_reconcile_idx",
            "environment",
            "slurm_cluster_id",
            "state",
            "created_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    slurm_cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    cpu_arch: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'issued'"),
        default="issued",
    )
    submitting_identity: Mapped[str] = mapped_column(Text, nullable=False)
    slurm_account: Mapped[str] = mapped_column(Text, nullable=False)
    slurm_partition: Mapped[str] = mapped_column(Text, nullable=False)
    slurm_qos: Mapped[str] = mapped_column(Text, nullable=False)
    request_spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    authority_spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    authority_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    grant_expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    slurm_comment: Mapped[str] = mapped_column(Text, nullable=False)
    ambiguity_settle_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    ambiguity_settle_until: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    invocation_started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    slurm_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    revoke_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    journal_sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    bound_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class TaskImageBuildGrantEvent(Base):
    """Append-only transition evidence for one build grant."""

    __tablename__ = "task_image_build_grant_events"
    __table_args__ = (
        CheckConstraint(
            "sequence > 0",
            name="task_image_build_grant_events_sequence_check",
        ),
        CheckConstraint(
            "event_type IN ('issued','submission_started','reconciliation_wait',"
            "'cancellation_requested','bound','released','revoked')",
            name="task_image_build_grant_events_type_check",
        ),
        UniqueConstraint(
            "grant_id",
            "sequence",
            name="task_image_build_grant_events_sequence_uidx",
        ),
        Index(
            "task_image_build_grant_events_created_idx",
            "grant_id",
            "created_at",
            "sequence",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    grant_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_image_build_grants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TaskImageBuildProjection(Base):
    """Credential-projection state for one exact released build grant."""

    __tablename__ = "task_image_build_projections"
    __table_args__ = (
        ForeignKeyConstraint(
            ["grant_id"],
            ["task_image_build_grants.id"],
            name="task_image_build_projections_grant_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "session_generation", "session_id"],
            [
                "task_image_build_session_generations.grant_id",
                "task_image_build_session_generations.generation",
                "task_image_build_session_generations.session_id",
            ],
            name="task_image_build_projections_current_session_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "grant_id",
            name="task_image_build_projections_grant_uidx",
        ),
        UniqueConstraint(
            "grant_id",
            "request_id",
            name="task_image_build_projections_request_uidx",
        ),
        UniqueConstraint(
            "grant_id",
            "proof_id",
            name="task_image_build_projections_proof_uidx",
        ),
        UniqueConstraint(
            "grant_id",
            "exchange_id",
            name="task_image_build_projections_exchange_uidx",
        ),
        UniqueConstraint(
            "session_id",
            name="task_image_build_projections_session_uidx",
        ),
        CheckConstraint(
            "state IN ('challenged','projected','exchanged','revoked','expired')",
            name="task_image_build_projections_state_check",
        ),
        CheckConstraint(
            "id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND node_boot_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND challenge_nonce <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND principal_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' "
            "AND principal_sha256 ~ '^[0-9a-f]{64}$' "
            "AND principal_sha256 <> repeat('0', 64) "
            "AND request_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_sha256 <> repeat('0', 64) "
            "AND challenge_sha256 ~ '^[0-9a-f]{64}$' "
            "AND challenge_sha256 <> repeat('0', 64) "
            "AND node_name ~ '^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$' "
            "AND slurm_cluster_id IN ('oldlab','gb10') "
            "AND slurm_job_id ~ '^[1-9][0-9]{0,31}$' "
            "AND supervisor_pid > 0 AND supervisor_uid > 0 AND supervisor_gid > 0 "
            "AND supervisor_executable_sha256 ~ '^[0-9a-f]{64}$' "
            "AND supervisor_executable_sha256 <> repeat('0', 64) "
            "AND cgroup_path LIKE '/sys/fs/cgroup/%' AND cgroup_inode > 0 "
            "AND jsonb_typeof(request_json) = 'object' "
            "AND jsonb_typeof(challenge_json) = 'object' "
            "AND event_sequence >= 0",
            name="task_image_build_projections_identity_check",
        ),
        CheckConstraint(
            "((proof_id IS NULL AND proof_json IS NULL AND proof_sha256 IS NULL "
            "AND bootstrap_token_hash IS NULL AND bootstrap_secret_ref IS NULL "
            "AND bootstrap_issued_at IS NULL AND bootstrap_expires_at IS NULL "
            "AND attestation_generation IS NULL AND attestation_sha256 IS NULL "
            "AND attestation_expires_at IS NULL) OR "
            "(proof_id IS NOT NULL AND proof_json IS NOT NULL "
            "AND jsonb_typeof(proof_json) = 'object' "
            "AND proof_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proof_sha256 <> repeat('0', 64) "
            "AND octet_length(bootstrap_token_hash) = 32 "
            "AND bootstrap_secret_ref ~ "
            "'^loom://task-image-bootstrap/[A-Za-z0-9._/-]+$' "
            "AND octet_length(bootstrap_secret_ref) BETWEEN "
            "octet_length('loom://task-image-bootstrap/') + 1 AND "
            "octet_length('loom://task-image-bootstrap/') + 512 "
            "AND bootstrap_issued_at IS NOT NULL AND bootstrap_expires_at IS NOT NULL "
            "AND attestation_generation > 0 "
            "AND attestation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND attestation_sha256 <> repeat('0', 64) "
            "AND attestation_expires_at IS NOT NULL)) "
            "AND ((exchange_id IS NULL AND exchange_json IS NULL "
            "AND exchange_sha256 IS NULL AND session_id IS NULL "
            "AND session_token_hash IS NULL AND session_secret_ref IS NULL "
            "AND session_json IS NULL AND session_sha256 IS NULL "
            "AND session_issued_at IS NULL AND session_expires_at IS NULL) OR "
            "(exchange_id IS NOT NULL AND exchange_json IS NOT NULL "
            "AND jsonb_typeof(exchange_json) = 'object' "
            "AND exchange_sha256 ~ '^[0-9a-f]{64}$' "
            "AND exchange_sha256 <> repeat('0', 64) "
            "AND session_id IS NOT NULL AND octet_length(session_token_hash) = 32 "
            "AND session_secret_ref ~ "
            "'^loom://task-image-session/[A-Za-z0-9._/-]+$' "
            "AND octet_length(session_secret_ref) BETWEEN "
            "octet_length('loom://task-image-session/') + 1 AND "
            "octet_length('loom://task-image-session/') + 512 "
            "AND jsonb_typeof(session_json) = 'object' "
            "AND session_sha256 ~ '^[0-9a-f]{64}$' "
            "AND session_sha256 <> repeat('0', 64) "
            "AND session_issued_at IS NOT NULL AND session_expires_at IS NOT NULL)) "
            "AND (exchange_id IS NULL OR proof_id IS NOT NULL) "
            "AND ((state = 'challenged' AND proof_id IS NULL AND exchange_id IS NULL) "
            "OR (state = 'projected' AND proof_id IS NOT NULL AND exchange_id IS NULL) "
            "OR (state = 'exchanged' AND proof_id IS NOT NULL "
            "AND exchange_id IS NOT NULL) OR state IN ('revoked','expired'))",
            name="task_image_build_projections_state_fields_check",
        ),
        CheckConstraint(
            "(state NOT IN ('revoked','expired') AND revoked_at IS NULL "
            "AND revoke_reason IS NULL AND expired_at IS NULL) OR "
            "(state = 'revoked' AND revoked_at IS NOT NULL "
            "AND revoke_reason ~ '^[a-z][a-z0-9_]{0,127}$' "
            "AND expired_at IS NULL) OR "
            "(state = 'expired' AND expired_at IS NOT NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL)",
            name="task_image_build_projections_terminal_check",
        ),
        CheckConstraint(
            "challenge_issued_at < challenge_expires_at AND created_at <= updated_at "
            "AND (proof_id IS NULL OR "
            "(bootstrap_issued_at < bootstrap_expires_at "
            "AND bootstrap_issued_at < attestation_expires_at)) "
            "AND (exchange_id IS NULL OR session_issued_at < session_expires_at)",
            name="task_image_build_projections_time_check",
        ),
        CheckConstraint(
            "(session_id IS NULL AND session_generation IS NULL) OR "
            "(session_id IS NOT NULL AND session_generation > 0)",
            name="task_image_build_projections_session_generation_check",
        ),
        Index(
            "task_image_build_projections_active_session_idx",
            "session_expires_at",
            "grant_id",
            postgresql_where=text("state = 'exchanged'"),
        ),
        Index(
            "task_image_build_projections_attestation_expiry_idx",
            "attestation_expires_at",
            "grant_id",
            postgresql_where=text("state IN ('projected','exchanged')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    principal_id: Mapped[str] = mapped_column(String(128), nullable=False)
    principal_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    node_name: Mapped[str] = mapped_column(String(253), nullable=False)
    node_boot_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    slurm_cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    slurm_job_id: Mapped[str] = mapped_column(String(32), nullable=False)
    supervisor_pid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    supervisor_uid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    supervisor_gid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    supervisor_executable_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    cgroup_path: Mapped[str] = mapped_column(Text, nullable=False)
    cgroup_inode: Mapped[int] = mapped_column(BigInteger, nullable=False)
    challenge_nonce: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    challenge_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    challenge_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    challenge_issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    challenge_expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    proof_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    proof_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    proof_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    bootstrap_token_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32), nullable=True)
    bootstrap_secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    bootstrap_issued_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    bootstrap_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    exchange_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    exchange_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    exchange_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    session_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    session_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    session_token_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32), nullable=True)
    session_secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    session_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    session_issued_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    session_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    attestation_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    attestation_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    attestation_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    event_sequence: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    revoke_reason: Mapped[str | None] = mapped_column(String(128))
    expired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class TaskImageBuildProjectionEvent(Base):
    """Append-only transition and bounded exact-replay evidence."""

    __tablename__ = "task_image_build_projection_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["grant_id"],
            ["task_image_build_projections.grant_id"],
            name="task_image_build_projection_events_projection_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "grant_id",
            "event_sequence",
            name="task_image_build_projection_events_sequence_uidx",
        ),
        UniqueConstraint(
            "grant_id",
            "event_type",
            "event_key",
            name="task_image_build_projection_events_type_key_uidx",
        ),
        CheckConstraint(
            "event_sequence > 0",
            name="task_image_build_projection_events_sequence_check",
        ),
        CheckConstraint(
            "event_type IN ('challenged','challenge_replayed','projected',"
            "'projection_replayed','exchanged','exchange_replayed','attested',"
            "'attestation_replayed','renewed','renewal_replayed','revoked','expired')",
            name="task_image_build_projection_events_type_check",
        ),
        CheckConstraint(
            "event_key ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$' AND jsonb_typeof(payload_json) = 'object'",
            name="task_image_build_projection_events_key_check",
        ),
        Index(
            "task_image_build_projection_events_created_idx",
            "grant_id",
            "created_at",
            "event_sequence",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    event_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"), default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TaskImageBuildContainmentAttestation(Base):
    """Append-only containment liveness evidence for one projection."""

    __tablename__ = "task_image_build_containment_attestations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["grant_id"],
            ["task_image_build_projections.grant_id"],
            name="task_image_build_containment_attestations_projection_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "grant_id",
            "generation",
            name="task_image_build_containment_attestations_generation_uidx",
        ),
        CheckConstraint(
            "id <> '00000000-0000-0000-0000-000000000000'::uuid AND generation > 0",
            name="task_image_build_containment_attestations_generation_check",
        ),
        CheckConstraint(
            "jsonb_typeof(attestation_json) = 'object' "
            "AND attestation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND attestation_sha256 <> repeat('0', 64)",
            name="task_image_build_containment_attestations_digest_check",
        ),
        CheckConstraint(
            "issued_at < expires_at",
            name="task_image_build_containment_attestations_time_check",
        ),
        Index(
            "task_image_build_containment_attestations_expiry_idx",
            "expires_at",
            "grant_id",
            "generation",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attestation_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attestation_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TaskImageBuildSessionGeneration(Base):
    """Immutable secret-hash and attestation binding for one session generation."""

    __tablename__ = "task_image_build_session_generations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["grant_id"],
            ["task_image_build_projections.grant_id"],
            name="task_image_build_session_generations_projection_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "attestation_generation"],
            [
                "task_image_build_containment_attestations.grant_id",
                "task_image_build_containment_attestations.generation",
            ],
            name="task_image_build_session_generations_attestation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["predecessor_session_id"],
            ["task_image_build_session_generations.session_id"],
            name="task_image_build_session_generations_predecessor_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "grant_id",
            "generation",
            name="task_image_build_session_generations_generation_uidx",
        ),
        UniqueConstraint(
            "session_id",
            name="task_image_build_session_generations_session_uidx",
        ),
        UniqueConstraint(
            "grant_id",
            "generation",
            "session_id",
            name="task_image_build_session_generations_current_uidx",
        ),
        UniqueConstraint(
            "renewal_id",
            name="task_image_build_session_generations_renewal_uidx",
        ),
        CheckConstraint(
            "id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND generation > 0 AND octet_length(session_token_hash) = 32 "
            "AND session_secret_ref ~ "
            "'^loom://task-image-session/[A-Za-z0-9._/-]+$' "
            "AND octet_length(session_secret_ref) BETWEEN "
            "octet_length('loom://task-image-session/') + 1 AND "
            "octet_length('loom://task-image-session/') + 512",
            name="task_image_build_session_generations_identity_check",
        ),
        CheckConstraint(
            "jsonb_typeof(session_json) = 'object' "
            "AND session_sha256 ~ '^[0-9a-f]{64}$' "
            "AND session_sha256 <> repeat('0', 64) "
            "AND attestation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND attestation_sha256 <> repeat('0', 64)",
            name="task_image_build_session_generations_digest_check",
        ),
        CheckConstraint(
            "(generation = 1 AND renewal_id IS NULL "
            "AND renewal_sha256 IS NULL AND predecessor_session_id IS NULL) OR "
            "(generation > 1 AND renewal_id IS NOT NULL "
            "AND renewal_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND renewal_sha256 ~ '^[0-9a-f]{64}$' "
            "AND renewal_sha256 <> repeat('0', 64) "
            "AND predecessor_session_id IS NOT NULL "
            "AND predecessor_session_id <> session_id)",
            name="task_image_build_session_generations_chain_check",
        ),
        CheckConstraint(
            "issued_at < expires_at",
            name="task_image_build_session_generations_time_check",
        ),
        Index(
            "task_image_build_session_generations_expiry_idx",
            "expires_at",
            "grant_id",
            "generation",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    session_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_token_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    session_secret_ref: Mapped[str] = mapped_column(Text, nullable=False)
    session_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    session_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    attestation_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attestation_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    renewal_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    renewal_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    predecessor_session_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TaskImageMaterializationAttempt(Base):
    """Immutable builder/attempt/lease identity created with every claim."""

    __tablename__ = "task_image_materialization_attempts"
    __table_args__ = (
        CheckConstraint(
            "attempt_number > 0 AND lease_epoch > 0",
            name="task_image_materialization_attempts_counters_check",
        ),
        CheckConstraint(
            "num_nonnulls(grant_id, session_id, session_generation, claim_id, "
            "claim_deterministic_failure_count, claim_lease_expires_at, "
            "claim_plan_json, claim_plan_sha256) IN (0, 8)",
            name="task_image_materialization_attempts_session_fields_check",
        ),
        CheckConstraint(
            "claim_id IS NULL OR ("
            "claim_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND session_generation > 0 "
            "AND builder_id = 'rootless:' || replace(session_id::text, '-', '') "
            "AND claim_deterministic_failure_count >= 0 "
            "AND claimed_at < claim_lease_expires_at "
            "AND jsonb_typeof(claim_plan_json) = 'object' "
            "AND claim_plan_sha256 ~ '^[0-9a-f]{64}$' "
            "AND claim_plan_sha256 <> repeat('0', 64))",
            name="task_image_materialization_attempts_claim_id_check",
        ),
        ForeignKeyConstraint(
            ["grant_id", "session_generation", "session_id"],
            [
                "task_image_build_session_generations.grant_id",
                "task_image_build_session_generations.generation",
                "task_image_build_session_generations.session_id",
            ],
            name="task_image_materialization_attempts_session_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "materialization_id",
            "lease_epoch",
            name="task_image_materialization_attempts_lease_uidx",
        ),
        UniqueConstraint(
            "id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
            name="task_image_materialization_attempts_binding_uidx",
        ),
        UniqueConstraint(
            "claim_id",
            name="task_image_materialization_attempts_claim_uidx",
        ),
        UniqueConstraint(
            "id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
            "grant_id",
            name="task_image_materialization_attempts_operation_binding_uidx",
        ),
        Index(
            "task_image_materialization_attempts_lookup_idx",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    materialization_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_image_materializations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    # Native Job identity, requested resources and observed lifecycle belong to
    # this existing attempt; retries never overwrite an older attempt's record.
    native_build: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    grant_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    session_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    session_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    claim_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    claim_deterministic_failure_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    claim_lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    claim_plan_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    claim_plan_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TaskImageAttemptRetention(Base):
    """Observation until retirement, then immutable exact-attempt evidence.

    This record is not proof of reference eligibility or registry quiescence.
    Alembic installs guards against rewriting retired evidence or moving its
    observation clock backward. Maintenance receipts are a separate lifecycle.
    """

    __tablename__ = "task_image_attempt_retention"
    __table_args__ = (
        CheckConstraint(
            "isfinite(observed_at) AND (unreferenced_since IS NULL OR "
            "(isfinite(unreferenced_since) AND unreferenced_since <= observed_at))",
            name="task_image_attempt_retention_time_check",
        ),
        CheckConstraint(
            "(retired_at IS NULL AND canonical_inventory IS NULL AND inventory_sha256 IS NULL) OR "
            "(retired_at IS NOT NULL AND unreferenced_since IS NOT NULL "
            "AND canonical_inventory IS NOT NULL AND inventory_sha256 IS NOT NULL "
            "AND retired_at = observed_at AND retired_at >= unreferenced_since "
            "AND octet_length(canonical_inventory) BETWEEN 1 AND 131072 "
            "AND inventory_sha256 = encode(sha256(canonical_inventory), 'hex'))",
            name="task_image_attempt_retention_shape_check",
        ),
        Index(
            "task_image_attempt_retention_pending_idx", "unreferenced_since", "attempt_id",
            postgresql_where=text("retired_at IS NULL"),
        ),
        Index(
            "task_image_attempt_retention_retired_idx", "retired_at", "attempt_id",
            postgresql_where=text("retired_at IS NOT NULL"),
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_image_materialization_attempts.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    unreferenced_since: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    canonical_inventory: Mapped[bytes | None] = mapped_column(LargeBinary)
    inventory_sha256: Mapped[str | None] = mapped_column(String(64))


class TaskImageMaterializationOperationEvent(Base):
    """One bounded idempotent transition result for a session-owned lease."""

    __tablename__ = "task_image_materialization_operation_events"
    __table_args__ = (
        ForeignKeyConstraint(
            [
                "materialization_attempt_id",
                "materialization_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
                "grant_id",
            ],
            [
                "task_image_materialization_attempts.id",
                "task_image_materialization_attempts.materialization_id",
                "task_image_materialization_attempts.attempt_number",
                "task_image_materialization_attempts.lease_epoch",
                "task_image_materialization_attempts.builder_id",
                "task_image_materialization_attempts.grant_id",
            ],
            name="task_image_materialization_operation_events_attempt_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "session_generation", "session_id"],
            [
                "task_image_build_session_generations.grant_id",
                "task_image_build_session_generations.generation",
                "task_image_build_session_generations.session_id",
            ],
            name="task_image_materialization_operation_events_session_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "operation_id",
            name="task_image_materialization_operation_events_operation_uidx",
        ),
        CheckConstraint(
            "id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND operation_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND attempt_number > 0 AND lease_epoch > 0 "
            "AND session_generation > 0 AND result_attempt_count >= 0",
            name="task_image_materialization_operation_events_identity_check",
        ),
        CheckConstraint(
            "operation_type IN ('start','heartbeat','bundle','release',"
            "'containment_release','deterministic_fail')",
            name="task_image_materialization_operation_events_type_check",
        ),
        CheckConstraint(
            "(operation_type IN ('start','heartbeat','bundle') "
            "AND result_state IN ('claimed','running') "
            "AND result_lease_expires_at IS NOT NULL) OR "
            "(operation_type IN ('release','containment_release','deterministic_fail') "
            "AND result_state IN ('queued','failed') "
            "AND result_lease_expires_at IS NULL)",
            name="task_image_materialization_operation_events_result_check",
        ),
        CheckConstraint(
            "(operation_type <> 'bundle' "
            "AND secret_response_ref IS NULL "
            "AND secret_response_sha256 IS NULL "
            "AND secret_response_expires_at IS NULL) OR "
            "(operation_type = 'bundle' "
            "AND num_nonnulls(secret_response_ref, secret_response_sha256, "
            "secret_response_expires_at) = 3 "
            "AND secret_response_ref ~ "
            "'^loom://task-image-bundle-capability/[A-Za-z0-9._/-]+$' "
            "AND octet_length(secret_response_ref) BETWEEN "
            "octet_length('loom://task-image-bundle-capability/') + 1 AND "
            "octet_length('loom://task-image-bundle-capability/') + 512 "
            "AND secret_response_sha256 ~ '^[0-9a-f]{64}$' "
            "AND secret_response_sha256 <> repeat('0', 64) "
            "AND recorded_at < secret_response_expires_at)",
            name="task_image_materialization_operation_events_secret_check",
        ),
        Index(
            "task_image_materialization_operation_events_attempt_idx",
            "materialization_id",
            "lease_epoch",
            "recorded_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    result_state: Mapped[str] = mapped_column(String(16), nullable=False)
    result_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    result_lease_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    secret_response_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    secret_response_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    secret_response_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    recorded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImageRegistryCredentialGeneration(Base):
    """Append-only exact-repository registry credential generation."""

    __tablename__ = "task_image_registry_credentials"
    __table_args__ = (
        ForeignKeyConstraint(
            [
                "materialization_attempt_id",
                "materialization_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
                "grant_id",
            ],
            [
                "task_image_materialization_attempts.id",
                "task_image_materialization_attempts.materialization_id",
                "task_image_materialization_attempts.attempt_number",
                "task_image_materialization_attempts.lease_epoch",
                "task_image_materialization_attempts.builder_id",
                "task_image_materialization_attempts.grant_id",
            ],
            name="task_image_registry_credentials_attempt_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "session_generation", "session_id"],
            [
                "task_image_build_session_generations.grant_id",
                "task_image_build_session_generations.generation",
                "task_image_build_session_generations.session_id",
            ],
            name="task_image_registry_credentials_session_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "attestation_generation"],
            [
                "task_image_build_containment_attestations.grant_id",
                "task_image_build_containment_attestations.generation",
            ],
            name="task_image_registry_credentials_attestation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["predecessor_credential_id"],
            ["task_image_registry_credentials.credential_id"],
            name="task_image_registry_credentials_predecessor_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["lease_heartbeat_operation_id"],
            ["task_image_materialization_operation_events.operation_id"],
            name="task_image_registry_credentials_heartbeat_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "request_id",
            name="task_image_registry_credentials_request_uidx",
        ),
        UniqueConstraint(
            "materialization_attempt_id",
            "component",
            "generation",
            name="task_image_registry_credentials_component_generation_uidx",
        ),
        UniqueConstraint(
            "credential_id",
            "materialization_attempt_id",
            "component",
            "repository",
            name="task_image_registry_credentials_candidate_binding_uidx",
        ),
        CheckConstraint(
            "credential_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND materialization_attempt_id <> "
            "'00000000-0000-0000-0000-000000000000'::uuid "
            "AND materialization_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND attempt_number > 0 AND lease_epoch > 0 "
            "AND session_generation > 0 AND attestation_generation > 0 "
            "AND generation BETWEEN 1 AND 512 "
            "AND component ~ '^(task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$'",
            name="task_image_registry_credentials_binding_check",
        ),
        CheckConstraint(
            "(generation = 1 AND predecessor_credential_id IS NULL "
            "AND lease_heartbeat_operation_id IS NULL) OR "
            "(generation > 1 AND predecessor_credential_id IS NOT NULL "
            "AND predecessor_credential_id <> credential_id "
            "AND lease_heartbeat_operation_id IS NOT NULL)",
            name="task_image_registry_credentials_chain_check",
        ),
        CheckConstraint(
            "attestation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND attestation_sha256 <> repeat('0', 64) "
            "AND request_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_sha256 <> repeat('0', 64) "
            "AND jsonb_typeof(response_public_json) = 'object' "
            "AND response_sha256 ~ '^[0-9a-f]{64}$' "
            "AND response_sha256 <> repeat('0', 64) "
            "AND octet_length(token_hash) = 32 "
            "AND registry_key_id ~ '^[A-Za-z0-9_-]{43}$'",
            name="task_image_registry_credentials_digest_check",
        ),
        CheckConstraint(
            "octet_length(repository) BETWEEN 1 AND 255 "
            "AND repository ~ "
            "'^loom-task-image-attempts/(x86_64|arm64)/"
            "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
            "[89ab][0-9a-f]{3}-[0-9a-f]{12}/"
            "(task|sidecar-sha256-[0-9a-f]{64})$' "
            "AND octet_length(registry_origin) BETWEEN 9 AND 2048 "
            "AND registry_origin ~ '^https://[^/@?#]+(:[1-9][0-9]{0,4})?$' "
            "AND registry_service ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$' "
            "AND registry_issuer ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$'",
            name="task_image_registry_credentials_registry_check",
        ),
        CheckConstraint(
            "secret_response_ref ~ "
            "'^loom://task-image-registry-credential/"
            "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'",
            name="task_image_registry_credentials_secret_check",
        ),
        CheckConstraint(
            "issued_at < expires_at "
            "AND expires_at <= issued_at + INTERVAL '45 seconds' "
            "AND issued_at <= recorded_at",
            name="task_image_registry_credentials_time_check",
        ),
        Index(
            "task_image_registry_credentials_expiry_idx",
            "expires_at",
            "credential_id",
        ),
        Index(
            "task_image_registry_credentials_renewal_idx",
            "materialization_attempt_id",
            "component",
            text("generation DESC"),
        ),
    )

    credential_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attestation_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attestation_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    component: Mapped[str] = mapped_column(String(136), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    predecessor_credential_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
    lease_heartbeat_operation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
    repository: Mapped[str] = mapped_column(Text, nullable=False)
    registry_origin: Mapped[str] = mapped_column(Text, nullable=False)
    registry_service: Mapped[str] = mapped_column(String(128), nullable=False)
    registry_issuer: Mapped[str] = mapped_column(String(128), nullable=False)
    registry_key_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    response_public_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    response_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    token_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    secret_response_ref: Mapped[str] = mapped_column(Text, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImagePublicationJob(Base):
    """Immutable attempt input with independently renewable worker ownership."""

    __tablename__ = "task_image_publication_jobs"
    __table_args__ = (
        UniqueConstraint(
            "operation_id",
            "materialization_id",
            name="task_image_publication_jobs_materialization_uidx",
        ),
        UniqueConstraint(
            "materialization_attempt_id", name="task_image_publication_jobs_attempt_uidx"
        ),
        ForeignKeyConstraint(
            [
                "materialization_attempt_id",
                "materialization_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
                "grant_id",
            ],
            [
                "task_image_materialization_attempts." + name
                for name in (
                    "id",
                    "materialization_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "grant_id",
                )
            ],
            name="task_image_publication_jobs_attempt_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "operation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND attempt_number BETWEEN 1 AND 9007199254740991 AND lease_epoch BETWEEN 1 AND 9007199254740991 AND worker_generation BETWEEN 0 AND 9007199254740991",
            name="task_image_publication_jobs_identity_check",
        ),
        CheckConstraint(
            "octet_length(canonical_snapshot) BETWEEN 1 AND 4194304 AND snapshot_sha256 = encode(sha256(canonical_snapshot), 'hex')",
            name="task_image_publication_jobs_snapshot_check",
        ),
        CheckConstraint(
            "isfinite(created_at) AND isfinite(deadline) AND isfinite(available_at) AND deadline > created_at AND deadline <= created_at + interval '7200 seconds' AND available_at >= created_at AND (worker_expires_at IS NULL OR (isfinite(worker_expires_at) AND worker_expires_at <= deadline))",
            name="task_image_publication_jobs_time_check",
        ),
        CheckConstraint(
            "(state = 'running' AND worker_id IS NOT NULL AND worker_id <> '00000000-0000-0000-0000-000000000000'::uuid AND worker_generation > 0 AND worker_expires_at IS NOT NULL AND failure_code IS NULL) OR (state IN ('queued', 'completed') AND worker_id IS NULL AND worker_expires_at IS NULL AND failure_code IS NULL) OR (state = 'failed' AND worker_id IS NULL AND worker_expires_at IS NULL AND failure_code IS NOT NULL AND failure_code IN ('integrity', 'authority_lost', 'verification_failed', 'deadline'))",
            name="task_image_publication_jobs_state_check",
        ),
        Index("task_image_publication_jobs_work_idx", "state", "available_at", "deadline"),
        CheckConstraint(
            "(state = 'completed' AND completed_at IS NOT NULL AND canonical_receipt IS NOT NULL "
            "AND receipt_sha256 IS NOT NULL AND isfinite(completed_at) "
            "AND completed_at >= created_at AND completed_at < deadline "
            "AND octet_length(canonical_receipt) BETWEEN 1 AND 2048 "
            "AND receipt_sha256 = encode(sha256(canonical_receipt), 'hex')) OR "
            "(state <> 'completed' AND completed_at IS NULL AND canonical_receipt IS NULL AND receipt_sha256 IS NULL)",
            name="task_image_publication_jobs_completion_check",
        ),
    )

    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    canonical_snapshot: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    deadline: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    worker_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    worker_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    failure_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    canonical_receipt: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    receipt_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)


class TaskImagePublicationCandidate(Base):
    """Inert immutable upload evidence awaiting independent verification."""

    __tablename__ = "task_image_publication_candidates"
    __table_args__ = (
        ForeignKeyConstraint(
            [
                "materialization_attempt_id",
                "materialization_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
                "grant_id",
            ],
            [
                "task_image_materialization_attempts.id",
                "task_image_materialization_attempts.materialization_id",
                "task_image_materialization_attempts.attempt_number",
                "task_image_materialization_attempts.lease_epoch",
                "task_image_materialization_attempts.builder_id",
                "task_image_materialization_attempts.grant_id",
            ],
            name="task_image_publication_candidates_attempt_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["grant_id", "session_generation", "session_id"],
            [
                "task_image_build_session_generations.grant_id",
                "task_image_build_session_generations.generation",
                "task_image_build_session_generations.session_id",
            ],
            name="task_image_publication_candidates_session_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["credential_id", "materialization_attempt_id", "component", "repository"],
            [
                "task_image_registry_credentials.credential_id",
                "task_image_registry_credentials.materialization_attempt_id",
                "task_image_registry_credentials.component",
                "task_image_registry_credentials.repository",
            ],
            name="task_image_publication_candidates_credential_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "operation_id",
            name="task_image_publication_candidates_operation_uidx",
        ),
        UniqueConstraint(
            "materialization_attempt_id",
            "component",
            name="task_image_publication_candidates_attempt_component_uidx",
        ),
        UniqueConstraint(
            "candidate_id",
            "materialization_attempt_id",
            "component",
            name="task_image_publication_candidates_envelope_binding_uidx",
        ),
        CheckConstraint(
            "candidate_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND operation_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND credential_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND materialization_attempt_id <> "
            "'00000000-0000-0000-0000-000000000000'::uuid "
            "AND materialization_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND attempt_number > 0 AND lease_epoch > 0 "
            "AND session_generation > 0 "
            "AND component ~ '^(task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$' "
            "AND repository ~ "
            "'^loom-task-image-attempts/(x86_64|arm64)/"
            "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
            "[89ab][0-9a-f]{3}-[0-9a-f]{12}/"
            "(task|sidecar-sha256-[0-9a-f]{64})$' "
            "AND platform IN ('linux/amd64', 'linux/arm64')",
            name="task_image_publication_candidates_binding_check",
        ),
        CheckConstraint(
            "manifest_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND manifest_digest <> 'sha256:' || repeat('0', 64) "
            "AND manifest_size > 0 "
            "AND oci_file_sha256 ~ '^[0-9a-f]{64}$' "
            "AND oci_file_sha256 <> repeat('0', 64) "
            "AND oci_file_size > 0",
            name="task_image_publication_candidates_digest_check",
        ),
        CheckConstraint(
            "jsonb_typeof(response_json) = 'object' "
            "AND response_sha256 ~ '^[0-9a-f]{64}$' "
            "AND response_sha256 <> repeat('0', 64)",
            name="task_image_publication_candidates_response_check",
        ),
        Index(
            "task_image_publication_candidates_observed_idx",
            "recorded_at",
            "candidate_id",
        ),
    )

    candidate_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    credential_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    session_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    component: Mapped[str] = mapped_column(String(136), nullable=False)
    repository: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    manifest_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    oci_file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    oci_file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    platform: Mapped[str] = mapped_column(String(16), nullable=False)
    response_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    response_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImagePublicationState(Base):
    """Lock singleton first, before keys and all other publication/start authority.

    Zero keyset version is deliberately inactive. This row is not evidence of
    distribution; a future authenticated adapter must prove current membership.
    """

    __tablename__ = "task_image_publication_state"
    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="task_image_publication_state_singleton_check"),
        CheckConstraint(
            "revocation_epoch BETWEEN 0 AND 9007199254740991 "
            "AND keyset_version BETWEEN 0 AND 9007199254740991",
            name="task_image_publication_state_counters_check",
        ),
    )
    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, server_default=text("1"))
    revocation_epoch: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    keyset_version: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )


class TaskImagePublicationKey(Base):
    """Retained public identity; migrations enforce immutable and monotonic fields."""

    __tablename__ = "task_image_publication_keys"
    __table_args__ = (
        CheckConstraint(
            "key_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' AND octet_length(public_key) = 32",
            name="task_image_publication_keys_identity_check",
        ),
        CheckConstraint(
            "(status = 'active' AND retired_at IS NULL AND revoked_at IS NULL) OR "
            "(status = 'verify_only' AND retired_at IS NOT NULL AND revoked_at IS NULL) OR "
            "(status = 'revoked' AND revoked_at IS NOT NULL)",
            name="task_image_publication_keys_lifecycle_check",
        ),
        CheckConstraint(
            "isfinite(activated_at) AND date_trunc('second', activated_at) = activated_at "
            "AND (retired_at IS NULL OR (isfinite(retired_at) AND retired_at >= activated_at "
            "AND date_trunc('second', retired_at) = retired_at)) "
            "AND (revoked_at IS NULL OR (isfinite(revoked_at) AND revoked_at >= activated_at "
            "AND date_trunc('second', revoked_at) = revoked_at)) "
            "AND (retired_at IS NULL OR revoked_at IS NULL OR revoked_at >= retired_at)",
            name="task_image_publication_keys_interval_check",
        ),
    )
    key_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    public_key: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    activated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'active'"))
    retired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class TaskImagePublicationKeyset(Base):
    """Original signed distribution artifact; never a worker-readiness certificate."""

    __tablename__ = "task_image_publication_keysets"
    __table_args__ = (
        CheckConstraint(
            "keyset_version BETWEEN 1 AND 9007199254740991 "
            "AND revocation_epoch BETWEEN 0 AND 9007199254740991",
            name="task_image_publication_keysets_counters_check",
        ),
        CheckConstraint(
            "environment ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' "
            "AND execution_key_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' "
            "AND root_sha256 ~ '^[0-9a-f]{64}$' AND keyset_sha256 ~ '^[0-9a-f]{64}$'",
            name="task_image_publication_keysets_identity_check",
        ),
        CheckConstraint(
            "octet_length(canonical_envelope) BETWEEN 1 AND 131072 "
            "AND snapshot_sha256 = encode(sha256(canonical_envelope), 'hex')",
            name="task_image_publication_keysets_bytes_check",
        ),
        CheckConstraint(
            "isfinite(issued_at) AND isfinite(expires_at) "
            "AND date_trunc('second', issued_at) = issued_at "
            "AND date_trunc('second', expires_at) = expires_at "
            "AND expires_at > issued_at AND expires_at <= issued_at + interval '15 minutes'",
            name="task_image_publication_keysets_interval_check",
        ),
    )
    keyset_version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    revocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    environment: Mapped[str] = mapped_column(String(128), nullable=False)
    execution_key_id: Mapped[str] = mapped_column(String(128), nullable=False)
    root_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    keyset_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_envelope: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImagePublicationKeysetMember(Base):
    """Restrictive immutable membership preserves historical public identities."""

    __tablename__ = "task_image_publication_keyset_members"
    keyset_version: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("task_image_publication_keysets.keyset_version", ondelete="RESTRICT"),
        primary_key=True,
    )
    key_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("task_image_publication_keys.key_id", ondelete="RESTRICT"),
        primary_key=True,
    )


class TaskImagePublicationEnvelope(Base):
    """Immutable signed audit bytes, distinct from live artifact retention pins.

    Closed canonical schema and Ed25519 verification belong to the application
    boundary; these database constraints preserve exact stored bytes and binds.
    """

    __tablename__ = "task_image_publication_envelopes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["candidate_id", "materialization_attempt_id", "component"],
            [
                "task_image_publication_candidates.candidate_id",
                "task_image_publication_candidates.materialization_attempt_id",
                "task_image_publication_candidates.component",
            ],
            name="task_image_publication_envelopes_candidate_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["key_id"],
            ["task_image_publication_keys.key_id"],
            name="task_image_publication_envelopes_key_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("candidate_id", name="task_image_publication_envelopes_candidate_uidx"),
        UniqueConstraint(
            "materialization_attempt_id",
            "component",
            name="task_image_publication_envelopes_attempt_component_uidx",
        ),
        CheckConstraint(
            "envelope_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND distributed_keyset_version BETWEEN 1 AND 9007199254740991 "
            "AND revocation_epoch BETWEEN 0 AND 9007199254740991",
            name="task_image_publication_envelopes_binding_check",
        ),
        CheckConstraint(
            "octet_length(canonical_statement) BETWEEN 1 AND 65536 "
            "AND statement_sha256 = encode(sha256(canonical_statement), 'hex') "
            "AND algorithm = 'Ed25519' AND signature ~ '^[A-Za-z0-9_-]{86}$'",
            name="task_image_publication_envelopes_bytes_check",
        ),
        CheckConstraint(
            "isfinite(issued_at) AND isfinite(recorded_at) "
            "AND date_trunc('second', issued_at) = issued_at "
            "AND recorded_at >= issued_at - interval '5 seconds'",
            name="task_image_publication_envelopes_time_check",
        ),
        Index("task_image_publication_envelopes_key_idx", "key_id", "envelope_id"),
    )
    envelope_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    candidate_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    component: Mapped[str] = mapped_column(String(136), nullable=False)
    key_id: Mapped[str] = mapped_column(String(128), nullable=False)
    canonical_statement: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    statement_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(16), nullable=False)
    signature: Mapped[str] = mapped_column(String(86), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    distributed_keyset_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


class TaskImageExecutionGrant(Base):
    """Immutable preparation, fill-once signature and monotonic revocation.

    Trial/worker UUIDs are historical identities, not permanent live-object
    retention pins. Admission rechecks their actual current locked rows.
    """

    __tablename__ = "task_image_execution_grants"
    __table_args__ = (
        UniqueConstraint("claim_id", "revision", name="exec_grants_claim_revision_unique"),
        UniqueConstraint("grant_id", "revision", "claim_id", name="exec_grants_start_binding_unique"),
        CheckConstraint(
            "grant_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND claim_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND revision BETWEEN 1 AND 9007199254740991", name="exec_grants_identity_check",
        ),
        CheckConstraint(
            "octet_length(canonical_grant) BETWEEN 1 AND 262144 "
            "AND grant_sha256 = encode(sha256(canonical_grant), 'hex') "
            "AND ((canonical_envelope IS NULL AND envelope_sha256 IS NULL) "
            "OR (canonical_envelope IS NOT NULL AND envelope_sha256 IS NOT NULL "
            "AND octet_length(canonical_envelope) BETWEEN 1 AND 524288 "
            "AND envelope_sha256 = encode(sha256(canonical_envelope), 'hex')))",
            name="exec_grants_bytes_check",
        ),
        CheckConstraint(
            "isfinite(created_at) AND (revoked_at IS NULL OR "
            "(isfinite(revoked_at) AND revoked_at >= created_at))", name="exec_grants_time_check",
        ),
        Index("exec_grants_trial_idx", "trial_id", "claim_id", "revision"),
    )
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    revision: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    claim_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    trial_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    worker_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    operation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("task_image_publication_jobs.operation_id", ondelete="RESTRICT"), nullable=False,
    )
    keyset_version: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("task_image_publication_keysets.keyset_version", ondelete="RESTRICT"), nullable=False,
    )
    canonical_grant: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    grant_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_envelope: Mapped[bytes | None] = mapped_column(LargeBinary)
    envelope_sha256: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class TaskImageExecutionStart(Base):
    """One immutable consume per claim, including across grant refreshes."""

    __tablename__ = "task_image_execution_starts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["grant_id", "revision", "claim_id"],
            ["task_image_execution_grants." + name for name in ("grant_id", "revision", "claim_id")],
            name="exec_starts_grant_fkey", ondelete="RESTRICT",
        ),
        CheckConstraint(
            "start_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND request_sha256 ~ '^[0-9a-f]{64}$'", name="exec_starts_identity_check",
        ),
        CheckConstraint(
            "octet_length(canonical_receipt) BETWEEN 1 AND 8192 "
            "AND receipt_sha256 = encode(sha256(canonical_receipt), 'hex')", name="exec_starts_bytes_check",
        ),
        CheckConstraint(
            "isfinite(consumed_at) AND isfinite(expires_at) "
            "AND date_trunc('second', consumed_at) = consumed_at "
            "AND date_trunc('second', expires_at) = expires_at "
            "AND expires_at > consumed_at AND expires_at <= consumed_at + interval '30 seconds'",
            name="exec_starts_time_check",
        ),
    )
    claim_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    grant_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_receipt: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    receipt_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    consumed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class TaskImagePublicationEvidence(Base):
    """Append-only publication evidence for one exact materialization attempt."""

    __tablename__ = "task_image_publication_evidence"
    __table_args__ = (
        CheckConstraint(
            "attempt_number > 0 AND lease_epoch > 0",
            name="task_image_publication_evidence_counters_check",
        ),
        CheckConstraint(
            "length(component) BETWEEN 1 AND 256",
            name="task_image_publication_evidence_component_check",
        ),
        CheckConstraint(
            "length(registry_image) BETWEEN 1 AND 2048",
            name="task_image_publication_evidence_image_check",
        ),
        ForeignKeyConstraint(
            (
                "materialization_attempt_id",
                "materialization_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
            ),
            (
                "task_image_materialization_attempts.id",
                "task_image_materialization_attempts.materialization_id",
                "task_image_materialization_attempts.attempt_number",
                "task_image_materialization_attempts.lease_epoch",
                "task_image_materialization_attempts.builder_id",
            ),
            name="task_image_publication_evidence_attempt_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "materialization_attempt_id",
            "component",
            "registry_image",
            name="task_image_publication_evidence_replay_uidx",
        ),
        Index(
            "task_image_publication_evidence_materialization_idx",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "recorded_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    materialization_attempt_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    materialization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    builder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    component: Mapped[str] = mapped_column(Text, nullable=False)
    registry_image: Mapped[str] = mapped_column(Text, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TrialTaskImageMaterialization(Base):
    """Immutable readiness prerequisite attached to one submitted trial."""

    __tablename__ = "trial_task_image_materializations"
    __table_args__ = (
        Index(
            "trial_task_image_materializations_materialization_idx",
            "materialization_id",
        ),
    )

    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        primary_key=True,
    )
    materialization_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_image_materializations.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class Benchmark(Base):
    """One row per registered benchmark suite (Plan 13)."""

    __tablename__ = "benchmarks"
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_kind: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_locator: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_revision: Mapped[str] = mapped_column(Text, nullable=False)
    license_spdx: Mapped[str] = mapped_column(Text, nullable=False)
    license_url: Mapped[str] = mapped_column(Text, nullable=False)
    splits: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    # PR-1 (benchmark series): groups related benchmarks for the SPA's
    # multi-select dropdown. Convention: "aime", "swe-bench", …; NULL =
    # standalone, not part of a series. Disjoint variants (AIME by year)
    # are siblings under the same series so group-select unions cleanly.
    series: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    # Physical profile state. Historical profiles remain readable but cannot
    # be selected for new execution.
    execution_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'runnable'"),
        default="runnable",
    )
    profile_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    imported_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    imported_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class BenchmarkAlias(Base):
    """Public selector for a runnable immutable benchmark profile."""

    __tablename__ = "benchmark_aliases"

    alias: Mapped[str] = mapped_column(Text, primary_key=True)
    benchmark_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("benchmarks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    activated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskSet(Base):
    """Team-owned user TaskSet"""

    __tablename__ = "task_sets"
    __table_args__ = (
        CheckConstraint(
            "visibility IN ('private')",
            name="task_sets_visibility_check",
        ),
        CheckConstraint(
            "status IN ('materializing', 'ready', 'partial', 'failed', 'deleted')",
            name="task_sets_status_check",
        ),
        CheckConstraint(
            "cardinality(intents) > 0 AND "
            "intents <@ ARRAY['trajectory_generation', 'evaluation']::text[]",
            name="task_sets_intents_check",
        ),
        CheckConstraint(
            "slug <> '' AND slug = trim(slug) AND slug !~ '[./\\\\]'",
            name="task_sets_slug_check",
        ),
        CheckConstraint(
            "id = 'ts/' || owning_team_id::text || '/' || slug",
            name="task_sets_id_namespace_check",
        ),
        CheckConstraint(
            "task_count >= 0",
            name="task_sets_task_count_nonneg_check",
        ),
        UniqueConstraint("owning_team_id", "slug", name="task_sets_team_slug_uidx"),
        Index(
            "task_sets_team_visibility_status_idx",
            "owning_team_id",
            "visibility",
            "status",
        ),
        Index(
            "task_sets_evaluation_ready_idx",
            "evaluation_ready",
            postgresql_where=text("evaluation_ready = true"),
        ),
    )
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    owning_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'private'"),
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    status_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    intents: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    evaluation_ready: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    manifest_blob_uri: Mapped[str] = mapped_column(Text, nullable=False)
    task_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    soft_deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
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


class TaskSetManifest(Base):
    """Current manifest sidecar for a user TaskSet."""

    __tablename__ = "task_set_manifests"
    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        primary_key=True,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    verifier_blob_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    transform_blob_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
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


class TaskSetMaterializationJob(Base):
    """Queued materialization work for a user TaskSet (#242 sub-plan 2)."""

    __tablename__ = "task_set_materialization_jobs"
    __table_args__ = (
        CheckConstraint(
            "state IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'cancelled')",
            name="task_set_materialization_jobs_state_check",
        ),
        Index(
            "task_set_materialization_jobs_active_uidx",
            "task_set_id",
            unique=True,
            postgresql_where=text(
                "state IN ('queued', 'claimed', 'running')",
            ),
        ),
        Index(
            "task_set_materialization_jobs_queued_idx",
            "enqueued_at",
            postgresql_where=text("state = 'queued'"),
        ),
        Index(
            "task_set_materialization_jobs_active_heartbeat_idx",
            "lease_heartbeat_at",
            postgresql_where=text(
                "state IN ('claimed', 'running') AND lease_heartbeat_at IS NOT NULL",
            ),
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid4,
    )
    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        nullable=False,
    )
    owning_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("3"),
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    enqueued_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    lease_epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    lease_heartbeat_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    published_materialization_generation: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    claimed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_summary: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )
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


class TaskSetFenceCanaryAuthorization(Base):
    """One deployment-created, one-use authority for the #756 canary."""

    __tablename__ = "task_set_fence_canary_authorizations"
    __table_args__ = (
        CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{40}$'",
            name="task_set_fence_canary_authorizations_candidate_sha_check",
        ),
        CheckConstraint(
            "image_tag ~ '^staging(-[a-z0-9][a-z0-9_-]*)?-[0-9a-f]{7}$'",
            name="task_set_fence_canary_authorizations_image_tag_check",
        ),
        CheckConstraint(
            "expected_task_checksum ~ '^[0-9a-f]{64}$'",
            name="task_set_fence_canary_authorizations_checksum_check",
        ),
        CheckConstraint(
            "octet_length(nonce_digest) = 32",
            name="task_set_fence_canary_authorizations_nonce_digest_check",
        ),
        CheckConstraint(
            "(consumed_at IS NULL AND consumed_lease_epoch IS NULL) OR "
            "(consumed_at IS NOT NULL AND consumed_lease_epoch > 0)",
            name="task_set_fence_canary_authorizations_consumption_check",
        ),
        UniqueConstraint(
            "materialization_job_id",
            name="task_set_fence_canary_authorizations_job_uidx",
        ),
    )

    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        primary_key=True,
    )
    materialization_job_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_set_materialization_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    candidate_sha: Mapped[str] = mapped_column(String(40), nullable=False)
    image_tag: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_task_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_digest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    consumed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    consumed_lease_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)


class TaskSetGenerationGcCursor(Base):
    """Scheduling-only progress for bounded live-generation reconciliation."""

    __tablename__ = "task_set_generation_gc_cursors"
    __table_args__ = (
        CheckConstraint(
            "name = 'live-generation-gc'",
            name="task_set_generation_gc_cursors_name_check",
        ),
        CheckConstraint(
            "next_sweep >= 0",
            name="task_set_generation_gc_cursors_next_sweep_nonneg_check",
        ),
    )

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    next_sweep: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )


class Agent(Base):
    __tablename__ = "agents"
    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, primary_key=True)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint(
            "drain_state IN ('active', 'draining', 'drained')",
            name="workers_drain_state_check",
        ),
        CheckConstraint(
            "supported_work_kinds = ARRAY['trial']::text[] OR "
            "supported_work_kinds = ARRAY['trial','execution_attempt']::text[]",
            name="workers_supported_work_kinds_check",
        ),
        CheckConstraint("lease_epoch > 0", name="workers_lease_epoch_positive_check"),
        CheckConstraint(
            "input_cache_capacity_bytes >= 0 AND input_cache_reserved_bytes >= 0 "
            "AND input_cache_ready_bytes >= 0 AND input_cache_reserved_bytes <= input_cache_capacity_bytes "
            "AND input_cache_ready_bytes <= input_cache_capacity_bytes",
            name="workers_input_cache_capacity_check",
        ),
        CheckConstraint(
            "capability_snapshot_json IS NULL OR jsonb_typeof(capability_snapshot_json) = 'object'",
            name="workers_capability_snapshot_json_check",
        ),
        CheckConstraint(
            "(slurm_gpu_allocation_evidence_json IS NULL) = "
            "(slurm_gpu_allocation_evidence_digest IS NULL)",
            name="workers_slurm_gpu_evidence_group_check",
        ),
        CheckConstraint(
            "slurm_gpu_allocation_evidence_json IS NULL OR "
            "jsonb_typeof(slurm_gpu_allocation_evidence_json) = 'object'",
            name="workers_slurm_gpu_evidence_json_check",
        ),
        CheckConstraint(
            "slurm_gpu_allocation_evidence_digest IS NULL OR "
            "slurm_gpu_allocation_evidence_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="workers_slurm_gpu_evidence_digest_check",
        ),
        Index("idx_workers_drain_state", "drain_state"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    hostname: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    capabilities: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    supported_work_kinds: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        server_default=text("ARRAY['trial']::text[]"),
        default=lambda: ["trial"],
    )
    capability_snapshot_digest: Mapped[str | None] = mapped_column(Text)
    capability_snapshot_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    slurm_gpu_allocation_evidence_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    slurm_gpu_allocation_evidence_digest: Mapped[str | None] = mapped_column(Text)
    auth_token_hash: Mapped[bytes | None] = mapped_column(LargeBinary)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    pool_name: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'default'"))
    input_cache_capacity_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    input_cache_reserved_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    input_cache_ready_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    drain_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    drain_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    drain_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    drain_owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    registered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)


class SlurmWorkerJob(Base):
    __tablename__ = "slurm_worker_jobs"
    __table_args__ = (
        CheckConstraint(
            "state IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'stale')",
            name="slurm_worker_jobs_state_check",
        ),
        CheckConstraint(
            "slurm_cluster_id IN ('oldlab','gb10')",
            name="slurm_worker_jobs_cluster_check",
        ),
        CheckConstraint(
            "requested_cpus IS NULL OR requested_cpus > 0",
            name="slurm_worker_jobs_requested_cpus_positive_check",
        ),
        CheckConstraint(
            "requested_memory_mib IS NULL OR requested_memory_mib > 0",
            name="slurm_worker_jobs_requested_memory_positive_check",
        ),
        CheckConstraint(
            "requested_pids IS NULL OR requested_pids > 0",
            name="slurm_worker_jobs_requested_pids_positive_check",
        ),
        CheckConstraint(
            "requested_gpus >= 0",
            name="slurm_worker_jobs_requested_gpus_nonnegative_check",
        ),
        CheckConstraint(
            "candidate_sha IS NULL OR candidate_sha ~ '^[0-9a-f]{40}$'",
            name="slurm_worker_jobs_candidate_sha_check",
        ),
        CheckConstraint(
            "requested_concurrency > 0",
            name="slurm_worker_jobs_requested_concurrency_positive_check",
        ),
        Index(
            "slurm_worker_jobs_job_id_uidx",
            "slurm_cluster_id",
            "job_id",
            unique=True,
            postgresql_where=text("job_id IS NOT NULL"),
        ),
        Index(
            "slurm_worker_jobs_active_capacity_uidx",
            "environment",
            "pool_name",
            "nodelist",
            text("coalesce(requested_cpus, -1)"),
            text("coalesce(requested_memory_mib, -1)"),
            text("coalesce(requested_pids, -1)"),
            text("coalesce(requested_gpu_tres, '')"),
            "requested_gpus",
            "requested_concurrency",
            unique=True,
            postgresql_where=text("state IN ('pending', 'running')"),
        ),
        Index(
            "slurm_worker_jobs_sandbox_candidate_state_idx",
            "sandbox_identity",
            "candidate_sha",
            "state",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    slurm_cluster_id: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'oldlab'")
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    nodelist: Mapped[str] = mapped_column(Text, nullable=False)
    requested_cpus: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_memory_mib: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_pids: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_gpu_tres: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_gpus: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    requested_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    sandbox_identity: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_sha: Mapped[str | None] = mapped_column(String(40), nullable=True)
    compose_project: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    slurm_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    pending_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
    )
    redacted_env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    submission_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    stale_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
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


class GB10WorkerPoolDesiredState(Base):
    __tablename__ = "gb10_worker_pool_desired_states"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_pool_desired_states_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_pool_desired_states_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(image_tag)) > 0",
            name="gb10_worker_pool_desired_states_image_tag_nonempty_check",
        ),
        CheckConstraint(
            "max_concurrent > 0",
            name="gb10_worker_pool_desired_states_max_concurrent_positive_check",
        ),
        CheckConstraint(
            "length(trim(env_config_version)) > 0",
            name="gb10_worker_pool_desired_states_env_version_nonempty_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            name="gb10_worker_pool_desired_states_environment_pool_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    image_tag: Mapped[str] = mapped_column(Text, nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False)
    env_config_version: Mapped[str] = mapped_column(Text, nullable=False)
    source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    rollout_policy: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    target_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    host_intents: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    previous_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    previous_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
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


class GB10WorkerNodeStatus(Base):
    __tablename__ = "gb10_worker_node_statuses"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_node_statuses_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_node_statuses_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(hostname)) > 0",
            name="gb10_worker_node_statuses_hostname_nonempty_check",
        ),
        CheckConstraint(
            "current_max_concurrent IS NULL OR current_max_concurrent > 0",
            name="gb10_worker_node_statuses_current_max_positive_check",
        ),
        CheckConstraint(
            "desired_max_concurrent IS NULL OR desired_max_concurrent > 0",
            name="gb10_worker_node_statuses_desired_max_positive_check",
        ),
        CheckConstraint(
            "apply_state IN ('unknown', 'idle', 'applying', 'draining', 'stopped', 'applied', 'blocked', 'failed', 'rolled_back')",
            name="gb10_worker_node_statuses_apply_state_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            "hostname",
            name="gb10_worker_node_statuses_environment_pool_host_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    hostname: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
    )
    current_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    desired_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    apply_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'unknown'"),
        default="unknown",
    )
    last_apply_result: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    agent_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    compose_project_dir: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_git_dirty: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_apply_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
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


class WorkerPoolAutoscalerPolicy(Base):
    __tablename__ = "worker_pool_autoscaler_policies"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="worker_pool_autoscaler_policies_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="worker_pool_autoscaler_policies_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "actuator IN ('slurm', 'gb10')",
            name="worker_pool_autoscaler_policies_actuator_check",
        ),
        CheckConstraint(
            "min_slots >= 0",
            name="worker_pool_autoscaler_policies_min_slots_nonnegative_check",
        ),
        CheckConstraint(
            "max_slots >= min_slots",
            name="worker_pool_autoscaler_policies_max_slots_check",
        ),
        CheckConstraint(
            "scale_up_threshold_slots >= 0",
            name="worker_pool_autoscaler_policies_scale_up_threshold_check",
        ),
        CheckConstraint(
            "scale_down_idle_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_idle_check",
        ),
        CheckConstraint(
            "scale_up_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_up_cooldown_check",
        ),
        CheckConstraint(
            "scale_down_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_cooldown_check",
        ),
        CheckConstraint(
            "drain_timeout_seconds > 0",
            name="worker_pool_autoscaler_policies_drain_timeout_positive_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            name="worker_pool_autoscaler_policies_environment_pool_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    actuator: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    min_slots: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    scale_up_threshold_slots: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
    )
    scale_down_idle_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("600"),
    )
    scale_up_cooldown_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("60"),
    )
    scale_down_cooldown_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("300"),
    )
    drain_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("600"),
    )
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    disabled_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    actuator_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    # Prod-pressure drain intent for this (environment, pool). The CP request
    # handler is the sole writer of this field; for actuator="slurm" the
    # external autoscaler actor reads it to scancel/release SlurmWorkerJobs, and
    # the scheduler claim path reads it to fence new claims. NULL = no active
    # prod-pressure drain. See #892.
    prod_pressure_state: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    idle_since_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_desired_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_actual_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_pending_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_draining_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_occupied_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_queued_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_blocked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_blocked_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_scale_up_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_scale_down_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_decision_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
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


class PipelineScopedPolicyActivation(Base):
    """Authorization-scoped mutable capacity for immutable Pipeline policies."""

    __tablename__ = "pipeline_scoped_policy_activations"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="pipeline_policy_activation_environment_nonempty_check",
        ),
        CheckConstraint(
            "policy_id IN ('behavior-cpu-data','behavior-gpu-oldlab','behavior-gpu-gb10')",
            name="pipeline_policy_activation_policy_check",
        ),
        CheckConstraint(
            "policy_config_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_policy_activation_config_digest_check",
        ),
        CheckConstraint(
            "authority_kind IN ('acceptance','profile_calibration')",
            name="pipeline_policy_activation_authority_kind_check",
        ),
        CheckConstraint(
            "activation_epoch > 0", name="pipeline_policy_activation_epoch_positive_check"
        ),
        CheckConstraint(
            "state IN ('active','draining','disabled')",
            name="pipeline_policy_activation_state_check",
        ),
        CheckConstraint(
            "((state = 'active' AND desired_slots > 0) OR "
            "(state IN ('draining','disabled') AND desired_slots = 0))",
            name="pipeline_policy_activation_state_slots_check",
        ),
        CheckConstraint(
            "((policy_id = 'behavior-cpu-data' AND desired_slots <= 2) OR "
            "(policy_id IN ('behavior-gpu-oldlab','behavior-gpu-gb10') "
            "AND desired_slots <= 1))",
            name="pipeline_policy_activation_slot_ceiling_check",
        ),
        UniqueConstraint(
            "authority_kind",
            "authority_id",
            "policy_id",
            name="pipeline_policy_activation_authority_policy_uidx",
        ),
        UniqueConstraint(
            "environment",
            "policy_id",
            "activation_epoch",
            name="pipeline_policy_activation_environment_epoch_uidx",
        ),
        Index(
            "pipeline_policy_activation_active_policy_uidx",
            "environment",
            "policy_id",
            unique=True,
            postgresql_where=text("state = 'active'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    policy_id: Mapped[str] = mapped_column(Text, nullable=False)
    policy_config_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    authority_kind: Mapped[str] = mapped_column(Text, nullable=False)
    authority_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    activation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    desired_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    activated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAdmissionPolicy(Base):
    """One independently enforceable concurrency ceiling (#1552)."""

    __tablename__ = "execution_admission_policies"
    __table_args__ = (
        CheckConstraint(
            "scope_kind IN ('global','environment','region','team','batch',"
            "'execution_class','pool')",
            name="execution_admission_policies_scope_kind_check",
        ),
        CheckConstraint(
            "length(trim(scope_key)) BETWEEN 1 AND 120",
            name="execution_admission_policies_scope_key_check",
        ),
        CheckConstraint(
            "max_concurrent > 0",
            name="execution_admission_policies_max_concurrent_check",
        ),
        CheckConstraint(
            "active_count >= 0",
            name="execution_admission_policies_active_count_check",
        ),
        CheckConstraint(
            "(scope_kind = 'global' AND scope_key = '*') OR scope_kind <> 'global'",
            name="execution_admission_policies_global_key_check",
        ),
        UniqueConstraint(
            "scope_kind",
            "scope_key",
            name="execution_admission_policies_scope_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    scope_kind: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False)
    active_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    counter_updated_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAdmissionReservation(Base):
    """Durable slot held by a worker claim or service execution lease."""

    __tablename__ = "execution_admission_reservations"
    __table_args__ = (
        CheckConstraint(
            "attempt > 0",
            name="execution_admission_reservations_attempt_check",
        ),
        CheckConstraint(
            "execution_role IN ('attempt','verifier')",
            name="execution_admission_reservations_role_check",
        ),
        CheckConstraint(
            "owner_kind IN ("
            "'legacy_worker_claim','service_execution_lease','protected_worker_claim'"
            ")",
            name="execution_admission_reservations_owner_kind_check",
        ),
        CheckConstraint(
            "state IN ('active','released')",
            name="execution_admission_reservations_state_check",
        ),
        CheckConstraint(
            "(state = 'active' AND released_at IS NULL AND release_reason IS NULL) OR "
            "(state = 'released' AND released_at IS NOT NULL "
            "AND length(trim(release_reason)) > 0)",
            name="execution_admission_reservations_release_group_check",
        ),
        Index(
            "execution_admission_reservations_trial_attempt_role_uidx",
            "trial_id",
            "attempt",
            "execution_role",
            unique=True,
            postgresql_where=text(
                "state = 'active' OR owner_kind <> 'legacy_worker_claim' "
                "OR release_reason IS DISTINCT FROM 'trial_setup_refund'"
            ),
        ),
        Index(
            "execution_admission_reservations_active_scope_idx",
            "state",
            "pool_id",
            "environment",
            "team_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("trials.id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_role: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("batches.id", ondelete="RESTRICT")
    )
    environment: Mapped[str | None] = mapped_column(Text)
    region: Mapped[str | None] = mapped_column(Text)
    execution_class_id: Mapped[str | None] = mapped_column(Text)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    owner_kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    acquired_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    release_reason: Mapped[str | None] = mapped_column(Text)


class Batch(Base):
    """One row per submitted batch (Plan 19 + Plan 28 rename).

    The runner fans out a batch's `task_filter` into N trial
    submissions and back-links each via `trials.batch_id`. State
    transitions: submitted → running → finished | cancelled.

    Renamed from `Campaign` (and table `campaigns`) in migration 0011
    so the SPA, docs, and code all share Harbor's vocabulary
    (Task / Trial / Batch / Benchmark) without per-layer translation.
    """

    __tablename__ = "batches"
    __table_args__ = (
        CheckConstraint(
            "budget_policy IN ('none', 'soft', 'hard')",
            name="batches_budget_policy_check",
        ),
        CheckConstraint(
            "budget_usd IS NULL OR budget_usd >= 0",
            name="batches_budget_usd_nonnegative_check",
        ),
        CheckConstraint(
            "purpose IN ('evaluation', 'trajectory_generation')",
            name="batches_purpose_check",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # evaluation = native benchmarks, verification required;
    # trajectory_generation = TaskSets and/or benchmarks (transition),
    # verifier optional. Same trial harness either way.
    purpose: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="trajectory_generation",
        server_default=text("'trajectory_generation'"),
    )
    task_filter: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trial_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'submitted'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_by_token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    submitted_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_trial_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    # Plan 23: n-sampling. Runner submits n_per_task trials per matched
    # task; expected_trial_count = len(task_ids) * n_per_task.
    # When `combinations` is non-empty, this `n_per_task` is ignored —
    # each Combination carries its own n_per_task.
    n_per_task: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    # Plan 28 PR-3: backend selection at the batch level. Catalog
    # lives at `/api/v1/backends` (derived from worker capabilities).
    backend: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'docker'"),
        default="docker",
    )
    # Automatic Nebius execution freezes the deployment-owned profile at
    # Batch acceptance. Queued Trials must never be reinterpreted by a later
    # control-plane rollout. NULL covers Docker, legacy explicit bindings, and
    # batches created before automatic materialization existed.
    service_execution_runtime_profile: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    # Plan 28 PR-3: multi-(agent, model) combinations. Each entry is
    # `{agent_name, agent_model, n_per_task, label?}`. Empty list ⇒
    # single-combination behaviour (agent + model + n_per_task live
    # on trial_config / Batch.n_per_task as before).
    combinations: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #188 / #1109: operator/admin canaries can request
    # deterministic terminal coverage on named worker pools. The batch
    # runner emits one extra pool-pinned coverage trial per entry.
    # User POST /batches no longer admits this field; ordinary user
    # eval batches keep [].
    required_worker_pools: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Plan 28 PR-3: outcome separate from lifecycle `status`. NULL
    # until terminal. Computed by the batch_runner when transitioning
    # to a terminal lifecycle state. Values: succeeded /
    # partial_failed / all_failed / cancelled.
    result_status: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    # Fan-out failures happen before Control Plane accepts a child Trial.
    # Store them on the batch for retry suppression and user diagnostics.
    fanout_errors: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #298: failed-case reruns are represented as ordinary child
    # batches with exact trial coordinates to re-submit. The parent batch
    # remains immutable; service/UI detail views compute an effective rollup.
    rerun_of_batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    rerun_targets: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("jsonb_build_array()"),
        default=list,
    )
    # cluster-deploy.md §Schema additions: per-batch provider override.
    # When set, the gateway uses this connection (decrypts its API key,
    # forwards to base_url) for every trial in this batch instead of
    # the platform default. NULL = use platform default. The Trial
    # row's same-named column overrides this if both are set (per-trial
    # specificity wins).
    provider_connection_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id"),
        nullable=True,
    )
    provider_model_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    budget_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 6),
        nullable=True,
    )
    budget_policy: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'none'"),
        default="none",
    )
    budget_confirmed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    pre_run_estimated_cost_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 6),
        nullable=True,
    )
    pre_run_cost_estimate_source: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    pre_run_cost_estimate_confidence: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    budget_diagnostics: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #336: completed-run sharing is org-visible by default.
    # Team/private keeps the run in the owner team's boundary; org +
    # shared makes safe metadata visible in the org-wide Run Library.
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'org'"),
        default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # #672 family-runs: resolved spec persisted at batch-accept time;
    # NULL for non-family-run batches. See `docs/architecture/family-runs.md`.
    family_run_spec: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    # NULL keeps pre-profile batches on their original selector semantics.
    # Non-empty lists are an immutable task-selection snapshot for new batches.
    resolved_task_ids: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )

    @property
    def failure_reason(self) -> str | None:
        return "fanout_submit_failed" if self.fanout_errors else None

    @property
    def failure_message(self) -> str | None:
        if not self.fanout_errors:
            return None
        first = self.fanout_errors[0]
        task_id = first.get("task_id", "unknown task")
        status_code = first.get("status_code", "unknown status")
        detail = first.get("detail") or first.get("message") or "no detail"
        if len(self.fanout_errors) == 1:
            return f"task {task_id} submit failed: HTTP {status_code}: {detail}"
        return (
            f"{len(self.fanout_errors)} trial submissions failed; "
            f"first: task {task_id} HTTP {status_code}: {detail}"
        )


class Trial(Base):
    __tablename__ = "trials"
    __table_args__ = (
        CheckConstraint(
            "legacy_claim_id IS NULL OR "
            "legacy_claim_id <> '00000000-0000-0000-0000-000000000000'::uuid",
            name="trials_legacy_claim_id_nonzero",
        ),
        Index(
            "trials_legacy_claim_id_uidx",
            "legacy_claim_id",
            unique=True,
            postgresql_where=text("legacy_claim_id IS NOT NULL"),
        ),
        # #416 Slice 4: a terminal-successful trial must carry its
        # TrialResult. Writeback in `loom_worker.trial_runner` already
        # patches `result` before the `state='succeeded'` transition;
        # this constraint pins the invariant at the DB so any future
        # writeback regression fails fast at PATCH time instead of
        # producing rows the SPA/ATIF/#426 reward gate can't consume.
        CheckConstraint(
            "state != 'succeeded' OR result IS NOT NULL",
            name="trials_succeeded_has_result",
        ),
        CheckConstraint(
            "execution_route_generation >= 0",
            name="trials_execution_route_generation_check",
        ),
        CheckConstraint(
            "(execution_route_pool_name IS NULL AND execution_route_json IS NULL "
            "AND execution_route_sha256 IS NULL) OR "
            "(length(trim(execution_route_pool_name)) BETWEEN 1 AND 80 "
            "AND execution_route_generation > 0 "
            "AND execution_route_json->>'schema_version' = "
            "'loom.execution-routing-decision.v1' "
            "AND execution_route_json->>'selected_pool_id' = execution_route_pool_name "
            "AND execution_route_sha256 ~ '^sha256:[0-9a-f]{64}$')",
            name="trials_execution_route_group_check",
        ),
        CheckConstraint(
            "autoscaler_pool_name IS NULL OR execution_route_pool_name = autoscaler_pool_name",
            name="trials_autoscaler_route_pool_check",
        ),
        Index(
            "trials_queued_execution_route_idx",
            "execution_route_pool_name",
            "submitted_at",
            postgresql_where=text("state = 'queued'"),
        ),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False
    )
    task_id: Mapped[str] = mapped_column(String, ForeignKey("tasks.id"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    requires_caps: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    # Internal capacity routing for architecture-neutral queued trials. User
    # requirements remain immutable in requires_caps; this assignment makes
    # independent pool autoscalers count the demand exactly once.
    autoscaler_pool_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    autoscaler_pool_assigned_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    execution_route_generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    execution_route_pool_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The route-group constraint and mutation trigger distinguish SQL NULL
    # from JSON null when a queued route is withdrawn.
    execution_route_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    execution_route_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduling_observation: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    state: Mapped[str] = mapped_column(String, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    submit_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    submitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    # Last identity minted by the legacy scheduler's atomic claim transaction.
    # Attempt counts can be refunded; every new claim replaces this UUID.
    # NULL historical claims cannot be upgraded into signed execution authority.
    # This retained identity alone proves neither current ownership nor a start.
    legacy_claim_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    pre_start_heartbeat_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    cancellation_observed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id"),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    trajectory_index: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    # Plan 19: batch back-link + idempotency key. batch_id is
    # NULL for hand-submitted trials. idempotency_key uniqueness is
    # enforced via a partial unique index (`trials_idempotency_key_uidx`).
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Plan 23: which sample within (batch_id, task_id) this trial is.
    # Pairs with batch.n_per_task to support n-sampling fan-out.
    # 0 for hand-submitted trials and the only sample of a 1-per-task
    # batch — preserves pre-migration semantics.
    sample_idx: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    # Plan 28 PR-3: which Combination this trial belongs to within
    # its parent Batch. 0 for single-combination batches (matches
    # the pre-migration behaviour exactly).
    combination_idx: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    # cluster-deploy.md §Schema additions: per-trial provider override.
    # When set, the gateway uses this connection's decrypted API key
    # + base_url instead of the platform default. NULL = inherit from
    # parent Batch (also nullable) or platform default. Wins over the
    # Batch.provider_connection_id when both are set (per-trial
    # specificity).
    #
    # No cascade rule: provider_connections never hard-deletes today
    # (team_id FK is ON DELETE RESTRICT per migration 0018 self-
    # review). Soft-delete on the parent leaves this FK valid;
    # historical trials retain attribution for billing/audit.
    provider_connection_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id"),
        nullable=True,
    )
    provider_model_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    submitted_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'org'"),
        default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # #672 family-runs: populated when the batch opts into family-run
    # mode. Groups this trial with siblings in the same family so the
    # scheduler predicate can serialise them. NULL for non-family trials.
    family_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class NebiusRolloutGuard(Base):
    """Durable deployment pause in the independent platform database."""

    __tablename__ = "nebius_rollout_guard"
    __table_args__ = (CheckConstraint("id = 1", name="nebius_rollout_guard_singleton_check"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_sha: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())


class ServiceExecutionClass(Base):
    """Immutable provider-neutral service execution capability snapshot."""

    __tablename__ = "execution_classes"
    __table_args__ = (
        CheckConstraint(
            "id ~ '^[a-z0-9][a-z0-9-]{0,79}$'",
            name="execution_classes_id_check",
        ),
        CheckConstraint(
            "schema_version = 'loom.execution-class.v1'",
            name="execution_classes_schema_check",
        ),
        CheckConstraint(
            "spec_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="execution_classes_digest_check",
        ),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    spec_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    spec_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    retired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class ServiceExecutionTarget(Base):
    """Provider-bound desired and observed regional execution target."""

    __tablename__ = "execution_targets"
    __table_args__ = (
        CheckConstraint(
            "schema_version = 'loom.execution-target.v1'",
            name="execution_targets_schema_check",
        ),
        CheckConstraint(
            "desired_state IN ('disabled','active','draining','retired')",
            name="execution_targets_desired_check",
        ),
        CheckConstraint(
            "health_status IN ('unknown','healthy','unhealthy','stale')",
            name="execution_targets_health_check",
        ),
        Index(
            "execution_targets_placement_idx",
            "environment",
            "desired_state",
            "health_status",
            "region",
            "id",
        ),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    logical_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    execution_class_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_classes.id", ondelete="RESTRICT"), nullable=False
    )
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    spec_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    spec_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    region: Mapped[str] = mapped_column(Text, nullable=False)
    failure_domain: Mapped[str] = mapped_column(Text, nullable=False)
    data_residency: Mapped[str] = mapped_column(Text, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False, default="disabled")
    observed_state: Mapped[str] = mapped_column(Text, nullable=False, default="unknown")
    health_status: Mapped[str] = mapped_column(Text, nullable=False, default="unknown")
    health_observed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    health_error_code: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ServiceExecutionLease(Base):
    """One immutable identity and current authority generation per trial attempt."""

    __tablename__ = "execution_leases"
    __table_args__ = (
        CheckConstraint("attempt > 0", name="execution_leases_attempt_check"),
        CheckConstraint(
            "execution_role IN ('attempt','verifier')",
            name="execution_leases_role_check",
        ),
        CheckConstraint(
            "(execution_role = 'attempt' AND parent_lease_id IS NULL) OR "
            "(execution_role = 'verifier' AND parent_lease_id IS NOT NULL)",
            name="execution_leases_parent_role_check",
        ),
        CheckConstraint("generation > 0", name="execution_leases_generation_check"),
        CheckConstraint(
            "resource_generation > 0 AND resource_generation <= generation",
            name="execution_leases_resource_generation_check",
        ),
        CheckConstraint(
            "routing_generation > 0 AND length(selected_pool_id) BETWEEN 1 AND 80 "
            "AND routing_reason IN "
            "('fresh_executable_capacity','configured_scale_headroom','operator_pin',"
            "'preexisting_assignment','admin_target_binding') "
            "AND routing_decision_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="execution_leases_routing_identity_check",
        ),
        CheckConstraint(
            "desired_state IN ('create','start','cancel','timeout','retry','finalize',"
            "'delete_pending','deleted')",
            name="execution_leases_desired_check",
        ),
        CheckConstraint(
            "cleanup_state IN ('not_requested','pending','in_progress','complete','blocked')",
            name="execution_leases_cleanup_check",
        ),
        CheckConstraint(
            "(cleanup_state = 'not_requested' AND cleanup_requested_at IS NULL "
            "AND cleanup_deadline_at IS NULL) OR "
            "(cleanup_state <> 'not_requested' AND cleanup_requested_at IS NOT NULL "
            "AND cleanup_deadline_at IS NOT NULL "
            "AND cleanup_deadline_at > cleanup_requested_at)",
            name="execution_leases_cleanup_time_check",
        ),
        CheckConstraint(
            "(job_uid IS NULL OR length(job_uid) BETWEEN 1 AND 128) AND "
            "(pod_uid IS NULL OR length(pod_uid) BETWEEN 1 AND 128) AND "
            "(kubernetes_resource_version IS NULL OR "
            "length(kubernetes_resource_version) BETWEEN 1 AND 128) AND "
            "(node_name IS NULL OR length(node_name) BETWEEN 1 AND 253)",
            name="execution_leases_kubernetes_identity_bound_check",
        ),
        CheckConstraint(
            "(pod_started_at IS NULL OR pod_scheduled_at IS NULL OR "
            "pod_started_at >= pod_scheduled_at) AND "
            "(pod_terminated_at IS NULL OR pod_started_at IS NULL OR "
            "pod_terminated_at >= pod_started_at)",
            name="execution_leases_pod_time_order_check",
        ),
        CheckConstraint(
            "output_commit_state IN ('not_started','uploading','committed','unavailable')",
            name="execution_leases_output_state_check",
        ),
        CheckConstraint(
            "(output_commit_state='not_started' AND output_upload_session_id IS NULL "
            "AND output_generation IS NULL AND output_manifest_sha256 IS NULL "
            "AND output_marker_sha256 IS NULL AND output_committed_at IS NULL "
            "AND output_unavailable_reason IS NULL) OR "
            "(output_commit_state='uploading' AND output_upload_session_id IS NOT NULL "
            "AND output_generation > 0 AND output_manifest_sha256 IS NULL "
            "AND output_marker_sha256 IS NULL AND output_committed_at IS NULL "
            "AND output_unavailable_reason IS NULL) OR "
            "(output_commit_state='committed' AND output_upload_session_id IS NOT NULL "
            "AND output_generation > 0 "
            "AND output_manifest_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND output_marker_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND output_committed_at IS NOT NULL AND output_unavailable_reason IS NULL) OR "
            "(output_commit_state='unavailable' AND output_generation > 0 "
            "AND output_manifest_sha256 IS NULL AND output_marker_sha256 IS NULL "
            "AND output_committed_at IS NULL "
            "AND length(output_unavailable_reason) BETWEEN 1 AND 120)",
            name="execution_leases_output_group_check",
        ),
        CheckConstraint(
            "materialization_state IN "
            "('not_started','pending','running','committed','unavailable')",
            name="execution_leases_materialization_state_check",
        ),
        CheckConstraint(
            "materialization_attempts >= 0",
            name="execution_leases_materialization_attempts_check",
        ),
        CheckConstraint(
            "(materialization_state='not_started' AND materialization_attempts=0 "
            "AND materialization_next_attempt_at IS NULL "
            "AND materialization_claim_id IS NULL "
            "AND materialization_claim_expires_at IS NULL "
            "AND materialization_started_at IS NULL "
            "AND materialization_committed_at IS NULL "
            "AND materialization_error_code IS NULL "
            "AND materialization_error_message IS NULL "
            "AND canonical_trajectory_sha256 IS NULL "
            "AND canonical_atif_sha256 IS NULL) OR "
            "(materialization_state='pending' AND output_commit_state='committed' "
            "AND materialization_next_attempt_at IS NOT NULL "
            "AND materialization_claim_id IS NULL "
            "AND materialization_claim_expires_at IS NULL "
            "AND materialization_committed_at IS NULL "
            "AND canonical_trajectory_sha256 IS NULL "
            "AND canonical_atif_sha256 IS NULL) OR "
            "(materialization_state='running' AND output_commit_state='committed' "
            "AND materialization_attempts > 0 AND materialization_claim_id IS NOT NULL "
            "AND materialization_claim_expires_at IS NOT NULL "
            "AND materialization_started_at IS NOT NULL "
            "AND materialization_committed_at IS NULL "
            "AND canonical_trajectory_sha256 IS NULL "
            "AND canonical_atif_sha256 IS NULL) OR "
            "(materialization_state='committed' AND output_commit_state='committed' "
            "AND materialization_attempts > 0 AND materialization_claim_id IS NULL "
            "AND materialization_claim_expires_at IS NULL "
            "AND materialization_next_attempt_at IS NULL "
            "AND materialization_committed_at IS NOT NULL "
            "AND materialization_error_code IS NULL "
            "AND materialization_error_message IS NULL "
            "AND canonical_trajectory_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND canonical_atif_sha256 ~ '^sha256:[0-9a-f]{64}$') OR "
            "(materialization_state='unavailable' AND output_commit_state='committed' "
            "AND materialization_claim_id IS NULL "
            "AND materialization_claim_expires_at IS NULL "
            "AND materialization_next_attempt_at IS NULL "
            "AND materialization_committed_at IS NULL "
            "AND length(materialization_error_code) BETWEEN 1 AND 120 "
            "AND length(materialization_error_message) BETWEEN 1 AND 2000 "
            "AND canonical_trajectory_sha256 IS NULL "
            "AND canonical_atif_sha256 IS NULL)",
            name="execution_leases_materialization_group_check",
        ),
        CheckConstraint(
            "source_cleanup_state IN ('not_ready','retained','running','complete')",
            name="execution_leases_source_cleanup_state_check",
        ),
        CheckConstraint(
            "source_cleanup_attempts >= 0 AND ((source_cleanup_state='not_ready' "
            "AND source_retain_until IS NULL AND source_cleanup_claim_id IS NULL "
            "AND source_cleanup_claim_expires_at IS NULL "
            "AND source_cleanup_error_message IS NULL) OR "
            "(source_cleanup_state='retained' AND materialization_state='committed' "
            "AND source_retain_until IS NOT NULL AND source_cleanup_claim_id IS NULL "
            "AND source_cleanup_claim_expires_at IS NULL) OR "
            "(source_cleanup_state='running' AND materialization_state='committed' "
            "AND source_retain_until IS NOT NULL AND source_cleanup_attempts > 0 "
            "AND source_cleanup_claim_id IS NOT NULL "
            "AND source_cleanup_claim_expires_at IS NOT NULL) OR "
            "(source_cleanup_state='complete' AND materialization_state='committed' "
            "AND source_retain_until IS NOT NULL AND source_cleanup_attempts > 0 "
            "AND source_cleanup_claim_id IS NULL "
            "AND source_cleanup_claim_expires_at IS NULL "
            "AND source_cleanup_error_message IS NULL))",
            name="execution_leases_source_cleanup_group_check",
        ),
        UniqueConstraint(
            "trial_id",
            "attempt",
            "execution_role",
            name="execution_leases_trial_attempt_role_uidx",
        ),
        Index(
            "execution_leases_trial_authoritative_uidx",
            "trial_id",
            unique=True,
            postgresql_where=text("revoked_at IS NULL AND execution_role = 'attempt'"),
        ),
        Index("execution_leases_team_created_idx", "team_id", "created_at", "id"),
        Index(
            "execution_leases_materialization_queue_idx",
            "materialization_next_attempt_at",
            "output_committed_at",
            "id",
            postgresql_where=text("materialization_state IN ('pending','running')"),
        ),
        Index(
            "execution_leases_source_cleanup_queue_idx",
            "source_retain_until",
            "id",
            postgresql_where=text("source_cleanup_state IN ('retained','running')"),
        ),
        Index(
            "execution_leases_reconcile_idx",
            "desired_state",
            "observed_state",
            "updated_at",
            "id",
            postgresql_where=text("deleted_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("trials.id", ondelete="RESTRICT"), nullable=False
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_role: Mapped[str] = mapped_column(Text, nullable=False, default="attempt")
    parent_lease_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="RESTRICT"),
    )
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    resource_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    execution_class_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_classes.id", ondelete="RESTRICT"), nullable=False
    )
    target_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT")
    )
    routing_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    selected_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    routing_reason: Mapped[str] = mapped_column(Text, nullable=False)
    routing_decision_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    workload_requirements_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    workload_requirements_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_contract_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    runtime_contract_sha256: Mapped[str | None] = mapped_column(Text)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    observed_state: Mapped[str] = mapped_column(Text, nullable=False)
    cleanup_state: Mapped[str] = mapped_column(Text, nullable=False)
    cleanup_requested_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cleanup_deadline_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    provider_scope_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    namespace_name: Mapped[str] = mapped_column(Text, nullable=False)
    job_name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    execution_unit_key: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), nullable=False, unique=True
    )
    job_uid: Mapped[str | None] = mapped_column(Text)
    pod_uid: Mapped[str | None] = mapped_column(Text)
    pod_ip: Mapped[str | None] = mapped_column(INET)
    kubernetes_resource_version: Mapped[str | None] = mapped_column(Text)
    node_name: Mapped[str | None] = mapped_column(Text)
    deadline_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    pod_scheduled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    pod_started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    pod_terminated_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_reconciled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_event_ordinal: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0"), default=0
    )
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finalized_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    output_commit_state: Mapped[str] = mapped_column(
        Text, nullable=False, default="not_started", server_default=text("'not_started'")
    )
    output_upload_session_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
    )
    output_generation: Mapped[int | None] = mapped_column(BigInteger)
    output_manifest_sha256: Mapped[str | None] = mapped_column(Text)
    output_marker_sha256: Mapped[str | None] = mapped_column(Text)
    output_committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    output_unavailable_reason: Mapped[str | None] = mapped_column(Text)
    materialization_state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'not_started'"), default="not_started"
    )
    materialization_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0
    )
    materialization_next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True)
    )
    materialization_claim_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    materialization_claim_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True)
    )
    materialization_recovery_requested_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    materialization_started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    materialization_committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    materialization_error_code: Mapped[str | None] = mapped_column(Text)
    materialization_error_message: Mapped[str | None] = mapped_column(Text)
    canonical_trajectory_sha256: Mapped[str | None] = mapped_column(Text)
    canonical_atif_sha256: Mapped[str | None] = mapped_column(Text)
    source_cleanup_state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'not_ready'"), default="not_ready"
    )
    source_retain_until: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    source_cleanup_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0
    )
    source_cleanup_claim_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    source_cleanup_claim_expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True)
    )
    source_cleanup_error_message: Mapped[str | None] = mapped_column(Text)
    deleted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    error_class: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionPriceSnapshot(Base):
    """Immutable provider price evidence used for execution estimates."""

    __tablename__ = "execution_price_snapshots"
    __table_args__ = (
        CheckConstraint("currency = 'USD'", name="execution_price_snapshots_currency_check"),
        CheckConstraint(
            "base_microusd_per_hour >= 0 AND vcpu_microusd_per_hour >= 0 "
            "AND memory_gib_microusd_per_hour >= 0 "
            "AND ephemeral_storage_gib_microusd_per_hour >= 0 "
            "AND base_microusd_per_hour + vcpu_microusd_per_hour "
            "+ memory_gib_microusd_per_hour "
            "+ ephemeral_storage_gib_microusd_per_hour > 0",
            name="execution_price_snapshots_rates_check",
        ),
        CheckConstraint(
            "rate_card_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="execution_price_snapshots_digest_check",
        ),
        UniqueConstraint(
            "provider",
            "region",
            "sku",
            "source",
            "source_version",
            name="execution_price_snapshots_source_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    region: Mapped[str] = mapped_column(Text, nullable=False)
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'USD'"))
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_version: Mapped[str] = mapped_column(Text, nullable=False)
    source_uri: Mapped[str] = mapped_column(Text, nullable=False)
    effective_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    base_microusd_per_hour: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    vcpu_microusd_per_hour: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    memory_gib_microusd_per_hour: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    ephemeral_storage_gib_microusd_per_hour: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    rate_card_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    rate_card_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionTargetPriceBinding(Base):
    """Versioned operator selection of one immutable target price snapshot."""

    __tablename__ = "execution_target_price_bindings"

    target_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_targets.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    price_snapshot_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_price_snapshots.id", ondelete="RESTRICT"),
        nullable=False,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionBudgetPolicy(Base):
    """Race-safe daily/monthly paid-execution guardrail for a pool or target."""

    __tablename__ = "execution_budget_policies"
    __table_args__ = (
        CheckConstraint(
            "scope_kind IN ('pool','target')",
            name="execution_budget_policies_scope_check",
        ),
        CheckConstraint(
            "daily_limit_microusd > 0 "
            "AND monthly_limit_microusd >= daily_limit_microusd "
            "AND per_attempt_limit_microusd > 0 "
            "AND per_attempt_limit_microusd <= daily_limit_microusd "
            "AND max_estimate_duration_seconds BETWEEN 1 AND 604800",
            name="execution_budget_policies_limits_check",
        ),
        UniqueConstraint(
            "scope_kind",
            "scope_key",
            name="execution_budget_policies_scope_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    scope_kind: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    daily_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    monthly_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    per_attempt_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_estimate_duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    emergency_stop: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reason: Mapped[str | None] = mapped_column(Text)
    current_day: Mapped[date | None] = mapped_column(Date)
    current_month: Mapped[date | None] = mapped_column(Date)
    daily_reserved_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    daily_settled_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    monthly_reserved_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    monthly_settled_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionCostReservation(Base):
    """Worst-case paid-execution estimate retained until billing settlement."""

    __tablename__ = "execution_cost_reservations"
    __table_args__ = (
        UniqueConstraint(
            "trial_id",
            "attempt",
            "execution_role",
            name="execution_cost_reservations_trial_attempt_role_uidx",
        ),
        Index("execution_cost_reservations_pool_state_idx", "pool_id", "state", "acquired_at"),
        Index("execution_cost_reservations_team_time_idx", "team_id", "acquired_at"),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("trials.id", ondelete="RESTRICT"), nullable=False
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("batches.id", ondelete="RESTRICT")
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_role: Mapped[str] = mapped_column(Text, nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT"), nullable=False
    )
    price_snapshot_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_price_snapshots.id", ondelete="RESTRICT"),
        nullable=False,
    )
    estimate_duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_cpu_millis: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_ephemeral_storage_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_cost_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    estimate_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'reserved'"))
    acquired_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    terminal_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    settled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    billing_complete_through: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    actual_allocated_microusd: Mapped[int | None] = mapped_column(BigInteger)
    release_reason: Mapped[str | None] = mapped_column(Text)


class ExecutionCostReservationDebit(Base):
    """The period-specific effect of one reservation on one budget policy."""

    __tablename__ = "execution_cost_reservation_debits"

    reservation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_cost_reservations.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    policy_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_budget_policies.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    budget_day: Mapped[date] = mapped_column(Date, primary_key=True)
    budget_month: Mapped[date] = mapped_column(Date, nullable=False)
    reserved_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    actual_microusd: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionNodeCostRecord(Base):
    """Immutable provider node-bill evidence with explicit unallocated overhead."""

    __tablename__ = "execution_node_cost_records"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_record_id",
            name="execution_node_cost_records_provider_uidx",
        ),
        Index(
            "execution_node_cost_records_target_time_idx",
            "target_id",
            "interval_started_at",
            "interval_stopped_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT"), nullable=False
    )
    price_snapshot_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_price_snapshots.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    provider_record_id: Mapped[str] = mapped_column(Text, nullable=False)
    node_identity_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    interval_started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    interval_stopped_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    node_cpu_millis: Mapped[int] = mapped_column(Integer, nullable=False)
    node_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    node_ephemeral_storage_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_billed_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocated_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    idle_system_fragmentation_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'USD'"))
    billing_source: Mapped[str] = mapped_column(Text, nullable=False)
    billing_source_version: Mapped[str] = mapped_column(Text, nullable=False)
    allocation_method: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionNodeCostAllocation(Base):
    """One provider-bill allocation to one execution cost reservation."""

    __tablename__ = "execution_node_cost_allocations"
    __table_args__ = (
        Index(
            "execution_node_cost_allocations_reservation_idx",
            "cost_reservation_id",
            "node_cost_record_id",
        ),
    )

    node_cost_record_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_node_cost_records.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    cost_reservation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_cost_reservations.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_leases.id", ondelete="RESTRICT"), nullable=False
    )
    overlap_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    dominant_resource_fraction_ppb: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocated_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionCapacityPolicy(Base):
    """Target-scoped hard limits for Nebius Job provisioning."""

    __tablename__ = "execution_capacity_policies"

    target_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_targets.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    max_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    max_vcpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    node_cpu_millis: Mapped[int] = mapped_column(Integer, nullable=False)
    node_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    node_storage_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    max_pending_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    max_unschedulable_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    max_image_pull_backoff_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    max_create_per_minute: Mapped[int] = mapped_column(Integer, nullable=False)
    observation_max_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionCapacityObservation(Base):
    """Immutable provider, cluster, and autoscaler capacity readback."""

    __tablename__ = "execution_capacity_observations"
    __table_args__ = (
        UniqueConstraint(
            "target_id",
            "source",
            "source_version",
            name="execution_capacity_observations_source_uidx",
        ),
        UniqueConstraint(
            "target_id",
            "observed_at",
            name="execution_capacity_observations_target_time_uidx",
        ),
        Index(
            "execution_capacity_observations_target_time_idx",
            "target_id",
            "observed_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT"), nullable=False
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_version: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    provider_capacity_state: Mapped[str] = mapped_column(Text, nullable=False)
    provider_capacity_reason: Mapped[str | None] = mapped_column(Text)
    autoscaler_state: Mapped[str] = mapped_column(Text, nullable=False)
    autoscaler_reason: Mapped[str | None] = mapped_column(Text)
    provider_quota_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_quota_vcpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_quota_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_quota_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_used_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_used_vcpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_used_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_used_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    active_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    provisioned_vcpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provisioned_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provisioned_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocatable_cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocatable_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocatable_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pending_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    unschedulable_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    image_pull_backoff_jobs: Mapped[int] = mapped_column(Integer, nullable=False)
    pending_reasons_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    observation_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    observation_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionResourceCalibration(Base):
    """Immutable #1503-derived resource recommendation evidence."""

    __tablename__ = "execution_resource_calibrations"
    __table_args__ = (
        UniqueConstraint(
            "target_id",
            "source_pool_id",
            "resource_profile",
            "candidate_sha",
            "source_version",
            name="execution_resource_calibrations_source_uidx",
        ),
        Index(
            "execution_resource_calibrations_target_created_idx",
            "target_id",
            "created_at",
            "id",
        ),
        UniqueConstraint(
            "id",
            "target_id",
            name="execution_resource_calibrations_id_target_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT"), nullable=False
    )
    source_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_architecture: Mapped[str] = mapped_column(Text, nullable=False)
    resource_profile: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_sha: Mapped[str] = mapped_column(Text, nullable=False)
    source_version: Mapped[str] = mapped_column(Text, nullable=False)
    window_started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    window_stopped_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    trial_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    distinct_tasks: Mapped[int] = mapped_column(Integer, nullable=False)
    usage_records: Mapped[int] = mapped_column(Integer, nullable=False)
    incomplete_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_duration_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)
    peak_batch_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    throttled_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    oom_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_limit_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    percentiles_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    recommended_cpu_millis: Mapped[int] = mapped_column(Integer, nullable=False)
    recommended_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    recommended_ephemeral_storage_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    recommended_pids: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionResourceProfileBinding(Base):
    """Versioned target binding to one accepted immutable calibration."""

    __tablename__ = "execution_resource_profile_bindings"
    __table_args__ = (
        CheckConstraint(
            "(reason IS NULL OR length(trim(reason)) BETWEEN 1 AND 500) "
            "AND (NOT enabled OR reason IS NOT NULL)",
            name="execution_resource_profile_bindings_reason_check",
        ),
        CheckConstraint(
            "version > 0",
            name="execution_resource_profile_bindings_version_check",
        ),
        ForeignKeyConstraint(
            ["calibration_id", "target_id"],
            ["execution_resource_calibrations.id", "execution_resource_calibrations.target_id"],
            name="execution_resource_profile_bindings_calibration_fk",
            ondelete="RESTRICT",
        ),
    )

    target_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_targets.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    calibration_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionProvisioningAuthorization(Base):
    """One durable pre-Kubernetes-create capacity authorization."""

    __tablename__ = "execution_provisioning_authorizations"
    __table_args__ = (
        Index(
            "execution_provisioning_authorizations_target_state_idx",
            "target_id",
            "state",
            "authorized_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    target_id: Mapped[str] = mapped_column(
        Text, ForeignKey("execution_targets.id", ondelete="RESTRICT"), nullable=False
    )
    observation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_capacity_observations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    policy_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_cpu_millis: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_storage_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    incremental_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    incremental_vcpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False)
    incremental_memory_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    incremental_storage_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    decision_reason: Mapped[str] = mapped_column(Text, nullable=False)
    authorization_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'authorized'"))
    authorized_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class ServiceExecutionCommand(Base):
    """At-least-once durable actuator command."""

    __tablename__ = "execution_commands"
    __table_args__ = (
        CheckConstraint("generation > 0", name="execution_commands_generation_check"),
        CheckConstraint("sequence > 0", name="execution_commands_sequence_check"),
        UniqueConstraint(
            "lease_id",
            "generation",
            "sequence",
            name="execution_commands_lease_generation_sequence_uidx",
        ),
        Index(
            "execution_commands_delivery_idx",
            "state",
            "available_at",
            "created_at",
            "id",
            postgresql_where=text("state IN ('pending','leased')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="CASCADE"),
        nullable=False,
    )
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    command_type: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    delivery_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    claimed_by: Mapped[str | None] = mapped_column(Text)
    claim_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    acknowledged_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    acknowledgement_sha256: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ServiceExecutionEvent(Base):
    """Bounded replay-safe observation from one execution generation."""

    __tablename__ = "execution_events"
    __table_args__ = (
        CheckConstraint(
            "ordinal BETWEEN 1 AND 10000",
            name="execution_events_ordinal_check",
        ),
        UniqueConstraint(
            "lease_id",
            "generation",
            "ordinal",
            name="execution_events_lease_generation_ordinal_uidx",
        ),
        Index("execution_events_lease_observed_idx", "lease_id", "observed_at", "ordinal"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="CASCADE"),
        nullable=False,
    )
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_kind: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ServiceExecutionLeaseHistory(Base):
    """Database-triggered bounded desired/observed transition history."""

    __tablename__ = "execution_lease_history"
    __table_args__ = (
        CheckConstraint(
            "transition_ordinal BETWEEN 1 AND 20000",
            name="execution_lease_history_ordinal_check",
        ),
        UniqueConstraint(
            "lease_id",
            "transition_ordinal",
            name="execution_lease_history_lease_ordinal_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    lease_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_leases.id", ondelete="CASCADE"),
        nullable=False,
    )
    transition_ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    observed_state: Mapped[str] = mapped_column(Text, nullable=False)
    cleanup_state: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineRun(Base):
    """One immutable official-Recipe graph submission (#1211)."""

    __tablename__ = "pipeline_runs"
    __table_args__ = (
        CheckConstraint(
            "submission_policy IN ('ordinary', 'acceptance_authorization_only')",
            name="pipeline_runs_submission_policy_check",
        ),
        CheckConstraint(
            "state IN ('submitted', 'running', 'cancelling', 'finished')",
            name="pipeline_runs_state_check",
        ),
        CheckConstraint(
            "result IS NULL OR result IN "
            "('succeeded', 'partial_failed', 'failed', 'cancelled', 'budget_exhausted')",
            name="pipeline_runs_result_check",
        ),
        CheckConstraint(
            "(state = 'finished') = (result IS NOT NULL AND finished_at IS NOT NULL)",
            name="pipeline_runs_terminal_result_check",
        ),
        CheckConstraint(
            "state = 'finished' OR result_reason IS NULL",
            name="pipeline_runs_result_reason_state_check",
        ),
        CheckConstraint(
            "(official_submission_kind IS NULL AND official_submission_authority_id IS NULL "
            "AND official_submission_authority_snapshot_digest IS NULL "
            "AND official_submission_identity_digest IS NULL) OR "
            "(official_submission_kind IS NOT NULL AND official_submission_authority_id IS NOT NULL "
            "AND official_submission_authority_snapshot_digest IS NOT NULL "
            "AND official_submission_identity_digest IS NOT NULL)",
            name="pipeline_runs_official_origin_group_check",
        ),
        CheckConstraint(
            "(submission_policy = 'acceptance_authorization_only' AND "
            "acceptance_authorization_id IS NOT NULL AND acceptance_candidate_sha256 IS NOT NULL "
            "AND recipe_name = 'behavior-recovery-acceptance-preflight' AND recipe_version = 1 "
            "AND retry_of_pipeline_run_id IS NULL AND retry_from_stage_run_id IS NULL "
            "AND official_submission_kind IS NULL) OR "
            "(submission_policy = 'ordinary' AND acceptance_authorization_id IS NULL "
            "AND acceptance_candidate_sha256 IS NULL)",
            name="pipeline_runs_submission_origin_check",
        ),
        CheckConstraint(
            "(retry_of_pipeline_run_id IS NULL) = (retry_from_stage_run_id IS NULL)",
            name="pipeline_runs_retry_group_check",
        ),
        CheckConstraint(
            "official_submission_kind IS NULL OR "
            "(submission_policy = 'ordinary' AND retry_of_pipeline_run_id IS NULL "
            "AND retry_from_stage_run_id IS NULL)",
            name="pipeline_runs_official_not_retry_check",
        ),
        CheckConstraint("next_event_seq > 0", name="pipeline_runs_next_event_seq_positive"),
        CheckConstraint("lease_epoch >= 0", name="pipeline_runs_lease_epoch_nonnegative"),
        CheckConstraint(
            "(claimed_by IS NULL AND lease_expires_at IS NULL) OR "
            "(claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="pipeline_runs_controller_lease_group_check",
        ),
        CheckConstraint("version >= 0", name="pipeline_runs_version_nonnegative"),
        UniqueConstraint("team_id", "idempotency_key", name="pipeline_runs_team_idempotency_uidx"),
        Index(
            "pipeline_runs_official_identity_uidx",
            "team_id",
            "official_submission_kind",
            "official_submission_identity_digest",
            unique=True,
            postgresql_where=text("official_submission_identity_digest IS NOT NULL"),
        ),
        Index("pipeline_runs_team_created_idx", "team_id", text("created_at DESC"), "id"),
        Index("pipeline_runs_state_created_idx", "state", "created_at", "id"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    display_name: Mapped[str | None] = mapped_column(Text)
    submission_policy: Mapped[str] = mapped_column(Text, nullable=False)
    acceptance_authorization_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    acceptance_candidate_sha256: Mapped[str | None] = mapped_column(Text)
    official_submission_kind: Mapped[str | None] = mapped_column(Text)
    official_submission_authority_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    official_submission_authority_snapshot_digest: Mapped[str | None] = mapped_column(Text)
    official_submission_identity_digest: Mapped[str | None] = mapped_column(Text)
    recipe_name: Mapped[str] = mapped_column(Text, nullable=False)
    recipe_version: Mapped[int] = mapped_column(Integer, nullable=False)
    recipe_digest: Mapped[str] = mapped_column(Text, nullable=False)
    graph_spec_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    graph_spec_digest: Mapped[str] = mapped_column(Text, nullable=False)
    parameters_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    parameters_digest: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_inputs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    control_binding_snapshots_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    control_binding_snapshots_digest: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text(
            "'sha256:37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570'"
        ),
    )
    budget_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'submitted'"))
    result: Mapped[str | None] = mapped_column(Text)
    result_reason: Mapped[str | None] = mapped_column(Text)
    retry_of_pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="RESTRICT"), nullable=True
    )
    retry_from_stage_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "pipeline_stage_runs.id",
            ondelete="RESTRICT",
            use_alter=True,
            name="pipeline_runs_retry_from_stage_fk",
            deferrable=True,
            initially="DEFERRED",
        ),
        nullable=True,
    )
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    budget_exhausted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    next_event_seq: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    claimed_by: Mapped[str | None] = mapped_column(Text)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class ApiIdempotencyRecord(Base):
    """Durable, endpoint-scoped replay authority for public Pipeline mutations."""

    __tablename__ = "api_idempotency_records"
    __table_args__ = (
        CheckConstraint(
            "state IN ('pending','completed','failed')",
            name="api_idempotency_records_state_check",
        ),
        CheckConstraint(
            "request_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="api_idempotency_records_digest_check",
        ),
        CheckConstraint(
            "octet_length(idempotency_key) BETWEEN 1 AND 128 "
            "AND idempotency_key = btrim(idempotency_key) "
            "AND idempotency_key !~ '[[:cntrl:]]'",
            name="api_idempotency_records_key_check",
        ),
        CheckConstraint(
            "(state = 'pending' AND response_status IS NULL AND resource_id IS NULL "
            "AND completed_at IS NULL) OR "
            "(state IN ('completed','failed') AND response_status IS NOT NULL "
            "AND completed_at IS NOT NULL)",
            name="api_idempotency_records_response_check",
        ),
        CheckConstraint(
            "(team_id IS NULL AND endpoint IN "
            "('judge_profile_apply','provider_binding_apply')) OR team_id IS NOT NULL",
            name="api_idempotency_records_scope_check",
        ),
        Index(
            "api_idempotency_records_team_endpoint_key_uidx",
            "team_id",
            "endpoint",
            "idempotency_key",
            unique=True,
            postgresql_where=text("team_id IS NOT NULL"),
        ),
        Index(
            "api_idempotency_records_global_endpoint_key_uidx",
            "endpoint",
            "idempotency_key",
            unique=True,
            postgresql_where=text("team_id IS NULL"),
        ),
        Index("api_idempotency_records_expiry_idx", "expires_at"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE")
    )
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    resource_type: Mapped[str | None] = mapped_column(Text)
    resource_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    owner_token: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class JudgeExecutionProfile(Base):
    """One immutable version of a server-owned offline-judge profile."""

    __tablename__ = "judge_execution_profiles"
    __table_args__ = (
        CheckConstraint("version > 0", name="judge_execution_profiles_version_positive"),
        CheckConstraint(
            "status IN ('active','disabled')",
            name="judge_execution_profiles_status_check",
        ),
        CheckConstraint(
            "snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="judge_execution_profiles_digest_check",
        ),
        CheckConstraint(
            "octet_length(snapshot_bytes) > 1 AND "
            "get_byte(snapshot_bytes, octet_length(snapshot_bytes)-1)=10",
            name="judge_execution_profiles_document_check",
        ),
        UniqueConstraint(
            "recipe_name",
            "recipe_version",
            "profile_name",
            "version",
            name="judge_execution_profiles_identity_uidx",
        ),
        Index(
            "judge_execution_profiles_current_uidx",
            "recipe_name",
            "recipe_version",
            "profile_name",
            unique=True,
            postgresql_where=text("is_current"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    profile_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    recipe_name: Mapped[str] = mapped_column(Text, nullable=False)
    recipe_version: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="RESTRICT"),
        nullable=False,
    )
    agent_adapter: Mapped[str] = mapped_column(Text, nullable=False)
    recipe_digest: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    snapshot_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_team_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), nullable=False
    )
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_by: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_by: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class RecipeProviderBinding(Base):
    """One immutable version of the recipe-owned primitive Provider binding."""

    __tablename__ = "recipe_provider_bindings"
    __table_args__ = (
        CheckConstraint("version > 0", name="recipe_provider_bindings_version_positive"),
        CheckConstraint(
            "status IN ('active','disabled')", name="recipe_provider_bindings_status_check"
        ),
        CheckConstraint(
            "snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="recipe_provider_bindings_digest_check",
        ),
        CheckConstraint(
            "octet_length(snapshot_bytes) > 1 AND "
            "get_byte(snapshot_bytes, octet_length(snapshot_bytes)-1)=10",
            name="recipe_provider_bindings_document_check",
        ),
        UniqueConstraint(
            "recipe_name",
            "recipe_version",
            "logical_name",
            "version",
            name="recipe_provider_bindings_identity_uidx",
        ),
        Index(
            "recipe_provider_bindings_current_uidx",
            "recipe_name",
            "recipe_version",
            "logical_name",
            unique=True,
            postgresql_where=text("is_current"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    binding_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    recipe_name: Mapped[str] = mapped_column(Text, nullable=False)
    recipe_version: Mapped[int] = mapped_column(Integer, nullable=False)
    logical_name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="RESTRICT"),
        nullable=False,
    )
    recipe_digest: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    snapshot_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_team_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), nullable=False
    )
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_by: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_by: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class PipelineRunControlBinding(Base):
    """The exact immutable source snapshot selected for one PipelineRun node."""

    __tablename__ = "pipeline_run_control_bindings"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('judge_profile','provider')", name="pipeline_run_control_bindings_kind_check"
        ),
        CheckConstraint(
            "snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_run_control_bindings_digest_check",
        ),
        CheckConstraint(
            "octet_length(snapshot_bytes) > 1 AND "
            "get_byte(snapshot_bytes, octet_length(snapshot_bytes)-1)=10",
            name="pipeline_run_control_bindings_document_check",
        ),
        UniqueConstraint(
            "pipeline_run_id", "logical_name", name="pipeline_run_control_bindings_name_uidx"
        ),
        UniqueConstraint(
            "pipeline_run_id", "node_key", name="pipeline_run_control_bindings_node_uidx"
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    logical_name: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    node_key: Mapped[str] = mapped_column(Text, nullable=False)
    source_object_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    source_version: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    snapshot_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_request_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_cost_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    per_call_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class PipelineRunGpuBackendSelection(Base):
    __tablename__ = "pipeline_run_gpu_backend_selections"
    __table_args__ = (
        CheckConstraint(
            "scope IN ('all_gpu_nodes','oldlab_preflight','gb10_preflight')",
            name="pipeline_gpu_selection_scope_check",
        ),
        CheckConstraint(
            "variant_id IN ('gb10-shared-1gpu','oldlab-rtx5080-2gpu')",
            name="pipeline_gpu_selection_variant_check",
        ),
        CheckConstraint(
            "policy_id IN ('behavior-gpu-gb10','behavior-gpu-oldlab')",
            name="pipeline_gpu_selection_policy_check",
        ),
        CheckConstraint(
            "selection_source IN "
            "('recipe_hash','acceptance_authority','profile_calibration_authority')",
            name="pipeline_gpu_selection_source_check",
        ),
        CheckConstraint(
            "gpu_backend_selection_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_gpu_selection_digest_check",
        ),
        CheckConstraint(
            "jsonb_typeof(selection_json) = 'object' AND "
            "octet_length(selection_bytes) > 1 AND "
            "get_byte(selection_bytes, octet_length(selection_bytes) - 1) = 10",
            name="pipeline_gpu_selection_document_check",
        ),
        CheckConstraint(
            "(variant_id = 'gb10-shared-1gpu' AND policy_id = 'behavior-gpu-gb10') "
            "OR (variant_id = 'oldlab-rtx5080-2gpu' "
            "AND policy_id = 'behavior-gpu-oldlab')",
            name="pipeline_gpu_selection_variant_policy_check",
        ),
        CheckConstraint(
            "(selection_source = 'recipe_hash' AND scope = 'all_gpu_nodes') OR "
            "(selection_source <> 'recipe_hash' AND (scope = 'all_gpu_nodes' OR "
            "((variant_id = 'gb10-shared-1gpu' AND scope = 'gb10_preflight') OR "
            "(variant_id = 'oldlab-rtx5080-2gpu' AND scope = 'oldlab_preflight'))))",
            name="pipeline_gpu_selection_scope_authority_check",
        ),
        UniqueConstraint("pipeline_run_id", "scope", name="pipeline_gpu_selection_run_scope_uidx"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    variant_id: Mapped[str] = mapped_column(Text, nullable=False)
    policy_id: Mapped[str] = mapped_column(Text, nullable=False)
    selection_source: Mapped[str] = mapped_column(Text, nullable=False)
    selected_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    selection_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    selection_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    gpu_backend_selection_sha256: Mapped[str] = mapped_column(Text, nullable=False)


class PipelineStage1SmokeAuthorization(Base):
    """One candidate-bound, two-phase Stage 1 live mutation authority."""

    __tablename__ = "pipeline_stage1_smoke_authorizations"
    __table_args__ = (
        CheckConstraint(
            "state IN ('capacity_pending','capacity_draining','capacity_aborted',"
            "'submitted','running','cleanup_required','cleanup_draining','accepted','rejected')",
            name="pipeline_stage1_smoke_authorizations_state_check",
        ),
        CheckConstraint(
            "candidate_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "authorization_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "nonce_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "capacity_request_digest ~ '^sha256:[0-9a-f]{64}$' AND "
            "capacity_signature_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "(preflight_sha256 IS NULL OR preflight_sha256 ~ '^sha256:[0-9a-f]{64}$') AND "
            "(execute_request_digest IS NULL OR "
            "execute_request_digest ~ '^sha256:[0-9a-f]{64}$') AND "
            "(execute_signature_sha256 IS NULL OR "
            "execute_signature_sha256 ~ '^sha256:[0-9a-f]{64}$')",
            name="pipeline_stage1_smoke_authorizations_digest_check",
        ),
        CheckConstraint(
            "length(environment) BETWEEN 1 AND 256 AND "
            "length(capacity_idempotency_key) BETWEEN 1 AND 128 AND "
            "capacity_idempotency_key = btrim(capacity_idempotency_key) AND "
            "capacity_idempotency_key ~ '^[ -~]+$' AND "
            "(execute_idempotency_key IS NULL OR ("
            "length(execute_idempotency_key) BETWEEN 1 AND 128 AND "
            "execute_idempotency_key = btrim(execute_idempotency_key) AND "
            "execute_idempotency_key ~ '^[ -~]+$')) AND "
            "capacity_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$' AND "
            "(execute_signature_key_id IS NULL OR "
            "execute_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$') AND "
            "(cleanup_begin_signature_key_id IS NULL OR "
            "cleanup_begin_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$') AND "
            "(cleanup_signature_key_id IS NULL OR "
            "cleanup_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$')",
            name="pipeline_stage1_smoke_authorizations_identity_check",
        ),
        CheckConstraint(
            "octet_length(candidate_bytes) BETWEEN 2 AND 1048576 AND "
            "get_byte(candidate_bytes, octet_length(candidate_bytes)-1)=10 AND "
            "octet_length(authorization_bytes) > 1 AND "
            "get_byte(authorization_bytes, octet_length(authorization_bytes)-1)=10 AND "
            "((preflight_json IS NULL AND preflight_bytes IS NULL) OR "
            "(jsonb_typeof(preflight_json) = 'object' AND octet_length(preflight_bytes) > 1 "
            "AND get_byte(preflight_bytes, octet_length(preflight_bytes)-1)=10)) AND "
            "((cleanup_begin_json IS NULL AND cleanup_begin_bytes IS NULL) OR "
            "(jsonb_typeof(cleanup_begin_json) = 'object' AND "
            "octet_length(cleanup_begin_bytes) > 1 AND "
            "get_byte(cleanup_begin_bytes, octet_length(cleanup_begin_bytes)-1)=10))",
            name="pipeline_stage1_smoke_authorizations_document_check",
        ),
        CheckConstraint(
            "(state IN ('capacity_pending','capacity_draining','capacity_aborted') AND "
            "preflight_json IS NULL AND "
            "preflight_bytes IS NULL AND preflight_sha256 IS NULL AND "
            "execute_idempotency_key IS NULL AND execute_request_digest IS NULL AND "
            "execute_signature_key_id IS NULL AND execute_signature_sha256 IS NULL AND "
            "pipeline_run_id IS NULL AND consumed_at IS NULL) OR "
            "(state NOT IN ('capacity_pending','capacity_draining','capacity_aborted') AND "
            "preflight_json IS NOT NULL AND "
            "preflight_bytes IS NOT NULL AND preflight_sha256 IS NOT NULL AND "
            "execute_idempotency_key IS NOT NULL AND execute_request_digest IS NOT NULL AND "
            "execute_signature_key_id IS NOT NULL AND execute_signature_sha256 IS NOT NULL AND "
            "pipeline_run_id IS NOT NULL AND consumed_at IS NOT NULL)",
            name="pipeline_stage1_smoke_authorizations_execution_phase_check",
        ),
        CheckConstraint(
            "(state IN ('capacity_pending','submitted','running','cleanup_required') AND "
            "cleanup_begin_json IS NULL AND cleanup_begin_bytes IS NULL AND "
            "cleanup_begin_sha256 IS NULL AND cleanup_begin_signature_key_id IS NULL AND "
            "cleanup_begin_signature_sha256 IS NULL AND cleanup_began_at IS NULL) OR "
            "(state IN ('capacity_draining','capacity_aborted','cleanup_draining',"
            "'accepted','rejected') AND "
            "cleanup_begin_json IS NOT NULL AND cleanup_begin_bytes IS NOT NULL AND "
            "cleanup_begin_sha256 IS NOT NULL AND cleanup_begin_signature_key_id IS NOT NULL AND "
            "cleanup_begin_signature_sha256 IS NOT NULL AND cleanup_began_at IS NOT NULL)",
            name="pipeline_stage1_smoke_authorizations_cleanup_phase_check",
        ),
        CheckConstraint(
            "expires_at > authorized_at AND cleanup_deadline > start_by",
            name="pipeline_stage1_smoke_authorizations_window_check",
        ),
        CheckConstraint("version >= 0", name="pipeline_stage1_smoke_authorizations_version_check"),
        CheckConstraint(
            "(evidence_sha256 IS NULL OR evidence_sha256 ~ '^sha256:[0-9a-f]{64}$') AND "
            "(cleanup_begin_sha256 IS NULL OR "
            "cleanup_begin_sha256 ~ '^sha256:[0-9a-f]{64}$') AND "
            "(cleanup_begin_signature_sha256 IS NULL OR "
            "cleanup_begin_signature_sha256 ~ '^sha256:[0-9a-f]{64}$') AND "
            "(cleanup_sha256 IS NULL OR cleanup_sha256 ~ '^sha256:[0-9a-f]{64}$') AND "
            "(cleanup_signature_sha256 IS NULL OR "
            "cleanup_signature_sha256 ~ '^sha256:[0-9a-f]{64}$')",
            name="pipeline_stage1_smoke_authorizations_result_digest_check",
        ),
        CheckConstraint(
            "(state IN ('capacity_pending','capacity_draining','capacity_aborted',"
            "'submitted','running') AND "
            "evidence_sha256 IS NULL) OR "
            "(state IN ('cleanup_required','cleanup_draining','accepted','rejected') AND "
            "evidence_sha256 IS NOT NULL)",
            name="pipeline_stage1_smoke_authorizations_evidence_phase_check",
        ),
        CheckConstraint(
            "(state IN ('capacity_aborted','accepted','rejected') AND cleanup_sha256 IS NOT NULL "
            "AND cleanup_signature_key_id IS NOT NULL "
            "AND cleanup_signature_sha256 IS NOT NULL AND finished_at IS NOT NULL) OR "
            "(state NOT IN ('capacity_aborted','accepted','rejected') AND cleanup_sha256 IS NULL "
            "AND cleanup_signature_key_id IS NULL "
            "AND cleanup_signature_sha256 IS NULL AND finished_at IS NULL)",
            name="pipeline_stage1_smoke_authorizations_terminal_check",
        ),
        UniqueConstraint(
            "candidate_sha256", name="pipeline_stage1_smoke_authorizations_candidate_uidx"
        ),
        UniqueConstraint("nonce_sha256", name="pipeline_stage1_smoke_authorizations_nonce_uidx"),
        UniqueConstraint(
            "team_id",
            "capacity_idempotency_key",
            name="pipeline_stage1_smoke_authorizations_team_capacity_idempotency_uidx",
        ),
        UniqueConstraint(
            "team_id",
            "execute_idempotency_key",
            name="pipeline_stage1_smoke_authorizations_team_execute_idempotency_uidx",
        ),
        Index(
            "pipeline_stage1_smoke_authorizations_active_environment_uidx",
            "environment",
            unique=True,
            postgresql_where=text(
                "state IN ('capacity_pending','capacity_draining','submitted','running',"
                "'cleanup_required','cleanup_draining')"
            ),
        ),
    )

    authorization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    operator_user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    candidate_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    candidate_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    authorization_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    authorization_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    authorization_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    preflight_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    preflight_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    preflight_sha256: Mapped[str | None] = mapped_column(Text)
    nonce_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    capacity_idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    capacity_request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    capacity_signature_key_id: Mapped[str] = mapped_column(Text, nullable=False)
    capacity_signature_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    execute_idempotency_key: Mapped[str | None] = mapped_column(Text)
    execute_request_digest: Mapped[str | None] = mapped_column(Text)
    execute_signature_key_id: Mapped[str | None] = mapped_column(Text)
    execute_signature_sha256: Mapped[str | None] = mapped_column(Text)
    policy_activation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_scoped_policy_activations.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_runs.id", ondelete="RESTRICT"),
        unique=True,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str | None] = mapped_column(Text)
    cleanup_begin_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    cleanup_begin_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    cleanup_begin_sha256: Mapped[str | None] = mapped_column(Text)
    cleanup_begin_signature_key_id: Mapped[str | None] = mapped_column(Text)
    cleanup_begin_signature_sha256: Mapped[str | None] = mapped_column(Text)
    cleanup_began_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cleanup_sha256: Mapped[str | None] = mapped_column(Text)
    cleanup_signature_key_id: Mapped[str | None] = mapped_column(Text)
    cleanup_signature_sha256: Mapped[str | None] = mapped_column(Text)
    authorized_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    start_by: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    cleanup_deadline: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineStage1SmokeEvent(Base):
    """Append-only canonical evidence for one Stage 1 authority."""

    __tablename__ = "pipeline_stage1_smoke_events"
    __table_args__ = (
        CheckConstraint("seq > 0", name="pipeline_stage1_smoke_events_seq_check"),
        CheckConstraint(
            "event_kind IN ('capacity_preflight_started','live_action_consumed',"
            "'evidence_recorded','cleanup_started','cleanup_complete','capacity_aborted',"
            "'accepted','rejected')",
            name="pipeline_stage1_smoke_events_kind_check",
        ),
        CheckConstraint(
            "payload_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_stage1_smoke_events_digest_check",
        ),
        CheckConstraint(
            "octet_length(payload_bytes) > 1 AND "
            "get_byte(payload_bytes, octet_length(payload_bytes)-1)=10",
            name="pipeline_stage1_smoke_events_document_check",
        ),
        UniqueConstraint(
            "authorization_id", "event_kind", name="pipeline_stage1_smoke_events_kind_uidx"
        ),
    )

    authorization_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_stage1_smoke_authorizations.authorization_id", ondelete="CASCADE"),
        primary_key=True,
    )
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_kind: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineStageRun(Base):
    __tablename__ = "pipeline_stage_runs"
    __table_args__ = (
        CheckConstraint(
            "node_kind IN ('container', 'gate')", name="pipeline_stage_runs_kind_check"
        ),
        CheckConstraint(
            "state IN ('blocked','ready','queued','claimed','running','retry_wait',"
            "'succeeded','failed','cancelled','skipped')",
            name="pipeline_stage_runs_state_check",
        ),
        CheckConstraint(
            "(resolved_execution_spec_json IS NULL AND resolved_execution_spec_bytes IS NULL "
            "AND execution_spec_digest IS NULL) OR "
            "(resolved_execution_spec_json IS NOT NULL AND resolved_execution_spec_bytes IS NOT NULL "
            "AND execution_spec_digest IS NOT NULL)",
            name="pipeline_stage_runs_execution_spec_group_check",
        ),
        CheckConstraint(
            "(resolved_input_bindings_json IS NULL AND resolved_input_bindings_digest IS NULL) OR "
            "(resolved_input_bindings_json IS NOT NULL AND resolved_input_bindings_digest IS NOT NULL)",
            name="pipeline_stage_runs_bindings_group_check",
        ),
        CheckConstraint(
            "(resolved_execution_spec_json IS NULL) = (resolved_input_bindings_json IS NULL)",
            name="pipeline_stage_runs_frozen_groups_together_check",
        ),
        CheckConstraint(
            "(resource_profile_json IS NULL) = (resource_profile_digest IS NULL)",
            name="pipeline_stage_runs_resource_group_check",
        ),
        CheckConstraint(
            "(request_renderer_json IS NULL) = (request_renderer_digest IS NULL)",
            name="pipeline_stage_runs_renderer_group_check",
        ),
        CheckConstraint(
            "(fanout_expansion_id IS NULL AND fanout_parameters_json IS NULL "
            "AND fanout_item_json IS NULL AND fanout_item_digest IS NULL) OR "
            "(fanout_expansion_id IS NOT NULL AND fanout_parameters_json IS NOT NULL "
            "AND fanout_item_json IS NOT NULL AND fanout_item_digest IS NOT NULL)",
            name="pipeline_stage_runs_fanout_group_check",
        ),
        CheckConstraint(
            "(node_kind = 'gate' AND gate_subject_stage_run_id IS NOT NULL "
            "AND resolved_execution_spec_json IS NULL AND resource_profile_json IS NULL "
            "AND resolved_input_bindings_json IS NULL AND fanout_expansion_id IS NULL "
            "AND request_renderer_json IS NULL AND failure_policy IS NULL "
            "AND latest_checkpoint_artifact_id IS NULL AND next_attempt_at IS NULL "
            "AND claimed_at IS NULL AND started_at IS NULL AND attempt_count = 0) OR "
            "(node_kind = 'container' AND gate_subject_stage_run_id IS NULL "
            "AND resource_profile_json IS NOT NULL AND failure_policy IN ('fail_run','continue'))",
            name="pipeline_stage_runs_kind_fields_check",
        ),
        CheckConstraint(
            "node_kind != 'container' OR NOT (state IN "
            "('ready','queued','claimed','running','retry_wait','succeeded') OR attempt_count > 0) OR "
            "(resolved_execution_spec_json IS NOT NULL AND resolved_input_bindings_json IS NOT NULL)",
            name="pipeline_stage_runs_ready_frozen_check",
        ),
        CheckConstraint(
            "node_kind != 'gate' OR state IN ('blocked','succeeded','skipped')",
            name="pipeline_stage_runs_gate_state_check",
        ),
        CheckConstraint(
            "node_kind != 'container' OR "
            "((shard_key = 'singleton') = (fanout_expansion_id IS NULL))",
            name="pipeline_stage_runs_shard_expansion_check",
        ),
        CheckConstraint(
            "attempt_count BETWEEN 0 AND 3", name="pipeline_stage_runs_attempt_count_range"
        ),
        CheckConstraint(
            "state IN ('succeeded','failed','cancelled','skipped') "
            "OR (domain_outcome IS NULL AND reason_code IS NULL)",
            name="pipeline_stage_runs_terminal_fields_check",
        ),
        CheckConstraint(
            "domain_outcome IS NULL OR state = 'succeeded'",
            name="pipeline_stage_runs_outcome_state_check",
        ),
        CheckConstraint("version >= 0", name="pipeline_stage_runs_version_nonnegative"),
        CheckConstraint(
            "(state IN ('succeeded','failed','cancelled','skipped')) = (finished_at IS NOT NULL)",
            name="pipeline_stage_runs_terminal_timestamp_check",
        ),
        UniqueConstraint(
            "pipeline_run_id", "node_key", "shard_key", name="pipeline_stage_runs_identity_uidx"
        ),
        UniqueConstraint("pipeline_run_id", "id", name="pipeline_stage_runs_run_id_uidx"),
        UniqueConstraint(
            "pipeline_run_id", "id", "shard_key", name="pipeline_stage_runs_run_id_shard_uidx"
        ),
        Index(
            "pipeline_stage_runs_state_retry_idx", "state", "next_attempt_at", "created_at", "id"
        ),
        Index("pipeline_stage_runs_run_node_state_idx", "pipeline_run_id", "node_key", "state"),
        Index("pipeline_stage_runs_expansion_shard_idx", "fanout_expansion_id", "shard_key"),
        ForeignKeyConstraint(
            ["pipeline_run_id", "gate_subject_stage_run_id", "shard_key"],
            [
                "pipeline_stage_runs.pipeline_run_id",
                "pipeline_stage_runs.id",
                "pipeline_stage_runs.shard_key",
            ],
            name="pipeline_stage_runs_gate_subject_same_shard_fk",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["pipeline_run_id", "fanout_expansion_id"],
            ["pipeline_fanout_expansions.pipeline_run_id", "pipeline_fanout_expansions.id"],
            name="pipeline_stage_runs_fanout_expansion_fk",
            ondelete="RESTRICT",
            use_alter=True,
            deferrable=True,
            initially="DEFERRED",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    node_key: Mapped[str] = mapped_column(Text, nullable=False)
    shard_key: Mapped[str] = mapped_column(Text, nullable=False)
    node_kind: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'blocked'"))
    domain_outcome: Mapped[str | None] = mapped_column(Text)
    reason_code: Mapped[str | None] = mapped_column(Text)
    resolved_execution_spec_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    resolved_execution_spec_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    execution_spec_digest: Mapped[str | None] = mapped_column(Text)
    resource_profile_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    resource_profile_digest: Mapped[str | None] = mapped_column(Text)
    image_runtime_contract_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    image_runtime_contract_digest: Mapped[str | None] = mapped_column(Text)
    provider_connection_ref: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    secret_refs: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'::text[]"), default=list
    )
    resolved_input_bindings_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)
    resolved_input_bindings_digest: Mapped[str | None] = mapped_column(Text)
    fanout_parameters_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    fanout_item_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    fanout_item_digest: Mapped[str | None] = mapped_column(Text)
    fanout_expansion_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
    )
    gate_subject_stage_run_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    request_renderer_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    request_renderer_digest: Mapped[str | None] = mapped_column(Text)
    failure_policy: Mapped[str | None] = mapped_column(Text)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    latest_checkpoint_artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifacts.id",
            ondelete="RESTRICT",
            use_alter=True,
            name="pipeline_stage_runs_latest_checkpoint_fk",
        ),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    ready_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    claimed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineStageDependency(Base):
    __tablename__ = "pipeline_stage_dependencies"
    __table_args__ = (
        CheckConstraint(
            "dependency_kind IN ('required','terminal_barrier','gate_matched','gate_unmatched',"
            "'gate_approved','gate_rejected_or_expired')",
            name="pipeline_stage_dependencies_kind_check",
        ),
        CheckConstraint(
            "upstream_stage_run_id <> downstream_stage_run_id",
            name="pipeline_stage_dependencies_distinct_check",
        ),
        ForeignKeyConstraint(
            ["pipeline_run_id", "upstream_stage_run_id"],
            ["pipeline_stage_runs.pipeline_run_id", "pipeline_stage_runs.id"],
            ondelete="CASCADE",
            name="pipeline_stage_dependencies_upstream_run_fk",
        ),
        ForeignKeyConstraint(
            ["pipeline_run_id", "downstream_stage_run_id"],
            ["pipeline_stage_runs.pipeline_run_id", "pipeline_stage_runs.id"],
            ondelete="CASCADE",
            name="pipeline_stage_dependencies_downstream_run_fk",
        ),
    )

    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    upstream_stage_run_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    downstream_stage_run_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    dependency_kind: Mapped[str] = mapped_column(Text, primary_key=True)
    selected: Mapped[bool | None] = mapped_column(Boolean)
    satisfied_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAttempt(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (
        CheckConstraint(
            "state IN ('fault_pending','queued','claimed','running','succeeded','failed','cancelled','lost')",
            name="execution_attempts_state_check",
        ),
        CheckConstraint(
            "retry_class IS NULL OR retry_class IN "
            "('none','contract_error','provider_transient','infrastructure_transient',"
            "'internal_defect','cancelled')",
            name="execution_attempts_retry_class_check",
        ),
        CheckConstraint("attempt_number BETWEEN 1 AND 3", name="execution_attempts_number_check"),
        CheckConstraint("lease_epoch >= 0", name="execution_attempts_lease_epoch_nonnegative"),
        CheckConstraint("version >= 0", name="execution_attempts_version_nonnegative"),
        CheckConstraint(
            "(stage_request_json IS NULL AND stage_request_bytes IS NULL AND stage_request_digest IS NULL) "
            "OR (stage_request_json IS NOT NULL AND stage_request_bytes IS NOT NULL "
            "AND stage_request_digest IS NOT NULL)",
            name="execution_attempts_stage_request_group_check",
        ),
        CheckConstraint(
            "(result_manifest_json IS NULL) = (result_manifest_digest IS NULL)",
            name="execution_attempts_result_group_check",
        ),
        CheckConstraint(
            "(execution_authorization_json IS NULL "
            "AND execution_authorization_bytes IS NULL "
            "AND execution_authorization_digest IS NULL) OR "
            "(execution_authorization_json IS NOT NULL "
            "AND execution_authorization_bytes IS NOT NULL "
            "AND execution_authorization_digest ~ '^sha256:[0-9a-f]{64}$')",
            name="execution_attempts_authorization_group_check",
        ),
        CheckConstraint(
            "state != 'fault_pending' OR (worker_id IS NULL AND claim_id IS NULL "
            "AND lease_token_digest IS NULL AND lease_expires_at IS NULL "
            "AND queued_at IS NULL AND claimed_at IS NULL AND started_at IS NULL)",
            name="execution_attempts_fault_pending_check",
        ),
        CheckConstraint(
            "state NOT IN ('queued','claimed','running','succeeded','failed','lost') "
            "OR queued_at IS NOT NULL",
            name="execution_attempts_queued_timestamp_check",
        ),
        CheckConstraint(
            "(worker_id IS NULL AND claim_id IS NULL AND lease_token_digest IS NULL "
            "AND lease_expires_at IS NULL) OR "
            "(worker_id IS NOT NULL AND claim_id IS NOT NULL AND lease_token_digest IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name="execution_attempts_claim_group_check",
        ),
        CheckConstraint(
            "state NOT IN ('claimed','running','succeeded','failed','lost') OR claim_id IS NOT NULL",
            name="execution_attempts_claimed_state_check",
        ),
        CheckConstraint(
            "state NOT IN ('fault_pending','queued') OR claim_id IS NULL",
            name="execution_attempts_unclaimed_state_check",
        ),
        CheckConstraint(
            "(cancellation_observed_at IS NULL) = (cancellation_outcome IS NULL)",
            name="execution_attempts_cancellation_group_check",
        ),
        CheckConstraint(
            "state != 'succeeded' OR (exit_code = 0 AND result_manifest_json IS NOT NULL)",
            name="execution_attempts_succeeded_result_check",
        ),
        CheckConstraint(
            "state IN ('succeeded','failed','cancelled','lost') OR "
            "(exit_code IS NULL AND retry_class IS NULL AND reason_code IS NULL "
            "AND result_manifest_json IS NULL)",
            name="execution_attempts_terminal_fields_check",
        ),
        CheckConstraint(
            "(state IN ('succeeded','failed','cancelled','lost')) = (finished_at IS NOT NULL)",
            name="execution_attempts_terminal_timestamp_check",
        ),
        UniqueConstraint(
            "stage_run_id", "attempt_number", name="execution_attempts_stage_number_uidx"
        ),
        UniqueConstraint("id", "worker_id", name="execution_attempts_worker_identity_uidx"),
        Index(
            "execution_attempts_claim_uidx",
            "claim_id",
            unique=True,
            postgresql_where=text("claim_id IS NOT NULL"),
        ),
        Index(
            "execution_attempts_state_lease_queue_idx",
            "state",
            "lease_expires_at",
            "queued_at",
            "id",
        ),
        Index("execution_attempts_worker_state_idx", "worker_id", "state"),
        CheckConstraint(
            "(cleanup_acknowledged_at IS NULL AND cleanup_proof_json IS NULL "
            "AND cleanup_proof_digest IS NULL) OR "
            "(cleanup_acknowledged_at IS NOT NULL AND cleanup_proof_json IS NOT NULL "
            "AND cleanup_proof_digest ~ '^sha256:[0-9a-f]{64}$')",
            name="execution_attempts_cleanup_proof_group_check",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    stage_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT")
    )
    claim_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    lease_token_digest: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    heartbeat_phase: Mapped[str | None] = mapped_column(Text)
    heartbeat_runtime_seconds: Mapped[float | None] = mapped_column(Float)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    container_id: Mapped[str | None] = mapped_column(Text)
    runtime_started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    input_view_digest: Mapped[str | None] = mapped_column(Text)
    step_jwt_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    stage_request_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    stage_request_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    stage_request_digest: Mapped[str | None] = mapped_column(Text)
    execution_authorization_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    execution_authorization_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    execution_authorization_digest: Mapped[str | None] = mapped_column(Text)
    exit_code: Mapped[int | None] = mapped_column(Integer)
    retry_class: Mapped[str | None] = mapped_column(Text)
    reason_code: Mapped[str | None] = mapped_column(Text)
    result_manifest_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    result_manifest_digest: Mapped[str | None] = mapped_column(Text)
    resumed_checkpoint_artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifacts.id",
            ondelete="RESTRICT",
            use_alter=True,
            name="execution_attempts_resumed_checkpoint_fk",
        ),
        nullable=True,
    )
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cancellation_observed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cancellation_outcome: Mapped[str | None] = mapped_column(Text)
    cleanup_acknowledged_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cleanup_proof_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    cleanup_proof_digest: Mapped[str | None] = mapped_column(Text)
    queued_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineLivePreviewGeneration(Base):
    """Bounded ephemeral preview identity; never an Artifact or lineage row."""

    __tablename__ = "pipeline_live_preview_generations"
    __table_args__ = (
        CheckConstraint(
            "state IN ('waiting','live','handoff','ended')",
            name="pipeline_live_preview_generations_state_check",
        ),
        CheckConstraint(
            "(latest_sequence IS NULL AND latest_step_idx IS NULL AND received_at IS NULL) OR "
            "(latest_sequence BETWEEN 0 AND 9007199254740991 AND "
            "latest_step_idx BETWEEN 0 AND 18446744073709551615 AND received_at IS NOT NULL)",
            name="pipeline_live_preview_generations_latest_group_check",
        ),
        CheckConstraint(
            "frame_count BETWEEN 0 AND 64 AND total_bytes BETWEEN 0 AND 33554432",
            name="pipeline_live_preview_generations_bounds_check",
        ),
        CheckConstraint(
            "(purged_at IS NULL) = (purge_reason IS NULL)",
            name="pipeline_live_preview_generations_purge_group_check",
        ),
        CheckConstraint(
            "generation = execution_attempt_id",
            name="pipeline_live_preview_generations_attempt_generation_check",
        ),
        CheckConstraint(
            "lease_epoch > 0",
            name="pipeline_live_preview_generations_lease_epoch_positive_check",
        ),
        Index("pipeline_live_preview_generations_expiry_idx", "expires_at"),
        Index("pipeline_live_preview_generations_team_state_idx", "team_id", "state"),
    )

    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        primary_key=True,
    )
    generation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_stage_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    worker_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT"), nullable=False
    )
    claim_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'waiting'"))
    latest_sequence: Mapped[int | None] = mapped_column(BigInteger)
    latest_step_idx: Mapped[Decimal | None] = mapped_column(Numeric(20, 0))
    received_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    frame_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    purge_reason: Mapped[str | None] = mapped_column(Text)
    purged_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineLivePreviewFrame(Base):
    """One bounded preview JPEG kept only in the ephemeral relational backend."""

    __tablename__ = "pipeline_live_preview_frames"
    __table_args__ = (
        CheckConstraint(
            "sequence BETWEEN 0 AND 9007199254740991",
            name="pipeline_live_preview_frames_sequence_check",
        ),
        CheckConstraint(
            "step_idx BETWEEN 0 AND 18446744073709551615",
            name="pipeline_live_preview_frames_step_check",
        ),
        CheckConstraint(
            "jpeg_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_live_preview_frames_digest_check",
        ),
        CheckConstraint(
            "jpeg_size_bytes BETWEEN 1 AND 524288 AND octet_length(jpeg_bytes) = jpeg_size_bytes",
            name="pipeline_live_preview_frames_size_check",
        ),
        UniqueConstraint(
            "execution_attempt_id",
            "idempotency_key",
            name="pipeline_live_preview_frames_idempotency_uidx",
        ),
        Index("pipeline_live_preview_frames_received_idx", "received_at"),
    )

    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_live_preview_generations.execution_attempt_id", ondelete="CASCADE"),
        primary_key=True,
    )
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    step_idx: Mapped[Decimal] = mapped_column(Numeric(20, 0), nullable=False)
    jpeg_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    jpeg_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    jpeg_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class PipelineInputMaterializationEvidence(Base):
    __tablename__ = "pipeline_input_materialization_evidence"
    __table_args__ = (
        CheckConstraint(
            "lease_epoch >= 0",
            name="pipeline_input_materialization_evidence_lease_epoch_nonnegative",
        ),
        CheckConstraint(
            "cache_expectation IN ('cold_after_eviction','warm_reuse_only')",
            name="pipeline_input_materialization_evidence_expectation_check",
        ),
        CheckConstraint(
            "manifest_open_count >= 0 AND file_open_count >= 0 AND file_bytes >= 0 "
            "AND archive_extraction_count >= 0 AND cas_rename_count >= 0",
            name="pipeline_input_materialization_evidence_counters_nonnegative",
        ),
        ForeignKeyConstraint(
            ["execution_attempt_id", "worker_id"],
            ["execution_attempts.id", "execution_attempts.worker_id"],
            ondelete="CASCADE",
            name="pipeline_input_materialization_evidence_attempt_worker_fk",
        ),
    )

    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
    )
    worker_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT"), nullable=False
    )
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cache_expectation: Mapped[str] = mapped_column(Text, nullable=False)
    ordered_manifest_sha256s_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    manifest_open_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_open_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    archive_extraction_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cas_rename_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_view_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    materialized_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    evidence_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)


class PipelineTerminalSnapshot(Base):
    __tablename__ = "pipeline_terminal_snapshots"
    __table_args__ = (
        CheckConstraint(
            "octet_length(snapshot_bytes) <= 16777216",
            name="pipeline_terminal_snapshots_size_check",
        ),
        ForeignKeyConstraint(
            ["pipeline_run_id", "consumer_stage_run_id"],
            ["pipeline_stage_runs.pipeline_run_id", "pipeline_stage_runs.id"],
            ondelete="CASCADE",
            name="pipeline_terminal_snapshots_consumer_run_fk",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    consumer_stage_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), nullable=False, unique=True
    )
    renderer_digest: Mapped[str] = mapped_column(Text, nullable=False)
    run_graph_digest: Mapped[str] = mapped_column(Text, nullable=False)
    terminal_stage_keys_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    stages_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    snapshot_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    snapshot_digest: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineFanoutExpansion(Base):
    __tablename__ = "pipeline_fanout_expansions"
    __table_args__ = (
        CheckConstraint(
            "source_kind IN ('run_input','stage_output')", name="pipeline_fanout_source_check"
        ),
        CheckConstraint(
            "(source_kind = 'run_input' AND source_stage_run_id IS NULL) OR "
            "(source_kind = 'stage_output' AND source_stage_run_id IS NOT NULL)",
            name="pipeline_fanout_source_stage_check",
        ),
        CheckConstraint("item_count BETWEEN 0 AND 5000", name="pipeline_fanout_item_count_range"),
        UniqueConstraint(
            "pipeline_run_id",
            "node_key",
            "source_artifact_id",
            name="pipeline_fanout_expansions_identity_uidx",
        ),
        UniqueConstraint("pipeline_run_id", "id", name="pipeline_fanout_expansions_run_id_uidx"),
        ForeignKeyConstraint(
            ["pipeline_run_id", "source_stage_run_id"],
            ["pipeline_stage_runs.pipeline_run_id", "pipeline_stage_runs.id"],
            name="pipeline_fanout_expansions_source_run_fk",
            deferrable=True,
            initially="DEFERRED",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    node_key: Mapped[str] = mapped_column(Text, nullable=False)
    source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    source_stage_run_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    source_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    source_manifest_digest: Mapped[str] = mapped_column(Text, nullable=False)
    fanout_spec_digest: Mapped[str] = mapped_column(Text, nullable=False)
    item_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineEvent(Base):
    __tablename__ = "pipeline_events"
    __table_args__ = (CheckConstraint("seq > 0", name="pipeline_events_seq_positive"),)

    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), primary_key=True
    )
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    stage_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE")
    )
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="CASCADE")
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    actor_kind: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str | None] = mapped_column(Text)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAttemptRequest(Base):
    """Durable idempotency journal for claim-fenced worker mutations."""

    __tablename__ = "execution_attempt_requests"
    __table_args__ = (
        UniqueConstraint(
            "execution_attempt_id",
            "route",
            "request_id",
            name="execution_attempt_requests_route_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    route: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAttemptWorkerEvent(Base):
    __tablename__ = "execution_attempt_worker_events"
    __table_args__ = (
        CheckConstraint("local_seq >= 0", name="execution_attempt_events_seq_nonnegative"),
        UniqueConstraint(
            "execution_attempt_id", "local_seq", name="execution_attempt_events_seq_uidx"
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    local_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    stream: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    message_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class ExecutionAttemptControlCommand(Base):
    __tablename__ = "execution_attempt_control_commands"
    __table_args__ = (
        CheckConstraint("seq > 0", name="execution_attempt_commands_seq_positive"),
        CheckConstraint(
            "command IN ('cancel_requested','rotate_step_jwt','drain_after_attempt')",
            name="execution_attempt_commands_command_check",
        ),
    )

    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        primary_key=True,
    )
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    command: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineAcceptancePreflightPrerequisite(Base):
    __tablename__ = "pipeline_acceptance_preflight_prerequisites"
    __table_args__ = (
        CheckConstraint(
            "state IN ('pending','satisfied','consumed')", name="pipeline_preflight_state_check"
        ),
        CheckConstraint(
            "preflight_input_set_id = 'S02'", name="pipeline_preflight_input_set_check"
        ),
        CheckConstraint(
            "fence_state IN ('pending','active','released')",
            name="pipeline_preflight_fence_state_check",
        ),
        CheckConstraint(
            "policy_activation_epoch IS NULL OR policy_activation_epoch > 0",
            name="pipeline_preflight_activation_epoch_positive",
        ),
        CheckConstraint(
            "worker_lease_epoch IS NULL OR worker_lease_epoch > 0",
            name="pipeline_preflight_worker_lease_epoch_positive",
        ),
        CheckConstraint("version >= 0", name="pipeline_preflight_version_nonnegative"),
        CheckConstraint(
            "(eviction_result_json IS NULL AND eviction_result_bytes IS NULL "
            "AND eviction_result_sha256 IS NULL) OR "
            "(eviction_result_json IS NOT NULL AND eviction_result_bytes IS NOT NULL "
            "AND eviction_result_sha256 IS NOT NULL)",
            name="pipeline_preflight_result_group_check",
        ),
        CheckConstraint(
            "(state = 'pending' AND eviction_result_json IS NULL AND consumed_attempt_id IS NULL "
            "AND satisfied_at IS NULL AND consumed_at IS NULL) OR "
            "(state = 'satisfied' AND eviction_result_json IS NOT NULL "
            "AND consumed_attempt_id IS NULL AND satisfied_at IS NOT NULL AND consumed_at IS NULL) OR "
            "(state = 'consumed' AND eviction_result_json IS NOT NULL "
            "AND consumed_attempt_id IS NOT NULL AND satisfied_at IS NOT NULL AND consumed_at IS NOT NULL)",
            name="pipeline_preflight_state_fields_check",
        ),
        CheckConstraint(
            "(fence_state = 'pending' AND worker_id IS NULL "
            "AND worker_capability_snapshot_digest IS NULL AND policy_id IS NULL "
            "AND policy_config_sha256 IS NULL AND policy_activation_epoch IS NULL "
            "AND worker_lease_epoch IS NULL AND slurm_cluster_id IS NULL "
            "AND slurm_cluster_config_sha256 IS NULL AND slurm_allocation_id IS NULL "
            "AND exclusive_fence_id IS NULL AND fence_acquired_at IS NULL "
            "AND fence_released_at IS NULL AND fence_release_reason IS NULL) OR "
            "(fence_state = 'active' AND worker_id IS NOT NULL "
            "AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL "
            "AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL "
            "AND worker_lease_epoch IS NOT NULL AND slurm_cluster_id IS NOT NULL "
            "AND slurm_cluster_config_sha256 IS NOT NULL AND slurm_allocation_id IS NOT NULL "
            "AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL "
            "AND fence_released_at IS NULL AND fence_release_reason IS NULL) OR "
            "(fence_state = 'released' AND worker_id IS NOT NULL "
            "AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL "
            "AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL "
            "AND worker_lease_epoch IS NOT NULL AND slurm_cluster_id IS NOT NULL "
            "AND slurm_cluster_config_sha256 IS NOT NULL AND slurm_allocation_id IS NOT NULL "
            "AND exclusive_fence_id IS NOT NULL "
            "AND fence_acquired_at IS NOT NULL AND fence_released_at IS NOT NULL "
            "AND fence_release_reason IS NOT NULL)",
            name="pipeline_preflight_fence_fields_check",
        ),
        Index(
            "pipeline_preflight_active_worker_uidx",
            "worker_id",
            unique=True,
            postgresql_where=text("fence_state = 'active'"),
        ),
    )

    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), primary_key=True
    )
    authorization_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    authorization_snapshot_sha256: Mapped[str | None] = mapped_column(Text)
    candidate_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    preflight_input_set_id: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'S02'")
    )
    sealed_input_descriptor_set_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workers.id", ondelete="RESTRICT")
    )
    worker_capability_snapshot_digest: Mapped[str | None] = mapped_column(Text)
    policy_id: Mapped[str | None] = mapped_column(Text)
    policy_config_sha256: Mapped[str | None] = mapped_column(Text)
    policy_activation_epoch: Mapped[int | None] = mapped_column(BigInteger)
    worker_lease_epoch: Mapped[int | None] = mapped_column(BigInteger)
    slurm_cluster_id: Mapped[str | None] = mapped_column(Text)
    slurm_cluster_config_sha256: Mapped[str | None] = mapped_column(Text)
    slurm_allocation_id: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    eviction_result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    eviction_result_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    eviction_result_sha256: Mapped[str | None] = mapped_column(Text)
    exclusive_fence_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    fence_state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    consumed_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="RESTRICT"), unique=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    satisfied_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    fence_acquired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    fence_released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    fence_release_reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineBudgetLedger(Base):
    """One locked hard-budget counter row per PipelineRun (#1212)."""

    __tablename__ = "pipeline_budget_ledgers"
    __table_args__ = (
        CheckConstraint(
            "provider_limit_microusd >= 0 AND provider_reserved_microusd >= 0 "
            "AND provider_settled_microusd >= 0 AND gpu_limit_seconds >= 0 "
            "AND gpu_reserved_seconds >= 0 AND gpu_settled_seconds >= 0 "
            "AND artifact_limit_bytes >= 0 AND artifact_reserved_bytes >= 0 "
            "AND artifact_settled_bytes >= 0 AND stage_run_limit >= 0 "
            "AND stage_runs_created >= 0 AND attempt_limit >= 0 AND attempts_created >= 0",
            name="pipeline_budget_ledgers_nonnegative_check",
        ),
        CheckConstraint(
            "artifact_reserved_bytes + artifact_settled_bytes <= artifact_limit_bytes "
            "AND stage_runs_created <= stage_run_limit AND attempts_created <= attempt_limit",
            name="pipeline_budget_ledgers_hard_limits_check",
        ),
        CheckConstraint(
            "terminal_cause = 'accounting_violation' OR "
            "(provider_reserved_microusd + provider_settled_microusd <= provider_limit_microusd "
            "AND gpu_reserved_seconds + gpu_settled_seconds <= gpu_limit_seconds)",
            name="pipeline_budget_ledgers_metered_limits_check",
        ),
        CheckConstraint(
            "terminal_cause IS NULL OR terminal_cause IN "
            "('user_cancel','provider_budget','gpu_budget','artifact_budget','stage_run_budget',"
            "'attempt_budget','wall_budget','accounting_violation')",
            name="pipeline_budget_ledgers_terminal_cause_check",
        ),
        CheckConstraint(
            "(terminal_cause IS NULL) = (terminal_cause_at IS NULL)",
            name="pipeline_budget_ledgers_terminal_cause_group_check",
        ),
        CheckConstraint("version >= 0", name="pipeline_budget_ledgers_version_nonnegative"),
        Index("pipeline_budget_ledgers_deadline_idx", "wall_deadline_at", "pipeline_run_id"),
    )

    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), primary_key=True
    )
    provider_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_reserved_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    provider_settled_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    gpu_limit_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gpu_reserved_seconds: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    gpu_settled_seconds: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    artifact_limit_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    artifact_reserved_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    artifact_settled_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    stage_run_limit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    stage_runs_created: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    attempt_limit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempts_created: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    wall_deadline_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    terminal_cause: Mapped[str | None] = mapped_column(Text)
    terminal_cause_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class PipelineBudgetReservation(Base):
    __tablename__ = "pipeline_budget_reservations"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('provider','gpu','artifact')",
            name="pipeline_budget_reservations_kind_check",
        ),
        CheckConstraint(
            "state IN ('active','settled','released')",
            name="pipeline_budget_reservations_state_check",
        ),
        CheckConstraint(
            "reserved_amount >= 0 AND (settled_amount IS NULL OR settled_amount >= 0)",
            name="pipeline_budget_reservations_amount_check",
        ),
        CheckConstraint(
            "(state = 'active' AND settled_amount IS NULL AND settled_at IS NULL) OR "
            "(state = 'settled' AND settled_amount IS NOT NULL AND settled_at IS NOT NULL) OR "
            "(state = 'released' AND settled_amount IS NULL AND settled_at IS NOT NULL)",
            name="pipeline_budget_reservations_terminal_fields_check",
        ),
        CheckConstraint(
            "(kind = 'provider' AND reservation_key ~ "
            "'^provider:[0-9a-f-]{36}:[0-9a-f-]{36}$') OR "
            "(kind = 'gpu' AND reservation_key ~ '^gpu:[0-9a-f-]{36}$') OR "
            "(kind = 'artifact' AND reservation_key ~ "
            "'^artifact:(final:[0-9a-f-]{36}|checkpoint:[0-9a-f-]{36}:[0-9]{12}|"
            "control:[a-z][a-z0-9_]{0,62}:[0-9a-f-]{36})$')",
            name="pipeline_budget_reservations_key_namespace_check",
        ),
        UniqueConstraint(
            "pipeline_run_id",
            "kind",
            "reservation_key",
            name="pipeline_budget_reservations_key_uidx",
        ),
        Index("pipeline_budget_reservations_run_state_idx", "pipeline_run_id", "state", "id"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="RESTRICT")
    )
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    reservation_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    reserved_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    settled_amount: Mapped[int | None] = mapped_column(BigInteger)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    settled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class ExecutionAttemptProviderBudget(Base):
    __tablename__ = "execution_attempt_provider_budgets"
    __table_args__ = (
        CheckConstraint(
            "request_limit > 0 AND requests_reserved >= 0 AND requests_settled >= 0 "
            "AND requests_reserved + requests_settled <= request_limit "
            "AND cost_limit_microusd > 0 AND cost_reserved_microusd >= 0 "
            "AND cost_settled_microusd >= 0 AND per_call_timeout_seconds > 0",
            name="execution_attempt_provider_budgets_counter_check",
        ),
        CheckConstraint(
            "version >= 0", name="execution_attempt_provider_budgets_version_nonnegative"
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        primary_key=True,
    )
    binding_snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    request_limit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requests_reserved: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    requests_settled: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    cost_limit_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cost_reserved_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    cost_settled_microusd: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    per_call_timeout_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineProviderDispatch(Base):
    """Durable pre-dispatch identity and post-dispatch accounting outcome."""

    __tablename__ = "pipeline_provider_dispatches"
    __table_args__ = (
        UniqueConstraint(
            "execution_attempt_id",
            "provider_request_id",
            name="pipeline_provider_dispatches_attempt_request_uidx",
        ),
        UniqueConstraint(
            "reservation_id",
            name="pipeline_provider_dispatches_reservation_uidx",
        ),
        CheckConstraint(
            "binding_snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND request_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND (response_digest IS NULL OR response_digest ~ '^sha256:[0-9a-f]{64}$')",
            name="pipeline_provider_dispatches_digest_check",
        ),
        CheckConstraint(
            "provider <> '' AND model <> '' AND wire_api IN ('responses','messages')",
            name="pipeline_provider_dispatches_text_check",
        ),
        CheckConstraint(
            "reserved_cost_microusd >= 0 "
            "AND (actual_cost_microusd IS NULL OR actual_cost_microusd >= 0) "
            "AND upstream_attempt_count BETWEEN 0 AND 1 AND version >= 0",
            name="pipeline_provider_dispatches_amount_check",
        ),
        CheckConstraint(
            "state IN ('reserved','dispatched','settled') "
            "AND (outcome IS NULL OR outcome IN "
            "('not_dispatched','succeeded','failed','uncertain'))",
            name="pipeline_provider_dispatches_state_check",
        ),
        CheckConstraint(
            "(state = 'reserved' AND outcome IS NULL AND actual_cost_microusd IS NULL "
            "AND upstream_attempt_count = 0 AND response_digest IS NULL "
            "AND llm_call_id IS NULL AND dispatched_at IS NULL "
            "AND outcome_at IS NULL AND settled_at IS NULL) OR "
            "(state = 'dispatched' AND outcome IS NULL AND actual_cost_microusd IS NULL "
            "AND upstream_attempt_count = 1 AND response_digest IS NULL "
            "AND llm_call_id IS NULL AND dispatched_at IS NOT NULL "
            "AND outcome_at IS NULL AND settled_at IS NULL) OR "
            "(state = 'settled' AND actual_cost_microusd IS NOT NULL "
            "AND outcome_at IS NOT NULL AND settled_at IS NOT NULL "
            "AND ((outcome = 'not_dispatched' AND actual_cost_microusd = 0 "
            "AND upstream_attempt_count = 0 AND llm_call_id IS NULL "
            "AND dispatched_at IS NULL AND response_digest IS NULL) OR "
            "(outcome IN ('succeeded','failed','uncertain') "
            "AND upstream_attempt_count = 1 AND llm_call_id IS NOT NULL "
            "AND dispatched_at IS NOT NULL AND ((outcome = 'succeeded' "
            "AND response_digest IS NOT NULL) OR "
            "(outcome <> 'succeeded' AND response_digest IS NULL)))))",
            name="pipeline_provider_dispatches_lifecycle_check",
        ),
        Index(
            "pipeline_provider_dispatches_attempt_state_idx",
            "execution_attempt_id",
            "state",
            "created_at",
            "id",
        ),
        Index(
            "pipeline_provider_dispatches_unsettled_idx",
            "state",
            "dispatched_at",
            "id",
            postgresql_where=text("state <> 'settled'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    provider_request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    reservation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_budget_reservations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    binding_snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    wire_api: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'reserved'"))
    outcome: Mapped[str | None] = mapped_column(Text)
    reserved_cost_microusd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actual_cost_microusd: Mapped[int | None] = mapped_column(BigInteger)
    upstream_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    response_digest: Mapped[str | None] = mapped_column(Text)
    llm_call_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("llm_calls.id", ondelete="RESTRICT"),
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    outcome_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    settled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class PipelineCancellationOutbox(Base):
    """Idempotent cancellation command and runtime acknowledgement interface."""

    __tablename__ = "pipeline_cancellation_outbox"
    __table_args__ = (
        CheckConstraint(
            "terminal_cause IN ('user_cancel','provider_budget','gpu_budget','artifact_budget',"
            "'stage_run_budget','attempt_budget','wall_budget','accounting_violation')",
            name="pipeline_cancellation_outbox_cause_check",
        ),
        CheckConstraint(
            "state IN ('pending','acked')", name="pipeline_cancellation_outbox_state_check"
        ),
        CheckConstraint(
            "(state = 'pending' AND ack_json IS NULL AND ack_digest IS NULL AND acked_at IS NULL) OR "
            "(state = 'acked' AND ack_json IS NOT NULL AND ack_digest IS NOT NULL "
            "AND acked_at IS NOT NULL)",
            name="pipeline_cancellation_outbox_ack_group_check",
        ),
        CheckConstraint("version >= 0", name="pipeline_cancellation_outbox_version_nonnegative"),
        UniqueConstraint("execution_attempt_id", name="pipeline_cancellation_outbox_attempt_uidx"),
        UniqueConstraint("idempotency_key", name="pipeline_cancellation_outbox_key_uidx"),
        Index("pipeline_cancellation_outbox_state_idx", "state", "created_at", "id"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    terminal_cause: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    ack_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    ack_digest: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    acked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))


class BatchFamilyState(Base):
    """Per-family progression state for family-run batches (#672).

    Composite PK (batch_id, family_key). One row per (batch, family)
    pair, seeded at batch-accept time. The scheduler consults
    ``task_sequence[current_index]`` to gate claim eligibility; the CP
    finalize hook advances the state after each trial terminates.
    """

    __tablename__ = "batch_family_state"

    batch_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="CASCADE"),
        primary_key=True,
    )
    family_key: Mapped[str] = mapped_column(Text, primary_key=True)
    task_sequence: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
    )
    current_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    state_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class PipelineInputImport(Base):
    __tablename__ = "pipeline_input_imports"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('dataset','policy','mop_bank')",
            name="pipeline_input_imports_kind_check",
        ),
        CheckConstraint(
            "trust_class = 'internal_trusted'",
            name="pipeline_input_imports_trust_check",
        ),
        CheckConstraint(
            "state IN ('preparing','uploading','committing','committed','aborted')",
            name="pipeline_input_imports_state_check",
        ),
        CheckConstraint(
            "(state = 'committed' AND committed_artifact_id IS NOT NULL AND committed_at IS NOT NULL) OR "
            "(state != 'committed' AND committed_artifact_id IS NULL AND committed_at IS NULL)",
            name="pipeline_input_imports_committed_group_check",
        ),
        CheckConstraint(
            "(state = 'aborted' AND aborted_at IS NOT NULL AND abort_reason IS NOT NULL) OR "
            "(state != 'aborted' AND aborted_at IS NULL AND abort_reason IS NULL)",
            name="pipeline_input_imports_aborted_group_check",
        ),
        UniqueConstraint(
            "team_id", "idempotency_key", name="pipeline_input_imports_idempotency_uidx"
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"))
    created_by_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"))
    recipe_name: Mapped[str] = mapped_column(Text)
    recipe_version: Mapped[int] = mapped_column(Integer)
    recipe_digest: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    target_artifact_type: Mapped[str] = mapped_column(Text)
    input_manifest_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
    input_manifest_digest: Mapped[str] = mapped_column(Text)
    trust_class: Mapped[str] = mapped_column(Text, server_default=text("'internal_trusted'"))
    max_bundle_bytes: Mapped[int] = mapped_column(BigInteger)
    max_file_count: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(Text)
    artifact_upload_session_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifact_upload_sessions.id",
            use_alter=True,
            name="pipeline_input_imports_upload_session_fk",
        ),
        unique=True,
    )
    committed_artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifacts.id",
            use_alter=True,
            name="pipeline_input_imports_committed_artifact_fk",
        ),
        unique=True,
    )
    idempotency_key: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str] = mapped_column(Text)
    abort_reason: Mapped[str | None] = mapped_column(Text)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    aborted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))


class PipelineInputMaterialization(Base):
    __tablename__ = "pipeline_input_materializations"
    __table_args__ = (
        CheckConstraint(
            "state IN ('preparing','committing','committed','aborted')",
            name="pipeline_input_materializations_state_check",
        ),
        CheckConstraint(
            "(official_materialization_kind IS NULL AND official_materialization_authority_id IS NULL "
            "AND official_materialization_authority_snapshot_digest IS NULL "
            "AND official_materialization_identity_digest IS NULL) OR "
            "(official_materialization_kind IS NOT NULL AND official_materialization_authority_id IS NOT NULL "
            "AND official_materialization_authority_snapshot_digest IS NOT NULL "
            "AND official_materialization_identity_digest IS NOT NULL)",
            name="pipeline_input_materializations_official_group_check",
        ),
        CheckConstraint(
            "(state = 'committed' AND result_bindings_json IS NOT NULL AND committed_at IS NOT NULL) OR "
            "(state != 'committed' AND result_bindings_json IS NULL AND committed_at IS NULL)",
            name="pipeline_input_materializations_committed_group_check",
        ),
        CheckConstraint(
            "(state = 'aborted' AND aborted_at IS NOT NULL AND abort_reason IS NOT NULL) OR "
            "(state != 'aborted' AND aborted_at IS NULL AND abort_reason IS NULL)",
            name="pipeline_input_materializations_aborted_group_check",
        ),
        UniqueConstraint(
            "team_id", "idempotency_key", name="pipeline_input_materializations_idempotency_uidx"
        ),
        Index(
            "pipeline_input_materializations_official_uidx",
            "team_id",
            "official_materialization_kind",
            "official_materialization_authority_id",
            "official_materialization_identity_digest",
            unique=True,
            postgresql_where=text("official_materialization_kind IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"))
    created_by_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"))
    recipe_name: Mapped[str] = mapped_column(Text)
    recipe_version: Mapped[int] = mapped_column(Integer)
    recipe_digest: Mapped[str] = mapped_column(Text)
    source_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
    source_snapshot_digest: Mapped[str] = mapped_column(Text)
    parameters_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
    parameters_digest: Mapped[str] = mapped_column(Text)
    materialization_identity_digest: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text)
    declared_outputs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    declared_outputs_digest: Mapped[str] = mapped_column(Text)
    artifact_upload_session_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifact_upload_sessions.id",
            use_alter=True,
            name="pipeline_input_materializations_upload_session_fk",
        ),
        unique=True,
    )
    result_bindings_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)
    official_materialization_kind: Mapped[str | None] = mapped_column(Text)
    official_materialization_authority_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    official_materialization_authority_snapshot_digest: Mapped[str | None] = mapped_column(Text)
    official_materialization_identity_digest: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str] = mapped_column(Text)
    abort_reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    aborted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))


class ArtifactUploadSession(Base):
    __tablename__ = "artifact_upload_sessions"
    __table_args__ = (
        CheckConstraint(
            "commit_kind IN ('final_output','checkpoint','service_execution_output','input_import','input_materialization',"
            "'acceptance_evidence','profile_calibration_evidence')",
            name="artifact_upload_sessions_kind_check",
        ),
        CheckConstraint(
            "state IN ('preparing','uploading','uploaded','committing','committed_ready','committed','aborted')",
            name="artifact_upload_sessions_state_check",
        ),
        CheckConstraint(
            "((checkpoint_envelope_json IS NULL) = (checkpoint_envelope_digest IS NULL)) "
            "AND (checkpoint_envelope_json IS NULL OR commit_kind='checkpoint') "
            "AND (checkpoint_envelope_digest IS NULL OR "
            "checkpoint_envelope_digest ~ '^sha256:[0-9a-f]{64}$')",
            name="artifact_upload_sessions_checkpoint_envelope_group_check",
        ),
        CheckConstraint(
            "control_producer_kind IS NULL AND control_producer_id IS NULL AND ("
            "(commit_kind='final_output' AND pipeline_run_id IS NOT NULL "
            "AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL "
            "AND attempt_number IS NOT NULL AND checkpoint_sequence IS NULL "
            "AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL "
            "AND service_execution_lease_id IS NULL "
            "AND stage_result_json IS NOT NULL AND stage_result_digest IS NOT NULL AND inventory_digest IS NOT NULL) OR "
            "(commit_kind='checkpoint' AND pipeline_run_id IS NOT NULL "
            "AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL "
            "AND attempt_number IS NOT NULL AND checkpoint_sequence IS NOT NULL "
            "AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL "
            "AND service_execution_lease_id IS NULL "
            "AND stage_result_json IS NULL AND stage_result_digest IS NULL AND inventory_digest IS NULL) OR "
            "(commit_kind='service_execution_output' AND service_execution_lease_id IS NOT NULL "
            "AND service_execution_generation IS NOT NULL "
            "AND service_execution_role IN ('attempt','verifier') "
            "AND service_execution_runtime_contract_sha256 IS NOT NULL "
            "AND service_execution_candidate_sha IS NOT NULL "
            "AND service_execution_task_revision_sha256 IS NOT NULL "
            "AND service_execution_command_identity_sha256 IS NOT NULL "
            "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL "
            "AND execution_attempt_id IS NULL AND attempt_number IS NULL "
            "AND checkpoint_sequence IS NULL AND pipeline_input_import_id IS NULL "
            "AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL "
            "AND actor_user_id IS NULL AND stage_result_json IS NULL "
            "AND stage_result_digest IS NULL AND inventory_digest IS NULL) OR "
            "(commit_kind='input_import' AND pipeline_input_import_id IS NOT NULL "
            "AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL "
            "AND execution_attempt_id IS NULL AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL) OR "
            "(commit_kind='input_materialization' AND pipeline_input_materialization_id IS NOT NULL "
            "AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL "
            "AND execution_attempt_id IS NULL AND pipeline_input_import_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL) OR "
            "(commit_kind='acceptance_evidence' AND pipeline_acceptance_authorization_id IS NOT NULL "
            "AND acceptance_action IN ('matrix','soak') AND acceptance_candidate_sha256 IS NOT NULL "
            "AND acceptance_result_kind IN ('success','terminal') AND actor_user_id IS NOT NULL "
            "AND ((acceptance_result_kind='success' AND acceptance_termination_reason IS NULL) OR "
            "(acceptance_result_kind='terminal' AND acceptance_termination_reason IS NOT NULL)) "
            "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL "
            "AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_profile_calibration_authorization_id IS NULL) OR "
            "(commit_kind='profile_calibration_evidence' "
            "AND pipeline_profile_calibration_authorization_id IS NOT NULL "
            "AND profile_calibration_spec_sha256 IS NOT NULL "
            "AND profile_calibration_result_kind IN ('certification','catalog','terminal') "
            "AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL "
            "AND execution_attempt_id IS NULL AND pipeline_input_import_id IS NULL "
            "AND pipeline_input_materialization_id IS NULL "
            "AND pipeline_acceptance_authorization_id IS NULL))",
            name="artifact_upload_sessions_producer_shape_check",
        ),
        CheckConstraint(
            "state != 'committed_ready' OR commit_kind='final_output'",
            name="artifact_upload_sessions_ready_kind_check",
        ),
        CheckConstraint(
            "(commit_kind='service_execution_output') = "
            "(service_execution_lease_id IS NOT NULL) AND "
            "((service_execution_lease_id IS NULL AND service_execution_generation IS NULL "
            "AND service_execution_role IS NULL "
            "AND service_execution_runtime_contract_sha256 IS NULL "
            "AND service_execution_candidate_sha IS NULL "
            "AND service_execution_task_revision_sha256 IS NULL "
            "AND service_execution_command_identity_sha256 IS NULL) OR "
            "(service_execution_lease_id IS NOT NULL AND service_execution_generation > 0 "
            "AND service_execution_role IN ('attempt','verifier') "
            "AND service_execution_runtime_contract_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND service_execution_candidate_sha ~ '^[0-9a-f]{40}$' "
            "AND service_execution_task_revision_sha256 ~ '^sha256:[0-9a-f]{64}$' "
            "AND service_execution_command_identity_sha256 ~ '^sha256:[0-9a-f]{64}$'))",
            name="artifact_upload_sessions_service_execution_group_check",
        ),
        CheckConstraint(
            "commit_kind != 'profile_calibration_evidence' OR "
            "(profile_calibration_result_kind='certification' "
            "AND profile_calibration_scenario_id IS NOT NULL "
            "AND profile_calibration_candidate_identity_sha256 IS NOT NULL "
            "AND profile_calibration_run_ordinal BETWEEN 1 AND 3 "
            "AND profile_calibration_source_pipeline_run_id IS NOT NULL "
            "AND profile_calibration_termination_reason IS NULL) OR "
            "(profile_calibration_result_kind='catalog' "
            "AND profile_calibration_scenario_id IS NULL "
            "AND profile_calibration_candidate_identity_sha256 IS NULL "
            "AND profile_calibration_run_ordinal IS NULL "
            "AND profile_calibration_source_pipeline_run_id IS NULL "
            "AND profile_calibration_termination_reason IS NULL) OR "
            "(profile_calibration_result_kind='terminal' "
            "AND profile_calibration_scenario_id IS NULL "
            "AND profile_calibration_candidate_identity_sha256 IS NULL "
            "AND profile_calibration_run_ordinal IS NULL "
            "AND profile_calibration_source_pipeline_run_id IS NULL "
            "AND profile_calibration_termination_reason IS NOT NULL)",
            name="artifact_upload_sessions_profile_shape_check",
        ),
        CheckConstraint(
            "actual_total_bytes >= 0 AND expected_total_max_bytes > 0 "
            "AND actual_total_bytes <= expected_total_max_bytes",
            name="artifact_upload_sessions_bytes_check",
        ),
        CheckConstraint(
            "(state IN ('committed_ready','committed') AND canonical_manifest_json IS NOT NULL "
            "AND manifest_sha256 IS NOT NULL AND committed_marker_sha256 IS NOT NULL) OR "
            "(state NOT IN ('committed_ready','committed') AND canonical_manifest_json IS NULL "
            "AND manifest_sha256 IS NULL AND committed_marker_sha256 IS NULL)",
            name="artifact_upload_sessions_manifest_group_check",
        ),
        UniqueConstraint("prefix", name="artifact_upload_sessions_prefix_uidx"),
        Index(
            "artifact_upload_sessions_final_request_uidx",
            "execution_attempt_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("commit_kind='final_output'"),
        ),
        Index(
            "artifact_upload_sessions_checkpoint_request_uidx",
            "execution_attempt_id",
            "checkpoint_sequence",
            "idempotency_key",
            unique=True,
            postgresql_where=text("commit_kind='checkpoint'"),
        ),
        Index(
            "artifact_upload_sessions_service_execution_uidx",
            "service_execution_lease_id",
            "service_execution_generation",
            "idempotency_key",
            unique=True,
            postgresql_where=text("commit_kind='service_execution_output'"),
        ),
        Index(
            "artifact_upload_sessions_import_request_uidx",
            "pipeline_input_import_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("commit_kind='input_import'"),
        ),
        Index(
            "artifact_upload_sessions_materialization_request_uidx",
            "pipeline_input_materialization_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("commit_kind='input_materialization'"),
        ),
        Index(
            "artifact_upload_sessions_acceptance_uidx",
            "pipeline_acceptance_authorization_id",
            "acceptance_action",
            "acceptance_candidate_sha256",
            unique=True,
            postgresql_where=text("commit_kind='acceptance_evidence'"),
        ),
        Index(
            "artifact_upload_sessions_profile_certification_uidx",
            "pipeline_profile_calibration_authorization_id",
            "profile_calibration_spec_sha256",
            "profile_calibration_scenario_id",
            "profile_calibration_candidate_identity_sha256",
            "profile_calibration_run_ordinal",
            unique=True,
            postgresql_where=text(
                "commit_kind='profile_calibration_evidence' "
                "AND profile_calibration_result_kind='certification'"
            ),
        ),
        Index(
            "artifact_upload_sessions_profile_final_uidx",
            "pipeline_profile_calibration_authorization_id",
            "profile_calibration_spec_sha256",
            unique=True,
            postgresql_where=text(
                "commit_kind='profile_calibration_evidence' "
                "AND profile_calibration_result_kind IN ('catalog','terminal')"
            ),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"))
    commit_kind: Mapped[str] = mapped_column(Text)
    pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE")
    )
    pipeline_stage_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE")
    )
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="CASCADE")
    )
    attempt_number: Mapped[int | None] = mapped_column(Integer)
    checkpoint_sequence: Mapped[int | None] = mapped_column(BigInteger)
    checkpoint_envelope_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    checkpoint_envelope_digest: Mapped[str | None] = mapped_column(Text)
    service_execution_lease_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_leases.id", ondelete="CASCADE")
    )
    service_execution_generation: Mapped[int | None] = mapped_column(BigInteger)
    service_execution_role: Mapped[str | None] = mapped_column(Text)
    service_execution_runtime_contract_sha256: Mapped[str | None] = mapped_column(Text)
    service_execution_candidate_sha: Mapped[str | None] = mapped_column(Text)
    service_execution_task_revision_sha256: Mapped[str | None] = mapped_column(Text)
    service_execution_command_identity_sha256: Mapped[str | None] = mapped_column(Text)
    control_producer_kind: Mapped[str | None] = mapped_column(Text)
    control_producer_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    pipeline_input_import_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_input_imports.id", ondelete="CASCADE")
    )
    pipeline_input_materialization_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_input_materializations.id", ondelete="CASCADE")
    )
    pipeline_acceptance_authorization_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    acceptance_action: Mapped[str | None] = mapped_column(Text)
    acceptance_candidate_sha256: Mapped[str | None] = mapped_column(Text)
    acceptance_result_kind: Mapped[str | None] = mapped_column(Text)
    acceptance_termination_reason: Mapped[str | None] = mapped_column(Text)
    pipeline_profile_calibration_authorization_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True)
    )
    profile_calibration_spec_sha256: Mapped[str | None] = mapped_column(Text)
    profile_calibration_result_kind: Mapped[str | None] = mapped_column(Text)
    profile_calibration_scenario_id: Mapped[str | None] = mapped_column(Text)
    profile_calibration_candidate_identity_sha256: Mapped[str | None] = mapped_column(Text)
    profile_calibration_run_ordinal: Mapped[int | None] = mapped_column(Integer)
    profile_calibration_source_pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="RESTRICT")
    )
    profile_calibration_termination_reason: Mapped[str | None] = mapped_column(Text)
    actor_user_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"))
    idempotency_key: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str] = mapped_column(Text)
    stage_result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    stage_result_digest: Mapped[str | None] = mapped_column(Text)
    inventory_digest: Mapped[str | None] = mapped_column(Text)
    prefix: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text)
    expected_total_max_bytes: Mapped[int] = mapped_column(BigInteger)
    actual_total_bytes: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    canonical_manifest_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    manifest_sha256: Mapped[str | None] = mapped_column(Text)
    committed_marker_sha256: Mapped[str | None] = mapped_column(Text)
    upload_token_digest: Mapped[bytes | None] = mapped_column(LargeBinary)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    committed_ready_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    aborted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    version: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))


class ArtifactUploadFile(Base):
    __tablename__ = "artifact_upload_files"
    __table_args__ = (
        CheckConstraint("file_index >= 0", name="artifact_upload_files_index_check"),
        CheckConstraint(
            "role IN ('semantic_document','payload','payload_archive')",
            name="artifact_upload_files_role_check",
        ),
        CheckConstraint(
            "archive_format IN ('none','tar','tar.zst','zip')",
            name="artifact_upload_files_archive_check",
        ),
        UniqueConstraint(
            "session_id",
            "preallocated_artifact_id",
            "relative_path",
            name="artifact_upload_files_path_uidx",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifact_upload_sessions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    file_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    preallocated_artifact_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True))
    relative_path: Mapped[str] = mapped_column(Text)
    artifact_name: Mapped[str] = mapped_column(Text)
    artifact_type: Mapped[str] = mapped_column(Text)
    producer: Mapped[str] = mapped_column(Text)
    media_type: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    archive_format: Mapped[str] = mapped_column(Text)
    expected_max_bytes: Mapped[int] = mapped_column(BigInteger)
    expected_sha256: Mapped[str | None] = mapped_column(Text)
    expected_size: Mapped[int | None] = mapped_column(BigInteger)
    multipart_upload_id: Mapped[str | None] = mapped_column(Text)
    computed_sha256: Mapped[str | None] = mapped_column(Text)
    actual_size: Mapped[int | None] = mapped_column(BigInteger)
    state: Mapped[str] = mapped_column(Text, server_default=text("'planned'"))
    ordered_part_receipts_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, server_default=text("'[]'::jsonb")
    )


class PipelineExecutionCheckpoint(Base):
    __tablename__ = "pipeline_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "execution_attempt_id", "checkpoint_sequence", name="pipeline_checkpoints_sequence_uidx"
        ),
        CheckConstraint(
            "attempt_number BETWEEN 1 AND 3", name="pipeline_checkpoints_attempt_check"
        ),
        CheckConstraint(
            "source_attempt_state IN ('claimed','running')",
            name="pipeline_checkpoints_source_state_check",
        ),
        CheckConstraint(
            "recipe_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_checkpoints_recipe_digest_check",
        ),
        CheckConstraint(
            "resolved_input_bindings_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_checkpoints_bindings_digest_check",
        ),
        CheckConstraint(
            "execution_spec_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_checkpoints_spec_digest_check",
        ),
        CheckConstraint(
            "resume_compatibility_key ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_checkpoints_resume_digest_check",
        ),
        CheckConstraint(
            "checkpoint_digest ~ '^sha256:[0-9a-f]{64}$'",
            name="pipeline_checkpoints_document_digest_check",
        ),
        Index(
            "pipeline_checkpoints_stage_latest_idx",
            "pipeline_stage_run_id",
            "attempt_number",
            "checkpoint_sequence",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="CASCADE")
    )
    checkpoint_sequence: Mapped[int] = mapped_column(BigInteger)
    artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifacts.id", use_alter=True, name="pipeline_checkpoints_artifact_fk"),
        unique=True,
    )
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_stage_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    recipe_digest: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_input_bindings_digest: Mapped[str] = mapped_column(Text, nullable=False)
    execution_spec_digest: Mapped[str] = mapped_column(Text, nullable=False)
    image_digest: Mapped[str] = mapped_column(Text, nullable=False)
    resume_compatibility_key: Mapped[str] = mapped_column(Text, nullable=False)
    checkpoint_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    checkpoint_digest: Mapped[str] = mapped_column(Text, nullable=False)
    source_attempt_state: Mapped[str] = mapped_column(Text, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )

    @property
    def sequence(self) -> int:
        return self.checkpoint_sequence


# Additive compatibility for #1214 callers that imported the minimal row name.
PipelineCheckpoint = PipelineExecutionCheckpoint


class PipelineAcceptanceEvidenceRun(Base):
    __tablename__ = "pipeline_acceptance_evidence_runs"
    artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "artifacts.id",
            use_alter=True,
            name="pipeline_acceptance_evidence_runs_artifact_fk",
        ),
        primary_key=True,
    )
    run_ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    result_kind: Mapped[str] = mapped_column(Text)
    run_kind: Mapped[str] = mapped_column(Text)
    scenario_id: Mapped[str | None] = mapped_column(Text)
    lane_or_input_set: Mapped[str | None] = mapped_column(Text)
    provenance_digest: Mapped[str] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))


class Artifact(Base):
    """Typed artifact registry entry for reusable run outputs."""

    __tablename__ = "artifacts"
    __table_args__ = (
        CheckConstraint(
            "producer_kind IS NULL OR producer_kind IN "
            "('container','platform','checkpoint','input_import','recipe_input_materialization',"
            "'pipeline_acceptance_evidence','pipeline_profile_calibration_evidence')",
            name="artifacts_pipeline_producer_kind_check",
        ),
        CheckConstraint(
            "producer_kind IS NULL OR "
            "(producer_kind IN ('container','platform','checkpoint') AND pipeline_run_id IS NOT NULL "
            "AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL) OR "
            "(producer_kind = 'input_import' AND pipeline_input_import_id IS NOT NULL "
            "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL) OR "
            "(producer_kind = 'recipe_input_materialization' AND pipeline_input_materialization_id IS NOT NULL "
            "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL) OR "
            "(producer_kind IN ('pipeline_acceptance_evidence','pipeline_profile_calibration_evidence') "
            "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL)",
            name="artifacts_pipeline_identity_group_check",
        ),
        CheckConstraint(
            "(artifact_upload_session_id IS NULL AND manifest_sha256 IS NULL "
            "AND stored_size_bytes IS NULL AND unpacked_size_bytes IS NULL AND file_count IS NULL) OR "
            "(artifact_upload_session_id IS NOT NULL AND manifest_sha256 IS NOT NULL "
            "AND stored_size_bytes IS NOT NULL AND unpacked_size_bytes IS NOT NULL AND file_count IS NOT NULL)",
            name="artifacts_pipeline_manifest_group_check",
        ),
        CheckConstraint(
            "(producer_kind IS NOT NULL AND control_producer_kind IS NULL "
            "AND control_producer_id IS NULL) OR "
            "(producer_kind IS NULL AND "
            "((control_producer_kind IS NULL) = (control_producer_id IS NULL)))",
            name="artifacts_control_producer_group_check",
        ),
        CheckConstraint(
            "access_class IN ('team_runtime','authoring_restricted','sanitized_audit')",
            name="artifacts_access_class_check",
        ),
        Index("artifacts_team_type_idx", "team_id", "artifact_type"),
        Index("artifacts_batch_idx", "batch_id"),
        Index("artifacts_trial_idx", "trial_id"),
        Index("artifacts_policy_idx", "visibility", "share_status", "safety_state"),
        Index("artifacts_team_access_class_idx", "team_id", "access_class", "created_at", "id"),
        Index(
            "artifacts_pipeline_stage_output_uidx",
            "pipeline_stage_run_id",
            "name",
            unique=True,
            postgresql_where=text("producer_kind IN ('container','platform')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    artifact_type: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_schema_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'1.0'"),
        default="1.0",
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    project_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    trial_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="SET NULL"),
        nullable=True,
    )
    pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
        nullable=True,
    )
    pipeline_stage_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_stage_runs.id", ondelete="CASCADE"),
        nullable=True,
    )
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(
            "execution_attempts.id",
            ondelete="CASCADE",
            use_alter=True,
            name="artifacts_execution_attempt_fk",
        ),
        nullable=True,
    )
    producer_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    control_producer_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    control_producer_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    pipeline_input_import_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_input_imports.id", ondelete="RESTRICT")
    )
    pipeline_input_materialization_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_input_materializations.id", ondelete="RESTRICT")
    )
    pipeline_acceptance_authorization_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    acceptance_action: Mapped[str | None] = mapped_column(Text)
    acceptance_candidate_sha256: Mapped[str | None] = mapped_column(Text)
    acceptance_result_kind: Mapped[str | None] = mapped_column(Text)
    acceptance_termination_reason: Mapped[str | None] = mapped_column(Text)
    pipeline_profile_calibration_authorization_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True)
    )
    profile_calibration_spec_sha256: Mapped[str | None] = mapped_column(Text)
    profile_calibration_result_kind: Mapped[str | None] = mapped_column(Text)
    profile_calibration_scenario_id: Mapped[str | None] = mapped_column(Text)
    profile_calibration_candidate_identity_sha256: Mapped[str | None] = mapped_column(Text)
    profile_calibration_run_ordinal: Mapped[int | None] = mapped_column(Integer)
    profile_calibration_source_pipeline_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="RESTRICT")
    )
    profile_calibration_termination_reason: Mapped[str | None] = mapped_column(Text)
    actor_user_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"))
    artifact_upload_session_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifact_upload_sessions.id", ondelete="RESTRICT")
    )
    manifest_sha256: Mapped[str | None] = mapped_column(Text)
    stored_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    unpacked_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    file_count: Mapped[int | None] = mapped_column(Integer)
    created_by: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    storage: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'team'"),
        default="team",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending_scan'"),
        default="pending_scan",
    )
    redaction_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    safety_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'unknown'"),
        default="unknown",
    )
    access_class: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'team_runtime'"),
        default="team_runtime",
    )
    blocked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    retention: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    artifact_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class ArtifactLineageEdge(Base):
    """Direct artifact parent edge for clone, reuse, export, and audit."""

    __tablename__ = "artifact_lineage_edges"
    __table_args__ = (
        Index("artifact_lineage_child_idx", "child_artifact_id"),
        Index("artifact_lineage_parent_idx", "parent_artifact_id"),
        Index("artifact_lineage_relation_idx", "relation"),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    child_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifacts.id", ondelete="CASCADE"),
        nullable=False,
    )
    parent_artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifacts.id", ondelete="SET NULL"),
        nullable=True,
    )
    relation: Mapped[str] = mapped_column(Text, nullable=False)
    edge_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TerminalGenCorpusVersion(Base):
    """Immutable published TerminalGen corpus identity and object boundary."""

    __tablename__ = "terminalgen_corpus_versions"
    __table_args__ = (
        CheckConstraint(
            "corpus_version > 0 AND task_count > 0 AND task_count <= 9000",
            name="terminalgen_corpus_versions_counts_check",
        ),
        CheckConstraint(
            "version_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "recipe_digest ~ '^sha256:[0-9a-f]{64}$' AND "
            "plan_identity_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "authoring_tree_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "runtime_tree_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "taskset_smoke_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "taskset_manifest_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="terminalgen_corpus_versions_digest_check",
        ),
        CheckConstraint(
            "taskset_smoke_task_count > 0 AND taskset_smoke_task_count <= 500 "
            "AND taskset_smoke_task_count <= task_count AND taskset_smoke_size_bytes > 0",
            name="terminalgen_corpus_versions_smoke_check",
        ),
        UniqueConstraint(
            "team_id",
            "corpus_id",
            "corpus_version",
            name="terminalgen_corpus_versions_identity_uidx",
        ),
        UniqueConstraint(
            "team_id",
            "version_sha256",
            name="terminalgen_corpus_versions_digest_uidx",
        ),
        UniqueConstraint(
            "pipeline_run_id",
            name="terminalgen_corpus_versions_run_uidx",
        ),
        Index(
            "terminalgen_corpus_versions_team_created_idx",
            "team_id",
            "published_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("pipeline_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    corpus_id: Mapped[str] = mapped_column(Text, nullable=False)
    corpus_version: Mapped[int] = mapped_column(Integer, nullable=False)
    version_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    recipe_digest: Mapped[str] = mapped_column(Text, nullable=False)
    plan_identity_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    final_audit_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    authoring_corpus_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    runtime_corpus_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    authoring_tree_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_tree_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    task_count: Mapped[int] = mapped_column(Integer, nullable=False)
    taskset_smoke_task_count: Mapped[int] = mapped_column(Integer, nullable=False)
    taskset_smoke_object_key: Mapped[str] = mapped_column(Text, nullable=False)
    taskset_smoke_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    taskset_smoke_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    taskset_manifest_object_key: Mapped[str] = mapped_column(Text, nullable=False)
    taskset_manifest_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    taskset_manifest_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class TerminalGenCorpusTask(Base):
    """Searchable immutable task projection for one published corpus version."""

    __tablename__ = "terminalgen_corpus_tasks"
    __table_args__ = (
        CheckConstraint("task_ordinal >= 0", name="terminalgen_corpus_tasks_ordinal_check"),
        CheckConstraint(
            "source_task_tree_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "projected_task_tree_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "authoring_bundle_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "runtime_bundle_sha256 ~ '^sha256:[0-9a-f]{64}$' AND "
            "verifier_bridge_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="terminalgen_corpus_tasks_digest_check",
        ),
        CheckConstraint(
            "authoring_bundle_size_bytes > 0 AND runtime_bundle_size_bytes > 0",
            name="terminalgen_corpus_tasks_size_check",
        ),
        UniqueConstraint(
            "corpus_version_id",
            "slot_id",
            name="terminalgen_corpus_tasks_slot_uidx",
        ),
        UniqueConstraint(
            "corpus_version_id",
            "task_id",
            name="terminalgen_corpus_tasks_task_uidx",
        ),
        UniqueConstraint(
            "corpus_version_id",
            "task_ordinal",
            name="terminalgen_corpus_tasks_ordinal_uidx",
        ),
        Index("terminalgen_corpus_tasks_task_id_idx", "task_id"),
    )

    corpus_version_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminalgen_corpus_versions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    task_ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    slot_id: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[str] = mapped_column(Text, nullable=False)
    task_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_task_tree_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    projected_task_tree_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    source_task_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    validation_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    authoring_bundle_path: Mapped[str] = mapped_column(Text, nullable=False)
    authoring_bundle_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    authoring_bundle_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    runtime_bundle_path: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_bundle_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_bundle_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    verifier_bridge_sha256: Mapped[str] = mapped_column(Text, nullable=False)


class TerminalGenCorpusAlias(Base):
    """Team-scoped logical alias switched only after immutable version readback."""

    __tablename__ = "terminalgen_corpus_aliases"
    __table_args__ = (
        CheckConstraint("generation > 0", name="terminalgen_corpus_aliases_generation_check"),
        UniqueConstraint(
            "team_id",
            "alias",
            name="terminalgen_corpus_aliases_identity_uidx",
        ),
    )

    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        primary_key=True,
    )
    alias: Mapped[str] = mapped_column(Text, primary_key=True)
    corpus_version_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminalgen_corpus_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    previous_corpus_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminalgen_corpus_versions.id", ondelete="RESTRICT"),
    )
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class TerminalGenCorpusPublication(Base):
    """Durable idempotency and terminal receipt for a server-owned publication."""

    __tablename__ = "terminalgen_corpus_publications"
    __table_args__ = (
        CheckConstraint(
            "state IN ('published','failed')",
            name="terminalgen_corpus_publications_state_check",
        ),
        CheckConstraint(
            "request_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="terminalgen_corpus_publications_request_digest_check",
        ),
        CheckConstraint(
            "(state='published' AND corpus_version_id IS NOT NULL AND reason_code IS NULL "
            "AND receipt_json IS NOT NULL AND receipt_bytes IS NOT NULL "
            "AND receipt_sha256 ~ '^sha256:[0-9a-f]{64}$') OR "
            "(state='failed' AND corpus_version_id IS NULL AND reason_code IS NOT NULL "
            "AND receipt_json IS NULL AND receipt_bytes IS NULL AND receipt_sha256 IS NULL)",
            name="terminalgen_corpus_publications_terminal_group_check",
        ),
        UniqueConstraint(
            "pipeline_run_id",
            name="terminalgen_corpus_publications_run_uidx",
        ),
        UniqueConstraint(
            "request_artifact_id",
            name="terminalgen_corpus_publications_request_uidx",
        ),
        Index(
            "terminalgen_corpus_publications_team_state_idx",
            "team_id",
            "state",
            "finished_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    pipeline_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="RESTRICT"), nullable=False
    )
    request_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="RESTRICT"), nullable=False
    )
    request_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    corpus_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminalgen_corpus_versions.id", ondelete="RESTRICT"),
    )
    reason_code: Mapped[str | None] = mapped_column(Text)
    receipt_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    receipt_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    receipt_sha256: Mapped[str | None] = mapped_column(Text)
    finished_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class Token(Base):
    __tablename__ = "tokens"
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=True,
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    # Plan 13 A13.3: service layer + auth helpers debounce-update this
    # on each successful verify_bearer_token, so /teams/{id}/members can
    # surface last-active without each route writing per-request.
    last_seen_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class RateCard(Base):
    __tablename__ = "rate_cards"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    captured_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    table: Mapped[dict[str, Any]] = mapped_column("table", JSONB, nullable=False)


class PriceCatalog(Base):
    """One public supplier or team-private catalog; replace prices atomically."""

    __tablename__ = "price_catalogs"
    __table_args__ = (
        CheckConstraint("supplier_id IS NULL OR team_id IS NULL", name="price_catalog_supplier_public"),
        Index("price_catalogs_team_idx", "team_id"),
    )
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE"), nullable=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    supplier_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    prices: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    aliases: Mapped[dict[str, str]] = mapped_column(JSONB, nullable=False, default=dict)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    updated_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    checked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ModelSwitchPlan(Base):
    """Immutable K1/K2 plan for a terminus-2 multi-model trial (#1380)."""

    __tablename__ = "model_switch_plans"
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    combination_idx: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mix_mode: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="student_teacher_student",
    )
    k1: Mapped[int | None] = mapped_column(Integer, nullable=True)
    k2: Mapped[int | None] = mapped_column(Integer, nullable=True)
    teacher_episodes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    beta: Mapped[float | None] = mapped_column(Float, nullable=True)
    seed: Mapped[str] = mapped_column(Text, nullable=False)
    prng_version: Mapped[str] = mapped_column(Text, nullable=False)
    student_model_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    teacher_model_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    provider_connection_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="RESTRICT"),
        nullable=True,
    )
    pricing_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    capability_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    inherited_from_plan_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("model_switch_plans.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class TerminusAgentExecution(Base):
    """One Terminus2 session per agent phase (not pipeline execution_attempts)."""

    __tablename__ = "terminus_agent_executions"
    __table_args__ = (
        UniqueConstraint("trial_id", "step_id", name="terminus_agent_executions_trial_step_uidx"),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
    )
    step_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_switch_plan_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("model_switch_plans.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class TerminusAgentRunAttempt(Base):
    """Worker reclaim tenure for one Terminus2 execution."""

    __tablename__ = "terminus_agent_run_attempts"
    __table_args__ = (
        UniqueConstraint(
            "execution_id",
            "attempt_number",
            name="terminus_agent_run_attempts_exec_num_uidx",
        ),
        CheckConstraint(
            "state IN ('running','succeeded','failed','recovery_failed')",
            name="terminus_agent_run_attempts_state_check",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    execution_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False, default="running")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class EpisodeCheckpoint(Base):
    """Multi-model progress marker for terminus reclaim (not Harbor resume)."""

    __tablename__ = "episode_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "execution_id",
            "version",
            name="episode_checkpoints_exec_version_uidx",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    execution_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    run_attempt_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_run_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    episode: Mapped[int] = mapped_column(Integer, nullable=False)
    tmux_session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_role: Mapped[str] = mapped_column(Text, nullable=False)
    last_call_ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class LlmCallIntent(Base):
    """Registered before upstream I/O so gateway rows correlate by id (#1380)."""

    __tablename__ = "llm_call_intents"
    client_call_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        unique=True,
        nullable=False,
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
    )
    step_id: Mapped[str] = mapped_column(Text, nullable=False)
    agent_execution_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_executions.id", ondelete="SET NULL"),
        nullable=True,
    )
    agent_run_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_run_attempts.id", ondelete="SET NULL"),
        nullable=True,
    )
    episode: Mapped[int] = mapped_column(Integer, nullable=False)
    call_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_model: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="registered")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class GatewayDispatchReceipt(Base):
    """Non-billing admission/transport observations, never execution evidence."""

    __tablename__ = "gateway_dispatch_receipts"
    __table_args__ = (
        UniqueConstraint("request_id", "dispatch_ordinal", name="gateway_receipt_request_uidx"),
        CheckConstraint(
            "(trial_id IS NOT NULL)::integer + (execution_attempt_id IS NOT NULL)::integer = 1",
            name="gateway_receipt_subject_check",
        ),
        CheckConstraint("dispatch_ordinal > 0 AND attempt > 0", name="gateway_receipt_count_check"),
        CheckConstraint(
            "provider_outcome IN ('admitted','response_received','stream_completed',"
            "'deadline','cancelled','transport_error','not_dispatched')",
            name="gateway_receipt_provider_check",
        ),
        CheckConstraint(
            "gateway_outcome IN ('pending','completed','error','cancelled','deadline')",
            name="gateway_receipt_gateway_check",
        ),
        CheckConstraint(
            "purpose IN ('model_call','capability_probe','adapter_call')",
            name="gateway_receipt_purpose_check",
        ),
        Index("gateway_receipt_trial_idx", "trial_id", "admitted_at"),
        Index("gateway_receipt_pending_idx", "provider_outcome", "admitted_at"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    request_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    dispatch_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE"), nullable=False
    )
    trial_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("trials.id", ondelete="CASCADE"), nullable=True
    )
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_attempts.id", ondelete="CASCADE"), nullable=True
    )
    step_id: Mapped[str] = mapped_column(Text, nullable=False)
    step_jwt_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    agent_attempt_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    provider_connection_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    dialect: Mapped[str] = mapped_column(Text, nullable=False)
    purpose: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_deadline_wall_clock: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    admitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    provider_outcome: Mapped[str] = mapped_column(Text, nullable=False, server_default="admitted")
    provider_observed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    provider_http_status: Mapped[int | None] = mapped_column(Integer)
    gateway_outcome: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    gateway_observed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    gateway_http_status: Mapped[int | None] = mapped_column(Integer)


class LlmCall(Base):
    """One row per LLM call routed through the Gateway. Written by every
    dialect endpoint after the upstream provider returns. Read by the
    worker at trial finalize to project LLMCallEvents into the trial's
    trajectory JSONL before ATIF projection runs."""

    __tablename__ = "llm_calls"
    __table_args__ = (
        CheckConstraint(
            "(trial_id IS NOT NULL)::integer + (execution_attempt_id IS NOT NULL)::integer = 1",
            name="llm_calls_exactly_one_subject_check",
        ),
        Index("llm_calls_execution_attempt_idx", "execution_attempt_id", "captured_at"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    trial_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    execution_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="RESTRICT"),
        nullable=True,
    )
    step_id: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    dialect: Mapped[str] = mapped_column(Text, nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_extras: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    request_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
    )
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_card_hash: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    # #298 Slice B (migration 0029): which gateway-internal attempt
    # produced this successful row. 1 = first try (the historical
    # case and the server default). > 1 = the gateway retried N-1
    # transient failures before this attempt succeeded. Operators
    # query `MAX(attempt) GROUP BY trial_id` to find trials that
    # suffered retries without parsing logs.
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    client_call_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=True,
    )
    agent_execution_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_executions.id", ondelete="SET NULL"),
        nullable=True,
    )
    agent_run_attempt_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("terminus_agent_run_attempts.id", ondelete="SET NULL"),
        nullable=True,
    )
    episode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    call_ordinal: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_model: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_model: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'legacy_uncorrelated'"),
        default="legacy_uncorrelated",
    )


class TrialEvent(Base):
    """One row per typed trajectory event a trial emits (#5 Slice 3a).

    Writers append events here so Phase 2's SSE endpoint can flip
    from MinIO-poll-based to LISTEN/NOTIFY push, and so analytics /
    debug paths can query by kind/source without scanning the
    per-trial MinIO JSONL.

    `seq` is the event's logical sequence within its trial (matches
    the existing `_EventBase.seq` envelope field in
    `loom.models.trajectory`). UNIQUE (trial_id, seq) doubles as the
    idempotency key — workers INSERT ... ON CONFLICT DO NOTHING so
    a retry after a partial batch ack doesn't dupe.

    `payload` is the full typed event body (matching the Pydantic
    union); `kind` mirrors `payload.kind` so cursor reads can filter
    without unpacking JSONB; `source` records the emitting subsystem
    ('worker' | 'control-plane' | future 'scheduler' | etc.); and
    `schema_version` lets readers interpret payloads against the
    version they were emitted under.

    `created_at` is the DB insert time (NOT the worker's
    `emitted_at` which lives in payload) — used as a tie-breaker
    for ORDER BY when seq is ambiguous and for retention sweeps.
    """

    __tablename__ = "trial_events"
    __table_args__ = (
        CheckConstraint("seq >= 0", name="trial_events_seq_nonneg_check"),
        CheckConstraint(
            "schema_version >= 1",
            name="trial_events_schema_version_positive_check",
        ),
        UniqueConstraint(
            "trial_id",
            "seq",
            name="trial_events_trial_seq_uidx",
        ),
        Index(
            "trial_events_trial_created_at_idx",
            "trial_id",
            "created_at",
        ),
        Index("trial_events_kind_idx", "kind"),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid4,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
    )
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class TrialResourceUsage(Base):
    """Durable resource counters for one trial-attempt execution container."""

    __tablename__ = "trial_resource_usage"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
    )
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="RESTRICT"),
        nullable=True,
    )
    execution_lease_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("execution_leases.id", ondelete="RESTRICT")
    )
    resource_generation: Mapped[int | None] = mapped_column(Integer)
    target_id: Mapped[str | None] = mapped_column(Text)
    pod_uid: Mapped[str | None] = mapped_column(Text)
    cpu_sampled_max_nanocores: Mapped[int | None] = mapped_column(BigInteger)
    memory_sampled_max_bytes: Mapped[int | None] = mapped_column(BigInteger)
    filesystem_sampled_max_bytes: Mapped[int | None] = mapped_column(BigInteger)
    ephemeral_storage_sampled_max_bytes: Mapped[int | None] = mapped_column(BigInteger)
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_key: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_id_hash: Mapped[str | None] = mapped_column(Text)
    container_role: Mapped[str] = mapped_column(Text, nullable=False)
    role_name: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str] = mapped_column(Text, nullable=False)
    architecture: Mapped[str | None] = mapped_column(Text)
    candidate_sha: Mapped[str | None] = mapped_column(Text)
    image_digest: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    observation_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    container_started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    first_observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    terminal_reason: Mapped[str | None] = mapped_column(Text)
    completeness: Mapped[str] = mapped_column(Text, nullable=False)
    diagnostic_code: Mapped[str | None] = mapped_column(Text)
    cpu_limit_cores: Mapped[float | None] = mapped_column(Float)
    memory_limit_bytes: Mapped[int | None] = mapped_column(BigInteger)
    pids_limit: Mapped[int | None] = mapped_column(Integer)
    resource_profile: Mapped[str | None] = mapped_column(Text)
    cpu_usage_usec: Mapped[int | None] = mapped_column(BigInteger)
    cpu_user_usec: Mapped[int | None] = mapped_column(BigInteger)
    cpu_system_usec: Mapped[int | None] = mapped_column(BigInteger)
    cpu_throttled_usec: Mapped[int | None] = mapped_column(BigInteger)
    cpu_periods: Mapped[int | None] = mapped_column(BigInteger)
    cpu_throttled_periods: Mapped[int | None] = mapped_column(BigInteger)
    memory_current_bytes: Mapped[int | None] = mapped_column(BigInteger)
    memory_peak_bytes: Mapped[int | None] = mapped_column(BigInteger)
    memory_events_low: Mapped[int | None] = mapped_column(BigInteger)
    memory_events_high: Mapped[int | None] = mapped_column(BigInteger)
    memory_events_max: Mapped[int | None] = mapped_column(BigInteger)
    memory_events_oom: Mapped[int | None] = mapped_column(BigInteger)
    memory_events_oom_kill: Mapped[int | None] = mapped_column(BigInteger)
    pids_current: Mapped[int | None] = mapped_column(BigInteger)
    pids_peak: Mapped[int | None] = mapped_column(BigInteger)
    io_read_bytes: Mapped[int | None] = mapped_column(BigInteger)
    io_write_bytes: Mapped[int | None] = mapped_column(BigInteger)
    io_read_ops: Mapped[int | None] = mapped_column(BigInteger)
    io_write_ops: Mapped[int | None] = mapped_column(BigInteger)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )


class Secret(Base):
    """One row per encrypted secret value managed by the `local-encrypted`
    SecretStore impl (cluster-deploy.md §Secrets). The `ref` is an opaque
    string of the form "loom://<namespace>/<uuid>" that callers store
    as the encrypted_api_key_ref on the consuming row (e.g.,
    provider_connections). master_key_version is bumped by the rotation
    walker when LOOM_SECRET_STORE_MASTER_KEY changes; pre-rotation rows
    are re-encrypted in place inside the rotation transaction."""

    __tablename__ = "secrets"
    __table_args__ = (
        Index(
            "secrets_provider_retired_idx", "provider_retired_at", "ref",
            postgresql_where=text("provider_retired_at IS NOT NULL"),
        ),
    )
    provider_retired_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    ref: Mapped[str] = mapped_column(Text, primary_key=True)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    # 12-byte AES-GCM nonce.
    nonce: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    master_key_version: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderConnection(Base):
    """Per-team record for a user-supplied LLM provider endpoint
    (cluster-deploy.md §Schema additions). Soft-deleted via `deleted_at`
    so in-flight trials' FKs stay valid for billing/audit; the partial
    UNIQUE index on (team_id, display_name) WHERE deleted_at IS NULL
    lets the operator reuse a name after deletion.

    `resolved_egress_ips` is the union (with bounded 24h window) of
    IPs the upstream_host has resolved to recently; populated by the
    re-resolver background task (advisory-lock-protected leader). The
    egress proxy gates outbound traffic to `target_ip ∈
    resolved_egress_ips[connection_id]`.

    `encrypted_api_key_ref` is opaque to the application: for the
    local-encrypted impl it's "loom://..."; for k8s-secret it's
    "k8s://ns/name". SecretStore.dispatch routes by URL scheme.
    """

    __tablename__ = "provider_connections"
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    provider_type: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    base_url: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_host: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_egress_ips: Mapped[list[str]] = mapped_column(
        ARRAY(INET),
        nullable=False,
        server_default=text("ARRAY[]::inet[]"),
    )
    egress_ips_refreshed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    egress_ips_min_ttl_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("300"),
    )
    encrypted_api_key_ref: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_models: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
    )
    last_validated_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # #277 / responses-api-support-probe: cache whether this upstream
    # implements POST /v1/responses. NULL means "never probed" — the
    # gateway probes at first use and refreshes on a TTL. TRUE routes
    # native pass-through; FALSE dispatches into responses_chat_compat.
    responses_api_supported: Mapped[bool | None] = mapped_column(
        Boolean,
        nullable=True,
    )
    responses_api_probed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    responses_api_probe_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    pricing_config: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    pricing_source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'tokens-only'"),
    )
    pricing_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    rate_card_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    # Maintained by the trigger created in migration 0018 — updates
    # automatically on every UPDATE. Don't set explicitly in app code.
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderConnectionShare(Base):
    """Authorization for one target team to use another team's provider.

    The source connection and its encrypted secret remain owned by the
    provider team. Sharing grants use/list/read access only; mutation and
    rotation stay with the owner team or platform admins.
    """

    __tablename__ = "provider_connection_shares"
    __table_args__ = (
        Index(
            "provider_connection_shares_target_team_idx",
            "target_team_id",
        ),
    )
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="CASCADE"),
        primary_key=True,
    )
    target_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        primary_key=True,
    )
    created_by_actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderModelCache(Base):
    """Per-connection enumeration of upstream models. Refreshed on a 1h
    TTL on read; not hard-deleted when a model disappears upstream —
    `upstream_present` flips to false (audit trail). Operator can
    override visibility independently via `visible` + `hidden_reason`.
    """

    __tablename__ = "provider_models_cache"
    __table_args__ = (
        CheckConstraint(
            "last_preflight_status IS NULL OR last_preflight_status IN ('valid', 'failed')",
            name="provider_models_cache_preflight_status_check",
        ),
    )
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="CASCADE"),
        primary_key=True,
    )
    model_id: Mapped[str] = mapped_column(Text, primary_key=True)
    family: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capabilities: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
    )
    visible: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    hidden_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    upstream_present: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    # Per-model entitlement probe. NULL means discovered/manual but not
    # yet preflighted. Failed entries can warn/block before submission.
    last_preflight_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_preflight_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_preflight_http_status: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    last_preflight_error_code: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    last_preflight_error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )


class ActiveTrialCacheBuild(Base):
    """One row per in-progress cache build for a (task_image_digest,
    install_script) combination (#317 Phase 1).

    Workers claim a slot here before docker-building the layered
    image; subsequent workers see the slot and wait via cheap SELECT
    polling. Crash safety via `expires_at`: when a builder's process
    dies, its heartbeat task stops refreshing the TTL; within ~60s
    the slot expires and another worker can steal it via
    `INSERT ON CONFLICT WHERE expires_at < now()`.

    NOT a Postgres advisory lock — those tie up a CP connection for
    the build duration; with N waiters that exhausts the CP pool.
    This table uses short transactions only (claim, exists, refresh,
    release each are one statement).
    """

    __tablename__ = "active_trial_cache_builds"
    cache_key: Mapped[str] = mapped_column(Text, primary_key=True)
    builder_worker_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
