"""Retire empty obsolete feature tables without deleting historical records.

Revision ID: 0171
Revises: 0170

The five task-image authority/evidence tables used by retained image-history
fixtures and their foreign keys are intentionally excluded. Published legacy migration chains remain.
The downgrade DDL is frozen from PostgreSQL 16 at revision 0166; it restores
empty structure, not discarded data (upgrade refuses every nonempty table).
"""

from alembic import op
from sqlalchemy import text

revision = "0171"
down_revision = "0170"
branch_labels = None
depends_on = None

RETIRED_TABLES = (
    "personal_dev_candidates",
    "personal_dev_candidate_artifact_collections",
    "personal_dev_candidate_build_attempts",
    "personal_dev_build_platform_requests",
    "personal_dev_native_builder_agents",
    "personal_dev_native_build_grants",
    "dev_lifecycle_operations",
    "dev_lifecycle_operation_attempts",
    "dev_lifecycle_activation_acknowledgements",
    "pipeline_scoped_policy_activations",
    "pipeline_run_gpu_backend_selections",
    "pipeline_stage1_smoke_authorizations",
    "pipeline_stage1_smoke_events",
    "pipeline_input_materialization_evidence",
    "pipeline_acceptance_evidence_runs",
    "dev_instances",
    "slurm_worker_jobs",
    "gb10_worker_pool_desired_states",
    "gb10_worker_node_statuses",
    "worker_pool_autoscaler_policies",
)

RETIRED_FUNCTIONS = (
    "guard_personal_build_platform_request",
    "loom_check_dev_lifecycle_current_attempt",
    "loom_check_dev_membership_successor_complete",
    "loom_check_personal_storage_handoff",
    "loom_guard_dev_lifecycle_activation_acknowledgement",
    "loom_guard_dev_lifecycle_attempt_binding",
    "loom_guard_dev_lifecycle_operation_binding",
    "loom_guard_dev_membership_lineage",
    "loom_guard_dev_membership_successor_attempt",
    "loom_guard_personal_storage_binding",
    "reject_personal_dev_artifact_collection_mutation",
)


def upgrade() -> None:
    bind = op.get_bind()
    op.execute("SET LOCAL lock_timeout = '2s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("SET LOCAL row_security = off")
    targets = ", ".join(f'public."{name}"' for name in RETIRED_TABLES)
    # Lock the complete fixed set before inspecting any rows. No CASCADE: an
    # unexpected dependent object must abort the entire transactional migration.
    op.execute(f"LOCK TABLE {targets} IN ACCESS EXCLUSIVE MODE NOWAIT")
    for name in RETIRED_TABLES:
        if bind.scalar(text(f'SELECT EXISTS (SELECT 1 FROM public."{name}")')):
            raise RuntimeError(f"legacy retirement requires retained-row disposition: {name}")
    dependencies = bind.execute(
        text(r"""
        SELECT n.nspname, p.proname FROM pg_proc p
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND p.prosrc ~ :pattern
            AND NOT (n.nspname='public' AND p.proname=ANY(:functions) AND p.pronargs=0)
        ORDER BY n.nspname, p.proname
    """),
        {
            "pattern": r"\m(" + "|".join(RETIRED_TABLES) + r")\M",
            "functions": list(RETIRED_FUNCTIONS),
        },
    ).all()
    if dependencies:
        raise RuntimeError("legacy retirement requires dependent SQL routine disposition")
    op.execute(f"DROP TABLE {targets}")
    for name in RETIRED_FUNCTIONS:
        op.execute(f'DROP FUNCTION public."{name}"()')


def downgrade() -> None:
    # Recreate only the removed empty structures. Current tables, records and
    # the shared provider-secret attachment function are never recreated/reset.
    op.execute(_RESTORE_SQL)


# Generated from an isolated, fully migrated 0166 database; no live data,
# ownership identities, grants, secrets or mutable imports are embedded.
_RESTORE_SQL = r"""
CREATE TABLE public.dev_instances (
    name text NOT NULL,
    owner_user_id uuid NOT NULL,
    owner_team_id uuid NOT NULL,
    min_slots integer DEFAULT 0 NOT NULL,
    max_slots integer NOT NULL,
    status text DEFAULT 'provisioning'::text NOT NULL,
    deployment_generation bigint NOT NULL,
    candidate_sha character varying(64) NOT NULL,
    operation_epoch bigint DEFAULT 1 NOT NULL,
    operation_id uuid NOT NULL,
    operation_step character varying(32) DEFAULT 'claimed'::character varying NOT NULL,
    secret_ref text,
    keep_data boolean DEFAULT false NOT NULL,
    failure_reason character varying(256),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    ready_at timestamp with time zone,
    deleted_at timestamp with time zone,
    subject_id uuid DEFAULT gen_random_uuid() NOT NULL,
    subject_incarnation uuid DEFAULT gen_random_uuid() NOT NULL,
    candidate_id uuid,
    capacity_configuration_epoch bigint,
    capacity_configuration_sha256 character varying(64),
    capacity_reporter_token_sha256 character varying(64),
    local_activation_sha256 character varying(64),
    protected_admission_sha256 character varying(64),
    capacity_agent_installation_sha256 character varying(64),
    capacity_reporter_incarnation uuid,
    capacity_supported_pool_ids jsonb,
    capacity_supported_architectures jsonb,
    capacity_namespace text,
    capacity_database text,
    accepted_capacity_mode text DEFAULT 'shadow-v1'::text NOT NULL,
    accepted_capacity_membership_checkpoint jsonb,
    storage_binding jsonb,
    storage_binding_sha256 character varying(64),
    CONSTRAINT dev_instances_accepted_capacity_mode_check CHECK (((accepted_capacity_mode = ANY (ARRAY['shadow-v1'::text, 'membership-v1'::text])) AND (((accepted_capacity_mode = 'shadow-v1'::text) AND (accepted_capacity_membership_checkpoint IS NULL)) OR (((accepted_capacity_mode = 'membership-v1'::text) AND (accepted_capacity_membership_checkpoint IS NOT NULL) AND (jsonb_typeof(accepted_capacity_membership_checkpoint) = 'object'::text) AND ((accepted_capacity_membership_checkpoint ->> 'schema_version'::text) = '1'::text) AND (jsonb_typeof((accepted_capacity_membership_checkpoint -> 'execution'::text)) = 'object'::text) AND ((accepted_capacity_membership_checkpoint ->> 'namespace_id'::text) IS NOT NULL) AND (jsonb_typeof((accepted_capacity_membership_checkpoint -> 'revision'::text)) = 'number'::text) AND ((accepted_capacity_membership_checkpoint ->> 'head_sha256'::text) ~ '^[0-9a-f]{64}$'::text) AND (capacity_reporter_incarnation IS NOT NULL) AND (capacity_reporter_token_sha256 IS NOT NULL) AND (local_activation_sha256 IS NOT NULL) AND (protected_admission_sha256 IS NOT NULL) AND (capacity_agent_installation_sha256 IS NOT NULL) AND (capacity_supported_pool_ids IS NOT NULL) AND (capacity_supported_architectures IS NOT NULL)) IS TRUE)))),
    CONSTRAINT dev_instances_candidate_sha_check CHECK ((((candidate_id IS NULL) AND ((candidate_sha)::text ~ '^[0-9a-f]{40}$'::text)) OR ((candidate_id IS NOT NULL) AND ((candidate_sha)::text ~ '^[0-9a-f]{64}$'::text)))),
    CONSTRAINT dev_instances_capacity_projection_check CHECK ((((capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL) AND (capacity_reporter_incarnation IS NULL) AND (capacity_reporter_token_sha256 IS NULL) AND (local_activation_sha256 IS NULL) AND (protected_admission_sha256 IS NULL) AND (capacity_agent_installation_sha256 IS NULL) AND (capacity_supported_pool_ids IS NULL) AND (capacity_supported_architectures IS NULL)) OR ((capacity_reporter_incarnation IS NOT NULL) AND ((capacity_reporter_token_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((local_activation_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((protected_admission_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((capacity_agent_installation_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (jsonb_typeof(capacity_supported_pool_ids) = 'array'::text) AND (jsonb_array_length(capacity_supported_pool_ids) > 0) AND (jsonb_typeof(capacity_supported_architectures) = 'array'::text) AND (jsonb_array_length(capacity_supported_architectures) > 0) AND (((accepted_capacity_mode = 'shadow-v1'::text) AND (capacity_configuration_epoch > 0) AND ((capacity_configuration_sha256)::text ~ '^[0-9a-f]{64}$'::text)) OR ((accepted_capacity_mode = 'membership-v1'::text) AND (capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL)))))),
    CONSTRAINT dev_instances_deployment_generation_check CHECK ((deployment_generation > 0)),
    CONSTRAINT dev_instances_name_check CHECK ((name ~ '^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$'::text)),
    CONSTRAINT dev_instances_operation_epoch_check CHECK ((operation_epoch > 0)),
    CONSTRAINT dev_instances_personal_capacity_identity_check CHECK ((((candidate_id IS NULL) AND (capacity_namespace IS NULL) AND (capacity_database IS NULL)) OR ((candidate_id IS NOT NULL) AND (capacity_namespace IS NOT NULL) AND (capacity_database IS NOT NULL) AND (capacity_namespace = ('loom-dev-'::text || name)) AND (capacity_database =
CASE
    WHEN (storage_binding IS NULL) THEN ('loom_dev_'::text || replace(name, '-'::text, '_'::text))
    ELSE ((('ld_'::text || replace(name, '-'::text, '_'::text)) || '_'::text) || replace((subject_incarnation)::text, '-'::text, ''::text))
END)))),
    CONSTRAINT dev_instances_personal_readiness_capacity_check CHECK (((status <> 'ready'::text) OR (candidate_id IS NULL) OR (((accepted_capacity_mode = 'shadow-v1'::text) AND (capacity_configuration_epoch IS NOT NULL)) OR ((accepted_capacity_mode = 'membership-v1'::text) AND (accepted_capacity_membership_checkpoint IS NOT NULL))))),
    CONSTRAINT dev_instances_slots_check CHECK (((min_slots >= 0) AND (max_slots >= min_slots) AND (max_slots <= 8))),
    CONSTRAINT dev_instances_status_check CHECK ((status = ANY (ARRAY['provisioning'::text, 'ready'::text, 'updating'::text, 'activating'::text, 'deleting'::text, 'draining'::text, 'failed'::text, 'deleted'::text]))),
    CONSTRAINT dev_instances_storage_binding_check CHECK ((((storage_binding IS NULL) AND (storage_binding_sha256 IS NULL)) OR (((storage_binding IS NOT NULL) AND ((storage_binding_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((storage_binding_sha256)::text <> repeat('0'::text, 64)) AND ((storage_binding ->> 'schema_version'::text) = '1'::text) AND (subject_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (subject_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid) AND (owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (name <> ALL (ARRAY['dev'::text, 'development'::text, 'staging'::text, 'production'::text, 'prod'::text, 'local'::text, 'loom'::text, 'shared'::text, 'default'::text])) AND (storage_binding = jsonb_build_object('schema_version', 1, 'layout', 'incarnation-v1', 'environment_name', name, 'subject_id', (subject_id)::text, 'subject_incarnation', (subject_incarnation)::text, 'owner_user_id', (owner_user_id)::text, 'owner_team_id', (owner_team_id)::text))) IS TRUE)))
);
CREATE TABLE public.dev_lifecycle_activation_acknowledgements (
    operation_id uuid NOT NULL,
    environment_name text NOT NULL,
    subject_id uuid NOT NULL,
    subject_incarnation uuid NOT NULL,
    operation_epoch bigint NOT NULL,
    attempt_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    candidate_sha character varying(64) NOT NULL,
    deployment_generation bigint NOT NULL,
    readiness_evidence_sha256 character varying(64) NOT NULL,
    local_activation_sha256 character varying(64) NOT NULL,
    payload_sha256 character varying(64) NOT NULL,
    signature_sha256 character varying(64) NOT NULL,
    agent_key_id character varying(64) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT dev_lifecycle_activation_acknowledgements_counters_check CHECK (((operation_epoch > 0) AND (deployment_generation > 0))),
    CONSTRAINT dev_lifecycle_activation_acknowledgements_digests_check CHECK ((((candidate_sha)::text ~ '^[0-9a-f]{64}$'::text) AND ((readiness_evidence_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((local_activation_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((payload_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((signature_sha256)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT dev_lifecycle_activation_acknowledgements_key_check CHECK (((agent_key_id)::text ~ '^[a-z][a-z0-9._-]{0,63}$'::text))
);
CREATE TABLE public.dev_lifecycle_operation_attempts (
    id uuid NOT NULL,
    operation_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    subject_incarnation uuid NOT NULL,
    operation_epoch bigint NOT NULL,
    attempt_sequence integer NOT NULL,
    state character varying(16) NOT NULL,
    checkpoint character varying(32) NOT NULL,
    failure_reason character varying(256),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    credential_binding_version integer DEFAULT 1 NOT NULL,
    bootstrap_auth_kind character varying(16) NOT NULL,
    bootstrap_credential_hash bytea NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    claimed_by character varying(128),
    lease_expires_at timestamp with time zone,
    CONSTRAINT dev_lifecycle_operation_attempts_counters_check CHECK (((operation_epoch > 0) AND (attempt_sequence >= 0) AND (lease_epoch >= 0))),
    CONSTRAINT dev_lifecycle_operation_attempts_credential_check CHECK (((credential_binding_version = 1) AND ((bootstrap_auth_kind)::text = ANY ((ARRAY['bearer'::character varying, 'session'::character varying])::text[])) AND (octet_length(bootstrap_credential_hash) = 32))),
    CONSTRAINT dev_lifecycle_operation_attempts_lease_check CHECK ((((claimed_by IS NULL) AND (lease_expires_at IS NULL)) OR ((claimed_by IS NOT NULL) AND (lease_expires_at IS NOT NULL)))),
    CONSTRAINT dev_lifecycle_operation_attempts_state_check CHECK ((((state)::text = ANY ((ARRAY['running'::character varying, 'activating'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelled'::character varying])::text[])) OR ((state)::text = 'superseded'::text))),
    CONSTRAINT dev_lifecycle_operation_attempts_terminal_fields_check CHECK (((((state)::text = ANY ((ARRAY['running'::character varying, 'activating'::character varying])::text[])) AND (finished_at IS NULL) AND (failure_reason IS NULL)) OR (((state)::text = 'succeeded'::text) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL)) OR (((state)::text = ANY ((ARRAY['failed'::character varying, 'cancelled'::character varying])::text[])) AND (finished_at IS NOT NULL)) OR (((state)::text = 'superseded'::text) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL))))
);
CREATE TABLE public.dev_lifecycle_operations (
    id uuid NOT NULL,
    idempotency_key uuid NOT NULL,
    environment_name text NOT NULL,
    subject_id uuid NOT NULL,
    subject_incarnation uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    owner_team_id uuid NOT NULL,
    operation_epoch bigint NOT NULL,
    expected_operation_epoch bigint NOT NULL,
    kind character varying(16) NOT NULL,
    state character varying(16) NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_sequence integer DEFAULT 0 NOT NULL,
    request_sha256 character varying(64) NOT NULL,
    candidate_id uuid NOT NULL,
    candidate_sha character varying(64) NOT NULL,
    min_slots integer NOT NULL,
    max_slots integer NOT NULL,
    deployment_generation bigint NOT NULL,
    readiness_evidence_sha256 character varying(64),
    activation_acknowledgement_sha256 character varying(64),
    checkpoint character varying(32) DEFAULT 'claimed'::character varying NOT NULL,
    failure_reason character varying(256),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    local_activation_sha256 character varying(64),
    capacity_expected_configuration_epoch bigint,
    capacity_projection_request_sha256 character varying(64),
    capacity_configuration_epoch bigint,
    capacity_configuration_sha256 character varying(64),
    capacity_reporter_incarnation uuid,
    capacity_reporter_token_sha256 character varying(64),
    protected_admission_sha256 character varying(64),
    capacity_agent_installation_sha256 character varying(64),
    capacity_supported_pool_ids jsonb,
    capacity_supported_architectures jsonb,
    keep_data boolean DEFAULT false NOT NULL,
    capacity_mode text DEFAULT 'shadow-v1'::text NOT NULL,
    capacity_membership_envelope jsonb,
    membership_predecessor_operation_id uuid,
    membership_accepted_operation_id uuid,
    membership_predecessor_envelope_sha256 character varying(64),
    membership_successor_binding jsonb,
    membership_successor_binding_sha256 character varying(64),
    membership_continuation_kind character varying(16),
    storage_binding jsonb,
    storage_binding_sha256 character varying(64),
    CONSTRAINT dev_lifecycle_operations_activation_evidence_check CHECK (((((kind)::text = ANY ((ARRAY['capacity'::character varying, 'destroy'::character varying, 'noop'::character varying])::text[])) AND (readiness_evidence_sha256 IS NULL) AND (activation_acknowledgement_sha256 IS NULL)) OR (((kind)::text = ANY ((ARRAY['create'::character varying, 'update'::character varying])::text[])) AND ((((state)::text = ANY ((ARRAY['requested'::character varying, 'running'::character varying, 'failed'::character varying, 'cancelling'::character varying, 'cancelled'::character varying])::text[])) AND (readiness_evidence_sha256 IS NULL) AND (activation_acknowledgement_sha256 IS NULL)) OR (((state)::text = 'activating'::text) AND (readiness_evidence_sha256 IS NOT NULL)) OR (((state)::text = 'succeeded'::text) AND (readiness_evidence_sha256 IS NOT NULL) AND (activation_acknowledgement_sha256 IS NOT NULL)))) OR (((state)::text = 'superseded'::text) AND ((kind)::text = ANY ((ARRAY['create'::character varying, 'update'::character varying])::text[])) AND (readiness_evidence_sha256 IS NOT NULL) AND (activation_acknowledgement_sha256 IS NOT NULL)))),
    CONSTRAINT dev_lifecycle_operations_capacity_completion_check CHECK (((((checkpoint)::text <> 'pre_activation_abandoned'::text) OR (((kind)::text = 'destroy'::text) AND ((state)::text = 'succeeded'::text) AND (readiness_evidence_sha256 IS NULL) AND (activation_acknowledgement_sha256 IS NULL) AND (local_activation_sha256 IS NULL) AND (capacity_expected_configuration_epoch IS NULL) AND (capacity_projection_request_sha256 IS NULL) AND (capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL) AND (capacity_reporter_incarnation IS NULL) AND (capacity_reporter_token_sha256 IS NULL) AND (protected_admission_sha256 IS NULL) AND (capacity_agent_installation_sha256 IS NULL) AND (capacity_supported_pool_ids IS NULL) AND (capacity_supported_architectures IS NULL))) AND (((state)::text <> 'succeeded'::text) OR ((kind)::text = 'noop'::text) OR ((checkpoint)::text = 'pre_activation_abandoned'::text) OR ((capacity_mode = 'shadow-v1'::text) AND (capacity_configuration_epoch IS NOT NULL)) OR ((capacity_mode = 'membership-v1'::text) AND ((((capacity_membership_envelope -> 'result'::text) IS NOT NULL) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'object'::text)) OR (((kind)::text = 'destroy'::text) AND ((checkpoint)::text = 'complete'::text) AND (((capacity_membership_envelope -> 'historical_outcome'::text) ->> 'outcome'::text) = 'committed'::text) AND (jsonb_typeof(((capacity_membership_envelope -> 'historical_outcome'::text) -> 'receipt'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'object'::text))))))),
    CONSTRAINT dev_lifecycle_operations_capacity_mode_check CHECK (((capacity_mode = ANY (ARRAY['shadow-v1'::text, 'membership-v1'::text])) AND (((capacity_mode = 'shadow-v1'::text) AND (capacity_membership_envelope IS NULL)) OR ((capacity_mode = 'membership-v1'::text) AND (capacity_expected_configuration_epoch IS NULL) AND (capacity_projection_request_sha256 IS NULL) AND (capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL) AND ((capacity_membership_envelope IS NULL) OR (((jsonb_typeof(capacity_membership_envelope) = 'object'::text) AND ((capacity_membership_envelope ->> 'schema_version'::text) = '1'::text) AND ((capacity_membership_envelope ->> 'mode'::text) = 'membership-v1'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'request'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'observation'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'expected_checkpoint'::text)) = 'object'::text) AND ((capacity_membership_envelope ->> 'request_sha256'::text) ~ '^[0-9a-f]{64}$'::text) AND ((capacity_membership_envelope ->> 'idempotency_key'::text) IS NOT NULL) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = ANY (ARRAY['null'::text, 'object'::text])) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = ANY (ARRAY['null'::text, 'object'::text])) AND (((jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text) OR ((jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'object'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'schema_version'::text) = '1'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'outcome'::text) = 'verified'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'query_sha256'::text) ~ '^[0-9a-f]{64}$'::text) AND (jsonb_typeof(((capacity_membership_envelope -> 'release'::text) -> 'membership_receipt'::text)) = 'object'::text) AND (jsonb_typeof(((capacity_membership_envelope -> 'release'::text) -> 'current'::text)) = 'object'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'historical'::text) = 'true'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'worker_available'::text) = 'false'::text) AND (jsonb_typeof(((capacity_membership_envelope -> 'release'::text) -> 'incarnation_work'::text)) = 'object'::text) AND (((capacity_membership_envelope -> 'release'::text) ->> 'release_set_sha256'::text) ~ '^[0-9a-f]{64}$'::text))) IS TRUE)) IS TRUE)) AND (((kind)::text = 'noop'::text) OR ((checkpoint)::text <> ALL ((ARRAY['capacity_projection_pending'::character varying, 'capacity_projected'::character varying, 'cleanup_pending'::character varying, 'membership_outcome_resolved'::character varying, 'release_verified'::character varying, 'local_authority_sealed'::character varying, 'namespace_deleted'::character varying, 'database_deleted'::character varying, 'buckets_deleted'::character varying, 'tenant_deleted'::character varying, 'complete'::character varying])::text[])) OR ((capacity_membership_envelope IS NOT NULL) AND (capacity_reporter_incarnation IS NOT NULL) AND (capacity_reporter_token_sha256 IS NOT NULL) AND (local_activation_sha256 IS NOT NULL) AND (protected_admission_sha256 IS NOT NULL) AND (capacity_agent_installation_sha256 IS NOT NULL) AND (capacity_supported_pool_ids IS NOT NULL) AND (jsonb_typeof(capacity_supported_pool_ids) = 'array'::text) AND (jsonb_array_length(capacity_supported_pool_ids) > 0) AND (capacity_supported_architectures IS NOT NULL) AND (jsonb_typeof(capacity_supported_architectures) = 'array'::text) AND (jsonb_array_length(capacity_supported_architectures) > 0))))))),
    CONSTRAINT dev_lifecycle_operations_capacity_projection_check CHECK (((capacity_mode = 'membership-v1'::text) OR (((capacity_expected_configuration_epoch IS NULL) AND (capacity_projection_request_sha256 IS NULL) AND (capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL) AND (capacity_reporter_incarnation IS NULL) AND (capacity_reporter_token_sha256 IS NULL) AND (protected_admission_sha256 IS NULL) AND (capacity_agent_installation_sha256 IS NULL) AND (capacity_supported_pool_ids IS NULL) AND (capacity_supported_architectures IS NULL)) OR ((((kind)::text = 'destroy'::text) AND (capacity_expected_configuration_epoch IS NULL) AND (capacity_projection_request_sha256 IS NULL) AND (capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL) AND (local_activation_sha256 IS NOT NULL) AND (capacity_reporter_incarnation IS NOT NULL) AND (capacity_reporter_token_sha256 IS NOT NULL) AND (protected_admission_sha256 IS NOT NULL) AND (capacity_agent_installation_sha256 IS NOT NULL) AND (jsonb_typeof(capacity_supported_pool_ids) = 'array'::text) AND (jsonb_array_length(capacity_supported_pool_ids) > 0) AND (jsonb_typeof(capacity_supported_architectures) = 'array'::text) AND (jsonb_array_length(capacity_supported_architectures) > 0)) OR ((capacity_expected_configuration_epoch > 0) AND (local_activation_sha256 IS NOT NULL) AND (capacity_projection_request_sha256 IS NOT NULL) AND (capacity_reporter_incarnation IS NOT NULL) AND (capacity_reporter_token_sha256 IS NOT NULL) AND (protected_admission_sha256 IS NOT NULL) AND (capacity_agent_installation_sha256 IS NOT NULL) AND (jsonb_typeof(capacity_supported_pool_ids) = 'array'::text) AND (jsonb_array_length(capacity_supported_pool_ids) > 0) AND (jsonb_typeof(capacity_supported_architectures) = 'array'::text) AND (jsonb_array_length(capacity_supported_architectures) > 0) AND (((capacity_configuration_epoch IS NULL) AND (capacity_configuration_sha256 IS NULL)) OR ((capacity_configuration_epoch = (capacity_expected_configuration_epoch + 1)) AND (capacity_configuration_sha256 IS NOT NULL)))))))),
    CONSTRAINT dev_lifecycle_operations_digests_check CHECK ((((request_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((candidate_sha)::text ~ '^[0-9a-f]{64}$'::text) AND ((readiness_evidence_sha256 IS NULL) OR ((readiness_evidence_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((activation_acknowledgement_sha256 IS NULL) OR ((activation_acknowledgement_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((local_activation_sha256 IS NULL) OR ((local_activation_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((capacity_projection_request_sha256 IS NULL) OR ((capacity_projection_request_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((capacity_configuration_sha256 IS NULL) OR ((capacity_configuration_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((capacity_reporter_token_sha256 IS NULL) OR ((capacity_reporter_token_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((protected_admission_sha256 IS NULL) OR ((protected_admission_sha256)::text ~ '^[0-9a-f]{64}$'::text)) AND ((capacity_agent_installation_sha256 IS NULL) OR ((capacity_agent_installation_sha256)::text ~ '^[0-9a-f]{64}$'::text)))),
    CONSTRAINT dev_lifecycle_operations_epochs_check CHECK (((operation_epoch >= expected_operation_epoch) AND (operation_epoch <= (expected_operation_epoch + 1)) AND (expected_operation_epoch >= 0) AND (attempt_sequence >= 0))),
    CONSTRAINT dev_lifecycle_operations_kind_check CHECK (((kind)::text = ANY ((ARRAY['create'::character varying, 'update'::character varying, 'capacity'::character varying, 'destroy'::character varying, 'noop'::character varying])::text[]))),
    CONSTRAINT dev_lifecycle_operations_membership_completion_check CHECK ((((capacity_mode = 'shadow-v1'::text) OR ((kind)::text = 'noop'::text) OR ((checkpoint)::text = 'pre_activation_abandoned'::text) OR (((checkpoint)::text <> ALL ((ARRAY['capacity_projection_pending'::character varying, 'capacity_projected'::character varying, 'cleanup_pending'::character varying, 'membership_outcome_resolved'::character varying, 'release_verified'::character varying, 'local_authority_sealed'::character varying, 'namespace_deleted'::character varying, 'database_deleted'::character varying, 'buckets_deleted'::character varying, 'tenant_deleted'::character varying, 'complete'::character varying])::text[])) AND ((capacity_membership_envelope IS NULL) OR (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text))) OR (((checkpoint)::text = 'capacity_projection_pending'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text)) OR (((checkpoint)::text = 'membership_outcome_resolved'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text)) OR (((checkpoint)::text = 'capacity_projected'::text) AND (((capacity_membership_envelope -> 'result'::text) IS NOT NULL) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'object'::text)) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text)) OR (((checkpoint)::text = 'cleanup_pending'::text) AND ((kind)::text = 'destroy'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text)) OR (((kind)::text = 'destroy'::text) AND ((checkpoint)::text = ANY ((ARRAY['release_verified'::character varying, 'local_authority_sealed'::character varying, 'namespace_deleted'::character varying, 'database_deleted'::character varying, 'buckets_deleted'::character varying, 'tenant_deleted'::character varying, 'complete'::character varying])::text[])) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'object'::text) AND (((jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'null'::text)) OR ((jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'null'::text) AND (((capacity_membership_envelope -> 'historical_outcome'::text) ->> 'outcome'::text) = 'committed'::text) AND (jsonb_typeof(((capacity_membership_envelope -> 'historical_outcome'::text) -> 'receipt'::text)) = 'object'::text)))) OR (((checkpoint)::text = 'complete'::text) AND ((kind)::text <> 'destroy'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text))) IS TRUE)),
    CONSTRAINT dev_lifecycle_operations_state_check CHECK ((((state)::text = ANY ((ARRAY['requested'::character varying, 'running'::character varying, 'activating'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelling'::character varying, 'cancelled'::character varying])::text[])) OR ((state)::text = 'superseded'::text))),
    CONSTRAINT dev_lifecycle_operations_storage_binding_check CHECK ((((storage_binding IS NULL) AND (storage_binding_sha256 IS NULL)) OR (((storage_binding IS NOT NULL) AND ((storage_binding_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((storage_binding_sha256)::text <> repeat('0'::text, 64)) AND ((storage_binding ->> 'schema_version'::text) = '1'::text) AND (subject_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (subject_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid) AND (owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND (environment_name <> ALL (ARRAY['dev'::text, 'development'::text, 'staging'::text, 'production'::text, 'prod'::text, 'local'::text, 'loom'::text, 'shared'::text, 'default'::text])) AND (storage_binding = jsonb_build_object('schema_version', 1, 'layout', 'incarnation-v1', 'environment_name', environment_name, 'subject_id', (subject_id)::text, 'subject_incarnation', (subject_incarnation)::text, 'owner_user_id', (owner_user_id)::text, 'owner_team_id', (owner_team_id)::text))) IS TRUE))),
    CONSTRAINT dev_lifecycle_operations_successor_fields_check CHECK ((((membership_predecessor_operation_id IS NULL) AND (membership_accepted_operation_id IS NULL) AND (membership_predecessor_envelope_sha256 IS NULL) AND (membership_successor_binding IS NULL) AND (membership_successor_binding_sha256 IS NULL) AND (membership_continuation_kind IS NULL)) OR (((membership_predecessor_operation_id IS NOT NULL) AND (membership_predecessor_operation_id <> id) AND (capacity_mode = 'membership-v1'::text) AND ((kind)::text = ANY ((ARRAY['create'::character varying, 'update'::character varying, 'destroy'::character varying])::text[])) AND ((membership_predecessor_envelope_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((membership_predecessor_envelope_sha256)::text <> repeat('0'::text, 64)) AND ((membership_successor_binding_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((membership_successor_binding_sha256)::text <> repeat('0'::text, 64)) AND ((membership_continuation_kind)::text = ANY ((ARRAY['create'::character varying, 'update'::character varying, 'capacity'::character varying, 'destroy'::character varying])::text[])) AND (jsonb_typeof(membership_successor_binding) = 'object'::text) AND ((membership_successor_binding ->> 'schema_version'::text) = '1'::text) AND ((membership_successor_binding ->> 'predecessor_operation_id'::text) = (membership_predecessor_operation_id)::text) AND ((membership_successor_binding ->> 'predecessor_envelope_sha256'::text) = (membership_predecessor_envelope_sha256)::text) AND (NOT ((membership_successor_binding ->> 'accepted_operation_id'::text) IS DISTINCT FROM (membership_accepted_operation_id)::text)) AND ((membership_successor_binding ->> 'owner_team_id'::text) = (owner_team_id)::text)) IS TRUE))),
    CONSTRAINT dev_lifecycle_operations_superseded_check CHECK (((((state)::text = 'superseded'::text) = ((checkpoint)::text = 'membership_successor_created'::text)) AND (((state)::text <> 'superseded'::text) OR (((capacity_mode = 'membership-v1'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'result'::text)) = 'null'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'historical_outcome'::text)) = 'object'::text) AND (jsonb_typeof((capacity_membership_envelope -> 'release'::text)) = 'null'::text) AND (((capacity_membership_envelope -> 'historical_outcome'::text) ->> 'outcome'::text) = ANY (ARRAY['committed'::text, 'terminal-not-committed'::text])) AND (NOT (((kind)::text = 'destroy'::text) AND (((capacity_membership_envelope -> 'historical_outcome'::text) ->> 'outcome'::text) = 'committed'::text)))) IS TRUE)))),
    CONSTRAINT dev_lifecycle_operations_target_check CHECK (((min_slots >= 0) AND (max_slots >= min_slots) AND (max_slots <= 8) AND (deployment_generation > 0))),
    CONSTRAINT dev_lifecycle_operations_terminal_fields_check CHECK (((((state)::text = ANY ((ARRAY['requested'::character varying, 'running'::character varying, 'activating'::character varying, 'cancelling'::character varying])::text[])) AND (finished_at IS NULL) AND (failure_reason IS NULL)) OR (((state)::text = 'succeeded'::text) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL)) OR (((state)::text = ANY ((ARRAY['failed'::character varying, 'cancelled'::character varying])::text[])) AND (finished_at IS NOT NULL)) OR (((state)::text = 'superseded'::text) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL)))),
    CONSTRAINT dev_lifecycle_operations_transition_check CHECK (((((kind)::text = 'noop'::text) AND (operation_epoch = expected_operation_epoch) AND ((state)::text = 'succeeded'::text)) OR (((kind)::text <> 'noop'::text) AND (operation_epoch = (expected_operation_epoch + 1)))))
);
CREATE TABLE public.gb10_worker_node_statuses (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    environment text NOT NULL,
    pool_name text NOT NULL,
    hostname text NOT NULL,
    worker_id uuid,
    current_image_tag text,
    current_max_concurrent integer,
    current_env_config_version text,
    desired_image_tag text,
    desired_max_concurrent integer,
    desired_env_config_version text,
    apply_state text DEFAULT 'unknown'::text NOT NULL,
    last_apply_result text,
    error_message text,
    agent_version text,
    compose_project_dir text,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    last_apply_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    desired_intent text,
    current_intent text,
    source_git_commit text,
    source_git_dirty boolean,
    desired_source_git_commit text,
    CONSTRAINT gb10_worker_node_statuses_apply_state_check CHECK ((apply_state = ANY (ARRAY['unknown'::text, 'idle'::text, 'applying'::text, 'draining'::text, 'stopped'::text, 'applied'::text, 'blocked'::text, 'failed'::text, 'rolled_back'::text]))),
    CONSTRAINT gb10_worker_node_statuses_current_max_positive_check CHECK (((current_max_concurrent IS NULL) OR (current_max_concurrent > 0))),
    CONSTRAINT gb10_worker_node_statuses_desired_max_positive_check CHECK (((desired_max_concurrent IS NULL) OR (desired_max_concurrent > 0))),
    CONSTRAINT gb10_worker_node_statuses_environment_nonempty_check CHECK ((length(TRIM(BOTH FROM environment)) > 0)),
    CONSTRAINT gb10_worker_node_statuses_hostname_nonempty_check CHECK ((length(TRIM(BOTH FROM hostname)) > 0)),
    CONSTRAINT gb10_worker_node_statuses_pool_name_nonempty_check CHECK ((length(TRIM(BOTH FROM pool_name)) > 0))
);
CREATE TABLE public.gb10_worker_pool_desired_states (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    environment text NOT NULL,
    pool_name text NOT NULL,
    image_tag text NOT NULL,
    max_concurrent integer NOT NULL,
    env_config_version text NOT NULL,
    rollout_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    env jsonb DEFAULT '{}'::jsonb NOT NULL,
    force boolean DEFAULT false NOT NULL,
    previous_image_tag text,
    previous_max_concurrent integer,
    previous_env_config_version text,
    previous_env jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    target_slots integer,
    host_intents jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_git_commit text,
    previous_source_git_commit text,
    CONSTRAINT gb10_worker_pool_desired_states_env_version_nonempty_check CHECK ((length(TRIM(BOTH FROM env_config_version)) > 0)),
    CONSTRAINT gb10_worker_pool_desired_states_environment_nonempty_check CHECK ((length(TRIM(BOTH FROM environment)) > 0)),
    CONSTRAINT gb10_worker_pool_desired_states_image_tag_nonempty_check CHECK ((length(TRIM(BOTH FROM image_tag)) > 0)),
    CONSTRAINT gb10_worker_pool_desired_states_max_concurrent_positive_check CHECK ((max_concurrent > 0)),
    CONSTRAINT gb10_worker_pool_desired_states_pool_name_nonempty_check CHECK ((length(TRIM(BOTH FROM pool_name)) > 0))
);
CREATE TABLE public.personal_dev_build_platform_requests (
    id uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_lease_epoch bigint NOT NULL,
    platform text NOT NULL,
    subject_id uuid NOT NULL,
    subject_incarnation uuid NOT NULL,
    deployment_generation bigint NOT NULL,
    bucket_id text NOT NULL,
    source_binding_sha256 text NOT NULL,
    runtime_installation_sha256 text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    cancelled_at timestamp with time zone,
    CONSTRAINT personal_build_request_digest_check CHECK (((bucket_id ~ '^build-[0-9a-f]{64}$'::text) AND (source_binding_sha256 ~ '^[0-9a-f]{64}$'::text) AND (source_binding_sha256 <> repeat('0'::text, 64)) AND (runtime_installation_sha256 ~ '^[0-9a-f]{64}$'::text) AND (runtime_installation_sha256 <> repeat('0'::text, 64)))),
    CONSTRAINT personal_build_request_identity_check CHECK (((platform = ANY (ARRAY['linux/amd64'::text, 'linux/arm64'::text])) AND (attempt_lease_epoch > 0) AND (deployment_generation > 0))),
    CONSTRAINT personal_build_request_time_check CHECK (((cancelled_at IS NULL) OR (cancelled_at >= created_at)))
);
CREATE TABLE public.personal_dev_candidate_artifact_collections (
    id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    collection_sequence integer NOT NULL,
    collector_id character varying(128) NOT NULL,
    gc_lease_epoch bigint NOT NULL,
    manifest_json jsonb NOT NULL,
    manifest_sha256 character varying(64) NOT NULL,
    unreferenced_at timestamp with time zone NOT NULL,
    collected_at timestamp with time zone NOT NULL,
    CONSTRAINT personal_dev_candidate_artifact_collections_check CHECK (((collection_sequence > 0) AND (gc_lease_epoch > 0) AND ((collector_id)::text <> ''::text) AND ((collector_id)::text = btrim((collector_id)::text)) AND ((manifest_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (jsonb_typeof(manifest_json) = 'object'::text) AND ((((manifest_json ->> 'schema_version'::text) = '1'::text) AND ((manifest_json ->> 'candidate_id'::text) = (candidate_id)::text)) IS TRUE) AND (collected_at >= unreferenced_at)))
);
CREATE TABLE public.personal_dev_candidate_build_attempts (
    id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    subject_incarnation uuid NOT NULL,
    operation_id uuid NOT NULL,
    operation_epoch bigint NOT NULL,
    attempt_sequence integer DEFAULT 0 NOT NULL,
    state text DEFAULT 'queued'::text NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    claimed_by character varying(128),
    lease_expires_at timestamp with time zone,
    failure_reason character varying(256),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    CONSTRAINT personal_dev_candidate_build_attempts_counters_check CHECK (((attempt_sequence >= 0) AND (operation_epoch > 0) AND (lease_epoch >= 0))),
    CONSTRAINT personal_dev_candidate_build_attempts_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'claimed'::text, 'running'::text, 'succeeded'::text, 'failed'::text]))),
    CONSTRAINT personal_dev_candidate_build_attempts_state_fields_check CHECK ((((state = 'queued'::text) AND (claimed_by IS NULL) AND (lease_expires_at IS NULL) AND (started_at IS NULL) AND (finished_at IS NULL) AND (failure_reason IS NULL)) OR ((state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (started_at IS NULL) AND (finished_at IS NULL) AND (failure_reason IS NULL)) OR ((state = 'running'::text) AND (claimed_by IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (started_at IS NOT NULL) AND (finished_at IS NULL) AND (failure_reason IS NULL)) OR ((state = 'succeeded'::text) AND (claimed_by IS NOT NULL) AND (lease_expires_at IS NULL) AND (started_at IS NOT NULL) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL)) OR ((state = 'failed'::text) AND (claimed_by IS NOT NULL) AND (lease_expires_at IS NULL) AND (started_at IS NOT NULL) AND (finished_at IS NOT NULL) AND (failure_reason IS NOT NULL))))
);
CREATE TABLE public.personal_dev_candidates (
    id uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    owner_team_id uuid NOT NULL,
    candidate_sha character varying(64) NOT NULL,
    source_sha256 character varying(64) NOT NULL,
    archive_sha256 character varying(64) NOT NULL,
    build_contract_sha256 character varying(64) NOT NULL,
    source_commit character varying(40) NOT NULL,
    dirty boolean NOT NULL,
    manifest_json jsonb NOT NULL,
    object_bucket text NOT NULL,
    object_key text NOT NULL,
    archive_size_bytes bigint NOT NULL,
    status text DEFAULT 'uploaded'::text NOT NULL,
    image_manifest_digest character varying(71),
    publication_json jsonb,
    publication_sha256 character varying(64),
    failure_reason character varying(256),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    ready_at timestamp with time zone,
    source_generation_id uuid NOT NULL,
    registry_prefix text,
    artifact_state character varying(16) DEFAULT 'retained'::character varying NOT NULL,
    artifact_gc_lease_epoch bigint DEFAULT 0 NOT NULL,
    artifact_gc_unreferenced_at timestamp with time zone,
    artifact_gc_lease_expires_at timestamp with time zone,
    artifact_collected_at timestamp with time zone,
    artifact_gc_claimed_by character varying(128),
    artifact_gc_blocked_reason character varying(64),
    artifact_gc_manifest_json jsonb,
    artifact_gc_manifest_sha256 character varying(64),
    CONSTRAINT personal_dev_candidates_archive_size_check CHECK ((archive_size_bytes > 0)),
    CONSTRAINT personal_dev_candidates_artifact_gc_check CHECK (((artifact_gc_lease_epoch >= 0) AND ((artifact_gc_blocked_reason IS NULL) OR ((artifact_gc_blocked_reason)::text = ANY ((ARRAY['manifest_authority_invalid'::character varying, 'registry_authority_unavailable'::character varying])::text[]))) AND ((((artifact_state)::text = 'retained'::text) AND (artifact_gc_claimed_by IS NULL) AND (artifact_gc_lease_expires_at IS NULL) AND (artifact_gc_manifest_json IS NULL) AND (artifact_gc_manifest_sha256 IS NULL) AND (artifact_collected_at IS NULL)) OR (((artifact_state)::text = 'collecting'::text) AND (artifact_gc_blocked_reason IS NULL) AND (artifact_gc_unreferenced_at IS NOT NULL) AND (artifact_gc_claimed_by IS NOT NULL) AND (artifact_gc_lease_expires_at IS NOT NULL) AND (artifact_gc_manifest_json IS NOT NULL) AND (jsonb_typeof(artifact_gc_manifest_json) = 'object'::text) AND ((artifact_gc_manifest_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (artifact_collected_at IS NULL)) OR (((artifact_state)::text = 'collected'::text) AND (artifact_gc_blocked_reason IS NULL) AND (artifact_gc_unreferenced_at IS NOT NULL) AND (artifact_gc_claimed_by IS NULL) AND (artifact_gc_lease_expires_at IS NULL) AND (artifact_gc_manifest_json IS NOT NULL) AND (jsonb_typeof(artifact_gc_manifest_json) = 'object'::text) AND ((artifact_gc_manifest_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (artifact_collected_at IS NOT NULL))))),
    CONSTRAINT personal_dev_candidates_artifact_manifest_binding_check CHECK (((artifact_gc_manifest_json IS NULL) OR (((jsonb_typeof(artifact_gc_manifest_json) = 'object'::text) AND ((artifact_gc_manifest_json ->> 'schema_version'::text) = '1'::text) AND ((artifact_gc_manifest_json ->> 'candidate_id'::text) = (id)::text) AND ((artifact_gc_manifest_json ->> 'owner_user_id'::text) = (owner_user_id)::text) AND ((artifact_gc_manifest_json ->> 'owner_team_id'::text) = (owner_team_id)::text) AND ((artifact_gc_manifest_json ->> 'candidate_sha'::text) = (candidate_sha)::text) AND ((artifact_gc_manifest_json ->> 'object_bucket'::text) = object_bucket) AND ((artifact_gc_manifest_json ->> 'source_generation_id'::text) = (source_generation_id)::text) AND ((artifact_gc_manifest_json ->> 'source_object_key'::text) = object_key)) IS TRUE))),
    CONSTRAINT personal_dev_candidates_digests_check CHECK ((((candidate_sha)::text ~ '^[0-9a-f]{64}$'::text) AND ((source_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((archive_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((build_contract_sha256)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT personal_dev_candidates_object_binding_check CHECK (((object_bucket <> ''::text) AND (object_bucket = btrim(object_bucket)) AND (POSITION(('/'::text) IN (object_bucket)) = 0) AND (((source_generation_id = id) AND (object_key = (((((((('personal-dev/sources/'::text || (owner_team_id)::text) || '/'::text) || (owner_user_id)::text) || '/'::text) || (candidate_sha)::text) || '/'::text) || (archive_sha256)::text) || '.tar'::text))) OR (object_key = (((((((((('personal-dev/sources/'::text || (owner_team_id)::text) || '/'::text) || (owner_user_id)::text) || '/'::text) || (candidate_sha)::text) || '/'::text) || (source_generation_id)::text) || '/'::text) || (archive_sha256)::text) || '.tar'::text))))),
    CONSTRAINT personal_dev_candidates_registry_prefix_check CHECK (((registry_prefix IS NULL) OR (((length(registry_prefix) >= 1) AND (length(registry_prefix) <= 309)) AND (registry_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*$'::text) AND ("right"(registry_prefix, 1) <> ALL (ARRAY['/'::text, ':'::text])) AND (POSITION(('://'::text) IN (registry_prefix)) = 0) AND (POSITION(('@'::text) IN (registry_prefix)) = 0)))),
    CONSTRAINT personal_dev_candidates_source_commit_check CHECK (((source_commit)::text ~ '^[0-9a-f]{40}$'::text)),
    CONSTRAINT personal_dev_candidates_status_check CHECK ((status = ANY (ARRAY['uploaded'::text, 'queued'::text, 'building'::text, 'ready'::text, 'failed'::text]))),
    CONSTRAINT personal_dev_candidates_terminal_fields_check CHECK ((((status = ANY (ARRAY['uploaded'::text, 'queued'::text, 'building'::text])) AND (image_manifest_digest IS NULL) AND (publication_json IS NULL) AND (publication_sha256 IS NULL) AND (failure_reason IS NULL) AND (ready_at IS NULL)) OR ((status = 'ready'::text) AND (image_manifest_digest IS NOT NULL) AND ((image_manifest_digest)::text ~ '^sha256:[0-9a-f]{64}$'::text) AND (publication_json IS NOT NULL) AND (publication_sha256 IS NOT NULL) AND ((publication_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (failure_reason IS NULL) AND (ready_at IS NOT NULL)) OR ((status = 'failed'::text) AND (image_manifest_digest IS NULL) AND (publication_json IS NULL) AND (publication_sha256 IS NULL) AND (failure_reason IS NOT NULL) AND (ready_at IS NULL))))
);
CREATE TABLE public.personal_dev_native_build_grants (
    id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_lease_epoch bigint NOT NULL,
    platform character varying(32) NOT NULL,
    provider character varying(64) NOT NULL,
    required_agent_instance_id uuid NOT NULL,
    required_agent_key_id character varying(64) NOT NULL,
    agent_image text NOT NULL,
    builder_image text NOT NULL,
    runtime_profile_sha256 character varying(64) NOT NULL,
    contract_json text NOT NULL,
    contract_sha256 character varying(64) NOT NULL,
    source_bucket text NOT NULL,
    source_object_key text NOT NULL,
    artifact_bucket text NOT NULL,
    artifact_object_key text NOT NULL,
    artifact_max_bytes bigint NOT NULL,
    active_deadline_seconds integer NOT NULL,
    state character varying(16) NOT NULL,
    running_agent_instance_id uuid,
    last_request_at timestamp with time zone,
    last_request_nonce uuid,
    failure_reason character varying(128),
    completion_json jsonb,
    completion_sha256 character varying(64),
    runtime_evidence_json jsonb,
    runtime_evidence_sha256 character varying(64),
    artifact_head_json jsonb,
    artifact_head_sha256 character varying(64),
    queued_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT personal_dev_native_build_grants_identity_check CHECK (((attempt_lease_epoch > 0) AND ((platform)::text = 'linux/arm64'::text) AND ((provider)::text = 'gb10-gvisor-docker-v1'::text) AND ((required_agent_key_id)::text ~ '^[a-z][a-z0-9._-]{0,63}$'::text) AND ((running_agent_instance_id IS NULL) OR (running_agent_instance_id = required_agent_instance_id)) AND ((octet_length(agent_image) >= 73) AND (octet_length(agent_image) <= 584)) AND (agent_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$'::text) AND ((octet_length(builder_image) >= 73) AND (octet_length(builder_image) <= 584)) AND (builder_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$'::text) AND ((runtime_profile_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((contract_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((octet_length(contract_json) >= 2) AND (octet_length(contract_json) <= 65536)) AND ((artifact_max_bytes >= 1) AND (artifact_max_bytes <= '17179869184'::bigint)) AND ((active_deadline_seconds >= 300) AND (active_deadline_seconds <= 7200)))),
    CONSTRAINT personal_dev_native_build_grants_object_binding_check CHECK (((source_bucket <> ''::text) AND (source_bucket = btrim(source_bucket)) AND (POSITION(('/'::text) IN (source_bucket)) = 0) AND (artifact_bucket = source_bucket) AND (source_object_key <> ''::text) AND (artifact_object_key <> ''::text) AND (octet_length(source_object_key) <= 2048) AND (octet_length(artifact_object_key) <= 2048) AND (source_object_key !~ '[[:cntrl:]]'::text) AND (artifact_object_key !~ '[[:cntrl:]]'::text) AND (artifact_object_key ~~ 'personal-dev/builds/%/artifacts.tar'::text))),
    CONSTRAINT personal_dev_native_build_grants_state_check CHECK (((state)::text = ANY ((ARRAY['queued'::character varying, 'running'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelled'::character varying])::text[]))),
    CONSTRAINT personal_dev_native_build_grants_terminal_check CHECK (((((last_request_at IS NULL) AND (last_request_nonce IS NULL)) OR ((last_request_at IS NOT NULL) AND (last_request_nonce IS NOT NULL))) AND ((((state)::text = 'queued'::text) AND (running_agent_instance_id IS NULL) AND (started_at IS NULL) AND (heartbeat_at IS NULL) AND (finished_at IS NULL) AND (failure_reason IS NULL) AND (completion_json IS NULL) AND (completion_sha256 IS NULL) AND (runtime_evidence_json IS NULL) AND (runtime_evidence_sha256 IS NULL) AND (artifact_head_json IS NULL) AND (artifact_head_sha256 IS NULL)) OR (((state)::text = 'running'::text) AND (running_agent_instance_id IS NOT NULL) AND (started_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (finished_at IS NULL) AND (failure_reason IS NULL) AND (completion_json IS NULL) AND (completion_sha256 IS NULL) AND (runtime_evidence_json IS NULL) AND (runtime_evidence_sha256 IS NULL) AND (artifact_head_json IS NULL) AND (artifact_head_sha256 IS NULL)) OR (((state)::text = 'succeeded'::text) AND (running_agent_instance_id IS NOT NULL) AND (started_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (finished_at IS NOT NULL) AND (failure_reason IS NULL) AND (jsonb_typeof(completion_json) = 'object'::text) AND ((completion_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (jsonb_typeof(runtime_evidence_json) = 'object'::text) AND ((runtime_evidence_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (jsonb_typeof(artifact_head_json) = 'object'::text) AND ((artifact_head_sha256)::text ~ '^[0-9a-f]{64}$'::text)) OR (((state)::text = 'failed'::text) AND (running_agent_instance_id IS NOT NULL) AND (started_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (finished_at IS NOT NULL) AND ((failure_reason)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text) AND (jsonb_typeof(completion_json) = 'object'::text) AND ((completion_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (runtime_evidence_json IS NULL) AND (runtime_evidence_sha256 IS NULL) AND (artifact_head_json IS NULL) AND (artifact_head_sha256 IS NULL)) OR (((state)::text = 'cancelled'::text) AND (finished_at IS NOT NULL) AND ((failure_reason)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text) AND (((running_agent_instance_id IS NULL) AND (started_at IS NULL) AND (heartbeat_at IS NULL)) OR ((running_agent_instance_id IS NOT NULL) AND (started_at IS NOT NULL) AND (heartbeat_at IS NOT NULL))) AND (completion_json IS NULL) AND (completion_sha256 IS NULL) AND (runtime_evidence_json IS NULL) AND (runtime_evidence_sha256 IS NULL) AND (artifact_head_json IS NULL) AND (artifact_head_sha256 IS NULL))) AND (queued_at <= updated_at) AND ((started_at IS NULL) OR (queued_at <= started_at)) AND ((heartbeat_at IS NULL) OR (started_at <= heartbeat_at)) AND ((finished_at IS NULL) OR (COALESCE(heartbeat_at, queued_at) <= finished_at))))
);
CREATE TABLE public.personal_dev_native_builder_agents (
    instance_id uuid NOT NULL,
    key_id character varying(64) NOT NULL,
    provider character varying(64) NOT NULL,
    platform character varying(32) NOT NULL,
    protocol_version integer NOT NULL,
    host_name character varying(253) NOT NULL,
    host_architecture character varying(32) NOT NULL,
    host_boot_id uuid NOT NULL,
    agent_image text NOT NULL,
    builder_image text NOT NULL,
    runtime_profile_sha256 character varying(64) NOT NULL,
    max_concurrency integer NOT NULL,
    managed_grant_ids_json jsonb NOT NULL,
    active_grant_ids_json jsonb NOT NULL,
    available boolean NOT NULL,
    unavailable_reason character varying(128),
    readiness_evidence_sha256 character varying(64) NOT NULL,
    status_json jsonb NOT NULL,
    status_sha256 character varying(64) NOT NULL,
    last_poll_requested_at timestamp with time zone NOT NULL,
    last_poll_nonce uuid NOT NULL,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT personal_dev_native_builder_agents_identity_check CHECK ((((key_id)::text ~ '^[a-z][a-z0-9._-]{0,63}$'::text) AND ((provider)::text = 'gb10-gvisor-docker-v1'::text) AND ((platform)::text = 'linux/arm64'::text) AND (protocol_version = 1) AND ((host_architecture)::text = 'aarch64'::text) AND ((host_name)::text <> ''::text) AND ((host_name)::text = btrim((host_name)::text)) AND ((octet_length(agent_image) >= 73) AND (octet_length(agent_image) <= 584)) AND (agent_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$'::text) AND ((octet_length(builder_image) >= 73) AND (octet_length(builder_image) <= 584)) AND (builder_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$'::text) AND ((runtime_profile_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((readiness_evidence_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((status_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND (max_concurrency = 2))),
    CONSTRAINT personal_dev_native_builder_agents_inventory_check CHECK (((jsonb_typeof(managed_grant_ids_json) = 'array'::text) AND (jsonb_array_length(managed_grant_ids_json) <= 64) AND (jsonb_typeof(active_grant_ids_json) = 'array'::text) AND (jsonb_array_length(active_grant_ids_json) <= 2))),
    CONSTRAINT personal_dev_native_builder_agents_status_check CHECK (((available = (unavailable_reason IS NULL)) AND ((unavailable_reason IS NULL) OR ((unavailable_reason)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text)) AND (jsonb_typeof(status_json) = 'object'::text) AND ((((status_json ->> 'agent_instance_id'::text) = (instance_id)::text) AND ((status_json ->> 'agent_key_id'::text) = (key_id)::text) AND ((status_json ->> 'provider'::text) = (provider)::text) AND ((status_json ->> 'platform'::text) = (platform)::text) AND (((status_json ->> 'protocol_version'::text))::integer = protocol_version) AND ((status_json ->> 'host_name'::text) = (host_name)::text) AND ((status_json ->> 'host_architecture'::text) = (host_architecture)::text) AND ((status_json ->> 'host_boot_id'::text) = (host_boot_id)::text) AND ((status_json ->> 'agent_image'::text) = agent_image) AND ((status_json ->> 'builder_image'::text) = builder_image) AND ((status_json ->> 'runtime_profile_sha256'::text) = (runtime_profile_sha256)::text) AND (((status_json ->> 'max_concurrency'::text))::integer = max_concurrency) AND ((status_json -> 'managed_grant_ids'::text) = managed_grant_ids_json) AND ((status_json -> 'active_grant_ids'::text) = active_grant_ids_json) AND (((status_json ->> 'available'::text))::boolean = available) AND (NOT ((status_json ->> 'unavailable_reason'::text) IS DISTINCT FROM (unavailable_reason)::text)) AND ((status_json ->> 'readiness_evidence_sha256'::text) = (readiness_evidence_sha256)::text)) IS TRUE) AND (first_seen_at <= last_seen_at) AND (last_seen_at <= updated_at)))
);
CREATE TABLE public.pipeline_acceptance_evidence_runs (
    artifact_id uuid NOT NULL,
    run_ordinal integer NOT NULL,
    result_kind text NOT NULL,
    run_kind text NOT NULL,
    scenario_id text,
    lane_or_input_set text,
    provenance_digest text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone NOT NULL
);
CREATE TABLE public.pipeline_input_materialization_evidence (
    execution_attempt_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    lease_epoch bigint NOT NULL,
    cache_expectation text NOT NULL,
    ordered_manifest_sha256s_json bytea NOT NULL,
    manifest_open_count bigint NOT NULL,
    file_open_count bigint NOT NULL,
    file_bytes bigint NOT NULL,
    archive_extraction_count bigint NOT NULL,
    cas_rename_count bigint NOT NULL,
    input_view_sha256 text NOT NULL,
    materialized_at timestamp with time zone NOT NULL,
    evidence_json bytea NOT NULL,
    evidence_sha256 text NOT NULL,
    CONSTRAINT pipeline_input_materialization_e_archive_extraction_count_check CHECK ((archive_extraction_count >= 0)),
    CONSTRAINT pipeline_input_materialization_eviden_manifest_open_count_check CHECK ((manifest_open_count >= 0)),
    CONSTRAINT pipeline_input_materialization_evidence_cache_expectation_check CHECK ((cache_expectation = ANY (ARRAY['cold_after_eviction'::text, 'warm_reuse_only'::text]))),
    CONSTRAINT pipeline_input_materialization_evidence_cas_rename_count_check CHECK ((cas_rename_count >= 0)),
    CONSTRAINT pipeline_input_materialization_evidence_file_bytes_check CHECK ((file_bytes >= 0)),
    CONSTRAINT pipeline_input_materialization_evidence_file_open_count_check CHECK ((file_open_count >= 0)),
    CONSTRAINT pipeline_input_materialization_evidence_lease_epoch_check CHECK ((lease_epoch >= 0))
);
CREATE TABLE public.pipeline_run_gpu_backend_selections (
    id uuid NOT NULL,
    pipeline_run_id uuid NOT NULL,
    scope text NOT NULL,
    variant_id text NOT NULL,
    policy_id text NOT NULL,
    selection_source text NOT NULL,
    selected_at timestamp with time zone NOT NULL,
    selection_json jsonb NOT NULL,
    selection_bytes bytea NOT NULL,
    gpu_backend_selection_sha256 text NOT NULL,
    CONSTRAINT pipeline_gpu_selection_digest_check CHECK ((gpu_backend_selection_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT pipeline_gpu_selection_document_check CHECK (((jsonb_typeof(selection_json) = 'object'::text) AND (octet_length(selection_bytes) > 1) AND (get_byte(selection_bytes, (octet_length(selection_bytes) - 1)) = 10))),
    CONSTRAINT pipeline_gpu_selection_scope_authority_check CHECK ((((selection_source = 'recipe_hash'::text) AND (scope = 'all_gpu_nodes'::text)) OR ((selection_source <> 'recipe_hash'::text) AND ((scope = 'all_gpu_nodes'::text) OR (((variant_id = 'gb10-shared-1gpu'::text) AND (scope = 'gb10_preflight'::text)) OR ((variant_id = 'oldlab-rtx5080-2gpu'::text) AND (scope = 'oldlab_preflight'::text))))))),
    CONSTRAINT pipeline_gpu_selection_variant_policy_check CHECK ((((variant_id = 'gb10-shared-1gpu'::text) AND (policy_id = 'behavior-gpu-gb10'::text)) OR ((variant_id = 'oldlab-rtx5080-2gpu'::text) AND (policy_id = 'behavior-gpu-oldlab'::text)))),
    CONSTRAINT pipeline_run_gpu_backend_selections_policy_id_check CHECK ((policy_id = ANY (ARRAY['behavior-gpu-gb10'::text, 'behavior-gpu-oldlab'::text]))),
    CONSTRAINT pipeline_run_gpu_backend_selections_scope_check CHECK ((scope = ANY (ARRAY['all_gpu_nodes'::text, 'oldlab_preflight'::text, 'gb10_preflight'::text]))),
    CONSTRAINT pipeline_run_gpu_backend_selections_selection_source_check CHECK ((selection_source = ANY (ARRAY['recipe_hash'::text, 'acceptance_authority'::text, 'profile_calibration_authority'::text]))),
    CONSTRAINT pipeline_run_gpu_backend_selections_variant_id_check CHECK ((variant_id = ANY (ARRAY['gb10-shared-1gpu'::text, 'oldlab-rtx5080-2gpu'::text])))
);
CREATE TABLE public.pipeline_scoped_policy_activations (
    id uuid NOT NULL,
    environment text NOT NULL,
    policy_id text NOT NULL,
    policy_config_sha256 text NOT NULL,
    authority_kind text NOT NULL,
    authority_id uuid NOT NULL,
    activation_epoch bigint NOT NULL,
    state text NOT NULL,
    desired_slots integer NOT NULL,
    activated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT pipeline_policy_activation_slot_ceiling_check CHECK ((((policy_id = 'behavior-cpu-data'::text) AND (desired_slots <= 2)) OR ((policy_id = ANY (ARRAY['behavior-gpu-oldlab'::text, 'behavior-gpu-gb10'::text])) AND (desired_slots <= 1)))),
    CONSTRAINT pipeline_policy_activation_state_slots_check CHECK ((((state = 'active'::text) AND (desired_slots > 0)) OR ((state = ANY (ARRAY['draining'::text, 'disabled'::text])) AND (desired_slots = 0)))),
    CONSTRAINT pipeline_scoped_policy_activations_activation_epoch_check CHECK ((activation_epoch > 0)),
    CONSTRAINT pipeline_scoped_policy_activations_authority_kind_check CHECK ((authority_kind = ANY (ARRAY['acceptance'::text, 'profile_calibration'::text]))),
    CONSTRAINT pipeline_scoped_policy_activations_environment_check CHECK ((length(TRIM(BOTH FROM environment)) > 0)),
    CONSTRAINT pipeline_scoped_policy_activations_policy_config_sha256_check CHECK ((policy_config_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT pipeline_scoped_policy_activations_policy_id_check CHECK ((policy_id = ANY (ARRAY['behavior-cpu-data'::text, 'behavior-gpu-oldlab'::text, 'behavior-gpu-gb10'::text]))),
    CONSTRAINT pipeline_scoped_policy_activations_state_check CHECK ((state = ANY (ARRAY['active'::text, 'draining'::text, 'disabled'::text])))
);
CREATE TABLE public.pipeline_stage1_smoke_authorizations (
    authorization_id uuid NOT NULL,
    team_id uuid NOT NULL,
    operator_user_id uuid NOT NULL,
    environment text NOT NULL,
    candidate_json jsonb NOT NULL,
    candidate_bytes bytea NOT NULL,
    candidate_sha256 text NOT NULL,
    authorization_json jsonb NOT NULL,
    authorization_bytes bytea NOT NULL,
    authorization_sha256 text NOT NULL,
    preflight_json jsonb,
    preflight_bytes bytea,
    preflight_sha256 text,
    nonce_sha256 text NOT NULL,
    execute_idempotency_key text,
    execute_request_digest text,
    execute_signature_key_id text,
    execute_signature_sha256 text,
    policy_activation_id uuid NOT NULL,
    pipeline_run_id uuid,
    state text NOT NULL,
    evidence_sha256 text,
    cleanup_sha256 text,
    authorized_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    start_by timestamp with time zone NOT NULL,
    cleanup_deadline timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    capacity_idempotency_key text NOT NULL,
    capacity_request_digest text NOT NULL,
    capacity_signature_key_id text NOT NULL,
    capacity_signature_sha256 text NOT NULL,
    cleanup_begin_json jsonb,
    cleanup_begin_bytes bytea,
    cleanup_begin_sha256 text,
    cleanup_begin_signature_key_id text,
    cleanup_begin_signature_sha256 text,
    cleanup_began_at timestamp with time zone,
    cleanup_signature_key_id text,
    cleanup_signature_sha256 text,
    CONSTRAINT pipeline_stage1_smoke_authorizations_cleanup_phase_check CHECK ((((state = ANY (ARRAY['capacity_pending'::text, 'submitted'::text, 'running'::text, 'cleanup_required'::text])) AND (cleanup_begin_json IS NULL) AND (cleanup_begin_bytes IS NULL) AND (cleanup_begin_sha256 IS NULL) AND (cleanup_begin_signature_key_id IS NULL) AND (cleanup_begin_signature_sha256 IS NULL) AND (cleanup_began_at IS NULL)) OR ((state = ANY (ARRAY['capacity_draining'::text, 'capacity_aborted'::text, 'cleanup_draining'::text, 'accepted'::text, 'rejected'::text])) AND (cleanup_begin_json IS NOT NULL) AND (cleanup_begin_bytes IS NOT NULL) AND (cleanup_begin_sha256 IS NOT NULL) AND (cleanup_begin_signature_key_id IS NOT NULL) AND (cleanup_begin_signature_sha256 IS NOT NULL) AND (cleanup_began_at IS NOT NULL)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check CHECK (((candidate_sha256 ~ '^sha256:[0-9a-f]{64}$'::text) AND (authorization_sha256 ~ '^sha256:[0-9a-f]{64}$'::text) AND (nonce_sha256 ~ '^sha256:[0-9a-f]{64}$'::text) AND (capacity_request_digest ~ '^sha256:[0-9a-f]{64}$'::text) AND (capacity_signature_sha256 ~ '^sha256:[0-9a-f]{64}$'::text) AND ((preflight_sha256 IS NULL) OR (preflight_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((execute_request_digest IS NULL) OR (execute_request_digest ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((execute_signature_sha256 IS NULL) OR (execute_signature_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_document_check CHECK ((((octet_length(candidate_bytes) >= 2) AND (octet_length(candidate_bytes) <= 1048576)) AND (get_byte(candidate_bytes, (octet_length(candidate_bytes) - 1)) = 10) AND (octet_length(authorization_bytes) > 1) AND (get_byte(authorization_bytes, (octet_length(authorization_bytes) - 1)) = 10) AND (((preflight_json IS NULL) AND (preflight_bytes IS NULL)) OR ((jsonb_typeof(preflight_json) = 'object'::text) AND (octet_length(preflight_bytes) > 1) AND (get_byte(preflight_bytes, (octet_length(preflight_bytes) - 1)) = 10))) AND (((cleanup_begin_json IS NULL) AND (cleanup_begin_bytes IS NULL)) OR ((jsonb_typeof(cleanup_begin_json) = 'object'::text) AND (octet_length(cleanup_begin_bytes) > 1) AND (get_byte(cleanup_begin_bytes, (octet_length(cleanup_begin_bytes) - 1)) = 10))))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_evidence_phase_check CHECK ((((state = ANY (ARRAY['capacity_pending'::text, 'capacity_draining'::text, 'capacity_aborted'::text, 'submitted'::text, 'running'::text])) AND (evidence_sha256 IS NULL)) OR ((state = ANY (ARRAY['cleanup_required'::text, 'cleanup_draining'::text, 'accepted'::text, 'rejected'::text])) AND (evidence_sha256 IS NOT NULL)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_execution_phase_check CHECK ((((state = ANY (ARRAY['capacity_pending'::text, 'capacity_draining'::text, 'capacity_aborted'::text])) AND (preflight_json IS NULL) AND (preflight_bytes IS NULL) AND (preflight_sha256 IS NULL) AND (execute_idempotency_key IS NULL) AND (execute_request_digest IS NULL) AND (execute_signature_key_id IS NULL) AND (execute_signature_sha256 IS NULL) AND (pipeline_run_id IS NULL) AND (consumed_at IS NULL)) OR ((state <> ALL (ARRAY['capacity_pending'::text, 'capacity_draining'::text, 'capacity_aborted'::text])) AND (preflight_json IS NOT NULL) AND (preflight_bytes IS NOT NULL) AND (preflight_sha256 IS NOT NULL) AND (execute_idempotency_key IS NOT NULL) AND (execute_request_digest IS NOT NULL) AND (execute_signature_key_id IS NOT NULL) AND (execute_signature_sha256 IS NOT NULL) AND (pipeline_run_id IS NOT NULL) AND (consumed_at IS NOT NULL)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check CHECK ((((length(environment) >= 1) AND (length(environment) <= 256)) AND ((length(capacity_idempotency_key) >= 1) AND (length(capacity_idempotency_key) <= 128)) AND (capacity_idempotency_key = btrim(capacity_idempotency_key)) AND (capacity_idempotency_key ~ '^[ -~]+$'::text) AND ((execute_idempotency_key IS NULL) OR (((length(execute_idempotency_key) >= 1) AND (length(execute_idempotency_key) <= 128)) AND (execute_idempotency_key = btrim(execute_idempotency_key)) AND (execute_idempotency_key ~ '^[ -~]+$'::text))) AND (capacity_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'::text) AND ((execute_signature_key_id IS NULL) OR (execute_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'::text)) AND ((cleanup_begin_signature_key_id IS NULL) OR (cleanup_begin_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'::text)) AND ((cleanup_signature_key_id IS NULL) OR (cleanup_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'::text)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check CHECK ((((evidence_sha256 IS NULL) OR (evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((cleanup_begin_sha256 IS NULL) OR (cleanup_begin_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((cleanup_begin_signature_sha256 IS NULL) OR (cleanup_begin_signature_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((cleanup_sha256 IS NULL) OR (cleanup_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)) AND ((cleanup_signature_sha256 IS NULL) OR (cleanup_signature_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_state_check CHECK ((state = ANY (ARRAY['capacity_pending'::text, 'capacity_draining'::text, 'capacity_aborted'::text, 'submitted'::text, 'running'::text, 'cleanup_required'::text, 'cleanup_draining'::text, 'accepted'::text, 'rejected'::text]))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check CHECK ((((state = ANY (ARRAY['capacity_aborted'::text, 'accepted'::text, 'rejected'::text])) AND (cleanup_sha256 IS NOT NULL) AND (cleanup_signature_key_id IS NOT NULL) AND (cleanup_signature_sha256 IS NOT NULL) AND (finished_at IS NOT NULL)) OR ((state <> ALL (ARRAY['capacity_aborted'::text, 'accepted'::text, 'rejected'::text])) AND (cleanup_sha256 IS NULL) AND (cleanup_signature_key_id IS NULL) AND (cleanup_signature_sha256 IS NULL) AND (finished_at IS NULL)))),
    CONSTRAINT pipeline_stage1_smoke_authorizations_version_check CHECK ((version >= 0)),
    CONSTRAINT pipeline_stage1_smoke_authorizations_window_check CHECK (((expires_at > authorized_at) AND (cleanup_deadline > start_by)))
);
CREATE TABLE public.pipeline_stage1_smoke_events (
    authorization_id uuid NOT NULL,
    seq bigint NOT NULL,
    event_kind text NOT NULL,
    payload_json jsonb NOT NULL,
    payload_bytes bytea NOT NULL,
    payload_sha256 text NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT pipeline_stage1_smoke_events_digest_check CHECK ((payload_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT pipeline_stage1_smoke_events_document_check CHECK (((octet_length(payload_bytes) > 1) AND (get_byte(payload_bytes, (octet_length(payload_bytes) - 1)) = 10))),
    CONSTRAINT pipeline_stage1_smoke_events_kind_check CHECK ((event_kind = ANY (ARRAY['capacity_preflight_started'::text, 'live_action_consumed'::text, 'evidence_recorded'::text, 'cleanup_started'::text, 'cleanup_complete'::text, 'capacity_aborted'::text, 'accepted'::text, 'rejected'::text]))),
    CONSTRAINT pipeline_stage1_smoke_events_seq_check CHECK ((seq > 0))
);
CREATE TABLE public.slurm_worker_jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    environment text NOT NULL,
    pool_name text NOT NULL,
    nodelist text NOT NULL,
    requested_cpus integer,
    requested_memory_mib integer,
    requested_concurrency integer NOT NULL,
    job_id text,
    slurm_state text,
    state text DEFAULT 'pending'::text NOT NULL,
    pending_reason text,
    worker_id uuid,
    redacted_env jsonb DEFAULT '{}'::jsonb NOT NULL,
    submission_error text,
    submitted_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    last_reconciled_at timestamp with time zone,
    stale_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    requested_pids integer,
    requested_gpu_tres text,
    requested_gpus integer DEFAULT 0 NOT NULL,
    sandbox_identity text,
    candidate_sha character varying(40),
    compose_project text,
    slurm_cluster_id text DEFAULT 'oldlab'::text NOT NULL,
    CONSTRAINT slurm_worker_jobs_candidate_sha_check CHECK (((candidate_sha IS NULL) OR ((candidate_sha)::text ~ '^[0-9a-f]{40}$'::text))),
    CONSTRAINT slurm_worker_jobs_cluster_check CHECK ((slurm_cluster_id = ANY (ARRAY['oldlab'::text, 'gb10'::text]))),
    CONSTRAINT slurm_worker_jobs_requested_concurrency_positive_check CHECK ((requested_concurrency > 0)),
    CONSTRAINT slurm_worker_jobs_requested_cpus_positive_check CHECK (((requested_cpus IS NULL) OR (requested_cpus > 0))),
    CONSTRAINT slurm_worker_jobs_requested_gpus_nonnegative_check CHECK ((requested_gpus >= 0)),
    CONSTRAINT slurm_worker_jobs_requested_memory_positive_check CHECK (((requested_memory_mib IS NULL) OR (requested_memory_mib > 0))),
    CONSTRAINT slurm_worker_jobs_requested_pids_positive_check CHECK (((requested_pids IS NULL) OR (requested_pids > 0))),
    CONSTRAINT slurm_worker_jobs_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'completed'::text, 'failed'::text, 'cancelled'::text, 'stale'::text])))
);
CREATE TABLE public.worker_pool_autoscaler_policies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    environment text NOT NULL,
    pool_name text NOT NULL,
    actuator text NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    min_slots integer DEFAULT 0 NOT NULL,
    max_slots integer NOT NULL,
    scale_up_threshold_slots integer DEFAULT 1 NOT NULL,
    scale_down_idle_seconds integer DEFAULT 600 NOT NULL,
    scale_up_cooldown_seconds integer DEFAULT 60 NOT NULL,
    scale_down_cooldown_seconds integer DEFAULT 300 NOT NULL,
    drain_timeout_seconds integer DEFAULT 600 NOT NULL,
    force boolean DEFAULT false NOT NULL,
    disabled_reason text,
    actuator_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    idle_since_at timestamp with time zone,
    last_decision text,
    last_decision_reason text,
    last_desired_slots integer,
    last_actual_slots integer,
    last_pending_slots integer,
    last_draining_slots integer,
    last_occupied_slots integer,
    last_queued_slots integer,
    last_blocked_reason text,
    last_error text,
    last_scale_up_at timestamp with time zone,
    last_scale_down_at timestamp with time zone,
    last_decision_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_blocked_details jsonb,
    prod_pressure_state jsonb,
    CONSTRAINT worker_pool_autoscaler_policies_actuator_check CHECK ((actuator = ANY (ARRAY['slurm'::text, 'gb10'::text]))),
    CONSTRAINT worker_pool_autoscaler_policies_drain_timeout_positive_check CHECK ((drain_timeout_seconds > 0)),
    CONSTRAINT worker_pool_autoscaler_policies_environment_nonempty_check CHECK ((length(TRIM(BOTH FROM environment)) > 0)),
    CONSTRAINT worker_pool_autoscaler_policies_max_slots_check CHECK ((max_slots >= min_slots)),
    CONSTRAINT worker_pool_autoscaler_policies_min_slots_nonnegative_check CHECK ((min_slots >= 0)),
    CONSTRAINT worker_pool_autoscaler_policies_pool_name_nonempty_check CHECK ((length(TRIM(BOTH FROM pool_name)) > 0)),
    CONSTRAINT worker_pool_autoscaler_policies_scale_down_cooldown_check CHECK ((scale_down_cooldown_seconds >= 0)),
    CONSTRAINT worker_pool_autoscaler_policies_scale_down_idle_check CHECK ((scale_down_idle_seconds >= 0)),
    CONSTRAINT worker_pool_autoscaler_policies_scale_up_cooldown_check CHECK ((scale_up_cooldown_seconds >= 0)),
    CONSTRAINT worker_pool_autoscaler_policies_scale_up_threshold_check CHECK ((scale_up_threshold_slots >= 0))
);

CREATE OR REPLACE FUNCTION public.guard_personal_build_platform_request()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'personal build platform requests cannot be removed';
          END IF;
          IF (to_jsonb(NEW) - 'cancelled_at') IS DISTINCT FROM (to_jsonb(OLD) - 'cancelled_at')
             OR OLD.cancelled_at IS NOT NULL OR NEW.cancelled_at IS NULL THEN
            RAISE EXCEPTION 'personal build platform request identity is immutable';
          END IF;
          RETURN NEW;
        END $function$;


CREATE OR REPLACE FUNCTION public.loom_check_dev_lifecycle_current_attempt()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM dev_lifecycle_operation_attempts AS attempt
                 WHERE attempt.id = NEW.attempt_id
                   AND attempt.operation_id = NEW.id
                   AND attempt.subject_id = NEW.subject_id
                   AND attempt.subject_incarnation = NEW.subject_incarnation
                   AND attempt.operation_epoch = NEW.operation_epoch
                   AND attempt.attempt_sequence = NEW.attempt_sequence
            ) THEN
                RAISE EXCEPTION 'dev lifecycle current attempt binding is invalid'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NULL;
        END
        $function$;


CREATE OR REPLACE FUNCTION public.loom_check_dev_membership_successor_complete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            IF OLD.state <> 'superseded' AND NEW.state = 'superseded' AND NOT EXISTS (
                SELECT 1 FROM dev_lifecycle_operations child
                JOIN dev_instances env ON env.name = child.environment_name
                JOIN dev_lifecycle_operation_attempts attempt ON attempt.id = NEW.attempt_id
                JOIN dev_lifecycle_operation_attempts child_attempt ON child_attempt.id = child.attempt_id
                WHERE child.membership_predecessor_operation_id = NEW.id
                  AND env.operation_id = child.id AND env.operation_epoch = child.operation_epoch
                  AND attempt.state = 'superseded'
                  AND attempt.checkpoint = 'membership_successor_created'
                  AND attempt.claimed_by IS NULL AND attempt.lease_expires_at IS NULL
                  AND child.state = 'running' AND child.attempt_sequence = 0
                  AND child.checkpoint = CASE WHEN child.kind = 'destroy'
                      THEN 'capacity_retirement_requested' ELSE 'candidate_build' END
                  AND child_attempt.operation_id = child.id
                  AND child_attempt.operation_epoch = child.operation_epoch
                  AND child_attempt.attempt_sequence = 0 AND child_attempt.state = 'running'
                  AND child_attempt.checkpoint = child.checkpoint
                  AND child_attempt.claimed_by IS NULL AND child_attempt.lease_expires_at IS NULL
                  AND child_attempt.lease_epoch = 0
                  AND env.operation_step = child.checkpoint
                  AND env.status = CASE child.kind WHEN 'destroy' THEN 'deleting'
                      WHEN 'create' THEN 'provisioning' ELSE 'updating' END
            ) THEN
                RAISE EXCEPTION 'membership successor transition is incomplete';
            END IF;
            RETURN NULL;
        END $function$;


CREATE OR REPLACE FUNCTION public.loom_check_personal_storage_handoff()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        DECLARE environment dev_instances%ROWTYPE; current_operation dev_lifecycle_operations%ROWTYPE;
        BEGIN
            IF TG_TABLE_NAME = 'dev_instances' THEN
                SELECT * INTO environment FROM dev_instances WHERE name = NEW.name;
            ELSE
                SELECT * INTO environment FROM dev_instances WHERE name = NEW.environment_name;
            END IF;
            SELECT * INTO current_operation FROM dev_lifecycle_operations WHERE id = environment.operation_id;
            IF environment.storage_binding IS DISTINCT FROM current_operation.storage_binding
               OR environment.storage_binding_sha256 IS DISTINCT FROM current_operation.storage_binding_sha256 THEN
                RAISE EXCEPTION 'personal storage handoff differs from current environment';
            END IF;
            RETURN NULL;
        END $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_dev_lifecycle_activation_acknowledgement()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            RAISE EXCEPTION 'dev lifecycle activation acknowledgement is append-only'
                USING ERRCODE = 'integrity_constraint_violation';
        END
        $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_dev_lifecycle_attempt_binding()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
               OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
               OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
               OR NEW.operation_epoch IS DISTINCT FROM OLD.operation_epoch
               OR NEW.attempt_sequence IS DISTINCT FROM OLD.attempt_sequence
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.started_at IS DISTINCT FROM OLD.started_at

               OR NEW.credential_binding_version IS DISTINCT FROM OLD.credential_binding_version
               OR NEW.bootstrap_auth_kind IS DISTINCT FROM OLD.bootstrap_auth_kind
               OR NEW.bootstrap_credential_hash IS DISTINCT FROM OLD.bootstrap_credential_hash
            THEN
                RAISE EXCEPTION 'dev lifecycle operation attempt binding is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END
        $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_dev_lifecycle_operation_binding()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
               OR NEW.environment_name IS DISTINCT FROM OLD.environment_name
               OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
               OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
               OR NEW.owner_user_id IS DISTINCT FROM OLD.owner_user_id
               OR NEW.owner_team_id IS DISTINCT FROM OLD.owner_team_id
               OR NEW.operation_epoch IS DISTINCT FROM OLD.operation_epoch
               OR NEW.expected_operation_epoch IS DISTINCT FROM OLD.expected_operation_epoch
               OR NEW.kind IS DISTINCT FROM OLD.kind
               OR NEW.request_sha256 IS DISTINCT FROM OLD.request_sha256
               OR NEW.candidate_id IS DISTINCT FROM OLD.candidate_id
               OR NEW.candidate_sha IS DISTINCT FROM OLD.candidate_sha
               OR NEW.min_slots IS DISTINCT FROM OLD.min_slots
               OR NEW.max_slots IS DISTINCT FROM OLD.max_slots
               OR NEW.deployment_generation IS DISTINCT FROM OLD.deployment_generation
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'dev lifecycle operation binding is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END
        $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_dev_membership_lineage()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        DECLARE parent dev_lifecycle_operations%ROWTYPE;
                accepted dev_lifecycle_operations%ROWTYPE;
        BEGIN
            IF TG_OP IN ('UPDATE', 'DELETE') AND EXISTS (
                SELECT 1 FROM dev_lifecycle_operations
                WHERE membership_accepted_operation_id = OLD.id
            ) THEN
                IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'accepted membership source history is immutable';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF OLD.state = 'superseded' OR OLD.membership_predecessor_operation_id IS NOT NULL THEN
                    RAISE EXCEPTION 'membership successor history cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF OLD.state = 'superseded' AND NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'superseded membership history is immutable';
                END IF;
                IF NEW.state = 'superseded' AND OLD.state <> 'superseded' THEN
                    IF OLD.state NOT IN ('running', 'activating')
                       OR OLD.checkpoint <> 'membership_outcome_resolved'
                       OR (to_jsonb(NEW) - ARRAY['state','checkpoint','updated_at','finished_at'])
                          IS DISTINCT FROM
                          (to_jsonb(OLD) - ARRAY['state','checkpoint','updated_at','finished_at']) THEN
                        RAISE EXCEPTION 'membership successor cannot rewrite predecessor';
                    END IF;
                END IF;
                IF ROW(NEW.membership_predecessor_operation_id,
                       NEW.membership_accepted_operation_id,
                       NEW.membership_predecessor_envelope_sha256, NEW.membership_successor_binding,
                       NEW.membership_successor_binding_sha256, NEW.membership_continuation_kind)
                   IS DISTINCT FROM ROW(OLD.membership_predecessor_operation_id,
                       OLD.membership_accepted_operation_id,
                       OLD.membership_predecessor_envelope_sha256, OLD.membership_successor_binding,
                       OLD.membership_successor_binding_sha256, OLD.membership_continuation_kind) THEN
                    RAISE EXCEPTION 'membership successor lineage is immutable';
                END IF;
                IF OLD.membership_predecessor_operation_id IS NOT NULL AND
                   ROW(NEW.id, NEW.idempotency_key, NEW.environment_name, NEW.subject_id,
                       NEW.subject_incarnation, NEW.owner_user_id, NEW.owner_team_id,
                       NEW.operation_epoch, NEW.expected_operation_epoch, NEW.kind,
                       NEW.request_sha256, NEW.candidate_id, NEW.candidate_sha,
                       NEW.min_slots, NEW.max_slots, NEW.deployment_generation, NEW.keep_data,
                       NEW.capacity_mode)
                   IS DISTINCT FROM
                   ROW(OLD.id, OLD.idempotency_key, OLD.environment_name, OLD.subject_id,
                       OLD.subject_incarnation, OLD.owner_user_id, OLD.owner_team_id,
                       OLD.operation_epoch, OLD.expected_operation_epoch, OLD.kind,
                       OLD.request_sha256, OLD.candidate_id, OLD.candidate_sha,
                       OLD.min_slots, OLD.max_slots, OLD.deployment_generation, OLD.keep_data,
                       OLD.capacity_mode) THEN
                    RAISE EXCEPTION 'membership successor owner intent is immutable';
                END IF;
                IF OLD.membership_predecessor_operation_id IS NOT NULL AND OLD.kind = 'destroy'
                   AND ROW(NEW.capacity_reporter_incarnation, NEW.capacity_reporter_token_sha256,
                       NEW.local_activation_sha256, NEW.protected_admission_sha256,
                       NEW.capacity_agent_installation_sha256, NEW.capacity_supported_pool_ids,
                       NEW.capacity_supported_architectures)
                   IS DISTINCT FROM ROW(OLD.capacity_reporter_incarnation, OLD.capacity_reporter_token_sha256,
                       OLD.local_activation_sha256, OLD.protected_admission_sha256,
                       OLD.capacity_agent_installation_sha256, OLD.capacity_supported_pool_ids,
                       OLD.capacity_supported_architectures) THEN
                    RAISE EXCEPTION 'destroy successor retained evidence is immutable';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.state = 'superseded' THEN
                RAISE EXCEPTION 'membership predecessor must transition under a lease';
            END IF;
            IF NEW.membership_predecessor_operation_id IS NULL THEN RETURN NEW; END IF;
            SELECT * INTO parent FROM dev_lifecycle_operations
              WHERE id = NEW.membership_predecessor_operation_id FOR KEY SHARE;
            IF NOT FOUND OR parent.state <> 'superseded'
               OR parent.capacity_mode <> 'membership-v1'
               OR ROW(NEW.environment_name, NEW.subject_id, NEW.subject_incarnation,
                   NEW.owner_user_id, NEW.owner_team_id, NEW.candidate_id, NEW.candidate_sha,
                   NEW.min_slots, NEW.max_slots, NEW.keep_data)
                  IS DISTINCT FROM
                  ROW(parent.environment_name, parent.subject_id, parent.subject_incarnation,
                   parent.owner_user_id, parent.owner_team_id, parent.candidate_id, parent.candidate_sha,
                   parent.min_slots, parent.max_slots, parent.keep_data)
               OR NEW.operation_epoch <> parent.operation_epoch + 1
               OR NEW.expected_operation_epoch <> parent.operation_epoch
               OR NEW.id = parent.id OR NEW.idempotency_key = parent.idempotency_key
               OR NEW.attempt_id = parent.attempt_id
               OR NEW.membership_successor_binding->>'request_sha256' IS DISTINCT FROM parent.request_sha256
               OR NEW.membership_continuation_kind IS DISTINCT FROM
                  COALESCE(parent.membership_continuation_kind, parent.kind)
               OR NEW.kind IS DISTINCT FROM (CASE
                   WHEN parent.kind = 'destroy' THEN 'destroy'
                   WHEN NEW.membership_successor_binding->'adopted_member' = 'null'::jsonb THEN 'create'
                   ELSE 'update' END)
               OR NEW.state <> 'running' OR NEW.attempt_sequence <> 0
               OR NEW.checkpoint IS DISTINCT FROM (CASE WHEN NEW.kind = 'destroy'
                   THEN 'capacity_retirement_requested' ELSE 'candidate_build' END)
               OR (NEW.kind <> 'destroy' AND NEW.deployment_generation <= parent.deployment_generation)
               OR (NEW.kind = 'destroy' AND NEW.deployment_generation IS DISTINCT FROM
                   (NEW.membership_successor_binding->'adopted_member'->'configuration'
                    ->>'deployment_generation')::bigint)
               OR NEW.capacity_membership_envelope IS NOT NULL THEN
                RAISE EXCEPTION 'membership successor differs from predecessor intent';
            END IF;
            IF NEW.kind = 'destroy' AND ROW(
                NEW.capacity_reporter_incarnation, NEW.capacity_reporter_token_sha256,
                NEW.local_activation_sha256, NEW.protected_admission_sha256,
                NEW.capacity_agent_installation_sha256, NEW.capacity_supported_pool_ids,
                NEW.capacity_supported_architectures
            ) IS DISTINCT FROM ROW(
                parent.capacity_reporter_incarnation, parent.capacity_reporter_token_sha256,
                parent.local_activation_sha256, parent.protected_admission_sha256,
                parent.capacity_agent_installation_sha256, parent.capacity_supported_pool_ids,
                parent.capacity_supported_architectures
            ) THEN
                RAISE EXCEPTION 'destroy successor must retain predecessor evidence';
            END IF;
            IF NEW.membership_accepted_operation_id IS NOT NULL THEN
                SELECT * INTO accepted FROM dev_lifecycle_operations
                    WHERE id = NEW.membership_accepted_operation_id FOR UPDATE;
                IF NOT FOUND OR accepted.state <> 'succeeded' OR accepted.checkpoint <> 'complete'
                   OR ROW(accepted.environment_name, accepted.subject_id, accepted.subject_incarnation,
                       accepted.owner_user_id, accepted.owner_team_id)
                   IS DISTINCT FROM ROW(NEW.environment_name, NEW.subject_id, NEW.subject_incarnation,
                       NEW.owner_user_id, NEW.owner_team_id) THEN
                    RAISE EXCEPTION 'membership successor accepted source is invalid';
                END IF;
            END IF;
            RETURN NEW;
        END $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_dev_membership_successor_attempt()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                       WHERE membership_accepted_operation_id = OLD.operation_id)
               AND (TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD) THEN
                RAISE EXCEPTION 'accepted membership source attempt is immutable';
            END IF;
            IF OLD.state = 'superseded' THEN
                IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'superseded membership attempt is immutable';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                           WHERE id = OLD.operation_id AND membership_predecessor_operation_id IS NOT NULL) THEN
                    RAISE EXCEPTION 'membership successor attempt cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF NEW.state = 'superseded' AND OLD.state <> 'superseded' AND (
                OLD.state NOT IN ('running', 'activating')
                OR OLD.checkpoint <> 'membership_outcome_resolved'
                OR (to_jsonb(NEW) - ARRAY['state','checkpoint','updated_at','finished_at','claimed_by','lease_expires_at'])
                   IS DISTINCT FROM
                   (to_jsonb(OLD) - ARRAY['state','checkpoint','updated_at','finished_at','claimed_by','lease_expires_at'])
                OR NOT EXISTS (SELECT 1 FROM dev_lifecycle_operations
                               WHERE id = OLD.operation_id AND state = 'superseded'
                                 AND attempt_id = OLD.id)
            ) THEN
                RAISE EXCEPTION 'membership attempt cannot rewrite predecessor';
            END IF;
            RETURN NEW;
        END $function$;


CREATE OR REPLACE FUNCTION public.loom_guard_personal_storage_binding()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        DECLARE calculated text; current_storage jsonb;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.storage_binding IS NOT NULL THEN
                    RAISE EXCEPTION 'personal storage history cannot be deleted';
                END IF;
                IF TG_TABLE_NAME = 'dev_lifecycle_operations' THEN
                    -- A locking read follows a concurrent recreation's updated
                    -- row rather than accepting the statement's old NULL image.
                    SELECT environment.storage_binding INTO current_storage
                    FROM dev_instances environment WHERE environment.name = OLD.environment_name
                    FOR UPDATE;
                    IF current_storage IS NOT NULL
                       OR EXISTS (SELECT 1 FROM dev_lifecycle_operations history
                                  WHERE history.environment_name = OLD.environment_name
                                    AND history.storage_binding IS NOT NULL) THEN
                        RAISE EXCEPTION 'personal storage legacy predecessor history cannot be deleted';
                    END IF;
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF (TG_TABLE_NAME = 'dev_lifecycle_operations'
                    OR NEW.subject_incarnation = OLD.subject_incarnation)
                   AND (NEW.storage_binding IS DISTINCT FROM OLD.storage_binding
                        OR NEW.storage_binding_sha256 IS DISTINCT FROM OLD.storage_binding_sha256) THEN
                    RAISE EXCEPTION 'personal storage binding is immutable within an incarnation';
                END IF;
                IF OLD.storage_binding IS NOT NULL AND NEW.storage_binding IS NULL THEN
                    RAISE EXCEPTION 'personal storage layout cannot downgrade to legacy';
                END IF;
                IF TG_TABLE_NAME = 'dev_instances'
                   AND NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
                   AND (OLD.storage_binding IS NOT NULL OR NEW.storage_binding IS NOT NULL) THEN
                    IF OLD.status <> 'deleted' OR NEW.status <> 'provisioning'
                       OR NEW.operation_epoch <> OLD.operation_epoch + 1
                       OR NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
                       OR ROW(NEW.name, NEW.subject_id, NEW.owner_user_id, NEW.owner_team_id)
                          IS DISTINCT FROM ROW(OLD.name, OLD.subject_id, OLD.owner_user_id, OLD.owner_team_id)
                       OR NOT EXISTS (
                           SELECT 1 FROM dev_lifecycle_operations retired
                           WHERE retired.id = OLD.operation_id AND retired.kind = 'destroy'
                             AND retired.state = 'succeeded'
                             AND retired.checkpoint IN ('complete', 'pre_activation_abandoned')
                             AND retired.checkpoint = OLD.operation_step
                             AND retired.environment_name = OLD.name
                             AND retired.subject_id = OLD.subject_id
                             AND retired.owner_user_id = OLD.owner_user_id
                             AND retired.owner_team_id = OLD.owner_team_id
                             AND retired.keep_data = OLD.keep_data
                             AND retired.subject_incarnation = OLD.subject_incarnation
                             AND retired.operation_epoch = OLD.operation_epoch
                             AND retired.storage_binding IS NOT DISTINCT FROM OLD.storage_binding
                             AND retired.storage_binding_sha256 IS NOT DISTINCT FROM OLD.storage_binding_sha256
                       )
                       OR EXISTS (
                           SELECT 1 FROM dev_lifecycle_operations history
                           WHERE history.environment_name = NEW.name
                             AND history.subject_incarnation = NEW.subject_incarnation
                             AND history.id IS DISTINCT FROM NEW.operation_id
                       ) THEN
                        RAISE EXCEPTION 'personal storage incarnation requires fresh release-gated recreation';
                    END IF;
                END IF;
            END IF;
            IF NEW.storage_binding IS NOT NULL THEN
                -- Every accepted value is an ASCII scalar under the exact shape
                -- constraint. Explicit lexical ordering matches canonical JSON.
                SELECT encode(sha256(convert_to('{' || string_agg(
                    to_json(key)::text || ':' || value::text, ',' ORDER BY key COLLATE "C"
                ) || '}', 'UTF8')), 'hex') INTO calculated
                FROM jsonb_each(NEW.storage_binding);
                IF calculated IS DISTINCT FROM NEW.storage_binding_sha256 THEN
                    RAISE EXCEPTION 'personal storage binding digest is invalid';
                END IF;
            END IF;
            IF TG_TABLE_NAME = 'dev_lifecycle_operations' THEN
               IF NEW.membership_accepted_operation_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM dev_lifecycle_operations accepted
                           WHERE accepted.id = NEW.membership_accepted_operation_id
                             AND (accepted.storage_binding IS DISTINCT FROM NEW.storage_binding
                               OR accepted.storage_binding_sha256 IS DISTINCT FROM NEW.storage_binding_sha256)) THEN
                RAISE EXCEPTION 'membership successor differs from accepted storage binding';
               END IF;
               IF NEW.membership_predecessor_operation_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM dev_lifecycle_operations previous
                           WHERE previous.id = NEW.membership_predecessor_operation_id
                             AND (previous.storage_binding IS DISTINCT FROM NEW.storage_binding
                               OR previous.storage_binding_sha256 IS DISTINCT FROM NEW.storage_binding_sha256)) THEN
                RAISE EXCEPTION 'membership successor changed retained storage binding';
               END IF;
            END IF;
            RETURN NEW;
        END $function$;


CREATE OR REPLACE FUNCTION public.reject_personal_dev_artifact_collection_mutation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            RAISE EXCEPTION
                'personal-dev artifact collection evidence is append-only';
        END
        $function$;


ALTER TABLE ONLY public.dev_instances
    ADD CONSTRAINT dev_instances_pkey PRIMARY KEY (name);
ALTER TABLE ONLY public.dev_instances
    ADD CONSTRAINT dev_instances_subject_id_uidx UNIQUE (subject_id);
ALTER TABLE ONLY public.dev_lifecycle_activation_acknowledgements
    ADD CONSTRAINT dev_lifecycle_activation_acknowledgements_payload_uidx UNIQUE (payload_sha256);
ALTER TABLE ONLY public.dev_lifecycle_activation_acknowledgements
    ADD CONSTRAINT dev_lifecycle_activation_acknowledgements_pkey PRIMARY KEY (operation_id);
ALTER TABLE ONLY public.dev_lifecycle_operation_attempts
    ADD CONSTRAINT dev_lifecycle_operation_attempts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.dev_lifecycle_operation_attempts
    ADD CONSTRAINT dev_lifecycle_operation_attempts_sequence_uidx UNIQUE (operation_id, attempt_sequence);
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_attempt_id_uidx UNIQUE (attempt_id);
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_owner_idempotency_uidx UNIQUE (owner_user_id, idempotency_key);
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_request_uidx UNIQUE (subject_id, subject_incarnation, expected_operation_epoch, request_sha256, capacity_mode);
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_successor_predecessor_uidx UNIQUE (membership_predecessor_operation_id);
ALTER TABLE ONLY public.gb10_worker_node_statuses
    ADD CONSTRAINT gb10_worker_node_statuses_environment_pool_host_uidx UNIQUE (environment, pool_name, hostname);
ALTER TABLE ONLY public.gb10_worker_node_statuses
    ADD CONSTRAINT gb10_worker_node_statuses_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gb10_worker_pool_desired_states
    ADD CONSTRAINT gb10_worker_pool_desired_states_environment_pool_uidx UNIQUE (environment, pool_name);
ALTER TABLE ONLY public.gb10_worker_pool_desired_states
    ADD CONSTRAINT gb10_worker_pool_desired_states_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_build_platform_requests
    ADD CONSTRAINT personal_build_request_attempt_uidx UNIQUE (attempt_id, attempt_lease_epoch, platform);
ALTER TABLE ONLY public.personal_dev_build_platform_requests
    ADD CONSTRAINT personal_dev_build_platform_requests_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_candidate_artifact_collections
    ADD CONSTRAINT personal_dev_candidate_artifact_collections_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_candidate_artifact_collections
    ADD CONSTRAINT personal_dev_candidate_artifact_collections_sequence_uidx UNIQUE (candidate_id, collection_sequence);
ALTER TABLE ONLY public.personal_dev_candidate_build_attempts
    ADD CONSTRAINT personal_dev_candidate_build_attempts_operation_uidx UNIQUE (subject_id, subject_incarnation, operation_epoch, attempt_sequence);
ALTER TABLE ONLY public.personal_dev_candidate_build_attempts
    ADD CONSTRAINT personal_dev_candidate_build_attempts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_candidate_build_attempts
    ADD CONSTRAINT personal_dev_candidate_build_attempts_sequence_uidx UNIQUE (candidate_id, attempt_sequence);
ALTER TABLE ONLY public.personal_dev_candidates
    ADD CONSTRAINT personal_dev_candidates_owner_source_uidx UNIQUE (owner_user_id, owner_team_id, source_sha256, archive_sha256, build_contract_sha256);
ALTER TABLE ONLY public.personal_dev_candidates
    ADD CONSTRAINT personal_dev_candidates_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_attempt_platform_uidx UNIQUE (attempt_id, attempt_lease_epoch, platform);
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.personal_dev_native_builder_agents
    ADD CONSTRAINT personal_dev_native_builder_agents_key_uidx UNIQUE (key_id);
ALTER TABLE ONLY public.personal_dev_native_builder_agents
    ADD CONSTRAINT personal_dev_native_builder_agents_pkey PRIMARY KEY (instance_id);
ALTER TABLE ONLY public.pipeline_acceptance_evidence_runs
    ADD CONSTRAINT pipeline_acceptance_evidence_runs_pkey PRIMARY KEY (artifact_id, run_ordinal);
ALTER TABLE ONLY public.pipeline_run_gpu_backend_selections
    ADD CONSTRAINT pipeline_gpu_selection_run_scope_uidx UNIQUE (pipeline_run_id, scope);
ALTER TABLE ONLY public.pipeline_input_materialization_evidence
    ADD CONSTRAINT pipeline_input_materialization_evidence_pkey PRIMARY KEY (execution_attempt_id);
ALTER TABLE ONLY public.pipeline_scoped_policy_activations
    ADD CONSTRAINT pipeline_policy_activation_authority_policy_uidx UNIQUE (authority_kind, authority_id, policy_id);
ALTER TABLE ONLY public.pipeline_scoped_policy_activations
    ADD CONSTRAINT pipeline_policy_activation_environment_epoch_uidx UNIQUE (environment, policy_id, activation_epoch);
ALTER TABLE ONLY public.pipeline_run_gpu_backend_selections
    ADD CONSTRAINT pipeline_run_gpu_backend_selections_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.pipeline_scoped_policy_activations
    ADD CONSTRAINT pipeline_scoped_policy_activations_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_candidate_uidx UNIQUE (candidate_sha256);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_nonce_uidx UNIQUE (nonce_sha256);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_pipeline_run_id_key UNIQUE (pipeline_run_id);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_pkey PRIMARY KEY (authorization_id);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_policy_activation_id_key UNIQUE (policy_activation_id);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_capacity_idempotency_ UNIQUE (team_id, capacity_idempotency_key);
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_execute_idempotency_u UNIQUE (team_id, execute_idempotency_key);
ALTER TABLE ONLY public.pipeline_stage1_smoke_events
    ADD CONSTRAINT pipeline_stage1_smoke_events_kind_uidx UNIQUE (authorization_id, event_kind);
ALTER TABLE ONLY public.pipeline_stage1_smoke_events
    ADD CONSTRAINT pipeline_stage1_smoke_events_pkey PRIMARY KEY (authorization_id, seq);
ALTER TABLE ONLY public.slurm_worker_jobs
    ADD CONSTRAINT slurm_worker_jobs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.worker_pool_autoscaler_policies
    ADD CONSTRAINT worker_pool_autoscaler_policies_environment_pool_uidx UNIQUE (environment, pool_name);
ALTER TABLE ONLY public.worker_pool_autoscaler_policies
    ADD CONSTRAINT worker_pool_autoscaler_policies_pkey PRIMARY KEY (id);
CREATE INDEX dev_instances_owner_status_idx ON public.dev_instances USING btree (owner_user_id, status);
CREATE INDEX dev_instances_team_status_idx ON public.dev_instances USING btree (owner_team_id, status);
CREATE UNIQUE INDEX dev_lifecycle_operation_attempts_active_operation_uidx ON public.dev_lifecycle_operation_attempts USING btree (operation_id) WHERE ((state)::text = ANY ((ARRAY['running'::character varying, 'activating'::character varying])::text[]));
CREATE INDEX dev_lifecycle_operation_attempts_picker_idx ON public.dev_lifecycle_operation_attempts USING btree (state, checkpoint, lease_expires_at, created_at, id);
CREATE UNIQUE INDEX dev_lifecycle_operations_active_environment_uidx ON public.dev_lifecycle_operations USING btree (environment_name) WHERE ((state)::text = ANY ((ARRAY['requested'::character varying, 'running'::character varying, 'activating'::character varying, 'cancelling'::character varying])::text[]));
CREATE INDEX dev_lifecycle_operations_environment_created_idx ON public.dev_lifecycle_operations USING btree (environment_name, created_at, id);
CREATE INDEX gb10_worker_node_statuses_pool_state_idx ON public.gb10_worker_node_statuses USING btree (environment, pool_name, apply_state);
CREATE INDEX gb10_worker_pool_desired_states_pool_idx ON public.gb10_worker_pool_desired_states USING btree (environment, pool_name);
CREATE INDEX personal_build_request_owner_pending_idx ON public.personal_dev_build_platform_requests USING btree (owner_user_id, subject_id, subject_incarnation, cancelled_at);
CREATE INDEX personal_dev_candidate_artifact_collections_candidate_idx ON public.personal_dev_candidate_artifact_collections USING btree (candidate_id, collected_at, id);
CREATE INDEX personal_dev_candidate_build_attempts_picker_idx ON public.personal_dev_candidate_build_attempts USING btree (state, lease_expires_at, created_at, id);
CREATE INDEX personal_dev_candidates_artifact_gc_idx ON public.personal_dev_candidates USING btree (artifact_state, artifact_gc_unreferenced_at, artifact_gc_lease_expires_at, id);
CREATE INDEX personal_dev_candidates_owner_created_idx ON public.personal_dev_candidates USING btree (owner_user_id, created_at, id);
CREATE INDEX personal_dev_candidates_status_created_idx ON public.personal_dev_candidates USING btree (status, created_at, id);
CREATE INDEX personal_dev_native_build_grants_agent_state_idx ON public.personal_dev_native_build_grants USING btree (required_agent_instance_id, state, updated_at, id);
CREATE INDEX personal_dev_native_build_grants_attempt_idx ON public.personal_dev_native_build_grants USING btree (attempt_id, attempt_lease_epoch, id);
CREATE INDEX personal_dev_native_build_grants_picker_idx ON public.personal_dev_native_build_grants USING btree (state, queued_at, id);
CREATE INDEX personal_dev_native_builder_agents_freshness_idx ON public.personal_dev_native_builder_agents USING btree (available, last_seen_at, instance_id);
CREATE UNIQUE INDEX pipeline_policy_activation_active_policy_uidx ON public.pipeline_scoped_policy_activations USING btree (environment, policy_id) WHERE (state = 'active'::text);
CREATE UNIQUE INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx ON public.pipeline_stage1_smoke_authorizations USING btree (environment) WHERE (state = ANY (ARRAY['capacity_pending'::text, 'capacity_draining'::text, 'submitted'::text, 'running'::text, 'cleanup_required'::text, 'cleanup_draining'::text]));
CREATE UNIQUE INDEX slurm_worker_jobs_active_capacity_uidx ON public.slurm_worker_jobs USING btree (environment, pool_name, nodelist, COALESCE(requested_cpus, '-1'::integer), COALESCE(requested_memory_mib, '-1'::integer), COALESCE(requested_pids, '-1'::integer), COALESCE(requested_gpu_tres, ''::text), requested_gpus, requested_concurrency) WHERE (state = ANY (ARRAY['pending'::text, 'running'::text]));
CREATE UNIQUE INDEX slurm_worker_jobs_job_id_uidx ON public.slurm_worker_jobs USING btree (slurm_cluster_id, job_id) WHERE (job_id IS NOT NULL);
CREATE INDEX slurm_worker_jobs_pool_state_idx ON public.slurm_worker_jobs USING btree (environment, pool_name, state);
CREATE INDEX slurm_worker_jobs_sandbox_candidate_state_idx ON public.slurm_worker_jobs USING btree (sandbox_identity, candidate_sha, state);
CREATE INDEX worker_pool_autoscaler_policies_pool_idx ON public.worker_pool_autoscaler_policies USING btree (environment, pool_name);
CREATE TRIGGER dev_instances_provider_secret_attachment BEFORE INSERT OR UPDATE OF secret_ref ON public.dev_instances FOR EACH ROW EXECUTE FUNCTION public.guard_provider_secret_attachment('secret_ref');
CREATE TRIGGER dev_instances_storage_binding_guard BEFORE INSERT OR DELETE OR UPDATE ON public.dev_instances FOR EACH ROW EXECUTE FUNCTION public.loom_guard_personal_storage_binding();
CREATE CONSTRAINT TRIGGER dev_instances_storage_handoff AFTER INSERT OR UPDATE ON public.dev_instances DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.loom_check_personal_storage_handoff();
CREATE TRIGGER dev_lifecycle_activation_acknowledgements_append_guard BEFORE DELETE OR UPDATE ON public.dev_lifecycle_activation_acknowledgements FOR EACH ROW EXECUTE FUNCTION public.loom_guard_dev_lifecycle_activation_acknowledgement();
CREATE TRIGGER dev_lifecycle_membership_lineage_guard BEFORE INSERT OR DELETE OR UPDATE ON public.dev_lifecycle_operations FOR EACH ROW EXECUTE FUNCTION public.loom_guard_dev_membership_lineage();
CREATE TRIGGER dev_lifecycle_membership_successor_attempt_guard BEFORE DELETE OR UPDATE ON public.dev_lifecycle_operation_attempts FOR EACH ROW EXECUTE FUNCTION public.loom_guard_dev_membership_successor_attempt();
CREATE CONSTRAINT TRIGGER dev_lifecycle_membership_successor_complete AFTER UPDATE OF state ON public.dev_lifecycle_operations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.loom_check_dev_membership_successor_complete();
CREATE TRIGGER dev_lifecycle_operation_attempts_binding_guard BEFORE UPDATE ON public.dev_lifecycle_operation_attempts FOR EACH ROW EXECUTE FUNCTION public.loom_guard_dev_lifecycle_attempt_binding();
CREATE TRIGGER dev_lifecycle_operations_binding_guard BEFORE UPDATE ON public.dev_lifecycle_operations FOR EACH ROW EXECUTE FUNCTION public.loom_guard_dev_lifecycle_operation_binding();
CREATE CONSTRAINT TRIGGER dev_lifecycle_operations_current_attempt_guard AFTER INSERT OR UPDATE OF attempt_id, attempt_sequence ON public.dev_lifecycle_operations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.loom_check_dev_lifecycle_current_attempt();
CREATE TRIGGER dev_lifecycle_operations_storage_binding_guard BEFORE INSERT OR DELETE OR UPDATE ON public.dev_lifecycle_operations FOR EACH ROW EXECUTE FUNCTION public.loom_guard_personal_storage_binding();
CREATE CONSTRAINT TRIGGER dev_lifecycle_operations_storage_handoff AFTER INSERT OR UPDATE ON public.dev_lifecycle_operations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.loom_check_personal_storage_handoff();
CREATE TRIGGER personal_build_platform_request_immutable BEFORE DELETE OR UPDATE ON public.personal_dev_build_platform_requests FOR EACH ROW EXECUTE FUNCTION public.guard_personal_build_platform_request();
CREATE TRIGGER personal_build_platform_request_no_truncate BEFORE TRUNCATE ON public.personal_dev_build_platform_requests FOR EACH STATEMENT EXECUTE FUNCTION public.guard_personal_build_platform_request();
CREATE TRIGGER personal_dev_artifact_collections_append_only BEFORE DELETE OR UPDATE ON public.personal_dev_candidate_artifact_collections FOR EACH ROW EXECUTE FUNCTION public.reject_personal_dev_artifact_collection_mutation();
ALTER TABLE ONLY public.dev_instances
    ADD CONSTRAINT dev_instances_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_instances
    ADD CONSTRAINT dev_instances_owner_team_id_fkey FOREIGN KEY (owner_team_id) REFERENCES public.teams(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_instances
    ADD CONSTRAINT dev_instances_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_activation_acknowledgements
    ADD CONSTRAINT dev_lifecycle_activation_acknowledgements_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES public.dev_lifecycle_operations(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operation_attempts
    ADD CONSTRAINT dev_lifecycle_operation_attempts_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES public.dev_lifecycle_operations(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_environment_name_fkey FOREIGN KEY (environment_name) REFERENCES public.dev_instances(name) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_membership_accepted_fkey FOREIGN KEY (membership_accepted_operation_id) REFERENCES public.dev_lifecycle_operations(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_membership_predecessor_fkey FOREIGN KEY (membership_predecessor_operation_id) REFERENCES public.dev_lifecycle_operations(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_owner_team_id_fkey FOREIGN KEY (owner_team_id) REFERENCES public.teams(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.dev_lifecycle_operations
    ADD CONSTRAINT dev_lifecycle_operations_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.gb10_worker_node_statuses
    ADD CONSTRAINT gb10_worker_node_statuses_worker_id_fkey FOREIGN KEY (worker_id) REFERENCES public.workers(id) ON DELETE SET NULL;
ALTER TABLE ONLY public.personal_dev_build_platform_requests
    ADD CONSTRAINT personal_dev_build_platform_requests_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.personal_dev_candidate_build_attempts(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_build_platform_requests
    ADD CONSTRAINT personal_dev_build_platform_requests_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_build_platform_requests
    ADD CONSTRAINT personal_dev_build_platform_requests_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_candidate_artifact_collections
    ADD CONSTRAINT personal_dev_candidate_artifact_collections_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_candidate_build_attempts
    ADD CONSTRAINT personal_dev_candidate_build_attempts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.personal_dev_candidates
    ADD CONSTRAINT personal_dev_candidates_owner_team_id_fkey FOREIGN KEY (owner_team_id) REFERENCES public.teams(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_candidates
    ADD CONSTRAINT personal_dev_candidates_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES public.personal_dev_candidate_build_attempts(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES public.personal_dev_candidates(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_required_agent_fkey FOREIGN KEY (required_agent_instance_id) REFERENCES public.personal_dev_native_builder_agents(instance_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.personal_dev_native_build_grants
    ADD CONSTRAINT personal_dev_native_build_grants_running_agent_fkey FOREIGN KEY (running_agent_instance_id) REFERENCES public.personal_dev_native_builder_agents(instance_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_acceptance_evidence_runs
    ADD CONSTRAINT pipeline_acceptance_evidence_runs_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.artifacts(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.pipeline_input_materialization_evidence
    ADD CONSTRAINT pipeline_input_materialization_evidence_attempt_worker_fk FOREIGN KEY (execution_attempt_id, worker_id) REFERENCES public.execution_attempts(id, worker_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.pipeline_input_materialization_evidence
    ADD CONSTRAINT pipeline_input_materialization_evidence_worker_id_fkey FOREIGN KEY (worker_id) REFERENCES public.workers(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_run_gpu_backend_selections
    ADD CONSTRAINT pipeline_run_gpu_backend_selections_pipeline_run_id_fkey FOREIGN KEY (pipeline_run_id) REFERENCES public.pipeline_runs(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_operator_user_id_fkey FOREIGN KEY (operator_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_pipeline_run_id_fkey FOREIGN KEY (pipeline_run_id) REFERENCES public.pipeline_runs(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_policy_activation_id_fkey FOREIGN KEY (policy_activation_id) REFERENCES public.pipeline_scoped_policy_activations(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_stage1_smoke_authorizations
    ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_stage1_smoke_events
    ADD CONSTRAINT pipeline_stage1_smoke_events_authorization_id_fkey FOREIGN KEY (authorization_id) REFERENCES public.pipeline_stage1_smoke_authorizations(authorization_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.slurm_worker_jobs
    ADD CONSTRAINT slurm_worker_jobs_worker_id_fkey FOREIGN KEY (worker_id) REFERENCES public.workers(id) ON DELETE SET NULL;
"""
