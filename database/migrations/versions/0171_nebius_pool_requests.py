"""Journal global pool registrations, immutable requests and bound cleanup evidence.

Revision ID: 0171
Revises: 0170

This migration does not activate global admission or authorize external writes.
"""
from alembic import op

revision = "0171"
down_revision = "0170"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
CREATE TABLE nebius_pool_bindings (
	pool_id UUID NOT NULL,
	installation_id UUID NOT NULL,
	cluster_id TEXT NOT NULL,
	node_group_id TEXT NOT NULL,
	policy_revision BIGINT NOT NULL,
	admission_epoch BIGINT NOT NULL,
	mode TEXT NOT NULL,
	binding_json JSONB NOT NULL,
	binding_sha256 TEXT NOT NULL,
	PRIMARY KEY (pool_id),
	CONSTRAINT nebius_pool_physical_identity_key UNIQUE (cluster_id, node_group_id),
	CONSTRAINT nebius_pool_binding_state_check CHECK (policy_revision > 0 AND admission_epoch > 0 AND mode IN ('legacy','closed','global')),
	CONSTRAINT nebius_pool_binding_identity_check CHECK (pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND installation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND length(cluster_id) BETWEEN 1 AND 253 AND length(node_group_id) BETWEEN 1 AND 253),
	CONSTRAINT nebius_pool_binding_payload_check CHECK (jsonb_typeof(binding_json) = 'object' AND binding_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE TABLE nebius_pool_participants (
	participant_id UUID NOT NULL,
	pool_id UUID NOT NULL,
	environment_id UUID NOT NULL,
	incarnation UUID NOT NULL,
	binding_revision BIGINT NOT NULL,
	admission_epoch BIGINT NOT NULL,
	phase TEXT NOT NULL,
	binding_json JSONB NOT NULL,
	binding_sha256 TEXT NOT NULL,
	PRIMARY KEY (participant_id),
	CONSTRAINT nebius_pool_participant_pool_key UNIQUE (participant_id, pool_id),
	CONSTRAINT nebius_pool_participant_incarnation_key UNIQUE (environment_id, incarnation),
	CONSTRAINT nebius_pool_participant_state_check CHECK (binding_revision > 0 AND admission_epoch > 0 AND phase IN ('active','fenced')),
	CONSTRAINT nebius_pool_participant_identity_check CHECK (participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND environment_id <> '00000000-0000-0000-0000-000000000000'::uuid AND incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
	CONSTRAINT nebius_pool_participant_payload_check CHECK (jsonb_typeof(binding_json) = 'object' AND binding_sha256 ~ '^[0-9a-f]{64}$'),
	FOREIGN KEY(pool_id) REFERENCES nebius_pool_bindings (pool_id) ON DELETE RESTRICT
);
CREATE TABLE nebius_pool_machines (
    machine_id UUID PRIMARY KEY,
    pool_id UUID NOT NULL REFERENCES nebius_pool_bindings(pool_id) ON DELETE RESTRICT,
    participant_id UUID,
    role TEXT NOT NULL,
    credential_epoch BIGINT NOT NULL,
    phase TEXT NOT NULL,
    CONSTRAINT nebius_pool_machine_participant_fk FOREIGN KEY(participant_id, pool_id)
        REFERENCES nebius_pool_participants(participant_id, pool_id) ON DELETE RESTRICT,
    CONSTRAINT nebius_pool_machine_state_check CHECK (
        machine_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        credential_epoch > 0 AND phase IN ('active','revoked')),
    CONSTRAINT nebius_pool_machine_role_check CHECK (
        (role = 'participant' AND participant_id IS NOT NULL) OR
        (role IN ('observer','gateway') AND participant_id IS NULL))
);
CREATE TABLE nebius_pool_machine_credentials (
    token_hash BYTEA PRIMARY KEY REFERENCES tokens(token_hash) ON DELETE RESTRICT,
    machine_id UUID NOT NULL REFERENCES nebius_pool_machines(machine_id) ON DELETE RESTRICT,
    credential_epoch BIGINT NOT NULL,
    CONSTRAINT nebius_pool_machine_credential_shape_check CHECK (
        octet_length(token_hash) = 32 AND credential_epoch > 0)
);
CREATE TABLE nebius_pool_requests (
	request_id UUID NOT NULL,
	pool_id UUID NOT NULL,
	participant_id UUID NOT NULL,
	namespace_uid UUID NOT NULL,
	workload_kind TEXT NOT NULL,
	local_work_id UUID NOT NULL,
	generation BIGINT NOT NULL,
	admission_epoch BIGINT NOT NULL,
	target_id TEXT NOT NULL,
	request_sha256 TEXT NOT NULL,
	request_json JSONB NOT NULL,
	deadline_at TIMESTAMP WITH TIME ZONE NOT NULL,
	cpu_millis BIGINT NOT NULL,
	memory_mib BIGINT NOT NULL,
	ephemeral_storage_mib BIGINT NOT NULL,
	pod_slots BIGINT NOT NULL,
	phase TEXT NOT NULL,
	plan_sha256 TEXT,
	plan_json JSONB,
	job_uid UUID,
	cleanup_observation_id UUID,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (request_id),
	CONSTRAINT nebius_pool_request_participant_fk FOREIGN KEY(participant_id, pool_id) REFERENCES nebius_pool_participants (participant_id, pool_id) ON DELETE RESTRICT,
	CONSTRAINT nebius_pool_request_replay_key UNIQUE (participant_id, workload_kind, local_work_id, generation),
	CONSTRAINT nebius_pool_request_plan_key UNIQUE (request_id, plan_sha256, namespace_uid),
	CONSTRAINT nebius_pool_request_identity_check CHECK (generation > 0 AND admission_epoch > 0 AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid AND namespace_uid <> '00000000-0000-0000-0000-000000000000'::uuid AND local_work_id <> '00000000-0000-0000-0000-000000000000'::uuid AND workload_kind IN ('trial','verifier','task_image_build','application_image_build') AND target_id ~ '^[a-z0-9][a-z0-9-]{0,79}$'),
	CONSTRAINT nebius_pool_request_envelope_check CHECK (cpu_millis > 0 AND memory_mib > 0 AND ephemeral_storage_mib >= 0 AND pod_slots > 0),
	CONSTRAINT nebius_pool_request_payload_check CHECK (request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object'),
	CONSTRAINT nebius_pool_request_plan_check CHECK (phase IN ('waiting','reserved','create_intent','observed','cleanup_intent','released','cancelled_unstarted') AND ((phase IN ('waiting','reserved','cancelled_unstarted')) = (plan_sha256 IS NULL)) AND ((plan_sha256 IS NULL) = (plan_json IS NULL)) AND (plan_sha256 IS NULL OR (plan_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(plan_json) = 'object'))),
	CONSTRAINT nebius_pool_request_evidence_check CHECK ((phase NOT IN ('waiting','reserved','cancelled_unstarted','create_intent') OR job_uid IS NULL) AND (phase <> 'observed' OR job_uid IS NOT NULL) AND (job_uid IS NULL OR job_uid <> '00000000-0000-0000-0000-000000000000'::uuid) AND ((phase = 'released') = (cleanup_observation_id IS NOT NULL)))
);
CREATE TABLE nebius_pool_cleanup_observations (
	observation_id UUID NOT NULL,
	request_id UUID NOT NULL,
	plan_sha256 TEXT NOT NULL,
	namespace_uid UUID NOT NULL,
	writer_epoch BIGINT NOT NULL,
	observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
	evidence_json JSONB NOT NULL,
	PRIMARY KEY (observation_id),
	CONSTRAINT nebius_pool_cleanup_binding_key UNIQUE (observation_id, request_id, plan_sha256, namespace_uid),
	CONSTRAINT nebius_pool_cleanup_request_fk FOREIGN KEY(request_id, plan_sha256, namespace_uid) REFERENCES nebius_pool_requests (request_id, plan_sha256, namespace_uid) ON DELETE RESTRICT,
	CONSTRAINT nebius_pool_cleanup_shape_check CHECK (writer_epoch > 0 AND observation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND namespace_uid <> '00000000-0000-0000-0000-000000000000'::uuid AND plan_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(evidence_json) = 'object')
);
ALTER TABLE nebius_pool_requests ADD CONSTRAINT nebius_pool_request_cleanup_fk FOREIGN KEY(cleanup_observation_id, request_id, plan_sha256, namespace_uid) REFERENCES nebius_pool_cleanup_observations (observation_id, request_id, plan_sha256, namespace_uid) ON DELETE RESTRICT;

    """)
    op.execute("""
        CREATE FUNCTION validate_nebius_pool_registration_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            mutable_fields text[];
            previous_revision bigint;
            next_revision bigint;
        BEGIN
            IF TG_TABLE_NAME = 'nebius_pool_bindings' THEN
                mutable_fields := ARRAY['policy_revision','admission_epoch','mode','binding_json','binding_sha256'];
                previous_revision := OLD.policy_revision;
                next_revision := NEW.policy_revision;
            ELSE
                mutable_fields := ARRAY['binding_revision','admission_epoch','phase','binding_json','binding_sha256'];
                previous_revision := OLD.binding_revision;
                next_revision := NEW.binding_revision;
            END IF;
            IF (to_jsonb(OLD) - mutable_fields) IS DISTINCT FROM (to_jsonb(NEW) - mutable_fields) THEN
                RAISE EXCEPTION 'global pool registration identity is immutable';
            END IF;
            IF NEW.admission_epoch < OLD.admission_epoch OR next_revision < previous_revision THEN
                RAISE EXCEPTION 'global pool registration epoch cannot move backwards';
            END IF;
            IF (NEW.binding_json IS DISTINCT FROM OLD.binding_json
                OR NEW.binding_sha256 IS DISTINCT FROM OLD.binding_sha256)
               AND next_revision <= previous_revision THEN
                RAISE EXCEPTION 'global pool binding change needs a new revision';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER nebius_pool_binding_mutation_guard
        BEFORE UPDATE ON nebius_pool_bindings
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_registration_mutation();
        CREATE TRIGGER nebius_pool_participant_mutation_guard
        BEFORE UPDATE ON nebius_pool_participants
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_registration_mutation();

        CREATE FUNCTION validate_nebius_pool_machine_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'global pool machine history is retained';
            END IF;
            IF to_jsonb(NEW) = to_jsonb(OLD) THEN
                RETURN NEW;
            END IF;
            IF TG_TABLE_NAME = 'nebius_pool_machine_credentials' THEN
                RAISE EXCEPTION 'global pool machine credential is immutable';
            END IF;
            IF (to_jsonb(NEW) - ARRAY['credential_epoch','phase']::text[])
                  IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['credential_epoch','phase']::text[])
               OR OLD.phase = 'revoked' OR NEW.credential_epoch < OLD.credential_epoch THEN
                RAISE EXCEPTION 'global pool machine identity is immutable';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER nebius_pool_machine_mutation_guard
        BEFORE UPDATE OR DELETE ON nebius_pool_machines
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_machine_mutation();
        CREATE TRIGGER nebius_pool_machine_credential_mutation_guard
        BEFORE UPDATE OR DELETE ON nebius_pool_machine_credentials
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_machine_mutation();

        CREATE FUNCTION validate_nebius_pool_request_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'global pool request history is retained';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.phase NOT IN ('waiting','reserved') THEN
                    RAISE EXCEPTION 'global pool request must start without external effects';
                END IF;
                RETURN NEW;
            END IF;
            IF (to_jsonb(NEW) - ARRAY['phase','plan_sha256','plan_json','job_uid','cleanup_observation_id']::text[])
               IS DISTINCT FROM
               (to_jsonb(OLD) - ARRAY['phase','plan_sha256','plan_json','job_uid','cleanup_observation_id']::text[]) THEN
                RAISE EXCEPTION 'global pool request identity is immutable';
            END IF;
            IF (OLD.plan_sha256 IS NOT NULL AND
                (NEW.plan_sha256 IS DISTINCT FROM OLD.plan_sha256 OR NEW.plan_json IS DISTINCT FROM OLD.plan_json))
               OR (OLD.job_uid IS NOT NULL AND NEW.job_uid IS DISTINCT FROM OLD.job_uid)
               OR (OLD.cleanup_observation_id IS NOT NULL AND
                   NEW.cleanup_observation_id IS DISTINCT FROM OLD.cleanup_observation_id) THEN
                RAISE EXCEPTION 'global pool request evidence is immutable';
            END IF;
            IF to_jsonb(NEW) = to_jsonb(OLD) THEN
                RETURN NEW;
            END IF;
            IF OLD.phase = 'cleanup_intent' AND NEW.phase = 'cleanup_intent'
               AND OLD.job_uid IS NULL AND NEW.job_uid IS NOT NULL THEN
                RETURN NEW;
            END IF;
            IF NOT (
                (OLD.phase = 'waiting' AND NEW.phase IN ('reserved','cancelled_unstarted'))
                OR (OLD.phase = 'reserved' AND NEW.phase IN ('create_intent','cancelled_unstarted'))
                OR (OLD.phase = 'create_intent' AND NEW.phase IN ('observed','cleanup_intent'))
                OR (OLD.phase = 'observed' AND NEW.phase = 'cleanup_intent')
                OR (OLD.phase = 'cleanup_intent' AND NEW.phase = 'released')
            ) THEN
                RAISE EXCEPTION 'global pool request transition forbidden';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER nebius_pool_request_mutation_guard
        BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_requests
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_request_mutation();

        CREATE FUNCTION retain_nebius_pool_cleanup_observation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NOT EXISTS (
                    SELECT 1 FROM nebius_pool_requests
                    WHERE request_id = NEW.request_id AND plan_sha256 = NEW.plan_sha256
                      AND namespace_uid = NEW.namespace_uid AND phase = 'cleanup_intent'
                ) THEN
                    RAISE EXCEPTION 'global pool cleanup evidence requires cleanup intent'
                        USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'global pool cleanup evidence is immutable';
        END;
        $$;
        CREATE TRIGGER nebius_pool_cleanup_retention_guard
        BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_cleanup_observations
        FOR EACH ROW EXECUTE FUNCTION retain_nebius_pool_cleanup_observation();
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_pool_bindings, nebius_pool_participants, nebius_pool_requests,
                   nebius_pool_cleanup_observations, nebius_pool_machines,
                   nebius_pool_machine_credentials IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_pool_bindings)
               OR EXISTS (SELECT 1 FROM nebius_pool_participants)
               OR EXISTS (SELECT 1 FROM nebius_pool_requests)
               OR EXISTS (SELECT 1 FROM nebius_pool_cleanup_observations) THEN
                RAISE EXCEPTION 'cannot remove global pool history';
            END IF;
        END $$;
        ALTER TABLE nebius_pool_requests DROP CONSTRAINT nebius_pool_request_cleanup_fk;
        DROP TABLE nebius_pool_cleanup_observations;
        DROP TABLE nebius_pool_requests;
        DROP TABLE nebius_pool_machine_credentials;
        DROP TABLE nebius_pool_machines;
        DROP TABLE nebius_pool_participants;
        DROP TABLE nebius_pool_bindings;
        DROP FUNCTION retain_nebius_pool_cleanup_observation();
        DROP FUNCTION validate_nebius_pool_request_mutation();
        DROP FUNCTION validate_nebius_pool_registration_mutation();
        DROP FUNCTION validate_nebius_pool_machine_mutation();
    """)
