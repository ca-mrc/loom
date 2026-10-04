"""Retain owner source uploads and cleanup-qualified common-pool application builds.

Revision ID: 0173
Revises: 0172
"""
from alembic import op

revision = "0173"
down_revision = "0172"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
ALTER TABLE nebius_pool_machines ADD COLUMN workload_scope text NOT NULL DEFAULT 'environment';
ALTER TABLE nebius_pool_machines ADD CONSTRAINT nebius_pool_machine_workload_scope_check CHECK (
    workload_scope IN ('environment','application_builder') AND
    (workload_scope = 'environment' OR role = 'participant'));
CREATE TABLE nebius_application_source_uploads (
    upload_id uuid PRIMARY KEY,
    owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    owner_team_id uuid NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
    installation_id uuid NOT NULL, data_environment_id uuid NOT NULL,
    cluster_id text NOT NULL, source_bucket text NOT NULL, object_key text NOT NULL,
    idempotency_key text NOT NULL, request_sha256 text NOT NULL,
    source_digest text NOT NULL, archive_sha256 text NOT NULL, archive_size_bytes bigint NOT NULL,
    base_commit text, phase text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, verified_at timestamptz,
    CONSTRAINT nebius_application_source_replay_key UNIQUE(owner_user_id,idempotency_key),
    CONSTRAINT nebius_application_source_identity_check CHECK (
        upload_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        installation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        data_environment_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CONSTRAINT nebius_application_source_binding_check CHECK (
        cluster_id ~ '^[a-zA-Z0-9_-]{1,128}$' AND source_bucket ~ '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$' AND
        idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$' AND request_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT nebius_application_source_content_check CHECK (
        source_digest ~ '^sha256:[0-9a-f]{64}$' AND archive_sha256 ~ '^[0-9a-f]{64}$' AND
        archive_size_bytes BETWEEN 10240 AND 570870784 AND archive_size_bytes % 10240 = 0 AND
        (base_commit IS NULL OR base_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$') AND
        object_key = 'application-sources/v1/sha256/' || archive_sha256 || '.tar'),
    CONSTRAINT nebius_application_source_phase_check CHECK (
        phase IN ('awaiting_source','source_verified') AND expires_at > created_at AND
        expires_at <= created_at + interval '1 hour' AND
        ((phase = 'source_verified') = (verified_at IS NOT NULL)) AND
        (verified_at IS NULL OR (verified_at >= created_at AND verified_at < expires_at)))
);
CREATE INDEX nebius_application_source_owner_idx ON nebius_application_source_uploads(owner_user_id,owner_team_id);
CREATE FUNCTION retain_nebius_application_source_upload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'application source history is retained' USING ERRCODE='23514';
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.phase <> 'awaiting_source' OR NEW.verified_at IS NOT NULL THEN
            RAISE EXCEPTION 'application source starts unverified' USING ERRCODE='23514';
        END IF;
    ELSE
        IF (to_jsonb(OLD)-ARRAY['phase','verified_at']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['phase','verified_at']) OR
           (OLD.verified_at IS NOT NULL AND ROW(OLD.phase,OLD.verified_at) IS DISTINCT FROM
                                           ROW(NEW.phase,NEW.verified_at)) THEN
            RAISE EXCEPTION 'application source identity and evidence are immutable' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_application_source_retain BEFORE INSERT OR UPDATE OR DELETE
    ON nebius_application_source_uploads FOR EACH ROW EXECUTE FUNCTION retain_nebius_application_source_upload();

CREATE TABLE nebius_application_builds (
    build_id uuid PRIMARY KEY,
    upload_id uuid NOT NULL REFERENCES nebius_application_source_uploads(upload_id) ON DELETE RESTRICT,
    owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    owner_team_id uuid NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
    installation_id uuid NOT NULL, data_environment_id uuid NOT NULL, cluster_id text NOT NULL,
    idempotency_key text NOT NULL, request_sha256 text NOT NULL, binding_json jsonb NOT NULL,
    current_attempt bigint NOT NULL, desired_state text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT nebius_application_build_replay_key UNIQUE(owner_user_id,idempotency_key),
    CONSTRAINT nebius_application_build_state_check CHECK (
        current_attempt > 0 AND desired_state IN ('running','cancelled')),
    CONSTRAINT nebius_application_build_request_check CHECK (
        idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$' AND request_sha256 ~ '^[0-9a-f]{64}$' AND
        jsonb_typeof(binding_json) = 'object')
);
CREATE INDEX nebius_application_build_owner_idx ON nebius_application_builds(owner_user_id,owner_team_id);
CREATE TABLE nebius_application_build_attempts (
    build_id uuid NOT NULL REFERENCES nebius_application_builds(build_id) ON DELETE RESTRICT,
    attempt bigint NOT NULL, claim_json jsonb NOT NULL, claim_sha256 text NOT NULL, phase text NOT NULL,
    pool_request_json jsonb, pool_request_sha256 text,
    runner_epoch bigint NOT NULL DEFAULT 0, lease_token uuid, lease_expires_at timestamptz,
    grant_json jsonb, activation_json jsonb, activated_json jsonb,
    settlement_json jsonb, terminal_receipt_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(build_id,attempt),
    CONSTRAINT nebius_application_build_attempt_state_check CHECK (
        attempt > 0 AND phase IN ('queued','running','settling','ready','failed','cancelled')),
    CONSTRAINT nebius_application_build_lease_check CHECK (
        runner_epoch >= 0 AND ((lease_token IS NULL) = (lease_expires_at IS NULL)) AND
        (lease_token IS NULL OR runner_epoch > 0)),
    CONSTRAINT nebius_application_build_completion_check CHECK (
        (phase <> 'running' OR activated_json IS NOT NULL) AND
        (phase <> 'settling' OR settlement_json IS NOT NULL) AND
        (phase NOT IN ('ready','failed','cancelled') OR lease_token IS NULL) AND
        (phase NOT IN ('ready','failed','cancelled') OR pool_request_json IS NULL OR terminal_receipt_json IS NOT NULL) AND
        (phase <> 'ready' OR COALESCE(terminal_receipt_json->>'phase' = 'released' AND
            settlement_json->>'outcome' = 'ready' AND jsonb_typeof(settlement_json->'publication') = 'object', false))),
    CONSTRAINT nebius_application_build_attempt_input_check CHECK (
        claim_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(claim_json) = 'object' AND
        COALESCE(claim_json->>'build_id' = build_id::text AND (claim_json->>'attempt')::bigint = attempt, false)),
    CONSTRAINT nebius_application_build_pool_request_check CHECK (
        (pool_request_json IS NULL AND pool_request_sha256 IS NULL) OR
        (pool_request_json IS NOT NULL AND pool_request_sha256 IS NOT NULL AND
         jsonb_typeof(pool_request_json) = 'object' AND pool_request_sha256 ~ '^[0-9a-f]{64}$'))
);
CREATE FUNCTION retain_nebius_application_build() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'application build history is retained' USING ERRCODE='23514';
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.current_attempt <> 1 OR NEW.desired_state <> 'running' OR
           NEW.build_id = '00000000-0000-0000-0000-000000000000'::uuid OR NOT EXISTS (
            SELECT 1 FROM nebius_application_source_uploads s WHERE s.upload_id=NEW.upload_id
            AND s.phase='source_verified' AND s.owner_user_id=NEW.owner_user_id AND s.owner_team_id=NEW.owner_team_id
            AND s.installation_id=NEW.installation_id AND s.data_environment_id=NEW.data_environment_id
            AND s.cluster_id=NEW.cluster_id) THEN
            RAISE EXCEPTION 'application build requires verified owned source' USING ERRCODE='23514';
        END IF;
    ELSIF (to_jsonb(OLD)-ARRAY['current_attempt','desired_state']) IS DISTINCT FROM
          (to_jsonb(NEW)-ARRAY['current_attempt','desired_state']) OR
          NEW.current_attempt < OLD.current_attempt OR NEW.current_attempt > OLD.current_attempt + 1 THEN
        RAISE EXCEPTION 'application build identity is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.current_attempt <> OLD.current_attempt AND NOT EXISTS (
        SELECT 1 FROM nebius_application_build_attempts a WHERE a.build_id=OLD.build_id
        AND a.attempt=OLD.current_attempt AND a.phase IN ('failed','cancelled')
        AND a.lease_token IS NULL AND (a.pool_request_json IS NULL OR
            a.terminal_receipt_json->>'phase' IN ('released','cancelled_unstarted'))) THEN
        RAISE EXCEPTION 'application retry requires prior cleanup' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_application_build_retain BEFORE INSERT OR UPDATE OR DELETE
    ON nebius_application_builds FOR EACH ROW EXECUTE FUNCTION retain_nebius_application_build();
CREATE FUNCTION retain_nebius_application_build_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE field text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'application build attempt history is retained' USING ERRCODE='23514';
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.phase <> 'queued' OR NEW.pool_request_json IS NOT NULL OR NEW.pool_request_sha256 IS NOT NULL OR
           NEW.runner_epoch <> 0 OR NEW.lease_token IS NOT NULL OR NEW.lease_expires_at IS NOT NULL OR
           NEW.grant_json IS NOT NULL OR NEW.activation_json IS NOT NULL OR NEW.activated_json IS NOT NULL OR
           NEW.settlement_json IS NOT NULL OR NEW.terminal_receipt_json IS NOT NULL OR NOT EXISTS (
            SELECT 1 FROM nebius_application_builds b
            JOIN nebius_application_source_uploads s ON s.upload_id=b.upload_id
            WHERE b.build_id=NEW.build_id AND b.current_attempt=NEW.attempt
            AND NEW.claim_json->>'owner_user_id'=b.owner_user_id::text
            AND NEW.claim_json->>'owner_team_id'=b.owner_team_id::text
            AND NEW.claim_json->>'installation_id'=b.installation_id::text
            AND NEW.claim_json->>'data_environment_id'=b.data_environment_id::text
            AND NEW.claim_json->>'cluster_id'=b.cluster_id
            AND NEW.claim_json->>'upload_id'=s.upload_id::text
            AND NEW.claim_json->'source'->>'source_digest'=s.source_digest
            AND NEW.claim_json->'source'->>'archive_sha256'=s.archive_sha256
            AND NEW.claim_json->'recipe'=b.binding_json->'recipe') THEN
            RAISE EXCEPTION 'application build attempt requires retained identity' USING ERRCODE='23514';
        END IF;
    ELSE
        IF (to_jsonb(OLD)-ARRAY['phase','pool_request_json','pool_request_sha256','runner_epoch','lease_token',
               'lease_expires_at','grant_json','activation_json','activated_json','settlement_json','terminal_receipt_json']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['phase','pool_request_json','pool_request_sha256','runner_epoch','lease_token',
               'lease_expires_at','grant_json','activation_json','activated_json','settlement_json','terminal_receipt_json']) OR
           (OLD.pool_request_json IS NOT NULL AND ROW(OLD.pool_request_json,OLD.pool_request_sha256)
                IS DISTINCT FROM ROW(NEW.pool_request_json,NEW.pool_request_sha256)) OR
           NEW.runner_epoch < OLD.runner_epoch OR NEW.runner_epoch > OLD.runner_epoch + 1 OR
           (OLD.phase IN ('ready','failed','cancelled') AND to_jsonb(OLD) IS DISTINCT FROM to_jsonb(NEW)) OR
           (OLD.phase = 'running' AND NEW.phase = 'queued') OR
           (OLD.phase = 'settling' AND NEW.phase IN ('queued','running')) THEN
            RAISE EXCEPTION 'application build attempt inputs are immutable' USING ERRCODE='23514';
        END IF;
        FOREACH field IN ARRAY ARRAY['grant_json','activation_json','activated_json','settlement_json','terminal_receipt_json'] LOOP
            IF to_jsonb(OLD)->field <> 'null'::jsonb AND
               to_jsonb(OLD)->field IS DISTINCT FROM to_jsonb(NEW)->field THEN
                RAISE EXCEPTION 'application build evidence is immutable' USING ERRCODE='23514';
            END IF;
        END LOOP;
        IF NEW.terminal_receipt_json IS NOT NULL AND NOT COALESCE(
            NEW.terminal_receipt_json->>'phase' IN ('released','cancelled_unstarted'), false) THEN
            RAISE EXCEPTION 'application build requires terminal pool receipt' USING ERRCODE='23514';
        END IF;
        IF NEW.phase = 'ready' AND NOT EXISTS (
            SELECT 1 FROM nebius_application_builds b WHERE b.build_id=NEW.build_id
            AND b.current_attempt=NEW.attempt AND b.desired_state='running') THEN
            RAISE EXCEPTION 'cancelled application build cannot become ready' USING ERRCODE='23514';
        END IF;
        IF OLD.pool_request_json IS NULL AND NEW.pool_request_json IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM nebius_application_builds b
            WHERE b.build_id=NEW.build_id AND b.current_attempt=NEW.attempt AND b.desired_state='running'
            AND NEW.pool_request_json->'build'=NEW.claim_json
            AND NEW.pool_request_json->>'schema_version'='loom.pool-application-image-prepare.v1'
            AND NEW.pool_request_json->'key'->>'local_work_id'=NEW.build_id::text
            AND NEW.pool_request_json->'key'->>'generation'=NEW.attempt::text
            AND NEW.pool_request_json->'key'->>'workload_kind'='application_image_build'
            AND NEW.pool_request_json->'key'->>'participant_id'=b.binding_json->>'participant_id'
            AND NEW.pool_request_json->>'pool_id'=b.binding_json->>'pool_id'
            AND NEW.pool_request_json->>'target_id'=b.binding_json->>'target_id'
            AND NEW.pool_request_json->>'admission_epoch'=b.binding_json->>'admission_epoch'
            AND NEW.pool_request_json->>'participant_revision'=b.binding_json->>'participant_revision'
            AND NEW.pool_request_json->'origin'->>'kind'='personal_build'
            AND NEW.pool_request_json->'origin'->>'submission_id'=NEW.build_id::text
            AND NEW.pool_request_json->'origin'->>'data_environment_id'=b.data_environment_id::text
            AND NEW.pool_request_json->'origin'->'application'='null'::jsonb) THEN
            RAISE EXCEPTION 'application pool request differs from retained build' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_application_build_attempt_retain BEFORE INSERT OR UPDATE OR DELETE
    ON nebius_application_build_attempts FOR EACH ROW EXECUTE FUNCTION retain_nebius_application_build_attempt();
""")


def downgrade() -> None:
    op.execute("""
LOCK TABLE nebius_application_source_uploads, nebius_application_builds, nebius_application_build_attempts, nebius_pool_machines
    IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nebius_application_source_uploads) OR
       EXISTS (SELECT 1 FROM nebius_application_builds) OR EXISTS (SELECT 1 FROM nebius_application_build_attempts) OR
       EXISTS (SELECT 1 FROM nebius_pool_machines WHERE workload_scope <> 'environment') THEN
        RAISE EXCEPTION 'cannot remove application source history';
    END IF;
END $$;
DROP TABLE nebius_application_build_attempts;
DROP TABLE nebius_application_builds;
DROP FUNCTION retain_nebius_application_build_attempt();
DROP FUNCTION retain_nebius_application_build();
DROP TABLE nebius_application_source_uploads;
DROP FUNCTION retain_nebius_application_source_upload();
ALTER TABLE nebius_pool_machines DROP CONSTRAINT nebius_pool_machine_workload_scope_check;
ALTER TABLE nebius_pool_machines DROP COLUMN workload_scope;
""")
