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
CREATE TABLE nebius_pool_execution_outbox (
    lease_id UUID PRIMARY KEY, trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE RESTRICT,
    pool_id UUID NOT NULL, participant_id UUID NOT NULL, request_sha256 TEXT NOT NULL,
    request_json JSONB NOT NULL, selection_json JSONB NOT NULL, phase TEXT NOT NULL,
    reservation_id UUID, receipt_json JSONB, cancelled_json JSONB, activation_json JSONB, activated_json JSONB,
    stop_json JSONB, drain_json JSONB, output_json JSONB,
    attached_lease_id UUID REFERENCES execution_leases(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nebius_pool_execution_outbox_grant_key UNIQUE (reservation_id),
    CONSTRAINT nebius_pool_execution_outbox_identity_check CHECK (
        lease_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json)='object' AND
        jsonb_typeof(selection_json)='object'),
    CONSTRAINT nebius_pool_execution_outbox_phase_check CHECK (
        phase IN ('selected','grant_pending','attached','activation_pending','active','stop_pending','cancel_pending','cancelled') AND
        (phase <> 'selected' OR reservation_id IS NULL) AND
        (phase NOT IN ('grant_pending','attached','cancelled') OR reservation_id IS NOT NULL) AND
        (reservation_id IS NULL) = (receipt_json IS NULL) AND
        (reservation_id IS NULL OR reservation_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND
        (receipt_json IS NULL OR jsonb_typeof(receipt_json)='object') AND
        (attached_lease_id IS NULL OR attached_lease_id=lease_id) AND
        (phase NOT IN ('attached','activation_pending','active','stop_pending') OR attached_lease_id IS NOT NULL) AND
        (phase NOT IN ('selected','grant_pending') OR attached_lease_id IS NULL) AND
        (attached_lease_id IS NULL OR reservation_id IS NOT NULL) AND
        (activation_json IS NULL OR (jsonb_typeof(activation_json)='object' AND attached_lease_id IS NOT NULL AND
            phase NOT IN ('selected','grant_pending','attached'))) AND
        (phase NOT IN ('activation_pending','active','stop_pending') OR activation_json IS NOT NULL) AND
        (phase IN ('active','stop_pending')) = (activated_json IS NOT NULL) AND
        (activated_json IS NULL OR jsonb_typeof(activated_json)='object') AND
        (phase='cancelled') = (cancelled_json IS NOT NULL) AND
        (cancelled_json IS NULL OR jsonb_typeof(cancelled_json)='object') AND
        (stop_json IS NULL OR (phase='stop_pending' AND jsonb_typeof(stop_json)='object')) AND
        (drain_json IS NULL) = (output_json IS NULL) AND
        (drain_json IS NULL OR (stop_json IS NOT NULL AND jsonb_typeof(drain_json)='object' AND
            jsonb_typeof(output_json)='object')))
);
CREATE UNIQUE INDEX nebius_pool_execution_outbox_live_key ON nebius_pool_execution_outbox(trial_id)
    WHERE phase <> 'cancelled';
CREATE FUNCTION validate_nebius_pool_execution_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'execution selection history is retained' USING ERRCODE='23514';
    END IF;
    IF (NEW.request_json->>'pool_id'=NEW.pool_id::text AND
        NEW.request_json->'key'->>'participant_id'=NEW.participant_id::text AND
        NEW.request_json->'key'->>'local_work_id'=NEW.lease_id::text AND
        NEW.request_json->'key'->>'generation'='1' AND
        NEW.request_json->'key'->>'workload_kind'='trial' AND
        NEW.selection_json->'trial'->>'id'=NEW.trial_id::text) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution selection identity differs' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.phase <> 'selected' THEN
            RAISE EXCEPTION 'execution selection must start unclaimed' USING ERRCODE='23514';
        END IF;
    ELSE
        IF (to_jsonb(OLD)-ARRAY['phase','reservation_id','receipt_json','cancelled_json','attached_lease_id','activation_json','activated_json','stop_json','drain_json','output_json']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['phase','reservation_id','receipt_json','cancelled_json','attached_lease_id','activation_json','activated_json','stop_json','drain_json','output_json']) OR
           (OLD.reservation_id IS NOT NULL AND ROW(OLD.reservation_id,OLD.receipt_json) IS DISTINCT FROM
                                               ROW(NEW.reservation_id,NEW.receipt_json)) OR
           (OLD.attached_lease_id IS NOT NULL AND OLD.attached_lease_id IS DISTINCT FROM NEW.attached_lease_id) OR
           (OLD.cancelled_json IS NOT NULL AND OLD.cancelled_json IS DISTINCT FROM NEW.cancelled_json) OR
           (OLD.activation_json IS NOT NULL AND OLD.activation_json IS DISTINCT FROM NEW.activation_json) OR
           (OLD.activated_json IS NOT NULL AND OLD.activated_json IS DISTINCT FROM NEW.activated_json) OR
           (OLD.stop_json IS NOT NULL AND OLD.stop_json IS DISTINCT FROM NEW.stop_json) OR
           (OLD.drain_json IS NOT NULL AND OLD.drain_json IS DISTINCT FROM NEW.drain_json) OR
           (OLD.output_json IS NOT NULL AND OLD.output_json IS DISTINCT FROM NEW.output_json) THEN
            RAISE EXCEPTION 'execution selection evidence is immutable' USING ERRCODE='23514';
        END IF;
        IF NOT (OLD.phase=NEW.phase OR (OLD.phase='selected' AND NEW.phase IN ('grant_pending','cancel_pending')) OR
            (OLD.phase='grant_pending' AND NEW.phase IN ('attached','cancel_pending')) OR
            (OLD.phase='attached' AND NEW.phase IN ('activation_pending','cancel_pending')) OR
            (OLD.phase='activation_pending' AND NEW.phase IN ('active','stop_pending','cancel_pending')) OR
            (OLD.phase='active' AND NEW.phase='stop_pending') OR
            (OLD.phase='cancel_pending' AND NEW.phase IN ('cancelled','stop_pending'))) THEN
            RAISE EXCEPTION 'execution selection transition forbidden' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.reservation_id IS NOT NULL AND (
        NEW.receipt_json->>'reservation_id'=NEW.reservation_id::text AND
        NEW.receipt_json->>'pool_id'=NEW.pool_id::text AND
        NEW.receipt_json->>'request_sha256'=NEW.request_sha256 AND
        NEW.receipt_json->>'admission_epoch'=NEW.request_json->>'admission_epoch' AND
        NEW.receipt_json->'request_key'=NEW.request_json->'key' AND
        NEW.receipt_json->>'phase' IN ('reserved','cancelled_unstarted') AND
        (NEW.attached_lease_id IS NULL OR NEW.receipt_json->>'phase'='reserved')) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution grant identity differs' USING ERRCODE='23514';
    END IF;
    IF NEW.cancelled_json IS NOT NULL AND (
        NEW.cancelled_json->>'reservation_id'=NEW.reservation_id::text AND
        NEW.cancelled_json->>'pool_id'=NEW.pool_id::text AND
        NEW.cancelled_json->>'request_sha256'=NEW.request_sha256 AND
        NEW.cancelled_json->>'admission_epoch'=NEW.request_json->>'admission_epoch' AND
        NEW.cancelled_json->'request_key'=NEW.request_json->'key' AND
        NEW.cancelled_json->>'phase'='cancelled_unstarted') IS NOT TRUE THEN
        RAISE EXCEPTION 'execution cancellation identity differs' USING ERRCODE='23514';
    END IF;
    IF NEW.activation_json IS NOT NULL AND (
        NEW.activation_json->'action'->>'pool_id'=NEW.pool_id::text AND
        NEW.activation_json->'action'->>'admission_epoch'=NEW.request_json->>'admission_epoch' AND
        NEW.activation_json->'action'->>'request_sha256'=NEW.request_sha256 AND
        NEW.activation_json->'action'->'request_key'=NEW.request_json->'key' AND
        (NEW.activation_json->>'not_after')::timestamptz <= (NEW.request_json->>'deadline_at')::timestamptz) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution activation consent differs' USING ERRCODE='23514';
    END IF;
    IF NEW.activated_json IS NOT NULL AND (
        NEW.activated_json->>'reservation_id'=NEW.reservation_id::text AND
        NEW.activated_json->>'pool_id'=NEW.pool_id::text AND
        NEW.activated_json->>'request_sha256'=NEW.request_sha256 AND
        NEW.activated_json->>'admission_epoch'=NEW.request_json->>'admission_epoch' AND
        NEW.activated_json->'request_key'=NEW.request_json->'key' AND
        NEW.activated_json->>'phase' IN ('create_intent','observed','cleanup_intent','released') AND
        NEW.activated_json->>'plan_sha256' ~ '^[0-9a-f]{64}$') IS NOT TRUE THEN
        RAISE EXCEPTION 'execution activated identity differs' USING ERRCODE='23514';
    END IF;
    IF NEW.attached_lease_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM execution_leases l WHERE l.id=NEW.attached_lease_id AND l.trial_id=NEW.trial_id AND
        l.request_id=NEW.lease_id AND l.job_name='loom-pool-' || replace(NEW.reservation_id::text,'-','') AND
        l.execution_unit_key::text=NEW.request_json->'execution'->>'execution_unit_key' AND
        l.target_id=NEW.request_json->>'target_id' AND
        l.runtime_contract_sha256=NEW.selection_json->>'runtime_contract_sha256' AND
        l.attempt=(NEW.selection_json->'trial'->>'attempt_count')::integer+1 AND
        l.resource_generation=1 AND l.execution_role='attempt' AND l.parent_lease_id IS NULL AND
        l.deadline_at=(NEW.request_json->>'deadline_at')::timestamptz AND
        l.workload_requirements_json=NEW.request_json->'execution'->'requirements'
    ) THEN
        RAISE EXCEPTION 'execution attached lease differs' USING ERRCODE='23514';
    END IF;
    IF NEW.stop_json IS NOT NULL AND (
        NEW.stop_json->'action'=NEW.activation_json->'action' AND
        NEW.stop_json->>'reservation_id'=NEW.reservation_id::text AND
        NEW.stop_json->>'plan_sha256'=NEW.activated_json->>'plan_sha256' AND
        NEW.stop_json->>'lease_generation'=NEW.request_json->'execution'->>'lease_generation' AND
        NEW.stop_json->>'cause' IN ('completed','failed','cancelled','lease_lost','deadline') AND
        EXISTS (SELECT 1 FROM execution_leases l WHERE l.id=NEW.attached_lease_id AND
            l.desired_state IN ('cancel','retry','timeout','delete_pending') AND l.revoked_at IS NOT NULL AND
            (NEW.stop_json->>'grace_deadline_at')::timestamptz <= l.cleanup_deadline_at)) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution stop identity differs' USING ERRCODE='23514';
    END IF;
    IF NEW.drain_json IS NOT NULL AND (
        NEW.drain_json->'action'=NEW.stop_json->'action' AND
        NEW.drain_json->>'reservation_id'=NEW.stop_json->>'reservation_id' AND
        NEW.drain_json->>'plan_sha256'=NEW.stop_json->>'plan_sha256' AND
        NEW.drain_json->>'lease_generation'=NEW.stop_json->>'lease_generation' AND
        NEW.drain_json->>'stop_sha256' ~ '^[0-9a-f]{64}$' AND
        NEW.drain_json->>'evidence_sha256' ~ '^[0-9a-f]{64}$' AND
        NEW.output_json->>'lease_id'=NEW.lease_id::text AND
        NEW.drain_json->>'output_generation'=NEW.output_json->>'output_generation' AND
        NEW.drain_json->>'output_state'=NEW.output_json->>'output_state' AND
        EXISTS (SELECT 1 FROM execution_leases l WHERE l.id=NEW.attached_lease_id AND
            l.output_commit_state IN ('committed','unavailable') AND l.output_generation=l.resource_generation AND
            NEW.output_json->>'output_state'=l.output_commit_state AND
            NEW.output_json->>'output_generation'=l.output_generation::text AND
            (NEW.output_json->>'upload_session_id')::uuid IS NOT DISTINCT FROM l.output_upload_session_id AND
            NEW.output_json->>'manifest_sha256' IS NOT DISTINCT FROM l.output_manifest_sha256 AND
            NEW.output_json->>'marker_sha256' IS NOT DISTINCT FROM l.output_marker_sha256 AND
            (NEW.output_json->>'committed_at')::timestamptz IS NOT DISTINCT FROM l.output_committed_at AND
            NEW.output_json->>'unavailable_reason' IS NOT DISTINCT FROM l.output_unavailable_reason)) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution drain evidence differs' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_pool_execution_outbox_guard BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_execution_outbox
    FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_execution_outbox();
CREATE FUNCTION require_nebius_execution_attachment() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM nebius_pool_execution_outbox o WHERE o.lease_id=NEW.id AND
               (o.attached_lease_id IS DISTINCT FROM NEW.id OR o.phase='grant_pending')) THEN
        RAISE EXCEPTION 'global execution attachment is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER nebius_execution_attachment_guard AFTER INSERT ON execution_leases
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_nebius_execution_attachment();
    """)
    op.execute("""
        CREATE TABLE nebius_pool_submissions (
            id UUID PRIMARY KEY, team_id UUID NOT NULL REFERENCES teams(id),
            user_id UUID NOT NULL REFERENCES users(id), request_sha256 TEXT NOT NULL,
            pool_origin JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT nebius_pool_submissions_payload_check CHECK (
                id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(pool_origin) = 'object' AND
                (pool_origin->>'submission_id' = id::text AND
                 pool_origin->>'kind' IN ('environment','application')) IS TRUE)
        );
        CREATE FUNCTION retain_nebius_submission_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW IS DISTINCT FROM OLD THEN
                RAISE EXCEPTION 'submission handoff is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER nebius_pool_submissions_guard BEFORE UPDATE ON nebius_pool_submissions
            FOR EACH ROW EXECUTE FUNCTION retain_nebius_submission_handoff();
        ALTER TABLE batches ADD COLUMN pool_origin JSONB;
        ALTER TABLE batches ADD CONSTRAINT batches_pool_origin_check
            CHECK (pool_origin IS NULL OR jsonb_typeof(pool_origin) = 'object');
        ALTER TABLE trials ADD COLUMN pool_origin JSONB;
        CREATE FUNCTION retain_nebius_submission_origin() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.pool_origin IS DISTINCT FROM OLD.pool_origin THEN
                RAISE EXCEPTION 'submission origin is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER batches_pool_origin_guard BEFORE UPDATE OF pool_origin ON batches
            FOR EACH ROW EXECUTE FUNCTION retain_nebius_submission_origin();
        CREATE TRIGGER trials_pool_origin_guard BEFORE UPDATE OF pool_origin ON trials
            FOR EACH ROW EXECUTE FUNCTION retain_nebius_submission_origin();
    """)
    op.execute("""
CREATE TABLE nebius_pool_build_outbox (
    outbox_id UUID PRIMARY KEY, pool_id UUID NOT NULL, participant_id UUID NOT NULL,
    materialization_id UUID NOT NULL REFERENCES task_image_materializations(id) ON DELETE RESTRICT,
    generation BIGINT NOT NULL, admission_epoch BIGINT NOT NULL,
    builder_id TEXT NOT NULL, logical_pool_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL, request_json JSONB NOT NULL, selection_json JSONB NOT NULL,
    phase TEXT NOT NULL, reservation_id UUID, receipt_json JSONB, cancelled_json JSONB,
    activation_json JSONB, activated_json JSONB, released_json JSONB,
    attempt_id UUID, attempt_number INTEGER, lease_epoch BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nebius_pool_build_outbox_replay_key UNIQUE (participant_id, materialization_id, generation),
    CONSTRAINT nebius_pool_build_outbox_grant_key UNIQUE (reservation_id),
    CONSTRAINT nebius_pool_build_outbox_claim_fk
        FOREIGN KEY (attempt_id, materialization_id, attempt_number, lease_epoch, builder_id)
        REFERENCES task_image_materialization_attempts(id, materialization_id, attempt_number, lease_epoch, builder_id)
        ON DELETE RESTRICT,
    CONSTRAINT nebius_pool_build_outbox_identity_check CHECK (
        generation > 0 AND admission_epoch > 0 AND
        outbox_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        participant_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        pool_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        length(builder_id) BETWEEN 1 AND 128 AND length(logical_pool_id) BETWEEN 1 AND 80),
    CONSTRAINT nebius_pool_build_outbox_payload_check CHECK (
        request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object' AND
        jsonb_typeof(selection_json) = 'object'),
    CONSTRAINT nebius_pool_build_outbox_phase_check CHECK (
        phase IN ('selected','attached','activation_pending','active','cancel_pending','stop_pending','cancelled','released') AND
        (phase NOT IN ('attached','activation_pending','active','stop_pending','released') OR num_nonnulls(attempt_id, attempt_number, lease_epoch) = 3) AND
        (phase <> 'selected' OR attempt_id IS NULL) AND
        (attempt_id IS NULL OR reservation_id IS NOT NULL) AND
        num_nonnulls(attempt_id, attempt_number, lease_epoch) IN (0,3) AND
        (phase NOT IN ('attached','cancelled') OR reservation_id IS NOT NULL) AND
        (phase <> 'selected' OR reservation_id IS NULL) AND
        (reservation_id IS NULL) = (receipt_json IS NULL) AND
        (reservation_id IS NULL OR reservation_id <> '00000000-0000-0000-0000-000000000000'::uuid) AND
        (receipt_json IS NULL OR jsonb_typeof(receipt_json) = 'object') AND
        (phase = 'cancelled') = (cancelled_json IS NOT NULL) AND
        (cancelled_json IS NULL OR jsonb_typeof(cancelled_json) = 'object') AND
        (activation_json IS NULL OR (jsonb_typeof(activation_json) = 'object' AND attempt_id IS NOT NULL AND
            phase NOT IN ('selected','attached'))) AND
        (phase NOT IN ('activation_pending','active','stop_pending','released') OR activation_json IS NOT NULL) AND
        (phase IN ('active','stop_pending','released')) = (activated_json IS NOT NULL) AND
        (activated_json IS NULL OR jsonb_typeof(activated_json) = 'object') AND
        (phase = 'released') = (released_json IS NOT NULL) AND
        (released_json IS NULL OR jsonb_typeof(released_json) = 'object'))
);
CREATE UNIQUE INDEX nebius_pool_build_outbox_live_key ON nebius_pool_build_outbox(materialization_id)
    WHERE phase NOT IN ('cancelled','released');
CREATE FUNCTION validate_nebius_pool_build_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'pool local selection history is retained' USING ERRCODE = '23514';
    END IF;
    IF (NEW.request_json->>'pool_id' = NEW.pool_id::text AND
        NEW.request_json->'key'->>'participant_id' = NEW.participant_id::text AND
        NEW.request_json->'key'->>'local_work_id' = NEW.materialization_id::text AND
        NEW.request_json->'key'->>'workload_kind' = 'task_image_build' AND
        NEW.request_json->'key'->>'generation' = NEW.generation::text AND
        NEW.request_json->>'admission_epoch' = NEW.admission_epoch::text) IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local selection identity differs' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.phase <> 'selected' THEN
            RAISE EXCEPTION 'pool local selection must begin unclaimed' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF (to_jsonb(OLD) - ARRAY['phase','reservation_id','receipt_json','cancelled_json','attempt_id','attempt_number','lease_epoch','activation_json','activated_json','released_json'])
           IS DISTINCT FROM
           (to_jsonb(NEW) - ARRAY['phase','reservation_id','receipt_json','cancelled_json','attempt_id','attempt_number','lease_epoch','activation_json','activated_json','released_json'])
           OR (OLD.reservation_id IS NOT NULL AND (NEW.reservation_id IS DISTINCT FROM OLD.reservation_id OR
                                                    NEW.receipt_json IS DISTINCT FROM OLD.receipt_json))
           OR (OLD.attempt_id IS NOT NULL AND ROW(NEW.attempt_id, NEW.attempt_number, NEW.lease_epoch)
                    IS DISTINCT FROM ROW(OLD.attempt_id, OLD.attempt_number, OLD.lease_epoch))
           OR (OLD.cancelled_json IS NOT NULL AND NEW.cancelled_json IS DISTINCT FROM OLD.cancelled_json)
           OR (OLD.activation_json IS NOT NULL AND NEW.activation_json IS DISTINCT FROM OLD.activation_json)
           OR (OLD.activated_json IS NOT NULL AND NEW.activated_json IS DISTINCT FROM OLD.activated_json)
           OR (OLD.released_json IS NOT NULL AND NEW.released_json IS DISTINCT FROM OLD.released_json) THEN
            RAISE EXCEPTION 'pool local selection evidence is immutable' USING ERRCODE = '23514';
        END IF;
        IF NOT (NEW.phase = OLD.phase OR (OLD.phase = 'selected' AND NEW.phase IN ('attached','cancel_pending'))
                OR (OLD.phase = 'attached' AND NEW.phase IN ('activation_pending','cancel_pending'))
                OR (OLD.phase = 'activation_pending' AND NEW.phase IN ('active','cancel_pending','stop_pending'))
                OR (OLD.phase = 'active' AND NEW.phase = 'stop_pending')
                OR (OLD.phase = 'stop_pending' AND NEW.phase = 'released')
                OR (OLD.phase = 'cancel_pending' AND NEW.phase IN ('cancelled','stop_pending'))) THEN
            RAISE EXCEPTION 'pool local handoff transition forbidden' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NEW.reservation_id IS NOT NULL AND (
        NEW.receipt_json->>'reservation_id' = NEW.reservation_id::text AND
        NEW.receipt_json->>'pool_id' = NEW.pool_id::text AND
        NEW.receipt_json->>'admission_epoch' = NEW.admission_epoch::text AND
        NEW.receipt_json->>'request_sha256' = NEW.request_sha256 AND
        NEW.receipt_json->'request_key' = NEW.request_json->'key' AND
        NEW.receipt_json->>'phase' IN ('reserved','cancelled_unstarted') AND
        (NEW.attempt_id IS NULL OR NEW.receipt_json->>'phase' = 'reserved')) IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local grant identity differs' USING ERRCODE = '23514';
    END IF;
    IF NEW.attempt_id IS NOT NULL AND NEW.lease_epoch <> (NEW.request_json->'build'->>'expected_lease_epoch')::bigint + 1 THEN
        RAISE EXCEPTION 'pool local claim epoch differs' USING ERRCODE = '23514';
    END IF;
    IF NEW.cancelled_json IS NOT NULL AND (
        NEW.cancelled_json->>'phase' = 'cancelled_unstarted' AND
        NEW.cancelled_json->>'reservation_id' = NEW.reservation_id::text AND
        NEW.cancelled_json->>'pool_id' = NEW.pool_id::text AND
        NEW.cancelled_json->>'admission_epoch' = NEW.admission_epoch::text AND
        NEW.cancelled_json->>'request_sha256' = NEW.request_sha256 AND
        NEW.cancelled_json->'request_key' = NEW.request_json->'key') IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local cancellation identity differs' USING ERRCODE = '23514';
    END IF;
    IF NEW.activation_json IS NOT NULL AND (
        NEW.activation_json->'action'->>'pool_id' = NEW.pool_id::text AND
        NEW.activation_json->'action'->>'admission_epoch' = NEW.admission_epoch::text AND
        NEW.activation_json->'action'->>'request_sha256' = NEW.request_sha256 AND
        NEW.activation_json->'action'->'request_key' = NEW.request_json->'key' AND
        (NEW.activation_json->>'not_after')::timestamptz <= (NEW.request_json->>'deadline_at')::timestamptz) IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local activation identity differs' USING ERRCODE = '23514';
    END IF;
    IF NEW.activated_json IS NOT NULL AND (
        NEW.activated_json->>'phase' IN ('create_intent','observed','cleanup_intent','released') AND
        NEW.activated_json->>'plan_sha256' ~ '^[0-9a-f]{64}$' AND
        NEW.activated_json->>'reservation_id' = NEW.reservation_id::text AND
        NEW.activated_json->>'pool_id' = NEW.pool_id::text AND
        NEW.activated_json->>'admission_epoch' = NEW.admission_epoch::text AND
        NEW.activated_json->>'request_sha256' = NEW.request_sha256 AND
        NEW.activated_json->'request_key' = NEW.request_json->'key') IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local activated receipt differs' USING ERRCODE = '23514';
    END IF;
    IF NEW.released_json IS NOT NULL AND (
        NEW.released_json->>'phase' = 'released' AND
        (NEW.released_json->>'cleanup_observation_id')::uuid <> '00000000-0000-0000-0000-000000000000'::uuid AND
        NEW.released_json->>'reservation_id' = NEW.reservation_id::text AND
        NEW.released_json->>'pool_id' = NEW.pool_id::text AND
        NEW.released_json->>'admission_epoch' = NEW.admission_epoch::text AND
        NEW.released_json->>'request_sha256' = NEW.request_sha256 AND
        NEW.released_json->'request_key' = NEW.request_json->'key' AND
        NEW.released_json->>'plan_sha256' = NEW.activated_json->>'plan_sha256' AND
        (NEW.activated_json->>'job_uid' IS NULL OR
            NEW.released_json->>'job_uid' = NEW.activated_json->>'job_uid')) IS NOT TRUE THEN
        RAISE EXCEPTION 'pool local release identity differs' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_pool_build_outbox_guard BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_build_outbox
    FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_build_outbox();
    """)
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
	stop_json JSONB,
	drain_json JSONB,
	job_uid UUID,
	cleanup_observation_id UUID,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    renewed_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    granted_at TIMESTAMPTZ,
    priority BIGINT NOT NULL,
	PRIMARY KEY (request_id),
	CONSTRAINT nebius_pool_request_participant_fk FOREIGN KEY(participant_id, pool_id) REFERENCES nebius_pool_participants (participant_id, pool_id) ON DELETE RESTRICT,
	CONSTRAINT nebius_pool_request_replay_key UNIQUE (participant_id, workload_kind, local_work_id, generation),
	CONSTRAINT nebius_pool_request_plan_key UNIQUE (request_id, plan_sha256, namespace_uid),
	CONSTRAINT nebius_pool_request_identity_check CHECK (generation > 0 AND admission_epoch > 0 AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid AND namespace_uid <> '00000000-0000-0000-0000-000000000000'::uuid AND local_work_id <> '00000000-0000-0000-0000-000000000000'::uuid AND workload_kind IN ('trial','verifier','task_image_build','application_image_build') AND target_id ~ '^[a-z0-9][a-z0-9-]{0,79}$'),
	CONSTRAINT nebius_pool_request_envelope_check CHECK (cpu_millis > 0 AND memory_mib > 0 AND ephemeral_storage_mib >= 0 AND pod_slots > 0),
    CONSTRAINT nebius_pool_request_admission_check CHECK (priority BETWEEN 0 AND 3 AND renewed_at >= created_at AND
        (granted_at IS NULL OR granted_at >= created_at) AND (phase <> 'waiting' OR granted_at IS NULL) AND
        (phase IN ('waiting','cancelled_unstarted') OR granted_at IS NOT NULL)),
	CONSTRAINT nebius_pool_request_payload_check CHECK (request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json) = 'object'),
    CONSTRAINT nebius_pool_request_lifecycle_check CHECK (
        (stop_json IS NULL OR (jsonb_typeof(stop_json) = 'object' AND phase IN ('cleanup_intent','released'))) AND
        (drain_json IS NULL OR (stop_json IS NOT NULL AND jsonb_typeof(drain_json) = 'object'))),
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

CREATE TABLE nebius_pool_captures (
    capture_id UUID PRIMARY KEY,
    pool_id UUID NOT NULL REFERENCES nebius_pool_bindings(pool_id) ON DELETE RESTRICT,
    admission_epoch BIGINT NOT NULL,
    registration_sha256 TEXT NOT NULL,
    scope_sha256 TEXT NOT NULL,
    scope_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT nebius_pool_capture_pool_key UNIQUE(capture_id, pool_id),
    CONSTRAINT nebius_pool_capture_shape_check CHECK (
        capture_id <> '00000000-0000-0000-0000-000000000000'::uuid AND admission_epoch > 0 AND
        registration_sha256 ~ '^[0-9a-f]{64}$' AND scope_sha256 ~ '^[0-9a-f]{64}$' AND
        jsonb_typeof(scope_json) = 'object')
);

CREATE TABLE nebius_pool_effects (
    effect_id UUID PRIMARY KEY,
    request_id UUID NOT NULL,
    plan_sha256 TEXT NOT NULL,
    namespace_uid UUID NOT NULL,
    effect_key TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    intent_json JSONB NOT NULL,
    phase TEXT NOT NULL,
    dispatch_id UUID,
    dispatch_machine_id UUID REFERENCES nebius_pool_machines(machine_id) ON DELETE RESTRICT,
    dispatch_epoch BIGINT,
    observed_uid UUID,
    observed_resource_version TEXT,
    rejection_status SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nebius_pool_effect_plan_fk FOREIGN KEY(request_id, plan_sha256, namespace_uid)
        REFERENCES nebius_pool_requests(request_id, plan_sha256, namespace_uid) ON DELETE RESTRICT,
    CONSTRAINT nebius_pool_effect_replay_key UNIQUE(request_id, effect_key),
    CONSTRAINT nebius_pool_effect_sequence_key UNIQUE(request_id, sequence),
    CONSTRAINT nebius_pool_effect_identity_check CHECK (
        effect_id <> '00000000-0000-0000-0000-000000000000'::uuid AND sequence > 0 AND
        effect_key ~ '^[a-zA-Z0-9._:-]{1,128}$'),
    CONSTRAINT nebius_pool_effect_shape_check CHECK (
        phase IN ('prepared','dispatched','observed','rejected') AND jsonb_typeof(intent_json) = 'object' AND
        ((intent_json->>'kind' IN ('Job','ConfigMap','Pod')) AND
         (intent_json->>'action' IN ('create','delete')) AND
         (intent_json->>'kind' <> 'Pod' OR intent_json->>'action' = 'delete')) IS TRUE),
    CONSTRAINT nebius_pool_effect_dispatch_check CHECK (
        (phase = 'prepared') = (dispatch_id IS NULL) AND
        (dispatch_id IS NULL) = (dispatch_machine_id IS NULL) AND
        (dispatch_id IS NULL) = (dispatch_epoch IS NULL) AND
        (dispatch_epoch IS NULL OR dispatch_epoch > 0) AND
        (dispatch_id IS NULL OR dispatch_id <> '00000000-0000-0000-0000-000000000000'::uuid)),
    CONSTRAINT nebius_pool_effect_observation_check CHECK (
        (phase = 'observed') = (observed_uid IS NOT NULL) AND
        (observed_uid IS NULL OR observed_uid <> '00000000-0000-0000-0000-000000000000'::uuid) AND
        (observed_resource_version IS NULL OR length(observed_resource_version) BETWEEN 1 AND 253) AND
        (observed_resource_version IS NOT NULL) = (phase = 'observed' AND intent_json->>'action' = 'create')),
    CONSTRAINT nebius_pool_effect_rejection_check CHECK (
        (phase = 'rejected') = (rejection_status IS NOT NULL) AND
        (rejection_status IS NULL OR rejection_status IN (409,422)))
);
CREATE TABLE nebius_pool_observations (
    observation_id UUID PRIMARY KEY,
    pool_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    observation_sha256 TEXT NOT NULL,
    observation_json JSONB NOT NULL,
    CONSTRAINT nebius_pool_observation_capture_fk FOREIGN KEY(capture_id, pool_id)
        REFERENCES nebius_pool_captures(capture_id, pool_id) ON DELETE RESTRICT,
    CONSTRAINT nebius_pool_observation_replay_key UNIQUE(capture_id),
    CONSTRAINT nebius_pool_observation_shape_check CHECK (
        observation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        observation_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(observation_json) = 'object')
);

    """)
    op.execute("""
        CREATE FUNCTION validate_nebius_pool_effect_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'global pool effect history is retained' USING ERRCODE = '23514';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.phase <> 'prepared' OR NOT EXISTS (
                    SELECT 1 FROM nebius_pool_requests WHERE request_id = NEW.request_id AND
                        ((phase = 'create_intent' AND NEW.intent_json->>'action' = 'create') OR
                         (phase = 'cleanup_intent' AND NEW.intent_json->>'action' = 'delete'))
                ) THEN
                    RAISE EXCEPTION 'global pool effect requires current fixed intent' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            IF to_jsonb(NEW) = to_jsonb(OLD) THEN
                RETURN NEW;
            END IF;
            IF (to_jsonb(NEW) - ARRAY['phase','dispatch_id','dispatch_machine_id','dispatch_epoch',
                    'observed_uid','observed_resource_version','rejection_status']::text[])
               IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['phase','dispatch_id','dispatch_machine_id','dispatch_epoch',
                    'observed_uid','observed_resource_version','rejection_status']::text[]) THEN
                RAISE EXCEPTION 'global pool effect intent is immutable' USING ERRCODE = '23514';
            END IF;
            IF OLD.phase = 'prepared' AND NEW.phase = 'dispatched' THEN
                IF NOT EXISTS (
                    SELECT 1 FROM nebius_pool_machines m JOIN nebius_pool_requests r ON r.pool_id = m.pool_id
                    JOIN nebius_pool_bindings p ON p.pool_id = r.pool_id
                    JOIN nebius_pool_participants e ON e.participant_id = r.participant_id
                    WHERE r.request_id = NEW.request_id AND m.machine_id = NEW.dispatch_machine_id
                      AND m.role = 'gateway' AND m.phase = 'active' AND m.credential_epoch = NEW.dispatch_epoch
                      AND p.mode IN ('closed','global') AND (
                        (NEW.intent_json->>'action' = 'create' AND r.phase = 'create_intent'
                         AND p.mode = 'global' AND r.admission_epoch = p.admission_epoch
                         AND e.phase = 'active' AND e.admission_epoch = p.admission_epoch
                         AND r.deadline_at > clock_timestamp()) OR
                        (NEW.intent_json->>'action' = 'delete' AND r.phase = 'cleanup_intent'))
                ) THEN
                    RAISE EXCEPTION 'global pool effect dispatcher is not qualified' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.phase = 'dispatched' AND NEW.phase IN ('observed','rejected') AND
               NEW.dispatch_id = OLD.dispatch_id AND NEW.dispatch_machine_id = OLD.dispatch_machine_id AND
               NEW.dispatch_epoch = OLD.dispatch_epoch THEN
                IF NEW.phase = 'observed' AND NEW.intent_json->>'action' = 'delete' AND
                   (NEW.intent_json->>'uid' IS NULL OR NEW.observed_uid::text <> NEW.intent_json->>'uid') THEN
                    RAISE EXCEPTION 'global pool delete observation identity differs' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'global pool effect history is immutable' USING ERRCODE = '23514';
        END;
        $$;
        CREATE TRIGGER nebius_pool_effect_mutation_guard
        BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_effects
        FOR EACH ROW EXECUTE FUNCTION validate_nebius_pool_effect_mutation();

        CREATE FUNCTION retain_nebius_pool_capture_evidence() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'global pool capture evidence is immutable' USING ERRCODE = '23514';
        END;
        $$;
        CREATE TRIGGER nebius_pool_capture_retention_guard
        BEFORE UPDATE OR DELETE ON nebius_pool_captures
        FOR EACH ROW EXECUTE FUNCTION retain_nebius_pool_capture_evidence();
        CREATE TRIGGER nebius_pool_observation_retention_guard
        BEFORE UPDATE OR DELETE ON nebius_pool_observations
        FOR EACH ROW EXECUTE FUNCTION retain_nebius_pool_capture_evidence();

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

        CREATE TABLE nebius_pool_cancellations (
            cancellation_id UUID PRIMARY KEY, pool_id UUID NOT NULL, participant_id UUID NOT NULL,
            workload_kind TEXT NOT NULL, local_work_id UUID NOT NULL, generation BIGINT NOT NULL,
            admission_epoch BIGINT NOT NULL, request_sha256 TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT nebius_pool_cancellation_participant_fk FOREIGN KEY (participant_id, pool_id)
                REFERENCES nebius_pool_participants (participant_id, pool_id) ON DELETE RESTRICT,
            CONSTRAINT nebius_pool_cancellation_replay_key UNIQUE (participant_id, workload_kind, local_work_id, generation),
            CONSTRAINT nebius_pool_cancellation_identity_check CHECK (
                generation > 0 AND admission_epoch > 0 AND
                cancellation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                local_work_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                workload_kind IN ('trial','verifier','task_image_build','application_image_build') AND
                request_sha256 ~ '^[0-9a-f]{64}$')
        );

        CREATE FUNCTION retain_nebius_pool_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'pool cancellation history is retained' USING ERRCODE = '23514';
            END IF;
            IF current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'pool mutation requires read committed';
            END IF;
            PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation', 1915));
            IF EXISTS (SELECT 1 FROM nebius_pool_requests WHERE participant_id = NEW.participant_id
                AND workload_kind = NEW.workload_kind AND local_work_id = NEW.local_work_id AND generation = NEW.generation) THEN
                RAISE EXCEPTION 'pool request identity already retained' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER nebius_pool_cancellation_guard BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_cancellations
            FOR EACH ROW EXECUTE FUNCTION retain_nebius_pool_cancellation();

        CREATE FUNCTION validate_nebius_pool_request_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'global pool request history is retained';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF current_setting('transaction_isolation') <> 'read committed' THEN
                    RAISE EXCEPTION 'pool mutation requires read committed';
                END IF;
                PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation', 1915));
                IF EXISTS (SELECT 1 FROM nebius_pool_cancellations WHERE participant_id = NEW.participant_id
                    AND workload_kind = NEW.workload_kind AND local_work_id = NEW.local_work_id AND generation = NEW.generation) THEN
                    RAISE EXCEPTION 'pool request identity already retained' USING ERRCODE = '23514';
                END IF;
                IF NEW.phase NOT IN ('waiting','reserved') THEN
                    RAISE EXCEPTION 'global pool request must start without external effects';
                END IF;
                RETURN NEW;
            END IF;
            IF (to_jsonb(NEW) - ARRAY['phase','plan_sha256','plan_json','job_uid','cleanup_observation_id','renewed_at','granted_at','stop_json','drain_json']::text[])
               IS DISTINCT FROM
               (to_jsonb(OLD) - ARRAY['phase','plan_sha256','plan_json','job_uid','cleanup_observation_id','renewed_at','granted_at','stop_json','drain_json']::text[]) THEN
                RAISE EXCEPTION 'global pool request identity is immutable';
            END IF;
            IF NEW.renewed_at < OLD.renewed_at OR
               (OLD.phase <> 'waiting' AND NEW.renewed_at IS DISTINCT FROM OLD.renewed_at) OR
               (NEW.granted_at IS DISTINCT FROM OLD.granted_at AND NOT
                 (OLD.phase = 'waiting' AND NEW.phase = 'reserved' AND OLD.granted_at IS NULL)) THEN
                RAISE EXCEPTION 'global pool grant and renewal history is immutable';
            END IF;
            IF (OLD.plan_sha256 IS NOT NULL AND
                (NEW.plan_sha256 IS DISTINCT FROM OLD.plan_sha256 OR NEW.plan_json IS DISTINCT FROM OLD.plan_json))
               OR (OLD.job_uid IS NOT NULL AND NEW.job_uid IS DISTINCT FROM OLD.job_uid)
               OR (OLD.stop_json IS NOT NULL AND NEW.stop_json IS DISTINCT FROM OLD.stop_json)
               OR (OLD.drain_json IS NOT NULL AND NEW.drain_json IS DISTINCT FROM OLD.drain_json)
               OR (OLD.cleanup_observation_id IS NOT NULL AND
                   NEW.cleanup_observation_id IS DISTINCT FROM OLD.cleanup_observation_id) THEN
                RAISE EXCEPTION 'global pool request evidence is immutable';
            END IF;
            IF NEW.stop_json IS NOT NULL AND (
                NEW.stop_json->'request'->>'reservation_id' = NEW.request_id::text AND
                NEW.stop_json->'request'->>'plan_sha256' = NEW.plan_sha256 AND
                NEW.stop_json->'request'->'action'->>'pool_id' = NEW.pool_id::text AND
                NEW.stop_json->'request'->'action'->>'admission_epoch' = NEW.admission_epoch::text AND
                NEW.stop_json->'request'->'action'->>'request_sha256' = NEW.request_sha256 AND
                NEW.stop_json->'request'->'action'->'request_key' = NEW.request_json->'key' AND
                NEW.stop_json->>'request_sha256' ~ '^[0-9a-f]{64}$' AND
                jsonb_typeof(NEW.stop_json->'grace_seconds') = 'number' AND
                (NEW.stop_json->>'grace_seconds') ~ '^[0-9]+$' AND
                (NEW.stop_json->>'grace_seconds')::bigint BETWEEN 0 AND 300) IS NOT TRUE THEN
                RAISE EXCEPTION 'global pool stop identity differs' USING ERRCODE = '23514';
            END IF;
            IF NEW.drain_json IS NOT NULL AND (
                NEW.drain_json->>'reservation_id' = NEW.request_id::text AND
                NEW.drain_json->>'plan_sha256' = NEW.plan_sha256 AND
                NEW.drain_json->'action' = NEW.stop_json->'request'->'action' AND
                NEW.drain_json->>'lease_generation' = NEW.stop_json->'request'->>'lease_generation' AND
                NEW.drain_json->>'stop_sha256' = NEW.stop_json->>'request_sha256' AND
                NEW.drain_json->>'output_state' IN ('committed','unavailable') AND
                (NEW.workload_kind <> 'task_image_build' OR
                    NEW.drain_json->>'output_generation' = NEW.drain_json->>'lease_generation') AND
                NEW.drain_json->>'evidence_sha256' ~ '^[0-9a-f]{64}$' AND
                (NEW.drain_json->>'output_generation')::bigint > 0 AND OLD.stop_json IS NOT NULL) IS NOT TRUE THEN
                RAISE EXCEPTION 'global pool drain identity differs' USING ERRCODE = '23514';
            END IF;
            IF to_jsonb(NEW) = to_jsonb(OLD) THEN
                RETURN NEW;
            END IF;
            IF OLD.phase = 'waiting' AND NEW.phase = 'waiting' THEN
                RETURN NEW;
            END IF;
            IF OLD.phase = 'cleanup_intent' AND NEW.phase = 'cleanup_intent'
               AND ((OLD.job_uid IS NULL AND NEW.job_uid IS NOT NULL)
                    OR (OLD.stop_json IS NULL AND NEW.stop_json IS NOT NULL)
                    OR (OLD.drain_json IS NULL AND NEW.drain_json IS NOT NULL)) THEN
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
                   nebius_pool_machine_credentials, nebius_pool_captures,
                   nebius_pool_observations, nebius_pool_effects, nebius_pool_build_outbox,
                   nebius_pool_submissions, nebius_pool_cancellations, nebius_pool_execution_outbox,
                   batches, trials IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_pool_bindings)
               OR EXISTS (SELECT 1 FROM nebius_pool_participants)
               OR EXISTS (SELECT 1 FROM nebius_pool_requests)
               OR EXISTS (SELECT 1 FROM nebius_pool_cleanup_observations)
               OR EXISTS (SELECT 1 FROM nebius_pool_build_outbox)
               OR EXISTS (SELECT 1 FROM nebius_pool_execution_outbox)
               OR EXISTS (SELECT 1 FROM nebius_pool_submissions)
               OR EXISTS (SELECT 1 FROM nebius_pool_cancellations)
               OR EXISTS (SELECT 1 FROM batches WHERE pool_origin IS NOT NULL)
               OR EXISTS (SELECT 1 FROM trials WHERE pool_origin IS NOT NULL) THEN
                RAISE EXCEPTION 'cannot remove global pool history';
            END IF;
        END $$;
        ALTER TABLE nebius_pool_requests DROP CONSTRAINT nebius_pool_request_cleanup_fk;
        DROP TABLE nebius_pool_observations;
        DROP TABLE nebius_pool_captures;
        DROP TABLE nebius_pool_cleanup_observations;
        DROP TABLE nebius_pool_effects;
        DROP TABLE nebius_pool_requests;
        DROP TABLE nebius_pool_cancellations;
        DROP TABLE nebius_pool_machine_credentials;
        DROP TABLE nebius_pool_machines;
        DROP TABLE nebius_pool_participants;
        DROP TABLE nebius_pool_bindings;
        DROP FUNCTION retain_nebius_pool_cleanup_observation();
        DROP FUNCTION validate_nebius_pool_request_mutation();
        DROP FUNCTION retain_nebius_pool_cancellation();
        DROP FUNCTION validate_nebius_pool_registration_mutation();
        DROP FUNCTION validate_nebius_pool_machine_mutation();
        DROP FUNCTION retain_nebius_pool_capture_evidence();
        DROP FUNCTION validate_nebius_pool_effect_mutation();
        DROP TABLE nebius_pool_build_outbox;
        DROP FUNCTION validate_nebius_pool_build_outbox();
        DROP TABLE nebius_pool_execution_outbox;
        DROP FUNCTION validate_nebius_pool_execution_outbox();
        DROP TRIGGER nebius_execution_attachment_guard ON execution_leases;
        DROP FUNCTION require_nebius_execution_attachment();
        DROP TABLE nebius_pool_submissions;
        DROP FUNCTION retain_nebius_submission_handoff();
        DROP TRIGGER batches_pool_origin_guard ON batches;
        DROP TRIGGER trials_pool_origin_guard ON trials;
        DROP FUNCTION retain_nebius_submission_origin();
        ALTER TABLE batches DROP COLUMN pool_origin;
        ALTER TABLE trials DROP COLUMN pool_origin;
    """)
