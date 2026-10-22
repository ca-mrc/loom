"""Qualify parent-bound verifier proposals through the existing global outbox.

Revision ID: 0179
Revises: 0178

No grant, workload or installed authority is created by this migration.
"""
from alembic import op

revision = "0179"
down_revision = "0178"
branch_labels = None
depends_on = None

# Freeze the predecessor guard here; published migration0172 remains immutable.
_PREDECESSOR = r"""
CREATE OR REPLACE FUNCTION validate_nebius_pool_execution_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
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
        IF (to_jsonb(OLD)-ARRAY['phase','reservation_id','receipt_json','cancelled_json','attached_lease_id','activation_json','activated_json','stop_json','drain_json','output_json','released_json']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['phase','reservation_id','receipt_json','cancelled_json','attached_lease_id','activation_json','activated_json','stop_json','drain_json','output_json','released_json']) OR
           (OLD.reservation_id IS NOT NULL AND ROW(OLD.reservation_id,OLD.receipt_json) IS DISTINCT FROM
                                               ROW(NEW.reservation_id,NEW.receipt_json)) OR
           (OLD.attached_lease_id IS NOT NULL AND OLD.attached_lease_id IS DISTINCT FROM NEW.attached_lease_id) OR
           (OLD.cancelled_json IS NOT NULL AND OLD.cancelled_json IS DISTINCT FROM NEW.cancelled_json) OR
           (OLD.activation_json IS NOT NULL AND OLD.activation_json IS DISTINCT FROM NEW.activation_json) OR
           (OLD.activated_json IS NOT NULL AND OLD.activated_json IS DISTINCT FROM NEW.activated_json) OR
           (OLD.stop_json IS NOT NULL AND OLD.stop_json IS DISTINCT FROM NEW.stop_json) OR
           (OLD.drain_json IS NOT NULL AND OLD.drain_json IS DISTINCT FROM NEW.drain_json) OR
           (OLD.output_json IS NOT NULL AND OLD.output_json IS DISTINCT FROM NEW.output_json) OR
           (OLD.released_json IS NOT NULL AND OLD.released_json IS DISTINCT FROM NEW.released_json) THEN
            RAISE EXCEPTION 'execution selection evidence is immutable' USING ERRCODE='23514';
        END IF;
        IF NOT (OLD.phase=NEW.phase OR (OLD.phase='selected' AND NEW.phase IN ('grant_pending','cancel_pending')) OR
            (OLD.phase='grant_pending' AND NEW.phase IN ('attached','cancel_pending')) OR
            (OLD.phase='attached' AND NEW.phase IN ('activation_pending','cancel_pending')) OR
            (OLD.phase='activation_pending' AND NEW.phase IN ('active','stop_pending','cancel_pending')) OR
            (OLD.phase='active' AND NEW.phase='stop_pending') OR
            (OLD.phase='stop_pending' AND NEW.phase='released') OR
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
            (l.desired_state IN ('cancel','retry','timeout','delete_pending') OR
             (NEW.phase='released' AND l.desired_state='deleted')) AND l.revoked_at IS NOT NULL AND
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
    IF NEW.released_json IS NOT NULL AND (
        NEW.released_json->>'reservation_id'=NEW.reservation_id::text AND
        NEW.released_json->>'pool_id'=NEW.pool_id::text AND
        NEW.released_json->>'request_sha256'=NEW.request_sha256 AND
        NEW.released_json->>'admission_epoch'=NEW.request_json->>'admission_epoch' AND
        NEW.released_json->'request_key'=NEW.request_json->'key' AND
        NEW.released_json->>'plan_sha256'=NEW.activated_json->>'plan_sha256' AND
        NEW.released_json->>'phase'='released' AND
        (NEW.released_json->>'cleanup_observation_id')::uuid <> '00000000-0000-0000-0000-000000000000'::uuid AND
        EXISTS (SELECT 1 FROM execution_leases l WHERE l.id=NEW.attached_lease_id AND
            l.desired_state='deleted' AND l.cleanup_state='complete' AND l.deleted_at IS NOT NULL AND
            l.job_uid IS NOT DISTINCT FROM NEW.released_json->>'job_uid')) IS NOT TRUE THEN
        RAISE EXCEPTION 'execution release identity differs' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
"""

_TRIAL_KIND = "NEW.request_json->'key'->>'workload_kind'='trial'"
_VERIFIER_KIND = """(NEW.request_json->'key'->>'workload_kind'='trial' OR
         (NEW.request_json->'key'->>'workload_kind'='verifier' AND
          NEW.request_json->'execution'->'runtime'->>'execution_role'='verifier' AND
          NEW.request_json->'execution'->>'parent_lease_id'=NEW.selection_json->'verifier'->>'parent_lease_id' AND
          NEW.selection_json->'verifier'->'handoff'->>'parent_lease_id'=NEW.selection_json->'verifier'->>'parent_lease_id' AND
          NEW.selection_json->'verifier'->'handoff'->>'state'='pending' AND
          NEW.selection_json->'trial'->>'attempt_count'=NEW.selection_json->'verifier'->>'attempt' AND
          EXISTS (SELECT 1 FROM nebius_pool_execution_outbox p JOIN execution_leases l ON l.id=p.attached_lease_id
            WHERE p.lease_id::text=NEW.selection_json->'verifier'->>'parent_lease_id'
              AND p.phase='released' AND p.released_json->>'phase'='released'
              AND p.trial_id=NEW.trial_id AND p.pool_id=NEW.pool_id AND p.participant_id=NEW.participant_id
              AND p.request_json->'key'->>'workload_kind'='trial'
              AND l.execution_role='attempt' AND l.output_commit_state='committed'
              AND l.attempt=(NEW.selection_json->'verifier'->>'attempt')::integer
              AND l.resource_generation=(NEW.selection_json->'verifier'->>'resource_generation')::integer
              AND l.cleanup_state='complete' AND l.deleted_at IS NOT NULL
              AND l.output_manifest_sha256=NEW.selection_json->'verifier'->>'manifest_sha256'
              AND l.output_marker_sha256=NEW.selection_json->'verifier'->>'marker_sha256'
              AND l.output_upload_session_id::text=NEW.selection_json->'verifier'->>'upload_session_id'
              AND l.runtime_contract_sha256=NEW.selection_json->'verifier'->>'runtime_contract_sha256'
              AND l.runtime_contract_json->>'verifier_execution'='separate_execution'
              AND l.target_id=NEW.request_json->>'target_id'
              AND l.workload_requirements_json=NEW.request_json->'execution'->'requirements')) )"""
_TRIAL_LEASE = """l.attempt=(NEW.selection_json->'trial'->>'attempt_count')::integer+1 AND
        l.resource_generation=1 AND l.execution_role='attempt' AND l.parent_lease_id IS NULL AND"""
_VERIFIER_LEASE = """l.resource_generation=1 AND (
          (NEW.request_json->'key'->>'workload_kind'='trial' AND
           l.attempt=(NEW.selection_json->'trial'->>'attempt_count')::integer+1 AND
           l.execution_role='attempt' AND l.parent_lease_id IS NULL AND l.verifier_retry=0) OR
          (NEW.request_json->'key'->>'workload_kind'='verifier' AND
           l.attempt=(NEW.selection_json->'verifier'->>'attempt')::integer AND
           l.execution_role='verifier' AND l.parent_lease_id::text=NEW.selection_json->'verifier'->>'parent_lease_id' AND
           l.verifier_retry=coalesce((NEW.selection_json->'verifier'->'handoff'->>'retries')::integer,0))
        ) AND"""


def upgrade() -> None:
    op.execute(_PREDECESSOR.replace(_TRIAL_KIND, _VERIFIER_KIND).replace(_TRIAL_LEASE, _VERIFIER_LEASE))


def downgrade() -> None:
    # Serialize the history check with new proposals before restoring a guard
    # that cannot understand verifier rows. Alembic keeps this in one transaction.
    op.execute("LOCK TABLE nebius_pool_execution_outbox IN ACCESS EXCLUSIVE MODE")
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM nebius_pool_execution_outbox WHERE request_json->'key'->>'workload_kind'='verifier') THEN
        RAISE EXCEPTION 'global verifier history requires revision0179';
      END IF;
    END $$;""")
    op.execute(_PREDECESSOR)
