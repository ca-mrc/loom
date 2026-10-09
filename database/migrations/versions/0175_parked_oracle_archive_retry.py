"""Allow one separately audited retry of a failed bounded Oracle archive recovery.

Revision ID: 0175
Revises: 0174
"""
import sqlalchemy as sa
from alembic import op

revision = "0175"
down_revision = "0174"
branch_labels = None
depends_on = None

_ANCHOR = "AND NOT COALESCE((\n            OLD.materialization_state = 'unavailable'"
_AUTHORIZATION = """(
            OLD.materialization_state = 'unavailable'
            AND OLD.materialization_error_code = 'recovery_incomplete'
            AND OLD.materialization_recovery_requested_at IS NULL
            AND NEW.materialization_recovery_requested_at IS NOT NULL
            AND NEW.materialization_state = 'pending'
            AND NEW.materialization_next_attempt_at = NEW.materialization_recovery_requested_at
            AND OLD.materialization_claim_id IS NULL AND OLD.materialization_claim_expires_at IS NULL
            AND OLD.materialization_next_attempt_at IS NULL
            AND OLD.canonical_trajectory_sha256 IS NULL AND OLD.canonical_atif_sha256 IS NULL
            AND OLD.output_commit_state = 'committed' AND OLD.output_generation = OLD.resource_generation
            AND OLD.execution_role = 'attempt' AND OLD.parent_lease_id IS NULL AND OLD.attempt = 1
            AND OLD.finalized_at IS NOT NULL AND OLD.revoked_at IS NOT NULL
            AND OLD.desired_state = 'deleted' AND OLD.observed_state = 'deleted'
            AND OLD.deleted_at IS NOT NULL AND OLD.cleanup_state = 'complete'
            AND OLD.source_cleanup_state = 'not_ready' AND OLD.source_retain_until IS NULL
            AND (to_jsonb(NEW) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
                = (to_jsonb(OLD) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
            AND NOT EXISTS (SELECT 1 FROM execution_leases c WHERE c.parent_lease_id = OLD.id)
            AND NOT EXISTS (SELECT 1 FROM llm_calls c WHERE c.trial_id = OLD.trial_id AND c.team_id = OLD.team_id)
            AND EXISTS (
                SELECT 1 FROM trials t JOIN artifacts a ON a.trial_id = t.id AND a.team_id = t.team_id
                JOIN data_lifecycle_authorities d ON d.id = t.lifecycle_authority_id
                CROSS JOIN LATERAL (SELECT a.metadata->'pending_archive_recovery' AS original,
                    a.metadata->'pending_archive_retry' AS retry) evidence
                WHERE t.id = OLD.trial_id AND t.team_id = OLD.team_id
                AND t.attempt_count = 1 AND t.state = 'materializing'
                AND t.config->>'agent_name' = 'oracle'
                AND COALESCE(t.config->'agent_model', 'null'::jsonb) = 'null'::jsonb
                AND t.result->'runtime_result'->>'status' = 'succeeded'
                AND COALESCE(t.result->'verifier_execution', 'null'::jsonb) = 'null'::jsonb
                AND COALESCE(t.result->>'cancelled', 'false') = 'false'
                AND a.control_producer_kind = 'service_execution' AND a.control_producer_id = OLD.id
                AND d.team_id = t.team_id AND d.owner_kind = 'trial' AND d.owner_id = t.id::text
                AND d.data_class = 'trial' AND d.state = 'active'
                AND d.namespace = evidence.original->'request'->>'namespace'
                AND evidence.original->>'request_sha256' = evidence.retry->'request'->>'previous_request_sha256'
                AND evidence.original->>'claim_id' = evidence.original->'failure'->>'claim_id'
                AND evidence.original->>'claim_id' = evidence.retry->'request'->>'previous_claim_id'
                AND evidence.original->'failure'->>'code' = 'recovery_incomplete'
                AND evidence.original->'request'->>'team_id' = OLD.team_id::text
                AND evidence.original->'request'->>'trial_id' = OLD.trial_id::text
                AND evidence.original->'request'->>'lease_id' = OLD.id::text
                AND evidence.original->'request'->>'artifact_id' = a.id::text
                AND evidence.original->'request'->>'upload_session_id' = OLD.output_upload_session_id::text
                AND evidence.original->'request'->>'attempt' = OLD.attempt::text
                AND evidence.original->'request'->>'generation' = OLD.output_generation::text
                AND evidence.original->'request'->>'output_manifest_sha256' = OLD.output_manifest_sha256
                AND evidence.original->'request'->>'output_marker_sha256' = OLD.output_marker_sha256
                AND (evidence.original->>'previous_materialization_attempts')::int + 1 = OLD.materialization_attempts
                AND evidence.retry->'request'->>'team_id' = OLD.team_id::text
                AND evidence.retry->'request'->>'lease_id' = OLD.id::text
                AND evidence.retry->'request'->>'schema_head' = (SELECT version_num FROM alembic_version)
                AND evidence.retry->>'status' = 'requeued'
                AND evidence.retry->>'request_sha256' ~ '^sha256:[0-9a-f]{64}$'
                AND evidence.retry->>'original_audit_sha256' ~ '^sha256:[0-9a-f]{64}$'
                AND evidence.retry->'request'->>'candidate_sha' ~ '^[0-9a-f]{40}$'
                AND evidence.retry->'request'->>'operation_id' ~ '^[0-9a-f-]{36}$'
                AND (evidence.retry->>'previous_materialization_attempts')::int = OLD.materialization_attempts
                AND (evidence.retry->>'requested_at')::timestamptz = NEW.materialization_recovery_requested_at
            )
          )"""
_REPLACEMENT = "AND NOT COALESCE(" + _AUTHORIZATION + " OR (\n            OLD.materialization_state = 'unavailable'"


def _replace(*, upgrade: bool) -> None:
    definition = op.get_bind().scalar(sa.text(
        "SELECT pg_get_functiondef(to_regprocedure('validate_execution_lease_mutation()'))"
    ))
    before, after = (_ANCHOR, _REPLACEMENT) if upgrade else (_REPLACEMENT, _ANCHOR)
    if not isinstance(definition, str) or definition.count(before) != 2:
        raise RuntimeError("unexpected archival recovery guard")
    op.execute(sa.text(definition.replace(before, after)))


def upgrade() -> None:
    _replace(upgrade=True)


def downgrade() -> None:
    # Remove this admission only; retain original and retry audits and timestamps.
    _replace(upgrade=False)
