"""Allow one audited requeue of a parked separate-verifier archive.

Before the settlement fix, a separate-verifier parent could never commit: settling
its verifier child violated ``execution_leases_materialization_group_check`` and
the whole archive rolled back on every retry. Both leases stayed ``pending``,
which also holds the idle rollout guard. The operator parks such a pair as
``unavailable`` (code ``verifier_settlement_constraint``) under the installed
schema, rolls out the fix, then requeues the pair once so the fixed materializer
archives them. Only the state, next attempt and recovery timestamp may change.

Revision ID: 0177
Revises: 0176
"""
import sqlalchemy as sa
from alembic import op

revision = "0177"
down_revision = "0176"
branch_labels = None
depends_on = None

_ANCHOR = "AND NOT COALESCE((\n            OLD.materialization_state = 'unavailable'"
_AUTHORIZATION = """(
            OLD.materialization_state = 'unavailable'
            AND OLD.materialization_error_code = 'verifier_settlement_constraint'
            AND OLD.materialization_recovery_requested_at IS NULL
            AND NEW.materialization_recovery_requested_at IS NOT NULL
            AND NEW.materialization_state = 'pending'
            AND NEW.materialization_next_attempt_at = NEW.materialization_recovery_requested_at
            AND OLD.materialization_claim_id IS NULL AND OLD.materialization_claim_expires_at IS NULL
            AND OLD.canonical_trajectory_sha256 IS NULL AND OLD.canonical_atif_sha256 IS NULL
            AND OLD.output_commit_state = 'committed' AND OLD.output_generation = OLD.resource_generation
            AND OLD.deleted_at IS NOT NULL AND OLD.cleanup_state = 'complete'
            AND OLD.source_cleanup_state = 'not_ready' AND OLD.source_retain_until IS NULL
            AND (to_jsonb(NEW) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
                = (to_jsonb(OLD) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
            AND EXISTS (
                SELECT 1 FROM artifacts a
                CROSS JOIN LATERAL (SELECT a.metadata->'verifier_settlement_park' AS park,
                    a.metadata->'verifier_settlement_requeue' AS requeue) evidence
                WHERE a.control_producer_kind = 'service_execution' AND a.team_id = OLD.team_id
                AND a.control_producer_id = COALESCE(OLD.parent_lease_id, OLD.id)
                AND evidence.park->>'parent_lease_id' = a.control_producer_id::text
                AND evidence.park->>'team_id' = OLD.team_id::text
                AND evidence.park->>'trial_id' = OLD.trial_id::text
                AND ((OLD.execution_role = 'attempt' AND OLD.parent_lease_id IS NULL
                      AND (evidence.park->>'parent_materialization_attempts')::int = OLD.materialization_attempts)
                  OR (OLD.execution_role = 'verifier' AND evidence.park->>'child_lease_id' = OLD.id::text
                      AND (evidence.park->>'child_materialization_attempts')::int = OLD.materialization_attempts))
                AND (evidence.requeue->>'requested_at')::timestamptz = NEW.materialization_recovery_requested_at
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
    # Remove this admission only; retain park/requeue audits and timestamps.
    _replace(upgrade=False)
