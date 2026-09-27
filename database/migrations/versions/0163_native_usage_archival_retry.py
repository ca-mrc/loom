"""Allow one audited storage retry for retained native usage-roundoff failures.

Revision ID: 0163
Revises: 0162
"""
import sqlalchemy as sa
from alembic import op

revision = "0163"
down_revision = "0162"
branch_labels = None
depends_on = None

_OLD_ERROR = "OLD.materialization_error_code = 'verifier_reward_drift'"
_NEW_ERROR = "OLD.materialization_error_code IN ('verifier_reward_drift','usage_output_identity_drift')"
_OLD_OUTCOME = """t.result->'runtime_result'->>'status' = 'verifier_error'
                AND t.result->'runtime_result'->'partial_evidence' = 'true'::jsonb
                AND COALESCE(t.result->'runtime_result'->'verifier_rewards', 'null'::jsonb) = 'null'::jsonb
                AND jsonb_path_query_first(t.result,
                    '$.runtime_result.outputs[*] ? (@.kind == "verifier" && @.state == "captured")')
                    ->>'relative_path' = 'diagnostics/verifier-exception.json'"""
_NEW_OUTCOME = """((OLD.materialization_error_code = 'verifier_reward_drift' AND """ + _OLD_OUTCOME + """)
                OR (OLD.materialization_error_code = 'usage_output_identity_drift'
                    AND t.config->>'agent_name' = 'terminus-2'
                    AND t.result->'runtime_result'->>'status' = 'succeeded'
                    AND t.result->'runtime_result'->'partial_evidence' = 'false'::jsonb
                    AND OLD.canonical_trajectory_sha256 IS NULL AND OLD.canonical_atif_sha256 IS NULL))"""


def _replace(*, upgrade: bool) -> None:
    definition = op.get_bind().scalar(sa.text(
        "SELECT pg_get_functiondef(to_regprocedure('validate_execution_lease_mutation()'))"
    ))
    if not isinstance(definition, str):
        raise RuntimeError("missing execution lease mutation function")
    for old, new in ((_OLD_ERROR, _NEW_ERROR), (_OLD_OUTCOME, _NEW_OUTCOME)):
        before, after = (old, new) if upgrade else (new, old)
        if definition.count(before) != 2:
            raise RuntimeError("unexpected archival recovery guard")
        definition = definition.replace(before, after)
    op.execute(sa.text(definition))


def upgrade() -> None:
    _replace(upgrade=True)


def downgrade() -> None:
    # Existing recovery timestamps, artifact audit records and lease history
    # remain immutable; only admission of another usage recovery is removed.
    _replace(upgrade=False)
