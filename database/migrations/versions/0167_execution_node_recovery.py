"""Allow evidence-bound recovery of missing nodes after execution deletion.

Revision ID: 0167
Revises: 0166
"""

import sqlalchemy as sa
from alembic import op

revision = "0167"
down_revision = "0166"
branch_labels = None
depends_on = None

_BEFORE = "          IF OLD.deleted_at IS NOT NULL\n"
_AFTER = """          IF OLD.deleted_at IS NOT NULL
             AND NOT (
               OLD.node_name IS NULL AND NEW.node_name IS NOT NULL
               AND length(NEW.node_name) BETWEEN 1 AND 253
               AND OLD.pod_uid IS NOT NULL
               AND (to_jsonb(NEW) - ARRAY['node_name','updated_at']::text[])
                   = (to_jsonb(OLD) - ARRAY['node_name','updated_at']::text[])
               AND EXISTS (
                 SELECT 1 FROM execution_events e
                 WHERE e.lease_id = OLD.id AND e.event_kind = 'kubernetes_observed'
                   AND e.generation BETWEEN OLD.resource_generation AND OLD.generation
                   AND e.payload_json->>'pod_uid' = OLD.pod_uid
                   AND e.payload_json->>'node_name' = NEW.node_name
               )
               AND NOT EXISTS (
                 SELECT 1 FROM execution_events e
                 WHERE e.lease_id = OLD.id AND e.event_kind = 'kubernetes_observed'
                   AND e.generation BETWEEN OLD.resource_generation AND OLD.generation
                   AND e.payload_json->>'node_name' IS NOT NULL
                   AND (
                     jsonb_typeof(e.payload_json->'node_name') IS DISTINCT FROM 'string'
                     OR length(e.payload_json->>'node_name') NOT BETWEEN 1 AND 253
                     OR (e.payload_json->>'pod_uid' IS NOT NULL AND (
                       e.payload_json->>'node_name' IS DISTINCT FROM NEW.node_name
                       OR e.payload_json->>'pod_uid' IS DISTINCT FROM OLD.pod_uid
                       OR (e.payload_json->>'job_uid' IS NOT NULL
                           AND e.payload_json->>'job_uid' IS DISTINCT FROM OLD.job_uid)
                     ))
                   )
               )
             )
"""


def _replace(before: str, after: str) -> None:
    definition = op.get_bind().scalar(
        sa.text("SELECT pg_get_functiondef('validate_execution_lease_mutation()'::regprocedure)")
    )
    if not isinstance(definition, str) or definition.count(before) != 1:
        raise RuntimeError("execution lease mutation guard has an unexpected definition")
    op.execute(sa.text(definition.replace(before, after, 1)))


def upgrade() -> None:
    _replace(_BEFORE, _AFTER)


def downgrade() -> None:
    # Restores the prior guard without deleting any recovered evidence.
    _replace(_AFTER, _BEFORE)
