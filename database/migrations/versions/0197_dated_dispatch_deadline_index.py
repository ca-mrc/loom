"""Keep undated dispatch counts off the deadline index.

`active_dispatch_count` counts admitted receipts for a connection in two
ranges: a future deadline, or no deadline and a recent admission. 0195 indexed
(connection, deadline) for every admitted receipt, including undated ones. On a
table where most rows carry a deadline, the planner then served the undated
range as `deadline IS NULL` on that index and filtered `admitted_at`
afterwards, visiting every expired undated receipt of the connection on each
capacity lookup. Restricting the index to dated receipts leaves the undated
range to `gateway_receipt_admitted_connection_idx` (connection, admitted_at);
`deadline > :now` still implies the new predicate, including in generic plans.

Revision ID: 0197
Revises: 0196
"""

import sqlalchemy as sa
from alembic import op

revision = "0197"
down_revision = "0196"
branch_labels = None
depends_on = None

_NAME = "gateway_receipt_admitted_deadline_idx"
_TABLE = "gateway_dispatch_receipts"
_COLUMNS = ["provider_connection_id", "attempt_deadline_wall_clock"]


def upgrade() -> None:
    # Only admitted (in-flight or unknown) receipts are indexed, so the
    # rebuild is small; a short lock timeout fails fast instead of queueing.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index(_NAME, _TABLE)
    op.create_index(
        _NAME,
        _TABLE,
        _COLUMNS,
        postgresql_where=sa.text(
            "provider_outcome = 'admitted' AND attempt_deadline_wall_clock IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index(_NAME, _TABLE)
    op.create_index(
        _NAME,
        _TABLE,
        _COLUMNS,
        postgresql_where=sa.text("provider_outcome = 'admitted'"),
    )
