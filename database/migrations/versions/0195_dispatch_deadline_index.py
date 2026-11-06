"""Bound active dispatch counts independently of expired unknown receipts.

Revision ID: 0195
Revises: 0194
"""

import sqlalchemy as sa
from alembic import op

revision = "0195"
down_revision = "0194"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_index(
        "gateway_receipt_admitted_deadline_idx",
        "gateway_dispatch_receipts",
        ["provider_connection_id", "attempt_deadline_wall_clock"],
        postgresql_where=sa.text("provider_outcome = 'admitted'"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index("gateway_receipt_admitted_deadline_idx", "gateway_dispatch_receipts")
