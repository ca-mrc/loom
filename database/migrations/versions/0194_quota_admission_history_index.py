"""Bound quota-group stall lookups independently of completed dispatch history.

Revision ID: 0194
Revises: 0193
"""

from alembic import op

revision = "0194"
down_revision = "0193"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_index(
        "gateway_receipt_connection_history_idx",
        "gateway_dispatch_receipts",
        ["provider_connection_id", "admitted_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index("gateway_receipt_connection_history_idx", "gateway_dispatch_receipts")
