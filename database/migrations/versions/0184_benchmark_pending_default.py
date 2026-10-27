"""New benchmark profiles require shared admission; retain existing catalogs.

Revision ID: 0184
Revises: 0183

Only the insert default changes. No accepted profile, alias, Batch, Trial,
source reference or qualification evidence is rewritten. Rollback restores
only the old insert default and does not grant admission to pending rows.
"""
import sqlalchemy as sa
from alembic import op

revision = "0184"
down_revision = "0183"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("benchmarks", "execution_state", server_default=sa.text("'pending'"))


def downgrade() -> None:
    op.alter_column("benchmarks", "execution_state", server_default=sa.text("'runnable'"))
