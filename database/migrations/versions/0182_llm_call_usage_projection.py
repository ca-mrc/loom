"""Insert-time cost/usage projection columns on llm_calls (#2687).

Revision ID: 0182
Revises: 0181

Nullable columns only: no table rewrite and no backfill. Rows written before
this revision keep NULL and readers fall back to provider_extras for them.
"""
from alembic import op

revision = "0182"
down_revision = "0181"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE llm_calls
            ADD COLUMN cost_source TEXT,
            ADD COLUMN cost_confidence TEXT,
            ADD COLUMN usage_status TEXT;
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE llm_calls
            DROP COLUMN usage_status,
            DROP COLUMN cost_confidence,
            DROP COLUMN cost_source;
    """)
