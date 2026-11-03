"""Durable batch drain cancellation.

Revision ID: 0189
Revises: 0188

Adds the non-terminal ``cancelling`` batch state, the cancellation mode that
recovery reads after a partial fanout, and the per-trial marker that keeps a
verifying or archiving trial out of later cancellation sweeps.
"""

from alembic import op

revision = "0189"
down_revision = "0188"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE batches DROP CONSTRAINT batches_state_chk;
        ALTER TABLE batches ADD CONSTRAINT batches_state_chk
            CHECK (state IN ('submitted', 'running', 'cancelling', 'finished', 'cancelled'));
        ALTER TABLE batches
            ADD COLUMN cancellation_requested_at TIMESTAMPTZ,
            ADD COLUMN cancellation_mode TEXT,
            ADD CONSTRAINT batches_cancellation_mode_chk
                CHECK (cancellation_mode IS NULL OR cancellation_mode IN ('full', 'drain'));
        ALTER TABLE trials
            ADD COLUMN cancellation_retained_at TIMESTAMPTZ;
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE trials DROP COLUMN cancellation_retained_at;
        ALTER TABLE batches DROP CONSTRAINT batches_cancellation_mode_chk;
        ALTER TABLE batches DROP COLUMN cancellation_mode;
        ALTER TABLE batches DROP COLUMN cancellation_requested_at;
        ALTER TABLE batches DROP CONSTRAINT batches_state_chk;
        ALTER TABLE batches ADD CONSTRAINT batches_state_chk
            CHECK (state IN ('submitted', 'running', 'finished', 'cancelled'));
    """)
