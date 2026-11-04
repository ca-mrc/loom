"""Platform-admin pause of one batch's pre-start queue.

Revision ID: 0190
Revises: 0189

Adds the batch-level admission hold. A paused batch stays ``running``; only
new attempt reservations are refused while ``admission_paused_at`` is set.
``trials.admission_pause_releases`` counts attempt numbers the pause drain
released before their pod was scheduled, so the retry budget excludes them.
"""

from alembic import op

revision = "0190"
down_revision = "0189"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Adding a column takes ACCESS EXCLUSIVE. A reader's ACCESS SHARE does not
    # conflict with the wait, so refuse it before statement_timeout.
    op.execute("LOCK TABLE batches IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("LOCK TABLE trials IN ACCESS EXCLUSIVE MODE NOWAIT")
    # A constant default is metadata-only, so this does not rewrite trials.
    op.execute("""
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS admission_paused_at TIMESTAMPTZ;
        ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS admission_paused_reason TEXT;
        ALTER TABLE trials
            ADD COLUMN IF NOT EXISTS admission_pause_releases INTEGER NOT NULL DEFAULT 0;
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE batches IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("LOCK TABLE trials IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        ALTER TABLE trials DROP COLUMN admission_pause_releases;
        ALTER TABLE batches DROP COLUMN admission_paused_reason;
        ALTER TABLE batches DROP COLUMN admission_paused_at;
    """)
