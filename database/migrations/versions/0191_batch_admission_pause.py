"""Platform-admin pause of one batch's pre-start queue.

Revision ID: 0191
Revises: 0190

Adds the batch-level admission hold. A paused batch stays ``running``; only
new attempt reservations are refused while ``admission_paused_at`` is set.
``trials.admission_pause_releases`` counts attempt numbers the pause drain
released before their pod was scheduled, so the retry budget excludes them.
"""

from alembic import op

revision = "0191"
down_revision = "0190"
branch_labels = None
depends_on = None


# The control plane keeps long read transactions open on ``trials`` while a
# rollout runs, with only short gaps between its passes. One NOWAIT attempt
# therefore loses almost every time. Retry both tables together for a bounded
# window, then refuse exactly as a single NOWAIT attempt would. Each attempt runs
# in its own subtransaction, so a miss on either table releases both before the
# next try and no partial schema is ever left behind.
# ``loom.migration_lock_wait_seconds`` overrides the default window.
_LOCK_BATCHES_AND_TRIALS_WITH_RETRY = """
DO $$
DECLARE
    wait_seconds numeric := COALESCE(
        NULLIF(current_setting('loom.migration_lock_wait_seconds', true), '')::numeric, 60
    );
    deadline timestamptz := clock_timestamp() + make_interval(secs => wait_seconds::double precision);
BEGIN
    LOOP
        BEGIN
            LOCK TABLE batches, trials IN ACCESS EXCLUSIVE MODE NOWAIT;
            EXIT;
        EXCEPTION WHEN lock_not_available THEN
            IF clock_timestamp() >= deadline THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(0.25);
        END;
    END LOOP;
END
$$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Adding a column takes ACCESS EXCLUSIVE. A reader's ACCESS SHARE does not
    # conflict with the wait, so refuse it before statement_timeout.
    op.execute(_LOCK_BATCHES_AND_TRIALS_WITH_RETRY)
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
    op.execute("LOCK TABLE batches, trials IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        ALTER TABLE trials DROP COLUMN admission_pause_releases;
        ALTER TABLE batches DROP COLUMN admission_paused_reason;
        ALTER TABLE batches DROP COLUMN admission_paused_at;
    """)
