"""Explicit TaskSet lifecycle; existing collections remain non-expiring.

Revision ID: 0168
Revises: 0167
"""
from alembic import op

revision = "0168"
down_revision = "0167"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE task_sets
            ADD COLUMN purpose text,
            ADD COLUMN expires_at timestamptz,
            ADD COLUMN hold boolean NOT NULL DEFAULT false;
        CREATE INDEX task_sets_expiry_idx ON task_sets (expires_at, id)
            WHERE soft_deleted_at IS NULL AND NOT hold AND expires_at IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE task_sets IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM task_sets
                       WHERE purpose IS NOT NULL OR expires_at IS NOT NULL OR hold) THEN
                RAISE EXCEPTION 'cannot discard TaskSet lifecycle policy';
            END IF;
        END $$;
        DROP INDEX task_sets_expiry_idx;
        ALTER TABLE task_sets DROP COLUMN purpose, DROP COLUMN expires_at, DROP COLUMN hold;
    """)
