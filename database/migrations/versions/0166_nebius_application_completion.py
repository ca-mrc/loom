"""Retain stopped application completion evidence independently of frozen intent.

Revision ID: 0166
Revises: 0165
"""
from alembic import op

revision = "0166"
down_revision = "0165"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE nebius_application_operations
            ADD COLUMN completion_json jsonb,
            ADD COLUMN completed_at timestamptz,
            ADD CONSTRAINT nebius_application_operation_completion_check CHECK (
                (completion_json IS NULL) = (completed_at IS NULL) AND
                (completion_json IS NULL OR (jsonb_typeof(completion_json) = 'object'
                    AND phase IN ('completed','superseded')))
            );
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_application_operations IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_application_operations
                       WHERE completion_json IS NOT NULL OR completed_at IS NOT NULL) THEN
                RAISE EXCEPTION 'cannot remove application completion evidence';
            END IF;
        END $$;
        ALTER TABLE nebius_application_operations
            DROP CONSTRAINT nebius_application_operation_completion_check,
            DROP COLUMN completion_json,
            DROP COLUMN completed_at;
    """)
