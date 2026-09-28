"""Keep historical shared verifier rows on the snapshot path.

Revision ID: 0167
Revises: 0166

Ingest used to store env_mode=shared while grading in a second sandbox.
shared now means in-place grading, so those rows become separate.
"""

from alembic import op

revision = "0167"
down_revision = "0166"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE tasks
        SET config = jsonb_set(config, '{verifier,env_mode}', '"separate"', false)
        WHERE config #>> '{verifier,env_mode}' = 'shared'
    """)


def downgrade() -> None:
    # Historical rows were all forced to shared. Reversing every separate row
    # would also rewrite tasks that opted into separate after this migration.
    pass
