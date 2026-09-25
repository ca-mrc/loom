"""Application generation material in the existing encrypted SecretStore.

Revision ID: 0163
Revises: 0162
"""
from alembic import op

revision = "0163"
down_revision = "0162"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_application_material (
            operation_id uuid PRIMARY KEY REFERENCES nebius_application_operations(operation_id) ON DELETE RESTRICT,
            secret_ref text NOT NULL REFERENCES secrets(ref) ON DELETE RESTRICT,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT nebius_application_material_ref_key UNIQUE(secret_ref)
        );
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_application_material IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_application_material) THEN
                RAISE EXCEPTION 'cannot remove application material history';
            END IF;
        END $$;
        DROP TABLE nebius_application_material;
    """)
