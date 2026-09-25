"""Retain application-only cloud mutation intent.

Revision ID: 0164
Revises: 0163
"""
from alembic import op

revision = "0164"
down_revision = "0163"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_application_cloud_effects (
            operation_id uuid NOT NULL REFERENCES nebius_application_operations(operation_id) ON DELETE RESTRICT,
            effect_key text NOT NULL,
            sequence bigint NOT NULL,
            intent_json jsonb NOT NULL,
            phase text NOT NULL,
            dispatch_epoch bigint,
            observed_resource_id text,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(operation_id,effect_key),
            CONSTRAINT nebius_application_cloud_sequence_key UNIQUE(operation_id,sequence),
            CONSTRAINT nebius_application_cloud_key_check CHECK (sequence > 0 AND effect_key ~ '^[a-z][a-z0-9:-]{0,127}$'),
            CONSTRAINT nebius_application_cloud_shape_check CHECK (
                phase IN ('prepared','dispatched','observed') AND jsonb_typeof(intent_json) = 'object'),
            CONSTRAINT nebius_application_cloud_dispatch_check CHECK (
                (phase = 'prepared') = (dispatch_epoch IS NULL) AND (dispatch_epoch IS NULL OR dispatch_epoch > 0)),
            CONSTRAINT nebius_application_cloud_observation_check CHECK (
                (phase = 'observed') = (observed_resource_id IS NOT NULL) AND
                (observed_resource_id IS NULL OR observed_resource_id ~ '^[a-zA-Z0-9_-]{1,128}$'))
        );
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_application_cloud_effects IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_application_cloud_effects) THEN
                RAISE EXCEPTION 'cannot remove application cloud history';
            END IF;
        END $$;
        DROP TABLE nebius_application_cloud_effects;
    """)
