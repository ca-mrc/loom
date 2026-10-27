"""Provisional native trajectory feed, separate from canonical archive sequences.

Revision ID: 0180
Revises: 0179
"""
from alembic import op

revision = "0180"
down_revision = "0179"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE service_execution_live_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
            lease_id UUID NOT NULL REFERENCES execution_leases(id) ON DELETE CASCADE,
            resource_generation BIGINT NOT NULL,
            seq BIGINT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            lifecycle_authority_id UUID NOT NULL REFERENCES data_lifecycle_authorities(id) ON DELETE RESTRICT,
            CONSTRAINT service_execution_live_events_sequence_check CHECK (resource_generation > 0 AND seq >= 0),
            CONSTRAINT service_execution_live_events_source_seq_uidx UNIQUE (lease_id, resource_generation, seq)
        );
        CREATE INDEX service_execution_live_events_trial_idx ON service_execution_live_events(trial_id);
        CREATE INDEX ix_service_execution_live_events_lifecycle_authority_id ON service_execution_live_events(lifecycle_authority_id);
        CREATE FUNCTION notify_service_execution_live_event() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('trial_events_inserted', NEW.trial_id::text || ':preview:' || NEW.seq::text);
            RETURN NEW;
        END $$;
        CREATE TRIGGER service_execution_live_events_notify AFTER INSERT ON service_execution_live_events
            FOR EACH ROW EXECUTE FUNCTION notify_service_execution_live_event();
    """)


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM service_execution_live_events) THEN
                RAISE EXCEPTION 'cannot discard native live trajectory evidence';
            END IF;
        END $$;
        DROP TABLE service_execution_live_events;
        DROP FUNCTION notify_service_execution_live_event();
    """)
