"""Sticky request-pool bindings, shared timeout health, and in-flight caps.

Revision ID: 0187
Revises: 0186
"""

from alembic import op

revision = "0187"
down_revision = "0186"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE provider_connections
            ADD COLUMN max_in_flight INTEGER,
            ADD CONSTRAINT provider_connection_max_in_flight_check
                CHECK (max_in_flight IS NULL OR (max_in_flight BETWEEN 1 AND 1000));
        ALTER TABLE provider_request_pools
            ADD COLUMN timeout_burst_count INTEGER NOT NULL DEFAULT 3,
            ADD COLUMN timeout_window_sec DOUBLE PRECISION NOT NULL DEFAULT 60,
            ADD CONSTRAINT provider_request_pool_timeout_burst_check
                CHECK (timeout_burst_count > 0 AND timeout_window_sec > 0);
        CREATE TABLE provider_request_pool_bindings (
            pool_connection_id UUID NOT NULL
                REFERENCES provider_request_pools(connection_id) ON DELETE CASCADE,
            trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
            member_connection_id UUID NOT NULL
                REFERENCES provider_connections(id) ON DELETE CASCADE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (pool_connection_id, trial_id)
        );
        CREATE TABLE provider_connection_timeout_health (
            connection_id UUID PRIMARY KEY
                REFERENCES provider_connections(id) ON DELETE CASCADE,
            suppressed_until TIMESTAMPTZ,
            suppression_started_at TIMESTAMPTZ
        );
        CREATE TABLE provider_connection_timeout_observations (
            receipt_id UUID PRIMARY KEY
                REFERENCES gateway_dispatch_receipts(id) ON DELETE CASCADE,
            connection_id UUID NOT NULL
                REFERENCES provider_connections(id) ON DELETE CASCADE,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX provider_connection_timeout_observations_window_idx
            ON provider_connection_timeout_observations (connection_id, observed_at);
        CREATE INDEX gateway_receipt_admitted_connection_idx
            ON gateway_dispatch_receipts (provider_connection_id, admitted_at)
            WHERE provider_outcome = 'admitted';
    """)


def downgrade() -> None:
    op.execute("""
        DROP INDEX gateway_receipt_admitted_connection_idx;
        DROP TABLE provider_connection_timeout_observations;
        DROP TABLE provider_connection_timeout_health;
        DROP TABLE provider_request_pool_bindings;
        ALTER TABLE provider_request_pools
            DROP CONSTRAINT provider_request_pool_timeout_burst_check,
            DROP COLUMN timeout_window_sec,
            DROP COLUMN timeout_burst_count;
        ALTER TABLE provider_connections
            DROP CONSTRAINT provider_connection_max_in_flight_check,
            DROP COLUMN max_in_flight;
    """)
