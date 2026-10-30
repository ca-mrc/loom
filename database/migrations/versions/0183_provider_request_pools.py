"""Opt-in provider request pools and physical dispatch attribution.

Revision ID: 0183
Revises: 0182
"""

from alembic import op

revision = "0183"
down_revision = "0182"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE provider_request_pools (
            connection_id UUID PRIMARY KEY REFERENCES provider_connections(id) ON DELETE CASCADE,
            required_models TEXT[] NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT true,
            next_member BIGINT NOT NULL DEFAULT 0,
            attempt_timeout_sec DOUBLE PRECISION NOT NULL DEFAULT 20,
            cooldown_sec DOUBLE PRECISION NOT NULL DEFAULT 30,
            CONSTRAINT provider_request_pool_models_check CHECK (cardinality(required_models) > 0),
            CONSTRAINT provider_request_pool_cursor_check CHECK (next_member >= 0),
            CONSTRAINT provider_request_pool_timeout_check
                CHECK (attempt_timeout_sec > 0 AND cooldown_sec > 0)
        );
        CREATE TABLE provider_request_pool_members (
            pool_connection_id UUID NOT NULL
                REFERENCES provider_request_pools(connection_id) ON DELETE CASCADE,
            connection_id UUID NOT NULL REFERENCES provider_connections(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            quota_group TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT true,
            cooldown_until TIMESTAMPTZ,
            PRIMARY KEY (pool_connection_id, connection_id),
            CONSTRAINT provider_request_pool_member_ordinal_key UNIQUE (pool_connection_id, ordinal),
            CONSTRAINT provider_request_pool_member_ordinal_check CHECK (ordinal >= 0)
        );
        ALTER TABLE gateway_dispatch_receipts
            ADD COLUMN pool_connection_id UUID
                REFERENCES provider_connections(id) ON DELETE SET NULL,
            ADD COLUMN failover_reason TEXT;
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE gateway_dispatch_receipts
            DROP COLUMN failover_reason,
            DROP COLUMN pool_connection_id;
        DROP TABLE provider_request_pool_members;
        DROP TABLE provider_request_pools;
    """)
