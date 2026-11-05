"""Per-pool quota-group queue and committed pause credit.

Revision ID: 0193
Revises: 0192

The queue is off until queue_enabled is set. Existing pools keep today's
admission path. Queue time is the database clock on a committed row.
"""

from alembic import op

revision = "0193"
down_revision = "0192"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE provider_request_pools
            ADD COLUMN IF NOT EXISTS queue_enabled boolean NOT NULL DEFAULT false,
            ADD COLUMN IF NOT EXISTS stall_sec double precision NOT NULL DEFAULT 200,
            ADD COLUMN IF NOT EXISTS max_pause_sec double precision NOT NULL DEFAULT 3600,
            ADD COLUMN IF NOT EXISTS config_version bigint NOT NULL DEFAULT 1,
            ADD CONSTRAINT provider_request_pool_queue_check
                CHECK (stall_sec > 0 AND max_pause_sec >= 0 AND config_version > 0)
        """
    )
    op.execute(
        """
        CREATE TABLE provider_request_pool_group_caps (
            pool_connection_id uuid NOT NULL
                REFERENCES provider_request_pools(connection_id) ON DELETE CASCADE,
            quota_group text NOT NULL,
            max_in_flight integer NOT NULL,
            PRIMARY KEY (pool_connection_id, quota_group),
            CONSTRAINT provider_request_pool_group_cap_check
                CHECK (max_in_flight BETWEEN 1 AND 1000)
        )
        """
    )
    op.execute("CREATE SEQUENCE gateway_dispatch_queue_seq")
    op.execute(
        """
        CREATE TABLE gateway_dispatch_queue (
            id uuid PRIMARY KEY,
            seq bigint NOT NULL DEFAULT nextval('gateway_dispatch_queue_seq') UNIQUE,
            pool_connection_id uuid NOT NULL
                REFERENCES provider_request_pools(connection_id) ON DELETE CASCADE,
            bound_group text NOT NULL,
            request_id uuid NOT NULL,
            trial_id uuid REFERENCES trials(id) ON DELETE CASCADE,
            enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            renewed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            closed_at timestamptz,
            outcome text,
            admitted_group text,
            admitted_connection_id uuid,
            receipt_id uuid,
            CONSTRAINT gateway_dispatch_queue_outcome_check CHECK (
                outcome IS NULL OR outcome IN (
                    'admitted', 'overflow_admitted', 'stalled', 'cancelled', 'expired'
                )
            ),
            CONSTRAINT gateway_dispatch_queue_closed_check
                CHECK ((closed_at IS NULL) = (outcome IS NULL))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX gateway_dispatch_queue_open_idx
            ON gateway_dispatch_queue (pool_connection_id, seq)
            WHERE closed_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX gateway_dispatch_queue_trial_idx
            ON gateway_dispatch_queue (trial_id, enqueued_at)
        """
    )
    op.execute(
        "ALTER SEQUENCE gateway_dispatch_queue_seq OWNED BY gateway_dispatch_queue.seq"
    )
    op.execute(
        """
        CREATE FUNCTION gateway_dispatch_queue_credit(
            p_trial_id uuid, p_max_pause double precision
        ) RETURNS double precision
        LANGUAGE sql
        VOLATILE
        AS $$
            SELECT CASE
                WHEN p_trial_id IS NULL OR p_max_pause IS NULL OR p_max_pause <= 0 THEN 0
                ELSE LEAST(
                    p_max_pause,
                    COALESCE((
                        SELECT SUM(GREATEST(0, EXTRACT(EPOCH FROM (
                            LEAST(
                                COALESCE(closed_at, clock_timestamp()),
                                renewed_at + interval '15 seconds',
                                clock_timestamp()
                            ) - enqueued_at
                        ))))
                        FROM gateway_dispatch_queue
                        WHERE trial_id = p_trial_id
                    ), 0)
                )
            END
        $$
        """
    )
    op.execute(
        """
        ALTER TABLE execution_leases
            ADD COLUMN IF NOT EXISTS working_deadline_at timestamptz
        """
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Dropping the queue foreign key takes ACCESS EXCLUSIVE on trials, and
    # dropping working_deadline_at takes it on execution_leases. A reader's
    # ACCESS SHARE does not conflict with a weaker lock, so those drops would
    # wait until statement_timeout. Refuse that reader first.
    op.execute("LOCK TABLE trials, execution_leases IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("DROP FUNCTION IF EXISTS gateway_dispatch_queue_credit(uuid, double precision)")
    op.execute("ALTER TABLE execution_leases DROP COLUMN IF EXISTS working_deadline_at")
    op.execute("DROP TABLE IF EXISTS gateway_dispatch_queue")
    op.execute("DROP SEQUENCE IF EXISTS gateway_dispatch_queue_seq")
    op.execute("DROP TABLE IF EXISTS provider_request_pool_group_caps")
    op.execute(
        """
        ALTER TABLE provider_request_pools
            DROP CONSTRAINT IF EXISTS provider_request_pool_queue_check,
            DROP COLUMN IF EXISTS config_version,
            DROP COLUMN IF EXISTS max_pause_sec,
            DROP COLUMN IF EXISTS stall_sec,
            DROP COLUMN IF EXISTS queue_enabled
        """
    )
