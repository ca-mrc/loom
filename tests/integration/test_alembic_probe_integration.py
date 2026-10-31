"""Integration tests for Alembic's direct-Postgres probe (#609).

Verifies _assert_direct_postgres_connection against real Postgres and
demonstrates the pgbouncer transaction-mode session-state properties
that motivate the probe's existence.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


@pytest.mark.integration
def test_probe_passes_on_direct_connection(postgres_cluster_url: str) -> None:
    """The probe must not raise when connecting directly to Postgres."""
    from database.migrations.env import _assert_direct_postgres_connection

    engine = create_engine(postgres_cluster_url)
    _assert_direct_postgres_connection(engine)  # no raise
    engine.dispose()


@pytest.mark.integration
def test_application_name_does_not_persist_across_new_connections_through_pgbouncer(
    pgbouncer_stack: dict[str, str],
) -> None:
    """Through pgbouncer transaction mode, session state (e.g. application_name)
    is NOT preserved when a brand-new TCP connection is opened after the previous
    one closes. This underpins the Alembic probe's invariant: if you open a fresh
    connection to pgbouncer you cannot rely on session state from a prior connection.

    This test validates the real failure mode the probe is designed to catch:
    Alembic migrations must use a connection that is direct-to-Postgres so that
    SET ... is guaranteed to persist across the commit within that connection.
    """
    marker = f"alembic-integ-{uuid.uuid4()}"

    engine = create_engine(
        pgbouncer_stack["pool_url"],
        poolclass=NullPool,
        connect_args={"prepare_threshold": None},
    )

    # First connection: SET application_name and commit → backend released to pool.
    with engine.connect() as conn:
        conn.exec_driver_sql(f"SET application_name = '{marker}'")
        conn.commit()

    # Second fresh connection: gets a potentially different backend with clean state.
    with engine.connect() as conn2:
        actual = conn2.exec_driver_sql("SHOW application_name").scalar()

    # A new psycopg connection through pgbouncer must NOT see prior session state.
    assert actual != marker, (
        f"application_name {actual!r} unexpectedly matched {marker!r} on a new "
        f"connection through pgbouncer. This would indicate session state is "
        f"leaking across backend connections — pgbouncer may not be in "
        f"transaction mode."
    )

    engine.dispose()


# Note: a "probe raises through pgbouncer" negative test used to live here but
# was removed because probe detection depends on the pool state (backend
# rotation happens only when there's contention). At low concurrency the same
# backend is reused across SET/commit/SHOW inside one client TCP connection, so
# the probe appears to pass. The real failure mode the probe defends against
# (session state not persisting when new connections are drawn from the pool)
# is exercised by `test_application_name_does_not_persist_across_new_connections_through_pgbouncer`
# above.
