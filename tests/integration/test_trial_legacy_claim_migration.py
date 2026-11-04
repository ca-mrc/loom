"""Upgrade ordering and history-preserving rollback of refundable claims."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema_startup import service_schema_head
from loom_control_plane.routes.workers import _REQUEUE_TRIAL_RETRY_SQL
from tests.integration.conftest import allow_current_cancellation_columns
from tests.integration.test_capacity_guard_migrations import _guard_config
from tests.integration.test_protected_claim_application_migration import _config
from tests.integration.test_trial_legacy_claim_identity import _claim, _seed


async def test_upgrade_does_not_invent_identity_for_historical_claim(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    await asyncio.to_thread(command.downgrade, config, "0146")
    # The current trial model writes admission_pause_releases. This case stays
    # on 0146, so add that column without moving the Alembic revision.
    await asyncio.to_thread(
        allow_current_cancellation_columns, isolated_migration_postgres_url,
    )
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            trial_id, worker_id = await _seed(session)
            await session.execute(text(
                "UPDATE trials SET state='claimed', worker_id=:worker, "
                "attempt_count=1, claimed_at=now() WHERE id=:id"
            ), {"worker": worker_id, "id": trial_id})
            before = (await session.execute(text(
                "SELECT state, worker_id, attempt_count, claimed_at FROM trials WHERE id=:id"
            ), {"id": trial_id})).one()
        await asyncio.to_thread(command.upgrade, config, "0147")
        async with sessions() as session:
            after = (await session.execute(text(
                "SELECT state, worker_id, attempt_count, claimed_at, legacy_claim_id "
                "FROM trials WHERE id=:id"
            ), {"id": trial_id})).one()
            assert after[:-1] == tuple(before) and after[-1] is None
        # No new claim identity exists, so an unchanged historical row permits
        # an ordinary schema rollback and reupgrade without data rewriting.
        await asyncio.to_thread(command.downgrade, config, "0146")
        await asyncio.to_thread(command.upgrade, config, "0147")
    finally:
        await engine.dispose()


async def test_retained_claim_prevents_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            trial_id, worker_id = await _seed(session)
            row = await _claim(session, worker_id, shared=False)
            assert row is not None and isinstance(row["claim_id"], UUID)
            identity = row["claim_id"]
        with pytest.raises(DBAPIError, match="retained legacy claim identities"):
            await asyncio.to_thread(command.downgrade, config, "0146")
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
            assert await connection.scalar(text(
                "SELECT legacy_claim_id FROM trials WHERE id=:id"
            ), {"id": trial_id}) == identity
    finally:
        await engine.dispose()


async def test_refund_history_without_claim_identity_prevents_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_async_engine(isolated_migration_postgres_url)
    history = text(
        "SELECT id, attempt, owner_kind, state, release_reason "
        "FROM execution_admission_reservations WHERE trial_id=:id"
    )
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            trial_id, worker_id = await _seed(session)
            assert await _claim(session, worker_id, shared=False) is not None
            assert await session.scalar(_REQUEUE_TRIAL_RETRY_SQL, {
                "trial_id": trial_id, "worker_id": worker_id,
                "failure_reason": "node_setup_health", "failure_message": "fixture",
                "retry_after_sec": 0,
            }) == trial_id
            # Disposable database only: isolate the independent refund-history
            # refusal from the retained-UUID refusal tested above.
            await session.execute(text(
                "UPDATE trials SET legacy_claim_id=NULL WHERE id=:id"
            ), {"id": trial_id})
            before = (await session.execute(history, {"id": trial_id})).one()
            assert before.state == "released" and before.release_reason == "trial_setup_refund"
            assert await session.scalar(text(
                "SELECT count(*) FROM trials WHERE legacy_claim_id IS NOT NULL"
            )) == 0
        with pytest.raises(DBAPIError, match="retained legacy claim identities"):
            await asyncio.to_thread(command.downgrade, config, "0146")
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
            assert (await connection.execute(history, {"id": trial_id})).one() == before
    finally:
        await engine.dispose()


def test_guard_compatibility_precedes_application_index_change(capacity_guard_database):
    database = capacity_guard_database
    guard_config = _guard_config(database)
    app_config = _config(str(database["admin_url"]))
    engine = create_engine(str(database["admin_url"]))
    signature = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
    old = "ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING"
    new = "-- guard_0033: refundable admission compatibility\n          ON CONFLICT DO NOTHING"
    query = text(
        "SELECT pg_get_functiondef(oid), proowner, proacl, prosecdef, proconfig "
        "FROM pg_proc WHERE oid=CAST(:signature AS regprocedure)"
    )
    try:
        # Recreate the real previous function in this disposable database only.
        command.downgrade(app_config, "0146")
        command.downgrade(guard_config, "guard_0032")
        with engine.begin() as connection:
            before = connection.execute(query, {"signature": signature}).one()
            assert before[0].count(new) == 1
            connection.execute(text(before[0].replace(new, old)))
        with pytest.raises(RuntimeError, match="installed guard_0033"):
            command.upgrade(app_config, "0147")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0146"
            assert connection.scalar(text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='trials' "
                "AND column_name='legacy_claim_id'"
            )) == 0
        command.upgrade(guard_config, "guard_0033")
        with engine.connect() as connection:
            compatible = connection.execute(query, {"signature": signature}).one()
            assert compatible == before
        command.upgrade(app_config, "0147")
        # Guard label rollback must not reopen an incompatible ON CONFLICT target.
        command.downgrade(guard_config, "guard_0032")
        command.upgrade(guard_config, "guard_0033")
        with engine.connect() as connection:
            assert connection.execute(query, {"signature": signature}).one() == before
    finally:
        engine.dispose()
