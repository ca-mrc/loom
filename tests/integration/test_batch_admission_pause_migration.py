"""Migration 0191 waits out short read transactions and still refuses a busy table."""

import threading
import time

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from tests.integration.test_protected_claim_application_migration import _config

_ADDED_COLUMNS = (
    "SELECT count(*) FROM information_schema.columns WHERE "
    "(table_name = 'batches' AND column_name IN ('admission_paused_at', 'admission_paused_reason')) "
    "OR (table_name = 'trials' AND column_name = 'admission_pause_releases')"
)


@pytest.fixture
def pause_migration(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    database = make_url(isolated_migration_postgres_url).database
    assert database is not None
    command.downgrade(config, "0190")
    # A short window keeps the refusal case fast; the default is a minute.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
        admin.execute(text(f'ALTER DATABASE "{database}" SET loom.migration_lock_wait_seconds = \'3\''))
    try:
        yield config, engine
    finally:
        engine.dispose()


def _upgrade_in_thread(config):
    outcome: dict[str, BaseException | None] = {"error": None}

    def run() -> None:
        try:
            command.upgrade(config, "0191")
        except BaseException as exc:  # reported to the test thread
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def test_upgrade_waits_for_a_reader_that_finishes_inside_the_window(pause_migration):
    config, engine = pause_migration
    with engine.connect() as reader:
        reader.execute(text("SELECT 1 FROM trials LIMIT 1"))
        thread, outcome = _upgrade_in_thread(config)
        time.sleep(1.0)
        # While retrying, neither table stays locked for other readers.
        with engine.connect() as other:
            other.execute(text("SET LOCAL lock_timeout = '2s'"))
            other.execute(text("SELECT 1 FROM batches LIMIT 1"))
        assert thread.is_alive()
        reader.rollback()
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert outcome["error"] is None
    with engine.connect() as check:
        assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0191"
        assert check.scalar(text(_ADDED_COLUMNS)) == 3


def test_upgrade_still_refuses_a_table_that_stays_busy(pause_migration):
    config, engine = pause_migration
    with engine.connect() as reader:
        reader.execute(text("SELECT 1 FROM trials LIMIT 1"))
        started = time.monotonic()
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            command.upgrade(config, "0191")
        assert time.monotonic() - started >= 3
        with engine.connect() as check:
            check.execute(text("SET LOCAL lock_timeout = '2s'"))
            # Nothing was left locked or half applied.
            check.execute(text("SELECT 1 FROM batches LIMIT 1"))
            assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0190"
            assert check.scalar(text(_ADDED_COLUMNS)) == 0
    command.upgrade(config, "0191")
    with engine.connect() as check:
        assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0191"
        assert check.scalar(text(_ADDED_COLUMNS)) == 3
