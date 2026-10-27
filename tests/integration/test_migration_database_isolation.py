"""Real PostgreSQL boundaries for the shared, immutable migration template."""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from tests.support import postgres_databases
from tests.support.postgres_databases import isolated_migration_database


def test_primary_mutation_cannot_reach_historical_database(
    postgres_url: str,
    migration_template_postgres_url: str,
    request: pytest.FixtureRequest,
) -> None:
    primary = create_engine(postgres_url)
    try:
        with primary.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE migration_fixture_primary_probe (value integer)")
            connection.exec_driver_sql("INSERT INTO migration_fixture_primary_probe VALUES (1)")
        # Resolve this only after contaminating the ordinary session database.
        historical_url = request.getfixturevalue("isolated_migration_postgres_url")
        historical = create_engine(historical_url)
        try:
            with historical.connect() as connection:
                assert connection.exec_driver_sql(
                    "SELECT to_regclass('migration_fixture_primary_probe')"
                ).scalar_one() is None
                assert connection.exec_driver_sql("SELECT count(*) FROM trials").scalar_one() == 0
                assert connection.exec_driver_sql(
                    "SELECT current_database()"
                ).scalar_one() != make_url(postgres_url).database
            with primary.connect() as connection:
                assert connection.execute(
                    text("SELECT datallowconn FROM pg_database WHERE datname=:name"),
                    {"name": make_url(migration_template_postgres_url).database},
                ).scalar_one() is False
                assert connection.execute(
                    text("SELECT count(*) FROM pg_database WHERE datname LIKE :prefix"),
                    {"prefix": "loom_migration_template_%"},
                ).scalar_one() == 1
        finally:
            historical.dispose()
    finally:
        with primary.begin() as connection:
            connection.exec_driver_sql("DROP TABLE IF EXISTS migration_fixture_primary_probe")
        primary.dispose()


def test_failed_template_initialization_drops_only_owned_database(
    postgres_url: str,
    migration_template_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            before = set(connection.exec_driver_sql("SELECT datname FROM pg_database").scalars())
            before_trials = connection.exec_driver_sql("SELECT count(*) FROM trials").scalar_one()

        def rejected_migration(*args, **kwargs):
            raise subprocess.CalledProcessError(1, ["alembic", "upgrade", "head"])

        monkeypatch.setattr(postgres_databases.subprocess, "run", rejected_migration)
        failed = isolated_migration_database(
            postgres_url, template_name="template0", prepare_template=True
        )
        with pytest.raises(subprocess.CalledProcessError):
            next(failed)
        with engine.connect() as connection:
            assert set(connection.exec_driver_sql("SELECT datname FROM pg_database").scalars()) == before
            assert connection.execute(
                text("SELECT datallowconn FROM pg_database WHERE datname=:name"),
                {"name": make_url(migration_template_postgres_url).database},
            ).scalar_one() is False
            assert connection.exec_driver_sql("SELECT count(*) FROM trials").scalar_one() == before_trials
    finally:
        engine.dispose()


def test_concurrent_historical_clones_have_separate_state_and_cleanup(
    postgres_url: str,
    migration_template_postgres_url: str,
) -> None:
    template_name = make_url(migration_template_postgres_url).database
    assert template_name is not None
    opened = []

    def mutate_clone(value: int):
        database = isolated_migration_database(
            migration_template_postgres_url, template_name=template_name, prepare_template=False
        )
        engine = None
        try:
            url = next(database)
            engine = create_engine(url)
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE migration_fixture_concurrent_probe (value integer)")
                connection.execute(
                    text("INSERT INTO migration_fixture_concurrent_probe VALUES (:value)"),
                    {"value": value},
                )
            name = make_url(url).database
            assert name is not None
            return database, engine, name, value
        except BaseException:
            if engine is not None:
                engine.dispose()
            database.close()
            raise

    # Keep both returned databases open while reading their independent state.
    # No inter-worker barrier can strand a peer when creation fails.
    try:
        failures = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(mutate_clone, value) for value in (1, 2)]
            for future in futures:
                try:
                    opened.append(future.result())
                except Exception as error:
                    failures.append(error)
        if failures:
            raise failures[0]
        names = [item[2] for item in opened]
        for _, engine, _, value in opened:
            with engine.connect() as connection:
                assert connection.exec_driver_sql(
                    "SELECT value FROM migration_fixture_concurrent_probe"
                ).scalars().all() == [value]
    finally:
        for database, engine, _, _ in opened:
            engine.dispose()
            database.close()
    assert len(set(names)) == 2
    primary = create_engine(postgres_url)
    try:
        with primary.connect() as connection:
            assert connection.execute(
                text("SELECT count(*) FROM pg_database WHERE datname IN (:one, :two)"),
                {"one": names[0], "two": names[1]},
            ).scalar_one() == 0
            assert connection.exec_driver_sql(
                "SELECT to_regclass('migration_fixture_concurrent_probe')"
            ).scalar_one() is None
    finally:
        primary.dispose()
