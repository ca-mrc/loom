"""Migration 0014: tasks.tags + benchmarks.series.

Pins the column shape + default behavior + downgrade reversibility so
PR-2's filter queries can rely on the columns existing with the right
defaults."""

from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


def _alembic_cfg(db_url: str) -> Config:
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _engine(db_url: str) -> Engine:
    return create_engine(db_url, future=True)


@pytest.fixture
def at_0013(isolated_migration_postgres_url: str) -> Engine:
    """Roll back to 0013 (the migration immediately before 0014)
    before each test so we can exercise the upgrade path freshly."""
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0013")
    return _engine(isolated_migration_postgres_url)


def test_0014_adds_tags_and_series(
    at_0013: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0014")
    insp = inspect(at_0013)
    task_cols = {c["name"] for c in insp.get_columns("tasks")}
    bench_cols = {c["name"] for c in insp.get_columns("benchmarks")}
    assert "tags" in task_cols
    assert "series" in bench_cols
    indexes = {ix["name"] for ix in insp.get_indexes("tasks")}
    assert "ix_tasks_tags_gin" in indexes
    bench_indexes = {ix["name"] for ix in insp.get_indexes("benchmarks")}
    assert "ix_benchmarks_series" in bench_indexes


def test_0014_tags_defaults_to_empty_object(
    at_0013: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    """Existing tasks (before this migration) must come back with
    tags={} — readers shouldn't have to special-case the absent column."""
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    # Insert a row at 0013 (no tags column yet).
    with at_0013.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO tasks (id, checksum, config) VALUES ('legacy-task', '00', '{}')",
            )
        )
    command.upgrade(cfg, "0014")
    with at_0013.begin() as conn:
        row = conn.execute(
            text(
                "SELECT tags FROM tasks WHERE id='legacy-task'",
            )
        ).fetchone()
    assert row is not None
    assert row[0] == {}


def test_0014_downgrade_restores_pre_0014_shape(
    at_0013: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0014")
    command.downgrade(cfg, "0013")
    insp = inspect(at_0013)
    task_cols = {c["name"] for c in insp.get_columns("tasks")}
    bench_cols = {c["name"] for c in insp.get_columns("benchmarks")}
    assert "tags" not in task_cols
    assert "series" not in bench_cols
