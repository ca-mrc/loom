"""Migration 0012 — drop workflows table + batches.workflow_id (Plan 28).

After this migration:
- `workflows` table is gone.
- `batches.workflow_id` column is gone.
- Trying to reference either should fail at the DB level.

The downgrade path recreates both (defensive — alembic history
should be symmetric even if no one expects to run it).
"""

from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def _cfg(url: str) -> Config:
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


@pytest.fixture
def at_revision_0011(
    isolated_migration_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back this test's disposable database before the workflow drop."""
    monkeypatch.setenv("LOOM_DB_URL", isolated_migration_postgres_url)
    cfg = _cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0011")


def test_0012_drops_workflows_table_and_batches_workflow_id(
    isolated_migration_postgres_url: str,
    at_revision_0011: None,
) -> None:
    cfg = _cfg(isolated_migration_postgres_url)

    # Sanity-check state at 0011: workflows + workflow_id both exist.
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    assert "workflows" in insp.get_table_names()
    batch_cols = {c["name"] for c in insp.get_columns("batches")}
    assert "workflow_id" in batch_cols
    engine.dispose()

    # Apply 0012.
    command.upgrade(cfg, "0012")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    tables = insp.get_table_names()
    assert "workflows" not in tables
    batch_cols = {c["name"] for c in insp.get_columns("batches")}
    assert "workflow_id" not in batch_cols
    engine.dispose()


def test_0012_downgrade_recreates_workflows_and_workflow_id(
    isolated_migration_postgres_url: str,
    at_revision_0011: None,
) -> None:
    cfg = _cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0012")
    command.downgrade(cfg, "0011")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    assert "workflows" in insp.get_table_names()
    batch_cols = {c["name"] for c in insp.get_columns("batches")}
    assert "workflow_id" in batch_cols
    engine.dispose()
