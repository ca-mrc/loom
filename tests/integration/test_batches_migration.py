"""Migrations 0007 + 0011 — campaigns table created in 0007,
renamed to batches in 0011 (Plan 19 Task 1 + Plan 28 rename).

These tests pin BOTH revisions of the schema so a future
contributor who collapses the two migrations into one breaks the
historical-step expectation explicitly.
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
def at_revision_0006(
    isolated_migration_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back this test's disposable database to pre-Plan-19.

    Older migration tests used to depend on `LOOM_DB_URL` in the process
    environment. Keep this pin here so any legacy env-only path still points at
    the live session container while Alembic now prefers the explicit Config URL.
    """
    monkeypatch.setenv("LOOM_DB_URL", isolated_migration_postgres_url)
    cfg = _cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0006")


def test_0007_creates_campaigns_table(
    isolated_migration_postgres_url: str,
    at_revision_0006: None,
) -> None:
    """Migration 0007 historically created the `campaigns` table
    (pre-rename). It's a historical fact pinned here so we can
    detect anyone who edits 0007 to short-circuit the rename."""
    cfg = _cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0007")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    assert "campaigns" in insp.get_table_names()
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "campaign_id" in trial_cols
    assert "idempotency_key" in trial_cols
    indexes = {i["name"] for i in insp.get_indexes("trials")}
    assert "trials_campaign_idx" in indexes
    assert "trials_idempotency_key_uidx" in indexes
    uniq = [i for i in insp.get_indexes("trials") if i["name"] == "trials_idempotency_key_uidx"]
    assert uniq and uniq[0]["unique"] is True
    engine.dispose()


def test_0007_downgrade_drops_campaigns_table(
    isolated_migration_postgres_url: str,
    at_revision_0006: None,
) -> None:
    cfg = _cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0007")
    command.downgrade(cfg, "0006")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    assert "campaigns" not in insp.get_table_names()
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "campaign_id" not in trial_cols
    assert "idempotency_key" not in trial_cols
    engine.dispose()


def test_0011_renames_campaigns_to_batches(
    isolated_migration_postgres_url: str,
    at_revision_0006: None,
) -> None:
    """Migration 0011 is the rename. After it runs, the schema
    speaks Batch (table + trials.batch_id + indexes named with
    `batches_` / `trials_batch_`)."""
    cfg = _cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0011")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    tables = insp.get_table_names()
    assert "batches" in tables
    assert "campaigns" not in tables
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "batch_id" in trial_cols
    assert "campaign_id" not in trial_cols
    indexes = {i["name"] for i in insp.get_indexes("trials")}
    assert "trials_batch_idx" in indexes
    assert "trials_campaign_idx" not in indexes
    engine.dispose()


def test_0011_downgrade_reverts_to_campaigns(
    isolated_migration_postgres_url: str,
    at_revision_0006: None,
) -> None:
    cfg = _cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0011")
    command.downgrade(cfg, "0010")
    engine = create_engine(isolated_migration_postgres_url)
    insp = inspect(engine)
    tables = insp.get_table_names()
    assert "campaigns" in tables
    assert "batches" not in tables
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "campaign_id" in trial_cols
    assert "batch_id" not in trial_cols
    engine.dispose()
