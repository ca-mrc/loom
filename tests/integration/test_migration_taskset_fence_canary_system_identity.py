"""Migration 0065 reserves the deployment-only TaskSet canary identity."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

_SYSTEM_CANARY_TEAM_ID = "2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9"
_SYSTEM_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"


def _cfg(postgres_url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "database" / "migrations" / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture(scope="module")
def db_url_before_0064_fixture() -> str | None:
    """Capture, but never prescribe, the suite's existing DB target."""
    return os.environ.get("LOOM_DB_URL")


@pytest.fixture(scope="module")
def postgres_url_at_0064(
    db_url_before_0064_fixture: str | None,
) -> Iterator[str]:
    _ = db_url_before_0064_fixture
    with PostgresContainer("postgres:16") as pg:
        postgres_url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        command.upgrade(_cfg(postgres_url), "0064")
        yield postgres_url


def test_0064_fixture_does_not_rebind_the_shared_integration_database(
    db_url_before_0064_fixture: str | None,
    postgres_url_at_0064: str,
) -> None:
    """A historical migration fixture cannot redirect later integration tests."""
    assert postgres_url_at_0064
    assert os.environ.get("LOOM_DB_URL") == db_url_before_0064_fixture


def test_pristine_system_identity_downgrade_restores_0064_and_allows_reupgrade(
    postgres_url_at_0064: str,
) -> None:
    """Historical migration tests can safely return a pristine DB to 0064."""
    cfg = _cfg(postgres_url_at_0064)
    engine = create_engine(postgres_url_at_0064)
    try:
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "0064")
        with engine.connect() as conn:
            team = conn.execute(
                text("SELECT 1 FROM teams WHERE id = CAST(:team_id AS uuid)"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).scalar_one_or_none()
            image_tag_constraint = conn.execute(
                text(
                    "SELECT pg_get_constraintdef(oid) "
                    "FROM pg_constraint "
                    "WHERE conname = :name",
                ),
                {"name": "task_set_fence_canary_authorizations_image_tag_check"},
            ).scalar_one()
        assert team is None
        assert "staging-[0-9a-f]{7}" in image_tag_constraint
        assert "staging(-[a-z0-9]" not in image_tag_constraint

        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            team = conn.execute(
                text("SELECT name FROM teams WHERE id = CAST(:team_id AS uuid)"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).scalar_one()
        assert team == _SYSTEM_CANARY_TEAM_NAME
    finally:
        engine.dispose()


def test_downgrade_refuses_to_delete_a_referenced_system_identity(
    isolated_migration_postgres_url: str,
) -> None:
    """Downgrade never cascades a real deployment canary's TaskSet data."""
    postgres_url = isolated_migration_postgres_url
    cfg = _cfg(postgres_url)
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO task_sets ("
                    "id, owning_team_id, slug, display_name, status, intents, manifest_blob_uri"
                    ") VALUES ("
                    "'ts/' || :team_id || '/downgrade-guard', "
                    "CAST(:team_id AS uuid), 'downgrade-guard', 'downgrade guard', "
                    "'ready', ARRAY['evaluation'], 's3://guard/manifest'"
                    ")",
                ),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            )

        with pytest.raises(Exception, match=r"cannot downgrade.*references"):
            command.downgrade(cfg, "0064")
    finally:
        engine.dispose()


def test_downgrade_refuses_to_delete_an_altered_system_identity(
    isolated_migration_postgres_url: str,
) -> None:
    """A paused Team or modified quota is deployment data, not migration state."""
    postgres_url = isolated_migration_postgres_url
    cfg = _cfg(postgres_url)
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE teams "
                    "SET disabled_reason = 'operator change', "
                    "submissions_paused_at = now(), "
                    "submissions_paused_reason = 'operator change' "
                    "WHERE id = CAST(:team_id AS uuid)"
                ),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            )
            conn.execute(
                text(
                    "UPDATE team_quotas "
                    "SET fair_share_weight = 2.0, max_attempts_ceiling = 4, "
                    "taskset_max_count = 1, taskset_max_storage_bytes = 1, "
                    "allow_private_endpoints = true "
                    "WHERE team_id = CAST(:team_id AS uuid)"
                ),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            )

        with pytest.raises(Exception, match=r"cannot downgrade.*altered"):
            command.downgrade(cfg, "0064")
    finally:
        engine.dispose()


def test_downgrade_is_reversible_when_the_system_identity_is_already_missing(
    isolated_migration_postgres_url: str,
) -> None:
    """A missing migration-owned Team is not adopted or treated as user data."""
    postgres_url = isolated_migration_postgres_url
    cfg = _cfg(postgres_url)
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM team_quotas WHERE team_id = CAST(:team_id AS uuid)"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            )
            conn.execute(
                text("DELETE FROM teams WHERE id = CAST(:team_id AS uuid)"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            )

        command.downgrade(cfg, "0064")
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            team = conn.execute(
                text("SELECT name FROM teams WHERE id = CAST(:team_id AS uuid)"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).scalar_one()
        assert team == _SYSTEM_CANARY_TEAM_NAME
    finally:
        engine.dispose()


def test_upgrade_reserves_the_fixed_canary_team_and_quota(
    postgres_url_at_0064: str,
) -> None:
    """The canary identity is independent of a mutable ``admin`` Team."""
    cfg = _cfg(postgres_url_at_0064)
    engine = create_engine(postgres_url_at_0064)
    try:
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            team = conn.execute(
                text("SELECT id::text, name, disabled_at FROM teams WHERE id = :team_id"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).one_or_none()
            quota_team_id = conn.execute(
                text("SELECT team_id::text FROM team_quotas WHERE team_id = :team_id"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).scalar_one_or_none()
    finally:
        engine.dispose()

    assert team == (_SYSTEM_CANARY_TEAM_ID, _SYSTEM_CANARY_TEAM_NAME, None)
    assert quota_team_id == _SYSTEM_CANARY_TEAM_ID


@pytest.mark.parametrize(
    ("team_id", "name"),
    [
        (_SYSTEM_CANARY_TEAM_ID, _SYSTEM_CANARY_TEAM_NAME),
        (_SYSTEM_CANARY_TEAM_ID, "unexpected-system-team"),
        ("9e0a00da-8f35-4ee5-a98d-31338cd52275", _SYSTEM_CANARY_TEAM_NAME),
    ],
)
def test_upgrade_rejects_any_preexisting_canary_identity(
    team_id: str,
    name: str,
) -> None:
    """A release migration must never adopt a possibly user-owned Team."""
    with PostgresContainer("postgres:16") as pg:
        postgres_url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        engine = create_engine(postgres_url)
        try:
            cfg = _cfg(postgres_url)
            command.upgrade(cfg, "0064")
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO teams (id, name) VALUES (CAST(:team_id AS uuid), :name)",
                    ),
                    {
                        "team_id": team_id,
                        "name": name,
                    },
                )

            with pytest.raises(Exception, match="reserved TaskSet fence-canary Team"):
                command.upgrade(cfg, "head")
        finally:
            engine.dispose()
