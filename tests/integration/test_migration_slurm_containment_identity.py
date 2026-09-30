from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, inspect

_ADDED_COLUMNS = {
    "requested_pids",
    "requested_gpu_tres",
    "requested_gpus",
    "sandbox_identity",
    "candidate_sha",
    "compose_project",
}
_ADDED_INDEX = "slurm_worker_jobs_sandbox_candidate_state_idx"


def _config(database_url: str) -> AlembicConfig:
    repo_root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(repo_root / "database" / "migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(repo_root / "database" / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _schema_state(database_url: str) -> tuple[set[str], set[str]]:
    engine = create_engine(database_url)
    try:
        inspector = inspect(engine)
        columns = {
            item["name"] for item in inspector.get_columns("slurm_worker_jobs")
        }
        indexes = {
            item["name"] for item in inspector.get_indexes("slurm_worker_jobs")
        }
        return columns, indexes
    finally:
        engine.dispose()


def test_slurm_containment_migration_downgrade_and_reupgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    # Qualify the historical Slurm schema before its explicit 0167 retirement.
    command.downgrade(config, "0074")

    columns, indexes = _schema_state(isolated_migration_postgres_url)
    assert _ADDED_COLUMNS <= columns
    assert _ADDED_INDEX in indexes

    command.downgrade(config, "0073")
    columns, indexes = _schema_state(isolated_migration_postgres_url)
    assert _ADDED_COLUMNS.isdisjoint(columns)
    assert _ADDED_INDEX not in indexes

    command.upgrade(config, "0074")
    columns, indexes = _schema_state(isolated_migration_postgres_url)
    assert _ADDED_COLUMNS <= columns
    assert _ADDED_INDEX in indexes
