"""Integration test for 0073 — the renumbered-lineage reconciliation (#949).

Simulates a pre-#857 staging database: it carries the full staging-lifecycle
content but is MISSING the three inserted migrations' content (0062 benchmark
profiles, 0066 autoscaler prod-pressure, 0067 gb10 pool rename) and is stamped
at 0072. Proves that stamping such a DB to 0072 and upgrading through 0166
(which includes 0073) heals it into the pre-retirement shape, and that a
second run is a guarded no-op.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

pytestmark = pytest.mark.integration


def _cfg(url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "database" / "migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "database" / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


_INSERTED_COLUMNS = {
    "benchmarks": ("execution_state", "profile_provenance"),
    "tasks": ("source_provenance",),
    "batches": ("resolved_task_ids",),
    "worker_pool_autoscaler_policies": ("prod_pressure_state",),
}


def _strip_inserted_migration_content(engine: object) -> None:
    """Revert a head DB to a pre-renumber shape: drop 0062/0066 artifacts and
    re-introduce a legacy ``gb10-arm64`` pool row for 0067 to rename."""
    with engine.begin() as conn:  # type: ignore[attr-defined]
        conn.execute(text("DROP TABLE IF EXISTS benchmark_aliases"))
        for table, columns in _INSERTED_COLUMNS.items():
            for column in columns:
                conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))
        # A pre-rename pool row that 0067's rename must move to 'gb10'.
        conn.execute(
            text(
                "INSERT INTO workers "
                "(id, hostname, version, capabilities, registered_at, "
                " last_seen_at, status, pool_name) "
                "VALUES (gen_random_uuid(), 'recon-probe', 'v', '{}'::jsonb, "
                " now(), now(), 'idle', 'gb10-arm64')"
            )
        )


def test_0073_reconciles_a_diverged_pre_renumber_database(
    isolated_migration_postgres_url: str,
) -> None:
    url = isolated_migration_postgres_url  # fresh DB already at current head
    cfg = _cfg(url)
    engine = create_engine(url)

    # Fresh head as the reference shape.
    fresh = inspect(engine)
    fresh_batches = {c["name"] for c in fresh.get_columns("batches")}
    assert "resolved_task_ids" in fresh_batches

    # Return the fresh database to the exact pre-0074 shape before simulating
    # the diverged staging lineage. A stamp alone changes only Alembic metadata
    # and would leave later physical columns behind.
    command.downgrade(cfg, "0073")

    # Simulate the diverged staging DB, then stamp it back to 0072.
    _strip_inserted_migration_content(engine)
    stripped = inspect(engine)
    assert not stripped.has_table("benchmark_aliases")
    assert "execution_state" not in {c["name"] for c in stripped.get_columns("benchmarks")}
    command.stamp(cfg, "0072")

    # Reconcile: upgrade head includes 0073 before later migrations.
    command.upgrade(cfg, "0166")

    healed = inspect(engine)
    for table, columns in _INSERTED_COLUMNS.items():
        present = {c["name"] for c in healed.get_columns(table)}
        for column in columns:
            assert column in present, f"{table}.{column} not restored by 0073"
    assert healed.has_table("benchmark_aliases")
    with engine.connect() as conn:
        probe_pool = conn.execute(
            text("SELECT pool_name FROM workers WHERE hostname = 'recon-probe'")
        ).scalar()
    assert probe_pool == "gb10", "0073 must apply the gb10-arm64 -> gb10 rename"

    # Idempotency: remove the later 0074 schema, then replay the 0073 reconcile
    # from the exact historical shape. The 0073 pass is a guarded no-op and
    # 0074 reapplies once from its real predecessor.
    command.downgrade(cfg, "0073")
    command.stamp(cfg, "0072")
    command.upgrade(cfg, "0166")
    again = inspect(engine)
    assert again.has_table("benchmark_aliases")
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT pool_name FROM workers WHERE hostname = 'recon-probe'")
            ).scalar()
            == "gb10"
        )

    # Reconciliation must also lead to the current head, where only the obsolete
    # pool policy table retires. Retained benchmark and Worker history survives.
    command.upgrade(cfg, "head")
    assert not inspect(engine).has_table("worker_pool_autoscaler_policies")
    assert inspect(engine).has_table("benchmark_aliases")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT pool_name FROM workers WHERE hostname='recon-probe'")).scalar_one() == "gb10"
    engine.dispose()
