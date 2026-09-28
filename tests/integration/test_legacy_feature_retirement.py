"""Forward retirement preserves data, refuses unknown dependencies and rolls back."""

from __future__ import annotations

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from scripts.ops.inventory_legacy_structures import CANDIDATES
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError

RETAINED = {
    "task_image_build_grants", "task_image_build_containment_attestations",
    "task_image_materialization_operation_events",
    "task_image_build_grant_events", "task_image_build_projection_events",
}
RETIRED = set(CANDIDATES) - RETAINED


def _config(url: str) -> Config:
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def test_fresh_head_has_only_retained_candidates_and_reversible_empty_retirement(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    cfg = _config(isolated_migration_postgres_url)
    try:
        assert set(inspect(engine).get_table_names()) & set(CANDIDATES) == RETAINED
        command.downgrade(cfg, "0166")
        assert set(CANDIDATES) <= set(inspect(engine).get_table_names())
        before = {}
        with engine.begin() as connection:
            team, trial = uuid4(), uuid4()
            connection.execute(text("INSERT INTO teams (id, name) VALUES (:id, 'retirement-history')"), {"id": team})
            connection.execute(text("INSERT INTO tasks (id, checksum, config) VALUES ('retirement-task', :checksum, '{}')"), {"checksum": "a" * 64})
            connection.execute(text("""
                INSERT INTO trials (id, team_id, task_id, config, requires_caps, state, result)
                VALUES (:id, :team, 'retirement-task', '{}', '{}', 'succeeded', '{}')
            """), {"id": trial, "team": team})
            for name in ("teams", "tasks", "trials"):
                before[name] = connection.execute(text(f"SELECT to_jsonb(t) FROM {name} t")).scalars().all()
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            for name, rows in before.items():
                assert connection.execute(text(f"SELECT to_jsonb(t) FROM {name} t")).scalars().all() == rows
        assert set(inspect(engine).get_table_names()) & set(CANDIDATES) == RETAINED
        command.downgrade(cfg, "0166")
        command.upgrade(cfg, "head")
    finally:
        engine.dispose()


def test_retirement_refuses_rows_and_preserves_the_entire_old_schema(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    cfg = _config(isolated_migration_postgres_url)
    try:
        command.downgrade(cfg, "0166")
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO gb10_worker_pool_desired_states
                    (environment, pool_name, image_tag, max_concurrent, env_config_version)
                VALUES ('development', 'retained-pool', 'retained', 1, 'retained')
            """))
        with pytest.raises(RuntimeError, match="retained-row disposition"):
            command.upgrade(cfg, "head")
        assert set(CANDIDATES) <= set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "0166"
            assert connection.execute(text("SELECT count(*) FROM gb10_worker_pool_desired_states")).scalar_one() == 1
    finally:
        engine.dispose()


def test_retirement_refuses_external_view_dependency_without_cascade(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    cfg = _config(isolated_migration_postgres_url)
    try:
        command.downgrade(cfg, "0166")
        with engine.begin() as connection:
            connection.execute(text("CREATE VIEW retained_external_view AS SELECT * FROM dev_instances"))
        with pytest.raises(DBAPIError, match="depend"):
            command.upgrade(cfg, "head")
        assert set(CANDIDATES) <= set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_downgrade_matches_postgres_dump_restore_of_the_original_catalog() -> None:
    import psycopg
    from testcontainers.postgres import PostgresContainer

    from loom.application_schema_inventory import read_application_schema_inventory

    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")
        cfg = _config(url)
        command.upgrade(cfg, "0166")
        dsn = url.replace("postgresql+psycopg://", "postgresql://")
        # pg_dump round-trips varchar-array casts into equivalent per-element
        # text casts. Compare against PostgreSQL's own restored catalog rather
        # than requiring identical pre-dump parse trees for those expressions.
        dumped = pg.get_wrapped_container().exec_run([
            "pg_dump", "-U", "test", "-d", "test", "--schema-only", "--no-owner",
        ])
        assert dumped.exit_code == 0
        sql = "\n".join(
            line for line in dumped.output.decode().splitlines()
            if not line.startswith(("\\restrict", "\\unrestrict"))
        )
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute("CREATE DATABASE restore_reference TEMPLATE template0")
        reference_dsn = dsn.rsplit("/", 1)[0] + "/restore_reference"
        with psycopg.connect(reference_dsn) as connection:
            connection.execute(sql)
        cfg = _config(reference_dsn.replace("postgresql://", "postgresql+psycopg://"))
        command.stamp(cfg, "0166")
        with psycopg.connect(reference_dsn) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            before = read_application_schema_inventory(connection, role_bindings={"test": "application-owner"})
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "0166")
        with psycopg.connect(reference_dsn) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            after = read_application_schema_inventory(connection, role_bindings={"test": "application-owner"})
        assert after.differences_from(before) == ()


def test_retirement_refuses_dynamic_sql_dependencies(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    cfg = _config(isolated_migration_postgres_url)
    try:
        command.downgrade(cfg, "0166")
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE FUNCTION external_legacy_reader() RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE n bigint;
                BEGIN SELECT count(*) INTO n FROM dev_instances; RETURN n; END $$
            """))
        with pytest.raises(RuntimeError, match="dependent SQL routine disposition"):
            command.upgrade(cfg, "head")
        assert set(CANDIDATES) <= set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
