"""Exercise the historical Nebius fork on real, disposable PostgreSQL."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from database.migrations.nebius_lineage import convert_lineage, inspect_lineage
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import DBAPIError

from loom.db.schema_startup import service_schema_head


def _scripts() -> ScriptDirectory:
    return ScriptDirectory.from_config(Config("database/migrations/alembic.ini"))


def _historical(url: str, revision: str) -> None:
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.downgrade(config, "0132")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            # These are the integrated copies of Nebius 0133–0135 at
            # 7912ec076babc2fe51768fd2596b70c7de9e3d31. Shared history ends at 0132.
            # The later native-usage 0136 is included in deployed d07718e2.
            with Operations.context(MigrationContext.configure(connection)):
                for number in (144, 145, 146, 150)[: int(revision) - 132]:
                    script = _scripts().get_revision(f"{number:04}")
                    assert script is not None
                    script.module.upgrade()
            connection.execute(
                text("UPDATE alembic_version SET version_num=:rev"), {"rev": revision}
            )
    finally:
        engine.dispose()


def _snapshot(connection: Connection) -> dict[str, list[dict[str, Any]]]:
    return {
        table: list(
            connection.exec_driver_sql(f"SELECT to_jsonb(t) FROM {table} t ORDER BY id").scalars()
        )
        for table in (
            "trials",
            "llm_calls",
            "task_image_materialization_attempts",
            "execution_capacity_observations",
        )
    }


def _schema(connection: Connection) -> list[tuple[Any, ...]]:
    # Compare logical schema, including all constraints, indexes and triggers;
    # physical column positions/OIDs differ legitimately between fork histories.
    return [
        tuple(row)
        for row in connection.exec_driver_sql("""
        SELECT 'column', c.relname, a.attname,
               concat_ws('|', format_type(a.atttypid,a.atttypmod), a.attnotnull,
                         pg_get_expr(d.adbin,d.adrelid), a.attgenerated)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_attribute a ON a.attrelid=c.oid
        LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE n.nspname='public' AND c.relkind IN ('r','p')
          AND a.attnum>0 AND NOT a.attisdropped
        UNION ALL
        SELECT 'constraint', c.relname, con.conname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
        UNION ALL
        SELECT 'index', tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public'
        UNION ALL
        SELECT 'trigger', c.relname, t.tgname, pg_get_triggerdef(t.oid)
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND NOT t.tgisinternal
        ORDER BY 1,2,3,4
    """)
    ]


@pytest.mark.parametrize("revision", ["0133", "0134", "0135", "0136"])
def test_conversion_preserves_history_and_reaches_dev(
    isolated_migration_postgres_url: str,
    revision: str,
) -> None:
    url = isolated_migration_postgres_url
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            expected_schema = _schema(connection)
        _historical(url, revision)
        with engine.begin() as connection:
            team, trial, materialization, attempt = (uuid4() for _ in range(4))
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team, "name": str(team)},
            )
            connection.execute(
                text("INSERT INTO tasks (id,config,checksum) VALUES ('lineage','{}',:sha)"),
                {"sha": "a" * 64},
            )
            connection.execute(
                text("""
                INSERT INTO trials (id,team_id,task_id,config,requires_caps,state,result,trajectory_index)
                VALUES (:id,:team,'lineage','{}','{}','succeeded',:result,'{"artifacts":["retained"]}')
            """),
                {
                    "id": trial,
                    "team": team,
                    "result": json.dumps(
                        {
                            "schema_version": "loom.service-execution-trial-result.v1",
                            "aggregate_reward": 1.0,
                            "reward": {"passed": 1.0},
                            "runtime_result": {"verifier_rewards": {"passed": 1.0}},
                            "usage": {"total_tokens": 59},
                        }
                    ),
                },
            )
            connection.execute(
                text("""
                INSERT INTO task_image_materializations
                    (id, materialization_key, task_id, task_checksum, cpu_arch, task_config)
                VALUES (:id,:key,'lineage',:sha,'arm64','{}')
            """),
                {"id": materialization, "key": "b" * 64, "sha": "a" * 64},
            )
            connection.execute(
                text("""
                INSERT INTO llm_calls (id,team_id,trial_id,step_id,model,dialect,
                    input_tokens,output_tokens,cost_usd,rate_card_hash)
                VALUES (:id,:team,:trial,'step','retained-model','openai',40,19,0,'retained')
            """),
                {"id": uuid4(), "team": team, "trial": trial},
            )
            connection.execute(
                text("""
                INSERT INTO execution_classes (id,schema_version,spec_json,spec_sha256)
                VALUES ('lineage','loom.execution-class.v1','{}',:sha)
            """),
                {"sha": "sha256:" + "a" * 64},
            )
            connection.execute(
                text("""
                INSERT INTO execution_targets (id,logical_pool_id,execution_class_id,schema_version,
                    spec_json,spec_sha256,environment,provider,region,failure_domain,data_residency)
                VALUES ('lineage','lineage','lineage','loom.execution-target.v1','{}',:sha,
                    'development','nebius','eu-north1','eu-north1','eu')
            """),
                {"sha": "sha256:" + "a" * 64},
            )
            observations = Table(
                "execution_capacity_observations", MetaData(), autoload_with=connection
            )
            values: dict[str, Any] = {
                column.name: (
                    1 if revision == "0133" and column.name.startswith("provider_quota_") else 0
                )
                for column in observations.columns
                if column.type.python_type is int
            }
            values.update(
                target_id="lineage",
                provider="nebius",
                source="lineage",
                source_version="1",
                observed_at=connection.exec_driver_sql("SELECT now()").scalar_one(),
                provider_capacity_state="unknown",
                autoscaler_state="unknown",
                pending_reasons_json=[],
                observation_json={"retained": True},
                observation_sha256="sha256:" + "b" * 64,
            )
            connection.execute(observations.insert().values(**values))
            connection.execute(
                text("""
                INSERT INTO task_image_materialization_attempts
                    (id,materialization_id,attempt_number,lease_epoch,builder_id,claimed_at)
                VALUES (:id,:materialization,1,1,'nebius:retained',now())
            """),
                {"id": attempt, "materialization": materialization},
            )
            if revision in {"0135", "0136"}:
                connection.exec_driver_sql("""UPDATE task_image_materialization_attempts
                    SET native_build='{"job_uid":"retained-native-job","cpu_millis":500}'""")
            before = _snapshot(connection)
            assert inspect_lineage(connection, revision) == revision
        with engine.begin() as connection:
            convert_lineage(connection, _scripts(), revision)
            assert connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one() == "0150"
        # Conversion is pinned to its audited dev checkpoint; normal Alembic
        # migrations advance that lineage to the current release afterward.
        config = Config("database/migrations/alembic.ini")
        config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert _schema(connection) == expected_schema
            after = _snapshot(connection)
            for row in before["trials"]:
                row["legacy_claim_id"] = None
                # 0153 adds nullable diagnostics; historical rows retain their
                # original data and must not acquire invented observations.
                row["scheduling_observation"] = None
                # 0172 preserves unknown historical origin; migration must not
                # promote old work into any environment's scheduling priority.
                row["pool_origin"] = None
            for row in before["llm_calls"]:
                # 0176 adds baseline attribution; historical calls retain their
                # original subject and must not acquire a baseline session.
                row["baseline_session_id"] = None
            if revision not in {"0135", "0136"}:
                for row in before["task_image_materialization_attempts"]:
                    row["native_build"] = None
            assert after == before
            assert (
                connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one()
                == service_schema_head()
            )
            assert connection.exec_driver_sql(
                "SELECT to_regclass('gateway_dispatch_receipts')"
            ).scalar_one()
            assert connection.exec_driver_sql(
                "SELECT to_regclass('task_image_publication_keysets')"
            ).scalar_one()
            # A repeat or wrong-lineage invocation cannot reinterpret dev history.
            with pytest.raises(ValueError, match="revision"):
                inspect_lineage(connection, revision)
        config = Config("database/migrations/alembic.ini")
        config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_conversion_rolls_back_all_ddl_on_midway_failure(
    isolated_migration_postgres_url: str,
) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            # 0136 must reject unexpected provenance, after 0133–0135 have run.
            connection.execute(
                text("""
                INSERT INTO task_image_materializations
                    (id,materialization_key,task_id,task_checksum,cpu_arch,task_config,task_source_provenance)
                VALUES (:id,:key,'lineage-drift',:sha,'arm64','{}',
                        '{"bundle_content_manifest_sha256":"unexpected"}')
            """),
                {"id": uuid4(), "key": "c" * 64, "sha": "d" * 64},
            )
        with pytest.raises(DBAPIError, match="unexpected content-manifest"):
            with engine.begin() as connection:
                convert_lineage(connection, _scripts(), "0135")
        with engine.connect() as connection:
            assert inspect_lineage(connection, "0135") == "0135"
            assert (
                connection.exec_driver_sql(
                    "SELECT to_regclass('gateway_dispatch_receipts')"
                ).scalar_one()
                is None
            )
            assert (
                connection.exec_driver_sql(
                    "SELECT to_regclass('task_image_publication_keys')"
                ).scalar_one()
                is None
            )
    finally:
        engine.dispose()


def test_busy_writer_fails_without_partial_conversion(isolated_migration_postgres_url: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    try:
        with engine.begin() as writer:
            writer.exec_driver_sql("LOCK TABLE trials IN ROW EXCLUSIVE MODE")
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                with engine.begin() as connection:
                    convert_lineage(connection, _scripts(), "0135")
        with engine.connect() as connection:
            assert inspect_lineage(connection, "0135") == "0135"
    finally:
        engine.dispose()


def test_driver_autocommit_is_rejected(isolated_migration_postgres_url: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    try:
        with engine.begin() as connection:
            with pytest.raises(ValueError, match="autocommit"):
                convert_lineage(connection, _scripts(), "0135")
            assert inspect_lineage(connection, "0135") == "0135"
    finally:
        engine.dispose()


def test_job_command_inspects_applies_and_sanitizes_rejection(
    isolated_migration_postgres_url: str,
) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    # The protected Secret uses a plain PostgreSQL URL, not a SQLAlchemy driver name.
    environment = dict(
        os.environ,
        LOOM_DB_URL=make_url(url)
        .set(drivername="postgresql")
        .render_as_string(hide_password=False),
    )
    argv = [sys.executable, "-m", "database.migrations.nebius_lineage", "--expected-revision", "0135"]
    engine = create_engine(url)
    try:
        result = subprocess.run(argv, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["applied"] is False
        with engine.connect() as connection:
            assert inspect_lineage(connection, "0135") == "0135"
        result = subprocess.run(
            [*argv, "--apply"], env=environment, capture_output=True, text=True, timeout=60
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["applied"] is True
        with engine.connect() as connection:
            assert (
                connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one()
                == "0150"
            )
        result = subprocess.run(
            [*argv, "--apply"], env=environment, capture_output=True, text=True, timeout=60
        )
        assert result.returncode == 1
        assert json.loads(result.stdout) == {
            "passed": False,
            "failure": "lineage-conversion-rejected",
        }
        assert result.stderr == ""
        assert environment["LOOM_DB_URL"] not in result.stdout
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "drift", ["native_type", "native_missing", "dev_marker", "quota", "multiple_heads"]
)
def test_conversion_rejects_source_drift(isolated_migration_postgres_url: str, drift: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    changes = {
        "native_type": "ALTER TABLE task_image_materialization_attempts ALTER COLUMN native_build TYPE text",
        "native_missing": "ALTER TABLE task_image_materialization_attempts DROP COLUMN native_build",
        "dev_marker": "CREATE TABLE gateway_dispatch_receipts (id uuid)",
        "quota": "ALTER TABLE execution_capacity_observations DROP CONSTRAINT execution_capacity_observations_quota_check",
        "multiple_heads": "INSERT INTO alembic_version VALUES ('0134')",
    }
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(changes[drift])
        with pytest.raises(ValueError):
            with engine.begin() as connection:
                convert_lineage(connection, _scripts(), "0135")
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "change",
    [
        "ALTER TABLE trial_resource_usage DROP CONSTRAINT trial_resource_usage_authority_check",
        "ALTER TABLE trial_resource_usage ALTER COLUMN pod_uid TYPE varchar(200)",
        "DROP INDEX trial_resource_usage_native_lease_idx",
    ],
)
def test_0136_conversion_rejects_native_usage_drift(
    isolated_migration_postgres_url: str,
    change: str,
) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0136")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(change)
        with pytest.raises(ValueError, match="native resource usage"):
            with engine.begin() as connection:
                convert_lineage(connection, _scripts(), "0136")
        with engine.connect() as connection:
            assert (
                connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one()
                == "0136"
            )
            assert (
                connection.exec_driver_sql(
                    "SELECT to_regclass('public.gateway_dispatch_receipts')"
                ).scalar_one()
                is None
            )
    finally:
        engine.dispose()
