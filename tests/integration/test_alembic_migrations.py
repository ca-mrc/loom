"""Verify published pre-retirement migrations and their retained data contracts.

This historical suite ends at 0166. The current head and retirement behavior are
verified separately in test_legacy_feature_retirement.py.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import CheckConstraint, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.db.schema import (
    Team,
    User,
)
from tests.support.legacy_personal_dev_schema import DevInstance, PersonalDevCandidate


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[2]
        # Use the venv's alembic via `python -m alembic` so PATH doesn't matter.
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "0120"],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "downgrade", "0081"],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "0166"],
            cwd=repo_root,
            check=True,
        )
        yield url


async def test_0122_downgrade_retains_repaired_constraint(postgres_url: str) -> None:
    """Re-upgrade repairs nullable coordinates and preserves the registry constraint."""
    repo_root = Path(__file__).resolve().parents[2]
    try:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "downgrade", "0120"],
            cwd=repo_root,
            check=True,
        )

        engine = create_engine(postgres_url)
        try:
            with engine.connect() as connection:
                revision = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
            assert revision == "0120"
        finally:
            engine.dispose()

        engine = create_async_engine(postgres_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        owner_id = uuid4()
        team_id = uuid4()
        candidate_id = uuid4()
        coordinate_name = "coordinate-repair"
        now = datetime.now(UTC)
        candidate = PersonalDevCandidate(
            id=candidate_id,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_sha="1" * 64,
            source_sha256="2" * 64,
            archive_sha256="3" * 64,
            build_contract_sha256="4" * 64,
            source_commit="5" * 40,
            dirty=True,
            manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{team_id}/{owner_id}/{'1' * 64}/{candidate_id}/{'3' * 64}.tar"
            ),
            source_generation_id=candidate_id,
            archive_size_bytes=10240,
            status="uploaded",
            created_at=now,
            updated_at=now,
        )
        try:
            async with sessions() as session:
                session.add(Team(id=team_id, name=f"downgrade-registry-{team_id}"))
                session.add(
                    User(
                        id=owner_id,
                        email=f"{owner_id}@example.test",
                        username=f"downgrade-registry-{owner_id}",
                        username_normalized=f"downgrade-registry-{owner_id}",
                        status="active",
                    )
                )
                await session.commit()

            async with sessions() as session:
                session.add(candidate)
                await session.commit()

            async with sessions() as session:
                await session.execute(
                    text(
                        "UPDATE personal_dev_candidates "
                        "SET registry_prefix = :registry_prefix WHERE id = :candidate_id"
                    ),
                    {"registry_prefix": "r" * 309, "candidate_id": candidate_id},
                )
                await session.commit()

            async with sessions() as session:
                with pytest.raises(DBAPIError) as exc_info:
                    await session.execute(
                        text(
                            "UPDATE personal_dev_candidates "
                            "SET registry_prefix = :registry_prefix WHERE id = :candidate_id"
                        ),
                        {"registry_prefix": "r" * 310, "candidate_id": candidate_id},
                    )
                assert exc_info.value.orig.sqlstate == "23514"
                await session.rollback()

            async with sessions() as session:
                await session.execute(
                    text(
                        "INSERT INTO dev_instances "
                        "(name, owner_user_id, owner_team_id, max_slots, "
                        "deployment_generation, candidate_id, candidate_sha, operation_id) "
                        "VALUES (:name, :owner_id, :team_id, 1, 1, :candidate_id, "
                        ":candidate_sha, :operation_id)"
                    ),
                    {
                        "name": coordinate_name,
                        "owner_id": owner_id,
                        "team_id": team_id,
                        "candidate_id": candidate_id,
                        "candidate_sha": candidate.candidate_sha,
                        "operation_id": uuid4(),
                    },
                )
                await session.commit()
        finally:
            await engine.dispose()

        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "0166"],
            cwd=repo_root,
            check=True,
        )
        engine = create_engine(postgres_url)
        try:
            with engine.connect() as connection:
                revision = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                coordinates = connection.execute(
                    text(
                        "SELECT capacity_namespace, capacity_database "
                        "FROM dev_instances WHERE name = :name"
                    ),
                    {"name": coordinate_name},
                ).one()
            assert revision == "0166"
            assert tuple(coordinates) == (
                f"loom-dev-{coordinate_name}",
                "loom_dev_coordinate_repair",
            )
        finally:
            engine.dispose()
    finally:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "0166"],
            cwd=repo_root,
            check=True,
        )


def test_all_tables_exist(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public'
        """)
        )
        names = {row[0] for row in result}
    expected = {
        "teams",
        "team_quotas",
        "tasks",
        "agents",
        "workers",
        "trials",
        "task_image_materializations",
        "task_image_materialization_attempts",
        "task_image_publication_evidence",
        "trial_task_image_materializations",
        "trial_resource_usage",
        "execution_classes",
        "execution_targets",
        "execution_leases",
        "execution_commands",
        "execution_events",
        "execution_lease_history",
        "execution_admission_policies",
        "execution_admission_reservations",
        "execution_price_snapshots",
        "execution_target_price_bindings",
        "execution_budget_policies",
        "execution_cost_reservations",
        "execution_cost_reservation_debits",
        "execution_node_cost_records",
        "execution_node_cost_allocations",
        "execution_capacity_policies",
        "execution_capacity_observations",
        "execution_resource_calibrations",
        "execution_resource_profile_bindings",
        "execution_provisioning_authorizations",
        "tokens",
        "rate_cards",
        "llm_calls",
        "benchmarks",
        "pending_team_registrations",
        "slurm_worker_jobs",
        "gb10_worker_pool_desired_states",
        "gb10_worker_node_statuses",
        "worker_pool_autoscaler_policies",
        "dev_instances",
        "dev_lifecycle_operations",
        "dev_lifecycle_operation_attempts",
        "dev_lifecycle_activation_acknowledgements",
        "personal_dev_candidates",
        "personal_dev_candidate_artifact_collections",
        "personal_dev_candidate_build_attempts",
        "artifacts",
        "artifact_lineage_edges",
        "pipeline_runs",
        "pipeline_stage_runs",
        "pipeline_stage_dependencies",
        "pipeline_fanout_expansions",
        "execution_attempts",
        "pipeline_events",
        "pipeline_terminal_snapshots",
        "pipeline_acceptance_preflight_prerequisites",
        "pipeline_budget_ledgers",
        "pipeline_budget_reservations",
        "execution_attempt_provider_budgets",
        "pipeline_cancellation_outbox",
        "alembic_version",
    }
    assert expected.issubset(names)
    native = next(column for column in inspect(engine).get_columns("task_image_materialization_attempts")
                  if column["name"] == "native_build")
    assert native["nullable"] and str(native["type"]) == "JSONB"


def test_in_flight_count_trigger(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    task_id = "demo"
    trial_id = uuid4()
    worker_id = uuid4()

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": "test"},
        )
        conn.execute(text("INSERT INTO team_quotas (team_id) VALUES (:tid)"), {"tid": team_id})
        conn.execute(
            text("INSERT INTO tasks (id, checksum, config) VALUES (:i, :c, '{}'::jsonb)"),
            {"i": task_id, "c": "0" * 64},
        )
        conn.execute(
            text(
                "INSERT INTO workers (id, hostname, version, capabilities, "
                "registered_at, last_seen_at, status) VALUES "
                "(:id, 'h', 'v', '[]'::jsonb, :now, :now, 'active')"
            ),
            {"id": worker_id, "now": datetime.now(UTC)},
        )
        conn.execute(
            text(
                "INSERT INTO trials (id, team_id, task_id, config, requires_caps, state) "
                "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued')"
            ),
            {"id": trial_id, "t": team_id, "ti": task_id},
        )

    def in_flight() -> int:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT in_flight_count FROM team_quotas WHERE team_id = :t"), {"t": team_id}
            ).scalar_one()

    assert in_flight() == 0

    # queued → claimed: +1
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state='claimed', worker_id=:w WHERE id=:id"),
            {"w": worker_id, "id": trial_id},
        )
    assert in_flight() == 1

    # claimed → running: 0 (both active)
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='running' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 1

    # A nonterminal retry releases capacity, then claims it again.
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='queued' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 0
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='claimed' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 1
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='running' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 1

    # running → succeeded: -1
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state='succeeded', result='{}'::jsonb WHERE id=:id"),
            {"id": trial_id},
        )
    assert in_flight() == 0

    # Terminal evidence cannot be reopened, and rejection must not change capacity.
    with pytest.raises(DBAPIError, match="terminal trial cannot become nonterminal"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE trials SET state='queued' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 0


def test_in_flight_count_trigger_is_safe_under_locked_search_path(
    postgres_url: str,
) -> None:
    """Security-definer callers keep pg_catalog-only name resolution."""

    engine = create_engine(postgres_url)
    team_id = uuid4()
    task_id = f"locked-search-path-{uuid4().hex}"
    trial_id = uuid4()
    worker_id = uuid4()
    now = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": f"locked-search-path-{team_id}"},
        )
        connection.execute(
            text("INSERT INTO team_quotas (team_id) VALUES (:team_id)"),
            {"team_id": team_id},
        )
        connection.execute(
            text("INSERT INTO tasks (id, checksum, config) VALUES (:id, :checksum, '{}'::jsonb)"),
            {"id": task_id, "checksum": "a" * 64},
        )
        connection.execute(
            text(
                "INSERT INTO workers (id, hostname, version, capabilities, registered_at, "
                "last_seen_at, status) VALUES "
                "(:id, 'locked-search-path', 'test', '[]'::jsonb, :now, :now, 'active')"
            ),
            {"id": worker_id, "now": now},
        )
        connection.execute(
            text(
                "INSERT INTO trials (id, team_id, task_id, config, requires_caps, state) "
                "VALUES (:id, :team_id, :task_id, '{}'::jsonb, '{}'::jsonb, 'queued')"
            ),
            {"id": trial_id, "team_id": team_id, "task_id": task_id},
        )
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL search_path = pg_catalog"))
        connection.execute(
            text(
                "UPDATE public.trials SET state='claimed', worker_id=:worker_id WHERE id=:trial_id"
            ),
            {"worker_id": worker_id, "trial_id": trial_id},
        )
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT in_flight_count FROM team_quotas WHERE team_id=:team_id"),
                {"team_id": team_id},
            ).scalar_one()
            == 1
        )
    engine.dispose()


@pytest.mark.parametrize(
    ("capacity_namespace", "capacity_database"),
    (
        pytest.param(None, None, id="missing"),
        pytest.param("loom-dev-other", "loom_dev_other", id="mismatched"),
    ),
)
@pytest.mark.legacy_pool
def test_dev_instance_capacity_coordinates_are_derived_from_personal_name(
    postgres_url: str,
    capacity_namespace: str | None,
    capacity_database: str | None,
) -> None:
    """Direct SQL cannot omit or misbind coordinates for a personal environment."""

    engine = create_engine(postgres_url)
    user_id = uuid4()
    team_id = uuid4()
    candidate_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO users (id, username, username_normalized) "
                    "VALUES (:id, 'coordinate-user', 'coordinate-user')"
                ),
                {"id": user_id},
            )
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, 'coordinate-team')"),
                {"id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO personal_dev_candidates "
                    "(id, owner_user_id, owner_team_id, candidate_sha, source_sha256, "
                    "archive_sha256, build_contract_sha256, source_commit, dirty, "
                    "manifest_json, object_bucket, object_key, source_generation_id, "
                    "archive_size_bytes) VALUES "
                    "(:candidate_id, :user_id, :team_id, repeat('a', 64), repeat('b', 64), "
                    "repeat('c', 64), repeat('d', 64), repeat('e', 40), false, "
                    "'{}'::jsonb, 'personal-dev-sources', "
                    "'personal-dev/sources/' || :team_text || '/' || :user_text || '/' || "
                    "repeat('a', 64) || '/' || repeat('c', 64) || '.tar', "
                    ":candidate_id, 1)"
                ),
                {
                    "candidate_id": candidate_id,
                    "user_id": user_id,
                    "team_id": team_id,
                    "user_text": str(user_id),
                    "team_text": str(team_id),
                },
            )
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "INSERT INTO dev_instances "
                        "(name, owner_user_id, owner_team_id, max_slots, "
                        "deployment_generation, candidate_id, candidate_sha, "
                        "capacity_namespace, capacity_database, operation_id) "
                        "VALUES ('alice', :user_id, :team_id, 1, 1, :candidate_id, "
                        "repeat('a', 64), :capacity_namespace, :capacity_database, "
                        ":operation_id)"
                    ),
                    {
                        "user_id": user_id,
                        "team_id": team_id,
                        "candidate_id": candidate_id,
                        "capacity_namespace": capacity_namespace,
                        "capacity_database": capacity_database,
                        "operation_id": uuid4(),
                    },
                )
    finally:
        engine.dispose()


@pytest.mark.legacy_pool
def test_dev_instance_capacity_coordinate_constraint_matches_model_and_migration(
    postgres_url: str,
) -> None:
    """The current fail-closed coordinate rule must match ORM and database."""

    expected_model_sql = (
        "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
        "OR (candidate_id IS NOT NULL AND capacity_namespace IS NOT NULL "
        "AND capacity_database IS NOT NULL "
        "AND capacity_namespace = 'loom-dev-' || name "
        "AND capacity_database = CASE WHEN storage_binding IS NULL "
        "THEN 'loom_dev_' || replace(name, '-', '_') "
        "ELSE 'ld_' || replace(name, '-', '_') || '_' || "
        "replace(subject_incarnation::text, '-', '') END)"
    )
    model_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in DevInstance.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert model_checks["dev_instances_personal_capacity_identity_check"] == expected_model_sql

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            database_checks = {
                constraint["name"]: constraint["sqltext"]
                for constraint in inspect(connection).get_check_constraints("dev_instances")
            }
    finally:
        engine.dispose()

    normalized_database_sql = (
        " ".join(database_checks["dev_instances_personal_capacity_identity_check"].lower().split())
        .replace("( ", "(")
        .replace(" )", ")")
    )
    assert "candidate_id is null" in normalized_database_sql
    assert "capacity_namespace is null" in normalized_database_sql
    assert "capacity_database is null" in normalized_database_sql
    assert "candidate_id is not null" in normalized_database_sql
    assert "capacity_namespace is not null" in normalized_database_sql
    assert "capacity_database is not null" in normalized_database_sql
    assert "capacity_namespace = ('loom-dev-'::text || name)" in normalized_database_sql
    assert (
        "capacity_database = case when storage_binding is null "
        "then 'loom_dev_'::text || replace(name, '-'::text, '_'::text) "
        "else (('ld_'::text || replace(name, '-'::text, '_'::text)) || '_'::text) "
        "|| replace(subject_incarnation::text, '-'::text, ''::text) end"
        in normalized_database_sql
    ), normalized_database_sql
