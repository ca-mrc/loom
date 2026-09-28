"""Real PostgreSQL identity/name constraints, independent of legacy DevInstance."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, insert, inspect, select, text, update
from sqlalchemy.exc import IntegrityError

from loom.db.schema_startup import service_schema_head


@pytest.fixture
def environment_database(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


def registration(connection, slug="alice", **changes):
    from loom.db.nebius_environment_schema import NebiusEnvironment
    from loom.db.schema import Team, User

    identity, owner, team = uuid4(), uuid4(), uuid4()
    connection.execute(insert(Team).values(id=team, name=str(team)))
    connection.execute(insert(User).values(id=owner, username=str(owner), username_normalized=str(owner)))
    values = dict(
        environment_id=identity, incarnation=identity, owner_user_id=owner, owner_team_id=team,
        scope="personal", kind="development", slug=slug, cluster_id="cluster-1", physical_pool_id="pool-1",
        application_namespace="loom-dev-" + slug, execution_namespace="loom-run-" + identity.hex,
        build_namespace="loom-run-" + identity.hex + "-build", public_host=slug + ".dev.example.com",
        target_id="env-" + identity.hex, binding_mode="generated", deployment_generation=1,
        desired_state="active",
    )
    values.update(changes)
    connection.execute(insert(NebiusEnvironment).values(**values))
    return values


def test_new_registration_tables_are_in_migration_and_orm(environment_database):
    tables = set(inspect(environment_database).get_table_names())
    assert {"nebius_environments", "nebius_environment_namespaces", "nebius_environment_operations",
            "nebius_environment_resources", "nebius_platform_budgets", "nebius_platform_reservations"} <= tables
    from loom.db.nebius_environment_schema import (
        NebiusEnvironment,
        NebiusEnvironmentNamespace,
        NebiusEnvironmentOperation,
        NebiusEnvironmentResource,
        NebiusPlatformBudget,
        NebiusPlatformReservation,
    )

    for model in (NebiusEnvironment, NebiusEnvironmentNamespace, NebiusEnvironmentOperation,
                  NebiusEnvironmentResource, NebiusPlatformBudget, NebiusPlatformReservation):
        actual = {c["name"] for c in inspect(environment_database).get_columns(model.__tablename__)}
        assert actual == set(model.__table__.columns.keys())


def test_provisioning_downgrade_refuses_to_discard_platform_budget(environment_database):
    from sqlalchemy.exc import DBAPIError

    from loom.db.nebius_environment_schema import NebiusPlatformBudget

    with environment_database.begin() as connection:
        connection.execute(insert(NebiusPlatformBudget).values(
            cluster_id="cluster", cpu_millis=1000, memory_mib=1000, storage_mib=1000, ephemeral_storage_mib=1000,
        ))
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", environment_database.url.render_as_string(hide_password=False).replace("%", "%%"))
    with pytest.raises(DBAPIError, match="cannot remove managed provisioning or platform budget history"):
        command.downgrade(cfg, "0154")
    with environment_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == service_schema_head()
        assert connection.execute(select(NebiusPlatformBudget.cpu_millis)).scalar_one() == 1000


@pytest.mark.parametrize("collision", ["slug", "public_host", "target_id", "incarnation"])
def test_live_identity_claims_cannot_collide(environment_database, collision):
    with environment_database.begin() as connection:
        first = registration(connection)
        with pytest.raises(IntegrityError), connection.begin_nested():
            registration(connection, **{"slug": "bob", collision: first[collision]})


def test_namespace_claims_are_unique_across_roles_and_transactions(environment_database):
    from loom.db.nebius_environment_schema import NebiusEnvironmentNamespace

    with environment_database.begin() as connection:
        a = registration(connection)
        b = registration(connection, "bob")
    ready = Barrier(2)

    def claim(row, role):
        ready.wait(timeout=10)
        try:
            with environment_database.begin() as connection:
                connection.execute(insert(NebiusEnvironmentNamespace).values(
                    environment_id=row["environment_id"], cluster_id="cluster-1",
                    namespace_name="loom-imported-collision", role=role,
                ))
            return "reserved"
        except IntegrityError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(claim, a, "application"), workers.submit(claim, b, "execution")]
        assert sorted(f.result(timeout=15) for f in futures) == ["conflict", "reserved"]
    with environment_database.connect() as connection:
        assert len(connection.execute(select(NebiusEnvironmentNamespace)).all()) == 1


def test_namespace_claim_cannot_change_cluster_or_duplicate_role(environment_database):
    from loom.db.nebius_environment_schema import NebiusEnvironmentNamespace

    with environment_database.begin() as connection:
        row = registration(connection)
        values = dict(environment_id=row["environment_id"], cluster_id="cluster-1",
                      namespace_name=row["application_namespace"], role="application")
        connection.execute(insert(NebiusEnvironmentNamespace).values(**values))
        for changes in ({"cluster_id": "another-cluster", "role": "execution"},
                        {"namespace_name": "another-name"}):
            with pytest.raises(IntegrityError), connection.begin_nested():
                connection.execute(insert(NebiusEnvironmentNamespace).values(**{**values, **changes}))


def test_destroy_retains_slug_until_verified_purge(environment_database):
    from loom.db.nebius_environment_schema import NebiusEnvironment

    with environment_database.begin() as connection:
        old = registration(connection, desired_state="destroyed")
        with pytest.raises(IntegrityError), connection.begin_nested():
            registration(connection)
        connection.execute(update(NebiusEnvironment).where(
            NebiusEnvironment.environment_id == old["environment_id"],
        ).values(purged_at=datetime.now(UTC)))
        new = registration(connection)
        assert old["incarnation"] != new["incarnation"]
        assert len(connection.execute(select(NebiusEnvironment)).all()) == 2


@pytest.mark.parametrize("changes", [
    {"scope": "personal", "kind": "production"}, {"owner_user_id": None},
    {"scope": "shared", "kind": "development", "slug": "alice"},
    {"deployment_generation": 0}, {"purged_at": datetime(2026, 1, 1, tzinfo=UTC)},
    {"execution_namespace": "same", "build_namespace": "same"},
])
def test_registration_database_rejects_invalid_state(environment_database, changes):
    with environment_database.begin() as connection:
        with pytest.raises(IntegrityError):
            registration(connection, **changes)


def test_upgrade_preserves_legacy_dev_rows(environment_database):
    from loom.db.schema import Team, User
    from tests.support.legacy_personal_dev_schema import DevInstance

    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", environment_database.url.render_as_string(hide_password=False).replace("%", "%%"))
    command.downgrade(cfg, "0153")
    owner, team = uuid4(), uuid4()
    with environment_database.begin() as connection:
        connection.execute(insert(Team).values(id=team, name=str(team)))
        connection.execute(insert(User).values(id=owner, username=str(owner), username_normalized=str(owner)))
        connection.execute(insert(DevInstance).values(
            name="retained", owner_user_id=owner, owner_team_id=team, max_slots=2,
            deployment_generation=1, candidate_sha="a" * 40, operation_id=uuid4(),
        ))
        before = connection.execute(text("SELECT to_jsonb(d) FROM dev_instances d")).scalar_one()
    command.upgrade(cfg, "0166")
    with pytest.raises(RuntimeError, match="retained-row disposition: dev_instances"):
        command.upgrade(cfg, "head")
    with environment_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "0166"
        assert connection.execute(text("SELECT to_jsonb(d) FROM dev_instances d")).scalar_one() == before
        assert connection.execute(text("SELECT count(*) FROM nebius_environments")).scalar_one() == 0
