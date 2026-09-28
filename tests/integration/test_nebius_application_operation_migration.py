"""Application journals survive rollback and keep ORM/migration constraints aligned."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import insert, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.test_nebius_application_registry import (
    application,
    migrate,
)
from tests.integration.test_nebius_application_registry import (
    application_database as application_database,
)


def test_empty_operation_downgrade_preserves_existing_application_records(application_database):
    with application_database.begin() as connection:
        application(connection)
        before = connection.execute(text("SELECT * FROM nebius_applications")).mappings().all()
        claims = connection.execute(text("SELECT * FROM nebius_deployment_name_claims ORDER BY kind")).mappings().all()
    migrate(application_database, "downgrade", "0160")
    with application_database.connect() as connection:
        assert connection.execute(text("SELECT * FROM nebius_applications")).mappings().all() == before
        assert connection.execute(text("SELECT * FROM nebius_deployment_name_claims ORDER BY kind")).mappings().all() == claims
    migrate(application_database, "upgrade", "head")
    from loom.db.nebius_application_operation_schema import (
        NebiusApplicationOperation,
        NebiusApplicationReservation,
    )
    for model in (NebiusApplicationOperation, NebiusApplicationReservation):
        assert {column["name"] for column in inspect(application_database).get_columns(model.__tablename__)} == set(model.__table__.columns.keys())


@pytest.mark.parametrize("history", ["operation", "reservation"])
def test_operation_downgrade_refuses_to_erase_history(application_database, history):
    # Pin the historical migration under test; later empty journals can downgrade first.
    migrate(application_database, "downgrade", "0161")
    from loom.db.nebius_application_operation_schema import (
        NebiusApplicationOperation,
        NebiusApplicationReservation,
    )
    from loom.db.nebius_environment_schema import NebiusPlatformBudget

    model = NebiusApplicationOperation if history == "operation" else NebiusApplicationReservation
    with application_database.begin() as connection:
        row = application(connection)
        if history == "operation":
            connection.execute(insert(model).values(operation_id=uuid4(), application_id=row["application_id"],
                owner_user_id=row["owner_user_id"], idempotency_key="retained", request_sha256="a" * 64,
                deployment_generation=1, access_generation=1, action="create", phase="pending", plan_json={}))
        else:
            costs = dict(cpu_millis=100, memory_mib=128, storage_mib=0, ephemeral_storage_mib=512)
            connection.execute(insert(NebiusPlatformBudget).values(cluster_id=row["cluster_id"], **costs))
            connection.execute(insert(model).values(application_id=row["application_id"], cluster_id=row["cluster_id"], **costs))
        snapshot = text("SELECT * FROM " + model.__tablename__)
        before = connection.execute(snapshot).mappings().all()
    with pytest.raises(DBAPIError, match="cannot remove application operation or reservation history"):
        migrate(application_database, "downgrade", "0160")
    with application_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0161"
        assert connection.execute(snapshot).mappings().all() == before


@pytest.mark.parametrize("change", [
    {"phase": "running"}, {"phase": "pending", "lease_token": uuid4()},
    {"access_generation": 0}, {"runner_epoch": -1}, {"action": "purge"},
])
def test_operation_database_rejects_invalid_fencing_state(application_database, change):
    from loom.db.nebius_application_operation_schema import NebiusApplicationOperation

    with application_database.begin() as connection:
        row = application(connection)
        values = dict(operation_id=uuid4(), application_id=row["application_id"], owner_user_id=row["owner_user_id"],
                      idempotency_key="invalid", request_sha256="a" * 64, deployment_generation=1,
                      access_generation=1, action="create", phase="pending", plan_json={}) | change
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(insert(NebiusApplicationOperation).values(**values))


@pytest.mark.parametrize("change", [{"cluster_id": "foreign"}, {"storage_mib": 1}, {"cpu_millis": -1}])
def test_reservation_rejects_cross_cluster_or_storage_ownership(application_database, change):
    from loom.db.nebius_application_operation_schema import NebiusApplicationReservation
    from loom.db.nebius_environment_schema import NebiusPlatformBudget

    with application_database.begin() as connection:
        row = application(connection)
        costs = dict(cpu_millis=100, memory_mib=128, storage_mib=0, ephemeral_storage_mib=512)
        for cluster in (row["cluster_id"], "foreign"):
            connection.execute(insert(NebiusPlatformBudget).values(cluster_id=cluster, **costs))
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(insert(NebiusApplicationReservation).values(
                **(dict(application_id=row["application_id"], cluster_id=row["cluster_id"], **costs) | change)))
