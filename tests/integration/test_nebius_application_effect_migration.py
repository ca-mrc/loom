"""Effect history survives upgrades and refuses lossy downgrades."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import insert, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.nebius_application_effect_schema import NebiusApplicationEffect
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from tests.integration.test_nebius_application_registry import application, migrate
from tests.integration.test_nebius_application_registry import (
    application_database as application_database,
)


def operation(connection):
    row = application(connection)
    operation_id = uuid4()
    connection.execute(insert(NebiusApplicationOperation).values(
        operation_id=operation_id, application_id=row["application_id"], owner_user_id=row["owner_user_id"],
        idempotency_key="effect-owner", request_sha256="a" * 64, deployment_generation=1,
        access_generation=1, action="create", phase="pending", plan_json={"frozen": "unchanged"},
    ))
    return operation_id


def test_empty_effect_downgrade_and_upgrade_preserve_frozen_operation_and_orm_shape(application_database):
    with application_database.begin() as connection:
        operation(connection)
        before = connection.execute(select(NebiusApplicationOperation)).mappings().all()
    migrate(application_database, "downgrade", "0161")
    migrate(application_database, "upgrade", "0162")
    with application_database.connect() as connection:
        assert connection.execute(select(NebiusApplicationOperation)).mappings().all() == before
    assert {col["name"] for col in inspect(application_database).get_columns("nebius_application_effects")} == set(
        NebiusApplicationEffect.__table__.columns.keys())


@pytest.mark.parametrize("phase", ["prepared", "dispatched", "observed", "rejected"])
def test_every_effect_phase_blocks_destructive_downgrade(application_database, phase):
    # Exercise this historical revision independently of later transactional migrations.
    migrate(application_database, "downgrade", "0162")
    with application_database.begin() as connection:
        owner = operation(connection)
        connection.execute(insert(NebiusApplicationEffect).values(
            operation_id=owner, effect_key="preserved", sequence=1, intent_json={}, phase=phase,
            dispatch_epoch=None if phase == "prepared" else 1, observed_uid="uid" if phase == "observed" else None,
            rejection_status=409 if phase == "rejected" else None,
        ))
        before = connection.execute(select(NebiusApplicationEffect)).mappings().all()
    with pytest.raises(DBAPIError, match="cannot remove application effect history"):
        migrate(application_database, "downgrade", "0161")
    with application_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0162"
        assert connection.execute(select(NebiusApplicationEffect)).mappings().all() == before


@pytest.mark.parametrize("changes", [
    {"phase": "dispatched"}, {"phase": "observed", "dispatch_epoch": 1}, {"dispatch_epoch": 1},
    {"phase": "dispatched", "dispatch_epoch": 0}, {"observed_uid": "uid"}, {"observed_resource_version": "4"},
    {"sequence": 0}, {"effect_key": "bad/key"}, {"intent_json": []}, {"operation_id": uuid4()},
    {"rejection_status": 409}, {"phase": "rejected", "dispatch_epoch": 1},
    {"phase": "rejected", "dispatch_epoch": 1, "rejection_status": 500},
    {"phase": "rejected", "dispatch_epoch": 1, "rejection_status": 409, "observed_uid": "uid"},
])
def test_database_rejects_broken_dispatch_identity_state(application_database, changes):
    with application_database.begin() as connection:
        owner = operation(connection)
        values = dict(operation_id=owner, effect_key="effect", sequence=1, intent_json={}, phase="prepared") | changes
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(insert(NebiusApplicationEffect).values(**values))
