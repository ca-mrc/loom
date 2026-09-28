"""IAM dispatch history is retained across supported schema changes."""
from __future__ import annotations

import pytest
from sqlalchemy import insert, inspect, select, text
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from tests.integration.test_nebius_application_effect_migration import operation
from tests.integration.test_nebius_application_registry import (
    application_database as application_database,
)
from tests.integration.test_nebius_application_registry import migrate


def test_empty_cloud_migration_preserves_operation_and_model(application_database):
    from loom.db.schema import NebiusApplicationCloudEffect

    with application_database.begin() as connection:
        operation(connection)
        before = connection.execute(select(NebiusApplicationOperation)).mappings().all()
    migrate(application_database, "downgrade", "0164")
    migrate(application_database, "upgrade", "head")
    with application_database.connect() as connection:
        assert connection.execute(select(NebiusApplicationOperation)).mappings().all() == before
    assert {column["name"] for column in inspect(application_database).get_columns("nebius_application_cloud_effects")} == set(
        NebiusApplicationCloudEffect.__table__.columns.keys())


@pytest.mark.parametrize("phase", ["prepared", "dispatched", "observed"])
def test_cloud_migration_refuses_loss_of_every_dispatch_phase(application_database, phase):
    from loom.db.schema import NebiusApplicationCloudEffect

    migrate(application_database, "downgrade", "0165")
    with application_database.begin() as connection:
        owner = operation(connection)
        connection.execute(insert(NebiusApplicationCloudEffect).values(
            operation_id=owner, effect_key="account", sequence=1, intent_json={}, phase=phase,
            dispatch_epoch=None if phase == "prepared" else 1,
            observed_resource_id="owned-account" if phase == "observed" else None))
        before = connection.execute(select(NebiusApplicationCloudEffect)).mappings().all()
    with pytest.raises(DBAPIError, match="cannot remove application cloud history"):
        migrate(application_database, "downgrade", "0164")
    with application_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0165"
        assert connection.execute(select(NebiusApplicationCloudEffect)).mappings().all() == before
