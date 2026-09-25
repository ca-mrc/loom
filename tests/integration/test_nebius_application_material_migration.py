"""Material references retain both immutable operation history and ciphertext."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.schema import Secret
from tests.integration.test_nebius_application_effect_migration import operation
from tests.integration.test_nebius_application_registry import (
    application_database as application_database,
)
from tests.integration.test_nebius_application_registry import migrate


def test_empty_material_downgrade_preserves_operations_and_orm_shape(application_database):
    from loom.db.schema import NebiusApplicationMaterial

    with application_database.begin() as connection:
        operation(connection)
        before = connection.execute(select(NebiusApplicationOperation)).mappings().all()
    migrate(application_database, "downgrade", "0162")
    migrate(application_database, "upgrade", "0163")
    with application_database.connect() as connection:
        assert connection.execute(select(NebiusApplicationOperation)).mappings().all() == before
    assert {col["name"] for col in inspect(application_database).get_columns("nebius_application_material")} == set(
        NebiusApplicationMaterial.__table__.columns.keys())


def test_material_history_cannot_be_downgraded_or_orphaned(application_database):
    from loom.db.schema import NebiusApplicationMaterial

    ref = "loom://synthetic-material/" + str(uuid4())
    with application_database.begin() as connection:
        owner = operation(connection)
        connection.execute(insert(Secret).values(ref=ref, ciphertext=b"test-only", nonce=b"n" * 12, master_key_version=1))
        connection.execute(insert(NebiusApplicationMaterial).values(operation_id=owner, secret_ref=ref))
        before = connection.execute(select(NebiusApplicationMaterial)).mappings().all()
        for target, condition in ((Secret, Secret.ref == ref),
                                  (NebiusApplicationOperation, NebiusApplicationOperation.operation_id == owner)):
            with pytest.raises(IntegrityError), connection.begin_nested():
                connection.execute(delete(target).where(condition))
        second = dict(connection.execute(select(NebiusApplicationOperation)).mappings().one()) | {
            "operation_id": uuid4(), "idempotency_key": "second", "deployment_generation": 2, "access_generation": 2,
        }
        connection.execute(insert(NebiusApplicationOperation).values(**second))
        unused_ref = "loom://synthetic-unused/" + str(uuid4())
        connection.execute(insert(Secret).values(
            ref=unused_ref, ciphertext=b"test-only", nonce=b"n" * 12, master_key_version=1))
        for invalid in ({"operation_id": uuid4(), "secret_ref": unused_ref},
                        {"operation_id": second["operation_id"], "secret_ref": ref + "-missing"}):
            with pytest.raises(IntegrityError) as rejected, connection.begin_nested():
                connection.execute(insert(NebiusApplicationMaterial).values(**invalid))
            assert rejected.value.orig.sqlstate == "23503"  # foreign_key_violation, not UNIQUE/PK
        with pytest.raises(IntegrityError) as duplicate, connection.begin_nested():
            connection.execute(insert(NebiusApplicationMaterial).values(
                operation_id=second["operation_id"], secret_ref=ref))
        assert duplicate.value.orig.sqlstate == "23505"  # a ciphertext reference cannot be adopted twice
    with pytest.raises(DBAPIError, match="cannot remove application material history"):
        migrate(application_database, "downgrade", "0162")
    with application_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0163"
        assert connection.execute(select(NebiusApplicationMaterial)).mappings().all() == before
