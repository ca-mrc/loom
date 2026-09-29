"""Stop evidence survives upgrade; downgrade cannot erase a completion receipt."""
from __future__ import annotations

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.schema_startup import service_schema_head
from tests.integration.test_nebius_application_effect_migration import operation
from tests.integration.test_nebius_application_registry import (
    application_database as application_database,
)
from tests.integration.test_nebius_application_registry import migrate


def test_completion_columns_pair_durable_evidence_and_time(application_database):
    assert {'completion_json', 'completed_at'} <= {
        item['name'] for item in inspect(application_database).get_columns('nebius_application_operations')}
    with application_database.begin() as connection:
        owner = operation(connection)
        for assignments in ("completion_json='{}'::jsonb", "completed_at=now()",
                            "completion_json='[]'::jsonb, completed_at=now(), phase='completed'",
                            "completion_json='{}'::jsonb, completed_at=now()"):
            with pytest.raises(IntegrityError), connection.begin_nested():
                connection.execute(text('UPDATE nebius_application_operations SET ' + assignments))
        connection.execute(text("UPDATE nebius_application_operations SET phase='completed', "
            "completion_json='{}'::jsonb, completed_at=now() WHERE operation_id=:id"), {'id': owner})
        # Transition retains a completed predecessor's receipt.
        connection.execute(text("UPDATE nebius_application_operations SET phase='superseded'"))


def test_empty_completion_downgrade_roundtrip_preserves_frozen_operation(application_database):
    with application_database.begin() as connection:
        operation(connection)
        before = connection.execute(text('SELECT * FROM nebius_application_operations')).mappings().all()
    migrate(application_database, 'downgrade', '0165')
    migrate(application_database, 'upgrade', '0166')
    with application_database.connect() as connection:
        assert connection.execute(text('SELECT * FROM nebius_application_operations')).mappings().all() == before


def test_completion_receipt_blocks_lossy_downgrade(application_database):
    with application_database.begin() as connection:
        operation(connection)
        connection.execute(text("UPDATE nebius_application_operations SET phase='completed', "
            "completion_json='{}'::jsonb, completed_at=now()"))
        before = connection.execute(text('SELECT * FROM nebius_application_operations')).mappings().all()
    with pytest.raises(DBAPIError, match='cannot remove application completion evidence'):
        migrate(application_database, 'downgrade', '0165')
    with application_database.connect() as connection:
        assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
        assert connection.execute(text('SELECT * FROM nebius_application_operations')).mappings().all() == before
