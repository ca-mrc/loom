"""The additive registry migration retains evidence and matches runtime metadata."""
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, insert, inspect, text
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_catalog_schema import (
    NebiusPoolParticipantCatalog,
    NebiusPoolProfileDocument,
)
from loom.db.nebius_pool_lifecycle_schema import (
    NebiusPoolLifecycleOperation,
    NebiusPoolParticipantLifecycle,
)
from loom.db.schema_startup import service_schema_head
from tests.integration.test_nebius_pool_schema import registered


def test_registry_upgrade_and_empty_downgrade_preserve_existing_authority(isolated_migration_postgres_url):
    config = Config('database/migrations/alembic.ini')
    config.set_main_option('sqlalchemy.url', isolated_migration_postgres_url.replace('%', '%%'))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            pool_id, _ = registered(connection)
        command.downgrade(config, '0177')
        with engine.connect() as connection:
            assert connection.scalar(text('SELECT count(*) FROM nebius_pool_bindings WHERE pool_id=:id'), {'id': pool_id}) == 1
        command.upgrade(config, 'head')
        for model in (NebiusPoolParticipantCatalog, NebiusPoolProfileDocument, NebiusPoolLifecycleOperation, NebiusPoolParticipantLifecycle):
            actual = inspect(engine).get_columns(model.__tablename__)
            assert {(row['name'], row['nullable']) for row in actual} == {
                (row.name, row.nullable) for row in model.__table__.columns}
        with engine.connect() as connection:
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
    finally:
        engine.dispose()


@pytest.mark.parametrize('evidence', ['profile', 'operation'])
def test_registry_downgrade_refuses_to_discard_retained_history(isolated_migration_postgres_url, evidence):
    config = Config('database/migrations/alembic.ini')
    config.set_main_option('sqlalchemy.url', isolated_migration_postgres_url.replace('%', '%%'))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            pool_id, _ = registered(connection)
            if evidence == 'profile':
                connection.execute(insert(NebiusPoolProfileDocument).values(pool_id=pool_id, profile_id=uuid4(),
                    family='execution', document_json={'retained': True}, document_sha256='a' * 64))
            else:
                connection.execute(insert(NebiusPoolLifecycleOperation).values(operation_id=uuid4(), pool_id=pool_id,
                    participant_id=None, request_json={'retained': True}, request_sha256='a' * 64, receipt_json={}))
        with pytest.raises(DBAPIError, match='cannot be discarded'):
            command.downgrade(config, '0177')
        with engine.connect() as connection:
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
    finally:
        engine.dispose()
