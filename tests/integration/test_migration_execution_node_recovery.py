from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text


def test_node_recovery_migration_restores_previous_guard(
    isolated_migration_postgres_url: str,
) -> None:
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    engine = create_engine(isolated_migration_postgres_url)
    definition = text(
        "SELECT pg_get_functiondef('validate_execution_lease_mutation()'::regprocedure)"
    )
    try:
        command.downgrade(config, "0168")
        with engine.connect() as connection:
            before = connection.scalar(definition)
        command.upgrade(config, "0169")
        with engine.connect() as connection:
            assert connection.scalar(definition) != before
        command.downgrade(config, "0168")
        with engine.connect() as connection:
            assert connection.scalar(definition) == before
    finally:
        command.upgrade(config, "head")
        engine.dispose()
