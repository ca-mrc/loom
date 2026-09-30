"""Installed origin history cannot be promoted, erased or lost by downgrade."""
from __future__ import annotations

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import Batch, Task, Team, Trial


def seed(connection, *, known=True):
    team_id, batch_id, trial_id = uuid4(), uuid4(), uuid4()
    origin = {"schema_version": "loom.pool-work-origin.v1", "data_environment_id": str(uuid4()),
        "submission_id": str(batch_id), "kind": "environment", "application": None} if known else None
    connection.execute(insert(Team).values(id=team_id, name=str(team_id)))
    task_id = "origin-schema/" + uuid4().hex
    connection.execute(insert(Task).values(id=task_id, checksum="a" * 64, config={}))
    connection.execute(insert(Batch).values(id=batch_id, team_id=team_id, name="origin-history",
        task_filter={}, trial_config={}, created_by_token_prefix="test", pool_origin=origin))
    connection.execute(insert(Trial).values(id=trial_id, team_id=team_id, task_id=task_id, batch_id=batch_id,
        state="queued", config={}, requires_caps={}, pool_origin=origin))
    return batch_id, trial_id, origin


@pytest.mark.parametrize("model,known", [(Batch, True), (Trial, True), (Batch, False), (Trial, False)])
def test_origin_is_immutable_in_actual_migration(isolated_migration_postgres_url, model, known):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            batch_id, trial_id, original = seed(connection, known=known)
            identity = batch_id if model is Batch else trial_id
            for replacement in ({"schema_version": "loom.pool-work-origin.v1", "kind": "application"},
                                None if known else {"kind": "environment"}):
                with pytest.raises(DBAPIError), connection.begin_nested():
                    connection.execute(update(model).where(model.id == identity).values(pool_origin=replacement))
            assert connection.execute(select(model.pool_origin).where(model.id == identity)).scalar_one() == original
    finally:
        engine.dispose()


def test_downgrade_refuses_to_drop_submission_provenance(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            seed(connection)
        config = Config("database/migrations/alembic.ini")
        config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
        with pytest.raises(DBAPIError, match="cannot remove global pool history"):
            command.downgrade(config, "0170")
    finally:
        engine.dispose()
