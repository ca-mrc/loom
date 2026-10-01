"""Installed origin history cannot be promoted, erased or lost by downgrade."""
from __future__ import annotations

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import Batch, NebiusPoolSubmission, Task, Team, Trial, User


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
            command.downgrade(config, "0171")
    finally:
        engine.dispose()


def seed_handoff(connection):
    identity, team_id, user_id = uuid4(), uuid4(), uuid4()
    connection.execute(insert(Team).values(id=team_id, name=str(team_id)))
    connection.execute(insert(User).values(id=user_id, username=str(user_id), username_normalized=str(user_id)))
    origin = {"schema_version": "loom.pool-work-origin.v1", "data_environment_id": str(uuid4()),
        "submission_id": str(identity), "kind": "environment", "application": None}
    connection.execute(insert(NebiusPoolSubmission).values(id=identity, team_id=team_id, user_id=user_id,
        request_sha256="a" * 64, pool_origin=origin))
    return identity, origin


@pytest.mark.parametrize("field", ["origin", "payload", "user", "team"])
def test_direct_origin_binding_cannot_be_reassigned(isolated_migration_postgres_url, field):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            identity, origin = seed_handoff(connection)
            other, _ = seed_handoff(connection)
            row = connection.execute(select(NebiusPoolSubmission).where(NebiusPoolSubmission.id == other)).one()
            changes = {"origin": {"pool_origin": origin | {"data_environment_id": str(uuid4())}},
                "payload": {"request_sha256": "b" * 64}, "user": {"user_id": row.user_id}, "team": {"team_id": row.team_id}}
            with pytest.raises(DBAPIError, match="submission handoff is immutable"), connection.begin_nested():
                connection.execute(update(NebiusPoolSubmission).where(NebiusPoolSubmission.id == identity).values(**changes[field]))
    finally:
        engine.dispose()


def test_downgrade_refuses_unconsumed_direct_handoff(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            seed_handoff(connection)
        config = Config("database/migrations/alembic.ini")
        config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
        with pytest.raises(DBAPIError, match="cannot remove global pool history"):
            command.downgrade(config, "0171")
    finally:
        engine.dispose()
