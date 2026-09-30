"""The gateway cannot erase or replay a dispatched external write in PostgreSQL."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, inspect, select, update
from sqlalchemy.exc import DBAPIError

from tests.integration.test_nebius_pool_schema import advance, registered, request
from tests.integration.test_nebius_pool_schema import pool_database as pool_database


def prepared(connection, **changes):
    from loom.db.nebius_pool_schema import NebiusPoolEffect

    pool, participant = registered(connection)
    row = request(connection, pool, participant)
    advance(connection, row, "create_intent", plan_sha256="d" * 64, plan_json={"fixed": True})
    effect = dict(effect_id=uuid4(), request_id=row["request_id"], plan_sha256=row["plan_sha256"],
        namespace_uid=row["namespace_uid"], effect_key="create:job", sequence=1,
        intent_json={"kind": "Job", "action": "create", "name": "fixed", "namespace": "run",
                     "request_sha256": "e" * 64, "uid": None, "resource_version": None}, phase="prepared") | changes
    connection.execute(insert(NebiusPoolEffect).values(**effect))
    return row, effect


def dispatch(connection, effect):
    from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolMachine, NebiusPoolRequest

    machine = uuid4()
    pool = connection.execute(select(NebiusPoolRequest.pool_id).where(
        NebiusPoolRequest.request_id == effect["request_id"])).scalar_one()
    connection.execute(insert(NebiusPoolMachine).values(machine_id=machine, pool_id=pool,
        role="gateway", credential_epoch=1, phase="active"))
    values = dict(phase="dispatched", dispatch_id=uuid4(), dispatch_machine_id=machine, dispatch_epoch=1)
    connection.execute(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect["effect_id"]).values(**values))
    effect.update(values)


def test_effect_model_matches_actual_migration(pool_database):
    from loom.db.nebius_pool_schema import NebiusPoolEffect

    assert {row["name"] for row in inspect(pool_database).get_columns(NebiusPoolEffect.__tablename__)} == set(NebiusPoolEffect.__table__.columns.keys())


@pytest.mark.parametrize("damage", ["delete", "reset", "dispatch_id", "dispatch_epoch", "body", "plan", "namespace", "key"])
def test_dispatched_effect_cannot_be_erased_rebound_or_redispatched(pool_database, damage):
    from loom.db.nebius_pool_schema import NebiusPoolEffect

    with pool_database.begin() as connection:
        _, effect = prepared(connection)
        dispatch(connection, effect)
        changes = {
            "reset": {"phase": "prepared", "dispatch_id": None, "dispatch_machine_id": None, "dispatch_epoch": None},
            "dispatch_id": {"dispatch_id": uuid4()}, "dispatch_epoch": {"dispatch_epoch": 2},
            "body": {"intent_json": {"changed": True}}, "plan": {"plan_sha256": "f" * 64},
            "namespace": {"namespace_uid": uuid4()}, "key": {"effect_key": "create:other"},
        }
        with pytest.raises(DBAPIError), connection.begin_nested():
            statement = delete(NebiusPoolEffect) if damage == "delete" else update(NebiusPoolEffect).values(**changes[damage])
            connection.execute(statement.where(NebiusPoolEffect.effect_id == effect["effect_id"]))


@pytest.mark.parametrize("terminal", ["observed", "rejected"])
def test_effect_terminal_evidence_is_append_once_not_request_release(pool_database, terminal):
    from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolRequest

    with pool_database.begin() as connection:
        row, effect = prepared(connection)
        dispatch(connection, effect)
        evidence = {"observed_uid": uuid4(), "observed_resource_version": "7"} if terminal == "observed" else {"rejection_status": 409}
        update_row = update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect["effect_id"])
        connection.execute(update_row.values(phase=terminal, **evidence))
        connection.execute(update_row.values(phase=terminal, **evidence))  # exact replay is harmless
        with pytest.raises(DBAPIError), connection.begin_nested():
            connection.execute(update_row.values(**({"observed_uid": uuid4()} if terminal == "observed" else {"rejection_status": 422})))
        assert connection.execute(select(NebiusPoolRequest.phase).where(NebiusPoolRequest.request_id == row["request_id"])).scalar_one() == "create_intent"


@pytest.mark.parametrize("changes", [{"phase": "dispatched"}, {"phase": "observed", "observed_uid": uuid4()},
    {"plan_sha256": "f" * 64}, {"namespace_uid": uuid4()}, {"sequence": 0}, {"effect_key": ""}])
def test_new_effect_must_bind_the_existing_frozen_plan_and_begin_prepared(pool_database, changes):
    with pool_database.begin() as connection:
        with pytest.raises(DBAPIError), connection.begin_nested():
            prepared(connection, **changes)
