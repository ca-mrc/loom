"""Global reservation identity and release barriers on actual PostgreSQL."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, delete, insert, inspect, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.schema_startup import service_schema_head


@pytest.mark.parametrize("table", ["execution_leases", "task_image_materializations", "task_image_materialization_attempts"])
def test_pool_downgrade_refuses_parent_read_locks_without_waiting(isolated_migration_postgres_url, table):
    # A separate connection retains a real read lock. The safety deadline turns
    # an accidental blocking DDL into a diagnostic error, not a hanging test;
    # only PostgreSQL's NOWAIT rejection satisfies the assertion.
    url = make_url(isolated_migration_postgres_url).update_query_dict({"options": "-c statement_timeout=2000"})
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'LOCK TABLE "{table}" IN ACCESS SHARE MODE')
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                command.downgrade(config, "0171")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
    finally:
        engine.dispose()


@pytest.fixture
def pool_database(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


def registered(connection, pool_id=None):
    from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant

    participant_id = uuid4()
    if pool_id is None:
        pool_id = uuid4()
        connection.execute(insert(NebiusPoolBinding).values(
            pool_id=pool_id, installation_id=uuid4(), cluster_id="cluster-" + uuid4().hex,
            node_group_id="group-1", policy_revision=1, admission_epoch=1, mode="closed",
            binding_json={"protected": True}, binding_sha256="a" * 64))
    connection.execute(insert(NebiusPoolParticipant).values(
        participant_id=participant_id, pool_id=pool_id, environment_id=uuid4(),
        incarnation=uuid4(), binding_revision=1, admission_epoch=1, phase="fenced",
        binding_json={"protected": True}, binding_sha256="b" * 64))
    return pool_id, participant_id


def request(connection, pool_id, participant_id, **changes):
    from loom.db.nebius_pool_schema import NebiusPoolRequest

    timestamp = datetime.now(UTC)
    values = dict(
        request_id=uuid4(), pool_id=pool_id, participant_id=participant_id,
        namespace_uid=uuid4(),
        workload_kind="trial", local_work_id=uuid4(), generation=1, admission_epoch=1,
        target_id="nebius-default", request_sha256="c" * 64, request_json={"typed": True},
        deadline_at=datetime.now(UTC) + timedelta(minutes=10), phase="reserved",
        cpu_millis=1000, memory_mib=1024, ephemeral_storage_mib=1024, pod_slots=1,
        created_at=timestamp, renewed_at=timestamp,
        granted_at=None if changes.get("phase") == "waiting" else timestamp, priority=2,
    ) | changes
    connection.execute(insert(NebiusPoolRequest).values(**values))
    return values


def advance(connection, row, phase, **values):
    from loom.db.nebius_pool_schema import NebiusPoolRequest

    if row["phase"] == "waiting" and phase == "reserved":
        values.setdefault("granted_at", datetime.now(UTC))
    connection.execute(update(NebiusPoolRequest).where(
        NebiusPoolRequest.request_id == row["request_id"]).values(phase=phase, **values))
    row.update(phase=phase, **values)


def cleanup_intent(connection, row):
    advance(connection, row, "create_intent", plan_sha256="d" * 64, plan_json={"fixed": True})
    advance(connection, row, "cleanup_intent")


def cleanup(connection, row, **changes):
    from loom.db.nebius_pool_schema import NebiusPoolCleanupObservation

    values = dict(
        observation_id=uuid4(), request_id=row["request_id"], plan_sha256="d" * 64,
        namespace_uid=row["namespace_uid"], writer_epoch=1, observed_at=datetime.now(UTC),
        evidence_json={"qualified_by_gateway": True},
    ) | changes
    connection.execute(insert(NebiusPoolCleanupObservation).values(**values))
    return values["observation_id"]


def test_migration_and_orm_have_the_same_pool_journal_columns(pool_database):
    from loom.db.nebius_pool_schema import (
        NebiusPoolBinding,
        NebiusPoolCancellation,
        NebiusPoolCapture,
        NebiusPoolCleanupObservation,
        NebiusPoolMachine,
        NebiusPoolMachineCredential,
        NebiusPoolObservation,
        NebiusPoolParticipant,
        NebiusPoolRequest,
    )

    for model in (NebiusPoolBinding, NebiusPoolCancellation, NebiusPoolParticipant, NebiusPoolMachine,
                  NebiusPoolMachineCredential, NebiusPoolRequest, NebiusPoolCleanupObservation,
                  NebiusPoolCapture, NebiusPoolObservation):
        assert {column["name"] for column in inspect(pool_database).get_columns(model.__tablename__)} == set(model.__table__.columns.keys())


def test_same_local_request_in_two_participants_is_distinct_but_replay_key_is_unique(pool_database):
    with pool_database.begin() as connection:
        a_pool, alice = registered(connection)
        b_pool, bob = registered(connection, pool_id=a_pool)
        first = request(connection, a_pool, alice)
        request(connection, b_pool, bob, local_work_id=first["local_work_id"])
        with pytest.raises(IntegrityError), connection.begin_nested():
            request(connection, a_pool, alice, local_work_id=first["local_work_id"])
        request(connection, a_pool, alice, local_work_id=first["local_work_id"], generation=2)


def test_request_cannot_claim_a_different_physical_pool(pool_database):
    with pool_database.begin() as connection:
        _, alice = registered(connection)
        other_pool, _ = registered(connection)
        with pytest.raises(IntegrityError), connection.begin_nested():
            request(connection, other_pool, alice)


@pytest.mark.parametrize("changes", [
    {"generation": 0}, {"admission_epoch": 0}, {"cpu_millis": -1},
    {"pod_slots": 0}, {"request_sha256": "bad"}, {"request_json": []},
    {"phase": "released"}, {"phase": "observed"},
])
def test_new_request_cannot_bypass_accounting_or_begin_with_external_effects(pool_database, changes):
    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        with pytest.raises(DBAPIError), connection.begin_nested():
            request(connection, pool, participant, **changes)


@pytest.mark.parametrize("phase", ["reserved", "waiting"])
def test_only_never_started_requests_cancel_without_cleanup(pool_database, phase):
    from loom.db.nebius_pool_schema import NebiusPoolRequest

    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant, phase=phase)
        advance(connection, row, "cancelled_unstarted")
        assert connection.execute(select(NebiusPoolRequest.phase)).scalar_one() == "cancelled_unstarted"
        with pytest.raises(DBAPIError), connection.begin_nested():
            advance(connection, row, "reserved")


@pytest.mark.parametrize("phase", ["create_intent", "observed", "cleanup_intent"])
def test_started_requests_cannot_skip_cleanup_or_cancel_as_unstarted(pool_database, phase):
    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant)
        advance(connection, row, "create_intent", plan_sha256="d" * 64, plan_json={"fixed": True})
        if phase != "create_intent":
            advance(connection, row, phase, **({"job_uid": uuid4()} if phase == "observed" else {}))
        for target in ("released", "cancelled_unstarted", "reserved"):
            with pytest.raises(DBAPIError), connection.begin_nested():
                advance(connection, row, target)


def test_release_requires_this_request_and_plan_cleanup_observation(pool_database):
    from loom.db.nebius_pool_schema import NebiusPoolRequest

    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        first = request(connection, pool, participant)
        second = request(connection, pool, participant)
        for row in (first, second):
            cleanup_intent(connection, row)
        wrong = cleanup(connection, second)
        with pytest.raises(IntegrityError), connection.begin_nested():
            advance(connection, first, "released", cleanup_observation_id=wrong)
        with pytest.raises(IntegrityError), connection.begin_nested():
            cleanup(connection, first, plan_sha256="e" * 64)
        with pytest.raises(IntegrityError), connection.begin_nested():
            cleanup(connection, first, namespace_uid=uuid4())
        own = cleanup(connection, first)
        advance(connection, first, "released", cleanup_observation_id=own)
        assert connection.execute(select(NebiusPoolRequest.phase).where(
            NebiusPoolRequest.request_id == first["request_id"])).scalar_one() == "released"


@pytest.mark.parametrize("changes", [
    {"request_sha256": "e" * 64}, {"generation": 2}, {"admission_epoch": 2},
    {"request_json": {"changed": True}}, {"cpu_millis": 500},
    {"deadline_at": datetime(2100, 1, 1, tzinfo=UTC)},
    {"plan_sha256": "e" * 64}, {"plan_json": {"changed": True}},
    {"namespace_uid": uuid4()},
    {"priority": 0}, {"created_at": datetime(2100, 1, 1, tzinfo=UTC)},
    {"granted_at": datetime(2100, 1, 1, tzinfo=UTC)},
    {"renewed_at": datetime(2100, 1, 1, tzinfo=UTC)},
])
def test_journal_rejects_mutating_identity_workload_envelope_deadline_or_plan(pool_database, changes):
    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant)
        cleanup_intent(connection, row)
        with pytest.raises(DBAPIError), connection.begin_nested():
            advance(connection, row, "cleanup_intent", **changes)


def test_waiting_renewal_cannot_fabricate_a_grant_or_erase_age(pool_database):
    from loom.db.nebius_pool_schema import NebiusPoolRequest

    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant, phase="waiting")
        created = row["created_at"]
        advance(connection, row, "waiting", renewed_at=created + timedelta(seconds=1))
        for changes in ({"renewed_at": created}, {"granted_at": created}, {"priority": 0}):
            with pytest.raises(DBAPIError), connection.begin_nested():
                advance(connection, row, "waiting", **changes)
        advance(connection, row, "reserved", granted_at=created + timedelta(seconds=2))
        stored = connection.execute(select(NebiusPoolRequest.created_at, NebiusPoolRequest.granted_at)).one()
        assert stored == (created, created + timedelta(seconds=2))


def test_late_job_uid_is_monotonic_while_cleanup_remains_pending(pool_database):
    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant)
        cleanup_intent(connection, row)
        job_uid = uuid4()
        advance(connection, row, "cleanup_intent", job_uid=job_uid)
        for changed in (None, uuid4()):
            with pytest.raises(DBAPIError), connection.begin_nested():
                advance(connection, row, "cleanup_intent", job_uid=changed)


def test_downgrade_refuses_to_erase_retained_pool_history(pool_database):
    with pool_database.begin() as connection:
        registered(connection)
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", pool_database.url.render_as_string(hide_password=False).replace("%", "%%"))
    with pytest.raises(DBAPIError, match="cannot remove global pool history"):
        command.downgrade(config, "0171")


def test_cleanup_observation_cannot_be_rewritten_or_removed_before_release(pool_database):
    from loom.db.nebius_pool_schema import NebiusPoolCleanupObservation, NebiusPoolRequest

    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant)
        cleanup_intent(connection, row)
        cleanup(connection, row)
        for statement in (
            update(NebiusPoolCleanupObservation).values(evidence_json={"replaced": True}),
            delete(NebiusPoolCleanupObservation), delete(NebiusPoolRequest),
        ):
            with pytest.raises(DBAPIError), connection.begin_nested():
                connection.execute(statement)


def test_cleanup_observation_cannot_precede_the_irreversible_cleanup_state(pool_database):
    with pool_database.begin() as connection:
        pool, participant = registered(connection)
        row = request(connection, pool, participant)
        advance(connection, row, "create_intent", plan_sha256="d" * 64, plan_json={"fixed": True})
        with pytest.raises(DBAPIError), connection.begin_nested():
            cleanup(connection, row)
        advance(connection, row, "cleanup_intent")
        cleanup(connection, row)


def test_empty_pool_journal_can_downgrade_and_upgrade_without_schema_drift(pool_database):
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", pool_database.url.render_as_string(hide_password=False).replace("%", "%%"))
    command.downgrade(config, "0171")
    assert "nebius_pool_requests" not in inspect(pool_database).get_table_names()
    # Removing an empty unpublished pool layer must not undo the already
    # published legacy retirement or recreate its obsolete tables.
    assert "personal_dev_candidates" not in inspect(pool_database).get_table_names()
    command.upgrade(config, "0172")
    assert "nebius_pool_requests" in inspect(pool_database).get_table_names()


@pytest.mark.parametrize("table,changes", [
    ("pool", {"cluster_id": "another-cluster"}),
    ("pool", {"node_group_id": "another-group"}),
    ("pool", {"installation_id": uuid4()}),
    ("participant", {"environment_id": uuid4()}),
    ("participant", {"incarnation": uuid4()}),
])
def test_registration_identity_cannot_be_reassigned_to_another_environment_or_pool(pool_database, table, changes):
    from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant

    with pool_database.begin() as connection:
        registered(connection)
        model = NebiusPoolBinding if table == "pool" else NebiusPoolParticipant
        with pytest.raises(DBAPIError), connection.begin_nested():
            connection.execute(update(model).values(**changes))


@pytest.mark.parametrize("table,revision", [("pool", "policy_revision"), ("participant", "binding_revision")])
def test_binding_changes_need_new_revision_and_epochs_cannot_go_backwards(pool_database, table, revision):
    from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant

    with pool_database.begin() as connection:
        registered(connection)
        model = NebiusPoolBinding if table == "pool" else NebiusPoolParticipant
        with pytest.raises(DBAPIError), connection.begin_nested():
            connection.execute(update(model).values(binding_json={"new": True}))
        connection.execute(update(model).values(**{
            revision: 2, "admission_epoch": 2, "binding_json": {"new": True}, "binding_sha256": "e" * 64,
        }))
        for changes in ({revision: 1}, {"admission_epoch": 1}):
            with pytest.raises(DBAPIError), connection.begin_nested():
                connection.execute(update(model).values(**changes))


def early_cancel_values(pool_id, participant_id, local_work_id):
    return dict(cancellation_id=uuid4(), pool_id=pool_id, participant_id=participant_id,
                workload_kind="trial", local_work_id=local_work_id, generation=1,
                admission_epoch=1, request_sha256="c" * 64)


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_pre_prepare_cancellation_is_terminal_retained_history(pool_database, operation):
    from loom.db.nebius_pool_schema import NebiusPoolCancellation

    with pool_database.begin() as connection:
        pool_id, participant_id = registered(connection)
        values = early_cancel_values(pool_id, participant_id, uuid4())
        connection.execute(insert(NebiusPoolCancellation).values(**values))
        statement = delete(NebiusPoolCancellation) if operation == "delete" else update(NebiusPoolCancellation).values(request_sha256="e" * 64)
        with pytest.raises(DBAPIError, match="cancellation history is retained"), connection.begin_nested():
            connection.execute(statement)


@pytest.mark.parametrize("cancellation_first", [True, False])
def test_sql_cannot_create_both_request_and_early_cancellation_for_one_key(pool_database, cancellation_first):
    from loom.db.nebius_pool_schema import NebiusPoolCancellation

    with pool_database.begin() as connection:
        pool_id, participant_id = registered(connection)
        local_work_id = uuid4()
        values = early_cancel_values(pool_id, participant_id, local_work_id)
        if cancellation_first:
            connection.execute(insert(NebiusPoolCancellation).values(**values))
            with pytest.raises(DBAPIError, match="pool request identity already retained"), connection.begin_nested():
                request(connection, pool_id, participant_id, local_work_id=local_work_id)
        else:
            request(connection, pool_id, participant_id, local_work_id=local_work_id)
            with pytest.raises(DBAPIError, match="pool request identity already retained"), connection.begin_nested():
                connection.execute(insert(NebiusPoolCancellation).values(**values))


def test_equal_local_ids_in_other_participants_are_not_cancelled(pool_database):
    from loom.db.nebius_pool_schema import NebiusPoolCancellation

    with pool_database.begin() as connection:
        pool_id, first = registered(connection)
        _, second = registered(connection, pool_id)
        local_work_id = uuid4()
        connection.execute(insert(NebiusPoolCancellation).values(**early_cancel_values(pool_id, first, local_work_id)))
        request(connection, pool_id, second, local_work_id=local_work_id)
