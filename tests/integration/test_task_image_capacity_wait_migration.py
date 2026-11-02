"""Native wait migration safety on isolated, disposable PostgreSQL databases."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, undefer

from loom.db.schema import (
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Task,
    TaskImageCapacityWait,
    TaskImageMaterialization,
)
from loom.db.schema_startup import service_schema_head
from loom.pipeline.keys import canonical_digest
from tests.integration.test_protected_claim_application_migration import _config
from tests.integration.test_service_execution_leases import NEBIUS_CPU_EXECUTION_CLASS_V1, _target


@pytest.fixture
def wait_migration(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    execution_class = NEBIUS_CPU_EXECUTION_CLASS_V1
    target = _target(uuid4().hex[:12])
    task_id = f"wait-migration/{uuid4()}"
    image_id = uuid4()
    with Session(engine) as session, session.begin():
        payload = execution_class.model_dump(mode="json")
        session.add(ServiceExecutionClass(
            id=execution_class.class_id, schema_version=execution_class.schema_version,
            spec_json=payload, spec_sha256=canonical_digest(payload),
        ))
        session.add(Task(id=task_id, checksum="a" * 64, config={}))
        session.flush()
        payload = target.model_dump(mode="json")
        session.add(ServiceExecutionTarget(
            id=target.target_id, spec_json=payload, spec_sha256=canonical_digest(payload),
            **{key: payload[key] for key in (
                "logical_pool_id", "execution_class_id", "schema_version", "environment",
                "provider", "region", "failure_domain", "data_residency",
            )},
        ))
        session.add(TaskImageMaterialization(
            id=image_id, materialization_key="b" * 64, task_id=task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={},
        ))
    now = datetime.now(UTC)
    values = dict(
        target_id=target.target_id, materialization_id=image_id, lease_epoch=0,
        pool_id=target.logical_pool_id, cpu_millis=1000, memory_mib=1024, storage_mib=2048,
        first_waited_at=now, renewed_at=now, expires_at=now + timedelta(seconds=120),
    )
    try:
        yield config, engine, values
    finally:
        engine.dispose()


def test_live_wait_refuses_downgrade_then_expiry_allows_roundtrip(wait_migration):
    config, engine, values = wait_migration
    with Session(engine) as session, session.begin():
        session.add(TaskImageCapacityWait(**values))
    with pytest.raises(DBAPIError, match="native builder capacity waits must expire"):
        command.downgrade(config, "0148")
    with engine.begin() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
        assert connection.scalar(text("SELECT count(*) FROM task_image_capacity_waits")) == 1
        connection.execute(text("UPDATE task_image_capacity_waits SET "
                                "first_waited_at = now() - interval '121 seconds', "
                                "renewed_at = now() - interval '121 seconds', "
                                "expires_at = now() - interval '1 second'"))
    command.downgrade(config, "0148")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regclass('task_image_capacity_waits')")) is None
        assert connection.scalar(text("SELECT count(*) FROM task_image_materializations")) == 1
    command.upgrade(config, "0149")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0149"
        assert connection.scalar(text("SELECT count(*) FROM task_image_capacity_waits")) == 0


@pytest.mark.parametrize("column,value,constraint", [
    ("lease_epoch", -1, "epoch_check"),
    ("cpu_millis", 0, "resources_check"),
    ("memory_mib", 0, "resources_check"),
    ("storage_mib", 0, "resources_check"),
    ("expires_at", 121, "lifetime_check"),
    ("expires_at", 0, "lifetime_check"),
    ("renewed_at", -1, "lifetime_check"),
    ("materialization_id", uuid4(), "materialization_id_fkey"),
    ("target_id", "missing-target", "target_id_fkey"),
])
def test_wait_constraints_reject_invalid_rows(wait_migration, column, value, constraint):
    _, engine, values = wait_migration
    if column.endswith("_at"):
        value = values["renewed_at"] + timedelta(seconds=value)
    with pytest.raises(IntegrityError, match=constraint):
        with Session(engine) as session, session.begin():
            session.add(TaskImageCapacityWait(**{**values, column: value}))
            session.flush()


@pytest.mark.parametrize("preview_runtime", [None, {"namespace": "retained-preview"}])
def test_wait_has_one_head_per_target_and_materialization(wait_migration, preview_runtime):
    _, engine, values = wait_migration
    with Session(engine) as session, session.begin():
        session.add(TaskImageCapacityWait(**values))
        if preview_runtime is not None:
            session.execute(update(ServiceExecutionTarget).where(
                ServiceExecutionTarget.id == values["target_id"]
            ).values(preview_runtime_json=preview_runtime))
    with pytest.raises(IntegrityError, match="task_image_capacity_waits_pkey"):
        with Session(engine) as session, session.begin():
            session.add(TaskImageCapacityWait(**values))
            session.flush()
    with Session(engine) as session, session.begin():
        first = session.get(ServiceExecutionTarget, values["target_id"], options=[undefer("*")])
        second_id = "second-target"
        session.add(ServiceExecutionTarget(**{
            column.name: second_id if column.name == "id" else getattr(first, column.name)
            for column in ServiceExecutionTarget.__table__.columns
        }))
    with Session(engine) as session:
        second = session.get(ServiceExecutionTarget, second_id, options=[undefer("*")])
        assert second.preview_runtime_json == preview_runtime
    with pytest.raises(IntegrityError, match="task_image_capacity_waits_materialization_key"):
        with Session(engine) as session, session.begin():
            session.add(TaskImageCapacityWait(**{**values, "target_id": second_id}))
            session.flush()


@pytest.mark.parametrize("busy_table", ["execution_targets", "task_image_materializations"])
def test_wait_upgrade_refuses_busy_parent_without_partial_schema(wait_migration, busy_table):
    config, engine, _ = wait_migration
    command.downgrade(config, "0148")
    with engine.begin() as busy:
        busy.execute(text(f"LOCK TABLE {busy_table} IN ROW EXCLUSIVE MODE"))
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            command.upgrade(config, "0149")
        with engine.connect() as check:
            assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0148"
            assert check.scalar(text("SELECT to_regclass('task_image_capacity_waits')")) is None
    command.upgrade(config, "0149")
    with engine.connect() as check:
        assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0149"


@pytest.mark.parametrize("busy_table", [
    "task_image_capacity_waits", "execution_targets", "task_image_materializations", "trials",
])
@pytest.mark.timeout(10, func_only=True)
def test_wait_downgrade_refuses_busy_table_without_partial_schema(wait_migration, busy_table):
    config, engine, _ = wait_migration
    with engine.begin() as busy:
        # DROP removes FK triggers on both parents, requiring locks even for an
        # empty child table. Ordinary readers must cause NOWAIT refusal too.
        busy.execute(text(f"SELECT 1 FROM {busy_table} LIMIT 1"))
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            command.downgrade(config, "0148")
        with engine.connect() as check:
            assert check.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
            assert check.scalar(text("SELECT to_regclass('task_image_capacity_waits')")) is not None
    command.downgrade(config, "0148")
    command.upgrade(config, "0149")
