"""Legacy build history remains authoritative through projection upgrade/rollback."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_protected_claim_application_migration import _config


@pytest.fixture
def projection_migration(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    command.downgrade(config, "0185")
    image, attempt, task = uuid4(), uuid4(), f"projection/{uuid4()}"
    native = {"target_id": "legacy-owner", "capacity_reserved_at": "2026-10-30T12:30:00-04:00",
              "capacity_released_at": "invalid-legacy-time", "image_archives": [],
              "configmap": {"data": {"claim.json": '{"archive_write_intents": []}'}},
              "job": {"opaque": "retained"}}
    with engine.begin() as c:
        c.execute(text("INSERT INTO tasks(id,checksum,config) VALUES(:id,:checksum,'{}')"),
                  {"id": task, "checksum": "a" * 64})
        c.execute(text("INSERT INTO task_image_materializations "
            "(id,materialization_key,task_id,task_checksum,cpu_arch,task_config) "
            "VALUES(:id,:key,:task,:checksum,'x86_64','{}')"),
            {"id": image, "key": uuid4().hex * 2, "task": task, "checksum": "a" * 64})
        c.execute(text("INSERT INTO task_image_materialization_attempts "
            "(id,materialization_id,attempt_number,lease_epoch,builder_id,claimed_at,native_build) "
            "VALUES(:id,:image,1,1,'legacy',:now,CAST(:native AS jsonb))"),
            {"id": attempt, "image": image, "now": datetime.now(UTC), "native": json.dumps(native)})
    try:
        yield config, engine, attempt, native
    finally:
        engine.dispose()


def test_projection_backfill_updates_and_rollback_keep_json_authority(projection_migration):
    config, engine, attempt, native = projection_migration
    command.upgrade(config, "0186")
    with engine.connect() as c:
        row = c.execute(text("SELECT * FROM task_image_materialization_attempts WHERE id=:id"),
                        {"id": attempt}).mappings().one()
        assert row["native_build"] == native
        assert row["native_capacity_target_id"] == "legacy-owner"
        assert row["native_capacity_reserved_at"] == native["capacity_reserved_at"]
        assert row["native_capacity_released_at"] == "invalid-legacy-time"
        assert row["native_archive_checked_at"] is None
        assert row["native_archive_has_images"] is True
        assert row["native_archive_has_intents"] is True
    # Older image writers only update JSON; no dual-write or repair step exists.
    changed = {**native, "capacity_released_at": None, "image_archives": None,
               "archive_cleanup_checked_at": "2026-10-31T12:00:00+00:00", "configmap": {}}
    with engine.begin() as c:
        c.execute(text("UPDATE task_image_materialization_attempts SET native_build=CAST(:native AS jsonb) "
                       "WHERE id=:id"), {"id": attempt, "native": json.dumps(changed)})
    with engine.connect() as c:
        row = c.execute(text("SELECT * FROM task_image_materialization_attempts WHERE id=:id"),
                        {"id": attempt}).mappings().one()
        assert row["native_build"] == changed
        assert row["native_capacity_released_at"] is None
        assert row["native_archive_checked_at"] == changed["archive_cleanup_checked_at"]
        assert row["native_archive_has_images"] is False
        assert row["native_archive_has_intents"] is False
    with pytest.raises(DBAPIError, match="generated"):
        with engine.begin() as c:
            c.execute(text("UPDATE task_image_materialization_attempts SET native_capacity_target_id='forged' "
                           "WHERE id=:id"), {"id": attempt})
    command.downgrade(config, "0185")
    with engine.connect() as c:
        assert c.scalar(text("SELECT native_build FROM task_image_materialization_attempts WHERE id=:id"),
                        {"id": attempt}) == changed
        assert c.scalar(text("SELECT count(*) FROM pg_indexes WHERE tablename='task_image_materialization_attempts' "
                             "AND indexname LIKE 'task_image_attempts_native_%'")) == 0
    command.upgrade(config, "0186")


@pytest.mark.timeout(10, func_only=True)
def test_busy_projection_upgrade_times_out_without_partial_backfill(projection_migration):
    config, engine, attempt, native = projection_migration
    with engine.begin() as reader:
        reader.execute(text("SELECT id FROM task_image_materialization_attempts WHERE id=:id"), {"id": attempt})
        with pytest.raises(DBAPIError, match="lock timeout"):
            command.upgrade(config, "0186")
        with engine.connect() as check:
            assert check.scalar(text("SELECT version_num FROM alembic_version")) == "0185"
            assert check.scalar(text("SELECT native_build FROM task_image_materialization_attempts WHERE id=:id"),
                                {"id": attempt}) == native
            assert check.scalar(text("SELECT count(*) FROM information_schema.columns "
                "WHERE table_name='task_image_materialization_attempts' "
                "AND column_name='native_capacity_target_id'")) == 0
    command.upgrade(config, "0186")
