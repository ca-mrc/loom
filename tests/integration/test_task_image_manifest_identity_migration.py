from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization
from loom.db.schema_startup import service_schema_head
from loom.task_image_materialization import ensure_task_image_materializations
from tests.integration.test_task_image_materialization_store import _task_values
from tests.integration.test_task_image_registry_credential_migration import _config

TABLE = "task_image_materializations"
COLUMN = "bundle_content_manifest_sha256"
CHECKSUM = "a" * 64


def _key(digest: str = "") -> str:
    parts = [
        "task-image-materialization-v2" if digest else "task-image-materialization-v1",
        "manifest-identity",
        CHECKSUM,
        "arm64",
    ]
    if digest:
        parts.append(digest)
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


def _insert(connection, expected_digest: str = "", **changes):
    values = dict(
        id=uuid4(),
        key=_key(expected_digest),
        checksum=CHECKSUM,
        digest=expected_digest,
        provenance=json.dumps({COLUMN: expected_digest} if expected_digest else {}),
    )
    values.update(changes)
    connection.execute(
        text(f"""
        INSERT INTO {TABLE}
          (id, materialization_key, task_id, task_checksum, cpu_arch,
           task_config, {COLUMN}, task_source_provenance)
        VALUES (:id, :key, 'manifest-identity', :checksum, 'arm64',
          '{{}}'::jsonb, :digest, CAST(:provenance AS jsonb))
    """),
        values,
    )
    return values["id"]


def test_manifest_identity_preserves_legacy_and_versions_uniqueness(
    isolated_migration_postgres_url,
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0135")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        legacy_id = uuid4()
        with engine.begin() as connection:
            connection.execute(
                text(f"""
                INSERT INTO {TABLE}
                  (id, materialization_key, task_id, task_checksum, cpu_arch, task_config)
                VALUES (:id, :key, 'manifest-identity', :checksum, 'arm64', '{{}}'::jsonb)
            """),
                dict(id=legacy_id, key=_key(), checksum=CHECKSUM),
            )
        command.upgrade(config, "head")
        assert {item["name"] for item in inspect(engine).get_columns(TABLE)} == set(
            TaskImageMaterialization.__table__.columns.keys()
        )
        assert COLUMN in TaskImageMaterialization.__table__.columns
        with engine.begin() as connection:
            assert connection.execute(
                text(f"SELECT id, materialization_key, {COLUMN}, state FROM {TABLE}")
            ).one() == (legacy_id, _key(), "", "queued")
            _insert(connection, "b" * 64)
            _insert(connection, "c" * 64)
            assert connection.scalar(text(f"SELECT count(*) FROM {TABLE}")) == 3
        # Both the key uniqueness and the four-part natural identity are required.
        for digest in ("", "b" * 64, "c" * 64):
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    _insert(connection, digest)
        unique = {
            item["name"]: item["column_names"]
            for item in inspect(engine).get_unique_constraints(TABLE)
        }
        assert unique["task_image_materializations_task_arch_uidx"] == [
            "task_id",
            "task_checksum",
            "cpu_arch",
            COLUMN,
            "preparation_revision",
        ]
        with pytest.raises(DBAPIError, match="manifest-qualified materializations"):
            command.downgrade(config, "0135")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
            assert connection.scalar(text(f"SELECT count(*) FROM {TABLE}")) == 3
    finally:
        engine.dispose()


def test_manifest_identity_database_binding_and_immutability(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert COLUMN in {item["name"] for item in inspect(engine).get_columns(TABLE)}
        invalid = [
            dict(digest=None),
            dict(digest="B" * 64),
            dict(digest="b" * 63),
            dict(digest="sha256:" + "b" * 64),
            dict(key="d" * 64),
            *(
                dict(provenance=json.dumps(value))
                for value in (
                    {},
                    None,
                    [],
                    {COLUMN: None},
                    {COLUMN: ""},
                    {COLUMN: "c" * 64},
                    {COLUMN: ["b" * 64]},
                    {COLUMN: True},
                )
            ),
        ]
        for changes in invalid:
            digest = changes.pop("digest", "b" * 64)
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    _insert(connection, "b" * 64, **{**changes, "digest": digest})
        # The absence of a discriminator is not permission to silently erase stronger provenance.
        for value in (None, "", "b" * 64):
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    _insert(connection, provenance=json.dumps({COLUMN: value}))
        with engine.begin() as connection:
            strong_id = _insert(connection, "b" * 64)
            legacy_id = _insert(connection)
        updates = [
            "id = gen_random_uuid()",
            "task_id = 'rewritten'",
            "task_checksum = repeat('d', 64)",
            "cpu_arch = 'x86_64'",
            "materialization_key = repeat('d', 64)",
            "task_config = jsonb_build_object('changed', true)",
            "task_source = 's3://other/source/'",
            "task_source_provenance = task_source_provenance || jsonb_build_object('extra', true)",
            f"{COLUMN} = repeat('d', 64), materialization_key = :other_key, "
            f"task_source_provenance = jsonb_build_object('{COLUMN}', repeat('d', 64))",
            f"{COLUMN} = '', task_source_provenance = '{{}}'::jsonb, materialization_key = :legacy_key",
        ]
        for assignment in updates:
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    connection.execute(
                        text(f"UPDATE {TABLE} SET {assignment} WHERE id = :id"),
                        dict(id=strong_id, legacy_key=_key(), other_key=_key("d" * 64)),
                    )
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(
                    text(f"""UPDATE {TABLE} SET
                    {COLUMN} = repeat('c', 64), materialization_key = :key,
                    task_source_provenance = jsonb_build_object('{COLUMN}', repeat('c', 64))
                    WHERE id = :id"""),
                    dict(id=legacy_id, key=_key("c" * 64)),
                )
        with engine.begin() as connection:
            connection.execute(
                text(f"UPDATE {TABLE} SET state = 'failed', attempt_count = 1 WHERE id = :id"),
                dict(id=strong_id),
            )
            assert (
                connection.scalar(
                    text(f"SELECT state FROM {TABLE} WHERE id = :id"), dict(id=strong_id)
                )
                == "failed"
            )
            # A BEFORE trigger can rewrite columns not named by the UPDATE. The AFTER
            # invariant must still inspect the final row, not just UPDATE OF fields.
            connection.execute(
                text(f"""
                CREATE FUNCTION test_rewrite_identity() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN NEW.task_source := 'rewritten'; RETURN NEW; END $$;
                CREATE TRIGGER test_rewrite_identity BEFORE UPDATE ON {TABLE}
                FOR EACH ROW EXECUTE FUNCTION test_rewrite_identity();
            """)
            )
        with pytest.raises(DBAPIError, match="identity is immutable"):
            with engine.begin() as connection:
                connection.execute(
                    text(f"UPDATE {TABLE} SET attempt_count = 2 WHERE id = :id"), dict(id=strong_id)
                )
    finally:
        engine.dispose()


@pytest.mark.parametrize("provenance", [{COLUMN: "b" * 64}, {COLUMN: None}])
def test_manifest_identity_upgrade_refuses_inferred_authority(
    isolated_migration_postgres_url, provenance
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0135")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(f"""INSERT INTO {TABLE}
                (id, materialization_key, task_id, task_checksum, cpu_arch, task_config, task_source_provenance)
                VALUES (:id, :key, 'manifest-identity', :checksum, 'arm64', '{{}}'::jsonb, CAST(:provenance AS jsonb))
            """),
                dict(id=uuid4(), key=_key(), checksum=CHECKSUM, provenance=json.dumps(provenance)),
            )
        with pytest.raises(DBAPIError, match="unexpected content-manifest provenance"):
            command.upgrade(config, "head")
        assert COLUMN not in {item["name"] for item in inspect(engine).get_columns(TABLE)}
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0135"
    finally:
        engine.dispose()


def test_manifest_identity_migration_legacy_roundtrip_and_busy_lock(
    isolated_migration_postgres_url,
):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            legacy_id = _insert(connection)
        for operation, revision in ((command.downgrade, "0135"), (command.upgrade, "head")):
            with engine.begin() as connection:
                connection.execute(text(f"SELECT id FROM {TABLE}"))
                with pytest.raises(DBAPIError, match="could not obtain lock"):
                    operation(config, revision)
            operation(config, revision)
        with engine.connect() as connection:
            assert connection.execute(
                text(f"SELECT id, materialization_key, {COLUMN} FROM {TABLE}")
            ).one() == (legacy_id, _key(), "")
    finally:
        engine.dispose()


def test_manifest_identity_downgrade_refuses_one_strong_row_without_collision(
    isolated_migration_postgres_url,
):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            row_id = _insert(connection, "b" * 64)
        with pytest.raises(DBAPIError, match="manifest-qualified materializations"):
            command.downgrade(_config(isolated_migration_postgres_url), "0135")
        with engine.connect() as connection:
            assert connection.execute(text(f"SELECT id, {COLUMN} FROM {TABLE}")).one() == (
                row_id,
                "b" * 64,
            )
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
    finally:
        engine.dispose()


async def test_legacy_insert_cannot_silently_reuse_ready_row_for_strong_provenance(
    isolated_migration_postgres_url,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            task = Task(**_task_values(task_id="manifest-identity", checksum=CHECKSUM))
            rows = await ensure_task_image_materializations(session, task_row=task)
            for row in rows:
                row.state = "ready"
                row.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
            await session.commit()
            before = (
                await session.execute(
                    text(
                        f"SELECT id, materialization_key, state, {COLUMN} FROM {TABLE} ORDER BY id"
                    )
                )
            ).all()
            task.source_provenance = {**task.source_provenance, COLUMN: "b" * 64}
            # An older writer omitting the discriminator must still reject,
            # not win ON CONFLICT and reuse a weak ready snapshot.
            with pytest.raises(DBAPIError, match="manifest_binding_check"):
                async with session.begin_nested():
                    await session.execute(
                        text(f"""
                        INSERT INTO {TABLE} (id, materialization_key, task_id,
                          task_checksum, cpu_arch, task_config, task_source_provenance)
                        VALUES (:id, :key, :task, :checksum, :arch, CAST(:config AS jsonb),
                          CAST(:provenance AS jsonb)) ON CONFLICT (materialization_key) DO NOTHING
                    """),
                        dict(
                            id=uuid4(),
                            key=rows[0].materialization_key,
                            task=task.id,
                            checksum=CHECKSUM,
                            arch=rows[0].cpu_arch,
                            config=json.dumps(task.config),
                            provenance=json.dumps(task.source_provenance),
                        ),
                    )
            assert (
                await session.execute(
                    text(
                        f"SELECT id, materialization_key, state, {COLUMN} FROM {TABLE} ORDER BY id"
                    )
                )
            ).all() == before
    finally:
        await engine.dispose()


@pytest.mark.parametrize("manifest", ["", "b" * 64])
def test_native_preparation_upgrade_keeps_old_rows_and_rejects_wrong_keys_or_relabeling(isolated_migration_postgres_url, manifest):
    from loom.task_image_recipe import NATIVE_HARBOR_PREPARATION, task_image_materialization_key

    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0180")
    engine = create_engine(isolated_migration_postgres_url)
    native = json.dumps({"upstream_conversion": {"importer": "loom.harbor-native.v1"}})
    legacy_id = uuid4()
    identity = dict(task_id="native-recipe-upgrade", task_checksum=CHECKSUM, cpu_arch="arm64")
    legacy_key = task_image_materialization_key(**identity)
    current_key = task_image_materialization_key(**identity, preparation_revision=NATIVE_HARBOR_PREPARATION, bundle_content_manifest_sha256=manifest)
    try:
        with engine.begin() as connection:
            connection.execute(text("""INSERT INTO task_image_materializations
                (id,materialization_key,task_id,task_checksum,cpu_arch,task_config,state,registry_images)
                VALUES (:id,:key,:task_id,:task_checksum,:cpu_arch,CAST(:config AS jsonb),'ready',CAST(:images AS jsonb))"""),
                {**identity,"id":legacy_id,"key":legacy_key,"config":native,"images":json.dumps({"task":"registry.test/old@sha256:"+"d"*64})})
        command.upgrade(config, "head")
        with engine.begin() as connection:
            row = connection.execute(text("SELECT id,materialization_key,preparation_revision,state,registry_images FROM task_image_materializations WHERE id=:id"),{"id":legacy_id}).one()
            assert row == (legacy_id,legacy_key,"","ready",{"task":"registry.test/old@sha256:"+"d"*64})
        statement = text("""INSERT INTO task_image_materializations
            (id,materialization_key,task_id,task_checksum,cpu_arch,task_config,preparation_revision,bundle_content_manifest_sha256,task_source_provenance)
            VALUES (:id,:key,:task_id,:task_checksum,:cpu_arch,CAST(:config AS jsonb),:recipe,:manifest,CAST(:provenance AS jsonb))""")
        values = {**identity,"id":uuid4(),"key":current_key,"config":native,"recipe":NATIVE_HARBOR_PREPARATION,"manifest":manifest,"provenance":json.dumps({"bundle_content_manifest_sha256":manifest} if manifest else {})}
        for changes in ({"key":"f"*64},{"recipe":"unknown"},{"config":"{}"}):
            with pytest.raises(DBAPIError, match="manifest_key_check"):
                with engine.begin() as connection:
                    connection.execute(statement,{**values,**changes})
        with engine.begin() as connection:
            connection.execute(statement,values)
        for row_id,assignment in ((legacy_id,"preparation_revision='harbor-reference-directory-v1'"),
                                  (values["id"],"preparation_revision=''"),
                                  (values["id"],"task_source='s3://different/source'"),
                                  (values["id"],"task_config=task_config || jsonb_build_object('different',true)")):
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    connection.execute(text(f"UPDATE task_image_materializations SET {assignment} WHERE id=:id"),{"id":row_id})
        with pytest.raises(DBAPIError,match="recipe-qualified materializations prevent downgrade"):
            command.downgrade(config,"0180")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM task_image_materializations")) == 2
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
    finally:
        engine.dispose()
