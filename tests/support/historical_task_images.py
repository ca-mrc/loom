"""Frozen pre-retirement rows for disposable migration/retained-data tests.

Captured from 020a6e928 using the former test issuer. These public audit rows
contain no signing keys or usable credentials. Restore bypass is restricted to
fixture setup on disposable test databases; all assertions run with guards on.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt

NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parents[1] / "fixtures" / "historical"
# json_populate_recordset supplies NULL rather than column defaults for fields
# absent from a frozen record. These rows predate preparation recipes and must
# retain the legacy recipe, without rewriting any captured field or fixture.
LEGACY_COLUMN_DEFAULTS = {"task_image_materializations": {"preparation_revision": ""}}


@pytest.fixture
async def registry_authority_session(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def restore_rows(session, fixture):
    data = json.loads((FIXTURES / f"{fixture}.json").read_text())
    await session.execute(text("SET LOCAL session_replication_role = replica"))
    for table, rows in data.items():
        assert table.replace("_", "").isalnum()
        rows = [{**LEGACY_COLUMN_DEFAULTS.get(table, {}), **row} for row in rows]
        await session.execute(text(f'DELETE FROM "{table}"'))
        await session.execute(text(
            f'INSERT INTO "{table}" SELECT * FROM json_populate_recordset(NULL::"{table}", :rows)'
        ), {"rows": json.dumps(rows)})
    await session.execute(text("SET LOCAL session_replication_role = origin"))


async def historical_attempt(session):
    await restore_rows(session, "task_image_attempt")
    materialization = (await session.scalars(select(TaskImageMaterialization))).one()
    attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
    return materialization, attempt


async def credential_values(session, value):
    return dict((await session.execute(text(
        "SELECT (json_populate_record(NULL::task_image_registry_credentials, :row)).*"
    ), {"row": json.dumps(value)})).mappings().one())


async def _prepared_insert(factory):
    async with factory() as session:
        await restore_rows(session, "retirable_task_image_attempt")
        attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
        values = await credential_values(session, json.loads(
            (FIXTURES / "historical_registry_credential.json").read_text()
        ))
        await session.commit()
        return attempt.id, values


async def _retire(factory, attempt_id):
    import hashlib
    from datetime import timedelta

    from loom.db.schema import TaskImageAttemptRetention
    from loom_task_image_authority.retention_inventory import derive_attempt_repository_inventory

    async with factory() as session:
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        # Sealed-owner handoff tests deliberately retain a pre-recipe schema.
        # Read its real columns, then validate a transient legacy snapshot;
        # never issue a current ORM query or rewrite the historical database.
        values = (await session.execute(text(
            "SELECT * FROM task_image_materializations WHERE id = :id"
        ), {"id": attempt.materialization_id})).mappings().one()
        materialization = TaskImageMaterialization(
            **{**LEGACY_COLUMN_DEFAULTS["task_image_materializations"], **values}
        )
        inventory = derive_attempt_repository_inventory(
            materialization=materialization, attempt=attempt, credentials=[],
            registry_origin="https://registry.example:5443",
        )
        row = await session.get(TaskImageAttemptRetention, attempt_id)
        if row is None:
            row = TaskImageAttemptRetention(attempt_id=attempt_id)
            session.add(row)
        row.observed_at = NOW + timedelta(hours=25)
        row.unreferenced_since = NOW + timedelta(hours=1)
        row.retired_at = row.observed_at
        row.canonical_inventory = inventory.canonical_bytes
        row.inventory_sha256 = hashlib.sha256(inventory.canonical_bytes).hexdigest()
        await session.commit()


async def _insert(session, values):
    from sqlalchemy import insert

    from loom.db.schema import TaskImageRegistryCredentialGeneration

    await session.execute(insert(TaskImageRegistryCredentialGeneration).values(**values))


async def _pair(factory):
    async with factory() as session:
        await restore_rows(session, "retired_and_live_task_image_attempts")
        result = [await credential_values(session, value) for value in json.loads(
            (FIXTURES / "historical_registry_credential_pair.json").read_text()
        )]
        await session.commit()
        return result
