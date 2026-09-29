"""Repair 0167 catalog drift without changing frozen image evidence."""
from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageMaterialization
from loom.task_image_materialization import (
    ensure_task_image_materializations,
    task_image_materialization_key,
)
from tests.integration.test_nebius_application_registry import migrate


@pytest.fixture
def repair_database(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


def _config(task_id: str, mode: str = "shared") -> dict:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {
            "os": "linux", "cpu_arch": "any", "dockerfile": "environment/Dockerfile",
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest", "env_mode": mode},
        "steps": [{"name": "main"}],
    }


def _seed(connection, *, state="queued", current=None, snapshot=None,
          checksum="a" * 64, snapshot_checksum=None, task_provenance=None,
          snapshot_manifest="", cpu_arch="x86_64", task_id=None, add_task=True):
    task_id = task_id or f"verifier-repair/{uuid4()}"
    current = current or _config(task_id)
    snapshot = snapshot or copy.deepcopy(current)
    source = f"s3://loom-tasks/{task_id}/bundle/"
    if add_task:
        connection.execute(text("""
            INSERT INTO tasks (id, checksum, config, source, source_provenance)
            VALUES (:id, :checksum, CAST(:config AS jsonb), :source, CAST(:provenance AS jsonb))
        """), {
            "id": task_id, "checksum": checksum, "config": json.dumps(current),
            "source": source, "provenance": json.dumps(task_provenance or {}),
        })
    snapshot_checksum = snapshot_checksum or checksum.removeprefix("sha256:")
    provenance = ({"bundle_content_manifest_sha256": snapshot_manifest}
                  if snapshot_manifest else {})
    connection.execute(insert(TaskImageMaterialization).values(
        id=uuid4(), task_id=task_id, task_checksum=snapshot_checksum, cpu_arch=cpu_arch,
        materialization_key=task_image_materialization_key(
            task_id=task_id, task_checksum=snapshot_checksum, cpu_arch=cpu_arch,
            bundle_content_manifest_sha256=snapshot_manifest,
        ),
        bundle_content_manifest_sha256=snapshot_manifest,
        task_config=snapshot, task_source=source, task_source_provenance=provenance,
        state=state,
        registry_images=({"task": "registry.test/task@sha256:" + "b" * 64}
                         if state == "ready" else {}),
    ))
    return task_id


def _snapshot_rows(connection):
    return connection.execute(text(
        "SELECT * FROM task_image_materializations ORDER BY id"
    )).mappings().all()


async def _ensure(engine, task):
    async_engine = create_async_engine(engine.url)
    try:
        async with async_sessionmaker(async_engine)() as session:
            rows = await ensure_task_image_materializations(
                session, task_row=SimpleNamespace(**task),
            )
            assert rows
            # Verification must not update last-reference timestamps in our evidence.
            await session.rollback()
    finally:
        await async_engine.dispose()


@pytest.mark.parametrize("state", ["ready", "queued", "failed"])
def test_0167_drift_is_repaired_without_rewriting_frozen_images(repair_database, state):
    migrate(repair_database, "downgrade", "0166")
    with repair_database.begin() as connection:
        task_id = _seed(connection, state=state, checksum="sha256:" + "a" * 64)
        frozen = _snapshot_rows(connection)
    migrate(repair_database, "upgrade", "0167")
    with repair_database.connect() as connection:
        broken = connection.execute(text("SELECT * FROM tasks WHERE id=:id"),
                                    {"id": task_id}).mappings().one()
        assert broken["config"]["verifier"]["env_mode"] == "separate"
    with pytest.raises(RuntimeError, match="snapshot conflicts with task checksum"):
        asyncio.run(_ensure(repair_database, broken))

    migrate(repair_database, "upgrade", "head")
    with repair_database.connect() as connection:
        repaired = connection.execute(text("SELECT * FROM tasks WHERE id=:id"),
                                      {"id": task_id}).mappings().one()
        assert repaired["config"] == frozen[0]["task_config"]
        assert repaired["legacy_separate_verifier_checksum"] == "a" * 64
        assert repaired["checksum"] == broken["checksum"]
        assert repaired["source"] == broken["source"]
        assert repaired["source_provenance"] == broken["source_provenance"]
        assert _snapshot_rows(connection) == frozen
    asyncio.run(_ensure(repair_database, repaired))
    migrate(repair_database, "upgrade", "head")
    with repair_database.connect() as connection:
        assert _snapshot_rows(connection) == frozen
    with pytest.raises(DBAPIError, match="cannot discard legacy verifier compatibility"):
        migrate(repair_database, "downgrade", "0168")
    with repair_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0169"
        assert _snapshot_rows(connection) == frozen


@pytest.mark.parametrize("case", [
    "new_shared", "other_config_difference", "ambiguous_architectures",
    "manifest_task", "manifest_snapshot", "different_checksum", "no_snapshot",
])
def test_repair_does_not_reinterpret_unrelated_revisions(repair_database, case):
    migrate(repair_database, "downgrade", "0168")
    task_id = f"verifier-repair/{uuid4()}"
    snapshot = _config(task_id)
    current = _config(task_id, "shared" if case == "new_shared" else "separate")
    if case == "other_config_difference":
        current["agent"]["name"] = "different-agent"
    with repair_database.begin() as connection:
        _seed(
            connection, task_id=task_id, current=current, snapshot=snapshot,
            task_provenance=({"bundle_content_manifest_sha256": "c" * 64}
                             if case == "manifest_task" else None),
            snapshot_manifest="c" * 64 if case == "manifest_snapshot" else "",
            snapshot_checksum="d" * 64 if case == "different_checksum" else None,
        )
        if case == "ambiguous_architectures":
            different = copy.deepcopy(snapshot)
            different["agent"]["name"] = "different-agent"
            _seed(connection, task_id=task_id, snapshot=different,
                  cpu_arch="arm64", add_task=False)
        if case == "no_snapshot":
            connection.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id == task_id,
            ))
        frozen = _snapshot_rows(connection)
    migrate(repair_database, "upgrade", "head")
    with repair_database.connect() as connection:
        task = connection.execute(text("SELECT * FROM tasks WHERE id=:id"),
                                  {"id": task_id}).mappings().one()
        assert task["config"] == current
        assert task["legacy_separate_verifier_checksum"] is None
        assert _snapshot_rows(connection) == frozen
