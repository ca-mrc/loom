from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, Trial, TrialTaskImageMaterialization
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_materialization import (
    TaskImageSnapshotConflictError,
    ensure_task_image_materializations,
)
from loom_control_plane.task_image_materializations import _durable_reference_exists
from tests.integration.test_task_bundle_source_journal import _publish, _receipts, _upload
from tests.unit.test_task_bundle_registration import _bundle

KEY = "bundle_content_manifest_sha256"


def _source_bundle(tmp_path):
    root = _bundle(tmp_path)
    config = root / "task.toml"
    config.write_text(config.read_text().replace('os = "linux"', 'os = "linux"\ncpu_arch = "any"'))
    return root


async def _registered(engine, root):
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(root, task_id="manifest-store"), bucket="task-sources"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    ticket = await _upload(factory, spec)
    await _receipts(factory, ticket)
    await _publish(factory, ticket)
    return spec


def _task(spec):
    return Task(
        id=spec.catalog_task_id,
        checksum=spec.manifest.task_checksum,
        config=spec.task_config,
        source=spec.source_uri,
        source_provenance=spec.provenance,
    )


async def test_ensure_versions_strong_rows_and_preserves_exact_historical_trial_pins(
    isolated_migration_postgres_url,
    tmp_path,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        root = _source_bundle(tmp_path)
        first_spec = await _registered(engine, root)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            task = _task(first_spec)
            task.source_provenance = {}
            session.add(task)
            await session.flush()
            legacy = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            for row in legacy:
                row.state = "ready"
                row.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
            await session.commit()
            task.source_provenance = first_spec.provenance
            first = await ensure_task_image_materializations(session, task_row=task)
            repeated = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            assert [row.id for row in repeated] == [row.id for row in first]
            assert all(
                row.state == "queued"
                and row.bundle_content_manifest_sha256 == first_spec.manifest.digest
                for row in first
            )
            assert not {row.id for row in legacy} & {row.id for row in first}
            (root / "Dockerfile").chmod(0o755)
            second_spec = await _registered(engine, root)
            assert second_spec.manifest.task_checksum == first_spec.manifest.task_checksum
            assert second_spec.manifest.digest != first_spec.manifest.digest
            task.source, task.source_provenance = second_spec.source_uri, second_spec.provenance
            second = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            assert len(legacy) == len(first) == len(second) == 1
            assert {row.cpu_arch for row in (*legacy, *first, *second)} == {"x86_64"}
            assert len({row.materialization_key for row in (*legacy, *first, *second)}) == 3
            # Catalog retention follows the full identity, not the old checksum.
            for rows, expected in ((legacy, False), (first, False), (second, True)):
                for row in rows:
                    assert (
                        bool(await session.scalar(select(_durable_reference_exists(row))))
                        is expected
                    )
            team = Team(id=uuid4(), name="manifest-reference")
            session.add(team)
            await session.flush()
            trial = Trial(
                id=uuid4(),
                team_id=team.id,
                task_id=task.id,
                config={},
                requires_caps={},
                state="queued",
            )
            session.add(trial)
            await session.flush()
            session.add(
                TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=legacy[0].id)
            )
            await session.commit()
            assert await session.scalar(select(_durable_reference_exists(legacy[0])))
            # Pinning one historical version must not retain a different superseded manifest.
            assert not await session.scalar(select(_durable_reference_exists(first[0])))
            assert legacy[0].state == "ready"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("changed", ["config", "source", "provenance"])
async def test_strong_ensure_rejects_different_frozen_snapshot(
    isolated_migration_postgres_url, tmp_path, changed
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        spec = await _registered(engine, _source_bundle(tmp_path))
        async with AsyncSession(engine, expire_on_commit=False) as session:
            task = _task(spec)
            original = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            if changed == "config":
                task.config = {**task.config, "task": {**task.config["task"], "name": "changed"}}
            elif changed == "source":
                task.source = "s3://another-bucket/another-prefix/"
            else:
                task.source_provenance = {**task.source_provenance, "extra": "changed"}
            with pytest.raises(TaskImageSnapshotConflictError, match=r"frozen.*snapshot"):
                await ensure_task_image_materializations(session, task_row=task)
            assert all(row.task_config["task"]["name"] == "Source task" for row in original)
    finally:
        await engine.dispose()
