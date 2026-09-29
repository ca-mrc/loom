from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_materialization import (
    TaskImageSnapshotConflictError,
    ensure_task_image_materializations,
    get_trial_task_image_execution_grant,
)
from loom_control_plane.task_image_materializations import (
    TaskImageLeaseConflictError,
    claim_task_image_materialization,
    claim_task_image_registry_gc,
    complete_task_image_registry_gc,
    fail_task_image_materialization,
    record_task_image_publication,
    retry_task_image_materialization,
)


async def test_registry_gc_scopes_observation_and_claim_to_native_inventory(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    identities = [uuid4(), uuid4()]
    old = datetime.now(UTC) - timedelta(days=31)
    async with materialization_session() as session, session.begin():
        for number, identity in enumerate(identities):
            await session.execute(insert(TaskImageMaterialization).values(
                id=identity, materialization_key=str(number + 1) * 64,
                task_id=f"retention/{identity}", task_checksum="a" * 64, cpu_arch="x86_64",
                task_config={}, task_source=f"s3://test/{identity}", state="failed",
                registry_images={"task": f"registry.test/task@sha256:{number + 1:064x}"},
                unreferenced_at=old,
            ))
    async with materialization_session() as session, session.begin():
        assert await claim_task_image_registry_gc(
            session, gc_id="test", grace_hours=720, materialization_ids=[],
        ) is None
        row = await claim_task_image_registry_gc(
            session, gc_id="test", grace_hours=720, materialization_ids=[identities[0]],
        )
        assert row is not None and row.id == identities[0] and row.state == "retiring"
        epoch = row.lease_epoch
    async with materialization_session() as session, session.begin():
        other = await session.get(TaskImageMaterialization, identities[1])
        assert other is not None and other.state == "failed" and other.unreferenced_at == old
        retired = await complete_task_image_registry_gc(
            session, materialization_id=identities[0], gc_id="test", lease_epoch=epoch,
        )
        assert retired.state == "retired" and not retired.registry_images


def _task_values(*, task_id: str, checksum: str) -> dict[str, object]:
    return {
        "id": task_id,
        "checksum": checksum,
        "source": f"s3://loom-tasks/{task_id}/{checksum}.tar.zst",
        "config": {
            "schema_version": "1",
            "task": {"id": task_id, "name": task_id},
            "environment": {
                "os": "linux",
                "cpu_arch": "any",
                "dockerfile": "environment/Dockerfile",
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        },
        "source_provenance": {"fixture": "task-image-materialization-test"},
    }


@pytest.fixture
async def materialization_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImagePublicationEvidence))
            await session.execute(delete(TaskImageMaterializationAttempt))
            await session.execute(delete(TaskImageMaterialization))
            await session.execute(
                delete(Task).where(
                    Task.source_provenance["fixture"].astext == "task-image-materialization-test"
                )
            )
            await session.commit()
        await engine.dispose()


async def test_ensure_enqueues_idempotent_architecture_snapshots(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    checksum = "1" * 64
    async with materialization_session() as session:
        await session.execute(
            insert(Task).values(**_task_values(task_id=task_id, checksum=checksum))
        )
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()

        first = await ensure_task_image_materializations(session, task_row=task)
        second = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()

        assert [row.cpu_arch for row in first] == ["x86_64"]
        assert [row.id for row in second] == [row.id for row in first]
        assert all(row.state == "queued" for row in first)
        assert all(row.task_config == task.config for row in first)
        assert all(row.task_source == task.source for row in first)
        assert all(row.task_source_provenance == task.source_provenance for row in first)
        count = await session.scalar(
            select(func.count(TaskImageMaterialization.id)).where(
                TaskImageMaterialization.task_id == task_id
            )
        )
        assert count == 1


async def test_publication_evidence_retains_identical_digest_across_attempt_leases(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    checksum = "9" * 64
    registry_image = "registry.example/loom/task@sha256:" + "a" * 64
    async with materialization_session() as session:
        await session.execute(
            insert(Task).values(**_task_values(task_id=task_id, checksum=checksum))
        )
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        await ensure_task_image_materializations(session, task_row=task)
        await session.commit()

        first = await claim_task_image_materialization(
            session,
            builder_id="builder:first",
            cpu_arch="x86_64",
        )
        assert first is not None
        assert (first.attempt_count, first.lease_epoch) == (1, 1)
        await record_task_image_publication(
            session,
            materialization_id=first.id,
            builder_id="builder:first",
            attempt_count=first.attempt_count,
            lease_epoch=first.lease_epoch,
            component="task",
            registry_image=registry_image,
        )
        await fail_task_image_materialization(
            session,
            materialization_id=first.id,
            builder_id="builder:first",
            lease_epoch=first.lease_epoch,
            retryable=False,
            failure_reason="fixture_failure",
            failure_message="force an admin retry",
            registry_images={"task": registry_image},
        )
        await retry_task_image_materialization(session, materialization_id=first.id)
        await session.commit()

        second = await claim_task_image_materialization(
            session,
            builder_id="builder:second",
            cpu_arch="x86_64",
        )
        assert second is not None
        assert second.id == first.id
        assert (second.attempt_count, second.lease_epoch) == (1, 3)
        for _ in range(2):
            await record_task_image_publication(
                session,
                materialization_id=second.id,
                builder_id="builder:second",
                attempt_count=second.attempt_count,
                lease_epoch=second.lease_epoch,
                component="task",
                registry_image=registry_image,
            )
        await session.commit()

        attempts = list(
            (
                await session.scalars(
                    select(TaskImageMaterializationAttempt)
                    .where(TaskImageMaterializationAttempt.materialization_id == second.id)
                    .order_by(TaskImageMaterializationAttempt.lease_epoch)
                )
            ).all()
        )
        evidence = list(
            (
                await session.scalars(
                    select(TaskImagePublicationEvidence)
                    .where(TaskImagePublicationEvidence.materialization_id == second.id)
                    .order_by(TaskImagePublicationEvidence.lease_epoch)
                )
            ).all()
        )
        assert [(row.attempt_number, row.lease_epoch, row.builder_id) for row in attempts] == [
            (1, 1, "builder:first"),
            (1, 3, "builder:second"),
        ]
        assert all(
            (
                row.grant_id,
                row.session_id,
                row.session_generation,
                row.claim_id,
            )
            == (None, None, None, None)
            for row in attempts
        )
        assert [
            (row.attempt_number, row.lease_epoch, row.builder_id, row.registry_image)
            for row in evidence
        ] == [
            (1, 1, "builder:first", registry_image),
            (1, 3, "builder:second", registry_image),
        ]
        refreshed = await session.get(TaskImageMaterialization, second.id)
        assert refreshed is not None
        assert [entry["lease_epoch"] for entry in refreshed.registry_image_history] == [1, 3]

        with pytest.raises(TaskImageLeaseConflictError, match="attempt"):
            await record_task_image_publication(
                session,
                materialization_id=second.id,
                builder_id="builder:forged",
                attempt_count=second.attempt_count,
                lease_epoch=second.lease_epoch,
                component="task",
                registry_image=registry_image,
            )


async def test_new_task_checksum_creates_new_immutable_materializations(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    original_checksum = "2" * 64
    replacement_checksum = "3" * 64
    async with materialization_session() as session:
        await session.execute(
            insert(Task).values(**_task_values(task_id=task_id, checksum=original_checksum))
        )
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        original = await ensure_task_image_materializations(session, task_row=task)
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                checksum=replacement_checksum,
                source=f"s3://loom-tasks/{task_id}/{replacement_checksum}.tar.zst",
            )
        )
        await session.commit()
        replacement_task = (
            await session.execute(select(Task).where(Task.id == task_id))
        ).scalar_one()

        replacement = await ensure_task_image_materializations(
            session,
            task_row=replacement_task,
        )
        await session.commit()

        assert {row.materialization_key for row in original}.isdisjoint(
            {row.materialization_key for row in replacement}
        )
        checksums = set(
            (
                await session.execute(
                    select(TaskImageMaterialization.task_checksum).where(
                        TaskImageMaterialization.task_id == task_id
                    )
                )
            ).scalars()
        )
        assert checksums == {original_checksum, replacement_checksum}


@pytest.mark.parametrize("previous_state", ["retired", "retiring"])
async def test_ensure_requeues_retired_images_and_marks_retiring_images_referenced(
    materialization_session: async_sessionmaker[AsyncSession],
    previous_state: str,
) -> None:
    task_id = f"materialization/{uuid4()}"
    old_reference = datetime.now(UTC) - timedelta(days=10)
    async with materialization_session() as session:
        await session.execute(
            insert(Task).values(**_task_values(task_id=task_id, checksum="4" * 64))
        )
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        created = await ensure_task_image_materializations(session, task_row=task)
        await session.execute(
            update(TaskImageMaterialization)
            .where(TaskImageMaterialization.id == created[0].id)
            .values(
                state=previous_state,
                attempt_count=3,
                failure_reason="registry_retired",
                failure_message="registry images were deleted",
                claimed_by="registry-gc-active" if previous_state == "retiring" else None,
                lease_epoch=2,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                last_referenced_at=old_reference,
                unreferenced_at=old_reference,
            )
        )
        await session.commit()

        rows = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()

        assert len(rows) == 1
        assert rows[0].unreferenced_at is None
        assert rows[0].last_referenced_at > old_reference
        if previous_state == "retired":
            assert rows[0].state == "queued"
            assert rows[0].attempt_count == 0
            assert rows[0].failure_reason is None
            assert rows[0].failure_message is None
        else:
            assert rows[0].state == "retiring"
            assert rows[0].claimed_by == "registry-gc-active"


async def test_same_checksum_cannot_rebind_frozen_task_snapshot(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    async with materialization_session() as session:
        await session.execute(insert(Task).values(**_task_values(task_id=task_id, checksum="5" * 64)))
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        rows = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()
        original_source = rows[0].task_source
        task.config = {**task.config, "agent": {"name": "different-agent"}}
        with pytest.raises(TaskImageSnapshotConflictError, match="snapshot conflicts"):
            await ensure_task_image_materializations(session, task_row=task)
        await session.rollback()
        stored = await session.scalar(select(TaskImageMaterialization).where(
            TaskImageMaterialization.task_id == task_id,
        ))
        assert stored is not None and stored.task_source == original_source


async def test_execution_grant_rejects_cross_task_link_and_keeps_original_revision(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    other_task_id = f"materialization/{uuid4()}"
    team_id, trial_id = uuid4(), uuid4()
    async with materialization_session() as session:
        try:
            await session.execute(insert(Team).values(id=team_id, name=f"image-grant-{team_id}"))
            for current_id in (task_id, other_task_id):
                await session.execute(insert(Task).values(**_task_values(
                    task_id=current_id, checksum="6" * 64,
                )))
            task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            rows = await ensure_task_image_materializations(session, task_row=task)
            ready = next(row for row in rows if row.cpu_arch == "x86_64")
            ready.state = "ready"
            ready.registry_images = {"task": "registry.example/task@sha256:" + "7" * 64}
            await session.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id=other_task_id, config={}, requires_caps={},
                state="queued",
            ))
            await session.execute(insert(TrialTaskImageMaterialization).values(
                trial_id=trial_id, materialization_id=ready.id,
            ))
            await session.commit()
            with pytest.raises(RuntimeError, match="no longer has a ready"):
                await get_trial_task_image_execution_grant(
                    session, trial_id=trial_id, cpu_arches=["x86_64"],
                )
            await session.execute(update(Trial).where(Trial.id == trial_id).values(task_id=task_id))
            await session.execute(update(Task).where(Task.id == task_id).values(
                checksum="8" * 64, source="s3://loom-tasks/new-version",
            ))
            grant = await get_trial_task_image_execution_grant(
                session, trial_id=trial_id, cpu_arches=["x86_64"],
            )
            assert grant is not None
            assert grant.materialization_id == ready.id
            assert grant.task_checksum == "6" * 64
            assert grant.task_source != "s3://loom-tasks/new-version"
        finally:
            await session.rollback()
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(Team).where(Team.id == team_id))
            await session.commit()


async def test_same_checksum_new_generation_reuses_frozen_source(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    async with materialization_session() as session:
        await session.execute(insert(Task).values(**_task_values(task_id=task_id, checksum="9" * 64)))
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        original = await ensure_task_image_materializations(session, task_row=task)
        original_ids = {row.id for row in original}
        original_source = original[0].task_source
        original_provenance = original[0].task_source_provenance
        await session.commit()
        task.source = "s3://loom-tasks/new-generation/tasks/unchanged/"
        task.source_provenance = {**task.source_provenance, "import_generation": "replacement"}
        reused = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()
        assert {row.id for row in reused} == original_ids
        assert all(row.task_source == original_source for row in reused)
        assert all(row.task_source_provenance == original_provenance for row in reused)


async def test_retired_same_checksum_rebuild_relocates_collected_source(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    async with materialization_session() as session:
        await session.execute(insert(Task).values(**_task_values(task_id=task_id, checksum="a" * 64)))
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        rows = await ensure_task_image_materializations(session, task_row=task)
        old_ids = {row.id for row in rows}
        for row in rows:
            row.state = "retired"
        task.source = "s3://loom-tasks/reuploaded/tasks/same-content/"
        task.source_provenance = {**task.source_provenance, "generation": "reuploaded"}
        await session.commit()
        requeued = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()
        assert {row.id for row in requeued} == old_ids
        assert all(row.state == "queued" for row in requeued)
        assert all(row.task_source == task.source for row in requeued)
        assert all(row.task_source_provenance == task.source_provenance for row in requeued)
