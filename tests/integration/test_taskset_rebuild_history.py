"""Real publication transactions preserve Trials and their submitted image revisions."""

from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import undefer

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskSet,
    TaskSetMaterializationJob,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.taskset.materialize import MaterializeOutput, TaskRowDraft
from loom_control_plane.routes.trials import _ensure_trial_task_image_links
from loom_service.taskset_materializer import LeaseLost, MaterializationLease, publish_if_current


@dataclass(frozen=True)
class Publication:
    sessions: async_sessionmaker[AsyncSession]
    task_set_id: str
    team_id: UUID
    lease: MaterializationLease

    def draft(self, name: str, revision: str = "a") -> TaskRowDraft:
        task_id = f"{self.task_set_id}/tasks/{name}"
        return TaskRowDraft(
            id=task_id,
            checksum=revision * 64,
            config={
                "schema_version": "1",
                "task": {"id": task_id, "name": f"{name}-{revision}"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "any",
                    "dockerfile": "environment/Dockerfile",
                    "workdir": f"/revision-{revision}",
                },
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            source=f"s3://tasks/{self.task_set_id}/generation-{revision}/{name}/",
            source_provenance={"generation": revision},
            tags={"revision": revision},
        )


@pytest.fixture
async def publication(postgres_url: str) -> AsyncIterator[Publication]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, job_id = uuid4(), uuid4()
    task_set_id = f"ts/{team_id}/rebuild-history"
    value = Publication(
        sessions,
        task_set_id,
        team_id,
        MaterializationLease(job_id=job_id, lease_epoch=2, claimed_by="history-test"),
    )
    try:
        async with sessions() as session, session.begin():
            session.add(Team(id=team_id, name=f"rebuild-history-{team_id}"))
            await session.flush()
            session.add(
                TaskSet(
                    id=task_set_id,
                    owning_team_id=team_id,
                    slug="rebuild-history",
                    display_name="Rebuild history",
                    status="ready",
                    status_reason="previous",
                    intents=["evaluation"],
                    manifest_blob_uri="s3://tasks/manifest.yaml",
                    task_count=1,
                    evaluation_ready=True,
                )
            )
            await session.flush()
            session.add(
                TaskSetMaterializationJob(
                    id=job_id,
                    task_set_id=task_set_id,
                    owning_team_id=team_id,
                    state="running",
                    lease_epoch=2,
                    claimed_by="history-test",
                    lease_heartbeat_at=datetime.now(UTC),
                    published_materialization_generation=1,
                )
            )
        yield value
    finally:
        async with sessions() as session, session.begin():
            trial_ids = select(Trial.id).where(Trial.team_id == team_id)
            await session.execute(
                delete(TrialTaskImageMaterialization).where(
                    TrialTaskImageMaterialization.trial_id.in_(trial_ids),
                )
            )
            await session.execute(delete(Trial).where(Trial.team_id == team_id))
            await session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.startswith(f"{task_set_id}/tasks/"),
                )
            )
            await session.execute(delete(Task).where(Task.task_set_id == task_set_id))
            await session.execute(delete(TaskSet).where(TaskSet.id == task_set_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()


async def _seed(
    publication: Publication, *drafts: TaskRowDraft, history: bool = False
) -> UUID | None:
    async with publication.sessions() as session, session.begin():
        tasks = [
            Task(
                id=draft.id,
                checksum=draft.checksum,
                config=draft.config,
                source=draft.source,
                source_provenance=draft.source_provenance,
                tags=draft.tags,
                task_set_id=publication.task_set_id,
            )
            for draft in drafts
        ]
        session.add_all(tasks)
        await session.flush()
        if not history:
            return None
        trial_id = uuid4()
        await session.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=publication.team_id,
                task_id=tasks[0].id,
                config={"agent_name": "oracle"},
                requires_caps={},
                state="failed",
                failure_reason="previous_oracle_failure",
                result={"fixture": "original"},
            )
        )
        await _ensure_trial_task_image_links(session, trial_id=trial_id, task_row=tasks[0])
        return trial_id


async def _publish(publication: Publication, *drafts: TaskRowDraft, lease=None) -> None:
    async with publication.sessions() as session:
        await publish_if_current(
            session,
            lease=lease or publication.lease,
            task_set_id=publication.task_set_id,
            output=MaterializeOutput(
                task_rows=list(drafts),
                task_count=len(drafts),
                status="ready",
                evaluation_ready=True,
            ),
            claim_ttl_sec=60,
        )


def _columns(row) -> dict:
    return deepcopy({column.key: getattr(row, column.key) for column in row.__table__.columns})


async def test_same_id_rebuild_preserves_trial_and_frozen_image_revision(publication):
    old = publication.draft("kept")
    trial_id = await _seed(publication, old, history=True)
    async with publication.sessions() as session:
        trial_before = _columns(await session.get(Trial, trial_id))
        image_before = (
            await session.scalars(
                select(TaskImageMaterialization).options(
                    undefer(TaskImageMaterialization.preparation_revision),
                ).where(
                    TaskImageMaterialization.task_id == old.id,
                )
            )
        ).one()
        image_id, frozen_before = image_before.id, _columns(image_before)
        assert frozen_before["preparation_revision"] == ""
        registered_at = (await session.get(Task, old.id)).registered_at

    replacement = publication.draft("kept", "b")
    await _publish(publication, replacement)

    async with publication.sessions() as session:
        current = await session.get(Task, old.id)
        assert current.checksum == replacement.checksum
        assert current.config == replacement.config
        assert current.source == replacement.source
        assert current.source_provenance == replacement.source_provenance
        assert current.tags == replacement.tags
        assert current.registered_at == registered_at
        assert current.task_set_id == publication.task_set_id
        assert _columns(await session.get(Trial, trial_id)) == trial_before
        frozen_after = await session.get(TaskImageMaterialization, image_id, options=(
            undefer(TaskImageMaterialization.preparation_revision),
        ))
        assert _columns(frozen_after) == frozen_before
        # A replay of the old submission must retain its original revision.
        await _ensure_trial_task_image_links(session, trial_id=trial_id, task_row=current)
        links = list(
            await session.scalars(
                select(TrialTaskImageMaterialization.materialization_id).where(
                    TrialTaskImageMaterialization.trial_id == trial_id,
                )
            )
        )
        assert links == [image_id]
        images = list(
            await session.scalars(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id == old.id,
                )
            )
        )
        assert {image.task_checksum for image in images} == {old.checksum, replacement.checksum}
        fresh = next(image for image in images if image.task_checksum == replacement.checksum)
        assert (fresh.task_config, fresh.task_source, fresh.task_source_provenance) == (
            replacement.config,
            replacement.source,
            replacement.source_provenance,
        )
        task_set = await session.get(TaskSet, publication.task_set_id)
        job = await session.get(TaskSetMaterializationJob, publication.lease.job_id)
        assert (task_set.status, task_set.task_count, task_set.evaluation_ready) == (
            "ready",
            1,
            True,
        )
        assert (job.state, job.published_materialization_generation) == ("succeeded", 2)


async def test_rebuild_replaces_only_unreferenced_membership(publication):
    kept, removed = publication.draft("kept"), publication.draft("removed")
    await _seed(publication, kept, removed)
    replacement, added = publication.draft("kept", "b"), publication.draft("added", "c")
    await _publish(publication, replacement, added)
    async with publication.sessions() as session:
        tasks = list(
            await session.scalars(select(Task).where(Task.task_set_id == publication.task_set_id))
        )
        assert {task.id for task in tasks} == {replacement.id, added.id}
        assert await session.get(Task, removed.id) is None
        task_set = await session.get(TaskSet, publication.task_set_id)
        assert task_set.task_count == len(tasks) == 2


async def test_referenced_removal_rolls_back_entire_publication(publication):
    removed, kept = publication.draft("removed"), publication.draft("kept")
    trial_id = await _seed(publication, removed, kept, history=True)
    async with publication.sessions() as session:
        job_before = _columns(
            await session.get(TaskSetMaterializationJob, publication.lease.job_id)
        )
        task_set_before = _columns(await session.get(TaskSet, publication.task_set_id))
        tasks_before = {
            task.id: _columns(task)
            for task in await session.scalars(
                select(Task).where(Task.task_set_id == publication.task_set_id),
            )
        }
    with pytest.raises(IntegrityError, match="trials_task_id_fkey"):
        await _publish(publication, publication.draft("kept", "b"), publication.draft("added", "c"))
    async with publication.sessions() as session:
        assert (
            _columns(await session.get(TaskSetMaterializationJob, publication.lease.job_id))
            == job_before
        )
        assert _columns(await session.get(TaskSet, publication.task_set_id)) == task_set_before
        assert {
            task.id: _columns(task)
            for task in await session.scalars(
                select(Task).where(Task.task_set_id == publication.task_set_id),
            )
        } == tasks_before
        assert (await session.get(Trial, trial_id)).task_id == removed.id


async def test_stale_owner_cannot_replace_referenced_task(publication):
    old = publication.draft("kept")
    trial_id = await _seed(publication, old, history=True)
    stale = MaterializationLease(
        job_id=publication.lease.job_id,
        lease_epoch=1,
        claimed_by=publication.lease.claimed_by,
    )
    with pytest.raises(LeaseLost):
        await _publish(publication, publication.draft("kept", "b"), lease=stale)
    async with publication.sessions() as session:
        assert (await session.get(Task, old.id)).checksum == old.checksum
        assert (await session.get(Trial, trial_id)).task_id == old.id
        job = await session.get(TaskSetMaterializationJob, publication.lease.job_id)
        assert (job.state, job.published_materialization_generation) == ("running", 1)


async def test_duplicate_retained_ids_reject_without_changing_publication(publication):
    old = publication.draft("kept")
    trial_id = await _seed(publication, old, history=True)

    async def snapshot():
        async with publication.sessions() as session:
            return {
                "task_set": _columns(await session.get(TaskSet, publication.task_set_id)),
                "job": _columns(
                    await session.get(TaskSetMaterializationJob, publication.lease.job_id)
                ),
                "trial": _columns(await session.get(Trial, trial_id)),
                "tasks": {
                    row.id: _columns(row)
                    for row in await session.scalars(
                        select(Task).where(Task.task_set_id == publication.task_set_id),
                    )
                },
                "images": {
                    row.id: _columns(row)
                    for row in await session.scalars(
                        select(TaskImageMaterialization).options(
                            undefer(TaskImageMaterialization.preparation_revision),
                        ).where(
                            TaskImageMaterialization.task_id == old.id
                        ),
                    )
                },
                "links": {
                    row.materialization_id: _columns(row)
                    for row in await session.scalars(
                        select(TrialTaskImageMaterialization).where(
                            TrialTaskImageMaterialization.trial_id == trial_id,
                        ),
                    )
                },
            }

    before = await snapshot()
    # Both drafts already exist in the catalog. Reusing their ORM object must
    # not turn a duplicate-key publication into a last-write-wins update.
    with pytest.raises((IntegrityError, ValueError)) as rejected:
        await _publish(publication, publication.draft("kept", "b"), publication.draft("kept", "c"))
    if isinstance(rejected.value, ValueError):
        assert "duplicate" in str(rejected.value).lower()
    assert await snapshot() == before
