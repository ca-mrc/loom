"""Background cleanup for retained TaskSets and abandoned staged generations."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import re
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, delete, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    Batch,
    Task,
    TaskBundleSource,
    TaskImageMaterialization,
    TaskSet,
    TaskSetGenerationGcCursor,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.taskset.storage_bytes import (
    generated_tasks_prefix,
    generation_prefix,
    generation_root,
    taskset_root,
)

logger = logging.getLogger(__name__)

_ACTIVE_JOB_STATES = frozenset({"claimed", "running"})
_CANONICAL_EPOCH_RE = re.compile(r"(?:0|[1-9][0-9]*)$")
_DELETE_BATCH_SIZE = 1_000
_ROOT_GC_OBJECT_DELETE_BUDGET = 1_000
_LIVE_GC_TASK_SET_LIMIT = 100
_LIVE_GC_JOB_LIMIT = 100
_LIVE_GC_OBJECT_DELETE_BUDGET = 1_000
_LIVE_GENERATION_GC_CURSOR_NAME = "live-generation-gc"


@dataclass(frozen=True)
class PrefixDeleteResult:
    """The bounded outcome of one S3 prefix-delete attempt."""

    attempted_objects: int = 0
    deleted_objects: int = 0
    error_objects: int = 0
    partial: bool = False


@dataclass(frozen=True)
class MaterializationGenerationGcResult:
    """Counters for one bounded live-generation reconciliation pass."""

    candidate_generations: int = 0
    protected_generations: int = 0
    attempted_objects: int = 0
    deleted_objects: int = 0
    error_objects: int = 0
    partial: bool = False


@dataclass(frozen=True)
class _LiveTaskSet:
    id: str
    owning_team_id: UUID
    slug: str


@dataclass(frozen=True)
class _LiveJob:
    id: UUID
    task_set_id: str
    state: str
    lease_epoch: int


@dataclass(frozen=True)
class _GenerationCandidate:
    task_set: _LiveTaskSet
    job: _LiveJob
    epoch: int
    prefix: str
    tasks_prefix: str


async def _load_live_generation_gc_sweep(session: AsyncSession) -> int:
    """Return the durable next sequence without holding a transaction for I/O."""
    try:
        await session.execute(
            pg_insert(TaskSetGenerationGcCursor)
            .values(name=_LIVE_GENERATION_GC_CURSOR_NAME, next_sweep=0)
            .on_conflict_do_nothing(index_elements=["name"]),
        )
        next_sweep = (await session.execute(
            select(TaskSetGenerationGcCursor.next_sweep).where(
                TaskSetGenerationGcCursor.name == _LIVE_GENERATION_GC_CURSOR_NAME,
            ),
        )).scalar_one()
        await session.commit()
        return next_sweep
    except Exception:
        await session.rollback()
        raise


async def _advance_live_generation_gc_sweep(
    session: AsyncSession,
    *,
    completed_sweep: int,
) -> None:
    """Advance once iff this worker completed the sequence it selected."""
    try:
        advanced_to = (await session.execute(
            update(TaskSetGenerationGcCursor)
            .where(
                TaskSetGenerationGcCursor.name == _LIVE_GENERATION_GC_CURSOR_NAME,
                TaskSetGenerationGcCursor.next_sweep == completed_sweep,
            )
            .values(next_sweep=completed_sweep + 1)
            .returning(TaskSetGenerationGcCursor.next_sweep),
        )).scalar_one_or_none()
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    if advanced_to is None:
        logger.info(
            "taskset_generation_gc sequence %d was already advanced by another worker",
            completed_sweep,
        )


def _storage_prefix(*, team_id: UUID | str, slug: str) -> str:
    """Compatibility wrapper for the delimiter-safe TaskSet root."""
    return taskset_root(team_id=team_id, slug=slug)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


@contextlib.contextmanager
def _with_delete_objects_content_md5(client: Any) -> Iterator[None]:
    """Add the MD5 MinIO requires after Botocore has serialized each delete body."""
    events = getattr(getattr(client, "meta", None), "events", None)
    if events is None:
        yield
        return

    def add_content_md5(*, request: Any, **_kwargs: Any) -> None:
        body = request.body
        if isinstance(body, bytes):
            digest = hashlib.md5(body, usedforsecurity=False).digest()
            request.headers["Content-MD5"] = base64.b64encode(digest).decode("ascii")

    event_name = "before-sign.s3.DeleteObjects"
    try:
        events.register(event_name, add_content_md5)
    except Exception:
        logger.warning("taskset_gc: could not register DeleteObjects MD5 hook", exc_info=True)
        yield
        return
    try:
        yield
    finally:
        events.unregister(event_name, add_content_md5)


def _delete_s3_prefix(
    client: Any,
    *,
    bucket: str,
    prefix: str,
    max_objects: int | None = None,
) -> PrefixDeleteResult:
    """Delete up to ``max_objects`` below an exact prefix and report every error."""
    if max_objects is not None and max_objects <= 0:
        return PrefixDeleteResult(partial=True)

    attempted = 0
    deleted = 0
    errors = 0
    partial = False
    remaining = max_objects
    try:
        with _with_delete_objects_content_md5(client):
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = page.get("Contents", [])
                keys = [
                    {"Key": key}
                    for obj in objects
                    if isinstance((key := obj.get("Key")), str)
                ]
                if not keys:
                    continue
                if remaining is not None and remaining <= 0:
                    partial = True
                    break
                if remaining is not None and len(keys) > remaining:
                    keys = keys[:remaining]
                    partial = True

                for start in range(0, len(keys), _DELETE_BATCH_SIZE):
                    batch = keys[start:start + _DELETE_BATCH_SIZE]
                    attempted += len(batch)
                    try:
                        response = client.delete_objects(
                            Bucket=bucket,
                            Delete={"Objects": batch},
                        )
                    except Exception:
                        logger.warning(
                            "taskset_gc: delete_objects failed for prefix %s", prefix,
                            exc_info=True,
                        )
                        errors += len(batch)
                        continue

                    if not isinstance(response, Mapping):
                        logger.warning(
                            "taskset_gc: delete_objects returned an invalid result for %s",
                            prefix,
                        )
                        errors += len(batch)
                        continue
                    deleted_entries = response.get("Deleted", [])
                    error_entries = response.get("Errors", [])
                    if not isinstance(deleted_entries, list) or not isinstance(error_entries, list):
                        logger.warning(
                            "taskset_gc: delete_objects omitted object results for %s",
                            prefix,
                        )
                        errors += len(batch)
                        continue
                    deleted_count = min(len(deleted_entries), len(batch))
                    error_count = min(len(error_entries), len(batch) - deleted_count)
                    deleted += deleted_count
                    errors += error_count + (len(batch) - deleted_count - error_count)

                if remaining is not None:
                    remaining -= len(keys)
    except Exception:
        logger.warning(
            "taskset_gc: listing prefix %s failed", prefix,
            exc_info=True,
        )
        errors += 1

    return PrefixDeleteResult(
        attempted_objects=attempted,
        deleted_objects=deleted,
        error_objects=errors,
        partial=partial,
    )


def _delete_single_blob(client: Any, blob_uri: str | None) -> None:
    if blob_uri is None:
        return
    try:
        bucket, key = _parse_s3_uri(blob_uri)
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        logger.warning("gc: failed to delete blob %s", blob_uri, exc_info=True)


def _listed_generation_epochs(
    client: Any,
    *,
    bucket: str,
    job_prefix: str,
    max_epoch: int,
) -> set[int]:
    """Discover only canonical epoch segments under one DB-derived job prefix."""
    epochs: set[int] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=job_prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not isinstance(key, str) or not key.startswith(job_prefix):
                continue
            epoch_segment, separator, remainder = key[len(job_prefix):].partition("/")
            if not separator or not remainder or not _CANONICAL_EPOCH_RE.fullmatch(epoch_segment):
                continue
            epoch = int(epoch_segment)
            if epoch <= max_epoch:
                epochs.add(epoch)
    return epochs


def _source_protects_candidate(
    source: str | None,
    *,
    artifacts_bucket: str,
    candidate: _GenerationCandidate,
) -> bool:
    if source is None:
        return False
    try:
        bucket, key = _parse_s3_uri(source)
    except ValueError:
        return False
    return bucket == artifacts_bucket and key.startswith(candidate.tasks_prefix)


async def _protected_task_image_sources(
    session: AsyncSession, *, source_prefixes: Sequence[str],
) -> list[str]:
    """Keep build inputs only while a cached image or durable consumer needs them."""
    if not source_prefixes:
        return []
    current_version = exists().where(
        Task.id == TaskImageMaterialization.task_id,
        or_(Task.checksum == TaskImageMaterialization.task_checksum,
            Task.checksum == func.concat("sha256:", TaskImageMaterialization.task_checksum)),
    )
    live_trial = exists().where(
        TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        Trial.id == TrialTaskImageMaterialization.trial_id,
        Trial.state.not_in(("succeeded", "failed", "cancelled")),
    )
    sources = await session.scalars(select(TaskImageMaterialization.task_source).where(
        or_(*(TaskImageMaterialization.task_source.startswith(prefix, autoescape=True)
              for prefix in source_prefixes)),
        or_(current_version, live_trial,
            TaskImageMaterialization.state == "ready",
            and_(TaskImageMaterialization.state.in_(("claimed", "running")),
                 TaskImageMaterialization.lease_expires_at > func.now())),
    ))
    return [source for source in sources if source is not None]


async def _candidate_is_protected_after_recheck(
    session: AsyncSession,
    *,
    candidate: _GenerationCandidate,
    artifacts_bucket: str,
) -> tuple[bool, bool]:
    """Return ``(protected, query_failed)`` from a short final DB snapshot."""
    try:
        task_set = await session.get(TaskSet, candidate.task_set.id)
        job = await session.get(TaskSetMaterializationJob, candidate.job.id)
        if (
            task_set is None
            or task_set.soft_deleted_at is not None
            or task_set.status == "deleted"
            or job is None
            or job.task_set_id != candidate.task_set.id
            or candidate.epoch > job.lease_epoch
        ):
            return True, False

        current_prefix = generation_prefix(
            team_id=task_set.owning_team_id,
            slug=task_set.slug,
            job_id=job.id,
            epoch=candidate.epoch,
        )
        if current_prefix != candidate.prefix:
            return True, False
        if job.state in _ACTIVE_JOB_STATES and candidate.epoch == job.lease_epoch:
            return True, False

        sources = (await session.execute(
            select(Task.source).where(Task.task_set_id == task_set.id),
        )).scalars().all()
        image_sources = await _protected_task_image_sources(
            session, source_prefixes=[f"s3://{artifacts_bucket}/{candidate.tasks_prefix}"],
        )
        return any(
            _source_protects_candidate(
                source,
                artifacts_bucket=artifacts_bucket,
                candidate=candidate,
            )
            for source in [*sources, *image_sources]
        ), False
    except Exception:
        logger.warning(
            "taskset_gc: final DB recheck failed for generation %s",
            candidate.prefix,
            exc_info=True,
        )
        return True, True
    finally:
        await session.rollback()


async def purge_abandoned_materialization_generations(
    session: AsyncSession,
    *,
    minio_client: Any,
    artifacts_bucket: str,
    task_set_limit: int = _LIVE_GC_TASK_SET_LIMIT,
    job_limit: int = _LIVE_GC_JOB_LIMIT,
    object_delete_budget: int = _LIVE_GC_OBJECT_DELETE_BUDGET,
) -> MaterializationGenerationGcResult:
    """Boundedly remove unreferenced generated output from live TaskSets.

    Object listings and deletes occur only after the candidate SQL snapshot has
    been closed. Every deletion prefix is reconstructed from a TaskSet row, a
    materialization-job UUID, and a persisted epoch; object keys never choose a
    target prefix.
    """
    if task_set_limit <= 0 or job_limit <= 0 or object_delete_budget < 0:
        raise ValueError("live generation GC bounds must be positive (budget may be zero)")

    sweep_sequence = await _load_live_generation_gc_sweep(session)
    task_sets: dict[str, _LiveTaskSet] = {}
    job_rows: Sequence[Any] = ()
    source_rows: Sequence[Any] = ()
    image_sources: list[str] = []
    image_prefixes: dict[str, str] = {}

    try:
        live_task_set_filter = (
            TaskSet.soft_deleted_at.is_(None),
            TaskSet.status != "deleted",
        )
        task_set_count = (await session.execute(
            select(func.count()).select_from(TaskSet).where(*live_task_set_filter),
        )).scalar_one()
        if task_set_count:
            task_set_page_count = (task_set_count + task_set_limit - 1) // task_set_limit
            task_set_offset = (sweep_sequence % task_set_page_count) * task_set_limit
            task_set_rows = (await session.execute(
                select(TaskSet.id, TaskSet.owning_team_id, TaskSet.slug)
                .where(*live_task_set_filter)
                .order_by(TaskSet.created_at, TaskSet.id)
                .offset(task_set_offset)
                .limit(task_set_limit),
            )).all()
            task_sets = {
                row.id: _LiveTaskSet(
                    id=row.id,
                    owning_team_id=row.owning_team_id,
                    slug=row.slug,
                )
                for row in task_set_rows
            }

        if task_sets:
            live_job_filter = TaskSetMaterializationJob.task_set_id.in_(task_sets)
            job_count = (await session.execute(
                select(func.count())
                .select_from(TaskSetMaterializationJob)
                .where(live_job_filter),
            )).scalar_one()
            if job_count:
                job_window = sweep_sequence // task_set_page_count
                job_offset = (job_window * job_limit) % job_count
                job_rows = (await session.execute(
                    select(
                        TaskSetMaterializationJob.id,
                        TaskSetMaterializationJob.task_set_id,
                        TaskSetMaterializationJob.state,
                        TaskSetMaterializationJob.lease_epoch,
                    )
                    .where(live_job_filter)
                    .order_by(
                        TaskSetMaterializationJob.enqueued_at,
                        TaskSetMaterializationJob.id,
                    )
                    .offset(job_offset)
                    .limit(job_limit),
                )).all()
            source_rows = (await session.execute(
                select(Task.task_set_id, Task.source).where(Task.task_set_id.in_(task_sets)),
            )).all()
            image_prefixes = {
                task_set.id: f"s3://{artifacts_bucket}/" + generation_root(
                    team_id=task_set.owning_team_id, slug=task_set.slug,
                )
                for task_set in task_sets.values()
            }
            image_sources = await _protected_task_image_sources(
                session, source_prefixes=list(image_prefixes.values()),
            )
    finally:
        # Do not hold a DB transaction while traversing object storage.
        await session.rollback()

    sources_by_task_set: dict[str, list[str | None]] = defaultdict(list)
    for row in source_rows:
        sources_by_task_set[row.task_set_id].append(row.source)
    for task_set_id, prefix in image_prefixes.items():
        sources_by_task_set[task_set_id].extend(
            source for source in image_sources if source.startswith(prefix)
        )

    jobs = [
        _LiveJob(
            id=row.id,
            task_set_id=row.task_set_id,
            state=row.state,
            lease_epoch=row.lease_epoch,
        )
        for row in job_rows
        if row.task_set_id in task_sets
    ]
    candidates: dict[tuple[str, UUID, int], _GenerationCandidate] = {}
    errors = 0
    for job in jobs:
        task_set = task_sets[job.task_set_id]
        try:
            job_prefix = f"{generation_root(team_id=task_set.owning_team_id, slug=task_set.slug)}{job.id}/"
            epochs = await asyncio.to_thread(
                _listed_generation_epochs,
                minio_client,
                bucket=artifacts_bucket,
                job_prefix=job_prefix,
                max_epoch=job.lease_epoch,
            )
        except Exception:
            logger.warning(
                "taskset_gc: failed to list generation prefix for job %s", job.id,
                exc_info=True,
            )
            errors += 1
            continue
        for epoch in epochs:
            try:
                candidate = _GenerationCandidate(
                    task_set=task_set,
                    job=job,
                    epoch=epoch,
                    prefix=generation_prefix(
                        team_id=task_set.owning_team_id,
                        slug=task_set.slug,
                        job_id=job.id,
                        epoch=epoch,
                    ),
                    tasks_prefix=generated_tasks_prefix(
                        team_id=task_set.owning_team_id,
                        slug=task_set.slug,
                        job_id=job.id,
                        epoch=epoch,
                    ),
                )
            except ValueError:
                logger.warning(
                    "taskset_gc: skipped malformed DB generation for job %s", job.id,
                    exc_info=True,
                )
                errors += 1
                continue
            candidates[(task_set.id, job.id, epoch)] = candidate

    protected = 0
    deletable: list[_GenerationCandidate] = []
    for candidate in sorted(
        candidates.values(),
        key=lambda item: (item.task_set.id, str(item.job.id), item.epoch),
    ):
        active_epoch = (
            candidate.job.state in _ACTIVE_JOB_STATES
            and candidate.epoch == candidate.job.lease_epoch
        )
        referenced = any(
            _source_protects_candidate(
                source,
                artifacts_bucket=artifacts_bucket,
                candidate=candidate,
            )
            for source in sources_by_task_set[candidate.task_set.id]
        )
        if active_epoch or referenced:
            protected += 1
        else:
            deletable.append(candidate)

    attempted = 0
    deleted = 0
    partial = False
    remaining_budget = object_delete_budget
    for candidate in deletable:
        if remaining_budget <= 0:
            partial = True
            break
        rechecked_protected, recheck_failed = await _candidate_is_protected_after_recheck(
            session,
            candidate=candidate,
            artifacts_bucket=artifacts_bucket,
        )
        if rechecked_protected:
            protected += 1
            errors += int(recheck_failed)
            continue
        delete_result = await asyncio.to_thread(
            _delete_s3_prefix,
            minio_client,
            bucket=artifacts_bucket,
            prefix=candidate.prefix,
            max_objects=remaining_budget,
        )
        attempted += delete_result.attempted_objects
        deleted += delete_result.deleted_objects
        errors += delete_result.error_objects
        remaining_budget -= delete_result.attempted_objects
        partial = partial or delete_result.partial

    result = MaterializationGenerationGcResult(
        candidate_generations=len(candidates),
        protected_generations=protected,
        attempted_objects=attempted,
        deleted_objects=deleted,
        error_objects=errors,
        partial=partial,
    )
    await _advance_live_generation_gc_sweep(
        session,
        completed_sweep=sweep_sequence,
    )
    return result


def _taskset_has_work_or_history(*, active_only: bool) -> Any:
    """Correlated reference predicate, including batches that have not fanned out.

    A legacy batch without a resolved selection retains its team's sets: we
    cannot prove which inputs it used from an arbitrary historical filter.
    """
    task_ids = select(Task.id).where(Task.task_set_id == TaskSet.id).correlate(TaskSet)
    trial_ref = select(Trial.id).where(Trial.task_id.in_(task_ids))
    batch_ref = select(Batch.id).where(
        Batch.team_id == TaskSet.owning_team_id,
        or_(
            Batch.resolved_task_ids.is_(None),
            Batch.resolved_task_ids == JSONB.NULL,
            Batch.task_filter["task_set_id"].astext == TaskSet.id,
            Batch.task_filter["task_set_ids"].contains(func.jsonb_build_array(TaskSet.id)),
            exists(select(Task.id).where(
                Task.task_set_id == TaskSet.id,
                or_(
                    Batch.resolved_task_ids.contains(func.jsonb_build_array(Task.id)),
                    Batch.task_filter["task_ids"].contains(func.jsonb_build_array(Task.id)),
                ),
            ).correlate(TaskSet, Batch)),
        ),
    )
    if active_only:
        trial_ref = trial_ref.where(Trial.state.not_in(("succeeded", "failed", "cancelled")))
        batch_ref = batch_ref.where(Batch.state.not_in(("finished", "cancelled")))
    return or_(exists(trial_ref.correlate(TaskSet)), exists(batch_ref.correlate(TaskSet)))


def _taskset_has_preparation() -> Any:
    return or_(
        exists(select(TaskSetMaterializationJob.id).where(
            TaskSetMaterializationJob.task_set_id == TaskSet.id,
            TaskSetMaterializationJob.state.in_(("queued", "claimed", "running")),
        ).correlate(TaskSet)),
        exists(select(TaskImageMaterialization.id).where(
            TaskImageMaterialization.task_id.in_(
                select(Task.id).where(Task.task_set_id == TaskSet.id).correlate(TaskSet),
            ),
            TaskImageMaterialization.state.in_(("queued", "claimed", "running")),
        ).correlate(TaskSet)),
    )


async def retire_expired_task_sets(session: AsyncSession) -> int:
    """Retire explicit, unheld expirations without deleting historical inputs.

    Admission and lifecycle edits lock the same TaskSet rows. Recheck active
    work in a new statement after the lock so a just-committed submission wins.
    """
    now = datetime.now(UTC)
    eligible = (
        TaskSet.soft_deleted_at.is_(None), TaskSet.expires_at <= now,
        TaskSet.hold.is_(False), ~_taskset_has_preparation(),
        ~_taskset_has_work_or_history(active_only=True),
    )
    retired = 0
    rows = (await session.scalars(
        select(TaskSet).where(*eligible).order_by(TaskSet.expires_at, TaskSet.id)
        .limit(100).with_for_update(skip_locked=True),
    )).all()
    for row in rows:
        still_eligible = await session.scalar(
            select(TaskSet.id).where(TaskSet.id == row.id, *eligible),
        )
        if still_eligible is None:
            continue
        row.status = "deleted"
        row.status_reason = "expired"
        row.soft_deleted_at = now
        row.updated_at = now
        retired += 1
    await session.commit()
    return retired


def _taskset_has_stored_input_refs(artifacts_bucket: str) -> Any:
    prefix = func.concat(
        "s3://", artifacts_bucket, "/tasksets/user/", TaskSet.owning_team_id,
        "/", TaskSet.slug, "/",
    )
    return or_(
        exists(select(TaskImageMaterialization.id).where(
            func.starts_with(TaskImageMaterialization.task_source, prefix),
        ).correlate(TaskSet)),
        # Journal-managed bundles have their own version-aware lifecycle.
        exists(select(TaskBundleSource.id).where(
            func.starts_with(TaskBundleSource.source_uri, prefix),
        ).correlate(TaskSet)),
    )


async def purge_expired_task_sets(
    session: AsyncSession,
    *,
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
    object_delete_budget: int = _ROOT_GC_OBJECT_DELETE_BUDGET,
) -> int:
    """Purge retention-expired TaskSet roots. Returns hard-deleted row count."""
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    try:
        expired_rows = (await session.execute(
            select(TaskSet.id, TaskSet.owning_team_id, TaskSet.slug)
            .where(
                TaskSet.soft_deleted_at.is_not(None),
                TaskSet.soft_deleted_at < cutoff,
                TaskSet.hold.is_(False),
                ~_taskset_has_preparation(),
                ~_taskset_has_work_or_history(active_only=False),
                ~_taskset_has_stored_input_refs(artifacts_bucket),
            )
            .order_by(TaskSet.soft_deleted_at, TaskSet.id)
            .limit(100),
        )).all()
    finally:
        await session.rollback()

    purged = 0
    for row in expired_rows:
        try:
            # Only retired rows are eligible. Supported mutations/admission
            # cannot restore or attach new work to them; recheck before S3 I/O.
            current = await session.scalar(select(TaskSet).where(
                TaskSet.id == row.id, TaskSet.soft_deleted_at < cutoff,
                TaskSet.hold.is_(False), ~_taskset_has_preparation(),
                ~_taskset_has_work_or_history(active_only=False),
                ~_taskset_has_stored_input_refs(artifacts_bucket),
            ))
            if current is None:
                await session.rollback()
                continue
            prefix = _storage_prefix(team_id=row.owning_team_id, slug=row.slug)
            manifest_row = (await session.execute(
                select(TaskSetManifest).where(TaskSetManifest.task_set_id == row.id),
            )).scalar_one_or_none()
            verifier_blob_uri = (
                manifest_row.verifier_blob_uri if manifest_row is not None else None
            )
            transform_blob_uri = (
                manifest_row.transform_blob_uri if manifest_row is not None else None
            )
            await session.rollback()

            delete_result = await asyncio.to_thread(
                _delete_s3_prefix,
                minio_client,
                bucket=artifacts_bucket,
                prefix=prefix,
                max_objects=object_delete_budget,
            )
            if delete_result.partial or delete_result.error_objects:
                logger.warning(
                    "taskset_gc: retained expired root %s after partial/error delete "
                    "attempted=%d deleted=%d errors=%d",
                    row.id,
                    delete_result.attempted_objects,
                    delete_result.deleted_objects,
                    delete_result.error_objects,
                )
                continue
            await asyncio.to_thread(_delete_single_blob, minio_client, verifier_blob_uri)
            await asyncio.to_thread(_delete_single_blob, minio_client, transform_blob_uri)

            current = await session.get(TaskSet, row.id)
            if (
                current is None
                or current.soft_deleted_at is None
                or current.soft_deleted_at >= cutoff
                or current.hold
            ):
                await session.rollback()
                continue
            await session.execute(delete(Task).where(Task.task_set_id == row.id))
            await session.execute(
                delete(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == row.id,
                ),
            )
            await session.execute(delete(TaskSetManifest).where(TaskSetManifest.task_set_id == row.id))
            await session.execute(delete(TaskSet).where(TaskSet.id == row.id))
            await session.commit()
            purged += 1
        except Exception:
            logger.exception("gc: failed to purge task_set %s", row.id)
            await session.rollback()

    return purged


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
) -> tuple[int, MaterializationGenerationGcResult]:
    """Retire declared expirations, then run the two existing cleanup contracts."""
    async with session_factory() as session:
        await retire_expired_task_sets(session)
    async with session_factory() as session:
        purged = await purge_expired_task_sets(
            session,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            retention_days=retention_days,
        )
    async with session_factory() as session:
        generation_result = await purge_abandoned_materialization_generations(
            session,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
        )
    return purged, generation_result


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
    poll_interval_sec: int,
) -> None:
    while True:
        try:
            purged, generation_result = await run_once(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                retention_days=retention_days,
            )
            if purged > 0:
                logger.info("taskset_gc purged %d expired task sets", purged)
            if (
                generation_result.deleted_objects
                or generation_result.error_objects
                or generation_result.partial
            ):
                logger.info(
                    "taskset_generation_gc candidates=%d protected=%d attempted=%d "
                    "deleted=%d errors=%d partial=%s",
                    generation_result.candidate_generations,
                    generation_result.protected_generations,
                    generation_result.attempted_objects,
                    generation_result.deleted_objects,
                    generation_result.error_objects,
                    generation_result.partial,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("taskset_gc iteration failed")
        await asyncio.sleep(poll_interval_sec)
