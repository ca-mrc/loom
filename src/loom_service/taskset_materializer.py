"""Background consumer for ``task_set_materialization_jobs`` (#242 sub-plan 3)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Task, TaskSet, TaskSetManifest, TaskSetMaterializationJob, TeamQuota
from loom.models.taskset import UserTaskSetManifest
from loom.task_image_materialization import ensure_task_image_materializations
from loom.taskset.materialize import MaterializeOutput, materialize_task_set
from loom.taskset.storage_bytes import team_taskset_storage_bytes
from loom.taskset.transform_sandbox import TransformSandboxConfig

logger = logging.getLogger(__name__)

# A random process-local token avoids storing or exposing host, process, or user
# information in the durable lease owner column.
_CLAIMED_BY = f"taskset-materializer-{uuid4()}"
_RETRY_BACKOFF_SEC = (30, 120, 300)


@dataclass(frozen=True)
class MaterializationLease:
    """Immutable fencing token for one materialization-job owner."""

    job_id: UUID
    lease_epoch: int
    claimed_by: str

    @property
    def id(self) -> UUID:
        """Compatibility alias for callers that only need the job identifier."""
        return self.job_id


class LeaseLost(RuntimeError):  # noqa: N818 - task contract names this result LeaseLost
    """Raised when a materializer no longer owns its durable lease."""

    def __init__(self) -> None:
        super().__init__("TaskSet materialization lease lost")


def _retry_delay_sec(attempt_count: int) -> int:
    idx = min(max(attempt_count - 1, 0), len(_RETRY_BACKOFF_SEC) - 1)
    return _RETRY_BACKOFF_SEC[idx]


def _lease_for_job(job: TaskSetMaterializationJob) -> MaterializationLease:
    if job.claimed_by is None:
        raise LeaseLost()
    return MaterializationLease(
        job_id=job.id,
        lease_epoch=job.lease_epoch,
        claimed_by=job.claimed_by,
    )


def _lease_conditions(lease: MaterializationLease) -> tuple[Any, ...]:
    return (
        TaskSetMaterializationJob.id == lease.job_id,
        TaskSetMaterializationJob.lease_epoch == lease.lease_epoch,
        TaskSetMaterializationJob.claimed_by == lease.claimed_by,
    )


async def _update_job_for_lease(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
    states: tuple[str, ...],
    values: Mapping[str, Any],
) -> None:
    """Apply one job transition only while ``lease`` remains the owner."""
    result = await session.execute(
        update(TaskSetMaterializationJob)
        .where(
            *_lease_conditions(lease),
            TaskSetMaterializationJob.state.in_(states),
        )
        .values(**dict(values))
        .returning(TaskSetMaterializationJob.id),
    )
    if len(result.scalars().all()) != 1:
        await session.rollback()
        raise LeaseLost()


async def reclaim_stale_jobs(
    session: AsyncSession,
    *,
    claim_ttl_sec: int,
) -> int:
    """Revoke stale active leases and return their jobs to the queue."""
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=claim_ttl_sec)
    result = await session.execute(
        update(TaskSetMaterializationJob)
        .where(
            TaskSetMaterializationJob.state.in_(("claimed", "running")),
            TaskSetMaterializationJob.lease_heartbeat_at.is_not(None),
            TaskSetMaterializationJob.lease_heartbeat_at < cutoff,
        )
        .values(
            state="queued",
            claimed_at=None,
            claimed_by=None,
            lease_epoch=TaskSetMaterializationJob.lease_epoch + 1,
            lease_heartbeat_at=None,
            started_at=None,
            next_attempt_at=now + timedelta(seconds=30),
            updated_at=now,
        )
        .returning(TaskSetMaterializationJob.id),
    )
    reclaimed = len(result.all())
    if reclaimed:
        await session.commit()
    return reclaimed


async def relinquish_if_current(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
) -> None:
    """Cooperatively revoke one current lease without touching staged output.

    This is an internal materializer transition, not an API or user-command
    control.  The increment makes any delayed owner fail the same CAS checks
    used after a stale-heartbeat reclaim, while the cleared ``next_attempt_at``
    lets an authorized deployment canary claim the queued job immediately.
    """
    now = datetime.now(UTC)
    await _update_job_for_lease(
        session,
        lease=lease,
        states=("claimed", "running"),
        values={
            "state": "queued",
            "claimed_at": None,
            "claimed_by": None,
            "lease_epoch": TaskSetMaterializationJob.lease_epoch + 1,
            "lease_heartbeat_at": None,
            "started_at": None,
            "next_attempt_at": None,
            "updated_at": now,
        },
    )
    await session.commit()


async def _claim_jobs(
    session: AsyncSession,
    *,
    batch_size: int,
    claimed_by: str,
    job_id: UUID | None = None,
) -> list[MaterializationLease]:
    """Claim available jobs and return immutable owner fencing tokens."""
    now = datetime.now(UTC)
    query = (
        select(TaskSetMaterializationJob.id)
        .where(
            TaskSetMaterializationJob.state == "queued",
            (TaskSetMaterializationJob.next_attempt_at.is_(None))
            | (TaskSetMaterializationJob.next_attempt_at <= now),
        )
        .order_by(TaskSetMaterializationJob.enqueued_at)
        .with_for_update(skip_locked=True)
        .limit(batch_size)
    )
    if job_id is not None:
        query = query.where(TaskSetMaterializationJob.id == job_id)
    job_ids = (await session.execute(query)).scalars().all()
    leases: list[MaterializationLease] = []
    for job_id in job_ids:
        claimed = (await session.execute(
            update(TaskSetMaterializationJob)
            .where(
                TaskSetMaterializationJob.id == job_id,
                TaskSetMaterializationJob.state == "queued",
            )
            .values(
                state="claimed",
                claimed_at=now,
                claimed_by=claimed_by,
                lease_epoch=TaskSetMaterializationJob.lease_epoch + 1,
                lease_heartbeat_at=now,
                attempt_count=TaskSetMaterializationJob.attempt_count + 1,
                updated_at=now,
            )
            .returning(
                TaskSetMaterializationJob.id,
                TaskSetMaterializationJob.lease_epoch,
            ),
        )).one_or_none()
        if claimed is not None:
            leases.append(
                MaterializationLease(
                    job_id=claimed.id,
                    lease_epoch=claimed.lease_epoch,
                    claimed_by=claimed_by,
                ),
            )
    if leases:
        await session.commit()
    return leases


async def _heartbeat_lease(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
) -> None:
    now = datetime.now(UTC)
    await _update_job_for_lease(
        session,
        lease=lease,
        states=("claimed", "running"),
        values={
            "claimed_at": now,
            "lease_heartbeat_at": now,
            "updated_at": now,
        },
    )
    await session.commit()


async def _start_job(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
) -> None:
    now = datetime.now(UTC)
    await _update_job_for_lease(
        session,
        lease=lease,
        states=("claimed",),
        values={
            "state": "running",
            "started_at": now,
            "claimed_at": now,
            "lease_heartbeat_at": now,
            "updated_at": now,
        },
    )
    await session.commit()


async def publish_if_current(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
    task_set_id: str,
    output: MaterializeOutput,
    claim_ttl_sec: int,
) -> None:
    """Atomically expose staged output only from a fresh current lease."""
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=claim_ttl_sec)
    job = (await session.execute(
        select(TaskSetMaterializationJob)
        .where(
            *_lease_conditions(lease),
            TaskSetMaterializationJob.state == "running",
            TaskSetMaterializationJob.lease_heartbeat_at.is_not(None),
            TaskSetMaterializationJob.lease_heartbeat_at >= cutoff,
        )
        .with_for_update(),
    )).scalar_one_or_none()
    if job is None or job.task_set_id != task_set_id:
        await session.rollback()
        raise LeaseLost()

    # Preserve Task 2's job-then-TaskSet lock order.
    task_set = (await session.execute(
        select(TaskSet)
        .where(
            TaskSet.id == task_set_id,
            TaskSet.soft_deleted_at.is_(None),
        )
        .with_for_update(),
    )).scalar_one_or_none()
    if task_set is None:
        await session.rollback()
        raise LeaseLost()

    attempt_count = job.attempt_count
    max_attempts = job.max_attempts
    if output.retry_source and attempt_count < max_attempts:
        await _update_job_for_lease(
            session,
            lease=lease,
            states=("running",),
            values={
                "state": "queued",
                "claimed_at": None,
                "claimed_by": None,
                "lease_heartbeat_at": None,
                "started_at": None,
                "next_attempt_at": now + timedelta(seconds=_retry_delay_sec(attempt_count)),
                "failure_reason": output.job_failure_reason,
                "failure_message": output.job_failure_message,
                "updated_at": now,
            },
        )
        result = await session.execute(
            update(TaskSet)
            .where(
                TaskSet.id == task_set_id,
                TaskSet.soft_deleted_at.is_(None),
            )
            .values(
                status="materializing",
                status_reason=output.status_reason,
                updated_at=now,
            )
            .returning(TaskSet.id),
        )
        if len(result.scalars().all()) != 1:
            await session.rollback()
            raise LeaseLost()
        await session.commit()
        return

    if output.retry_source:
        output = MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=output.job_failure_message,
            error_summary=output.error_summary,
        )

    has_publishable_rows = bool(output.task_rows)
    state = "succeeded" if has_publishable_rows else "failed"
    if output.job_failure_reason and not has_publishable_rows:
        state = "failed"

    # This transaction is the publication point: staged objects are not
    # reachable until the Task rows below commit.
    job_values: dict[str, Any] = {
        "state": state,
        "finished_at": now,
        "updated_at": now,
        "failure_reason": output.job_failure_reason,
        "failure_message": output.job_failure_message,
        "error_summary": output.error_summary,
    }
    if has_publishable_rows:
        job_values["published_materialization_generation"] = lease.lease_epoch
    await _update_job_for_lease(
        session,
        lease=lease,
        states=("running",),
        values=job_values,
    )
    if has_publishable_rows:
        # A rebuild replaces catalog rows and relocates legacy source objects.
        # Keep the repaired default only for the exact same task revision; it
        # must not leak into changed configs/checksums or manifest-backed input.
        previous_legacy = {
            row.id: row
            for row in (await session.execute(
                select(
                    Task.id, Task.checksum, Task.config, Task.source_provenance,
                    Task.legacy_separate_verifier_checksum,
                )
                .where(
                    Task.task_set_id == task_set_id,
                    Task.legacy_separate_verifier_checksum.is_not(None),
                )
                .order_by(Task.id)
                .with_for_update(),
            )).all()
        }
        legacy_defaults: dict[str, str] = {}
        for draft in output.task_rows:
            previous = previous_legacy.get(draft.id)
            if (
                previous is not None
                and previous.legacy_separate_verifier_checksum
                == previous.checksum.removeprefix("sha256:")
                == draft.checksum.removeprefix("sha256:")
                and previous.config == draft.config
                and "bundle_content_manifest_sha256" not in (previous.source_provenance or {})
                and "bundle_content_manifest_sha256" not in draft.source_provenance
            ):
                legacy_defaults[draft.id] = previous.legacy_separate_verifier_checksum
        await session.execute(delete(Task).where(Task.task_set_id == task_set_id))
        published_tasks = [
            Task(
                id=row.id,
                checksum=row.checksum,
                legacy_separate_verifier_checksum=legacy_defaults.get(row.id),
                config=row.config,
                source=row.source,
                source_provenance=row.source_provenance,
                tags=row.tags,
                task_set_id=task_set_id,
                benchmark_id=None,
            )
            for row in sorted(output.task_rows, key=lambda row: row.id)
        ]
        session.add_all(published_tasks)
        await session.flush()
        for task_row in published_tasks:
            if task_row.config:
                await ensure_task_image_materializations(session, task_row=task_row)
    else:
        has_previous_publication = (await session.execute(
            select(Task.id)
            .where(Task.task_set_id == task_set_id)
            .limit(1),
        )).scalar_one_or_none() is not None
        if has_previous_publication:
            await session.commit()
            return
    result = await session.execute(
        update(TaskSet)
        .where(
            TaskSet.id == task_set_id,
            TaskSet.soft_deleted_at.is_(None),
        )
        .values(
            status=output.status,
            status_reason=output.status_reason,
            task_count=output.task_count,
            evaluation_ready=output.evaluation_ready,
            updated_at=now,
        )
        .returning(TaskSet.id),
    )
    if len(result.scalars().all()) != 1:
        await session.rollback()
        raise LeaseLost()
    await session.commit()


async def _fail_lease(
    session: AsyncSession,
    *,
    lease: MaterializationLease,
    failure_reason: str,
    failure_message: str,
    claim_ttl_sec: int,
) -> None:
    """Mark an owned job failed without allowing a stale fallback write."""
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=claim_ttl_sec)
    task_set_id = (await session.execute(
        select(TaskSetMaterializationJob.task_set_id)
        .where(
            *_lease_conditions(lease),
            TaskSetMaterializationJob.state.in_(("claimed", "running")),
            TaskSetMaterializationJob.lease_heartbeat_at.is_not(None),
            TaskSetMaterializationJob.lease_heartbeat_at >= cutoff,
        )
        .with_for_update(),
    )).scalar_one_or_none()
    if task_set_id is None:
        await session.rollback()
        raise LeaseLost()
    await _update_job_for_lease(
        session,
        lease=lease,
        states=("claimed", "running"),
        values={
            "state": "failed",
            "failure_reason": failure_reason,
            "failure_message": failure_message,
            "finished_at": now,
            "updated_at": now,
        },
    )
    result = await session.execute(
        update(TaskSet)
        .where(TaskSet.id == task_set_id)
        .values(
            status="failed",
            status_reason=failure_reason,
            updated_at=now,
        )
        .returning(TaskSet.id),
    )
    if len(result.scalars().all()) != 1:
        await session.rollback()
        raise LeaseLost()
    await session.commit()


async def _heartbeat_loop(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease: MaterializationLease,
    claim_ttl_sec: int,
) -> None:
    interval_sec = min(max(claim_ttl_sec / 3, 0.1), 30.0)
    while True:
        await asyncio.sleep(interval_sec)
        async with session_factory() as session:
            await _heartbeat_lease(session, lease=lease)


async def _stop_heartbeat(heartbeat_task: asyncio.Task[None]) -> None:
    if heartbeat_task.done():
        await heartbeat_task
        return
    heartbeat_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await heartbeat_task


async def stage_claimed_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None = None,
    max_team_storage_bytes: int | None = None,
    claim_ttl_sec: int = 300,
    lease: MaterializationLease | None = None,
) -> tuple[MaterializationLease, str, MaterializeOutput] | None:
    """Stage a current lease with the worker's ordinary materialization path.

    Normal workers publish the returned output immediately.  The only other
    caller is the deployment-only fence canary, which voluntarily relinquishes
    its lease before any publication attempt; this function itself has no
    pause, failure, deletion, or externally exposed control.
    """
    if lease is None:
        async with session_factory() as session:
            job = await session.get(TaskSetMaterializationJob, job_id)
            if job is None or job.state != "claimed":
                return None
            lease = _lease_for_job(job)

    async with session_factory() as session:
        await _start_job(session, lease=lease)
        task_set_id = (await session.execute(
            select(TaskSetMaterializationJob.task_set_id).where(
                *_lease_conditions(lease),
                TaskSetMaterializationJob.state == "running",
            ),
        )).scalar_one_or_none()
        if task_set_id is None:
            await session.rollback()
            raise LeaseLost()
        task_set = await session.get(TaskSet, task_set_id)
        manifest_row = await session.get(TaskSetManifest, task_set_id)
        if task_set is None or manifest_row is None:
            await _fail_lease(
                session,
                lease=lease,
                failure_reason="missing_task_set",
                failure_message="task set or manifest row missing",
                claim_ttl_sec=claim_ttl_sec,
            )
            return None
        manifest = UserTaskSetManifest.model_validate(manifest_row.manifest)
        quota_row = (await session.execute(
            select(TeamQuota).where(TeamQuota.team_id == task_set.owning_team_id),
        )).scalar_one_or_none()
        effective_max_team_storage = (
            quota_row.taskset_max_storage_bytes
            if quota_row is not None and quota_row.taskset_max_storage_bytes is not None
            else max_team_storage_bytes
        )

        owning_team_id = str(task_set.owning_team_id)
        intents = list(task_set.intents)
        verifier_blob_uri = manifest_row.verifier_blob_uri
        transform_blob_uri = manifest_row.transform_blob_uri

    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            session_factory,
            lease=lease,
            claim_ttl_sec=claim_ttl_sec,
        ),
    )
    try:
        # Existing generated output remains referenced until publication. Count
        # the full team total so staging reserves the required rebuild headroom
        # without treating durable TaskSet inputs as replaceable bytes.
        team_storage_baseline = await asyncio.to_thread(
            team_taskset_storage_bytes,
            minio_client,
            bucket=artifacts_bucket,
            team_id=owning_team_id,
        )
        output = await asyncio.to_thread(
            materialize_task_set,
            manifest=manifest,
            task_set_id=task_set_id,
            owning_team_id=owning_team_id,
            materialization_job_id=lease.job_id,
            materialization_epoch=lease.lease_epoch,
            intents=intents,
            verifier_blob_uri=verifier_blob_uri,
            transform_blob_uri=transform_blob_uri,
            transform_config=transform_config,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            upstream_cache_root=upstream_cache_root,
            max_bundle_bytes=max_bundle_bytes,
            team_storage_baseline=team_storage_baseline,
            max_team_storage_bytes=effective_max_team_storage,
        )
    except BaseException:
        await _stop_heartbeat(heartbeat_task)
        raise
    await _stop_heartbeat(heartbeat_task)

    return lease, task_set_id, output


async def _materialize_claimed_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None = None,
    max_team_storage_bytes: int | None = None,
    claim_ttl_sec: int = 300,
    lease: MaterializationLease | None = None,
) -> None:
    """Stage then publish a claimed job for the ordinary worker loop."""
    staged = await stage_claimed_job(
        session_factory,
        job_id=job_id,
        minio_client=minio_client,
        artifacts_bucket=artifacts_bucket,
        upstream_cache_root=upstream_cache_root,
        transform_config=transform_config,
        max_bundle_bytes=max_bundle_bytes,
        max_team_storage_bytes=max_team_storage_bytes,
        claim_ttl_sec=claim_ttl_sec,
        lease=lease,
    )
    if staged is None:
        return
    current_lease, task_set_id, output = staged
    async with session_factory() as session:
        await publish_if_current(
            session,
            lease=current_lease,
            task_set_id=task_set_id,
            output=output,
            claim_ttl_sec=claim_ttl_sec,
        )


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    batch_size: int,
    claim_ttl_sec: int,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None = None,
    max_team_storage_bytes: int | None = None,
) -> None:
    async with session_factory() as session:
        await reclaim_stale_jobs(session, claim_ttl_sec=claim_ttl_sec)

    async with session_factory() as session:
        leases = await _claim_jobs(
            session,
            batch_size=batch_size,
            claimed_by=_CLAIMED_BY,
        )

    for lease in leases:
        try:
            await _materialize_claimed_job(
                session_factory,
                job_id=lease.job_id,
                lease=lease,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                upstream_cache_root=upstream_cache_root,
                transform_config=transform_config,
                max_bundle_bytes=max_bundle_bytes,
                max_team_storage_bytes=max_team_storage_bytes,
                claim_ttl_sec=claim_ttl_sec,
            )
        except LeaseLost:
            logger.info("taskset_materializer lease lost job_id=%s", lease.job_id)
        except Exception:
            logger.exception("taskset_materializer failed job_id=%s", lease.job_id)
            async with session_factory() as session:
                try:
                    await _fail_lease(
                        session,
                        lease=lease,
                        failure_reason="internal_error",
                        failure_message="materialization worker crashed",
                        claim_ttl_sec=claim_ttl_sec,
                    )
                except LeaseLost:
                    logger.info("taskset_materializer lease lost job_id=%s", lease.job_id)


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    batch_size: int,
    poll_interval_sec: int,
    claim_ttl_sec: int,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None = None,
    max_team_storage_bytes: int | None = None,
) -> None:
    upstream_cache_root.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            await run_once(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                upstream_cache_root=upstream_cache_root,
                batch_size=batch_size,
                claim_ttl_sec=claim_ttl_sec,
                transform_config=transform_config,
                max_bundle_bytes=max_bundle_bytes,
                max_team_storage_bytes=max_team_storage_bytes,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("taskset_materializer iteration failed")
        await asyncio.sleep(poll_interval_sec)
