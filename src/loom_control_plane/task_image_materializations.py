"""Lease-fenced control-plane operations for task-image builders."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import and_, exists, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import (
    Batch,
    ServiceExecutionLease,
    TaskImageAttemptRetention,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig
from loom.task_image_materialization import (
    _assert_no_pending_task_image_writes,
    admit_task_image_source,
    current_task_image_reference,
    release_task_image_source,
    required_task_image_components,
    validate_task_image_registry_images,
)

DEFAULT_TASK_IMAGE_LEASE_SECONDS = 300.0
_MAX_RETRY_BACKOFF_SECONDS = 600.0
_TERMINAL_TRIAL_STATES = ("succeeded", "failed", "cancelled")


class TaskImageLeaseConflictError(Exception):
    """The caller no longer owns the materialization lease."""


class TaskImageCompletionError(ValueError):
    """Published component evidence does not match the immutable task snapshot."""


class TaskImageRetryConflictError(RuntimeError):
    """The requested materialization cannot be safely requeued."""


def _expected_registry_image_components(task_config: dict[str, Any]) -> set[str]:
    task = TaskConfig.model_validate(task_config)
    return required_task_image_components(task)


def _record_registry_image_history(
    row: TaskImageMaterialization,
    *,
    builder_id: str,
    attempt_count: int,
    lease_epoch: int,
    registry_images: dict[str, str],
    now: datetime,
) -> None:
    history = list(row.registry_image_history or [])
    observed = {
        (
            entry.get("attempt_count"),
            entry.get("lease_epoch"),
            str(entry.get("builder_id")),
            str(entry.get("component")),
            str(entry.get("registry_image")),
        )
        for entry in history
        if isinstance(entry, dict)
    }
    for component, registry_image in registry_images.items():
        evidence_key = (
            attempt_count,
            lease_epoch,
            builder_id,
            component,
            registry_image,
        )
        if evidence_key in observed:
            continue
        history.append(
            {
                "component": component,
                "registry_image": registry_image,
                "builder_id": builder_id,
                "attempt_count": attempt_count,
                "lease_epoch": lease_epoch,
                "recorded_at": now.isoformat(),
            }
        )
    row.registry_image_history = history


async def _record_attempt_publication_evidence(
    session: AsyncSession,
    row: TaskImageMaterialization,
    *,
    builder_id: str,
    attempt_count: int,
    lease_epoch: int,
    registry_images: dict[str, str],
    now: datetime,
) -> None:
    attempt = await session.scalar(
        select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.materialization_id == row.id,
            TaskImageMaterializationAttempt.attempt_number == attempt_count,
            TaskImageMaterializationAttempt.lease_epoch == lease_epoch,
            TaskImageMaterializationAttempt.builder_id == builder_id,
        )
    )
    if attempt is None:
        raise TaskImageLeaseConflictError("task image publication attempt does not exist")
    if attempt.grant_id is not None:
        raise TaskImageCompletionError("rootless attempts cannot publish legacy cleanup evidence")
    for component, registry_image in registry_images.items():
        await session.execute(
            pg_insert(TaskImagePublicationEvidence)
            .values(
                materialization_attempt_id=attempt.id,
                materialization_id=row.id,
                attempt_number=attempt.attempt_number,
                lease_epoch=attempt.lease_epoch,
                builder_id=attempt.builder_id,
                component=component,
                registry_image=registry_image,
                recorded_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=(
                    TaskImagePublicationEvidence.materialization_attempt_id,
                    TaskImagePublicationEvidence.component,
                    TaskImagePublicationEvidence.registry_image,
                )
            )
        )
    _record_registry_image_history(
        row,
        builder_id=builder_id,
        attempt_count=attempt_count,
        lease_epoch=lease_epoch,
        registry_images=registry_images,
        now=now,
    )


async def record_task_image_publication(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    attempt_count: int | None,
    lease_epoch: int,
    component: str,
    registry_image: str,
) -> TaskImageMaterialization:
    """Append cleanup evidence without granting stale builders readiness authority."""
    from loom.task_bundle_source_journal import require_task_bundle_transaction

    _assert_no_pending_task_image_writes(session)
    await require_task_bundle_transaction(session)
    now = datetime.now(UTC)
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if row is None:
        raise TaskImageLeaseConflictError("task image materialization does not exist")
    if attempt_count is None:
        if (
            row.state not in {"claimed", "running"}
            or row.claimed_by != builder_id
            or row.lease_epoch != lease_epoch
            or row.lease_expires_at is None
            or row.lease_expires_at <= now
        ):
            raise TaskImageLeaseConflictError(
                "legacy task image publication requires the current live lease"
            )
        attempt_count = row.attempt_count
    try:
        images = validate_task_image_registry_images(
            {component: registry_image},
            expected_components=_expected_registry_image_components(row.task_config),
        )
    except ValueError as exc:
        raise TaskImageCompletionError(str(exc)) from exc
    previous_history = list(row.registry_image_history or [])
    await _record_attempt_publication_evidence(
        session,
        row,
        builder_id=builder_id,
        attempt_count=attempt_count,
        lease_epoch=lease_epoch,
        registry_images=images,
        now=now,
    )
    if row.registry_image_history != previous_history:
        if row.state == "retiring":
            # A collector only deleted the inventory returned by its claim.
            # New evidence invalidates that acknowledgement and makes the full
            # retained history immediately reclaimable under a newer epoch.
            row.lease_epoch += 1
            row.lease_expires_at = now
        elif row.state == "retired":
            # Late physical cleanup debt never restores build/source authority.
            row.unreferenced_at = now
    if (
        row.state in {"claimed", "running"}
        and row.claimed_by == builder_id
        and row.lease_epoch == lease_epoch
        and row.lease_expires_at is not None
        and row.lease_expires_at > now
    ):
        row.registry_images = {**row.registry_images, **images}
    row.updated_at = now
    await session.flush()
    return row


def _durable_reference_exists(row: Any) -> ColumnElement[bool]:
    current_task = exists().where(current_task_image_reference(row))
    live_trial = exists().where(
        TrialTaskImageMaterialization.materialization_id == row.id,
        Trial.id == TrialTaskImageMaterialization.trial_id,
        Trial.state.not_in(_TERMINAL_TRIAL_STATES),
    )
    return or_(current_task, live_trial)


def _registry_publication_exists(row: Any) -> ColumnElement[bool]:
    return or_(row.registry_images != {}, row.registry_image_history != [])


def _lease_deadline(*, now: datetime, lease_seconds: float) -> datetime:
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    return now + timedelta(seconds=lease_seconds)


def _nebius_demand_exists(row: Any, *, pool_id: str) -> ColumnElement[bool]:
    # Mirror the durable service scheduler's consumer path. A build remains
    # useful during scheduling backoff or transient target unavailability.
    # Use the frozen task binding, never a later revision of the catalog Task.
    binding = row.task_config["service_execution"]
    return and_(
        row.cpu_arch == "x86_64",
        exists(
            select(1)
            .select_from(TrialTaskImageMaterialization)
            .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
            .join(Batch, Batch.id == Trial.batch_id)
            .where(
                TrialTaskImageMaterialization.materialization_id == row.id,
                Trial.task_id == row.task_id,
                Trial.team_id == Batch.team_id,
                Trial.state.not_in(_TERMINAL_TRIAL_STATES),
                Trial.cancellation_requested_at.is_(None),
                Trial.family_key.is_(None),
                Batch.backend == "nebius",
                Batch.state.not_in(("finished", "cancelled")),
                Trial.requires_caps["worker_pool"].astext == pool_id,
                or_(
                    Trial.execution_route_pool_name.is_(None),
                    and_(
                        Trial.execution_route_pool_name == pool_id,
                        Trial.execution_route_json["selected_adapter_kind"].astext == "kubernetes_job",
                    ),
                ),
                or_(
                    binding["logical_pool_id"].astext == pool_id,
                    and_(
                        binding.astext.is_(None),
                        Batch.service_execution_runtime_profile["logical_pool_id"].astext == pool_id,
                    ),
                ),
            )
        ),
    )


async def has_nebius_task_image_demand(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    pool_id: str,
) -> bool:
    """Whether any live native consumer still needs this shared build.

    This is a demand observation, not lease ownership. Callers retain the
    existing builder/epoch fencing when heartbeating or stopping a build.
    """
    return bool(await session.scalar(
        select(_nebius_demand_exists(TaskImageMaterialization, pool_id=pool_id))
        .where(TaskImageMaterialization.id == materialization_id)
    ))


async def claim_task_image_materialization(
    session: AsyncSession,
    *,
    builder_id: str,
    cpu_arch: str,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
    nebius_pool_id: str | None = None,
    materialization_id: UUID | None = None,
    expected_lease_epoch: int | None = None,
) -> TaskImageMaterialization | None:
    """Atomically claim queued work, or the exact selection admitted by a pool.

    Exact selection never substitutes another queue head or performs unrelated
    queue maintenance. Its caller still owns snapshot qualification, the durable
    global-grant link, and commit/rollback; no network operation belongs here.
    """
    from loom.nebius_rollout_guard import admission_open

    if materialization_id is not None or expected_lease_epoch is not None:
        if (not isinstance(materialization_id, UUID) or not materialization_id.int
                or type(expected_lease_epoch) is not int or not 0 <= expected_lease_epoch < 2**63 - 1):
            raise ValueError("exact materialization claim requires a valid ID and expected lease epoch")
    cpu_arch = execution_cpu_arch(cpu_arch)
    _assert_no_pending_task_image_writes(session)
    # Queue maintenance writes precede candidate selection. Establish retained
    # ownership before those writes, not only after learning the candidate's
    # source kind. An AUTOCOMMIT lease cannot keep any admission lock atomic.
    from loom.task_bundle_source_journal import require_task_bundle_transaction

    await require_task_bundle_transaction(session)
    scope: tuple[ColumnElement[bool], ...] = ()
    if materialization_id is not None:
        scope = (TaskImageMaterialization.id == materialization_id,
                 TaskImageMaterialization.lease_epoch == expected_lease_epoch)
    if nebius_pool_id is not None:
        if not nebius_pool_id.strip():
            raise ValueError("nebius_pool_id must not be empty")
        scope += (_nebius_demand_exists(TaskImageMaterialization, pool_id=nebius_pool_id),)
    if not await admission_open(session):
        return None
    now = datetime.now(UTC)
    if materialization_id is None:
        await session.execute(
            update(TaskImageMaterialization)
            .where(
                *scope,
                TaskImageMaterialization.state.in_(("claimed", "running")),
                TaskImageMaterialization.lease_expires_at <= now,
                TaskImageMaterialization.attempt_count >= TaskImageMaterialization.max_attempts,
            )
            .values(
                state="failed",
                claimed_by=None,
                lease_expires_at=None,
                failure_reason="lease_expired",
                failure_message="task image build lease expired at the attempt limit",
                finished_at=now,
                updated_at=now,
            )
        )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(
            *scope,
            TaskImageMaterialization.cpu_arch == cpu_arch,
            TaskImageMaterialization.attempt_count < TaskImageMaterialization.max_attempts,
            or_(
                and_(
                    TaskImageMaterialization.state == "queued",
                    or_(
                        TaskImageMaterialization.next_attempt_at.is_(None),
                        TaskImageMaterialization.next_attempt_at <= now,
                    ),
                ),
                and_(
                    TaskImageMaterialization.state.in_(("claimed", "running")),
                    TaskImageMaterialization.lease_expires_at <= now,
                ),
            ),
        )
        .order_by(TaskImageMaterialization.created_at, TaskImageMaterialization.id)
        .limit(1)
        .execution_options(populate_existing=True)
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None
    await admit_task_image_source(session, row=row)
    row.state = "claimed"
    row.claimed_by = builder_id
    row.lease_epoch += 1
    row.attempt_count += 1
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.claimed_at = now
    row.started_at = None
    row.finished_at = None
    row.next_attempt_at = None
    row.failure_reason = None
    row.failure_message = None
    row.updated_at = now
    session.add(
        TaskImageMaterializationAttempt(
            materialization_id=row.id,
            attempt_number=row.attempt_count,
            lease_epoch=row.lease_epoch,
            builder_id=builder_id,
            claimed_at=now,
        )
    )
    await session.flush()
    return row


async def _locked_owned_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    allowed_states: tuple[str, ...],
    now: datetime,
) -> TaskImageMaterialization:
    _assert_no_pending_task_image_writes(session)
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if (
        row is None
        or row.state not in allowed_states
        or row.claimed_by != builder_id
        or row.lease_epoch != lease_epoch
        or row.lease_expires_at is None
        or row.lease_expires_at <= now
    ):
        raise TaskImageLeaseConflictError("stale task image materialization lease")
    return row


async def start_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    _assert_no_pending_task_image_writes(session)
    with session.no_autoflush:
        row = await _locked_owned_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=builder_id,
            lease_epoch=lease_epoch,
            allowed_states=("claimed",),
            now=now,
        )
    await admit_task_image_source(session, row=row)
    row.state = "running"
    row.started_at = now
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.updated_at = now
    await session.flush()
    return row


async def heartbeat_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.updated_at = now
    await session.flush()
    return row


async def complete_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    registry_images: dict[str, str],
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("running",),
        now=now,
    )
    # The current immutable attempt, not a caller-controlled builder-name
    # prefix or a failure-budget counter, determines publication authority.
    # Keep materialization -> attempt ordering; this path never waits for the
    # earlier publication epoch/key/grant locks used by verified completion.
    current_attempt = await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(
            TaskImageMaterializationAttempt.materialization_id == row.id,
            TaskImageMaterializationAttempt.lease_epoch == row.lease_epoch,
        )
        .with_for_update()
    )
    if current_attempt is not None and current_attempt.grant_id is not None:
        raise TaskImageCompletionError("rootless attempts require verified publication")
    try:
        registry_images = validate_task_image_registry_images(
            registry_images,
            expected_components=_expected_registry_image_components(row.task_config),
            require_complete=True,
        )
    except ValueError as exc:
        raise TaskImageCompletionError(str(exc)) from exc
    await _record_attempt_publication_evidence(
        session,
        row,
        builder_id=builder_id,
        attempt_count=row.attempt_count,
        lease_epoch=lease_epoch,
        registry_images=registry_images,
        now=now,
    )
    row.state = "ready"
    row.registry_images = dict(registry_images)
    row.claimed_by = None
    row.lease_expires_at = None
    row.ready_at = now
    row.finished_at = now
    row.updated_at = now
    await session.flush()
    return row


async def fail_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    retryable: bool,
    failure_reason: str,
    failure_message: str,
    registry_images: dict[str, str],
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    if registry_images:
        try:
            registry_images = validate_task_image_registry_images(
                registry_images,
                expected_components=_expected_registry_image_components(row.task_config),
                require_nonempty=False,
            )
        except ValueError as exc:
            raise TaskImageCompletionError(str(exc)) from exc
    await _record_attempt_publication_evidence(
        session,
        row,
        builder_id=builder_id,
        attempt_count=row.attempt_count,
        lease_epoch=lease_epoch,
        registry_images=registry_images,
        now=now,
    )
    row.claimed_by = None
    row.lease_expires_at = None
    row.failure_reason = failure_reason
    row.failure_message = failure_message
    row.registry_images = {**row.registry_images, **registry_images}
    row.updated_at = now
    if retryable and row.attempt_count < row.max_attempts:
        backoff_seconds = min(
            30.0 * (2 ** max(row.attempt_count - 1, 0)),
            _MAX_RETRY_BACKOFF_SECONDS,
        )
        row.state = "queued"
        row.next_attempt_at = now + timedelta(seconds=backoff_seconds)
        row.finished_at = None
    else:
        row.state = "failed"
        row.next_attempt_at = None
        row.finished_at = now
    await session.flush()
    return row


async def retry_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
) -> TaskImageMaterialization:
    """Requeue an exhausted or suspect ready image under an admin decision."""
    if any(
        isinstance(row, TaskImageMaterialization)
        for row in (*session.new, *session.dirty, *session.deleted)
    ):
        raise TaskImageRetryConflictError("task image retry has pending materialization writes")
    now = datetime.now(UTC)
    # Inspect the locked source kind before selecting its admission contract.
    # An unsafe strong-source caller must not flush unrelated pending Task rows
    # merely to discover that its transaction cannot retain the required locks.
    with session.no_autoflush:
        row = await session.scalar(
            select(TaskImageMaterialization)
            .where(TaskImageMaterialization.id == materialization_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    if row is None:
        raise TaskImageRetryConflictError("task image materialization does not exist")
    if row.state not in {"failed", "ready"}:
        raise TaskImageRetryConflictError(
            f"task image materialization in state {row.state!r} cannot be retried"
        )
    try:
        await admit_task_image_source(session, row=row)
    except ValueError as error:
        raise TaskImageRetryConflictError(str(error)) from error
    row.state = "queued"
    row.attempt_count = 0
    row.next_attempt_at = None
    row.claimed_by = None
    row.lease_epoch += 1
    row.lease_expires_at = None
    row.failure_reason = None
    row.failure_message = None
    row.claimed_at = None
    row.started_at = None
    row.ready_at = None
    row.finished_at = None
    row.registry_images = {}
    row.ready_publication_operation_id = None
    row.last_referenced_at = now
    row.unreferenced_at = None
    row.updated_at = now
    await session.flush()
    return row


async def claim_task_image_registry_gc(
    session: AsyncSession,
    *,
    gc_id: str,
    grace_hours: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
    materialization_ids: list[UUID] | None = None,
) -> TaskImageMaterialization | None:
    """Fence one image set without a signed publication owner.

    Native maintenance supplies an explicit inventory so it cannot observe or
    retire another registry's historical rows. Signed publication maps retain
    their separate ownership contract.
    """
    if grace_hours < 0:
        raise ValueError("grace_hours must be non-negative")
    # The locked refresh must not discard or implicitly flush caller-owned edits.
    if any(
        isinstance(row, TaskImageMaterialization)
        for row in (*session.new, *session.dirty, *session.deleted)
    ):
        raise ValueError("task image GC has pending materialization writes")
    from loom.task_bundle_source_journal import require_task_bundle_transaction

    await require_task_bundle_transaction(session)
    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=grace_hours)
    legacy_owned: ColumnElement[bool] = TaskImageMaterialization.ready_publication_operation_id.is_(None)
    if materialization_ids is not None:
        legacy_owned = and_(legacy_owned, TaskImageMaterialization.id.in_(materialization_ids))
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            legacy_owned,
            TaskImageMaterialization.state.in_(("ready", "failed", "retired")),
            _registry_publication_exists(TaskImageMaterialization),
            _durable_reference_exists(TaskImageMaterialization),
        )
        .values(
            last_referenced_at=now,
            unreferenced_at=None,
            updated_at=now,
        )
    )
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            legacy_owned,
            TaskImageMaterialization.state.in_(("ready", "failed", "retired")),
            _registry_publication_exists(TaskImageMaterialization),
            TaskImageMaterialization.unreferenced_at.is_(None),
            ~_durable_reference_exists(TaskImageMaterialization),
        )
        .values(unreferenced_at=now, updated_at=now)
    )
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            legacy_owned,
            TaskImageMaterialization.state == "retiring",
            TaskImageMaterialization.lease_expires_at <= now,
            _durable_reference_exists(TaskImageMaterialization),
        )
        .values(
            state="queued",
            attempt_count=0,
            next_attempt_at=None,
            claimed_by=None,
            lease_expires_at=None,
            registry_images={},
            failure_reason=None,
            failure_message=None,
            ready_at=None,
            finished_at=None,
            last_referenced_at=now,
            unreferenced_at=None,
            updated_at=now,
        )
    )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(
            legacy_owned,
            or_(
                and_(
                    TaskImageMaterialization.state.in_(("ready", "failed", "retired")),
                    _registry_publication_exists(TaskImageMaterialization),
                    TaskImageMaterialization.unreferenced_at <= cutoff,
                ),
                and_(
                    TaskImageMaterialization.state == "retiring",
                    TaskImageMaterialization.lease_expires_at <= now,
                ),
            ),
            ~_durable_reference_exists(TaskImageMaterialization),
        )
        .order_by(
            TaskImageMaterialization.unreferenced_at,
            TaskImageMaterialization.id,
        )
        .limit(1)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    if row is None:
        return None
    row.state = "retiring"
    row.claimed_by = gc_id
    row.lease_epoch += 1
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.claimed_at = now
    row.updated_at = now
    await session.flush()
    return row


async def complete_task_image_registry_gc(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    gc_id: str,
    lease_epoch: int,
) -> TaskImageMaterialization:
    """Commit deletion only for the current owner, requeuing raced references."""
    from loom.task_bundle_source_journal import require_task_bundle_transaction

    await require_task_bundle_transaction(session)
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=gc_id,
        lease_epoch=lease_epoch,
        allowed_states=("retiring",),
        now=now,
    )
    referenced = bool(await session.scalar(select(_durable_reference_exists(row))))
    if referenced:
        await admit_task_image_source(session, row=row)
    row.registry_images = {}
    row.registry_image_history = []
    row.claimed_by = None
    row.lease_expires_at = None
    row.updated_at = now
    if referenced:
        row.state = "queued"
        row.attempt_count = 0
        row.next_attempt_at = None
        row.failure_reason = None
        row.failure_message = None
        row.ready_at = None
        row.finished_at = None
        row.last_referenced_at = now
        row.unreferenced_at = None
    else:
        row.state = "retired"
        row.finished_at = now
        await release_task_image_source(session, row=row)
    await session.flush()
    return row


async def observe_unpublished_task_image_retirement(
    engine: AsyncEngine,
    *,
    materialization_id: UUID,
    now: datetime,
    grace: timedelta = timedelta(hours=24),
) -> Literal["absent", "ineligible", "pinned", "observing", "retired"]:
    """Retire one abandoned strong-image owner without registry or object I/O.

    Own a short READ COMMITTED transaction so no caller's pending writes or
    fixed snapshot can cross the image/source fence. The caller's bounded,
    fair inventory chooses IDs; busy catalog/image locks abort for a later pass.
    Grace measures observations, not proof of continuous absence. Native
    attempt retirement must independently finish before releasing its input.
    This primitive is not scheduled or enabled by its presence.
    """
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("retirement time must be UTC")
    if not timedelta(0) < grace <= timedelta(days=365):
        raise ValueError("retirement grace outside bounds")
    async with asyncio.timeout(5):
        async with engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with (
                AsyncSession(connection, expire_on_commit=False, autoflush=False) as session,
                session.begin(),
            ):
                await session.execute(text("SET LOCAL statement_timeout = '1s'"))
                await session.execute(text("SET LOCAL idle_in_transaction_session_timeout = '1s'"))
                # Legacy catalog producers do not all acquire image locks.
                # Match attempt retention's conservative catalog fence until
                # every producer has converged on the shared admission API.
                await session.execute(text("LOCK TABLE public.tasks IN SHARE MODE NOWAIT"))
                row = await session.scalar(
                    select(TaskImageMaterialization)
                    .where(TaskImageMaterialization.id == materialization_id)
                    .with_for_update(nowait=True)
                )
                if row is None:
                    return "absent"
                if not row.bundle_content_manifest_sha256:
                    return "ineligible"
                if row.state == "retired":
                    return "retired"
                if (
                    row.state not in ("queued", "failed")
                    or row.claimed_by is not None
                    or row.registry_images
                    or row.registry_image_history
                    or row.ready_publication_operation_id is not None
                ):
                    return "ineligible"
                if now < row.updated_at:
                    raise ValueError("retirement observation time moved backward")
                publication = exists().where(
                    TaskImagePublicationEvidence.materialization_id == row.id,
                )
                unretired_native_attempt = (
                    select(TaskImageMaterializationAttempt.id)
                    .outerjoin(
                        TaskImageAttemptRetention,
                        TaskImageAttemptRetention.attempt_id == TaskImageMaterializationAttempt.id,
                    )
                    .where(
                        TaskImageMaterializationAttempt.materialization_id == row.id,
                        TaskImageMaterializationAttempt.grant_id.is_not(None),
                        TaskImageAttemptRetention.retired_at.is_(None),
                    )
                    .exists()
                )
                execution = exists().where(
                    TrialTaskImageMaterialization.materialization_id == row.id,
                    ServiceExecutionLease.trial_id == TrialTaskImageMaterialization.trial_id,
                    or_(
                        ServiceExecutionLease.deleted_at.is_(None),
                        ServiceExecutionLease.cleanup_state != "complete",
                    ),
                )
                if await session.scalar(select(publication)):
                    return "ineligible"
                pinned = bool(await session.scalar(select(or_(
                    _durable_reference_exists(row), execution, unretired_native_attempt,
                )))) or (row.lease_expires_at is not None and row.lease_expires_at > now)
                row.updated_at = now
                if pinned:
                    row.last_referenced_at = now
                    row.unreferenced_at = None
                    return "pinned"
                if row.unreferenced_at is None:
                    row.unreferenced_at = now
                if now - row.unreferenced_at < grace:
                    return "observing"
                row.state = "retired"
                row.lease_epoch += 1
                row.claimed_by = None
                row.lease_expires_at = None
                row.next_attempt_at = None
                row.finished_at = now
                await release_task_image_source(session, row=row)
                return "retired"


def task_image_materialization_payload(
    row: TaskImageMaterialization,
) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "materialization_key": row.materialization_key,
        "task_id": row.task_id,
        "task_checksum": row.task_checksum,
        "cpu_arch": row.cpu_arch,
        "task_config": row.task_config,
        "task_source": row.task_source,
        "task_source_provenance": row.task_source_provenance,
        "state": row.state,
        "attempt_count": row.attempt_count,
        "max_attempts": row.max_attempts,
        "lease_epoch": row.lease_epoch,
        "lease_expires_at": (row.lease_expires_at.isoformat() if row.lease_expires_at else None),
        "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
        "registry_images": row.registry_images,
        "registry_image_history": row.registry_image_history,
        "failure_reason": row.failure_reason,
        "failure_message": row.failure_message,
    }


async def same_task_compatible_cache_key(
    session: AsyncSession,
    *,
    task_id: str,
    cpu_arch: str,
    exclude_materialization_key: str,
) -> str | None:
    """Return a prior ready same-task materialization_key for BuildKit cache import.

    Cross-task keys are never returned. Results still publish under the current
    materialization; this only seeds disposable layer cache.
    """
    return cast(
        str | None,
        await session.scalar(
            select(TaskImageMaterialization.materialization_key)
            .where(
                TaskImageMaterialization.task_id == task_id,
                TaskImageMaterialization.cpu_arch == cpu_arch,
                TaskImageMaterialization.materialization_key != exclude_materialization_key,
                TaskImageMaterialization.state == "ready",
            )
            .order_by(
                TaskImageMaterialization.ready_at.desc().nulls_last(),
                TaskImageMaterialization.created_at.desc(),
            )
            .limit(1)
        ),
    )
