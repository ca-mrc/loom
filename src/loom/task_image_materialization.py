"""Durable task-image materialization identities and queue helpers."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig

if TYPE_CHECKING:
    from loom.task_bundle_source import TaskBundleSourceSpecV1

NativeCPUArch = Literal["x86_64", "arm64"]
_CHECKSUM_RE = re.compile(r"[0-9a-f]{64}")
_KEY_DOMAIN = "task-image-materialization-v1"
_BareSHA256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ImmutableRegistryImage = Annotated[
    str,
    StringConstraints(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$"),
]
_IMMUTABLE_REGISTRY_IMAGE_RE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")
_TASK_IMAGE_COMPONENT_RE = re.compile(r"[^\s]{1,256}")
MAX_TASK_IMAGE_COMPONENTS = 128


class TaskImageSnapshotConflictError(RuntimeError):
    """The persisted image identity conflicts with the submitted task snapshot."""


class TaskImageExecutionGrantV1(BaseModel):
    """Immutable build evidence carried from scheduling into execution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["loom.task-image-execution-grant.v1"]
    materialization_id: UUID
    materialization_key: _BareSHA256
    cpu_arch: NativeCPUArch
    task_checksum: _BareSHA256
    task_config: dict[str, Any]
    task_source: str | None
    task_source_provenance: dict[str, Any]
    registry_images: dict[str, ImmutableRegistryImage]

    @field_validator("registry_images")
    @classmethod
    def registry_images_must_not_be_empty(cls, values: dict[str, str]) -> dict[str, str]:
        if not values:
            raise ValueError("registry_images must not be empty")
        return values

    @model_validator(mode="after")
    def matches_frozen_task_snapshot(self) -> TaskImageExecutionGrantV1:
        task = TaskConfig.model_validate(self.task_config)
        if self.cpu_arch not in declared_task_image_architectures(task):
            raise ValueError("cpu_arch is not required by the frozen task snapshot")
        expected = required_task_image_components(task)
        if set(self.registry_images) != expected:
            raise ValueError("registry_images do not match the frozen task snapshot")
        manifest_digest = task_bundle_content_manifest_digest(self.task_source_provenance)
        if manifest_digest and self.materialization_key != task_image_materialization_key(
            task_id=task.task.id,
            task_checksum=self.task_checksum,
            cpu_arch=self.cpu_arch,
            bundle_content_manifest_sha256=manifest_digest,
        ):
            raise ValueError("content manifest does not match the execution grant identity")
        return self


def task_bundle_content_manifest_digest(provenance: Mapping[str, Any]) -> str:
    """Only absent provenance selects legacy identity; malformed presence rejects."""
    key = "bundle_content_manifest_sha256"
    if key not in provenance:
        return ""
    digest = provenance[key]
    if type(digest) is not str or _CHECKSUM_RE.fullmatch(digest) is None:
        raise ValueError("bundle_content_manifest_sha256 must be a bare SHA-256 digest")
    return digest

def resolve_prepared_task(task: TaskConfig, grant: TaskImageExecutionGrantV1) -> TaskConfig:
    """Use the ready image for resource admission without changing its frozen source."""
    if task != TaskConfig.model_validate(grant.task_config):
        raise ValueError("prepared images do not match the frozen task")
    payload = task.model_dump(mode="json")
    environment = payload["environment"]
    environment["cpu_arch"] = grant.cpu_arch
    if task.environment.dockerfile is not None:
        environment.update(
            dockerfile=None, docker_build_context=None, docker_build_args={},
            docker_build_target=None, docker_image=grant.registry_images["task"],
        )
    for sidecar in environment["sidecars"]:
        if sidecar["dockerfile"] is not None:
            sidecar.update(dockerfile=None, docker_build_context=None,
                           docker_image=grant.registry_images[f"sidecar:{sidecar['name']}"])
    return TaskConfig.model_validate(payload)


def canonical_task_checksum(task_checksum: str) -> str:
    checksum = task_checksum.removeprefix("sha256:")
    if _CHECKSUM_RE.fullmatch(checksum) is None:
        raise ValueError("task_checksum must be a SHA-256 digest")
    return checksum


def current_task_image_reference(row: Any) -> ColumnElement[bool]:
    """Match the catalog revision without broadening exact historical trial pins."""
    key = "bundle_content_manifest_sha256"
    digest = row.bundle_content_manifest_sha256
    return and_(
        Task.id == row.task_id,
        or_(
            Task.checksum == row.task_checksum,
            Task.checksum == func.concat("sha256:", row.task_checksum),
        ),
        or_(
            and_(digest == "", ~Task.source_provenance.bool_op("?")(key)),
            and_(
                digest != "",
                func.jsonb_typeof(Task.source_provenance[key]) == "string",
                Task.source_provenance[key].astext == digest,
            ),
        ),
    )


def required_task_image_architectures(task: TaskConfig) -> tuple[NativeCPUArch, ...]:
    """Plan new builds only for supported execution architectures."""
    architecture = execution_cpu_arch(task.environment.cpu_arch)
    return (architecture,) if declared_task_image_architectures(task) else ()


def declared_task_image_architectures(task: TaskConfig) -> tuple[NativeCPUArch, ...]:
    """Read historical grants without rewriting their architecture identity."""
    has_dockerfile = task.environment.dockerfile is not None or any(
        sidecar.dockerfile is not None for sidecar in task.environment.sidecars
    )
    if not has_dockerfile:
        return ()
    if task.environment.cpu_arch == "any":
        return ("x86_64", "arm64")
    return (task.environment.cpu_arch,)


def required_task_image_components(task: TaskConfig) -> set[str]:
    components: set[str] = set()
    if task.environment.dockerfile is not None:
        components.add("task")
    components.update(
        f"sidecar:{sidecar.name}"
        for sidecar in task.environment.sidecars
        if sidecar.dockerfile is not None
    )
    return components


def validate_task_image_registry_images(
    registry_images: dict[str, str],
    *,
    expected_components: set[str] | None = None,
    require_complete: bool = False,
    require_nonempty: bool = True,
) -> dict[str, str]:
    if require_nonempty and not registry_images:
        raise ValueError("registry_images must not be empty")
    if len(registry_images) > MAX_TASK_IMAGE_COMPONENTS:
        raise ValueError("registry_images contains too many components")
    if any(
        _TASK_IMAGE_COMPONENT_RE.fullmatch(component) is None
        or len(image) > 2048
        or _IMMUTABLE_REGISTRY_IMAGE_RE.fullmatch(image) is None
        for component, image in registry_images.items()
    ):
        raise ValueError(
            "registry_images must map bounded component names to immutable digest references"
        )
    if expected_components is not None:
        actual_components = set(registry_images)
        unexpected = actual_components - expected_components
        missing = expected_components - actual_components if require_complete else set()
        if unexpected or missing:
            missing_label = ",".join(sorted(missing)) or "none"
            unexpected_label = ",".join(sorted(unexpected)) or "none"
            raise ValueError(
                "registry_images do not match the task snapshot "
                f"(missing={missing_label}; unexpected={unexpected_label})"
            )
    return dict(registry_images)


def task_image_materialization_key(
    *,
    task_id: str,
    task_checksum: str,
    cpu_arch: str,
    bundle_content_manifest_sha256: str = "",
) -> str:
    """Preserve v1 identities; opt-in content manifests select a separate domain.

    The empty discriminator means historical, checksum-only identity. A supplied
    digest is deliberately bare and strict, unlike the legacy checksum input.
    Producers must not opt in until their complete reader path verifies content.
    """
    if cpu_arch not in {"x86_64", "arm64"}:
        raise ValueError("cpu_arch must be x86_64 or arm64")
    checksum = canonical_task_checksum(task_checksum)
    if type(bundle_content_manifest_sha256) is not str or (
        bundle_content_manifest_sha256 != ""
        and _CHECKSUM_RE.fullmatch(bundle_content_manifest_sha256) is None
    ):
        raise ValueError("bundle_content_manifest_sha256 must be empty or a bare SHA-256 digest")
    if bundle_content_manifest_sha256:
        if "\0" in task_id:
            raise ValueError("task_id must not contain an identity separator")
        material = "\0".join(
            (
                "task-image-materialization-v2",
                task_id,
                checksum,
                cpu_arch,
                bundle_content_manifest_sha256,
            )
        )
    else:
        material = "\0".join((_KEY_DOMAIN, task_id, checksum, cpu_arch))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


async def ensure_task_image_materializations(
    session: AsyncSession,
    *,
    task_row: Task,
) -> tuple[TaskImageMaterialization, ...]:
    rows = await _lock_task_image_materializations(session, task_row=task_row)
    await _reference_task_image_materializations(session, rows=rows)
    return rows


def _assert_no_pending_task_image_writes(session: AsyncSession) -> None:
    if any(
        isinstance(row, TaskImageMaterialization)
        for row in (*session.new, *session.dirty, *session.deleted)
    ):
        raise RuntimeError("task image ensure has pending materialization writes")


async def _lock_task_image_materializations(
    session: AsyncSession, *, task_row: Task
) -> tuple[TaskImageMaterialization, ...]:
    """Internal staging for caller-atomic publication; not source admission."""
    task = TaskConfig.model_validate(task_row.config)
    architectures = required_task_image_architectures(task)
    if not architectures:
        return ()

    # Locked refresh below must never overwrite pending caller-owned state,
    # including when the caller has deliberately suppressed ORM autoflush.
    _assert_no_pending_task_image_writes(session)

    task_checksum = canonical_task_checksum(task_row.checksum)
    manifest_digest = task_bundle_content_manifest_digest(task_row.source_provenance)
    if manifest_digest and task.task.id != task_row.id:
        raise TaskImageSnapshotConflictError("frozen task snapshot identity differs from the materialization")
    if manifest_digest:
        from loom.task_bundle_source_journal import require_task_bundle_transaction

        await require_task_bundle_transaction(session)
    keys = {
        cpu_arch: task_image_materialization_key(
            task_id=task_row.id,
            task_checksum=task_checksum,
            cpu_arch=cpu_arch,
            bundle_content_manifest_sha256=manifest_digest,
        )
        for cpu_arch in architectures
    }
    for cpu_arch in architectures:
        await session.execute(
            pg_insert(TaskImageMaterialization)
            .values(
                id=uuid4(),
                materialization_key=keys[cpu_arch],
                task_id=task_row.id,
                task_checksum=task_checksum,
                bundle_content_manifest_sha256=manifest_digest,
                cpu_arch=cpu_arch,
                task_config=task_row.config,
                task_source=task_row.source,
                task_source_provenance=task_row.source_provenance,
                state="queued",
            )
            .on_conflict_do_nothing(index_elements=["materialization_key"])
        )

    rows = (
        (
            await session.execute(
                select(TaskImageMaterialization)
                .where(TaskImageMaterialization.materialization_key.in_(keys.values()))
                .order_by(TaskImageMaterialization.cpu_arch, TaskImageMaterialization.id)
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    by_arch = {row.cpu_arch: row for row in rows}
    if set(by_arch) != set(architectures):
        raise TaskImageSnapshotConflictError("task image materialization identity conflict")
    if manifest_digest and any(
        row.bundle_content_manifest_sha256 != manifest_digest
        or row.task_id != task_row.id
        or row.task_checksum != task_checksum
        or row.task_config != task_row.config
        or row.task_source != task_row.source
        or row.task_source_provenance != task_row.source_provenance
        for row in rows
    ):
        raise TaskImageSnapshotConflictError("frozen content-manifest snapshot conflicts with existing materialization")
    for row in rows:
        if row.task_config != task_row.config:
            raise TaskImageSnapshotConflictError("task image materialization snapshot conflicts with task checksum")
        if row.state == "retired" and not manifest_digest:
            # Legacy caches may need current upload objects after retention GC.
            # Manifest-bearing sources retain the exact admitted journal identity.
            row.task_source = task_row.source
            row.task_source_provenance = task_row.source_provenance or {}
    return tuple(by_arch[cpu_arch] for cpu_arch in architectures)


async def _reference_task_image_materializations(
    session: AsyncSession, *, rows: Sequence[TaskImageMaterialization]
) -> None:
    """Finish staged image rows only after source admission in the same transaction."""
    if not rows:
        return
    for row in rows:
        await admit_task_image_source(session, row=row)
    now = datetime.now(UTC)
    for row in rows:
        row.last_referenced_at = now
        row.unreferenced_at = None
        row.updated_at = now
        await _refund_cancelled_image_builds(session, row)
        if row.state == "retired":
            row.state = "queued"
            row.attempt_count = 0
            row.next_attempt_at = None
            row.claimed_by = None
            row.lease_expires_at = None
            row.registry_images = {}
            row.failure_reason = None
            row.failure_message = None
            row.claimed_at = None
            row.started_at = None
            row.ready_at = None
            row.finished_at = None
    await session.flush()


async def _refund_cancelled_image_builds(session: AsyncSession, row: TaskImageMaterialization) -> None:
    """Recover pre-refund native cancellations under the caller's row lock.

    Walk only the current budget: admin retry/cache retirement can reset its
    counter, while immutable attempt identities and epochs span all budgets.
    Missing history remains charged rather than inventing retry authority.
    """
    if row.state not in {"queued", "failed"} or row.failure_reason != "build_cancelled" or not row.attempt_count:
        return
    attempts = await session.scalars(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.materialization_id == row.id,
        TaskImageMaterializationAttempt.lease_epoch <= row.lease_epoch,
    ).order_by(TaskImageMaterializationAttempt.lease_epoch.desc()).with_for_update())
    expected, refunded = row.attempt_count, 0
    for attempt in attempts:
        native = attempt.native_build or {}
        if native.get("retry_budget_refunded"):
            continue
        if attempt.attempt_number != expected:
            break
        if native.get("failure_reason") == "build_cancelled":
            attempt.native_build = {**native, "retry_budget_refunded": True}
            refunded += 1
        expected -= 1
        if expected == 0:
            break
    if refunded:
        row.attempt_count -= refunded
        row.state = "queued"
        row.next_attempt_at = None
        row.finished_at = None


async def admit_task_image_source(
    session: AsyncSession, *, row: TaskImageMaterialization
) -> TaskBundleSourceSpecV1 | None:
    """Pin a registered strong source while holding its materialization lock."""
    digest = task_bundle_content_manifest_digest(row.task_source_provenance)
    if row.bundle_content_manifest_sha256 != digest:
        raise ValueError("registered source manifest differs from materialization identity")
    if not digest:
        return None
    if row.materialization_key != task_image_materialization_key(
        task_id=row.task_id, task_checksum=row.task_checksum, cpu_arch=row.cpu_arch,
        bundle_content_manifest_sha256=digest,
    ):
        raise ValueError("registered source differs from materialization key")
    # Bundle parsing depends on native-plan identity helpers in this module.
    # Resolve the journal at the call boundary, not during that import cycle.
    from loom.task_bundle_source_journal import admit_task_bundle_source

    return await admit_task_bundle_source(
        session,
        task_id=row.task_id,
        task_checksum=row.task_checksum,
        task_config=row.task_config,
        task_source=row.task_source,
        task_source_provenance=row.task_source_provenance,
        reference_kind="materialization",
        owner_id=str(row.id),
    )


async def release_task_image_source(
    session: AsyncSession, *, row: TaskImageMaterialization
) -> None:
    """Release only this retired image's input pin under its owning row lock.

    Catalog and trial references have separate lifecycle owners. Releasing this
    reference does not delete storage or retire another source incarnation.
    """
    digest = task_bundle_content_manifest_digest(row.task_source_provenance)
    if not digest:
        return
    if row.state != "retired" or row.bundle_content_manifest_sha256 != digest or not row.task_source:
        raise ValueError("source release requires an exact retired materialization")
    from loom.task_bundle_source_journal import release_task_bundle_reference

    await release_task_bundle_reference(
        session,
        source_id=hashlib.sha256(row.task_source.encode()).hexdigest(),
        reference_kind="materialization",
        owner_id=str(row.id),
    )


async def get_trial_task_image_execution_grant(
    session: AsyncSession,
    *,
    trial_id: UUID,
    cpu_arches: list[str],
) -> TaskImageExecutionGrantV1 | None:
    """Lock and return the exact ready image snapshot selected for a claim."""
    _assert_no_pending_task_image_writes(session)
    with session.no_autoflush:
        row = await session.scalar(
            select(TaskImageMaterialization)
            .join(
                TrialTaskImageMaterialization,
                TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
            )
            .join(Trial, Trial.id == TrialTaskImageMaterialization.trial_id)
            .where(
                Trial.id == trial_id,
                TaskImageMaterialization.task_id == Trial.task_id,
                TaskImageMaterialization.cpu_arch.in_(cpu_arches),
                TaskImageMaterialization.state == "ready",
                # Native publications require the signed V2 reader and one-use
                # start authority; this legacy snapshot must never downgrade them.
                TaskImageMaterialization.ready_publication_operation_id.is_(None),
            )
            .order_by(TaskImageMaterialization.cpu_arch, TaskImageMaterialization.id)
            .limit(1)
            .execution_options(populate_existing=True)
            .with_for_update()
        )

    if row is None:
        has_prerequisite = bool(
            await session.scalar(
                select(exists().where(TrialTaskImageMaterialization.trial_id == trial_id))
            )
        )
        if has_prerequisite:
            raise RuntimeError("claimed trial no longer has a ready task-image materialization")
        return None
    try:
        await admit_task_image_source(session, row=row)
    except ValueError as error:
        raise RuntimeError(str(error)) from error
    return TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=row.id,
        materialization_key=row.materialization_key,
        cpu_arch=row.cpu_arch,
        task_checksum=row.task_checksum,
        task_config=row.task_config,
        task_source=row.task_source,
        task_source_provenance=row.task_source_provenance,
        registry_images=row.registry_images,
    )
