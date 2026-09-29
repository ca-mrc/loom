"""TaskSet intake: manifest validation, blob upload, transactional enqueue.

Sub-plan 3 implements the ``task_set_materialization_jobs`` consumer.
Jobs remain ``queued`` after successful POST until that worker ships.
"""

from __future__ import annotations

import asyncio
import re
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from fastapi import HTTPException, UploadFile
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Task,
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
    TeamQuota,
)
from loom.db.task_set_visibility import visible_task_sets
from loom.models.taskset import (
    UserTaskSetManifest,
    bundle_object_key,
    task_set_id_for,
)
from loom.taskset.intents import IntentWarning, normalize_intents
from loom.taskset.storage_bytes import team_taskset_storage_bytes

_ACTIVE_JOB_STATES = frozenset({"queued", "claimed", "running"})
_PATH_TRAVERSAL = re.compile(r"(^|[/\\])\.\.([/\\]|$)|(^|[/\\])\.\.?$")
_MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TaskSetIntakeResult:
    task_set_id: str
    status: str
    intents: list[str]
    manifest_intents: list[str]
    inferred_intents: list[str]
    capabilities: list[str]
    warnings: list[dict[str, str]]
    evaluation_ready: bool
    task_count: int
    job_id: UUID


def _reject_unsafe_filename(filename: str) -> None:
    if not filename or filename.strip() != filename:
        raise HTTPException(status_code=400, detail="invalid upload filename")
    if _PATH_TRAVERSAL.search(filename) or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="path traversal in upload filename")


def _blob_uri(*, bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _storage_prefix(*, team_id: UUID, slug: str) -> str:
    return f"tasksets/user/{team_id}/{slug}"


async def _read_upload(upload: UploadFile) -> bytes:
    data = await upload.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty upload part")
    return data


async def _read_upload_with_size_cap(
    upload: UploadFile,
    *,
    max_bytes: int,
    too_large_detail: str = "manifest_too_large",
) -> bytes:
    data = await upload.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty upload part")
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail=too_large_detail)
    return data


async def parse_manifest_upload(
    manifest: UploadFile,
    *,
    max_bytes: int = 1_048_576,
) -> tuple[UserTaskSetManifest, dict[str, Any]]:
    _reject_unsafe_filename(manifest.filename or "manifest")
    raw_bytes = await _read_upload_with_size_cap(manifest, max_bytes=max_bytes)
    try:
        parsed = yaml.safe_load(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid manifest yaml: {exc}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="manifest must be a yaml mapping")
    try:
        model = UserTaskSetManifest.model_validate(parsed)
    except ValidationError as exc:
        for err in exc.errors():
            if "verifier_required_for_evaluation" in str(err.get("msg", "")):
                raise HTTPException(
                    status_code=400,
                    detail="verifier_required_for_evaluation",
                ) from exc
        raise HTTPException(status_code=400, detail=exc.errors()) from exc
    # YAML parses unquoted ISO timestamps into datetime objects. Keep the
    # persisted manifest JSON-compatible without injecting optional defaults.
    if model.metadata.expires_at is not None:
        parsed["metadata"]["expires_at"] = model.metadata.expires_at.isoformat()
    return model, parsed


def _warnings_to_dict(warnings: tuple[IntentWarning, ...]) -> list[dict[str, str]]:
    return [{"code": w.code, "message": w.message} for w in warnings]


def _upload_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


@dataclass
class _PreparedUpload:
    file: Any
    size_bytes: int


async def _prepare_upload_with_size_cap(
    upload: UploadFile,
    *,
    max_bytes: int,
) -> _PreparedUpload:
    total = 0
    tmp = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)
    try:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(status_code=413, detail="bundle_too_large")
            tmp.write(chunk)
        if total == 0:
            raise HTTPException(status_code=400, detail="empty upload part")
        tmp.seek(0)
        return _PreparedUpload(file=tmp, size_bytes=total)
    except Exception:
        tmp.close()
        raise


def _upload_prepared_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    upload: _PreparedUpload,
    content_type: str,
) -> None:
    upload.file.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=upload.file,
        ContentType=content_type,
    )


async def check_taskset_storage_quota(
    session: AsyncSession,
    *,
    team_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    default_max_storage_bytes: int,
    incoming_bytes: int = 0,
) -> None:
    """Reject if team TaskSet blob storage is at or would exceed the cap."""
    if incoming_bytes < 0:
        raise ValueError("incoming_bytes must be non-negative")
    quota_row = (
        await session.execute(
            select(TeamQuota).where(TeamQuota.team_id == team_id),
        )
    ).scalar_one_or_none()
    max_storage = (
        quota_row.taskset_max_storage_bytes
        if quota_row is not None and quota_row.taskset_max_storage_bytes is not None
        else default_max_storage_bytes
    )

    team_bytes = await asyncio.to_thread(
        team_taskset_storage_bytes,
        minio_client,
        bucket=artifacts_bucket,
        team_id=team_id,
    )
    if team_bytes + incoming_bytes > max_storage:
        raise HTTPException(
            status_code=429,
            detail="taskset_storage_quota_exceeded",
        )


async def submit_task_set(
    session: AsyncSession,
    *,
    team_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    manifest_upload: UploadFile,
    verifier_upload: UploadFile | None,
    transform_upload: UploadFile | None,
    bundle_upload: UploadFile | None = None,
    taskset_quota_max_storage_bytes: int = 21_474_836_480,
    manifest_max_bytes: int = 1_048_576,
    bundle_max_bytes: int = 5_368_709_120,
    before_commit: Callable[[TaskSetIntakeResult], Awaitable[None]] | None = None,
) -> TaskSetIntakeResult:
    manifest_model, raw_manifest = await parse_manifest_upload(
        manifest_upload,
        max_bytes=manifest_max_bytes,
    )
    slug = manifest_model.slug
    task_set_id = task_set_id_for(team_id=str(team_id), slug=slug)
    prefix = _storage_prefix(team_id=team_id, slug=slug)

    verifier_bytes: bytes | None = None
    verifier_key: str | None = None
    if manifest_model.verifier is not None:
        if verifier_upload is None:
            raise HTTPException(
                status_code=400,
                detail="verifier file required when manifest declares verifier",
            )
        _reject_unsafe_filename(verifier_upload.filename or manifest_model.verifier.file)
        verifier_bytes = await _read_upload(verifier_upload)
        verifier_key = bundle_object_key(
            prefix=prefix,
            relative_path=manifest_model.verifier.file,
        )

    transform_bytes: bytes | None = None
    transform_key: str | None = None
    if manifest_model.transform is not None:
        if transform_upload is None:
            raise HTTPException(
                status_code=400,
                detail="transform file required when manifest declares transform",
            )
        _reject_unsafe_filename(transform_upload.filename or manifest_model.transform.file)
        transform_bytes = await _read_upload(transform_upload)
        transform_key = bundle_object_key(
            prefix=prefix,
            relative_path=manifest_model.transform.file,
        )

    bundle_key: str | None = None
    bundle_file: _PreparedUpload | None = None
    if manifest_model.source.type == "bundle-upload":
        if bundle_upload is None:
            raise HTTPException(
                status_code=400,
                detail="bundle file required when manifest source is bundle-upload",
            )
        _reject_unsafe_filename(bundle_upload.filename or manifest_model.source.locator)
        bundle_key = bundle_object_key(
            prefix=prefix,
            relative_path=manifest_model.source.locator,
        )
        bundle_file = await _prepare_upload_with_size_cap(
            bundle_upload,
            max_bytes=bundle_max_bytes,
        )
    elif bundle_upload is not None:
        raise HTTPException(
            status_code=400,
            detail="bundle file is only allowed when manifest source is bundle-upload",
        )

    manifest_key = f"{prefix}/manifest.yaml"
    manifest_bytes = yaml.safe_dump(
        raw_manifest,
        sort_keys=False,
    ).encode("utf-8")

    incoming_bytes = len(manifest_bytes)
    if verifier_bytes is not None:
        incoming_bytes += len(verifier_bytes)
    if transform_bytes is not None:
        incoming_bytes += len(transform_bytes)
    if bundle_file is not None:
        incoming_bytes += bundle_file.size_bytes
    try:
        await check_taskset_storage_quota(
            session,
            team_id=team_id,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            default_max_storage_bytes=taskset_quota_max_storage_bytes,
            incoming_bytes=incoming_bytes,
        )
    except Exception:
        if bundle_file is not None:
            bundle_file.file.close()
        raise

    normalized = normalize_intents(
        manifest_model,
        verifier_file_present=verifier_bytes is not None,
    )

    manifest_blob_uri = _blob_uri(bucket=artifacts_bucket, key=manifest_key)
    verifier_blob_uri: str | None = None
    transform_blob_uri: str | None = None
    if verifier_bytes is not None and verifier_key is not None:
        verifier_blob_uri = _blob_uri(bucket=artifacts_bucket, key=verifier_key)
    if transform_bytes is not None and transform_key is not None:
        transform_blob_uri = _blob_uri(bucket=artifacts_bucket, key=transform_key)

    job = TaskSetMaterializationJob(
        task_set_id=task_set_id,
        owning_team_id=team_id,
        state="queued",
    )
    task_set = TaskSet(
        id=task_set_id,
        owning_team_id=team_id,
        slug=slug,
        display_name=manifest_model.metadata.display_name,
        purpose=manifest_model.metadata.purpose,
        expires_at=manifest_model.metadata.expires_at,
        hold=manifest_model.metadata.hold,
        status="materializing",
        intents=normalized.effective_intents,
        evaluation_ready=False,
        manifest_blob_uri=manifest_blob_uri,
        task_count=0,
    )
    manifest_row = TaskSetManifest(
        task_set_id=task_set_id,
        schema_version=_MANIFEST_SCHEMA_VERSION,
        manifest=raw_manifest,
        verifier_blob_uri=verifier_blob_uri,
        transform_blob_uri=transform_blob_uri,
    )
    session.add(task_set)
    session.add(manifest_row)
    session.add(job)
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="task_set slug already exists for this team",
        ) from exc

    try:
        _upload_object(
            minio_client,
            bucket=artifacts_bucket,
            key=manifest_key,
            body=manifest_bytes,
            content_type="application/x-yaml",
        )
        if verifier_bytes is not None and verifier_key is not None:
            _upload_object(
                minio_client,
                bucket=artifacts_bucket,
                key=verifier_key,
                body=verifier_bytes,
                content_type="application/octet-stream",
            )
        if transform_bytes is not None and transform_key is not None:
            _upload_object(
                minio_client,
                bucket=artifacts_bucket,
                key=transform_key,
                body=transform_bytes,
                content_type="text/x-python",
            )
        if bundle_key is not None and bundle_file is not None:
            _upload_prepared_object(
                minio_client,
                bucket=artifacts_bucket,
                key=bundle_key,
                upload=bundle_file,
                content_type="application/gzip",
            )
        result = TaskSetIntakeResult(
            task_set_id=task_set_id,
            status="materializing",
            intents=normalized.effective_intents,
            manifest_intents=normalized.manifest_intents,
            inferred_intents=normalized.inferred_intents,
            capabilities=normalized.capabilities,
            warnings=_warnings_to_dict(normalized.warnings),
            evaluation_ready=False,
            task_count=0,
            job_id=job.id,
        )
        if before_commit is not None:
            await before_commit(result)
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        if bundle_file is not None:
            bundle_file.file.close()
    await session.refresh(job)

    return result


async def get_visible_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
    for_update: bool = False,
) -> TaskSet:
    query = visible_task_sets(team_id=team_id).where(TaskSet.id == task_set_id)
    if for_update:
        query = query.with_for_update()
    row = (
        await session.execute(
            query,
        )
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="task_set not found")
    return row


async def get_latest_job(
    session: AsyncSession,
    task_set_id: str,
) -> TaskSetMaterializationJob | None:
    return (
        await session.execute(
            select(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.task_set_id == task_set_id)
            .order_by(TaskSetMaterializationJob.enqueued_at.desc())
            .limit(1),
        )
    ).scalar_one_or_none()


async def rebuild_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
) -> TaskSetIntakeResult:
    task_set = await get_visible_task_set(
        session,
        team_id=team_id,
        task_set_id=task_set_id,
        for_update=True,
    )
    if task_set.status == "deleted":
        raise HTTPException(status_code=404, detail="task_set not found")

    active = (
        await session.execute(
            select(TaskSetMaterializationJob).where(
                TaskSetMaterializationJob.task_set_id == task_set_id,
                TaskSetMaterializationJob.state.in_(_ACTIVE_JOB_STATES),
            ),
        )
    ).scalar_one_or_none()
    if active is not None:
        raise HTTPException(
            status_code=409,
            detail="materialization_already_active",
        )

    manifest_row = (
        await session.execute(
            select(TaskSetManifest).where(TaskSetManifest.task_set_id == task_set_id),
        )
    ).scalar_one_or_none()
    if manifest_row is None:
        raise HTTPException(status_code=404, detail="task_set manifest not found")

    try:
        manifest_model = UserTaskSetManifest.model_validate(manifest_row.manifest)
    except ValidationError as exc:
        raise HTTPException(status_code=500, detail="stored manifest invalid") from exc

    normalized = normalize_intents(
        manifest_model,
        verifier_file_present=manifest_row.verifier_blob_uri is not None,
    )

    has_published_rows = (
        await session.execute(
            select(Task.id).where(Task.task_set_id == task_set_id).limit(1),
        )
    ).scalar_one_or_none() is not None
    if not has_published_rows:
        task_set.status = "materializing"
        task_set.status_reason = None
    task_set.updated_at = datetime.now(UTC)
    job = TaskSetMaterializationJob(
        task_set_id=task_set_id,
        owning_team_id=task_set.owning_team_id,
        state="queued",
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="materialization_already_active",
        ) from exc
    await session.refresh(job)

    return TaskSetIntakeResult(
        task_set_id=task_set_id,
        status="materializing",
        intents=normalized.effective_intents,
        manifest_intents=normalized.manifest_intents,
        inferred_intents=normalized.inferred_intents,
        capabilities=normalized.capabilities,
        warnings=_warnings_to_dict(normalized.warnings),
        evaluation_ready=task_set.evaluation_ready,
        task_count=task_set.task_count,
        job_id=job.id,
    )


async def delete_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
) -> None:
    task_set = await get_visible_task_set(
        session,
        team_id=team_id,
        task_set_id=task_set_id,
        for_update=True,
    )
    active_jobs = (
        (
            await session.execute(
                select(TaskSetMaterializationJob)
                .where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                    TaskSetMaterializationJob.state.in_(_ACTIVE_JOB_STATES),
                )
                .with_for_update(),
            )
        )
        .scalars()
        .all()
    )
    now = datetime.now(UTC)
    task_set.status = "deleted"
    task_set.soft_deleted_at = now
    task_set.updated_at = now
    for job in active_jobs:
        job.state = "cancelled"
        # Cancellation explicitly revokes any materializer lease.  A worker
        # that resumes after deletion must fail its lease CAS instead of
        # publishing rows or a terminal status for this TaskSet.
        job.lease_epoch += 1
        job.claimed_at = None
        job.claimed_by = None
        job.lease_heartbeat_at = None
        job.finished_at = now
        job.updated_at = now
    await session.commit()


def intake_result_to_response(result: TaskSetIntakeResult) -> dict[str, Any]:
    return {
        "task_set_id": result.task_set_id,
        "status": result.status,
        "intents": result.intents,
        "manifest_intents": result.manifest_intents,
        "inferred_intents": result.inferred_intents,
        "capabilities": result.capabilities,
        "warnings": result.warnings,
        "evaluation_ready": result.evaluation_ready,
        "task_count": result.task_count,
        "materialization_job_id": str(result.job_id),
    }
