"""User TaskSet intake API (#242 sub-plan 2, list in sub-plan 5).

Routes:
- POST   /api/v1/tasksets              — multipart submit (202)
- GET    /api/v1/tasksets              — team-scoped list
- GET    /api/v1/tasksets/{id}         — status + capabilities
- POST   /api/v1/tasksets/{id}/rebuild — re-enqueue materialization (202)
- DELETE /api/v1/tasksets/{id}         — soft-delete (204)

Cross-team access returns 404. Native ``/api/v1/benchmarks`` is unchanged.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from loom.auth import AuthContext
from loom.db.schema import Task, TaskSet, TaskSetManifest, TaskSetMaterializationJob
from loom.db.task_set_visibility import visible_task_sets
from loom.models.taskset import UserTaskSetManifest
from loom.taskset.intents import normalize_intents
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.taskset_intake import (
    delete_task_set,
    get_latest_job,
    get_visible_task_set,
    intake_result_to_response,
    rebuild_task_set,
    submit_task_set,
)

router = APIRouter()


class TaskSetWarning(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str


class TaskSetSubmitResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_set_id: str
    status: str
    intents: list[str]
    manifest_intents: list[str]
    inferred_intents: list[str]
    capabilities: list[str]
    warnings: list[TaskSetWarning] = Field(default_factory=list)
    evaluation_ready: bool
    task_count: int
    materialization_job_id: str


class TaskSetDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    display_name: str | None = None
    task_preview: list[str] = Field(default_factory=list)
    task_set_id: str
    status: str
    status_reason: str | None
    intents: list[str]
    manifest_intents: list[str]
    inferred_intents: list[str]
    capabilities: list[str]
    warnings: list[TaskSetWarning] = Field(default_factory=list)
    evaluation_ready: bool
    task_count: int
    error_summary: list[Any] = Field(default_factory=list)
    materialization_job_state: str | None = None
    materialization_fence: MaterializationFenceResponse | None = None


class MaterializationFenceResponse(BaseModel):
    """Secret-safe lease status for the most recent materialization job."""

    model_config = ConfigDict(extra="forbid")

    lease_epoch: int
    lease_heartbeat_at: datetime | None
    lease_heartbeat_state: Literal["fresh", "stale", "not_leased"]
    owner_fingerprint: str | None
    published_generation: int


def _owner_fingerprint(owner: str | None) -> str | None:
    if owner is None:
        return None
    digest = hashlib.sha256(owner.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest}"


def _materialization_fence(
    job: TaskSetMaterializationJob | None,
    *,
    claim_ttl_sec: int,
) -> MaterializationFenceResponse | None:
    if job is None:
        return None
    heartbeat_state = "not_leased"
    if (
        job.state in {"claimed", "running"}
        and job.claimed_by is not None
        and job.lease_heartbeat_at is not None
    ):
        cutoff = datetime.now(UTC) - timedelta(seconds=claim_ttl_sec)
        heartbeat_state = (
            "stale" if job.lease_heartbeat_at < cutoff else "fresh"
        )
    return MaterializationFenceResponse(
        lease_epoch=job.lease_epoch,
        lease_heartbeat_at=job.lease_heartbeat_at,
        lease_heartbeat_state=heartbeat_state,
        owner_fingerprint=_owner_fingerprint(job.claimed_by),
        published_generation=job.published_materialization_generation,
    )


class TaskSetListItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_set_id: str
    display_name: str
    status: str
    intents: list[str]
    evaluation_ready: bool
    task_count: int
    created_at: datetime


class TaskSetListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[TaskSetListItem] = Field(default_factory=list)


def _require_team_id(ctx: AuthContext) -> UUID:
    if ctx.team_id is None:
        raise HTTPException(
            status_code=403,
            detail="team token required for TaskSet mutations",
        )
    return ctx.team_id


def _team_id_for_read(ctx: AuthContext) -> UUID | None:
    if is_admin(ctx):
        return ctx.team_id
    return _require_team_id(ctx)


@router.post("/tasksets", status_code=202, response_model=TaskSetSubmitResponse)
async def create_task_set(
    request: Request,
    sc: SessionAndCtx,
    manifest: Annotated[UploadFile, File()],
    verifier: Annotated[UploadFile | None, File()] = None,
    transform: Annotated[UploadFile | None, File()] = None,
    bundle: Annotated[UploadFile | None, File()] = None,
) -> TaskSetSubmitResponse:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    team_id = _require_team_id(ctx)
    settings = request.app.state.settings
    result = await submit_task_set(
        session,
        team_id=team_id,
        minio_client=request.app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        manifest_upload=manifest,
        verifier_upload=verifier,
        transform_upload=transform,
        bundle_upload=bundle,
        taskset_quota_max_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        manifest_max_bytes=settings.taskset_manifest_max_bytes,
        bundle_max_bytes=settings.taskset_quota_max_bundle_bytes,
    )
    return TaskSetSubmitResponse.model_validate(intake_result_to_response(result))


@router.get("/tasksets", response_model=TaskSetListResponse)
async def list_task_sets(sc: SessionAndCtx) -> TaskSetListResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    team_id = _team_id_for_read(ctx)
    stmt = visible_task_sets(team_id=team_id).order_by(TaskSet.created_at.desc())
    rows = (await session.execute(stmt)).scalars().all()
    return TaskSetListResponse(
        items=[
            TaskSetListItem(
                task_set_id=row.id,
                display_name=row.display_name,
                status=row.status,
                intents=list(row.intents),
                evaluation_ready=row.evaluation_ready,
                task_count=row.task_count,
                created_at=row.created_at,
            )
            for row in rows
        ],
    )


@router.get("/tasksets/{task_set_id:path}", response_model=TaskSetDetailResponse)
async def get_task_set(
    task_set_id: str,
    request: Request,
    sc: SessionAndCtx,
) -> TaskSetDetailResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    team_id = _team_id_for_read(ctx)
    task_set = await get_visible_task_set(
        session, team_id=team_id, task_set_id=task_set_id,
    )
    job = await get_latest_job(session, task_set_id)

    manifest_db = (await session.execute(
        select(TaskSetManifest).where(TaskSetManifest.task_set_id == task_set_id),
    )).scalar_one_or_none()
    if manifest_db is None:
        raise HTTPException(status_code=404, detail="task_set manifest not found")

    manifest_model = UserTaskSetManifest.model_validate(manifest_db.manifest)
    normalized = normalize_intents(
        manifest_model,
        verifier_file_present=manifest_db.verifier_blob_uri is not None,
    )

    task_preview = (await session.scalars(
        select(Task.id).where(Task.task_set_id == task_set_id).order_by(Task.id).limit(5),
    )).all()
    return TaskSetDetailResponse(
        display_name=task_set.display_name,
        task_preview=list(task_preview),
        task_set_id=task_set.id,
        status=task_set.status,
        status_reason=task_set.status_reason,
        intents=task_set.intents,
        manifest_intents=normalized.manifest_intents,
        inferred_intents=normalized.inferred_intents,
        capabilities=normalized.capabilities,
        warnings=[
            {"code": w.code, "message": w.message} for w in normalized.warnings
        ],
        evaluation_ready=task_set.evaluation_ready,
        task_count=task_set.task_count,
        error_summary=list(job.error_summary) if job is not None else [],
        materialization_job_state=job.state if job is not None else None,
        materialization_fence=_materialization_fence(
            job,
            claim_ttl_sec=request.app.state.settings.taskset_materializer_claim_ttl_sec,
        ),
    )


@router.post(
    "/tasksets/{task_set_id:path}/rebuild",
    status_code=202,
    response_model=TaskSetSubmitResponse,
)
async def rebuild_task_set_route(
    task_set_id: str, sc: SessionAndCtx,
) -> TaskSetSubmitResponse:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    team_id = _team_id_for_read(ctx)
    result = await rebuild_task_set(
        session, team_id=team_id, task_set_id=task_set_id,
    )
    return TaskSetSubmitResponse.model_validate(intake_result_to_response(result))


@router.delete("/tasksets/{task_set_id:path}", status_code=204)
async def delete_task_set_route(task_set_id: str, sc: SessionAndCtx) -> None:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    team_id = _team_id_for_read(ctx)
    await delete_task_set(session, team_id=team_id, task_set_id=task_set_id)
