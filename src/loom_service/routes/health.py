"""GET /api/v1/health — unauthenticated liveness probe.

Used by the docker-compose healthcheck + k8s readinessProbe. Does NOT
hit the DB; authenticated `/health/ready` checks PostgreSQL and configured
object-store buckets. This endpoint only proves the FastAPI process is alive."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom_service import wire_responses as wire
from loom_service.build_info import read_build_revision, read_build_source, read_build_time
from loom_service.dependencies import authed_session
from loom_service.readiness import probe_api_dependencies, probe_dependencies

router = APIRouter()


async def _readiness_session(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> AsyncIterator[tuple[AsyncSession, AuthContext]]:
    """Retain authentication, but classify DB unavailability before the probe."""
    try:
        async with asynccontextmanager(authed_session)(request, authorization) as value:
            yield value
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail="readiness database unavailable") from None


@router.get("/health", response_model=wire.GetHealthResponse, response_model_exclude_unset=True)
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/version")
async def version(response: Response) -> dict[str, str | None]:
    """#2009: this responding instance's own immutable build revision.

    Unauthenticated, like `/health` — the frontend needs it before login,
    and it carries nothing sensitive. Reads locally available build
    metadata only (never GitHub, Kubernetes, or the database), so it stays
    cheap and never blocks on an unhealthy dependency. One response is
    evidence for this instance only, not proof every replica has rolled.
    """
    response.headers["Cache-Control"] = "no-store"
    return {
        "buildRevision": read_build_revision(),
        "buildTime": read_build_time(),
        **read_build_source(),
    }


@router.get("/health/ready")
async def dependency_readiness(
    request: Request,
    response: Response,
    sc: Annotated[tuple[AsyncSession, AuthContext], Depends(_readiness_session)],
) -> dict[str, object]:
    """Authenticated read-only PostgreSQL and object-store readiness.

    The dedicated rollout readonly principal may call this route.  It never
    returns provider exception text or credential material.
    """
    session, _ctx = sc
    settings = request.app.state.settings
    if settings.service_mode == "api_only":
        api_result = await probe_api_dependencies(
            session,
            minio_client=request.app.state.minio_client,
            buckets=(settings.artifacts_bucket, settings.trajectories_bucket),
        )
        if not api_result.ready:
            response.status_code = 503
        return api_result.to_dict()
    result = await probe_dependencies(
        session,
        minio_client=request.app.state.minio_client,
        buckets=(settings.artifacts_bucket, settings.trajectories_bucket),
        environment=os.environ.get("LOOM_ENV", "").strip().lower(),
        namespace=os.environ.get("LOOM_NAMESPACE", "").strip(),
    )
    if not result.ready:
        response.status_code = 503
    return result.to_dict()
