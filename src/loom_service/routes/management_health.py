"""Secret-free management probes, independent of every child environment."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request, Response
from sqlalchemy import text

from loom_service import wire_responses as wire

router = APIRouter()


@router.get("/health", response_model=wire.GetHealthResponse, response_model_exclude_unset=True)
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request, response: Response) -> dict[str, str]:
    """Report only this process's database availability, never child readiness.

    Unauthenticated for Kubernetes probes. No credentials, environment identities
    or database exception text may be included in the response.
    """
    ready = False
    try:
        async with asyncio.timeout(3):
            async with request.app.state.session_factory() as session:
                ready = (await session.execute(text("SELECT 1"))).scalar_one() == 1
    except Exception:
        pass
    runtime = getattr(request.app.state, "environment_runtime", None)
    application = getattr(request.app.state, "application_runtime", None)
    runtime_ready = (runtime is None or runtime.ready) and (application is None or application.ready)
    if not ready or not runtime_ready:
        response.status_code = 503
    result = {
        "status": "ready" if ready and runtime_ready else "not-ready",
        "mode": "management",
        "postgres": "ready" if ready else "not-ready",
    }
    if runtime is not None:
        result["provisioner"] = "ready" if runtime.ready else "not-ready"
    if application is not None:
        result["application_provisioner"] = "ready" if application.ready else "not-ready"
    return result
