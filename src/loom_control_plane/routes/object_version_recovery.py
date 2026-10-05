"""Administrative preview/apply boundary for exact historical version metadata."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from loom.auth import verify_bearer_token
from loom_control_plane.object_version_recovery import (
    ObjectVersionRecovery,
    RecoveryConflictError,
    RecoveryRequest,
)

router = APIRouter(prefix="/admin")


@router.post("/object-version-recovery")
async def recover_object_versions(
    payload: RecoveryRequest, request: Request, authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    sessions = request.app.state.session_factory
    async with sessions() as session:
        auth = await verify_bearer_token(session, authorization,
            admin_verifier=request.app.state.admin_secret_verifier)
        if auth is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        if auth.type != "admin":
            raise HTTPException(status_code=403, detail="Singleton administrator credential required")
    settings = request.app.state.settings
    recovery: ObjectVersionRecovery = request.app.state.object_version_recovery
    try:
        return await recovery.recover(
            payload, sessions=sessions, client=request.app.state.minio_client,
            actor=f"admin:{auth.type}:{auth.token_hash.hex()[:16]}",
            artifacts_bucket=settings.artifacts_bucket, trajectories_bucket=settings.trajectories_bucket,
        )
    except RecoveryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except TimeoutError:
        raise HTTPException(status_code=409, detail="verification_timeout") from None
    except SQLAlchemyError:
        raise HTTPException(status_code=409, detail="database_recovery_conflict") from None
