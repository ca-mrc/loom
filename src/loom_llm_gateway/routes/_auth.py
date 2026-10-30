"""Shared HTTP bearer boundary for every user-facing LLM dialect."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, Request
from sqlalchemy import select

from loom.auth import AuthContext, validate_bearer_token
from loom.db.schema import ProviderRequestPool
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    AttemptDeadlineRequiredError,
    bind_request_attempt_deadline,
    raise_deadline_http_exception,
)
from loom_llm_gateway.metrics import AUTH_REJECTIONS_TOTAL

logger = logging.getLogger(__name__)

_LLM_CALL_SCOPE = "llm:call"
_INVALID_BEARER_DETAIL = {"code": "invalid_or_expired_bearer"}
_INVALID_BEARER_HEADERS = {
    "WWW-Authenticate": 'Bearer error="invalid_token"',
}


async def require_llm_call_bearer(
    session: Any,
    authorization: str | None,
    *,
    signing_key: str,
    request: Request | None = None,
) -> AuthContext:
    """Authenticate a Gateway LLM call with one dialect-neutral contract.

    The externally visible 401 intentionally does not distinguish a missing,
    malformed, invalid, revoked, or expired credential. The internal log field
    contains only the validator's bounded reason vocabulary and never includes
    the Authorization header or token.
    """
    result = await validate_bearer_token(
        session,
        authorization,
        signing_key=signing_key,
    )
    if result.context is None:
        AUTH_REJECTIONS_TOTAL.labels(reason=result.reason).inc()
        logger.info(
            "gateway bearer authentication rejected",
            extra={"bearer_validation_reason": result.reason},
        )
        raise HTTPException(
            status_code=401,
            detail=_INVALID_BEARER_DETAIL,
            headers=_INVALID_BEARER_HEADERS,
        )
    if _LLM_CALL_SCOPE not in result.context.scopes:
        AUTH_REJECTIONS_TOTAL.labels(reason="missing_scope").inc()
        logger.info(
            "gateway bearer authorization rejected",
            extra={"bearer_validation_reason": "missing_scope"},
        )
        raise HTTPException(
            status_code=403,
            detail={
                "code": "missing_scope",
                "required_scope": _LLM_CALL_SCOPE,
            },
        )
    if request is not None:
        # A bound logical pool must not silently use a platform credential on
        # another dialect. Caller headers cannot opt unsupported routes in.
        if (result.context.provider_connection_id is not None
            and request.scope.get("path") != "/openai/v1/chat/completions"):
            pool_enabled = await session.scalar(select(ProviderRequestPool.enabled).where(
                ProviderRequestPool.connection_id == result.context.provider_connection_id,
            ))
            if pool_enabled:
                raise HTTPException(
                    400, "request pool requires hosted Terminus2 non-streaming complete-messages Chat",
                )
        try:
            bind_request_attempt_deadline(request, result.context)
        except AttemptDeadlineRequiredError:
            raise HTTPException(
                status_code=401,
                detail=_INVALID_BEARER_DETAIL,
                headers=_INVALID_BEARER_HEADERS,
            ) from None
        except AttemptDeadlineReachedError as exc:
            raise_deadline_http_exception(exc)
        # Internal verified principal only; never derive audit identity from
        # model payloads or forward these fields to a provider.
        request.scope["_loom_dispatch_auth"] = result.context
    return result.context
