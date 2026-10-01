"""Thin httpx-based forwarder used to proxy Control Plane writes.

The service layer doesn't reimplement trial submit / cancel — it
authenticates the caller, runs the local scope/team checks, then
proxies the request to the Control Plane with the caller's bearer
token intact. Submission and cancellation also forward the browser session and CSRF token;
the CP independently authenticates the caller and enforces its team scope.

`propagate` returns a `JSONResponse` so we can carry through the
upstream's status_code AND a small allowlist of response headers
(Retry-After, Location, X-RateLimit-*) — Plan 19's rate-limited
batch submits need Retry-After for client backoff to work.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

# Headers the service forwards verbatim from the upstream response.
# Allowlist (not blocklist) so we don't accidentally leak internal
# trace headers or set-cookie values from a misconfigured CP.
_PASSTHROUGH_HEADERS: frozenset[str] = frozenset({
    "retry-after",
    "location",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-idempotency-key",
})


async def forward(
    client: httpx.AsyncClient,
    *,
    method: str,
    path: str,
    authorization: str | None,
    json_body: Any | None = None,
    cancellation_request: Request | None = None,
    submission_request: Request | None = None,
    submission_id: UUID | None = None,
) -> httpx.Response:
    headers: dict[str, str] = {}
    caller_request = submission_request if submission_request is not None else cancellation_request
    if submission_id is not None:
        # Produced by the service's DB write, never copied from HTTP input.
        headers["X-Loom-Submission-ID"] = str(submission_id)
    if authorization:
        headers["Authorization"] = authorization
    elif caller_request is not None:
        # Explicit submission/cancellation hops forward browser credentials.
        # Normalize configurable
        # public names to the existing cookie/CSRF contract on the internal hop;
        # the CP independently verifies both and applies the caller's team scope.
        settings = caller_request.app.state.settings
        cookie = caller_request.cookies.get(settings.session_cookie_name)
        csrf = caller_request.headers.get(settings.auth_csrf_header_name)
        if cookie:
            headers["Cookie"] = f"loom_session={cookie}"
        if csrf:
            headers["X-Loom-CSRF"] = csrf
        if settings.session_audience is not None:
            # Assertion of lookup context, not a credential. Never forward a
            # caller-provided audience: this process already authenticated only
            # its own protected binding. CP still verifies cookie/CSRF/team.
            headers["X-Loom-Session-Audience"] = settings.session_audience.model_dump_json()
    try:
        resp = await client.request(
            method, path, headers=headers, json=json_body,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"upstream unreachable: {exc}",
        ) from exc
    return resp


def _allowed_headers(resp: httpx.Response) -> dict[str, str]:
    return {
        k: v
        for k, v in resp.headers.items()
        if k.lower() in _PASSTHROUGH_HEADERS
    }


def propagate(resp: httpx.Response) -> JSONResponse:
    """Mirror upstream status + allowlisted headers + JSON body.

    4xx/5xx responses are returned as JSONResponse with the upstream
    status code (NOT raised as HTTPException) so the allowlisted
    response headers (Retry-After, etc.) carry through. The body is
    the upstream JSON if parseable, else the raw text wrapped in
    {"detail": ...} for shape consistency.
    """
    if resp.content:
        try:
            body: Any = resp.json()
        except ValueError:
            body = {"detail": resp.text}
    else:
        body = {}
    return JSONResponse(
        status_code=resp.status_code,
        content=body,
        headers=_allowed_headers(resp),
    )
