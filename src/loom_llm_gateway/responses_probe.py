"""Runtime probe for OpenAI-Responses-API support at a provider connection's
upstream. Used by `routes/responses.py` to decide whether to dispatch a
codex-shaped request through the native `POST /v1/responses` path or
through the existing `responses_chat_compat` translator.

Availability is not capability: a 5xx / timeout / connect-error says the
upstream is unwell right now, not that it lacks Responses, so it is reported as
transient and never cached. The caller still sends that request through the
shim so codex sees a real translated response instead of hanging on a 40-min
agent-timeout window, and waits out `ProbeBackoff` before probing again.

The probe uses a *minimal but complete* Responses payload —
`{"model": "<sentinel>", "input": "loom-probe", "max_output_tokens": 1}` —
so it exercises the full upstream routing path rather than short-circuiting
on early validation. Empty-body probes were tried first and rejected
because BYO providers that reverse-proxy /v1/responses (yibuapi) will
happily 400 an empty body from their edge without ever attempting to
route — the response looks like a real Responses handler, but the
first live request hits an upstream timeout. A sentinel model name
that isn't a real provider model forces the proxy to try to route,
which is what actually matters.

Reference: docs/architecture/responses-api.md
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

import httpx

from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline

if TYPE_CHECKING:
    from loom_llm_gateway.dispatch_audit import DispatchAudit

logger = logging.getLogger(__name__)

_PROBE_TIMEOUT_SEC = 5.0
_TRANSIENT_PROBE_BACKOFF_SEC = 60.0

_SUPPORTED_STATUSES = frozenset({200, 400, 401})
_UNSUPPORTED_STATUSES = frozenset({404, 501})

# Sentinel model id in the probe body. Deliberately chosen to be one that
# no real provider ships. Real Responses handlers return 400
# ("model not found") quickly; reverse-proxied endpoints that only
# forward valid requests will 4xx or 5xx depending on their edge policy,
# which is the routing signal we care about.
_PROBE_MODEL_SENTINEL = "loom-probe-nonexistent-model"


@dataclass(frozen=True)
class ProbeOutcome:
    """Result of one probe attempt.

    - `supported = True`: upstream implements Responses; route native.
    - `supported = False`: upstream does not implement Responses; dispatch
       through the existing Chat-completions translator.
    - `supported = None`: probe was inconclusive (ambiguous 4xx that
      wasn't 400/401/404). Caller should keep the previously-cached
      value if any, or fall back to the existing per-request behaviour.
    - `transient = True` (with `supported = None`): the upstream was
      unavailable, so the probe learned nothing about Responses. Callers
      must not cache it.
    """

    supported: bool | None
    error_detail: str | None
    transient: bool = False


class ProbeBackoff:
    """Pauses probing a connection after a transient failure.

    In-process: each replica probes a struggling upstream at most once per
    window, and a restart only costs one more probe. Entries are bounded by
    the number of connections and removed on expiry or a conclusive probe.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._until: dict[UUID, float] = {}

    def active(self, connection_id: UUID) -> bool:
        until = self._until.get(connection_id)
        if until is None:
            return False
        if self._clock() < until:
            return True
        del self._until[connection_id]
        return False

    def record(self, connection_id: UUID) -> None:
        self._until[connection_id] = self._clock() + _TRANSIENT_PROBE_BACKOFF_SEC

    def clear(self, connection_id: UUID) -> None:
        self._until.pop(connection_id, None)


def classify_probe_status(status_code: int) -> bool | None:
    """Pure classifier — see ProbeOutcome for the tri-state semantics."""
    if status_code in _SUPPORTED_STATUSES:
        return True
    if status_code in _UNSUPPORTED_STATUSES:
        return False
    # Other 4xx (403, 429, 422, …): endpoint is reachable but the
    # response neither confirms nor denies Responses-API semantics.
    # 5xx (other than 501) says nothing about Responses either; `_run_probe`
    # reports those as transient.
    return None


async def probe_responses_api(
    *,
    upstream_url: str,
    api_key: str,
    client: httpx.AsyncClient | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    deadline: GatewayAttemptDeadline | None = None,
    dispatch_audit: DispatchAudit | None = None,
) -> ProbeOutcome:
    """Fire one probe request.

    Callers in production pass the same `httpx.AsyncClient` they use for
    real upstream traffic (via the egress client pool) so the probe
    honours the connection's egress proxy, DNS resolution set, and
    connection reuse. Test callers pass `transport=httpx.MockTransport(...)`
    to intercept probe traffic alongside their upstream mocks.
    """
    if client is None:
        async with httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC,
            transport=transport,
        ) as owned_client:
            return await _run_probe(
                owned_client,
                upstream_url,
                api_key,
                deadline=deadline,
                dispatch_audit=dispatch_audit,
            )
    return await _run_probe(
        client,
        upstream_url,
        api_key,
        deadline=deadline,
        dispatch_audit=dispatch_audit,
    )


async def _run_probe(
    client: httpx.AsyncClient,
    upstream_url: str,
    api_key: str,
    *,
    deadline: GatewayAttemptDeadline | None = None,
    dispatch_audit: DispatchAudit | None = None,
) -> ProbeOutcome:
    try:
        timeout: float | httpx.Timeout = _PROBE_TIMEOUT_SEC
        if deadline is not None:
            timeout = deadline.httpx_timeout(_PROBE_TIMEOUT_SEC)

        async def _post() -> httpx.Response:
            return await client.post(
                upstream_url,
                json={
                    "model": _PROBE_MODEL_SENTINEL,
                    "input": "loom-probe",
                    "max_output_tokens": 1,
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=timeout,
            )

        if dispatch_audit is not None:
            response = await dispatch_audit.send(_post, deadline=deadline)
        else:
            response = await _post() if deadline is None else await deadline.run(_post)
    except (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
        httpx.TransportError,
    ) as exc:
        reason = type(exc).__name__
        logger.info(
            "responses_probe transport error url=%s err=%s",
            upstream_url,
            reason,
        )
        return ProbeOutcome(
            supported=None,
            error_detail=f"transport_{reason.lower()}",
            transient=True,
        )
    verdict = classify_probe_status(response.status_code)
    if verdict is True:
        return ProbeOutcome(supported=True, error_detail=None)
    if verdict is False:
        return ProbeOutcome(
            supported=False,
            error_detail=f"upstream_{response.status_code}",
        )
    if response.status_code >= 500:
        return ProbeOutcome(
            supported=None,
            error_detail=f"upstream_{response.status_code}",
            transient=True,
        )
    # Ambiguous 4xx.
    return ProbeOutcome(
        supported=None,
        error_detail=f"ambiguous_{response.status_code}",
    )
