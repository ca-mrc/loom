"""Transient-failure retry for upstream LLM requests (#298 Slice A).

Wraps a per-request `await client.post(...)` (or any awaitable
returning `httpx.Response`) with exponential-backoff retry on
retryable failures. Settings come from `GatewaySettings.llm_retry_*`.

Why gateway-side (not worker-side): subprocess agents (aider,
claude-code, qwen-cli, etc.) hit the gateway directly via
`OPENAI_API_BASE`. Worker-side retry only catches the in-process
direct-completion runtime. The gateway is the universal interception point.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

import httpx

from loom.errors import is_retryable
from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline

if TYPE_CHECKING:
    from loom_llm_gateway.config import GatewaySettings
    from loom_llm_gateway.dispatch_audit import DispatchAudit

from loom_llm_gateway.metrics import (
    RETRY_AMBIGUOUS_504_TOTAL,
    RETRY_ATTEMPTS,
    RETRY_TOTAL,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryOutcome:
    """The result of one `send_with_retry` call.

    `response` is the final httpx.Response (the caller still does the
    `if status >= 400: raise HTTPException(...)` check). `attempt` is
    the 1-indexed attempt number that produced this response — passed
    to `record_call(attempt=...)` so the llm_calls row reflects how
    many retries were needed (#298 Slice B).
    """

    response: httpx.Response
    attempt: int


async def send_with_retry(
    send: Callable[[], Awaitable[httpx.Response]],
    *,
    settings: GatewaySettings,
    dialect: str,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now: Callable[[], float] = time.monotonic,
    deadline: GatewayAttemptDeadline | None = None,
    dispatch_audit: DispatchAudit | None = None,
    response_retryable: Callable[[httpx.Response], bool] | None = None,
    respect_retry_after: bool = False,
) -> RetryOutcome:
    """Run `send()` with retry on transient failures.

    Returns a `RetryOutcome(response, attempt)` carrying both the
    final response (which may itself be a 4xx/5xx if exhausted — the
    caller's downstream `if status >= 400: raise` handles that as it
    does today) AND the 1-indexed attempt that produced it (#298
    Slice B; callers thread this into `record_call(attempt=...)` so
    the llm_calls row captures retry depth).

    Raises `httpx` transport errors only when every attempt raised
    and retry was exhausted; the caller's try/except shape sees the
    same exception as today.

    `sleep` + `now` are injected for testability.
    """
    max_attempts = max(1, int(settings.llm_retry_max_attempts))
    base_backoff = max(0.0, float(settings.llm_retry_base_backoff_sec))
    jitter = max(0.0, float(settings.llm_retry_jitter_sec))
    max_backoff = max(0.0, float(settings.llm_retry_max_backoff_sec))
    budget = max(0.0, float(settings.llm_retry_budget_sec))

    started = now()
    last_response: httpx.Response | None = None
    last_exc: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        last_exc = None
        try:
            if dispatch_audit is not None:
                response = await dispatch_audit.send(
                    send,
                    deadline=deadline,
                    attempt=attempt,
                )
            else:
                response = await send() if deadline is None else await deadline.run(send)
        except httpx.HTTPError as exc:
            last_exc = exc
            if not is_retryable(exc) or attempt >= max_attempts:
                _record_exhaustion(dialect, attempt, status=0)
                raise
            wait_for = _next_backoff(
                attempt=attempt,
                base=base_backoff,
                jitter=jitter,
                cap=max_backoff,
            )
            if deadline is not None and wait_for >= deadline.require_remaining():
                await deadline.sleep(wait_for, sleep=sleep)
            if (now() - started) + wait_for > budget:
                _record_budget_exceeded(dialect, attempt, status=0)
                raise
            logger.info(
                "gateway_retry dialect=%s attempt=%d transport_error=%r wait=%.2fs",
                dialect,
                attempt,
                exc,
                wait_for,
            )
            if deadline is None:
                await sleep(wait_for)
            else:
                await deadline.sleep(wait_for, sleep=sleep)
            continue

        last_response = response
        status = response.status_code

        # Successful (2xx/3xx) or non-retryable status (deterministic
        # 4xx like 401/403/404). Return immediately — let the caller's
        # existing `if status >= 400` raise an HTTPException.
        if status < 400 or not (
            response_retryable(response) if response_retryable else _status_retryable(status)
        ):
            RETRY_ATTEMPTS.labels(dialect=dialect).observe(attempt)
            outcome = "success" if status < 400 else "non_retryable_error"
            RETRY_TOTAL.labels(
                dialect=dialect,
                outcome=outcome,
                status=str(status),
            ).inc()
            return RetryOutcome(response=response, attempt=attempt)

        # Retryable status (5xx / 408 / 429). Decide attempt vs give-up.
        if status == 504:
            RETRY_AMBIGUOUS_504_TOTAL.labels(dialect=dialect).inc()

        if attempt >= max_attempts:
            _record_exhaustion(dialect, attempt, status)
            # Caller's `if status >= 400` will raise. record_call won't
            # be invoked because the route bails before reaching it.
            return RetryOutcome(response=response, attempt=attempt)

        wait_for = _next_backoff(
            attempt=attempt,
            base=base_backoff,
            jitter=jitter,
            cap=max_backoff,
        )
        if respect_retry_after:
            wait_for = max(wait_for, retry_after_seconds(response))
        if deadline is not None and wait_for >= deadline.require_remaining():
            await deadline.sleep(wait_for, sleep=sleep)
        if (now() - started) + wait_for > budget:
            _record_budget_exceeded(dialect, attempt, status)
            return RetryOutcome(response=response, attempt=attempt)

        logger.info(
            "gateway_retry dialect=%s attempt=%d status=%d wait=%.2fs",
            dialect,
            attempt,
            status,
            wait_for,
        )
        if deadline is None:
            await sleep(wait_for)
        else:
            await deadline.sleep(wait_for, sleep=sleep)

    # Loop exit without return: shouldn't happen given the bounds, but
    # defensive — re-raise the last exception or return the last response.
    if last_exc is not None:  # pragma: no cover
        raise last_exc
    assert last_response is not None  # nosec - last_response set on every iter
    return RetryOutcome(response=last_response, attempt=max_attempts)


def retry_after_seconds(response: httpx.Response) -> float:
    """A server delay is a minimum, including when it exhausts the retry budget."""
    raw = response.headers.get("Retry-After", "")
    try:
        seconds = float(raw)
        return max(0.0, seconds) if math.isfinite(seconds) else 0.0
    except ValueError:
        try:
            date = parsedate_to_datetime(raw)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            return max(0.0, (date - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def _status_retryable(status: int) -> bool:
    # Mirrors loom.errors._RETRYABLE_HTTP_STATUSES — kept inline so we
    # don't fabricate a fake httpx.HTTPStatusError just to ask.
    return status in {408, 429, 500, 502, 503, 504}


def _next_backoff(
    *,
    attempt: int,
    base: float,
    jitter: float,
    cap: float,
) -> float:
    raw: float = base * (2 ** (attempt - 1))
    if jitter > 0:
        raw += random.uniform(-jitter, jitter)
    return float(max(0.0, min(raw, cap)))


def _record_exhaustion(dialect: str, attempt: int, status: int) -> None:
    RETRY_ATTEMPTS.labels(dialect=dialect).observe(attempt)
    RETRY_TOTAL.labels(
        dialect=dialect,
        outcome="exhausted",
        status=str(status),
    ).inc()


def _record_budget_exceeded(dialect: str, attempt: int, status: int) -> None:
    RETRY_ATTEMPTS.labels(dialect=dialect).observe(attempt)
    RETRY_TOTAL.labels(
        dialect=dialect,
        outcome="budget_exceeded",
        status=str(status),
    ).inc()
