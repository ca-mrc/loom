"""Request-local enforcement of signed attempt wall-clock deadlines.

The JWT carries an absolute UTC timestamp so it is meaningful across
processes.  At the authenticated HTTP boundary we translate that timestamp
exactly once onto this process' monotonic clock.  All later provider work uses
the monotonic cutoff, avoiding wall-clock adjustments during a request.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Never, TypeVar

import httpx
from fastapi import HTTPException, Request

from loom.auth import AuthContext
from loom_llm_gateway.metrics import (
    ATTEMPT_DEADLINE_REACHED_TOTAL,
    LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL,
)

T = TypeVar("T")

ATTEMPT_DEADLINE_CODE = "agent_timeout"
_REQUEST_STATE_KEY = "loom_gateway_attempt_deadline"
_PROCESS_STARTED_MONOTONIC = time.monotonic()


class AttemptDeadlineReachedError(TimeoutError):
    """The authenticated attempt no longer has provider-dispatch time."""


class AttemptDeadlineRequiredError(PermissionError):
    """A pre-deadline token arrived after bounded rollout compatibility."""


class GatewayAttemptDeadline:
    """A request-scoped monotonic cutoff derived from a signed JWT claim."""

    __slots__ = ("_clock", "_metric_recorded", "_queue_credit", "monotonic_cutoff")

    def __init__(
        self,
        monotonic_cutoff: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.monotonic_cutoff = float(monotonic_cutoff)
        self._clock = clock
        self._metric_recorded = False
        self._queue_credit = 0.0

    def credit_queue_wait(self, waited_sec: float, *, max_pause_sec: float) -> None:
        """Move this request's cutoff forward by time it spent in the queue.

        The grant is the measured wait, capped at the pool pause ceiling.
        Calling again with a larger total adds only the difference.
        """

        grant = min(max(0.0, waited_sec), max(0.0, max_pause_sec))
        extra = grant - self._queue_credit
        if extra > 0:
            self.monotonic_cutoff += extra
            self._queue_credit = grant

    @classmethod
    def from_wall_clock(
        cls,
        wall_deadline: datetime,
        *,
        wall_now: datetime | None = None,
        monotonic_now: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> GatewayAttemptDeadline:
        if wall_deadline.tzinfo is None or wall_deadline.utcoffset() is None:
            raise ValueError("attempt deadline must be timezone-aware")
        observed_wall_now = wall_now or datetime.now(UTC)
        if observed_wall_now.tzinfo is None or observed_wall_now.utcoffset() is None:
            raise ValueError("wall clock observation must be timezone-aware")
        observed_monotonic = clock() if monotonic_now is None else monotonic_now
        remaining = (
            wall_deadline.astimezone(UTC) - observed_wall_now.astimezone(UTC)
        ).total_seconds()
        # Deliberately no JWT-expiry cleanup reserve: the attempt deadline is
        # the provider-dispatch boundary, while exp's +300s is only cleanup
        # authentication headroom.
        return cls(observed_monotonic + remaining, clock=clock)

    def remaining(self) -> float:
        return self.monotonic_cutoff - self._clock()

    @property
    def deadline_observed(self) -> bool:
        """A dispatch/read/retry boundary actually rejected this request.

        Do not infer a timeout from the wall clock during later audit writes.
        """
        return self._metric_recorded

    @property
    def reached(self) -> bool:
        return self.remaining() <= 0

    def require_remaining(self) -> float:
        remaining = self.remaining()
        if remaining <= 0:
            self._record_reached()
            raise AttemptDeadlineReachedError("signed attempt deadline reached")
        return remaining

    def cap_seconds(self, configured_seconds: float) -> float:
        return min(max(0.0, float(configured_seconds)), self.require_remaining())

    def httpx_timeout(self, configured_seconds: float) -> httpx.Timeout:
        """Cap every httpx phase, not only the aggregate request timeout."""
        cap = self.cap_seconds(configured_seconds)
        return httpx.Timeout(
            timeout=cap,
            connect=cap,
            read=cap,
            write=cap,
            pool=cap,
        )

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Bound one complete await and prefer the attempt timeout at races."""
        remaining = self.require_remaining()
        timeout = asyncio.timeout(remaining)
        try:
            async with timeout:
                result = await operation()
        except TimeoutError:
            if timeout.expired() or self.reached:
                self._record_reached()
                raise AttemptDeadlineReachedError("signed attempt deadline reached") from None
            raise
        except Exception:
            if self.reached:
                self._record_reached()
                raise AttemptDeadlineReachedError("signed attempt deadline reached") from None
            raise
        # A fake clock or an operation completing exactly at the boundary must
        # not let a provider result/error win the deadline race.
        self.require_remaining()
        return result

    async def sleep(
        self,
        delay: float,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        remaining = self.require_remaining()
        if delay >= remaining:
            self._record_reached()
            raise AttemptDeadlineReachedError("retry backoff would cross signed attempt deadline")
        await self.run(lambda: sleep(delay))

    async def anext(self, iterator: AsyncIterator[T]) -> T:
        """Bound every upstream stream read by the same request cutoff."""
        return await self.run(iterator.__anext__)

    def _record_reached(self) -> None:
        if not self._metric_recorded:
            ATTEMPT_DEADLINE_REACHED_TOTAL.inc()
            self._metric_recorded = True


def bind_request_attempt_deadline(
    request: Request,
    ctx: AuthContext,
    *,
    wall_now: datetime | None = None,
    monotonic_now: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> GatewayAttemptDeadline | None:
    """Translate an authenticated signed deadline once for this request."""
    try:
        bound = getattr(request.state, _REQUEST_STATE_KEY)
    except AttributeError:
        bound = None
    else:
        if bound is False:
            return None
        assert isinstance(bound, GatewayAttemptDeadline)
        bound.require_remaining()
        return bound

    if ctx.attempt_deadline_wall_clock is None:
        compat_seconds = float(
            getattr(
                request.app.state.settings,
                "legacy_attempt_deadline_compat_sec",
                0.0,
            )
        )
        compat_elapsed = max(0.0, clock() - _PROCESS_STARTED_MONOTONIC)
        if compat_seconds <= compat_elapsed:
            LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL.labels(outcome="rejected").inc()
            raise AttemptDeadlineRequiredError(
                "signed attempt deadline is required by gateway policy"
            )
        LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL.labels(outcome="accepted").inc()
        setattr(request.state, _REQUEST_STATE_KEY, False)
        return None
    deadline = GatewayAttemptDeadline.from_wall_clock(
        ctx.attempt_deadline_wall_clock,
        wall_now=wall_now,
        monotonic_now=monotonic_now,
        clock=clock,
    )
    setattr(request.state, _REQUEST_STATE_KEY, deadline)
    deadline.require_remaining()
    return deadline


def request_attempt_deadline(request: Request) -> GatewayAttemptDeadline | None:
    try:
        bound = getattr(request.state, _REQUEST_STATE_KEY)
    except AttributeError:
        return None
    return bound if isinstance(bound, GatewayAttemptDeadline) else None


def enforce_request_attempt_deadline(request: Request) -> None:
    deadline = request_attempt_deadline(request)
    if deadline is None:
        return
    try:
        deadline.require_remaining()
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)


def upstream_timeout(request: Request, configured_seconds: float) -> float | httpx.Timeout:
    deadline = request_attempt_deadline(request)
    if deadline is None:
        return configured_seconds
    return deadline.httpx_timeout(configured_seconds)


def deadline_http_exception() -> HTTPException:
    return HTTPException(
        status_code=504,
        detail={
            "code": ATTEMPT_DEADLINE_CODE,
            "reason": "attempt_deadline_reached",
        },
    )


def raise_deadline_http_exception(exc: AttemptDeadlineReachedError) -> Never:
    raise deadline_http_exception() from exc
