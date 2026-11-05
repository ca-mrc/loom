"""Monotonic absolute deadlines shared by one agent attempt."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID


class AttemptDeadlineExceededError(TimeoutError):
    """The owning agent attempt can no longer start or finish work."""


@dataclass(frozen=True, slots=True)
class AttemptDeadline:
    """An immutable absolute deadline measured on a monotonic clock.

    The clock is injectable so boundary behavior can be tested without using
    wall-clock time. The optional UUID identifies an internal agent attempt;
    clocks and the safe grant observer are observation mechanisms, not identity.
    """

    monotonic_deadline: float
    wall_deadline_epoch_sec: float | None = None
    _clock: Callable[[], float] = field(
        default=time.monotonic,
        repr=False,
        compare=False,
    )
    _wall_clock: Callable[[], float] = field(
        default=time.time,
        repr=False,
        compare=False,
    )
    agent_attempt_id: UUID | None = None
    _grant_observer: Callable[[UUID, UUID], Awaitable[None]] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not math.isfinite(self.monotonic_deadline):
            raise ValueError("attempt deadline must be finite")
        if self.wall_deadline_epoch_sec is None:
            object.__setattr__(
                self,
                "wall_deadline_epoch_sec",
                self._wall_clock() + max(0.0, self.monotonic_deadline - self._clock()),
            )
        elif not math.isfinite(self.wall_deadline_epoch_sec):
            raise ValueError("attempt wall deadline must be finite")

    @classmethod
    def after(
        cls,
        timeout_sec: float,
        *,
        clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], float] = time.time,
        agent_attempt_id: UUID | None = None,
        grant_observer: Callable[[UUID, UUID], Awaitable[None]] | None = None,
    ) -> AttemptDeadline:
        """Create a deadline ``timeout_sec`` from one monotonic observation.

        Async callers use the running event loop's clock. Synchronous callers
        fall back to :func:`time.monotonic`; tests may inject either clock.
        """

        if not math.isfinite(timeout_sec) or timeout_sec < 0:
            raise ValueError("attempt timeout must be finite and non-negative")
        if clock is None:
            try:
                clock = asyncio.get_running_loop().time
            except RuntimeError:
                clock = time.monotonic
        return cls(
            monotonic_deadline=clock() + timeout_sec,
            wall_deadline_epoch_sec=wall_clock() + timeout_sec,
            _clock=clock,
            _wall_clock=wall_clock,
            agent_attempt_id=agent_attempt_id,
            _grant_observer=grant_observer,
        )

    async def record_step_token_grant(
        self,
        *,
        agent_attempt_id: UUID | None,
        step_jwt_id: UUID | None,
    ) -> None:
        """Record safe CP-returned identity before any agent provider dispatch."""
        if self.agent_attempt_id is None:
            return  # Legacy callers have no strong attempt join.
        if agent_attempt_id != self.agent_attempt_id or step_jwt_id is None:
            raise ValueError("step-token grant does not match the supervised agent attempt")
        self.require_remaining()
        if self._grant_observer is not None:
            await self._grant_observer(self.agent_attempt_id, step_jwt_id)

    @classmethod
    def from_wall_deadline(
        cls,
        wall_deadline_epoch_sec: float,
        *,
        clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], float] = time.time,
    ) -> AttemptDeadline:
        """Translate a signed cross-process wall deadline into a local cutoff.

        Monotonic values are process-local and must never cross the token
        boundary.  The signed epoch claim is observed once on admission and
        converted to this process' monotonic clock without adding a skew or
        cleanup allowance.
        """

        if not math.isfinite(wall_deadline_epoch_sec):
            raise ValueError("attempt wall deadline must be finite")
        if clock is None:
            try:
                clock = asyncio.get_running_loop().time
            except RuntimeError:
                clock = time.monotonic
        remaining = max(0.0, wall_deadline_epoch_sec - wall_clock())
        return cls(
            monotonic_deadline=clock() + remaining,
            wall_deadline_epoch_sec=wall_deadline_epoch_sec,
            _clock=clock,
            _wall_clock=wall_clock,
        )

    def extend_to(self, wall_epoch_sec: float) -> bool:
        """Move this deadline later. An earlier value leaves it unchanged."""

        current = self.wall_deadline_epoch_sec
        if current is None or not math.isfinite(wall_epoch_sec) or wall_epoch_sec <= current:
            return False
        object.__setattr__(
            self,
            "monotonic_deadline",
            self.monotonic_deadline + (wall_epoch_sec - current),
        )
        object.__setattr__(self, "wall_deadline_epoch_sec", wall_epoch_sec)
        return True

    def remaining(self) -> float:
        """Return the non-negative duration remaining on this attempt."""

        return max(0.0, self.monotonic_deadline - self._clock())

    def require_remaining(self) -> float:
        """Return remaining time or fail before downstream work can start."""

        remaining = self.remaining()
        if remaining <= 0:
            raise AttemptDeadlineExceededError("agent attempt deadline has been reached")
        return remaining

    @property
    def reached(self) -> bool:
        return self.remaining() <= 0

    @property
    def wall_deadline(self) -> datetime:
        """Return the signed cross-process boundary as an aware UTC time."""

        assert self.wall_deadline_epoch_sec is not None
        return datetime.fromtimestamp(self.wall_deadline_epoch_sec, tz=UTC)
