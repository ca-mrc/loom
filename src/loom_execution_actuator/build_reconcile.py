"""Keep one build reconcile in flight across admission passes (#2754).

A pass used to await its full reconcile before the next pass could admit, so one
slow unrelated build delayed every later admission opportunity. The runner
starts a reconcile only when none is running, waits at most the grace for a new
one, and reports a finished reconcile in whichever pass collects it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

# A reconcile still running after this long fails the pass that notices it, so
# the build loop's readiness still reports a stuck reconcile.
RECONCILE_STALL_SECONDS = 300.0


class ReconcileRunner:
    def __init__(self, *, grace_seconds: float, stall_seconds: float = RECONCILE_STALL_SECONDS) -> None:
        self.grace_seconds, self.stall_seconds = grace_seconds, stall_seconds
        self._task: asyncio.Task[bool] | None = None
        self._started = 0.0
        self._seconds = 0.0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def advance(self, start: Callable[[], Awaitable[bool]]) -> None:
        """Start a reconcile unless one is in flight; wait up to the grace only for a new one."""
        if self._task is not None:
            return

        async def timed() -> bool:
            try:
                return await start()
            finally:
                self._seconds = time.monotonic() - self._started

        self._started = time.monotonic()
        self._task = asyncio.create_task(timed())
        await asyncio.wait({self._task}, timeout=self.grace_seconds)

    def collect(self) -> tuple[float | None, bool | BaseException | None]:
        """Return (seconds, result or error) of a finished reconcile, or (None, None)."""
        task = self._task
        if task is None or not task.done():
            return None, None
        self._task = None
        if task.cancelled():
            return self._seconds, asyncio.CancelledError()
        return self._seconds, task.exception() or task.result()

    def stalled(self) -> bool:
        return self.running and time.monotonic() - self._started > self.stall_seconds

    async def close(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
