"""poll_local_http — fetch events from an HTTP endpoint inside the sandbox.

This helper supports adapters whose agent runs a local server inside the
sandbox. The worker cannot reach the container's loopback directly, so the
helper executes `curl` inside the sandbox and parses the output. No included
adapter currently selects this capture path.

**Sandbox requirement.** The sandbox image MUST have `curl` installed
on PATH. Any adapter using this capture mechanism MUST document the dependency
in its Dockerfile and adapter docstring. If curl is missing, `exec_oneshot`
returns rc=127 and the polling loop emits zero events (logged at
DEBUG, not an exception — silent failure is better than crashing the
trial for what is fundamentally an image-build issue).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from loom_launcher.adapter import ExecHandle, TrajectoryEventLike
from loom_launcher.capture.stdout_jsonl import _DictEvent

logger = logging.getLogger(__name__)


async def poll_local_http(
    handle: ExecHandle,
    *,
    port: int,
    path: str = "/events",
    poll_interval_sec: float = 0.5,
) -> AsyncIterator[TrajectoryEventLike]:
    """Yield events by repeatedly fetching `http://localhost:{port}{path}`
    inside the sandbox until `handle.wait()` resolves.

    Assumes the endpoint returns a JSON array of new events on each poll
    (the openhands `/events?since=N` pattern). We track the last seen
    index locally.
    """
    if handle.sandbox is None:
        raise RuntimeError(
            "poll_local_http requires ExecHandle.sandbox to be populated",
        )

    seen = 0

    async def _drain_once() -> AsyncIterator[TrajectoryEventLike]:
        nonlocal seen
        # Audit fix: if the caller's path already contains a query string
        # (e.g., path="/events?format=json"), append `since=N` with `&`
        # not `?`. Otherwise we'd produce `...?format=json?since=0` which
        # most servers misparse.
        sep = "&" if "?" in path else "?"
        url = f"http://localhost:{port}{path}{sep}since={seen}"
        rc, stdout = await handle.sandbox.exec_oneshot(  # type: ignore[union-attr]
            ["curl", "-fsS", url], timeout_sec=5.0,
        )
        if rc != 0:
            # Endpoint not ready yet (typical during the first ~1s); skip.
            return
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            logger.warning(
                "poll_local_http: %s returned non-JSON (%d bytes): %s",
                url, len(stdout), exc,
            )
            return
        if not isinstance(payload, list):
            logger.warning(
                "poll_local_http: %s returned %s, expected list",
                url, type(payload).__name__,
            )
            return
        seen += len(payload)
        for item in payload:
            if isinstance(item, dict):
                yield _DictEvent(item)

    async def discard_stdout() -> None:
        # Events come from HTTP; stdout still needs a bounded drain so verbose
        # agents cannot fill their output pipe and block before process exit.
        async for _ in handle.stdout:
            pass

    stdout_task = asyncio.create_task(discard_stdout())
    wait_task = asyncio.create_task(handle.wait())
    try:
        while not wait_task.done():
            async for event in _drain_once():
                yield event
            try:
                await asyncio.wait_for(
                    asyncio.shield(wait_task), timeout=poll_interval_sec,
                )
            except TimeoutError:
                pass
        # One final poll after the agent exits.
        async for event in _drain_once():
            yield event
        await wait_task
        await stdout_task
    finally:
        for task in (stdout_task, wait_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, wait_task, return_exceptions=True)
