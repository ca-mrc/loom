"""tail_log_file — poll a file inside the sandbox, yield new lines as events.

Used by adapters whose agent writes to disk instead of stdout (aider
streams chat history to `.aider.chat.history.md`; swe-agent writes
`trajectory.jsonl`). The polling cadence is conservative (500 ms) to
balance latency against `sandbox.read_text` overhead.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from pathlib import PurePosixPath
from typing import Any

from loom_launcher.adapter import ExecHandle, TrajectoryEventLike
from loom_launcher.capture.stdout_jsonl import _DictEvent

logger = logging.getLogger(__name__)


async def tail_log_file(
    handle: ExecHandle,
    *,
    path: PurePosixPath,
    poll_interval_sec: float = 0.5,
    line_to_event: Callable[[str], dict[str, Any] | None] | None = None,
) -> AsyncIterator[TrajectoryEventLike]:
    """Yield events as new lines appear at `path` inside the sandbox.

    Continues until `handle.wait()` resolves and one final read returns
    no new bytes. Tracks file offset locally; relies on the agent never
    truncating the log mid-run (true for all log-file adapters at v1).

    `line_to_event`: optional callable `(str) -> dict | None`. If None,
    each non-empty line becomes a `_DictEvent({"line": line})`.
    """
    if handle.sandbox is None:
        raise RuntimeError(
            "tail_log_file requires ExecHandle.sandbox to be populated "
            "(the worker's SubprocessAgent wires this through; sandbox "
            "is None when only the stdout path is needed)",
        )

    offset = 0

    async def _drain_once(final: bool) -> AsyncIterator[TrajectoryEventLike]:
        nonlocal offset
        try:
            content = await handle.sandbox.read_text(path)  # type: ignore[union-attr]
        except FileNotFoundError:
            # The agent may create the log lazily; absence early on isn't fatal.
            return
        new = content[offset:]
        if not new:
            return
        offset = len(content)
        # Don't yield a partial trailing line until process exits.
        if not final and not new.endswith("\n"):
            tail_idx = new.rfind("\n")
            if tail_idx == -1:
                offset -= len(new)
                return
            consumable = new[: tail_idx + 1]
            offset -= len(new) - len(consumable)
            new = consumable
        for raw_line in new.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line_to_event is None:
                yield _DictEvent({"line": line})
            else:
                parsed = line_to_event(line)
                if parsed is not None:
                    yield _DictEvent(parsed) if isinstance(parsed, dict) else parsed

    async def discard_stdout() -> None:
        # Console output is not this adapter's trajectory source. Drain it
        # without retaining bytes so bounded pipes cannot stall the agent.
        async for _ in handle.stdout:
            pass

    stdout_task = asyncio.create_task(discard_stdout())
    wait_task = asyncio.create_task(handle.wait())
    try:
        while not wait_task.done():
            async for event in _drain_once(final=False):
                yield event
            try:
                await asyncio.wait_for(
                    asyncio.shield(wait_task), timeout=poll_interval_sec,
                )
            except TimeoutError:
                pass
        # Final flush after process exits.
        async for event in _drain_once(final=True):
            yield event
        await wait_task
        await stdout_task
    finally:
        for task in (stdout_task, wait_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, wait_task, return_exceptions=True)
