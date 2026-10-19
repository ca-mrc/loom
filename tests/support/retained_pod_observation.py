"""Observe the real retirement boundary before releasing a retained test Pod."""
from __future__ import annotations

from asyncio import sleep, to_thread
from collections.abc import Callable
from time import monotonic
from typing import Any


async def wait_for_retained_pod_deletion(
    *, retire: Callable[[], dict[str, Any]], read_pod: Callable[[], Any], expected_uid: str,
) -> None:
    deadline = monotonic() + 45
    while True:
        result = await to_thread(retire)
        assert result["status"] == "pending_drain" and result["writer_migration_complete"] is False
        held = await to_thread(read_pod)
        assert held.metadata.uid == expected_uid
        assert "qualification.loom.dev/retain" in (held.metadata.finalizers or [])
        if held.metadata.deletion_timestamp is not None:
            return
        assert monotonic() < deadline, "retained controller Pod did not enter deletion"
        await sleep(0.5)
