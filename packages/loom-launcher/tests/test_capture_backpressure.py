"""Non-stdout capture must drain console output without stealing stderr."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path, PurePosixPath

import pytest

from loom_launcher.adapter import ExecHandle
from loom_launcher.capture import poll_local_http, tail_log_file


class _CaptureSource:
    """Real file events; the HTTP response boundary uses the same source."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def read_text(self, path: PurePosixPath) -> str:
        return self.path.read_text()

    async def exec_oneshot(self, argv, *, timeout_sec=10.0):
        # Transport parsing has its own tests; here only console backpressure
        # and independent capture-source completion are under test.
        if not self.path.exists() or "since=0" not in argv[-1]:
            return 0, b"[]"
        return 0, json.dumps([{"line": self.path.read_text().strip()}]).encode()


@pytest.mark.parametrize("capture", ["log", "http"])
async def test_side_channel_capture_drains_console_stdout(tmp_path, capture):
    source = tmp_path / "events"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c",
        "import os, pathlib, sys; os.write(1, b'x' * 4194304); "
        "os.write(2, b'diagnostic\\n'); pathlib.Path(sys.argv[1]).write_text('finished\\n')",
        str(source), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    async def chunks(stream):
        while data := await stream.read(65536):
            yield data

    async def kill():
        if proc.returncode is None:
            proc.kill()

    handle = ExecHandle(
        pid=proc.pid, stdout=chunks(proc.stdout), stderr=chunks(proc.stderr),
        _wait=proc.wait, _kill=kill, sandbox=_CaptureSource(source),
    )
    events = (
        tail_log_file(handle, path=PurePosixPath("/events"), poll_interval_sec=0.01)
        if capture == "log" else poll_local_http(handle, port=9000, poll_interval_sec=0.01)
    )
    try:
        async with asyncio.timeout(5):
            captured = [event.model_dump() async for event in events]
        assert captured == [{"line": "finished"}]
        assert await handle.wait() == 0
        assert b"".join([chunk async for chunk in handle.stderr]) == b"diagnostic\n"
    finally:
        await events.aclose()
        await kill()
        await proc.communicate()


@pytest.mark.parametrize("capture", ["log", "http"])
async def test_side_channel_cancellation_joins_owned_tasks(tmp_path, capture):
    source = tmp_path / "events"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(30)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    draining = asyncio.Event()

    async def chunks(stream):
        draining.set()
        while data := await stream.read(65536):
            yield data

    async def kill():
        if proc.returncode is None:
            proc.kill()

    handle = ExecHandle(
        pid=proc.pid, stdout=chunks(proc.stdout), stderr=chunks(proc.stderr),
        _wait=proc.wait, _kill=kill, sandbox=_CaptureSource(source),
    )
    events = (
        tail_log_file(handle, path=PurePosixPath("/events"), poll_interval_sec=0.01)
        if capture == "log" else poll_local_http(handle, port=9000, poll_interval_sec=0.01)
    )

    async def collect():
        return [event async for event in events]

    before = asyncio.all_tasks()
    task = asyncio.create_task(collect())
    try:
        await asyncio.wait_for(draining.wait(), timeout=3)
        owned = asyncio.all_tasks() - before
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert all(owned_task.done() for owned_task in owned)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await events.aclose()
        await kill()
        await proc.communicate()
