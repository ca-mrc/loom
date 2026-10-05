"""Deterministic Docker SDK scheduling boundaries during driver teardown."""

from __future__ import annotations

import asyncio
import threading
from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest

from loom.driver.docker import DockerDriver
from loom.errors import DriverNotStartedError


class _Stream:
    def __init__(self):
        self.closed = threading.Event()

    def __iter__(self):
        while not self.closed.is_set():
            yield b"x" * 65536, None

    def close(self):
        self.closed.set()


@pytest.mark.parametrize("boundary", ["removing", "starting"])
async def test_stop_rejects_new_and_late_streams(boundary):
    entered, release = threading.Event(), threading.Event()
    stream = _Stream()

    def start(_exec_id, *, stream=False, demux=False, detach=False):
        if detach:
            return None
        if boundary == "starting":
            entered.set()
            assert release.wait(5)
        return output

    def remove(**kwargs):
        if boundary == "removing":
            entered.set()
            assert release.wait(5)

    output = stream
    api = SimpleNamespace(exec_create=lambda **kw: {"Id": "owned-exec"}, exec_start=start)
    driver = DockerDriver(image="unused")
    driver._client = SimpleNamespace(api=api, close=lambda: None)
    driver._container = SimpleNamespace(id="owned-container", remove=remove)
    driver._state = "running"
    started = None
    stopped = None
    handle = None
    try:
        if boundary == "removing":
            stopped = asyncio.create_task(driver.stop())
            assert await asyncio.to_thread(entered.wait, 3)
        started = asyncio.create_task(driver.exec_streaming(
            ["unused"], env_vars={}, cwd=PurePosixPath("/workspace"),
        ))
        if boundary == "starting":
            assert await asyncio.to_thread(entered.wait, 3)
            await driver.stop()
            release.set()
        try:
            handle = await asyncio.wait_for(started, timeout=3)
        except DriverNotStartedError:
            pass
        else:
            pytest.fail("a stream escaped driver shutdown")
        if boundary == "starting":
            assert stream.closed.is_set(), "late transport was not closed"
    finally:
        release.set()
        if stopped is not None:
            await stopped
        if handle is not None:
            await handle.kill()
        stream.close()
        await driver.stop()
