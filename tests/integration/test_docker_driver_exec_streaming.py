"""DockerDriver.exec_streaming integration tests. Docker-gated."""

from __future__ import annotations

import asyncio
import tracemalloc
from pathlib import PurePosixPath

import pytest

from loom.driver.docker import DockerDriver

pytestmark = pytest.mark.docker


async def test_docker_exec_streaming_echo() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "for i in 1 2 3; do echo line $i; sleep 0.05; done"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        out = b""
        async for chunk in handle.stdout:
            out += chunk
        rc = await handle.wait()
        assert rc == 0
        assert out == b"line 1\nline 2\nline 3\n"
    finally:
        await driver.stop()


async def test_docker_exec_streaming_env_vars_visible() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "echo $LOOM_TEST_VAR"],
            env_vars={"LOOM_TEST_VAR": "hello-from-test"},
            cwd=PurePosixPath("/workspace"),
        )
        out = b""
        async for chunk in handle.stdout:
            out += chunk
        rc = await handle.wait()
        assert rc == 0
        assert b"hello-from-test" in out
    finally:
        await driver.stop()


async def test_docker_exec_streaming_non_zero_exit() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "exit 7"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        async for _ in handle.stdout:
            pass
        rc = await handle.wait()
        assert rc == 7
    finally:
        await driver.stop()


async def test_docker_exec_streaming_no_10mb_cap() -> None:
    """exec_streaming's whole reason for existing is bypassing the 10 MB
    buffered exec() cap. Produce ~20 MiB of stdout and assert all of it
    flows through."""
    target_bytes = 20 * 1024 * 1024
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            # `dd` produces exactly `target_bytes` of zeros at high speed.
            ["sh", "-c", "dd if=/dev/zero bs=1M count=20 2>/dev/null"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        total = 0
        async for chunk in handle.stdout:
            total += len(chunk)
        rc = await handle.wait()
        assert rc == 0
        assert total == target_bytes, (
            f"expected {target_bytes} bytes through stream, got {total}"
        )
    finally:
        await driver.stop()


async def test_docker_exec_streaming_bounds_slow_consumer_memory(record_property) -> None:
    """A delayed consumer must backpressure Docker without dropping either stream."""
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    tracemalloc.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "head -c 33554432 /dev/zero; head -c 33554432 /dev/zero >&2; exit 7"],
            env_vars={}, cwd=PurePosixPath("/workspace"),
        )
        # Deliberately pause consumption, as a slow projection or event sink can.
        await asyncio.sleep(0.5)

        async def consume(stream):
            size = 0
            async for chunk in stream:
                size += len(chunk)
                await asyncio.sleep(0.001)
            return size

        sizes = await asyncio.wait_for(
            asyncio.gather(consume(handle.stdout), consume(handle.stderr)), timeout=20,
        )
        assert await asyncio.wait_for(handle.wait(), timeout=5) == 7
        _, peak = tracemalloc.get_traced_memory()
        record_property("peak_traced_bytes", peak)
        assert sizes == [32 * 1024 * 1024, 32 * 1024 * 1024]
        assert peak < 16 * 1024 * 1024, f"slow consumers buffered {peak} bytes"
    finally:
        tracemalloc.stop()
        await driver.stop()


async def test_docker_exec_streaming_cancelled_wait_releases_blocked_reader() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "head -c 67108864 /dev/zero; sleep 30"],
            env_vars={}, cwd=PurePosixPath("/workspace"),
        )
        # With no consumer, the reader fills its bounded queue and blocks.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(handle.wait(), timeout=0.2)

        async def drain():
            size = 0
            async for chunk in handle.stdout:
                size += len(chunk)
            async for _ in handle.stderr:
                pass
            return size

        size = await asyncio.wait_for(drain(), timeout=3)
        assert size <= 2 * 1024 * 1024
        assert (await driver.exec("echo still-running")).stdout == b"still-running\n"
    finally:
        await driver.stop()


async def test_docker_exec_streaming_kill_is_callable() -> None:
    """kill() is best-effort across docker's PID namespaces (see ExecHandle
    docstring). We verify the public contract: kill() doesn't raise on a
    process that has already exited naturally, and wait() still resolves
    to the natural exit code.

    Order matters: wait first, THEN kill. Otherwise kill races with the
    short-lived `exit 0` and the test flakes (137 vs 0)."""
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "exit 0"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        rc = await asyncio.wait_for(handle.wait(), timeout=5.0)
        assert rc == 0
        await handle.kill()  # must not raise even if the process is gone
    finally:
        await driver.stop()
