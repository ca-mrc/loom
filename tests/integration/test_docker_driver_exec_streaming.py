"""DockerDriver.exec_streaming integration tests. Docker-gated."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
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


async def test_docker_streaming_wait_preserves_slow_output_and_repeated_wait() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "head -c 4194304 /dev/zero; head -c 4194304 /dev/zero >&2; exit 7"],
            env_vars={}, cwd=PurePosixPath("/workspace"),
        )

        async def consume(stream):
            size = 0
            async for chunk in stream:
                size += len(chunk)
                await asyncio.sleep(0.005)
            return size

        results = await asyncio.wait_for(
            asyncio.gather(consume(handle.stdout), consume(handle.stderr), handle.wait()),
            timeout=15,
        )
        assert results == [4194304, 4194304, 7]
        assert await asyncio.wait_for(handle.wait(), timeout=3) == 7
    finally:
        await driver.stop()


async def test_docker_stop_releases_abandoned_full_stream() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    before = asyncio.all_tasks()
    handle = await driver.exec_streaming(
        ["sh", "-c", "head -c 67108864 /dev/zero; sleep 30"],
        env_vars={}, cwd=PurePosixPath("/workspace"),
    )
    readers = asyncio.all_tasks() - before
    try:
        await asyncio.sleep(0.3)
        await driver.stop()
        _, pending = await asyncio.wait(readers, timeout=2)
        assert not pending, "driver.stop left an abandoned output reader alive"
    finally:
        # Release a broken implementation's reader so the executor can shut down.
        await handle.kill()
        await asyncio.gather(*readers, return_exceptions=True)
        await driver.stop()


def test_docker_cancel_before_reader_starts_still_signals_eof() -> None:
    """Executor saturation must not make cancellation lose both EOF signals."""
    async def exercise() -> None:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        driver = DockerDriver(image="alpine:3.20")
        await driver.start()
        release = threading.Event()
        blocker = None
        try:
            handle = await driver.exec_streaming(
                ["sh", "-c", "sleep 30"],
                env_vars={}, cwd=PurePosixPath("/workspace"),
            )
            # The reader task exists, but has not yet submitted its thread job.
            blocker = loop.run_in_executor(None, release.wait)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(handle.wait(), timeout=0.1)
            release.set()
            await blocker

            async def drain() -> None:
                async for _ in handle.stdout:
                    pass
                async for _ in handle.stderr:
                    pass

            await asyncio.wait_for(drain(), timeout=2)
        finally:
            release.set()
            if blocker is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await blocker
            await driver.stop()

    asyncio.run(exercise())


async def test_docker_streaming_wait_after_kill_retains_exit_status() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sleep", "30"], env_vars={}, cwd=PurePosixPath("/workspace"),
        )
        # Docker can return the upgraded stream before the command starts.
        # Synchronize this fixture before its best-effort kill, so the test
        # exercises wait-after-kill rather than racing process creation.
        async def wait_until_started() -> None:
            while (await driver.exec("pgrep -fx 'sleep 30'")).return_code != 0:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_until_started(), timeout=5)
        await handle.kill()
        assert await asyncio.wait_for(handle.wait(), timeout=5) == 137
        assert await asyncio.wait_for(handle.wait(), timeout=5) == 137
    finally:
        await driver.stop()
