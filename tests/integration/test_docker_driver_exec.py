"""DockerDriver.exec live tests."""

from __future__ import annotations

import asyncio
import tracemalloc
from collections.abc import AsyncGenerator
from pathlib import PurePosixPath

import pytest

from loom.driver.base import MAX_EXEC_STREAM_BYTES, StartOptions

pytestmark = pytest.mark.docker


@pytest.fixture
async def docker_driver() -> AsyncGenerator[object, None]:
    pytest.importorskip("docker")
    import docker
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    await d.start(options=StartOptions())
    try:
        yield d
    finally:
        await d.stop(delete=True)


async def test_exec_success(docker_driver):  # type: ignore[no-untyped-def]
    r = await docker_driver.exec("echo hi && echo err 1>&2")
    assert r.return_code == 0
    assert b"hi" in r.stdout
    assert b"err" in r.stderr
    assert r.truncated is False
    assert r.duration_sec > 0


async def test_exec_nonzero(docker_driver):  # type: ignore[no-untyped-def]
    r = await docker_driver.exec("false")
    assert r.return_code == 1


async def test_exec_truncates_large_stdout(docker_driver):  # type: ignore[no-untyped-def]
    r = await docker_driver.exec("yes a | head -c 12582912")
    assert r.truncated is True
    assert len(r.stdout) <= MAX_EXEC_STREAM_BYTES


async def test_exec_caps_memory_while_draining_both_streams(docker_driver, record_property):  # type: ignore[no-untyped-def]
    """Truncating after docker-py buffers the whole reply does not bound RAM."""
    tracemalloc.start()
    try:
        result = await docker_driver.exec(
            "head -c 33554432 /dev/zero; head -c 33554432 /dev/zero >&2; exit 23",
            timeout_sec=30,
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    record_property("peak_traced_bytes", peak)
    assert result.return_code == 23
    assert len(result.stdout) == len(result.stderr) == MAX_EXEC_STREAM_BYTES
    assert result.truncated
    assert peak < 5 * MAX_EXEC_STREAM_BYTES, f"buffering 64 MiB output allocated {peak} bytes"


async def test_exec_with_env_and_cwd(docker_driver):  # type: ignore[no-untyped-def]
    await docker_driver.exec("mkdir -p /tmp/sub")
    r = await docker_driver.exec(
        "echo $MY_VAR", cwd=PurePosixPath("/tmp/sub"), env={"MY_VAR": "value123"},
    )
    assert b"value123" in r.stdout


async def test_exec_with_timeout(docker_driver):  # type: ignore[no-untyped-def]
    with pytest.raises(asyncio.TimeoutError):
        await docker_driver.exec("sleep 5", timeout_sec=0.5)
    assert (await docker_driver.exec("echo still-running")).stdout == b"still-running\n"
