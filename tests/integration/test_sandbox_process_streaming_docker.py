"""#2310: `ServiceSandboxDriver.exec_streaming` against the real sandbox runtime.

The runtime runs as an unprivileged, network-less container, as a native task
sidecar does, and the driver talks to it only through its Unix socket.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import PurePosixPath
from uuid import uuid4

import httpx
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverError
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(240)]

_IMAGE = "python:3.12-slim"
_WORKDIR = PurePosixPath("/tmp")


@pytest.fixture
async def driver(native_binary, tmp_path):  # noqa: F811
    import docker

    client = docker.from_env()
    socket = tmp_path / "sandbox"
    socket.mkdir(mode=0o777)
    socket.chmod(0o2777)
    container = client.containers.run(
        _IMAGE, ["--socket", "/socket/sandbox.sock", "--exec-timeout-seconds", "60"], detach=True,
        entrypoint="/loom/bin/loom-sandbox-runtime", user="65532:65532",
        network_mode="none", cap_drop=["ALL"], security_opt=["no-new-privileges"],
        volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                 str(socket): {"bind": "/socket", "mode": "rw"}},
    )
    sandbox = ServiceSandboxDriver(socket / "sandbox.sock", capabilities=Capabilities(
        os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
        dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"}),
    ), network_policy=NoNetwork())
    try:
        for attempt in range(100):
            try:
                await sandbox.start()
                break
            except (OSError, RuntimeError, httpx.TransportError):
                if attempt == 99:
                    raise
                await asyncio.sleep(0.05)
        yield sandbox
    finally:
        await sandbox.stop(delete=True)
        container.remove(force=True)
        client.close()


async def _collect(stream) -> bytes:
    return b"".join([chunk async for chunk in stream])


async def test_streams_both_outputs_and_reports_exit_status(driver) -> None:
    handle = await driver.exec_streaming(
        ["/bin/sh", "-c", 'printf "$GREETING"; printf oops >&2; exit 7'],
        env_vars={"GREETING": "hello"}, cwd=_WORKDIR,
    )
    stdout, stderr = await asyncio.gather(_collect(handle.stdout), _collect(handle.stderr))

    assert (stdout, stderr) == (b"hello", b"oops")
    assert await handle.wait() == 7
    assert handle.pid > 1


async def test_large_output_is_lossless_and_ordered(driver) -> None:
    size = 6 * 1024 * 1024  # more than one server-side stream window
    handle = await driver.exec_streaming(
        ["python3", "-c", f"import sys; sys.stdout.write(''.join(str(i % 10) for i in range({size})))"],
        env_vars={}, cwd=_WORKDIR,
    )
    stdout, stderr = await asyncio.gather(_collect(handle.stdout), _collect(handle.stderr))

    assert stderr == b"" and len(stdout) == size
    assert stdout == "".join(str(i % 10) for i in range(size)).encode()
    assert await handle.wait() == 0


async def test_jsonl_stdout_feeds_the_existing_launcher_capture(driver) -> None:
    from loom_launcher.capture import stream_stdout_jsonl

    from loom.agent.subprocess import _bridge_driver, _bridge_exec_handle

    events = [{"kind": "agent_thought", "n": n} for n in range(50)]
    script = "".join(f"print({json.dumps(json.dumps(event))}, flush=True)\n" for event in events)
    handle = await driver.exec_streaming(["python3", "-c", script], env_vars={}, cwd=_WORKDIR)
    launcher = _bridge_exec_handle(handle, _bridge_driver(driver, cwd=_WORKDIR))

    captured = [event.model_dump() async for event in stream_stdout_jsonl(launcher)]

    assert [item["n"] for item in captured if "n" in item] == list(range(50))
    assert await handle.wait() == 0


async def test_kill_stops_the_process_group(driver) -> None:
    handle = await driver.exec_streaming(
        ["/bin/sh", "-c", "sleep 300 & echo started; sleep 300"], env_vars={}, cwd=_WORKDIR,
    )
    first = await anext(handle.stdout)
    assert first == b"started\n"

    await handle.kill()

    assert await asyncio.wait_for(handle.wait(), 10) == 137
    rest = await asyncio.wait_for(_collect(handle.stdout), 10)
    assert rest == b""  # the background child died with its group, closing the pipe


async def test_deadline_is_enforced_by_the_sandbox(driver) -> None:
    handle = await driver.exec_streaming(["sleep", "300"], env_vars={}, cwd=_WORKDIR, timeout_sec=0.5)

    assert await asyncio.wait_for(handle.wait(), 10) == 124


async def test_records_are_released_after_exit_and_drain(driver) -> None:
    # The server keeps at most 16 records; fully consumed handles must not leak.
    for index in range(20):
        handle = await driver.exec_streaming(["echo", str(index)], env_vars={}, cwd=_WORKDIR)
        assert await _collect(handle.stdout) == f"{index}\n".encode()
        await _collect(handle.stderr)
        assert await handle.wait() == 0


async def test_invalid_requests_fail_with_safe_reason_codes(driver) -> None:
    with pytest.raises(DriverError, match="exec_timeout_invalid"):
        await driver.exec_streaming(["true"], env_vars={}, cwd=_WORKDIR, timeout_sec=3600)
    with pytest.raises(DriverError, match="exec_user_mismatch"):
        await driver.exec_streaming(["true"], env_vars={}, cwd=_WORKDIR, user=str(uuid4().int % 60000 + 1))
    with pytest.raises(DriverError, match="requires a command"):
        await driver.exec_streaming([], env_vars={}, cwd=_WORKDIR)
