"""Real software-guest qualification; no host devices, sysctl writes or model.

LOOM_GUEST_PAYLOAD names a built immutable payload containing both Go binaries.
The CI owning lane builds it before enabling this fixture.
"""
from __future__ import annotations

import base64
import contextlib
import os
import shutil
import signal
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.integration


@contextlib.contextmanager
def guest() -> Iterator[tuple[httpx.Client, subprocess.Popen[bytes], Path]]:
    configured = os.environ.get("LOOM_GUEST_PAYLOAD")
    if configured is None:
        pytest.skip("requires built LOOM_GUEST_PAYLOAD")
    payload = Path(configured).resolve()
    assert (payload / "bin/loom-guest-runtime").is_file()
    # Linux Unix sockets have a short path ceiling; avoid pytest's long IDs.
    with tempfile.TemporaryDirectory(prefix="loom-guest-", dir="/tmp") as temporary:
        directory = Path(temporary)
        root = directory / "root"
        for item in ("bin", "etc", "tmp", "proc", "sys", "dev", "var", "lib", "usr"):
            (root / item).mkdir(parents=True)
        shutil.copyfile(payload / "bin/busybox", root / "bin/busybox")
        (root / "bin/busybox").chmod(0o755)
        for applet in ("sh", "cat", "sleep", "uname", "kill", "reboot", "ls", "test"):
            (root / "bin" / applet).symlink_to("busybox")
        socket = directory / "sandbox.sock"
        state = directory / "state"
        command = [
            str(payload / "bin/loom-guest-runtime"), "--payload", str(payload),
            "--root", str(root), "--state", str(state), "--socket", str(socket),
            "--memory-mib", "1024", "--storage-mib", "512", "--cpu-millis", "1000",
            "--exec-timeout-seconds", "60",
        ]
        with (directory / "console.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            client = httpx.Client(transport=httpx.HTTPTransport(uds=str(socket)), timeout=40)
            try:
                deadline = time.monotonic() + 65
                while True:
                    try:
                        response = client.get("http://sandbox/health")
                        if response.status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    assert process.poll() is None, (directory / "console.log").read_text()
                    assert time.monotonic() < deadline, (directory / "console.log").read_text()
                    time.sleep(0.05)
                yield client, process, directory
            finally:
                client.close()
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                        pytest.fail("guest launcher did not terminate its guest on cancellation")


def execute(client: httpx.Client, command: str) -> str:
    response = client.post("http://sandbox/exec", json={
        "argv": ["/bin/sh", "-c", command], "cwd": "/", "timeout_sec": 30,
    })
    response.raise_for_status()
    result = response.json()
    assert result["return_code"] == 0, result
    return base64.b64decode(result["stdout"]).decode()


def test_real_guest_core_and_filesystem_are_private() -> None:
    paths = [Path("/proc/sys/kernel") / name for name in ("core_pattern", "core_uses_pid")]
    before = [path.read_bytes() for path in paths]
    with guest() as (client, process, directory):
        output = execute(client, "set -eu; echo /tmp/guest-core.%p > /proc/sys/kernel/core_pattern; "
                         "echo private > /etc/guest-marker; ulimit -c unlimited; "
                         "sh -c 'kill -ABRT $$' || true; test -s /tmp/guest-core.*; "
                         "cat /etc/guest-marker; cat /proc/sys/kernel/core_pattern")
        assert output == "private\n/tmp/guest-core.%p\n"
        assert not (directory / "root/etc/guest-marker").exists()
        assert [path.read_bytes() for path in paths] == before
        response = client.get("http://sandbox/file", params={"path": "/etc/guest-marker"})
        response.raise_for_status()
        assert response.content == b"private\n"
        execute(client, "(sleep 300) </dev/null >/dev/null 2>&1 &")
        for action in ("pause-processes", "resume-processes", "stop-processes"):
            response = client.post(f"http://sandbox/{action}")
            assert response.status_code == 204, response.text
        child_pids = Path(f"/proc/{process.pid}/task/{process.pid}/children").read_text().split()
        assert child_pids, "guest compute missing"
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)
        assert all(not Path(f"/proc/{pid}").exists() for pid in child_pids)
        assert not (directory / "state/state.ext4").exists()
        assert (directory / "state/retired").exists()
    assert [path.read_bytes() for path in paths] == before


def test_parallel_guests_have_independent_kernel_and_artifacts() -> None:
    with guest() as (left, _, _), guest() as (right, _, _):
        left_id = execute(left, "cat /proc/sys/kernel/random/boot_id")
        right_id = execute(right, "cat /proc/sys/kernel/random/boot_id")
        assert left_id != right_id
        execute(left, "echo left > /proc/sys/kernel/core_pattern; echo left > /tmp/owner")
        execute(right, "echo right > /proc/sys/kernel/core_pattern; echo right > /tmp/owner")
        assert execute(left, "cat /proc/sys/kernel/core_pattern /tmp/owner") == "left\nleft\n"
        assert execute(right, "cat /proc/sys/kernel/core_pattern /tmp/owner") == "right\nright\n"
