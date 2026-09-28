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

pytestmark = [pytest.mark.integration, pytest.mark.docker]


@contextlib.contextmanager
def guest(*, docker: bool = False, plugin_layout: str | None = None,
          wait_ready: bool = True) -> Iterator[tuple[httpx.Client, subprocess.Popen[bytes], Path]]:
    configured = os.environ.get("LOOM_GUEST_PAYLOAD")
    if configured is None:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            pytest.fail("CI must build LOOM_GUEST_PAYLOAD before running guest qualification")
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
        # Standard distro absolute link must resolve inside the task root.
        (root / "var/run").symlink_to("/run")
        if plugin_layout:
            plugins = root / "usr/local/lib/docker/cli-plugins"
            plugins.parent.mkdir(parents=True)
            if plugin_layout == "symlink":
                (root / "opt/task-plugins").mkdir(parents=True)
                plugins.symlink_to("/opt/task-plugins")
            else:
                plugins.mkdir()
        socket = directory / "sandbox.sock"
        state = directory / "state"
        command = [
            str(payload / "bin/loom-guest-runtime"), "--payload", str(payload),
            "--root", str(root), "--state", str(state), "--socket", str(socket),
            "--memory-mib", "2048" if docker else "1024", "--storage-mib", "1024" if docker else "512", "--cpu-millis", "1000",
            "--exec-timeout-seconds", "60",
        ]
        if docker:
            command.append("--nested-docker")
        with (directory / "console.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            client = httpx.Client(transport=httpx.HTTPTransport(uds=str(socket)), timeout=40)
            try:
                deadline = time.monotonic() + 65
                while wait_ready:
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
    return base64.b64decode(result["stdout"] or "").decode()


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
        child_pids = {pid for children in Path(f"/proc/{process.pid}/task").glob("*/children")
                      for pid in children.read_text().split()}
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


def test_cancellation_during_boot_retires_state_and_prevents_restart() -> None:
    with guest(wait_ready=False) as (_, process, directory):
        deadline = time.monotonic() + 10
        while not (directory / "state/channel.sock").exists():
            assert process.poll() is None, (directory / "console.log").read_text()
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert not (directory / "sandbox.sock").exists(), "probe must cancel before readiness"
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) != 0
        assert not (directory / "state/state.ext4").exists()
        assert not (directory / "state/channel.sock").exists()
        assert (directory / "state/retired").exists()
        restarted = subprocess.run(process.args, capture_output=True, timeout=5)
        assert restarted.returncode != 0
        assert b"state already exists" in restarted.stderr
        assert not (directory / "state/state.ext4").exists()


def test_failed_command_deadline_and_guest_exit_remain_distinct() -> None:
    with guest() as (client, process, directory):
        for command, timeout, code in (("exit 7", 10, 7), ("sleep 30", 0.1, 124)):
            response = client.post("http://sandbox/exec", json={
                "argv": ["/bin/sh", "-c", command], "timeout_sec": timeout,
            })
            response.raise_for_status()
            assert response.json()["return_code"] == code
        assert execute(client, "echo still-alive") == "still-alive\n"
        # Reboot must terminate the incarnation, never silently create a fresh
        # guest disk behind the same Unix socket and lease.
        with contextlib.suppress(httpx.TransportError):
            client.post("http://sandbox/exec", json={"argv": ["reboot", "-f"]})
        assert process.wait(timeout=5) != 0
        assert not (directory / "state/state.ext4").exists()
        assert (directory / "state/retired").exists()


@pytest.mark.timeout(180)
def test_guest_docker_build_cache_invalidation_and_artifact() -> None:
    with guest(docker=True) as (client, _, _):
        execute(client, "set -eu; /bin/busybox mkdir -p /tmp/context; "
                "/bin/busybox cp /bin/busybox /tmp/context/shell; "
                "printf 'first\\n' > /tmp/context/input")
        dockerfile = rb'''FROM scratch
COPY shell /bin/sh
RUN ["/bin/sh", "-c", "echo dependency > /dependency"]
COPY input /input
RUN ["/bin/sh", "-c", "read value < /input; printf '%s' \"$value\" > /output"]
CMD ["/bin/sh", "-c", "read value < /output; printf '%s' \"$value\""]
'''
        response = client.put("http://sandbox/file", params={"path": "/tmp/context/Dockerfile"}, content=dockerfile)
        response.raise_for_status()
        build = "docker build --progress=plain -t loom-cache-probe /tmp/context 2>&1"
        execute(client, build)
        first = execute(client, "docker image inspect --format '{{json .RootFS.Layers}}' loom-cache-probe")
        cached = execute(client, build)
        assert cached.count("CACHED") >= 3, cached
        assert execute(client, "docker image inspect --format '{{json .RootFS.Layers}}' loom-cache-probe") == first
        execute(client, "printf 'second\\n' > /tmp/context/input")
        changed = execute(client, build)
        assert "CACHED" in changed, changed
        second = execute(client, "docker image inspect --format '{{json .RootFS.Layers}}' loom-cache-probe")
        import json
        before, after = json.loads(first), json.loads(second)
        assert before[:2] == after[:2]
        assert before[2:] != after[2:]
        assert execute(client, "docker run --rm --network none loom-cache-probe") == "second"
        execute(client, "set -eu; docker create --name export-probe loom-cache-probe; "
                "docker cp export-probe:/output /tmp/result; docker rm -v export-probe")
        result = client.get("http://sandbox/file", params={"path": "/tmp/result"})
        result.raise_for_status()
        assert result.content == b"second"
        with guest(docker=True) as (other, _, _):
            assert execute(other, "docker image ls -q") == ""
            assert execute(other, "docker ps -aq") == ""


def test_guest_docker_registry_traffic_uses_outer_allowlist_proxy() -> None:
    import http.server
    import threading

    destinations: list[str] = []

    class Proxy(http.server.BaseHTTPRequestHandler):
        def do_CONNECT(self) -> None:
            destinations.append(self.path)
            self.send_error(403, "fixture allowlist denied")

        def log_message(self, *_args: object) -> None:
            pass

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 18791), Proxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    try:
        with guest(docker=True) as (client, _, _):
            assert execute(client, "docker info --format '{{.HTTPProxy}}'").strip() == "http://10.0.2.2:18791"
            response = client.post("http://sandbox/exec", json={
                "argv": ["docker", "pull", "registry.example.org/blocked/image:fixture"], "timeout_sec": 20,
            })
            response.raise_for_status()
            result = response.json()
            assert result["return_code"] != 0
            assert "fixture allowlist denied" in base64.b64decode(result["stderr"] or "").decode()
            assert destinations == ["registry.example.org:443"]
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("layout", ["directory", "symlink"])
def test_existing_docker_plugin_layout_does_not_block_guest(layout: str) -> None:
    with guest(docker=True, plugin_layout=layout) as (client, _, _):
        assert "v0.36.1" in execute(client, "docker buildx version")
        assert execute(client, "echo $DOCKER_CONFIG").strip() == "/loom/docker-client"


@contextlib.contextmanager
def container_guest(*, memory_mib: int = 1024) -> Iterator[tuple[httpx.Client, str]]:
    """Launch with the rendered read-only root and limited outer capabilities."""
    import uuid

    configured = os.environ.get("LOOM_GUEST_PAYLOAD")
    if configured is None:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            pytest.fail("CI must build LOOM_GUEST_PAYLOAD before running guest qualification")
        pytest.skip("requires built LOOM_GUEST_PAYLOAD")
    payload = Path(configured).resolve()
    name = "loom-guest-mounts-" + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix="loom-mounts-", dir="/tmp") as temporary:
        directory = Path(temporary)
        root = directory / "root"
        for item in ("bin", "etc", "tmp", "proc", "sys", "dev", "var", "lib", "usr"):
            (root / item).mkdir(parents=True)
        shutil.copyfile(payload / "bin/busybox", root / "bin/busybox")
        (root / "bin/busybox").chmod(0o755)
        (root / "bin/sh").symlink_to("busybox")
        (directory / "Dockerfile").write_text("FROM scratch\nCOPY root /\n")
        subprocess.run(["docker", "build", "-q", "-t", name, str(directory)], check=True, capture_output=True)
        rpc = directory / "rpc"
        rpc.mkdir(mode=0o770)
        rpc.chmod(0o2770)  # inherit the caller's group for the root-owned socket
        command = [
            "docker", "run", "--name", name, "--read-only", "--cap-drop=ALL", "--cap-add=DAC_OVERRIDE",
            "--security-opt=no-new-privileges", "--cpus=1", f"--memory={memory_mib}m", "--pids-limit=128",
            "--tmpfs", "/left", "--tmpfs", "/right", "--tmpfs", "/state:size=1g",
            "--volume", f"{payload}:/payload:ro", "--volume", f"{rpc}:/rpc",
            name, "/bin/busybox", "sh", "-ec",
            "echo left > /left/value; echo right > /right/value; "
            "exec /payload/bin/loom-guest-runtime --payload /payload --root / "
            f"--state /state/incarnation --socket /rpc/sandbox.sock --memory-mib {memory_mib} "
            "--storage-mib 160 --cpu-millis 1000 --exec-timeout-seconds 60",
        ]
        with (directory / "console.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                with httpx.Client(
                    transport=httpx.HTTPTransport(uds=str(rpc / "sandbox.sock")), timeout=35
                ) as client:
                    deadline = time.monotonic() + 65
                    while True:
                        try:
                            if client.get("http://sandbox/health").is_success:
                                break
                        except httpx.TransportError:
                            pass
                        assert process.poll() is None, (directory / "console.log").read_text()
                        assert time.monotonic() < deadline, (directory / "console.log").read_text()
                        time.sleep(0.05)
                    yield client, name
            finally:
                subprocess.run(["docker", "stop", "--time=5", name], capture_output=True, check=False)
                process.wait(timeout=10)
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
                subprocess.run(["docker", "image", "rm", name], capture_output=True, check=False)


def test_container_guest_preserves_distinct_files_on_separate_mounts() -> None:
    """9p must remap equal inode numbers from different outer filesystems."""
    with container_guest() as (client, name):
        outer = subprocess.run([
            "docker", "exec", name, "/bin/busybox", "stat", "-c", "%d:%i", "/left/value", "/right/value",
        ], check=True, capture_output=True, text=True).stdout.splitlines()
        left, right = [value.split(":") for value in outer]
        assert left[0] != right[0] and left[1] == right[1], outer
        assert execute(client, "/bin/busybox cat /left/value /right/value") == "left\nright\n"


@pytest.mark.parametrize("memory_mib,pressure_mib", [(512, 128), (1024, 600)])
def test_container_guest_stays_within_memory_limit_under_ram_and_disk_pressure(
    memory_mib: int, pressure_mib: int,
) -> None:
    with container_guest(memory_mib=memory_mib) as (client, name):
        # Leave room for the guest kernel. Concurrent disk traffic must not
        # consume the outer envelope, even at the admitted boot minimum.
        response = client.post("http://sandbox/exec", json={
            "argv": ["/bin/sh", "-ec",
                     f"/bin/busybox mkdir /pressure; /bin/busybox mount -t tmpfs -o size={pressure_mib}m tmpfs /pressure; "
                     f"/bin/busybox dd if=/dev/zero of=/pressure/data bs=1M count={pressure_mib}; "
                     "/bin/busybox dd if=/dev/zero of=/tmp/data bs=1M count=64; "
                     "/bin/busybox sha256sum /pressure/data; /bin/busybox sync"],
            "timeout_sec": 60,
        }, timeout=65)
        response.raise_for_status()
        result = response.json()
        assert result["return_code"] == 0, result
        events = subprocess.run([
            "docker", "exec", name, "/bin/busybox", "cat", "/sys/fs/cgroup/memory.events",
        ], check=True, capture_output=True, text=True).stdout
        counts = dict(line.split() for line in events.splitlines())
        assert counts["oom"] == counts["oom_kill"] == "0", events
        assert execute(client, "echo alive") == "alive\n"
