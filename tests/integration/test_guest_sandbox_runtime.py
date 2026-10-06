"""Real software-guest qualification; no host devices, sysctl writes or model.

LOOM_GUEST_PAYLOAD names a built immutable payload containing both Go binaries.
The CI owning lane builds it before enabling this fixture.
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import os
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.docker]


@contextlib.contextmanager
def guest(*, docker: bool = False, plugin_layout: str | None = None, wait_ready: bool = True,
          root_image: str | None = None, storage_mib: int | None = None,
          exec_timeout_seconds: int = 60) -> Iterator[tuple[httpx.Client, subprocess.Popen[bytes], Path]]:
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
        if root_image is not None:
            # A real task image's filesystem as the guest's read-only root.
            _export_image_root(root_image, root)
        else:
            for item in ("bin", "etc", "tmp", "proc", "sys", "dev", "var", "lib", "usr"):
                (root / item).mkdir(parents=True)
            for applet in ("sh", "cat", "sleep", "uname", "kill", "reboot", "ls", "test"):
                (root / "bin" / applet).symlink_to("busybox")
            # Standard distro absolute link must resolve inside the task root.
            (root / "var/run").symlink_to("/run")
        shutil.copyfile(payload / "bin/busybox", root / "bin/busybox")
        (root / "bin/busybox").chmod(0o755)
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
            "--memory-mib", "2048" if docker else "1024",
            "--storage-mib", str(storage_mib or (1024 if docker else 512)), "--cpu-millis", "1000",
            "--exec-timeout-seconds", str(exec_timeout_seconds),
        ]
        if docker:
            command.append("--nested-docker")
        with (directory / "console.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            client = httpx.Client(transport=httpx.HTTPTransport(uds=str(socket)), timeout=40)
            try:
                # Cover the runtime's 30s disk + 90s boot limits before the
                # harness declares failure; healthy slow Docker boot is valid.
                deadline = time.monotonic() + 125
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


def _export_image_root(image: str, root: Path) -> None:
    container = subprocess.run(["docker", "create", image], check=True, capture_output=True, text=True).stdout.strip()
    try:
        exported = subprocess.Popen(["docker", "export", container], stdout=subprocess.PIPE)
        assert exported.stdout is not None
        with tarfile.open(fileobj=exported.stdout, mode="r|") as stream:
            stream.extractall(root, filter="tar")
        assert exported.wait() == 0
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)


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


def test_guest_posix_semaphores_work_and_are_private(tmp_path: Path) -> None:
    """Python multiprocessing and legacy image downloaders need sem_open."""
    source = tmp_path / "semaphore.c"
    source.write_text(r'''
#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>
int main(void) {
    sem_t *sem = sem_open("/loom-private-probe", O_CREAT | O_EXCL, 0600, 0);
    if (sem == SEM_FAILED) { int code = errno; perror("sem_open"); return code == EEXIST ? 17 : 1; }
    pid_t child = fork();
    if (child < 0) return 2;
    if (child == 0) _exit(sem_post(sem) == 0 ? 0 : 3);
    if (sem_wait(sem) != 0) return 4;
    int status;
    if (waitpid(child, &status, 0) != child || status != 0) return 5;
    if (sem_close(sem) != 0) return 6;
    puts("semaphore handoff passed");
    return 0;
}
''')
    binary = tmp_path / "semaphore"
    subprocess.run(["cc", "-static", "-pthread", "-o", str(binary), str(source)],
                   check=True, capture_output=True)
    with guest() as (first, _, _), guest() as (other, _, _):
        for client in (first, other):
            response = client.put("http://sandbox/file", params={"path": "/tmp/semaphore"},
                                  content=binary.read_bytes())
            response.raise_for_status()
            assert execute(client, "/bin/busybox chmod +x /tmp/semaphore; /tmp/semaphore") == "semaphore handoff passed\n"
        # The name persists in its own guest, while the unrelated guest could
        # create that same exclusive name and operate on its own semaphore.
        response = first.post("http://sandbox/exec", json={"argv": ["/tmp/semaphore"], "timeout_sec": 10})
        response.raise_for_status()
        assert response.json()["return_code"] == 17


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
def container_guest(*, memory_mib: int = 1024, storage_mib: int = 160,
                    disk_state: bool = False, cpus: int = 1) -> Iterator[tuple[httpx.Client, str]]:
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
        state_mount = ["--tmpfs", "/state:size=1g"]
        if disk_state:
            # Production emptyDir uses disk, not an additional RAM allocation.
            state = directory / "state"
            state.mkdir(mode=0o700)
            state_mount = ["--volume", f"{state}:/state"]
        command = [
            "docker", "run", "--name", name, "--read-only", "--cap-drop=ALL", "--cap-add=DAC_OVERRIDE",
            "--security-opt=no-new-privileges", f"--cpus={cpus}", f"--memory={memory_mib}m",
            f"--memory-swap={memory_mib}m", "--pids-limit=128",
            "--tmpfs", "/left", "--tmpfs", "/right", *state_mount,
            "--volume", f"{payload}:/payload:ro", "--volume", f"{rpc}:/rpc",
            name, "/bin/busybox", "sh", "-ec",
            "echo left > /left/value; echo right > /right/value; "
            "exec /payload/bin/loom-guest-runtime --payload /payload --root / "
            f"--state /state/incarnation --socket /rpc/sandbox.sock --memory-mib {memory_mib} "
            f"--storage-mib {storage_mib} --cpu-millis {cpus * 1000} --exec-timeout-seconds 900 "
            f"--max-transfer-bytes {6 * 1024**3}",
        ]
        with (directory / "console.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                with httpx.Client(
                    transport=httpx.HTTPTransport(uds=str(rpc / "sandbox.sock")), timeout=35
                ) as client:
                    deadline = time.monotonic() + 125
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
                if disk_state:
                    subprocess.run([
                        "docker", "run", "--rm", "--network=none", "--read-only", "--cap-drop=ALL",
                        "--cap-add=DAC_OVERRIDE", "--volume", f"{state}:/state", name,
                        "/bin/busybox", "rm", "-rf", "/state/incarnation",
                    ], capture_output=True, check=True)
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
        swap = subprocess.run([
            "docker", "exec", name, "/bin/busybox", "cat",
            "/sys/fs/cgroup/memory.swap.max", "/sys/fs/cgroup/memory.swap.current",
        ], check=True, capture_output=True, text=True).stdout
        assert swap.splitlines() == ["0", "0"], swap
        assert execute(client, "echo alive") == "alive\n"


@pytest.mark.timeout(600)
def test_large_archive_restore_survives_full_guest_page_cache(tmp_path: Path) -> None:
    """A 4 GiB handoff must not OOM the outer 8 GiB sandbox as RAM fills."""
    source = tmp_path / "image"
    size = 4000 * 1024**2
    with source.open("wb") as stream:
        stream.truncate(size)
    archive = tmp_path / "workspace.tar"
    with tarfile.open(archive, "w") as stream:
        stream.add(source, arcname="image")
    with source.open("rb") as stream:
        expected = hashlib.file_digest(stream, "sha256").hexdigest()
    with container_guest(memory_mib=8192, storage_mib=12288, disk_state=True, cpus=2) as (client, name):
        with archive.open("rb") as stream:
            response = client.put(
                "http://sandbox/file", params={"path": "/tmp/input.tar"},
                content=iter(lambda: stream.read(1024 * 1024), b""),
                headers={"Content-Length": str(archive.stat().st_size)}, timeout=180,
            )
        response.raise_for_status()
        response = client.post("http://sandbox/exec", json={
            "argv": ["/bin/sh", "-ec", "/bin/busybox mkdir /tmp/restored; "
                     "/bin/busybox tar -C /tmp/restored -xpf /tmp/input.tar; "
                     # Touch more pages than guest RAM, including cache reclaim.
                     "/bin/busybox dd if=/dev/zero of=/tmp/pressure bs=1M count=1024; "
                     "/bin/busybox sha256sum /tmp/restored/image"],
            "timeout_sec": 180,
        }, timeout=190)
        response.raise_for_status()
        result = response.json()
        assert result["return_code"] == 0, result
        assert base64.b64decode(result["stdout"] or "").decode().split()[0] == expected
        events = subprocess.run([
            "docker", "exec", name, "/bin/busybox", "cat", "/sys/fs/cgroup/memory.events",
        ], check=True, capture_output=True, text=True).stdout
        counts = dict(line.split() for line in events.splitlines())
        assert counts["oom"] == counts["oom_kill"] == "0", events
        assert execute(client, "echo alive") == "alive\n"


def _guest_driver(directory: Path):
    from loom.driver.service_sandbox import ServiceSandboxDriver
    from loom.models.capabilities import Capabilities
    from loom.models.networking import NoNetwork

    return ServiceSandboxDriver(directory / "sandbox.sock", capabilities=Capabilities(
        os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
        dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"}),
    ), network_policy=NoNetwork())


async def _collect(stream) -> bytes:
    return b"".join([chunk async for chunk in stream])


def test_supervised_processes_through_the_guest_channel() -> None:
    """#2362: the guest's outer socket proxies `/processes` over its RPC
    channel to the same sandbox server, so `exec_streaming` behaves as it
    does on a native sandbox."""
    import asyncio
    from pathlib import PurePosixPath

    cwd = PurePosixPath("/tmp")

    async def scenario(driver) -> None:
        await driver.start()
        try:
            handle = await driver.exec_streaming(
                ["/bin/sh", "-c", 'printf "$GREETING"; printf oops >&2; exit 7'],
                env_vars={"GREETING": "hello"}, cwd=cwd,
            )
            assert await asyncio.gather(_collect(handle.stdout), _collect(handle.stderr)) == [b"hello", b"oops"]
            assert await handle.wait() == 7

            # An output long-poll held open across the channel while idle.
            handle = await driver.exec_streaming(["/bin/sh", "-c", "echo first; sleep 3; echo second"],
                                                 env_vars={}, cwd=cwd)
            assert await _collect(handle.stdout) == b"first\nsecond\n"
            assert await handle.wait() == 0

            # Lossless and ordered beyond one 4 MiB server-side window.
            size = 6 * 1024 * 1024
            handle = await driver.exec_streaming(
                ["/bin/sh", "-c", f"/bin/busybox yes 012345678 | /bin/busybox head -c {size}"], env_vars={}, cwd=cwd,
            )
            stdout, stderr = await asyncio.gather(_collect(handle.stdout), _collect(handle.stderr))
            assert stderr == b"" and stdout == (b"012345678\n" * (size // 10 + 1))[:size]
            assert await handle.wait() == 0

            # Kill reaches the whole process group inside the guest.
            handle = await driver.exec_streaming(["/bin/sh", "-c", "sleep 300 & echo started; sleep 300"],
                                                 env_vars={}, cwd=cwd)
            assert await anext(handle.stdout) == b"started\n"
            await handle.kill()
            assert await asyncio.wait_for(handle.wait(), 15) == 137
            assert await asyncio.wait_for(_collect(handle.stdout), 15) == b""

            handle = await driver.exec_streaming(["sleep", "300"], env_vars={}, cwd=cwd, timeout_sec=0.5)
            assert await asyncio.wait_for(handle.wait(), 15) == 124
        finally:
            await driver.stop()

    with guest() as (_, _, directory):
        asyncio.run(scenario(_guest_driver(directory)))


def test_installed_agent_reaches_the_pod_loopback_broker_from_a_guest(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2362: an installed agent in a guest reaches the Pod-local model broker
    through QEMU user networking, with exactly the environment the controller
    gives it."""
    import asyncio
    import http.server
    import threading
    from pathlib import PurePosixPath

    from loom.hosted_harness import CODEX
    from loom.service_execution_sandbox_task import installed_agent_model_environment

    seen: list[tuple[str, str | None]] = []

    class Broker(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("content-length", 0)))
            seen.append((self.path, self.headers.get("authorization")))
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: object) -> None:
            pass

    broker = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Broker)
    thread = threading.Thread(target=broker.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("LOOM_GATEWAY_URL", f"http://127.0.0.1:{broker.server_address[1]}")
    environment = installed_agent_model_environment(
        CODEX, base_url_env="OPENAI_BASE_URL", api_key_env="OPENAI_API_KEY", guest=True,
    )

    async def scenario(driver) -> bytes:
        await driver.start()
        try:
            handle = await driver.exec_streaming([
                "/bin/sh", "-c", '/bin/busybox wget -qO- --post-data "{}" '
                '--header "Authorization: Bearer $OPENAI_API_KEY" "$OPENAI_BASE_URL/responses"',
            ], env_vars=environment, cwd=PurePosixPath("/tmp"))
            stdout, stderr = await asyncio.gather(_collect(handle.stdout), _collect(handle.stderr))
            assert await handle.wait() == 0, stderr
            return stdout
        finally:
            await driver.stop()

    try:
        with guest() as (_, _, directory):
            assert asyncio.run(scenario(_guest_driver(directory))) == b'{"ok":true}'
        assert seen == [("/v1/responses", "Bearer loom_workload_proxy")]
    finally:
        broker.shutdown()
        broker.server_close()
        thread.join(timeout=5)
