"""DockerDriver — production Driver using docker-py.

The container is created from `image` with no command (Loom holds it alive via
a sleep-infinity entrypoint override). All workloads run via `docker exec`.
Network policy switches happen via iptables rules applied inside the container
(see `loom.driver.network_policy`).

Spec §2.2 (Driver contract), §2.3 (Capabilities), §5.1 (timeouts).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import re
import shlex
import tarfile
import threading
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from queue import Empty, Full, Queue
from typing import Any, cast

import docker
from docker.errors import APIError, ImageNotFound, NotFound

from loom.driver.base import (
    MAX_EXEC_STREAM_BYTES,
    DriverResourceSnapshot,
    ExecHandle,
    StartOptions,
)
from loom.driver.network_policy import compute_iptables_rules, render_iptables_commands
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy, Public
from loom.models.types import OS

logger = logging.getLogger(__name__)

_KEEPALIVE_CMD = ["sh", "-c", "exec sleep infinity"]
_SYSTEMD_SLICE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*[.]slice$")
_CGROUP_V2_READ_SCRIPT = """
for f in cpu.stat memory.current memory.peak memory.events pids.current pids.peak io.stat; do
  printf '@@%s\\n' "$f"
  cat "/sys/fs/cgroup/$f" 2>/dev/null || true
done
""".strip()


class _DockerOutputBuffer:
    """At most 1 MiB per stream, with one pending event-loop notification.

    Blocking the Docker reader applies socket backpressure; neither queued
    bytes nor callbacks grow with the total output or a slow consumer.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._queue: Queue[bytes] = Queue(maxsize=16)
        self._available = asyncio.Event()
        self._finished = threading.Event()
        self._notification_pending = threading.Event()

    def _wake(self) -> None:
        self._notification_pending.clear()
        self._available.set()

    def _notify(self) -> None:
        if not self._notification_pending.is_set():
            self._notification_pending.set()
            with contextlib.suppress(RuntimeError):  # loop may be shutting down
                self._loop.call_soon_threadsafe(self._wake)

    def put(self, data: bytes, stopped: threading.Event) -> bool:
        for offset in range(0, len(data), 64 * 1024):
            chunk = data[offset:offset + 64 * 1024]
            while not stopped.is_set():
                try:
                    self._queue.put(chunk, timeout=0.1)
                except Full:
                    continue
                self._notify()
                break
            else:
                return False
        return True

    def finish(self) -> None:
        self._finished.set()
        self._notify()

    async def drain(self) -> AsyncIterator[bytes]:
        while True:
            await self._available.wait()
            try:
                chunk = self._queue.get_nowait()
            except Empty:
                self._available.clear()
                # finish follows the last put; recheck the queue to retain a
                # final chunk arriving between get_nowait and this check.
                if self._finished.is_set() and self._queue.empty():
                    return
            else:
                yield chunk


def _default_caps() -> Capabilities:
    return Capabilities(
        backend="docker",
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network", "allowlist"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto", "limit", "guarantee"]),
        # DockerDriver honors StartOptions.network — required for #78
        # sandbox isolation (attach to per-trial --internal bridge).
        supports_custom_network=True,
    )


def _tmpfs_specs_to_docker_map(specs: tuple[str, ...]) -> dict[str, str]:
    mounts: dict[str, str] = {}
    for spec in specs:
        path, separator, options = spec.partition(":")
        if not path:
            continue
        mounts[path] = options if separator else ""
    return mounts


def _validated_cgroup_parent(value: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise DriverError("Docker cgroup parent is malformed")
    if _SYSTEMD_SLICE_RE.fullmatch(value) is not None:
        return value
    path = PurePosixPath(value)
    if not path.is_absolute() or path == PurePosixPath("/"):
        raise DriverError("Docker cgroup parent must be a non-root absolute path")
    if any(part in {".", ".."} for part in value.split("/")):
        raise DriverError("Docker cgroup parent contains traversal")
    return path.as_posix()


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _nanoseconds_to_microseconds(value: object) -> int | None:
    parsed = _nonnegative_int(value)
    return parsed // 1_000 if parsed is not None else None


def _parse_docker_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _blkio_total(stats: dict[str, Any], key: str, operation: str) -> int | None:
    rows = stats.get("blkio_stats", {}).get(key)
    if not isinstance(rows, list):
        return None
    values: list[int] = []
    for row in rows:
        if not isinstance(row, dict) or str(row.get("op", "")).lower() != operation:
            continue
        value = _nonnegative_int(row.get("value"))
        if value is not None:
            values.append(value)
    return sum(values) if values else None


def _dict_value(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def read_container_cgroup_v2_files(container: Any) -> dict[str, str]:
    """Read the container's own namespaced cgroup-v2 files, if exposed."""

    try:
        result = container.exec_run(["sh", "-c", _CGROUP_V2_READ_SCRIPT])
    except Exception:
        return {}
    exit_code = getattr(result, "exit_code", None)
    output = getattr(result, "output", None)
    if exit_code is None and isinstance(result, tuple) and len(result) == 2:
        exit_code, output = result
    if exit_code != 0 or not isinstance(output, (bytes, bytearray)):
        return {}
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for line in bytes(output).decode("utf-8", errors="replace").splitlines():
        if line.startswith("@@"):
            current = line[2:]
            sections.setdefault(current, [])
        elif current is not None:
            sections[current].append(line)
    return {name: "\n".join(lines).strip() for name, lines in sections.items() if lines}


def _cgroup_scalar(files: dict[str, str], name: str) -> int | None:
    value = files.get(name)
    if value is None or value == "max":
        return None
    return _nonnegative_int(value)


def _cgroup_map(files: dict[str, str], name: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in files.get(name, "").splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        parsed = _nonnegative_int(parts[1])
        if parsed is not None:
            values[parts[0]] = parsed
    return values


def _cgroup_io(files: dict[str, str]) -> dict[str, int]:
    totals = {"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
    observed: set[str] = set()
    for line in files.get("io.stat", "").splitlines():
        for item in line.split()[1:]:
            key, separator, raw = item.partition("=")
            if not separator or key not in totals:
                continue
            parsed = _nonnegative_int(raw)
            if parsed is not None:
                totals[key] += parsed
                observed.add(key)
    return {key: value for key, value in totals.items() if key in observed}


def _reported(primary: int | None, fallback: int | None) -> int | None:
    return primary if primary is not None else fallback


def snapshot_from_docker_stats(
    stats: dict[str, Any],
    *,
    attrs: dict[str, Any],
    observed_at: datetime,
    cgroup_files: dict[str, str] | None = None,
) -> DriverResourceSnapshot:
    """Normalize Docker's cgroup-v1/v2 compatible stats response."""

    cpu = _dict_value(stats.get("cpu_stats"))
    usage = _dict_value(cpu.get("cpu_usage"))
    throttle = _dict_value(cpu.get("throttling_data"))
    memory = _dict_value(stats.get("memory_stats"))
    pids = _dict_value(stats.get("pids_stats"))
    state = _dict_value(attrs.get("State"))
    image = attrs.get("Image")
    image_digest = str(image) if isinstance(image, str) and image else None
    runtime_id = attrs.get("Id")
    if not isinstance(runtime_id, str) or not runtime_id:
        runtime_id = None
    cgroup = cgroup_files or {}
    cpu_cgroup = _cgroup_map(cgroup, "cpu.stat")
    memory_events = _cgroup_map(cgroup, "memory.events")
    io_cgroup = _cgroup_io(cgroup)
    oom_killed = state.get("OOMKilled") is True
    return DriverResourceSnapshot(
        observed_at=observed_at,
        source="docker_stats",
        runtime_id=runtime_id,
        image_digest=image_digest,
        container_started_at=_parse_docker_timestamp(state.get("StartedAt")),
        cpu_usage_usec=_reported(
            cpu_cgroup.get("usage_usec"),
            _nanoseconds_to_microseconds(usage.get("total_usage")),
        ),
        cpu_user_usec=_reported(
            cpu_cgroup.get("user_usec"),
            _nanoseconds_to_microseconds(usage.get("usage_in_usermode")),
        ),
        cpu_system_usec=_reported(
            cpu_cgroup.get("system_usec"),
            _nanoseconds_to_microseconds(usage.get("usage_in_kernelmode")),
        ),
        cpu_throttled_usec=_reported(
            cpu_cgroup.get("throttled_usec"),
            _nanoseconds_to_microseconds(throttle.get("throttled_time")),
        ),
        cpu_periods=_reported(
            cpu_cgroup.get("nr_periods"),
            _nonnegative_int(throttle.get("periods")),
        ),
        cpu_throttled_periods=_reported(
            cpu_cgroup.get("nr_throttled"),
            _nonnegative_int(throttle.get("throttled_periods")),
        ),
        memory_current_bytes=_reported(
            _cgroup_scalar(cgroup, "memory.current"),
            _nonnegative_int(memory.get("usage")),
        ),
        memory_peak_bytes=_reported(
            _cgroup_scalar(cgroup, "memory.peak"),
            _nonnegative_int(memory.get("max_usage")),
        ),
        memory_events_low=memory_events.get("low"),
        memory_events_high=memory_events.get("high"),
        memory_events_max=memory_events.get("max"),
        memory_events_oom=(
            max(memory_events.get("oom", 0), 1) if oom_killed else memory_events.get("oom")
        ),
        memory_events_oom_kill=(
            max(memory_events.get("oom_kill", 0), 1)
            if oom_killed
            else memory_events.get("oom_kill")
        ),
        pids_current=_reported(
            _cgroup_scalar(cgroup, "pids.current"),
            _nonnegative_int(pids.get("current")),
        ),
        pids_peak=_cgroup_scalar(cgroup, "pids.peak"),
        io_read_bytes=io_cgroup.get("rbytes")
        if "rbytes" in io_cgroup
        else _blkio_total(stats, "io_service_bytes_recursive", "read"),
        io_write_bytes=io_cgroup.get("wbytes")
        if "wbytes" in io_cgroup
        else _blkio_total(stats, "io_service_bytes_recursive", "write"),
        io_read_ops=io_cgroup.get("rios")
        if "rios" in io_cgroup
        else _blkio_total(stats, "io_serviced_recursive", "read"),
        io_write_ops=io_cgroup.get("wios")
        if "wios" in io_cgroup
        else _blkio_total(stats, "io_serviced_recursive", "write"),
    )


@dataclass
class DockerDriver:
    """Real-Docker Driver. Uses docker-py SDK throughout."""

    image: str
    workspace: PurePosixPath = field(default_factory=lambda: PurePosixPath("/workspace"))
    container_name: str | None = None
    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"
    network_policy_baseline: NetworkPolicy = field(default_factory=Public)
    docker_api_timeout_sec: int | None = None
    _client: Any | None = field(default=None, init=False, repr=False)
    _container: Any | None = field(default=None, init=False, repr=False)
    _state: str = field(default="constructed", init=False)
    _stream_readers: dict[asyncio.Task[None], Callable[[], None]] = field(
        default_factory=dict, init=False, repr=False,
    )

    async def start(self, *, options: StartOptions | None = None) -> None:
        # Spec §2.2: start() at most once. Reject when running OR stopped.
        if self._state != "constructed":
            raise DriverAlreadyStartedError(
                f"DockerDriver.start() rejected in state {self._state!r}",
            )

        opts = options or StartOptions()
        try:
            self._client = (
                docker.from_env()
                if self.docker_api_timeout_sec is None
                else docker.from_env(timeout=self.docker_api_timeout_sec)
            )
            await asyncio.to_thread(self._ensure_image, opts)

            run_kwargs: dict[str, Any] = dict(
                command=_KEEPALIVE_CMD,
                name=self.container_name,
                detach=True,
                tty=False,
                stdin_open=False,
                working_dir=str(self.workspace),
                remove=False,
                # NET_ADMIN lets the workload modify iptables inside its own
                # netns. Bounded to the container's network namespace — host
                # iptables is unaffected. Required for Allowlist + NoNetwork.
                cap_add=["NET_ADMIN"],
            )
            if opts.network is not None:
                # Phase B (#188): attach to a specific docker network
                # at create time. With sandbox isolation on, this is
                # the per-trial `--internal` bridge from #189, which
                # blocks all host egress — the singleton (PR-B2 #221)
                # is the only attached endpoint the sandbox can reach.
                run_kwargs["network"] = opts.network
            if opts.volumes:
                # Phase D (#78 PR-D1): bind-mounts. Worker uses this
                # for the rotating step-JWT + loom-CA cert dir, so
                # the sandbox sees a stable /run/loom/ path that the
                # rotator atomically refreshes from the outside.
                # docker-py format: {host: {'bind': container,
                # 'mode': mode}}.
                run_kwargs["volumes"] = {
                    host: {"bind": container, "mode": mode}
                    for host, container, mode in opts.volumes
                }
            if opts.environment:
                run_kwargs["environment"] = dict(opts.environment)
            if opts.extra_hosts:
                run_kwargs["extra_hosts"] = dict(opts.extra_hosts)
            if opts.dns:
                run_kwargs["dns"] = list(opts.dns)
            if opts.tmpfs:
                run_kwargs["tmpfs"] = _tmpfs_specs_to_docker_map(opts.tmpfs)
            if opts.labels:
                run_kwargs["labels"] = dict(opts.labels)
            # Per-sandbox benchmark limits (#853) compose with #896 per-container
            # packed-worker caps; when both bound cpu/memory the most restrictive
            # value wins. Each is preserved in its own format when the other is
            # unset so both driver contracts stay intact.
            if opts.cpus is not None and opts.container_cpus > 0:
                run_kwargs["nano_cpus"] = min(
                    int(opts.cpus * 1_000_000_000),
                    int(opts.container_cpus * 1_000_000_000),
                )
            elif opts.cpus is not None:
                run_kwargs["nano_cpus"] = int(opts.cpus * 1_000_000_000)
            elif opts.container_cpus > 0:
                run_kwargs["nano_cpus"] = int(opts.container_cpus * 1_000_000_000)
            if opts.memory_mb is not None and opts.container_memory_mib > 0:
                run_kwargs["mem_limit"] = min(
                    opts.memory_mb * 1024 * 1024,
                    opts.container_memory_mib * 1024 * 1024,
                )
            elif opts.memory_mb is not None:
                run_kwargs["mem_limit"] = f"{opts.memory_mb}m"
            elif opts.container_memory_mib > 0:
                run_kwargs["mem_limit"] = opts.container_memory_mib * 1024 * 1024
            # ``storage_mb`` remains part of imported benchmark metadata and
            # service-execution planning, but the Docker backend deliberately
            # does not translate it to HostConfig.StorageOpt.  Docker only
            # supports writable-layer sizing for selected storage-driver /
            # backing-filesystem combinations (for example overlay2 on XFS
            # with project quotas); sending it unconditionally makes otherwise
            # compatible workers fail during container creation.  Host-level
            # free-space admission and cleanup remain the Docker worker's disk
            # safety boundary.
            if opts.container_pids > 0:
                run_kwargs["pids_limit"] = opts.container_pids
            if opts.cgroup_parent is not None:
                run_kwargs["cgroup_parent"] = _validated_cgroup_parent(
                    opts.cgroup_parent,
                )
            if opts.gpus:
                run_kwargs["device_requests"] = [
                    docker.types.DeviceRequest(
                        count=opts.gpus,
                        capabilities=[["gpu"]],
                    )
                ]
            # docker-py's high-level containers.run() creates before it starts,
            # but it does not return the Container if start() raises. Split the
            # steps so the failure cleanup path can remove Created containers.
            client = self._client
            assert client is not None

            create_kwargs = dict(run_kwargs)
            create_kwargs.pop("remove", None)

            def _create_container() -> Any:
                return client.containers.create(self.image, **create_kwargs)

            container = await asyncio.to_thread(_create_container)
            self._container = container
            await asyncio.to_thread(container.start)
            await self._wait_until_running()
            # Mark running BEFORE applying baseline policy —
            # set_network_policy() calls _require_running() which checks state.
            self._state = "running"
            await self.set_network_policy(self.network_policy_baseline)
        except BaseException:
            # Any failure during start() must NOT leak the partially-created
            # container/client. Self-clean before re-raising so the caller
            # isn't obligated to also call stop() on a failed start.
            with contextlib.suppress(Exception):
                await self._teardown(delete=True)
            self._state = "stopped"
            raise

    def _ensure_image(self, opts: StartOptions) -> None:
        assert self._client is not None
        try:
            self._client.images.get(self.image)
            if not opts.force_build:
                return
        except ImageNotFound:
            pass
        if opts.pull:
            self._client.images.pull(self.image)

    async def _wait_until_running(self, timeout_sec: float = 10.0) -> None:
        assert self._container is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec
        while loop.time() < deadline:
            await asyncio.to_thread(self._container.reload)
            if self._container.status == "running":
                return
            await asyncio.sleep(0.1)
        raise DriverError(
            f"Container failed to reach 'running' within {timeout_sec}s; "
            f"final status={self._container.status!r}",
        )

    async def stop(self, *, delete: bool = True) -> None:
        # Idempotent. Only running→stopped is a real transition; calling stop()
        # from 'constructed' leaves state intact so start() can still fire.
        await self._teardown(delete=delete)

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        """Read one bounded Docker stats observation before container removal."""

        container = self._container
        if container is None or self._state != "running":
            return None
        stats = await asyncio.to_thread(container.stats, stream=False, one_shot=True)
        cgroup_files = await asyncio.to_thread(read_container_cgroup_v2_files, container)
        with contextlib.suppress(Exception):
            await asyncio.to_thread(container.reload)
        attrs = container.attrs if isinstance(container.attrs, dict) else {}
        return snapshot_from_docker_stats(
            stats if isinstance(stats, dict) else {},
            attrs=attrs,
            observed_at=datetime.now(UTC),
            cgroup_files=cgroup_files,
        )

    async def _teardown(self, *, delete: bool) -> None:
        """Shared docker-resource cleanup used by stop() and start()'s failure
        path. Each step has its own suppress so a stop() error doesn't skip
        the remove() — the plan-shown stop() bundled them and leaked
        stopped-but-not-removed containers when stop() raised.

        When delete=True we skip the graceful `stop(timeout=10)` because
        `remove(force=True)` SIGKILLs running containers in one step. The
        graceful stop is only meaningful when delete=False (operator
        wants to keep the stopped container around for inspection).
        Skipping it saves ~10s per test in the docker-tier suite.
        """
        # Close admission before the first await. A late exec_start below also
        # checks this state before registering any reader or returning a handle.
        if self._state == "running":
            self._state = "stopped"
        readers = dict(self._stream_readers)
        for stop_reader in readers.values():
            stop_reader()
        await asyncio.gather(*readers, return_exceptions=True)
        if self._container is not None:
            if delete:
                with contextlib.suppress(APIError, NotFound):
                    await asyncio.to_thread(self._container.remove, force=True)
            else:
                with contextlib.suppress(APIError, NotFound):
                    await asyncio.to_thread(self._container.stop, timeout=10)
            self._container = None
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _require_running(self) -> None:
        if self._state != "running" or self._container is None:
            raise DriverNotStartedError(
                f"DockerDriver in state {self._state!r}",
            )

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        self._require_running()
        # Capture the container handle now. If stop() races in concurrently
        # while we're suspended in to_thread, self._container becomes None
        # but our local `container` is still valid for the in-flight call.
        container = self._container
        assert container is not None
        assert self._client is not None
        api = self._client.api

        exec_kwargs: dict[str, Any] = {
            "container": container.id,
            "cmd": ["/bin/sh", "-c", cmd],
            "stdout": True,
            "stderr": True,
            "tty": False,
        }
        if user is not None:
            exec_kwargs["user"] = str(user)
        if cwd is not None:
            exec_kwargs["workdir"] = str(cwd)
        if env is not None:
            exec_kwargs["environment"] = dict(env)

        loop = asyncio.get_running_loop()
        started = loop.time()
        cancelled = threading.Event()
        stream_lock = threading.Lock()
        active_stream: list[Any] = []

        def close_stream() -> None:
            with stream_lock:
                stream = active_stream.pop() if active_stream else None
            if stream is not None:
                with contextlib.suppress(Exception):
                    stream.close()

        def _sync() -> tuple[int, bytes, bytes, bool]:
            exec_id = api.exec_create(**exec_kwargs)["Id"]
            if cancelled.is_set():
                raise DriverError("command output capture cancelled")
            stream = api.exec_start(exec_id, stream=True, demux=True)
            with stream_lock:
                active_stream.append(stream)
            stdout, stderr = bytearray(), bytearray()
            truncated = False
            try:
                if cancelled.is_set():
                    raise DriverError("command output capture cancelled")
                for chunk in stream:
                    if cancelled.is_set():
                        raise DriverError("command output capture cancelled")
                    out, err = chunk if isinstance(chunk, tuple) else (chunk, None)
                    for target, data in ((stdout, out), (stderr, err)):
                        if data:
                            remaining = MAX_EXEC_STREAM_BYTES - len(target)
                            target.extend(data[:remaining])
                            truncated |= len(data) > remaining
                # Keep draining after the caps, so output pressure cannot block
                # the command or hide its actual exit code.
                info = api.exec_inspect(exec_id)
                return int(info["ExitCode"]), bytes(stdout), bytes(stderr), truncated
            finally:
                close_stream()

        try:
            if timeout_sec is not None:
                exit_code, stdout, stderr, truncated = await asyncio.wait_for(
                    asyncio.to_thread(_sync), timeout=timeout_sec,
                )
            else:
                exit_code, stdout, stderr, truncated = await asyncio.to_thread(_sync)
        except BaseException:
            cancelled.set()
            # Docker's CancellableStream closes its socket from another thread,
            # including if the command has gone silent after producing output.
            close_stream()
            raise

        duration = loop.time() - started

        return ExecResult(
            return_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            truncated=truncated,
            duration_sec=duration,
        )

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        self._require_running()
        assert self._client is not None
        assert self._container is not None
        api = self._client.api  # docker.APIClient — low-level
        # Capture the container id once so the _kill closure doesn't
        # re-read self._container (which may be None after stop()).
        container_id = self._container.id

        exec_create_kwargs: dict[str, Any] = {
            "container": container_id,
            "cmd": argv,
            "environment": [f"{k}={v}" for k, v in env_vars.items()],
            "workdir": str(cwd),
            "tty": False,
            "stdout": True,
            "stderr": True,
        }
        if user is not None:
            exec_create_kwargs["user"] = str(user)

        exec_info = await asyncio.to_thread(api.exec_create, **exec_create_kwargs)
        exec_id = exec_info["Id"]
        self._require_running()

        # `exec_start(stream=True, demux=True)` returns a SYNC generator of
        # (stdout_chunk, stderr_chunk) tuples (either side may be None for
        # a chunk that only carries the other stream).
        raw_stream = await asyncio.to_thread(
            api.exec_start,
            exec_id,
            stream=True,
            demux=True,
        )
        if self._state != "running":
            with contextlib.suppress(Exception):
                raw_stream.close()
            raise DriverNotStartedError("DockerDriver stopped during stream startup")

        loop = asyncio.get_running_loop()
        stdout_buffer = _DockerOutputBuffer(loop)
        stderr_buffer = _DockerOutputBuffer(loop)
        # Only explicit stop/kill/cancellation may discard unread output.
        # Process exit alone does not imply that the transport reached EOF.
        stop_reader = threading.Event()

        def _drain_blocking() -> None:
            try:
                for chunk in raw_stream:
                    if stop_reader.is_set():
                        break
                    out_chunk, err_chunk = chunk if isinstance(chunk, tuple) else (chunk, None)
                    if out_chunk and not stdout_buffer.put(out_chunk, stop_reader):
                        break
                    if err_chunk and not stderr_buffer.put(err_chunk, stop_reader):
                        break
            finally:
                with contextlib.suppress(Exception):
                    raw_stream.close()
                stdout_buffer.finish()
                stderr_buffer.finish()

        reader_task = asyncio.create_task(asyncio.to_thread(_drain_blocking))

        def _stop_output() -> None:
            stop_reader.set()
            with contextlib.suppress(Exception):
                raw_stream.close()
            # The executor job may still be queued. Its finally block cannot
            # be the sole owner of EOF when cancelling that queued job.
            stdout_buffer.finish()
            stderr_buffer.finish()
            if not reader_task.done():
                reader_task.cancel()

        self._stream_readers[reader_task] = _stop_output

        def _reader_done(task: asyncio.Task[None]) -> None:
            self._stream_readers.pop(task, None)
            if not task.cancelled():
                task.exception()  # Observe failures even if the handle is abandoned.

        reader_task.add_done_callback(_reader_done)

        async def _poll_exit() -> int:
            # EOF can arrive just before Docker publishes the final exit status.
            backoff = 0.05
            while True:
                info = await asyncio.to_thread(api.exec_inspect, exec_id)
                if not info.get("Running"):
                    return int(info.get("ExitCode") or 0)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 0.5)

        async def _wait() -> int:
            try:
                # Waiters share the completed reader without cancelling it on
                # success. Both concurrent and repeated wait() retain output.
                (reader_result,) = await asyncio.shield(
                    asyncio.gather(reader_task, return_exceptions=True),
                )
                if isinstance(reader_result, BaseException) and not (
                    isinstance(reader_result, asyncio.CancelledError) and stop_reader.is_set()
                ):
                    raise reader_result
                return await _poll_exit()
            except BaseException:
                _stop_output()
                await asyncio.gather(reader_task, return_exceptions=True)
                raise

        async def _kill() -> None:
            # docker exec has no direct kill API; exec_inspect's Pid field
            # is the HOST's view and is useless from inside the container's
            # PID namespace. Best-effort: pkill matching the joined argv.
            # May or may not succeed depending on whether `pkill -f` is
            # available (busybox without procps doesn't ship it). The
            # ExecHandle.kill() contract documents this as best-effort.
            try:
                target = " ".join(argv)
                killer = await asyncio.to_thread(
                    api.exec_create,
                    container=container_id,
                    cmd=["sh", "-c", f"pkill -9 -f {target!r} 2>/dev/null; true"],
                )
                await asyncio.to_thread(api.exec_start, killer["Id"], detach=True)
            except (APIError, NotFound):
                pass
            finally:
                _stop_output()
                await asyncio.gather(reader_task, return_exceptions=True)

        return ExecHandle(
            pid=0,  # docker exec doesn't surface a host-side PID we trust
            stdout=stdout_buffer.drain(),
            stderr=stderr_buffer.drain(),
            _wait=_wait,
            _kill=_kill,
        )

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._require_running()
        container = self._container
        assert container is not None
        if not src.is_file():
            raise FileNotFoundError(f"upload source {src} is not a regular file")

        def _build_tar() -> bytes:
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tf:
                info = tarfile.TarInfo(name=dst.name)
                data = src.read_bytes()
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            return buf.getvalue()

        tar_bytes = await asyncio.to_thread(_build_tar)

        # Ensure parent dir exists in the container.
        await self.exec(f"mkdir -p {shlex.quote(dst.parent.as_posix())}", user="root")

        def _put() -> None:
            ok = container.put_archive(path=str(dst.parent), data=tar_bytes)
            if not ok:
                raise DriverError(f"put_archive returned False for {dst}")

        await asyncio.to_thread(_put)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        self._require_running()
        container = self._container
        assert container is not None

        def _get() -> bytes:
            try:
                stream, _stat = container.get_archive(str(src))
            except NotFound as exc:
                raise FileNotFoundError(f"{src} not found in container") from exc
            return b"".join(stream)

        data = await asyncio.to_thread(_get)

        def _extract() -> None:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
                # File members only — get_archive on a directory returns the
                # whole subtree as a tar with many entries. The Driver spec
                # is single-file; refuse silently-lossy multi-member extracts.
                file_members = [m for m in tf.getmembers() if m.isfile()]
                if not file_members:
                    raise DriverError(f"no regular files in tarball for {src}")
                if len(file_members) > 1:
                    raise DriverError(
                        f"{src} is a directory (tarball had {len(file_members)} "
                        f"files); Driver.download is single-file only",
                    )
                fobj = tf.extractfile(file_members[0])
                if fobj is None:
                    raise DriverError(f"could not extract {src} from tarball")
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(fobj.read())

        await asyncio.to_thread(_extract)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        self._require_running()
        plan = compute_iptables_rules(policy)
        cmds = render_iptables_commands(plan)
        if not cmds:
            # Public-equivalent: no enforcement needed. Skip the exec so
            # containers without iptables installed still work.
            self.network_policy_baseline = policy
            return
        script = " && ".join(cmds)
        r = await self.exec(script, user="root", timeout_sec=30)
        if r.return_code != 0:
            raise DriverError(
                f"network policy apply failed: rc={r.return_code} stderr={r.stderr[:512]!r}",
            )
        self.network_policy_baseline = policy

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        self._require_running()
        if hc is None:
            return

        loop = asyncio.get_running_loop()
        deadline_start_period = loop.time() + hc.start_period_sec
        consecutive_failures = 0
        while True:
            in_grace = loop.time() < deadline_start_period
            try:
                r = await self.exec(hc.command, timeout_sec=hc.timeout_sec)
                if r.return_code == 0:
                    return
                logger.debug("healthcheck non-zero exit", extra={"rc": r.return_code})
            except TimeoutError:
                logger.debug("healthcheck timed out")
            if not in_grace:
                consecutive_failures += 1
                if consecutive_failures > hc.retries:
                    raise DriverError(
                        f"Healthcheck failed after {hc.retries} consecutive "
                        f"retries: {hc.command!r}",
                    )
            await asyncio.sleep(hc.interval_sec)
