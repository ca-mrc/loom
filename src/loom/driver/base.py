"""Driver Protocol — what every sandbox backend implements (spec §2.2).

The Protocol is intentionally minimal: 9 async methods + 2 declared attributes.
New backends target ≤300 LOC; FakeDriver and DockerDriver are reference impls.

Constants:
- MAX_EXEC_STREAM_BYTES : hard cap on stdout/stderr per `exec()` call (10 MB).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy
from loom.models.types import OS

MAX_EXEC_STREAM_BYTES: int = 10 * 1024 * 1024  # 10 MB; spec §2.2 + §4.9


@dataclass(frozen=True)
class StartOptions:
    """Options for `Driver.start()`."""

    force_build: bool = False
    pull: bool = True
    # Phase B (#78 / #188): attach the container to a specific docker
    # network on creation. None means "default network" (current
    # behavior). Set to the per-trial sandbox bridge name when sandbox
    # isolation is on. Honored only by drivers whose
    # `capabilities.supports_custom_network` is True.
    network: str | None = None
    # Phase D (#78 PR-D1): tuple of (host_path, container_path,
    # mode) bind mounts to apply at container creation. Used by the
    # worker to expose the rotating step-JWT + loom-CA cert to the
    # sandbox under /run/loom without baking them into the image.
    # Frozen-tuple shape so StartOptions stays hashable + the
    # dataclass-frozen contract holds.
    volumes: tuple[tuple[str, str, str], ...] = ()
    # Environment variables to set on the primary sandbox container at
    # create time. Tuple form preserves StartOptions' frozen/hashable shape.
    environment: tuple[tuple[str, str], ...] = ()
    # Hostname mappings to inject at container creation. DockerDriver
    # passes this through to docker-py's `extra_hosts`; workers use it
    # to make sandbox-facing gateway URLs such as `host.docker.internal`
    # resolve on Linux Docker.
    extra_hosts: tuple[tuple[str, str], ...] = ()
    # DNS servers for the primary sandbox container. This preserves benchmark
    # task definitions that intentionally alter resolver behavior.
    dns: tuple[str, ...] = ()
    # Docker tmpfs mount specs, in docker-compose form such as
    # "/root:size=100M,mode=755" or just "/run".
    tmpfs: tuple[str, ...] = ()
    # Docker labels applied to the primary sandbox container. Workers use this
    # for operator-safe inspection/reaping of Loom-owned containers.
    labels: tuple[tuple[str, str], ...] = ()
    # Optional per-sandbox compute limits. Drivers must either enforce non-None
    # CPU/memory values or fail closed. ``storage_mb`` is retained as benchmark
    # metadata and for service-execution planning; the Docker backend does not
    # map it to the non-portable HostConfig.StorageOpt API.
    cpus: float | None = None
    memory_mb: int | None = None
    storage_mb: int | None = None
    gpus: int = 0
    # #896: per-container hard resource caps for non-exclusive (packed) workers.
    # 0 means unbounded (default, current behavior). DockerDriver maps these to
    # nano_cpus / mem_limit / pids_limit at container create so an escaped trial
    # container cannot consume unbounded CPU/RAM/PIDs and starve co-tenants
    # (k3s/MinIO/Longhorn) on a shared double-duty node. These compose with the
    # per-sandbox limits above: when both bound the same knob, the most
    # restrictive value wins.
    container_cpus: float = 0.0
    container_memory_mib: int = 0
    container_pids: int = 0
    # Exact host cgroup v2 parent for Docker HostConfig.CgroupParent.
    # None uses Docker's default parent for local execution.
    cgroup_parent: str | None = None


@dataclass(frozen=True)
class DriverResourceSnapshot:
    """Backend-neutral instantaneous/cumulative resource observation.

    ``None`` means the backend did not report the field. It never means zero.
    Runtime identities are hashed before crossing the worker boundary.
    """

    observed_at: datetime
    source: str
    runtime_id: str | None = None
    image_digest: str | None = None
    container_started_at: datetime | None = None
    cpu_usage_usec: int | None = None
    cpu_user_usec: int | None = None
    cpu_system_usec: int | None = None
    cpu_throttled_usec: int | None = None
    cpu_periods: int | None = None
    cpu_throttled_periods: int | None = None
    memory_current_bytes: int | None = None
    memory_peak_bytes: int | None = None
    memory_events_low: int | None = None
    memory_events_high: int | None = None
    memory_events_max: int | None = None
    memory_events_oom: int | None = None
    memory_events_oom_kill: int | None = None
    pids_current: int | None = None
    pids_peak: int | None = None
    io_read_bytes: int | None = None
    io_write_bytes: int | None = None
    io_read_ops: int | None = None
    io_write_ops: int | None = None


@dataclass
class ExecHandle:
    """Long-running process handle returned by Driver.exec_streaming.

    Caller drains stdout/stderr concurrently (async; chunks of any size), then
    `await handle.wait()` for the exit code. Transport buffers are bounded;
    slow consumers apply backpressure. Stream length has no 10 MB cap.
    """

    pid: int
    stdout: AsyncIterator[bytes]
    stderr: AsyncIterator[bytes]
    _wait: Callable[[], Awaitable[int]]
    _kill: Callable[[], Awaitable[None]]

    async def wait(self) -> int:
        return await self._wait()

    async def kill(self) -> None:
        """Best-effort signal to the running process. Implementations
        SHOULD make a reasonable attempt to terminate the process; they
        do NOT guarantee a fast exit. Callers wanting a hard deadline
        should wrap `wait()` in `asyncio.wait_for(..., timeout=...)` and
        accept that `wait()` may not resolve before the deadline expires.
        For docker-backed drivers, killing a docker exec across PID
        namespaces is unreliable — the cleanest path to bounded execution
        is a `timeout` flag inside the agent command itself."""
        await self._kill()


@runtime_checkable
class Driver(Protocol):
    """Sandbox execution surface. See spec §2.2.

    Lifecycle contract (spec §2.2 addendum):
    - `start()` may be called at most once per instance. Re-`start()` raises
      `DriverAlreadyStartedError`.
    - `stop()` MUST be idempotent and safe to call:
      (a) before `start()` ever succeeded, (b) after `start()` succeeded,
      (c) multiple times.
    - `exec`/`upload`/`download` MUST raise `DriverNotStartedError` if called
      before `start()` or after `stop()`.
    """

    capabilities: Capabilities
    os: OS

    async def start(self, *, options: StartOptions | None = None) -> None: ...

    async def stop(self, *, delete: bool = True) -> None: ...

    async def resource_snapshot(self) -> DriverResourceSnapshot | None: ...

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult: ...

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        """Start a process and return immediately. Caller iterates stdout/
        stderr (each yield is whatever chunk size the underlying driver
        produces), then `await handle.wait()` for the exit code. Driver
        buffers nothing — no 10 MB cap. Used for long-running agent
        subprocesses; existing `exec()` stays for short commands.

        `env_vars` are MERGED with whatever the container already has.
        """
        ...

    async def upload(self, src: Path, dst: PurePosixPath) -> None: ...

    async def download(self, src: PurePosixPath, dst: Path) -> None: ...

    async def set_network_policy(self, policy: NetworkPolicy) -> None: ...

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None: ...
