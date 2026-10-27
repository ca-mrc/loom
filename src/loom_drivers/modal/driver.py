"""ModalDriver — Loom Driver Protocol over Modal Sandbox.

Mirrors ``DockerDriver``'s state-machine contract:
  - ``start()`` at most once (raises ``DriverAlreadyStartedError`` on re-call).
  - ``stop()`` is idempotent and safe in any state.
  - ``exec`` / ``upload`` / ``download`` raise ``DriverNotStartedError`` unless
    running.

All modal SDK calls go through :class:`loom_drivers.modal.client.ModalClient`
(sync→async bridge). The driver never imports ``modal`` directly.

Network policy is applied AT CREATE TIME via ``block_network`` /
``outbound_cidr_allowlist``. After start, calls to ``set_network_policy``
either no-op (policy matches baseline) or raise — Modal does not support
hot-mutating sandbox egress. The cross-driver consistency test (Task 13)
uses Public policy across all backends to avoid the divergence.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import logging
import shlex
import signal
import sys
import weakref
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import FrameType
from typing import Any

from loom.driver.base import (
    MAX_EXEC_STREAM_BYTES,
    DriverResourceSnapshot,
    ExecHandle,
    StartOptions,
)
from loom.driver.healthcheck import run_command_healthcheck
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import (
    Allowlist,
    NetworkPolicy,
    NoNetwork,
    Public,
)
from loom.models.types import OS
from loom_drivers.modal.client import ModalClient
from loom_drivers.modal.config import ModalConfig
from loom_drivers.modal.exec_stream import make_exec_handle
from loom_drivers.modal.gpu import MODAL_GPU_TYPES, validate_gpu
from loom_drivers.modal.images import ModalImageCache

logger = logging.getLogger(__name__)


def _default_caps() -> Capabilities:
    # `allowlist` is intentionally absent: Loom's Allowlist requires
    # at least one domain (min_length=1) and Modal only enforces CIDR
    # ranges, so the driver raises DriverError on Allowlist at start.
    # Advertising the policy would cause the scheduler to dispatch
    # allowlist-requiring trials here and only fail at worker time.
    return Capabilities(
        backend="modal",
        os="linux",
        gpu_vendor="nvidia",
        network_policies=frozenset(["public", "no-network"]),
        dynamic_network_policy=False,  # Modal is create-time-only
        mounted_fs=False,
        resource_modes=frozenset(["auto"]),
        gpu_types=MODAL_GPU_TYPES,
    )


def _policy_to_modal_kwargs(
    policy: NetworkPolicy,
) -> tuple[bool, list[str] | None]:
    """Translate a ``NetworkPolicy`` into ``(block_network, outbound_cidr_allowlist)``."""
    if isinstance(policy, Public):
        return False, None
    if isinstance(policy, NoNetwork):
        return True, None
    if isinstance(policy, Allowlist):
        # Modal accepts CIDR ranges only. Loom's Allowlist requires at
        # least one domain (min_length=1); we refuse domain-based
        # allowlists at create-time rather than silently resolving DNS
        # (which races as TTLs expire). Callers can use Public or
        # NoNetwork instead — or run --backend docker for hostname
        # allowlists.
        raise DriverError(
            "ModalDriver: domain-based Allowlist not supported "
            "(Modal accepts CIDR ranges only). Use Public/NoNetwork "
            "or run under --backend docker for hostname allowlists.",
        )
    raise DriverError(f"ModalDriver: unsupported network policy {policy!r}")


_LIVE_DRIVERS: weakref.WeakSet[ModalDriver] = weakref.WeakSet()
_HANDLERS_INSTALLED = False
_PRIOR_SIGINT: Any = None
_PRIOR_SIGTERM: Any = None


def _atexit_cleanup() -> None:
    """Synchronously terminate any live Modal sandboxes.

    Runs in the interpreter shutdown thread; no event loop available, so
    we call the SYNC modal API directly here (bypassing ModalClient's
    async wrappers) — that is the entire reason this exists.
    """
    for drv in list(_LIVE_DRIVERS):
        sb = drv._sandbox
        if sb is None:
            continue
        try:
            sb.terminate(wait=False)
        except Exception as exc:
            logger.warning("atexit terminate sandbox failed: %s", exc)
        drv._sandbox = None
        drv._state = "stopped"


def _signal_cleanup(signum: int, frame: FrameType | None) -> None:
    _atexit_cleanup()
    if signum == signal.SIGINT and callable(_PRIOR_SIGINT):
        _PRIOR_SIGINT(signum, frame)
        return
    if signum == signal.SIGTERM and callable(_PRIOR_SIGTERM):
        _PRIOR_SIGTERM(signum, frame)
        return
    if signum == signal.SIGINT:
        # No prior callable handler (SIG_DFL / SIG_IGN are ints, not
        # callables); honor the default Ctrl-C behavior.
        raise KeyboardInterrupt
    # SIGTERM with no prior callable handler. Default SIGTERM action is
    # to exit with status 128 + 15. If we just returned here, the
    # process would keep running after cleanup — silently swallowing
    # the signal. Exit explicitly.
    sys.exit(128 + signal.SIGTERM)


def _install_handlers_once() -> None:
    global _HANDLERS_INSTALLED, _PRIOR_SIGINT, _PRIOR_SIGTERM
    if _HANDLERS_INSTALLED:
        return
    atexit.register(_atexit_cleanup)
    try:
        _PRIOR_SIGINT = signal.signal(signal.SIGINT, _signal_cleanup)
        _PRIOR_SIGTERM = signal.signal(signal.SIGTERM, _signal_cleanup)
    except ValueError:
        # signal.signal only works on the main thread; atexit alone
        # covers normal interpreter shutdown when off-main.
        pass
    _HANDLERS_INSTALLED = True


@dataclass(eq=False)
class ModalDriver:
    """Driver implementation against Modal Sandbox.

    ``eq=False`` keeps the default identity-based ``__hash__`` so instances
    can live in the module-level ``_LIVE_DRIVERS`` WeakSet for cleanup.
    """

    image: str
    config: ModalConfig
    workspace: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )
    gpu: str | None = None
    cpu: float | None = None
    memory_mb: int | None = None
    env: dict[str, str] = field(default_factory=dict)
    pip_packages: list[str] = field(default_factory=list)
    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"
    network_policy_baseline: NetworkPolicy = field(default_factory=Public)

    _client: ModalClient | None = field(default=None, init=False, repr=False)
    _image_cache: ModalImageCache | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _sandbox: Any | None = field(default=None, init=False, repr=False)
    _app: Any | None = field(default=None, init=False, repr=False)
    _state: str = field(default="constructed", init=False)
    _applied_policy: NetworkPolicy | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        # Validate eagerly so callers see ModalGPUError at construction,
        # not at start() time.
        self.gpu = validate_gpu(self.gpu)

    async def start(self, *, options: StartOptions | None = None) -> None:
        if self._state != "constructed":
            raise DriverAlreadyStartedError(
                f"ModalDriver.start() rejected in state {self._state!r}",
            )
        opts = options or StartOptions()
        if (
            any(value is not None for value in (opts.cpus, opts.memory_mb, opts.storage_mb))
            or opts.gpus
        ):
            raise DriverError(
                "ModalDriver cannot enforce task resource limits through the "
                "generic StartOptions contract; use a compatible backend instead",
            )
        _install_handlers_once()
        block, allowlist = _policy_to_modal_kwargs(
            self.network_policy_baseline,
        )

        self._client = ModalClient(self.config)
        self._image_cache = ModalImageCache(self._client)
        try:
            self._app = await self._client.lookup_app(self.config.app_name)
            image = await self._image_cache.get(
                base=self.image,
                pip_packages=self.pip_packages or None,
            )
            self._sandbox = await self._client.create_sandbox(
                app=self._app,
                image=image,
                timeout_sec=self.config.sandbox_timeout_sec,
                gpu=self.gpu,
                block_network=block,
                outbound_cidr_allowlist=allowlist,
                workdir=str(self.workspace),
                env=dict(self.env),
            )
            self._state = "running"
            self._applied_policy = self.network_policy_baseline
            _LIVE_DRIVERS.add(self)
            logger.info(
                "ModalDriver started",
                extra={
                    "sandbox_id": getattr(self._sandbox, "object_id", None),
                    "image": self.image,
                    "gpu": self.gpu,
                },
            )
        except BaseException:
            # Self-clean on partial-start failure; caller need not call stop().
            await self._teardown()
            self._state = "stopped"
            raise

    async def stop(self, *, delete: bool = True) -> None:
        await self._teardown()
        if self._state == "running":
            self._state = "stopped"
        _LIVE_DRIVERS.discard(self)

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        # Modal does not currently expose a stable per-sandbox cgroup stats
        # contract through this adapter. Persist typed unavailability upstream.
        return None

    async def _teardown(self) -> None:
        if self._sandbox is not None and self._client is not None:
            try:
                await self._client.terminate_sandbox(
                    self._sandbox,
                    wait=True,
                )
            except Exception as exc:
                logger.warning(
                    "ModalDriver teardown: terminate raised %s",
                    exc,
                )
        self._sandbox = None
        self._app = None
        self._client = None
        self._image_cache = None

    def _require_running(self) -> None:
        if self._state != "running" or self._sandbox is None:
            raise DriverNotStartedError(
                f"ModalDriver in state {self._state!r}",
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
        assert self._client is not None and self._sandbox is not None

        loop = asyncio.get_running_loop()
        started = loop.time()
        argv: list[str] = ["/bin/sh", "-c", cmd]
        if user is not None:
            argv = ["su", "-", str(user), "-c", cmd]

        proc = await self._client.exec_sandbox(
            self._sandbox,
            argv,
            workdir=str(cwd) if cwd is not None else None,
            env=dict(env) if env is not None else None,
        )

        def _drain_to_bytes() -> tuple[int, bytes, bytes]:
            out = proc.stdout.read()
            err = proc.stderr.read()
            rc = proc.wait()
            if isinstance(out, str):
                out_b = out.encode("utf-8", errors="surrogateescape")
            else:
                out_b = out or b""
            if isinstance(err, str):
                err_b = err.encode("utf-8", errors="surrogateescape")
            else:
                err_b = err or b""
            return int(rc), out_b, err_b

        if timeout_sec is not None:
            try:
                rc, stdout, stderr = await asyncio.wait_for(
                    asyncio.to_thread(_drain_to_bytes),
                    timeout=timeout_sec,
                )
            except TimeoutError:
                # asyncio.wait_for cancels the awaiter but the worker
                # thread keeps blocking inside proc.stdout.read() /
                # proc.wait(). Terminate the underlying ContainerProcess
                # so the sandbox isn't left running (and racking up
                # billed seconds) until Modal's sandbox-wide cap fires.
                try:
                    await asyncio.to_thread(proc.terminate)
                except Exception as exc:
                    # Best-effort cleanup; even an exception here
                    # shouldn't swallow the TimeoutError we're about
                    # to re-raise below.
                    logger.warning(
                        "ModalDriver.exec: proc.terminate() raised after timeout: %s",
                        exc,
                    )
                raise
        else:
            rc, stdout, stderr = await asyncio.to_thread(_drain_to_bytes)
        duration = loop.time() - started

        truncated = False
        if len(stdout) > MAX_EXEC_STREAM_BYTES:
            stdout = stdout[:MAX_EXEC_STREAM_BYTES]
            truncated = True
        if len(stderr) > MAX_EXEC_STREAM_BYTES:
            stderr = stderr[:MAX_EXEC_STREAM_BYTES]
            truncated = True

        return ExecResult(
            return_code=rc,
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
        assert self._client is not None and self._sandbox is not None

        real_argv = argv
        if user is not None:
            real_argv = ["su", "-", str(user), "-c", " ".join(argv)]

        proc = await self._client.exec_sandbox(
            self._sandbox,
            real_argv,
            workdir=str(cwd),
            env=dict(env_vars),
        )
        return make_exec_handle(proc)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._require_running()
        if not src.is_file():
            raise FileNotFoundError(
                f"upload source {src} is not a regular file",
            )
        # Modal sandbox has no direct put_archive. Encode + write via exec.
        # Small files only — for larger payloads use a NetworkFileSystem.
        # Paths are shlex.quote()'d to prevent caller-supplied paths
        # (with spaces, quotes, or shell metacharacters) from breaking
        # out of the redirect. The base64 alphabet is shell-safe so the
        # payload itself doesn't need quoting beyond the single-quote
        # wrapping.
        data = src.read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        dst_q = shlex.quote(dst.as_posix())
        parent_q = shlex.quote(dst.parent.as_posix())
        await self.exec(f"mkdir -p {parent_q}", user=None)
        write_cmd = f"printf '%s' '{b64}' | base64 -d > {dst_q}"
        r = await self.exec(write_cmd, user=None)
        if r.return_code != 0:
            raise DriverError(
                f"upload failed rc={r.return_code} stderr={r.stderr[:512]!r}",
            )

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        self._require_running()
        src_q = shlex.quote(src.as_posix())
        r = await self.exec(f"base64 -w0 {src_q}", user=None)
        if r.return_code != 0:
            raise FileNotFoundError(
                f"download {src}: rc={r.return_code} stderr={r.stderr[:256]!r}",
            )
        data = base64.b64decode(r.stdout.strip())
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        self._require_running()
        # Modal sandbox network policy is create-time only. No-op if the
        # new policy structurally equals what we applied at start();
        # otherwise raise so callers see an honest error.
        if (
            self._applied_policy is not None
            and type(policy) is type(self._applied_policy)
            and policy == self._applied_policy
        ):
            return
        raise DriverError(
            "ModalDriver.set_network_policy: Modal sandbox network is "
            "create-time only. Pass the desired policy as "
            "ModalDriver(network_policy_baseline=...) before start(), "
            "or stop() and restart with the new policy.",
        )

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        self._require_running()
        if hc is None:
            return
        await run_command_healthcheck(self, hc)

    async def billed_seconds(self) -> float:
        """Wall-clock seconds Modal billed for this sandbox so far.

        Used by the cost reporter (Task 11) — NOT part of the Driver
        Protocol. The cost reporter dispatches on backend type.
        """
        if self._client is None or self._sandbox is None:
            return 0.0
        return await self._client.sandbox_billed_seconds(self._sandbox)
