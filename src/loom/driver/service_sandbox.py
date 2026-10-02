"""Driver for an already scheduled, isolated Linux task or verifier container.

Only a dedicated Unix socket is exposed to the trusted agent. Kubernetes owns
container lifetime, resources and network policy; this adapter cannot change
them. It reuses the ordinary Harbor environment and workspace snapshot flow.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import re
import shlex
import stat
import tempfile
from collections.abc import AsyncIterator, Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import httpx

from loom.driver.base import DriverResourceSnapshot, ExecHandle, StartOptions
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy
from loom.models.types import OS

if TYPE_CHECKING:
    from loom.trial.workspace import WorkspaceStagingPolicy

_RPC_OPERATIONS = {"/health": "health", "/exec": "exec", "/file": "file_transfer",
                   "/stop-processes": "stop_processes", "/pause-processes": "pause_processes",
                   "/resume-processes": "resume_processes", "/restore-directory": "directory_restore"}
_CLEANUP_REASONS = frozenset({
    "pid_namespace_invalid", "process_owner_mismatch", "process_inspection_failed",
    "cleanup_timeout", "cleanup_signal_denied", "cleanup_signal_failed",
    "cleanup_cancelled", "cleanup_failed",
})
_EXEC_REASONS = frozenset({
    "exec_request_invalid", "exec_user_mismatch", "exec_timeout_invalid",
    "exec_environment_invalid",
})
_PROCESS_REASONS = _EXEC_REASONS | {"process_limit_reached", "process_offset_unavailable"}
# Long-poll budget per process status/output request; the server caps it at 10s.
_PROCESS_POLL_MS = 5000
_PROCESS_DIAGNOSTIC = re.compile(
    r"pid=([1-9][0-9]{0,9});ppid=([0-9]{1,10});state=([RSDTtXZPIUW]);"
    r"uid=([0-9]{1,10});expected_uid=([0-9]{1,10})"
)
_CLEANUP_SNAPSHOT_RECORD = (
    r"pid=[1-9][0-9]{0,9};ppid=[0-9]{1,10};state=[RSDTtXZPIUW];"
    r"uid=[0-9]{1,10};wchan=[A-Za-z0-9_.]{1,64};"
    r"kill_errno=[0-9]{1,3};sigkill_pending=[01]"
)
_CLEANUP_SNAPSHOT = re.compile(
    rf"{_CLEANUP_SNAPSHOT_RECORD}(?:,{_CLEANUP_SNAPSHOT_RECORD}){{0,3}}"
)
_CLEANUP_DIAGNOSTIC_REASONS = frozenset({
    "cleanup_timeout", "cleanup_signal_denied", "cleanup_signal_failed",
})


def _bounded_cleanup_snapshot(value: str) -> bool:
    if _CLEANUP_SNAPSHOT.fullmatch(value) is None:
        return False
    for record in value.split(","):
        fields = dict(part.split("=", 1) for part in record.split(";"))
        if max(int(fields["pid"]), int(fields["ppid"])) >= 2**31:
            return False
        if int(fields["uid"]) >= 2**32 or int(fields["kill_errno"]) > 255:
            return False
    return True


class SandboxRPCError(DriverError):
    """Only fixed operation/reason codes and HTTP status are safe to publish."""

    def __init__(self, path: str, exc: httpx.HTTPError) -> None:
        process = path == "/processes" or path.startswith("/processes/")
        operation = "process" if process else _RPC_OPERATIONS.get(path, "request")
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            reason = exc.response.headers.get("X-Loom-Sandbox-Error", "")
            allowed_reasons = (
                _EXEC_REASONS if path == "/exec" else
                _PROCESS_REASONS if process else
                frozenset({"restore_request_invalid", "directory_restore_failed"})
                if path == "/restore-directory" else
                _CLEANUP_REASONS if path in {"/stop-processes", "/pause-processes", "/resume-processes"}
                else frozenset()
            )
            if reason not in allowed_reasons:
                reason = "http_error"
            detail = f"HTTP {status}; {reason}"
            if reason == "process_owner_mismatch":
                diagnostic = exc.response.headers.get("X-Loom-Sandbox-Process", "")
                match = _PROCESS_DIAGNOSTIC.fullmatch(diagnostic)
                if (
                    match and max(int(match[1]), int(match[2])) < 2**31
                    and max(int(match[4]), int(match[5])) < 2**32
                ):
                    detail += "; " + diagnostic
            elif reason in _CLEANUP_DIAGNOSTIC_REASONS:
                diagnostic = exc.response.headers.get("X-Loom-Sandbox-Process", "")
                if _bounded_cleanup_snapshot(diagnostic):
                    detail += "; " + diagnostic
        else:
            detail = "transport_timeout" if isinstance(exc, httpx.TimeoutException) else "transport_error"
        super().__init__(f"sandbox {operation} failed ({detail})")


class _SandboxProcess:
    """Client half of one supervised sandbox process."""

    def __init__(self, driver: ServiceSandboxDriver, process_id: str, pid: int) -> None:
        self._driver = driver
        self._path = f"/processes/{process_id}"
        self._pid = pid
        self._exit_code: int | None = None
        self._drained: set[str] = set()
        self._released = False

    def handle(self) -> ExecHandle:
        return ExecHandle(
            pid=self._pid, stdout=self._stream("stdout"), stderr=self._stream("stderr"),
            _wait=self._wait, _kill=self._kill,
        )

    async def _stream(self, name: str) -> AsyncIterator[bytes]:
        offset = 0
        while True:
            response = await self._driver._request(
                "GET", self._path + "/output",
                params={"stream": name, "offset": offset, "wait_ms": _PROCESS_POLL_MS},
                timeout=_PROCESS_POLL_MS / 1000 + 10,
            )
            chunk = response.content
            try:
                following = int(response.headers["X-Loom-Next-Offset"])
            except (KeyError, ValueError):
                raise DriverError("sandbox process output response invalid") from None
            if following != offset + len(chunk):
                raise DriverError("sandbox process output offset mismatch")
            offset = following
            if chunk:
                yield chunk
            if response.headers.get("X-Loom-EOF") == "1":
                self._drained.add(name)
                await self._release()
                return

    async def _wait(self) -> int:
        while self._exit_code is None:
            response = await self._driver._request(
                "GET", self._path, params={"wait_ms": _PROCESS_POLL_MS},
                timeout=_PROCESS_POLL_MS / 1000 + 10,
            )
            status = response.json()
            if status.get("running") is False:
                self._exit_code = int(status["exit_code"])
        await self._release()
        return self._exit_code

    async def _kill(self) -> None:
        if self._exit_code is None and not self._released:
            await self._driver._request("POST", self._path + "/kill", timeout=10)

    async def _release(self) -> None:
        """Free the sandbox record once the exit status and both streams are read."""
        if self._released or self._exit_code is None or self._drained != {"stdout", "stderr"}:
            return
        self._released = True
        try:
            await self._driver._request("DELETE", self._path, timeout=10)
        except DriverError:
            pass  # The record is bounded server-side and dies with the sandbox.


class ServiceSandboxDriver:
    """Connect to one native sidecar; never create Pods or host containers."""

    os: OS = "linux"

    def __init__(
        self,
        socket_path: Path,
        *,
        capabilities: Capabilities,
        network_policy: NetworkPolicy,
        max_transfer_bytes: int = 256 * 1024 * 1024,
        command_environment: Mapping[str, str] | None = None,
    ) -> None:
        if max_transfer_bytes <= 0:
            raise ValueError("positive max_transfer_bytes required")
        self.capabilities = capabilities
        self._network_policy = network_policy
        self._command_environment = dict(command_environment or {})
        self._max_transfer = max_transfer_bytes
        self._socket_path = socket_path
        self._client: httpx.AsyncClient | None = None
        self._started = False
        self._requests: set[asyncio.Task[Any]] = set()

    async def start(self, *, options: StartOptions | None = None) -> None:
        if self._started:
            raise DriverAlreadyStartedError("sandbox driver already started")
        if options is not None and options != StartOptions():
            raise DriverError("native sandbox options must be enforced by its Pod")
        client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(self._socket_path)),
            base_url="http://sandbox",
            timeout=10,
            trust_env=False,
        )
        try:
            response = await client.get("/health")
            response.raise_for_status()
            health = response.json()
            if not isinstance(health, dict) or health.get("ready") is not True:
                raise DriverError("sandbox readiness response invalid")
        except BaseException:
            await client.aclose()
            raise
        self._client = client
        self._started = True

    def _running_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise DriverNotStartedError("sandbox driver is not running")
        return self._client

    async def stop(self, *, delete: bool = True) -> None:
        client, self._client = self._client, None
        current = asyncio.current_task()
        pending = [task for task in self._requests if task is not current]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if client is not None:
            await client.aclose()

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        return None  # The existing Pod collector owns resource accounting.

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        client = self._running_client()
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        try:
            response = await client.request(method, path, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPError as exc:
            # Do not attach request bodies (commands/env may contain secrets).
            raise SandboxRPCError(path, exc) from exc
        finally:
            self._requests.discard(task)

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        response = await self._request(
            "POST",
            "/exec",
            json={
                "argv": ["/bin/sh", "-c", cmd],
                "user": str(user) if user is not None else None,
                "cwd": str(cwd) if cwd is not None else "",
                "env": {**self._command_environment, **dict(env or {})},
                "timeout_sec": timeout_sec or 0,
            },
            timeout=None,  # The server bounds execution and kills its process group.
        )
        result = response.json()
        for key in ("stdout", "stderr"):
            result[key] = base64.b64decode(result[key] or "", validate=True)
        return ExecResult.model_validate(result)

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
        timeout_sec: float | None = None,
    ) -> ExecHandle:
        """Start a supervised sandbox process (#2310).

        The sandbox bounds it by `timeout_sec` (or its configured maximum) and
        kills its process group on deadline or `kill()`. Output is read by
        offset; a slow reader pauses the process instead of losing output.
        """
        if not argv:
            raise DriverError("exec_streaming requires a command")
        response = await self._request(
            "POST",
            "/processes",
            json={
                "argv": list(argv),
                "user": str(user) if user is not None else None,
                "cwd": str(cwd),
                "env": {**self._command_environment, **env_vars},
                "timeout_sec": timeout_sec or 0,
            },
            timeout=30,
        )
        started = response.json()
        return _SandboxProcess(self, str(started["id"]), int(started["pid"])).handle()

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._running_client()
        fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > self._max_transfer:
                raise DriverError("upload requires a regular file within transfer limit")
            async def chunks() -> AsyncIterator[bytes]:
                remaining = info.st_size
                while remaining:
                    chunk = await asyncio.to_thread(stream.read, min(1024 * 1024, remaining))
                    if not chunk:
                        raise DriverError("upload source shrank during transfer")
                    remaining -= len(chunk)
                    yield chunk
                if stream.read(1):
                    raise DriverError("upload source grew during transfer")

            await self._request(
                "PUT",
                "/file",
                params={"path": str(dst)},
                headers={"X-File-Mode": format(stat.S_IMODE(info.st_mode) & 0o777, "o"),
                         "Content-Length": str(info.st_size)},
                content=chunks(),
                timeout=120,
            )

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        client = self._running_client()
        # Download to a private temporary file and atomically publish only a
        # complete, bounded transfer. Existing output survives failed reads.
        if dst.is_symlink() or any(p.is_symlink() for p in dst.parents):
            raise DriverError("download destination must not traverse symlinks")
        dst.parent.mkdir(parents=True, exist_ok=True)
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        temporary: Path | None = None
        try:
            async with client.stream(
                "GET", "/file", params={"path": str(src)}, timeout=120
            ) as response:
                response.raise_for_status()
                if int(response.headers.get("Content-Length", "0")) > self._max_transfer:
                    raise DriverError("download exceeds transfer limit")
                with tempfile.NamedTemporaryFile(dir=dst.parent, delete=False) as output:
                    temporary = Path(output.name)
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self._max_transfer:
                            raise DriverError("download exceeds transfer limit")
                        output.write(chunk)
                temporary.replace(dst)
        except httpx.HTTPError as exc:
            raise DriverError("sandbox download failed") from exc
        finally:
            self._requests.discard(task)
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    async def inspect_reference_file(
        self, path: PurePosixPath, *, max_bytes: int,
    ) -> dict[str, int | str]:
        """Hash a pinned regular executable through the trusted native file RPC.

        The server rejects symlinks in every path component and reads metadata
        from the same descriptor it streams. No task-owned executable is used.
        """
        client = self._running_client()
        limit = min(max_bytes, self._max_transfer)
        if limit < 0:
            raise DriverError("reference inspection budget is negative")
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        try:
            async with client.stream(
                "GET", "/file", params={"path": str(path), "max_bytes": str(limit)}, timeout=120,
            ) as response:
                response.raise_for_status()
                try:
                    size = int(response.headers["Content-Length"])
                    mode = int(response.headers["X-File-Unix-Mode"], 8)
                    uid = int(response.headers["X-File-UID"])
                    gid = int(response.headers["X-File-GID"])
                    if (not 0 <= size <= limit or not stat.S_ISREG(mode)
                            or not mode & 0o111 or not 0 <= uid < 2**32
                            or not 0 <= gid < 2**32):
                        raise ValueError("invalid file metadata")
                except (KeyError, ValueError) as exc:
                    raise DriverError("reference metadata or inspection budget invalid") from exc
                digest = hashlib.sha256()
                received = 0
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > size:
                        raise DriverError("reference content exceeds declared size")
                    digest.update(chunk)
                if received != size:
                    raise DriverError("reference content differs from declared size")
                return {"path": str(path), "size_bytes": size, "mode": stat.S_IMODE(mode),
                        "uid": uid, "gid": gid, "sha256": digest.hexdigest()}
        except httpx.HTTPError as exc:
            raise DriverError("sandbox reference inspection failed") from exc
        finally:
            self._requests.discard(task)

    async def inspect_reference_symlink(self, path: PurePosixPath) -> str:
        """Read bounded literal link text without following the image alias."""
        client = self._running_client()
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        try:
            async with client.stream(
                "GET", "/readlink", params={"path": str(path)}, timeout=120,
            ) as response:
                response.raise_for_status()
                try:
                    size = int(response.headers["Content-Length"])
                    if not 0 < size <= 4096:
                        raise ValueError("invalid size")
                except (KeyError, ValueError) as exc:
                    raise DriverError("symlink inspection size invalid") from exc
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > size:
                        raise DriverError("symlink target exceeds declared size")
                    data.extend(chunk)
                if len(data) != size or b"\x00" in data:
                    raise DriverError("symlink target invalid")
                try:
                    return data.decode("utf-8", errors="strict")
                except UnicodeDecodeError as exc:
                    raise DriverError("symlink target encoding invalid") from exc
        except httpx.HTTPError as exc:
            raise DriverError("sandbox symlink inspection failed") from exc
        finally:
            self._requests.discard(task)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        if policy != self._network_policy:
            raise DriverError("native sandbox network policy is fixed by its Pod")

    async def stop_processes(self) -> None:
        """Stop task descendants before exporting the separate verifier snapshot."""
        await self._request("POST", "/stop-processes")

    async def pause_processes(self) -> None:
        """Suspend task descendants while keeping the snapshot RPC available."""
        await self._request("POST", "/pause-processes")

    async def resume_processes(self) -> None:
        """Resume only processes suspended by the preceding pause operation."""
        await self._request("POST", "/resume-processes")

    async def export_workspace_archive(
        self, src: PurePosixPath, dst: Path, *, preserve_acls: bool = False,
    ) -> None:
        remote = PurePosixPath(f"/tmp/loom-workspace-{uuid4().hex}.tar")
        source, archive = shlex.quote(str(src)), shlex.quote(str(remote))
        special = await self.exec(
            f"find {source} \\( -type b -o -type c -o -type p -o -type s \\) -print -quit"
        )
        if special.return_code or special.stdout:
            raise DriverError("workspace has unsupported special files or cannot be inspected")
        try:
            result = await self.exec(
                f"tar {'--acls --numeric-owner --format=pax ' if preserve_acls else ''}"
                f"-C {source} -cf {archive} .",
            )
            if result.return_code or result.stderr:
                raise DriverError(
                    "unable to export POSIX ACL workspace archive (tar --acls required)"
                    if preserve_acls else "unable to export a stable workspace archive",
                )
            await self.download(remote, dst)
        finally:
            await self.exec(f"rm -f {archive}")

    async def import_workspace_archive(
        self, src: Path, dst: PurePosixPath, *, policy: WorkspaceStagingPolicy | None = None,
        preserve_acls: bool = False,
        external_reference_files: frozenset[PurePosixPath] = frozenset(),
    ) -> None:
        # workspace_snapshot validates/strips the archive in the trusted agent
        # before invoking this hook. The sandbox never chooses verifier inputs.
        from loom.trial.workspace_acls import check_acl_declaration, require_acl_support

        await asyncio.to_thread(check_acl_declaration, src, preserve_acls=preserve_acls)
        if preserve_acls:
            await require_acl_support(self, dst)
        if policy is not None:
            from loom.trial.workspace_snapshot import _prepare_workspace_import

            await _prepare_workspace_import(self, src, dst, policy,
                                            external_reference_files=external_reference_files)
        remote = PurePosixPath(f"/tmp/loom-workspace-{uuid4().hex}.tar")
        destination, archive = shlex.quote(str(dst)), shlex.quote(str(remote))
        try:
            await self.upload(src, remote)
            result = await self.exec(
                f"mkdir -p {destination} && tar {'--acls ' if preserve_acls else ''}"
                f"--numeric-owner -C {destination} -xpf {archive}"
            )
            if result.return_code or result.stderr:
                raise DriverError("unable to restore workspace archive")
        finally:
            await self.exec(f"rm -f {archive}")

    async def replace_workspace_archive(
        self, src: Path, dst: PurePosixPath, *, preserve_acls: bool = False,
    ) -> None:
        await self.replace_mutable_archives(((src, dst),), preserve_acls=preserve_acls)

    async def replace_mutable_archives(
        self, archives: tuple[tuple[Path, PurePosixPath], ...], *, preserve_acls: bool = False,
    ) -> None:
        """Replace a validated mutable root without executing from a cleared tree.

        Extract every root while the original shell/toolchain is present, then let the
        static runtime promote staged entries. The controller's mutable-path
        manifest, archive, reference and ownership checks precede this hook.
        """
        stages: list[tuple[PurePosixPath, PurePosixPath]] = []
        promotion_started = False
        try:
            for src, dst in archives:
                stage = dst / (".loom-restore-" + uuid4().hex)
                stages.append((dst, stage))
                destination, staged = shlex.quote(str(dst)), shlex.quote(str(stage))
                result = await self.exec(f"mkdir -p {destination} && mkdir -m 0700 {staged}")
                if result.return_code or result.stderr or result.truncated:
                    raise DriverError("unable to stage mutable directory restore")
                await self.import_workspace_archive(src, stage, preserve_acls=preserve_acls)
            promotion_started = True
            for dst, stage in stages:
                await self._request(
                    "POST", "/restore-directory", json={"root": str(dst), "stage": stage.name}, timeout=120,
                )
        finally:
            if not promotion_started:
                # The baseline remains intact. After promotion starts, an
                # ambiguous RPC may still be moving entries: never race it with
                # shell cleanup. The native operation removes its stage on
                # success; authoritative sandbox teardown cleans any failure.
                for _, stage in reversed(stages):
                    try:
                        await self.exec(f"rm -rf -- {shlex.quote(str(stage))}")
                    except DriverError:
                        pass

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        if hc is None:
            await self._request("GET", "/health")
            return
        await asyncio.sleep(hc.start_period_sec)
        for attempt in range(hc.retries + 1):
            result = await self.exec(hc.command, timeout_sec=hc.timeout_sec)
            if result.return_code == 0:
                return
            if attempt < hc.retries:
                await asyncio.sleep(hc.interval_sec)
        raise DriverError("sandbox healthcheck failed")
