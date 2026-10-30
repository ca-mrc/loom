"""Docker service drivers behind a task-owned guest's trusted control channel.

Only this controller talks to the guest daemon. Agent commands execute inside
the selected service container, which receives neither a Docker socket nor the
controller's files. The outer guest lease remains the cleanup authority.
"""
from __future__ import annotations

import asyncio
import io
import json
import shlex
import tarfile
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import urlsplit
from uuid import uuid4

from typing_extensions import Buffer

from loom.driver.base import Driver, ExecHandle, StartOptions
from loom.errors import DriverError
from loom.models.compose import ComposeServiceSpec, NativeComposeSpec
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy


class _ComposeControlError(DriverError):
    """Keep only safe result numbers from a fixed control operation."""

    def __init__(self, phase: Literal["identity", "identity read", "activation", "cleanup"],
                 *, return_code: int, duration_sec: float) -> None:
        self.phase = phase
        self.return_code = return_code
        self.duration_sec = duration_sec
        message = {
            "identity": "guest Compose process identity is invalid",
            "identity read": "guest Compose process identity read failed",
            "activation": "guest Compose process activation failed",
            "cleanup": "guest Compose process group cleanup failed",
        }[phase]
        super().__init__(f"{message} (exit_code={return_code}, duration_sec={duration_sec:.6f})")


def _safe_control_failure(error: BaseException) -> str:
    detail = type(error).__name__
    if isinstance(error, _ComposeControlError):
        detail += (f", phase={error.phase}, exit_code={error.return_code}, "
                   f"duration_sec={error.duration_sec:.6f}")
    return detail


def _docker_transport_archive(source: Path, target: Path, manifest: dict[str, Any]) -> None:
    from loom.task_image_archives import MAX_ARCHIVE_BYTES

    if source.is_symlink() or not source.is_file() or not 0 < source.stat().st_size <= MAX_ARCHIVE_BYTES:
        raise DriverError("guest Compose source archive exceeds its prepared input budget")

    class BoundedArchive(io.BufferedWriter):
        def write(self, data: Buffer) -> int:
            if self.tell() + memoryview(data).nbytes > MAX_ARCHIVE_BYTES:
                raise DriverError("guest Compose Docker archive exceeds its scratch budget")
            return super().write(data)

    def blob(digest: Any) -> str:
        if (not isinstance(digest, str) or not digest.startswith("sha256:") or len(digest) != 71
                or any(c not in "0123456789abcdef" for c in digest[7:])):
            raise DriverError("guest Compose OCI blob identity is invalid")
        return "blobs/sha256/" + digest[7:]

    docker_manifest = [{"Config": blob(manifest["config"]["digest"]), "RepoTags": [],
                        "Layers": [blob(layer["digest"]) for layer in manifest["layers"]]}]
    with tarfile.open(source) as incoming, io.FileIO(target, "w") as raw_stream, BoundedArchive(raw_stream) as stream:
        with tarfile.open(fileobj=stream, mode="w") as output:
            for member in incoming:
                if member.name != "manifest.json":
                    output.addfile(member, incoming.extractfile(member) if member.isfile() else None)
            raw = json.dumps(docker_manifest).encode()
            entry = tarfile.TarInfo("manifest.json")
            entry.size, entry.mode = len(raw), 0o600
            output.addfile(entry, io.BytesIO(raw))


class GuestComposeRuntime:
    def __init__(self, *, guest: Driver, topology: NativeComposeSpec, image_archives: Mapping[str, Path],
                 startup_timeout_sec: float = 1200, main_proxy: str | None = None,
                 supervisor_path: str = "/loom/guest-tools/bin/busybox") -> None:
        self.guest = guest
        self.topology = topology
        self.image_archives = dict(image_archives)
        self.startup_timeout_sec = startup_timeout_sec
        self.project = "loom-" + uuid4().hex
        self.containers: dict[str, str] = {}
        self._network = False
        self.supervisor_path = supervisor_path
        self.main_environment: dict[str, str] = {}
        if main_proxy is not None:
            proxy = urlsplit(main_proxy)
            if (proxy.scheme != "http" or proxy.hostname != "10.0.2.2" or not proxy.port
                    or proxy.username or proxy.password or proxy.path or proxy.query or proxy.fragment):
                raise DriverError("guest Compose proxy must be the credential-free guest egress address")
            self.main_environment = dict.fromkeys(("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"), main_proxy)
            bypass = ",".join(("localhost", "127.0.0.1", "::1", "10.0.2.2", *(s.name for s in topology.services)))
            self.main_environment.update(no_proxy=bypass, NO_PROXY=bypass)

    async def _command(self, argv: list[str], *, timeout: float = 120) -> ExecResult:
        result = await self.guest.exec(shlex.join(["docker", *argv]), timeout_sec=timeout)
        if result.return_code:
            # Docker diagnostics may contain authored environment values.
            raise DriverError("guest Compose operation failed: " + argv[0])
        return result

    async def _load_image(self, service: ComposeServiceSpec) -> str:
        archive = self.image_archives.get(service.name)
        if archive is None or not service.image or "@sha256:" not in service.image:
            raise DriverError("guest Compose requires a prepared immutable image archive")
        # The ordinary immutable input transport already validated these bytes.
        # Read the OCI identity; Docker verifies its blobs when loading it.
        with tarfile.open(archive) as tar:
            index_file = tar.extractfile("index.json")
            if index_file is None:
                raise DriverError("guest Compose OCI index is missing")
            index = json.load(index_file)
            manifests = index.get("manifests", [])
            if len(manifests) != 1 or manifests[0]["digest"] != service.image.split("@", 1)[1]:
                raise DriverError("guest Compose OCI archive differs from its image grant")
            manifest_file = tar.extractfile("blobs/sha256/" + manifests[0]["digest"].removeprefix("sha256:"))
            if manifest_file is None:
                raise DriverError("guest Compose OCI manifest is missing")
            manifest = json.load(manifest_file)
            image_id: str = manifest["config"]["digest"]
            manifest_digest: str = manifests[0]["digest"]
        remote = PurePosixPath("/tmp/loom-workspace-" + uuid4().hex + ".tar")
        try:
            # The guest's classic Docker store needs the Docker archive index.
            # Add that transport view without changing OCI blobs or identity,
            # and never carry mutable registry tags into the task daemon.
            with tempfile.TemporaryDirectory(prefix="loom-compose-load-") as temporary:
                load_archive = Path(temporary) / "load.tar"
                copy = asyncio.create_task(asyncio.to_thread(_docker_transport_archive, archive, load_archive, manifest))
                try:
                    await asyncio.shield(copy)
                except asyncio.CancelledError:
                    # Do not retire scratch storage while its bounded writer
                    # still runs in a thread after controller cancellation.
                    while not copy.done():
                        try:
                            await asyncio.shield(copy)
                        except asyncio.CancelledError:
                            pass
                    if not copy.cancelled():
                        copy.exception()
                    raise
                upload = getattr(self.guest, "_upload_file", None)
                if upload is None:
                    await self.guest.upload(load_archive, remote)
                else:
                    await upload(load_archive, remote, workspace_archive=True)
            await self._command(["load", "--input", str(remote)], timeout=self.startup_timeout_sec)
            # Docker's containerd store identifies images by manifest digest;
            # the classic store uses the config digest. Both originate in the
            # admitted manifest, with no mutable tag involved.
            for candidate in (manifest_digest, image_id):
                inspected = await self.guest.exec(shlex.join(["docker", "image", "inspect", candidate, "--format", "{{.Id}}"] ))
                if inspected.return_code == 0 and inspected.stdout.decode().strip() == candidate:
                    return candidate
            raise DriverError("guest Compose loaded image identity mismatch")
        finally:
            await self.guest.exec("rm -f -- " + shlex.quote(str(remote)))
    async def start(self) -> None:
        if self.containers or self._network:
            raise DriverError("guest Compose runtime already started")
        names = {service.name for service in self.topology.services}
        if set(self.image_archives) != names:
            raise DriverError("guest Compose image archive inventory differs from topology")
        try:
            async with asyncio.timeout(self.startup_timeout_sec):
                images = {service.name: await self._load_image(service) for service in self.topology.services}
                await self._command(["network", "create", "--label", "loom.compose=" + self.project, self.project])
                self._network = True
                for service in self.topology.ordered_services():
                    from loom.guest_compose import SIDECAR_CPUS, SIDECAR_MEMORY_MB
                    for dependency, condition in service.depends_on.items():
                        await self._wait(dependency, condition)
                    name = self.project + "-" + service.name
                    argv = ["create", "--name", name, "--label", "loom.compose=" + self.project]
                    argv.extend(["--mount", "type=bind,source=" + self.supervisor_path +
                                 ",target=/loom/compose-tools/busybox,readonly"])
                    if service.network_service:
                        argv.extend(["--network", "container:" + self.containers[service.network_service]])
                    else:
                        argv.extend(["--network", self.project, "--network-alias", service.name])
                    environment = {**(self.main_environment if service.name == "main" else {}), **service.environment}
                    for key, value in environment.items():
                        argv.extend(["--env", key + "=" + value])
                    cpus = service.cpus if service.name == "main" else service.cpus or SIDECAR_CPUS
                    memory = service.memory_mb if service.name == "main" else service.memory_mb or SIDECAR_MEMORY_MB
                    for flag, resource_value in [("--cpus", cpus), ("--memory", f"{memory}m" if memory else None),
                                        ("--shm-size", service.shm_size_bytes), ("--user", service.user), ("--workdir", service.workdir)]:
                        if resource_value is not None:
                            argv.extend([flag, str(resource_value)])
                    for cap in service.capabilities:
                        argv.extend(["--cap-add", cap])
                    for port in service.ports:
                        argv.extend(["--expose", str(port)])
                    if service.healthcheck:
                        health = service.healthcheck
                        argv.extend(["--health-cmd", health.command, "--health-interval", f"{health.interval_sec}s",
                                     "--health-timeout", f"{health.timeout_sec}s", "--health-retries", str(health.retries),
                                     "--health-start-period", f"{health.start_period_sec}s"])
                        if health.start_interval_sec is not None:
                            argv.extend(["--health-start-interval", f"{health.start_interval_sec}s"])
                    command = list(service.command) if service.command is not None else None
                    if service.entrypoint is not None:
                        argv.extend(["--entrypoint", service.entrypoint[0] if service.entrypoint else ""])
                        command = [*service.entrypoint[1:], *(command or [])]
                    argv.append(images[service.name])
                    if command is not None:
                        argv.extend(command)
                    created = await self._command(argv)
                    container = created.stdout.decode().strip()
                    if len(container) != 64 or any(c not in "0123456789abcdef" for c in container):
                        raise DriverError("guest Compose container identity is invalid")
                    self.containers[service.name] = container
                    await self._command(["start", container])
                for service in self.topology.services:
                    if service.healthcheck is not None:
                        await self._wait(service.name, "service_healthy")
        except BaseException:
            await self.stop()
            raise

    async def _wait(self, name: str, condition: str) -> None:
        while True:
            result = await self._command(["inspect", self.containers[name], "--format", "{{json .State}}"])
            state = json.loads(result.stdout)
            if condition == "service_started" and state.get("Running"):
                return
            if condition == "service_completed_successfully" and state.get("Status") == "exited":
                if state.get("ExitCode") == 0:
                    return
                raise DriverError("guest Compose initialization service failed")
            health = state.get("Health", {}).get("Status")
            if condition == "service_healthy" and health == "healthy":
                return
            if state.get("Status") in {"dead", "exited"} or health == "unhealthy":
                raise DriverError("guest Compose dependency failed")
            await asyncio.sleep(.2)

    def driver(self, name: str) -> GuestComposeServiceDriver:
        if name not in self.containers:
            raise DriverError("guest Compose service is unavailable")
        return GuestComposeServiceDriver(self, name)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"project": self.project, "containers": self.containers}))
        path.chmod(0o600)

    async def attach(self, path: Path) -> None:
        """Reconnect the trusted setup phase's containers without reinstalling."""
        receipt = json.loads(path.read_text())
        project, containers = receipt["project"], receipt["containers"]
        if (not isinstance(project, str) or not project.startswith("loom-") or len(project) != 37
                or any(c not in "0123456789abcdef" for c in project[5:])
                or set(containers) != {s.name for s in self.topology.services}):
            raise DriverError("guest Compose setup receipt is invalid")
        for service in self.topology.services:
            container = containers[service.name]
            if not isinstance(container, str) or len(container) != 64 or any(c not in "0123456789abcdef" for c in container):
                raise DriverError("guest Compose setup container is invalid")
            inspected = await self._command(["inspect", container, "--format", "{{json .Config}}"])
            config = json.loads(inspected.stdout)
            with tarfile.open(self.image_archives[service.name]) as archive:
                index_file = archive.extractfile("index.json")
                if index_file is None:
                    raise DriverError("guest Compose OCI index is missing")
                index = json.load(index_file)
                digest = index["manifests"][0]["digest"]
                manifest_file = archive.extractfile("blobs/sha256/" + digest.removeprefix("sha256:"))
                if manifest_file is None:
                    raise DriverError("guest Compose OCI manifest is missing")
                manifest = json.load(manifest_file)
            if (not service.image or digest != service.image.split("@", 1)[-1]
                    or config.get("Labels", {}).get("loom.compose") != project
                    or config.get("Image") not in {digest, manifest["config"]["digest"]}):
                raise DriverError("guest Compose setup identity differs from frozen topology")
        self.project, self.containers, self._network = project, containers, True

    async def stop(self) -> None:
        failures = []
        for name, container in reversed(tuple(self.containers.items())):
            try:
                await self._command(["rm", "--force", "--volumes", container])
                del self.containers[name]
            except Exception as exc:
                failures.append(exc)
        if self._network:
            try:
                await self._command(["network", "rm", self.project])
                self._network = False
            except Exception as exc:
                failures.append(exc)
        if failures:
            raise DriverError("guest Compose cleanup failed; guest retirement required")


class GuestComposeServiceDriver:
    _SUPERVISOR = "/loom/compose-tools/busybox"
    _IDENTITY_NOT_READY = 75  # Inner EX_TEMPFAIL; Docker CLI failures commonly use 1.
    def __init__(self, runtime: GuestComposeRuntime, service: str) -> None:
        self.runtime, self.service = runtime, service
        self.capabilities = runtime.guest.capabilities
        self.os = runtime.guest.os

    @property
    def container(self) -> str:
        try:
            return self.runtime.containers[self.service]
        except KeyError:
            raise DriverError("guest Compose service has stopped") from None

    async def start(self, *, options: StartOptions | None = None) -> None:
        if options is not None:
            raise DriverError("guest Compose service options belong to its frozen topology")
        _ = self.container

    async def stop(self, *, delete: bool = True) -> None:
        # The enclosing guest runtime owns service retirement, not an individual
        # connection used for artifact collection or an agent exec session.
        return None

    def _exec(self, argv: list[str], *, user: str | int | None = None, cwd: PurePosixPath | None = None,
              env: Mapping[str, str] | None = None) -> list[str]:
        command = ["docker", "exec"]
        if user is not None:
            command.extend(["--user", str(user)])
        if cwd is not None:
            command.extend(["--workdir", str(cwd)])
        proxy = self.runtime.main_environment if self.service == "main" else {}
        for key, value in {**proxy, **(env or {})}.items():
            command.extend(["--env", key + "=" + value])
        return [*command, self.container, *argv]

    async def _control(self, argv: list[str]) -> ExecResult:
        from loom.driver.service_sandbox import ServiceSandboxDriver

        # Starting another Docker CLI competes with the supervised command for
        # the guest CPU before Docker can even accept this five-second control.
        # The private guest RPC uses the same daemon exec without a CLI process.
        if isinstance(self.runtime.guest, ServiceSandboxDriver):
            return await self.runtime.guest._exec_container(self.container, argv, timeout_sec=5)
        return await self.runtime.guest.exec(shlex.join(self._exec(argv, user="0:0")), timeout_sec=5)

    async def exec(self, cmd: str, *, user: str | int | None = None, cwd: PurePosixPath | None = None,
                   env: Mapping[str, str] | None = None, timeout_sec: float | None = None) -> ExecResult:
        argv, marker = self._controlled(["/bin/sh", "-c", cmd])
        operation = asyncio.create_task(self.runtime.guest.exec(
            shlex.join(self._exec(argv, user=user, cwd=cwd, env=env)), timeout_sec=timeout_sec))
        failure: BaseException | None = None
        phase = "activation"
        try:
            await self._activate(marker, operation)
            phase = "execution"
            return await operation
        except BaseException as exc:
            failure = exc
            raise
        finally:
            try:
                await self._cleanup_with_context(marker, failure, phase)
            finally:
                if not operation.done():
                    operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)

    async def exec_streaming(self, argv: list[str], *, env_vars: dict[str, str], cwd: PurePosixPath,
                             user: str | int | None = None, timeout_sec: float | None = None) -> ExecHandle:
        controlled, marker = self._controlled(argv)
        if timeout_sec is not None:
            # Native setup has an explicit per-process bound beyond Driver's
            # minimum streaming protocol; the guest channel enforces it.
            stream = cast(Callable[..., Awaitable[ExecHandle]], self.runtime.guest.exec_streaming)
            handle = await stream(
                self._exec(controlled, user=user, cwd=cwd, env=env_vars), env_vars={}, cwd=PurePosixPath("/"), timeout_sec=timeout_sec,
            )
        else:
            handle = await self.runtime.guest.exec_streaming(
                self._exec(controlled, user=user, cwd=cwd, env=env_vars), env_vars={}, cwd=PurePosixPath("/"),
            )
        try:
            await self._activate(marker)
        except BaseException as exc:
            try:
                await self._cleanup_with_context(marker, exc, "activation")
            finally:
                await handle.kill()
            raise

        async def kill() -> None:
            try:
                await self._cleanup_control(marker)
            finally:
                await handle.kill()

        async def wait() -> int:
            try:
                return await handle.wait()
            finally:
                await kill()

        return ExecHandle(pid=handle.pid, stdout=handle.stdout, stderr=handle.stderr, _wait=wait, _kill=kill)

    def _controlled(self, argv: list[str]) -> tuple[list[str], str]:
        marker = "/tmp/loom-compose-exec-" + uuid4().hex
        helper = self._SUPERVISOR
        # Redirection exposes an empty file before printf writes the PID. Keep
        # that intermediate state private, then publish the complete PID with
        # a same-directory atomic rename before the controller can read it.
        script = ("old_umask=$(umask); umask 077; "
                  f"trap '{helper} rm -f {marker} {marker}.pending' EXIT; "
                  "trap 'exit 143' TERM; "
                  f"printf '%s' \"$$\" > {marker}.pending && "
                  f"{helper} mv -f {marker}.pending {marker} || exit 125; i=0; "
                  f"while test ! -f {marker}.go; do i=$((i+1)); test \"$i\" -lt 300 || exit 124; "
                  f"{helper} sleep 0.1; done; umask \"$old_umask\"; \"$@\"")
        # Docker exec starts a process-group leader. Start setsid as a shell
        # child so BusyBox need not fork-and-return, which would lose the CLI's
        # completion/stream ownership before the inner command finishes.
        return [helper, "sh", "-c", '"$@" & child=$!; wait "$child"', "loom-parent",
                helper, "setsid", helper, "sh", "-c", script, "loom-exec", *argv], marker

    async def _activate(self, marker: str, operation: asyncio.Task[ExecResult] | None = None) -> None:
        # Commands cannot begin until the controller has observed their group
        # leader. A cancellation before that handshake never releases the gate.
        async with asyncio.timeout(30):
            while True:
                if operation is not None and operation.done():
                    await operation
                    raise DriverError("guest Compose process exited before activation")
                # Keep this probe read-only: cancelling Docker's client does
                # not stop an inner exec already accepted by its daemon. Only
                # the controller may release the gate after observing the PID.
                # Keep each accepted inner exec small: killing its Docker CLI
                # does not reap an inner polling helper. An observed absent
                # marker gets a dedicated status distinct from Docker errors.
                helper = self._SUPERVISOR
                script = (f"if test ! -e {marker}; then exit {self._IDENTITY_NOT_READY}; fi; "
                          f"if {helper} cat {marker} 2>/dev/null; then exit 0; "
                          f"elif test ! -e {marker}; then exit {self._IDENTITY_NOT_READY}; "
                          "else exit 2; fi")
                result = await self._control([helper, "sh", "-c", script])
                if result.return_code == 0:
                    value = result.stdout.decode().strip()
                    if not value.isdecimal() or not 1 < int(value) < 2**31:
                        raise _ComposeControlError("identity", return_code=result.return_code,
                                                   duration_sec=result.duration_sec)
                    activated = await self._control([helper, "touch", marker + ".go"])
                    if activated.return_code:
                        raise _ComposeControlError("activation", return_code=activated.return_code,
                                                   duration_sec=activated.duration_sec)
                    return
                if result.return_code != self._IDENTITY_NOT_READY:
                    raise _ComposeControlError("identity read", return_code=result.return_code,
                                               duration_sec=result.duration_sec)
                await asyncio.sleep(.05)

    async def _cleanup_with_context(self, marker: str, failure: BaseException | None, phase: str) -> None:
        try:
            await self._cleanup_control(marker)
        except Exception as cleanup:
            if failure is None:
                raise
            # Report both failures without publishing commands, stderr or env.
            # A cleanup error must not misidentify an earlier activation expiry
            # as the first fault, nor may a cleanup failure be suppressed.
            raise DriverError(
                f"guest Compose {phase} failed ({_safe_control_failure(failure)}); "
                f"process group cleanup failed ({_safe_control_failure(cleanup)})"
            ) from failure

    async def _cleanup_control(self, marker: str) -> None:
        helper = self._SUPERVISOR
        # Marker is controller-generated; PID is checked inside this container
        # before a negative process-group signal can be constructed.
        # Read the identity once. The supervisor's EXIT trap may remove it
        # between a separate existence check and cat; only a still-existing
        # unreadable marker is an error, not an already-retired supervisor.
        script = (f"if p=$({helper} cat {marker} 2>/dev/null); then "
                  "case \"$p\" in ''|*[!0-9]*) exit 1;; esac; "
                  'test "$p" -gt 1 && test "$p" -lt 2147483648 || exit 1; '
                  f"{helper} kill -TERM -- -\"$p\" 2>/dev/null || true; {helper} sleep 0.1; "
                  f"{helper} kill -KILL -- -\"$p\" 2>/dev/null || true; "
                  f"elif test -e {marker}; then exit 1; fi; {helper} rm -f {marker} {marker}.go {marker}.pending")
        operation = asyncio.create_task(self._control([helper, "sh", "-c", script]))
        cancelled = False
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                cancelled = True
        result = operation.result()
        if result.return_code:
            raise _ComposeControlError("cleanup", return_code=result.return_code,
                                       duration_sec=result.duration_sec)
        if cancelled:
            raise asyncio.CancelledError

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        await self._upload_file(src, dst)

    async def _upload_file(self, src: Path, dst: PurePosixPath, *, workspace_archive: bool = False) -> None:
        remote = PurePosixPath("/tmp/loom-workspace-" + uuid4().hex + ".tar")
        try:
            upload = getattr(self.runtime.guest, "_upload_file", None)
            if workspace_archive and upload is not None:
                await upload(src, remote, workspace_archive=True)
            else:
                await self.runtime.guest.upload(src, remote)
            # Validate through the service filesystem before Docker's copy API.
            checks = " && ".join(f"test ! -L {shlex.quote(str(p))}" for p in (*reversed(dst.parents), dst))
            result = await self.exec(checks + " && mkdir -p -- " + shlex.quote(str(dst.parent)))
            if result.return_code:
                raise DriverError("guest Compose upload path is unsafe")
            await self.runtime._command(["cp", str(remote), self.container + ":" + str(dst)])
            identity = await self.exec("printf '%s:%s' \"$(id -u)\" \"$(id -g)\"")
            owner = identity.stdout.decode().strip()
            if identity.return_code or len(owner.split(":")) != 2 or not all(part.isdecimal() for part in owner.split(":")):
                raise DriverError("guest Compose upload identity is invalid")
            owned = await self.exec(shlex.join(["chown", owner, str(dst)]), user="0:0")
            if owned.return_code:
                raise DriverError("guest Compose upload ownership failed")
        finally:
            await self.runtime.guest.exec("rm -f -- " + shlex.quote(str(remote)))

    async def import_workspace_archive(self, src: Path, dst: PurePosixPath, **kwargs: Any) -> None:
        # The native implementation validates/replaces through this service's
        # exec and archive-upload boundary, preserving the inner image USER.
        from loom.driver.service_sandbox import ServiceSandboxDriver

        await ServiceSandboxDriver.import_workspace_archive(cast(ServiceSandboxDriver, self), src, dst, **kwargs)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        remote = PurePosixPath("/tmp/loom-compose-" + uuid4().hex)
        try:
            await self.runtime._command(["cp", self.container + ":" + str(src), str(remote)])
            await self.runtime.guest.download(remote, dst)
        finally:
            await self.runtime.guest.exec("rm -f -- " + shlex.quote(str(remote)))

    async def export_workspace_archive(self, src: PurePosixPath, dst: Path, **kwargs: Any) -> None:
        if kwargs.get("preserve_acls"):
            raise DriverError("guest Compose archive export does not preserve ACLs")
        remote = PurePosixPath("/tmp/loom-workspace-" + uuid4().hex + ".tar")
        try:
            command = shlex.join(["docker", "cp", self.container + ":" + str(src).rstrip("/") + "/.", "-"])
            result = await self.runtime.guest.exec(command + " > " + shlex.quote(str(remote)))
            if result.return_code:
                raise DriverError("guest Compose archive export failed")
            download = getattr(self.runtime.guest, "_download_file", None)
            if download is None:
                await self.runtime.guest.download(remote, dst)
            else:
                await download(remote, dst, workspace_archive=True)
        finally:
            await self.runtime.guest.exec("rm -f -- " + shlex.quote(str(remote)))

    async def stop_processes(self) -> None:
        await self.runtime._command(["stop", "--time", "0", self.container])

    async def pause_processes(self) -> None:
        await self.runtime._command(["pause", self.container])

    async def resume_processes(self) -> None:
        await self.runtime._command(["unpause", self.container])

    async def resource_snapshot(self) -> None:
        return None

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        await self.runtime.guest.set_network_policy(policy)

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        from loom.driver.healthcheck import run_command_healthcheck

        if hc is not None:
            await run_command_healthcheck(self, hc)
