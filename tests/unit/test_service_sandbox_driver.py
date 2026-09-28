"""Unix socket driver contract, without Kubernetes, cloud or model calls."""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverAlreadyStartedError, DriverError, DriverNotStartedError
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork, Public


def driver_for(path: Path, *, limit: int = 1024) -> ServiceSandboxDriver:
    return ServiceSandboxDriver(
        path,
        capabilities=Capabilities(
            os="linux",
            gpu_vendor="none",
            network_policies=frozenset({"public"}),
            dynamic_network_policy=False,
            mounted_fs=False,
            resource_modes=frozenset({"limit"}),
        ),
        network_policy=Public(),
        max_transfer_bytes=limit,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("health", [{"ready": True}, {"ready": True, "instance_id": "a" * 32}])
async def test_socket_rpc_exec_files_and_lifecycle(tmp_path: Path, health: dict) -> None:
    requests: list[tuple[str, str, bytes]] = []
    data = b""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal data
        headers = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
        method, target, _ = headers[0].split()
        length = next(
            (int(h.split(":", 1)[1]) for h in headers if h.lower().startswith("content-length:")), 0
        )
        body = await reader.readexactly(length)
        requests.append((method, target, body))
        if target == "/health":
            output = json.dumps(health).encode()
        elif target == "/exec":
            output = json.dumps(
                {
                    "return_code": 3,
                    "stdout": base64.b64encode(b"out").decode(),
                    "stderr": base64.b64encode(b"err").decode(),
                    "truncated": False,
                    "duration_sec": 0.1,
                }
            ).encode()
        elif method == "PUT":
            data, output = body, b""
        else:
            output = data
        writer.write(
            f"HTTP/1.1 200 OK\r\nContent-Length: {len(output)}\r\nConnection: close\r\n\r\n".encode()
            + output
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    # pytest's macOS tmp path can exceed the Unix socket path length limit.
    import tempfile

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="loom-driver-") as directory:
        socket = Path(directory) / "rpc.sock"
        server = await asyncio.start_unix_server(handle, path=socket)
        driver = driver_for(socket)
        async with server:
            with pytest.raises(DriverNotStartedError):
                await driver.exec("true")
            await driver.stop()
            await driver.start()
            result = await driver.exec(
                "printf x", user="root", cwd=PurePosixPath("/app"), env={"A": "b"}, timeout_sec=2
            )
            assert (result.return_code, result.stdout, result.stderr) == (3, b"out", b"err")
            body = json.loads(requests[-1][2])
            assert body == {
                "argv": ["/bin/sh", "-c", "printf x"],
                "user": "root",
                "cwd": "/app",
                "env": {"A": "b"},
                "timeout_sec": 2,
            }
            source = tmp_path / "source"
            source.write_bytes(b"transfer")
            await driver.upload(source, PurePosixPath("/app/source"))
            target = tmp_path / "download"
            # Resolve macOS /var -> /private/var in trusted local fixture.
            await driver.download(PurePosixPath("/app/source"), target.resolve())
            assert target.read_bytes() == b"transfer"
            source.write_bytes(b"x" * 1025)
            with pytest.raises(DriverError, match="transfer limit"):
                await driver.upload(source, PurePosixPath("/app/source"))
            data = b"x" * 1025
            with pytest.raises(DriverError, match="transfer limit"):
                await driver.download(PurePosixPath("/app/source"), target.resolve())
            assert target.read_bytes() == b"transfer"
            await driver.set_network_policy(Public())
            with pytest.raises(DriverError, match="fixed"):
                await driver.set_network_policy(NoNetwork())
            await driver.stop()
            await driver.stop()
            with pytest.raises(DriverAlreadyStartedError):
                await driver.start()
            with pytest.raises(DriverNotStartedError):
                await driver.download(PurePosixPath("/app/source"), target)


@pytest.mark.asyncio
async def test_nondefault_start_options_fail_closed(tmp_path: Path) -> None:
    driver = driver_for(tmp_path / "absent.sock")
    with pytest.raises(DriverError, match="Pod"):
        await driver.start(options=StartOptions(cpus=4))


@pytest.mark.asyncio
async def test_snapshot_hooks_preserve_public_files_without_root(tmp_path: Path) -> None:
    import shutil

    from loom.models.exec import ExecResult
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_snapshot import _import_workspace_archive, _strip_private_entries

    class LocalTransport(ServiceSandboxDriver):
        # Exercise the production archive hooks against real POSIX processes;
        # only the RPC transport is replaced with this process's filesystem.
        async def exec(self, cmd, *, user=None, cwd=None, env=None, timeout_sec=None):
            assert user is None
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(cwd) if cwd else None,
            )
            stdout, stderr = await process.communicate()
            return ExecResult(
                return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0
            )

        async def upload(self, src, dst):
            shutil.copyfile(src, str(dst))

        async def download(self, src, dst):
            shutil.copyfile(str(src), dst)

    base = driver_for(tmp_path / "unused.sock")
    driver = LocalTransport(
        tmp_path / "unused.sock", capabilities=base.capabilities, network_policy=Public()
    )
    source = tmp_path / "source"
    source.mkdir()
    (source / "answer.txt").write_text("result")
    (source / "tests").mkdir()
    (source / "tests" / "forged.py").write_text("untrusted verifier")
    archive = tmp_path / "workspace.tar"
    await driver.export_workspace_archive(PurePosixPath(str(source)), archive)
    policy = WorkspaceStagingPolicy(
        agent_excluded_paths=("tests/**",),
        verifier_only_paths=("tests/**",),
        trusted_oracle_paths=(),
    )
    _strip_private_entries(archive, policy)
    destination = tmp_path / "verifier"
    (destination / "tests").mkdir(parents=True)
    (destination / "tests" / "trusted.py").write_text("private verifier")
    (destination / "deleted.txt").write_text("stale image state")
    await _import_workspace_archive(driver, archive, PurePosixPath(str(destination)), policy=policy)
    assert (destination / "answer.txt").read_text() == "result"
    assert not (destination / "tests" / "forged.py").exists()
    assert not (destination / "deleted.txt").exists()
    assert (destination / "tests" / "trusted.py").read_text() == "private verifier"


@pytest.mark.parametrize("reason", [
    "exec_request_invalid", "exec_user_mismatch", "exec_timeout_invalid",
    "exec_environment_invalid",
])
@pytest.mark.parametrize("path", ["/exec", "/file", "/stop-processes"])
def test_exec_reason_codes_are_operation_scoped_and_redacted(reason: str, path: str) -> None:
    import httpx

    from loom.driver.service_sandbox import SandboxRPCError

    request = httpx.Request("POST", "http://sandbox" + path, content=b"private-command")
    response = httpx.Response(
        400, request=request, headers={"X-Loom-Sandbox-Error": reason},
        text="private-environment",
    )
    error = SandboxRPCError(path, httpx.HTTPStatusError("private-error", request=request, response=response))
    expected = reason if path == "/exec" else "http_error"
    assert f"HTTP 400; {expected}" in str(error)
    assert "private-" not in str(error)


def test_unrecognized_exec_reason_is_not_published() -> None:
    import httpx

    from loom.driver.service_sandbox import SandboxRPCError

    request = httpx.Request("POST", "http://sandbox/exec")
    response = httpx.Response(400, request=request, headers={"X-Loom-Sandbox-Error": "private-value"})
    error = SandboxRPCError("/exec", httpx.HTTPStatusError("private-error", request=request, response=response))
    assert str(error) == "sandbox exec failed (HTTP 400; http_error)"


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [
    {"Content-Length": "9"},
    {"X-File-Unix-Mode": "100644"},
    {"X-File-Unix-Mode": "120755"},
    {"X-File-UID": "-1"},
    {"X-File-GID": "invalid"},
])
async def test_reference_rejects_bad_metadata_before_reading_body(tmp_path, headers):
    import httpx

    class UnreadableBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            pytest.fail("rejected reference content must not be read")
            yield b""

    metadata = {"Content-Length": "7", "X-File-Unix-Mode": "100755",
                "X-File-UID": "0", "X-File-GID": "0", **headers}

    def handle(request):
        assert request.url.params["max_bytes"] == "7"
        return httpx.Response(200, headers=metadata, stream=UnreadableBody())

    driver = driver_for(tmp_path / "unused.sock")
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://sandbox")
    try:
        with pytest.raises(DriverError, match="reference"):
            await driver.inspect_reference_file(PurePosixPath("/bin/interpreter"), max_bytes=7)
        assert not driver._requests
    finally:
        await driver.stop()


@pytest.mark.asyncio
async def test_stop_cancels_reference_inspection_and_closes_stream(tmp_path):
    import httpx

    reading, closed = asyncio.Event(), asyncio.Event()

    class PendingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            reading.set()
            await asyncio.Event().wait()
            yield b'x'

        async def aclose(self):
            closed.set()

    def handle(request):
        return httpx.Response(200, stream=PendingBody(), headers={
            'Content-Length': '1', 'X-File-Unix-Mode': '100755',
            'X-File-UID': '0', 'X-File-GID': '0'})

    driver = driver_for(tmp_path / 'unused.sock')
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url='http://sandbox')
    inspection = asyncio.create_task(driver.inspect_reference_file(PurePosixPath('/bin/python'), max_bytes=1))
    try:
        await asyncio.wait_for(reading.wait(), timeout=2)
        await driver.stop()
        with pytest.raises(asyncio.CancelledError):
            await inspection
        assert closed.is_set() and not driver._requests
    finally:
        await driver.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [b"python3.11", b"/usr/local/bin/python3.11"])
async def test_inspect_reference_symlink_preserves_literal_target(tmp_path, target):
    import httpx

    def handle(request):
        assert request.url.path == "/readlink"
        assert request.url.params["path"] == "/usr/local/bin/python3"
        return httpx.Response(200, content=target)

    driver = driver_for(tmp_path / "unused.sock")
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://sandbox")
    try:
        assert await driver.inspect_reference_symlink(PurePosixPath("/usr/local/bin/python3")) == target.decode()
        assert not driver._requests
    finally:
        await driver.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("content,headers", [
    (b"", {}), (b"a\x00b", {}), (b"\xff", {}), (b"x" * 4097, {}),
    (b"abc", {"Content-Length": "2"}), (b"abc", {"Content-Length": "4"}),
    (b"abc", {"Content-Length": "invalid"}),
])
async def test_inspect_reference_symlink_rejects_malformed_reply(tmp_path, content, headers):
    import httpx

    driver = driver_for(tmp_path / "unused.sock")
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=content, headers=headers)), base_url="http://sandbox")
    try:
        with pytest.raises(DriverError, match="symlink"):
            await driver.inspect_reference_symlink(PurePosixPath("/usr/local/bin/python3"))
        assert not driver._requests
    finally:
        await driver.stop()


@pytest.mark.asyncio
async def test_stop_cancels_reference_symlink_read(tmp_path):
    import httpx

    reading, closed = asyncio.Event(), asyncio.Event()

    class PendingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            reading.set()
            await asyncio.Event().wait()
            yield b"x"

        async def aclose(self):
            closed.set()

    driver = driver_for(tmp_path / "unused.sock")
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, headers={"Content-Length": "1"}, stream=PendingBody())),
        base_url="http://sandbox")
    inspection = asyncio.create_task(driver.inspect_reference_symlink(PurePosixPath("/bin/python")))
    try:
        await asyncio.wait_for(reading.wait(), timeout=2)
        await driver.stop()
        with pytest.raises(asyncio.CancelledError):
            await inspection
        assert closed.is_set() and not driver._requests
    finally:
        await driver.stop()


@pytest.mark.asyncio
async def test_upload_streams_bounded_chunks_with_exact_content_length(tmp_path: Path) -> None:
    import hashlib

    import httpx

    size = 9 * 1024 * 1024 + 31
    source = tmp_path / "large-artifact"
    with source.open("wb") as output:
        output.truncate(size)
    observed = hashlib.sha256()
    sizes = []

    class Receiver(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            assert int(request.headers["Content-Length"]) == size
            async for chunk in request.stream:
                sizes.append(len(chunk))
                assert len(chunk) <= 1024 * 1024
                observed.update(chunk)
            return httpx.Response(204)

    driver = driver_for(tmp_path / "unused.sock", limit=size)
    async with httpx.AsyncClient(transport=Receiver(), base_url="http://sandbox") as client:
        driver._client = client
        await driver.upload(source, PurePosixPath("/app/large-artifact"))
    assert sum(sizes) == size
    with source.open("rb") as stream:
        assert observed.hexdigest() == hashlib.file_digest(stream, "sha256").hexdigest()
