"""Unix socket driver contract, without Kubernetes, cloud or model calls."""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.service_sandbox import SandboxRPCError, ServiceSandboxDriver
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

        async def _upload_file(self, src, dst, **kwargs):
            shutil.copyfile(src, str(dst))

        async def _download_file(self, src, dst, **kwargs):
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


@pytest.mark.parametrize("reason", [
    "file_permission_denied", "file_read_only_filesystem", "file_invalid_target", "file_io_error",
])
@pytest.mark.parametrize("path", ["/file", "/exec"])
def test_file_failure_reason_is_allowlisted_without_private_diagnostics(reason: str, path: str) -> None:
    import httpx

    from loom.driver.service_sandbox import SandboxRPCError

    request = httpx.Request("PUT", "http://sandbox" + path, params={"path": "/private-file"},
                            content=b"private-payload")
    response = httpx.Response(400, request=request, headers={
        "X-Loom-Sandbox-Error": reason, "X-Loom-Sandbox-File": "private-details",
    }, text="private-error-body")
    error = SandboxRPCError(path, httpx.HTTPStatusError("private-error", request=request, response=response))
    expected = reason if path == "/file" else "http_error"
    assert str(error) == f"sandbox {'file_transfer' if path == '/file' else 'exec'} failed (HTTP 400; {expected})"


def test_unrecognized_file_reason_is_not_published() -> None:
    import httpx

    from loom.driver.service_sandbox import SandboxRPCError

    request = httpx.Request("PUT", "http://sandbox/file")
    response = httpx.Response(400, request=request, headers={"X-Loom-Sandbox-Error": "private-value"})
    error = SandboxRPCError("/file", httpx.HTTPStatusError("private-error", request=request, response=response))
    assert str(error) == "sandbox file_transfer failed (HTTP 400; http_error)"


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


@pytest.mark.asyncio
async def test_unreachable_sandbox_fails_start_with_a_safe_code(tmp_path: Path) -> None:
    driver = driver_for(tmp_path / "absent.sock")
    with pytest.raises(SandboxRPCError, match=r"^sandbox health failed \(transport_error\)$"):
        await driver.start()
    with pytest.raises(DriverNotStartedError):
        await driver.exec("true")


@pytest.mark.asyncio
async def test_download_http_rejection_keeps_status_and_safe_byte_diagnostics(tmp_path: Path) -> None:
    import httpx

    driver = driver_for(tmp_path / "unused.sock", limit=8)
    target = tmp_path.resolve() / "download"
    target.write_bytes(b"old")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        413, headers={"X-Loom-Sandbox-Error": "file_too_large",
                      "X-Loom-File-Size": "9", "X-Loom-File-Limit": "8"},
        text="private-path private-body",
    )), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(SandboxRPCError) as failure:
            await driver.download(PurePosixPath("/private-path"), target)
    assert "HTTP 413; file_too_large" in str(failure.value)
    assert "download" in str(failure.value) and "bytes=9" in str(failure.value)
    assert "limit=8" in str(failure.value) and "private-" not in str(failure.value)
    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
@pytest.mark.parametrize("declared", ["4", "9"])
async def test_download_inexact_body_does_not_publish_incomplete_file(tmp_path: Path, declared: str) -> None:
    import httpx

    driver = driver_for(tmp_path / "unused.sock")
    target = tmp_path.resolve() / "download"
    target.write_bytes(b"old")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, content=b"short", headers={"Content-Length": declared},
    )), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(DriverError, match="declared size"):
            await driver.download(PurePosixPath("/app/file"), target)
    assert target.read_bytes() == b"old"
    assert sorted(p.name for p in target.parent.iterdir()) == ["download"]


@pytest.mark.asyncio
async def test_workspace_archives_stream_both_ways_with_separate_budget(tmp_path: Path) -> None:
    import io
    import tarfile

    import httpx

    from loom.models.exec import ExecResult

    content = b"workspace" * 1000
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        info = tarfile.TarInfo("result.txt")
        info.size = len(content)
        tar.addfile(info, io.BytesIO(content))
    payload = buffer.getvalue()
    requests = []
    class ArchiveDriver(ServiceSandboxDriver):
        async def exec(self, cmd, **kwargs):
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)
    base = driver_for(tmp_path / "unused.sock", limit=8)
    driver = ArchiveDriver(tmp_path / "unused.sock", capabilities=base.capabilities,
        network_policy=Public(), max_transfer_bytes=8, max_workspace_archive_bytes=len(payload))
    async def handle(request):
        requests.append((request.method, request.url.path))
        if request.url.path == "/file":
            return httpx.Response(413, headers={"X-Loom-Sandbox-Error": "file_too_large"})
        assert request.url.path == "/workspace-archive"
        assert request.url.params["path"].startswith("/tmp/loom-workspace-")
        if request.method == "GET":
            return httpx.Response(200, content=payload)
        assert int(request.headers["Content-Length"]) == len(payload)
        body = bytearray()
        async for chunk in request.stream:
            assert len(chunk) <= 1024 * 1024
            body.extend(chunk)
        assert bytes(body) == payload
        return httpx.Response(204)
    archive = tmp_path.resolve() / "archive.tar"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://sandbox") as client:
        driver._client = client
        await driver.export_workspace_archive(PurePosixPath("/app"), archive)
        assert archive.read_bytes() == payload
        await driver.import_workspace_archive(archive, PurePosixPath("/app"))
        with pytest.raises(DriverError, match="transfer limit"):
            await driver.upload(archive, PurePosixPath("/app/ordinary"))
        with pytest.raises(SandboxRPCError, match="HTTP 413"):
            await driver.download(PurePosixPath("/app/ordinary"), archive)
    assert requests == [("GET", "/workspace-archive"), ("PUT", "/workspace-archive"), ("GET", "/file")]
    assert archive.read_bytes() == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [False, True])
async def test_download_interruption_is_atomic_and_safe(tmp_path: Path, workspace: bool) -> None:
    import httpx

    class Interrupted(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"part"
            raise httpx.ReadError("private-path private-body")
    driver = driver_for(tmp_path / "unused.sock", limit=16)
    target = tmp_path.resolve() / "download"
    target.write_bytes(b"old")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, headers={"Content-Length": "8"}, stream=Interrupted(),
    )), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(SandboxRPCError) as failure:
            await driver._download_file(PurePosixPath("/tmp/loom-workspace-" + "a"*32 + ".tar"),
                                        target, workspace_archive=workspace)
    assert "transport_error" in str(failure.value) and "private-" not in str(failure.value)
    assert target.read_bytes() == b"old" and not driver._requests
    assert [p.name for p in target.parent.iterdir()] == ["download"]


@pytest.mark.asyncio
async def test_workspace_download_rejects_oversized_metadata_before_body(tmp_path: Path) -> None:
    import httpx

    class Unreadable(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("oversized body must not be read")
            yield b""
    driver = driver_for(tmp_path / "unused.sock", limit=8)
    target = tmp_path.resolve() / "archive.tar"
    target.write_bytes(b"old")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, headers={"Content-Length": "9"}, stream=Unreadable(),
    )), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(DriverError, match="transfer limit"):
            await driver._download_file(PurePosixPath("/tmp/loom-workspace-" + "a"*32 + ".tar"),
                                        target, workspace_archive=True)
    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [b"tiny", b"changed!", b"grown-data"])
async def test_upload_detects_source_change_even_with_original_body_already_read(tmp_path: Path, mutation: bytes) -> None:
    import httpx

    source = tmp_path / "source"
    source.write_bytes(b"original")
    driver = driver_for(tmp_path / "unused.sock", limit=16)
    async def handle(request):
        async for chunk in request.stream:
            assert chunk == b"origina"
            source.write_bytes(mutation)
        return httpx.Response(204)
    class MutatingReceiver(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return await handle(request)
    async with httpx.AsyncClient(transport=MutatingReceiver(), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(DriverError, match=r"source (changed|grew)"):
            await driver.upload(source, PurePosixPath("/app/file"))


@pytest.mark.asyncio
async def test_workspace_transfer_streams_more_than_one_chunk_and_rejects_import_budget(tmp_path: Path) -> None:
    import httpx

    payload = b"archive" * 400_000
    source = tmp_path / "source"
    source.write_bytes(payload)
    base = driver_for(tmp_path / "unused.sock", limit=8)
    driver = ServiceSandboxDriver(tmp_path / "unused.sock", capabilities=base.capabilities,
        network_policy=Public(), max_transfer_bytes=8, max_workspace_archive_bytes=len(payload))
    class ArchiveStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for start in range(0, len(payload), 512 * 1024):
                yield payload[start:start + 512 * 1024]
    class StreamingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            assert request.url.path == "/workspace-archive"
            if request.method == "GET":
                return httpx.Response(200, stream=ArchiveStream(), headers={"Content-Length": str(len(payload))})
            sizes = []
            received = bytearray()
            async for chunk in request.stream:
                sizes.append(len(chunk))
                received.extend(chunk)
            assert len(sizes) > 1 and max(sizes) <= 1024 * 1024
            assert bytes(received) == payload
            return httpx.Response(204)
    destination = tmp_path.resolve() / "downloaded"
    remote = PurePosixPath("/tmp/loom-workspace-" + "a" * 32 + ".tar")
    async with httpx.AsyncClient(transport=StreamingTransport(), base_url="http://sandbox") as client:
        driver._client = client
        await driver._upload_file(source, remote, workspace_archive=True)
        await driver._download_file(remote, destination, workspace_archive=True)
        assert destination.read_bytes() == payload
        source.write_bytes(payload + b"x")
        with pytest.raises(DriverError, match="transfer limit"):
            await driver._upload_file(source, remote, workspace_archive=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("size", ["-1", "private-path", str(2**63)])
async def test_file_http_diagnostics_discard_untrusted_header_text(tmp_path: Path, size: str) -> None:
    import httpx

    driver = driver_for(tmp_path / "unused.sock")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(413,
        headers={"X-Loom-Sandbox-Error": "private-path", "X-Loom-File-Size": size,
                 "X-Loom-File-Limit": "private-body"})), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(SandboxRPCError) as failure:
            await driver.download(PurePosixPath("/private-path"), tmp_path.resolve() / "output")
    assert str(failure.value) == "sandbox file_transfer download failed (HTTP 413; http_error)"


@pytest.mark.asyncio
async def test_stop_cancels_workspace_download_without_publishing_partial_file(tmp_path: Path) -> None:
    import httpx

    started = asyncio.Event()
    closed = asyncio.Event()
    class PendingArchive(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"part"
            started.set()
            await asyncio.Event().wait()
        async def aclose(self):
            closed.set()
    driver = driver_for(tmp_path / "unused.sock")
    target = tmp_path.resolve() / "archive.tar"
    target.write_bytes(b"old")
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, stream=PendingArchive(), headers={"Content-Length": "8"})), base_url="http://sandbox")
    transfer = asyncio.create_task(driver._download_file(
        PurePosixPath("/tmp/loom-workspace-" + "a" * 32 + ".tar"), target, workspace_archive=True))
    await asyncio.wait_for(started.wait(), timeout=1)
    await driver.stop()
    assert transfer.cancelled() and closed.is_set() and not driver._requests
    assert target.read_bytes() == b"old"
    assert [p.name for p in target.parent.iterdir()] == ["archive.tar"]


@pytest.mark.asyncio
async def test_changed_upload_does_not_complete_real_http_body(tmp_path: Path, monkeypatch) -> None:
    """An HTTP receiver commits at Content-Length, not iterator completion."""
    import io
    import tempfile

    import httpx

    remote = b"old"
    handled = asyncio.Event()
    async def receive(reader, writer):
        nonlocal remote
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
            length = next(int(h.split(":", 1)[1]) for h in headers if h.lower().startswith("content-length:"))
            remote = await reader.readexactly(length)
            writer.write(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            handled.set()
    source = tmp_path / "source"
    source.write_bytes(b"A" * (2 * 1024 * 1024))
    original = asyncio.to_thread
    reads = 0
    async def mutate_after_read(fn, *args, **kwargs):
        nonlocal reads
        result = await original(fn, *args, **kwargs)
        if getattr(fn, "__name__", "") == "read" and isinstance(getattr(fn, "__self__", None), io.BufferedReader):
            reads += 1
            if reads == 2:
                with source.open("r+b") as output:
                    output.write(b"B")
        return result
    monkeypatch.setattr(asyncio, "to_thread", mutate_after_read)
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="loom-upload-") as directory:
        socket = Path(directory) / "rpc.sock"
        server = await asyncio.start_unix_server(receive, path=socket)
        driver = driver_for(socket, limit=4 * 1024 * 1024)
        async with server, httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=str(socket)), base_url="http://sandbox") as client:
            driver._client = client
            with pytest.raises(DriverError, match="source changed"):
                await driver._upload_file(source, PurePosixPath("/tmp/loom-workspace-" + "a" * 32 + ".tar"), workspace_archive=True)
            await asyncio.wait_for(handled.wait(), timeout=1)
    assert reads == 2 and remote == b"old"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"", b"x"])
async def test_upload_empty_and_single_byte_files_complete(tmp_path: Path, payload: bytes) -> None:
    import httpx

    source = tmp_path / "source"
    source.write_bytes(payload)
    driver = driver_for(tmp_path / "unused.sock")
    class Receiver(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            received = b"".join([chunk async for chunk in request.stream])
            assert received == payload
            assert int(request.headers["Content-Length"]) == len(payload)
            return httpx.Response(204)
    async with httpx.AsyncClient(transport=Receiver(), base_url="http://sandbox") as client:
        driver._client = client
        await driver._upload_file(source, PurePosixPath("/tmp/loom-workspace-" + "a" * 32 + ".tar"), workspace_archive=True)


@pytest.mark.asyncio
async def test_empty_upload_changed_before_request_never_sends_headers(tmp_path: Path, monkeypatch) -> None:
    import os

    import httpx

    source = tmp_path / "source"
    source.write_bytes(b"")
    driver = driver_for(tmp_path / "unused.sock")
    original = os.fstat
    calls = 0
    def mutate_after_stat(fd):
        nonlocal calls
        info = original(fd)
        calls += 1
        if calls == 1:
            source.write_bytes(b"grown")
        return info
    monkeypatch.setattr(os, "fstat", mutate_after_stat)
    class NoRequest(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise AssertionError("zero-length body can commit at headers; validate before sending")
    async with httpx.AsyncClient(transport=NoRequest(), base_url="http://sandbox") as client:
        driver._client = client
        with pytest.raises(DriverError, match="source grew"):
            await driver._upload_file(source, PurePosixPath("/tmp/loom-workspace-" + "a" * 32 + ".tar"), workspace_archive=True)
