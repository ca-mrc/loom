"""Real staging filesystem with failures injected only at the native RPC."""
import asyncio
import json
import shutil
import tarfile
import tempfile
from contextlib import suppress
from pathlib import Path, PurePosixPath

import httpx
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverError
from loom.models.exec import ExecResult
from tests.unit.test_service_sandbox_driver import driver_for


class LocalFilesystemDriver(ServiceSandboxDriver):
    async def exec(self, cmd, **kwargs):
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)

    async def _upload_file(self, src, dst, *, workspace_archive=False):
        shutil.copyfile(src, str(dst))


@pytest.mark.parametrize("failure", ["invalid-archive", "ambiguous-promotion"])
async def test_failed_restore_preserves_baseline_and_does_not_race_native_promotion(tmp_path, failure):
    class LocalTransport(LocalFilesystemDriver):
        promotion_requested = False

        async def _request(self, method, path, **kwargs):
            assert method == "POST" and path == "/restore-directory"
            request = kwargs["json"]
            stage = Path(request["root"]) / request["stage"]
            assert (stage / "new").read_text() == "new state"
            assert (destination / "baseline").read_text() == "original state"
            self.promotion_requested = True
            raise DriverError("ambiguous promotion transport failure")

    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "new").write_text("new state")
    (destination / "baseline").write_text("original state")
    archive = tmp_path / "state.tar"
    if failure == "invalid-archive":
        archive.write_bytes(b"not a tar archive")
    else:
        with tarfile.open(archive, "w") as stream:
            stream.add(source, arcname=".")
    base = driver_for(tmp_path / "unused.sock")
    driver = LocalTransport(tmp_path / "unused.sock", capabilities=base.capabilities,
                            network_policy=base._network_policy)
    expected = tarfile.ReadError if failure == "invalid-archive" else DriverError
    with pytest.raises(expected):
        await driver.replace_workspace_archive(archive, PurePosixPath(str(destination)))
    assert (destination / "baseline").read_text() == "original state"
    stages = list(destination.glob(".loom-restore-*"))
    if failure == "invalid-archive":
        assert not driver.promotion_requested and not stages
    else:
        assert driver.promotion_requested and len(stages) == 1
        assert (stages[0] / "new").read_text() == "new state"


async def test_directory_promotion_can_exceed_the_healthcheck_rpc_timeout(tmp_path):
    destination = tmp_path / "destination"
    destination.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    (source / "new").write_text("restored")
    archive = tmp_path / "state.tar"
    with tarfile.open(archive, "w") as stream:
        stream.add(source, arcname=".")
    finished = asyncio.Event()

    async def handle(reader, writer):
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
            assert headers[0] == "POST /restore-directory HTTP/1.1"
            length = next(int(h.split(":", 1)[1]) for h in headers if h.lower().startswith("content-length:"))
            request = json.loads(await reader.readexactly(length))
            stage = Path(request["root"]) / request["stage"]
            # A short default models the healthcheck deadline independently of
            # the filesystem operation's larger budget, without a slow test.
            await asyncio.sleep(0.1)
            (stage / "new").rename(destination / "new")
            stage.rmdir()
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            with suppress(ConnectionError):
                await writer.drain()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            finished.set()

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="loom-restore-") as directory:
        socket = Path(directory) / "rpc.sock"
        server = await asyncio.start_unix_server(handle, path=socket)
        base = driver_for(socket)
        driver = LocalFilesystemDriver(socket, capabilities=base.capabilities, network_policy=base._network_policy)
        async with server, httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(socket)), base_url="http://sandbox", timeout=0.01,
        ) as client:
            driver._client = client
            try:
                await driver.replace_workspace_archive(archive, PurePosixPath(str(destination)))
                assert (destination / "new").read_text() == "restored"
            finally:
                await asyncio.wait_for(finished.wait(), 2)


@pytest.mark.parametrize('failure', ['second-stage', 'second-promotion'])
async def test_group_restore_cleans_all_stages_only_before_promotion(tmp_path, failure):
    destinations = [tmp_path / str(index) for index in range(2)]
    archives = []
    for index, destination in enumerate(destinations):
        destination.mkdir()
        (destination / 'baseline').write_text('original')
        source = tmp_path / f'source{index}'
        source.mkdir()
        (source / 'new').write_text('new')
        archive = tmp_path / f'{index}.tar'
        with tarfile.open(archive, 'w') as stream:
            stream.add(source, arcname='.')
        archives.append((archive, PurePosixPath(str(destination))))
    if failure == 'second-stage':
        archives[1][0].write_bytes(b'invalid archive')

    class Transport(LocalFilesystemDriver):
        promotions = 0

        async def _request(self, method, path, **kwargs):
            assert method == 'POST' and path == '/restore-directory'
            # Every extraction must be complete before the first promotion.
            for destination in destinations:
                stage, = destination.glob('.loom-restore-*')
                assert (stage / 'new').read_text() == 'new'
            self.promotions += 1
            if self.promotions == 2:
                raise DriverError('ambiguous second promotion')

    base = driver_for(tmp_path / 'unused.sock')
    driver = Transport(tmp_path / 'unused.sock', capabilities=base.capabilities,
                       network_policy=base._network_policy)
    expected = tarfile.ReadError if failure == 'second-stage' else DriverError
    with pytest.raises(expected):
        await driver.replace_mutable_archives(tuple(archives))
    assert driver.promotions == (0 if failure == 'second-stage' else 2)
    for destination in destinations:
        assert (destination / 'baseline').read_text() == 'original'
        assert len(list(destination.glob('.loom-restore-*'))) == (0 if failure == 'second-stage' else 1)
