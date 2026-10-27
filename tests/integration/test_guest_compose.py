"""Real Docker topology semantics; guest kernel isolation has its own fixture.

The control adapter invokes a disposable local Docker project without exposing
its daemon to any service. This test needs no model, credentials or cloud node.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import shlex
import shutil
import signal
import subprocess
import tarfile
from pathlib import PurePosixPath

import pytest

from loom.driver.base import ExecHandle
from loom.driver.fake import FakeDriver
from loom.driver.guest_compose import GuestComposeRuntime
from loom.errors import DriverError
from loom.models.compose import ComposeServiceSpec, NativeComposeSpec
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec

pytestmark = [pytest.mark.integration, pytest.mark.docker]


class Control(FakeDriver):
    async def exec(self, cmd, *, cwd=None, timeout_sec=None, **kwargs):
        process = await asyncio.create_subprocess_shell(cmd, cwd=str(cwd) if cwd else None,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout_sec)
        except BaseException:
            if process.returncode is None:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            raise
        return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)

    async def exec_streaming(self, argv, *, cwd=None, env_vars=None, **kwargs):
        process = await asyncio.create_subprocess_exec(*argv, cwd=str(cwd) if cwd else None,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        async def chunks(stream):
            while data := await stream.read(65536):
                yield data
        async def kill():
            if process.returncode is None:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        return ExecHandle(pid=process.pid, stdout=chunks(process.stdout), stderr=chunks(process.stderr),
                          _wait=process.wait, _kill=kill)

    async def upload(self, source, target):
        shutil.copyfile(source, target)

    async def download(self, source, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)


async def assert_inner_commands_cancel_without_stopping_main(main, *, activation_response_delay=0):
    baseline = await main.runtime.guest.exec(shlex.join(main._exec(["/bin/sh", "-c", "umask"])))
    controlled = await main.exec("umask")
    assert baseline.return_code == controlled.return_code == 0
    assert controlled.stdout.strip() == baseline.stdout.strip()
    activated = asyncio.Event()
    activate = main._activate

    async def observe_activation(*args, **kwargs):
        await activate(*args, **kwargs)
        # Model a legal slow control response after the guest has released the
        # command. A fixed sleep before writing would race cancellation here.
        await asyncio.sleep(activation_response_delay)
        activated.set()

    main._activate = observe_activation
    try:
        operation = asyncio.create_task(main.exec(
            "while test ! -e /tmp/release-cancelled-exec; do sleep 0.1; done; "
            "echo late >/tmp/cancelled-exec"))
        ready = asyncio.create_task(activated.wait())
        await asyncio.wait((operation, ready), return_when=asyncio.FIRST_COMPLETED)
        if operation.done():
            ready.cancel()
            await operation
        await ready
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        handle = await main.exec_streaming([
            "sh", "-c", "while test ! -e /tmp/release-cancelled-stream; do sleep 0.1; done; "
            "echo late >/tmp/cancelled-stream"], env_vars={}, cwd=PurePosixPath("/tmp"))
    finally:
        main._activate = activate
    await handle.kill()

    async def drain(stream):
        async for _ in stream:
            pass
    await asyncio.gather(drain(handle.stdout), drain(handle.stderr), handle.wait())
    # Neither writer may advance until both cancellation cleanup calls return.
    # Releasing from the same still-usable main container makes an orphan write
    # observable, while the bounded poll also verifies collection can continue.
    collection = await main.exec(
        "touch /tmp/release-cancelled-exec /tmp/release-cancelled-stream; i=0; "
        "while test \"$i\" -lt 30; do "
        "if test -e /tmp/cancelled-exec; then echo cancelled-exec-present >&2; exit 1; fi; "
        "if test -e /tmp/cancelled-stream; then echo cancelled-stream-present >&2; exit 1; fi; "
        "sleep 0.1; i=$((i+1)); done; echo collection-ready")
    assert collection.return_code == 0 and collection.stdout.strip() == b"collection-ready", {
        "return_code": collection.return_code,
        "stdout": collection.stdout[:512], "stderr": collection.stderr[:512],
    }


@pytest.fixture
def image(tmp_path):
    raw = tmp_path / "raw.tar"
    if subprocess.run(["docker", "image", "inspect", "alpine:3.21"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        subprocess.run(["docker", "pull", "alpine:3.21"], check=True, stdout=subprocess.DEVNULL)
    architecture = subprocess.check_output(["docker", "image", "inspect", "alpine:3.21", "--format", "{{.Architecture}}"], text=True).strip()
    container = subprocess.check_output(["docker", "create", "alpine:3.21"], text=True).strip()
    try:
        subprocess.run(["docker", "cp", container + ":/bin/busybox", str(tmp_path / "busybox")], check=True)
    finally:
        subprocess.run(["docker", "rm", container], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "image", "save", "alpine:3.21", "--output", str(raw)], check=True)
    target = tmp_path / "image.tar"
    with tarfile.open(raw) as source, tarfile.open(target, "w") as output:
        index = json.load(source.extractfile("index.json"))
        descriptor = index["manifests"][0]
        if descriptor["mediaType"].endswith("image.index.v1+json"):
            nested = json.load(source.extractfile("blobs/sha256/" + descriptor["digest"].removeprefix("sha256:")))
            descriptor = next(d for d in nested["manifests"] if d.get("platform", {}).get("architecture") == architecture)
        index["manifests"] = [descriptor]
        descriptor.pop("annotations", None)
        for member in source:
            # Keep a pure, untagged OCI archive. Docker's legacy manifest.json
            # carries the developer's original tag and must not retag it.
            if member.name == "manifest.json":
                continue
            if member.name == "index.json":
                data = json.dumps(index).encode()
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
            else:
                output.addfile(member, source.extractfile(member) if member.isfile() else None)
    return "loom-compose-fixture@" + descriptor["digest"], target


async def test_health_completed_dependency_namespace_sharing_private_environment_and_cleanup(image, tmp_path):
    reference, archive = image
    services = (
        ComposeServiceSpec(name="api", image=reference,
            environment={"SERVICE_ONLY": "fixture-private-value"},
            command=("sh", "-c", "while true; do printf 'HTTP/1.0 200 OK\\r\\n\\r\\nready\\n' | nc -l -p 8080; done"),
            healthcheck=HealthcheckSpec(command="wget -qO- http://localhost:8080", interval_sec=.1, timeout_sec=2, retries=20)),
        ComposeServiceSpec(name="seeder", image=reference, depends_on={"api": "service_healthy"},
            command=("sh", "-c", "wget -qO- http://api:8080 >/seeded")),
        ComposeServiceSpec(name="main", image=reference, network_service="api",
            depends_on={"seeder": "service_completed_successfully"}, command=("sleep", "infinity"), user="1000:1000"),
    )
    runtime = GuestComposeRuntime(guest=Control(), topology=NativeComposeSpec(source="environment/compose.yaml", services=services),
                                 image_archives=dict.fromkeys((s.name for s in services), archive), startup_timeout_sec=40,
                                 supervisor_path=str(archive.parent / "busybox"))
    try:
        await runtime.start()
        receipt = tmp_path / "runtime.json"
        runtime.save(receipt)
        original_containers = dict(runtime.containers)
        resumed = GuestComposeRuntime(guest=Control(), topology=runtime.topology,
            image_archives=runtime.image_archives)
        await resumed.attach(receipt)
        assert resumed.containers == original_containers
        runtime = resumed
        main = runtime.driver("main")
        await assert_inner_commands_cancel_without_stopping_main(main, activation_response_delay=3.5)
        with pytest.raises(TimeoutError):
            await main.exec("sleep 3; echo late >/tmp/timed-out-exec", timeout_sec=1)
        await asyncio.sleep(3.1)
        assert (await main.exec("test ! -e /tmp/timed-out-exec")).return_code == 0
        response = await main.exec("wget -qO- http://localhost:8080")
        assert response.return_code == 0 and response.stdout.strip() == b"ready"
        isolation = await main.exec('test ! -S /var/run/docker.sock && test -z "$SERVICE_ONLY"')
        assert isolation.return_code == 0
        assert (await runtime.driver("api").exec('printf "$SERVICE_ONLY"')).stdout == b"fixture-private-value"
        local = tmp_path / "answer.txt"
        local.write_text("output")
        await main.upload(local, PurePosixPath("/tmp/task-output/answer.txt"))
        assert (await main.exec("test -w /tmp/task-output/answer.txt")).return_code == 0
        await main.download(PurePosixPath("/tmp/task-output/answer.txt"), tmp_path / "readback.txt")
        assert (tmp_path / "readback.txt").read_text() == "output"
        with tarfile.open(tmp_path / "input.tar", "w") as source:
            source.add(local, arcname="imported.txt")
        await main.import_workspace_archive(tmp_path / "input.tar", PurePosixPath("/tmp/imported"))
        assert (await main.exec("test -w /tmp/imported/imported.txt")).return_code == 0
        await main.stop_processes()
        await main.export_workspace_archive(PurePosixPath("/tmp/task-output"), tmp_path / "workspace.tar")
        with tarfile.open(tmp_path / "workspace.tar") as tar:
            assert any(m.name.endswith("answer.txt") for m in tar)
    finally:
        await runtime.stop()
    assert not subprocess.check_output(["docker", "ps", "-aq", "--filter", "label=loom.compose=" + runtime.project]).strip()
    assert not subprocess.check_output(["docker", "network", "ls", "-q", "--filter", "label=loom.compose=" + runtime.project]).strip()


async def test_failed_initializer_cannot_start_main_and_retires_owned_resources(image):
    reference, archive = image
    services = (
        ComposeServiceSpec(name="seed", image=reference, command=("sh", "-c", "exit 7")),
        ComposeServiceSpec(name="main", image=reference, depends_on={"seed": "service_completed_successfully"}, command=("sleep", "infinity")),
    )
    runtime = GuestComposeRuntime(guest=Control(), topology=NativeComposeSpec(source="environment/compose.yaml", services=services),
                                 image_archives=dict.fromkeys((s.name for s in services), archive), startup_timeout_sec=40,
                                 supervisor_path=str(archive.parent / "busybox"))
    with pytest.raises(DriverError, match="initialization service failed"):
        await runtime.start()
    assert runtime.containers == {}
    assert not subprocess.check_output(["docker", "ps", "-aq", "--filter", "label=loom.compose=" + runtime.project]).strip()


async def test_cancellation_witness_detects_orphaned_inner_commands(image):
    reference, archive = image
    runtime = GuestComposeRuntime(guest=Control(), topology=NativeComposeSpec(
        source="compose.yaml", services=(ComposeServiceSpec(name="main", image=reference,
            command=("sleep", "infinity"), user="1000:1000"),)),
        image_archives={"main": archive}, supervisor_path=str(archive.parent / "busybox"))
    try:
        await runtime.start()
        main = runtime.driver("main")

        async def skip_inner_cleanup(marker):
            pass

        # Negative control: killing Docker's CLI alone must fail the witness.
        # All deliberately orphaned processes remain in this disposable project.
        main._cleanup_control = skip_inner_cleanup
        with pytest.raises(AssertionError, match=r"cancelled-(exec|stream)-present"):
            await assert_inner_commands_cancel_without_stopping_main(main)
    finally:
        await runtime.stop()
    assert not subprocess.check_output(["docker", "ps", "-aq", "--filter", "label=loom.compose=" + runtime.project]).strip()
    assert not subprocess.check_output(["docker", "network", "ls", "-q", "--filter", "label=loom.compose=" + runtime.project]).strip()


@pytest.mark.timeout(240)
async def test_real_guest_compose_control_channel_and_service_isolation(image, tmp_path, monkeypatch):
    from loom.driver.service_sandbox import ServiceSandboxDriver
    from loom.models.networking import PublicWeb
    from tests.integration.test_guest_sandbox_runtime import guest

    reference, archive = image
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/host-only-missing")
    services = (
        ComposeServiceSpec(name="api", image=reference, environment={"SERVICE_ONLY": "guest-private"},
            command=("sh", "-c", "while true; do printf 'HTTP/1.0 200 OK\\r\\n\\r\\nguest-ready\\n' | nc -l -p 8080; done"),
            healthcheck=HealthcheckSpec(command="wget -qO- http://localhost:8080", interval_sec=1, timeout_sec=5, retries=20)),
        ComposeServiceSpec(name="main", image=reference, network_service="api",
            depends_on={"api": "service_healthy"}, command=("sleep", "infinity"), user="1000:1000"),
    )
    with guest(docker=True, storage_mib=2048, exec_timeout_seconds=120) as (_, _, directory):
        control = ServiceSandboxDriver(directory / "sandbox.sock", capabilities=Control().capabilities,
                                       network_policy=PublicWeb())
        await control.start()
        runtime = GuestComposeRuntime(guest=control,
            topology=NativeComposeSpec(source="compose.yaml", services=services),
            image_archives=dict.fromkeys((service.name for service in services), archive), startup_timeout_sec=120)
        try:
            await runtime.start()
            main = runtime.driver("main")
            await assert_inner_commands_cancel_without_stopping_main(main)
            response = await main.exec('test ! -S /var/run/docker.sock && test -z "$SERVICE_ONLY" && wget -qO- http://localhost:8080')
            assert response.return_code == 0 and response.stdout.strip() == b"guest-ready"
            assert (await main.exec("id -u")).stdout.strip() == b"1000"
            artifact = tmp_path / "answer.txt"
            artifact.write_text("guest-output")
            await main.upload(artifact, PurePosixPath("/tmp/task-output/answer.txt"))
            assert (await main.exec("test -w /tmp/task-output/answer.txt")).return_code == 0
            await main.download(PurePosixPath("/tmp/task-output/answer.txt"), tmp_path / "readback.txt")
            assert (tmp_path / "readback.txt").read_text() == "guest-output"
            await main.stop_processes()
            await main.export_workspace_archive(PurePosixPath("/tmp/task-output"), tmp_path / "workspace.tar")
            with tarfile.open(tmp_path / "workspace.tar") as exported:
                assert any(member.name.endswith("answer.txt") for member in exported)
        finally:
            await runtime.stop()
            result = await control.exec("docker ps -aq --filter label=loom.compose=" + runtime.project)
            assert result.return_code == 0 and not result.stdout.strip()
            networks = await control.exec("docker network ls -q --filter label=loom.compose=" + runtime.project)
            assert networks.return_code == 0 and not networks.stdout.strip()
            await control.stop()
