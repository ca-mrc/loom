from pathlib import Path

import pytest

from loom.guest_compose import compile_native_compose


@pytest.mark.parametrize("body", [b"ready\n", b"guest-ready\n"])
def test_http_fixture_frames_the_actual_response_body(body):
    import subprocess

    from tests.integration.test_guest_compose import _http_fixture_command

    response = subprocess.check_output(_http_fixture_command(body)[-3:])
    headers, actual_body = response.split(b"\r\n\r\n", 1)
    assert actual_body == body
    assert f"Content-Length: {len(actual_body)}".encode() in headers.split(b"\r\n")


@pytest.mark.parametrize("failure", ["exception", "extra", "oversized", "invalid"])
async def test_http_state_diagnostics_do_not_leak_or_replace_original_failure(failure, capsys):
    import json
    import shlex
    from types import SimpleNamespace

    from loom.models.exec import ExecResult
    from tests.integration.test_guest_compose import _report_http_fixture_state

    safe = {"status": "running", "running": True, "dead": False,
            "oom_killed": False, "exit_code": 0, "health": "healthy"}
    calls = []

    async def execute(command, *, timeout_sec):
        calls.append((shlex.split(command), timeout_sec))
        if len(calls) == 1:
            if failure == "exception":
                raise RuntimeError("private-token-in-exception")
            payload = dict(safe)
            if failure == "extra":
                payload["Error"] = "private-token-in-state"
            if failure == "invalid":
                payload["status"] = "private-token-in-status"
            data = json.dumps(payload).encode()
            if failure == "oversized":
                data = b"private-token" * 200
        else:
            data = json.dumps(safe).encode()
        return ExecResult(return_code=0, stdout=data, stderr=b"private-token-in-stderr",
                          duration_sec=0)

    runtime = SimpleNamespace(guest=SimpleNamespace(exec=execute),
                              containers={"api": "a" * 64, "main": "b" * 64, "foreign": "c" * 64})
    with pytest.raises(AssertionError, match="original HTTP assertion"):
        await _report_http_fixture_state(runtime)
        raise AssertionError("original HTTP assertion")
    output = capsys.readouterr().out
    assert "private-token" not in output
    report = json.loads(output.removeprefix("compose-http-state "))
    assert report["main"] == safe
    assert report["api"] == {"state": "unavailable", "error_type":
                              "RuntimeError" if failure == "exception" else "ValueError"}
    assert [args[-1] for args, _ in calls] == ["a" * 64, "b" * 64]
    assert all(timeout == 5 for _, timeout in calls)
    for args, _ in calls:
        assert args[:3] == ["docker", "inspect", "--format"]
        assert all(value not in args[3] for value in ("Config", "Env", "Error", "Log", "Output"))


async def test_http_state_diagnostics_do_not_query_unowned_reference(capsys):
    from types import SimpleNamespace

    from tests.integration.test_guest_compose import _report_http_fixture_state

    async def execute(*args, **kwargs):
        pytest.fail("malformed or absent ownership must not reach Docker")

    await _report_http_fixture_state(SimpleNamespace(
        guest=SimpleNamespace(exec=execute), containers={"api": "foreign; echo private-token"}))
    output = capsys.readouterr().out
    assert "private-token" not in output
    assert output.count('"state": "unavailable"') == 2


def bundle(tmp_path: Path, yaml: str):
    environment = tmp_path / "environment"
    environment.mkdir()
    (environment / "Dockerfile").write_text("FROM alpine:3.21\n")
    (environment / "compose.yaml").write_text(yaml)
    return {"compose_files": ["environment/compose.yaml"], "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment", "cpus": 2, "memory_mb": 4096}


def test_preserves_shared_namespace_completed_dependency_and_exact_fields(tmp_path):
    env = bundle(tmp_path, """services:
  main:
    network_mode: service:api
    depends_on:
      seeder: {condition: service_completed_successfully}
    cap_add: [SYS_PTRACE]
  api:
    build: {context: ., no_cache: true}
    environment: [MODE=service]
    command: [sh, -c, sleep infinity]
    shm_size: 1gb
    cpus: 1
    expose: ['5000']
    healthcheck:
      test: [CMD-SHELL, 'test "$${MODE}" = service']
      start_period: 10s
      start_interval: 250ms
  seeder:
    image: alpine:3.21
    depends_on:
      api: {condition: service_healthy}
    command: [sh, -c, exit 0]
""")
    spec = compile_native_compose(tmp_path, env)
    assert [s.name for s in spec.ordered_services()] == ["api", "seeder", "main"]
    main, api, _ = spec.services
    assert main.command == ("sh", "-c", "sleep infinity")
    assert main.capabilities == ("SYS_PTRACE",)
    assert main.network_service == "api"
    assert api.build.no_cache
    assert api.healthcheck.start_interval_sec == .25
    assert api.healthcheck.command == 'test "${MODE}" = service'
    assert api.shm_size_bytes == 1024**3


@pytest.mark.parametrize("field", ["volumes: ['/var/run/docker.sock:/var/run/docker.sock']", "privileged: true",
    "network_mode: host", "devices: ['/dev/kvm']", "environment: [HOST_SECRET]"])
def test_host_escape_or_ambient_secret_inputs_fail_closed(tmp_path, field):
    env = bundle(tmp_path, "services:\n  main:\n    " + field + "\n")
    with pytest.raises(ValueError):
        compile_native_compose(tmp_path, env)


def test_missing_healthcheck_and_cycle_are_rejected(tmp_path):
    env = bundle(tmp_path, """services:
  main:
    depends_on: {db: {condition: service_healthy}}
  db:
    image: alpine:3.21
""")
    with pytest.raises(ValueError, match="healthcheck"):
        compile_native_compose(tmp_path, env)
    (tmp_path / "environment/compose.yaml").write_text("""services:
  main:
    depends_on: [db]
  db:
    image: alpine:3.21
    depends_on: [main]
""")
    with pytest.raises(ValueError, match="cycle"):
        compile_native_compose(tmp_path, env)


def test_host_environment_interpolation_is_never_read(tmp_path, monkeypatch):
    monkeypatch.setenv("OWNER_SECRET", "must-never-be-consumed")
    env = bundle(tmp_path, "services:\n  main:\n    environment: {TOKEN: '${OWNER_SECRET}'}\n")
    with pytest.raises(ValueError, match="interpolation"):
        compile_native_compose(tmp_path, env)


def test_guest_envelope_preserves_main_and_counts_explicit_and_policy_allocations():
    from loom.guest_compose import compose_resource_envelope
    from loom.models.compose import ComposeServiceSpec, NativeComposeSpec

    topology = NativeComposeSpec(source="compose.yaml", services=(
        ComposeServiceSpec(name="main", image="main", cpus=4, memory_mb=8192),
        ComposeServiceSpec(name="db", image="db", cpus=2, memory_mb=4096),
        ComposeServiceSpec(name="api", image="api"),
    ))
    envelope = compose_resource_envelope(topology, main_cpus=6, main_memory_mb=4096, main_storage_mb=10240)
    assert (envelope.cpus, envelope.memory_mb, envelope.storage_mb) == (10, 14336, 18432)
    assert (envelope.policy_cpus, envelope.policy_memory_mb, envelope.policy_storage_mb) == (2, 2048, 8192)


def test_outer_guest_controller_is_root_but_inner_authored_identity_is_preserved(monkeypatch):
    from loom.task_environment_handoff import (
        RESOLVED_TASK_ENVIRONMENT,
        decode_resolved_task_environment,
    )
    from tests.unit import test_compose_image_inputs as fixtures

    original = fixtures._independent_plan

    def nonroot():
        profile, task, trial, plan = original()
        task = task.model_copy(update={"environment": task.environment.model_copy(update={"user": "1000:1000"})})
        return profile, task, trial, plan

    monkeypatch.setattr(fixtures, "_independent_plan", nonroot)
    task, _, plan = fixtures.prepared_plan(native=True)
    sandbox = next(sidecar for sidecar in plan.sidecars if sidecar.role_name == "task-sandbox")
    assert sandbox.guest_task_identity.run_as_user == sandbox.guest_task_identity.run_as_group == 0
    frozen = decode_resolved_task_environment(plan.main.environment[RESOLVED_TASK_ENVIRONMENT])
    assert frozen.user == task.environment.user == "1000:1000"
    assert plan.setup[0].argv[-3] == "setup-native-compose"


def test_compose_acl_transfer_remains_explicitly_blocked():
    from loom.task_runtime_compatibility import task_runtime_rejections
    from tests.unit.test_compose_image_inputs import prepared_plan

    task, _, _ = prepared_plan(native=True)
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"preserve_acls": True})})
    assert "environment.preserve_acls: native_compose_acl_transfer_unavailable" in task_runtime_rejections(task)


def test_oci_transport_keeps_blobs_untagged_and_bounds_conversion_scratch(tmp_path, monkeypatch):
    import io
    import json
    import tarfile

    from loom.driver.guest_compose import _docker_transport_archive
    from loom.errors import DriverError

    source, target = tmp_path / "source.tar", tmp_path / "docker.tar"
    name = "blobs/sha256/" + "a" * 64
    blob = b"x" * 8192
    with tarfile.open(source, "w") as archive:
        entry = tarfile.TarInfo(name)
        entry.size = len(blob)
        archive.addfile(entry, io.BytesIO(blob))
    original = source.read_bytes()
    manifest = {"config": {"digest": "sha256:" + "a" * 64}, "layers": []}
    _docker_transport_archive(source, target, manifest)
    assert source.read_bytes() == original
    with tarfile.open(target) as archive:
        assert archive.extractfile(name).read() == blob
        assert json.load(archive.extractfile("manifest.json")) == [{"Config": name, "RepoTags": [], "Layers": []}]
    monkeypatch.setattr("loom.task_image_archives.MAX_ARCHIVE_BYTES", 12000)
    with pytest.raises(DriverError, match="scratch budget"):
        _docker_transport_archive(source, target, manifest)
    assert target.stat().st_size <= 12000


@pytest.fixture
def shell_control(tmp_path):
    """Execute the trusted control script without a Docker daemon or a VM."""
    import asyncio
    import shlex

    from loom.driver.fake import FakeDriver
    from loom.driver.guest_compose import GuestComposeRuntime
    from loom.models.compose import ComposeServiceSpec, NativeComposeSpec
    from loom.models.exec import ExecResult

    helper = tmp_path / "busybox"
    deny = tmp_path / "deny-touch"
    race = tmp_path / "remove-before-cat"
    helper.write_text(
        "#!/bin/sh\ncommand=$1; shift\n"
        f"if test \"$command\" = touch && test -e {deny}; then exit 9; fi\n"
        f"if test \"$command\" = cat && test -e {race}; then rm -f -- \"$1\"; fi\n"
        'exec "$command" "$@"\n'
    )
    helper.chmod(0o755)

    class ShellControl(FakeDriver):
        calls = 0
        observed = asyncio.Event()
        started = asyncio.Event()

        async def exec(self, command, **kwargs):
            self.calls += 1
            argv = shlex.split(command)
            process = await asyncio.create_subprocess_exec(
                *argv[argv.index("f" * 64) + 1:],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            self.started.set()
            stdout, stderr = await process.communicate()
            self.observed.set()
            return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)

    guest = ShellControl()
    runtime = GuestComposeRuntime(guest=guest, topology=NativeComposeSpec(
        source="compose.yaml", services=(ComposeServiceSpec(name="main", image="fixture"),)),
        image_archives={})
    runtime.containers["main"] = "f" * 64
    driver = runtime.driver("main")
    driver._SUPERVISOR = str(helper)
    return driver, guest, tmp_path / "marker", deny, race


async def test_activation_observes_identity_before_releasing_gate(shell_control):
    driver, guest, marker, _, _ = shell_control
    marker.write_text("123")
    await driver._activate(str(marker))
    assert marker.with_suffix(".go").is_file()
    assert guest.calls == 2


async def test_readiness_wait_does_not_restart_docker_for_a_late_marker(shell_control):
    import asyncio

    driver, guest, marker, _, _ = shell_control
    activation = asyncio.create_task(driver._activate(str(marker)))
    await guest.started.wait()
    await asyncio.sleep(.1)
    marker.write_text("123")
    await activation
    assert marker.with_suffix(".go").is_file()
    assert guest.calls == 2


@pytest.mark.parametrize("pid", ["", "1", "0", "2147483648", "123; touch injected"])
async def test_activation_invalid_identity_never_releases_gate(shell_control, pid):
    from loom.errors import DriverError

    driver, _, marker, _, _ = shell_control
    marker.write_text(pid)
    with pytest.raises(DriverError, match="identity is invalid"):
        await driver._activate(str(marker))
    assert not marker.with_suffix(".go").exists()


async def test_activation_failed_gate_is_not_retried_or_reported_ready(shell_control):
    from loom.errors import DriverError

    driver, guest, marker, deny, _ = shell_control
    marker.write_text("123")
    deny.touch()
    with pytest.raises(DriverError, match="activation failed"):
        await driver._activate(str(marker))
    assert not marker.with_suffix(".go").exists()
    assert guest.calls == 2


async def test_cancelled_activation_does_not_release_a_missing_marker(shell_control):
    import asyncio

    driver, guest, marker, _, _ = shell_control
    activation = asyncio.create_task(driver._activate(str(marker)))
    await guest.observed.wait()
    activation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await activation
    assert not marker.with_suffix(".go").exists()


async def test_cancelled_read_probe_cannot_activate_a_late_marker_after_cleanup(shell_control, monkeypatch):
    import asyncio

    driver, guest, marker, _, _ = shell_control
    original_exec = guest.exec
    probe_started, release_probe = asyncio.Event(), asyncio.Event()
    background = []

    async def delayed_exec(command, **kwargs):
        if " cat " in command and "kill -TERM" not in command:
            async def continue_on_server():
                await release_probe.wait()
                return await original_exec(command, **kwargs)

            # Killing a Docker client does not kill an accepted inner exec.
            # Keep running the real control shell after its RPC is cancelled.
            server = asyncio.create_task(continue_on_server())
            background.append(server)
            probe_started.set()
            return await asyncio.shield(server)
        return await original_exec(command, **kwargs)

    monkeypatch.setattr(guest, "exec", delayed_exec)
    activation = asyncio.create_task(driver._activate(str(marker)))
    try:
        await probe_started.wait()
        activation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await activation
        await driver._cleanup_control(str(marker))
        marker.write_text("123")  # The supervised exec starts after cleanup.
        release_probe.set()
        await asyncio.gather(*background)
        assert not marker.with_suffix(".go").exists()
    finally:
        release_probe.set()
        await asyncio.gather(*background, return_exceptions=True)


async def test_cleanup_accepts_marker_removed_by_a_completed_supervisor(shell_control):
    import os
    import subprocess

    driver, _, marker, _, race = shell_control
    process = subprocess.Popen(["/bin/sh", "-c", "exit 0"], start_new_session=True)
    assert process.wait() == 0
    with pytest.raises(ProcessLookupError):
        os.killpg(process.pid, 0)
    marker.write_text(str(process.pid))
    race.touch()  # The completed supervisor's EXIT trap wins before cat opens.
    await driver._cleanup_control(str(marker))
    assert not marker.exists()


@pytest.mark.parametrize("pid", ["", "1", "0", "2147483648", "123; touch injected"])
async def test_cleanup_rejects_an_existing_invalid_marker(shell_control, pid):
    from loom.errors import DriverError

    driver, _, marker, _, _ = shell_control
    marker.write_text(pid)
    with pytest.raises(DriverError, match="process group cleanup failed"):
        await driver._cleanup_control(str(marker))


async def test_cleanup_failure_keeps_the_original_activation_failure(shell_control, monkeypatch):
    from loom.errors import DriverError

    driver, _, _, _, _ = shell_control
    original = TimeoutError("activation expired")

    async def fail_activation(*args):
        raise original

    async def fail_cleanup(*args):
        raise DriverError("guest Compose process group cleanup failed")

    monkeypatch.setattr(driver, "_activate", fail_activation)
    monkeypatch.setattr(driver, "_cleanup_control", fail_cleanup)
    with pytest.raises(DriverError, match=r"activation failed.*TimeoutError.*cleanup failed") as error:
        await driver.exec("umask")
    assert error.value.__cause__ is original


async def test_stream_activation_cleanup_failure_still_kills_the_outer_handle(shell_control, monkeypatch):
    from pathlib import PurePosixPath

    from loom.driver.base import ExecHandle
    from loom.errors import DriverError

    driver, guest, _, _, _ = shell_control
    original = TimeoutError("private command text must not enter diagnostics")
    killed = []

    async def output():
        if False:
            yield b""

    async def wait():
        return 0

    async def kill():
        killed.append(True)

    async def stream(*args, **kwargs):
        return ExecHandle(pid=123, stdout=output(), stderr=output(), _wait=wait, _kill=kill)

    async def fail_activation(*args):
        raise original

    async def fail_cleanup(*args):
        raise DriverError("guest Compose process group cleanup failed")

    monkeypatch.setattr(guest, "exec_streaming", stream)
    monkeypatch.setattr(driver, "_activate", fail_activation)
    monkeypatch.setattr(driver, "_cleanup_control", fail_cleanup)
    with pytest.raises(DriverError, match=r"activation failed.*TimeoutError.*cleanup failed") as error:
        await driver.exec_streaming(["true"], env_vars={}, cwd=PurePosixPath("/"))
    assert error.value.__cause__ is original and killed == [True]
    assert "private command text" not in str(error.value)


async def test_control_pid_is_published_only_after_the_complete_write(tmp_path, monkeypatch):
    import asyncio
    import os
    import signal
    from types import SimpleNamespace

    from loom.driver.fake import FakeDriver
    from loom.driver.guest_compose import GuestComposeServiceDriver

    # Exercise the actual inner producer on a POSIX shell. env supplies the
    # supervisor's ordinary mv/rm/sleep commands; session ownership is covered
    # by the real-guest integration tests.
    monkeypatch.setattr(GuestComposeServiceDriver, "_SUPERVISOR", "/usr/bin/env")
    driver = GuestComposeServiceDriver(SimpleNamespace(guest=FakeDriver()), "main")
    argv, marker = driver._controlled(["/bin/sh", "-c", 'touch "$WITNESS_RAN"'])
    script = argv[argv.index("loom-exec") - 1]
    entered, release, ran = (tmp_path / name for name in ("entered", "release", "ran"))
    # Redirection opens the destination before the printf function runs. Pause
    # there deterministically, instead of depending on the CI scheduler's race.
    prelude = '''printf() {
        : > "$WITNESS_ENTERED"
        while test ! -f "$WITNESS_RELEASE"; do sleep 0.01; done
        command printf "$@"
    }; '''
    process = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c", prelude + script, "loom-exec", *argv[argv.index("loom-exec") + 1:],
        env={"PATH": os.defpath, "WITNESS_ENTERED": str(entered),
             "WITNESS_RELEASE": str(release), "WITNESS_RAN": str(ran)},
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        async with asyncio.timeout(5):
            while not entered.exists():
                await asyncio.sleep(.01)
        assert not Path(marker).exists(), "reader can observe a PID file before its contents exist"
        assert not ran.exists()
        release.touch()
        async with asyncio.timeout(5):
            while not Path(marker).exists():
                await asyncio.sleep(.01)
        assert Path(marker).read_text() == str(process.pid)
        assert Path(marker).stat().st_mode & 0o777 == 0o600
        assert not ran.exists(), "publication must not release the controller's activation gate"
        Path(marker + ".go").touch()
        assert await asyncio.wait_for(process.wait(), 5) == 0
        assert ran.exists()
        assert not Path(marker).exists()
        assert not Path(marker + ".pending").exists()
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        for suffix in ("", ".go", ".pending"):
            Path(marker + suffix).unlink(missing_ok=True)
