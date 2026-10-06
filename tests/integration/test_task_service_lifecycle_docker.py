"""Native process retention and private verification across one trial network."""
from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path, PurePosixPath

import docker
import httpx
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


@pytest.fixture(scope="module")
def native_runtime_tests(native_binary):  # noqa: F811
    """Reuse the native fixture's downloaded modules for an offline test build."""
    directory = native_binary.parent
    repository = Path(__file__).resolve().parents[2]
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{repository}:/src:ro", "-v", f"{directory}:/output", "-w", "/src",
        "-e", "GOCACHE=/tmp/go-cache", "-e", "GOMODCACHE=/output/modules",
        "-e", "CGO_ENABLED=0", "-e", "GOPROXY=off", "golang:1.26-alpine3.23",
        "go", "test", "-c", "-o", "/output/loom-sandbox-runtime.test", "./cmd/loom-sandbox-runtime",
    ], check=True, timeout=120, capture_output=True)
    return directory / "loom-sandbox-runtime.test"


@pytest.mark.parametrize("uid", [0, 65532], ids=["root", "nonroot"])
def test_native_cleanup_request_cancellation_as_pid1(native_runtime_tests, uid):
    """Run real process cleanup regressions that ordinary Go tests skip outside PID 1."""
    client = docker.from_env()
    container = None
    try:
        container = client.containers.run(
            "python:3.11-slim",
            ["-test.run", "^(TestStopProcesses|TestDeadlineAndDisconnect)", "-test.v", "-test.timeout", "20s"],
            entrypoint="/loom-sandbox-runtime.test", detach=True, user=f"{uid}:{uid}",
            network_mode="none", cap_drop=["ALL"], security_opt=["no-new-privileges"],
            volumes={str(native_runtime_tests): {"bind": "/loom-sandbox-runtime.test", "mode": "ro"}},
        )
        result = container.wait(timeout=30)
        output = container.logs().decode("utf-8", errors="replace")
        assert result["StatusCode"] == 0, output
        assert "--- PASS: TestStopProcessesFinishesAfterRequestCancellation" in output, output
        assert "--- PASS: TestDeadlineAndDisconnectKillChildren" in output, output
    finally:
        if container is not None:
            container.remove(force=True)
        client.close()


@pytest.fixture
async def service_sandboxes(native_binary, tmp_path):  # noqa: F811
    client = docker.from_env()
    containers, drivers = [], []
    try:
        for role in ("agent", "verifier"):
            socket = tmp_path / role
            socket.mkdir(mode=0o777)
            socket.chmod(0o2777)
            container = client.containers.run(
                "python:3.11-slim", ["--socket", "/socket/sandbox.sock"], detach=True,
                entrypoint="/loom/bin/loom-sandbox-runtime", user="0:0",
                network_mode="none" if not containers else f"container:{containers[0].id}",
                cap_drop=["ALL"], security_opt=["no-new-privileges"],
                volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                         str(socket): {"bind": "/socket", "mode": "rw"}},
            )
            containers.append(container)
            driver = ServiceSandboxDriver(socket / "sandbox.sock",
                capabilities=Capabilities(os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
                                          dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"})),
                network_policy=NoNetwork())
            for attempt in range(100):
                try:
                    await driver.start()
                    break
                except (OSError, RuntimeError, httpx.TransportError):
                    if attempt == 99:
                        raise
                    await asyncio.sleep(0.05)
            drivers.append(driver)
            assert (await driver.exec("mkdir -p /app /data")).return_code == 0
        yield drivers
    finally:
        for driver in drivers:
            await driver.stop()
        for container in reversed(containers):
            container.remove(force=True)
        client.close()


async def test_native_workspace_handoff_keeps_deletions_and_private_inputs(service_sandboxes):
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_snapshot import handoff_workspace_snapshot

    agent, verifier = service_sandboxes
    for driver in (agent, verifier):
        assert (await driver.exec(
            "mkdir -p /app/data && echo keep > /app/data/file && ln -s data /app/foo",
        )).return_code == 0
    assert (await agent.exec("unlink /app/foo")).return_code == 0
    assert (await verifier.exec("mkdir /app/tests && echo private > /app/tests/secret")).return_code == 0
    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier, workdir=PurePosixPath("/app"),
        policy=WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ()),
    )
    checked = await verifier.exec(
        "test ! -L /app/foo && test ! -e /app/foo && "
        'test "$(cat /app/data/file)" = keep && test "$(cat /app/tests/secret)" = private',
    )
    assert checked.return_code == 0, checked.stderr


async def test_service_survives_consistent_snapshot_and_private_verifier(service_sandboxes, tmp_path):
    agent, verifier = service_sandboxes
    checked = await verifier.exec("mkdir -p /tests; echo private > /tests/secret")
    assert checked.return_code == 0
    started = await agent.exec(
        "test ! -e /tests/secret && echo response > /data/answer && "
        "(python -m http.server 8000 --bind 127.0.0.1 --directory /data >/tmp/service.log 2>&1 & "
        "echo $! > /tmp/service.pid) && "
        "(while true; do echo tick >> /data/heartbeat; sleep .02; done) >/dev/null 2>&1 &",
    )
    assert started.return_code == 0, started.stderr
    request = "python -c \"import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/answer').read().decode().strip())\""
    for _attempt in range(50):
        ready = await verifier.exec(request)
        if ready.return_code == 0:
            break
        await asyncio.sleep(0.02)
    assert ready.stdout.strip() == b"response", ready.stderr
    await agent.pause_processes()
    before = await agent.exec("wc -c < /data/heartbeat")
    await asyncio.sleep(0.1)
    after = await agent.exec("wc -c < /data/heartbeat")
    assert before.stdout == after.stdout
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await agent.resume_processes()
    assert (await verifier.exec(request)).stdout.strip() == b"response"
    checked = await verifier.exec("test \"$(cat /tests/secret)\" = private && test \"$(cat /data/answer)\" = response")
    assert checked.return_code == 0, checked.stderr
    assert (await agent.exec("test ! -e /tests/secret")).return_code == 0
    await agent.stop_processes()
    assert (await verifier.exec(request)).return_code != 0
    assert (await agent.exec("test ! -e /proc/$(cat /tmp/service.pid)")).return_code == 0


@pytest.mark.parametrize("agent_kills_listener", [True, False])
async def test_startup_only_readiness_grades_actual_post_agent_process_state(
    service_sandboxes, tmp_path, monkeypatch, agent_kills_listener,
):
    import json
    from uuid import uuid4

    from loom import service_execution_sandbox_task as module
    from loom.models.task import TaskConfig
    from tests.unit.test_service_execution_terminus_plan import _inputs

    agent, verifier = service_sandboxes
    task, trial, _ = _inputs()
    ready = (
        "python -c 'import socket; "
        'socket.create_connection(("127.0.0.1", 8080), timeout=1).close()\''
    )
    raw = task.model_dump(mode="json")
    raw["environment"]["service_lifecycle"] = {
        "startup_command": ["/bin/sh", "-c", "python -m http.server 8080 --bind 127.0.0.1 "
                            ">/tmp/listener.log 2>&1 </dev/null & echo $! > /tmp/listener.pid"],
        "readiness": {"command": ready, "interval_sec": 0.05, "retries": 40},
        "readiness_scope": "startup_only",
    }
    raw["verifier"]["args"]["script_path"] = "verifier/run.sh"
    task = TaskConfig.model_validate(raw)
    workspace = tmp_path / "workspace"
    (workspace / "verifier").mkdir(parents=True)
    (workspace / "instruction.md").write_text("Find and terminate the process using port 8080.")
    (workspace / "verifier/run.sh").write_text("""python - <<'PY'
import json, os, socket
with socket.socket() as client:
    client.settimeout(1)
    occupied = client.connect_ex(('127.0.0.1', 8080)) == 0
os.makedirs(os.path.dirname(os.environ['LOOM_VERIFIER_OUTPUT']), exist_ok=True)
with open(os.environ['LOOM_VERIFIER_OUTPUT'], 'w') as report:
    json.dump({'rewards': {'passed': int(not occupied)}}, report)
PY
""")

    def connect(role, _task, _trial):
        name = "agent" if role == "task-sandbox" else "verifier"
        return ServiceSandboxDriver(
            tmp_path / name / "sandbox.sock", capabilities=agent.capabilities,
            network_policy=NoNetwork(),
        )

    async def identity(_):
        return uuid4(), uuid4()

    async def act(**kwargs):
        driver = kwargs["driver"]
        assert (await driver.exec(ready)).return_code == 0, "startup must establish the listener"
        assert (await driver.exec("test ! -e /app/verifier/run.sh")).return_code == 0
        if agent_kills_listener:
            assert (await driver.exec("kill $(cat /tmp/listener.pid)")).return_code == 0
            for _ in range(50):
                if (await driver.exec(ready)).return_code != 0:
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("agent did not stop the listener")

    monkeypatch.setattr(module, "sandbox_driver", connect)
    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", act)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    await module.run_agent(workspace, task, trial)
    # Snapshot handoff must not kill an agent-surviving listener or restart a
    # listener the agent killed; either would change the verifier's answer.
    assert ((await verifier.exec(ready)).return_code == 0) is not agent_kills_listener
    await module.run_verifier(workspace, task, trial)
    report = json.loads((workspace / ".loom/verifier/output.json").read_text())
    assert report["rewards"]["passed"] == int(agent_kills_listener)
    assert (await agent.exec("test ! -e /proc/$(cat /tmp/listener.pid)")).return_code == 0


@pytest.mark.parametrize("termination", ["signal", "deadline"])
async def test_real_sigterm_unwinds_verifier_and_stops_retained_service(service_sandboxes, tmp_path, termination):
    import os
    import signal
    import sys

    from loom.trial.workspace_snapshot import _export_workspace_archive
    from tests.unit.test_service_execution_terminus_plan import _inputs

    agent, verifier = service_sandboxes
    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["service_lifecycle"] = {"readiness": {"command": "true"}}
    raw["verifier"]["args"]["script_path"] = "verifier/run.sh"
    workspace = tmp_path / "workspace"
    (workspace / "verifier").mkdir(parents=True)
    (workspace / "verifier/run.sh").write_text("touch /tmp/verifier-entered\nsleep 300\n")
    (workspace / ".loom").mkdir()
    await _export_workspace_archive(agent, PurePosixPath("/app"), workspace / ".loom/workspace.tar")
    assert (await agent.exec("sleep 300 >/dev/null 2>&1 & echo $! > /tmp/retained.pid")).return_code == 0
    child = """
import asyncio, json, os
from pathlib import Path
from loom import service_execution_sandbox_task as module
from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
root = Path(os.environ['LOOM_TEST_ROOT'])
def connect(role, task, trial):
    name = 'agent' if role == 'task-sandbox' else 'verifier'
    return ServiceSandboxDriver(root / name / 'sandbox.sock',
        capabilities=Capabilities(os='linux', gpu_vendor='none', network_policies=frozenset({'no-network'}),
                                  dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({'limit'})),
        network_policy=NoNetwork())
module.sandbox_driver = connect
asyncio.run(module.run_verifier(root / 'workspace', TaskConfig.model_validate_json(os.environ['LOOM_TEST_TASK']),
                               TrialConfig.model_validate_json(os.environ['LOOM_TEST_TRIAL'])))
"""
    import json
    import time

    process = await asyncio.create_subprocess_exec(sys.executable, "-c", child, env={
        **os.environ, "LOOM_TEST_ROOT": str(tmp_path), "LOOM_TEST_TASK": json.dumps(raw),
        "LOOM_TEST_TRIAL": trial.model_dump_json(),
        "LOOM_EXECUTION_PHASE_DEADLINE": str(time.time() + (60 if termination == "signal" else 1.5)),
        "LOOM_EXECUTION_TERMINATION_GRACE_SECONDS": "5",
    }, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        for _ in range(100):
            if (await verifier.exec("test -e /tmp/verifier-entered")).return_code == 0:
                break
            await asyncio.sleep(0.02)
        else:
            if process.returncode is not None:
                stdout, stderr = await process.communicate()
                raise AssertionError(f"verifier did not enter: {stdout!r} {stderr!r}")
            raise AssertionError("verifier did not enter")
        if termination == "signal":
            process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
        assert process.returncode != 0, (stdout, stderr)
        checked = await agent.exec("test ! -e /proc/$(cat /tmp/retained.pid)")
        assert checked.return_code == 0, "retained service survived verifier SIGTERM"
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
