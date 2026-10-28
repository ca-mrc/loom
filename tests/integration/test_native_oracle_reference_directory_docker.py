"""Prepared nonroot image + real native RPC preserves Harbor reference layout."""

from __future__ import annotations

import asyncio
import io
import sys
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from uuid import uuid4

import httpx
import pytest

from loom.driver.service_sandbox import SandboxRPCError, ServiceSandboxDriver
from loom.harbor_verifier_script import native_verifier_run_sh_bytes
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.models.trial import TrialConfig
from loom.nebius_terminus_image import append_terminus_runtime_tools
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401
from tests.unit.test_service_execution_terminus_plan import _inputs

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


@pytest.fixture(scope="module")
def reference_image():
    import docker

    client = docker.from_env()
    tag = "loom-oracle-reference-" + uuid4().hex
    authored = (
        "FROM python:3.12-slim\n"
        "RUN mkdir -p /app/output && chmod 755 /app && chown nobody:nogroup /app/output\n"
        "USER nobody\nWORKDIR /app\n"
    )
    prepared = append_terminus_runtime_tools(authored, prepare_harness_dirs=True)
    image = None
    try:
        image, _ = client.images.build(
            fileobj=io.BytesIO(prepared.encode()),
            tag=tag,
            rm=True,
            labels={"loom.task": "2292-oracle-reference"},
        )
        assert image.attrs["Config"]["User"] == "nobody"
        assert image.attrs["Config"]["WorkingDir"] == "/app"
        yield client, image.id
    finally:
        if image is not None:
            client.images.remove(image.id, force=True)
        client.close()


@pytest.mark.parametrize("agent_name", ["oracle", "terminus-2"])
async def test_nonroot_harbor_reference_tree_and_cleanup_use_original_user(
    native_binary,  # noqa: F811
    reference_image,
    tmp_path,
    monkeypatch,
    agent_name,
):
    from loom import service_execution_sandbox_task as module

    client, image = reference_image
    socket_directory = TemporaryDirectory(
        prefix="or-", dir="/private/tmp" if sys.platform == "darwin" else None
    )
    socket = Path(socket_directory.name).resolve()
    socket.chmod(0o2777)
    controller = tmp_path / "controller"
    (controller / "solution/precomputed").mkdir(parents=True)
    (controller / "solution/node_modules").mkdir()
    (controller / "tests").mkdir()
    (controller / "verifier").mkdir()
    (controller / "instruction.md").write_text("Produce output")
    (controller / "README.md").write_text("Never inject this into root-owned /app")
    (controller / "tests/private.py").write_text("private evaluation contents")
    (controller / "verifier/run.sh").write_bytes(native_verifier_run_sh_bytes())
    binary = b"\x93NUMPY\x00\x01\xfffixture"
    (controller / "solution/precomputed/design.npy").write_bytes(binary)
    (controller / "solution/node_modules/helper.txt").write_text("authored helper")
    (controller / "solution/.hidden").write_text("retained")
    (controller / "solution/solve.py").write_text(
        "from pathlib import Path\nimport os\n"
        "assert os.getuid() == 65534 and Path.cwd() == Path('/app')\n"
        "assert not Path('/tests/private.py').exists()\n"
        "assert not Path('/app/README.md').exists()\n"
        "assert Path('/solution/node_modules/helper.txt').read_text() == 'authored helper'\n"
        "Path('/app/output/design.npy').write_bytes(Path('/solution/precomputed/design.npy').read_bytes())\n"
    )
    (controller / "solution/solve.sh").write_text(
        "#!/bin/sh\nset -eu\npython3 /solution/solve.py\n"
    )
    task, model_trial, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    trial = (
        TrialConfig(agent_name="oracle", agent_model=None)
        if agent_name == "oracle"
        else model_trial
    )
    container = client.containers.run(
        image,
        ["--socket", "/socket/sandbox.sock"],
        detach=True,
        entrypoint="/loom/bin/loom-sandbox-runtime",
        network_mode="none",
        cap_drop=["ALL"],
        security_opt=["no-new-privileges"],
        mem_limit="512m",
        nano_cpus=1_000_000_000,
        pids_limit=128,
        labels={"loom.task": "2292-oracle-reference"},
        volumes={
            str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
            str(socket): {"bind": "/socket", "mode": "rw"},
        },
    )
    driver = ServiceSandboxDriver(
        socket / "sandbox.sock",
        capabilities=Capabilities(
            os="linux",
            gpu_vendor="none",
            network_policies=frozenset({"no-network"}),
            dynamic_network_policy=False,
            mounted_fs=False,
            resource_modes=frozenset({"limit"}),
        ),
        network_policy=NoNetwork(),
    )
    try:
        for attempt in range(100):
            try:
                await driver.start()
                break
            except (OSError, RuntimeError, httpx.TransportError, SandboxRPCError):
                if attempt == 99:
                    print(container.logs().decode(errors="replace"))
                    raise
                await asyncio.sleep(0.05)
        await driver.stop()
        # Native drivers have one start lifecycle. Use a fresh controller client
        # after the readiness probe; run_agent owns its normal start/stop.
        driver = ServiceSandboxDriver(
            socket / "sandbox.sock",
            capabilities=driver.capabilities,
            network_policy=NoNetwork(),
        )
        monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
        monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
        monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")

        async def identity(_):
            return uuid4(), uuid4()

        async def model(**kwargs):
            # No model call: exercise the same native staging path and inspect
            # what a model would see before any agent implementation runs.
            result = await driver.exec(
                'test -z "$(find /solution /tests -mindepth 1 -print -quit)" && test ! -e /app/README.md',
                cwd=PurePosixPath("/app"),
            )
            assert result.return_code == 0, result.stderr
            (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

        monkeypatch.setattr(module, "_execution_identity", identity)
        monkeypatch.setattr(module, "run_terminus2", model)
        await module.run_agent(controller, task, trial)
        result = container.exec_run(
            [
                "/bin/sh",
                "-c",
                'test "$(id -u)" = 65534 && test "$PWD" = /app && test ! -w /app && test -d /solution && test -z "$(find /solution -mindepth 1 -print -quit)" && test ! -e /tests/private.py',
            ]
        )
        assert result.exit_code == 0, result.output
        if agent_name == "oracle":
            result = container.exec_run(["cat", "/app/output/design.npy"])
            assert result.exit_code == 0 and result.output == binary
    finally:
        await driver.stop(delete=True)
        container.remove(force=True, v=True)
        socket_directory.cleanup()
