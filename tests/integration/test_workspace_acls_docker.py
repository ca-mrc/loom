"""Real ACL metadata and inheritance across independent verifier filesystems."""
from __future__ import annotations

import asyncio
import io
from pathlib import PurePosixPath
from uuid import uuid4

import docker
import httpx
import pytest

from loom.driver.docker import DockerDriver
from loom.driver.service_sandbox import SandboxRPCError, ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_snapshot import handoff_workspace_snapshot
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


@pytest.fixture(scope="module")
def acl_image():
    client = docker.from_env()
    tag = f"loom-test-workspace-acls:{uuid4().hex}"
    dockerfile = b"""FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends acl && rm -rf /var/lib/apt/lists/*
RUN mkdir /workspace /data && chown 65532:65532 /workspace /data
"""
    try:
        client.images.build(fileobj=io.BytesIO(dockerfile), tag=tag, rm=True)
        yield tag
    finally:
        client.images.remove(tag, force=True)
        client.close()


@pytest.fixture(params=["posix", "native"])
async def acl_drivers(request, acl_image, native_binary, tmp_path):  # noqa: F811
    client = docker.from_env()
    containers, drivers = [], []
    try:
        for role in ("agent", "verifier"):
            if request.param == "posix":
                driver = DockerDriver(image=acl_image, workspace=PurePosixPath("/workspace"))
                await driver.start()
            else:
                socket = tmp_path / role
                socket.mkdir(mode=0o777)
                socket.chmod(0o2777)
                container = client.containers.run(
                    acl_image, ["--socket", "/socket/sandbox.sock"], detach=True,
                    entrypoint="/loom/bin/loom-sandbox-runtime", user="65532:65532",
                    network_mode="none", cap_drop=["ALL"], security_opt=["no-new-privileges"],
                    volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                             str(socket): {"bind": "/socket", "mode": "rw"}},
                )
                containers.append(container)
                driver = ServiceSandboxDriver(socket / "sandbox.sock", capabilities=Capabilities(
                    os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
                    dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"}),
                ), network_policy=NoNetwork())
                for attempt in range(100):
                    try:
                        await driver.start()
                        break
                    except (OSError, RuntimeError, httpx.TransportError, SandboxRPCError):
                        if attempt == 99:
                            raise
                        await asyncio.sleep(0.05)
            drivers.append(driver)
        yield drivers
    finally:
        for driver in drivers:
            await driver.stop(delete=True)
        for container in reversed(containers):
            container.remove(force=True)
        client.close()


async def test_handoff_preserves_access_default_acls_and_child_inheritance(acl_drivers, tmp_path):
    agent, verifier = acl_drivers
    for root in ("/workspace", "/data"):
        result = await agent.exec(
            f"mkdir {root}/htdocs && printf answer > {root}/htdocs/file && "
            f"chmod 0755 {root}/htdocs && "
            f"setfacl -m u:12345:r-- {root}/htdocs/file && "
            f"setfacl -m d:u::rwx,d:u:12345:r-x,d:g::r-x,d:m::r-x,d:o::r-x {root}/htdocs",
        )
        assert result.return_code == 0, result.stderr
    result = await verifier.exec(
        "mkdir /workspace/tests && printf trusted > /workspace/tests/secret && "
        "setfacl -m u:12345:r-- /workspace/tests/secret && "
        "setfacl -m d:u::rwx,d:g::---,d:o::--- /workspace",
    )
    assert result.return_code == 0, result.stderr
    private_before = await verifier.exec("getfacl -cpn /workspace/tests/secret")
    policy = WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ())

    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier, workdir=PurePosixPath("/workspace"),
        policy=policy, preserve_acls=True,
    )
    await export_mutable_paths(
        agent, (PurePosixPath("/data"),), tmp_path / "mutable",
        workdir=PurePosixPath("/workspace"), preserve_acls=True,
    )
    await import_mutable_paths(
        verifier, (PurePosixPath("/data"),), tmp_path / "mutable",
        workdir=PurePosixPath("/workspace"), preserve_acls=True,
    )

    for root in ("/workspace", "/data"):
        for suffix in ("", "/htdocs", "/htdocs/file"):
            before = await agent.exec(f"getfacl -cpn {root}{suffix}")
            after = await verifier.exec(f"getfacl -cpn {root}{suffix}")
            assert before.return_code == after.return_code == 0
            assert after.stdout == before.stdout
        # The kernel must apply the restored default ACL to newly created files,
        # even under a restrictive umask; archive headers alone do not prove it.
        for driver in (agent, verifier):
            result = await driver.exec(f"umask 077; touch {root}/htdocs/child; mkdir {root}/htdocs/sub")
            assert result.return_code == 0, result.stderr
        for suffix in ("child", "sub"):
            before = await agent.exec(f"getfacl -cpn {root}/htdocs/{suffix}")
            after = await verifier.exec(f"getfacl -cpn {root}/htdocs/{suffix}")
            assert after.stdout == before.stdout
            assert b"user:12345:r-x" in after.stdout
    assert (await verifier.exec("getfacl -cpn /workspace/tests/secret")).stdout == private_before.stdout
    assert (await verifier.exec("cat /workspace/tests/secret")).stdout == b"trusted"


@pytest.mark.parametrize("root", ["/workspace/link", "/workspace/link/sub"])
async def test_acl_probe_rejects_symlink_root(acl_drivers, root):
    from loom.trial.workspace_acls import require_acl_support
    from loom.trial.workspace_snapshot import WorkspaceSnapshotError

    agent, _ = acl_drivers
    assert (await agent.exec("mkdir /data/sub && ln -s /data /workspace/link")).return_code == 0
    with pytest.raises(WorkspaceSnapshotError, match="ACL"):
        await require_acl_support(agent, PurePosixPath(root))


async def test_acl_capture_accepts_read_only_final_roots(acl_drivers, tmp_path):
    agent, verifier = acl_drivers
    for root in ("/workspace", "/data"):
        changed = await agent.exec(
            f"printf answer > {root}/file && "
            f"setfacl -m d:u::rwx,d:g::r-x,d:o::--- {root} && chmod 0555 {root}",
        )
        assert changed.return_code == 0, changed.stderr
    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier, workdir=PurePosixPath("/workspace"),
        policy=WorkspaceStagingPolicy((), (), ()), preserve_acls=True,
    )
    await export_mutable_paths(
        agent, (PurePosixPath("/data"),), tmp_path / "mutable",
        workdir=PurePosixPath("/workspace"), preserve_acls=True,
    )
    await import_mutable_paths(
        verifier, (PurePosixPath("/data"),), tmp_path / "mutable",
        workdir=PurePosixPath("/workspace"), preserve_acls=True,
    )
    for root in ("/workspace", "/data"):
        assert (await verifier.exec(f"stat -c %a {root}")).stdout.strip() == b"555"
        assert (await verifier.exec(f"getfacl -cpn {root}")).stdout == (
            await agent.exec(f"getfacl -cpn {root}")
        ).stdout
