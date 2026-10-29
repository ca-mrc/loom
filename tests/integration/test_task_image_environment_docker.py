"""Published OCI fixture ENV survives admission, rendering and real sandbox exec."""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pytest

from loom.driver.service_sandbox import SandboxRPCError, ServiceSandboxDriver
from loom.execution_contract import workload_requirements_from_task
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import compile_service_execution_plan
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task
from loom_execution_actuator import task_image_runtime
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.integration.test_harbor_native_verifier_defaults_docker import (
    _docker,
    _metadata_archive_from_docker_save,
)
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

pytestmark = [pytest.mark.docker, pytest.mark.timeout(240)]


@pytest.mark.parametrize("native_binary", ["amd64"], indirect=True)
async def test_published_password_environment_reaches_real_task_sandbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, native_binary: Path,  # noqa: F811
) -> None:
    import docker

    context = tmp_path / "environment"
    context.mkdir()
    (context / "Dockerfile").write_text(
        "FROM python:3.11-slim\n"
        "RUN mkdir -p /app/image-default\n"
        "ENV PASSWORD=diagnostic-fixture-value KEPT=image-value\n"
        "WORKDIR /app/image-default\n",
    )
    tag = "loom-task-env-regression:" + uuid4().hex
    saved = tmp_path / "image-docker.tar"
    work = tmp_path / "publication"
    (work / "oci").mkdir(parents=True)
    archive = work / "oci/0000.tar"
    container = None
    driver = None
    client = docker.from_env()
    try:
        built = _docker("build", "--platform", "linux/amd64", "--tag", tag, str(context))
        assert built.returncode == 0, built.stderr
        exported = _docker("image", "save", "--output", str(saved), tag)
        assert exported.returncode == 0, exported.stderr
        digest, architecture = _metadata_archive_from_docker_save(saved, archive)
        assert architecture == "amd64"
        task, trial, profile = _inputs()
        raw = task.model_dump(mode="json")
        raw["environment"].update(
            docker_image=None, dockerfile="environment/Dockerfile", user="image",
            image_workdir=True, image_entrypoint=True, environment={"KEPT": "authored"},
            baseline_network_policy={"kind": "no-network"}, network_policies_supported=["no-network"],
        )
        task = TaskConfig.model_validate(raw)
        frozen_source = task.model_dump_json()
        profile = profile.model_copy(update={
            "supports_task_identity": True,
            "runtime_binary_sha256": "sha256:" + hashlib.sha256(native_binary.read_bytes()).hexdigest(),
        })
        # Only registry transport is substituted; publish parses actual built
        # OCI config/layers and produces the grant consumed by the compiler.
        def copy_to_registry(argv, **kwargs):
            assert argv[:1] == ["skopeo"] and "copy" in argv
            Path(argv[argv.index("--digestfile") + 1]).write_text(digest)
            return task_image_runtime.subprocess.CompletedProcess(argv, 0, "", "")

        claim = {"id": str(uuid4()), "lease_epoch": 1, "materialization_key": "1" * 64,
                 "cpu_arch": "x86_64", "task_config": task.model_dump(mode="json"),
                 "registry_repository": "registry.invalid/fixture"}
        with monkeypatch.context() as patch:
            patch.setattr(task_image_runtime.subprocess, "run", copy_to_registry)
            receipt = task_image_runtime.publish(claim, work, tmp_path / "secrets",
                                                  receipt_path=tmp_path / "publication.json")
        grant = TaskImageExecutionGrantV1(
            schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
            materialization_key="1" * 64, cpu_arch="x86_64", task_checksum=_REVISION.removeprefix("sha256:"),
            task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance=_provenance(),
            registry_images=receipt["registry_images"], image_runtime_metadata=receipt["image_runtime_metadata"],
        )
        resolved = resolve_prepared_task(task, grant)
        plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
            task_image_grant=grant, source_provenance=_provenance(), task_revision_sha256=_REVISION)
        lease = _lease()
        lease.runtime_contract_json = plan.canonical_payload()
        lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
        lease.workload_requirements_json = workload_requirements_from_task(resolved, trial).model_dump(mode="json")
        lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
        pod = render_execution_job(lease, target=ExecutionTargetRuntime(
            target_id=lease.target_id, namespace=lease.namespace_name,
        ))["spec"]["template"]["spec"]
        sandbox = next(c for c in pod["initContainers"] if c["name"] == "task-sandbox")
        assert sandbox["image"] == receipt["registry_images"]["task"]
        environment = {item["name"]: item["value"] for item in sandbox["env"]}
        assert environment["PASSWORD"] == "diagnostic-fixture-value"
        for other in (*pod["initContainers"], *pod["containers"]):
            if other is not sandbox:
                assert "PASSWORD" not in {v["name"] for v in other.get("env", [])}
        socket = tmp_path / "socket"
        socket.mkdir(mode=0o777)
        socket.chmod(0o2777)
        command = sandbox["command"]
        socket_path = PurePosixPath(command[command.index("--socket") + 1])
        security = sandbox["securityContext"]
        # Resolve the stubbed registry reference to those same local image
        # bytes. Start the actual rendered sandbox command, identity and ENV.
        container = client.containers.run(tag, command[1:], entrypoint=command[0], detach=True,
            platform="linux/amd64", network_mode="none", cap_drop=["ALL"],
            cap_add=security["capabilities"].get("add", []), security_opt=["no-new-privileges"],
            user=f"{security['runAsUser']}:{security['runAsGroup']}",
            environment=environment, read_only=security["readOnlyRootFilesystem"],
            volumes={str(native_binary): {"bind": command[0], "mode": "ro"},
                     str(socket): {"bind": str(socket_path.parent), "mode": "rw"}})
        driver = ServiceSandboxDriver(socket / socket_path.name, capabilities=Capabilities(
            os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
            dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"}),
        ), network_policy=NoNetwork())
        for attempt in range(100):
            try:
                await driver.start()
                break
            except (OSError, RuntimeError, httpx.TransportError, SandboxRPCError):
                if attempt == 99:
                    container.reload()
                    present = (container.exec_run(["test", "-S", str(socket_path)]).exit_code == 0
                               if container.status == "running" else False)
                    pytest.fail(f"sandbox health unavailable: status={container.status}, "
                                f"container_socket={present}, host_socket={(socket / socket_path.name).exists()}, "
                                f"logs={container.logs(tail=10)!r}")
                await asyncio.sleep(0.05)
        result = await driver.exec(
            'test "$PASSWORD" = diagnostic-fixture-value && test "$KEPT" = authored '
            '&& test "$PWD" = /app/image-default && echo fixture-environment-ok',
            cwd=resolved.environment.workdir,
        )
        assert result.return_code == 0, result.stderr
        assert result.stdout.strip() == b"fixture-environment-ok"
        assert task.model_dump_json() == frozen_source
    finally:
        if driver is not None:
            await driver.stop()
        if container is not None:
            container.remove(force=True)
        client.close()
        _docker("image", "rm", "--force", tag)
        saved.unlink(missing_ok=True)
        archive.unlink(missing_ok=True)
