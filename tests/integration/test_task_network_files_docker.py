"""Rendered private mounts isolate the network files supplied by the runtime."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import compile_service_execution_plan
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.integration.test_execution_actuator_k3s import _lease
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_task_sandbox_identity import _identity_task

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


def test_root_network_file_writes_cannot_change_controller_or_verifier(tmp_path):
    import docker

    repository = Path(__file__).resolve().parents[2]
    binaries = tmp_path / "bin"
    binaries.mkdir()
    build = subprocess.run([
        "docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{repository}:/src:ro", "-v", f"{binaries}:/output", "-w", "/src",
        "-e", "GOCACHE=/tmp/go-cache", "-e", "GOMODCACHE=/tmp/go-mod", "-e", "CGO_ENABLED=0",
        "golang:1.26-alpine3.23", "go", "build", "-o", "/output/",
        "./cmd/loom-execution-runtime", "./cmd/loom-sandbox-runtime",
    ], timeout=120, capture_output=True)
    assert build.returncode == 0, build.stderr.decode()
    task, trial, profile = _identity_task("root")
    binary_digest = hashlib.sha256((binaries / "loom-execution-runtime").read_bytes()).hexdigest()
    profile = profile.model_copy(update={
        "supports_task_identity": True, "runtime_binary_sha256": "sha256:" + binary_digest,
    })
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256="sha256:" + "c" * 64)
    lease = _lease("network-file-test")
    lease.execution_class_id = plan.execution_class_id
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    spec = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]
    directories = {}
    for volume in spec["volumes"]:
        assert "emptyDir" in volume
        directory = tmp_path / volume["name"]
        directory.mkdir(mode=0o777)
        directory.chmod(0o777)
        directories[volume["name"]] = directory

    def mounts(container):
        return [docker.types.Mount(
            m["mountPath"], str(directories[m["name"]] / m.get("subPath", "")),
            type="bind", read_only=m.get("readOnly", False),
        ) for m in container["volumeMounts"]]

    client = docker.from_env()
    containers = []
    common = {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges"],
              "mem_limit": "128m", "nano_cpus": 250_000_000}
    try:
        controller = client.containers.run(
            "python:3.11-slim", ["-c", "import time; time.sleep(120)"], entrypoint="python",
            user="65532:65532", network_mode="none", detach=True, **common,
        )
        containers.append(controller)
        materializer = spec["initContainers"][0]
        # Execute the real materializer and the renderer's exact mount mapping.
        result = client.containers.run(
            "python:3.11-slim", materializer["command"][1:],
            entrypoint=materializer["command"][0], user="65532:65532", remove=True,
            network_mode=f"container:{controller.id}", **common,
            mounts=mounts(materializer) + [docker.types.Mount(
                "/" + name, str(binaries / name), type="bind", read_only=True,
            ) for name in ("loom-execution-runtime", "loom-sandbox-runtime")],
        )
        assert result == b""
        for private in spec["initContainers"][1:]:
            security = private["securityContext"]
            container = client.containers.run(
                "python:3.11-slim", ["-c", "import time; time.sleep(120)"], entrypoint="python",
                user=f"{security['runAsUser']}:{security['runAsGroup']}", detach=True,
                network_mode=f"container:{controller.id}", mounts=mounts(private),
                cap_add=security["capabilities"].get("add", []), **common,
            )
            containers.append(container)
        agent = containers[1]

        def read(container, path):
            response = container.exec_run(["cat", path])
            assert response.exit_code == 0, response.output
            return response.output

        for path in ("/etc/hosts", "/etc/resolv.conf"):
            baseline = [read(container, path) for container in containers]
            for writer, marker in ((agent, b"task-only"),):
                result = writer.exec_run(["python", "-c",
                    "import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(sys.argv[2].encode())",
                    path, marker.decode()])
                assert result.exit_code == 0, result.output
                assert read(writer, path) == marker
                for index, observer in enumerate(containers):
                    if observer is not writer:
                        assert read(observer, path) == baseline[index], (path, writer.id, observer.id)
                result = writer.exec_run(["python", "-c",
                    "import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(bytes.fromhex(sys.argv[2]))",
                    path, baseline[containers.index(writer)].hex()])
                assert result.exit_code == 0, result.output
        for container in containers:
            response = container.exec_run(["python", "-c",
                "import socket; assert socket.gethostbyname('localhost') == '127.0.0.1'"])
            assert response.exit_code == 0, response.output
    finally:
        for container in reversed(containers):
            container.remove(force=True)
        client.close()
