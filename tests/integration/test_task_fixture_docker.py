"""Execute the real materializer with separate, unmounted fixture source images."""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import compile_service_execution_plan
from loom.task_image_materialization import resolve_prepared_task
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.integration.test_execution_actuator_k3s import _lease
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_task_fixtures import _prepared

pytestmark = [pytest.mark.docker, pytest.mark.timeout(240)]


def test_prepared_fixture_socket_source_and_hostname_are_trial_private(tmp_path: Path) -> None:
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
    digest = "sha256:" + hashlib.sha256((binaries / "loom-execution-runtime").read_bytes()).hexdigest()
    task, trial, profile, grant = _prepared()
    profile = profile.model_copy(update={"runtime_binary_sha256": digest})
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
        task_image_grant=grant, source_provenance=_provenance(), task_revision_sha256="sha256:" + "2" * 64)
    lease = _lease("fixture-docker-test")
    lease.execution_class_id = plan.execution_class_id
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(resolve_prepared_task(task, grant), trial).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    spec = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]
    materializer, fixture_spec, agent_spec = spec["initContainers"]
    assert not fixture_spec["volumeMounts"]
    assert "hostAliases" not in spec
    client = docker.from_env()
    containers, image_tags = [], []
    common = {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges"],
              "mem_limit": "128m", "nano_cpus": 100_000_000}

    def run(image, command, **kwargs):
        container = client.containers.run(image, command, detach=True, **common, **kwargs)
        containers.append(container)
        return container

    def execute(container, code):
        result = container.exec_run(["python3", "-c", code])
        assert result.exit_code == 0, result.output
        return result.output

    try:
        agents = []
        for ordinal in range(2):
            root = tmp_path / f"trial-{ordinal}"
            root.mkdir()
            directories = {}
            for volume in spec["volumes"]:
                assert "emptyDir" in volume
                directory = root / volume["name"]
                directory.mkdir(mode=0o777)
                directory.chmod(0o777)
                directories[volume["name"]] = directory

            def mounts(container_spec, directories=directories):
                return [docker.types.Mount(m["mountPath"],
                    str(directories[m["name"]] / m.get("subPath", "")),
                    type="bind", read_only=m.get("readOnly", False),
                ) for m in container_spec["volumeMounts"]]

            controller = run("python:3.11-slim", ["python3", "-c", "import time; time.sleep(120)"],
                user="65532:65532", network_mode="none", read_only=True)
            original_hosts = execute(controller, "print(open('/etc/hosts').read(), end='')")
            initialized = run("python:3.11-slim", materializer["command"], user="65532:65532",
                network_mode=f"container:{controller.id}", read_only=True,
                mounts=mounts(materializer) + [docker.types.Mount("/" + name, str(binaries / name),
                    type="bind", read_only=True) for name in ("loom-execution-runtime", "loom-sandbox-runtime")])
            assert initialized.wait(timeout=30)["StatusCode"] == 0, initialized.logs()
            agent = run("python:3.11-slim", ["python3", "-c", "import time; time.sleep(120)"],
                user="65532:65532", network_mode=f"container:{controller.id}", mounts=mounts(agent_spec))
            # The other trial may already be listening on TCP23. This trial's
            # identical loopback address and alias must still have no server.
            execute(agent, "import socket; s=socket.socket(); s.settimeout(1); "
                "assert socket.gethostbyname('fixture.example') == '127.0.0.1'; "
                "assert s.connect_ex(('fixture.example',23)) != 0")
            context = root / "fixture-source"
            context.mkdir()
            secret = f"fixture-only-{uuid4().hex}"
            (context / "server.py").write_text(
                "import socket\ns=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
                "s.bind(('127.0.0.1',23)); s.listen()\nwhile True:\n c,_=s.accept(); "
                f"c.sendall({secret.encode()!r}); c.close()\n"
            )
            (context / "healthcheck.py").write_text("import socket; socket.create_connection(('127.0.0.1',23),1).close()\n")
            (context / "Dockerfile").write_text(
                "FROM python:3.11-slim\nCOPY server.py healthcheck.py /\nRUN chmod 0444 /server.py /healthcheck.py\n"
            )
            tag = "loom-fixture-isolation:" + uuid4().hex
            client.images.build(path=str(context), tag=tag, rm=True)
            image_tags.append(tag)
            security = fixture_spec["securityContext"]
            fixture = run(tag, fixture_spec["command"],
                user=f"{security['runAsUser']}:{security['runAsGroup']}",
                read_only=security["readOnlyRootFilesystem"],
                network_mode=f"container:{controller.id}", mounts=mounts(fixture_spec))
            # Probe command and bounded socket retries exercise actual startup.
            execute(agent, "import socket,time\nfor attempt in range(50):\n"
                " try:\n  c=socket.create_connection(('fixture.example',23),1); "
                f"assert c.recv(256) == {secret.encode()!r}; c.close(); break\n"
                " except ConnectionRefusedError:\n  time.sleep(.1)\nelse:\n raise AssertionError('fixture not ready')")
            probe = fixture.exec_run(fixture_spec["startupProbe"]["exec"]["command"])
            assert probe.exit_code == 0, probe.output
            execute(agent, "from pathlib import Path; assert not Path('/server.py').exists(); "
                "assert not Path('/healthcheck.py').exists(); assert not Path('/fixture-source').exists()")
            execute(agent, "import socket; c=socket.create_connection(('fixture.example',23),1); "
                f"assert c.recv(256) == {secret.encode()!r}; c.close()")
            execute(agent, "from pathlib import Path; assert not Path('/loom/sandboxes/verifier-sandbox').exists()")
            assert execute(controller, "print(open('/etc/hosts').read(), end='')") == original_hosts
            assert b"fixture.example" not in original_hosts
            fixture.reload()
            assert not fixture.attrs["Mounts"]
            assert fixture.attrs["HostConfig"]["ReadonlyRootfs"]
            agents.append((agent, secret))
        # With both fixtures active, each agent still receives its own source's value.
        for agent, secret in agents:
            execute(agent, "import socket; c=socket.create_connection(('fixture.example',23),1); "
                f"assert c.recv(256) == {secret.encode()!r}")
        # Service loss produces a bounded connection failure, without crossing
        # into the other trial's fixture on the same port.
        fixture.stop(timeout=1)
        execute(agents[-1][0], "import socket; c=socket.socket(); c.settimeout(1); "
            "assert c.connect_ex(('fixture.example',23)) != 0")
    finally:
        ids = [container.id for container in containers]
        for container in reversed(containers):
            container.remove(force=True)
        for identifier in ids:
            with pytest.raises(docker.errors.NotFound):
                client.containers.get(identifier)
        for tag in image_tags:
            client.images.remove(tag, force=True)
        client.close()
