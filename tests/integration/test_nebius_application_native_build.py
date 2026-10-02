"""Run the rendered rootless builder against verified dirty application source."""
from __future__ import annotations

import io
import json
import tarfile
from uuid import uuid4

import docker
import pytest

from loom_execution_actuator import application_image_runtime as runtime
from loom_execution_actuator.application_image_renderer import render_application_image_job
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_oci import validate_native_oci_archive
from loom_execution_actuator.task_image_renderer import BUILDKIT_IMAGE, TaskImageJobConfig
from tests.unit.test_nebius_application_image_runtime import SourceObject
from tests.unit.test_nebius_application_image_runtime import source_build as source_build


@pytest.mark.timeout(300)
def test_rootless_service_and_web_build_contain_dirty_source_without_credential_mounts(source_build, tmp_path, monkeypatch):
    claim, payload, _, marker = source_build
    claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"snapshotter": "native"})})
    work = tmp_path / "work"
    work.mkdir()
    source = SourceObject(claim, payload)
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: source)
    runtime.prepare(claim, work, tmp_path / "not-mounted-secrets")
    # The host developer's UID differs from the image's UID1000. Only this
    # credential-free disposable fixture is made readable/writable across UIDs;
    # installed prepare/build phases both use UID1000 and keep the private mode.
    work.chmod(0o777)
    (work / "oci").chmod(0o777)
    (work / "context").chmod(0o755)
    for path in (work / "context").rglob("*"):
        if path.is_dir():
            path.chmod(0o755)
    config = TaskImageJobConfig(service_image=claim.recipe.trusted_image_ref,
        source_secret_name="not-mounted-source", registry_secret_name="not-mounted-registry",
        registry_auth_kind="nebius", snapshotter="native", active_deadline_seconds=180)
    _, job = render_application_image_job(claim=claim,
        target=ExecutionTargetRuntime(target_id="local-proof", namespace="not-installed"), config=config)
    build = job["spec"]["template"]["spec"]["initContainers"][1]
    client = docker.from_env(timeout=60)
    container = None
    try:
        try:
            client.images.get(BUILDKIT_IMAGE)
        except docker.errors.ImageNotFound:
            client.images.pull(BUILDKIT_IMAGE)
        container = client.containers.run(BUILDKIT_IMAGE, entrypoint=build["command"],
            detach=True, name="loom-app-build-proof-" + uuid4().hex, user="1000:1000", read_only=True,
            network_mode="none", cap_drop=["ALL"], cap_add=["SETUID", "SETGID"],
            security_opt=["seccomp=unconfined", "apparmor=unconfined"], pids_limit=4096,
            mem_limit="1g", nano_cpus=2_000_000_000,
            volumes={str(work): {"bind": "/loom/build", "mode": "rw"}},
            tmpfs={"/scratch": "rw,uid=1000,gid=1000,mode=0700,size=512m",
                "/tmp": "rw,uid=1000,gid=1000,mode=0700,size=128m",
                # Docker honors the image's VOLUME declaration; Kubernetes
                # does not. Mask that unused path instead of leaving a volume.
                "/home/user/.local/share/buildkit": "ro,uid=1000,gid=1000,mode=0700,size=1m"},
            environment={row["name"]: row["value"] for row in build["env"]})
        result = container.wait(timeout=240)
        assert result["StatusCode"] == 0, container.logs(tail=70).decode(errors="replace")
        container.reload()
        assert {mount["Destination"] for mount in container.attrs["Mounts"]} == {"/loom/build"}
        assert container.attrs["HostConfig"]["Privileged"] is False
        for index in range(2):
            archive_path = work / f"oci/{index:04d}.tar"
            validate_native_oci_archive(archive_path)
            with tarfile.open(archive_path, "r:") as archive:
                def metadata(path):
                    stream = archive.extractfile(path)
                    assert stream is not None
                    with stream:
                        return json.load(stream)

                descriptor = metadata("index.json")["manifests"][0]
                image = metadata("blobs/sha256/" + descriptor["digest"].removeprefix("sha256:"))
                assert len(image["layers"]) == 1
                layer = archive.extractfile("blobs/sha256/" + image["layers"][0]["digest"].removeprefix("sha256:"))
                assert layer is not None
                with layer, tarfile.open(fileobj=io.BytesIO(layer.read()), mode="r:*") as content:
                    actual = content.extractfile("local.txt")
                    assert actual is not None and actual.read() == b"dirty source, not HEAD\n"
        assert not marker.exists() and source.closed
    finally:
        if container is not None:
            container.remove(force=True, v=True)
        client.close()
