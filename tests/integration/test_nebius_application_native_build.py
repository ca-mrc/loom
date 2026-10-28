"""Run the rendered rootless builder against verified dirty application source."""
from __future__ import annotations

import io
import json
import platform
import tarfile
from uuid import uuid4

import docker
import pytest

from loom.native_image_build import NativeImageBuildComponentV1
from loom_execution_actuator import application_image_runtime as runtime
from loom_execution_actuator.application_image_renderer import render_application_image_job
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_oci import validate_native_oci_archive
from loom_execution_actuator.task_image_renderer import (
    _BUILDER_SUBIDS,
    BUILDKIT_IMAGE,
    TaskImageJobConfig,
    _build_script,
)
from tests.unit.test_nebius_application_image_runtime import SourceObject
from tests.unit.test_nebius_application_image_runtime import source_build as source_build

pytestmark = pytest.mark.docker


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
    configmap, job = render_application_image_job(claim=claim,
        target=ExecutionTargetRuntime(target_id="local-proof", namespace="not-installed"), config=config)
    build = job["spec"]["template"]["spec"]["initContainers"][1]
    client = docker.from_env(timeout=60)
    mounts = {str(work): {"bind": "/loom/build", "mode": "rw"}}
    for key in ("subuid", "subgid"):
        path = tmp_path / key
        path.write_text(configmap["data"][key])
        path.chmod(0o444)
        mounts[str(path)] = {"bind": "/etc/" + key, "mode": "ro"}
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
            volumes=mounts,
            tmpfs={"/scratch": "rw,exec,uid=1000,gid=1000,mode=0700,size=512m",
                "/tmp": "rw,uid=1000,gid=1000,mode=0700,size=128m",
                # Docker honors the image's VOLUME declaration; Kubernetes
                # does not. Mask that unused path instead of leaving a volume.
                "/home/user/.local/share/buildkit": "ro,uid=1000,gid=1000,mode=0700,size=1m"},
            environment={row["name"]: row["value"] for row in build["env"]})
        result = container.wait(timeout=240)
        assert result["StatusCode"] == 0, container.logs(tail=70).decode(errors="replace")
        container.reload()
        assert {mount["Destination"] for mount in container.attrs["Mounts"]} == {"/loom/build", "/etc/subuid", "/etc/subgid"}
        assert container.attrs["HostConfig"]["Privileged"] is False
        events = [json.loads(line) for line in container.logs().decode().splitlines() if line.startswith('{"loom_task_image_stage"')]
        assert [row["component_index"] for row in events if row["loom_task_image_stage"] == "cleanup" and row["event"] == "end"] == [0, 1]
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


def _run_generated(tmp_path, script, *, extra_mounts=None, environment=None):
    """Exact public image/security; only credential-free fixture paths mounted."""
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    work.chmod(0o777)
    mounts = {str(work): {"bind": "/loom/build", "mode": "rw"}}
    for key in ("subuid", "subgid"):
        path = tmp_path / key
        path.write_text(_BUILDER_SUBIDS)
        path.chmod(0o444)
        mounts[str(path)] = {"bind": "/etc/" + key, "mode": "ro"}
    mounts.update(extra_mounts or {})
    client = docker.from_env(timeout=60)
    container = None
    try:
        container = client.containers.run(BUILDKIT_IMAGE, entrypoint=["sh", "-ec", script],
            detach=True, name="loom-nested-build-proof-" + uuid4().hex, user="1000:1000", read_only=True,
            network_mode="none", cap_drop=["ALL"], cap_add=["SETUID", "SETGID"],
            security_opt=["seccomp=unconfined", "apparmor=unconfined"], pids_limit=4096,
            mem_limit="1g", nano_cpus=2_000_000_000, volumes=mounts,
            tmpfs={"/scratch": "rw,exec,uid=1000,gid=1000,mode=0700,size=512m",
                "/tmp": "rw,uid=1000,gid=1000,mode=0700,size=128m",
                "/home/user/.local/share/buildkit": "ro,uid=1000,gid=1000,mode=0700,size=1m"},
            environment={"TMPDIR": "/scratch/tmp", "XDG_RUNTIME_DIR": "/scratch/runtime",
                "DOCKER_CONFIG": "/scratch/docker-config",
                "BUILDKITD_FLAGS": "--root /scratch/state --oci-worker-no-process-sandbox --oci-worker-snapshotter=native",
                **(environment or {})})
        result = container.wait(timeout=120)
        return result["StatusCode"], container.logs().decode(errors="replace")
    finally:
        if container is not None:
            container.remove(force=True, v=True)
        client.close()


def _script(component_count=1, *, export_format="archive", timeout=30):
    return _build_script(tuple(NativeImageBuildComponentV1(name="task" if i == 0 else "sidecar:second",
        dockerfile_path="Dockerfile", context_path=".", oci_output_path=f"oci/{i:04d}.tar")
        for i in range(component_count)), platform="linux/arm64" if platform.machine() in {'arm64', 'aarch64'} else "linux/amd64",
        max_processes=512, cache_enabled=False, build_timeout_seconds=timeout, oci_export_format=export_format)


@pytest.mark.timeout(150)
@pytest.mark.parametrize("component_count", [1, 2])
@pytest.mark.parametrize("export_format", ["archive", "directory"])
@pytest.mark.parametrize(("build_exit", "expected_exit"), [(0, 0), (42, 1), (124, 124)])
def test_generated_nested_script_preserves_linux_outcomes_and_cleanup(tmp_path, component_count, export_format, build_exit, expected_exit):
    binary = tmp_path / "buildctl-daemonless.sh"
    binary.write_text('''#!/bin/sh
set -eu
test "$(id -u)" = 0
test "$(ulimit -Su):$(ulimit -Hu)" = 512:512
test -d /scratch/scope/a && test -d /scratch/scope/b
test ! -e /scratch/state/private
if [ "$LOOM_TEST_BUILD_EXIT" != 0 ]; then exit "$LOOM_TEST_BUILD_EXIT"; fi
mkdir /scratch/state/private; touch /scratch/state/private/file
chown -R 197609:197121 /scratch/state/private; chmod 700 /scratch/state/private
while [ "$1" != --output ]; do shift; done
output=$2; dest=${output#*dest=}; dest=${dest%%,*}
case "$output" in
  *tar=false*) mkdir -p "$dest"; printf payload > "$dest/index.json" ;;
  *) printf payload > "$dest" ;;
esac
''')
    binary.chmod(0o555)
    status, logs = _run_generated(tmp_path, _script(component_count, export_format=export_format),
        extra_mounts={str(binary): {"bind": "/proof/buildctl-daemonless.sh", "mode": "ro"}},
        environment={"LOOM_TEST_BUILD_EXIT": str(build_exit), "PATH": "/proof:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"})
    assert status == expected_exit, logs
    events = [json.loads(line) for line in logs.splitlines() if line.startswith('{"loom_task_image_stage"')]
    ended = [row for row in events if row["loom_task_image_stage"] == "solve" and row["event"] == "end"]
    assert all(isinstance(row["duration_ms"], int) and row["duration_ms"] >= 0 for row in ended)
    if build_exit:
        assert len(events) == 2 and ended[0]["failed"] and ended[0]["exit"] == build_exit
    else:
        assert len(events) == component_count * 5
        assert [row["component_index"] for row in ended] == list(range(component_count))
        assert [row["bytes"] for row in events if row["loom_task_image_stage"] == "oci_export"] == [7] * component_count


@pytest.mark.timeout(150)
def test_generated_nested_script_enforces_actual_timer(tmp_path):
    binary = tmp_path / "buildctl-daemonless.sh"
    binary.write_text('#!/bin/sh\nexec sleep 30\n')
    binary.chmod(0o555)
    status, logs = _run_generated(tmp_path, _script(timeout=1),
        extra_mounts={str(binary): {"bind": "/proof/buildctl-daemonless.sh", "mode": "ro"}},
        environment={"PATH": "/proof:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"})
    assert status == 124, logs
    events = [json.loads(line) for line in logs.splitlines() if line.startswith('{"loom_task_image_stage"')]
    assert len(events) == 2 and events[-1]["failed"] and events[-1]["exit"] == 124


@pytest.mark.timeout(150)
@pytest.mark.parametrize("private_run", [False, True])
def test_real_buildkit_run_and_user_preserve_limits_and_high_owners(tmp_path, private_run):
    """Minimal public BusyBox rootfs; real Dockerfile RUN/USER and OCI output."""
    context = tmp_path / "work/context"
    context.mkdir(parents=True)
    client = docker.from_env(timeout=60)
    source = None
    try:
        image = client.images.get(BUILDKIT_IMAGE)
        machine = {"arm64": "aarch64", "amd64": "x86_64"}[image.attrs["Architecture"]]
        source = client.containers.create(BUILDKIT_IMAGE, entrypoint=["true"], network_mode="none")
        # Read two ordinary files from the exact cached public image; this
        # container never starts, and no registry/network access is needed.
        for name in ("bin/busybox", f"lib/ld-musl-{machine}.so.1"):
            chunks, _ = source.get_archive("/" + name)
            with tarfile.open(fileobj=io.BytesIO(b''.join(chunks)), mode="r:") as archive:
                member, = archive.getmembers()
                assert member.isreg()
                body = archive.extractfile(member)
                assert body is not None
                path = context / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(body.read())
                path.chmod(0o755)
    finally:
        if source is not None:
            source.remove(v=True)
        client.close()
    (context / "Dockerfile").write_text('''FROM scratch
COPY bin/ /bin/
COPY lib/ /lib/
RUN ["/bin/busybox", "sh", "-ec", "test $(ulimit -Su):$(ulimit -Hu) = 512:512; /bin/busybox mkdir /out; /bin/busybox chmod 777 /out; echo root > /out/root"]
USER 197609:197121
RUN ["/bin/busybox", "sh", "-ec", "test $(/bin/busybox id -u):$(/bin/busybox id -g) = 197609:197121; test $(ulimit -Su):$(ulimit -Hu) = 512:512; echo user > /out/user"]
''')
    script = _script(timeout=60)
    if not private_run:
        script = script.replace(" --copy-up=/run", "")
    status, logs = _run_generated(tmp_path, script)
    if not private_run:
        assert status == 1 and ("permission denied" in logs or "read-only file system" in logs), logs
        assert "/run/buildkit" in logs
        return
    assert status == 0, logs
    archive_path = tmp_path / "work/oci/0000.tar"
    validate_native_oci_archive(archive_path, architecture=image.attrs["Architecture"])
    observed = {}
    with tarfile.open(archive_path, "r:") as archive:
        def metadata(name):
            body = archive.extractfile(name)
            assert body is not None
            return json.load(body)
        descriptor = metadata("index.json")["manifests"][0]
        manifest = metadata("blobs/sha256/" + descriptor["digest"].removeprefix("sha256:"))
        config = metadata("blobs/sha256/" + manifest["config"]["digest"].removeprefix("sha256:"))
        assert config["config"]["User"] == "197609:197121"
        for layer in manifest["layers"]:
            body = archive.extractfile("blobs/sha256/" + layer["digest"].removeprefix("sha256:"))
            assert body is not None
            with tarfile.open(fileobj=io.BytesIO(body.read()), mode="r:*") as content:
                for member in content:
                    if member.name in {"out/root", "out/user"}:
                        file = content.extractfile(member)
                        assert file is not None
                        observed[member.name] = (member.uid, member.gid, file.read())
    assert observed == {"out/root": (0, 0, b"root\n"), "out/user": (197609, 197121, b"user\n")}
