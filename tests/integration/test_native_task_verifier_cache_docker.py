"""Real pinned rootless/native cache imports; only task-owned local Docker volumes."""

from __future__ import annotations

import gzip
import io
import json
import re
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest

from loom.task_image_build_plan import TaskImageBuildComponentV1
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import (
    BUILDKIT_IMAGE,
    TaskImageJobConfig,
    render_task_image_job,
)

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


def _archive(files: dict[str, bytes]) -> bytes:
    result = io.BytesIO()
    with tarfile.open(fileobj=result, mode="w") as archive:
        for name, body in files.items():
            item = tarfile.TarInfo(name)
            item.size, item.mode = len(body), 0o644
            archive.addfile(item, io.BytesIO(body))
    return result.getvalue()


def _file(container, path: str) -> bytes:
    stream, _ = container.get_archive(path)
    with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as archive:
        return archive.extractfile(next(m for m in archive if m.isfile())).read()


@dataclass
class Build:
    code: int
    log: str
    work: object
    scratch: object


class Harness:
    def __init__(self, client, subids: Path):
        self.client, self.subids = client, subids
        self.volumes = []
        self.containers = []

    def container(self, work, scratch=None, *, root=False, extra=None):
        volumes = {work.name: {"bind": "/loom/build", "mode": "rw"}}
        if scratch:
            volumes[scratch.name] = {"bind": "/scratch", "mode": "rw"}
        volumes.update(extra or {})
        for name in ("subuid", "subgid"):
            volumes[str(self.subids / name)] = {"bind": "/etc/" + name, "mode": "ro"}
        host = self.client.api.create_host_config(
            binds=volumes,
            network_mode="none",
            nano_cpus=8_000_000_000,
            mem_limit="8g",
            pids_limit=1024,
            security_opt=["seccomp=unconfined", "apparmor=unconfined"],
        )
        # Docker CLI's systempaths=unconfined translates to these OCI paths;
        # it is not a daemon SecurityOpt string. Match production procMount.
        host.update(MaskedPaths=[], ReadonlyPaths=[])
        created = self.client.api.create_container(
            BUILDKIT_IMAGE,
            ["-c", "sleep 300"],
            entrypoint="sh",
            user="0" if root else "1000:1000",
            volumes=[v["bind"] for v in volumes.values()],
            host_config=host,
        )
        result = self.client.containers.get(created["Id"])
        self.containers.append(result)
        result.start()
        return result

    def volume(self):
        result = self.client.volumes.create("loom-p1-native-cache-" + uuid4().hex)
        self.volumes.append(result)
        return result

    def build(
        self, dockerfiles: list[tuple[str, str]], *, sibling=False, own=None, damage=None
    ) -> Build:
        work, scratch = self.volume(), self.volume()
        extra = {own.work.name: {"bind": "/seed", "mode": "ro"}} if own else {}
        init = self.container(work, scratch, root=True, extra=extra)
        commands = ["mkdir -p /loom/build/context /scratch", "chown 1000:1000 /loom/build /scratch"]
        for i, _ in enumerate(dockerfiles):
            rootfs = f"/loom/build/context/c{i}/rootfs"
            commands.append(
                f"mkdir -p {rootfs}/bin {rootfs}/lib; cp /bin/busybox {rootfs}/bin/; cp /lib/ld-musl-*.so.1 {rootfs}/lib/"
            )
        if own:
            index = len(dockerfiles) - 1
            commands.append(
                f"mkdir -p /loom/build/cache-in/{index}; cp -a /seed/cache-out/0/. /loom/build/cache-in/{index}/"
            )
        result = init.exec_run(["sh", "-ec", "\n".join(commands)])
        assert result.exit_code == 0, result.output.decode()
        assert init.put_archive(
            "/loom/build/context",
            _archive(
                {f"c{i}/Dockerfile": body.encode() for i, (_, body) in enumerate(dockerfiles)}
            ),
        )
        if own and damage:
            damage(init, f"/loom/build/cache-in/{len(dockerfiles) - 1}")
        assert (
            init.exec_run(["sh", "-ec", "chown -R 1000:1000 /loom/build /scratch"]).exit_code == 0
        )
        init.remove(force=True)
        container = self.container(work, scratch)
        components = tuple(
            TaskImageBuildComponentV1(
                name=name,
                dockerfile_path=f"c{i}/Dockerfile",
                context_path=f"c{i}",
                oci_output_path=f"oci/{i:04d}.tar",
            )
            for i, (name, _) in enumerate(dockerfiles)
        )
        architecture = self.client.info()["Architecture"]
        _, job = render_task_image_job(
            materialization_id=uuid4(),
            lease_epoch=1,
            claim={"cpu_arch": "arm64" if architecture in {"aarch64", "arm64"} else "x86_64"},
            components=components,
            target=ExecutionTargetRuntime(
                target_id="local-test", namespace="local-test", node_selector={}, tolerations=()
            ),
            config=TaskImageJobConfig(
                service_image="fixture.invalid/service@sha256:" + "a" * 64,
                source_secret_name="fixture-source",
                registry_secret_name="fixture-registry",
                cache_secret_name="fixture-cache",
                cpu_millis=8000,
                memory_mib=8192,
                ephemeral_storage_mib=65536,
                reuse_task_cache_for_verifier=sibling,
            ),
        )
        builder = job["spec"]["template"]["spec"]["initContainers"][1]
        result = container.exec_run(
            builder["command"], environment={x["name"]: x["value"] for x in builder["env"]}
        )
        return Build(result.exit_code, result.output.decode(), work, scratch)

    def reader(self, build):
        return self.container(build.work, root=True)

    def close(self):
        for container in reversed(self.containers):
            try:
                container.remove(force=True)
            except Exception:
                pass
        for volume in reversed(self.volumes):
            volume.remove()


@pytest.fixture(scope="module")
def harness(tmp_path_factory):
    import docker

    context = subprocess.check_output(["docker", "context", "show"], text=True).strip()
    endpoint = subprocess.check_output(
        ["docker", "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"],
        text=True,
    ).strip()
    assert context in {"default", "desktop-linux"} and endpoint.startswith("unix://"), (
        "local disposable Docker only"
    )
    client = docker.DockerClient(base_url=endpoint)
    assert client.info()["OSType"] == "linux"
    # Never use an unpinned replacement. Pulling the fixed public digest is a
    # normal developer dependency download, not a cloud deployment.
    try:
        client.images.get(BUILDKIT_IMAGE)
    except docker.errors.ImageNotFound:
        client.images.pull(BUILDKIT_IMAGE)
    directory = tmp_path_factory.mktemp("public-native-subids")
    for name in ("subuid", "subgid"):
        (directory / name).write_text("user:100000:262144\nroot:1:262144\n")
    result = Harness(client, directory)
    try:
        yield result
    finally:
        result.close()
        client.close()


def _shared(value="new"):
    return (
        "FROM scratch AS shared\nCOPY rootfs/ /\n"
        'RUN ["/bin/busybox","sh","-c","echo COMMON_' + value + ' > /common"]\n'
        'RUN ["/bin/busybox","sh","-c","/bin/busybox ln /common /hardlink; test $(/bin/busybox stat -c %i /common) = $(/bin/busybox stat -c %i /hardlink)"]\n'
    )


def _verifier(common="new", private="new"):
    return (
        "FROM scratch AS own\nCOPY rootfs/ /\n"
        'RUN ["/bin/busybox","sh","-c","echo OWN_BRANCH > /own"]\n'
        + _shared(common)
        + "COPY --from=own /own /own\n"
        'RUN ["/bin/busybox","sh","-c","/bin/busybox mkdir /private; /bin/busybox chmod 700 /private; echo PRIVATE_'
        + private
        + ' > /private/marker"]\n'
    )


def _cached(log, marker):
    ids = re.findall(r"^#(\d+) \[.*?\] RUN .*" + re.escape(marker) + r".*$", log, re.M)
    assert ids, log
    return any(re.search(r"^#" + number + r" CACHED$", log, re.M) for number in ids)


def _oci_files(archive_body):
    with tarfile.open(fileobj=io.BytesIO(archive_body)) as archive:

        def blob(digest):
            return archive.extractfile("blobs/sha256/" + digest.split(":")[1]).read()

        index = json.loads(archive.extractfile("index.json").read())
        manifest = json.loads(blob(index["manifests"][0]["digest"]))
        result = {}
        for layer in manifest["layers"]:
            body = blob(layer["digest"])
            if layer["mediaType"].endswith("+gzip"):
                body = gzip.decompress(body)
            with tarfile.open(fileobj=io.BytesIO(body)) as content:
                for member in content:
                    if member.isfile():
                        result[member.name.removeprefix("./")] = content.extractfile(member).read()
        return result


def test_real_same_job_sibling_and_own_prefixes_remain_independent(harness):
    own = harness.build([("verifier", _verifier("old", "old"))])
    assert own.code == 0, own.log
    original_index = _file(harness.reader(own), "/loom/build/cache-out/0/index.json")
    cold = harness.build([("task", _shared()), ("verifier", _verifier())], own=own)
    assert cold.code == 0, cold.log
    cold_verifier = cold.log.split('"component_index":1,"budget_seconds"')[1]
    assert not _cached(cold_verifier, "COMMON_new")
    warm = harness.build([("task", _shared()), ("verifier", _verifier())], sibling=True, own=own)
    assert warm.code == 0, warm.log
    verifier = warm.log.split('"component_index":1,"budget_seconds"')[1]
    assert _cached(verifier, "COMMON_new") and _cached(verifier, "OWN_BRANCH")
    assert not _cached(verifier, "PRIVATE_new")
    with_reader = harness.reader(warm)
    output = _oci_files(_file(with_reader, "/loom/build/oci/0001.tar"))
    assert output["private/marker"] == b"PRIVATE_new\n" and output["own"] == b"OWN_BRANCH\n"
    task_output = _oci_files(_file(with_reader, "/loom/build/oci/0000.tar"))
    assert "private/marker" not in task_output and "own" not in task_output
    assert _file(with_reader, "/loom/build/cache-in/1/index.json") == original_index
    assert _file(with_reader, "/loom/build/cache-out/0/index.json") != _file(
        with_reader, "/loom/build/cache-out/1/index.json"
    )
    cleanup_reader = harness.container(warm.work, warm.scratch, root=True)
    remaining = cleanup_reader.exec_run(
        [
            "find",
            "/scratch/state",
            "/scratch/tmp",
            "/scratch/runtime",
            "/scratch/docker-config",
            "-mindepth",
            "1",
            "-print",
        ]
    )
    assert remaining.exit_code == 0 and remaining.output == b""


def test_real_run_exit42_is_not_repeated_as_cold_fallback(harness):
    dockerfile = _shared() + 'RUN ["/bin/busybox","sh","-c","echo LOOM_EXIT42_ONCE; exit 42"]\n'
    result = harness.build([("task", dockerfile)])
    assert result.code == 1, result.log
    assert "exit code: 42" in result.log
    assert len(re.findall(r"^#\d+ [0-9.]+ LOOM_EXIT42_ONCE$", result.log, re.M)) == 1, result.log
    # RootlessKit's failure diagnostic quotes its argv (including the script).
    # Count emitted JSON lines, not that quoted source text.
    stages = [
        json.loads(line)
        for line in result.log.splitlines()
        if line.startswith('{"loom_task_image_stage":')
    ]
    assert sum(e["loom_task_image_stage"] == "solve" and e["event"] == "start" for e in stages) == 1


@pytest.fixture(scope="module")
def native_cache_seed(harness):
    result = harness.build([("task", _shared())])
    assert result.code == 0, result.log
    return result


@pytest.mark.parametrize(
    "damage,expected",
    [
        ("missing_index", 0),
        ("bad_index", 0),
        ("missing_manifest", 0),
        ("missing_config", 0),
        ("missing_layer", 1),
        ("bad_layer", 1),
    ],
)
def test_pinned_native_cache_damage_has_explicit_upstream_boundary(
    harness, native_cache_seed, damage, expected
):
    def mutate(container, directory):
        if damage in {"missing_index", "bad_index"}:
            path = directory + "/index.json"
        else:
            index = json.loads(_file(container, directory + "/index.json"))
            digest = index["manifests"][0]["digest"]
            assert re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
            path = directory + "/blobs/sha256/" + digest.split(":")[1]
            if damage != "missing_manifest":
                manifest = json.loads(_file(container, path))
                # Cache manifest order does not identify demanded content.
                # Select the fixture's required rootfs COPY layer instead of
                # an arbitrary small record that upstream can recompute.
                descriptor = (
                    manifest["config"]
                    if damage == "missing_config"
                    else max(manifest["layers"], key=lambda layer: layer["size"])
                )
                digest = descriptor["digest"]
                assert re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
                path = directory + "/blobs/sha256/" + digest.split(":")[1]
                if damage != "missing_config":
                    with tarfile.open(
                        fileobj=io.BytesIO(_file(container, path)), mode="r:*"
                    ) as layer:
                        assert any(m.name.removeprefix("./") == "bin/busybox" for m in layer)
        if damage.startswith("missing_"):
            result = container.exec_run(["rm", "-f", path])
        else:
            result = container.exec_run(
                ["sh", "-ec", 'printf not-valid-cache > "$1"', "mutate", path]
            )
        assert result.exit_code == 0

    result = harness.build([("task", _shared())], own=native_cache_seed, damage=mutate)
    assert result.code == expected, result.log[-2500:]
    if expected == 0:
        assert not _cached(result.log, "COMMON_new"), result.log
    stages = [
        json.loads(line)
        for line in result.log.splitlines()
        if line.startswith('{"loom_task_image_stage":')
    ]
    assert sum(e["loom_task_image_stage"] == "solve" and e["event"] == "start" for e in stages) == 1
