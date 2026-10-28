"""Run independent native grading with defaults read from actual built OCI images."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path
from uuid import uuid4

import pytest

from loom.harbor_bundle import prepare_harbor_bundle
from loom.harbor_task_import import project_harbor_task
from loom.harbor_verifier_script import (
    native_verifier_run_sh_bytes,
    native_verifier_workdir_run_sh_bytes,
)
from loom.models.task import TaskConfig
from loom.sandbox_identity import resolve_sandbox_identity
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task
from loom_execution_actuator import task_image_runtime

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def _docker(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _metadata_archive_from_docker_save(saved: Path, target: Path) -> tuple[str, str]:
    # Docker save supplies this compatibility manifest for both the classic
    # graphdriver layout and the containerd OCI layout. Repackage its actual
    # config/layer bytes into the one-image OCI layout consumed in production.
    with tarfile.open(saved) as source, tarfile.open(target, "w") as output:

        def read(name: str) -> bytes:
            stream = source.extractfile(name)
            assert stream is not None
            return stream.read()

        def add(name: str, data: bytes) -> None:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            output.addfile(member, io.BytesIO(data))

        def blob(data: bytes, media_type: str) -> dict:
            digest = "sha256:" + hashlib.sha256(data).hexdigest()
            add("blobs/sha256/" + digest.removeprefix("sha256:"), data)
            return {"digest": digest, "size": len(data), "mediaType": media_type}

        entries = json.loads(read("manifest.json"))
        assert len(entries) == 1
        image = entries[0]
        config = read(image["Config"])
        architecture = json.loads(config)["architecture"]
        assert architecture in {"amd64", "arm64"}
        layers = []
        for name in image["Layers"]:
            data = read(name)
            compression = "+gzip" if data.startswith(b"\x1f\x8b") else ""
            if data.startswith(b"\x28\xb5\x2f\xfd"):
                compression = "+zstd"
            layers.append(blob(data, "application/vnd.oci.image.layer.v1.tar" + compression))
        manifest = blob(
            json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": blob(config, "application/vnd.oci.image.config.v1+json"),
                    "layers": layers,
                }
            ).encode(),
            "application/vnd.oci.image.manifest.v1+json",
        )
        add("oci-layout", b'{"imageLayoutVersion":"1.0.0"}')
        add("index.json", json.dumps({"schemaVersion": 2, "manifests": [manifest]}).encode())
        return manifest["digest"], architecture


@pytest.mark.parametrize("verifier_workdir", [None, "/tests", "/grading"])
def test_independent_native_verifier_runs_with_own_image_defaults_and_json_reward(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verifier_workdir: str | None,
) -> None:
    expected_workdir = verifier_workdir or "/"
    bundle = tmp_path / "bundle"
    (bundle / "environment").mkdir(parents=True)
    (bundle / "tests").mkdir()
    (bundle / "instruction.md").write_text("Independent grading fixture\n")
    (bundle / "environment/Dockerfile").write_text(
        "FROM python:3.11-slim\n"
        "RUN groupadd --gid 65532 agent && useradd --uid 65532 --gid 65532 --create-home agent\n"
        "USER agent\nWORKDIR /agent-work\n",
    )
    (bundle / "tests/Dockerfile").write_text(
        "FROM python:3.11-slim\n"
        "RUN mkdir -p /tests /audit && chmod 755 /tests /audit\n"
        + (f"WORKDIR {verifier_workdir}\n" if verifier_workdir is not None else ""),
    )
    (bundle / "tests/sentinel").write_text("private tests were injected\n")
    (bundle / "tests/test.sh").write_text(
        "set -eu\n"
        'test "$(id -u)" = 0\n'
        f'test "$PWD" = {expected_workdir}\n'
        'test "$(cat /tests/sentinel)" = "private tests were injected"\n'
        'printf "uid=%s cwd=%s\\n" "$(id -u)" "$PWD" > /audit/observed\n'
        'echo .7 > "$LOOM_HARBOR_LOG_DIR/reward.txt"\n'
        'echo \'{"reward": 0.73}\' > "$LOOM_HARBOR_LOG_DIR/reward.json"\n',
    )
    config = project_harbor_task(
        {
            "task": {"name": "independent-native-grader"},
            "environment": {"architecture": "any", "cpus": 1, "memory_mb": 512, "storage_mb": 1024},
            "verifier": {
                "environment": {
                    "architecture": "any",
                    "cpus": 1,
                    "memory_mb": 512,
                    "storage_mb": 1024,
                }
            },
        }
    )
    prepare_harbor_bundle(bundle, config)
    task = TaskConfig.model_validate(config)
    # Hosted independent grading keeps the immutable task assets private and
    # supplies the image working directory to the shared runtime bridge.
    assert (bundle / "verifier/run.sh").read_bytes() == native_verifier_run_sh_bytes()
    runtime_bridge = tmp_path / "runtime-bridge.sh"
    runtime_bridge.write_bytes(native_verifier_workdir_run_sh_bytes())
    output = tmp_path / "output"
    output.mkdir()
    tags: dict[str, str] = {}
    images: dict[str, str] = {}
    work = tmp_path / "publication"
    (work / "oci").mkdir(parents=True)
    container_name = f"loom-grader-defaults-{uuid4().hex}"
    try:
        for component, directory in (("task", "environment"), ("verifier", "tests")):
            tag = f"loom-grader-defaults-{uuid4().hex}"
            tags[component] = tag
            archive = work / "oci" / f"{len(images):04}.tar"
            saved = tmp_path / f"{component}-docker.tar"
            built = _docker("build", "--tag", tag, str(bundle / directory))
            assert built.returncode == 0, built.stderr
            exported = _docker("image", "save", "--output", str(saved), tag)
            assert exported.returncode == 0, exported.stderr
            digest, architecture = _metadata_archive_from_docker_save(saved, archive)
            images[component] = f"{tag}@{digest}"
        # Run the actual publisher against real built OCI bytes. Only registry
        # transport is stubbed; no private registry or credentials are needed.
        published = []

        def copy_to_registry(argv, **kwargs):
            assert argv[:1] == ["skopeo"] and "copy" in argv
            component = ("task", "verifier")[len(published)]
            Path(argv[argv.index("--digestfile") + 1]).write_text(images[component].split("@", 1)[1])
            published.append(component)
            return subprocess.CompletedProcess(argv, 0, "", "")

        claim = {"id": str(uuid4()), "lease_epoch": 1, "materialization_key": "1" * 64,
                 "cpu_arch": "arm64" if architecture == "arm64" else "x86_64",
                 "task_config": task.model_dump(mode="json"), "registry_repository": "registry.invalid/native"}
        with monkeypatch.context() as patch:
            patch.setattr(task_image_runtime.subprocess, "run", copy_to_registry)
            receipt = task_image_runtime.publish(claim, work, tmp_path / "secrets",
                                                  receipt_path=tmp_path / "publication.json")
        assert published == ["task", "verifier"]
        assert receipt["image_runtime_metadata"]["verifier"]["workdir"] == expected_workdir
        grant = TaskImageExecutionGrantV1(
            schema_version="loom.task-image-execution-grant.v1",
            materialization_id=uuid4(),
            materialization_key="1" * 64,
            cpu_arch="arm64" if architecture == "arm64" else "x86_64",
            task_checksum="2" * 64,
            task_config=task.model_dump(mode="json"),
            task_source=None,
            task_source_provenance={},
            registry_images=receipt["registry_images"],
            image_runtime_metadata=receipt["image_runtime_metadata"],
        )
        resolved = resolve_prepared_task(task, grant)
        verifier = resolved.verifier.environment
        assert verifier is not None
        identity = resolve_sandbox_identity(verifier.user, "/home/agent")
        assert identity is not None
        result = _docker(
            "run",
            "--rm",
            "--name",
            container_name,
            "--network",
            "none",
            "--user",
            f"{identity.run_as_user}:{identity.run_as_group}",
            "--workdir",
            str(verifier.workdir),
            "--volume",
            f"{bundle}:/loom/verifier/task:ro",
            "--volume",
            f"{runtime_bridge}:/loom/runtime-bridge.sh:ro",
            "--volume",
            f"{output}:/output",
            "--env",
            "LOOM_TASK_DIR=/loom/verifier/task",
            "--env",
            "LOOM_HARBOR_WORKDIR=" + str(verifier.workdir),
            "--env",
            "LOOM_HARBOR_LOG_DIR=/logs/verifier",
            "--env",
            "LOOM_VERIFIER_OUTPUT=/output/result.json",
            tags["verifier"],
            "sh",
            "-c",
            "sh /loom/runtime-bridge.sh && cat /audit/observed",
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert resolved.environment.user == "65532:65532"
        assert str(resolved.environment.workdir) == "/agent-work"
        assert verifier.user == "0:0"
        assert str(verifier.workdir) == expected_workdir
        assert result.stdout.strip() == f"uid=0 cwd={expected_workdir}"
        assert json.loads((output / "result.json").read_text())["rewards"] == {"reward": 0.73}
    finally:
        _docker("rm", "--force", container_name)
        for index, (component, tag) in enumerate(tags.items()):
            _docker("image", "rm", "--force", tag)
            (work / "oci" / f"{index:04}.tar").unlink(missing_ok=True)
            (tmp_path / f"{component}-docker.tar").unlink(missing_ok=True)
