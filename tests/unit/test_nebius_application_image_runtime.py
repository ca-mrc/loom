"""Trusted preparation consumes source bytes without executing developer Python."""
from __future__ import annotations

import hashlib
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path
from uuid import uuid4

import pytest

from loom.application_image_build import ApplicationImageBuildClaimV1
from loom.application_source_archive import write_application_source_archive
from loom_execution_actuator.application_image_renderer import render_application_image_job
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import BUILDKIT_IMAGE, TaskImageJobConfig
from tests.unit.test_application_source import entry, manifest
from tests.unit.test_nebius_task_image_runtime import FakeS3


@pytest.fixture
def source_build(tmp_path):
    root = tmp_path / "snapshot"
    root.mkdir(mode=0o700)
    marker = tmp_path / "source-must-not-execute"
    files = {
        "database/migrations/versions/0001_initial.py": b'revision: str = "0001"\ndown_revision = None\n',
        "database/migrations/versions/0173_current.py": (
            'revision = "0173"\ndown_revision = "0001"\nbranch_labels = None\ndepends_on = None\n'
            f'from pathlib import Path\nPath({str(marker)!r}).touch()\n'
            'def upgrade():\n    raise RuntimeError("never run")\n'
        ).encode(),
        "deploy/Dockerfile.service": b"FROM scratch\nCOPY local.txt /local.txt\n",
        "deploy/Dockerfile.web": b"FROM scratch\nCOPY local.txt /local.txt\n",
        "local.txt": b"dirty source, not HEAD\n",
    }
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    source = manifest(*(entry(name, body) for name, body in sorted(files.items())))
    archive = io.BytesIO()
    write_application_source_archive(root, source, archive)
    payload = archive.getvalue()
    claim = ApplicationImageBuildClaimV1.model_validate({
        "build_id": str(uuid4()), "attempt": 1, "upload_id": str(uuid4()),
        "installation_id": str(uuid4()), "owner_user_id": str(uuid4()), "owner_team_id": str(uuid4()),
        "data_environment_id": str(uuid4()), "cluster_id": "cluster-development",
        "source": {"source_digest": source.digest, "archive_sha256": hashlib.sha256(payload).hexdigest(),
            "archive_size_bytes": len(payload), "base_commit": "c" * 40},
        "recipe": {"cpu_arch": "x86_64", "schema_revision": "0173",
            "trusted_image_ref": "registry.example/service@sha256:" + "d" * 64,
            "buildkit_image_ref": BUILDKIT_IMAGE},
        "storage_endpoint": "https://storage.eu-north1.nebius.cloud", "storage_region": "eu-north1",
        "source_bucket": "shared-source", "registry_repository": "cr.eu-north1.nebius.cloud/registryid/personal-apps",
    })
    return claim, payload, root, marker


class SourceObject:
    """Only the remote object store is doubled; extraction and parsing are real."""

    def __init__(self, claim, payload):
        self.claim, self.payload = claim, payload
        self.body = io.BytesIO(payload)
        self.closed = False

    def get_object(self, **kwargs):
        assert kwargs == {"Bucket": "shared-source", "Key": self.claim.source_key}
        return {"Body": self.body, "ContentLength": len(self.payload)}

    def close(self):
        self.closed = True


def rendered_claim(claim):
    config = TaskImageJobConfig(service_image=claim.recipe.trusted_image_ref,
        source_secret_name="source", registry_secret_name="registry", registry_auth_kind="nebius",
        oci_export_format=claim.recipe.oci_export_format)
    cm, _ = render_application_image_job(claim=claim,
        target=ExecutionTargetRuntime(target_id="primary", namespace="builds"), config=config)
    return json.loads(cm["data"]["claim.json"])


@pytest.mark.parametrize("export_format", ["archive", "directory"])
def test_loads_actual_renderer_claim_and_preserves_independent_application_identity(source_build, tmp_path, export_format):
    from loom_execution_actuator.application_image_runtime import load_claim

    claim = source_build[0]
    claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"oci_export_format": export_format})})
    path = tmp_path / "claim.json"
    path.write_text(json.dumps(rendered_claim(claim)))
    assert load_claim(path) == claim


@pytest.mark.parametrize("damage", ["arch", "component", "path", "extra", "duplicate", "size"])
def test_claim_rejects_renderer_envelope_drift_and_ambiguous_json(source_build, tmp_path, damage):
    from loom_execution_actuator.application_image_runtime import load_claim

    document = rendered_claim(source_build[0])
    if damage == "arch":
        document["cpu_arch"] = "arm64"
    elif damage == "component":
        document["components"].pop()
    elif damage == "path":
        document["components"][0]["dockerfile_path"] = "private/Dockerfile"
    elif damage == "extra":
        document["task_config"] = {}
    body = json.dumps(document)
    if damage == "duplicate":
        body = '{"attempt":999,' + body[1:]
    elif damage == "size":
        body += " " * (256 * 1024)
    path = tmp_path / "claim.json"
    path.write_text(body)
    with pytest.raises(ValueError):
        load_claim(path)


def test_prepare_extracts_exact_dirty_source_without_executing_migrations(source_build, tmp_path, monkeypatch):
    from loom_execution_actuator import application_image_runtime as runtime

    claim, payload, _, marker = source_build
    source = SourceObject(claim, payload)
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: source)
    work = tmp_path / "work"
    work.mkdir()
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "context/local.txt").read_bytes() == b"dirty source, not HEAD\n"
    assert (work / "context/deploy/Dockerfile.web").read_bytes() == b"FROM scratch\nCOPY local.txt /local.txt\n"
    assert (work / "oci").is_dir()
    assert not marker.exists()
    assert source.closed and source.body.closed


def test_schema_qualification_accepts_the_repository_git_placeholder(source_build):
    from loom_execution_actuator.application_image_runtime import qualify_source_schema

    _, _, root, marker = source_build
    (root / "database/migrations/versions/.gitkeep").write_bytes(b"")
    qualify_source_schema(root, expected_revision="0173")
    assert not marker.exists()


@pytest.mark.parametrize("kind", ["directory", "symlink"])
def test_schema_placeholder_must_remain_a_regular_file(source_build, kind):
    from loom_execution_actuator.application_image_runtime import qualify_source_schema

    _, _, root, _ = source_build
    path = root / "database/migrations/versions/.gitkeep"
    if kind == "directory":
        path.mkdir()
    else:
        path.symlink_to("0001_initial.py")
    with pytest.raises(ValueError):
        qualify_source_schema(root, expected_revision="0173")


@pytest.mark.parametrize("damage", ["truncated", "extra", "hash", "manifest", "schema", "occupied", "link"])
def test_prepare_rejects_unbound_content_or_destination_and_closes_source(source_build, tmp_path, monkeypatch, damage):
    from loom_execution_actuator import application_image_runtime as runtime

    claim, payload, _, marker = source_build
    if damage == "truncated":
        payload = payload[:-512]
    elif damage == "extra":
        payload += bytes(10240)
    elif damage == "hash":
        payload = payload[:-1] + b"!"
    elif damage == "manifest":
        claim = claim.model_copy(update={"source": claim.source.model_copy(update={"source_digest": "sha256:" + "f" * 64})})
    elif damage == "schema":
        claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"schema_revision": "0172"})})
    source = SourceObject(claim, payload)
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: source)
    work = tmp_path / "work"
    work.mkdir()
    if damage == "occupied":
        (work / "context").mkdir()
        (work / "context/retain").write_text("retain")
    elif damage == "link":
        (work / "context").symlink_to(source_build[2], target_is_directory=True)
    with pytest.raises(ValueError):
        runtime.prepare(claim, work, tmp_path / "secrets")
    assert not (work / "oci").exists() and not marker.exists()
    if damage not in {"occupied", "link"}:
        assert source.closed and source.body.closed
    if damage == "occupied":
        assert (work / "context/retain").read_text() == "retain"


@pytest.mark.parametrize("damage", ["wrong", "dynamic", "duplicate", "branch", "missing_parent", "cycle", "linked", "oversize"])
def test_static_schema_parser_rejects_ambiguous_or_incompatible_history(source_build, damage):
    from loom_execution_actuator.application_image_runtime import qualify_source_schema

    _, _, root, marker = source_build
    versions = root / "database/migrations/versions"
    path = versions / "0173_current.py"
    if damage == "wrong":
        path.write_text('revision="0174"\ndown_revision="0001"\n')
    elif damage == "dynamic":
        path.write_text('revision=str(173)\ndown_revision="0001"\n')
    elif damage == "duplicate":
        path.write_text('revision="0173"\nrevision="0173"\ndown_revision="0001"\n')
    elif damage == "branch":
        (versions / "other.py").write_text('revision="branch"\ndown_revision="0001"\n')
    elif damage == "missing_parent":
        path.write_text('revision="0173"\ndown_revision="missing"\n')
    elif damage == "cycle":
        (versions / "cycle1.py").write_text('revision="c1"\ndown_revision="c2"\n')
        (versions / "cycle2.py").write_text('revision="c2"\ndown_revision="c1"\n')
    elif damage == "linked":
        saved = root / "saved.py"
        path.rename(saved)
        path.symlink_to(saved)
    else:
        path.write_text("#" * (1024 * 1024 + 1))
    with pytest.raises(ValueError):
        qualify_source_schema(root, expected_revision="0173")
    assert not marker.exists()


def test_static_schema_parser_reads_real_repository_without_loading_migrations(tmp_path):
    from loom_execution_actuator.application_image_runtime import qualify_source_schema

    root = Path(__file__).resolve().parents[2]
    versions = tmp_path / "database/migrations/versions"
    versions.mkdir(parents=True)
    # Uploaded source excludes ignored bytecode generated by local test runs.
    for path in (root / "database/migrations/versions").glob("*.py"):
        shutil.copyfile(path, versions / path.name)
    qualify_source_schema(tmp_path, expected_revision="0175")


def oci_output(work, index, *, arch="amd64", directory=False):
    config = json.dumps({"architecture": arch, "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": []}}).encode()
    config_digest = hashlib.sha256(config).hexdigest()
    image = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:" + config_digest, "size": len(config)}, "layers": []}).encode()
    image_digest = hashlib.sha256(image).hexdigest()
    contents = {
        "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
        "index.json": json.dumps({"schemaVersion": 2, "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:" + image_digest, "size": len(image),
            "platform": {"architecture": arch, "os": "linux"}}]}).encode(),
        "blobs/sha256/" + config_digest: config, "blobs/sha256/" + image_digest: image,
    }
    path = work / "oci" / f"{index:04d}"
    path.parent.mkdir(exist_ok=True)
    if directory:
        path.mkdir()
        for name, body in contents.items():
            target = path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
    else:
        path = path.with_suffix(".tar")
        with tarfile.open(path, "w:") as archive:
            for name, body in contents.items():
                member = tarfile.TarInfo(name)
                member.size = len(body)
                archive.addfile(member, io.BytesIO(body))
    return path, image


@pytest.mark.parametrize("arch", ["x86_64", "arm64"])
@pytest.mark.parametrize("export_format", ["archive", "directory"])
def test_publish_validates_both_outputs_and_reads_back_exact_immutable_images(source_build, tmp_path, monkeypatch, arch, export_format):
    from loom_execution_actuator import application_image_runtime as runtime

    claim = source_build[0]
    claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"cpu_arch": arch, "oci_export_format": export_format})})
    work = tmp_path / "work"
    work.mkdir()
    images = [oci_output(work, index, arch="amd64" if arch == "x86_64" else "arm64",
        directory=export_format == "directory") for index in range(2)]
    receipt_path = tmp_path / "receipt.json"
    commands = []

    def run(argv, **kwargs):
        commands.append(argv)
        assert kwargs["check"] and kwargs["timeout"] <= 300
        if "copy" in argv:
            index = sum("copy" in previous for previous in commands) - 1
            assert "--preserve-digests" in argv
            assert argv[-1] == f"docker://{claim.registry_repository}:app-{claim.build_id.hex}-a1-{index}"
            transport = "oci" if export_format == "directory" else "oci-archive"
            assert argv[-2] == f"{transport}:{images[index][0]}"
            Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + hashlib.sha256(images[index][1]).hexdigest())
        else:
            assert "inspect" in argv and "--raw" in argv
            assert argv[-1] == f"docker://{claim.registry_repository}@sha256:" + hashlib.sha256(images[0][1]).hexdigest()
            kwargs["stdout"].write(images[0][1])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", run)
    receipt = runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_path)
    assert receipt.build_id == claim.build_id and receipt.attempt == 1
    assert receipt.upload_id == claim.upload_id and receipt.owner_user_id == claim.owner_user_id
    assert receipt.installation_id == claim.installation_id and receipt.data_environment_id == claim.data_environment_id
    assert receipt.source_digest == claim.source.source_digest and receipt.recipe_digest == claim.recipe.digest
    assert receipt.schema_revision == "0173" and receipt.cpu_arch == arch
    expected_ref = claim.registry_repository + "@sha256:" + hashlib.sha256(images[0][1]).hexdigest()
    assert receipt.registry_images == {"service": expected_ref, "web": expected_ref}
    assert json.loads(receipt_path.read_text()) == receipt.model_dump(mode="json")
    assert len(commands) == 4


@pytest.mark.parametrize("damage", ["link", "truncated", "extra", "wrong_platform", "wrong_readback", "lost_reply"])
def test_invalid_or_uncertain_publication_never_becomes_a_ready_release(source_build, tmp_path, monkeypatch, damage):
    from loom_execution_actuator import application_image_runtime as runtime

    claim = source_build[0]
    work = tmp_path / "work"
    work.mkdir()
    first, body = oci_output(work, 0)
    second, _ = oci_output(work, 1, arch="arm64" if damage == "wrong_platform" else "amd64")
    if damage == "link":
        second.unlink()
        second.symlink_to(first)
    elif damage == "truncated":
        second.write_bytes(b"invalid")
    elif damage == "extra":
        oci_output(work, 2)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert damage in {"wrong_readback", "lost_reply"}, "invalid local outputs must fail before registry writes"
        if damage == "lost_reply":
            raise subprocess.TimeoutExpired(argv, 300)
        if "copy" in argv:
            Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + hashlib.sha256(body).hexdigest())
        else:
            kwargs["stdout"].write(b"different manifest")
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", run)
    receipt = tmp_path / "receipt.json"
    with pytest.raises((ValueError, subprocess.TimeoutExpired)):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt)
    if receipt.exists():
        assert set(json.loads(receipt.read_text()).get("registry_images", {})) != {"service", "web"}
    if damage == "lost_reply":
        assert len(calls) == 1, "unknown publication is never automatically retried"


def test_application_cache_identity_reuses_source_recipe_not_owner_or_attempt(source_build):
    claim = source_build[0]
    assert claim.cache_key == claim.model_copy(update={"owner_user_id": uuid4(), "attempt": 3}).cache_key
    assert claim.cache_key != claim.model_copy(update={"source": claim.source.model_copy(
        update={"source_digest": "sha256:" + "f" * 64})}).cache_key
    assert claim.cache_key != claim.model_copy(update={"recipe": claim.recipe.model_copy(
        update={"cpu_arch": "arm64"})}).cache_key


def test_prepare_imports_verified_shared_cache_and_closes_each_credential_client(source_build, tmp_path, monkeypatch):
    from loom_execution_actuator import application_image_runtime as runtime

    claim, payload, _, _ = source_build
    claim = claim.model_copy(update={"cache_bucket": "shared-data"})
    content = b'{"schemaVersion":2,"manifests":[]}'
    sha = hashlib.sha256(content).hexdigest()
    cache = FakeS3({f"task-build-cache/v2/{claim.cache_key}/0/manifest.json": json.dumps({"version": 1,
        "files": [{"path": "index.json", "sha256": sha, "size": len(content)}]}).encode(),
        f"task-build-cache/v2/blobs/{sha}": content})
    source = SourceObject(claim, payload)
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: source if secret.name == "source" else cache)
    work = tmp_path / "work"
    work.mkdir()
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").read_bytes() == content
    assert source.closed and cache.closed and all(body.closed for body in cache.bodies)


def test_publication_exports_bounded_cache_through_the_existing_shared_blob_store(source_build, tmp_path, monkeypatch):
    from loom_execution_actuator import application_image_runtime as runtime

    claim = source_build[0].model_copy(update={"cache_bucket": "shared-data"})
    work = tmp_path / "work"
    work.mkdir()
    _, body = oci_output(work, 0)
    oci_output(work, 1)
    content = b'{"schemaVersion":2,"manifests":[]}'
    for index in range(2):
        directory = work / "cache-out" / str(index)
        directory.mkdir(parents=True)
        (directory / "index.json").write_bytes(content)
    cache = FakeS3({})
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: cache)

    def run(argv, **kwargs):
        if "copy" in argv:
            Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + hashlib.sha256(body).hexdigest())
        else:
            kwargs["stdout"].write(body)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", run)
    runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt")
    assert cache.objects["task-build-cache/v2/blobs/" + hashlib.sha256(content).hexdigest()] == content
    assert {f"task-build-cache/v2/{claim.cache_key}/{index}/manifest.json" for index in range(2)} <= cache.objects.keys()
    assert cache.closed


def test_interrupted_publication_progress_cannot_parse_as_a_ready_receipt(source_build, tmp_path, monkeypatch):
    from loom.application_image_build import ApplicationImagePublicationV1
    from loom_execution_actuator import application_image_runtime as runtime

    claim = source_build[0].model_copy(update={"cache_bucket": "shared-data"})
    work = tmp_path / "work"
    work.mkdir()
    _, body = oci_output(work, 0)
    oci_output(work, 1)
    cache_dir = work / "cache-out/0"
    cache_dir.mkdir(parents=True)
    (cache_dir / "index.json").write_text("cache")

    class InterruptedCache(FakeS3):
        def put_object(self, **kwargs):
            raise RuntimeError("interrupted before completion")

    cache = InterruptedCache({})
    monkeypatch.setattr(runtime, "_client", lambda binding, secret: cache)

    def run(argv, **kwargs):
        if "copy" in argv:
            Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + hashlib.sha256(body).hexdigest())
        else:
            kwargs["stdout"].write(body)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", run)
    path = tmp_path / "receipt"
    with pytest.raises(RuntimeError, match="interrupted"):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=path)
    with pytest.raises(ValueError):
        ApplicationImagePublicationV1.model_validate_json(path.read_bytes())
    assert cache.closed
