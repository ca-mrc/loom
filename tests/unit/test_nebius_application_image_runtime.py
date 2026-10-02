"""Trusted preparation consumes source bytes without executing developer Python."""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

import pytest

from loom.application_image_build import ApplicationImageBuildClaimV1
from loom.application_source_archive import write_application_source_archive
from loom_execution_actuator.application_image_renderer import render_application_image_job
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import BUILDKIT_IMAGE, TaskImageJobConfig
from tests.unit.test_application_source import entry, manifest


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


def test_static_schema_parser_reads_real_repository_without_loading_migrations():
    from loom_execution_actuator.application_image_runtime import qualify_source_schema

    root = Path(__file__).resolve().parents[2]
    qualify_source_schema(root, expected_revision="0173")
