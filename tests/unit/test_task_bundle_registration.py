"""Catalog identities must not rewrite or race the authored bundle snapshot."""

from __future__ import annotations

import hashlib
import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
import rfc8785

from loom.models.task_checksum import task_checksum
from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest


def _module():
    return importlib.import_module("loom.task_bundle_registration")


def _bundle(tmp_path):
    root = tmp_path / "bundle"
    root.mkdir()
    (root / "task.toml").write_text(
        'schema_version = "1"\n'
        '[task]\nid = "local-id"\nname = "Source task"\n'
        '[environment]\nos = "linux"\ndockerfile = "Dockerfile"\n'
        '[agent]\nname = "oracle"\n'
        '[verifier]\nname = "pytest"\n'
        '[[steps]]\nname = "main"\n',
    )
    (root / "Dockerfile").write_text("FROM python:3.11-slim\n")
    return root


def test_registration_binds_catalog_and_authored_identity_without_rewriting_bytes(tmp_path):
    root = _bundle(tmp_path)
    before = (root / "task.toml").read_bytes()
    captured = capture_task_image_bundle_manifest(root)
    registered = _module().prepare_task_bundle_registration(
        root,
        task_id="benchmark/local-id",
    )
    assert registered.task_config.task.id == "benchmark/local-id"
    assert registered.task_config.task.name == "Source task"
    assert registered.manifest == captured
    assert registered.manifest.task_checksum == task_checksum(root)
    assert (root / "task.toml").read_bytes() == before
    provenance = registered.source_provenance
    assert provenance["bundle_content_manifest_sha256"] == captured.digest
    assert provenance["bundle_file_metadata_sha256"] == (
        "sha256:" + captured.bundle_file_metadata_sha256
    )
    identity = provenance["bundle_task_identity"]
    assert identity == {
        "schema_version": "loom.task-bundle-identity.v1",
        "catalog_task_id": "benchmark/local-id",
        "bundle_task_id": "local-id",
        "bundle_task_toml_sha256": hashlib.sha256(before).hexdigest(),
        "registered_config_sha256": hashlib.sha256(
            rfc8785.dumps(registered.task_config.model_dump(mode="json")),
        ).hexdigest(),
    }
    assert (
        _module().prepare_task_bundle_registration(
            root,
            task_id="benchmark/local-id",
        )
        == registered
    )


@pytest.mark.parametrize("task_id", ["", "a" * 513, "bad\0id", "bad\nid"])
def test_registration_refuses_unrepresentable_catalog_identity(tmp_path, task_id):
    root = _bundle(tmp_path)
    with pytest.raises(ValueError):
        _module().prepare_task_bundle_registration(root, task_id=task_id)


def test_registration_rejects_task_toml_changed_after_manifest_capture(tmp_path, monkeypatch):
    root = _bundle(tmp_path)
    module = _module()
    original_capture = module.capture_task_image_bundle_manifest

    def capture_then_change(path):
        manifest = original_capture(path)
        task_toml = path / "task.toml"
        task_toml.write_bytes(task_toml.read_bytes().replace(b"local-id", b"other-id"))
        return manifest

    monkeypatch.setattr(module, "capture_task_image_bundle_manifest", capture_then_change)
    with pytest.raises(ValueError):
        module.prepare_task_bundle_registration(root, task_id="benchmark/local-id")


def test_registration_requires_the_captured_task_configuration(tmp_path):
    root = _bundle(tmp_path)
    (root / "task.toml").unlink()
    with pytest.raises(ValueError):
        _module().prepare_task_bundle_registration(root, task_id="benchmark/local-id")


def test_registration_normalizes_terminal_bench_shape_and_retains_source_identity(tmp_path):
    root = _bundle(tmp_path)
    path = root / "task.toml"
    path.write_text(path.read_text().replace("[task]", "[metadata]"))
    registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    assert registered.task_config.task.id == "bench/local-id"
    assert registered.source_provenance["bundle_task_identity"]["bundle_task_id"] == "local-id"
    assert b"[metadata]" in path.read_bytes()


def test_registration_supplies_anonymous_harbor_identity_without_changing_source(tmp_path):
    root = _bundle(tmp_path)
    path = root / "task.toml"
    source = b'[metadata]\ntags = ["shell"]\n[environment]\ncpus = 2\nmemory = "4G"\nstorage = "10G"\n'
    path.write_bytes(source)

    registered = _module().prepare_task_bundle_registration(root, task_id="custom/alpha")

    assert registered.task_config.task.id == "custom/alpha"
    assert registered.task_config.environment.cpus == 2
    assert registered.task_config.environment.memory_mb == 4096
    assert path.read_bytes() == source


def test_registration_views_cannot_change_the_frozen_binding(tmp_path):
    root = _bundle(tmp_path)
    registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    provenance = registered.source_provenance
    provenance["bundle_task_identity"]["catalog_task_id"] = "changed"
    config = registered.task_config
    config.task.labels.append("changed")
    assert registered.task_config.task.labels == []
    assert (
        registered.source_provenance["bundle_task_identity"]["catalog_task_id"] == "bench/local-id"
    )


@pytest.mark.parametrize("explicit_arch", [None, "x86_64", "arm64", "any"])
def test_registration_rejects_arm_without_rewriting_architecture(
    tmp_path,
    explicit_arch,
):
    root = _bundle(tmp_path)
    (root / "Dockerfile").write_text("FROM mictern2/terminus2-full:latest\n")
    if explicit_arch is not None:
        path = root / "task.toml"
        path.write_text(
            path.read_text().replace(
                "[environment]",
                f'[environment]\ncpu_arch = "{explicit_arch}"',
            )
        )
    unchanged = capture_task_image_bundle_manifest(root)
    if explicit_arch == "arm64":
        with pytest.raises(ValueError, match="x86"):
            _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    else:
        registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
        assert registered.task_config.environment.cpu_arch == (explicit_arch or "x86_64")
        assert registered.manifest == unchanged


@pytest.mark.parametrize("name", ["task.toml"])
def test_registration_bounds_parsed_text_before_allocating_verified_payload(
    tmp_path,
    monkeypatch,
    name,
):
    root = _bundle(tmp_path)
    module = _module()
    path = root / name
    original = path.read_bytes()
    # Valid comment padding: reject size, not TOML syntax or Dockerfile contents.
    path.write_bytes(original + b"\n#" + b"x" * (1024 * 1024))
    read = module.read_verified_task_image_bundle_file
    oversized_reads = []

    def observe_read(directory, entry):
        if entry.path == name:
            oversized_reads.append(entry.path)
        return read(directory, entry)

    monkeypatch.setattr(module, "read_verified_task_image_bundle_file", observe_read)
    with pytest.raises(ValueError, match="text size limit"):
        module.prepare_task_bundle_registration(
            root,
            task_id="bench/local-id",
        )
    assert oversized_reads == []


def test_registration_config_and_digest_are_stable_across_process_hash_seeds(tmp_path):
    root = _bundle(tmp_path)
    path = root / "task.toml"
    path.write_text(
        path.read_text()
        .replace(
            "[environment]",
            '[environment]\nnetwork_policies_supported = ["no-network", "public", "allowlist"]',
        )
        .replace(
            'schema_version = "1"',
            'schema_version = "1"\nrequired_agent_capabilities = ["zeta", "alpha", "beta"]',
        )
    )
    script = """
import json, sys
from pathlib import Path
from loom.task_bundle_registration import prepare_task_bundle_registration
value = prepare_task_bundle_registration(Path(sys.argv[1]), task_id='bench/local-id')
config = getattr(value, 'task_config_document', value.task_config.model_dump(mode='json'))
print(json.dumps([value.source_provenance, config], sort_keys=True))
"""
    snapshots = [
        subprocess.run(
            [sys.executable, "-c", script, str(root)],
            env={"PYTHONHASHSEED": str(seed), "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")},
            check=True,
            text=True,
            capture_output=True,
            timeout=30,
        ).stdout
        for seed in (1, 2, 3)
    ]
    assert len(set(snapshots)) == 1
    provenance, config = json.loads(snapshots[0])
    assert config["environment"]["network_policies_supported"] == [
        "allowlist",
        "no-network",
        "public",
    ]
    assert config["required_agent_capabilities"] == ["alpha", "beta", "zeta"]
    assert provenance["bundle_task_identity"]["registered_config_sha256"] == (
        hashlib.sha256(rfc8785.dumps(config)).hexdigest()
    )
    registered = _module().prepare_task_bundle_registration(root, task_id="bench/local-id")
    copy = registered.task_config_document
    copy["required_agent_capabilities"].append("changed")
    assert registered.task_config_document == config


def test_source_capture_can_preserve_target_architecture_for_catalog_admission(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    (root / "task.toml").write_text("""schema_version = "1"
[task]
id = "authored"
name = "ARM source"
[environment]
os = "linux"
cpu_arch = "arm64"
docker_image = "alpine"
[agent]
name = "oracle"
[verifier]
name = "pytest"
[[steps]]
name = "main"
""")
    registered = _module().prepare_task_bundle_registration(
        root, task_id="bench/arm", check_target_architecture=False,
    )
    assert registered.task_config.environment.cpu_arch == "arm64"
    assert registered.task_config.task.id == "bench/arm"
