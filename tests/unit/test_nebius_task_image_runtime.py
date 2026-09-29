from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import subprocess
import tarfile
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import tomli_w
from botocore.exceptions import ClientError
from loom_bundle_checksum import sha256_of_dir

from loom.models.taskset import UserTaskSetManifest, bundle_object_key
from loom.taskset.materialize import _publish_service_execution_input_manifest, materialize_task_set
from loom.taskset.storage_bytes import taskset_root
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    bundle_file_metadata_body,
    bundle_file_metadata_sha256,
)
from loom_execution_actuator import task_image_runtime as runtime


def _write_native_oci(path: Path, *, platform: str = "linux/amd64") -> None:
    """Valid minimal local image, while subprocess tests retain a fake registry."""
    image_os, _, architecture = platform.partition("/")
    config = json.dumps(
        {"architecture": architecture, "os": image_os, "rootfs": {"type": "layers", "diff_ids": []}}
    ).encode()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:" + "c" * 64,
                "size": len(config),
            },
            "layers": [],
        }
    ).encode()
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:" + "d" * 64,
                    "size": len(manifest),
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
        }
    ).encode()
    with tarfile.open(path, "w:") as archive:
        for name, body in {
            "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
            "index.json": index,
            "blobs/sha256/" + "c" * 64: config,
            "blobs/sha256/" + "d" * 64: manifest,
        }.items():
            member = tarfile.TarInfo(name)
            member.size = len(body)
            archive.addfile(member, io.BytesIO(body))


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, listing: list[dict[str, Any]] | None = None):
        self.objects = objects
        self.listing = listing
        self.gets: list[str] = []
        self.heads: list[str] = []
        self.deletes: list[str] = []
        self.bodies: list[io.BytesIO] = []
        self.uploads: list[tuple[str, str, bytes]] = []
        self.closed = False
        self._now = __import__("datetime").datetime.now(__import__("datetime").UTC)

    def get_paginator(self, name: str) -> FakeS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        prefix = kwargs["Prefix"]
        if self.listing is not None:
            contents = [row for row in self.listing if row["Key"].startswith(prefix)]
        else:
            contents = [
                {
                    "Key": key,
                    "Size": len(body),
                    "LastModified": self._now,
                }
                for key, body in self.objects.items()
                if key.startswith(prefix)
            ]
        yield {"Contents": contents}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        self.gets.append(key)
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = io.BytesIO(self.objects[key])
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(self.objects[key])}

    def head_object(self, **kwargs):
        key = kwargs["Key"]
        self.heads.append(key)
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[key])}

    def delete_object(self, **kwargs):
        key = kwargs["Key"]
        self.deletes.append(key)
        self.objects.pop(key, None)

    def close(self) -> None:
        self.closed = True

    def put_object(self, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and kwargs["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        body = Path(filename).read_bytes()
        self.objects[key] = body
        self.uploads.append((bucket, key, body))


@pytest.fixture
def source_bundle(tmp_path):
    original = tmp_path / "original"
    (original / "environment").mkdir(parents=True)
    (original / "environment/Dockerfile").write_text("FROM scratch\n")
    (original / "instruction.md").write_text("Run the task.\n")
    (original / "run.sh").write_text("#!/bin/sh\nexit 0\n")
    (original / "run.sh").chmod(0o755)
    for name in ("environment/Dockerfile", "instruction.md"):
        (original / name).chmod(0o644)
    claim = {
        "id": str(uuid4()),
        "task_checksum": sha256_of_dir(original),
        "materialization_key": "a" * 64,
        "task_source": "s3://source/tasks/revision/",
        "source_bucket": "source",
        "cache_bucket": "cache",
        "task_source_provenance": {
            "bundle_file_metadata_sha256": bundle_file_metadata_sha256(original)
        },
        "registry_repository": "registry.example/tasks",
        "lease_epoch": 3,
        "task_config": {
            "schema_version": "1",
            "task": {"id": "test", "name": "test"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "dockerfile": "environment/Dockerfile",
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
        },
    }
    objects = {
        "tasks/revision/" + path.relative_to(original).as_posix(): path.read_bytes()
        for path in original.rglob("*")
        if path.is_file()
    }
    objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME] = bundle_file_metadata_body(original)
    return claim, FakeS3(objects)


def test_download_restores_verified_source_bytes_and_executable_modes(
    source_bundle, tmp_path
) -> None:
    claim, source = source_bundle
    destination = tmp_path / "download"
    runtime.download_bundle(claim, source, destination)
    assert sha256_of_dir(destination) == claim["task_checksum"]
    assert (destination / "instruction.md").read_text() == "Run the task.\n"
    assert stat.S_IMODE((destination / "run.sh").stat().st_mode) == 0o755
    assert stat.S_IMODE((destination / "instruction.md").stat().st_mode) == 0o644
    assert not (destination / BUNDLE_FILE_METADATA_NAME).exists()
    assert source.bodies and all(body.closed for body in source.bodies)


def test_download_ordinary_uploaded_taskset_without_benchmark_sidecar(source_bundle, tmp_path):
    claim, source = source_bundle
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "native-input", "display_name": "Native input"},
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz", "subset": "tasks"},
    })
    buffer = io.BytesIO()
    files = {
        "task.toml": tomli_w.dumps(claim["task_config"]).encode(),
        "instruction.md": b"Run the task.\n",
        "environment/Dockerfile": b"FROM scratch\n",
    }
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, body in files.items():
            member = tarfile.TarInfo("tasks/test/" + name)
            member.size = len(body)
            member.mode = 0o644
            archive.addfile(member, io.BytesIO(body))
    source.objects = {
        bundle_object_key(
            prefix=taskset_root(team_id="team-test", slug=manifest.slug).removesuffix("/"),
            relative_path=manifest.source.locator,
        ): buffer.getvalue()
    }
    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team-test/native-input", owning_team_id="team-test",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None, minio_client=source,
        artifacts_bucket="source", upstream_cache_root=tmp_path / "upstream",
    )
    assert result.status == "ready", result.error_summary
    row, = result.task_rows
    assert "service_execution_input" in row.source_provenance
    assert not any(key.endswith(BUNDLE_FILE_METADATA_NAME) for key in source.objects)
    claim.update(task_source=row.source, task_checksum=row.checksum,
                 task_config=row.config, task_source_provenance=row.source_provenance)
    runtime.download_bundle(claim, source, tmp_path / "download")
    assert sha256_of_dir(tmp_path / "download") == row.checksum
    assert (tmp_path / "download/environment/Dockerfile").read_bytes() == b"FROM scratch\n"


@pytest.fixture
def input_manifest_bundle(source_bundle, tmp_path):
    claim, source = source_bundle
    del source.objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME]
    provenance, _ = _publish_service_execution_input_manifest(
        source, bucket="source", manifest_key="task-inputs/revision.json",
        bundle_dir=tmp_path / "original", task_checksum_value=claim["task_checksum"],
    )
    claim["task_source_provenance"].update(provenance)
    return claim, source


def test_input_manifest_restores_frozen_executable_modes(input_manifest_bundle, tmp_path):
    claim, source = input_manifest_bundle
    runtime.download_bundle(claim, source, tmp_path / "download")
    assert stat.S_IMODE((tmp_path / "download/run.sh").stat().st_mode) == 0o755
    assert stat.S_IMODE((tmp_path / "download/instruction.md").stat().st_mode) == 0o644
    assert not any(key.endswith(BUNDLE_FILE_METADATA_NAME) for key in source.gets)
    assert all(body.closed for body in source.bodies)


@pytest.mark.parametrize("change,match", [
    ("manifest_bytes", "frozen binding"),
    ("content", "content does not match"),
    ("missing_file", "frozen bundle"),
    ("extra_file", "frozen bundle"),
    ("revision", "frozen bundle"),
    ("count", "frozen bundle"),
    ("total", "frozen bundle"),
    ("unsafe_mode", "Input should be"),
    ("mode", "file modes do not match"),
    ("noncanonical", "frozen bundle"),
    ("outside_bucket", "outside the configured source bucket"),
])
def test_input_manifest_rejects_frozen_input_drift(input_manifest_bundle, tmp_path, change, match):
    claim, source = input_manifest_bundle
    key = "task-inputs/revision.json"
    binding = claim["task_source_provenance"]["service_execution_input"]
    if change == "manifest_bytes":
        source.objects[key] += b" "
    elif change == "content":
        source.objects["tasks/revision/instruction.md"] = b"changed bytes"
    elif change == "missing_file":
        del source.objects["tasks/revision/run.sh"]
    elif change == "extra_file":
        source.objects["tasks/revision/extra"] = b"unlisted"
    elif change == "outside_bucket":
        binding["manifest_uri"] = "s3://other/task-inputs/revision.json"
    elif change in {"count", "total"}:
        binding["file_count" if change == "count" else "total_bytes"] += 1
    else:
        manifest = json.loads(source.objects[key])
        if change == "revision":
            manifest["task_revision_sha256"] = "sha256:" + "0" * 64
        elif change in {"mode", "unsafe_mode"}:
            next(row for row in manifest["files"] if row["relative_path"] == "run.sh")[
                "mode"
            ] = "0644" if change == "mode" else "4755"
        body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        if change == "noncanonical":
            body += b" "
        source.objects[key] = body
        binding["manifest_sha256"] = "sha256:" + hashlib.sha256(body).hexdigest()
    with pytest.raises(ValueError, match=match):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert all(body.closed for body in source.bodies)


def test_missing_bound_manifest_does_not_fall_back_to_sidecar(input_manifest_bundle, tmp_path):
    claim, source = input_manifest_bundle
    del source.objects["task-inputs/revision.json"]
    source.objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME] = bundle_file_metadata_body(
        tmp_path / "original"
    )
    with pytest.raises(ClientError, match="NoSuchKey"):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == ["task-inputs/revision.json"]


@pytest.mark.parametrize(
    "change,match",
    [
        ("content", "content does not match"),
        ("mode", "file modes do not match"),
        ("unsafe_mode", "unsafe file mode"),
        ("missing_path", "paths do not exactly match"),
    ],
)
def test_download_rejects_source_or_metadata_drift(source_bundle, tmp_path, change, match) -> None:
    claim, source = source_bundle
    metadata_key = "tasks/revision/" + BUNDLE_FILE_METADATA_NAME
    if change == "content":
        source.objects["tasks/revision/instruction.md"] = b"changed source"
    else:
        metadata = json.loads(source.objects[metadata_key])
        if change == "missing_path":
            del metadata["files"]["run.sh"]
        else:
            metadata["files"]["run.sh"]["mode"] = "0644" if change == "mode" else "4755"
        source.objects[metadata_key] = json.dumps(metadata).encode()
    with pytest.raises(ValueError, match=match):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert all(body.closed for body in source.bodies)


@pytest.mark.parametrize(
    "relative", ["../outside", "/absolute", "nested/../../outside", "a\\outside"]
)
def test_download_rejects_nonrelative_listing_before_writing(
    source_bundle, tmp_path, relative
) -> None:
    claim, source = source_bundle
    source.listing = [{"Key": "tasks/revision/" + relative, "Size": 1}]
    with pytest.raises(ValueError):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == []
    assert not (tmp_path / "download").exists()


@pytest.mark.parametrize("limit", ["files", "bytes"])
def test_download_rejects_listing_limits_before_getting_objects(
    source_bundle, tmp_path, monkeypatch, limit
) -> None:
    claim, source = source_bundle
    monkeypatch.setattr(runtime, "_FILE_LIMIT" if limit == "files" else "_BUNDLE_BYTES", 1)
    with pytest.raises(runtime.BuildPreparationError, match="limits"):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == []


@pytest.mark.parametrize("reported", [0, 6])
def test_download_bounds_stream_even_when_content_length_underreports(tmp_path, reported) -> None:
    body = io.BytesIO(b"123456")

    class Client:
        def get_object(self, **_kwargs):
            return {"Body": body, "ContentLength": reported}

    with pytest.raises(runtime.BuildPreparationError, match="byte limit"):
        runtime._download(
            Client(), bucket="source", key="key", destination=tmp_path / "file", limit=4
        )
    assert body.closed
    assert not (tmp_path / "file").exists() or (tmp_path / "file").stat().st_size <= 4


def test_prepare_accepts_missing_cache_and_closes_both_clients(
    source_bundle, tmp_path, monkeypatch
) -> None:
    claim, source = source_bundle
    cache = FakeS3({})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    work = tmp_path / "work"
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "context/run.sh").is_file() and (work / "oci").is_dir()
    assert cache.gets == [
        f"task-build-cache/v2/{claim['materialization_key']}/0/manifest.json",
        "task-build-cache/" + claim["materialization_key"] + "/0.tar",
    ]
    assert source.closed and cache.closed


def test_prepare_without_cache_secret_never_creates_cache_client(
    source_bundle, tmp_path, monkeypatch
) -> None:
    claim, source = source_bundle
    clients = []

    def client(_claim, secret):
        clients.append(secret.name)
        assert secret.name == "source"
        return source

    monkeypatch.setattr(runtime, "_client", client)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    assert clients == ["source"] and source.closed
    assert (tmp_path / "work/oci").is_dir()


def test_invalid_disposable_cache_becomes_a_cold_build(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    cache = FakeS3(
        {"task-build-cache/" + claim["materialization_key"] + "/0.tar": b"broken archive"}
    )
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    assert (tmp_path / "work/context/run.sh").is_file()
    assert not (tmp_path / "work/cache-in/0").exists()
    events = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    assert any(row.get("reason") == "invalid_archive" for row in events)
    assert any(
        row.get("loom_task_image_stage") == "cache_import" and row.get("event") == "miss"
        for row in events
    )
    assert any(
        row.get("loom_task_image_stage") == "prepare" and row.get("event") == "end"
        for row in events
    )
    assert cache.closed and source.closed


def test_prepare_emits_stage_timing_for_absent_cache(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    cache = FakeS3({})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    events = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    stages = [(row["loom_task_image_stage"], row["event"]) for row in events]
    assert ("prepare", "start") in stages and ("prepare", "end") in stages
    assert ("cache_import", "start") in stages and ("cache_import", "miss") in stages
    assert any(
        row.get("loom_task_image_stage") == "prepare"
        and row.get("event") == "end"
        and "duration_ms" in row
        for row in events
    )


def test_prepare_imports_compatible_revision_cache_after_exact_miss(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    donor = "c" * 64
    assert donor != claim["materialization_key"]
    claim = {**claim, "cache_import_materialization_key": donor}
    cache_root = tmp_path / "donor-cache"
    cache_root.mkdir()
    (cache_root / "index.json").write_bytes(b'{"schemaVersion":2}')
    archive = tmp_path / "donor.tar"
    runtime.pack_cache(cache_root, archive)
    cache = FakeS3({f"task-build-cache/{donor}/0.tar": archive.read_bytes()})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    work = tmp_path / "work"
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").is_file()
    assert cache.gets == [
        f"task-build-cache/v2/{claim['materialization_key']}/0/manifest.json",
        f"task-build-cache/{claim['materialization_key']}/0.tar",
        f"task-build-cache/v2/{donor}/0/manifest.json",
        f"task-build-cache/{donor}/0.tar",
    ]
    events = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    assert any(
        row.get("loom_task_image_stage") == "cache_import"
        and row.get("event") == "miss"
        and row.get("source") == "exact"
        for row in events
    )
    assert any(
        row.get("loom_task_image_stage") == "cache_import"
        and row.get("event") == "hit"
        and row.get("source") == "compatible"
        for row in events
    )


def test_load_claim_rejects_invalid_compatible_cache_hint(source_bundle, tmp_path) -> None:
    claim, _ = source_bundle
    claim = {**claim, "cache_import_materialization_key": "not-a-key"}
    path = tmp_path / "claim.json"
    path.write_text(json.dumps(claim))
    with pytest.raises(runtime.BuildPreparationError, match="compatible cache"):
        runtime.load_claim(path)


def test_cache_round_trip_preserves_nested_regular_files(tmp_path) -> None:
    cache = tmp_path / "cache"
    (cache / "blobs/sha256").mkdir(parents=True)
    (cache / "index.json").write_bytes(b'{"schemaVersion":2}')
    (cache / "blobs/sha256/layer").write_bytes(b"cache layer")
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    restored = tmp_path / "restored"
    runtime.unpack_cache(archive, restored)
    assert (restored / "index.json").read_bytes() == (cache / "index.json").read_bytes()
    assert (restored / "blobs/sha256/layer").read_bytes() == b"cache layer"


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "traversal", "absolute", "device"])
def test_unpack_cache_rejects_links_devices_and_escaping_paths(tmp_path, kind) -> None:
    archive = tmp_path / "cache.tar"
    member = tarfile.TarInfo({"traversal": "../outside", "absolute": "/outside"}.get(kind, "entry"))
    if kind in {"symlink", "hardlink"}:
        member.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
        member.linkname = "../credential"
    elif kind == "device":
        member.type = tarfile.CHRTYPE
    with tarfile.open(archive, "w:") as output:
        output.addfile(member)
    with pytest.raises(ValueError):
        runtime.unpack_cache(archive, tmp_path / "restored")
    assert not (tmp_path / "outside").exists()


@pytest.mark.parametrize("kind", ["root_symlink", "file_symlink", "directory_symlink"])
def test_pack_cache_cannot_archive_linked_credentials(tmp_path, kind) -> None:
    secret = tmp_path / "credential"
    secret.write_text("fake credential")
    cache = tmp_path / "cache"
    if kind == "root_symlink":
        cache.symlink_to(tmp_path, target_is_directory=True)
    else:
        cache.mkdir()
        target = cache / "entry"
        target.symlink_to(
            tmp_path if kind == "directory_symlink" else secret,
            target_is_directory=kind == "directory_symlink",
        )
    with pytest.raises(runtime.BuildPreparationError):
        runtime.pack_cache(cache, tmp_path / "cache.tar")


def test_pack_cache_normalizes_internal_hardlinks_to_regular_entries(tmp_path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "first").write_bytes(b"shared cache layer")
    os.link(cache / "first", cache / "second")
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    with tarfile.open(archive) as packed:
        assert all(member.isfile() and not member.islnk() for member in packed)
    runtime.unpack_cache(archive, tmp_path / "restored")
    assert (
        (tmp_path / "restored/first").read_bytes()
        == (tmp_path / "restored/second").read_bytes()
        == b"shared cache layer"
    )


def test_cache_size_limits_apply_to_pack_and_unpack(tmp_path, monkeypatch) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "blob").write_bytes(b"12345")
    archive = tmp_path / "valid.tar"
    runtime.pack_cache(cache, archive)
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 4)
    with pytest.raises(runtime.BuildPreparationError):
        runtime.pack_cache(cache, tmp_path / "oversized.tar")
    with pytest.raises(runtime.BuildPreparationError):
        runtime.unpack_cache(archive, tmp_path / "restored")


@pytest.mark.parametrize("path", ["oci", "oci/0000.tar", "cache-out", "cache-out/0"])
def test_publisher_output_path_rejects_task_created_credential_symlinks(tmp_path, path) -> None:
    work, secrets = tmp_path / "work", tmp_path / "secrets"
    work.mkdir()
    secrets.mkdir()
    (secrets / "config.json").write_text("fake credential")
    linked = work / path
    linked.parent.mkdir(parents=True, exist_ok=True)
    linked.symlink_to(secrets, target_is_directory=True)
    relative = "oci/0000.tar" if path.startswith("oci") else "cache-out/0"
    with pytest.raises(runtime.BuildPreparationError, match="link"):
        runtime._output_path(work, relative, directory=path.startswith("cache-out"))


@pytest.mark.parametrize("platform", ["linux/arm64", "windows/amd64", ""])
def test_publisher_rejects_wrong_architecture_before_copy_or_receipt(
    source_bundle, tmp_path, monkeypatch, platform
) -> None:
    claim, _ = source_bundle
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar", platform=platform)
    (tmp_path / "secrets/cache").mkdir(parents=True)
    cache = FakeS3({})
    calls = []
    monkeypatch.setattr(runtime, "_client", lambda *_args: cache)

    def run(argv, **kwargs):
        calls.append(argv)
        raise AssertionError("invalid OCI configuration must fail before Skopeo")

    monkeypatch.setattr(runtime.subprocess, "run", run)
    with pytest.raises(runtime.NativeOCIArchiveError, match="linux/amd64"):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
    assert calls == [] and cache.closed
    assert not (tmp_path / "receipt.json").exists()


@pytest.fixture
def publisher(monkeypatch, tmp_path):
    (tmp_path / "secrets/cache").mkdir(parents=True)
    cache = FakeS3({})
    calls = []
    monkeypatch.setattr(runtime, "_client", lambda *_args: cache)

    def run(argv, **kwargs):
        calls.append(argv)
        assert argv[:2] == ["skopeo", "--tmpdir"] and argv[3] == "copy"
        assert Path(argv[2]).is_dir() and "--preserve-digests" in argv
        Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + "b" * 64)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(runtime.subprocess, "run", run)
    return cache, calls


@pytest.mark.parametrize("cache_enabled", [True, False])
def test_publisher_records_registry_digest_with_optional_cache(
    source_bundle, tmp_path, publisher, cache_enabled
) -> None:
    claim, _ = source_bundle
    cache, calls = publisher
    if not cache_enabled:
        (tmp_path / "secrets/cache").rmdir()
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    (work / "cache-out/0/blobs").mkdir(parents=True)
    (work / "cache-out/0/blobs/layer").write_bytes(b"cached layer")
    receipt_file = tmp_path / "receipt.json"
    result = runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_file)
    assert (
        result
        == json.loads(receipt_file.read_text())
        == {
            "materialization_id": claim["id"],
            "lease_epoch": 3,
            "registry_images": {"task": "registry.example/tasks@sha256:" + "b" * 64},
        }
    )
    assert [call[3] for call in calls] == ["copy"]
    if not cache_enabled:
        assert cache.uploads == [] and not cache.closed
        return
    # Default cache_transfer=blobs: content-addressed upload under current mat key.
    assert cache.closed
    manifest_key = f"task-build-cache/v2/{claim['materialization_key']}/0/manifest.json"
    assert manifest_key in cache.objects
    manifest = json.loads(cache.objects[manifest_key])
    assert manifest["version"] == 1
    assert {row["path"] for row in manifest["files"]} == {"blobs/layer"}
    digest = manifest["files"][0]["sha256"]
    assert cache.objects[f"task-build-cache/v2/blobs/{digest}"] == b"cached layer"
    assert any(key.endswith(f"/blobs/{digest}") for _, key, _ in cache.uploads)


@pytest.mark.parametrize("linked_directory", ["oci", "cache-out"])
def test_publisher_rejects_symlinked_output_ancestors_before_reading_or_uploading_secrets(
    source_bundle,
    tmp_path,
    publisher,
    linked_directory,
) -> None:
    claim, _ = source_bundle
    cache, calls = publisher
    work, secret = tmp_path / "work", tmp_path / "secrets/registry"
    work.mkdir()
    (secret / "0").mkdir(parents=True)
    (secret / "0000.tar").write_bytes(b"fake credential")
    (secret / "0/config.json").write_bytes(b"fake credential")
    (work / linked_directory).symlink_to(secret, target_is_directory=True)
    if linked_directory == "cache-out":
        (work / "oci").mkdir()
        _write_native_oci(work / "oci/0000.tar")
    receipt_file = tmp_path / "receipt.json"
    with pytest.raises(runtime.BuildPreparationError, match="link"):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_file)
    assert cache.uploads == [] and cache.closed
    if linked_directory == "oci":
        assert calls == [] and not receipt_file.exists()
    else:
        # Keep publication evidence even when a later cache step is rejected.
        assert json.loads(receipt_file.read_text())["registry_images"]["task"].endswith("b" * 64)


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_source", "s3://another-bucket/tasks/revision/"),
        ("task_source", "s3://source/tasks/../outside/"),
        ("task_source", "s3://source/tasks/revision"),
        ("materialization_key", "not-an-identity"),
        ("registry_repository", "registry.example/tasks;injected"),
    ],
)
def test_claim_rejects_out_of_scope_source_or_invalid_identity(
    source_bundle, tmp_path, field, value
) -> None:
    claim, _ = source_bundle
    claim[field] = value
    path = tmp_path / "claim.json"
    path.write_text(json.dumps(claim))
    with pytest.raises(ValueError):
        runtime.load_claim(path)


def test_native_publisher_mints_auth_outside_task_volume_and_removes_it(
    source_bundle, tmp_path, publisher, monkeypatch
) -> None:
    from loom import nebius_registry_auth

    claim, _ = source_bundle
    _, calls = publisher
    claim["registry_repository"] = "cr.eu-north1.nebius.cloud/test/task-images"
    secrets = tmp_path / "secrets"
    (secrets / "registry").mkdir()
    credentials = secrets / "registry/credentials.json"
    credentials.write_text("trusted-key-fixture")
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    minted = []

    def mint(path, prefix, auth_file):
        assert path == credentials and prefix == "cr.eu-north1.nebius.cloud/test"
        assert not auth_file.is_relative_to(work)
        auth_file.write_text("temporary-auth-fixture")
        minted.append(auth_file)
        return {}

    monkeypatch.setattr(nebius_registry_auth, "mint_registry_auth", mint)
    runtime.publish(claim, work, secrets, receipt_path=tmp_path / "receipt.json")
    assert len(minted) == 1
    assert calls[0][calls[0].index("--authfile") + 1] == str(minted[0])
    assert not minted[0].exists()


def _v2_cache_objects(materialization_key: str, index: int, files: dict[str, bytes]) -> dict[str, bytes]:
    entries = []
    objects: dict[str, bytes] = {}
    for path, body in sorted(files.items()):
        digest = hashlib.sha256(body).hexdigest()
        entries.append({"path": path, "sha256": digest, "size": len(body)})
        objects[f"task-build-cache/v2/blobs/{digest}"] = body
    manifest = json.dumps({"version": 1, "files": entries}, separators=(",", ":"), sort_keys=True).encode()
    objects[f"task-build-cache/v2/{materialization_key}/{index}/manifest.json"] = manifest
    return objects


def test_prepare_imports_v2_blobs_and_skips_legacy_tar(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    objects = _v2_cache_objects(
        claim["materialization_key"], 0, {"index.json": b'{"schemaVersion":2}'}
    )
    cache = FakeS3(objects)
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    work = tmp_path / "work"
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").read_bytes() == b'{"schemaVersion":2}'
    assert cache.gets[0].endswith("/manifest.json")
    assert not any(key.endswith(".tar") for key in cache.gets)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert any(
        row.get("event") == "hit" and row.get("format") == "blobs" for row in events
    )


def test_prepare_dual_reads_legacy_tar_after_blob_miss(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    cache_root = tmp_path / "legacy"
    cache_root.mkdir()
    (cache_root / "index.json").write_bytes(b'{"schemaVersion":2}')
    archive = tmp_path / "legacy.tar"
    runtime.pack_cache(cache_root, archive)
    cache = FakeS3({f"task-build-cache/{claim['materialization_key']}/0.tar": archive.read_bytes()})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    work = tmp_path / "work"
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").is_file()
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert any(row.get("format") == "blobs" and row.get("event") == "miss" for row in events)
    assert any(row.get("format") == "tar" and row.get("event") == "hit" for row in events)


def test_prepare_tar_mode_skips_blob_path(source_bundle, tmp_path, monkeypatch) -> None:
    claim, source = source_bundle
    claim = {**claim, "cache_transfer": "tar"}
    cache_root = tmp_path / "legacy"
    cache_root.mkdir()
    (cache_root / "index.json").write_bytes(b'{"schemaVersion":2}')
    archive = tmp_path / "legacy.tar"
    runtime.pack_cache(cache_root, archive)
    cache = FakeS3({f"task-build-cache/{claim['materialization_key']}/0.tar": archive.read_bytes()})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    assert cache.gets == [f"task-build-cache/{claim['materialization_key']}/0.tar"]


def test_prepare_invalid_blob_digest_falls_back_to_tar(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    digest = "a" * 64
    manifest = json.dumps(
        {"version": 1, "files": [{"path": "index.json", "sha256": digest, "size": 4}]},
        separators=(",", ":"),
    ).encode()
    cache = FakeS3(
        {
            f"task-build-cache/v2/{claim['materialization_key']}/0/manifest.json": manifest,
            f"task-build-cache/v2/blobs/{digest}": b"nope",
        }
    )
    cache_root = tmp_path / "legacy"
    cache_root.mkdir()
    (cache_root / "index.json").write_bytes(b'{"ok":true}')
    archive = tmp_path / "legacy.tar"
    runtime.pack_cache(cache_root, archive)
    cache.objects[f"task-build-cache/{claim['materialization_key']}/0.tar"] = archive.read_bytes()
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    work = tmp_path / "work"
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").read_bytes() == b'{"ok":true}'
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert any(row.get("reason") == "invalid_blobs" for row in events)


def test_prepare_compatible_revision_hits_donor_blobs(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    donor = "c" * 64
    claim = {**claim, "cache_import_materialization_key": donor}
    objects = _v2_cache_objects(donor, 0, {"index.json": b'{"schemaVersion":2}'})
    cache = FakeS3(objects)
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    work = tmp_path / "work"
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "cache-in/0/index.json").is_file()
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert any(
        row.get("source") == "compatible" and row.get("format") == "blobs" and row.get("event") == "hit"
        for row in events
    )


def test_publish_blobs_skips_existing_digest(source_bundle, tmp_path, publisher) -> None:
    claim, _ = source_bundle
    cache, _calls = publisher
    body = b"cached layer"
    digest = hashlib.sha256(body).hexdigest()
    cache.objects[f"task-build-cache/v2/blobs/{digest}"] = body
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    (work / "cache-out/0").mkdir(parents=True)
    (work / "cache-out/0/blobs").mkdir()
    (work / "cache-out/0/blobs/layer").write_bytes(body)
    runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
    assert cache.uploads == []
    manifest_key = f"task-build-cache/v2/{claim['materialization_key']}/0/manifest.json"
    assert manifest_key in cache.objects


def test_publish_tar_mode_writes_legacy_archive(source_bundle, tmp_path, publisher) -> None:
    claim, _ = source_bundle
    claim = {**claim, "cache_transfer": "tar"}
    cache, _calls = publisher
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    (work / "cache-out/0/blobs").mkdir(parents=True)
    (work / "cache-out/0/blobs/layer").write_bytes(b"cached layer")
    runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
    assert len(cache.uploads) == 1
    assert cache.uploads[0][1] == f"task-build-cache/{claim['materialization_key']}/0.tar"


def test_trim_cache_deletes_unreferenced_old_blob(tmp_path) -> None:
    from datetime import UTC, datetime, timedelta

    old = datetime.now(UTC) - timedelta(days=10)
    recent = datetime.now(UTC) - timedelta(days=1)
    orphan = "f" * 64
    live = "e" * 64
    manifest = json.dumps(
        {"version": 1, "files": [{"path": "index.json", "sha256": live, "size": 2}]},
        separators=(",", ":"),
    ).encode()
    objects = {
        f"task-build-cache/v2/blobs/{orphan}": b"xx",
        f"task-build-cache/v2/blobs/{live}": b"ok",
        f"task-build-cache/v2/{'a' * 64}/0/manifest.json": manifest,
    }
    listing = [
        {"Key": f"task-build-cache/v2/blobs/{orphan}", "Size": 2, "LastModified": old},
        {"Key": f"task-build-cache/v2/blobs/{live}", "Size": 2, "LastModified": old},
        {
            "Key": f"task-build-cache/v2/{'a' * 64}/0/manifest.json",
            "Size": len(manifest),
            "LastModified": recent,
        },
    ]
    cache = FakeS3(objects, listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert f"task-build-cache/v2/blobs/{orphan}" not in cache.objects
    assert f"task-build-cache/v2/blobs/{live}" in cache.objects
    assert f"task-build-cache/v2/{'a' * 64}/0/manifest.json" in cache.objects


def _digest(marker: str) -> str:
    return (marker * 64)[:64]


def _manifest_body(*digests: str) -> bytes:
    return json.dumps(
        {
            "version": 1,
            "files": [
                {"path": f"{digest[:8]}.bin", "sha256": digest, "size": 1}
                for digest in digests
            ],
        },
        separators=(",", ":"),
    ).encode()


def _cache_moment(*, days: int):
    from datetime import UTC, datetime, timedelta

    return datetime.now(UTC) - timedelta(days=days)


def _listed(key: str, size: int, when) -> dict[str, Any]:
    return {"Key": key, "Size": size, "LastModified": when}


def _manifest_key(marker: str, index: int = 0) -> str:
    return f"task-build-cache/v2/{_digest(marker)}/{index}/manifest.json"


def _blob_key(digest: str) -> str:
    return f"task-build-cache/v2/blobs/{digest}"


class _UnreadableGet(FakeS3):
    def get_object(self, **kwargs):
        raise ClientError(
            {"Error": {"Code": "InternalError", "Message": "secret sdk detail"}},
            "GetObject",
        )


class _BrokenStream(FakeS3):
    def get_object(self, **kwargs):
        result = super().get_object(**kwargs)
        stream = result["Body"]

        def read(_size=-1):
            raise OSError("broken stream")

        stream.read = read
        return result


def _live_manifest_cache(body: bytes) -> tuple[FakeS3, str, str, str]:
    live = _digest("e")
    orphan = _digest("f")
    manifest_key = _manifest_key("a")
    objects = {
        _blob_key(live): b"ok",
        _blob_key(orphan): b"xx",
        manifest_key: body,
    }
    listing = [
        _listed(_blob_key(orphan), 2, _cache_moment(days=10)),
        _listed(_blob_key(live), 2, _cache_moment(days=10)),
        _listed(manifest_key, len(body), _cache_moment(days=1)),
    ]
    return FakeS3(objects, listing=listing), manifest_key, live, orphan


@pytest.mark.parametrize("kind", ["get", "stream", "malformed"])
def test_unreadable_manifest_does_not_delete_referenced_blobs(kind, capsys) -> None:
    body = b"not-json" if kind == "malformed" else _manifest_body(_digest("e"))
    cache, manifest_key, live, orphan = _live_manifest_cache(body)
    if kind == "get":
        cache = _UnreadableGet(cache.objects, listing=cache.listing)
    elif kind == "stream":
        cache = _BrokenStream(cache.objects, listing=cache.listing)
    runtime.trim_cache(cache, "cache", 0)
    logged = capsys.readouterr().out
    assert "manifest_unreadable" in logged
    assert "secret sdk detail" not in logged
    assert "broken stream" not in logged
    assert _blob_key(live) in cache.objects
    assert _blob_key(orphan) in cache.objects
    assert manifest_key in cache.objects
    assert cache.deletes == []
    if kind != "get":
        assert cache.bodies[0].closed
    cache.objects[manifest_key] = _manifest_body(live)
    if kind == "get":
        cache.get_object = FakeS3.get_object.__get__(cache, FakeS3)
    elif kind == "stream":
        cache.get_object = FakeS3.get_object.__get__(cache, FakeS3)
    runtime.trim_cache(cache, "cache", 0)
    assert _blob_key(orphan) not in cache.objects
    assert _blob_key(live) in cache.objects


def test_over_budget_evicts_one_oldest_manifest_group() -> None:
    mebibyte = 1024 * 1024
    blob_size = 900 * mebibyte
    objects: dict[str, bytes] = {}
    listing = []
    oldest_manifest = _manifest_key("1")
    oldest_blob = _blob_key(_digest("1"))
    for slot in range(1, 6):
        digest = _digest(str(slot))
        manifest_key = _manifest_key(str(slot))
        body = _manifest_body(digest)
        objects[manifest_key] = body
        objects[_blob_key(digest)] = b"x"
        listing.append(_listed(manifest_key, len(body), _cache_moment(days=13 - slot)))
        listing.append(_listed(_blob_key(digest), blob_size, _cache_moment(days=10)))
    cache = FakeS3(objects, listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert oldest_manifest not in cache.objects
    assert oldest_blob not in cache.objects
    for slot in range(2, 6):
        assert _manifest_key(str(slot)) in cache.objects
        assert _blob_key(_digest(str(slot))) in cache.objects


def test_orphan_is_removed_before_grace_protected_manifests(capsys) -> None:
    mebibyte = 1024 * 1024
    orphan = _digest("9")
    objects: dict[str, bytes] = {_blob_key(orphan): b"x"}
    listing = [_listed(_blob_key(orphan), 900 * mebibyte, _cache_moment(days=10))]
    for slot in range(1, 6):
        digest = _digest(str(slot))
        manifest_key = _manifest_key(str(slot))
        body = _manifest_body(digest)
        objects[manifest_key] = body
        objects[_blob_key(digest)] = b"x"
        listing.append(_listed(manifest_key, len(body), _cache_moment(days=1)))
        listing.append(_listed(_blob_key(digest), 700 * mebibyte, _cache_moment(days=10)))
    cache = FakeS3(objects, listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert cache.deletes == [_blob_key(orphan)]
    assert "grace_protected" not in capsys.readouterr().out
    for slot in range(1, 6):
        assert _manifest_key(str(slot)) in cache.objects
        assert _blob_key(_digest(str(slot))) in cache.objects


def test_shared_blob_is_reclaimed_only_with_its_last_manifest() -> None:
    mebibyte = 1024 * 1024
    shared = _digest("a")
    older_only = _digest("b")
    newer_only = _digest("c")
    older = _manifest_key("1")
    newer = _manifest_key("2")
    objects = {
        older: _manifest_body(shared, older_only),
        newer: _manifest_body(shared, newer_only),
        _blob_key(shared): b"s",
        _blob_key(older_only): b"a",
        _blob_key(newer_only): b"b",
    }
    listing = [
        _listed(older, len(objects[older]), _cache_moment(days=12)),
        _listed(newer, len(objects[newer]), _cache_moment(days=11)),
        _listed(_blob_key(shared), 2500 * mebibyte, _cache_moment(days=10)),
        _listed(_blob_key(older_only), 2000 * mebibyte, _cache_moment(days=10)),
        _listed(_blob_key(newer_only), 2000 * mebibyte, _cache_moment(days=10)),
    ]
    cache = FakeS3(objects, listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert cache.deletes.index(_blob_key(older_only)) < cache.deletes.index(newer)
    assert cache.deletes.index(_blob_key(shared)) > cache.deletes.index(newer)
    assert _blob_key(shared) not in cache.objects
    assert newer not in cache.objects
    assert older not in cache.objects


def test_legacy_tar_is_evicted_as_one_object() -> None:
    key = f"task-build-cache/{_digest('c')}/0.tar"
    cache = FakeS3(
        {key: b"tar"},
        listing=[_listed(key, 5 * 1024 * 1024 * 1024, _cache_moment(days=10))],
    )
    runtime.trim_cache(cache, "cache", 0)
    assert cache.deletes == [key]


def test_recent_orphan_blob_is_kept_and_reported_when_over_budget(capsys) -> None:
    key = _blob_key(_digest("d"))
    cache = FakeS3(
        {key: b"new"},
        listing=[_listed(key, 5 * 1024 * 1024 * 1024, _cache_moment(days=1))],
    )
    runtime.trim_cache(cache, "cache", 0)
    assert key in cache.objects
    assert cache.deletes == []
    logged = capsys.readouterr().out
    assert "grace_protected" in logged
    assert "InternalError" not in logged
@pytest.mark.parametrize("orphan_mib,layer_mib,expected_roots", [(0, 900, 4), (900, 700, 5)])
def test_trim_budget_counts_actual_reclaimable_blobs(orphan_mib, layer_mib, expected_roots):
    from datetime import UTC, datetime, timedelta
    now = datetime.now(UTC)
    objects = {}
    listing = []
    for i in range(5):
        content = f"layer-{i}".encode()
        part = _v2_cache_objects(f"{i:064x}", 0, {"layer": content})
        objects.update(part)
        for key, value in part.items():
            blob = "/blobs/" in key
            listing.append({"Key": key, "Size": layer_mib * 1024**2 if blob else len(value),
                            "LastModified": now - timedelta(days=10 if blob else 1, seconds=5-i)})
    if orphan_mib:
        key = runtime._v2_blob_key("f" * 64)
        objects[key] = b"orphan"
        listing.append({"Key": key, "Size": orphan_mib * 1024**2, "LastModified": now-timedelta(days=10)})
    cache = FakeS3(objects, listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert sum(k.endswith("manifest.json") for k in cache.objects) == expected_roots
    assert sum("/blobs/" in k for k in cache.objects) == expected_roots


def test_trim_does_not_discard_roots_when_grace_prevents_reclaim(capsys):
    from datetime import UTC, datetime, timedelta
    now = datetime.now(UTC)
    objects = _v2_cache_objects("a" * 64, 0, {"layer": b"large"})
    listing = [{"Key": key, "Size": 5 * 1024**3 if "/blobs/" in key else len(value),
                "LastModified": now-timedelta(days=1)} for key,value in objects.items()]
    cache = FakeS3(dict(objects), listing=listing)
    runtime.trim_cache(cache, "cache", 0)
    assert cache.objects == objects
    assert "budget_temporarily_unreachable" in capsys.readouterr().out


def test_cache_publisher_and_gc_do_not_interleave(source_bundle, tmp_path):
    claim, _ = source_bundle
    directory = tmp_path / "cache-out"
    directory.mkdir()
    (directory / "layer").write_bytes(b"layer")
    class InterleavedS3(FakeS3):
        def upload_file(self, filename, bucket, key):
            before = dict(self.objects)
            runtime.trim_cache(self, bucket, runtime._CACHE_TOTAL_BYTES)
            assert self.objects == before
            super().upload_file(filename,bucket,key)
    cache = InterleavedS3(_v2_cache_objects("f" * 64, 0, {"layer": b"other-cache"}))
    runtime._publish_cache_blobs(cache, claim, index=0, cache_dir=directory)
    manifest = json.loads(cache.objects[runtime._v2_manifest_key(claim["materialization_key"],0)])
    assert all(runtime._v2_blob_key(e["sha256"]) in cache.objects for e in manifest["files"])


@pytest.mark.parametrize("failure", ["get", "read", "parse"])
def test_trim_cache_defers_blob_sweep_until_retained_manifest_is_readable(failure, capsys) -> None:
    from datetime import UTC, datetime, timedelta

    old = datetime.now(UTC) - timedelta(days=10)
    recent = datetime.now(UTC) - timedelta(days=1)
    live = "e" * 64
    live_key = f"task-build-cache/v2/blobs/{live}"
    orphan_key = f"task-build-cache/v2/blobs/{'f' * 64}"
    manifest_key = f"task-build-cache/v2/{'a' * 64}/0/manifest.json"
    manifest = json.dumps(
        {"version": 1, "files": [{"path": "index.json", "sha256": live, "size": 2}]}
    ).encode()

    class UnreadableBody(io.BytesIO):
        def read(self, *args, **kwargs):
            raise OSError("interrupted cache manifest read")

    class RecoverableS3(FakeS3):
        fail_manifest = True

        def get_object(self, **kwargs):
            if kwargs["Key"] == manifest_key and self.fail_manifest:
                if failure == "get":
                    raise ClientError(
                        {"Error": {"Code": "InternalError", "Message": "private-sdk-detail"}},
                        "GetObject",
                    )
                body = UnreadableBody(manifest) if failure == "read" else io.BytesIO(b"{")
                self.bodies.append(body)
                return {"Body": body}
            return super().get_object(**kwargs)

    objects = {manifest_key: manifest, live_key: b"ok", orphan_key: b"xx"}
    listing = [
        {
            "Key": key,
            "Size": len(body),
            "LastModified": recent if key == manifest_key else old,
        }
        for key, body in objects.items()
    ]
    cache = RecoverableS3(objects, listing=listing)

    runtime.trim_cache(cache, "cache", 0)

    assert live_key in cache.objects, "unreadable references do not prove a blob is orphaned"
    assert orphan_key in cache.objects, "defer the sweep until the complete root set is known"
    assert manifest_key in cache.objects
    assert all(body.closed for body in cache.bodies)
    output = capsys.readouterr().out
    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    assert any(
        row.get("loom_task_image_stage") == "cache_gc"
        and row.get("event") == "deferred"
        and row.get("reason") == "retained_manifest_unreadable"
        for row in events
    )
    assert "private-sdk-detail" not in output
    assert "interrupted cache manifest read" not in output

    cache.fail_manifest = False
    runtime.trim_cache(cache, "cache", 0)

    assert live_key in cache.objects
    assert manifest_key in cache.objects
    assert orphan_key not in cache.objects, "a later healthy sweep still reclaims orphaned blobs"
    assert all(body.closed for body in cache.bodies)


def test_publish_directory_oci_uses_skopeo_oci_transport(
    source_bundle, tmp_path, publisher
) -> None:
    from loom_execution_actuator import task_image_oci as oci

    claim, _ = source_bundle
    claim = {**claim, "oci_export_format": "directory", "cache_transfer": "tar"}
    cache, calls = publisher
    work = tmp_path / "work"
    layout_dir = work / "oci/0000"
    files = {
        "blobs/sha256/" + "c" * 64: json.dumps(
            {"architecture": "amd64", "os": "linux", "rootfs": {"type": "layers", "diff_ids": []}}
        ).encode(),
        "blobs/sha256/" + "d" * 64: b"",
        "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
    }
    # Build a valid directory via archive round-trip helpers from oci tests pattern.
    config = files["blobs/sha256/" + "c" * 64]
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:" + "c" * 64,
                "size": len(config),
            },
            "layers": [],
        }
    ).encode()
    files["blobs/sha256/" + "d" * 64] = manifest
    files["index.json"] = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:" + "d" * 64,
                    "size": len(manifest),
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
        }
    ).encode()
    for name, body in files.items():
        path = layout_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    oci.validate_native_oci_directory(layout_dir)
    runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
    source = next(arg for arg in calls[0] if arg.startswith("oci:"))
    assert source == f"oci:{layout_dir}"
    assert cache.uploads == []
