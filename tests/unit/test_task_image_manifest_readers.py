from __future__ import annotations

import os
from dataclasses import replace
from unittest.mock import patch
from uuid import uuid4

import pytest

from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from loom.task_image_materialization import task_image_materialization_key
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME
from loom_worker import main_loop, task_image_builder
from loom_worker.runner_pool import RunnerPool
from loom_worker.task_bundle_integrity import verified_task_image_cache_identity
from tests.unit.test_main_loop_cleanup import _FakeCPClient, _FakeSettings, _RunnerDouble
from tests.unit.test_task_image_builder import _claim, _settings
from tests.unit.test_task_image_bundle_content_manifest import _legacy_collision


def test_replaced_transport_sidecar_rejects_without_unlinking_replacement(tmp_path, monkeypatch):
    left, _right = _legacy_collision(tmp_path)
    manifest = capture_task_image_bundle_manifest(left)
    sidecar = left / BUNDLE_FILE_METADATA_NAME
    sidecar.write_bytes(manifest.mode_metadata_bytes)
    original_inode = sidecar.stat().st_ino
    original_read = os.read
    changed = False

    def read(descriptor, size):
        nonlocal changed
        data = original_read(descriptor, size)
        if not changed and os.fstat(descriptor).st_ino == original_inode:
            changed = True
            sidecar.rename(tmp_path / "previous-sidecar")
            sidecar.write_bytes(manifest.mode_metadata_bytes)
        return data

    monkeypatch.setattr(os, "read", read)
    with pytest.raises(task_image_builder.TaskImageBuildError, match="content manifest"):
        verified_task_image_cache_identity(
            left,
            task_checksum=manifest.task_checksum,
            source_provenance={"bundle_content_manifest_sha256": manifest.digest},
        )
    assert changed
    assert sidecar.read_bytes() == manifest.mode_metadata_bytes


@pytest.mark.parametrize("matching", [False, True])
@pytest.mark.parametrize("sidecar", ["absent", "valid", "tampered"])
async def test_builder_verifies_manifest_before_both_component_cache_lookups(
    tmp_path,
    monkeypatch,
    matching,
    sidecar,
):
    left, right = _legacy_collision(tmp_path)
    manifest = capture_task_image_bundle_manifest(left)
    claim = replace(
        _claim(),
        task_checksum=manifest.task_checksum,
        materialization_key=task_image_materialization_key(
            task_id=_claim().task_id,
            task_checksum=manifest.task_checksum,
            cpu_arch="x86_64",
            bundle_content_manifest_sha256=manifest.digest,
        ),
        task_source_provenance={
            "bundle_content_manifest_sha256": manifest.digest,
            "bundle_file_metadata_sha256": "sha256:" + manifest.bundle_file_metadata_sha256,
        },
    )
    calls = []
    downloaded = left if matching else right
    if sidecar != "absent":
        (downloaded / BUNDLE_FILE_METADATA_NAME).write_bytes(
            manifest.mode_metadata_bytes if sidecar == "valid" else b"unverified input"
        )

    async def materialize(**_kwargs):
        return downloaded

    async def main(**kwargs):
        assert not (downloaded / BUNDLE_FILE_METADATA_NAME).exists()
        calls.append(("main", kwargs["task_checksum"]))
        return "loom-task:main"

    async def sidecars(**kwargs):
        assert not (downloaded / BUNDLE_FILE_METADATA_NAME).exists()
        calls.append(("sidecar", kwargs["task_checksum"]))
        return {"database": "loom-task:sidecar"}

    async def publish(**_kwargs):
        return "registry.example/task@sha256:" + "d" * 64

    async def architecture(**_kwargs):
        return None

    monkeypatch.setattr(task_image_builder, "host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(task_image_builder, "_build_worker_object_store", lambda _settings: None)
    monkeypatch.setattr(task_image_builder, "_materialize_task_dir", materialize)
    monkeypatch.setattr(task_image_builder, "resolve_task_image", main)
    monkeypatch.setattr(task_image_builder, "build_task_sidecar_images", sidecars)
    monkeypatch.setattr(task_image_builder, "publish_local_image_to_registry", publish)
    monkeypatch.setattr(task_image_builder, "verify_local_image_architecture", architecture)
    if matching and sidecar != "tampered":
        result = await task_image_builder.materialize_and_publish_task_images(claim, _settings())
        assert set(result) == {"task", "sidecar:database"}
        assert calls == [
            ("main", "bundle-manifest-sha256:" + manifest.digest),
            ("sidecar", "bundle-manifest-sha256:" + manifest.digest),
        ]
    else:
        with pytest.raises(task_image_builder.TaskImageBuildError, match="content manifest"):
            await task_image_builder.materialize_and_publish_task_images(claim, _settings())
        assert calls == []
    assert not downloaded.exists(), "both rejection and successful build must clean up"


@pytest.mark.parametrize("matching", [False, True])
@pytest.mark.parametrize("sidecar", ["absent", "valid", "tampered"])
async def test_trial_verifies_manifest_before_runtime_and_preserves_legacy_metadata(
    tmp_path,
    matching,
    sidecar,
):
    left, right = _legacy_collision(tmp_path)
    manifest = capture_task_image_bundle_manifest(left)
    claim = _claim(cpu_arch="x86_64")
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    calls = {}
    downloaded = left if matching else right
    if sidecar != "absent":
        (downloaded / BUNDLE_FILE_METADATA_NAME).write_bytes(
            manifest.mode_metadata_bytes if sidecar == "valid" else b"unverified input"
        )

    class Runner(_RunnerDouble):
        def __init__(self, **kwargs):
            calls["runner"] = kwargs

        async def run(self):
            return None

    async def materialize(**_kwargs):
        return downloaded

    async def resolve(**kwargs):
        calls["image"] = kwargs
        assert not (downloaded / BUNDLE_FILE_METADATA_NAME).exists()
        return kwargs["registry_image"]

    payload = {
        "trial_id": str(uuid4()),
        "team_id": str(uuid4()),
        "task_id": claim.task_id,
        "attempt_count": 1,
        "config": {"agent_name": "oracle", "agent_model": None},
        "task_image_materialization": {
            "schema_version": "loom.task-image-execution-grant.v1",
            "materialization_id": str(uuid4()),
            "materialization_key": task_image_materialization_key(
                task_id=claim.task_id,
                task_checksum=manifest.task_checksum,
                cpu_arch="x86_64",
                bundle_content_manifest_sha256=manifest.digest,
            ),
            "task_checksum": manifest.task_checksum,
            "cpu_arch": "x86_64",
            "task_config": claim.task_config,
            "task_source": None,
            "task_source_provenance": {
                "bundle_content_manifest_sha256": manifest.digest,
                "bundle_file_metadata_sha256": "sha256:" + manifest.bundle_file_metadata_sha256,
            },
            "registry_images": {
                "task": "registry.example/task@sha256:" + "d" * 64,
                "sidecar:database": "registry.example/sidecar@sha256:" + "e" * 64,
            },
        },
    }
    with (
        patch.object(main_loop, "_materialize_task_dir", materialize),
        patch.object(main_loop, "_host_cpu_arch", lambda: "x86_64"),
        patch.object(main_loop, "LocalTrialRunner", Runner),
        patch.object(main_loop, "resolve_task_image", resolve),
    ):
        await main_loop._spawn_trial(
            pool=pool,
            settings=_FakeSettings(),
            cp_client=cp,
            gateway_client=None,
            object_store=None,
            worker_id=uuid4(),
            payload=payload,
        )
        await pool.wait_all(timeout=2.0)
    if matching and sidecar != "tampered":
        assert calls["image"]["task_checksum"] == "bundle-manifest-sha256:" + manifest.digest
        assert calls["runner"]["task_checksum"] == manifest.task_checksum
        assert (
            calls["runner"]["sidecar_runtime_factory"]().task_checksum
            == "bundle-manifest-sha256:" + manifest.digest
        )
    else:
        assert calls == {}, (
            "manifest mismatch must reject before image lookup or runner construction"
        )
    assert not downloaded.exists()
