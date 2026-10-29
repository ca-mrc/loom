"""Cache publication metadata scaling and checkpoint failure boundaries."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_nebius_task_image_runtime import FakeS3


class CountingS3(FakeS3):
    def __init__(self, manifests: int = 0):
        objects = {
            runtime._v2_manifest_key(f"{index:064x}", 0): b'{"version":1,"files":[]}'
            for index in range(manifests)
        }
        super().__init__(objects)
        self.lists = 0
        self.puts = 0

    def paginate(self, **kwargs):
        self.lists += 1
        yield from super().paginate(**kwargs)

    def put_object(self, **kwargs):
        self.puts += 1
        return super().put_object(**kwargs)


def _publish(cache: FakeS3, directory: Path, index: int = 0) -> None:
    runtime._publish_cache_blobs(
        cache, {"cache_bucket": "cache", "materialization_key": f"{100000 + index:064x}"},
        index=0, cache_dir=directory,
    )


@pytest.mark.parametrize("manifests", [1000, 10000])
def test_ordinary_publications_do_not_rescan_retained_inventory(manifests, tmp_path, capsys):
    directory = tmp_path / "cache"
    directory.mkdir()
    (directory / "layer").write_bytes(b"shared layer")
    cache = CountingS3(manifests)
    for index in range(8):
        _publish(cache, directory, index)
    manifest_reads = sum(key.endswith("/manifest.json") for key in cache.gets)
    assert cache.lists == 1
    assert manifest_reads == manifests
    assert len([key for key in cache.objects if key.endswith("/manifest.json")]) == manifests + 8
    assert "cache_mutation_busy" not in capsys.readouterr().out


def _cache_directory(tmp_path):
    directory = tmp_path / "cache"
    directory.mkdir()
    (directory / "layer").write_bytes(b"shared layer")
    return directory


def _checkpoint(cache):
    return json.loads(cache.objects[runtime._CACHE_GC_CHECKPOINT])


def test_sweep_runs_only_after_a_bounded_number_of_publications(tmp_path, monkeypatch):
    cache = CountingS3()
    directory = _cache_directory(tmp_path)
    monkeypatch.setattr(runtime, "_CACHE_GC_PUBLICATIONS", 3)
    for index in range(3):
        _publish(cache, directory, index)
    assert cache.lists == 1
    assert _checkpoint(cache)["publications"] == 3
    _publish(cache, directory, 3)
    assert cache.lists == 2
    assert _checkpoint(cache)["publications"] == 1


def test_byte_debt_triggers_collection_without_unbounded_growth(tmp_path, monkeypatch):
    cache = CountingS3()
    directory = _cache_directory(tmp_path)
    monkeypatch.setattr(runtime, "_CACHE_GC_BYTES", 200)
    _publish(cache, directory)
    first_debt = _checkpoint(cache)["debt_bytes"]
    _publish(cache, directory, 1)
    assert first_debt < 200
    assert cache.lists == 2
    assert _checkpoint(cache)["debt_bytes"] <= first_debt


def test_age_tick_collects_at_next_publication_and_explicit_trim_can_run_idle(tmp_path, monkeypatch):
    cache = CountingS3()
    directory = _cache_directory(tmp_path)
    _publish(cache, directory)
    scanned_at = _checkpoint(cache)["scanned_at"]
    monkeypatch.setattr(runtime.time, "time", lambda: scanned_at + runtime._CACHE_GC_SECONDS + 1)
    _publish(cache, directory, 1)
    assert cache.lists == 2
    runtime.trim_cache(cache, "cache", 0)
    assert cache.lists == 3
    assert _checkpoint(cache)["publications"] == 0


@pytest.mark.parametrize("body", [b"{", b"{}", b'{"version":1,"retained_bytes":0}', b"x" * 4097])
def test_unknown_checkpoint_never_becomes_zero_debt(body, tmp_path, capsys):
    cache = CountingS3()
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = body
    _publish(cache, _cache_directory(tmp_path))
    assert cache.objects == {runtime._CACHE_GC_CHECKPOINT: body}
    assert cache.lists == 0
    assert "cache_checkpoint_unavailable" in capsys.readouterr().out
    # An explicit complete sweep, under the same lock, can repair the checkpoint.
    runtime.trim_cache(cache, "cache", 0)
    _publish(cache, tmp_path / "cache")
    assert cache.uploads


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("failure_at", [1, 2])
def test_checkpoint_put_failure_never_writes_unaccounted_cache(committed, failure_at, tmp_path, capsys):
    class FailedCheckpoint(CountingS3):
        checkpoint_writes = 0
        def put_object(self, **kwargs):
            if kwargs["Key"] == runtime._CACHE_GC_CHECKPOINT:
                self.checkpoint_writes += 1
                if self.checkpoint_writes == failure_at:
                    if committed:
                        super().put_object(**kwargs)
                    raise OSError("ambiguous write")
            return super().put_object(**kwargs)
    cache = FailedCheckpoint()
    _publish(cache, _cache_directory(tmp_path))
    assert cache.uploads == []
    assert all(not key.endswith("/manifest.json") for key in cache.objects)
    assert "cache_checkpoint_unavailable" in capsys.readouterr().out
    if failure_at == 2 and committed:
        assert _checkpoint(cache)["publications"] == 1
        assert _checkpoint(cache)["debt_bytes"] > 0


@pytest.mark.parametrize("after_manifest", [False, True])
def test_crash_after_preaccount_preserves_debt_until_next_complete_sweep(after_manifest, tmp_path):
    class CrashedPublisher(CountingS3):
        fail = True
        def upload_file(self, filename, bucket, key):
            assert _checkpoint(self)["publications"] == 1
            if self.fail and not after_manifest:
                super().upload_file(filename, bucket, key)
                raise OSError("cancelled before manifest")
            super().upload_file(filename, bucket, key)
        def put_object(self, **kwargs):
            result = super().put_object(**kwargs)
            if self.fail and after_manifest and kwargs["Key"].endswith("/manifest.json"):
                raise OSError("cancelled after manifest")
            return result
    cache = CrashedPublisher()
    directory = _cache_directory(tmp_path)
    with pytest.raises(OSError, match="cancelled"):
        _publish(cache, directory)
    assert _checkpoint(cache)["publications"] == 1
    debt = _checkpoint(cache)["debt_bytes"]
    assert debt > 0
    cache.fail = False
    _publish(cache, directory, 1)
    assert _checkpoint(cache)["publications"] == 2
    assert _checkpoint(cache)["debt_bytes"] >= debt
    assert cache.lists == 1
    runtime.trim_cache(cache, "cache", 0)
    assert _checkpoint(cache)["debt_bytes"] == 0
    for key, body in cache.objects.items():
        if key.endswith("/manifest.json"):
            assert all(runtime._v2_blob_key(item["sha256"]) in cache.objects
                       for item in json.loads(body)["files"])


def test_hard_budget_defers_and_does_not_rescan_for_each_refusal(tmp_path, monkeypatch, capsys):
    cache = CountingS3()
    directory = _cache_directory(tmp_path)
    monkeypatch.setattr(runtime, "_CACHE_TOTAL_BYTES", 160)
    _publish(cache, directory)
    assert cache.uploads
    _publish(cache, directory, 1)
    _publish(cache, directory, 2)
    assert cache.lists == 2
    assert len([key for key in cache.objects if key.endswith("/manifest.json")]) == 1
    assert "cache_budget_protected" in capsys.readouterr().out
    assert _checkpoint(cache)["retained_bytes"] <= 160


def test_unsupported_conditional_write_defers_cache_instead_of_append_only_growth(tmp_path, capsys):
    class IgnoresCondition(CountingS3):
        def put_object(self, **kwargs):
            kwargs.pop("IfNoneMatch", None)
            return super().put_object(**kwargs)
    cache = IgnoresCondition()
    _publish(cache, _cache_directory(tmp_path))
    assert not cache.objects
    assert cache.uploads == []
    assert "conditional_write_unsupported" in capsys.readouterr().out


def test_due_collection_with_unreadable_manifest_cannot_reset_debt(tmp_path, capsys):
    cache = CountingS3()
    directory = _cache_directory(tmp_path)
    _publish(cache, directory)
    old_checkpoint = cache.objects[runtime._CACHE_GC_CHECKPOINT]
    bad_key = runtime._v2_manifest_key("b" * 64, 0)
    cache.objects[bad_key] = b"{"
    state = _checkpoint(cache)
    state["publications"] = runtime._CACHE_GC_PUBLICATIONS
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(state).encode()
    debt_checkpoint = cache.objects[runtime._CACHE_GC_CHECKPOINT]
    _publish(cache, directory, 1)
    assert cache.objects[runtime._CACHE_GC_CHECKPOINT] == debt_checkpoint
    assert cache.objects[runtime._CACHE_GC_CHECKPOINT] != old_checkpoint
    assert not any(key == runtime._v2_manifest_key(f"{100001:064x}", 0) for key in cache.objects)
    assert "cache_gc_incomplete" in capsys.readouterr().out



def test_large_shared_warm_blob_does_not_trigger_a_sweep_for_each_publication(tmp_path, monkeypatch):
    # Model a 512 MiB stored blob with sparse local output and object metadata;
    # no test needs to hash, allocate, upload or download half a GiB.
    size = 512 * 1024 * 1024
    directory = tmp_path / "large-cache"
    directory.mkdir()
    layer = directory / "layer"
    with layer.open("wb") as output:
        output.truncate(size)
    sha = "d" * 64
    blob_key = runtime._v2_blob_key(sha)

    class LargeBlobMetadataS3(CountingS3):
        def paginate(self, **kwargs):
            for page in super().paginate(**kwargs):
                for item in page["Contents"]:
                    if item["Key"] == blob_key:
                        item["Size"] = size
                yield page

    cache = LargeBlobMetadataS3()
    cache.objects[blob_key] = b"stored blob represented by metadata"
    monkeypatch.setattr(runtime, "_inventory_cache_directory", lambda _directory: [
        {"path": "layer", "sha256": sha, "size": size, "local": layer},
    ])
    for index in range(8):
        _publish(cache, directory, index)
    assert cache.lists == 1
    assert cache.uploads == []
    assert len(cache.heads) == 9  # One each, plus the bootstrap-GC refresh.
    assert _checkpoint(cache)["retained_bytes"] == size
    assert _checkpoint(cache)["debt_bytes"] < 4096
    assert _checkpoint(cache)["publications"] == 8


def test_gc_removed_existing_orphan_is_rechecked_before_byte_reservation(tmp_path):
    import hashlib
    from datetime import UTC, datetime, timedelta

    body = b"shared layer"
    sha = hashlib.sha256(body).hexdigest()
    key = runtime._v2_blob_key(sha)
    directory = _cache_directory(tmp_path)
    cache = CountingS3()
    cache.objects[key] = body
    cache.listing = [{"Key": key, "Size": len(body),
                      "LastModified": datetime.now(UTC) - timedelta(days=10)}]
    _publish(cache, directory)
    # The first HEAD sees it; bootstrap GC removes the old unreferenced blob;
    # the post-GC HEAD must turn it into accounted uploaded bytes.
    assert key in cache.deletes
    assert len(cache.heads) == 2
    assert len(cache.uploads) == 1
    manifest_key = runtime._v2_manifest_key(f"{100000:064x}", 0)
    assert _checkpoint(cache)["debt_bytes"] == len(cache.objects[manifest_key]) + len(body)
    assert cache.objects[key] == body
