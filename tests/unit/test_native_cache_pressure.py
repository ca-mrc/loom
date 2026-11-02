"""Optional cache may disappear; source and local filesystem errors may not."""
import errno
import hashlib
import json
from pathlib import Path

import pytest
from botocore.exceptions import EndpointConnectionError

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_nebius_task_image_runtime import (
    FakeS3,
    _cache_data_deletes,
    _cache_moment,
    _listed,
    _v2_cache_objects,
    _write_native_oci,
)
from tests.unit.test_nebius_task_image_runtime import publisher as publisher
from tests.unit.test_nebius_task_image_runtime import source_bundle as source_bundle


def claim(**fields):
    return {"materialization_key": "a" * 64, "cache_bucket": "cache", **fields}


@pytest.mark.parametrize("transfer", ["tar", "blobs"])
def test_optional_cache_transport_failure_is_bounded_cold_miss(tmp_path, transfer):
    class Offline(FakeS3):
        def get_object(self, **kwargs):
            self.gets.append(kwargs["Key"])
            raise EndpointConnectionError(endpoint_url="https://private.invalid/token")
    cache = Offline({})
    runtime._try_import_cache(cache, claim(cache_transfer=transfer), index=0, work=tmp_path)
    assert len(cache.gets) <= 2
    assert not (tmp_path / "cache-in/0").exists()


def test_failed_component_never_commits_verified_aliases_or_partial_directory(tmp_path):
    objects = _v2_cache_objects("a" * 64, 0, {"first": b"first", "second": b"second"})
    cache = FakeS3(objects)
    del cache.objects[runtime._v2_blob_key(hashlib.sha256(b"second").hexdigest())]
    verified = {}
    runtime._try_import_cache(cache, claim(), index=0, work=tmp_path, verified=verified)
    assert verified == {}
    assert not (tmp_path / "cache-in/0").exists()
    assert list((tmp_path / "cache-in").iterdir()) == []


@pytest.mark.parametrize("paths", [("layer", "layer"), ("layer", "layer/child"), ("../escape",)])
def test_cache_manifest_rejects_ambiguous_paths_before_local_writes(paths):
    body = json.dumps({"version": 1, "files": [{"path": path, "sha256": "a" * 64, "size": 1} for path in paths]}).encode()
    with pytest.raises(runtime.BuildPreparationError):
        runtime._parse_cache_manifest(body)


def test_optional_cache_local_enospc_is_not_a_cold_miss(tmp_path, monkeypatch):
    cache = FakeS3(_v2_cache_objects("a" * 64, 0, {"layer": b"data"}))
    def full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "disk full")
    monkeypatch.setattr(runtime.shutil, "copyfile", full)
    with pytest.raises(OSError) as caught:
        runtime._try_import_cache(cache, claim(), index=0, work=tmp_path)
    assert caught.value.errno == errno.ENOSPC


def test_failed_cache_cleanup_is_fatal_not_a_usable_partial_import(tmp_path, monkeypatch):
    cache = FakeS3(_v2_cache_objects("a" * 64, 0, {"first": b"first", "second": b"second"}))
    del cache.objects[runtime._v2_blob_key(hashlib.sha256(b"second").hexdigest())]
    original = runtime.shutil.rmtree
    def denied(path, *args, **kwargs):
        if Path(path).name.startswith('.partial-'):
            raise OSError(errno.EACCES, 'cleanup denied')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(runtime.shutil, 'rmtree', denied)
    with pytest.raises(OSError) as caught:
        runtime._try_import_cache(cache, claim(), index=0, work=tmp_path)
    assert caught.value.errno == errno.EACCES


def test_cache_stream_interruption_is_optional_but_source_download_is_fatal(tmp_path):
    class Broken(FakeS3):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            def interrupted(*args):
                raise OSError('interrupted remote body')
            response['Body'].read = interrupted
            return response
    cache = Broken(_v2_cache_objects('a' * 64, 0, {'layer': b'bytes'}))
    runtime._try_import_cache(cache, claim(), index=0, work=tmp_path)
    assert not (tmp_path / 'cache-in/0').exists()
    with pytest.raises(OSError, match='interrupted'):
        runtime._download(cache, bucket='source', key=runtime._v2_manifest_key('a' * 64, 0),
            destination=tmp_path / 'source', limit=4096)


def test_complete_scan_backoff_still_allows_smaller_publication_without_rescan():
    class NoScan(FakeS3):
        def get_paginator(self, name):
            raise AssertionError('backoff must not rescan')
    state = runtime._cache_checkpoint(100)
    state.update(retry_after=runtime.time.time()+1000, gc_policy='budget-pressure', budget_bytes=200)
    cache = NoScan({runtime._CACHE_GC_CHECKPOINT: json.dumps(state).encode()})
    assert runtime._reserve_cache_publication(cache, 'cache', 10, total_bytes=200, gc_policy='budget-pressure')
    assert json.loads(cache.objects[runtime._CACHE_GC_CHECKPOINT])['debt_bytes'] == 10


@pytest.mark.parametrize("policy,evicted", [("legacy-seven-day", False), ("budget-pressure", True)])
def test_pressure_reclaims_recent_last_reference_only(policy, evicted, monkeypatch):
    objects = _v2_cache_objects("a" * 64, 0, {"layer": b"shared"})
    objects.update(_v2_cache_objects("b" * 64, 0, {"layer": b"shared"}))
    # Both roots share one physical blob, whose bytes exceed the budget.
    listing = [_listed(key, 1000 if "/blobs/" in key else len(value), _cache_moment(days=1)) for key, value in objects.items()]
    cache = FakeS3(dict(objects), listing=listing)
    runtime.trim_cache(cache, "cache", 10, total_bytes=500, gc_policy=policy)
    data = {key for key in cache.objects if key != runtime._CACHE_GC_CHECKPOINT}
    assert bool(data) is not evicted
    if evicted:
        deletes = _cache_data_deletes(cache)
        blob = next(key for key in objects if "/blobs/" in key)
        assert deletes.index(blob) > max(deletes.index(runtime._v2_manifest_key(k * 64, 0)) for k in ("a", "b"))


@pytest.mark.parametrize('transfer', ['tar', 'blobs'])
def test_real_publisher_path_applies_frozen_pressure_and_refreshes_reclaimed_incoming(
    source_bundle, publisher, tmp_path, monkeypatch, transfer,
):
    cache, copies = publisher
    frozen = {**source_bundle[0], 'cache_transfer': transfer, 'cache_gc_policy': 'budget-pressure'}
    work = tmp_path / 'work'
    (work / 'oci').mkdir(parents=True)
    _write_native_oci(work / 'oci/0000.tar')
    output = work / 'cache-out/0'
    output.mkdir(parents=True)
    (output / 'layer').write_bytes(b'shared bytes')
    if transfer == 'blobs':
        cache.objects.update(_v2_cache_objects(frozen['materialization_key'], 0, {'layer': b'shared bytes'}))
        # Pretend the existing physical blob is larger than the store's tiny
        # test budget. GC removes it; refreshed HEAD must force re-upload.
        cache.listing = [_listed(k, 1000 if '/blobs/' in k else len(v), _cache_moment(days=1)) for k,v in cache.objects.items()]
        monkeypatch.setattr(runtime, '_CACHE_TOTAL_BYTES', 500)
    else:
        key = f"task-build-cache/{'b'*64}/0.tar"
        cache.objects[key] = b'recent old cache'
        cache.listing = [_listed(key, 12000, _cache_moment(days=1))]
        monkeypatch.setattr(runtime, '_CACHE_TOTAL_BYTES', 11000)
    runtime.publish(frozen, work, tmp_path / 'secrets', receipt_path=tmp_path / 'receipt')
    assert len(copies) == 1 and len(cache.uploads) == 1
    saved = json.loads(cache.objects[runtime._CACHE_GC_CHECKPOINT])
    assert saved['gc_policy'] == 'budget-pressure' and saved['retained_bytes'] == 0
    assert saved['debt_bytes'] <= runtime._CACHE_TOTAL_BYTES


def test_bounded_incomplete_scan_never_resets_debt_and_backs_off(monkeypatch):
    class Counted(FakeS3):
        scans = 0
        def paginate(self, **kwargs):
            self.scans += 1
            yield from super().paginate(**kwargs)
    cache = Counted({runtime._CACHE_GC_CHECKPOINT: json.dumps({
        "version": 1, "scanned_at": runtime.time.time()-1000, "retained_bytes": 100,
        "debt_bytes": 77, "publications": 2}).encode()}, listing=[
        _listed(f"task-build-cache/{i:064x}/0.tar", 1, _cache_moment(days=1)) for i in range(3)])
    monkeypatch.setattr(runtime, "_CACHE_GC_MAX_OBJECTS", 2)
    for _ in range(2):
        assert not runtime._reserve_cache_publication(cache, "cache", 1, total_bytes=1000, gc_policy="budget-pressure")
    saved = json.loads(cache.objects[runtime._CACHE_GC_CHECKPOINT])
    assert saved["debt_bytes"] == 77 and saved["retained_bytes"] == 100
    assert not saved["inventory_complete"] and cache.scans == 1
    assert saved['version'] == 2, 'old writers must not treat incomplete accounting as an empty v1 inventory'
    assert not _cache_data_deletes(cache)


@pytest.mark.parametrize('limit', ['manifests', 'bytes', 'seconds', 'sdk'])
def test_scan_failure_bounds_preserve_prior_accounting_and_backoff(monkeypatch, limit):
    class Counted(FakeS3):
        scans = 0
        def get_paginator(self, name):
            self.scans += 1
            if limit == 'sdk':
                raise EndpointConnectionError(endpoint_url='https://private.invalid')
            return super().get_paginator(name)
    objects = _v2_cache_objects('a' * 64, 0, {'layer': b'data'})
    objects.update(_v2_cache_objects('b' * 64, 0, {'layer': b'data'}))
    old = runtime._cache_checkpoint(900)
    old.update(scanned_at=runtime.time.time()-1000, debt_bytes=77, publications=2)
    objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(old).encode()
    cache = Counted(objects)
    if limit == 'manifests':
        monkeypatch.setattr(runtime, '_CACHE_GC_MAX_MANIFESTS', 1)
    elif limit == 'bytes':
        monkeypatch.setattr(runtime, '_CACHE_GC_MAX_MANIFEST_BYTES', 10)
    elif limit == 'seconds':
        clock = iter([0, 31])
        monkeypatch.setattr(runtime.time, 'monotonic', lambda: next(clock))
    for _ in range(2):
        assert not runtime._reserve_cache_publication(cache, 'cache', 1, total_bytes=1000, gc_policy='budget-pressure')
    saved = json.loads(cache.objects[runtime._CACHE_GC_CHECKPOINT])
    assert (saved['retained_bytes'], saved['debt_bytes'], saved['version']) == (900, 77, 2)
    assert cache.scans == 1 and not _cache_data_deletes(cache)
    assert all(body.closed for body in cache.bodies)


def test_explicit_invalid_runtime_policy_and_sibling_flag_are_rejected(source_bundle, tmp_path):
    original = source_bundle[0]
    for field, value in (("cache_gc_policy", "unknown"), ("cache_gc_policy", True), ("reuse_task_cache_for_verifier", 1)):
        path = tmp_path / "claim.json"
        path.write_text(json.dumps({**original, field: value}))
        with pytest.raises(runtime.BuildPreparationError):
            runtime.load_claim(path)


