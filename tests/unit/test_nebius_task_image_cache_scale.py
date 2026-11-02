"""Cache publication metadata scaling and checkpoint failure boundaries."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_nebius_task_image_runtime import FakeS3, _v2_cache_objects


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

    def delete_object(self, **kwargs):
        super().delete_object(**kwargs)
        if self.listing is not None:
            self.listing = [row for row in self.listing if row['Key'] != kwargs['Key']]


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
    # Complete size accounting needs no reference graph when no object is
    # eligible for age cleanup and the whole inventory already fits.
    assert manifest_reads == 0
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
        def put_object(self, **kwargs):
            if kwargs["Key"] == runtime._CACHE_GC_CHECKPOINT:
                state = json.loads(kwargs["Body"])
                # Fault the actual freeze/preaccount boundaries, independent
                # of the resumable collector's additional checkpoint writes.
                target = state.get('publications', 0) > 0 if failure_at == 2 else state.get('version') == 2
                if target:
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
    after_refusal = cache.lists
    _publish(cache, directory, 2)
    assert cache.lists == after_refusal == 3  # LIST + final post-delete accounting.
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
    # Age an actual blob so a reference graph is needed even though the bytes
    # fit; the new underbudget/recent fast path deliberately does no GETs.
    from datetime import timedelta
    cache._now -= timedelta(days=8)
    bad_key = runtime._v2_manifest_key("b" * 64, 0)
    cache.objects[bad_key] = b"{"
    state = _checkpoint(cache)
    state["publications"] = runtime._CACHE_GC_PUBLICATIONS
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(state).encode()
    _publish(cache, directory, 1)
    retained = _checkpoint(cache)
    for field in ('retained_bytes', 'debt_bytes', 'publications', 'scanned_at'):
        assert retained[field] == state[field]
    assert retained['version'] == 2 and retained['inventory_complete'] is False
    assert retained['retry_after'] > runtime.time.time()
    assert cache.objects[runtime._CACHE_GC_CHECKPOINT] != old_checkpoint
    assert not any(key == runtime._v2_manifest_key(f"{100001:064x}", 0) for key in cache.objects)
    assert "cache_gc_incomplete" in capsys.readouterr().out
    del cache.objects[bad_key]
    retained['retry_after'] = runtime.time.time() - 1
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(retained).encode()
    _publish(cache, directory, 1)
    assert _checkpoint(cache)['version'] == 1
    assert runtime._v2_manifest_key(f"{100001:064x}", 0) in cache.objects



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
    assert len(cache.heads) == 3  # Publisher HEAD, replay-safe GC HEAD, refreshed HEAD.
    assert len(cache.uploads) == 1
    manifest_key = runtime._v2_manifest_key(f"{100000:064x}", 0)
    assert _checkpoint(cache)["debt_bytes"] == len(cache.objects[manifest_key]) + len(body)
    assert cache.objects[key] == body


def _drive(cache, *, budget, incoming=1, policy='budget-pressure', passes=300):
    for _ in range(passes):
        previous = _checkpoint(cache) if runtime._CACHE_GC_CHECKPOINT in cache.objects else None
        reads = sum(key.endswith('/manifest.json') for key in cache.gets)
        ready = runtime._reserve_cache_publication(cache, 'cache', incoming, total_bytes=budget, gc_policy=policy)
        assert sum(key.endswith('/manifest.json') for key in cache.gets) - reads <= runtime._CACHE_GC_MAX_MANIFESTS
        if ready:
            assert _checkpoint(cache)['version'] == 1
            assert _checkpoint(cache)['retained_bytes'] + _checkpoint(cache)['debt_bytes'] <= budget
            assert not any(key.startswith(runtime._CACHE_GC_CONTROL) for key in cache.objects)
            return
        saved = _checkpoint(cache)
        assert saved['inventory_complete'] is False and saved['version'] == 2
        if previous is not None and previous['version'] == 2:
            for field in ('retained_bytes', 'debt_bytes', 'publications', 'scanned_at'):
                assert saved[field] == previous[field]
        assert 'retry_after' not in saved, 'positive progress must resume without a 30-minute delay'
    pytest.fail('cache collector did not converge within bounded passes')


def _small_passes(monkeypatch):
    monkeypatch.setattr(runtime, '_CACHE_GC_MAX_OBJECTS', 5)
    monkeypatch.setattr(runtime, '_CACHE_GC_MAX_MANIFESTS', 2)
    monkeypatch.setattr(runtime, '_CACHE_GC_ROOT_BATCH', 2)


def test_complete_list_resumes_exact_midpage_without_graph_or_control_recursion(monkeypatch, tmp_path):
    _small_passes(monkeypatch)
    cache = CountingS3(17)
    old = runtime._cache_checkpoint(900)
    old.update(scanned_at=runtime.time.time()-1000, debt_bytes=77, publications=2)
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(old).encode()
    _drive(cache, budget=65536 * 1024 * 1024)
    expected = sum(len(body) for key, body in cache.objects.items() if key.endswith('/manifest.json'))
    assert _checkpoint(cache)['retained_bytes'] == expected
    assert not any(key.endswith('/manifest.json') for key in cache.gets)
    before = cache.lists
    for index in range(8):
        _publish(cache, _cache_directory(tmp_path) if index == 0 else tmp_path / 'cache', index)
    assert cache.lists == before
    assert len([key for key in cache.objects if key.endswith('/manifest.json')]) == 25


@pytest.mark.parametrize('budget', [800, 1])
def test_large_graph_resumes_and_preserves_shared_last_reference(monkeypatch, budget):
    _small_passes(monkeypatch)
    objects = {}
    for index in range(7):
        objects.update(_v2_cache_objects(f'{index:064x}', 0, {'layer': b'shared' * 16}))
    cache = CountingS3()
    cache.objects.update(objects)
    _drive(cache, budget=budget, incoming=0)
    roots = [key for key in cache.objects if key.endswith('/manifest.json')]
    blobs = [key for key in cache.objects if runtime._V2_BLOB.fullmatch(key)]
    assert len(roots) == (5 if budget == 800 else 0)
    assert len(blobs) == bool(roots)
    if not roots:
        last_root = max(cache.deletes.index(key) for key in objects if key.endswith('/manifest.json'))
        assert cache.deletes.index(next(key for key in objects if runtime._V2_BLOB.fullmatch(key))) > last_root
    assert _checkpoint(cache)['retained_bytes'] == sum(len(cache.objects[key]) for key in (*roots, *blobs))


@pytest.mark.parametrize('at', ['segment_before_cursor', 'delete_after_commit', 'cleanup_after_commit', 'delete_then_change_budget'])
def test_resume_after_committed_side_effect_and_lost_checkpoint(monkeypatch, at):
    _small_passes(monkeypatch)
    class Crashed(CountingS3):
        crash = True
        def put_object(self, **kwargs):
            result = super().put_object(**kwargs)
            if self.crash and at == 'segment_before_cursor' and '/list-' in kwargs['Key']:
                self.crash = False
                raise SystemExit('simulated process death')
            return result
        def delete_object(self, **kwargs):
            result = super().delete_object(**kwargs)
            match = (at in ('delete_after_commit', 'delete_then_change_budget') and bool(runtime._V2_MANIFEST.fullmatch(kwargs['Key']))) or (
                at == 'cleanup_after_commit' and '/list-' in kwargs['Key'])
            if self.crash and match:
                self.crash = False
                raise SystemExit('simulated process death')
            return result
    cache = Crashed()
    for index in range(7):
        cache.objects.update(_v2_cache_objects(f'{index:064x}', 0, {'layer': b'shared' * 16}))
    with pytest.raises(SystemExit):
        _drive(cache, budget=800 if at == 'delete_then_change_budget' else 1, incoming=0)
    frozen = _checkpoint(cache)
    assert frozen['version'] == 2 and frozen['inventory_complete'] is False
    if at in ('delete_after_commit', 'delete_then_change_budget'):
        assert frozen['scan']['phase'] == 'delete', 'reference check must be durable before DELETE'
    if at == 'cleanup_after_commit':
        assert frozen['scan']['phase'] == 'cleanup', 'replay must not need removed metadata'
    _drive(cache, budget=1, incoming=0)
    assert _checkpoint(cache)['retained_bytes'] == 0


def test_policy_budget_and_incoming_changes_cannot_reuse_unsafe_decision(monkeypatch):
    _small_passes(monkeypatch)
    cache = CountingS3(10)
    # Advance a pressure snapshot far enough to have selected recent victims.
    for _ in range(100):
        assert not runtime._reserve_cache_publication(cache, 'cache', 0, total_bytes=1, gc_policy='budget-pressure')
        if _checkpoint(cache)['scan']['phase'] in ('load', 'refs'):
            break
    else:
        pytest.fail('did not reach the resumable graph')
    _drive(cache, budget=10000, incoming=100, policy='legacy-seven-day')
    assert len([key for key in cache.objects if key.endswith('/manifest.json')]) == 10
    assert not any(runtime._V2_MANIFEST.fullmatch(key) for key in cache.deletes)
    assert _checkpoint(cache)['debt_bytes'] == 100


def test_partial_inventory_blocks_new_optional_roots_until_complete(monkeypatch, tmp_path):
    _small_passes(monkeypatch)
    cache = CountingS3(13)
    directory = _cache_directory(tmp_path)
    _publish(cache, directory)
    assert runtime._v2_manifest_key(f'{100000:064x}', 0) not in cache.objects
    assert not cache.uploads
    _drive(cache, budget=runtime._CACHE_TOTAL_BYTES)
    _publish(cache, directory)
    assert runtime._v2_manifest_key(f'{100000:064x}', 0) in cache.objects


def test_resume_candidate_limit_matches_legal_manifest_entry_contract(monkeypatch):
    _small_passes(monkeypatch)
    monkeypatch.setattr(runtime, '_CACHE_GC_ROOT_BATCH', 4)
    monkeypatch.setattr(runtime, '_CACHE_GC_MAX_MANIFESTS', 4)
    cache = CountingS3()
    for index in range(4):
        entries = [{'path': str(offset), 'sha256': f'{index * 9000 + offset:064x}', 'size': 0}
                   for offset in range(9000)]
        cache.objects[runtime._v2_manifest_key(f'{index:064x}', 0)] = json.dumps({'version': 1, 'files': entries}).encode()
    for _ in range(100):
        assert not runtime._reserve_cache_publication(cache, 'cache', 0, total_bytes=0, gc_policy='budget-pressure')
        state = _checkpoint(cache)
        scan = state['scan']
        if scan['phase'] == 'refs':
            work_key = runtime._CACHE_GC_CONTROL + scan['generation'] + '/work-' + str(scan['work_slot']) + '.json'
            if len(json.loads(cache.objects[work_key])['candidates']) == 36000:
                break
    else:
        pytest.fail('did not persist the legal large candidate set')
    _drive(cache, budget=0, incoming=0)
    assert _checkpoint(cache)['retained_bytes'] == 0


@pytest.mark.parametrize('corruption', ['generation', 'candidate_identity', 'delete_closure'])
def test_resume_rejects_inconsistent_control_metadata_before_data_deletion(monkeypatch, corruption):
    _small_passes(monkeypatch)
    cache = CountingS3()
    for index in range(7):
        cache.objects.update(_v2_cache_objects(f'{index:064x}', 0, {'layer': b'shared' * 16}))
    original = runtime._write_cache_checkpoint
    def crash_before_delete(client, bucket, state):
        original(client, bucket, state)
        if state.get('scan', {}).get('phase') == 'delete':
            raise SystemExit('complete reference check committed')
    with monkeypatch.context() as patch:
        patch.setattr(runtime, '_write_cache_checkpoint', crash_before_delete)
        with pytest.raises(SystemExit):
            _drive(cache, budget=800, incoming=0)
    state = _checkpoint(cache)
    scan = state['scan']
    key = runtime._CACHE_GC_CONTROL + scan['generation'] + '/work-' + str(scan['work_slot']) + '.json'
    saved = cache.objects[key]
    work = json.loads(saved)
    wrong_row = [runtime._v2_blob_key('f' * 64), 1, runtime.time.time()-1000000]
    if corruption == 'generation':
        work['generation'] = 'f' * 32
    elif corruption == 'candidate_identity':
        next(iter(work['candidates'].values()))['row'] = wrong_row
    else:
        work['deletes'].append(wrong_row)
    cache.objects[key] = json.dumps(work).encode()
    before = list(cache.deletes)
    assert not runtime._reserve_cache_publication(cache, 'cache', 0, total_bytes=800, gc_policy='budget-pressure')
    assert cache.deletes == before
    assert _checkpoint(cache)['inventory_complete'] is False
    cache.objects[key] = saved
    state = _checkpoint(cache)
    state.pop('retry_after', None)
    cache.objects[runtime._CACHE_GC_CHECKPOINT] = json.dumps(state).encode()
    _drive(cache, budget=800, incoming=0)
