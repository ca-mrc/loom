"""The adapter prepares input; publication uses the local-folder backend."""

import asyncio
import threading
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource

from loom.models.task_checksum import task_checksum
from loom_cli import benchmark_prepare, benchmark_publish


class FakeAdapter:
    name = "adapter-bench"
    display_name = "Adapter fixture"
    series = "fixtures"
    license_spdx = "MIT"
    license_url = "https://example.test/license"
    splits = ("test",)
    upstream_source = UpstreamSource(
        kind="git", locator="https://example.test/source", revision="abc"
    )

    def list_instances(self, **kwargs):
        for instance in ("HumanEval/0", "HumanEval_0"):
            yield BenchmarkInstance(
                instance_id=instance, split="test", raw={}, tags={"language": "python"}
            )

    def convert_instance(self, inst, *, out_dir):
        task_id = f"{self.name}/{inst.instance_id}"
        (out_dir / "task.toml").write_text(f'''schema_version = "1"
[task]
id = "{task_id}"
name = "Fixture"
[environment]
os = "linux"
docker_image = "python:3.11-alpine"
[agent]
name = "oracle"
[verifier]
name = "pytest"
[[steps]]
name = "main"
''')
        (out_dir / "instruction.md").write_text("fixture")
        return ConvertedTask(task_id, task_checksum(out_dir), "MIT", ("conversion note",))

    def task_source_provenance(self, **kwargs):
        return {"upstream_fixture": "preserve-me"}


@pytest.fixture
def adapter_source(monkeypatch, tmp_path):
    adapter = FakeAdapter()
    monkeypatch.setitem(benchmark_prepare.REGISTRY, adapter.name, adapter)
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kw: tmp_path)
    return adapter


def test_prepare_preserves_identity_selection_and_metadata(adapter_source, tmp_path):
    prepared = benchmark_prepare.prepare_adapter_benchmark(
        adapter_source.name,
        cache_dir=tmp_path / "cache",
        staging_dir=tmp_path,
    )
    assert {task["task_id"] for task in prepared.tasks.values()} == {
        "adapter-bench/HumanEval/0",
        "adapter-bench/HumanEval_0",
    }
    assert len(prepared.task_tomls) == 2
    for item in prepared.tasks.values():
        assert item["tags"]["split"] == "test"
        assert item["tags"]["language"] == "python"
        assert item["source_provenance"]["upstream_fixture"] == "preserve-me"
    assert prepared.manifest["upstream_revision"] == "abc"
    assert prepared.warnings == (
        "adapter-bench/HumanEval/0: conversion note",
        "adapter-bench/HumanEval_0: conversion note",
    )


def test_prepare_rejects_physical_profile_without_isolation(adapter_source, tmp_path, monkeypatch):
    adapter_source.name = "terminal-bench-2@tb2.1-r6"
    monkeypatch.setitem(benchmark_prepare.REGISTRY, adapter_source.name, adapter_source)
    with pytest.raises(ValueError, match="private workspace isolation"):
        benchmark_prepare.prepare_adapter_benchmark(
            adapter_source.name,
            cache_dir=tmp_path / "cache",
            staging_dir=tmp_path,
        )


def test_preparation_fetches_independent_audit_source(tmp_path, monkeypatch):
    execution = tmp_path / "execution"
    execution.mkdir()
    audit = tmp_path / "audit"
    (audit / "repo").mkdir(parents=True)
    (audit / "repo" / "manifest.json").write_text('{"reviewed": true}')
    adapter = FakeAdapter()
    adapter.audit_source = UpstreamSource(
        kind="git", locator="https://example.test/audit", revision="pinned-audit"
    )
    adapter.audit_manifest_source = "manifest.json"
    seen = []

    def fetch(source, **kwargs):
        seen.append((source, kwargs))
        return audit if source == adapter.audit_source else execution

    monkeypatch.setattr(benchmark_prepare, "fetch_upstream", fetch)
    result = benchmark_prepare._prepare_adapter_source(
        adapter=adapter,
        cache_dir=tmp_path / "cache",
        refresh=True,
    )
    assert result == execution
    assert (result / "audit" / "manifest.json").read_text() == '{"reviewed": true}'
    assert [source for source, _ in seen] == [adapter.upstream_source, adapter.audit_source]
    assert all(
        options == {"cache_root": tmp_path / "cache", "refresh": True} for _, options in seen
    )


def test_prepare_cache_permission_diagnostic(adapter_source, monkeypatch, tmp_path):
    def fail(**kw):
        raise PermissionError(13, "Permission denied", "SkillLearnBench_logo.png")

    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", fail)
    cache = tmp_path / "shared-cache"
    with pytest.raises(ValueError, match="--cache-dir") as error:
        benchmark_prepare.prepare_adapter_benchmark(
            adapter_source.name,
            cache_dir=cache,
            staging_dir=tmp_path,
            refresh=True,
        )
    assert str(cache) in str(error.value)
    assert "owned by another user" in str(error.value)


def test_cache_defaults_are_user_scoped_and_overridable(monkeypatch, tmp_path):
    monkeypatch.delenv("LOOM_BENCHMARK_CACHE", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "user-cache"))
    assert benchmark_publish.default_benchmark_cache() == tmp_path / "user-cache/loom/benchmarks"
    monkeypatch.setenv("LOOM_BENCHMARK_CACHE", str(tmp_path / "explicit"))
    assert benchmark_publish.default_benchmark_cache() == tmp_path / "explicit"


@pytest.mark.asyncio
async def test_both_inputs_use_same_publication_backend(adapter_source, monkeypatch, tmp_path):
    publish = AsyncMock(return_value=SimpleNamespace(task_count=1))
    monkeypatch.setattr(
        benchmark_publish.local_benchmark_publish, "publish_local_benchmark", publish
    )
    await benchmark_publish.publish_benchmark(tmp_path, db_url="local-test")
    assert publish.call_args.args == (tmp_path,)
    await benchmark_publish.publish_benchmark(
        benchmark=adapter_source.name,
        cache_dir=tmp_path / "cache",
        db_url="local-test",
        instance_ids={"HumanEval/0"},
        execution_profile="nebius-terminus",
    )
    kwargs = publish.call_args.kwargs
    assert kwargs["prepared_adapter"].task_count == 1
    assert kwargs["execution_profile"] == "nebius-terminus"
    assert not publish.call_args.args[0].exists()  # temporary conversion tree was cleaned


@pytest.mark.asyncio
async def test_harbor_publication_prepares_cold_cache_and_refresh(monkeypatch, tmp_path):
    adapter = FakeAdapter()
    adapter.upstream_source = UpstreamSource(
        kind="harbor-package", locator="terminal-bench/terminal-bench-2-1", revision="6"
    )
    monkeypatch.setitem(benchmark_prepare.REGISTRY, adapter.name, adapter)
    downloads = []

    async def download(source, output_dir):
        downloads.append((source, output_dir))
        (output_dir / "harbor-materialization.json").write_text("{}")

    async def publish(root, *, prepared_adapter):
        assert prepared_adapter.task_count == 2
        assert all(path.is_file() for path in prepared_adapter.task_tomls)
        return SimpleNamespace(task_count=2)

    backend = AsyncMock(side_effect=publish)
    monkeypatch.setattr("loom_benchmarks.harbor_dataset.download_harbor_dataset", download)
    monkeypatch.setattr(
        benchmark_publish.local_benchmark_publish, "publish_local_benchmark", backend
    )
    for refresh in (False, False, True):
        result = await benchmark_publish.publish_benchmark(
            benchmark=adapter.name, cache_dir=tmp_path / "cache", refresh=refresh
        )
        assert result.task_count == 2
        assert not backend.call_args.args[0].exists()
    assert len(downloads) == 2
    assert all(source == adapter.upstream_source for source, _ in downloads)


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_fails", [False, True])
async def test_cancelled_preparation_finishes_before_staging_cleanup(
    monkeypatch, tmp_path, worker_fails
):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    staging = []
    writes = []

    def prepare(benchmark, *, staging_dir, **kwargs):
        staging.append(staging_dir)
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=5)
            marker = staging_dir / "worker-finished"
            marker.write_text("complete")
            writes.append(marker.read_text())
            if worker_fails:
                raise ValueError("preparation failed after cancellation")
            return object()
        finally:
            finished.set()

    backend = AsyncMock()
    monkeypatch.setattr(benchmark_prepare, "prepare_adapter_benchmark", prepare)
    monkeypatch.setattr(
        benchmark_publish.local_benchmark_publish, "publish_local_benchmark", backend
    )
    publication = asyncio.create_task(benchmark_publish.publish_benchmark(
        benchmark="adapter-bench", cache_dir=tmp_path / "cache"
    ))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        for _ in range(2):
            publication.cancel()
            cancellation_delivered = asyncio.Event()
            loop.call_soon(cancellation_delivered.set)
            await cancellation_delivered.wait()
            assert not publication.done()
            assert staging[0].is_dir()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await publication
        assert finished.is_set()
        assert writes == ["complete"]
        assert not staging[0].exists()
        backend.assert_not_awaited()
    finally:
        release.set()
        with suppress(asyncio.CancelledError):
            await publication
        assert await asyncio.to_thread(finished.wait, 5)


@pytest.mark.asyncio
async def test_input_selection_does_not_guess_from_path(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="exactly one"):
        await benchmark_publish.publish_benchmark(tmp_path, benchmark="adapter-bench")
    with pytest.raises(ValueError, match="require --benchmark"):
        await benchmark_publish.publish_benchmark(tmp_path, refresh=True)
