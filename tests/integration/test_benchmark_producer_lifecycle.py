"""Importer and config-sync publication cannot grant or revive admission."""

from pathlib import Path
from uuid import uuid4

import pytest
import tomli_w
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.config.benchmarks import load_benchmarks_config
from loom.db.schema import Benchmark, Task
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import
from loom_cli.benchmarks_sync import sync
from tests.integration.test_benchmarks_sync_local import _write_bundle, _write_toml


class _Adapter:
    display_name = "Example"
    series = "test"
    license_spdx = "MIT"
    license_url = "https://example.test/license"
    splits = ("test",)
    upstream_source = UpstreamSource(kind="git", locator="https://example.test/tasks", revision="a" * 40)
    content = "Original instruction\n"

    def __init__(self):
        self.name = "producer-" + uuid4().hex

    def list_instances(self, **kwargs):
        for identity in ("first", "second"):
            yield BenchmarkInstance(instance_id=identity, split="test", raw={})

    def convert_instance(self, instance, *, out_dir: Path):
        identity = self.name + "/" + instance.instance_id
        config = {
            "schema_version": "1", "task": {"id": identity, "name": "Example"},
            "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
            "agent": {"name": "oracle"}, "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
        (out_dir / "task.toml").write_text(tomli_w.dumps(config))
        (out_dir / "instruction.md").write_text(self.content)
        return ConvertedTask(task_id=identity, checksum=task_checksum(out_dir), license_spdx="MIT", warnings=())


async def test_adapter_import_pending_exact_preservation_and_semantic_invalidation(
    postgres_url, tmp_path, monkeypatch,
):
    adapter = _Adapter()
    monkeypatch.setattr("loom_benchmark_tool.import_cmd._resolve_adapter", lambda *args, **kwargs: adapter)
    monkeypatch.setattr("loom_benchmark_tool.import_cmd.fetch_upstream", lambda *args, **kwargs: tmp_path)
    store = FakeObjectStore()
    options = dict(benchmark=adapter.name, db_url=postgres_url, object_store=store,
                   bucket="bundles", cache_dir=tmp_path / "cache", limit=1)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        assert await run_import(**options) == {"converted": 1, "warnings": 0}
        async with sessions.begin() as session:
            profile = await session.get(Benchmark, adapter.name)
            assert profile.execution_state == "pending"
            assert profile.profile_provenance["source_task_count"] == 2
            assert profile.profile_provenance["selected_task_ids"] == ["first"]
            assert profile.profile_provenance["selection"] == "subset"
            # Seed an existing accepted legacy catalog; publication must retain
            # its observations without claiming this test ran a benchmark.
            profile.execution_state = "runnable"
            profile.profile_provenance = {**profile.profile_provenance,
                                          "qualification": {"trial_id": "existing-observation"}}
            original_source = (await session.get(Task, adapter.name + "/first")).source
        adapter.display_name = "Updated description"
        await run_import(**options)
        async with sessions() as session:
            profile = await session.get(Benchmark, adapter.name)
            assert profile.execution_state == "runnable"
            assert profile.profile_provenance["qualification"]["trial_id"] == "existing-observation"
            assert profile.display_name == "Updated description"
        adapter.content = "Changed instruction\n"
        await run_import(**options)
        async with sessions.begin() as session:
            profile = await session.get(Benchmark, adapter.name)
            assert profile.execution_state == "pending"
            assert (await session.get(Task, adapter.name + "/first")).source != original_source
            old_key = original_source.removeprefix("s3://bundles/") + "instruction.md"
            assert await store.get_object(bucket="bundles", key=old_key) == b"Original instruction\n"
            profile.execution_state = "historical"
        with pytest.raises(ValueError, match="profile_historical"):
            await run_import(**options)
        async with sessions() as session:
            assert (await session.get(Benchmark, adapter.name)).execution_state == "historical"
    finally:
        await engine.dispose()


async def test_sync_preserves_admission_on_metadata_and_invalidates_changed_bundle(
    postgres_url, tmp_path,
):
    profile_id = "sync-" + uuid4().hex
    fixture_root = tmp_path / "fixtures"
    bundle = _write_bundle(fixture_root / profile_id, "first")
    config_path = tmp_path / "catalog.toml"
    _write_toml(config_path, profile_id)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def synchronize():
        async with sessions() as session:
            return await sync(load_benchmarks_config(config_path), fixtures_root=fixture_root,
                              session=session, registry_names=set())

    try:
        await synchronize()
        async with sessions.begin() as session:
            profile = await session.get(Benchmark, profile_id)
            assert profile.execution_state == "pending"
            profile.execution_state = "runnable"
        config_path.write_text(config_path.read_text().replace("Test bundle", "New display text"))
        await synchronize()
        async with sessions() as session:
            assert (await session.get(Benchmark, profile_id)).execution_state == "runnable"
        (bundle / "instruction.md").write_text("Changed semantics\n")
        await synchronize()
        async with sessions.begin() as session:
            profile = await session.get(Benchmark, profile_id)
            assert profile.execution_state == "pending"
            profile.execution_state = "historical"
        with pytest.raises(ValueError, match="profile_historical"):
            await synchronize()
    finally:
        await engine.dispose()


async def test_import_conversion_failure_never_registers_a_partial_profile(
    postgres_url, tmp_path, monkeypatch,
):
    class BrokenAdapter(_Adapter):
        def convert_instance(self, instance, *, out_dir):
            if instance.instance_id == "second":
                raise ValueError("invalid second upstream task")
            return super().convert_instance(instance, out_dir=out_dir)

    adapter = BrokenAdapter()
    monkeypatch.setattr("loom_benchmark_tool.import_cmd._resolve_adapter", lambda *args, **kwargs: adapter)
    monkeypatch.setattr("loom_benchmark_tool.import_cmd.fetch_upstream", lambda *args, **kwargs: tmp_path)
    store = FakeObjectStore()
    with pytest.raises(ValueError, match="invalid second upstream task"):
        await run_import(benchmark=adapter.name, db_url=postgres_url, object_store=store,
                         bucket="bundles", cache_dir=tmp_path / "cache")
    assert store.objects == {}
    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            assert await session.get(Benchmark, adapter.name) is None
            assert await session.get(Task, adapter.name + "/first") is None
    finally:
        await engine.dispose()
