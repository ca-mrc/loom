"""Real DB/versioned TLS storage publication and concurrent origin binding."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark, Task
from loom_cli import benchmark_prepare, benchmark_publish
from loom_cli.benchmark_types import AdapterBenchmarkEntry
from loom_cli.local_benchmark_publish import _upsert_benchmark
from tests.integration.test_task_bundle_source_storage import _store
from tests.support.minio_tls import minio_tls as minio_tls
from tests.unit.test_harbor_native_import import native_tree, spec

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


async def test_native_publisher_persists_source_and_reuses_exact_version(
    postgres_url, tmp_path, monkeypatch, minio_tls
):
    profile = "native-fixture@" + uuid4().hex
    source = tmp_path / "source"
    source_task = native_tree(source)
    (source_task / "environment/Dockerfile").write_text(
        "FROM python:3.11-slim\nRUN first && second || true\n"
    )
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    descriptor = tmp_path / "source.json"
    descriptor.write_text(spec(profile).model_dump_json())
    bucket = "native-source-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)
    options = dict(harbor_source=descriptor, db_url=postgres_url, object_store=store, bucket=bucket)
    first = await benchmark_publish.publish_benchmark(**options)
    second = await benchmark_publish.publish_benchmark(**options)
    assert first.inserted == second.unchanged == 1
    assert second.uploaded_objects == 0
    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            benchmark = await session.get(Benchmark, profile)
            task = await session.get(Task, profile + "/upstream/sample")
            assert benchmark.execution_state == "pending"
            assert benchmark.profile_provenance["compatibility"]["blocked_tasks"] == 1
            assert task.config["import_blockers"][0]["code"] == "TASK_COMPAT_BROAD_TRAILING_TRUE"
            assert task.config["verifier"]["environment"]["gpu_types"] == ["H100"]
            assert task.source_provenance["upstream_task_id"] == "upstream/sample"
            assert task.source_provenance["conversion"]["original_config"] == "upstream-task.toml"
            assert task.source_provenance["compatibility"]["status"] == "blocked"
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM task_bundle_source_references WHERE kind='catalog' AND owner_id=:id"
                    ),
                    {"id": task.id},
                )
                == 1
            )
            source_key = task.source.split(bucket + "/", 1)[1]
            assert "loom-task-bundles/v1/" in source_key
            assert (
                admin.get_object(Bucket=bucket, Key=source_key + "upstream-task.toml")["Body"]
                .read()
                .startswith(b"artifacts =")
            )
            original = task.config
        changed = spec(profile).model_copy(
            update={"origin": spec(profile).origin.model_copy(update={"revision": "b" * 40})}
        )
        descriptor.write_text(changed.model_dump_json())
        with pytest.raises(ValueError, match="another upstream origin"):
            await benchmark_publish.publish_benchmark(**options)
        async with async_sessionmaker(engine)() as session:
            task = await session.get(Task, profile + "/upstream/sample")
            assert task.config == original
    finally:
        await engine.dispose()


async def test_concurrent_first_import_cannot_rebind_profile_origin(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine)
    profile = "origin-race@" + uuid4().hex
    entry = AdapterBenchmarkEntry(profile, "Origin race", "fixtures", "MIT")

    async def bind(revision):
        descriptor = spec(profile).model_dump(mode="json")
        descriptor["origin"]["revision"] = revision
        manifest = {
            "splits": ["test"],
            "benchmark_profile_provenance": {"upstream_origin": descriptor["origin"]},
        }
        async with sessions.begin() as session:
            await _upsert_benchmark(
                session,
                entry=entry,
                source_prefix="s3://fixture/tasks/",
                imported_by=None,
                adapter_manifest=manifest,
            )
        return revision

    try:
        results = await asyncio.gather(bind("a" * 40), bind("b" * 40), return_exceptions=True)
        winner = next(result for result in results if isinstance(result, str))
        assert sum(isinstance(result, ValueError) for result in results) == 1
        async with sessions() as session:
            benchmark = await session.get(Benchmark, profile)
            assert benchmark.profile_provenance["upstream_origin"]["revision"] == winner
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="another upstream origin"):
                await _upsert_benchmark(
                    session, entry=entry, source_prefix="s3://local/", imported_by=None
                )
    finally:
        await engine.dispose()
