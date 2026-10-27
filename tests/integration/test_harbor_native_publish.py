"""Real DB/versioned TLS storage publication and concurrent origin binding."""

import asyncio
import hashlib
import json
import os
import shutil
import sys
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_profiles import resolve_benchmark_selectors
from loom.db.schema import (
    Benchmark,
    Task,
    TaskBundleSourceIncarnation,
    TaskBundleSourceReference,
)
from loom.task_bundle_source_journal import retire_task_bundle_source
from loom_cli import benchmark_prepare, benchmark_publish
from loom_cli.benchmark_types import AdapterBenchmarkEntry
from loom_cli.local_benchmark_publish import _upsert_benchmark
from tests.integration.test_task_bundle_source_storage import _store
from tests.support.minio_tls import minio_tls as minio_tls
from tests.unit.test_harbor_native_import import native_tree, spec

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


async def _native_cli(descriptor, postgres_url):
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "loom_cli", "datasets", "activate-native",
        "--harbor-source", str(descriptor),
        env={**os.environ, "LOOM_DB_URL": postgres_url},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


@pytest.fixture
async def native_publication(postgres_url, tmp_path, monkeypatch, minio_tls):
    import tomli_w

    profile = "admission-subset@" + uuid4().hex
    source = tmp_path / "source"
    first = native_tree(source, raw={
        "task": {"name": "upstream/sample"},
        "environment": {"cpus": 1, "memory": "1G", "storage": "1G"},
    })
    second = first.parent / "second"
    shutil.copytree(first, second)
    (second / "task.toml").write_text(tomli_w.dumps({
        "task": {"name": "upstream/second"},
        "environment": {"cpus": 1, "memory": "1G", "storage": "1G"},
    }))
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    descriptor = tmp_path / "source.json"
    document = spec(profile).model_dump(mode="json")
    document["expected_task_count"] = 2
    document["display_name"] = "Native fixture (unqualified)"
    document["origin"]["subset"] = ["upstream/sample", "upstream/second"]
    descriptor.write_text(json.dumps(document))
    bucket = "native-admission-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    options = dict(
        harbor_source=descriptor, db_url=postgres_url, object_store=_store(minio_tls), bucket=bucket,
    )
    await benchmark_publish.publish_benchmark(**options)
    engine = create_async_engine(postgres_url)
    try:
        yield profile, descriptor, async_sessionmaker(engine, expire_on_commit=False), options, first
    finally:
        await engine.dispose()


async def test_native_activation_cli_and_exact_reimport(native_publication, postgres_url):
    profile, descriptor, sessions, options, source_task = native_publication
    async with sessions() as session:
        with pytest.raises(HTTPException) as failure:
            await resolve_benchmark_selectors(session, [profile], require_runnable=True)
        assert failure.value.detail["reason"] == "benchmark_not_runnable"
        original_provenance = deepcopy((await session.get(Benchmark, profile)).profile_provenance)
    code, stdout, stderr = await _native_cli(descriptor, postgres_url)
    assert code == 0, stderr
    assert "2 tasks admitted; runtime qualification unchanged" in stdout
    async with sessions() as session:
        resolved = await resolve_benchmark_selectors(session, [profile], require_runnable=True)
        assert resolved.physical_ids == (profile,)
        benchmark = await session.get(Benchmark, profile)
        assert benchmark.display_name.endswith("(unqualified)")
        assert benchmark.profile_provenance == original_provenance
        assert benchmark.profile_provenance["runtime_verified"] is False
    assert (await _native_cli(descriptor, postgres_url))[0] == 0
    unchanged = await benchmark_publish.publish_benchmark(**options)
    assert unchanged.unchanged == 2 and unchanged.uploaded_objects == 0
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "runnable"
    (source_task / "instruction.md").write_text("Changed native instruction\n")
    changed = await benchmark_publish.publish_benchmark(**options)
    assert changed.updated == 1
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"


@pytest.mark.parametrize("damage,diagnostic", [
    ("missing", "census"), ("origin", "identity"), ("config", "admission blocked"),
    ("source", "frozen snapshot"), ("historical", "cannot be activated"),
    ("subset", "exact source subset"),
])
async def test_native_activation_cli_rejects_invalid_catalog(
    native_publication, postgres_url, damage, diagnostic,
):
    profile, descriptor, sessions, options, _ = native_publication
    async with sessions.begin() as session:
        benchmark = await session.get(Benchmark, profile)
        task = await session.get(Task, profile + "/upstream/sample")
        if damage == "missing":
            await session.delete(task)
        elif damage == "historical":
            benchmark.execution_state = "historical"
        else:
            config = deepcopy(task.config)
            if damage == "origin":
                config["upstream_origin"]["revision"] = "b" * 40
            elif damage == "config":
                config["environment"]["gpu_types"] = ["H100"]
                config["environment"]["gpus"] = 1
            elif damage == "source":
                config["task"]["name"] = "Changed registered config"
            else:
                document = json.loads(descriptor.read_text())
                document["origin"]["subset"] = ["upstream/other", "upstream/second"]
                descriptor.write_text(json.dumps(document))
                provenance = deepcopy(benchmark.profile_provenance)
                provenance["upstream_origin"] = document["origin"]
                benchmark.profile_provenance = provenance
                for row in await session.scalars(select(Task).where(Task.benchmark_id == profile)):
                    row_config = deepcopy(row.config)
                    row_config["upstream_origin"] = document["origin"]
                    row.config = row_config
                    row_provenance = deepcopy(row.source_provenance)
                    row_provenance["upstream_origin"] = document["origin"]
                    row.source_provenance = row_provenance
                config["upstream_origin"] = document["origin"]
            task.config = config
    code, _, stderr = await _native_cli(descriptor, postgres_url)
    assert code == 1 and diagnostic in stderr, stderr
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == (
            "historical" if damage == "historical" else "pending"
        )
    if damage == "historical":
        with pytest.raises(ValueError, match="historical"):
            await benchmark_publish.publish_benchmark(**options)


async def test_native_activation_rolls_back_partial_source_admission(native_publication, postgres_url):
    profile, descriptor, sessions, _, _ = native_publication
    async with sessions.begin() as session:
        tasks = list(await session.scalars(select(Task).where(Task.benchmark_id == profile)))
        source_ids = sorted(hashlib.sha256(task.source.encode()).hexdigest() for task in tasks)
        await session.execute(delete(TaskBundleSourceReference).where(
            TaskBundleSourceReference.source_id.in_(source_ids),
        ))
        incarnation = await session.scalar(select(TaskBundleSourceIncarnation).where(
            TaskBundleSourceIncarnation.source_id == source_ids[-1],
            TaskBundleSourceIncarnation.state == "available",
        ))
        assert await retire_task_bundle_source(
            session, incarnation_id=incarnation.id, now=datetime.now(UTC),
        )
    code, _, stderr = await _native_cli(descriptor, postgres_url)
    assert code == 1 and "available incarnation" in stderr
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"
        references = list(await session.scalars(select(TaskBundleSourceReference).where(
            TaskBundleSourceReference.source_id.in_(source_ids),
        )))
        assert references == []


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
