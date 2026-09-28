"""Integration coverage for user-owned local benchmark publishing (#275)."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark, TaskImageMaterialization
from loom.db.schema import Task as TaskRow
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    FakeObjectStore,
    bundle_file_metadata_sha256,
)
from loom_cli.benchmark_readiness import run_bundle_presence_audit
from loom_cli.local_benchmark_publish import (
    PUBLISH_IMPORTED_BY,
    S3_FOLDER_KIND,
    publish_local_benchmark,
)
from loom_cli.local_benchmark_validate import LocalBenchmarkValidationError

_TASK_TOML = """\
schema_version = "1"

[task]
id = "{tid}"
name = "Sample task {tid}"

[environment]
os = "linux"
docker_image = "python:3.11-alpine"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def _write_layout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "benchmark.toml").write_text(
        "schema_version = 1\n"
        'id = "team-evals"\n'
        'display_name = "Team evaluations"\n'
        'series = "internal"\n'
        'license_spdx = "MIT"\n',
    )
    alpha = root / "tasks" / "alpha"
    alpha.mkdir(parents=True)
    (alpha / "task.toml").write_text(_TASK_TOML.format(tid="alpha"))
    (alpha / "instruction.md").write_text("do alpha\n")


def _write_environment_path_mismatch_layout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "benchmark.toml").write_text(
        "schema_version = 1\n"
        'id = "source-useful-compat"\n'
        'display_name = "Source Useful compat fixture"\n'
        'series = "internal"\n'
        'license_spdx = "MIT"\n',
    )
    task = root / "tasks" / "app-path-missing"
    environment = task / "environment"
    environment.mkdir(parents=True)
    (task / "task.toml").write_text(
        'schema_version = "1"\n'
        "[task]\n"
        'id = "app-path-missing"\n'
        'name = "App path missing"\n'
        "[environment]\n"
        'os = "linux"\n'
        'dockerfile = "environment/Dockerfile"\n'
        "[agent]\n"
        'name = "oracle"\n'
        "[verifier]\n"
        'name = "pytest"\n'
        "[[steps]]\n"
        'name = "main"\n',
    )
    (task / "instruction.md").write_text("do task\n")
    (environment / "setup_repo.sh").write_text("#!/bin/sh\necho setup\n")
    (environment / "Dockerfile").write_text(
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh\n",
    )


class ObjectOnlyStore(FakeObjectStore):
    async def ensure_bucket(self, bucket: str) -> None:
        raise ClientError({"Error": {"Code": "403"}}, "HeadBucket")


@pytest.mark.asyncio
@pytest.mark.parametrize("create_bucket", [False, True], ids=["object-only", "bootstrap"])
async def test_publish_local_benchmark_uploads_and_registers(
    postgres_url: str,
    tmp_path: Path,
    create_bucket: bool,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore() if create_bucket else ObjectOnlyStore()
    task_dir = root / "tasks" / "alpha"
    expected_checksum = task_checksum(task_dir)
    metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix("sha256:")
    revision_prefix = (
        f"team-evals/alpha/.loom-revisions-v2/{expected_checksum}/{metadata_digest}/bundle/"
    )

    stats = await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        **({"create_bucket": True} if create_bucket else {}),
    )

    assert ("loom-benchmarks" in store.buckets) is create_bucket
    assert stats.benchmark_id == "team-evals"
    assert stats.task_count == 1
    assert stats.inserted == 1
    assert stats.updated == 0
    assert stats.unchanged == 0
    assert stats.uploaded_objects == 3
    assert stats.source_prefix == "s3://loom-benchmarks/team-evals/"
    assert ("loom-benchmarks", f"{revision_prefix}task.toml") in store.objects
    assert ("loom-benchmarks", f"{revision_prefix}instruction.md") in store.objects
    manifest_key = f"{revision_prefix.removesuffix('bundle/')}service-execution-input.json"
    assert ("loom-benchmarks", manifest_key) in store.objects
    manifest_body = store.objects[("loom-benchmarks", manifest_key)]
    expected_sei = {
        "schema_version": "loom.service-execution-input.v1",
        "manifest_uri": f"s3://loom-benchmarks/{manifest_key}",
        "manifest_sha256": "sha256:" + hashlib.sha256(manifest_body).hexdigest(),
        "file_count": 2,
        "total_bytes": len((task_dir / "task.toml").read_bytes())
        + len((task_dir / "instruction.md").read_bytes()),
    }

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            bench = (
                await session.execute(
                    select(Benchmark).where(Benchmark.id == "team-evals"),
                )
            ).scalar_one()
            assert bench.display_name == "Team evaluations"
            assert bench.series == "internal"
            assert bench.license_spdx == "MIT"
            assert bench.upstream_kind == S3_FOLDER_KIND
            assert bench.upstream_locator == "s3://loom-benchmarks/team-evals/"
            assert bench.imported_by == PUBLISH_IMPORTED_BY

            task = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert task.source == f"s3://loom-benchmarks/{revision_prefix}"
            assert task.license == "MIT"
            assert task.benchmark_id == "team-evals"
            assert task.config["task"]["id"] == "alpha"
            assert task.checksum == expected_checksum
            assert task.source_provenance == {
                "bundle_file_metadata_sha256": f"sha256:{metadata_digest}",
                "service_execution_input": expected_sei,
            }
            # Audit/worker materialization must see exactly the published task
            # files, not publication metadata added after the bundle upload.
            downloaded = tmp_path / "downloaded"
            await store.download_prefix(
                bucket="loom-benchmarks",
                prefix=task.source.removeprefix("s3://loom-benchmarks/"),
                out_dir=downloaded,
            )
            assert task_checksum(downloaded) == task.checksum
            audit = await run_bundle_presence_audit(
                db_url=postgres_url, object_store=store, benchmark="team-evals",
            )
            assert audit.verified == 1 and audit.failed == 0
    finally:
        async with factory() as session:
            await session.execute(
                delete(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "team-evals"),
            )
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_failed_database_commit_does_not_overwrite_live_task_bundle(
    postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()

    await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            before = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            before_checksum = before.checksum
            before_source = before.source
        assert before_source is not None
        before_prefix = before_source.removeprefix("s3://loom-benchmarks/")
        before_key = f"{before_prefix}instruction.md"
        assert store.objects[("loom-benchmarks", before_key)] == b"do alpha\n"

        task_dir = root / "tasks" / "alpha"
        (task_dir / "instruction.md").write_text("do revised alpha\n")
        revised_checksum = task_checksum(task_dir)
        revised_metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix(
            "sha256:",
        )

        async def reject_commit(_session: AsyncSession) -> None:
            raise RuntimeError("injected database commit failure")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", reject_commit)
            with pytest.raises(RuntimeError, match="injected database commit failure"):
                await publish_local_benchmark(
                    root,
                    db_url=postgres_url,
                    object_store=store,
                    bucket="loom-benchmarks",
                )

        async with factory() as session:
            after = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert after.checksum == before_checksum
            assert after.source == before_source

        assert store.objects[("loom-benchmarks", before_key)] == b"do alpha\n"
        revised_key = (
            "team-evals/alpha/.loom-revisions-v2/"
            f"{revised_checksum}/{revised_metadata_digest}/bundle/instruction.md"
        )
        assert store.objects[("loom-benchmarks", revised_key)] == b"do revised alpha\n"
    finally:
        async with factory() as session:
            await session.execute(
                delete(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "team-evals"),
            )
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_mode_only_revision_does_not_overwrite_live_transport_metadata(
    postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()

    await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            before = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            before_checksum = before.checksum
            before_source = before.source
        assert before_source is not None
        before_prefix = before_source.removeprefix("s3://loom-benchmarks/")
        before_metadata_key = f"{before_prefix}{BUNDLE_FILE_METADATA_NAME}"
        before_metadata = store.objects[("loom-benchmarks", before_metadata_key)]

        task_dir = root / "tasks" / "alpha"
        instruction = task_dir / "instruction.md"
        instruction.chmod(0o755)
        assert task_checksum(task_dir) == before_checksum
        revised_metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix(
            "sha256:",
        )

        async def reject_commit(_session: AsyncSession) -> None:
            raise RuntimeError("injected database commit failure")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", reject_commit)
            with pytest.raises(RuntimeError, match="injected database commit failure"):
                await publish_local_benchmark(
                    root,
                    db_url=postgres_url,
                    object_store=store,
                    bucket="loom-benchmarks",
                )

        async with factory() as session:
            after = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert after.checksum == before_checksum
            assert after.source == before_source

        assert store.objects[("loom-benchmarks", before_metadata_key)] == before_metadata
        revised_metadata_key = (
            "team-evals/alpha/.loom-revisions-v2/"
            f"{before_checksum}/{revised_metadata_digest}/bundle/"
            f"{BUNDLE_FILE_METADATA_NAME}"
        )
        assert store.objects[("loom-benchmarks", revised_metadata_key)] != before_metadata
    finally:
        async with factory() as session:
            await session.execute(
                delete(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "team-evals"),
            )
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_local_benchmark_rejects_environment_path_mismatch_by_default(
    postgres_url: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "source-useful-compat"
    _write_environment_path_mismatch_layout(root)
    store = FakeObjectStore()

    with pytest.raises(LocalBenchmarkValidationError) as excinfo:
        await publish_local_benchmark(
            root,
            db_url=postgres_url,
            object_store=store,
            bucket="loom-benchmarks",
        )

    message = str(excinfo.value)
    assert "TASK_COMPAT_APP_PATH_MISSING" in message
    assert "environment/setup_repo.sh" in message
    assert store.objects == {}

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            bench = (
                await session.execute(
                    select(Benchmark).where(Benchmark.id == "source-useful-compat"),
                )
            ).scalar_one_or_none()
            tasks = (
                (
                    await session.execute(
                        select(TaskRow).where(
                            TaskRow.benchmark_id == "source-useful-compat",
                        ),
                    )
                )
                .scalars()
                .all()
            )
            assert bench is None
            assert tasks == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_local_explicit_flatten_override_records_evidence(
    postgres_url: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "source-useful-compat"
    _write_environment_path_mismatch_layout(root)
    store = FakeObjectStore()

    stats = await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        compat_flatten_environment=True,
    )

    assert stats.benchmark_id == "source-useful-compat"
    assert stats.task_count == 1
    assert stats.compat_flattened_files == 2
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            task = (
                await session.execute(
                    select(TaskRow).where(
                        TaskRow.id == "source-useful-compat/app-path-missing",
                    ),
                )
            ).scalar_one()
            source_prefix = task.source.removeprefix("s3://loom-benchmarks/")
            revision_prefix = (
                f"source-useful-compat/app-path-missing/.loom-revisions-v2/{task.checksum}/"
            )
            assert source_prefix.startswith(revision_prefix)
            metadata_digest = source_prefix.removeprefix(revision_prefix).removesuffix(
                "/bundle/",
            )
            assert len(metadata_digest) == 64
            assert set(metadata_digest) <= set("0123456789abcdef")
            assert (
                "loom-benchmarks",
                f"{source_prefix}setup_repo.sh",
            ) in store.objects
            assert (
                "loom-benchmarks",
                f"{source_prefix}environment/setup_repo.sh",
            ) in store.objects
    finally:
        async with factory() as session:
            # Publication queues prerequisites independently of the Task FK graph.
            # Remove every revision in this test's namespace before its tasks.
            await session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.startswith("source-useful-compat/")
                ),
            )
            await session.execute(
                delete(TaskRow).where(
                    TaskRow.benchmark_id == "source-useful-compat",
                ),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "source-useful-compat"),
            )
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["AccessDenied", "NoSuchBucket"])
async def test_publish_object_failure_propagates_without_database_commit(
    postgres_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_code: str,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = ObjectOnlyStore()
    original_put = store.put_object
    put_count = 0

    async def reject_second_put(*, bucket: str, key: str, body: bytes) -> None:
        nonlocal put_count
        put_count += 1
        if put_count == 2:
            raise ClientError({"Error": {"Code": error_code}}, "PutObject")
        await original_put(bucket=bucket, key=key, body=body)

    monkeypatch.setattr(store, "put_object", reject_second_put)
    engine = create_async_engine(postgres_url)
    try:
        with pytest.raises(ClientError) as error:
            await publish_local_benchmark(
                root, db_url=postgres_url, object_store=store, bucket="loom-benchmarks",
            )
        assert error.value.operation_name == "PutObject"
        assert error.value.response["Error"]["Code"] == error_code
        assert put_count == 2
        async with async_sessionmaker(engine)() as session:
            assert await session.get(Benchmark, "team-evals") is None
            assert await session.get(TaskRow, "team-evals/alpha") is None
    finally:
        await engine.dispose()


_HARBOR_TASK_TOML = """\
schema_version = "1.1"

[task]
name = "terminal-bench/harbor-sample"
description = "Harbor-shaped sample for Nebius profile"

[environment]
dockerfile = "environment/Dockerfile"
docker_build_context = "environment"
build_timeout_sec = 600.0
user = "root"
architecture = "x86_64"
allow_internet = true

[agent]
timeout_sec = 900.0

[verifier]
timeout_sec = 900.0
user = "root"
"""


def _write_harbor_layout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "benchmark.toml").write_text(
        "schema_version = 1\n"
        'id = "harbor-nebius-profile"\n'
        'display_name = "Harbor Nebius profile fixture"\n'
        'series = "terminal-bench"\n'
        'license_spdx = "MIT"\n',
    )
    task = root / "tasks" / "harbor-sample"
    environment = task / "environment"
    environment.mkdir(parents=True)
    (task / "task.toml").write_text(_HARBOR_TASK_TOML)
    (task / "instruction.md").write_text("do the harbor thing\n")
    (environment / "Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /app\n")
    tests = task / "tests"
    tests.mkdir()
    (tests / "test_outputs.py").write_text("def test_ok():\n    assert True\n")
    (tests / "test.sh").write_text(
        "#!/bin/bash\napt-get update\napt-get install -y curl\n"
        "curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n"
        "source $HOME/.local/bin/env\n"
        "uvx -p 3.13 -w pytest==8.4.1 -w pytest-json-ctrf==0.3.5 "
        "pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA\n"
        "if [ $? -eq 0 ]; then echo 1 > /logs/verifier/reward.txt; "
        "else echo 0 > /logs/verifier/reward.txt; fi\n",
    )
    online = task / "verifier" / "run.sh"
    online.parent.mkdir()
    online.write_text(
        "#!/bin/sh\n"
        "echo 'harbor loom bridge: tests/test.sh not found' >&2\n"
        "python3 -m pip install -q pytest\n",
        encoding="utf-8",
    )
    online.chmod(0o755)


@pytest.fixture
async def harbor_publication_cleanup(postgres_url: str) -> AsyncIterator[None]:
    """Keep these publications from leaving work in the shared test database."""
    engine = create_async_engine(postgres_url)
    try:
        yield
    finally:
        try:
            async with async_sessionmaker(engine)() as session:
                await session.execute(delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id == "harbor-nebius-profile/harbor-sample",
                ))
                await session.execute(delete(TaskRow).where(
                    TaskRow.benchmark_id == "harbor-nebius-profile",
                ))
                await session.execute(delete(Benchmark).where(
                    Benchmark.id == "harbor-nebius-profile",
                ))
                await session.commit()
        finally:
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.usefixtures("harbor_publication_cleanup")
async def test_publish_nebius_terminus_profile_adapts_harbor_pack(
    postgres_url: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "harbor-nebius-profile"
    _write_harbor_layout(root)
    store = ObjectOnlyStore()

    stats = await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        execution_profile="nebius-terminus",
    )

    assert stats.execution_profile == "nebius-terminus"
    assert stats.profile_stats is not None
    assert stats.profile_stats.adapted_tasks == 1
    assert stats.profile_stats.verifier_wrappers_installed == 1
    assert stats.profile_stats.preflight_passed == 1
    assert any(
        key.endswith("bundle/verifier/run.sh")
        for (_bucket, key) in store.objects
    )
    wrapper_bodies = [
        body
        for (_bucket, key), body in store.objects.items()
        if key.endswith("bundle/verifier/run.sh")
    ]
    assert wrapper_bodies
    assert b"harbor-offline.sh" in wrapper_bodies[0]
    assert b"pip install" not in wrapper_bodies[0]
    assert any(key.endswith("bundle/environment/Dockerfile.loom-nebius")
               for (_bucket, key) in store.objects)
    assert any(b"/opt/verifier/bin/pytest" in body
               for (_bucket, key), body in store.objects.items()
               if key.endswith("bundle/verifier/harbor-offline.sh"))

    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            task = (
                await session.execute(
                    select(TaskRow).where(
                        TaskRow.id == "harbor-nebius-profile/harbor-sample",
                    ),
                )
            ).scalar_one()
            env = task.config["environment"]
            assert env["cpu_arch"] == "x86_64"
            # Publication preserves each source identity; runtime admission
            # separately requires the constrained private-root policy.
            assert env["user"] == "root"
            assert env["workdir"] == "/app"
            assert env["dockerfile"] == "environment/Dockerfile.loom-nebius"
            assert env["cpus"] == 1
            assert env["memory_mb"] == 2048
            assert env["storage_mb"] == 4096
            assert env["network_policies_supported"] == ["gateway-only"]
            assert env["baseline_network_policy"] == {"kind": "gateway-only"}
            assert task.config["verifier"]["user"] == "root"
            assert task.config["verifier"]["env_mode"] == "separate"
            assert task.config["verifier"]["args"]["script_path"] == "verifier/run.sh"
            assert "service_execution_input" in task.source_provenance
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.usefixtures("harbor_publication_cleanup")
async def test_publish_without_profile_keeps_harbor_root_verifier(
    postgres_url: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "harbor-nebius-profile"
    _write_harbor_layout(root)
    store = ObjectOnlyStore()

    await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            task = (
                await session.execute(
                    select(TaskRow).where(
                        TaskRow.id == "harbor-nebius-profile/harbor-sample",
                    ),
                )
            ).scalar_one()
            assert task.config["verifier"].get("user") == "root"
            assert task.config["verifier"]["args"]["script_path"] == (
                "/app/verifier/run.sh"
            )
            assert task.config["environment"].get("cpu_arch") == "x86_64"
            assert task.config["environment"].get("network_policies_supported") != [
                "gateway-only"
            ]
            assert task.config["environment"].get("user") == "root"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("physical_profile", [False, True])
async def test_adapter_publication_preserves_catalog_and_execution_contract(
    postgres_url, tmp_path, monkeypatch, capsys, physical_profile,
):
    from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource

    from loom.trial.workspace import TB21_AGENT_WORKSPACE_POLICY
    from loom_cli import benchmark_prepare
    from loom_cli.benchmark_publish import publish_benchmark
    from loom_cli.datasets_cmd import dispatch

    benchmark_id = "terminal-bench-2@tb2.1-r6" if physical_profile else "unified-adapter"
    task_id = f"{benchmark_id}/HumanEval/0"

    class Adapter:
        name = benchmark_id
        display_name = "Unified adapter fixture"
        series = "test"
        license_spdx = "MIT"
        license_url = "https://example.test/license"
        splits = ("test",)
        upstream_source = UpstreamSource(kind="git", locator="https://example.test/upstream", revision="abc")
        instruction = "original"

        def list_instances(self, **kwargs):
            yield BenchmarkInstance("HumanEval/0", "test", {}, {"language": "python"})

        def convert_instance(self, instance, *, out_dir):
            (out_dir / "task.toml").write_text(_TASK_TOML.format(tid=task_id))
            (out_dir / "instruction.md").write_text(self.instruction)
            script = out_dir / "verifier" / "run.sh"
            script.parent.mkdir()
            script.write_text("#!/bin/sh\nexit 0\n")
            script.chmod(0o755)
            return ConvertedTask(task_id, task_checksum(out_dir), "MIT", ())

        def profile_provenance(self):
            return {"workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY}

        def task_source_provenance(self, **kwargs):
            script = kwargs["bundle_dir"] / "verifier" / "run.sh"
            return {
                "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
                "verifier_asset": {
                    "script_path": "/app/verifier/run.sh", "mode": "0755",
                    "sha256": "sha256:" + hashlib.sha256(script.read_bytes()).hexdigest(),
                },
            }

    adapter = Adapter()
    monkeypatch.setitem(benchmark_prepare.REGISTRY, benchmark_id, adapter)
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kw: tmp_path)
    store = FakeObjectStore()
    args = dict(benchmark=benchmark_id, cache_dir=tmp_path / "cache", db_url=postgres_url,
                object_store=store, bucket="benchmark-test")
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", lambda **kwargs: store)
    monkeypatch.setenv("LOOM_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_MINIO_ACCESS_KEY", "fixture-access")
    monkeypatch.setenv("LOOM_MINIO_SECRET_KEY", "fixture-secret")
    command = [
        "publish", "--benchmark", benchmark_id, "--cache-dir", str(tmp_path / "cache"),
        "--minio-endpoint", "https://example.test", "--bucket", "benchmark-test",
    ]
    assert await asyncio.to_thread(dispatch, command) == 0
    assert "inserted=1" in capsys.readouterr().out
    again = await publish_benchmark(**args)
    assert again.unchanged == 1
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            task = await session.get(TaskRow, task_id)
            benchmark = await session.get(Benchmark, benchmark_id)
            assert task.config["task"]["id"] == task_id
            assert task.tags["split"] == "test"
            assert task.tags["language"] == "python"
            assert task.source_provenance["workspace_staging_policy"] == TB21_AGENT_WORKSPACE_POLICY
            assert task.source_provenance["service_execution_input"]
            assert "/HumanEval/0/.loom-revisions-v2/" in task.source
            assert benchmark.upstream_locator == adapter.upstream_source.locator
            assert benchmark.upstream_revision == "abc"
            assert benchmark.splits == ["test"]
            if physical_profile:
                assert benchmark.execution_state == "pending"
        if physical_profile:
            adapter.instruction = "changed"
            assert await asyncio.to_thread(dispatch, command) == 1
            assert "immutable TB2.1 physical profile drift" in capsys.readouterr().err
    finally:
        await engine.dispose()
