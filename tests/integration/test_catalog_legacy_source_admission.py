"""Actual legacy register → canonical CLI with PostgreSQL and TLS object bytes."""

import asyncio
from copy import deepcopy
from uuid import uuid4

import pytest
import tomli_w
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark, BenchmarkAlias, Task
from loom.models.benchmark_profile import BenchmarkProfileSpec
from loom.models.task_checksum import task_checksum
from loom_benchmark_tool.register_cmd import run_register
from loom_benchmark_tool.upload import upload_task_dir
from loom_cli.datasets_cmd import dispatch
from tests.integration.test_task_bundle_source_storage import _store
from tests.support.minio_tls import minio_tls as minio_tls

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def _bundle(root, identity):
    root.mkdir(parents=True)
    config = {
        "schema_version": "1", "task": {"id": identity, "name": "Example"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"}, "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }
    (root / "task.toml").write_text(tomli_w.dumps(config))
    (root / "instruction.md").write_text("Do the original task.\n")
    return config


@pytest.fixture
async def legacy_catalog(postgres_url, minio_tls, tmp_path, monkeypatch):
    profile = "legacy-adapter-" + uuid4().hex
    bucket = "legacy-catalog-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)
    task_entries = []
    for name in ("first", "second"):
        bundle = tmp_path / "bundles" / name
        config = _bundle(bundle, profile + "/" + name)
        await upload_task_dir(store=store, bucket=bucket,
                              prefix=f"{profile}/catalog-v1/tasks/{name}/", task_dir=bundle)
        task_entries.append({
            "task_id": profile + "/" + name, "instance_id": name,
            "hf_path": f"tasks/{name}/", "checksum": task_checksum(bundle),
            "license_spdx": "MIT", "split": "test", "task_config": config,
        })
    manifest = {
        "manifest_version": 2, "benchmark_id": profile, "display_name": "Legacy adapter",
        "series": "test", "upstream_kind": "json", "upstream_locator": "fixture:canonical-json",
        "upstream_revision": "revision-one", "license_spdx": "MIT",
        "license_url": "https://example.test/license", "splits": ["test"],
        "tasks": task_entries, "task_count": len(task_entries),
    }
    await run_register(benchmark=profile, source="object-store", db_url=postgres_url,
                       revision="catalog-v1", object_store=store, bucket=bucket, manifest=manifest)
    spec = BenchmarkProfileSpec(
        id=profile, origin={"kind": "json", "locator": "fixture:canonical-json",
                            "revision": "revision-one", "release": "one", "importer": "loom.canonical.v1"},
        expected_task_count=2, selected_task_ids=("first", "second"),
    )
    descriptor = tmp_path / "profile.json"
    descriptor.write_text(spec.model_dump_json())
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", lambda **kwargs: store)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    arguments = ["activate", "--profile", profile, "--descriptor", str(descriptor),
                 "--db-url", postgres_url, "--minio-endpoint", minio_tls[0],
                 "--minio-access-key", minio_tls[1].access_key,
                 "--minio-secret-key", minio_tls[1].secret_key]
    try:
        yield profile, bucket, admin, sessions, arguments, spec
    finally:
        await engine.dispose()


async def test_legacy_register_then_canonical_cli_admits_actual_bundles(legacy_catalog, capsys):
    profile, _bucket, _admin, sessions, arguments, _spec = legacy_catalog
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"
    assert await asyncio.to_thread(dispatch, arguments) == 0
    assert "2 tasks admitted; runtime qualification unchanged" in capsys.readouterr().out
    async with sessions() as session:
        benchmark = await session.get(Benchmark, profile)
        assert benchmark.execution_state == "runnable"
        assert "qualification" not in benchmark.profile_provenance
    assert await asyncio.to_thread(dispatch, arguments) == 0


@pytest.mark.parametrize("damage,reason", [
    ("bytes", "legacy_source_checksum_mismatch"),
    ("config", "legacy_source_config_drift"),
    ("strong_marker", "source_unavailable"),
])
async def test_legacy_source_audit_rejects_tamper_without_partial_admission(
    legacy_catalog, capsys, damage, reason,
):
    profile, bucket, admin, sessions, arguments, _spec = legacy_catalog
    if damage == "bytes":
        admin.put_object(Bucket=bucket, Key=f"{profile}/catalog-v1/tasks/second/instruction.md",
                         Body=b"Changed after registration\n")
    else:
        async with sessions.begin() as session:
            row = await session.get(Task, profile + "/second")
            if damage == "config":
                config = deepcopy(row.config)
                config["task"]["name"] = "Config changed without authored bytes"
                row.config = config
            else:
                row.source_provenance = {"bundle_content_manifest_sha256": "a" * 64}
    alias = "legacy-public-" + uuid4().hex
    assert await asyncio.to_thread(dispatch, [*arguments, "--alias", alias, "--expect-absent"]) == 1
    assert reason in capsys.readouterr().err
    async with sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"
        assert await session.get(BenchmarkAlias, alias) is None


async def test_legacy_receipt_rechecks_observed_catalog_after_byte_audit(legacy_catalog):
    from loom.benchmark_admission import BenchmarkAdmissionError, admit_benchmark_profile
    from loom.benchmark_admission_legacy_sources import prepare_legacy_source_validation

    profile, _bucket, _admin, sessions, _arguments, spec = legacy_catalog
    # Obtain fixture-owned TLS storage via the exact factory installed by fixture.
    from loom.trajectory.storage import MinioObjectStore
    store = MinioObjectStore()
    async with sessions.begin() as session:
        receipt = await prepare_legacy_source_validation(session, spec=spec, object_store=store)
    async with sessions.begin() as session:
        row = await session.get(Task, profile + "/first")
        row.checksum = "b" * 64
    with pytest.raises(BenchmarkAdmissionError, match="adapter_snapshot_drift"):
        async with sessions.begin() as session:
            await admit_benchmark_profile(session, spec=spec, validation=receipt)


@pytest.mark.parametrize("revision,success", [("a" * 40, True), ("main", False)])
async def test_legacy_hub_source_requires_immutable_materialization(
    postgres_url, tmp_path, monkeypatch, capsys, revision, success,
):
    profile = "hub-source-" + uuid4().hex
    bundle = tmp_path / "snapshot" / "tasks" / "first"
    config = _bundle(bundle, profile + "/first")
    manifest = {
        "manifest_version": 2, "benchmark_id": profile, "display_name": "Hub source",
        "upstream_kind": "json", "upstream_locator": "fixture:hub-source",
        "upstream_revision": "revision-one", "license_spdx": "MIT", "license_url": "",
        "splits": ["test"], "tasks": [{
            "task_id": profile + "/first", "instance_id": "first", "hf_path": "tasks/first/",
            "checksum": task_checksum(bundle), "license_spdx": "MIT", "split": "test",
            "task_config": config,
        }],
    }
    await run_register(benchmark=profile, source="hf", hf_org="fixture-org", revision=revision,
                       db_url=postgres_url, manifest=manifest)
    observations = []

    def snapshot(**kwargs):
        observations.append(kwargs)
        assert success, "mutable Hub revisions must be rejected before a download"
        assert kwargs["revision"] == revision
        assert kwargs["allow_patterns"] == ["tasks/first/*"]
        return str(tmp_path / "snapshot")

    monkeypatch.setattr("huggingface_hub.snapshot_download", snapshot)
    descriptor = tmp_path / "profile.json"
    descriptor.write_text(BenchmarkProfileSpec(
        id=profile, origin={"kind": "json", "locator": "fixture:hub-source", "revision": "revision-one",
                            "release": "one", "importer": "loom.canonical.v1"},
        expected_task_count=1, selected_task_ids=("first",),
    ).model_dump_json())
    code = await asyncio.to_thread(dispatch, ["activate", "--profile", profile,
                                  "--descriptor", str(descriptor), "--db-url", postgres_url])
    assert code == (0 if success else 1)
    if not success:
        assert "legacy_source_unpinned" in capsys.readouterr().err
    assert bool(observations) == success


async def test_ordinary_local_folder_publication_reaches_canonical_admission(
    postgres_url, minio_tls, tmp_path, monkeypatch,
):
    from loom_cli.local_benchmark_publish import publish_local_benchmark

    profile = "local-source-" + uuid4().hex
    root = tmp_path / "published"
    _bundle(root / "tasks" / "first", "first")
    bucket = "local-source-" + uuid4().hex
    minio_tls[3].create_bucket(Bucket=bucket)
    minio_tls[3].put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)
    await publish_local_benchmark(
        root, benchmark_id=profile, display_name="Local source", series="test", license_spdx="MIT",
        source_subdir="tasks", db_url=postgres_url, object_store=store, bucket=bucket,
    )
    descriptor = tmp_path / "local-profile.json"
    descriptor.write_text(BenchmarkProfileSpec(
        id=profile, origin={"kind": "s3-folder", "locator": f"s3://{bucket}/{profile}/",
                            "revision": "", "release": "local-one", "importer": "loom.canonical.v1"},
        expected_task_count=1, selected_task_ids=("first",),
    ).model_dump_json())
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", lambda **kwargs: store)
    assert await asyncio.to_thread(dispatch, [
        "activate", "--profile", profile, "--descriptor", str(descriptor), "--db-url", postgres_url,
        "--minio-endpoint", minio_tls[0], "--minio-access-key", minio_tls[1].access_key,
        "--minio-secret-key", minio_tls[1].secret_key,
    ]) == 0


@pytest.mark.parametrize("root_task", [False, True])
async def test_fixture_sync_uses_explicit_root_and_canonical_admission(
    postgres_url, tmp_path, monkeypatch, capsys, root_task,
):
    from loom.config.benchmarks import load_benchmarks_config
    from loom_cli.benchmarks_sync import sync
    from tests.integration.test_benchmarks_sync_local import _write_bundle, _write_toml

    profile = "fixture-source-" + uuid4().hex
    fixtures_root = tmp_path / "fixtures"
    if root_task:
        _bundle(fixtures_root / profile, profile)
    else:
        _write_bundle(fixtures_root / profile, "first")
    config_path = tmp_path / "benchmarks.toml"
    _write_toml(config_path, profile)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            await sync(load_benchmarks_config(config_path), fixtures_root=fixtures_root,
                       session=session, registry_names=set())
        descriptor = tmp_path / "fixture-profile.json"
        descriptor.write_text(BenchmarkProfileSpec(
            id=profile, origin={"kind": "local-folder", "locator": str(fixtures_root / profile),
                                "revision": "", "release": "local-one", "importer": "loom.canonical.v1"},
            expected_task_count=1, selected_task_ids=("root" if root_task else "first",),
            catalog_task_ids=(profile,) if root_task else None,
        ).model_dump_json())
        arguments = ["activate", "--profile", profile, "--descriptor", str(descriptor), "--db-url", postgres_url]
        monkeypatch.delenv("LOOM_WORKER_FIXTURES_ROOT", raising=False)
        assert await asyncio.to_thread(dispatch, arguments) == 1
        assert "source_access_required" in capsys.readouterr().err
        assert await asyncio.to_thread(dispatch, [*arguments, "--fixtures-root", str(fixtures_root)]) == 0
        async with sessions() as session:
            assert (await session.get(Benchmark, profile)).execution_state == "runnable"
    finally:
        await engine.dispose()


async def test_fixture_source_cannot_escape_explicit_operator_root(postgres_url, tmp_path, capsys):
    from loom.config.benchmarks import load_benchmarks_config
    from loom_cli.benchmarks_sync import sync
    from tests.integration.test_benchmarks_sync_local import _write_bundle, _write_toml

    profile = "fixture-boundary-" + uuid4().hex
    root = tmp_path / "fixtures"
    _write_bundle(root / profile, "first")
    outside = tmp_path / "outside"
    _bundle(outside, profile + "/first")
    (root / "escape").symlink_to(outside, target_is_directory=True)
    config_path = tmp_path / "benchmarks.toml"
    _write_toml(config_path, profile)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            await sync(load_benchmarks_config(config_path), fixtures_root=root,
                       session=session, registry_names=set())
        descriptor = tmp_path / "profile.json"
        descriptor.write_text(BenchmarkProfileSpec(
            id=profile, origin={"kind": "local-folder", "locator": str(root / profile),
                                "revision": "", "release": "one", "importer": "loom.canonical.v1"},
            expected_task_count=1, selected_task_ids=("first",),
        ).model_dump_json())
        for source in ("fixture://../outside", "fixture://escape"):
            async with sessions.begin() as session:
                row = await session.get(Task, profile + "/first")
                row.source = source
            assert await asyncio.to_thread(dispatch, [
                "activate", "--profile", profile, "--descriptor", str(descriptor),
                "--db-url", postgres_url, "--fixtures-root", str(root),
            ]) == 1
            diagnostic = capsys.readouterr().err
            assert "legacy_source_invalid" in diagnostic
            assert str(outside) not in diagnostic
        async with sessions() as session:
            assert (await session.get(Benchmark, profile)).execution_state == "pending"
    finally:
        await engine.dispose()
