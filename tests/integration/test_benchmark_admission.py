"""Catalog admission and competing alias updates use disposable PostgreSQL."""

import asyncio
from copy import deepcopy
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.benchmark_admission import (
    AdapterValidation,
    AliasPromotion,
    BenchmarkAdmissionError,
    admit_benchmark_profile,
    promote_benchmark_alias,
    snapshot_profile,
    snapshot_task,
)
from loom.db.schema import Benchmark, BenchmarkAlias, Task
from loom.models.benchmark_profile import BenchmarkProfileSpec
from tests.unit.test_benchmark_readiness import _valid_task_config

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]


@pytest.fixture(scope="module")
def admission_postgres():
    with PostgresContainer("postgres:16") as database:
        yield database.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")


@pytest.fixture
async def catalog_sessions(admission_postgres):
    engine = create_async_engine(admission_postgres)
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: Benchmark.__table__.create(sync, checkfirst=True))
        await connection.run_sync(lambda sync: BenchmarkAlias.__table__.create(sync, checkfirst=True))
        await connection.execute(text("""
            CREATE TABLE IF NOT EXISTS tasks (
                id varchar PRIMARY KEY, checksum varchar NOT NULL, legacy_separate_verifier_checksum varchar(64),
                config jsonb NOT NULL, source varchar, license varchar, benchmark_id text REFERENCES benchmarks(id),
                task_set_id text, tags jsonb NOT NULL DEFAULT '{}', source_provenance jsonb NOT NULL DEFAULT '{}',
                registered_at timestamptz NOT NULL DEFAULT now()
            )
        """))
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def publish_inline(sessions, *, tasks=("one", "two")):
    profile_id = "arbitrary-json@" + uuid4().hex
    origin = dict(kind="json", locator="https://example.test/tasks.json",
                  revision="sha256:" + "a" * 64, release="7", importer="loom.json.v1")
    spec = BenchmarkProfileSpec(id=profile_id, origin=origin,
                                expected_task_count=len(tasks), selected_task_ids=tasks)
    async with sessions.begin() as session:
        session.add(Benchmark(
            id=profile_id, display_name="New adapter without a core name change", series=None,
            upstream_kind=origin["kind"], upstream_locator=origin["locator"],
            upstream_revision=origin["revision"], license_spdx="MIT", license_url="https://example.test/license",
            splits=["test"], execution_state="pending", profile_provenance={"upstream_origin": origin},
        ))
        await session.flush()
        session.add_all([
            Task(id=profile_id + "/" + task_id, benchmark_id=profile_id, checksum="a" * 64,
                 config=_valid_task_config(profile_id + "/" + task_id), source=None,
                 source_provenance={}, tags={}) for task_id in tasks
        ])
    return spec


async def test_non_harbor_admission_and_alias_failure_roll_back_together(catalog_sessions):
    spec = await publish_inline(catalog_sessions)
    alias = "public-" + uuid4().hex
    async with catalog_sessions.begin() as session:
        with pytest.raises(BenchmarkAdmissionError, match="alias_conflict"):
            await admit_benchmark_profile(session, spec=spec,
                alias=AliasPromotion(alias, spec.id, "unexpected-current"))
        # Catching an error inside a transaction cannot accidentally admit it.
        assert (await session.get(Benchmark, spec.id)).execution_state == "pending"
        assert await session.get(BenchmarkAlias, alias) is None
        assert await admit_benchmark_profile(session, spec=spec,
                alias=AliasPromotion(alias, spec.id, None)) == 2
    async with catalog_sessions() as session:
        assert (await session.get(Benchmark, spec.id)).execution_state == "runnable"
        assert (await session.get(BenchmarkAlias, alias)).benchmark_id == spec.id


async def test_exact_selection_and_common_import_blockers_reject(catalog_sessions):
    spec = await publish_inline(catalog_sessions)
    async with catalog_sessions.begin() as session:
        row = await session.get(Task, spec.id + "/one")
        document = deepcopy(row.config)
        document["task"]["id"] = spec.id + "/wrong"
        row.config = document
    async with catalog_sessions.begin() as session:
        with pytest.raises(BenchmarkAdmissionError, match="task_identity_drift"):
            await admit_benchmark_profile(session, spec=spec)
    async with catalog_sessions() as session:
        assert (await session.get(Benchmark, spec.id)).execution_state == "pending"


async def test_unknown_required_validator_does_not_enable_uploaded_bypass(catalog_sessions):
    spec = await publish_inline(catalog_sessions)
    async with catalog_sessions.begin() as session:
        profile = await session.get(Benchmark, spec.id)
        provenance = deepcopy(profile.profile_provenance)
        provenance["validation_contract"] = "unreviewed.v1"
        profile.profile_provenance = provenance
    async with catalog_sessions.begin() as session:
        with pytest.raises(BenchmarkAdmissionError, match="validation_contract_unknown"):
            await admit_benchmark_profile(session, spec=spec)


async def test_two_concurrent_alias_updates_have_one_winner(catalog_sessions):
    first, second = await publish_inline(catalog_sessions), await publish_inline(catalog_sessions)
    for spec in (first, second):
        async with catalog_sessions.begin() as session:
            await admit_benchmark_profile(session, spec=spec)
    alias = "competing-" + uuid4().hex

    async def promote(spec):
        async with catalog_sessions.begin() as session:
            try:
                await promote_benchmark_alias(session, alias=alias, profile_id=spec.id,
                                              expected_current=None)
                return spec.id
            except BenchmarkAdmissionError as error:
                assert error.findings[0].code == "alias_conflict"
                return None

    results = await asyncio.gather(promote(first), promote(second))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    async with catalog_sessions() as session:
        assert (await session.get(BenchmarkAlias, alias)).benchmark_id == winners[0]


async def test_adapter_receipt_rejects_concurrent_config_drift(catalog_sessions):
    spec = await publish_inline(catalog_sessions)
    async with catalog_sessions() as session:
        profile = await session.get(Benchmark, spec.id)
        rows = list(await session.scalars(select(Task).where(Task.benchmark_id == spec.id)))
        validation = AdapterValidation(contract="canonical.v1", profile_facts=snapshot_profile(profile),
                                        task_facts={row.id: snapshot_task(row) for row in rows})
    async with catalog_sessions.begin() as session:
        row = await session.get(Task, spec.id + "/one")
        row.config = {**row.config, "task": {**row.config["task"], "name": "semantic update"}}
    async with catalog_sessions.begin() as session:
        with pytest.raises(BenchmarkAdmissionError, match="adapter_snapshot_drift"):
            await admit_benchmark_profile(session, spec=spec, validation=validation)


async def _activate_cli(spec, descriptor, db_url, *arguments):
    import os
    import sys

    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "loom_cli", "datasets", "activate", "--profile", spec.id,
        "--descriptor", str(descriptor), "--db-url", db_url, *arguments,
        env=os.environ.copy(), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


from tests.support.minio_tls import minio_tls  # noqa: E402,F401


async def test_non_harbor_cli_uses_versioned_sources_and_atomic_alias_cas(
    postgres_url, tmp_path, minio_tls,  # noqa: F811
):
    import tomli_w

    from loom_cli.benchmark_types import AdapterBenchmarkEntry, PreparedAdapterBenchmark
    from loom_cli.local_benchmark_source_publish import publish_versioned_local_benchmark
    from tests.integration.test_task_bundle_source_storage import _store

    profile = "fresh-json-adapter@" + uuid4().hex
    origin = dict(kind="json", locator="https://example.test/exam.json",
                  revision="sha256:" + "b" * 64, release="9", importer="loom.json.v1")
    task_root = tmp_path / "json-source"
    task_tomls, adapter_tasks = [], {}
    for identity in ("question-1", "question-2"):
        folder = task_root / identity
        folder.mkdir(parents=True)
        config = _valid_task_config(profile + "/" + identity)
        path = folder / "task.toml"
        path.write_text(tomli_w.dumps(config))
        (folder / "instruction.md").write_text("Solve the original JSON question.")
        task_tomls.append(path)
        adapter_tasks[identity] = {"task_id": profile + "/" + identity, "tags": {}, "source_provenance": {}}
    prepared = PreparedAdapterBenchmark(
        entry=AdapterBenchmarkEntry(profile, "JSON adapter fixture", "fixtures", "MIT"),
        task_root=task_root, task_tomls=tuple(task_tomls), tasks=adapter_tasks, warnings=(),
        manifest={"splits": ["test"], "upstream_kind": origin["kind"], "upstream_locator": origin["locator"],
                  "upstream_revision": origin["revision"], "benchmark_profile_provenance": {
                      "upstream_origin": origin, "source_task_count": 2,
                      "selected_task_ids": ["question-1", "question-2"],
                  }},
    )
    bucket = "neutral-catalog-" + uuid4().hex
    minio_tls[3].create_bucket(Bucket=bucket)
    minio_tls[3].put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    options = dict(db_url=postgres_url, object_store=_store(minio_tls), bucket=bucket,
                   imported_by=None, compat_flatten_environment=False)
    assert (await publish_versioned_local_benchmark(prepared, **options)).inserted == 2
    spec = BenchmarkProfileSpec(id=profile, origin=origin, expected_task_count=2,
                                selected_task_ids=("question-1", "question-2"))
    descriptor = tmp_path / "profile.json"
    descriptor.write_text(spec.model_dump_json())
    alias = "json-public-" + uuid4().hex
    code, _, stderr = await _activate_cli(spec, descriptor, postgres_url,
                                         "--alias", alias, "--expected-current", "stale")
    assert code == 1 and "alias_conflict" in stderr, stderr
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            assert (await session.get(Benchmark, profile)).execution_state == "pending"
            assert await session.get(BenchmarkAlias, alias) is None
        code, stdout, stderr = await _activate_cli(spec, descriptor, postgres_url,
                                                 "--alias", alias, "--expect-absent")
        assert code == 0 and "2 tasks admitted; runtime qualification unchanged" in stdout, stderr
        assert (await publish_versioned_local_benchmark(prepared, **options)).unchanged == 2
        async with sessions() as session:
            assert (await session.get(Benchmark, profile)).execution_state == "runnable"
            assert (await session.get(BenchmarkAlias, alias)).benchmark_id == profile
        versions = minio_tls[3].list_object_versions(Bucket=bucket)["Versions"]
        assert versions and all(item["VersionId"] != "null" for item in versions)
        # A legacy audit receipt cannot downgrade a managed, retired source.
        import hashlib
        from datetime import UTC, datetime

        from sqlalchemy import delete

        from loom.benchmark_admission import invalidate_benchmark_profile
        from loom.db.schema import TaskBundleSourceIncarnation, TaskBundleSourceReference
        from loom.task_bundle_source_journal import retire_task_bundle_source

        async with sessions.begin() as session:
            await invalidate_benchmark_profile(session, profile)
            rows = list(await session.scalars(select(Task).where(Task.benchmark_id == profile)))
            source_id = hashlib.sha256(rows[0].source.encode()).hexdigest()
            await session.execute(delete(TaskBundleSourceReference).where(
                TaskBundleSourceReference.source_id == source_id,
            ))
            incarnation = await session.scalar(select(TaskBundleSourceIncarnation).where(
                TaskBundleSourceIncarnation.source_id == source_id,
                TaskBundleSourceIncarnation.state == "available",
            ))
            assert await retire_task_bundle_source(session, incarnation_id=incarnation.id,
                                                  now=datetime.now(UTC))
            benchmark = await session.get(Benchmark, profile)
            receipt = AdapterValidation(
                contract="canonical.v1", profile_facts=snapshot_profile(benchmark),
                task_facts={row.id: snapshot_task(row) for row in rows},
                legacy_source_authority=True, legacy_source_task_ids=tuple(row.id for row in rows),
            )
        async with sessions.begin() as session:
            with pytest.raises(BenchmarkAdmissionError, match=r"source_unavailable.*available incarnation"):
                await admit_benchmark_profile(session, spec=spec, validation=receipt)
            assert (await session.get(Benchmark, profile)).execution_state == "pending"
    finally:
        await engine.dispose()


async def test_concurrent_retirement_cannot_be_revived_by_admission(catalog_sessions):
    from loom.benchmark_admission import retire_benchmark_profile

    spec = await publish_inline(catalog_sessions)

    async def activate():
        async with catalog_sessions.begin() as session:
            try:
                await admit_benchmark_profile(session, spec=spec)
            except BenchmarkAdmissionError as error:
                assert error.findings[0].code == "profile_historical"

    async def retire():
        async with catalog_sessions.begin() as session:
            await retire_benchmark_profile(session, spec.id)

    await asyncio.wait_for(asyncio.gather(activate(), retire()), timeout=10)
    async with catalog_sessions() as session:
        assert (await session.get(Benchmark, spec.id)).execution_state == "historical"


async def test_concurrent_cross_alias_promotions_do_not_invert_profile_locks(catalog_sessions):
    first, second = await publish_inline(catalog_sessions), await publish_inline(catalog_sessions)
    first_alias, second_alias = "first-" + uuid4().hex, "second-" + uuid4().hex
    for spec, alias in ((first, first_alias), (second, second_alias)):
        async with catalog_sessions.begin() as session:
            await admit_benchmark_profile(session, spec=spec,
                alias=AliasPromotion(alias, spec.id, None))

    async def swap(alias, destination, expected):
        async with catalog_sessions.begin() as session:
            await promote_benchmark_alias(session, alias=alias, profile_id=destination,
                                          expected_current=expected)

    await asyncio.wait_for(asyncio.gather(
        swap(first_alias, second.id, first.id), swap(second_alias, first.id, second.id),
    ), timeout=10)
    async with catalog_sessions() as session:
        assert (await session.get(BenchmarkAlias, first_alias)).benchmark_id == second.id
        assert (await session.get(BenchmarkAlias, second_alias)).benchmark_id == first.id


async def test_public_default_alias_cannot_be_rebound_to_an_arbitrary_adapter(catalog_sessions):
    spec = await publish_inline(catalog_sessions)
    async with catalog_sessions.begin() as session:
        with pytest.raises(BenchmarkAdmissionError, match="alias_promotion_requirements"):
            await admit_benchmark_profile(session, spec=spec,
                alias=AliasPromotion("terminal-bench-2", spec.id, None))
        assert (await session.get(Benchmark, spec.id)).execution_state == "pending"
        assert await session.get(BenchmarkAlias, "terminal-bench-2") is None


async def test_alias_cannot_shadow_a_physical_profile(catalog_sessions):
    first, second = await publish_inline(catalog_sessions), await publish_inline(catalog_sessions)
    async with catalog_sessions.begin() as session:
        await admit_benchmark_profile(session, spec=first)
        with pytest.raises(BenchmarkAdmissionError, match="alias_selector_collision"):
            await promote_benchmark_alias(session, alias=second.id, profile_id=first.id,
                                          expected_current=None)
    async with catalog_sessions() as session:
        assert await session.get(BenchmarkAlias, second.id) is None
        assert (await session.get(Benchmark, second.id)).execution_state == "pending"


@pytest.mark.parametrize("kind", ["s3-folder", "local-folder"])
async def test_local_root_task_uses_explicit_canonical_census_through_cli(
    catalog_sessions, admission_postgres, tmp_path, kind,
):
    profile_id = "local-root@" + uuid4().hex
    origin = dict(kind=kind, locator="s3://fixtures/local-content/", revision="",
                  release="local", importer="loom.local.v1")
    spec = BenchmarkProfileSpec(id=profile_id, origin=origin, expected_task_count=1,
                                selected_task_ids=("root",), catalog_task_ids=(profile_id,))
    async with catalog_sessions.begin() as session:
        session.add(Benchmark(
            id=profile_id, display_name="Local root task", series=None,
            upstream_kind=kind, upstream_locator=origin["locator"], upstream_revision="",
            license_spdx="MIT", license_url="https://example.test/license", splits=[],
            execution_state="pending", profile_provenance={"upstream_origin": origin},
        ))
        await session.flush()
        session.add(Task(id=profile_id, benchmark_id=profile_id, checksum="a" * 64,
                         config=_valid_task_config(profile_id), source=None,
                         source_provenance={}, tags={}))
    descriptor = tmp_path / "local-profile.json"
    descriptor.write_text(spec.model_copy(update={"catalog_task_ids": None}).model_dump_json())
    code, _, stderr = await _activate_cli(spec, descriptor, admission_postgres)
    assert code == 1 and "task_census_mismatch" in stderr, stderr
    async with catalog_sessions() as session:
        assert (await session.get(Benchmark, profile_id)).execution_state == "pending"
    descriptor.write_text(spec.model_dump_json())
    code, stdout, stderr = await _activate_cli(spec, descriptor, admission_postgres)
    assert code == 0 and "1 tasks admitted" in stdout, stderr
    async with catalog_sessions() as session:
        assert (await session.get(Benchmark, profile_id)).execution_state == "runnable"
        assert (await session.get(Task, profile_id)).config["task"]["id"] == profile_id
