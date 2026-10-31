"""Producer lifecycle semantics use disposable PostgreSQL, never live catalogs."""
from __future__ import annotations

import importlib.util
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_admission import BenchmarkAdmissionError
from loom.db.schema import Benchmark, Task
from loom.db.schema_startup import service_schema_head
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.register_cmd import run_register
from tests.unit.test_benchmark_readiness import _valid_task_config

pytestmark = [pytest.mark.docker, pytest.mark.timeout(90)]


@pytest.fixture
async def publication_sessions(postgres_url):
    engine = create_async_engine(postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def manifest(profile):
    task_id = profile + "/sample"
    return {
        "benchmark_id": profile, "display_name": "Arbitrary adapter", "upstream_kind": "json",
        "upstream_locator": "https://example.test/source.json", "upstream_revision": "release-7",
        "license_spdx": "MIT", "license_url": "https://example.test/license", "splits": ["test"],
        "benchmark_profile_provenance": {"source_task_count": 1},
        "tasks": [{"task_id": task_id, "checksum": "a" * 64, "hf_path": "sample/",
                   "task_config": _valid_task_config(task_id), "source_provenance": {}}],
    }


async def register(document, postgres_url):
    return await run_register(
        benchmark=document["benchmark_id"], db_url=postgres_url, source="object-store",
        revision="release-7", bucket="disposable-bundles", object_store=FakeObjectStore(), manifest=document,
    )


async def test_register_pending_and_descriptive_republish_preserves_prior_acceptance(
    publication_sessions, postgres_url,
):
    profile = "neutral-lifecycle@" + uuid4().hex
    document = manifest(profile)
    await register(document, postgres_url)
    async with publication_sessions.begin() as session:
        benchmark = await session.get(Benchmark, profile)
        assert benchmark.execution_state == "pending"
        # Represents an existing accepted catalog during the default migration;
        # this fixture does not claim runtime qualification from registration.
        benchmark.execution_state = "runnable"
        benchmark.profile_provenance = {
            **benchmark.profile_provenance, "activation_audit": {"observation": "retained"},
        }
    descriptive = deepcopy(document)
    descriptive["display_name"] = "Updated display only"
    descriptive["license_spdx"] = "Apache-2.0"
    await register(descriptive, postgres_url)
    async with publication_sessions() as session:
        benchmark = await session.get(Benchmark, profile)
        assert benchmark.execution_state == "runnable"
        assert benchmark.display_name == "Updated display only"
        assert benchmark.profile_provenance["activation_audit"] == {"observation": "retained"}
    changed = deepcopy(descriptive)
    changed["tasks"][0]["task_config"]["steps"][0]["name"] = "changed-execution"
    await register(changed, postgres_url)
    async with publication_sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"


async def test_registration_rejects_revision_rebinding_and_historical_revival(
    publication_sessions, postgres_url,
):
    profile = "neutral-history@" + uuid4().hex
    document = manifest(profile)
    await register(document, postgres_url)
    drift = deepcopy(document)
    drift["upstream_revision"] = "release-8"
    with pytest.raises(ValueError, match="profile_origin_mismatch"):
        await register(drift, postgres_url)
    async with publication_sessions.begin() as session:
        row = await session.get(Benchmark, profile)
        assert row.upstream_revision == "release-7"
        row.execution_state = "historical"
    with pytest.raises(BenchmarkAdmissionError, match="profile_historical"):
        await register(document, postgres_url)
    async with publication_sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "historical"
        assert await session.scalar(select(Task.checksum).where(Task.benchmark_id == profile)) == "a" * 64


async def test_migration_default_does_not_rewrite_existing_profiles(publication_sessions):
    profile = "migration-default@" + uuid4().hex
    async with publication_sessions.begin() as session:
        # Check the server default independently of the ORM's Python default.
        actual = await session.scalar(text("""
            SELECT column_default FROM information_schema.columns
            WHERE table_name='benchmarks' AND column_name='execution_state'
        """))
        assert actual == "'pending'::text"
        await session.execute(insert(Benchmark).values(
            id=profile, display_name="default", upstream_kind="json", upstream_locator="example",
            upstream_revision="7", license_spdx="MIT", license_url="", splits=["test"],
        ))
    async with publication_sessions() as session:
        assert (await session.get(Benchmark, profile)).execution_state == "pending"


async def test_pending_default_upgrade_and_rollback_retain_accepted_rows(publication_sessions):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = Path("database/migrations/versions/0184_benchmark_pending_default.py")
    module_spec = importlib.util.spec_from_file_location("benchmark_pending_default", path)
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    profile = "accepted-before-default@" + uuid4().hex
    async with publication_sessions.begin() as session:
        await session.execute(insert(Benchmark).values(
            id=profile, display_name="accepted", upstream_kind="json", upstream_locator="example",
            upstream_revision="7", license_spdx="MIT", license_url="", splits=["test"],
            execution_state="runnable", profile_provenance={"activation_audit": {"retained": True}},
        ))
        connection = await session.connection()
        def roundtrip(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                default = sync.scalar(text("SELECT column_default FROM information_schema.columns WHERE table_name='benchmarks' AND column_name='execution_state'"))
                assert default == "'runnable'::text"
                migration.upgrade()
        await connection.run_sync(roundtrip)
    async with publication_sessions() as session:
        row = await session.get(Benchmark, profile)
        assert row.execution_state == "runnable"
        assert row.profile_provenance == {"activation_audit": {"retained": True}}


async def test_concurrent_registration_cannot_steal_an_absent_foreign_task(
    publication_sessions, postgres_url,
):
    import asyncio

    first, second = "owner-a@" + uuid4().hex, "owner-b@" + uuid4().hex
    documents = [manifest(first), manifest(second)]
    shared_task = "foreign-namespace/shared-" + uuid4().hex
    for document in documents:
        document["tasks"][0]["task_id"] = shared_task
        document["tasks"][0]["task_config"] = _valid_task_config(shared_task)
    outcomes = await asyncio.gather(
        *(register(document, postgres_url) for document in documents), return_exceptions=True,
    )
    assert all(isinstance(result, ValueError) and "profile_task_identity_mismatch" in str(result)
               for result in outcomes)
    async with publication_sessions() as session:
        assert list(await session.scalars(select(Benchmark.id).where(Benchmark.id.in_([first, second])))) == []
        assert await session.get(Task, shared_task) is None


@pytest.mark.parametrize("old_state", ["pending", "runnable"])
async def test_first_registration_can_pin_an_empty_registry_placeholder(
    publication_sessions, postgres_url, old_state,
):
    profile = "registry-placeholder@" + uuid4().hex
    document = manifest(profile)
    async with publication_sessions.begin() as session:
        await session.execute(insert(Benchmark).values(
            id=profile, display_name="Registry placeholder", upstream_kind="json",
            upstream_locator=document["upstream_locator"], upstream_revision="main",
            license_spdx="MIT", license_url="", splits=["test"], execution_state=old_state,
        ))
    await register(document, postgres_url)
    async with publication_sessions() as session:
        row = await session.get(Benchmark, profile)
        assert row.execution_state == "pending"
        assert row.upstream_revision == "release-7"
        assert await session.scalar(select(Task.id).where(Task.benchmark_id == profile)) == profile + "/sample"


async def test_cli_migration_roundtrip_retains_preceding_schema_and_accepted_catalog(
    publication_sessions, postgres_url,
):
    import asyncio
    import os
    import subprocess
    import sys

    profile = "migration-chain@" + uuid4().hex
    async with publication_sessions.begin() as session:
        await session.execute(insert(Benchmark).values(
            id=profile, display_name="accepted", upstream_kind="json", upstream_locator="fixture:chain",
            upstream_revision="7", license_spdx="MIT", license_url="", splits=["test"],
            execution_state="runnable", profile_provenance={"activation_audit": {"retained": True}},
        ))

    async def migrate(*arguments):
        await asyncio.to_thread(subprocess.run,
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", *arguments],
            env={**os.environ, "LOOM_DB_URL": postgres_url}, check=True, capture_output=True, text=True)

    try:
        await migrate("downgrade", "0183")
        async with publication_sessions() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0183"
            assert await session.scalar(text("SELECT to_regclass('public.service_execution_live_events')")) is not None
            for table in ("provider_request_pools", "provider_request_pool_members"):
                assert await session.scalar(text(f"SELECT to_regclass('public.{table}')")) is not None
            assert await session.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_name='task_image_materializations' AND column_name='preparation_revision'")) == 1
            assert await session.scalar(text("SELECT column_default FROM information_schema.columns WHERE table_name='benchmarks' AND column_name='execution_state'")) == "'runnable'::text"
            row = await session.get(Benchmark, profile)
            assert row.execution_state == "runnable"
            assert row.profile_provenance == {"activation_audit": {"retained": True}}
    finally:
        await migrate("upgrade", "head")
    async with publication_sessions() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()
        assert await session.scalar(text("SELECT to_regclass('public.service_execution_live_events')")) is not None
        for table in ("provider_request_pools", "provider_request_pool_members"):
            assert await session.scalar(text(f"SELECT to_regclass('public.{table}')")) is not None
        assert await session.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_name='task_image_materializations' AND column_name='preparation_revision'")) == 1
        assert await session.scalar(text("SELECT column_default FROM information_schema.columns WHERE table_name='benchmarks' AND column_name='execution_state'")) == "'pending'::text"
        assert (await session.get(Benchmark, profile)).execution_state == "runnable"
