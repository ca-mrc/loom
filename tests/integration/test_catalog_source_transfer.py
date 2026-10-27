"""Restore adopts destination versions through the existing source journal."""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.db.schema import Benchmark, Task, TaskBundleSourceVersion
from loom.models.harbor import HarborBenchmarkSpec
from loom_cli.catalog_provision import (
    Boto3CatalogObjectStore,
    PostgresCatalogStore,
    provision_ready_benchmark_catalog,
)
from loom_cli.native_benchmark_activation import activate_native_benchmark
from tests.integration.test_harbor_native_publish import native_publication  # noqa: F401
from tests.integration.test_task_bundle_source_storage import _store
from tests.support.minio_tls import minio_tls  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.fixture(scope="module")
def destination_postgres():
    with PostgresContainer("postgres:16") as database:
        url = database.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "head"],
            env={**os.environ, "LOOM_DB_URL": url}, check=True, capture_output=True,
        )
        yield url


def objects(fixture):
    # Reuse the real fixture client with its TLS CA and bounded transport.
    result = Boto3CatalogObjectStore.__new__(Boto3CatalogObjectStore)
    result._client = fixture[3]
    return result


async def test_destination_versions_admit_independently_and_copy_never_inherits_flags(
    native_publication, postgres_url, destination_postgres, minio_tls,  # noqa: F811
):
    profile, descriptor, source_sessions, _options, _source_task = native_publication
    spec = HarborBenchmarkSpec.model_validate_json(descriptor.read_text())
    async with source_sessions.begin() as session:
        await activate_native_benchmark(session, spec=spec)
        benchmark = await session.get(Benchmark, profile)
        benchmark.profile_provenance = {**benchmark.profile_provenance, "runtime_verified": True}
        source_versions = set(await session.scalars(select(TaskBundleSourceVersion.version_id)))
    target_bucket = "restore-native-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=target_bucket)
    admin.put_bucket_versioning(Bucket=target_bucket, VersioningConfiguration={"Status": "Enabled"})
    target_store = _store(minio_tls)
    class SelectedSourceCatalog(PostgresCatalogStore):
        async def load_rows(self):
            rows = await super().load_rows()
            return replace(rows,
                benchmarks=[row for row in rows.benchmarks if row.id == profile],
                tasks=[row for row in rows.tasks if row.benchmark_id == profile], agents=[])

    options = dict(
        source_catalog=SelectedSourceCatalog(postgres_url),
        target_catalog=PostgresCatalogStore(destination_postgres),
        source_objects=objects(minio_tls), target_objects=objects(minio_tls),
        target_source_store=target_store, target_bucket=target_bucket,
    )
    # A later overwrite/delete marker cannot hide the retained authoritative
    # source version. Transfer must read its actual registered receipt.
    async with source_sessions() as session:
        original_task = await session.get(Task, profile + "/upstream/sample")
        source_bucket, source_key = original_task.source.removeprefix("s3://").split("/", 1)
        source_key = source_key.rstrip("/") + "/instruction.md"
    admin.put_object(Bucket=source_bucket, Key=source_key, Body=b"unrelated later version")
    admin.delete_object(Bucket=source_bucket, Key=source_key)
    stats = await provision_ready_benchmark_catalog(**options)
    assert stats.ready_tasks == 2 and stats.target_objects_uploaded > 0
    engine = create_async_engine(destination_postgres)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            benchmark = await session.get(Benchmark, profile)
            assert benchmark.execution_state == "pending"
            assert "runtime_verified" not in benchmark.profile_provenance
            target_versions = set(await session.scalars(select(TaskBundleSourceVersion.version_id)))
            assert target_versions and not source_versions.intersection(target_versions)
            tasks = list(await session.scalars(select(Task).where(Task.benchmark_id == profile)))
            assert all(row.source.startswith("s3://" + target_bucket + "/") for row in tasks)
            # The same descriptor now consumes actual destination authority.
            assert await activate_native_benchmark(session, spec=spec) == 2
            benchmark.profile_provenance = {
                **benchmark.profile_provenance, "activation_audit": {"destination": "retained"},
            }
        again = await provision_ready_benchmark_catalog(**options)
        assert again.target_objects_uploaded == 0 and again.target_objects_skipped > 0
        async with sessions() as session:
            # The existing destination's own exact source authority and
            # observations survive an identical copy; source flags grant none.
            row = await session.get(Benchmark, profile)
            assert row.execution_state == "runnable"
            assert row.profile_provenance["activation_audit"] == {"destination": "retained"}
            assert "runtime_verified" not in row.profile_provenance
    finally:
        await engine.dispose()
