"""Exercise catalog publication through native Nebius input consumption (#1978)."""

from __future__ import annotations

import asyncio
import os
import stat
import subprocess
import sys
from pathlib import Path

import boto3
import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark, TaskImageMaterialization
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.models.task_checksum import task_checksum
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import automatic_service_execution_rejections
from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.upload import upload_task_dir
from loom_cli.benchmark_readiness import run_bundle_presence_audit
from loom_execution_actuator.task_image_runtime import download_bundle


@pytest.mark.asyncio
@pytest.mark.parametrize("command_name", ["publish", "publish-local"])
async def test_cli_republish_repairs_legacy_prefix_and_feeds_native_builder(
    postgres_url: str, shared_minio, tmp_path: Path, command_name: str,
) -> None:
    root = tmp_path / "native-benchmark"
    task_dir = root / "tasks" / "alpha"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "verifier").mkdir()
    (root / "benchmark.toml").write_text(
        'schema_version = 1\nid = "native-benchmark"\n'
        'display_name = "Native benchmark"\nseries = "internal"\nlicense_spdx = "MIT"\n'
    )
    (task_dir / "task.toml").write_text('''schema_version = "1"
[task]
id = "alpha"
name = "Native benchmark task"
[environment]
os = "linux"
cpu_arch = "x86_64"
dockerfile = "environment/Dockerfile"
cpus = 1
memory_mb = 2048
storage_mb = 500
workdir = "/app"
network_policies_supported = ["gateway-only"]
[environment.baseline_network_policy]
kind = "gateway-only"
[agent]
name = "terminus-2"
[verifier]
name = "script"
[verifier.args]
script_path = "verifier/check.sh"
[[steps]]
name = "main"
instruction_file = "instruction.md"
''')
    (task_dir / "environment" / "Dockerfile").write_text("FROM debian:bookworm-slim\n")
    (task_dir / "instruction.md").write_text("Write an answer.\n")
    verifier = task_dir / "verifier" / "check.sh"
    verifier.write_text("#!/bin/sh\nprintf '1' > /tmp/reward.txt\n")
    verifier.chmod(0o755)
    config = shared_minio.get_config()
    endpoint = f"http://{config['endpoint']}"
    bucket = "benchmark-nebius-publish"
    store = MinioObjectStore(
        endpoint_url=endpoint, access_key=config["access_key"], secret_key=config["secret_key"],
    )
    client = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"], region_name="us-east-1",
    )
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    # A fresh interpreter prevents other tests' adapter imports from hiding the
    # service/base installation boundary. Exercise the real CLI, DB and MinIO.
    without_adapters = """
import importlib.abc
import runpy
import sys
class NoAdapters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'loom_benchmarks', 'loom_benchmark_terminal_bench_2', 'datasets'}:
            raise ModuleNotFoundError(f"No module named '{fullname}'", name=fullname)
sys.meta_path.insert(0, NoAdapters())
runpy.run_module('loom_cli', run_name='__main__')
"""
    command = [sys.executable, "-c", without_adapters, "datasets", command_name, str(root),
               "--bucket", bucket]
    env = {
        **os.environ,
        "LOOM_DB_URL": postgres_url,
        "LOOM_MINIO_ENDPOINT": endpoint,
        "LOOM_MINIO_ACCESS_KEY": config["access_key"],
        "LOOM_MINIO_SECRET_KEY": config["secret_key"],
    }

    async def publish() -> subprocess.CompletedProcess[str]:
        result = await asyncio.to_thread(
            subprocess.run, command, env=env, capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, result.stderr
        return result

    try:
        # Infrastructure owns bucket creation; ordinary publication uses the
        # existing bucket, just as it does in the Nebius environment.
        await store.ensure_bucket(bucket)
        await publish()
        async with sessions() as session:
            task = await session.get(TaskRow, "native-benchmark/alpha")
            assert task is not None
            expected_checksum = task.checksum
            binding = task.source_provenance["service_execution_input"]
            # Reconstruct the previously published layout, including its extra
            # manifest object. Republishing must leave these frozen bytes alone.
            legacy_prefix = (
                f"native-benchmark/alpha/.loom-revisions/{task.checksum}/"
                f"{task.source_provenance['bundle_file_metadata_sha256'].removeprefix('sha256:')}/"
            )
            await upload_task_dir(store=store, bucket=bucket, prefix=legacy_prefix,
                                  task_dir=task_dir)
            manifest_key = binding["manifest_uri"].removeprefix(f"s3://{bucket}/")
            body = await store.get_object(bucket=bucket, key=manifest_key)
            legacy_manifest_key = f"{legacy_prefix}service-execution-input.json"
            await store.put_object(bucket=bucket, key=legacy_manifest_key, body=body)
            await session.execute(update(TaskRow).where(TaskRow.id == task.id).values(
                source=f"s3://{bucket}/{legacy_prefix}",
                source_provenance={**task.source_provenance, "service_execution_input": {
                    **binding, "manifest_uri": f"s3://{bucket}/{legacy_manifest_key}",
                }},
            ))
            await session.commit()

        result = await publish()
        assert "updated=1" in result.stdout
        async with sessions() as session:
            task = (await session.scalars(select(TaskRow).where(
                TaskRow.id == "native-benchmark/alpha",
            ))).one()
            assert task.checksum == expected_checksum == task_checksum(task_dir)
            assert task.benchmark_id == "native-benchmark" and task.task_set_id is None
            assert not automatic_service_execution_rejections(
                TaskConfig.model_validate(task.config),
                TrialConfig(agent_name="terminus-2", agent_model=ModelSpec(
                    provider="openai", name="test-model",
                )),
                source_provenance=task.source_provenance, allow_task_image_preparation=True,
            )
            claim = {"task_source": task.source, "source_bucket": bucket,
                     "task_checksum": task.checksum,
                     "task_source_provenance": task.source_provenance}
        downloaded = tmp_path / "native-builder-input"
        await asyncio.to_thread(download_bundle, claim, client, downloaded)
        assert task_checksum(downloaded) == expected_checksum
        assert stat.S_IMODE((downloaded / "verifier" / "check.sh").stat().st_mode) == 0o755
        assert not (downloaded / "service-execution-input.json").exists()
        report = await run_bundle_presence_audit(
            db_url=postgres_url, object_store=store, benchmark="native-benchmark",
        )
        assert report.verified == 1 and report.failed == 0
        assert await store.get_object(bucket=bucket, key=legacy_manifest_key) == body
        assert "unchanged=1" in (await publish()).stdout
    finally:
        async with sessions() as session:
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id == "native-benchmark/alpha",
            ))
            await session.execute(delete(TaskRow).where(TaskRow.benchmark_id == "native-benchmark"))
            await session.execute(delete(Benchmark).where(Benchmark.id == "native-benchmark"))
            await session.commit()
        await engine.dispose()
        client.close()
