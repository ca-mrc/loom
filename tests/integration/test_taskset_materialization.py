"""Integration tests for TaskSet materialization worker (#242 sub-plan 3)."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tarfile
import threading
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, delete, event, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskSet,
    TaskSetFenceCanaryAuthorization,
    TaskSetGenerationGcCursor,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Team,
    TeamQuota,
    Token,
    User,
)
from loom.taskset.materialize import MaterializeOutput, TaskRowDraft
from loom.taskset.storage_bytes import team_taskset_storage_bytes
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom_service import taskset_gc, taskset_materializer
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.submission_compat import validate_submission_agent_task_compatibility
from loom_service.taskset_gc import (
    purge_abandoned_materialization_generations,
    purge_expired_task_sets,
)
from loom_service.taskset_intake import delete_task_set, get_latest_job, rebuild_task_set
from loom_service.taskset_materializer import run_once
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.support.minio import ensure_test_bucket
from tests.support.minio_images import prepare_test_image

_MANIFEST_INLINE = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: inline-tasks
  display_name: Inline Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"What is 1+1?"}
    {"id":"2","question":"What is 2+2?"}
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }}"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
"""

_MANIFEST_PARTIAL = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: partial-tasks
  display_name: Partial Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"ok"}
    {"bad":"missing id"}
    {"id":"3","question":"ok3"}
    {"id":"4"}
    {"id":"5","question":"ok5"}
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
"""

_MANIFEST_TRANSFORM = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: transform-tasks
  display_name: Transform Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1"}
instance_mapping:
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
transform:
  file: transform.py
"""

_MANIFEST_DNS_INCOMPATIBLE = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: dns-incompatible-tasks
  display_name: DNS Incompatible Tasks
intents:
  - evaluation
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"break dns"}
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
    dockerfile: environment/Dockerfile
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
verifier:
  type: script
  file: environment/Dockerfile
"""

_MANIFEST_BUNDLE_UPLOAD = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: bundle-upload-tasks
  display_name: Bundle Upload Tasks
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
limits:
  max_instances: 1
"""


_SYSTEM_CANARY_TEAM_ID = UUID("2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9")
_SYSTEM_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"


def _add_tar_file(tar: tarfile.TarFile, name: str, body: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(body))


def _bundle_tar_bytes() -> bytes:
    task_toml = b"""
version = "1"

[metadata]
id = "source-useful-frontier-5003/alpha"
name = "Alpha Task"

[environment]
dockerfile = "environment/Dockerfile"

[verifier]
name = "script"

[verifier.args]
script_path = "verifier/check.sh"
"""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        _add_tar_file(tar, "tasks/alpha/task.toml", task_toml)
        _add_tar_file(tar, "tasks/alpha/instruction.md", b"Create answer.txt\n")
        _add_tar_file(tar, "tasks/alpha/data/input.txt", b"per-task payload\n")
        _add_tar_file(tar, "tasks/alpha/environment/Dockerfile", b"FROM alpine:3.20\n")
        _add_tar_file(tar, "tasks/alpha/verifier/check.sh", b"#!/bin/sh\nexit 0\n")
    return out.getvalue()


def _unsafe_symlink_bundle_tar_bytes() -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        info = tarfile.TarInfo("tasks/alpha/escape")
        info.type = tarfile.SYMTYPE
        info.linkname = "../outside"
        tar.addfile(info)
    return out.getvalue()


def _unsafe_traversal_bundle_tar_bytes() -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        _add_tar_file(tar, "../outside.txt", b"escape\n")
    return out.getvalue()


@pytest.fixture(scope="module")
def materialization_minio() -> MinioContainer:
    # The pinned MinIO image reports liveness before its object layer is writable.
    with MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster")) as m:
        yield m


@pytest.fixture
async def materialization_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    materialization_minio: MinioContainer,
    tmp_path: Path,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    cfg = materialization_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    cache_root = tmp_path / "upstream-cache"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_TASKSET_MATERIALIZER_UPSTREAM_CACHE_ROOT": str(cache_root),
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    ensure_test_bucket(materialization_minio, settings.artifacts_bucket)

    team_a = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"t-{team_a}"))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        user_id = uuid4()
        username = f"MatOwner-{team_a.hex[:8]}"
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            ),
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_a.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_a,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            ),
        )
        s.commit()

    tokens = {"team_a": raw_a}
    teams = {"team_a": team_a}
    try:
        yield app, tokens, teams
    finally:
        await app.state.http_client.aclose()
        if hasattr(app.state, "batch_runner_task"):
            app.state.batch_runner_task.cancel()
        if hasattr(app.state, "taskset_materializer_task"):
            app.state.taskset_materializer_task.cancel()
        await engine.dispose()
        with sl() as s:
            # Materializations retain task identities without a cascading FK.
            # The owned namespace also covers removed tasks and old revisions.
            s.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.startswith(f"ts/{team_a}/")
                )
            )
            s.execute(delete(Task).where(Task.task_set_id.is_not(None)))
            s.execute(delete(TaskSetFenceCanaryAuthorization))
            s.execute(delete(TaskSetGenerationGcCursor))
            s.execute(delete(TaskSetMaterializationJob))
            s.execute(delete(TaskSetManifest))
            s.execute(delete(TaskSet))
            s.execute(delete(Token))
            s.execute(delete(User))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _run_materializer_once(app: FastAPI) -> None:
    settings = app.state.settings
    await run_once(
        session_factory=app.state.session_factory,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        batch_size=settings.taskset_materializer_batch_size,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
    )


async def _submit_inline_task_set(
    app: FastAPI,
    *,
    token: str,
) -> str:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {token}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_INLINE.encode(),
                    "application/x-yaml",
                ),
            },
        )
    assert response.status_code == 202, response.text
    return response.json()["task_set_id"]


async def _seed_canary_system_team(app: FastAPI) -> UUID:
    """Return the migration-created fixed deployment-owned Team.

    Seed a separate historical ``admin`` Team too: the canary must not select
    it by display name now that its deployment identity is explicit.
    """
    async with app.state.session_factory() as session:
        team = await session.get(Team, _SYSTEM_CANARY_TEAM_ID)
        if team is None:
            team = Team(id=_SYSTEM_CANARY_TEAM_ID, name=_SYSTEM_CANARY_TEAM_NAME)
            session.add(team)
            await session.flush()
            session.add(TeamQuota(team_id=team.id))
        admin_team = (
            await session.execute(
                select(Team).where(Team.name == "admin"),
            )
        ).scalar_one_or_none()
        if admin_team is None:
            admin_team = Team(name="admin")
            session.add(admin_team)
            await session.flush()
            session.add(TeamQuota(team_id=admin_team.id))
        await session.commit()
    return team.id


async def _prepare_deployment_fence_canary(
    app: FastAPI,
    *,
    image_tag: str = "staging-aaaaaaa",
):  # type: ignore[no-untyped-def]
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryPreparationRequest,
        prepare_deployment_fence_canary,
    )

    await _seed_canary_system_team(app)
    settings = app.state.settings
    return await prepare_deployment_fence_canary(
        app.state.session_factory,
        request=TaskSetFenceCanaryPreparationRequest.from_mapping(
            {
                "candidate_sha": "a" * 40,
                "image_tag": image_tag,
                "authorization_token": "deployment-only-capability",
            }
        ),
        configured_token="deployment-only-capability",
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        taskset_quota_max_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        manifest_max_bytes=settings.taskset_manifest_max_bytes,
        bundle_max_bytes=settings.taskset_quota_max_bundle_bytes,
        nonce_factory=lambda: "n" * 43,
    )


@pytest.mark.asyncio
async def test_deployment_fence_canary_preparation_persists_a_candidate_bound_retry_tag(
    materialization_setup,
) -> None:
    """A deployment retry tag remains bound to the same candidate in storage."""
    app, _tokens, _teams = materialization_setup

    contract = await _prepare_deployment_fence_canary(
        app,
        image_tag="staging-rerun-aaaaaaa",
    )

    async with app.state.session_factory() as session:
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )

    assert authorization is not None
    assert authorization.image_tag == "staging-rerun-aaaaaaa"


@pytest.mark.asyncio
async def test_next_preparation_retires_an_unclaimed_canary_left_by_a_dead_driver(
    materialization_setup,
) -> None:
    """An interrupted prepare/run handoff does not accumulate active fixtures."""
    app, _tokens, _teams = materialization_setup

    abandoned = await _prepare_deployment_fence_canary(app)
    successor = await _prepare_deployment_fence_canary(app)

    async with app.state.session_factory() as session:
        abandoned_task_set = await session.get(TaskSet, abandoned.task_set_id)
        abandoned_job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == abandoned.task_set_id,
                ),
            )
        ).scalar_one()
        abandoned_authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            abandoned.task_set_id,
        )
        successor_task_set = await session.get(TaskSet, successor.task_set_id)

    assert abandoned_task_set is not None
    assert abandoned_task_set.status == "deleted"
    assert abandoned_task_set.soft_deleted_at is not None
    assert abandoned_job.state == "cancelled"
    assert abandoned_authorization is not None
    assert abandoned_authorization.consumed_at is None
    assert successor_task_set is not None
    assert successor_task_set.status == "materializing"


@pytest.mark.asyncio
async def test_deployment_fence_canary_retires_before_claim_when_owner_setup_fails(
    materialization_setup,
) -> None:
    """A local pre-claim runner error releases its disposable quota immediately."""
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryRuntimeError,
        run_deployment_fence_canary,
    )

    app, _tokens, _teams = materialization_setup
    settings = app.state.settings
    contract = await _prepare_deployment_fence_canary(app)

    with pytest.raises(TaskSetFenceCanaryRuntimeError, match="owners were not available"):
        await run_deployment_fence_canary(
            contract,
            configured_token="deployment-only-capability",
            session_factory=app.state.session_factory,
            minio_client=app.state.minio_client,
            artifacts_bucket=settings.artifacts_bucket,
            upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(
                enabled=settings.taskset_materializer_transforms_enabled,
                network_isolated=settings.taskset_materializer_transform_network_isolated,
                workload_contract=settings.workload_contract,
                wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
                cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
                memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
            ),
            max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
            max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
            claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
            owner_factory=iter(("same-owner", "same-owner")).__next__,
        )

    async with app.state.session_factory() as session:
        task_set = await session.get(TaskSet, contract.task_set_id)
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                ),
            )
        ).scalar_one()
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )

    assert task_set is not None
    assert task_set.status == "deleted"
    assert job.state == "cancelled"
    assert authorization is not None
    assert authorization.consumed_at is None


def _published_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _stage_output_for_lease(
    app: FastAPI,
    *,
    team_id: UUID,
    slug: str,
    lease: taskset_materializer.MaterializationLease,
    task_set_id: str,
    task_name: str,
) -> MaterializeOutput:
    """Write a disposable generated object and return its staged Task row."""
    output_prefix = (
        f"tasksets/user/{team_id}/{slug}/materializations/"
        f"{lease.job_id}/{lease.lease_epoch}/tasks/{task_name}"
    )
    app.state.minio_client.put_object(
        Bucket=app.state.settings.artifacts_bucket,
        Key=f"{output_prefix}/task.toml",
        Body=b"version = '1'\n",
    )
    return MaterializeOutput(
        task_rows=[
            TaskRowDraft(
                id=f"{task_set_id}/tasks/{task_name}",
                checksum=f"checksum-{task_name}",
                config=_published_task_config(task_name),
                source=f"s3://{app.state.settings.artifacts_bucket}/{output_prefix}/",
            ),
        ],
        task_count=1,
        status="ready",
        evaluation_ready=True,
    )


def _object_keys(app: FastAPI, *, prefix: str) -> set[str]:
    return {
        item["Key"]
        for item in app.state.minio_client.list_objects_v2(
            Bucket=app.state.settings.artifacts_bucket,
            Prefix=prefix,
        ).get("Contents", [])
    }


@pytest.mark.asyncio
async def test_live_generation_gc_deletes_only_unreferenced_db_generation(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="generation-gc-owner-a",
        )
    lease_a = claimed_by_a[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_a)
    output_a = _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease_a,
        task_set_id=task_set_id,
        task_name="loser",
    )

    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(lease_heartbeat_at=datetime.now(UTC) - timedelta(minutes=5)),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 1
        )
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="generation-gc-owner-b",
        )
    lease_b = claimed_by_b[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_b)
    output_b = _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease_b,
        task_set_id=task_set_id,
        task_name="winner",
    )
    async with app.state.session_factory() as session:
        await taskset_materializer.publish_if_current(
            session,
            lease=lease_b,
            task_set_id=task_set_id,
            output=output_b,
            claim_ttl_sec=60,
        )
        session.add(
            Task(
                id=f"{task_set_id}/tasks/foreign-source",
                checksum="foreign-source",
                config={"task": {"id": "foreign-source"}},
                source="s3://foreign-bucket/attacker-controlled-prefix/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
        await session.commit()

    root = f"tasksets/user/{teams['team_a']}/inline-tasks/"
    loser_prefix = f"{root}materializations/{lease_a.job_id}/{lease_a.lease_epoch}/"
    winner_prefix = f"{root}materializations/{lease_b.job_id}/{lease_b.lease_epoch}/"
    stable_key = f"{root}manifest.yaml"
    legacy_key = f"{root}tasks/legacy/task.toml"
    unknown_key = f"{root}materializations/not-a-uuid/0/tasks/unknown/task.toml"
    malformed_key = f"{root}materializations/{lease_b.job_id}/01/tasks/bad/task.toml"
    future_key = (
        f"{root}materializations/{lease_b.job_id}/{lease_b.lease_epoch + 1}/tasks/future/task.toml"
    )
    for key in (stable_key, legacy_key, unknown_key, malformed_key, future_key):
        app.state.minio_client.put_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=key,
            Body=b"must-survive",
        )

    async with app.state.session_factory() as session:
        result = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            object_delete_budget=10,
        )

    assert result.deleted_objects == 1
    assert _object_keys(app, prefix=loser_prefix) == set()
    assert _object_keys(app, prefix=winner_prefix)
    for key in (stable_key, legacy_key, unknown_key, malformed_key, future_key):
        assert key in _object_keys(app, prefix=key)
    assert output_a.task_rows[0].source != output_b.task_rows[0].source


@pytest.mark.asyncio
async def test_live_generation_gc_preserves_active_epoch_and_resumes_at_budget(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="generation-gc-active-owner",
        )
    lease = claimed[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease)
    _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease,
        task_set_id=task_set_id,
        task_name="active",
    )
    generation_prefix = (
        f"tasksets/user/{teams['team_a']}/inline-tasks/materializations/"
        f"{lease.job_id}/{lease.lease_epoch}/"
    )
    for name in ("one", "two"):
        app.state.minio_client.put_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=f"{generation_prefix}tasks/active/{name}.txt",
            Body=name.encode(),
        )

    async with app.state.session_factory() as session:
        active_result = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            object_delete_budget=2,
        )
    assert active_result.deleted_objects == 0
    assert len(_object_keys(app, prefix=generation_prefix)) == 3

    async with app.state.session_factory() as session:
        await taskset_materializer._fail_lease(
            session,
            lease=lease,
            failure_reason="test_terminal_failure",
            failure_message="make the staged generation collectable",
            claim_ttl_sec=60,
        )
    async with app.state.session_factory() as session:
        first_pass = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            object_delete_budget=2,
        )
    assert first_pass.deleted_objects == 2
    assert first_pass.partial is True
    assert len(_object_keys(app, prefix=generation_prefix)) == 1

    async with app.state.session_factory() as session:
        second_pass = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            object_delete_budget=2,
        )
    assert second_pass.deleted_objects == 1
    assert second_pass.partial is False
    assert _object_keys(app, prefix=generation_prefix) == set()


@pytest.mark.asyncio
async def test_live_generation_gc_uses_durable_cursor_across_skipped_clock_restarts(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable sequence reaches tail pages despite restart times 0, 2, 4, and 6."""
    app, _tokens, teams = materialization_setup
    created_at = datetime(2025, 1, 1, tzinfo=UTC)
    tail_job_id = uuid4()

    task_sets: list[TaskSet] = []
    jobs: list[TaskSetMaterializationJob] = []
    for task_set_index in range(101):
        slug = f"gc-window-{task_set_index:03d}"
        task_set_id = f"ts/{teams['team_a']}/{slug}"
        task_sets.append(
            TaskSet(
                id=task_set_id,
                owning_team_id=teams["team_a"],
                slug=slug,
                display_name=f"GC window {task_set_index}",
                visibility="private",
                status="failed",
                intents=["trajectory_generation"],
                manifest_blob_uri=f"s3://artifacts/{slug}/manifest.yaml",
                created_at=created_at + timedelta(seconds=task_set_index),
            )
        )
        job_count = 1 if task_set_index < 100 else 101
        for job_index in range(job_count):
            jobs.append(
                TaskSetMaterializationJob(
                    id=(tail_job_id if job_index == 100 else uuid4()),
                    task_set_id=task_set_id,
                    owning_team_id=teams["team_a"],
                    state="failed",
                    lease_epoch=1,
                    enqueued_at=created_at + timedelta(seconds=job_index),
                )
            )

    async with app.state.session_factory() as session:
        session.add_all(task_sets)
        session.add_all(jobs)
        await session.commit()

    orphan_prefix = (
        f"tasksets/user/{teams['team_a']}/gc-window-100/materializations/{tail_job_id}/1/"
    )
    orphan_key = f"{orphan_prefix}tasks/orphan/task.toml"
    app.state.minio_client.put_object(
        Bucket=app.state.settings.artifacts_bucket,
        Key=orphan_key,
        Body=b"orphan",
    )

    skipped_clock_starts = [0, 2, 4, 6]

    class _SkippedClock:
        @classmethod
        def now(cls, _timezone: object) -> datetime:
            return datetime.fromtimestamp(skipped_clock_starts.pop(0) * 3_600, UTC)

    monkeypatch.setattr(taskset_gc, "datetime", _SkippedClock)

    results = []
    for expected_next_sweep in range(1, 5):
        # A new session for every call models a supervisor restart.  The old
        # wall-clock scheme would observe only 0, 2, 4, and 6 and never leave
        # the first TaskSet page; durable scheduling must not consult it.
        async with app.state.session_factory() as session:
            results.append(
                await purge_abandoned_materialization_generations(
                    session,
                    minio_client=app.state.minio_client,
                    artifacts_bucket=app.state.settings.artifacts_bucket,
                    task_set_limit=100,
                    job_limit=100,
                    object_delete_budget=10,
                )
            )
        async with app.state.session_factory() as session:
            next_sweep = (
                await session.execute(
                    select(TaskSetGenerationGcCursor.next_sweep),
                )
            ).scalar_one()
        assert next_sweep == expected_next_sweep

    assert skipped_clock_starts == [0, 2, 4, 6]
    assert [result.deleted_objects for result in results] == [0, 0, 0, 1]
    assert results[-1].partial is False
    assert _object_keys(app, prefix=orphan_prefix) == set()


@pytest.mark.asyncio
async def test_live_generation_gc_repeats_unadvanced_cursor_after_cancellation(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash before completion leaves the cursor on the same safe page."""
    app, _tokens, teams = materialization_setup
    job_id = uuid4()
    task_set_id = f"ts/{teams['team_a']}/gc-cancelled"
    async with app.state.session_factory() as session:
        session.add(
            TaskSet(
                id=task_set_id,
                owning_team_id=teams["team_a"],
                slug="gc-cancelled",
                display_name="GC cancelled",
                visibility="private",
                status="failed",
                intents=["trajectory_generation"],
                manifest_blob_uri="s3://artifacts/gc-cancelled/manifest.yaml",
            )
        )
        session.add(
            TaskSetMaterializationJob(
                id=job_id,
                task_set_id=task_set_id,
                owning_team_id=teams["team_a"],
                state="failed",
                lease_epoch=1,
            )
        )
        await session.commit()

    orphan_prefix = f"tasksets/user/{teams['team_a']}/gc-cancelled/materializations/{job_id}/1/"
    orphan_key = f"{orphan_prefix}tasks/orphan/task.toml"
    app.state.minio_client.put_object(
        Bucket=app.state.settings.artifacts_bucket,
        Key=orphan_key,
        Body=b"orphan",
    )

    async def cancel_recheck(_session: object, **_kwargs: object) -> tuple[bool, bool]:
        raise asyncio.CancelledError

    original_recheck = taskset_gc._candidate_is_protected_after_recheck
    monkeypatch.setattr(taskset_gc, "_candidate_is_protected_after_recheck", cancel_recheck)
    async with app.state.session_factory() as session:
        with pytest.raises(asyncio.CancelledError):
            await purge_abandoned_materialization_generations(
                session,
                minio_client=app.state.minio_client,
                artifacts_bucket=app.state.settings.artifacts_bucket,
            )

    async with app.state.session_factory() as session:
        next_sweep_after_cancellation = (
            await session.execute(
                select(TaskSetGenerationGcCursor.next_sweep),
            )
        ).scalar_one()
    assert next_sweep_after_cancellation == 0
    assert orphan_key in _object_keys(app, prefix=orphan_key)

    monkeypatch.setattr(
        taskset_gc,
        "_candidate_is_protected_after_recheck",
        original_recheck,
    )
    async with app.state.session_factory() as session:
        result = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
        )

    assert result.deleted_objects == 1
    async with app.state.session_factory() as session:
        next_sweep_after_retry = (
            await session.execute(
                select(TaskSetGenerationGcCursor.next_sweep),
            )
        ).scalar_one()
    assert next_sweep_after_retry == 1
    assert _object_keys(app, prefix=orphan_prefix) == set()


@pytest.mark.asyncio
async def test_soft_delete_gc_uses_taskset_root_delimiter(materialization_setup) -> None:
    app, _tokens, teams = materialization_setup
    deleted_at = datetime.now(UTC) - timedelta(days=8)
    alpha_id = f"ts/{teams['team_a']}/alpha"
    alpha_next_id = f"ts/{teams['team_a']}/alpha-next"
    async with app.state.session_factory() as session:
        session.add_all(
            [
                TaskSet(
                    id=alpha_id,
                    owning_team_id=teams["team_a"],
                    slug="alpha",
                    display_name="Alpha",
                    visibility="private",
                    status="deleted",
                    intents=["trajectory_generation"],
                    manifest_blob_uri="s3://artifacts/alpha/manifest.yaml",
                    soft_deleted_at=deleted_at,
                ),
                TaskSet(
                    id=alpha_next_id,
                    owning_team_id=teams["team_a"],
                    slug="alpha-next",
                    display_name="Alpha Next",
                    visibility="private",
                    status="ready",
                    intents=["trajectory_generation"],
                    manifest_blob_uri="s3://artifacts/alpha-next/manifest.yaml",
                ),
            ]
        )
        await session.commit()

    alpha_key = f"tasksets/user/{teams['team_a']}/alpha/manifest.yaml"
    alpha_next_key = f"tasksets/user/{teams['team_a']}/alpha-next/manifest.yaml"
    for key in (alpha_key, alpha_next_key):
        app.state.minio_client.put_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=key,
            Body=b"root-gc",
        )

    async with app.state.session_factory() as session:
        assert (
            await purge_expired_task_sets(
                session,
                minio_client=app.state.minio_client,
                artifacts_bucket=app.state.settings.artifacts_bucket,
                retention_days=7,
            )
            == 1
        )

    assert alpha_key not in _object_keys(app, prefix=alpha_key)
    assert alpha_next_key in _object_keys(app, prefix=alpha_next_key)


@pytest.mark.asyncio
async def test_rebuild_quota_counts_existing_taskset_root_bytes(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    await _run_materializer_once(app)

    async with app.state.session_factory() as session:
        published_rows = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        published_bytes = team_taskset_storage_bytes(
            app.state.minio_client,
            bucket=app.state.settings.artifacts_bucket,
            team_id=teams["team_a"],
        )
        quota = await session.get(TeamQuota, teams["team_a"])
        assert published_rows
        assert published_bytes > 0
        assert quota is not None
        quota.taskset_max_storage_bytes = published_bytes
        await rebuild_task_set(
            session,
            team_id=teams["team_a"],
            task_set_id=task_set_id,
        )

    await _run_materializer_once(app)

    async with app.state.session_factory() as session:
        rows_after = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        latest_job = await get_latest_job(session, task_set_id)
    assert latest_job is not None
    assert rows_after == published_rows
    assert latest_job.state == "failed"
    assert latest_job.failure_reason == "size_exceeded"


@pytest.mark.asyncio
async def test_delete_locks_active_job_before_task_set_and_revokes_lease(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    claimed_at = datetime.now(UTC)

    async with app.state.session_factory() as session:
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()
        job.state = "running"
        job.claimed_by = "delete-lock-order-owner"
        job.claimed_at = claimed_at
        job.lease_heartbeat_at = claimed_at
        job.lease_epoch = 7
        job_id = job.id
        await session.commit()

    statements: list[str] = []

    def capture_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if "task_set_materialization_jobs" in normalized or "task_sets" in normalized:
            statements.append(normalized)

    engine = app.state.session_factory.kw["bind"]
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        async with app.state.session_factory() as session:
            await delete_task_set(
                session,
                team_id=teams["team_a"],
                task_set_id=task_set_id,
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    active_job_select_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith("select") and "task_set_materialization_jobs" in statement
    )
    task_set_update_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith("update task_sets")
    )
    assert "for update" in statements[active_job_select_index]
    assert active_job_select_index < task_set_update_index

    async with app.state.session_factory() as session:
        job_after_delete = await session.get(TaskSetMaterializationJob, job_id)
        task_set_after_delete = await session.get(TaskSet, task_set_id)

    assert job_after_delete is not None
    assert job_after_delete.state == "cancelled"
    assert job_after_delete.lease_epoch == 8
    assert job_after_delete.claimed_at is None
    assert job_after_delete.claimed_by is None
    assert job_after_delete.lease_heartbeat_at is None
    assert task_set_after_delete is not None
    assert task_set_after_delete.status == "deleted"
    assert task_set_after_delete.soft_deleted_at is not None


@pytest.mark.asyncio
async def test_materialization_reclaims_only_stale_lease_heartbeats(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    lease_a = claimed_by_a[0]

    now = datetime.now(UTC)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(
                claimed_at=now - timedelta(minutes=5),
                lease_heartbeat_at=now,
            ),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 0
        )

    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(lease_heartbeat_at=now - timedelta(minutes=5)),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 1
        )
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=now - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )

    assert len(claimed_by_b) == 1
    lease_b = claimed_by_b[0]
    assert lease_b.id == lease_a.id
    assert lease_b.lease_epoch > lease_a.lease_epoch
    assert lease_b.claimed_by == owner_b


@pytest.mark.asyncio
async def test_materialization_heartbeats_while_blocked_in_threaded_work(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    loop = asyncio.get_running_loop()
    materialization_started = asyncio.Event()
    enough_heartbeats = asyncio.Event()
    release_materialization = threading.Event()
    heartbeat_count = 0

    def blocking_materialize(**_kwargs: object) -> MaterializeOutput:
        loop.call_soon_threadsafe(materialization_started.set)
        assert release_materialization.wait(timeout=5)
        return MaterializeOutput(
            task_rows=[
                TaskRowDraft(
                    id=f"{task_set_id}/tasks/heartbeat-owner",
                    checksum="heartbeat-checksum",
                    config=_published_task_config("heartbeat-owner"),
                    source="s3://staged/heartbeat-owner/",
                ),
            ],
            task_count=1,
            status="ready",
            evaluation_ready=True,
        )

    original_heartbeat = taskset_materializer._heartbeat_lease

    async def observe_heartbeat(*args: object, **kwargs: object) -> None:
        nonlocal heartbeat_count
        await original_heartbeat(*args, **kwargs)
        heartbeat_count += 1
        if heartbeat_count == 4:
            enough_heartbeats.set()

    monkeypatch.setattr(taskset_materializer, "materialize_task_set", blocking_materialize)
    monkeypatch.setattr(taskset_materializer, "_heartbeat_lease", observe_heartbeat)

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="blocking-heartbeat-owner",
        )
    assert len(claimed) == 1
    lease = claimed[0]

    owner_task = asyncio.create_task(
        taskset_materializer._materialize_claimed_job(
            app.state.session_factory,
            job_id=lease.id,
            lease=lease,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(enabled=False, network_isolated=False),
            claim_ttl_sec=1,
        ),
    )
    try:
        await asyncio.wait_for(materialization_started.wait(), timeout=5)
        # Four periodic heartbeats require the owner to outlive the one-second
        # test TTL while the blocking ``to_thread`` materialization is still live.
        await asyncio.wait_for(enough_heartbeats.wait(), timeout=5)
        async with app.state.session_factory() as session:
            assert (
                await taskset_materializer.reclaim_stale_jobs(
                    session,
                    claim_ttl_sec=1,
                )
                == 0
            )
            job_while_blocked = await session.get(TaskSetMaterializationJob, lease.id)
        assert job_while_blocked is not None
        assert job_while_blocked.state == "running"
        assert job_while_blocked.lease_epoch == lease.lease_epoch
    finally:
        release_materialization.set()
        owner_result = (
            await asyncio.wait_for(
                asyncio.gather(owner_task, return_exceptions=True),
                timeout=5,
            )
        )[0]

    assert owner_result is None
    async with app.state.session_factory() as session:
        job_after = await session.get(TaskSetMaterializationJob, lease.id)
        rows_after = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert job_after is not None
    assert job_after.state == "succeeded"
    assert [row.id for row in rows_after] == [f"{task_set_id}/tasks/heartbeat-owner"]


@pytest.mark.asyncio
async def test_cancelled_materializer_cannot_publish_after_blocking_work_resumes(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    loop = asyncio.get_running_loop()
    materialization_started = asyncio.Event()
    release_materialization = threading.Event()

    def blocking_materialize(**_kwargs: object) -> MaterializeOutput:
        loop.call_soon_threadsafe(materialization_started.set)
        assert release_materialization.wait(timeout=5)
        return MaterializeOutput(
            task_rows=[
                TaskRowDraft(
                    id=f"{task_set_id}/tasks/cancelled-owner",
                    checksum="cancelled-checksum",
                    config=_published_task_config("cancelled-owner"),
                    source="s3://staged/cancelled-owner/",
                ),
            ],
            task_count=1,
            status="ready",
            evaluation_ready=True,
        )

    monkeypatch.setattr(taskset_materializer, "materialize_task_set", blocking_materialize)
    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="cancellation-owner",
        )
    assert len(claimed) == 1
    lease = claimed[0]

    owner_task = asyncio.create_task(
        taskset_materializer._materialize_claimed_job(
            app.state.session_factory,
            job_id=lease.id,
            lease=lease,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(enabled=False, network_isolated=False),
        ),
    )
    try:
        await asyncio.wait_for(materialization_started.wait(), timeout=5)
        async with app.state.session_factory() as session:
            await delete_task_set(
                session,
                team_id=teams["team_a"],
                task_set_id=task_set_id,
            )
            cancelled_job = await session.get(TaskSetMaterializationJob, lease.id)
            cancelled_task_set = await session.get(TaskSet, task_set_id)
        assert cancelled_job is not None
        assert cancelled_task_set is not None
        cancelled_job_snapshot = (
            cancelled_job.state,
            cancelled_job.lease_epoch,
            cancelled_job.claimed_by,
            cancelled_job.finished_at,
        )
        cancelled_task_set_snapshot = (
            cancelled_task_set.status,
            cancelled_task_set.soft_deleted_at,
            cancelled_task_set.task_count,
            cancelled_task_set.evaluation_ready,
        )
    finally:
        release_materialization.set()
        owner_result = (
            await asyncio.wait_for(
                asyncio.gather(owner_task, return_exceptions=True),
                timeout=5,
            )
        )[0]

    assert isinstance(owner_result, taskset_materializer.LeaseLost)
    async with app.state.session_factory() as session:
        job_after = await session.get(TaskSetMaterializationJob, lease.id)
        task_set_after = await session.get(TaskSet, task_set_id)
        rows_after = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert job_after is not None
    assert task_set_after is not None
    assert (
        job_after.state,
        job_after.lease_epoch,
        job_after.claimed_by,
        job_after.finished_at,
    ) == cancelled_job_snapshot
    assert (
        task_set_after.status,
        task_set_after.soft_deleted_at,
        task_set_after.task_count,
        task_set_after.evaluation_ready,
    ) == cancelled_task_set_snapshot
    assert rows_after == []


@pytest.mark.asyncio
async def test_crash_after_staged_upload_leaves_only_orphaned_generation_output(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    loop = asyncio.get_running_loop()
    upload_completed = asyncio.Event()
    materialization_finished = asyncio.Event()
    release_materialization = threading.Event()

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="crash-after-upload-owner",
        )
    assert len(claimed) == 1
    lease = claimed[0]
    output_key = (
        f"tasksets/user/{teams['team_a']}/inline-tasks/materializations/"
        f"{lease.job_id}/{lease.lease_epoch}/tasks/crash-after-upload/task.toml"
    )

    def stage_then_block(**_kwargs: object) -> MaterializeOutput:
        app.state.minio_client.put_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=output_key,
            Body=b"version = '1'\n",
        )
        loop.call_soon_threadsafe(upload_completed.set)
        try:
            assert release_materialization.wait(timeout=5)
        finally:
            loop.call_soon_threadsafe(materialization_finished.set)
        return MaterializeOutput(
            task_rows=[
                TaskRowDraft(
                    id=f"{task_set_id}/tasks/crash-after-upload",
                    checksum="crash-after-upload-checksum",
                    config=_published_task_config("crash-after-upload"),
                    source=(
                        f"s3://{app.state.settings.artifacts_bucket}/"
                        f"{output_key.rsplit('/', 1)[0]}/"
                    ),
                ),
            ],
            task_count=1,
            status="ready",
            evaluation_ready=True,
        )

    monkeypatch.setattr(taskset_materializer, "materialize_task_set", stage_then_block)
    owner_task = asyncio.create_task(
        taskset_materializer._materialize_claimed_job(
            app.state.session_factory,
            job_id=lease.id,
            lease=lease,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(enabled=False, network_isolated=False),
        ),
    )
    try:
        await asyncio.wait_for(upload_completed.wait(), timeout=5)
        owner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(owner_task, timeout=5)
    finally:
        release_materialization.set()
        if upload_completed.is_set():
            await asyncio.wait_for(materialization_finished.wait(), timeout=5)

    async with app.state.session_factory() as session:
        job_after = await session.get(TaskSetMaterializationJob, lease.id)
        task_set_after = await session.get(TaskSet, task_set_id)
        rows_after = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert job_after is not None
    assert task_set_after is not None
    assert (
        job_after.state,
        job_after.lease_epoch,
        job_after.claimed_by,
        job_after.published_materialization_generation,
    ) == ("running", lease.lease_epoch, lease.claimed_by, 0)
    assert (
        task_set_after.status,
        task_set_after.task_count,
        task_set_after.evaluation_ready,
    ) == ("materializing", 0, False)
    assert rows_after == []
    assert (
        app.state.minio_client.get_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=output_key,
        )["Body"].read()
        == b"version = '1'\n"
    )


@pytest.mark.asyncio
async def test_materialization_stale_owner_cannot_overwrite_reclaimed_winner(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"
    loop = asyncio.get_running_loop()
    materialization_started = asyncio.Event()
    release_owner_a = threading.Event()
    materialization_calls = 0

    def materialize_for_owner_race(**_kwargs: object) -> MaterializeOutput:
        nonlocal materialization_calls
        materialization_calls += 1
        if materialization_calls == 1:
            loop.call_soon_threadsafe(materialization_started.set)
            assert release_owner_a.wait(timeout=10)
            return MaterializeOutput(
                status="failed",
                status_reason="owner_a_result",
                job_failure_reason="owner_a_result",
                job_failure_message="owner A resumed after reclaim",
            )
        return MaterializeOutput(
            status="failed",
            status_reason="owner_b_result",
            job_failure_reason="owner_b_result",
            job_failure_message="owner B is the fenced winner",
        )

    monkeypatch.setattr(
        taskset_materializer,
        "materialize_task_set",
        materialize_for_owner_race,
    )

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    lease_a = claimed_by_a[0]

    owner_a_task = asyncio.create_task(
        taskset_materializer._materialize_claimed_job(
            app.state.session_factory,
            job_id=lease_a.id,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(
                enabled=False,
                network_isolated=False,
            ),
        ),
    )
    await asyncio.wait_for(materialization_started.wait(), timeout=5)

    stale_at = datetime.now(UTC) - timedelta(minutes=5)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(claimed_at=stale_at, lease_heartbeat_at=stale_at),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 1
        )
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )
    assert len(claimed_by_b) == 1

    await taskset_materializer._materialize_claimed_job(
        app.state.session_factory,
        job_id=lease_a.id,
        minio_client=app.state.minio_client,
        artifacts_bucket=app.state.settings.artifacts_bucket,
        upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=False,
            network_isolated=False,
        ),
    )

    async with app.state.session_factory() as session:
        winner_before_resume = await session.get(
            TaskSetMaterializationJob,
            lease_a.id,
        )
        task_set_before_resume = await session.get(TaskSet, task_set_id)
    assert winner_before_resume is not None
    assert task_set_before_resume is not None
    winner_snapshot = (
        winner_before_resume.state,
        winner_before_resume.lease_epoch,
        winner_before_resume.claimed_by,
        winner_before_resume.failure_reason,
        task_set_before_resume.status,
        task_set_before_resume.status_reason,
    )

    release_owner_a.set()
    owner_a_result = (await asyncio.gather(owner_a_task, return_exceptions=True))[0]

    async with app.state.session_factory() as session:
        winner_after_resume = await session.get(
            TaskSetMaterializationJob,
            lease_a.id,
        )
        task_set_after_resume = await session.get(TaskSet, task_set_id)
    assert winner_after_resume is not None
    assert task_set_after_resume is not None
    assert (
        winner_after_resume.state,
        winner_after_resume.lease_epoch,
        winner_after_resume.claimed_by,
        winner_after_resume.failure_reason,
        task_set_after_resume.status,
        task_set_after_resume.status_reason,
    ) == winner_snapshot
    lease_lost = getattr(taskset_materializer, "LeaseLost", RuntimeError)
    assert isinstance(owner_a_result, lease_lost)


@pytest.mark.asyncio
async def test_materialization_staged_loser_cannot_publish_over_winner(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    lease_a = claimed_by_a[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_a)
    output_a = _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease_a,
        task_set_id=task_set_id,
        task_name="stale-owner-a",
    )

    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(lease_heartbeat_at=datetime.now(UTC) - timedelta(minutes=5)),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 1
        )
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )
    lease_b = claimed_by_b[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_b)
    output_b = _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease_b,
        task_set_id=task_set_id,
        task_name="winner-b",
    )

    async with app.state.session_factory() as session:
        await taskset_materializer.publish_if_current(
            session,
            lease=lease_b,
            task_set_id=task_set_id,
            output=output_b,
            claim_ttl_sec=60,
        )

    async with app.state.session_factory() as session:
        winner_rows_before = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        winner_task_set_before = await session.get(TaskSet, task_set_id)
        winner_job_before = await session.get(TaskSetMaterializationJob, lease_b.id)
    assert winner_task_set_before is not None
    assert winner_job_before is not None
    winner_snapshot = (
        winner_rows_before,
        winner_task_set_before.status,
        winner_task_set_before.task_count,
        winner_task_set_before.evaluation_ready,
        winner_job_before.published_materialization_generation,
    )
    assert winner_rows_before == ((f"{task_set_id}/tasks/winner-b", output_b.task_rows[0].source),)
    assert f"/materializations/{lease_b.job_id}/{lease_b.lease_epoch}/tasks/" in (
        output_b.task_rows[0].source
    )
    assert winner_job_before.published_materialization_generation == lease_b.lease_epoch

    async with app.state.session_factory() as session:
        with pytest.raises(taskset_materializer.LeaseLost):
            await taskset_materializer.publish_if_current(
                session,
                lease=lease_a,
                task_set_id=task_set_id,
                output=output_a,
                claim_ttl_sec=60,
            )

    # The stale executor's crash path must be the same strict no-op.
    async with app.state.session_factory() as session:
        with pytest.raises(taskset_materializer.LeaseLost):
            await taskset_materializer._fail_lease(
                session,
                lease=lease_a,
                failure_reason="stale_owner_crashed",
                failure_message="A resumed after B published",
                claim_ttl_sec=60,
            )

    async with app.state.session_factory() as session:
        winner_rows_after = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        winner_task_set_after = await session.get(TaskSet, task_set_id)
        winner_job_after = await session.get(TaskSetMaterializationJob, lease_b.id)
    assert winner_task_set_after is not None
    assert winner_job_after is not None
    assert (
        winner_rows_after,
        winner_task_set_after.status,
        winner_task_set_after.task_count,
        winner_task_set_after.evaluation_ready,
        winner_job_after.published_materialization_generation,
    ) == winner_snapshot


@pytest.mark.asyncio
async def test_materialization_relinquish_revokes_a_current_lease_for_reclaim(
    materialization_setup,
) -> None:
    """The deployment canary may only hand off through a fenced transition."""
    app, tokens, _teams = materialization_setup
    await _submit_inline_task_set(app, token=tokens["team_a"])

    async with app.state.session_factory() as session:
        lease_a = (
            await taskset_materializer._claim_jobs(
                session,
                batch_size=1,
                claimed_by="test-relinquish-a",
            )
        )[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_a)
    async with app.state.session_factory() as session:
        await taskset_materializer.relinquish_if_current(session, lease=lease_a)

    async with app.state.session_factory() as session:
        job_after_relinquish = await session.get(
            TaskSetMaterializationJob,
            lease_a.job_id,
        )
    assert job_after_relinquish is not None
    assert job_after_relinquish.state == "queued"
    assert job_after_relinquish.claimed_by is None
    assert job_after_relinquish.lease_heartbeat_at is None
    assert job_after_relinquish.started_at is None
    assert job_after_relinquish.lease_epoch == lease_a.lease_epoch + 1
    assert job_after_relinquish.next_attempt_at is None

    async with app.state.session_factory() as session:
        lease_b = (
            await taskset_materializer._claim_jobs(
                session,
                batch_size=1,
                claimed_by="test-relinquish-b",
                job_id=lease_a.job_id,
            )
        )[0]
    assert lease_b.job_id == lease_a.job_id
    assert lease_b.lease_epoch > lease_a.lease_epoch

    async with app.state.session_factory() as session:
        with pytest.raises(taskset_materializer.LeaseLost):
            await taskset_materializer._start_job(session, lease=lease_a)


@pytest.mark.asyncio
async def test_materialization_can_stage_a_current_lease_before_publication(
    materialization_setup,
) -> None:
    """The deployment canary stages through the worker's normal code path."""
    app, tokens, _teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    settings = app.state.settings

    async with app.state.session_factory() as session:
        lease = (
            await taskset_materializer._claim_jobs(
                session,
                batch_size=1,
                claimed_by="test-stage-only",
            )
        )[0]

    staged_lease, staged_task_set_id, output = await taskset_materializer.stage_claimed_job(
        app.state.session_factory,
        job_id=lease.job_id,
        lease=lease,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            workload_contract=settings.workload_contract,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
        max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
        max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
    )

    assert staged_lease == lease
    assert staged_task_set_id == task_set_id
    assert output.task_rows
    async with app.state.session_factory() as session:
        job = await session.get(TaskSetMaterializationJob, lease.job_id)
        published_rows = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert job is not None
    assert job.state == "running"
    assert job.claimed_by == lease.claimed_by
    assert published_rows == []


@pytest.mark.asyncio
async def test_deployment_fence_canary_preparation_creates_a_system_owned_one_use_authorization(
    materialization_setup,
) -> None:
    """A prepared canary is created and bound under deployment control only."""
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryContract,
        TaskSetFenceCanaryPreparationRequest,
        TaskSetFenceCanaryRuntimeError,
        _claim_authorized_disposable_job,
        prepare_deployment_fence_canary,
    )

    app, tokens, _teams = materialization_setup
    system_team_id = await _seed_canary_system_team(app)
    user_task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    settings = app.state.settings
    contract = await prepare_deployment_fence_canary(
        app.state.session_factory,
        request=TaskSetFenceCanaryPreparationRequest.from_mapping(
            {
                "candidate_sha": "a" * 40,
                "image_tag": "staging-aaaaaaa",
                "authorization_token": "deployment-only-capability",
            }
        ),
        configured_token="deployment-only-capability",
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        taskset_quota_max_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        manifest_max_bytes=settings.taskset_manifest_max_bytes,
        bundle_max_bytes=settings.taskset_quota_max_bundle_bytes,
        nonce_factory=lambda: "n" * 43,
    )

    assert contract.task_set_id != user_task_set_id
    assert contract.expected_task_checksum
    async with app.state.session_factory() as session:
        canary_task_set = await session.get(TaskSet, contract.task_set_id)
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )
    assert canary_task_set is not None
    assert canary_task_set.owning_team_id == system_team_id
    assert authorization is not None
    assert authorization.expected_task_checksum == contract.expected_task_checksum

    user_contract = TaskSetFenceCanaryContract.from_mapping(
        {
            "candidate_sha": "a" * 40,
            "image_tag": "staging-aaaaaaa",
            "task_set_id": user_task_set_id,
            "expected_task_checksum": "c" * 64,
            "authorization_token": "deployment-only-capability",
            "nonce": "n" * 43,
        }
    )

    async with app.state.session_factory() as session:
        user_job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == user_task_set_id,
                )
            )
        ).scalar_one()
        session.add(
            TaskSetFenceCanaryAuthorization(
                task_set_id=user_task_set_id,
                materialization_job_id=user_job.id,
                candidate_sha=user_contract.candidate_sha,
                image_tag=user_contract.image_tag,
                expected_task_checksum=user_contract.expected_task_checksum,
                nonce_digest=hashlib.sha256(user_contract.nonce.encode()).digest(),
            )
        )
        await session.commit()

    with pytest.raises(TaskSetFenceCanaryRuntimeError) as user_authorization:
        await _claim_authorized_disposable_job(
            app.state.session_factory,
            contract=user_contract,
            owner="canary-owner-a",
        )
    assert str(user_authorization.value) == "canary authorization was not available"

    lease = await _claim_authorized_disposable_job(
        app.state.session_factory,
        contract=contract,
        owner="canary-owner-a",
    )
    assert lease.claimed_by == "canary-owner-a"

    with pytest.raises(TaskSetFenceCanaryRuntimeError) as consumed_authorization:
        await _claim_authorized_disposable_job(
            app.state.session_factory,
            contract=contract,
            owner="canary-owner-b",
        )
    assert str(consumed_authorization.value) == "canary authorization was not available"

    async with app.state.session_factory() as session:
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                ),
            )
        ).scalar_one()
    assert authorization is not None
    assert authorization.consumed_at is not None
    assert authorization.materialization_job_id == lease.job_id
    assert authorization.consumed_lease_epoch == lease.lease_epoch
    assert job.state == "claimed"


@pytest.mark.asyncio
async def test_deployment_fence_canary_rejects_bound_contract_mismatch_without_claiming(
    materialization_setup,
) -> None:
    """A durable authorization cannot be replayed for another candidate or nonce."""
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryContract,
        TaskSetFenceCanaryRuntimeError,
        _claim_authorized_disposable_job,
    )

    app, _tokens, _teams = materialization_setup
    contract = await _prepare_deployment_fence_canary(app)
    mismatched_contract = TaskSetFenceCanaryContract.from_mapping(
        {
            "candidate_sha": "d" * 40,
            "image_tag": "staging-ddddddd",
            "task_set_id": contract.task_set_id,
            "expected_task_checksum": contract.expected_task_checksum,
            "authorization_token": "deployment-only-capability",
            "nonce": "n" * 43,
        }
    )

    with pytest.raises(TaskSetFenceCanaryRuntimeError) as exc_info:
        await _claim_authorized_disposable_job(
            app.state.session_factory,
            contract=mismatched_contract,
            owner="canary-owner-a",
        )
    assert str(exc_info.value) == "canary authorization was not available"

    async with app.state.session_factory() as session:
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                ),
            )
        ).scalar_one()
    assert authorization is not None
    assert authorization.consumed_at is None
    assert job.state == "queued"
    assert job.claimed_by is None


@pytest.mark.asyncio
async def test_deployment_fence_canary_rechecks_a_deleted_task_set_at_claim(
    materialization_setup,
) -> None:
    """Deletion after authorization cannot race into a canary materialization claim."""
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryRuntimeError,
        _claim_authorized_disposable_job,
    )

    app, _tokens, _teams = materialization_setup
    system_team_id = await _seed_canary_system_team(app)
    contract = await _prepare_deployment_fence_canary(app)
    async with app.state.session_factory() as session:
        await delete_task_set(
            session,
            team_id=system_team_id,
            task_set_id=contract.task_set_id,
        )

    with pytest.raises(TaskSetFenceCanaryRuntimeError) as exc_info:
        await _claim_authorized_disposable_job(
            app.state.session_factory,
            contract=contract,
            owner="canary-owner-a",
        )
    assert str(exc_info.value) == "canary authorization was not available"

    async with app.state.session_factory() as session:
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                ),
            )
        ).scalar_one()
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )
    assert job.state == "cancelled"
    assert job.claimed_by is None
    assert authorization is not None
    assert authorization.consumed_at is None


@pytest.mark.asyncio
async def test_deployment_fence_canary_retires_a_post_stage_mismatch(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed canary cannot retain an active system TaskSet."""
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryRuntimeError,
        run_deployment_fence_canary,
    )

    app, _tokens, _teams = materialization_setup
    settings = app.state.settings
    contract = await _prepare_deployment_fence_canary(app)

    def stage_wrong_checksum(**_kwargs: object) -> MaterializeOutput:
        return MaterializeOutput(
            task_rows=[
                TaskRowDraft(
                    id=f"{contract.task_set_id}/tasks/mismatch",
                    checksum="d" * 64,
                    config={},
                    source="s3://private-bucket/canary-mismatch",
                )
            ],
            task_count=1,
            status="ready",
            evaluation_ready=True,
        )

    monkeypatch.setattr(taskset_materializer, "materialize_task_set", stage_wrong_checksum)
    monkeypatch.setattr(taskset_materializer, "team_taskset_storage_bytes", lambda *_a, **_k: 0)

    with pytest.raises(TaskSetFenceCanaryRuntimeError) as exc_info:
        await run_deployment_fence_canary(
            contract,
            configured_token="deployment-only-capability",
            session_factory=app.state.session_factory,
            minio_client=app.state.minio_client,
            artifacts_bucket=settings.artifacts_bucket,
            upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(
                enabled=settings.taskset_materializer_transforms_enabled,
                network_isolated=settings.taskset_materializer_transform_network_isolated,
                workload_contract=settings.workload_contract,
                wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
                cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
                memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
            ),
            max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
            max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
            claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
            owner_factory=iter(("canary-owner-a", "canary-owner-b")).__next__,
        )
    assert str(exc_info.value) == "canary staged output did not match contract"

    async with app.state.session_factory() as session:
        task_set = await session.get(TaskSet, contract.task_set_id)
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                ),
            )
        ).scalar_one()
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )
    assert task_set is not None
    assert task_set.status == "deleted"
    assert task_set.soft_deleted_at is not None
    assert job.state == "cancelled"
    assert job.claimed_by is None
    assert job.lease_heartbeat_at is None
    assert job.started_at is None
    assert authorization is not None
    assert authorization.consumed_at is not None


@pytest.mark.asyncio
async def test_deployment_fence_canary_uses_normal_primitives_and_redacts_evidence(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployment runner is a canary, not a generic materializer control."""
    from loom_cli.taskset_fence_canary import (
        run_deployment_fence_canary,
    )

    app, _tokens, _teams = materialization_setup
    settings = app.state.settings
    raw_owner_a = "private-owner-a"
    raw_owner_b = "private-owner-b"
    destructive_operations: list[str] = []
    timestamps = iter(
        (
            datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2030, 1, 1, 0, 0, 1, tzinfo=UTC),
            datetime(2030, 1, 1, 0, 0, 2, tzinfo=UTC),
        )
    )

    def reject_delete(*_args: object, **_kwargs: object) -> None:
        destructive_operations.append("delete")
        raise AssertionError("the fence canary must not delete object storage")

    contract = await _prepare_deployment_fence_canary(app)

    def fake_stage(**kwargs: object) -> MaterializeOutput:
        job_id = kwargs["materialization_job_id"]
        epoch = kwargs["materialization_epoch"]
        assert isinstance(job_id, UUID)
        assert isinstance(epoch, int)
        return MaterializeOutput(
            task_rows=[
                TaskRowDraft(
                    id=f"{contract.task_set_id}/tasks/disposable",
                    checksum=contract.expected_task_checksum,
                    config={},
                    source=(
                        "s3://private-bucket/tasksets/user/private-team/"
                        f"materializations/{job_id}/{epoch}/tasks/disposable"
                    ),
                )
            ],
            task_count=1,
            status="ready",
            evaluation_ready=True,
        )

    monkeypatch.setattr(taskset_materializer, "materialize_task_set", fake_stage)
    monkeypatch.setattr(taskset_materializer, "team_taskset_storage_bytes", lambda *_a, **_k: 0)
    monkeypatch.setattr(app.state.minio_client, "delete_object", reject_delete)
    monkeypatch.setattr(app.state.minio_client, "delete_objects", reject_delete)

    evidence = await run_deployment_fence_canary(
        contract,
        configured_token="deployment-only-capability",
        session_factory=app.state.session_factory,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            workload_contract=settings.workload_contract,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
        max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
        max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        owner_factory=iter((raw_owner_a, raw_owner_b)).__next__,
        now=timestamps.__next__,
    )

    assert evidence == {
        "schema_version": 1,
        "candidate_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "task_set_id": contract.task_set_id,
        "winner": {
            "job_id": evidence["winner"]["job_id"],
            "lease_epoch": evidence["winner"]["lease_epoch"],
            "owner_fingerprint": "sha256:7194da2956c8",
            "published_generation": evidence["winner"]["lease_epoch"],
            "outcome": "published",
        },
        "loser": {
            "job_id": evidence["loser"]["job_id"],
            "lease_epoch": evidence["loser"]["lease_epoch"],
            "owner_fingerprint": "sha256:fb82085ccef8",
            "outcome": "fenced_before_publish",
            "gc_eligible": True,
        },
        "published_task": {
            "task_count": 1,
            "checksum": contract.expected_task_checksum,
        },
        "stale_cas_outcome": "LeaseLost",
        "timestamps": {
            "a_staged_at": "2030-01-01T00:00:00Z",
            "b_published_at": "2030-01-01T00:00:01Z",
            "a_lease_lost_at": "2030-01-01T00:00:02Z",
        },
    }
    assert evidence["winner"]["lease_epoch"] > evidence["loser"]["lease_epoch"]
    encoded_evidence = json.dumps(evidence, sort_keys=True)
    for forbidden in (
        raw_owner_a,
        raw_owner_b,
        "private-bucket",
        "private-team",
        "s3://",
        "source",
        "claimed_by",
    ):
        assert forbidden not in encoded_evidence
    assert destructive_operations == []


@pytest.mark.asyncio
async def test_deployment_fence_canary_materializes_its_bundle_without_mocks(
    materialization_setup,
) -> None:
    """The deployment-owned bundle reaches normal staged publication unmocked."""
    from loom_cli.taskset_fence_canary import run_deployment_fence_canary

    app, _tokens, _teams = materialization_setup
    settings = app.state.settings
    contract = await _prepare_deployment_fence_canary(app)

    evidence = await run_deployment_fence_canary(
        contract,
        configured_token="deployment-only-capability",
        session_factory=app.state.session_factory,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            workload_contract=settings.workload_contract,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
        max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
        max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        owner_factory=iter(("bundle-owner-a", "bundle-owner-b")).__next__,
    )

    async with app.state.session_factory() as session:
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == contract.task_set_id,
                )
            )
        ).scalar_one()
        tasks = (
            await session.execute(
                select(Task).where(Task.task_set_id == contract.task_set_id),
            )
        ).scalars().all()

    assert evidence["published_task"] == {
        "task_count": 1,
        "checksum": contract.expected_task_checksum,
    }
    assert job.state == "succeeded"
    assert len(tasks) == 1
    assert tasks[0].checksum == contract.expected_task_checksum
    assert tasks[0].source.startswith("s3://")


@pytest.mark.asyncio
async def test_deployment_fence_canary_retires_its_system_task_set_after_evidence(
    materialization_setup,
) -> None:
    """A successful rollout canary retires its disposable TaskSet."""
    from loom_cli.taskset_fence_canary import run_deployment_fence_canary

    app, _tokens, _teams = materialization_setup
    settings = app.state.settings
    contract = await _prepare_deployment_fence_canary(app)

    evidence = await run_deployment_fence_canary(
        contract,
        configured_token="deployment-only-capability",
        session_factory=app.state.session_factory,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            workload_contract=settings.workload_contract,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
        max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
        max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        owner_factory=iter(("retire-owner-a", "retire-owner-b")).__next__,
    )

    async with app.state.session_factory() as session:
        task_set = await session.get(TaskSet, contract.task_set_id)
        authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            contract.task_set_id,
        )

    assert evidence["stale_cas_outcome"] == "LeaseLost"
    assert task_set is not None
    assert task_set.status == "deleted"
    assert task_set.soft_deleted_at is not None
    assert authorization is not None
    assert authorization.consumed_at is not None


@pytest.mark.asyncio
async def test_deployment_fence_canary_preparation_cannot_select_a_noninitial_user_task_set(
    materialization_setup,
) -> None:
    """Preparation makes a new system canary instead of selecting a user TaskSet."""

    app, tokens, _teams = materialization_setup
    user_task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.task_set_id == user_task_set_id)
            .values(attempt_count=1),
        )
        await session.commit()

    contract = await _prepare_deployment_fence_canary(app)

    assert contract.task_set_id != user_task_set_id
    async with app.state.session_factory() as session:
        user_authorization = await session.get(
            TaskSetFenceCanaryAuthorization,
            user_task_set_id,
        )
    assert user_authorization is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handoff_pause_sec", (0, 1.1))
async def test_materialization_cooperative_two_owner_canary_records_safe_evidence(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
    handoff_pause_sec: float,
) -> None:
    """Exercise the staging-canary handoff without a kill, GC, or external state."""
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "cooperative-canary-owner-a-raw"
    owner_b = "cooperative-canary-owner-b-raw"
    claim_ttl_sec = 60

    class CanaryClock:
        current = datetime(2030, 1, 1, tzinfo=UTC)

        @classmethod
        def now(cls, _timezone: object) -> datetime:
            return cls.current

    destructive_object_operations: list[str] = []

    def reject_destructive_object_operation(*_args: object, **_kwargs: object) -> None:
        destructive_object_operations.append("attempted")
        raise AssertionError("the cooperative canary must not delete objects")

    monkeypatch.setattr(taskset_materializer, "datetime", CanaryClock)
    monkeypatch.setattr(
        app.state.minio_client,
        "delete_object",
        reject_destructive_object_operation,
    )
    monkeypatch.setattr(
        app.state.minio_client,
        "delete_objects",
        reject_destructive_object_operation,
    )

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    lease_a = claimed_by_a[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease_a)

    a_staged = asyncio.Event()
    allow_a_resume = asyncio.Event()

    async def stage_then_resume_as_a() -> tuple[MaterializeOutput, datetime]:
        output = _stage_output_for_lease(
            app,
            team_id=teams["team_a"],
            slug="inline-tasks",
            lease=lease_a,
            task_set_id=task_set_id,
            task_name="cooperative-loser-a",
        )
        a_staged.set()
        await allow_a_resume.wait()
        async with app.state.session_factory() as session:
            with pytest.raises(taskset_materializer.LeaseLost):
                await taskset_materializer.publish_if_current(
                    session,
                    lease=lease_a,
                    task_set_id=task_set_id,
                    output=output,
                    claim_ttl_sec=claim_ttl_sec,
                )
        return output, CanaryClock.current

    # One deadlock budget bounds the whole handoff. TaskGroup cancels and joins
    # owner A if staging or owner B fails; no orphan waiter survives the test.
    async with asyncio.timeout(30), asyncio.TaskGroup() as owners:
        owner_a_task = owners.create_task(stage_then_resume_as_a())
        await a_staged.wait()
        # Deliberately exercise a slow handoff. Only CanaryClock expires leases.
        await asyncio.sleep(handoff_pause_sec)

        # Reclaim uses the production CAS path, without editing a row directly
        # or killing any driver, pod or object.
        CanaryClock.current += timedelta(seconds=claim_ttl_sec + 1)
        async with app.state.session_factory() as session:
            assert (
                await taskset_materializer.reclaim_stale_jobs(
                    session, claim_ttl_sec=claim_ttl_sec,
                )
                == 1
            )

        # The reclaimer applies the normal retry delay before B can claim.
        CanaryClock.current += timedelta(seconds=31)
        async with app.state.session_factory() as session:
            claimed_by_b = await taskset_materializer._claim_jobs(
                session, batch_size=1, claimed_by=owner_b,
            )
        assert len(claimed_by_b) == 1
        lease_b = claimed_by_b[0]
        assert lease_b.id == lease_a.id
        assert lease_b.lease_epoch > lease_a.lease_epoch

        async with app.state.session_factory() as session:
            await taskset_materializer._start_job(session, lease=lease_b)
        output_b = _stage_output_for_lease(
            app, team_id=teams["team_a"], slug="inline-tasks", lease=lease_b,
            task_set_id=task_set_id, task_name="cooperative-winner-b",
        )
        async with app.state.session_factory() as session:
            await taskset_materializer.publish_if_current(
                session, lease=lease_b, task_set_id=task_set_id,
                output=output_b, claim_ttl_sec=claim_ttl_sec,
            )
        b_published_at = CanaryClock.current

        allow_a_resume.set()
        output_a, a_lost_at = await owner_a_task

    async with app.state.session_factory() as session:
        winner_job = await session.get(TaskSetMaterializationJob, lease_b.id)
        winner_rows = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert winner_job is not None
    assert winner_job.published_materialization_generation == lease_b.lease_epoch
    assert [(row.id, row.checksum) for row in winner_rows] == [
        (output_b.task_rows[0].id, output_b.task_rows[0].checksum),
    ]

    loser_prefix = (
        f"tasksets/user/{teams['team_a']}/inline-tasks/materializations/"
        f"{lease_a.job_id}/{lease_a.lease_epoch}/"
    )
    assert _object_keys(app, prefix=loser_prefix)
    assert output_a.task_rows[0].source != output_b.task_rows[0].source
    assert destructive_object_operations == []

    def fingerprint(owner: str) -> str:
        return f"sha256:{hashlib.sha256(owner.encode()).hexdigest()[:12]}"

    evidence = {
        "candidate_sha": "<candidate-sha>",
        "image_tag": "<candidate-image-tag>",
        "task_set_id": task_set_id,
        "winner": {
            "job_id": str(lease_b.job_id),
            "lease_epoch": lease_b.lease_epoch,
            "owner_fingerprint": fingerprint(owner_b),
            "published_generation": winner_job.published_materialization_generation,
            "outcome": "published",
        },
        "loser": {
            "job_id": str(lease_a.job_id),
            "lease_epoch": lease_a.lease_epoch,
            "owner_fingerprint": fingerprint(owner_a),
            "outcome": "fenced_before_publish",
            "gc_eligible": True,
        },
        "published_task": {
            "id": output_b.task_rows[0].id,
            "checksum": output_b.task_rows[0].checksum,
            "task_count": len(winner_rows),
        },
        "stale_cas_outcome": "LeaseLost",
        "timestamps": {
            "a_staged_at": "2030-01-01T00:00:00Z",
            "b_published_at": b_published_at.isoformat().replace("+00:00", "Z"),
            "a_lease_lost_at": a_lost_at.isoformat().replace("+00:00", "Z"),
        },
    }
    encoded_evidence = json.dumps(evidence, sort_keys=True)
    assert evidence["winner"]["published_generation"] == lease_b.lease_epoch
    assert evidence["loser"]["gc_eligible"] is True
    for forbidden in (owner_a, owner_b, "claimed_by", "s3://", "source"):
        assert forbidden not in encoded_evidence


@pytest.mark.asyncio
async def test_materialization_db_failure_after_staging_keeps_published_generation(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    old_source = (
        f"s3://{app.state.settings.artifacts_bucket}/tasksets/user/{teams['team_a']}/"
        "inline-tasks/materializations/original-job/0/tasks/old/"
    )

    async with app.state.session_factory() as session:
        task_set = await session.get(TaskSet, task_set_id)
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()
        assert task_set is not None
        task_set.status = "ready"
        task_set.task_count = 1
        task_set.evaluation_ready = True
        job.published_materialization_generation = 0
        session.add(
            Task(
                id=f"{task_set_id}/tasks/old",
                checksum="old-checksum",
                config={"task": {"id": "old"}},
                source=old_source,
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
        await session.commit()

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="db-error-owner",
        )
    lease = claimed[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease)
    staged_output = _stage_output_for_lease(
        app,
        team_id=teams["team_a"],
        slug="inline-tasks",
        lease=lease,
        task_set_id=task_set_id,
        task_name="new",
    )
    duplicate_output = MaterializeOutput(
        task_rows=staged_output.task_rows * 2,
        task_count=2,
        status="ready",
        evaluation_ready=True,
    )

    async with app.state.session_factory() as session:
        with pytest.raises(IntegrityError):
            await taskset_materializer.publish_if_current(
                session,
                lease=lease,
                task_set_id=task_set_id,
                output=duplicate_output,
                claim_ttl_sec=60,
            )

    async with app.state.session_factory() as session:
        rows_after_failure = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        task_set_after_failure = await session.get(TaskSet, task_set_id)
        job_after_failure = await session.get(TaskSetMaterializationJob, lease.id)
    assert task_set_after_failure is not None
    assert job_after_failure is not None
    assert rows_after_failure == ((f"{task_set_id}/tasks/old", old_source),)
    assert task_set_after_failure.status == "ready"
    assert task_set_after_failure.task_count == 1
    assert task_set_after_failure.evaluation_ready is True
    assert job_after_failure.state == "running"
    assert job_after_failure.published_materialization_generation == 0
    assert (
        app.state.minio_client.get_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=(
                f"tasksets/user/{teams['team_a']}/inline-tasks/materializations/"
                f"{lease.job_id}/{lease.lease_epoch}/tasks/new/task.toml"
            ),
        )["Body"].read()
        == b"version = '1'\n"
    )
    assert (
        app.state.minio_client.list_objects_v2(
            Bucket=app.state.settings.artifacts_bucket,
            Prefix=f"tasksets/user/{teams['team_a']}/inline-tasks/tasks/",
        ).get("Contents", [])
        == []
    )

    staged_prefix = (
        f"tasksets/user/{teams['team_a']}/inline-tasks/materializations/"
        f"{lease.job_id}/{lease.lease_epoch}/"
    )
    async with app.state.session_factory() as session:
        active_gc = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
        )
    assert active_gc.deleted_objects == 0
    assert _object_keys(app, prefix=staged_prefix)

    async with app.state.session_factory() as session:
        await taskset_materializer._fail_lease(
            session,
            lease=lease,
            failure_reason="db_error_terminalized",
            failure_message="the forced database error left staged output",
            claim_ttl_sec=60,
        )
    async with app.state.session_factory() as session:
        terminal_gc = await purge_abandoned_materialization_generations(
            session,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
        )
        rows_after_gc = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
    assert terminal_gc.deleted_objects == 1
    assert _object_keys(app, prefix=staged_prefix) == set()
    assert rows_after_gc == rows_after_failure


@pytest.mark.asyncio
async def test_empty_terminal_failure_keeps_prior_published_generation(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    old_source = (
        f"s3://{app.state.settings.artifacts_bucket}/tasksets/user/{teams['team_a']}/"
        "inline-tasks/materializations/previous-job/7/tasks/old/"
    )

    async with app.state.session_factory() as session:
        task_set = await session.get(TaskSet, task_set_id)
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()
        assert task_set is not None
        task_set.status = "ready"
        task_set.status_reason = "published_generation"
        task_set.task_count = 1
        task_set.evaluation_ready = True
        job.lease_epoch = 7
        job.published_materialization_generation = 7
        session.add(
            Task(
                id=f"{task_set_id}/tasks/old",
                checksum="old-checksum",
                config={"task": {"id": "old"}},
                source=old_source,
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
        await session.commit()

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="empty-failure-owner",
        )
    lease = claimed[0]
    assert lease.lease_epoch == 8
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease)
        await taskset_materializer.publish_if_current(
            session,
            lease=lease,
            task_set_id=task_set_id,
            output=MaterializeOutput(
                status="failed",
                status_reason="transform_unavailable_in_internal_trusted",
                job_failure_reason="transform_unavailable_in_internal_trusted",
            ),
            claim_ttl_sec=60,
        )

    async with app.state.session_factory() as session:
        rows = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        task_set_after = await session.get(TaskSet, task_set_id)
        job_after = await session.get(TaskSetMaterializationJob, lease.id)
    assert task_set_after is not None
    assert job_after is not None
    assert rows == ((f"{task_set_id}/tasks/old", old_source),)
    assert (
        task_set_after.status,
        task_set_after.status_reason,
        task_set_after.task_count,
        task_set_after.evaluation_ready,
    ) == ("ready", "published_generation", 1, True)
    assert job_after.state == "failed"
    assert job_after.failure_reason == "transform_unavailable_in_internal_trusted"
    assert job_after.published_materialization_generation == 7


@pytest.mark.asyncio
async def test_empty_failed_rebuild_keeps_published_task_set_state(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    await _run_materializer_once(app)

    async with app.state.session_factory() as session:
        published_task_set = await session.get(TaskSet, task_set_id)
        published_job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()
        published_rows = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
    assert published_task_set is not None
    assert published_job.state == "succeeded"
    published_task_set_state = (
        published_task_set.status,
        published_task_set.status_reason,
        published_task_set.task_count,
        published_task_set.evaluation_ready,
    )
    published_generation = published_job.published_materialization_generation

    async with app.state.session_factory() as session:
        rebuild = await rebuild_task_set(
            session,
            team_id=teams["team_a"],
            task_set_id=task_set_id,
        )
        task_set_after_enqueue = await session.get(TaskSet, task_set_id)
        rebuilt_job = await session.get(TaskSetMaterializationJob, rebuild.job_id)
    assert task_set_after_enqueue is not None
    assert rebuilt_job is not None
    assert (
        task_set_after_enqueue.status,
        task_set_after_enqueue.status_reason,
        task_set_after_enqueue.task_count,
        task_set_after_enqueue.evaluation_ready,
    ) == published_task_set_state
    assert rebuilt_job.state == "queued"

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="empty-rebuild-owner",
        )
    assert len(claimed) == 1
    lease = claimed[0]
    assert lease.id == rebuild.job_id

    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease)
        await taskset_materializer.publish_if_current(
            session,
            lease=lease,
            task_set_id=task_set_id,
            output=MaterializeOutput(
                status="failed",
                status_reason="transform_unavailable_in_internal_trusted",
                job_failure_reason="transform_unavailable_in_internal_trusted",
            ),
            claim_ttl_sec=60,
        )

    async with app.state.session_factory() as session:
        rows_after_failure = tuple(
            sorted(
                (row.id, row.source)
                for row in (
                    await session.execute(
                        select(Task).where(Task.task_set_id == task_set_id),
                    )
                )
                .scalars()
                .all()
            )
        )
        task_set_after_failure = await session.get(TaskSet, task_set_id)
        latest_job_after_failure = await get_latest_job(session, task_set_id)
        published_job_after_failure = await session.get(
            TaskSetMaterializationJob,
            published_job.id,
        )
    assert task_set_after_failure is not None
    assert latest_job_after_failure is not None
    assert published_job_after_failure is not None
    assert rows_after_failure == published_rows
    assert (
        task_set_after_failure.status,
        task_set_after_failure.status_reason,
        task_set_after_failure.task_count,
        task_set_after_failure.evaluation_ready,
    ) == published_task_set_state
    assert latest_job_after_failure.id == rebuild.job_id
    assert latest_job_after_failure.state == "failed"
    assert latest_job_after_failure.failure_reason == "transform_unavailable_in_internal_trusted"
    assert published_job_after_failure.published_materialization_generation == published_generation


@pytest.mark.asyncio
async def test_expired_lease_cannot_fail_before_reclaim(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])

    async with app.state.session_factory() as session:
        claimed = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by="expired-crash-owner",
        )
    lease = claimed[0]
    async with app.state.session_factory() as session:
        await taskset_materializer._start_job(session, lease=lease)
        stale_at = datetime.now(UTC) - timedelta(minutes=5)
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease.id)
            .values(lease_heartbeat_at=stale_at),
        )
        await session.commit()

    async with app.state.session_factory() as session:
        with pytest.raises(taskset_materializer.LeaseLost):
            await taskset_materializer._fail_lease(
                session,
                lease=lease,
                failure_reason="internal_error",
                failure_message="materialization worker crashed",
                claim_ttl_sec=60,
            )

    async with app.state.session_factory() as session:
        job_after = await session.get(TaskSetMaterializationJob, lease.id)
        task_set_after = await session.get(TaskSet, task_set_id)
    assert job_after is not None
    assert task_set_after is not None
    assert (
        job_after.state,
        job_after.lease_epoch,
        job_after.claimed_by,
        job_after.failure_reason,
        job_after.failure_message,
        job_after.finished_at,
    ) == ("running", lease.lease_epoch, lease.claimed_by, None, None, None)
    assert (
        task_set_after.status,
        task_set_after.status_reason,
        task_set_after.task_count,
        task_set_after.evaluation_ready,
    ) == ("materializing", None, 0, False)


@pytest.mark.asyncio
async def test_materialization_rejects_stale_lease_for_every_state_transition(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    initial_job = claimed_by_a[0]
    stale_at = datetime.now(UTC) - timedelta(minutes=5)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == initial_job.id)
            .values(claimed_at=stale_at, lease_heartbeat_at=stale_at),
        )
        await session.commit()
        assert (
            await taskset_materializer.reclaim_stale_jobs(
                session,
                claim_ttl_sec=60,
            )
            == 1
        )
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == initial_job.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )
    assert len(claimed_by_b) == 1

    lease_type = getattr(taskset_materializer, "MaterializationLease", None)
    lease_lost = getattr(taskset_materializer, "LeaseLost", None)
    transition = getattr(taskset_materializer, "_update_job_for_lease", None)
    assert lease_type is not None
    assert lease_lost is not None
    assert callable(transition)
    stale_lease = lease_type(
        job_id=initial_job.id,
        lease_epoch=initial_job.lease_epoch,
        claimed_by=owner_a,
    )
    now = datetime.now(UTC)
    transitions = (
        (("claimed", "running"), {"lease_heartbeat_at": now}),
        (("claimed",), {"state": "running", "started_at": now}),
        (("running",), {"state": "queued", "next_attempt_at": now}),
        (("running",), {"state": "failed", "finished_at": now}),
    )
    for states, values in transitions:
        async with app.state.session_factory() as session:
            with pytest.raises(lease_lost):
                await transition(
                    session,
                    lease=stale_lease,
                    states=states,
                    values=values,
                )


@pytest.mark.asyncio
async def test_materialization_e2e_jsonl_inline(materialization_setup) -> None:
    app, tokens, teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_INLINE.encode(),
                    "application/x-yaml",
                ),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        assert task_set_id == f"ts/{teams['team_a']}/inline-tasks"

        await _run_materializer_once(app)

        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["task_count"] == 2
    assert body["materialization_job_state"] == "succeeded"

    async with app.state.session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert all(row.source and row.source.startswith("s3://") for row in rows)


@pytest.mark.asyncio
async def test_materialization_partial_status(materialization_setup) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_PARTIAL.encode(),
                    "application/x-yaml",
                ),
            },
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "partial"
    assert body["task_count"] == 3
    assert len(body["error_summary"]) >= 2


@pytest.mark.asyncio
async def test_materialization_transform_fails_when_gates_disabled(materialization_setup) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_TRANSFORM.encode(),
                    "application/x-yaml",
                ),
                "transform": ("transform.py", b"def transform(row): return row", "text/x-python"),
            },
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "transform_unavailable_in_internal_trusted"
    assert body["materialization_job_state"] == "failed"


@pytest.mark.asyncio
async def test_materialization_transform_rejects_legacy_gates_before_fetch_or_run(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED", "true")
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED", "true")
    app, tokens, _teams = materialization_setup
    app.state.settings = LoomServiceSettings(_env_file=None)
    transport = ASGITransport(app=app)
    with (
        patch(
            "loom.taskset.materialize._fetch_blob_bytes",
            side_effect=AssertionError("v1 rejection must not fetch blobs"),
        ) as fetch_blob,
        patch(
            "loom.taskset.transform_sandbox.run_transform",
            side_effect=AssertionError("v1 rejection must not invoke a runner"),
        ) as run,
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            post = await client.post(
                "/api/v1/tasksets",
                headers={"Authorization": f"Bearer {tokens['team_a']}"},
                files={
                    "manifest": (
                        "manifest.yaml",
                        _MANIFEST_TRANSFORM.encode(),
                        "application/x-yaml",
                    ),
                    "transform": (
                        "transform.py",
                        b"def transform(row): return row",
                        "text/x-python",
                    ),
                },
            )
            assert post.status_code == 202
            task_set_id = post.json()["task_set_id"]
            await _run_materializer_once(app)
            get_resp = await client.get(
                f"/api/v1/tasksets/{task_set_id}",
                headers={"Authorization": f"Bearer {tokens['team_a']}"},
            )
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "transform_unavailable_in_internal_trusted"
    assert body["task_count"] == 0
    assert body["materialization_job_state"] == "failed"
    fetch_blob.assert_not_called()
    run.assert_not_called()


@pytest.mark.asyncio
async def test_materialization_rejects_incompatible_task_bundle(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    verifier = (
        b"FROM debian:bookworm\n"
        b"COPY broken_resolv.conf /app/broken_resolv.conf\n"
        b"RUN cp /app/broken_resolv.conf /etc/resolv.conf\n"
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_DNS_INCOMPATIBLE.encode(),
                    "application/x-yaml",
                ),
                "verifier": ("environment/Dockerfile", verifier, "text/x-dockerfile"),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "bundle_compatibility_error"
    assert body["task_count"] == 0
    assert body["materialization_job_state"] == "failed"
    assert body["error_summary"] == [
        {
            "row": "1",
            "code": "TASK_COMPAT_DNS_MUTATION",
            "severity": "error",
            "path": "environment/Dockerfile",
            "line": "3",
            "phase": "agent_layer_build",
            "message": (
                "Dockerfile mutates DNS/NSS configuration before Loom installs "
                "the service-mode agent layer"
            ),
            "hint": (
                "Do not change /etc/resolv.conf in the task image before agent "
                "setup. Move network breakage into the task runtime or verifier "
                "phase after the agent is installed. Move DNS breakage into a "
                "phase that runs after Loom has installed the agent layer."
            ),
            "evidence": '{"target":"/etc/resolv.conf"}',
        }
    ]

    async with app.state.session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set_id),
                )
            )
            .scalars()
            .all()
        )
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()
    assert rows == []
    assert job.failure_reason == "bundle_compatibility_error"


@pytest.mark.asyncio
async def test_materialization_e2e_bundle_upload_preserves_per_task_assets(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    settings = app.state.settings
    bundle_archive = _bundle_tar_bytes()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_BUNDLE_UPLOAD.encode(),
                    "application/x-yaml",
                ),
                "bundle": ("bundle.tar.gz", bundle_archive, "application/gzip"),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        assert task_set_id == f"ts/{teams['team_a']}/bundle-upload-tasks"
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["task_count"] == 1
    assert body["evaluation_ready"] is True
    assert body["materialization_job_state"] == "succeeded"

    async with app.state.session_factory() as session:
        row = (
            await session.execute(
                select(Task).where(Task.task_set_id == task_set_id),
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set_id,
                ),
            )
        ).scalar_one()

    assert row.id == (f"{task_set_id}/tasks/source-useful-frontier-5003_alpha")
    assert row.config["task"]["id"] == "source-useful-frontier-5003/alpha"
    assert row.config["environment"]["os"] == "linux"
    assert row.config["verifier"]["args"]["script_path"] == "verifier/check.sh"
    output_prefix = (
        f"tasksets/user/{teams['team_a']}/bundle-upload-tasks/materializations/"
        f"{job.id}/{job.published_materialization_generation}/tasks/"
        "source-useful-frontier-5003_alpha"
    )
    assert row.source == f"s3://{settings.artifacts_bucket}/{output_prefix}/"
    data_key = f"{output_prefix}/data/input.txt"
    assert app.state.minio_client.get_object(
        Bucket=settings.artifacts_bucket,
        Key=f"tasksets/user/{teams['team_a']}/bundle-upload-tasks/manifest.yaml",
    )["Body"].read()
    assert (
        app.state.minio_client.get_object(
            Bucket=settings.artifacts_bucket,
            Key=f"tasksets/user/{teams['team_a']}/bundle-upload-tasks/bundle.tar.gz",
        )["Body"].read()
        == bundle_archive
    )
    assert (
        app.state.minio_client.list_objects_v2(
            Bucket=settings.artifacts_bucket,
            Prefix=f"tasksets/user/{teams['team_a']}/bundle-upload-tasks/tasks/",
        ).get("Contents", [])
        == []
    )
    payload = app.state.minio_client.get_object(
        Bucket=settings.artifacts_bucket,
        Key=data_key,
    )["Body"].read()
    assert payload == b"per-task payload\n"


@pytest.mark.parametrize("solver_path,verifier", [
    ("solution/solve.sh", "script"), ("solution/other.sh", "script"),
    (None, "script"), ("solution/solve.sh/child", "script"), (None, "pytest"),
])
async def test_uploaded_solver_controls_oracle_admission(materialization_setup, solver_path, verifier):
    """Publishing without source-derived capability rejects a real Oracle bundle."""
    app, tokens, teams = materialization_setup
    archive = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(_bundle_tar_bytes()), mode="r:gz") as original:
        with tarfile.open(fileobj=archive, mode="w:gz") as target:
            for member in original.getmembers():
                content = original.extractfile(member).read()
                if member.name.endswith("task.toml") and verifier == "pytest":
                    content = content.replace(
                        b'name = "script"\n\n[verifier.args]\nscript_path = "verifier/check.sh"',
                        b'name = "pytest"',
                    )
                _add_tar_file(target, member.name, content)
            if solver_path is not None:
                _add_tar_file(target, f"tasks/alpha/{solver_path}", b"#!/bin/sh\necho answer\n")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", _MANIFEST_BUNDLE_UPLOAD.encode(), "application/x-yaml"),
                "bundle": ("bundle.tar.gz", archive.getvalue(), "application/gzip"),
            },
        )
    assert response.status_code == 202, response.text
    task_set_id = response.json()["task_set_id"]
    await _run_materializer_once(app)
    async with app.state.session_factory() as session:
        task = (await session.scalars(select(Task).where(Task.task_set_id == task_set_id))).one()
        admission = validate_submission_agent_task_compatibility(
            session, team_id=teams['team_a'], task_ids=[task.id],
            trial_config={"agent_name": "oracle", "agent_model": None},
        )
        if solver_path == "solution/solve.sh":
            await admission
            assert task.tags == {"oracle_eligible": "true"}
        else:
            with pytest.raises(HTTPException, match="solution_solve_sh") as rejection:
                await admission
            assert rejection.value.status_code == 400
            assert task.tags == {"oracle_eligible": "false"}
        # Simulate stale metadata in this disposable DB, then let the ordinary
        # rebuild recompute it from the unchanged immutable uploaded archive.
        task.tags = {"oracle_eligible": "false" if solver_path == "solution/solve.sh" else "true"}
        await session.commit()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        rebuilt = await client.post(
            f"/api/v1/tasksets/{task_set_id}/rebuild",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert rebuilt.status_code == 202, rebuilt.text
    await _run_materializer_once(app)
    async with app.state.session_factory() as session:
        task = (await session.scalars(select(Task).where(Task.task_set_id == task_set_id))).one()
        assert task.tags == {"oracle_eligible": "true" if solver_path == "solution/solve.sh" else "false"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "archive_factory",
    [
        _unsafe_symlink_bundle_tar_bytes,
        _unsafe_traversal_bundle_tar_bytes,
    ],
)
async def test_materialization_rejects_bundle_upload_unsafe_archives(
    materialization_setup,
    archive_factory: Callable[[], bytes],
) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_BUNDLE_UPLOAD.encode(),
                    "application/x-yaml",
                ),
                "bundle": (
                    "bundle.tar.gz",
                    archive_factory(),
                    "application/gzip",
                ),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "bundle_extract_unsafe"
    assert body["materialization_job_state"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_limit", [None, 1])
async def test_submission_above_former_count_limits(
    materialization_setup, legacy_limit: int | None,
) -> None:
    """Many small sets must not block intake while storage still fits."""
    from sqlalchemy import text

    app, tokens, teams = materialization_setup
    async with app.state.session_factory() as session:
        for index in range(51):
            slug = f"existing-{index}"
            session.add(TaskSet(
                id=f"ts/{teams['team_a']}/{slug}",
                owning_team_id=teams["team_a"], slug=slug, display_name=slug,
                status="ready", intents=["trajectory_generation"],
                manifest_blob_uri=f"s3://{app.state.settings.artifacts_bucket}/{slug}",
            ))
        # Retained historical overrides must no longer affect admission.
        await session.execute(text(
            "UPDATE team_quotas SET taskset_max_count = :limit WHERE team_id = :team"
        ), {"limit": legacy_limit, "team": teams["team_a"]})
        await session.commit()
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    async with app.state.session_factory() as session:
        created = await session.get(TaskSet, task_set_id)
        assert created is not None and created.status == "materializing"
        manifest = await session.scalar(select(TaskSetManifest).where(
            TaskSetManifest.task_set_id == task_set_id,
        ))
        assert manifest is not None
    bucket, key = created.manifest_blob_uri.removeprefix("s3://").split("/", 1)
    stored = app.state.minio_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert b"inline-tasks" in stored
