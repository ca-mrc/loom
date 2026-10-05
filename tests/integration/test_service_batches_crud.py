"""Batches CRUD: POST creates with materialized expected count,
GET lists + detail with rollup, cancel cascades (Plan 19 Task 3)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, func, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    AdminAuditEvent,
    Batch,
    Benchmark,
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    LlmCall,
    ProviderConnection,
    ProviderModelCache,
    RateCard,
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Task,
    TaskSet,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    Trial,
    User,
    Worker,
)
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
)
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_llm_gateway.rate_card import RateCardTable, hash_table
from loom_service import agent_catalog
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.provider_connection_fixtures import insert_openai_connection
from tests.support.execution_image_admission import signed_image_admission_bundle

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _service_execution_task_config(task_id: str) -> dict[str, object]:
    task_image = "registry.example/task@sha256:" + "a" * 64
    runtime_image = "registry.example/runtime@sha256:" + "b" * 64
    resources = ContainerResourcesV1(
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
    )
    plan = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=task_image,
        runtime_image_ref=runtime_image,
        runtime_binary_sha256="sha256:" + "4" * 64,
        image_admission=signed_image_admission_bundle((task_image, runtime_image)),
        task_resources=resources,
        workspace_mib=1024,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
    )
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {
            "os": "linux",
            "docker_image": task_image,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "baseline_network_policy": {"kind": "gateway-only"},
            "network_policies_supported": ["gateway-only"],
        },
        "agent": {"name": "direct-completion"},
        "verifier": {"name": "script"},
        "service_execution": {
            "schema_version": "loom.task-service-execution.v1",
            "logical_pool_id": "nebius-cpu",
            "runtime_template": plan.model_dump(
                mode="json",
                exclude={"task_revision_sha256"},
            ),
        },
        "steps": [{"name": "main"}],
    }


def _automatic_service_execution_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "docker_image": "registry.example/task@sha256:" + "a" * 64,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "baseline_network_policy": {"kind": "gateway-only"},
            "network_policies_supported": ["gateway-only"],
        },
        "agent": {"name": "direct-completion"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/check.sh"},
        },
        "steps": [
            {
                "name": "main",
                "instruction_file": "instruction.md",
                "artifacts": ["answer.txt"],
            }
        ],
    }


def _service_execution_runtime_profile() -> ServiceExecutionRuntimeProfileV1:
    task_image = "registry.example/task@sha256:" + "a" * 64
    runtime_image = "registry.example/runtime@sha256:" + "b" * 64
    return ServiceExecutionRuntimeProfileV1(
        candidate_sha="1" * 40,
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        task_image_ref=task_image,
        runtime_image_ref=runtime_image,
        runtime_binary_sha256="sha256:" + "4" * 64,
        image_admission=signed_image_admission_bundle((task_image, runtime_image)),
    )


def _script_verifier_task_config(task_id: str) -> dict[str, object]:
    """Used by tests covering oracle×non-pytest-verifier preflight
    rejection (#320). Mirrors the aime/gpqa/mmlu-pro shape: the
    verifier is `script`, so the bundle ships no `solution/solve.sh`
    and oracle can't run it."""
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "/workspace/verifier/run.sh"},
        },
        "steps": [{"name": "main"}],
    }


def _workspace_task_config(task_id: str) -> dict[str, object]:
    config = _script_verifier_task_config(task_id)
    config["required_agent_capabilities"] = ["workspace_exec"]
    return config


def _counter_value(
    metric_name: str,
    sample_name: str,
    labels: dict[str, str],
) -> float:
    from prometheus_client import REGISTRY

    for metric in REGISTRY.collect():
        if metric.name != metric_name:
            continue
        for sample in metric.samples:
            if sample.name == sample_name and all(
                sample.labels.get(k) == v for k, v in labels.items()
            ):
                return float(sample.value)
    return 0.0


@pytest.fixture
async def camp_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID]]:
    for k, v in {
        "LOOM_ENV": "development",
        "LOOM_LOCAL_EXECUTION": "1",
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    team_id = uuid4()
    username = f"BatchOwner-{team_id.hex[:8]}"
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)

    def control_plane(request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or not request.url.path.endswith("/cancel"):
            return httpx.Response(404)
        trial_id = UUID(request.url.path.split("/")[-2])
        with sl() as session:
            trial = session.get(Trial, trial_id)
            if trial is None:
                return httpx.Response(404)
            if trial.state in {"succeeded", "failed", "cancelled"}:
                return httpx.Response(409)
            trial.state = "cancelled"
            trial.cancellation_requested_at = datetime.now(UTC)
            trial.finished_at = datetime.now(UTC)
            session.commit()
        return httpx.Response(200, json={"state": "cancelled"})

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(control_plane),
        base_url="http://cp",
    )
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(TeamMembership).values(
                team_id=team_id,
                user_id=user_id,
                role="owner",
            )
        )
        # 3 MIT tasks + 2 Apache to test license-filter materialization.
        for i in range(3):
            tid = f"local/mit-{i}"
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="x" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="MIT",
                )
            )
        for i in range(2):
            tid = f"local/apache-{i}"
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="x" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="Apache-2.0",
                )
            )
        # Live worker advertising every backend Loom ships drivers for —
        # required by the POST /batches reject-when-no-worker check.
        # Individual tests that want to exercise the rejection path
        # delete this row before issuing their POST.
        s.execute(
            insert(Worker).values(
                id=uuid4(),
                hostname="fixture-worker",
                version="test",
                capabilities=[
                    {"backend": "docker"},
                    {"backend": "fake"},
                    {"backend": "modal"},
                ],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()
    try:
        yield app, raw, team_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(RateCard))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(AdminAuditEvent))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Task))
            s.execute(delete(TaskSet))
            s.execute(delete(Benchmark))
            s.execute(delete(Worker))
            s.execute(delete(TeamQuota))
            s.execute(delete(TeamMembership))
            s.execute(delete(User).where(User.username_normalized == username.casefold()))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _seed_connection(postgres_url: str, team_id: UUID, *model_ids: str) -> str:
    """Model-backed submissions require an explicit Provider Connection (#2054)."""
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as s:
        connection_id = insert_openai_connection(s, team_id=team_id, model_ids=model_ids)
        s.commit()
    engine.dispose()
    return str(connection_id)


async def test_post_batch_materializes_count(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "MIT slate",
                "description": "all MIT-licensed tasks",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )
        detail = await ac.get(
            f"/api/v1/batches/{r.json()['batch_id']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 3
    assert body["state"] == "submitted"
    assert body["purpose"] == "evaluation"
    UUID(body["batch_id"])  # parseable
    assert detail.status_code == 200, detail.text
    detail_body = detail.json()
    assert detail_body["purpose"] == "evaluation"
    assert detail_body["owner_team"] == {
        "id": str(team_id),
        "name": f"t-{team_id}",
    }
    assert detail_body["submitted_by_user"]["username"].startswith("BatchOwner-")
    assert detail_body["submitted_by_user"]["team_id"] == str(team_id)
    assert detail_body["network_policy"] == {
        "authored_defaults": [{
            "policy": {"kind": "public"},
            "task_ids": ["local/mit-0", "local/mit-1", "local/mit-2"],
        }],
        "requested_override": None,
        "resolved_effective": [{
            "policy": {"kind": "public"},
            "task_ids": ["local/mit-0", "local/mit-1", "local/mit-2"],
        }],
        "unavailable_task_ids": [],
    }
    async with app.state.session_factory() as session:
        batch, authority = (
            await session.execute(
                select(Batch, DataLifecycleAuthority)
                .join(
                    DataLifecycleAuthority,
                    DataLifecycleAuthority.id == Batch.lifecycle_authority_id,
                )
                .where(Batch.id == UUID(body["batch_id"]))
            )
        ).one()
    assert authority.environment == "development"
    assert authority.namespace == "loom"
    assert authority.team_id == team_id
    assert authority.data_class == "run"
    assert authority.owner_kind == "batch"
    assert authority.owner_id == str(batch.id)
    assert authority.created_at == batch.created_at
    assert authority.pinned is True
    assert authority.expires_at is None


async def test_post_batch_accepts_owned_task_set_filter(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    task_set_id = f"ts/{team_id}/batch-taskset"
    task_id = f"{task_set_id}/tasks/row-1"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_id,
                slug="batch-taskset",
                display_name="Batch TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=f"s3://bucket/tasksets/user/{team_id}/batch-taskset/manifest.yaml",
            ),
        )
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_id}/batch-taskset/tasks/row-1/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "TaskSet slate",
                "purpose": "trajectory_generation",
                "task_filter": {"task_set_id": task_set_id},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 1


async def test_post_batch_evaluation_rejects_task_set_and_skip_verifier(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    task_set_id = f"ts/{team_id}/purpose-reject"
    task_id = f"{task_set_id}/tasks/row-1"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_id,
                slug="purpose-reject",
                display_name="Purpose Reject",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=f"s3://bucket/tasksets/user/{team_id}/purpose-reject/manifest.yaml",
            ),
        )
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_id}/purpose-reject/tasks/row-1/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        reject_task_set = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "eval-with-taskset",
                "purpose": "evaluation",
                "task_filter": {"task_set_id": task_set_id},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )
        reject_skip = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "eval-skip",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent": {"name": "oracle"},
                    "skip_verifier": True,
                },
            },
        )

    assert reject_task_set.status_code == 400, reject_task_set.text
    assert "evaluation_disallows_task_set" in reject_task_set.text
    assert reject_skip.status_code == 400, reject_skip.text
    assert "evaluation_disallows_skip_verifier" in reject_skip.text


async def test_post_batch_unions_benchmark_and_task_set_filters(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    task_set_id = f"ts/{team_id}/mixed-taskset"
    taskset_task_id = f"{task_set_id}/tasks/row-1"
    benchmark_task_id = "humaneval/HumanEval/42"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Benchmark).values(
                id="humaneval",
                display_name="HumanEval",
                upstream_kind="huggingface",
                upstream_locator="openai_humaneval",
                upstream_revision="",
                license_spdx="MIT",
                license_url="https://example/license",
                splits=["test"],
            ),
        )
        conn.execute(
            insert(Task).values(
                id=benchmark_task_id,
                checksum="x" * 64,
                config=_valid_task_config(benchmark_task_id),
                source="local",
                license="MIT",
                benchmark_id="humaneval",
            ),
        )
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_id,
                slug="mixed-taskset",
                display_name="Mixed TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=f"s3://bucket/tasksets/user/{team_id}/mixed-taskset/manifest.yaml",
            ),
        )
        conn.execute(
            insert(Task).values(
                id=taskset_task_id,
                checksum="x" * 64,
                config=_valid_task_config(taskset_task_id),
                source=f"s3://bucket/tasksets/user/{team_id}/mixed-taskset/tasks/row-1/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "Mixed source slate",
                "purpose": "trajectory_generation",
                "task_filter": {
                    "benchmark_ids": ["humaneval"],
                    "task_set_ids": [task_set_id],
                },
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 201, r.text
    assert r.json()["expected_trial_count"] == 2


async def test_post_batch_rejects_cross_team_task_set_filter(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw_a, team_a = camp_setup
    team_b = uuid4()
    user_b = uuid4()
    raw_b = f"loom_team_{uuid4().hex}"
    task_set_id = f"ts/{team_a}/private-taskset"
    task_id = f"{task_set_id}/tasks/row-1"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Team).values(id=team_b, name=f"t-{team_b}"))
        conn.execute(
            insert(User).values(
                id=user_b,
                username=f"BatchOther-{team_b.hex[:8]}",
                username_normalized=f"batchother-{team_b.hex[:8]}",
                status="active",
                is_platform_admin=False,
            ),
        )
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_b.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_b,
                created_by_user_id=user_b,
                issued_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(TeamMembership).values(
                team_id=team_b,
                user_id=user_b,
                role="owner",
            ),
        )
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_a,
                slug="private-taskset",
                display_name="Private TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=f"s3://bucket/tasksets/user/{team_a}/private-taskset/manifest.yaml",
            ),
        )
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_a}/private-taskset/tasks/row-1/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={
                "name": "Cross-team TaskSet",
                "purpose": "trajectory_generation",
                "task_filter": {"task_set_id": task_set_id},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 404, r.text


async def test_post_batch_rejects_cross_team_explicit_task_set_task_id(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw_a, team_a = camp_setup
    team_b = uuid4()
    user_b = uuid4()
    raw_b = f"loom_team_{uuid4().hex}"
    task_set_id = f"ts/{team_a}/explicit-private-taskset"
    task_id = f"{task_set_id}/tasks/row-1"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Team).values(id=team_b, name=f"t-{team_b}"))
        conn.execute(
            insert(User).values(
                id=user_b,
                username=f"BatchExplicitOther-{team_b.hex[:8]}",
                username_normalized=f"batchexplicitother-{team_b.hex[:8]}",
                status="active",
                is_platform_admin=False,
            ),
        )
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_b.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_b,
                created_by_user_id=user_b,
                issued_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(TeamMembership).values(
                team_id=team_b,
                user_id=user_b,
                role="owner",
            ),
        )
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_a,
                slug="explicit-private-taskset",
                display_name="Explicit Private TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{team_a}/explicit-private-taskset/manifest.yaml"
                ),
            ),
        )
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_a}/explicit-private-taskset/tasks/row-1/",
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={
                "name": "Cross-team explicit TaskSet task",
                "purpose": "trajectory_generation",
                "task_filter": {"task_ids": [task_id], "subset_kind": "explicit"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 404, r.text
    assert r.json()["detail"] == "task not found"


async def test_post_batch_rejects_mixed_visible_and_cross_team_explicit_task_ids_atomically(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw_a, team_a = camp_setup
    team_b = uuid4()
    user_b = uuid4()
    raw_b = f"loom_team_{uuid4().hex}"
    task_set_id = f"ts/{team_a}/mixed-explicit-private-taskset"
    private_task_id = f"{task_set_id}/tasks/row-1"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Team).values(id=team_b, name=f"t-{team_b}"))
        conn.execute(
            insert(User).values(
                id=user_b,
                username=f"BatchMixedExplicitOther-{team_b.hex[:8]}",
                username_normalized=f"batchmixedexplicitother-{team_b.hex[:8]}",
                status="active",
                is_platform_admin=False,
            ),
        )
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_b.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_b,
                created_by_user_id=user_b,
                issued_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(TeamMembership).values(
                team_id=team_b,
                user_id=user_b,
                role="owner",
            ),
        )
        conn.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_a,
                slug="mixed-explicit-private-taskset",
                display_name="Mixed Explicit Private TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{team_a}/mixed-explicit-private-taskset/"
                    "manifest.yaml"
                ),
            ),
        )
        conn.execute(
            insert(Task).values(
                id=private_task_id,
                checksum="x" * 64,
                config=_valid_task_config(private_task_id),
                source=(
                    f"s3://bucket/tasksets/user/{team_a}/mixed-explicit-private-taskset/"
                    "tasks/row-1/"
                ),
                task_set_id=task_set_id,
                benchmark_id=None,
            ),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={
                "name": "Mixed visible and private explicit tasks",
                "purpose": "trajectory_generation",
                "task_filter": {
                    "task_ids": ["local/mit-0", private_task_id],
                    "subset_kind": "explicit",
                },
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    with sync_engine.connect() as conn:
        batch_count = conn.execute(select(func.count(Batch.id))).scalar_one()
    sync_engine.dispose()

    assert r.status_code == 404, r.text
    assert r.json()["detail"] == "task not found"
    assert batch_count == 0


async def test_admin_submit_on_behalf_records_represented_user_owner_access_and_audit(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    represented_username = ""
    with sync_engine.begin() as conn:
        represented_username = conn.execute(
            select(User.username).where(User.username.like("BatchOwner-%")),
        ).scalar_one()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        created = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "release-operator",
                "User-Agent": "loom-test-admin-on-behalf",
            },
            json={
                "represented_username": represented_username,
                "team_id": str(team_id),
                "name": "admin canary",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )
        detail = await ac.get(
            f"/api/v1/batches/{created.json().get('batch_id')}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        represented_list = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
        represented_detail = await ac.get(
            f"/api/v1/batches/{created.json().get('batch_id')}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        represented_cancel = await ac.post(
            f"/api/v1/batches/{created.json().get('batch_id')}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
        audit = await ac.get(
            "/api/v1/admin/audit-events?limit=20",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["team_id"] == str(team_id)
    assert body["expected_trial_count"] == 3

    assert detail.status_code == 200, detail.text
    submitted = detail.json()["submitted_by_user"]
    assert submitted["username"] == represented_username
    assert submitted["team_id"] == str(team_id)

    assert represented_list.status_code == 200, represented_list.text
    listed = represented_list.json()["items"]
    assert body["batch_id"] in {item["id"] for item in listed}
    assert represented_detail.status_code == 200, represented_detail.text
    assert represented_detail.json()["submitted_by_user"]["username"] == represented_username
    assert represented_detail.json()["owner_team"]["id"] == str(team_id)
    assert represented_cancel.status_code == 200, represented_cancel.text
    assert represented_cancel.json() == {
        "batch_id": body["batch_id"],
        "state": "cancelled",
    }

    with sync_engine.begin() as conn:
        batch_row = conn.execute(
            select(
                Batch.submitted_by_user_id,
                Batch.usage_attributed_user_id,
                Batch.usage_attributed_actor,
            ).where(
                Batch.id == UUID(body["batch_id"]),
            ),
        ).one()
        represented_user_id = conn.execute(
            select(User.id).where(User.username == represented_username),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.submitted_by_user_id == represented_user_id
    assert batch_row.usage_attributed_user_id is None
    assert batch_row.usage_attributed_actor == "release-operator"

    assert audit.status_code == 200, audit.text
    event = next(
        item for item in audit.json()["items"] if item["action"] == "batch.submit_on_behalf"
    )
    assert event["actor"] == "release-operator"
    assert event["target_type"] == "batch"
    assert event["target_id"] == body["batch_id"]
    assert event["metadata"] == {
        "represented_user_id": str(represented_user_id),
        "represented_username": represented_username,
        "represented_team_id": str(team_id),
        "expected_trial_count": 3,
        "backend": "docker",
    }


async def test_admin_submit_on_behalf_requires_admin_actor(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "represented_username": "BatchOwner-missing",
                "team_id": str(team_id),
                "name": "missing actor",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 400
    assert "X-Loom-Admin-Actor" in r.json()["detail"]


async def test_admin_submit_on_behalf_rejects_inactive_represented_user(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    inactive_user_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(User).values(
                id=inactive_user_id,
                username="InactiveBatchOwner",
                username_normalized="inactivebatchowner",
                status="pending_setup",
                is_platform_admin=False,
            )
        )
        conn.execute(
            insert(TeamMembership).values(
                team_id=team_id,
                user_id=inactive_user_id,
                role="member",
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "release-operator",
            },
            json={
                "represented_username": "InactiveBatchOwner",
                "team_id": str(team_id),
                "name": "inactive user",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 403
    assert "represented user is not active" in r.json()["detail"]


async def test_admin_submit_on_behalf_rejects_non_member_user(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    other_user_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(User).values(
                id=other_user_id,
                username="ActiveNonMember",
                username_normalized="activenonmember",
                status="active",
                is_platform_admin=False,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "release-operator",
            },
            json={
                "represented_username": "ActiveNonMember",
                "team_id": str(team_id),
                "name": "non member",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 403
    assert "not a member" in r.json()["detail"]


async def test_legacy_team_token_cannot_submit_on_behalf(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    legacy_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        represented_username = conn.execute(
            select(User.username).where(User.username.like("BatchOwner-%")),
        ).scalar_one()
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={
                "Authorization": f"Bearer {legacy_raw}",
                "X-Loom-Admin-Actor": "release-operator",
            },
            json={
                "represented_username": represented_username,
                "team_id": str(team_id),
                "name": "legacy token on-behalf",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 403
    assert "admin scope required" in r.json()["detail"]


def _insert_budget_provider(
    postgres_url: str,
    team_id: UUID,
    *,
    pricing_source: str = "operator-supplied",
    pricing_data: dict[str, float] | None = None,
    rate_card_provider: str | None = None,
) -> UUID:
    provider_connection_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(ProviderConnection).values(
                id=provider_connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=f"budget-provider-{provider_connection_id.hex[:8]}",
                base_url="https://budget-provider.example/v1",
                upstream_host="budget-provider.example",
                encrypted_api_key_ref="env:BUDGET_PROVIDER_KEY",
                pricing_source=pricing_source,
                pricing_data=pricing_data
                if pricing_data is not None
                else (
                    {
                        "input_usd_per_1m": 1.0,
                        "output_usd_per_1m": 0.0,
                    }
                    if pricing_source == "operator-supplied"
                    else None
                ),
                rate_card_provider=rate_card_provider,
                created_by="test:budget",
                status="valid",
            )
        )
        conn.execute(
            insert(ProviderModelCache).values(
                provider_connection_id=provider_connection_id,
                model_id="glm-5.1-thinking",
                upstream_present=True,
                visible=True,
            )
        )
    sync_engine.dispose()
    return provider_connection_id


async def test_post_batch_soft_budget_requires_confirmation_when_estimate_exceeds(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    provider_connection_id = _insert_budget_provider(postgres_url, team_id)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "soft-budget-needs-confirm",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "provider_connection_id": str(provider_connection_id),
                "provider_model_id": "glm-5.1-thinking",
                "budget_usd": 1.0,
                "budget_policy": "soft",
            },
        )

    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["reason"] == "batch_budget_confirmation_required"
    assert detail["budget"]["budget_policy"] == "soft"
    assert detail["budget"]["budget_usd"] == pytest.approx(1.0)
    assert detail["budget"]["pre_run_estimated_cost_usd"] == pytest.approx(3.0)
    assert detail["budget"]["cost_estimate_source"] == "operator-supplied"
    assert detail["budget"]["cost_estimate_confidence"] == "configured"


async def test_post_batch_hard_budget_rejects_when_estimate_exceeds(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    provider_connection_id = _insert_budget_provider(postgres_url, team_id)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "hard-budget-reject",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "provider_connection_id": str(provider_connection_id),
                "provider_model_id": "glm-5.1-thinking",
                "budget_usd": 1.0,
                "budget_policy": "hard",
            },
        )
        listed = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert detail["reason"] == "batch_budget_exceeded"
    assert detail["budget"]["pre_run_estimated_cost_usd"] == pytest.approx(3.0)
    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []


async def test_post_batch_confirmed_soft_budget_persists_budget_projection(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    provider_connection_id = _insert_budget_provider(postgres_url, team_id)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "confirmed-soft-budget",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "provider_connection_id": str(provider_connection_id),
                "provider_model_id": "glm-5.1-thinking",
                "budget_usd": 1.0,
                "budget_policy": "soft",
                "budget_confirmed": True,
            },
        )
        detail = await ac.get(
            f"/api/v1/batches/{r.json().get('batch_id')}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["budget_usd"] == pytest.approx(1.0)
    assert body["budget_policy"] == "soft"
    assert body["pre_run_estimated_cost_usd"] == pytest.approx(3.0)
    assert body["budget_remaining_usd"] == pytest.approx(1.0)
    assert body["budget_status"] == "soft_over_pre_run_estimate"

    assert detail.status_code == 200, detail.text
    detail_body = detail.json()
    assert detail_body["budget_usd"] == pytest.approx(1.0)
    assert detail_body["budget_policy"] == "soft"
    assert detail_body["pre_run_estimated_cost_usd"] == pytest.approx(3.0)
    assert detail_body["budget_remaining_usd"] == pytest.approx(1.0)


async def test_post_batch_combination_provider_budget_summarizes_per_combo(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    provider_a = _insert_budget_provider(
        postgres_url,
        team_id,
        pricing_data={"input_usd_per_1m": 1.0, "output_usd_per_1m": 0.0},
    )
    provider_b = _insert_budget_provider(
        postgres_url,
        team_id,
        pricing_data={"input_usd_per_1m": 2.0, "output_usd_per_1m": 0.0},
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "combination-budget",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "combinations": [
                    {
                        "agent_name": "litellm",
                        "agent_model": {
                            "provider": "openai",
                            "name": "glm-5.1-thinking",
                        },
                        "provider_connection_id": str(provider_a),
                        "provider_model_id": "glm-5.1-thinking",
                        "n_per_task": 1,
                        "label": "provider-a",
                    },
                    {
                        "agent_name": "litellm",
                        "agent_model": {
                            "provider": "openai",
                            "name": "glm-5.1-thinking",
                        },
                        "provider_connection_id": str(provider_b),
                        "provider_model_id": "glm-5.1-thinking",
                        "n_per_task": 2,
                        "label": "provider-b",
                    },
                ],
                "budget_usd": 20.0,
                "budget_policy": "soft",
                "budget_confirmed": True,
            },
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 9
    assert body["pre_run_estimated_cost_usd"] == pytest.approx(15.0)
    diagnostics = body["budget_diagnostics"]
    assert diagnostics[0]["reason"] == "combination_budget_estimate"
    items = diagnostics[0]["items"]
    assert [
        (
            item["combination_idx"],
            item["label"],
            item["provider_connection_id"],
            item["provider_model_id"],
            item["expected_trial_count"],
            item["pre_run_estimated_cost_usd"],
        )
        for item in items
    ] == [
        (0, "provider-a", str(provider_a), "glm-5.1-thinking", 3, 3.0),
        (1, "provider-b", str(provider_b), "glm-5.1-thinking", 6, 12.0),
    ]


async def test_legacy_team_token_cannot_create_batch(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    legacy_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {legacy_raw}"},
            json={
                "name": "legacy token batch",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 403
    assert "legacy team token" in r.json()["detail"]


async def test_post_batch_sanitizes_trial_request_params(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "request params",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "stub"),
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "local", "name": "stub"},
                    "request_params": {
                        "temperature": 0,
                        "top_p": 0.5,
                        "seed": 1234,
                        "max_tokens": 7,
                        "max_output_tokens": 8,
                        "messages": [{"role": "user", "content": "secret"}],
                        "api_key": "sk-hidden",
                        "extra_body": {"top_k": 40, "prompt": "secret"},
                    },
                },
            },
        )
        assert r.status_code == 201, r.text
        batch_id = UUID(r.json()["batch_id"])
        detail = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert detail.status_code == 200, detail.text
    expected = {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "max_tokens": 7,
        "max_output_tokens": 8,
        "extra_body": {"top_k": 40},
    }
    assert detail.json()["trial_config"]["request_params"] == expected
    rendered = json.dumps(detail.json()["trial_config"])
    assert "api_key" not in rendered
    assert "messages" not in rendered
    assert "secret" not in rendered

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.connect() as conn:
            stored = conn.execute(
                select(Batch.trial_config).where(Batch.id == batch_id)
            ).scalar_one()
    finally:
        sync_engine.dispose()
    assert stored["request_params"] == expected


async def test_post_batch_generates_concise_identity_when_name_omitted(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Benchmark),
            [
                {
                    "id": "humaneval",
                    "display_name": "HumanEval",
                    "upstream_kind": "fixture",
                    "upstream_locator": "fixture://humaneval",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/license",
                    "splits": ["test"],
                },
                {
                    "id": "mbpp",
                    "display_name": "MBPP",
                    "upstream_kind": "fixture",
                    "upstream_locator": "fixture://mbpp",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/license",
                    "splits": ["test"],
                },
            ],
        )
        for benchmark_id in ("humaneval", "mbpp"):
            for i in range(2):
                tid = f"{benchmark_id}/task-{i}"
                conn.execute(
                    insert(Task).values(
                        id=tid,
                        checksum=f"{benchmark_id}-{i}".encode().hex().ljust(64, "0")[:64],
                        config=_valid_task_config(tid),
                        source="local",
                        benchmark_id=benchmark_id,
                    )
                )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name_suffix": "canary",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
                "task_filter": {
                    "benchmark_ids": ["humaneval", "mbpp"],
                    "subset_kind": "random_n",
                    "n": 5,
                    "seed": 17,
                },
                "trial_config": {},
                "combinations": [
                    {
                        "agent_name": "litellm",
                        "agent_model": {
                            "provider": "openai",
                            "name": "gpt-4o-mini",
                        },
                        "n_per_task": 2,
                    }
                ],
            },
        )
        assert r.status_code == 201, r.text
        batch_id = UUID(r.json()["batch_id"])
        detail = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["name"] == "humaneval+mbpp random5 | litellm/gpt-4o-mini x2 - canary"
    assert len(body["name"]) <= 90
    assert body["description"] == (
        "Tasks: humaneval, mbpp; subset: random 5 seed 17. "
        "Combinations: litellm/openai/gpt-4o-mini x2. Backend: docker."
    )


async def test_post_batch_keeps_explicit_identity_over_generated_values(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "Manual display name",
                "name_suffix": "ignored",
                "description": "Manual description",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent_name": "oracle", "agent_model": None},
            },
        )
        assert r.status_code == 201, r.text
        detail = await ac.get(
            f"/api/v1/batches/{r.json()['batch_id']}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["name"] == "Manual display name"
    assert body["description"] == "Manual description"


async def test_post_batch_accepts_noncommercial_license_tasks(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Service-mode submit must not block tasks by source license."""
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(TeamQuota).values(
                team_id=team_id,
                license_allowlist=["MIT"],
            )
        )
        s.execute(
            insert(Task).values(
                id="local/noncommercial",
                checksum="z" * 64,
                config=_valid_task_config("local/noncommercial"),
                source="local",
                license="CC-BY-NC-4.0",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "NC slate",
                "purpose": "evaluation",
                "task_filter": {"license": "CC-BY-NC-4.0"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 1

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        created = s.execute(
            select(Batch).where(Batch.name == "NC slate"),
        ).scalar_one_or_none()
    sync_engine.dispose()
    assert created is not None


async def test_post_batch_with_n_per_task_multiplies_count(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Plan 23: expected_trial_count = len(matched_tasks) * n_per_task."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "MIT-x3",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "n_per_task": 3,
            },
        )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 9
    assert body["n_per_task"] == 3


async def test_post_batch_rejects_required_worker_pools_on_user_path(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """#1109: user eval batches must not admit pool-coverage trials."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "MIT with deterministic pool coverage",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "required_worker_pools": [" local-worker ", "k8s-worker", "local-worker"],
            },
        )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "required_worker_pools is operator-only" in detail
    assert "#1109" in detail


async def test_admin_on_behalf_required_worker_pools_adds_coverage_count(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """#188 / #1109: coverage +1 stays on admin on-behalf canaries only."""
    app, _raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        represented_username = conn.execute(
            select(User.username).where(User.username.like("BatchOwner-%")),
        ).scalar_one()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/admin/batches/on-behalf",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "release-operator",
            },
            json={
                "represented_username": represented_username,
                "team_id": str(team_id),
                "name": "MIT with deterministic pool coverage",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "required_worker_pools": [" local-worker ", "k8s-worker", "local-worker"],
            },
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["required_worker_pools"] == ["local-worker", "k8s-worker"]
    assert body["expected_trial_count"] == 5

    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
    sync_engine.dispose()
    assert row.required_worker_pools == ["local-worker", "k8s-worker"]


async def test_admin_on_behalf_rejects_k8s_worker_pool_when_disabled(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """#383: On profiles with k8s_worker.enabled=false the k8s worker
    Deployment is not rendered, so a submission that requires the
    k8s-worker pool would queue a coverage trial no worker can claim.
    The API must fail loudly at submit time instead of silently
    accepting it."""
    app, _raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        represented_username = conn.execute(
            select(User.username).where(User.username.like("BatchOwner-%")),
        ).scalar_one()
    sync_engine.dispose()
    # Simulate rendering with k8s_worker.enabled=false by flipping the
    # loom-service Settings that the renderer would have injected.
    original = app.state.settings.k8s_worker_enabled
    app.state.settings.k8s_worker_enabled = False
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
        ) as ac:
            r = await ac.post(
                "/api/v1/admin/batches/on-behalf",
                headers={
                    "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                    "X-Loom-Admin-Actor": "release-operator",
                },
                json={
                    "represented_username": represented_username,
                    "team_id": str(team_id),
                    "name": "coverage on disabled cluster",
                    "purpose": "evaluation",
                    "task_filter": {"license": "MIT"},
                    "trial_config": {},
                    "required_worker_pools": ["local-worker", "k8s-worker"],
                },
            )
    finally:
        app.state.settings.k8s_worker_enabled = original

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "k8s-worker" in detail
    assert "k8s_worker.enabled=false" in detail
    assert "available x86_64 execution pool" in detail


async def test_admin_on_behalf_without_k8s_worker_pool_still_works_when_disabled(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """The rejection is targeted: a submission that only requires
    `local-worker` on a k8s-worker-disabled cluster is unaffected."""
    app, _raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        represented_username = conn.execute(
            select(User.username).where(User.username.like("BatchOwner-%")),
        ).scalar_one()
    sync_engine.dispose()
    original = app.state.settings.k8s_worker_enabled
    app.state.settings.k8s_worker_enabled = False
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
        ) as ac:
            r = await ac.post(
                "/api/v1/admin/batches/on-behalf",
                headers={
                    "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                    "X-Loom-Admin-Actor": "release-operator",
                },
                json={
                    "represented_username": represented_username,
                    "team_id": str(team_id),
                    "name": "local-worker-only coverage on disabled cluster",
                    "purpose": "evaluation",
                    "task_filter": {"license": "MIT"},
                    "trial_config": {},
                    "required_worker_pools": ["local-worker"],
                },
            )
    finally:
        app.state.settings.k8s_worker_enabled = original
    assert r.status_code == 201, r.text


async def test_paused_team_rejects_batch_and_records_reason(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE teams "
                "SET submissions_paused_at = NOW(), "
                "submissions_paused_reason = 'incident hold' "
                "WHERE id = :team_id",
            ),
            {"team_id": team_id},
        )
    sync_engine.dispose()

    before = _counter_value(
        "loom_svc_submission_rejects",
        "loom_svc_submission_rejects_total",
        {"reason": "team_paused"},
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "paused-submit",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )

    assert r.status_code == 403, r.text
    assert "paused" in r.json()["detail"]
    after = _counter_value(
        "loom_svc_submission_rejects",
        "loom_svc_submission_rejects_total",
        {"reason": "team_paused"},
    )
    assert after == before + 1


async def test_post_batch_rejects_n_per_task_out_of_range(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "bad-n",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "n_per_task": 0,
            },
        )
    assert r.status_code == 422


async def test_post_batch_rejects_unknown_agent_name(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Plan 25: a batch whose trial_config.agent_name isn't in the
    catalog is rejected at the API boundary so the batch runner
    doesn't fan out trials that would all 422."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "phantom-agent",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "not-an-agent",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 400
    assert "agent" in r.json()["detail"].lower()


async def test_post_batch_rejects_agent_name_without_agent_model(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "missing-agent-model",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent_name": "oracle"},
            },
        )
        listed = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400
    assert "agent_model" in r.json()["detail"]
    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []


async def test_post_batch_rejects_agent_without_service_runtime(
    camp_setup: tuple[FastAPI, str, UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#289/#288: unsupported displayed adapters must fail before
    fan-out, not after every child trial hits `command not found`."""
    base_agents = agent_catalog.list_agents()
    opencode = agent_catalog.AgentEntry(
        name="opencode",
        needs_model=True,
        kind="adapter",
        description="opencode test adapter",
        supported_providers=("*",),
        supported_model_sources=("api",),
        runtime_contract=agent_catalog.RuntimeContract(
            execution="subprocess-adapter",
            capture="stdout_jsonl",
            required_executables=("opencode",),
            required_packages=("opencode-ai",),
            endpoint_dialect="openai_chat",
            sandbox_network="gateway",
        ),
        service_mode_ready=False,
        readiness_status="unavailable",
        readiness_message="agent opencode requires runtime dependency",
    )
    monkeypatch.setattr(
        agent_catalog,
        "list_agents",
        lambda **_kwargs: [e for e in base_agents if e.name != "opencode"] + [opencode],
    )
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "opencode-not-provisioned",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "opencode",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                },
            },
        )
        listed = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "opencode" in detail
    assert "runtime" in detail.lower()
    assert "GET /api/v1/agents" in detail
    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []


async def test_post_rejects_unknown_filter_key(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Typo'd filter keys (`liscense`) get a 400 rather than silently
    matching zero tasks."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "broken",
                "purpose": "evaluation",
                "task_filter": {"liscense": "MIT"},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    assert "liscense" in r.json()["detail"]


async def test_post_rejects_empty_filter_match(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Audit M2: a filter that materializes to zero tasks would
    create a batch stuck in `submitted` forever
    (next_batch_state needs `expected > 0`). Reject up front."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "empty",
                "purpose": "evaluation",
                "task_filter": {"license": "no-such-license"},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    assert "zero tasks" in r.json()["detail"]


async def test_post_rejects_unsupported_ui_benchmark_filter(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark).values(
                id="osworld",
                display_name="OSWorld",
                upstream_kind="git",
                upstream_locator="https://github.com/xlang-ai/OSWorld.git",
                upstream_revision="main",
                license_spdx="Apache-2.0",
                license_url="https://example/osworld",
                splits=["test"],
            )
        )
        s.execute(
            insert(Task).values(
                id="osworld/task-001",
                checksum="o" * 64,
                config=_valid_task_config("osworld/task-001"),
                source="s3://bucket/osworld/task-001/",
                license="Apache-2.0",
                benchmark_id="osworld",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "unsupported-ui-benchmark",
                "purpose": "evaluation",
                "task_filter": {"benchmark_id": "osworld"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 400
    assert "zero tasks" in r.json()["detail"]


async def test_post_rejects_non_v1_builtin_benchmark_filter(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark).values(
                id="browsecomp",
                display_name="BrowseComp",
                upstream_kind="huggingface",
                upstream_locator="upstream/browsecomp",
                upstream_revision="",
                license_spdx="CC-BY-4.0",
                license_url="https://example/browsecomp",
                splits=["test"],
            )
        )
        s.execute(
            insert(Task).values(
                id="browsecomp/task-001",
                checksum="b" * 64,
                config=_valid_task_config("browsecomp/task-001"),
                source="s3://bucket/browsecomp/task-001/",
                license="CC-BY-4.0",
                benchmark_id="browsecomp",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "non-v1-benchmark",
                "purpose": "evaluation",
                "task_filter": {"benchmark_id": "browsecomp"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 400
    assert "zero tasks" in r.json()["detail"]


async def test_post_rejects_invalid_task_config(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Task).values(
                id="local/broken-config",
                checksum="b" * 64,
                config={},
                source="local",
                license="BrokenFixture",
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "broken-task",
                "purpose": "evaluation",
                "task_filter": {"task_ids": ["local/broken-config"]},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "invalid task config" in detail
    assert "local/broken-config" in detail


async def test_post_rejects_when_no_worker_advertises_backend(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """cluster-deploy.md §POST /batches: reject when no live worker
    advertises the requested backend. Saves the operator from a batch
    that would stall in 'submitted' forever (no claim ever comes)."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Tear down the fixture worker so no backend is live.
    with sl() as s:
        s.execute(delete(Worker))
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "lonely",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "no active worker advertises backend 'docker'" in detail
    assert "no active workers" in detail


async def test_post_docker_does_not_implicitly_select_nebius(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """A Nebius-capable task still needs Docker capacity when Docker is selected."""
    app, raw, _team_id = camp_setup
    task_id = "local/service-execution"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Worker))
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="5" * 64,
                config=_service_execution_task_config(task_id),
                source="local",
                license="MIT",
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        response = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "service-only",
                "purpose": "evaluation",
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": [task_id],
                },
                "trial_config": {},
                "backend": "docker",
            },
        )

    sync_engine.dispose()
    assert response.status_code == 400, response.text
    assert "no active worker advertises backend 'docker'" in response.json()["detail"]


@pytest.mark.usefixtures("hosted_environment")
@pytest.mark.parametrize("backend", ["docker", "modal", "fake", "Nebius"])
async def test_post_rejects_unsupported_backend_when_hosted(
    camp_setup: tuple[FastAPI, str, UUID],
    backend: str,
) -> None:
    """Hosted execution is Nebius-only. Any other explicit value is rejected
    before task resolution with an actionable error, even when a live worker
    advertises it; it is never reinterpreted as Nebius."""
    app, raw, _team_id = camp_setup

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "retired-backend",
                "purpose": "evaluation",
                # Would match zero tasks; the backend check must fire first.
                "task_filter": {"license": "no-such-license"},
                "trial_config": {},
                "backend": backend,
            },
        )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert detail["reason"] == "unsupported_hosted_backend"
    assert detail["backend"] == backend
    assert "Nebius only" in detail["message"]
    assert "Omit `backend`" in detail["message"]


@pytest.mark.usefixtures("hosted_environment")
async def test_rerun_failed_rejects_batch_on_retired_backend_when_hosted(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Historical batches stay readable, but a hosted rerun cannot dispatch to
    the backend they were originally submitted with."""
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="historical-docker",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                n_per_task=1,
                backend="docker",
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 503.",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
            )
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        rerun = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )
        detail = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    sync_engine.dispose()
    assert rerun.status_code == 400, rerun.text
    body = rerun.json()["detail"]
    assert body["reason"] == "unsupported_hosted_backend"
    assert "ran on backend 'docker'" in body["message"]
    assert "Submit a new batch" in body["message"]
    assert detail.status_code == 200, detail.text
    assert detail.json()["backend"] == "docker"


@pytest.mark.usefixtures("hosted_environment")
@pytest.mark.parametrize("send_backend", [True, False], ids=["explicit-nebius", "omitted"])
async def test_post_accepts_explicit_nebius_backend_without_legacy_worker(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    send_backend: bool,
) -> None:
    """Nebius is admitted by a fresh Nebius target, whether or not `backend`
    is sent: an explicit "nebius" is compatibility input and an omitted value
    resolves to Nebius server-side."""
    app, raw, team_id = camp_setup
    task_id = "local/explicit-nebius"
    target_id = "nebius-explicit-backend-test"
    now = datetime.now(UTC)
    execution_class_spec = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Worker))
        s.add(
            ServiceExecutionClass(
                id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                schema_version=NEBIUS_CPU_EXECUTION_CLASS_V1.schema_version,
                spec_json=execution_class_spec,
                spec_sha256=canonical_digest(execution_class_spec),
                enabled=True,
            )
        )
        s.add(
            ServiceExecutionTarget(
                id=target_id,
                logical_pool_id="nebius-cpu",
                execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                schema_version="loom.execution-target.v1",
                spec_json={"health_stale_after_seconds": 60},
                spec_sha256="sha256:" + "d" * 64,
                environment="development",
                provider="nebius",
                region="eu-north1",
                failure_domain="eu-north1-a",
                data_residency="eu",
                desired_state="active",
                observed_state="ready",
                health_status="healthy",
                health_observed_at=now,
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="7" * 64,
                config=_service_execution_task_config(task_id),
                source="local",
                license="MIT",
            )
        )
        s.commit()

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            catalog = await ac.get(
                "/api/v1/backends",
                headers={"Authorization": f"Bearer {raw}"},
            )
            response = await ac.post(
                "/api/v1/batches",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "name": "explicit-nebius",
                    "purpose": "evaluation",
                    "task_filter": {
                        "subset_kind": "explicit",
                        "task_ids": [task_id],
                    },
                    "trial_config": {},
                    **({"backend": "nebius"} if send_backend else {}),
                },
            )

        assert catalog.status_code == 200, catalog.text
        nebius = next(item for item in catalog.json()["items"] if item["name"] == "nebius")
        assert nebius == {
            "name": "nebius",
            "description": "Nebius Kubernetes execution; scales from zero.",
            "available": False,
            "cold_start_available": True,
            "cold_start_pools": ["nebius-cpu"],
        }
        assert response.status_code == 201, response.text
        assert response.json()["expected_trial_count"] == 1

        batch_id = UUID(response.json()["batch_id"])
        with sl() as s:
            s.execute(
                Batch.__table__.update()
                .where(Batch.id == batch_id)
                .values(
                    state="finished",
                    result_status="all_failed",
                    finished_at=datetime.now(UTC),
                )
            )
            s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    task_id=task_id,
                    team_id=team_id,
                    state="failed",
                    failure_reason="gateway_error",
                    failure_message="gateway 503",
                    config={},
                    requires_caps={
                        "backend": "nebius",
                        "cpu_arch": "x86_64",
                        "worker_pool": "nebius-cpu",
                    },
                    submitted_at=datetime.now(UTC),
                    batch_id=batch_id,
                    sample_idx=0,
                    combination_idx=0,
                )
            )
            s.commit()
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            rerun = await ac.post(
                f"/api/v1/batches/{batch_id}/rerun-failed",
                headers={"Authorization": f"Bearer {raw}"},
            )
        assert rerun.status_code == 201, rerun.text
        assert rerun.json()["rerun_target_count"] == 1
        with sl() as s:
            rerun_backend = s.execute(
                select(Batch.backend).where(
                    Batch.id == UUID(rerun.json()["batch_id"]),
                )
            ).scalar_one()
        assert rerun_backend == "nebius"
    finally:
        with sl() as s:
            s.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id == target_id,
                )
            )
            s.execute(
                delete(ServiceExecutionClass).where(
                    ServiceExecutionClass.id == NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                )
            )
            s.commit()
        sync_engine.dispose()


@pytest.mark.usefixtures("hosted_environment")
@pytest.mark.parametrize("use_combinations", [False, True])
@pytest.mark.parametrize("agent_name", ["direct-completion", "terminus-2"])
@pytest.mark.parametrize("dockerfile", [False, True], ids=["prebuilt", "dockerfile"])
async def test_post_accepts_ordinary_task_from_deployment_runtime_profile(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    use_combinations: bool,
    agent_name: str,
    dockerfile: bool,
) -> None:
    app, raw, team_id = camp_setup
    suffix = "combinations" if use_combinations else "single"
    task_id = f"local/automatic-nebius-{suffix}"
    target_id = f"nebius-automatic-backend-{suffix}"
    now = datetime.now(UTC)
    profile = _service_execution_runtime_profile()
    if agent_name == "terminus-2":
        controller_image = "registry.example/worker@sha256:" + "9" * 64
        service_image = "registry.example/service@sha256:" + "8" * 64
        profile = profile.model_copy(update={
            "task_image_ref": service_image,
            "agent_image_ref": controller_image,
            "image_admission": signed_image_admission_bundle((
                profile.task_image_ref, profile.runtime_image_ref, controller_image, service_image,
            )),
        })
    app.state.settings = app.state.settings.model_copy(
        update={
            "service_execution_runtime_profile_json": json.dumps(
                profile.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            )
        }
    )
    task_config = _automatic_service_execution_task_config(task_id)
    if dockerfile:
        environment = task_config["environment"]
        assert isinstance(environment, dict)
        environment.pop("docker_image")
        environment.update({
            "dockerfile": "environment/Dockerfile", "cpu_arch": "any",
        })
    execution_class_spec = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Worker))
        s.add(
            ServiceExecutionClass(
                id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                schema_version=NEBIUS_CPU_EXECUTION_CLASS_V1.schema_version,
                spec_json=execution_class_spec,
                spec_sha256=canonical_digest(execution_class_spec),
                enabled=True,
            )
        )
        s.add(
            ServiceExecutionTarget(
                id=target_id,
                logical_pool_id="nebius-cpu",
                execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                schema_version="loom.execution-target.v1",
                spec_json={"health_stale_after_seconds": 60},
                spec_sha256="sha256:" + "e" * 64,
                environment="development",
                provider="nebius",
                region="eu-north1",
                failure_domain="eu-north1-a",
                data_residency="eu",
                desired_state="active",
                observed_state="ready",
                health_status="healthy",
                health_observed_at=now,
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="c" * 64,
                config=task_config,
                source="s3://artifacts/task-inputs/task/",
                source_provenance={
                    "service_execution_input": {
                        "schema_version": "loom.service-execution-input.v1",
                        "manifest_uri": "s3://artifacts/task-inputs/task.json",
                        "manifest_sha256": "sha256:" + "d" * 64,
                        "file_count": 3,
                        "total_bytes": 4096,
                    }
                },
                license="MIT",
            )
        )
        s.commit()

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            payload: dict[str, object] = {
                "name": f"automatic-nebius-{suffix}",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-5"),
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": [task_id],
                },
                "trial_config": {},
                "backend": "nebius",
                "budget_policy": "none",
            }
            model = {
                "provider": "openai",
                "name": "gpt-5",
                "source": "api",
            }
            if use_combinations:
                payload["combinations"] = [
                    {
                        "agent_name": agent_name,
                        "agent_model": model,
                        "n_per_task": 1,
                    }
                ]
            else:
                payload["trial_config"] = {
                    "agent_name": agent_name,
                    "agent_model": model,
                }
            response = await ac.post(
                "/api/v1/batches",
                headers={"Authorization": f"Bearer {raw}"},
                json=payload,
            )

        if dockerfile and agent_name != "terminus-2":
            assert response.status_code == 400, response.text
            assert response.json()["detail"]["reason"] == "nebius_task_incompatible"
            return
        assert response.status_code == 201, response.text
        assert response.json()["backend"] == "nebius"
        assert response.json()["expected_trial_count"] == 1
        with sl() as s:
            batch = s.get(Batch, UUID(response.json()["batch_id"]))
            assert batch is not None
            assert batch.service_execution_runtime_profile == profile.model_dump(mode="json")
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            detail = await ac.get(
                f"/api/v1/batches/{response.json()['batch_id']}",
                headers={"Authorization": f"Bearer {raw}"},
            )
        assert detail.status_code == 200, detail.text
        assert detail.json()["service_execution_runtime_profile"] == {
            "candidate_sha": profile.candidate_sha,
            "sha256": canonical_digest(profile.model_dump(mode="json")),
        }
    finally:
        with sl() as s:
            s.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id == target_id,
                )
            )
            s.execute(
                delete(ServiceExecutionClass).where(
                    ServiceExecutionClass.id == NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                )
            )
            s.commit()
        sync_engine.dispose()


async def test_post_nebius_rejects_task_without_nebius_binding(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Selecting Nebius never falls back to a legacy Docker task route."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "incompatible-nebius",
                "purpose": "evaluation",
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": ["local/mit-0"],
                },
                "trial_config": {},
                "backend": "nebius",
            },
        )

    assert response.status_code == 400, response.text
    assert response.json()["detail"] == {
        "reason": "nebius_task_incompatible",
        "backend": "nebius",
        "logical_pool_id": "nebius-cpu",
        "task_ids": ["local/mit-0"],
        "rejection_reasons": {
            "local/mit-0": [
                "automatic_trial_config_invalid",
                "runtime_profile_unavailable",
            ]
        },
    }


async def test_network_policy_preview_reports_every_incompatible_task(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/network-policy-preview",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "team_id": str(team_id),
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": ["local/mit-0", "local/mit-1"],
                },
                "baseline_network_policy_override": {"kind": "gateway-only"},
                "agent_names": ["terminus-2"],
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["task_count"] == 2
    assert body["selected_incompatible_task_ids"] == ["local/mit-0", "local/mit-1"]
    assert body["selected_rejection_reasons"] == {
        "local/mit-0": ["network_policy_override_not_supported"],
        "local/mit-1": ["network_policy_override_not_supported"],
    }


async def test_post_rejects_mixed_service_and_legacy_without_capacity(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """One service task must not hide a legacy task's capacity requirement."""
    app, raw, _team_id = camp_setup
    service_task_id = "local/mixed-service-execution"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Worker))
        s.execute(
            insert(Task).values(
                id=service_task_id,
                checksum="6" * 64,
                config=_service_execution_task_config(service_task_id),
                source="local",
                license="MIT",
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "mixed-service-legacy",
                "purpose": "evaluation",
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": [service_task_id, "local/mit-0"],
                },
                "trial_config": {},
                "backend": "docker",
            },
        )

    sync_engine.dispose()
    assert response.status_code == 400, response.text
    assert "no active worker" in response.json()["detail"]






async def test_post_rejects_when_no_worker_serves_specific_backend(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """A worker exists, but it doesn't advertise the requested backend.
    The 400 detail names what IS available so operators can switch."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Replace fixture worker with one that ONLY serves docker.
    with sl() as s:
        s.execute(delete(Worker))
        s.execute(
            insert(Worker).values(
                id=uuid4(),
                hostname="docker-only",
                version="test",
                capabilities=[{"backend": "docker"}],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "wants-modal",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "modal",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "'modal'" in detail
    assert "docker" in detail  # what IS available


async def test_post_rejects_when_only_worker_is_inactive(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """An inactive worker doesn't count — `status='active'` is the
    predicate. Catches a regression to checking presence-only."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        # Demote the fixture worker to inactive.
        s.execute(Worker.__table__.update().values(status="shutting-down"))
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "stale-only",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    assert "no active workers" in r.json()["detail"]


async def test_post_rejects_when_worker_heartbeat_is_stale(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Issue #68: a worker that crashed without SIGTERM keeps
    status='active' forever (no CP-side reaper flips it). The
    freshness predicate on `last_seen_at` ensures we don't keep
    handing batches to a dead worker. Heartbeat older than 30s
    ⇒ excluded from the catalog."""

    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Age the fixture worker's heartbeat past the 30s freshness
    # window. `status` stays 'active' — that's the bug we're guarding
    # against (no reaper updates status today).
    with sl() as s:
        s.execute(
            Worker.__table__.update().values(
                last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "stale-hb",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    assert "no active workers" in r.json()["detail"]


async def test_post_requires_submit_scope(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """A read:own-only token cannot create batches."""
    app, _raw, team_id = camp_setup
    no_submit_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(no_submit_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {no_submit_raw}"},
            json={
                "name": "X",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
    assert r.status_code == 403


async def test_list_batches(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "C1",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        c2 = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "C2",
                "purpose": "evaluation",
                "task_filter": {"license": "Apache-2.0"},
                "trial_config": {},
            },
        )
        assert c2.status_code == 201, c2.text
        c2_id = UUID(c2.json()["batch_id"])

        sync_engine = create_engine(postgres_url)
        with sync_engine.begin() as conn:
            trial_id = uuid4()
            conn.execute(
                insert(Trial).values(
                    id=trial_id,
                    task_id="local/apache-0",
                    team_id=team_id,
                    state="succeeded",
                    batch_id=c2_id,
                    sample_idx=0,
                    combination_idx=0,
                    config={},
                    requires_caps={},
                    result={"aggregate_reward": 1.0, "cost_usd": 99.0},
                )
            )
            conn.execute(
                insert(LlmCall).values(
                    id=uuid4(),
                    team_id=team_id,
                    trial_id=trial_id,
                    step_id="main",
                    model="openai/gpt-test",
                    dialect="openai",
                    input_tokens=4,
                    output_tokens=2,
                    provider_extras={},
                    cost_usd=Decimal("99.000000"),
                    rate_card_hash="stale-rate-card",
                )
            )
        sync_engine.dispose()

        r = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    # Newest first.
    assert items[0]["name"] == "C2"
    assert items[0]["team_id"] == str(team_id)
    assert items[0]["team_name"] == f"t-{team_id}"
    assert items[0]["owner_team"] == {
        "id": str(team_id),
        "name": f"t-{team_id}",
    }
    submitted = items[0]["submitted_by_user"]
    UUID(submitted["id"])
    assert submitted["username"] == f"BatchOwner-{team_id.hex[:8]}"
    assert submitted["team_id"] == str(team_id)
    assert submitted["team_name"] == f"t-{team_id}"
    assert "total_cost_usd" not in items[0]
    assert items[0]["total_prompt_tokens"] == 4
    assert items[0]["total_completion_tokens"] == 2
    assert items[0]["estimated_cost_usd"] == pytest.approx(99.0)
    assert items[0]["cost_currency"] == "USD"
    assert items[0]["cost_status"] == "estimated"
    assert items[0]["pricing_modes"] == ["priced"]
    assert items[0]["llm_calls_count"] == 1
    assert items[1]["total_prompt_tokens"] == 0
    assert items[1]["total_completion_tokens"] == 0
    assert items[1]["llm_calls_count"] == 0


async def test_list_batches_filters_by_benchmark_agent_and_model(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    wanted_id = uuid4()
    wrong_agent_id = uuid4()
    wrong_benchmark_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch),
            [
                {
                    "id": wanted_id,
                    "team_id": team_id,
                    "name": "wanted",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_ids": ["mbpp"]},
                    "trial_config": {
                        "agent_name": "litellm",
                        "agent_model": {"provider": "openai-compatible", "name": "qwen"},
                    },
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                },
                {
                    "id": wrong_agent_id,
                    "team_id": team_id,
                    "name": "wrong-agent",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_ids": ["mbpp"]},
                    "trial_config": {
                        "agent_name": "swe-agent",
                        "agent_model": {"provider": "openai-compatible", "name": "qwen"},
                    },
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                },
                {
                    "id": wrong_benchmark_id,
                    "team_id": team_id,
                    "name": "wrong-benchmark",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_id": "humaneval"},
                    "trial_config": {
                        "agent_name": "litellm",
                        "agent_model": {"provider": "openai-compatible", "name": "qwen"},
                    },
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/batches?benchmark_id=mbpp&agent=litellm&model=qwen",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert [item["id"] for item in items] == [str(wanted_id)]


async def test_list_batches_filters_by_query_provider_and_model_fields(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    wanted_id = uuid4()
    wrong_provider_id = uuid4()
    wrong_text_id = uuid4()
    provider_connection_id = uuid4()
    other_provider_connection_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(ProviderConnection),
            [
                {
                    "id": provider_connection_id,
                    "team_id": team_id,
                    "provider_type": "openai-compatible",
                    "display_name": "qwen-provider",
                    "base_url": "https://api.example.test/v1",
                    "upstream_host": "api.example.test",
                    "encrypted_api_key_ref": "env:QWEN_PROVIDER_KEY",
                    "created_by": "test",
                    "status": "valid",
                },
                {
                    "id": other_provider_connection_id,
                    "team_id": team_id,
                    "provider_type": "anthropic",
                    "display_name": "claude-provider",
                    "base_url": "https://api.anthropic.test/v1",
                    "upstream_host": "api.anthropic.test",
                    "encrypted_api_key_ref": "env:CLAUDE_PROVIDER_KEY",
                    "created_by": "test",
                    "status": "valid",
                },
            ],
        )
        conn.execute(
            insert(Batch),
            [
                {
                    "id": wanted_id,
                    "team_id": team_id,
                    "name": "skilllearnbench codex qwen sweep",
                    "description": "Needle text for generated identity search",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_ids": ["skilllearnbench"]},
                    "trial_config": {},
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                    "combinations": [
                        {
                            "agent_name": "codex",
                            "agent_model": {
                                "provider": "openai",
                                "name": "qwen3.6-35b-a3b",
                            },
                            "n_per_task": 1,
                            "provider_connection_id": str(provider_connection_id),
                            "provider_model_id": "qwen3.6-35b-a3b",
                        }
                    ],
                    "provider_connection_id": None,
                    "provider_model_id": None,
                },
                {
                    "id": wrong_provider_id,
                    "team_id": team_id,
                    "name": "skilllearnbench codex qwen other provider",
                    "description": "Needle text for generated identity search",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_ids": ["skilllearnbench"]},
                    "trial_config": {},
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                    "combinations": [
                        {
                            "agent_name": "codex",
                            "agent_model": {
                                "provider": "anthropic",
                                "name": "claude-sonnet-4-6",
                            },
                            "n_per_task": 1,
                        }
                    ],
                    "provider_connection_id": other_provider_connection_id,
                    "provider_model_id": "claude-sonnet-4-6",
                },
                {
                    "id": wrong_text_id,
                    "team_id": team_id,
                    "name": "unrelated batch",
                    "description": "No matching terms here",
                    "purpose": "evaluation",
                    "task_filter": {"benchmark_ids": ["skilllearnbench"]},
                    "trial_config": {
                        "agent_name": "codex",
                        "agent_model": {
                            "provider": "openai",
                            "name": "qwen3.6-35b-a3b",
                        },
                    },
                    "combinations": [],
                    "state": "submitted",
                    "created_by_token_prefix": "abcdef12",
                    "expected_trial_count": 1,
                    "provider_connection_id": provider_connection_id,
                    "provider_model_id": "qwen3.6-35b-a3b",
                },
            ],
        )
    sync_engine.dispose()

    params = (
        "q=needle&benchmark_id=skilllearnbench&agent_name=codex"
        "&model_provider=openai&model_name=qwen3.6-35b-a3b"
        f"&provider_connection_id={provider_connection_id}"
        "&provider_model_id=qwen3.6-35b-a3b"
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches?{params}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert [item["id"] for item in items] == [str(wanted_id)]


async def test_get_batch_detail_with_rollup(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Detail surfaces per-state counts + reward/cost rollups extracted
    from Trial.result JSONB."""
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "rollup-test",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        cid = UUID(post.json()["batch_id"])

    # Seed 3 trial rows under this batch: 2 succeeded with rewards,
    # 1 still running. LLM usage should come from llm_calls, not the
    # stale/frozen cost_usd values in Trial.result.
    captured_at = datetime(2026, 6, 27, tzinfo=UTC)
    table = RateCardTable(
        id="yibuapi-pricing-v1",
        captured_at=captured_at,
        provider="yibuapi",
        source_url="https://yibuapi.com/api/pricing",
        pricing_version="pricing-v1",
        last_checked_at=captured_at,
        currency="USD",
        group="default",
        group_ratio=1.0,
        entry_count=1,
        skipped_model_count=0,
        entries=[
            {
                "provider": "yibuapi",
                "model": "qwen3.6-35b-a3b",
                "input_per_mtok": 0.25,
                "output_per_mtok": 0.75,
                "cache_read_per_mtok": 0.0,
                "cache_write_per_mtok": 0.0,
                "currency": "USD",
                "source_url": "https://yibuapi.com/api/pricing",
                "pricing_version": "pricing-v1",
                "source_model": "Qwen3.6 35B A3B",
                "pricing_unit": "mtok",
            }
        ],
    )
    table_hash = hash_table(table)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(RateCard).values(
                id=table.id,
                captured_at=table.captured_at,
                table=table.model_dump(mode="json", exclude={"captured_at"}),
            )
        )
        seeded_trial_ids: list[UUID] = []
        for i, (state, result) in enumerate(
            (
                ("succeeded", {"aggregate_reward": 1.0, "cost_usd": 0.05}),
                ("succeeded", {"aggregate_reward": 0.5, "cost_usd": 0.03}),
                ("running", None),
            )
        ):
            trial_id = uuid4()
            seeded_trial_ids.append(trial_id)
            s.execute(
                insert(Trial).values(
                    id=trial_id,
                    task_id=f"local/mit-{i}",
                    team_id=team_id,
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=cid,
                    result=result,
                )
            )
        s.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[0],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "provider_extras": {},
                    "cost_usd": Decimal("9.990000"),
                    "rate_card_hash": table_hash,
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[1],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000000"),
                    "rate_card_hash": "facade:rate-card:missing",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[2],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000001"),
                    "rate_card_hash": table_hash,
                },
            ],
        )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{cid}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["trial_summary"]["succeeded"] == 2
    assert body["trial_summary"]["running"] == 1
    submitted = body["submitted_by_user"]
    UUID(submitted["id"])
    assert submitted["username"] == f"BatchOwner-{team_id.hex[:8]}"
    assert submitted["team_id"] == str(team_id)
    assert submitted["team_name"] == f"t-{team_id}"
    # avg of 1.0 + 0.5 = 0.75
    assert body["aggregate_reward"] == pytest.approx(0.75)
    assert "total_cost_usd" not in body
    assert body["total_prompt_tokens"] == 18
    assert body["total_completion_tokens"] == 9
    assert body["estimated_cost_usd"] == pytest.approx(9.990001)
    assert body["cost_currency"] == "USD"
    assert body["cost_status"] == "mixed"
    assert body["pricing_modes"] == ["priced", "price-unknown"]
    assert body["llm_calls_count"] == 3
    assert body["price_snapshots"] == [
        {
            "rate_card_hash": table_hash,
            "rate_card_id": "yibuapi-pricing-v1",
            "resolved": True,
            "provider": "yibuapi",
            "source_url": "https://yibuapi.com/api/pricing",
            "pricing_version": "pricing-v1",
            "last_checked_at": "2026-06-27T00:00:00+00:00",
            "currency": "USD",
            "group": "default",
            "group_ratio": 1.0,
        }
    ]


async def test_get_batch_detail_marks_real_provider_zero_call_evidence_invalid(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Task).values(
                id="skilllearnbench/poster-design",
                checksum="z" * 64,
                config=_valid_task_config("skilllearnbench/poster-design"),
                source="local",
                license="MIT",
            )
        )
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="zero-call-real-provider",
                task_filter={"benchmark_id": "skilllearnbench"},
                trial_config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                n_per_task=1,
                result_status="partial_failed",
            )
        )
        for state, reward in (
            ("succeeded", 0.0),
            ("failed", None),
        ):
            conn.execute(
                insert(Trial).values(
                    id=uuid4(),
                    task_id="skilllearnbench/poster-design",
                    team_id=team_id,
                    state=state,
                    failure_reason=None if state == "succeeded" else "agent_error",
                    failure_message=None if state == "succeeded" else "gateway refused",
                    config={
                        "agent_name": "codex",
                        "agent_model": {"provider": "openai", "name": "qwen"},
                    },
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=batch_id,
                    result=({"aggregate_reward": reward} if reward is not None else None),
                )
            )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}?include_debug=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["llm_calls_count"] == 0
    assert body["no_call_trial_count"] == 2
    assert body["no_call_reason_counts"] == {
        "agent_step_no_call": 1,
        "terminal_model_backed_no_call": 1,
    }
    assert body["llm_evidence_status"] == "no_calls_invalid"
    assert body["diagnosis"]["primary_cause"]["reason_code"] == "batch.no_llm_calls"
    assert "did not record any LLM calls" in body["diagnosis"]["summary"]


async def test_get_batch_detail_includes_per_benchmark_rollup(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark),
            [
                {
                    "id": "humaneval-420",
                    "display_name": "HumanEval 420",
                    "upstream_kind": "local",
                    "upstream_locator": "fixture",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/mit",
                    "splits": ["test"],
                },
                {
                    "id": "mbpp-420",
                    "display_name": "MBPP 420",
                    "upstream_kind": "local",
                    "upstream_locator": "fixture",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/mit",
                    "splits": ["test"],
                },
            ],
        )
        for tid, benchmark_id in (
            ("humaneval-420/task-0", "humaneval-420"),
            ("humaneval-420/task-1", "humaneval-420"),
            ("mbpp-420/task-0", "mbpp-420"),
        ):
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="b" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="MIT",
                    benchmark_id=benchmark_id,
                )
            )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "mixed benchmarks",
                "purpose": "evaluation",
                "task_filter": {
                    "subset_kind": "all",
                    "benchmark_ids": ["humaneval-420", "mbpp-420"],
                },
                "trial_config": {},
            },
        )
        assert post.status_code == 201, post.text
        batch_id = UUID(post.json()["batch_id"])

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for task_id, state, reward in (
            ("humaneval-420/task-0", "succeeded", 1.0),
            ("humaneval-420/task-1", "failed", 0.0),
            ("mbpp-420/task-0", "succeeded", 0.5),
        ):
            s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    task_id=task_id,
                    team_id=team_id,
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=batch_id,
                    result={"aggregate_reward": reward},
                )
            )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["aggregate_reward"] == pytest.approx(0.5)
    summary_by_id = {row["benchmark_id"]: row for row in body["benchmark_summary"]}
    assert set(summary_by_id) == {"humaneval-420", "mbpp-420"}

    humaneval = summary_by_id["humaneval-420"]
    assert humaneval["display_name"] == "HumanEval 420"
    assert humaneval["expected_trial_count"] == 2
    assert humaneval["completed_trial_count"] == 2
    assert humaneval["platform_failed_count"] == 1
    assert humaneval["trial_summary"]["succeeded"] == 1
    assert humaneval["trial_summary"]["failed"] == 1
    assert humaneval["aggregate_reward"] == pytest.approx(0.5)

    mbpp = summary_by_id["mbpp-420"]
    assert mbpp["display_name"] == "MBPP 420"
    assert mbpp["expected_trial_count"] == 1
    assert mbpp["completed_trial_count"] == 1
    assert mbpp["platform_failed_count"] == 0
    assert mbpp["trial_summary"]["succeeded"] == 1
    assert mbpp["aggregate_reward"] == pytest.approx(0.5)


async def test_get_batch_detail_includes_per_combination_summary(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    now = datetime.now(UTC)
    combinations = [
        {
            "agent_name": "opencode",
            "agent_model": {"provider": "openai", "name": "glm5.1-thinking"},
            "provider_model_id": "glm5.1-thinking",
            "n_per_task": 2,
            "label": "opencode / glm5.1-thinking",
        },
        {
            "agent_name": "codex",
            "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b"},
            "provider_model_id": "qwen3.6-35b-a3b",
            "n_per_task": 2,
        },
        {
            "agent_name": "oracle",
            "agent_model": None,
            "n_per_task": 2,
            "label": "oracle / no model",
        },
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="multi combo summary",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=6,
                result_status="partial_failed",
                combinations=combinations,
            )
        )
        trial_rows: list[dict[str, object]] = []
        for combination_idx, state, result in (
            (0, "succeeded", {"aggregate_reward": 1.0}),
            (0, "failed", {"aggregate_reward": 0.0}),
            (1, "succeeded", {"aggregate_reward": 0.0}),
            (1, "succeeded", {"notes": "scorer output missing reward"}),
        ):
            trial_id = uuid4()
            trial_rows.append(
                {
                    "id": trial_id,
                    "combination_idx": combination_idx,
                    "state": state,
                }
            )
            conn.execute(
                insert(Trial).values(
                    id=trial_id,
                    batch_id=batch_id,
                    team_id=team_id,
                    task_id="local/mit-0",
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=now,
                    started_at=now,
                    finished_at=now if state != "running" else None,
                    combination_idx=combination_idx,
                    result=result,
                )
            )
        conn.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": trial_rows[0]["id"],
                    "step_id": "main",
                    "model": "openai/glm5.1-thinking",
                    "dialect": "openai",
                    "input_tokens": 10,
                    "output_tokens": 4,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.010000"),
                    "rate_card_hash": "facade:tokens-only:test",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": trial_rows[1]["id"],
                    "step_id": "main",
                    "model": "openai/glm5.1-thinking",
                    "dialect": "openai",
                    "input_tokens": 6,
                    "output_tokens": 3,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.020000"),
                    "rate_card_hash": "facade:rate-card:missing:test",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": trial_rows[2]["id"],
                    "step_id": "main",
                    "model": "openai/qwen3.6-35b-a3b",
                    "dialect": "openai",
                    "input_tokens": 2,
                    "output_tokens": 1,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.030000"),
                    "rate_card_hash": "failed-upstream",
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["aggregate_reward"] == pytest.approx(1 / 3)

    summary = body["combination_summary"]
    assert [row["combination_idx"] for row in summary] == [0, 1, 2]

    opencode = summary[0]
    assert opencode["label"] == "opencode / glm5.1-thinking"
    assert opencode["agent_name"] == "opencode"
    assert opencode["provider_model_id"] == "glm5.1-thinking"
    assert opencode["trial_count"] == 2
    assert opencode["expected_trial_count"] == 2
    assert opencode["scored_trial_count"] == 2
    assert opencode["succeeded_count"] == 1
    assert opencode["failed_count"] == 1
    assert opencode["aggregate_reward"] == pytest.approx(0.5)
    assert opencode["llm_calls_count"] == 2
    assert opencode["total_prompt_tokens"] == 16
    assert opencode["total_completion_tokens"] == 7

    codex = summary[1]
    assert codex["label"] == "codex / qwen3.6-35b-a3b"
    assert codex["trial_count"] == 2
    assert codex["expected_trial_count"] == 2
    assert codex["scored_trial_count"] == 1
    assert codex["succeeded_count"] == 2
    assert codex["failed_count"] == 0
    assert codex["aggregate_reward"] == 0
    assert codex["llm_calls_count"] == 1
    assert codex["total_prompt_tokens"] == 2
    assert codex["total_completion_tokens"] == 1

    oracle = summary[2]
    assert oracle["label"] == "oracle / no model"
    assert oracle["trial_count"] == 0
    assert oracle["expected_trial_count"] == 2
    assert oracle["scored_trial_count"] == 0
    assert oracle["succeeded_count"] == 0
    assert oracle["failed_count"] == 0
    assert oracle["aggregate_reward"] is None
    assert oracle["llm_calls_count"] == 0


async def test_get_batch_detail_returns_empty_combination_summary_for_legacy_batch(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="legacy single combo",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="succeeded",
                combinations=[],
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                batch_id=batch_id,
                team_id=team_id,
                task_id="local/mit-0",
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                result={"aggregate_reward": 1.0},
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    assert r.json()["combination_summary"] == []


async def test_get_batch_detail_combination_expected_counts_honor_required_pools_and_fanout(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    now = datetime.now(UTC)
    combinations = [
        {
            "agent_name": "opencode",
            "agent_model": {"provider": "openai", "name": "glm5.1-thinking"},
            "provider_model_id": "glm5.1-thinking",
            "n_per_task": 1,
            "label": "opencode / glm5.1-thinking",
        },
        {
            "agent_name": "codex",
            "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b"},
            "provider_model_id": "qwen3.6-35b-a3b",
            "n_per_task": 1,
            "label": "codex / qwen3.6-35b-a3b",
        },
    ]
    fanout_errors = [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": 1,
            "idempotency_key": f"{batch_id}::local/mit-0::1::0",
            "status_code": 403,
            "detail": "agent incompatible with task",
            "created_at": now.isoformat(),
        }
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="multi combo expected counts",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                # Original plan was 1 task * 2 combos + 1 coverage trial.
                # One non-retryable fanout failure on combo 1 adjusts this to 2.
                expected_trial_count=2,
                result_status="partial_failed",
                combinations=combinations,
                required_worker_pools=["local-gpu-canary"],
                fanout_errors=fanout_errors,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                batch_id=batch_id,
                team_id=team_id,
                task_id="local/mit-0",
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                sample_idx=0,
                combination_idx=0,
                result={"aggregate_reward": 1.0},
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    summary = r.json()["combination_summary"]
    assert [row["expected_trial_count"] for row in summary] == [2, 0]
    assert [row["trial_count"] for row in summary] == [1, 0]


async def test_get_batch_detail_exposes_fanout_failure(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    fanout_errors = [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": None,
            "idempotency_key": f"{batch_id}::local/mit-0::0",
            "status_code": 403,
            "detail": (
                "task license proprietary-MAA not in team allowlist; "
                "Authorization: Bearer loom_api_supersecret; "
                "http://loom-control-plane:8080/trials; "
                "https://minio.internal/a?X-Amz-Signature=secret"
            ),
            "seen_at": datetime.now(UTC).isoformat(),
        }
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="policy-blocked",
                task_filter={"license": "MIT"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=0,
                result_status="all_failed",
                fanout_errors=fanout_errors,
                finished_at=datetime.now(UTC),
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        debug = await ac.get(
            f"/api/v1/batches/{batch_id}/debug",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["failure_reason"] == "fanout_submit_failed"
    assert "proprietary-MAA" in body["failure_message"]
    assert "loom_api_supersecret" not in body["failure_message"]
    assert "loom-control-plane" not in body["failure_message"]
    assert "X-Amz-Signature=secret" not in body["failure_message"]
    assert body["fanout_errors"][0]["task_id"] == "local/mit-0"
    assert "loom_api_supersecret" not in json.dumps(body["fanout_errors"])
    assert "debug_evidence" not in body

    assert debug.status_code == 200, debug.text
    evidence = debug.json()
    assert evidence["schema_version"] == "1"
    assert evidence["entity"]["type"] == "batch"
    assert evidence["entity"]["id"] == str(batch_id)
    assert evidence["entity"]["team_id"] == str(team_id)
    assert evidence["failure"]["reason_code"] == "batch.fanout_submit_failed"
    assert evidence["failure"]["category"] == "submit"
    assert evidence["failure"]["attribution"] == "platform"
    assert evidence["lifecycle"]["state"] == "finished"
    assert evidence["task_selection"]["expected_trial_count"] == 0
    assert evidence["task_selection"]["fanout_errors"][0]["task_id"] == ("local/mit-0")
    rendered = json.dumps(evidence)
    assert "loom_api_supersecret" not in rendered
    assert "loom-control-plane" not in rendered
    assert "X-Amz-Signature=secret" not in rendered
    assert "Inspect batch fan-out errors" in " ".join(evidence["next_actions"])


async def test_batch_debug_cross_team_forbidden(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    batch_id = uuid4()
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="private-batch",
                task_filter={"license": "MIT"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=0,
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            )
        )
        s.execute(insert(Team).values(id=other_team, name=f"o-{other_team}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=other_team,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
        ) as ac:
            r = await ac.get(
                f"/api/v1/batches/{batch_id}/debug",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
            diagnosis = await ac.get(
                f"/api/v1/batches/{batch_id}/diagnosis",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
        assert r.status_code == 403
        assert diagnosis.status_code == 403
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            from loom.db.schema import Token as TokenModel

            s.execute(
                delete(TokenModel).where(
                    TokenModel.team_id == other_team,
                )
            )
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == other_team))
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()


async def test_batch_diagnosis_clusters_failed_trials(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    trial_ids = [uuid4() for _ in range(4)]
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="gateway-cluster",
                task_filter={"benchmark_ids": ["humaneval"]},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=4,
                result_status="all_failed",
                provider_model_id="qwen2.5-coder",
                finished_at=now,
            )
        )
        for i, trial_id in enumerate(trial_ids):
            reason = "gateway_error" if i < 3 else "verifier_error"
            conn.execute(
                insert(Trial).values(
                    id=trial_id,
                    batch_id=batch_id,
                    team_id=team_id,
                    task_id=f"local/mit-{i % 3}",
                    state="failed",
                    config={"agent_name": "litellm"},
                    requires_caps={},
                    submitted_at=now,
                    started_at=now,
                    finished_at=now,
                    failure_reason=reason,
                    failure_message=(
                        "provider returned error with Authorization: Bearer loom_api_supersecret"
                    ),
                )
            )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        detail = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        diagnosis = await ac.get(
            f"/api/v1/batches/{batch_id}/diagnosis",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert diagnosis.status_code == 200, diagnosis.text
    body = diagnosis.json()
    assert body["schema_version"] == "1"
    assert body["entity"] == {"type": "batch", "id": str(batch_id)}
    assert body["summary"] == (
        "The batch failed because most failed child trials hit provider "
        "gateway errors before scoring."
    )
    assert body["primary_cause"]["reason_code"] == "trial.gateway_error"
    assert body["primary_cause"]["affected_trials"] == 3
    assert body["primary_cause"]["affected_ratio"] == pytest.approx(0.75)
    assert body["reason_clusters"][0]["reason_code"] == "trial.gateway_error"
    assert body["reason_clusters"][0]["count"] == 3
    assert body["reason_clusters"][0]["representative_trial_id"] == (str(trial_ids[0]))
    assert "not reliable" in body["impact"]
    assert any(action.get("action") == "rerun_failed" for action in body["next_actions"])
    assert "loom_api_supersecret" not in json.dumps(body)

    assert detail.status_code == 200, detail.text
    assert "diagnosis" not in detail.json()


async def test_batch_detail_omits_heavy_debug_by_default_and_includes_on_request(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="debug-opt-in",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="all_failed",
                finished_at=now,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                batch_id=batch_id,
                team_id=team_id,
                task_id="local/mit-0",
                state="failed",
                config={},
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                failure_reason="gateway_error",
                failure_message="provider disconnected",
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        default = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        opt_in = await ac.get(
            f"/api/v1/batches/{batch_id}?include_debug=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert default.status_code == 200, default.text
    default_body = default.json()
    assert "debug_evidence" not in default_body
    assert "diagnosis" not in default_body

    assert opt_in.status_code == 200, opt_in.text
    opt_in_body = opt_in.json()
    assert opt_in_body["debug_evidence"]["entity"]["id"] == str(batch_id)
    assert opt_in_body["diagnosis"]["primary_cause"]["reason_code"] == ("trial.gateway_error")


async def test_batch_detail_default_does_not_materialize_llm_call_rows(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.routes import batches as batch_routes

    app, raw, team_id = camp_setup
    batch_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="lightweight-detail",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="succeeded",
                finished_at=now,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                batch_id=batch_id,
                team_id=team_id,
                task_id="local/mit-0",
                state="succeeded",
                config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                result={"aggregate_reward": 1.0},
            )
        )
    sync_engine.dispose()

    async def fail_if_full_rows_are_loaded(*_args: object, **_kwargs: object) -> list:
        raise AssertionError("default batch detail should not load LlmCall rows")

    monkeypatch.setattr(
        batch_routes,
        "_llm_calls_for_trials",
        fail_if_full_rows_are_loaded,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        response = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["llm_calls_count"] == 0
    assert body["llm_evidence_status"] == "no_calls_invalid"
    assert body["no_call_reason_counts"] == {"terminal_model_backed_no_call": 1}


async def test_batch_detail_default_does_not_select_trajectory_index(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.routes import batches as batch_routes

    app, raw, team_id = camp_setup
    batch_id = uuid4()
    trial_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="lightweight-default-detail",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="succeeded",
                finished_at=now,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=trial_id,
                batch_id=batch_id,
                team_id=team_id,
                task_id="local/mit-0",
                state="succeeded",
                config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                result={"aggregate_reward": 1.0},
                trajectory_index={
                    "trajectory_uri": "s3://trajectories/large.jsonl",
                    "artifacts": [
                        {
                            "key": f"artifact-{i}.json",
                            "size": 1024,
                            "share_status": "shared",
                        }
                        for i in range(200)
                    ],
                },
            )
        )
    sync_engine.dispose()

    original_select = batch_routes.select

    def fail_on_full_trial_row_select(*entities: object, **kwargs: object) -> object:
        if any(entity is Trial for entity in entities):
            raise AssertionError("default batch detail should not load full Trial rows")
        return original_select(*entities, **kwargs)

    monkeypatch.setattr(batch_routes, "select", fail_on_full_trial_row_select)

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        response = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "debug_path",
    [
        "/api/v1/batches/{batch_id}?include_debug=true",
        "/api/v1/batches/{batch_id}/debug",
        "/api/v1/batches/{batch_id}/diagnosis",
    ],
)
async def test_batch_debug_surfaces_do_not_select_trajectory_index(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    debug_path: str,
) -> None:
    from loom_service.routes import batches as batch_routes

    app, raw, team_id = camp_setup
    batch_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="bounded-debug-detail",
                task_filter={"task_ids": ["local/mit-0", "local/mit-1", "local/mit-2"]},
                trial_config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=3,
                result_status="partial_failed",
                finished_at=now,
            )
        )
        for idx in range(3):
            conn.execute(
                insert(Trial).values(
                    id=uuid4(),
                    batch_id=batch_id,
                    team_id=team_id,
                    task_id=f"local/mit-{idx}",
                    state="failed" if idx == 1 else "succeeded",
                    config={
                        "agent_name": "codex",
                        "agent_model": {
                            "provider": "openai",
                            "name": "gpt-4o-mini",
                        },
                    },
                    requires_caps={},
                    submitted_at=now,
                    started_at=now,
                    finished_at=now,
                    sample_idx=idx,
                    combination_idx=0,
                    provider_model_id="gpt-4o-mini",
                    failure_reason="gateway_error" if idx == 1 else None,
                    failure_message="provider disconnected" if idx == 1 else None,
                    result={"aggregate_reward": 1.0 if idx != 1 else 0.0},
                    trajectory_index={
                        "trajectory_uri": "s3://trajectories/large.jsonl",
                        "artifacts": [
                            {
                                "key": f"trial-{idx}/artifact-{i}.json",
                                "size": 1024,
                                "share_status": "shared",
                            }
                            for i in range(500)
                        ],
                    },
                )
            )
    sync_engine.dispose()

    original_select = batch_routes.select

    def fail_on_full_trial_row_select(*entities: object, **kwargs: object) -> object:
        if any(entity is Trial for entity in entities):
            raise AssertionError("batch debug surfaces should not load full Trial rows")
        return original_select(*entities, **kwargs)

    monkeypatch.setattr(batch_routes, "select", fail_on_full_trial_row_select)

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        response = await ac.get(
            debug_path.format(batch_id=batch_id),
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text
    assert "large.jsonl" not in response.text
    assert "artifact-499" not in response.text
    body = response.json()
    if debug_path.endswith("/debug"):
        assert body["entity"]["type"] == "batch"
        assert body["trials"]["summary"]["failed"] == 1
    elif debug_path.endswith("/diagnosis"):
        assert body["entity"]["type"] == "batch"
        assert body["primary_cause"]["reason_code"]
    else:
        assert body["debug_evidence"]["trials"]["summary"]["failed"] == 1
        assert body["diagnosis"]["primary_cause"]["reason_code"]


async def test_rerun_failed_batch_creates_linked_exact_targets(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    failed_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                provider_connection_id=_seed_connection(postgres_url, team_id, "qwen"),
                id=batch_id,
                team_id=team_id,
                name="gateway-flaked",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                n_per_task=2,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result={"aggregate_reward": 1.0, "cost_usd": 0.01},
            )
        )
        conn.execute(
            insert(Trial).values(
                id=failed_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 503.",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=1,
                combination_idx=0,
                result=None,
            )
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    rerun_batch_id = UUID(body["batch_id"])
    assert body["rerun_of_batch_id"] == str(batch_id)
    assert body["expected_trial_count"] == 1
    assert body["rerun_target_count"] == 1

    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == rerun_batch_id),
        ).scalar_one()
    sync_engine.dispose()

    assert row.rerun_of_batch_id == batch_id
    assert row.resolved_task_ids == ["local/mit-0"]
    assert row.source_provenance[0] == {
        "kind": "supplemental_rerun",
        "source_batch_id": str(batch_id),
    }
    assert row.rerun_targets == [
        {
            "task_id": "local/mit-0",
            "sample_idx": 1,
            "combination_idx": 0,
            "original_trial_id": str(failed_trial_id),
            "failure_reason": "gateway_error",
        }
    ]


async def test_rerun_failed_validates_only_failed_agent_task_coordinates(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    failed_trial_id = uuid4()
    combinations = [
        {
            "agent_name": "terminus-2",
            "agent_model": {"provider": "openai", "name": "qwen"},
            "n_per_task": 1,
        },
        {
            "agent_name": "direct-completion",
            "agent_model": {"provider": "openai", "name": "qwen"},
            "n_per_task": 1,
        },
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            Task.__table__.update()
            .where(Task.id == "local/mit-0")
            .values(config=_workspace_task_config("local/mit-0")),
        )
        conn.execute(
            insert(Batch).values(
                provider_connection_id=_seed_connection(postgres_url, team_id, "qwen"),
                id=batch_id,
                team_id=team_id,
                name="coordinate-specific-admission",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                combinations=combinations,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(Trial),
            [
                {
                    "id": failed_trial_id,
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "gateway_error",
                    "failure_message": "gateway 503",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": uuid4(),
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "succeeded",
                    "failure_reason": None,
                    "failure_message": None,
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 1,
                    "result": {"aggregate_reward": 1.0},
                },
            ],
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 201, response.text
    body = response.json()
    rerun_batch_id = UUID(body["batch_id"])
    assert body["rerun_target_count"] == 1

    with sync_engine.connect() as conn:
        rerun_targets = conn.execute(
            select(Batch.rerun_targets).where(Batch.id == rerun_batch_id),
        ).scalar_one()
    sync_engine.dispose()

    assert rerun_targets == [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": 0,
            "original_trial_id": str(failed_trial_id),
            "failure_reason": "gateway_error",
        }
    ]


async def test_rerun_failed_rejects_invalid_stored_combination_index(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="invalid-rerun-combination-index",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                combinations=[
                    {
                        "agent_name": "direct-completion",
                        "agent_model": {"provider": "openai", "name": "qwen"},
                        "n_per_task": 1,
                    }
                ],
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="gateway 503",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=1,
                result=None,
            ),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    with sync_engine.connect() as conn:
        batch_count = conn.execute(select(func.count(Batch.id))).scalar_one()
    sync_engine.dispose()

    assert response.status_code == 400, response.text
    assert response.json()["detail"] == "rerun target combination_idx 1 is invalid"
    assert batch_count == 1


async def test_rerun_failed_rejects_task_that_became_agent_incompatible(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            Task.__table__.update()
            .where(Task.id == "local/mit-0")
            .values(config=_workspace_task_config("local/mit-0")),
        )
        conn.execute(
            insert(Batch).values(
                provider_connection_id=_seed_connection(postgres_url, team_id, "qwen"),
                id=batch_id,
                team_id=team_id,
                name="now-incompatible",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "direct-completion",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                n_per_task=1,
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            ),
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="gateway 503",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result=None,
            ),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 400, response.text
    assert "workspace_exec" in response.json()["detail"]
    with sync_engine.begin() as conn:
        batch_count = conn.execute(select(func.count()).select_from(Batch)).scalar_one()
    sync_engine.dispose()
    assert batch_count == 1


async def test_rerun_failed_rejects_historical_benchmark_task(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    profile_id = f"historical-rerun-{uuid4().hex}"
    batch_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as connection:
        connection.execute(
            insert(Benchmark).values(
                id=profile_id,
                display_name="Historical rerun profile",
                upstream_kind="test",
                upstream_locator="test",
                upstream_revision="1",
                license_spdx="MIT",
                license_url="https://example.test/license",
                splits=["test"],
                execution_state="historical",
            )
        )
        connection.execute(
            Task.__table__.update().where(Task.id == "local/mit-0").values(benchmark_id=profile_id)
        )
        connection.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="historical-failure",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                n_per_task=1,
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            )
        )
        connection.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="gateway 503",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result=None,
            )
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["reason"] == "benchmark_retired"


async def test_rerun_failed_batch_preserves_duplicate_task_coordinates(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    first_failed_trial_id = uuid4()
    second_failed_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                provider_connection_id=_seed_connection(postgres_url, team_id, "qwen"),
                id=batch_id,
                team_id=team_id,
                name="gateway-flaked-twice",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=3,
                n_per_task=3,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial),
            [
                {
                    "id": first_failed_trial_id,
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "gateway_error",
                    "failure_message": "gateway 503",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": uuid4(),
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "succeeded",
                    "failure_reason": None,
                    "failure_message": None,
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 1,
                    "combination_idx": 0,
                    "result": {"aggregate_reward": 1.0},
                },
                {
                    "id": second_failed_trial_id,
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "provider_timeout",
                    "failure_message": "provider timeout",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 2,
                    "combination_idx": 0,
                    "result": None,
                },
            ],
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 2
    assert body["rerun_target_count"] == 2
    assert body["rerun_plan"]["supplemental_task_ids"] == ["local/mit-0"]
    assert body["rerun_plan"]["supplemental_coordinates"] == [
        {"task_id": "local/mit-0", "sample_idx": 0, "combination_idx": 0},
        {"task_id": "local/mit-0", "sample_idx": 2, "combination_idx": 0},
    ]
    rerun_batch_id = UUID(body["batch_id"])

    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == rerun_batch_id),
        ).scalar_one()
    sync_engine.dispose()

    assert row.rerun_targets == [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": 0,
            "original_trial_id": str(first_failed_trial_id),
            "failure_reason": "gateway_error",
        },
        {
            "task_id": "local/mit-0",
            "sample_idx": 2,
            "combination_idx": 0,
            "original_trial_id": str(second_failed_trial_id),
            "failure_reason": "provider_timeout",
        },
    ]


async def test_batch_rerun_plan_classifies_score_task_and_platform_outcomes(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="source-useful-main",
                task_filter={
                    "task_ids": [
                        "local/mit-0",
                        "local/mit-1",
                        "local/apache-0",
                        "local/apache-1",
                    ],
                    "subset_kind": "explicit",
                },
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=4,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial),
            [
                {
                    "id": uuid4(),
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "gateway_error",
                    "failure_message": "gateway 503",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": uuid4(),
                    "task_id": "local/mit-1",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "verifier_timeout",
                    "failure_message": "verifier exceeded timeout",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": uuid4(),
                    "task_id": "local/apache-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "task_compatibility",
                    "failure_message": "task bundle is incompatible",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": uuid4(),
                    "task_id": "local/apache-1",
                    "team_id": team_id,
                    "state": "succeeded",
                    "failure_reason": None,
                    "failure_message": None,
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": {"aggregate_reward": 0.0},
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}/rerun-plan",
            headers={"Authorization": f"Bearer {raw}"},
            params=[
                ("task_id", "local/apache-1"),
                ("task_id", "local/mit-0"),
                ("task_id", "local/mit-1"),
                ("task_id", "local/apache-0"),
            ],
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["supplemental_task_ids"] == ["local/mit-0"]
    assert [item["task_id"] for item in body["auto_safe"]] == ["local/mit-0"]
    assert [item["task_id"] for item in body["operator_approval"]] == ["local/mit-1"]
    assert [item["task_id"] for item in body["not_rerunnable"]] == [
        "local/apache-0",
        "local/apache-1",
    ]
    assert body["not_rerunnable"][1]["failure_class"] == "score_failure"
    assert body["not_rerunnable"][1]["platform_outcome"] == "success"
    assert body["summary"] == {
        "auto_safe": 1,
        "operator_approval": 1,
        "not_rerunnable": 2,
        "already_covered": 0,
        "selected_final_trials": 4,
    }


async def test_rerun_failed_batch_accepts_task_ids_and_returns_plan(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    auto_safe_trial_id = uuid4()
    task_failure_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="mixed-failures",
                task_filter={
                    "task_ids": ["local/mit-0", "local/apache-0"],
                    "subset_kind": "explicit",
                },
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial),
            [
                {
                    "id": auto_safe_trial_id,
                    "task_id": "local/mit-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "gateway_error",
                    "failure_message": "gateway 503",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
                {
                    "id": task_failure_trial_id,
                    "task_id": "local/apache-0",
                    "team_id": team_id,
                    "state": "failed",
                    "failure_reason": "task_compatibility",
                    "failure_message": "task bundle is incompatible",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": datetime.now(UTC),
                    "batch_id": batch_id,
                    "sample_idx": 0,
                    "combination_idx": 0,
                    "result": None,
                },
            ],
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_ids": ["local/apache-0", "local/mit-0"]},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["rerun_plan"]["supplemental_task_ids"] == ["local/mit-0"]
    assert body["rerun_plan"]["summary"]["not_rerunnable"] == 1
    assert body["rerun_plan"]["auto_safe"][0]["failure_class"] == "platform_failure"
    rerun_batch_id = UUID(body["batch_id"])

    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == rerun_batch_id),
        ).scalar_one()
    sync_engine.dispose()

    assert row.rerun_targets == [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": 0,
            "original_trial_id": str(auto_safe_trial_id),
            "failure_reason": "gateway_error",
        }
    ]


async def test_legacy_team_token_cannot_rerun_failed_batch(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = camp_setup
    batch_id = uuid4()
    failed_trial_id = uuid4()
    legacy_raw = f"loom_team_{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="gateway-flaked",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                n_per_task=1,
                result_status="failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=failed_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 503.",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result=None,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {legacy_raw}"},
        )

    assert r.status_code == 403
    assert "legacy team token" in r.json()["detail"]


async def test_get_batch_detail_effective_rollup_uses_successful_rerun(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    rerun_batch_id = uuid4()
    failed_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    original_success_trial_id = uuid4()
    rerun_success_trial_id = uuid4()
    combinations = [
        {
            "agent_name": "opencode",
            "agent_model": {"provider": "openai", "name": "glm5.1-thinking"},
            "provider_model_id": "glm5.1-thinking",
            "n_per_task": 1,
            "label": "opencode / glm5.1-thinking",
        },
        {
            "agent_name": "codex",
            "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b"},
            "provider_model_id": "qwen3.6-35b-a3b",
            "n_per_task": 1,
            "label": "codex / qwen3.6-35b-a3b",
        },
    ]
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="original",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                n_per_task=2,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
                combinations=combinations,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=original_success_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result={"aggregate_reward": 1.0, "cost_usd": 0.01},
            )
        )
        conn.execute(
            insert(Trial).values(
                id=failed_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=1,
            )
        )
        conn.execute(
            insert(Batch).values(
                id=rerun_batch_id,
                team_id=team_id,
                name="original failed-case rerun",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="succeeded",
                finished_at=datetime.now(UTC),
                rerun_of_batch_id=batch_id,
                combinations=combinations,
                rerun_targets=[
                    {
                        "task_id": "local/mit-0",
                        "sample_idx": 0,
                        "combination_idx": 1,
                        "original_trial_id": str(failed_trial_id),
                        "failure_reason": "gateway_error",
                    }
                ],
            )
        )
        conn.execute(
            insert(Trial).values(
                id=rerun_success_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=rerun_batch_id,
                sample_idx=0,
                combination_idx=1,
                result={"aggregate_reward": 0.8, "cost_usd": 0.02},
            )
        )
        conn.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": original_success_trial_id,
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 5,
                    "output_tokens": 2,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.010000"),
                    "rate_card_hash": "old-rate-card",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": rerun_success_trial_id,
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 11,
                    "output_tokens": 4,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.020000"),
                    "rate_card_hash": "old-rate-card",
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["trial_summary"]["succeeded"] == 1
    assert body["trial_summary"]["failed"] == 1
    assert body["effective_trial_summary"]["succeeded"] == 2
    assert body["effective_trial_summary"]["failed"] == 0
    assert body["effective_result_status"] == "succeeded"
    assert body["effective_aggregate_reward"] == pytest.approx(0.9)
    assert "total_cost_usd" not in body
    assert "effective_total_cost_usd" not in body
    assert body["total_prompt_tokens"] == 5
    assert body["total_completion_tokens"] == 2
    assert body["estimated_cost_usd"] == pytest.approx(0.01)
    assert body["cost_status"] == "estimated"
    assert body["llm_calls_count"] == 1
    assert body["effective_total_prompt_tokens"] == 16
    assert body["effective_total_completion_tokens"] == 6
    assert body["effective_estimated_cost_usd"] == pytest.approx(0.03)
    assert body["effective_cost_status"] == "estimated"
    assert body["effective_llm_calls_count"] == 2
    assert body["rerunnable_failed_count"] == 1
    assert body["rerun_batches"][0]["id"] == str(rerun_batch_id)
    assert [row["combination_idx"] for row in body["combination_summary"]] == [0, 1]
    assert [row["combination_idx"] for row in body["effective_combination_summary"]] == [
        0,
        1,
    ]
    original_combo = body["combination_summary"][1]
    assert original_combo["expected_trial_count"] == 1
    assert original_combo["trial_count"] == 1
    assert original_combo["scored_trial_count"] == 0
    assert original_combo["aggregate_reward"] is None
    assert original_combo["llm_calls_count"] == 0

    effective_combo = body["effective_combination_summary"][1]
    assert effective_combo["expected_trial_count"] == 1
    assert effective_combo["trial_count"] == 1
    assert effective_combo["scored_trial_count"] == 1
    assert effective_combo["aggregate_reward"] == pytest.approx(0.8)
    assert effective_combo["llm_calls_count"] == 1
    assert effective_combo["total_prompt_tokens"] == 11
    assert effective_combo["total_completion_tokens"] == 4


async def test_get_batch_not_found(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{uuid4()}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_cancel_batch_cascades_to_active_trials(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "to-cancel",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        cid = UUID(post.json()["batch_id"])

    # 1 queued, 1 succeeded — cancel should only touch the queued.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    queued_id = uuid4()
    succ_id = uuid4()
    with sl() as s:
        s.execute(
            insert(Trial).values(
                id=queued_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="queued",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=cid,
            )
        )
        s.execute(
            insert(Trial).values(
                id=succ_id,
                task_id="local/mit-1",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=cid,
                result={"aggregate_reward": 1.0},
            )
        )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        cancel = await ac.post(
            f"/api/v1/batches/{cid}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert cancel.status_code == 200
    assert cancel.json()["state"] == "cancelled"

    # Re-fetch trial states.
    from sqlalchemy import select

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        queued_state = s.execute(
            select(Trial.state).where(Trial.id == queued_id),
        ).scalar_one()
        succ_state = s.execute(
            select(Trial.state).where(Trial.id == succ_id),
        ).scalar_one()
    sync_engine.dispose()
    assert queued_state == "cancelled"
    assert succ_state == "succeeded"  # terminal trial untouched


# ──────────────────────────────────────────────────────────────────────
# #320: oracle × incompatible-task preflight rejection
# ──────────────────────────────────────────────────────────────────────


async def test_post_batch_rejects_oracle_against_non_pytest_tasks(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """oracle hard-requires `solution/solve.sh`; aime/gpqa-style
    script-verifier tasks do not ship one. Submitting `agent_name=oracle`
    against those tasks used to deterministically AgentError mid-trial.
    After #320, the preflight rejects upfront with a structured detail."""
    _app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        # An aime-like task: verifier=script, no solve.sh in bundle.
        s.execute(
            insert(Task).values(
                id="local/script-only-0",
                checksum="x" * 64,
                config=_script_verifier_task_config("local/script-only-0"),
                source="local",
                license="MIT",
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "oracle-on-script-task",
                "purpose": "evaluation",
                "task_filter": {
                    "task_ids": ["local/script-only-0"],
                    "subset_kind": "explicit",
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "agent×task capability mismatch" in detail
    assert "oracle" in detail
    assert "local/script-only-0" in detail
    assert "solve.sh" in detail


async def test_post_batch_allows_oracle_against_terminal_bench_tasks(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Terminal-Bench-2 uses the script verifier, but its adapter emits
    `solution/solve.sh` wrappers for oracle reference runs."""
    _app, raw, _team_id = camp_setup
    task_id = "terminal-bench-2/simple-web-scraper"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="y" * 64,
                config=_script_verifier_task_config(task_id),
                source="local",
                license="MIT",
                tags={"oracle_eligible": "true"},
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "oracle-on-terminal-bench-task",
                "purpose": "evaluation",
                "task_filter": {
                    "task_ids": [task_id],
                    "subset_kind": "explicit",
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 201, r.text


async def test_post_batch_allows_oracle_against_pytest_tasks(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Negative-space guard: oracle×pytest-verifier tasks (mbpp shape)
    keep working unchanged. Fixture's MIT tasks all use verifier=pytest
    so this is the unmodified happy path."""
    _app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "oracle-on-pytest-tasks",
                "purpose": "evaluation",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 201, r.text


async def test_post_batch_does_not_filter_when_agent_has_no_requirements(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """litellm and the subprocess agents have empty
    `requires_capabilities`. The preflight must short-circuit them so
    a model-backed batch isn't blocked from running against an
    aime-shape task."""
    _app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="local/script-only-1",
                checksum="x" * 64,
                config=_script_verifier_task_config("local/script-only-1"),
                source="local",
                license="MIT",
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "litellm-on-script-task",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
                "task_filter": {
                    "task_ids": ["local/script-only-1"],
                    "subset_kind": "explicit",
                },
                "trial_config": {
                    "agent_name": "litellm",
                    "agent_model": {
                        "provider": "openai",
                        "name": "gpt-4o-mini",
                    },
                },
            },
        )
    assert r.status_code == 201, r.text


@pytest.mark.parametrize("agent_name", ["direct-completion", "litellm"])
async def test_post_batch_rejects_completion_agent_for_workspace_task(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
    agent_name: str,
) -> None:
    app, raw, team_id = camp_setup
    task_id = f"local/workspace-{agent_name}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="w" * 64,
                config=_workspace_task_config(task_id),
                source="local",
                license="MIT",
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "completion-on-workspace",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
                "task_filter": {
                    "task_ids": [task_id],
                    "subset_kind": "explicit",
                },
                "trial_config": {
                    "agent_name": agent_name,
                    "agent_model": {
                        "provider": "openai",
                        "name": "gpt-4o-mini",
                    },
                },
            },
        )

    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert "agent×task capability mismatch" in detail
    assert task_id in detail
    assert "workspace_exec" in detail


async def test_post_batch_allows_workspace_agent_for_workspace_task(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    task_id = "local/workspace-terminus"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="z" * 64,
                config=_workspace_task_config(task_id),
                source="local",
                license="MIT",
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "terminus-on-workspace",
                "purpose": "evaluation",
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
                "task_filter": {
                    "task_ids": [task_id],
                    "subset_kind": "explicit",
                },
                "trial_config": {
                    "agent_name": "terminus-2",
                    "agent_model": {
                        "provider": "openai",
                        "name": "gpt-4o-mini",
                    },
                },
            },
        )

    assert response.status_code == 201, response.text


@pytest.fixture
def hosted_environment(camp_setup, monkeypatch):
    """Native target records use a supported hosted environment identity."""
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)


async def test_rerun_failed_rejects_old_conflicting_selection_and_keeps_parent(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """#2054: a rerun creates new trials, so an old batch whose stored
    provider_model_id disagrees with the model that runs cannot be rerun.
    The accepted parent batch is left untouched."""
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    connection_id = _seed_connection(postgres_url, team_id, "qwen", "qwen-mini")
    combinations = [
        {
            "agent_name": "litellm",
            "agent_model": {"provider": "openai", "name": "qwen"},
            "n_per_task": 1,
            "label": "old-conflict",
            "provider_connection_id": connection_id,
            "provider_model_id": "qwen-mini",
        },
    ]
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="pre-2054 conflict",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                combinations=combinations,
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                n_per_task=1,
                result_status="all_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 503.",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result=None,
            )
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 400, r.text
    assert "conflicts with provider_model_id 'qwen-mini'" in r.json()["detail"]
    with sessionmaker(sync_engine)() as s:
        parent = s.execute(select(Batch).where(Batch.id == batch_id)).scalar_one()
        assert parent.combinations == combinations
        assert s.execute(select(Batch).where(Batch.rerun_of_batch_id == batch_id)).first() is None
    sync_engine.dispose()


@pytest.mark.usefixtures("hosted_environment")
async def test_execution_selection_dry_run_resolves_or_rejects_without_writing(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """#2314: one submission reports every axis conflict; dry run never writes."""
    app, raw, team_id = camp_setup
    task_id = "local/execution-selection-dry-run"
    target_id = "nebius-execution-selection-dry-run"
    profile = _service_execution_runtime_profile()
    app.state.settings = app.state.settings.model_copy(update={
        "service_execution_runtime_profile_json": json.dumps(profile.model_dump(mode="json")),
    })
    execution_class_spec = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Worker))
        s.add(ServiceExecutionClass(
            id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
            schema_version=NEBIUS_CPU_EXECUTION_CLASS_V1.schema_version,
            spec_json=execution_class_spec, spec_sha256=canonical_digest(execution_class_spec),
            enabled=True,
        ))
        s.add(ServiceExecutionTarget(
            id=target_id, logical_pool_id="nebius-cpu",
            execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
            schema_version="loom.execution-target.v1", spec_json={"health_stale_after_seconds": 60},
            spec_sha256="sha256:" + "e" * 64, environment="development", provider="nebius",
            region="eu-north1", failure_domain="eu-north1-a", data_residency="eu",
            desired_state="active", observed_state="ready", health_status="healthy",
            health_observed_at=datetime.now(UTC),
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="c" * 64, config=_automatic_service_execution_task_config(task_id),
            source="s3://artifacts/task-inputs/task/",
            source_provenance={"service_execution_input": {
                "schema_version": "loom.service-execution-input.v1",
                "manifest_uri": "s3://artifacts/task-inputs/task.json",
                "manifest_sha256": "sha256:" + "d" * 64, "file_count": 3, "total_bytes": 4096,
            }},
            license="MIT",
        ))
        s.commit()
        batches_before = s.scalar(select(func.count()).select_from(Batch))

    connection_id = _seed_connection(postgres_url, team_id, "gpt-5")

    def payload(**trial: object) -> dict[str, object]:
        return {
            "name": "execution-selection-dry-run",
            "purpose": "evaluation",
            "provider_connection_id": connection_id,
            "task_filter": {"subset_kind": "explicit", "task_ids": [task_id]},
            "trial_config": {
                "agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "gpt-5", "source": "api"},
                **trial,
            },
            "backend": "nebius",
            "budget_policy": "none",
        }

    headers = {"Authorization": f"Bearer {raw}"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as ac:
            created = await ac.post("/api/v1/batches", headers=headers, json=payload(isolation="guest"))
            rejected = await ac.post("/api/v1/batches/dry-run", headers=headers, json=payload(isolation="guest"))
            accepted = await ac.post("/api/v1/batches/dry-run", headers=headers, json=payload(isolation="auto"))

        assert created.status_code == 400, created.text
        detail = created.json()["detail"]
        assert detail["reason"] == "nebius_task_incompatible"
        reasons = detail["rejection_reasons"][task_id]
        assert "isolation_guest_response_only" in reasons and len(reasons) >= 2

        assert rejected.status_code == 200, rejected.text
        assert rejected.json()["accepted"] is False
        assert rejected.json()["rejection_reasons"] == {task_id: reasons}
        assert rejected.json()["rejected_task_ids"] == [task_id]

        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["accepted"] is True
        assert accepted.json()["tasks"] == [{"task_id": task_id, "trials": [{
            "harness": {"name": "direct-completion", "version": None},
            "network_policy": {"kind": "gateway-only"},
            "verification": "separate",
            "isolation": "container",
        }]}]
        with sl() as s:
            assert s.scalar(select(func.count()).select_from(Batch)) == batches_before
    finally:
        with sl() as s:
            s.execute(delete(Task).where(Task.id == task_id))
            s.execute(delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target_id))
            s.execute(delete(ServiceExecutionClass).where(
                ServiceExecutionClass.id == NEBIUS_CPU_EXECUTION_CLASS_V1.class_id))
            s.commit()
        sync_engine.dispose()
