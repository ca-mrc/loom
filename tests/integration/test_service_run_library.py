"""Org-wide Run Library and shared artifact behavior (#336)."""

from __future__ import annotations

import base64
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
from sqlalchemy import create_engine, delete, event, func, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    Batch,
    Benchmark,
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    LlmCall,
    ProviderConnection,
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    User,
)
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.routes import run_library as run_library_routes
from tests.integration.test_service_batch_resource_requests import _NEBIUS_DEFAULT_REQUESTS
from tests.integration.test_service_batches_crud import (
    _automatic_service_execution_task_config,
    _service_execution_runtime_profile,
)
from tests.support.execution_image_admission import signed_image_admission_bundle


@pytest.fixture
async def run_library_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    shared_minio: MinioContainer,
) -> AsyncIterator[dict[str, object]]:
    cfg = shared_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)

    monkeypatch.setenv("LOOM_ENV", "development")
    profile = _service_execution_runtime_profile()
    controller = "registry.example/controller@sha256:" + "9" * 64
    profile = ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"),
        "agent_image_ref": controller,
        "image_admission": signed_image_admission_bundle((
            profile.task_image_ref, profile.runtime_image_ref, controller,
        )).model_dump(mode="json"),
        "default_task_resource_requests": _NEBIUS_DEFAULT_REQUESTS,
    })
    settings = LoomServiceSettings(_env_file=None).model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _req: httpx.Response(404)),
        base_url=str(settings.control_plane_url),
    )

    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    legacy_raw_b = f"loom_team_{uuid4().hex}"
    user_a = uuid4()
    user_b = uuid4()
    task_id = f"local/task-{uuid4().hex[:8]}"
    batch_shared = uuid4()
    batch_default = uuid4()
    batch_private = uuid4()
    batch_running = uuid4()
    batch_b = uuid4()
    trial_shared = uuid4()
    trial_private = uuid4()
    conn_a = uuid4()
    conn_b = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    safe_key = f"{team_a}/{trial_shared}/main/report.json"
    private_key = f"{team_a}/{trial_private}/main/report.json"
    blocked_key = f"{team_a}/{trial_shared}/main/debug.log"
    safe_artifact_id = uuid4()
    blocked_artifact_id = uuid4()
    parent_artifact_id = uuid4()

    target_id = "library-" + uuid4().hex
    execution_class = NEBIUS_CPU_EXECUTION_CLASS_V1
    with sl() as s:
        s.add(ServiceExecutionClass(
            id=execution_class.class_id, schema_version=execution_class.schema_version,
            spec_json=execution_class.model_dump(mode="json"),
            spec_sha256=canonical_digest(execution_class.model_dump(mode="json")), enabled=True,
        ))
        s.add(ServiceExecutionTarget(
            id=target_id, logical_pool_id="nebius-cpu", execution_class_id=execution_class.class_id,
            schema_version="loom.execution-target.v1", spec_json={"health_stale_after_seconds": 60},
            spec_sha256="sha256:" + "e" * 64, environment="development", provider="nebius",
            region="eu-north1", failure_domain="eu-north1-a", data_residency="eu",
            desired_state="active", observed_state="ready", health_status="healthy",
            health_observed_at=now,
        ))
        s.execute(insert(Team).values(id=team_a, name="Alpha Research"))
        s.execute(insert(Team).values(id=team_b, name="Beta Apps"))
        s.execute(
            insert(User).values(
                id=user_a,
                username="AlphaOwner",
                username_normalized="alphaowner",
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(
            insert(User).values(
                id=user_b,
                username="BetaOwner",
                username_normalized="betaowner",
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(insert(TeamQuota).values(team_id=team_a))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        for raw, team_id, user_id in (
            (raw_a, team_a, user_a),
            (raw_b, team_b, user_b),
        ):
            s.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode()).digest(),
                    type="team",
                    scopes=["read:own", "submit", "providers:manage"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=now,
                    expires_at=None,
                )
            )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw_b.encode()).digest(),
                type="team",
                scopes=["read:own", "submit", "providers:manage"],
                team_id=team_b,
                issued_at=now,
                expires_at=None,
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="1" * 64,
                config=_automatic_service_execution_task_config(task_id),
                source="local",
                source_provenance={"service_execution_input": {
                    "schema_version": "loom.service-execution-input.v1",
                    "manifest_uri": "s3://artifacts/task-inputs/task.json",
                    "manifest_sha256": "sha256:" + "d" * 64, "file_count": 3, "total_bytes": 4096,
                }},
            )
        )
        for conn_id, team_id, name in (
            (conn_a, team_a, "alpha-provider"),
            (conn_b, team_b, "beta-provider"),
        ):
            s.execute(
                insert(ProviderConnection).values(
                    id=conn_id,
                    team_id=team_id,
                    provider_type="openai-compatible",
                    display_name=name,
                    base_url="https://api.example.test/v1",
                    upstream_host="api.example.test",
                    encrypted_api_key_ref=f"env:{name.upper().replace('-', '_')}",
                    created_by="test",
                    status="valid",
                )
            )
        common_config = {
            "agent_name": "litellm",
            "agent_model": {
                "provider": "openai",
                "name": "gpt-4o-mini",
            },
        }
        for batch_id, team_id, name, state, visibility, share_status in (
            (batch_shared, team_a, "shared alpha run", "finished", "org", "shared"),
            (batch_private, team_a, "private alpha run", "finished", "private", "shared"),
            (batch_running, team_a, "running alpha run", "running", "org", "shared"),
            (batch_b, team_b, "beta team run", "finished", "team", "pending_scan"),
        ):
            s.execute(
                insert(Batch).values(
                    id=batch_id,
                    team_id=team_id,
                    name=name,
                    description=None,
                    task_filter={"subset_kind": "explicit", "task_ids": [task_id]},
                    trial_config=common_config,
                    state=state,
                    result_status="succeeded" if state == "finished" else None,
                    created_at=now,
                    finished_at=now if state == "finished" else None,
                    created_by_token_prefix="test:web",
                    expected_trial_count=1,
                    n_per_task=1,
                    backend="nebius",
                    combinations=[],
                    provider_connection_id=conn_a if team_id == team_a else conn_b,
                    provider_model_id="gpt-4o-mini",
                    visibility=visibility,
                    share_status=share_status,
                )
            )
        s.execute(
            insert(Batch).values(
                id=batch_default,
                team_id=team_a,
                name="default shared alpha run",
                description=None,
                task_filter={"subset_kind": "explicit", "task_ids": [task_id]},
                trial_config=common_config,
                state="finished",
                result_status="succeeded",
                created_at=now,
                finished_at=now,
                created_by_token_prefix="test:web",
                expected_trial_count=1,
                n_per_task=1,
                backend="nebius",
                combinations=[],
                provider_connection_id=conn_a,
                provider_model_id="gpt-4o-mini",
            )
        )
        s.execute(
            insert(Trial).values(
                id=trial_shared,
                task_id=task_id,
                team_id=team_a,
                batch_id=batch_shared,
                state="succeeded",
                config=common_config,
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                result={"aggregate_reward": 1.0, "cost_usd": 0.02},
                visibility="org",
                share_status="shared",
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": safe_key,
                            "size": 17,
                            "role": "report",
                            "share_status": "shared",
                        },
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": blocked_key,
                            "size": 21,
                            "role": "raw_diagnostics",
                            "share_status": "shared",
                            "blocked_reason": "legacy field is stale",
                        },
                    ],
                },
            )
        )
        s.execute(
            insert(Trial).values(
                id=trial_private,
                task_id=task_id,
                team_id=team_a,
                batch_id=batch_private,
                state="succeeded",
                config=common_config,
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                result={"aggregate_reward": 0.5, "cost_usd": 0.01},
                visibility="org",
                share_status="shared",
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": private_key,
                            "size": 17,
                            "role": "report",
                            "share_status": "shared",
                        },
                    ],
                },
            )
        )
        s.execute(
            insert(Artifact).values(
                id=safe_artifact_id,
                artifact_type="metric_table",
                artifact_schema_version="1.0",
                name="Alpha aggregate metrics",
                team_id=team_a,
                batch_id=batch_shared,
                trial_id=trial_shared,
                created_by={
                    "kind": "trial",
                    "batch_id": str(batch_shared),
                    "trial_id": str(trial_shared),
                },
                content_hash="sha256:" + ("a" * 64),
                storage={
                    "backend": "object_store",
                    "bucket": settings.artifacts_bucket,
                    "key": safe_key,
                    "media_type": "application/json",
                    "size_bytes": 17,
                },
                visibility="org",
                share_status="shared",
                redaction_state="redacted",
                safety_state="safe",
                blocked_reason=None,
                retention={"class": "shared_reusable", "expires_at": None},
                provenance={
                    "batch_id": str(batch_shared),
                    "trial_id": str(trial_shared),
                    "source_trial_ids": [str(trial_shared)],
                    "agent": "litellm",
                    "model": "gpt-4o-mini",
                    "relation": "produced_from",
                },
                artifact_metadata={"metric_name": "aggregate_reward"},
            )
        )
        s.execute(
            insert(Artifact).values(
                id=parent_artifact_id,
                artifact_type="task_set",
                artifact_schema_version="1.0",
                name="Parent task set",
                team_id=team_a,
                created_by={"kind": "manual_import"},
                content_hash="sha256:" + ("d" * 64),
                storage={
                    "backend": "object_store",
                    "bucket": settings.artifacts_bucket,
                    "key": f"{team_a}/parents/task-set.json",
                    "media_type": "application/json",
                    "size_bytes": 12,
                },
                visibility="org",
                share_status="shared",
                redaction_state="redacted",
                safety_state="safe",
                blocked_reason=None,
                retention={"class": "shared_reusable", "expires_at": None},
                provenance={},
                artifact_metadata={"task_count": 1},
            )
        )
        s.execute(
            insert(ArtifactLineageEdge).values(
                child_artifact_id=safe_artifact_id,
                parent_artifact_id=parent_artifact_id,
                relation="produced_from",
                edge_metadata={"source": "test"},
            )
        )
        s.execute(
            insert(Artifact).values(
                id=blocked_artifact_id,
                artifact_type="debug_bundle",
                artifact_schema_version="1.0",
                name="Unsafe debug bundle",
                team_id=team_a,
                batch_id=batch_shared,
                trial_id=trial_shared,
                created_by={
                    "kind": "trial",
                    "batch_id": str(batch_shared),
                    "trial_id": str(trial_shared),
                },
                content_hash="sha256:" + ("b" * 64),
                storage={
                    "backend": "object_store",
                    "bucket": settings.artifacts_bucket,
                    "key": blocked_key,
                    "media_type": "text/plain",
                    "size_bytes": 21,
                },
                visibility="org",
                share_status="shared",
                redaction_state="blocked",
                safety_state="unsafe",
                blocked_reason="secret-like content detected",
                retention={"class": "owner_only_debug", "expires_at": None},
                provenance={
                    "batch_id": str(batch_shared),
                    "trial_id": str(trial_shared),
                    "source_trial_ids": [str(trial_shared)],
                    "relation": "produced_from",
                },
                artifact_metadata={"debug_kind": "raw_log"},
            )
        )
        s.commit()

    existing = {bucket["Name"] for bucket in app.state.minio_client.list_buckets()["Buckets"]}
    if settings.artifacts_bucket not in existing:
        app.state.minio_client.create_bucket(Bucket=settings.artifacts_bucket)
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=safe_key,
        Body=b'{"ok": true}\n',
    )
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=blocked_key,
        Body=b"sk-test-should-not-leak",
    )

    try:
        yield {
            "app": app,
            "raw_a": raw_a,
            "raw_b": raw_b,
            "legacy_raw_b": legacy_raw_b,
            "team_a": team_a,
            "team_b": team_b,
            "user_b": user_b,
            "batch_shared": batch_shared,
            "batch_default": batch_default,
            "batch_private": batch_private,
            "batch_running": batch_running,
            "batch_b": batch_b,
            "trial_shared": trial_shared,
            "trial_private": trial_private,
            "conn_a": conn_a,
            "conn_b": conn_b,
            "task_id": task_id,
            "safe_key": safe_key,
            "private_key": private_key,
            "blocked_key": blocked_key,
            "safe_artifact_id": safe_artifact_id,
            "blocked_artifact_id": blocked_artifact_id,
            "parent_artifact_id": parent_artifact_id,
            "postgres_url": postgres_url,
        }
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target_id))
            s.execute(delete(ServiceExecutionClass).where(ServiceExecutionClass.id == execution_class.class_id))
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.id.in_([user_a, user_b])))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _seed_cursor_batches(
    *,
    postgres_url: str,
    team_id: UUID,
    task_id: str,
    provider_connection_id: UUID,
) -> tuple[list[str], str]:
    tied_at = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    visible: list[tuple[UUID, datetime]] = []
    batch_rows: list[dict[str, object]] = []
    trial_rows: list[dict[str, object]] = []
    artifact_rows: list[dict[str, object]] = []

    for index in range(53):
        batch_id = UUID(int=10_000 + index)
        trial_id = UUID(int=20_000 + index)
        artifact_id = UUID(int=30_000 + index)
        created_at = tied_at if index < 27 else tied_at - timedelta(minutes=index - 26)
        visible.append((batch_id, created_at))
        batch_rows.append(
            {
                "id": batch_id,
                "team_id": team_id,
                "name": f"cursor completeness {index:02d}",
                "description": "issue 774 deterministic traversal fixture",
                "purpose": "evaluation",
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": [task_id],
                },
                "trial_config": {},
                "state": "finished",
                "result_status": "succeeded",
                "created_at": created_at,
                "finished_at": created_at,
                "created_by_token_prefix": "test:774",
                "expected_trial_count": 1,
                "n_per_task": 1,
                "backend": "nebius",
                "combinations": [],
                "provider_connection_id": provider_connection_id,
                "provider_model_id": "gpt-4o-mini",
                "visibility": "org",
                "share_status": "shared",
            }
        )
        trial_rows.append(
            {
                "id": trial_id,
                "team_id": team_id,
                "batch_id": batch_id,
                "task_id": task_id,
                "config": {},
                "requires_caps": {},
                "state": "succeeded",
                "submitted_at": created_at,
                "started_at": created_at,
                "finished_at": created_at,
                "result": {"aggregate_reward": 1.0},
                "visibility": "org",
                "share_status": "shared",
            }
        )
        artifact_rows.append(
            {
                "id": artifact_id,
                "artifact_type": "training_data_export",
                "artifact_schema_version": "1.0",
                "name": f"cursor export {index:02d}",
                "team_id": team_id,
                "batch_id": batch_id,
                "trial_id": trial_id,
                "created_by": {
                    "kind": "trial",
                    "batch_id": str(batch_id),
                    "trial_id": str(trial_id),
                },
                "content_hash": f"sha256:{index:064x}",
                "storage": {
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": f"cursor-774/{trial_id}/export.jsonl",
                    "media_type": "application/x-ndjson",
                    "size_bytes": 1,
                },
                "visibility": "org",
                "share_status": "shared",
                "redaction_state": "redacted",
                "safety_state": "safe",
                "retention": {"class": "shared_reusable"},
                "provenance": {
                    "batch_id": str(batch_id),
                    "trial_id": str(trial_id),
                    "source_trial_ids": [str(trial_id)],
                    "relation": "produced_from",
                },
                "artifact_metadata": {"fixture_index": index},
                "created_at": created_at,
            }
        )

    private_batch_id = UUID(int=90_000)
    private_trial_id = UUID(int=90_001)
    private_artifact_id = UUID(int=90_002)
    batch_rows.append(
        {
            "id": private_batch_id,
            "team_id": team_id,
            "name": "cursor completeness private",
            "description": "must remain outside cross-team traversal",
            "purpose": "evaluation",
            "task_filter": {"subset_kind": "explicit", "task_ids": [task_id]},
            "trial_config": {},
            "state": "finished",
            "result_status": "succeeded",
            "created_at": tied_at,
            "finished_at": tied_at,
            "created_by_token_prefix": "test:774",
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "nebius",
            "combinations": [],
            "provider_connection_id": provider_connection_id,
            "provider_model_id": "gpt-4o-mini",
            "visibility": "private",
            "share_status": "shared",
        }
    )
    trial_rows.append(
        {
            "id": private_trial_id,
            "team_id": team_id,
            "batch_id": private_batch_id,
            "task_id": task_id,
            "config": {},
            "requires_caps": {},
            "state": "succeeded",
            "submitted_at": tied_at,
            "started_at": tied_at,
            "finished_at": tied_at,
            "result": {"aggregate_reward": 1.0},
            "visibility": "private",
            "share_status": "shared",
        }
    )
    artifact_rows.append(
        {
            "id": private_artifact_id,
            "artifact_type": "training_data_export",
            "artifact_schema_version": "1.0",
            "name": "private cursor export",
            "team_id": team_id,
            "batch_id": private_batch_id,
            "trial_id": private_trial_id,
            "created_by": {"kind": "trial", "trial_id": str(private_trial_id)},
            "content_hash": "sha256:" + ("f" * 64),
            "storage": {
                "backend": "object_store",
                "bucket": "artifacts",
                "key": f"cursor-774/{private_trial_id}/export.jsonl",
                "media_type": "application/x-ndjson",
                "size_bytes": 1,
            },
            "visibility": "private",
            "share_status": "shared",
            "redaction_state": "redacted",
            "safety_state": "safe",
            "retention": {"class": "owner_only"},
            "provenance": {
                "batch_id": str(private_batch_id),
                "trial_id": str(private_trial_id),
                "relation": "produced_from",
            },
            "artifact_metadata": {},
            "created_at": tied_at,
        }
    )

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Batch), batch_rows)
        conn.execute(insert(Trial), trial_rows)
        conn.execute(insert(Artifact), artifact_rows)
    sync_engine.dispose()

    expected_ids = [
        str(batch_id)
        for batch_id, _created_at in sorted(
            visible,
            key=lambda item: (item[1], item[0].int),
            reverse=True,
        )
    ]
    return expected_ids, str(private_batch_id)


async def _walk_run_library_pages(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    params: dict[str, str],
) -> tuple[list[str], list[int], list[str | None]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    ids: list[str] = []
    page_sizes: list[int] = []
    returned_cursors: list[str | None] = []

    while True:
        request_params = dict(params)
        if cursor is not None:
            request_params["cursor"] = cursor
        response = await client.get(
            "/api/v1/run-library/batches",
            params=request_params,
            headers=headers,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        page_ids = [item["id"] for item in body["items"]]
        ids.extend(page_ids)
        page_sizes.append(len(page_ids))
        returned_cursors.append(body["next_cursor"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
        assert cursor not in seen_cursors
        seen_cursors.add(cursor)
        assert len(returned_cursors) < 10

    return ids, page_sizes, returned_cursors


async def test_run_library_defaults_to_my_team_and_all_teams_shows_shared_owner(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    batch_default = run_library_setup["batch_default"]
    batch_private = run_library_setup["batch_private"]
    batch_running = run_library_setup["batch_running"]
    batch_b = run_library_setup["batch_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        mine = await ac.get(
            "/api/v1/run-library/batches",
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        all_teams = await ac.get(
            "/api/v1/run-library/batches?scope=all",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert mine.status_code == 200, mine.text
    assert [item["id"] for item in mine.json()["items"]] == [str(batch_b)]

    assert all_teams.status_code == 200, all_teams.text
    ids = {item["id"] for item in all_teams.json()["items"]}
    assert str(batch_b) in ids
    assert str(batch_shared) in ids
    assert str(batch_default) in ids
    assert str(batch_private) not in ids
    assert str(batch_running) not in ids

    shared = next(item for item in all_teams.json()["items"] if item["id"] == str(batch_shared))
    assert shared["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    assert shared["visibility"] == "org"
    assert shared["share_status"] == "shared"
    assert shared["artifact_summary"]["reports"] == 1
    assert shared["artifact_summary"]["raw_diagnostics"] == 1

    default_shared = next(
        item for item in all_teams.json()["items"] if item["id"] == str(batch_default)
    )
    assert default_shared["visibility"] == "org"
    assert default_shared["share_status"] == "shared"


async def test_run_library_cursor_walk_has_no_gaps_or_duplicates(
    run_library_setup: dict[str, object],
) -> None:
    expected_ids, private_id = _seed_cursor_batches(
        postgres_url=str(run_library_setup["postgres_url"]),
        team_id=run_library_setup["team_a"],
        task_id=str(run_library_setup["task_id"]),
        provider_connection_id=run_library_setup["conn_a"],
    )
    transport = httpx.ASGITransport(app=run_library_setup["app"])
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        ids, page_sizes, cursors = await _walk_run_library_pages(
            client,
            headers={"Authorization": f"Bearer {run_library_setup['raw_b']}"},
            params={
                "scope": "all",
                "q": "cursor completeness",
                "limit": "17",
            },
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(ids) == 53
    assert len(set(ids)) == 53
    assert private_id not in ids


async def test_artifact_filtered_cursor_walk_is_complete_and_bounded(
    run_library_setup: dict[str, object],
) -> None:
    expected_ids, private_id = _seed_cursor_batches(
        postgres_url=str(run_library_setup["postgres_url"]),
        team_id=run_library_setup["team_a"],
        task_id=str(run_library_setup["task_id"]),
        provider_connection_id=run_library_setup["conn_a"],
    )
    transport = httpx.ASGITransport(app=run_library_setup["app"])
    headers = {"Authorization": f"Bearer {run_library_setup['raw_b']}"}
    params = {
        "scope": "all",
        "q": "cursor completeness",
        "artifact_type": "training_data_export",
        "limit": "17",
    }
    statements: list[str] = []

    def capture_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(" ".join(statement.lower().split()))

    engine = run_library_setup["app"].state.session_factory.kw["bind"]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
        try:
            first = await client.get(
                "/api/v1/run-library/batches",
                params=params,
                headers=headers,
            )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)
        assert first.status_code == 200, first.text
        assert len(first.json()["items"]) == 17
        assert first.json()["next_cursor"] is not None
        ids, page_sizes, cursors = await _walk_run_library_pages(
            client,
            headers=headers,
            params=params,
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(set(ids)) == 53
    assert private_id not in ids
    candidate_queries = [
        statement for statement in statements if "from batches join teams" in statement
    ]
    assert len(candidate_queries) == 1
    assert "exists (select artifacts.id" in candidate_queries[0]


async def test_run_library_rejects_naive_timestamp_cursor(
    run_library_setup: dict[str, object],
) -> None:
    body = json.dumps(
        {
            "t": "2026-07-10T12:00:00",
            "i": str(uuid4()),
        }
    ).encode()
    cursor = base64.urlsafe_b64encode(body).decode().rstrip("=")
    transport = httpx.ASGITransport(app=run_library_setup["app"])

    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        response = await client.get(
            "/api/v1/run-library/batches",
            params={"scope": "all", "cursor": cursor},
            headers={"Authorization": f"Bearer {run_library_setup['raw_b']}"},
        )

    assert response.status_code == 400
    assert "timezone offset" in response.json()["detail"]


async def test_run_library_batch_list_does_not_load_trials_without_artifact_filters(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]

    original_batch_trials = run_library_routes._batch_trials
    calls: list[UUID] = []

    async def counting_batch_trials(session: object, batch_id: UUID) -> list[Trial]:
        calls.append(batch_id)
        return await original_batch_trials(session, batch_id)

    monkeypatch.setattr(
        run_library_routes,
        "_batch_trials",
        counting_batch_trials,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        all_teams = await ac.get(
            "/api/v1/run-library/batches?scope=all",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert all_teams.status_code == 200, all_teams.text
    ids = {item["id"] for item in all_teams.json()["items"]}
    assert str(batch_shared) in ids
    assert calls == []


async def test_run_library_batch_list_summarizes_only_visible_attributed_artifacts(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = run_library_setup["app"]
    raw_a = run_library_setup["raw_a"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    trial_shared = run_library_setup["trial_shared"]
    team_a = run_library_setup["team_a"]
    postgres_url = run_library_setup["postgres_url"]

    monkeypatch.setattr(
        run_library_routes,
        "_BATCH_LIST_ARTIFACT_SUMMARY_PER_BATCH_LIMIT",
        3,
        raising=False,
    )

    now = datetime.now(UTC)
    artifact_rows: list[dict[str, object]] = []
    for idx in range(4):
        artifact_rows.append(
            {
                "id": uuid4(),
                "artifact_type": "training_data_export",
                "artifact_schema_version": "1.0",
                "name": f"Private batch export {idx}",
                "team_id": team_a,
                "batch_id": batch_shared,
                "trial_id": None,
                "created_by": {"kind": "batch", "batch_id": str(batch_shared)},
                "content_hash": f"sha256:{idx + 10:064x}",
                "storage": {
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": f"private-batch/{idx}.jsonl",
                    "media_type": "application/jsonl",
                    "size_bytes": 1024,
                },
                "visibility": "private",
                "share_status": "shared",
                "redaction_state": "redacted",
                "safety_state": "safe",
                "retention": {"class": "owner_only", "expires_at": None},
                "provenance": {
                    "batch_id": str(batch_shared),
                    "relation": "produced_from",
                },
                "artifact_metadata": {"shard": idx},
                "created_at": now,
            }
        )
    artifact_rows.extend(
        [
            {
                "id": uuid4(),
                "artifact_type": "task_set",
                "artifact_schema_version": "1.0",
                "name": "Private legacy-attributed task set",
                "team_id": team_a,
                "batch_id": None,
                "trial_id": trial_shared,
                "created_by": {"kind": "trial", "trial_id": str(trial_shared)},
                "content_hash": f"sha256:{20:064x}",
                "storage": {
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": "private-trial/task-set.json",
                    "media_type": "application/json",
                    "size_bytes": 512,
                },
                "visibility": "private",
                "share_status": "shared",
                "redaction_state": "redacted",
                "safety_state": "safe",
                "retention": {"class": "owner_only", "expires_at": None},
                "provenance": {
                    "trial_id": str(trial_shared),
                    "relation": "produced_from",
                },
                "artifact_metadata": {},
                "created_at": now,
            },
            {
                "id": uuid4(),
                "artifact_type": "trajectory",
                "artifact_schema_version": "1.0",
                "name": "Shared legacy-attributed trajectory",
                "team_id": team_a,
                "batch_id": None,
                "trial_id": trial_shared,
                "created_by": {"kind": "trial", "trial_id": str(trial_shared)},
                "content_hash": f"sha256:{21:064x}",
                "storage": {
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": "shared-trial/trajectory.jsonl",
                    "media_type": "application/jsonl",
                    "size_bytes": 512,
                },
                "visibility": "org",
                "share_status": "shared",
                "redaction_state": "redacted",
                "safety_state": "safe",
                "retention": {"class": "shared_reusable", "expires_at": None},
                "provenance": {
                    "trial_id": str(trial_shared),
                    "relation": "produced_from",
                },
                "artifact_metadata": {},
                "created_at": now,
            },
        ]
    )
    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(insert(Artifact), artifact_rows)
    sync_engine.dispose()

    statements: list[str] = []

    def capture_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(" ".join(statement.lower().split()))

    engine = app.state.session_factory.kw["bind"]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
        try:
            cross_team = await ac.get(
                "/api/v1/run-library/batches?scope=all",
                headers={"Authorization": f"Bearer {raw_b}"},
            )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)
        owner = await ac.get(
            "/api/v1/run-library/batches",
            headers={"Authorization": f"Bearer {raw_a}"},
        )

    assert cross_team.status_code == 200, cross_team.text
    shared = next(item for item in cross_team.json()["items"] if item["id"] == str(batch_shared))
    assert shared["artifact_summary"] == {
        "reports": 1,
        "trajectories": 1,
        "reusable_outputs": 0,
        "logs_diagnostics": 0,
        "raw_diagnostics": 1,
    }
    assert shared["artifact_summary_truncated"] is False
    summary_queries = [
        statement for statement in statements if "visible_batch_artifacts" in statement
    ]
    assert len(summary_queries) == 1
    assert "join lateral" in summary_queries[0]

    assert owner.status_code == 200, owner.text
    owned = next(item for item in owner.json()["items"] if item["id"] == str(batch_shared))
    assert sum(owned["artifact_summary"].values()) == 3
    assert owned["artifact_summary_truncated"] is True

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Artifact).values(
                id=uuid4(),
                artifact_type="task_split",
                artifact_schema_version="1.0",
                name="Fourth shared batch artifact",
                team_id=team_a,
                batch_id=batch_shared,
                trial_id=None,
                created_by={"kind": "batch", "batch_id": str(batch_shared)},
                content_hash=f"sha256:{22:064x}",
                storage={
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": "shared-batch/task-split.json",
                    "media_type": "application/json",
                    "size_bytes": 256,
                },
                visibility="org",
                share_status="shared",
                redaction_state="redacted",
                safety_state="safe",
                retention={"class": "shared_reusable", "expires_at": None},
                provenance={
                    "batch_id": str(batch_shared),
                    "relation": "produced_from",
                },
                artifact_metadata={},
                created_at=now + timedelta(seconds=1),
            )
        )
    sync_engine.dispose()

    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        cross_team_truncated = await ac.get(
            "/api/v1/run-library/batches?scope=all",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert cross_team_truncated.status_code == 200, cross_team_truncated.text
    shared_truncated = next(
        item for item in cross_team_truncated.json()["items"] if item["id"] == str(batch_shared)
    )
    assert sum(shared_truncated["artifact_summary"].values()) == 3
    assert shared_truncated["artifact_summary_truncated"] is True


async def test_run_library_filters_by_structured_batch_fields(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    conn_a = run_library_setup["conn_a"]
    postgres_url = run_library_setup["postgres_url"]
    wanted_id = uuid4()
    wrong_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch),
            [
                {
                    "id": wanted_id,
                    "team_id": team_a,
                    "name": "skilllearnbench codex qwen generated",
                    "description": "structured library needle",
                    "purpose": "evaluation",
                    "task_filter": {
                        "subset_kind": "random_n",
                        "benchmark_ids": ["skilllearnbench"],
                        "n": 5,
                    },
                    "trial_config": {},
                    "state": "finished",
                    "result_status": "succeeded",
                    "created_at": now,
                    "finished_at": now,
                    "created_by_token_prefix": "test:web",
                    "expected_trial_count": 1,
                    "backend": "nebius",
                    "combinations": [
                        {
                            "agent_name": "codex",
                            "agent_model": {
                                "provider": "openai",
                                "name": "qwen3.6-35b-a3b",
                            },
                            "n_per_task": 1,
                        }
                    ],
                    "provider_connection_id": conn_a,
                    "provider_model_id": "qwen3.6-35b-a3b",
                    "visibility": "org",
                    "share_status": "shared",
                },
                {
                    "id": wrong_id,
                    "team_id": team_a,
                    "name": "skilllearnbench codex claude generated",
                    "description": "structured library needle",
                    "purpose": "evaluation",
                    "task_filter": {
                        "subset_kind": "random_n",
                        "benchmark_ids": ["skilllearnbench"],
                        "n": 5,
                    },
                    "trial_config": {},
                    "state": "finished",
                    "result_status": "succeeded",
                    "created_at": now,
                    "finished_at": now,
                    "created_by_token_prefix": "test:web",
                    "expected_trial_count": 1,
                    "backend": "nebius",
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
                    "provider_connection_id": conn_a,
                    "provider_model_id": "claude-sonnet-4-6",
                    "visibility": "org",
                    "share_status": "shared",
                },
            ],
        )
    sync_engine.dispose()

    params = (
        "scope=all&q=needle&benchmark_id=skilllearnbench&agent_name=codex"
        "&model_provider=openai&model_name=qwen3.6-35b-a3b"
        f"&provider_connection_id={conn_a}&provider_model_id=qwen3.6-35b-a3b"
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/run-library/batches?{params}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert r.status_code == 200, r.text
    assert [item["id"] for item in r.json()["items"]] == [str(wanted_id)]


async def test_cross_team_shared_artifact_downloads_and_blocked_denials(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    private_trial_id = run_library_setup["trial_private"]
    safe_key = run_library_setup["safe_key"]
    private_key = run_library_setup["private_key"]
    blocked_key = run_library_setup["blocked_key"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        direct = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": safe_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        shared = await ac.get(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/download",
            params={"key": safe_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.get(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/download",
            params={"key": blocked_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        private_parent = await ac.get(
            f"/api/v1/run-library/trials/{private_trial_id}/artifacts/download",
            params={"key": private_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert direct.status_code == 403
    assert shared.status_code == 200
    assert "location" not in shared.headers
    assert shared.content == b'{"ok": true}\n'
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"
    assert private_parent.status_code == 403


async def test_typed_registry_filters_detail_inventory_and_metadata_export(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    raw_a = run_library_setup["raw_a"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    safe_artifact_id = run_library_setup["safe_artifact_id"]
    blocked_artifact_id = run_library_setup["blocked_artifact_id"]
    parent_artifact_id = run_library_setup["parent_artifact_id"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        batches = await ac.get(
            "/api/v1/run-library/batches",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "owner_team_id": str(team_a),
                "source_trial_id": str(trial_id),
                "safety_state": "safe",
                "provenance_relation": "produced_from",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        artifacts = await ac.get(
            "/api/v1/run-library/artifacts",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "owner_team_id": str(team_a),
                "source_trial_id": str(trial_id),
                "safety_state": "safe",
                "provenance_relation": "produced_from",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        exported = await ac.get(
            "/api/v1/run-library/artifacts/export",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "format": "jsonl",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        owner_detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_a}"},
        )

    assert batches.status_code == 200, batches.text
    assert [item["id"] for item in batches.json()["items"]] == [str(batch_shared)]

    assert artifacts.status_code == 200, artifacts.text
    artifact_items = artifacts.json()["items"]
    assert [item["id"] for item in artifact_items] == [str(safe_artifact_id)]
    artifact = artifact_items[0]
    assert artifact["artifact_type"] == "metric_table"
    assert artifact["artifact_type_label"] == "Metric table"
    assert artifact["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    assert artifact["source"] == {
        "kind": "trial",
        "batch_id": str(batch_shared),
        "trial_id": str(trial_id),
    }
    assert artifact["storage"]["key"] == safe_key
    assert artifact["safety_state"] == "safe"
    assert artifact["redaction_state"] == "redacted"
    assert artifact["content_hash"] == "sha256:" + ("a" * 64)
    assert artifact["metadata"] == {"metric_name": "aggregate_reward"}
    assert artifact["parents"] == [
        {
            "artifact_id": str(parent_artifact_id),
            "relation": "produced_from",
            "metadata": {"source": "test"},
        }
    ]
    assert artifact["download_url"].endswith(
        f"/api/v1/run-library/trials/{trial_id}/artifacts/download?key="
        f"{safe_key.replace('/', '%2F')}",
    )

    assert detail.status_code == 200, detail.text
    detail_json = detail.json()
    assert detail_json["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    inventory = detail_json["artifact_inventory"]
    metric = next(item for item in inventory["reports"] if item["id"] == str(safe_artifact_id))
    assert metric["artifact_type_label"] == "Metric table"
    assert metric["owner_team"]["name"] == "Alpha Research"
    assert metric["source"]["trial_id"] == str(trial_id)
    assert metric["safety_state"] == "safe"
    unsafe = next(
        item for item in inventory["raw_diagnostics"] if item["id"] == str(blocked_artifact_id)
    )
    assert unsafe["artifact_type"] == "debug_bundle"
    assert unsafe["safety_state"] == "unsafe"
    assert unsafe["share_status"] == "shared"
    assert unsafe["blocked_reason"] == "secret-like content detected"
    assert unsafe["key"].startswith("redacted-artifact:")
    assert unsafe["storage"] is None
    assert unsafe["download_url"] is None
    assert unsafe["metadata"] == {}
    assert "debug.log" not in json.dumps(unsafe)

    assert owner_detail.status_code == 200, owner_detail.text
    owner_unsafe = next(
        item
        for item in owner_detail.json()["artifact_inventory"]["raw_diagnostics"]
        if item["id"] == str(blocked_artifact_id)
    )
    assert owner_unsafe["storage"]["key"].endswith("/debug.log")
    assert owner_unsafe["download_url"].endswith(
        f"/api/v1/run-library/trials/{trial_id}/artifacts/download?key="
        f"{run_library_setup['blocked_key'].replace('/', '%2F')}",
    )

    assert exported.status_code == 200, exported.text
    assert exported.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(line) for line in exported.text.splitlines()]
    assert [item["id"] for item in lines] == [str(safe_artifact_id)]
    assert str(blocked_artifact_id) not in exported.text
    assert "sk-test-should-not-leak" not in exported.text


async def test_run_library_batch_detail_includes_combination_summary(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    trial_shared = run_library_setup["trial_shared"]
    task_id = run_library_setup["task_id"]
    postgres_url = run_library_setup["postgres_url"]
    now = datetime.now(UTC)
    combo_failed_trial = uuid4()
    rerun_batch_id = uuid4()
    rerun_success_trial = uuid4()
    combinations = [
        {
            "agent_name": "opencode",
            "agent_model": {
                "provider": "openai",
                "name": "glm5.1-thinking",
            },
            "provider_model_id": "glm5.1-thinking",
            "n_per_task": 1,
            "label": "opencode / glm5.1-thinking",
        },
        {
            "agent_name": "codex",
            "agent_model": {
                "provider": "openai",
                "name": "qwen3.6-35b-a3b",
            },
            "provider_model_id": "qwen3.6-35b-a3b",
            "n_per_task": 1,
        },
        {
            "agent_name": "oracle",
            "agent_model": None,
            "n_per_task": 1,
            "label": "oracle / no model",
        },
    ]

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            update(Batch)
            .where(Batch.id == batch_shared)
            .values(
                expected_trial_count=3,
                combinations=combinations,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=combo_failed_trial,
                task_id=task_id,
                team_id=team_a,
                batch_id=batch_shared,
                state="failed",
                failure_reason="gateway_error",
                config={},
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                sample_idx=0,
                combination_idx=1,
                result=None,
                visibility="org",
                share_status="shared",
            )
        )
        conn.execute(
            insert(Batch).values(
                id=rerun_batch_id,
                team_id=team_a,
                name="shared alpha run failed-case rerun",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                result_status="succeeded",
                created_at=now,
                finished_at=now,
                created_by_token_prefix="test:web",
                expected_trial_count=1,
                backend="nebius",
                combinations=combinations,
                visibility="org",
                share_status="shared",
                rerun_of_batch_id=batch_shared,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=rerun_success_trial,
                task_id=task_id,
                team_id=team_a,
                batch_id=rerun_batch_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=now,
                started_at=now,
                finished_at=now,
                sample_idx=0,
                combination_idx=1,
                result={"aggregate_reward": 0.75},
                visibility="org",
                share_status="shared",
            )
        )
        conn.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_a,
                    "trial_id": trial_shared,
                    "step_id": "main",
                    "model": "openai/glm5.1-thinking",
                    "dialect": "openai",
                    "input_tokens": 4,
                    "output_tokens": 2,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.010000"),
                    "rate_card_hash": "facade:tokens-only:test",
                },
                {
                    "id": uuid4(),
                    "team_id": team_a,
                    "trial_id": combo_failed_trial,
                    "step_id": "main",
                    "model": "openai/qwen3.6-35b-a3b",
                    "dialect": "openai",
                    "input_tokens": 3,
                    "output_tokens": 1,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000000"),
                    "rate_card_hash": "facade:rate-card:missing:test",
                },
                {
                    "id": uuid4(),
                    "team_id": team_a,
                    "trial_id": rerun_success_trial,
                    "step_id": "main",
                    "model": "openai/qwen3.6-35b-a3b",
                    "dialect": "openai",
                    "input_tokens": 8,
                    "output_tokens": 2,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.010000"),
                    "rate_card_hash": "facade:tokens-only:test",
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    summary = body["combination_summary"]
    assert [row["combination_idx"] for row in summary] == [0, 1, 2]
    assert (
        summary[0]
        | {
            "label": "opencode / glm5.1-thinking",
            "trial_count": 1,
            "expected_trial_count": 1,
            "scored_trial_count": 1,
            "succeeded_count": 1,
            "failed_count": 0,
            "aggregate_reward": 1.0,
            "llm_calls_count": 1,
            "total_prompt_tokens": 4,
            "total_completion_tokens": 2,
        }
        == summary[0]
    )
    assert (
        summary[1]
        | {
            "label": "codex / qwen3.6-35b-a3b",
            "trial_count": 1,
            "expected_trial_count": 1,
            "scored_trial_count": 0,
            "failed_count": 1,
            "aggregate_reward": None,
            "llm_calls_count": 1,
            "total_prompt_tokens": 3,
            "total_completion_tokens": 1,
        }
        == summary[1]
    )
    assert (
        summary[2]
        | {
            "label": "oracle / no model",
            "trial_count": 0,
            "expected_trial_count": 1,
            "scored_trial_count": 0,
            "aggregate_reward": None,
            "llm_calls_count": 0,
        }
        == summary[2]
    )
    effective = body["effective_combination_summary"]
    assert effective[1]["trial_count"] == 1
    assert effective[1]["expected_trial_count"] == 1
    assert effective[1]["scored_trial_count"] == 1
    assert effective[1]["succeeded_count"] == 1
    assert effective[1]["failed_count"] == 0
    assert effective[1]["aggregate_reward"] == pytest.approx(0.75)
    assert effective[1]["llm_calls_count"] == 1
    assert effective[1]["total_prompt_tokens"] == 8
    assert effective[1]["total_completion_tokens"] == 2


async def test_run_library_batch_detail_default_does_not_materialize_llm_calls(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.routes import run_library as run_library_routes

    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]

    async def fail_if_full_rows_are_loaded(*_args: object, **_kwargs: object) -> list:
        raise AssertionError("default Run Library detail should not load LlmCall rows")

    monkeypatch.setattr(
        run_library_routes,
        "_llm_calls_for_trials",
        fail_if_full_rows_are_loaded,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert "artifact_inventory" in body
    assert "debug_evidence" not in body
    assert "diagnosis" not in body


async def test_run_library_batch_detail_default_does_not_select_trajectory_index(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    original_select = run_library_routes.select

    def fail_on_full_trial_row_select(*entities: object, **kwargs: object) -> object:
        if any(entity is Trial for entity in entities):
            raise AssertionError("default Run Library detail should not load full Trial rows")
        return original_select(*entities, **kwargs)

    monkeypatch.setattr(run_library_routes, "select", fail_on_full_trial_row_select)

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["owner_team"]["name"] == "Alpha Research"
    assert "artifact_inventory" in body


async def test_run_library_batch_detail_default_uses_bounded_artifact_preview(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]

    monkeypatch.setattr(
        run_library_routes,
        "_BATCH_DETAIL_ARTIFACT_PREVIEW_LIMIT",
        1,
        raising=False,
    )

    async def fail_if_full_artifacts_are_loaded(*_args: object, **_kwargs: object) -> dict:
        raise AssertionError("default Run Library detail should not load all typed artifacts")

    monkeypatch.setattr(
        run_library_routes,
        "_typed_artifacts_for_trials",
        fail_if_full_artifacts_are_loaded,
    )

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["artifact_summary_truncated"] is True
    assert body["artifact_inventory_truncated"] is True
    assert sum(len(items) for items in body["artifact_inventory"].values()) == 1


async def test_artifact_filter_is_applied_before_batch_limit(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    conn_a = run_library_setup["conn_a"]
    task_id = run_library_setup["task_id"]
    postgres_url = run_library_setup["postgres_url"]
    older_batch = uuid4()
    older_trial = uuid4()
    newer_rows = [(uuid4(), uuid4()) for _index in range(55)]
    metric_artifact = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        rows = [
            (older_batch, older_trial, now - timedelta(minutes=1), "older metric batch"),
            *[
                (batch_id, trial_id, now, f"newer nonmatching batch {index:02d}")
                for index, (batch_id, trial_id) in enumerate(newer_rows)
            ],
        ]
        for batch_id, trial_id, created, name in rows:
            conn.execute(
                insert(Batch).values(
                    id=batch_id,
                    team_id=team_a,
                    name=name,
                    description=None,
                    task_filter={"subset_kind": "explicit", "task_ids": ["t"]},
                    trial_config={},
                    state="finished",
                    result_status="succeeded",
                    created_at=created,
                    finished_at=created,
                    created_by_token_prefix="test:web",
                    expected_trial_count=1,
                    backend="nebius",
                    combinations=[],
                    provider_connection_id=conn_a,
                    provider_model_id="gpt-4o-mini",
                    visibility="org",
                    share_status="shared",
                )
            )
            conn.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_a,
                    batch_id=batch_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="succeeded",
                    submitted_at=created,
                    started_at=created,
                    finished_at=created,
                    result={"aggregate_reward": 1.0},
                    visibility="org",
                    share_status="shared",
                )
            )
        conn.execute(
            insert(Artifact).values(
                id=metric_artifact,
                artifact_type="training_data_export",
                artifact_schema_version="1.0",
                name="older export",
                team_id=team_a,
                batch_id=older_batch,
                trial_id=older_trial,
                created_by={"kind": "trial", "trial_id": str(older_trial)},
                content_hash="sha256:" + ("c" * 64),
                storage={
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": f"{team_a}/{older_trial}/main/export.jsonl",
                    "media_type": "application/json",
                    "size_bytes": 2,
                },
                visibility="org",
                share_status="shared",
                redaction_state="redacted",
                safety_state="safe",
                retention={"class": "shared_reusable"},
                provenance={
                    "batch_id": str(older_batch),
                    "trial_id": str(older_trial),
                    "relation": "produced_from",
                },
                artifact_metadata={},
                created_at=now,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/run-library/batches",
            params={
                "scope": "all",
                "artifact_type": "training_data_export",
                "limit": "1",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )

        statements: list[str] = []

        def capture_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            statements.append(" ".join(statement.lower().split()))

        engine = app.state.session_factory.kw["bind"]
        event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
        try:
            missing = await ac.get(
                "/api/v1/run-library/batches",
                params={
                    "scope": "all",
                    "artifact_type": "artifact-type-that-does-not-exist",
                    "limit": "1",
                },
                headers={"Authorization": f"Bearer {raw_b}"},
            )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    assert r.status_code == 200, r.text
    assert [item["id"] for item in r.json()["items"]] == [str(older_batch)]
    assert r.json()["next_cursor"] is None
    assert missing.status_code == 200, missing.text
    assert missing.json() == {"items": [], "next_cursor": None}
    candidate_queries = [
        statement for statement in statements if "from batches join teams" in statement
    ]
    assert len(candidate_queries) == 1
    assert not any(
        statement.startswith(("select trials.", "select artifacts.")) for statement in statements
    )


async def test_batch_level_artifact_filter_respects_metadata_visibility(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_a = run_library_setup["raw_a"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    postgres_url = run_library_setup["postgres_url"]
    now = datetime.now(UTC)

    artifact_rows = [
        {
            "id": uuid4(),
            "artifact_type": "trajectory_bundle",
            "artifact_schema_version": "1.0",
            "name": "owner-only batch trajectory",
            "team_id": team_a,
            "batch_id": batch_shared,
            "trial_id": None,
            "created_by": {"kind": "batch", "batch_id": str(batch_shared)},
            "content_hash": "sha256:" + ("7" * 64),
            "storage": {
                "backend": "object_store",
                "bucket": "artifacts",
                "key": f"{team_a}/{batch_shared}/trajectory.tar.zst",
                "media_type": "application/zstd",
                "size_bytes": 7,
            },
            "visibility": "private",
            "share_status": "shared",
            "redaction_state": "redacted",
            "safety_state": "safe",
            "retention": {"class": "owner_only"},
            "provenance": {
                "batch_id": str(batch_shared),
                "relation": "produced_from",
            },
            "artifact_metadata": {},
            "created_at": now,
        },
        {
            "id": uuid4(),
            "artifact_type": "task_split",
            "artifact_schema_version": "1.0",
            "name": "shared batch task split",
            "team_id": team_a,
            "batch_id": batch_shared,
            "trial_id": None,
            "created_by": {"kind": "batch", "batch_id": str(batch_shared)},
            "content_hash": "sha256:" + ("8" * 64),
            "storage": {
                "backend": "object_store",
                "bucket": "artifacts",
                "key": f"{team_a}/{batch_shared}/task-split.json",
                "media_type": "application/json",
                "size_bytes": 8,
            },
            "visibility": "org",
            "share_status": "shared",
            "redaction_state": "redacted",
            "safety_state": "safe",
            "retention": {"class": "shared_reusable"},
            "provenance": {
                "batch_id": str(batch_shared),
                "relation": "produced_from",
            },
            "artifact_metadata": {},
            "created_at": now,
        },
    ]
    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(insert(Artifact), artifact_rows)
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        hidden_cross_team = await ac.get(
            "/api/v1/run-library/batches",
            params={"scope": "all", "artifact_type": "trajectory_bundle"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        shared_cross_team = await ac.get(
            "/api/v1/run-library/batches",
            params={"scope": "all", "artifact_type": "task_split"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        owner_private = await ac.get(
            "/api/v1/run-library/batches",
            params={"artifact_type": "trajectory_bundle"},
            headers={"Authorization": f"Bearer {raw_a}"},
        )

    assert hidden_cross_team.status_code == 200, hidden_cross_team.text
    assert hidden_cross_team.json()["items"] == []
    assert shared_cross_team.status_code == 200, shared_cross_team.text
    assert [item["id"] for item in shared_cross_team.json()["items"]] == [str(batch_shared)]
    assert owner_private.status_code == 200, owner_private.text
    assert [item["id"] for item in owner_private.json()["items"]] == [str(batch_shared)]


async def test_batch_artifact_filter_preserves_legacy_trial_fallback(
    run_library_setup: dict[str, object],
) -> None:
    transport = httpx.ASGITransport(app=run_library_setup["app"])
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.get(
            "/api/v1/run-library/batches",
            params={"artifact_type": "atif_projection"},
            headers={"Authorization": f"Bearer {run_library_setup['raw_a']}"},
        )

    assert response.status_code == 200, response.text
    assert [item["id"] for item in response.json()["items"]] == [
        str(run_library_setup["batch_private"])
    ]


async def test_typed_registry_policy_controls_reuse_and_records_provenance(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    blocked_key = run_library_setup["blocked_key"]
    safe_artifact_id = run_library_setup["safe_artifact_id"]
    conn_a = run_library_setup["conn_a"]
    postgres_url = run_library_setup["postgres_url"]
    user_b = run_library_setup["user_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"provider_connection_id": str(run_library_setup["conn_b"]), "key": safe_key, "name": "beta reuse of typed metrics"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"provider_connection_id": str(run_library_setup["conn_b"]), "key": blocked_key, "name": "blocked typed debug reuse"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert reused.status_code == 201, reused.text
    body = reused.json()
    assert body["source_artifact"]["id"] == str(safe_artifact_id)
    assert body["source_artifact"]["artifact_type"] == "metric_table"
    assert body["source_artifact"]["content_hash"] == "sha256:" + ("a" * 64)
    assert body["source_artifact"]["artifact_schema_version"] == "1.0"
    provenance = body["source_provenance"][0]
    assert provenance["kind"] == "reused_artifact"
    assert provenance["relation"] == "reused_as_input"
    assert provenance["source_artifact_id"] == str(safe_artifact_id)
    assert provenance["source_artifact_key"] == safe_key
    assert provenance["source_content_hash"] == "sha256:" + ("a" * 64)
    assert provenance["source_artifact_schema_version"] == "1.0"
    assert "provider_connection_id" not in provenance

    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"

    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
        # The caller's own connection (#2054), never the source team's.
        assert row.provider_connection_id == run_library_setup["conn_b"]
        assert row.provider_connection_id != conn_a
        assert row.submitted_by_user_id == user_b
        assert row.source_provenance[0]["source_artifact_id"] == str(safe_artifact_id)
        assert row.resolved_task_ids == [run_library_setup["task_id"]]
        assert row.expected_trial_count == 1
    sync_engine.dispose()


@pytest.mark.parametrize("purpose", ["evaluation", "trajectory_generation"])
async def test_clone_config_uses_destination_provider_and_records_provenance(
    run_library_setup: dict[str, object], purpose: str,
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_a = run_library_setup["conn_a"]
    conn_b = run_library_setup["conn_b"]
    team_b = run_library_setup["team_b"]
    user_b = run_library_setup["user_b"]
    postgres_url = run_library_setup["postgres_url"]

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            update(Batch)
            .where(Batch.id == batch_shared)
            .values(required_worker_pools=["gpu-a", "gpu-b"], purpose=purpose),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "beta clone from alpha",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        mutation = await ac.post(
            f"/api/v1/batches/{batch_shared}/cancel",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert cloned.status_code == 201, cloned.text
    body = cloned.json()
    assert body["cloned_from_batch_id"] == str(batch_shared)
    assert body["provider_connection_id"] == str(conn_b)
    assert body["provider_connection_id"] != str(conn_a)
    assert body["source_provenance"][0]["source_batch_id"] == str(batch_shared)
    # #401 PR-3: source batch has no explicit `retry` → mismatch is null;
    # cloned trials will inherit current deployment defaults at submit.
    assert body["retry_default_snapshot_mismatch"] is None
    assert mutation.status_code == 403

    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
        assert row.team_id == team_b
        assert row.purpose == purpose
        assert row.submitted_by_user_id == user_b
        assert row.provider_connection_id == conn_b
        assert row.provider_connection_id != conn_a
        assert row.source_provenance[0]["source_batch_id"] == str(batch_shared)
        assert row.resolved_task_ids == [run_library_setup["task_id"]]
        # #1109: clone must not re-inject source pool-coverage trials.
        assert row.required_worker_pools == []
        assert row.expected_trial_count == 1
    sync_engine.dispose()


async def test_clone_and_reuse_reject_batch_on_retired_backend(
    run_library_setup: dict[str, object],
) -> None:
    """A historical batch stays readable but cannot seed a new hosted
    submission: it would inherit the retired backend or be silently relabelled
    as Nebius."""
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    trial_shared = run_library_setup["trial_shared"]
    conn_b = run_library_setup["conn_b"]
    safe_key = run_library_setup["safe_key"]
    postgres_url = run_library_setup["postgres_url"]

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(update(Batch).where(Batch.id == batch_shared).values(backend="docker"))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        headers = {"Authorization": f"Bearer {raw_b}"}
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={"name": "clone of historical", "provider_connection_id": str(conn_b)},
            headers=headers,
        )
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_shared}/artifacts/reuse",
            json={"key": safe_key, "name": "reuse of historical"},
            headers=headers,
        )
        detail = await ac.get(f"/api/v1/run-library/batches/{batch_shared}", headers=headers)

    with sync_engine.connect() as conn:
        derived = conn.execute(
            select(Batch.id).where(Batch.name.in_(["clone of historical", "reuse of historical"]))
        ).all()
    sync_engine.dispose()

    for response in (cloned, reused):
        assert response.status_code == 400, response.text
        assert "'docker'" in response.json()["detail"]
        assert "Nebius-only" in response.json()["detail"]
    assert derived == []
    assert detail.status_code == 200, detail.text
    assert detail.json()["backend"] == "docker"


async def test_clone_config_rejects_task_that_became_agent_incompatible(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_b = run_library_setup["conn_b"]
    task_id = str(run_library_setup["task_id"])
    postgres_url = str(run_library_setup["postgres_url"])

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as connection:
        task_config = connection.execute(
            select(Task.config).where(Task.id == task_id),
        ).scalar_one()
        connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                config={
                    **task_config,
                    "required_agent_capabilities": ["workspace_exec"],
                },
            ),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "must reject incompatible clone",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert response.status_code == 400, response.text
    assert "workspace_exec" in response.json()["detail"]
    with sync_engine.begin() as connection:
        clone_count = connection.execute(
            select(func.count())
            .select_from(Batch)
            .where(Batch.name == "must reject incompatible clone"),
        ).scalar_one()
    sync_engine.dispose()
    assert clone_count == 0


async def test_clone_and_reuse_reject_historical_benchmark_tasks(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    conn_b = run_library_setup["conn_b"]
    task_id = run_library_setup["task_id"]
    postgres_url = str(run_library_setup["postgres_url"])
    profile_id = f"historical-clone-{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as connection:
        connection.execute(
            insert(Benchmark).values(
                id=profile_id,
                display_name="Historical clone profile",
                upstream_kind="test",
                upstream_locator="test",
                upstream_revision="1",
                license_spdx="MIT",
                license_url="https://example.test/license",
                splits=["test"],
                execution_state="historical",
            )
        )
        connection.execute(update(Task).where(Task.id == task_id).values(benchmark_id=profile_id))

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            cloned = await ac.post(
                f"/api/v1/run-library/batches/{batch_shared}/clone-config",
                json={
                    "name": "must reject historical clone",
                    "provider_connection_id": str(conn_b),
                },
                headers={"Authorization": f"Bearer {raw_b}"},
            )
            reused = await ac.post(
                f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
                json={"provider_connection_id": str(run_library_setup["conn_b"]), "key": safe_key, "name": "must reject historical reuse"},
                headers={"Authorization": f"Bearer {raw_b}"},
            )

        for response in (cloned, reused):
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["reason"] == "benchmark_retired"
    finally:
        with sync_engine.begin() as connection:
            connection.execute(update(Task).where(Task.id == task_id).values(benchmark_id=None))
            connection.execute(delete(Benchmark).where(Benchmark.id == profile_id))
        sync_engine.dispose()


async def test_clone_config_surfaces_retry_default_mismatch(
    run_library_setup: dict[str, object],
) -> None:
    """#401 PR-3: source batch with explicit RetryPolicy that diverges from
    current deployment defaults surfaces a `retry_default_snapshot_mismatch`
    payload so the SPA can warn the operator before they run the clone."""
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_b = run_library_setup["conn_b"]
    postgres_url = run_library_setup["postgres_url"]

    # Patch the source batch's trial_config to carry an explicit retry policy
    # that differs from the current cluster defaults.
    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == batch_shared),
        ).scalar_one()
        patched = dict(row.trial_config)
        patched["retry"] = {
            "max_attempts": 7,
            "retry_on": ["worker_crash", "agent_timeout"],
            "backoff": {
                "base_sec": 5.0,
                "max_sec": 60.0,
                "multiplier": 3.0,
                "jitter": 0.5,
            },
        }
        s.execute(
            Batch.__table__.update().where(Batch.id == batch_shared).values(trial_config=patched),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "beta clone with retry mismatch",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert cloned.status_code == 201, cloned.text
    mismatch = cloned.json()["retry_default_snapshot_mismatch"]
    assert mismatch is not None
    assert mismatch["source"]["max_attempts"] == 7
    assert mismatch["source"]["retry_on"] == ["agent_timeout", "worker_crash"]
    assert mismatch["source"]["backoff"] == {
        "base_sec": 5.0,
        "max_sec": 60.0,
        "multiplier": 3.0,
        "jitter": 0.5,
    }
    assert mismatch["current"]["max_attempts"] == 3
    assert mismatch["current"]["retry_on"] == [
        "gateway_error",
        "node_setup_health",
        "provider_transport_disconnect",
    ]


async def test_legacy_team_token_cannot_clone_run_library_batch(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    legacy_raw_b = run_library_setup["legacy_raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_b = run_library_setup["conn_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "legacy clone from alpha",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {legacy_raw_b}"},
        )

    assert cloned.status_code == 403
    assert "legacy team token" in cloned.json()["detail"]


async def test_legacy_team_token_cannot_reuse_run_library_artifact(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    legacy_raw_b = run_library_setup["legacy_raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": safe_key, "name": "legacy reuse of alpha report"},
            headers={"Authorization": f"Bearer {legacy_raw_b}"},
        )

    assert reused.status_code == 403
    assert "legacy team token" in reused.json()["detail"]


async def test_reuse_shared_artifact_creates_provenance_and_blocks_raw(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    blocked_key = run_library_setup["blocked_key"]
    batch_shared = run_library_setup["batch_shared"]
    postgres_url = run_library_setup["postgres_url"]

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            update(Batch)
            .where(Batch.id == batch_shared)
            .values(required_worker_pools=["gpu-a", "gpu-b"]),
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"provider_connection_id": str(run_library_setup["conn_b"]), "key": safe_key, "name": "beta reuse of alpha report"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"provider_connection_id": str(run_library_setup["conn_b"]), "key": blocked_key, "name": "blocked reuse"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert reused.status_code == 201, reused.text
    body = reused.json()
    assert body["source_artifact"]["trial_id"] == str(trial_id)
    assert body["source_artifact"]["key"] == safe_key
    assert body["source_provenance"][0]["source_trial_id"] == str(trial_id)
    assert body["source_provenance"][0]["source_artifact_key"] == safe_key
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"

    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
        # #1109: artifact reuse must not re-inject source pool-coverage.
        assert row.required_worker_pools == []
        assert row.expected_trial_count == 1
    sync_engine.dispose()


@pytest.mark.asyncio
async def test_library_costs_match_batch_accounting_without_inventing_zero(run_library_setup):
    setup = run_library_setup
    engine = create_engine(str(setup["postgres_url"]))
    with engine.begin() as conn:
        conn.execute(insert(LlmCall).values(
            id=uuid4(), team_id=setup["team_a"], trial_id=setup["trial_shared"],
            step_id="main", model="openai/test", dialect="openai",
            input_tokens=10, output_tokens=5, provider_extras={},
            cost_usd=Decimal("0"), rate_card_hash="facade:tokens-only:test",
        ))
    engine.dispose()
    headers = {"Authorization": f"Bearer {setup['raw_a']}"}
    batch_id = str(setup["batch_shared"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=setup["app"]), base_url="http://svc") as client:
        listing = await client.get("/api/v1/run-library/batches?scope=my", headers=headers)
        detail = await client.get(f"/api/v1/run-library/batches/{batch_id}", headers=headers)
        batch = await client.get(f"/api/v1/batches/{batch_id}", headers=headers)
    assert listing.status_code == detail.status_code == batch.status_code == 200
    item = next(row for row in listing.json()["items"] if row["id"] == batch_id)
    for row in (item, detail.json(), batch.json()):
        assert row["estimated_cost_usd"] is None
        assert row["cost_status"] == "not_applicable"
        assert row["cost_estimate_source"] == "tokens-only"
        assert row["llm_calls_count"] == 1


@pytest.mark.parametrize("route", ["clone", "reuse", "native_reuse"])
async def test_derived_nebius_batch_freezes_current_runtime_and_enters_trial_admission(
    run_library_setup, route,
):
    from loom_control_plane.routes.trials import router as cp_trials_router

    f = run_library_setup
    app = f["app"]
    app.include_router(cp_trials_router, prefix="/cp")
    engine = create_engine(str(f["postgres_url"]))
    sessions = sessionmaker(engine)
    config = {"agent_name": "terminus-2", "agent_model": {"provider": "openai", "name": "gpt-4o-mini"}}
    combinations = [{**config, "label": label, "n_per_task": 2} for label in ("a", "b")]
    stale_profile = {**json.loads(app.state.settings.service_execution_runtime_profile_json),
                     "candidate_sha": "2" * 40}
    with sessions() as s:
        source = s.get(Batch, f["batch_shared"])
        source.trial_config = config
        source.combinations = combinations
        source.n_per_task = 2
        source.service_execution_runtime_profile = stale_profile
        s.commit()
    native = route == "native_reuse"
    destination = "a" if native else "b"
    file_path = "artifacts/verifier/ctrf.json"
    file_key = f"native/{f['trial_shared']}/{file_path}"
    if native:
        # Real native storage: no top-level key and no legacy trajectory inventory.
        content = b'{"results": {"tests": []}}\n'
        native_bucket = f"native-{f['trial_shared']}"
        storage = {
            "schema_version": "loom.canonical-trial-bundle-storage.v1",
            "attempt": 1,
            "files": [{"relative_path": file_path, "key": file_key,
                       "bucket": native_bucket,
                       "size_bytes": len(content), "sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
                       "media_type": "application/json"}],
            "source_evidence": [{"relative_path": "source/_manifest.json",
                                 "bucket": app.state.settings.artifacts_bucket,
                                 "key": f"native/{f['trial_shared']}/_manifest.json",
                                 "size_bytes": 2, "sha256": "sha256:" + "a" * 64,
                                 "media_type": "application/json"}],
        }
        storage["files"].append({
            **storage["files"][0], "relative_path": "result.json", "key": file_key + ".result",
        })
        with sessions() as s:
            artifact = s.get(Artifact, f["safe_artifact_id"])
            artifact.artifact_type = "loom.trial-artifact-bundle.v1"
            artifact.storage = storage
            artifact.content_hash = "sha256:" + "b" * 64
            artifact.visibility = "team"
            artifact.share_status = "pending_scan"
            artifact.safety_state = "verified_internal"
            artifact.redaction_state = "pending"
            source_trial = s.get(Trial, f["trial_shared"])
            source_trial.attempt_count = 1
            source_trial.trajectory_index = {"artifacts": []}
            s.commit()
        app.state.minio_client.create_bucket(Bucket=native_bucket)
        for item in storage["files"]:
            app.state.minio_client.put_object(Bucket=native_bucket, Key=item["key"], Body=content)
    url = (f"/api/v1/run-library/batches/{f['batch_shared']}/clone-config" if route == "clone"
           else f"/api/v1/run-library/trials/{f['trial_shared']}/artifacts/reuse")
    provider_id = f[f"conn_{destination}"]
    payload = {"name": "derived native", "provider_connection_id": str(provider_id)}
    if route != "clone":
        payload["key"] = file_key if native else f["safe_key"]
    headers = {"Authorization": f"Bearer {f[f'raw_{destination}']}"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            if native:
                detail = await c.get(
                    f"/api/v1/run-library/batches/{f['batch_shared']}", headers=headers,
                )
                assert detail.status_code == 200, detail.text
                items = [item for group in detail.json()["artifact_inventory"].values() for item in group]
                selected = next((item for item in items if item["key"] == file_key), None)
                assert selected is not None, "native bundle file missing from Run Library"
                assert selected["can_reuse"] is True
                assert selected["relative_path"] == "files/" + file_path
                assert {item["key"] for item in items if item["id"] == str(f["safe_artifact_id"])} == {
                    file_key, file_key + ".result",
                }
                listing = await c.get("/api/v1/run-library/artifacts", headers=headers)
                assert any(item["key"] == file_key for item in listing.json()["items"])
                download = await c.get(selected["download_url"], headers=headers)
                assert download.status_code == 200 and download.content == content
                foreign_headers = {"Authorization": f"Bearer {f['raw_b']}"}
                foreign_payload = {**payload, "provider_connection_id": str(f["conn_b"])}
                denied = await c.post(url, json=foreign_payload, headers=foreign_headers)
                assert denied.status_code == 403, denied.text
                denied_download = await c.get(selected["download_url"], headers=foreign_headers)
                assert denied_download.status_code == 403
                foreign_detail = await c.get(
                    f"/api/v1/run-library/batches/{f['batch_shared']}", headers=foreign_headers,
                )
                assert all(
                    item["id"] != str(f["safe_artifact_id"])
                    for group in foreign_detail.json()["artifact_inventory"].values() for item in group
                )
            response = await c.post(url, json=payload, headers=headers)
            assert response.status_code == 201, response.text
            derived_id = UUID(response.json()["batch_id"])
            # Follow the real persisted batch through Control Plane admission;
            # a 201 from Run Library alone did not detect the original defect.
            submitted = await c.post("/cp/trials", headers=headers, json={
                "batch_id": str(derived_id), "task_id": f["task_id"], "config": config,
                "provider_connection_id": str(provider_id),
            })
            assert submitted.status_code == 201, submitted.text
            with sessions() as s:
                derived = s.get(Batch, derived_id)
                frozen = derived.service_execution_runtime_profile
                current = json.loads(app.state.settings.service_execution_runtime_profile_json)
                assert frozen["candidate_sha"] == current["candidate_sha"]
                assert frozen["agent_image_ref"] == current["agent_image_ref"]
                assert frozen["task_resource_requests"] == {f["task_id"]: {
                    "task_revision_sha256": "sha256:" + "1" * 64,
                    "requests": _NEBIUS_DEFAULT_REQUESTS,
                }}
                assert derived.team_id == f[f"team_{destination}"] and derived.provider_connection_id == provider_id
                assert derived.combinations == combinations and derived.expected_trial_count == 4
                assert derived.source_provenance[0]["source_batch_id"] == str(f["batch_shared"])
                assert s.get(Batch, f["batch_shared"]).service_execution_runtime_profile == stale_profile
                admitted = s.get(Trial, UUID(submitted.json()["trial_id"]))
                assert admitted.requires_caps["worker_pool"] == "nebius-cpu"
            if native:
                with sessions() as s:
                    derived = s.get(Batch, derived_id)
                    provenance = derived.source_provenance[0]
                    assert provenance["source_artifact_id"] == str(f["safe_artifact_id"])
                    assert provenance["source_artifact_relative_path"] == "files/" + file_path
                    assert provenance["source_file_sha256"] == "sha256:" + hashlib.sha256(content).hexdigest()
                    original = s.get(Artifact, f["safe_artifact_id"])
                    assert original.storage == storage
                    assert original.share_status == "pending_scan"
                    assert original.safety_state == "verified_internal"
                    original.share_status = "blocked"
                    original.safety_state = "unsafe"
                    original.blocked_reason = "secret-like content detected"
                    s.commit()
                denied = await c.post(url, json=payload, headers=headers)
                assert denied.status_code == 403
                assert denied.json()["detail"] == "secret-like content detected"
                with sessions() as s:
                    original = s.get(Artifact, f["safe_artifact_id"])
                    original.share_status = "pending_scan"
                    original.safety_state = "verified_internal"
                    original.blocked_reason = None
                    s.commit()
            # No profile means reject before persisting another derived batch.
            app.state.settings = app.state.settings.model_copy(update={
                "service_execution_runtime_profile_json": "{}",
            })
            rejected = await c.post(url, json={**payload, "name": "no runtime"}, headers=headers)
            assert rejected.status_code == 400, rejected.text
            assert "runtime_profile_unavailable" in rejected.text
            with sessions() as s:
                assert s.scalar(select(func.count()).select_from(Batch).where(Batch.name == "no runtime")) == 0
    finally:
        engine.dispose()
