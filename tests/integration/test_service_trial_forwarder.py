"""POST /api/v1/trials + /cancel forwarders (Plan 18 Task 8).

Uses httpx.MockTransport on `app.state.http_client` so we capture
the outbound CP request without bringing up an actual CP instance.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    ProviderConnection,
    Task,
    TaskSet,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    Trial,
    User,
)
from loom_service import agent_catalog
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_service_batches_crud import _seed_connection


@pytest.fixture
async def fwd_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]]]:
    for k, v in {
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
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )

    captured: dict[str, list[dict[str, str]]] = {"reqs": []}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["reqs"].append(
            {
                "method": req.method,
                "url": str(req.url),
                "body": req.content.decode() if req.content else "",
                "auth": req.headers.get("authorization") or "",
            }
        )
        if req.url.path == "/trials" and req.method == "POST":
            return httpx.Response(
                201,
                json={
                    "trial_id": "00000000-0000-0000-0000-000000000001",
                    "state": "queued",
                },
            )
        if req.url.path.endswith("/cancel") and req.method == "POST":
            trial_id = UUID(req.url.path.split("/")[-2])
            with sl() as session:
                session.execute(
                    update(Trial)
                    .where(Trial.id == trial_id)
                    .values(
                        state="cancelled",
                        cancellation_requested_at=now,
                        finished_at=now,
                    )
                )
                session.commit()
            return httpx.Response(200, json={"state": "cancelled"})
        return httpx.Response(404)

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://cp",
    )

    team_id = uuid4()
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        username = f"submitter-{user_id.hex[:8]}"
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username,
                status="active",
                is_platform_admin=False,
                created_at=now,
            )
        )
        s.execute(
            insert(TeamMembership).values(
                team_id=team_id,
                user_id=user_id,
                role="member",
                created_at=now,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=now,
            )
        )
        for task_id in ("local/task-1", "local/t"):
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum=hashlib.sha256(task_id.encode()).hexdigest(),
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                    source="local",
                ),
            )
        s.commit()
    try:
        yield app, raw, team_id, captured
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(Token))
            s.execute(delete(TeamMembership))
            s.execute(delete(Task))
            s.execute(delete(TaskSet))
            s.execute(delete(TeamQuota))
            s.execute(delete(User))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_post_trial_forwards(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
) -> None:
    app, raw, _team_id, captured = fwd_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "local/task-1",
                "config": {"agent": {"name": "fake"}},
            },
        )
    assert r.status_code == 201
    assert "trial_id" in r.json()
    # Upstream got the call, with the original bearer token forwarded.
    assert captured["reqs"][0]["method"] == "POST"
    assert captured["reqs"][0]["url"].endswith("/trials")
    assert captured["reqs"][0]["auth"] == f"Bearer {raw}"


async def test_post_trial_forwards_pool_pin_and_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
) -> None:
    # Forwarding mechanics; hosted Nebius admission is covered separately.
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    app, raw, _team_id, captured = fwd_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        response = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "local/task-1",
                "config": {"agent_name": "oracle", "agent_model": None},
                "required_worker_pool": "local_cpu",
                "idempotency_key": "local_cpu-release-witness-1",
            },
        )

    assert response.status_code == 201
    assert json.loads(captured["reqs"][0]["body"]) == {
        "task_id": "local/task-1",
        "config": {"agent_name": "oracle", "agent_model": None},
        "provider_connection_id": None,
        "provider_model_id": None,
        "required_worker_pool": "local_cpu",
        "idempotency_key": "local_cpu-release-witness-1",
    }


async def test_post_trial_no_scope_403(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
) -> None:
    """Caller without `submit` scope is rejected locally; no upstream
    round-trip."""
    app, _raw, team_id, captured = fwd_setup
    other = f"loom_team_{uuid4().hex}"
    other_user_id = uuid4()
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        username = f"read-user-{other_user_id.hex[:8]}"
        s.execute(
            insert(User).values(
                id=other_user_id,
                username=username,
                username_normalized=username,
                status="active",
                is_platform_admin=False,
                created_at=now,
            )
        )
        s.execute(
            insert(TeamMembership).values(
                team_id=team_id,
                user_id=other_user_id,
                role="member",
                created_at=now,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                created_by_user_id=other_user_id,
                issued_at=now,
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
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {other}"},
            json={
                "task_id": "local/task-1",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
    assert r.status_code == 403
    assert all(req["auth"] != f"Bearer {other}" for req in captured["reqs"])


async def test_post_trial_rejects_agent_without_service_runtime(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(agent_catalog._ADAPTER_RUNTIME_READY, "codex", False)
    app, raw, _team_id, captured = fwd_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "local/task-1",
                "config": {
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                },
            },
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "codex" in detail
    assert "runtime" in detail.lower()
    assert "GET /api/v1/agents" in detail
    assert captured["reqs"] == []


async def test_post_trial_rejects_completion_agent_for_workspace_task(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
) -> None:
    app, raw, team_id, captured = fwd_setup
    task_id = "local/workspace-trial"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="w" * 64,
                config={
                    "schema_version": "1",
                    "required_agent_capabilities": ["workspace_exec"],
                    "task": {"id": task_id, "name": task_id},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "script"},
                    "steps": [{"name": "main"}],
                },
                source="local",
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": task_id,
                "config": {
                    "agent_name": "direct-completion",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
            },
        )

    assert response.status_code == 400, response.text
    assert "workspace_exec" in response.json()["detail"]
    assert captured["reqs"] == []


async def test_post_trial_hides_foreign_private_task_set_task(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
) -> None:
    app, raw, team_id, captured = fwd_setup
    foreign_team_id = uuid4()
    task_set_id = f"ts/{foreign_team_id}/private-source"
    task_id = f"{task_set_id}/tasks/row-1"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=foreign_team_id, name=f"t-{foreign_team_id}"))
        s.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=foreign_team_id,
                slug="private-source",
                display_name="Private Source",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{foreign_team_id}/private-source/manifest.yaml"
                ),
            ),
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="f" * 64,
                config={
                    "schema_version": "1",
                    "required_agent_capabilities": ["workspace_exec"],
                    "task": {"id": task_id, "name": task_id},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "script"},
                    "steps": [{"name": "main"}],
                },
                source="local",
                task_set_id=task_set_id,
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": task_id,
                "config": {
                    "agent_name": "direct-completion",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
            },
        )

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "task not found"
    assert captured["reqs"] == []


async def test_cancel_trial_forwards(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
) -> None:
    app, raw, team_id, captured = fwd_setup
    trial_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="local/task-cancel",
                checksum="x" * 64,
                config={},
                source="local",
            )
        )
        s.execute(
            insert(Trial).values(
                id=trial_id,
                task_id="local/task-cancel",
                team_id=team_id,
                state="running",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
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
            f"/api/v1/trials/{trial_id}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json()["state"] == "cancelled"
    cancel_reqs = [req for req in captured["reqs"] if req["url"].endswith("/cancel")]
    assert len(cancel_reqs) == 1
    assert cancel_reqs[0]["auth"] == f"Bearer {raw}"


@pytest.mark.parametrize(
    ("backend", "task_config", "trial_state"),
    [
        (
            "nebius",
            {"service_execution": {"logical_pool_id": "nebius-cpu"}},
            "running",
        ),
        ("nebius", {}, "running"),
        (
            "docker",
            {"service_execution": {"logical_pool_id": "nebius-cpu"}},
            "running",
        ),
        ("docker", {}, "protected-pending"),
    ],
)
async def test_cancel_batch_routes_every_active_trial_through_control_plane(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
    backend: str,
    task_config: dict[str, object],
    trial_state: str,
) -> None:
    app, raw, team_id, captured = fwd_setup
    batch_id = uuid4()
    trial_id = uuid4()
    task_id = "local/service-execution-cancel"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as session:
        session.execute(
            insert(Task).values(
                id=task_id,
                checksum="c" * 64,
                config=task_config,
                source="local",
            )
        )
        session.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="service execution cancellation",
                task_filter={},
                trial_config={},
                backend=backend,
                state="running",
                created_by_token_prefix="test",
                expected_trial_count=1,
            )
        )
        session.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                batch_id=batch_id,
                state=trial_state,
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
            )
        )
        session.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{batch_id}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["state"] == "cancelled"
    cancel_reqs = [req for req in captured["reqs"] if req["url"].endswith("/cancel")]
    assert len(cancel_reqs) == 1
    assert cancel_reqs[0]["url"].endswith(f"/trials/{trial_id}/cancel")
    assert cancel_reqs[0]["auth"] == f"Bearer {raw}"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as session:
        assert session.get(Trial, trial_id).state == "cancelled"
        assert session.get(Batch, batch_id).state == "cancelled"
    sync_engine.dispose()


async def test_cancel_unknown_trial_404(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
) -> None:
    app, raw, _team_id, _captured = fwd_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            f"/api/v1/trials/{uuid4()}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_forwarder_propagates_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
) -> None:
    """Audit H3: upstream 429 with Retry-After must reach the client
    so backoff works. Plan 19's rate-limited batch submits depend
    on this."""
    # Forwarding mechanics; hosted Nebius admission is covered separately.
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    app, raw, _team_id, _captured = fwd_setup
    # Re-wire the mock to return 429 with a Retry-After header.
    await app.state.http_client.aclose()

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"detail": "rate limited"},
            headers={
                "Retry-After": "30",
                "X-RateLimit-Remaining": "0",
                "X-Internal-Trace": "leak-me",  # NOT in allowlist
            },
        )

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://cp",
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "local/t", "config": {"agent_name": "oracle", "agent_model": None}},
        )
    assert r.status_code == 429
    assert r.headers.get("retry-after") == "30"
    assert r.headers.get("x-ratelimit-remaining") == "0"
    # Internal trace header NOT propagated.
    assert "x-internal-trace" not in r.headers


async def test_cancel_cross_team_403(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    postgres_url: str,
) -> None:
    """Caller from team B cannot cancel team A's trial."""
    app, _raw_a, team_a, captured = fwd_setup
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    trial_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Team).values(
                id=other_team,
                name=f"o-{other_team}",
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other_raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=other_team,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Task).values(
                id="local/task-xt",
                checksum="x" * 64,
                config={},
                source="local",
            )
        )
        # Trial belongs to team_a (the original fixture team).
        s.execute(
            insert(Trial).values(
                id=trial_id,
                task_id="local/task-xt",
                team_id=team_a,
                state="running",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
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
            f"/api/v1/trials/{trial_id}/cancel",
            headers={"Authorization": f"Bearer {other_raw}"},
        )
    assert r.status_code == 403
    assert all(req["auth"] != f"Bearer {other_raw}" for req in captured["reqs"])


async def test_hosted_trial_rejects_agent_without_native_execution(
    fwd_setup: tuple[FastAPI, str, UUID, dict[str, list[dict[str, str]]]],
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> None:
    """#2054: hosted single trials run natively on Nebius, which cannot run
    OpenHands yet; say so instead of forwarding a doomed trial."""
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    app, raw, team_id, captured = fwd_setup
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "local/task-1",
                "config": {
                    "agent_name": "openhands-sdk",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
                "provider_connection_id": _seed_connection(postgres_url, team_id, "gpt-4o-mini"),
            },
        )

    assert r.status_code == 400, r.text
    assert "agent 'openhands' is not yet runnable on hosted (Nebius) execution" in r.json()["detail"]
    assert captured["reqs"] == []
