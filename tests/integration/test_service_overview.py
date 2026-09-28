"""Home overview summary API (#455).

The overview endpoint is the SPA's first-run control surface: it should
summarize what the signed-in team can do now, what needs attention, and
which operator-owned prerequisites are missing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Batch,
    Benchmark,
    ProviderConnection,
    Task,
    Team,
    TeamMembership,
    TeamQuota,
    Trial,
    User,
    UserSession,
    Worker,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.routes import overview as overview_route

RAW_ADMIN_TOKEN = "loom_admin_" + "O" * 43


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def overview_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN": "1",
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
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    other_team_id = uuid4()
    user_id = uuid4()
    now = datetime.now(UTC)
    task_id = "humaneval/HumanEval/0"
    latest_batch_id = uuid4()
    old_batch_id = uuid4()
    other_batch_id = uuid4()
    stale_worker_id = uuid4()

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name="EAI"))
        s.execute(insert(Team).values(id=other_team_id, name="Other Team"))
        s.execute(insert(User).values(
            id=user_id,
            username="Owner",
            username_normalized="owner",
            email="owner@example.com",
            display_name="Owner Example",
            status="active",
            is_platform_admin=False,
            created_at=now,
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_id,
            user_id=user_id,
            role="owner",
        ))
        s.execute(insert(Benchmark).values(
            id="humaneval",
            display_name="HumanEval",
            upstream_kind="huggingface",
            upstream_locator="openai/openai_humaneval",
            upstream_revision="",
            license_spdx="MIT",
            license_url="https://example.test/humaneval",
            splits=["test"],
        ))
        s.execute(insert(Benchmark).values(
            id="gaia",
            display_name="GAIA",
            upstream_kind="huggingface",
            upstream_locator="gaia-benchmark/GAIA",
            upstream_revision="",
            license_spdx="CC-BY-SA-4.0",
            license_url="https://example.test/gaia",
            splits=["validation"],
        ))
        s.execute(insert(Task).values(
            id=task_id,
            benchmark_id="humaneval",
            config=_valid_task_config(task_id),
            checksum="1" * 64,
            source="s3://bucket/humaneval/",
            license="MIT",
            registered_at=now,
        ))
        for name, status, err, deleted in (
            ("ready-provider", "valid", None, None),
            ("needs-fix", "invalid", "timeout after 5s", None),
            ("deleted", "valid", None, now),
        ):
            s.execute(insert(ProviderConnection).values(
                id=uuid4(),
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=name,
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                encrypted_api_key_ref=f"env:{name.upper().replace('-', '_')}",
                created_by="test",
                status=status,
                last_validated_at=now if status != "pending" else None,
                last_validation_error=err,
                deleted_at=deleted,
            ))
        s.execute(insert(ProviderConnection).values(
            id=uuid4(),
            team_id=other_team_id,
            provider_type="openai-compatible",
            display_name="other-provider",
            base_url="https://api.example.test/v1",
            upstream_host="api.example.test",
            encrypted_api_key_ref="env:OTHER",
            created_by="test",
            status="valid",
        ))
        s.execute(insert(Worker).values(
            id=uuid4(),
            hostname="trt-local-arm-1",
            version="test",
            capabilities=[{"backend": "docker", "cpu_arch": "arm64"}],
            max_concurrent=10,
            pool_name="local-arm",
            registered_at=now,
            last_seen_at=now,
            status="active",
        ))
        fresh_x86_worker_id = uuid4()
        s.execute(insert(Worker).values(
            id=fresh_x86_worker_id,
            hostname="staging-x86-1",
            version="test",
            capabilities=[
                {"backend": "docker", "cpu_arch": "x86_64"},
                {"backend": "fake", "cpu_arch": "x86_64"},
            ],
            max_concurrent=2,
            pool_name="staging-x86",
            registered_at=now,
            last_seen_at=now,
            status="active",
        ))
        s.execute(insert(Worker).values(
            id=stale_worker_id,
            hostname="stale-worker",
            version="test",
            capabilities=[{"backend": "modal", "cpu_arch": "x86_64"}],
            max_concurrent=99,
            pool_name="stale-pool",
            registered_at=now - timedelta(minutes=10),
            last_seen_at=now - timedelta(minutes=10),
            status="active",
        ))
        s.execute(insert(Batch).values(
            id=old_batch_id,
            team_id=team_id,
            name="older submitted",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="submitted",
            created_at=now - timedelta(hours=1),
            created_by_token_prefix="test:web",
            expected_trial_count=1,
            backend="docker",
        ))
        s.execute(insert(Batch).values(
            id=latest_batch_id,
            team_id=team_id,
            name="latest running",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="running",
            created_at=now,
            created_by_token_prefix="test:web",
            expected_trial_count=2,
            backend="docker",
        ))
        s.execute(insert(Batch).values(
            id=other_batch_id,
            team_id=other_team_id,
            name="other submitted",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="submitted",
            created_at=now,
            created_by_token_prefix="test:web",
            expected_trial_count=100,
            backend="docker",
        ))
        for state, batch_id, worker_id in (
            ("queued", old_batch_id, None),
            ("claimed", latest_batch_id, fresh_x86_worker_id),
            ("running", latest_batch_id, fresh_x86_worker_id),
            ("succeeded", latest_batch_id, None),
        ):
            s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id=task_id,
                batch_id=batch_id,
                config={"agent_name": "oracle"},
                requires_caps={},
                state=state,
                worker_id=worker_id,
                result={"aggregate_reward": 1.0} if state == "succeeded" else None,
                submitted_at=now,
            ))
        s.execute(insert(Trial).values(
            id=uuid4(),
            team_id=other_team_id,
            task_id=task_id,
            batch_id=other_batch_id,
            config={"agent_name": "oracle"},
            requires_caps={},
            state="queued",
            submitted_at=now,
        ))
        s.commit()
    try:
        yield app, team_id, latest_batch_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            from loom.db.schema import LoginChallenge

            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(Worker))
            s.execute(delete(UserSession))
            s.execute(delete(LoginChallenge))
            s.execute(delete(TeamMembership))
            s.execute(delete(TeamQuota))
            s.execute(delete(User))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _login(ac: httpx.AsyncClient) -> dict[str, object]:
    start = await ac.post(
        "/api/v1/auth/login/start",
        json={"email": "owner@example.com"},
    )
    assert start.status_code == 200, start.text
    login_token = start.json().get("login_token")
    assert isinstance(login_token, str)
    complete = await ac.post(
        "/api/v1/auth/login/complete",
        json={"token": login_token},
    )
    assert complete.status_code == 200, complete.text
    return complete.json()


async def test_overview_summarizes_signed_in_team_readiness(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get("/api/v1/overview")

    assert r.status_code == 200, r.text
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["status"] == "needs_setup"
    assert body["team_context"] == {
        "team_id": str(team_id),
        "team_name": "EAI",
        "role": "owner",
        "scopes": [
            "read:own",
            "submit",
            "tokens:manage",
            "providers:manage",
            "team:manage",
        ],
        "is_platform_admin": False,
        "submissions_paused": False,
    }
    assert body["capabilities"] == {
        "can_read": True,
        "can_submit": True,
        "can_manage_providers": True,
        "can_manage_team": True,
    }
    assert body["provider_health"] | {
        "total": 2,
        "ready": 1,
        "needs_attention": 1,
        "untested": 0,
    } == body["provider_health"]
    assert [p["name"] for p in body["provider_health"]["latest"]] == [
        "needs-fix",
        "ready-provider",
    ]
    assert body["benchmark_readiness"] | {
        "total": 2,
        "runnable": 1,
        "needs_attention": 1,
    } == body["benchmark_readiness"]
    assert body["benchmark_readiness"]["blocked"][0]["id"] == "gaia"
    assert body["worker_health"] == {
        "active": 2,
        "available_backends": ["docker", "fake"],
        "has_default_backend": True,
    }
    assert body["run_activity"]["batches"] == {
        "submitted": 1,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["run_activity"]["trials"] == {
        "queued": 1,
        "protected-pending": 0,
        "claimed": 1,
        "running": 1,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["run_activity"]["latest_batch"] | {
        "id": str(latest_batch_id),
        "name": "latest running",
        "state": "running",
        "expected_trial_count": 2,
    } == body["run_activity"]["latest_batch"]
    action_ids = {item["id"] for item in body["next_actions"]}
    assert {"configure_execution", "repair_provider"} <= action_ids
    assert "create_batch" not in action_ids
    assert "start_worker" not in action_ids


async def test_monitor_summary_scopes_state_counts_and_worker_capacity(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, _latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get(
            "/api/v1/monitor/summary",
            params={
                "view": "trials",
                "team_id": str(team_id),
                "benchmark_id": "humaneval",
                "agent": "oracle",
                "state": "failed",
            },
        )

    assert r.status_code == 200, r.text
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["scope"] == {
        "view": "trials",
        "team_id": str(team_id),
        "q": None,
        "benchmark_id": "humaneval",
        "agent_name": "oracle",
        "model_provider": None,
        "model_name": None,
        "provider_connection_id": None,
        "provider_model_id": None,
        "batch_id": None,
        "state": "failed",
    }
    assert body["state_counts"]["batches"] == {
        "submitted": 1,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["state_counts"]["trials"] == {
        "queued": 1,
        "protected-pending": 0,
        "claimed": 1,
        "running": 1,
        "materializing": 0,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["queue"] == {
        "queued": 1,
        "protected_pending": 0,
        "claimed": 1,
        "running": 1,
        "waiting": 2,
        "active_workers": 2,
        "available_backends": ["docker", "fake"],
        "has_default_backend": True,
        "status": "waiting",
    }
    assert body["resources"]["aggregate"] == {
        "current_active_slots": 12,
        "active_workers": 2,
        "draining_workers": 0,
        "total_slots": 12,
        "draining_slots": 0,
        "occupied_slots": 2,
        "free_slots": 10,
        "running_tasks": 1,
        "starting_tasks": 1,
        "pre_start_heartbeat_fresh_tasks": 0,
        "oldest_starting_task_age_sec": None,
        "queued_tasks": 1,
    }
    assert body["resources"]["pools"] == [
        {
            "pool_name": "local-arm",
            "backend": "docker",
            "cpu_arch": "arm64",
            "current_active_slots": 10,
            "active_workers": 1,
            "draining_workers": 0,
            "total_slots": 10,
            "draining_slots": 0,
            "occupied_slots": 0,
            "free_slots": 10,
            "running_tasks": 0,
            "starting_tasks": 0,
            "pre_start_heartbeat_fresh_tasks": 0,
            "oldest_starting_task_age_sec": None,
            "queued_tasks": 1,
        },
        {
            "pool_name": "staging-x86",
            "backend": "docker",
            "cpu_arch": "x86_64",
            "current_active_slots": 2,
            "active_workers": 1,
            "draining_workers": 0,
            "total_slots": 2,
            "draining_slots": 0,
            "occupied_slots": 2,
            "free_slots": 0,
            "running_tasks": 1,
            "starting_tasks": 1,
            "pre_start_heartbeat_fresh_tasks": 0,
            "oldest_starting_task_age_sec": None,
            "queued_tasks": 1,
        },
    ]
    assert body["service_execution"] == {
        "targets": [],
        "activity": {
            "lease_count": 0,
            "execution_states": {},
            "lifecycle_stages": {
                "queued": 0,
                "admission_blocked": 0,
                "provisioning": 0,
                "running": 0,
                "verifying": 0,
                "materializing": 0,
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "output_unavailable": 0,
            },
            "materialization": {
                "states": {
                    "not_started": 0,
                    "pending": 0,
                    "running": 0,
                    "committed": 0,
                    "unavailable": 0,
                },
                "backlog": 0,
                "retry_attempts": 0,
                "oldest_next_attempt_at": None,
                "oldest_pending_at": None,
                "oldest_pending_age_seconds": None,
                "last_committed_at": None,
                "pending_bytes": 0,
                "source_retained_bytes": 0,
            },
            "source_cleanup_states": {
                "not_ready": 0,
                "retained": 0,
                "running": 0,
                "complete": 0,
            },
        },
    }


async def test_monitor_summary_reports_protected_pending_as_waiting_demand(
    overview_setup: tuple[FastAPI, UUID, UUID],
    postgres_url: str,
) -> None:
    app, team_id, latest_batch_id = overview_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as session:
            session.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="humaneval/HumanEval/0",
                    batch_id=latest_batch_id,
                    config={"agent_name": "oracle"},
                    requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                    state="protected-pending",
                    submitted_at=datetime.now(UTC),
                )
            )
            session.commit()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            await _login(ac)
            response = await ac.get(
                "/api/v1/monitor/summary",
                params={"view": "trials", "team_id": str(team_id)},
            )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["state_counts"]["trials"]["protected-pending"] == 1
        assert body["queue"]["protected_pending"] == 1
        assert body["queue"]["waiting"] == 3
        assert body["resources"]["aggregate"]["queued_tasks"] == 2
    finally:
        sync_engine.dispose()


async def test_monitor_summary_batch_filter_does_not_count_other_batches(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, batch_id = overview_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as ac:
        await _login(ac)
        response = await ac.get("/api/v1/monitor/summary", params={
            "view": "trials", "team_id": str(team_id), "batch_id": str(batch_id),
        })
    assert response.status_code == 200, response.text
    assert response.json()["state_counts"]["batches"] == {
        "submitted": 0, "running": 1, "finished": 0, "cancelled": 0,
    }


async def test_monitor_summary_search_scopes_batch_identity_counts(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, _latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get(
            "/api/v1/monitor/summary",
            params={
                "view": "batches",
                "team_id": str(team_id),
                "q": "latest",
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scope"]["q"] == "latest"
    assert body["state_counts"]["batches"] == {
        "submitted": 0,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["state_counts"]["trials"] == {
        "queued": 0,
        "protected-pending": 0,
        "claimed": 1,
        "running": 1,
        "materializing": 0,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["queue"] | {
        "queued": 0,
        "claimed": 1,
        "running": 1,
        "waiting": 1,
    } == body["queue"]


async def test_overview_marks_operator_prerequisites_separately(
    overview_setup: tuple[FastAPI, UUID, UUID],
    postgres_url: str,
) -> None:
    app, _team_id, _latest_batch_id = overview_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(update(Trial).values(worker_id=None))
        s.execute(delete(Worker))
        s.execute(update(Task).values(benchmark_id=None))
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get("/api/v1/overview")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "needs_setup"
    assert body["worker_health"] == {
        "active": 0,
        "available_backends": [],
        "has_default_backend": False,
    }
    assert body["benchmark_readiness"]["runnable"] == 0
    operator_actions = [
        action for action in body["next_actions"]
        if action["kind"] == "operator"
    ]
    assert {action["id"] for action in operator_actions} == {
        "publish_benchmarks",
        "configure_execution",
    }


@pytest.mark.parametrize("fresh,nodes,blockers,expected", [
    (True, 1, [], "observed"),
    (True, 0, [], "observed"),
    (False, 0, ["execution_capacity_observation_stale"], "unknown"),
    (True, 0, ["execution_capacity_provider_quota_nodes_exceeded"], "needs_attention"),
])
async def test_overview_native_submission_does_not_require_legacy_workers(
    overview_setup, monkeypatch, fresh: bool, nodes: int, blockers: list[str], expected: str,
) -> None:
    app, _team_id, _batch_id = overview_setup
    monkeypatch.setattr(overview_route, "get_active_worker_count", AsyncMock(return_value=0))
    monkeypatch.setattr(overview_route, "get_active_backends", AsyncMock(return_value=set()))
    monkeypatch.setattr(overview_route, "fetch_execution_capacity_status", AsyncMock(return_value={
        "targets": [{
            "desired_state": "active", "policy": {"enabled": True},
            "observation": {"is_fresh": fresh, "active_nodes": nodes},
            "blockers": blockers,
        }, {
            "desired_state": "disabled", "policy": {"enabled": True},
            "observation": None, "blockers": ["execution_capacity_target_not_active"],
        }],
    }), raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as ac:
        await _login(ac)
        response = await ac.get("/api/v1/overview")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["execution_health"] == {
        "configured_targets": 1, "status": expected,
    }
    actions = {item["id"] for item in body["next_actions"]}
    assert "create_batch" in actions
    assert "start_worker" not in actions
    assert ("inspect_execution" in actions) is (expected != "observed")
