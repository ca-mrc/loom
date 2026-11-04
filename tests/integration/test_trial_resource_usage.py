from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialResourceUsage,
    Worker,
)
from loom_control_plane.app import create_app as create_cp_app
from loom_control_plane.config import ControlPlaneSettings
from loom_service.app import create_app as create_service_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
def resource_seed(postgres_url: str) -> Iterator[dict[str, Any]]:
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    team_id = uuid4()
    other_team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    batch_id = uuid4()
    worker_token = f"worker_{uuid4().hex}"
    team_token = f"team_{uuid4().hex}"
    other_token = f"other_{uuid4().hex}"
    task_id = f"resource-accounting-{uuid4().hex}"
    now = datetime.now(UTC)
    with sessions() as session:
        session.execute(insert(Team).values(id=team_id, name=f"team-{team_id}"))
        session.execute(insert(Team).values(id=other_team_id, name=f"team-{other_team_id}"))
        session.execute(insert(TeamQuota).values(team_id=team_id))
        session.execute(insert(TeamQuota).values(team_id=other_team_id))
        for raw, token_type, scopes, token_team in (
            (worker_token, "worker", ["worker:report"], None),
            (team_token, "team", ["read:own"], team_id),
            (other_token, "team", ["read:own"], other_team_id),
        ):
            session.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode()).digest(),
                    type=token_type,
                    scopes=scopes,
                    team_id=token_team,
                    issued_at=now,
                    expires_at=None,
                )
            )
        session.execute(
            insert(Worker).values(
                id=worker_id,
                hostname=f"worker-{worker_id}",
                version="test",
                capabilities=[],
                registered_at=now,
                last_seen_at=now,
                status="active",
            )
        )
        session.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        session.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="resource accounting test",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="running",
                created_by_token_prefix="resource",
                expected_trial_count=1,
            )
        )
        session.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                batch_id=batch_id,
                config={},
                requires_caps={},
                state="running",
                worker_id=worker_id,
                attempt_count=1,
            )
        )
        session.commit()
    try:
        yield {
            "trial_id": trial_id,
            "batch_id": batch_id,
            "team_id": team_id,
            "worker_id": worker_id,
            "worker_token": worker_token,
            "team_token": team_token,
            "other_token": other_token,
        }
    finally:
        with sessions() as session:
            session.execute(
                delete(TrialResourceUsage).where(TrialResourceUsage.trial_id == trial_id)
            )
            session.execute(delete(Trial).where(Trial.id == trial_id))
            session.execute(delete(Batch).where(Batch.id == batch_id))
            session.execute(delete(Worker).where(Worker.id == worker_id))
            session.execute(delete(Token).where(Token.team_id.in_([team_id, other_team_id])))
            session.execute(
                delete(Token).where(
                    Token.token_hash == hashlib.sha256(worker_token.encode()).digest(),
                )
            )
            session.execute(
                delete(TeamQuota).where(TeamQuota.team_id.in_([team_id, other_team_id]))
            )
            session.execute(delete(Team).where(Team.id.in_([team_id, other_team_id])))
            session.execute(delete(Task).where(Task.id == task_id))
            session.commit()
        engine.dispose()


def _payload(
    seed: dict[str, Any],
    *,
    seq: int,
    final: bool,
    cpu: int,
    memory_current: int = 100,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "schema_version": 1,
        "trial_id": str(seed["trial_id"]),
        "attempt_count": 1,
        "worker_id": str(seed["worker_id"]),
        "execution_key": "a" * 64,
        "runtime_id_hash": "b" * 64,
        "container_role": "agent",
        "role_name": "primary",
        "backend": "docker",
        "architecture": "arm64",
        "candidate_sha": "c" * 40,
        "image_digest": "sha256:" + "d" * 64,
        "source": "docker_stats",
        "observation_seq": seq,
        "container_started_at": (now - timedelta(seconds=5)).isoformat(),
        "first_observed_at": (now - timedelta(seconds=4)).isoformat(),
        "last_observed_at": now.isoformat(),
        "finalized_at": now.isoformat() if final else None,
        "terminal_reason": "container_stopped" if final else None,
        "completeness": "complete" if final else "partial",
        "diagnostic_code": None,
        "limits": {"cpu_cores": 2, "memory_bytes": 8 * 1024**3, "pids": 512},
        "counters": {
            "cpu_usage_usec": cpu,
            "cpu_throttled_usec": 7,
            "memory_current_bytes": memory_current,
            "memory_peak_bytes": 200,
            "pids_current": 2,
            "pids_peak": 3,
        },
    }


def test_control_plane_upsert_is_idempotent_and_monotonic(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    resource_seed: dict[str, Any],
) -> None:
    for key, value in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_cp_app(ControlPlaneSettings(_env_file=None))
    headers = {"Authorization": f"Bearer {resource_seed['worker_token']}"}
    trial_id: UUID = resource_seed["trial_id"]
    with TestClient(app) as client:
        first = client.put(
            f"/trials/{trial_id}/resource-usage",
            headers=headers,
            json=_payload(resource_seed, seq=1, final=False, cpu=10, memory_current=100),
        )
        final = client.put(
            f"/trials/{trial_id}/resource-usage",
            headers=headers,
            json=_payload(resource_seed, seq=2, final=True, cpu=20, memory_current=80),
        )
        duplicate = client.put(
            f"/trials/{trial_id}/resource-usage",
            headers=headers,
            json=_payload(resource_seed, seq=1, final=False, cpu=5, memory_current=150),
        )
    assert first.status_code == final.status_code == duplicate.status_code == 200
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            record = session.scalars(
                select(TrialResourceUsage).where(TrialResourceUsage.trial_id == trial_id),
            ).one()
            assert record.observation_seq == 2
            assert record.cpu_usage_usec == 20
            assert record.memory_current_bytes == 80
            assert record.completeness == "complete"
            assert record.finalized_at is not None
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_service_read_surface_enforces_team_and_reports_legacy_absence(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    resource_seed: dict[str, Any],
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "x",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)
    settings = LoomServiceSettings(_env_file=None)
    app = create_service_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    transport = httpx.ASGITransport(app=app)
    trial_id = resource_seed["trial_id"]
    batch_id = resource_seed["batch_id"]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        own = await client.get(
            f"/api/v1/trials/{trial_id}/resource-usage",
            headers={"Authorization": f"Bearer {resource_seed['team_token']}"},
        )
        other = await client.get(
            f"/api/v1/trials/{trial_id}/resource-usage",
            headers={"Authorization": f"Bearer {resource_seed['other_token']}"},
        )
        batch = await client.get(
            f"/api/v1/batches/{batch_id}/resource-usage",
            headers={"Authorization": f"Bearer {resource_seed['team_token']}"},
        )
        summary = await client.get(
            f"/api/v1/batches/{batch_id}/resource-usage/summary",
            headers={"Authorization": f"Bearer {resource_seed['team_token']}"},
        )
    await engine.dispose()
    assert summary.status_code == 200
    assert summary.json() == {
        "cpu_usage_usec": None,
        "largest_trial_memory_peak_bound_bytes": None,
        "largest_trial_disk_peak_bound_bytes": None,
        "sampled_trial_count": 0,
        "trial_count": 1,
        "last_observed_at": None,
    }
    assert own.status_code == 200
    assert own.json()["items"] == []
    assert own.json()["aggregate"]["telemetry_status"] == "unavailable"
    assert other.status_code == 403
    assert batch.status_code == 200
    assert batch.json()["batch_id"] == str(batch_id)
    assert batch.json()["trials_with_telemetry"] == 0
    assert batch.json()["aggregate"]["telemetry_status"] == "unavailable"
