"""Admin batch admission pause and resume routes (#2777)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import AdminAuditEvent, Batch, Task, Team, TeamQuota, Token, Trial
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "P" * 43


@pytest.fixture
async def admission_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, list[UUID]]]:
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
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    team_id, batch_id, task_id = uuid4(), uuid4(), f"admission-pause/{uuid4().hex[:12]}"
    trial_ids = [uuid4(), uuid4()]
    team_raw = f"loom_team_{uuid4().hex}"
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"admission-pause-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(team_raw.encode()).digest(),
            type="team", scopes=["read:own", "submit"], team_id=team_id, issued_at=now,
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="c" * 64, config={"schema_version": "1", "task": {"id": task_id}},
        ))
        s.execute(insert(Batch).values(
            id=batch_id, team_id=team_id, name="admission pause", task_filter={},
            trial_config={}, backend="nebius", state="running",
            created_by_token_prefix="test", expected_trial_count=len(trial_ids),
        ))
        for trial_id in trial_ids:
            s.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id=task_id, batch_id=batch_id,
                config={"agent_name": "test", "agent_model": None},
                requires_caps={"os": "linux"}, state="queued", attempt_count=0,
                scheduling_observation={"reason": "capacity_wait"},
            ))
        s.commit()
    try:
        yield app, team_raw, batch_id, trial_ids
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(AdminAuditEvent).where(AdminAuditEvent.target_id == str(batch_id)))
            s.execute(delete(Trial).where(Trial.batch_id == batch_id))
            s.execute(delete(Batch).where(Batch.id == batch_id))
            s.execute(delete(Task).where(Task.id == task_id))
            s.execute(delete(Token).where(Token.team_id == team_id))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()
        sync_engine.dispose()


def _admin_headers(actor: str = "incident-commander") -> dict[str, str]:
    return {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}", "X-Loom-Admin-Actor": actor}


async def test_pause_and_resume_admission_are_idempotent_and_audited(
    admission_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, team_raw, batch_id, _ = admission_setup
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        base = f"/api/v1/admin/batches/{batch_id}"
        denied = await ac.post(
            f"{base}/pause-admission", headers={"Authorization": f"Bearer {team_raw}"}, json={},
        )
        paused = await ac.post(
            f"{base}/pause-admission", headers=_admin_headers(),
            json={"reason": "  provider incident  "},
        )
        repeated = await ac.post(
            f"{base}/pause-admission", headers=_admin_headers(), json={"reason": "other"},
        )
        detail = await ac.get(f"/api/v1/batches/{batch_id}", headers=_admin_headers())
        resumed = await ac.post(f"{base}/resume-admission", headers=_admin_headers())
        resumed_again = await ac.post(f"{base}/resume-admission", headers=_admin_headers())
        missing = await ac.post(
            f"/api/v1/admin/batches/{uuid4()}/pause-admission", headers=_admin_headers(),
        )

    assert denied.status_code in (401, 403), denied.text
    assert paused.status_code == 200, paused.text
    first = paused.json()
    assert first["changed"] is True and first["state"] == "running"
    assert first["admission_paused_at"] is not None
    assert first["admission_paused_reason"] == "provider incident"
    assert (first["held_count"], first["pending_release_count"]) == (2, 0)
    assert repeated.status_code == 200, repeated.text
    again = repeated.json()
    # A repeated pause keeps the original pause time and reason.
    assert again["changed"] is False
    assert again["admission_paused_at"] == first["admission_paused_at"]
    assert again["admission_paused_reason"] == "provider incident"
    assert detail.status_code == 200, detail.text
    assert detail.json()["admission_paused_at"] is not None
    assert detail.json()["admission_held_count"] == 2
    assert resumed.status_code == 200 and resumed.json()["changed"] is True
    assert resumed.json()["admission_paused_at"] is None
    assert resumed_again.status_code == 200 and resumed_again.json()["changed"] is False
    assert missing.status_code == 404

    sessions = app.state.session_factory
    async with sessions() as session:
        batch = await session.get(Batch, batch_id)
        assert batch is not None and batch.state == "running"
        assert batch.admission_paused_at is None and batch.admission_paused_reason is None
        events = (await session.scalars(
            select(AdminAuditEvent)
            .where(AdminAuditEvent.target_id == str(batch_id))
            .order_by(AdminAuditEvent.created_at, AdminAuditEvent.id)
        )).all()
    assert [event.action for event in events] == [
        "batch.admission.pause",
        "batch.admission.pause",
        "batch.admission.resume",
        "batch.admission.resume",
    ]
    assert {event.actor for event in events} == {"incident-commander"}
    assert events[0].event_metadata["already_paused"] is False
    assert events[1].event_metadata["already_paused"] is True
    assert events[3].event_metadata["already_resumed"] is True
    assert events[0].event_metadata["held_count"] == 2
    for event in events:
        assert "provider incident" not in str(event.event_metadata)


async def test_resume_clears_only_admission_pause_hints(
    admission_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    from loom_control_plane.batch_admission import mark_paused_queued_trials

    app, _, batch_id, trial_ids = admission_setup
    sessions = app.state.session_factory
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        base = f"/api/v1/admin/batches/{batch_id}"
        assert (await ac.post(f"{base}/pause-admission", headers=_admin_headers())).status_code == 200
        async with sessions.begin() as session:
            assert await mark_paused_queued_trials(
                session, batch_id=batch_id, now=datetime.now(UTC),
            ) == 2
            await session.execute(update(Trial).where(Trial.id == trial_ids[1]).values(
                scheduling_observation={"reason": "capacity_wait"},
            ))
        assert (await ac.post(f"{base}/resume-admission", headers=_admin_headers())).status_code == 200
    async with sessions() as session:
        cleared = await session.get(Trial, trial_ids[0])
        untouched = await session.get(Trial, trial_ids[1])
        assert cleared is not None and cleared.scheduling_observation is None
        assert untouched is not None
        assert untouched.scheduling_observation == {"reason": "capacity_wait"}


@pytest.mark.parametrize("state", ["cancelling", "cancelled", "finished"])
async def test_pause_rejects_batches_that_no_longer_admit(
    admission_setup: tuple[FastAPI, str, UUID, list[UUID]], state: str,
) -> None:
    app, _, batch_id, _ = admission_setup
    async with app.state.session_factory.begin() as session:
        await session.execute(update(Batch).where(Batch.id == batch_id).values(state=state))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        response = await ac.post(
            f"/api/v1/admin/batches/{batch_id}/pause-admission", headers=_admin_headers(),
        )
        resumed = await ac.post(
            f"/api/v1/admin/batches/{batch_id}/resume-admission", headers=_admin_headers(),
        )
    assert response.status_code == 409, response.text
    assert state in response.json()["detail"]
    # Resume stays available so a pause never outlives its batch.
    assert resumed.status_code == 200 and resumed.json()["changed"] is False
