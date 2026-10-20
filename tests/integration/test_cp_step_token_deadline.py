from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, tzinfo
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.auth import verify_step_jwt
from loom.db.schema import Task, Team, TeamQuota, Token, Trial
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.routes import step_tokens
from loom_control_plane.routes.step_tokens import _execution_attempt_step_token_ttl


@pytest.fixture
def deadline_app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> Iterator[tuple[object, str, UUID, UUID]]:
    for key, value in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)

    raw_token = f"loom_w_{uuid4().hex}"
    token_hash = hashlib.sha256(raw_token.encode()).digest()
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"deadline-task-{uuid4().hex}"
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as session:
        session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["worker:report"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        session.execute(insert(Team).values(id=team_id, name=f"deadline-{team_id}"))
        session.execute(insert(TeamQuota).values(team_id=team_id))
        session.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        session.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="running",
            )
        )
        session.commit()

    app = create_app(ControlPlaneSettings(_env_file=None))
    try:
        yield app, raw_token, team_id, trial_id
    finally:
        with session_local() as session:
            session.execute(delete(Trial).where(Trial.id == trial_id))
            session.execute(delete(Token).where(Token.token_hash == token_hash))
            session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            session.execute(delete(Team).where(Team.id == team_id))
            session.execute(delete(Task).where(Task.id == task_id))
            session.commit()
        engine.dispose()


def test_endpoint_returns_signed_deadline_and_exact_expiry(deadline_app) -> None:  # type: ignore[no-untyped-def]
    app, raw_token, team_id, trial_id = deadline_app
    deadline = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60)
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 360,
                "attempt_deadline_wall_clock": deadline.isoformat(),
            },
        )

    assert response.status_code == 201, response.text
    body = response.json()
    context = verify_step_jwt(
        body["token"],
        signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
    )
    returned_expiry = datetime.fromisoformat(body["expires_at"])
    returned_deadline = datetime.fromisoformat(body["attempt_deadline_wall_clock"])
    assert context.expires_at == returned_expiry
    assert context.attempt_deadline_wall_clock == returned_deadline == deadline
    assert returned_expiry >= deadline + timedelta(seconds=300)


def test_real_trial_mints_distinct_grants_for_same_agent_attempt(deadline_app) -> None:  # type: ignore[no-untyped-def]
    app, raw_token, team_id, trial_id = deadline_app
    deadline = datetime.now(UTC) + timedelta(seconds=60)
    attempt_id = uuid4()
    with TestClient(app) as client:
        responses = [
            client.post(
                "/admin/step-tokens",
                headers={"Authorization": f"Bearer {raw_token}"},
                json={
                    "team_id": str(team_id),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "ttl_sec": 600,
                    "attempt_deadline_wall_clock": deadline.isoformat(),
                    "agent_attempt_id": str(attempt_id),
                },
            )
            for _ in range(2)
        ]
    for response in responses:
        assert response.status_code == 201
        body = response.json()
        context = verify_step_jwt(
            body["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert context.agent_attempt_id == attempt_id == UUID(body["agent_attempt_id"])
        assert context.step_jwt_id == UUID(body["step_jwt_id"])
        assert context.trial_id == trial_id
    assert responses[0].json()["step_jwt_id"] != responses[1].json()["step_jwt_id"]


def test_endpoint_rejects_numeric_monotonic_deadline(deadline_app) -> None:  # type: ignore[no-untyped-def]
    app, raw_token, team_id, trial_id = deadline_app
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 360,
                "attempt_deadline_wall_clock": 12345.0,
            },
        )

    assert response.status_code == 422


@pytest.fixture
def step_token_wall_clock(monkeypatch: pytest.MonkeyPatch) -> datetime:
    now = datetime.now(UTC).replace(microsecond=0)

    class RouteClock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return now.astimezone(tz) if tz is not None else now.replace(tzinfo=None)

    # Startup/authentication delays must not turn a deliberately short token
    # into one that covers the deadline; freeze only the owning route's clock.
    monkeypatch.setattr(step_tokens, "datetime", RouteClock)
    return now


def test_endpoint_rejects_expiry_without_deadline_grace(deadline_app, step_token_wall_clock) -> None:  # type: ignore[no-untyped-def]
    app, raw_token, team_id, trial_id = deadline_app
    deadline = step_token_wall_clock + timedelta(seconds=60)
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 350,
                "attempt_deadline_wall_clock": deadline.isoformat(),
            },
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "attempt_deadline_not_covered"


def test_endpoint_rejects_attempt_above_30000_second_token_ceiling(
    deadline_app, step_token_wall_clock,
) -> None:  # type: ignore[no-untyped-def]
    app, raw_token, team_id, trial_id = deadline_app
    deadline = step_token_wall_clock + timedelta(seconds=29_710)
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 30_000,
                "attempt_deadline_wall_clock": deadline.isoformat(),
            },
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "attempt_deadline_ttl_exceeded"


def test_execution_attempt_ttl_is_not_truncated_to_ceiling() -> None:
    with pytest.raises(HTTPException) as exc:
        _execution_attempt_step_token_ttl(29_701)

    assert exc.value.status_code == 409
    assert exc.value.detail == "execution_attempt_ttl_exceeded"
