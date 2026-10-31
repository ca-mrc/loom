import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def artifact_seed(
    postgres_url: str,
) -> Iterator[tuple[UUID, UUID, UUID, UUID, str, str, str]]:
    """Seeds two teams + tokens + trials so Bug 6 (cross-team blocking) can
    be exercised without relying on order-of-tests.

    Yields:
        team_a_id, team_b_id, trial_a_id, trial_b_id, raw_team_a, raw_team_b,
        raw_worker
    """
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_a = uuid4()
    team_b = uuid4()
    trial_a = uuid4()
    trial_b = uuid4()
    raw_a = f"a_{uuid4().hex}"
    raw_b = f"b_{uuid4().hex}"
    raw_w = f"w_{uuid4().hex}"
    with session_factory() as s:
        for tid in (team_a, team_b):
            s.execute(insert(Team).values(id=tid, name=f"team-{tid}"))
            s.execute(insert(TeamQuota).values(team_id=tid))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_a.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_a,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_b.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_b,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_w.encode()).digest(),
            type="worker", scopes=["worker:index"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_a, team_id=team_a, task_id="t",
            config={}, requires_caps={}, state="queued",
        ))
        s.execute(insert(Trial).values(
            id=trial_b, team_id=team_b, task_id="t",
            config={}, requires_caps={}, state="queued",
        ))
        s.commit()
    try:
        yield team_a, team_b, trial_a, trial_b, raw_a, raw_b, raw_w
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    artifact_seed: tuple[UUID, UUID, UUID, UUID, str, str, str],
):
    # Route tests keep the real lifespan; independent background workers have dedicated coverage.
    for name in (
        "run_crash_detector_loop", "run_metrics_refresher_loop",
        "run_retry_exhausted_sweeper_loop", "run_live_preview_reconciler_loop",
    ):
        monkeypatch.setattr(f"loom_control_plane.app.{name}", AsyncMock(return_value=None))
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_signed_url_format(app, artifact_seed):  # type: ignore[no-untyped-def]
    team_a, _, trial_a, _, raw_a, _, _ = artifact_seed
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"trial_id": str(trial_a), "key": "step/out.json"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["url"].startswith("http")
        assert body["expires_in_sec"] > 0
        assert body["key"] == f"{team_a}/{trial_a}/step/out.json"
        assert body["bucket"] == "artifacts"


@pytest.mark.usefixtures("app")
def test_signed_url_uses_configured_artifacts_bucket(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    artifact_seed: tuple,
):  # type: ignore[no-untyped-def]
    _, _, trial_a, _, raw_a, _, _ = artifact_seed
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
        "LOOM_CP_ARTIFACTS_BUCKET": "loom-staging-artifacts",
        "LOOM_CP_TRAJECTORIES_BUCKET": "loom-staging-trajectories",
    }.items():
        monkeypatch.setenv(k, v)
    configured = create_app(ControlPlaneSettings(_env_file=None))
    with TestClient(configured) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"trial_id": str(trial_a), "key": "step/out.json"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["bucket"] == "loom-staging-artifacts"


def test_signed_url_rejects_unauth(app, artifact_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            json={"trial_id": str(uuid4()), "key": "x"},
        )
        assert r.status_code == 401


@pytest.mark.parametrize("bad_key", [
    "../escape",
    "a/../b",
    "./local",
    "/absolute",
    "a//b",
    "",
    "trailing/..",
    "with\x00nul",
])
def test_signed_url_rejects_path_traversal(  # type: ignore[no-untyped-def]
    app, artifact_seed, bad_key,
):
    """Bug 2 regression: keys with .. / . / leading / / NUL / empty
    segments are rejected with 400 so a team A token can't mint a URL that
    presigns against team B's prefix on the wire."""
    _, _, trial_a, _, raw_a, _, _ = artifact_seed
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"trial_id": str(trial_a), "key": bad_key},
        )
        assert r.status_code == 400, r.text


def test_team_token_cant_upload_to_other_teams_trial(  # type: ignore[no-untyped-def]
    app, artifact_seed,
):
    """Bug 6 regression: team A's token presigning against team B's trial
    must be refused with 403 even though the wire key would land under
    team A's own prefix."""
    _, _, _, trial_b, raw_a, _, _ = artifact_seed
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"trial_id": str(trial_b), "key": "out.json"},
        )
        assert r.status_code == 403
        assert "another team" in r.json()["detail"]


def test_worker_token_resolves_team_from_trial(  # type: ignore[no-untyped-def]
    app, artifact_seed,
):
    """Worker tokens have no team_id, so they must look up the trial's
    team and embed that in the key path."""
    team_a, _, trial_a, _, _, _, raw_w = artifact_seed
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_w}"},
            json={"trial_id": str(trial_a), "key": "out.json"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["key"] == f"{team_a}/{trial_a}/out.json"


def test_worker_token_404s_unknown_trial(  # type: ignore[no-untyped-def]
    app, artifact_seed,
):
    _, _, _, _, _, _, raw_w = artifact_seed
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw_w}"},
            json={"trial_id": str(uuid4()), "key": "out.json"},
        )
        assert r.status_code == 404
