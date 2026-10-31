import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import TaskImageMaterialization, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "C" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-16T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _set_cp_env(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    *,
    environment: str = "development",
) -> None:
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
        "LOOM_ENV": environment,
        "LOOM_LOCAL_EXECUTION": "1",
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def legacy_admin_seed(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"loom_admin_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="admin", scopes=["admin:tokens"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
):
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    yield create_app(ControlPlaneSettings(_env_file=None))


def test_issue_worker_token(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )
        assert r.status_code == 201
        body = r.json()
        assert body["token"].startswith("loom_w_")
        assert "token_hash_prefix" in body


@pytest.mark.parametrize("environment,enabled", [("production", "1"), ("development", "0")])
def test_issue_worker_token_rejects_hosted_or_disabled_local_execution(
    app,  # type: ignore[no-untyped-def]
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
    enabled: str,
) -> None:
    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", enabled)
    with TestClient(app) as client:
        response = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 30},
        )
    assert response.status_code == 409
    assert "local development" in response.json()["detail"]


def test_issue_batch_runner_token(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        r = client.post(
            "/admin/batch-runner-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201
    body = r.json()
    assert body["token"].startswith("loom_br_")
    assert "token_hash_prefix" in body

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        row = s.execute(
            select(Token).where(
                Token.token_hash == hashlib.sha256(
                    body["token"].encode(),
                ).digest(),
            ),
        ).scalar_one()
    engine.dispose()
    assert row.type == "worker"
    assert row.team_id is None
    assert row.scopes == ["submit:batch"]


def test_issue_family_orchestrator_token(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        r = client.post(
            "/admin/family-orchestrator-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["token"].startswith("loom_fo_")

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        row = s.execute(
            select(Token).where(
                Token.token_hash == hashlib.sha256(body["token"].encode()).digest(),
            ),
        ).scalar_one()
    engine.dispose()
    assert row.type == "family_orchestrator"
    assert row.team_id is None
    assert row.scopes == ["family:cancel", "family:evolve"]


def test_issue_worker_token_accepts_singleton_admin_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url, environment="development")
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    admin_app = create_app(ControlPlaneSettings(_env_file=None))

    with TestClient(admin_app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["token"].startswith("loom_w_")
    assert "token_hash_prefix" in body


def test_issue_task_image_builder_token_is_least_privilege(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/admin/task-image-builder-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 30},
        )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["token"].startswith("loom_tib_")

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(Token).where(
                    Token.token_hash == hashlib.sha256(body["token"].encode()).digest(),
                )
            )
            assert row is not None
            assert row.type == "worker"
            assert row.scopes == ["task-image:build"]
            assert row.team_id is None
            assert row.expires_at is not None
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "payload",
    [
        {"expires_in_days": 0},
        {"expires_in_days": "30"},
        {"expires_in_days": 30, "scopes": ["worker:claim"]},
    ],
)
def test_issue_task_image_builder_token_rejects_unsafe_payloads(
    app,  # type: ignore[no-untyped-def]
    payload: dict[str, object],
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/admin/task-image-builder-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=payload,
        )

    assert response.status_code == 422


def test_issue_task_image_registry_gc_token_is_least_privilege(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/admin/task-image-registry-gc-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 30},
        )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["token"].startswith("loom_tigc_")

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(Token).where(
                    Token.token_hash == hashlib.sha256(body["token"].encode()).digest(),
                )
            )
            assert row is not None
            assert row.type == "worker"
            assert row.scopes == ["task-image:gc"]
            assert row.team_id is None
            assert row.expires_at is not None
    finally:
        engine.dispose()


def test_admin_can_requeue_exhausted_task_image_materialization(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    materialization_id = uuid4()
    task_id = f"admin-task-image-retry/{materialization_id}"
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.add(
                TaskImageMaterialization(
                    id=materialization_id,
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="a" * 64,
                    cpu_arch="x86_64",
                    task_config={},
                    state="failed",
                    attempt_count=3,
                    failure_reason="registry_unavailable",
                    failure_message="attempts exhausted",
                    finished_at=datetime.now(UTC),
                )
            )
            session.commit()

        with TestClient(app) as client:
            response = client.post(
                f"/admin/task-image-materializations/{materialization_id}/retry",
                headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            )

        assert response.status_code == 200, response.text
        assert response.json()["state"] == "queued"
        with sessionmaker(engine)() as session:
            row = session.get(TaskImageMaterialization, materialization_id)
            assert row is not None
            assert row.state == "queued"
            assert row.attempt_count == 0
            assert row.failure_reason is None
            assert row.failure_message is None
            assert row.finished_at is None
    finally:
        with sessionmaker(engine)() as session:
            row = session.get(TaskImageMaterialization, materialization_id)
            if row is not None:
                session.delete(row)
                session.commit()
        engine.dispose()


def test_admin_retry_rejects_active_task_image_materialization(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    materialization_id = uuid4()
    task_id = f"admin-task-image-active/{materialization_id}"
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.add(
                TaskImageMaterialization(
                    id=materialization_id,
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="b" * 64,
                    cpu_arch="x86_64",
                    task_config={},
                    state="running",
                    claimed_by="builder-active",
                    lease_epoch=1,
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
            session.commit()

        with TestClient(app) as client:
            response = client.post(
                f"/admin/task-image-materializations/{materialization_id}/retry",
                headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            )

        assert response.status_code == 409
        assert "cannot be retried" in response.json()["detail"]
    finally:
        with sessionmaker(engine)() as session:
            row = session.get(TaskImageMaterialization, materialization_id)
            if row is not None:
                session.delete(row)
                session.commit()
        engine.dispose()


def test_revoke_token(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        prefix = r.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r2.status_code == 200


def test_revoke_token_accepts_singleton_admin_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url, environment="development")
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    admin_app = create_app(ControlPlaneSettings(_env_file=None))

    with TestClient(admin_app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        assert r.status_code == 201, r.text
        prefix = r.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )

    assert r2.status_code == 200, r2.text


def test_issue_without_admin_scope_rejected(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": "Bearer wrong"},
            json={"expires_in_days": 1},
        )
        assert r.status_code == 403


@pytest.mark.parametrize("bad_prefix", [
    "abc",  # too short
    "ZZZZ",  # non-hex (uppercase, and also non-hex letters)
    "abcg",  # 'g' not hex
    "ab--",  # punctuation
    "abc!",  # punctuation
    "a" * 65,  # too long
])
def test_revoke_rejects_invalid_prefix(  # type: ignore[no-untyped-def]
    app, bad_prefix,
):
    """Bug 3 regression: revoke prefix must be 4-64 hex chars. Anything
    else — including the empty / wildcard / punctuation that would feed
    straight into a SQL LIKE — must be rejected with 400 BEFORE the
    UPDATE runs."""
    with TestClient(app) as client:
        r = client.delete(
            f"/admin/worker-tokens/{bad_prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r.status_code == 400, r.text


def test_revoke_doesnt_affect_unrelated_tokens(  # type: ignore[no-untyped-def]
    app,
):
    """Bug 3 regression: a precise prefix must only revoke matching tokens
    (not all of them). We issue two worker tokens, revoke one by its
    prefix, then confirm the admin token is still usable."""
    with TestClient(app) as client:
        r1 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        prefix_1 = r1.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix_1}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r2.status_code == 200
        # admin token still works
        r3 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        assert r3.status_code == 201


def test_legacy_db_admin_token_rejected(app, legacy_admin_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {legacy_admin_seed}"},
            json={"expires_in_days": 1},
        )

    assert r.status_code == 403
