"""Real S3 faults reach ordinary Trial diagnostics through the worker and CP."""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete

from loom.db.schema import Task, Token, Trial, Worker
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient
from loom_worker.main_loop import _spawn_trial
from loom_worker.runner_pool import RunnerPool
from loom_worker.vllm_registry import WorkerVLLMRegistry
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401

_SECRET_SENTINEL = "setup-secret-do-not-publish"


@pytest.fixture
def faulty_s3() -> Iterator[tuple[MinioObjectStore, threading.Event, threading.Event]]:
    requested, release = threading.Event(), threading.Event()

    class FaultHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requested.set()
            release.wait(15)
            body = (
                "<Error><Code>AccessDenied</Code><Message>"
                f"Authorization: Bearer {_SECRET_SENTINEL}</Message></Error>"
            ).encode()
            try:
                self.send_response(403)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), FaultHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield MinioObjectStore(
            endpoint_url=f"http://127.0.0.1:{server.server_port}",
            access_key="test", secret_key="test", operation_attempts=1,
            read_timeout=5, operation_timeout=6,
        ), requested, release
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize("fault", ["hang", "denied"])
async def test_s3_setup_failure_is_bounded_and_visible_to_user(
    trials_setup, tmp_path: Path, fault: str, monkeypatch: pytest.MonkeyPatch, faulty_s3,  # noqa: F811
):
    app, team_token, _, trial_ids = trials_setup
    store, requested, release = faulty_s3
    trial_id = uuid4()
    worker_id = uuid4()
    worker_token = f"setup-worker-{uuid4().hex}"
    token_hash = hashlib.sha256(worker_token.encode()).digest()
    pool = RunnerPool(max_concurrent=1)
    now = datetime.now(UTC)
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, trial_ids[0])
        task = await session.get(Task, seed.task_id)
        task.config = {
            "schema_version": "1", "task": {"id": task.id, "name": "setup fault"},
            "environment": {"os": "linux", "docker_image": "alpine"},
            "agent": {"name": "oracle"}, "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
        task.source = "s3://setup-fixture/private-task/"
        session.add(Worker(id=worker_id, hostname="setup-test", version="test",
                           capabilities=[], registered_at=now, last_seen_at=now,
                           status="active"))
        session.add(Token(token_hash=token_hash, type="worker", scopes=["worker:report"],
                          issued_at=now))
        await session.flush()
        trial = Trial(id=trial_id, task_id=task.id, team_id=seed.team_id,
                      submitted_by_user_id=seed.submitted_by_user_id,
                      state="claimed", config={}, requires_caps={})
        session.add(trial)
        trial.worker_id, trial.claimed_at = worker_id, now
        trial.started_at = trial.finished_at = None
        trial.attempt_count = 1
        trial.config = {"agent_name": "oracle", "agent_model": None,
                        "retry": {"max_attempts": 1, "retry_on": []}}
        payload = {"trial_id": str(trial_id), "team_id": str(trial.team_id),
                   "task_id": task.id, "attempt_count": 1, "config": trial.config}

    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    cp_settings = ControlPlaneSettings(
        _env_file=None, db_url=str(app.state.settings.db_url),
        minio_access_key="test", minio_secret_key="test",
    )
    cp_app = create_app(cp_settings)
    cp_app.state.settings = cp_settings
    cp_app.state.session_factory = app.state.session_factory
    settings = WorkerSettings(
        _env_file=None, token=worker_token, minio_access_key="test", minio_secret_key="test",
        trajectory_cache_dir=tmp_path / "trajectories",
        task_materialize_timeout_sec=2 if fault == "hang" else 10,
        pre_start_heartbeat_interval_sec=0.02,
    )
    try:
        async with (
            httpx.AsyncClient(transport=httpx.ASGITransport(app=cp_app), base_url="http://cp") as cp_http,
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc",
                              headers={"Authorization": f"Bearer {team_token}"}) as user,
        ):
            cp = HttpControlPlaneClient(base_url="http://cp", token=worker_token, _client=cp_http)
            await _spawn_trial(
                pool=pool, settings=settings, cp_client=cp, gateway_client=None,
                object_store=store, worker_id=worker_id, payload=payload,
                vllm_registry=WorkerVLLMRegistry(enabled=False),
            )
            assert await asyncio.to_thread(requested.wait, 3), "worker did not reach S3"
            response = await user.get(f"/api/v1/trials/{trial_id}")
            assert response.status_code == 200, response.text
            assert response.json()["state"] == "claimed"
            assert response.json()["started_at"] is None
            if fault == "denied":
                release.set()
            await pool.wait_all(timeout=5)
            assert pool.in_flight == 0
            assert not list(Path(tempfile.gettempdir()).glob(f"loom-trial-{trial_id}-*"))
            response = await user.get(f"/api/v1/trials/{trial_id}")
            assert response.status_code == 200, response.text
            detail = response.json()
            assert detail["state"] == "failed", detail
            assert detail["started_at"] is None
            assert detail["finished_at"] is not None
            assert detail["failure_reason"] == "internal_error"
            assert "task materialization" in detail["failure_message"]
            assert "source_scheme=s3" in detail["failure_message"]
            assert _SECRET_SENTINEL not in response.text
            assert "private-task" not in detail["failure_message"]
            if fault == "hang":
                assert "timed out after 2s" in detail["failure_message"]
            else:
                assert "AccessDenied" in detail["failure_message"]
            listed = await user.get("/api/v1/trials", params={"state": "failed"})
            assert listed.status_code == 200, listed.text
            item = next(item for item in listed.json()["items"] if item["id"] == str(trial_id))
            assert item["failure_message"] == detail["failure_message"]
            async with app.state.session_factory() as session:
                trial = await session.get(Trial, trial_id)
                assert trial.pre_start_heartbeat_at is not None
                worker = await session.get(Worker, worker_id)
                assert worker.status == "active"
    finally:
        release.set()
        pool.cancel_all()
        await pool.wait_all(timeout=3)
        async with app.state.session_factory() as session, session.begin():
            trial = await session.get(Trial, trial_id)
            trial.worker_id = None
            await session.flush()
            await session.execute(delete(Worker).where(Worker.id == worker_id))
            await session.execute(delete(Token).where(Token.token_hash == token_hash))
