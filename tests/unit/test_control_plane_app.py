from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient


class _FakeEngine:
    async def dispose(self) -> None:
        pass


@pytest.mark.parametrize(
    ("environment", "local_opt_in", "expect_worker_recovery"),
    [("production", "0", False), ("production", "1", False),
     ("development", "0", False), ("development", "1", True)],
)
def test_startup_runs_native_execution_without_retired_autoscalers(
    monkeypatch: pytest.MonkeyPatch,
    environment: str, local_opt_in: str, expect_worker_recovery: bool,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", local_opt_in)
    started: set[str] = set()

    async def schema_ready(_engine: object) -> int:
        return 0

    async def background(**_kwargs: object) -> None:
        task = asyncio.current_task()
        assert task is not None
        started.add(task.get_name())
        await asyncio.Event().wait()

    monkeypatch.setattr(control_plane_app, "_load_admin_secret_verifier", lambda _: None)
    monkeypatch.setattr(control_plane_app, "_assert_schema_startup", schema_ready)
    monkeypatch.setattr(control_plane_app, "create_async_engine", lambda *_a, **_kw: _FakeEngine())
    monkeypatch.setattr(control_plane_app, "build_s3_client", lambda **_: object())
    for name in (
        "run_crash_detector_loop", "run_metrics_refresher_loop",
        "run_retry_exhausted_sweeper_loop", "run_live_preview_reconciler_loop",
        "run_service_execution_scheduler_loop", "run_service_execution_materializer_loop",
        "run_worker_pool_autoscaler_loop", "run_elastic_slurm_worker_controller_loop",
    ):
        monkeypatch.setattr(control_plane_app, name, background, raising=False)
    settings = ControlPlaneSettings(
        _env_file=None,
        db_url="postgresql+psycopg://loom:loom@example/loom",
        minio_endpoint="http://minio.example",
        minio_access_key="minio-access",
        minio_secret_key="minio-secret",
        step_jwt_signing_key="test-step-jwt-signing-key",
        service_execution_scheduler_enabled=True,
    )
    with TestClient(control_plane_app.create_app(settings)) as client:
        assert client.get("/healthz").status_code == 200
    assert ("loom-cp-crash-detector" in started) is expect_worker_recovery
    assert "loom-cp-retry-exhausted-sweeper" in started
    assert "loom-cp-live-preview-reconciler" in started
    assert "loom-cp-service-execution-scheduler" in started
    assert "loom-cp-worker-pool-autoscaler" not in started
    assert "loom-cp-elastic-slurm-worker-controller" not in started


@pytest.mark.asyncio
async def test_background_task_drain_follow_up_cancels_all_pending_tasks_together() -> None:
    from loom_control_plane import app as control_plane_app

    drain_tasks = control_plane_app._cancel_and_drain_tasks
    second_cancellations: set[int] = set()
    all_second_cancellations = asyncio.Event()

    async def cancellation_resistant_task(index: int) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                second_cancellations.add(index)
                if len(second_cancellations) == 3:
                    all_second_cancellations.set()
                await all_second_cancellations.wait()
                raise

    tasks = tuple(
        asyncio.create_task(cancellation_resistant_task(index)) for index in range(3)
    )
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(
            drain_tasks(tasks, grace_seconds=0.01),
            timeout=0.5,
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert second_cancellations == {0, 1, 2}
    assert all(task.done() for task in tasks)


def test_control_plane_lifespan_configures_db_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    create_engine_calls: list[dict[str, Any]] = []

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _background_noop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    def _create_async_engine(_db_url: str, **kwargs: Any) -> _FakeEngine:
        create_engine_calls.append(kwargs)
        return _FakeEngine()

    monkeypatch.setattr(
        control_plane_app,
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        control_plane_app,
        "create_async_engine",
        _create_async_engine,
    )
    monkeypatch.setattr(
        control_plane_app,
        "build_s3_client",
        lambda **_: object(),
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_crash_detector_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_metrics_refresher_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_retry_exhausted_sweeper_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_service_execution_materializer_loop",
        _background_noop,
    )

    app = control_plane_app.create_app(
        ControlPlaneSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            step_jwt_signing_key="test-step-jwt-signing-key",
            db_pool_size=33,
            db_max_overflow=44,
            db_pool_timeout_sec=55.5,
        ),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert create_engine_calls == [
        {
            "connect_args": {},
            "pool_pre_ping": True,
            "pool_size": 33,
            "max_overflow": 44,
            "pool_timeout": 55.5,
        }
    ]


def test_control_plane_lifespan_signals_materializer_before_cancelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    materializer_stop_states: list[bool] = []

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _background_noop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    async def _materializer_background(**kwargs: object) -> None:
        stop_event = kwargs.get("stop_event")
        try:
            await asyncio.Event().wait()
        finally:
            materializer_stop_states.append(
                isinstance(stop_event, asyncio.Event) and stop_event.is_set()
            )

    monkeypatch.setattr(control_plane_app, "_assert_schema_startup", _schema_noop)
    monkeypatch.setattr(control_plane_app, "create_async_engine", lambda *_a, **_kw: _FakeEngine())
    monkeypatch.setattr(control_plane_app, "build_s3_client", lambda **_: object())
    monkeypatch.setattr(control_plane_app, "run_crash_detector_loop", _background_noop)
    monkeypatch.setattr(control_plane_app, "run_metrics_refresher_loop", _background_noop)
    monkeypatch.setattr(control_plane_app, "run_retry_exhausted_sweeper_loop", _background_noop)
    monkeypatch.setattr(
        control_plane_app,
        "run_service_execution_materializer_loop",
        _materializer_background,
    )

    app = control_plane_app.create_app(
        ControlPlaneSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            step_jwt_signing_key="test-step-jwt-signing-key",
        ),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert materializer_stop_states == [True]
