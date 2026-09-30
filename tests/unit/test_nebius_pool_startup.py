"""Protected runtime settings select one authority and independent maintenance."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from tests.unit.test_nebius_pool_execution_render import inputs


def configuration(tmp_path):
    participant, _ = inputs()
    participant = participant.model_copy(update={"environment_class": "development", "targets": (
        participant.targets[0].model_copy(update={"workload_kinds": ("trial", "verifier", "task_image_build")}),)})
    token = tmp_path / "pool-token"
    token.write_text("private-test-token")
    token.chmod(0o600)
    return {"participant": participant.model_dump(mode="json"), "environment": "development",
        "logical_pool_id": "nebius-cpu", "management_origin": "https://management.example",
        "bearer_token_file": str(token), "timeout_seconds": 5}


@pytest.mark.parametrize("damage", ["environment", "url", "execution-namespace", "build-namespace", "target"])
def test_global_actuator_configuration_rejects_cross_binding_before_startup(tmp_path, damage):
    from loom_execution_actuator.config import ExecutionActuatorSettings

    config = configuration(tmp_path)
    participant = config["participant"]
    target = participant["targets"][0]["target_id"]
    namespace = participant["execution_namespace"]["name"]
    build_namespace = participant["build_namespace"]["name"]
    if damage == "environment":
        config["environment"] = "production"
    elif damage == "url":
        config["management_origin"] = "http://management.example"
    elif damage == "execution-namespace":
        namespace = "foreign"
    elif damage == "build-namespace":
        build_namespace = "foreign"
    else:
        target = "foreign"
    with pytest.raises(ValueError):
        ExecutionActuatorSettings(_env_file=None, db_url="postgresql+psycopg://unused/unused",
            controller_id="test", target_id=target, namespace=namespace, global_pool=config,
            task_image_builder={"namespace": build_namespace, "service_image": "registry.example/service@sha256:" + "a" * 64,
                "storage_endpoint": "https://storage.example", "storage_region": "eu-north1", "source_bucket": "source",
                "registry_repository": "registry.example/task-images"})


@pytest.mark.parametrize("shutdown", ["cancel", "loop_failure", "server_exit"])
async def test_actual_actuator_entrypoint_selects_global_readers_without_watch_and_schedules_heartbeat(tmp_path, monkeypatch, shutdown):
    from loom_execution_actuator import __main__ as entrypoint
    from loom_execution_actuator.config import ExecutionActuatorSettings
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    loop_failure = shutdown == "loop_failure"
    config = configuration(tmp_path)
    participant = config["participant"]
    settings = ExecutionActuatorSettings(_env_file=None, db_url="postgresql+psycopg://unused/unused", controller_id="global",
        target_id=participant["targets"][0]["target_id"], namespace=participant["execution_namespace"]["name"],
        global_pool=config, task_image_builder={"namespace": participant["build_namespace"]["name"],
            "service_image": "registry.example/service@sha256:" + "a" * 64, "storage_endpoint": "https://storage.example",
            "storage_region": "eu-north1", "source_bucket": "source", "registry_repository": "registry.example/task-images"})
    observed, closed = {}, []
    started, stop = asyncio.Event(), asyncio.Event()
    server_stop = asyncio.Event()
    fail, cleanup_started, cleanup_allowed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ExternalResource:
        def __init__(self, name):
            self.name = name

        async def close(self):
            closed.append(self.name)

        async def dispose(self):
            await self.close()

    async def schema(*_args, **_kwargs):
        pass

    async def serve():
        await server_stop.wait()

    def loop(name):
        async def run(controller, interval, health):
            observed[name] = (controller, interval, health)
            if len(observed) == 4:
                started.set()
            try:
                if loop_failure and name == "command":
                    await fail.wait()
                    raise RuntimeError("test controller loop failed")
                await stop.wait()
            finally:
                if loop_failure and name != "command":
                    cleanup_started.set()
                    await cleanup_allowed.wait()
        return run

    async def forbidden_watch(*_args, **_kwargs):
        raise AssertionError("global startup scheduled the legacy namespace watch")

    monkeypatch.setattr(entrypoint, "ExecutionActuatorSettings", lambda: settings)
    monkeypatch.setattr(entrypoint, "create_async_engine", lambda *_a, **_k: ExternalResource("db"))
    monkeypatch.setattr(entrypoint, "assert_schema_at_head", schema)
    monkeypatch.setattr(entrypoint, "InClusterKubernetesJobApi", lambda **_: ExternalResource("execution-reader"))
    monkeypatch.setattr(entrypoint, "NativeBuildKubernetesApi", lambda **_: ExternalResource("build-reader"))
    monkeypatch.setattr(entrypoint.uvicorn, "Server", lambda _: SimpleNamespace(serve=serve))
    for name, label in (("_command_loop", "command"), ("_reconcile_loop", "reconcile"),
                        ("_build_loop", "build"), ("_build_heartbeat_loop", "heartbeat")):
        monkeypatch.setattr(entrypoint, name, loop(label), raising=False)
    monkeypatch.setattr(entrypoint, "_watch_loop", forbidden_watch)
    task = asyncio.create_task(entrypoint._run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        actuator = observed["command"][0]
        builder = observed["build"][0]
        assert actuator is observed["reconcile"][0] and actuator._pool_resources is not None
        assert isinstance(builder, PoolNativeBuildController) and builder is observed["heartbeat"][0]
        assert builder.selector is not None and builder.driver.management is actuator._pool_resources.driver.management
        assert observed["heartbeat"][1] <= 10
        health = observed["command"][2]
        for name in ("command", "reconcile", "build"):
            health.mark_success(name)
        assert not health.ready()
        health.mark_success("build_heartbeat")
        assert health.ready()
        if loop_failure:
            fail.set()
            await asyncio.wait_for(cleanup_started.wait(), timeout=5)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.02)
            assert not closed  # Shared clients must outlive every settling loop.
            cleanup_allowed.set()
            with pytest.raises(RuntimeError, match="test controller loop failed"):
                await asyncio.wait_for(asyncio.shield(task), timeout=1)
        elif shutdown == "server_exit":
            server_stop.set()
            await asyncio.wait_for(asyncio.shield(task), timeout=1)
    finally:
        cleanup_allowed.set()
        server_stop.set()
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert sorted(closed) == ["build-reader", "db", "execution-reader"]
    assert actuator._pool_resources.driver.management._closed


def test_control_plane_global_configuration_requires_matching_scheduler_identity(tmp_path):
    import json

    from loom_control_plane.config import ControlPlaneSettings

    config = configuration(tmp_path)
    common = {"_env_file": None, "db_url": "postgresql+psycopg://unused/unused", "minio_endpoint": "http://minio.example",
        "minio_access_key": "test", "minio_secret_key": "test", "step_jwt_signing_key": "test-signing-key",
        "service_execution_global_pool_json": json.dumps(config)}
    configured = ControlPlaneSettings(**common)
    assert str(configured.global_pool.participant.participant_id) == config["participant"]["participant_id"]
    with pytest.raises(ValueError):
        ControlPlaneSettings(**common, service_execution_scheduler_environment="production")
    with pytest.raises(ValueError):
        ControlPlaneSettings(**common, service_execution_scheduler_pool_id="foreign")


def test_actual_control_plane_startup_selects_global_queue_and_closes_its_client(tmp_path, monkeypatch):
    import json

    from fastapi.testclient import TestClient

    from loom_control_plane import app as entrypoint
    from loom_control_plane.config import ControlPlaneSettings
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector

    observed = {}

    class Engine:
        async def dispose(self):
            pass

    async def schema(_engine):
        return 0

    async def background(**kwargs):
        if "global_selector" in kwargs:
            observed.update(kwargs)
        try:
            await asyncio.Event().wait()
        finally:
            if kwargs.get("global_selector") is not None:
                observed["closed_before_scheduler_stopped"] = kwargs["global_selector"].allocation_reader._closed

    monkeypatch.setattr(entrypoint, "_assert_schema_startup", schema)
    monkeypatch.setattr(entrypoint, "create_async_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(entrypoint, "build_s3_client", lambda **_: object())
    for name in ("run_crash_detector_loop", "run_metrics_refresher_loop", "run_retry_exhausted_sweeper_loop",
        "run_live_preview_reconciler_loop", "run_service_execution_scheduler_loop", "run_service_execution_materializer_loop"):
        monkeypatch.setattr(entrypoint, name, background)
    settings = ControlPlaneSettings(_env_file=None, db_url="postgresql+psycopg://unused/unused",
        minio_endpoint="http://minio.example", minio_access_key="test", minio_secret_key="test",
        step_jwt_signing_key="test-signing-key", service_execution_scheduler_enabled=True,
        service_execution_global_pool_json=json.dumps(configuration(tmp_path)))
    with TestClient(entrypoint.create_app(settings)) as http:
        assert http.get("/healthz").status_code == 200
        selected = observed["global_selector"]
        assert isinstance(selected, PoolExecutionSelector)
        assert selected.outbox.environment == "development" and selected.allocation_reader is not None
        assert not selected.allocation_reader._closed
    assert selected.allocation_reader._closed
    assert not observed["closed_before_scheduler_stopped"]


async def test_global_mode_rejects_direct_admin_reservation_before_local_admission(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from loom_control_plane.routes import service_executions

    async def authenticated(*_args):
        pass

    def forbidden_session():
        raise AssertionError("global admin reservation opened a local admission session")

    monkeypatch.setattr(service_executions, "_admin", authenticated)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        settings=SimpleNamespace(global_pool=configuration(tmp_path)), session_factory=forbidden_session)))
    with pytest.raises(HTTPException) as caught:
        await service_executions.create_execution_reservation(body=None, request=request)
    assert caught.value.status_code == 409
