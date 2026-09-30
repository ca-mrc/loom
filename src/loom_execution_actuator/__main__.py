from __future__ import annotations

import asyncio
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime

import uvicorn
from fastapi import FastAPI, Response, status
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema_startup import assert_schema_at_head
from loom.execution_image_admission import ImageAdmissionKeyring
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController
from loom_execution_actuator.pool_build_selection import PoolBuildSelector
from loom_execution_actuator.pool_client import PoolClient
from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
from loom_execution_actuator.pool_execution_outbox import PoolExecutionOutbox
from loom_execution_actuator.pool_execution_resources import PoolExecutionResources
from loom_execution_actuator.pool_outbox import PoolBuildOutbox
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_controller import (
    NativeBuildKubernetesApi,
    NativeTaskImageController,
)

_LOG = logging.getLogger(__name__)


@dataclass
class ActuatorRuntimeHealth:
    stale_after_seconds: float
    task_image_builder_enabled: bool = False
    global_build_enabled: bool = False
    started_at: float = field(default_factory=time.monotonic)
    last_command_success: float | None = None
    last_reconcile_success: float | None = None
    last_build_success: float | None = None
    last_build_heartbeat_success: float | None = None

    def mark_success(self, loop: str) -> None:
        if loop == "command":
            self.last_command_success = time.monotonic()
        elif loop == "reconcile":
            self.last_reconcile_success = time.monotonic()
        elif loop == "build":
            self.last_build_success = time.monotonic()
        elif loop == "build_heartbeat":
            self.last_build_heartbeat_success = time.monotonic()

    def ready(self) -> bool:
        now = time.monotonic()
        observations = [self.last_command_success, self.last_reconcile_success]
        if self.task_image_builder_enabled:
            observations.append(self.last_build_success)
        if self.global_build_enabled:
            observations.append(self.last_build_heartbeat_success)
        return all(
            observed is not None and now - observed <= self.stale_after_seconds
            for observed in observations
        )


def _health_app(runtime_health: ActuatorRuntimeHealth) -> FastAPI:
    app = FastAPI(title="Loom Execution Actuator", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready(response: Response) -> dict[str, str]:
        if not runtime_health.ready():
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "not_ready"}
        return {"status": "ready"}

    app.mount("/metrics", make_asgi_app())
    return app


async def _command_loop(
    actuator: ExecutionActuator,
    poll_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        try:
            await actuator.run_commands_once()
            runtime_health.mark_success("command")
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator command loop iteration failed")
        await asyncio.sleep(poll_seconds)


async def _reconcile_loop(
    actuator: ExecutionActuator,
    interval_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        started = datetime.now(UTC)
        try:
            await actuator.reconcile_full_once(now=started)
            runtime_health.mark_success("reconcile")
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator full reconciliation failed")
        elapsed = (datetime.now(UTC) - started).total_seconds()
        await asyncio.sleep(max(0.25, interval_seconds - elapsed))


async def _watch_loop(actuator: ExecutionActuator, timeout_seconds: int) -> None:
    retry_seconds = 0.25
    while True:
        try:
            await actuator.watch_once(timeout_seconds=timeout_seconds)
            retry_seconds = 0.25
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator watch iteration failed")
            await asyncio.sleep(retry_seconds)
            retry_seconds = min(30.0, retry_seconds * 2)
            continue
        await asyncio.sleep(0.25)


async def _build_loop(
    builder: NativeTaskImageController | PoolNativeBuildController,
    poll_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        try:
            await builder.run_once()
            runtime_health.mark_success("build")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # SDK errors may contain credential-bearing URLs or request bodies.
            _LOG.error("native task image reconciliation failed (%s)", type(error).__name__)
        await asyncio.sleep(poll_seconds)


async def _build_heartbeat_loop(
    builder: PoolNativeBuildController,
    poll_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        try:
            await builder.heartbeat_once()
            runtime_health.mark_success("build_heartbeat")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _LOG.error("global native build heartbeat failed (%s)", type(error).__name__)
        await asyncio.sleep(poll_seconds)


async def _run() -> None:
    settings = ExecutionActuatorSettings()
    async with AsyncExitStack() as resources:
        engine = create_async_engine(settings.db_url, pool_pre_ping=True)
        resources.push_async_callback(engine.dispose)
        await assert_schema_at_head(engine, db_url_env_var="LOOM_EXECUTION_ACTUATOR_DB_URL")
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        kubernetes = InClusterKubernetesJobApi(connection=settings.kubernetes_connection)
        resources.push_async_callback(kubernetes.close)
        target = ExecutionTargetRuntime(
            target_id=settings.target_id,
            namespace=settings.namespace,
            runtime_class_name=settings.runtime_class_name,
            node_selector=settings.node_selector,
            tolerations=settings.tolerations,
            service_account_name=settings.service_account_name,
            credential_broker_url=settings.credential_broker_url,
            pod_identity_audience=settings.pod_identity_audience,
        )
        pool, management, pool_resources = settings.global_pool, None, None
        if pool is not None:
            management = PoolClient(origin=pool.management_origin, bearer_token_file=pool.bearer_token_file,
                timeout_seconds=pool.timeout_seconds)
            resources.push_async_callback(management.close)
            outbox = PoolExecutionOutbox(sessions=sessions, participant=pool.participant,
                environment=pool.environment, logical_pool_id=pool.logical_pool_id,
                image_admission_keyring=ImageAdmissionKeyring.from_json(settings.execution_image_admission_public_keys_json))
            pool_resources = PoolExecutionResources(driver=PoolExecutionDriver(outbox=outbox, management=management),
                kubernetes=kubernetes, target_id=target.target_id)
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=target,
            controller_id=settings.controller_id,
            command_limit=settings.command_limit,
            command_lease_seconds=settings.command_lease_seconds,
            delete_grace_seconds=settings.delete_grace_seconds,
            pool_resources=pool_resources,
        )
        builder: NativeTaskImageController | PoolNativeBuildController | None = None
        if settings.task_image_builder is not None:
            build_kubernetes = NativeBuildKubernetesApi(connection=settings.kubernetes_connection)
            resources.push_async_callback(build_kubernetes.close)
            if pool is not None:
                assert management is not None
                build_outbox = PoolBuildOutbox(sessions=sessions, participant=pool.participant,
                    logical_pool_id=pool.logical_pool_id, builder_id=settings.controller_id + "-native")
                builder = PoolNativeBuildController(driver=PoolBuildDriver(outbox=build_outbox, management=management),
                    kubernetes=build_kubernetes, selector=PoolBuildSelector(outbox=build_outbox,
                        target_id=target.target_id, deadline_seconds=settings.task_image_builder.active_deadline_seconds))
            else:
                builder = NativeTaskImageController(sessions=sessions, kubernetes=build_kubernetes,
                    target=target, settings=settings.task_image_builder)
        runtime_health = ActuatorRuntimeHealth(
            stale_after_seconds=max(15.0, settings.full_reconcile_seconds * 3),
            task_image_builder_enabled=builder is not None,
            global_build_enabled=isinstance(builder, PoolNativeBuildController),
        )
        server = uvicorn.Server(uvicorn.Config(_health_app(runtime_health), host=settings.health_host,
            port=settings.health_port, log_level="info"))
        tasks = [
            asyncio.create_task(server.serve()),
            asyncio.create_task(_command_loop(actuator, settings.poll_seconds, runtime_health)),
            asyncio.create_task(_reconcile_loop(actuator, settings.full_reconcile_seconds, runtime_health)),
        ]
        if pool is None:
            tasks.append(asyncio.create_task(_watch_loop(actuator, settings.watch_timeout_seconds)))
        if builder is not None:
            tasks.append(asyncio.create_task(_build_loop(builder, settings.poll_seconds, runtime_health)))
        if isinstance(builder, PoolNativeBuildController):
            tasks.append(asyncio.create_task(_build_heartbeat_loop(builder, min(10.0, settings.poll_seconds), runtime_health)))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            # Drain every sibling even when one failed. The original gather's
            # exception propagates only after shared resources are safe to close.
            await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
