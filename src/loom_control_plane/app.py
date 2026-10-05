"""Control Plane app factory."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier, load_optional_admin_secret_verifier
from loom.db.schema_startup import assert_schema_at_head
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.service_execution_backend import local_execution_enabled
from loom.storage_credentials import build_s3_client
from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import (
    CheckpointRouteService,
    ExecutionAttemptCompletionService,
    FinalOutputRouteService,
    SqlArtifactCommitRepository,
    SqlArtifactInputResolver,
)
from loom_control_plane.artifact_read_service import ArtifactReadService
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.live_preview import run_live_preview_reconciler_loop
from loom_control_plane.metrics_refresher import run_metrics_refresher_loop
from loom_control_plane.object_version_recovery import ObjectVersionRecovery
from loom_control_plane.retry_exhausted_sweeper import (
    run_retry_exhausted_sweeper_loop,
)
from loom_control_plane.routes import (
    admin,
    artifacts,
    execution_attempts,
    health,
    object_version_recovery,
    pipeline_catalog,
    resource_usage,
    service_executions,
    state,
    step_tokens,
    task_image_execution,
    task_image_materializations,
    tasks,
    trajectory,
    trial_cache,
    trials,
    workers,
)
from loom_control_plane.scheduler.crash_detector import run_crash_detector_loop
from loom_control_plane.service_execution_materializer import (
    ServiceExecutionMaterializer,
    run_service_execution_materializer_loop,
)
from loom_control_plane.service_execution_scheduler import (
    run_service_execution_scheduler_loop,
)
from loom_control_plane.task_image_execution import (
    TaskImageExecutionService,
    configured_execution_service,
)
from loom_control_plane.task_lifecycle import cancel_and_drain_tasks as _cancel_and_drain_tasks
from loom_execution_actuator.pool_client import PoolClient
from loom_execution_actuator.pool_execution_outbox import PoolExecutionOutbox
from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
from loom_task_image_authority.execution_config import load_execution_admission_settings


def _load_admin_secret_verifier(
    settings: ControlPlaneSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for Control Plane startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_CP_DB_URL")


def create_app(
    settings: ControlPlaneSettings, *,
    task_image_execution_factory: Callable[[AsyncEngine], TaskImageExecutionService] | None = None,
) -> FastAPI:
    execution_config_file = settings.task_image_execution_config_file
    if task_image_execution_factory is not None and execution_config_file is not None:
        raise ValueError("execution configuration conflicts with injected factory")
    execution_config = load_execution_admission_settings(execution_config_file) if execution_config_file is not None else None
    source_config = ServiceExecutionSourceConfig.from_settings(settings)

    @asynccontextmanager
    async def application_lifespan(app: FastAPI, resources: AsyncExitStack) -> AsyncIterator[None]:
        engine = create_async_engine(
            settings.db_engine_url,
            connect_args=settings.db_engine_connect_args,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout_sec,
        )
        resources.push_async_callback(engine.dispose)
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        admin_secret_verifier = _load_admin_secret_verifier(settings)


        minio_client = build_s3_client(
            endpoint_url=settings.minio_endpoint,
            auth_kind=settings.storage_auth_kind,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            region=settings.minio_region,
        )

        app.state.settings = settings
        app.state.session_factory = session_factory
        app.state.task_image_execution = (
            task_image_execution_factory(engine) if task_image_execution_factory is not None else None
        )
        if execution_config is not None:
            app.state.task_image_execution = await resources.enter_async_context(configured_execution_service(engine, execution_config))
        app.state.admin_secret_verifier = admin_secret_verifier
        app.state.minio_client = minio_client

        artifact_store = MinioObjectStore(
            endpoint_url=settings.minio_endpoint,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            region=settings.minio_region,
        )
        source_store = (
            source_config.build_store(MinioObjectStore) if source_config else artifact_store
        )
        source_bucket = source_config.bucket if source_config else settings.artifacts_bucket
        artifact_repository = SqlArtifactCommitRepository(
            session_factory=session_factory,
            store=artifact_store,
            bucket=settings.artifacts_bucket,
        )
        artifact_commit_service = ArtifactCommitService(
            store=artifact_store,
            bucket=settings.artifacts_bucket,
            repository=artifact_repository,
        )
        app.state.final_output_service = FinalOutputRouteService(
            service=artifact_commit_service,
            session_factory=session_factory,
        )
        app.state.checkpoint_service = CheckpointRouteService(
            service=artifact_commit_service,
            session_factory=session_factory,
        )
        app.state.execution_attempt_completion_service = ExecutionAttemptCompletionService()
        app.state.artifact_read_service = ArtifactReadService(
            resolver=SqlArtifactInputResolver(
                session_factory=session_factory,
                store=artifact_store,
                bucket=settings.artifacts_bucket,
            ),
            store=artifact_store,
            bucket=settings.artifacts_bucket,
        )

        global_selector = None
        if settings.service_execution_scheduler_enabled:
            image_keyring = ImageAdmissionKeyring.from_json(settings.execution_image_admission_public_keys_json)
            pool = settings.global_pool
            if pool is not None:
                management = PoolClient(origin=pool.management_origin, bearer_token_file=pool.bearer_token_file,
                    timeout_seconds=pool.timeout_seconds)
                resources.push_async_callback(management.close)
                global_selector = PoolExecutionSelector(outbox=PoolExecutionOutbox(
                    sessions=session_factory, participant=pool.participant, environment=pool.environment,
                    logical_pool_id=pool.logical_pool_id, image_admission_keyring=image_keyring,
                    maximum_deadline_seconds=settings.service_execution_scheduler_max_deadline_sec),
                    allocation_reader=management)

        background_tasks: list[asyncio.Task[None]] = []
        service_execution_materializer_stop_event: asyncio.Event | None = None

        async def stop_background() -> None:
            if service_execution_materializer_stop_event is not None:
                service_execution_materializer_stop_event.set()
            await _cancel_and_drain_tasks(background_tasks)

        # Register teardown before spawning: partial startup must drain work
        # before the management client, signer or database engines are closed.
        resources.push_async_callback(stop_background)
        # Worker heartbeat recovery belongs to the explicitly enabled local
        # execution path. Hosted executions are reconciled through their leases.
        if local_execution_enabled():
            crash_detector_task = asyncio.create_task(
                run_crash_detector_loop(
                    session_factory=session_factory,
                    expiry_sec=settings.worker_heartbeat_expiry_sec,
                    interval_sec=settings.worker_reclaim_sweep_interval_sec,
                    claimed_without_start_expiry_sec=(settings.claimed_without_start_expiry_sec),
                    running_stale_timeout_multiplier=(
                        settings.stale_running_trial_timeout_multiplier
                        if settings.stale_running_trial_reclaim_enabled
                        else None
                    ),
                    running_stale_grace_sec=settings.stale_running_trial_grace_sec,
                    running_stale_silence_sec=settings.stale_running_trial_silence_sec,
                ),
                name="loom-cp-crash-detector",
            )
            background_tasks.append(crash_detector_task)
        # Background refresher for gauge metrics (workers_active,
        # queue_depth, trials_inflight). See metrics_refresher.py
        # for the cadence rationale.
        metrics_refresher_task = asyncio.create_task(
            run_metrics_refresher_loop(
                session_factory=session_factory,
                expiry_sec=settings.worker_heartbeat_expiry_sec,
                interval_sec=30,
            ),
            name="loom-cp-metrics-refresher",
        )
        background_tasks.append(metrics_refresher_task)
        # Background sweep that transitions queued trials with
        # attempt_count >= team_quotas.max_attempts_ceiling to state='failed' with
        # failure_reason='retry_exhausted'. This also serves native executions
        # and remains active when local worker recovery is disabled.
        retry_exhausted_task = asyncio.create_task(
            run_retry_exhausted_sweeper_loop(
                session_factory=session_factory,
                interval_sec=settings.worker_reclaim_sweep_interval_sec,
            ),
            name="loom-cp-retry-exhausted-sweeper",
        )
        background_tasks.append(retry_exhausted_task)
        live_preview_reconciler_task = asyncio.create_task(
            run_live_preview_reconciler_loop(
                session_factory=session_factory,
                interval_sec=30,
            ),
            name="loom-cp-live-preview-reconciler",
        )
        background_tasks.append(live_preview_reconciler_task)
        service_execution_scheduler_task: asyncio.Task[None] | None = None
        if settings.service_execution_scheduler_enabled:
            service_execution_scheduler_task = asyncio.create_task(
                run_service_execution_scheduler_loop(
                    session_factory=session_factory,
                    environment=settings.service_execution_scheduler_environment,
                    pool_id=settings.service_execution_scheduler_pool_id,
                    image_admission_keyring=image_keyring,
                    interval_seconds=settings.service_execution_scheduler_interval_sec,
                    maximum_deadline_seconds=(
                        settings.service_execution_scheduler_max_deadline_sec
                    ),
                    global_selector=global_selector,
                ),
                name="loom-cp-service-execution-scheduler",
            )
            background_tasks.append(service_execution_scheduler_task)
        service_execution_materializer_task: asyncio.Task[None] | None = None
        if settings.service_execution_materializer_enabled:
            service_execution_materializer_stop_event = asyncio.Event()
            service_execution_materializer_task = asyncio.create_task(
                run_service_execution_materializer_loop(
                    materializer=ServiceExecutionMaterializer(
                        session_factory=session_factory,
                        source_store=source_store,
                        source_bucket=source_bucket,
                        canonical_store=artifact_store,
                        artifacts_bucket=settings.artifacts_bucket,
                        trajectories_bucket=settings.trajectories_bucket,
                        claim_ttl_seconds=(settings.service_execution_materializer_claim_ttl_sec),
                        source_retention_seconds=(settings.service_execution_source_retention_sec),
                    ),
                    interval_seconds=settings.service_execution_materializer_interval_sec,
                    concurrency=settings.service_execution_materializer_concurrency,
                    stop_event=service_execution_materializer_stop_event,
                ),
                name="loom-cp-service-execution-materializer",
            )
            background_tasks.append(service_execution_materializer_task)
        yield

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as resources:
            async with application_lifespan(app, resources):
                yield

    app = FastAPI(
        title="Loom Control Plane",
        version="0.0.1",
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(pipeline_catalog.router)
    app.include_router(trials.router)
    app.include_router(resource_usage.router)
    app.include_router(service_executions.router)
    if local_execution_enabled():
        app.include_router(workers.router)
        app.include_router(task_image_materializations.router)
    app.include_router(state.router)
    app.include_router(trajectory.router)
    app.include_router(artifacts.router)
    app.include_router(execution_attempts.router)
    app.include_router(tasks.router)
    app.state.object_version_recovery = ObjectVersionRecovery()
    app.include_router(admin.router)
    app.include_router(object_version_recovery.router)
    app.include_router(step_tokens.router)
    app.include_router(trial_cache.router)
    app.include_router(task_image_execution.router)
    # /metrics: standard prometheus_client ASGI app. Mounted at the
    # top-level for prometheus scrapers (operator-supplied
    # ServiceMonitor / PodMonitor uses the default `/metrics` path).
    # The CP service is internal (not exposed via Ingress); scrapers
    # reach it through cluster DNS.
    app.mount("/metrics", make_asgi_app())
    return app
