"""FastAPI factory for loom_service (spec §2).

Stateless service. The lifespan opens a per-process async SQLAlchemy
engine, a boto3 S3 client for internal object-store operations, and an
httpx AsyncClient pointed at the Control Plane (for Plan 18+ forwarders).
Routes pull these off `request.app.state`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.admin_secret import (
    AdminSecretVerifier,
    load_optional_admin_secret_verifier,
)
from loom.data_lifecycle_capacity import StagingAdmissionError
from loom.db.schema_startup import assert_schema_at_head
from loom.security.secret_store import assert_existing_secrets_decryptable
from loom.service_execution_backend import local_execution_enabled
from loom.service_execution_materialization import load_service_execution_runtime_profile
from loom.startup_retry import retry_startup_dependency
from loom.system_identities import assert_pipeline_controller_identity
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom.workload_trust import WorkloadTrustContract
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.application_management.service_runtime import ApplicationServiceRuntime
from loom_service.application_management.source_upload import ApplicationSourceUploader
from loom_service.batch_runner import run_loop as batch_run_loop
from loom_service.behavior_pipeline_adapter import install_behavior_pipeline_public_adapter
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.child import load_child_registration
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.environment_management.registry import ManagementError
from loom_service.environment_management.runtime import EnvironmentRuntime
from loom_service.metrics import (
    HTTP_REQUEST_LATENCY_SEC,
    HTTP_REQUESTS_TOTAL,
)
from loom_service.pipeline_control_bindings import SqlPipelineRecipeBindingResolver
from loom_service.provider_secret_gc import run_loop as provider_secret_gc_run_loop
from loom_service.routes import (
    admin_audit,
    agents,
    applications,
    atif,
    auth,
    backends,
    batches,
    benchmarks,
    delivery_exports,
    environments,
    health,
    invites,
    managed_child,
    management_health,
    models,
    monitor,
    overview,
    pipeline,
    platform_admins,
    provider_connections,
    rate_cards,
    run_library,
    secret_store_admin,
    tasks,
    tasksets,
    team_registrations,
    teams,
    terminalgen_corpora,
    tokens,
    trajectory,
    trials,
    usage,
)
from loom_service.session_auth import (
    browser_origin_allowed,
)
from loom_service.storage import create_minio_client
from loom_service.taskset_gc import run_loop as taskset_gc_run_loop
from loom_service.taskset_materializer import run_loop as taskset_materializer_run_loop


def _load_admin_secret_verifier(
    settings: LoomServiceSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for loom_service startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


def _validated_v1_workload_contract(
    settings: LoomServiceSettings,
) -> WorkloadTrustContract:
    """Return the deployment workload contract or reject an invalid v1 startup."""
    contract = settings.workload_contract
    violations = contract.v1_violations()
    if violations:
        raise RuntimeError(
            "invalid v1 workload trust contract: " + "; ".join(violations),
        )
    return contract


_NATIVE_BUILDER_DIGEST = re.compile(r"[0-9a-f]{64}")
_NATIVE_BUILDER_KEY_ID = re.compile(r"[a-z][a-z0-9._-]{0,63}")
_NATIVE_BUILDER_IMAGE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}"
)






async def _assert_secret_store_startup(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        count = await assert_existing_secrets_decryptable(session)
        await assert_pipeline_controller_identity(session)
        return count


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_SVC_DB_URL")


def register_api_routes(
    app: FastAPI, *, management: bool = False, include_local_execution: bool = True,
) -> None:
    """Share route registration with credential-free offline OpenAPI export."""
    app.include_router(management_health.router if management else health.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(auth.admin_router, prefix="/api/v1")
    app.include_router(invites.router, prefix="/api/v1")
    app.include_router(tokens.router, prefix="/api/v1")
    app.include_router(admin_audit.router, prefix="/api/v1")
    app.include_router(platform_admins.router, prefix="/api/v1")
    app.include_router(team_registrations.router, prefix="/api/v1")
    app.include_router(teams.router, prefix="/api/v1")
    if management:
        from loom_service.pool_management.routes import router as pool_observer_router

        app.include_router(environments.router, prefix="/api/v1")
        app.include_router(applications.router, prefix="/api/v1")
        app.include_router(pool_observer_router)
    if not management:
        app.include_router(managed_child.router, prefix="/api/v1")
        for workload_router in (
            trials.router, trajectory.router, atif.router, tasks.router, benchmarks.router,
            tasksets.router, terminalgen_corpora.router, batches.router, delivery_exports.router,
            run_library.router, rate_cards.router, usage.router, agents.router, models.router,
            monitor.router, overview.router, pipeline.router, backends.router,
            provider_connections.router, secret_store_admin.router,
        ):
            app.include_router(workload_router, prefix="/api/v1")
        if include_local_execution:
            app.include_router(pipeline.local_execution_router, prefix="/api/v1")


def create_app(settings: LoomServiceSettings) -> FastAPI:
    management = settings.service_mode == "management"
    pool_profiles = None
    if settings.pool_profiles_file is not None:
        from loom_service.pool_management.profiles import load_pool_profiles

        pool_profiles = load_pool_profiles(settings.pool_profiles_file)
    child_registration = load_child_registration(settings)
    workload_contract = None if management else _validated_v1_workload_contract(settings)
    # Fail deployment health immediately rather than discovering a malformed
    # automatic-execution profile on the first user Batch.
    if not management:
        load_service_execution_runtime_profile(settings.service_execution_runtime_profile_json)

    @asynccontextmanager
    async def _management_lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(
            settings.db_engine_url, connect_args=settings.db_engine_connect_args,
        )
        app.state._owned_service_engine = engine
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async def validate_secrets() -> int:
            async with session_factory() as session:
                return await assert_existing_secrets_decryptable(session)

        await retry_startup_dependency(
            validate_secrets, operation_name="management secret-store startup validation",
        )
        app.state.admin_secret_verifier = _load_admin_secret_verifier(settings)
        app.state.settings = settings
        app.state.session_factory = session_factory
        async with contextlib.AsyncExitStack() as resources:
            if pool_profiles is not None:
                app.state.pool_profiles = pool_profiles
            if settings.environment_management_config_file is not None:
                installation = ManagementInstallation.load(settings.environment_management_config_file)
                client = httpx.AsyncClient(trust_env=False, timeout=30, follow_redirects=False)
                app.state._owned_management_http_client = client
                assert settings.environment_management_github_token is not None
                app.state.environment_manager = await installation.manager(
                    session_factory, http=client, token=settings.environment_management_github_token.get_secret_value(),
                )
                if installation.provider_runtime is not None:
                    app.state.environment_runtime = await resources.enter_async_context(EnvironmentRuntime.open(
                        installation.provider_runtime, app.state.environment_manager.registry, child_http=client,
                    ))
                if installation.applications is not None:
                    application = installation.applications
                    manager = ApplicationManager(ApplicationRegistry(session_factory), foundation=installation.foundation,
                        shared=application.shared, authority=application.authority, releases=application.releases,
                        builds=ApplicationBuildRegistry(session_factory, binding=application.runtime.build.binding)
                            if application.runtime.build is not None else None)
                    runtime = await resources.enter_async_context(ApplicationServiceRuntime.open(application, manager,
                        pool_profiles=pool_profiles, management_origin=str(settings.public_base_url)))
                    app.state.application_runtime = runtime
                    app.state.application_manager = manager
                    app.state.application_login = runtime.login
                    if runtime.source_uploader is not None:
                        app.state.application_source_uploader = runtime.source_uploader
                    if runtime.build_worker is not None:
                        app.state.application_build_registry = runtime.build_worker.journal.registry
            try:
                yield
            finally:
                if hasattr(app.state, "environment_runtime"):
                    del app.state.environment_runtime
                if hasattr(app.state, "application_manager"):
                    del app.state.application_manager
                if hasattr(app.state, "application_runtime"):
                    del app.state.application_runtime
                if hasattr(app.state, "application_login"):
                    del app.state.application_login
                if hasattr(app.state, "application_source_uploader"):
                    del app.state.application_source_uploader
                if hasattr(app.state, "application_build_registry"):
                    del app.state.application_build_registry

    @asynccontextmanager
    async def _service_lifespan(app: FastAPI) -> AsyncIterator[None]:
        assert workload_contract is not None
        # Validate deterministic URL shape before opening database, mTLS, or
        # HTTP resources so a startup rejection cannot leak any of them.
        gw_path = settings.gateway_url.path or "/"
        if gw_path not in ("", "/"):
            raise RuntimeError(
                f"LOOM_SVC_GATEWAY_URL must not include a path prefix "
                f"(got {settings.gateway_url!s}). Forwarders use "
                f"absolute paths; a prefix would be silently dropped."
            )
        engine = create_async_engine(
            settings.db_engine_url,
            connect_args=settings.db_engine_connect_args,
        )
        app.state._owned_service_engine = engine
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await retry_startup_dependency(
            lambda: _assert_secret_store_startup(session_factory),
            operation_name="service secret-store startup validation",
        )
        admin_secret_verifier = _load_admin_secret_verifier(settings)
        minio_client = create_minio_client(
            settings,
            endpoint_url=settings.minio_endpoint,
        )
        app.state._owned_service_minio_client = minio_client
        http_client = httpx.AsyncClient(
            base_url=str(settings.control_plane_url),
            timeout=10.0,
        )
        app.state._owned_service_http_client = http_client

        # Plan 20: separate httpx client for the Gateway. Rate-card
        # routes proxy to /admin/rate-cards on the Gateway; we keep
        # CP + Gateway clients independent so a slow CP doesn't
        # starve the rate-card surface.
        # Path-prefix sanity check: a base_url with a non-root path
        # (e.g. `https://gw/loom/`) silently strips the prefix when
        # the forwarder uses absolute paths like `/admin/rate-cards`.
        # Fail at startup rather than route to the wrong URL.
        gateway_client = httpx.AsyncClient(
            base_url=str(settings.gateway_url),
            timeout=10.0,
        )
        app.state._owned_service_gateway_client = gateway_client

        app.state.settings = settings
        app.state.session_factory = session_factory
        pipeline_binding_resolver = SqlPipelineRecipeBindingResolver(session_factory)
        app.state.pipeline_binding_resolver = pipeline_binding_resolver
        app.state.pipeline_judge_profile_reader = pipeline_binding_resolver
        app.state.admin_secret_verifier = admin_secret_verifier
        app.state.minio_client = minio_client
        app.state.http_client = http_client
        app.state.gateway_client = gateway_client
        if local_execution_enabled():
            install_behavior_pipeline_public_adapter(app=app, settings=settings)

        # Personal HTTP processes share the data plane, but do not own its
        # workers. Keep schema/auth/client initialization and request behavior
        # above this boundary; every background loop belongs below it.
        # The outer lifespan still closes this process's clients and engine.
        if settings.service_mode == "api_only":
            yield
            return

        # Plan 19: batch runner background task. Picks up
        # submitted/running batches on each poll, fans out trial
        # submissions to Control Plane via the shared http_client.
        # The runner's CP token is read from settings — without it
        # every CP submit 401s, so the loop short-circuits with a
        # warning per tick and waits for the operator to provision
        # the secret.
        runner_token = (
            settings.batch_runner_cp_token.get_secret_value()
            if settings.batch_runner_cp_token is not None
            else None
        )
        runner_authorization = f"Bearer {runner_token}" if runner_token else None
        runner_task = asyncio.create_task(
            batch_run_loop(
                session_factory=session_factory,
                http_client=http_client,
                batch_size=settings.batch_runner_batch_size,
                submit_rate_per_sec=(settings.batch_runner_submit_rate_per_sec),
                poll_interval_sec=(settings.batch_runner_poll_interval_sec),
                cp_authorization=runner_authorization,
            ),
            name="loom-svc-batch-runner",
        )
        app.state.batch_runner_task = runner_task

        materializer_task = asyncio.create_task(
            taskset_materializer_run_loop(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=settings.artifacts_bucket,
                upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
                batch_size=settings.taskset_materializer_batch_size,
                poll_interval_sec=settings.taskset_materializer_poll_interval_sec,
                claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
                transform_config=TransformSandboxConfig(
                    enabled=settings.taskset_materializer_transforms_enabled,
                    network_isolated=settings.taskset_materializer_transform_network_isolated,
                    workload_contract=workload_contract,
                    wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
                    cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
                    memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
                ),
                max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
                max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
            ),
            name="loom-svc-taskset-materializer",
        )
        app.state.taskset_materializer_task = materializer_task

        gc_task = asyncio.create_task(
            taskset_gc_run_loop(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=settings.artifacts_bucket,
                retention_days=settings.taskset_gc_retention_days,
                poll_interval_sec=settings.taskset_gc_poll_interval_sec,
            ),
            name="loom-svc-taskset-gc",
        )
        app.state.taskset_gc_task = gc_task

        secret_gc_task = asyncio.create_task(
            provider_secret_gc_run_loop(session_factory=session_factory),
            name="loom-svc-provider-secret-gc",
        )
        app.state.provider_secret_gc_task = secret_gc_task
        from loom_service.price_catalogs import run_loop as catalog_sync_loop
        catalog_task = asyncio.create_task(catalog_sync_loop(session_factory=session_factory),
                                           name="loom-svc-price-catalogs")


        try:
            yield
        finally:
            catalog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await catalog_task
            runner_task.cancel()
            secret_gc_task.cancel()
            materializer_task.cancel()
            gc_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner_task
            with contextlib.suppress(asyncio.CancelledError):
                await materializer_task
            with contextlib.suppress(asyncio.CancelledError):
                await gc_task
            with contextlib.suppress(asyncio.CancelledError):
                await secret_gc_task

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Close every owned client even when startup rejects before yielding."""

        try:
            async with (_management_lifespan(app) if management else _service_lifespan(app)):
                yield
        finally:
            # SQLAlchemy engines can reconnect after dispose. Remove admission
            # access before closing its resources, including failed startup.
            if hasattr(app.state, "environment_manager"):
                del app.state.environment_manager
            if hasattr(app.state, "application_manager"):
                del app.state.application_manager
            if hasattr(app.state, "application_login"):
                del app.state.application_login
            if hasattr(app.state, "session_factory"):
                del app.state.session_factory
            if hasattr(app.state, "pool_profiles"):
                del app.state.pool_profiles
            for attribute in (
                "_owned_management_http_client",
                "_owned_service_gateway_client",
                "_owned_service_http_client",
            ):
                client = getattr(app.state, attribute, None)
                close = getattr(client, "aclose", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        await close()
            minio = getattr(app.state, "_owned_service_minio_client", None)
            close_minio = getattr(minio, "close", None)
            if callable(close_minio):
                with contextlib.suppress(Exception):
                    close_minio()
            owned_engine = getattr(app.state, "_owned_service_engine", None)
            dispose = getattr(owned_engine, "dispose", None)
            if callable(dispose):
                with contextlib.suppress(Exception):
                    await dispose()

    app = FastAPI(title="Loom Service", version="0.0.1", lifespan=lifespan)
    app.state.managed_environment = child_registration
    @app.exception_handler(ManagementError)
    async def _management_error(_request: Request, exc: ManagementError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={
            "detail": {"code": exc.code, **exc.details},
        }, headers={"Cache-Control": "no-store"})

    @app.exception_handler(StagingAdmissionError)
    async def _staging_admission_error(
        _request: Request,
        exc: StagingAdmissionError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "detail": {
                    "reason": str(exc),
                    "retryable": True,
                }
            },
        )

    @app.get("/", include_in_schema=False)
    async def _root() -> dict[str, object]:
        """Root landing — every other surface lives under `/api/v1/*`
        or `/docs`. Without this handler users hitting the bare URL
        see FastAPI's `{"detail": "Not Found"}` and assume the
        service is broken; return a tiny manifest of where to go
        next instead."""
        return {
            "service": "loom-service",
            "version": app.version,
            "links": {
                "swagger_ui": "/docs",
                "openapi_schema": "/openapi.json",
                "health": "/api/v1/health",
            },
            "note": (
                "API surface lives under /api/v1/* (most routes "
                "require a Bearer token; mint one with `loom service "
                "up` or via the admin tooling)."
            ),
        }

    register_api_routes(
        app, management=management, include_local_execution=local_execution_enabled(),
    )

    @app.middleware("http")
    async def _browser_origin_middleware(  # type: ignore[no-untyped-def]
        request: Request,
        call_next,
    ):
        """Require the configured origin for browser writes."""
        if not browser_origin_allowed(request, settings):
            return JSONResponse(
                status_code=403, content={"detail": "browser origin rejected"},
                headers={"Cache-Control": "no-store"},
            )
        return await call_next(request)

    @app.middleware("http")
    async def _metrics_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        """Observe every HTTP request once. Uses FastAPI's matched
        route template (e.g., `/api/v1/trials/{trial_id}`) as the
        label so cardinality is bounded by the route count, not the
        UUIDs in the URL."""
        import time as _time

        t0 = _time.perf_counter()
        response = await call_next(request)
        elapsed = _time.perf_counter() - t0
        # The matched route lands on request.scope["route"] for
        # APIRoute matches; missing for /metrics and 404s. Fall back
        # to the raw URL path in those cases (bounded — 404s typically
        # come from a small set of operator typos).
        route_obj = request.scope.get("route")
        route_path = getattr(route_obj, "path", None) or request.url.path
        status_class = f"{response.status_code // 100}xx"
        HTTP_REQUESTS_TOTAL.labels(
            route=route_path,
            method=request.method,
            status_class=status_class,
        ).inc()
        HTTP_REQUEST_LATENCY_SEC.labels(
            route=route_path,
            method=request.method,
        ).observe(elapsed)
        return response

    # /metrics: prometheus_client ASGI app. Note the Ingress only
    # routes `/api/v1/*` to loom-service (see
    # `src/loom_cli/templates/k8s/ingress.yaml.j2`), so `/metrics`
    # is NOT reachable from the public Internet — only from cluster
    # scrapers using the ClusterIP Service. The #78 slice C
    # NetworkPolicy on loom-service still allows any-namespace
    # ingress (the Ingress controller is in another namespace and
    # hard to label-select), so production scrapers should target
    # the Service's cluster DNS, not the public URL.
    app.mount("/metrics", make_asgi_app())
    if management:
        from loom_service.management_request_limits import ManagementRequestLimitsMiddleware

        # Last added is outermost: bound reception before any parser or auth.
        app.add_middleware(
            ManagementRequestLimitsMiddleware,
            max_body_bytes=settings.management_http_max_body_bytes,
            max_inflight=settings.management_http_max_inflight,
            body_timeout_sec=settings.management_http_body_timeout_sec,
            source_upload_enabled=lambda: isinstance(getattr(app.state, "application_source_uploader", None), ApplicationSourceUploader),
        )
    return app
