"""FastAPI app factory."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta

import httpx
from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from starlette.responses import JSONResponse

from loom.admin_secret import AdminSecretVerifier, load_optional_admin_secret_verifier
from loom.db.schema_startup import assert_schema_at_head
from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.security.secret_store import assert_existing_secrets_decryptable
from loom.startup_retry import retry_startup_dependency
from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution_output import ServiceExecutionOutputRouteService
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.dispatch_audit import DispatchAuditMiddleware, DispatchAuditUnavailableError
from loom_llm_gateway.drain import ensure_drain_state, install_drain_middleware
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.pod_identity import ExecutionPodReviewer
from loom_llm_gateway.provider_dispatch import settle_stale_provider_dispatches
from loom_llm_gateway.rate_card import RateCardCache
from loom_llm_gateway.routes import (
    admin,
    chat,
    drain,
    facade_anthropic,
    facade_google,
    facade_openai,
    gemini,
    harbor_baseline,
    health,
    messages,
    responses,
    service_execution,
    task_egress,
)

logger = logging.getLogger(__name__)
_PROVIDER_DISPATCH_SWEEP_INTERVAL_SECONDS = 15.0
_PROVIDER_DISPATCH_SETTLEMENT_GRACE_SECONDS = 5.0


async def _reconcile_provider_dispatches(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    upstream_timeout_seconds: float,
) -> None:
    stale_after = max(1.0, upstream_timeout_seconds) + _PROVIDER_DISPATCH_SETTLEMENT_GRACE_SECONDS
    while True:
        # Do not race startup/short-lived probes or shutdown with a recovery
        # transaction. Rows are already held past the upstream timeout plus a
        # grace period, so one bounded sweep interval preserves semantics.
        await asyncio.sleep(_PROVIDER_DISPATCH_SWEEP_INTERVAL_SECONDS)
        try:
            async with session_factory() as session:
                settled = await settle_stale_provider_dispatches(
                    session,
                    stale_before=datetime.now(UTC) - timedelta(seconds=stale_after),
                )
            if settled:
                logger.warning(
                    "provider_dispatch_recovered_uncertain count=%d",
                    settled,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("provider_dispatch_reconciliation_failed")


def _load_admin_secret_verifier(
    settings: GatewaySettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for Gateway admin routes."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


async def _assert_secret_store_startup(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return await assert_existing_secrets_decryptable(session)


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_GW_DB_URL")


def create_app(settings: GatewaySettings) -> FastAPI:
    source_config = ServiceExecutionSourceConfig.from_settings(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(
            settings.db_engine_url,
            connect_args=settings.db_engine_connect_args,
        )
        admin_secret_verifier = _load_admin_secret_verifier(settings)
        await _assert_schema_startup(engine)
        app.state.settings = settings
        # #547: drain state must be attached BEFORE middleware runs its
        # first request. Live-migrated to app.state so the middleware
        # and both /healthz and /drain share one instance.
        ensure_drain_state(app)
        app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
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
            session_factory=app.state.session_factory,
            store=source_store,
            bucket=source_bucket,
        )
        app.state.artifact_store = artifact_store
        app.state.output_source_store = source_store
        app.state.output_source_bucket = source_bucket
        from loom_llm_gateway.task_egress import TaskEgressConfig, TaskEgressRuntime

        app.state.task_egress = (
            TaskEgressRuntime(TaskEgressConfig.model_validate_json(
                settings.task_egress_config_file.read_text()
            )) if settings.task_egress_config_file is not None else None
        )
        app.state.service_execution_output_service = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=source_store,
                bucket=source_bucket,
                repository=artifact_repository,
            ),
            session_factory=app.state.session_factory,
        )
        await retry_startup_dependency(
            lambda: _assert_secret_store_startup(app.state.session_factory),
            operation_name="gateway secret-store startup validation",
        )
        app.state.admin_secret_verifier = admin_secret_verifier
        app.state.rate_card_cache = RateCardCache(
            session_factory=app.state.session_factory,
            ttl_sec=settings.rate_card_cache_ttl_sec,
        )
        # Long-lived httpx client for native dialect passthroughs
        # (anthropic, openai responses, gemini). Plan 9 A9.1 — we do NOT
        # round-trip through LiteLLM for these.
        app.state.execution_pod_reviewer = ExecutionPodReviewer.from_file(
            settings.service_execution_kubernetes_config_file,
        )
        app.state.upstream_client = httpx.AsyncClient(
            timeout=settings.upstream_timeout_sec,
        )
        # #190 PR-C2: per-connection-id egress-proxy client pool. When
        # `egress_proxy_url` is empty (default), `pool.get(...)` falls
        # through to the shared upstream_client — egress mode opt-in,
        # rollback is one env-var flip + restart.
        app.state.egress_client_pool = EgressClientPool(
            upstream_client=app.state.upstream_client,
            proxy_url=settings.egress_proxy_url,
            upstream_timeout_sec=settings.upstream_timeout_sec,
        )
        provider_dispatch_reconciler = asyncio.create_task(
            _reconcile_provider_dispatches(
                app.state.session_factory,
                upstream_timeout_seconds=float(settings.upstream_timeout_sec),
            ),
            name="pipeline-provider-dispatch-reconciler",
        )
        try:
            yield
        finally:
            provider_dispatch_reconciler.cancel()
            with suppress(asyncio.CancelledError):
                await provider_dispatch_reconciler
            await app.state.egress_client_pool.aclose()
            await app.state.execution_pod_reviewer.close()
            await app.state.upstream_client.aclose()
            await engine.dispose()

    app: FastAPI = FastAPI(
        title="Loom LLM Gateway",
        version="0.0.1",
        lifespan=lifespan,
    )
    # #547: attach before middleware registration so the first request
    # (including ASGI-transport integration tests that skip lifespan)
    # can increment in_flight. Lifespan startup reuses this instance.
    ensure_drain_state(app)
    # #547: register middleware BEFORE routers so it wraps every route.
    install_drain_middleware(app)
    app.add_middleware(DispatchAuditMiddleware)

    @app.exception_handler(DispatchAuditUnavailableError)
    async def dispatch_audit_unavailable(
        request: Request,
        exc: DispatchAuditUnavailableError,
    ) -> JSONResponse:
        # Admission failed before provider I/O; do not leak driver/SQL details
        # or misreport this infrastructure failure as provider/auth failure.
        return JSONResponse(
            status_code=503,
            content={"detail": {"code": "dispatch_audit_unavailable"}},
        )

    app.include_router(health.router)
    app.include_router(drain.router)
    app.include_router(chat.router)
    app.include_router(messages.router)
    app.include_router(responses.router)
    app.include_router(gemini.router)
    app.include_router(facade_openai.router)
    app.include_router(harbor_baseline.router)
    app.include_router(facade_anthropic.router)
    app.include_router(facade_google.router)
    app.include_router(admin.router)
    app.include_router(service_execution.router)
    app.include_router(task_egress.router)
    # /metrics: prometheus_client ASGI app. Gateway is internal-only
    # per #77 boundary — scrapers reach it via cluster DNS.
    app.mount("/metrics", make_asgi_app())
    return app


__all__ = ["create_app"]
