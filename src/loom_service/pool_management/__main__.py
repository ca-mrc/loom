"""Installed fixed Job gateway; distinct from management HTTP API replicas."""
from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import httpx
import uvicorn
from fastapi import FastAPI, Response
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema_startup import assert_schema_at_head
from loom_execution_capacity_collector.control_plane import read_owner_only_secret
from loom_service.environment_management.kubernetes_credentials import (
    ProjectedKubernetesConnection,
    ProjectedKubernetesCredentials,
)
from loom_service.environment_management.runtime import NebiusManagementAuth
from loom_service.pool_management.auth import PoolPrincipal, resolve_pool_machine
from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from loom_service.pool_management.kubernetes import KubernetesPoolGateway
from loom_service.pool_management.worker import PoolGatewayWorker

_LOG = logging.getLogger(__name__)


class PoolGatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOOM_POOL_GATEWAY_", extra="forbid")

    db_url: str = Field(repr=False)
    pool_id: UUID
    installation_id: UUID
    machine_id: UUID
    admission_epoch: int = Field(gt=0, strict=True)
    bearer_token_file: Path
    kubernetes: ProjectedKubernetesConnection
    poll_seconds: float = Field(default=2, ge=0.1, le=60, allow_inf_nan=False)
    health_host: str = "0.0.0.0"
    health_port: int = Field(default=9120, ge=1, le=65535)

    @field_validator("admission_epoch", mode="before")
    @classmethod
    def environment_epoch(cls, value: object) -> object:
        # Environment settings are strings; retain strict integer validation for
        # direct inputs and reject booleans, decimals and noncanonical spellings.
        if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,18}", value):
            return int(value)
        return value

    @model_validator(mode="after")
    def bound(self) -> PoolGatewaySettings:
        if (not all(value.int for value in (self.pool_id, self.installation_id, self.machine_id))
                or not all(path.is_absolute() for path in (
                    self.bearer_token_file, self.kubernetes.ca_file, self.kubernetes.token_file))):
            raise ValueError("gateway requires non-nil identities and absolute credential paths")
        return self


@dataclass
class GatewayHealth:
    stale_after_seconds: float
    last_success: float | None = None

    @property
    def ready(self) -> bool:
        return self.last_success is not None and time.monotonic() - self.last_success <= self.stale_after_seconds


def _health_app(health: GatewayHealth) -> FastAPI:
    app = FastAPI(title="Loom Pool Gateway", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready(response: Response) -> dict[str, str]:
        if not health.ready:
            response.status_code = 503
        return {"status": "ready" if health.ready else "not_ready"}

    return app


async def _principal(settings: PoolGatewaySettings, sessions: async_sessionmaker[AsyncSession]) -> PoolPrincipal:
    try:
        token = read_owner_only_secret(settings.bearer_token_file, maximum_bytes=512)
        async with sessions() as session:
            principal = await resolve_pool_machine(session, "Bearer " + token)
        if principal is not None and (
                principal.role, principal.participant_id, principal.pool_id, principal.installation_id,
                principal.machine_id, principal.pool_epoch, principal.pool_mode) in (
                    ("gateway", None, settings.pool_id, settings.installation_id, settings.machine_id,
                     settings.admission_epoch, "global"),
                    ("gateway", None, settings.pool_id, settings.installation_id, settings.machine_id,
                     settings.admission_epoch, "closed")):
            return principal
    except (OSError, ValueError):
        pass
    raise ValueError("pool_gateway_identity_unavailable")


async def _loop(settings: PoolGatewaySettings, sessions: async_sessionmaker[AsyncSession],
                gateway: KubernetesPoolGateway, health: GatewayHealth) -> None:
    while True:
        try:
            # Reopen the dedicated credential and resolve current authority every
            # pass. The journal reauthorizes again under its mutation locks.
            principal = await _principal(settings, sessions)
            await PoolGatewayWorker(gateway=gateway, principal=principal).run_once()
            health.last_success = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            health.last_success = None
            _LOG.warning("pool gateway reconciliation deferred (%s)", type(error).__name__)
        await asyncio.sleep(settings.poll_seconds)


async def _run() -> None:
    settings = PoolGatewaySettings()
    async with AsyncExitStack() as resources:
        engine = create_async_engine(settings.db_url, pool_pre_ping=True)
        resources.push_async_callback(engine.dispose)
        await assert_schema_at_head(engine, db_url_env_var="LOOM_POOL_GATEWAY_DB_URL")
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        await _principal(settings, sessions)
        credentials = ProjectedKubernetesCredentials(settings.kubernetes)
        resources.push_async_callback(credentials.close)
        await credentials.get_token()
        http = await resources.enter_async_context(httpx.AsyncClient(
            base_url=settings.kubernetes.endpoint, verify=credentials.ssl_context,
            auth=NebiusManagementAuth(settings.kubernetes, credentials),
            trust_env=False, timeout=30, follow_redirects=False))
        gateway = KubernetesPoolGateway(PoolGatewayJournal(sessions), http)
        health = GatewayHealth(stale_after_seconds=max(15, settings.poll_seconds * 3))
        server = uvicorn.Server(uvicorn.Config(_health_app(health), host=settings.health_host,
            port=settings.health_port, log_level="info"))
        tasks = [asyncio.create_task(_loop(settings, sessions, gateway, health), name="loom-pool-gateway"),
            asyncio.create_task(server.serve(), name="loom-pool-gateway-health")]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        finally:
            health.last_success = None
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
