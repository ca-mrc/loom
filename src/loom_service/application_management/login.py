"""One-use owner session exchange using a ready generation's shared SQL access."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from loom.application_session import ApplicationSessionAudienceV1
from loom.auth import AuthContext
from loom.nebius_application_contract import SharedDevelopmentBindingV1
from loom_service.application_management.credentials import SharedApplicationCredentials
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.registry import ManagementError
from loom_service.session_auth import create_application_login_challenge


class ApplicationLogin:
    def __init__(self, registry: ApplicationRegistry, *, shared: SharedDevelopmentBindingV1,
                 credentials: SharedApplicationCredentials, ca_file: Path):
        self.registry, self.shared, self.credentials, self.ca_file = registry, shared, credentials, ca_file

    async def issue(self, principal: AuthContext, application_id: UUID) -> dict[str, Any]:
        row, database = await self.registry.ready_access(application_id, principal=principal)
        audience = ApplicationSessionAudienceV1(application_id=row.application_id,
            origin="https://" + row.public_host, access_generation=row.access_generation)
        try:
            url = make_url(database["url"])
            if (row.data_environment_id != self.shared.data_environment_id
                    or self.credentials.data_environment_id != row.data_environment_id
                    or set(database) != {"url", "ca.crt"} or database["ca.crt"] != self.credentials.ca_pem
                    or url.drivername != "postgresql" or url.host != f"loom-postgres.{self.shared.platform_namespace}.svc"
                    or url.port != 5432 or url.database != self.credentials.database_name
                    or url.username != f"lap_{row.incarnation.hex}_g{row.access_generation}" or not url.password
                    or dict(url.query) != {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}):
                raise ValueError
            engine = create_async_engine(url.set(drivername="postgresql+psycopg",
                query={"sslmode": "verify-full", "sslrootcert": str(self.ca_file)}), poolclass=NullPool,
                connect_args={"connect_timeout": 10, "options": "-c statement_timeout=10000 -c lock_timeout=5000"})
            try:
                async with asyncio.timeout(30), async_sessionmaker(engine, expire_on_commit=False).begin() as session:
                    token = await create_application_login_challenge(session, user_id=row.owner_user_id,
                        team_id=row.owner_team_id, audience=audience)
            finally:
                await engine.dispose()
        except HTTPException:
            raise ManagementError("application_login_identity_unavailable", 403) from None
        except (SQLAlchemyError, ValueError, KeyError, OSError, TimeoutError):
            raise ManagementError("application_login_unavailable", 503) from None
        # Never return a proof after observing a newer desired generation. The
        # unreturned short-lived challenge grants nothing by itself; lifecycle
        # revocation and old-process retirement remain the cross-DB fence.
        current, _ = await self.registry.ready_access(application_id, principal=principal)
        if current != row:
            raise ManagementError("application_not_ready")
        return {"application_id": str(row.application_id), "incarnation": str(row.incarnation),
                "deployment_generation": row.deployment_generation, "access_generation": row.access_generation,
                "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
                "origin": audience.origin, "login_token": token, "expires_in": 90}
