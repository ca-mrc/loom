"""Nonblocking access to protected, preinstalled shared SQL routines.

The installation qualifies the manager connection/TLS route. Each invocation
opens its own bounded autocommit connection; no connection crosses worker threads.
Cancellation cannot undo SQL already sent. Monotonic SQL retirement fences late
grants, and the lifecycle caller must reconcile/drain before claiming retirement.
"""
from __future__ import annotations

import asyncio
from uuid import UUID

import psycopg

from loom.nebius_application_database import (
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
)
from loom_service.application_management.leases import ApplicationLease
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError


class AsyncApplicationDatabaseAccess:
    def __init__(self, connection_url: str, data_environment_id: UUID):
        if not isinstance(data_environment_id, UUID) or data_environment_id.int == 0:
            raise ValueError("invalid shared application database identity")
        self._connection_url = connection_url
        self.data_environment_id = data_environment_id

    def _call(self, action: str, lease: ApplicationLease, generation: int,
              password: str | None = None) -> str | bool | None:
        try:
            with psycopg.connect(self._connection_url, autocommit=True, connect_timeout=10,
                                  options="-c statement_timeout=30000 -c lock_timeout=10000") as connection:
                access = ApplicationDatabaseAccess(connection, self.data_environment_id)
                if action == "grant":
                    assert password is not None
                    return access.grant(lease.application_id, lease.incarnation, generation, password)
                if action == "revoke":
                    access.revoke(lease.application_id, lease.incarnation, generation)
                    return None
                if action == "drain":
                    return access.drain(lease.application_id, lease.incarnation, generation)
                raise ValueError("invalid application database action")
        except ApplicationDatabaseAccessError as exc:
            if str(exc) == "application_database_operation_failed":
                raise ProviderRetryError("application_database_unconfirmed") from None
            raise ProviderBlockedError(str(exc)) from None
        except psycopg.Error:
            raise ProviderRetryError("application_database_unavailable") from None

    async def grant(self, lease: ApplicationLease, password: str) -> str:
        value = await asyncio.to_thread(self._call, "grant", lease, lease.access_generation, password)
        if not isinstance(value, str):
            raise ProviderBlockedError("application_database_result_invalid")
        return value

    async def revoke(self, lease: ApplicationLease, through_generation: int) -> None:
        await asyncio.to_thread(self._call, "revoke", lease, through_generation)

    async def drain(self, lease: ApplicationLease, through_generation: int) -> bool:
        value = await asyncio.to_thread(self._call, "drain", lease, through_generation)
        if type(value) is not bool:
            raise ProviderBlockedError("application_database_result_invalid")
        return value
