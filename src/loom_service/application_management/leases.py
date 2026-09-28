"""DB-time operation leases; these do not fence running Pods or provider effects."""
from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_application_schema import NebiusApplication
from loom_service.environment_management.registry import ManagementError


@dataclass(frozen=True)
class ApplicationLease:
    operation_id: UUID
    application_id: UUID
    incarnation: UUID
    deployment_generation: int
    access_generation: int
    runner_epoch: int
    lease_token: UUID


class ApplicationOperationJournal:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def _duration(seconds: int) -> timedelta:
        if type(seconds) is not int or not 1 <= seconds <= 300:
            raise ValueError("operation lease must be between 1 and 300 seconds")
        return timedelta(seconds=seconds)

    @staticmethod
    def _current(operation: NebiusApplicationOperation, row: NebiusApplication) -> bool:
        return (row.purged_at is None and row.deployment_generation == operation.deployment_generation
                and row.access_generation == operation.access_generation)

    async def _locked_operation(self, session: AsyncSession, operation_id: UUID
                                ) -> tuple[NebiusApplicationOperation, NebiusApplication, datetime]:
        application_id = await session.scalar(select(NebiusApplicationOperation.application_id).where(
            NebiusApplicationOperation.operation_id == operation_id,
        ))
        if application_id is None:
            raise ManagementError("operation_not_found", 404)
        # No budget lock may be acquired after these locks. Transitions acquire it first.
        row = (await session.scalars(select(NebiusApplication).where(
            NebiusApplication.application_id == application_id,
        ).with_for_update())).one()
        operation = (await session.scalars(select(NebiusApplicationOperation).where(
            NebiusApplicationOperation.operation_id == operation_id,
        ).with_for_update())).one()
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return operation, row, now

    async def _leased(self, session: AsyncSession, lease: ApplicationLease
                      ) -> tuple[NebiusApplicationOperation, datetime]:
        operation, row, now = await self._locked_operation(session, lease.operation_id)
        if (not self._current(operation, row) or operation.phase != "running"
                or row.application_id != lease.application_id or row.incarnation != lease.incarnation
                or operation.deployment_generation != lease.deployment_generation
                or operation.access_generation != lease.access_generation
                or operation.runner_epoch != lease.runner_epoch or operation.lease_token != lease.lease_token
                or operation.lease_expires_at is None or operation.lease_expires_at <= now):
            raise ManagementError("stale_operation_lease")
        return operation, now

    async def claim(self, operation_id: UUID, *, lease_seconds: int = 60) -> ApplicationLease | None:
        duration = self._duration(lease_seconds)
        async with self.session_factory.begin() as session:
            operation, row, now = await self._locked_operation(session, operation_id)
            if operation.phase in {"completed", "blocked", "superseded"}:
                return None
            if not self._current(operation, row):
                raise ManagementError("stale_operation_generation")
            if operation.lease_expires_at is not None and operation.lease_expires_at > now:
                return None
            token = uuid4()
            operation.runner_epoch += 1
            operation.phase, operation.error_code = "running", None
            operation.lease_token, operation.lease_expires_at = token, now + duration
            return ApplicationLease(operation_id, row.application_id, row.incarnation,
                                    row.deployment_generation, row.access_generation, operation.runner_epoch, token)

    async def renew(self, lease: ApplicationLease, *, lease_seconds: int = 60) -> None:
        duration = self._duration(lease_seconds)
        async with self.session_factory.begin() as session:
            operation, now = await self._leased(session, lease)
            operation.lease_expires_at = now + duration

    @staticmethod
    async def _historical_operation(session: AsyncSession, current: NebiusApplicationOperation,
                                     operation_id: UUID) -> NebiusApplicationOperation:
        target = await session.get(NebiusApplicationOperation, operation_id)
        if (target is None or target.application_id != current.application_id
                or target.deployment_generation > current.deployment_generation):
            raise ManagementError("application_history_forbidden", 403)
        return target

    async def frozen_plan(self, lease: ApplicationLease, *, operation_id: UUID | None = None) -> dict[str, Any]:
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            if operation_id is not None:
                operation = await self._historical_operation(session, operation, operation_id)
            return copy.deepcopy(operation.plan_json)

    async def finish_attempt(self, lease: ApplicationLease, *, error_code: str, retry: bool) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_]{0,95}", error_code) is None or type(retry) is not bool:
            raise ValueError("invalid operation failure")
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            operation.phase = "pending" if retry else "blocked"
            operation.error_code = error_code
            operation.lease_token = operation.lease_expires_at = None
