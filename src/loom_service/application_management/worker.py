"""Bounded personal lifecycle execution; the concrete coordinator owns completion."""
from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError

from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderRetryError,
    ProviderWaitingError,
)
from loom_service.environment_management.registry import ManagementError

_LOG = logging.getLogger(__name__)


class ApplicationWorker:
    def __init__(self, registry: ApplicationRegistry, coordinator: ApplicationLifecycleCoordinator, *,
                 lease_seconds: int = 60, attempt_timeout: float = 45, max_attempts: int = 20,
                 readiness_poll_seconds: float = 2, readiness_timeout: float = 900):
        if (type(lease_seconds) is not int or not 3 <= lease_seconds <= 300
                or type(max_attempts) is not int or not 1 <= max_attempts <= 100
                or not 0 < attempt_timeout <= 300):
            raise ValueError("invalid application worker limits")
        if not 0 < readiness_poll_seconds <= 30 or not 0 < readiness_timeout <= 1800:
            raise ValueError("invalid readiness limits")
        self.registry, self.coordinator = registry, coordinator
        self.lease_seconds, self.attempt_timeout, self.max_attempts = lease_seconds, attempt_timeout, max_attempts
        self.readiness_poll_seconds, self.readiness_timeout = readiness_poll_seconds, readiness_timeout
        self.healthy = False

    async def _heartbeat(self, lease: ApplicationLease) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            await self.registry.renew(lease, lease_seconds=self.lease_seconds)

    async def _advance(self, lease: ApplicationLease) -> None:
        plan = await self.registry.frozen_plan(lease)
        advance = self.coordinator.start if plan["registration"]["desired_state"] == "active" else self.coordinator.stop
        deadline = asyncio.get_running_loop().time() + self.readiness_timeout
        while True:
            try:
                async with asyncio.timeout(self.attempt_timeout):
                    await advance(lease)
                return  # The concrete coordinator already recorded completion.
            except ProviderWaitingError:
                if asyncio.get_running_loop().time() >= deadline:
                    raise ProviderBlockedError("application_readiness_timeout") from None
                await asyncio.sleep(self.readiness_poll_seconds)

    async def reconcile_once(self, operation_id: UUID) -> None:
        lease = await self.registry.claim(operation_id, lease_seconds=self.lease_seconds)
        if lease is None:
            return
        heartbeat = asyncio.create_task(self._heartbeat(lease))
        work = asyncio.create_task(self._advance(lease))
        try:
            try:
                done, _ = await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)
                # Completion itself clears the lease. Prefer its result over a
                # simultaneous stale heartbeat; never add a second completion path.
                (work if work in done else heartbeat).result()
            finally:
                # Drain all in-flight work BEFORE releasing this lease on error.
                # Cancellation cannot undo an external request; journals retain it.
                heartbeat.cancel()
                work.cancel()
                await asyncio.gather(heartbeat, work, return_exceptions=True)
        except ManagementError:
            return  # A successor/current completion fences this attempt's reporting.
        except (ProviderRetryError, TimeoutError) as exc:
            code = exc.code if isinstance(exc, ProviderRetryError) else "provider_timeout"
            await self._failure(lease, code, retry=lease.runner_epoch < self.max_attempts)
        except ProviderBlockedError as exc:
            await self._failure(lease, exc.code, retry=False)
        except (SQLAlchemyError, OSError):
            raise  # Poll loop recovers DB availability; the durable lease remains.
        except Exception:
            await self._failure(lease, "provider_internal_error", retry=False)

    async def _failure(self, lease: ApplicationLease, code: str, *, retry: bool) -> None:
        try:
            await self.registry.finish_attempt(lease, error_code=code, retry=retry)
        except ManagementError:
            pass

    async def run(self, *, concurrency: int = 4, poll_seconds: float = 5) -> None:
        if type(concurrency) is not int or not 1 <= concurrency <= 16 or not 1 <= poll_seconds <= 60:
            raise ValueError("invalid application worker loop limits")
        active: dict[UUID, asyncio.Task[None]] = {}

        async def cancel_active() -> None:
            for task in active.values():
                task.cancel()
            await asyncio.gather(*active.values(), return_exceptions=True)
            active.clear()

        try:
            while True:
                try:
                    for identity, task in list(active.items()):
                        if task.done():
                            del active[identity]
                            task.result()
                    # Poll at full concurrency too: DB loss must revoke health
                    # and cancel active work, not silently keep a green endpoint.
                    operations = await self.registry.runnable_operations(limit=concurrency)
                    for identity in operations:
                        if identity not in active and len(active) < concurrency:
                            active[identity] = asyncio.create_task(self.reconcile_once(identity))
                    self.healthy = True
                except Exception:
                    self.healthy = False
                    _LOG.warning("application_worker_recovering")
                    await cancel_active()
                await asyncio.sleep(poll_seconds)
        finally:
            self.healthy = False
            await cancel_active()
