"""Automatic personal image builds: common-pool admission and read-only results."""
from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from loom.nebius_pool_contract import PoolReceiptV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom_execution_actuator.pool_build_runtime import PoolNativeBuildApi
from loom_execution_actuator.pool_client import PoolClient, PoolRequestUnconfirmedError
from loom_service.application_management.build_journal import (
    ApplicationBuildJournal,
    ApplicationBuildLease,
    ApplicationBuildState,
)
from loom_service.environment_management.registry import ManagementError

_LOG = logging.getLogger(__name__)


class ApplicationBuildWorker:
    def __init__(self, journal: ApplicationBuildJournal, management: PoolClient, kubernetes: PoolNativeBuildApi, *,
                 lease_seconds: int = 60, reconcile_timeout: float = 45):
        journal._duration(lease_seconds)
        if type(reconcile_timeout) not in {int, float} or not 0 < reconcile_timeout <= 300:
            raise ValueError("invalid_application_build_reconcile_timeout")
        self.journal, self.management, self.kubernetes = journal, management, kubernetes
        self.lease_seconds, self.reconcile_timeout = lease_seconds, reconcile_timeout
        self.healthy = False

    async def _heartbeat(self, lease: ApplicationBuildLease) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            async with asyncio.timeout(min(5, self.lease_seconds / 6)):
                await self.journal.renew(lease, lease_seconds=self.lease_seconds)

    async def _cancel(self, lease: ApplicationBuildLease, state: ApplicationBuildState) -> bool:
        if state.request is None:
            await self.journal.cancelled_unstarted(lease, None)
            return True
        try:
            receipt = await self.management.cancel_unstarted(state.action)
        except PoolRequestUnconfirmedError:
            result = await self.management.status(state.action)
            if not isinstance(result, PoolReceiptV1) or result.phase == "reserved":
                return True  # Unknown cancellation is retried with the same key.
            receipt = result
        if receipt.phase == "cancelled_unstarted":
            await self.journal.cancelled_unstarted(lease, receipt)
            return True
        # Activation won the race (or its response was lost). Keep it charged
        # and drive the ordinary stop/drain/cleanup path, never another attempt.
        await self.journal.accept_grant(lease, receipt)
        await self.journal.activated(lease, receipt)
        return False

    async def _advance(self, lease: ApplicationBuildLease) -> None:
        state = await self.journal.state(lease)
        if state.request is None and not state.must_cancel:
            await self.journal.dispatch.freeze(lease.build_id, attempt=lease.attempt)
            state = await self.journal.state(lease)
        if state.phase == "queued":
            if state.must_cancel:
                if await self._cancel(lease, state):
                    return
            else:
                if state.grant is None:
                    assert state.request is not None
                    result = await self.management.prepare(state.request)
                    if not isinstance(result, PoolReceiptV1):
                        return
                    if result.phase == "cancelled_unstarted":
                        await self.journal.cancelled_unstarted(lease, result)
                        return
                    await self.journal.accept_grant(lease, result)
                    state = await self.journal.state(lease)
                # Status precedes every activation, including recovery after a
                # lost reply. Consent is retained once and never extended.
                result = await self.management.status(state.action)
                if not isinstance(result, PoolReceiptV1):
                    raise PoolRequestUnconfirmedError
                if result.phase == "cancelled_unstarted":
                    await self.journal.cancelled_unstarted(lease, result)
                    return
                if result.phase == "reserved":
                    consent = await self.journal.begin_activation(lease)
                    if consent is None:
                        if await self._cancel(lease, await self.journal.state(lease)):
                            return
                    else:
                        await self.journal.activated(lease, await self.management.activate(consent))
                else:
                    await self.journal.activated(lease, result)
        state = await self.journal.state(lease)
        runtime = await self.management.native_runtime(state.action)
        state = await self.journal.observe(lease, runtime)
        if state.phase == "running" and runtime.receipt.job_uid is not None:
            observed = await self.kubernetes.observe_pool(runtime)
            if observed is not None:
                state = await self.journal.observe(lease, runtime, observed)
        if state.phase == "settling":
            assert state.settlement is not None
            await self.management.stop(PoolStopV1.model_validate(state.settlement["stop"]))
            await self.management.drain(PoolDrainV1.model_validate(state.settlement["drain"]))

    async def _claim(self, build_id: UUID, *, attempt: int) -> ApplicationBuildLease | None:
        async with asyncio.timeout(min(5, self.lease_seconds / 6)):
            return await self.journal.claim(build_id, attempt=attempt, lease_seconds=self.lease_seconds)

    async def _release(self, claim: asyncio.Task[ApplicationBuildLease | None],
                       children: list[asyncio.Task[None]]) -> None:
        # Drain in-flight I/O BEFORE releasing our lease. Cancellation does
        # not undo remote writes; their immutable messages survive this task.
        for task in children:
            task.cancel()
        await asyncio.gather(*children, return_exceptions=True)
        await asyncio.gather(claim, return_exceptions=True)
        if claim.cancelled() or claim.exception() is not None:
            return  # The caller already observes the claim failure.
        lease = claim.result()
        if lease is not None:
            try:
                async with asyncio.timeout(min(5, self.lease_seconds / 6)):
                    await self.journal.release(lease)
            except ManagementError as error:
                if error.code != "stale_application_build_lease":
                    raise

    async def reconcile_once(self, build_id: UUID, *, attempt: int) -> None:
        claim = asyncio.create_task(self._claim(build_id, attempt=attempt))
        children: list[asyncio.Task[None]] = []
        try:
            # A committed claim must reach cleanup even if shutdown arrives
            # before its result. The claim has its own bounded timeout.
            lease = await asyncio.shield(claim)
            if lease is None:
                return
            heartbeat = asyncio.create_task(self._heartbeat(lease))
            work = asyncio.create_task(self._advance(lease))
            children.extend((heartbeat, work))
            async with asyncio.timeout(self.reconcile_timeout):
                done, _ = await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)
                (work if work in done else heartbeat).result()
        finally:
            cleanup = asyncio.create_task(self._release(claim, children))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Repeated shutdown signals must not interrupt cleanup or leave
                # an unowned task using a closed database/HTTP client.
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if not cleanup.cancelled() and (error := cleanup.exception()) is not None:
                    _LOG.warning("application_build_cleanup_failed error=%s", type(error).__name__)
                raise

    async def run(self, *, concurrency: int = 4, poll_seconds: float = 5) -> None:
        if type(concurrency) is not int or not 1 <= concurrency <= 16 or not 1 <= poll_seconds <= 60:
            raise ValueError("invalid_application_build_loop_limits")
        active: dict[UUID, asyncio.Task[None]] = {}
        cursor: UUID | None = None
        try:
            while True:
                try:
                    for identity, task in list(active.items()):
                        if task.done():
                            del active[identity]
                            try:
                                task.result()
                            except Exception as error:
                                _LOG.warning("application_build_reconcile_deferred build=%s error=%s", identity, type(error).__name__)
                    # Keyset scan wraps only after the end, so a waiting first
                    # page cannot starve other owners. Poll even when full.
                    async with asyncio.timeout(min(5, self.lease_seconds / 6)):
                        pending = await self.journal.pending(after=cursor, limit=max(1, concurrency - len(active)))
                    if not pending:
                        cursor = None
                    else:
                        for identity, attempt in pending:
                            if len(active) >= concurrency:
                                break
                            cursor = identity
                            if identity not in active:
                                active[identity] = asyncio.create_task(self.reconcile_once(identity, attempt=attempt))
                    self.healthy = True
                except Exception:
                    self.healthy = False
                    _LOG.warning("application_build_worker_recovering")
                    for task in active.values():
                        task.cancel()
                    await asyncio.gather(*active.values(), return_exceptions=True)
                    active.clear()
                await asyncio.sleep(poll_seconds)
        finally:
            self.healthy = False
            for task in active.values():
                task.cancel()
            await asyncio.gather(*active.values(), return_exceptions=True)
