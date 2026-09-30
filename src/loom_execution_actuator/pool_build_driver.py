"""Recover one native handoff using committed SQL steps and bounded HTTP.

This driver has no Job writer or local admission fallback. Active work and stop
intents are returned to the controller's result/drain reconciler; they never
become free capacity merely because a request/lease has expired.
"""
from __future__ import annotations

from loom.nebius_pool_contract import PoolReceiptV1, PoolRequestKeyV1
from loom_execution_actuator.pool_client import PoolClient, PoolRequestUnconfirmedError
from loom_execution_actuator.pool_outbox import PoolBuildHandoff, PoolBuildOutbox, PoolHandoffError


class PoolBuildDriver:
    def __init__(self, *, outbox: PoolBuildOutbox, management: PoolClient) -> None:
        self.outbox, self.management = outbox, management

    async def _cancel(self, handoff: PoolBuildHandoff) -> PoolBuildHandoff:
        try:
            receipt = await self.management.cancel_unstarted(handoff.action)
        except PoolRequestUnconfirmedError:
            # Cancellation may have lost to activation or lost its committed
            # reply. Status never renews demand or repeats an external write.
            result = await self.management.status(handoff.action)
            if not isinstance(result, PoolReceiptV1) or result.phase == "reserved":
                return await self.outbox.get(handoff.request.key)
            receipt = result
        if receipt.phase == "cancelled_unstarted":
            return await self.outbox.confirm_cancel(handoff.request.key, receipt)
        return await self.outbox.confirm_activation(handoff.request.key, receipt)

    async def advance(self, key: PoolRequestKeyV1) -> PoolBuildHandoff:
        handoff = await self.outbox.refresh_selection(key)
        if handoff.phase == "selected":
            result = await self.management.prepare(handoff.request)
            if not isinstance(result, PoolReceiptV1):
                return await self.outbox.get(key)
            if result.phase == "cancelled_unstarted":
                await self.outbox.request_cancel(key)
                return await self.outbox.confirm_cancel(key, result)
            handoff = await self.outbox.accept_grant(key, result)
        if handoff.phase in {"attached", "activation_pending"}:
            # Always recover the manager's durable outcome BEFORE deciding
            # whether this still-current claim may issue its first activation.
            result = await self.management.status(handoff.action)
            if not isinstance(result, PoolReceiptV1):
                raise PoolHandoffError
            if result.phase == "cancelled_unstarted":
                await self.outbox.request_cancel(key)
                return await self.outbox.confirm_cancel(key, result)
            if result.phase != "reserved":
                return await self.outbox.confirm_activation(key, result)
            handoff = await self.outbox.begin_activation(key)
            if handoff.phase == "activation_pending":
                assert handoff.activation is not None
                receipt = await self.management.activate(handoff.activation)
                return await self.outbox.confirm_activation(key, receipt)
        if handoff.phase == "cancel_pending":
            return await self._cancel(handoff)
        return handoff
