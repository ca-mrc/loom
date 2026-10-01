"""Pool-scoped orchestration of the fixed gateway, without new write authority."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, tuple_

from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolRequest
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.gateway_journal import CreateKind
from loom_service.pool_management.kubernetes import (
    KubernetesPoolGateway,
    PoolKubernetesWaitingError,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Work:
    phase: str
    has_configmap: bool
    drained: bool
    creates: dict[CreateKind, str]


class PoolGatewayWorker:
    def __init__(self, *, gateway: KubernetesPoolGateway, principal: PoolPrincipal) -> None:
        self.gateway, self.principal = gateway, principal

    async def _pending(self) -> AsyncIterator[UUID]:
        row = NebiusPoolRequest
        query = select(row.created_at, row.request_id).where(
            row.pool_id == self.principal.pool_id, row.phase.in_(("create_intent", "cleanup_intent")))
        journal = self.gateway.journal
        async with journal._transaction(self.principal) as (session, _):
            last = (await session.execute(query.order_by(row.created_at.desc(), row.request_id.desc()).limit(1))).first()
        if last is None:
            return
        ceiling = (last[0], last[1])
        after: tuple[datetime, UUID] | None = None
        while True:
            page = query.where(tuple_(row.created_at, row.request_id) <= ceiling)
            if after is not None:
                page = page.where(tuple_(row.created_at, row.request_id) > after)
            async with journal._transaction(self.principal) as (session, _):
                values = (await session.execute(page.order_by(row.created_at, row.request_id).limit(100))).all()
            if not values:
                return
            after = (values[-1][0], values[-1][1])
            for _, identity in values:
                yield identity
            if after == ceiling:
                return

    async def _work(self, identity: UUID) -> _Work:
        journal = self.gateway.journal
        async with journal._transaction(self.principal) as (session, pool):
            row = await journal._request(session, pool, identity)
            assert row.plan_json is not None  # Validated against the retained plan digest.
            effects = {key: phase for key, phase in await session.execute(
                select(NebiusPoolEffect.effect_key, NebiusPoolEffect.phase).where(NebiusPoolEffect.request_id == identity))}
            kinds: tuple[CreateKind, ...] = ("ConfigMap", "Job")
            creates = {kind: effects["create:" + kind.lower()] for kind in kinds if "create:" + kind.lower() in effects}
            return _Work(row.phase, row.plan_json["configmap"] is not None, row.drain_json is not None, creates)

    async def _reconcile(self, identity: UUID) -> None:
        work = await self._work(identity)
        gateway, principal = self.gateway, self.principal
        if work.phase == "create_intent":
            if work.has_configmap:
                await gateway.create(principal, identity, kind="ConfigMap")
            await gateway.create(principal, identity, kind="Job")
            return
        if work.phase != "cleanup_intent":
            return  # Another reconciler may have completed this request.
        # Recover an uncertain CREATE by observation, never a fresh permit. The
        # journal rechecks phase and authority at every mutation boundary.
        for kind, phase in work.creates.items():
            if phase == "dispatched":
                await gateway.create(principal, identity, kind=kind)
        work = await self._work(identity)
        if work.phase != "cleanup_intent":
            return
        if work.creates.get("Job") == "observed":
            await gateway.delete(principal, identity, kind="Job")
        if not work.drained:
            return  # Stop precedes output drain; auxiliaries and charge remain.
        if work.creates.get("ConfigMap") == "observed":
            await gateway.delete(principal, identity, kind="ConfigMap")
        if work.creates.get("Job") == "observed":
            inventory = await gateway.pod_inventory(principal, identity)
            for pod in inventory.pods:
                await gateway.delete_pod(principal, identity, pod=pod)
        await gateway.verify_cleanup(principal, identity)

    async def run_once(self) -> None:
        first_error: Exception | None = None
        async for identity in self._pending():
            try:
                await self._reconcile(identity)
            except PoolKubernetesWaitingError:
                continue  # Retain the charge and reconcile the exact effect next pass.
            except Exception as error:
                if first_error is None:
                    first_error = error
                _LOG.warning("Pool gateway deferred reservation=%s error=%s", identity, type(error).__name__)
        if first_error is not None:
            raise first_error
