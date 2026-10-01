"""One-use residual Pod deletion, sharing the fixed gateway's effect journal."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolEffect, NebiusPoolRequest
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.gateway_journal import (
    PoolGatewayEffect,
    PoolGatewayError,
    PoolGatewayJournal,
    _view,
)
from loom_service.pool_management.pod_inventory import PoolPodReference


@dataclass(frozen=True)
class PoolPodDeletion:
    effect_id: UUID
    phase: str
    created: PoolGatewayEffect
    pod: PoolPodReference
    document: dict[str, Any]
    dispatch_id: UUID | None
    rejection_status: int | None


def _document(pod: PoolPodReference, created: PoolGatewayEffect) -> dict[str, Any]:
    if (not isinstance(pod.uid, UUID) or not pod.uid.int or not isinstance(pod.name, str)
            or len(pod.name) > 253 or re.fullmatch(re.escape(created.document["metadata"]["name"]) + r"-[a-z0-9-]+", pod.name) is None
            or not isinstance(pod.resource_version, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", pod.resource_version) is None):
        raise PoolGatewayError
    return {"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 0, "preconditions": {"uid": str(pod.uid)}}


def _intent(pod: PoolPodReference, created: PoolGatewayEffect) -> dict[str, Any]:
    return {"api_version": "v1", "kind": "Pod", "action": "delete", "name": pod.name,
        "namespace": created.document["metadata"]["namespace"], "uid": str(pod.uid),
        "resource_version": pod.resource_version, "create_effect_id": str(created.effect_id),
        "request_sha256": digest(_document(pod, created))}


def _deletion(effect: NebiusPoolEffect, request: NebiusPoolRequest, created: PoolGatewayEffect) -> PoolPodDeletion:
    intent = effect.intent_json
    pod = PoolPodReference(intent["name"], UUID(intent["uid"]), intent["resource_version"], False)
    if (intent != _intent(pod, created) or effect.effect_key != "delete:pod:" + pod.uid.hex
            or effect.namespace_uid != request.namespace_uid or effect.plan_sha256 != request.plan_sha256):
        raise PoolGatewayError
    return PoolPodDeletion(effect.effect_id, effect.phase, created, pod, _document(pod, created),
                           effect.dispatch_id, effect.rejection_status)


class PoolPodCleanupJournal:
    def __init__(self, journal: PoolGatewayJournal) -> None:
        self.journal = journal

    async def _parent(self, session: AsyncSession, request: NebiusPoolRequest) -> PoolGatewayEffect:
        created = await self.journal._created(session, request, "Job")
        retired = await session.scalar(select(NebiusPoolEffect).where(
            NebiusPoolEffect.request_id == request.request_id, NebiusPoolEffect.effect_key == "delete:job"))
        if (request.stop_json is None or request.drain_json is None or retired is None
                or retired.phase != "observed" or retired.observed_uid != created.observed_uid
                or retired.intent_json.get("create_effect_id") != str(created.effect_id)
                or retired.intent_json.get("uid") != str(created.observed_uid)):
            raise PoolGatewayError("pool_pod_cleanup_not_authorized")
        return _view(created, request)

    async def _effect(self, session: AsyncSession, pool: NebiusPoolBinding, effect_id: UUID) -> tuple[
        NebiusPoolEffect, NebiusPoolRequest, PoolGatewayEffect,
    ]:
        reservation_id = await session.scalar(select(NebiusPoolEffect.request_id).where(NebiusPoolEffect.effect_id == effect_id))
        if reservation_id is None:
            raise PoolGatewayError
        request = await self.journal._request(session, pool, reservation_id)
        effect = (await session.scalars(select(NebiusPoolEffect).where(
            NebiusPoolEffect.effect_id == effect_id).with_for_update())).one()
        created = await self._parent(session, request)
        _deletion(effect, request, created)
        return effect, request, created

    async def prepare(self, principal: PoolPrincipal, reservation_id: UUID, *, pod: PoolPodReference) -> PoolPodDeletion:
        async with self.journal._transaction(principal) as (session, pool):
            request = await self.journal._request(session, pool, reservation_id)
            created = await self._parent(session, request)
            intent = _intent(pod, created)
            key = "delete:pod:" + pod.uid.hex
            existing = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == reservation_id, NebiusPoolEffect.effect_key == key))
            if existing is not None:
                saved = _deletion(existing, request, created)
                if saved.pod.name != pod.name:
                    raise PoolGatewayError
                return saved  # A later resource version cannot rewrite first intent.
            if request.phase != "cleanup_intent":
                raise PoolGatewayError
            last = await session.scalar(select(func.max(NebiusPoolEffect.sequence)).where(NebiusPoolEffect.request_id == reservation_id))
            effect = (await session.scalars(insert(NebiusPoolEffect).values(
                effect_id=uuid4(), request_id=reservation_id, plan_sha256=request.plan_sha256, namespace_uid=request.namespace_uid,
                effect_key=key, sequence=(last or 0) + 1, intent_json=intent, phase="prepared").returning(NebiusPoolEffect))).one()
            return _deletion(effect, request, created)

    async def get(self, principal: PoolPrincipal, effect_id: UUID) -> PoolPodDeletion:
        async with self.journal._transaction(principal) as (session, pool):
            effect, request, created = await self._effect(session, pool, effect_id)
            return _deletion(effect, request, created)

    async def dispatch(self, principal: PoolPrincipal, effect_id: UUID) -> PoolPodDeletion | None:
        async with self.journal._transaction(principal) as (session, pool):
            effect, request, created = await self._effect(session, pool, effect_id)
            if effect.phase != "prepared":
                return None
            if request.phase != "cleanup_intent":
                raise PoolGatewayError
            effect = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="dispatched", dispatch_id=uuid4(), dispatch_machine_id=principal.machine_id,
                dispatch_epoch=principal.credential_epoch).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            result = _deletion(effect, request, created)
        return result

    async def observe(self, principal: PoolPrincipal, effect_id: UUID) -> PoolPodDeletion:
        async with self.journal._transaction(principal) as (session, pool):
            effect, request, created = await self._effect(session, pool, effect_id)
            if effect.phase == "observed":
                return _deletion(effect, request, created)
            if effect.phase != "dispatched" or request.phase != "cleanup_intent":
                raise PoolGatewayError
            effect = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="observed", observed_uid=UUID(effect.intent_json["uid"])).returning(NebiusPoolEffect)
                .execution_options(populate_existing=True))).one()
            return _deletion(effect, request, created)

    async def reject(self, principal: PoolPrincipal, effect_id: UUID, *, status_code: int) -> PoolPodDeletion:
        if type(status_code) is not int or status_code not in {409, 422}:
            raise PoolGatewayError
        async with self.journal._transaction(principal) as (session, pool):
            effect, request, created = await self._effect(session, pool, effect_id)
            if effect.phase == "rejected" and effect.rejection_status == status_code:
                return _deletion(effect, request, created)
            if effect.phase != "dispatched":
                raise PoolGatewayError
            effect = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="rejected", rejection_status=status_code).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            return _deletion(effect, request, created)
