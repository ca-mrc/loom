"""Fixed mutation journal. Dispatch permission returns only AFTER its own commit.

This trusted internal interface receives identities, never caller manifests. It
does not perform Kubernetes I/O, retry an uncertain write or release capacity.
Actual namespace/object verification belongs to the fixed gateway provider.
"""
from __future__ import annotations

import copy
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolEffect,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.locks import acquire_pool_mutation_lock

CreateKind = Literal["Job", "ConfigMap"]


class PoolGatewayError(ValueError):
    def __init__(self, reason: str = "pool_gateway_unavailable") -> None:
        super().__init__(reason)


@dataclass(frozen=True)
class PoolGatewayEffect:
    effect_id: UUID
    reservation_id: UUID
    namespace_uid: UUID
    phase: str
    document: dict[str, Any]
    dispatch_id: UUID | None
    observed_uid: UUID | None
    observed_resource_version: str | None
    rejection_status: int | None
    requires_configmap: bool


@dataclass(frozen=True)
class PoolGatewayDeletion:
    effect_id: UUID
    phase: str
    created: PoolGatewayEffect
    document: dict[str, Any]
    dispatch_id: UUID | None
    rejection_status: int | None


def _document(request: NebiusPoolRequest, effect_id: UUID, kind: CreateKind) -> dict[str, Any]:
    if kind not in {"Job", "ConfigMap"} or request.plan_json is None:
        raise PoolGatewayError
    value = request.plan_json["job" if kind == "Job" else "configmap"]
    if not isinstance(value, dict):
        raise PoolGatewayError
    document = copy.deepcopy(value)
    metadata = document["metadata"]
    if (document["kind"] != kind or document["apiVersion"] != ("batch/v1" if kind == "Job" else "v1")
            or metadata["name"] != f"loom-pool-{request.request_id.hex}"
            or not isinstance(metadata["namespace"], str)
            or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", metadata["namespace"]) is None):
        raise PoolGatewayError
    markers = {"loom.nebius/pool-reservation-id": str(request.request_id),
               "loom.nebius/pool-plan-sha256": request.plan_sha256,
               "loom.nebius/pool-effect-id": str(effect_id)}
    metadata.setdefault("annotations", {}).update(markers)
    if kind == "Job":
        document["spec"]["template"]["metadata"].setdefault("annotations", {}).update(markers)
    return document


def _intent(document: dict[str, Any]) -> dict[str, Any]:
    return {"api_version": document["apiVersion"], "kind": document["kind"], "action": "create",
            "name": document["metadata"]["name"], "namespace": document["metadata"]["namespace"],
            "request_sha256": digest(document), "uid": None, "resource_version": None}


def _view(effect: NebiusPoolEffect, request: NebiusPoolRequest) -> PoolGatewayEffect:
    document = _document(request, effect.effect_id, effect.intent_json["kind"])
    if effect.intent_json != _intent(document) or (effect.plan_sha256, effect.namespace_uid) != (request.plan_sha256, request.namespace_uid):
        raise PoolGatewayError
    return PoolGatewayEffect(effect.effect_id, request.request_id, request.namespace_uid, effect.phase, document,
        effect.dispatch_id, effect.observed_uid, effect.observed_resource_version, effect.rejection_status,
        request.plan_json is not None and request.plan_json["configmap"] is not None)


def _delete_document(created: PoolGatewayEffect, request: NebiusPoolRequest) -> dict[str, Any]:
    if created.phase != "observed" or created.observed_uid is None:
        raise PoolGatewayError
    if request.stop_json is None:
        raise PoolGatewayError("pool_gateway_stop_not_authorized")
    if created.document["kind"] == "Job":
        grace = request.stop_json.get("grace_seconds")
        if type(grace) is not int or not 0 <= grace <= 300:
            raise PoolGatewayError
        return {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
                "gracePeriodSeconds": grace, "preconditions": {"uid": str(created.observed_uid)}}
    if request.drain_json is None:
        raise PoolGatewayError("pool_gateway_output_not_drained")
    return {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
            "preconditions": {"uid": str(created.observed_uid)}}


def _delete_intent(created: PoolGatewayEffect, request: NebiusPoolRequest) -> dict[str, Any]:
    return _intent(created.document) | {"action": "delete", "uid": str(created.observed_uid),
        "request_sha256": digest(_delete_document(created, request)), "create_effect_id": str(created.effect_id)}


def _delete_view(effect: NebiusPoolEffect, request: NebiusPoolRequest, created: NebiusPoolEffect) -> PoolGatewayDeletion:
    create_view = _view(created, request)
    if (effect.intent_json != _delete_intent(create_view, request)
            or (effect.plan_sha256, effect.namespace_uid) != (request.plan_sha256, request.namespace_uid)):
        raise PoolGatewayError
    return PoolGatewayDeletion(effect.effect_id, effect.phase, create_view, _delete_document(create_view, request),
                               effect.dispatch_id, effect.rejection_status)


class PoolGatewayJournal:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    @asynccontextmanager
    async def _transaction(self, principal: PoolPrincipal) -> AsyncIterator[tuple[AsyncSession, NebiusPoolBinding]]:
        try:
            async with self.sessions.begin() as session:
                await acquire_pool_mutation_lock(session)
                pool = (await session.scalars(select(NebiusPoolBinding).where(
                    NebiusPoolBinding.pool_id == principal.pool_id).with_for_update())).one_or_none()
                await authorize_pool_machine(session, principal, role="gateway", pool_id=principal.pool_id)
                if pool is None or digest(pool.binding_json) != pool.binding_sha256:
                    raise PoolGatewayError
                yield session, pool
        except PoolGatewayError:
            raise
        except (ValueError, KeyError, TypeError, DBAPIError):
            raise PoolGatewayError from None

    async def _request(self, session: AsyncSession, pool: NebiusPoolBinding, reservation_id: UUID) -> NebiusPoolRequest:
        request = (await session.scalars(select(NebiusPoolRequest).where(
            NebiusPoolRequest.pool_id == pool.pool_id, NebiusPoolRequest.request_id == reservation_id,
        ).with_for_update())).one_or_none()
        if (request is None or request.plan_json is None or digest(request.plan_json) != request.plan_sha256
                or request.plan_json.get("schema_version") != "loom.pool-workload-plan.v1"
                or digest(request.request_json) != request.request_sha256
                or request.plan_json.get("request_sha256") != request.request_sha256):
            raise PoolGatewayError
        return request

    async def _effect(self, session: AsyncSession, pool: NebiusPoolBinding, effect_id: UUID) -> tuple[NebiusPoolEffect, NebiusPoolRequest]:
        # Immutable locator only; mutation locks still precede the request/effect.
        reservation_id = await session.scalar(select(NebiusPoolEffect.request_id).where(NebiusPoolEffect.effect_id == effect_id))
        if reservation_id is None:
            raise PoolGatewayError
        request = await self._request(session, pool, reservation_id)
        effect = (await session.scalars(select(NebiusPoolEffect).where(
            NebiusPoolEffect.effect_id == effect_id).with_for_update())).one()
        _view(effect, request)
        return effect, request

    async def _new_create(self, session: AsyncSession, pool: NebiusPoolBinding, request: NebiusPoolRequest, kind: CreateKind) -> None:
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        participant = (await session.scalars(select(NebiusPoolParticipant).where(
            NebiusPoolParticipant.participant_id == request.participant_id).with_for_update(read=True))).one()
        binding = PoolParticipantV1.model_validate(participant.binding_json)
        namespace = (binding.build_namespace if request.workload_kind in {"task_image_build", "application_image_build"}
                     else binding.execution_namespace)
        if (request.phase != "create_intent" or pool.mode != "global" or request.deadline_at <= now
                or request.admission_epoch != pool.admission_epoch or participant.phase != "active"
                or participant.admission_epoch != pool.admission_epoch or binding.admission_epoch != pool.admission_epoch
                or binding.participant_id != request.participant_id or binding.pool_id != pool.pool_id
                or participant.binding_revision != request.request_json["participant_revision"]
                or digest(participant.binding_json) != participant.binding_sha256 or namespace.uid != request.namespace_uid
                or request.plan_json is None or request.plan_json["job"]["metadata"]["namespace"] != namespace.name):
            raise PoolGatewayError
        if kind == "Job" and request.plan_json["configmap"] is not None:
            auxiliary = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == request.request_id, NebiusPoolEffect.effect_key == "create:configmap"))
            if auxiliary is None or auxiliary.phase != "observed":
                raise PoolGatewayError("pool_gateway_configmap_unconfirmed")

    async def prepare_create(self, principal: PoolPrincipal, reservation_id: UUID, *, kind: CreateKind) -> PoolGatewayEffect:
        async with self._transaction(principal) as (session, pool):
            request = await self._request(session, pool, reservation_id)
            if kind not in {"Job", "ConfigMap"}:
                raise PoolGatewayError
            key = "create:" + kind.lower()
            existing = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == reservation_id, NebiusPoolEffect.effect_key == key))
            if existing is not None:
                return _view(existing, request)
            await self._new_create(session, pool, request, kind)
            effect_id = uuid4()
            document = _document(request, effect_id, kind)
            last = await session.scalar(select(func.max(NebiusPoolEffect.sequence)).where(NebiusPoolEffect.request_id == reservation_id))
            effect = (await session.scalars(insert(NebiusPoolEffect).values(
                effect_id=effect_id, request_id=reservation_id, plan_sha256=request.plan_sha256,
                namespace_uid=request.namespace_uid, effect_key=key, sequence=(last or 0) + 1,
                intent_json=_intent(document), phase="prepared").returning(NebiusPoolEffect))).one()
            return _view(effect, request)

    async def get_effect(self, principal: PoolPrincipal, effect_id: UUID) -> PoolGatewayEffect:
        async with self._transaction(principal) as (session, pool):
            effect, request = await self._effect(session, pool, effect_id)
            return _view(effect, request)

    async def get_created(self, principal: PoolPrincipal, reservation_id: UUID, *, kind: CreateKind) -> PoolGatewayEffect:
        """Read existing observed authority only, without preparing an effect."""
        async with self._transaction(principal) as (session, pool):
            request = await self._request(session, pool, reservation_id)
            return _view(await self._created(session, request, kind), request)

    async def dispatch_create(self, principal: PoolPrincipal, effect_id: UUID) -> PoolGatewayEffect | None:
        async with self._transaction(principal) as (session, pool):
            effect, request = await self._effect(session, pool, effect_id)
            if effect.phase != "prepared":
                return None  # Historical dispatch/rejection is NEVER another permit.
            await self._new_create(session, pool, request, effect.intent_json["kind"])
            dispatched = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="dispatched", dispatch_id=uuid4(), dispatch_machine_id=principal.machine_id,
                dispatch_epoch=principal.credential_epoch).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            result = _view(dispatched, request)
        # Unlike participant preparation, dispatch OWNS the commit. A failed or
        # uncertain commit cannot return permission for an external write.
        return result

    async def observe_create(self, principal: PoolPrincipal, effect_id: UUID, *, uid: UUID,
                             resource_version: str) -> PoolGatewayEffect:
        if not isinstance(uid, UUID) or not uid.int or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", resource_version) is None:
            raise PoolGatewayError
        async with self._transaction(principal) as (session, pool):
            effect, request = await self._effect(session, pool, effect_id)
            if effect.phase == "observed":
                if effect.observed_uid != uid:
                    raise PoolGatewayError
                return _view(effect, request)  # Retain first RV when only server decoration advanced.
            if effect.phase != "dispatched" or request.phase not in {"create_intent", "observed", "cleanup_intent"}:
                raise PoolGatewayError
            if effect.intent_json["kind"] == "Job":
                if request.job_uid is not None and request.job_uid != uid:
                    raise PoolGatewayError
                await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == request.request_id).values(
                    job_uid=uid, phase="observed" if request.phase == "create_intent" else request.phase))
            observed = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="observed", observed_uid=uid, observed_resource_version=resource_version)
                .returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            return _view(observed, request)

    async def reject_create(self, principal: PoolPrincipal, effect_id: UUID, *, status_code: int) -> PoolGatewayEffect:
        if type(status_code) is not int or status_code not in {409, 422}:
            raise PoolGatewayError
        async with self._transaction(principal) as (session, pool):
            effect, request = await self._effect(session, pool, effect_id)
            if effect.phase == "rejected" and effect.rejection_status == status_code:
                return _view(effect, request)
            if effect.phase != "dispatched":
                raise PoolGatewayError
            rejected = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="rejected", rejection_status=status_code).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            return _view(rejected, request)

    async def _deletion(self, session: AsyncSession, pool: NebiusPoolBinding, effect_id: UUID) -> tuple[
        NebiusPoolEffect, NebiusPoolRequest, NebiusPoolEffect,
    ]:
        reservation_id = await session.scalar(select(NebiusPoolEffect.request_id).where(NebiusPoolEffect.effect_id == effect_id))
        if reservation_id is None:
            raise PoolGatewayError
        request = await self._request(session, pool, reservation_id)
        effect = (await session.scalars(select(NebiusPoolEffect).where(
            NebiusPoolEffect.effect_id == effect_id).with_for_update())).one()
        created = await self._created(session, request, effect.intent_json["kind"])
        _delete_view(effect, request, created)
        return effect, request, created

    async def _created(self, session: AsyncSession, request: NebiusPoolRequest, kind: CreateKind) -> NebiusPoolEffect:
        if kind not in {"Job", "ConfigMap"}:
            raise PoolGatewayError
        created = await session.scalar(select(NebiusPoolEffect).where(
            NebiusPoolEffect.request_id == request.request_id, NebiusPoolEffect.effect_key == "create:" + kind.lower()))
        if created is None or created.phase != "observed":
            raise PoolGatewayError
        _view(created, request)
        return created

    async def prepare_delete(self, principal: PoolPrincipal, reservation_id: UUID, *, kind: CreateKind) -> PoolGatewayDeletion:
        """Trusted internal cleanup only; no caller-supplied target or UID.

        Stop permits foreground Job termination before output drain. Auxiliary
        cleanup separately requires the participant's retained drain evidence.
        Deletion observations NEVER release capacity or assert residual Pod absence.
        """
        async with self._transaction(principal) as (session, pool):
            request = await self._request(session, pool, reservation_id)
            created = await self._created(session, request, kind)
            key = "delete:" + kind.lower()
            existing = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == reservation_id, NebiusPoolEffect.effect_key == key))
            if existing is not None:
                return _delete_view(existing, request, created)
            if request.phase != "cleanup_intent":
                raise PoolGatewayError("pool_gateway_cleanup_not_authorized")
            last = await session.scalar(select(func.max(NebiusPoolEffect.sequence)).where(NebiusPoolEffect.request_id == reservation_id))
            effect = (await session.scalars(insert(NebiusPoolEffect).values(
                effect_id=uuid4(), request_id=reservation_id, plan_sha256=request.plan_sha256,
                namespace_uid=request.namespace_uid, effect_key=key, sequence=(last or 0) + 1,
                intent_json=_delete_intent(_view(created, request), request), phase="prepared").returning(NebiusPoolEffect))).one()
            return _delete_view(effect, request, created)

    async def get_delete(self, principal: PoolPrincipal, effect_id: UUID) -> PoolGatewayDeletion:
        async with self._transaction(principal) as (session, pool):
            effect, request, created = await self._deletion(session, pool, effect_id)
            return _delete_view(effect, request, created)

    async def dispatch_delete(self, principal: PoolPrincipal, effect_id: UUID) -> PoolGatewayDeletion | None:
        async with self._transaction(principal) as (session, pool):
            effect, request, created = await self._deletion(session, pool, effect_id)
            if effect.phase != "prepared":
                return None
            if request.phase != "cleanup_intent":
                raise PoolGatewayError("pool_gateway_cleanup_not_authorized")
            dispatched = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="dispatched", dispatch_id=uuid4(), dispatch_machine_id=principal.machine_id,
                dispatch_epoch=principal.credential_epoch).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            result = _delete_view(dispatched, request, created)
        return result

    async def observe_delete(self, principal: PoolPrincipal, effect_id: UUID) -> PoolGatewayDeletion:
        async with self._transaction(principal) as (session, pool):
            effect, request, created = await self._deletion(session, pool, effect_id)
            if effect.phase == "observed":
                return _delete_view(effect, request, created)
            if effect.phase != "dispatched" or request.phase != "cleanup_intent":
                raise PoolGatewayError
            observed = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="observed", observed_uid=created.observed_uid).returning(NebiusPoolEffect)
                .execution_options(populate_existing=True))).one()
            return _delete_view(observed, request, created)

    async def reject_delete(self, principal: PoolPrincipal, effect_id: UUID, *, status_code: int) -> PoolGatewayDeletion:
        if type(status_code) is not int or status_code not in {409, 422}:
            raise PoolGatewayError
        async with self._transaction(principal) as (session, pool):
            effect, request, created = await self._deletion(session, pool, effect_id)
            if effect.phase == "rejected" and effect.rejection_status == status_code:
                return _delete_view(effect, request, created)
            if effect.phase != "dispatched":
                raise PoolGatewayError
            rejected = (await session.scalars(update(NebiusPoolEffect).where(NebiusPoolEffect.effect_id == effect_id).values(
                phase="rejected", rejection_status=status_code).returning(NebiusPoolEffect).execution_options(populate_existing=True))).one()
            return _delete_view(rejected, request, created)
