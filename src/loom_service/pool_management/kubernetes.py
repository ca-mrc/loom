"""Fixed pool mutations; no caller manifests, ambient kubeconfig or write retries.

The journal commits one dispatch permit before HTTP. A timeout (even followed by
404) can only be reconciled, never resent. Observed effects are history, so a
caller reusing an auxiliary always verifies its live UID and frozen contents.
The installer owns the restricted HTTPS client and its lifetime.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from uuid import UUID

import httpx

from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.gateway_journal import (
    CreateKind,
    PoolGatewayDeletion,
    PoolGatewayEffect,
    PoolGatewayJournal,
)
from loom_service.pool_management.kubernetes_identity import matches_frozen_workload

_MAX_BODY = 2 * 1024 * 1024
_TIMEOUT = 30


class PoolKubernetesError(ValueError):
    def __init__(self, reason: str = "pool_kubernetes_identity_conflict") -> None:
        super().__init__(reason)


class PoolKubernetesWaitingError(PoolKubernetesError):
    def __init__(self) -> None:
        super().__init__("pool_kubernetes_unconfirmed")


class PoolKubernetesRejectedError(PoolKubernetesError):
    def __init__(self, status_code: int) -> None:
        super().__init__("pool_kubernetes_effect_rejected")
        self.status_code = status_code


class KubernetesPoolGateway:
    def __init__(self, journal: PoolGatewayJournal, http: httpx.AsyncClient) -> None:
        url = http.base_url
        if url.scheme != "https" or url.path != "/" or url.query or url.fragment or url.userinfo:
            raise ValueError("pool Kubernetes endpoint must be an HTTPS origin")
        self.journal, self.http = journal, http

    async def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any] | None:
        try:
            async with asyncio.timeout(_TIMEOUT):
                async with self.http.stream(method, path, json=body, follow_redirects=False, timeout=_TIMEOUT) as response:
                    if response.status_code == 404 and method in {"GET", "DELETE"}:
                        return None
                    if method != "GET" and response.status_code in {409, 422}:
                        raise PoolKubernetesRejectedError(response.status_code)
                    if response.status_code in {409, 422, 429} or response.status_code >= 500:
                        raise PoolKubernetesWaitingError
                    if response.status_code not in {200, 201, 202}:
                        raise PoolKubernetesError("pool_kubernetes_request_rejected")
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(data) + len(chunk) > _MAX_BODY:
                            raise PoolKubernetesError("pool_kubernetes_invalid_response")
                        data.extend(chunk)
                    value = json.loads(data)
                    if not isinstance(value, dict):
                        raise ValueError
                    return value
        except (httpx.TransportError, TimeoutError):
            raise PoolKubernetesWaitingError from None
        except PoolKubernetesError:
            raise
        except (ValueError, UnicodeError):
            raise PoolKubernetesError("pool_kubernetes_invalid_response") from None

    @staticmethod
    def _identity(value: dict[str, Any], *, terminating: bool = False) -> tuple[UUID, str]:
        try:
            metadata = value["metadata"]
            uid, rv = UUID(metadata["uid"]), metadata["resourceVersion"]
            if (not uid.int or str(uid) != metadata["uid"] or not isinstance(rv, str)
                    or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", rv) is None
                    or (metadata.get("deletionTimestamp") and not terminating)):
                raise ValueError
            return uid, rv
        except (ValueError, TypeError, KeyError, AttributeError):
            raise PoolKubernetesError from None

    async def _namespace(self, effect: PoolGatewayEffect) -> None:
        name = effect.document["metadata"]["namespace"]
        value = await self._request("GET", "/api/v1/namespaces/" + name)
        if (value is None or value.get("kind") != "Namespace" or value.get("apiVersion") != "v1"
                or self._identity(value)[0] != effect.namespace_uid or value["metadata"].get("name") != name):
            raise PoolKubernetesError("pool_kubernetes_namespace_conflict")

    @staticmethod
    def _collection(effect: PoolGatewayEffect) -> str:
        document = effect.document
        prefix, resource = (("/apis/batch/v1", "jobs") if document["kind"] == "Job" else ("/api/v1", "configmaps"))
        namespace: str = document["metadata"]["namespace"]
        return prefix + "/namespaces/" + namespace + "/" + resource

    async def _observe(self, principal: PoolPrincipal, effect: PoolGatewayEffect) -> PoolGatewayEffect:
        value = await self._request("GET", self._collection(effect) + "/" + effect.document["metadata"]["name"])
        if value is None:
            raise PoolKubernetesWaitingError
        uid, rv = self._identity(value)
        if ((effect.observed_uid is not None and effect.observed_uid != uid)
                or not matches_frozen_workload(value, effect.document)):
            raise PoolKubernetesError
        await self._namespace(effect)
        return await self.journal.observe_create(principal, effect.effect_id, uid=uid, resource_version=rv)

    async def create(self, principal: PoolPrincipal, reservation_id: UUID, *, kind: CreateKind) -> PoolGatewayEffect:
        effect = await self.journal.prepare_create(principal, reservation_id, kind=kind)
        if effect.phase == "rejected":
            assert effect.rejection_status is not None
            raise PoolKubernetesRejectedError(effect.rejection_status)
        await self._namespace(effect)
        if effect.phase == "prepared":
            if kind == "Job" and effect.requires_configmap:
                auxiliary = await self.journal.prepare_create(principal, reservation_id, kind="ConfigMap")
                await self._observe(principal, auxiliary)
            permit = await self.journal.dispatch_create(principal, effect.effect_id)
            if permit is not None:
                # Always use the freshly qualified committed permit, not a copy
                # retained across the network reads above. No retry surrounds I/O.
                try:
                    await self._request("POST", self._collection(permit), permit.document)
                except PoolKubernetesRejectedError as exc:
                    await self.journal.reject_create(principal, effect.effect_id, status_code=exc.status_code)
                    raise
        current = await self.journal.get_effect(principal, effect.effect_id)
        if current.phase == "rejected":
            assert current.rejection_status is not None
            raise PoolKubernetesRejectedError(current.rejection_status)
        return await self._observe(principal, current)

    async def _delete_present(self, effect: PoolGatewayDeletion, *, qualify: bool = False) -> bool:
        created = effect.created
        path = self._collection(created) + "/" + created.document["metadata"]["name"]
        value = await self._request("GET", path)
        if value is None:
            return False
        if self._identity(value, terminating=True)[0] != created.observed_uid:
            # A replacement is neither our delete target nor qualified absence.
            raise PoolKubernetesError
        if qualify:
            value["metadata"].pop("deletionTimestamp", None)
            value["metadata"].pop("deletionGracePeriodSeconds", None)
            if not matches_frozen_workload(value, created.document):
                raise PoolKubernetesError
        return True

    async def delete(self, principal: PoolPrincipal, reservation_id: UUID, *, kind: CreateKind) -> PoolGatewayDeletion:
        """Retire an observed fixed object after separately authorized output drain.

        Returning an observed deletion proves only this object's live absence,
        never Pod absence, a fenced writer, task completion or capacity release.
        """
        effect = await self.journal.prepare_delete(principal, reservation_id, kind=kind)
        if effect.phase == "rejected":
            assert effect.rejection_status is not None
            raise PoolKubernetesRejectedError(effect.rejection_status)
        await self._namespace(effect.created)
        if effect.phase == "prepared":
            present = await self._delete_present(effect, qualify=True)
            permit = await self.journal.dispatch_delete(principal, effect.effect_id)
            if permit is not None and present:
                path = self._collection(permit.created) + "/" + permit.created.document["metadata"]["name"]
                try:
                    await self._request("DELETE", path, permit.document)
                except PoolKubernetesRejectedError as exc:
                    await self.journal.reject_delete(principal, effect.effect_id, status_code=exc.status_code)
                    raise
        current = await self.journal.get_delete(principal, effect.effect_id)
        if current.phase == "rejected":
            assert current.rejection_status is not None
            raise PoolKubernetesRejectedError(current.rejection_status)
        if await self._delete_present(current):
            raise PoolKubernetesWaitingError
        await self._namespace(current.created)
        return await self.journal.observe_delete(principal, effect.effect_id)
