"""One-attempt application IAM changes, distinct from legacy environment setup.

The installed lifecycle caller qualifies shared groups and controls ordering.
Resource observation is neither S3 revocation propagation nor runtime readiness.
No database transaction is held across an SDK call.
"""
from __future__ import annotations

import re
from typing import Any, Protocol
from uuid import UUID, uuid5

from loom.nebius_environment_contract import _PROVIDER_ID
from loom_service.application_management.cloud_effects import (
    ApplicationCloudEffect,
    ApplicationCloudJournal,
)
from loom_service.application_management.leases import ApplicationLease
from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError


class ApplicationCloudApi(Protocol):
    async def find(self, kind: str, expected: dict[str, Any]) -> dict[str, Any] | None: ...
    async def create(self, kind: str, expected: dict[str, Any], *, idempotency_key: str) -> dict[str, Any]: ...
    async def get_resource(self, kind: str, identity: str) -> dict[str, Any] | None: ...
    async def delete_resource(self, kind: str, identity: str, *, idempotency_key: str) -> None: ...
    async def access_key_secret(self, identity: str) -> dict[str, str]: ...


class ApplicationCloudProvider:
    def __init__(self, registry: ApplicationCloudJournal, api: ApplicationCloudApi):
        self.registry, self.api = registry, api

    async def _effect(self, lease: ApplicationLease, operation_id: UUID, key: str) -> ApplicationCloudEffect:
        # History validates the current lease and restricts sibling/future access.
        for effect in await self.registry.cloud_history(lease):
            if effect.operation_id == operation_id and effect.key == key:
                return effect
        raise ProviderBlockedError("application_cloud_effect_missing")

    @staticmethod
    def _identity(effect: ApplicationCloudEffect, value: dict[str, Any]) -> str:
        metadata = value.get("metadata")
        identity = metadata.get("id") if isinstance(metadata, dict) else None
        if (not isinstance(identity, str) or re.fullmatch(_PROVIDER_ID, identity) is None
                or not _contains(value, effect.expected)
                or (effect.observed_resource_id is not None and identity != effect.observed_resource_id)
                or (effect.resource_id is not None and identity != effect.resource_id)):
            raise ProviderBlockedError("application_cloud_identity_conflict")
        return identity

    @staticmethod
    def _key(effect: ApplicationCloudEffect) -> str:
        return str(uuid5(effect.operation_id, "application-cloud:" + effect.key))

    async def _observe(self, lease: ApplicationLease, effect: ApplicationCloudEffect,
                       resource_id: str) -> ApplicationCloudEffect:
        await self.registry.observe_cloud_effect(lease, effect.operation_id, effect.key, resource_id=resource_id)
        return await self._effect(lease, effect.operation_id, effect.key)

    async def create(self, lease: ApplicationLease, key: str, binding: dict[str, Any]) -> ApplicationCloudEffect:
        effect = await self.registry.prepare_cloud_create(lease, key, binding)
        if effect.phase != "prepared":
            return await self.reconcile(lease, effect.operation_id, effect.key)
        existing = await self.api.find(effect.kind, effect.expected)
        if existing is not None:
            # A concurrent winner may have dispatched while this caller read.
            effect = await self._effect(lease, effect.operation_id, effect.key)
            if effect.phase == "prepared":
                raise ProviderBlockedError("application_cloud_unrecorded_resource")
            return await self.reconcile(lease, effect.operation_id, effect.key)
        if not await self.registry.dispatch_cloud_effect(lease, effect.key):
            return await self.reconcile(lease, effect.operation_id, effect.key)
        actual = await self.api.create(effect.kind, effect.expected, idempotency_key=self._key(effect))
        return await self._observe(lease, effect, self._identity(effect, actual))

    async def reconcile(self, lease: ApplicationLease, operation_id: UUID, key: str) -> ApplicationCloudEffect:
        """Read-only provider reconciliation, including superseded dispatched requests."""
        effect = await self._effect(lease, operation_id, key)
        if effect.phase == "prepared":
            raise ProviderWaitingError("application_cloud_not_dispatched")
        if effect.action == "delete":
            if effect.resource_id is None:
                raise ProviderBlockedError("application_cloud_identity_missing")
            actual = await self.api.get_resource(effect.kind, effect.resource_id)
            if actual is not None:
                self._identity(effect, actual)
                raise ProviderWaitingError("application_cloud_unconfirmed")
            return await self._observe(lease, effect, effect.resource_id)
        actual = (await self.api.get_resource(effect.kind, effect.observed_resource_id)
                  if effect.observed_resource_id is not None else await self.api.find(effect.kind, effect.expected))
        if actual is None:
            if effect.phase == "observed":
                raise ProviderBlockedError("application_cloud_recorded_resource_missing")
            raise ProviderWaitingError("application_cloud_unconfirmed")
        return await self._observe(lease, effect, self._identity(effect, actual))

    async def delete(self, lease: ApplicationLease, operation_id: UUID, key: str) -> ApplicationCloudEffect:
        effect = await self.registry.prepare_cloud_delete(lease, operation_id, key)
        if effect.phase != "prepared":
            return await self.reconcile(lease, effect.operation_id, effect.key)
        if effect.resource_id is None:
            raise ProviderBlockedError("application_cloud_identity_missing")
        current = await self.api.get_resource(effect.kind, effect.resource_id)
        if current is not None:
            self._identity(effect, current)
        if not await self.registry.dispatch_cloud_effect(lease, effect.key):
            return await self.reconcile(lease, effect.operation_id, effect.key)
        if current is not None:
            await self.api.delete_resource(effect.kind, effect.resource_id, idempotency_key=self._key(effect))
        return await self.reconcile(lease, effect.operation_id, effect.key)

    async def key_material(self, lease: ApplicationLease) -> dict[str, str]:
        # Only this active generation's confirmed key may supply new material.
        effect = await self.reconcile(lease, lease.operation_id, "key")
        if effect.kind != "access_key" or effect.action != "create" or effect.observed_resource_id is None:
            raise ProviderBlockedError("application_cloud_identity_missing")
        value = await self.api.access_key_secret(effect.observed_resource_id)
        if (set(value) != {"access-key", "secret-key"}
                or any(not isinstance(item, str) or not 1 <= len(item) <= 4096 for item in value.values())):
            raise ProviderBlockedError("application_cloud_material_invalid")
        await self._effect(lease, effect.operation_id, effect.key)
        return value
