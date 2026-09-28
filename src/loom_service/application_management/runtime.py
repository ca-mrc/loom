"""Fixed application Kubernetes runtime gates, not a completed lifecycle worker.

Closing Pod admission does not stop existing processes, revoke credentials, prove
readiness or release capacity. Protected installation supplies the authority and
qualifies actual Kubernetes admission before this internal adapter is used.
"""
from __future__ import annotations

import hashlib
from typing import Any
from uuid import UUID

from loom.nebius_application_authority import (
    APPLICATION_INSTALLATION_LABEL,
    ApplicationNamespaceAuthorityV1,
    application_pod_fence,
)
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom_service.application_management.effects import ApplicationEffect
from loom_service.application_management.kubernetes import (
    ApplicationKubernetesProvider,
    KubernetesEffectRejectedError,
)
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError

_FENCE = "loom-application-retired"


class ApplicationRuntimeProvider:
    def __init__(self, registry: ApplicationRegistry, kubernetes: ApplicationKubernetesProvider, *,
                 authority: ApplicationNamespaceAuthorityV1):
        self.registry, self.kubernetes = registry, kubernetes
        self.authority = ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())

    async def _fence(self, lease: ApplicationLease, *, operation_id: UUID | None = None) -> dict[str, Any]:
        plan = await self.registry.frozen_plan(lease, operation_id=operation_id)
        try:
            row = ApplicationRegistrationV1.model_validate(plan["registration"])
            namespace = next(doc for docs in plan["files"].values() for doc in docs if doc["kind"] == "Namespace")
            if (plan["shared"]["platform_namespace"] != self.authority.shared_namespace
                    or namespace["metadata"]["labels"].get(APPLICATION_INSTALLATION_LABEL)
                    != str(self.authority.installation_id)):
                raise ValueError
            return application_pod_fence(self.authority, row, operation_id=operation_id or lease.operation_id)
        except (ValueError, TypeError, KeyError, StopIteration):
            raise ProviderBlockedError("application_runtime_authority_conflict") from None

    async def _history(self, lease: ApplicationLease) -> list[ApplicationEffect]:
        return [effect for effect in await self.registry.effect_history(lease)
                if effect.intent.kind == "ResourceQuota" and effect.intent.name == _FENCE]

    async def _read(self, lease: ApplicationLease, namespace: str) -> dict[str, Any] | None:
        await self.kubernetes._namespace(lease, "ResourceQuota", namespace)
        actual = await self.kubernetes._read(f"/api/v1/namespaces/{namespace}/resourcequotas/{_FENCE}")
        await self.kubernetes._namespace(lease, "ResourceQuota", namespace)
        await self.registry.frozen_plan(lease)
        return actual

    async def _identity(self, lease: ApplicationLease, actual: dict[str, Any],
                         history: list[ApplicationEffect]) -> ApplicationEffect:
        metadata = actual["metadata"]
        annotations = metadata.get("annotations", {})
        recorded = next((effect for effect in reversed(history) if effect.phase == "observed"
            and effect.intent.action in {"create", "patch"} and effect.observed_uid == metadata["uid"]
            and annotations.get("loom.nebius/operation-id") == str(effect.operation_id)
            and annotations.get("loom.nebius/effect-key") == effect.key), None)
        if recorded is None:
            raise ProviderBlockedError("application_pod_fence_conflict")
        expected = await self._fence(lease, operation_id=recorded.operation_id)
        spec = actual.get("spec", {})
        if (not _contains(actual, expected) or spec.get("hard") != {"pods": "0"}
                or spec.get("scopes") or spec.get("scopeSelector") is not None
                or metadata.get("deletionTimestamp")):
            raise ProviderBlockedError("application_pod_fence_conflict")
        return recorded

    async def close_admission(self, lease: ApplicationLease) -> None:
        """Close and confirm the fixed zero-Pod quota without ever unfencing."""
        document = await self._fence(lease)
        namespace = document["metadata"]["namespace"]
        for effect in await self._history(lease):
            if effect.phase == "prepared" and effect.operation_id == lease.operation_id:
                # Resume the original unsent request before considering a new
                # RV-derived key. A controller may have changed status since
                # preparation; only the API can definitively reject its old RV.
                try:
                    if effect.intent.action == "create":
                        await self.kubernetes.create(lease, effect.key, document)
                    elif effect.intent.action == "patch":
                        assert effect.intent.uid is not None and effect.intent.resource_version is not None
                        await self.kubernetes.patch_spec(lease, effect.key, document,
                            uid=effect.intent.uid, resource_version=effect.intent.resource_version)
                    else:
                        # Closing admission never sends an unfence request.
                        raise ProviderWaitingError("application_pod_fence_pending")
                except KubernetesEffectRejectedError:
                    raise ProviderWaitingError("application_pod_fence_pending") from None
            if effect.phase == "dispatched":
                original = (None if effect.intent.action == "delete"
                            else await self._fence(lease, operation_id=effect.operation_id))
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key, document=original)
        history = await self._history(lease)
        actual = await self._read(lease, namespace)
        if actual is None:
            retired = {effect.observed_uid for effect in history
                       if effect.phase == "observed" and effect.intent.action == "delete"}
            if any(effect.phase == "observed" and effect.intent.action == "create"
                   and effect.observed_uid not in retired for effect in history):
                raise ProviderBlockedError("application_pod_fence_conflict")
            await self.kubernetes.create(lease, "pod-fence:create", document)
        else:
            recorded = await self._identity(lease, actual, history)
            if recorded.operation_id != lease.operation_id:
                metadata = actual["metadata"]
                # A new key is only eligible after a definitive rejection and
                # new exact preconditions. An uncertain patch is reconciled above.
                identity = metadata["uid"] + ":" + metadata["resourceVersion"]
                key = "pod-fence:advance:" + hashlib.sha256(identity.encode()).hexdigest()
                try:
                    await self.kubernetes.patch_spec(lease, key, document,
                        uid=metadata["uid"], resource_version=metadata["resourceVersion"])
                except KubernetesEffectRejectedError:
                    raise ProviderWaitingError("application_pod_fence_pending") from None
        actual = await self._read(lease, namespace)
        if actual is None:
            raise ProviderBlockedError("application_pod_fence_conflict")
        recorded = await self._identity(lease, actual, await self._history(lease))
        if (recorded.operation_id != lease.operation_id
                or actual.get("status", {}).get("hard", {}).get("pods") != "0"):
            raise ProviderWaitingError("application_pod_fence_pending")
        await self.registry.frozen_plan(lease)
