"""Fixed application Kubernetes runtime gates, not a completed lifecycle worker.

Closing Pod admission does not stop existing processes, revoke credentials, prove
readiness or release capacity. Protected installation supplies the authority and
qualifies actual Kubernetes admission before this internal adapter is used.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import re
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
from loom_service.application_management.proofs import (
    ApplicationDeploymentRetirement,
    ApplicationResourceObservation,
    ApplicationRetirementIdentity,
    ApplicationWorkloadRetirement,
)
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError

_FENCE = "loom-application-retired"
_WORKLOAD_RESOURCES = {"Deployment": ("apps/v1", "deployments"),
    "Ingress": ("networking.k8s.io/v1", "ingresses"), "Service": ("v1", "services")}
_SCALE_KEY = re.compile(r"retire:scale:([0-9a-f]{32}):[0-9a-f]{64}\Z")


def _observation(actual: dict[str, Any], recorded: ApplicationEffect) -> ApplicationResourceObservation:
    metadata = actual["metadata"]
    return ApplicationResourceObservation(operation_id=recorded.operation_id, key=recorded.key,
        name=metadata["name"], uid=metadata["uid"], resource_version=metadata["resourceVersion"])


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

    async def _request_document(self, lease: ApplicationLease, effect: ApplicationEffect) -> dict[str, Any]:
        """Recover exact request content from durable authority, never live fields."""
        match = _SCALE_KEY.fullmatch(effect.key)
        source = UUID(hex=match[1]) if match else effect.operation_id
        plan = await self.registry.frozen_plan(lease, operation_id=source)
        intent = effect.intent
        if intent.kind == "Secret" and intent.action == "create":
            material = await self.registry.load_material(lease, operation_id=source)
            if intent.name in material:
                return {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                    "metadata": {"name": intent.name, "namespace": intent.namespace},
                    "data": {key: base64.b64encode(value.encode()).decode()
                             for key, value in material[intent.name].items()}}
        for docs in plan["files"].values():
            for document in docs:
                if (document["kind"] == intent.kind and document["apiVersion"] == intent.api_version
                        and document["metadata"]["name"] == intent.name
                        and document["metadata"].get("namespace") == intent.namespace):
                    result: dict[str, Any] = copy.deepcopy(document)
                    if match:
                        if intent.kind != "Deployment" or intent.action != "patch":
                            break
                        result["spec"]["replicas"] = 0
                    return result
        raise ProviderBlockedError("application_kubernetes_request_conflict")

    async def _resume_retirement(self, lease: ApplicationLease) -> None:
        # A prepared request owns the current operation's journal slot. Resume
        # its original preconditions even if live resourceVersion has advanced.
        for effect in await self.registry.effect_history(lease):
            if effect.operation_id != lease.operation_id or effect.phase not in {"prepared", "dispatched"}:
                continue
            if effect.intent.kind == "ResourceQuota":
                continue  # close_admission owns only the fixed quota protocol.
            intent = effect.intent
            if not effect.key.startswith("retire:"):
                raise ProviderWaitingError("application_workloads_retirement_pending")
            document = None if intent.action == "delete" else await self._request_document(lease, effect)
            if effect.phase == "dispatched":
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key, document=document)
                continue
            assert intent.uid is not None and intent.resource_version is not None
            try:
                if intent.action == "delete" and intent.kind in {"Ingress", "Service"}:
                    assert intent.namespace is not None
                    await self.kubernetes.delete(lease, effect.key, api_version=intent.api_version,
                        kind=intent.kind, namespace=intent.namespace, name=intent.name,
                        uid=intent.uid, resource_version=intent.resource_version)
                elif intent.action == "patch" and _SCALE_KEY.fullmatch(effect.key):
                    assert document is not None
                    await self.kubernetes.patch_spec(lease, effect.key, document,
                        uid=intent.uid, resource_version=intent.resource_version)
                else:
                    raise ProviderBlockedError("application_kubernetes_request_conflict")
            except KubernetesEffectRejectedError:
                raise ProviderWaitingError("application_workloads_retirement_pending") from None

    async def _workload(self, lease: ApplicationLease, kind: str, namespace: str, name: str
                        ) -> tuple[dict[str, Any] | None, ApplicationEffect | None]:
        version, resource = _WORKLOAD_RESOURCES[kind]
        prefix = "/api/v1" if version == "v1" else "/apis/" + version
        await self.kubernetes._namespace(lease, kind, namespace)
        actual = await self.kubernetes._read(f"{prefix}/namespaces/{namespace}/{resource}/{name}")
        await self.kubernetes._namespace(lease, kind, namespace)
        history = [item for item in await self.registry.effect_history(lease)
                   if item.intent.kind == kind and item.intent.namespace == namespace and item.intent.name == name]
        if any(item.phase == "dispatched" or (item.phase == "prepared" and item.operation_id == lease.operation_id)
               for item in history):
            raise ProviderWaitingError("application_workloads_retirement_pending")
        if actual is None:
            deleted = {item.observed_uid for item in history
                       if item.phase == "observed" and item.intent.action == "delete"}
            if any(item.phase == "observed" and item.intent.action == "create"
                   and item.observed_uid not in deleted for item in history):
                raise ProviderBlockedError("application_workload_identity_conflict")
            return None, None
        metadata = actual["metadata"]
        annotations = metadata.get("annotations", {})
        recorded = next((item for item in reversed(history) if item.phase == "observed"
            and item.intent.action in {"create", "patch"} and item.observed_uid == metadata["uid"]
            and annotations.get("loom.nebius/operation-id") == str(item.operation_id)
            and annotations.get("loom.nebius/effect-key") == item.key), None)
        if recorded is None:
            raise ProviderBlockedError("application_workload_identity_conflict")
        document = await self._request_document(lease, recorded)
        plan = await self.registry.frozen_plan(lease, operation_id=recorded.operation_id)
        expected = self.kubernetes._document(lease, recorded.key, document, operation_id=recorded.operation_id,
            deployment_generation=plan["registration"]["deployment_generation"])
        if not _contains(actual, expected):
            raise ProviderBlockedError("application_workload_identity_conflict")
        if metadata.get("deletionTimestamp"):
            raise ProviderWaitingError("application_workloads_retirement_pending")
        return actual, recorded

    async def ensure_namespace(self, lease: ApplicationLease) -> None:
        """Bootstrap the retained personal namespace, never adopt or recreate it."""
        await self._fence(lease)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        # Namespace identity is needed for every namespaced call, including the
        # fence. A lost bootstrap reply must be observed before using that gate.
        # This never dispatches a predecessor's unsent bootstrap request.
        for effect in await self.registry.effect_history(lease):
            if effect.intent.kind != "Namespace":
                continue
            if effect.phase == "dispatched":
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key,
                    document=await self._request_document(lease, effect))
            elif effect.phase == "prepared" and effect.operation_id == lease.operation_id:
                await self.kubernetes.create(lease, effect.key, await self._request_document(lease, effect))
        actual = await self.kubernetes._read("/api/v1/namespaces/" + namespace)
        history = [item for item in await self.registry.effect_history(lease) if item.intent.kind == "Namespace"]
        if any(item.phase == "observed" for item in history):
            await self.kubernetes._namespace(lease, "Pod", namespace)
            return
        if any(item.phase == "dispatched" or (item.phase == "prepared" and item.operation_id == lease.operation_id)
               for item in history):
            raise ProviderWaitingError("application_namespace_pending")
        if actual is not None:
            raise ProviderBlockedError("application_namespace_identity_conflict")
        document = next(doc for docs in plan["files"].values() for doc in docs if doc["kind"] == "Namespace")
        await self.kubernetes.create(lease, "namespace:create", document)
        await self.kubernetes._namespace(lease, "Pod", namespace)

    async def stop_workloads(self, lease: ApplicationLease) -> ApplicationWorkloadRetirement:
        """Fence admission, retire routes and prove personal processes have exited.

        Shared data, access revocation and reservation accounting are deliberately
        not changed here. Even foreign/terminating Pods retain the retirement gate.
        """
        await self.ensure_namespace(lease)
        await self._resume_retirement(lease)
        await self.close_admission(lease)
        for effect in await self.registry.effect_history(lease):
            if effect.phase == "dispatched":
                document = None if effect.intent.action == "delete" else await self._request_document(lease, effect)
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key, document=document)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        documents = [doc for docs in plan["files"].values() for doc in docs]
        for kind in ("Ingress", "Service", "Deployment"):
            for doc in documents:
                if doc["kind"] != kind:
                    continue
                name = doc["metadata"]["name"]
                actual, recorded = await self._workload(lease, kind, namespace, name)
                if actual is None:
                    continue
                assert recorded is not None
                metadata = actual["metadata"]
                identity = ":".join((kind, name, metadata["uid"], metadata["resourceVersion"]))
                suffix = hashlib.sha256(identity.encode()).hexdigest()
                try:
                    if kind != "Deployment":
                        await self.kubernetes.delete(lease, "retire:route:" + suffix,
                            api_version=doc["apiVersion"], kind=kind, namespace=namespace, name=name,
                            uid=metadata["uid"], resource_version=metadata["resourceVersion"])
                    elif actual["spec"]["replicas"] != 0:
                        match = _SCALE_KEY.fullmatch(recorded.key)
                        source = UUID(hex=match[1]) if match else recorded.operation_id
                        document = await self._request_document(lease, recorded)
                        document["spec"]["replicas"] = 0
                        await self.kubernetes.patch_spec(lease, f"retire:scale:{source.hex}:{suffix}", document,
                            uid=metadata["uid"], resource_version=metadata["resourceVersion"])
                except KubernetesEffectRejectedError:
                    raise ProviderWaitingError("application_workloads_retirement_pending") from None
        # Separate live evidence from historical success. A controller can lag
        # behind its accepted scale-down; terminating Pods still run processes.
        deployments: list[ApplicationDeploymentRetirement] = []
        for doc in documents:
            if doc["kind"] not in _WORKLOAD_RESOURCES:
                continue
            actual, recorded = await self._workload(lease, doc["kind"], namespace, doc["metadata"]["name"])
            if actual is None:
                continue
            generation = actual["metadata"].get("generation")
            observed = actual.get("status", {}).get("observedGeneration")
            if (doc["kind"] != "Deployment" or actual["spec"]["replicas"] != 0
                    or type(generation) is not int or type(observed) is not int or observed < generation):
                raise ProviderWaitingError("application_workloads_retirement_pending")
            assert recorded is not None
            deployments.append(ApplicationDeploymentRetirement(**_observation(actual, recorded).model_dump(),
                generation=generation, observed_generation=observed))
        await self.kubernetes._namespace(lease, "Pod", namespace)
        response = await self.kubernetes._request("GET", f"/api/v1/namespaces/{namespace}/pods")
        try:
            pods = response.json()
            if (response.status_code != 200 or not isinstance(pods, dict) or pods.get("kind") != "PodList"
                    or not isinstance(pods.get("items"), list) or not isinstance(pods.get("metadata"), dict)
                    or not isinstance(pods["metadata"].get("resourceVersion"), str)
                    or re.fullmatch(r"[A-Za-z0-9._:-]{1,256}", pods["metadata"]["resourceVersion"]) is None
                    or pods["metadata"].get("continue")):
                raise ValueError
        except ValueError:
            raise ProviderBlockedError("application_kubernetes_invalid_response") from None
        if pods["items"]:
            raise ProviderWaitingError("application_workloads_retirement_pending")
        actual_namespace = await self.kubernetes._namespace(lease, "Pod", namespace)
        assert actual_namespace is not None
        namespace_source = next(item for item in await self.registry.effect_history(lease)
            if item.intent.kind == "Namespace" and item.intent.action == "create"
            and item.phase == "observed" and item.observed_uid == actual_namespace["metadata"]["uid"])
        fence = await self.close_admission(lease)
        await self.registry.frozen_plan(lease)
        return ApplicationWorkloadRetirement(
            identity=ApplicationRetirementIdentity.for_lease(lease, UUID(plan["registration"]["data_environment_id"])),
            namespace=_observation(actual_namespace, namespace_source), fence=fence,
            pods_resource_version=pods["metadata"]["resourceVersion"], deployments=tuple(deployments))

    async def _identity(self, lease: ApplicationLease, actual: dict[str, Any],
                         history: list[ApplicationEffect]) -> ApplicationEffect:
        metadata = actual["metadata"]
        annotations = metadata.get("annotations", {})
        recorded = next((effect for effect in reversed(history) if effect.phase == "observed"
            and effect.intent.action in {"create", "patch"} and effect.observed_uid == metadata["uid"]
            and annotations.get("loom.nebius/operation-id") == str(effect.operation_id)
            and annotations.get("loom.nebius/effect-key") == effect.key), None)
        if recorded is None:
            if any(effect.phase == "dispatched" and effect.intent.action in {"create", "patch"}
                   and annotations.get("loom.nebius/operation-id") == str(effect.operation_id)
                   and annotations.get("loom.nebius/effect-key") == effect.key for effect in history):
                # A peer has sent this request but not yet recorded readback.
                # Next reconciliation verifies its digest/UID before adoption.
                raise ProviderWaitingError("application_pod_fence_pending")
            raise ProviderBlockedError("application_pod_fence_conflict")
        expected = await self._fence(lease, operation_id=recorded.operation_id)
        spec = actual.get("spec", {})
        if (not _contains(actual, expected) or spec.get("hard") != {"pods": "0"}
                or spec.get("scopes") or spec.get("scopeSelector") is not None
                or metadata.get("deletionTimestamp")):
            raise ProviderBlockedError("application_pod_fence_conflict")
        return recorded

    async def close_admission(self, lease: ApplicationLease) -> ApplicationResourceObservation:
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
        actual = await self._read(lease, namespace)
        # A peer can create/observe the quota while this caller awaits the API.
        # Judge the returned object against a post-read journal snapshot.
        history = await self._history(lease)
        if any(effect.operation_id == lease.operation_id and effect.phase in {"prepared", "dispatched"}
               for effect in history):
            # A peer may prepare/send while we read. Let the next pass resume
            # that exact intent before deriving another request from live RV.
            raise ProviderWaitingError("application_pod_fence_pending")
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
        return _observation(actual, recorded)
