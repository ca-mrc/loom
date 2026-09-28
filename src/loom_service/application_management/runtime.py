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
    application_namespace_binding,
    application_pod_fence,
)
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_credentials import application_credential_names
from loom.nebius_application_network import application_shared_network_policies
from loom_service.application_management.effects import ApplicationEffect
from loom_service.application_management.kubernetes import (
    ApplicationKubernetesProvider,
    KubernetesEffectRejectedError,
)
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.proofs import (
    ApplicationDeploymentReadiness,
    ApplicationDeploymentRetirement,
    ApplicationPreparationReadiness,
    ApplicationResourceObservation,
    ApplicationRetirementIdentity,
    ApplicationSharedPolicyObservation,
    ApplicationWorkloadReadiness,
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

    async def resume_preparation(self, lease: ApplicationLease) -> None:
        """Recover only current static/credential writes before Pod admission.

        Historical requests are never dispatched. Bootstrap and retirement keep
        their specialized recovery paths; no workload or route starts here.
        """
        plan = await self.registry.frozen_plan(lease)
        if plan["registration"]["desired_state"] != "active":
            raise ProviderBlockedError("application_preparation_not_requested")
        if await self.registry.activation_started(lease):
            raise ProviderBlockedError("application_activation_started")
        for effect in await self.registry.effect_history(lease):
            if effect.operation_id != lease.operation_id or effect.phase not in {"prepared", "dispatched"}:
                continue
            intent = effect.intent
            if intent.kind in {"Namespace", "RoleBinding", "ResourceQuota"} or effect.key.startswith("retire:"):
                continue
            if not ((intent.action == "create" and intent.kind in {"ServiceAccount", "NetworkPolicy", "Secret"})
                    or (intent.action == "patch" and intent.kind == "NetworkPolicy")):
                raise ProviderBlockedError("application_preparation_effect_conflict")
            document = await self._request_document(lease, effect)
            if effect.phase == "dispatched":
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key, document=document)
                continue
            try:
                if intent.action == "create":
                    await self.kubernetes.create(lease, effect.key, document)
                else:
                    assert intent.uid is not None and intent.resource_version is not None
                    await self.kubernetes.patch_spec(lease, effect.key, document,
                        uid=intent.uid, resource_version=intent.resource_version)
            except KubernetesEffectRejectedError:
                raise ProviderWaitingError("application_preparation_pending") from None

    async def _static(self, lease: ApplicationLease, document: dict[str, Any]
                      ) -> tuple[dict[str, Any] | None, ApplicationEffect | None]:
        kind, name, namespace = document["kind"], document["metadata"]["name"], document["metadata"]["namespace"]
        resource = {"ServiceAccount": "serviceaccounts", "NetworkPolicy": "networkpolicies", "Secret": "secrets"}[kind]
        prefix = "/apis/networking.k8s.io/v1" if kind == "NetworkPolicy" else "/api/v1"
        path = f"{prefix}/namespaces/{namespace}/{resource}/{name}"
        await self.kubernetes._namespace(lease, kind, namespace)
        actual = await self.kubernetes._read(path)
        history = [item for item in await self.registry.effect_history(lease)
                   if item.intent.kind == kind and item.intent.name == name and item.intent.namespace == namespace]
        if any(item.phase == "dispatched" or (item.phase == "prepared" and item.operation_id == lease.operation_id)
               for item in history):
            raise ProviderWaitingError("application_preparation_pending")
        if actual is None and any(item.phase == "observed" for item in history):
            # A same-lease peer can observe its CREATE between our GET and
            # history read. Re-read once; never recreate an observed identity.
            actual = await self.kubernetes._read(path)
            if actual is None:
                raise ProviderBlockedError("application_static_resource_conflict")
        if actual is None:
            return None, None
        metadata = actual["metadata"]
        annotations = metadata.get("annotations", {})
        recorded = next((item for item in reversed(history) if item.phase == "observed"
            and item.intent.action in {"create", "patch"} and item.observed_uid == metadata["uid"]
            and annotations.get("loom.nebius/operation-id") == str(item.operation_id)
            and annotations.get("loom.nebius/effect-key") == item.key), None)
        if recorded is None or metadata.get("deletionTimestamp"):
            raise ProviderBlockedError("application_static_resource_conflict")
        plan = await self.registry.frozen_plan(lease, operation_id=recorded.operation_id)
        original = await self._request_document(lease, recorded)
        expected = self.kubernetes._document(lease, recorded.key, original, operation_id=recorded.operation_id,
            deployment_generation=plan["registration"]["deployment_generation"])
        if not _contains(actual, expected) or (kind == "Secret" and actual.get("data") != expected["data"]):
            raise ProviderBlockedError("application_static_resource_conflict")
        await self.kubernetes._namespace(lease, kind, namespace)
        return actual, recorded

    async def prepare_static(self, lease: ApplicationLease) -> None:
        """Install frozen account/network resources while Pod admission is closed."""
        await self.resume_preparation(lease)
        await self.ensure_resource_authority(lease)
        await self.close_admission(lease)
        for effect in await self.registry.effect_history(lease):
            if effect.phase == "dispatched" and effect.intent.kind in {"ServiceAccount", "NetworkPolicy"}:
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key,
                    document=await self._request_document(lease, effect))
        plan = await self.registry.frozen_plan(lease)
        for docs in plan["files"].values():
            for document in docs:
                kind, name = document["kind"], document["metadata"]["name"]
                if kind not in {"ServiceAccount", "NetworkPolicy"}:
                    continue
                actual, recorded = await self._static(lease, document)
                try:
                    if actual is None:
                        await self.kubernetes.create(lease, f"prepare:{kind}:{name}", document)
                    elif kind == "NetworkPolicy" and recorded is not None and recorded.operation_id != lease.operation_id:
                        metadata = actual["metadata"]
                        identity = ":".join((kind, name, metadata["uid"], metadata["resourceVersion"]))
                        await self.kubernetes.patch_spec(lease, "prepare:patch:" + hashlib.sha256(identity.encode()).hexdigest(),
                            document, uid=metadata["uid"], resource_version=metadata["resourceVersion"])
                except KubernetesEffectRejectedError:
                    raise ProviderWaitingError("application_preparation_pending") from None
                actual, recorded = await self._static(lease, document)
                if actual is None or recorded is None:
                    raise ProviderBlockedError("application_static_resource_conflict")
                original = await self.registry.frozen_plan(lease, operation_id=recorded.operation_id)
                expected = self.kubernetes._document(lease, recorded.key, document, operation_id=recorded.operation_id,
                    deployment_generation=original["registration"]["deployment_generation"])
                if not _contains(actual, expected):
                    raise ProviderBlockedError("application_static_resource_conflict")
        if await self.registry.activation_started(lease):
            raise ProviderBlockedError("application_activation_started")

    async def read_shared_network(self, lease: ApplicationLease) -> tuple[ApplicationSharedPolicyObservation, ...]:
        """Observe only the protected installation's three shared ingress rules.

        Uses separately installed named GET authority. It grants no shared
        mutation, namespace adoption, Pod admission or application readiness.
        """
        await self._fence(lease)
        plan = await self.registry.frozen_plan(lease)
        if plan["registration"]["desired_state"] != "active":
            raise ProviderBlockedError("application_preparation_not_requested")
        observations = []
        for document in application_shared_network_policies(self.authority):
            metadata = document["metadata"]
            actual = await self.kubernetes._read(
                f"/apis/networking.k8s.io/v1/namespaces/{metadata['namespace']}/networkpolicies/{metadata['name']}")
            await self._fence(lease)
            if actual is None or actual["metadata"].get("deletionTimestamp") or not _contains(actual, document):
                raise ProviderBlockedError("application_shared_network_conflict")
            observations.append(ApplicationSharedPolicyObservation(name=metadata["name"],
                uid=actual["metadata"]["uid"], resource_version=actual["metadata"]["resourceVersion"]))
        return tuple(observations)

    async def read_prepared(self, lease: ApplicationLease) -> ApplicationPreparationReadiness:
        """Read actual journal-owned static resources and immutable material.

        This is usable after unfencing, so it never closes admission, installs
        resources or dispatches unfinished writes. Credentials are not returned.
        """
        await self._fence(lease)
        plan = await self.registry.frozen_plan(lease)
        row = ApplicationRegistrationV1.model_validate(plan["registration"])
        if row.desired_state != "active":
            raise ProviderBlockedError("application_preparation_not_requested")
        namespace = await self.kubernetes._namespace(lease, "Secret", row.application_namespace)
        assert namespace is not None
        namespace_effect = next(item for item in await self.registry.effect_history(lease)
            if item.intent.kind == "Namespace" and item.intent.action == "create"
            and item.phase == "observed" and item.observed_uid == namespace["metadata"]["uid"])
        resources = []
        for docs in plan["files"].values():
            for document in docs:
                if document["kind"] not in {"ServiceAccount", "NetworkPolicy"}:
                    continue
                actual, effect = await self._static(lease, document)
                if (actual is None or effect is None
                        or (document["kind"] == "NetworkPolicy" and effect.operation_id != lease.operation_id)):
                    raise ProviderBlockedError("application_static_resource_conflict")
                resources.append(_observation(actual, effect))
        material = await self.registry.load_material(lease)
        for name in application_credential_names(row).values():
            document = {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": row.application_namespace},
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in material[name].items()}}
            actual, effect = await self._static(lease, document)
            if actual is None or effect is None or effect.operation_id != lease.operation_id or effect.intent.action != "create":
                raise ProviderBlockedError("application_static_resource_conflict")
            resources.append(_observation(actual, effect))
        await self.registry.frozen_plan(lease)
        return ApplicationPreparationReadiness(
            identity=ApplicationRetirementIdentity.for_lease(lease, row.data_environment_id),
            namespace=_observation(namespace, namespace_effect), resources=tuple(resources))

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
        await self.ensure_resource_authority(lease)
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
        return await self.read_retired(lease)

    async def read_retired(self, lease: ApplicationLease) -> ApplicationWorkloadRetirement:
        """Observe closed admission and exited processes without changing either.

        In particular, a prepared activation DELETE must not re-enter quota
        closing or resend retirement writes while prerequisites are refreshed.
        """
        await self._fence(lease)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        documents = [doc for docs in plan["files"].values() for doc in docs]
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
        actual_fence = await self._read(lease, namespace)
        if actual_fence is None:
            raise ProviderBlockedError("application_pod_fence_conflict")
        fence_source = await self._identity(lease, actual_fence, await self._history(lease))
        if (fence_source.operation_id != lease.operation_id
                or actual_fence.get("status", {}).get("hard", {}).get("pods") != "0"):
            raise ProviderWaitingError("application_pod_fence_pending")
        fence = _observation(actual_fence, fence_source)
        await self.registry.frozen_plan(lease)
        return ApplicationWorkloadRetirement(
            identity=ApplicationRetirementIdentity.for_lease(lease, UUID(plan["registration"]["data_environment_id"])),
            namespace=_observation(actual_namespace, namespace_source), fence=fence,
            pods_resource_version=pods["metadata"]["resourceVersion"], deployments=tuple(deployments))

    async def ensure_resource_authority(self, lease: ApplicationLease) -> None:
        """Bootstrap the frozen exact binding, then observe effective permission.

        Namespace creation alone grants no namespaced resource authority. Binding
        writes are journaled once; authorization reviews are read-only and do not
        consume effect slots or turn a lost mutation into retry permission.
        """
        await self.ensure_namespace(lease)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        document = next((doc for docs in plan["files"].values() for doc in docs
            if doc["kind"] == "RoleBinding" and doc["metadata"]["name"] == self.authority.name), None)
        if document is None or not _contains(document, application_namespace_binding(self.authority, namespace)):
            raise ProviderBlockedError("application_resource_authority_conflict")
        for effect in await self.registry.effect_history(lease):
            if effect.intent.kind != "RoleBinding":
                continue
            if effect.intent.action != "create" or effect.intent.name != self.authority.name:
                raise ProviderBlockedError("application_resource_authority_conflict")
            if effect.phase == "dispatched":
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key,
                    document=await self._request_document(lease, effect))
            elif effect.phase == "prepared" and effect.operation_id == lease.operation_id:
                await self.kubernetes.create(lease, effect.key, document)
        path = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{namespace}/rolebindings/{self.authority.name}"
        actual = await self.kubernetes._read(path)
        history = [item for item in await self.registry.effect_history(lease) if item.intent.kind == "RoleBinding"]
        if any(item.phase == "dispatched" or (item.phase == "prepared" and item.operation_id == lease.operation_id)
               for item in history):
            raise ProviderWaitingError("application_resource_authority_pending")
        observed = [item for item in history if item.phase == "observed"]
        if not observed:
            if actual is not None:
                raise ProviderBlockedError("application_resource_authority_conflict")
            recorded = await self.kubernetes.create(lease, "resource-authority:create", document)
            actual = await self.kubernetes._read(path)
        else:
            if len(observed) != 1:
                raise ProviderBlockedError("application_resource_authority_conflict")
            recorded = observed[0]
            # A same-lease peer can create and observe after our absent GET but
            # before this journal snapshot. Re-read; never turn that progress
            # into a blocked conflict or authority for another CREATE.
            if actual is None:
                actual = await self.kubernetes._read(path)
        original = await self.registry.frozen_plan(lease, operation_id=recorded.operation_id)
        expected = self.kubernetes._document(lease, recorded.key, document, operation_id=recorded.operation_id,
            deployment_generation=original["registration"]["deployment_generation"])
        if (actual is None or actual["metadata"]["uid"] != recorded.observed_uid
                or actual["metadata"].get("deletionTimestamp") or not _contains(actual, expected)):
            raise ProviderBlockedError("application_resource_authority_conflict")
        await self.kubernetes._namespace(lease, "RoleBinding", namespace)
        response = await self.kubernetes._request("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", {
            "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
            "spec": {"resourceAttributes": {"namespace": namespace, "group": "", "resource": "resourcequotas",
                                             "verb": "create"}},
        })
        try:
            result = response.json()
            status = result["status"]
            if result.get("kind") != "SelfSubjectAccessReview" or type(status.get("allowed")) is not bool:
                raise ValueError
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ProviderBlockedError("application_kubernetes_invalid_response") from None
        if not status["allowed"] or status.get("denied") or status.get("evaluationError"):
            raise ProviderWaitingError("application_resource_authority_pending")
        await self.registry.frozen_plan(lease)

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

    async def activation_effect(self, lease: ApplicationLease) -> ApplicationEffect | None:
        """Return the current durable opening phase, never infer it from absence."""
        await self._fence(lease)
        plan = await self.registry.frozen_plan(lease)
        if plan["registration"]["desired_state"] != "active":
            raise ProviderBlockedError("application_activation_not_requested")
        return next((item for item in reversed(await self._history(lease))
            if item.operation_id == lease.operation_id and item.key.startswith("activate:unfence:")), None)

    async def open_admission(self, lease: ApplicationLease) -> ApplicationEffect:
        """Open the qualified generation with one exact journaled quota DELETE.

        The concrete coordinator qualifies shared access/static resources before
        calling. A prepared request retains its original UID/RV; uncertainty is
        observation-only. Opening admission is not workload readiness.
        """
        effect = await self.activation_effect(lease)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        if effect is not None and effect.phase == "dispatched":
            effect = await self.kubernetes.reconcile(lease, effect.operation_id, effect.key)
        if effect is None or effect.phase in {"prepared", "rejected"}:
            retired = await self.read_retired(lease)
            if effect is not None and effect.phase == "prepared":
                uid, version, key = effect.intent.uid, effect.intent.resource_version, effect.key
                assert uid is not None and version is not None
            else:
                uid, version = retired.fence.uid, retired.fence.resource_version
                key = "activate:unfence:" + hashlib.sha256(f"{uid}:{version}".encode()).hexdigest()
            try:
                effect = await self.kubernetes.delete(lease, key, api_version="v1", kind="ResourceQuota",
                    namespace=namespace, name=_FENCE, uid=uid, resource_version=version)
            except KubernetesEffectRejectedError:
                raise ProviderWaitingError("application_activation_pending") from None
        assert effect is not None and effect.phase == "observed"
        if await self._read(lease, namespace) is not None:
            # The journal proves the original UID retired, not that a foreign or
            # successor quota with this fixed name may also be deleted.
            raise ProviderBlockedError("application_pod_fence_conflict")
        await self.registry.frozen_plan(lease)
        return effect

    async def _activated(self, lease: ApplicationLease) -> ApplicationEffect:
        effect = await self.activation_effect(lease)
        if effect is None or effect.phase != "observed":
            raise ProviderWaitingError("application_activation_pending")
        plan = await self.registry.frozen_plan(lease)
        if await self._read(lease, plan["registration"]["application_namespace"]) is not None:
            raise ProviderBlockedError("application_pod_fence_conflict")
        return effect

    async def _start_resources(self, lease: ApplicationLease, kinds: set[str]) -> None:
        await self._activated(lease)
        for effect in await self.registry.effect_history(lease):
            if effect.operation_id != lease.operation_id or effect.phase not in {"prepared", "dispatched"}:
                continue
            if not effect.key.startswith("start:") or effect.intent.kind not in kinds:
                continue
            document = await self._request_document(lease, effect)
            if effect.phase == "dispatched":
                await self.kubernetes.reconcile(lease, effect.operation_id, effect.key, document=document)
                continue
            try:
                if effect.intent.action == "create":
                    await self.kubernetes.create(lease, effect.key, document)
                else:
                    assert effect.intent.uid is not None and effect.intent.resource_version is not None
                    await self.kubernetes.patch_spec(lease, effect.key, document,
                        uid=effect.intent.uid, resource_version=effect.intent.resource_version)
            except KubernetesEffectRejectedError:
                raise ProviderWaitingError("application_workloads_start_pending") from None
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        for docs in plan["files"].values():
            for doc in docs:
                kind, name = doc["kind"], doc["metadata"]["name"]
                if kind not in kinds:
                    continue
                actual, recorded = await self._workload(lease, kind, namespace, name)
                try:
                    if actual is None:
                        await self.kubernetes.create(lease, f"start:create:{kind}:{name}", doc)
                    elif recorded is not None and recorded.operation_id == lease.operation_id and recorded.key.startswith("start:"):
                        continue  # _workload checked the current frozen document.
                    elif kind == "Deployment" and actual["spec"]["replicas"] == 0:
                        metadata = actual["metadata"]
                        identity = ":".join((kind, name, metadata["uid"], metadata["resourceVersion"]))
                        await self.kubernetes.patch_spec(lease, "start:patch:" + hashlib.sha256(identity.encode()).hexdigest(),
                            doc, uid=metadata["uid"], resource_version=metadata["resourceVersion"])
                    else:
                        raise ProviderBlockedError("application_workload_identity_conflict")
                except KubernetesEffectRejectedError:
                    raise ProviderWaitingError("application_workloads_start_pending") from None

    async def _ready_deployments(self, lease: ApplicationLease) -> tuple[ApplicationDeploymentReadiness, ...]:
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        results = []
        for docs in plan["files"].values():
            for doc in docs:
                if doc["kind"] != "Deployment":
                    continue
                actual, recorded = await self._workload(lease, "Deployment", namespace, doc["metadata"]["name"])
                if actual is None or recorded is None or recorded.operation_id != lease.operation_id or not recorded.key.startswith("start:"):
                    raise ProviderWaitingError("application_workloads_readiness_pending")
                generation, replicas = actual["metadata"].get("generation"), doc["spec"]["replicas"]
                status = actual.get("status", {})
                observed = status.get("observedGeneration")
                if (type(generation) is not int or generation < 1 or type(observed) is not int or observed < generation
                        or type(replicas) is not int or replicas < 1
                        or any(type(status.get(key)) is not int or status[key] != replicas for key in (
                            "replicas", "updatedReplicas", "readyReplicas", "availableReplicas"))
                        or any(type(status.get(key, 0)) is not int or status.get(key, 0) != 0
                               for key in ("unavailableReplicas", "terminatingReplicas"))):
                    raise ProviderWaitingError("application_workloads_readiness_pending")
                results.append(ApplicationDeploymentReadiness(**_observation(actual, recorded).model_dump(),
                    generation=generation, observed_generation=observed, replicas=replicas))
        return tuple(results)

    async def start_workloads(self, lease: ApplicationLease) -> ApplicationWorkloadReadiness:
        """Install frozen backends, wait for health, then publish their route."""
        await self._start_resources(lease, {"Deployment", "Service"})
        await self._ready_deployments(lease)
        await self._start_resources(lease, {"Ingress"})
        return await self.read_ready(lease)

    async def read_ready(self, lease: ApplicationLease) -> ApplicationWorkloadReadiness:
        """Live desired identity/template/controller proof, never a repair path."""
        activation = await self._activated(lease)
        deployments = await self._ready_deployments(lease)
        plan = await self.registry.frozen_plan(lease)
        namespace = plan["registration"]["application_namespace"]
        services, ingress = [], None
        for docs in plan["files"].values():
            for doc in docs:
                if doc["kind"] not in {"Service", "Ingress"}:
                    continue
                actual, recorded = await self._workload(lease, doc["kind"], namespace, doc["metadata"]["name"])
                if actual is None or recorded is None or recorded.operation_id != lease.operation_id or not recorded.key.startswith("start:"):
                    raise ProviderWaitingError("application_workloads_readiness_pending")
                if doc["kind"] == "Service":
                    services.append(_observation(actual, recorded))
                else:
                    ingress = _observation(actual, recorded)
        actual_namespace = await self.kubernetes._namespace(lease, "Deployment", namespace)
        assert actual_namespace is not None and ingress is not None and activation.observed_uid is not None
        namespace_source = next(item for item in await self.registry.effect_history(lease)
            if item.intent.kind == "Namespace" and item.intent.action == "create"
            and item.phase == "observed" and item.observed_uid == actual_namespace["metadata"]["uid"])
        await self._activated(lease)
        return ApplicationWorkloadReadiness(
            identity=ApplicationRetirementIdentity.for_lease(lease, UUID(plan["registration"]["data_environment_id"])),
            namespace=_observation(actual_namespace, namespace_source), activation_key=activation.key,
            retired_quota_uid=activation.observed_uid, deployments=deployments, services=tuple(services), ingress=ingress)
