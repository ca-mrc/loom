"""Pre-execution retained destroy; no namespace, PVC, bucket or result deletion."""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import replace
from typing import Any
from uuid import uuid5

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
from loom_service.environment_management.kubernetes_provider import (
    KubernetesEnvironmentProvider,
    _contains,
)
from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderWaitingError,
    ProvisioningContext,
)
from loom_service.environment_management.registry import EnvironmentRegistry
from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup
from loom_service.environment_management.steps import ProvisioningStep


class EnvironmentRetainedDestroy:
    def __init__(
        self, registry: EnvironmentRegistry, kubernetes: KubernetesEnvironmentProvider,
        cloud: NebiusSdkEnvironmentApi | None, child: ChildEnvironmentClient | None,
    ):
        self.registry, self.kubernetes, self.cloud, self.child = registry, kubernetes, cloud, child
        self.cleanup = RetainedKubernetesCleanup(kubernetes)

    async def _close_pod_admission(
        self, context: ProvisioningContext, source: ProvisioningContext, namespace: str, *, require_idle: bool = False,
    ) -> str:
        # Retained controllers prevent stale create from restoring desired
        # replicas; this quota also closes delayed controller-created children.
        authority = replace(context, identities={**source.identities, **context.identities})
        step = ProvisioningStep("retain:quota:" + namespace, "kubernetes", {
            "apiVersion": "v1", "kind": "ResourceQuota", "metadata": {
                "name": "loom-environment-retained", "namespace": namespace,
            }, "spec": {"hard": {"pods": "0"}},
        })
        identity = await self.kubernetes.apply(authority, step)
        _, path = self.kubernetes._path(authority, step.payload)
        observed = await self.kubernetes._request("GET", path)
        if observed is None:
            raise ProviderBlockedError("kubernetes_recorded_resource_missing")
        self.kubernetes._identity(observed, self.kubernetes._expected(authority, step), identity)
        status = observed.get("status", {})
        if (status.get("hard", {}).get("pods") != "0"
                or (require_idle and status.get("used", {}).get("pods") != "0")):
            raise ProviderWaitingError("retained_quota_not_quiescent")
        return identity

    @staticmethod
    def _controller(doc: dict[str, Any], *, kind: str) -> str | None:
        owners = doc.get("metadata", {}).get("ownerReferences", [])
        matches = [owner for owner in owners if owner.get("controller") is True]
        if len(matches) != 1 or matches[0].get("kind") != kind or not isinstance(matches[0].get("uid"), str):
            return None
        return str(matches[0]["uid"])

    async def _descendants(self, context: ProvisioningContext, source: ProvisioningContext, namespace: str) -> None:
        """Discovery is read-only; each discovered write becomes a durable intent."""
        known = set(source.identities.values()) | set(context.identities.values())
        jobs = await self.cleanup.inventory(source, namespace, "jobs")
        crons = {source.identities[key]: doc for key, doc in source.documents.items()
                 if doc["kind"] == "CronJob" and key in source.identities and doc["metadata"]["namespace"] == namespace}
        discovered = []
        for job in jobs:
            metadata = job.get("metadata", {})
            uid = metadata.get("uid")
            if uid in known:
                continue
            parent = crons.get(self._controller(job, kind="CronJob") or "")
            if (parent is None or not isinstance(uid, str) or metadata.get("namespace") != namespace
                    or not _contains(job.get("spec"), parent["spec"]["jobTemplate"]["spec"])):
                raise ProviderBlockedError("retained_unowned_job")
            snapshot = {"apiVersion": "batch/v1", "kind": "Job", "spec": job["spec"], "metadata": {
                key: metadata[key] for key in ("name", "namespace", "uid", "ownerReferences")
            }}
            discovered.append(ProvisioningStep("retain:dependent:Job:" + uid, "credentials", {
                "action": "retained_dependent_job", "resource": snapshot,
            }))
        if discovered:
            await self.registry.journal_retained_resources(context.lease, discovered)
            raise ProviderWaitingError("retained_children_discovered")
        # An already-journaled dependent may have acquired a controller status
        # change, but it must still be suspended with the SAME UID and body.
        for payload in context.documents.values():
            if payload.get("action") == "retained_dependent_job":
                await self.cleanup.dependent_job(source, payload["resource"], cleanup_id=context.lease.operation_id)
        pods = await self.cleanup.inventory(source, namespace, "pods")
        terminal = [pod for pod in pods if pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}]
        if terminal:
            # Deployment Pods belong to a ReplicaSet, so prove that intermediate
            # owner rather than trusting an arbitrary Pod label.
            deployments = {source.identities[key] for key, doc in source.documents.items()
                           if doc["kind"] == "Deployment" and key in source.identities
                           and doc["metadata"]["namespace"] == namespace}
            replicas = await self.cleanup.inventory(source, namespace, "replicasets")
            replicasets = {item["metadata"]["uid"] for item in replicas
                           if self._controller(item, kind="Deployment") in deployments}
            job_uids = {job["metadata"]["uid"] for job in jobs} | {
                payload["resource"]["metadata"]["uid"] for payload in context.documents.values()
                if payload.get("action") == "retained_dependent_job"
            } | {source.identities[key] for key, doc in source.documents.items()
                 if doc["kind"] == "Job" and key in source.identities}
            databases = {source.identities[key] for key, doc in source.documents.items()
                         if doc["kind"] == "StatefulSet" and key in source.identities}
            for pod in terminal:
                metadata = pod.get("metadata", {})
                if (metadata.get("namespace") != namespace or not isinstance(metadata.get("uid"), str)
                        or not (self._controller(pod, kind="Job") in job_uids
                                or self._controller(pod, kind="ReplicaSet") in replicasets
                                or self._controller(pod, kind="StatefulSet") in databases)):
                    raise ProviderBlockedError("retained_unowned_pod")
                snapshot = {"apiVersion": "v1", "kind": "Pod", "metadata": {
                    key: metadata[key] for key in ("name", "namespace", "uid", "ownerReferences")
                }}
                key = "retain:dependent:Pod:" + metadata["uid"]
                if key not in context.identities:
                    discovered.append(ProvisioningStep(key, "credentials", {"action": "retained_terminal_pod", "resource": snapshot}))
            if discovered:
                await self.registry.journal_retained_resources(context.lease, discovered)
                raise ProviderWaitingError("retained_children_discovered")
        if pods:
            raise ProviderWaitingError("retained_pods_remaining")

    async def apply(self, context: ProvisioningContext, step: ProvisioningStep) -> str:
        source = context.source
        if source is None or context.action != "destroy_retained":
            raise ProviderBlockedError("retained_source_operation_invalid")
        action = step.payload.get("action")
        if action == "retained_quota":
            return await self._close_pod_admission(context, source, step.payload["namespace"])
        if action == "retained_dependent_job":
            return await self.cleanup.dependent_job(source, step.payload["resource"], cleanup_id=context.lease.operation_id)
        if action == "retained_terminal_pod":
            return await self.cleanup.terminal_pod(source, step.payload["resource"])
        if action in {"retained_namespace", "retained_stop"}:
            key = step.payload["source_key"]
            doc = source.documents.get(key)
            if doc is None:
                raise ProviderBlockedError("retained_source_resource_invalid")
            original = ProvisioningStep(key, "kubernetes", doc)
            if action == "retained_namespace":
                if doc.get("kind") != "Namespace":
                    raise ProviderBlockedError("retained_source_resource_invalid")
                return await self.kubernetes.apply(source, original)
            return await self.cleanup.stop(source, original, cleanup_id=context.lease.operation_id)
        if action == "retained_owner_revoke":
            if self.child is None:
                raise ProviderBlockedError("retained_child_client_required")
            material = await self.registry.load_material(context.lease, "credentials:material")
            try:
                token = tomllib.loads(material["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
                if not isinstance(token, str) or not token:
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                raise ProviderBlockedError("credential_admin_invalid") from None
            row = EnvironmentRegistrationV1.model_validate(source.registration)
            await self.child.revoke(row, admin_token=token)
            return "revoked:" + str(row.owner_user_id)
        if action == "retained_key_revoke":
            # No material commit means no access key was delivered to a child.
            # Unknown earlier cloud creates remain retained, not guessed/deleted.
            if "credentials:material" not in source.identities:
                return "not-delivered"
            if self.cloud is None:
                raise ProviderBlockedError("retained_cloud_client_required")
            purpose = step.payload["purpose"]
            key = "iam:" + purpose + ":access_key"
            identity = source.identities.get(key)
            if identity is None:
                raise ProviderBlockedError("retained_access_key_identity_missing")
            original = ProvisioningStep(key, "credentials", {"action": "access_key", "purpose": purpose})
            _, expected = NebiusEnvironmentCloudProvider(self.cloud).intent(source, original)
            await self.cloud.revoke_access_key(identity, expected, idempotency_key=str(uuid5(context.lease.operation_id, step.key)))
            return "revoked:" + identity
        if step.kind == "application_ready" and step.payload.get("phase") == "retained":
            for key, doc in source.documents.items():
                if doc["kind"] not in {"Deployment", "StatefulSet", "Job", "CronJob", "Ingress"}:
                    continue
                original = ProvisioningStep(key, "kubernetes", doc)
                await self.cleanup.stop(source, original, cleanup_id=context.lease.operation_id)
                if doc["kind"] in {"Deployment", "StatefulSet"}:
                    _, path = self.kubernetes._path(source, doc)
                    current = await self.kubernetes._request("GET", path)
                    if current is None:
                        raise ProviderBlockedError("kubernetes_recorded_resource_missing")
                    status = current.get("status", {})
                    if (status.get("observedGeneration", 0) < current["metadata"].get("generation", 1)
                            or status.get("replicas", 0) != 0):
                        raise ProviderWaitingError("retained_controllers_stopping")
            for namespace in source.namespaces:
                await self._close_pod_admission(context, source, namespace)
                await self._descendants(context, source, namespace)
                await self._close_pod_admission(context, source, namespace, require_idle=True)
            return "stopped:" + hashlib.sha256(json.dumps(source.identities, sort_keys=True).encode()).hexdigest()
        raise ProviderBlockedError("retained_step_not_supported")
