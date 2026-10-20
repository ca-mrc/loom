"""Create-only namespace-local Kubernetes intents and UID-bound readiness.

The installation owns the authenticated HTTPS client. This module does not read
ambient kubeconfig, follow redirects, apply/patch foreign state, or delete data.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any

import httpx

from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderRetryError,
    ProviderWaitingError,
    ProvisioningContext,
)
from loom_service.environment_management.steps import ProvisioningStep

_RESOURCES = {
    "Namespace": ("v1", "namespaces"), "Secret": ("v1", "secrets"),
    "Service": ("v1", "services"), "ServiceAccount": ("v1", "serviceaccounts"),
    "ConfigMap": ("v1", "configmaps"), "ResourceQuota": ("v1", "resourcequotas"),
    "PersistentVolumeClaim": ("v1", "persistentvolumeclaims"),
    "Deployment": ("apps/v1", "deployments"), "StatefulSet": ("apps/v1", "statefulsets"),
    "Job": ("batch/v1", "jobs"), "CronJob": ("batch/v1", "cronjobs"),
    "Role": ("rbac.authorization.k8s.io/v1", "roles"),
    "RoleBinding": ("rbac.authorization.k8s.io/v1", "rolebindings"),
    "Ingress": ("networking.k8s.io/v1", "ingresses"),
    "NetworkPolicy": ("networking.k8s.io/v1", "networkpolicies"),
}
_NAME = re.compile(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?")


def _contains(
    actual: Any, expected: Any, path: tuple[str, ...] = (), *,
    kind: str | None = None, exact: bool = False,
) -> bool:
    """Allow API decoration, not additions inside frozen policy/selector maps.

    Empty selectors mean *all*, so an extra key is a changed security boundary,
    not a default. Quota scopes likewise narrow which Pods consume the quota.
    Keep those complete subtrees closed while tolerating metadata/status and
    normal controller defaulting outside them. Cloud callers have no K8s kind.
    """
    if isinstance(expected, dict):
        if not path and isinstance(expected.get("kind"), str):
            kind = expected["kind"]
        exact = exact or (kind in _RESOURCES and (
            (path == ("spec",) and kind in {"NetworkPolicy", "ResourceQuota"})
            or (bool(path) and path[-1] in {"selector", "podSelector", "namespaceSelector", "securityContext"})
        ))
        if not isinstance(actual, dict):
            return False
        if exact and any(
            # NetworkPolicyPort defaults to TCP; no other rule addition is an
            # API default. In particular, from/to peers and empty selectors
            # cannot silently acquire another selector or expression.
            not (kind == "NetworkPolicy" and path[-2:] == ("ports", "*")
                 and key == "protocol" and actual[key] == "TCP")
            for key in actual.keys() - expected.keys()
        ):
            return False
        if (path[-2:] == ("env", "*") and "value" in expected and "valueFrom" not in expected
                and "valueFrom" in actual):
            return False
        # Kubernetes omits zero-length optional lists (e.g. default-deny
        # ingress/egress rules). Scalar false/zero MUST NOT be treated as absent:
        # their defaults can grant authority or start replicas.
        return all(
            (_contains(actual[key], value, (*path, key), kind=kind, exact=exact) if key in actual else (
                value == [] or (path[-2:] == ("env", "*") and key == "value"
                                and value == "" and "valueFrom" not in actual)
            ))
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (isinstance(actual, list) and len(actual) == len(expected)
                and all(_contains(a, b, (*path, "*"), kind=kind, exact=exact)
                        for a, b in zip(actual, expected, strict=True)))
    return type(actual) is type(expected) and actual == expected


class KubernetesEnvironmentProvider:
    def __init__(self, http: httpx.AsyncClient):
        if http.base_url.scheme != "https":
            raise ValueError("Kubernetes provider requires HTTPS")
        self.http = http

    def _path(self, context: ProvisioningContext, doc: dict[str, Any]) -> tuple[str, str]:
        kind, metadata = doc.get("kind"), doc.get("metadata", {})
        binding = _RESOURCES.get(kind) if isinstance(kind, str) else None
        name, namespace = metadata.get("name"), metadata.get("namespace")
        if (binding is None or doc.get("apiVersion") != binding[0] or not isinstance(name, str)
                or _NAME.fullmatch(name) is None):
            raise ProviderBlockedError("kubernetes_intent_not_allowed")
        api, resource = binding
        prefix = "/api/v1" if api == "v1" else "/apis/" + api
        if kind == "Namespace":
            if name not in context.namespaces or namespace is not None:
                raise ProviderBlockedError("kubernetes_namespace_not_owned")
        else:
            if namespace not in context.namespaces:
                raise ProviderBlockedError("kubernetes_namespace_not_owned")
            prefix += "/namespaces/" + namespace
        collection = prefix + "/" + resource
        return collection, collection + "/" + name

    async def _request(
        self, method: str, path: str, *, body: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        try:
            headers = {"Content-Type": "application/json-patch+json"} if isinstance(body, list) else {}
            response = await self.http.request(method, path, json=body, headers=headers, follow_redirects=False, timeout=30)
        except httpx.TransportError:
            raise ProviderRetryError("kubernetes_unavailable") from None
        if response.status_code == 404 and method in {"GET", "DELETE"}:
            return None
        if (response.status_code in (409, 429) or response.status_code >= 500
                or (method == "PATCH" and response.status_code == 422)):
            raise ProviderRetryError("kubernetes_retry_required")
        if response.status_code not in ((200, 202) if method == "DELETE" else (200, 201)):
            raise ProviderBlockedError("kubernetes_request_rejected")
        try:
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError
            return value
        except ValueError:
            raise ProviderBlockedError("kubernetes_invalid_response") from None

    @staticmethod
    def _expected(context: ProvisioningContext, step: ProvisioningStep) -> dict[str, Any]:
        doc = copy.deepcopy(step.payload)
        metadata = doc.setdefault("metadata", {})
        metadata.setdefault("labels", {}).update({
            "loom.nebius/environment-id": str(context.lease.environment_id),
            "loom.nebius/incarnation": context.registration["incarnation"],
        })
        metadata.setdefault("annotations", {}).update({
            "loom.nebius/operation-id": str(context.lease.operation_id),
            "loom.nebius/deployment-generation": str(context.lease.deployment_generation),
        })
        digest = hashlib.sha256(json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        metadata["annotations"]["loom.nebius/intent-sha256"] = digest
        return doc

    @staticmethod
    def _identity(actual: dict[str, Any], expected: dict[str, Any], recorded: str | None) -> str:
        metadata = actual.get("metadata", {})
        uid = metadata.get("uid")
        if (not isinstance(uid, str) or not uid or metadata.get("deletionTimestamp")
                or metadata.get('ownerReferences', []) != expected.get('metadata', {}).get('ownerReferences', [])
                or (recorded is not None and recorded != uid) or not _contains(actual, expected)):
            raise ProviderBlockedError("kubernetes_resource_identity_conflict")
        if expected.get('kind') in {'Deployment', 'StatefulSet', 'Job', 'CronJob'}:
            observed, wanted = actual['spec'], expected['spec']
            if expected['kind'] == 'CronJob':
                observed, wanted = observed['jobTemplate']['spec'], wanted['jobTemplate']['spec']
            observed, wanted = observed['template']['spec'], wanted['template']['spec']
            defaults: dict[str, Any] = {'initContainers': [], 'ephemeralContainers': [],
                'hostNetwork': False, 'hostPID': False, 'hostIPC': False, 'shareProcessNamespace': False,
                'serviceAccountName': 'default', 'securityContext': {}, 'volumes': [], 'imagePullSecrets': [],
                'hostAliases': [], 'runtimeClassName': None, 'nodeName': '', 'priorityClassName': '',
                'schedulerName': 'default-scheduler', 'dnsConfig': {}, 'affinity': {}}
            if any(not _contains(observed.get(key, default), wanted.get(key, default), exact=key == 'securityContext')
                    for key, default in defaults.items()):
                raise ProviderBlockedError('kubernetes_resource_identity_conflict')
            # An omitted command/env source/mount is part of the frozen image
            # contract. A mutating admission webhook must not inject a command,
            # credential source or privileged context under the same UID and
            # otherwise matching requested fields. API decoration remains valid.
            container_defaults: dict[str, Any] = {'command': [], 'args': [], 'env': [], 'envFrom': [],
                'volumeMounts': [], 'volumeDevices': [], 'securityContext': {}, 'lifecycle': {},
                'stdin': False, 'stdinOnce': False, 'tty': False}
            for field in ('containers', 'initContainers'):
                for actual_container, expected_container in zip(observed.get(field, []), wanted.get(field, []), strict=True):
                    # Preserve the env path: the API omits empty literal values,
                    # but an injected valueFrom must still fail qualification.
                    if any(not _contains(actual_container.get(key, default), expected_container.get(key, default),
                            (field, '*', key), exact=key == 'securityContext') for key, default in container_defaults.items()):
                        raise ProviderBlockedError('kubernetes_resource_identity_conflict')
        return uid

    async def apply(self, context: ProvisioningContext, step: ProvisioningStep, *, create: bool = True) -> str:
        if step.kind == "kubernetes":
            if not create and not context.identities.get(step.key):
                raise ProviderBlockedError("kubernetes_import_identity_missing")
            expected = self._expected(context, step)
            collection, path = self._path(context, expected)
            namespace = expected["metadata"].get("namespace")
            if namespace is not None:
                recorded_namespace = context.identities.get("k8s:Namespace:-:" + namespace)
                if recorded_namespace is None:
                    raise ProviderBlockedError("kubernetes_namespace_identity_missing")
                observed_namespace = await self._request("GET", "/api/v1/namespaces/" + namespace)
                if observed_namespace is None:
                    raise ProviderBlockedError("kubernetes_recorded_namespace_missing")
                self._identity(observed_namespace, {"metadata": {"labels": {
                    "loom.nebius/environment-id": str(context.lease.environment_id),
                    "loom.nebius/incarnation": context.registration["incarnation"],
                }}}, recorded_namespace)
            actual = await self._request("GET", path)
            if actual is None:
                if not create or step.key in context.identities:
                    raise ProviderBlockedError("kubernetes_recorded_resource_missing")
                reply = await self._request("POST", collection, body=expected)
                if reply is None:
                    raise ProviderRetryError("kubernetes_create_unconfirmed")
                created_uid = self._identity(reply, expected, None)
                actual = await self._request("GET", path)
                if actual is None:
                    raise ProviderRetryError("kubernetes_create_unconfirmed")
                self._identity(actual, expected, created_uid)
            assert actual is not None
            if actual.get("metadata", {}).get("annotations", {}).get("loom.nebius/retained-by") is not None:
                raise ProviderBlockedError("kubernetes_resource_retained")
            return self._identity(actual, expected, context.identities.get(step.key))
        if step.kind in {"job_ready", "database_ready"}:
            kind = "Job" if step.kind == "job_ready" else "StatefulSet"
            resource_key = step.payload.get("resource_key", (
                f"k8s:{kind}:{step.payload['namespace']}:{step.payload['name']}"
            ))
            recorded = context.identities.get(resource_key)
            if recorded is None:
                raise ProviderBlockedError("kubernetes_readiness_identity_missing")
            _, path = self._path(context, {
                "apiVersion": _RESOURCES[kind][0], "kind": kind,
                "metadata": {"namespace": step.payload["namespace"], "name": step.payload["name"]},
            })
            actual = await self._request("GET", path)
            if actual is None:
                raise ProviderBlockedError("kubernetes_recorded_resource_missing")
            uid = self._identity(actual, {}, recorded)
            status = actual.get("status", {})
            if kind == "Job":
                conditions = {row.get("type"): row.get("status") for row in status.get("conditions", [])}
                if conditions.get("Failed") == "True":
                    raise ProviderBlockedError("kubernetes_bootstrap_job_failed")
                ready = conditions.get("Complete") == "True"
            else:
                ready = (status.get("readyReplicas", 0) == actual.get("spec", {}).get("replicas", 1)
                         and status.get("observedGeneration", 0) >= actual["metadata"].get("generation", 1))
            if not ready:
                raise ProviderWaitingError("kubernetes_not_ready")
            return uid
        if step.kind == "application_ready":
            namespace = step.payload["namespace"]
            identities = []
            for name in ("loom-service", "loom-control-plane", "loom-llm-gateway", "loom-web"):
                key = f"k8s:Deployment:{namespace}:{name}"
                doc, recorded = context.documents.get(key), context.identities.get(key)
                if doc is None or recorded is None:
                    raise ProviderBlockedError("kubernetes_readiness_identity_missing")
                expected = self._expected(context, ProvisioningStep(key, "kubernetes", doc))
                _, path = self._path(context, expected)
                actual = await self._request("GET", path)
                if actual is None:
                    raise ProviderBlockedError("kubernetes_recorded_resource_missing")
                identities.append(self._identity(actual, expected, recorded))
                status = actual.get("status", {})
                replicas = expected["spec"]["replicas"]
                if (replicas < 1 or status.get("observedGeneration", 0) < actual["metadata"].get("generation", 1)
                        or any(status.get(field, 0) != replicas for field in (
                            "replicas", "readyReplicas", "updatedReplicas", "availableReplicas",
                        ))):
                    raise ProviderWaitingError("kubernetes_not_ready")
            return "deployments:" + hashlib.sha256(json.dumps(identities).encode()).hexdigest()
        raise ProviderBlockedError("kubernetes_step_not_supported")
