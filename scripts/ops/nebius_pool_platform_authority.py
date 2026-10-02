"""Pinned platform trust for cutover, not an exemption based on a binding name.

Provider administration and Kubernetes controllers remain trusted platform
authority. No permission here is granted, removed, or claimed to be fenced.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches

_RBAC = "rbac.authorization.k8s.io"
_READ = {"get", "list", "watch"}
_EVENTS = {(group, "events"): {"create", "patch", "update"} for group in ("", "events.k8s.io")}
_CONTROLLERS = {
    "cronjob-controller": {
        ("batch", "cronjobs"): _READ | {"update"},
        ("batch", "jobs"): _READ | {"create", "delete", "patch", "update"},
        ("batch", "cronjobs/status"): {"update"},
        ("batch", "cronjobs/finalizers"): {"update"},
        ("", "pods"): {"delete", "list", "watch"},
        **_EVENTS,
    },
    "job-controller": {
        ("batch", "jobs"): _READ | {"patch", "update"},
        ("batch", "jobs/status"): {"update"},
        ("batch", "jobs/finalizers"): {"update"},
        ("", "pods"): {"create", "delete", "list", "patch", "watch"},
        **_EVENTS,
    },
    "generic-garbage-collector": {("*", "*"): _READ | {"delete", "patch", "update"}, **_EVENTS},
    "namespace-controller": {
        ("", "namespaces"): _READ | {"delete"},
        ("", "namespaces/finalize"): {"update"},
        ("", "namespaces/status"): {"update"},
        ("*", "*"): _READ | {"delete", "deletecollection"},
    },
    "ttl-after-finished-controller": {("batch", "jobs"): _READ | {"delete"}, **_EVENTS},
}
_ADMINISTRATORS = {
    "cluster-admin": ("system:masters", "cluster-admin"),
    "kubeadm:cluster-admins": ("kubeadm:cluster-admins", "cluster-admin"),
    "nebius:admin": ("nebius:admin", "cluster-admin"),
    "nebius:editor": ("nebius:editor", "edit"),
}


class PoolPlatformAuthority(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.pool-platform-authority.v1"]
    kube_system_uid: UUID
    resources: tuple[dict[str, Any], ...] = Field(min_length=2, max_length=20)

    @model_validator(mode="after")
    def qualified(self) -> PoolPlatformAuthority:
        platform_authority_documents(self)
        return self


def platform_authority_documents(authority: PoolPlatformAuthority) -> dict[str, dict[str, Any]]:
    """Accept exact operator-group bindings and bounded native capabilities.

    The complete retained objects/UIDs are still compared to live state. Bootstrap
    labels alone are not evidence; names cannot approve extra subjects or grants.
    """
    try:
        if not authority.kube_system_uid.int:
            raise ValueError
        documents: dict[str, dict[str, Any]] = {}
        uids: set[str] = set()
        for row in authority.resources:
            uid, key = _uid(row), _key(row)
            metadata = row["metadata"]
            if (
                row.get("apiVersion") != _RBAC + "/v1"
                or row.get("kind") not in {"ClusterRole", "ClusterRoleBinding"}
                or metadata.get("namespace")
                or key in documents
                or uid in uids
                or not isinstance(metadata.get("resourceVersion"), str)
                or not metadata["resourceVersion"]
            ):
                raise ValueError
            _snapshot(row)
            documents[key], uids = row, uids | {uid}
        used: set[str] = set()
        for row in authority.resources:
            if row["kind"] != "ClusterRoleBinding":
                continue
            name = row["metadata"]["name"]
            if name in _ADMINISTRATORS:
                group, role_name = _ADMINISTRATORS[name]
                subjects = [{"apiGroup": _RBAC, "kind": "Group", "name": group}]
            else:
                controller = name.removeprefix("system:controller:")
                if name != "system:controller:" + controller or controller not in _CONTROLLERS:
                    raise ValueError
                role_name = name
                subjects = [
                    {"kind": "ServiceAccount", "name": controller, "namespace": "kube-system"}
                ]
            role_key = "ClusterRole:-:" + role_name
            role = documents[role_key]
            entries = [dict(entry) for entry in row.get("subjects", [])]
            for entry in entries:
                if entry.get("kind") == "ServiceAccount" and entry.get("apiGroup") == "":
                    entry.pop("apiGroup")
            if entries != subjects or row["roleRef"] != {
                "apiGroup": _RBAC,
                "kind": "ClusterRole",
                "name": role_name,
            }:
                raise ValueError
            if name not in _ADMINISTRATORS:
                if "aggregationRule" in role or any(
                    document["metadata"].get("labels", {}).get("kubernetes.io/bootstrapping")
                    != "rbac-defaults"
                    for document in (row, role)
                ):
                    raise ValueError
                rules = role.get("rules")
                if not isinstance(rules, list) or not 0 < len(rules) <= 32:
                    raise ValueError
                for rule in rules:
                    if set(rule) != {"apiGroups", "resources", "verbs"} or any(
                        not isinstance(rule[field], list)
                        or not rule[field]
                        or any(not isinstance(value, str) for value in rule[field])
                        for field in rule
                    ):
                        raise ValueError
                    if any(
                        not set(rule["verbs"])
                        <= _CONTROLLERS[controller].get((group, resource), set())
                        for group in rule["apiGroups"]
                        for resource in rule["resources"]
                    ):
                        raise ValueError
            used.add(role_key)
        if used != {key for key, row in documents.items() if row["kind"] == "ClusterRole"}:
            raise ValueError
        return documents
    except Exception:
        raise ValueError("pool_platform_authority_unqualified") from None


def qualify_platform_authority(
    authority: PoolPlatformAuthority | None,
    *,
    kube_system_uid: str,
    documents: dict[str, dict[str, Any]],
) -> set[str]:
    """No absent, replaced or broadened snapshot is implicit platform trust."""
    if authority is None or str(authority.kube_system_uid) != kube_system_uid:
        raise ValueError("pool_platform_authority_unqualified")
    retained = platform_authority_documents(authority)
    for key, original in retained.items():
        if key not in documents or not _matches(documents[key], original, _uid(original)):
            raise ValueError("pool_platform_authority_unqualified")
    return {key for key, row in retained.items() if row["kind"] == "ClusterRoleBinding"}


def platform_controller_subjects(
    authority: PoolPlatformAuthority | None,
) -> frozenset[tuple[str, str]]:
    if authority is None:
        raise ValueError("pool_platform_authority_unqualified")
    documents = platform_authority_documents(authority)
    return frozenset(
        (entry["namespace"], entry["name"])
        for row in documents.values()
        if row["kind"] == "ClusterRoleBinding"
        for entry in row["subjects"]
        if entry["kind"] == "ServiceAccount"
    )
