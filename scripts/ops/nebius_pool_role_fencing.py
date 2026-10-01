"""Replace retained participant writer roles with readers after process retirement.

No new bindings, gateway authority or activation. Effective permission reviews
are repeated on recovery; the parent still owns the complete writer inventory
and activation-time requalification.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_retirement import (
    PoolRetirementAPI,
    PoolRetirementRequest,
    retire_pool_workloads,
    retirement_documents,
)
from scripts.ops.nebius_pool_runtime import participant_readonly_roles

from loom.nebius_platform_render import digest

MARKER = "loom.nebius/pool-role-fencing-operation"


@dataclass(frozen=True, repr=False)
class PoolRoleFenceRequest:
    retirement: PoolRetirementRequest
    originals: tuple[dict[str, Any], ...]


class PoolRoleFenceAPI(Protocol):
    retirement: PoolRetirementAPI

    def verify_readonly(self) -> None: ...
    def read_role(self, key: str) -> dict[str, Any]: ...
    def restrict_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        """False only on a complete definite rejection; exceptions are unknown."""
        ...


def role_fence_review_scope(request: PoolRoleFenceRequest) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    """Fixed retained subjects crossed with every qualified operation namespace."""
    subjects = set()
    for document in retirement_documents(request.retirement).values():
        pod = (document["spec"]["jobTemplate"]["spec"]["template"]["spec"] if document["kind"] == "CronJob"
            else document["spec"]["template"]["spec"])
        account = pod.get("serviceAccountName", "default")
        if (not isinstance(account, str) or len(account) > 253
                or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*", account) is None):
            raise ValueError("pool_role_fence_subject_unqualified")
        subjects.add((str(document["metadata"]["namespace"]), account))
    migration = request.retirement.migration
    namespaces = {migration.registration.binding.namespace, *(row.namespace for row in migration.guards),
        *(ns.name for row in migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}
    return tuple(sorted(subjects)), tuple(sorted(namespaces))


def qualify_pool_reader_rules(review: dict[str, Any], *, namespace: str) -> None:
    """Reject incomplete resolution and all authority outside fixed readers.

Named grants are checked too. Default Kubernetes self-inspection and discovery
are harmless exceptions to reads; credential access and arbitrary subresources
are not. A resolver that cannot enumerate effective rules cannot qualify cutover.
"""
    try:
        status = review["status"]
        if (review.get("apiVersion") != "authorization.k8s.io/v1" or review.get("kind") != "SelfSubjectRulesReview"
                # Kubernetes returns an empty spec, not an echo. The fixed TLS
                # request binds scope; reject a contradictory echo if supplied.
                or review.get("spec") not in ({}, {"namespace": namespace}) or status.get("incomplete") is not False
                or status.get("evaluationError", "") != ""):
            raise ValueError

        def strings(value: Any, *, empty: bool = False) -> set[str]:
            if (not isinstance(value, list) or not 0 < len(value) <= 1000
                    or any(not isinstance(item, str) or len(item) > 1024 or (not item and not empty) for item in value)):
                raise ValueError
            return set(value)

        allowed = {
            ("", "nodes"): {"get", "list", "watch"}, ("", "pods"): {"get", "list", "watch"},
            ("", "pods/log"): {"get"}, ("", "namespaces"): {"get"}, ("batch", "jobs"): {"get", "list", "watch"},
            ("authorization.k8s.io", "selfsubjectaccessreviews"): {"create"},
            ("authorization.k8s.io", "selfsubjectrulesreviews"): {"create"},
            ("authentication.k8s.io", "selfsubjectreviews"): {"create"},
        }
        discovery = {"/api", "/api/*", "/apis", "/apis/*", "/openapi", "/openapi/*", "/healthz", "/livez", "/readyz",
            "/version", "/version/", "/.well-known/openid-configuration", "/.well-known/openid-configuration/",
            "/openid/v1/jwks", "/openid/v1/jwks/"}
        for field in ("resourceRules", "nonResourceRules"):
            rules = status[field]
            if not isinstance(rules, list) or len(rules) > 1000:
                raise ValueError
            for rule in rules:
                verbs = strings(rule["verbs"])
                if field == "nonResourceRules":
                    if set(rule) != {"verbs", "nonResourceURLs"} or verbs != {"get"}:
                        raise ValueError
                    if not strings(rule["nonResourceURLs"]) <= discovery:
                        raise ValueError
                    continue
                if set(rule) - {"verbs", "apiGroups", "resources", "resourceNames"}:
                    raise ValueError
                groups, resources = strings(rule["apiGroups"], empty=True), strings(rule["resources"])
                if "resourceNames" in rule and rule["resourceNames"] != []:
                    strings(rule["resourceNames"])
                if any(not verbs <= allowed.get((group, resource), set()) for group in groups for resource in resources):
                    raise ValueError
    except Exception:
        raise ValueError("pool_role_fence_effective_authority_unqualified") from None


def role_fence_documents(request: PoolRoleFenceRequest) -> dict[str, dict[str, Any]]:
    try:
        role_fence_review_scope(request)  # Qualify subjects before any downtime.
        targets = {_key(row): row for row in participant_readonly_roles(request=request.retirement.migration) if row["kind"] == "Role"}
        originals = {_key(row): row for row in request.originals}
        if (set(originals) != set(targets) or len(request.originals) != len(targets)
                or len({_uid(row) for row in request.originals}) != len(targets)):
            raise ValueError
        result = {}
        for key, row in originals.items():
            if (row.get("apiVersion") != "rbac.authorization.k8s.io/v1" or not isinstance(row.get("rules"), list)
                    or MARKER in row["metadata"].get("annotations", {})):
                raise ValueError
            desired = _snapshot(row)
            desired["rules"] = copy.deepcopy(targets[key]["rules"])
            desired["metadata"].setdefault("annotations", {})[MARKER] = str(request.retirement.migration.registration.spec.operation_id)
            result[key] = desired
        return result
    except Exception:
        raise ValueError("pool_role_fence_inputs_unqualified") from None


def fence_pool_roles(*, request: PoolRoleFenceRequest, api: PoolRoleFenceAPI,
                     state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        targets = role_fence_documents(request)
        originals = {_key(row): row for row in request.originals}
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        operation = str(request.retirement.migration.registration.spec.operation_id)
        marker, path = anchor / (operation + "-role-fencing.json"), state / "role-fencing.json"
        if not any(item.exists() or item.is_symlink() for item in (marker, path)):
            # Qualify every original Role before initiating downtime. A resumed
            # fenced phase instead validates retained targets below.
            if any(not _matches(api.read_role(key), row, _uid(row)) for key, row in originals.items()):
                raise ValueError
        retirement = retire_pool_workloads(request=request.retirement, api=api.retirement, state_dir=state, anchor_dir=anchor)
        if retirement["status"] != "old_pool_workloads_retired":
            return retirement

        def result(status: str) -> dict[str, Any]:
            return {"status": status, "operation_id": operation, "writer_migration_complete": False}

        with private_state._locked_state(anchor):
            identity = {"schema": "loom.nebius-pool-role-fencing.v1", "operation_id": operation,
                "state_dir": str(state), "retirement_sha256": _hash(state / "retirement.json"),
                "originals_sha256": digest({key: {"uid": _uid(row), "document": _stable(row)} for key, row in originals.items()}),
                "targets_sha256": digest(targets)}
            if marker.exists() or marker.is_symlink():
                if json.loads(private_state._private_read(marker)) != identity:
                    raise ValueError
                record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
                if (set(record) != {*identity, "roles"} or any(record[key] != value for key, value in identity.items())
                        or set(record["roles"]) != set(originals)
                        or any(value not in {"prepared", "intent", "restricted"} for value in record["roles"].values())):
                    raise ValueError
            else:
                if path.exists() or path.is_symlink() or any(not _matches(api.read_role(key), row, _uid(row)) for key, row in originals.items()):
                    raise ValueError
                private_state._atomic_json(marker, identity)
                record = {**identity, "roles": dict.fromkeys(originals, "prepared")}
                private_state._atomic_json(path, record)

            def save(key: str, phase: str) -> None:
                record["roles"][key] = phase
                private_state._atomic_json(path, record)

            for key, original in originals.items():
                actual = api.read_role(key)
                if record["roles"][key] == "prepared":
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                    save(key, "intent")
                    try:
                        accepted = api.restrict_role(key, actual, targets[key])
                    except Exception:
                        accepted = True  # Unknown outcome: only read back the retained target.
                    if accepted is False:
                        save(key, "prepared")
                        return result("pending_role_fence")
                    if accepted is not True:
                        raise ValueError
                    actual = api.read_role(key)
                if not _matches(actual, targets[key], _uid(original)):
                    raise ValueError
                if record["roles"][key] != "restricted":
                    save(key, "restricted")
            if any(not _matches(api.read_role(key), targets[key], _uid(row)) for key, row in originals.items()):
                raise ValueError
        # The two phases share an anchor lock, so requalification stays outside
        # that lock. A changed or reactivated old workload cannot qualify fencing.
        retirement = retire_pool_workloads(request=request.retirement, api=api.retirement, state_dir=state, anchor_dir=anchor)
        if retirement["status"] != "old_pool_workloads_retired":
            return retirement
        # Never persist this as permanent proof: extra bindings can change while
        # the six retained Role documents and their journal remain unchanged.
        api.verify_readonly()
        return result("participant_roles_restricted")
    except Exception:
        raise ValueError("pool_role_fencing_unconfirmed_preserve_evidence") from None
