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
from scripts.ops.nebius_pool_platform_authority import (
    PoolPlatformAuthority,
    qualify_platform_authority,
)
from scripts.ops.nebius_pool_projection import pure_projection
from scripts.ops.nebius_pool_retirement import (
    PoolRetirementAPI,
    PoolRetirementRequest,
    retire_pool_workloads,
    retirement_documents,
)
from scripts.ops.nebius_pool_runtime import participant_readonly_roles

from loom.nebius_platform_render import digest

MARKER = "loom.nebius/pool-role-fencing-operation"

POOL_WRITER_WORKLOAD_COLLECTIONS = (
    ("apps/v1", "deployments", "Deployment"), ("apps/v1", "statefulsets", "StatefulSet"),
    ("apps/v1", "daemonsets", "DaemonSet"), ("apps/v1", "replicasets", "ReplicaSet"),
    ("v1", "replicationcontrollers", "ReplicationController"),
    ("batch/v1", "cronjobs", "CronJob"), ("batch/v1", "jobs", "Job"), ("v1", "pods", "Pod"),
)


@dataclass(frozen=True, repr=False)
class PoolRoleFenceRequest:
    retirement: PoolRetirementRequest
    originals: tuple[dict[str, Any], ...]


class PoolRoleFenceAPI(Protocol):
    @property
    def retirement(self) -> PoolRetirementAPI: ...

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
            ("", "nodes"): {"get", "list", "watch"}, ("", "nodes/stats"): {"get"},
            ("", "pods"): {"get", "list", "watch"},
            ("", "pods/log"): {"get"}, ("", "namespaces"): {"get"}, ("batch", "jobs"): {"get", "list", "watch"},
            ("apps", "daemonsets"): {"get", "list"},
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


@pure_projection
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


def qualify_retained_writer_bindings(request: PoolRoleFenceRequest,
                                     inventory: dict[str, list[dict[str, Any]]], *,
                                     staged_authority: dict[str, dict[str, Any]] | None = None,
                                     platform_authority: PoolPlatformAuthority | None = None) -> None:
    """Qualify affected bindings without declaring unrelated authority fenced.

    Complete RBAC discovery protects both sides of a Role reduction: no foreign
    subject may share a retained Role, and no retired identity may keep an extra
    named, group or cross-namespace grant. Native/operator identities require
    explicit, pinned platform trust; names alone are not an exemption. Unrelated
    namespace-local bindings outside the pool remain unchanged.
    Effective rules reviews remain mandatory after the reductions. The parent
    still owns complete external-writer and runtime/backend qualification.
    """
    try:
        originals = {_key(row): row for row in request.originals}
        staged = {} if staged_authority is None else staged_authority
        targets = role_fence_documents(request)
        subjects, _ = role_fence_review_scope(request)
        writer_namespaces = {namespace.name for participant in request.retirement.migration.registration.spec.participants
            for namespace in (participant.execution_namespace, participant.build_namespace)}
        expected_subjects = {}
        for row in participant_readonly_roles(request=request.retirement.migration):
            if row["kind"] == "RoleBinding":
                key = "Role:" + row["metadata"]["namespace"] + ":" + row["roleRef"]["name"]
                expected_subjects[key] = {(item["namespace"], item["name"]) for item in row["subjects"]}
        kinds = {"roles": "Role", "clusterroles": "ClusterRole",
            "rolebindings": "RoleBinding", "clusterrolebindings": "ClusterRoleBinding"}
        if set(inventory) != set(kinds):
            raise ValueError
        documents: dict[str, dict[str, Any]] = {}
        uids: set[str] = set()
        for resource, kind in kinds.items():
            for row in inventory[resource]:
                metadata = row["metadata"]
                uid, key = _uid(row), _key(row)
                if (row.get("apiVersion") != "rbac.authorization.k8s.io/v1" or row.get("kind") != kind
                        or not isinstance(metadata.get("name"), str) or not metadata["name"]
                        or not isinstance(metadata.get("resourceVersion"), str) or not metadata["resourceVersion"]
                        or (kind in {"Role", "RoleBinding"}) != bool(metadata.get("namespace"))
                        or key in documents or uid in uids):
                    raise ValueError
                documents[key], uids = row, uids | {uid}
        platform = qualify_platform_authority(platform_authority,
            kube_system_uid=request.retirement.migration.registration.binding.kube_system_uid, documents=documents)
        for key, original in originals.items():
            actual = documents[key]
            if not any(_matches(actual, wanted, _uid(original)) for wanted in (original, targets[key])):
                raise ValueError

        def accounts(subject: dict[str, Any]) -> set[tuple[str, str]]:
            kind, name = subject["kind"], subject["name"]
            if not isinstance(name, str) or not name or len(name) > 1024:
                raise ValueError
            if kind == "ServiceAccount":
                if (subject.keys() - {"kind", "name", "namespace", "apiGroup"}
                        or subject.get("apiGroup", "") != "" or not subject.get("namespace")):
                    raise ValueError
                return {(subject["namespace"], name)}
            if (kind not in {"User", "Group"} or set(subject) != {"kind", "name", "apiGroup"}
                    or subject["apiGroup"] != "rbac.authorization.k8s.io"):
                raise ValueError
            return {(namespace, account) for namespace, account in subjects
                if (kind == "User" and name == f"system:serviceaccount:{namespace}:{account}")
                or (kind == "Group" and name in {"system:authenticated", "system:serviceaccounts",
                    "system:serviceaccounts:" + namespace})}

        found = set()
        for resource in ("rolebindings", "clusterrolebindings"):
            for binding in inventory[resource]:
                reference = binding["roleRef"]
                namespace = binding["metadata"].get("namespace", "-")
                if (set(reference) != {"apiGroup", "kind", "name"}
                        or reference["apiGroup"] != "rbac.authorization.k8s.io"
                        or reference["kind"] not in {"Role", "ClusterRole"}
                        or (resource == "clusterrolebindings" and reference["kind"] != "ClusterRole")):
                    raise ValueError
                key = reference["kind"] + ":" + (namespace if reference["kind"] == "Role" else "-") + ":" + reference["name"]
                role = documents[key]
                entries = binding.get("subjects", [])
                if not isinstance(entries, list) or len(entries) > 1000:
                    raise ValueError
                identities = [accounts(entry) for entry in entries]
                binding_key = _key(binding)
                if binding_key in staged:
                    if (key not in staged or not _matches(binding, staged[binding_key], _uid(staged[binding_key]))
                            or not _matches(role, staged[key], _uid(staged[key]))):
                        raise ValueError
                elif key in originals:
                    _snapshot(binding)
                    if (not identities or any(entry["kind"] == "Group" or not identity
                            or not identity <= expected_subjects[key] for entry, identity in zip(entries, identities, strict=True))
                            or set().union(*identities) != expected_subjects[key]):
                        raise ValueError
                    found.add(key)
                elif any(identity & set(subjects) for identity in identities):
                    _snapshot(binding)
                    if "aggregationRule" in role:
                        raise ValueError
                    rules = role.get("rules", [])
                    qualify_pool_reader_rules({"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview",
                        "spec": {}, "status": {"incomplete": False,
                            "resourceRules": [rule for rule in rules if "nonResourceURLs" not in rule],
                            "nonResourceRules": [rule for rule in rules if "nonResourceURLs" in rule],
                            "evaluationError": ""}}, namespace=namespace)
                elif binding_key not in platform and (resource == "clusterrolebindings" or namespace in writer_namespaces):
                    # CronJob mutations can create Jobs indirectly through the
                    # trusted native controller, without any Job grant to the
                    # caller. Named grants and suspended schedules count too.
                    for rule in role.get("rules", []):
                        if (set(rule.get("apiGroups", [])) & {"batch", "*"}
                                and set(rule.get("resources", [])) & {"jobs", "cronjobs", "*"}
                                and set(rule.get("verbs", [])) & {"*", "create", "update", "patch", "delete", "deletecollection"}):
                            raise ValueError
        if found != set(originals):
            raise ValueError
    except Exception:
        raise ValueError("pool_retained_writer_binding_inventory_unqualified") from None


def _terminal_platform_job(job: dict[str, Any], pods: list[dict[str, Any]],
                           identities: dict[str, tuple[str, str]]) -> bool:
    """Prove a native singleton Job and every associated process are inert.

    This is census evidence only: no adoption, deletion or retirement target.
    The caller limits it to retained CP identities outside worker namespaces.
    """
    uid, spec, status = _uid(job), job["spec"], job.get("status", {})
    if (job["metadata"].get("ownerReferences", []) != []
            or spec.get("managedBy", "kubernetes.io/job-controller") != "kubernetes.io/job-controller"
            or spec.get("manualSelector", False) is not False
            or spec.get("completionMode", "NonIndexed") != "NonIndexed"
            or any(type(spec.get(field, 1)) is not int or spec.get(field, 1) != 1
                for field in ("parallelism", "completions"))
            or spec["template"]["spec"].get("restartPolicy") != "Never"
            or any(type(status.get(field, 0)) is not int or status.get(field, 0) != 0
                for field in ("active", "terminating"))):
        return False
    terminal = [row["type"] for row in status.get("conditions", [])
        if row.get("type") in {"Complete", "Failed"} and row.get("status") == "True"]
    if len(terminal) != 1:
        return False
    selector = spec["selector"]
    labels = selector.get("matchLabels", {})
    if (set(selector) - {"matchLabels", "matchExpressions"} or selector.get("matchExpressions", []) != []
            or not labels or set(labels) - {"controller-uid", "batch.kubernetes.io/controller-uid"}
            or any(value != uid for value in labels.values())
            or any(spec["template"]["metadata"].get("labels", {}).get(key) != value for key, value in labels.items())):
        return False
    identity = identities[uid]
    for pod in pods:
        owners = pod["metadata"].get("ownerReferences", [])
        selected = pod["metadata"]["namespace"] == identity[0] and all(
            pod["metadata"].get("labels", {}).get(key) == value for key, value in labels.items())
        if not selected and not any(owner.get("uid") == uid for owner in owners):
            continue
        if len(owners) != 1 or identities[_uid(pod)] != identity:
            return False
        owner = dict(owners[0])
        blocking = owner.pop("blockOwnerDeletion", False)
        if (type(blocking) is not bool or owner != {"apiVersion": "batch/v1", "kind": "Job",
                "name": job["metadata"]["name"], "uid": uid, "controller": True}
                or pod["spec"].get("restartPolicy") != "Never"
                or pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
                or not pod["spec"].get("containers")):
            return False
        for field, status_field in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses"),
                ("ephemeralContainers", "ephemeralContainerStatuses")):
            containers = pod["spec"].get(field, [])
            expected = {row["name"] for row in containers}
            statuses = pod["status"].get(status_field, [])
            if (len(expected) != len(containers) or len(statuses) != len(expected)
                    or {row["name"] for row in statuses} != expected):
                return False
            for row in statuses:
                state = row.get("state", {})
                if set(state) != {"terminated"} or type(state["terminated"].get("exitCode")) is not int:
                    return False
    return True


def qualify_retained_writer_workloads(request: PoolRoleFenceRequest,
                                      inventory: dict[str, list[dict[str, Any]]], *,
                                      originals: dict[str, dict[str, Any]],
                                      expected: dict[str, dict[str, Any]],
                                      platform_subjects: frozenset[tuple[str, str]] = frozenset()) -> None:
    """Account for built-in workloads declaring retiring ServiceAccounts.

    Exact retained roots and typed UID ancestry qualify ownership, not process
    shutdown or workload contents. Inert historical descendants still count;
    the retirement phase independently proves their drain. Standalone native
    platform Jobs qualify only with complete terminal process proof. Unrelated accounts
    remain untouched. This does not attest external tokens or custom controllers.
    """
    try:
        retired = retirement_documents(request.retirement)
        subjects, _ = role_fence_review_scope(request)
        writer_namespaces = {namespace.name for participant in request.retirement.migration.registration.spec.participants
            for namespace in (participant.execution_namespace, participant.build_namespace)}
        if (set(inventory) != {resource for _api, resource, _kind in POOL_WRITER_WORKLOAD_COLLECTIONS}
                or set(expected) != set(originals)
                or any(originals.get(key) != row for key, row in retired.items())):
            raise ValueError
        documents: dict[str, dict[str, Any]] = {}
        identities: dict[str, tuple[str, str]] = {}
        keys: dict[str, str] = {}

        def name(value: Any) -> str:
            if (not isinstance(value, str) or len(value) > 253
                    or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*", value) is None):
                raise ValueError
            return value

        for api, resource, kind in POOL_WRITER_WORKLOAD_COLLECTIONS:
            for row in inventory[resource]:
                metadata = row["metadata"]
                uid, key = _uid(row), _key(row)
                namespace = name(metadata["namespace"])
                name(metadata["name"])
                version = metadata["resourceVersion"]
                if (row.get("apiVersion") != api or row.get("kind") != kind
                        or not isinstance(version, str) or not 0 < len(version) <= 1024
                        or uid in documents or key in keys):
                    raise ValueError
                documents[uid], keys[key] = row, uid
                # Kubernetes permits adopt-only replication controllers with
                # no template. They cannot choose a Pod identity; any adopted
                # Pods still pass the independent complete Pod inventory below.
                if kind == "ReplicationController" and row["spec"].get("template") is None:
                    continue
                pod = (row["spec"] if kind == "Pod" else row["spec"]["jobTemplate"]["spec"]["template"]["spec"]
                    if kind == "CronJob" else row["spec"]["template"]["spec"])
                account = name(pod.get("serviceAccountName", "default"))
                if pod.get("serviceAccount", account) != account:
                    raise ValueError
                identities[uid] = (namespace, account)
                if identities[uid] in platform_subjects:
                    raise ValueError  # Managed native identities are not application ServiceAccounts.
        roots: set[str] = set()
        for key, original in originals.items():
            uid = _uid(original)
            if keys.get(key) != uid or not _matches(documents[uid], expected[key], uid):
                raise ValueError
            roots.add(uid)

        controllers = {_uid(guard.controller) for guard in request.retirement.migration.guards}
        other_subjects = {identities[_uid(row)] for row in retired.values() if _uid(row) not in controllers}
        history_subjects = {identities[uid] for uid in controllers if identities[uid][0] not in writer_namespaces} - other_subjects
        for row in inventory["jobs"]:
            uid = _uid(row)
            if identities[uid] in history_subjects and _terminal_platform_job(row, inventory["pods"], identities):
                roots.add(uid)

        for uid, identity in identities.items():
            if (documents[uid]["kind"] == "CronJob" and identity[0] in writer_namespaces
                    and uid not in roots):
                # Existing schedules need no remaining creator credential to
                # emit Jobs. Require a retained, phase-bound original even for
                # another ServiceAccount or a currently suspended schedule.
                raise ValueError
            if identity not in subjects:
                continue
            # The supported ancestry has at most two edges. No replica/phase
            # shortcut, label adoption, dangling parent or arbitrary owner kind.
            visited: set[str] = set()
            while uid not in roots:
                if uid in visited or len(visited) >= 2:
                    raise ValueError
                visited.add(uid)
                row = documents[uid]
                owners = row["metadata"].get("ownerReferences", [])
                if not isinstance(owners, list) or len(owners) != 1:
                    raise ValueError
                owner = dict(owners[0])
                blocking = owner.pop("blockOwnerDeletion", False)
                parent_uid = owner["uid"]
                parent = documents[parent_uid]
                if (type(blocking) is not bool or owner.get("controller") is not True
                        or owner != {"apiVersion": parent["apiVersion"], "kind": parent["kind"],
                            "name": parent["metadata"]["name"], "uid": parent_uid, "controller": True}
                        or (row["kind"], parent["kind"]) not in {
                            ("Pod", "ReplicaSet"), ("Pod", "Job"), ("Pod", "StatefulSet"),
                            ("ReplicaSet", "Deployment"), ("Job", "CronJob")}
                        or identities[parent_uid] != identity):
                    raise ValueError
                uid = parent_uid
    except Exception:
        raise ValueError("pool_retained_writer_workload_inventory_unqualified") from None


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
