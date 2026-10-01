"""Read-only declared controller topology; never a writer-fencing attestation."""
from __future__ import annotations

import base64
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID

from scripts.ops.deploy_nebius_platform import DeploymentError, Kubectl

_ACTUATOR = "LOOM_EXECUTION_ACTUATOR_"
_COLLECTOR = "LOOM_EXECUTION_CAPACITY_COLLECTOR_"
_DATABASES = {"LOOM_CP_DB_URL", "LOOM_CP_DB_URL_POOL", _ACTUATOR + "DB_URL"}
_VALUES = {"LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT", "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID",
    *(_ACTUATOR + suffix for suffix in ("TARGET_ID", "NAMESPACE", "SERVICE_ACCOUNT_NAME", "RUNTIME_CLASS_NAME")),
    *(_COLLECTOR + suffix for suffix in ("TARGET_ID", "POOL_ID", "NAMESPACE", "NEBIUS_NODE_GROUP_ID",
        "NEBIUS_PROJECT_ID", "NEBIUS_QUOTA_PARENT_ID", "NEBIUS_REGION"))}
_WRITES = {"create", "update", "patch", "delete", "deletecollection"}


def _identity(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row["metadata"]
    return {**{key: metadata[key] for key in ("name", "namespace", "uid") if key in metadata},
        "resource_version": metadata["resourceVersion"]}


def _setting(name: str, entry: dict[str, Any], namespace: str) -> dict[str, Any]:
    source = entry.get("valueFrom", {})
    for kind, field in (("secretKeyRef", "secret_ref"), ("configMapKeyRef", "config_map_ref")):
        if kind in source:
            ref = source[kind]
            return {field: {"namespace": namespace, "name": ref["name"], "key": ref["key"]}}
    value = entry.get("value")
    if name in _VALUES and isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
        return {"value": value}
    return {"value_withheld": True}


def _container(row: dict[str, Any], namespace: str, maps: Callable[[str, str], dict[str, Any]]) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    references, unresolved = [], []
    for source in row.get("envFrom", []):
        prefix = source.get("prefix", "")
        if "secretRef" in source:
            unresolved.append({"namespace": namespace, "name": source["secretRef"]["name"], "prefix": prefix})
            # An unread Secret can override any earlier matching envFrom setting.
            for name in tuple(settings):
                if name.startswith(prefix):
                    settings[name] = {"value_withheld": True}
        elif "configMapRef" in source:
            cm = maps(namespace, source["configMapRef"]["name"])
            references.append(_identity(cm))
            for key, value in cm.get("data", {}).items():
                name = prefix + key
                if name in _VALUES | _DATABASES:
                    settings[name] = _setting(name, {"value": value}, namespace)
    for entry in row.get("env", []):
        name = entry["name"]
        if name in _VALUES | _DATABASES:
            settings[name] = _setting(name, entry, namespace)
    return {"name": row["name"], "settings": settings, "config_maps": references,
        "unresolved_secret_env_from": unresolved}


def _database_endpoints(kube: Kubectl, controllers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Observe explicit DB references only; never export a URL or authentication."""
    secrets: dict[tuple[str, str], dict[str, Any] | None] = {}
    reports = []
    for controller in controllers:
        for container in controller["containers"]:
            for setting in sorted(_DATABASES & container["settings"].keys()):
                report: dict[str, Any] = {"controller": {key: controller[key] for key in
                    ("kind", "namespace", "name", "uid", "resource_version")},
                    "container": container["name"], "setting": setting, "status": "unavailable"}
                reports.append(report)
                try:
                    ref = container["settings"][setting]["secret_ref"]
                    if (ref["namespace"] != controller["namespace"]
                            or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", ref["name"])
                            or not re.fullmatch(r"[A-Za-z0-9._-]{1,253}", ref["key"])):
                        raise ValueError
                    identity = ref["namespace"], ref["name"]
                    if identity not in secrets:
                        secrets[identity] = None  # A failed GET is not retried for a sibling.
                        secrets[identity] = kube.get("secret", ref["name"], ref["namespace"])
                    secret = secrets[identity]
                    if secret is None:
                        raise ValueError
                    meta = secret["metadata"]
                    if (secret.get("apiVersion") != "v1" or secret.get("kind") != "Secret"
                            or (meta.get("namespace"), meta.get("name")) != identity
                            or str(UUID(meta["uid"])) != meta["uid"] or not UUID(meta["uid"]).int
                            or not meta.get("resourceVersion") or meta.get("deletionTimestamp")):
                        raise ValueError
                    encoded = secret["data"][ref["key"]]
                    if not isinstance(encoded, str) or len(encoded) > 131072:
                        raise ValueError
                    raw = base64.b64decode(encoded, validate=True).decode()
                    if not raw or any(ord(char) < 33 or ord(char) == 127 for char in raw):
                        raise ValueError
                    url = urlsplit(raw)
                    query = parse_qsl(url.query, keep_blank_values=True, strict_parsing=True)
                    # Only the canonical subset shared with SQLAlchemy is safe
                    # to project. In particular it does not decode DB paths and
                    # does not treat a raw @ in the password like urllib does.
                    host, database = url.hostname, url.path.removeprefix('/')
                    port = 5432 if url.port is None else url.port
                    if (url.scheme not in {"postgresql", "postgresql+psycopg", "postgresql+asyncpg"}
                            or not raw.startswith(url.scheme + '://')
                            or url.netloc.count('@') != 1 or url.netloc.endswith(':')
                            or not url.username or not url.password or url.fragment
                            or host is None or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", host)
                            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$-]{0,62}", database) or not 1 <= port <= 65535
                            or len({key for key, _ in query}) != len(query)
                            or any(key not in {"sslmode", "sslrootcert", "sslcert", "sslkey", "connect_timeout", "application_name"}
                                for key, _ in query)):
                        raise ValueError
                    report.update(status="observed", credential={**_identity(secret), "key": ref["key"]},
                        endpoint={"host": host, "port": port, "database": database})
                except Exception:
                    # Endpoint diagnostics must not leak server errors or URL fragments.
                    pass
    return reports


def controller_inventory(kube: Kubectl, read_list: Callable[[str, bool], list[dict[str, Any]]]) -> dict[str, Any]:
    """Discover guests/aliases by inventory, not a hard-coded environment count.

    Only selected identifiers, DB endpoint fields and Secret identities leave
    this function. Only explicit DB Secret references are fetched. No Pod exec,
    mutation, arbitrary env/args, database URL, username, password or token.
    Observations are not atomic and do not establish effective authorization.
    """
    cache: dict[tuple[str, str], dict[str, Any]] = {}

    def config_map(namespace: str, name: str) -> dict[str, Any]:
        key = namespace, name
        if key not in cache:
            cm = kube.get("configmap", name, namespace)
            if (cm["metadata"]["namespace"], cm["metadata"]["name"]) != key:
                raise DeploymentError("controller configuration identity differs")
            cache[key] = cm
        return cache[key]

    controllers = []
    for resource, kind in (("deployments", "Deployment"), ("cronjobs", "CronJob")):
        for row in read_list(resource, True):
            spec = row["spec"]
            template = spec["template"] if kind == "Deployment" else spec["jobTemplate"]["spec"]["template"]
            pod = template["spec"]
            controllers.append({**_identity(row), "kind": kind,
                **({"replicas": spec.get("replicas", 1)} if kind == "Deployment" else {"suspended": spec.get("suspend", False)}),
                "service_account": pod.get("serviceAccountName", "default"),
                "containers": [_container(item, row["metadata"]["namespace"], config_map)
                    for item in [*pod.get("initContainers", []), *pod.get("containers", [])]]})

    roles: dict[tuple[str, str | None, str], dict[str, Any]] = {}
    for resource, kind in (("roles", "Role"), ("clusterroles", "ClusterRole")):
        for row in read_list(resource, kind == "Role"):
            meta = row["metadata"]
            roles[kind, meta.get("namespace"), meta["name"]] = row
    writers, unresolved = [], []
    for resource, kind in (("rolebindings", "RoleBinding"), ("clusterrolebindings", "ClusterRoleBinding")):
        for row in read_list(resource, kind == "RoleBinding"):
            ref = row["roleRef"]
            identity = {**_identity(row), "kind": kind}
            namespace = row["metadata"].get("namespace") if ref["kind"] == "Role" else None
            role = roles.get((ref["kind"], namespace, ref["name"]))
            if ref.get("apiGroup") != "rbac.authorization.k8s.io" or role is None:
                unresolved.append(identity)
                continue
            rules = []
            for rule in role.get("rules") or []:
                if not (set(rule.get("apiGroups", [])) & {"batch", "*"}
                        and set(rule.get("resources", [])) & {"jobs", "*"}):
                    continue
                verbs = set(rule["verbs"])
                writes = _WRITES if "*" in verbs else _WRITES & verbs
                if writes:
                    rules.append({"verbs": sorted(writes), "resource_names": rule.get("resourceNames", [])})
            if rules:
                writers.append({**identity,
                    "role": {"kind": ref["kind"], "name": ref["name"], "uid": role["metadata"]["uid"]},
                    "subjects": [{key: item[key] for key in ("kind", "name", "namespace") if key in item}
                        for item in row.get("subjects") or []], "rules": rules})
    return {"controllers": controllers, "declared_database_endpoints": _database_endpoints(kube, controllers),
        "job_write_bindings": writers, "unresolved_bindings": unresolved,
        "unverified": ["resolved_database_identity", "running_configuration_matches_templates",
            "effective_writer_fencing", "non_job_workload_and_external_writer_authority"]}
