"""Narrow extension of the retained standalone rollout SSH command boundary.

The old wrapper continues to own primary commands. This module adds only
credential-free context discovery and the already installed regional target's
preflight/apply operations. It needs only Python's standard library.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

SCHEMA = "loom.nebius-regional-rollout-scope.v1"
DENIED = "Regional rollout command outside the installed scope"
LIMIT = 4 * 1024 * 1024
SECRET_KEY_TEMPLATE = r'go-template={{range $key,$value := .data}}{{if $value}}{{$key}}{{"\n"}}{{end}}{{end}}'
CLUSTER_KINDS = {"Namespace", "ClusterRole", "ClusterRoleBinding", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"}
RESOURCE_KINDS = {
    "namespaces": "Namespace", "serviceaccounts": "ServiceAccount",
    "roles.rbac.authorization.k8s.io": "Role", "rolebindings.rbac.authorization.k8s.io": "RoleBinding",
    "clusterroles.rbac.authorization.k8s.io": "ClusterRole",
    "clusterrolebindings.rbac.authorization.k8s.io": "ClusterRoleBinding",
    "resourcequotas": "ResourceQuota", "networkpolicies.networking.k8s.io": "NetworkPolicy",
    "validatingadmissionpolicies.admissionregistration.k8s.io": "ValidatingAdmissionPolicy",
    "validatingadmissionpolicybindings.admissionregistration.k8s.io": "ValidatingAdmissionPolicyBinding",
}

API_VERSIONS = {
    **{kind: "v1" for kind in ("Namespace", "ServiceAccount", "ResourceQuota", "Pod")},
    **{kind: "rbac.authorization.k8s.io/v1" for kind in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding")},
    "NetworkPolicy": "networking.k8s.io/v1",
    "ValidatingAdmissionPolicy": "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicyBinding": "admissionregistration.k8s.io/v1",
}


class ScopeError(ValueError):
    """Fixed diagnostics only; arbitrary process output is never an error message."""


def validate_scope(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "kubeconfig", "kubectl", "regions"} or value["schema_version"] != SCHEMA:
        raise ScopeError("invalid regional rollout scope")
    for field in ("kubeconfig", "kubectl"):
        path = Path(value[field])
        if not path.is_absolute() or ".." in path.parts:
            raise ScopeError("invalid regional rollout path")
    regions = value["regions"]
    if not isinstance(regions, list) or not regions or len(regions) > 8:
        raise ScopeError("explicit existing regional targets required")
    contexts = set()
    for region in regions:
        if not isinstance(region, dict) or set(region) != {"context", "cluster_name", "api_server", "namespace", "namespace_uids", "resources", "secret_key_reads"}:
            raise ScopeError("invalid regional target scope")
        for key in ("context", "cluster_name", "namespace"):
            if not isinstance(region[key], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,255}", region[key]):
                raise ScopeError("invalid regional target identity")
        if region["context"] in contexts:
            raise ScopeError("regional contexts must be unique")
        contexts.add(region["context"])
        endpoint = urlsplit(region["api_server"])
        if endpoint.scheme != "https" or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment or endpoint.path:
            raise ScopeError("explicit HTTPS regional endpoint required")
        namespace_uids = region["namespace_uids"]
        if (not isinstance(namespace_uids, dict) or region["namespace"] not in namespace_uids
                or set(namespace_uids) - {region["namespace"], region["namespace"] + "-build"}
                or any(not isinstance(uid, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,255}", uid) for uid in namespace_uids.values())):
            raise ScopeError("only retained execution and optional build namespace UIDs allowed")
        resources = region["resources"]
        if not isinstance(resources, list) or not resources or len(resources) > 64:
            raise ScopeError("existing regional resource identities required")
        identities = set()
        for resource in resources:
            if not isinstance(resource, dict) or set(resource) != {"kind", "name", "namespace"} or resource["kind"] not in set(RESOURCE_KINDS.values()):
                raise ScopeError("invalid regional resource identity")
            name = resource["name"]
            if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name):
                raise ScopeError("invalid regional resource name")
            namespace = resource["namespace"]
            if namespace is not None and namespace not in namespace_uids:
                raise ScopeError("regional resource namespace outside retained scope")
            if (resource["kind"] in CLUSTER_KINDS) != (namespace is None):
                raise ScopeError("regional resource scope differs from its API kind")
            identities.add((resource["kind"], name, namespace))
        if (any(("Namespace", namespace, None) not in identities for namespace in namespace_uids)
                or any(kind == "Namespace" and name not in namespace_uids for kind, name, _ in identities)
                or len(identities) != len(resources)):
            raise ScopeError("regional namespaces must be explicit existing resources")
        secrets = region["secret_key_reads"]
        if not isinstance(secrets, list) or len(secrets) > 4:
            raise ScopeError("explicit build secret key-name reads required")
        seen = set()
        for secret in secrets:
            if not isinstance(secret, dict) or set(secret) != {"name", "namespace", "required_keys"}:
                raise ScopeError("invalid secret key-name scope")
            if secret["namespace"] != region["namespace"] + "-build" or secret["namespace"] not in namespace_uids:
                raise ScopeError("secret key-name reads require the retained build namespace")
            if not isinstance(secret["name"], str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", secret["name"]):
                raise ScopeError("invalid secret key-name resource")
            keys = secret["required_keys"]
            if not isinstance(keys, list) or not keys or len(keys) > 8 or any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,253}", key) for key in keys):
                raise ScopeError("explicit secret key names required")
            identity = secret["namespace"], secret["name"]
            if identity in seen:
                raise ScopeError("duplicate secret key-name resource")
            seen.add(identity)
    return value


def _run(scope: dict[str, Any], args: list[str], *, payload: bytes | None = None, timeout: int = 30) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [scope["kubectl"], "--kubeconfig", scope["kubeconfig"], *args],
        input=payload, capture_output=True, timeout=timeout, check=False,
    )


def _json(result: subprocess.CompletedProcess[bytes]) -> Any:
    if result.returncode or len(result.stdout) > LIMIT:
        raise ScopeError("regional metadata unavailable")
    try:
        return json.loads(result.stdout)
    except (TypeError, ValueError):
        raise ScopeError("regional metadata unavailable") from None


def projected_contexts(scope: dict[str, Any], *, selected: str | None = None) -> dict[str, Any]:
    """Never return users, authentication plugins, tokens, client keys or CA bytes."""
    view = _json(_run(scope, ["config", "view", "-o", "json"]))
    clusters = {r["name"]: r["cluster"] for r in view.get("clusters", [])}
    contexts = {r["name"]: r["context"] for r in view.get("contexts", [])}
    output: dict[str, Any] = {"apiVersion": "v1", "kind": "Config", "clusters": [], "contexts": []}
    for region in scope["regions"]:
        if selected is not None and region["context"] != selected:
            continue
        context = contexts.get(region["context"], {})
        cluster = clusters.get(region["cluster_name"], {})
        if context.get("cluster") != region["cluster_name"] or cluster.get("server") != region["api_server"] or cluster.get("insecure-skip-tls-verify"):
            raise ScopeError("regional context differs from installed scope")
        safe = {"server": cluster["server"]}
        # Identity verification needs proof of CA-backed TLS, not CA material.
        # A fixed marker names the retained host-local CA when its data is inline.
        if selected is None:
            pass
        elif cluster.get("certificate-authority"):
            safe["certificate-authority"] = cluster["certificate-authority"]
        elif cluster.get("certificate-authority-data"):
            safe["certificate-authority"] = "retained-inline-ca"
        else:
            raise ScopeError("regional context requires trusted CA")
        output["clusters"].append({"name": region["cluster_name"], "cluster": safe})
        output["contexts"].append({"name": region["context"], "context": {"cluster": region["cluster_name"]}})
    if selected is not None and not output["contexts"]:
        raise ScopeError("regional context outside installed scope")
    return output


def _known(region: dict[str, Any], kind: str, name: str, namespace: str | None = None) -> bool:
    return {"kind": kind, "name": name, "namespace": namespace} in region["resources"]


def _read_command(region: dict[str, Any], args: list[str]) -> bool:
    if args == ["get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json"]:
        return True
    # Kubectl.get uses this exact shape, including -n for cluster-scoped VAP.
    if len(args) == 8 and args[0] == "get" and args[3:] == ["-n", region["namespace"], "--ignore-not-found", "-o", "json"]:
        kind = {"namespace": "Namespace", "validatingadmissionpolicy": "ValidatingAdmissionPolicy"}.get(args[1])
        return kind is not None and _known(region, kind, args[2])
    return False


def _permission_command(region: dict[str, Any], args: list[str]) -> bool:
    if len(args) not in {4, 6} or args[:2] != ["auth", "can-i"] or args[2] not in {"get", "create", "patch"}:
        return False
    kind = RESOURCE_KINDS.get(args[3])
    namespace = None if kind in CLUSTER_KINDS else (args[5] if len(args) == 6 and args[4] == "-n" else None)
    return (kind is not None and args[4:] == ([] if kind in CLUSTER_KINDS else ["-n", namespace])
            and any(r["kind"] == kind and r["namespace"] == namespace for r in region["resources"]))


def _secret_command(region: dict[str, Any], args: list[str]) -> dict[str, Any] | None:
    if len(args) != 7 or args[:2] != ["get", "secret"] or args[3] != "-n" or args[5:] != ["-o", SECRET_KEY_TEMPLATE]:
        return None
    return next((row for row in region["secret_key_reads"] if row["name"] == args[2] and row["namespace"] == args[4]), None)


def _manifest(scope: dict[str, Any], region: dict[str, Any], payload: bytes, *, dry_run: bool) -> None:
    if not payload or len(payload) > LIMIT:
        raise ScopeError("invalid regional manifest")
    # Reuse kubectl's native YAML/JSON parser without installing another parser
    # on the gateway. Client dry-run cannot create resources; validation/discovery
    # may make read-only API requests. The original bytes are later applied.
    result = _run(scope, ["--context", region["context"], "--request-timeout=20s", "create", "--dry-run=client", "--validate=false", "-f", "-", "-o", "json"], payload=payload, timeout=25)
    if result.returncode or len(result.stdout) > LIMIT:
        raise ScopeError("invalid regional manifest")
    # kubectl prints concatenated JSON objects for multi-document YAML. A JSON
    # List is also supported; neither representation can bypass per-object scope.
    decoder = json.JSONDecoder()
    remaining = result.stdout.decode().strip()
    documents = []
    while remaining:
        data, offset = decoder.raw_decode(remaining)
        documents.extend(data.get("items", []) if data.get("kind") == "List" else [data])
        if len(documents) > 64:
            raise ScopeError("invalid regional manifest")
        remaining = remaining[offset:].lstrip()
    if not documents or len(documents) > 64:
        raise ScopeError("invalid regional manifest")
    for doc in documents:
        metadata = doc.get("metadata", {})
        kind, name = doc.get("kind"), metadata.get("name")
        if kind not in API_VERSIONS or doc.get("apiVersion") != API_VERSIONS[kind]:
            raise ScopeError("regional manifest API group outside installed resource identities")
        if metadata.get("generateName"):
            raise ScopeError("generated regional resource names unsupported")
        if kind == "Pod":
            if not dry_run or name != "loom-execution-policy-probe" or metadata.get("namespace") != region["namespace"]:
                raise ScopeError("only the existing server-dry-run policy probe is allowed")
        elif not _known(region, kind, name, metadata.get("namespace")):
            raise ScopeError("regional manifest outside installed resource identities")


def handle(scope: dict[str, Any], command: str, *, stdin: bytes | None = None) -> tuple[int, bytes, bytes] | None:
    """Return None for old primary/fixed-reader commands, preserving their grants."""
    args = shlex.split(command)
    full = args[3:] in (["config", "view", "-o", "json"], ["--request-timeout=30s", "config", "view", "-o", "json"])
    explicit = any(a.startswith("--context") for a in args)
    if not full and not explicit:
        return None
    if args[:3] != ["kubectl", "--kubeconfig", scope["kubeconfig"]]:
        raise ScopeError("regional entrypoint differs")
    rest = args[3:]
    if full:
        return 0, (json.dumps(projected_contexts(scope)) + "\n").encode(), b""
    if len(rest) < 3 or rest[0] != "--context":
        raise ScopeError("regional context must use its fixed argument position")
    region = next((r for r in scope["regions"] if r["context"] == rest[1]), None)
    if region is None:
        raise ScopeError("regional context outside installed scope")
    rest = rest[2:]
    if rest[:1] == ["--request-timeout=30s"]:
        rest = rest[1:]
    if any(a.startswith(("--kubeconfig", "--server", "--context", "--token", "--client-", "--certificate-authority", "--as", "--raw")) for a in rest):
        raise ScopeError("regional connection overrides forbidden")
    projection = projected_contexts(scope, selected=region["context"])
    if rest == ["config", "view", "--minify", "-o", "json"]:
        return 0, (json.dumps(projection) + "\n").encode(), b""
    apply = rest in (["apply", "-f", "-"], ["apply", "-f", "-", "--dry-run=server"])
    if apply:
        payload = sys.stdin.buffer.read(LIMIT + 1) if stdin is None else stdin
        _manifest(scope, region, payload, dry_run="--dry-run=server" in rest)
        for name, uid in region["namespace_uids"].items():
            namespace = _json(_run(scope, ["--context", region["context"], "--request-timeout=10s", "get", "namespace", name, "-o", "json"], timeout=15))
            if namespace.get("metadata", {}).get("uid") != uid:
                raise ScopeError("regional namespace identity changed")
    elif secret := _secret_command(region, rest):
        namespace = _json(_run(scope, ["--context", region["context"], "--request-timeout=10s", "get", "namespace", secret["namespace"], "-o", "json"], timeout=15))
        if namespace.get("metadata", {}).get("uid") != region["namespace_uids"][secret["namespace"]]:
            raise ScopeError("regional namespace identity changed")
        result = _run(scope, ["--context", region["context"], "--request-timeout=30s", *rest], timeout=40)
        if result.returncode or len(result.stdout) > LIMIT:
            return result.returncode or 126, b"", b"Regional secret key names unavailable\n"
        # Only declared populated key names cross the boundary. Never forward
        # arbitrary stdout/stderr from this credential-adjacent operation.
        observed = set(result.stdout.decode().splitlines())
        keys = sorted(observed.intersection(secret["required_keys"]))
        return 0, ("".join(key + "\n" for key in keys)).encode(), b""
    elif _read_command(region, rest) or _permission_command(region, rest):
        payload = None
    else:
        raise ScopeError("regional operation outside installed scope")
    result = _run(scope, ["--context", region["context"], "--request-timeout=30s", *rest], payload=payload, timeout=40)
    return result.returncode, result.stdout, result.stderr


def entry(scope_path: str) -> int | None:
    try:
        scope = validate_scope(json.loads(Path(scope_path).read_text()))
        result = handle(scope, os.environ.get("SSH_ORIGINAL_COMMAND", ""))
        if result is None:
            return None
        code, stdout, stderr = result
        sys.stdout.buffer.write(stdout)
        sys.stderr.buffer.write(stderr)
        return code
    except Exception:
        print(DENIED, file=sys.stderr)
        return 126
