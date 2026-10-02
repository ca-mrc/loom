"""Render a Nebius-local execution attachment to the existing staging authority.

This is an offline render, not a provisioner. All credentials are references to
pre-provisioned namespace-local Secrets; no database or canonical store is created.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_KEY = re.compile(r"[A-Za-z0-9._-]{1,253}\Z")
_IMAGE = re.compile(r"[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}\Z")
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_DB_CA_DIRECTORY = "/var/run/loom/postgres-tls"
_DB_CA_VOLUME = "loom-postgres-ca"
_RFC1918 = tuple(
    ipaddress.IPv4Network(cidr) for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_NETWORKS = frozenset(
    {
        "database",
        "canonical_store",
        "source_store",
        "control_plane",
        "kubernetes_api",
        "kubelet",
        "provider_api",
        "model_api",
    }
)


class StagingAttachmentError(ValueError):
    """Attachment is incomplete, ambiguous or contains unsupported values."""


def _object(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise StagingAttachmentError(f"{label} fields are invalid")
    return value


def _name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise StagingAttachmentError(f"{label} is invalid")
    return value


def _secret(value: Any, fields: set[str], label: str) -> dict[str, Any]:
    result = _object(value, fields | {"name"}, label)
    _name(result["name"], label)
    for field in fields:
        if not isinstance(result[field], str) or not _KEY.fullmatch(result[field]):
            raise StagingAttachmentError(f"{label} key reference is invalid")
    return result


def _endpoint(value: Any, label: str) -> str:
    try:
        if not isinstance(value, str) or any(c.isspace() for c in value):
            raise ValueError
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
            or parsed.port == 0
        ):
            raise ValueError
    except ValueError:
        raise StagingAttachmentError(f"{label} must be a credential-free HTTPS origin") from None
    return value


def validate_staging_attachment(
    value: dict[str, Any], *, environment: str, target: dict[str, Any]
) -> None:
    _object(
        value,
        {
            "schema_version",
            "environment",
            "target_id",
            "namespace",
            "canonical_database",
            "gateway_image",
            "configuration_revision",
            "local_providers_secret_name",
            "canonical",
            "source",
            "gateway_secret",
            "collector",
            "network",
        }
        | (
            {field for field in ("private_entry", "database_tls") if field in value}
            if isinstance(value, dict)
            else set()
        ),
        "staging attachment",
    )
    if (
        environment != "staging"
        or value["environment"] != "staging"
        or value["schema_version"] != "loom.nebius-staging-attachment.v1"
        or value["target_id"] != target["target_id"]
        or value["namespace"] != target["namespace_name"]
        # Existing shared staging uses `loom`; retain the named staging profile
        # for compatibility. This declaration is not proof of the Secret DSN's
        # endpoint identity (development can also have a database named `loom`).
        or value["canonical_database"] not in ("loom", "loom_staging")
    ):
        raise StagingAttachmentError(
            "attachment must bind the selected staging target and database"
        )
    if not isinstance(value["gateway_image"], str) or not _IMAGE.fullmatch(value["gateway_image"]):
        raise StagingAttachmentError("staging gateway image must be digest-pinned")
    if not isinstance(value["configuration_revision"], str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["configuration_revision"]
    ):
        raise StagingAttachmentError("configuration_revision must be a SHA-256 deployment revision")
    _name(value["local_providers_secret_name"], "local providers Secret name")
    canonical = _object(
        value["canonical"],
        {
            "endpoint",
            "region",
            "artifacts_bucket",
            "trajectories_bucket",
            "db_secret",
            "storage_secret",
        },
        "canonical",
    )
    _endpoint(canonical["endpoint"], "canonical endpoint")
    _name(canonical["region"], "canonical region")
    if (
        canonical["artifacts_bucket"] != "loom-staging-artifacts"
        or canonical["trajectories_bucket"] != "loom-staging-trajectories"
    ):
        raise StagingAttachmentError("canonical buckets must be the existing staging buckets")
    _secret(canonical["db_secret"], {"gateway_key", "actuator_key"}, "canonical DB Secret")
    _secret(canonical["storage_secret"], {"access_key", "secret_key"}, "canonical store Secret")
    source = _object(
        value["source"], {"endpoint", "region", "bucket", "credentials_secret"}, "source"
    )
    _endpoint(source["endpoint"], "source endpoint")
    _name(source["region"], "source region")
    _name(source["bucket"], "source bucket")
    if source["endpoint"].rstrip("/") == canonical["endpoint"].rstrip("/"):
        raise StagingAttachmentError("staging attachment requires an independent source spool")
    _secret(source["credentials_secret"], {"access_key", "secret_key"}, "source Secret")
    _secret(value["gateway_secret"], {"step_jwt_key", "master_key"}, "gateway Secret")
    collector = _object(
        value["collector"], {"control_plane_url", "token_secret", "nebius_secret"}, "collector"
    )
    _endpoint(collector["control_plane_url"], "collector control-plane URL")
    _secret(collector["token_secret"], {"key"}, "collector token Secret")
    _secret(collector["nebius_secret"], {"key"}, "Nebius observer Secret")
    if isinstance(value["network"], dict) and "kubelet" not in value["network"]:
        raise StagingAttachmentError("network kubelet requires explicit private node destinations")
    network = _object(value["network"], set(_NETWORKS), "network")
    for name, entries in network.items():
        if not isinstance(entries, list) or not entries or len(entries) > 32:
            raise StagingAttachmentError(f"network {name} requires bounded destinations")
        for item in entries:
            _object(item, {"cidr", "port"}, f"network {name}")
            try:
                subnet = ipaddress.ip_network(item["cidr"], strict=True)
            except (TypeError, ValueError):
                raise StagingAttachmentError(f"network {name} CIDR is invalid") from None
            if subnet.prefixlen == 0 or subnet.is_loopback or subnet.is_multicast:
                raise StagingAttachmentError(
                    f"network {name} CIDR must be a scoped routed destination"
                )
            if type(item["port"]) is not int or not 1 <= item["port"] <= 65535:
                raise StagingAttachmentError(f"network {name} TCP port is invalid")
            if name == "kubelet":
                private_node_subnet = (
                    any(subnet.subnet_of(parent) for parent in _RFC1918)
                    if isinstance(subnet, ipaddress.IPv4Network)
                    else subnet.subnet_of(ipaddress.IPv6Network("fc00::/7"))
                )
                if not private_node_subnet or item["port"] != 10250:
                    raise StagingAttachmentError(
                        "network kubelet requires private node CIDRs on TCP 10250"
                    )
    for endpoint, name in (
        (canonical["endpoint"], "canonical_store"),
        (source["endpoint"], "source_store"),
        (collector["control_plane_url"], "control_plane"),
    ):
        parsed = urlsplit(endpoint)
        if (parsed.port or 443) not in {item["port"] for item in network[name]}:
            raise StagingAttachmentError(f"network {name} does not permit its endpoint port")
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError:
            continue  # DNS/IP convergence is a deployment preflight, never a render side effect.
        if not any(
            address in ipaddress.ip_network(item["cidr"])
            for item in network[name]
            if item["port"] == (parsed.port or 443)
        ):
            raise StagingAttachmentError(f"network {name} does not contain its endpoint address")
    if "private_entry" in value:
        _validate_private_entry(value)
    if "database_tls" in value:
        _validate_database_tls(value)


def _dns_name(hostname: Any, label: str) -> str:
    if (
        not isinstance(hostname, str)
        or len(hostname) > 253
        or "." not in hostname
        or not all(_DNS_LABEL.fullmatch(label) for label in hostname.split("."))
    ):
        raise StagingAttachmentError(f"{label} must be a DNS name")
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        raise StagingAttachmentError(f"{label} must not be an IP literal")
    return hostname


def _validate_private_entry(value: dict[str, Any]) -> None:
    entry = _object(value["private_entry"], {"hostname", "address"}, "private entry")
    hostname = _dns_name(entry["hostname"], "private entry hostname")
    try:
        if not isinstance(entry["address"], str):
            raise ValueError
        address = ipaddress.IPv4Address(entry["address"])
        if not any(address in subnet for subnet in _RFC1918):
            raise ValueError
    except (TypeError, ValueError):
        raise StagingAttachmentError(
            "private entry address must be an RFC1918 IPv4 literal"
        ) from None
    for endpoint, port in (
        (value["canonical"]["endpoint"], 19443),
        (value["collector"]["control_plane_url"], 18443),
    ):
        parsed = urlsplit(endpoint)
        if parsed.hostname != hostname or parsed.port != port:
            raise StagingAttachmentError(
                "private entry must match the canonical and control-plane TLS origins"
            )
    if urlsplit(value["source"]["endpoint"]).hostname == hostname:
        raise StagingAttachmentError("private entry must not override the independent spool host")
    # The Secret DSNs are not read or rewritten here. Deployment preflight must
    # separately verify the DB name from database_tls (or the legacy private
    # entry name when omitted), port 15432 and the canonical database identity.
    for name, port in (("database", 15432), ("canonical_store", 19443), ("control_plane", 18443)):
        if not any(
            item["port"] == port and address in ipaddress.ip_network(item["cidr"])
            for item in value["network"][name]
        ):
            raise StagingAttachmentError(
                f"private entry network {name} must allow its address and port"
            )


def _validate_database_tls(value: dict[str, Any]) -> None:
    if "private_entry" not in value:
        raise StagingAttachmentError("database TLS requires a private entry")
    tls = _object(value["database_tls"], {"server_name", "ca_secret"}, "database TLS")
    hostname = _dns_name(tls["server_name"], "database TLS server name")
    if hostname in (
        value["private_entry"]["hostname"],
        urlsplit(value["source"]["endpoint"]).hostname,
    ):
        raise StagingAttachmentError("database TLS server name must be distinct from HTTPS hosts")
    _secret(tls["ca_secret"], {"key"}, "database TLS CA Secret")


def _database_tls_pod(pod: dict[str, Any], value: dict[str, Any]) -> None:
    tls = value["database_tls"]
    pod["hostAliases"].append(
        {"ip": value["private_entry"]["address"], "hostnames": [tls["server_name"]]}
    )
    pod.setdefault("volumes", []).append(
        {
            "name": _DB_CA_VOLUME,
            "secret": {
                "secretName": tls["ca_secret"]["name"],
                # This is public trust material, readable by the runtime UID.
                "defaultMode": 0o444,
                "items": [{"key": tls["ca_secret"]["key"], "path": "ca.crt"}],
            },
        }
    )
    container = pod["containers"][0]
    container.setdefault("volumeMounts", []).append(
        {"name": _DB_CA_VOLUME, "mountPath": _DB_CA_DIRECTORY, "readOnly": True}
    )
    # Directory mount (never subPath) follows kubelet's atomic Secret updates.
    # libpq reads this CA for each new connection; existing TLS sessions persist.
    _set_env(container, _env("PGSSLMODE", "verify-full"))
    _set_env(container, _env("PGSSLROOTCERT", f"{_DB_CA_DIRECTORY}/ca.crt"))


def _env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


def _secret_env(name: str, reference: dict[str, Any], key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": reference["name"], "key": reference[key]}},
    }


def _set_env(container: dict[str, Any], item: dict[str, Any]) -> None:
    container["env"] = [row for row in container.get("env", []) if row["name"] != item["name"]] + [
        item
    ]


def _policy(
    namespace: str,
    name: str,
    selector: dict[str, Any],
    *,
    egress: list[dict[str, Any]],
    ingress: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"podSelector": selector, "policyTypes": ["Egress"], "egress": egress}
    if ingress is not None:
        spec.update(policyTypes=["Ingress", "Egress"], ingress=ingress)
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"namespace": namespace, "name": name},
        "spec": spec,
    }


def render_staging_attachment(
    value: dict[str, Any], *, actuator: bytes, collector: bytes
) -> dict[str, bytes]:
    """Adapt already environment-bound runtime objects without reading any Secret."""
    namespace = value["namespace"]
    canonical, source = value["canonical"], value["source"]
    gateway_name = "loom-llm-gateway"
    gateway_selector = {"matchLabels": {"app": gateway_name}}
    execution_selector = {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}}
    revision = {"loom.ca/nebius-configuration-revision": value["configuration_revision"]}
    actuator_docs = list(yaml.safe_load_all(actuator))
    for doc in actuator_docs:
        if doc["kind"] == "Deployment":
            doc["spec"]["template"]["metadata"].setdefault("annotations", {}).update(revision)
            container = doc["spec"]["template"]["spec"]["containers"][0]
            _set_env(
                container,
                _secret_env(
                    "LOOM_EXECUTION_ACTUATOR_DB_URL", canonical["db_secret"], "actuator_key"
                ),
            )
            _set_env(
                container,
                _env(
                    "LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL",
                    f"http://{gateway_name}.{namespace}.svc.cluster.local:9100/internal/service-execution",
                ),
            )
        if doc["metadata"]["name"] == "loom-execution-attempt-egress":
            doc["spec"]["egress"][-1]["to"] = [{"podSelector": gateway_selector}]
    collector_docs = list(yaml.safe_load_all(collector))
    for doc in collector_docs:
        if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
            doc["metadata"]["name"] += "-staging"
        if doc["kind"] == "ClusterRoleBinding":
            doc["roleRef"]["name"] += "-staging"
        if doc["kind"] == "ConfigMap":
            doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL"] = value["collector"][
                "control_plane_url"
            ]
        if doc["kind"] == "CronJob":
            doc["spec"]["jobTemplate"].setdefault("metadata", {}).setdefault(
                "annotations", {}
            ).update(revision)
            doc["spec"]["jobTemplate"]["spec"]["template"]["metadata"].setdefault(
                "annotations", {}
            ).update(revision)
            sources = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]["volumes"][0][
                "projected"
            ]["sources"]
            for entry, label in zip(sources, ("nebius_secret", "token_secret"), strict=True):
                reference = value["collector"][label]
                entry["secret"]["name"] = reference["name"]
                entry["secret"]["items"][0]["key"] = reference["key"]
    gateway_env = [
        _env("LOOM_ENV", "staging"),
        _env("LOOM_NAMESPACE", namespace),
        _env("LOOM_GW_MINIO_ENDPOINT", canonical["endpoint"]),
        _env("LOOM_GW_MINIO_REGION", canonical["region"]),
        _env("LOOM_GW_ARTIFACTS_BUCKET", canonical["artifacts_bucket"]),
        _secret_env("LOOM_GW_DB_URL", canonical["db_secret"], "gateway_key"),
        _secret_env("LOOM_GW_MINIO_ACCESS_KEY", canonical["storage_secret"], "access_key"),
        _secret_env("LOOM_GW_MINIO_SECRET_KEY", canonical["storage_secret"], "secret_key"),
        _secret_env("LOOM_GW_STEP_JWT_SIGNING_KEY", value["gateway_secret"], "step_jwt_key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", value["gateway_secret"], "master_key"),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT", source["endpoint"]),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_REGION", source["region"]),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_BUCKET", source["bucket"]),
        _secret_env(
            "LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
            source["credentials_secret"],
            "access_key",
        ),
        _secret_env(
            "LOOM_GW_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
            source["credentials_secret"],
            "secret_key",
        ),
    ]
    gateway_docs = [
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": gateway_name, "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": gateway_selector,
                "template": {
                    "metadata": {
                        "labels": gateway_selector["matchLabels"],
                        "annotations": revision,
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "terminationGracePeriodSeconds": 300,
                        "nodeSelector": {"loom.nebius/node-role": "system"},
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "gateway",
                                "image": value["gateway_image"],
                                "imagePullPolicy": "IfNotPresent",
                                "env": gateway_env,
                                "envFrom": [
                                    {"secretRef": {"name": value["local_providers_secret_name"]}}
                                ],
                                "ports": [{"containerPort": 9100}],
                                "readinessProbe": {"httpGet": {"path": "/healthz", "port": 9100}},
                                "lifecycle": {
                                    "preStop": {
                                        "exec": {
                                            "command": [
                                                "python",
                                                "-c",
                                                "import urllib.request\n"
                                                "try:\n"
                                                "    req = urllib.request.Request(\n"
                                                "        'http://127.0.0.1:9100/drain', method='POST', data=b''\n"
                                                "    )\n"
                                                "    urllib.request.urlopen(req, timeout=280).read()\n"
                                                "except Exception:\n"
                                                "    pass\n",
                                            ]
                                        }
                                    }
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "resources": {
                                    "requests": {"cpu": "200m", "memory": "256Mi"},
                                    "limits": {"cpu": "1", "memory": "1Gi"},
                                },
                            }
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": gateway_name, "namespace": namespace},
            "spec": {
                "selector": gateway_selector["matchLabels"],
                "ports": [{"port": 9100, "targetPort": 9100}],
            },
        },
    ]

    def destinations(*names: str) -> list[dict[str, Any]]:
        return [
            {
                "to": [{"ipBlock": {"cidr": row["cidr"]}}],
                "ports": [{"protocol": "TCP", "port": row["port"]}],
            }
            for name in names
            for row in value["network"][name]
        ]

    dns = {
        "to": [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                },
                "podSelector": {
                    "matchExpressions": [
                        {"key": "k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]}
                    ]
                },
            }
        ],
        "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
    }
    policies = [
        _policy(
            namespace,
            "loom-attachment-gateway",
            gateway_selector,
            ingress=[
                {
                    "from": [{"podSelector": execution_selector}],
                    "ports": [{"protocol": "TCP", "port": 9100}],
                }
            ],
            egress=[dns, *destinations("database", "canonical_store", "source_store", "model_api")],
        ),
        _policy(
            namespace,
            "loom-attachment-actuator",
            {"matchLabels": {"app.kubernetes.io/name": "loom-execution-actuator"}},
            ingress=[],
            egress=[dns, *destinations("database", "kubernetes_api", "kubelet")],
        ),
        _policy(
            namespace,
            "loom-attachment-collector",
            {"matchLabels": {"app.kubernetes.io/name": "loom-execution-capacity-collector"}},
            ingress=[],
            egress=[dns, *destinations("control_plane", "kubernetes_api", "provider_api")],
        ),
    ]
    if "private_entry" in value:
        entry = value["private_entry"]
        for doc in [*actuator_docs, *collector_docs, *gateway_docs]:
            if doc["kind"] == "Deployment":
                pod = doc["spec"]["template"]["spec"]
            elif doc["kind"] == "CronJob":
                pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            else:
                continue
            pod["hostAliases"] = [{"ip": entry["address"], "hostnames": [entry["hostname"]]}]
            if "database_tls" in value and doc["kind"] == "Deployment":
                _database_tls_pod(pod, value)
    return {
        name: yaml.safe_dump_all(documents, sort_keys=False).encode()
        for name, documents in (
            ("nebius-execution-actuator.yaml", actuator_docs),
            ("nebius-capacity-collector.yaml", collector_docs),
            ("nebius-staging-gateway.yaml", gateway_docs),
            ("nebius-staging-attachment-network.yaml", policies),
        )
    }
