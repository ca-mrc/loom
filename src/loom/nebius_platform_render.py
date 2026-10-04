"""Render an isolated Nebius platform from explicit, immutable inputs.

This renderer has no cloud or Kubernetes side effects. Existing execution
actuator/collector templates remain the single implementation of those pods.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime
from decimal import ROUND_CEILING, Decimal
from ipaddress import IPv4Address, ip_address, ip_network
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    nebius_cpu_execution_class,
    nebius_guest_execution_class,
)
from loom.execution_runtime_contract import (
    ExecutionResourceRequestsV1,
    TaskExecutionResourceRequestsV1,
)
from loom.nebius_guest_target import emulated_auth_target_id, guest_target_id, guest_target_ids
from loom.nebius_task_identity_policy import (
    identity_namespace_labels,
    identity_policy_documents,
    validate_identity_policy,
)

# Scheduling baseline for future automatic Nebius Terminus tasks, not task limits.
DEFAULT_TASK_RESOURCE_REQUESTS = {
    "controller": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
    "task_sandbox": {"cpu_millis": 600, "memory_mib": 1024, "ephemeral_storage_mib": 1024},
    "verifier_sandbox": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
}


class NebiusPlatformError(ValueError):
    """An input cannot describe an independent, reproducible environment."""


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(value)).hexdigest()


def _name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value
    ):
        raise NebiusPlatformError(f"{label} must be a DNS label")
    return value


def validate_environment(config: dict[str, Any]) -> None:
    managed = config.get("schema_version") == "loom.nebius-managed-environment.v1"
    if not managed and config.get("schema_version") != "loom.nebius-platform.v1":
        raise NebiusPlatformError("unsupported platform configuration")
    expected = {
        "schema_version",
        "namespace",
        "execution_namespace",
        "environment",
        "target_id",
        "cluster_scope_id",
        "region",
        "public_host",
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
        "kubernetes_api_server",
        "tls_secret_name",
        "db_tls_secret_name",
        "storage_class",
        "postgres_image",
        "backup_image",
        "postgres_storage_gi",
        "storage_endpoint",
        "buckets",
        "model_provider_base_url",
        "max_concurrent",
        "capacity_policy",
        "execution_price",
    }
    if managed:
        expected.add("registration")
    if (
        set(config)
        - {
            "public_tls_bootstrap",
            "shared_ingress_enabled",
            "execution_resource_quota",
            "regional_execution_targets",
            "public_gateway_ipv4",
            "task_image_builder",
            "service_execution_scheduler_max_deadline_sec",
            "task_resource_requests",
            "default_task_resource_requests",
            "task_egress",
            "task_identity_policy",
            "guest_execution_target",
            "emulated_auth_execution_target",
        }
        != expected
    ):
        raise NebiusPlatformError("platform configuration has missing or unknown fields")
    try:
        guest_target_ids(config)
        validate_identity_policy(config)
    except ValueError as exc:
        raise NebiusPlatformError(str(exc)) from exc
    if type(config.get("public_tls_bootstrap", False)) is not bool:
        raise NebiusPlatformError("public_tls_bootstrap must be a boolean")
    if "task_egress" in config:
        from loom_llm_gateway.task_egress import TaskEgressConfig

        egress = TaskEgressConfig.model_validate(config["task_egress"])
        protected = [ip_network(cidr) for cidr in egress.protected_cidrs]
        # Check literal addresses already known to this offline renderer. DNS
        # hosts and other platform addresses still require an operator inventory.
        hosts = [config.get("public_gateway_ipv4"),
                 urlsplit(str(config.get("kubernetes_api_server", ""))).hostname]
        for target in config.get("regional_execution_targets", []):
            hosts.append(urlsplit(str(target.get("kubernetes_api_server", ""))).hostname)
        for host in hosts:
            if host is None:
                continue
            try:
                address = ip_address(host)
            except ValueError:
                continue
            if not any(address.version == block.version and address in block for block in protected):
                raise NebiusPlatformError(
                    "task egress protected_cidrs must cover configured platform addresses"
                )
    if type(config.get("shared_ingress_enabled", False)) is not bool:
        raise NebiusPlatformError("shared_ingress_enabled must be a boolean")
    if managed and "shared_ingress_enabled" in config:
        raise NebiusPlatformError("shared_ingress_enabled belongs only to the standalone platform")
    ExecutionResourceRequestsV1.model_validate(
        config.get("default_task_resource_requests", DEFAULT_TASK_RESOURCE_REQUESTS)
    )
    requests = config.get("task_resource_requests", {})
    if not isinstance(requests, dict):
        raise NebiusPlatformError("task_resource_requests must be a task-ID keyed object")
    for task_id, entry in requests.items():
        if not isinstance(task_id, str) or not task_id.strip():
            raise NebiusPlatformError("task_resource_requests requires nonempty task IDs")
        TaskExecutionResourceRequestsV1.model_validate(entry)
    for key in (
        "namespace",
        "execution_namespace",
        "target_id",
        "cluster_scope_id",
        "storage_class",
    ):
        _name(config.get(key), key)
    if managed:
        from loom.nebius_environment_contract import EnvironmentRegistrationV1

        registration = EnvironmentRegistrationV1.model_validate(config["registration"])
        for key, expected_value in (
            ("namespace", registration.application_namespace),
            ("execution_namespace", registration.execution_namespace),
            ("environment", registration.kind),
            ("target_id", registration.target_id),
            ("cluster_id", registration.cluster_id),
            ("execution_node_group_id", registration.physical_pool_id),
            ("public_host", registration.public_host),
        ):
            if config[key] != expected_value:
                raise NebiusPlatformError(f"managed {key} differs from registered binding")
        if (
            config.get("regional_execution_targets")
            or "task_image_builder" in config
            or not isinstance(config["capacity_policy"], dict)
            or config["capacity_policy"].get("enabled") is not False
        ):
            raise NebiusPlatformError("managed execution requires shared admission; currently disabled")
    else:
        if config["namespace"] == config["execution_namespace"] or any(
            not config[key].startswith("loom-nebius-") for key in ("namespace", "execution_namespace")
        ):
            raise NebiusPlatformError("independent Nebius system and execution namespaces are required")
        if config.get("environment") != "development":
            raise NebiusPlatformError(
                "this independent integration lane requires environment=development"
            )
    host = config.get("public_host", "")
    if not isinstance(host, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+\.[a-z]{2,63}", host):
        raise NebiusPlatformError("public_host must be a DNS hostname")
    for key in (
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
    ):
        if not isinstance(config.get(key), str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", config[key]):
            raise NebiusPlatformError(f"{key} is required")
    if not config["quota_parent_id"].startswith("tenant-"):
        raise NebiusPlatformError("quota_parent_id must identify the tenant, not the project")
    api = urlsplit(str(config["kubernetes_api_server"]))
    if (
        api.scheme != "https"
        or not api.hostname
        or api.username
        or api.password
        or api.query
        or api.fragment
        or api.path not in ("", "/")
    ):
        raise NebiusPlatformError("kubernetes_api_server must be an HTTPS control plane origin")
    region = config.get("region", "")
    if not isinstance(region, str) or not re.fullmatch(r"[a-z]+-[a-z]+[0-9]", region):
        raise NebiusPlatformError("region is invalid")
    for key in ("postgres_image", "backup_image"):
        if not re.fullmatch(
            r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-zA-Z0-9_./-]+@sha256:[0-9a-f]{64}",
            str(config.get(key, "")),
        ):
            raise NebiusPlatformError(f"{key} must be mirrored and digest-pinned in Nebius")
    endpoint = urlsplit(str(config.get("storage_endpoint", "")))
    if endpoint.geturl() != f"https://storage.{region}.nebius.cloud":
        raise NebiusPlatformError(
            "storage_endpoint must be the region's native Nebius HTTPS endpoint"
        )
    buckets = config.get("buckets", {})
    if set(buckets) != {"artifacts", "trajectories", "source", "backup"}:
        raise NebiusPlatformError(
            "four independent canonical, source and backup buckets are required"
        )
    for value in buckets.values():
        _name(value, "bucket")
    if len(set(buckets.values())) != 4:
        raise NebiusPlatformError("canonical, source and backup buckets must be distinct")
    if not isinstance(config.get("postgres_storage_gi"), int) or config["postgres_storage_gi"] < 10:
        raise NebiusPlatformError("postgres_storage_gi must be at least 10")
    for key in ("tls_secret_name", "db_tls_secret_name"):
        _name(config.get(key), key)
    policy = config.get("capacity_policy", {})
    required = (
        "max_nodes",
        "max_vcpu_millis",
        "max_memory_mib",
        "max_storage_mib",
        "node_cpu_millis",
        "node_memory_mib",
        "node_storage_mib",
        "max_pending_jobs",
        "max_create_per_minute",
        "observation_max_age_seconds",
    )
    if not isinstance(policy, dict) or any(
        type(policy.get(key)) is not int or policy[key] <= 0 for key in required
    ):
        raise NebiusPlatformError("capacity_policy requires positive measured resource limits")
    if set(policy) != {
        *required,
        "enabled",
        "max_unschedulable_jobs",
        "max_image_pull_backoff_jobs",
        "reason",
    }:
        raise NebiusPlatformError("capacity policy has missing or unknown fields")
    for key in ("max_unschedulable_jobs", "max_image_pull_backoff_jobs"):
        if type(policy[key]) is not int or policy[key] < 0:
            raise NebiusPlatformError("capacity failure allowances must be nonnegative integers")
    if config.get("max_concurrent") is not None and (
        type(config["max_concurrent"]) is not int or config["max_concurrent"] < 1
    ):
        raise NebiusPlatformError("max_concurrent must be positive or null for quota-following")
    deadline = config.get("service_execution_scheduler_max_deadline_sec", 7200)
    if type(deadline) is not int or deadline <= 0:
        raise NebiusPlatformError("service_execution_scheduler_max_deadline_sec must be a positive integer")
    if policy["max_nodes"] > 100:
        raise NebiusPlatformError(
            "max_nodes exceeds the native node-group technical maximum of 100"
        )
    overrides = config.get("execution_resource_quota", {})
    if not isinstance(overrides, dict) or set(overrides) - {
        "pods",
        "requests.cpu",
        "requests.memory",
        "requests.ephemeral-storage",
    }:
        raise NebiusPlatformError(
            "execution_resource_quota supports only Pod and requested-resource limits"
        )
    for resource, value in overrides.items():
        pattern = (
            r"[0-9]+"
            if resource == "pods"
            else (
                r"[0-9]+(?:\.[0-9]+)?m?"
                if resource == "requests.cpu"
                else r"[0-9]+(?:\.[0-9]+)?(?:Ki|Mi|Gi|Ti)?"
            )
        )
        if not isinstance(value, str) or not re.fullmatch(pattern, value):
            raise NebiusPlatformError(
                "execution_resource_quota values must be nonnegative Kubernetes quantities"
            )
    if not managed and policy.get("enabled") is not True:
        raise NebiusPlatformError("capacity policy must explicitly enable the bounded target")
    price = config.get("execution_price", {})
    rate_keys = (
        "base_microusd_per_hour",
        "vcpu_microusd_per_hour",
        "memory_gib_microusd_per_hour",
        "ephemeral_storage_gib_microusd_per_hour",
    )
    price_keys = {
        "provider",
        "region",
        "sku",
        "source",
        "source_version",
        "source_uri",
        "effective_at",
        "observed_at",
        *rate_keys,
    }
    if (
        not isinstance(price, dict)
        or set(price) != price_keys
        or price["provider"] != "nebius"
        or price["region"] != region
    ):
        raise NebiusPlatformError(
            "execution price must identify this Nebius region and exact source"
        )
    if (
        any(type(price[key]) is not int or price[key] < 0 for key in rate_keys)
        or sum(price[key] for key in rate_keys) <= 0
    ):
        raise NebiusPlatformError(
            "execution price requires measured nonnegative rates, including a positive rate"
        )
    for key in ("sku", "source", "source_version", "source_uri"):
        if not isinstance(price[key], str) or not price[key].strip():
            raise NebiusPlatformError("execution price source identity is incomplete")
    for key in ("effective_at", "observed_at"):
        try:
            parsed = datetime.fromisoformat(price[key].replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError
        except (ValueError, TypeError, AttributeError) as exc:
            raise NebiusPlatformError("execution price timestamps must be timezone-aware") from exc
    provider = urlsplit(str(config.get("model_provider_base_url", "")))
    if (
        provider.scheme != "https"
        or not provider.hostname
        or provider.username
        or provider.password
        or provider.query
        or provider.fragment
    ):
        raise NebiusPlatformError("model provider must be an HTTPS URL without credentials")

    _validate_regions(config)
    if "task_image_builder" in config:
        _task_image_builder_settings(config, service_image="test.invalid/loom@sha256:" + "a" * 64)


_REGIONAL_FIELDS = {
    "target_id",
    "cluster_scope_id",
    "region",
    "execution_namespace",
    "project_id",
    "quota_parent_id",
    "cluster_id",
    "execution_node_group_id",
    "kubernetes_api_server",
    "capacity_policy",
    "execution_price",
    "service_account_ids",
}


def _regional_config(config: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    result = dict(config)
    result.pop("regional_execution_targets", None)
    result.pop("execution_resource_quota", None)
    # Native image builds remain in the primary region, even when execution
    # targets are configured elsewhere.
    result.pop("task_image_builder", None)
    result.update({key: value for key, value in target.items() if key != "service_account_ids"})
    result["storage_endpoint"] = f"https://storage.{target['region']}.nebius.cloud"
    return result


def _validate_regions(config: dict[str, Any]) -> None:
    regions = config.get("regional_execution_targets", [])
    if not isinstance(regions, list):
        raise NebiusPlatformError("regional_execution_targets must be a list")
    if regions:
        try:
            address = IPv4Address(config.get("public_gateway_ipv4", ""))
        except ValueError as exc:
            raise NebiusPlatformError(
                "regional execution requires public_gateway_ipv4 from the fixed allocation"
            ) from exc
        if not address.is_global:
            raise NebiusPlatformError("public_gateway_ipv4 must be a global IPv4 address")
    ids, scopes, namespaces = (
        {config["target_id"]},
        {config["cluster_scope_id"]},
        {config["execution_namespace"]},
    )
    for target in regions:
        if (
            not isinstance(target, dict)
            or set(target) - {"execution_resource_quota"} != _REGIONAL_FIELDS
        ):
            raise NebiusPlatformError("regional execution target has missing or unknown fields")
        validate_environment(_regional_config(config, target))
        if not target["region"].startswith("eu-") or target["region"] == config["region"]:
            raise NebiusPlatformError("regional execution targets require a distinct EU region")
        if len(target["target_id"]) > 40:
            raise NebiusPlatformError(
                "regional target_id must leave room for Kubernetes role suffixes"
            )
        for value, seen in (
            (target["target_id"], ids),
            (target["cluster_scope_id"], scopes),
            (target["execution_namespace"], namespaces),
        ):
            if value in seen:
                raise NebiusPlatformError(
                    "regional target, cluster and namespace identities must be unique"
                )
            seen.add(value)
        identities = target["service_account_ids"]
        if not isinstance(identities, dict) or set(identities) != {
            "actuator",
            "collector",
            "gateway",
        }:
            raise NebiusPlatformError(
                "regional target needs distinct actuator, collector and gateway service_account_ids"
            )
        if len(set(identities.values())) != 3 or any(
            not isinstance(value, str) or not re.fullmatch(r"serviceaccount-[a-zA-Z0-9_-]+", value)
            for value in identities.values()
        ):
            raise NebiusPlatformError(
                "regional Kubernetes identities must be distinct native service account IDs"
            )


def _obj(
    kind: str, name: str, namespace: str | None, spec: Any = None, *, api: str = "v1"
) -> dict[str, Any]:
    value: dict[str, Any] = {"apiVersion": api, "kind": kind, "metadata": {"name": name}}
    if namespace:
        value["metadata"]["namespace"] = namespace
    if spec is not None:
        value["spec"] = spec
    return value


def _env(values: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"name": key, "value": str(value)} for key, value in values.items()]


def _secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


def _mount_secret(
    pod: dict[str, Any],
    name: str,
    secret: str,
    path: str,
    *,
    mode: int = 0o440,
    ca_only: bool = False,
) -> None:
    if name == "admin":
        pod.setdefault("volumes", []).extend(
            [
                {
                    "name": "admin-source",
                    "secret": {
                        "secretName": secret,
                        "defaultMode": 0o440,
                        "items": [{"key": "secrets.toml", "path": "secrets.toml"}],
                    },
                },
                {"name": "admin-owned", "emptyDir": {"medium": "Memory"}},
            ]
        )
        pod.setdefault("initContainers", []).append(
            {
                "name": "prepare-admin-secret",
                "image": pod["containers"][0]["image"],
                "command": [
                    "python",
                    "-c",
                    "from pathlib import Path; p=Path('/var/run/loom-admin-owned/secrets.toml'); p.write_bytes(Path('/var/run/loom-admin-source/secrets.toml').read_bytes()); p.chmod(0o400)",
                ],
                "volumeMounts": [
                    {
                        "name": "admin-source",
                        "mountPath": "/var/run/loom-admin-source",
                        "readOnly": True,
                    },
                    {"name": "admin-owned", "mountPath": "/var/run/loom-admin-owned"},
                ],
                "resources": {
                    "requests": {"cpu": "10m", "memory": "32Mi"},
                    "limits": {"cpu": "100m", "memory": "64Mi"},
                },
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                },
            }
        )
        for container in pod["containers"]:
            container.setdefault("volumeMounts", []).append(
                {"name": "admin-owned", "mountPath": path, "readOnly": True}
            )
        return
    pod.setdefault("volumes", []).append(
        {"name": name, "secret": {"secretName": secret, "defaultMode": mode}}
    )
    if ca_only:
        pod["volumes"][-1]["secret"]["items"] = [{"key": "ca.crt", "path": "ca.crt"}]
    for container in pod["containers"]:
        container.setdefault("volumeMounts", []).append(
            {"name": name, "mountPath": path, "readOnly": True}
        )


def _service(name: str, ns: str, port: int, target: int | None = None) -> dict[str, Any]:
    return _obj(
        "Service",
        name,
        ns,
        {
            "selector": {"app": name},
            "ports": [
                {
                    "name": "http" if port != 5432 else "postgres",
                    "port": port,
                    "targetPort": target or port,
                }
            ],
        },
    )


def _deployment(
    name: str,
    ns: str,
    image: str,
    port: int,
    health: str,
    env: list[dict[str, Any]],
    revision: str,
    *,
    cpu: str = "100m",
    memory: str = "256Mi",
) -> dict[str, Any]:
    return _obj(
        "Deployment",
        name,
        ns,
        {
            "replicas": 1,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
            },
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {
                    "labels": {"app": name},
                    "annotations": {"loom.nebius/configuration-revision": revision},
                },
                "spec": {
                    "automountServiceAccountToken": False,
                    "serviceAccountName": "loom-platform",
                    "nodeSelector": {
                        "loom.nebius/node-role": "system",
                        "loom.nebius/platform": "integration",
                    },
                    "tolerations": [
                        {
                            "key": "loom.nebius/platform",
                            "operator": "Equal",
                            "value": "integration",
                            "effect": "NoSchedule",
                        }
                    ],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": name,
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": env,
                            "ports": [{"containerPort": port}],
                            "readinessProbe": {
                                "httpGet": {"path": health, "port": port},
                                "periodSeconds": 5,
                            },
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                        }
                    ],
                },
            },
        },
        api="apps/v1",
    )


def _namespace(name: str) -> dict[str, Any]:
    obj = _obj("Namespace", name, None)
    obj["metadata"]["labels"] = {
        "loom.nebius/platform": "true",
        "pod-security.kubernetes.io/enforce": "restricted",
    }
    return obj


def _network_policy(
    name: str,
    ns: str,
    selector: dict[str, Any],
    ingress: list[Any],
    egress: list[Any] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"podSelector": selector, "policyTypes": ["Ingress"], "ingress": ingress}
    if egress is not None:
        spec.update(policyTypes=["Ingress", "Egress"], egress=egress)
    return _obj("NetworkPolicy", name, ns, spec, api="networking.k8s.io/v1")


def _peer(ns: str, app: str | None = None) -> dict[str, Any]:
    peer: dict[str, Any] = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ns}}
    }
    if app:
        peer["podSelector"] = {"matchLabels": {"app": app}}
    return peer


def _replace_tree(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [_replace_tree(row, replacements) for row in value]
    if isinstance(value, dict):
        return {key: _replace_tree(row, replacements) for key, row in value.items()}
    return value


def public_tls_config(config: dict[str, Any]) -> dict[str, Any]:
    """Native Caddy TLS automation with a valid manual certificate for migration."""
    host, ns = config["public_host"], config["namespace"]
    api_proxy = {
        "handler": "reverse_proxy",
        "upstreams": [{"dial": f"loom-service.{ns}.svc:8090"}],
        "transport": {"protocol": "http", "read_timeout": "300s"},
        "flush_interval": -1,
        "headers": {
            "request": {
                "set": {
                    "Host": ["{http.request.host}"],
                    "X-Forwarded-Proto": ["https"],
                    "X-Forwarded-For": ["{http.request.remote.host}"],
                }
            }
        },
    }
    result: dict[str, Any] = {
        "admin": {"disabled": True},
        "storage": {"module": "file_system", "root": "/data"},
        "apps": {
            "tls": {
                # Caddyfile `tls cert key` pins certificate selection to the manual
                # certificate's tag, even after expiry. Untagged JSON loading lets
                # CertMagic select an unexpired managed certificate automatically.
                "certificates": {
                    "load_files": [
                        {
                            "certificate": "/var/run/loom-public-tls/tls.crt",
                            "key": "/var/run/loom-public-tls/tls.key",
                        }
                    ]
                },
                "automation": {
                    "policies": [
                        {
                            "subjects": [host],
                            "issuers": [
                                {
                                    "module": "acme",
                                    "ca": "https://acme-v02.api.letsencrypt.org/directory",
                                    "challenges": {
                                        "http": {"disabled": True},
                                        "tls-alpn": {"alternate_port": 8443},
                                    },
                                }
                            ],
                        }
                    ]
                },
            },
            "http": {
                "https_port": 8443,
                "servers": {
                    "public": {
                        "listen": [":8443"],
                        "protocols": ["h1", "h2"],
                        "automatic_https": {
                            "disable_redirects": True,
                            "ignore_loaded_certificates": True,
                        },
                        # Kubelet probes a Pod IP without DNS SNI; the Host header
                        # still selects the public HTTP route after the handshake.
                        "tls_connection_policies": [{"default_sni": host}],
                        "routes": [
                            {
                                "match": [{"host": [host]}],
                                "handle": [
                                    {
                                        "handler": "headers",
                                        "response": {
                                            "set": {
                                                "Strict-Transport-Security": ["max-age=31536000"]
                                            }
                                        },
                                    },
                                    {"handler": "request_body", "max_size": 100 * 1024 * 1024},
                                    {
                                        "handler": "subroute",
                                        "routes": [
                                            {
                                                "match": [{"path": ["/api/v1/*"]}],
                                                "handle": [api_proxy],
                                                "terminal": True,
                                            },
                                            {
                                                "handle": [
                                                    {
                                                        "handler": "reverse_proxy",
                                                        "upstreams": [{"dial": "127.0.0.1:8080"}],
                                                    }
                                                ]
                                            },
                                        ],
                                    },
                                ],
                            }
                        ],
                    }
                },
            },
        },
    }
    if config.get("regional_execution_targets"):
        routes = result["apps"]["http"]["servers"]["public"]["routes"][0]["handle"][-1]["routes"]
        gateway = dict(api_proxy, upstreams=[{"dial": f"loom-llm-gateway.{ns}.svc:9100"}])
        routes[0:0] = [
            {
                "match": [{"path": ["/internal/service-execution/*"]}],
                "handle": [gateway],
                "terminal": True,
            },
            {
                "match": [
                    {
                        "method": ["POST"],
                        "path": [
                            "/openai/v1/chat/completions",
                            "/openai/v1/responses",
                            "/anthropic/v1/messages",
                            "/google/v1beta/models/*",
                            "/v1/chat/completions",
                            "/v1/responses",
                            "/v1/messages",
                            "/v1beta/models/*",
                        ],
                    }
                ],
                "handle": [gateway],
                "terminal": True,
            },
            {
                "match": [
                    {
                        "path": [
                            "/admin",
                            "/admin/*",
                            "/internal/*",
                            "/openai/*",
                            "/anthropic/*",
                            "/google/*",
                            "/v1/*",
                            "/v1beta/*",
                        ]
                    }
                ],
                "handle": [{"handler": "static_response", "status_code": 404}],
                "terminal": True,
            },
        ]
    if not config.get("public_tls_bootstrap", False):
        result["apps"]["tls"].pop("certificates")
    return result


def _requested_quantity(value: str | int, resource: str) -> int:
    """Round supported template quantities up to millicores or MiB, offline."""
    text = str(value)
    if resource == "cpu":
        amount = Decimal(text[:-1]) if text.endswith("m") else Decimal(text) * 1000
    else:
        factor = Decimal(1) / (1024 * 1024)
        for unit, scale in (("Ki", 1 / 1024), ("Mi", 1), ("Gi", 1024), ("Ti", 1024 * 1024)):
            if text.endswith(unit):
                text, factor = text[: -len(unit)], Decimal(str(scale))
                break
        amount = Decimal(text) * factor
    return int(amount.to_integral_value(rounding=ROUND_CEILING))


def _execution_quota(config: dict[str, Any], documents: list[dict[str, Any]]) -> dict[str, str]:
    """Technical envelope plus the actual namespace controller/collector requests.

    Physical Pod fit and DaemonSet deductions belong to CP observations. This
    static namespace quota must not recreate the old independent twelve-Pod cap.
    """
    policy = config["capacity_policy"]
    totals = {
        "pods": policy["max_nodes"] * 64,
        "cpu": policy["max_vcpu_millis"],
        "memory": policy["max_memory_mib"],
        "ephemeral-storage": policy["max_storage_mib"],
    }

    for doc in documents:
        if doc["kind"] == "Deployment":
            pod = doc["spec"]["template"]["spec"]
            replicas = doc["spec"].get("replicas", 1)
            strategy = doc["spec"].get("strategy", {})
            surge = strategy.get("rollingUpdate", {}).get("maxSurge", "25%")
            count = replicas
            if strategy.get("type", "RollingUpdate") == "RollingUpdate":
                count += (
                    (replicas * int(surge[:-1]) + 99) // 100
                    if isinstance(surge, str) and surge.endswith("%")
                    else int(surge)
                )
        elif doc["kind"] == "CronJob":
            pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            count = 1  # The existing collector uses concurrencyPolicy=Forbid.
        else:
            continue
        totals["pods"] += count
        for resource in ("cpu", "memory", "ephemeral-storage"):

            def request(container: dict[str, Any], resource: str = resource) -> int:
                return _requested_quantity(
                    container.get("resources", {}).get("requests", {}).get(resource, 0), resource
                )

            regular = sum(request(container) for container in pod["containers"])
            init_peak = max(
                (request(container) for container in pod.get("initContainers", [])), default=0
            )
            totals[resource] += count * max(regular, init_peak)
    hard = {
        "pods": str(totals["pods"]),
        "requests.cpu": f"{totals['cpu']}m",
        "requests.memory": f"{totals['memory']}Mi",
        "requests.ephemeral-storage": f"{totals['ephemeral-storage']}Mi",
    }
    hard.update(config.get("execution_resource_quota", {}))
    return hard


def _task_image_builder_settings(config: dict[str, Any], *, service_image: str) -> Any:
    from loom_execution_actuator.task_image_settings import NativeTaskImageSettings

    supplied = config.get("task_image_builder")
    if supplied is None:
        return None
    allowed = {
        "registry_repository",
        "cache_bucket",
        "cpu_millis",
        "memory_mib",
        "ephemeral_storage_mib",
        "max_processes",
        "active_deadline_seconds",
        "max_concurrent",
        "snapshotter",
        "compatible_revision_cache",
        "export_cache_mode",
        "cache_transfer",
        "oci_export_format",
    }
    if (
        not isinstance(supplied, dict)
        or set(supplied) - allowed
        or not supplied.get("registry_repository")
    ):
        raise NebiusPlatformError(
            "task_image_builder requires a repository and supported build limits"
        )
    repository = supplied["registry_repository"]
    if not isinstance(repository, str) or not re.fullmatch(
        r"cr\.eu-north1\.nebius\.cloud/[a-z0-9][a-z0-9/_.-]{1,160}/task-images", repository
    ):
        raise NebiusPlatformError(
            "task image publication requires a dedicated primary Nebius task-images repository"
        )
    cache = supplied.get("cache_bucket")
    if cache is not None:
        _name(cache, "task image cache bucket")
        if cache in {
            config["buckets"]["source"],
            config["buckets"]["backup"],
            config["buckets"]["trajectories"],
        }:
            raise NebiusPlatformError(
                "build cache must not share source, backup or trajectory storage"
            )
    try:
        return NativeTaskImageSettings(
            namespace=config["execution_namespace"] + "-build",
            service_image=service_image,
            storage_endpoint=config["storage_endpoint"],
            storage_region=config["region"],
            # Ordinary catalog/task uploads live in the canonical artifacts
            # bucket. The separate source bucket stores execution input packs.
            source_bucket=config["buckets"]["artifacts"],
            cache_secret_name="loom-task-build-cache" if cache else None,
            **{**supplied, "cache_bucket": cache},
        )
    except ValueError:
        raise NebiusPlatformError("invalid task image build resource limits or namespace") from None


def _task_image_builder_documents(config: dict[str, Any], settings: Any) -> list[dict[str, Any]]:
    ns = settings.namespace
    namespace = _namespace(ns)
    # Rootless BuildKit requires user-namespace helpers and an unconfined
    # profile. The ordinary execution namespace remains restricted.
    namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    quota = _obj("ResourceQuota", "loom-task-image-build-capacity", ns)
    count = settings.max_concurrent
    quota["spec"] = {
        "hard": {
            "pods": str(count),
            "count/jobs.batch": str(count),
            # Kubernetes creates kube-root-ca.crt in every namespace.
            "count/configmaps": str(count + 1),
            "requests.cpu": f"{settings.cpu_millis * count}m",
            "requests.memory": f"{settings.memory_mib * count}Mi",
            "requests.ephemeral-storage": f"{settings.ephemeral_storage_mib * count}Mi",
            "limits.cpu": f"{settings.cpu_millis * count}m",
            "limits.memory": f"{settings.memory_mib * count}Mi",
            "limits.ephemeral-storage": f"{settings.ephemeral_storage_mib * count}Mi",
        }
    }
    account = _obj("ServiceAccount", "loom-execution-attempt", ns)
    account["automountServiceAccountToken"] = False
    role = _obj("Role", "loom-task-image-builder", ns, api="rbac.authorization.k8s.io/v1")
    role["rules"] = [
        {
            "apiGroups": ["batch"],
            "resources": ["jobs"],
            "verbs": ["create", "get", "list", "delete"],
        },
        {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["create", "get", "delete"]},
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "delete"]},
        {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
    ]
    binding = _obj("RoleBinding", "loom-task-image-builder", ns, api="rbac.authorization.k8s.io/v1")
    binding["roleRef"] = {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "Role",
        "name": role["metadata"]["name"],
    }
    binding["subjects"] = [
        {
            "kind": "ServiceAccount",
            "name": "loom-execution-actuator",
            "namespace": config["execution_namespace"],
        }
    ]
    network = _obj("NetworkPolicy", "loom-task-image-builder", ns, api="networking.k8s.io/v1")
    network["spec"] = {
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress"],
        "ingress": [],
        "egress": [
            {
                "to": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                        },
                        "podSelector": {
                            "matchExpressions": [
                                {
                                    "key": "k8s-app",
                                    "operator": "In",
                                    "values": ["kube-dns", "coredns"],
                                }
                            ]
                        },
                    }
                ],
                "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
            },
            {
                "to": [
                    {
                        "ipBlock": {
                            "cidr": "0.0.0.0/0",
                            "except": [
                                "0.0.0.0/8",
                                "10.0.0.0/8",
                                "100.64.0.0/10",
                                "127.0.0.0/8",
                                "169.254.0.0/16",
                                "172.16.0.0/12",
                                "192.168.0.0/16",
                                "224.0.0.0/4",
                                "240.0.0.0/4",
                            ],
                        }
                    }
                ],
                "ports": [{"protocol": "TCP", "port": 80}, {"protocol": "TCP", "port": 443}],
            },
        ],
    }
    return [namespace, quota, account, role, binding, network]


def _execution_documents(
    config: dict[str, Any], images: dict[str, str], repo_root: Path
) -> list[dict[str, Any]]:
    ns, ex = config["namespace"], config["execution_namespace"]
    builder = _task_image_builder_settings(config, service_image=images["service"])
    # Reuse the existing least-privilege execution pod and collector definitions.
    execution_docs = []
    replacements = {
        "loom-nebius-development": ex,
        "nebius-eu-north1-development": config["target_id"],
        ".loom.svc.cluster.local": f".{ns}.svc.cluster.local",
    }
    for filename in ("nebius-execution-actuator.yaml", "nebius-capacity-collector.yaml"):
        docs = list(yaml.safe_load_all((repo_root / "deploy/k8s" / filename).read_text()))
        for doc in docs:
            if not doc or doc["kind"] in {"Namespace", "PodDisruptionBudget"}:
                continue
            doc = _replace_tree(doc, replacements)
            if doc["kind"] == "ClusterRole" and filename == "nebius-capacity-collector.yaml":
                doc["rules"].append(
                    {"apiGroups": ["apps"], "resources": ["daemonsets"], "verbs": ["get", "list"]}
                )
            if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                role_name = ex + ("-collector" if filename == "nebius-capacity-collector.yaml" else "-actuator-usage")
                doc["metadata"]["name"] = role_name
                if doc["kind"] == "ClusterRoleBinding":
                    doc["roleRef"]["name"] = role_name
            if doc["kind"] == "NetworkPolicy":
                for rule in doc["spec"].get("egress", []):
                    for peer in rule.get("to", []):
                        labels = peer.get("namespaceSelector", {}).get("matchLabels", {})
                        if labels.get("kubernetes.io/metadata.name") == "loom":
                            labels["kubernetes.io/metadata.name"] = ns
            if doc["kind"] == "ConfigMap":
                if filename == "nebius-capacity-collector.yaml" and builder is not None:
                    doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_BUILD_CONCURRENCY_LIMIT"] = str(
                        builder.max_concurrent
                    )
                for suffix, key in (
                    ("NEBIUS_PROJECT_ID", "project_id"),
                    ("NEBIUS_QUOTA_PARENT_ID", "quota_parent_id"),
                    ("NEBIUS_NODE_GROUP_ID", "execution_node_group_id"),
                    ("NEBIUS_REGION", "region"),
                ):
                    doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_" + suffix] = config[key]
            pod = None
            if doc["kind"] == "Deployment":
                pod = doc["spec"]["template"]["spec"]
                if builder is not None:
                    pod["containers"][0].setdefault("env", []).extend(
                        _env(
                            {
                                "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER": builder.model_dump_json(),
                            }
                        )
                    )
                _mount_secret(
                    pod, "db-ca", "loom-execution-actuator-db", "/var/run/loom-db", ca_only=True
                )
            elif doc["kind"] == "CronJob":
                pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            if pod is not None:
                pod["nodeSelector"] = {
                    "loom.nebius/node-role": "system",
                    "loom.nebius/platform": "integration",
                }
                pod["tolerations"] = [
                    {
                        "key": "loom.nebius/platform",
                        "operator": "Equal",
                        "value": "integration",
                        "effect": "NoSchedule",
                    }
                ]
                for container in pod.get("initContainers", []) + pod["containers"]:
                    container["image"] = images["execution_actuator"]
                    for env_row in container.get("env", []):
                        if env_row["name"] == "LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR":
                            env_row["value"] = json.dumps(
                                {
                                    "loom.nebius/node-role": "integration-execution",
                                    "loom.nebius/platform": "integration",
                                }
                            )
                        elif env_row["name"] == "LOOM_EXECUTION_ACTUATOR_TOLERATIONS":
                            env_row["value"] = json.dumps(
                                [
                                    {
                                        "key": "loom.nebius/execution",
                                        "operator": "Equal",
                                        "value": "true",
                                        "effect": "NoSchedule",
                                    },
                                    {
                                        "key": "loom.nebius/platform",
                                        "operator": "Equal",
                                        "value": "integration",
                                        "effect": "NoSchedule",
                                    },
                                ]
                            )
                pod.setdefault("securityContext", {}).update(
                    runAsUser=65532, runAsGroup=65532, fsGroup=65532
                )
            if doc["kind"] == "ConfigMap":
                doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR"] = (
                    "loom.nebius/node-role=integration-execution,loom.nebius/platform=integration"
                )
            execution_docs.append(doc)
    for doc in execution_docs:
        if doc["kind"] == "ResourceQuota":
            doc["spec"]["hard"] = _execution_quota(config, execution_docs)
    if builder is not None:
        execution_docs.extend(_task_image_builder_documents(config, builder))
    for guest_id in guest_target_ids(config):
        # Reuse namespace RBAC and network policy, with independent controller
        # health/lease identity. The physical collector and native builder stay
        # on the ordinary owner; cloning either would duplicate its authority.
        guest = deepcopy(next(doc for doc in execution_docs if doc["kind"] == "Deployment"
                              and doc["metadata"]["name"] == "loom-execution-actuator"))
        guest_name = guest_id + "-actuator"
        guest["metadata"]["name"] = guest_name
        guest["metadata"]["labels"]["app.kubernetes.io/name"] = guest_name
        guest["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"] = guest_name
        guest["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/name"] = guest_name
        pod = guest["spec"]["template"]["spec"]
        if "affinity" in pod:
            pod["affinity"] = _replace_tree(pod["affinity"], {"loom-execution-actuator": guest_name})
        for container in pod["containers"]:
            container["env"] = [entry for entry in container["env"]
                                if entry["name"] != "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]
            for entry in container["env"]:
                if entry["name"] == "LOOM_EXECUTION_ACTUATOR_TARGET_ID":
                    entry["value"] = guest_id
        execution_docs.append(guest)
    return execution_docs


def _regional_documents(
    config: dict[str, Any], target: dict[str, Any], images: dict[str, str], repo_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    regional = _regional_config(config, target)
    primary: list[dict[str, Any]] = []
    remote = [_namespace(target["execution_namespace"])]
    ns, tid = config["execution_namespace"], target["target_id"]
    identities = target["service_account_ids"]
    for doc in _execution_documents(regional, images, repo_root):
        kind, name = doc["kind"], doc["metadata"]["name"]
        if kind in {"Deployment", "CronJob", "ConfigMap"} or (
            kind == "ServiceAccount" and name != "loom-execution-attempt"
        ):
            role = "actuator" if "actuator" in name else "collector"
            # Rename Kubernetes identities, never arbitrary strings such as
            # published image repositories, commands or configuration values.
            regional_name = tid + "-" + role
            doc["metadata"]["name"] = regional_name
            doc["metadata"]["namespace"] = ns
            if "app.kubernetes.io/name" in doc["metadata"].get("labels", {}):
                doc["metadata"]["labels"]["app.kubernetes.io/name"] = regional_name
            pod = None
            if kind == "Deployment":
                template = doc["spec"]["template"]
                pod = template["spec"]
                doc["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"] = regional_name
                anti_affinity = pod.get("affinity", {}).get("podAntiAffinity", {})
                for preference in anti_affinity.get(
                    "preferredDuringSchedulingIgnoredDuringExecution", []
                ):
                    labels = (
                        preference["podAffinityTerm"]
                        .get("labelSelector", {})
                        .get("matchLabels", {})
                    )
                    if labels.get("app.kubernetes.io/name") == name:
                        labels["app.kubernetes.io/name"] = regional_name
            elif kind == "CronJob":
                template = doc["spec"]["jobTemplate"]["spec"]["template"]
                pod = template["spec"]
                # Reuse the existing CP token and give only the native cloud
                # credential reference the regional Secret name.
                for volume in pod["volumes"]:
                    for source in volume.get("projected", {}).get("sources", []):
                        secret = source.get("secret", {})
                        if secret.get("name") == "loom-execution-capacity-collector-nebius":
                            secret["name"] = tid + "-collector-kubernetes"
            if pod is not None:
                template["metadata"]["labels"]["app.kubernetes.io/name"] = regional_name
                pod["serviceAccountName"] = regional_name
                for container in pod.get("initContainers", []) + pod["containers"]:
                    for source in container.get("envFrom", []):
                        ref = source.get("configMapRef", {})
                        if ref.get("name") == "loom-execution-capacity-collector":
                            ref["name"] = tid + "-collector"
                _mount_secret(
                    pod,
                    "remote-kubernetes",
                    tid + "-" + role + "-kubernetes",
                    "/var/run/loom-remote-kubernetes",
                )
                pod["volumes"][-1]["secret"]["items"] = [
                    {"key": name, "path": name} for name in ("ca.crt", "credentials.json")
                ]
                prefix = (
                    "LOOM_EXECUTION_ACTUATOR_"
                    if role == "actuator"
                    else "LOOM_EXECUTION_CAPACITY_COLLECTOR_"
                )
                container = pod["containers"][0]
                container.setdefault("env", []).extend(
                    _env(
                        {
                            prefix + "KUBERNETES_ENDPOINT": target["kubernetes_api_server"],
                            prefix + "KUBERNETES_CA_FILE": "/var/run/loom-remote-kubernetes/ca.crt",
                            prefix
                            + "KUBERNETES_NEBIUS_CREDENTIALS_FILE": "/var/run/loom-remote-kubernetes/credentials.json",
                        }
                    )
                )
                if role == "actuator":
                    for row in container["env"]:
                        if row["name"] == prefix + "CREDENTIAL_BROKER_URL":
                            row["value"] = (
                                f"https://{config['public_host']}/internal/service-execution"
                            )
                    container["env"].extend(
                        _env({prefix + "POD_IDENTITY_AUDIENCE": "loom-execution"})
                    )
            primary.append(doc)
        elif kind in {"RoleBinding", "ClusterRoleBinding"}:
            role = "actuator" if kind == "RoleBinding" or name.endswith("-actuator-usage") else "collector"
            doc["subjects"] = [
                {"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": identities[role]}
            ]
            remote.append(doc)
        elif kind == "ResourceQuota":
            doc["spec"]["hard"] = _execution_quota(regional, [])
            remote.append(doc)
        elif kind == "NetworkPolicy":
            # Remote tasks use the public TLS Gateway, never a primary-cluster Pod IP.
            if name.endswith("-egress"):
                doc["spec"]["egress"] = [
                    doc["spec"]["egress"][0],
                    {
                        "to": [{"ipBlock": {"cidr": str(config["public_gateway_ipv4"]) + "/32"}}],
                        "ports": [{"protocol": "TCP", "port": 443}],
                    },
                ]
            remote.append(doc)
        else:
            remote.append(doc)
    gateway_role = _obj(
        "ClusterRole", tid + "-gateway-tokenreview", None, api="rbac.authorization.k8s.io/v1"
    )
    gateway_role["rules"] = [
        {"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]}
    ]
    binding = _obj(
        "ClusterRoleBinding",
        gateway_role["metadata"]["name"],
        None,
        api="rbac.authorization.k8s.io/v1",
    )
    binding["roleRef"] = {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": gateway_role["metadata"]["name"],
    }
    binding["subjects"] = [
        {"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": identities["gateway"]}
    ]
    remote.extend([gateway_role, binding])
    return primary, remote


def build_regional_execution(
    config: dict[str, Any], candidate: dict[str, Any], *, repo_root: Path
) -> dict[str, list[dict[str, Any]]]:
    """Separate cluster-bound manifests; never add these to primary deploy files."""
    validate_environment(config)
    images = {key: value["image_ref"] for key, value in candidate["images"].items()}
    return {
        target["target_id"]: _regional_documents(config, target, images, repo_root)[1]
        for target in config.get("regional_execution_targets", [])
    }


def build_platform(
    config: dict[str, Any],
    candidate: dict[str, Any],
    profile: dict[str, Any],
    keyring: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, list[dict[str, Any]]]:
    """Build Kubernetes resources from published image refs and environment settings."""
    if config.get("schema_version") == "loom.nebius-managed-environment.v1":
        raise NebiusPlatformError("use render_environment for managed children")
    return _build_platform(config, candidate, profile, keyring, repo_root=repo_root)


def _build_platform(
    config: dict[str, Any],
    candidate: dict[str, Any],
    profile: dict[str, Any],
    keyring: dict[str, Any],
    *,
    repo_root: Path,
    execution_enabled: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    """Shared templates; only closed renderers may omit execution components.

    They retain the published profile unchanged and own the final zero-Pod,
    scheduler and service-mode restrictions. The public standalone renderer
    always keeps execution validation and enforcement enabled.
    """
    if not execution_enabled and ("task_identity_policy" in config or config.get("regional_execution_targets")):
        raise NebiusPlatformError("non-executing templates cannot inherit execution authority")
    validate_environment(config)
    guest_id = guest_target_id(config)
    auth_id = emulated_auth_target_id(config)
    if profile.get("guest_runtime") not in (None, "qemu-tcg-v1"):
        raise NebiusPlatformError("unsupported guest runtime")
    if profile.get("guest_runtime") is not None and guest_id is None:
        raise NebiusPlatformError("guest runtime readiness requires a distinct guest execution target")
    if guest_id is not None and (not execution_enabled or not validate_identity_policy(config)):
        raise NebiusPlatformError("guest execution requires the constrained private-root namespace policy")
    if profile.get("supports_emulated_pkcs11", False) and auth_id is None:
        raise NebiusPlatformError("emulated authentication readiness requires its distinct guest target")
    if auth_id is not None and profile.get("guest_runtime") != "qemu-tcg-v1":
        raise NebiusPlatformError("emulated authentication requires the explicit guest runtime")
    for capability in ("supports_task_web_egress", "service_lifecycle_ready", "supports_task_identity",
                       "supports_emulated_pkcs11", "supports_task_artifact_inputs"):
        if type(profile.get(capability, False)) is not bool:
            raise NebiusPlatformError(f"runtime profile {capability} must be a boolean")
    if profile.get("supports_task_web_egress", False) != ("task_egress" in config):
        raise NebiusPlatformError(
            "task egress configuration and runtime profile readiness must agree"
        )
    execution_class = nebius_cpu_execution_class(
        supports_task_web_egress=profile.get("supports_task_web_egress", False),
    )
    # Management-only rendering also reuses these templates with a minimal
    # non-execution profile. Keep its existing CPU default; egress is explicit.
    if profile.get("execution_class_id", NEBIUS_CPU_EXECUTION_CLASS_V1.class_id) != execution_class.class_id:
        raise NebiusPlatformError(
            "runtime profile execution class must match its task egress capability; "
            "new capabilities require distinct class and target identities"
        )
    if execution_enabled and profile.get("supports_task_identity", False) and not validate_identity_policy(config):
        raise NebiusPlatformError(
            "task identity readiness requires a qualified policy; execution namespaces remain restricted"
        )
    # Keep the legacy environment template for older profiles. New node-share
    # profiles resolve their effective budget only after selecting a target.
    default_requests = ExecutionResourceRequestsV1.model_validate(
        config.get("default_task_resource_requests", DEFAULT_TASK_RESOURCE_REQUESTS)
    ).model_dump(mode="json")
    config = {**config, "default_task_resource_requests": default_requests}
    if profile.get("resource_allocation_policy") == "node-share-v1":
        profile = {key: value for key, value in profile.items() if key != "default_task_resource_requests"}
    else:
        profile = {**profile, "default_task_resource_requests": default_requests}
    # Exact task/revision entries remain higher-priority overrides.
    if "task_resource_requests" in config:
        profile = {**profile, "task_resource_requests": config["task_resource_requests"]}
    if (
        candidate.get("source_ref") not in {"refs/heads/dev", "refs/heads/codex/nebius-main"}
        or candidate.get("repository") != "qianyi-sun/loom"
    ):
        raise NebiusPlatformError("candidate must originate from the Nebius integration branch")
    if profile.get("candidate_sha") != candidate.get("candidate_sha"):
        raise NebiusPlatformError("runtime profile does not match the candidate")
    images = {key: value["image_ref"] for key, value in candidate["images"].items()}
    if (
        profile.get("task_image_ref") != images["service"]
        or profile.get("runtime_image_ref") != images["execution_runtime"]
        or profile.get("agent_image_ref") != images.get("harbor_runtime", images.get("worker"))
    ):
        raise NebiusPlatformError("execution images do not match the deployed candidate")
    ns, ex = config["namespace"], config["execution_namespace"]
    # One rollout fingerprint triggers config-only Pod updates and distinct Jobs.
    # It is an identifier, not an integrity or deployment admission check.
    revision = digest(
        {"config": config, "candidate": candidate, "profile": profile, "keyring": keyring}
    )
    short = revision.removeprefix("sha256:")[:12]
    files: dict[str, list[dict[str, Any]]] = {}
    files["00-namespaces.yaml"] = [_namespace(ns), _namespace(ex)]
    if validate_identity_policy(config):
        files["00-namespaces.yaml"][1]["metadata"]["labels"].update(identity_namespace_labels())
        files["00-task-identity-policy.yaml"] = identity_policy_documents(
            ex, config["target_id"], guest_target_id=guest_id, emulated_auth_target_id=auth_id,
        )
    db_host = f"loom-postgres.{ns}.svc"
    cm = _obj("ConfigMap", "loom-platform-config", ns)
    target = {
        "schema_version": "loom.execution-target.v1",
        "target_id": config["target_id"],
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": execution_class.class_id,
        "cluster_scope_id": config["cluster_scope_id"],
        "environment": config["environment"],
        "provider": "nebius",
        "region": config["region"],
        "failure_domain": config["cluster_scope_id"],
        "data_residency": "eu",
        "namespace_name": ex,
        "health_role": "primary",
        "health_check_id": config["target_id"],
        "health_check_interval_seconds": 30,
        "health_stale_after_seconds": 90,
    }
    catalog = {
        "execution_class": execution_class.model_dump(mode="json"),
        "topology": {
            "schema_version": "loom.execution-topology.v1",
            "logical_pool_id": "nebius-cpu",
            "execution_class_id": execution_class.class_id,
            "placement_policy": "environment-local-health-first",
            "targets": [target],
        },
    }
    for regional in config.get("regional_execution_targets", []):
        catalog["topology"]["targets"].append(
            dict(
                target,
                target_id=regional["target_id"],
                cluster_scope_id=regional["cluster_scope_id"],
                region=regional["region"],
                failure_domain=regional["cluster_scope_id"],
                namespace_name=regional["execution_namespace"],
                health_role="secondary",
                health_check_id=regional["target_id"],
                pod_identity_audience="loom-execution",
                service_account_name="loom-execution-attempt",
            )
        )
    cm["data"] = {
        "profile.json": canonical(profile).decode(),
        "keyring.json": canonical(keyring).decode(),
        "environment.json": canonical(config).decode(),
        "catalog.json": canonical(catalog).decode(),
        "public-tls.json": canonical(public_tls_config(config)).decode(),
    }
    for sibling_id, filename, auth in (
        (guest_id, "guest-catalog.json", False), (auth_id, "emulated-auth-catalog.json", True),
    ):
        if sibling_id is None:
            continue
        guest_class = nebius_guest_execution_class(
            supports_task_web_egress=profile.get("supports_task_web_egress", False),
            supports_emulated_pkcs11=auth,
        )
        guest_target = dict(target, target_id=sibling_id, execution_class_id=guest_class.class_id,
                            capacity_owner_target_id=config["target_id"], health_check_id=sibling_id)
        cm["data"][filename] = canonical({
            "execution_class": guest_class.model_dump(mode="json"),
            "topology": {**catalog["topology"], "execution_class_id": guest_class.class_id,
                         "targets": [guest_target]},
        }).decode()
    if "task_egress" in config:
        cm["data"]["task-egress.json"] = canonical(config["task_egress"]).decode()
    private_ingress = [
        {"from": [_peer(ns), _peer(ex)], "ports": [{"protocol": "TCP", "port": port}]}
        for port in (8080, 8090, 9100)
    ]
    platform_account = _obj("ServiceAccount", "loom-platform", ns)
    platform_account["automountServiceAccountToken"] = False
    files["10-config-network.yaml"] = [
        cm,
        platform_account,
        _network_policy("default-deny-ingress", ns, {}, []),
        _network_policy(
            "platform-internal",
            ns,
            {
                "matchExpressions": [
                    {
                        "key": "app",
                        "operator": "In",
                        "values": ["loom-service", "loom-control-plane", "loom-llm-gateway"],
                    }
                ]
            },
            private_ingress,
        ),
        _network_policy(
            "postgres-private",
            ns,
            {"matchLabels": {"app": "loom-postgres"}},
            [
                {
                    "from": [
                        _peer(ns),
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": ex}
                            },
                            "podSelector": {
                                "matchLabels": {"app.kubernetes.io/name": "loom-execution-actuator"}
                            },
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": 5432}],
                }
            ],
        ),
        _network_policy(
            "public-web",
            ns,
            {"matchLabels": {"app": "loom-web"}},
            [{"ports": [{"protocol": "TCP", "port": 8443}]}],
        ),
    ]
    postgres = _deployment(
        "loom-postgres",
        ns,
        config["postgres_image"],
        5432,
        "/",
        [
            _secret_env("POSTGRES_PASSWORD", "loom-platform-db", "postgres-password"),
            {"name": "POSTGRES_DB", "value": "loom"},
            {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
        ],
        revision,
        cpu="100m",
    )
    postgres["kind"] = "StatefulSet"
    postgres["spec"].pop("strategy")
    postgres["spec"]["serviceName"] = "loom-postgres"
    postgres["spec"]["persistentVolumeClaimRetentionPolicy"] = {
        "whenDeleted": "Retain",
        "whenScaled": "Retain",
    }
    postgres["spec"]["volumeClaimTemplates"] = [
        {
            "metadata": {"name": "data"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": config["storage_class"],
                "resources": {"requests": {"storage": f"{config['postgres_storage_gi']}Gi"}},
            },
        }
    ]
    pgpod = postgres["spec"]["template"]["spec"]
    pgpod["securityContext"].update(runAsUser=999, runAsGroup=999, fsGroup=999)
    pg = pgpod["containers"][0]
    pg["args"] = [
        "-c",
        "ssl=on",
        "-c",
        "ssl_cert_file=/var/run/loom-db-tls/tls.crt",
        "-c",
        "ssl_key_file=/var/run/loom-db-tls/tls.key",
    ]
    pg["readinessProbe"] = {
        "exec": {"command": ["pg_isready", "-U", "postgres", "-d", "loom"]},
        "periodSeconds": 5,
    }
    pg["volumeMounts"] = [{"name": "data", "mountPath": "/var/lib/postgresql/data"}]
    _mount_secret(pgpod, "db-tls", config["db_tls_secret_name"], "/var/run/loom-db-tls")
    files["20-database.yaml"] = [_service("loom-postgres", ns, 5432), postgres]

    def job(name: str, phase: str) -> dict[str, Any]:
        deployment = _deployment(
            name,
            ns,
            images["service"],
            8090,
            "/",
            _env(
                {
                    "LOOM_ENV": config["environment"],
                    "LOOM_NAMESPACE": ns,
                    "LOOM_PLATFORM_CONFIG": "/var/run/loom-platform/environment.json",
                }
            ),
            revision,
        )
        pod = deployment["spec"]["template"]["spec"]
        container = pod["containers"][0]
        container.pop("readinessProbe")
        container["command"] = ["python", "-m", "loom.nebius_platform_bootstrap", phase]
        container["env"].extend(
            [
                _secret_env("LOOM_DB_URL", "loom-platform-db", "admin-url"),
                _secret_env("LOOM_COLLECTOR_TOKEN", "loom-platform-collector", "token"),
            ]
        )
        if phase == "database":
            container["env"].append(
                _secret_env("LOOM_BATCH_RUNNER_TOKEN", "loom-platform-batch-runner", "token")
            )
        for component in ("service", "control-plane", "gateway", "actuator"):
            container["env"].append(
                _secret_env(
                    "LOOM_DB_" + component.upper().replace("-", "_") + "_PASSWORD",
                    "loom-platform-db",
                    component + "-password",
                )
            )
        pod["volumes"] = [
            {"name": "platform-config", "configMap": {"name": "loom-platform-config"}}
        ]
        container["volumeMounts"] = [
            {"name": "platform-config", "mountPath": "/var/run/loom-platform", "readOnly": True}
        ]
        _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
        _mount_secret(pod, "admin", "loom-admin-secret", "/var/run/loom/admin")
        pod["restartPolicy"] = "Never"
        return _obj(
            "Job",
            name,
            ns,
            {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 600,
                "template": deployment["spec"]["template"],
            },
            api="batch/v1",
        )

    files["30-migrate.yaml"] = [job(f"loom-platform-migrate-{short}", "database")]
    app_docs = []
    for component, name, prefix, port, health in (
        ("service", "loom-service", "LOOM_SVC", 8090, "/api/v1/health"),
        ("control_plane", "loom-control-plane", "LOOM_CP", 8080, "/healthz"),
        ("gateway", "loom-llm-gateway", "LOOM_GW", 9100, "/healthz"),
    ):
        env = _env(
            {
                "LOOM_ENV": config["environment"],
                "LOOM_NAMESPACE": ns,
                f"{prefix}_BIND_HOST": "0.0.0.0",
                f"{prefix}_BIND_PORT": port,
                f"{prefix}_ADMIN_SECRET_FILE": "/var/run/loom/admin/secrets.toml",
                f"{prefix}_MINIO_ENDPOINT": config["storage_endpoint"],
                f"{prefix}_MINIO_REGION": config["region"],
                f"{prefix}_STORAGE_BACKEND": "minio",
                f"{prefix}_STORAGE_AUTH_KIND": "static_keys",
                f"{prefix}_ARTIFACTS_BUCKET": config["buckets"]["artifacts"],
                f"{prefix}_TRAJECTORIES_BUCKET": config["buckets"]["trajectories"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_ENDPOINT": config["storage_endpoint"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_REGION": config["region"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_BUCKET": config["buckets"]["source"],
                f"{prefix}_DB_POOL_SIZE": 5,
                f"{prefix}_DB_MAX_OVERFLOW": 5,
            }
        )
        env += [
            _secret_env(
                f"{prefix}_DB_URL", "loom-platform-db", component.replace("_", "-") + "-url"
            ),
            _secret_env(f"{prefix}_MINIO_ACCESS_KEY", "loom-platform-storage", "access-key"),
            _secret_env(f"{prefix}_MINIO_SECRET_KEY", "loom-platform-storage", "secret-key"),
            _secret_env(
                f"{prefix}_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
                "loom-platform-storage",
                "source-access-key",
            ),
            _secret_env(
                f"{prefix}_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
                "loom-platform-storage",
                "source-secret-key",
            ),
            _secret_env(f"{prefix}_STEP_JWT_SIGNING_KEY", "loom-platform-auth", "jwt-signing-key"),
            _secret_env(
                "LOOM_SECRET_STORE_MASTER_KEY", "loom-platform-auth", "secret-store-master-key"
            ),
        ]
        if component == "control_plane":
            env += _env(
                {
                    "LOOM_CP_LLM_GATEWAY_URL": f"http://loom-llm-gateway.{ns}.svc:9100",
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED": "true",
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT": config["environment"],
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID": "nebius-cpu",
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_MAX_DEADLINE_SEC": config.get(
                        "service_execution_scheduler_max_deadline_sec", 7200
                    ),
                    "LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED": "true",
                    "LOOM_CP_SERVICE_EXECUTION_SOURCE_RETENTION_SEC": 86400,
                    "LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON": canonical(
                        keyring
                    ).decode(),
                }
            )
        elif component == "service":
            env.append(
                _secret_env("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "loom-platform-batch-runner", "token")
            )
            env += _env(
                {
                    "LOOM_SVC_CONTROL_PLANE_URL": f"http://loom-control-plane.{ns}.svc:8080",
                    "LOOM_SVC_GATEWAY_URL": f"http://loom-llm-gateway.{ns}.svc:9100",
                    "LOOM_SVC_PUBLIC_BASE_URL": "https://" + config["public_host"],
                    "LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON": canonical(profile).decode(),
                    "LOOM_SVC_TEAM_REGISTRATION_OPEN": "false",
                }
            )
        else:
            env += _env({"LOOM_GW_LOCAL_YIBU_BASE_URL": config["model_provider_base_url"]})
            env += [_secret_env("LOOM_GW_LOCAL_YIBU_API_KEY", "loom-model-provider", "api-key")]
        # Settings are service-specific. Do not ship ignored source/JWT secrets
        # to the public API or imply that unsupported pool knobs are active.
        if component == "service":
            env = [
                row
                for row in env
                if not row["name"].startswith("LOOM_SVC_SERVICE_EXECUTION_SOURCE_")
                and row["name"]
                not in {
                    "LOOM_SVC_STEP_JWT_SIGNING_KEY",
                    "LOOM_SVC_DB_POOL_SIZE",
                    "LOOM_SVC_DB_MAX_OVERFLOW",
                }
            ]
        elif component == "gateway":
            env = [
                row
                for row in env
                if row["name"]
                not in {
                    "LOOM_GW_TRAJECTORIES_BUCKET",
                    "LOOM_GW_DB_POOL_SIZE",
                    "LOOM_GW_DB_MAX_OVERFLOW",
                }
            ]
        deployment = _deployment(name, ns, images[component], port, health, env, revision)
        pod = deployment["spec"]["template"]["spec"]
        _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
        _mount_secret(pod, "admin", "loom-admin-secret", "/var/run/loom/admin")
        if component == "gateway":
            pod["terminationGracePeriodSeconds"] = 300
            if "task_egress" in config:
                pod.setdefault("volumes", []).append({
                    "name": "task-egress-config",
                    "configMap": {"name": "loom-platform-config", "items": [
                        {"key": "task-egress.json", "path": "task-egress.json"},
                    ]},
                })
                pod["containers"][0].setdefault("volumeMounts", []).append({
                    "name": "task-egress-config", "mountPath": "/var/run/loom-task-egress",
                    "readOnly": True,
                })
                pod["containers"][0]["env"].extend(_env({
                    "LOOM_GW_TASK_EGRESS_CONFIG_FILE": "/var/run/loom-task-egress/task-egress.json",
                }))
            connections = {}
            for regional in config.get("regional_execution_targets", []):
                tid = regional["target_id"]
                path = "/var/run/loom-regional-kubernetes/" + tid
                _mount_secret(pod, tid + "-kubernetes", tid + "-gateway-kubernetes", path)
                pod["volumes"][-1]["secret"]["items"] = [
                    {"key": name, "path": name} for name in ("ca.crt", "credentials.json")
                ]
                connections[regional["cluster_scope_id"]] = {
                    "endpoint": regional["kubernetes_api_server"],
                    "ca_file": path + "/ca.crt",
                    "credentials_file": path + "/credentials.json",
                }
            if connections:
                cm["data"]["regional-kubernetes.json"] = canonical(connections).decode()
                pod.setdefault("volumes", []).append(
                    {
                        "name": "regional-config",
                        "configMap": {
                            "name": "loom-platform-config",
                            "items": [
                                {"key": "regional-kubernetes.json", "path": "connections.json"}
                            ],
                        },
                    }
                )
                pod["containers"][0].setdefault("volumeMounts", []).append(
                    {
                        "name": "regional-config",
                        "mountPath": "/var/run/loom-regional-config",
                        "readOnly": True,
                    }
                )
                pod["containers"][0]["env"].extend(
                    _env(
                        {
                            "LOOM_GW_SERVICE_EXECUTION_KUBERNETES_CONFIG_FILE": "/var/run/loom-regional-config/connections.json"
                        }
                    )
                )
        app_docs += [deployment, _service(name, ns, port)]
    web = _deployment(
        "loom-web",
        ns,
        images["web"],
        8080,
        "/",
        _env(
            {
                "LOOM_FRONTEND_ENVIRONMENT": config["environment"],
                "LOOM_FRONTEND_ENVIRONMENT_LABEL": "Nebius integration",
                "LOOM_FRONTEND_ROUTE_PATH": "",
                "LOOM_FRONTEND_API_BASE": "",
                "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://" + config["public_host"],
            }
        ),
        revision,
        cpu="25m",
        memory="64Mi",
    )
    wpod = web["spec"]["template"]["spec"]
    wpod["securityContext"].update(runAsUser=101, runAsGroup=101, fsGroup=101)
    wpod["volumes"] = [
        {"name": "public-config", "configMap": {"name": "loom-platform-config"}},
        {"name": "tls-data", "persistentVolumeClaim": {"claimName": "loom-web-tls"}},
    ]
    wpod["containers"].append(
        {
            "name": "public-tls",
            "image": images["web"],
            "imagePullPolicy": "IfNotPresent",
            "command": ["/usr/bin/caddy"],
            "args": ["run", "--config", "/etc/loom-public/public-tls.json"],
            "env": _env({"XDG_CONFIG_HOME": "/data/config", "XDG_DATA_HOME": "/data"}),
            "ports": [{"name": "https", "containerPort": 8443}],
            "volumeMounts": [
                {"name": "public-config", "mountPath": "/etc/loom-public", "readOnly": True},
                {"name": "tls-data", "mountPath": "/data"},
            ],
            "readinessProbe": {
                "httpGet": {
                    "scheme": "HTTPS",
                    "path": "/",
                    "port": "https",
                    "httpHeaders": [{"name": "Host", "value": config["public_host"]}],
                },
                "periodSeconds": 5,
            },
            "resources": {
                "requests": {"cpu": "25m", "memory": "64Mi"},
                "limits": {"cpu": "500m", "memory": "256Mi"},
            },
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }
    )
    if config.get("public_tls_bootstrap", False):
        wpod["volumes"].append(
            {"name": "public-tls", "secret": {"secretName": config["tls_secret_name"]}}
        )
        wpod["containers"][-1]["volumeMounts"].append(
            {"name": "public-tls", "mountPath": "/var/run/loom-public-tls", "readOnly": True}
        )
    # Services are applied after the deployment's mandatory pre-mutation backup.
    # RWO permits the rolling Pods on the single integration system node.
    app_docs.append(
        _obj(
            "PersistentVolumeClaim",
            "loom-web-tls",
            ns,
            {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": config["storage_class"],
                "resources": {"requests": {"storage": "4Gi"}},
            },
        )
    )
    app_docs.append(web)
    if config.get("shared_ingress_enabled", False):
        origin = _service("loom-web-origin", ns, 443, 8443)
        origin["spec"]["selector"] = {"app": "loom-web"}
        app_docs.append(origin)
    files["40-services.yaml"] = app_docs
    files["50-configure.yaml"] = [job(f"loom-platform-configure-{short}", "configure")]
    execution_docs = (
        [] if not execution_enabled or config["schema_version"] == "loom.nebius-managed-environment.v1"
        else _execution_documents(config, images, repo_root)
    )
    for regional in config.get("regional_execution_targets", []):
        primary_docs, _ = _regional_documents(config, regional, images, repo_root)
        execution_docs.extend(primary_docs)
    for document in execution_docs:
        if document["kind"] == "ResourceQuota" and document["metadata"].get("namespace") == ex:
            document["spec"]["hard"] = _execution_quota(config, execution_docs)
    if config.get("regional_execution_targets") or guest_id is not None:
        database_policy = next(
            doc
            for doc in files["10-config-network.yaml"]
            if doc["kind"] == "NetworkPolicy" and doc["metadata"]["name"] == "postgres-private"
        )
        database_policy["spec"]["ingress"][0]["from"].extend(
            [
                {
                    "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ex}},
                    "podSelector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": regional["target_id"] + "-actuator"
                        }
                    },
                }
                for regional in config.get("regional_execution_targets", [])
            ]
        )
        for sibling_id in guest_target_ids(config):
            database_policy["spec"]["ingress"][0]["from"].append({
                "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ex}},
                "podSelector": {"matchLabels": {"app.kubernetes.io/name": sibling_id + "-actuator"}},
            })
    files["00-namespaces.yaml"].extend(doc for doc in execution_docs if doc["kind"] == "Namespace")
    files["60-execution.yaml"] = [doc for doc in execution_docs if doc["kind"] != "Namespace"]
    public = _service("loom-web", ns, 443, 8443)
    public["spec"]["type"] = "LoadBalancer"
    if config.get("shared_ingress_enabled", False):
        public["spec"]["selector"] = {"app": "loom-shared-ingress"}
    public["metadata"]["annotations"] = {
        "nebius.com/load-balancer-allocation-id": config["public_allocation_id"]
    }
    files["70-public.yaml"] = [public]
    backup_pod = job("loom-platform-backup", "backup")["spec"]["template"]
    backup_spec = backup_pod["spec"]
    # A database credential is needed only by pg_dump; the uploader receives
    # only the separate backup bucket key and a transient dump volume.
    uploader = backup_spec["containers"][0]
    uploader["env"] = [
        *_env({"LOOM_PLATFORM_CONFIG": "/var/run/loom-platform/environment.json"}),
        _secret_env("LOOM_BACKUP_ACCESS_KEY", "loom-platform-storage", "backup-access-key"),
        _secret_env("LOOM_BACKUP_SECRET_KEY", "loom-platform-storage", "backup-secret-key"),
    ]
    uploader["volumeMounts"] = [
        {"name": "platform-config", "mountPath": "/var/run/loom-platform", "readOnly": True},
        {"name": "dump", "mountPath": "/backup", "readOnly": True},
    ]
    backup_spec["volumes"] = [
        {"name": "platform-config", "configMap": {"name": "loom-platform-config"}},
        {
            "name": "db-ca",
            "secret": {
                "secretName": "loom-platform-db",
                "items": [{"key": "ca.crt", "path": "ca.crt"}],
                "defaultMode": 0o440,
            },
        },
        {"name": "dump", "emptyDir": {"sizeLimit": f"{config['postgres_storage_gi']}Gi"}},
    ]
    backup_spec["initContainers"] = [
        {
            "name": "pg-dump",
            "image": config["backup_image"],
            "command": ["pg_dump", "--format=custom", "--no-owner", "--file=/backup/loom.dump"],
            "env": [
                *_env(
                    {
                        "PGHOST": db_host,
                        "PGPORT": 5432,
                        "PGUSER": "postgres",
                        "PGDATABASE": "loom",
                        "PGSSLMODE": "verify-full",
                        "PGSSLROOTCERT": "/var/run/loom-db/ca.crt",
                    }
                ),
                _secret_env("PGPASSWORD", "loom-platform-db", "postgres-password"),
            ],
            "volumeMounts": [
                {"name": "db-ca", "mountPath": "/var/run/loom-db", "readOnly": True},
                {"name": "dump", "mountPath": "/backup"},
            ],
            "resources": {
                "requests": {"cpu": "100m", "memory": "128Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }
    ]
    files["80-backup.yaml"] = [
        _obj(
            "CronJob",
            "loom-platform-backup",
            ns,
            {
                "schedule": "17 */6 * * *",
                "concurrencyPolicy": "Forbid",
                "successfulJobsHistoryLimit": 2,
                "failedJobsHistoryLimit": 3,
                "jobTemplate": {
                    "spec": {
                        "backoffLimit": 1,
                        "activeDeadlineSeconds": 1800,
                        "template": backup_pod,
                    }
                },
            },
            api="batch/v1",
        )
    ]
    return files


def write_platform(
    files: dict[str, list[dict[str, Any]]],
    config: dict[str, Any],
    candidate: dict[str, Any],
    output: Path,
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    for filename, docs in files.items():
        (output / filename).write_text(yaml.safe_dump_all(docs, sort_keys=False))
    return {
        "candidate_sha": candidate["candidate_sha"],
        "namespace": config["namespace"],
        "execution_namespace": config["execution_namespace"],
        "files": list(files),
    }
