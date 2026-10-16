"""Cluster-deploy config (#76 Phase 1B + #146).

Public surface unchanged: `ClusterConfig` dataclass with operator
fields, `load_cluster_config(path)` returns one. Fields and defaults
now come from `config/loom-schema.toml` (`render_config` section).
The dataclass shape is materialized at import time so call sites
keep dot-access (`cfg.image_tag`, `cfg.replicas.service`).
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, make_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loom_cli.runtime_resources import load_bundled_schema
from loom_config.loader import RenderConfigEntry

_SCHEMA = load_bundled_schema()


def _make_table_dataclass(entry: RenderConfigEntry) -> type:
    """For `python_type = "table"` entries (e.g. replicas), build a
    frozen dataclass whose fields are `entry.fields.keys()`."""
    assert entry.fields is not None

    def _field_for(default: Any) -> Any:
        if isinstance(default, list):

            def _list_factory(default: list[Any] = default) -> list[Any]:
                return list(default)

            return field(default_factory=_list_factory)
        if isinstance(default, dict):

            def _dict_factory(default: dict[str, Any] = default) -> dict[str, Any]:
                return dict(default)

            return field(default_factory=_dict_factory)
        return field(default=default)

    cls = make_dataclass(
        f"_{entry.name.capitalize()}Config",
        [(k, type(v), _field_for(v)) for k, v in entry.fields.items()],
        frozen=True,
    )
    return cls


def _build_cluster_config_cls() -> type:
    """Materialize `ClusterConfig` from `render_config`."""
    spec: list[tuple[str, type, Any]] = []
    for name in sorted(_SCHEMA.render_config):
        entry = _SCHEMA.render_config[name]
        if entry.python_type == "table":
            sub_cls = _make_table_dataclass(entry)
            spec.append((name, sub_cls, field(default_factory=sub_cls)))
        elif entry.python_type == "str_list":
            default = tuple(entry.default or ())
            spec.append((name, tuple[str, ...], field(default=default)))
        else:
            py_type = {"str": str, "int": int, "bool": bool, "float": float}[entry.python_type]
            spec.append((name, py_type, field(default=entry.default)))

    def _to_render_context(self: Any) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in fields(self):
            val = getattr(self, f.name)
            if hasattr(val, "__dataclass_fields__"):
                out[f.name] = {sub.name: getattr(val, sub.name) for sub in fields(val)}
            else:
                out[f.name] = val
        return out

    return make_dataclass(
        "ClusterConfig",
        spec,
        frozen=True,
        namespace={"to_render_context": _to_render_context},
    )


if TYPE_CHECKING:
    # Static shape for mypy. Mirrors the runtime dataclass produced by
    # _build_cluster_config_cls() so call sites get attribute checking
    # and `-> ClusterConfig` annotations type-check. When the schema
    # (config/loom-schema.toml) changes, refresh this stub to match.

    @dataclass(frozen=True)
    class _ReplicasConfig:
        service: int = 2
        control_plane: int = 2
        gateway: int = 2
        web: int = 0
        worker: int = 3

    @dataclass(frozen=True)
    class _WorkerCapacityConfig:
        max_concurrent: int = 16
        cpu_request: str = "500m"
        cpu_limit: str = "8"
        memory_request: str = "1Gi"
        memory_limit: str = "16Gi"

    @dataclass(frozen=True)
    class _WorkloadContractConfig:
        workload_trust_mode: str = "internal_trusted"
        taskset_transforms_enabled: bool = False
        taskset_transform_network_isolated: bool = False
        untrusted_workload_isolation: bool = False

    @dataclass(frozen=True)
    class _K8sWorkerConfig:
        enabled: bool = False

    @dataclass(frozen=True)
    class _GatewayHpaConfig:
        enabled: bool = False
        min_replicas: int = 2
        max_replicas: int = 8
        cpu_target_pct: int = 60

    @dataclass(frozen=True)
    class _LlmGatewaySandboxConfig:
        enabled: bool = False

    @dataclass(frozen=True)
    class _NebiusExecutionConfig:
        enabled: bool = False
        source_secret_name: str = ""
        runtime_profile_secret_name: str = ""
        image_admission_secret_name: str = ""
        configuration_revision: str = ""
        source_egress_allowlist: list[str] = field(default_factory=list)
        execution_ingress_cidrs: list[str] = field(default_factory=list)

    @dataclass(frozen=True)
    class _NebiusPrivateEntryConfig:
        enabled: bool = False
        node_name: str = ""
        wireguard_address: str = ""
        peer_address: str = ""
        proxy_image: str = ""

    @dataclass(frozen=True)
    class _TopologyConfig:
        multi_node: bool = False
        storage_backend: str = "host_path"
        postgres_replicas: int = 1
        minio_replicas: int = 1
        min_available: int = 1
        anti_affinity: str = "preferred"

    @dataclass(frozen=True)
    class ClusterConfig:
        image_tag: str = "0.7"
        # Optional container registry prefix applied to every locally-built
        # `loom-*` image reference in the rendered manifest. Empty (default)
        # keeps unprefixed names for unprotected local rendering. For
        # protected multi-node k3s / production, set
        # to the registry host+port (no trailing slash) and point each
        # node's `/etc/rancher/k3s/registries.yaml` at the same endpoint so
        # containerd can pull the published candidate images.
        container_registry: str = ""
        # Operator-local publication endpoint for container_registry. This is
        # explicit because a k3s mirror may pull through a LAN address while
        # the rollout host pushes through a loopback-bound registry port.
        container_registry_push: str = ""
        runtime_environment: str = "production"
        ingress_cert_manager_cluster_issuer: str = ""
        ingress_class_name: str = "nginx"
        ingress_host: str = "loom.example.com"
        ingress_redirect_hosts: tuple[str, ...] = ()
        ingress_tls_secret_name: str = "loom-tls"
        lifecycle_legacy_buckets: tuple[str, ...] = ()
        frontend_environment: str = "local"
        frontend_environment_label: str = "Local development"
        frontend_route_path: str = ""
        frontend_api_base_path: str = ""
        gateway_local_providers: tuple[str, ...] = ()
        frontend_route_path_from: str = ""
        artifacts_bucket: str = "artifacts"
        gateway_hpa: _GatewayHpaConfig = field(default_factory=_GatewayHpaConfig)
        k8s_worker: _K8sWorkerConfig = field(default_factory=_K8sWorkerConfig)
        llm_gateway_sandbox: _LlmGatewaySandboxConfig = field(
            default_factory=_LlmGatewaySandboxConfig
        )
        nebius_execution: _NebiusExecutionConfig = field(default_factory=_NebiusExecutionConfig)
        nebius_private_entry: _NebiusPrivateEntryConfig = field(
            default_factory=_NebiusPrivateEntryConfig
        )
        minio_image: str = "minio/minio"
        minio_storage_gi: int = 500
        namespace: str = "loom"
        persistent_storage_backend: str = "dynamic"
        persistent_storage_host_path_root: str = ""
        postgres_image: str = "postgres:16"
        postgres_storage_gi: int = 50
        provider_egress_allowlist: tuple[str, ...] = ()
        replicas: _ReplicasConfig = field(default_factory=_ReplicasConfig)
        trajectories_bucket: str = "trajectories"
        topology: _TopologyConfig = field(default_factory=_TopologyConfig)
        trial_cache_registry_repo: str = ""
        worker_capacity: _WorkerCapacityConfig = field(default_factory=_WorkerCapacityConfig)
        worker_subprocess_gateway_url: str = "http://host.docker.internal:30443/openai/v1"
        worker_trajectory_storage_gi: int = 100
        workload_contract: _WorkloadContractConfig = field(default_factory=_WorkloadContractConfig)

        def to_render_context(self) -> dict[str, Any]: ...
else:
    ClusterConfig = _build_cluster_config_cls()


def lifecycle_inventory_buckets(config: ClusterConfig) -> tuple[str, ...]:
    """Return the exact, unambiguous staging object-inventory boundary."""
    buckets = (
        config.trajectories_bucket,
        config.artifacts_bucket,
        *config.lifecycle_legacy_buckets,
    )
    if any(not bucket or bucket != bucket.strip() for bucket in buckets) or len(
        set(buckets)
    ) != len(buckets):
        raise ValueError("lifecycle inventory buckets must be non-empty, normalized, and unique")
    return buckets


_DEPRECATED_TOP_LEVEL_KEYS: dict[str, str] = {
    "gateway_public_host": (
        "gateway_public_host is no longer supported; staging keeps "
        "loom-llm-gateway internal-only. Remove it and route all public "
        "clients through ingress_host + /api/v1."
    ),
}


def cluster_config_from_mapping(raw: Mapping[str, Any]) -> ClusterConfig:
    """Build one immutable cluster config from an already parsed snapshot."""
    deprecated = set(raw) & set(_DEPRECATED_TOP_LEVEL_KEYS)
    if deprecated:
        messages = [_DEPRECATED_TOP_LEVEL_KEYS[key] for key in sorted(deprecated)]
        raise ValueError("; ".join(messages))
    field_names = {f.name for f in fields(ClusterConfig)}
    unknown = set(raw.keys()) - field_names
    if unknown:
        raise ValueError(
            f"unknown keys in cluster config: {sorted(unknown)} (known: {sorted(field_names)})"
        )
    kwargs: dict[str, Any] = {}
    for name, val in raw.items():
        entry_field = next(f for f in fields(ClusterConfig) if f.name == name)
        field_type = entry_field.type
        if isinstance(field_type, type) and hasattr(field_type, "__dataclass_fields__"):
            sub_cls: type = field_type
            sub_known = {f.name for f in fields(sub_cls)}
            if not isinstance(val, dict):
                raise ValueError(f"[{name}] must be a TOML table")
            sub_unknown = set(val.keys()) - sub_known
            if sub_unknown:
                raise ValueError(
                    f"unknown keys under [{name}]: {sorted(sub_unknown)} "
                    f"(known: {sorted(sub_known)})"
                )
            # Coerce values to the right type via the sub-dataclass default's type
            default_instance = sub_cls()
            coerced: dict[str, Any] = {}
            for k, v in val.items():
                default_value = getattr(default_instance, k)
                if isinstance(default_value, list):
                    if not isinstance(v, list):
                        raise ValueError(f"{name}.{k} must be a TOML array")
                    coerced[k] = list(v)
                elif isinstance(default_value, dict):
                    if not isinstance(v, dict):
                        raise ValueError(f"{name}.{k} must be a TOML table")
                    coerced[k] = dict(v)
                else:
                    coerced[k] = type(default_value)(v)
            kwargs[name] = sub_cls(**coerced)
        else:
            if entry_field.type == tuple[str, ...]:
                if not isinstance(val, list) or not all(isinstance(v, str) for v in val):
                    raise ValueError(f"{name} must be a TOML array of strings")
                kwargs[name] = tuple(val)
            else:
                kwargs[name] = val
    config = ClusterConfig(**kwargs)
    validate_container_registry_publication(config)
    return config


_REGISTRY_PREFIX_RE = re.compile(
    r"(?:localhost|(?:[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?)|"
    r"(?:[0-9]{1,3}[.]){3}[0-9]{1,3})"
    r"(?::[1-9][0-9]{0,4})?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*\Z"
)


def validate_container_registry_prefixes(pull: str, push: str) -> tuple[str, str] | None:
    """Return exact pull/push prefixes, rejecting implicit publication authority."""
    if not pull and not push:
        return None
    if not pull or not push:
        raise ValueError(
            "container_registry and container_registry_push must be configured together"
        )
    validate_container_registry_prefix(pull, name="container_registry")
    validate_container_registry_prefix(push, name="container_registry_push")
    return pull, push


def validate_container_registry_prefix(value: str, *, name: str) -> str:
    if (
        not value
        or value != value.strip()
        or value.endswith("/")
        or "://" in value
        or "@" in value
        or _REGISTRY_PREFIX_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{name} must be a registry prefix without scheme or trailing slash")
    return value


def validate_container_registry_publication(config: ClusterConfig) -> tuple[str, str] | None:
    """Return exact pull/push prefixes from one cluster config."""
    return validate_container_registry_prefixes(
        str(config.container_registry),
        str(config.container_registry_push),
    )


def load_cluster_config(path: Path | None) -> ClusterConfig:
    """Same semantics as before #146: empty/missing path → defaults;
    unknown top-level or nested keys raise loudly."""
    if path is None:
        return ClusterConfig()
    if not path.exists():
        raise FileNotFoundError(f"cluster config not found: {path}")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    return cluster_config_from_mapping(raw)
