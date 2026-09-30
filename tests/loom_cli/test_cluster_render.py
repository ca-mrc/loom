"""`loom cluster render` unit + golden-file tests (#76 Phase 1B).

The golden test compares the default render against the canonical
`deploy/k8s/*.yaml` set. Future drift in either the templates or
the example manifests gets caught immediately — operators reading
the YAML files in the repo are seeing the same thing the CLI emits.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from urllib.parse import unquote

import pytest
import yaml

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import _resolve_config_target, render_manifests
from loom_cli.cluster_config import (
    ClusterConfig,
    load_cluster_config,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY_DIR = _REPO_ROOT / "deploy" / "k8s"
# Order MUST match `cluster_cmd._TEMPLATE_ORDER` so per-doc comparison
# lines up: postgres → minio → control-plane → service → gateway →
# worker → web → ingress.
_GOLDEN_FILES = (
    "postgres.yaml",
    "pgbouncer.yaml",
    "minio.yaml",
    "control-plane.yaml",
    "family-orchestrator.yaml",
    "pipeline-orchestrator.yaml",
    "loom-service.yaml",
    "llm-gateway.yaml",
    "worker.yaml",
    "web.yaml",
    "ingress.yaml",
    "gateway-router.yaml",
    # Phase C (#190) — egress proxy chain. Default replicas=0 so
    # the resources exist in the manifest but no pods until
    # operators scale up.
    "egress-xds.yaml",
    "egress-proxy.yaml",
    "network-policies.yaml",
    "grafana-dashboards.yaml",
)


def _load_docs(yaml_text: str) -> list[dict]:
    """Parse a multi-document YAML string and drop None placeholders
    (empty documents at start/end produced by stray separators)."""
    return [d for d in yaml.safe_load_all(yaml_text) if d]


def _deployment_env_value(docs: list[dict], deployment: str, env_name: str) -> str | None:
    doc = next(
        d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == deployment
    )
    env = doc["spec"]["template"]["spec"]["containers"][0]["env"]
    item = next((entry for entry in env if entry["name"] == env_name), None)
    return item.get("value") if item else None


# Local in-cluster workers require explicit opt-in.
# Render tests that exercise the worker Deployment / worker
# NetworkPolicy must opt in explicitly. `_DEFAULT_CFG` mirrors the
# local worker configuration so the canonical
# `deploy/k8s/*.yaml` golden files still apply.
def _default_cfg(**kwargs: object) -> ClusterConfig:
    k8s_worker_cls = type(ClusterConfig().k8s_worker)
    return ClusterConfig(k8s_worker=k8s_worker_cls(enabled=True), **kwargs)


_DEFAULT_CFG = _default_cfg()


# ──────────────────────────────────────────────────────────────────────
# ClusterConfig + load
# ──────────────────────────────────────────────────────────────────────


def test_default_config_includes_public_tls_knobs() -> None:
    """Public deploys must render an HTTPS ingress by default, with
    operator-overridable host, class, and certificate secret knobs."""
    cfg = ClusterConfig()
    assert cfg.ingress_host == "loom.example.com"
    assert cfg.ingress_class_name == "nginx"
    assert cfg.ingress_tls_secret_name == "loom-tls"
    assert cfg.ingress_cert_manager_cluster_issuer == ""
    assert cfg.persistent_storage_backend == "dynamic"
    assert cfg.persistent_storage_host_path_root == ""


def test_default_replicas_match_spec() -> None:
    """cluster-deploy.md §Component map specifies these defaults."""
    cfg = ClusterConfig()
    assert cfg.replicas.service == 2
    assert cfg.replicas.control_plane == 2
    assert cfg.replicas.gateway == 2
    # Paused per spec — operators scale up explicitly.
    assert cfg.replicas.web == 0
    assert cfg.replicas.worker == 3


def test_default_worker_capacity_is_above_single_worker_baseline() -> None:
    """Production render must not silently regress to the staging
    incident shape: one worker with five execution slots.
    """
    cfg = ClusterConfig()
    assert cfg.replicas.worker >= 3
    assert cfg.worker_capacity.max_concurrent >= 16
    assert cfg.replicas.worker * cfg.worker_capacity.max_concurrent >= 48


def test_load_config_from_toml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'image_tag = "1.2.3"\n'
        'ingress_host = "loom.acme.example"\n'
        'ingress_redirect_hosts = ["www.loom.acme.example"]\n'
        'ingress_class_name = "public-nginx"\n'
        'ingress_tls_secret_name = "loom-acme-tls"\n'
        'ingress_cert_manager_cluster_issuer = "letsencrypt-prod"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-staging"\n'
        'provider_egress_allowlist = ["202.78.161.51:18001"]\n'
        "[replicas]\n"
        "service = 5\n"
        "worker = 10\n",
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.image_tag == "1.2.3"
    assert cfg.ingress_host == "loom.acme.example"
    assert cfg.ingress_redirect_hosts == ("www.loom.acme.example",)
    assert cfg.ingress_class_name == "public-nginx"
    assert cfg.ingress_tls_secret_name == "loom-acme-tls"
    assert cfg.ingress_cert_manager_cluster_issuer == "letsencrypt-prod"
    assert cfg.persistent_storage_backend == "static-host-path"
    assert cfg.persistent_storage_host_path_root == "/data/loom-staging"
    assert cfg.provider_egress_allowlist == ("202.78.161.51:18001",)
    assert cfg.replicas.service == 5
    assert cfg.replicas.worker == 10
    # Unspecified fields keep their defaults.
    assert cfg.replicas.control_plane == 2


def test_load_config_from_toml_accepts_worker_capacity(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        "[worker_capacity]\n"
        "max_concurrent = 24\n"
        'cpu_request = "2"\n'
        'cpu_limit = "32"\n'
        'memory_request = "8Gi"\n'
        'memory_limit = "128Gi"\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.worker_capacity.max_concurrent == 24
    assert cfg.worker_capacity.cpu_request == "2"
    assert cfg.worker_capacity.cpu_limit == "32"
    assert cfg.worker_capacity.memory_request == "8Gi"
    assert cfg.worker_capacity.memory_limit == "128Gi"


def test_load_config_from_toml_accepts_worker_subprocess_gateway_url(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'worker_subprocess_gateway_url = "http://host.docker.internal:30444/openai/v1"\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.worker_subprocess_gateway_url == "http://host.docker.internal:30444/openai/v1"


def test_load_config_rejects_deprecated_gateway_public_host(
    tmp_path: Path,
) -> None:
    """The staging boundary no longer allows a public LLM Gateway
    host; old configs must fail instead of silently exposing it."""
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text('gateway_public_host = "gw.acme.example"\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"gateway_public_host.*no longer supported"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    """Typos must surface immediately, not silently use the default.
    Operators staring at `imag_tag = "..."` (typo) deserve a clear
    error, not a manifest with the wrong tag."""
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="unknown keys in cluster config"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_unknown_replicas_key(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        "[replicas]\nservvice = 4\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"unknown keys under \[replicas\]"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_non_string_provider_egress_allowlist(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'provider_egress_allowlist = ["202.78.161.51:18001", 18001]\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="array of strings"):
        load_cluster_config(cfg_path)


def test_load_config_missing_path_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_cluster_config(Path("/nonexistent/cluster.toml"))


def test_load_config_path_none_returns_defaults() -> None:
    cfg = load_cluster_config(None)
    assert cfg == ClusterConfig()


def _gateway_env(cfg: ClusterConfig) -> dict[str, dict]:
    """Return the llm-gateway container's env keyed by name."""
    docs = [d for d in yaml.safe_load_all(render_manifests(cfg)) if d]
    gw = next(
        d
        for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"] == "loom-llm-gateway"
    )
    return {e["name"]: e for e in gw["spec"]["template"]["spec"]["containers"][0]["env"]}


def test_gateway_local_providers_render_env() -> None:
    """A configured local provider renders LOOM_GW_LOCAL_<NAME>_* env: a
    literal base URL + an optional secret-backed API key, so agents can
    route `local/<name>/<model>` through an operator relay (#1141)."""
    cfg = ClusterConfig(gateway_local_providers=("yibu|https://yibuapi.com/v1|qa-relay-api-key",))
    env = _gateway_env(cfg)
    assert env["LOOM_GW_LOCAL_YIBU_BASE_URL"]["value"] == "https://yibuapi.com/v1"
    key_ref = env["LOOM_GW_LOCAL_YIBU_API_KEY"]["valueFrom"]["secretKeyRef"]
    assert key_ref == {"name": "loom-secrets", "key": "qa-relay-api-key", "optional": True}


def test_gateway_local_providers_absent_by_default() -> None:
    """Production (empty allowlist) must not leak any local-provider env —
    keeps the default golden manifest and prod deploys relay-free."""
    env = _gateway_env(ClusterConfig())
    assert not [name for name in env if name.startswith("LOOM_GW_LOCAL_")]


def test_gateway_local_providers_name_is_uppercased_and_dash_normalized() -> None:
    cfg = ClusterConfig(gateway_local_providers=("qa-relay|https://relay.example/v1|relay-key",))
    env = _gateway_env(cfg)
    assert "LOOM_GW_LOCAL_QA_RELAY_BASE_URL" in env


@pytest.mark.parametrize(
    "entry",
    [
        "yibu|https://yibuapi.com/v1",  # too few fields
        "yibu|https://yibuapi.com/v1|key|extra",  # too many
        "|https://yibuapi.com/v1|key",  # empty name
        "yibu||key",  # empty base_url
        "yibu|https://yibuapi.com/v1|",  # empty secret_key
        "yi bu|https://yibuapi.com/v1|key",  # non-env-safe name
    ],
)
def test_gateway_local_providers_rejects_malformed(entry: str) -> None:
    with pytest.raises(ValueError, match="gateway_local_providers"):
        render_manifests(ClusterConfig(gateway_local_providers=(entry,)))


def test_gateway_local_providers_rejects_duplicate_name() -> None:
    cfg = ClusterConfig(
        gateway_local_providers=(
            "yibu|https://a.example/v1|k1",
            "yibu|https://b.example/v1|k2",
        )
    )
    with pytest.raises(ValueError, match="duplicate"):
        render_manifests(cfg)


def test_load_config_gateway_local_providers_from_toml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'gateway_local_providers = ["yibu|https://yibuapi.com/v1|qa-relay-api-key"]\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.gateway_local_providers == ("yibu|https://yibuapi.com/v1|qa-relay-api-key",)


# ──────────────────────────────────────────────────────────────────────
# render_manifests
# ──────────────────────────────────────────────────────────────────────


def test_render_produces_valid_yaml_with_expected_kinds() -> None:
    """Smoke: every document parses, the set covers the 9 Deployments
    + 1 DaemonSet + 10 Services + 3 StatefulSets + 1 Ingress
    + 1 PodDisruptionBudget + 13 NetworkPolicies + 2 ConfigMaps
    (Grafana dashboards + egress-proxy bootstrap) expected by
    cluster-deploy.md §Component map + sandbox-isolation.md."""
    text = render_manifests(_DEFAULT_CFG)
    assert text.startswith("apiVersion: apps/v1\nkind: StatefulSet\n")
    docs = _load_docs(text)
    kinds = [d["kind"] for d in docs]
    # postgres, minio, worker (#673: dynamic-storage worker uses a
    # StatefulSet with volumeClaimTemplates so multi-node RWO PVCs
    # don't strand replicas).
    assert kinds.count("StatefulSet") == 3
    # cp, service, gateway, web + egress-xds + egress-proxy +
    # pgbouncer + family-orchestrator (#672) + disabled pipeline orchestrator.
    assert kinds.count("Deployment") == 9
    # gateway-router
    assert kinds.count("DaemonSet") == 1
    # postgres + pgbouncer + minio + cp + gateway + service + web
    # + ingress + egress + worker (headless, StatefulSet peer DNS) = 10
    assert kinds.count("Service") == 10
    assert kinds.count("Ingress") == 1
    # Dynamic-storage default: worker PVCs live in the StatefulSet's
    # volumeClaimTemplates, so no top-level PVC survives here.
    assert kinds.count("PersistentVolumeClaim") == 0
    # pgbouncer PodDisruptionBudget.
    assert kinds.count("PodDisruptionBudget") == 1
    # NetworkPolicies: postgres + minio + cp + gateway + worker + svc
    # + web + gateway-router + egress-xds
    # + egress-proxy + pgbouncer + both orchestrators = 13.
    assert kinds.count("NetworkPolicy") == 13
    assert kinds.count("CronJob") == 0
    # Grafana dashboards ConfigMap + egress-proxy bootstrap ConfigMap.
    assert kinds.count("ConfigMap") == 2


def test_worker_manifest_sets_subprocess_gateway_url_for_sandboxes() -> None:
    cfg = _default_cfg(
        worker_subprocess_gateway_url="http://host.docker.internal:30444/openai/v1",
    )
    docs = _load_docs(render_manifests(cfg))
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    by_name = {entry["name"]: entry for entry in env}
    assert by_name["LOOM_WORKER_SUBPROCESS_GATEWAY_URL"]["value"] == (
        "http://host.docker.internal:30444/openai/v1"
    )


def test_dataset_hf_token_is_not_injected_into_service_or_worker() -> None:
    """Dataset download runs upstream; neither hosted API nor workers get its token."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    service = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-service"
    )
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    service_env = service["spec"]["template"]["spec"]["containers"][0]["env"]
    service_by_name = {entry["name"]: entry for entry in service_env}
    assert "HF_TOKEN" not in service_by_name
    assert "LOOM_SVC_LOCAL_SERVERS_JSON" not in service_by_name
    worker_env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    worker_by_name = {entry["name"]: entry for entry in worker_env}
    assert "HF_TOKEN" not in worker_by_name


def test_worker_manifest_mounts_docker_registry_auth_config() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    pod_spec = worker["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert {
        "name": "docker-config",
        "mountPath": "/root/.docker",
        "readOnly": True,
    } in container["volumeMounts"]
    assert {
        "name": "docker-config",
        "secret": {
            "secretName": "docker-config",
            "optional": True,
            "items": [
                {
                    "key": ".dockerconfigjson",
                    "path": "config.json",
                },
            ],
        },
    } in pod_spec["volumes"]


def test_worker_manifest_renders_configured_capacity_and_resources() -> None:
    worker_capacity_cls = type(ClusterConfig().worker_capacity)
    cfg = _default_cfg(
        worker_capacity=worker_capacity_cls(
            max_concurrent=24,
            cpu_request="2",
            cpu_limit="32",
            memory_request="8Gi",
            memory_limit="128Gi",
        ),
    )
    docs = _load_docs(render_manifests(cfg))
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    container = worker["spec"]["template"]["spec"]["containers"][0]
    env = [entry for entry in container["env"] if entry["name"] == "LOOM_WORKER_MAX_CONCURRENT"]
    assert env == [{"name": "LOOM_WORKER_MAX_CONCURRENT", "value": "24"}]
    env_map = {entry["name"]: entry for entry in container["env"]}
    assert env_map["LOOM_WORKER_DOCKER_API_TIMEOUT_SEC"] == {
        "name": "LOOM_WORKER_DOCKER_API_TIMEOUT_SEC",
        "value": "1800",
    }
    assert env_map["LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS"] == {
        "name": "LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS",
        "value": "256",
    }
    assert env_map["LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC"] == {
        "name": "LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC",
        "value": "300.0",
    }
    assert container["resources"] == {
        "requests": {"cpu": "2", "memory": "8Gi"},
        "limits": {"cpu": "32", "memory": "128Gi"},
    }


def test_render_default_matches_deploy_k8s_yamls() -> None:
    """Golden test: rendering with default config produces the same
    set of k8s objects as the canonical `deploy/k8s/*.yaml` files.

    This pins the templates against drift in either direction — if
    someone edits a deploy/k8s/*.yaml without updating the matching
    template, this test fails. Same in reverse.
    """
    rendered = _load_docs(render_manifests(_DEFAULT_CFG))
    expected: list[dict] = []
    for fn in _GOLDEN_FILES:
        expected.extend(_load_docs((_DEPLOY_DIR / fn).read_text()))
    assert len(rendered) == len(expected), (
        f"render emitted {len(rendered)} docs; deploy/k8s/*.yaml has "
        f"{len(expected)}. Either the template order changed or a "
        f"manifest was added/removed without updating "
        f"`_TEMPLATE_ORDER` / this test's golden list."
    )
    for i, (r, e) in enumerate(zip(rendered, expected, strict=True)):
        assert r == e, (
            f"doc {i} ({r.get('kind')}/"
            f"{r.get('metadata', {}).get('name')}) differs from "
            f"deploy/k8s/. Re-run `loom cluster render > /tmp/r.yaml` "
            f"and diff against the appropriate file."
        )


def test_render_with_custom_image_tag_applies_to_every_loom_image() -> None:
    """Configure a non-default image_tag; every loom-* image MUST pick
    it up. External images (postgres, minio) stay on their own tags."""
    cfg = _default_cfg(image_tag="2.0.0-rc1")
    docs = _load_docs(render_manifests(cfg))
    loom_images = []
    for d in docs:
        if d["kind"] not in ("Deployment", "StatefulSet"):
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            if c["image"].startswith("loom-"):
                loom_images.append(c["image"])
    assert loom_images, "no loom-* images found in render output"
    for img in loom_images:
        assert img.endswith(":2.0.0-rc1"), f"expected :2.0.0-rc1 suffix, got {img}"


def test_render_injects_secret_store_master_key_for_provider_paths() -> None:
    """BYO provider create/test in service and provider dispatch in
    gateway both use LocalEncryptedSecretStore. Cluster mode must wire
    the shared master key into both pods from loom-secrets.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    deployments = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Deployment"}

    expected = {
        "loom-service": "LOOM_SECRET_STORE_MASTER_KEY",
        "loom-llm-gateway": "LOOM_SECRET_STORE_MASTER_KEY",
    }
    for deployment_name, env_name in expected.items():
        env = deployments[deployment_name]["spec"]["template"]["spec"]["containers"][0]["env"]
        assert {
            "name": env_name,
            "valueFrom": {
                "secretKeyRef": {
                    "name": "loom-secrets",
                    "key": "secret-store-master-key",
                },
            },
        } in env


def _network_policy_named(docs: list[dict], name: str) -> dict:
    return next(d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == name)


def _ipblock_ports(policy: dict) -> set[tuple[str, int]]:
    out: set[tuple[str, int]] = set()
    for rule in policy["spec"].get("egress", []):
        ports = [p["port"] for p in rule.get("ports", []) if p.get("protocol", "TCP") == "TCP"]
        for target in rule.get("to", []):
            ip_block = target.get("ipBlock")
            if ip_block is None:
                continue
            for port in ports:
                out.add((ip_block["cidr"], int(port)))
    return out


def _public_ipblock_ports(policy: dict) -> set[int]:
    """Return TCP ports allowed to public internet egress."""
    out: set[int] = set()
    for rule in policy["spec"].get("egress", []):
        for target in rule.get("to", []):
            ip_block = target.get("ipBlock")
            if ip_block is None or ip_block.get("cidr") != "0.0.0.0/0":
                continue
            if set(ip_block.get("except", [])) != {
                "10.0.0.0/8",
                "172.16.0.0/12",
                "192.168.0.0/16",
                "169.254.0.0/16",
            }:
                continue
            for port in rule.get("ports", []):
                if port.get("protocol", "TCP") == "TCP":
                    out.add(int(port["port"]))
    return out


def test_pipeline_orchestrator_pgbouncer_egress_has_matching_ingress() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    policy = _network_policy_named(docs, "loom-pgbouncer")
    source_apps = {
        peer["podSelector"]["matchLabels"]["app"]
        for rule in policy["spec"]["ingress"]
        if {port["port"] for port in rule.get("ports", [])} == {6432}
        for peer in rule.get("from", [])
    }

    assert "loom-pipeline-orchestrator" in source_apps


def test_render_allows_service_public_https_for_provider_discovery() -> None:
    """Provider validation and model discovery run in loom-service, so
    the service pod needs the same public HTTPS/HTTP egress baseline as
    the gateway for ordinary hosted OpenAI-compatible `/models` APIs.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))

    assert {443, 80} <= _public_ipblock_ports(
        _network_policy_named(docs, "loom-service"),
    )
    assert {443, 80} <= _public_ipblock_ports(
        _network_policy_named(docs, "loom-llm-gateway"),
    )


def test_render_provider_egress_allowlist_adds_service_and_gateway_rules() -> None:
    """Operators can approve a non-standard BYO provider endpoint once
    in cluster-config.toml; render must preserve that policy for both
    provider validation in loom-service and runtime calls in gateway.
    """
    cfg = _default_cfg(
        provider_egress_allowlist=(
            "202.78.161.51:18001",
            "203.0.113.0/24:8443",
        ),
    )
    docs = _load_docs(render_manifests(cfg))

    expected = {
        ("202.78.161.51/32", 18001),
        ("203.0.113.0/24", 8443),
    }
    for policy_name in ("loom-service", "loom-llm-gateway", "loom-egress-proxy"):
        assert expected <= _ipblock_ports(_network_policy_named(docs, policy_name))
    assert not _ipblock_ports(_network_policy_named(docs, "loom-worker"))


def test_render_provider_egress_allowlist_rejects_hostname_targets() -> None:
    """Kubernetes NetworkPolicy can only enforce CIDRs. Hostnames
    must be resolved by the operator and listed as IP/CIDR entries so
    `loom cluster render` stays deterministic and auditable.
    """
    cfg = ClusterConfig(provider_egress_allowlist=("lux.example.com:18001",))
    with pytest.raises(ValueError, match="IP address or CIDR"):
        render_manifests(cfg)


@pytest.mark.parametrize(
    "target",
    [
        "0.0.0.0/0:18001",
        "127.0.0.1:18001",
        "169.254.169.254:80",
    ],
)
def test_render_provider_egress_allowlist_rejects_unsafe_targets(
    target: str,
) -> None:
    cfg = ClusterConfig(provider_egress_allowlist=(target,))
    with pytest.raises(ValueError, match=r"too broad|reserved"):
        render_manifests(cfg)


def test_render_default_ingress_uses_tls_secret_and_class() -> None:
    cfg = ClusterConfig()
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "nginx"
    assert ingress["spec"]["tls"] == [
        {
            "hosts": ["loom.example.com"],
            "secretName": "loom-tls",
        }
    ]
    assert "cert-manager.io/cluster-issuer" not in ingress["metadata"]["annotations"]


def test_render_ip_ingress_host_uses_hostless_tls_ingress() -> None:
    """Kubernetes rejects IP literals in Ingress host fields. For
    staging deployments reached directly by IP, render a hostless
    Ingress and let the operator-provided TLS secret carry the IP SAN.
    """
    cfg = ClusterConfig(
        ingress_host="192.168.50.13",
        ingress_tls_secret_name="loom-ip-tls",
    )
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["tls"] == [{"secretName": "loom-ip-tls"}]
    assert "host" not in ingress["spec"]["rules"][0]


def test_render_with_cert_manager_cluster_issuer_annotation() -> None:
    cfg = ClusterConfig(
        ingress_host="loom.acme.example",
        ingress_class_name="public-nginx",
        ingress_tls_secret_name="loom-acme-tls",
        ingress_cert_manager_cluster_issuer="letsencrypt-prod",
    )
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "public-nginx"
    assert ingress["metadata"]["annotations"]["cert-manager.io/cluster-issuer"] == (
        "letsencrypt-prod"
    )
    assert ingress["spec"]["tls"] == [
        {
            "hosts": ["loom.acme.example"],
            "secretName": "loom-acme-tls",
        }
    ]


def test_render_ingress_redirect_hosts_bind_tls_and_redirect_to_canonical() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("www.yylx.world",),
        ingress_tls_secret_name="loom-staging-tls",
        ingress_cert_manager_cluster_issuer="letsencrypt-prod",
        frontend_environment="staging",
        frontend_environment_label="Development / staging",
        frontend_route_path="/dev",
        frontend_api_base_path="/dev",
    )
    docs = _load_docs(render_manifests(cfg))
    ingresses = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Ingress"}
    assert set(ingresses) == {"loom-ingress", "loom-frontend-prefix-redirect"}
    main = ingresses["loom-ingress"]
    redirect = ingresses["loom-frontend-prefix-redirect"]

    assert (
        main["metadata"]["annotations"]["nginx.ingress.kubernetes.io/from-to-www-redirect"]
        == "true"
    )
    assert main["metadata"]["annotations"]["cert-manager.io/cluster-issuer"] == ("letsencrypt-prod")
    assert (
        "nginx.ingress.kubernetes.io/from-to-www-redirect"
        not in redirect["metadata"]["annotations"]
    )
    assert "cert-manager.io/cluster-issuer" not in redirect["metadata"]["annotations"]
    assert [r["host"] for r in main["spec"]["rules"]] == ["yylx.world"]
    assert (
        redirect["spec"]["tls"]
        == main["spec"]["tls"]
        == [
            {
                "hosts": ["yylx.world", "www.yylx.world"],
                "secretName": "loom-staging-tls",
            }
        ]
    )


def test_render_accepts_route_transition_marker_and_renders_target() -> None:
    # frontend_route_path_from marks an in-flight route migration for the rollout
    # preflight; the manifests still render the target route_path.
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_tls_secret_name="loom-staging-tls",
        frontend_environment="staging",
        frontend_environment_label="Staging",
        frontend_route_path="/staging",
        frontend_api_base_path="/staging",
        frontend_route_path_from="/dev",
    )
    docs = _load_docs(render_manifests(cfg))
    main = next(
        d for d in docs if d["kind"] == "Ingress" and d["metadata"]["name"] == "loom-ingress"
    )
    paths = [p["path"] for r in main["spec"]["rules"] for p in r["http"]["paths"]]
    assert all("staging" in p for p in paths)
    assert not any("/(?-i:dev)" in p for p in paths)


def test_render_rejects_noop_route_transition_marker() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        frontend_environment="staging",
        frontend_environment_label="Staging",
        frontend_route_path="/staging",
        frontend_api_base_path="/staging",
        frontend_route_path_from="/staging",
    )
    with pytest.raises(ValueError, match="frontend_route_path_from must differ"):
        render_manifests(cfg)


def test_render_rejects_malformed_route_transition_marker() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        frontend_environment="staging",
        frontend_environment_label="Staging",
        frontend_route_path="/staging",
        frontend_api_base_path="/staging",
        frontend_route_path_from="/nope",
    )
    with pytest.raises(
        ValueError,
        match="frontend_route_path_from must be empty, /, /prod, /staging, or /dev",
    ):
        render_manifests(cfg)


def test_render_rejects_redirect_host_matching_canonical_host() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("yylx.world",),
    )
    with pytest.raises(ValueError, match="must not include ingress_host"):
        render_manifests(cfg)


def test_render_rejects_redirect_host_outside_www_counterpart() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("staging.example.com",),
    )
    with pytest.raises(ValueError, match="www/non-www counterpart"):
        render_manifests(cfg)


def test_render_ingress_routes_only_api_and_spa_backends() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    ingresses = [d for d in docs if d["kind"] == "Ingress"]
    assert [d["metadata"]["name"] for d in ingresses] == ["loom-ingress"]
    ingress = ingresses[0]
    assert [r["host"] for r in ingress["spec"]["rules"]] == ["loom.example.com"]
    paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert [
        (
            p["path"],
            p["pathType"],
            p["backend"]["service"]["name"],
            p["backend"]["service"]["port"]["number"],
        )
        for p in paths
    ] == [
        ("/api/v1", "Prefix", "loom-service", 8090),
        ("/", "Prefix", "loom-web", 80),
    ]


@pytest.mark.parametrize(
    ("filename", "route_path"),
    [
        ("production.cluster.toml", "/prod"),
        ("development.cluster.toml", "/dev"),
        ("staging.cluster.toml", "/staging"),
        ("staging.multinode.cluster.toml", "/staging"),
    ],
)
def test_render_profile_ingress_routes_api_and_spa_under_frontend_prefix(
    filename: str,
    route_path: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "tests" / "fixtures" / "cluster-render" / filename)
    docs = _load_docs(render_manifests(cfg))
    ingresses = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Ingress"}
    assert set(ingresses) == {"loom-ingress", "loom-frontend-prefix-redirect"}

    ingress = ingresses["loom-ingress"]
    annotations = ingress["metadata"]["annotations"]
    assert annotations["nginx.ingress.kubernetes.io/use-regex"] == "true"
    assert annotations["nginx.ingress.kubernetes.io/rewrite-target"] == "/$1"

    rule = ingress["spec"]["rules"][0]
    assert rule["host"] == "yylx.world"
    paths = rule["http"]["paths"]
    prefix_expression = f"/(?-i:{route_path[1:]})"
    assert [
        (
            p["path"],
            p["pathType"],
            p["backend"]["service"]["name"],
            p["backend"]["service"]["port"]["number"],
        )
        for p in paths
    ] == [
        (
            f"{prefix_expression}/(api/v1((/[^/%]+)*/?))$",
            "ImplementationSpecific",
            "loom-service",
            8090,
        ),
        (
            f"{prefix_expression}/(([^/%]+/)*[^/%]+/?)?$",
            "ImplementationSpecific",
            "loom-web",
            80,
        ),
    ]

    redirect = ingresses["loom-frontend-prefix-redirect"]
    assert "nginx.ingress.kubernetes.io/rewrite-target" not in (redirect["metadata"]["annotations"])
    redirect_path = redirect["spec"]["rules"][0]["http"]["paths"][0]
    assert redirect_path["path"] == f"{prefix_expression}$"
    assert redirect_path["pathType"] == "ImplementationSpecific"
    assert redirect_path["backend"]["service"]["name"] == "loom-web"
    assert redirect_path["backend"]["service"]["port"]["number"] == 80
    assert (
        _deployment_env_value(docs, "loom-service", "LOOM_SVC_PUBLIC_BASE_URL")
        == f"https://yylx.world{route_path}"
    )


def _rendered_dev_ingress_paths() -> dict[str, str]:
    cfg = _default_cfg(
        ingress_host="yylx.world",
        frontend_environment="staging",
        frontend_environment_label="Development / staging",
        frontend_route_path="/dev",
        frontend_api_base_path="/dev",
    )
    docs = _load_docs(render_manifests(cfg))
    ingress = next(
        doc
        for doc in docs
        if doc.get("kind") == "Ingress" and doc["metadata"]["name"] == "loom-ingress"
    )
    return {
        path["backend"]["service"]["name"]: path["path"]
        for path in ingress["spec"]["rules"][0]["http"]["paths"]
    }


def _ingress_nginx_fullmatch(pattern: str, request_path: str) -> re.Match[str] | None:
    """Model ingress-nginx's generated ``location ~*`` semantics."""
    return re.fullmatch(pattern, request_path, flags=re.IGNORECASE)


def _trusted_controller_guard_rejects(request_uri: str) -> bool:
    path = request_uri.partition("?")[0]
    if re.search(r"(?:%2f|%5c|\\|//)", path, flags=re.IGNORECASE):
        return True
    first_segment = path.partition("/")[2].partition("/")[0]
    if re.search(r"%[0-9a-f]{2}", first_segment, flags=re.IGNORECASE):
        return True
    canonical_prefix = re.match(r"^/(?:dev|prod)(?:/|$)", path)
    case_insensitive_prefix = re.match(
        r"^/(?:dev|prod)(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    return case_insensitive_prefix is not None and canonical_prefix is None


def test_render_prefixed_ingress_regexes_preserve_outer_rewrite_capture() -> None:
    paths = _rendered_dev_ingress_paths()
    cases = (
        ("loom-service", "/dev/api/v1", "api/v1"),
        ("loom-service", "/dev/api/v1/", "api/v1/"),
        ("loom-service", "/dev/api/v1/health", "api/v1/health"),
        ("loom-service", "/dev/api/v1/trials/example-id/", "api/v1/trials/example-id/"),
        ("loom-web", "/dev/", None),
        ("loom-web", "/dev/monitor", "monitor"),
        ("loom-web", "/dev/monitor/", "monitor/"),
        ("loom-web", "/dev/library/batches/example-id", "library/batches/example-id"),
    )

    for backend, request_path, expected_rewrite in cases:
        match = _ingress_nginx_fullmatch(paths[backend], request_path)
        assert match is not None, (backend, request_path)
        assert match.group(1) == expected_rewrite


def test_render_prefixed_ingress_controller_order_routes_api_before_spa() -> None:
    paths = _rendered_dev_ingress_paths()
    controller_order = sorted(
        paths.items(),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    assert len(paths["loom-service"]) > len(paths["loom-web"])

    cases = (
        ("/dev/api/v1", "loom-service"),
        ("/dev/api/v1/health", "loom-service"),
        ("/dev/api/v1/trials/example-id/artifacts", "loom-service"),
        ("/dev/", "loom-web"),
        ("/dev/monitor", "loom-web"),
        ("/dev/library/batches/example-id", "loom-web"),
    )
    for request_path, expected_backend in cases:
        first_match = next(
            backend
            for backend, pattern in controller_order
            if _ingress_nginx_fullmatch(pattern, request_path)
        )
        assert first_match == expected_backend


def test_render_prefixed_ingress_regexes_reject_cross_matches_and_empty_segments() -> None:
    paths = _rendered_dev_ingress_paths()
    non_routes = (
        "/dev",
        "/devil",
        "/devapi",
        "/prodfoo",
        "/dev%2Fmonitor",
        "/dev/%2Fmonitor",
        "/dev//monitor",
        "/dev/monitor//details",
        "/dev/api/v1/%2Fhealth",
        "/dev/api/v1//health",
        "/dev/api/v1/health//",
    )

    for request_path in non_routes:
        assert all(
            _ingress_nginx_fullmatch(pattern, request_path) is None for pattern in paths.values()
        ), request_path


def test_ingress_nginx_inline_prefix_case_resists_controller_ignorecase() -> None:
    paths = _rendered_dev_ingress_paths()
    redirect_pattern = "/(?-i:dev)$"
    noncanonical_routes = (
        ("/DEV", redirect_pattern),
        ("/Dev/monitor", paths["loom-web"]),
        ("/dEV/api/v1/health", paths["loom-service"]),
    )

    for request_path, controller_pattern in noncanonical_routes:
        assert _ingress_nginx_fullmatch(controller_pattern, request_path) is None
        assert _trusted_controller_guard_rejects(request_path)


def test_ingress_nginx_decoded_prefixes_cannot_bypass_raw_and_case_guards() -> None:
    paths = _rendered_dev_ingress_paths()
    cases = (
        ("/D%45V/monitor", paths["loom-web"], False),
        ("/d%45v/api/v1/health", paths["loom-service"], False),
        ("/d%65v/monitor", paths["loom-web"], True),
        ("/PR%4fD/", paths["loom-web"], False),
        ("/pr%4Fd/", paths["loom-web"], False),
    )

    for request_uri, controller_pattern, normalized_would_match in cases:
        normalized = unquote(request_uri)
        assert (
            _ingress_nginx_fullmatch(controller_pattern, normalized) is not None
        ) is normalized_would_match
        assert _trusted_controller_guard_rejects(request_uri)


@pytest.mark.parametrize(
    "request_uri",
    [
        "/dev%2Fmonitor",
        "/dev/%2fmonitor",
        "/dev%5Cmonitor",
        "/dev/%5cmonitor",
        r"/dev\monitor",
        r"/dev/\monitor",
        "/dev//monitor",
    ],
)
def test_trusted_controller_guard_rejects_ambiguous_raw_separators(
    request_uri: str,
) -> None:
    assert _trusted_controller_guard_rejects(request_uri)


def test_trusted_controller_guard_ignores_encoded_separator_in_query() -> None:
    assert not _trusted_controller_guard_rejects("/dev?next=%2Fmonitor&windows=%5Ctemp")


@pytest.mark.parametrize(
    ("filename", "runtime_environment"),
    [
        ("development.cluster.toml", "development"),
        ("production.cluster.toml", "production"),
        ("staging.cluster.toml", "staging"),
        ("staging.multinode.cluster.toml", "staging"),
    ],
)
def test_render_profiles_set_backend_runtime_environment(
    filename: str,
    runtime_environment: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "tests" / "fixtures" / "cluster-render" / filename)
    docs = _load_docs(render_manifests(cfg))

    for deployment in ("loom-control-plane", "loom-service", "loom-llm-gateway"):
        assert _deployment_env_value(docs, deployment, "LOOM_ENV") == runtime_environment
        assert _deployment_env_value(docs, deployment, "LOOM_NAMESPACE") == cfg.namespace


@pytest.mark.parametrize(
    "filename",
    ["staging.cluster.toml", "staging.multinode.cluster.toml"],
)
def test_staging_control_plane_has_no_retired_worker_runtime_dependency(
    filename: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "tests" / "fixtures" / "cluster-render" / filename)
    docs = _load_docs(render_manifests(cfg))
    deployment = next(
        document
        for document in docs
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"] == "loom-control-plane"
    )
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert not pod.get("initContainers")
    assert not any("PROTECTED_WORKER_RUNTIME" in item["name"] for item in container["env"])
    assert {item["name"] for item in container["volumeMounts"]} == {"loom-admin-secret"}
    assert {item["name"] for item in pod["volumes"]} == {"loom-admin-secret"}
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "fsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


@pytest.mark.parametrize(
    ("filename", "host_root"),
    [
        ("production.cluster.toml", "/data/loom-prod"),
    ],
)
def test_protected_profiles_declare_static_host_path_storage(
    filename: str,
    host_root: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "tests" / "fixtures" / "cluster-render" / filename)

    assert cfg.persistent_storage_backend == "static-host-path"
    assert cfg.persistent_storage_host_path_root == host_root


def test_staging_minio_render_pins_reachable_source_without_changing_storage_topology() -> None:
    cfg = load_cluster_config(
        _REPO_ROOT / "tests" / "fixtures" / "cluster-render" / "staging.multinode.cluster.toml"
    )
    docs = _load_docs(render_manifests(cfg))
    minio = next(
        doc
        for doc in docs
        if doc["kind"] == "StatefulSet" and doc["metadata"]["name"] == "loom-minio"
    )
    assert minio["spec"]["template"]["spec"]["containers"][0]["image"] == (
        "quay.io/minio/minio@sha256:"
        "14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
    )
    assert minio["spec"]["replicas"] == 4
    assert minio["spec"]["serviceName"] == "loom-minio-headless"
    claim = minio["spec"]["volumeClaimTemplates"][0]
    assert claim["metadata"]["name"] == "data"
    assert claim["spec"]["storageClassName"] == "longhorn"
    pdb = next(
        doc
        for doc in docs
        if doc["kind"] == "PodDisruptionBudget" and doc["metadata"]["name"] == "loom-minio"
    )
    assert pdb["spec"]["minAvailable"] == 3


def test_render_custom_storage_sizes() -> None:
    cfg = _default_cfg(
        postgres_storage_gi=200,
        minio_storage_gi=2000,
        worker_trajectory_storage_gi=500,
    )
    docs = _load_docs(render_manifests(cfg))
    pg = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-postgres"
    )
    pg_storage = pg["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"]
    assert pg_storage == "200Gi"
    minio = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    )
    minio_storage = minio["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"][
        "storage"
    ]
    assert minio_storage == "2000Gi"
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    worker_storage = worker["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"][
        "storage"
    ]
    assert worker_storage == "500Gi"


def test_render_static_host_path_storage_binds_critical_state_to_retain_pvs() -> None:
    cfg = _default_cfg(
        namespace="loom-staging",
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-staging",
    )

    docs = _load_docs(render_manifests(cfg))
    pvs = {d["metadata"]["name"]: d for d in docs if d["kind"] == "PersistentVolume"}

    assert pvs["loom-staging-postgres-data"]["spec"] == {
        "capacity": {"storage": "50Gi"},
        "accessModes": ["ReadWriteOnce"],
        "persistentVolumeReclaimPolicy": "Retain",
        "storageClassName": "",
        "hostPath": {
            "path": "/data/loom-staging/postgres",
            "type": "DirectoryOrCreate",
        },
        "claimRef": {
            "namespace": "loom-staging",
            "name": "data-loom-postgres-0",
        },
    }
    assert pvs["loom-staging-minio-data"]["spec"]["hostPath"]["path"] == (
        "/data/loom-staging/minio"
    )
    assert pvs["loom-staging-worker-trajectories-data"]["spec"]["hostPath"]["path"] == (
        "/data/loom-staging/trajectories"
    )

    postgres = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-postgres"
    )
    postgres_claim = postgres["spec"]["volumeClaimTemplates"][0]["spec"]
    assert postgres_claim["storageClassName"] == ""
    assert postgres_claim["volumeName"] == "loom-staging-postgres-data"

    minio = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    )
    minio_claim = minio["spec"]["volumeClaimTemplates"][0]["spec"]
    assert minio_claim["storageClassName"] == ""
    assert minio_claim["volumeName"] == "loom-staging-minio-data"

    worker_pvc = next(
        d
        for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["storageClassName"] == ""
    assert worker_pvc["spec"]["volumeName"] == ("loom-staging-worker-trajectories-data")


def test_render_static_host_path_keeps_worker_trajectories_pvc_when_worker_disabled() -> None:
    cfg = ClusterConfig(
        namespace="loom-staging",
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-staging",
    )

    docs = _load_docs(render_manifests(cfg))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}

    assert ("Deployment", "loom-worker") not in kinds_names
    assert ("NetworkPolicy", "loom-worker") not in kinds_names
    assert ("PersistentVolumeClaim", "loom-worker-trajectories") in kinds_names

    worker_pvc = next(
        d
        for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["storageClassName"] == ""
    assert worker_pvc["spec"]["volumeName"] == ("loom-staging-worker-trajectories-data")


def test_render_rejects_unknown_persistent_storage_backend() -> None:
    cfg = ClusterConfig(persistent_storage_backend="local-path")
    with pytest.raises(ValueError, match="persistent_storage_backend"):
        render_manifests(cfg)


def test_render_static_host_path_requires_absolute_host_root() -> None:
    cfg = ClusterConfig(
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="data/loom-staging",
    )
    with pytest.raises(ValueError, match="absolute host path"):
        render_manifests(cfg)


def test_render_worker_max_concurrent_schema_default() -> None:
    """LOOM_WORKER_MAX_CONCURRENT comes from render worker_capacity;
    it must appear exactly once so Kubernetes does not depend on
    duplicate env-var ordering."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    env_list = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    concurrent_entries = [e for e in env_list if e["name"] == "LOOM_WORKER_MAX_CONCURRENT"]
    assert len(concurrent_entries) == 1, (
        f"expected exactly 1 LOOM_WORKER_MAX_CONCURRENT entry, got {len(concurrent_entries)}"
    )
    assert concurrent_entries[0]["value"] == "16"


def test_render_worker_pool_name_schema_default() -> None:
    """Fixed Kubernetes workers should register under an explicit pool."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-worker"
    )
    env_list = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    pool_entries = [e for e in env_list if e["name"] == "LOOM_WORKER_POOL_NAME"]
    assert len(pool_entries) == 1
    assert pool_entries[0]["value"] == "k8s-worker"


def test_render_can_override_service_object_buckets() -> None:
    """Environment-specific cluster configs must not share artifact or
    trajectory buckets by accident when they use the same object-store
    endpoint. Control-plane and workers must receive the same overrides
    so signed-URL uploads and trial object writes land where loom-service
    expects to read them.
    """
    cfg = _default_cfg(
        trajectories_bucket="loom-prod-trajectories",
        artifacts_bucket="loom-prod-artifacts",
    )
    docs = _load_docs(render_manifests(cfg))
    expected = {
        "loom-service": (
            "LOOM_SVC_TRAJECTORIES_BUCKET",
            "LOOM_SVC_ARTIFACTS_BUCKET",
        ),
        "loom-control-plane": (
            "LOOM_CP_TRAJECTORIES_BUCKET",
            "LOOM_CP_ARTIFACTS_BUCKET",
        ),
        "loom-family-orchestrator": (
            "LOOM_CP_TRAJECTORIES_BUCKET",
            "LOOM_CP_ARTIFACTS_BUCKET",
        ),
    }
    for name, (traj_env, art_env) in expected.items():
        workload = next(
            d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == name
        )
        env = workload["spec"]["template"]["spec"]["containers"][0]["env"]
        by_name = {entry["name"]: entry["value"] for entry in env if "value" in entry}
        assert by_name[traj_env] == "loom-prod-trajectories", name
        assert by_name[art_env] == "loom-prod-artifacts", name

    worker = next(
        d
        for d in docs
        if d["metadata"]["name"] == "loom-worker" and d["kind"] in {"Deployment", "StatefulSet"}
    )
    worker_env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    worker_by_name = {entry["name"]: entry["value"] for entry in worker_env if "value" in entry}
    assert worker_by_name["LOOM_WORKER_TRAJECTORIES_BUCKET"] == "loom-prod-trajectories"
    assert worker_by_name["LOOM_WORKER_ARTIFACTS_BUCKET"] == "loom-prod-artifacts"


def test_render_uses_strict_undefined_so_missing_var_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: if a future template references {{ foo }} but
    ClusterConfig doesn't have `foo`, render must raise, not emit
    a manifest with `image: loom-service:` (no tag)."""
    # Inject a sentinel template by patching _TEMPLATE_ORDER to a
    # single name + monkeypatching the resources call. Easier: just
    # call jinja2 directly with StrictUndefined to assert behavior.
    from jinja2 import Environment, StrictUndefined, UndefinedError

    env = Environment(undefined=StrictUndefined)
    with pytest.raises(UndefinedError):
        env.from_string("image: loom:{{ does_not_exist }}").render()


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def test_cli_render_writes_to_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["cluster", "render"])
    assert rc == 0
    out = capsys.readouterr().out
    # Smoke: the output should contain at least the postgres + service
    # manifests as the first/middle workloads.
    assert "loom-postgres" in out
    assert "loom-service" in out
    assert "Ingress" in out


def test_cli_render_with_config_picks_up_overrides(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text('image_tag = "9.9.9"\n', encoding="utf-8")
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom-service:9.9.9" in out


def test_cli_render_missing_config_file_exits_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["cluster", "render", "--config", "/nonexistent.toml"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not found" in err


def test_cli_render_invalid_config_exits_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "bad.toml"
    cfg.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "unknown keys" in err


def test_cli_render_invalid_provider_egress_allowlist_exits_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "bad-provider-egress.toml"
    cfg.write_text(
        'provider_egress_allowlist = ["lux.example.com:18001"]\n',
        encoding="utf-8",
    )
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "provider_egress_allowlist" in err
    assert "IP address or CIDR" in err


def test_render_embeds_config_namespace_in_every_namespaced_object() -> None:
    namespace = "loom-incident-restore"
    rendered = render_manifests(ClusterConfig(namespace=namespace))
    documents = _load_docs(rendered)

    assert documents
    for document in documents:
        if document["kind"] == "PersistentVolume":
            assert "namespace" not in document["metadata"]
        else:
            assert document["metadata"]["namespace"] == namespace


def test_config_target_is_inferred_when_flags_are_omitted(tmp_path: Path) -> None:
    config_path = tmp_path / "staging.cluster.toml"
    config_path.write_text(
        'namespace = "loom-staging"\nruntime_environment = "staging"\n',
        encoding="utf-8",
    )
    args = argparse.Namespace(
        config=str(config_path),
        namespace=None,
        environment=None,
    )

    _resolve_config_target(args)

    assert args.namespace == "loom-staging"
    assert args.environment == "staging"


@pytest.mark.parametrize(
    "command,extra",
    [
        ("status", []),
        ("preflight", ["--no-doctor"]),
        ("up", ["--no-wait"]),
        ("down", ["--yes"]),
    ],
)
@pytest.mark.parametrize(
    "conflicting_flags,expected",
    [
        (["--namespace", "loom-team-b"], "--namespace 'loom-team-b' conflicts"),
        (["--environment", "local"], "--environment 'local' conflicts"),
    ],
)
def test_cluster_commands_reject_explicit_config_target_conflicts_before_cluster_access(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
    extra: list[str],
    conflicting_flags: list[str],
    expected: str,
) -> None:
    config_path = tmp_path / "team-a.cluster.toml"
    config_path.write_text(
        'namespace = "loom-team-a"\nruntime_environment = "development"\n',
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            command,
            "--config",
            str(config_path),
            *conflicting_flags,
            *extra,
        ]
    )

    assert rc == 2
    assert expected in capsys.readouterr().err


# ──────────────────────────────────────────────────────────────────────
# #383: k8s_worker.enabled toggle
# ──────────────────────────────────────────────────────────────────────


def test_default_config_disables_k8s_worker() -> None:
    """Local cluster workers require an explicit opt-in."""
    cfg = ClusterConfig()
    assert cfg.k8s_worker.enabled is False


def test_render_omits_worker_deployment_when_disabled() -> None:
    """Default dynamic rendering with k8s_worker.enabled=false must omit
    the whole loom-worker workload (StatefulSet + headless Service +
    trajectories PVC + NetworkPolicy), not merely scale it to zero
    replicas — belt-and-suspenders against `kubectl scale` drift."""
    docs = _load_docs(render_manifests(ClusterConfig()))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("Deployment", "loom-worker") not in kinds_names
    assert ("StatefulSet", "loom-worker") not in kinds_names
    assert ("Service", "loom-worker") not in kinds_names
    assert (
        "PersistentVolumeClaim",
        "loom-worker-trajectories",
    ) not in kinds_names
    assert ("NetworkPolicy", "loom-worker") not in kinds_names


def test_render_includes_worker_when_enabled_via_profile() -> None:
    """The local render config opts into in-cluster workers.

    Dynamic-storage profiles render loom-worker as a StatefulSet with
    per-pod PVCs from volumeClaimTemplates so RWO Longhorn volumes on
    multi-node clusters don't strand additional replicas (#673). The
    headless Service backs stable per-pod DNS.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("StatefulSet", "loom-worker") in kinds_names
    assert ("Service", "loom-worker") in kinds_names
    assert ("NetworkPolicy", "loom-worker") in kinds_names
    # Top-level PVC exists only for static_host_path_storage; dynamic
    # profiles get per-pod PVCs (trajectories-loom-worker-0, ...) via
    # volumeClaimTemplates.
    assert ("PersistentVolumeClaim", "loom-worker-trajectories") not in kinds_names


def test_synthetic_profiles_have_explicit_k8s_worker_setting() -> None:
    """Every synthetic renderer profile in `tests/fixtures/cluster-render/` must
    declare k8s_worker.enabled explicitly — no silent inheritance
    of the schema default. See #383 rationale."""
    envs_dir = _REPO_ROOT / "tests" / "fixtures" / "cluster-render"
    expected = {
        # Hosted identity fixtures omit local workers; the shipped local
        # example explicitly opts into its in-cluster worker.
        "development.cluster.toml": False,
        "staging.cluster.toml": False,
        "staging.multinode.cluster.toml": False,
        "production.cluster.toml": False,
    }
    for filename, want_enabled in expected.items():
        cfg = load_cluster_config(envs_dir / filename)
        assert cfg.k8s_worker.enabled is want_enabled, (
            f"{filename}: expected k8s_worker.enabled={want_enabled}, got {cfg.k8s_worker.enabled}"
        )


def test_loom_service_env_carries_k8s_worker_enabled_from_profile() -> None:
    """The rendered loom-service Deployment must propagate the
    profile's k8s_worker.enabled value into LOOM_SVC_K8S_WORKER_ENABLED
    so the API rejection path (in `_reject_if_k8s_worker_unavailable`)
    knows whether k8s-worker is available on this cluster."""
    for enabled in (True, False):
        k8s_worker_cls = type(ClusterConfig().k8s_worker)
        cfg = ClusterConfig(k8s_worker=k8s_worker_cls(enabled=enabled))
        docs = _load_docs(render_manifests(cfg))
        svc = next(
            d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-service"
        )
        env = svc["spec"]["template"]["spec"]["containers"][0]["env"]
        by_name = {entry["name"]: entry for entry in env}
        assert by_name["LOOM_SVC_K8S_WORKER_ENABLED"]["value"] == str(enabled)


def test_loom_service_env_carries_v1_workload_trust_contract_from_profile() -> None:
    expected = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    for profile_name in (
        "staging.cluster.toml",
        "staging.multinode.cluster.toml",
        "production.cluster.toml",
    ):
        cfg = load_cluster_config(
            _REPO_ROOT / "tests" / "fixtures" / "cluster-render" / profile_name
        )
        assert (
            cfg.workload_contract.workload_trust_mode,
            cfg.workload_contract.taskset_transforms_enabled,
            cfg.workload_contract.taskset_transform_network_isolated,
            cfg.workload_contract.untrusted_workload_isolation,
        ) == ("internal_trusted", False, False, False)
        docs = _load_docs(render_manifests(cfg))
        service = next(
            d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-service"
        )
        env = service["spec"]["template"]["spec"]["containers"][0]["env"]
        by_name = {entry["name"]: entry["value"] for entry in env if "value" in entry}
        assert {name: by_name[name] for name in expected} == expected


# ──────────────────────────────────────────────────────────────────────
# #547 drain hook + HPA
# ──────────────────────────────────────────────────────────────────────


def test_llm_gateway_deployment_includes_drain_prestop_and_grace() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    gateway = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-llm-gateway"
    )
    pod_spec = gateway["spec"]["template"]["spec"]
    assert pod_spec["terminationGracePeriodSeconds"] == 300
    container = pod_spec["containers"][0]
    prestop_command = container["lifecycle"]["preStop"]["exec"]["command"]
    joined = " ".join(str(part) for part in prestop_command)
    assert "127.0.0.1:9100/drain" in joined


def test_default_config_disables_gateway_hpa() -> None:
    assert ClusterConfig().gateway_hpa.enabled is False


def test_render_omits_hpa_when_disabled() -> None:
    docs = _load_docs(render_manifests(ClusterConfig()))
    kinds = {d["kind"] for d in docs}
    assert "HorizontalPodAutoscaler" not in kinds


def test_render_includes_hpa_when_enabled_with_custom_thresholds() -> None:
    hpa_cls = type(ClusterConfig().gateway_hpa)
    cfg = _default_cfg(
        gateway_hpa=hpa_cls(
            enabled=True,
            min_replicas=3,
            max_replicas=10,
            cpu_target_pct=70,
        ),
    )
    docs = _load_docs(render_manifests(cfg))
    hpa = next(
        d
        for d in docs
        if d["kind"] == "HorizontalPodAutoscaler" and d["metadata"]["name"] == "loom-llm-gateway"
    )
    spec = hpa["spec"]
    assert spec["minReplicas"] == 3
    assert spec["maxReplicas"] == 10
    assert spec["metrics"][0]["resource"]["target"]["averageUtilization"] == 70


# ──────────────────────────────────────────────────────────────────────
# #547 item #3: loom-llm-gateway-sandbox
# ──────────────────────────────────────────────────────────────────────


def test_default_config_disables_llm_gateway_sandbox() -> None:
    """Off by default because the DaemonSet needs an operator-
    provisioned TLS Secret (loom-sandbox-gateway-tls)."""
    assert ClusterConfig().llm_gateway_sandbox.enabled is False


def test_render_omits_llm_gateway_sandbox_when_disabled() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("DaemonSet", "loom-llm-gateway-sandbox") not in kinds_names
    assert (
        "NetworkPolicy",
        "loom-llm-gateway-sandbox",
    ) not in kinds_names


def test_render_includes_sandbox_daemonset_when_enabled() -> None:
    sandbox_cls = type(ClusterConfig().llm_gateway_sandbox)
    cfg = _default_cfg(llm_gateway_sandbox=sandbox_cls(enabled=True))
    docs = _load_docs(render_manifests(cfg))

    ds = next(
        d
        for d in docs
        if d["kind"] == "DaemonSet" and d["metadata"]["name"] == "loom-llm-gateway-sandbox"
    )
    pod_spec = ds["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    # Binary points at the intended k8s Service (skipping the socat
    # router — the Go proxy resolves cluster DNS directly).
    args = container["args"]
    upstream = next(a for a in args if a.startswith("--upstream-url="))
    assert "loom-llm-gateway." in upstream
    assert ".svc.cluster.local:9100" in upstream

    # hostPort 8443 is exposed for sandbox Docker containers to dial.
    port = container["ports"][0]
    assert port["containerPort"] == 8443
    assert port["hostPort"] == 8443

    # Both secrets are mounted read-only into /run/loom.
    vol_secret_names = {v["secret"]["secretName"] for v in pod_spec["volumes"] if "secret" in v}
    assert vol_secret_names == {
        "loom-secrets",
        "loom-sandbox-gateway-tls",
    }


def test_render_includes_sandbox_network_policy_when_enabled() -> None:
    """The NetworkPolicy must permit hostPort ingress on 8443 (node-
    local traffic can't be further restricted by k8s NetworkPolicy)
    and egress to the in-cluster llm-gateway + kube-dns only."""
    sandbox_cls = type(ClusterConfig().llm_gateway_sandbox)
    cfg = _default_cfg(llm_gateway_sandbox=sandbox_cls(enabled=True))
    docs = _load_docs(render_manifests(cfg))

    policy = next(
        d
        for d in docs
        if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "loom-llm-gateway-sandbox"
    )
    spec = policy["spec"]
    assert spec["podSelector"] == {
        "matchLabels": {"app": "loom-llm-gateway-sandbox"},
    }
    ingress_ports = {p["port"] for p in spec["ingress"][0]["ports"]}
    assert ingress_ports == {8443}
    egress_pods = {
        target["podSelector"]["matchLabels"].get("app")
        for rule in spec["egress"]
        for target in rule.get("to", [])
        if "podSelector" in target
    }
    assert "loom-llm-gateway" in egress_pods


def test_cluster_audit_exempts_sandbox_hostport() -> None:
    """The auditor's `_HOSTPORT_ALLOWLIST` must include
    loom-llm-gateway-sandbox; otherwise every enabled render would
    flag its hostPort as a boundary violation."""
    from loom_cli.cluster_boundary import _HOSTPORT_ALLOWLIST

    assert "loom-llm-gateway-sandbox" in _HOSTPORT_ALLOWLIST


# ──────────────────────────────────────────────────────────────────────
# container_registry (#TBD) — optional prefix for locally-built loom-*
# images so multi-node clusters can pull from an in-cluster registry
# without a per-node side-channel `ctr images import` step.
# ──────────────────────────────────────────────────────────────────────


def test_container_registry_default_leaves_images_unprefixed() -> None:
    """The default supports unprefixed images for unprotected local rendering."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "StatefulSet")]
    loom_images = [
        d["spec"]["template"]["spec"]["containers"][0]["image"]
        for d in workloads
        if d["spec"]["template"]["spec"]["containers"][0]["image"].startswith("loom-")
    ]
    assert loom_images, "expected at least one loom-* image in default render"
    for img in loom_images:
        assert not img.startswith("192.168."), f"bare loom-* image expected, got {img!r}"
        assert "/" not in img.split(":")[0], f"unprefixed image expected, got {img!r}"


def test_container_registry_prefixes_all_locally_built_images() -> None:
    """Setting container_registry prepends the registry host to every
    locally-built loom-* image. Third-party images (minio, envoy, socat,
    pgbouncer, exporter) stay unchanged — those come from public
    registries and shouldn't be routed through the internal one."""
    registry = "192.168.50.13:5000"
    cfg = _default_cfg(container_registry=registry)
    docs = _load_docs(render_manifests(cfg))
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "StatefulSet")]
    prefixed = 0
    for d in workloads:
        img = d["spec"]["template"]["spec"]["containers"][0]["image"]
        name = d["metadata"]["name"]
        # Third-party images (postgres, minio, pgbouncer, envoy, socat)
        # stay on their upstream registries — the prefix only applies to
        # locally-built loom-* images. Detect "locally built" as an
        # image whose repository begins with `loom-` when the registry
        # is empty.
        if img.startswith("loom-") or img.startswith(f"{registry}/loom-"):
            assert img.startswith(f"{registry}/loom-"), (
                f"{name}: expected registry prefix on locally-built image, got {img!r}"
            )
            prefixed += 1
    assert prefixed >= 3, "expected at least three loom-* workloads to be prefixed"


def test_container_registry_prefixes_migration_job() -> None:
    """render-migration honors --container-registry too so operators
    running Alembic upgrades on multi-node clusters don't have to
    hand-edit the Job image reference."""
    from loom_cli.cluster_migration import render_migration_manifest

    manifest = render_migration_manifest(
        image_tag="staging-abc123",
        namespace="loom-staging",
        job_suffix="test",
        container_registry="192.168.50.13:5000",
    )
    doc = yaml.safe_load(manifest)
    img = doc["spec"]["template"]["spec"]["containers"][0]["image"]
    assert img == "192.168.50.13:5000/loom-control-plane:staging-abc123"


def test_render_migration_cli_pins_registry_manifest_digest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    digest = "sha256:" + "d" * 64

    rc = main(
        [
            "cluster",
            "render-migration",
            "--image-tag",
            "staging-abc123",
            "--namespace",
            "loom-staging",
            "--job-suffix",
            "test",
            "--container-registry",
            "192.168.50.13:5000",
            "--registry-digest",
            digest,
        ]
    )

    assert rc == 0
    doc = yaml.safe_load(capsys.readouterr().out)
    assert (
        doc["spec"]["template"]["spec"]["containers"][0]["image"]
        == f"192.168.50.13:5000/loom-control-plane@{digest}"
    )


def test_container_registry_load_from_toml(tmp_path: Path) -> None:
    """cluster.toml load path accepts container_registry as a top-level
    string field."""
    from loom_cli.cluster_config import load_cluster_config

    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'container_registry = "192.168.50.13:5000"\ncontainer_registry_push = "localhost:5000"\n'
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.container_registry == "192.168.50.13:5000"
    assert cfg.container_registry_push == "localhost:5000"


@pytest.mark.parametrize(
    "contents",
    [
        'container_registry = "192.168.50.13:5000"\n',
        'container_registry_push = "localhost:5000"\n',
        (
            'container_registry = "http://192.168.50.13:5000"\n'
            'container_registry_push = "localhost:5000"\n'
        ),
    ],
)
def test_container_registry_publication_requires_explicit_safe_pair(
    tmp_path: Path,
    contents: str,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(contents)
    with pytest.raises(ValueError, match="container_registry"):
        load_cluster_config(cfg_path)


def test_local_example_template_renders() -> None:
    """The shipped local dev template must actually RENDER, not just load: e.g.
    frontend_api_base_path must be a renderer-valid root/prefix form ("/"), not
    "/api" which `cluster render` rejects. Guards the #882 template against a
    render-invalid value that load_cluster_config alone would not catch."""
    cfg = load_cluster_config(_REPO_ROOT / "deploy/local/local.example.cluster.toml")
    docs = _load_docs(render_manifests(cfg))
    assert docs  # rendered manifests without raising
    assert cfg.runtime_environment == "local"
    assert cfg.frontend_environment == "local"
    for deployment in ("loom-control-plane", "loom-service", "loom-llm-gateway"):
        assert _deployment_env_value(docs, deployment, "LOOM_ENV") == "local"
    assert _deployment_env_value(docs, "loom-web", "LOOM_FRONTEND_ENVIRONMENT") == "local"
    assert cfg.k8s_worker.enabled is True  # default local worker path


def _multi_node_cfg(**kwargs: object) -> ClusterConfig:
    """Build a 4-pod distributed-MinIO render config (#893).

    Uses the synthetic topology values in
    `tests/fixtures/cluster-render/staging.multinode.cluster.toml`. The topology
    sub-dataclass is materialized from the schema at import time, so we
    reach it the same way the single-node tests reach `k8s_worker`
    (`type(ClusterConfig().<field>)`) rather than hand-rolling a config
    mechanism. `persistent_storage_backend="dynamic"` keeps the static
    host-path PV path from colliding with the StatefulSet's PVC.
    """
    topology_cls = type(ClusterConfig().topology)
    return ClusterConfig(
        namespace="loom-staging",
        persistent_storage_backend="dynamic",
        topology=topology_cls(
            multi_node=True,
            minio_replicas=4,
            anti_affinity="required",
            storage_backend="longhorn",
        ),
        **kwargs,
    )


def test_render_distributed_minio_statefulset_shape() -> None:
    """Distributed (multi-node) MinIO renders as a 4-pod HA StatefulSet.

    Covers the live k3s manifest shape: multi_node=true / minio_replicas=4 /
    anti_affinity="required" / storage_backend="longhorn". The synthetic
    config keeps the distributed render path (`minio-distributed.yaml.j2`)
    independently testable in addition to the shipped-profile contract (#893).
    """
    docs = _load_docs(render_manifests(_multi_node_cfg()))

    minio_statefulsets = [
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    ]
    # Exactly one loom-minio StatefulSet — the single-node minio.yaml.j2 is
    # gated on `not topology.multi_node` and must emit nothing here.
    assert len(minio_statefulsets) == 1
    minio = minio_statefulsets[0]

    # 1. distributed StatefulSet with replicas: 4.
    assert minio["spec"]["replicas"] == 4
    # The distributed StatefulSet fronts the headless peer-discovery
    # Service; the single-node one uses serviceName == loom-minio.
    assert minio["spec"]["serviceName"] == "loom-minio-headless"

    # 2. required cross-node anti-affinity keyed on hostname.
    anti_affinity = minio["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]
    assert "requiredDuringSchedulingIgnoredDuringExecution" in anti_affinity
    assert "preferredDuringSchedulingIgnoredDuringExecution" not in anti_affinity
    required_terms = anti_affinity["requiredDuringSchedulingIgnoredDuringExecution"]
    assert required_terms[0]["topologyKey"] == "kubernetes.io/hostname"
    assert required_terms[0]["labelSelector"]["matchLabels"] == {"app": "loom-minio"}

    # 3. peer-discovery arg spans the {0...3} ordinal set for 4 pods.
    args = minio["spec"]["template"]["spec"]["containers"][0]["args"]
    peer_arg = next(a for a in args if a.startswith("http://"))
    assert "loom-minio-{0...3}" in peer_arg
    assert "loom-minio-headless.loom-staging.svc.cluster.local:9000/data" in peer_arg

    # 4. the data volumeClaimTemplate binds the Longhorn storage class.
    vct = minio["spec"]["volumeClaimTemplates"][0]
    assert vct["metadata"]["name"] == "data"
    assert vct["spec"]["storageClassName"] == "longhorn"

    # 5. PodDisruptionBudget preserves erasure quorum (minAvailable: 3).
    pdb = next(
        d
        for d in docs
        if d["kind"] == "PodDisruptionBudget" and d["metadata"]["name"] == "loom-minio"
    )
    assert pdb["spec"]["minAvailable"] == 3
    assert pdb["spec"]["selector"]["matchLabels"] == {"app": "loom-minio"}

    # Headless peer-discovery Service is present alongside the client Service.
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("Service", "loom-minio-headless") in kinds_names
    assert ("Service", "loom-minio") in kinds_names


def test_render_distributed_minio_omits_single_node_shape() -> None:
    """Multi-node mode must not co-render the single-node MinIO StatefulSet
    or a static host-path PV (#893).

    The single-node `minio.yaml.j2` is gated on `not topology.multi_node`;
    with `persistent_storage_backend="dynamic"` no `*-minio-data` host-path
    PersistentVolume is emitted to collide with the StatefulSet's dynamic PVC.
    """
    docs = _load_docs(render_manifests(_multi_node_cfg()))

    # The single-node StatefulSet serves args ["server", "/data", ...]; the
    # distributed one uses a peer-discovery URL. No StatefulSet may carry the
    # single-node arg shape.
    minio_statefulsets = [
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    ]
    assert len(minio_statefulsets) == 1
    assert minio_statefulsets[0]["spec"]["template"]["spec"]["containers"][0]["args"][:2] != [
        "server",
        "/data",
    ]

    # No static host-path PV under the dynamic backend.
    pv_names = {d["metadata"]["name"] for d in docs if d["kind"] == "PersistentVolume"}
    assert "loom-minio-data" not in pv_names
    assert "loom-staging-minio-data" not in pv_names


def test_single_node_minio_pins_ifnotpresent_pull_policy() -> None:
    """The single-node MinIO's `minio_image` is commonly untagged (→
    `:latest`), whose default pull policy is Always — which ErrImagePulls
    on a cached/offline kind/k3d node. The single-node template must pin
    IfNotPresent. (Distributed staging uses minio-distributed.yaml.j2.)"""
    from importlib import resources

    text = (resources.files("loom_cli.templates.k8s") / "minio.yaml.j2").read_text()
    assert "imagePullPolicy: IfNotPresent" in text


def test_local_render_omits_retired_external_worker_transports() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    names = {doc["metadata"]["name"] for doc in docs}
    assert not {"loom-worker-router", "loom-minio-router"} & names
    # Local Docker sandboxes still need their Gateway transport.
    assert "loom-gateway-router" in names
    policies = [doc for doc in docs if doc["kind"] == "NetworkPolicy"]
    assert "loom-worker-router" not in str(policies)
    assert "loom-minio-router" not in str(policies)
