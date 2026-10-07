"""Private, execution-closed shared development foundation templates.

This is not an installer or an upgrade/closure operation on an existing platform.
A protected installer must first qualify a fresh dev-owned namespace/database,
independent credentials and physical system-node headroom. No public routing or
shared-pool activation is emitted, and ordinary standalone rollout cannot consume
this bundle.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.nebius_environment_render import PlatformEnvelope, _envelope
from loom.nebius_platform_render import (
    NebiusPlatformError,
    _build_platform,
    _env,
    _namespace,
    _network_policy,
    _peer,
    _secret_env,
    _service,
    canonical,
    digest,
    validate_environment,
)

DEVELOPMENT_BOOTSTRAP_CONFIG = {
    "schema_version": "loom.nebius-development-bootstrap.v1",
    "namespace": "loom-dev",
    "environment": "development",
}


@dataclass(frozen=True)
class RenderedDevelopmentFoundation:
    files: dict[str, list[dict[str, Any]]]
    revision: str
    platform_envelope: PlatformEnvelope


def render_development_foundation(
    config: dict[str, Any], candidate: dict[str, Any], profile: dict[str, Any],
    keyring: dict[str, Any], *, repo_root: Path,
) -> RenderedDevelopmentFoundation:
    """Reuse system templates without inheriting their execution or public authority.

    Input platform settings describe the intended foundation, not permission to
    install its execution configuration. Candidate verification and namespace/DB
    ownership checks remain the protected installer's responsibility.
    """
    if (config.get("namespace") != "loom-dev" or config.get("environment") != "development"
            or config.get("schema_version") != "loom.nebius-platform.v1"):
        raise NebiusPlatformError("private foundation requires canonical shared development")
    validate_environment(config)
    if candidate.get("source_ref") != "refs/heads/dev":
        raise NebiusPlatformError("private foundation requires a protected dev publication")
    revision = digest({"bootstrap": DEVELOPMENT_BOOTSTRAP_CONFIG, "config": config,
                       "candidate": candidate, "profile": profile, "keyring": keyring})
    template_config = deepcopy(config)
    for field in ("task_image_builder", "task_identity_policy", "guest_execution_target",
                  "emulated_auth_execution_target", "regional_execution_targets", "task_egress"):
        template_config.pop(field, None)
    # Templates need published image identity, not execution readiness. Neither
    # this reduced template profile nor a task catalog is exposed by the result.
    template_profile = {key: profile[key] for key in (
        "candidate_sha", "task_image_ref", "runtime_image_ref", "agent_image_ref",
    ) if key in profile}
    files = _build_platform(template_config, candidate, template_profile, keyring,
                            repo_root=repo_root, execution_enabled=False)
    ns = "loom-dev"
    cm = next(doc for doc in files["10-config-network.yaml"] if doc["kind"] == "ConfigMap")
    cm["data"] = {"environment.json": canonical(DEVELOPMENT_BOOTSTRAP_CONFIG).decode()}
    account = next(doc for doc in files["10-config-network.yaml"] if doc["kind"] == "ServiceAccount")
    files = {
        "00-namespaces.yaml": [_namespace(ns)],
        "10-config-network.yaml": [cm, account,
            _network_policy("default-deny-ingress", ns, {}, []),
            _network_policy("development-internal", ns, {}, [{"from": [_peer(ns)],
                "ports": [{"protocol": "TCP", "port": port} for port in (5432, 8080, 8090, 9100)]}]),
        ],
        "20-database.yaml": files["20-database.yaml"],
        "30-migrate.yaml": files["30-migrate.yaml"],
        "40-services.yaml": [doc for doc in files["40-services.yaml"]
                             if doc["kind"] == "Deployment" or (
                                 doc["kind"] == "Service" and doc["metadata"]["name"] != "loom-web-origin")],
    }
    files["40-services.yaml"].append(_service("loom-web", ns, 8080))
    for doc in files["40-services.yaml"]:
        if doc["kind"] != "Deployment":
            continue
        pod = doc["spec"]["template"]["spec"]
        pod["containers"] = pod["containers"][:1]
        container, = pod["containers"]
        env = container["env"]
        name = doc["metadata"]["name"]
        if name == "loom-web":
            pod.pop("volumes", None)
            for row in env:
                if row["name"] == "LOOM_FRONTEND_ENVIRONMENT_LABEL":
                    row["value"] = "Shared development (private bootstrap)"
        elif name == "loom-service":
            container["env"] = [row for row in env if row["name"] not in {
                "LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON",
            }] + _env({"LOOM_SVC_SERVICE_MODE": "api_only", "LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON": "{}"})
        elif name == "loom-control-plane":
            for row in env:
                if row["name"] in {"LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED",
                                   "LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED"}:
                    row["value"] = "false"
        elif name == "loom-llm-gateway":
            container["env"] = [row for row in env if not row["name"].startswith("LOOM_GW_LOCAL_")]
    migration, = files["30-migrate.yaml"]
    migration["metadata"]["name"] = "loom-development-migrate-" + revision[7:19]
    pod = migration["spec"]["template"]["spec"]
    pod.pop("initContainers", None)
    pod["volumes"] = [volume for volume in pod["volumes"] if volume["name"] in {"platform-config", "db-ca"}]
    container, = pod["containers"]
    container["command"] = ["python", "-m", "loom.nebius_platform_bootstrap", "development-database"]
    container["env"] = [
        {"name": "LOOM_PLATFORM_CONFIG", "value": "/var/run/loom-platform/environment.json"},
        _secret_env("LOOM_DB_URL", "loom-platform-db", "admin-url"),
        *[_secret_env("LOOM_DB_" + role.upper().replace("-", "_") + "_PASSWORD",
                      "loom-platform-db", role + "-password") for role in ("service", "control-plane", "gateway")],
    ]
    container["volumeMounts"] = [volume for volume in container["volumeMounts"]
                                 if volume["name"] in {"platform-config", "db-ca"}]
    for docs in files.values():
        for doc in docs:
            doc["metadata"].setdefault("labels", {})["loom.nebius/development-phase"] = "private-bootstrap"
            if doc["kind"] not in {"Deployment", "StatefulSet", "Job"}:
                continue
            template = doc["spec"]["template"]
            template["metadata"]["annotations"]["loom.nebius/configuration-revision"] = revision
            for container in template["spec"].get("initContainers", []) + template["spec"]["containers"]:
                for kind in ("requests", "limits"):
                    container["resources"][kind]["ephemeral-storage"] = "256Mi"
    return RenderedDevelopmentFoundation(files, revision, _envelope(files))
