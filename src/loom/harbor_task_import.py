"""Shared Harbor-native task translation; no benchmark-specific runtime policy."""

from __future__ import annotations

import re
from copy import deepcopy
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any

from loom.models.task import EnvironmentConfig

DEFAULT_HARBOR_DOCKERFILE = "environment/Dockerfile"
DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT = "environment"
DEFAULT_VERIFIER_SCRIPT_PATH = "/app/verifier/run.sh"
_HARBOR_ENV_MODES = {"shared", "separate"}
_HARBOR_VERIFIER_ARTIFACT_GLOB = "logs/verifier/**"
_RESOURCE_SIZE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([MGT])(?:I?B)?", re.IGNORECASE)


def project_harbor_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Project a Harbor-native task into Loom's runnable schema.

    Preserve typed declarations even when the selected runtime cannot execute
    them. Unknown execution fields are rejected with their source field path;
    runtime support is checked separately from import/schema validity.
    """
    payload = deepcopy(payload)
    source_task = payload.get("task")
    if not isinstance(source_task, dict):  # protected by shape detection
        return payload
    source_name = source_task.get("name")
    if not isinstance(source_name, str) or not source_name:
        return payload

    task: dict[str, Any] = {"id": source_task.get("id", source_name), "name": source_name}
    description = source_task.get("description")
    if isinstance(description, str):
        task["description"] = description
    labels = source_task.get("labels")
    if not isinstance(labels, list):
        labels = source_task.get("keywords")
    if not isinstance(labels, list):
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            labels = metadata.get("tags")
    if isinstance(labels, list) and all(isinstance(item, str) for item in labels):
        task["labels"] = list(labels)

    for stamp in ("schema_version", "version"):
        if stamp in payload and (
            not isinstance(payload[stamp], str)
            or payload[stamp] not in {"1", "1.0", "1.1", "1.2", "1.3", "2.0"}
        ):
            raise ValueError(f"unsupported Harbor requirement: {stamp}={payload[stamp]!r}")
    allowed_root = {
        "task",
        "environment",
        "agent",
        "verifier",
        "artifacts",
        "metadata",
        "solution",
        "schema_version",
        "version",
        "upstream_origin",
        "upstream_task_id",
        "required_agent_capabilities",
    }
    _require_known_fields(payload, allowed_root, "task.toml")
    for section, allowed in (
        (
            "agent",
            {
                "name",
                "version",
                "model",
                "timeout_sec",
                "setup_timeout_sec",
                "user",
                "extra_mcp_servers",
                "skills",
                "continue_until_timeout",
            },
        ),
        (
            "verifier",
            {
                "name",
                "args",
                "timeout_sec",
                "env_mode",
                "environment_mode",
                "user",
                "env",
                "environment",
                "collect",
            },
        ),
    ):
        if section in payload:
            _require_known_fields(payload[section], allowed, section)
    solution = payload.get("solution", {})
    if solution:
        _require_known_fields(solution, {"env"}, "solution")

    source_environment = payload.get("environment")
    if source_environment is not None and not isinstance(source_environment, dict):
        raise ValueError("Harbor environment must be a table")
    source_environment = source_environment or {}
    _require_known_fields(
        source_environment,
        set(EnvironmentConfig.model_fields)
        | {"architecture", "allow_internet", "network_mode", "env", "memory", "storage"},
        "environment",
    )
    _normalize_resource_sizes(source_environment)
    environment: dict[str, Any] = {
        "os": source_environment.get("os", "linux"),
        # Harbor-native images and the verifier bridge use /app. Without this
        # explicit projection Loom defaults to /workspace while the normalized
        # script verifier still points at /app/verifier/run.sh.
        "workdir": source_environment.get("workdir", "/app"),
    }
    for field in EnvironmentConfig.model_fields:
        if field in source_environment:
            environment[field] = deepcopy(source_environment[field])
    if "dockerfile" not in environment and "docker_image" not in environment:
        environment["dockerfile"] = DEFAULT_HARBOR_DOCKERFILE
        environment.setdefault(
            "docker_build_context",
            DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT,
        )
    architecture = source_environment.get("architecture")
    if architecture in {"x86_64", "arm64", "any"}:
        environment["cpu_arch"] = architecture
    elif architecture == "amd64":
        environment["cpu_arch"] = "x86_64"
    elif architecture is not None:
        raise ValueError("unsupported Harbor requirement: environment.architecture")
    source_env = source_environment.get("environment")
    if not isinstance(source_env, dict):
        source_env = source_environment.get("env")
    if source_env is not None and not isinstance(source_env, dict):
        raise ValueError("Harbor environment.env must be a table")
    if isinstance(source_env, dict):
        environment["environment"] = deepcopy(source_env)
    allow_internet = source_environment.get("allow_internet")
    _normalize_internet_declaration(
        environment,
        allow_internet,
        source_environment.get("network_mode"),
    )
    gpus = source_environment.get("gpus")
    if isinstance(gpus, int) and gpus > 0 and "gpu_vendor" not in environment:
        environment["gpu_vendor"] = "nvidia"

    source_agent = payload.get("agent")
    source_agent = source_agent if isinstance(source_agent, dict) else {}
    agent: dict[str, Any] = {"name": source_agent.get("name", "oracle")}
    for field in (
        "version",
        "model",
        "timeout_sec",
        "setup_timeout_sec",
        "user",
        "extra_mcp_servers",
        "skills",
        "continue_until_timeout",
    ):
        if field in source_agent:
            agent[field] = deepcopy(source_agent[field])

    source_verifier = payload.get("verifier")
    source_verifier = source_verifier if isinstance(source_verifier, dict) else {}
    verifier: dict[str, Any] = {
        "name": source_verifier.get("name", "script"),
        "args": deepcopy(source_verifier.get("args", {})),
    }
    if not isinstance(verifier["args"], dict):
        raise ValueError("Harbor verifier.args must be a table")
    verifier["args"].setdefault("script_path", DEFAULT_VERIFIER_SCRIPT_PATH)
    for field in ("timeout_sec", "env_mode", "user"):
        if field in source_verifier:
            verifier[field] = deepcopy(source_verifier[field])
    if (
        "env_mode" in source_verifier
        and "environment_mode" in source_verifier
        and source_verifier["env_mode"] != source_verifier["environment_mode"]
    ):
        raise ValueError("verifier.env_mode conflicts with verifier.environment_mode")
    if "env_mode" not in verifier:
        environment_mode = source_verifier.get("environment_mode")
        if environment_mode in _HARBOR_ENV_MODES:
            verifier["env_mode"] = environment_mode
        elif environment_mode is not None:
            raise ValueError("unsupported Harbor requirement: verifier.environment_mode")

    if source_verifier.get("environment") is not None:
        nested = source_verifier["environment"]
        if not isinstance(nested, dict):
            raise ValueError("verifier.environment must be a table")
        projected = project_harbor_task({"task": {"name": source_name}, "environment": nested})
        verifier["environment"] = projected["environment"]
        verifier.setdefault("env_mode", "separate")
    if source_verifier.get("env"):
        verifier["environment_vars"] = deepcopy(source_verifier["env"])
    if source_verifier.get("collect"):
        verifier["collect"] = deepcopy(source_verifier["collect"])

    artifacts = payload.get("artifacts")
    relative_artifacts: list[str] = []
    artifact_sources: list[dict[str, Any]] = []
    if artifacts is not None and not isinstance(artifacts, list):
        raise ValueError("artifacts must be a list")
    for item in artifacts or []:
        if isinstance(item, str) and not PurePosixPath(item).is_absolute():
            if not item or ".." in PurePosixPath(item).parts or "\\" in item or "\x00" in item:
                raise ValueError("artifacts cannot contain traversal")
            relative_artifacts.append(item)
        elif isinstance(item, str):
            artifact_sources.append({"source": item})
        elif isinstance(item, dict):
            artifact_sources.append(deepcopy(item))
        else:
            raise ValueError("artifacts require paths or structured source tables")
    if _HARBOR_VERIFIER_ARTIFACT_GLOB not in relative_artifacts:
        relative_artifacts.append(_HARBOR_VERIFIER_ARTIFACT_GLOB)
    steps: list[dict[str, Any]] = [{"name": "main", "artifacts": relative_artifacts}]
    if artifact_sources:
        steps[0]["artifact_sources"] = artifact_sources

    result = {
        "schema_version": "1",
        "task": task,
        "environment": environment,
        "agent": agent,
        "verifier": verifier,
        "steps": steps,
    }
    for key in ("upstream_origin", "upstream_task_id", "required_agent_capabilities"):
        if key in payload:
            result[key] = deepcopy(payload[key])
    if solution.get("env"):
        result["solution_environment"] = deepcopy(solution["env"])
    return result


def _require_known_fields(value: Any, allowed: set[str], section: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"Harbor {section} must be a table")
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(
            "unsupported Harbor requirement: " + ", ".join(f"{section}.{key}" for key in unknown)
        )


def _normalize_resource_sizes(environment: dict[str, Any]) -> None:
    """Harbor legacy G/M quantities use the same binary units as *_mb fields."""
    for field in ("memory", "storage"):
        if field not in environment:
            continue
        value = environment[field]
        match = _RESOURCE_SIZE.fullmatch(value.strip()) if isinstance(value, str) else None
        if match is None:
            raise ValueError(
                f"environment.{field} requires a positive M/G/T size (for example '2G')"
            )
        amount = Decimal(match.group(1)) * {"M": 1, "G": 1024, "T": 1024**2}[match.group(2).upper()]
        if amount <= 0 or amount != amount.to_integral_value():
            raise ValueError(f"environment.{field} must resolve to a positive whole number of MiB")
        target = f"{field}_mb"
        if target in environment and environment[target] != int(amount):
            raise ValueError(f"environment.{field} conflicts with environment.{target}")
        environment[target] = int(amount)
        del environment[field]


def _normalize_internet_declaration(
    environment: dict[str, Any],
    allow_internet: Any,
    network_mode: Any = None,
) -> None:
    """Map Harbor internet declarations onto hosted dialer policy.

    An existing web-allowlist is kept. ``true`` and ``network_mode = public``
    become public HTTP(S) through the gateway dialer. ``false`` and
    ``no-network`` stay offline. Omission leaves the policy untouched.
    """
    existing = environment.get("baseline_network_policy")
    if isinstance(existing, dict) and existing.get("kind") == "web-allowlist":
        if allow_internet not in {None, True, False}:
            raise ValueError("Terminal-Bench environment.allow_internet must be boolean")
        return
    if allow_internet not in {None, True, False}:
        raise ValueError("Terminal-Bench environment.allow_internet must be boolean")
    if network_mode not in {None, "public", "no-network", "allowlist"}:
        raise ValueError("environment.network_mode is unsupported")
    if allow_internet is False or network_mode == "no-network":
        environment["network_policies_supported"] = ["no-network"]
        environment["baseline_network_policy"] = {"kind": "no-network"}
        return
    if allow_internet is True or network_mode == "public":
        environment["network_policies_supported"] = ["public-web"]
        environment["baseline_network_policy"] = {"kind": "public-web"}
