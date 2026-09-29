"""Normalize Terminal-Bench-style ``task.toml`` files to Loom TaskConfig.

The 5003-task Source Useful bundle and similar Terminal-Bench imports ship
``task.toml`` files with top-level ``metadata`` instead of Loom's ``task``
section. Harbor-native Terminal-Bench 2.1 / 3 / 4 packages use a ``[task]``
section with an upstream name but no Loom task id. The worker stores a Loom
``TaskConfig`` in the DB, while preserving the uploaded bundle files for audit
and verifier/runtime use.
"""

from __future__ import annotations

import re
from copy import deepcopy
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any

DEFAULT_AGENT_TIMEOUT_SEC = 360.0
DEFAULT_VERIFIER_TIMEOUT_SEC = 60.0
DEFAULT_VERIFIER_SCRIPT_PATH = "/app/verifier/run.sh"
DEFAULT_HARBOR_DOCKERFILE = "environment/Dockerfile"
DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT = "environment"
_HARBOR_VERIFIER_ARTIFACT_GLOB = "logs/verifier/**"
_HARBOR_ENV_MODES = frozenset({"shared", "separate"})

_RESOURCE_SIZE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([MGT])(?:I?B)?", re.IGNORECASE)


def is_terminal_bench_shape(raw: dict[str, Any]) -> bool:
    """True if ``raw`` looks like a Terminal-Bench-style task.toml."""
    if isinstance(raw.get("metadata"), dict) and "task" not in raw:
        return True
    # Harbor-native Terminal-Bench packages (2.1 / 3 / 4) declare an upstream
    # ``[task].name`` without Loom's ``task.id``. Do not require a specific
    # ``schema_version`` stamp; TB3/TB4 often omit ``1.1`` and still carry
    # ``[metadata]`` alongside ``[task]``.
    return _is_harbor_native_task(raw)


def _is_harbor_native_task(raw: dict[str, Any]) -> bool:
    task = raw.get("task")
    if not isinstance(task, dict):
        return False
    if "id" in task and not (
        isinstance(raw.get("metadata"), dict)
        and (raw.get("version") == "1.0" or raw.get("schema_version") == "1.1")
    ):
        return False
    name = task.get("name")
    return isinstance(name, str) and bool(name)


def normalize_terminal_bench_task_toml(
    raw: dict[str, Any], *, task_id: str | None = None,
) -> dict[str, Any]:
    """Return a Loom-TaskConfig-shaped dict derived from a TB-shaped source.

    Idempotent for already-Loom-shaped inputs; the input object is never
    mutated. ``task_id`` supplies deterministic intake identity only when the
    Harbor source omits one; it never replaces authored task identity.
    """
    payload = deepcopy(raw)
    if not is_terminal_bench_shape(payload):
        return payload

    if _is_harbor_native_task(payload):
        return _normalize_harbor_native_task_toml(payload)

    metadata = payload.pop("metadata")
    payload.pop("version", None)
    payload["schema_version"] = "1"

    task_section: dict[str, Any] = {}
    if "id" in metadata:
        task_section["id"] = metadata["id"]
    elif task_id:
        task_section["id"] = task_id
    if "name" in metadata:
        task_section["name"] = metadata["name"]
    elif "id" in task_section:
        task_section["name"] = task_section["id"]
    if "description" in metadata:
        task_section["description"] = metadata["description"]
    tags = metadata.get("tags")
    if isinstance(tags, list) and all(isinstance(t, str) for t in tags):
        task_section["labels"] = list(tags)
    payload["task"] = task_section

    environment = payload.get("environment")
    if isinstance(environment, dict):
        environment.setdefault("os", "linux")
    else:
        environment = {"os": "linux"}
        payload["environment"] = environment
    _normalize_resource_sizes(environment)
    environment.setdefault("workdir", "/app")
    if "dockerfile" not in environment and "docker_image" not in environment:
        environment["dockerfile"] = DEFAULT_HARBOR_DOCKERFILE
        environment.setdefault("docker_build_context", DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT)
    if "allow_internet" in environment or "network_mode" in environment:
        _normalize_internet_declaration(
            environment,
            environment.pop("allow_internet", None),
            environment.pop("network_mode", None),
        )

    agent = payload.get("agent")
    if not isinstance(agent, dict):
        payload["agent"] = {
            "name": "oracle",
            "timeout_sec": DEFAULT_AGENT_TIMEOUT_SEC,
        }
    else:
        agent.setdefault("name", "oracle")
        agent.setdefault("timeout_sec", DEFAULT_AGENT_TIMEOUT_SEC)

    verifier = payload.get("verifier")
    if not isinstance(verifier, dict):
        payload["verifier"] = {
            "name": "script",
            "timeout_sec": DEFAULT_VERIFIER_TIMEOUT_SEC,
            "args": {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH},
        }
    else:
        verifier.setdefault("name", "script")
        verifier.setdefault("timeout_sec", DEFAULT_VERIFIER_TIMEOUT_SEC)
        args = verifier.get("args")
        if not isinstance(args, dict):
            verifier["args"] = {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH}
        else:
            args.setdefault("script_path", DEFAULT_VERIFIER_SCRIPT_PATH)

    return payload


def _normalize_resource_sizes(environment: dict[str, Any]) -> None:
    """Harbor legacy G/M quantities use the same binary units as *_mb fields."""
    for field in ("memory", "storage"):
        if field not in environment:
            continue
        value = environment[field]
        match = _RESOURCE_SIZE.fullmatch(value.strip()) if isinstance(value, str) else None
        if match is None:
            raise ValueError(f"environment.{field} requires a positive M/G/T size (for example '2G')")
        amount = Decimal(match.group(1)) * {"M": 1, "G": 1024, "T": 1024 ** 2}[match.group(2).upper()]
        if amount <= 0 or amount != amount.to_integral_value():
            raise ValueError(f"environment.{field} must resolve to a positive whole number of MiB")
        target = f"{field}_mb"
        if target in environment and environment[target] != int(amount):
            raise ValueError(f"environment.{field} conflicts with environment.{target}")
        environment[target] = int(amount)
        del environment[field]


def _normalize_internet_declaration(
    environment: dict[str, Any], allow_internet: Any, network_mode: Any = None,
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


def _normalize_harbor_native_task_toml(payload: dict[str, Any]) -> dict[str, Any]:
    """Project a Harbor-native Terminal-Bench task into Loom's runnable schema.

    Native-only resource, internet, architecture, verifier-env, and solution
    metadata is deliberately not invented in the Loom projection. Callers that
    need the untouched source keep it as ``upstream-task.toml``.
    """
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

    source_environment = payload.get("environment")
    source_environment = source_environment if isinstance(source_environment, dict) else {}
    _normalize_resource_sizes(source_environment)
    environment: dict[str, Any] = {
        "os": source_environment.get("os", "linux"),
        # Harbor-native images and the verifier bridge use /app. Without this
        # explicit projection Loom defaults to /workspace while the normalized
        # script verifier still points at /app/verifier/run.sh.
        "workdir": source_environment.get("workdir", "/app"),
    }
    for field in (
        "cpu_arch",
        "gpu_vendor",
        "docker_image",
        "dockerfile",
        "docker_build_context",
        "extra_hosts",
        "dns",
        "tmpfs",
        "healthcheck",
        "workdir",
        "mutable_paths",
        "mutable_path_reference_files",
        "workspace_reference_files",
        "reference_file_symlinks",
        "preserve_acls",
        "service_lifecycle",
        "execution_requirements",
        "user",
        "network_policies_supported",
        "baseline_network_policy",
        "skills_dir",
        "mcp_servers",
        "build_timeout_sec",
        "cpus",
        "memory_mb",
        "storage_mb",
        "gpus",
        "sidecars",
    ):
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
    source_env = source_environment.get("environment")
    if not isinstance(source_env, dict):
        source_env = source_environment.get("env")
    if isinstance(source_env, dict):
        environment["environment"] = {str(key): str(value) for key, value in source_env.items()}
    allow_internet = source_environment.get("allow_internet")
    _normalize_internet_declaration(
        environment, allow_internet, source_environment.get("network_mode"),
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
        verifier["args"] = {}
    verifier["args"].setdefault("script_path", DEFAULT_VERIFIER_SCRIPT_PATH)
    for field in ("timeout_sec", "env_mode", "user"):
        if field in source_verifier:
            verifier[field] = deepcopy(source_verifier[field])
    if "env_mode" not in verifier:
        environment_mode = source_verifier.get("environment_mode")
        if environment_mode in _HARBOR_ENV_MODES:
            verifier["env_mode"] = environment_mode

    artifacts = payload.get("artifacts")
    source_artifacts = (
        list(artifacts)
        if isinstance(artifacts, list) and all(isinstance(item, str) for item in artifacts)
        else []
    )
    relative_artifacts = [
        item
        for item in source_artifacts
        if not PurePosixPath(item).is_absolute() and ".." not in PurePosixPath(item).parts
    ]
    if _HARBOR_VERIFIER_ARTIFACT_GLOB not in relative_artifacts:
        relative_artifacts.append(_HARBOR_VERIFIER_ARTIFACT_GLOB)
    steps = [{"name": "main", "artifacts": relative_artifacts}]

    return {
        "schema_version": "1",
        "task": task,
        "environment": environment,
        "agent": agent,
        "verifier": verifier,
        "steps": steps,
    }


# Back-compat alias for callers/tests that still name the TB2.1 projector.
_normalize_native_tb21_task_toml = _normalize_harbor_native_task_toml


__all__ = [
    "DEFAULT_AGENT_TIMEOUT_SEC",
    "DEFAULT_HARBOR_DOCKERFILE",
    "DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT",
    "DEFAULT_VERIFIER_SCRIPT_PATH",
    "DEFAULT_VERIFIER_TIMEOUT_SEC",
    "is_terminal_bench_shape",
    "normalize_terminal_bench_task_toml",
]
