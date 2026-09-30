"""Normalize Terminal-Bench-style ``task.toml`` files to Loom TaskConfig.

The 5003-task Source Useful bundle and similar Terminal-Bench imports ship
``task.toml`` files with top-level ``metadata`` instead of Loom's ``task``
section. Harbor-native Terminal-Bench 2.1 / 3 / 4 packages use a ``[task]``
section with an upstream name but no Loom task id. The worker stores a Loom
``TaskConfig`` in the DB, while preserving the uploaded bundle files for audit
and verifier/runtime use.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from loom.harbor_task_import import (
    DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT,
    DEFAULT_HARBOR_DOCKERFILE,
    _normalize_internet_declaration,
    _normalize_resource_sizes,
)

DEFAULT_AGENT_TIMEOUT_SEC = 360.0
DEFAULT_VERIFIER_TIMEOUT_SEC = 60.0
DEFAULT_VERIFIER_SCRIPT_PATH = "/app/verifier/run.sh"



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


def _normalize_harbor_native_task_toml(payload: dict[str, Any]) -> dict[str, Any]:
    from loom.harbor_task_import import project_harbor_task

    return project_harbor_task(payload)


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
