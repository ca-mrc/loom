"""Opt-in Nebius Terminus ingest adaptations for Harbor/TB packs (#1996).

Keeps ``automatic_service_execution_rejections`` contracts unchanged. When
``publish-local --execution-profile nebius-terminus`` (or validate-local with
the same flag) is set, adapt staged bundles + Loom-schema config so admission
reasons become satisfiable without silently rewriting every publish.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from loom.harbor_verifier_script import (
    VERIFIER_SCRIPT_PATH as VERIFIER_SCRIPT_PATH,
)
from loom.harbor_verifier_script import (
    offline_verifier_run_sh_bytes as offline_verifier_run_sh_bytes,
)
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.mutable_paths import validate_task_workdir
from loom.nebius_terminus_image import prepare_nebius_terminus_image
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
)

NEBIUS_TERMINUS_PROFILE = "nebius-terminus"
KNOWN_EXECUTION_PROFILES: frozenset[str] = frozenset({NEBIUS_TERMINUS_PROFILE})

DEFAULT_CPUS = 1
DEFAULT_MEMORY_MB = 2048
DEFAULT_STORAGE_MB = 4096
_GLOB_MAGIC = re.compile(r"[][*?]")


class UnsupportedComposeEnvironmentError(ValueError):
    """The task's packaged service topology has no equivalent profile mapping."""

    def __init__(self, relative_paths: tuple[str, ...]) -> None:
        self.relative_paths = relative_paths
        super().__init__(
            "nebius-terminus: packaged Docker Compose configuration is unsupported: "
            + ", ".join(relative_paths)
            + "; preserve its service images, mounts, network aliases, health checks and dependencies"
        )


# Exact previous generated wrapper, before failed-exit rewards were retained.
# Unknown custom scripts must still require an explicit reviewed adaptation.
_LEGACY_OFFLINE_WRAPPER_SHA256 = "652a23b2adc7c088d149b895e286a1541ef6d1ad0eeeea6252c67c82ece861ae"

_HARBOR_BRIDGE_MARKERS = (
    b"harbor loom bridge",
    b"tests/test.sh",
)
_OFFLINE_MARKER = b"/opt/verifier/bin/pytest"


@dataclass(frozen=True)
class NebiusTerminusAdaptStats:
    resources_filled: bool = False
    network_forced_gateway_only: bool = False
    verifier_identity_stripped: bool = False
    verifier_path_forced: bool = False
    cpu_arch_forced: bool = False
    workspace_identity_forced: bool = False
    verifier_wrapper_installed: bool = False
    artifact_globs_stripped: bool = False


def resolve_execution_profile(value: str | None) -> str | None:
    """Return a known profile name, or raise ``ValueError`` for unknowns."""

    if value is None or value == "":
        return None
    if value not in KNOWN_EXECUTION_PROFILES:
        known = ", ".join(sorted(KNOWN_EXECUTION_PROFILES))
        raise ValueError(
            f"unknown execution profile {value!r}; known profiles: {known}",
        )
    return value


def _needs_offline_verifier_wrapper(existing: bytes | None) -> bool:
    if existing is None:
        return True
    if existing == offline_verifier_run_sh_bytes():
        return False
    if hashlib.sha256(existing).hexdigest() == _LEGACY_OFFLINE_WRAPPER_SHA256:
        return True
    if _OFFLINE_MARKER in existing:
        return True
    if any(marker in existing for marker in _HARBOR_BRIDGE_MARKERS):
        return True
    raise ValueError("nebius-terminus cannot replace a custom verifier; provide a reviewed native adaptation")


def _ensure_offline_verifier_wrapper(staged: Path) -> bool:
    target = staged / "verifier" / "run.sh"
    existing = target.read_bytes() if target.is_file() else None
    if not _needs_offline_verifier_wrapper(existing):
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(offline_verifier_run_sh_bytes())
    target.chmod(0o755)
    return True


def _is_exact_relative_artifact(path: str) -> bool:
    posix = PurePosixPath(path)
    return not (
        posix.is_absolute()
        or ".." in posix.parts
        or _GLOB_MAGIC.search(path)
        or posix.parts[:1] == (".loom",)
    )


def _strip_inexact_artifacts(steps: list[Any]) -> tuple[list[dict[str, Any]], bool]:
    """Drop Harbor TB2.1 verifier globs that Nebius admission rejects."""

    changed = False
    cleaned: list[dict[str, Any]] = []
    for step in steps:
        if not isinstance(step, dict):
            cleaned.append(step)
            continue
        item = dict(step)
        for field in ("artifacts", "required_artifacts"):
            values = item.get(field)
            if not isinstance(values, list):
                continue
            kept = [path for path in values if isinstance(path, str) and _is_exact_relative_artifact(path)]
            if kept != values:
                changed = True
            if kept:
                item[field] = kept
            else:
                item.pop(field, None)
        cleaned.append(item)
    return cleaned, changed


def adapt_bundle_for_nebius_terminus(
    staged: Path,
    config: dict[str, Any],
) -> tuple[dict[str, Any], NebiusTerminusAdaptStats]:
    """Derive staged image/verifier inputs and config for Nebius Terminus.

    Returns a shallow-copied config dict (nested sections that are mutated are
    also copied) and counters describing what changed.
    """

    # Harbor consumes environment/docker-compose.yaml alongside its Dockerfile.
    # Also surface the standard Compose aliases; none is translated by this
    # single-image adapter. Do this before writing any derived inputs, including
    # for main-only overrides whose environment or mounts would otherwise vanish.
    compose_paths = tuple(
        f"environment/{name}"
        for name in ("docker-compose.yaml", "docker-compose.yml", "compose.yaml", "compose.yml")
        if (staged / "environment" / name).exists() or (staged / "environment" / name).is_symlink()
    )
    if compose_paths:
        raise UnsupportedComposeEnvironmentError(compose_paths)

    adapted = dict(config)
    environment = dict(adapted.get("environment") or {})
    verifier = dict(adapted.get("verifier") or {})
    verifier_args = dict(verifier.get("args") or {})

    resources_filled = False
    if environment.get("cpus") is None:
        environment["cpus"] = DEFAULT_CPUS
        resources_filled = True
    if environment.get("memory_mb") is None:
        environment["memory_mb"] = DEFAULT_MEMORY_MB
        resources_filled = True
    if environment.get("storage_mb") is None:
        environment["storage_mb"] = DEFAULT_STORAGE_MB
        resources_filled = True

    network_forced = (
        environment.get("network_policies_supported") != ["gateway-only"]
        or environment.get("baseline_network_policy") != {"kind": "gateway-only"}
    )
    if (environment.get("baseline_network_policy") or {}).get("kind") in {
        "web-allowlist", "public-web",
    }:
        # Admission validates the declared dialer policy. Do not replace an
        # exact host list or explicit public HTTP(S) with gateway-only.
        network_forced = False
    else:
        environment["network_policies_supported"] = ["gateway-only"]
        environment["baseline_network_policy"] = {"kind": "gateway-only"}

    cpu_arch_forced = environment.get("cpu_arch") != "x86_64"
    environment["cpu_arch"] = "x86_64"
    if environment.get("os") is None:
        environment["os"] = "linux"

    workdir = environment.get("workdir")
    workspace_forced = workdir is None
    environment["workdir"] = validate_task_workdir("/app" if workdir is None else workdir)
    environment.setdefault("user", "agent")

    verifier_identity_stripped = False
    verifier["name"] = verifier.get("name") or "script"
    # Preserve an explicit Harbor environment_mode. Omission stays separate,
    # which is the snapshot verifier. Do not rewrite that to shared.
    if verifier.get("env_mode") not in {"shared", "separate"}:
        verifier["env_mode"] = "separate"
    verifier_path_forced = verifier_args.get("script_path") != VERIFIER_SCRIPT_PATH
    verifier_args["script_path"] = VERIFIER_SCRIPT_PATH
    verifier["args"] = verifier_args

    steps_raw = adapted.get("steps")
    artifact_globs_stripped = False
    if isinstance(steps_raw, list):
        cleaned_steps, artifact_globs_stripped = _strip_inexact_artifacts(steps_raw)
        adapted["steps"] = cleaned_steps

    adapted["environment"] = environment
    adapted["verifier"] = verifier

    prepare_nebius_terminus_image(staged, environment)
    wrapper_installed = _ensure_offline_verifier_wrapper(staged)
    return adapted, NebiusTerminusAdaptStats(
        resources_filled=resources_filled,
        network_forced_gateway_only=network_forced,
        verifier_identity_stripped=verifier_identity_stripped,
        verifier_path_forced=verifier_path_forced,
        cpu_arch_forced=cpu_arch_forced,
        workspace_identity_forced=workspace_forced,
        verifier_wrapper_installed=wrapper_installed,
        artifact_globs_stripped=artifact_globs_stripped,
    )


def preflight_nebius_terminus_admission(
    config: dict[str, Any],
    source_provenance: dict[str, Any],
) -> tuple[str, ...]:
    """Run Nebius Terminus admission gates against an adapted config + SEI."""

    task = TaskConfig.model_validate(config)
    trial = TrialConfig(
        agent_name="terminus-2",
        agent_model=ModelSpec(provider="openai", name="gpt-5", source="api"),
        workspace_staging_policy_name="tb21",
    )
    return automatic_service_execution_rejections(
        task,
        trial,
        source_provenance=source_provenance,
        allow_task_image_preparation=True,
    )


@dataclass(frozen=True)
class NebiusTerminusProfileStats:
    adapted_tasks: int = 0
    verifier_wrappers_installed: int = 0
    resources_filled_tasks: int = 0
    preflight_passed: int = 0


def merge_profile_stats(
    total: NebiusTerminusProfileStats,
    adapt: NebiusTerminusAdaptStats,
    *,
    preflight_ok: bool,
) -> NebiusTerminusProfileStats:
    return NebiusTerminusProfileStats(
        adapted_tasks=total.adapted_tasks + 1,
        verifier_wrappers_installed=(
            total.verifier_wrappers_installed + int(adapt.verifier_wrapper_installed)
        ),
        resources_filled_tasks=(
            total.resources_filled_tasks + int(adapt.resources_filled)
        ),
        preflight_passed=total.preflight_passed + int(preflight_ok),
    )
