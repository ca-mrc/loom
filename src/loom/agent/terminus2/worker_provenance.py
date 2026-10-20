"""Worker image dependency provenance for Harbor-embedded Terminus-2 (#744 Gate 1)."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA, HARBOR_RUNTIME_VERSION

WORKER_PYTHON_VERSION = "3.12"
WORKER_IMAGE_LOCK_REL = "deploy/worker-image.lock"
WORKER_WHEELS_REL = "deploy/worker-image.wheels.json"


def _repo_root() -> Path:
    # src/loom/agent/terminus2/worker_provenance.py -> repo root
    return Path(__file__).resolve().parents[4]


@lru_cache(maxsize=1)
def load_worker_wheel_provenance() -> dict[str, Any]:
    """Load pinned wheel hashes for worker-image key packages."""
    path = _repo_root() / WORKER_WHEELS_REL
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


@lru_cache(maxsize=1)
def load_worker_image_lock_lines() -> tuple[str, ...]:
    path = _repo_root() / WORKER_IMAGE_LOCK_REL
    return tuple(path.read_text(encoding="utf-8").splitlines())


def worker_image_lock_pins() -> dict[str, str]:
    """Return ``name -> pin`` for harbor/openai/litellm from the lock file."""
    pins: dict[str, str] = {}
    for line in load_worker_image_lock_lines():
        stripped = line.strip()
        if stripped.startswith("harbor @ git+"):
            pins["harbor"] = stripped.removeprefix("harbor @ ")
            continue
        if "==" in stripped and not stripped.startswith("#") and not stripped.startswith("-e"):
            name, version = stripped.split("==", 1)
            pins[name] = version
    return pins


def worker_image_provenance_summary() -> dict[str, Any]:
    """Compact provenance blob for attestation / staging preflight."""
    wheels = load_worker_wheel_provenance()
    pins = worker_image_lock_pins()
    return {
        "python_version": WORKER_PYTHON_VERSION,
        "harbor_compat_sha": HARBOR_COMPAT_SHA,
        "harbor_runtime_version": HARBOR_RUNTIME_VERSION,
        "openai_version": pins.get("openai"),
        "litellm_version": pins.get("litellm"),
        "harbor_source": pins.get("harbor"),
        "wheel_hashes": {
            name: entry["sha256"]
            for name, entry in wheels.get("packages", {}).items()
        },
    }
