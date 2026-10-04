"""Locally available build identity for this running service instance
(#2009).

Read from files the image writes at build time (see `deploy/Dockerfile.service`).
Never queries GitHub, Kubernetes, or the database — this is the responding
instance's own local, immutable build metadata, nothing more. Lenient by
design: a local dev tree or an image built without this metadata must never
raise from here. Callers get `None` and show an honest "unknown" instead of
blocking startup or normal use.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _read_stripped(env_var: str, default_path: str) -> str | None:
    # Re-read the path from the environment on every call rather than
    # caching it at import time: cheap (one local file read), and it keeps
    # the override live for tests and any future operational escape hatch.
    path = Path(os.environ.get(env_var, default_path))
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    return value or None


def read_build_revision() -> str | None:
    """The full 40-character commit SHA this image was built from, or
    `None` when unavailable or malformed (never partially trusted)."""
    if _read_stripped("LOOM_BUILD_KIND_PATH", "/opt/loom/build-kind") == "personal":
        return None
    value = _read_stripped("LOOM_BUILD_SHA_PATH", "/opt/loom/build-sha")
    if value is None or not _SHA_RE.fullmatch(value):
        return None
    return value


def read_build_time() -> str | None:
    """An informational UTC build timestamp, or `None` when unavailable.
    Not validated beyond non-empty — display-only, never used for
    ordering or trust decisions."""
    return _read_stripped("LOOM_BUILD_TIME_PATH", "/opt/loom/build-time")


def read_build_source() -> dict[str, str | None]:
    """Informational image-stamped source identity, never CI/rollout authority."""
    kind = _read_stripped("LOOM_BUILD_KIND_PATH", "/opt/loom/build-kind")
    if kind not in {"personal", "commit"}:
        return {}  # Preserve the legacy unstamped response.
    if kind == "commit":
        return {"buildKind": kind}
    digest = _read_stripped("LOOM_BUILD_SOURCE_DIGEST_PATH", "/opt/loom/source-digest")
    base = _read_stripped("LOOM_BUILD_SOURCE_BASE_COMMIT_PATH", "/opt/loom/source-base-commit")
    return {"buildKind": kind,
        "sourceDigest": digest if digest is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", digest) else None,
        "sourceBaseCommit": base if base is not None and re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", base) else None}
