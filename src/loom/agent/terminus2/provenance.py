"""Runtime provenance constants for Harbor-embedded Terminus-2 (#744)."""

from __future__ import annotations

import os
from importlib.metadata import PackageNotFoundError, version

HARBOR_COMPAT_SHA = os.environ.get(
    "LOOM_HARBOR_SOURCE_REVISION", "09f5b964f8e5075f7e52fd08c9994dfc80ad5d24",
)
try:
    HARBOR_RUNTIME_VERSION = version("harbor")
except PackageNotFoundError:
    HARBOR_RUNTIME_VERSION = "0.24.0"
LOOM_BRIDGE_REVISION = os.environ.get("LOOM_BRIDGE_REVISION", "1.0")


def harbor_template_hashes() -> dict[str, str]:
    """Best-effort template hashes from the installed Harbor package."""
    try:
        import hashlib
        from importlib.resources import files

        templates = files("harbor.agents.terminus_2.templates")
        out: dict[str, str] = {}
        for name in ("terminus-json-plain.txt", "timeout.txt"):
            try:
                data = (templates / name).read_bytes()
            except (FileNotFoundError, OSError, TypeError):
                continue
            out[name] = hashlib.sha256(data).hexdigest()
        return out
    except Exception:
        return {}
