"""Parse opt-in build-phase storage readings from the build termination message.

The untrusted build phase writes these records itself (#2750). Validation here
is schema validation only: a fabricated record that matches the schema is still
accepted, which is why every result carries ``build_phase_self_report``
provenance. They are sizing diagnostics, never enforcement, never a Pod total
and never a guaranteed peak. Parsing is total: any input yields a result.
"""
from __future__ import annotations

import json
from typing import Any

from loom_execution_actuator.task_image_renderer import (
    BOUNDARY_USAGE_LINE_BYTES,
    BOUNDARY_USAGE_MAX_RECORDS,
    BOUNDARY_USAGE_MESSAGE_BYTES,
    MAX_NATIVE_BUILD_COMPONENTS,
)

SOURCE = "build_phase_self_report"
MARKER = "loom_boundary_usage() {"
_POINTS = frozenset({"solve_end", "solve_failed"})
_WITH_DIRS = frozenset({"ok", "partial"})
_WITHOUT_DIRS = frozenset({"failed", "timeout", "skipped"})
_VOLUMES = frozenset({"scratch", "build"})
_RECORD_KEYS = frozenset({"v", "point", "component", "at", "status", "elapsed_ms"})
_VOLUME_KEYS = frozenset({"entries", "kib", "errors", "rc"})
_MAX_COUNTER = 2**53


def instrumented(pod: dict[str, Any]) -> bool:
    """Whether this Pod's rendered build command carries the reading function."""
    spec = pod.get("spec") if isinstance(pod, dict) else None
    containers = spec.get("initContainers") if isinstance(spec, dict) else None
    for container in containers if isinstance(containers, list) else []:
        if isinstance(container, dict) and container.get("name") == "build":
            command = container.get("command")
            return isinstance(command, list) and bool(command) and isinstance(command[-1], str) and MARKER in command[-1]
    return False


def _counter(value: Any, limit: int = _MAX_COUNTER) -> bool:
    return type(value) is int and 0 <= value < limit


def _string_in(value: Any, allowed: frozenset[str]) -> bool:
    return isinstance(value, str) and value in allowed


def _record(line: str) -> dict[str, Any] | None:
    if len(line.encode()) > BOUNDARY_USAGE_LINE_BYTES:
        return None
    try:
        value = json.loads(line)
    except (ValueError, RecursionError):
        return None
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        return None
    status = value.get("status")
    if set(value) - {"dirs"} != _RECORD_KEYS or not (_string_in(status, _WITH_DIRS) or _string_in(status, _WITHOUT_DIRS)):
        return None
    if ("dirs" in value) != (status in _WITH_DIRS):
        return None
    if (type(value["v"]) is not int or value["v"] != 1 or not _string_in(value["point"], _POINTS)
            or not _counter(value["component"], MAX_NATIVE_BUILD_COMPONENTS)
            or not _counter(value["at"]) or not _counter(value["elapsed_ms"])):
        return None
    record = {key: value[key] for key in ("point", "component", "at", "status", "elapsed_ms")}
    if status in _WITH_DIRS:
        dirs = value["dirs"]
        if not isinstance(dirs, dict) or not dirs or not all(_string_in(name, _VOLUMES) for name in dirs):
            return None
        for row in dirs.values():
            if (not isinstance(row, dict) or set(row) != _VOLUME_KEYS
                    or not all(_counter(row[key]) for key in ("entries", "kib", "errors"))
                    or not _counter(row["rc"], 256)):
                return None
        complete = set(dirs) == _VOLUMES and all(row["rc"] == 0 and row["errors"] == 0 for row in dirs.values())
        if complete != (status == "ok"):
            return None  # "ok" must be complete; anything incomplete must say "partial".
        record["dirs"] = {name: {key: dirs[name][key] for key in sorted(_VOLUME_KEYS)} for name in sorted(dirs)}
    return record


def parse_boundary_usage(message: Any) -> dict[str, Any]:
    """Return bounded, validated records plus explicit rejected/missing status."""
    records: list[dict[str, Any]] = []
    rejected = 0
    seen: set[tuple[str, int]] = set()
    if isinstance(message, str) and len(message.encode()) <= BOUNDARY_USAGE_MESSAGE_BYTES:
        lines = [line for line in message.splitlines() if line.strip()]
        for line in lines[:BOUNDARY_USAGE_MAX_RECORDS]:
            record = _record(line)
            if record is None or (record["point"], record["component"]) in seen:
                rejected += 1
                continue
            seen.add((record["point"], record["component"]))
            records.append(record)
        rejected += max(0, len(lines) - BOUNDARY_USAGE_MAX_RECORDS)
    elif message not in (None, ""):
        rejected = 1
    status = "recorded" if records else "rejected" if rejected else "missing"
    return {"source": SOURCE, "status": status, "records": records, "rejected": rejected}


def observed_boundary_usage(message: Any) -> dict[str, Any]:
    """Lifecycle-safe wrapper: telemetry can never raise into build reconciliation."""
    try:
        return parse_boundary_usage(message)
    except Exception:  # pragma: no cover - parse_boundary_usage is total; this is the isolation boundary
        return {"source": SOURCE, "status": "rejected", "records": [], "rejected": 1}
