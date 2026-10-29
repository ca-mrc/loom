#!/usr/bin/env python3
"""Read-only, aggregate-only census of local single-component task inputs.

Fingerprints are grouping aids, never build/reuse authorization. No task text,
paths, image names, build-arg values or fingerprints are emitted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import stat
import sys
import tomllib
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any

_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_CONTEXT_FILES = 2_000
_MAX_CONTEXT_BYTES = 512 * 1024 * 1024
_REMOTE_INPUT = re.compile(r"\b(?:curl|wget|apt(?:-get)?|apk|pip\d*|npm|yarn|cargo|gem|go\s+get|git\s+clone)\b", re.I)
_SECRET_INPUT = re.compile(r"(?:type\s*=\s*(?:secret|ssh)|\b(?:secret|ssh)_mount\b)", re.I)


class CensusInputError(ValueError):
    """A bounded reason code suitable for aggregate output."""


def _relative(value: Any) -> Path:
    if not isinstance(value, str) or not value or "\\" in value:
        raise CensusInputError("unsupported_path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise CensusInputError("unsupported_path")
    return Path(*path.parts)


def _regular_file(path: Path, *, limit: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise CensusInputError("unsupported_file")
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise CensusInputError("input_budget_exceeded")
    return data


def _within(task: Path, relative: Path) -> Path:
    current = task
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise CensusInputError("symlink_input")
    return current


def _context_fingerprint(root: Path) -> tuple[str, int, int]:
    if not root.is_dir() or root.is_symlink():
        raise CensusInputError("unsupported_context")
    digest = hashlib.sha256()
    count, total = 0, 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise CensusInputError("symlink_input")
        info = path.stat()
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise CensusInputError("unsupported_file")
        name = path.relative_to(root).as_posix()
        digest.update(json.dumps([name, stat.S_IMODE(info.st_mode), path.is_dir()]).encode())
        if path.is_file():
            count += 1
            total += info.st_size
            if count > _MAX_CONTEXT_FILES or total > _MAX_CONTEXT_BYTES:
                raise CensusInputError("input_budget_exceeded")
            data = _regular_file(path, limit=_MAX_CONTEXT_BYTES - (total - info.st_size))
            if len(data) != info.st_size:
                raise CensusInputError("input_changed")
            digest.update(hashlib.sha256(data).digest())
    return digest.hexdigest(), count, total


def _base_references(dockerfile: str) -> tuple[tuple[str, ...], bool]:
    aliases: set[str] = set()
    bases: list[str] = []
    dynamic = False
    for line in dockerfile.replace("\\\n", " ").splitlines():
        if not re.match(r"^\s*FROM\s+", line, re.I):
            continue
        try:
            parts = shlex.split(line, comments=True)[1:]
        except ValueError as exc:
            raise CensusInputError("unparsed_dockerfile") from exc
        dynamic |= "$" in line
        while parts and parts[0].startswith("--"):
            parts.pop(0)
        if not parts:
            raise CensusInputError("unparsed_dockerfile")
        base = parts[0]
        if base.lower() not in aliases:
            bases.append(base)
        if len(parts) >= 3 and parts[1].lower() == "as":
            aliases.add(parts[2].lower())
    if not bases:
        raise CensusInputError("unparsed_dockerfile")
    return tuple(bases), dynamic


def _task_record(task: Path) -> dict[str, Any]:
    raw = _regular_file(task / "task.toml", limit=_MAX_MANIFEST_BYTES)
    config = tomllib.loads(raw.decode("utf-8"))
    environment = config.get("environment", {})
    if not isinstance(environment, dict):
        raise CensusInputError("unsupported_configuration")
    verifier = config.get("verifier", {})
    if (environment.get("sidecars") or environment.get("native_compose")
            or environment.get("compose_file")
            or (isinstance(verifier, dict) and verifier.get("environment"))):
        raise CensusInputError("multi_component_configuration")
    context_path = _relative(environment.get("docker_build_context", "environment"))
    dockerfile_path = _relative(environment.get("dockerfile", "environment/Dockerfile"))
    context, files, size = _context_fingerprint(_within(task, context_path))
    dockerfile = _regular_file(_within(task, dockerfile_path), limit=_MAX_MANIFEST_BYTES)
    text = dockerfile.decode("utf-8")
    bases, dynamic = _base_references(text)
    # Include all environment settings conservatively, including target/args.
    # Inactive settings can split candidates; they must never merge unlike inputs.
    settings = json.dumps(environment, sort_keys=True, separators=(",", ":"))
    static = hashlib.sha256(json.dumps([context, dockerfile_path.as_posix(),
        context_path.as_posix(), settings, hashlib.sha256(dockerfile).hexdigest()]).encode()).hexdigest()
    return {"static": static, "context": context, "dockerfile": hashlib.sha256(dockerfile).hexdigest(),
            "first_base": bases[0], "files": files, "bytes": size,
            "all_bases_pinned": all(base.lower() == "scratch" or re.search(r"@sha256:[0-9a-f]{64}$", base) for base in bases),
            "dynamic_base": dynamic, "remote_input_marker": bool(_REMOTE_INPUT.search(text)),
            "secret_input_marker": bool(_SECRET_INPUT.search(text))}


def _groups(values: list[str]) -> dict[str, Any]:
    sizes = sorted(Counter(values).values(), reverse=True)
    duplicates = [size for size in sizes if size > 1]
    return {"unique": len(sizes), "duplicate_groups": len(duplicates),
            "duplicate_occurrences": sum(size - 1 for size in duplicates),
            "largest_group": max(sizes, default=0)}


def census(roots: list[Path]) -> dict[str, Any]:
    tasks: set[Path] = set()
    for root in roots:
        if root.is_symlink() or not root.is_dir():
            raise CensusInputError("invalid_tasks_root")
        if (root / "task.toml").is_file():
            tasks.add(root.resolve())
        else:
            tasks.update(path.parent.resolve() for path in root.glob("*/task.toml")
                         if not path.parent.is_symlink())
    skipped: Counter[str] = Counter()
    records = []
    for task in sorted(tasks):
        try:
            records.append(_task_record(task))
        except CensusInputError as exc:
            skipped[str(exc)] += 1
        except (OSError, UnicodeError, ValueError, TypeError):
            skipped["unreadable_or_invalid_input"] += 1
    return {"schema_version": "loom.task-image-reuse-census.v1", "scope": "local_static_single_component",
            "tasks_discovered": len(tasks), "tasks_scanned": len(records),
            "components_scanned": len(records), "skipped": dict(sorted(skipped.items())),
            "dockerfile_bytes": _groups([record["dockerfile"] for record in records]),
            "context_tree": _groups([record["context"] for record in records]),
            "static_input_candidates": _groups([record["static"] for record in records]),
            "first_base_reference": _groups([record["first_base"] for record in records]),
            "context_files": sum(record["files"] for record in records),
            "context_bytes": sum(record["bytes"] for record in records),
            "input_risks": {key: sum(record[key] for record in records) for key in
                ("all_bases_pinned", "dynamic_base", "remote_input_marker", "secret_input_marker")},
            "runtime_observations": {key: None for key in
                ("trials", "exact_ready_hits", "layer_cache_hits", "equivalent_inflight_requests",
                 "build_seconds_avoided", "transferred_bytes_avoided")},
            "reuse_authorized": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks-root", type=Path, action="append", required=True,
                        help="A task directory or a directory of immediate task directories; read only")
    args = parser.parse_args(argv)
    try:
        result = census(args.tasks_root)
    except (CensusInputError, OSError):
        print(json.dumps({"error": "invalid_tasks_root"}), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
