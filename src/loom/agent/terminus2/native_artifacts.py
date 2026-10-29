"""Preserve Harbor's referenced native subagent trajectories for transport.

The archive is a fixed declared output. Its members remain Harbor JSON files;
this adapter discovers and checks them, without implementing summarization.
"""

from __future__ import annotations

import io
import json
import os
import re
import stat
import tarfile
import tempfile
from collections import deque
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

HARBOR_SUBAGENT_ARCHIVE = "subagent-trajectories.tar"
HARBOR_SUBAGENT_ISSUES = "subagent-trajectories-errors.json"
HARBOR_ARTIFACT_NAMES = ("trajectory.json", "recording.cast", HARBOR_SUBAGENT_ARCHIVE)
MAX_HARBOR_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_SUBAGENT_FILES = 4096
_MAX_ISSUES = 256
_SUBAGENT_NAME = re.compile(r"trajectory\.summarization-[1-9][0-9]*-(summary|questions|answers)\.json")


def is_harbor_subagent_name(name: str) -> bool:
    """Accept only the flat filenames written by pinned Harbor Terminus2."""
    return len(name) <= 100 and _SUBAGENT_NAME.fullmatch(name) is not None


class _InvalidArtifactError(Exception):
    pass


def _read_trajectory(path: Path, limit: int) -> tuple[bytes, dict[str, Any]]:
    try:
        # O_NOFOLLOW also closes the check/open race for a replaced symlink.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError as exc:
        raise _InvalidArtifactError("missing") from exc
    except OSError as exc:
        raise _InvalidArtifactError("unsafe_file") from exc
    with os.fdopen(fd, "rb") as stream:
        details = os.fstat(stream.fileno())
        if not stat.S_ISREG(details.st_mode):
            raise _InvalidArtifactError("unsafe_file")
        if details.st_size > limit:
            raise _InvalidArtifactError("size_limit")
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise _InvalidArtifactError("size_limit")
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeError) as exc:
        raise _InvalidArtifactError("invalid_json") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("steps"), list):
        raise _InvalidArtifactError("invalid_trajectory")
    return body, payload


def _references(payload: dict[str, Any]) -> Iterator[Any]:
    for step in payload["steps"]:
        if not isinstance(step, dict):
            continue
        observation = step.get("observation")
        if not isinstance(observation, dict):
            continue
        results = observation.get("results")
        if not isinstance(results, list):
            continue
        for result in results:
            if not isinstance(result, dict) or "subagent_trajectory_ref" not in result:
                continue
            refs = result["subagent_trajectory_ref"]
            if isinstance(refs, list):
                yield from refs
            elif refs is not None:
                yield None


def _add_member(archive: tarfile.TarFile, name: str, body: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mode = 0o600
    archive.addfile(info, io.BytesIO(body))


def prepare_subagent_archive(
    logs_root: Path, *, sanitize_trajectory: Callable[[Path], None],
) -> Path | None:
    """Pack the valid reference closure, with bounded diagnostic failures.

    Missing or corrupt upstream diagnostic files produce an issue member rather
    than an execution failure. The caller's credential sanitizer is deliberately
    allowed to raise: unsafe bytes must never reach the archive. All work is
    synchronous and local, including when invoked by the timeout retention sink.
    """
    if logs_root.is_symlink() or not logs_root.is_dir():
        return None
    destination = logs_root / HARBOR_SUBAGENT_ARCHIVE
    # A second invocation must not retain stale members, including on failure.
    destination.unlink(missing_ok=True)
    try:
        _, main = _read_trajectory(logs_root / "trajectory.json", MAX_HARBOR_ARCHIVE_BYTES)
    except _InvalidArtifactError:
        # The main trajectory's existing publisher owns its validity contract.
        return None
    initial = list(_references(main))
    if not initial:
        return None

    issues: list[dict[str, str]] = []

    def issue(name: str, reason: str) -> None:
        if len(issues) < _MAX_ISSUES:
            issues.append({"trajectory_path": name, "reason": reason})
        elif issues[-1]["reason"] != "issue_limit":
            issues[-1] = {"trajectory_path": "[remaining]", "reason": "issue_limit"}

    pending = deque(initial)
    sessions: dict[str, str] = {}
    conflicts: set[str] = set()
    bodies: dict[str, bytes] = {}
    children: dict[str, list[Any]] = {}
    total_bytes = 0
    # Reserve room for diagnostics, tar headers and its final record padding.
    reserve = min(4 * 1024 * 1024, MAX_HARBOR_ARCHIVE_BYTES // 4)
    payload_budget = MAX_HARBOR_ARCHIVE_BYTES - reserve
    while pending:
        ref = pending.popleft()
        if not isinstance(ref, dict):
            issue("[invalid]", "invalid_reference")
            continue
        name, session = ref.get("trajectory_path"), ref.get("session_id")
        if not isinstance(name, str) or not is_harbor_subagent_name(name):
            issue("[invalid]", "unsafe_path")
            continue
        if not isinstance(session, str) or not session:
            issue(name, "invalid_reference")
            continue
        if name in sessions:
            if sessions[name] != session and name not in conflicts:
                conflicts.add(name)
                bodies.pop(name, None)
                issue(name, "conflicting_session")
            continue
        if len(sessions) >= MAX_SUBAGENT_FILES:
            issue("[remaining]", "count_limit")
            continue
        sessions[name] = session
        path = logs_root / name
        try:
            _, payload = _read_trajectory(path, payload_budget - total_bytes)
            if payload.get("session_id") != session:
                raise _InvalidArtifactError("session_mismatch")
        except _InvalidArtifactError as exc:
            issue(name, str(exc))
            continue
        sanitize_trajectory(path)
        try:
            body, payload = _read_trajectory(path, payload_budget - total_bytes)
            if payload.get("session_id") != session:
                raise _InvalidArtifactError("session_mismatch")
        except _InvalidArtifactError as exc:
            issue(name, str(exc))
            continue
        bodies[name] = body
        total_bytes += len(body) + 512 + (-len(body) % 512)
        children[name] = list(_references(payload))
        pending.extend(children[name])

    # A conflict discovered through a later reference can invalidate a parent;
    # keep only files still reachable through validated, unambiguous parents.
    reachable: set[str] = set()
    pending = deque(initial)
    while pending:
        ref = pending.popleft()
        name = ref.get("trajectory_path") if isinstance(ref, dict) else None
        if not isinstance(name, str) or name in reachable or name not in bodies:
            continue
        reachable.add(name)
        pending.extend(children.get(name, []))

    errors = json.dumps({"issues": issues}, indent=2).encode() + b"\n" if issues else None
    # Header overhead is included above; diagnostics are bounded independently.
    fd, temporary = tempfile.mkstemp(prefix=".subagent-trajectories-", dir=logs_root)
    staging = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream, tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for name in sorted(reachable):
                _add_member(archive, name, bodies[name])
            if errors is not None:
                _add_member(archive, HARBOR_SUBAGENT_ISSUES, errors)
        if staging.stat().st_size > MAX_HARBOR_ARCHIVE_BYTES:
            raise ValueError("Harbor subagent archive exceeded its transport limit")
        staging.replace(destination)
    finally:
        staging.unlink(missing_ok=True)
    return destination
