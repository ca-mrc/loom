"""Capture authored Git worktree bytes, never Git HEAD or owner credentials."""
from __future__ import annotations

import hashlib
import os
import selectors
import stat
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from loom.application_source import (
    MAX_SOURCE_BYTES,
    MAX_SOURCE_FILES,
    ApplicationSourceManifestV1,
    capture_application_source_file,
    source_inode,
    source_parent,
    source_path,
)
from loom.application_source_archive import write_application_source_archive

_EXCLUDED_DIRECTORIES = frozenset({
    ".git", ".loom", ".codex", ".claude", ".worktrees", "worktrees", ".venv",
})
_EXCLUDED_NAMES = frozenset({"AGENTS.md", "MEMORY.md", "NEW_SESSION_BRIEFING.md", "id_rsa", "id_ed25519"})
_MAX_GIT_OUTPUT = MAX_SOURCE_FILES * (1024 + 128)
_GIT_TIMEOUT = 20.0


@dataclass(frozen=True)
class CapturedApplicationSource:
    root: Path
    manifest: ApplicationSourceManifestV1
    base_commit: str | None


@dataclass(frozen=True)
class PackagedApplicationSource:
    manifest: ApplicationSourceManifestV1
    base_commit: str | None
    archive: BinaryIO = field(repr=False)
    archive_sha256: str
    archive_size_bytes: int


@contextmanager
def package_application_source(root: Path) -> Iterator[PackagedApplicationSource]:
    """Yield a private upload stream; neither digest asserts CI approval."""
    with tempfile.TemporaryFile(mode="w+b") as archive:
        with capture_application_source(root) as source:
            write_application_source_archive(source.root, source.manifest, archive)
        length = archive.tell()
        archive.seek(0)
        checksum = hashlib.sha256()
        while chunk := archive.read(1024 * 1024):
            checksum.update(chunk)
        archive.seek(0)
        yield PackagedApplicationSource(source.manifest, source.base_commit, archive,
                                        "sha256:" + checksum.hexdigest(), length)


@dataclass(frozen=True)
class _Inventory:
    files: tuple[tuple[str, tuple[int, ...]], ...]
    directories: tuple[tuple[str, tuple[int, ...]], ...]


def _git(root: Path, *args: str, missing_ok: bool = False) -> bytes:
    """Bound read-only Git output/time, disabling ambient tree and hook overrides."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    # Keep normal global/system excludes; -c below disables executable hooks
    # without turning normally ignored local credentials into upload candidates.
    env.update(GIT_OPTIONAL_LOCKS="0", GIT_TERMINAL_PROMPT="0",
               GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="")
    command = ["git", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false",
               "-c", "core.hooksPath=/dev/null", "--literal-pathspecs", "-C", str(root), *args]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env) as process:
        assert process.stdout is not None
        try:
            chunks = bytearray()
            deadline = time.monotonic() + _GIT_TIMEOUT
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise ValueError("application source Git read timed out")
                    chunk = os.read(process.stdout.fileno(), min(65536, _MAX_GIT_OUTPUT - len(chunks) + 1))
                    if not chunk:
                        break
                    chunks.extend(chunk)
                    if len(chunks) > _MAX_GIT_OUTPUT:
                        raise ValueError("application source Git inventory too large")
            code = process.wait(timeout=max(0.001, deadline - time.monotonic()))
            if code != 0 and not (missing_ok and code == 1 and not chunks):
                raise ValueError("application source Git read failed")
            return bytes(chunks)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def _excluded(path: str) -> bool:
    parts = path.split("/")
    return any(part in _EXCLUDED_DIRECTORIES or part in _EXCLUDED_NAMES
               or part.endswith((".key", ".pem"))
               or ((part == ".env" or part.startswith(".env.")) and part != ".env.example")
               for part in parts)


def _selected_paths(root: Path) -> dict[str, bool]:
    selected: dict[str, bool] = {}
    for row in _git(root, "ls-files", "--sparse", "--stage", "-z").split(b"\0"):
        if not row:
            continue
        metadata, raw_path = row.split(b"\t", 1)
        mode, _object, stage = metadata.split(b" ")
        if stage != b"0" or mode in {b"160000", b"040000"}:
            raise ValueError("application source contains unmerged entries, submodules or sparse directories")
        path = raw_path.decode("utf-8")
        if not _excluded(path):
            selected[source_path(path)] = True
    for raw_path in _git(root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0"):
        if raw_path:
            path = raw_path.decode("utf-8")
            if not _excluded(path):
                selected[source_path(path)] = False
    if len(selected) > MAX_SOURCE_FILES:
        raise ValueError("application source file count exceeded")
    return selected


def _inventory(root: Path) -> _Inventory:
    files: list[tuple[str, tuple[int, ...]]] = []
    parents = {"."}
    for path, tracked in sorted(_selected_paths(root).items(), key=lambda item: item[0].encode("utf-8")):
        try:
            with source_parent(root, path) as (directory, name):
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            if tracked:
                continue  # Local deletion is intentional, never restored from HEAD.
            raise
        if (info.st_uid != os.getuid() or info.st_nlink != 1
                or not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode))):
            raise ValueError("unsupported application source file")
        files.append((path, source_inode(info)))
        parents.update(str(parent) for parent in PurePosixPath(path).parents)
    directories = []
    for path in sorted(parents):
        # Open the directory itself without opening or requiring a leaf.
        probe = "unused" if path == "." else f"{path}/unused"
        with source_parent(root, probe) as (directory, _name):
            directories.append((path, source_inode(os.fstat(directory))))
    return _Inventory(tuple(files), tuple(directories))


def _temporary_parent(root: Path) -> Path:
    for candidate in (Path(tempfile.gettempdir()), Path("/tmp"), Path("/var/tmp")):
        candidate = candidate.resolve()
        if candidate.is_dir() and not candidate.is_relative_to(root):
            return candidate
    raise ValueError("application source requires temporary storage outside the checkout")


def _base_commit(root: Path) -> str | None:
    value = _git(root, "rev-parse", "--verify", "-q", "HEAD", missing_ok=True).strip().decode("ascii")
    if value and (len(value) not in {40, 64} or any(char not in "0123456789abcdef" for char in value)):
        raise ValueError("invalid application source base commit")
    return value or None


@contextmanager
def capture_application_source(root: Path) -> Iterator[CapturedApplicationSource]:
    """Yield a private snapshot; consumers must use verified manifest reads."""
    try:
        root = root.resolve(strict=True)
        actual = _git(root, "rev-parse", "--show-toplevel").rstrip(b"\n").decode("utf-8")
        if Path(actual).resolve() != root:
            raise ValueError("application source must select the complete Git worktree root")
        if _git(root, "config", "--bool", "--get", "core.sparseCheckout", missing_ok=True).strip() == b"true":
            raise ValueError("application source sparse checkouts are unsupported")
        base = _base_commit(root)
        before = _inventory(root)
    except (OSError, ValueError, subprocess.SubprocessError):
        raise ValueError("invalid application source worktree") from None
    with tempfile.TemporaryDirectory(prefix="loom-application-source-", dir=_temporary_parent(root)) as temporary:
        stage = Path(temporary)
        try:
            entries, total = [], 0
            for path, identity in before.files:
                entry, body, observed = capture_application_source_file(root, path, limit=MAX_SOURCE_BYTES - total)
                if observed != identity:
                    raise ValueError("application source changed during capture")
                entries.append(entry)
                total += len(body)
                if entry.link_target is None:
                    destination = stage / path
                    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    destination.write_bytes(body)
                    destination.chmod(int(entry.mode, 8))
            manifest = ApplicationSourceManifestV1(files=tuple(entries))
            manifest.canonical_bytes()  # Enforce transfer size before yielding any content.
            if _inventory(root) != before or _base_commit(root) != base:
                raise ValueError("application source changed during capture")
            # Only install links after the complete manifest rejects escapes and collisions.
            for entry in entries:
                if entry.link_target is not None:
                    destination = stage / entry.path
                    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    destination.symlink_to(entry.link_target)
        except (OSError, ValueError, subprocess.SubprocessError):
            raise ValueError("application source snapshot failed; retry from a stable checkout") from None
        yield CapturedApplicationSource(stage, manifest, base)
