"""Immutable personal application source, independent of task and release authority."""
from __future__ import annotations

import hashlib
import os
import stat
from bisect import bisect_left
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, Self

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_SOURCE_BYTES = 512 * 1024**2
MAX_SOURCE_FILES = 25_000
MAX_MANIFEST_BYTES = 8 * 1024**2
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def source_path(value: str) -> str:
    parts = value.split("/")
    if (not value or len(value.encode("utf-8")) > 1024 or len(parts) > 64
            or any(part in {"", ".", ".."} for part in parts) or "\\" in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError("invalid application source path")
    return value


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


class ApplicationSourceFileV1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    path: str
    size_bytes: int = Field(ge=0, le=MAX_SOURCE_BYTES)
    sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    mode: Literal["0644", "0755", "0777"]
    link_target: str | None = None

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return source_path(value)

    @model_validator(mode="after")
    def _link(self) -> Self:
        if self.link_target is None:
            if self.mode == "0777":
                raise ValueError("source link requires a target")
        else:
            target = self.link_target
            body = target.encode("utf-8")
            if (self.mode != "0777" or not body or len(body) > 1024 or target.startswith("/")
                    or "\\" in target or len(target.split("/")) > 64
                    or any(ord(char) < 32 or ord(char) == 127 for char in target)
                    or self.size_bytes != len(body) or self.sha256 != _digest(body)):
                raise ValueError("invalid application source link")
        return self


def _is_directory(path: str, sorted_paths: list[str]) -> bool:
    """Find a descendant without materializing all ancestors of every file.

    Valid UTF-8 paths have the same scalar and byte ordering. Searching for the
    slash-qualified prefix also handles intervening names such as a- before a/b.
    """
    prefix = path + "/"
    index = bisect_left(sorted_paths, prefix)
    return index < len(sorted_paths) and sorted_paths[index].startswith(prefix)


def _resolve(path: str, files: dict[str, ApplicationSourceFileV1], sorted_paths: list[str]) -> None:
    remaining = deque(path.split("/"))
    resolved: list[str] = []
    links = 0
    while remaining:
        part = remaining.popleft()
        if part in {"", "."}:
            continue
        if part == "..":
            if not resolved:
                raise ValueError("application source link escapes root")
            resolved.pop()
            continue
        candidate = "/".join([*resolved, part])
        item = files.get(candidate)
        if item is not None and item.link_target is not None:
            links += 1
            if links > 40:
                raise ValueError("application source link cycle or chain too long")
            remaining.extendleft(reversed(item.link_target.split("/")))
        elif _is_directory(candidate, sorted_paths):
            resolved.append(part)
        elif item is not None and not remaining:
            return
        else:
            raise ValueError("application source link target is unavailable")


class ApplicationSourceManifestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal["loom.application-source.v1"] = "loom.application-source.v1"
    files: tuple[ApplicationSourceFileV1, ...] = Field(min_length=1, max_length=MAX_SOURCE_FILES)

    @model_validator(mode="after")
    def _inventory(self) -> Self:
        paths = [item.path for item in self.files]
        if paths != sorted(set(paths), key=lambda item: item.encode("utf-8")):
            raise ValueError("application source files must be uniquely sorted")
        if sum(item.size_bytes for item in self.files) > MAX_SOURCE_BYTES:
            raise ValueError("application source byte limit exceeded")
        indexed = {item.path: item for item in self.files}
        if any(_is_directory(path, paths) for path in paths):
            raise ValueError("application source file is also a directory")
        for item in self.files:
            if item.link_target is not None:
                _resolve(item.path, indexed, paths)
        return self

    def canonical_bytes(self) -> bytes:
        body = rfc8785.dumps(self.model_dump(mode="json"))
        if len(body) > MAX_MANIFEST_BYTES:
            raise ValueError("application source manifest too large")
        return body

    @property
    def digest(self) -> str:
        return _digest(self.canonical_bytes())


def parse_application_source_manifest(payload: bytes, *, expected_digest: str) -> ApplicationSourceManifestV1:
    try:
        if (type(payload) is not bytes or not 0 < len(payload) <= MAX_MANIFEST_BYTES
                or _digest(payload) != expected_digest):
            raise ValueError
        source = ApplicationSourceManifestV1.model_validate_json(payload)
        if source.canonical_bytes() != payload:
            raise ValueError
        return source
    except (ValueError, TypeError, RecursionError):
        raise ValueError("invalid application source manifest") from None


def source_inode(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


@contextmanager
def source_parent(root: Path, path: str) -> Iterator[tuple[int, str]]:
    """Open only owned, non-link directories; callers never follow entry links."""
    source_path(path)
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(root, _DIRECTORY_FLAGS))
        for part in path.split("/")[:-1]:
            if os.fstat(descriptors[-1]).st_uid != os.getuid():
                raise ValueError("application source directory is not owned")
            descriptors.append(os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptors[-1]))
        if os.fstat(descriptors[-1]).st_uid != os.getuid():
            raise ValueError("application source directory is not owned")
        yield descriptors[-1], path.split("/")[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def capture_application_source_file(root: Path, path: str, *, limit: int = MAX_SOURCE_BYTES
                                    ) -> tuple[ApplicationSourceFileV1, bytes, tuple[int, ...]]:
    """Capture one no-follow entry; complete inventory/link checks remain required."""
    try:
        with source_parent(root, path) as (directory, name):
            before = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (before.st_uid != os.getuid() or before.st_nlink != 1 or before.st_size > limit
                    or not (stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode))):
                raise ValueError
            target = None
            mode: Literal["0644", "0755", "0777"]
            if stat.S_ISLNK(before.st_mode):
                target = os.readlink(name, dir_fd=directory)
                body, mode = target.encode("utf-8"), "0777"
            else:
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=directory)
                try:
                    if source_inode(os.fstat(descriptor)) != source_inode(before):
                        raise ValueError
                    chunks, count = [], 0
                    while True:
                        chunk = os.read(descriptor, min(1024**2, before.st_size - count + 1))
                        if not chunk:
                            break
                        count += len(chunk)
                        if count > before.st_size:
                            raise ValueError
                        chunks.append(chunk)
                    if source_inode(os.fstat(descriptor)) != source_inode(before):
                        raise ValueError
                    body, mode = b"".join(chunks), "0755" if before.st_mode & 0o111 else "0644"
                finally:
                    os.close(descriptor)
            if (len(body) != before.st_size or len(body) > limit
                    or source_inode(os.stat(name, dir_fd=directory, follow_symlinks=False)) != source_inode(before)):
                raise ValueError
            entry = ApplicationSourceFileV1(path=path, size_bytes=len(body), sha256=_digest(body),
                                           mode=mode, link_target=target)
            return entry, body, source_inode(before)
    except (OSError, ValueError):
        raise ValueError("application source capture failed") from None


def read_application_source_file(root: Path, entry: ApplicationSourceFileV1) -> bytes:
    entry = ApplicationSourceFileV1.model_validate_json(entry.model_dump_json())
    actual, body, _ = capture_application_source_file(root, entry.path, limit=entry.size_bytes)
    if actual != entry:
        raise ValueError("application source no longer matches its manifest")
    return body
