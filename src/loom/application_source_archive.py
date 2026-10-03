"""Bounded source transport for upload verification and trusted build preparation.

Only manifest-bound bytes cross this boundary. Archive paths are fixed numbered
records, never extraction paths; safe source links are installed last. A failed
extraction leaves unaccepted private partial files for the caller to clean up.
"""
from __future__ import annotations

import hashlib
import os
import stat
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from loom.application_source import (
    MAX_MANIFEST_BYTES,
    MAX_SOURCE_BYTES,
    MAX_SOURCE_FILES,
    ApplicationSourceManifestV1,
    parse_application_source_manifest,
    read_application_source_file,
)

_BLOCK = tarfile.BLOCKSIZE
_RECORD = tarfile.RECORDSIZE
MAX_APPLICATION_SOURCE_ARCHIVE_BYTES = MAX_SOURCE_BYTES + MAX_MANIFEST_BYTES + (MAX_SOURCE_FILES + 1) * 2 * _BLOCK + _RECORD
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _header(name: str, size: int) -> bytes:
    member = tarfile.TarInfo(name)
    member.size, member.mode = size, 0o644
    return member.tobuf(format=tarfile.USTAR_FORMAT, encoding="utf-8", errors="strict")


def _write_record(output: BinaryIO, name: str, body: bytes) -> None:
    for content in (_header(name, len(body)), body, bytes(-len(body) % _BLOCK)):
        if output.write(content) != len(content):
            raise ValueError("incomplete application source archive write")


def write_application_source_archive(root: Path, manifest: ApplicationSourceManifestV1, output: BinaryIO) -> None:
    """Encode verified captured bytes into deterministic, uncompressed USTAR."""
    manifest = ApplicationSourceManifestV1.model_validate_json(manifest.model_dump_json())
    if output.seek(0, os.SEEK_END) != 0:
        raise ValueError("application source archive output must be empty")
    _write_record(output, "manifest.json", manifest.canonical_bytes())
    for index, entry in enumerate(manifest.files):
        _write_record(output, f"files/{index:05d}", read_application_source_file(root, entry))
    padding = bytes(2 * _BLOCK + (-(output.tell() + 2 * _BLOCK) % _RECORD))
    if output.write(padding) != len(padding):
        raise ValueError("incomplete application source archive write")


def _exact(source: BinaryIO, size: int) -> bytes:
    value = source.read(size)
    if len(value) != size:
        raise ValueError
    return value


def _record_size(source: BinaryIO, name: str, *, limit: int, size: int | None = None) -> int:
    header = _exact(source, _BLOCK)
    # Parse only one bounded header, never extension records or compressed data.
    member = tarfile.TarInfo.frombuf(header, "utf-8", "strict")
    if (not 0 <= member.size <= limit or (size is not None and member.size != size)
            or header != _header(name, member.size)):
        raise ValueError
    return member.size


def _read_record(source: BinaryIO, name: str, *, limit: int, size: int | None = None) -> bytes:
    length = _record_size(source, name, limit=limit, size=size)
    body = _exact(source, length)
    if any(_exact(source, -length % _BLOCK)):
        raise ValueError
    return body


@contextmanager
def _parent(root: int, path: str) -> Iterator[tuple[int, str]]:
    descriptor = os.dup(root)
    try:
        parts = path.split("/")
        for part in parts[:-1]:
            try:
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            child = os.open(part, _DIRECTORY, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
                raise ValueError
        yield descriptor, parts[-1]
    finally:
        os.close(descriptor)


def extract_application_source_archive(source: BinaryIO, *, expected_digest: str,
                                       destination: Path) -> ApplicationSourceManifestV1:
    """Verify all transport content before accepting the private build context."""
    descriptor = None
    try:
        length = source.seek(0, os.SEEK_END)
        if not _RECORD <= length <= MAX_APPLICATION_SOURCE_ARCHIVE_BYTES or length % _RECORD:
            raise ValueError
        source.seek(0)
        if destination != destination.resolve(strict=True):
            raise ValueError
        descriptor = os.open(destination, _DIRECTORY)
        metadata = os.fstat(descriptor)
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or metadata.st_mode & 0o077 or os.listdir(descriptor)):
            raise ValueError
        manifest = parse_application_source_manifest(_read_record(source, "manifest.json", limit=MAX_MANIFEST_BYTES),
                                                    expected_digest=expected_digest)
        for index, entry in enumerate(manifest.files):
            if entry.link_target is not None:
                # Link payloads are bounded to 1024 bytes by the manifest.
                body = _read_record(source, f"files/{index:05d}", limit=entry.size_bytes, size=entry.size_bytes)
                if "sha256:" + hashlib.sha256(body).hexdigest() != entry.sha256:
                    raise ValueError
                continue
            remaining = _record_size(source, f"files/{index:05d}", limit=entry.size_bytes, size=entry.size_bytes)
            with _parent(descriptor, entry.path) as (parent, name):
                output = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 0o600, dir_fd=parent)
                with os.fdopen(output, "wb") as stream:
                    checksum = hashlib.sha256()
                    while remaining:
                        chunk = _exact(source, min(remaining, 1024 * 1024))
                        remaining -= len(chunk)
                        checksum.update(chunk)
                        if stream.write(chunk) != len(chunk):
                            raise ValueError
                    if (any(_exact(source, -entry.size_bytes % _BLOCK))
                            or "sha256:" + checksum.hexdigest() != entry.sha256):
                        raise ValueError
                    stream.flush()
                    os.fchmod(stream.fileno(), int(entry.mode, 8))
        padding = 2 * _BLOCK + (-(source.tell() + 2 * _BLOCK) % _RECORD)
        if source.tell() + padding != length or any(_exact(source, padding)):
            raise ValueError
        for entry in manifest.files:
            if entry.link_target is not None:
                with _parent(descriptor, entry.path) as (parent, name):
                    os.symlink(entry.link_target, name, dir_fd=parent)
        current = destination.stat(follow_symlinks=False)
        if (destination != destination.resolve(strict=True) or not stat.S_ISDIR(current.st_mode)
                or (current.st_dev, current.st_ino, current.st_uid) != (
                    metadata.st_dev, metadata.st_ino, metadata.st_uid)
                or current.st_mode & 0o077):
            raise ValueError
        return manifest
    except (OSError, ValueError, tarfile.TarError, UnicodeError, OverflowError):
        raise ValueError("invalid application source archive") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
