"""Validate untrusted OCI structure before a credentialed publisher reads it.

This checks local paths and references, not blob integrity. Skopeo owns digest
verification during publication. No image layers are unpacked or executed here.
"""

from __future__ import annotations

import json
import re
import stat
import tarfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Protocol

_MAX_BYTES = 3 * 1024**3
_MAX_MEMBERS = 10000
_MAX_JSON_BYTES = 4 * 1024**2
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_BLOB_PATH = re.compile(r"blobs/sha256/[0-9a-f]{64}\Z")
_INDEX = "application/vnd.oci.image.index.v1+json"
_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
_CONFIG = "application/vnd.oci.image.config.v1+json"
_LAYERS = {
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.oci.image.layer.v1.tar+zstd",
}


class NativeOCIArchiveError(ValueError):
    """The archive cannot safely serve as a native build publication input."""


class _BlobMember(Protocol):
    size: int

    def isreg(self) -> bool: ...


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise NativeOCIArchiveError("OCI metadata contains duplicate JSON keys")
        result[key] = value
    return result


def _json_bytes(data: bytes) -> dict[str, Any]:
    if not 0 < len(data) <= _MAX_JSON_BYTES:
        raise NativeOCIArchiveError("OCI metadata is missing or exceeds its byte limit")
    value = json.loads(data, object_pairs_hook=_object)
    if not isinstance(value, dict):
        raise NativeOCIArchiveError("OCI metadata must be an object")
    return value


def _json(archive: tarfile.TarFile, members: dict[str, tarfile.TarInfo], name: str) -> dict[str, Any]:
    member = members.get(name)
    if member is None or not member.isreg() or not 0 < member.size <= _MAX_JSON_BYTES:
        raise NativeOCIArchiveError("OCI metadata is missing or exceeds its byte limit")
    stream = archive.extractfile(member)
    assert stream is not None
    with stream:
        data = stream.read(_MAX_JSON_BYTES + 1)
    if len(data) != member.size:
        raise NativeOCIArchiveError("OCI metadata is truncated")
    return _json_bytes(data)


def _descriptor(value: Any, members: Mapping[str, _BlobMember], media_types: set[str]) -> str:
    if (not isinstance(value, dict) or not isinstance(value.get("mediaType"), str)
            or value["mediaType"] not in media_types):
        raise NativeOCIArchiveError("OCI descriptor has an unsupported media type")
    # Inline data and external URLs can make a source reader use something other
    # than the local blob whose existence is checked below.
    if "urls" in value or "data" in value:
        raise NativeOCIArchiveError("OCI descriptors must reference local blobs only")
    digest, size = value.get("digest"), value.get("size")
    if (not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            or type(size) is not int or size < 0):
        raise NativeOCIArchiveError("OCI descriptor identity or size is invalid")
    name = "blobs/sha256/" + digest.removeprefix("sha256:")
    member = members.get(name)
    if member is None or not member.isreg() or member.size != size:
        raise NativeOCIArchiveError("OCI referenced blob is missing or has the wrong size")
    return name


def _validate_image(
    *,
    members: Mapping[str, _BlobMember],
    read_json: Any,
    architecture: Literal["amd64", "arm64"],
) -> None:
    if architecture not in {"amd64", "arm64"}:
        raise NativeOCIArchiveError("OCI requested architecture is unsupported")
    layout = read_json("oci-layout")
    if layout.get("imageLayoutVersion") != "1.0.0":
        raise NativeOCIArchiveError("OCI layout version is unsupported")
    index = read_json("index.json")
    if (index.get("schemaVersion") != 2 or index.get("mediaType", _INDEX) != _INDEX
            or not isinstance(index.get("manifests"), list) or len(index["manifests"]) != 1
            or "subject" in index):
        raise NativeOCIArchiveError("OCI index must contain one local platform image")
    image = index["manifests"][0]
    manifest_name = _descriptor(image, members, {_MANIFEST})
    if "platform" in image:
        platform = image["platform"]
        if not isinstance(platform, dict) or platform.get("os") != "linux" or platform.get("architecture") != architecture:
            raise NativeOCIArchiveError(f"OCI image platform must be linux/{architecture}")
    manifest = read_json(manifest_name)
    if (manifest.get("schemaVersion") != 2 or manifest.get("mediaType", _MANIFEST) != _MANIFEST
            or not isinstance(manifest.get("layers"), list) or "subject" in manifest):
        raise NativeOCIArchiveError("OCI image manifest is invalid")
    config_name = _descriptor(manifest.get("config"), members, {_CONFIG})
    for layer in manifest["layers"]:
        _descriptor(layer, members, _LAYERS)
    config = read_json(config_name)
    if config.get("os") != "linux" or config.get("architecture") != architecture:
        raise NativeOCIArchiveError(f"OCI image configuration must be linux/{architecture}")
    rootfs = config.get("rootfs")
    if (not isinstance(rootfs, dict) or rootfs.get("type") != "layers"
            or not isinstance(rootfs.get("diff_ids"), list)
            or len(rootfs["diff_ids"]) != len(manifest["layers"])
            or any(not isinstance(value, str) or not _DIGEST.fullmatch(value) for value in rootfs["diff_ids"])):
        raise NativeOCIArchiveError("OCI image root filesystem metadata is incomplete")


def _validate(archive: tarfile.TarFile, archive_size: int, *, architecture: Literal["amd64", "arm64"]) -> None:
    members: dict[str, tarfile.TarInfo] = {}
    total = 0
    for member in archive:
        name = member.name.removesuffix("/") if member.isdir() else member.name
        if ("\\" in name or any(part in {"", ".", ".."} for part in name.split("/"))
                or name in members):
            raise NativeOCIArchiveError("OCI archive contains an unsafe or duplicate path")
        if member.isdir():
            if name not in {"blobs", "blobs/sha256"} or member.size != 0:
                raise NativeOCIArchiveError("OCI archive contains an unexpected directory")
        elif member.type not in {tarfile.REGTYPE, tarfile.AREGTYPE} or member.sparse is not None:
            raise NativeOCIArchiveError("OCI archive contains a link or special file")
        elif name not in {"oci-layout", "index.json"} and not _BLOB_PATH.fullmatch(name):
            raise NativeOCIArchiveError("OCI archive contains an unexpected file")
        if member.size < 0 or member.offset_data + member.size > archive_size:
            raise NativeOCIArchiveError("OCI archive contains a truncated member")
        # OCI layout files do not need xattrs or sparse encodings. Timestamp PAX
        # records produced by tar writers are harmless and remain supported.
        if set(member.pax_headers) - {"mtime", "atime", "ctime", "path", "size"}:
            raise NativeOCIArchiveError("OCI archive contains unsupported tar metadata")
        total += member.size
        members[name] = member
        if len(members) > _MAX_MEMBERS or total > _MAX_BYTES:
            raise NativeOCIArchiveError("OCI archive exceeds its unpacked budget")

    _validate_image(
        members=members,
        read_json=lambda name: _json(archive, members, name),
        architecture=architecture,
    )


def validate_native_oci_archive(path: Path, *, architecture: Literal["amd64", "arm64"] = "amd64") -> None:
    """Require a bounded, local, single OCI image for the specified Linux platform.

    The caller must keep the build volume immutable throughout validation and
    publication; sequential init containers and a read-only publisher mount do
    this. The archive is scanned in place, without extracting layer contents.
    """
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_BYTES:
            raise NativeOCIArchiveError("OCI archive must be a bounded regular file")
        with tarfile.open(path, "r:") as archive:
            _validate(archive, metadata.st_size, architecture=architecture)
    except NativeOCIArchiveError:
        raise
    except (OSError, tarfile.TarError, ValueError, RecursionError) as error:
        raise NativeOCIArchiveError("OCI archive or metadata is malformed") from error


class _DirMember:
    __slots__ = ("size",)

    def __init__(self, size: int) -> None:
        self.size = size

    def isreg(self) -> bool:
        return True


def validate_native_oci_directory(path: Path, *, architecture: Literal["amd64", "arm64"] = "amd64") -> None:
    """Require a bounded OCI layout directory (BuildKit tar=false) before Skopeo.

    Same layout rules as the archive validator; walks the filesystem without
    unpacking layer payloads.
    """
    try:
        root = path.lstat()
        if not stat.S_ISDIR(root.st_mode):
            raise NativeOCIArchiveError("OCI directory must be a real directory")
        members: dict[str, _DirMember] = {}
        total = 0
        for child in sorted(path.rglob("*")):
            relative = child.relative_to(path).as_posix()
            mode = child.lstat().st_mode
            if ("\\" in relative or any(part in {"", ".", ".."} for part in relative.split("/"))
                    or relative in members):
                raise NativeOCIArchiveError("OCI directory contains an unsafe or duplicate path")
            if stat.S_ISDIR(mode):
                # BuildKit tar=false leaves an empty content-store staging root.
                # Its children remain forbidden by the path rules below.
                if relative not in {"blobs", "blobs/sha256", "ingest"}:
                    raise NativeOCIArchiveError("OCI directory contains an unexpected directory")
                continue
            if not stat.S_ISREG(mode):
                raise NativeOCIArchiveError("OCI directory contains a link or special file")
            if relative not in {"oci-layout", "index.json"} and not _BLOB_PATH.fullmatch(relative):
                raise NativeOCIArchiveError("OCI directory contains an unexpected file")
            size = child.stat().st_size
            if size < 0:
                raise NativeOCIArchiveError("OCI directory contains a truncated member")
            total += size
            members[relative] = _DirMember(size)
            if len(members) > _MAX_MEMBERS or total > _MAX_BYTES:
                raise NativeOCIArchiveError("OCI directory exceeds its unpacked budget")

        def read_json(name: str) -> dict[str, Any]:
            member = members.get(name)
            if member is None or not 0 < member.size <= _MAX_JSON_BYTES:
                raise NativeOCIArchiveError("OCI metadata is missing or exceeds its byte limit")
            data = (path / name).read_bytes()
            if len(data) != member.size:
                raise NativeOCIArchiveError("OCI metadata is truncated")
            return _json_bytes(data)

        _validate_image(members=members, read_json=read_json, architecture=architecture)
    except NativeOCIArchiveError:
        raise
    except (OSError, ValueError, RecursionError) as error:
        raise NativeOCIArchiveError("OCI directory or metadata is malformed") from error
