"""Read final OCI defaults without executing an image or extracting its files.

The ordinary OCI validator owns source layout safety; publication owns digest
verification. Only bounded passwd/group contents are retained while layers are
streamed in order. No task-controlled links are followed.
"""

from __future__ import annotations

import gzip
import io
import re
import tarfile
import zlib
from contextlib import ExitStack
from pathlib import Path
from typing import IO, Any, Literal, Protocol

import zstandard

from loom.runtime_environment import TASK_IMAGE_ENVIRONMENT_REJECTION
from loom.task_image_metadata import TaskImageRuntimeMetadataV1
from loom_execution_actuator import task_image_oci as oci

_IDENTITY_FILES = {"etc/passwd", "etc/group"}
_MAX_IDENTITY_BYTES = 1024 * 1024
_MAX_LAYER_BYTES = 8 * 1024**3
_MAX_LAYER_MEMBERS = 1_000_000
_NUMERIC = re.compile(r"[0-9]+\Z")
_FAILURE = "OCI runtime metadata unsupported"


class _Readable(Protocol):
    def read(self, size: int = -1) -> bytes: ...


class _BoundedReader(io.RawIOBase):
    def __init__(self, stream: _Readable, budget: list[int]) -> None:
        self.stream = stream
        self.budget = budget

    def read(self, size: int = -1) -> bytes:
        size = min(size, self.budget[0] + 1) if size >= 0 else self.budget[0] + 1
        data = self.stream.read(size)
        self.budget[0] -= len(data)
        if self.budget[0] < 0:
            raise ValueError(_FAILURE)
        return data


def _layer_identity(stream: IO[bytes], media_type: str, state: dict[str, bytes], budget: list[int]) -> None:
    with ExitStack() as stack:
        reader: _Readable = stream
        if media_type.endswith("+gzip"):
            reader = stack.enter_context(gzip.GzipFile(fileobj=stream))
        elif media_type.endswith("+zstd"):
            reader = stack.enter_context(zstandard.ZstdDecompressor().stream_reader(stream, closefd=False))
        archive = stack.enter_context(tarfile.open(fileobj=_BoundedReader(reader, budget), mode="r|"))
        changes: dict[str, bytes] = {}
        removed: set[str] = set()
        count = 0
        for member in archive:
            count += 1
            if count > _MAX_LAYER_MEMBERS or member.size < 0 or member.size > _MAX_LAYER_BYTES:
                raise ValueError(_FAILURE)
            name = member.name
            while name.startswith("./"):
                name = name[2:]
            name = name.rstrip("/")
            if name in {"", "."} and member.isdir():
                continue
            if (name.startswith("/") or "\\" in name
                    or any(part in {"", ".", ".."} for part in name.split("/"))):
                raise ValueError(_FAILURE)
            if name in {".wh.etc", ".wh..wh..opq", "etc/.wh..wh..opq"}:
                if not member.isreg() or member.size != 0:
                    raise ValueError(_FAILURE)
                removed.update(_IDENTITY_FILES)
            elif name in {"etc/.wh.passwd", "etc/.wh.group"}:
                if not member.isreg() or member.size != 0:
                    raise ValueError(_FAILURE)
                removed.add("etc/" + name.rsplit(".wh.", 1)[1])
            elif name == "etc":
                if not member.isdir():
                    raise ValueError(_FAILURE)
            elif name in _IDENTITY_FILES:
                if (not member.isreg() or member.sparse is not None
                        or member.size > _MAX_IDENTITY_BYTES or name in changes):
                    raise ValueError(_FAILURE)
                content = archive.extractfile(member)
                if content is None:
                    raise ValueError(_FAILURE)
                with content:
                    data = content.read(_MAX_IDENTITY_BYTES + 1)
                if len(data) != member.size:
                    raise ValueError(_FAILURE)
                changes[name] = data
        # OCI whiteouts remove lower-layer entries, never their replacements in
        # this same layer, regardless of the order of records in its tar stream.
        for name in removed:
            state.pop(name, None)
        state.update(changes)


def _records(data: bytes | None, columns: int) -> list[list[str]]:
    if data is None:
        return []
    lines = data.decode("utf-8").splitlines()
    if len(lines) > 10_000:
        raise ValueError(_FAILURE)
    result = []
    for line in lines:
        if not line or line.startswith("#"):
            continue
        fields = line.split(":")
        if len(line) > 4096 or len(fields) != columns:
            raise ValueError(_FAILURE)
        result.append(fields)
    if len({record[0] for record in result}) != len(result):
        raise ValueError(_FAILURE)
    return result


def _supplementary_groups(groups: list[list[str]], name: str, primary: int) -> tuple[int, ...]:
    if not name:
        return ()
    found: list[int] = []
    for row in groups:
        members = {item for item in row[3].split(",") if item}
        if name not in members:
            continue
        if not _NUMERIC.fullmatch(row[2]):
            raise ValueError(_FAILURE)
        gid = int(row[2])
        if gid != primary:
            found.append(gid)
    unique = tuple(sorted(set(found)))
    if len(unique) > 32 or any(item > 2_147_483_647 for item in unique):
        raise ValueError(_FAILURE)
    return unique


def _resolve(config: dict[str, Any], state: dict[str, bytes]) -> dict[str, Any]:
    raw_user = config.get("User")
    user = "" if raw_user is None else raw_user
    if not isinstance(user, str) or len(user) > 256 or user.count(":") > 1:
        raise ValueError(_FAILURE)
    users = _records(state.get("etc/passwd"), 7)
    groups = _records(state.get("etc/group"), 4)
    account, separator, group = user.partition(":")
    if separator and not group:
        raise ValueError(_FAILURE)
    record = next((row for row in users if row[0] == account), None)
    if account == "" or _NUMERIC.fullmatch(account):
        uid = int(account or "0")
        record = next((row for row in users if _NUMERIC.fullmatch(row[2]) and int(row[2]) == uid), None)
    else:
        if record is None or not _NUMERIC.fullmatch(record[2]):
            raise ValueError(_FAILURE)
        uid = int(record[2])
    if separator:
        if _NUMERIC.fullmatch(group):
            gid = int(group)
        else:
            group_record = next((row for row in groups if row[0] == group), None)
            if group_record is None or not _NUMERIC.fullmatch(group_record[2]):
                raise ValueError(_FAILURE)
            gid = int(group_record[2])
    else:
        if record is not None and not _NUMERIC.fullmatch(record[3]):
            raise ValueError(_FAILURE)
        gid = int(record[3]) if record is not None and account != "" else 0
    raw_env = config.get("Env")
    raw_env = [] if raw_env is None else raw_env
    if not isinstance(raw_env, list) or len(raw_env) > 64:
        raise ValueError(_FAILURE)
    environment: dict[str, str] = {}
    for item in raw_env:
        if not isinstance(item, str) or "=" not in item:
            raise ValueError(_FAILURE)
        name, value = item.split("=", 1)
        if name in environment:
            raise ValueError(_FAILURE)
        environment[name] = value
    home = environment.get("HOME", record[5] if record is not None else None)
    entrypoint = config.get("Entrypoint")
    entrypoint = [] if entrypoint is None else entrypoint
    if not isinstance(entrypoint, list) or any(not isinstance(value, str) for value in entrypoint):
        raise ValueError(_FAILURE)
    member = record[0] if record is not None else account
    result = TaskImageRuntimeMetadataV1.model_validate({
        "image_ref": "validated-local-image@sha256:" + "0" * 64,
        "user": user, "uid": uid, "gid": gid, "home": home,
        "workdir": config.get("WorkingDir") or "/", "entrypoint": entrypoint,
        "environment": environment,
        "supplementary_groups": _supplementary_groups(groups, member, gid),
    }).model_dump(mode="json")
    result.pop("image_ref")
    return result


def read_task_image_runtime_metadata(
    path: Path, *, architecture: Literal["amd64", "arm64"] = "amd64", directory: bool = False,
) -> dict[str, Any]:
    """Resolve final OCI USER/group/HOME/ENTRYPOINT; reject unsupported metadata.

    Keep the validated source immutable through this read and publication. Error
    text is deliberately fixed: config values and filesystem contents are not
    suitable for build receipts or operator logs.
    """
    try:
        validator = oci.validate_native_oci_directory if directory else oci.validate_native_oci_archive
        validator(path, architecture=architecture)
        with ExitStack() as stack:
            archive = None if directory else stack.enter_context(tarfile.open(path, "r:"))
            def open_blob(name: str) -> IO[bytes]:
                if directory:
                    return (path / name).open("rb")
                assert archive is not None
                stream = archive.extractfile(name)
                if stream is None:
                    raise ValueError(_FAILURE)
                return stream
            def read_json(name: str) -> dict[str, Any]:
                with open_blob(name) as stream:
                    return oci._json_bytes(stream.read(oci._MAX_JSON_BYTES + 1))
            def blob_name(descriptor: dict[str, Any]) -> str:
                return "blobs/sha256/" + str(descriptor["digest"]).removeprefix("sha256:")
            index = read_json("index.json")
            manifest = read_json(blob_name(index["manifests"][0]))
            config = read_json(blob_name(manifest["config"])).get("config", {})
            if not isinstance(config, dict):
                raise ValueError(_FAILURE)
            state: dict[str, bytes] = {}
            budget = [_MAX_LAYER_BYTES]
            for descriptor in manifest["layers"]:
                with open_blob(blob_name(descriptor)) as stream:
                    _layer_identity(stream, descriptor["mediaType"], state, budget)
            return _resolve(config, state)
    except (ValueError, TypeError, KeyError, OSError, EOFError, tarfile.TarError,
            zstandard.ZstdError, zlib.error, RecursionError) as error:
        safe = _safe_environment_rejection(error)
        if safe is not None:
            raise ValueError(safe) from None
        raise ValueError(_FAILURE) from None


def _safe_environment_rejection(error: BaseException) -> str | None:
    """Keep a reserved-name phrase. Drop every other validator detail and the value."""

    errors = getattr(error, "errors", None)
    if not callable(errors):
        return None
    try:
        items = errors()
    except Exception:
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        message = item.get("msg")
        if not isinstance(message, str):
            continue
        message = message.removeprefix("Value error, ").strip()
        if TASK_IMAGE_ENVIRONMENT_REJECTION.fullmatch(message):
            return message
    return None
