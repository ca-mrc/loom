"""Credentialed preparation/publication; never executed alongside a Dockerfile RUN.

The intervening rootless init container sees only the scratch data volume.
Configuration is mounted separately, read-only, by the trusted actuator.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from loom_bundle_checksum import sha256_of_dir

from loom.service_execution_materialization import (
    MAX_INPUT_MANIFEST_BYTES,
    ServiceExecutionInputManifestV1,
    service_execution_input_binding,
)
from loom.task_image_build_plan import derive_task_image_build_components
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    _parse_bundle_file_metadata,
    _validate_bundle_relative_path,
    bundle_file_metadata_sha256,
)
from loom_execution_actuator.task_image_oci import (
    NativeOCIArchiveError,
    validate_native_oci_archive,
    validate_native_oci_directory,
)

_FILE_LIMIT = 2000
_BUNDLE_BYTES = 512 * 1024 * 1024
_CACHE_BYTES = 1024 * 1024 * 1024
_CACHE_TOTAL_BYTES = 4 * _CACHE_BYTES
_CACHE_REUSE_BYTES = 256 * 1024 * 1024
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_KEY = re.compile(r"[0-9a-f]{64}\Z")
# Stable Job-log markers for stage timing (Phase 2). Logs only — no DB schema.
_STAGE_KEY = "loom_task_image_stage"


class BuildPreparationError(ValueError):
    pass


def emit_stage(stage: str, event: str, **fields: Any) -> None:
    """Emit one JSON line operators can grep from prepare/build/publish logs."""
    payload = {_STAGE_KEY: stage, "event": event, **fields}
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)


@contextmanager
def stage_span(stage: str, **fields: Any) -> Iterator[None]:
    started = time.perf_counter()
    emit_stage(stage, "start", **fields)
    try:
        yield
    finally:
        emit_stage(
            stage,
            "end",
            duration_ms=int((time.perf_counter() - started) * 1000),
            **fields,
        )


@contextmanager
def cache_request_counts(client: Any, stage: str, **fields: Any) -> Iterator[None]:
    """Count actual SDK HTTP attempts (including retries), without request data."""
    events = getattr(getattr(client, "meta", None), "events", None)
    counts: Counter[str] = Counter()
    guard = Lock()
    identifier = str(uuid4())

    def sent(*, event_name: str, **kwargs: Any) -> None:
        with guard:
            counts[event_name.rsplit(".", 1)[-1]] += 1

    if events is not None:
        events.register("before-send.s3", sent, unique_id=identifier)
    try:
        yield
    finally:
        if events is not None:
            events.unregister("before-send.s3", unique_id=identifier)
        emit_stage(stage, "requests", request_attempts=dict(counts) if events else None, **fields)


def load_claim(path: Path) -> dict[str, Any]:
    if path.stat().st_size > 256 * 1024:
        raise BuildPreparationError("build claim is too large")
    claim = json.loads(path.read_text())
    if not isinstance(claim, dict):
        raise BuildPreparationError("build claim must be an object")
    for key in ("task_checksum", "materialization_key"):
        if not isinstance(claim.get(key), str) or not _KEY.fullmatch(claim[key]):
            raise BuildPreparationError("invalid build identity")
    source = claim.get("task_source", "")
    expected = "s3://" + claim["source_bucket"] + "/"
    if not isinstance(source, str) or not source.startswith(expected):
        raise BuildPreparationError("task source is outside the configured source bucket")
    prefix = source[len(expected) :]
    if not prefix.endswith("/"):
        raise BuildPreparationError("task source must be a directory prefix")
    _validate_bundle_relative_path(prefix[:-1])
    repo = claim["registry_repository"]
    if not isinstance(repo, str) or not re.fullmatch(
        r"[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9/_.-]+", repo
    ):
        raise BuildPreparationError("invalid configured task image repository")
    if len(repo) > 200:
        raise BuildPreparationError("task image repository exceeds receipt size budget")
    if len(derive_task_image_build_components(claim["task_config"])) > 8:
        raise BuildPreparationError("native build supports at most eight image components")
    hint = claim.get("cache_import_materialization_key")
    if hint is not None and (
        not isinstance(hint, str) or not _KEY.fullmatch(hint)
    ):
        raise BuildPreparationError("invalid compatible cache import identity")
    transfer = claim.get("cache_transfer", "blobs")
    if transfer not in {"tar", "blobs"}:
        raise BuildPreparationError("invalid cache transfer mode")
    export_format = claim.get("oci_export_format", "archive")
    if export_format not in {"archive", "directory"}:
        raise BuildPreparationError("invalid OCI export format")
    return claim


def _client(claim: dict[str, Any], secret: Path) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=claim["storage_endpoint"],
        region_name=claim["storage_region"],
        aws_access_key_id=(secret / "access-key").read_text().strip(),
        aws_secret_access_key=(secret / "secret-key").read_text().strip(),
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def _download(client: Any, *, bucket: str, key: str, destination: Path, limit: int) -> int:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    size = 0
    try:
        if response.get("ContentLength", 0) > limit:
            raise BuildPreparationError("input exceeds its byte limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as output:
            while chunk := body.read(min(1024 * 1024, limit - size + 1)):
                size += len(chunk)
                if size > limit:
                    raise BuildPreparationError("input exceeds its byte limit")
                output.write(chunk)
    finally:
        body.close()
    return size


def _input_manifest_modes(
    claim: dict[str, Any], client: Any, objects: list[str],
) -> dict[str, int] | None:
    binding = service_execution_input_binding(claim["task_source_provenance"])
    if binding is None:
        return None
    _, _, location = binding.manifest_uri.partition("s3://")
    bucket, _, key = location.partition("/")
    if bucket != claim["source_bucket"]:
        raise BuildPreparationError("task input manifest is outside the configured source bucket")
    _validate_bundle_relative_path(key)
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "input-manifest.json"
        _download(client, bucket=bucket, key=key, destination=path, limit=MAX_INPUT_MANIFEST_BYTES)
        body = path.read_bytes()
    if "sha256:" + hashlib.sha256(body).hexdigest() != binding.manifest_sha256:
        raise BuildPreparationError("task input manifest does not match its frozen binding")
    manifest = ServiceExecutionInputManifestV1.model_validate_json(body)
    if (
        manifest.canonical_bytes() != body
        or manifest.task_revision_sha256 != "sha256:" + claim["task_checksum"]
        or len(manifest.files) != binding.file_count
        or sum(item.size_bytes for item in manifest.files) != binding.total_bytes
        or {item.relative_path for item in manifest.files} != set(objects)
    ):
        raise BuildPreparationError("task input manifest does not match its frozen bundle")
    return {item.relative_path: int(item.mode, 8) for item in manifest.files}


def download_bundle(claim: dict[str, Any], client: Any, directory: Path) -> None:
    prefix = claim["task_source"].split("/", 3)[3]
    objects: list[str] = []
    total = 0
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=claim["source_bucket"], Prefix=prefix
    ):
        for item in page.get("Contents", []):
            relative = item["Key"][len(prefix) :]
            if relative == BUNDLE_FILE_METADATA_NAME:
                continue
            _validate_bundle_relative_path(relative)
            total += item["Size"]
            objects.append(relative)
            if len(objects) > _FILE_LIMIT or total > _BUNDLE_BYTES:
                raise BuildPreparationError("task bundle exceeds native build limits")
    if not objects:
        raise BuildPreparationError("task bundle is empty")
    # Ordinary TaskSets already publish a frozen input manifest containing modes.
    # Benchmark publishers use the older dedicated mode sidecar instead.
    modes = _input_manifest_modes(claim, client, objects)
    directory.mkdir(parents=True, exist_ok=False)
    downloaded = 0
    for relative in sorted(objects):
        downloaded += _download(
            client,
            bucket=claim["source_bucket"],
            key=prefix + relative,
            destination=directory / relative,
            limit=_BUNDLE_BYTES - downloaded,
        )
    if modes is None:
        metadata = directory / BUNDLE_FILE_METADATA_NAME
        _download(
            client,
            bucket=claim["source_bucket"],
            key=prefix + BUNDLE_FILE_METADATA_NAME,
            destination=metadata,
            limit=4 * 1024 * 1024,
        )
        modes = _parse_bundle_file_metadata(metadata.read_bytes(), expected_paths=set(objects))
        metadata.unlink()
    for relative, mode in modes.items():
        (directory / relative).chmod(mode)
    # This is the source transfer boundary; downstream phases use these bytes.
    if sha256_of_dir(directory) != claim["task_checksum"]:
        raise BuildPreparationError("task bundle content does not match its frozen revision")
    expected_modes = claim["task_source_provenance"].get("bundle_file_metadata_sha256")
    if expected_modes is not None and bundle_file_metadata_sha256(directory) != expected_modes:
        raise BuildPreparationError("task bundle file modes do not match its frozen revision")


def unpack_cache(archive: Path, directory: Path) -> None:
    """Cache is untrusted data: reject links, devices and path traversal."""
    directory.mkdir(parents=True, exist_ok=False)
    total = 0
    with tarfile.open(archive, "r:") as source:
        for index, member in enumerate(source):
            if index >= 10000 or member.size < 0:
                raise BuildPreparationError("build cache exceeds file limit")
            _validate_bundle_relative_path(member.name)
            total += member.size
            if total > _CACHE_BYTES or not (member.isdir() or member.isfile()):
                raise BuildPreparationError("build cache contains unsupported content")
            target = directory / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                stream = source.extractfile(member)
                assert stream is not None
                with stream, target.open("xb") as output:
                    while chunk := stream.read(1024 * 1024):
                        output.write(chunk)


def _cache_prefix(claim: dict[str, Any]) -> str:
    # Content-bound keys prevent a task from importing another task's private data.
    return f"task-build-cache/{claim['materialization_key']}/"


def _cache_transfer_mode(claim: dict[str, Any]) -> str:
    mode = claim.get("cache_transfer", "blobs")
    return mode if mode in {"tar", "blobs"} else "blobs"


def _oci_export_format(claim: dict[str, Any]) -> str:
    mode = claim.get("oci_export_format", "archive")
    return mode if mode in {"archive", "directory"} else "archive"


def _v2_manifest_key(materialization_key: str, index: int) -> str:
    return f"task-build-cache/v2/{materialization_key}/{index}/manifest.json"


def _v2_blob_key(digest: str) -> str:
    return f"task-build-cache/v2/blobs/{digest}"


_LEGACY_TAR = re.compile(r"task-build-cache/[0-9a-f]{64}/[0-7]\.tar\Z")
_V2_MANIFEST = re.compile(r"task-build-cache/v2/[0-9a-f]{64}/[0-7]/manifest\.json\Z")
_V2_BLOB = re.compile(r"task-build-cache/v2/blobs/[0-9a-f]{64}\Z")


def _cache_import_candidates(claim: dict[str, Any]) -> list[tuple[str, str]]:
    """Ordered (source, materialization_key) pairs for BuildKit cache import.

    Exact key first; optional same-task prior key only when the controller set a
    claim hint. Publish always writes under the claim's own materialization_key.
    """
    current = claim["materialization_key"]
    candidates = [("exact", current)]
    hint = claim.get("cache_import_materialization_key")
    if isinstance(hint, str) and _KEY.fullmatch(hint) and hint != current:
        candidates.append(("compatible", hint))
    return candidates


def _object_absent(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {
        "NoSuchKey",
        "404",
        "NotFound",
        "NoSuchBucket",
    }


def _parse_cache_manifest(body: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(body)
    except ValueError as error:
        raise BuildPreparationError("cache manifest is malformed") from error
    if (
        not isinstance(payload, dict)
        or payload.get("version") != 1
        or not isinstance(payload.get("files"), list)
        or len(payload["files"]) > 10000
    ):
        raise BuildPreparationError("cache manifest is unsupported")
    entries: list[dict[str, Any]] = []
    total = 0
    for item in payload["files"]:
        if not isinstance(item, dict):
            raise BuildPreparationError("cache manifest entry is invalid")
        path, digest, size = item.get("path"), item.get("sha256"), item.get("size")
        if (
            not isinstance(path, str)
            or not isinstance(digest, str)
            or not _KEY.fullmatch(digest)
            or type(size) is not int
            or size < 0
        ):
            raise BuildPreparationError("cache manifest entry is invalid")
        _validate_bundle_relative_path(path)
        total += size
        if total > _CACHE_BYTES:
            raise BuildPreparationError("build cache exceeds byte limit")
        entries.append({"path": path, "sha256": digest, "size": size})
    return entries


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _inventory_cache_directory(directory: Path) -> list[dict[str, Any]]:
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise BuildPreparationError("cache root must be a real directory")
    entries: list[dict[str, Any]] = []
    total = 0
    files = 0
    for path in sorted(directory.rglob("*")):
        mode = path.lstat().st_mode
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise BuildPreparationError("build cache contains unsupported content")
        if path.is_dir():
            continue
        files += 1
        if files > 10000:
            raise BuildPreparationError("build cache exceeds file limit")
        relative = path.relative_to(directory).as_posix()
        _validate_bundle_relative_path(relative)
        size = path.stat().st_size
        total += size
        if total > _CACHE_BYTES:
            raise BuildPreparationError("build cache exceeds byte limit")
        entries.append(
            {
                "path": relative,
                "sha256": _sha256_file(path),
                "size": size,
                "local": path,
            }
        )
    return entries


def _materialize_cache_blobs(
    client: Any,
    claim: dict[str, Any],
    *,
    materialization_key: str,
    index: int,
    destination: Path,
    verified: dict[str, tuple[Path, int]] | None = None,
) -> None:
    """Import a v2 manifest + content-addressed blobs into cache-in/{index}."""
    with tempfile.TemporaryDirectory(prefix="loom-cache-manifest-") as temporary:
        manifest_path = Path(temporary) / "manifest.json"
        try:
            manifest_bytes = _download(
                client,
                bucket=claim["cache_bucket"],
                key=_v2_manifest_key(materialization_key, index),
                destination=manifest_path,
                limit=4 * 1024 * 1024,
            )
        except ClientError as error:
            if _object_absent(error):
                raise FileNotFoundError from error
            raise
        entries = _parse_cache_manifest(manifest_path.read_bytes())
    destination.mkdir(parents=True, exist_ok=False)
    downloaded_bytes = 0
    downloaded: set[str] = set()
    # Only trusted prepare reads/writes these independent cache-in copies. The
    # builder starts after prepare exits; this index never survives the Job.
    if verified is not None:
        for digest, (path, size) in list(verified.items()):
            if not path.is_file() or path.is_symlink() or path.stat().st_size != size:
                del verified[digest]
    reuse_bytes = sum(size for _, size in verified.values()) if verified is not None else 0
    try:
        with tempfile.TemporaryDirectory(prefix="loom-cache-blob-") as blob_tmp:
            for entry in entries:
                target = destination / entry["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary_path = Path(blob_tmp) / entry["sha256"]
                prior = verified.get(entry["sha256"]) if verified is not None else None
                if prior is not None:
                    temporary_path = prior[0]
                elif entry["sha256"] not in downloaded:
                    downloaded_bytes += _download(
                        client,
                        bucket=claim["cache_bucket"],
                        key=_v2_blob_key(entry["sha256"]),
                        destination=temporary_path,
                        limit=max(entry["size"], 1),
                    )
                    if _sha256_file(temporary_path) != entry["sha256"]:
                        raise BuildPreparationError("cache blob digest mismatch")
                downloaded.add(entry["sha256"])
                if temporary_path.stat().st_size != entry["size"]:
                    raise BuildPreparationError("cache blob size mismatch")
                shutil.copyfile(temporary_path, target)
                if verified is not None and prior is None and reuse_bytes + entry["size"] <= _CACHE_REUSE_BYTES:
                    verified[entry["sha256"]] = (target, entry["size"])
                    reuse_bytes += entry["size"]
        emit_stage("cache_import", "transfer", component_index=index,
                   logical_bytes=sum(entry["size"] for entry in entries),
                   locally_reused_bytes=sum(entry["size"] for entry in entries) - downloaded_bytes,
                   downloaded_blob_bytes=downloaded_bytes, unique_digests=len(downloaded),
                   manifest_bytes=manifest_bytes)
    except Exception:
        shutil.rmtree(destination, ignore_errors=False)
        raise


def _blob_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if _object_absent(error):
            return False
        raise


_CACHE_MUTATION_LOCK = "task-build-cache/.mutation-lock"


def _supports_conditional_cache_write(client: Any, bucket: str) -> bool:
    # Some S3-compatible servers silently ignore If-None-Match. Qualify the
    # actual endpoint using a disposable, uniquely owned object before locking.
    key = f"task-build-cache/.conditional-probe/{uuid4()}"
    try:
        client.put_object(Bucket=bucket, Key=key, Body=b"probe")
        try:
            client.put_object(Bucket=bucket, Key=key, Body=b"probe", IfNoneMatch="*")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"PreconditionFailed", "412", "KeyAlreadyExists"}:
                return True
            raise
        return False
    finally:
        client.delete_object(Bucket=bucket, Key=key)


@contextmanager
def _cache_mutation(client: Any, bucket: str) -> Iterator[bool | None]:
    """Serialize cache publishers and GC; never steal a possibly live lock.

    A crashed holder leaves a visible, fail-closed cache-only lock. Recovery
    requires an operator to establish that all old publishers have stopped.
    """
    if not _supports_conditional_cache_write(client, bucket):
        emit_stage("cache_gc", "deferred", reason="conditional_write_unsupported")
        yield None  # Append-only cache publication is safe without GC.
        return
    try:
        client.put_object(Bucket=bucket, Key=_CACHE_MUTATION_LOCK,
                          Body=str(uuid4()).encode(), IfNoneMatch="*")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"PreconditionFailed", "412", "KeyAlreadyExists", "ConditionalRequestConflict", "409"}:
            emit_stage("cache_gc", "deferred", reason="cache_mutation_busy")
            yield False
            return
        raise
    try:
        yield True
    finally:
        client.delete_object(Bucket=bucket, Key=_CACHE_MUTATION_LOCK)


def _publish_cache_blobs(
    client: Any, claim: dict[str, Any], *, index: int, cache_dir: Path,
) -> None:
    with _cache_mutation(client, claim["cache_bucket"]) as acquired:
        if acquired is not False:
            _publish_cache_blobs_locked(client, claim, index=index, cache_dir=cache_dir,
                                        collect=acquired is True)
        else:
            emit_stage("cache_export", "deferred", reason="cache_mutation_busy", component_index=index)


def _publish_cache_blobs_locked(
    client: Any,
    claim: dict[str, Any],
    *,
    index: int,
    cache_dir: Path,
    collect: bool = True,
) -> None:
    entries = _inventory_cache_directory(cache_dir)
    unique = {entry["sha256"]: entry for entry in entries}
    manifest = {
        "version": 1,
        "files": [
            {"path": item["path"], "sha256": item["sha256"], "size": item["size"]}
            for item in entries
        ],
    }
    manifest_body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    incoming = len(manifest_body)
    for item in unique.values():
        key = _v2_blob_key(item["sha256"])
        if not _blob_exists(client, claim["cache_bucket"], key):
            incoming += item["size"]
    if collect:
        _trim_cache_locked(client, claim["cache_bucket"], incoming)
    uploaded_bytes = 0
    reused_bytes = 0
    for item in unique.values():
        key = _v2_blob_key(item["sha256"])
        if _blob_exists(client, claim["cache_bucket"], key):
            reused_bytes += item["size"]
            continue
        client.upload_file(str(item["local"]), claim["cache_bucket"], key)
        uploaded_bytes += item["size"]
    client.put_object(
        Bucket=claim["cache_bucket"],
        Key=_v2_manifest_key(claim["materialization_key"], index),
        Body=manifest_body,
    )
    emit_stage("cache_export", "transfer", component_index=index,
               logical_bytes=sum(item["size"] for item in entries), unique_digests=len(unique),
               unique_blob_bytes=sum(item["size"] for item in unique.values()),
               uploaded_blob_bytes=uploaded_bytes, reused_blob_bytes=reused_bytes,
               manifest_bytes=len(manifest_body))


def _try_import_legacy_tar(
    cache: Any,
    claim: dict[str, Any],
    *,
    source: str,
    key: str,
    index: int,
    work: Path,
    archive_parent: Path,
) -> bool:
    archive = archive_parent / f"{source}.tar"
    try:
        _download(
            cache,
            bucket=claim["cache_bucket"],
            key=f"task-build-cache/{key}/{index}.tar",
            destination=archive,
            limit=_CACHE_BYTES,
        )
    except ClientError as error:
        if _object_absent(error):
            emit_stage(
                "cache_import",
                "miss",
                component_index=index,
                reason="absent",
                source=source,
                format="tar",
            )
            return False
        raise
    cache_directory = work / "cache-in" / str(index)
    try:
        unpack_cache(archive, cache_directory)
    except (BuildPreparationError, tarfile.TarError):
        shutil.rmtree(cache_directory, ignore_errors=False)
        emit_stage(
            "cache_import",
            "miss",
            component_index=index,
            reason="invalid_archive",
            source=source,
            format="tar",
        )
        print(
            json.dumps(
                {
                    "cache": "miss",
                    "reason": "invalid_archive",
                    "component_index": index,
                    "source": source,
                }
            ),
            flush=True,
        )
        return False
    emit_stage(
        "cache_import",
        "hit",
        component_index=index,
        source=source,
        format="tar",
    )
    return True


def _try_import_cache(
    cache: Any,
    claim: dict[str, Any],
    *,
    index: int,
    work: Path,
    verified: dict[str, tuple[Path, int]] | None = None,
) -> None:
    transfer = _cache_transfer_mode(claim)
    archive_parent = Path(tempfile.mkdtemp(prefix="loom-cache-"))
    try:
        for source, key in _cache_import_candidates(claim):
            if transfer == "blobs":
                cache_directory = work / "cache-in" / str(index)
                try:
                    _materialize_cache_blobs(
                        cache,
                        claim,
                        materialization_key=key,
                        index=index,
                        destination=cache_directory,
                        verified=verified,
                    )
                except FileNotFoundError:
                    emit_stage(
                        "cache_import",
                        "miss",
                        component_index=index,
                        reason="absent",
                        source=source,
                        format="blobs",
                    )
                except (BuildPreparationError, ClientError) as error:
                    shutil.rmtree(cache_directory, ignore_errors=True)
                    reason = (
                        "invalid_blobs"
                        if isinstance(error, BuildPreparationError)
                        else "unavailable"
                    )
                    emit_stage(
                        "cache_import",
                        "miss",
                        component_index=index,
                        reason=reason,
                        source=source,
                        format="blobs",
                    )
                else:
                    emit_stage(
                        "cache_import",
                        "hit",
                        component_index=index,
                        source=source,
                        format="blobs",
                    )
                    return
                # Dual-read: warm legacy donors and mixed rollouts after a blob miss.
                if _try_import_legacy_tar(
                    cache,
                    claim,
                    source=source,
                    key=key,
                    index=index,
                    work=work,
                    archive_parent=archive_parent,
                ):
                    return
                continue
            if _try_import_legacy_tar(
                cache,
                claim,
                source=source,
                key=key,
                index=index,
                work=work,
                archive_parent=archive_parent,
            ):
                return
    finally:
        shutil.rmtree(archive_parent, ignore_errors=True)


def prepare(claim: dict[str, Any], work: Path, secrets: Path) -> None:
    with stage_span("prepare"):
        source = _client(claim, secrets / "source")
        try:
            download_bundle(claim, source, work / "context")
        finally:
            source.close()
        (work / "oci").mkdir(exist_ok=True)
        if not (secrets / "cache").is_dir():
            return
        cache = _client(claim, secrets / "cache")
        try:
            verified: dict[str, tuple[Path, int]] = {}
            for index, _ in enumerate(derive_task_image_build_components(claim["task_config"])):
                with stage_span("cache_import", component_index=index), cache_request_counts(
                    cache, "cache_import", component_index=index,
                ):
                    _try_import_cache(cache, claim, index=index, work=work, verified=verified)
        finally:
            cache.close()


def pack_cache(directory: Path, archive: Path) -> None:
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise BuildPreparationError("cache root must be a real directory")
    total = 0
    with tarfile.open(archive, "w:", dereference=True) as output:
        for index, path in enumerate(sorted(directory.rglob("*"))):
            mode = path.lstat().st_mode
            if index >= 10000 or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise BuildPreparationError("build cache contains unsupported content")
            if path.is_file():
                total += path.stat().st_size
                if total > _CACHE_BYTES:
                    raise BuildPreparationError("build cache exceeds byte limit")
            output.add(path, arcname=path.relative_to(directory).as_posix(), recursive=False)


def _read_cache_manifest(client: Any, bucket: str, key: str) -> list[str]:
    """Return blob digests. Close the response body even when the read or parse fails."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        payload = body.read(4 * 1024 * 1024 + 1)
    finally:
        body.close()
    if len(payload) > 4 * 1024 * 1024:
        raise BuildPreparationError("cache manifest exceeds byte limit")
    return [entry["sha256"] for entry in _parse_cache_manifest(payload)]


def _delete_cache_object(client: Any, bucket: str, item: dict[str, Any], deleted: set[str]) -> None:
    client.delete_object(Bucket=bucket, Key=item["Key"])
    deleted.add(item["Key"])


def trim_cache(client: Any, bucket: str, incoming_bytes: int) -> None:
    """Bound disposable cache size/age while excluding concurrent publishers."""
    with _cache_mutation(client, bucket) as acquired:
        if acquired:
            _trim_cache_locked(client, bucket, incoming_bytes)


def _trim_cache_locked(client: Any, bucket: str, incoming_bytes: int) -> None:
    with stage_span("cache_gc"), cache_request_counts(client, "cache_gc"):
        _trim_cache_inventory(client, bucket, incoming_bytes)


def _trim_cache_inventory(client: Any, bucket: str, incoming_bytes: int) -> None:
    roots: list[dict[str, Any]] = []
    blobs: list[dict[str, Any]] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="task-build-cache/"
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            if _LEGACY_TAR.fullmatch(key) or _V2_MANIFEST.fullmatch(key):
                roots.append(item)
            elif _V2_BLOB.fullmatch(key):
                blobs.append(item)
    referenced: dict[str, set[str]] = {}
    try:
        for root in roots:
            if not _V2_MANIFEST.fullmatch(root["Key"]):
                continue
            for digest in _read_cache_manifest(client, bucket, root["Key"]):
                referenced.setdefault(digest, set()).add(root["Key"])
    except (BotoCoreError, ClientError, BuildPreparationError, OSError):
        emit_stage("cache_gc", "deferred", reason="manifest_unreadable")
        return
    blobs_by_digest = {item["Key"].rsplit("/", 1)[-1]: item for item in blobs}
    total = (
        sum(item["Size"] for item in roots)
        + sum(item["Size"] for item in blobs)
        + incoming_bytes
    )
    initial_total = total
    cutoff = datetime.now(UTC) - timedelta(days=7)
    deleted: set[str] = set()
    for blob in sorted(blobs, key=lambda row: row["LastModified"]):
        digest = blob["Key"].rsplit("/", 1)[-1]
        if digest in referenced or blob["LastModified"] >= cutoff:
            continue
        _delete_cache_object(client, bucket, blob, deleted)
        total -= blob["Size"]
    for root in sorted(roots, key=lambda row: row["LastModified"]):
        if total <= _CACHE_TOTAL_BYTES:
            break
        if root["LastModified"] >= cutoff:
            continue
        exclusive: list[dict[str, Any]] = []
        if _V2_MANIFEST.fullmatch(root["Key"]):
            for digest, owners in referenced.items():
                referenced_blob = blobs_by_digest.get(digest)
                if (
                    owners == {root["Key"]}
                    and referenced_blob is not None
                    and referenced_blob["Key"] not in deleted
                    and referenced_blob["LastModified"] < cutoff
                ):
                    exclusive.append(referenced_blob)
        _delete_cache_object(client, bucket, root, deleted)
        total -= root["Size"]
        for blob in exclusive:
            _delete_cache_object(client, bucket, blob, deleted)
            total -= blob["Size"]
        for owners in referenced.values():
            owners.discard(root["Key"])
    emit_stage("cache_gc", "inventory", reclaimed_bytes=initial_total - total,
               remaining_bytes=total - incoming_bytes, incoming_bytes=incoming_bytes,
               budget_bytes=_CACHE_TOTAL_BYTES)
    if total > _CACHE_TOTAL_BYTES and any(
        item["LastModified"] >= cutoff and item["Key"] not in deleted
        for item in (*roots, *blobs)
    ):
        emit_stage("cache_gc", "deferred", reason="grace_protected")


def _output_path(work: Path, relative: str, *, directory: bool = False) -> Path:
    """No task-created ancestor may redirect a credentialed publisher's reads."""
    _validate_bundle_relative_path(relative)
    current = work
    parts = relative.split("/")
    for index, part in enumerate(parts):
        current = current / part
        mode = current.lstat().st_mode
        expected_directory = index < len(parts) - 1 or directory
        if not (stat.S_ISDIR(mode) if expected_directory else stat.S_ISREG(mode)):
            raise BuildPreparationError("build output contains a link or special file")
    return current


def publish(
    claim: dict[str, Any],
    work: Path,
    secrets: Path,
    *,
    receipt_path: Path = Path("/dev/termination-log"),
) -> dict[str, Any]:
    images: dict[str, str] = {}
    cache = _client(claim, secrets / "cache") if (secrets / "cache").is_dir() else None
    auth_directory = tempfile.TemporaryDirectory()
    export_format = _oci_export_format(claim)
    transfer = _cache_transfer_mode(claim)
    try:
        registry_auth = secrets / "registry" / "config.json"
        credentials = secrets / "registry" / "credentials.json"
        if credentials.is_file():
            from loom.nebius_registry_auth import mint_registry_auth

            registry_auth = Path(auth_directory.name) / "config.json"
            mint_registry_auth(
                credentials, "/".join(claim["registry_repository"].split("/")[:2]), registry_auth
            )
        for index, component in enumerate(derive_task_image_build_components(claim["task_config"])):
            if export_format == "directory":
                # Plan schema still emits oci/NNNN.tar; directory Jobs rewrite dest.
                relative = component.oci_output_path.removesuffix(".tar")
                output = _output_path(work, relative, directory=True)
                validate_native_oci_directory(output)
                skopeo_source = f"oci:{output}"
            else:
                archive = _output_path(work, component.oci_output_path)
                if archive.stat().st_size > 3 * _CACHE_BYTES:
                    raise BuildPreparationError("build output is not a bounded regular OCI archive")
                validate_native_oci_archive(archive)
                skopeo_source = f"oci-archive:{archive}"
            tag = f"{claim['registry_repository']}:{claim['materialization_key']}-{claim['lease_epoch']}-{index}"
            with tempfile.TemporaryDirectory() as temporary:
                digest_file = Path(temporary) / "digest"
                with stage_span("publish", component_index=index):
                    subprocess.run(
                        [
                            "skopeo",
                            "--tmpdir",
                            temporary,
                            "copy",
                            "--authfile",
                            str(registry_auth),
                            "--preserve-digests",
                            "--digestfile",
                            str(digest_file),
                            skopeo_source,
                            f"docker://{tag}",
                        ],
                        check=True,
                        timeout=300,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    digest = digest_file.read_text().strip()
                    if not _DIGEST.fullmatch(digest):
                        raise BuildPreparationError("registry did not return an OCI digest")
                    images[component.name] = f"{claim['registry_repository']}@{digest}"
                    receipt_path.write_text(
                        json.dumps(
                            {
                                "materialization_id": claim["id"],
                                "lease_epoch": claim["lease_epoch"],
                                "registry_images": images,
                            }
                        )
                    )
                cache_dir = work / "cache-out" / str(index)
                if cache is not None and (cache_dir.exists() or cache_dir.is_symlink()):
                    with stage_span("cache_export", component_index=index), cache_request_counts(
                        cache, "cache_export", component_index=index,
                    ):
                        cache_dir = _output_path(work, f"cache-out/{index}", directory=True)
                        if transfer == "blobs":
                            _publish_cache_blobs(
                                cache, claim, index=index, cache_dir=cache_dir
                            )
                        else:
                            cache_archive = Path(temporary) / "cache.tar"
                            pack_cache(cache_dir, cache_archive)
                            with _cache_mutation(cache, claim["cache_bucket"]) as acquired:
                                if acquired is not False:
                                    if acquired:
                                        _trim_cache_locked(cache, claim["cache_bucket"], cache_archive.stat().st_size)
                                    cache.upload_file(
                                        str(cache_archive), claim["cache_bucket"],
                                        _cache_prefix(claim) + f"{index}.tar",
                                    )
                                else:
                                    emit_stage("cache_export", "deferred", reason="cache_mutation_busy", component_index=index)
    finally:
        if cache is not None:
            cache.close()
        auth_directory.cleanup()
    receipt = {
        "materialization_id": claim["id"],
        "lease_epoch": claim["lease_epoch"],
        "registry_images": images,
    }
    if len(json.dumps(receipt).encode()) > 4000:
        raise BuildPreparationError(
            "publication receipt exceeds Kubernetes termination message limit"
        )
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "publish"))
    parser.add_argument("--claim", type=Path, required=True)
    args = parser.parse_args()
    try:
        claim = load_claim(args.claim)
        work, secrets = Path("/loom/build"), Path("/var/run/loom-task-build")
        if args.phase == "prepare":
            prepare(claim, work, secrets)
        else:
            receipt = publish(claim, work, secrets)
            Path("/dev/termination-log").write_text(json.dumps(receipt))
    except Exception as error:
        # SDK/tool exceptions can carry credentials, endpoints or task-controlled text.
        reason = (
            str(error)
            if isinstance(error, (BuildPreparationError, NativeOCIArchiveError))
            else type(error).__name__
        )
        error_receipt: dict[str, Any] = {}
        try:
            error_receipt = json.loads(Path("/dev/termination-log").read_text())
        except (OSError, ValueError):
            pass
        error_receipt.update(phase=args.phase, error=reason[:500])
        Path("/dev/termination-log").write_text(json.dumps(error_receipt))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
