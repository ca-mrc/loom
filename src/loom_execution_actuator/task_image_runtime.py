"""Credentialed preparation/publication; never executed alongside a Dockerfile RUN.

The intervening rootless init container sees only the scratch data volume.
Configuration is mounted separately, read-only, by the trusted actuator.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import signal
import stat
import subprocess
import tarfile
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock, current_thread, main_thread
from types import FrameType
from typing import Any, Literal
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)
from loom_bundle_checksum import sha256_of_dir

from loom.nebius_terminus_image import append_terminus_runtime_tools
from loom.runtime_environment import TASK_IMAGE_ENVIRONMENT_REJECTION
from loom.security.redaction import redact_log_text
from loom.service_execution_materialization import (
    MAX_INPUT_MANIFEST_BYTES,
    ServiceExecutionInputManifestV1,
    service_execution_input_binding,
)
from loom.task_image_build_plan import derive_task_image_build_components
from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from loom.task_image_errors import TaskImagePreparationError, classify_preparation_failure
from loom.task_image_materialization import task_bundle_content_manifest_digest
from loom.task_image_metadata import TaskImageRuntimeMetadataV1
from loom.task_image_recipe import frozen_preparation_revision
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    _parse_bundle_file_metadata,
    _validate_bundle_relative_path,
    bundle_file_metadata_sha256,
)
from loom_execution_actuator.image_publication_receipt import (
    decode_publication_receipt,
    encode_publication_receipt,
)
from loom_execution_actuator.task_image_oci import (
    NativeOCIArchiveError,
    validate_native_oci_archive,
    validate_native_oci_directory,
)

# Cache tests replace the public name with a component-count stub. Tool
# installation must keep reading the authored Dockerfiles.
_derive_task_image_build_components = derive_task_image_build_components

_FILE_LIMIT = 2000
_BUNDLE_BYTES = 512 * 1024 * 1024
# Logical bytes per component, independent of OCI publication and cache GC.
_CACHE_BYTES = 8 * 1024 * 1024 * 1024
_CACHE_TOTAL_BYTES = 4 * 1024 * 1024 * 1024
_CACHE_REUSE_BYTES = 256 * 1024 * 1024
_CACHE_MANIFEST_FILES = 10000
# A bounded starting point, not a measured optimum for every family. The DNA
# acceptance observed 12/15 GETs and about 1/2 seconds per warm import.
_CACHE_IMPORT_SECONDS = 60.0
_CACHE_IMPORT_REQUESTS = 128
_CACHE_TAR_ENTRIES = 10000
# Linux paths plus ordinary PAX time/identity records fit an 8 KiB extension.
# The writer checks the same bound that the reader checks before allocation.
_CACHE_TAR_EXTENDED_HEADER_BYTES = 8 * 1024
_CACHE_TAR_HEADER_BYTES = _CACHE_TAR_ENTRIES * (tarfile.BLOCKSIZE + _CACHE_TAR_EXTENDED_HEADER_BYTES)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_KEY = re.compile(r"[0-9a-f]{64}\Z")
# Stable Job-log markers for stage timing (Phase 2). Logs only — no DB schema.
_STAGE_KEY = "loom_task_image_stage"
_IO_FULL = re.compile(
    r"full\s+avg10=([0-9]+(?:\.[0-9]+)?)\s+avg60=[0-9]+(?:\.[0-9]+)?"
    r"\s+avg300=[0-9]+(?:\.[0-9]+)?\s+total=[0-9]+\Z"
)


class BuildPreparationError(TaskImagePreparationError):
    def __init__(self, message: str, *, code: str = "image_preparation_input_invalid") -> None:
        super().__init__(code, message)


class _CacheAbsentError(Exception):
    """Only a remote optional-cache miss, never a local filesystem failure."""


class _CacheReadUnavailableError(Exception):
    """An optional remote stream failed, independently of local filesystem I/O."""


class _CacheBudgetExceededError(Exception):
    """Optional cache exhausted its shared prepare budget."""


class _CacheImportBudget:
    def __init__(self) -> None:
        self.deadline = time.monotonic() + _CACHE_IMPORT_SECONDS
        self.requests = 0
        self.external_alarm_exception = False

    def check(self) -> None:
        if time.monotonic() >= self.deadline:
            raise _CacheBudgetExceededError

    @contextmanager
    def network(self, *, request: bool = False) -> Iterator[None]:
        self.check()
        if request:
            if self.requests >= _CACHE_IMPORT_REQUESTS:
                raise _CacheBudgetExceededError
            self.requests += 1
        # Trusted prepare entrypoints run in the main thread. Library callers
        # in other threads retain cooperative checks and the SDK timeout bound.
        if current_thread() is not main_thread() or not hasattr(signal, 'setitimer'):
            yield
            self.check()
            return
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        if previous_handler is None:
            yield
            self.check()
            return
        remaining = self.deadline - time.monotonic()
        earlier = 0 < previous_timer[0] <= remaining
        started = time.monotonic()

        def preserve(signum: int, frame: FrameType | None) -> None:
            assert callable(previous_handler)
            try:
                previous_handler(signum, frame)
            except BaseException:
                self.external_alarm_exception = True
                raise

        def expired(_signum: int, _frame: FrameType | None) -> None:
            raise _CacheBudgetExceededError

        if earlier:
            # Do not move an earlier caller deadline. Mark its exception so a
            # caller's BotoCoreError/ValueError cannot become an optional miss.
            # If that handler returns or ignores the signal, this library-only
            # branch remains cooperative + SDK-bounded until the call returns.
            # Normal main-thread prepare entrypoints have no outer alarm.
            if callable(previous_handler):
                signal.signal(signal.SIGALRM, preserve)
            try:
                yield
            finally:
                signal.signal(signal.SIGALRM, previous_handler)
            self.check()
            return
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(remaining, 0.000001))
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
            if previous_timer[0] > 0:
                left = previous_timer[0] - (time.monotonic() - started)
                signal.setitimer(signal.ITIMER_REAL, max(left, 0.000001), previous_timer[1])
                if left <= 0 and callable(previous_handler):
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    preserve(signal.SIGALRM, None)
        self.check()


class PublishCommandError(Exception):
    """Skopeo failure whose message is already a bounded redacted summary.

    ``str(CalledProcessError)`` includes the argv, so the publisher must not
    stringify that exception into a termination receipt.
    """

    def __init__(self, message: str, *, exit_code: int | None = None) -> None:
        super().__init__(message)
        self.exit_code = exit_code


_SKOPEO_COPY_TIMEOUT_SECONDS = 300
_PUBLISH_STDERR_TAIL_BYTES = 4096
_PUBLISH_ERROR_CHARS = 500
_DOCKER_REF_RE = re.compile(r"docker://\S+")


def _metadata_publication_error(error: BaseException) -> str:
    """Keep a reserved image-variable name. Hide every other metadata detail."""

    message = str(error)
    if TASK_IMAGE_ENVIRONMENT_REJECTION.fullmatch(message):
        return message
    return "OCI runtime metadata is unsupported"


def termination_failure_update(error: BaseException) -> dict[str, Any]:
    """Fields safe to merge into a termination receipt.

    Untyped SDK and process exceptions contribute only their class name.
    """

    reason = (
        str(error)
        if isinstance(error, (BuildPreparationError, NativeOCIArchiveError, PublishCommandError))
        else type(error).__name__
    )
    update: dict[str, Any] = {"error": reason[:_PUBLISH_ERROR_CHARS]}
    if (
        isinstance(error, PublishCommandError)
        and type(error.exit_code) is int
        and 0 <= error.exit_code <= 255
    ):
        update["exit_code"] = error.exit_code
    return update


def _read_tail_text(path: Path, *, limit: int) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return ""
    with path.open("rb") as handle:
        if size > limit:
            handle.seek(size - limit)
        data = handle.read(limit)
    return data.decode("utf-8", errors="replace")


def _sanitize_publish_stderr(
    text: str, *, registry_repository: str, authfile: str, argv: list[str],
) -> str:
    rendered = " ".join(argv)
    if rendered:
        text = text.replace(rendered, "")
    if authfile:
        text = text.replace(authfile, "[authfile]")
    if registry_repository:
        text = text.replace(registry_repository, "[registry]")
    text = _DOCKER_REF_RE.sub("docker://[registry]", text)
    summary = redact_log_text(text).strip()
    if len(summary) > _PUBLISH_ERROR_CHARS:
        summary = summary[-_PUBLISH_ERROR_CHARS:]
    return summary or "skopeo copy failed"


def _publish_command_error(
    stderr_path: Path, *, registry_repository: str, authfile: str, argv: list[str], exit_code: int,
) -> PublishCommandError:
    summary = _sanitize_publish_stderr(
        _read_tail_text(stderr_path, limit=_PUBLISH_STDERR_TAIL_BYTES),
        registry_repository=registry_repository, authfile=authfile, argv=argv,
    )
    bounded_exit = exit_code if 0 <= exit_code <= 255 else None
    return PublishCommandError(summary, exit_code=bounded_exit)


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
    _cache_total_bytes(claim)
    _cache_gc_policy(claim)
    if "reuse_task_cache_for_verifier" in claim and type(claim["reuse_task_cache_for_verifier"]) is not bool:
        raise BuildPreparationError("invalid sibling cache policy")
    _io_full_avg10_limit(claim)
    return claim


def _cache_total_bytes(claim: dict[str, Any]) -> int:
    """Read the frozen store budget; old claims retain the historical default."""
    if "cache_total_mib" not in claim:
        return _CACHE_TOTAL_BYTES
    value = claim["cache_total_mib"]
    if type(value) is not int or not 1024 <= value <= 1048576:
        raise BuildPreparationError("invalid cache total budget")
    return value * 1024 * 1024


def _cache_gc_policy(claim: dict[str, Any]) -> str:
    value = claim.get("cache_gc_policy", "legacy-seven-day")
    if not isinstance(value, str) or value not in ("legacy-seven-day", "budget-pressure"):
        raise BuildPreparationError("invalid cache GC policy")
    return value


def _io_full_avg10_limit(claim: dict[str, Any]) -> float | None:
    if "io_full_avg10_max" not in claim:
        return None
    value = claim["io_full_avg10_max"]
    if type(value) not in {int, float} or not 0 <= value <= 100 or not math.isfinite(value):
        raise BuildPreparationError("invalid I/O pressure threshold")
    return float(value)


def _parse_io_full_avg10(body: str) -> float | None:
    full = [line.strip() for line in body.splitlines() if line.split()[:1] == ["full"]]
    if len(full) != 1 or (match := _IO_FULL.fullmatch(full[0])) is None:
        return None
    value = float(match[1])
    return value if math.isfinite(value) and 0 <= value <= 100 else None


def _wait_for_io_health(claim: dict[str, Any]) -> None:
    maximum = _io_full_avg10_limit(claim)
    if maximum is None:
        return
    with stage_span("io_pressure", full_avg10_max=maximum):
        while True:
            try:
                value = _parse_io_full_avg10(Path("/proc/pressure/io").read_text())
            except (OSError, UnicodeError):
                value = None
            if value is not None and value <= maximum:
                emit_stage("io_pressure", "healthy", full_avg10=value, full_avg10_max=maximum)
                return
            emit_stage(
                "io_pressure", "wait", full_avg10=value, full_avg10_max=maximum,
                poll_interval_sec=5,
            )
            # The existing Kubernetes Job deadline bounds waiting and the build.
            time.sleep(5)


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


def _download(client: Any, *, bucket: str, key: str, destination: Path, limit: int,
              optional_cache: bool = False, cache_budget: _CacheImportBudget | None = None) -> int:
    response: Any = None
    try:
        if cache_budget is None:
            response = client.get_object(Bucket=bucket, Key=key)
        else:
            with cache_budget.network(request=True):
                response = client.get_object(Bucket=bucket, Key=key)
    except BaseException:
        if response is not None:
            response['Body'].close()
        raise
    body = response["Body"]
    size = 0
    try:
        if response.get("ContentLength", 0) > limit:
            raise BuildPreparationError("input exceeds its byte limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as output:
            while True:
                try:
                    if cache_budget is None:
                        chunk = body.read(min(1024 * 1024, limit - size + 1))
                    else:
                        with cache_budget.network():
                            chunk = body.read(min(1024 * 1024, limit - size + 1))
                except OSError as error:
                    if optional_cache:
                        raise _CacheReadUnavailableError from error
                    raise
                if not chunk:
                    break
                size += len(chunk)
                if size > limit:
                    raise BuildPreparationError("input exceeds its byte limit")
                output.write(chunk)
                if cache_budget is not None:
                    cache_budget.check()
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
    registered_digest = task_bundle_content_manifest_digest(claim["task_source_provenance"])
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
    if registered_digest:
        # Reuse the registration algorithm on the actual downloaded bytes and
        # restored modes. Legacy checksum/mode checks alone do not bind the v2
        # identity, including canonical file paths, lengths and per-file hashes.
        try:
            measured = capture_task_image_bundle_manifest(directory)
        except ValueError:
            raise BuildPreparationError("task bundle does not match registered content") from None
        if (measured.digest != registered_digest or measured.task_checksum != claim["task_checksum"]
                or claim["task_source_provenance"].get("bundle_file_metadata_sha256")
                != "sha256:" + measured.bundle_file_metadata_sha256):
            raise BuildPreparationError("task bundle does not match registered content")
        return
    # This is the source transfer boundary; downstream phases use these bytes.
    if sha256_of_dir(directory) != claim["task_checksum"]:
        raise BuildPreparationError("task bundle content does not match its frozen revision")
    expected_modes = claim["task_source_provenance"].get("bundle_file_metadata_sha256")
    if expected_modes is not None and bundle_file_metadata_sha256(directory) != expected_modes:
        raise BuildPreparationError("task bundle file modes do not match its frozen revision")


def _cache_tar_byte_limit() -> int:
    # Logical payload excludes each main header, up to 511 payload-padding
    # bytes, bounded PAX/GNU headers, two EOF blocks and record alignment.
    return (_CACHE_BYTES + _CACHE_TAR_ENTRIES * (2 * tarfile.BLOCKSIZE - 1)
            + _CACHE_TAR_HEADER_BYTES + 2 * tarfile.BLOCKSIZE + tarfile.RECORDSIZE)


class _CacheTarInfo(tarfile.TarInfo):
    """Keep stdlib tar parsing, but bound extension reads before allocation."""

    def tobuf(self, format: Literal[0, 1, 2] | None = tarfile.DEFAULT_FORMAT,
              encoding: str | None = tarfile.ENCODING,
              errors: str = "surrogateescape") -> bytes:
        header = super().tobuf(format, encoding, errors)
        if len(header) > _CACHE_TAR_EXTENDED_HEADER_BYTES + 2 * tarfile.BLOCKSIZE:
            raise BuildPreparationError("build cache tar header exceeds byte limit")
        return header

    def _proc_member(self, archive: Any) -> Any:
        if self.size < 0 or self.type == tarfile.GNUTYPE_SPARSE:
            raise BuildPreparationError("build cache contains unsupported content")
        extended = self.type in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE,
                                 tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK)
        if not extended:
            return super()._proc_member(archive)  # type: ignore[misc]  # stdlib parser hook is not in typeshed
        padded = (self.size + tarfile.BLOCKSIZE - 1) // tarfile.BLOCKSIZE * tarfile.BLOCKSIZE
        headers = getattr(archive, "_cache_header_bytes", 0) + tarfile.BLOCKSIZE + padded
        depth = getattr(archive, "_cache_header_depth", 0)
        if self.size > _CACHE_TAR_EXTENDED_HEADER_BYTES or headers > _CACHE_TAR_HEADER_BYTES or depth >= 2:
            raise BuildPreparationError("build cache tar header exceeds byte limit")
        archive._cache_header_bytes = headers
        archive._cache_header_depth = depth + 1
        try:
            member = super()._proc_member(archive)  # type: ignore[misc]
        finally:
            archive._cache_header_depth = depth
        # Global headers may accumulate across members. Bound that retained
        # dictionary as well as each individual extension read.
        if sum(len(key.encode("utf-8", "surrogateescape")) + len(value.encode("utf-8", "surrogateescape"))
               for key, value in archive.pax_headers.items()) > _CACHE_TAR_EXTENDED_HEADER_BYTES:
            raise BuildPreparationError("build cache tar global header exceeds byte limit")
        return member

    def _proc_gnusparse_10(self, *_args: Any) -> None:
        # Its sparse map is read from payload, outside the bounded PAX body.
        # Ordinary pack_cache never emits sparse formats; it streams all bytes.
        raise BuildPreparationError("build cache contains unsupported content")


def unpack_cache(archive: Path, directory: Path) -> None:
    """Cache is untrusted data: reject links, devices and path traversal."""
    directory.mkdir(parents=True, exist_ok=False)
    info = archive.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > _cache_tar_byte_limit():
        raise BuildPreparationError("build cache tar exceeds transfer byte limit")
    total = 0
    paths: dict[str, bool] = {}
    with tarfile.open(archive, "r|", tarinfo=_CacheTarInfo) as source:
        for index, member in enumerate(source):
            if index >= _CACHE_TAR_ENTRIES or member.size < 0:
                raise BuildPreparationError("build cache exceeds file limit")
            _validate_bundle_relative_path(member.name)
            if len(member.name) > 4096 or member.name.count("/") > 128:
                raise BuildPreparationError("cache archive path exceeds limit")
            if (member.name in paths
                    or any(not paths.get("/".join(member.name.split("/")[:n]), True)
                           for n in range(1, len(member.name.split("/"))))
                    or (not member.isdir() and any(path.startswith(member.name + "/") for path in paths))):
                raise BuildPreparationError("cache archive paths conflict")
            paths[member.name] = member.isdir()
            total += member.size
            if total > _CACHE_BYTES or member.sparse is not None or not (member.isdir() or member.isfile()):
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
            # Extraction uses this member directly, never name lookups. Do not
            # retain every PAX dictionary while streaming thousands of entries.
            source.members.clear()  # type: ignore[attr-defined]  # stdlib streaming member cache


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
        or len(payload["files"]) > _CACHE_MANIFEST_FILES
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
            or len(path) > 4096 or path.count("/") > 128
            or not isinstance(digest, str)
            or not _KEY.fullmatch(digest)
            or type(size) is not int
            or size < 0
        ):
            raise BuildPreparationError("cache manifest entry is invalid")
        try:
            _validate_bundle_relative_path(path)
        except ValueError as error:
            raise BuildPreparationError("cache manifest path is invalid") from error
        total += size
        if total > _CACHE_BYTES:
            raise BuildPreparationError("build cache exceeds byte limit")
        entries.append({"path": path, "sha256": digest, "size": size})
    paths = {entry["path"] for entry in entries}
    if len(paths) != len(entries) or any(
        "/".join(path.split("/")[:index]) in paths
        for path in paths for index in range(1, len(path.split("/")))
    ):
        raise BuildPreparationError("cache manifest paths conflict")
    return entries


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_directory_files(directory: Path) -> list[tuple[Path, str, int]]:
    """Validate the entire cache tree before an optional capacity deferral.

    An oversized early file must not conceal a later link or special file.
    This pass reads metadata only; content is hashed only for bounded exports.
    """
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise BuildPreparationError("cache root must be a real directory")
    files: list[tuple[Path, str, int]] = []
    for index, path in enumerate(sorted(directory.rglob("*"))):
        if index >= 10000:
            raise BuildPreparationError("build cache exceeds file limit")
        mode = path.lstat().st_mode
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise BuildPreparationError("build cache contains unsupported content")
        relative = path.relative_to(directory).as_posix()
        _validate_bundle_relative_path(relative)
        if stat.S_ISDIR(mode):
            continue
        size = path.stat().st_size
        files.append((path, relative, size))
    return files


def _inventory_cache_directory(directory: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    total = 0
    for path, relative, size in _cache_directory_files(directory):
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
    budget: _CacheImportBudget | None = None,
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
                optional_cache=True,
                cache_budget=budget,
            )
        except ClientError as error:
            if _object_absent(error):
                raise _CacheAbsentError from error
            raise
        entries = _parse_cache_manifest(manifest_path.read_bytes())
        if budget is not None:
            budget.check()
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / (".partial-" + uuid4().hex)
    staging.mkdir()
    downloaded_bytes = 0
    downloaded: set[str] = set()
    # Only trusted prepare reads/writes these independent cache-in copies. The
    # builder starts after prepare exits; this index never survives the Job.
    if verified is not None:
        for digest, (path, size) in list(verified.items()):
            if not path.is_file() or path.is_symlink() or path.stat().st_size != size:
                del verified[digest]
    reuse_bytes = sum(size for _, size in verified.values()) if verified is not None else 0
    committed: dict[str, tuple[Path, int]] = {}
    try:
        with tempfile.TemporaryDirectory(prefix="loom-cache-blob-") as blob_tmp:
            for entry in entries:
                if budget is not None:
                    budget.check()
                target = staging / entry["path"]
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
                        optional_cache=True,
                        cache_budget=budget,
                    )
                    if _sha256_file(temporary_path) != entry["sha256"]:
                        raise BuildPreparationError("cache blob digest mismatch")
                downloaded.add(entry["sha256"])
                if temporary_path.stat().st_size != entry["size"]:
                    raise BuildPreparationError("cache blob size mismatch")
                shutil.copyfile(temporary_path, target)
                if budget is not None:
                    budget.check()
                if (verified is not None and prior is None and entry["sha256"] not in committed
                        and reuse_bytes + entry["size"] <= _CACHE_REUSE_BYTES):
                    committed[entry["sha256"]] = (destination / entry["path"], entry["size"])
                    reuse_bytes += entry["size"]
        staging.rename(destination)
        if verified is not None:
            verified.update(committed)
        emit_stage("cache_import", "transfer", component_index=index,
                   logical_bytes=sum(entry["size"] for entry in entries),
                   locally_reused_bytes=sum(entry["size"] for entry in entries) - downloaded_bytes,
                   downloaded_blob_bytes=downloaded_bytes, unique_digests=len(downloaded),
                   manifest_bytes=manifest_bytes)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _blob_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if _object_absent(error):
            return False
        raise


_CACHE_MUTATION_LOCK = "task-build-cache/.mutation-lock"
_CACHE_GC_CHECKPOINT = "task-build-cache/.gc-checkpoint.json"
_CACHE_GC_PUBLICATIONS = 64
_CACHE_GC_BYTES = 256 * 1024 * 1024
_CACHE_GC_SECONDS = 300
_CACHE_GC_BACKOFF_SECONDS = 1800
_CACHE_GC_MAX_OBJECTS = 20000
_CACHE_GC_MAX_MANIFESTS = 1024
_CACHE_GC_MAX_MANIFEST_BYTES = 32 * 1024 * 1024
_CACHE_GC_MAX_SECONDS = 30
_CACHE_GC_ROOT_BATCH = 8
_CACHE_GC_ORPHAN_BATCH = 256
_CACHE_GC_CONTROL = "task-build-cache/.gc-scan/"


def _read_cache_checkpoint(client: Any, bucket: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket, Key=_CACHE_GC_CHECKPOINT)
    except ClientError as error:
        if _object_absent(error):
            return None
        raise
    body = response["Body"]
    try:
        raw = body.read(4097)
    finally:
        body.close()
    try:
        state = json.loads(raw)
        if (
            len(raw) > 4096 or not isinstance(state, dict)
            or (state.get("version") != 1 and not (
                state.get("version") == 2 and state.get("inventory_complete") is False))
            or any(type(state.get(field)) is not int or state[field] < 0
                   for field in ("retained_bytes", "debt_bytes", "publications"))
            or type(state.get("scanned_at")) not in {int, float}
            or not math.isfinite(state["scanned_at"])
            or not 0 < state["scanned_at"] <= time.time() + 60
            or type(state.get("inventory_complete", True)) is not bool
            or ("retry_after" in state and (type(state["retry_after"]) not in {int, float}
                or not math.isfinite(state["retry_after"]) or state["retry_after"] < 0))
        ):
            raise ValueError
    except (ValueError, TypeError, UnicodeError) as error:
        raise BuildPreparationError("invalid cache GC checkpoint") from error
    return state


def _write_cache_checkpoint(client: Any, bucket: str, state: dict[str, Any]) -> None:
    client.put_object(Bucket=bucket, Key=_CACHE_GC_CHECKPOINT,
                      Body=json.dumps(state, separators=(",", ":"), sort_keys=True).encode())


def _cache_checkpoint(retained_bytes: int) -> dict[str, Any]:
    return {"version": 1, "scanned_at": time.time(), "retained_bytes": retained_bytes,
            "debt_bytes": 0, "publications": 0}


def _reserve_cache_publication(
    client: Any, bucket: str, incoming_bytes: int, *, refresh_incoming: Callable[[], int] | None = None,
    total_bytes: int | None = None, gc_policy: str = "legacy-seven-day",
) -> bool:
    """Under the mutation lock, pre-account an upper bound before writing data.

    Ordinary publication reads/writes one checkpoint, not the retained inventory.
    A crash or ambiguous PUT can overcount; only a complete sweep resets debt.
    """
    budget = _CACHE_TOTAL_BYTES if total_bytes is None else total_bytes
    _cache_gc_policy({"cache_gc_policy": gc_policy})
    try:
        state = _read_cache_checkpoint(client, bucket)
        policy_changed = state is not None and (
            state.get("gc_policy", "legacy-seven-day") != gc_policy
            or state.get("budget_bytes", budget) != budget)
        backoff = state is not None and not policy_changed and state.get("retry_after", 0) > time.time()
        if (state is not None and backoff and (not state.get("inventory_complete", True)
                or state["retained_bytes"] + state["debt_bytes"] + incoming_bytes > budget)):
            emit_stage("cache_export", "deferred", reason="cache_gc_backoff", budget_bytes=budget)
            return False
        expired = state is not None and time.time() - state["scanned_at"] >= _CACHE_GC_SECONDS
        over_budget = state is not None and (
            state["retained_bytes"] + state["debt_bytes"] + incoming_bytes > budget
        )
        incomplete = state is not None and not state.get("inventory_complete", True)
        due = not backoff and (state is None or expired or incomplete or (policy_changed and over_budget) or (
            state["publications"] >= _CACHE_GC_PUBLICATIONS
            or state["debt_bytes"] + incoming_bytes > _CACHE_GC_BYTES
            or over_budget
        ))
        # A just-completed sweep cannot reclaim grace-protected data. Avoid a
        # full scan for every refused publication while waiting for its next tick.
        if due and state is not None and not expired and not incomplete and not policy_changed and state["publications"] == 0:
            due = False
        if due:
            remaining = _trim_cache_locked(client, bucket, incoming_bytes, total_bytes=budget, gc_policy=gc_policy)
            if remaining is None:
                # The collector owns the durable continuation. Do not replace
                # its newer cursor with this caller's pre-scan checkpoint.
                emit_stage("cache_export", "deferred", reason="cache_gc_incomplete")
                return False
            state = _cache_checkpoint(remaining)
            state.update(gc_policy=gc_policy, budget_bytes=budget)
            # The sweep may have removed an old unreferenced blob that the
            # publisher's first HEAD saw. Refresh under this same lock before
            # reserving or reusing its missing set; never discount deleted data.
            if refresh_incoming is not None:
                incoming_bytes = refresh_incoming()
        assert state is not None
        if state["retained_bytes"] + state["debt_bytes"] + incoming_bytes > budget:
            state.update(gc_policy=gc_policy, budget_bytes=budget,
                         retry_after=time.time() + _CACHE_GC_BACKOFF_SECONDS)
            _write_cache_checkpoint(client, bucket, state)
            emit_stage("cache_export", "deferred", reason="cache_budget_protected",
                       accounted_bytes=state["retained_bytes"] + state["debt_bytes"],
                       incoming_bytes=incoming_bytes, budget_bytes=budget)
            return False
        state = {**state, "debt_bytes": state["debt_bytes"] + incoming_bytes,
                 "publications": state["publications"] + 1, "gc_policy": gc_policy, "budget_bytes": budget}
        if policy_changed:
            state.pop("retry_after", None)
        _write_cache_checkpoint(client, bucket, state)
    except (BotoCoreError, ClientError, BuildPreparationError, OSError):
        emit_stage("cache_export", "deferred", reason="cache_checkpoint_unavailable")
        return False
    emit_stage("cache_gc", "debt", publications=state["publications"],
               debt_bytes=state["debt_bytes"], retained_bytes=state["retained_bytes"],
               publication_limit=_CACHE_GC_PUBLICATIONS, byte_batch=_CACHE_GC_BYTES,
               budget_bytes=budget)
    return True


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
        yield None  # Callers defer optional cache: no safe bounded collector exists.
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
    started = time.perf_counter()
    try:
        yield True
    finally:
        client.delete_object(Bucket=bucket, Key=_CACHE_MUTATION_LOCK)
        emit_stage("cache_gc", "lock_released", duration_ms=int((time.perf_counter() - started) * 1000))


def _publish_cache_blobs(
    client: Any, claim: dict[str, Any], *, index: int, cache_dir: Path,
) -> None:
    # Hash and validate the bounded, immutable post-build output before taking
    # the shared store lock. Only object-store mutation needs serialization.
    entries = _inventory_cache_directory(cache_dir)
    with _cache_mutation(client, claim["cache_bucket"]) as acquired:
        if acquired is True:
            _publish_cache_blobs_locked(client, claim, index=index, entries=entries)
        else:
            emit_stage("cache_export", "deferred", reason=(
                "cache_mutation_busy" if acquired is False else "conditional_write_unsupported"
            ), component_index=index)


def _publish_cache_blobs_locked(
    client: Any,
    claim: dict[str, Any],
    *,
    index: int,
    entries: list[dict[str, Any]],
) -> None:
    unique = {entry["sha256"]: entry for entry in entries}
    manifest = {
        "version": 1,
        "files": [
            {"path": item["path"], "sha256": item["sha256"], "size": item["size"]}
            for item in entries
        ],
    }
    manifest_body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    missing: set[str] = set()

    def missing_bytes() -> int:
        missing.clear()
        missing.update(sha for sha in unique if not _blob_exists(
            client, claim["cache_bucket"], _v2_blob_key(sha),
        ))
        return len(manifest_body) + sum(int(unique[sha]["size"]) for sha in missing)

    # Shared warm blobs do not create new storage debt. Recheck only after a
    # sweep; ordinary publication reuses this missing set for its uploads.
    incoming = missing_bytes()
    if not _reserve_cache_publication(
        client, claim["cache_bucket"], incoming, refresh_incoming=missing_bytes,
        total_bytes=_cache_total_bytes(claim),
        gc_policy=_cache_gc_policy(claim),
    ):
        return
    uploaded_bytes = 0
    reused_bytes = 0
    for sha, item in unique.items():
        key = _v2_blob_key(sha)
        if sha not in missing:
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
    budget: _CacheImportBudget | None = None,
) -> bool:
    archive = archive_parent / f"{source}.tar"
    try:
        _download(
            cache,
            bucket=claim["cache_bucket"],
            key=f"task-build-cache/{key}/{index}.tar",
            destination=archive,
            limit=_cache_tar_byte_limit(),
            optional_cache=True,
            cache_budget=budget,
        )
    except (BotoCoreError, ClientError, BuildPreparationError, _CacheReadUnavailableError) as error:
        if budget is not None and budget.external_alarm_exception:
            raise
        emit_stage("cache_import", "miss", component_index=index,
                   reason="absent" if isinstance(error, ClientError) and _object_absent(error) else "unavailable",
                   source=source, format="tar")
        return False
    cache_directory = work / "cache-in" / str(index)
    cache_directory.parent.mkdir(parents=True, exist_ok=True)
    staging = cache_directory.parent / (".partial-" + uuid4().hex)
    try:
        unpack_cache(archive, staging)
        staging.rename(cache_directory)
    except (BuildPreparationError, tarfile.TarError, ValueError):
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
    finally:
        if staging.exists():
            shutil.rmtree(staging)
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
    budget: _CacheImportBudget | None = None,
) -> None:
    budget = budget or _CacheImportBudget()
    transfer = _cache_transfer_mode(claim)
    archive_parent = Path(tempfile.mkdtemp(prefix="loom-cache-"))
    try:
        for source, key in _cache_import_candidates(claim):
            budget.check()
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
                        budget=budget,
                    )
                except _CacheAbsentError:
                    emit_stage(
                        "cache_import",
                        "miss",
                        component_index=index,
                        reason="absent",
                        source=source,
                        format="blobs",
                    )
                except (BuildPreparationError, ClientError, BotoCoreError, _CacheReadUnavailableError) as error:
                    if budget.external_alarm_exception:
                        raise
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
                    budget=budget,
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
                budget=budget,
            ):
                return
    except _CacheBudgetExceededError:
        if budget.external_alarm_exception:
            raise
        emit_stage('cache_import', 'miss', component_index=index, reason='cache_budget_exhausted',
                   request_count=budget.requests, budget_seconds=_CACHE_IMPORT_SECONDS)
    finally:
        shutil.rmtree(archive_parent)


def _install_terminus_runtime_tools(claim: dict[str, Any], context: Path) -> None:
    """Prepare the primary task Dockerfile before buildkit reads it.

    Sidecars stay as authored. An unsupported base fails this build instead of
    the first trial.
    """
    for component in _derive_task_image_build_components(claim["task_config"]):
        if component.name not in {"task", "verifier"}:
            continue
        dockerfile = context / component.dockerfile_path
        environment = (claim["task_config"].get("environment", {}) if component.name == "task"
                       else claim["task_config"].get("verifier", {}).get("environment") or {})
        try:
            rewritten = append_terminus_runtime_tools(
                dockerfile.read_text(), prepare_harness_dirs=bool(frozen_preparation_revision(claim)),
                build_arg_names=environment.get("docker_build_args", {}),
            )
        except TaskImagePreparationError as exc:
            raise BuildPreparationError(str(exc), code=exc.code) from exc
        except (OSError, UnicodeError, ValueError) as exc:
            raise BuildPreparationError(str(exc)) from exc
        dockerfile.write_text(rewritten)


def prepare(claim: dict[str, Any], work: Path, secrets: Path) -> None:
    with stage_span("prepare"):
        source = _client(claim, secrets / "source")
        try:
            download_bundle(claim, source, work / "context")
            _install_terminus_runtime_tools(claim, work / "context")
        finally:
            source.close()
        (work / "oci").mkdir(exist_ok=True)
        if (secrets / "cache").is_dir():
            cache = _client(claim, secrets / "cache")
            try:
                verified: dict[str, tuple[Path, int]] = {}
                budget = _CacheImportBudget()
                for index, _ in enumerate(derive_task_image_build_components(claim["task_config"])):
                    with stage_span("cache_import", component_index=index), cache_request_counts(
                        cache, "cache_import", component_index=index,
                    ):
                        _try_import_cache(cache, claim, index=index, work=work, verified=verified, budget=budget)
            finally:
                cache.close()
        _wait_for_io_health(claim)


def pack_cache(directory: Path, archive: Path) -> None:
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise BuildPreparationError("cache root must be a real directory")
    total = 0
    with tarfile.open(archive, "w:", dereference=True, tarinfo=_CacheTarInfo) as output:
        for index, path in enumerate(sorted(directory.rglob("*"))):
            mode = path.lstat().st_mode
            if index >= _CACHE_TAR_ENTRIES or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise BuildPreparationError("build cache contains unsupported content")
            if path.is_file():
                total += path.stat().st_size
                if total > _CACHE_BYTES:
                    raise BuildPreparationError("build cache exceeds byte limit")
            output.add(path, arcname=path.relative_to(directory).as_posix(), recursive=False)


def _read_cache_manifest(client: Any, bucket: str, key: str, *, limit: int = 4 * 1024 * 1024) -> tuple[list[str], int]:
    """Return blob digests. Close the response body even when the read or parse fails."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        payload = body.read(limit + 1)
    finally:
        body.close()
    if len(payload) > limit:
        raise BuildPreparationError("cache manifest exceeds byte limit")
    return [entry["sha256"] for entry in _parse_cache_manifest(payload)], len(payload)


def _delete_cache_object(client: Any, bucket: str, item: dict[str, Any], deleted: set[str]) -> None:
    client.delete_object(Bucket=bucket, Key=item["Key"])
    deleted.add(item["Key"])


def trim_cache(client: Any, bucket: str, incoming_bytes: int, *, total_bytes: int | None = None,
               gc_policy: str = "legacy-seven-day") -> None:
    """Bound disposable cache size/age while excluding concurrent publishers."""
    with _cache_mutation(client, bucket) as acquired:
        if acquired:
            _trim_cache_locked(client, bucket, incoming_bytes, total_bytes=total_bytes, gc_policy=gc_policy)


def _trim_cache_locked(
    client: Any, bucket: str, incoming_bytes: int, *, total_bytes: int | None = None,
    gc_policy: str = "legacy-seven-day",
) -> int | None:
    with stage_span("cache_gc"), cache_request_counts(client, "cache_gc"):
        sweep: _CacheSweep | None = None
        try:
            try:
                state = _read_cache_checkpoint(client, bucket)
            except BuildPreparationError:
                state = None  # Explicit trim can repair an unknown checkpoint.
            sweep = _CacheSweep(client, bucket, state, incoming_bytes,
                _CACHE_TOTAL_BYTES if total_bytes is None else total_bytes, gc_policy)
            remaining = sweep.run()
        except (BotoCoreError, ClientError, BuildPreparationError, OSError):
            # Never commit in-memory cursors after an ambiguous segment PUT.
            # Resume only the last durable checkpoint and its committed slot.
            try:
                retained = _read_cache_checkpoint(client, bucket)
                if retained is not None and not retained.get("inventory_complete", True):
                    retained["retry_after"] = time.time() + _CACHE_GC_BACKOFF_SECONDS
                    _write_cache_checkpoint(client, bucket, retained)
            except (BotoCoreError, ClientError, BuildPreparationError, OSError):
                pass  # The last durable v2 freeze remains conservative.
            emit_stage("cache_gc", "deferred", reason="inventory_unavailable")
            emit_stage("cache_export", "deferred", reason="cache_checkpoint_unavailable")
            return None
        return remaining


class _CacheSweepPausedError(Exception):
    pass


class _CacheSweep:
    """Bounded passes over a stable cache-only snapshot, under the existing lock.

    v2 blocks optional publishers (including old writers) until completion. Each
    pass releases the lock; optimistic readers remain independent. LIST metadata
    is segmented remotely; only one segment and a bounded victim set enter RAM.
    No blob is removed until every retained root has been checked for that set.
    """

    def __init__(self, client: Any, bucket: str, previous: dict[str, Any] | None,
                 incoming: int, budget: int, policy: str):
        self.client, self.bucket, self.budget = client, bucket, budget
        self.policy = _cache_gc_policy({"cache_gc_policy": policy})
        self.state = dict(previous or _cache_checkpoint(0))
        scan = self.state.get("scan")
        if scan is None:
            scan = dict(generation=uuid4().hex, phase="list", cursor="", parts=0, total=0,
                aged=False, part=0, row=0, work_slot=0, incoming=incoming, budget=budget, policy=policy)
        if (not isinstance(scan, dict) or not re.fullmatch(r"[0-9a-f]{32}", str(scan.get("generation", "")))
                or scan.get("phase") not in {"list", "select", "load", "refs", "delete", "account", "cleanup"}
                or any(type(scan.get(key)) is not int or scan[key] < 0 for key in
                       ("parts", "total", "part", "row", "work_slot", "incoming", "budget"))
                or scan["work_slot"] not in (0, 1) or not isinstance(scan.get("cursor"), str)):
            raise BuildPreparationError("invalid cache inventory continuation")
        self.scan = dict(scan)
        self.work: dict[str, Any] | None = None
        if self.scan["phase"] in {"select", "load", "refs", "delete"}:
            self.work = self.read("work-" + str(self.scan["work_slot"]))
            if (not isinstance(self.work.get("roots"), list) or len(self.work["roots"]) > _CACHE_GC_ROOT_BATCH
                    or not isinstance(self.work.get("orphans"), list) or len(self.work["orphans"]) > _CACHE_GC_ORPHAN_BATCH
                    or any(not self.valid_row(row) for row in (*self.work["roots"], *self.work["orphans"]))
                    or any(_V2_BLOB.fullmatch(row[0]) for row in self.work["roots"])
                    or any(not _V2_BLOB.fullmatch(row[0]) for row in self.work["orphans"])
                    or not isinstance(self.work.get("candidates"), dict)
                    or len(self.work["candidates"]) > _CACHE_GC_ROOT_BATCH * _CACHE_MANIFEST_FILES + _CACHE_GC_ORPHAN_BATCH
                    or any(not _KEY.fullmatch(digest) or not isinstance(value, dict)
                        or type(value.get("referenced")) is not bool
                        or (value.get("row") is not None and (not self.valid_row(value["row"])
                            or value["row"][0] != _v2_blob_key(digest)))
                        for digest, value in self.work["candidates"].items())):
                raise BuildPreparationError("invalid cache inventory candidates")
            roots = {row[0] for row in self.work['roots']}
            if any(not isinstance(value.get('owners', []), list) or not set(value.get('owners', [])) <= roots
                   for value in self.work['candidates'].values()):
                raise BuildPreparationError('invalid cache inventory owners')
            if self.scan['phase'] == 'delete':
                self.qualify_deletes()
            if (self.scan["budget"], self.scan["policy"]) != (budget, policy):
                # A prior DELETE may have committed without its cursor. Old
                # segments can therefore name absent roots. Re-account first,
                # then rebuild the snapshot if this new policy needs more GC.
                self.account()
                self.scan['restart'] = True
        elif (self.scan["budget"], self.scan["policy"]) != (budget, policy) and self.scan['phase'] != 'list':
            self.scan['restart'] = True
        self.scan.update(incoming=max(incoming, self.scan["incoming"]), budget=budget, policy=policy)
        self.state.update(version=2, inventory_complete=False, scan=self.scan,
                          gc_policy=policy, budget_bytes=budget)
        self.started = time.monotonic()
        self.objects = self.manifests = self.manifest_bytes = 0
        _write_cache_checkpoint(client, bucket, self.state)  # Freeze BEFORE LIST.

    def key(self, name: str) -> str:
        return _CACHE_GC_CONTROL + str(self.scan["generation"]) + "/" + name + ".json"

    def room(self, *, objects: bool = False, manifest: int | None = None) -> None:
        if (time.monotonic() - self.started >= _CACHE_GC_MAX_SECONDS
                or (objects and self.objects >= _CACHE_GC_MAX_OBJECTS)
                or (manifest is not None and (self.manifests >= _CACHE_GC_MAX_MANIFESTS
                    or self.manifest_bytes + manifest > _CACHE_GC_MAX_MANIFEST_BYTES))):
            raise _CacheSweepPausedError
        if objects:
            self.objects += 1
        if manifest is not None:
            self.manifests += 1
            self.manifest_bytes += manifest

    def read(self, name: str) -> Any:
        response = self.client.get_object(Bucket=self.bucket, Key=self.key(name))
        body = response["Body"]
        try:
            raw = body.read(40 * 1024 * 1024 + 1)
        finally:
            body.close()
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError) as error:
            raise BuildPreparationError("invalid cache inventory segment") from error
        if (len(raw) > 40 * 1024 * 1024 or not isinstance(value, dict)
                or value.get("generation") != self.scan["generation"]):
            raise BuildPreparationError("invalid cache inventory segment")
        return value

    def write(self, name: str, value: dict[str, Any]) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self.key(name),
            Body=json.dumps(dict(value, generation=self.scan["generation"]), separators=(",", ":")).encode())

    def save(self, *, backoff: bool = False) -> None:
        if self.work is not None:
            slot = 1 - self.scan["work_slot"]
            self.write("work-" + str(slot), self.work)
            self.scan["work_slot"] = slot
        self.state["scan"] = self.scan
        if backoff:
            self.state["retry_after"] = time.time() + _CACHE_GC_BACKOFF_SECONDS
        else:
            self.state.pop("retry_after", None)
        _write_cache_checkpoint(self.client, self.bucket, self.state)

    @staticmethod
    def order(row: list[Any]) -> tuple[float, str]:
        return row[2], row[0]

    @staticmethod
    def valid_row(row: Any) -> bool:
        return (isinstance(row, list) and len(row) == 3 and isinstance(row[0], str)
            and bool(_LEGACY_TAR.fullmatch(row[0]) or _V2_MANIFEST.fullmatch(row[0]) or _V2_BLOB.fullmatch(row[0]))
            and type(row[1]) is int and row[1] >= 0 and type(row[2]) in (int, float) and math.isfinite(row[2]))

    def rows(self) -> Iterator[list[Any]]:
        while self.scan["part"] < self.scan["parts"]:
            self.room()
            rows = self.read("list-" + str(self.scan["part"])).get("rows")
            if not isinstance(rows, list) or any(not self.valid_row(row) for row in rows):
                raise BuildPreparationError("invalid cache inventory metadata")
            while self.scan["row"] < len(rows):
                self.room(objects=True)
                yield rows[self.scan["row"]]
                self.scan["row"] += 1
            self.scan["part"] += 1
            self.scan["row"] = 0

    def listing(self, *, account: bool = False) -> None:
        records: list[list[Any]] = []
        kwargs = dict(Bucket=self.bucket, Prefix="task-build-cache/")
        if self.scan["cursor"]:
            kwargs["StartAfter"] = self.scan["cursor"]
        try:
            for page in self.client.get_paginator("list_objects_v2").paginate(**kwargs):
                # S3 general-purpose buckets are lexicographic; sorting also
                # preserves the exact mid-page cursor in one-page test doubles.
                for item in sorted(page.get("Contents", []), key=lambda row: row["Key"]):
                    key = item["Key"]
                    if key <= self.scan["cursor"] or key.startswith(_CACHE_GC_CONTROL):
                        continue
                    self.room(objects=True)
                    if _LEGACY_TAR.fullmatch(key) or _V2_MANIFEST.fullmatch(key) or _V2_BLOB.fullmatch(key):
                        row = [key, item["Size"], item["LastModified"].timestamp()]
                        if not self.valid_row(row):
                            raise BuildPreparationError("invalid cache inventory metadata")
                        self.scan["total"] += row[1]
                        if not account:
                            records.append(row)
                            self.scan["aged"] |= row[2] < time.time() - 7 * 86400
                    self.scan["cursor"] = key
        finally:
            if records:
                self.write("list-" + str(self.scan["parts"]), {"rows": records})
                self.scan["parts"] += 1
        if not account:
            self.scan["initial_total"] = self.scan["total"]
        if account or (self.scan["total"] + self.scan["incoming"] <= self.budget and not self.scan["aged"]):
            self.scan.update(phase="cleanup", cleanup=0)
            self.save()  # Cleanup replay must not need already-deleted segments.
        else:
            self.select()

    def select(self) -> None:
        self.scan.update(phase="select", part=0, row=0)
        self.work = {"roots": [], "orphans": [], "candidates": {}, "load": 0}

    def selection(self) -> None:
        assert self.work is not None
        over = self.scan["total"] + self.scan["incoming"] > self.budget
        pressure = self.policy == "budget-pressure" and over
        for row in self.rows():
            blob = bool(_V2_BLOB.fullmatch(row[0]))
            after = self.scan.get("blob_after" if blob else "root_after")
            if after is not None and self.order(row) <= tuple(after):
                continue
            if row[2] >= time.time() - 7 * 86400 and not pressure:
                continue
            if not blob and not over:
                continue
            chosen = self.work["orphans" if blob else "roots"]
            chosen.append(row)
            chosen.sort(key=self.order)
            del chosen[_CACHE_GC_ORPHAN_BATCH if blob else _CACHE_GC_ROOT_BATCH:]
        if not self.work["roots"] and not self.work["orphans"]:
            if self.scan["aged"]:
                self.scan.update(phase="refs", part=0, row=0)
            else:
                self.account()
            return
        for row in self.work["orphans"]:
            self.work["candidates"][row[0].rsplit("/", 1)[-1]] = {"row": row, "referenced": False}
        self.scan.update(phase="load", part=0, row=0)

    def digests(self, row: list[Any]) -> list[str]:
        if not _V2_MANIFEST.fullmatch(row[0]):
            return []
        if row[1] > min(4 * 1024 * 1024, _CACHE_GC_MAX_MANIFEST_BYTES):
            raise BuildPreparationError("cache manifest exceeds collection limit")
        self.room(manifest=row[1])
        try:
            digests, size = _read_cache_manifest(self.client, self.bucket, row[0],
                limit=min(4 * 1024 * 1024, _CACHE_GC_MAX_MANIFEST_BYTES - self.manifest_bytes + row[1]))
            self.manifest_bytes += size - row[1]
        except (BotoCoreError, ClientError, BuildPreparationError, OSError):
            emit_stage("cache_gc", "deferred", reason="manifest_unreadable")
            raise
        return digests

    def load(self) -> None:
        assert self.work is not None
        while self.work["load"] < len(self.work["roots"]):
            for digest in self.digests(self.work["roots"][self.work["load"]]):
                candidate = self.work["candidates"].setdefault(digest, {"row": None, "referenced": False})
                candidate.setdefault("owners", []).append(self.work["roots"][self.work["load"]][0])
            self.work["load"] += 1
        self.scan.update(phase="refs", part=0, row=0)

    def references(self) -> None:
        assert self.work is not None
        victims = {row[0] for row in self.work["roots"]}
        for row in self.rows():
            if _V2_BLOB.fullmatch(row[0]):
                candidate = self.work["candidates"].get(row[0].rsplit("/", 1)[-1])
                if candidate is not None:
                    candidate["row"] = row
            elif row[0] not in victims and (self.scan.get("root_after") is None
                    or self.order(row) > tuple(self.scan["root_after"])):
                for digest in self.digests(row):
                    if digest in self.work["candidates"]:
                        self.work["candidates"][digest]["referenced"] = True
        # The complete retained-root stream has now qualified this bounded set.
        available = {digest: value for digest, value in self.work["candidates"].items()
            if value["row"] is not None and not value["referenced"] and
                (self.policy == "budget-pressure" or value["row"][2] < time.time() - 7 * 86400)}
        deletes = [value["row"] for value in available.values() if not value.get("owners")]
        remaining = self.scan["total"] - sum(row[1] for row in deletes)
        deleted = {row[0] for row in deletes}
        victims_deleted: set[str] = set()
        for root in self.work["roots"]:
            if remaining + self.scan["incoming"] <= self.budget:
                break
            deletes.append(root)
            victims_deleted.add(root[0])
            remaining -= root[1]
            for value in available.values():
                row = value["row"]
                if row[0] not in deleted and set(value.get("owners", ())) <= victims_deleted:
                    deletes.append(row)
                    deleted.add(row[0])
                    remaining -= row[1]
        self.work["deletes"] = deletes
        self.work["delete"] = 0
        self.scan["phase"] = "delete"
        self.save()  # Commit the complete reference check BEFORE any DELETE.

    def deletion(self) -> None:
        assert self.work is not None
        for row in self.work["deletes"][self.work["delete"]:]:
            self.room(objects=True)
            if not self.valid_row(row):
                raise BuildPreparationError("invalid cache deletion candidate")
            try:
                self.client.head_object(Bucket=self.bucket, Key=row[0])
            except ClientError as error:
                if not _object_absent(error):
                    raise
            else:
                self.client.delete_object(Bucket=self.bucket, Key=row[0])
                self.scan["total"] -= row[1]
            self.work["delete"] += 1
        deleted_roots = [row for row in self.work["deletes"] if not _V2_BLOB.fullmatch(row[0])]
        if deleted_roots:
            self.scan["root_after"] = list(max(map(self.order, deleted_roots)))
        if self.work["orphans"]:
            self.scan["blob_after"] = list(max(map(self.order, self.work["orphans"])))
        if self.scan["total"] + self.scan["incoming"] > self.budget and deleted_roots:
            self.select()
        else:
            self.account()

    def qualify_deletes(self) -> None:
        assert self.work is not None
        rows = self.work.get('deletes')
        position = self.work.get('delete')
        if not isinstance(rows, list) or type(position) is not int or not 0 <= position <= len(rows):
            raise BuildPreparationError('invalid cache inventory deletion')
        roots = {row[0]: row for row in self.work['roots']}
        preceding: set[str] = set()
        for row in rows:
            if not self.valid_row(row):
                raise BuildPreparationError('invalid cache inventory deletion')
            if row[0] in roots and row == roots[row[0]]:
                preceding.add(row[0])
                continue
            value = self.work['candidates'].get(row[0].rsplit('/', 1)[-1])
            if (not _V2_BLOB.fullmatch(row[0]) or value is None or value['row'] != row
                    or value['referenced'] or not set(value.get('owners', [])) <= preceding):
                raise BuildPreparationError('unqualified cache inventory deletion')

    def account(self) -> None:
        # A final complete LIST discharges ambiguous DELETE debt. Never infer
        # free space from an uncertain reply or replayed deletion counters.
        self.scan.update(phase="account", cursor="", total=0)
        self.work = None

    def cleanup(self) -> int | None:
        while self.scan["cleanup"] < self.scan["parts"] + 2:
            self.room(objects=True)
            index = self.scan["cleanup"]
            name = "list-" + str(index) if index < self.scan["parts"] else "work-" + str(index - self.scan["parts"])
            self.client.delete_object(Bucket=self.bucket, Key=self.key(name))
            self.scan["cleanup"] += 1
        remaining = int(self.scan["total"])
        if self.scan.get('restart') and remaining + self.scan['incoming'] > self.budget:
            incoming = self.scan['incoming']
            self.scan = dict(generation=uuid4().hex, phase="list", cursor="", parts=0, total=0,
                aged=False, part=0, row=0, work_slot=0, incoming=incoming, budget=self.budget, policy=self.policy)
            self.work = None
            self.save()
            return None
        state = _cache_checkpoint(remaining)
        state.update(gc_policy=self.policy, budget_bytes=self.budget)
        if remaining + self.scan["incoming"] > self.budget:
            state["retry_after"] = time.time() + _CACHE_GC_BACKOFF_SECONDS
            emit_stage("cache_gc", "deferred", reason="grace_protected")
        _write_cache_checkpoint(self.client, self.bucket, state)
        emit_stage("cache_gc", "inventory", remaining_bytes=remaining,
            incoming_bytes=self.scan["incoming"], budget_bytes=self.budget,
            reclaimed_bytes=max(0, self.scan["initial_total"] - remaining))
        return remaining

    def run(self) -> int | None:
        try:
            while True:
                self.room()
                phase = self.scan["phase"]
                if phase in {"list", "account"}:
                    self.listing(account=phase == "account")
                elif phase == "select":
                    self.selection()
                elif phase == "load":
                    self.load()
                elif phase == "refs":
                    self.references()
                elif phase == "delete":
                    self.deletion()
                else:
                    remaining = self.cleanup()
                    if remaining is not None:
                        return remaining
        except _CacheSweepPausedError:
            self.save(backoff=self.objects == 0 and self.manifests == 0)
            emit_stage("cache_gc", "deferred", reason="inventory_progress")
            return None


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
    runtime_metadata: dict[str, dict[str, Any]] = {}
    image_archives: dict[str, dict[str, Any]] = {}
    from loom.task_image_archives import checked_archive_intents, publish_image_archive
    archive_intents = checked_archive_intents(claim)
    archive_client = _client(claim, secrets / "archive") if archive_intents else None
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
            environment = (claim["task_config"]["verifier"]["environment"]
                           if component.name == "verifier" else claim["task_config"]["environment"])
            requires_metadata = component.name in {"task", "verifier"} and (
                environment.get("user") == "image" or environment.get("image_entrypoint") or environment.get("image_workdir")
            )
            architecture: Literal["amd64", "arm64"] = "arm64" if claim["cpu_arch"] == "arm64" else "amd64"
            if export_format == "directory":
                # Plan schema still emits oci/NNNN.tar; directory Jobs rewrite dest.
                relative = component.oci_output_path.removesuffix(".tar")
                output = _output_path(work, relative, directory=True)
                if not requires_metadata:
                    validate_native_oci_directory(output, architecture=architecture)
                skopeo_source = f"oci:{output}"
            else:
                archive = _output_path(work, component.oci_output_path)
                # The OCI validator owns the independent image-size bound,
                # including when runtime metadata is read below.
                if not requires_metadata:
                    validate_native_oci_archive(archive, architecture=architecture)
                skopeo_source = f"oci-archive:{archive}"
            metadata = None
            if requires_metadata:
                from loom_execution_actuator.task_image_metadata import (
                    read_task_image_runtime_metadata,
                )

                metadata = read_task_image_runtime_metadata(
                    output if export_format == "directory" else archive,
                    architecture=architecture,
                    directory=export_format == "directory",
                )
                # Only the agent transfers its writable workspace. A private
                # verifier may legitimately start at / or in its own /tests;
                # its OCI metadata still requires a canonical absolute cwd.
                if component.name == "task" and environment.get("image_workdir"):
                    from loom.mutable_paths import validate_task_workdir

                    try:
                        validate_task_workdir(metadata["workdir"])
                    except ValueError:
                        raise BuildPreparationError("OCI image workdir is unsupported by the selected runtime") from None
                try:
                    preview = TaskImageRuntimeMetadataV1.model_validate({
                        **metadata, "image_ref": claim["registry_repository"] + "@sha256:" + "0" * 64,
                    }).model_dump(mode="json")
                except ValueError as error:
                    raise BuildPreparationError(_metadata_publication_error(error)) from None
                preview_receipt = {
                    "materialization_id": claim["id"], "lease_epoch": claim["lease_epoch"],
                    "registry_images": {**images, component.name: preview["image_ref"]},
                    "image_runtime_metadata": {**runtime_metadata, component.name: preview},
                }
                if len(json.dumps(preview_receipt).encode()) > 4000:
                    raise BuildPreparationError("OCI runtime metadata exceeds publication receipt budget")
            tag = f"{claim['registry_repository']}:{claim['materialization_key']}-{claim['lease_epoch']}-{index}"
            with tempfile.TemporaryDirectory() as temporary:
                digest_file = Path(temporary) / "digest"
                stderr_path = Path(temporary) / "skopeo.stderr"
                with stage_span("publish", component_index=index):
                    argv = [
                        "skopeo", "--tmpdir", temporary, "copy",
                        "--authfile", str(registry_auth),
                        "--preserve-digests", "--digestfile", str(digest_file),
                        skopeo_source, f"docker://{tag}",
                    ]
                    try:
                        with stderr_path.open("w+b") as stderr_handle:
                            subprocess.run(
                                argv, check=True, timeout=_SKOPEO_COPY_TIMEOUT_SECONDS,
                                stdout=subprocess.DEVNULL, stderr=stderr_handle,
                            )
                    except subprocess.CalledProcessError as error:
                        raise _publish_command_error(
                            stderr_path, registry_repository=claim["registry_repository"],
                            authfile=str(registry_auth), argv=argv, exit_code=error.returncode,
                        ) from None
                    except subprocess.TimeoutExpired:
                        raise PublishCommandError(
                            f"skopeo copy timed out after {_SKOPEO_COPY_TIMEOUT_SECONDS} seconds",
                        ) from None
                    digest = digest_file.read_text().strip()
                    if not _DIGEST.fullmatch(digest):
                        raise BuildPreparationError("registry did not return an OCI digest")
                    images[component.name] = f"{claim['registry_repository']}@{digest}"
                    if component.name in archive_intents:
                        assert archive_client is not None
                        archive_path = archive if export_format == "archive" else Path(temporary) / "image.tar"
                        if export_format == "directory":
                            with tarfile.open(archive_path, "w") as packed:
                                for child in sorted(output.iterdir()):
                                    packed.add(child, arcname=child.name)
                        image_archives[component.name] = publish_image_archive(
                            archive_client, claim, component=component.name,
                            image_ref=images[component.name], archive=archive_path,
                        ).model_dump(mode="json")
                    if metadata is not None:
                        runtime_metadata[component.name] = TaskImageRuntimeMetadataV1.model_validate({
                            **metadata, "image_ref": images[component.name],
                        }).model_dump(mode="json")
                    receipt_path.write_text(
                        _publication_message(
                            {
                                "materialization_id": claim["id"],
                                "lease_epoch": claim["lease_epoch"],
                                "registry_images": images,
                                **({"image_archives": image_archives} if image_archives else {}),
                                **({"image_runtime_metadata": runtime_metadata} if runtime_metadata else {}),
                            }
                        )
                    )
                cache_dir = work / "cache-out" / str(index)
                if cache is not None and (cache_dir.exists() or cache_dir.is_symlink()):
                    with stage_span("cache_export", component_index=index), cache_request_counts(
                        cache, "cache_export", component_index=index,
                    ):
                        cache_dir = _output_path(work, f"cache-out/{index}", directory=True)
                        cache_bytes = sum(size for _, _, size in _cache_directory_files(cache_dir))
                        if cache_bytes > _CACHE_BYTES:
                            emit_stage(
                                "cache_export", "deferred", reason="byte_budget_exceeded",
                                component_index=index, logical_bytes=cache_bytes,
                                budget_bytes=_CACHE_BYTES,
                            )
                            continue
                        if transfer == "blobs":
                            _publish_cache_blobs(
                                cache, claim, index=index, cache_dir=cache_dir
                            )
                        else:
                            cache_archive = Path(temporary) / "cache.tar"
                            pack_cache(cache_dir, cache_archive)
                            with _cache_mutation(cache, claim["cache_bucket"]) as acquired:
                                if acquired is True and _reserve_cache_publication(
                                    cache, claim["cache_bucket"], cache_archive.stat().st_size,
                                    total_bytes=_cache_total_bytes(claim),
                                    gc_policy=_cache_gc_policy(claim),
                                ):
                                    cache.upload_file(
                                        str(cache_archive), claim["cache_bucket"],
                                        _cache_prefix(claim) + f"{index}.tar",
                                    )
                                elif acquired is not True:
                                    emit_stage("cache_export", "deferred", reason=(
                                        "cache_mutation_busy" if acquired is False else "conditional_write_unsupported"
                                    ), component_index=index)
    finally:
        if archive_client is not None:
            archive_client.close()
        if cache is not None:
            cache.close()
        auth_directory.cleanup()
    receipt = {
        "materialization_id": claim["id"],
        "lease_epoch": claim["lease_epoch"],
        "registry_images": images,
        **({"image_archives": image_archives} if image_archives else {}),
        **({"image_runtime_metadata": runtime_metadata} if runtime_metadata else {}),
    }
    return receipt


def _publication_message(receipt: dict[str, Any]) -> str:
    try:
        return encode_publication_receipt(receipt)
    except ValueError as error:
        raise BuildPreparationError(str(error)) from None


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
            Path("/dev/termination-log").write_text(_publication_message(receipt))
    except Exception as error:
        # SDK/tool exceptions can carry credentials, endpoints or task-controlled text.
        error_receipt: dict[str, Any] = {}
        previous_message: str | None = None
        failure = termination_failure_update(error)
        try:
            previous_message = Path("/dev/termination-log").read_text()
            parsed = decode_publication_receipt(previous_message)
        except (OSError, ValueError):
            previous_message = None
            parsed = None
        if isinstance(parsed, dict):
            error_receipt = parsed
        error_receipt.update(phase=args.phase, **failure)
        if args.phase == "prepare":
            error_receipt.update(classify_preparation_failure(error).receipt())
        try:
            message = encode_publication_receipt(error_receipt)
        except ValueError:
            # Publication already failed. Preserve every last known image and
            # archive rather than replace valid progress with a small error.
            # Without valid progress, emit only the bounded plain diagnostic.
            message = previous_message if previous_message is not None else json.dumps({
                "phase": args.phase, "error": str(failure.get("error", ""))[:200],
            })
        Path("/dev/termination-log").write_text(message)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
