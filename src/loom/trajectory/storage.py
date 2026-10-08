"""ObjectStore Protocol + FakeObjectStore + boto3-backed MinioObjectStore.

The trajectory writer is the only producer; it uses multipart upload for
flushed-chunk streaming. ATIF docs are uploaded via single-shot put_object.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import stat
import tempfile
import threading
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol, TypeVar, cast
from uuid import uuid4

import boto3
import botocore.handlers
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError, HTTPClientError

_DEFAULT_S3_MAX_POOL_CONNECTIONS = 64
_DEFAULT_S3_CONNECT_TIMEOUT_SECONDS = 5.0
_DEFAULT_S3_READ_TIMEOUT_SECONDS = 30.0
_DEFAULT_S3_OPERATION_TIMEOUT_SECONDS = 30.0
_DEFAULT_S3_OPERATION_ATTEMPTS = 2
_STREAM_UPLOAD_PART_BYTES = 8 * 1024 * 1024
_S3_MAX_UPLOAD_PARTS = 10_000
_RETRYABLE_S3_ERROR_CODES = frozenset(
    {
        "InternalError",
        "RequestTimeout",
        "RequestTimeoutException",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
    }
)
_T = TypeVar("_T")
BUNDLE_FILE_METADATA_NAME = ".loom-bundle-files.v1.json"
_BUNDLE_FILE_METADATA_MAX_BYTES = 4 * 1024 * 1024
_SAFE_BUNDLE_FILE_MODES = frozenset({0o644, 0o755})


def _remove_expect_header(*, params: dict[str, Any], **_kwargs: Any) -> None:
    headers = params.get("headers")
    if headers is None:
        return
    headers.pop("Expect", None)
    headers.pop(b"Expect", None)


def _has_traversal(rel: str) -> bool:
    """True if a relative key contains a `..` segment, an absolute root,
    or a drive-letter prefix. Used by download_prefix to reject keys that
    would escape `out_dir` once joined."""
    parts = Path(rel).parts
    if not parts:
        return False
    if parts[0] in ("/", "\\") or (len(parts[0]) == 2 and parts[0][1] == ":"):
        return True
    return ".." in parts


def _validate_bundle_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or value.startswith(("/", "\\")):
        raise ValueError("bundle file metadata paths must be non-empty relative strings")
    if "\\" in value:
        raise ValueError("bundle file metadata paths must use POSIX separators")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("bundle file metadata paths must not traverse")
    if value == BUNDLE_FILE_METADATA_NAME:
        raise ValueError("bundle file metadata cannot describe its reserved sidecar")
    return value


def bundle_file_metadata_body(task_dir: Path) -> bytes:
    """Return canonical, content-independent file mode metadata for a bundle.

    S3 stores bytes rather than POSIX inode modes.  We persist only the two
    portable modes the runtime needs: ordinary data (0644) and executable
    assets (0755).  set-id, sticky, and write-policy bits are never propagated.
    The reserved sidecar is transport metadata and is intentionally excluded
    from task checksums and materialized file counts.
    """
    files: dict[str, dict[str, str]] = {}
    for path in sorted(task_dir.rglob("*")):
        if path.is_dir():
            continue
        raw_rel = path.relative_to(task_dir).as_posix()
        if raw_rel == BUNDLE_FILE_METADATA_NAME:
            continue
        rel = _validate_bundle_relative_path(raw_rel)
        mode = 0o755 if stat.S_IMODE(path.stat().st_mode) & 0o111 else 0o644
        files[rel] = {"mode": f"{mode:04o}"}
    return json.dumps(
        {"schema_version": 1, "files": files},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def bundle_file_metadata_sha256(task_dir: Path) -> str:
    """Digest the canonical mode manifest for immutable provenance binding."""
    return f"sha256:{sha256(bundle_file_metadata_body(task_dir)).hexdigest()}"


def write_bundle_file_metadata_sidecar(task_dir: Path) -> Path:
    """Write canonical transport metadata after bundle identity is computed."""
    if not task_dir.is_dir():
        raise ValueError("bundle file metadata requires an existing task directory")
    sidecar = task_dir / BUNDLE_FILE_METADATA_NAME
    if sidecar.exists() and (sidecar.is_symlink() or not sidecar.is_file()):
        raise ValueError("bundle file metadata sidecar must be a regular file")
    sidecar.write_bytes(bundle_file_metadata_body(task_dir))
    sidecar.chmod(0o644)
    return sidecar


async def upload_bundle_file_metadata(
    *,
    store: ObjectStore,
    bucket: str,
    prefix: str,
    task_dir: Path,
) -> None:
    """Upload the reserved mode sidecar without changing bundle file counts."""
    if not prefix or not prefix.endswith("/"):
        raise ValueError("bundle file metadata prefix must be non-empty and end with '/'")
    await store.put_object(
        bucket=bucket,
        key=f"{prefix}{BUNDLE_FILE_METADATA_NAME}",
        body=bundle_file_metadata_body(task_dir),
    )


def _parse_bundle_file_metadata(
    body: bytes,
    *,
    expected_paths: set[str],
) -> dict[str, int]:
    if len(body) > _BUNDLE_FILE_METADATA_MAX_BYTES:
        raise ValueError("bundle file metadata exceeds the safe size limit")
    try:
        raw = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("bundle file metadata must be valid UTF-8 JSON") from exc
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "files"}:
        raise ValueError("bundle file metadata must contain the exact v1 fields")
    if raw["schema_version"] != 1 or not isinstance(raw["files"], dict):
        raise ValueError("bundle file metadata schema_version must be 1")
    modes: dict[str, int] = {}
    for raw_path, raw_entry in raw["files"].items():
        path = _validate_bundle_relative_path(raw_path)
        if not isinstance(raw_entry, dict) or set(raw_entry) != {"mode"}:
            raise ValueError("bundle file metadata entries must contain only mode")
        mode_text = raw_entry["mode"]
        if not isinstance(mode_text, str) or mode_text not in {"0644", "0755"}:
            raise ValueError(f"unsafe file mode in bundle metadata for {path!r}")
        mode = int(mode_text, 8)
        if mode not in _SAFE_BUNDLE_FILE_MODES:
            raise ValueError(f"unsafe file mode in bundle metadata for {path!r}")
        modes[path] = mode
    if set(modes) != expected_paths:
        raise ValueError("bundle file metadata paths do not exactly match stored bundle files")
    return modes


def _restore_bundle_file_modes(
    *,
    out_dir: Path,
    modes: dict[str, int],
) -> None:
    root = out_dir.resolve()
    for rel, mode in modes.items():
        dest = out_dir / rel
        if dest.is_symlink() or not dest.is_file() or not dest.resolve().is_relative_to(root):
            raise ValueError(f"bundle file metadata target is not a safe regular file: {rel!r}")
        dest.chmod(mode)


def restore_bundle_file_metadata_sidecar(
    task_dir: Path,
    *,
    expected_sha256: str | None,
    remove: bool,
) -> str:
    """Validate an HF/object transport sidecar and restore safe inode modes.

    The raw canonical sidecar digest is the immutable provenance value.  The
    caller must run this only in an owned directory: shared HF cache symlinks
    must be copied by bytes before calling, so chmod cannot mutate blob cache
    targets used by another task or process.
    """
    sidecar = task_dir / BUNDLE_FILE_METADATA_NAME
    if sidecar.is_symlink() or not sidecar.is_file():
        raise ValueError("bundle file metadata sidecar is missing or not regular")
    body = sidecar.read_bytes()
    observed_sha256 = f"sha256:{sha256(body).hexdigest()}"
    if expected_sha256 is not None and observed_sha256 != expected_sha256:
        raise ValueError(
            "bundle file metadata raw digest mismatch "
            f"expected={expected_sha256} actual={observed_sha256}",
        )
    expected_paths: set[str] = set()
    for path in sorted(task_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(task_dir).as_posix()
        if rel == BUNDLE_FILE_METADATA_NAME:
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"bundle file metadata target is not regular: {rel!r}")
        expected_paths.add(_validate_bundle_relative_path(rel))
    modes = _parse_bundle_file_metadata(body, expected_paths=expected_paths)
    canonical = json.dumps(
        {
            "schema_version": 1,
            "files": {
                rel: {"mode": f"{mode:04o}"}
                for rel, mode in sorted(modes.items())
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if body != canonical:
        raise ValueError("bundle file metadata sidecar is not canonical")
    _restore_bundle_file_modes(out_dir=task_dir, modes=modes)
    if bundle_file_metadata_sha256(task_dir) != observed_sha256:
        raise ValueError("restored bundle file metadata digest mismatch")
    if remove:
        sidecar.unlink()
    return observed_sha256


def _is_retryable_client_error(exc: ClientError) -> bool:
    response = exc.response or {}
    metadata = response.get("ResponseMetadata") or {}
    status = metadata.get("HTTPStatusCode")
    try:
        status_int = int("" if status is None else str(status))
    except (TypeError, ValueError):
        status_int = 0
    if status_int in {500, 502, 503, 504}:
        return True
    error = response.get("Error") or {}
    code = str(error.get("Code") or "")
    return code in _RETRYABLE_S3_ERROR_CODES


@dataclass
class MultipartUpload:
    bucket: str
    key: str
    upload_id: str
    parts: list[tuple[int, str]] = field(default_factory=list)  # (part_number, etag)
    write_identity: str | None = None


@dataclass(frozen=True)
class ObjectReadback:
    """Trusted immutable facts returned after an object write.

    ``checksum_sha256`` is populated only for a backend-provided full-object
    checksum. Multipart ETags and caller metadata are intentionally excluded.
    """

    content_length: int
    checksum_sha256: str | None


@dataclass(frozen=True, slots=True)
class ObjectWriteResult:
    """Exact identity returned by an object-store write."""

    uri: str
    version_id: str | None


def _object_write_version_id(response: object) -> str | None:
    """Return normalized S3 version evidence, preserving an absent value."""
    if not isinstance(response, Mapping) or "VersionId" not in response:
        return None
    version_id = response["VersionId"]
    if (
        not isinstance(version_id, str)
        or not version_id
        or version_id != version_id.strip()
    ):
        raise ValueError("object write returned malformed VersionId evidence")
    return version_id


def _object_user_metadata(metadata: Mapping[str, str] | None) -> tuple[tuple[str, str], ...] | None:
    """Copy bounded canonical S3 user metadata before yielding to storage I/O."""
    if metadata is None:
        return None
    if not isinstance(metadata, Mapping) or len(metadata) > 32:
        raise ValueError("object metadata is invalid")
    items = tuple(metadata.items())
    if any(
        type(key) is not str or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", key) is None
        or type(value) is not str or any(not 32 <= ord(char) <= 126 for char in value)
        for key, value in items
    ) or sum(len(key) + len(value) for key, value in items) > 2048:
        raise ValueError("object metadata is invalid")
    return items


class ObjectStore(Protocol):
    """Trajectory + artifact storage. Multipart for streaming writes; put_object
    for one-shot uploads; presign_put for client-driven uploads (workers ship
    artifacts to MinIO via signed URLs).
    """

    async def create_multipart_upload(
        self,
        *,
        bucket: str,
        key: str,
    ) -> MultipartUpload: ...

    async def resume_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str
    ) -> MultipartUpload: ...

    async def ensure_bucket(self, bucket: str) -> None:
        """Create the bucket when missing; no-op when it already exists."""
        ...

    async def upload_part(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: bytes,
    ) -> None: ...

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        """Returns the final object URI (s3://bucket/key)."""
        ...

    async def complete_multipart_upload_with_metadata(
        self,
        upload: MultipartUpload,
    ) -> ObjectWriteResult: ...

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None: ...

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str: ...

    async def put_object_with_metadata(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        metadata: Mapping[str, str] | None = None,
        require_versioning: bool = False,
    ) -> ObjectWriteResult: ...

    async def get_object(self, *, bucket: str, key: str) -> bytes: ...

    async def upload_part_stream(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: AsyncIterator[bytes],
    ) -> None: ...

    async def put_object_stream(
        self,
        *,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
    ) -> str: ...

    async def put_object_stream_with_metadata(
        self,
        *,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
    ) -> ObjectWriteResult:
        """Stream bounded requests and return the completed write's exact version."""
        ...

    async def stat_object(self, *, bucket: str, key: str) -> ObjectReadback: ...

    def stream_object(
        self,
        *,
        bucket: str,
        key: str,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> AsyncIterator[bytes]: ...

    async def delete_object(self, *, bucket: str, key: str) -> None: ...

    async def presign_put(
        self,
        *,
        bucket: str,
        key: str,
        expires_sec: int,
    ) -> str: ...

    async def download_prefix(
        self,
        *,
        bucket: str,
        prefix: str,
        out_dir: Path,
    ) -> int:
        """List every object under `prefix` in `bucket` and stream each to
        `out_dir` preserving the relative path. Returns the count of
        objects downloaded.

        `prefix` must be non-empty — an empty string would match every
        key in the bucket, which would let a misconfigured bundle source
        (`s3://bucket/` or `s3://bucket`) drain the entire bucket into
        one trial workspace. Implementations MUST raise ValueError on
        empty prefix. Keys containing `..` segments after the prefix is
        stripped are skipped to prevent path traversal out of `out_dir`.

        Used by the worker's `_materialize_task_dir` to pull a benchmark-
        imported task's fixture content (task.toml + instruction.md +
        solution/ + tests/ + environment/) from MinIO before the trial
        runs. Plan 13 Task 2 / agent integrations spec §6.3."""
        ...

    async def list_task_prefixes(
        self,
        *,
        bucket: str,
        benchmark: str,
    ) -> list[str]:
        """List per-task prefixes under `<bucket>/<benchmark>/`. Returns
        sorted strings ending in `/`, e.g. `["humaneval/HumanEval/0/",
        "humaneval/HumanEval/1/", ...]`.

        Used by `loom_benchmark_tool verify` to enumerate imported tasks
        and sample a subset for Oracle baseline runs. Plan 16."""
        ...


@dataclass
class FakeObjectStore:
    """In-memory ObjectStore for unit tests. Maps (bucket, key) → bytes."""

    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    buckets: set[str] = field(default_factory=set)
    _multiparts: dict[str, list[tuple[int, bytes]]] = field(default_factory=dict)
    _next_upload_id: int = 0

    async def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    async def create_multipart_upload(
        self,
        *,
        bucket: str,
        key: str,
    ) -> MultipartUpload:
        self._next_upload_id += 1
        upload_id = f"upload-{self._next_upload_id}"
        self._multiparts[upload_id] = []
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id)

    async def resume_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str
    ) -> MultipartUpload:
        parts = self._multiparts.get(upload_id)
        if parts is None:
            raise KeyError(upload_id)
        return MultipartUpload(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            parts=[(number, f"etag-{number}") for number, _body in sorted(parts)],
        )

    async def upload_part(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: bytes,
    ) -> None:
        self._multiparts[upload.upload_id].append((part_number, body))
        upload.parts.append((part_number, f"etag-{part_number}"))

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        parts = sorted(self._multiparts.pop(upload.upload_id), key=lambda p: p[0])
        self.objects[(upload.bucket, upload.key)] = b"".join(p[1] for p in parts)
        return f"s3://{upload.bucket}/{upload.key}"

    async def complete_multipart_upload_with_metadata(
        self,
        upload: MultipartUpload,
    ) -> ObjectWriteResult:
        uri = await self.complete_multipart_upload(upload)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        self._multiparts.pop(upload.upload_id, None)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        self.objects[(bucket, key)] = body
        return f"s3://{bucket}/{key}"

    async def put_object_with_metadata(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        metadata: Mapping[str, str] | None = None,
        require_versioning: bool = False,
    ) -> ObjectWriteResult:
        if require_versioning or metadata is not None:
            raise ValueError("fake object store does not support versioning or user metadata")
        uri = await self.put_object(bucket=bucket, key=key, body=body)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        if (bucket, key) not in self.objects:
            raise KeyError(f"s3://{bucket}/{key}")
        return self.objects[(bucket, key)]

    async def upload_part_stream(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: AsyncIterator[bytes],
    ) -> None:
        chunks = bytearray()
        async for chunk in body:
            chunks.extend(chunk)
        await self.upload_part(upload, part_number=part_number, body=bytes(chunks))

    async def put_object_stream(
        self,
        *,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
    ) -> str:
        chunks = bytearray()
        async for chunk in body:
            chunks.extend(chunk)
        return await self.put_object(bucket=bucket, key=key, body=bytes(chunks))

    async def stat_object(self, *, bucket: str, key: str) -> ObjectReadback:
        payload = self.objects[(bucket, key)]
        return ObjectReadback(
            content_length=len(payload),
            checksum_sha256=f"sha256:{sha256(payload).hexdigest()}",
        )

    async def put_object_stream_with_metadata(
        self, *, bucket: str, key: str, body: AsyncIterator[bytes],
    ) -> ObjectWriteResult:
        uri = await self.put_object_stream(bucket=bucket, key=key, body=body)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def stream_object(
        self,
        *,
        bucket: str,
        key: str,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> AsyncIterator[bytes]:
        payload = self.objects[(bucket, key)]
        for offset in range(start_offset, len(payload), chunk_size):
            yield payload[offset : offset + chunk_size]

    async def delete_object(self, *, bucket: str, key: str) -> None:
        self.objects.pop((bucket, key), None)

    async def presign_put(
        self,
        *,
        bucket: str,
        key: str,
        expires_sec: int,
    ) -> str:
        return f"https://fake/{bucket}/{key}?expires_sec={expires_sec}"

    async def list_task_prefixes(
        self,
        *,
        bucket: str,
        benchmark: str,
    ) -> list[str]:
        """Find every `<benchmark>/.../task.toml` key and return its
        parent prefix. One prefix per converted task. Matches the
        MinioObjectStore impl: anchored on `task.toml` so multi-segment
        instance_ids (HumanEval is `HumanEval/0`) round-trip correctly."""
        base = f"{benchmark}/"
        seen: set[str] = set()
        for b, k in self.objects:
            if b != bucket or not k.startswith(base):
                continue
            if not k.endswith("/task.toml"):
                continue
            seen.add(k[: -len("task.toml")])
        return sorted(seen)

    async def download_prefix(
        self,
        *,
        bucket: str,
        prefix: str,
        out_dir: Path,
    ) -> int:
        """Stream every (bucket, key) where key startswith `prefix` into
        `out_dir`, preserving the suffix path. Used by the worker's
        materialize_task_dir test path."""
        if not prefix:
            raise ValueError(
                "download_prefix requires a non-empty prefix; refusing to drain entire bucket",
            )
        count = 0
        out_dir.mkdir(parents=True, exist_ok=True)
        selected: list[tuple[str, str, bytes]] = []
        metadata_body: bytes | None = None
        for (b, k), body in self.objects.items():
            if b != bucket or not k.startswith(prefix):
                continue
            rel = k[len(prefix) :].lstrip("/")
            if not rel or _has_traversal(rel):
                continue
            if rel == BUNDLE_FILE_METADATA_NAME:
                metadata_body = body
                continue
            selected.append((k, rel, body))
        modes = (
            _parse_bundle_file_metadata(
                metadata_body,
                expected_paths={rel for _key, rel, _body in selected},
            )
            if metadata_body is not None
            else None
        )
        for _key, rel, body in selected:
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(body)
            count += 1
        if modes is not None:
            _restore_bundle_file_modes(out_dir=out_dir, modes=modes)
        return count


class MinioObjectStore:
    """boto3-backed ObjectStore against a MinIO endpoint.

    Operations are blocking; we use asyncio.to_thread to keep them off the
    event loop. Configure via constructor; do NOT read env vars here (config
    loading lives in `loom_control_plane.config` and `loom_worker.config`).
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        max_pool_connections: int = _DEFAULT_S3_MAX_POOL_CONNECTIONS,
        connect_timeout: float = _DEFAULT_S3_CONNECT_TIMEOUT_SECONDS,
        read_timeout: float = _DEFAULT_S3_READ_TIMEOUT_SECONDS,
        operation_timeout: float = _DEFAULT_S3_OPERATION_TIMEOUT_SECONDS,
        operation_attempts: int = _DEFAULT_S3_OPERATION_ATTEMPTS,
    ) -> None:
        self._client_kwargs = {
            "service_name": "s3",
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
            "config": Config(
                signature_version="s3v4",
                retries={"max_attempts": 3},
                max_pool_connections=max_pool_connections,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                tcp_keepalive=True,
            ),
        }
        self._operation_timeout = operation_timeout
        self._operation_attempts = max(1, operation_attempts)
        self._client_lock = threading.Lock()
        self._closed = False
        self._client = self._build_client()

    def close(self) -> None:
        """Close the owned SDK client after callers have drained operations.

        Idempotent; a timed-out operation cannot reopen this store on a late
        retry. Threads already inside the SDK retain its network timeouts.
        """
        with self._client_lock:
            if self._closed:
                return
            self._closed = True
            self._client.close()

    def _build_client(self) -> Any:
        client = boto3.client(**self._client_kwargs)
        self._configure_client_events(client)
        return client

    @staticmethod
    def _configure_client_events(client: Any) -> None:
        client.meta.events.unregister(
            "before-call.s3",
            botocore.handlers.add_expect_header,
        )
        client.meta.events.register_last("before-call.s3", _remove_expect_header)

    def _replace_client(self, stale_client: Any) -> None:
        with self._client_lock:
            if self._closed:
                return
            with contextlib.suppress(Exception):
                stale_client.close()
            if self._client is stale_client:
                self._client = self._build_client()

    async def _run_client_call(
        self,
        operation: str,
        call: Callable[[Any], _T],
    ) -> _T:
        last_retryable: BaseException | None = None
        for attempt in range(self._operation_attempts):
            with self._client_lock:
                if self._closed:
                    raise RuntimeError("object store is closed")
                client = self._client
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(call, client),
                    timeout=self._operation_timeout,
                )
            except TimeoutError as exc:
                last_retryable = exc
                self._replace_client(client)
                if attempt + 1 == self._operation_attempts:
                    break
            except (HTTPClientError, ConnectionError) as exc:
                last_retryable = exc
                self._replace_client(client)
                if attempt + 1 == self._operation_attempts:
                    break
            except ClientError as exc:
                if not _is_retryable_client_error(exc):
                    raise
                last_retryable = exc
                self._replace_client(client)
                if attempt + 1 == self._operation_attempts:
                    break
        detail = (
            f": {type(last_retryable).__name__}: {last_retryable}"
            if last_retryable is not None
            else ""
        )
        raise TimeoutError(
            f"S3 {operation} failed after {self._operation_attempts} "
            f"attempt(s) with per-attempt timeout "
            f"{self._operation_timeout:g}s{detail}",
        ) from last_retryable

    @property
    def _client_config(self) -> Config:
        return cast(Config, self._client_kwargs["config"])

    async def ensure_bucket(self, bucket: str) -> None:
        """Idempotently create a MinIO/S3 bucket.

        Dev and test stacks can retain MinIO data across restarts; a
        partially-initialized object store should not leave worker
        trials stuck before trajectory upload starts.
        """

        def _do(client: Any) -> None:
            try:
                client.head_bucket(Bucket=bucket)
                return
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchBucket", "NotFound"}:
                    raise
            try:
                client.create_bucket(Bucket=bucket)
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {
                    "BucketAlreadyExists",
                    "BucketAlreadyOwnedByYou",
                }:
                    raise

        await self._run_client_call("ensure_bucket", _do)

    async def create_multipart_upload(
        self,
        *,
        bucket: str,
        key: str,
    ) -> MultipartUpload:
        def _do(client: Any) -> str:
            return cast(
                str,
                client.create_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                )["UploadId"],
            )

        upload_id = await self._run_client_call("create_multipart_upload", _do)
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id)

    async def resume_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str
    ) -> MultipartUpload:
        def _do(client: Any) -> list[tuple[int, str]]:
            marker: int | None = None
            parts: list[tuple[int, str]] = []
            while True:
                kwargs: dict[str, Any] = {
                    "Bucket": bucket,
                    "Key": key,
                    "UploadId": upload_id,
                }
                if marker is not None:
                    kwargs["PartNumberMarker"] = marker
                response = client.list_parts(**kwargs)
                parts.extend(
                    (int(item["PartNumber"]), str(item["ETag"]))
                    for item in response.get("Parts", [])
                )
                if not response.get("IsTruncated"):
                    return parts
                marker = int(response["NextPartNumberMarker"])

        parts = await self._run_client_call("resume_multipart_upload", _do)
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id, parts=parts)

    async def upload_part(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: bytes,
    ) -> None:
        def _do(client: Any) -> str:
            resp = client.upload_part(
                Bucket=upload.bucket,
                Key=upload.key,
                PartNumber=part_number,
                UploadId=upload.upload_id,
                Body=body,
            )
            return cast(str, resp["ETag"])

        etag = await self._run_client_call("upload_part", _do)
        upload.parts.append((part_number, etag))

    async def complete_multipart_upload_with_metadata(
        self,
        upload: MultipartUpload,
    ) -> ObjectWriteResult:
        def _do(client: Any) -> object:
            return client.complete_multipart_upload(
                Bucket=upload.bucket,
                Key=upload.key,
                UploadId=upload.upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": pn, "ETag": etag}
                        for pn, etag in sorted(upload.parts, key=lambda p: p[0])
                    ],
                },
            )

        response = await self._run_client_call("complete_multipart_upload", _do)
        version_id = _object_write_version_id(response)
        write_identity = getattr(upload, "write_identity", None)
        if version_id is None and write_identity is not None:
            # Some S3-compatible backends omit the completion version. A
            # current HEAD is safe only when its creation metadata binds it
            # to this exact upload; identical content/ETags alone are not a
            # write identity. Existing/resumed uploads without this binding
            # retain their original receipt semantics.
            def read_owned_version(client: Any) -> str | None:
                head = client.head_object(Bucket=upload.bucket, Key=upload.key)
                metadata = head.get("Metadata")
                # S3 user metadata names are case-insensitive HTTP headers;
                # compatible providers can preserve that casing in this map.
                # Require one unambiguous field and its exact original value.
                identities = [
                    value for name, value in metadata.items()
                    if isinstance(name, str) and name.lower() == "loom-write-id"
                ] if isinstance(metadata, Mapping) else []
                if identities != [write_identity]:
                    raise ValueError("multipart object readback identity mismatch")
                return _object_write_version_id(head)

            version_id = await self._run_client_call("multipart_version_readback", read_owned_version)
        return ObjectWriteResult(
            uri=f"s3://{upload.bucket}/{upload.key}",
            version_id=version_id,
        )

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        return (await self.complete_multipart_upload_with_metadata(upload)).uri

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        def _do(client: Any) -> None:
            client.abort_multipart_upload(
                Bucket=upload.bucket,
                Key=upload.key,
                UploadId=upload.upload_id,
            )

        await self._run_client_call("abort_multipart_upload", _do)

    async def put_object_with_metadata(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        metadata: Mapping[str, str] | None = None,
        require_versioning: bool = False,
    ) -> ObjectWriteResult:
        """Optionally bind a durable intent to every immutable version written.

        Requiring versioning checks Enabled before each application retry, then
        rejects missing/null receipts. It does not cancel already accepted writes
        on timeout or revoke external versioning authority. SDK-internal retries
        carry the same metadata but do not repeat that preflight. Callers must
        journal intents before I/O and reconcile late/unknown versions after errors.
        """
        frozen_metadata = _object_user_metadata(metadata)

        def _do(client: Any) -> object:
            if require_versioning and client.get_bucket_versioning(Bucket=bucket).get("Status") != "Enabled":
                raise ValueError("object write requires Enabled bucket versioning")
            kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": body}
            if frozen_metadata is not None:
                kwargs["Metadata"] = dict(frozen_metadata)
            response = client.put_object(**kwargs)
            if require_versioning and _object_write_version_id(response) in {None, "null"}:
                raise ValueError("object write did not return an immutable object version")
            return response

        response = await self._run_client_call("put_object", _do)
        return ObjectWriteResult(
            uri=f"s3://{bucket}/{key}",
            version_id=_object_write_version_id(response),
        )

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        return (
            await self.put_object_with_metadata(bucket=bucket, key=key, body=body)
        ).uri

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        def _do(client: Any) -> bytes:
            resp = client.get_object(Bucket=bucket, Key=key)
            return cast(bytes, resp["Body"].read())

        return await self._run_client_call("get_object", _do)

    @staticmethod
    async def _copy_async_body_to_file(body: AsyncIterator[bytes], target: Any) -> None:
        async for chunk in body:
            if not isinstance(chunk, bytes | bytearray | memoryview):
                raise TypeError("object stream chunks must be bytes-like")
            await asyncio.to_thread(target.write, chunk)
        await asyncio.to_thread(target.flush)
        await asyncio.to_thread(target.seek, 0)

    async def upload_part_stream(
        self,
        upload: MultipartUpload,
        *,
        part_number: int,
        body: AsyncIterator[bytes],
    ) -> None:
        # Bridge the async request body into boto3 with an anonymous disk file,
        # keeping the complete part out of Python memory and deleting it on exit.
        with tempfile.TemporaryFile() as source:
            await self._copy_async_body_to_file(body, source)

            def _do(client: Any) -> str:
                response = client.upload_part(
                    Bucket=upload.bucket,
                    Key=upload.key,
                    PartNumber=part_number,
                    UploadId=upload.upload_id,
                    Body=source,
                    ChecksumAlgorithm="SHA256",
                )
                return cast(str, response["ETag"])

            etag = await self._run_client_call("upload_part_stream", _do)
            upload.parts.append((part_number, etag))

    async def _create_stream_upload(self, *, bucket: str, key: str) -> MultipartUpload:
        # Initiation is not safely replayable: a timed-out SDK call can still
        # create an upload. Keep ownership until that thread returns, and let
        # it abort a late result instead of abandoning an unknown upload ID.
        lock = threading.Lock()
        abandoned = False
        allocated: MultipartUpload | None = None
        write_identity = uuid4().hex
        with self._client_lock:
            if self._closed:
                raise RuntimeError("object store is closed")
            client_kwargs = dict(self._client_kwargs)
        client_kwargs["config"] = self._client_config.merge(Config(retries={"max_attempts": 0}))

        def create() -> MultipartUpload:
            nonlocal allocated
            client = boto3.client(**client_kwargs)
            self._configure_client_events(client)
            try:
                upload = MultipartUpload(
                    bucket=bucket, key=key,
                    upload_id=client.create_multipart_upload(
                        Bucket=bucket, Key=key, Metadata={"loom-write-id": write_identity},
                    )["UploadId"],
                    write_identity=write_identity,
                )
                with lock:
                    if not abandoned:
                        allocated = upload
                        return upload
                with contextlib.suppress(Exception):
                    client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload.upload_id)
                return upload
            finally:
                client.close()

        try:
            return await asyncio.wait_for(asyncio.to_thread(create), timeout=self._operation_timeout)
        except BaseException:
            with lock:
                abandoned = True
                owned = allocated
            if owned is not None:
                with contextlib.suppress(Exception):
                    await self.abort_multipart_upload(owned)
            raise

    async def put_object_stream(
        self,
        *,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
    ) -> str:
        result = await self.put_object_stream_with_metadata(bucket=bucket, key=key, body=body)
        return result.uri

    async def put_object_stream_with_metadata(
        self,
        *,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
    ) -> ObjectWriteResult:
        # Bound each request independently of total artifact size. Immutable
        # bytes also give SDK retries (and threads surviving a timeout) their
        # own cursor instead of racing over a shared temporary file.
        pending = bytearray()
        upload: MultipartUpload | None = None

        async def send_part() -> None:
            nonlocal upload
            if upload is None:
                upload = await self._create_stream_upload(bucket=bucket, key=key)
            part_number = len(upload.parts) + 1
            if part_number > _S3_MAX_UPLOAD_PARTS:
                raise ValueError("object stream exceeds the multipart part limit")
            payload = bytes(pending)
            pending.clear()
            await self.upload_part(upload, part_number=part_number, body=payload)

        try:
            async for chunk in body:
                if not isinstance(chunk, bytes | bytearray | memoryview):
                    raise TypeError("object stream chunks must be bytes-like")
                view = memoryview(chunk).cast("B")
                for offset in range(0, len(view), _STREAM_UPLOAD_PART_BYTES):
                    piece = view[offset:offset + _STREAM_UPLOAD_PART_BYTES]
                    available = _STREAM_UPLOAD_PART_BYTES - len(pending)
                    pending.extend(piece[:available])
                    if len(pending) == _STREAM_UPLOAD_PART_BYTES:
                        await send_part()
                    pending.extend(piece[available:])
            if upload is None:
                payload = bytes(pending)

                def put(client: Any) -> Any:
                    return client.put_object(Bucket=bucket, Key=key, Body=payload, ChecksumAlgorithm="SHA256")

                response = await self._run_client_call("put_object_stream", put)
                return ObjectWriteResult(
                    uri=f"s3://{bucket}/{key}", version_id=_object_write_version_id(response),
                )
            if pending:
                await send_part()
            return await self.complete_multipart_upload_with_metadata(upload)
        except BaseException:
            if upload is not None:
                # Preserve the original failure/cancellation if best-effort
                # cleanup also fails. An aborted upload cannot publish a part.
                with contextlib.suppress(Exception):
                    await self.abort_multipart_upload(upload)
            raise

    async def stat_object(self, *, bucket: str, key: str) -> ObjectReadback:
        def _do(client: Any) -> ObjectReadback:
            response = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
            encoded = response.get("ChecksumSHA256")
            checksum: str | None = None
            if isinstance(encoded, str):
                import base64

                try:
                    raw = base64.b64decode(encoded, validate=True)
                except ValueError:
                    raw = b""
                if len(raw) == 32:
                    checksum = f"sha256:{raw.hex()}"
            return ObjectReadback(
                content_length=int(response["ContentLength"]),
                checksum_sha256=checksum,
            )

        return await self._run_client_call("stat_object", _do)

    async def stream_object(
        self,
        *,
        bucket: str,
        key: str,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> AsyncIterator[bytes]:
        def _open(client: Any) -> Any:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
            if start_offset:
                kwargs["Range"] = f"bytes={start_offset}-"
            return client.get_object(**kwargs)["Body"]

        stream = await self._run_client_call("stream_object_open", _open)
        try:
            while True:
                chunk = await asyncio.to_thread(stream.read, chunk_size)
                if not chunk:
                    break
                yield cast(bytes, chunk)
        finally:
            await asyncio.to_thread(stream.close)

    async def delete_object(self, *, bucket: str, key: str) -> None:
        def _do(client: Any) -> None:
            client.delete_object(Bucket=bucket, Key=key)

        await self._run_client_call("delete_object", _do)

    async def presign_put(
        self,
        *,
        bucket: str,
        key: str,
        expires_sec: int,
    ) -> str:
        def _do(client: Any) -> str:
            return cast(
                str,
                client.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=expires_sec,
                ),
            )

        return await self._run_client_call("presign_put", _do)

    async def list_task_prefixes(
        self,
        *,
        bucket: str,
        benchmark: str,
    ) -> list[str]:
        """Walk `<benchmark>/` keys and infer per-task prefixes — each
        prefix maps to one converted task bundle. We can't use S3's
        Delimiter listing because some benchmarks have multi-segment
        instance_ids (HumanEval is `HumanEval/0`); we walk all keys
        and group by `task.toml`'s parent prefix."""

        def _do(client: Any) -> list[str]:
            paginator = client.get_paginator("list_objects_v2")
            base = f"{benchmark}/"
            seen: set[str] = set()
            for page in paginator.paginate(Bucket=bucket, Prefix=base):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith("/task.toml"):
                        continue
                    seen.add(key[: -len("task.toml")])
            return sorted(seen)

        return await self._run_client_call("list_task_prefixes", _do)

    async def download_prefix(
        self,
        *,
        bucket: str,
        prefix: str,
        out_dir: Path,
    ) -> int:
        """List + download every object under `prefix`.

        Listing and individual object downloads are separate retry units. A
        transient disconnect while downloading one object must not restart the
        entire prefix, because high-concurrency task materialization can make
        that duplicate already-downloaded files and spend the outer worker
        setup timeout before S3 retries have useful signal.
        """
        if not prefix:
            raise ValueError(
                "download_prefix requires a non-empty prefix; refusing to drain entire bucket",
            )

        def _list(client: Any) -> list[tuple[str, str]]:
            paginator = client.get_paginator("list_objects_v2")
            objects: list[tuple[str, str]] = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    rel = key[len(prefix) :].lstrip("/")
                    if not rel or _has_traversal(rel):
                        continue
                    objects.append((key, rel))
            return objects

        objects = await self._run_client_call("download_prefix.list", _list)
        metadata_keys = [key for key, rel in objects if rel == BUNDLE_FILE_METADATA_NAME]
        data_objects = [(key, rel) for key, rel in objects if rel != BUNDLE_FILE_METADATA_NAME]
        modes: dict[str, int] | None = None
        if metadata_keys:
            metadata_body = await self.get_object(bucket=bucket, key=metadata_keys[0])
            modes = _parse_bundle_file_metadata(
                metadata_body,
                expected_paths={rel for _key, rel in data_objects},
            )
        out_dir.mkdir(parents=True, exist_ok=True)

        for key, rel in data_objects:
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            partial = dest.with_name(f"{dest.name}.part")

            def _download(
                client: Any,
                *,
                key: str = key,
                partial: Path = partial,
                dest: Path = dest,
            ) -> None:
                with contextlib.suppress(FileNotFoundError):
                    partial.unlink()
                client.download_file(bucket, key, str(partial))
                partial.replace(dest)

            await self._run_client_call(f"download_prefix.download:{key}", _download)

        if modes is not None:
            _restore_bundle_file_modes(out_dir=out_dir, modes=modes)
        return len(data_objects)
