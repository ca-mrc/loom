"""LocalDiskObjectStore — ObjectStore Protocol implementation that
writes to <root>/<bucket>/<key> on the host filesystem.

Used by the CLI so a stateless `loom run` can land trajectories +
ATIF docs on disk without MinIO. Mirrors the methods the trajectory
writer + finalizer call. Path-traversal-safe for `download_prefix`."""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from loom.trajectory.storage import (
    MultipartUpload,
    ObjectReadback,
    ObjectWriteResult,
    _has_traversal,
)


@dataclass
class LocalDiskObjectStore:
    """Disk-backed ObjectStore. `root` is the host directory under which
    every bucket gets a subdirectory."""

    root: Path
    _multiparts: dict[str, list[tuple[int, bytes]]] = field(default_factory=dict)
    _multipart_meta: dict[str, tuple[str, str]] = field(default_factory=dict)
    _next_upload_id: int = 0

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    async def ensure_bucket(self, bucket: str) -> None:
        (self.root / bucket).mkdir(parents=True, exist_ok=True)

    async def create_multipart_upload(
        self, *, bucket: str, key: str,
    ) -> MultipartUpload:
        self._next_upload_id += 1
        upload_id = f"upload-{self._next_upload_id}"
        self._multiparts[upload_id] = []
        self._multipart_meta[upload_id] = (bucket, key)
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id)

    async def resume_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str
    ) -> MultipartUpload:
        if self._multipart_meta.get(upload_id) != (bucket, key):
            raise KeyError(upload_id)
        return MultipartUpload(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            parts=[
                (part_number, f"etag-{part_number}")
                for part_number, _body in sorted(self._multiparts[upload_id])
            ],
        )

    async def upload_part(
        self, upload: MultipartUpload, *, part_number: int, body: bytes,
    ) -> None:
        self._multiparts[upload.upload_id].append((part_number, body))
        upload.parts.append((part_number, f"etag-{part_number}"))

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        parts = sorted(
            self._multiparts.pop(upload.upload_id), key=lambda p: p[0],
        )
        self._multipart_meta.pop(upload.upload_id, None)
        body = b"".join(p[1] for p in parts)
        dest = self._path(upload.bucket, upload.key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return f"s3://{upload.bucket}/{upload.key}"

    async def complete_multipart_upload_with_metadata(
        self,
        upload: MultipartUpload,
    ) -> ObjectWriteResult:
        uri = await self.complete_multipart_upload(upload)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        self._multiparts.pop(upload.upload_id, None)
        self._multipart_meta.pop(upload.upload_id, None)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        dest = self._path(bucket, key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
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
            raise ValueError("local object store does not support versioning or user metadata")
        uri = await self.put_object(bucket=bucket, key=key, body=body)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        p = self._path(bucket, key)
        if not p.exists():
            raise KeyError(f"s3://{bucket}/{key}")
        return p.read_bytes()

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
        destination = self._path(bucket, key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("xb") as target:
                async for chunk in body:
                    await asyncio.to_thread(target.write, chunk)
                await asyncio.to_thread(target.flush)
                await asyncio.to_thread(os.fsync, target.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return f"s3://{bucket}/{key}"

    async def put_object_stream_with_metadata(
        self, *, bucket: str, key: str, body: AsyncIterator[bytes],
    ) -> ObjectWriteResult:
        uri = await self.put_object_stream(bucket=bucket, key=key, body=body)
        return ObjectWriteResult(uri=uri, version_id=None)

    async def stat_object(self, *, bucket: str, key: str) -> ObjectReadback:
        path = self._path(bucket, key)
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return ObjectReadback(
            content_length=path.stat().st_size,
            checksum_sha256=f"sha256:{digest.hexdigest()}",
        )

    async def stream_object(
        self,
        *,
        bucket: str,
        key: str,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> AsyncIterator[bytes]:
        with self._path(bucket, key).open("rb") as source:
            source.seek(start_offset)
            while chunk := await asyncio.to_thread(source.read, chunk_size):
                yield chunk

    async def delete_object(self, *, bucket: str, key: str) -> None:
        self._path(bucket, key).unlink(missing_ok=True)

    async def presign_put(
        self, *, bucket: str, key: str, expires_sec: int,
    ) -> str:
        return f"file://{self._path(bucket, key).resolve()}?expires_sec={expires_sec}"

    async def list_task_prefixes(
        self, *, bucket: str, benchmark: str,
    ) -> list[str]:
        base = self.root / bucket / benchmark
        if not base.is_dir():
            return []
        seen: set[str] = set()
        for task_toml in base.rglob("task.toml"):
            rel = task_toml.parent.relative_to(self.root / bucket).as_posix()
            seen.add(f"{rel}/")
        return sorted(seen)

    async def download_prefix(
        self, *, bucket: str, prefix: str, out_dir: Path,
    ) -> int:
        if not prefix:
            raise ValueError(
                "download_prefix requires a non-empty prefix; refusing "
                "to drain entire bucket",
            )
        src = self.root / bucket / prefix
        if not src.is_dir():
            return 0
        count = 0
        out_dir.mkdir(parents=True, exist_ok=True)
        for path in src.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(src).as_posix()
            if not rel or _has_traversal(rel):
                continue
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(path.read_bytes())
            count += 1
        return count
