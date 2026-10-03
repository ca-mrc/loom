"""Authenticate, spool and verify exact source bytes before durable acceptance.

The caller owns HTTP framing. This verifier grants neither build capacity nor a
release. Storage keys are server-derived, and an uncertain PUT is only observed.
"""
from __future__ import annotations

import asyncio
import hashlib
import math
import os
import stat
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO
from uuid import UUID

from loom.application_source_archive import extract_application_source_archive
from loom.application_source_upload import ApplicationSourceUploadV1, application_source_object_key
from loom.auth import AuthContext
from loom.trajectory.storage import ObjectStore
from loom_service.application_management.source_registry import ApplicationSourceRegistry
from loom_service.environment_management.registry import ManagementError

_CHUNK = 1024 * 1024


def _verify_archive(stream: BinaryIO, receipt: ApplicationSourceUploadV1, directory: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="verify-source-", dir=directory) as target:
        extract_application_source_archive(stream, expected_digest=receipt.source_digest, destination=Path(target))


async def _verify_off_loop(stream: BinaryIO, receipt: ApplicationSourceUploadV1, directory: Path) -> None:
    # Keep the spool/admission alive until verification ends, even on repeated
    # cancellation. The worker cannot race a closed descriptor or its cleanup.
    task = asyncio.create_task(asyncio.to_thread(_verify_archive, stream, receipt, directory))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()  # Retrieve any validation failure without masking cancellation.
        raise


class ApplicationSourceUploader:
    def __init__(self, registry: ApplicationSourceRegistry, store: ObjectStore, *, spool_directory: Path,
                 max_inflight: int = 2, receive_timeout_seconds: float = 300,
                 storage_timeout_seconds: float = 300):
        directory = spool_directory.absolute()
        metadata = directory.stat(follow_symlinks=False)
        if (directory != directory.resolve(strict=True) or not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077
                or type(max_inflight) is not int or not 1 <= max_inflight <= 16
                or any(type(value) not in {int, float} or not math.isfinite(value) or not 0 < value <= 3600
                       for value in (receive_timeout_seconds, storage_timeout_seconds))):
            raise ValueError("invalid application source upload limits")
        self.registry, self.store, self.directory = registry, store, directory
        self.max_inflight, self.receive_timeout = max_inflight, receive_timeout_seconds
        self.storage_timeout = storage_timeout_seconds
        self._active = 0
        # Reception/storage stream through disk. Parsed manifests need a separate
        # memory bound in the 1Gi manager even when up to 16 uploads are admitted.
        self._verification_slots = asyncio.Semaphore(2)

    async def _stored(self, receipt: ApplicationSourceUploadV1, key: str) -> None:
        checksum, size = hashlib.sha256(), 0
        chunks = self.store.stream_object(bucket=self.registry.binding.source_bucket, key=key, chunk_size=_CHUNK)
        try:
            async for chunk in chunks:
                size += len(chunk)
                if size > receipt.archive_size_bytes:
                    raise ValueError
                checksum.update(chunk)
            if size != receipt.archive_size_bytes or checksum.hexdigest() != receipt.archive_sha256:
                raise ValueError
        finally:
            close = getattr(chunks, "aclose", None)
            if close is not None:
                await close()

    async def upload(self, upload_id: UUID, *, principal: AuthContext,
                     body: AsyncIterator[bytes]) -> ApplicationSourceUploadV1:
        # Per-process admission has no await between test/increment and no queue.
        if self._active >= self.max_inflight:
            raise ManagementError("application_source_capacity_exhausted", 503)
        self._active += 1
        try:
            receipt = await self.registry.for_upload(upload_id, principal=principal)
            if receipt.phase == "source_verified":
                return receipt
            with tempfile.TemporaryFile(mode="w+b", dir=self.directory) as spool:
                checksum, size = hashlib.sha256(), 0
                try:
                    async with asyncio.timeout(self.receive_timeout):
                        async for chunk in body:
                            size += len(chunk)
                            if size > receipt.archive_size_bytes:
                                raise ManagementError("application_source_invalid", 422)
                            checksum.update(chunk)
                            if spool.write(chunk) != len(chunk):
                                raise OSError
                except TimeoutError:
                    raise ManagementError("application_source_reception_timeout", 408) from None
                if size != receipt.archive_size_bytes or checksum.hexdigest() != receipt.archive_sha256:
                    raise ManagementError("application_source_invalid", 422)
                spool.flush()
                try:
                    async with self._verification_slots:
                        await _verify_off_loop(spool, receipt, self.directory)
                except ValueError:
                    raise ManagementError("application_source_invalid", 422) from None
                # Recheck DB-clock expiry after potentially slow reception.
                await self.registry.for_upload(upload_id, principal=principal)
                spool.seek(0)
                key = application_source_object_key(receipt.archive_sha256)

                async def content() -> AsyncIterator[bytes]:
                    while chunk := spool.read(_CHUNK):
                        yield chunk

                try:
                    async with asyncio.timeout(self.storage_timeout):
                        try:
                            await self.store.put_object_stream(bucket=self.registry.binding.source_bucket,
                                                              key=key, body=content())
                        except Exception:
                            pass  # An uncertain write is never resent in this invocation.
                        await self._stored(receipt, key)
                except Exception:
                    raise ManagementError("application_source_storage_unverified", 503) from None
                return await self.registry.complete(upload_id, principal=principal)
        except ManagementError:
            raise
        except OSError:
            raise ManagementError("application_source_upload_unavailable", 503) from None
        finally:
            self._active -= 1
