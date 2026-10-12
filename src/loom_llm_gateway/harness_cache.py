"""Cached installs of installed agent harnesses (#2310).

An entry is keyed by the lease's team, its plan's exact task image and the
harness install identity, all taken from the lease and its frozen plan. A Pod
never supplies a key, so an install produced on one image or by one team is
never reused by another. Existing entries are reused; racing publishers may
replace the reference, but every reader sees one verified immutable archive
and its matching digest.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID, uuid4

from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.trajectory.storage import ObjectStore

_PREFIX = "harness-cache/v2"


class HarnessCacheError(Exception):
    """A fixed reason code; never request data."""


@dataclass(frozen=True)
class HarnessCacheEntry:
    archive_key: str
    digest_key: str
    max_bytes: int


@dataclass(frozen=True)
class PublishedHarnessCache:
    archive_key: str
    digest: str


def harness_cache_entry(*, team_id: UUID, plan: ExecutionRuntimePlanV1) -> HarnessCacheEntry:
    cache = plan.setup_cache
    if cache is None or plan.execution_role != "attempt":
        raise HarnessCacheError("harness_cache_not_declared")
    document = {
        "schema_version": "loom.harness-cache-key.v1",
        "team_id": str(team_id),
        "task_image_ref": plan.task_image_ref,
        "identity_sha256": cache.identity_sha256,
    }
    key = hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    prefix = f"{_PREFIX}/{team_id}/{key}"
    return HarnessCacheEntry(archive_key=f"{prefix}.tar.gz", digest_key=f"{prefix}.sha256", max_bytes=cache.max_bytes)


async def read_entry(store: ObjectStore, *, bucket: str, entry: HarnessCacheEntry) -> PublishedHarnessCache | None:
    try:
        raw = await store.get_object(bucket=bucket, key=entry.digest_key)
        if len(raw) > 4096:
            return None
        reference = json.loads(raw)
    except Exception:
        return None
    if not isinstance(reference, dict) or set(reference) != {"upload_id", "digest"}:
        return None
    upload_id, digest = reference["upload_id"], reference["digest"]
    if (not isinstance(upload_id, str) or re.fullmatch(r"[0-9a-f]{32}", upload_id) is None
            or not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None):
        return None
    return PublishedHarnessCache(archive_key=f"{entry.archive_key}/{upload_id}", digest=digest)


async def read_entry_digest(store: ObjectStore, *, bucket: str, entry: HarnessCacheEntry) -> str | None:
    published = await read_entry(store, bucket=bucket, entry=entry)
    return published.digest if published is not None else None


async def store_entry(
    store: ObjectStore, *, bucket: str, entry: HarnessCacheEntry, body: AsyncIterator[bytes],
    declared_size: int, declared_digest: str,
) -> bool:
    """Store a verified archive; False when an entry already exists."""
    if not 0 < declared_size <= entry.max_bytes:
        raise HarnessCacheError("harness_cache_size_invalid")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", declared_digest) is None:
        raise HarnessCacheError("harness_cache_digest_mismatch")
    if await read_entry_digest(store, bucket=bucket, entry=entry) is not None:
        return False
    digest = hashlib.sha256()
    received = 0

    async def counted() -> AsyncIterator[bytes]:
        nonlocal received
        async for chunk in body:
            received += len(chunk)
            if received > declared_size:
                raise HarnessCacheError("harness_cache_size_invalid")
            digest.update(chunk)
            yield chunk

    upload_id = uuid4().hex
    await store.put_object_stream(bucket=bucket, key=f"{entry.archive_key}/{upload_id}", body=counted())
    if received != declared_size or "sha256:" + digest.hexdigest() != declared_digest:
        # The archive object is left unreferenced; only a recorded digest publishes it.
        raise HarnessCacheError("harness_cache_digest_mismatch")
    # One indivisible reference binds a verified immutable archive to its hash.
    # Keep the archive on ambiguous publication: a timed-out write may succeed.
    reference = json.dumps({"upload_id": upload_id, "digest": declared_digest}, sort_keys=True).encode()
    await store.put_object(bucket=bucket, key=entry.digest_key, body=reference)
    return True


__all__ = [
    "HarnessCacheEntry",
    "HarnessCacheError",
    "harness_cache_entry",
    "read_entry",
    "read_entry_digest",
    "store_entry",
]
