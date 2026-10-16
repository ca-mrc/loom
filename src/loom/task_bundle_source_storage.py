"""Storage primitives for journaled task-source publication and recovery.

An intent MUST be committed before a caller writes. Inventory is observational:
it neither grants publication/deletion authority nor proves absence of future
late writes. The durable lifecycle owner must fence references and retain retired
intents for reconciliation. This module does not enable source registration.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal
from uuid import UUID

import rfc8785
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.task_image_build_plan import MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES
from loom.trajectory.immutable_object import (
    ImmutableObjectVersion,
    read_verified_object_version,
    validate_object_version_id,
)
from loom.trajectory.storage import ObjectStore, ObjectWriteResult


def _version(value: object) -> str:
    return validate_object_version_id(value)


class TaskBundleObjectIntentV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.task-bundle-object-intent.v1"] = (
        "loom.task-bundle-object-intent.v1"
    )
    id: UUID
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    object_key: str = Field(min_length=1, max_length=1024)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES, strict=True)

    @field_validator("object_key")
    @classmethod
    def _key(cls, value: str) -> str:
        if (
            len(value.encode("utf-8")) > 1024
            or value.startswith("/")
            or "\\" in value
            or PurePosixPath(value).as_posix() != value
            or any(part in {"", ".", ".."} for part in value.split("/"))
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ValueError("source object key is invalid")
        return value

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.object_key}"

    @property
    def metadata(self) -> dict[str, str]:
        return {
            "loom-source-write-id": str(self.id),
            "loom-source-sha256": self.content_sha256,
            "loom-source-size": str(self.size_bytes),
        }


async def write_task_bundle_source_object(
    store: ObjectStore,
    intent: TaskBundleObjectIntentV1,
    body: bytes,
) -> ObjectWriteResult:
    """Write only verified bytes, with recoverable identity on every retry."""
    if (
        type(body) is not bytes
        or len(body) != intent.size_bytes
        or hashlib.sha256(body).hexdigest() != intent.content_sha256
    ):
        raise ValueError("source object bytes differ from the committed intent")
    result = await store.put_object_with_metadata(
        bucket=intent.bucket,
        key=intent.object_key,
        body=body,
        metadata=intent.metadata,
        require_versioning=True,
    )
    if result.uri != intent.uri:
        raise ValueError("source object receipt names another object")
    _version(result.version_id)
    return result


def _absent(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {
        "NoSuchKey",
        "NoSuchVersion",
        "404",
        "NotFound",
    }


class TaskBundleInventoryCursorV1(BaseModel):
    """Persist with the receipts from this batch, bound to the whole intent."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.task-bundle-inventory-cursor.v1"] = (
        "loom.task-bundle-inventory-cursor.v1"
    )
    intent_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_marker: str = Field(min_length=1, max_length=4096)
    version_marker: str = Field(min_length=1, max_length=1024)

    @field_validator("key_marker")
    @classmethod
    def _key(cls, value: str) -> str:
        # MinIO appends an opaque cache token to NextKeyMarker. This is a
        # continuation value, never an object key, path or read/delete authority.
        if len(value.encode("utf-8")) > 4096 or any(
            ord(char) < 32 or ord(char) == 127 for char in value
        ):
            raise ValueError("source inventory cursor marker is invalid")
        return value

    @field_validator("version_marker")
    @classmethod
    def _version_marker(cls, value: str) -> str:
        return _version(value)


@dataclass(frozen=True, slots=True)
class TaskBundleVersionBatch:
    versions: tuple[ObjectWriteResult, ...]
    continuation: TaskBundleInventoryCursorV1 | None

    @property
    def observed_end(self) -> bool:
        """End of this observation only; retain and periodically rescan intents."""
        return self.continuation is None


class S3TaskBundleVersionInventory:
    """Bounded exact-key reconciliation, including non-current retry versions.

    An empty result is just this observation. In particular, it is NOT authority
    to forget an uncertain write intent or retire its reconciliation tombstone.
    The injected client owns transport timeouts/retries; no ambient endpoint or
    credentials are discovered here. Production callers run this blocking I/O
    outside their database transaction and event loop.
    """

    def __init__(
        self,
        client: Any,
        *,
        max_versions: int = 1000,
        max_read_bytes: int = MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    ) -> None:
        if (
            type(max_versions) is not int
            or not 1 <= max_versions <= 1000
            or type(max_read_bytes) is not int
            or not 1 <= max_read_bytes <= 2**30
        ):
            raise ValueError("source inventory limits are invalid")
        self._client = client
        self._max_versions = max_versions
        self._max_read_bytes = max_read_bytes

    def _read_own_version(
        self, intent: TaskBundleObjectIntentV1, version: str, *, remaining_bytes: int
    ) -> ObjectWriteResult | None:
        params = dict(Bucket=intent.bucket, Key=intent.object_key, VersionId=version)
        try:
            head = self._client.head_object(**params)
        except ClientError as error:
            if _absent(error):
                return None
            raise
        metadata = head.get("Metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("source version metadata is invalid")
        if metadata.get("loom-source-write-id") != str(intent.id):
            return None

        def check(response: Any) -> None:
            if (
                response.get("VersionId") != version
                or response.get("ContentLength") != intent.size_bytes
                or response.get("Metadata") != intent.metadata
            ):
                raise ValueError("source version does not match committed intent")

        check(head)
        if intent.size_bytes > remaining_bytes:
            raise ValueError("source inventory read budget exceeded")
        try:
            read_verified_object_version(
                self._client,
                ImmutableObjectVersion(
                    bucket=intent.bucket, object_key=intent.object_key,
                    version_id=version, content_sha256=intent.content_sha256,
                    size_bytes=intent.size_bytes,
                ),
                max_bytes=remaining_bytes,
                expected_metadata=intent.metadata,
            )
        except ClientError as error:
            if _absent(error):
                return None
            raise
        return ObjectWriteResult(uri=intent.uri, version_id=version)

    def scan_batch(
        self,
        intent: TaskBundleObjectIntentV1,
        *,
        cursor: TaskBundleInventoryCursorV1 | None = None,
    ) -> TaskBundleVersionBatch:
        """Return bounded verified progress, including an empty foreign-only batch.

        Consumers must durably journal the receipts and cursor atomically before
        advancing. Do not delete a page's marker versions until reaching its end:
        the provider may require them to resume listing. Concurrent writes/deletes
        can change inventory during pagination; periodic fresh scans remain
        necessary, including for retired intents and after observed_end.
        """
        fingerprint = hashlib.sha256(rfc8785.dumps(intent.model_dump(mode="json"))).hexdigest()
        if cursor is not None and cursor.intent_sha256 != fingerprint:
            raise ValueError("source inventory cursor differs from intent")
        if intent.size_bytes > self._max_read_bytes:
            raise ValueError("source inventory budget cannot hold one object")
        # Limit at the S3 page boundary, assuming every listed entry requires a
        # complete own-version read. Never discard valid progress at a hard quota.
        maximum = (
            min(self._max_versions, self._max_read_bytes // intent.size_bytes)
            if intent.size_bytes
            else self._max_versions
        )
        if self._client.get_bucket_versioning(Bucket=intent.bucket).get("Status") != "Enabled":
            raise ValueError("source inventory requires Enabled bucket versioning")
        params: dict[str, Any] = dict(
            Bucket=intent.bucket, Prefix=intent.object_key, MaxKeys=maximum
        )
        if cursor is not None:
            params.update(KeyMarker=cursor.key_marker, VersionIdMarker=cursor.version_marker)
        page = self._client.list_object_versions(**params)
        entries = (*page.get("Versions", ()), *page.get("DeleteMarkers", ()))
        if len(entries) > maximum:
            raise ValueError("source inventory exceeded requested page limit")
        seen: set[tuple[str, str]] = set()
        for item in entries:
            key = item.get("Key")
            if type(key) is not str or not key.startswith(intent.object_key):
                raise ValueError("source inventory returned a foreign key")
            version = _version(item.get("VersionId"))
            identity = (key, version)
            if identity in seen or (
                cursor is not None and identity == (cursor.key_marker, cursor.version_marker)
            ):
                raise ValueError("source inventory pagination repeated an object version")
            seen.add(identity)
        continuation = None
        if page.get("IsTruncated") is True:
            key_marker, version_marker = page.get("NextKeyMarker"), page.get("NextVersionIdMarker")
            if not entries or type(key_marker) is not str or type(version_marker) is not str:
                raise ValueError("source inventory pagination is invalid")
            continuation = TaskBundleInventoryCursorV1(
                intent_sha256=fingerprint,
                key_marker=key_marker,
                version_marker=version_marker,
            )
            if continuation == cursor:
                raise ValueError("source inventory pagination did not progress")
        elif page.get("IsTruncated") is not False:
            raise ValueError("source inventory pagination is invalid")
        results: list[ObjectWriteResult] = []
        read_bytes = 0
        for item in page.get("Versions", ()):
            if item["Key"] != intent.object_key:
                continue
            result = self._read_own_version(
                intent, item["VersionId"], remaining_bytes=self._max_read_bytes - read_bytes
            )
            if result is not None:
                results.append(result)
                read_bytes += intent.size_bytes
        return TaskBundleVersionBatch(versions=tuple(results), continuation=continuation)
