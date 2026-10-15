"""Bounded integrity reads of an exact S3 version, not access/retention authority.

The caller supplies an authenticated, timeout-bounded client and independently
authorizes the stored descriptor. This module never discovers an endpoint or
credentials, retries a request, writes an object, or falls back to its latest key.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any, BinaryIO

from pydantic import BaseModel, ConfigDict, Field, field_validator

_READ_CHUNK_BYTES = 1024 * 1024


def validate_object_version_id(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 1024
        or value == "null"
        or value != value.strip()
        or any(not 33 <= ord(char) <= 126 for char in value)
    ):
        raise ValueError("object requires an exact immutable version")
    return value


class ImmutableObjectVersion(BaseModel):
    """Expected bytes and location; possession grants no permission to read."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    object_key: str = Field(min_length=1, max_length=1024)
    version_id: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=2**63 - 1)

    @field_validator("version_id")
    @classmethod
    def _version(cls, value: str) -> str:
        return validate_object_version_id(value)

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
            raise ValueError("object version key is invalid")
        return value


def read_verified_object_version(
    client: Any,
    version: ImmutableObjectVersion,
    *,
    max_bytes: int,
    destination: BinaryIO | None = None,
    expected_metadata: Mapping[str, str] | None = None,
) -> None:
    """Verify a whole immutable version, optionally copying to a private sink.

    Destination bytes are UNTRUSTED until successful return. The caller owns and
    must discard that sink on failure, never publish partial content. A read-only
    integrity check can omit the sink. Storage I/O is synchronous: callers run it
    outside their database transactions and event loop. The caller also owns
    client timeouts/retry configuration; this function issues exactly one GET.
    """
    version = ImmutableObjectVersion.model_validate(version.model_dump())
    if type(max_bytes) is not int or not 0 <= version.size_bytes <= max_bytes:
        raise ValueError("object version read budget is invalid or exceeded")
    metadata = dict(expected_metadata) if expected_metadata is not None else None
    response = client.get_object(
        Bucket=version.bucket, Key=version.object_key, VersionId=version.version_id,
    )
    body = response["Body"]
    try:
        if (
            response.get("VersionId") != version.version_id
            or type(response.get("ContentLength")) is not int
            or response["ContentLength"] != version.size_bytes
            or (metadata is not None and response.get("Metadata") != metadata)
        ):
            raise ValueError("object version response differs from descriptor")
        digest, count = hashlib.sha256(), 0
        while True:
            chunk = body.read(min(_READ_CHUNK_BYTES, version.size_bytes - count + 1))
            if type(chunk) is not bytes:
                raise ValueError("object version stream returned invalid bytes")
            if not chunk:
                break
            count += len(chunk)
            if count > version.size_bytes:
                raise ValueError("object version byte size exceeded descriptor")
            digest.update(chunk)
            if destination is not None:
                written = destination.write(chunk)
                if type(written) is not int or written != len(chunk):
                    raise OSError("object version destination write was incomplete")
        if count != version.size_bytes or digest.hexdigest() != version.content_sha256:
            raise ValueError("object version content differs from descriptor")
    finally:
        body.close()
