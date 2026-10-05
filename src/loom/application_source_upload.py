"""Personal source upload identities, without build or release authority."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.application_source_archive import MAX_APPLICATION_SOURCE_ARCHIVE_BYTES


class _SourceIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    @model_validator(mode="after")
    def _non_nil(self) -> Self:
        if any(isinstance(value, UUID) and not value.int for value in self.__dict__.values()):
            raise ValueError("nil application source identity")
        return self


class ApplicationSourceUploadRequestV1(_SourceIdentity):
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    archive_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    archive_size_bytes: int = Field(ge=10240, le=MAX_APPLICATION_SOURCE_ARCHIVE_BYTES, multiple_of=10240, strict=True)
    base_commit: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class ApplicationSourceUploadV1(ApplicationSourceUploadRequestV1):
    schema_version: Literal["loom.application-source-upload.v1"] = "loom.application-source-upload.v1"
    upload_id: UUID
    phase: Literal["awaiting_source", "source_verified"]
    expires_at: datetime

    @model_validator(mode="after")
    def _aware(self) -> Self:
        if self.expires_at.utcoffset() is None:
            raise ValueError("application source expiry must be timezone aware")
        return self


class ApplicationSourceUploadBindingV1(_SourceIdentity):
    """Protected installation input, never a field in the owner request."""

    installation_id: UUID
    data_environment_id: UUID
    cluster_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    source_bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    upload_ttl_seconds: int = Field(default=3600, ge=60, le=3600, strict=True)


def application_source_object_key(archive_sha256: str) -> str:
    """A verified archive has one shared key, not an owner-specific copy."""
    import re

    if re.fullmatch(r"[0-9a-f]{64}", archive_sha256) is None:
        raise ValueError("invalid application source archive digest")
    return f"application-sources/v1/sha256/{archive_sha256}.tar"
