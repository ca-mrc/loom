"""Personal source/recipe identities for the protected native application builder."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.application_source_upload import (
    ApplicationSourceUploadBindingV1,
    ApplicationSourceUploadRequestV1,
    application_source_object_key,
)
from loom.native_image_build import NativeImageBuildComponentV1
from loom.nebius_application_contract import ApplicationReleaseV1
from loom.pipeline.keys import canonical_digest

_IMAGE = r"^[a-z0-9][a-z0-9.:-]*/[a-z0-9][a-z0-9/._-]*@sha256:[0-9a-f]{64}$"
_BUCKET = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"


def application_image_components() -> tuple[NativeImageBuildComponentV1, ...]:
    return tuple(NativeImageBuildComponentV1(name=name, dockerfile_path=f"deploy/Dockerfile.{name}",
        context_path=".", oci_output_path=f"oci/{index:04d}.tar") for index, name in enumerate(("service", "web")))


class ApplicationImageRecipeV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.application-image-recipe.v1"] = "loom.application-image-recipe.v1"
    cpu_arch: Literal["x86_64", "arm64"]
    schema_revision: str = Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")
    trusted_image_ref: str = Field(pattern=_IMAGE)
    buildkit_image_ref: str = Field(pattern=r"^[^\s]+@sha256:[0-9a-f]{64}$")
    snapshotter: Literal["overlayfs", "native"] = "overlayfs"
    export_cache_mode: Literal["max", "min"] = "max"
    oci_export_format: Literal["archive", "directory"] = "archive"

    @property
    def digest(self) -> str:
        # Paths and component order are part of the protected recipe, not owner
        # options. A future recipe layout must not hit an old cache/release key.
        return canonical_digest({"recipe": self.model_dump(mode="json"),
            "components": [row.model_dump(mode="json") for row in application_image_components()]})


class ApplicationImageBuildClaimV1(BaseModel):
    """Manager-frozen build input, never accepted directly from an owner route."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.application-image-build.v1"] = "loom.application-image-build.v1"
    build_id: UUID
    attempt: int = Field(gt=0, le=2**63 - 1, strict=True)
    upload_id: UUID
    installation_id: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    data_environment_id: UUID
    cluster_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    source: ApplicationSourceUploadRequestV1
    recipe: ApplicationImageRecipeV1
    storage_endpoint: str
    storage_region: str = Field(pattern=r"^[a-z0-9-]{1,63}$")
    source_bucket: str = Field(pattern=_BUCKET)
    cache_bucket: str | None = Field(default=None, pattern=_BUCKET)
    registry_repository: str = Field(max_length=200, pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/[a-z0-9][a-z0-9/._-]*$")

    @model_validator(mode="after")
    def _bindings(self) -> Self:
        if (any(isinstance(value, UUID) and not value.int for value in self.__dict__.values())
                or self.storage_endpoint != f"https://storage.{self.storage_region}.nebius.cloud"
                or any(part in {"", ".", ".."} for part in self.registry_repository.split("/"))
                or (self.cache_bucket is not None and self.cache_bucket == self.source_bucket)):
            raise ValueError("invalid application image build binding")
        return self

    @property
    def source_key(self) -> str:
        return application_source_object_key(self.source.archive_sha256)

    @property
    def cache_key(self) -> str:
        # The common bounded blob store may share identical app source/recipe
        # across owners. This domain cannot collide with task materializations.
        return canonical_digest({"schema_version": "loom.application-image-cache.v1",
            "source_digest": self.source.source_digest, "recipe_digest": self.recipe.digest}).removeprefix("sha256:")


class ApplicationImagePublicationV1(BaseModel):
    """A complete trusted publisher receipt, not an owner-supplied image claim."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.application-image-publication.v1"] = "loom.application-image-publication.v1"
    build_id: UUID
    attempt: int = Field(gt=0, le=2**63 - 1, strict=True)
    upload_id: UUID
    installation_id: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    data_environment_id: UUID
    cluster_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    recipe_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    schema_revision: str = Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")
    cpu_arch: Literal["x86_64", "arm64"]
    registry_images: dict[Literal["service", "web"], str]

    @model_validator(mode="after")
    def _complete(self) -> Self:
        import re

        if (set(self.registry_images) != {"service", "web"}
                or any(not value.int for value in self.__dict__.values() if isinstance(value, UUID))
                or any(len(ref) > 280 or re.fullmatch(_IMAGE, ref) is None for ref in self.registry_images.values())):
            raise ValueError("application publication is incomplete or invalid")
        return self


class ApplicationImageBuildBindingV1(BaseModel):
    """Protected management build configuration, never owner request fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    source: ApplicationSourceUploadBindingV1
    recipe: ApplicationImageRecipeV1
    storage_endpoint: str
    storage_region: str = Field(pattern=r"^[a-z0-9-]{1,63}$")
    cache_bucket: str | None = Field(default=None, pattern=_BUCKET)
    registry_repository: str = Field(max_length=200, pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/[a-z0-9][a-z0-9/._-]*$")
    pool_id: UUID
    participant_id: UUID
    profile_id: UUID
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    admission_epoch: int = Field(gt=0, le=2**63 - 1, strict=True)
    participant_revision: int = Field(gt=0, le=2**63 - 1, strict=True)

    @model_validator(mode="after")
    def _bindings(self) -> Self:
        if (not all(value.int for value in (self.pool_id, self.participant_id, self.profile_id))
                or self.storage_endpoint != f"https://storage.{self.storage_region}.nebius.cloud"
                or self.cache_bucket == self.source.source_bucket
                or any(part in {"", ".", ".."} for part in self.registry_repository.split("/"))):
            raise ValueError("invalid application builder installation binding")
        return self


class ApplicationImageBuildRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    upload_id: UUID

    @model_validator(mode="after")
    def _identity(self) -> Self:
        if not self.upload_id.int:
            raise ValueError("application source identity required")
        return self


class ApplicationImageBuildAttemptRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    attempt: int = Field(gt=0, le=2**63 - 1, strict=True)


class ApplicationImageBuildStatusV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.application-image-build-status.v1"] = "loom.application-image-build-status.v1"
    build_id: UUID
    upload_id: UUID
    attempt: int = Field(gt=0, le=2**63 - 1, strict=True)
    phase: Literal["queued", "running", "settling", "ready", "failed", "cancelled"]
    desired_state: Literal["running", "cancelled"]
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    recipe_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    created_at: datetime
    release: ApplicationReleaseV1 | None = None
