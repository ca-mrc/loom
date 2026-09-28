"""Personal application identities, distinct from legacy full environments.

These immutable inputs describe already-qualified releases and shared bindings;
they do not authorize publication, grant credentials, or rewrite v1 journals.
"""

from __future__ import annotations

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.application_session import ApplicationSessionAudienceV1
from loom.nebius_environment_contract import (
    _LABEL,
    _PROVIDER_ID,
    _RESERVED_SLUGS,
    FoundationBinding,
    _hostname,
)
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

_REVISION = r"^[a-zA-Z0-9_]{1,64}$"
_IMAGE = r"^[a-z0-9][a-z0-9.:-]*/[a-z0-9][a-z0-9/._-]*@sha256:[0-9a-f]{64}$"
ApplicationAction = Literal["create", "update", "suspend", "resume", "destroy_retained"]


class _Binding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _non_nil_identities(self) -> Self:
        if any(isinstance(value, UUID) and value.int == 0 for value in self.__dict__.values()):
            raise ValueError("application and data identities must not be nil")
        return self


class ApplicationReleaseV1(_Binding):
    """Frontend/API source and images; never an executor runtime selection."""

    schema_version: Literal["loom.nebius-application-release.v1"] = "loom.nebius-application-release.v1"
    release_id: UUID
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    schema_revision: str = Field(pattern=_REVISION)
    service_image_ref: str = Field(pattern=_IMAGE)
    web_image_ref: str = Field(pattern=_IMAGE)


class ApplicationOperationV1(_Binding):
    """Public progress, never private deployment plans or credential material."""

    schema_version: Literal["loom.nebius-application-operation.v1"] = "loom.nebius-application-operation.v1"
    operation_id: UUID
    application_id: UUID
    deployment_generation: int = Field(ge=1, strict=True)
    access_generation: int = Field(ge=1, strict=True)
    action: ApplicationAction
    phase: Literal["pending", "running", "blocked", "completed", "superseded"]
    error_code: str | None = None


class SharedDevelopmentBindingV1(_Binding):
    """References to operator-owned development data and execution services."""

    schema_version: Literal["loom.nebius-shared-development.v1"] = "loom.nebius-shared-development.v1"
    data_environment_id: UUID
    cluster_id: str = Field(pattern=_PROVIDER_ID)
    platform_namespace: str = Field(pattern="^" + _LABEL + "$")
    schema_revision: str = Field(pattern=_REVISION)
    runtime_profile_json: str = Field(min_length=2, max_length=262144)

    @field_validator("runtime_profile_json")
    @classmethod
    def _runtime_shape(cls, value: str) -> str:
        ServiceExecutionRuntimeProfileV1.model_validate_json(value)
        return value

    def validate_foundation(self, foundation: FoundationBinding) -> None:
        config = foundation.platform_config
        if (config["environment"] != "development" or self.cluster_id != config["cluster_id"]
                or self.platform_namespace != config["namespace"]):
            raise ValueError("application requires the protected shared development foundation")


class ApplicationRegistrationV1(_Binding):
    """One personal namespace, with no execution/storage ownership fields."""

    schema_version: Literal["loom.nebius-application-registration.v1"] = "loom.nebius-application-registration.v1"
    application_id: UUID
    incarnation: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    data_environment_id: UUID
    cluster_id: str = Field(pattern=_PROVIDER_ID)
    slug: str = Field(min_length=1, max_length=54, pattern="^" + _LABEL + "$")
    application_namespace: str = Field(pattern="^" + _LABEL + "$")
    public_host: str
    release_id: UUID
    deployment_generation: int = Field(default=1, ge=1, strict=True)
    access_generation: int = Field(default=1, ge=1, strict=True)
    desired_state: Literal["active", "suspended", "destroyed"] = "active"

    _host = field_validator("public_host")(_hostname)

    @model_validator(mode="after")
    def _physical_identity(self) -> Self:
        if self.slug in _RESERVED_SLUGS or self.application_namespace != "loom-dev-" + self.slug:
            raise ValueError("application requires its generated personal namespace")
        return self

    @property
    def session_audience(self) -> ApplicationSessionAudienceV1:
        return ApplicationSessionAudienceV1(application_id=self.application_id,
                                            access_generation=self.access_generation,
                                            origin="https://" + self.public_host)


class ApplicationCreateRequestV1(_Binding):
    slug: str = Field(min_length=1, max_length=54, pattern="^" + _LABEL + "$")
    release_id: UUID

    @field_validator("slug")
    @classmethod
    def _personal_slug(cls, value: str) -> str:
        if value in _RESERVED_SLUGS:
            raise ValueError("application requires a personal slug")
        return value


class ApplicationOperationRequestV1(_Binding):
    action: Literal["update", "suspend", "resume", "destroy_retained"]
    expected_generation: int = Field(ge=1, strict=True)
    release_id: UUID | None = None

    @model_validator(mode="after")
    def _update_release(self) -> Self:
        if (self.action == "update") != (self.release_id is not None):
            raise ValueError("only update requires a release identity")
        return self


class ApplicationStatusV1(_Binding):
    registration: ApplicationRegistrationV1
    operation: ApplicationOperationV1 | None


def new_application_registration(
    foundation: FoundationBinding, shared: SharedDevelopmentBindingV1, *,
    application_id: UUID, incarnation: UUID, owner_user_id: UUID, owner_team_id: UUID,
    slug: str, release_id: UUID,
) -> ApplicationRegistrationV1:
    shared.validate_foundation(foundation)
    return ApplicationRegistrationV1(
        application_id=application_id, incarnation=incarnation,
        owner_user_id=owner_user_id, owner_team_id=owner_team_id,
        data_environment_id=shared.data_environment_id, cluster_id=shared.cluster_id,
        slug=slug, application_namespace="loom-dev-" + slug,
        public_host=slug + "." + foundation.public_dns_zone, release_id=release_id,
    )
