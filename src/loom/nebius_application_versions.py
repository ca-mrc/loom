"""Public application versions and declared shared-schema compatibility."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_application_contract import ApplicationReleaseV1, ApplicationStatusV1


class _Report(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ApplicationReleaseCompatibilityV1(_Report):
    schema_version: Literal["loom.nebius-application-release-compatibility.v1"] = "loom.nebius-application-release-compatibility.v1"
    scope: Literal["configured_shared_schema"] = "configured_shared_schema"
    release: ApplicationReleaseV1
    shared_schema_revision: str = Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")
    compatibility: Literal["compatible", "schema_mismatch"]

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if (self.release.schema_revision == self.shared_schema_revision) != (self.compatibility == "compatible"):
            raise ValueError("inconsistent application schema compatibility")
        return self


class ApplicationCompletedDeploymentV1(_Report):
    operation_id: UUID
    deployment_generation: int = Field(ge=1, strict=True)
    completed_at: datetime
    release: ApplicationReleaseV1


class ApplicationVersionsV1(_Report):
    schema_version: Literal["loom.nebius-application-versions.v1"] = "loom.nebius-application-versions.v1"
    scope: Literal["deployment_journal"] = "deployment_journal"
    status: ApplicationStatusV1
    requested_release: ApplicationReleaseV1
    last_completed_deployment: ApplicationCompletedDeploymentV1 | None
    shared_schema_revision: str = Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")
    schema_compatibility: Literal["compatible", "schema_mismatch"]

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        row, operation = self.status.registration, self.status.operation
        previous = self.last_completed_deployment
        if (row.release_id != self.requested_release.release_id or operation is None
                or (operation.application_id, operation.deployment_generation, operation.access_generation)
                != (row.application_id, row.deployment_generation, row.access_generation)
                or (previous is not None and (previous.operation_id.int == 0
                    or previous.deployment_generation > row.deployment_generation
                    or (previous.deployment_generation == row.deployment_generation
                        and (previous.release != self.requested_release or previous.operation_id != operation.operation_id
                             or operation.phase != "completed" or operation.action not in {"create", "update", "resume"}))))):
            raise ValueError("inconsistent application version identities")
        ApplicationReleaseCompatibilityV1(release=self.requested_release,
            shared_schema_revision=self.shared_schema_revision, compatibility=self.schema_compatibility)
        return self
