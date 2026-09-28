"""Internal no-secret attestations from concrete retirement adapters.

These are not owner input or substitutes for provider checks. Completion validates
them against the still-current lease, immutable effects and encrypted material.
"""
from __future__ import annotations

import hashlib
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from loom.nebius_application_identity import MembershipRole
from loom_service.application_management.leases import ApplicationLease

Identifier = Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9._:-]{1,256}$")]
Digest = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[StrictInt, Field(gt=0)]


class _Proof(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class ApplicationRetirementIdentity(_Proof):
    operation_id: UUID
    application_id: UUID
    incarnation: UUID
    data_environment_id: UUID
    deployment_generation: Positive
    access_generation: Positive
    runner_epoch: Positive
    lease_sha256: Digest

    @classmethod
    def for_lease(cls, lease: ApplicationLease, data_environment_id: UUID) -> ApplicationRetirementIdentity:
        return cls(operation_id=lease.operation_id, application_id=lease.application_id,
            incarnation=lease.incarnation, data_environment_id=data_environment_id,
            deployment_generation=lease.deployment_generation, access_generation=lease.access_generation,
            runner_epoch=lease.runner_epoch, lease_sha256=hashlib.sha256(lease.lease_token.bytes).hexdigest())


class ApplicationResourceObservation(_Proof):
    operation_id: UUID
    key: Identifier
    name: Identifier
    uid: Identifier
    resource_version: Identifier


class ApplicationSharedPolicyObservation(_Proof):
    name: Identifier
    uid: Identifier
    resource_version: Identifier


class ApplicationAccessReadiness(_Proof):
    identity: ApplicationRetirementIdentity
    schema_revision: Annotated[StrictStr, Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")]
    database_role: Identifier
    user_id: UUID
    team_id: UUID
    membership_role: MembershipRole
    access_key_sha256: Digest


class ApplicationPreparationReadiness(_Proof):
    identity: ApplicationRetirementIdentity
    namespace: ApplicationResourceObservation
    resources: tuple[ApplicationResourceObservation, ...]


class ApplicationDeploymentRetirement(ApplicationResourceObservation):
    generation: Positive
    observed_generation: Positive


class ApplicationWorkloadRetirement(_Proof):
    identity: ApplicationRetirementIdentity
    namespace: ApplicationResourceObservation
    fence: ApplicationResourceObservation
    pods_resource_version: Identifier
    deployments: tuple[ApplicationDeploymentRetirement, ...]


class ApplicationDeploymentReadiness(ApplicationDeploymentRetirement):
    replicas: Positive


class ApplicationWorkloadReadiness(_Proof):
    identity: ApplicationRetirementIdentity
    namespace: ApplicationResourceObservation
    activation_key: Identifier
    retired_quota_uid: Identifier
    deployments: tuple[ApplicationDeploymentReadiness, ...]
    services: tuple[ApplicationResourceObservation, ...]
    ingress: ApplicationResourceObservation


class ApplicationDatabaseRetirement(_Proof):
    identity: ApplicationRetirementIdentity
    retired_through: Annotated[StrictInt, Field(ge=0)]


class ApplicationKeyRetirement(_Proof):
    operation_id: UUID
    key: Identifier
    access_key_sha256: Digest


class ApplicationCloudRetirement(_Proof):
    identity: ApplicationRetirementIdentity
    keys: tuple[ApplicationKeyRetirement, ...]


class ApplicationStopEvidence(_Proof):
    workloads: ApplicationWorkloadRetirement
    database: ApplicationDatabaseRetirement
    objects: ApplicationCloudRetirement


class ApplicationStartupPreparation(_Proof):
    workloads: ApplicationWorkloadRetirement
    database: ApplicationDatabaseRetirement
    objects: ApplicationCloudRetirement
    prepared: ApplicationPreparationReadiness
    access: ApplicationAccessReadiness
    network: tuple[ApplicationSharedPolicyObservation, ...]
