"""Bounded owner journal projection, never live readiness or retry authority."""
from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_application_contract import ApplicationOperationV1


class _Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ApplicationKubernetesEvidenceV1(_Evidence):
    kind: Literal["Namespace", "Secret", "Service", "ServiceAccount", "Pod", "ResourceQuota",
                  "Deployment", "RoleBinding", "Ingress", "NetworkPolicy"]
    action: Literal["create", "patch", "delete"]
    phase: Literal["prepared", "dispatched", "observed", "rejected"]
    count: int = Field(ge=1, le=2**63 - 1, strict=True)


class ApplicationCloudEvidenceV1(_Evidence):
    kind: Literal["service_account", "access_key", "membership"]
    action: Literal["create", "delete"]
    phase: Literal["prepared", "dispatched", "observed"]
    count: int = Field(ge=1, le=2**63 - 1, strict=True)


class ApplicationOperationEvidenceV1(_Evidence):
    schema_version: Literal["loom.nebius-application-operation-evidence.v1"] = "loom.nebius-application-operation-evidence.v1"
    operation: ApplicationOperationV1
    runner_epoch: int = Field(ge=0, strict=True)
    lease_active: bool = Field(strict=True)
    completion_recorded: bool = Field(strict=True)
    kubernetes: tuple[ApplicationKubernetesEvidenceV1, ...] = Field(max_length=128)
    cloud: tuple[ApplicationCloudEvidenceV1, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.lease_active and self.operation.phase != "running":
            raise ValueError("application_evidence_lease_phase")
        if self.completion_recorded and self.operation.phase not in {"completed", "superseded"}:
            raise ValueError("application_evidence_completion_phase")
        for rows in (self.kubernetes, self.cloud):
            if len({(row.kind, row.action, row.phase) for row in rows}) != len(rows):
                raise ValueError("duplicate_application_evidence_group")
        return self
