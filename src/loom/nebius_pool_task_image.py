"""Typed native build selection; no caller Kubernetes or credential settings."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.models.task import TaskConfig
from loom.nebius_pool_contract import PoolRequestKeyV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from loom.service_execution_materialization import ServiceExecutionInputBindingV1
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_build_plan import _canonical_bundle_location
from loom.task_image_materialization import (
    NativeCPUArch,
    declared_task_image_architectures,
    task_image_materialization_key,
)

_Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
_ModeDigest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class PoolLegacyBuildSourceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["legacy"]
    uri: str = Field(max_length=4096)
    bundle_file_metadata_sha256: _ModeDigest | None
    input_manifest: ServiceExecutionInputBindingV1 | None

    @model_validator(mode="after")
    def qualified_source(self) -> PoolLegacyBuildSourceV1:
        bucket, _ = _canonical_bundle_location(self.uri)
        if self.bundle_file_metadata_sha256 is None and self.input_manifest is None:
            raise ValueError("pool_build_source_modes_unbound")
        if self.input_manifest is not None:
            manifest_bucket, _ = _canonical_bundle_location(self.input_manifest.manifest_uri + "/")
            if bucket != manifest_bucket or self.input_manifest.manifest_uri.endswith("/"):
                raise ValueError("pool_build_manifest_scope")
        return self

    def provenance(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.bundle_file_metadata_sha256 is not None:
            result["bundle_file_metadata_sha256"] = self.bundle_file_metadata_sha256
        if self.input_manifest is not None:
            result["service_execution_input"] = self.input_manifest.model_dump(mode="json")
        return result


class PoolRegisteredBuildSourceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["registered"]
    registration: TaskBundleSourceSpecV1

    @field_validator("registration", mode="before")
    @classmethod
    def json_registration(cls, value: Any) -> TaskBundleSourceSpecV1:
        # The existing manifest requires strict tuples in Python mode, but its
        # wire format is JSON arrays. Always use its canonical JSON reader.
        document = value.model_dump_json() if isinstance(value, TaskBundleSourceSpecV1) else json.dumps(value, allow_nan=False)
        return TaskBundleSourceSpecV1.model_validate_json(document)


class PoolTaskImageWorkloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    expected_lease_epoch: int = Field(ge=0, le=2**63 - 2, strict=True)
    materialization_key: _Digest
    task_id: str = Field(min_length=1, max_length=512)
    task_checksum: _Digest
    cpu_arch: NativeCPUArch
    task_config_json: str = Field(min_length=1, max_length=256 * 1024)
    source: Annotated[PoolLegacyBuildSourceV1 | PoolRegisteredBuildSourceV1, Field(discriminator="kind")]

    @model_validator(mode="after")
    def frozen_identity(self) -> PoolTaskImageWorkloadV1:
        task = TaskConfig.model_validate_json(self.task_config_json)
        payload = json.loads(self.task_config_json)
        if ("\0" in self.task_id or rfc8785.dumps(payload).decode() != self.task_config_json
                or self.cpu_arch not in declared_task_image_architectures(task)):
            raise ValueError("pool_build_task_identity")
        content_digest = ""
        if isinstance(self.source, PoolRegisteredBuildSourceV1):
            source = self.source.registration
            if (self.task_id, self.task_checksum, self.task_config_json) != (
                    source.catalog_task_id, source.manifest.task_checksum, source.task_config_json):
                raise ValueError("pool_build_registered_source_mismatch")
            content_digest = source.manifest.digest
        if self.materialization_key != task_image_materialization_key(
            task_id=self.task_id, task_checksum=self.task_checksum, cpu_arch=self.cpu_arch,
            bundle_content_manifest_sha256=content_digest,
        ):
            raise ValueError("pool_build_materialization_mismatch")
        return self

    def claim_snapshot(self) -> dict[str, Any]:
        """Source data only. Caller adds protected configuration and Job identity."""
        if isinstance(self.source, PoolRegisteredBuildSourceV1):
            uri = self.source.registration.source_uri
            provenance = self.source.registration.provenance
        else:
            uri, provenance = self.source.uri, self.source.provenance()
        return {"materialization_key": self.materialization_key, "task_id": self.task_id,
                "task_checksum": self.task_checksum, "cpu_arch": self.cpu_arch,
                "task_config": json.loads(self.task_config_json), "task_source": uri,
                "task_source_provenance": provenance}


class PoolTaskImagePrepareV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.pool-task-image-prepare.v1"] = "loom.pool-task-image-prepare.v1"
    pool_id: UUID
    admission_epoch: _Generation
    participant_revision: _Generation
    key: PoolRequestKeyV1
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    deadline_at: datetime
    origin: PoolWorkOriginV1
    build: PoolTaskImageWorkloadV1

    @model_validator(mode="after")
    def consistent_request(self) -> PoolTaskImagePrepareV1:
        if (not self.pool_id.int or self.deadline_at.utcoffset() is None
                or self.key.workload_kind != "task_image_build" or self.origin.kind == "personal_build"):
            raise ValueError("pool_build_request_identity")
        object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))
        return self
