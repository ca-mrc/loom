"""Stable registered source identity and inventory, independent of upload attempts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Self

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.models.task import TaskConfig, normalize_steps
from loom.service_execution_materialization import (
    ServiceExecutionInputFileV1,
    ServiceExecutionInputManifestV1,
)
from loom.task_bundle_registration import RegisteredTaskBundle, _task_id
from loom.task_image_bundle_manifest import (
    TaskImageBundleContentManifestV1,
    TaskImageBundleManifestFileV1,
    read_verified_task_image_bundle_file,
    task_image_bundle_manifest_key,
)
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME


def task_bundle_catalog_prefix(catalog_id: str) -> str:
    """Group a catalog's sources without making the group an access authority.

    Benchmark IDs contain no slash; qualified task IDs begin with that ID.
    Hashing both group and full task identity keeps arbitrary authored names out
    of object keys. Exact registered objects, not this prefix, authorize access.
    """
    group = _task_id(catalog_id).split("/", 1)[0]
    return f"loom-task-bundles/v1/{hashlib.sha256(group.encode()).hexdigest()}/"


@dataclass(frozen=True, slots=True)
class TaskBundleObjectSpec:
    object_key: str
    content_sha256: str
    size_bytes: int


class TaskBundleSourceSpecV1(BaseModel):
    """Immutable source facts; physical receipt and generation IDs never enter it."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.task-bundle-source.v1"] = "loom.task-bundle-source.v1"
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    catalog_task_id: str = Field(min_length=1, max_length=512)
    bundle_task_id: str = Field(min_length=1, max_length=512)
    task_config_json: str = Field(min_length=1, max_length=1024 * 1024)
    manifest: TaskImageBundleContentManifestV1

    @field_validator("catalog_task_id", "bundle_task_id")
    @classmethod
    def _identity(cls, value: str) -> str:
        return _task_id(value)

    @model_validator(mode="after")
    def _registration_binding(self) -> Self:
        task = normalize_steps(TaskConfig.model_validate_json(self.task_config_json))
        document = task.model_dump(mode="json")
        document["environment"]["network_policies_supported"] = sorted(
            task.environment.network_policies_supported
        )
        if (
            task.task.id != self.catalog_task_id
            or rfc8785.dumps(document).decode() != self.task_config_json
        ):
            raise ValueError("source config must be canonical and bound to the catalog task")
        if not any(item.path == "task.toml" for item in self.manifest.files):
            raise ValueError("source manifest requires task.toml")
        if any(len(item.object_key.encode("utf-8")) > 1024 for item in self.objects):
            raise ValueError("source object key exceeds storage limit")
        return self

    @classmethod
    def from_registration(cls, registration: RegisteredTaskBundle, *, bucket: str) -> Self:
        return cls(
            bucket=bucket,
            catalog_task_id=registration.catalog_task_id,
            bundle_task_id=registration.bundle_task_id,
            task_config_json=rfc8785.dumps(registration.task_config_document).decode(),
            manifest=registration.manifest,
        )

    @property
    def task_config(self) -> dict[str, Any]:
        result: dict[str, Any] = json.loads(self.task_config_json)
        return result

    @property
    def data_prefix(self) -> str:
        task_key = hashlib.sha256(self.catalog_task_id.encode()).hexdigest()
        return f"{task_bundle_catalog_prefix(self.catalog_task_id)}{task_key}/{self.manifest.digest}/"

    @property
    def source_uri(self) -> str:
        return f"s3://{self.bucket}/{self.data_prefix}"

    @property
    def id(self) -> str:
        return hashlib.sha256(self.source_uri.encode()).hexdigest()

    @property
    def service_manifest_key(self) -> str:
        relative = self.data_prefix.removeprefix("loom-task-bundles/v1/").rstrip("/")
        return f"loom-task-bundle-inputs/v1/{relative}.json"

    @property
    def service_manifest(self) -> ServiceExecutionInputManifestV1:
        return ServiceExecutionInputManifestV1(
            task_revision_sha256="sha256:" + self.manifest.task_checksum,
            files=tuple(
                ServiceExecutionInputFileV1(
                    relative_path=item.path,
                    size_bytes=item.size_bytes,
                    sha256="sha256:" + item.sha256,
                    mode=item.mode,
                )
                for item in self.manifest.files
            ),
        )

    @property
    def provenance(self) -> dict[str, Any]:
        config = TaskConfig.model_validate(self.task_config)
        upstream = {} if config.upstream_origin is None else {
            "upstream_origin": config.upstream_origin.model_dump(mode="json", exclude_none=True),
            "upstream_task_id": config.upstream_task_id,
            **({"upstream_package_digest": config.upstream_package_digest} if config.upstream_package_digest else {}),
            **({"conversion": config.upstream_conversion.model_dump(mode="json")}
               if config.upstream_conversion else {}),
        }
        task_file = next(item for item in self.manifest.files if item.path == "task.toml")
        return {
            **upstream,
            "bundle_content_manifest_sha256": self.manifest.digest,
            "bundle_file_metadata_sha256": "sha256:" + self.manifest.bundle_file_metadata_sha256,
            "bundle_task_identity": {
                "schema_version": "loom.task-bundle-identity.v1",
                "catalog_task_id": self.catalog_task_id,
                "bundle_task_id": self.bundle_task_id,
                "bundle_task_toml_sha256": task_file.sha256,
                "registered_config_sha256": hashlib.sha256(
                    self.task_config_json.encode()
                ).hexdigest(),
            },
            "service_execution_input": {
                "schema_version": "loom.service-execution-input.v1",
                "manifest_uri": f"s3://{self.bucket}/{self.service_manifest_key}",
                "manifest_sha256": "sha256:"
                + hashlib.sha256(self.service_manifest.canonical_bytes()).hexdigest(),
                "file_count": len(self.manifest.files),
                "total_bytes": sum(item.size_bytes for item in self.manifest.files),
            },
        }

    @cached_property
    def transport_bodies(self) -> Mapping[str, bytes]:
        return MappingProxyType(
            {
                self.data_prefix + BUNDLE_FILE_METADATA_NAME: self.manifest.mode_metadata_bytes,
                self.service_manifest_key: self.service_manifest.canonical_bytes(),
                task_image_bundle_manifest_key(self.manifest.digest): self.manifest.canonical_bytes,
            }
        )

    @cached_property
    def _authored_files(self) -> Mapping[str, TaskImageBundleManifestFileV1]:
        prefix = self.data_prefix
        return MappingProxyType({prefix + item.path: item for item in self.manifest.files})

    @property
    def objects(self) -> tuple[TaskBundleObjectSpec, ...]:
        # Authored files first, sidecar and service input second, global manifest last.
        prefix = self.data_prefix
        return tuple(
            TaskBundleObjectSpec(prefix + item.path, item.sha256, item.size_bytes)
            for item in self.manifest.files
        ) + tuple(
            TaskBundleObjectSpec(key, hashlib.sha256(body).hexdigest(), len(body))
            for key, body in self.transport_bodies.items()
        )

    def read_object(self, task_dir: Path, key: str) -> bytes:
        transports = self.transport_bodies
        if key in transports:
            return transports[key]
        file = self._authored_files.get(key)
        if file is None:
            raise ValueError("object is outside the registered source inventory")
        return read_verified_task_image_bundle_file(task_dir, file)
