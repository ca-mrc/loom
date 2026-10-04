"""Bounded installer-owned catalog for the existing fixed workload renderers."""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.application_image_build import ApplicationImageRecipeV1
from loom.execution_contract import ExecutionClassV1
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.pipeline.keys import canonical_digest
from loom.task_image_materialization import NativeCPUArch
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_service.pool_management.application_images import PoolApplicationImageProfile
from loom_service.pool_management.registry import PoolProfiles
from loom_service.pool_management.render import PoolExecutionProfile
from loom_service.pool_management.task_images import PoolTaskImageProfile


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _target(target: ExecutionTargetRuntime, overhead: ResourceTotals | None) -> None:
    if (not (target.node_selector or {}).get("nebius.com/node-group-id")
            or (target.runtime_class_name is None) != (overhead is None)):
        raise ValueError("unqualified physical target")


class _Execution(_Strict):
    profile_id: UUID
    runtime: ExecutionTargetRuntime
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    execution_class_id: str
    runtime_image_ref: str = Field(pattern=r"^.+@sha256:[0-9a-f]{64}$")
    runtime_binary_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    execution_class: ExecutionClassV1
    runtime_class_overhead: ResourceTotals | None = None

    @model_validator(mode="after")
    def qualified(self) -> _Execution:
        _target(self.runtime, self.runtime_class_overhead)
        if not self.profile_id.int or self.execution_class_id != self.execution_class.class_id:
            raise ValueError("invalid execution profile identity")
        return self

    def profile(self, keyring: ImageAdmissionKeyring) -> PoolExecutionProfile:
        return PoolExecutionProfile(profile_id=self.profile_id, runtime=self.runtime, candidate_sha=self.candidate_sha,
            execution_class_id=self.execution_class_id, runtime_image_ref=self.runtime_image_ref,
            runtime_binary_sha256=self.runtime_binary_sha256, execution_class=self.execution_class,
            image_admission_keyring=keyring, runtime_class_overhead=self.runtime_class_overhead)


class _TaskImage(_Strict):
    profile_id: UUID
    cpu_arch: NativeCPUArch
    target: ExecutionTargetRuntime
    settings: NativeTaskImageSettings
    runtime_class_overhead: ResourceTotals | None = None

    @model_validator(mode="after")
    def qualified(self) -> _TaskImage:
        _target(self.target, self.runtime_class_overhead)
        if (not self.profile_id.int or self.target.namespace != self.settings.namespace
                or re.fullmatch(r".+@sha256:[0-9a-f]{64}", self.settings.service_image) is None):
            raise ValueError("invalid native build profile identity")
        self.settings.job_config()
        return self

    def profile(self) -> PoolTaskImageProfile:
        return PoolTaskImageProfile(profile_id=self.profile_id, cpu_arch=self.cpu_arch,
            target=self.target, settings=self.settings, runtime_class_overhead=self.runtime_class_overhead)


class _ApplicationImage(_Strict):
    profile_id: UUID
    recipe: ApplicationImageRecipeV1
    target: ExecutionTargetRuntime
    settings: NativeTaskImageSettings
    runtime_class_overhead: ResourceTotals | None = None

    @model_validator(mode="after")
    def qualified(self) -> _ApplicationImage:
        _target(self.target, self.runtime_class_overhead)
        config = self.settings.job_config()
        if (not self.profile_id.int or self.target.namespace != self.settings.namespace
                or (config.service_image, config.buildkit_image, config.snapshotter, config.export_cache_mode,
                    config.oci_export_format) != (self.recipe.trusted_image_ref, self.recipe.buildkit_image_ref,
                    self.recipe.snapshotter, self.recipe.export_cache_mode, self.recipe.oci_export_format)
                or (self.settings.cache_secret_name is None) != (self.settings.cache_bucket is None)):
            raise ValueError("invalid application build profile identity")
        return self

    def profile(self) -> PoolApplicationImageProfile:
        return PoolApplicationImageProfile(profile_id=self.profile_id, recipe=self.recipe,
            target=self.target, settings=self.settings, runtime_class_overhead=self.runtime_class_overhead)


class PoolProfileCatalog(_Strict):
    schema_version: Literal["loom.pool-profiles.v1"]
    image_admission_keyring: dict[str, Any]
    execution: tuple[_Execution, ...] = Field(max_length=128)
    task_images: tuple[_TaskImage, ...] = Field(max_length=128)
    application_images: tuple[_ApplicationImage, ...] = Field(default=(), max_length=128, exclude_if=lambda value: not value)

    def profiles(self) -> PoolProfiles:
        if ((not self.execution and not self.task_images and not self.application_images)
                or len({row.profile_id for row in self.execution}) != len(self.execution)
                or len({row.profile_id for row in self.task_images}) != len(self.task_images)
                or len({row.profile_id for row in self.application_images}) != len(self.application_images)):
            raise ValueError("empty or duplicate pool profiles")
        keyring = ImageAdmissionKeyring.from_json(json.dumps(self.image_admission_keyring))
        return PoolProfiles(MappingProxyType({row.profile_id: row.profile(keyring) for row in self.execution}),
            MappingProxyType({row.profile_id: row.profile() for row in self.task_images}),
            MappingProxyType({row.profile_id: row.profile() for row in self.application_images}),
            canonical_digest(self.model_dump(mode="json")).removeprefix("sha256:"))


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate catalog field")
        result[key] = value
    return result


def load_pool_profiles(path: Path) -> PoolProfiles:
    try:
        # Projected ConfigMaps use symlinks. Qualify the opened inode, not the
        # link; nonblocking open avoids hanging on an accidentally supplied FIFO.
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("not a regular file")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(2 * 1024 * 1024 + 1)
        finally:
            os.close(descriptor)
        if len(payload) > 2 * 1024 * 1024:
            raise ValueError("catalog too large")
        return PoolProfileCatalog.model_validate(json.loads(payload, object_pairs_hook=_object)).profiles()
    except (OSError, ValueError, TypeError):
        # Configuration errors must not echo credentials accidentally pasted into
        # the catalog. The catalog itself never establishes registration authority.
        raise ValueError("invalid_pool_profile_catalog") from None
