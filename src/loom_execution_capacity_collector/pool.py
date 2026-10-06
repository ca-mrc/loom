"""Protected identity bindings for one physical, multi-environment observation.

These inputs come from the management registry and the protected Job journal,
never from child-supplied labels. They confer no create or cleanup authority.
The reservation key is placement-only; it is not a child execution lease.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_execution_capacity_collector.contracts import ManagedPodPlacement

_NAMESPACE = r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$"
_LABEL_NAME = re.compile(r"^[A-Za-z0-9](?:[-_.A-Za-z0-9]{0,61}[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_TargetId = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")]


class PoolEnvironmentBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    environment_id: UUID
    incarnation: UUID
    execution_namespace: str = Field(pattern=_NAMESPACE)
    build_namespace: str = Field(pattern=_NAMESPACE)
    target_ids: tuple[_TargetId, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def identities(self) -> PoolEnvironmentBinding:
        if not self.environment_id.int or not self.incarnation.int:
            raise ValueError("nil environment identity")
        if self.execution_namespace == self.build_namespace:
            raise ValueError("execution and build namespaces must differ")
        if len(set(self.target_ids)) != len(self.target_ids):
            raise ValueError("duplicate environment-local target")
        return self


class GatewayJobBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    reservation_id: UUID
    environment_id: UUID
    incarnation: UUID
    namespace: str = Field(pattern=_NAMESPACE)
    job_name: str = Field(min_length=1, max_length=253)
    job_uid: str = Field(min_length=1, max_length=253)
    target_id: _TargetId
    workload_kind: Literal["trial", "verifier", "task_image_build", "application_image_build"]
    lease_id: str = Field(min_length=1, max_length=160)
    generation: int = Field(gt=0, strict=True)

    @model_validator(mode="after")
    def identities(self) -> GatewayJobBinding:
        if not all(value.int for value in (self.reservation_id, self.environment_id, self.incarnation)):
            raise ValueError("nil gateway identity")
        return self


class PoolObservationScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # Exact physical pool labels from protected installation/native readback.
    # This entrypoint does not itself attest a selector's cloud node-group ID.
    node_selector: dict[str, str] = Field(min_length=1, max_length=32)
    environments: tuple[PoolEnvironmentBinding, ...] = Field(max_length=10_000)
    jobs: tuple[GatewayJobBinding, ...] = Field(max_length=200_000)

    @field_validator("node_selector")
    @classmethod
    def valid_selector(cls, selector: dict[str, str]) -> dict[str, str]:
        for key, value in selector.items():
            prefix, separator, name = key.rpartition("/")
            if not separator:
                name = key
            if (not _LABEL_NAME.fullmatch(name) or (value and not _LABEL_NAME.fullmatch(value))
                    or (separator and (not prefix or len(prefix) > 253 or any(
                        not _DNS_LABEL.fullmatch(part) for part in prefix.split("."))))):
                raise ValueError("invalid physical pool selector")
        return dict(selector)

    @model_validator(mode="after")
    def unambiguous_scope(self) -> PoolObservationScope:
        for values in (
            [row.environment_id for row in self.environments],
            [row.incarnation for row in self.environments],
            [name for row in self.environments for name in (row.execution_namespace, row.build_namespace)],
            [row.reservation_id for row in self.jobs],
            [row.job_uid for row in self.jobs],
            [(row.namespace, row.job_name) for row in self.jobs],
        ):
            if len(values) != len(set(values)):
                raise ValueError("duplicate pool binding identity")
        environments = {row.environment_id: row for row in self.environments}
        for job in self.jobs:
            environment = environments.get(job.environment_id)
            if environment is None or environment.incarnation != job.incarnation:
                raise ValueError("gateway Job has no matching environment incarnation")
            if job.target_id not in environment.target_ids:
                raise ValueError("gateway Job has no matching environment-local target")
            expected = (environment.build_namespace if job.workload_kind in {"task_image_build", "application_image_build"}
                        else environment.execution_namespace)
            if job.namespace != expected:
                raise ValueError("gateway Job namespace does not match workload scope")
        return self


class PoolPodClassifier:
    """A defensive per-capture copy; no ambient or child namespace authority."""

    def __init__(self, scope: PoolObservationScope):
        # Revalidate even model_copy/construct inputs and detach mutable dicts
        # before inventory runs in its background thread.
        self.scope = PoolObservationScope.model_validate(scope.model_dump())
        self.namespaces = {name: row for row in self.scope.environments
                           for name in (row.execution_namespace, row.build_namespace)}
        self.jobs = {(row.namespace, row.job_uid): row for row in self.scope.jobs}
        self.node_selector = ",".join(f"{key}={value}" for key, value in sorted(self.scope.node_selector.items()))
        payload = self.scope.model_dump(mode="json")
        payload["environments"] = sorted(payload["environments"], key=lambda row: row["environment_id"])
        for environment in payload["environments"]:
            environment["target_ids"] = sorted(environment["target_ids"])
        payload["jobs"] = sorted(payload["jobs"], key=lambda row: row["reservation_id"])
        self.fingerprint = "sha256:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
        ).hexdigest()

    def registered(self, pod: Any) -> bool:
        return pod.metadata.namespace in self.namespaces

    def includes_pending(self, pod: Any) -> bool:
        # A namespace also hosts platform-resident controllers and collectors.
        # Only a protected Job receipt overrides a contradictory pool selector.
        # Preserve fail-closed handling even if that Job's Pod has damaged owner
        # or workload labels and cannot receive a managed-reservation discount.
        if any((pod.metadata.namespace, getattr(owner, "uid", None)) in self.jobs
               for owner in (getattr(pod.metadata, "owner_references", None) or [])):
            return True
        selector = getattr(pod.spec, "node_selector", None) or {}
        # Only a contradictory hard equality on a pool label proves exclusion.
        # Unknown affinity/taints/selectors are conservative potential occupancy.
        return not any(key in selector and selector[key] != value
                       for key, value in self.scope.node_selector.items())

    def managed(self, pod: Any) -> ManagedPodPlacement | None:
        from loom_execution_capacity_collector.kubernetes import _pod_request

        namespace = pod.metadata.namespace
        environment = self.namespaces.get(namespace)
        if environment is None:
            return None
        owners = [row for row in (getattr(pod.metadata, "owner_references", None) or [])
                  if getattr(row, "controller", None) is True]
        if len(owners) != 1:
            return None
        owner = owners[0]
        owner_uid = getattr(owner, "uid", None)
        if not isinstance(owner_uid, str):
            return None
        job = self.jobs.get((namespace, owner_uid))
        if (job is None or owner.kind != "Job" or owner.api_version != "batch/v1"
                or owner.name != job.job_name):
            return None
        labels = getattr(pod.metadata, "labels", None) or {}
        annotations = getattr(pod.metadata, "annotations", None) or {}
        component = labels.get("app.kubernetes.io/component")
        task_build = component == "task-image-builder"
        app_build = component == "application-image-builder"
        if (task_build != (job.workload_kind == "task_image_build")
                or app_build != (job.workload_kind == "application_image_build")):
            return None
        if annotations.get("loom.openai.com/target-id") != job.target_id:
            return None
        identity: object
        if task_build:
            identity = "task-image:" + str(labels.get("loom.materialization-id", ""))
            generation = labels.get("loom.lease-epoch")
        elif app_build:
            identity = "application-image:" + str(labels.get("loom.application-build-id", ""))
            generation = labels.get("loom.build-attempt")
        else:
            if labels.get("app.kubernetes.io/managed-by") != "loom-execution-actuator":
                return None
            role = "attempt" if job.workload_kind == "trial" else "verifier"
            if annotations.get("loom.openai.com/execution-role") != role:
                return None
            identity = labels.get("loom.openai.com/lease-id")
            generation = labels.get("loom.openai.com/generation")
        if identity != job.lease_id or generation != str(job.generation):
            return None
        return ManagedPodPlacement(
            uid=pod.metadata.uid, lease_id=f"reservation:{job.reservation_id}",
            generation=1, requests=_pod_request(pod),
        )

    def validate_inventory(self, nodes: list[Any], pods: list[Any]) -> None:
        from loom_execution_capacity_collector.kubernetes import (
            KubernetesObservationError,
            _identity,
        )

        for values, name in (
            ([getattr(node.metadata, "uid", None) for node in nodes], "Node UID"),
            ([getattr(node.spec, "provider_id", None) for node in nodes], "Node provider ID"),
            ([getattr(pod.metadata, "uid", None) for pod in pods], "Pod UID"),
        ):
            identities = [_identity(value, name=name) for value in values]
            if len(set(identities)) != len(identities):
                raise KubernetesObservationError(f"Kubernetes {name} is duplicated")
        for node in nodes:
            labels = getattr(node.metadata, "labels", None) or {}
            if any(labels.get(key) != value for key, value in self.scope.node_selector.items()):
                raise KubernetesObservationError("Kubernetes node does not match physical pool selector")
