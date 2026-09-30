"""Prepare the existing native builder from a protected profile, without writes."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from uuid import UUID

from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import pool_request_priority
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom.task_image_build_plan import (
    _canonical_bundle_location,
    derive_task_image_build_components,
)
from loom.task_image_materialization import NativeCPUArch
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import render_task_image_job
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_execution_capacity_collector.kubernetes import rendered_pod_resources


@dataclass(frozen=True)
class PoolTaskImageProfile:
    profile_id: UUID
    cpu_arch: NativeCPUArch
    target: ExecutionTargetRuntime
    settings: NativeTaskImageSettings
    runtime_class_overhead: ResourceTotals | None = None


@dataclass(frozen=True)
class PreparedPoolTaskImage:
    request_sha256: str
    resources: ResourceTotals
    pod_slots: int
    namespace_uid: UUID
    lease_epoch: int
    configmap: dict[str, Any]
    job: dict[str, Any]


def prepare_pool_task_image(request: PoolTaskImagePrepareV1, *, participant: PoolParticipantV1,
                            profile: PoolTaskImageProfile, reservation_id: UUID,
                            now: datetime) -> PreparedPoolTaskImage:
    """No capacity/attempt is acquired; activation must freeze the final plan."""
    request = PoolTaskImagePrepareV1.model_validate_json(request.model_dump_json())
    participant = PoolParticipantV1.model_validate(participant.model_dump())
    settings = NativeTaskImageSettings.model_validate(profile.settings.model_dump())
    if (not reservation_id.int or now.utcoffset() is None or request.deadline_at <= now
            or (request.pool_id, request.key.participant_id, request.admission_epoch, request.participant_revision) != (
                participant.pool_id, participant.participant_id, participant.admission_epoch, participant.binding_revision)):
        raise ValueError("pool_build_binding_mismatch")
    target = participant.target(request.target_id, request.key.workload_kind)
    if (profile.profile_id != target.profile_id or profile.cpu_arch != request.build.cpu_arch
            or profile.target.target_id != request.target_id
            or profile.target.namespace != participant.build_namespace.name or settings.namespace != profile.target.namespace
            or (profile.target.runtime_class_name is None) != (profile.runtime_class_overhead is None)):
        raise ValueError("pool_build_profile_mismatch")
    pool_request_priority(participant, request.origin, workload_kind=request.key.workload_kind)
    claim = request.build.claim_snapshot()
    bucket, _ = _canonical_bundle_location(claim["task_source"])
    if bucket != settings.source_bucket:
        raise ValueError("pool_build_source_bucket_mismatch")
    claim.update(settings.runtime_configuration())
    remaining = min(settings.active_deadline_seconds, math.ceil((request.deadline_at - now).total_seconds()))
    config = replace(settings.job_config(), active_deadline_seconds=remaining)
    lease_epoch = request.build.expected_lease_epoch + 1
    configmap, job = render_task_image_job(materialization_id=request.key.local_work_id, lease_epoch=lease_epoch,
        claim=claim, components=derive_task_image_build_components(claim["task_config"]), target=profile.target, config=config)
    # Global selection generations and native attempt epochs are independent.
    # Keep the actual native labels/claim for publication, while each reservation
    # owns a unique Job and ConfigMap even after an unstarted reselection.
    name = f"loom-pool-{reservation_id.hex}"
    for document in (configmap, job):
        document["metadata"]["name"] = name
    for metadata in (configmap["metadata"], job["metadata"], job["spec"]["template"]["metadata"]):
        metadata["labels"]["app.kubernetes.io/managed-by"] = "loom-pool-gateway"
        metadata.setdefault("annotations", {})["loom.openai.com/target-id"] = request.target_id
    pod = job["spec"]["template"]["spec"]
    # Job.activeDeadlineSeconds starts at Job startup, not original admission.
    # Each trusted phase therefore keeps the same absolute cutoff even when
    # CREATE, scheduling or a preceding phase was delayed.
    pod["volumes"].append({"name": "deadline-runtime", "emptyDir": {"sizeLimit": "8Mi"}})
    for phase in [*pod["initContainers"], *pod["containers"]]:
        runtime = "/usr/local/bin/loom-build-deadline"
        extra: list[str] = []
        if phase["name"] in {"prepare", "build"}:
            phase["volumeMounts"].append({"name": "deadline-runtime", "mountPath": "/loom/deadline-runtime",
                                         "readOnly": phase["name"] == "build"})
        if phase["name"] == "prepare":
            extra.append("--install-runtime")
        elif phase["name"] == "build":
            runtime = "/loom/deadline-runtime/loom-build-deadline"
        phase["command"] = [runtime, "--deadline-at", request.deadline_at.isoformat(), *extra, "--", *phase["command"]]
    for volume in pod["volumes"]:
        if volume["name"] == "claim":
            volume["configMap"]["name"] = name
    if (job["spec"]["parallelism"] != 1 or job["spec"]["completions"] != 1
            or pod["nodeSelector"] != profile.target.node_selector
            or not pod["nodeSelector"].get("nebius.com/node-group-id")):
        raise ValueError("pool_build_physical_profile_mismatch")
    accounting = dict(pod)
    if profile.runtime_class_overhead is not None:
        overhead = profile.runtime_class_overhead
        accounting["overhead"] = {"cpu": f"{overhead.cpu_millis}m", "memory": f"{overhead.memory_mib}Mi",
                                  "ephemeral-storage": f"{overhead.storage_mib}Mi"}
    return PreparedPoolTaskImage(
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"),
        resources=rendered_pod_resources(accounting), pod_slots=1, namespace_uid=participant.build_namespace.uid,
        lease_epoch=lease_epoch, configmap=configmap, job=job,
    )
