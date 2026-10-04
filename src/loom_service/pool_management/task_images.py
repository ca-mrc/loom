"""Prepare the existing native builder from a protected profile, without writes."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime
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
from loom_service.pool_management.native_builds import PreparedPoolNativeBuild, bind_native_build

PreparedPoolTaskImage = PreparedPoolNativeBuild


@dataclass(frozen=True)
class PoolTaskImageProfile:
    profile_id: UUID
    cpu_arch: NativeCPUArch
    target: ExecutionTargetRuntime
    settings: NativeTaskImageSettings
    runtime_class_overhead: ResourceTotals | None = None


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
    return bind_native_build(configmap=configmap, job=job, reservation_id=reservation_id,
        target=profile.target, namespace_uid=participant.build_namespace.uid, lease_epoch=lease_epoch,
        deadline_at=request.deadline_at, runtime_class_overhead=profile.runtime_class_overhead,
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"),
    )
