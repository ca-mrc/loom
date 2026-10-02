"""Render a retained application build with the common native pool mechanism."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

from loom.application_image_build import ApplicationImageRecipeV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import pool_request_priority
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.application_image_renderer import render_application_image_job
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_service.pool_management.native_builds import PreparedPoolNativeBuild, bind_native_build


@dataclass(frozen=True)
class PoolApplicationImageProfile:
    profile_id: UUID
    recipe: ApplicationImageRecipeV1
    target: ExecutionTargetRuntime
    settings: NativeTaskImageSettings
    runtime_class_overhead: ResourceTotals | None = None


def prepare_pool_application_image(request: PoolApplicationImagePrepareV1, *, participant: PoolParticipantV1,
                                   profile: PoolApplicationImageProfile, reservation_id: UUID,
                                   now: datetime) -> PreparedPoolNativeBuild:
    """Pure renderer; admission must additionally verify management build history."""
    request = PoolApplicationImagePrepareV1.model_validate_json(request.model_dump_json())
    participant = PoolParticipantV1.model_validate(participant.model_dump())
    settings = NativeTaskImageSettings.model_validate(profile.settings.model_dump())
    claim = request.build
    if (not reservation_id.int or now.utcoffset() is None or request.deadline_at <= now
            or (request.pool_id, request.key.participant_id, request.admission_epoch, request.participant_revision,
                claim.installation_id, claim.data_environment_id) != (
                participant.pool_id, participant.participant_id, participant.admission_epoch, participant.binding_revision,
                participant.installation_id, participant.environment_id)):
        raise ValueError("pool_application_build_binding_mismatch")
    target = participant.target(request.target_id, request.key.workload_kind)
    if (profile.profile_id != target.profile_id or profile.recipe != claim.recipe
            or profile.target.target_id != request.target_id
            or profile.target.namespace != participant.build_namespace.name or settings.namespace != profile.target.namespace
            or (profile.target.runtime_class_name is None) != (profile.runtime_class_overhead is None)):
        raise ValueError("pool_application_build_profile_mismatch")
    pool_request_priority(participant, request.origin, workload_kind=request.key.workload_kind)
    if any(getattr(claim, field) != getattr(settings, field) for field in (
            "storage_endpoint", "storage_region", "source_bucket", "cache_bucket", "registry_repository")):
        raise ValueError("pool_application_build_storage_mismatch")
    remaining = min(settings.active_deadline_seconds, math.ceil((request.deadline_at - now).total_seconds()))
    config = replace(settings.job_config(), active_deadline_seconds=remaining)
    configmap, job = render_application_image_job(claim=claim, target=profile.target, config=config)
    return bind_native_build(configmap=configmap, job=job, reservation_id=reservation_id,
        target=profile.target, namespace_uid=participant.build_namespace.uid, lease_epoch=claim.attempt,
        deadline_at=request.deadline_at, runtime_class_overhead=profile.runtime_class_overhead,
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"))
