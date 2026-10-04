"""Qualify management-owned personal build inputs after the global pool lock."""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from loom.pipeline.keys import canonical_digest
from loom_service.application_management.build_registry import retained_build_claim


async def qualify_application_build_history(session: AsyncSession, origin: PoolWorkOriginV1, *,
                                            participant: PoolParticipantV1, cluster_id: str,
                                            request: PoolApplicationImagePrepareV1 | None,
                                            lock_history: bool) -> None:
    """No network, flush or commit; retain one exact attempt, never caller source."""
    if any(isinstance(value, (NebiusApplicationBuild, NebiusApplicationBuildAttempt, NebiusApplicationSourceUpload))
            for value in session.new | session.dirty | session.deleted):
        raise ValueError("uncommitted_application_build_history")
    lock = {"read": True} if lock_history else None
    row = await session.get(NebiusApplicationBuild, origin.submission_id,
        with_for_update=lock, populate_existing=True)
    if row is None:
        raise ValueError("application_build_history_unavailable")
    attempt = await session.get(NebiusApplicationBuildAttempt, (row.build_id, row.current_attempt),
        with_for_update=lock, populate_existing=True)
    source = await session.get(NebiusApplicationSourceUpload, row.upload_id,
        with_for_update=lock, populate_existing=True)
    binding, claim = retained_build_claim(row, attempt, source)
    target = participant.target(binding.target_id, "application_image_build")
    if (row.installation_id, row.data_environment_id, row.cluster_id, binding.pool_id,
            binding.participant_id, binding.profile_id, binding.participant_revision, binding.admission_epoch) != (
            participant.installation_id, participant.environment_id, cluster_id, participant.pool_id,
            participant.participant_id, target.profile_id, participant.binding_revision, participant.admission_epoch):
        raise ValueError("application_build_history_scope")
    if lock_history and request is None:
        raise ValueError("application_build_request_required")
    if request is not None:
        if (request.build != claim or request.target_id != binding.target_id
                or request.origin != origin or request.pool_id != binding.pool_id
                or request.key.participant_id != binding.participant_id
                or request.admission_epoch != binding.admission_epoch
                or request.participant_revision != binding.participant_revision
                or row.desired_state != "running" or attempt is None or attempt.phase not in {"queued", "running"}
                or attempt.pool_request_json != request.model_dump(mode="json")
                or attempt.pool_request_sha256 != canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:")):
            raise ValueError("application_build_attempt_unavailable")
