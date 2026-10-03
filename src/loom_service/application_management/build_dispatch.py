"""Freeze one application build request before any shared-pool network operation.

This is not admission or a running worker. The caller uses the committed request
for both first dispatch and uncertain-reply recovery, never a regenerated deadline.
No transaction or row lock escapes a method into network I/O.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select

from loom.application_image_build import (
    ApplicationImageBuildBindingV1,
    ApplicationImageBuildClaimV1,
)
from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolRequestKeyV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from loom.pipeline.keys import canonical_digest
from loom_service.application_management.build_registry import (
    ApplicationBuildRegistry,
    retained_build_claim,
)
from loom_service.environment_management.registry import ManagementError


def _request(binding: ApplicationImageBuildBindingV1, claim: ApplicationImageBuildClaimV1,
             deadline: datetime) -> PoolApplicationImagePrepareV1:
    return PoolApplicationImagePrepareV1(pool_id=binding.pool_id, admission_epoch=binding.admission_epoch,
        participant_revision=binding.participant_revision, target_id=binding.target_id,
        key=PoolRequestKeyV1(participant_id=binding.participant_id, workload_kind="application_image_build",
            local_work_id=claim.build_id, generation=claim.attempt),
        origin=PoolWorkOriginV1(kind="personal_build", submission_id=claim.build_id,
            data_environment_id=claim.data_environment_id, application=None), deadline_at=deadline, build=claim)


class ApplicationBuildDispatch:
    def __init__(self, registry: ApplicationBuildRegistry, *, request_lifetime_seconds: int = 3600):
        if type(request_lifetime_seconds) is not int or not 60 <= request_lifetime_seconds <= 86400:
            raise ValueError("invalid_application_build_request_lifetime")
        self.registry, self.request_lifetime_seconds = registry, request_lifetime_seconds

    async def freeze(self, build_id: UUID, *, attempt: int) -> PoolApplicationImagePrepareV1:
        """Internal management operation, never a route accepting owner authority.

        Existing requests remain readable for cancellation/reconciliation after
        the desired state changes. Returning one is not permission to activate it.
        Recipe changes cannot replace a retained build's source/profile binding.
        """
        if not isinstance(build_id, UUID) or not build_id.int or type(attempt) is not int or not 0 < attempt < 2**63:
            raise ManagementError("invalid_application_build_attempt", 422)
        scope = self.registry.binding.source
        async with self.registry.session_factory.begin() as session:
            # No pool call or lock acquisition while holding history locks. Pool
            # admission later takes its own pool-first locks in another transaction.
            row = await session.scalar(select(NebiusApplicationBuild).where(
                NebiusApplicationBuild.build_id == build_id, NebiusApplicationBuild.current_attempt == attempt,
                NebiusApplicationBuild.installation_id == scope.installation_id,
                NebiusApplicationBuild.data_environment_id == scope.data_environment_id,
                NebiusApplicationBuild.cluster_id == scope.cluster_id).with_for_update())
            if row is None:
                raise ManagementError("application_build_dispatch_unavailable")
            saved = await session.get(NebiusApplicationBuildAttempt, (build_id, attempt), with_for_update=True)
            source = await session.get(NebiusApplicationSourceUpload, row.upload_id, with_for_update={"read": True})
            try:
                binding, claim = retained_build_claim(row, saved, source)
                assert saved is not None
                if saved.pool_request_json is not None:
                    request = PoolApplicationImagePrepareV1.model_validate(saved.pool_request_json)
                    if (request != _request(binding, claim, request.deadline_at)
                            or canonical_digest(saved.pool_request_json).removeprefix("sha256:") != saved.pool_request_sha256):
                        raise ValueError
                    return request
            except (ValueError, TypeError):
                raise ManagementError("application_build_dispatch_conflict") from None
            if row.desired_state != "running" or saved.phase != "queued":
                raise ManagementError("application_build_not_dispatchable")
            now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            request = _request(binding, claim, now + timedelta(seconds=self.request_lifetime_seconds))
            saved.pool_request_json = request.model_dump(mode="json")
            saved.pool_request_sha256 = canonical_digest(saved.pool_request_json).removeprefix("sha256:")
            await session.flush()
            return request  # Context commits before the caller receives the request.
