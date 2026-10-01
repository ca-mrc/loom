"""Qualify registered work origin inside the caller's admission transaction.

This resolves retained application source identity, not deployment readiness or
proof against malicious developer backends with shared SQL access. Trusted
submission/outbox code must stamp and freeze origin before machine submission.
No capacity is granted here, and no network call, flush or commit is performed.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_application_schema import NebiusApplication
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom.nebius_application_contract import ApplicationRegistrationV1, ApplicationReleaseV1
from loom.nebius_pool_contract import PoolParticipantV1, PoolWorkloadKind
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.pipeline.keys import canonical_digest
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine


class PoolOriginError(ValueError):
    def __init__(self) -> None:
        super().__init__("pool_origin_unavailable")


async def qualify_pool_origin(session: AsyncSession, principal: PoolPrincipal, origin: PoolWorkOriginV1, *,
                              target_id: str, workload_kind: PoolWorkloadKind) -> int:
    """Return class for a new request after locked current-authority readback.

    Call after the pool mutation lock, before persisting the immutable request.
    Closed/fenced cleanup must use its separate reconciliation path, not this intake
    qualifier. Historical source generations remain valid after lifecycle changes.
    """
    try:
        with session.no_autoflush:
            if principal.participant_id is None:
                raise ValueError
            current = await authorize_pool_machine(session, principal, role="participant",
                pool_id=principal.pool_id, participant_id=principal.participant_id)
            if current.pool_mode != "global" or current.participant_phase != "active":
                raise ValueError
            row = await session.get(NebiusPoolParticipant, current.participant_id)
            pool = await session.get(NebiusPoolBinding, current.pool_id)
            if row is None or pool is None or canonical_digest(row.binding_json).removeprefix("sha256:") != row.binding_sha256:
                raise ValueError
            binding = PoolParticipantV1.model_validate(row.binding_json)
            if (binding.participant_id, binding.pool_id, binding.installation_id, binding.environment_id,
                    binding.incarnation, binding.binding_revision, binding.admission_epoch) != (
                    current.participant_id, current.pool_id, current.installation_id, current.environment_id,
                    current.incarnation, current.participant_revision, current.participant_epoch):
                raise ValueError
            binding.target(target_id, workload_kind)
            origin = PoolWorkOriginV1.model_validate(origin.model_dump())
            priority = pool_request_priority(binding, origin, workload_kind=workload_kind)
            if origin.kind == "personal_build":
                # No source-build registry/renderer is installed by this package.
                raise ValueError
            if origin.application is None:
                return priority
            if any(isinstance(value, (NebiusApplication, NebiusApplicationOperation))
                    for value in session.new | session.dirty | session.deleted):
                raise ValueError
            recorded = origin.application
            app = (await session.scalars(select(NebiusApplication).where(
                NebiusApplication.application_id == recorded.application_id,
            ).execution_options(populate_existing=True).with_for_update(read=True))).one_or_none()
            operation = (await session.scalars(select(NebiusApplicationOperation).where(
                NebiusApplicationOperation.application_id == recorded.application_id,
                NebiusApplicationOperation.deployment_generation == recorded.deployment_generation,
            ).execution_options(populate_existing=True).with_for_update(read=True))).one_or_none()
            if (app is None or operation is None or app.incarnation != recorded.incarnation
                    or app.data_environment_id != binding.environment_id or app.cluster_id != pool.cluster_id
                    or app.deployment_generation < recorded.deployment_generation
                    or operation.owner_user_id != app.owner_user_id
                    or operation.action not in {"create", "update", "resume"}
                    or operation.plan_json.get("schema_version") != "loom.nebius-application-plan.v1"):
                raise ValueError
            registration = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
            release = ApplicationReleaseV1.model_validate(operation.plan_json["release"])
            if (registration.application_id, registration.incarnation, registration.data_environment_id,
                    registration.cluster_id, registration.owner_user_id, registration.owner_team_id,
                    registration.deployment_generation, registration.release_id, registration.desired_state) != (
                    app.application_id, app.incarnation, app.data_environment_id, app.cluster_id,
                    app.owner_user_id, app.owner_team_id, recorded.deployment_generation, recorded.release_id, "active"):
                raise ValueError
            if release.release_id != recorded.release_id or release.source_digest != recorded.source_digest:
                raise ValueError
            return priority
    except (ValueError, KeyError, TypeError):
        raise PoolOriginError from None
