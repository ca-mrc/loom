"""Qualify registered work origin inside the caller's admission transaction.

This resolves retained application source identity, not deployment readiness or
proof against malicious developer backends with shared SQL access. Trusted
submission/outbox code must stamp and freeze origin before machine submission.
No capacity is granted here, and no network call, flush or commit is performed.
"""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_application_schema import NebiusApplication
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom.nebius_application_contract import ApplicationRegistrationV1, ApplicationReleaseV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolParticipantV1, PoolWorkloadKind
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.pipeline.keys import canonical_digest
from loom_service.pool_management.application_history import qualify_application_build_history
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine


class PoolOriginError(ValueError):
    def __init__(self) -> None:
        super().__init__("pool_origin_unavailable")


class PoolApplicationHistoryV1(BaseModel):
    """Only the retained identity fields needed to qualify original work."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    application_id: UUID
    incarnation: UUID
    data_environment_id: UUID
    cluster_id: str
    owner_user_id: UUID
    owner_team_id: UUID
    deployment_generation: int = Field(gt=0, strict=True)


class PoolOperationHistoryV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    application_id: UUID
    owner_user_id: UUID
    deployment_generation: int = Field(gt=0, strict=True)
    action: Literal["create", "update", "resume"]
    plan_schema: Literal["loom.nebius-application-plan.v1"]
    registration: ApplicationRegistrationV1
    release: ApplicationReleaseV1


def qualify_pool_history(origin: PoolWorkOriginV1, *, participant: PoolParticipantV1, cluster_id: str,
                         workload_kind: PoolWorkloadKind, application: PoolApplicationHistoryV1 | None,
                         operation: PoolOperationHistoryV1 | None) -> int:
    """One validator for locked admission and protected READ ONLY SQL readback.

    These projections must come from the bound management database. Parsing an
    owner-provided projection is not proof of registration or dispatch authority.
    """
    try:
        binding = PoolParticipantV1.model_validate(participant.model_dump())
        origin = PoolWorkOriginV1.model_validate(origin.model_dump())
        priority = pool_request_priority(binding, origin, workload_kind=workload_kind)
        if origin.kind == "personal_build":
            raise ValueError
        if origin.application is None:
            if application is not None or operation is not None:
                raise ValueError
            return priority
        if application is None or operation is None:
            raise ValueError
        app = PoolApplicationHistoryV1.model_validate(application.model_dump())
        op = PoolOperationHistoryV1.model_validate(operation.model_dump())
        recorded = origin.application
        if (app.application_id != recorded.application_id or app.incarnation != recorded.incarnation
                or app.data_environment_id != binding.environment_id or app.cluster_id != cluster_id
                or app.deployment_generation < recorded.deployment_generation
                or op.application_id != app.application_id or op.owner_user_id != app.owner_user_id
                or op.deployment_generation != recorded.deployment_generation):
            raise ValueError
        registration, release = op.registration, op.release
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


async def qualify_pool_origin(session: AsyncSession, principal: PoolPrincipal, origin: PoolWorkOriginV1, *,
                              target_id: str, workload_kind: PoolWorkloadKind,
                              application_build: PoolApplicationImagePrepareV1 | None = None) -> int:
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
                pool_id=principal.pool_id, participant_id=principal.participant_id, workload_kind=workload_kind)
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
            return await qualify_retained_pool_origin(session, origin, participant=binding,
                cluster_id=pool.cluster_id, workload_kind=workload_kind, lock_history=True,
                application_build=application_build)
    except (ValueError, KeyError, TypeError):
        raise PoolOriginError from None


async def qualify_retained_pool_origin(session: AsyncSession, origin: PoolWorkOriginV1, *,
                                      participant: PoolParticipantV1, cluster_id: str,
                                      workload_kind: PoolWorkloadKind, lock_history: bool = False,
                                      application_build: PoolApplicationImagePrepareV1 | None = None) -> int:
    """Read source history without authenticating a machine or opening admission.

    The protected cutover uses its explicitly bound management DB and a READ ONLY
    transaction. Normal admission still requires current machine authority and
    global/active mode above, and retains row locks on the source history. This
    helper is not a route, grant, readiness check or permission to run work.
    """
    try:
        with session.no_autoflush:
            binding = PoolParticipantV1.model_validate(participant.model_dump())
            origin = PoolWorkOriginV1.model_validate(origin.model_dump())
            priority = pool_request_priority(binding, origin, workload_kind=workload_kind)
            if origin.kind == "personal_build":
                await qualify_application_build_history(session, origin, participant=binding,
                    cluster_id=cluster_id, request=application_build, lock_history=lock_history)
                return priority
            if origin.application is None:
                return priority
            if any(isinstance(value, (NebiusApplication, NebiusApplicationOperation))
                    for value in session.new | session.dirty | session.deleted):
                raise ValueError
            recorded = origin.application
            app_query = select(NebiusApplication).where(
                NebiusApplication.application_id == recorded.application_id,
            ).execution_options(populate_existing=True)
            operation_query = select(NebiusApplicationOperation).where(
                NebiusApplicationOperation.application_id == recorded.application_id,
                NebiusApplicationOperation.deployment_generation == recorded.deployment_generation,
            ).execution_options(populate_existing=True)
            if lock_history:
                app_query, operation_query = app_query.with_for_update(read=True), operation_query.with_for_update(read=True)
            app = (await session.scalars(app_query)).one_or_none()
            operation = (await session.scalars(operation_query)).one_or_none()
            return qualify_pool_history(origin, participant=binding, cluster_id=cluster_id, workload_kind=workload_kind,
                application=PoolApplicationHistoryV1.model_validate(app, from_attributes=True) if app is not None else None,
                operation=PoolOperationHistoryV1.model_validate({"application_id": operation.application_id,
                    "owner_user_id": operation.owner_user_id, "deployment_generation": operation.deployment_generation,
                    "action": operation.action, "plan_schema": operation.plan_json.get("schema_version"),
                    "registration": operation.plan_json.get("registration"), "release": operation.plan_json.get("release")})
                    if operation is not None else None)
    except (ValueError, KeyError, TypeError):
        raise PoolOriginError from None
