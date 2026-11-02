"""Resolve an App builder from current or committed pending release authority."""
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.application_image_build import ApplicationImageBuildBindingV1
from loom.db.nebius_pool_lifecycle_schema import (
    NebiusPoolLifecycleOperation,
    NebiusPoolParticipantLifecycle,
)
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.profile_registry import registry_enabled, resolve_profiles
from loom_service.pool_management.profile_types import PoolProfiles


async def builder_dispatch_allowed(session: AsyncSession, binding: ApplicationImageBuildBindingV1) -> bool:
    """A pool-first caller can freeze/claim work only after normal activation."""
    exists = await session.scalar(select(NebiusPoolBinding.pool_id).where(NebiusPoolBinding.pool_id == binding.pool_id))
    if exists is None:
        return True  # Existing standalone registry fixtures; runtime startup still requires a pool.
    row = await session.get(NebiusPoolParticipant, binding.participant_id, populate_existing=True)
    if (row is None or row.phase != 'active' or row.pool_id != binding.pool_id
            or row.binding_revision != binding.participant_revision or row.admission_epoch != binding.admission_epoch):
        return False
    participant = PoolParticipantV1.model_validate(row.binding_json)
    if await session.scalar(text("SELECT to_regclass('nebius_pool_participant_lifecycle') IS NOT NULL")):
        head = await session.get(NebiusPoolParticipantLifecycle, row.participant_id, populate_existing=True)
        if head is not None and head.state != 'active':
            return False
    return any(target.target_id == binding.target_id and target.profile_id == binding.profile_id
        and 'application_image_build' in target.workload_kinds for target in participant.targets)


async def builder_recovery_allowed(session: AsyncSession, binding: ApplicationImageBuildBindingV1,
                                    build_id: UUID, attempt: int) -> bool:
    """Keep exact committed cleanup/cancellation live while new intake pauses."""
    from loom.db.nebius_application_build_schema import (
        NebiusApplicationBuild,
        NebiusApplicationBuildAttempt,
    )
    from loom.db.nebius_pool_schema import NebiusPoolRequest
    from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
    from loom.nebius_pool_contract import PoolActivationV1

    row = await session.get(NebiusApplicationBuild, build_id, populate_existing=True)
    saved = await session.get(NebiusApplicationBuildAttempt, (build_id, attempt), populate_existing=True)
    if row is None or saved is None or row.current_attempt != attempt:
        return False
    try:
        if ApplicationImageBuildBindingV1.model_validate(row.binding_json) != binding:
            return False
        if row.desired_state == 'cancelled':
            return True
        if saved.pool_request_json is None:
            return False
        request = PoolApplicationImagePrepareV1.model_validate(saved.pool_request_json)
        now = await session.scalar(select(func.clock_timestamp()))
        assert now is not None
        if request.deadline_at <= now:
            return True
        if saved.activated_json is None and saved.activation_json is not None:
            if PoolActivationV1.model_validate(saved.activation_json).not_after <= now:
                return True
        retained = await session.scalar(select(NebiusPoolRequest).where(
            NebiusPoolRequest.pool_id == binding.pool_id, NebiusPoolRequest.participant_id == binding.participant_id,
            NebiusPoolRequest.workload_kind == 'application_image_build',
            NebiusPoolRequest.local_work_id == build_id, NebiusPoolRequest.generation == attempt))
        return (retained is not None and retained.request_json == saved.pool_request_json
            and retained.request_sha256 == saved.pool_request_sha256
            # Reserved is already charged. Ordinary suspension preserves its
            # existing activation authority; a frozen-only/waiting request has
            # acquired no such commitment and may not advance while inactive.
            and (retained.phase == 'reserved' or (retained.plan_json is not None
                and retained.phase in ('create_intent','observed','cleanup_intent','released'))))
    except (ValueError, TypeError, KeyError):
        return False


async def qualified_builder_profiles(session: AsyncSession, binding: ApplicationImageBuildBindingV1,
                                     mounted: PoolProfiles) -> tuple[PoolParticipantV1, PoolProfiles]:
    from loom_service.pool_management.lifecycle_contracts import PoolParticipantReleaseV2
    from loom_service.pool_management.release_compatibility import qualify_release

    pool = await session.get(NebiusPoolBinding, binding.pool_id, populate_existing=True)
    row = await session.get(NebiusPoolParticipant, binding.participant_id, populate_existing=True)
    if pool is None or row is None or row.pool_id != pool.pool_id:
        raise ValueError('unbound builder catalog')
    current = PoolParticipantV1.model_validate(row.binding_json)
    if not registry_enabled(pool):
        if pool.binding_json.get('profile_catalog_sha256') != mounted.catalog_sha256:
            raise ValueError('unbound builder catalog')
        return current, mounted
    if binding.participant_revision == current.binding_revision:
        return await resolve_profiles(session, pool, current, binding.participant_revision, retained=False, fallback=mounted)
    head = await session.get(NebiusPoolParticipantLifecycle, row.participant_id, populate_existing=True)
    operation = await session.get(NebiusPoolLifecycleOperation, head.head_operation_id, populate_existing=True) if head else None
    if (head is None or head.state != 'suspended' or operation is None
            or operation.request_json.get('action') != 'begin_release'):
        raise ValueError('unbound pending builder release')
    release = PoolParticipantReleaseV2.model_validate(operation.request_json['replacement'])
    await qualify_release(session, pool, current, release)
    if release.participant.binding_revision != binding.participant_revision:
        raise ValueError('unbound pending builder revision')
    return release.participant, release.profiles.profiles()
