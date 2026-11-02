"""Independent cache soft-state policy; never changes execution authority.

Only protected platform administration writes. Profiles select an already owned
physical cache scope; neither participant input nor a bucket name grants access.
"""
from __future__ import annotations

import json
from dataclasses import replace
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Text, cast, func, literal, select, text
from sqlalchemy.dialects.postgresql import JSONPATH
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_cache_policy_schema import NebiusCacheRuntimePolicy
from loom.db.nebius_pool_catalog_schema import NebiusPoolProfileDocument
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.task_images import PoolTaskImageProfile

_BOUND = 1024
_ACTIVE = ('waiting', 'reserved', 'create_intent', 'observed', 'cleanup_intent')
_FIELDS = ('cache_total_mib', 'cache_gc_policy', 'reuse_task_cache_for_verifier')


class PoolCachePolicyV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.pool-cache-policy.v1'] = 'loom.pool-cache-policy.v1'
    operation_id: UUID
    installation_id: UUID
    pool_id: UUID
    profile_id: UUID
    cache_total_mib: int | None = Field(default=None, ge=1024, le=1048576, strict=True)
    cache_gc_policy: Literal['legacy-seven-day', 'budget-pressure'] | None = None
    reuse_task_cache_for_verifier: bool | None = Field(default=None, strict=True)

    @model_validator(mode='after')
    def identity(self) -> PoolCachePolicyV1:
        if not all(value.int for value in (self.operation_id, self.installation_id, self.pool_id, self.profile_id)):
            raise ValueError('cache_policy_identity_unavailable')
        return self


def cache_scope(value: Any) -> tuple[str, str, str] | None:
    def get(key: str) -> Any:
        return value.get(key) if isinstance(value, dict) else getattr(value, key, None)
    parts = tuple(get(key) for key in ('storage_endpoint', 'storage_region', 'cache_bucket'))
    return parts if all(isinstance(part, str) and part for part in parts) else None


async def effective_cache_configuration(session: AsyncSession, *, installation_id: UUID, settings: Any) -> dict[str, Any]:
    """Caller holds the existing pool mutation lock when freezing a new claim."""
    scope = cache_scope(settings)
    if scope is None:
        return {}
    if not await session.scalar(text("SELECT to_regclass('nebius_cache_runtime_policies') IS NOT NULL")):
        return {}  # Unenabled old schemas retain their historical profile policy.
    row = await session.get(NebiusCacheRuntimePolicy, scope, populate_existing=True)
    if row is None:
        return {}
    if row.installation_id != installation_id:
        raise ValueError('cache_scope_owner_conflict')
    return {field: getattr(row, field) for field in _FIELDS}


async def effective_task_profile(session: AsyncSession, *, installation_id: UUID,
                                 profile: PoolTaskImageProfile) -> PoolTaskImageProfile:
    values = await effective_cache_configuration(session, installation_id=installation_id, settings=profile.settings)
    if not values:
        return profile
    settings = NativeTaskImageSettings.model_validate({**profile.settings.model_dump(), **values})
    return replace(profile, settings=settings)


async def _bounded(session: AsyncSession, query: Any) -> list[Any]:
    rows = list((await session.scalars(query.limit(_BOUND + 1))).all())
    if len(rows) > _BOUND:
        raise ValueError('cache_policy_inventory_limit')
    return rows


def _targets(row: NebiusPoolParticipant) -> set[tuple[UUID, str]]:
    result = set()
    for target in row.binding_json.get('targets', []):
        kinds = target.get('workload_kinds', [])
        for kind, family in (('task_image_build', 'task_images'), ('application_image_build', 'application_images')):
            if kind in kinds:
                result.add((UUID(target['profile_id']), family))
    return result


async def _registered_scope(session: AsyncSession, pool: NebiusPoolBinding, profile_id: UUID,
                            *, retained: bool) -> tuple[NativeTaskImageSettings, list[Any], list[Any]]:
    selected = await _bounded(session, select(NebiusPoolProfileDocument).where(
        NebiusPoolProfileDocument.pool_id == pool.pool_id, NebiusPoolProfileDocument.profile_id == profile_id,
        NebiusPoolProfileDocument.family.in_(('task_images', 'application_images'))))
    if len(selected) != 1:
        # Old file-only catalogs have no DB authority from which an admin may
        # infer a private scope. Their existing owner must register that source.
        raise ValueError('cache_policy_registered_profile_unavailable')
    try:
        settings = NativeTaskImageSettings.model_validate(selected[0].document_json['profile']['settings'])
    except (KeyError, TypeError, ValueError):
        raise ValueError('cache_policy_profile_unavailable') from None
    scope = cache_scope(settings)
    if scope is None:
        raise ValueError('cache_policy_cache_disabled')
    profile = NebiusPoolProfileDocument.document_json['profile']['settings']
    current_profile = select(NebiusPoolParticipant.participant_id).where(
        NebiusPoolParticipant.pool_id == NebiusPoolProfileDocument.pool_id,
        func.jsonb_path_exists(NebiusPoolParticipant.binding_json,
            cast(literal('$.targets[*] ? (@.profile_id == $selected)'), JSONPATH),
            func.jsonb_build_object('selected', cast(NebiusPoolProfileDocument.profile_id, Text)))).exists()
    documents = await _bounded(session, select(NebiusPoolProfileDocument).where(
        NebiusPoolProfileDocument.family.in_(('task_images', 'application_images')),
        profile['storage_endpoint'].astext == scope[0], profile['storage_region'].astext == scope[1],
        profile['cache_bucket'].astext == scope[2], current_profile))
    participants = await _bounded(session, select(NebiusPoolParticipant).where(
        NebiusPoolParticipant.pool_id.in_({row.pool_id for row in documents})))
    current = {(row.pool_id, profile_id, family) for row in participants for profile_id, family in _targets(row)}
    documents = [row for row in documents if (row.pool_id, row.profile_id, row.family) in current]
    qualified = {(row.pool_id, row.profile_id, row.family): row for row in [*selected, *documents]}
    if any(canonical_digest(row.document_json).removeprefix('sha256:') != row.document_sha256 for row in qualified.values()):
        raise ValueError('cache_policy_profile_unavailable')
    if not retained and not any(row.pool_id == pool.pool_id and row.profile_id == profile_id for row in documents):
        raise ValueError('cache_policy_current_profile_required')
    owners = await _bounded(session, select(NebiusPoolBinding).where(
        NebiusPoolBinding.pool_id.in_({row.pool_id for row in documents})))
    if any(row.installation_id != pool.installation_id for row in owners):
        raise ValueError('cache_scope_owner_conflict')
    existing = NebiusApplicationBuild.binding_json
    foreign_application = await session.scalar(select(NebiusApplicationBuild.build_id).where(
        NebiusApplicationBuild.installation_id != pool.installation_id,
        existing['storage_endpoint'].astext == scope[0], existing['storage_region'].astext == scope[1],
        existing['cache_bucket'].astext == scope[2]).limit(1))
    if foreign_application is not None:
        raise ValueError('cache_scope_owner_conflict')
    return settings, documents, participants


def _claim_projection(row: NebiusPoolRequest) -> tuple[dict[str, Any], dict[str, Any] | None]:
    result: dict[str, Any] = dict(request_id=str(row.request_id), workload_kind=row.workload_kind, phase=row.phase,
        projection_status='unfrozen', cache_total_mib=None, cache_gc_policy=None, reuse_task_cache_for_verifier=None)
    if row.plan_json is None:
        return result, None
    result['projection_status'] = 'unknown'
    try:
        raw = row.plan_json['configmap']['data']['claim.json']
        value = json.loads(raw)
        if not isinstance(value, dict):
            return result, None
        total = value.get('cache_total_mib', 4096)
        policy = value.get('cache_gc_policy', 'legacy-seven-day')
        sibling = value.get('reuse_task_cache_for_verifier', False)
        if type(total) is not int or not 1024 <= total <= 1048576 or policy not in ('legacy-seven-day', 'budget-pressure') or type(sibling) is not bool:
            return result, value
        result.update(projection_status='frozen', cache_total_mib=total, cache_gc_policy=policy,
            reuse_task_cache_for_verifier=sibling if row.workload_kind == 'task_image_build' else None)
        return result, value
    except (KeyError, TypeError, ValueError):
        return result, None


async def _active_scope(session: AsyncSession, scope: tuple[str, str, str], documents: list[Any],
                        participants: list[Any]) -> tuple[list[dict[str, Any]], int]:
    refs = {(row.pool_id, row.profile_id, row.family) for row in documents}
    targets = {(row.participant_id, target['target_id']) for row in participants
        for target in row.binding_json.get('targets', [])
        for kind, family in (('task_image_build', 'task_images'), ('application_image_build', 'application_images'))
        if kind in target.get('workload_kinds', []) and (row.pool_id, UUID(target['profile_id']), family) in refs}
    requests = await _bounded(session, select(NebiusPoolRequest).where(
        NebiusPoolRequest.pool_id.in_({row.pool_id for row in documents}),
        NebiusPoolRequest.workload_kind.in_(('task_image_build', 'application_image_build')),
        NebiusPoolRequest.phase.in_(_ACTIVE)))
    projections = []
    for row in requests:
        projection, frozen = _claim_projection(row)
        if frozen is not None:
            matches = cache_scope(frozen) == scope
        elif row.workload_kind == 'application_image_build':
            matches = cache_scope(row.request_json.get('build')) == scope
        else:
            matches = (row.participant_id, row.target_id) in targets
        if matches:
            projections.append(projection)
    claim = NebiusApplicationBuildAttempt.claim_json
    attempts = await _bounded(session, select(NebiusApplicationBuildAttempt).join(NebiusApplicationBuild,
        (NebiusApplicationBuild.build_id == NebiusApplicationBuildAttempt.build_id)
        & (NebiusApplicationBuild.current_attempt == NebiusApplicationBuildAttempt.attempt)).where(
        NebiusApplicationBuildAttempt.phase.in_(('queued', 'running', 'settling')),
        claim['storage_endpoint'].astext == scope[0], claim['storage_region'].astext == scope[1],
        claim['cache_bucket'].astext == scope[2]))
    return projections, len(attempts)


async def cache_policy(session: AsyncSession, request: PoolCachePolicyV1, *, apply: bool,
                       trusted_reader_image: str | None = None) -> dict[str, Any]:
    from loom_service.pool_management.lifecycle_journal import replay, retain_operation

    if apply:
        await acquire_pool_mutation_lock(session)
    document = request.model_dump(mode='json')
    previous = await replay(session, request.operation_id, document) if apply else None
    pool = await session.get(NebiusPoolBinding, request.pool_id, populate_existing=True)
    if pool is None or pool.installation_id != request.installation_id:
        raise ValueError('cache_policy_scope_unavailable')
    settings, documents, participants = await _registered_scope(session, pool, request.profile_id, retained=previous is not None)
    scope = cache_scope(settings)
    assert scope is not None
    values = await effective_cache_configuration(session, installation_id=pool.installation_id, settings=settings)
    current_values = {field: values.get(field, getattr(settings, field)) for field in _FIELDS}
    requested = {field: getattr(request, field) for field in _FIELDS if getattr(request, field) is not None}
    reader_compatible = None if trusted_reader_image is None else all(
        row.document_json['profile']['settings']['service_image'] == trusted_reader_image
        and (row.family != 'application_images'
             or row.document_json['profile']['recipe']['trusted_image_ref'] == trusted_reader_image)
        for row in documents)
    projections, unsettled = await _active_scope(session, scope, documents, participants)
    if apply and previous is None:
        if not requested:
            raise ValueError('cache_policy_inspection_is_read_only')
        if reader_compatible is not True:
            raise ValueError('cache_policy_future_reader_unqualified')
        desired = {**current_values, **requested}
        pressure_transition = desired['cache_gc_policy'] == 'budget-pressure' and (
            current_values['cache_gc_policy'] != 'budget-pressure'
            or desired['cache_total_mib'] != current_values['cache_total_mib'])
        if pressure_transition and (projections or unsettled):
            raise ValueError('cache_policy_build_drain_required')
        row = await session.get(NebiusCacheRuntimePolicy, scope, populate_existing=True)
        if row is None:
            row = NebiusCacheRuntimePolicy(storage_endpoint=scope[0], storage_region=scope[1], cache_bucket=scope[2],
                installation_id=pool.installation_id, **desired)
            session.add(row)
        else:
            for field, value in desired.items():
                setattr(row, field, value)
            row.updated_at = func.clock_timestamp()
        await session.flush()
        values = desired
        current_values = desired
    policies = {tuple(values.get(field, row.document_json['profile']['settings'].get(field, default))
        for field, default in (('cache_total_mib', 4096), ('cache_gc_policy', 'legacy-seven-day'))) for row in documents}
    current = dict(pool_id=str(pool.pool_id), installation_id=str(pool.installation_id), profile_id=str(request.profile_id),
        **current_values, policy_source='management-db' if values else 'profile-default', admission_epoch=pool.admission_epoch,
        policy_revision=pool.policy_revision, registered_scope_profiles=len(documents),
        future_reader_profiles_compatible=reader_compatible,
        registered_scope_pools=len({row.pool_id for row in documents}), registered_scope_owner_consistent=True,
        effective_scope_consistent=len(policies) <= 1, active_build_requests=len(projections),
        unsettled_application_attempts=unsettled, frozen_claims_truncated=len(projections) > 64, frozen_claims=projections[:64])
    receipt = previous or dict(schema_version='loom.pool-cache-policy-receipt.v1', operation_id=str(request.operation_id), **current)
    if apply and previous is None:
        await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=None)
    return dict(receipt=receipt, current=current)
