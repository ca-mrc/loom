"""Protected immutable catalog writes and transactional runtime resolution.

No public route invokes the writer. Its protected caller owns source/release
authorization, installation readiness and commit. Runtime readers requalify the
current participant; historical profiles serve only already-retained requests.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_catalog_schema import (
    NebiusPoolParticipantCatalog,
    NebiusPoolProfileDocument,
)
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom.nebius_pool_contract import PoolParticipantV1
from loom.pipeline.keys import canonical_digest
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.profile_types import PoolProfiles
from loom_service.pool_management.profiles import PoolProfileCatalog


def _digest(value: Any) -> str:
    return canonical_digest(value).removeprefix('sha256:')


def registry_enabled(pool: NebiusPoolBinding) -> bool:
    source = pool.binding_json.get('profile_source')
    if source is None:
        return False
    if source != 'registry-v1' or pool.binding_json.get('observation_binding') != 'physical-v1':
        raise ValueError('pool_profile_registry_unavailable')
    return True


def _validate_catalog(pool: NebiusPoolBinding, participant: PoolParticipantV1, catalog: PoolProfileCatalog) -> None:
    if (participant.pool_id, participant.installation_id, participant.admission_epoch) != (
            pool.pool_id, pool.installation_id, pool.admission_epoch):
        raise ValueError('pool_profile_registry_unavailable')
    profiles = catalog.profiles()
    used: dict[str, set[Any]] = {'execution': set(), 'task_images': set(), 'application_images': set()}
    for target in participant.targets:
        for kind in target.workload_kinds:
            if kind in {'trial', 'verifier'}:
                execution = profiles.execution[target.profile_id]
                runtime, namespace, family = execution.runtime, participant.execution_namespace.name, 'execution'
            elif kind == 'task_image_build':
                build = profiles.task_images[target.profile_id]
                runtime, namespace, family = build.target, participant.build_namespace.name, 'task_images'
            else:
                if participant.environment_class != 'development' or target.workload_kinds != ('application_image_build',):
                    raise ValueError('pool_profile_registry_unavailable')
                application = profiles.application_images[target.profile_id]
                runtime, namespace, family = application.target, participant.build_namespace.name, 'application_images'
            if (runtime.target_id != target.target_id or runtime.namespace != namespace
                    or runtime.node_selector != pool.binding_json['node_selector']):
                raise ValueError('pool_profile_registry_unavailable')
            used[family].add(target.profile_id)
    if any(used[family] != set(getattr(profiles, family)) for family in used):
        raise ValueError('pool_profile_registry_unavailable')


async def store_participant_catalog(session: AsyncSession, participant: PoolParticipantV1,
                                    catalog: PoolProfileCatalog) -> str:
    """Retain one exact registered revision; never replace existing profile bytes."""
    if session.new or session.dirty or session.deleted:
        raise ValueError('pool_profile_registry_requires_clean_transaction')
    participant = PoolParticipantV1.model_validate(participant.model_dump())
    catalog = PoolProfileCatalog.model_validate(catalog.model_dump())
    await acquire_pool_mutation_lock(session)
    pool = await session.get(NebiusPoolBinding, participant.pool_id, with_for_update=True, populate_existing=True)
    selected = await session.get(NebiusPoolParticipant, participant.participant_id,
        with_for_update=True, populate_existing=True)
    if (pool is None or selected is None or not registry_enabled(pool)
            or pool.mode not in {'closed', 'global'} or _digest(pool.binding_json) != pool.binding_sha256
            or selected.binding_json != participant.model_dump(mode='json')
            or _digest(selected.binding_json) != selected.binding_sha256
            or (selected.pool_id, selected.environment_id, selected.incarnation, selected.binding_revision,
                selected.admission_epoch) != (participant.pool_id, participant.environment_id, participant.incarnation,
                    participant.binding_revision, participant.admission_epoch)):
        raise ValueError('pool_profile_registry_unavailable')
    try:
        _validate_catalog(pool, participant, catalog)
    except (ValueError, KeyError, TypeError):
        raise ValueError('pool_profile_registry_unavailable') from None
    for family in ('execution', 'task_images', 'application_images'):
        for profile in getattr(catalog, family):
            document = {'profile': profile.model_dump(mode='json')}
            if family == 'execution':
                document['image_admission_keyring'] = catalog.image_admission_keyring
            sha = _digest(document)
            await session.execute(insert(NebiusPoolProfileDocument).values(pool_id=pool.pool_id,
                profile_id=profile.profile_id, family=family, document_json=document, document_sha256=sha)
                .on_conflict_do_nothing())
            existing = await session.get(NebiusPoolProfileDocument, (pool.pool_id, profile.profile_id, family),
                populate_existing=True)
            if existing is None or existing.document_json != document or existing.document_sha256 != sha:
                raise ValueError('pool_profile_identity_conflict')
    document = {'participant': participant.model_dump(mode='json'), 'profiles': catalog.model_dump(mode='json')}
    sha = _digest(document)
    await session.execute(insert(NebiusPoolParticipantCatalog).values(participant_id=participant.participant_id,
        binding_revision=participant.binding_revision, pool_id=pool.pool_id,
        document_json=document, document_sha256=sha).on_conflict_do_nothing())
    retained = await session.get(NebiusPoolParticipantCatalog, (participant.participant_id, participant.binding_revision),
        populate_existing=True)
    if retained is None or retained.pool_id != pool.pool_id or retained.document_json != document or retained.document_sha256 != sha:
        raise ValueError('pool_participant_catalog_conflict')
    return sha


async def resolve_profiles(session: AsyncSession, pool: NebiusPoolBinding, participant: PoolParticipantV1,
                           revision: int, *, retained: bool, fallback: PoolProfiles | None) -> tuple[PoolParticipantV1, PoolProfiles]:
    """Read under admission's existing pool lock; never use an unqualified cache."""
    if any(isinstance(row, (NebiusPoolParticipantCatalog, NebiusPoolProfileDocument))
           for row in session.new | session.dirty | session.deleted):
        raise ValueError('pool_profile_registry_requires_clean_transaction')
    if not registry_enabled(pool):
        if fallback is None:
            raise ValueError('pool_profiles_unavailable')
        return participant, fallback
    if revision > participant.binding_revision or (not retained and revision != participant.binding_revision):
        raise ValueError('pool_participant_revision_unavailable')
    row = (await session.scalars(select(NebiusPoolParticipantCatalog).where(
        NebiusPoolParticipantCatalog.participant_id == participant.participant_id,
        NebiusPoolParticipantCatalog.binding_revision == revision,
    ).execution_options(populate_existing=True))).one_or_none()
    if row is None or row.pool_id != pool.pool_id or _digest(row.document_json) != row.document_sha256:
        raise ValueError('pool_profiles_unavailable')
    original = PoolParticipantV1.model_validate(row.document_json['participant'])
    if (original.participant_id != participant.participant_id or original.binding_revision != revision
            or original.model_dump(exclude={'binding_revision', 'targets'})
                != participant.model_dump(exclude={'binding_revision', 'targets'})
            or (revision == participant.binding_revision and original != participant)):
        raise ValueError('pool_participant_revision_unavailable')
    catalog = PoolProfileCatalog.model_validate(row.document_json['profiles'])
    _validate_catalog(pool, original, catalog)
    return original, catalog.profiles()
