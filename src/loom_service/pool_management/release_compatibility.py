"""Bounded image-only releases preserve retained requests and access bindings.

Namespace, credential, architecture and protocol migrations require a separate
protected binding operation, not a release that silently drops old capabilities.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_catalog_schema import (
    NebiusPoolParticipantCatalog,
    NebiusPoolProfileDocument,
)
from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.lifecycle_contracts import PoolParticipantReleaseV2
from loom_service.pool_management.profile_registry import _validate_catalog
from loom_service.pool_management.profiles import PoolProfileCatalog


async def qualify_release(session: AsyncSession, pool: NebiusPoolBinding, current: PoolParticipantV1,
                          release: PoolParticipantReleaseV2) -> None:
    following = release.participant
    if (current.model_dump(exclude={'binding_revision', 'targets'}) != following.model_dump(exclude={'binding_revision', 'targets'})
            or following.binding_revision != current.binding_revision + 1
            or len(following.targets) != len(current.targets)):
        raise ValueError('pool_release_incompatible_binding')
    replacements = {}
    for before, after in zip(current.targets, following.targets, strict=True):
        if before.model_dump(exclude={'profile_id'}) != after.model_dump(exclude={'profile_id'}):
            raise ValueError('pool_release_incompatible_target')
        replacements[before.profile_id] = after.profile_id
    if len(set(replacements.values())) != len(replacements) or set(replacements) & set(replacements.values()):
        raise ValueError('pool_release_incompatible_profile_identity')
    if await session.scalar(select(NebiusPoolProfileDocument.profile_id).where(
            NebiusPoolProfileDocument.pool_id == pool.pool_id,
            NebiusPoolProfileDocument.profile_id.in_(replacements.values())).limit(1)) is not None:
        raise ValueError('pool_release_incompatible_profile_reuse')
    retained = await session.get(NebiusPoolParticipantCatalog, (current.participant_id, current.binding_revision))
    if retained is None or retained.pool_id != pool.pool_id or digest(retained.document_json) != retained.document_sha256:
        raise ValueError('pool_release_incompatible_catalog')
    original = PoolProfileCatalog.model_validate(retained.document_json['profiles'])
    _validate_catalog(pool, following, release.profiles)
    if original.image_admission_keyring != release.profiles.image_admission_keyring:
        raise ValueError('pool_release_incompatible_trust')
    for family in ('execution', 'task_images', 'application_images'):
        before_profiles, after_profiles = getattr(original, family), getattr(release.profiles, family)
        after_by_id = {row.profile_id: row for row in after_profiles}
        if len(before_profiles) != len(after_profiles):
            raise ValueError('pool_release_incompatible_profile_family')
        for before_profile in before_profiles:
            after_profile = after_by_id.get(replacements[before_profile.profile_id])
            if after_profile is None:
                raise ValueError('pool_release_incompatible_profile_family')
            old, new = before_profile.model_dump(mode='json'), after_profile.model_dump(mode='json')
            old.pop('profile_id')
            new.pop('profile_id')
            if family == 'execution':
                for key in ('candidate_sha', 'runtime_image_ref', 'runtime_binary_sha256'):
                    old.pop(key)
                    new.pop(key)
            else:
                old['settings'].pop('service_image')
                new['settings'].pop('service_image')
                if family == 'application_images':
                    old['recipe'].pop('trusted_image_ref')
                    new['recipe'].pop('trusted_image_ref')
            if old != new:
                raise ValueError('pool_release_incompatible_retained_contract')
