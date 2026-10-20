"""Closed-development schema/catalog projection; no migration or write authority.

The connected parent resolves both protected publications, verifies the retained
runtime and stops all writers. Ordinary image release and global refresh retain
their narrower contracts. Original installer objects and receipts stay immutable.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication
from scripts.ops.nebius_development_runtime_retained import CompletedDevelopmentRuntime
from scripts.ops.nebius_environment_release_contracts import PUBLICATION_FIELDS

from loom.nebius_application_contract import ApplicationReleaseV1
from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_pool_contract import PoolParticipantV1
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.pool_management.installation import PoolInstallation


def _profile(publication: DevelopmentRuntimePublication) -> ServiceExecutionRuntimeProfileV1:
    candidate = publication.bundle.candidate
    profile = ServiceExecutionRuntimeProfileV1.model_validate(publication.bundle.profile)
    if (profile.candidate_sha != candidate['candidate_sha']
            or any(getattr(profile, field) != candidate['images'][role]['image_ref'] for field, role in (
                ('task_image_ref', 'service'), ('runtime_image_ref', 'execution_runtime'), ('agent_image_ref', 'harbor_runtime')))):
        raise ValueError('development_schema_publication_profile_changed')
    return profile


def _pool_successor(*, before: PoolInstallation, participant_id: UUID, operation_id: UUID,
        source_schema_revision: str, target_schema_revision: str,
        source_profile: ServiceExecutionRuntimeProfileV1,
        runtime_profile: ServiceExecutionRuntimeProfileV1) -> PoolInstallation:
    selected, = (row for row in before.participants if row.participant_id == participant_id)
    if (selected.environment_class != 'development' or not operation_id.int or operation_id == before.operation_id
            or source_schema_revision == target_schema_revision
            or source_profile.candidate_sha == runtime_profile.candidate_sha
            or source_profile.model_dump(exclude=PUBLICATION_FIELDS) != runtime_profile.model_dump(exclude=PUBLICATION_FIELDS)):
        raise ValueError('development_schema_catalog_scope_changed')
    mapping = {row.profile_id: uuid5(operation_id, str(row.profile_id)) for row in selected.targets}
    prior_ids = {row.profile_id for family in ('execution', 'task_images', 'application_images')
        for row in getattr(before.profiles, family)}
    if len(mapping) != len(selected.targets) or set(mapping.values()) & prior_ids:
        raise ValueError('development_schema_profile_identity_changed')
    data = before.model_dump(mode='json')
    data.update(operation_id=str(operation_id), policy_revision=before.policy_revision + 1)
    for participant in data['participants']:
        if participant['participant_id'] == str(participant_id):
            participant['binding_revision'] += 1
            for target in participant['targets']:
                target['profile_id'] = str(mapping[UUID(target['profile_id'])])
    applications = 0
    for family in ('execution', 'task_images', 'application_images'):
        for row in data['profiles'].get(family, []):
            old_id = UUID(row['profile_id'])
            if old_id not in mapping:
                continue
            row['profile_id'] = str(mapping[old_id])
            if family == 'execution':
                fields = ('candidate_sha', 'runtime_image_ref', 'runtime_binary_sha256')
                if any(row[field] != getattr(source_profile, field) for field in fields):
                    raise ValueError('development_schema_execution_predecessor_changed')
                row.update({field: getattr(runtime_profile, field) for field in fields})
            else:
                if row['settings']['service_image'] != source_profile.task_image_ref:
                    raise ValueError('development_schema_builder_predecessor_changed')
                row['settings']['service_image'] = runtime_profile.task_image_ref
                if family == 'application_images':
                    applications += 1
                    if (row['recipe']['schema_revision'] != source_schema_revision
                            or row['recipe']['trusted_image_ref'] != source_profile.task_image_ref):
                        raise ValueError('development_schema_recipe_predecessor_changed')
                    row['recipe'].update(schema_revision=target_schema_revision,
                        trusted_image_ref=runtime_profile.task_image_ref)
    if applications != 1:
        raise ValueError('development_schema_requires_configured_application_builder')
    return PoolInstallation.model_validate(data)


class ClosedDevelopmentCatalogRefresh(BaseModel):
    """Exact catalog delta; publication approval and SQL quiescence are external."""
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.closed-development-schema-catalog.v1'] = 'loom.closed-development-schema-catalog.v1'
    participant_id: UUID
    before: PoolInstallation
    after: PoolInstallation
    source_schema_revision: str = Field(pattern=r'^[A-Za-z0-9_]{1,64}$')
    target_schema_revision: str = Field(pattern=r'^[A-Za-z0-9_]{1,64}$')
    manager_schema_revision: str = Field(pattern=r'^[A-Za-z0-9_]{1,64}$')
    source_profile: ServiceExecutionRuntimeProfileV1
    runtime_profile: ServiceExecutionRuntimeProfileV1

    @model_validator(mode='after')
    def exact_successor(self) -> ClosedDevelopmentCatalogRefresh:
        if self.after != _pool_successor(before=self.before, participant_id=self.participant_id,
                operation_id=self.after.operation_id, source_schema_revision=self.source_schema_revision,
                target_schema_revision=self.target_schema_revision, source_profile=self.source_profile,
                runtime_profile=self.runtime_profile):
            raise ValueError('development_schema_catalog_successor_changed')
        return self


@dataclass(frozen=True, repr=False)
class DevelopmentSchemaProjection:
    before_pool: PoolInstallation
    after_pool: PoolInstallation
    before_manager: ManagementDeployment
    after_manager: ManagementDeployment
    participant: PoolParticipantV1
    data_publication: DevelopmentRuntimePublication
    manager_publication: DevelopmentRuntimePublication
    catalog_request: ClosedDevelopmentCatalogRefresh


def project_development_schema_successor(*, runtime: CompletedDevelopmentRuntime, operation_id: UUID,
        data_publication: DevelopmentRuntimePublication,
        manager_publication: DevelopmentRuntimePublication) -> DevelopmentSchemaProjection:
    """Derive configured builder, personal releases and pool references together.

    This consumes an already-qualified completed runtime. It never re-renders the
    installer, reads live state, changes historical releases, or grants approval.
    """
    try:
        database = copy.deepcopy(runtime.request.database)
        before_pool = PoolInstallation.model_validate(database.manager.retained.request.registration.spec.model_dump())
        before_manager = ManagementDeployment.model_validate(database.manager.deployment.model_dump())
        prefix = before_manager.installation.registry_prefix
        previous_data = database.data_runtime.validate(prefix)
        previous_manager = database.manager.publication.validate(prefix)
        data, manager = (value.validate(prefix) for value in (data_publication, manager_publication))
        source_profile, target_profile = _profile(previous_data), _profile(data)
        if _profile(previous_manager).model_dump(exclude=PUBLICATION_FIELDS) != _profile(manager).model_dump(exclude=PUBLICATION_FIELDS):
            raise ValueError('development_schema_manager_protocol_changed')
        old_schema, new_schema = (candidate_service_schema_head(value.bundle.candidate) for value in (previous_data, data))
        application = before_manager.installation.applications
        selected, = (row for row in before_pool.participants if row.environment_class == 'development')
        if (application is None or application.runtime.build is None
                or before_manager.namespace != 'loom-nebius-management-dev'
                or application.shared.platform_namespace != 'loom-dev'
                or application.shared.schema_revision != old_schema
                or application.shared.data_environment_id != selected.environment_id
                or before_manager.pool_catalog_operation_id != before_pool.operation_id):
            raise ValueError('development_schema_manager_predecessor_changed')
        build = application.runtime.build.binding
        target, = (row for row in selected.targets if row.workload_kinds == ('application_image_build',))
        profile, = (row for row in before_pool.profiles.application_images if row.profile_id == target.profile_id)
        machine, = (row for row in before_pool.machines
            if row.participant_id == selected.participant_id and row.workload_scope == 'application_builder')
        if ((build.pool_id, build.participant_id, build.profile_id, build.target_id, build.admission_epoch,
                build.participant_revision, build.recipe, before_manager.application_builder_machine_id) != (
                before_pool.pool_id, selected.participant_id, target.profile_id, target.target_id,
                before_pool.admission_epoch, selected.binding_revision, profile.recipe, machine.machine_id)
                or any(getattr(build, key) != getattr(profile.settings, key) for key in (
                    'storage_endpoint', 'storage_region', 'cache_bucket', 'registry_repository'))
                or build.source.source_bucket != profile.settings.source_bucket):
            raise ValueError('development_schema_application_builder_changed')
        after_pool = _pool_successor(before=before_pool, participant_id=selected.participant_id,
            operation_id=operation_id, source_schema_revision=old_schema, target_schema_revision=new_schema,
            source_profile=source_profile, runtime_profile=target_profile)
        participant, = (row for row in after_pool.participants if row.participant_id == selected.participant_id)
        next_target, = (row for row in participant.targets if row.workload_kinds == ('application_image_build',))
        next_build, = (row for row in after_pool.profiles.application_images if row.profile_id == next_target.profile_id)
        config = before_manager.model_dump(mode='json')
        config['pool_catalog_operation_id'] = str(operation_id)
        app = config['installation']['applications']
        app['shared'].update(schema_revision=new_schema, runtime_profile_json=target_profile.model_dump_json())
        app['runtime']['build']['binding'].update(profile_id=str(next_build.profile_id),
            participant_revision=participant.binding_revision, recipe=next_build.recipe.model_dump(mode='json'))
        images = data.bundle.candidate['images']
        release = ApplicationReleaseV1(release_id=uuid5(operation_id, 'application-release'),
            source_digest=data.bundle.candidate['source_archive_sha256'], schema_revision=new_schema,
            service_image_ref=images['service']['image_ref'], web_image_ref=images['web']['image_ref'])
        if release.release_id in {row.release_id for row in application.releases}:
            raise ValueError('development_schema_application_release_reused')
        app['releases'] = [release.model_dump(mode='json')]
        publications = {row.candidate_id: row for row in before_manager.installation.publications}
        for publication in (data.publication, manager.publication):
            if publication.candidate_id in publications and publications[publication.candidate_id] != publication:
                raise ValueError('development_schema_publication_identity_changed')
            publications[publication.candidate_id] = publication
        config['installation']['publications'] = [row.model_dump(mode='json') for row in publications.values()]
        after_manager = ManagementDeployment.model_validate(config)
        catalog = ClosedDevelopmentCatalogRefresh(participant_id=selected.participant_id, before=before_pool, after=after_pool,
            source_schema_revision=old_schema, target_schema_revision=new_schema,
            manager_schema_revision=candidate_service_schema_head(manager.bundle.candidate),
            source_profile=source_profile, runtime_profile=target_profile)
        return DevelopmentSchemaProjection(before_pool, after_pool, before_manager, after_manager,
            participant, data, manager, catalog)
    except Exception:
        raise ValueError('development_schema_projection_unqualified') from None
