"""Derive first-pool application settings without accepting replacement authority.

The protected parent must still qualify the completed predecessor and publication,
deliver credentials, and stage the exact generated resources before activation.
This pure projection performs no Kubernetes, storage or database operation.
"""
from __future__ import annotations

from pathlib import Path

from loom.application_image_build import ApplicationImageBuildBindingV1
from loom.application_source_upload import ApplicationSourceUploadBindingV1
from loom_service.application_management.build_deployment import (
    SOURCE_CREDENTIALS_PATH,
    SOURCE_SPOOL_PATH,
)
from loom_service.application_management.installation import (
    ApplicationBuildSettings,
    ApplicationSourceUploadSettings,
)
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.pool_management.installation import PoolInstallation


def derive_application_build_deployment(before: ManagementDeployment, pool: PoolInstallation) -> ManagementDeployment:
    """Add only source/build delivery rooted in the same shared dev installation."""
    try:
        before = ManagementDeployment.model_validate(before.model_dump())
        pool = PoolInstallation.model_validate(pool.model_dump())
        application = before.installation.applications
        if (application is None or before.pool_catalog_operation_id is not None
                or before.application_builder_machine_id is not None or application.runtime.build is not None):
            raise ValueError
        config = before.installation.foundation.platform_config
        if (pool.installation_id != before.installation_id or pool.cluster_id != application.shared.cluster_id
                or pool.node_group_id != config['execution_node_group_id']):
            raise ValueError
        participant, = (row for row in pool.participants
            if row.environment_class == 'development' and row.environment_id == application.shared.data_environment_id)
        if (participant.execution_namespace.name != config['execution_namespace']
                or participant.build_namespace.name != config['execution_namespace'] + '-build'):
            raise ValueError
        target, = (row for row in participant.targets if row.workload_kinds == ('application_image_build',))
        profile, = (row for row in pool.profiles.application_images if row.profile_id == target.profile_id)
        machine, = (row for row in pool.machines
            if row.participant_id == participant.participant_id and row.workload_scope == 'application_builder')
        if (profile.recipe.schema_revision != application.shared.schema_revision
                or profile.target.node_selector != pool.node_selector
                or (profile.settings.storage_endpoint, profile.settings.storage_region, profile.settings.source_bucket) != (
                    config['storage_endpoint'], config['region'], config['buckets']['source'])):
            raise ValueError
        source = application.runtime.source_upload or ApplicationSourceUploadSettings(
            credentials_file=Path(SOURCE_CREDENTIALS_PATH + '/credentials.json'), spool_directory=Path(SOURCE_SPOOL_PATH))
        build = ApplicationBuildSettings(binding=ApplicationImageBuildBindingV1(
            source=ApplicationSourceUploadBindingV1(installation_id=before.installation_id,
                data_environment_id=application.shared.data_environment_id, cluster_id=pool.cluster_id,
                source_bucket=profile.settings.source_bucket, upload_ttl_seconds=source.upload_ttl_seconds),
            recipe=profile.recipe, storage_endpoint=profile.settings.storage_endpoint,
            storage_region=profile.settings.storage_region, cache_bucket=profile.settings.cache_bucket,
            registry_repository=profile.settings.registry_repository, pool_id=pool.pool_id,
            participant_id=participant.participant_id, profile_id=target.profile_id, target_id=target.target_id,
            admission_epoch=pool.admission_epoch, participant_revision=participant.binding_revision),
            management_origin='https://' + before.public_host, bearer_token_file=Path('/var/run/loom-pool-token/token'))
        value = before.model_dump(mode='json')
        value.update(pool_catalog_operation_id=str(pool.operation_id), application_builder_machine_id=str(machine.machine_id))
        value['installation']['applications']['runtime'].update(
            source_upload=source.model_dump(mode='json'), build=build.model_dump(mode='json'))
        return ManagementDeployment.model_validate(value)
    except Exception:
        raise ValueError('pool_application_delivery_unqualified') from None
