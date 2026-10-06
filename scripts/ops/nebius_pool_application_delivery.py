"""Derive first-pool application settings without accepting replacement authority.

The protected parent must still qualify the completed predecessor and publication,
deliver credentials, and stage the exact generated resources before activation.
This pure projection performs no Kubernetes, storage or database operation.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh

from loom.application_image_build import ApplicationImageBuildBindingV1
from loom.application_source_upload import ApplicationSourceUploadBindingV1
from loom_service.application_management.build_deployment import (
    SOURCE_CREDENTIALS_PATH,
    SOURCE_SPOOL_PATH,
    render_application_build_reader,
)
from loom_service.application_management.installation import (
    ApplicationBuildSettings,
    ApplicationSourceUploadSettings,
)
from loom_service.environment_management.deployment import ManagementDeployment, render_management
from loom_service.pool_management.installation import PoolInstallation


class ApplicationSourceCredentialPin(BaseModel):
    """Protected shared source identity; never the credential itself."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    uid: UUID
    resource_version: str = Field(min_length=1, max_length=160)
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def non_nil_identity(self) -> ApplicationSourceCredentialPin:
        if not self.uid.int:
            raise ValueError('nil source credential identity')
        return self


@dataclass(frozen=True, repr=False)
class ApplicationBuildDeliveryRequest:
    before: ManagementDeployment
    profile: dict[str, Any]
    repo_root: Path
    source_credential: ApplicationSourceCredentialPin
    source_delivery_version: Literal['v1', 'v2'] = 'v2'


def source_material_json(material: dict[str, str], pin: ApplicationSourceCredentialPin) -> str:
    """Same bounded source-only payload for live qualification and fixed delivery."""
    try:
        pin = ApplicationSourceCredentialPin.model_validate(pin.model_dump())
        if (set(material) != {'access-key', 'secret-key'} or any(
                not isinstance(value, str) or not 1 <= len(value) <= 4096 or not value.isascii()
                or any(ord(char) < 33 or ord(char) == 127 for char in value) for value in material.values())):
            raise ValueError
        payload = json.dumps(material, sort_keys=True, separators=(',', ':'))
        if hashlib.sha256(payload.encode()).hexdigest() != pin.sha256:
            raise ValueError
        return payload
    except Exception:
        raise ValueError('pool_application_source_unqualified') from None


def qualify_application_source_material(*, before: ManagementDeployment, controller: dict[str, Any],
        secret: dict[str, Any], pin: ApplicationSourceCredentialPin) -> dict[str, str]:
    """Qualify the installed shared source credential, not data/operator material.

    The protected caller must read this exact Secret and qualify the retained
    control-plane UID/template against current cutover ancestry before any write.
    """
    try:
        before = ManagementDeployment.model_validate(before.model_dump())
        pin = ApplicationSourceCredentialPin.model_validate(pin.model_dump())
        application = before.installation.applications
        if application is None:
            raise ValueError
        namespace = application.shared.platform_namespace
        config = before.installation.foundation.platform_config
        active = _snapshot(controller)
        _uid(controller)
        container, = active['spec']['template']['spec']['containers']
        env = {row['name']: row for row in container['env']}
        if (active['apiVersion'] != 'apps/v1' or active['kind'] != 'Deployment'
                or active['metadata']['namespace'] != namespace or active['metadata']['name'] != 'loom-control-plane'
                or container['name'] != 'loom-control-plane' or len(env) != len(container['env'])):
            raise ValueError
        prefix = 'LOOM_CP_SERVICE_EXECUTION_SOURCE_'
        for suffix, value in (('ENDPOINT', config['storage_endpoint']), ('REGION', config['region']),
                ('BUCKET', config['buckets']['source'])):
            if env[prefix + suffix] != {'name': prefix + suffix, 'value': value}:
                raise ValueError
        for suffix, key in (('ACCESS_KEY', 'source-access-key'), ('SECRET_KEY', 'source-secret-key')):
            if env[prefix + suffix] != {'name': prefix + suffix, 'valueFrom': {
                    'secretKeyRef': {'name': 'loom-platform-storage', 'key': key}}}:
                raise ValueError
        _snapshot(secret)
        if (secret.get('apiVersion') != 'v1' or secret.get('kind') != 'Secret'
                or secret.get('type') != 'Opaque' or secret.get('stringData')
                or (secret['metadata']['namespace'], secret['metadata']['name'],
                    _uid(secret), secret['metadata']['resourceVersion']) !=
                    (namespace, 'loom-platform-storage', str(pin.uid), pin.resource_version)):
            raise ValueError
        material = {}
        for key in ('access-key', 'secret-key'):
            encoded = secret['data']['source-' + key]
            if not isinstance(encoded, str) or not 0 < len(encoded) <= 4 * ((4096 + 2) // 3):
                raise ValueError
            material[key] = base64.b64decode(encoded, validate=True).decode('ascii')
        source_material_json(material, pin)
        return material
    except Exception:
        raise ValueError('pool_application_source_unqualified') from None


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


@dataclass(frozen=True, repr=False)
class RenderedApplicationBuildDelivery:
    deployment: dict[str, Any]
    configuration: tuple[dict[str, Any], ...]
    source_secret_name: str


def render_application_build_delivery(*, before: ManagementDeployment, pool: PoolInstallation,
        active: dict[str, Any], candidate: dict[str, Any], profile: dict[str, Any],
        repo_root: Path, source_delivery_version: Literal['v1', 'v2'] = 'v2') -> RenderedApplicationBuildDelivery:
    """Derive fixed first-cutover resources, retaining the old material identity.

    Only the protected parent can supply the completed predecessor and exact
    catalog. This projection grants no permission to stage or start the result.
    """
    try:
        if source_delivery_version == 'v1':
            from scripts.ops.nebius_pool_application_history import render_legacy_source_delivery

            return render_legacy_source_delivery(before=before, pool=pool, active=active,
                candidate=candidate, profile=profile, repo_root=repo_root)
        if source_delivery_version != 'v2':
            raise ValueError
        render_refresh(ManagementRefreshRenderRequest(before, before, active, candidate, profile, repo_root))
        after = derive_application_build_deployment(before, pool)
        rendered = render_management(after, candidate=candidate, profile=profile, repo_root=repo_root)
        deployment, = (copy.deepcopy(row) for row in rendered.files['40-services.yaml'] if row['kind'] == 'Deployment')
        deployment['metadata'] = _snapshot(active)['metadata']
        deployment['spec']['replicas'] = 0
        pod = deployment['spec']['template']['spec']
        old_volumes = {row['name']: row for row in active['spec']['template']['spec']['volumes']}
        for volume in pod['volumes']:
            if volume['name'] in {'management-cloud', 'application-shared'}:
                volume['secret']['secretName'] = old_volumes[volume['name']]['secret']['secretName']
        source, = (row for row in pod['volumes'] if row['name'] == 'application-source-credentials')
        name = 'loom-management-applications-' + rendered.revision[7:19]
        config, = (copy.deepcopy(row) for row in rendered.files['10-config-network.yaml']
            if row['kind'] == 'ConfigMap' and row['metadata']['name'] == name)
        config['data']['installation.json'] = json.dumps(after.installation.model_dump(mode='json'),
            sort_keys=True, separators=(',', ':'))
        application = after.installation.applications
        assert application is not None
        namespace = after.installation.foundation.platform_config['execution_namespace'] + '-build'
        reader = render_application_build_reader(application.authority, namespace=namespace)
        return RenderedApplicationBuildDelivery(deployment, (config, *reader), source['secret']['secretName'])
    except Exception:
        raise ValueError('pool_application_delivery_unqualified') from None
