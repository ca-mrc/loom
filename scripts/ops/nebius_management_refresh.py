"""Narrow refresh rendering; predecessor/publication/live proof belongs to entry.

This module confers no installation authority. It preserves the already-qualified
runtime and credentials, refusing a candidate that needs a wider template change.
Historical install and bootstrap-to-applications upgrade renderers stay unchanged.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_stage import _qualified_defaulted

from loom_service.environment_management.deployment import ManagementDeployment, render_management

_REVISION = 'loom.nebius/configuration-revision'
_INSTALLATION = 'loom.nebius/management-installation'


@dataclass(frozen=True, repr=False)
class ManagementRefreshRenderRequest:
    before: ManagementDeployment
    after: ManagementDeployment
    active: dict[str, Any]
    candidate: dict[str, Any]
    profile: dict[str, Any]
    repo_root: Path
    application_catalog: dict[str, Any] | None = None
    application_release: Any = None


@dataclass(frozen=True, repr=False)
class RenderedManagementRefresh:
    config: dict[str, Any]
    deployment: dict[str, Any]
    revision: str
    catalog: dict[str, Any] | None = None


def _application_catalog(request: ManagementRefreshRenderRequest) -> dict[str, Any] | None:
    """Keep peer bytes while staging the exact normal image-only replacement."""
    if request.application_catalog is None and request.application_release is None:
        return None
    from loom_service.pool_management.lifecycle_contracts import PoolParticipantReleaseV2
    from loom_service.pool_management.profiles import PoolProfileCatalog

    release = PoolParticipantReleaseV2.model_validate(request.application_release.model_dump())
    old = request.application_catalog
    _snapshot(old)
    _uid(old)
    if (old['kind'] != 'ConfigMap' or old['metadata']['namespace'] != request.before.namespace
            or old['metadata']['name'] != 'loom-pool-profiles-' + request.before.pool_catalog_operation_id.hex
            or old.get('immutable') is not True or set(old['data']) != {'profiles.json'}):
        raise ValueError('application catalog reference differs')
    catalog = PoolProfileCatalog.model_validate_json(old['data']['profiles.json'])
    before_binding = request.before.installation.applications.runtime.build.binding
    previous, = (row for row in catalog.application_images if row.profile_id == before_binding.profile_id)
    derived = derive_application_builder_image_refresh(request.before, previous_profile=previous,
        release=release, catalog_operation_id=request.after.pool_catalog_operation_id, candidate=request.candidate)
    if derived.installation.applications.runtime.build.binding != request.after.installation.applications.runtime.build.binding:
        raise ValueError('application release binding differs')
    value = catalog.model_dump(mode='json')
    operation = request.after.pool_catalog_operation_id
    for family in ('execution', 'task_images', 'application_images'):
        replacements = {row.profile_id: row.model_dump(mode='json') for row in getattr(release.profiles, family)}
        used = set()
        for index, row in enumerate(value.get(family, [])):
            replacement_id = uuid5(operation, row['profile_id'])
            if replacement_id not in replacements:
                continue
            following = replacements[replacement_id]
            previous_fields, next_fields = copy.deepcopy(row), copy.deepcopy(following)
            previous_fields.pop('profile_id')
            next_fields.pop('profile_id')
            if family == 'execution':
                for key in ('candidate_sha','runtime_image_ref','runtime_binary_sha256'):
                    previous_fields.pop(key)
                    next_fields.pop(key)
            else:
                previous_fields['settings'].pop('service_image')
                next_fields['settings'].pop('service_image')
                if following['settings']['service_image'] != request.candidate['images']['service']['image_ref']:
                    raise ValueError('application release image differs')
                if family == 'application_images':
                    previous_fields['recipe'].pop('trusted_image_ref')
                    next_fields['recipe'].pop('trusted_image_ref')
            if previous_fields != next_fields:
                raise ValueError('application release changes retained profile')
            value[family][index] = following
            used.add(replacement_id)
        if used != set(replacements):
            raise ValueError('application release changes catalog scope')
    selected = PoolProfileCatalog.model_validate(value)
    return {'apiVersion':'v1','kind':'ConfigMap','metadata':{
        'name':'loom-pool-profiles-'+operation.hex,'namespace':request.after.namespace,
        'labels':{'loom.nebius/management-installation':str(request.after.installation_id)}},
        'immutable':True,'data':{'profiles.json':selected.model_dump_json()}}


def platform_disk_type(deployment: ManagementDeployment) -> str:
    """Normalize the canonical reference; cloud capacity remains live-qualified."""
    value = deployment.installation.foundation.platform_config.get('execution_disk_type', 'NETWORK_SSD')
    if value not in {'NETWORK_SSD', 'NETWORK_SSD_NON_REPLICATED'}:
        raise ValueError
    return str(value)


def platform_builder_limit(deployment: ManagementDeployment) -> int | None:
    """Read the ordinary builder's optional limit, never a refresh-owned budget."""
    builder = deployment.installation.foundation.platform_config.get('task_image_builder')
    if builder is None:
        return None
    if not isinstance(builder, dict):
        raise ValueError
    value = builder.get('max_concurrent')
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError
    return value


def platform_references_changed(before: ManagementDeployment, after: ManagementDeployment) -> bool:
    """Execution references require the existing qualified ordinary-rollout proof."""
    old = (platform_disk_type(before), platform_builder_limit(before))
    new = (platform_disk_type(after), platform_builder_limit(after))
    return old != new


def _application_reader_delta(before: ManagementDeployment, after: ManagementDeployment,
                               service_image: str | None) -> bool:
    old_app, new_app = before.installation.applications, after.installation.applications
    old = old_app.runtime.build.binding if old_app is not None and old_app.runtime.build is not None else None
    new = new_app.runtime.build.binding if new_app is not None and new_app.runtime.build is not None else None
    if old is None or new is None or old.recipe.trusted_image_ref == new.recipe.trusted_image_ref:
        return False
    if (service_image is None or new.recipe.trusted_image_ref != service_image
            or after.pool_catalog_operation_id is None or before.pool_catalog_operation_id == after.pool_catalog_operation_id
            or new.profile_id != uuid5(after.pool_catalog_operation_id, str(old.profile_id))
            or new.participant_revision != old.participant_revision + 1):
        raise ValueError('unqualified application reader release')
    return True


def derive_application_builder_image_refresh(before: ManagementDeployment, *, previous_profile: Any,
                                             release: Any, catalog_operation_id: UUID,
                                             candidate: dict[str, Any]) -> ManagementDeployment:
    """Connect an existing normal image-only release to its manager binding.

    The normal owner still qualifies publication/pending release and stages the
    immutable catalog before the existing atomic manager transition. This pure
    projection creates no credential, request, profile or installation authority.
    """
    from loom_service.pool_management.lifecycle_contracts import PoolParticipantReleaseV2

    release = PoolParticipantReleaseV2.model_validate(release.model_dump())
    app = before.installation.applications
    if app is None or app.runtime.build is None:
        raise ValueError('application builder image refresh unqualified')
    binding = app.runtime.build.binding
    participant = release.participant
    target = participant.target(binding.target_id, 'application_image_build')
    selected, = (row for row in release.profiles.application_images if row.profile_id == target.profile_id)
    image = candidate['images']['service']['image_ref']
    if (participant.pool_id != binding.pool_id or participant.participant_id != binding.participant_id
            or participant.installation_id != binding.source.installation_id
            or participant.environment_id != binding.source.data_environment_id
            or participant.admission_epoch != binding.admission_epoch
            or previous_profile.profile_id != binding.profile_id or previous_profile.recipe != binding.recipe
            or previous_profile.target != selected.target
            or previous_profile.runtime_class_overhead != selected.runtime_class_overhead
            or previous_profile.settings.model_dump(exclude={'service_image'}) != selected.settings.model_dump(exclude={'service_image'})
            or previous_profile.recipe.model_dump(exclude={'trusted_image_ref'}) != selected.recipe.model_dump(exclude={'trusted_image_ref'})
            or selected.settings.service_image != image or selected.recipe.trusted_image_ref != image):
        raise ValueError('application builder image refresh unqualified')
    raw = before.model_dump(mode='json')
    raw['pool_catalog_operation_id'] = str(catalog_operation_id)
    next_binding = raw['installation']['applications']['runtime']['build']['binding']
    next_binding.update(profile_id=str(selected.profile_id), participant_revision=participant.binding_revision)
    next_binding['recipe']['trusted_image_ref'] = image
    after = ManagementDeployment.model_validate(raw)
    _configuration(before, after, service_image=image)
    return after


def _configuration(before: ManagementDeployment, after: ManagementDeployment, *, service_image: str | None = None) -> None:
    for deployment in (before, after):
        platform_disk_type(deployment)
        platform_builder_limit(deployment)
    old, new = (ManagementDeployment.model_validate(value.model_dump()).model_dump(mode='json')
        for value in (before, after))
    if _application_reader_delta(before, after, service_image):
        for value in (old, new):
            value.pop('pool_catalog_operation_id')
            binding = value['installation']['applications']['runtime']['build']['binding']
            binding['recipe'].pop('trusted_image_ref')
            binding.pop('profile_id')
            binding.pop('participant_revision')
    previous, current = old['installation'], new['installation']
    old_publications, new_publications = previous.pop('publications'), current.pop('publications')
    if any(publication not in new_publications for publication in old_publications):
        raise ValueError
    applications = [installation['applications'] for installation in (previous, current)]
    if any(application is None for application in applications):
        raise ValueError
    releases = [application.pop('releases') for application in applications]
    prior = {release['release_id']: release for release in releases[0]}
    if (not releases[1] or any(release['release_id'] in prior and prior[release['release_id']] != release
            for release in releases[1])
            or any(release['schema_revision'] != applications[1]['shared']['schema_revision'] for release in releases[1])):
        raise ValueError
    for installation, application in zip((previous, current), applications, strict=True):
        application['shared'].pop('schema_revision')
        application['shared'].pop('runtime_profile_json')
        foundation = installation['foundation']
        config = json.loads(foundation['platform_config_json'])
        # A shared guest target is an observed reference, never refresh-owned
        # infrastructure. The connected preflight must qualify its current value.
        config.pop('guest_execution_target', None)
        config.pop('emulated_auth_execution_target', None)
        # This is a reference to the ordinary rollout's execution disk. Entry
        # binds a change to its protected publication; refresh creates no disk.
        config.pop('execution_disk_type', None)
        # Ordinary rollout owns the builder and its ResourceQuota. Preserve all
        # other builder settings; only its observed concurrency reference varies.
        if isinstance(config.get('task_image_builder'), dict):
            config['task_image_builder'].pop('max_concurrent', None)
        foundation['platform_config_json'] = json.dumps(config, sort_keys=True)
    if old != new:
        raise ValueError


def render_refresh(request: ManagementRefreshRenderRequest) -> RenderedManagementRefresh:
    """Return only a new ConfigMap and an image/config-only retained Deployment.

    `active` must separately be bound to a completed predecessor receipt and its
    live UID/template. Matching this pure output is not a substitute for either.
    """
    try:
        new_image = request.candidate['images']['service']['image_ref']
        reader_delta = _application_reader_delta(request.before, request.after, new_image)
        catalog = _application_catalog(request)
        if catalog is not None and not reader_delta:
            raise ValueError('application catalog requires reader release')
        _configuration(request.before, request.after, service_image=new_image)
        active = _snapshot(request.active)
        _uid(request.active)
        metadata, spec = active['metadata'], active['spec']
        pod = spec['template']['spec']
        if (active['kind'] != 'Deployment' or active['apiVersion'] != 'apps/v1'
                or metadata['name'] != 'loom-service' or metadata['namespace'] != request.before.namespace
                or metadata['labels'].get(_INSTALLATION) != str(request.before.installation_id)
                or type(spec['replicas']) is not int or spec['replicas'] != 1
                or spec['selector'] != {'matchLabels': {'app': 'loom-service'}}
                or pod['serviceAccountName'] != 'loom-application-provisioner' or len(pod['containers']) != 1
                or pod['containers'][0]['name'] != 'loom-service'):
            raise ValueError
        old_image = pod['containers'][0]['image']
        registry = re.escape(request.before.installation.registry_prefix)
        if re.fullmatch(registry + r'/[a-z0-9._/-]+@sha256:[0-9a-f]{64}', old_image) is None:
            raise ValueError
        old_revision = spec['template']['metadata']['annotations'][_REVISION]
        if re.fullmatch(r'sha256:[0-9a-f]{64}', old_revision) is None:
            raise ValueError
        volumes = {row['name']: row for row in pod['volumes']}
        if len(volumes) != len(pod['volumes']):
            raise ValueError
        old_config = volumes['management-config']['configMap']['name']
        if old_config != 'loom-management-applications-' + old_revision[7:19]:
            raise ValueError
        retained_secrets = {}
        for name, prefix in (('management-cloud', 'loom-applications-cloud-'),
                             ('application-shared', 'loom-applications-shared-')):
            secret = volumes[name]['secret']['secretName']
            if re.fullmatch(prefix + r'[0-9a-f]{12}', secret) is None:
                raise ValueError
            retained_secrets[name] = secret
        if len({name[-12:] for name in retained_secrets.values()}) != 1:
            raise ValueError
        application = request.before.installation.applications
        if application is not None and application.runtime.source_upload is not None:
            # First pool cutover introduces source-only material after the
            # original cloud/shared bundles. Preserve its independent revision;
            # image-only refresh neither creates nor rotates credentials.
            secret = volumes['application-source-credentials']['secret']['secretName']
            if re.fullmatch(r'loom-applications-source-[0-9a-f]{12}', secret) is None:
                raise ValueError
            retained_secrets['application-source-credentials'] = secret
        rendered = render_management(request.after, candidate=request.candidate, profile=request.profile,
            repo_root=request.repo_root)
        wanted = copy.deepcopy(next(doc for doc in rendered.files['40-services.yaml'] if doc['kind'] == 'Deployment'))
        config_name = 'loom-management-applications-' + rendered.revision[7:19]
        config = copy.deepcopy(next(doc for doc in rendered.files['10-config-network.yaml']
            if doc['kind'] == 'ConfigMap' and doc['metadata']['name'] == config_name))
        # Private journals canonicalize nested mappings. Reconstructing a
        # completion must not change these opaque ConfigMap JSON bytes merely
        # because Pydantic retained a different insertion order in a dict field.
        config['data']['installation.json'] = json.dumps(request.after.installation.model_dump(mode='json'),
            sort_keys=True, separators=(',', ':'))
        # Normalize only the allowed delta back to the retained runtime. The
        # existing defaulting/security qualifier rejects any other candidate need.
        wanted['metadata'] = copy.deepcopy(metadata)
        wanted['spec']['template']['metadata']['annotations'][_REVISION] = old_revision
        wanted_pod = wanted['spec']['template']['spec']
        wanted_pod['containers'][0]['image'] = old_image
        new_image = request.candidate['images']['service']['image_ref']
        if (any(container['image'] != old_image for container in pod.get('initContainers', []))
                or any(container['image'] != new_image for container in wanted_pod.get('initContainers', []))):
            raise ValueError
        for container in wanted_pod.get('initContainers', []):
            container['image'] = old_image
        for volume in wanted_pod['volumes']:
            if volume['name'] == 'management-config':
                volume['configMap']['name'] = old_config
            elif volume['name'] in retained_secrets:
                volume['secret']['secretName'] = retained_secrets[volume['name']]
            elif volume['name'] == 'pool-profiles' and reader_delta:
                old_catalog = 'loom-pool-profiles-' + request.before.pool_catalog_operation_id.hex
                new_catalog = 'loom-pool-profiles-' + request.after.pool_catalog_operation_id.hex
                if (volumes['pool-profiles']['configMap']['name'] != old_catalog
                        or volume['configMap']['name'] != new_catalog):
                    raise ValueError
                volume['configMap']['name'] = old_catalog
        _qualified_defaulted(wanted, active)
        target = copy.deepcopy(active)
        target['spec']['template']['metadata']['annotations'][_REVISION] = rendered.revision
        target['spec']['template']['spec']['containers'][0]['image'] = new_image
        for container in target['spec']['template']['spec'].get('initContainers', []):
            container['image'] = new_image
        for volume in target['spec']['template']['spec']['volumes']:
            if volume['name'] == 'management-config':
                volume['configMap']['name'] = config_name
            elif volume['name'] == 'pool-profiles' and reader_delta:
                volume['configMap']['name'] = 'loom-pool-profiles-' + request.after.pool_catalog_operation_id.hex
        return RenderedManagementRefresh(config, target, rendered.revision, catalog)
    except Exception:
        raise ValueError('management refresh configuration or retained runtime differs') from None
