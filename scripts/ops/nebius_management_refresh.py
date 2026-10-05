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


@dataclass(frozen=True, repr=False)
class RenderedManagementRefresh:
    config: dict[str, Any]
    deployment: dict[str, Any]
    revision: str


def _configuration(before: ManagementDeployment, after: ManagementDeployment) -> None:
    old, new = (ManagementDeployment.model_validate(value.model_dump()).model_dump(mode='json')
        for value in (before, after))
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
        foundation['platform_config_json'] = json.dumps(config, sort_keys=True)
    if old != new:
        raise ValueError


def render_refresh(request: ManagementRefreshRenderRequest) -> RenderedManagementRefresh:
    """Return only a new ConfigMap and an image/config-only retained Deployment.

    `active` must separately be bound to a completed predecessor receipt and its
    live UID/template. Matching this pure output is not a substitute for either.
    """
    try:
        _configuration(request.before, request.after)
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
        _qualified_defaulted(wanted, active)
        target = copy.deepcopy(active)
        target['spec']['template']['metadata']['annotations'][_REVISION] = rendered.revision
        target['spec']['template']['spec']['containers'][0]['image'] = new_image
        for container in target['spec']['template']['spec'].get('initContainers', []):
            container['image'] = new_image
        for volume in target['spec']['template']['spec']['volumes']:
            if volume['name'] == 'management-config':
                volume['configMap']['name'] = config_name
        return RenderedManagementRefresh(config, target, rendered.revision)
    except Exception:
        raise ValueError('management refresh configuration or retained runtime differs') from None
