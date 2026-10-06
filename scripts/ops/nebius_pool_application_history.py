"""Read-only reconstruction of the original source-spool delivery contract.

This projection preserves v1 cutover hashes; it is not a supported new install.
Current rendering and live repair use the canonical, private v2 initializer.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.ops.nebius_pool_application_delivery import (
    RenderedApplicationBuildDelivery,
    derive_application_build_deployment,
    render_application_build_delivery,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.pool_management.installation import PoolInstallation

LEGACY_SOURCE_VOLUME = '/var/run/loom-application-source'
LEGACY_SOURCE_SPOOL = LEGACY_SOURCE_VOLUME + '/spool'
LEGACY_SOURCE_COMMAND = ['python', '-c', 'import os,stat,sys; from pathlib import Path; '
    'p=Path(sys.argv[1]); p.mkdir(mode=0o700,exist_ok=True); s=p.lstat(); '
    'assert p.resolve(strict=True)==p and stat.S_ISDIR(s.st_mode) and '
    's.st_uid==os.getuid() and stat.S_IMODE(s.st_mode)==0o700', LEGACY_SOURCE_SPOOL]


def render_legacy_source_delivery(*, before: ManagementDeployment, pool: PoolInstallation,
        active: dict[str, Any], candidate: dict[str, Any], profile: dict[str, Any],
        repo_root: Path) -> RenderedApplicationBuildDelivery:
    """Reproduce v1 bytes without relaxing current model or mount validation."""
    after = derive_application_build_deployment(before, pool)
    current = render_application_build_delivery(before=before, pool=pool, active=active,
        candidate=candidate, profile=profile, repo_root=repo_root)
    old_deployment = after.model_dump(mode='json')
    old_installation = old_deployment['installation']
    old_installation['applications']['runtime']['source_upload']['spool_directory'] = LEGACY_SOURCE_SPOOL
    revision = digest({'deployment': old_deployment, 'candidate': candidate, 'profile': profile})
    config_name = 'loom-management-applications-' + revision[7:19]
    source_secret = 'loom-applications-source-' + revision[7:19]
    deployment = copy.deepcopy(current.deployment)
    template = deployment['spec']['template']
    template['metadata']['annotations']['loom.nebius/configuration-revision'] = revision
    pod = template['spec']
    for volume in pod['volumes']:
        if volume['name'] == 'management-config':
            volume['configMap']['name'] = config_name
        elif volume['name'] == 'application-source-credentials':
            volume['secret']['secretName'] = source_secret
    initializer, = (row for row in pod['initContainers'] if row['name'] == 'prepare-application-source')
    initializer['command'] = list(LEGACY_SOURCE_COMMAND)
    for container in (*pod['containers'], initializer):
        for mount in container['volumeMounts']:
            if mount['name'] == 'application-source':
                mount['mountPath'] = LEGACY_SOURCE_VOLUME
    configuration = copy.deepcopy(current.configuration)
    config, = (row for row in configuration if row['kind'] == 'ConfigMap')
    config['metadata']['name'] = config_name
    config['data']['installation.json'] = json.dumps(old_installation, sort_keys=True, separators=(',', ':'))
    return RenderedApplicationBuildDelivery(deployment, configuration, source_secret)
