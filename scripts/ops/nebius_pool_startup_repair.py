"""Fixed pre-opening source-spool recovery, preserving original cutover ancestry.

This module is internal to the protected continuation. It is not an arbitrary
manifest update or permission to open admission without runtime qualification.
"""
from __future__ import annotations

import copy
from dataclasses import replace
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _qualified_defaulted
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, cutover_documents


def source_repair_documents(request: PoolCutoverRequest, original: dict[str, Any]
                            ) -> tuple[dict[str, Any], dict[str, Any]]:
    """Derive the one supported v1→v2 delta, retaining server defaults and Secrets."""
    try:
        delivery = request.application_delivery
        if delivery is None or delivery.source_delivery_version != 'v1' or _uid(original) != _uid(request.manager):
            raise ValueError
        key = _key(request.manager)
        old = cutover_documents(request)['runtime'][key]
        old['spec']['replicas'] = 1
        _qualified_defaulted(old, original)
        current = cutover_documents(replace(request,
            application_delivery=replace(delivery, source_delivery_version='v2')))
        new = current['runtime'][key]
        config, = (row for row in current['configuration'] if row['kind'] == 'ConfigMap'
            and row['metadata']['name'].startswith('loom-management-applications-'))
        desired = _snapshot(original)
        template = desired['spec']['template']
        template['metadata']['annotations']['loom.nebius/configuration-revision'] = (
            new['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision'])
        pod = template['spec']
        initializer, = (row for row in new['spec']['template']['spec']['initContainers']
            if row['name'] == 'prepare-application-source')
        for volume in pod['volumes']:
            if volume['name'] == 'management-config':
                volume['configMap']['name'] = config['metadata']['name']
        for container in (*pod['containers'], *pod['initContainers']):
            for mount in container.get('volumeMounts', []):
                if mount['name'] == 'application-source':
                    mount['mountPath'] = '/run/loom-application-source'
            if container['name'] == 'prepare-application-source':
                container['command'] = copy.deepcopy(initializer['command'])
        # The fixed new projection creates a different Secret name only because
        # it hashes the whole configuration. This recovery must retain the
        # original, already qualified source credential, not recreate it.
        source, = (row for row in pod['volumes'] if row['name'] == 'application-source-credentials')
        new_source, = (row for row in new['spec']['template']['spec']['volumes']
            if row['name'] == 'application-source-credentials')
        new_source['secret']['secretName'] = source['secret']['secretName']
        new['spec']['replicas'] = 1
        _qualified_defaulted(new, desired)
        return desired, config
    except Exception:
        raise ValueError('pool_source_repair_projection_unqualified') from None
