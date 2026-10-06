"""Fixed target selection and image-only deltas; never installation authority."""
from __future__ import annotations

import re
from typing import Any, Literal

from scripts.ops.nebius_ingress_stage import _key, _snapshot
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, _cutover_documents

from loom.nebius_candidate_contract import NEBIUS_PLATFORM_IMAGES

RuntimeImageTarget = Literal['gateway', 'collector']


def runtime_image_key(request: PoolCutoverRequest, target: RuntimeImageTarget) -> str:
    """Names alone confer nothing: the caller resolves this key in closed roots."""
    if target == 'gateway':
        workload, = _cutover_documents(request)['workload']
        if workload['kind'] != 'Deployment':
            raise ValueError('pool_runtime_image_target_unqualified')
        return _key(workload)
    if target != 'collector':
        raise ValueError('pool_runtime_image_target_unqualified')
    migration = request.fencing.retirement.migration
    development, = (row for row in migration.registration.spec.participants if row.environment_class == 'development')
    collector, = (row for row in request.fencing.retirement.collectors
        if row['metadata']['namespace'] == development.execution_namespace.name)
    if collector['kind'] != 'CronJob':
        raise ValueError('pool_runtime_image_target_unqualified')
    return _key(collector)


def runtime_image_component(target: RuntimeImageTarget) -> str:
    if target not in {'gateway', 'collector'}:
        raise ValueError('pool_runtime_image_target_unqualified')
    return 'service' if target == 'gateway' else 'execution_actuator'


def runtime_image_template(document: dict[str, Any]) -> dict[str, Any]:
    if document['kind'] == 'CronJob':
        return dict(document['spec']['jobTemplate']['spec']['template'])
    if document['kind'] != 'Deployment':
        raise ValueError('pool_runtime_image_target_unqualified')
    return dict(document['spec']['template'])


def runtime_image_target(request: PoolCutoverRequest, target: RuntimeImageTarget,
        original: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """The retained startup ancestry owns all non-image fields and live identity."""
    if _key(original) != runtime_image_key(request, target) or request.application_delivery is None:
        raise ValueError('pool_runtime_image_target_unqualified')
    component = runtime_image_component(target)
    image = candidate['images'][component]['image_ref']
    registry = request.application_delivery.before.installation.registry_prefix
    pattern = re.escape(registry + '/' + NEBIUS_PLATFORM_IMAGES[component]) + r'@sha256:[0-9a-f]{64}'
    if not isinstance(image, str) or re.fullmatch(pattern, image) is None:
        raise ValueError('pool_runtime_image_target_unqualified')
    desired = _snapshot(original)
    pod = runtime_image_template(desired)['spec']
    container, = pod['containers']
    old_image = container['image']
    if old_image == image or re.fullmatch(pattern, old_image) is None or pod.get('ephemeralContainers'):
        raise ValueError('pool_runtime_image_target_unqualified')
    for row in (*pod['containers'], *pod.get('initContainers', [])):
        if row['image'] != old_image:
            raise ValueError('pool_runtime_image_target_unqualified')
        row['image'] = image
    return desired
