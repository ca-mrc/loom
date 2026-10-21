"""Fixed runtime/configuration successors for the closed development upgrade.

Only the connected, anchored parent may deliver these documents. PostgreSQL,
credential objects, namespace identity, storage and neighbor participants stay
outside this projection's mutation set.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.ops.nebius_development_build_policy import build_policy_documents
from scripts.ops.nebius_development_runtime_retained import CompletedDevelopmentRuntime
from scripts.ops.nebius_development_schema_projection import (
    DevelopmentSchemaProjection,
    project_development_schema_successor,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_pool_runtime import _environment

from loom.nebius_platform_render import digest
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.deployment import render_management
from loom_service.pool_management.installation_render import render_gateway

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_storage_operation import CompletedClosedStorage

MARKER = 'loom.nebius/development-schema-operation'
_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, repr=False)
class DevelopmentSchemaResources:
    source: dict[str, dict[str, Any]]
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    targets: dict[str, dict[str, Any]]
    fixed: dict[str, dict[str, Any]]
    updates: dict[str, dict[str, Any]]
    input_digest: str


def schema_original_resources(runtime: CompletedDevelopmentRuntime, *,
        storage: CompletedClosedStorage | None = None) -> dict[str, dict[str, Any]]:
    """Original foundation and manager resources, with completed runtime overlays."""
    result: dict[str, dict[str, Any]] = {}
    foundation = runtime.request.database.foundation
    manager = runtime.request.database.manager.retained.request.retained
    for phase, record in foundation.phases.items():
        if phase != 'storage':
            for item in record['resources'].values():
                row = copy.deepcopy(item['observed'])
                row['metadata']['uid'] = item['uid']
                result[_key(row)] = row
    state = Path(manager.operation['state_dir'])
    for path, raw in manager.files.items():
        if path.name == 'stage.json' and path.parent.parent == state:
            for item in json.loads(raw).get('resources', {}).values():
                row = copy.deepcopy(item['observed'])
                row['metadata']['uid'] = item['uid']
                result[_key(row)] = row
    result.update(copy.deepcopy(runtime.resources))
    if storage is not None:
        if (storage.retained.runtime != runtime.request
                or storage.retained.inputs.refresh.before != runtime.request.database.manager.retained.request.registration.spec):
            raise ValueError('development_schema_storage_predecessor_changed')
        for row in (storage.resources.configuration, storage.resources.collector):
            key = _key(row)
            if key in result and (row['kind'] != 'CronJob' or _uid(row) != _uid(result[key])):
                raise ValueError('development_schema_storage_resource_collision')
            result[key] = copy.deepcopy(row)
    for key, row in result.items():
        if key != _key(row):
            raise ValueError('development_schema_original_resource_invalid')
        _uid(row)
    return result


def schema_runtime_resources(*, runtime: CompletedDevelopmentRuntime,
        projection: DevelopmentSchemaProjection, storage: CompletedClosedStorage | None = None) -> DevelopmentSchemaResources:
    """Retain exact process settings except the validated publication/catalog delta."""
    expected = project_development_schema_successor(runtime=runtime, operation_id=projection.after_pool.operation_id,
        data_publication=projection.data_publication, manager_publication=projection.manager_publication, storage=storage)
    if expected != projection:
        raise ValueError('development_schema_projection_changed')
    source = schema_original_resources(runtime, storage=storage)
    database = runtime.request.database
    manager_namespace = projection.after_manager.namespace
    participant = projection.participant
    before, = (row for row in projection.before_pool.participants if row.participant_id == participant.participant_id)
    allowed = {'loom-dev', manager_namespace, participant.execution_namespace.name, participant.build_namespace.name}
    data_profile = ServiceExecutionRuntimeProfileV1.model_validate(projection.data_publication.bundle.profile)
    prior_profile = ServiceExecutionRuntimeProfileV1.model_validate(database.data_runtime.bundle.profile)
    selected_ids = {row.profile_id for row in participant.targets}
    profiles = projection.after_pool.profiles.model_copy(update={family: tuple(row
        for row in getattr(projection.after_pool.profiles, family) if row.profile_id in selected_ids)
        for family in ('execution', 'task_images', 'application_images')})
    rendered = render_management(projection.after_manager, candidate=projection.manager_publication.bundle.candidate,
        profile=projection.manager_publication.bundle.profile, repo_root=_ROOT)
    config_name = 'loom-management-applications-' + rendered.revision[7:19]
    config, = (copy.deepcopy(row) for row in rendered.files['10-config-network.yaml']
        if row['kind'] == 'ConfigMap' and row['metadata']['name'] == config_name)
    config['data']['installation.json'] = json.dumps(projection.after_manager.installation.model_dump(mode='json'),
        sort_keys=True, separators=(',', ':'))
    gateway = render_gateway(projection.after_pool, namespace=manager_namespace,
        service_image=projection.manager_publication.bundle.candidate['images']['service']['image_ref'],
        kubernetes_endpoint=database.foundation.inputs.config['kubernetes_api_server'])
    catalog, = (copy.deepcopy(row) for group in gateway.values() for row in group if row['kind'] == 'ConfigMap')
    fixed = {_key(row): row for row in (config, catalog)}
    originals, stopped, targets, updates = {}, {}, {}, {}
    for key, original in source.items():
        kind = original['kind']
        if kind not in {'Deployment', 'CronJob'}:
            continue
        if original['metadata'].get('namespace') not in allowed:
            raise ValueError('development_schema_workload_scope_invalid')
        value = copy.deepcopy(original)
        manager = value['metadata']['namespace'] == manager_namespace
        previous = database.manager.publication if manager else database.data_runtime
        following = projection.manager_publication if manager else projection.data_publication
        old_images, new_images = previous.bundle.candidate['images'], following.bundle.candidate['images']
        if key == f'Deployment:{manager_namespace}:loom-pool-gateway':
            old_images = database.manager.retained.request.registration.candidate['images']
        if old_images.keys() != new_images.keys():
            raise ValueError('development_schema_publication_roles_changed')
        image_map = {row['image_ref']: new_images[role]['image_ref'] for role, row in old_images.items()}
        pod = value['spec']['template']['spec'] if kind == 'Deployment' else value['spec']['jobTemplate']['spec']['template']['spec']
        backup = kind == 'CronJob' and value['metadata']['name'] == 'loom-platform-backup'
        for container in pod['containers']:
            if backup:
                continue  # Retained dump/upload code has no application-schema consumer.
            if container['image'] not in image_map:
                raise ValueError('development_schema_process_publication_changed:' + key)
            container['image'] = image_map[container['image']]
            settings = _environment(container)
            for name in ('LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'):
                if name in settings:
                    bound = PoolRuntimeSettings.model_validate_json(settings[name]['value'])
                    if bound.participant != before:
                        raise ValueError('development_schema_process_participant_changed')
                    settings[name]['value'] = bound.model_copy(update={'participant': participant}).model_dump_json()
            name = 'LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON'
            if name in settings and settings[name].get('value') != '{}':
                if ServiceExecutionRuntimeProfileV1.model_validate_json(settings[name]['value']) != prior_profile:
                    raise ValueError('development_schema_process_profile_changed')
                settings[name]['value'] = data_profile.model_dump_json()
            name = 'LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER'
            if name in settings:
                old_build, = (row for row in projection.before_pool.profiles.task_images
                    if row.profile_id in {target.profile_id for target in before.targets})
                new_build, = profiles.task_images
                if json.loads(settings[name]['value']) != old_build.settings.model_dump(mode='json'):
                    raise ValueError('development_schema_builder_settings_changed')
                settings[name]['value'] = new_build.settings.model_dump_json()
        for container in pod.get('initContainers', []):
            if backup:
                continue
            # Native machine-token helpers use the same candidate service image;
            # backup pg_dump retains its separately pinned PostgreSQL image.
            if container['image'] in image_map:
                container['image'] = image_map[container['image']]
            elif kind != 'CronJob' or container['image'] != database.foundation.inputs.config['backup_image']:
                raise ValueError('development_schema_initializer_publication_changed')
        for volume in pod.get('volumes', []):
            if volume['name'] == 'pool-profiles':
                volume['configMap']['name'] = catalog['metadata']['name']
            if manager and value['metadata']['name'] == 'loom-service' and volume['name'] == 'management-config':
                volume['configMap']['name'] = config_name
        if manager and value['metadata']['name'] == 'loom-service':
            value['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision'] = rendered.revision
        value['metadata'].setdefault('annotations', {})[MARKER] = str(projection.after_pool.operation_id)
        stop = copy.deepcopy(original)
        stop['spec']['replicas' if kind == 'Deployment' else 'suspend'] = 0 if kind == 'Deployment' else True
        stop['metadata'].setdefault('annotations', {})[MARKER] = str(projection.after_pool.operation_id)
        originals[key], stopped[key], targets[key] = original, stop, value
    key = 'ConfigMap:loom-dev:loom-platform-config'
    config = copy.deepcopy(source[key])
    if 'profile.json' in config['data']:
        if ServiceExecutionRuntimeProfileV1.model_validate_json(config['data']['profile.json']) != prior_profile:
            raise ValueError('development_schema_foundation_profile_changed')
        config['data']['profile.json'] = data_profile.model_dump_json()
        updates[key] = config
    for desired in build_policy_documents(participant.build_namespace.name, profiles):
        key = _key(desired)
        original = source[key]
        value = copy.deepcopy(original)
        value['spec'] = desired['spec']
        if _snapshot(value) != _snapshot(original):
            updates[key] = value
    required = {f'Deployment:{namespace}:{name}' for namespace, name in (
        ('loom-dev', 'loom-service'), ('loom-dev', 'loom-control-plane'), (manager_namespace, 'loom-service'),
        (manager_namespace, 'loom-pool-gateway'), (participant.execution_namespace.name, 'loom-execution-actuator'))}
    if (not required <= targets.keys() or fixed.keys() & source.keys()
            or targets[f'Deployment:{manager_namespace}:loom-pool-gateway']['spec']['replicas'] != 0):
        raise ValueError('development_schema_runtime_incomplete_or_activated')
    return DevelopmentSchemaResources(source, originals, stopped, targets, fixed, updates,
        digest({'history': runtime.history_sha256, 'catalog': projection.catalog_request.model_dump(mode='json'),
            'targets': targets, 'fixed': fixed, 'updates': updates}))
