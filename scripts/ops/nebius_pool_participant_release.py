"""Project a source-qualified normal release without restoring standalone writers.

This pure connection does not prove publication, completed predecessor history,
guard ownership or live readiness. The protected rollout parent must establish
those barriers before using either the registry delta or these candidate files.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from scripts.ops.deploy_nebius_platform import validate_render
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
from scripts.ops.nebius_pool_runtime import (
    _environment,
    participant_readonly_roles,
    wire_participant,
)
from scripts.ops.nebius_staging_pool_attachment import (
    _exact_pod,
    derive_staging_profiles,
    prepare_staging_pool_attachment,
)

from loom.nebius_platform_render import _obj, build_platform
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.installation_render import render_registration
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh, RefreshAction
from loom_service.pool_management.profiles import PoolProfileCatalog

_RBAC = {'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
_HISTORY = {'loom.nebius/pool-retirement-operation', 'loom.nebius/pool-cutover-operation',
    'loom.nebius/pool-startup-operation', 'loom.nebius/shared-pool-handoff-operation',
    'loom.nebius/participant-refresh-operation'}


@dataclass(frozen=True, repr=False)
class PreparedParticipantRelease:
    refresh: PoolParticipantRefresh
    migration: PoolMigrationRequest
    files: dict[str, list[dict[str, Any]]]
    preserved: dict[str, dict[str, Any]]


@dataclass(frozen=True, repr=False)
class PoolCatalogRefresh:
    """Catalog-only selection; no participant workload or dispatch authority."""

    refresh: PoolParticipantRefresh
    binding: ManagementBinding


def catalog_binding(release: PreparedParticipantRelease | PoolCatalogRefresh) -> ManagementBinding:
    refresh = PoolParticipantRefresh.model_validate(release.refresh.model_dump())
    if isinstance(release, PreparedParticipantRelease):
        if release.migration.registration.spec != refresh.after:
            raise ValueError('catalog refresh installation differs')
        binding = release.migration.registration.binding
    elif isinstance(release, PoolCatalogRefresh):
        binding = release.binding
    else:
        raise ValueError('catalog refresh selection differs')
    if binding.installation_id != str(refresh.before.installation_id):
        raise ValueError('catalog refresh binding differs')
    return binding


def _body(document: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key not in {'metadata', 'status'}}


def _release_participant(selected: PoolParticipantV1, operation_id: UUID) -> PoolParticipantV1:
    successor = selected.model_dump(mode='json')
    successor['binding_revision'] += 1
    for target in successor['targets']:
        target['profile_id'] = str(uuid5(operation_id, target['profile_id']))
    return PoolParticipantV1.model_validate(successor)


def _catalog_refresh(spec: PoolInstallation, participant: PoolParticipantV1,
        profiles: PoolProfileCatalog, operation_id: UUID) -> PoolParticipantRefresh:
    selected, = (row for row in spec.participants if row.participant_id == participant.participant_id)
    if profiles.image_admission_keyring != spec.profiles.image_admission_keyring or profiles.application_images:
        raise ValueError
    after = spec.model_dump(mode='json')
    after.update(operation_id=str(operation_id), policy_revision=spec.policy_revision + 1)
    after['participants'] = [participant.model_dump(mode='json')
        if row['participant_id'] == str(selected.participant_id) else row for row in after['participants']]
    replacements = {old.profile_id: new.profile_id for old, new in zip(selected.targets, participant.targets, strict=True)}
    for kind in ('execution', 'task_images'):
        fresh = {row['profile_id']: row for row in profiles.model_dump(mode='json')[kind]}
        after['profiles'][kind] = [fresh[str(replacements[UUID(row['profile_id'])])]
            if UUID(row['profile_id']) in replacements else row for row in after['profiles'][kind]]
    return PoolParticipantRefresh(schema_version='loom.pool-participant-refresh.v1', participant_id=selected.participant_id,
        before=spec, after=PoolInstallation.model_validate(after))


def prepare_staging_preactivation_refresh(*, registration: PoolRegistrationRequest,
        observation: dict[str, Any], candidate: dict[str, Any], operation_id: UUID,
        repo_root: Path) -> PoolParticipantRefresh:
    """Project a release-only successor from current standalone staging.

    This does not qualify publication, CLOSED state or completed ancestry and
    grants no mutation authority. The protected preactivation parent owns those
    barriers. Registration remains immutable; the ordinary global refresh entry
    point must not be used to apply this projection to a closed pool.
    """
    try:
        spec = PoolInstallation.model_validate(registration.spec.model_dump())
        if (not isinstance(operation_id, UUID) or not operation_id.int
                or registration.binding.installation_id != str(spec.installation_id)):
            raise ValueError
        selected, = (row for row in spec.participants if row.environment_class == 'staging')
        participant = _release_participant(selected, operation_id)
        attachment = prepare_staging_pool_attachment(participant=participant, observation=observation,
            candidate=candidate, cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, repo_root=repo_root)
        return _catalog_refresh(spec, participant, attachment.profiles, operation_id)
    except Exception:
        raise ValueError('staging preactivation refresh unqualified') from None


def _prepare(*, previous: PoolMigrationRequest, current: tuple[dict[str, Any], ...],
        config: dict[str, Any], candidate: dict[str, Any], runtime_profile: ServiceExecutionRuntimeProfileV1,
        operation_id: UUID, management_origin: str, repo_root: Path,
        rendered_files: dict[str, list[dict[str, Any]]] | None) -> PreparedParticipantRelease:
    migration_contract(previous)
    spec = PoolInstallation.model_validate(previous.registration.spec.model_dump())
    selected, = (row for row in spec.participants if row.environment_class == 'staging')
    guard, = (row for row in previous.guards if row.participant_id == selected.participant_id)
    runtime_profile = ServiceExecutionRuntimeProfileV1.model_validate(runtime_profile.model_dump())
    if (candidate.get('repository') != 'ca-mrc/loom' or candidate.get('source_ref') != 'refs/heads/dev'
            or re.fullmatch(r'[0-9a-f]{40}', candidate['candidate_sha']) is None
            or runtime_profile.candidate_sha != candidate['candidate_sha']
            or (config['namespace'], config['execution_namespace'], config['cluster_id'], config['execution_node_group_id'])
                != (guard.namespace, selected.execution_namespace.name, spec.cluster_id, spec.node_group_id)
            or selected.build_namespace.name != config['execution_namespace'] + '-build'
            or not 0 < len(current) <= 32768):
        raise ValueError
    indexed = {_key(row): copy.deepcopy(row) for row in current}
    if len(indexed) != len(current) or len({_uid(row) for row in current}) != len(current):
        raise ValueError
    for row in current:
        _snapshot(row)
    for namespace, uid in ((guard.namespace, guard.namespace_uid),
            (selected.execution_namespace.name, selected.execution_namespace.uid),
            (selected.build_namespace.name, selected.build_namespace.uid)):
        if _uid(indexed[f'Namespace:-:{namespace}']) != str(uid):
            raise ValueError
    platform_key = f'ConfigMap:{guard.namespace}:loom-platform-config'
    data = indexed[platform_key]['data']
    if json.loads(data['environment.json']) != config or json.loads(data['keyring.json']) != spec.profiles.image_admission_keyring:
        raise ValueError
    controller_key = f'Deployment:{guard.namespace}:loom-control-plane'
    service_key = f'Deployment:{guard.namespace}:loom-service'
    if _uid(indexed[controller_key]) != _uid(guard.controller):
        raise ValueError
    old_profile = ServiceExecutionRuntimeProfileV1.model_validate_json(data['profile.json'])
    api_container, = indexed[service_key]['spec']['template']['spec']['containers']
    api_env = _environment(api_container)
    if ServiceExecutionRuntimeProfileV1.model_validate_json(
            api_env['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value']) != old_profile:
        raise ValueError
    source = PoolSubmissionSourceV1.model_validate_json(api_env['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])
    if source != PoolSubmissionSourceV1(kind='environment', data_environment_id=selected.environment_id, application=None):
        raise ValueError
    for profile in spec.profiles.execution:
        if profile.profile_id in {row.profile_id for row in selected.targets} and (
                profile.candidate_sha, profile.runtime_image_ref, profile.runtime_binary_sha256) != (
                old_profile.candidate_sha, old_profile.runtime_image_ref, old_profile.runtime_binary_sha256):
            raise ValueError
    consumers = {controller_key: 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON'}
    for index, target in enumerate(selected.targets):
        name = 'loom-execution-actuator' if index == 0 else target.target_id + '-actuator'
        consumers[f'Deployment:{selected.execution_namespace.name}:{name}'] = 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'
    for key, setting in consumers.items():
        container, = indexed[key]['spec']['template']['spec']['containers']
        runtime = PoolRuntimeSettings.model_validate_json(_environment(container)[setting]['value'])
        if runtime.participant != selected or runtime.management_origin != management_origin:
            raise ValueError

    participant = _release_participant(selected, operation_id)
    files = (build_platform(copy.deepcopy(config), candidate, runtime_profile.model_dump(mode='json'),
        spec.profiles.image_admission_keyring, repo_root=repo_root)
        if rendered_files is None else copy.deepcopy(rendered_files))
    _, rendered_config, _ = validate_render(files)
    desired = {_key(row): row for group in files.values() for row in group}
    rendered_data = desired[platform_key]['data']
    if (rendered_config != config
            or ServiceExecutionRuntimeProfileV1.model_validate_json(rendered_data['profile.json']) != runtime_profile
            or json.loads(rendered_data['keyring.json']) != spec.profiles.image_admission_keyring):
        raise ValueError
    profiles, actuators = derive_staging_profiles(participant=participant, data=desired[platform_key]['data'],
        runtime_profile=runtime_profile, node_group_id=spec.node_group_id,
        deployments={row['metadata']['name']: row for row in desired.values()
            if row['kind'] == 'Deployment' and row['metadata']['namespace'] == selected.execution_namespace.name})
    refresh = _catalog_refresh(spec, participant, profiles, operation_id)

    def retained_identity(document: dict[str, Any]) -> dict[str, Any]:
        """Attach the retained identity to a desired template, never call it observed."""
        result = copy.deepcopy(document)
        old = indexed[_key(document)]
        result['metadata']['uid'] = _uid(old)
        for name, value in old['metadata'].get('annotations', {}).items():
            if name in _HISTORY:
                result['metadata'].setdefault('annotations', {})[name] = value
        return result

    projected_guard = replace(guard, controller=retained_identity(desired[controller_key]))
    migration = replace(previous, registration=replace(previous.registration, spec=refresh.after, candidate=copy.deepcopy(candidate)),
        guards=tuple(projected_guard if row.participant_id == selected.participant_id else row for row in previous.guards))
    ordinary, *guests = actuators
    wired = wire_participant(request=migration, participant_id=selected.participant_id, management_origin=management_origin,
        actuator=retained_identity(ordinary), guest_actuators=tuple(retained_identity(row) for row in guests),
        service=retained_identity(desired[service_key]), runtime_profile=runtime_profile)
    replacements_by_key = {}
    for row in wired.values():
        row['spec']['replicas'] = 1
        row['metadata'].setdefault('annotations', {})['loom.nebius/participant-refresh-operation'] = str(operation_id)
        replacements_by_key[_key(row)] = _snapshot(row)
    readonly = {_key(row): row for row in participant_readonly_roles(request=migration)
        if row['metadata'].get('namespace') in {selected.execution_namespace.name, selected.build_namespace.name}}
    collector = 'loom-execution-capacity-collector'
    collector_keys = {f'{kind}:{selected.execution_namespace.name}:{collector}' for kind in ('CronJob', 'ConfigMap', 'ServiceAccount')}
    collector_keys |= {f'{kind}:-:{selected.execution_namespace.name}-collector' for kind in ('ClusterRole', 'ClusterRoleBinding')}
    if indexed[f'CronJob:{selected.execution_namespace.name}:{collector}']['spec'].get('suspend') is not True:
        raise ValueError
    preserved = {}
    result = {}
    for filename, documents in files.items():
        planned = []
        for document in documents:
            key = _key(document)
            if key in replacements_by_key:
                document = replacements_by_key[key]
            elif key in readonly:
                if _body(indexed[key]) != _body(readonly[key]):
                    raise ValueError
                document = _snapshot(indexed[key])
            elif document['kind'] in _RBAC or key in collector_keys:
                original = indexed[key]
                if document['kind'] in _RBAC and _body(original) != _body(document):
                    raise ValueError
                preserved[key] = copy.deepcopy(original)
                continue
            planned.append(document)
        result[filename] = planned
    if not collector_keys <= preserved.keys() or not readonly.keys() <= {_key(row) for rows in result.values() for row in rows}:
        raise ValueError
    return PreparedParticipantRelease(refresh, migration, result, preserved)


def prepare_staging_pool_release(*, previous: PoolMigrationRequest, current: tuple[dict[str, Any], ...],
        config: dict[str, Any], candidate: dict[str, Any], runtime_profile: ServiceExecutionRuntimeProfileV1,
        operation_id: UUID, management_origin: str, repo_root: Path,
        rendered_files: dict[str, list[dict[str, Any]]] | None = None) -> PreparedParticipantRelease:
    try:
        return _prepare(previous=previous, current=current, config=config, candidate=candidate,
            runtime_profile=runtime_profile, operation_id=operation_id, management_origin=management_origin,
            repo_root=repo_root, rendered_files=rendered_files)
    except Exception:
        raise ValueError('pool participant release unqualified') from None


def _manager(release: PreparedParticipantRelease | PoolCatalogRefresh,
        manager: dict[str, Any]) -> tuple[PoolParticipantRefresh, dict[str, Any]]:
    refresh = PoolParticipantRefresh.model_validate(release.refresh.model_dump())
    binding = catalog_binding(release)
    _uid(manager)
    _snapshot(manager)
    if (manager.get('apiVersion') != 'apps/v1' or manager.get('kind') != 'Deployment'
            or manager['metadata'].get('name') != 'loom-service' or manager['metadata'].get('namespace') != binding.namespace
            or manager['metadata'].get('labels', {}).get('loom.nebius/management-installation') != binding.installation_id
            or type(manager['spec'].get('replicas')) is not int or manager['spec']['replicas'] != 1
            or manager['spec'].get('strategy') != {'type': 'Recreate'}):
        raise ValueError
    pod = manager['spec']['template']['spec']
    container, = pod['containers']
    settings = _environment(container)
    if (container['name'] != 'loom-service' or container.get('envFrom')
            or re.fullmatch(r'.+@sha256:[0-9a-f]{64}', container['image']) is None
            or settings['LOOM_SVC_SERVICE_MODE'] != {'name': 'LOOM_SVC_SERVICE_MODE', 'value': 'management'}
            or settings['LOOM_SVC_POOL_PROFILES_FILE'] != {'name': 'LOOM_SVC_POOL_PROFILES_FILE',
                'value': '/var/run/loom-pool-profiles/profiles.json'}):
        raise ValueError
    volume, = (row for row in pod['volumes'] if row['name'] == 'pool-profiles')
    mount, = (row for row in container['volumeMounts'] if row['name'] == 'pool-profiles')
    if (not _exact_pod(volume, {'name': 'pool-profiles', 'configMap': {
            'name': 'loom-pool-profiles-' + refresh.before.operation_id.hex,
            'items': [{'key': 'profiles.json', 'path': 'profiles.json'}]}}, ('volumes', '*'))
            or not _exact_pod(mount, {'name': 'pool-profiles', 'mountPath': '/var/run/loom-pool-profiles',
                'readOnly': True}, ('containers', '*', 'volumeMounts', '*'))):
        raise ValueError
    return refresh, container


def prepare_manager_catalog_refresh(*, release: PreparedParticipantRelease | PoolCatalogRefresh, manager: dict[str, Any],
        catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Catalog-only desired resources; parent must qualify and retain the restart."""
    try:
        refresh, _ = _manager(release, manager)
        _uid(catalog)
        _snapshot(catalog)
        namespace = catalog_binding(release).namespace
        if (catalog.get('apiVersion') != 'v1' or catalog.get('kind') != 'ConfigMap'
                or catalog['metadata'].get('namespace') != namespace
                or catalog['metadata'].get('name') != 'loom-pool-profiles-' + refresh.before.operation_id.hex
                or catalog.get('immutable') is not True or set(catalog['data']) != {'profiles.json'}
                or PoolProfileCatalog.model_validate_json(catalog['data']['profiles.json']) != refresh.before.profiles):
            raise ValueError
        successor = _obj('ConfigMap', 'loom-pool-profiles-' + refresh.after.operation_id.hex, namespace)
        successor['metadata']['labels'] = {'loom.nebius/management-installation': str(refresh.after.installation_id),
            'loom.nebius/pool': str(refresh.after.pool_id), 'loom.nebius/pool-operation': str(refresh.after.operation_id)}
        successor.update(immutable=True, data={'profiles.json': refresh.after.profiles.model_dump_json()})
        target = _snapshot(manager)
        volume, = (row for row in target['spec']['template']['spec']['volumes'] if row['name'] == 'pool-profiles')
        volume['configMap']['name'] = successor['metadata']['name']
        return {'catalog': successor, 'manager': target}
    except Exception:
        raise ValueError('pool manager catalog refresh unqualified') from None


def refresh_documents(release: PreparedParticipantRelease, *, manager: dict[str, Any],
        action: RefreshAction, observation_id: UUID | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fixed mutations and separately identified reads using the retained image.

    The parent must retain a fresh observation identity for each new read. A
    completed Job proves that historical read only, never current registry state.
    An observation identity cannot create another attempt of a mutation Job.
    """
    try:
        if action not in {'observe', 'capacity', 'fence', 'stage', 'open'}:
            raise ValueError
        if action in {'observe', 'capacity'}:
            if not isinstance(observation_id, UUID) or not observation_id.int:
                raise ValueError
        elif observation_id is not None:
            raise ValueError
        refresh, manager_container = _manager(release, manager)
        namespace = release.migration.registration.binding.namespace
        config, job = render_registration(refresh.before, namespace=namespace, service_image=manager_container['image'])
        name = 'loom-pool-refresh-' + refresh.after.operation_id.hex
        config['metadata']['name'] = name
        config['data'] = {'refresh.json': refresh.model_dump_json()}
        job['metadata']['name'] = ('loom-pool-' + action + '-' + observation_id.hex if observation_id else name + '-' + action)
        labels = {'loom.nebius/management-installation': str(refresh.after.installation_id),
            'loom.nebius/pool-operation': str(refresh.after.operation_id)}
        config['metadata']['labels'] = labels.copy()
        if observation_id is not None:
            labels['loom.nebius/pool-observation'] = str(observation_id)
        job['metadata']['labels'] = labels.copy()
        job['spec']['template']['metadata']['labels'] = labels.copy()
        pod = job['spec']['template']['spec']
        container, = pod['containers']
        container['command'] = ['python', '-m', 'loom_service.pool_management.participant_refresh', action]
        container['env'] = [
            {'name': 'LOOM_POOL_PARTICIPANT_REFRESH_FILE', 'value': '/var/run/loom-pool-installation/refresh.json'},
            {'name': 'LOOM_POOL_PARTICIPANT_REFRESH_DB_URL',
                'valueFrom': {'secretKeyRef': {'name': 'loom-platform-db', 'key': 'admin-url'}}}]
        volume, = (row for row in pod['volumes'] if row['name'] == 'installation')
        volume['configMap']['name'] = name
        return config, job
    except Exception:
        raise ValueError('pool participant refresh command unqualified') from None
