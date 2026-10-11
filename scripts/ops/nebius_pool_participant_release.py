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

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract
from scripts.ops.nebius_pool_runtime import (
    _environment,
    participant_readonly_roles,
    wire_participant,
)
from scripts.ops.nebius_staging_pool_attachment import derive_staging_profiles

from loom.nebius_platform_render import build_platform
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh

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


def _body(document: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key not in {'metadata', 'status'}}


def _prepare(*, previous: PoolMigrationRequest, current: tuple[dict[str, Any], ...],
        config: dict[str, Any], candidate: dict[str, Any], runtime_profile: ServiceExecutionRuntimeProfileV1,
        operation_id: UUID, management_origin: str, repo_root: Path) -> PreparedParticipantRelease:
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

    successor = selected.model_dump(mode='json')
    successor['binding_revision'] += 1
    for target in successor['targets']:
        target['profile_id'] = str(uuid5(operation_id, target['profile_id']))
    participant = PoolParticipantV1.model_validate(successor)
    files = build_platform(copy.deepcopy(config), candidate, runtime_profile.model_dump(mode='json'),
        spec.profiles.image_admission_keyring, repo_root=repo_root)
    desired = {_key(row): row for group in files.values() for row in group}
    profiles, actuators = derive_staging_profiles(participant=participant, data=desired[platform_key]['data'],
        runtime_profile=runtime_profile, node_group_id=spec.node_group_id,
        deployments={row['metadata']['name']: row for row in desired.values()
            if row['kind'] == 'Deployment' and row['metadata']['namespace'] == selected.execution_namespace.name})
    after = spec.model_dump(mode='json')
    after.update(operation_id=str(operation_id), policy_revision=spec.policy_revision + 1)
    after['participants'] = [successor if row['participant_id'] == str(selected.participant_id) else row
        for row in after['participants']]
    replacements = {old.profile_id: new.profile_id for old, new in zip(selected.targets, participant.targets, strict=True)}
    for kind in ('execution', 'task_images'):
        fresh = {row['profile_id']: row for row in profiles.model_dump(mode='json')[kind]}
        after['profiles'][kind] = [fresh[str(replacements[UUID(row['profile_id'])])]
            if UUID(row['profile_id']) in replacements else row for row in after['profiles'][kind]]
    refresh = PoolParticipantRefresh(schema_version='loom.pool-participant-refresh.v1', participant_id=selected.participant_id,
        before=spec, after=PoolInstallation.model_validate(after))

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
        operation_id: UUID, management_origin: str, repo_root: Path) -> PreparedParticipantRelease:
    try:
        return _prepare(previous=previous, current=current, config=config, candidate=candidate,
            runtime_profile=runtime_profile, operation_id=operation_id, management_origin=management_origin, repo_root=repo_root)
    except Exception:
        raise ValueError('pool participant release unqualified') from None
