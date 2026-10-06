"""Fixed pre-opening source-spool recovery, preserving original cutover ancestry.

This module is internal to the protected continuation. It is not an arbitrary
manifest update or permission to open admission without runtime qualification.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import _activation_record
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, cutover_documents
from scripts.ops.nebius_pool_migration import _hash

from loom.nebius_platform_render import digest

_STEPS = ('stop', 'template', 'start')
_RECOVERY = ('startup-fence', 'shutdown', 'machine-retirement', 'gateway-retirement',
    'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening', 'completion')


class PoolStartupRepairBinding(BaseModel):
    """Non-secret entry pins, separately qualified by the protected envelope."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_id: UUID
    source_sha: str = Field(pattern=r'^[0-9a-f]{40}$')
    original_operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    closure_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    startup_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    activation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class PoolStartupRepairAPI(Protocol):
    @property
    def resources(self) -> ManagementStageAPI: ...
    def qualify_closed(self) -> None: ...
    def read_workload(self, key: str) -> dict[str, Any]: ...
    def manager_drained(self, key: str, desired: dict[str, Any]) -> bool: ...
    def preview_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def patch_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> bool: ...


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


def _exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    return state / 'startup-repair.json', anchor / (operation + '-startup-repair.json')


def startup_repair_exists(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> bool:
    return any(_exists(path) for path in (*_paths(request, state, anchor), state / 'source-repair-configuration'))


def _configuration_record(request: PoolCutoverRequest, config: dict[str, Any], state: Path) -> dict[str, Any] | None:
    path = state / 'source-repair-configuration' / 'stage.json'
    if not _exists(path):
        return None
    documents = {_key(config): config}
    record = json.loads(private_state._private_read(path, limit=1024**2))
    _validate_record(record, {'schema': 'loom.nebius-management-stage.v1',
        'binding': asdict(request.fencing.retirement.migration.registration.binding),
        'revision': digest(documents), 'phase': 'pool-startup-repair-configuration'}, documents)
    return record


def _repair_record(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                   binding: PoolStartupRepairBinding | None = None
                   ) -> tuple[tuple[dict[str, Any], ...], dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """Validate historical entry independently of subsequent activation progress."""
    from scripts.ops.nebius_pool_startup import _startup_record, closed_startup_documents

    path, marker = _paths(request, state, anchor)
    retained = json.loads(private_state._private_read(marker, limit=1024**2)) if _exists(marker) else None
    if retained is None and (_exists(path) or _exists(state / 'source-repair-configuration')):
        raise ValueError
    if retained is not None:
        saved_binding = PoolStartupRepairBinding.model_validate(retained['binding'])
        if binding is not None and binding != saved_binding:
            raise ValueError
        binding = saved_binding
    if binding is None:
        raise ValueError
    binding = PoolStartupRepairBinding.model_validate(binding.model_dump())
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    if not binding.operation_id.int or binding.operation_id == operation:
        raise ValueError
    closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
    _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
    activation_identity, activation = _activation_record(request, state=state, anchor=anchor)
    if (startup is None or any(row['phase'] != 'started' for row in startup['workloads'].values())
            or activation is None or _hash(state / 'cutover.json') != binding.closure_sha256
            or _hash(state / 'startup.json') != binding.startup_sha256):
        raise ValueError
    entry = private_state._private_read(state / 'activation.json').decode() if retained is None else retained['activation_entry']
    prepared = {**activation_identity, 'opening': 'prepared', 'cancellation': 'prepared',
        'guards': {str(row.participant_id): {'release': 'prepared', 'fence': 'prepared'}
            for row in request.fencing.retirement.migration.guards}}
    if (not isinstance(entry, str) or hashlib.sha256(entry.encode()).hexdigest() != binding.activation_sha256
            or json.loads(entry) != prepared):
        raise ValueError
    original = copy.deepcopy(targets[_key(request.manager)])
    original['metadata']['uid'] = _uid(request.manager)
    desired, config = source_repair_documents(request, original)
    stopped, replaced = _snapshot(original), copy.deepcopy(desired)
    stopped['spec']['replicas'] = replaced['spec']['replicas'] = 0
    documents = (_snapshot(original), stopped, replaced, desired)
    identity = {'schema': 'loom.nebius-pool-startup-repair.v1', 'operation_id': str(operation),
        'state_dir': str(state), 'binding': binding.model_dump(mode='json'), 'activation_entry': entry,
        'workloads_sha256': digest(documents), 'configuration_sha256': digest(config)}
    if retained is None:
        return documents, config, identity, None
    if retained != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'configuration', 'phases'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['phases'], dict) or set(record['phases']) != set(_STEPS)):
        raise ValueError
    previous_complete = record['configuration'] is not None
    configuration = _configuration_record(request, config, state)
    if previous_complete and (configuration is None
            or record['configuration'] != _hash(state / 'source-repair-configuration/stage.json')
            or any(row['status'] != 'created' for row in configuration['resources'].values())):
        raise ValueError
    for step in _STEPS:
        row = record['phases'][step]
        if (not isinstance(row, dict) or set(row) != {'phase', 'before_resource_version'}
                or row['phase'] not in {'prepared', 'intent', 'applied'}
                or (not previous_complete and row['phase'] != 'prepared')):
            raise ValueError
        version = row['before_resource_version']
        if row['phase'] == 'prepared':
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        previous_complete = row['phase'] == 'applied'
    return documents, config, identity, record


def _manager_options(documents: tuple[dict[str, Any], ...], record: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    for index, phase in enumerate(_STEPS):
        status = record['phases'][phase]['phase']
        if status != 'applied':
            return (documents[index],) if status == 'prepared' else (documents[index], documents[index + 1])
    return (documents[-1],)


def repaired_startup_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
        choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    if not startup_repair_exists(request, state=state, anchor=anchor):
        return choices
    documents, _, _, record = _repair_record(request, state=state, anchor=anchor)
    key = _key(request.manager)
    if record is None or len(choices[key]) != 1 or _stable(choices[key][0]) != _stable(documents[0]):
        raise ValueError
    return {**choices, key: _manager_options(documents, record)}


def repair_pool_startup(*, request: PoolCutoverRequest, binding: PoolStartupRepairBinding,
        api: PoolStartupRepairAPI, state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Stop/drain/replace/start only the manager; every uncertain write is observed."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            documents, config, identity, record = _repair_record(request, state=state, anchor=anchor, binding=binding)
            operation, key = identity['operation_id'], _key(request.manager)
            path, marker = _paths(request, state, anchor)
            if any(_exists(value) for phase in _RECOVERY for value in (
                    state / (phase + '.json'), anchor / (operation + '-' + phase + '.json'))):
                raise ValueError
            if record is None:
                record = {**identity, 'configuration': None,
                    'phases': {name: {'phase': 'prepared', 'before_resource_version': None} for name in _STEPS}}

            def observe() -> dict[str, Any]:
                if (_hash(state / 'activation.json') != binding.activation_sha256
                        or _hash(state / 'cutover.json') != binding.closure_sha256
                        or _hash(state / 'startup.json') != binding.startup_sha256):
                    raise ValueError
                api.qualify_closed()
                actual = api.read_workload(key)
                if not any(_matches(actual, option, _uid(request.manager)) for option in _manager_options(documents, record)):
                    raise ValueError
                return actual

            def save() -> None:
                private_state._atomic_json(path, record)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': operation, 'admission_open': False, 'runtime_verified': False}

            observe()
            if not _exists(marker):
                private_state._atomic_json(marker, identity)
                save()
            if record['configuration'] is None:
                _stage_fixed_documents(documents={_key(config): config}, revision=digest({_key(config): config}),
                    phase='pool-startup-repair-configuration', binding=request.fencing.retirement.migration.registration.binding,
                    api=api.resources, state_dir=state / 'source-repair-configuration')
                record['configuration'] = _hash(state / 'source-repair-configuration/stage.json')
                save()
            for index, phase in enumerate(_STEPS):
                item = record['phases'][phase]
                if item['phase'] == 'applied':
                    continue
                actual = observe()
                desired = copy.deepcopy(documents[index + 1])
                if item['phase'] == 'prepared' and phase in {'template', 'start'}:
                    drained = api.manager_drained(key, documents[index])
                    if type(drained) is not bool:
                        raise ValueError
                    if not drained:
                        return result('pending_source_repair_drain')
                if item['phase'] == 'prepared':
                    preview = api.preview_repair(phase, actual, desired)
                    if preview is None:
                        return result('pending_source_repair_update')
                    if _stable(preview) != _stable(desired):
                        raise ValueError
                    observe()
                    version = actual['metadata']['resourceVersion']
                    if not isinstance(version, str) or not 0 < len(version) <= 128:
                        raise ValueError
                    item.update(phase='intent', before_resource_version=version)
                    save()
                    try:
                        accepted = api.patch_repair(phase, actual, desired)
                    except Exception:
                        accepted = None
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        save()
                        return result('pending_source_repair_update')
                    actual = api.read_workload(key)
                if not _matches(actual, desired, _uid(request.manager)):
                    if _matches(actual, documents[index], _uid(request.manager)):
                        return result('pending_source_repair_outcome')
                    raise ValueError
                if actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'applied'
                save()
            observe()
            return result('pool_startup_repaired_closed')
    except Exception:
        raise ValueError('pool_startup_repair_unconfirmed_preserve_evidence') from None
