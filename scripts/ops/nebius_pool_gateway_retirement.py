"""Reduce exact successor gateway Roles after machine/process retirement.

Bindings, reader ClusterRole and all other resources are retained. An anchored
projection qualifies partial recovery; effective reviews still gate completion.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, cutover_documents
from scripts.ops.nebius_pool_machine_retirement import (
    PoolMachineRetirementAPI,
    _machine_record,
    qualify_machine_retirement_drain,
)
from scripts.ops.nebius_pool_migration import _hash

from loom.nebius_platform_render import digest

Documents = dict[str, dict[str, Any]]


class PoolGatewayRetirementAPI(PoolMachineRetirementAPI, Protocol):
    def read_gateway_authority(self, key: str) -> dict[str, Any]: ...
    def preview_gateway_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def restrict_gateway_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool: ...
    def qualify_gateway_retired(self) -> None: ...
    def qualify_gateway_readonly(self) -> None: ...


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'gateway-retirement.json', anchor / (str(operation) + '-gateway-retirement.json')


def _gateway_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                    ) -> tuple[Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    _, _, machine = _machine_record(request, state=state, anchor=anchor)
    if machine is None or machine['phase'] != 'revoked':
        raise ValueError('gateway_retirement_requires_revoked_machines')
    rendered = {_key(row): row for row in cutover_documents(request)['authority']}
    child = json.loads(private_state._private_read(state / 'authority/stage.json', limit=4 * 1024**2))
    originals, targets = {}, {}
    for key, item in child['resources'].items():
        if item['status'] != 'created':
            raise ValueError
        value = copy.deepcopy(item['observed'])
        value['metadata']['uid'] = item['uid']
        _uid(value)
        expected = rendered[key]
        if value['kind'] in {'Role', 'ClusterRole'}:
            if 'aggregationRule' in value or value['rules'] != expected['rules']:
                raise ValueError
        elif value['subjects'] != expected['subjects'] or value['roleRef'] != expected['roleRef']:
            raise ValueError
        originals[key] = value
        if value['kind'] == 'Role':
            target = _snapshot(value)
            for rule in target['rules']:
                rule['verbs'] = [verb for verb in rule['verbs'] if verb not in {'create', 'delete'}]
                if not rule['verbs'] or not set(rule['verbs']) <= {'get', 'list', 'watch'}:
                    raise ValueError
            if target['rules'] == value['rules']:
                raise ValueError
            targets[key] = target
    if set(originals) != set(rendered) or not targets:
        raise ValueError
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-gateway-retirement.v1', 'operation_id': operation,
        'state_dir': str(state), 'machine_retirement_sha256': _hash(state / 'machine-retirement.json'),
        'authority_sha256': _hash(state / 'authority/stage.json'),
        'roles_sha256': digest({'originals': originals, 'targets': targets})}
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return originals, targets, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path))
    if (not isinstance(record, dict) or set(record) != {*identity, 'roles'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['roles'], dict) or set(record['roles']) != set(targets)):
        raise ValueError
    for item in record['roles'].values():
        if (not isinstance(item, dict) or set(item) != {'phase', 'before_resource_version'}
                or item['phase'] not in {'prepared', 'intent', 'restricted'}):
            raise ValueError
        version = item['before_resource_version']
        if item['phase'] == 'prepared':
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
    return originals, targets, identity, record


def gateway_retirement_options(request: PoolCutoverRequest, *, state: Path, anchor: Path
                               ) -> dict[str, tuple[dict[str, Any], ...]] | None:
    if not any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor)):
        return None
    originals, targets, _, record = _gateway_record(request, state=state, anchor=anchor)
    if record is None:
        raise ValueError
    options: dict[str, tuple[dict[str, Any], ...]] = {key: (row,) for key, row in originals.items()}
    for key, item in record['roles'].items():
        options[key] = ((originals[key],) if item['phase'] == 'prepared' else (targets[key],)
            if item['phase'] == 'restricted' else (originals[key], targets[key]))
    return options


def qualify_gateway_retirement_drain(request: PoolCutoverRequest, api: PoolMachineRetirementAPI, *,
                                     state: Path, anchor: Path) -> str | None:
    _gateway_record(request, state=state, anchor=anchor)
    if api.machine_authority() != 'revoked':
        raise ValueError
    pending = qualify_machine_retirement_drain(request, api, state=state, anchor=anchor)
    if api.machine_authority() != 'revoked':
        raise ValueError
    return pending


def retire_gateway_roles(*, request: PoolCutoverRequest, api: PoolGatewayRetirementAPI,
                          state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            originals, targets, identity, record = _gateway_record(request, state=state, anchor=anchor)
            path, marker = _paths(request, state, anchor)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            def observe() -> Documents:
                api.verify_retained()
                options = gateway_retirement_options(request, state=state, anchor=anchor)
                actual = {key: api.read_gateway_authority(key) for key in originals}
                for key, row in actual.items():
                    choices = (originals[key],) if options is None else options[key]
                    if not any(_matches(row, wanted, _uid(originals[key])) for wanted in choices):
                        raise ValueError
                return actual

            def drain() -> str | None:
                return qualify_gateway_retirement_drain(request, api, state=state, anchor=anchor)

            if record is None:
                # Existing journals are freshly qualified in the active row or
                # completion path; preparing new evidence still needs this proof.
                observe()
                pending = drain()
                if pending is not None:
                    return result(pending)
                record = {**identity, 'roles': {key: {'phase': 'prepared', 'before_resource_version': None} for key in targets}}
                private_state._atomic_json(marker, identity)
                private_state._atomic_json(path, record)
            for key, target in targets.items():
                item = record['roles'][key]
                if item['phase'] == 'restricted':
                    continue
                actual = observe()[key]
                pending = drain()
                if pending is not None:
                    return result(pending)
                if item['phase'] == 'prepared':
                    preview = api.preview_gateway_role(key, actual, target)
                    if preview is None:
                        return result('pending_gateway_role_update')
                    if _stable(preview) != _stable(target):
                        raise ValueError
                    observe()
                    pending = drain()
                    if pending is not None:
                        return result(pending)
                    version = actual['metadata']['resourceVersion']
                    if not isinstance(version, str) or not 0 < len(version) <= 128:
                        raise ValueError
                    item.update(phase='intent', before_resource_version=version)
                    private_state._atomic_json(path, record)
                    try:
                        accepted = api.restrict_gateway_role(key, actual, target)
                    except Exception:
                        accepted = None
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        private_state._atomic_json(path, record)
                        return result('pending_gateway_role_update')
                    actual = api.read_gateway_authority(key)
                if not _matches(actual, target, _uid(originals[key])):
                    if item['phase'] == 'intent' and _matches(actual, originals[key], _uid(originals[key])):
                        return result('pending_gateway_role_outcome')
                    raise ValueError
                if actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'restricted'
                private_state._atomic_json(path, record)
            observe()
            pending = drain()
            if pending is not None:
                return result(pending)
            api.qualify_gateway_retired()
            observe()
            pending = drain()
            return result(pending if pending is not None else 'pool_gateway_roles_retired')
    except Exception:
        raise ValueError('pool_gateway_retirement_unconfirmed_preserve_evidence') from None
