"""Restart exact retained predecessors while admission and successor stay fenced."""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_retirement import retirement_documents
from scripts.ops.nebius_pool_role_restoration import (
    PoolRoleRestorationAPI,
    _role_record,
    qualify_role_restoration,
)
from scripts.ops.nebius_pool_shutdown import _shutdown_record
from scripts.ops.nebius_pool_startup_fence import observe_recovery_workloads
from scripts.ops.nebius_pool_template_restoration import RecoveryDrainPending, _template_record

from loom.nebius_platform_render import digest

Documents = dict[str, dict[str, Any]]


class PoolLegacyRestartAPI(PoolRoleRestorationAPI, Protocol):
    def qualify_gateway_readonly(self) -> None: ...
    def preview_legacy_restart(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def restart_legacy_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                              record_intent: Callable[[dict[str, Any]], None]) -> bool | RecoveryDrainPending: ...


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'legacy-restart.json', anchor / (str(operation) + '-legacy-restart.json')


def legacy_restart_exists(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> bool:
    return any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor))


def _restart_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                    ) -> tuple[Documents, Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    _, _, _, _, roles = _role_record(request, state=state, anchor=anchor)
    if roles is None or any(row['phase'] != 'restored' for row in roles['roles'].values()):
        raise ValueError('pool_legacy_restart_restored_roles_required')
    _, _, before, _, _ = _template_record(request, state=state, anchor=anchor)
    retired = retirement_documents(request.fencing.retirement)
    # Producers first, then the reverse of retirement order. Intake stays closed;
    # no dormant root is enabled and no gateway is part of this restart catalog.
    originals = {**{_key(row): row for row in (request.manager, *request.services)},
        **dict(reversed(tuple(retired.items())))}
    targets = {}
    for key, original in originals.items():
        desired = copy.deepcopy(before[key])
        field = 'suspend' if original['kind'] == 'CronJob' else 'replicas'
        desired['spec'][field] = original['spec'][field]
        if desired['spec'] != _stable(original)['spec']:
            raise ValueError
        targets[key] = desired
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-legacy-restart.v1', 'operation_id': operation,
        'state_dir': str(state), 'role_restoration_sha256': _hash(state / 'role-restoration.json'),
        'template_restoration_sha256': _hash(state / 'template-restoration.json'),
        'workloads_sha256': digest({'originals': originals, 'before': before, 'targets': targets})}
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return originals, before, targets, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'workloads'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['workloads'], dict) or set(record['workloads']) != set(targets)):
        raise ValueError
    for key, item in record['workloads'].items():
        if (not isinstance(item, dict) or set(item) != {'phase', 'before_resource_version'}
                or item['phase'] not in {'prepared', 'intent', 'started'}):
            raise ValueError
        version = item['before_resource_version']
        if item['phase'] == 'prepared' or (item['phase'] == 'started' and before[key] == targets[key]):
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128 or before[key] == targets[key]:
            raise ValueError
    return originals, before, targets, identity, record


def restarted_legacy_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                              choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    if not legacy_restart_exists(request, state=state, anchor=anchor):
        return choices
    _, before, targets, _, record = _restart_record(request, state=state, anchor=anchor)
    if record is None or not set(before) <= set(choices):
        raise ValueError
    if any(len(choices[key]) != 1 or _stable(choices[key][0]) != value for key, value in before.items()):
        raise ValueError
    result = dict(choices)
    for key, item in record['workloads'].items():
        result[key] = ((before[key],) if item['phase'] == 'prepared' else (targets[key],)
            if item['phase'] == 'started' else (before[key], targets[key]))
    return result


def qualify_legacy_restart(request: PoolCutoverRequest, api: PoolLegacyRestartAPI, *,
                            state: Path, anchor: Path) -> str | None:
    _restart_record(request, state=state, anchor=anchor)

    def closed() -> bool:
        drained = api.recovery_drained()
        # Read ledgers before the final authority/fence proof so drift during
        # those reads cannot qualify a legacy workload restart.
        api.verify_retained()
        if (api.pool_state() != 'fenced' or api.machine_authority() != 'revoked'
                or any(api.guard_state(str(row.participant_id)) != 'fenced'
                    for row in request.fencing.retirement.migration.guards)):
            raise ValueError
        if type(drained) is not bool:
            raise ValueError
        return drained

    if not closed():
        return 'pending_pool_cleanup'
    api.qualify_legacy_roles()
    api.qualify_gateway_readonly()
    gateway = 'Deployment:' + request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
    stopped = _shutdown_record(request, state=state, anchor=anchor)[2]
    drained = api.successor_drained(gateway, stopped[gateway])
    if type(drained) is not bool:
        raise ValueError
    if not drained:
        return 'pending_successor_drain'
    return None if closed() else 'pending_pool_cleanup'


def restart_pool_legacy(*, request: PoolCutoverRequest, api: PoolLegacyRestartAPI,
                        state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            originals, before, targets, identity, record = _restart_record(request, state=state, anchor=anchor)
            path, marker = _paths(request, state, anchor)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'],
                    'legacy_restore_allowed': False, 'runtime_verified': False}

            def observe() -> Documents:
                api.verify_retained()
                return observe_recovery_workloads(request, api, state=state, anchor=anchor)

            def qualify() -> str | None:
                return qualify_legacy_restart(request, api, state=state, anchor=anchor)

            if record is None:
                # Before the first restart journal, prove every successor stopped.
                # Prepared dispatch later owns its fresh restart qualification.
                observe()
                pending = qualify_role_restoration(request, api, state=state, anchor=anchor)
                if pending is not None:
                    return result(pending)
                record = {**identity, 'workloads': {key: {'phase': 'prepared', 'before_resource_version': None} for key in targets}}
                private_state._atomic_json(marker, identity)
                private_state._atomic_json(path, record)
            for key, desired in targets.items():
                item = record['workloads'][key]
                if item['phase'] == 'started':
                    continue
                changing = item['phase'] == 'prepared' and before[key] != desired
                if changing:
                    actual = api.read_workload(key)
                    if not _matches(actual, before[key], _uid(originals[key])):
                        raise ValueError
                else:
                    # Unknown intents and unchanged rows remain observation-only.
                    actual = observe()[key]
                    pending = qualify()
                    if pending is not None:
                        return result(pending)
                if changing:
                    preview = api.preview_legacy_restart(key, actual, desired)
                    if preview is None:
                        return result('pending_legacy_restart_update')
                    if _stable(preview) != desired:
                        raise ValueError

                    def record_intent(fresh: dict[str, Any], *, item: dict[str, Any] = item, key: str = key) -> None:
                        nonlocal actual
                        # Qualification may outlast controller status updates.
                        # Only a fresh prepared attempt may bind the final version;
                        # uncertain intents keep their original write-ahead record.
                        if item['phase'] != 'prepared' or not _matches(fresh, actual, _uid(originals[key])):
                            raise ValueError
                        version = fresh['metadata']['resourceVersion']
                        if not isinstance(version, str) or not 0 < len(version) <= 128:
                            raise ValueError
                        actual = fresh
                        item.update(phase='intent', before_resource_version=version)
                        private_state._atomic_json(path, record)

                    try:
                        accepted = api.restart_legacy_workload(key, actual, desired, record_intent=record_intent)
                    except Exception:
                        if item['phase'] != 'intent':
                            raise
                        accepted = None
                    if isinstance(accepted, RecoveryDrainPending):
                        if item != {'phase': 'prepared', 'before_resource_version': None}:
                            raise ValueError
                        return result(accepted.value)
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        private_state._atomic_json(path, record)
                        return result('pending_legacy_restart_update')
                    actual = api.read_workload(key)
                if not _matches(actual, desired, _uid(originals[key])):
                    if item['phase'] == 'intent' and _matches(actual, before[key], _uid(originals[key])):
                        return result('pending_legacy_restart_outcome')
                    raise ValueError
                if item['phase'] == 'intent' and actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'started'
                private_state._atomic_json(path, record)
            observe()
            pending = qualify()
            return result(pending if pending is not None else 'pool_legacy_restart_staged_closed')
    except Exception:
        raise ValueError('pool_legacy_restart_unconfirmed_preserve_evidence') from None
