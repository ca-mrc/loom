"""Restore retained predecessor specs, without restoring writers or admission."""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_gateway_retirement import (
    PoolGatewayRetirementAPI,
    _read_gateway_record,
    qualify_gateway_retirement_drain,
)
from scripts.ops.nebius_pool_machine_retirement import _read_machine_record
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_retirement import retirement_documents
from scripts.ops.nebius_pool_shutdown import ShutdownRecord, _shutdown_record
from scripts.ops.nebius_pool_startup_fence import observe_recovery_workloads

from loom.nebius_platform_render import digest

Documents = dict[str, dict[str, Any]]


class RecoveryDrainPending(Enum):
    """Known drain result returned before an intent or mutation is attempted."""

    POOL_CLEANUP = 'pending_pool_cleanup'
    SUCCESSOR_DRAIN = 'pending_successor_drain'


class PoolTemplateRestorationAPI(PoolGatewayRetirementAPI, Protocol):
    def preview_legacy_template(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def restore_legacy_template(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                              record_intent: Callable[[dict[str, Any]], None]) -> bool | RecoveryDrainPending: ...


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'template-restoration.json', anchor / (str(operation) + '-template-restoration.json')


def template_restoration_exists(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> bool:
    return any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor))


def _template_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                     ) -> tuple[Documents, Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    shutdown = _shutdown_record(request, state=state, anchor=anchor)
    return _read_template_record(request, state=state, anchor=anchor, shutdown=shutdown)


def _read_template_record(request: PoolCutoverRequest, *, state: Path, anchor: Path, shutdown: ShutdownRecord
                          ) -> tuple[Documents, Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    # Each current journal, anchor and parent hash is still freshly read here.
    closed, _, stopped, _, shutdown_record = shutdown
    _, _, machine = _read_machine_record(request, state=state, anchor=anchor, targets=stopped, shutdown=shutdown_record)
    _, _, _, gateway = _read_gateway_record(request, state=state, anchor=anchor, machine=machine)
    if gateway is None or any(row['phase'] != 'restricted' for row in gateway['roles'].values()):
        raise ValueError('pool_template_restoration_gateway_retirement_required')
    before = {key: _stable(row) for key, row in closed.items()}
    before.update(copy.deepcopy(stopped))
    originals = {**retirement_documents(request.fencing.retirement),
        **{_key(row): row for row in (request.manager, *request.services)}}
    targets = {}
    for key, original in originals.items():
        if _uid(original) != _uid(closed[key]):
            raise ValueError
        target = copy.deepcopy(before[key])
        target['spec'] = _stable(original)['spec']
        field = 'suspend' if original['kind'] == 'CronJob' else 'replicas'
        target['spec'][field] = True if field == 'suspend' else 0
        targets[key] = target
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-template-restoration.v1', 'operation_id': operation,
        'state_dir': str(state), 'gateway_retirement_sha256': _hash(state / 'gateway-retirement.json'),
        'shutdown_sha256': _hash(state / 'shutdown.json'),
        'workloads_sha256': digest({'before': before, 'targets': targets})}
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return closed, before, targets, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'workloads'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['workloads'], dict) or set(record['workloads']) != set(targets)):
        raise ValueError
    for key, item in record['workloads'].items():
        if (not isinstance(item, dict) or set(item) != {'phase', 'before_resource_version'}
                or item['phase'] not in {'prepared', 'intent', 'restored'}):
            raise ValueError
        version = item['before_resource_version']
        if item['phase'] == 'prepared' or (item['phase'] == 'restored' and before[key] == targets[key]):
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128 or before[key] == targets[key]:
            raise ValueError
    return closed, before, targets, identity, record


def restored_template_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                               choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    if not template_restoration_exists(request, state=state, anchor=anchor):
        return choices
    _, before, targets, _, record = _template_record(request, state=state, anchor=anchor)
    return _restored_template_options(choices=choices, before=before, targets=targets, record=record)


def _restored_template_options(*, choices: dict[str, tuple[dict[str, Any], ...]],
                               before: Documents, targets: Documents, record: dict[str, Any] | None
                               ) -> dict[str, tuple[dict[str, Any], ...]]:
    if record is None or set(choices) != set(before):
        raise ValueError
    if any(len(choices[key]) != 1 or _stable(choices[key][0]) != value for key, value in before.items()):
        raise ValueError
    result = dict(choices)
    for key, item in record['workloads'].items():
        result[key] = ((before[key],) if item['phase'] == 'prepared' else (targets[key],)
            if item['phase'] == 'restored' else (before[key], targets[key]))
    return result


def qualify_template_restoration(request: PoolCutoverRequest, api: PoolGatewayRetirementAPI, *,
                                  state: Path, anchor: Path) -> str | None:
    _template_record(request, state=state, anchor=anchor)
    pending = qualify_gateway_retirement_drain(request, api, state=state, anchor=anchor)
    if pending is not None:
        return pending
    # This boundary owns the drain before and after the effective rights review.
    # Calling the retired wrapper here would repeat its initial full drain.
    api.qualify_gateway_readonly()
    return qualify_gateway_retirement_drain(request, api, state=state, anchor=anchor)


def restore_pool_templates(*, request: PoolCutoverRequest, api: PoolTemplateRestorationAPI,
                            state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            closed, before, targets, identity, record = _template_record(request, state=state, anchor=anchor)
            path, marker = _paths(request, state, anchor)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            def observe() -> Documents:
                api.verify_retained()
                return observe_recovery_workloads(request, api, state=state, anchor=anchor)

            def qualify() -> str | None:
                return qualify_template_restoration(request, api, state=state, anchor=anchor)

            if record is None:
                # Existing journals are freshly qualified in the active row or
                # completion path; preparing new evidence still needs this proof.
                observe()
                pending = qualify()
                if pending is not None:
                    return result(pending)
                record = {**identity, 'workloads': {key: {'phase': 'prepared', 'before_resource_version': None} for key in targets}}
                private_state._atomic_json(marker, identity)
                private_state._atomic_json(path, record)
            for key, desired in targets.items():
                item = record['workloads'][key]
                if item['phase'] == 'restored':
                    continue
                if item['phase'] == 'prepared' and before[key] != desired:
                    # Preview only the fixed target. The actual adapter owns the
                    # full fresh qualification before recording intent and CAS.
                    actual = api.read_workload(key)
                    preview = api.preview_legacy_template(key, actual, desired)
                    if preview is None:
                        return result('pending_template_restoration_update')
                    if _stable(preview) != desired:
                        raise ValueError

                    def record_intent(fresh: dict[str, Any], *, item: dict[str, Any] = item, key: str = key) -> None:
                        nonlocal actual
                        # Qualification may outlast controller status updates.
                        # Only a fresh prepared attempt may bind the final version;
                        # uncertain intents keep their original write-ahead record.
                        if item['phase'] != 'prepared' or not _matches(fresh, actual, _uid(closed[key])):
                            raise ValueError
                        version = fresh['metadata']['resourceVersion']
                        if not isinstance(version, str) or not 0 < len(version) <= 128:
                            raise ValueError
                        actual = fresh
                        item.update(phase='intent', before_resource_version=version)
                        private_state._atomic_json(path, record)

                    try:
                        accepted = api.restore_legacy_template(key, actual, desired, record_intent=record_intent)
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
                        return result('pending_template_restoration_update')
                    actual = api.read_workload(key)
                else:
                    # Unknown intents and no-op rows have no actual dispatch to
                    # own their proof. Keep their fresh observation boundary.
                    actual = observe()[key]
                    pending = qualify()
                    if pending is not None:
                        return result(pending)
                if not _matches(actual, desired, _uid(closed[key])):
                    if item['phase'] == 'intent' and _matches(actual, before[key], _uid(closed[key])):
                        return result('pending_template_restoration_outcome')
                    raise ValueError
                if item['phase'] == 'intent' and actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'restored'
                private_state._atomic_json(path, record)
            observe()
            pending = qualify()
            return result(pending if pending is not None else 'pool_legacy_templates_restored_closed')
    except Exception:
        raise ValueError('pool_template_restoration_unconfirmed_preserve_evidence') from None
