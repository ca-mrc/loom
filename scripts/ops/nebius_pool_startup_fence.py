"""Settle delayed startup CAS after cancellation, without claiming shutdown.

Only an unresolved original version needs a metadata write. Changed versions
already invalidate the original CAS. Unknown fence writes are readback-only.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import activation_record
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup import (
    PoolWorkloadReader,
    _startup_record,
    closed_startup_documents,
)

STARTUP_FENCE_MARKER = 'loom.nebius/pool-startup-fence'


class PoolStartupFenceAPI(PoolWorkloadReader, Protocol):
    def verify_retained(self) -> None: ...
    def pool_state(self) -> str: ...
    def guard_state(self, participant: str) -> str: ...
    def preview_startup_fence(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def fence_startup(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool: ...


def marked_startup_document(original: dict[str, Any], operation: UUID) -> dict[str, Any]:
    if not isinstance(operation, UUID) or not operation.int or STARTUP_FENCE_MARKER in original['metadata'].get('annotations', {}):
        raise ValueError('startup_fence_marker_unqualified')
    value = _snapshot(original)
    value['metadata'].setdefault('annotations', {})[STARTUP_FENCE_MARKER] = str(operation)
    return value


def startup_fence_patches(original: dict[str, Any], before: dict[str, Any], operation: UUID) -> list[dict[str, Any]]:
    """Fixed real metadata change, bound to the entire observed object version."""
    version = before['metadata']['resourceVersion']
    field = 'suspend' if original['kind'] == 'CronJob' else 'replicas'
    if (original['kind'] not in {'Deployment', 'CronJob'}
            or not _matches(before, original, _uid(original))
            or type(before['spec'][field]) is not (bool if field == 'suspend' else int)
            or before['spec'][field] != (True if field == 'suspend' else 0)
            or not isinstance(version, str) or not 0 < len(version) <= 128):
        raise ValueError('startup_fence_patch_unqualified')
    desired = marked_startup_document(before, operation)
    return [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(original)},
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
        {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
        {'op': 'test', 'path': '/spec', 'value': before['spec']},
        {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']}]


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'startup-fence.json', anchor / (str(operation) + '-startup-fence.json')


def startup_fence_exists(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> bool:
    return any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor))


def _fence_sources(request: PoolCutoverRequest, *, state: Path, anchor: Path,
        closed: dict[str, dict[str, Any]], targets: dict[str, dict[str, Any]], startup: dict[str, Any] | None
        ) -> tuple[dict[str, tuple[str | None, tuple[dict[str, Any], ...]]], dict[str, str | None]]:
    """Original starts plus the one anchored repair; no new recovery protocol."""
    from scripts.ops.nebius_pool_startup_repair import (
        _manager_options,
        _repair_record,
        original_recovery_repair,
        startup_repair_exists,
    )

    sources = {key: (row['before_resource_version'], (closed[key], targets[key]))
        for key, row in ({} if startup is None else startup['workloads']).items() if row['phase'] == 'intent'}
    history: dict[str, str | None] = {}
    if startup_repair_exists(request, state=state, anchor=anchor):
        if original_recovery_repair(request, state=state, anchor=anchor) is not None:
            return sources, history
        documents, _, _, repair = _repair_record(request, state=state, anchor=anchor)
        if repair is None or _key(request.manager) in sources:
            raise ValueError
        version = next((row['before_resource_version'] for row in repair['phases'].values() if row['phase'] == 'intent'), None)
        sources[_key(request.manager)] = (version, _manager_options(documents, repair))
        config = state / 'source-repair-configuration/stage.json'
        history = {'repair_sha256': _hash(state / 'startup-repair.json'),
            'repair_configuration_sha256': _hash(config) if config.exists() or config.is_symlink() else None}
    return sources, history


def _fence_record(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                  closed: dict[str, dict[str, Any]], targets: dict[str, dict[str, Any]],
                  startup: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any] | None]:
    cancellation = activation_record(request, state_dir=state, anchor_dir=anchor)
    if (cancellation is None or cancellation['cancellation'] != 'fenced'
            or any(row['fence'] != 'fenced' for row in cancellation['guards'].values())):
        raise ValueError('startup_fence_requires_cancelled_activation')
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    identity = {'schema': 'loom.nebius-pool-startup-fence.v1', 'operation_id': str(operation),
        'state_dir': str(state), 'closure_sha256': _hash(state / 'cutover.json'),
        'startup_sha256': None if startup is None else _hash(state / 'startup.json'),
        'cancellation_sha256': _hash(state / 'activation.json')}
    sources, repair_history = _fence_sources(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)
    identity.update(repair_history)
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    pending = set(sources)
    if (not isinstance(record, dict) or set(record) != {*identity, 'workloads'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['workloads'], dict) or set(record['workloads']) != pending):
        raise ValueError
    for key, row in record['workloads'].items():
        if not isinstance(row, dict) or set(row) != {'phase', 'expected'} or row['phase'] not in {'prepared', 'intent', 'fenced'}:
            raise ValueError
        version, options = sources[key]
        if row['phase'] == 'intent' and version is None:
            raise ValueError
        if row['phase'] == 'fenced':
            expected = row['expected']
            allowed = (*options, marked_startup_document(options[0], operation)) if version is not None else options
            if (not isinstance(expected, dict) or _stable(expected) != expected
                    or not any(expected == _stable(value) for value in allowed)):
                raise ValueError
        elif row['expected'] is not None:
            raise ValueError
    return identity, record


def fenced_startup_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                           closed: dict[str, dict[str, Any]], targets: dict[str, dict[str, Any]],
                           startup: dict[str, Any] | None,
                           choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    """Extend only the anchored recovery projection; initial startup stays strict."""
    if not startup_fence_exists(request, state=state, anchor=anchor):
        return choices
    _, record = _fence_record(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)
    if record is None:
        raise ValueError
    result = dict(choices)
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    sources, _ = _fence_sources(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)
    for key, row in record['workloads'].items():
        if row['phase'] == 'fenced':
            result[key] = (row['expected'],)
        elif row['phase'] == 'intent':
            result[key] = (*choices[key], marked_startup_document(sources[key][1][0], operation))
    return result


def observe_recovery_workloads(request: PoolCutoverRequest, api: PoolWorkloadReader, *,
                               state: Path, anchor: Path) -> dict[str, dict[str, Any]]:
    from scripts.ops.nebius_pool_startup import startup_workload_options
    from scripts.ops.nebius_pool_startup_repair import original_recovery_repair

    closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
    options = startup_workload_options(request, state_dir=state, anchor_dir=anchor)
    if options is None:
        options = {key: (row,) for key, row in closed.items()}
    _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
    record = (_fence_record(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)[1]
        if startup_fence_exists(request, state=state, anchor=anchor) else None)
    sources = ({} if record is None else _fence_sources(request, state=state, anchor=anchor,
        closed=closed, targets=targets, startup=startup)[0])
    repair = original_recovery_repair(request, state=state, anchor=anchor)
    shutdown = state / 'shutdown.json'
    # startup_workload_options has already qualified the complete shutdown chain.
    stopped = (repair is not None and shutdown.exists()
        and json.loads(private_state._private_read(shutdown, limit=4 * 1024**2))['workloads'][_key(request.manager)]['phase'] == 'stopped')
    observed = {}
    for key, original in closed.items():
        actual = api.read_workload(key)
        if not any(_matches(actual, choice, _uid(original)) for choice in options[key]):
            raise ValueError
        if record is not None and key in record['workloads'] and record['workloads'][key]['phase'] == 'fenced':
            if sources[key][0] is not None and actual['metadata']['resourceVersion'] == sources[key][0]:
                raise ValueError
        if (stopped and key == _key(request.manager) and repair is not None
                and actual['metadata']['resourceVersion'] == repair['phases']['stop']['before_resource_version']):
            raise ValueError
        observed[key] = actual
    return observed


def fence_pool_startup(*, request: PoolCutoverRequest, api: PoolStartupFenceAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Invalidate outstanding startup requests, preserving running cleanup paths."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
            _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
            identity, record = _fence_record(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)
            sources, _ = _fence_sources(request, state=state, anchor=anchor, closed=closed, targets=targets, startup=startup)
            if record is None:
                record = {**identity, 'workloads': {key: {'phase': 'prepared', 'expected': None}
                    for key in sources}}

            def observe() -> dict[str, dict[str, Any]]:
                api.verify_retained()
                if (api.pool_state() != 'fenced' or any(api.guard_state(str(row.participant_id)) != 'fenced'
                        for row in request.fencing.retirement.migration.guards)):
                    raise ValueError
                return observe_recovery_workloads(request, api, state=state, anchor=anchor)

            def save() -> None:
                private_state._atomic_json(state / 'startup-fence.json', record)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            observe()
            _, marker = _paths(request, state, anchor)
            if not marker.exists():
                private_state._atomic_json(marker, identity)
                save()
            for key, item in record['workloads'].items():
                if item['phase'] == 'fenced':
                    continue
                actual = observe()[key]
                version, options = sources[key]
                if version is not None and actual['metadata']['resourceVersion'] == version:
                    if item['phase'] == 'prepared':
                        desired = marked_startup_document(actual, UUID(identity['operation_id']))
                        preview = api.preview_startup_fence(key, actual, desired)
                        if preview is None:
                            return result('pending_startup_fence_update')
                        if _stable(preview) != _stable(desired) or not _matches(actual, options[0], _uid(closed[key])):
                            raise ValueError
                        observe()
                        item['phase'] = 'intent'
                        save()
                        try:
                            accepted = api.fence_startup(key, actual, desired)
                        except Exception:
                            accepted = None
                        if accepted is False:
                            item['phase'] = 'prepared'
                            save()
                            return result('pending_startup_fence_update')
                        actual = observe()[key]
                    if actual['metadata']['resourceVersion'] == version:
                        return result('pending_startup_fence')
                item.update(phase='fenced', expected=_stable(actual))
                save()
            observe()
            return result('startup_writes_fenced')
    except Exception:
        raise ValueError('pool_startup_fence_unconfirmed_preserve_evidence') from None
