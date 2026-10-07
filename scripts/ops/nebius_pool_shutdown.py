"""Stop only fenced successors after both sides of the handoff have drained.

This phase preserves templates, credentials, intake guards and all task history.
An unknown stop only observes its retained CAS. Process drain is a fresh final
barrier, not a saved replica count or authority to restore legacy writers.
"""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import _activation_record
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_manager_image_history import IMAGE_MARKER, original_recovery_image
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup import (
    PoolWorkloadReader,
    _startup_record,
    closed_startup_documents,
)
from scripts.ops.nebius_pool_startup_fence import _read_fence_record, observe_recovery_workloads
from scripts.ops.nebius_pool_startup_repair import original_recovery_repair

from loom.nebius_platform_render import digest

Documents = dict[str, dict[str, Any]]


class PoolShutdownAPI(PoolWorkloadReader, Protocol):
    def verify_retained(self) -> None: ...
    def pool_state(self) -> str: ...
    def guard_state(self, participant: str) -> str: ...
    def recovery_drained(self) -> bool: ...
    def preview_stop(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def stop_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                      record_intent: Callable[[dict[str, Any]], None]) -> bool: ...
    def successor_drained(self, key: str, desired: dict[str, Any]) -> bool: ...


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'shutdown.json', anchor / (str(operation) + '-shutdown.json')


def shutdown_exists(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> bool:
    return any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor))


def _shutdown_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                     ) -> tuple[Documents, Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
    _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
    # These ancestors are freshly read together, without intervening remote I/O.
    # The standalone fence reader still qualifies its own complete ancestry.
    _, cancellation = _activation_record(request, state=state, anchor=anchor)
    _, fence = _read_fence_record(request, state=state, anchor=anchor, closed=closed,
        targets=targets, startup=startup, cancellation=cancellation)
    if fence is None or any(row['phase'] != 'fenced' for row in fence['workloads'].values()):
        raise ValueError('pool_shutdown_startup_fence_required')
    original: Documents = {}
    # Reverse startup order: retire adapters/collector before gateway/manager.
    for key in reversed(targets):
        phase = 'prepared' if startup is None else startup['workloads'][key]['phase']
        value = (fence['workloads'][key]['expected'] if key in fence['workloads'] else
            closed[key] if phase == 'prepared' else targets[key])
        original[key] = _stable(value)
    desired = copy.deepcopy(original)
    for value in desired.values():
        value['spec']['suspend' if value['kind'] == 'CronJob' else 'replicas'] = True if value['kind'] == 'CronJob' else 0
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    identity = {'schema': 'loom.nebius-pool-shutdown.v1', 'operation_id': str(operation), 'state_dir': str(state),
        'closure_sha256': _hash(state / 'cutover.json'), 'cancellation_sha256': _hash(state / 'activation.json'),
        'startup_sha256': None if startup is None else _hash(state / 'startup.json'),
        'startup_fence_sha256': _hash(state / 'startup-fence.json'),
        'workloads_sha256': digest({'original': original, 'desired': desired})}
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return closed, original, desired, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'workloads'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['workloads'], dict) or set(record['workloads']) != set(desired)):
        raise ValueError
    for key, item in record['workloads'].items():
        if (not isinstance(item, dict) or set(item) not in (
                {'phase', 'before_resource_version'}, {'phase', 'before_resource_version', 'isolation_stop'})
                or item['phase'] not in {'prepared', 'intent', 'stopped'}):
            raise ValueError
        version = item['before_resource_version']
        if item['phase'] == 'prepared' or (item['phase'] == 'stopped' and original[key] == desired[key]):
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128 or original[key] == desired[key]:
            raise ValueError
        if 'isolation_stop' in item:
            image = original_recovery_image(request, state=state, anchor=anchor)
            retry = item['isolation_stop']
            if (image is None or image.record is None or key != _key(request.manager)
                    or image.record['phases']['isolate'] != {'phase': 'intent', 'before_resource_version': version}
                    or item['phase'] not in {'intent', 'stopped'}
                    or not isinstance(retry, dict) or set(retry) != {'phase', 'before_resource_version'}
                    or retry['phase'] not in {'prepared', 'intent', 'stopped'}
                    or (item['phase'] == 'stopped') != (retry['phase'] == 'stopped')):
                raise ValueError
            extra_version = retry['before_resource_version']
            if retry['phase'] == 'prepared':
                if extra_version is not None:
                    raise ValueError
            elif not isinstance(extra_version, str) or not 0 < len(extra_version) <= 128 or extra_version == version:
                raise ValueError
    return closed, original, desired, identity, record


def shutdown_workload_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                               choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    if not shutdown_exists(request, state=state, anchor=anchor):
        return choices
    _, original, desired, _, record = _shutdown_record(request, state=state, anchor=anchor)
    if record is None:
        raise ValueError
    repair = original_recovery_repair(request, state=state, anchor=anchor)
    image = original_recovery_image(request, state=state, anchor=anchor)
    result = dict(choices)
    for key, item in record['workloads'].items():
        if (image is not None and image.record is not None and key == _key(request.manager)
                and image.record['phases']['isolate']['phase'] == 'intent'):
            if (tuple(_stable(row) for row in choices[key]) != tuple(_stable(row) for row in image.documents[:2])
                    or original[key] != _stable(image.documents[0])):
                raise ValueError
            result[key] = ((desired[key],) if item['phase'] == 'stopped' else choices[key]
                if item['phase'] == 'prepared' else (*choices[key], desired[key]))
            continue
        if (repair is not None and repair['phases']['stop']['phase'] == 'intent'
                and key == _key(request.manager)):
            if tuple(_stable(row) for row in choices[key]) != (original[key], desired[key]):
                raise ValueError
            result[key] = (desired[key],) if item['phase'] == 'stopped' else (original[key], desired[key])
            continue
        if len(choices[key]) != 1 or _stable(choices[key][0]) != original[key]:
            raise ValueError
        result[key] = ((original[key],) if item['phase'] == 'prepared' else (desired[key],)
            if item['phase'] == 'stopped' else (original[key], desired[key]))
    return result


def stop_pool_successors(*, request: PoolCutoverRequest, api: PoolShutdownAPI,
                         state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            closed, original, desired, identity, record = _shutdown_record(request, state=state, anchor=anchor)
            repair = original_recovery_repair(request, state=state, anchor=anchor)
            image = original_recovery_image(request, state=state, anchor=anchor)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            def observe() -> Documents:
                api.verify_retained()
                if (api.pool_state() != 'fenced' or any(api.guard_state(str(row.participant_id)) != 'fenced'
                        for row in request.fencing.retirement.migration.guards)):
                    raise ValueError
                return observe_recovery_workloads(request, api, state=state, anchor=anchor)

            def drained() -> bool:
                value = api.recovery_drained()
                if type(value) is not bool:
                    raise ValueError
                return value

            observe()
            if not drained():
                return result('pending_pool_cleanup')
            if record is None:
                record = {**identity, 'workloads': {key: {'phase': 'prepared', 'before_resource_version': None} for key in desired}}
                private_state._atomic_json(_paths(request, state, anchor)[1], identity)
                private_state._atomic_json(state / 'shutdown.json', record)
            for key, target in desired.items():
                item = record['workloads'][key]
                if item['phase'] == 'stopped':
                    continue
                actual = observe()[key]
                if not drained():
                    return result('pending_pool_cleanup')
                primary = item
                if (image is not None and image.record is not None and key == _key(request.manager)
                        and item['phase'] == 'intent' and 'isolation_stop' not in item
                        and item['before_resource_version'] == image.record['phases']['isolate']['before_resource_version']
                        and _matches(actual, image.documents[1], _uid(closed[key]))):
                    # A late metadata-only isolate proves the original full-CAS
                    # stop impossible. Retain that attempt and journal a distinct
                    # stop removing the marker; never resend an ambiguous CAS.
                    if (image.record['phases']['isolate'] != {'phase': 'intent',
                            'before_resource_version': item['before_resource_version']}
                            or actual['metadata']['resourceVersion'] == item['before_resource_version']):
                        raise ValueError
                    item['isolation_stop'] = {'phase': 'prepared', 'before_resource_version': None}
                    private_state._atomic_json(state / 'shutdown.json', record)
                if 'isolation_stop' in item:
                    item = item['isolation_stop']
                if (item['phase'] == 'prepared' and repair is not None and key == _key(request.manager)
                        and repair['phases']['stop']['phase'] == 'intent'
                        and _matches(actual, target, _uid(closed[key]))):
                    version = repair['phases']['stop']['before_resource_version']
                    if actual['metadata']['resourceVersion'] == version:
                        raise ValueError
                    # The delayed repair stop already reached this exact target.
                    # Retain its CAS version; do not issue a duplicate stop.
                    item.update(phase='stopped', before_resource_version=version)
                    private_state._atomic_json(state / 'shutdown.json', record)
                    continue
                if item['phase'] == 'prepared' and original[key] != target:
                    proposed = _snapshot(actual)
                    if image is not None and key == _key(request.manager):
                        proposed['metadata'].get('annotations', {}).pop(IMAGE_MARKER, None)
                    field = 'suspend' if proposed['kind'] == 'CronJob' else 'replicas'
                    proposed['spec'][field] = target['spec'][field]
                    preview = api.preview_stop(key, actual, proposed)
                    if preview is None:
                        return result('pending_shutdown_update')
                    if _stable(preview) != target:
                        raise ValueError
                    actual = observe()[key]
                    if not drained():
                        return result('pending_pool_cleanup')

                    def record_intent(fresh: dict[str, Any], *, item: dict[str, Any] = item, key: str = key) -> None:
                        nonlocal actual
                        # The adapter finishes its live drain checks before this
                        # last read and write-ahead record. Only a newly prepared
                        # attempt may record a version; unknown intents never call it.
                        if item['phase'] != 'prepared' or not _matches(fresh, actual, _uid(closed[key])):
                            raise ValueError
                        version = fresh['metadata']['resourceVersion']
                        if not isinstance(version, str) or not 0 < len(version) <= 128:
                            raise ValueError
                        actual = fresh
                        item.update(phase='intent', before_resource_version=version)
                        private_state._atomic_json(state / 'shutdown.json', record)

                    try:
                        accepted = api.stop_workload(key, actual, proposed, record_intent=record_intent)
                    except Exception:
                        if item['phase'] != 'intent':
                            raise
                        accepted = None
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        private_state._atomic_json(state / 'shutdown.json', record)
                        return result('pending_shutdown_update')
                    actual = api.read_workload(key)
                if not _matches(actual, target, _uid(closed[key])):
                    allowed = (original[key],) if image is None or key != _key(request.manager) else image.documents[:2]
                    if item['phase'] == 'intent' and any(_matches(actual, row, _uid(closed[key])) for row in allowed):
                        return result('pending_shutdown_outcome')
                    raise ValueError
                if item['phase'] == 'intent' and actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'stopped'
                primary['phase'] = 'stopped'
                private_state._atomic_json(state / 'shutdown.json', record)
            observe()
            if not drained():
                return result('pending_pool_cleanup')
            for key, target in desired.items():
                complete = api.successor_drained(key, target)
                if type(complete) is not bool:
                    raise ValueError
                if not complete:
                    return result('pending_successor_drain')
            observe()
            if not drained():
                return result('pending_pool_cleanup')
            return result('pool_successors_stopped')
    except Exception:
        raise ValueError('pool_shutdown_unconfirmed_preserve_evidence') from None
