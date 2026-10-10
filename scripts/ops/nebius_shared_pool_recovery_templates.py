"""Restore captured current staging templates while every writer stays stopped.

Reuses the fixed CAS response and actual descendant-drain primitives. The shared
parent supplies current SQL/fence/effective-rights qualification; completed
revocation receipts alone are not live permission to restore a template.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import runtime_resource_path
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_gateway import validate_shared_recovery_gateway
from scripts.ops.nebius_shared_pool_recovery_machines import _frame as _revoke_frame
from scripts.ops.nebius_shared_pool_recovery_machines import validate_shared_recovery_machines
from scripts.ops.nebius_shared_pool_recovery_workloads import (
    MARKER,
    validate_shared_recovery_workloads,
)
from scripts.ops.nebius_shared_pool_workloads import _frame as _base_frame

from loom.nebius_platform_render import digest

_PHASE = 'recovery-restore-workloads'
Documents = dict[str, dict[str, Any]]


def _frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _revoke_frame(bindings, state)
    if frame['recovery']['recovery-revoke']['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_machines(bindings=bindings, state_dir=state / 'recovery-revoke', complete=True)
    validate_shared_recovery_gateway(bindings=bindings, state_dir=state / 'recovery-revoke', complete=True)
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    # Semantic dependency qualification belongs to input construction. Every
    # boundary still freshly checks the parent's exact completed receipt bytes.
    frame = _base_frame(bindings, state.parent)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or frame['recovery']['recovery-revoke']['status'] != 'complete'
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[5:])):
        raise ValueError
    return frame


def _inputs(bindings: SharedPoolGuardBindings, state: Path) -> tuple[Documents, Documents]:
    frame = _frame(bindings, state)
    stop = validate_shared_recovery_workloads(bindings=bindings, state_dir=state / 'recovery-stop', complete=True)
    originals, targets = {}, {}
    for key, row in stop['resources'].items():
        value = copy.deepcopy(row['expected'])
        value['metadata']['uid'] = _uid(row['before'])
        originals[key] = value
    for key, captured in bindings.plan.staging.originals.items():
        if _uid(originals[key]) != _uid(captured):
            raise ValueError
        target = _snapshot(captured)
        target['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        target['spec']['suspend' if captured['kind'] == 'CronJob' else 'replicas'] = True if captured['kind'] == 'CronJob' else 0
        targets[key] = target
    if _base_frame(bindings, state) != frame:
        raise ValueError
    return originals, targets


def _identity(bindings: SharedPoolGuardBindings, state: Path, originals: Documents, targets: Documents) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-templates.v1', 'input_digest': bindings.input_digest,
        'state_dir': str(state), 'originals_sha256': digest(originals), 'targets_sha256': digest(targets)}


def _prepared() -> dict[str, Any]:
    return {'status': 'prepared', 'before_resource_version': None}


def _validate_record(record: dict[str, Any], *, identity: dict[str, str], originals: Documents,
        targets: Documents, complete: bool, prepared: bool = False) -> None:
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'resources'} or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['resources'], dict) or record['resources'].keys() != targets.keys()):
        raise ValueError
    unfinished = False
    for key in targets:
        row = record['resources'][key]
        if (not isinstance(row, dict) or set(row) != set(_prepared()) or row['status'] not in {'prepared', 'intent', 'restored'}
                or (complete and row['status'] != 'restored') or ((prepared or unfinished) and row != _prepared())):
            raise ValueError
        same = _stable(originals[key]) == _stable(targets[key])
        version = row['before_resource_version']
        if row['status'] == 'prepared' or (same and row['status'] == 'restored'):
            if version is not None:
                raise ValueError
        elif same or not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        unfinished |= row['status'] != 'restored'


def validate_shared_recovery_templates(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _base_frame(bindings, state_dir.parent)
    originals, targets = _inputs(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir, originals, targets)
    record = _read(state_dir / 'templates.json')
    _validate_record(record, identity=identity, originals=originals, targets=targets, complete=complete, prepared=prepared)
    if ((prepared and frame['recovery'][_PHASE]['status'] != 'prepared')
            or _base_frame(bindings, state_dir.parent) != frame):
        raise ValueError
    return record


def prepare_shared_recovery_templates(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    originals, targets = _inputs(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'templates.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir, originals, targets),
                'resources': {key: _prepared() for key in targets}})
        validate_shared_recovery_templates(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class HTTPSSharedRecoveryTemplates(ManagementKubernetesTransport):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None], recovery_drained: Callable[[], bool],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        frame = _base_frame(self.bindings, self.state.parent)
        self.originals, self.targets = _inputs(self.bindings, self.state.parent)
        if _base_frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared template recovery dependencies changed during construction')
        self.identity = _identity(self.bindings, self.state, self.originals, self.targets)
        self.dependencies = self._dependencies(frame)
        self.qualify_runtime, self.recovery_drained = qualify_runtime, recovery_drained
        self.private_files = dict(private_files or {})
        if (not callable(qualify_runtime) or not callable(recovery_drained)
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared template recovery connection differs')
        self._scope()
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    @staticmethod
    def _dependencies(frame: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy({'forward': frame['forward'],
            'recovery': {phase: frame['recovery'][phase] for phase in RECOVERY_PHASES[:4]}})

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _base_frame(self.bindings, self.state.parent)
        status = frame['recovery'][_PHASE]['status']
        if (frame['outcome'] != 'pending' or status not in {'started', 'complete'}
                or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[6:])
                or self._dependencies(frame) != self.dependencies
                or _identity(self.bindings, self.state, self.originals, self.targets) != self.identity):
            raise ValueError('shared template recovery scope differs')
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=min(len(raw), 4 * 1024**2)) != raw for path, raw in files):
            raise ValueError('shared template recovery private inputs changed')
        receipts = _files(self.state)
        record = _read(self.state / 'templates.json')
        _validate_record(record, identity=self.identity, originals=self.originals, targets=self.targets, complete=status == 'complete')
        if _files(self.state) != receipts or _base_frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared template recovery receipts changed')
        return frame, record, receipts

    def qualify(self, *, writing: bool = False) -> None:
        before = self._scope()
        if writing:
            _active(self.bindings, self.state, 'started')
        self.qualify_runtime(copy.deepcopy(self.request), _PHASE, writing)
        if self._scope() != before:
            raise ValueError('shared template recovery authority changed')

    def _path(self, key: str) -> str:
        original = self.originals[key]
        return runtime_resource_path(original) + '/' + str(original['metadata']['name'])

    def observe(self) -> Documents:
        before = self._scope()
        values = {}
        for key, original in self.originals.items():
            current = self._request('GET', self._path(key))
            row = before[1]['resources'].get(key)
            choices = ((original,) if row is None or row['status'] == 'prepared' else (self.targets[key],)
                if row['status'] == 'restored' else (original, self.targets[key]))
            if current is None or not any(_matches(current, choice, _uid(original)) for choice in choices):
                raise ValueError('shared template recovery workload differs')
            if (row is not None and row['status'] == 'restored' and row['before_resource_version'] is not None
                    and current['metadata']['resourceVersion'] == row['before_resource_version']):
                raise ValueError('shared template recovery version barrier missing')
            values[key] = current
        if self._scope() != before:
            raise ValueError('shared template recovery read authority changed')
        return values

    def drained(self) -> bool:
        before = self._scope()
        self.qualify()
        values = self.observe()
        collections: Documents = {}
        ready = True
        for key, current in values.items():
            namespace = current['metadata']['namespace']
            children_path = ('/apis/apps/v1/namespaces/' + namespace + '/replicasets' if current['kind'] == 'Deployment'
                else '/apis/batch/v1/namespaces/' + namespace + '/jobs')
            pods_path = '/api/v1/namespaces/' + namespace + '/pods'
            for path in (children_path, pods_path):
                if path not in collections:
                    value = self._request('GET', path + '?limit=1000')
                    if value is None:
                        raise ValueError('shared template recovery process collections absent')
                    collections[path] = value
            closed = qualify_closed_workload_drain(original=self.originals[key], desired=_snapshot(current), current=current,
                children=collections[children_path], pods=collections[pods_path])
            ready = ready and closed
        sql_drained = self.recovery_drained()
        if type(sql_drained) is not bool or self.observe() != values:
            raise ValueError('shared template recovery drain changed')
        self.qualify()
        if self._scope() != before:
            raise ValueError('shared template recovery drain authority changed')
        return ready and sql_drained

    def restore_workload(self, key: str, *, record_intent: Callable[[dict[str, Any]], None]) -> bool:
        _active(self.bindings, self.state, 'started')
        scope = self._scope()
        if scope[1]['resources'][key] != _prepared() or not callable(record_intent):
            raise ValueError('shared template recovery has no prepared target')
        desired = self.targets[key]
        if _stable(self.originals[key]) == _stable(desired):
            raise ValueError('unchanged stopped template does not need a write')

        def patches(value: dict[str, Any]) -> list[dict[str, Any]]:
            version = value['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            result = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(value)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': value['metadata']},
                {'op': 'test', 'path': '/spec', 'value': value['spec']}]
            for field in ('labels', 'annotations'):
                if field in desired['metadata']:
                    result.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
                elif field in value['metadata']:
                    result.append({'op': 'remove', 'path': '/metadata/' + field})
            return [*result, {'op': 'replace', 'path': '/spec', 'value': desired['spec']}]

        self.qualify(writing=True)
        if not self.drained():
            return False
        current = self.observe()[key]
        with self.client.stream('PATCH', self._path(key) + '?dryRun=All', json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if not _patch_result(response, desired=desired, uid=_uid(current)):
                return False
        self.qualify(writing=True)
        if not self.drained():
            return False
        current = self.observe()[key]
        if self._scope() != scope:
            raise ValueError('shared template recovery pre-dispatch authority changed')
        record_intent(current)
        selected = self._scope()
        if selected[1]['resources'][key] != {'status': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}:
            raise ValueError('shared template recovery intent differs')
        with self.client.stream('PATCH', self._path(key), json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            accepted = _patch_result(response, desired=desired, uid=_uid(current))
        if self._scope() != selected:
            raise ValueError('shared template recovery outcome unconfirmed')
        return accepted


def advance_shared_recovery_templates(*, bindings: SharedPoolGuardBindings,
        api: HTTPSSharedRecoveryTemplates, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_templates(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'templates.json', record)
                if api._scope()[1] != record:
                    raise ValueError

            if not api.drained():
                return False
            for key, desired in api.targets.items():
                row = record['resources'][key]
                if row['status'] == 'prepared' and _stable(api.originals[key]) != _stable(desired):
                    def intent(value: dict[str, Any], *, selected_key: str = key) -> None:
                        if record['resources'][selected_key] != _prepared() or not _matches(value, api.originals[selected_key], _uid(api.originals[selected_key])):
                            raise ValueError
                        record['resources'][selected_key].update(status='intent', before_resource_version=value['metadata']['resourceVersion'])
                        save()
                    try:
                        accepted = api.restore_workload(key, record_intent=intent)
                    except Exception:
                        if row['status'] != 'intent':
                            raise
                        accepted = True
                    if accepted is False:
                        row.update(_prepared())
                        save()
                        return False
                    if accepted is not True:
                        raise ValueError
                current = api.observe()[key]
                if not _matches(current, desired, _uid(api.originals[key])):
                    return False
                if row['before_resource_version'] is not None and current['metadata']['resourceVersion'] == row['before_resource_version']:
                    raise ValueError
                if row['status'] != 'restored':
                    row['status'] = 'restored'
                    save()
            if not api.drained():
                return False
            validate_shared_recovery_templates(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared template recovery unconfirmed; preserve retained evidence') from None
