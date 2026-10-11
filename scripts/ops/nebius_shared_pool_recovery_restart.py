"""Restart captured current staging, keeping the cancelled shared runtime closed.

Only the retained staging roots can change. Fixed CAS and actual descendant/
started-Pod primitives are reused; the protected parent supplies current SQL,
writer and runtime challenges. This child cannot reopen either participant.
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
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_gateway import HTTPSSharedRecoveryGateway
from scripts.ops.nebius_shared_pool_recovery_roles import (
    HTTPSSharedRecoveryRoles,
    validate_shared_recovery_roles,
)
from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest

_PHASE = 'recovery-restart'
_INDEX = RECOVERY_PHASES.index(_PHASE)
Documents = dict[str, dict[str, Any]]


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][prior]['status'] != 'complete' for prior in RECOVERY_PHASES[:_INDEX])
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[_INDEX + 1:])):
        raise ValueError
    return frame


def _project_inputs(bindings: SharedPoolGuardBindings, stop: dict[str, Any]) -> tuple[Documents, Documents]:
    """Pure targets from a semantically validated, byte-pinned stop receipt."""
    originals = {}
    for key, row in stop['resources'].items():
        original = copy.deepcopy(row['expected'])
        original['metadata']['uid'] = _uid(row['before'])
        originals[key] = original
    targets = {}
    # The source-qualified forward stop order is collector, actuators,
    # controller, service. Reverse it; do not enable previously dormant roots.
    for key, captured in reversed(tuple(bindings.plan.staging.originals.items())):
        if _uid(originals[key]) != _uid(captured):
            raise ValueError
        target = _snapshot(captured)
        target['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        before = copy.deepcopy(target)
        before['metadata']['uid'] = _uid(captured)
        before['spec']['suspend' if captured['kind'] == 'CronJob' else 'replicas'] = True if captured['kind'] == 'CronJob' else 0
        originals[key], targets[key] = before, target
    return originals, targets


def _inputs(bindings: SharedPoolGuardBindings, state: Path) -> tuple[Documents, Documents]:
    frame = _frame(bindings, state)
    if frame['recovery']['recovery-restore-roles']['status'] != 'complete':
        raise ValueError
    # This validates the complete dependency chain once, including the exact
    # template/stop records consumed below. Active boundaries seal their bytes.
    validate_shared_recovery_roles(bindings=bindings, state_dir=state / 'recovery-restore-roles', complete=True)
    result = _project_inputs(bindings, _read(state / 'recovery-stop/stop.json'))
    if _frame(bindings, state) != frame:
        raise ValueError
    return result


def _identity(bindings: SharedPoolGuardBindings, state: Path, originals: Documents, targets: Documents) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-restart.v1', 'input_digest': bindings.input_digest,
        'state_dir': str(state), 'originals_sha256': digest(originals), 'targets_sha256': digest(targets),
        'order_sha256': digest(list(targets))}


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
        if (not isinstance(row, dict) or set(row) != set(_prepared()) or row['status'] not in {'prepared', 'intent', 'started'}
                or (complete and row['status'] != 'started') or ((prepared or unfinished) and row != _prepared())):
            raise ValueError
        same = _stable(originals[key]) == _stable(targets[key])
        version = row['before_resource_version']
        if row['status'] == 'prepared' or (same and row['status'] == 'started'):
            if version is not None:
                raise ValueError
        elif same or not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        unfinished |= row['status'] != 'started'


def validate_shared_recovery_restart(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    originals, targets = _inputs(bindings, state_dir.parent)
    record = _read(state_dir / 'restart.json')
    _validate_record(record, identity=_identity(bindings, state_dir, originals, targets),
        originals=originals, targets=targets, complete=complete, prepared=prepared)
    if ((prepared and frame['recovery'][_PHASE]['status'] != 'prepared')
            or _frame(bindings, state_dir.parent) != frame):
        raise ValueError
    return record


def prepare_shared_recovery_restart(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    originals, targets = _inputs(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'restart.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir, originals, targets),
                'resources': {key: _prepared() for key in targets}})
        validate_shared_recovery_restart(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def _process_snapshot(current: dict[str, Any], children: dict[str, Any], pods: dict[str, Any]) -> tuple[Documents, Documents]:
    """Pin only this fixed root's processes across runtime challenges.

    Full collection validation is retained, but unrelated namespace work may
    proceed. UID, template, ownership and status changes remain significant;
    server bookkeeping updates alone do not invalidate a process observation.
    """
    deployment = current['kind'] == 'Deployment'
    namespace = current['metadata']['namespace']

    def collection(document: dict[str, Any], kind: str, version: str) -> list[dict[str, Any]]:
        revision = document['metadata']['resourceVersion']
        if (document['apiVersion'] != version or document['kind'] != kind + 'List'
                or not isinstance(revision, str) or not 0 < len(revision) <= 128
                or document['metadata'].get('continue') or not isinstance(document['items'], list)
                or len(document['items']) > 1000):
            raise ValueError('shared recovery restart process collection unqualified')
        rows = [{'apiVersion': version, 'kind': kind, **row} for row in document['items']]
        if (any(row['apiVersion'] != version or row['kind'] != kind or row['metadata']['namespace'] != namespace for row in rows)
                or len({_uid(row) for row in rows}) != len(rows)):
            raise ValueError('shared recovery restart process identity differs')
        return rows

    children_rows = collection(children, 'ReplicaSet' if deployment else 'Job', 'apps/v1' if deployment else 'batch/v1')
    pod_rows = collection(pods, 'Pod', 'v1')
    labels = (current['spec']['selector']['matchLabels'] if deployment
        else current['spec']['jobTemplate']['spec']['template']['metadata']['labels'])
    if not labels:
        raise ValueError('shared recovery restart process selector missing')

    def selected(row: dict[str, Any], parents: set[str]) -> bool:
        owners = row['metadata'].get('ownerReferences', [])
        if not isinstance(owners, list) or any(not isinstance(owner, dict) for owner in owners):
            raise ValueError('shared recovery restart process ownership malformed')
        return (all(row['metadata'].get('labels', {}).get(key) == value for key, value in labels.items())
            or any(owner.get('uid') in parents for owner in owners))

    def stable(row: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(row)
        for field in ('resourceVersion', 'managedFields'):
            result['metadata'].pop(field, None)
        return result

    related = {_uid(row): stable(row) for row in children_rows if selected(row, {_uid(current)})}
    return related, {_uid(row): stable(row) for row in pod_rows if selected(row, set(related))}


class HTTPSSharedRecoveryRestart(ManagementKubernetesTransport):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            roles: HTTPSSharedRecoveryRoles, gateway: HTTPSSharedRecoveryGateway,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None], recovery_drained: Callable[[], bool],
            observe_ready: Callable[[DevelopmentRuntimeInstallRequest, str, dict[str, Any], dict[str, Any], dict[str, Any]], bool],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        frame = _frame(self.bindings, self.state.parent)
        self.originals, self.targets = _inputs(self.bindings, self.state.parent)
        if _frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared recovery restart dependencies changed')
        self.identity = _identity(self.bindings, self.state, self.originals, self.targets)
        self.dependencies = self._dependencies(frame)
        self.roles, self.gateway = roles, gateway
        self.qualify_runtime, self.observe_ready, self.recovery_drained = qualify_runtime, observe_ready, recovery_drained
        self.private_files = dict(private_files or {})
        if (not all(callable(value) for value in (qualify_runtime, observe_ready, recovery_drained))
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared recovery restart connection differs')
        self._scope()
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    @staticmethod
    def _dependencies(frame: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy({'forward': frame['forward'],
            'recovery': {phase: frame['recovery'][phase] for phase in RECOVERY_PHASES[:_INDEX]}})

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _frame(self.bindings, self.state.parent)
        status = frame['recovery'][_PHASE]['status']
        if (frame['outcome'] != 'pending' or status not in {'started', 'complete'}
                or self._dependencies(frame) != self.dependencies
                or _identity(self.bindings, self.state, self.originals, self.targets) != self.identity
                or self.roles.bindings != self.bindings or self.roles.state != self.state.parent / 'recovery-restore-roles'
                or self.gateway.bindings != self.bindings or self.gateway.state != self.state.parent / 'recovery-revoke'):
            raise ValueError('shared recovery restart scope differs')
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=min(len(raw), 4 * 1024**2)) != raw for path, raw in files):
            raise ValueError('shared recovery restart private inputs changed')
        receipts = _files(self.state)
        record = _read(self.state / 'restart.json')
        _validate_record(record, identity=self.identity, originals=self.originals, targets=self.targets, complete=status == 'complete')
        if _files(self.state) != receipts or _frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared recovery restart receipts changed')
        return frame, record, receipts

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
                if row['status'] == 'started' else (original, self.targets[key]))
            if current is None or not any(_matches(current, choice, _uid(original)) for choice in choices):
                raise ValueError('shared recovery restart workload differs')
            if (row is not None and row['status'] == 'started' and row['before_resource_version'] is not None
                    and current['metadata']['resourceVersion'] == row['before_resource_version']):
                raise ValueError('shared recovery restart version did not advance')
            values[key] = current
        if self._scope() != before:
            raise ValueError('shared recovery restart observation authority changed')
        return values

    def _processes(self) -> bool:
        values = self.observe()
        collections: Documents = {}
        processes: dict[str, tuple[str, str, tuple[Documents, Documents]]] = {}
        ready = True
        for key, current in values.items():
            deployment = current['kind'] == 'Deployment'
            namespace = current['metadata']['namespace']
            children_path = ('/apis/apps/v1/namespaces/' + namespace + '/replicasets' if deployment
                else '/apis/batch/v1/namespaces/' + namespace + '/jobs')
            pods_path = '/api/v1/namespaces/' + namespace + '/pods'
            for path in (children_path, pods_path):
                if path not in collections:
                    value = self._request('GET', path + '?limit=1000')
                    if value is None:
                        raise ValueError('shared recovery restart collections absent')
                    collections[path] = value
            processes[key] = (children_path, pods_path,
                _process_snapshot(current, collections[children_path], collections[pods_path]))
            if deployment and current['spec']['replicas'] == 1:
                observed = qualify_started_deployment(current=current, children=collections[children_path],
                    pods=collections[pods_path], region=self.request.database.foundation.inputs.config['region'])
            elif deployment or current['spec']['suspend'] is True:
                observed = qualify_closed_workload_drain(original=self.originals[key], desired=_snapshot(current), current=current,
                    children=collections[children_path], pods=collections[pods_path])
            else:
                # Active captured CronJobs require the parent's current fixed
                # process/runtime challenge; they cannot be treated as drained.
                observed = True
            if key in self.targets and _matches(current, self.targets[key], _uid(self.originals[key])) and observed:
                observed = self.observe_ready(copy.deepcopy(self.request), _PHASE, copy.deepcopy(self.originals[key]),
                    copy.deepcopy(self.targets[key]), copy.deepcopy(current))
                if type(observed) is not bool:
                    raise ValueError('shared recovery restart challenge unqualified')
            ready = ready and observed
        if self.observe() != values:
            raise ValueError('shared recovery restart workloads changed during process observation')
        # A successful runtime challenge is about the processes just inspected,
        # not a Pod that was replaced or became unready during that challenge.
        final = {}
        for path in collections:
            value = self._request('GET', path + '?limit=1000')
            if value is None:
                raise ValueError('shared recovery restart final collections absent')
            final[path] = value
        for key, (children_path, pods_path, expected) in processes.items():
            observed_processes = _process_snapshot(values[key], final[children_path], final[pods_path])
            ready = ready and observed_processes == expected
        if self.observe() != values:
            raise ValueError('shared recovery restart workloads changed during final process observation')
        return ready

    def _ready(self, *, writing: bool = False, drained: bool = False) -> bool:
        before = self._scope()
        if writing or drained:
            _active(self.bindings, self.state, 'started')
        self.qualify_runtime(copy.deepcopy(self.request), _PHASE, writing)
        self.roles.verify_permissions()
        self.gateway.verify_permissions()
        ready = self._processes()
        self.qualify_runtime(copy.deepcopy(self.request), _PHASE, writing)
        self.roles.verify_permissions()
        self.gateway.verify_permissions()
        current_ready = self._processes()
        idle = self.recovery_drained() if drained else True
        if type(idle) is not bool or self._scope() != before:
            raise ValueError('shared recovery restart qualification changed')
        return ready and current_ready and idle

    def ready(self) -> bool:
        """Read-only current readiness through reopening, not historical idle proof."""
        return self._ready()

    def restart_workload(self, key: str, *, record_intent: Callable[[dict[str, Any]], None]) -> bool:
        _active(self.bindings, self.state, 'started')
        scope = self._scope()
        if scope[1]['resources'][key] != _prepared() or not callable(record_intent):
            raise ValueError('shared recovery restart has no prepared target')
        desired = self.targets[key]
        if _stable(self.originals[key]) == _stable(desired):
            raise ValueError('unchanged dormant workload does not need a write')

        def patches(value: dict[str, Any]) -> list[dict[str, Any]]:
            version = value['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            changes = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(value)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': value['metadata']},
                {'op': 'test', 'path': '/spec', 'value': value['spec']}]
            for field in ('labels', 'annotations'):
                if field in desired['metadata']:
                    changes.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
                elif field in value['metadata']:
                    changes.append({'op': 'remove', 'path': '/metadata/' + field})
            return [*changes, {'op': 'replace', 'path': '/spec', 'value': desired['spec']}]

        if not self._ready(writing=True, drained=True):
            return False
        current = self.observe()[key]
        with self.client.stream('PATCH', self._path(key) + '?dryRun=All', json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if not _patch_result(response, desired=desired, uid=_uid(current)):
                return False
        if not self._ready(writing=True, drained=True):
            return False
        current = self.observe()[key]
        if self._scope() != scope:
            raise ValueError('shared recovery restart pre-dispatch authority changed')
        record_intent(current)
        selected = self._scope()
        if selected[1]['resources'][key] != {'status': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}:
            raise ValueError('shared recovery restart intent differs')
        with self.client.stream('PATCH', self._path(key), json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            accepted = _patch_result(response, desired=desired, uid=_uid(current))
        if self._scope() != selected:
            raise ValueError('shared recovery restart outcome unconfirmed')
        return accepted


def advance_shared_recovery_restart(*, bindings: SharedPoolGuardBindings,
        api: HTTPSSharedRecoveryRestart, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_restart(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'restart.json', record)
                if api._scope()[1] != record:
                    raise ValueError

            if not api._ready(drained=True):
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
                        accepted = api.restart_workload(key, record_intent=intent)
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
                if row['status'] != 'started':
                    row['status'] = 'started'
                    save()
                if not api._ready(drained=True):
                    return False
            validate_shared_recovery_restart(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery restart unconfirmed; preserve retained evidence') from None
