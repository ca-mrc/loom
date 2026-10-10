"""Stop exact shared-pool successors under terminal recovery authority.

One fixed CAS both stops a root and adds a recovery marker, invalidating delayed
forward updates even for already-stopped roots. The transport publishes intent
only after its last pre-dispatch qualification. Unknown dispatched writes only
observe; database/backup workloads, template restoration and grants are excluded.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import HTTPSDevelopmentRuntimeWorkloads
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_shared_pool_gateway import validate_shared_gateway
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files, _history
from scripts.ops.nebius_shared_pool_recovery_drain import validate_shared_recovery_drain
from scripts.ops.nebius_shared_pool_recovery_fence import _recovery_frame
from scripts.ops.nebius_shared_pool_workloads import shared_forward_staging_options

from loom.nebius_platform_render import digest

MARKER = 'loom.nebius/shared-pool-recovery-operation'
_PHASE = 'recovery-stop'


def _frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _recovery_frame(bindings, state)
    if any(frame['recovery'][prior]['status'] != 'complete' for prior in RECOVERY_PHASES[:2]):
        raise ValueError
    validate_shared_recovery_drain(bindings=bindings, state_dir=state / 'recovery-drain', complete=True)
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    if (frame['outcome'] != 'pending' or state.name != _PHASE or status not in {'prepared', 'started'}
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[3:])):
        raise ValueError
    return frame


def _read_frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    """Observe current shutdown through revocation; never authorize stop replay."""
    frame = _frame(bindings, state.parent)
    status = frame['recovery'][_PHASE]['status']
    if (state.name != _PHASE or frame['outcome'] != 'pending' or status not in {'started', 'complete'}
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[4:])):
        raise ValueError
    if status == 'started':
        return _active(bindings, state, status)
    return frame


def _sources(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, tuple[dict[str, Any], ...]]:
    frame = _frame(bindings, state)
    result = shared_forward_staging_options(bindings=bindings, state_dir=state)
    completed = bindings.plan.development.runtime
    for key in (*completed.plan.fixed['workloads'], *completed.plan.originals):
        if key in result:
            raise ValueError
        result[key] = (copy.deepcopy(completed.resources[key]),)
    original = copy.deepcopy(bindings.plan.development.gateway_original)
    result[_key(original)] = (original,)
    phase = frame['forward']['forward-gateway']['status']
    child = state / 'forward-gateway'
    if phase != 'prepared' or child.exists() or child.is_symlink():
        files = _files(child, allow_empty=phase == 'prepared')
        if files or phase != 'prepared':
            record = validate_shared_gateway(bindings=bindings, state_dir=child, complete=phase == 'complete', prepared=phase == 'prepared')
            if record['start'] is not None:
                row = record['start']['resources'][_key(original)]
                if row['status'] != 'prepared':
                    after = copy.deepcopy(row['expected'])
                    after['metadata']['uid'] = _uid(original)
                    result[_key(original)] = (original, after) if row['status'] == 'intent' else (after,)
    for key, options in result.items():
        uid = _uid(options[0])
        for row in options:
            if (_key(row) != key or _uid(row) != uid or row['kind'] not in {'Deployment', 'CronJob'}
                    or MARKER in row['metadata'].get('annotations', {})):
                raise ValueError
    if _frame(bindings, state) != frame:
        raise ValueError
    return result


def _stopped(bindings: SharedPoolGuardBindings, original: dict[str, Any]) -> dict[str, Any]:
    result = _snapshot(original)
    if MARKER in result['metadata'].get('annotations', {}):
        raise ValueError
    result['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
    field = 'suspend' if result['kind'] == 'CronJob' else 'replicas'
    result['spec'][field] = True if field == 'suspend' else 0
    return result


def _identity(bindings: SharedPoolGuardBindings, state: Path, sources: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-stop.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state), 'sources_sha256': digest(sources)}


def _prepared() -> dict[str, Any]:
    return {'status': 'prepared', 'before': None, 'before_resource_version': None, 'expected': None}


def validate_shared_recovery_workloads(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    sources = _sources(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir, sources)
    record = _read(state_dir / 'stop.json')
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'resources'} or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['resources'], dict) or record['resources'].keys() != sources.keys()):
        raise ValueError
    unfinished = False
    for key, choices in sources.items():
        row = record['resources'][key]
        if (not isinstance(row, dict) or set(row) != set(_prepared()) or row['status'] not in {'prepared', 'intent', 'stopped'}
                or (complete and row['status'] != 'stopped') or (prepared and row != _prepared())
                or (unfinished and row != _prepared())):
            raise ValueError
        if row['status'] == 'prepared':
            if row != _prepared():
                raise ValueError
        else:
            before, version = row['before'], row['before_resource_version']
            if (not isinstance(before, dict) or not any(_matches(before, choice, _uid(choice)) for choice in choices)
                    or before != {**_snapshot(before), 'metadata': {**_snapshot(before)['metadata'], 'uid': _uid(before)}}
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or row['expected'] != _stopped(bindings, before)):
                raise ValueError
        unfinished |= row['status'] != 'stopped'
    if prepared and _frame(bindings, state_dir.parent)['recovery'][_PHASE]['status'] != 'prepared':
        raise ValueError
    return record


def prepare_shared_recovery_workloads(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    sources = _sources(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'stop.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir, sources),
                'resources': {key: _prepared() for key in sources}})
        validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class HTTPSSharedRecoveryWorkloads(HTTPSDevelopmentRuntimeWorkloads):
    """Reuse fixed workload reads and descendant checks, with recovery-owned CAS."""

    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None], recovery_drained: Callable[[], bool],
            observe_ready: Callable[[DevelopmentRuntimeInstallRequest, str, dict[str, Any], dict[str, Any], dict[str, Any]], bool],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        self.phase = _PHASE
        if (api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')
                or not all(callable(value) for value in (qualify_runtime, recovery_drained, observe_ready))):
            raise ValueError('shared recovery workload connection differs')
        _read_frame(self.bindings, self.state)
        self.sources = _sources(self.bindings, self.state.parent)
        self.originals = {key: copy.deepcopy(options[0]) for key, options in self.sources.items()}
        self.targets = {key: _stopped(self.bindings, row) for key, row in self.originals.items()}
        self.private_files = dict(private_files or {})
        self.qualify_runtime, self.recovery_drained, self.observe_ready = qualify_runtime, recovery_drained, observe_ready
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        _history(self.bindings)
        super()._private_inputs()

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _read_frame(self.bindings, self.state)
        files = _files(self.state)
        record = validate_shared_recovery_workloads(bindings=self.bindings, state_dir=self.state,
            complete=frame['recovery'][_PHASE]['status'] == 'complete')
        if (self.sources != _sources(self.bindings, self.state.parent)
                or self.originals != {key: rows[0] for key, rows in self.sources.items()} or _files(self.state) != files):
            raise ValueError('shared recovery workload sources changed')
        return frame, record, files

    def _qualify(self, *, writing: bool) -> None:
        if writing:
            _active(self.bindings, self.state, 'started')
        before = self._scope()
        self._private_inputs()
        self.qualify_runtime(copy.deepcopy(self.request), self.phase, writing)
        self._private_inputs()
        if self._scope() != before:
            raise ValueError('shared recovery workload authority changed')

    def drained(self) -> bool:
        before = self._scope()
        result = self.recovery_drained()
        if type(result) is not bool or self._scope() != before:
            raise ValueError('shared recovery drain unqualified')
        return result

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        raise ValueError('shared recovery stops require the anchored intent callback')

    def stop_workload(self, key: str, before: dict[str, Any], *, record_intent: Callable[[dict[str, Any]], None]) -> bool:
        _active(self.bindings, self.state, 'started')
        frame, record, _ = self._scope()
        if (record['resources'][key] != _prepared() or not callable(record_intent)
                or not any(_matches(before, row, _uid(row)) for row in self.sources[key])):
            raise ValueError('shared recovery stop predecessor differs')

        def patches(current: dict[str, Any]) -> list[dict[str, Any]]:
            version = current['metadata'].get('resourceVersion')
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            desired = _stopped(self.bindings, current)
            field = 'suspend' if current['kind'] == 'CronJob' else 'replicas'
            return [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(current)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': current['spec']},
                {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']},
                {'op': 'replace', 'path': '/spec/' + field, 'value': desired['spec'][field]}]

        self._qualify(writing=True)
        if not self.drained():
            return False
        with self.client.stream('PATCH', self._path(key) + '?dryRun=All', json=patches(before),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if not _patch_result(response, desired=_stopped(self.bindings, before), uid=_uid(before)):
                return False
        self._qualify(writing=True)
        current = self.read_workload(key)
        if not _matches(current, before, _uid(before)) or not self.drained():
            return False
        if self._scope()[0] != frame or self._scope()[1] != record:
            raise ValueError('shared recovery stop scope changed')
        record_intent(current)
        _, selected, _ = self._scope()
        actual = selected['resources'][key]
        if (actual['status'] != 'intent' or not _matches(current, actual['before'], _uid(before))
                or actual['before_resource_version'] != current['metadata']['resourceVersion']
                or actual['expected'] != _stopped(self.bindings, current)):
            raise ValueError('shared recovery stop intent changed')
        self._private_inputs()
        with self.client.stream('PATCH', self._path(key), json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            return _patch_result(response, desired=actual['expected'], uid=_uid(before))

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        _, record, _ = self._scope()
        row = record['resources'][key]
        if row['status'] != 'stopped' or expected != row['expected']:
            raise ValueError('shared recovery workload is not stopped')
        self.targets[key] = copy.deepcopy(expected)
        return super().workload_ready(key, expected)


def advance_shared_recovery_workloads(*, bindings: SharedPoolGuardBindings, api: HTTPSSharedRecoveryWorkloads, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        sources = _sources(bindings, state_dir.parent)
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'stop.json', record)
                validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=False)

            def observe() -> dict[str, dict[str, Any]]:
                files = _files(state_dir)
                if validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=False) != record:
                    raise ValueError
                api.qualify()
                values = {}
                for key, options in sources.items():
                    row = record['resources'][key]
                    allowed = ((row['expected'],) if row['status'] == 'stopped' else (*options, row['expected'])
                        if row['status'] == 'intent' else options)
                    current = api.read_workload(key)
                    if not any(_matches(current, option, _uid(options[0])) for option in allowed):
                        raise ValueError
                    if row['status'] == 'stopped' and current['metadata']['resourceVersion'] == row['before_resource_version']:
                        raise ValueError
                    values[key] = current
                if (_files(state_dir) != files or _active(bindings, state_dir, 'started') != frame
                        or validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                return values

            observe()
            if not api.drained():
                return False
            for key, options in sources.items():
                row = record['resources'][key]
                current = observe()[key]
                if row['status'] == 'prepared':
                    def intent(value: dict[str, Any], *, selected_key: str = key,
                            choices: tuple[dict[str, Any], ...] = options) -> None:
                        if (record['resources'][selected_key] != _prepared()
                                or not any(_matches(value, option, _uid(option)) for option in choices)):
                            raise ValueError
                        original = _snapshot(value)
                        original['metadata']['uid'] = _uid(value)
                        record['resources'][selected_key] = {'status': 'intent', 'before': original,
                            'before_resource_version': value['metadata']['resourceVersion'], 'expected': _stopped(bindings, value)}
                        save()
                    try:
                        accepted = api.stop_workload(key, current, record_intent=intent)
                    except Exception:
                        if record['resources'][key]['status'] != 'intent':
                            raise
                        accepted = True  # Persisted intent is ambiguous even if dispatch was interrupted.
                    if accepted is False:
                        record['resources'][key] = _prepared()
                        save()
                        return False
                    if accepted is not True:
                        raise ValueError
                    row = record['resources'][key]
                current = observe()[key]
                if not _matches(current, row['expected'], _uid(options[0])):
                    return False
                if current['metadata']['resourceVersion'] == row['before_resource_version']:
                    raise ValueError
                if row['status'] != 'stopped':
                    row['status'] = 'stopped'
                    save()
                if api.workload_ready(key, row['expected']) is not True:
                    return False
            observe()
            if any(api.workload_ready(key, row['expected']) is not True for key, row in record['resources'].items()):
                return False
            if not api.drained():
                return False
            observe()
            validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery workload stop unconfirmed; preserve retained evidence') from None


def observe_shared_recovery_workload_drain(*, bindings: SharedPoolGuardBindings,
        api: HTTPSSharedRecoveryWorkloads, state_dir: Path) -> bool:
    """Recheck actual stopped descendants and ledgers before authority revocation."""
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _read_frame(bindings, state_dir)
        if frame['recovery'][_PHASE]['status'] != 'complete':
            raise ValueError
        files = _files(state_dir)
        record = validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=True)
        api.qualify()
        ledger_before = api.drained()
        ready = [api.workload_ready(key, row['expected']) for key, row in record['resources'].items()]
        api.qualify()
        ledger_after = api.drained()
        if (_read_frame(bindings, state_dir) != frame or _files(state_dir) != files
                or validate_shared_recovery_workloads(bindings=bindings, state_dir=state_dir, complete=True) != record
                or any(type(value) is not bool for value in (ledger_before, *ready, ledger_after))):
            raise ValueError
        return ledger_before and all(ready) and ledger_after
    except Exception:
        raise ValueError('shared recovery current workload drain unqualified') from None
