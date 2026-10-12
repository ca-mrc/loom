"""Exact current-staging transitions for the protected shared-pool parent.

Reuses the existing workload CAS, descendant-drain and runtime-readiness engines.
No caller supplies manifests, no historical rollback template is substituted,
and this child cannot grant authority or release intake. Live global qualification
and process/settings challenges belong to the connected protected parent.
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
from scripts.ops.nebius_development_runtime_transition import (
    advance_runtime_workloads,
    validate_runtime_transition,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_migration import migration_contract
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import (
    FORWARD_PHASES,
    RECOVERY_PHASES,
    _files,
    _frozen_forward,
    _history,
)

from loom.nebius_platform_render import digest

_PHASES = {'forward-stop': 'stop', 'forward-wire': 'replace', 'forward-start': 'start'}


def _frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    """Read parent authority/byte pins; the parent owns other child semantics."""
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    anchor = Path(manager.operation['anchor_dir'])
    if (state != Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
            or not state.is_absolute() or state != state.resolve()
            or bindings.input_digest != digest({'handoff': bindings.plan.input_digest,
                'migration': migration_contract(bindings.migration)})):
        raise ValueError
    _history(bindings)
    identity = {'schema': 'loom.nebius-shared-pool-handoff.v1', 'operation_id': str(bindings.migration.registration.spec.operation_id),
        'input_digest': bindings.input_digest, 'state_dir': str(state)}
    if _read(anchor / 'shared-pool-handoff.json') != identity:
        raise ValueError
    record = _read(state / 'operation.json')
    if (set(record) != {*identity, 'outcome', 'forward', 'recovery'}
            or any(record[key] != value for key, value in identity.items())
            or record['outcome'] not in {'pending', 'shared_pool_handoff_complete', 'staging_restored_dev_closed'}):
        raise ValueError
    for direction, phases in (('forward', FORWARD_PHASES), ('recovery', RECOVERY_PHASES)):
        if not isinstance(record[direction], dict) or set(record[direction]) != set(phases):
            raise ValueError
        unfinished = False
        for phase in phases:
            item = record[direction][phase]
            if (not isinstance(item, dict) or set(item) != {'status', 'files'}
                    or item['status'] not in {'prepared', 'started', 'complete'}
                    or (unfinished and item['status'] != 'prepared')):
                raise ValueError
            if item['status'] == 'complete':
                if item['files'] != _files(state / phase):
                    raise ValueError
            elif item['files'] is not None:
                raise ValueError
            unfinished |= item['status'] != 'complete'
    recovery = anchor / 'shared-pool-recovery.json'
    if recovery.exists() or recovery.is_symlink():
        if _read(recovery) != {**identity, **_frozen_forward(state, record)} or record['outcome'] == 'shared_pool_handoff_complete':
            raise ValueError
    elif (record['outcome'] == 'staging_restored_dev_closed'
            or any(item['status'] != 'prepared' for item in record['recovery'].values())):
        raise ValueError
    _history(bindings)
    return record


def _active(bindings: SharedPoolGuardBindings, state: Path, phase: str, status: str) -> dict[str, Any]:
    if phase not in _PHASES or status not in {'prepared', 'started'}:
        raise ValueError
    record = _frame(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(phase)
    if (record['outcome'] != 'pending' or recovery.exists() or recovery.is_symlink()
            or any(record['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or record['forward'][phase]['status'] != status
            or any(record['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])
            or any(item['status'] != 'prepared' for item in record['recovery'].values())):
        raise ValueError
    return record


def _inputs(bindings: SharedPoolGuardBindings, state: Path, phase: str,
        frame: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if phase not in _PHASES:
        raise ValueError
    plan = bindings.plan
    current = copy.deepcopy(plan.staging.originals)
    for step, engine_phase in _PHASES.items():
        if step == 'forward-stop':
            originals, targets = current, copy.deepcopy(plan.staging.stopped)
        else:
            # The old collector stays stopped. Neither wiring nor startup needs
            # another PATCH to its unchanged suspended template.
            originals = {key: copy.deepcopy(row) for key, row in current.items() if row['kind'] == 'Deployment'}
            targets = {key: copy.deepcopy(plan.staging_targets[key]) for key in originals}
            if step == 'forward-start':
                targets = {key: _snapshot(row) for key, row in originals.items()}
                for key, row in targets.items():
                    row['spec']['replicas'] = plan.staging.originals[key]['spec']['replicas']
        if step == phase:
            return originals, targets
        if frame['forward'][step]['status'] != 'complete':
            raise ValueError
        child = _read(state / step / 'transition.json')
        validate_runtime_transition(child, originals=originals, targets=targets,
            input_digest=bindings.input_digest, phase=engine_phase)
        for key, item in child['resources'].items():
            if item['status'] != 'applied':
                raise ValueError
            successor = copy.deepcopy(item['expected'])
            successor['metadata']['uid'] = _uid(originals[key])
            current[key] = successor
    raise ValueError


def validate_shared_staging_workloads(*, bindings: SharedPoolGuardBindings, phase: str, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or phase not in _PHASES or state_dir.name != phase):
        raise ValueError
    frame = _frame(bindings, state_dir.parent)
    if prepared and frame['forward'][phase]['status'] != 'prepared':
        raise ValueError
    originals, targets = _inputs(bindings, state_dir.parent, phase, frame)
    child = _read(state_dir / 'transition.json')
    validate_runtime_transition(child, originals=originals, targets=targets,
        input_digest=bindings.input_digest, phase=_PHASES[phase])
    for item in child['resources'].values():
        if ((complete and item['status'] != 'applied')
                or (prepared and (item['status'] != 'prepared' or item['expected'] is not None))):
            raise ValueError
    return child


def prepare_shared_staging_workloads(*, bindings: SharedPoolGuardBindings, phase: str, state_dir: Path) -> None:
    if state_dir.name != phase:
        raise ValueError
    frame = _active(bindings, state_dir.parent, phase, 'prepared')
    originals, targets = _inputs(bindings, state_dir.parent, phase, frame)
    with private_state._locked_state(state_dir):
        path = state_dir / 'transition.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {'schema': 'loom.nebius-development-runtime-transition.v1',
                'input_digest': bindings.input_digest, 'phase': _PHASES[phase],
                'originals_digest': digest(originals), 'targets_digest': digest(targets),
                'resources': {key: {'status': 'prepared', 'expected': None} for key in originals}})
        validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir.parent, phase, 'prepared') != frame:
            raise ValueError


def shared_forward_staging_options(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> dict[str, tuple[dict[str, Any], ...]]:
    """Exact forward old/new choices, including an uncertain committed PATCH.

    These are forward history, not live readiness or recovery successors. A
    recovery adapter may use them to qualify its initial captured predecessor,
    but must retain and qualify its own stopped/restored targets afterward.
    """
    frame = _frame(bindings, state_dir)
    result: dict[str, tuple[dict[str, Any], ...]] = {
        key: (copy.deepcopy(row),) for key, row in bindings.plan.staging.originals.items()}
    for phase in _PHASES:
        item = frame['forward'][phase]
        if item['status'] == 'prepared':
            path = state_dir / phase / 'transition.json'
            if path.exists() or path.is_symlink():
                validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=path.parent, complete=False, prepared=True)
            break
        originals, _ = _inputs(bindings, state_dir, phase, frame)
        child = validate_shared_staging_workloads(bindings=bindings, phase=phase,
            state_dir=state_dir / phase, complete=item['status'] == 'complete')
        for key, resource in child['resources'].items():
            before = copy.deepcopy(originals[key])
            if resource['status'] == 'prepared':
                result[key] = (before,)
            else:
                after = copy.deepcopy(resource['expected'])
                after['metadata']['uid'] = _uid(before)
                result[key] = (before, after) if resource['status'] == 'intent' else (after,)
        if item['status'] != 'complete':
            break
    if _frame(bindings, state_dir) != frame:
        raise ValueError
    return result


class HTTPSSharedStagingWorkloads(HTTPSDevelopmentRuntimeWorkloads):
    """Same fixed CAS/readiness transport, with current shared-handoff authority.

    Only the constructor's source selection and private/phase qualification
    differ from CLOSED runtime installation. Reuse the existing actual PATCH,
    descendant collection, default validation and started-Pod checks unchanged.
    """

    def __init__(self, *, bindings: SharedPoolGuardBindings, phase: str, api_server: str,
            ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
            observe_ready: Callable[[DevelopmentRuntimeInstallRequest, str, dict[str, Any], dict[str, Any], dict[str, Any]], bool],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        self.phase = phase
        if (not callable(qualify_runtime) or not callable(observe_ready)
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared staging workload connection differs')
        frame = _active(self.bindings, self.state, phase, 'started')
        self.originals, self.targets = _inputs(self.bindings, self.state, phase, frame)
        self.private_files = dict(private_files or {})
        self.qualify_runtime, self.observe_ready = qualify_runtime, observe_ready
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        _history(self.bindings)
        super()._private_inputs()

    def _qualify(self, *, writing: bool) -> None:
        frame = _active(self.bindings, self.state, self.phase, 'started')
        if _inputs(self.bindings, self.state, self.phase, frame) != (self.originals, self.targets):
            raise ValueError('shared staging workload projection changed')
        path = self.state / self.phase / 'transition.json'
        child = _read(path)
        validate_runtime_transition(child, originals=self.originals, targets=self.targets,
            input_digest=self.bindings.input_digest, phase=_PHASES[self.phase])
        super()._qualify(writing=writing)
        # The reusable transition engine can initialize a brand-new journal.
        # This parent already prepared one: losing/changing it during a live
        # qualification must never turn back into bootstrap or a repeated write.
        if _read(path) != child or _active(self.bindings, self.state, self.phase, 'started') != frame:
            raise ValueError('shared staging workload phase changed')


def advance_shared_staging_workloads(*, bindings: SharedPoolGuardBindings, phase: str,
        api: HTTPSSharedStagingWorkloads, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.phase != phase or state_dir != api.state / phase:
            raise ValueError
        frame = _active(bindings, state_dir.parent, phase, 'started')
        originals, targets = _inputs(bindings, state_dir.parent, phase, frame)
        validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state_dir, complete=False)
        return advance_runtime_workloads(originals=originals, targets=targets, input_digest=bindings.input_digest,
            phase=_PHASES[phase], api=api, state_dir=state_dir)
    except Exception:
        raise ValueError('shared staging workload transition unconfirmed; preserve retained evidence') from None
