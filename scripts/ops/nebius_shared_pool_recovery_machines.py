"""Terminal machine-token retirement within the shared recovery revoke phase.

Uses only the retained new manager's fixed transaction after fresh workload and
ledger drain. Machine completion is not whole-phase completion: the parent must
also retire gateway RBAC before restoring staging. No token renewal, SQL choice,
old cutover history fabrication, Kubernetes mutation or uncertain-write retry.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_machine_database import MachineRetirementState
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_workloads import (
    HTTPSSharedRecoveryWorkloads,
    observe_shared_recovery_workload_drain,
    validate_shared_recovery_workloads,
)
from scripts.ops.nebius_shared_pool_recovery_workloads import (
    _frame as _stop_frame,
)

_PHASE = 'recovery-revoke'


def _frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _stop_frame(bindings, state)
    if frame['recovery']['recovery-stop']['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_workloads(bindings=bindings, state_dir=state / 'recovery-stop', complete=True)
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[4:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-machines.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state)}


def validate_shared_recovery_machines(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir)
    record = _read(state_dir / 'machines.json')
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'status'} or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'intent', 'revoked'}
            or (complete and record['status'] != 'revoked')
            or (prepared and (record['status'] != 'prepared' or frame['recovery'][_PHASE]['status'] != 'prepared'))):
        raise ValueError
    return record


def prepare_shared_recovery_machines(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'machines.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'status': 'prepared'})
        validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class SharedRecoveryMachineTransport:
    """The protected parent's bound SQL transport; no caller-supplied SQL."""

    def __init__(self, *, bindings: SharedPoolGuardBindings, workloads: HTTPSSharedRecoveryWorkloads,
            guards: KubectlPoolGuardAPI, history: KubectlPoolHistoryAPI):
        try:
            self.bindings = copy.deepcopy(bindings)
            self.workloads, self.guards, self.history = workloads, guards, history
            self.migration = self.bindings.migration
            manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
            self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
            self.prefix = tuple(guards.prefix)
            self.kubeconfig, self.kubeconfig_sha256 = guards.kubeconfig, guards.kubeconfig_sha256
            before = self._scope()
            self.namespace = self.migration.registration.binding.namespace
            self.credential = guards._get('secret', 'loom-platform-db', self.namespace)
            self.target = _gateway_history_target(self.bindings, self.credential)
            self._connection()
            if self._scope() != before:
                raise ValueError
        except Exception:
            raise ValueError('shared recovery machine connection unqualified') from None

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        if (self.migration != self.bindings.migration or self.workloads.bindings != self.bindings
                or self.workloads.state != self.state.parent / 'recovery-stop'):
            raise ValueError('shared recovery machine scope differs')
        frame = _active(self.bindings, self.state, 'started')
        files = _files(self.state)
        record = validate_shared_recovery_machines(bindings=self.bindings, state_dir=self.state, complete=False)
        if _files(self.state) != files:
            raise ValueError('shared recovery machine receipts changed')
        return frame, record, files

    def _connection(self) -> None:
        if (self.guards.request != self.migration or self.history.request != self.migration
                or tuple(self.guards.prefix) != self.prefix or tuple(self.history.prefix) != self.prefix
                or self.guards.kubeconfig != self.kubeconfig or self.history.kubeconfig != self.kubeconfig
                or self.guards.kubeconfig_sha256 != self.kubeconfig_sha256 or self.history.kubeconfig_sha256 != self.kubeconfig_sha256
                or self.guards._get('secret', 'loom-platform-db', self.namespace) != self.credential
                or self.history.target != self.target or _gateway_history_target(self.bindings, self.credential) != self.target):
            raise ValueError('shared recovery machine database authority changed')

    def drained(self) -> bool:
        before = self._scope()
        self._connection()
        result = observe_shared_recovery_workload_drain(bindings=self.bindings, api=self.workloads,
            state_dir=self.state.parent / 'recovery-stop')
        self._connection()
        if self._scope() != before:
            raise ValueError('shared recovery machine drain authority changed')
        return result

    def machine_authority(self) -> MachineRetirementState:
        before = self._scope()
        self._connection()
        self.workloads.qualify()
        self._connection()
        result = self.history.machine_retirement('observe')
        # The final retained-runtime callback must not hide an opened fence.
        if (self.history.activation_pool('observe') != 'fenced'
                or any(self.guards.activation_guard(target, 'observe') != 'fenced' for target in self.migration.guards)):
            raise ValueError('shared recovery machine fencing changed')
        self._connection()
        if self._scope() != before:
            raise ValueError('shared recovery machine authority changed during observation')
        return result

    def retire_machines(self, *, record_intent: Callable[[], None]) -> bool:
        before = self._scope()
        if before[1]['status'] != 'prepared' or not callable(record_intent):
            raise ValueError('shared recovery machine dispatch requires prepared authority')
        if self.machine_authority() != 'active':
            raise ValueError('shared recovery machine predecessor differs')
        if not self.drained():
            return False
        if self._scope() != before:
            raise ValueError('shared recovery machine pre-dispatch authority changed')
        record_intent()
        selected = self._scope()
        if selected[0] != before[0] or selected[1] != {**before[1], 'status': 'intent'}:
            raise ValueError('shared recovery machine intent differs')
        self._connection()
        if self.history.machine_retirement('revoke') != 'revoked':
            raise ValueError('shared recovery machine outcome unconfirmed')
        self._connection()
        if self._scope() != selected:
            raise ValueError('shared recovery machine outcome unconfirmed')
        return True


def advance_shared_recovery_machines(*, bindings: SharedPoolGuardBindings,
        api: SharedRecoveryMachineTransport, state_dir: Path) -> bool:
    """Retire machines once; True still requires the parent's gateway retirement."""
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'machines.json', record)
                validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=False)

            def observe() -> MachineRetirementState:
                files = _files(state_dir)
                if validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=False) != record:
                    raise ValueError
                current = api.machine_authority()
                allowed = ('active', 'revoked') if record['status'] == 'intent' else ('active',) if record['status'] == 'prepared' else ('revoked',)
                if (current not in allowed or _files(state_dir) != files or _active(bindings, state_dir, 'started') != frame
                        or validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                return current

            if not api.drained():
                return False
            observe()
            if record['status'] == 'prepared':
                def intent() -> None:
                    if record['status'] != 'prepared':
                        raise ValueError
                    record['status'] = 'intent'
                    save()
                try:
                    accepted = api.retire_machines(record_intent=intent)
                except Exception:
                    if record['status'] != 'intent':
                        raise
                    accepted = True  # Only readback may resolve a persisted uncertain intent.
                if accepted is False:
                    if record['status'] != 'prepared':
                        raise ValueError
                    return False
                if accepted is not True:
                    raise ValueError
            if observe() != 'revoked':
                return False
            if record['status'] != 'revoked':
                record['status'] = 'revoked'
                save()
            if not api.drained():
                return False
            if observe() != 'revoked':
                raise ValueError
            validate_shared_recovery_machines(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery machine retirement unconfirmed; preserve retained evidence') from None
