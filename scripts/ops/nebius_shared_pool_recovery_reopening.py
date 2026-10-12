"""Reopen only captured staging after recovery; cancelled dev stays fenced.

The protected parent supplies current writer/provider qualification. This child
uses the existing fixed recovery-owner transaction, never ordinary activation
release, and never redispatches an uncertain intent. Terminal verification is a
fresh read-only barrier, not a health claim derived from a historical receipt.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_restart import (
    HTTPSSharedRecoveryRestart,
    validate_shared_recovery_restart,
)
from scripts.ops.nebius_shared_pool_workloads import _frame

_REOPEN = 'recovery-reopen'
_VERIFY = 'recovery-verify'
_INDEX = RECOVERY_PHASES.index(_REOPEN)


def _staging(bindings: SharedPoolGuardBindings) -> PoolGuardTarget:
    captured = bindings.plan.staging.attachment.guard
    target, = (row for row in bindings.migration.guards if row.participant_id == captured.participant_id)
    if target != captured or target.database is None:
        raise ValueError('shared recovery staging guard differs')
    return target


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    if state.name not in {_REOPEN, _VERIFY} or status not in {'prepared', 'started'}:
        raise ValueError
    index = RECOVERY_PHASES.index(state.name)
    if (frame['outcome'] != 'pending' or frame['recovery'][state.name]['status'] != status
            or any(frame['recovery'][prior]['status'] != 'complete' for prior in RECOVERY_PHASES[:index])
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[index + 1:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    if state.name not in {_REOPEN, _VERIFY}:
        raise ValueError
    return {'schema': 'loom.nebius-shared-' + state.name + '.v1', 'input_digest': bindings.input_digest,
        'state_dir': str(state), 'staging_participant_id': str(_staging(bindings).participant_id)}


def _validate_record(record: dict[str, Any], identity: dict[str, str], *, complete: bool,
        prepared: bool = False, verification: bool = False) -> None:
    terminal = 'observed' if verification else 'released'
    allowed = {'prepared', terminal} if verification else {'prepared', 'intent', terminal}
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'status'} or any(record[key] != value for key, value in identity.items())
            or record['status'] not in allowed or (complete and record['status'] != terminal)
            or (prepared and record['status'] != 'prepared')):
        raise ValueError


def validate_shared_recovery_reopening(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    if state_dir.name != _REOPEN or frame['recovery']['recovery-restart']['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_restart(bindings=bindings, state_dir=state_dir.parent / 'recovery-restart', complete=True)
    record = _read(state_dir / 'reopen.json')
    _validate_record(record, _identity(bindings, state_dir), complete=complete, prepared=prepared)
    if ((prepared and frame['recovery'][_REOPEN]['status'] != 'prepared')
            or _frame(bindings, state_dir.parent) != frame):
        raise ValueError
    return record


def prepare_shared_recovery_reopening(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'reopen.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'status': 'prepared'})
        validate_shared_recovery_reopening(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class SharedRecoveryReopeningTransport:
    """Exact retained SQL connections plus current restored-runtime readiness."""

    def __init__(self, *, bindings: SharedPoolGuardBindings, restart: HTTPSSharedRecoveryRestart,
            guards: KubectlPoolGuardAPI, history: KubectlPoolHistoryAPI, qualify: Callable[[], None]):
        try:
            self.bindings = copy.deepcopy(bindings)
            self.migration = self.bindings.migration
            self.restart, self.guards, self.history, self.qualify = restart, guards, history, qualify
            manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
            self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _REOPEN
            frame = _frame(self.bindings, self.state.parent)
            validate_shared_recovery_reopening(bindings=self.bindings, state_dir=self.state,
                complete=frame['recovery'][_REOPEN]['status'] == 'complete')
            self.dependencies = self._dependencies(frame)
            self.identity = _identity(self.bindings, self.state)
            self.target = _staging(self.bindings)
            self.prefix = tuple(guards.prefix)
            self.kubeconfig, self.kubeconfig_sha256 = guards.kubeconfig, guards.kubeconfig_sha256
            self.namespace = self.migration.registration.binding.namespace
            before = self._scope()
            self.credential = guards._get('secret', 'loom-platform-db', self.namespace)
            self.history_target = _gateway_history_target(self.bindings, self.credential)
            self._connection()
            if self._scope() != before or before[0] != frame:
                raise ValueError
        except Exception:
            raise ValueError('shared recovery reopening connection unqualified') from None

    @staticmethod
    def _dependencies(frame: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy({'forward': frame['forward'],
            'recovery': {phase: frame['recovery'][phase] for phase in RECOVERY_PHASES[:_INDEX]}})

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _frame(self.bindings, self.state.parent)
        status = frame['recovery'][_REOPEN]['status']
        if (frame['outcome'] != 'pending' or status not in {'started', 'complete'}
                or any(frame['recovery'][phase]['status'] != 'complete' for phase in RECOVERY_PHASES[:_INDEX])
                or self.migration != self.bindings.migration or self._dependencies(frame) != self.dependencies
                or _identity(self.bindings, self.state) != self.identity or self.target != _staging(self.bindings)
                or self.restart.bindings != self.bindings or self.restart.state != self.state.parent / 'recovery-restart'
                or not callable(self.qualify) or not callable(self.restart.ready)):
            raise ValueError('shared recovery reopening scope differs')
        database = self.bindings.plan.development.runtime.request.database
        if any(private_state._private_read(path, limit=min(len(raw), 4 * 1024**2)) != raw
                for path, raw in (*database.manager.retained.files.items(), *database.foundation.files.items())):
            raise ValueError('shared recovery reopening private inputs changed')
        files = _files(self.state)
        record = _read(self.state / 'reopen.json')
        _validate_record(record, self.identity, complete=status == 'complete')
        if _files(self.state) != files or _frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared recovery reopening receipts changed')
        return frame, record, files

    def _connection(self) -> None:
        if (self.guards.request != self.migration or self.history.request != self.migration
                or tuple(self.guards.prefix) != self.prefix or tuple(self.history.prefix) != self.prefix
                or self.guards.kubeconfig != self.kubeconfig or self.history.kubeconfig != self.kubeconfig
                or self.guards.kubeconfig_sha256 != self.kubeconfig_sha256 or self.history.kubeconfig_sha256 != self.kubeconfig_sha256
                or self.guards._get('secret', 'loom-platform-db', self.namespace) != self.credential
                or self.history.target != self.history_target
                or _gateway_history_target(self.bindings, self.credential) != self.history_target):
            raise ValueError('shared recovery reopening database authority changed')

    def _authority(self, status: str) -> dict[str, str]:
        if self.history.activation_pool('observe') != 'fenced' or self.history.machine_retirement('observe') != 'revoked':
            raise ValueError('shared recovery cancelled pool authority changed')
        states = {}
        for target in self.migration.guards:
            current = self.guards.activation_guard(target, 'observe')
            allowed = ({'prepared': ('fenced',), 'intent': ('fenced', 'open'), 'released': ('open',)}[status]
                if target == self.target else ('fenced',))
            if current not in allowed:
                raise ValueError('shared recovery reopening guard unqualified')
            states[str(target.participant_id)] = current
        return states

    def observe(self) -> tuple[str, bool]:
        """Opened staging may work; only cancelled or still-fenced ledgers idle."""
        before = self._scope()
        self._connection()
        self.qualify()
        self._authority(before[1]['status'])
        ready = self.restart.ready()
        self.qualify()
        current_ready = self.restart.ready()
        self._connection()
        states = self._authority(before[1]['status'])
        values = [ready, current_ready, self.history.recovery_pool_drained()]
        values.extend(self.guards.recovery_participant_drained(target) for target in self.migration.guards
            if states[str(target.participant_id)] == 'fenced')
        after = self._authority(before[1]['status'])
        self._connection()
        if any(type(value) is not bool for value in values) or self._scope() != before:
            raise ValueError('shared recovery reopening observation changed')
        # An uncertain release can settle during observation. Re-observe that
        # new state; do not treat the prior drained snapshot as its qualification.
        return after[str(self.target.participant_id)], all(values) and states == after

    def release_staging(self, *, record_intent: Callable[[], None]) -> bool:
        frame = _active(self.bindings, self.state, 'started')
        before = self._scope()
        if before[1]['status'] != 'prepared' or not callable(record_intent):
            raise ValueError('shared recovery reopening requires prepared authority')
        current, ready = self.observe()
        if current != 'fenced':
            raise ValueError('shared recovery staging predecessor differs')
        if not ready:
            return False
        if self._scope() != before:
            raise ValueError('shared recovery reopening pre-dispatch authority changed')
        record_intent()
        selected = self._scope()
        if selected[0] != frame or selected[1] != {**before[1], 'status': 'intent'}:
            raise ValueError('shared recovery reopening intent differs')
        self._connection()
        # This fixed transaction also checks exact recovery ownership and fresh
        # staging activity under the admission lock. It never retries a write.
        if self.guards.release_recovery_guard(self.target) != 'open':
            raise ValueError('shared recovery reopening outcome unconfirmed')
        self._connection()
        if self._scope() != selected:
            raise ValueError('shared recovery reopening outcome unconfirmed')
        return True


def advance_shared_recovery_reopening(*, bindings: SharedPoolGuardBindings,
        api: SharedRecoveryReopeningTransport, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_reopening(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'reopen.json', record)
                if api._scope()[1] != record:
                    raise ValueError

            _, ready = api.observe()
            if not ready:
                return False
            if record['status'] == 'prepared':
                def intent() -> None:
                    if record['status'] != 'prepared':
                        raise ValueError
                    record['status'] = 'intent'
                    save()
                try:
                    accepted = api.release_staging(record_intent=intent)
                except Exception:
                    if record['status'] != 'intent':
                        raise
                    accepted = True  # Uncertain intent is observation-only, including no-dispatch interruptions.
                if accepted is False:
                    if record['status'] != 'prepared':
                        raise ValueError
                    return False
                if accepted is not True:
                    raise ValueError
            current, ready = api.observe()
            if current != 'open' or not ready:
                return False
            if record['status'] != 'released':
                record['status'] = 'released'
                save()
            current, ready = api.observe()
            if current != 'open' or not ready:
                return False
            validate_shared_recovery_reopening(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery reopening unconfirmed; preserve retained evidence') from None


def validate_shared_recovery_verification(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    if state_dir.name != _VERIFY or frame['recovery'][_REOPEN]['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_reopening(bindings=bindings, state_dir=state_dir.parent / _REOPEN, complete=True)
    record = _read(state_dir / 'verify.json')
    _validate_record(record, _identity(bindings, state_dir), complete=complete, prepared=prepared, verification=True)
    if ((prepared and frame['recovery'][_VERIFY]['status'] != 'prepared')
            or _frame(bindings, state_dir.parent) != frame):
        raise ValueError
    return record


def prepare_shared_recovery_verification(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'verify.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'status': 'prepared'})
        validate_shared_recovery_verification(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def advance_shared_recovery_verification(*, bindings: SharedPoolGuardBindings,
        api: SharedRecoveryReopeningTransport, state_dir: Path) -> bool:
    """Read current health/authority even if an earlier verification was saved."""
    try:
        if api.bindings != bindings or api.state != state_dir.parent / _REOPEN or state_dir.name != _VERIFY:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            validate_shared_recovery_verification(bindings=bindings, state_dir=state_dir, complete=False)
            files = _files(state_dir)
            current, ready = api.observe()
            if _files(state_dir) != files or _active(bindings, state_dir, 'started') != frame:
                raise ValueError
            if current != 'open' or not ready:
                return False
            private_state._atomic_json(state_dir / 'verify.json', {**_identity(bindings, state_dir), 'status': 'observed'})
            validate_shared_recovery_verification(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery terminal verification unconfirmed; preserve retained evidence') from None
