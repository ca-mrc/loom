"""Fixed terminal fencing transactions for the shared-pool recovery child.

The protected parent supplies retained writer/provider qualification, not an
operator callback. Database targets and transaction choices come only from the
retained installation and frozen forward history. No healthy gateway, idle work
journal, arbitrary SQL, write retry or command-line entry point is required.
"""
from __future__ import annotations

import copy
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import _files
from scripts.ops.nebius_shared_pool_recovery_fence import (
    _active,
    _entry_choices,
    validate_shared_recovery_fence,
)


class SharedPoolRecoveryFenceTransport:
    def __init__(self, *, bindings: SharedPoolGuardBindings, kubeconfig: Path,
            executable: Path, qualify: Callable[[], None]):
        try:
            self.bindings = copy.deepcopy(bindings)
            self.migration = self.bindings.migration
            self.kubeconfig, self.executable, self.qualify = kubeconfig, executable, qualify
            self.kubeconfig_sha256 = hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest()
            manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
            self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
            self.child = self.state / 'recovery-fence'
            before = self._scope()
            self.entry_pool, self.entry_guards = _entry_choices(self.bindings, self.state)
            self.guards = KubectlPoolGuardAPI(request=self.migration, kubeconfig=kubeconfig, executable=executable)
            self.prefix = tuple(self.guards.prefix)
            self.namespace = self.migration.registration.binding.namespace
            self.credential = self.guards._get('secret', 'loom-platform-db', self.namespace)
            target = _gateway_history_target(self.bindings, self.credential)
            self.history = KubectlPoolHistoryAPI(request=self.migration, target=target,
                kubeconfig=kubeconfig, executable=executable)
            self.verify_retained()
            if self._scope() != before:
                raise ValueError
        except Exception:
            raise ValueError('shared recovery fence connection unqualified') from None

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        if (self.migration != self.bindings.migration or not callable(self.qualify)
                or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
            raise ValueError('shared recovery fence connection differs')
        frame = _active(self.bindings, self.child, 'started')
        files = _files(self.child)
        record = validate_shared_recovery_fence(bindings=self.bindings, state_dir=self.child, complete=False)
        if _files(self.child) != files:
            raise ValueError('shared recovery fence receipts changed')
        return frame, record, files

    def _connection(self) -> None:
        if (self.guards.request != self.migration or self.history.request != self.migration
                or tuple(self.guards.prefix) != self.prefix or tuple(self.history.prefix) != self.prefix
                or self.guards.kubeconfig != self.kubeconfig or self.history.kubeconfig != self.kubeconfig
                or self.guards.kubeconfig_sha256 != self.kubeconfig_sha256 or self.history.kubeconfig_sha256 != self.kubeconfig_sha256
                or self.guards._get('secret', 'loom-platform-db', self.namespace) != self.credential
                or _gateway_history_target(self.bindings, self.credential) != self.history.target):
            raise ValueError('shared recovery retained authority changed')

    def verify_retained(self) -> None:
        before = self._scope()
        self._connection()
        self.qualify()
        self._connection()
        if self._scope() != before:
            raise ValueError('shared recovery fence authority changed during qualification')

    @staticmethod
    def _choices(status: str, original: tuple[str, ...]) -> tuple[str, ...]:
        return ('fenced',) if status == 'fenced' else (*original, 'fenced') if status == 'intent' else original

    def pool_state(self) -> str:
        before = self._scope()
        self._connection()
        result = self.history.activation_pool('observe')
        self._connection()
        if result not in self._choices(before[1]['pool'], self.entry_pool) or self._scope() != before:
            raise ValueError('shared recovery pool state differs')
        return result

    def _guard(self, participant: str) -> PoolGuardTarget:
        target, = (row for row in self.migration.guards if str(row.participant_id) == participant)
        return target

    def guard_state(self, participant: str) -> str:
        before = self._scope()
        self._connection()
        result = self.guards.activation_guard(self._guard(participant), 'observe')
        self._connection()
        if (result not in self._choices(before[1]['guards'][participant], self.entry_guards[participant])
                or self._scope() != before):
            raise ValueError('shared recovery guard state differs')
        return result

    def fence_pool(self) -> None:
        before = self._scope()
        if before[1]['pool'] != 'intent':
            raise ValueError('shared recovery pool fencing intent is required')
        self.verify_retained()
        if self.pool_state() not in self.entry_pool or self._scope() != before:
            raise ValueError('shared recovery pool fencing authority changed')
        if self.history.activation_pool('fence') != 'fenced':
            raise ValueError('shared recovery pool fencing result is unconfirmed')
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared recovery pool fencing result is unconfirmed')

    def fence_guard(self, participant: str) -> None:
        before = self._scope()
        target = self._guard(participant)
        if before[1]['pool'] != 'fenced' or before[1]['guards'][participant] != 'intent':
            raise ValueError('shared recovery local fencing intent is required')
        self.verify_retained()
        if (self.pool_state() != 'fenced' or self.guard_state(participant) not in self.entry_guards[participant]
                or self._scope() != before):
            raise ValueError('shared recovery local fencing authority changed')
        if self.guards.activation_guard(target, 'fence') != 'fenced':
            raise ValueError('shared recovery local fencing result is unconfirmed')
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared recovery local fencing result is unconfirmed')
