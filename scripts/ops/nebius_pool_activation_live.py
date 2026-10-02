"""Activation/recovery on the closed parent's retained protected authority.

No public shortcut, new credentials, arbitrary SQL or automatic write retry.
The anchored activation journal owns ordering and all uncertain outcomes.
"""
from __future__ import annotations

from typing import Any

from scripts.ops.nebius_pool_activation_stage import activation_record
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_startup import (
    _observe_workloads,
    _startup_record,
    closed_startup_documents,
)
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI


class HTTPSPoolActivationAPI(HTTPSPoolStartupAPI):
    def verify_retained(self) -> None:
        """Recover with active work or unhealthy successors, never weaker scope."""
        try:
            self._scope()
            if closed_startup_documents(self.request, state_dir=self.state, anchor_dir=self.anchor) != (self.closed, self.targets):
                raise ValueError
            parent = self.parent
            migration = self.request.fencing.retirement.migration
            if parent.guards.request != migration:
                raise ValueError
            parent.history.qualify_binding(migration, self.request.manager)
            parent.qualify_writer_bindings()
            # Do not call parent.preflight: its initial idle/backlog checks are
            # invalid after intake opens. These retained entry checks still
            # qualify private inputs, exact backends and physical provider scope.
            parent.checks.preflight(self.request)
            self._qualify_retained_resources()
            for guard in migration.guards:
                if parent.guards.runtime_role(guard, 'inspect') != {'status': 'qualified'}:
                    raise ValueError
            _, startup = _startup_record(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets)
            _observe_workloads(self, self.closed, self.targets, startup or {'workloads': {}})
            parent.qualify_writer_bindings()
            self._scope()
        except Exception:
            raise ValueError('pool_activation_retained_scope_unqualified') from None

    def qualify_runtime(self) -> None:
        self.qualify_database_runtimes()
        self.qualify_gateway_authority()

    def pool_state(self) -> str:
        self._scope()
        return self.parent.history.activation_pool('observe')

    def _guard(self, participant: str) -> PoolGuardTarget:
        self._scope()
        target, = (row for row in self.request.fencing.retirement.migration.guards if str(row.participant_id) == participant)
        return target

    def guard_state(self, participant: str) -> str:
        return self.parent.guards.activation_guard(self._guard(participant), 'observe')

    def _write_record(self) -> dict[str, Any]:
        record = activation_record(self.request, state_dir=self.state, anchor_dir=self.anchor)
        if record is None:
            raise ValueError('pool_activation_intent_required')
        self.verify_retained()
        return record

    def _post_write(self, record: dict[str, Any]) -> None:
        self.verify_retained()
        if activation_record(self.request, state_dir=self.state, anchor_dir=self.anchor) != record:
            raise ValueError('pool_activation_intent_changed')

    def open_pool(self) -> None:
        record = self._write_record()
        if record['opening'] != 'intent' or record['cancellation'] != 'prepared':
            raise ValueError('pool_activation_open_intent_required')
        self.qualify_runtime()
        expected = self._started_workloads()
        key = 'Deployment:' + self.request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
        self.parent.history.open_pool(original=self.closed[key], expected=expected[key])
        self._post_write(record)

    def fence_pool(self) -> None:
        record = self._write_record()
        if record['cancellation'] != 'intent' or self.pool_state() not in {'closed', 'global'}:
            raise ValueError('pool_activation_fence_intent_required')
        if self.parent.history.activation_pool('fence') != 'fenced':
            raise ValueError('pool_activation_fence_unconfirmed')
        self._post_write(record)

    def release_guard(self, participant: str) -> None:
        record, target = self._write_record(), self._guard(participant)
        item = record['guards'][participant]
        if (record['opening'] != 'opened' or record['cancellation'] != 'prepared'
                or item != {'release': 'intent', 'fence': 'prepared'}
                or self.pool_state() != 'global' or self.guard_state(participant) != 'held'):
            raise ValueError('pool_activation_release_intent_required')
        if self.parent.guards.activation_guard(target, 'release') != 'open':
            raise ValueError('pool_activation_release_unconfirmed')
        self._post_write(record)

    def fence_guard(self, participant: str) -> None:
        record, target = self._write_record(), self._guard(participant)
        if (record['cancellation'] != 'fenced' or record['guards'][participant]['fence'] != 'intent'
                or self.pool_state() != 'fenced' or self.guard_state(participant) not in {'held', 'open'}):
            raise ValueError('pool_activation_guard_fence_intent_required')
        if self.parent.guards.activation_guard(target, 'fence') != 'fenced':
            raise ValueError('pool_activation_guard_fence_unconfirmed')
        self._post_write(record)
