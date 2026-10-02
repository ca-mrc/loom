"""Activation/recovery on the closed parent's retained protected authority.

No public shortcut, new credentials, arbitrary SQL or automatic write retry.
The anchored activation journal owns ordering and all uncertain outcomes.
"""
from __future__ import annotations

import copy
from typing import Any

from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_switch import _stable
from scripts.ops.nebius_pool_activation_stage import activation_record
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_startup import _startup_record, closed_startup_documents
from scripts.ops.nebius_pool_startup_fence import (
    _fence_record,
    marked_startup_document,
    observe_recovery_workloads,
    startup_fence_patches,
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
            observe_recovery_workloads(self.request, self, state=self.state, anchor=self.anchor)
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

    def _recovery_fence(self) -> dict[str, Any]:
        _, startup = _startup_record(self.request, state=self.state, anchor=self.anchor,
            closed=self.closed, targets=self.targets)
        _, record = _fence_record(self.request, state=self.state, anchor=self.anchor,
            closed=self.closed, targets=self.targets, startup=startup)
        if (record is None or any(row['phase'] != 'fenced' for row in record['workloads'].values())
                or self.pool_state() != 'fenced' or any(self.guard_state(str(row.participant_id)) != 'fenced'
                    for row in self.request.fencing.retirement.migration.guards)):
            raise ValueError('pool_recovery_fence_unconfirmed')
        return record

    def recovery_drained(self) -> bool:
        """Fresh closed-intake drain across both journals, not shutdown authority."""
        try:
            self.verify_retained()
            before = self._recovery_fence()
            # Check every participant even when the global ledger is still busy.
            # No status is persisted, and no callback may mutate the journals.
            results = [self.parent.history.recovery_pool_drained()]
            results.extend(self.parent.guards.recovery_participant_drained(row)
                for row in self.request.fencing.retirement.migration.guards)
            self.verify_retained()
            if self._recovery_fence() != before or any(type(value) is not bool for value in results):
                raise ValueError
            return all(results)
        except Exception:
            raise ValueError('pool_recovery_drain_unconfirmed') from None

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

    def _startup_fence_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> bool:
        try:
            self._scope()
            _, startup = _startup_record(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets)
            _, record = _fence_record(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets, startup=startup)
            if (startup is None or record is None or key not in record['workloads']
                    or record['workloads'][key] != {'phase': 'prepared' if preview else 'intent', 'expected': None}
                    or startup['workloads'][key]['phase'] != 'intent'
                    or before['metadata']['resourceVersion'] != startup['workloads'][key]['before_resource_version']):
                raise ValueError
            operation = self.request.fencing.retirement.migration.registration.spec.operation_id
            if _stable(desired) != _stable(marked_startup_document(self.closed[key], operation)):
                raise ValueError
            patches = startup_fence_patches(self.closed[key], before, operation)
            self.verify_retained()
            if self.pool_state() != 'fenced' or any(self.guard_state(str(row.participant_id)) != 'fenced'
                    for row in self.request.fencing.retirement.migration.guards):
                raise ValueError
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(self.closed[key]))
        except Exception:
            raise ValueError('pool_startup_fence_update_unconfirmed') from None

    def preview_startup_fence(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._startup_fence_patch(key, before, desired, preview=True) else None

    def fence_startup(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._startup_fence_patch(key, before, desired, preview=False)
