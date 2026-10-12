"""Fixed opening/release transactions for the shared parent's activation child.

The protected parent's callback supplies its current writer/provider checks.
This adapter additionally requires the real receipt-selected process, database
and capacity observations; a callback cannot substitute for runtime health.
There is no command-line entry point, caller SQL, write retry or recovery path.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_activation import (
    SharedForwardActivationView,
    shared_forward_activation_view,
)
from scripts.ops.nebius_shared_pool_forward_processes import (
    SharedPoolForwardProcessObservation,
    _gateway_history_target,
    observe_shared_pool_forward_processes,
)
from scripts.ops.nebius_shared_pool_gateway import shared_forward_gateway_view
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes


class SharedPoolActivationTransport:
    def __init__(self, *, bindings: SharedPoolGuardBindings, development: HTTPSSharedPoolRuntimeProbes,
            qualify: Callable[[], None]):
        self.bindings = copy.deepcopy(bindings)
        self.migration = self.bindings.migration
        self.development, self.qualify = development, qualify
        manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        self.child = self.state / 'forward-activate'
        self.prefix = tuple(development.prefix)
        self.executable = Path(self.prefix[0])
        self.kubeconfig = Path(self.prefix[self.prefix.index('--kubeconfig') + 1])
        before = self._scope()
        self.guards = KubectlPoolGuardAPI(request=self.migration, kubeconfig=self.kubeconfig, executable=self.executable)
        self.namespace = self.migration.registration.binding.namespace
        self.credential = self.guards._get('secret', 'loom-platform-db', self.namespace)
        target = _gateway_history_target(self.bindings, self.credential)
        self.history = KubectlPoolHistoryAPI(request=self.migration, target=target,
            kubeconfig=self.kubeconfig, executable=self.executable)
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared activation connection changed')

    def _scope(self) -> SharedForwardActivationView:
        if (self.migration != self.bindings.migration or self.development.shared_bindings != self.bindings
                or self.development.request != self.bindings.plan.development.runtime.request
                or tuple(self.development.prefix) != self.prefix or not callable(self.qualify)):
            raise ValueError('shared activation connection differs')
        self.development._private_inputs()
        view = shared_forward_activation_view(bindings=self.bindings, state_dir=self.state)
        if (view.frame['outcome'] != 'pending' or view.frame['forward']['forward-activate']['status'] != 'started'
                or view.guard_states is None):
            raise ValueError('shared activation child is not active')
        return view

    def verify_retained(self) -> None:
        before = self._scope()
        if (self.guards.request != self.migration or self.history.request != self.migration
                or self.guards._get('secret', 'loom-platform-db', self.namespace) != self.credential
                or _gateway_history_target(self.bindings, self.credential) != self.history.target
                or self._scope() != before):
            raise ValueError('shared activation retained authority changed')

    def _qualified_processes(self) -> SharedPoolForwardProcessObservation:
        before = self._scope()
        self.verify_retained()
        self.qualify()
        result = observe_shared_pool_forward_processes(bindings=self.bindings, state_dir=self.state,
            development=self.development, kubeconfig=self.kubeconfig, executable=self.executable)
        if result.bindings != self.bindings or result.pending_workloads or result.retired_authority != 'revoked':
            raise ValueError('shared activation runtime is not ready')
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared activation receipts changed during qualification')
        return result

    def qualify_shared_state(self) -> None:
        self._qualified_processes()

    def qualify_runtime(self) -> None:
        result = self._qualified_processes()
        if result.pool_state != 'closed' or set(result.guard_states.values()) != {'held'}:
            raise ValueError('shared opening requires closed capacity and held intake')

    def read_workload(self, key: str) -> dict[str, Any]:
        before = self._scope()
        original = self.bindings.plan.development.gateway_original
        if key != _key(original):
            raise ValueError('shared activation workload is outside the gateway')
        view = shared_forward_gateway_view(bindings=self.bindings, state_dir=self.state)
        current = self.guards._get('deployment', 'loom-pool-gateway', self.namespace)
        if (not any(_matches(current, row, _uid(original)) for row in view[2][key])
                or shared_forward_gateway_view(bindings=self.bindings, state_dir=self.state) != view
                or self._scope() != before):
            raise ValueError('shared activation gateway changed')
        return current

    def pool_state(self) -> str:
        before = self._scope()
        result = self.history.activation_pool('observe')
        if result not in before.pool_states or self._scope() != before:
            raise ValueError('shared activation pool state differs')
        return result

    def _guard(self, participant: str) -> PoolGuardTarget:
        target, = (row for row in self.migration.guards if str(row.participant_id) == participant)
        return target

    def guard_state(self, participant: str) -> str:
        before = self._scope()
        target = self._guard(participant)
        result = self.guards.activation_guard(target, 'observe')
        if (before.guard_states is None or result not in before.guard_states[target.namespace]
                or self._scope() != before):
            raise ValueError('shared activation guard state differs')
        return result

    def open_pool(self) -> None:
        before = self._scope()
        record = _read(self.child / 'activation.json')
        if record['opening'] != 'intent':
            raise ValueError('shared pool opening intent is required')
        self.qualify_runtime()
        original = self.bindings.plan.development.gateway_original
        expected = self.read_workload(_key(original))
        if self._scope() != before:
            raise ValueError('shared pool opening authority changed')
        self.history.open_pool(original=original, expected=expected)
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared pool opening result is unconfirmed')

    def release_guard(self, participant: str) -> None:
        before = self._scope()
        target = self._guard(participant)
        record = _read(self.child / 'activation.json')
        if (record['opening'] != 'opened'
                or record['guards'][participant] != {'release': 'intent', 'fence': 'prepared'}):
            raise ValueError('shared guard release intent is required')
        result = self._qualified_processes()
        if (result.pool_state != 'global' or result.guard_states[target.namespace] != 'held'
                or self._scope() != before):
            raise ValueError('shared guard release authority changed')
        if self.guards.activation_guard(target, 'release') != 'open':
            raise ValueError('shared guard release result is unconfirmed')
        self.verify_retained()
        if self._scope() != before:
            raise ValueError('shared guard release result is unconfirmed')

    def fence_pool(self) -> None:
        raise ValueError('shared recovery requires its own terminal authority')

    def fence_guard(self, participant: str) -> None:
        raise ValueError('shared recovery requires its own terminal authority')
