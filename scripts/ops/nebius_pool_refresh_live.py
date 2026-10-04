"""Read-only retained-pool qualification around an exact manager refresh.

Open owner work is not a reason to drain or close admission. Completed rollback
instead retains a fenced, drained global ledger and a stopped read-only gateway.
No method here changes workloads, permissions, credentials or admission.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_gateway_authority import (
    gateway_review_namespaces,
    review_gateway_rules,
)
from scripts.ops.nebius_pool_gateway_retirement import _gateway_record
from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI


class HTTPSPoolRefreshAPI:
    """One fixed refresh binding and the existing separately scoped pool reader."""

    def __init__(self, *, parent: HTTPSPoolCutoverAPI):
        if type(parent.refresh) is not PoolManagerRefresh:
            raise ValueError('pool_refresh_binding_required')
        self.parent, self.refresh = parent, parent.refresh
        self.pool = self.refresh.qualify()
        self.retained = HTTPSPoolStartupAPI(parent=parent)
        self._scope()

    def _scope(self) -> None:
        context = self.pool.context
        if (self.parent.refresh is not self.refresh or self.refresh.qualify() != self.pool
                or self.parent.request != context.request
                or self.parent.state_dir != Path(context.operation['state_dir'])
                or self.parent.anchor_dir != Path(context.operation['anchor_dir'])
                or self.parent.guards.request != context.request.fencing.retirement.migration):
            raise ValueError('pool_refresh_scope_changed')
        self.retained._scope()

    def _workloads(self, options: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, dict[str, Any]]:
        if set(options) != set(self.retained.closed):
            raise ValueError
        observed = {}
        for key, choices in options.items():
            current = self.retained.read_workload(key)
            if not any(_matches(current, wanted, _uid(self.pool.completion.workloads[key])) for wanted in choices):
                raise ValueError
            observed[key] = _stable(current)
        return observed

    def _common(self) -> dict[str, dict[str, Any]]:
        """Exact backends, physical scope, full writer inventory and pool material."""
        self._scope()
        parent, request = self.parent, self.parent.request
        options = self.refresh.workload_options()
        before = self._workloads(options)
        parent.history.qualify_binding(request.fencing.retirement.migration, request.manager)
        parent.qualify_writer_bindings()
        # Initial cutover preflight also requires idle local journals. The entry
        # check retains its private inputs/backend/provider proof without that
        # closed-intake condition, which is invalid after either reopening.
        parent.checks.preflight(request)
        self.retained._qualify_retained_resources()
        for guard in request.fencing.retirement.migration.guards:
            if parent.guards.runtime_role(guard, 'inspect') != {'status': 'qualified'}:
                raise ValueError
        if self._workloads(options) != before or self.refresh.workload_options() != options:
            raise ValueError
        self._scope()
        return before

    def _admission(self) -> None:
        history = self.parent.history
        if self.pool.completion.outcome == 'global':
            history.qualify_active_pool()
        else:
            if (history.activation_pool('observe') != 'fenced'
                    or history.machine_retirement('observe') != 'revoked'
                    or history.recovery_pool_drained() is not True):
                raise ValueError
        for guard in self.parent.request.fencing.retirement.migration.guards:
            if self.parent.guards.activation_guard(guard, 'observe') != 'open':
                raise ValueError

    def _outcome(self) -> None:
        self._admission()
        parent, request = self.parent, self.parent.request
        migration = request.fencing.retirement.migration
        authority = parent.catalog['authority']
        if self.pool.completion.outcome == 'legacy':
            originals, targets, _, record = _gateway_record(request, state=self.retained.state, anchor=self.retained.anchor)
            if record is None or any(item['phase'] != 'restricted' for item in record['roles'].values()):
                raise ValueError
            authority = list({**originals, **targets}.values())
            gateway = 'Deployment:' + migration.registration.binding.namespace + ':loom-pool-gateway'
            # Only the gateway must still be stopped. Reopened legacy owners
            # may already be executing work; never invoke their drain probes.
            if not HTTPSPoolActivationAPI(parent=parent).successor_drained(gateway, self.pool.completion.workloads[gateway]):
                raise ValueError
        bindings = [row for resource, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
            for row in inventory_resources(parent._request, 'rbac.authorization.k8s.io/v1', resource, kind)]
        for namespace in gateway_review_namespaces(migration, bindings):
            review_gateway_rules(parent.client, manager_namespace=migration.registration.binding.namespace,
                namespace=namespace, authority=authority)
        self._admission()

    def qualify(self) -> None:
        """Fresh observation only; this result is never a reusable write permit."""
        try:
            before = self._common()
            self._outcome()
            if self._common() != before:
                raise ValueError
        except Exception:
            raise ValueError('pool_refresh_live_unqualified') from None
