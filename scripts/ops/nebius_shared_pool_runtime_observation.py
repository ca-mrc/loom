"""Existing runtime probes with only anchored shared-handoff successors.

Nested settings/database probes call inspect themselves. Bind that inventory
here too; accepting the namespace only in an outer observer is insufficient.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_shared_pool_activation import shared_forward_activation_view
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import _files, _history


def shared_forward_runtime_view(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> tuple[dict[str, Any], dict[str, dict[str, str]], dict[str, tuple[dict[str, Any], ...]]]:
    """Bind both dev successors without widening the dev reader to staging.

    Live writer authority is checked by the separate revision-bound census;
    this inventory retains only the namespace and gateway workload projections.
    All relevant receipt bytes and the shared parent bracket remote reads.
    """
    from scripts.ops.nebius_shared_pool_build_activation import shared_forward_build_namespace_view
    from scripts.ops.nebius_shared_pool_gateway import shared_forward_gateway_view

    opening = shared_forward_activation_view(bindings=bindings, state_dir=state_dir)
    build = shared_forward_build_namespace_view(bindings=bindings, state_dir=state_dir)
    gateway = shared_forward_gateway_view(bindings=bindings, state_dir=state_dir)
    if (build[0] != gateway[0] or build[0] != opening.frame or build[2].keys() & gateway[2].keys()
            or shared_forward_build_namespace_view(bindings=bindings, state_dir=state_dir) != build
            or shared_forward_activation_view(bindings=bindings, state_dir=state_dir) != opening):
        raise ValueError('shared runtime successor history changed')
    return build[0], {'forward-build-policy': build[1], 'forward-gateway': gateway[1],
        'forward-activate': opening.files}, {**build[2], **gateway[2]}


def shared_recovery_runtime_view(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> tuple[dict[str, Any], dict[str, dict[str, str]], dict[str, tuple[dict[str, Any], ...]]]:
    """Project only retained development roots under actual recovery receipts.

    The build namespace keeps its frozen forward choice. Recovery changes only
    the recorded development workloads; staging projections never enter this
    reader. This inventory does not attest readiness or writer exclusivity.
    """
    from scripts.ops.nebius_shared_pool_build_activation import shared_build_namespace_history_view
    from scripts.ops.nebius_shared_pool_recovery_census import _receipt_files, _recovery_options
    from scripts.ops.nebius_shared_pool_recovery_fence import _recovery_frame

    frame, roots, _, receipts = _recovery_options(bindings, state_dir)
    build = shared_build_namespace_history_view(bindings=bindings, state_dir=state_dir)
    completed = bindings.plan.development.runtime
    keys = {*completed.plan.fixed['workloads'], *completed.plan.originals,
        _key(bindings.plan.development.gateway_original)}
    if (build[0] != frame or build[1] != receipts.get('forward-build-policy', {})
            or not keys <= roots.keys() or keys & build[2].keys()):
        raise ValueError('shared recovery runtime history differs')
    workloads = {}
    for key in keys:
        options = roots[key]
        if any(value is None for value in options):
            raise ValueError('shared recovery runtime root is absent')
        workloads[key] = tuple(value for value in options if value is not None)
    if _recovery_frame(bindings, state_dir) != frame or _receipt_files(state_dir) != receipts:
        raise ValueError('shared recovery runtime history changed')
    return frame, receipts, {**workloads, **build[2]}


def shared_runtime_view(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> tuple[dict[str, Any], dict[str, dict[str, str]], dict[str, tuple[dict[str, Any], ...]]]:
    """Select direction from its verified anchor, never from a caller flag."""
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    if recovery.exists() or recovery.is_symlink():
        return shared_recovery_runtime_view(bindings=bindings, state_dir=state_dir)
    return shared_forward_runtime_view(bindings=bindings, state_dir=state_dir)


@dataclass(frozen=True, repr=False)
class _ProcessObservation:
    bindings: SharedPoolGuardBindings
    state: Path
    inventory: dict[str, dict[str, Any]]
    receipts: dict[str, dict[str, str]]


class HTTPSSharedPoolRuntimeProbes(HTTPSDevelopmentRuntimeProbes):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str,
            ssl_context: ssl.SSLContext, token: str,
            private_files: dict[Path, bytes] | None = None):
        self.shared_bindings = copy.deepcopy(bindings)
        self._process_observation: _ProcessObservation | None = None
        super().__init__(request=self.shared_bindings.plan.development.runtime.request,
            api_server=api_server, ssl_context=ssl_context, token=token, private_files=private_files)

    def _process_receipts(self, bindings: SharedPoolGuardBindings, state: Path) -> dict[str, dict[str, str]]:
        completed = bindings.plan.development.runtime
        manager = completed.request.database.manager.retained.request.retained
        if (self.shared_bindings != bindings or self.request != completed.request or self.plan != completed.plan
                or state != completed.state_dir
                or self.api_server.rstrip('/') != completed.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared process observation binding changed')
        self._private_inputs()
        _history(bindings)
        # Re-read bytes, ownership and paths, not recursively reconstruct every
        # completed ancestor for each process. No remote state is cached here.
        return {name: _files(path) for name, path in (
            ('runtime', state), ('shared', Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'),
            ('anchors', Path(manager.operation['anchor_dir'])))}

    @contextmanager
    def process_observation(self, *, state_dir: Path) -> Iterator[None]:
        """Full inventory barriers around fresh, targeted nested process probes.

        The scope is read-only and cannot span a mutation or another observation.
        Current Deployment, ReplicaSet, Pod, Secret and backend checks still run
        at each challenge. Only unchanged local history projections are reused.
        """
        if self._process_observation is not None:
            raise ValueError('shared process observation is already active')
        bindings = copy.deepcopy(self.shared_bindings)
        receipts = self._process_receipts(bindings, state_dir)
        before = self.inspect(state_dir=state_dir)
        if self._process_receipts(bindings, state_dir) != receipts:
            raise ValueError('shared process observation history changed')
        self._process_observation = _ProcessObservation(bindings, state_dir, copy.deepcopy(before), receipts)
        try:
            yield
            after = self.inspect(state_dir=state_dir)
            if (after.keys() != before.keys()
                    or any(not _matches(after[key], original, _uid(original))
                        or after[key]['metadata'].get('generation') != original['metadata'].get('generation')
                        for key, original in before.items())
                    or self._process_receipts(bindings, state_dir) != receipts):
                raise ValueError('shared process observation changed')
        finally:
            self._process_observation = None

    def _running_workload(self, key: str, state_dir: Path) -> dict[str, Any]:
        view = self._process_observation
        if view is None:
            return super()._running_workload(key, state_dir)
        if state_dir != view.state or self._process_receipts(view.bindings, view.state) != view.receipts:
            raise ValueError('shared process observation history changed')
        original = view.inventory[key]
        current = self._get(original)
        if (not _matches(current, original, _uid(original))
                or current['metadata'].get('generation') != original['metadata'].get('generation')
                or self._process_receipts(view.bindings, view.state) != view.receipts):
            raise ValueError('shared process observation workload changed')
        return current

    def inspect(self, *, state_dir: Path, _prechild_phase: str | None = None,
            _expected_record: dict[str, Any] | None = None,
            _shared_handoff: SharedPoolGuardBindings | None = None) -> dict[str, dict[str, Any]]:
        if _shared_handoff is not None and _shared_handoff != self.shared_bindings:
            raise ValueError('shared runtime observation binding differs')
        return super().inspect(state_dir=state_dir, _prechild_phase=_prechild_phase,
            _expected_record=_expected_record, _shared_handoff=self.shared_bindings)
