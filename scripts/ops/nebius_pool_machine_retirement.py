"""Anchor one machine-authority revocation after fresh successor shutdown proof.

This stage does not retire Kubernetes roles or restore any legacy authority.
Unknown transaction replies only observe the same exact retained identities.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_machine_database import MachineRetirementState
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_shutdown import PoolShutdownAPI, _shutdown_record


class PoolMachineRetirementAPI(PoolShutdownAPI, Protocol):
    def machine_authority(self) -> MachineRetirementState: ...
    def retire_machines(self) -> None: ...


def _machine_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
    _, _, targets, _, shutdown = _shutdown_record(request, state=state, anchor=anchor)
    return _read_machine_record(request, state=state, anchor=anchor, targets=targets, shutdown=shutdown)


def _read_machine_record(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                         targets: dict[str, dict[str, Any]], shutdown: dict[str, Any] | None
                         ) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
    """Read the current machine journal after the same call's shutdown read."""
    if shutdown is None or any(row['phase'] != 'stopped' for row in shutdown['workloads'].values()):
        raise ValueError('pool_machine_retirement_shutdown_required')
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-machine-retirement.v1', 'operation_id': operation,
        'state_dir': str(state), 'shutdown_sha256': _hash(state / 'shutdown.json')}
    path, marker = state / 'machine-retirement.json', anchor / (operation + '-machine-retirement.json')
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return targets, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path))
    if (not isinstance(record, dict) or set(record) != {*identity, 'phase'}
            or any(record[key] != value for key, value in identity.items())
            or record['phase'] not in {'prepared', 'intent', 'revoked'}):
        raise ValueError
    return targets, identity, record


def qualify_machine_retirement_drain(request: PoolCutoverRequest, api: PoolShutdownAPI, *,
                                     state: Path, anchor: Path) -> str | None:
    """Fresh nonpersisted barriers, shared by the stage and protected dispatch."""
    targets, _, _ = _machine_record(request, state=state, anchor=anchor)

    def journals() -> bool:
        drained = api.recovery_drained()
        # Detect authority drift during the ledger reads before this boundary
        # can permit retirement; the drain reader itself only proves quiescence.
        api.verify_retained()
        if (api.pool_state() != 'fenced' or any(api.guard_state(str(row.participant_id)) != 'fenced'
                for row in request.fencing.retirement.migration.guards)):
            raise ValueError
        if type(drained) is not bool:
            raise ValueError
        return drained

    if not journals():
        return 'pending_pool_cleanup'
    for key, desired in targets.items():
        drained = api.successor_drained(key, desired)
        if type(drained) is not bool:
            raise ValueError
        if not drained:
            return 'pending_successor_drain'
    return None if journals() else 'pending_pool_cleanup'


def retire_pool_machines(*, request: PoolCutoverRequest, api: PoolMachineRetirementAPI,
                          state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            _, identity, record = _machine_record(request, state=state, anchor=anchor)
            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            pending = qualify_machine_retirement_drain(request, api, state=state, anchor=anchor)
            if pending is not None:
                return result(pending)
            current = api.machine_authority()
            if current not in ('active', 'revoked'):
                raise ValueError
            path = state / 'machine-retirement.json'
            if record is None:
                if current != 'active':
                    raise ValueError
                record = {**identity, 'phase': 'prepared'}
                private_state._atomic_json(anchor / (identity['operation_id'] + '-machine-retirement.json'), identity)
                private_state._atomic_json(path, record)
            if record['phase'] == 'prepared':
                if current != 'active':
                    raise ValueError
                record['phase'] = 'intent'
                private_state._atomic_json(path, record)
                try:
                    api.retire_machines()
                except Exception:
                    pass  # Retain the intent and observe; never resend it.
                current = api.machine_authority()
            if current != 'revoked':
                if record['phase'] == 'intent' and current == 'active':
                    return result('pending_machine_retirement')
                raise ValueError
            if record['phase'] != 'revoked':
                record['phase'] = 'revoked'
                private_state._atomic_json(path, record)
            pending = qualify_machine_retirement_drain(request, api, state=state, anchor=anchor)
            if pending is not None:
                return result(pending)
            if api.machine_authority() != 'revoked':
                raise ValueError
            return result('pool_machines_retired')
    except Exception:
        raise ValueError('pool_machine_retirement_unconfirmed_preserve_evidence') from None
