"""Journaled recovery-owner release; successor admission remains terminally fenced."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_legacy_restart import PoolLegacyRestartAPI, _restart_record
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_shutdown import _shutdown_record
from scripts.ops.nebius_pool_startup_fence import observe_recovery_workloads


class PoolLegacyReopeningAPI(PoolLegacyRestartAPI, Protocol):
    def pool_recovery_drained(self) -> bool: ...
    def participant_recovery_drained(self, participant: str) -> bool: ...
    def qualify_legacy_runtimes(self) -> None: ...
    def qualify_reopening_runtimes(self) -> None: ...
    def release_recovery_guard(self, participant: str) -> None: ...


def _reopening_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                      ) -> tuple[dict[str, Any], dict[str, Any] | None]:
    restart = _restart_record(request, state=state, anchor=anchor)[-1]
    if restart is None or any(row['phase'] != 'started' for row in restart['workloads'].values()):
        raise ValueError('pool_legacy_reopening_completed_restart_required')
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    participants = tuple(str(row.participant_id) for row in request.fencing.retirement.migration.guards)
    identity = {'schema': 'loom.nebius-pool-legacy-reopening.v1', 'operation_id': operation,
        'state_dir': str(state), 'restart_sha256': _hash(state / 'legacy-restart.json')}
    path, marker = state / 'legacy-reopening.json', anchor / (operation + '-legacy-reopening.json')
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=512 * 1024))
    if (not isinstance(record, dict) or set(record) != {*identity, 'guards'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['guards'], dict) or set(record['guards']) != set(participants)):
        raise ValueError
    # One ordered writer: a released prefix, at most one uncertain dispatch,
    # and the untouched suffix. File serialization order is not dispatch order.
    remaining = False
    for key in participants:
        phase = record['guards'][key]
        if phase not in ('prepared', 'intent', 'released') or (remaining and phase != 'prepared'):
            raise ValueError
        remaining = phase != 'released'
    return identity, record


def observe_legacy_reopening(request: PoolCutoverRequest, api: PoolLegacyReopeningAPI, *, state: Path, anchor: Path
                            ) -> tuple[dict[str, Any] | None, dict[str, str]]:
    """Only anchored intent can account for an already-open participant."""
    _, record = _reopening_record(request, state=state, anchor=anchor)
    api.verify_retained()
    observe_recovery_workloads(request, api, state=state, anchor=anchor)
    if api.pool_state() != 'fenced' or api.machine_authority() != 'revoked':
        raise ValueError
    states = {}
    for target in request.fencing.retirement.migration.guards:
        key = str(target.participant_id)
        phase = 'prepared' if record is None else record['guards'][key]
        status = api.guard_state(key)
        if status not in {'prepared': ('fenced',), 'intent': ('fenced', 'open'), 'released': ('open',)}[phase]:
            raise ValueError
        states[key] = status
    if _reopening_record(request, state=state, anchor=anchor)[1] != record:
        raise ValueError
    return record, states


def qualify_legacy_reopening(request: PoolCutoverRequest, api: PoolLegacyReopeningAPI, *, state: Path, anchor: Path) -> str | None:
    """Opened owners may work; only still-fenced local journals must be idle."""
    record, states = observe_legacy_reopening(request, api, state=state, anchor=anchor)

    def drained(current: dict[str, str]) -> bool:
        values = [api.pool_recovery_drained()]
        values.extend(api.participant_recovery_drained(key) for key, status in current.items() if status == 'fenced')
        if any(type(value) is not bool for value in values):
            raise ValueError
        return all(values)

    if not drained(states):
        return 'pending_pool_cleanup'
    api.qualify_legacy_roles()
    api.qualify_gateway_readonly()
    gateway = 'Deployment:' + request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
    stopped = _shutdown_record(request, state=state, anchor=anchor)[2]
    absent = api.successor_drained(gateway, stopped[gateway])
    if type(absent) is not bool:
        raise ValueError
    if not absent:
        return 'pending_successor_drain'
    after, states = observe_legacy_reopening(request, api, state=state, anchor=anchor)
    if after != record:
        raise ValueError
    return None if drained(states) else 'pending_pool_cleanup'


def reopen_pool_legacy(*, request: PoolCutoverRequest, api: PoolLegacyReopeningAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Resume this operation only; lost release replies are never redispatched."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            identity, record = _reopening_record(request, state=state, anchor=anchor)
            participants = tuple(str(row.participant_id) for row in request.fencing.retirement.migration.guards)

            def observe() -> dict[str, str]:
                saved, states = observe_legacy_reopening(request, api, state=state, anchor=anchor)
                if saved != record:
                    raise ValueError
                return states

            def qualify() -> str | None:
                pending = qualify_legacy_reopening(request, api, state=state, anchor=anchor)
                observe()
                return pending

            def result(status: str) -> dict[str, Any]:
                states = observe()
                return {'status': status, 'operation_id': identity['operation_id'],
                    'legacy_admission_open': all(value == 'open' for value in states.values()), 'global_admission_open': False}

            def save() -> None:
                if record is None:
                    raise ValueError
                private_state._atomic_json(state / 'legacy-reopening.json', record)

            pending = qualify()
            if pending is not None:
                return result(pending)
            if record is None:
                api.qualify_legacy_runtimes()
                observe()
                record = {**identity, 'guards': dict.fromkeys(participants, 'prepared')}
                private_state._atomic_json(anchor / (identity['operation_id'] + '-legacy-reopening.json'), identity)
                save()
            for key in participants:
                if record['guards'][key] == 'released':
                    continue
                pending = qualify()
                if pending is not None:
                    return result(pending)
                if record['guards'][key] == 'prepared':
                    api.qualify_reopening_runtimes()
                    pending = qualify()
                    if pending is not None:
                        return result(pending)
                    record['guards'][key] = 'intent'
                    save()
                    try:
                        api.release_recovery_guard(key)
                    except Exception:
                        pass  # Only exact observation can settle this dispatch.
                if observe()[key] != 'open':
                    return result('pending_legacy_guard_release')
                record['guards'][key] = 'released'
                save()
            api.qualify_reopening_runtimes()
            pending = qualify()
            return result(pending if pending is not None else 'pool_legacy_reopened')
    except Exception:
        raise ValueError('pool_legacy_reopening_unconfirmed_preserve_evidence') from None
