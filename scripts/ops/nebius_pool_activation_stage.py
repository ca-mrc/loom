"""Anchored one-dispatch opening, local release and terminal cancellation.

The protected parent supplies fixed transports. This internal stage is not a
standalone deployment command. Cancellation leaves successors and charged work
in place; it never grants permission to restore legacy writers.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup import (
    _startup_record,
    closed_startup_documents,
)


class PoolActivationAPI(Protocol):
    def verify_retained(self) -> None:
        """Qualify fixed inputs, backends, restricted authority and retained roots, not health."""
        ...
    def read_workload(self, key: str) -> dict[str, Any]: ...
    def qualify_runtime(self) -> None:
        """Fresh closed-mode runtime, gateway-authority and capacity qualification."""
        ...
    def pool_state(self) -> str: ...
    def guard_state(self, participant: str) -> str: ...
    def open_pool(self) -> None: ...
    def fence_pool(self) -> None: ...
    def release_guard(self, participant: str) -> None: ...
    def fence_guard(self, participant: str) -> None: ...


def _startup_hash(state: Path) -> str | None:
    path = state / 'startup.json'
    return _hash(path) if path.exists() or path.is_symlink() else None


def _activation_record(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> tuple[dict[str, Any], dict[str, Any] | None]:
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-activation.v1', 'operation_id': operation,
        'state_dir': str(state), 'closure_sha256': _hash(state / 'cutover.json'), 'startup_sha256': _startup_hash(state)}
    marker, path = anchor / (operation + '-activation.json'), state / 'activation.json'
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=512 * 1024))
    guards = {str(row.participant_id) for row in request.fencing.retirement.migration.guards}
    if (not isinstance(record, dict) or set(record) != {*identity, 'opening', 'cancellation', 'guards'}
            or any(record[key] != value for key, value in identity.items())
            or record['opening'] not in {'prepared', 'intent', 'opened'}
            or record['cancellation'] not in {'prepared', 'intent', 'fenced'}
            or not isinstance(record['guards'], dict) or set(record['guards']) != guards):
        raise ValueError
    for item in record['guards'].values():
        if (not isinstance(item, dict) or set(item) != {'release', 'fence'}
                or item['release'] not in {'prepared', 'intent', 'released'}
                or item['fence'] not in {'prepared', 'intent', 'fenced'}
                or (item['release'] != 'prepared' and record['opening'] != 'opened')
                or (item['fence'] != 'prepared' and record['cancellation'] != 'fenced')):
            raise ValueError
    return identity, record


def activation_record(request: PoolCutoverRequest, *, state_dir: Path, anchor_dir: Path) -> dict[str, Any] | None:
    """Validated phase evidence for fixed read-only recovery transports."""
    closed, targets = closed_startup_documents(request, state_dir=state_dir, anchor_dir=anchor_dir)
    _startup_record(request, state=state_dir, anchor=anchor_dir, closed=closed, targets=targets)
    return _activation_record(request, state=state_dir, anchor=anchor_dir)[1]


def _pool_choices(record: dict[str, Any]) -> tuple[str, ...]:
    if record['cancellation'] == 'fenced':
        return ('fenced',)
    if record['cancellation'] == 'intent':
        return ('closed', 'global', 'fenced') if record['opening'] == 'intent' else (
            ('global', 'fenced') if record['opening'] == 'opened' else ('closed', 'fenced'))
    return {'prepared': ('closed',), 'intent': ('closed', 'global'), 'opened': ('global',)}[record['opening']]


def _guard_choices(item: dict[str, str]) -> tuple[str, ...]:
    if item['fence'] == 'fenced':
        return ('fenced',)
    original = {'prepared': ('held',), 'intent': ('held', 'open'), 'released': ('open',)}[item['release']]
    return (*original, 'fenced') if item['fence'] == 'intent' else original


def advance_pool_activation(*, request: PoolCutoverRequest, api: PoolActivationAPI,
                            state_dir: Path, anchor_dir: Path, cancel: bool = False) -> dict[str, Any]:
    """Advance fixed opening or cancellation; uncertain outcomes are readback-only."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            from scripts.ops.nebius_pool_manager_image_history import (
                qualify_completed_manager_images,
            )
            from scripts.ops.nebius_pool_startup_repair import qualify_completed_startup_repair

            if not cancel:
                qualify_completed_startup_repair(request, state=state, anchor=anchor)
                qualify_completed_manager_images(request, state=state, anchor=anchor)
            closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
            _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
            identity, record = _activation_record(request, state=state, anchor=anchor)
            # JSON persistence sorts object keys. Dispatch order belongs to the
            # protected roster, not whichever dict insertion order was loaded.
            participants = tuple(str(row.participant_id) for row in request.fencing.retirement.migration.guards)
            if not cancel and (startup is None or any(row['phase'] != 'started' for row in startup['workloads'].values())):
                raise ValueError
            if record is None:
                record = {**identity, 'opening': 'prepared', 'cancellation': 'prepared',
                    'guards': {str(row.participant_id): {'release': 'prepared', 'fence': 'prepared'}
                        for row in request.fencing.retirement.migration.guards}}

            def observe() -> tuple[str, dict[str, str]]:
                from scripts.ops.nebius_pool_startup_fence import observe_recovery_workloads

                if _hash(state / 'cutover.json') != identity['closure_sha256'] or _startup_hash(state) != identity['startup_sha256']:
                    raise ValueError
                api.verify_retained()
                observe_recovery_workloads(request, api, state=state, anchor=anchor)
                mode = api.pool_state()
                guard_states = {key: api.guard_state(key) for key in record['guards']}
                if mode not in _pool_choices(record) or any(
                        value not in _guard_choices(record['guards'][key]) for key, value in guard_states.items()):
                    raise ValueError
                return mode, guard_states

            def save() -> None:
                private_state._atomic_json(state / 'activation.json', record)

            def result(status: str, mode: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'],
                    'admission_open': mode == 'global', 'legacy_restore_allowed': False}

            observe()
            marker = anchor / (identity['operation_id'] + '-activation.json')
            if not marker.exists():
                private_state._atomic_json(marker, identity)
                save()
            if not cancel and record['cancellation'] != 'prepared':
                raise ValueError
            if cancel:
                if record['cancellation'] == 'prepared':
                    record['cancellation'] = 'intent'
                    save()
                    try:
                        api.fence_pool()
                    except Exception:
                        pass  # Intent is durable; only readback can settle it.
                mode, _ = observe()
                if mode != 'fenced':
                    return result('pending_pool_fence', mode)
                if record['cancellation'] != 'fenced':
                    record['cancellation'] = 'fenced'
                    save()
                for key in participants:
                    item = record['guards'][key]
                    if item['fence'] == 'fenced':
                        continue
                    observe()
                    if item['fence'] == 'prepared':
                        item['fence'] = 'intent'
                        save()
                        try:
                            api.fence_guard(key)
                        except Exception:
                            pass
                    _, states = observe()
                    if states[key] != 'fenced':
                        return result('pending_guard_fence', 'fenced')
                    item['fence'] = 'fenced'
                    save()
                mode, _ = observe()
                return result('pool_activation_cancelled', mode)

            if record['opening'] == 'prepared':
                api.qualify_runtime()
                observe()
                record['opening'] = 'intent'
                save()
                try:
                    api.open_pool()
                except Exception:
                    pass
            mode, _ = observe()
            if mode != 'global':
                return result('pending_pool_opening', mode)
            if record['opening'] != 'opened':
                record['opening'] = 'opened'
                save()
            for key in participants:
                item = record['guards'][key]
                if item['release'] == 'released':
                    continue
                observe()
                if item['release'] == 'prepared':
                    item['release'] = 'intent'
                    save()
                    try:
                        api.release_guard(key)
                    except Exception:
                        pass
                _, states = observe()
                if states[key] != 'open':
                    return result('pending_guard_release', 'global')
                item['release'] = 'released'
                save()
            mode, _ = observe()
            return result('pool_activation_complete', mode)
    except Exception:
        raise ValueError('pool_activation_unconfirmed_preserve_evidence') from None
