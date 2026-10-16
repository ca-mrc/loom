"""One CLOSED catalog transaction with durable intent and read-only reconciliation.

This internal child is not deployment authority. The protected parent binds the
completed runtime, source, stopped gateway, manager readiness and common handoff
lock before calling it. It exposes neither arbitrary SQL nor a write retry.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_preactivation_database import _identity, qualify_preactivation_report

from loom_service.environment_management.candidates import _json
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh


class PreactivationRegistryAPI(Protocol):
    def observe(self, request: PoolParticipantRefresh) -> dict[str, Any]: ...
    def commit(self, request: PoolParticipantRefresh) -> dict[str, Any]: ...


class PreactivationNotDispatchedError(ValueError):
    """Only a proven failure before the write transport may raise this signal."""


def _journal_identity(request: PoolParticipantRefresh) -> dict[str, str]:
    request = PoolParticipantRefresh.model_validate(request.model_dump())
    return {**_identity(request), 'schema': 'loom.pool-preactivation-registry.v1'}


def validate_preactivation_registry(*, request: PoolParticipantRefresh, state_dir: Path,
        complete: bool = False) -> dict[str, Any]:
    identity = _journal_identity(request)
    if not state_dir.is_absolute() or state_dir != state_dir.resolve() or type(complete) is not bool:
        raise ValueError
    record = _json(private_state._private_read(state_dir / 'registry.json'))
    if (set(record) != {*identity, 'status', 'report'} or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'intent', 'complete'}
            or (complete and record['status'] != 'complete')):
        raise ValueError
    if record['status'] == 'complete':
        if qualify_preactivation_report(request, record['report'], read_only=True) != 'complete':
            raise ValueError
    elif record['report'] is not None:
        raise ValueError
    return record


def prepare_preactivation_registry(*, request: PoolParticipantRefresh, state_dir: Path) -> None:
    identity = _journal_identity(request)
    if not state_dir.is_absolute() or state_dir != state_dir.resolve() or state_dir == Path('/'):
        raise ValueError
    with private_state._locked_state(state_dir):
        path = state_dir / 'registry.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**identity, 'status': 'prepared', 'report': None})
        validate_preactivation_registry(request=request, state_dir=state_dir)


def advance_preactivation_registry(*, request: PoolParticipantRefresh, api: PreactivationRegistryAPI,
        state_dir: Path, qualify: Callable[[], None]) -> bool:
    """Unknown dispatches are observed, never retried, even if still at predecessor."""
    try:
        original = request
        request = PoolParticipantRefresh.model_validate(request.model_dump())
        identity = _journal_identity(request)

        def scope(record: dict[str, Any] | None = None) -> None:
            if _journal_identity(original) != identity:
                raise ValueError
            qualify()
            if (_journal_identity(original) != identity or (record is not None
                    and validate_preactivation_registry(request=request, state_dir=state_dir) != record)):
                raise ValueError

        scope()
        prepare_preactivation_registry(request=request, state_dir=state_dir)
        with private_state._locked_state(state_dir):
            record = validate_preactivation_registry(request=request, state_dir=state_dir)
            path = state_dir / 'registry.json'
            scope(record)
            report = api.observe(request.model_copy(deep=True))
            state = qualify_preactivation_report(request, report, read_only=True)
            scope(record)
            if record['status'] == 'prepared':
                if state != 'ready':
                    raise ValueError  # No journaled intent can explain this successor.
                record['status'] = 'intent'
                private_state._atomic_json(path, record)
                intent = private_state._private_read(path)

                def undispatched() -> None:
                    # Restore only this exact, intact intent. Never recreate
                    # lost evidence or clear the protected parent's failure
                    # latch. A later invocation must requalify from scratch.
                    if (_journal_identity(original) != identity
                            or private_state._private_read(path) != intent
                            or validate_preactivation_registry(request=request, state_dir=state_dir) != record):
                        raise ValueError
                    private_state._atomic_json(path, {**record, 'status': 'prepared'})

                try:
                    scope(record)
                except Exception:
                    undispatched()  # commit has conclusively not been called.
                    raise
                try:
                    api.commit(request.model_copy(deep=True))
                except PreactivationNotDispatchedError:
                    undispatched()
                    raise
                except Exception:
                    pass  # Commit may have succeeded; read the exact closed authority.
                scope(record)
                report = api.observe(request.model_copy(deep=True))
                state = qualify_preactivation_report(request, report, read_only=True)
                scope(record)
            if state != 'complete':
                raise ValueError
            if record['status'] != 'complete':
                record.update(status='complete', report=report)
                private_state._atomic_json(path, record)
            scope(record)
            validate_preactivation_registry(request=request, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('preactivation registry outcome unconfirmed; preserve evidence') from None
