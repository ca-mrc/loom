"""One CLOSED catalog transaction with durable intent and read-only reconciliation.

This internal child is not deployment authority. The protected parent binds the
completed runtime, source, stopped gateway, manager readiness and common handoff
lock before calling it. It exposes neither arbitrary SQL nor a write retry.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from scripts.ops.nebius_closed_registry import ClosedRegistryJournal, RegistryNotDispatchedError
from scripts.ops.nebius_pool_preactivation_database import _identity, qualify_preactivation_report

from loom_service.pool_management.participant_refresh import PoolParticipantRefresh


class PreactivationRegistryAPI(Protocol):
    def observe(self, request: PoolParticipantRefresh) -> dict[str, Any]: ...
    def commit(self, request: PoolParticipantRefresh) -> dict[str, Any]: ...


class PreactivationNotDispatchedError(RegistryNotDispatchedError):
    """Only a proven failure before the write transport may raise this signal."""


_JOURNAL = ClosedRegistryJournal(model=PoolParticipantRefresh, schema='loom.pool-preactivation-registry.v1',
    identity=_identity, read_report=lambda request, report: qualify_preactivation_report(request, report, read_only=True))


def validate_preactivation_registry(*, request: PoolParticipantRefresh, state_dir: Path,
        complete: bool = False) -> dict[str, Any]:
    return _JOURNAL.validate(request=request, state_dir=state_dir, complete=complete)


def prepare_preactivation_registry(*, request: PoolParticipantRefresh, state_dir: Path) -> None:
    _JOURNAL.prepare(request=request, state_dir=state_dir)


def advance_preactivation_registry(*, request: PoolParticipantRefresh, api: PreactivationRegistryAPI,
        state_dir: Path, qualify: Callable[[], None]) -> bool:
    try:
        return _JOURNAL.advance(request=request, api=api, state_dir=state_dir, qualify=qualify)
    except Exception:
        raise ValueError('preactivation registry outcome unconfirmed; preserve evidence') from None
