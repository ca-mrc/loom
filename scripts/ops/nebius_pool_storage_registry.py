"""Storage-only CLOSED registry journal, not an installation entrypoint."""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops.nebius_closed_registry import ClosedRegistryAPI, ClosedRegistryJournal
from scripts.ops.nebius_pool_storage_binding import (
    ClosedPoolStorageRefresh,
    _identity,
    qualify_storage_report,
)

_JOURNAL = ClosedRegistryJournal(model=ClosedPoolStorageRefresh,
    schema='loom.closed-pool-storage-registry.v1', identity=_identity,
    read_report=lambda request, report: qualify_storage_report(request, report, read_only=True))


def validate_storage_registry(*, request: ClosedPoolStorageRefresh, state_dir: Path,
        complete: bool = False) -> dict[str, Any]:
    return _JOURNAL.validate(request=request, state_dir=state_dir, complete=complete)


def prepare_storage_registry(*, request: ClosedPoolStorageRefresh, state_dir: Path) -> None:
    _JOURNAL.prepare(request=request, state_dir=state_dir)


def advance_storage_registry(*, request: ClosedPoolStorageRefresh, api: ClosedRegistryAPI[ClosedPoolStorageRefresh],
        state_dir: Path, qualify: Callable[[], None]) -> bool:
    return _JOURNAL.advance(request=request, api=api, state_dir=state_dir, qualify=qualify)
