"""Shared intent/readback engine for fixed CLOSED-only registry operations.

Concrete callers retain separate models, SQL, report schemas and protected
authority. This engine never accepts SQL or permits a caller to retry an
uncertain commit. The parent owns live qualification and its shared lock.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar

from pydantic import BaseModel
from scripts.ops import nebius_certificates as private_state

from loom_service.environment_management.candidates import _json

Request = TypeVar('Request', bound=BaseModel)
Input = TypeVar('Input', bound=BaseModel, contravariant=True)


class ClosedRegistryAPI(Protocol[Input]):
    def observe(self, request: Input) -> dict[str, Any]: ...
    def commit(self, request: Input) -> dict[str, Any]: ...


class RegistryNotDispatchedError(ValueError):
    """Only a proven failure before write transport may raise this signal."""


class ClosedRegistryJournal(Generic[Request]):
    def __init__(self, *, model: type[Request], schema: str,
            identity: Callable[[Request], dict[str, str]], read_report: Callable[[Request, Any], str]):
        self.model, self.schema = model, schema
        self.identity, self.read_report = identity, read_report

    def _identity(self, request: Request) -> dict[str, str]:
        selected = self.model.model_validate(request.model_dump())
        return {**self.identity(selected), 'schema': self.schema}

    def validate(self, *, request: Request, state_dir: Path, complete: bool = False) -> dict[str, Any]:
        identity = self._identity(request)
        if not state_dir.is_absolute() or state_dir != state_dir.resolve() or type(complete) is not bool:
            raise ValueError
        record = _json(private_state._private_read(state_dir / 'registry.json'))
        if (set(record) != {*identity, 'status', 'report'} or any(record[key] != value for key, value in identity.items())
                or record['status'] not in {'prepared', 'intent', 'complete'}
                or (complete and record['status'] != 'complete')):
            raise ValueError
        if record['status'] == 'complete':
            if self.read_report(request, record['report']) != 'complete':
                raise ValueError
        elif record['report'] is not None:
            raise ValueError
        return record

    def prepare(self, *, request: Request, state_dir: Path) -> None:
        identity = self._identity(request)
        if not state_dir.is_absolute() or state_dir != state_dir.resolve() or state_dir == Path('/'):
            raise ValueError
        with private_state._locked_state(state_dir):
            path = state_dir / 'registry.json'
            if not path.exists() and not path.is_symlink():
                private_state._atomic_json(path, {**identity, 'status': 'prepared', 'report': None})
            self.validate(request=request, state_dir=state_dir)

    def advance(self, *, request: Request, api: ClosedRegistryAPI[Request], state_dir: Path,
            qualify: Callable[[], None]) -> bool:
        """Unknown dispatches are observed, even when still at the predecessor."""
        try:
            original = request
            request = self.model.model_validate(request.model_dump())
            identity = self._identity(request)

            def scope(record: dict[str, Any] | None = None) -> None:
                if self._identity(original) != identity:
                    raise ValueError
                qualify()
                if (self._identity(original) != identity or (record is not None
                        and self.validate(request=request, state_dir=state_dir) != record)):
                    raise ValueError

            scope()
            self.prepare(request=request, state_dir=state_dir)
            with private_state._locked_state(state_dir):
                record = self.validate(request=request, state_dir=state_dir)
                path = state_dir / 'registry.json'
                scope(record)
                report = api.observe(request.model_copy(deep=True))
                state = self.read_report(request, report)
                scope(record)
                if record['status'] == 'prepared':
                    if state != 'ready':
                        raise ValueError  # No retained intent explains this successor.
                    record['status'] = 'intent'
                    private_state._atomic_json(path, record)
                    intent = private_state._private_read(path)

                    def undispatched() -> None:
                        # Restore only this exact intact intent. Never recreate
                        # missing evidence or clear the parent's failure latch.
                        if (self._identity(original) != identity
                                or private_state._private_read(path) != intent
                                or self.validate(request=request, state_dir=state_dir) != record):
                            raise ValueError
                        private_state._atomic_json(path, {**record, 'status': 'prepared'})

                    try:
                        scope(record)
                    except Exception:
                        undispatched()  # commit has conclusively not been called.
                        raise
                    try:
                        api.commit(request.model_copy(deep=True))
                    except RegistryNotDispatchedError:
                        undispatched()
                        raise
                    except Exception:
                        pass  # May have committed: observe exact authority.
                    scope(record)
                    report = api.observe(request.model_copy(deep=True))
                    state = self.read_report(request, report)
                    scope(record)
                if state != 'complete':
                    raise ValueError
                if record['status'] != 'complete':
                    record.update(status='complete', report=report)
                    private_state._atomic_json(path, record)
                scope(record)
                self.validate(request=request, state_dir=state_dir, complete=True)
                return True
        except Exception:
            raise ValueError('closed registry outcome unconfirmed; preserve evidence') from None
