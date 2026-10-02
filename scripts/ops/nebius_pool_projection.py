"""Bound repeated pure manifest derivation; never memoize operational evidence.

Only explicitly decorated input-only renderers use this single-entry memo. Every
call snapshots the complete typed input again, including nested mutable fields.
Journal reads, live observations, permission checks and writes do not use it.
Returned documents are detached from both inputs and the retained result.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import PurePath
from typing import Any, TypeVar
from uuid import UUID

from pydantic import BaseModel
from scripts.ops.nebius_ingress_stage import _copy_document

Request = TypeVar("Request")
Result = TypeVar("Result")


def _snapshot(value: Any) -> object:
    """Keep types and ordering: Python equality aliases e.g. True and 1."""
    kind = type(value)
    if value is None or kind in (str, int, bool, bytes):
        return kind, value
    if kind is float:
        return kind, value.hex()
    if kind is datetime:
        return kind, value.isoformat(), value.fold
    if kind in (tuple, list):
        return kind, tuple(_snapshot(item) for item in value)
    if kind in (set, frozenset):
        return kind, frozenset(_snapshot(item) for item in value)
    if kind is dict:
        return kind, tuple((_snapshot(key), _snapshot(item)) for key, item in value.items())
    if kind is UUID:
        return kind, value.int
    if isinstance(value, PurePath):
        return kind, str(value)
    if isinstance(value, Enum):
        return kind, value.name
    if isinstance(value, BaseModel):
        # model_dump may normalize invalid model_copy values or omit fields.
        # Capture raw values instead so a prior qualification cannot hide drift.
        return (kind, _snapshot(value.__dict__), _snapshot(value.__pydantic_extra__),
            _snapshot(value.__pydantic_private__), _snapshot(value.__pydantic_fields_set__))
    if not isinstance(value, type) and is_dataclass(value):
        return kind, tuple((field.name, _snapshot(getattr(value, field.name))) for field in fields(value))
    raise TypeError("pool projection input is not memoizable: " + kind.__name__)


def pure_projection(function: Callable[[Request], Result]) -> Callable[[Request], Result]:
    """Reuse one exact input-only result; unknown input types take the normal path.

The memo is bounded to one entry per renderer, has no identity/hash-only keys,
and stores no failed qualification. A concurrent replacement cannot pair a key
with another result: each reader retains one complete entry locally.
"""
    last: tuple[object, Result] | None = None

    @wraps(function)
    def project(request: Request) -> Result:
        nonlocal last
        try:
            key = _snapshot(request)
        except TypeError:
            return function(request)
        entry = last
        if entry is not None and key == entry[0]:
            return _copy_document(entry[1])
        result = function(request)
        retained = _copy_document(result)
        # Never associate a projection with inputs changed during its derivation.
        try:
            if _snapshot(request) == key:
                last = key, retained
        except TypeError:
            pass
        return _copy_document(retained)

    return project
