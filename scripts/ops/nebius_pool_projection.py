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
    """Fresh typed graph, not an exponentially expanded tree of shared inputs.

    References are traversal-local indices, never persistent object identities.
    Flat records also bound equality work: nested tuple snapshots would compare
    shared subtrees repeatedly even if their construction used a local memo.
    """
    references: dict[int, int] = {}
    retained: list[Any] = []  # Keep objects alive until this capture is complete.
    records: list[object] = []

    def visit(item: Any) -> object:
        kind = type(item)
        if item is None or kind in (str, int, bool, bytes):
            return kind, item
        if kind is float:
            return kind, item.hex()
        if kind is datetime:
            return kind, item.isoformat(), item.fold
        if kind is UUID:
            return kind, item.int
        if isinstance(item, PurePath):
            return kind, str(item)
        if isinstance(item, Enum):
            return kind, item.name
        identity = id(item)
        if identity in references:
            return references[identity]
        index = len(records)
        references[identity] = index
        retained.append(item)
        records.append(None)
        record: object
        if kind in (tuple, list):
            record = kind, tuple(visit(child) for child in item)
        elif kind in (set, frozenset):
            record = kind, frozenset(visit(child) for child in item)
        elif kind is dict:
            record = kind, tuple((visit(key), visit(child)) for key, child in item.items())
        elif isinstance(item, BaseModel):
            # Raw values retain field-set/invalid model_copy drift; model_dump
            # can normalize or omit it. No serialization hooks execute here.
            record = (kind, visit(item.__dict__), visit(item.__pydantic_extra__),
                visit(item.__pydantic_private__), visit(item.__pydantic_fields_set__))
        elif not isinstance(item, type) and is_dataclass(item):
            record = kind, tuple((field.name, visit(getattr(item, field.name))) for field in fields(item))
        else:
            raise TypeError("pool projection input is not memoizable: " + kind.__name__)
        records[index] = record
        return index

    root = visit(value)
    return root, tuple(records)


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
