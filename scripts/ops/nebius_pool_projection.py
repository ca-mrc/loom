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
from io import BytesIO
from pathlib import PurePath
from pickle import Pickler
from typing import Any, TypeVar
from uuid import UUID

from pydantic import BaseModel
from scripts.ops.nebius_ingress_stage import _copy_document

Request = TypeVar("Request")
Result = TypeVar("Result")


def _snapshot(value: Any) -> object:
    """Fresh opaque typed graph, captured by the standard-library C encoder.

    These bytes are only compared in memory, NEVER deserialized or persisted.
    Exact builtin containers use the encoder's linear reference traversal. Our
    override alone selects custom object state: no user reducer, serializer or
    state hook executes. Class identities stay outside the bytes, so local
    classes need no import/global name and distinct types cannot alias.
    """
    classes: list[type[Any]] = []
    indices: dict[int, int] = {}

    def reject_buffer(_buffer: object) -> None:
        # PickleBuffer bypasses reducer_override and otherwise aliases bytes or
        # bytearray. Its actual type still needs the renderer's qualification.
        raise TypeError("pool projection input buffer is not memoizable")

    class Encoder(Pickler):
        def reducer_override(self, item: Any) -> Any:
            if item is dict:  # The sole constructor used by our fixed encoding.
                return NotImplemented
            kind = type(item)
            state: object
            if kind is datetime:
                state = item.isoformat(), item.fold
            elif kind is UUID:
                state = item.int
            elif isinstance(item, PurePath):
                state = str(item)
            elif isinstance(item, Enum):
                state = item.name
            elif isinstance(item, BaseModel):
                state = (item.__dict__, item.__pydantic_extra__,
                    item.__pydantic_private__, item.__pydantic_fields_set__)
            elif not isinstance(item, type) and is_dataclass(item):
                state = tuple((field.name, getattr(item, field.name)) for field in fields(item))
            else:
                raise TypeError("pool projection input is not memoizable: " + kind.__name__)
            kind_id = id(kind)
            if kind_id not in indices:
                indices[kind_id] = len(classes)
                classes.append(kind)
            # Items, rather than constructor arguments, let the C encoder memoize
            # the object before traversing state, including shared/cyclic graphs.
            return dict, (), None, None, iter((('class', indices[kind_id]), ('value', state)))

    output = BytesIO()
    Encoder(output, protocol=5, buffer_callback=reject_buffer).dump(value)
    # Compare identity tags before class objects: a metaclass may override ==.
    # Strong references prevent tag reuse while this snapshot remains retained.
    return tuple(indices), output.getvalue(), tuple(classes)


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
