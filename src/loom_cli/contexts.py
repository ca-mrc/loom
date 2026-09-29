"""Process-local, explicit CLI config selection; no shared active-context file."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

_CONTEXT: ContextVar[str | None] = ContextVar("loom_cli_context", default=None)
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,95}")


def https_origin(value: str) -> str:
    """A complete explicit HTTPS origin, not a URL with credentials or a path."""
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path or parsed.query or parsed.fragment
                or parsed.hostname != parsed.hostname.lower() or parsed.port == 0
                or value != "https://" + parsed.netloc):
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("managed context binding requires exact HTTPS origins") from None
    return value


@dataclass(frozen=True)
class ManagedEnvironmentBinding:
    environment_id: str
    incarnation: str
    management_origin: str
    child_origin: str

    def __post_init__(self) -> None:
        try:
            for value in (self.environment_id, self.incarnation):
                if not isinstance(value, str) or str(UUID(value)) != value:
                    raise ValueError
            https_origin(self.management_origin)
            https_origin(self.child_origin)
            if self.management_origin == self.child_origin:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("invalid managed context binding") from None

    @classmethod
    def from_dict(cls, value: object) -> ManagedEnvironmentBinding | None:
        if value is None:
            return None
        if (not isinstance(value, dict) or set(value) != {
            "environment_id", "incarnation", "management_origin", "child_origin",
        } or any(not isinstance(item, str) for item in value.values())):
            raise ValueError("invalid managed context binding")
        return cls(**value)


def current_context() -> str | None:
    return _CONTEXT.get()


@dataclass(frozen=True)
class ManagedApplicationBinding:
    """Stable application identity; mutable generations belong to login proofs."""

    application_id: str
    incarnation: str
    management_origin: str
    child_origin: str

    def __post_init__(self) -> None:
        try:
            for value in (self.application_id, self.incarnation):
                if not isinstance(value, str) or str(UUID(value)) != value or UUID(value).int == 0:
                    raise ValueError
            https_origin(self.management_origin)
            https_origin(self.child_origin)
            if self.management_origin == self.child_origin:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("invalid managed application binding") from None

    @classmethod
    def from_dict(cls, value: object) -> ManagedApplicationBinding | None:
        if value is None:
            return None
        if (not isinstance(value, dict) or set(value) != {
            "application_id", "incarnation", "management_origin", "child_origin",
        } or any(not isinstance(item, str) for item in value.values())):
            raise ValueError("invalid managed application binding")
        return cls(**value)


@contextmanager
def selected_context(name: str | None) -> Iterator[None]:
    if name is not None and (not isinstance(name, str) or _NAME.fullmatch(name) is None):
        raise ValueError("context must contain 1-96 lowercase letters, digits, underscores or hyphens")
    token = _CONTEXT.set(name)
    try:
        yield
    finally:
        _CONTEXT.reset(token)
