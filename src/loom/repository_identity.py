"""Stable identity and historical names of Loom's GitHub repository."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

REPOSITORY_ID = 1281629473
REPOSITORY_NAMES = frozenset({"qianyi-sun/loom", "ca-mrc/loom"})
GITHUB_API_ROOT = f"https://api.github.com/repositories/{REPOSITORY_ID}/"


def is_repository_name(value: Any) -> bool:
    return isinstance(value, str) and value in REPOSITORY_NAMES


def is_loom_repository(value: Any) -> bool:
    """A familiar name alone never grants a fork the original repo's authority."""
    return (
        isinstance(value, Mapping)
        and type(value.get("id")) is int
        and value["id"] == REPOSITORY_ID
        and is_repository_name(value.get("full_name"))
    )
