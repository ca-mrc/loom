"""Construct task-configured verifiers for local CLI and worker trials."""

from __future__ import annotations

from collections.abc import Callable

from loom.models.task import TaskConfig
from loom.verifier.base import Verifier
from loom.verifier.pytest_verifier import PytestVerifier
from loom.verifier.script_verifier import ScriptVerifier

_VERIFIER_CTORS: dict[str, Callable[..., Verifier]] = {
    "pytest": PytestVerifier,
    "script": ScriptVerifier,
}


def task_verifier_factory(task_config: TaskConfig) -> Callable[[], Verifier]:
    """Freeze the selected constructor and arguments; create a fresh verifier per call."""
    name = task_config.verifier.name
    ctor = _VERIFIER_CTORS.get(name)
    if ctor is None:
        raise ValueError(f"unknown verifier: {name!r}")
    args = dict(task_config.verifier.args)
    return lambda: ctor(**args)
