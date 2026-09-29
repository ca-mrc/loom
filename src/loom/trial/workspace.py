"""Workspace materialization — upload the task bundle into the sandbox.

The worker materializes `bundle["source"]` into a host-side `task_dir`
that contains everything the bundle ships: `instruction.md`,
`solution/`, `tests/`, `task.toml`, etc. Before #186 the trial loop
left that directory on the host — only the OracleAgent uploaded its
own `solve.sh`, and the verifier path was effectively dead for any
benchmark whose grading tests live in the bundle (HumanEval,
SWE-Bench).

This module ports the `docker cp task_dir → /workspace` step that
`loom_benchmark_tool.oracle_runner` has used for the verify CLI into
the production trial path, using the Driver protocol's single-file
`upload()` (no Protocol change needed).
"""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Literal

from loom.driver.base import Driver

# Path components the worker excludes from the upload set.
#
# `task.toml` is host-only metadata (read before the trial starts).
# The rest are common dev-cruft that hand-authored or git-cloned
# bundles can carry — uploading them is wasteful and, in a few cases
# (.git/), can leak the bundle's full history into the sandbox where
# the agent could exfiltrate it via the LLM. Match is on the FIRST
# component of the relative path; nested matches require the same
# name (e.g., src/__pycache__ is skipped because rglob walks into it,
# but its files have `.parts == ("src", "__pycache__", "x.pyc")` so
# the per-file check below specifically also looks at any segment).
_SKIP_NAMES = frozenset(
    {
        "task.toml",
        ".loom-build",
        ".git",
        "__pycache__",
        "node_modules",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".DS_Store",
    }
)

# Suffix-match (case-sensitive) for individual files to skip. Cheaper
# than a full glob and covers the common compiled-Python case.
_SKIP_SUFFIXES = frozenset({".pyc", ".pyo"})

# Cap concurrent uploads so a multi-hundred-file bundle (SWE-Bench)
# doesn't open hundreds of simultaneous docker HTTP connections. 16
# is comfortably under docker's default 100-req cap with headroom for
# other in-flight calls (exec, status, etc.).
_MAX_PARALLEL_UPLOADS = 16

WorkspacePhase = Literal["agent", "verifier"]
WorkspaceStagingPolicyName = Literal["tb21", "none"]

TB21_TASK_ID_PREFIX = "terminal-bench-2@tb2.1-r6/"

# This exact schema is persisted in the TB2.1 rev-6 manifest and task
# provenance.  Keep it as plain JSON-compatible data so publication, audit,
# catalog copying, and the worker can all attest to the same boundary.
TB21_AGENT_WORKSPACE_POLICY: dict[str, object] = {
    "schema_version": 1,
    "agent_excluded_paths": [
        "solution/**",
        "tests/**",
        "verifier/**",
        "upstream-task.toml",
    ],
    "verifier_only_paths": [
        "solution/**",
        "tests/**",
        "verifier/**",
        "upstream-task.toml",
    ],
    "trusted_oracle_paths": ["solution/**"],
}


@dataclass(frozen=True)
class WorkspaceStagingPolicy:
    """Persisted allow/deny boundary for a task's agent workspace.

    Private verifier assets remain in the host-side materialized bundle until
    the verifier phase.  The policy is data carried by task provenance rather
    than a benchmark-name convention, so catalog copies and workers can
    independently verify and enforce the same contract.
    """

    agent_excluded_paths: tuple[str, ...]
    verifier_only_paths: tuple[str, ...]
    trusted_oracle_paths: tuple[str, ...]

    @classmethod
    def from_provenance(cls, raw: Mapping[str, object]) -> WorkspaceStagingPolicy:
        if raw.get("schema_version") != 1:
            raise ValueError("workspace staging policy schema_version must be 1")
        required = {
            "schema_version",
            "agent_excluded_paths",
            "verifier_only_paths",
            "trusted_oracle_paths",
        }
        if set(raw) != required:
            raise ValueError("workspace staging policy must contain the exact v1 fields")
        agent_paths = cls._validate_paths(raw["agent_excluded_paths"])
        verifier_paths = cls._validate_paths(raw["verifier_only_paths"])
        trusted_oracle_paths = cls._validate_paths(raw["trusted_oracle_paths"])
        if agent_paths != verifier_paths:
            raise ValueError(
                "workspace staging policy verifier_only_paths must equal agent_excluded_paths",
            )
        if any(path not in agent_paths for path in trusted_oracle_paths):
            raise ValueError(
                "workspace staging policy trusted_oracle_paths must be a subset "
                "of agent_excluded_paths",
            )
        return cls(
            agent_excluded_paths=agent_paths,
            verifier_only_paths=verifier_paths,
            trusted_oracle_paths=trusted_oracle_paths,
        )

    @staticmethod
    def _validate_paths(value: object) -> tuple[str, ...]:
        if not isinstance(value, list) or not value:
            raise ValueError("workspace staging policy paths must be a non-empty list")
        paths: list[str] = []
        for path in value:
            if not isinstance(path, str) or not path or path.startswith("/"):
                raise ValueError("workspace staging policy paths must be relative strings")
            parts = PurePosixPath(path).parts
            if ".." in parts or "." in parts:
                raise ValueError("workspace staging policy paths must not traverse")
            paths.append(path)
        if len(paths) != len(set(paths)):
            raise ValueError("workspace staging policy paths must be unique")
        return tuple(paths)

    def is_private(self, relative_path: PurePosixPath) -> bool:
        candidate = relative_path.as_posix()
        return any(fnmatchcase(candidate, pattern) for pattern in self.agent_excluded_paths)


def resolve_workspace_staging_policy_name(
    name: str,
) -> WorkspaceStagingPolicy | None:
    """Map a CLI/TrialConfig policy name to a concrete staging policy (#1263)."""
    if name == "tb21":
        return WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    if name == "none":
        return None
    raise ValueError(
        f"unknown workspace_staging_policy_name {name!r}; expected 'tb21' or 'none'",
    )


def require_tb21_workspace_staging_policy(
    raw_policy: object,
) -> WorkspaceStagingPolicy:
    """Parse only the reviewed TB2.1 rev-6 workspace boundary.

    WorkspaceStagingPolicy intentionally supports future benchmark-specific
    layouts.  The physical TB2.1 profile is stricter: its four private paths
    are part of the reviewed immutable contract, so a merely valid alternate
    policy must fail before materialization or driver startup.
    """
    if not isinstance(raw_policy, dict):
        raise ValueError("TB2.1 task is missing canonical private workspace isolation policy")
    try:
        policy = WorkspaceStagingPolicy.from_provenance(raw_policy)
    except ValueError as exc:
        raise ValueError("TB2.1 task has invalid private workspace isolation policy") from exc
    canonical = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    if policy != canonical:
        raise ValueError("TB2.1 task requires the canonical private workspace isolation policy")
    return policy


def resolve_trial_workspace_staging_policy(
    *,
    policy_name: WorkspaceStagingPolicyName | None,
    task_id: str,
    raw_provenance_policy: object,
) -> WorkspaceStagingPolicy | None:
    """Resolve the effective agent-workspace staging policy for a trial (#1263).

    Order:
    1. Explicit ``trial_config.workspace_staging_policy_name``
    2. TB2.1-r6 task-id prefix → required canonical policy (``none`` rejected)
    3. Task provenance dict, else no filter
    """
    is_tb21 = task_id.startswith(TB21_TASK_ID_PREFIX)
    if policy_name == "tb21":
        return resolve_workspace_staging_policy_name("tb21")
    if policy_name == "none":
        if is_tb21:
            raise ValueError(
                "TB2.1 task cannot use workspace_staging_policy_name=none; "
                "private workspace isolation is required",
            )
        return None
    if is_tb21:
        return require_tb21_workspace_staging_policy(raw_provenance_policy)
    if isinstance(raw_provenance_policy, dict):
        return WorkspaceStagingPolicy.from_provenance(raw_provenance_policy)
    return None


_PLANTED_PRIVATE = ("tests", "verifier", "solution", "upstream-task.toml")


async def refuse_planted_private_paths(driver: Driver, workdir: PurePosixPath) -> None:
    """Fail closed when the agent already created a private-path name.

    Shared grading uploads tests into the live agent sandbox. Deleting an
    agent-created ``tests`` directory or symlink and writing the real tests
    on top would hide that the path was planted.
    """
    for name in _PLANTED_PRIVATE:
        path = shlex.quote(str(workdir / name))
        result = await driver.exec(f"if [ -e {path} ] || [ -L {path} ]; then exit 42; fi")
        if result.return_code == 42:
            raise RuntimeError(f"planted private path: {name}")
        if result.return_code not in {0, 42}:
            raise RuntimeError("planted private path check failed")


async def materialize_workspace(
    *,
    driver: Driver,
    task_dir: Path,
    dst: PurePosixPath,
    policy: WorkspaceStagingPolicy | None = None,
    phase: WorkspacePhase = "agent",
    trusted_private_paths: tuple[str, ...] = (),
    excluded_paths: tuple[str, ...] = (),
) -> int:
    """Recursively upload every regular file under `task_dir` to `dst`
    inside the sandbox, preserving the relative path layout. Returns
    the number of files uploaded.

    Empty directories are not created — the driver's `upload()` calls
    `mkdir -p` on each file's parent before writing, which is the only
    way a directory becomes observable inside the container anyway.

    Uploads run in parallel up to `_MAX_PARALLEL_UPLOADS` to keep
    SWE-Bench-sized bundles (~100s of files) under a few seconds
    instead of one-per-file serial round-trips.
    """
    if not task_dir.is_dir():
        return 0
    targets: list[tuple[Path, PurePosixPath]] = []
    for src in sorted(task_dir.rglob("*")):
        if not src.is_file():
            continue
        rel = src.relative_to(task_dir)
        if any(fnmatchcase(rel.as_posix(), pattern) for pattern in excluded_paths):
            continue
        # Skip if ANY path segment matches a skip name (catches both
        # top-level `.git/` AND nested `src/__pycache__/`).
        if any(part in _SKIP_NAMES for part in rel.parts):
            continue
        if src.suffix in _SKIP_SUFFIXES:
            continue
        if policy is not None:
            private = policy.is_private(PurePosixPath(*rel.parts))
            trusted_private = any(
                fnmatchcase(PurePosixPath(*rel.parts).as_posix(), pattern)
                for pattern in trusted_private_paths
            )
            if phase == "agent" and private and not trusted_private:
                continue
            if phase == "verifier" and not private:
                continue
        targets.append((src, dst / PurePosixPath(*rel.parts)))
    if not targets:
        return 0
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_UPLOADS)

    async def _upload_one(src: Path, target: PurePosixPath) -> None:
        async with semaphore:
            await driver.upload(src, target)

    await asyncio.gather(*(_upload_one(s, t) for s, t in targets))
    return len(targets)
