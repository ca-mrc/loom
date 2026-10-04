"""AgentAdapter Protocol + supporting types.

These types duplicate shapes from `loom.driver.base` and `loom.models.types`
on purpose: this package is distributed for installation inside
sandbox containers, where importing the full `loom` library would pull in
unrelated server-side dependencies (sqlalchemy, fastapi, alembic).
`loom_worker.SubprocessAgent._bridge()` adapts between the two surfaces.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class ModelSpec:
    """Mirror of `loom.models.types.ModelSpec`. Held by an AgentAdapter
    when its task config specifies a model to call (most adapters take it
    from the task's `[agent.model]` block at trial-spawn time)."""

    provider: str
    name: str
    tier: str | None = None
    region: str | None = None


@runtime_checkable
class SandboxAccess(Protocol):
    """The minimal sandbox capability surface a capture utility needs to
    reach inside the sandbox container. Mirrors a strict subset of
    `loom.driver.base.Driver`. `SubprocessAgent._bridge()` wraps a real
    Driver into this Protocol; tests use a fake."""

    async def read_text(self, path: PurePosixPath) -> str:
        """Read `path` from inside the sandbox as UTF-8 text. Used by
        `tail_log_file` to poll a log file the agent writes to."""

    async def exec_oneshot(
        self,
        argv: list[str],
        *,
        timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        """Run `argv` inside the sandbox and return (rc, stdout).
        Used by `poll_local_http` to curl localhost endpoints exposed
        by server-mode adapters."""


@dataclass
class ExecHandle:
    """Long-running process handle the worker hands to `capture_events()`.

    Mirrors `loom.driver.base.ExecHandle` plus the `sandbox` side-channel so
    file-tail / http-poll capture mechanisms can reach inside the sandbox
    without importing Loom's Driver Protocol.
    `sandbox` is None when the adapter only needs stdout streaming.
    Capture owns stdout consumption, including discarding it when events come
    from a side channel. The worker separately drains stderr for diagnostics.
    """

    pid: int
    stdout: AsyncIterator[bytes]
    stderr: AsyncIterator[bytes]
    _wait: Callable[[], Awaitable[int]]
    _kill: Callable[[], Awaitable[None]]
    sandbox: SandboxAccess | None = None

    async def wait(self) -> int:
        return await self._wait()

    async def kill(self) -> None:
        await self._kill()


@runtime_checkable
class TrajectoryEventLike(Protocol):
    """Structural alias for what `capture_events()` yields. The full
    `loom.models.trajectory.TrajectoryEvent` discriminated union is the
    intended type; we accept anything pydantic-serialisable so this
    package doesn't have to import the union directly."""

    def model_dump(self) -> dict[str, Any]: ...


EndpointDialect = Literal[
    "openai_chat",
    "openai_responses",
    "anthropic",
    "gemini",
]


@runtime_checkable
class AgentAdapter(Protocol):
    """The contract every per-agent module implements.

    Adapters are immutable dataclasses (frozen=True) — the registry
    holds module-level instances. `build_invocation()` returns argv for
    the agent CLI inside the sandbox; `capture_events()` yields events
    while the process runs.
    """

    name: str
    supports_os: frozenset[str]  # {"linux", ...}
    endpoint_dialect: EndpointDialect
    api_key_env: str  # e.g. "ANTHROPIC_API_KEY"
    base_url_env: str
    model_name_template: str  # Agent-facing model id template.
    supports_multi_turn: bool  # metadata only; execution does not branch on it
    additional_egress: frozenset[str]  # hostnames beyond Gateway

    # Single multi-line shell script the worker runs inside the trial sandbox
    # before the agent starts, to install the
    # adapter's CLI. None = adapter assumes pre-installed binaries
    # (e.g. oracle, in-box agents — they're added in a different image).
    # Mounted as /tmp/install.sh and executed via `bash /tmp/install.sh`
    # so authors can rely on bashisms. Empty/whitespace-only strings
    # are normalized to None by the resolver.
    #
    # Pinned-version mandate enforced by
    # scripts/check_install_scripts_pinned.py (CI):
    #   - `npm install -g <pkg>@<version>`  (NOT `@latest`)
    #   - `pip install <pkg>==<version>`
    #   - `uv tool install <pkg>==<version>`
    install_script: str | None

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        """Return the argv for the agent. `env` is mutated in-place if
        the adapter needs to set additional env vars (telemetry-off,
        config paths, etc.)."""

    def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        """Yield events for the duration of the agent run. Implementations
        choose a capture mechanism from `loom_launcher.capture`
        (stream_stdout_jsonl / tail_log_file / poll_local_http /
        tail_pty) and pass the handle / sandbox / step_id / trial_id
        through. Async generator function (NOT an async def returning
        an iterator) so the Protocol matches the natural impl shape."""
        ...
