"""LoomTerminus2Runtime — Harbor Terminus2 wrapper in the worker pool (#744)."""

from __future__ import annotations

import asyncio
import json
import shlex
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from loom.agent.direct_provider import DirectCallLedger, DirectProviderConnection
from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.agent.terminus2.dispatch_checkpoint import install_dispatch_checkpoint
from loom.agent.terminus2.gateway_ledger import CheckpointBridgeError
from loom.agent.terminus2.harbor_environment import (
    LoomHarborEnvironment,
    ensure_sandbox_deps,
    make_trial_paths,
)
from loom.agent.terminus2.model_switch import (
    Role,
    install_role_router,
    redact_agent_llm_kwargs,
    seed_fingerprint,
)
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA, LOOM_BRIDGE_REVISION
from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.driver.base import Driver
from loom.errors import AgentContinuationError, AgentError, exception_info
from loom.models.harbor_config import HarborConfig
from loom.models.mcp import MCPConnection
from loom.models.trajectory import (
    Terminus2EpisodeCheckpointEvent,
    Terminus2LlmCallCompletedEvent,
    Terminus2LlmCallFailedEvent,
    Terminus2LlmCallStartedEvent,
    Terminus2ModelMixPlannedEvent,
    Terminus2ModelSwitchPlannedEvent,
    Terminus2RecoveryFailedEvent,
)
from loom.models.trial import MultiModelSwitchSpec
from loom.models.types import OS, ModelSpec
from loom.request_params import sanitize_request_extras
from loom.security.redaction import redact_mapping, redact_text
from loom.trajectory.attempt_guard import AttemptTrajectoryFencedError

if TYPE_CHECKING:
    from loom.trajectory.writer import TrajectoryWriter


def _require_attempt_mutation_active(
    trajectory: TrajectoryWriter,
    deadline: AttemptDeadline | None,
) -> None:
    checker = getattr(trajectory, "require_attempt_active", None)
    if callable(checker):
        checker()
    if deadline is not None:
        deadline.require_remaining()


def _import_terminus2() -> tuple[type, type]:
    try:
        from harbor.agents.terminus_2.terminus_2 import Terminus2
        from harbor.models.agent.context import AgentContext
    except ImportError as exc:
        raise AgentError(
            f"terminus-2 requires harbor@{HARBOR_COMPAT_SHA} preinstalled in the controller image",
        ) from exc
    return Terminus2, AgentContext


def _openai_gateway_base(gateway_url: str) -> str:
    parts = urlsplit(gateway_url.rstrip("/"))
    path = parts.path.rstrip("/")
    if not path or path == "/openai":
        return urlunsplit(parts._replace(path="/openai/v1"))
    if path == "/openai/v1" or path.startswith("/openai/v1/"):
        return urlunsplit(parts._replace(path="/openai/v1"))
    return gateway_url


def _harbor_model_name(model: ModelSpec) -> str:
    # LiteLLM provider prefix for the OpenAI facade, not ModelSpec.provider.
    # The gateway strips this ``openai/`` when correlating loom_requested_model
    # against the HTTP body ``model`` field (see canonical_facade_model_id).
    return f"openai/{model.name}"


_HARBOR_ARTIFACT_NAMES = ("trajectory.json", "recording.cast")
_HARBOR_TMUX_SESSION = "terminus-2"
_TMUX_SESSION_LOST_MID_DISPATCH = "Terminus2 tmux session/server lost mid-dispatch."
_TMUX_SOFT_RECOVER_NOTICE = (
    "Terminus2 tmux session was lost mid-dispatch and has been recreated once.\n"
    "Previous in-flight keystrokes were NOT re-run. Shell state (cwd, env) was "
    "reset.\n"
    "Filesystem in the sandbox may be partially updated — inspect before "
    "continuing.\n"
    "Current directory: {cwd}\n"
)


async def _reset_harbor_tmux_session(env: Driver) -> None:
    """Remove Harbor's fixed-name session before a sequential agent retry.

    Harbor pins every Terminus2 run in a sandbox to ``terminus-2`` and does not
    stop that session after ``run``. Loom retries reuse the same sandbox, so a
    prior attempt otherwise makes ``tmux new-session`` fail with a duplicate
    session error. Each trial owns its sandbox; target only Harbor's fixed name.
    """
    await env.exec(
        f"tmux kill-session -t ={_HARBOR_TMUX_SESSION} 2>/dev/null || true",
    )


def _harbor_tmux_name(session: Any) -> str:
    return getattr(session, "_loom_canonical_session_name", None) or getattr(
        session, "_session_name", None,
    ) or _HARBOR_TMUX_SESSION


def _bind_exact_harbor_tmux_target(session: Any) -> None:
    """Bind operations to an exact target, retaining the creation identity.

    Pinned Harbor uses _session_name in send-keys, paste-buffer and capture.
    A permanent instance-local binding closes the check/send race: after the
    owned session exits, tmux must not fall back to a same-prefix session.
    Render the creation command first; '=name' is a target, not a new name.
    """
    if getattr(session, "_loom_canonical_session_name", None) is not None:
        return
    name = _harbor_tmux_name(session)
    start = getattr(session, "_tmux_start_session", None)
    start = start() if callable(start) else start
    if isinstance(start, str):
        # Pinned Harbor creates the literal name, then attaches its log pipe.
        # The latter is a target operation and must be exact as well.
        start = start.replace(f"pipe-pane -t {name} ", f"pipe-pane -t {shlex.quote('=' + name + ':')} ")
    session._loom_tmux_start_command = start
    session._loom_canonical_session_name = name
    session._session_name = "=" + name + ":"


async def _recreate_harbor_tmux_session(session: Any) -> str:
    """Recreate Harbor's fixed tmux session without replaying keystrokes.

    Uses Harbor's ``_tmux_start_session`` command when present so pane size /
    pipe-pane logging match setup. Skips asciinema re-attach (recording may
    have a gap). Returns the fresh shell's ``pwd`` for the recovery notice.
    """
    environment = getattr(session, "environment", None)
    if environment is None:
        raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH)

    session_name = _harbor_tmux_name(session)
    await _tmux_exec(session, f"tmux kill-session -t {shlex.quote('=' + session_name)} 2>/dev/null || true")

    start_cmd = getattr(session, "_loom_tmux_start_command", None) or getattr(session, "_tmux_start_session", None)
    if isinstance(start_cmd, str) and start_cmd.strip():
        command = start_cmd
    elif callable(start_cmd):
        command = start_cmd()
    else:
        command = (
            f"export TERM=xterm-256color && export SHELL=/bin/bash && "
            f"tmux new-session -d -s {session_name} 'bash --login'"
        )

    started = await _tmux_exec(session, command)
    return_code = getattr(started, "return_code", None)
    if return_code not in (0, None):
        stderr = getattr(started, "stderr", "") or ""
        raise AgentError(f"{_TMUX_SESSION_LOST_MID_DISPATCH} Recreate failed: {stderr}".strip())

    await _retain_harbor_dead_pane(session)

    # Incremental capture state from the dead pane is meaningless.
    if hasattr(session, "_previous_buffer"):
        session._previous_buffer = None

    is_alive = getattr(session, "_loom_is_alive_without_recovery", None) or getattr(session, "is_session_alive", None)
    if is_alive is not None and not await is_alive():
        raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH)

    pwd = await _tmux_exec(session, "pwd")
    cwd = (getattr(pwd, "stdout", None) or "").strip() or "unknown"
    return cwd


async def _tmux_exec(session: Any, command: str) -> Any:
    environment = getattr(session, "environment", None)
    if environment is None:
        raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH)
    require_active = getattr(session, "_loom_require_attempt_active", lambda: None)
    require_active()
    deadline = getattr(session, "_loom_attempt_deadline", None)
    timeout = min(5.0, deadline.require_remaining()) if deadline is not None else 5.0
    user = getattr(session, "_user", None)
    async with asyncio.timeout(timeout):
        result = await environment.exec(
            command=command, timeout_sec=timeout,
            **({"user": user} if user is not None else {}),
        )
    require_active()
    return result


async def _retain_harbor_dead_pane(session: Any) -> None:
    target = shlex.quote("=" + _harbor_tmux_name(session) + ":")
    result = await _tmux_exec(session, f"tmux set-option -w -t {target} remain-on-exit on")
    if getattr(result, "return_code", 0) != 0:
        raise AgentError("Failed to retain Terminus2 shell-exit diagnostics")


async def _capture_harbor_session_loss(session: Any) -> str:
    target = shlex.quote("=" + _harbor_tmux_name(session) + ":")
    # Query only this trial's pane, never another session or server-wide state.
    result = await _tmux_exec(session, (
        f"tmux list-panes -t {target} -F "
        "'pane_id=#{pane_id} pane_dead=#{pane_dead} exit=#{pane_dead_status} signal=#{pane_dead_signal}' "
        f"2>&1; tmux capture-pane -p -t {target} -S -80 2>&1"
    ))
    text = "Terminus2 session loss before recovery:\n" + (getattr(result, "stdout", "") or "")
    text += getattr(result, "stderr", "") or ""
    # The bridge applies known-credential redaction before its 16 KiB bound.
    return redact_text(text)


def _install_tmux_session_alive_guard(
    agent: Any, *, record_loss: Callable[[str], Awaitable[None]] | None = None,
    require_active: Callable[[], None] = lambda: None,
) -> None:
    """Soft-recover once if Harbor's tmux session dies mid-dispatch (#1068).

    After ``send_keys``, if the session/server is gone (or ``send_keys`` raises
    a ``no server running`` error):

    1. Recreate ``terminus-2`` once in the same sandbox.
    2. Do **not** replay the failed keystrokes; suppress further keys in the
       current command batch.
    3. Prepend an honest recovery notice to the next
       ``get_incremental_output`` so the model can re-orient.
    4. On recreate failure, or a second death after a recover → fail closed
       with :data:`_TMUX_SESSION_LOST_MID_DISPATCH`.

    Keep the wrap instance-local: concurrent Terminus2 trials would race
    with a process-global monkeypatch.
    """
    session = getattr(agent, "_session", None)
    if session is None:
        return
    _bind_exact_harbor_tmux_target(session)
    original_send_keys = session.send_keys
    original_get_incremental = getattr(session, "get_incremental_output", None)
    original_is_alive = getattr(session, "is_session_alive", None)

    async def is_alive_with_dead_pane_check() -> bool:
        require_active()
        if original_is_alive is not None and not await original_is_alive():
            return False
        if getattr(session, "environment", None) is None:
            return True
        target = shlex.quote("=" + _harbor_tmux_name(session) + ":")
        result = await _tmux_exec(session, f"tmux display-message -p -t {target} '#{{pane_dead}}'")
        require_active()
        return getattr(result, "return_code", 0) == 0 and (getattr(result, "stdout", "") or "").strip() == "0"

    session._loom_is_alive_without_recovery = is_alive_with_dead_pane_check

    recovered_once = False
    suppress_keys = False
    pending_notice: str | None = None

    async def _soft_recover_or_raise(cause: BaseException | None = None) -> None:
        nonlocal recovered_once, suppress_keys, pending_notice
        require_active()
        if record_loss is not None:
            # Persistence failure must preserve the dead pane and stop recovery.
            try:
                text = await _capture_harbor_session_loss(session)
                policy = (
                    "Failed keystrokes are not replayed; remaining commands in "
                    "this batch are suppressed.\n"
                    + ("Recovery budget exhausted; fail closed.\n" if recovered_once
                       else "One session recreation may be attempted after persistence.\n")
                )
                await record_loss(policy + text)
            except (AttemptDeadlineExceededError, AttemptTrajectoryFencedError):
                raise
            except Exception as exc:
                # Harbor treats TimeoutError from send_keys as a normal
                # command timeout and continues. A failed diagnostic is terminal.
                raise AgentError(
                    "Terminus2 tmux session loss detected; diagnostic persistence unavailable",
                ) from exc
        require_active()
        if recovered_once:
            raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH) from cause
        try:
            cwd = await _recreate_harbor_tmux_session(session)
        except (AgentError, AttemptDeadlineExceededError):
            raise
        except Exception as exc:
            raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH) from exc
        recovered_once = True
        suppress_keys = True
        pending_notice = _TMUX_SOFT_RECOVER_NOTICE.format(cwd=cwd)

    async def _send_keys_with_alive_check(*args: Any, **kwargs: Any) -> Any:
        require_active()
        if suppress_keys:
            # Remainder of the in-flight command batch after a soft recover.
            return None
        if not await is_alive_with_dead_pane_check():
            await _soft_recover_or_raise()
            return None
        try:
            result = await original_send_keys(*args, **kwargs)
        except Exception as exc:
            if not await is_alive_with_dead_pane_check():
                await _soft_recover_or_raise(exc)
                return None
            raise
        if not await is_alive_with_dead_pane_check():
            await _soft_recover_or_raise()
            return None
        return result

    session.send_keys = _send_keys_with_alive_check

    async def _alive_with_recovery() -> bool:
        # A long-running command can exit after send_keys' immediate check.
        # Harbor's next loop must retain diagnostics instead of silently return.
        if not await is_alive_with_dead_pane_check():
            await _soft_recover_or_raise()
        return True

    session.is_session_alive = _alive_with_recovery

    if original_get_incremental is None:
        return

    async def _get_incremental_with_recovery_notice(*args: Any, **kwargs: Any) -> str:
        nonlocal suppress_keys, pending_notice
        output = cast(str, await original_get_incremental(*args, **kwargs))
        if pending_notice is None:
            return output
        notice = pending_notice
        pending_notice = None
        suppress_keys = False
        return f"{notice}\n{output}"

    session.get_incremental_output = _get_incremental_with_recovery_notice


async def _publish_harbor_artifacts_to_sandbox(
    env: Driver,
    logs_root: Path,
    workdir: PurePosixPath,
) -> dict[str, PurePosixPath]:
    """Upload Harbor-native artifacts into the sandbox for step_runner collection."""
    agent_dir = workdir / ".loom/agent"
    published: dict[str, PurePosixPath] = {}
    for name in _HARBOR_ARTIFACT_NAMES:
        src = logs_root / name
        if not src.is_file():
            continue
        dst = agent_dir / name
        await env.upload(src, dst)
        published[name] = dst
    return published


def _scrub_harbor_trajectory_llm_kwargs(path: Path) -> None:
    """Drop Harbor's dumped ``agent.extra.llm_kwargs.api_key`` before publish.

    Harbor Terminus2 persists constructor ``llm_kwargs`` into trajectory.json.
    Loom passes the step JWT as ``api_key`` there; multi-model already redacts
    constructor metadata before dump, but single-model did not. Scrub the file
    so a late dump cannot publish the JWT.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict):
        return
    agent = payload.get("agent")
    if not isinstance(agent, dict):
        return
    extra = agent.get("extra")
    if not isinstance(extra, dict):
        return
    llm_kwargs = extra.get("llm_kwargs")
    if not isinstance(llm_kwargs, dict) or "api_key" not in llm_kwargs:
        return
    scrubbed = {
        key: value
        for key, value in llm_kwargs.items()
        if key != "api_key" and "loom_step_" not in str(value)
    }
    if scrubbed:
        extra["llm_kwargs"] = scrubbed
    else:
        extra.pop("llm_kwargs", None)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _assert_harbor_artifacts_have_no_step_secrets(
    logs_root: Path, direct_provider: DirectProviderConnection | None = None,
) -> None:
    """Fail closed if a step JWT leaked into Harbor's published dump.

    Match the real credential (``loom_step_`` JWT prefix). Do not treat a bare
    ``"api_key"`` JSON key as a secret: agents often read third-party package
    sources that contain that string, which previously false-failed publish
    after long exploratory runs (e.g. greenlinestanport).
    """
    trajectory_path = logs_root / "trajectory.json"
    if trajectory_path.is_file():
        _scrub_harbor_trajectory_llm_kwargs(trajectory_path)
    for name in _HARBOR_ARTIFACT_NAMES:
        path = logs_root / name
        if not path.is_file():
            continue
        body = path.read_bytes()
        if direct_provider is not None:
            text = body.decode("utf-8")
            cleaned = direct_provider.redact_text(text)
            if cleaned != text:
                path.write_text(cleaned, encoding="utf-8")
                body = cleaned.encode()
        if b"loom_step_" in body:
            raise AgentError(
                f"refusing to publish Harbor artifact {name}: step credential material is present",
            )


class _RouterEventSink:
    def __init__(
        self,
        *,
        bridge: HarborCheckpointBridge,
        trajectory: TrajectoryWriter,
        trial_id: UUID,
        step_id: str,
        student: ModelSpec,
        teacher: ModelSpec,
    ) -> None:
        self.bridge = bridge
        self.trajectory = trajectory
        self.trial_id = trial_id
        self.step_id = step_id
        self.student = student
        self.teacher = teacher

    async def on_switch(
        self,
        *,
        switch_episode: int,
        from_role: Role,
        to_role: Role,
    ) -> None:
        await self.bridge.emit_model_switch(
            switch_episode=switch_episode,
            from_role=from_role,
            to_role=to_role,
            from_model=self.student if from_role == "student" else self.teacher,
            to_model=self.teacher if to_role == "teacher" else self.student,
        )

    async def on_llm_started(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        first_of_role: bool,
    ) -> None:
        del first_of_role
        await self.trajectory.append(
            Terminus2LlmCallStartedEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self.trial_id,
                step_id=self.step_id,
                seq=0,
                client_call_id=UUID(client_call_id),
                episode=episode,
                call_ordinal=call_ordinal,
                role=role,
                requested_model=requested_model,
            ),
        )

    async def on_llm_completed(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        response_model: str | None,
    ) -> None:
        await self.trajectory.append(
            Terminus2LlmCallCompletedEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self.trial_id,
                step_id=self.step_id,
                seq=0,
                client_call_id=UUID(client_call_id),
                episode=episode,
                call_ordinal=call_ordinal,
                role=role,
                requested_model=requested_model,
                response_model=response_model,
            ),
        )

    async def on_llm_failed(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        error: str,
    ) -> None:
        await self.trajectory.append(
            Terminus2LlmCallFailedEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self.trial_id,
                step_id=self.step_id,
                seq=0,
                client_call_id=UUID(client_call_id),
                episode=episode,
                call_ordinal=call_ordinal,
                role=role,
                requested_model=requested_model,
                error=error,
            ),
        )


def _install_continuation_policy(agent: Any, deadline: AttemptDeadline) -> None:
    """Keep the pinned Harbor session running after real completion actions.

    Harbor remembers a completion request and returns on the next one. Reset
    only that instance's confirmation state: its parser, commands and recorded
    mark_task_complete actions remain untouched. The existing supervisor owns
    the absolute deadline, cancellation and terminal errors.
    """
    if not callable(getattr(agent, "_get_completion_confirmation_message", None)):
        raise AgentContinuationError("Harbor runtime does not support continue_until_timeout")

    def continue_message(terminal_output: str) -> str:
        remaining = deadline.require_remaining()
        agent._pending_completion = False
        return (
            f"Current terminal state:\n{terminal_output}\n\n"
            "This task declares agent.continue_until_timeout=true. "
            f"Continue checking and improving your work for the remaining {remaining:.1f} seconds. "
            "A completion request does not end this attempt; its deadline remains unchanged."
        )

    agent._get_completion_confirmation_message = continue_message


@dataclass
class LoomTerminus2Runtime:
    """Worker-side wrapper around pinned Harbor ``Terminus2``."""

    model: ModelSpec
    team_id: str
    trial_id: UUID
    cp_client: Any
    gateway_url: str
    direct_provider: DirectProviderConnection | None = field(default=None, repr=False)
    direct_ledger: DirectCallLedger | None = field(default=None, repr=False)
    name: str = "terminus-2"
    mode: Literal["out-of-box", "in-box"] = "in-box"
    version: str = LOOM_BRIDGE_REVISION
    supports_os: frozenset[OS] = field(default_factory=lambda: frozenset({"linux"}))
    emits_gateway_llm_call_events: bool = True
    provider_connection_id: str | None = None
    request_params: dict[str, Any] = field(default_factory=dict)
    harbor_config: HarborConfig | None = None
    multi_model: MultiModelSwitchSpec | None = None
    model_switch_plan: dict[str, Any] | None = None
    max_turns: int = 50
    continue_until_timeout: bool = False
    workdir: PurePosixPath = field(default_factory=lambda: PurePosixPath("/workspace"))
    step_token_ttl_sec: int = 1800
    local_artifact_sink: Callable[[Path], None] | None = field(default=None, repr=False)
    _attempt_deadline: AttemptDeadline | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _active_env: Driver | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.request_params = sanitize_request_extras(self.request_params)

    def begin_attempt(self, deadline: AttemptDeadline) -> None:
        self._attempt_deadline = deadline

    async def aclose_attempt(self) -> None:
        """Interrupt Harbor's attempt-owned tmux session on supervisor stop."""

        if self._active_env is not None:
            await _reset_harbor_tmux_session(self._active_env)

    async def setup(self, env: Driver) -> None:
        await ensure_sandbox_deps(env)
        await env.exec(f"mkdir -p {(self.workdir / '.loom/agent').as_posix()}")

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None:
        try:
            await self._run(instruction=instruction, env=env, trajectory=trajectory,
                            mcp=mcp, skills_dir=skills_dir, step_id=step_id)
        except Exception as exc:
            if self.direct_provider is None:
                raise
            cleaned = self.direct_provider.redact_text(str(exc))
            if cleaned == str(exc):
                raise
            raise AgentError(cleaned) from None

    async def _run(
        self, *, instruction: str, env: Driver, trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection], skills_dir: PurePosixPath | None, step_id: str,
    ) -> None:
        del mcp, skills_dir
        if self.direct_provider is not None:
            if self.direct_ledger is None:
                raise AgentError("direct Terminus requires a local usage ledger")
            if self.multi_model is not None and self.multi_model.enabled:
                raise AgentError("local Terminus does not support multi_model sessions")
        if self.continue_until_timeout and self._attempt_deadline is None:
            raise AgentContinuationError("continue_until_timeout requires an absolute attempt deadline")
        terminus2_cls, agent_context_cls = _import_terminus2()

        if self.direct_provider is not None:
            step_token = self.direct_provider.api_key
            if self._attempt_deadline is not None:
                self._attempt_deadline.require_remaining()
        elif self._attempt_deadline is None:
            step_token = await self.cp_client.mint_step_token(
                team_id=UUID(self.team_id),
                trial_id=self.trial_id,
                step_id=step_id,
                ttl_sec=self.step_token_ttl_sec,
            )
        else:
            grant = await self.cp_client.mint_attempt_step_token(
                team_id=UUID(self.team_id),
                trial_id=self.trial_id,
                step_id=step_id,
                ttl_sec=self.step_token_ttl_sec,
                attempt_deadline_wall_clock=self._attempt_deadline.wall_deadline,
                **(
                    {"agent_attempt_id": self._attempt_deadline.agent_attempt_id}
                    if self._attempt_deadline.agent_attempt_id is not None
                    else {}
                ),
            )
            if self._attempt_deadline.agent_attempt_id is not None and not grant.local_only:
                await self._attempt_deadline.record_step_token_grant(
                    agent_attempt_id=grant.agent_attempt_id,
                    step_jwt_id=grant.step_jwt_id,
                )
            step_token = grant.token
            self._attempt_deadline.require_remaining()
        api_base = self.direct_provider.base_url if self.direct_provider else _openai_gateway_base(self.gateway_url)

        logs_ctx = tempfile.TemporaryDirectory(
            prefix=f"loom-terminus2-{self.trial_id}-",
        )
        logs_root = Path(logs_ctx.name)
        self._active_env = env
        try:
            await self._run_harbor(
                instruction=instruction,
                env=env,
                trajectory=trajectory,
                step_id=step_id,
                step_token=step_token,
                api_base=api_base,
                logs_root=logs_root,
                terminus2_cls=terminus2_cls,
                agent_context_cls=agent_context_cls,
            )
        finally:
            try:
                # Only the native service adapter supplies this trusted local
                # sink. It retains partial files without reopening checkpoint,
                # trajectory, sandbox-upload or CP mutation fences at timeout.
                if self.local_artifact_sink is not None and self._attempt_deadline is not None:
                    if self._attempt_deadline.reached:
                        _assert_harbor_artifacts_have_no_step_secrets(logs_root, self.direct_provider)
                        self.local_artifact_sink(logs_root)
            finally:
                logs_ctx.cleanup()
                self._active_env = None

    async def _run_harbor(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        step_id: str,
        step_token: str,
        api_base: str | None,
        logs_root: Path,
        terminus2_cls: type,
        agent_context_cls: type,
    ) -> None:
        trial_paths = make_trial_paths(logs_root)
        trial_paths.mkdir()

        def redact_payload(value: Any) -> Any:
            value = redact_mapping(value)
            if self.direct_provider is not None:
                value = self.direct_provider.redact(value)
            if isinstance(value, str):
                return value.replace(step_token, "[REDACTED:step-token]") if step_token else value
            if isinstance(value, dict):
                return {key: redact_payload(item) for key, item in value.items()}
            if isinstance(value, list):
                return [redact_payload(item) for item in value]
            return value

        bridge = HarborCheckpointBridge(
            trajectory=trajectory,
            trial_id=self.trial_id,
            step_id=step_id,
            model=self.model,
            cp_client=self.direct_ledger if self.direct_provider else self.cp_client,
            emit_llm_call_events=self.direct_provider is None,
            redact_payload=redact_payload,
        )
        _require_attempt_mutation_active(trajectory, self._attempt_deadline)
        # These compatibility overrides are observable behavior, independent
        # of Harbor's source revision. Reuse the exact constructor options in
        # provenance so a version upgrade cannot silently claim default parity.
        harbor = self.harbor_config
        if isinstance(harbor, dict):
            harbor = HarborConfig.model_validate(harbor)
            self.harbor_config = harbor
        max_turns = self.max_turns
        enable_summarize = False
        if harbor is not None:
            if harbor.max_turns is not None:
                max_turns = harbor.max_turns
            if harbor.enable_summarize is not None:
                enable_summarize = harbor.enable_summarize
        harbor_options: dict[str, Any] = {
            "max_turns": None if self.continue_until_timeout else max_turns,
            "record_terminal_session": True,
            "enable_summarize": enable_summarize,
        }
        if harbor is not None:
            if harbor.proactive_summarization_threshold is not None:
                harbor_options["proactive_summarization_threshold"] = (
                    harbor.proactive_summarization_threshold
                )
            if harbor.max_thinking_tokens is not None:
                harbor_options["max_thinking_tokens"] = harbor.max_thinking_tokens
            if harbor.max_input_tokens is not None:
                harbor_options["model_info"] = {
                    "max_input_tokens": harbor.max_input_tokens,
                }
        await bridge.emit_provenance(effective_options={
            **harbor_options,
            "continue_until_timeout": self.continue_until_timeout,
            "multi_model": self.multi_model is not None and self.multi_model.enabled,
        })

        harbor_env = LoomHarborEnvironment.create(
            driver=env,
            trial_paths=trial_paths,
            workdir=self.workdir,
            trial_id=self.trial_id,
            step_id=step_id,
        )

        generation_params = sanitize_request_extras(self.request_params)
        if harbor is not None:
            for key, value in harbor.completion_parameters().items():
                current = generation_params.get(key)
                if current is not None and current != value:
                    raise AgentError(
                        f"harbor_config.{key} conflicts with request_params.{key}",
                    )
                generation_params[key] = value
        # Harbor binds reasoning_effort explicitly while expanding llm_kwargs.
        # Passing it in both places raises before the first model call.
        reasoning_effort = generation_params.pop("reasoning_effort", None)
        agent = terminus2_cls(
            logs_dir=logs_root,
            model_name=self.direct_provider.harbor_model_name if self.direct_provider else _harbor_model_name(self.model),
            api_base=api_base,
            session_id=None if self.direct_provider else str(self.trial_id),
            reasoning_effort=reasoning_effort,
            llm_kwargs={**generation_params, "api_key": step_token},
            **harbor_options,
        )
        if self.continue_until_timeout:
            assert self._attempt_deadline is not None
            _install_continuation_policy(agent, self._attempt_deadline)
        # Student LiteLLM already received the step JWT via constructor kwargs.
        # Multi-model invariant (all policies): never redact constructor metadata
        # before install_role_router. Teacher construction copies gateway auth
        # from those kwargs (and can recover from student._llm_kwargs).
        # install_role_router redacts the dump field only after the teacher
        # LiteLLM exists. Single-model redacts here.
        model_switch = None
        if self.multi_model is not None and self.multi_model.enabled:
            if self.multi_model.secondary_model is None:
                raise AgentError(
                    "multi_model.enabled requires secondary_model",
                )
            plan = self.model_switch_plan or {}
            mix_mode = str(
                plan.get("mix_mode") or self.multi_model.policy or "student_teacher_student"
            )
            student = self.model
            teacher = self.multi_model.secondary_model
            recovery = {"recovery": "fresh", "last_call_ordinal": 0}
            reclaim = getattr(self.cp_client, "reclaim_terminus_execution", None)
            if callable(reclaim):
                recovery = await reclaim(
                    trial_id=self.trial_id,
                    step_id=step_id,
                )
            # Harbor Terminus2 always boots at episode 1. If this execution
            # already has a checkpoint, starting again would merge a new
            # student run onto the partial log. Fail closed instead.
            if recovery.get("recovery") in {"recovery_failed", "resumed"}:
                await trajectory.append(
                    Terminus2RecoveryFailedEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=0,
                        reason=(
                            "unverifiable checkpoint"
                            if recovery.get("recovery") == "recovery_failed"
                            else (
                                "harbor cannot resume mid-session; refusing "
                                "to start episode 1 on the same trajectory"
                            )
                        ),
                        last_episode=recovery.get("last_episode"),
                    ),
                )
                raise AgentError(
                    "terminus-2 recovery_failed: Harbor cannot resume a "
                    "multi-model session mid-run; a new Harbor start would "
                    "merge a second episode-1 onto this trial's trajectory",
                )
            last_call_ordinal = recovery.get("last_call_ordinal")
            router_kwargs: dict[str, Any] = {
                "teacher_model_name": _harbor_model_name(teacher),
                "student_model_name": _harbor_model_name(student),
                "agent_execution_id": str(recovery.get("agent_execution_id") or ""),
                "agent_run_attempt_id": str(recovery.get("agent_run_attempt_id") or ""),
                "starting_call_ordinal": (
                    last_call_ordinal if isinstance(last_call_ordinal, int) else 0
                ),
            }
            if mix_mode == "beta_mixture":
                beta = plan.get("beta")
                if beta is None:
                    beta = self.multi_model.beta
                seed = plan.get("seed")
                if beta is None or not seed:
                    raise AgentError(
                        "model_switch_plan beta/seed must be materialized before terminus-2 run",
                    )
                await trajectory.append(
                    Terminus2ModelMixPlannedEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=0,
                        policy="beta_mixture",
                        beta=float(beta),
                        grain="episode",
                        seed_fingerprint=seed_fingerprint(str(seed)),
                        student_model=student,
                        teacher_model=teacher,
                    ),
                )
                router_kwargs.update(
                    mix_mode="beta_mixture",
                    beta=float(beta),
                    seed=str(seed),
                    trial_id=self.trial_id,
                )
            elif mix_mode == "student_to_teacher_turns":
                step_start = int(
                    plan.get("k1") or self.multi_model.step_start or 0,
                )
                step_end = int(
                    plan.get("k2") or self.multi_model.step_end or 0,
                )
                seed = plan.get("seed")
                if step_start < 2 or step_end <= step_start or not seed:
                    raise AgentError(
                        "model_switch_plan step_start/step_end/seed must be "
                        "materialized before terminus-2 run",
                    )
                await trajectory.append(
                    Terminus2ModelMixPlannedEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=0,
                        policy="student_to_teacher_turns",
                        beta=None,
                        grain="turn",
                        step_start=step_start,
                        step_end=step_end,
                        seed_fingerprint=seed_fingerprint(str(seed)),
                        student_model=student,
                        teacher_model=teacher,
                    ),
                )
                router_kwargs.update(
                    mix_mode="student_to_teacher_turns",
                    step_start=step_start,
                    step_end=step_end,
                    seed=str(seed),
                    trial_id=self.trial_id,
                )
            else:
                k1 = int(plan.get("k1") or self.multi_model.switch_episode or 0)
                k2 = int(plan.get("k2") or self.multi_model.return_switch_episode or 0)
                if k1 < 2 or k2 <= k1:
                    raise AgentError(
                        "model_switch_plan K1/K2 must be materialized before terminus-2 run",
                    )
                await trajectory.append(
                    Terminus2ModelSwitchPlannedEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=0,
                        switch_episode=k1,
                        from_role="student",
                        to_role="teacher",
                        from_model=student,
                        to_model=teacher,
                    ),
                )
                await trajectory.append(
                    Terminus2ModelSwitchPlannedEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=0,
                        switch_episode=k2,
                        from_role="teacher",
                        to_role="student",
                        from_model=teacher,
                        to_model=student,
                    ),
                )
                router_kwargs.update(
                    first_switch_episode=k1,
                    return_switch_episode=k2,
                )
            sink = _RouterEventSink(
                bridge=bridge,
                trajectory=trajectory,
                trial_id=self.trial_id,
                step_id=step_id,
                student=student,
                teacher=teacher,
            )
            model_switch = install_role_router(
                agent,
                event_sink=sink,
                **router_kwargs,
            )
            self._terminus_execution_id = recovery.get("agent_execution_id")
            self._terminus_run_attempt_id = recovery.get("agent_run_attempt_id")
        else:
            redact_agent_llm_kwargs(agent)

        if self.direct_ledger is not None and self.direct_provider is not None:
            self.direct_ledger.install(agent, step_id=step_id, trajectory=trajectory)

        context = agent_context_cls()
        trajectory_path = logs_root / "trajectory.json"
        completeness = "full"
        poll_stop = asyncio.Event()

        last_checkpointed_episode = 0

        async def _write_episode_checkpoint() -> None:
            nonlocal last_checkpointed_episode
            _require_attempt_mutation_active(trajectory, self._attempt_deadline)
            if model_switch is None:
                return
            post = getattr(self.cp_client, "post_episode_checkpoint", None)
            exec_id = getattr(self, "_terminus_execution_id", None)
            attempt_id = getattr(self, "_terminus_run_attempt_id", None)
            if not callable(post) or not exec_id or not attempt_id:
                return
            episode = int(getattr(agent, "_n_episodes", 0) or 0)
            if episode < 1 or episode <= last_checkpointed_episode:
                return
            role = model_switch.role()
            post_kwargs = {
                "trial_id": self.trial_id,
                "execution_id": UUID(str(exec_id)),
                "run_attempt_id": UUID(str(attempt_id)),
                "episode": episode,
                "active_role": role,
                "last_call_ordinal": int(getattr(model_switch, "_call_ordinal", 0) or 0),
                "last_seq": int(getattr(trajectory, "_next_seq", 0) or 0),
                "attempt_deadline_wall_clock": (
                    self._attempt_deadline.wall_deadline
                    if self._attempt_deadline is not None
                    else None
                ),
            }
            if self._attempt_deadline is None:
                checksum_row = await post(**post_kwargs)
            else:
                async with asyncio.timeout(self._attempt_deadline.require_remaining()):
                    checksum_row = await post(**post_kwargs)
            _require_attempt_mutation_active(trajectory, self._attempt_deadline)
            last_checkpointed_episode = episode
            await trajectory.append(
                Terminus2EpisodeCheckpointEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self.trial_id,
                    step_id=step_id,
                    seq=0,
                    episode=episode,
                    active_role=role,
                    checksum=str(checksum_row.get("checksum") or ""),
                    last_call_ordinal=int(getattr(model_switch, "_call_ordinal", 0) or 0),
                ),
            )

        async def _poll_checkpoints() -> None:
            while not poll_stop.is_set():
                _require_attempt_mutation_active(trajectory, self._attempt_deadline)
                await bridge.sync_trajectory_file(
                    trajectory_path,
                    allow_incomplete=True,
                )
                await _write_episode_checkpoint()
                try:
                    await asyncio.wait_for(poll_stop.wait(), timeout=0.5)
                except TimeoutError:
                    continue

        async def _run_session() -> None:
            await _reset_harbor_tmux_session(env)
            await agent.setup(harbor_env)
            session = getattr(agent, "_session", None)
            if session is not None:
                def require_active() -> None:
                    _require_attempt_mutation_active(trajectory, self._attempt_deadline)

                async def record_dispatch(step: dict[str, Any], episode: int) -> None:
                    require_active()
                    previous = [entry.model_dump(mode="json") for entry in agent._trajectory_steps]
                    if self._attempt_deadline is None:
                        await bridge.record_dispatch(step, episode=episode, preceding_steps=previous)
                    else:
                        async with asyncio.timeout(self._attempt_deadline.require_remaining()):
                            await bridge.record_dispatch(step, episode=episode, preceding_steps=previous)
                    require_active()

                async def record_loss(text: str) -> None:
                    require_active()
                    if self._attempt_deadline is None:
                        await bridge.record_session_loss(text)
                    else:
                        async with asyncio.timeout(self._attempt_deadline.require_remaining()):
                            await bridge.record_session_loss(text)
                    require_active()

                session._loom_require_attempt_active = require_active
                session._loom_attempt_deadline = self._attempt_deadline
                require_active()
                await _retain_harbor_dead_pane(session)
                require_active()
                install_dispatch_checkpoint(agent, record=record_dispatch, require_active=require_active)
                _install_tmux_session_alive_guard(
                    agent, record_loss=record_loss, require_active=require_active,
                )
            await agent.run(instruction, harbor_env, context)

        async def _supervise_session() -> None:
            poll_task = asyncio.create_task(_poll_checkpoints())
            session_task = asyncio.create_task(_run_session())
            try:
                done, _ = await asyncio.wait(
                    (poll_task, session_task), return_when=asyncio.FIRST_COMPLETED,
                )
                if poll_task in done:
                    # Persistence/resource failures are terminal while Harbor
                    # is running, not deferred until its next completion.
                    poll_task.result()
                await session_task
                if poll_task.done():
                    poll_task.result()
            finally:
                poll_stop.set()
                for task in (poll_task, session_task):
                    if not task.done():
                        task.cancel()
                # Never leave a session or writer running after this attempt.
                await asyncio.gather(poll_task, session_task, return_exceptions=True)

        try:
            await _supervise_session()
        except asyncio.CancelledError:
            completeness = "partial"
            try:
                _require_attempt_mutation_active(trajectory, self._attempt_deadline)
                await bridge.sync_trajectory_file(
                    trajectory_path,
                    completeness=completeness,
                )
                await _write_episode_checkpoint()
            except (
                AttemptDeadlineExceededError,
                AttemptTrajectoryFencedError,
                CheckpointBridgeError,
            ):
                pass
            raise
        except (AgentError, AttemptDeadlineExceededError):
            raise
        except Exception as exc:
            # Harbor tmux/session failures are bare RuntimeError; wrap so
            # step_runner keeps an actionable message (#1068).
            info = exception_info(exc)
            raise AgentError(f"{info.exception_type}: {info.exception_message}") from exc
        finally:
            try:
                _require_attempt_mutation_active(trajectory, self._attempt_deadline)
                await bridge.sync_trajectory_file(
                    trajectory_path,
                    completeness=completeness,
                )
                await _write_episode_checkpoint()
                _require_attempt_mutation_active(trajectory, self._attempt_deadline)
                _assert_harbor_artifacts_have_no_step_secrets(logs_root, self.direct_provider)
                if self._attempt_deadline is None:
                    sandbox_paths = await _publish_harbor_artifacts_to_sandbox(
                        env,
                        logs_root,
                        self.workdir,
                    )
                else:
                    async with asyncio.timeout(self._attempt_deadline.require_remaining()):
                        sandbox_paths = await _publish_harbor_artifacts_to_sandbox(
                            env,
                            logs_root,
                            self.workdir,
                        )
                _require_attempt_mutation_active(trajectory, self._attempt_deadline)
                await bridge.emit_artifact_refs(logs_root, sandbox_paths=sandbox_paths)
            except CheckpointBridgeError as exc:
                raise AgentError(str(exc)) from exc
            except (AttemptDeadlineExceededError, AttemptTrajectoryFencedError):
                pass

        if not trajectory_path.is_file():
            raise AgentError(
                f"Harbor Terminus2 did not produce trajectory.json "
                f"(harbor_compat={HARBOR_COMPAT_SHA})",
            )
