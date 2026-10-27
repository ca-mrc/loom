"""SubprocessAgent — generic AgentRuntime wrapping any loom_launcher AgentAdapter.

This is the worker-side glue that bridges loom's Driver Protocol to
loom_launcher's SandboxAccess Protocol (A11.2) and the launcher's
ExecHandle facade to loom's exec_streaming output. The launcher package
is sandbox-safe (no loom imports); SubprocessAgent lives in loom proper
and depends on both sides.

Per the agent integrations spec §4, each step gets a fresh agent.run()
invocation; multi-turn sessions across steps are v1.5.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from loom_launcher.adapter import AgentAdapter, SandboxAccess
from loom_launcher.adapter import ExecHandle as LauncherExecHandle
from loom_launcher.adapter import ModelSpec as LauncherModelSpec

from loom.agent.direct_provider import DirectProviderConnection
from loom.attempt_deadline import AttemptDeadline
from loom.driver.base import Driver
from loom.driver.base import ExecHandle as DriverExecHandle
from loom.errors import AgentError
from loom.models.mcp import MCPConnection
from loom.models.trajectory import AgentThoughtEvent, EventKind
from loom.models.types import OS, ModelSpec
from loom.request_params import sanitize_request_extras
from loom.security.redaction import redact_mapping, redact_text
from loom.trajectory.writer import TrajectoryWriter
from loom_worker.control_plane_client import StepTokenClient

logger = logging.getLogger(__name__)
_LOOM_EVENT_REQUIRED_KEYS = frozenset({"kind", "emitted_at", "trial_id", "step_id", "seq"})
_LOOM_EVENT_KINDS = frozenset(kind.value for kind in EventKind)
_SUBPROCESS_AGENT_ENV_PASSTHROUGH = (
    "LOOM_CODEX_SETTINGS_JSON",
    "LOOM_OPENHANDS_TERMINUS_STYLE",
)


def _bridge_driver(driver: Driver, *, cwd: PurePosixPath) -> SandboxAccess:
    """Adapt loom.driver.base.Driver to loom_launcher.SandboxAccess.

    `tail_log_file` calls `sandbox.read_text(path)` — implemented via
    a one-shot `cat` exec in the sandbox.
    `poll_local_http` calls `sandbox.exec_oneshot(argv)` — implemented
    directly via the Driver's buffered exec.

    The launcher Protocol mirrors a strict subset of Driver; this
    function makes a real Driver satisfy it without leaking the full
    Driver surface to the launcher (which has to stay sandbox-safe).
    """

    class _Bridge:
        async def read_text(self, path: PurePosixPath) -> str:
            # Use `test -e && cat` so we can distinguish "file doesn't
            # exist yet" (FileNotFoundError, which polling adapters
            # interpret as "keep waiting") from a real I/O failure
            # (OSError, which we surface so the trial fails fast rather
            # than looping forever on a misconfigured volume).
            path_q = shlex.quote(str(path))
            result = await driver.exec(
                f"if [ -e {path_q} ]; then cat {path_q}; else exit 66; fi",
                cwd=cwd,
                timeout_sec=10.0,
            )
            if result.return_code == 66:
                raise FileNotFoundError(str(path))
            if result.return_code != 0:
                stderr = result.stderr.decode("utf-8", errors="replace")[:200]
                raise OSError(
                    f"sandbox read_text({path}) failed rc={result.return_code}: {stderr}",
                )
            return result.stdout.decode("utf-8", errors="replace")

        async def exec_oneshot(
            self,
            argv: list[str],
            *,
            timeout_sec: float = 10.0,
        ) -> tuple[int, bytes]:
            cmd = " ".join(shlex.quote(a) for a in argv)
            result = await driver.exec(cmd, cwd=cwd, timeout_sec=timeout_sec)
            return (result.return_code, result.stdout)

    return _Bridge()


def _bridge_exec_handle(
    driver_handle: DriverExecHandle,
    sandbox: SandboxAccess,
) -> LauncherExecHandle:
    """Wrap a loom.driver.base.ExecHandle as a loom_launcher.ExecHandle
    with the SandboxAccess side-channel populated (A11.2)."""
    return LauncherExecHandle(
        pid=driver_handle.pid,
        stdout=driver_handle.stdout,
        stderr=driver_handle.stderr,
        _wait=driver_handle._wait,
        _kill=driver_handle._kill,
        sandbox=sandbox,
    )


def _bridge_model(model: ModelSpec) -> LauncherModelSpec:
    """Adapt loom's ModelSpec to the launcher's (duplicated) ModelSpec."""
    return LauncherModelSpec(
        provider=model.provider,
        name=model.name,
        tier=model.tier,
        region=model.region,
    )


def _gateway_url_for_adapter(base_url: str, adapter: AgentAdapter) -> str:
    """Return the sandbox-facing gateway base URL for the adapter dialect.

    `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` may be a bare gateway-router host URL
    (`...:30443`) or the OpenAI facade (`.../openai/v1`) because the first
    subprocess agents were OpenAI-compatible. SDKs append their own provider
    paths after the configured root, so each adapter receives the facade root
    for its declared dialect.
    """
    parts = urlsplit(base_url.rstrip("/"))
    path = parts.path.rstrip("/")
    dialect = adapter.endpoint_dialect

    def _invalid(expected: str) -> AgentError:
        return AgentError(
            f"{adapter.name}: incompatible subprocess gateway URL {base_url!r} "
            f"for endpoint dialect {dialect!r}; expected {expected}, got path "
            f"{path or '/'}",
        )

    if dialect in {"openai_chat", "openai_responses"}:
        if path in {"/anthropic", "/google"} or path.startswith(("/anthropic/", "/google/")):
            raise _invalid("a bare gateway root or /openai/v1")
        if not path or path == "/openai":
            return urlunsplit(parts._replace(path="/openai/v1"))
        if path.startswith("/openai/v1/"):
            raise _invalid("/openai/v1, not a concrete OpenAI endpoint path")
        return urlunsplit(parts._replace(path=path))

    if dialect == "gemini":
        if not path or path in {"/openai", "/openai/v1"} or path.startswith("/openai/v1/"):
            return urlunsplit(parts._replace(path="/google"))
        if path == "/google":
            return urlunsplit(parts._replace(path=path))
        if path.startswith("/google/") or path.startswith("/anthropic"):
            raise _invalid("a bare gateway root or /google")
        return base_url

    if dialect != "anthropic":
        return base_url

    if path == "/anthropic":
        return urlunsplit(parts._replace(path=path))
    if path.startswith("/anthropic/") or path.startswith("/google"):
        raise _invalid("a bare gateway root or /anthropic")
    if path in {"/openai", "/openai/v1"} or path.startswith("/openai/v1/"):
        return urlunsplit(parts._replace(path="/anthropic"))
    if not path:
        return urlunsplit(parts._replace(path="/anthropic"))
    return base_url


@dataclass
class SubprocessAgent:
    """Generic AgentRuntime wrapping any loom_launcher AgentAdapter.

    Holds `adapter` + `model` + `cp_client` + `gateway_url` + `team_id`
    + `trial_id` at construction. Each `run()` invocation mints a fresh
    step-scoped JWT (so the agent's API key is per-step + auto-expiring),
    builds env + argv via the adapter, exec_streaming's the agent, and
    forwards adapter-emitted events to the trajectory writer.
    """

    adapter: AgentAdapter
    model: ModelSpec
    cp_client: StepTokenClient | None
    gateway_url: str
    team_id: UUID
    trial_id: UUID
    agent_gateway_url: str | None = None
    direct_provider: DirectProviderConnection | None = field(default=None, repr=False)
    # Standard AgentRuntime Protocol fields:
    mode: Literal["out-of-box", "in-box"] = "out-of-box"
    name: str = field(init=False)
    version: str = "1.0"
    supports_os: frozenset[OS] = field(init=False)
    workdir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

    # Optional per-step JWT TTL override; defaults to 1800s (30 min) per
    # spec §6.1 typical step_timeout.
    step_token_ttl_sec: int = 1800
    request_params: dict[str, object] = field(default_factory=dict)
    _attempt_deadline: AttemptDeadline | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _active_exec_handle: DriverExecHandle | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _active_step_id: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.name = self.adapter.name
        # The adapter declares OS as `frozenset[str]`; loom's AgentRuntime
        # Protocol expects `frozenset[OS]` (a Literal alias). They're
        # structurally identical at runtime.
        self.supports_os = cast(frozenset[OS], self.adapter.supports_os)

    def begin_attempt(self, deadline: AttemptDeadline) -> None:
        self._attempt_deadline = deadline

    async def aclose_attempt(self) -> None:
        """Interrupt the exact subprocess owned by the active attempt."""

        handle = self._active_exec_handle
        step_id = self._active_step_id
        if handle is not None and step_id is not None:
            await _kill_exec_handle(
                handle,
                adapter_name=self.adapter.name,
                step_id=step_id,
            )

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
        if self.direct_provider is not None and (
            self.request_params or (
                self.adapter.name == "codex" and os.environ.get("LOOM_CODEX_SETTINGS_JSON")
            )
        ):
            raise AgentError(
                "direct subprocess request_params and Codex gateway settings are unsupported; "
                "use native agent configuration or terminus-2"
            )
        # 1. Mint a step-scoped JWT (Plan 9). Errors here are fatal: we
        # can't run the agent without a Gateway-acceptable bearer.
        try:
            if self.direct_provider is not None:
                step_token = self.direct_provider.api_key
            elif self.cp_client is None:
                raise AgentError("hosted subprocess requires a step-token client")
            elif self._attempt_deadline is None:
                step_token = await self.cp_client.mint_step_token(
                    team_id=self.team_id,
                    trial_id=self.trial_id,
                    step_id=step_id,
                    ttl_sec=self.step_token_ttl_sec,
                )
            else:
                grant = await self.cp_client.mint_attempt_step_token(
                    team_id=self.team_id,
                    trial_id=self.trial_id,
                    step_id=step_id,
                    ttl_sec=self.step_token_ttl_sec,
                    attempt_deadline_wall_clock=(self._attempt_deadline.wall_deadline),
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
        except Exception as exc:
            raise AgentError(
                f"{self.adapter.name}: failed to mint step token: {exc}",
            ) from exc

        # 2. Build env + argv via the adapter.
        base_url = (self.direct_provider.base_url or "") if self.direct_provider else _gateway_url_for_adapter(
            self.agent_gateway_url or self.gateway_url,
            self.adapter,
        )
        logger.info(
            "subprocess_agent_gateway_config adapter=%s dialect=%s base_url=%s",
            self.adapter.name,
            self.adapter.endpoint_dialect,
            base_url,
        )
        env_vars: dict[str, str] = {
            (self.direct_provider.api_key_env if self.direct_provider and self.direct_provider.api_key_env
             else self.adapter.api_key_env): step_token,
            self.adapter.base_url_env: base_url,
            "LOOM_TRIAL_ID": str(self.trial_id),
            "LOOM_STEP_ID": step_id,
        }
        if self.direct_provider and self.adapter.name == "claude-code":
            env_vars["ANTHROPIC_AUTH_TOKEN"] = ""
        for name in _SUBPROCESS_AGENT_ENV_PASSTHROUGH:
            value = os.environ.get(name)
            if value:
                env_vars[name] = value
        if self.adapter.name == "codex" and self.request_params:
            env_vars["LOOM_CODEX_SETTINGS_JSON"] = json.dumps(
                sanitize_request_extras(self.request_params),
                separators=(",", ":"),
            )
        cwd = self.workdir
        argv = self.adapter.build_invocation(
            instruction=instruction,
            workdir=cwd,
            model=_bridge_model(self.direct_provider.model if self.direct_provider else self.model),
            env=env_vars,
        )

        # 3. Streaming exec inside the sandbox.
        if self._attempt_deadline is not None:
            self._attempt_deadline.require_remaining()
        driver_handle = await env.exec_streaming(
            argv,
            env_vars=env_vars,
            cwd=cwd,
        )
        self._active_exec_handle = driver_handle
        self._active_step_id = step_id
        stderr_task = asyncio.create_task(_collect_stream_tail(driver_handle.stderr))

        # 4. Build the launcher-side ExecHandle with SandboxAccess wired in.
        sandbox = _bridge_driver(env, cwd=cwd)
        launcher_handle = _bridge_exec_handle(driver_handle, sandbox)

        # 5. Forward adapter events into the trajectory.
        event_seq = 0
        useful_events = 0
        capture_warning: dict[str, object] | None = None
        failure_diagnostics = _AdapterFailureDiagnostics()
        process_finished = False
        try:
            async for event in self.adapter.capture_events(
                exec_handle=launcher_handle,
                step_id=step_id,
                trial_id=self.trial_id,
            ):
                # The launcher emits TrajectoryEventLike (dict-like); the
                # trajectory writer accepts dicts via .write_raw_dict (added
                # by Plan 11 task 4) or pre-validates against the event union.
                # For v1 we use write_raw_dict to stay decoupled.
                payload = event.model_dump()
                if self.direct_provider is not None:
                    payload = self.direct_provider.redact(payload)
                failure_diagnostics.observe(payload)
                # #321: capture helpers emit a synthetic terminal event
                # `{"kind": "stream_capture_warning", ...}` when they
                # silently dropped malformed lines. Hold onto the last one
                # so we can include it in the AgentError when the process
                # finishes without any usable events.
                if payload.get("kind") == "stream_capture_warning":
                    capture_warning = payload
                if _is_complete_loom_event_payload(payload):
                    await trajectory.write_raw_dict(payload)
                    useful_events += 1
                else:
                    await trajectory.append(
                        AgentThoughtEvent(
                            emitted_at=datetime.now(UTC),
                            trial_id=self.trial_id,
                            step_id=step_id,
                            seq=event_seq,
                            content=_adapter_payload_to_content(payload),
                        )
                    )
                    event_seq += 1
                    # Plain AgentThoughtEvents are "useful" too (claude-code,
                    # codex, etc. fall through to this branch). Only the
                    # synthetic stream_capture_warning is excluded.
                    if payload.get("kind") != "stream_capture_warning":
                        useful_events += 1

            rc = await driver_handle.wait()
            process_finished = True
            stderr_tail = await _finish_tail_task(stderr_task)
        except BaseException:
            if not process_finished:
                await _kill_exec_handle(
                    driver_handle,
                    adapter_name=self.adapter.name,
                    step_id=step_id,
                )
            await _cancel_tail_task(stderr_task)
            raise
        finally:
            self._active_exec_handle = None
            self._active_step_id = None

        if rc != 0:
            detail = f"{self.adapter.name} exited rc={rc} on step {step_id}"
            diagnostics = failure_diagnostics.format_summary()
            if diagnostics:
                detail = f"{detail}; stdout: {diagnostics}"
            if stderr_tail:
                # Redact provider keys / bearer tokens / signed URLs
                # from the captured stderr before it lands in the
                # persisted failure_message (#321).
                detail = f"{detail}; stderr: {redact_text(stderr_tail)}"
            if capture_warning is not None:
                detail = (
                    f"{detail}; capture: skipped "
                    f"{capture_warning.get('skipped_lines')} malformed "
                    f"output line(s), last reason: "
                    f"{capture_warning.get('last_skip_reason')!r}"
                )
            raise AgentError(
                detail,
            )
        # #321: rc==0 BUT zero usable events captured. The agent process
        # finished cleanly but its output was unparseable (the bfcl/hello
        # symptom from #316). Surface the capture warning explicitly so
        # the trial gets a real failure_message instead of a downstream
        # empty INTERNAL_ERROR.
        if useful_events == 0 and capture_warning is not None:
            raw_sample = str(capture_warning.get("last_skip_sample", ""))
            # last_skip_sample is the first bytes of an unparseable
            # stdout line — could carry an env-leaked API key or other
            # secret. Redact before it lands in failure_message.
            sample = redact_text(raw_sample) if raw_sample else ""
            sample_text = f"; first bad line: {sample!r}" if sample else ""
            raise AgentError(
                f"{self.adapter.name} emitted no parseable events on "
                f"step {step_id} (rc=0, but "
                f"{capture_warning.get('skipped_lines')} output line(s) "
                f"were malformed JSONL; "
                f"last reason: {capture_warning.get('last_skip_reason')!r}"
                f"{sample_text})",
            )


def _is_complete_loom_event_payload(payload: dict[str, object]) -> bool:
    kind = payload.get("kind")
    return (
        isinstance(kind, str)
        and kind in _LOOM_EVENT_KINDS
        and _LOOM_EVENT_REQUIRED_KEYS.issubset(payload.keys())
    )


def _adapter_payload_to_content(payload: dict[str, object]) -> str:
    line = payload.get("line")
    if isinstance(line, str):
        return line
    return json.dumps(payload, sort_keys=True)


@dataclass
class _AdapterFailureDiagnostics:
    terminal_error: str | None = None
    fallback_error: str | None = None
    permission_denials: int = 0
    first_permission_denial: str | None = None

    def observe(self, payload: dict[str, object]) -> None:
        terminal_error = _extract_terminal_error(payload)
        if terminal_error is not None:
            if _is_terminal_result_error(payload):
                self.terminal_error = terminal_error
            elif self.terminal_error is None:
                self.fallback_error = terminal_error

        denial_count, first_denial = _extract_permission_denials(payload)
        if denial_count:
            self.permission_denials += denial_count
            if self.first_permission_denial is None:
                self.first_permission_denial = first_denial

    def format_summary(self) -> str:
        parts: list[str] = []
        error = self.terminal_error or self.fallback_error
        if error:
            parts.append(error)
        if self.permission_denials:
            denial = f"permission_denials={self.permission_denials}"
            if self.first_permission_denial:
                denial = f"{denial} (first: {self.first_permission_denial})"
            parts.append(denial)
        return "; ".join(parts)


def _is_terminal_result_error(payload: dict[str, object]) -> bool:
    return payload.get("type") == "result" and payload.get("is_error") is True


def _extract_terminal_error(payload: dict[str, object]) -> str | None:
    if _is_terminal_result_error(payload):
        return _extract_diagnostic_text(
            payload,
            ("result", "error", "message", "detail", "details", "reason"),
        )

    if payload.get("is_error") is True:
        return _extract_diagnostic_text(
            payload,
            ("error", "message", "detail", "details", "reason", "result"),
        )

    event_type = payload.get("type")
    event_subtype = payload.get("subtype")
    if (isinstance(event_type, str) and "error" in event_type.lower()) or (
        isinstance(event_subtype, str) and "error" in event_subtype.lower()
    ):
        return _extract_diagnostic_text(
            payload,
            ("error", "message", "detail", "details", "reason", "result"),
        )

    return None


def _extract_diagnostic_text(
    payload: dict[str, object],
    candidate_keys: Sequence[str],
) -> str | None:
    for key in candidate_keys:
        if key not in payload:
            continue
        text = _diagnostic_value_to_text(payload[key])
        if text:
            return _redact_diagnostic_text(text)
    return None


def _diagnostic_value_to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool | int | float):
        return str(value)
    if isinstance(value, list):
        parts = [_diagnostic_value_to_text(item) for item in value[:5]]
        return "; ".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in (
            "text",
            "content",
            "message",
            "error",
            "detail",
            "details",
            "reason",
            "result",
        ):
            if key in value:
                text = _diagnostic_value_to_text(value[key])
                if text:
                    return text
        return json.dumps(redact_mapping(value), sort_keys=True)
    return str(value).strip()


def _redact_diagnostic_text(value: str, *, limit: int = 500) -> str:
    return redact_text(value, limit=limit).strip()


def _extract_permission_denials(payload: dict[str, object]) -> tuple[int, str | None]:
    count = 0
    first: str | None = None

    def add_denial(value: object) -> None:
        nonlocal count, first
        if isinstance(value, list):
            count += len(value)
            if first is None and value:
                first = _summarize_permission_denial(value[0])
            return
        if isinstance(value, dict):
            explicit_count = value.get("count")
            if isinstance(explicit_count, int):
                count += explicit_count
            else:
                count += 1
            if first is None:
                first = _summarize_permission_denial(value)
            return
        if isinstance(value, int):
            count += value
            return
        if isinstance(value, str):
            count += 1
            if first is None:
                first = _redact_diagnostic_text(value)

    def walk(value: object) -> None:
        if isinstance(value, dict):
            if value.get("type") == "permission_denial":
                add_denial(value)
                return
            for key, item in value.items():
                normalized = key.lower().replace("-", "_")
                if normalized in {
                    "permission_denial",
                    "permission_denials",
                    "permission_denial_count",
                    "permissiondenial",
                    "permissiondenials",
                    "permissiondenialcount",
                }:
                    add_denial(item)
                else:
                    walk(item)
            return
        if isinstance(value, list):
            for item in value:
                walk(item)

    walk(payload)
    return count, first


def _summarize_permission_denial(value: object) -> str | None:
    if isinstance(value, str):
        return _redact_diagnostic_text(value)
    if not isinstance(value, dict):
        return None

    tool = _first_string_value(value, ("tool_name", "tool", "name"))
    pattern = _first_string_value(value, ("pattern", "command", "input", "reason"))
    if tool and pattern:
        return _redact_diagnostic_text(f"{tool} {pattern}")
    if tool:
        return _redact_diagnostic_text(tool)
    if pattern:
        return _redact_diagnostic_text(pattern)
    return _redact_diagnostic_text(json.dumps(redact_mapping(value), sort_keys=True))


def _first_string_value(value: dict[str, object], keys: Sequence[str]) -> str | None:
    for key in keys:
        item = value.get(key)
        if isinstance(item, str) and item.strip():
            return item.strip()
    return None


async def _collect_stream_tail(
    stream: AsyncIterator[bytes],
    *,
    max_bytes: int = 4096,
) -> str:
    buf = bytearray()
    async for chunk in stream:
        buf.extend(chunk)
        if len(buf) > max_bytes:
            del buf[: len(buf) - max_bytes]
    return bytes(buf).decode("utf-8", errors="replace").strip()


async def _finish_tail_task(task: asyncio.Task[str]) -> str:
    try:
        return await asyncio.wait_for(task, timeout=1.0)
    except TimeoutError:
        task.cancel()
        return ""
    except Exception:
        return ""


async def _kill_exec_handle(
    handle: DriverExecHandle,
    *,
    adapter_name: str,
    step_id: str,
) -> None:
    kill_task = asyncio.create_task(handle.kill())
    try:
        await asyncio.wait_for(asyncio.shield(kill_task), timeout=5.0)
    except TimeoutError:
        kill_task.cancel()
        try:
            await kill_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        logger.warning(
            "%s exec kill timed out during step %s cleanup",
            adapter_name,
            step_id,
        )
    except Exception:
        logger.warning(
            "%s exec kill failed during step %s cleanup",
            adapter_name,
            step_id,
            exc_info=True,
        )


async def _cancel_tail_task(task: asyncio.Task[str]) -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
