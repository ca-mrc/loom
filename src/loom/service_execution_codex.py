"""Codex CLI on native execution, as an installed harness (#2311).

The pinned Codex binary runs inside the task sandbox, supervised by this
trusted controller through `exec_streaming`. It reaches the model only through
the Pod-local broker (see `installed_agent_model_environment`). Its `exec
--json` stream is kept as native evidence and mapped to typed trajectory
events. Model calls come from the Gateway ledger, never from Codex's own
reporting, so accounting, model identity and cost are authoritative.
"""

from __future__ import annotations

import asyncio
import json
import math
import sys
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.attempt_deadline import AttemptDeadline
from loom.errors import AgentError
from loom.hosted_harness import CODEX, CODEX_HOME, CODEX_INSTALL_ROOT, CODEX_TMPDIR
from loom.models.task import TaskConfig
from loom.models.trajectory import (
    AgentThoughtEvent,
    LLMCallEvent,
    ToolUseEvent,
    TrajectoryEvent,
)
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.llm_call_events import llm_call_diagnostic_counts, llm_call_row_to_event

CODEX_USAGE_SCHEMA = "loom.service-execution-codex-usage.v1"
CODEX_EXTRA_CONFIG = ('web_search="disabled"',)
CODEX_NATIVE_EVENTS = Path("codex/events.jsonl")
# Bound the native evidence and any single mapped text field.
MAX_NATIVE_BYTES = 64 * 1024 * 1024
_MAX_TEXT = 64 * 1024
_EVENT: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
_COUNTERS = ("input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "thinking_tokens")
_CANONICAL_KINDS = (ToolUseEvent, AgentThoughtEvent, LLMCallEvent)


def _text(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= _MAX_TEXT else text[:_MAX_TEXT] + "…[truncated]"


def map_codex_event(
    payload: Mapping[str, Any], *, trial_id: UUID, emitted_at: datetime,
) -> TrajectoryEvent | None:
    """One Codex `exec --json` line to a canonical event, or None.

    Only completed items and failures become canonical events; lifecycle
    lines (thread/turn started, item updates, usage) stay native evidence.
    """
    base = {"trial_id": trial_id, "step_id": "agent", "seq": 0, "emitted_at": emitted_at}
    kind = payload.get("type")
    if kind in {"turn.failed", "error"}:
        error = payload.get("error") if kind == "turn.failed" else payload
        message = error.get("message") if isinstance(error, Mapping) else None
        return AgentThoughtEvent(**base, content=_text(message or error), sdk_event_type=f"codex.{kind}")
    if kind != "item.completed" or not isinstance(payload.get("item"), Mapping):
        return None
    item: Mapping[str, Any] = payload["item"]
    item_type = item.get("type")
    if item_type == "command_execution":
        return ToolUseEvent(
            **base, tool_name="shell", args={"command": _text(item.get("command", ""))},
            result={"exit_code": item.get("exit_code"), "status": item.get("status"),
                    "output": _text(item.get("aggregated_output", ""))},
            duration_sec=0.0,
        )
    if item_type == "file_change":
        raw_changes = item.get("changes")
        changes: list[Any] = raw_changes if isinstance(raw_changes, list) else []
        return ToolUseEvent(
            **base, tool_name="apply_patch",
            args={"changes": [{"path": _text(c.get("path", "")), "kind": c.get("kind")}
                              for c in changes if isinstance(c, Mapping)]},
            result={"status": item.get("status")}, duration_sec=0.0,
        )
    if item_type in {"agent_message", "reasoning"}:
        text = _text(item.get("text", ""))
        return AgentThoughtEvent(
            **base, content=text, sdk_event_type=f"codex.{item_type}",
            reasoning_content=text if item_type == "reasoning" else None,
        )
    if item_type == "error":
        return AgentThoughtEvent(**base, content=_text(item.get("message", "")), sdk_event_type="codex.error")
    if isinstance(item_type, str):
        details = {key: value for key, value in item.items() if key not in {"id", "type"}}
        return ToolUseEvent(**base, tool_name=item_type, args={"item": _text(details)}, duration_sec=0.0)
    return None


def _same_model(actual: ModelSpec, expected: ModelSpec | None) -> bool:
    limits = {"max_input_tokens", "max_output_tokens"}
    return expected is not None and actual.model_dump(exclude=limits) == expected.model_dump(exclude=limits)


def ledger_calls(rows: list[dict[str, Any]], *, trial: TrialConfig, trial_id: UUID) -> list[LLMCallEvent]:
    """Gateway ledger rows as call events; another model fails the Trial."""
    calls: list[LLMCallEvent] = []
    seen: set[str] = set()
    for row in rows:
        call_id = str(row.get("id") or "")
        if not call_id or call_id in seen or row.get("trial_id") != str(trial_id) or row.get("step_id") != "agent":
            raise ValueError("Gateway ledger has invalid or duplicate call identity")
        seen.add(call_id)
        call = llm_call_row_to_event(row, trial_id=trial_id, seq=0)
        if not _same_model(call.model, trial.agent_model):
            raise ValueError("Gateway ledger has another model identity")
        calls.append(call)
    return calls


def codex_usage(events: list[TrajectoryEvent], trial: TrialConfig) -> dict[str, Any]:
    calls = [event for event in events if isinstance(event, LLMCallEvent)]
    return {
        "schema_version": CODEX_USAGE_SCHEMA,
        "model": trial.agent_model.to_gateway_model_string() if trial.agent_model else None,
        "call_count": len(calls),
        **llm_call_diagnostic_counts(calls),
        "gateway_request_ids": [call.gateway_request_id for call in calls],
        "totals": {
            **{name: sum(getattr(call, name) for call in calls) for name in _COUNTERS},
            "cost_usd": math.fsum(call.cost_usd_snapshot for call in calls),
            "duration_sec": math.fsum(call.duration_sec for call in calls),
        },
    }


def parse_codex_events(
    body: bytes | None, *, trial: TrialConfig, trial_id: UUID | None = None,
) -> list[TrajectoryEvent]:
    """Validate a Codex canonical trace: ordered, one Trial, own model only."""
    events: list[TrajectoryEvent] = []
    call_ids: set[str] = set()
    for line in (body or b"").splitlines():
        event = _EVENT.validate_json(line)
        if not isinstance(event, _CANONICAL_KINDS):
            raise ValueError("Codex trace may only record tool use, thoughts and model calls")
        if event.seq != len(events) or event.step_id != "agent":
            raise ValueError("Codex trace order or step identity is invalid")
        if trial_id is None:
            trial_id = event.trial_id
        if event.trial_id != trial_id:
            raise ValueError("Codex trace has another Trial identity")
        if isinstance(event, LLMCallEvent):
            if not _same_model(event.model, trial.agent_model):
                raise ValueError("Codex trace has another model identity")
            if not event.gateway_request_id or event.gateway_request_id in call_ids:
                raise ValueError("Codex trace has missing or duplicate Gateway calls")
            call_ids.add(event.gateway_request_id)
        events.append(event)
    return events


def codex_environment(model_environment: Mapping[str, str], trial: TrialConfig) -> dict[str, str]:
    from loom.request_params import sanitize_request_extras

    environment = {
        **model_environment,
        "CODEX_HOME": CODEX_HOME,
        # Codex refuses a home under the temp dir; give it its own.
        "TMPDIR": CODEX_TMPDIR,
        "PATH": f"{CODEX_INSTALL_ROOT}/bin:{CODEX_INSTALL_ROOT}/codex-path:/usr/local/bin:/usr/bin:/bin",
    }
    if trial.request_params:
        environment["LOOM_CODEX_SETTINGS_JSON"] = json.dumps(
            sanitize_request_extras(trial.request_params), separators=(",", ":"),
        )
    return environment


async def run_codex(
    *,
    driver: Any,
    workspace: Path,
    task_config: TaskConfig,
    trial_config: TrialConfig,
    trial_id: UUID,
    instruction: str,
    model_environment: Mapping[str, str],
    ledger: Any,
    deadline: AttemptDeadline | None = None,
) -> None:
    """Run Codex in the task sandbox; leave native and canonical files in `workspace`.

    `ledger` returns this Trial's Gateway rows; calls are appended after
    Codex exits, whatever its outcome, so accounting is never lost.
    """
    from loom_launcher.adapter import ModelSpec as LauncherModelSpec
    from loom_launcher.adapters.codex import CodexAdapter

    if trial_config.agent_name != CODEX.name or trial_config.agent_model is None:
        raise AgentError("Codex execution requires the codex harness and an explicit model")
    if not instruction.strip() or len(task_config.steps) != 1:
        raise AgentError("Codex requires one step with a non-empty instruction")
    workspace.mkdir(parents=True, exist_ok=True)
    trace_path, native_path = workspace / "trajectory.jsonl", workspace / CODEX_NATIVE_EVENTS
    native_path.parent.mkdir(parents=True, exist_ok=True)
    with trace_path.open("xb"), native_path.open("xb"):
        pass
    environment = codex_environment(model_environment, trial_config)
    model = trial_config.agent_model
    argv = CodexAdapter().build_invocation(
        instruction=instruction, workdir=task_config.environment.workdir,
        model=LauncherModelSpec(provider=model.provider, name=model.name, tier=model.tier, region=model.region),
        env=environment,
        # Provider-side web search would bypass the task's network policy.
        extra_config=CODEX_EXTRA_CONFIG,
    )
    # The adapter's default home is inside the task workdir, which the
    # verifier grades. Native execution keeps it outside.
    environment["CODEX_HOME"] = CODEX_HOME
    events: list[TrajectoryEvent] = []
    native_bytes = 0
    exit_code: int | None = None

    def record(event: TrajectoryEvent) -> None:
        events.append(event.model_copy(update={"seq": len(events)}))

    try:
        handle = await driver.exec_streaming(
            argv, env_vars=environment, cwd=task_config.environment.workdir,
            timeout_sec=deadline.remaining() if deadline else None,
        )
        try:
            async def stdout_lines() -> None:
                nonlocal native_bytes
                pending = b""
                with native_path.open("ab") as native:
                    async for chunk in handle.stdout:
                        if native_bytes + len(chunk) <= MAX_NATIVE_BYTES:
                            native.write(chunk)
                        native_bytes += len(chunk)
                        pending += chunk
                        *lines, pending = pending.split(b"\n")
                        for line in lines:
                            try:
                                payload = json.loads(line)
                            except ValueError:
                                continue
                            if isinstance(payload, dict) and (event := map_codex_event(
                                payload, trial_id=trial_id, emitted_at=datetime.now(UTC),
                            )) is not None:
                                record(event)

            async def stderr_tail(stream: AsyncIterator[bytes]) -> None:
                async for chunk in stream:
                    sys.stderr.buffer.write(chunk[-4096:])
                    sys.stderr.flush()

            await asyncio.gather(stdout_lines(), stderr_tail(handle.stderr))
            exit_code = await handle.wait()
        except BaseException:
            await handle.kill()
            raise
    finally:
        try:
            for call in ledger_calls(await ledger(trial_id), trial=trial_config, trial_id=trial_id):
                record(call)
        finally:
            trace_path.write_bytes(b"".join(event.model_dump_json().encode() + b"\n" for event in events))
            (workspace / "usage.json").write_text(json.dumps(codex_usage(events, trial_config), sort_keys=True))
    if native_bytes > MAX_NATIVE_BYTES:
        print("codex native event stream exceeded its evidence limit", file=sys.stderr)
    if exit_code != 0:
        raise AgentError(f"codex exited rc={exit_code}")


__all__ = [
    "CODEX_NATIVE_EVENTS",
    "CODEX_USAGE_SCHEMA",
    "codex_environment",
    "codex_usage",
    "ledger_calls",
    "map_codex_event",
    "parse_codex_events",
    "run_codex",
]
