"""Supervised Claude Code execution with native evidence and external accounting."""

from __future__ import annotations

import asyncio
import json
import shlex
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from anyio import CancelScope, fail_after

from loom.attempt_deadline import AttemptDeadline
from loom.claude_code_capture import MAX_CLAUDE_NATIVE_BYTES, ClaudeCapture
from loom.errors import AgentError
from loom.hosted_harness import CLAUDE_CODE_HOME, CLAUDE_CODE_INSTALL_ROOT, CLAUDE_CODE_TMPDIR
from loom.installed_agent_capture import (
    installed_agent_usage,
    ledger_calls,
    parse_installed_agent_events,
)
from loom.models.task import TaskConfig
from loom.models.trajectory import TrajectoryEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec

CLAUDE_USAGE_SCHEMA = "loom.service-execution-claude-code-usage.v1"
CLAUDE_NATIVE_EVENTS = Path("claude-code/events.jsonl")
CLAUDE_INSTRUCTION = Path("claude-code/instruction.txt")


def claude_usage(events: list[TrajectoryEvent], trial: TrialConfig) -> dict[str, Any]:
    usage = installed_agent_usage(events, trial, schema=CLAUDE_USAGE_SCHEMA)
    from loom.models.trajectory import LLMCallEvent
    calls = [event for event in events if isinstance(event, LLMCallEvent)]
    usage["price_unknown_call_count"] = sum(call.cost_status != "priced" for call in calls)
    return usage


def parse_claude_events(
    body: bytes | None, *, trial: TrialConfig, trial_id: UUID | None = None,
) -> list[TrajectoryEvent]:
    return parse_installed_agent_events(body, trial=trial, trial_id=trial_id, label="Claude")


async def run_claude_code(
    *, driver: Any, workspace: Path, task_config: TaskConfig, trial_config: TrialConfig,
    trial_id: UUID, instruction: str, model_environment: Mapping[str, str], ledger: Any,
    deadline: AttemptDeadline | None = None,
    wire_model: ModelSpec | None = None,
) -> None:
    from loom_launcher.adapter import ModelSpec as LauncherModelSpec
    from loom_launcher.adapters.claude_code import ClaudeCodeAdapter

    model = trial_config.agent_model
    if trial_config.agent_name != "claude-code" or model is None or trial_config.claude_code_mode is None:
        raise AgentError("Claude requires an explicit model and resolved connection mode")
    if trial_config.request_params:
        raise AgentError("Claude Code does not support Loom generation overrides")
    if len(task_config.steps) != 1:
        raise AgentError("Claude requires one task step")
    capture = ClaudeCapture(trial_id=trial_id, instruction=instruction)
    workspace.mkdir(parents=True, exist_ok=True)
    native_path, trace_path = workspace / CLAUDE_NATIVE_EVENTS, workspace / "trajectory.jsonl"
    native_path.parent.mkdir(parents=True, exist_ok=True)
    with native_path.open("xb"), trace_path.open("xb"):
        pass
    (workspace / CLAUDE_INSTRUCTION).write_text(instruction)
    environment = {
        **model_environment, "CLAUDE_CONFIG_DIR": CLAUDE_CODE_HOME, "HOME": CLAUDE_CODE_HOME,
        "TMPDIR": CLAUDE_CODE_TMPDIR, "PATH": f"{CLAUDE_CODE_INSTALL_ROOT}:/usr/local/bin:/usr/bin:/bin",
    }
    invocation_model = wire_model or model
    argv = ClaudeCodeAdapter().build_invocation(
        instruction=instruction, workdir=task_config.environment.workdir,
        model=LauncherModelSpec(provider=invocation_model.provider, name=invocation_model.name,
                               tier=invocation_model.tier, region=invocation_model.region),
        env=environment, runtime_mode=trial_config.claude_code_mode,
    )
    calls: list[Any] = []
    exit_code: int | None = None
    try:
        async with asyncio.timeout(deadline.remaining() if deadline else None):
            prepared = await driver.exec(
                f"mkdir -p -- {shlex.quote(CLAUDE_CODE_HOME)} {shlex.quote(CLAUDE_CODE_TMPDIR)}",
                timeout_sec=deadline.remaining() if deadline else 30,
            )
            if prepared.return_code:
                raise AgentError("Claude runtime directories could not be prepared")
            handle = await driver.exec_streaming(argv, env_vars=environment, cwd=task_config.environment.workdir,
                                                 timeout_sec=deadline.remaining() if deadline else None)
            async def stdout() -> None:
                size = 0
                with native_path.open("ab") as native:
                    async for chunk in handle.stdout:
                        native.write(chunk[:max(0, MAX_CLAUDE_NATIVE_BYTES - size)])
                        size += len(chunk)
                        capture.feed(chunk)

            async def drain(stream: AsyncIterator[bytes]) -> None:
                # CLI stderr can contain request diagnostics. It is drained, not
                # copied into product logs or artifacts with credential values.
                async for _ in stream:
                    pass

            readers = [asyncio.create_task(stdout()), asyncio.create_task(drain(handle.stderr))]
            try:
                await asyncio.gather(*readers)
                exit_code = await handle.wait()
                capture.finish(exit_code=exit_code)
            except BaseException:
                with CancelScope(shield=True), fail_after(5):
                    await handle.kill()
                raise
            finally:
                for reader in readers:
                    reader.cancel()
                await asyncio.gather(*readers, return_exceptions=True)
    finally:
        # Accounting and evidence have a cleanup budget even after cancellation.
        try:
            with CancelScope(shield=True), fail_after(10):
                calls = ledger_calls(await ledger(trial_id), trial=trial_config, trial_id=trial_id)
                start_seq = len(capture.events)
                capture.events.extend(call.model_copy(update={"seq": start_seq + index})
                                      for index, call in enumerate(calls))
        finally:
            trace_path.write_bytes(b"".join(event.model_dump_json().encode() + b"\n" for event in capture.events))
            (workspace / "usage.json").write_text(json.dumps(claude_usage(capture.events, trial_config), sort_keys=True))
    if not any(call.call_status == "completed" and call.usage_status is None for call in calls):
        raise AgentError("Claude success requires an observed model call with reported usage")
