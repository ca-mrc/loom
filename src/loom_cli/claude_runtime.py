"""Pinned local Claude runtime with trial-owned model authority and native evidence."""
from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from ipaddress import IPv4Address
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from anyio import CancelScope, fail_after

from loom.attempt_deadline import AttemptDeadline
from loom.claude_code_atif import claude_atif_steps
from loom.driver.base import Driver, ExecHandle
from loom.errors import AgentError
from loom.hosted_harness import CLAUDE_CODE, CLAUDE_CODE_VERSION
from loom.models.exec import ExecResult
from loom.models.mcp import MCPConnection
from loom.models.networking import NetworkPolicy, Public
from loom.models.task import TaskConfig
from loom.models.trajectory import AgentThoughtEvent, ToolUseEvent, TrajectoryEvent
from loom.models.trial import TrialConfig
from loom.models.types import OS, ModelSpec
from loom.service_execution_claude_code import (
    CLAUDE_NATIVE_EVENTS,
    claude_usage,
    run_claude_code,
)
from loom.trajectory.atif import AtifTrajectory
from loom.trajectory.writer import TrajectoryWriter
from loom_cli.claude_gateway import LocalClaudeRelay


async def local_docker_relay_host() -> str:
    """Return an interface on this CLI host that the local bridge can reach."""
    def inspect() -> str:
        import docker
        client = docker.from_env()
        try:
            if client.api.base_url != "http+docker://localhost":
                raise AgentError("Claude local relay requires a local Linux Docker daemon; remote Docker is unsupported")
            network = client.networks.get("bridge")
            addresses = [entry.get("Gateway") for entry in network.attrs.get("IPAM", {}).get("Config", [])]
            for value in addresses:
                if not value or ":" in value:
                    continue
                address = IPv4Address(value)
                if not (address.is_loopback or address.is_link_local or address.is_unspecified):
                    return str(address)
            raise AgentError("local Docker bridge has no reachable IPv4 gateway for Claude")
        finally:
            client.close()
    return await asyncio.to_thread(inspect)


@asynccontextmanager
async def relay_endpoint_access(driver: Driver, base_url: str, policy: NetworkPolicy) -> AsyncIterator[None]:
    """Permit only the model relay under a restricted tool network policy."""
    if isinstance(policy, Public):
        yield
        return
    endpoint = urlsplit(base_url)
    address = IPv4Address(endpoint.hostname or "")
    if address.is_link_local or address.is_unspecified or not endpoint.port:
        raise AgentError("invalid local Claude relay endpoint")
    rule = f"OUTPUT -d {address} -p tcp --dport {endpoint.port} -j ACCEPT"
    added = await driver.exec("iptables -I " + rule, user="root", timeout_sec=30)
    if added.return_code:
        raise AgentError("could not authorize the local Claude relay in the task network policy")
    try:
        yield
    finally:
        with CancelScope(shield=True), fail_after(5):
            removed = await driver.exec("iptables -D " + rule, user="root", timeout_sec=5)
            if removed.return_code:
                raise AgentError("could not revoke the local Claude relay network exception")


@dataclass
class _Invocation:
    output: Path
    instruction: str
    step_id: str
    event_count: int = 0
    succeeded: bool = False


@dataclass
class _LocalSandbox:
    """Adapt local streaming drivers to the controller's supervised contract.

    Service RPC accepts a process timeout; local Driver streaming does not.
    The controller's enclosing deadline and kill-on-cancellation own that limit.
    """
    driver: Driver

    async def exec(self, command: str, *, timeout_sec: float) -> ExecResult:
        return await self.driver.exec(command, timeout_sec=timeout_sec)

    async def exec_streaming(self, argv: list[str], *, env_vars: dict[str, str],
                             cwd: PurePosixPath, timeout_sec: float | None) -> ExecHandle:
        return await self.driver.exec_streaming(argv, env_vars=env_vars, cwd=cwd)


class LocalClaudeRuntime:
    name = "claude-code"
    version = CLAUDE_CODE_VERSION
    mode: Literal["out-of-box", "in-box"] = "in-box"
    supports_os: frozenset[OS] = frozenset({"linux"})

    def __init__(self, *, trial_id: UUID, task_config: TaskConfig, trial_config: TrialConfig,
                 relay: LocalClaudeRelay, output_dir: Path) -> None:
        self.trial_id, self.task_config, self.trial_config = trial_id, task_config, trial_config
        self.model: ModelSpec | None = trial_config.agent_model
        self.relay, self.output_dir = relay, output_dir
        self._deadline: AttemptDeadline | None = None
        self._invocations: list[_Invocation] = []

    async def setup(self, env: Driver) -> None:
        from loom.service_execution_sandbox_task import _check_install, _install_pinned_archive
        setup = CLAUDE_CODE.setup
        assert setup is not None
        cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "loom/harness-archives"
        async with asyncio.timeout(setup.timeout_seconds):
            await _install_pinned_archive(env, self.output_dir, setup, {}, None,
                                          local_direct=True, archive_cache=cache)
            if not await _check_install(env, setup, 30):
                raise AgentError("installed Claude Code does not match the pinned version")

    def begin_attempt(self, deadline: AttemptDeadline) -> None:
        self._deadline = deadline

    async def aclose_attempt(self) -> None:
        await self.relay.deactivate()

    async def run(self, *, instruction: str, env: Driver, trajectory: TrajectoryWriter,
                  mcp: Sequence[MCPConnection], skills_dir: PurePosixPath | None, step_id: str) -> None:
        if mcp or skills_dir is not None:
            raise AgentError("local Claude does not support external MCP or skills overrides")
        step = next(step for step in self.task_config.steps if step.name == step_id)
        policy = (step.network.agent_phase if step.network is not None else None) or self.task_config.environment.baseline_network_policy
        invocation = _Invocation(self.output_dir / str(len(self._invocations) + 1), instruction, step_id)
        self._invocations.append(invocation)
        prior = {row["id"] for row in await self.relay.get_trial_llm_calls(self.trial_id)}

        async def invocation_ledger(trial_id: UUID) -> list[dict[str, Any]]:
            # Closing streams settles observed usage before controller validation.
            await self.relay.deactivate()
            return [{**row, "step_id": "agent"} for row in await self.relay.get_trial_llm_calls(trial_id)
                    if row["id"] not in prior and row["step_id"] == step_id]

        async def observe(event: TrajectoryEvent) -> None:
            await trajectory.append(event.model_copy(update={"step_id": step_id, "seq": 0}))
            invocation.event_count += 1

        async with relay_endpoint_access(env, self.relay.base_url, policy):
            self.relay.activate(step_id=step_id, deadline=self._deadline)
            try:
                await run_claude_code(driver=_LocalSandbox(env), workspace=invocation.output,
                    task_config=self.task_config.model_copy(update={"steps": [step]}),
                    trial_config=self.trial_config, trial_id=self.trial_id, instruction=instruction,
                    model_environment={"ANTHROPIC_BASE_URL": self.relay.base_url, "ANTHROPIC_AUTH_TOKEN": self.relay.token},
                    ledger=invocation_ledger, deadline=self._deadline, wire_model=self.relay.connection.model,
                    event_sink=observe)
                invocation.succeeded = True
            finally:
                with CancelScope(shield=True), fail_after(10):
                    await self.relay.deactivate()

    def build_atif(self, generic: AtifTrajectory, events: list[TrajectoryEvent]) -> dict[str, Any]:
        document = generic.model_dump(mode="json")
        semantic = [event for event in events if isinstance(event, (AgentThoughtEvent, ToolUseEvent))]
        steps: list[dict[str, Any]] = []
        offset = 0
        for invocation in self._invocations:
            native_path = invocation.output / CLAUDE_NATIVE_EVENTS
            if not native_path.exists():
                if invocation.succeeded:
                    raise AgentError("Claude native evidence missing")
                continue
            selected = semantic[offset:offset + invocation.event_count]
            offset += invocation.event_count
            if any(event.step_id != invocation.step_id for event in selected):
                raise AgentError("Claude native/canonical step identity drift")
            native_steps = claude_atif_steps(native_path.read_bytes(), invocation.instruction,
                trial_id=self.trial_id, events=[event.model_copy(update={"step_id": "agent"}) for event in selected],
                model_name=self.model.name if self.model else None, succeeded=invocation.succeeded)
            for item in native_steps:
                steps.append({**item, "step_id": len(steps) + 1})
        if offset != len(semantic):
            raise AgentError("Claude canonical evidence has unjoined messages")
        document["steps"] = steps
        document["accounting"] = claude_usage(events, self.trial_config)
        return document
