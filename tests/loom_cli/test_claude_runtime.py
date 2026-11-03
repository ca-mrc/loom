from __future__ import annotations

from uuid import uuid4

import pytest

from loom.errors import AgentError
from loom.models.networking import NoNetwork
from loom.models.trajectory import LLMCallEvent, TrialEndEvent, TrialStartEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.atif import project_to_atif
from loom_cli.claude_runtime import LocalClaudeRuntime, relay_endpoint_access
from tests.unit.test_claude_code_capture import _lines
from tests.unit.test_native_claude_code import _ClaudeSandbox
from tests.unit.test_service_execution_terminus_plan import _inputs


class _Relay:
    def __init__(self, trial_id):
        from types import SimpleNamespace
        self.trial_id, self.active, self.closed = trial_id, None, 0
        self.base_url, self.token, self.native = "http://172.17.0.1:45678", "trial-token", False
        self.connection = SimpleNamespace(model=ModelSpec(provider="openai", name="wire-model"))
        self.rows = []

    def activate(self, *, step_id, deadline=None):
        assert self.active is None
        self.active = step_id
        self.rows.append({"id": str(uuid4()), "trial_id": str(self.trial_id), "step_id": step_id,
            "model": "endpoint/wire-model", "dialect": "anthropic_facade", "input_tokens": 20, "output_tokens": 5,
            "cost_usd": 0.01, "rate_card_hash": "test", "provider_extras": {
                "_loom_model_identity": {"provider": "local", "name": "endpoint/wire-model"},
                "_loom_gateway_route": "anthropic_to_chat", "_loom_cost_confidence": "configured"}})

    async def deactivate(self):
        self.active = None
        self.closed += 1

    async def get_trial_llm_calls(self, trial_id):
        assert trial_id == self.trial_id
        return list(self.rows)


class _Writer:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


@pytest.mark.parametrize("terminal_error", [False, True])
async def test_local_controller_preserves_wire_identity_native_trace_and_releases_authority(tmp_path, terminal_error):
    from datetime import UTC, datetime

    trial_id, writer = uuid4(), _Writer()
    relay = _Relay(trial_id)
    task, _, _ = _inputs()
    cfg = TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="local", name="endpoint/wire-model"), claude_code_mode="chat")
    runtime = LocalClaudeRuntime(trial_id=trial_id, task_config=task, trial_config=cfg, relay=relay, output_dir=tmp_path)
    lines = _lines()
    if terminal_error:
        lines[-1].update(is_error=True, subtype="error_during_execution")
    sandbox = _ClaudeSandbox(lines)
    for _ in range(2):
        sandbox = _ClaudeSandbox(lines)
        try:
            await runtime.run(instruction="Fix it.", env=sandbox, trajectory=writer, mcp=[], skills_dir=None, step_id=task.steps[0].name)
        except AgentError:
            assert terminal_error
        else:
            assert not terminal_error
        assert relay.active is None
    assert len(list(tmp_path.glob("*/claude-code/events.jsonl"))) == 2
    assert "--model wire-model" in " ".join(sandbox.argv) and "endpoint/wire-model" not in " ".join(sandbox.argv)
    assert sandbox.environment["ANTHROPIC_AUTH_TOKEN"] == "trial-token"
    assert len(writer.events) == 8 and not any(isinstance(e, LLMCallEvent) for e in writer.events)
    assert all(e.step_id == task.steps[0].name for e in writer.events)
    base = dict(trial_id=trial_id, emitted_at=datetime.now(UTC), step_id="__trial__", seq=0)
    from loom.trajectory.llm_call_events import llm_call_row_to_event
    events = [TrialStartEvent(**base, task_id="t", agent_name="claude-code", agent_mode="in-box"), *writer.events,
        *(llm_call_row_to_event(row, trial_id=trial_id, seq=0) for row in relay.rows),
        TrialEndEvent(**base, final_state="failed" if terminal_error else "succeeded", reward={"pass": 1.0})]
    generic = project_to_atif(events, task_id="t", agent_name=runtime.name, agent_version=runtime.version)
    atif = runtime.build_atif(generic, events)
    assert len(atif["steps"]) == 6 and [s["step_id"] for s in atif["steps"]] == list(range(1, 7))
    assert atif["steps"][1]["observation"]["results"][0]["source_call_id"] == "a"
    assert atif["accounting"]["call_count"] == 2 and atif["accounting"]["totals"]["cost_usd"] == .02
    assert atif["metadata"]["reward"] == {"pass": 1.0}


async def test_restricted_network_exception_is_only_relay_tcp_port_and_removed_on_failure():
    from loom.models.exec import ExecResult
    class Driver:
        def __init__(self):
            self.commands = []
        async def exec(self, command, **kwargs):
            self.commands.append(command)
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)
    driver = Driver()
    with pytest.raises(RuntimeError, match="cancelled"):
        async with relay_endpoint_access(driver, "http://172.17.0.1:45678", NoNetwork()):
            raise RuntimeError("cancelled")
    assert len(driver.commands) == 2
    assert "-I OUTPUT" in driver.commands[0] and "-D OUTPUT" in driver.commands[1]
    assert all("-d 172.17.0.1 -p tcp --dport 45678" in c for c in driver.commands)
    assert not any("-F" in c or "/16" in c for c in driver.commands)


async def test_cancellation_keeps_already_observed_turns_and_kills_process(tmp_path):
    import asyncio
    import json
    from datetime import UTC, datetime

    from loom.trajectory.attempt_guard import AttemptFence, AttemptTrajectoryGuard

    trial_id, writer = uuid4(), _Writer()
    relay = _Relay(trial_id)
    task, _, _ = _inputs()
    cfg = TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="local", name="endpoint/wire-model"), claude_code_mode="chat")
    runtime = LocalClaudeRuntime(trial_id=trial_id, task_config=task, trial_config=cfg, relay=relay, output_dir=tmp_path)
    sandbox = _ClaudeSandbox([])
    waiting = asyncio.Event()
    async def partial():
        yield b"".join(json.dumps(item).encode() + b"\n" for item in _lines()[:3])
        waiting.set()
        await asyncio.Event().wait()
    sandbox.process.stdout = partial()
    fence = AttemptFence()
    guard = AttemptTrajectoryGuard(writer, fence)
    running = asyncio.create_task(runtime.run(instruction="Fix it.", env=sandbox, trajectory=guard,
        mcp=[], skills_dir=None, step_id=task.steps[0].name))
    await asyncio.wait_for(waiting.wait(), 3)
    assert len(writer.events) == 3  # visible before process termination
    fence.latch("agent_timeout")
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert sandbox.process.killed and relay.active is None
    base = dict(trial_id=trial_id, emitted_at=datetime.now(UTC), step_id="__trial__", seq=0)
    events = [TrialStartEvent(**base, task_id="t", agent_name="claude-code", agent_mode="in-box"),
              *writer.events, TrialEndEvent(**base, final_state="cancelled")]
    generic = project_to_atif(events, task_id="t", agent_name=runtime.name, agent_version=runtime.version)
    document = runtime.build_atif(generic, events)
    assert len(document["steps"]) == 2 and document["metadata"]["final_state"] == "cancelled"


async def test_cancel_during_canonical_append_exports_marked_native_partial_trace(tmp_path):
    import asyncio
    from datetime import UTC, datetime

    entered = asyncio.Event()
    class InterruptedWriter(_Writer):
        async def append(self, event):
            entered.set()
            await asyncio.Event().wait()
    trial_id, writer = uuid4(), InterruptedWriter()
    relay = _Relay(trial_id)
    task, _, _ = _inputs()
    cfg = TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="local", name="endpoint/wire-model"), claude_code_mode="chat")
    runtime = LocalClaudeRuntime(trial_id=trial_id, task_config=task, trial_config=cfg, relay=relay, output_dir=tmp_path)
    sandbox = _ClaudeSandbox(_lines()[:3])
    running = asyncio.create_task(runtime.run(instruction="Fix it.", env=sandbox, trajectory=writer,
        mcp=[], skills_dir=None, step_id=task.steps[0].name))
    await asyncio.wait_for(entered.wait(), 3)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    base = dict(trial_id=trial_id, emitted_at=datetime.now(UTC), step_id="__trial__", seq=0)
    events = [TrialStartEvent(**base, task_id="t", agent_name="claude-code", agent_mode="in-box"),
              TrialEndEvent(**base, final_state="cancelled")]
    generic = project_to_atif(events, task_id="t", agent_name=runtime.name, agent_version=runtime.version)
    document = runtime.build_atif(generic, events)
    assert document["steps"][1]["extra"]["canonical_event_join"] == "partial_prefix"
    assert document["metadata"]["final_state"] == "cancelled"
