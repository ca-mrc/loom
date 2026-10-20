"""Commands and shell-exit evidence survive a failed Harbor dispatch."""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.agent.terminus2.dispatch_checkpoint import install_dispatch_checkpoint
from loom.agent.terminus2.gateway_ledger import CheckpointBridgeError
from loom.agent.terminus2.runtime import _install_tmux_session_alive_guard, _tmux_exec
from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


class _CP:
    async def get_trial_llm_calls(self, trial_id):
        return [{"id": "real-call", "step_id": "agent", "input_tokens": 10,
                 "output_tokens": 5, "model": "test", "cost_usd": 0.01,
                 "captured_at": "2026-10-19T00:00:00Z", "rate_card_hash": "x"}]


def _step():
    return {"step_id": 2, "source": "agent", "message": "Analysis: inspect\nPlan: run",
            "metrics": {"prompt_tokens": 10, "completion_tokens": 5},
            "tool_calls": [{"tool_call_id": "call_0_1", "function_name": "bash_command",
                            "arguments": {"keystrokes": "set -e\nfalse\n", "duration": 0.1}}]}


@pytest.mark.asyncio
async def test_dispatch_persisted_before_execution_and_later_checkpoint_deduplicates(tmp_path):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(local_path=local, store=FakeObjectStore(), bucket="t", key="t")
    bridge = HarborCheckpointBridge(trajectory=writer, trial_id=uuid4(), step_id="agent",
                                    model=ModelSpec(provider="openai", name="test"), cp_client=_CP())
    step = _step()
    async with writer:
        await bridge.record_dispatch(step, episode=1)
        # Durable before the caller is allowed to send any keys, even when
        # Harbor never appends this failed turn to its own trajectory.json.
        kinds = [json.loads(line)["kind"] for line in local.read_text().splitlines()]
        assert kinds.count("terminus2_command") == 1
        await bridge.record_session_loss("pane_dead=1\nexit=1\nFileNotFoundError\n" + "x" * 30000)
        step["observation"] = {"results": [{"content": "recreated once"}]}
        checkpoint = tmp_path / "trajectory.json"
        checkpoint.write_text(json.dumps({"steps": [step]}))
        assert await bridge.sync_trajectory_file(checkpoint) == 1
        assert await bridge.sync_trajectory_file(checkpoint) == 0
    rows = [json.loads(line) for line in local.read_text().splitlines()]
    assert len([r for r in rows if r["kind"] == "terminus2_turn"]) == 1
    assert len([r for r in rows if r["kind"] == "terminus2_command"]) == 1
    assert next(r for r in rows if r["kind"] == "terminus2_command")["recorded_before_dispatch"] is True
    observations = [r for r in rows if r["kind"] == "terminus2_terminal_observation"]
    assert [r["capture_source"] for r in observations] == ["error_feedback", "incremental"]
    assert observations[0]["turn_id"] == observations[1]["turn_id"]
    assert observations[0]["completeness"] == "partial"
    assert observations[0]["truncated"]
    assert len(observations[0]["text"].encode()) <= 16384


class _DeadPaneSession:
    _session_name = "terminus-2"
    _user = None
    _previous_buffer = None
    _tmux_start_session = "tmux new-session -d -s terminus-2 'bash --login'"

    def __init__(self):
        self.environment = self
        self.dead = False
        self.operations = []

    async def send_keys(self, keys):
        self.operations.append("keys")
        self.dead = True

    async def is_session_alive(self):
        return True  # remain-on-exit retains a dead pane and the session.

    async def get_incremental_output(self):
        return "new shell"

    async def exec(self, command, **kwargs):
        self.operations.append(command)
        output = ""
        if "pane_dead}" in command:
            output = "1\n" if self.dead else "0\n"
        if "capture-pane" in command:
            output = "FileNotFoundError\nAuthorization: Bearer private-token\n" + "x" * 30000
        if "new-session" in command:
            self.dead = False
        return SimpleNamespace(return_code=0, stdout=output, stderr="")


@pytest.mark.asyncio
async def test_dead_pane_diagnostics_saved_before_recovery_and_terminal_failure():
    session = _DeadPaneSession()
    diagnostics = []

    async def record(text):
        session.operations.append("durable-diagnostic")
        diagnostics.append(text)

    _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
    await session.send_keys("set -e\nfalse\n")
    await session.get_incremental_output()
    with pytest.raises(AgentError, match="lost mid-dispatch"):
        await session.send_keys("exit 3\n")
    assert len(diagnostics) == 2
    assert "FileNotFoundError" in diagnostics[0]
    assert "remaining commands in this batch are suppressed" in diagnostics[0]
    assert "Recovery budget exhausted" in diagnostics[1]
    assert "private-token" not in diagnostics[0]
    assert session.operations.index("durable-diagnostic") < next(
        i for i, op in enumerate(session.operations) if "kill-session" in op)
    assert not any("kill-server" in op for op in session.operations)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OSError("disk full"), CheckpointBridgeError("storage failed")])
async def test_diagnostic_write_failure_does_not_destroy_dead_pane(failure):
    session = _DeadPaneSession()

    async def record(text):
        raise failure

    _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
    with pytest.raises(AgentError, match="tmux session loss detected; diagnostic persistence unavailable") as captured:
        await session.send_keys("exit 1\n")
    assert captured.value.__cause__ is failure
    assert not any("kill-session" in op or "new-session" in op for op in session.operations)


@pytest.mark.asyncio
async def test_diagnostic_timeout_cannot_be_swallowed_as_a_command_timeout():
    session = _DeadPaneSession()

    async def record(text):
        raise TimeoutError("diagnostic storage unavailable")

    _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
    with pytest.raises(AgentError, match="tmux session loss detected; diagnostic persistence unavailable"):
        await session.send_keys("exit 1\n")
    assert not any("kill-session" in op or "new-session" in op for op in session.operations)


@pytest.mark.asyncio
async def test_diagnostic_queries_use_remaining_attempt_budget_and_stop_when_expired():
    time = [0.75]
    deadline = AttemptDeadline(1.0, _clock=lambda: time[0])
    calls = []

    async def exec(command, timeout_sec, **kwargs):
        calls.append(timeout_sec)
        return SimpleNamespace(return_code=0, stdout="0", stderr="")

    session = SimpleNamespace(environment=SimpleNamespace(exec=exec),
        _loom_attempt_deadline=deadline, _loom_require_attempt_active=deadline.require_remaining)
    await _tmux_exec(session, "tmux display-message")
    assert calls == [0.25]
    time[0] = 1.0
    with pytest.raises(AttemptDeadlineExceededError):
        await _tmux_exec(session, "tmux kill-session")
    assert calls == [0.25]


@pytest.mark.asyncio
async def test_delayed_shell_exit_is_diagnosed_at_next_harbor_liveness_check():
    session = _DeadPaneSession()
    diagnostics = []

    async def record(text):
        diagnostics.append(text)

    _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
    session.dead = True
    assert await session.is_session_alive()
    assert len(diagnostics) == 1
    session.dead = True
    with pytest.raises(AgentError, match="lost mid-dispatch"):
        await session.is_session_alive()
    assert len(diagnostics) == 2


@pytest.mark.asyncio
async def test_misleading_send_error_does_not_recover_an_exact_live_pane():
    session = _DeadPaneSession()

    async def send(keys):
        raise RuntimeError("no server running in unrelated command output")

    async def record(text):
        pytest.fail("the exact owned pane is still live")

    session.send_keys = send
    _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
    with pytest.raises(RuntimeError, match="no server running"):
        await session.send_keys("echo test\n")
    assert not any("kill-session" in op or "new-session" in op for op in session.operations)


@pytest.mark.asyncio
async def test_empty_pane_liveness_cannot_authorize_keystrokes():
    session = _DeadPaneSession()
    original = session.exec

    async def exec(command, **kwargs):
        if "display-message" in command:
            return SimpleNamespace(return_code=0, stdout="", stderr="")
        return await original(command, **kwargs)

    session.exec = exec
    _install_tmux_session_alive_guard(SimpleNamespace(_session=session))
    with pytest.raises(AgentError, match="lost mid-dispatch"):
        await session.send_keys("echo must-not-dispatch\n")
    assert "keys" not in session.operations


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disk", "expired", "cancelled"])
async def test_parsed_checkpoint_failure_prevents_command_dispatch(failure):
    import asyncio

    operations = []
    active = True
    chat = SimpleNamespace(total_input_tokens=0, total_output_tokens=0, total_cache_tokens=0)

    class Agent:
        _n_episodes = 1

        def __init__(self):
            self._trajectory_steps = [SimpleNamespace(source="user")]

        async def _handle_llm_interaction(self, chat, *args):
            chat.total_input_tokens = 10
            chat.total_output_tokens = 5
            return ([SimpleNamespace(keystrokes="false\n", duration_sec=0.1)], False, "", "inspect", "run",
                    SimpleNamespace(reasoning_content=""))

        async def _execute_commands(self, *args):
            operations.append("send")

    async def record(step, episode):
        nonlocal active
        operations.append("persist")
        assert step["tool_calls"][0]["arguments"]["keystrokes"] == "false\n"
        if failure == "disk":
            raise OSError("disk full")
        if failure == "cancelled":
            raise asyncio.CancelledError
        active = False

    def require_active():
        if not active:
            raise TimeoutError("expired")

    agent = Agent()
    install_dispatch_checkpoint(agent, record=record, require_active=require_active)
    expected = {"disk": OSError, "expired": TimeoutError, "cancelled": asyncio.CancelledError}[failure]
    with pytest.raises(expected):
        result = await agent._handle_llm_interaction(chat)
        await agent._execute_commands(result[0])
    assert operations == ["persist"]
