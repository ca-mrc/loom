"""Real pinned Harbor parser/loop and real tmux; scripted, network-free model."""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.agent.terminus2.dispatch_checkpoint import install_dispatch_checkpoint
from loom.agent.terminus2.provenance import HARBOR_RUNTIME_VERSION
from loom.agent.terminus2.runtime import (
    _install_tmux_session_alive_guard,
    _reset_harbor_tmux_session,
    _retain_harbor_dead_pane,
)
from loom.attempt_deadline import AttemptDeadline
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.service_execution_terminus2 import _LocalTrajectory


class _Environment:
    session_id = "local-scripted-acceptance"

    def __init__(self, path):
        self.path = path

    async def exec(self, command, timeout_sec=None, **kwargs):
        proc = await asyncio.create_subprocess_shell(
            command, cwd=self.path, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout_sec or 10)
        except BaseException:
            proc.kill()
            await proc.wait()
            raise
        return SimpleNamespace(return_code=proc.returncode, stdout=stdout.decode(), stderr=stderr.decode())


class DispatchDiagnosticsProbe(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def test_failed_current_turn(self):
        await self.fault_case(False)

    async def test_failed_current_turn_after_parse_error(self):
        await self.fault_case(True)

    async def test_missing_owned_session(self):
        await self.isolation_case(False)

    async def test_session_dies_between_check_and_send(self):
        await self.isolation_case(True)

    async def fault_case(self, parse_error_first):
        tmp_path = Path(self.directory.name)
        self.assertIsNotNone(shutil.which("tmux"), "real tmux acceptance requires the terminal fixture")
        assert importlib.metadata.version("harbor") == HARBOR_RUNTIME_VERSION
        harbor = importlib.import_module("harbor.agents.terminus_2.terminus_2")
        from harbor.agents.terminus_2.tmux_session import TmuxSession
        from harbor.models.agent.context import AgentContext
        from harbor.models.trajectories import Step

        env = _Environment(tmp_path)
        session = TmuxSession("terminus-2", env, tmp_path / "terminal.log", None, None)
        rows = []

        class CP:
            async def get_trial_llm_calls(self, trial_id):
                return rows[:]

        class Chat:
            total_input_tokens = 0
            total_output_tokens = 0
            total_cache_tokens = 0
            total_cost = 0.0

            def __init__(self):
                self.messages = []
                self.calls = 0

            async def chat(self, prompt, **kwargs):
                scripted = [
                    [{"keystrokes": f"echo first >> {tmp_path}/first-count\nset -e\npython3 -c 'open(\"/missing-profile-targets.json\")'\n", "duration": 0.3},
                     {"keystrokes": f"echo forbidden > {tmp_path}/must-not-run\n", "duration": 0.1}],
                    [{"keystrokes": f"echo recovered > {tmp_path}/recovered\n", "duration": 0.1}],
                    [{"keystrokes": "set -e\nprintf 'Authorization: Bearer private-acceptance-token\\n' >&2\nexit 7\n", "duration": 0.3}],
                ]
                parse_error = parse_error_first and self.calls == 0
                commands = [] if parse_error else scripted[self.calls - int(parse_error_first)]
                self.calls += 1
                self.total_input_tokens += 10
                self.total_output_tokens += 5
                self.total_cost += 0.01
                rows.append({"id": f"scripted-call-{self.calls}", "step_id": "agent",
                             "input_tokens": 10, "output_tokens": 5, "model": "gpt-4",
                             "cost_usd": 0.01, "rate_card_hash": "local",
                             "captured_at": "2026-10-19T00:00:00Z", "episode": self.calls,
                             "call_ordinal": self.calls, "correlation_status": "correlated"})
                content = "invalid-json" if parse_error else json.dumps({"analysis": "scripted local fault",
                    "plan": "exercise diagnostics", "commands": commands})
                return harbor.LLMResponse(content=content, model_name="gpt-4")

        # Keep the real parser and loop. Replace only model construction/network I/O.
        patched = patch.object(harbor.Terminus2, "_init_llm", lambda *args, **kwargs: object())
        patched.start()
        self.addCleanup(patched.stop)
        agent = harbor.Terminus2(logs_dir=tmp_path, model_name="gpt-4", max_turns=5,
                                 enable_summarize=False, record_terminal_session=False)
        agent._context = AgentContext()
        agent._session = session
        agent._trajectory_steps = [Step(step_id=1, source="user", message="scripted local fault",
                                       timestamp="2026-10-19T00:00:00Z")]
        local = tmp_path / "events.jsonl"
        local.touch(mode=0o600)
        writer = _LocalTrajectory(local, AttemptDeadline.after(30), 64 * 1024 * 1024)
        bridge = HarborCheckpointBridge(trajectory=writer, trial_id=uuid4(), step_id="agent",
            model=ModelSpec(provider="openai", name="gpt-4"), cp_client=CP())

        async def record(step, episode):
            await bridge.record_dispatch(step, episode=episode,
                preceding_steps=[entry.model_dump(mode="json") for entry in agent._trajectory_steps])

        await session.start()
        try:
            await _retain_harbor_dead_pane(session)
            install_dispatch_checkpoint(agent, record=record, require_active=lambda: None)
            _install_tmux_session_alive_guard(agent, record_loss=bridge.record_session_loss)
            with self.assertRaisesRegex(AgentError, "lost mid-dispatch"):
                await agent._run_agent_loop("scripted local fault", Chat())
            await bridge.sync_trajectory_file(tmp_path / "trajectory.json")
        finally:
            await env.exec("tmux kill-session -t =terminus-2 2>/dev/null || true")

        events = [json.loads(line) for line in local.read_text().splitlines()]
        turns = [e for e in events if e["kind"] == "terminus2_turn"]
        commands = [e for e in events if e["kind"] == "terminus2_command"]
        losses = [e for e in events if e.get("capture_source") == "error_feedback"]
        assert len(turns) == 3 + int(parse_error_first) and len(commands) == 4
        assert all(e["recorded_before_dispatch"] is True for e in commands)
        assert len({e["turn_id"] for e in turns}) == len(turns)
        assert len(losses) == 2
        assert "FileNotFoundError" in losses[0]["text"] and "exit=1" in losses[0]["text"]
        assert "exit=7" in losses[1]["text"]
        assert "private-acceptance-token" not in losses[1]["text"]
        assert "private-acceptance-token" not in local.read_text()
        assert losses[1]["turn_id"] == turns[-1]["turn_id"]
        assert (tmp_path / "first-count").read_text().splitlines() == ["first"]
        assert (tmp_path / "recovered").exists()
        assert not (tmp_path / "must-not-run").exists()
        harbor_steps = json.loads((tmp_path / "trajectory.json").read_text())["steps"]
        assert len([s for s in harbor_steps if s["source"] == "agent"]) == 2 + int(parse_error_first)

    async def isolation_case(self, die_after_check):
        tmp_path = Path(self.directory.name)
        self.assertIsNotNone(shutil.which("tmux"), "real tmux acceptance requires the terminal fixture")
        assert importlib.metadata.version("harbor") == HARBOR_RUNTIME_VERSION
        from harbor.agents.terminus_2.tmux_session import TmuxSession

        env = _Environment(tmp_path)
        session = TmuxSession("terminus-2", env, tmp_path / "terminal.log", None, None)
        await session.start()
        await env.exec("tmux new-session -d -s terminus-2-other 'bash --login'")
        await env.exec("tmux send-keys -t =terminus-2-other: 'echo FOREIGN_PRIVATE_PANE' Enter")
        before = await env.exec("tmux show-options -wv -t =terminus-2-other: remain-on-exit")
        losses = []

        async def record(text):
            losses.append(text)

        original_send = session.send_keys

        async def lose_own_session_then_send(*args, **kwargs):
            # Force the check/send race inside Harbor's send boundary. Its actual
            # send/paste commands must use '=name', independent of the precheck.
            await env.exec("tmux kill-session -t =terminus-2")
            return await original_send(*args, **kwargs)

        if die_after_check:
            session.send_keys = lose_own_session_then_send
        else:
            await env.exec("tmux kill-session -t =terminus-2")
            await _reset_harbor_tmux_session(env)
            with self.assertRaisesRegex(AgentError, "retain"):
                await _retain_harbor_dead_pane(session)
        try:
            _install_tmux_session_alive_guard(SimpleNamespace(_session=session), record_loss=record)
            await session.send_keys(f"echo INJECTED > {tmp_path}/injected\n", min_timeout_sec=0.1)
            assert losses and "FOREIGN_PRIVATE_PANE" not in "\n".join(losses)
            assert not (tmp_path / "injected").exists()
            foreign = await env.exec("tmux capture-pane -p -t =terminus-2-other:")
            assert foreign.return_code == 0 and "INJECTED" not in foreign.stdout
            after = await env.exec("tmux show-options -wv -t =terminus-2-other: remain-on-exit")
            assert after.stdout == before.stdout
            sessions = await env.exec("tmux list-sessions -F '#{session_name}'")
            assert set(sessions.stdout.splitlines()) == {"terminus-2", "terminus-2-other"}
            assert session._session_name == "=terminus-2:"
            assert "pipe-pane -t =terminus-2: " in session._loom_tmux_start_command
        finally:
            await env.exec("tmux kill-session -t =terminus-2 2>/dev/null || true")
            await env.exec("tmux kill-session -t =terminus-2-other 2>/dev/null || true")

if __name__ == "__main__":
    unittest.main(verbosity=2)
