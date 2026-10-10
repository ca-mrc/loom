"""Exercise the pinned Harbor loop in its image, with no network or model service."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid4, uuid5

from harbor.agents.terminus_2.terminus_2 import Terminus2
from harbor.llms.base import LLMResponse
from harbor.models.agent.context import AgentContext

from loom.agent.terminus2 import runtime as bridge
from loom.attempt_deadline import AttemptDeadline
from loom.driver.fake import FakeDriver
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_terminus2 import LeaseGatewayClient, run_terminus2


class ScriptedLLM:
    def __init__(self):
        self.prompts = []
        self.blocked = asyncio.Event()
        self.cancelled = False

    async def call(self, *, prompt, **kwargs):
        self.prompts.append(prompt)
        if len(self.prompts) > 3:
            self.blocked.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        return LLMResponse(content=json.dumps({
            "analysis": "Inspect the completed work.", "plan": "Review again if time remains.",
            "commands": [], "task_complete": True,
        }))


class Session:
    async def is_session_alive(self):
        return True

    async def send_keys(self, *args, **kwargs):
        raise AssertionError("the scripted model issues no shell commands")

    async def get_incremental_output(self):
        return "fixture terminal output"


class RealHarborCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.instances = []
        instances = self.instances

        class Agent(Terminus2):
            @staticmethod
            def _init_llm(**kwargs):
                return ScriptedLLM()

            async def setup(self, environment):
                # Only the terminal transport and LLM are fixtures. Keep the
                # pinned parser, Chat, completion loop and trajectory writer.
                self._session = Session()
                instances.append(self)

        async def ledger(self, trial_id):
            agents = [agent for agent in instances if agent.options.session_id == str(trial_id)]
            if not agents:
                return []
            agent, = agents
            return [{"id": str(uuid5(NAMESPACE_URL, f"{trial_id}/{episode}")),
                     "trial_id": str(trial_id), "step_id": "agent",
                     "episode": episode, "call_ordinal": episode, "correlation_status": "correlated",
                     "input_tokens": 0, "output_tokens": 0, "model": "gpt-4o",
                     "dialect": "openai_chat", "cost_usd": 0, "rate_card_hash": "fixture"}
                    for episode in range(1, min(3, len(agent._llm.prompts)) + 1)]

        patches = [
            patch.object(bridge, "_import_terminus2", return_value=(Agent, AgentContext)),
            patch.object(LeaseGatewayClient, "get_trial_llm_calls", ledger),
        ]
        for patched in patches:
            patched.start()
            self.addCleanup(patched.stop)

    async def execute(self, enabled, *, deadline=None, max_turns=50, directory="trial",
                      max_trajectory_bytes=16 * 1024 * 1024):
        task = TaskConfig.model_validate({
            "schema_version": "1", "task": {"id": "fixture", "name": "fixture"},
            "environment": {"os": "linux", "workdir": "/app"},
            "agent": {"name": "oracle", "continue_until_timeout": enabled},
            "verifier": {"name": "script"}, "steps": [{"name": "main"}],
        })
        driver = FakeDriver()
        await driver.start()
        await run_terminus2(
            driver=driver, workspace=Path(self.directory.name) / directory,
            task_config=task,
            trial_config=TrialConfig(agent_name="terminus-2", agent_model={
                "provider": "openai", "name": "gpt-4o",
            }),
            trial_id=uuid4(), team_id=uuid4(), gateway_url="http://127.0.0.1:9000",
            instruction="Inspect the fixture.", deadline=deadline or AttemptDeadline.after(5),
            max_turns=max_turns, max_trajectory_bytes=max_trajectory_bytes,
        )

    async def test_default_still_completes_after_confirmation(self):
        await self.execute(False)
        agent, = self.instances
        self.assertEqual(len(agent._llm.prompts), 2)
        self.assertEqual(agent._n_episodes, 2)

    async def test_continuation_keeps_one_session_and_real_completion_actions_until_deadline(self):
        deadline = AttemptDeadline.after(1)
        with self.assertRaises(TimeoutError):
            await self.execute(True, deadline=deadline, max_turns=2)
        agent, = self.instances
        self.assertTrue(deadline.reached)
        self.assertEqual(len(agent._llm.prompts), 4)
        self.assertTrue(agent._llm.cancelled)
        native = Path(self.directory.name) / "trial/harbor/trajectory.json"
        payload = json.loads(native.read_text())
        steps = [step for step in payload["steps"] if step["source"] == "agent"]
        self.assertEqual(len(steps), 3)
        self.assertTrue(all(step["tool_calls"][0]["function_name"] == "mark_task_complete" for step in steps))
        self.assertTrue(all("continue_until_timeout" in prompt for prompt in agent._llm.prompts[1:]))
        self.assertNotIn("api_key", native.read_text())

    async def test_cancellation_does_not_restart_or_swallow_stop(self):
        running = asyncio.create_task(self.execute(True))
        try:
            async with asyncio.timeout(2):
                while not self.instances:
                    await asyncio.sleep(0)
                agent, = self.instances
                await agent._llm.blocked.wait()
            running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await running
            self.assertTrue(agent._llm.cancelled)
            self.assertEqual(len(self.instances), 1)
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)

    async def test_policy_is_local_to_each_concurrent_instance(self):
        continuing = asyncio.create_task(self.execute(True, directory="continuing"))
        try:
            async with asyncio.timeout(2):
                while not self.instances:
                    await asyncio.sleep(0)
                agent, = self.instances
                await agent._llm.blocked.wait()
            await self.execute(False, directory="ordinary")
            self.assertEqual(len(self.instances[1]._llm.prompts), 2)
            self.assertFalse(continuing.done())
        finally:
            continuing.cancel()
            await asyncio.gather(continuing, return_exceptions=True)

    async def test_runtime_error_remains_terminal_without_a_new_session(self):
        from loom.errors import AgentError

        original = ScriptedLLM.call

        async def fail_after_completion(llm, **kwargs):
            if len(llm.prompts) == 3:
                raise RuntimeError("fixture terminal model error")
            return await original(llm, **kwargs)

        with patch.object(ScriptedLLM, "call", fail_after_completion):
            with self.assertRaisesRegex(AgentError, "fixture terminal model error"):
                await self.execute(True)
        agent, = self.instances
        self.assertEqual(len(agent._llm.prompts), 3)

    async def test_trajectory_cap_cancels_active_model_before_deadline(self):
        from loom.errors import AgentError

        deadline = AttemptDeadline.after(4)
        with self.assertRaisesRegex(AgentError, "trajectory exceeds output limit"):
            await self.execute(True, deadline=deadline, max_trajectory_bytes=2500)
        agent, = self.instances
        self.assertGreater(deadline.remaining(), 2, "resource cap was deferred until timeout")
        self.assertTrue(agent._llm.cancelled)
        self.assertEqual(len(agent._llm.prompts), 4)
        self.assertLessEqual((Path(self.directory.name) / "trial/trajectory.jsonl").stat().st_size, 2500)


if __name__ == "__main__":
    unittest.main(verbosity=2)
