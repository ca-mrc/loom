"""Exercise pinned Harbor's real summarization loop with offline transports.

Only model responses and terminal I/O are scripted. Harbor owns the episode
loop, all three summarization subagents, compaction and handoff; Loom uses its
real dispatch checkpoint, Gateway ledger join and trajectory bridge.
"""

from __future__ import annotations

import copy
import importlib.metadata
import json
import unittest
from pathlib import PurePosixPath
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from harbor.agents.terminus_2.terminus_2 import Terminus2
from harbor.llms.base import LLMResponse
from harbor.models.agent.context import AgentContext
from harbor.models.metric.usage_info import UsageInfo

from loom.agent.terminus2 import runtime as bridge
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.driver.fake import FakeDriver
from loom.models.trajectory import EventKind
from loom.models.types import ModelSpec

CONTEXT_LIMIT = 131072
INSTRUCTION = "Inspect the fixture, then display the generated chess board."


class ScriptedLLM:
    def __init__(self, *, context_limit=CONTEXT_LIMIT, explicit_correlation=False, duplicate_metrics=False):
        self.context_limit = context_limit
        self.requests = []
        self.main_calls = 0
        self.agent = None
        self.explicit_correlation = explicit_correlation
        self.duplicate_metrics = duplicate_metrics

    def get_model_context_limit(self):
        return self.context_limit

    def get_model_output_limit(self):
        return 16384

    def get_model_max_output_tokens(self):
        return self.get_model_output_limit()

    async def call(self, *, prompt, message_history=(), **kwargs):
        if prompt.startswith("You are about to hand off your work"):
            stage, content = "summary", "The fixture was inspected; display chess_board.png next."
        elif prompt.startswith("You are picking up work from a previous AI agent"):
            stage, content = "questions", "Where is the board? What remains? Which file? Which command? Any errors?"
        elif prompt.startswith("The next agent has a few questions for you"):
            stage, content = "answers", "Use display chess_board.png in the current directory; no errors."
        else:
            stage = "main"
            self.main_calls += 1
            command = "ls\n" if self.main_calls == 1 else "display chess_board.png\n"
            content = json.dumps({
                "analysis": "Inspect the fixture.",
                "plan": "Display the board after inspection.",
                "commands": [{"keystrokes": command, "duration": 0.01}],
                "task_complete": False,
            })
        self.requests.append(copy.deepcopy({
            "stage": stage, "prompt": prompt, "message_history": message_history,
            "episode": self.agent._n_episodes,
        }))
        ordinal = len(self.requests)
        usage_ordinal = 1 if self.duplicate_metrics else ordinal
        return LLMResponse(
            content=content, reasoning_content="fixture reasoning", model_name="gpt-4o",
            usage=UsageInfo(prompt_tokens=100 + usage_ordinal, completion_tokens=10 + usage_ordinal,
                            cache_tokens=0, cost_usd=0),
            extra={"loom_client_call_id": f"fixture-request-{ordinal}"} if self.explicit_correlation else None,
        )


class Session:
    def __init__(self):
        self.commands = []
        self.environment = self

    async def exec(self, command, **kwargs):
        return SimpleNamespace(return_code=0, stdout="0\n", stderr="")

    async def is_session_alive(self):
        return True

    async def send_keys(self, keys, **kwargs):
        self.commands.append(keys)

    async def get_incremental_output(self):
        return "chess_board.png\n$ "

    async def capture_pane(self, **kwargs):
        return "chess_board.png\n$ "


class Events:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class CP:
    def __init__(self, llm, *, correlated=False):
        self.llm = llm
        self.correlated = correlated

    async def mint_step_token(self, **kwargs):
        return "loom_step_fixture_credential"

    async def get_trial_llm_calls(self, trial_id):
        return [{
            "id": f"fixture-request-{ordinal}", "trial_id": str(trial_id),
            "step_id": "agent", "input_tokens": 100 + (1 if self.llm.duplicate_metrics else ordinal),
            "output_tokens": 10 + (1 if self.llm.duplicate_metrics else ordinal),
            "dialect": "openai_chat", "model": "gpt-4o",
            "cost_usd": 0, "rate_card_hash": "fixture", "captured_at": "2026-10-28T00:00:00Z",
            **({"correlation_status": "correlated", "episode": request["episode"],
                "client_call_id": f"fixture-request-{ordinal}",
                "call_ordinal": ordinal} if self.correlated else {}),
        } for ordinal, request in enumerate(self.llm.requests, start=1)]


def agent_class(llm, session):
    class Agent(Terminus2):
        @staticmethod
        def _init_llm(**kwargs):
            return llm

        async def setup(self, environment):
            self._session = session
            llm.agent = self

    return Agent


class HarborSummarizationTests(unittest.IsolatedAsyncioTestCase):
    def test_real_harbor_install_matches_pinned_revision(self):
        direct_url = json.loads(importlib.metadata.distribution("harbor").read_text("direct_url.json"))
        self.assertEqual(direct_url["vcs_info"]["commit_id"], HARBOR_COMPAT_SHA)

    async def run_fixture(self, *, threshold, context_limit=CONTEXT_LIMIT,
                          enable_summarize=True, max_turns=2, correlated=False,
                          explicit_correlation=False, duplicate_metrics=False):
        llm = ScriptedLLM(context_limit=context_limit, explicit_correlation=explicit_correlation,
                          duplicate_metrics=duplicate_metrics)
        session, events = Session(), Events()
        driver = FakeDriver()
        await driver.start()
        self.addAsyncCleanup(driver.stop)
        runtime = bridge.LoomTerminus2Runtime(
            model=ModelSpec(provider="openai", name="gpt-4o"),
            team_id=str(uuid4()), trial_id=uuid4(), cp_client=CP(llm, correlated=correlated),
            gateway_url="http://gateway", harbor_config={
                "enable_summarize": enable_summarize,
                "max_input_tokens": context_limit,
                "proactive_summarization_threshold": threshold,
                "max_turns": max_turns,
            },
        )
        failure = None
        with patch.object(bridge, "_import_terminus2",
                          return_value=(agent_class(llm, session), AgentContext)):
            try:
                await runtime.run(instruction=INSTRUCTION, env=driver, trajectory=events,
                                  mcp=[], skills_dir=None, step_id="agent")
            except Exception as exc:
                failure = exc
        raw = driver.filesystem.get(PurePosixPath("/workspace/.loom/agent/trajectory.json"))
        native = json.loads(raw) if raw else None
        return llm, session, events, native, failure

    async def test_native_summary_handoff_keeps_commands_and_gateway_joins(self):
        llm, session, events, native, failure = await self.run_fixture(threshold=CONTEXT_LIMIT - 10)
        self.assertEqual([r["stage"] for r in llm.requests],
                         ["main", "summary", "questions", "answers", "main"])
        self.assertEqual(session.commands, ["ls\n", "display chess_board.png\n"])
        self.assertIsNone(failure, str(failure))
        self.assertIsNotNone(native)
        compactions = [step for step in native["steps"]
                       if step.get("extra", {}).get("context_management", {}).get("type") == "compaction"]
        self.assertEqual(len(compactions), 1)
        refs = compactions[0]["observation"]["results"][0]["subagent_trajectory_ref"]
        self.assertEqual(len(refs), 3)
        native_agents = [step for step in native["steps"] if step["source"] == "agent"]
        self.assertEqual(len(native_agents), 2)
        turns = [event for event in events.events if event.kind == EventKind.TERMINUS2_TURN]
        self.assertEqual([event.gateway_request_id for event in turns],
                         ["fixture-request-1", "fixture-request-5"])
        self.assertEqual([event.harbor_step_id for event in turns],
                         [step["step_id"] for step in native_agents])
        observations = [event for event in events.events
                        if event.kind == EventKind.TERMINUS2_TERMINAL_OBSERVATION]
        self.assertEqual(len(observations), 2)
        self.assertEqual([event.turn_id for event in observations], [event.turn_id for event in turns])
        self.assertEqual([event.text for event in observations],
                         [step["observation"]["results"][0]["content"] for step in native_agents])
        # Previous-turn checkpoints may be labelled partial while Harbor is
        # still running; the final checkpoint must retain its full observation.
        self.assertEqual(observations[-1].completeness, "full")
        self.assertIn("Here are the answers the other agent provided.", llm.requests[-1]["prompt"])
        self.assertTrue(llm.requests[-1]["message_history"])

    async def test_high_threshold_keeps_empty_history_initial_protocol_prompt(self):
        baseline = await self.run_fixture(threshold=CONTEXT_LIMIT + 1000,
                                          enable_summarize=False, max_turns=1)
        enabled = await self.run_fixture(threshold=CONTEXT_LIMIT + 1000, max_turns=1)
        self.assertEqual(enabled[0].requests[0]["prompt"], baseline[0].requests[0]["prompt"])
        self.assertIsNone(enabled[-1], str(enabled[-1]))
        self.assertEqual([r["stage"] for r in enabled[0].requests], ["main"])

    async def test_summary_subcalls_do_not_take_the_correlated_agent_join(self):
        llm, session, events, native, failure = await self.run_fixture(
            threshold=CONTEXT_LIMIT - 10, correlated=True,
        )
        self.assertIsNone(failure, str(failure))
        self.assertIsNotNone(native)
        self.assertEqual([r["stage"] for r in llm.requests],
                         ["main", "summary", "questions", "answers", "main"])
        self.assertEqual(session.commands, ["ls\n", "display chess_board.png\n"])
        turns = [event for event in events.events if event.kind == EventKind.TERMINUS2_TURN]
        self.assertEqual([event.gateway_request_id for event in turns],
                         ["fixture-request-1", "fixture-request-5"])

    async def test_explicit_call_identity_handles_identical_summary_usage(self):
        llm, session, events, native, failure = await self.run_fixture(
            threshold=CONTEXT_LIMIT - 10, correlated=True,
            explicit_correlation=True, duplicate_metrics=True,
        )
        self.assertIsNone(failure, str(failure))
        self.assertIsNotNone(native)
        self.assertEqual(len(llm.requests), 5)
        self.assertEqual(session.commands, ["ls\n", "display chess_board.png\n"])
        turns = [event for event in events.events if event.kind == EventKind.TERMINUS2_TURN]
        self.assertEqual([event.gateway_request_id for event in turns],
                         ["fixture-request-1", "fixture-request-5"])

    async def test_zero_threshold_disables_proactive_summary_even_over_context(self):
        llm, session, _events, native, failure = await self.run_fixture(threshold=0, context_limit=1)
        self.assertEqual([r["stage"] for r in llm.requests], ["main", "main"])
        self.assertIsNone(failure, str(failure))
        self.assertEqual(session.commands, ["ls\n", "display chess_board.png\n"])
        self.assertIsNotNone(native)
        self.assertFalse(any(step.get("extra", {}).get("context_management") for step in native["steps"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
