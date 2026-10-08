"""Differential real-Harbor probe; only model and terminal transports are scripted.

Run in the production Harbor image, with network disabled. This checks the
documented Loom compatibility policy (50 turns, no summarization), not parity
with every upstream default or with Loom's optional multi-model extensions.
"""

from __future__ import annotations

import copy
import importlib.metadata
import json
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch
from uuid import uuid4

from harbor.agents.terminus_2.terminus_2 import Terminus2
from harbor.llms.base import LLMResponse
from harbor.models.agent.context import AgentContext
from harbor.models.metric.usage_info import UsageInfo

from loom.agent.terminus2 import runtime as bridge
from loom.agent.terminus2.mapper import Terminus2TrajectoryMapper
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.models.trajectory import EventKind
from loom.models.types import ModelSpec


def response(*, complete=False, commands=()):
    return json.dumps(
        {
            "analysis": "Inspect the fixture.",
            "plan": "Execute the declared commands.",
            "commands": [{"keystrokes": command, "duration": 0.01} for command in commands],
            "task_complete": complete,
        }
    )


class ScriptedLLM:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []
        self.init_options = None
        self.effective_options = None

    async def call(self, *, prompt, message_history, **kwargs):
        # Copy at call time: Harbor mutates Chat history after the response.
        self.requests.append(
            copy.deepcopy(
                {
                    "prompt": prompt,
                    "message_history": message_history,
                    **{key: value for key, value in kwargs.items() if key != "logging_path"},
                }
            )
        )
        item = next(self.responses)
        if isinstance(item, BaseException):
            raise item
        ordinal = len(self.requests)
        return LLMResponse(
            content=item,
            reasoning_content="fixture reasoning",
            model_name="gpt-4o",
            usage=UsageInfo(
                prompt_tokens=100 + ordinal,
                completion_tokens=10 + ordinal,
                cache_tokens=0,
                cost_usd=0,
            ),
        )


class Session:
    def __init__(self):
        self.commands = []
        self.fail_next = False

    async def is_session_alive(self):
        return True

    async def send_keys(self, keys, **kwargs):
        self.commands.append((keys, kwargs))
        if self.fail_next:
            self.fail_next = False
            raise TimeoutError("fixture command timed out")

    async def get_incremental_output(self):
        return f"fixture terminal output: {len(self.commands)} commands\n$ "


class Events:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class CP:
    def __init__(self, llm):
        self.llm = llm

    async def mint_step_token(self, **kwargs):
        return "loom_step_fixture_credential"

    async def get_trial_llm_calls(self, trial_id):
        return [
            {
                "id": f"fixture-request-{index}",
                "trial_id": str(trial_id),
                "step_id": "agent",
                "input_tokens": 100 + index,
                "output_tokens": 10 + index,
                "dialect": "openai_chat",
                "model": "gpt-4o",
                "cost_usd": 0,
                "rate_card_hash": "fixture",
                "captured_at": "2026-10-08T00:00:00Z",
            }
            for index in range(1, len(self.llm.requests) + 1)
        ]


def agent_class(llm, session):
    class Agent(Terminus2):
        @staticmethod
        def _init_llm(**kwargs):
            # Retain constructor wiring too: a fake transport must not hide
            # model, endpoint, sampling or backend parameter drift.
            llm.init_options = copy.deepcopy(
                {key: value for key, value in kwargs.items() if key != "session_id"}
            )
            return llm

        async def setup(self, environment):
            self._session = session
            llm.effective_options = {
                "max_turns": self._max_episodes,
                "enable_summarize": self._enable_summarize,
                "record_terminal_session": self._record_terminal_session,
            }

    return Agent


def native_steps(document):
    # Only timestamps vary. Keep prompts, messages, tool arguments, observations,
    # usage and reasoning intact; deleting these could hide a compatibility bug.
    return [
        {key: value for key, value in step.items() if key != "timestamp"}
        for step in document["steps"]
    ]


class HarborConformanceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_real_harbor_install_matches_pinned_revision(self):
        direct_url = json.loads(
            importlib.metadata.distribution("harbor").read_text("direct_url.json")
        )
        self.assertEqual(direct_url["vcs_info"]["commit_id"], HARBOR_COMPAT_SHA)

    async def pair(
        self, responses, *, max_turns=50, timeout=False, instruction_suffix="", request_params=None
    ):
        instruction = "Inspect the fixture and confirm completion."
        baseline_llm, loom_llm = ScriptedLLM(responses), ScriptedLLM(responses)
        baseline_session, loom_session = Session(), Session()
        baseline_session.fail_next = loom_session.fail_next = timeout
        root = Path(self.directory.name)
        baseline_root = root / str(uuid4())
        baseline_root.mkdir()
        baseline = agent_class(baseline_llm, baseline_session)(
            logs_dir=baseline_root,
            model_name="openai/gpt-4o",
            max_turns=max_turns,
            api_base="http://gateway/openai/v1",
            record_terminal_session=True,
            enable_summarize=False,
            llm_kwargs={
                "api_key": "loom_step_fixture_credential",
                "temperature": 0.25,
                "top_p": 0.8,
            },
        )
        await baseline.setup(None)
        baseline_error = None
        try:
            await baseline.run(instruction, None, AgentContext())
        except RuntimeError as exc:
            baseline_error = str(exc)
        baseline_native = json.loads((baseline_root / "trajectory.json").read_text())

        driver = FakeDriver()
        await driver.start()
        self.addAsyncCleanup(driver.stop)
        events = Events()
        runtime = bridge.LoomTerminus2Runtime(
            model=ModelSpec(provider="openai", name="gpt-4o"),
            team_id=str(uuid4()),
            trial_id=uuid4(),
            cp_client=CP(loom_llm),
            gateway_url="http://gateway",
            max_turns=max_turns,
            request_params=(
                request_params
                if request_params is not None
                else {"temperature": 0.25, "top_p": 0.8}
            ),
        )
        loom_error = None
        with patch.object(
            bridge,
            "_import_terminus2",
            return_value=(
                agent_class(loom_llm, loom_session),
                AgentContext,
            ),
        ):
            try:
                await runtime.run(
                    instruction=instruction + instruction_suffix,
                    env=driver,
                    trajectory=events,
                    mcp=[],
                    skills_dir=None,
                    step_id="agent",
                )
            except AgentError as exc:
                loom_error = str(exc)
        native_bytes = driver.filesystem[PurePosixPath("/workspace/.loom/agent/trajectory.json")]
        self.assertNotIn(b"loom_step_fixture_credential", native_bytes)
        self.assertNotIn(b'"api_key"', native_bytes)
        loom_native = json.loads(native_bytes)
        if baseline_error is None:
            self.assertIsNone(loom_error)
        else:
            self.assertIn(baseline_error, loom_error)
        return (
            baseline_llm,
            loom_llm,
            baseline_session,
            loom_session,
            baseline_native,
            loom_native,
            events,
        )

    def assert_equivalent(self, pair):
        baseline_llm, loom_llm, baseline_session, loom_session, baseline, loom, events = pair
        (provenance,) = [
            event for event in events.events if event.kind == EventKind.TERMINUS2_RUNTIME_PROVENANCE
        ]
        self.assertEqual(provenance.effective_options["enable_summarize"], False)
        self.assertEqual(provenance.effective_options["record_terminal_session"], True)
        self.assertEqual(provenance.effective_options["continue_until_timeout"], False)
        self.assertEqual(provenance.effective_options["multi_model"], False)
        self.assertEqual(baseline_llm.init_options, loom_llm.init_options)
        self.assertEqual(baseline_llm.effective_options, loom_llm.effective_options)
        for name, value in loom_llm.effective_options.items():
            self.assertEqual(provenance.effective_options[name], value)
        self.assertEqual(baseline_llm.requests, loom_llm.requests)
        self.assertEqual(baseline_session.commands, loom_session.commands)
        self.assertEqual(native_steps(baseline), native_steps(loom))
        projected = Terminus2TrajectoryMapper.project_to_atif(
            events.events,
            task_id="fixture",
            agent_name="terminus-2",
            agent_version="fixture",
        )
        self.assertEqual(len(projected["steps"]), len(loom["steps"]))
        for source, exported in zip(loom["steps"], projected["steps"], strict=True):
            self.assertEqual(str(source["step_id"]), exported["step_id"])
            self.assertEqual(source["source"], exported["source"])
            self.assertEqual(source["message"], exported["message"])
            if source["source"] != "agent":
                continue
            self.assertEqual(source.get("reasoning_content"), exported.get("reasoning_content"))
            self.assertEqual(
                [
                    (call["function_name"], call["arguments"])
                    for call in source.get("tool_calls") or []
                ],
                [
                    (call["function_name"], call["arguments"])
                    for call in exported.get("tool_calls") or []
                ],
            )
            self.assertEqual(
                source["observation"]["results"][0]["content"], exported["observation"]
            )
            self.assertEqual(
                source["metrics"]["prompt_tokens"], exported["metrics"]["input_tokens"]
            )
            self.assertEqual(
                source["metrics"]["completion_tokens"], exported["metrics"]["output_tokens"]
            )

    async def test_commands_observations_completion_and_export(self):
        pair = await self.pair(
            [
                response(commands=("printf fixture\n", "pwd\n")),
                response(complete=True),
                response(complete=True),
            ]
        )
        self.assert_equivalent(pair)
        self.assertEqual(len(pair[0].requests), 3)
        self.assertEqual(len(pair[2].commands), 2)

    async def test_parser_retry_preserves_messages_and_usage(self):
        pair = await self.pair(["invalid JSON", response(complete=True), response(complete=True)])
        self.assert_equivalent(pair)
        self.assertIn("Previous response had parsing errors", pair[0].requests[1]["prompt"])

    async def test_explicit_turn_limit(self):
        pair = await self.pair([response(), response()], max_turns=2)
        self.assert_equivalent(pair)
        self.assertEqual(len(pair[0].requests), 2)

    async def test_command_timeout_observation(self):
        pair = await self.pair(
            [
                response(commands=("sleep 10\n",)),
                response(complete=True),
                response(complete=True),
            ],
            timeout=True,
        )
        self.assert_equivalent(pair)

    async def test_terminal_model_error_is_preserved(self):
        pair = await self.pair(
            [response(), *[RuntimeError("fixture model failure") for _ in range(3)]]
        )
        self.assert_equivalent(pair)

    async def test_prompt_drift_is_detected(self):
        pair = await self.pair(
            [response(complete=True), response(complete=True)],
            instruction_suffix=" silently changed",
        )
        with self.assertRaises(AssertionError):
            self.assert_equivalent(pair)

    async def test_sampling_parameter_drift_is_detected(self):
        pair = await self.pair(
            [response(complete=True), response(complete=True)],
            request_params={"temperature": 0.9, "top_p": 0.8},
        )
        with self.assertRaises(AssertionError):
            self.assert_equivalent(pair)

    async def test_command_drift_is_detected(self):
        pair = await self.pair([response(commands=("pwd\n",))], max_turns=1)
        pair[3].commands[0] = ("different command", {})
        with self.assertRaises(AssertionError):
            self.assert_equivalent(pair)


if __name__ == "__main__":
    unittest.main(verbosity=2)
