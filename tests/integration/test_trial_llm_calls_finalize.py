"""A11.1 regression: Trial.run() queries TrialContext.llm_calls_fetcher
at finalize and appends each row as an LLMCallEvent on the local
trajectory JSONL BEFORE the ATIF projection runs.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

from loom.agent.gateway_client import FakeLLMGatewayClient, GatewayCallResponse
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import Trial, TrialContext
from tests._trial_config_defaults import stub_trial_config


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


async def test_trial_appends_llm_calls_before_finalize(tmp_path: Path) -> None:
    """Seed two llm_calls rows via a fetcher; assert the local JSONL has
    them as `kind=llm_call` events alongside the agent's normal events."""

    # Fixture task: hello-world style.
    sol = tmp_path / "task" / "solution"
    sol.mkdir(parents=True)
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    (sol.parent / "task.toml").write_text('schema_version = "1"\n')

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler(
        {
            "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
                return_code=0,
                stdout=b"hello\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.05,
            ),
        }
    )
    trial_id = uuid4()

    captured_calls: list[UUID] = []

    async def fetcher(tid: UUID) -> list[dict]:
        captured_calls.append(tid)
        return [
            {
                "captured_at": "2026-06-07T00:00:00Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "claude-opus-4-7",
                "dialect": "anthropic",
                "input_tokens": 100,
                "output_tokens": 50,
                "provider_extras": {"cache_read_input_tokens": 20},
                "request_params": {
                    "status": "available",
                    "parameters": {
                        "temperature": 0,
                        "top_p": 1,
                    },
                },
                "cost_usd": 0.001,
                "rate_card_hash": "abc",
            },
            {
                "captured_at": "2026-06-07T00:00:01Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "claude-opus-4-7",
                "dialect": "anthropic",
                "input_tokens": 80,
                "output_tokens": 40,
                "provider_extras": {},
                "cost_usd": 0.0008,
                "rate_card_hash": "abc",
            },
            # Provider-connection facade row: dialect="openai_facade"
            # MUST map to provider="openai" in the projection (else
            # ATIF gets provider="unknown"). Regression guard for the
            # cluster-deploy.md §gateway-facade plumbing.
            {
                "captured_at": "2026-06-07T00:00:02Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "gpt-4o",
                "dialect": "openai_facade",
                "input_tokens": 10,
                "output_tokens": 5,
                "provider_extras": {},
                "cost_usd": 0.0001,
                "rate_card_hash": "facade:operator-supplied",
            },
            # Anthropic facade row — same projection guard, different
            # dialect string. Was unknown before PR adding
            # "anthropic_facade": "anthropic" to the map.
            {
                "captured_at": "2026-06-07T00:00:03Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "claude-opus-4-7",
                "dialect": "anthropic_facade",
                "input_tokens": 7,
                "output_tokens": 3,
                "provider_extras": {},
                "cost_usd": 0.00007,
                "rate_card_hash": "facade:operator-supplied",
            },
            # Gemini facade row — same projection guard for the
            # google variant. Was unknown before adding the
            # "gemini_facade": "google" entry to the map.
            {
                "captured_at": "2026-06-07T00:00:04Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "gemini-2.5-flash",
                "dialect": "gemini_facade",
                "input_tokens": 5,
                "output_tokens": 2,
                "provider_extras": {},
                "cost_usd": 0.00005,
                "rate_card_hash": "facade:operator-supplied",
            },
        ]

    store = FakeObjectStore()
    local_path = tmp_path / "trajectory.jsonl"
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=sol.parent,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=sol.parent, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),  # type: ignore[arg-type]
        object_store=store,
        local_trajectory_path=local_path,
        llm_calls_fetcher=fetcher,
        verifier_driver_factory=FakeDriver,
    )

    trial = Trial(ctx=ctx)
    result = await trial.run()
    assert result.state.value == "succeeded"

    # The fetcher was called with the trial_id.
    assert captured_calls == [trial_id]

    # The local JSONL has both llm_call events appended.
    parsed_lines = [json.loads(line) for line in local_path.read_text().splitlines()]
    assert [event["seq"] for event in parsed_lines] == list(range(len(parsed_lines)))
    assert parsed_lines[-1]["kind"] == "trial_end"

    parsed = [event for event in parsed_lines if event.get("kind") == "llm_call"]
    assert len(parsed) == 5
    assert parsed[0]["input_tokens"] == 100
    assert parsed[1]["input_tokens"] == 80
    assert parsed[2]["input_tokens"] == 10
    assert parsed[3]["input_tokens"] == 7
    assert parsed[4]["input_tokens"] == 5
    # Dialect is encoded as ModelSpec.provider on the synthetic event.
    assert parsed[0]["model"]["provider"] == "anthropic"
    # Regression: openai_facade dialect → provider="openai" (was
    # "unknown" before the projection-map fix in PR #65).
    assert parsed[2]["model"]["provider"] == "openai"
    assert parsed[2]["model"]["name"] == "gpt-4o"
    # Regression: anthropic_facade dialect → provider="anthropic" (was
    # "unknown" before adding the entry in PR #83 — same gotcha that
    # PR #65 surfaced for openai_facade).
    assert parsed[3]["model"]["provider"] == "anthropic"
    assert parsed[3]["model"]["name"] == "claude-opus-4-7"
    # Regression: gemini_facade dialect → provider="google" (added
    # alongside the google facade route in this PR).
    assert parsed[4]["model"]["provider"] == "google"
    assert parsed[4]["model"]["name"] == "gemini-2.5-flash"
    assert parsed[0]["cached_input_tokens"] == 20
    assert parsed[0]["cost_usd_snapshot"] == 0.001
    assert parsed[0]["request_params"] == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "top_p": 1,
        },
    }
    assert parsed[1]["request_params"] == {
        "status": "unavailable_legacy",
        "parameters": {},
    }


async def test_trial_does_not_duplicate_litellm_agent_llm_calls(
    tmp_path: Path,
) -> None:
    """#257: LiteLLMAgent writes rich LLMCallEvents itself.

    The gateway also persists the same call in `llm_calls` for usage
    accounting. Finalize must not project that row back into the same
    trajectory, or Trial Detail shows two LLM calls for one model request.
    """

    task_dir = tmp_path / "task"
    task_dir.mkdir()
    (task_dir / "task.toml").write_text('schema_version = "1"\n')
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello-world", name="hello-world"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="litellm"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    trial_id = uuid4()
    model = ModelSpec(provider="openai", name="qwen2.5-coder-7b-instruct")
    agent = LiteLLMAgent(
        model=model,
        gateway=FakeLLMGatewayClient(
            scripted=[
                GatewayCallResponse(
                    response=ChatMessage(role="assistant", content="done"),
                    input_tokens=41,
                    cached_input_tokens=0,
                    cache_write_tokens=0,
                    output_tokens=142,
                    thinking_tokens=0,
                    provider_extras={},
                    cost_usd=0.0001,
                    finish_reason="stop",
                    duration_sec=0.05,
                    streamed=False,
                    time_to_first_token_sec=None,
                    rate_card_hash="card",
                    gateway_request_id="gateway-row-1",
                ),
            ]
        ),
        team_id=str(uuid4()),
        trial_id=trial_id,
        max_turns=1,
    )
    captured_calls: list[UUID] = []

    async def fetcher(tid: UUID) -> list[dict]:
        captured_calls.append(tid)
        return [
            {
                "id": "gateway-row-1",
                "captured_at": "2026-06-19T02:15:44Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "qwen2.5-coder-7b-instruct",
                "dialect": "openai_chat",
                "input_tokens": 41,
                "output_tokens": 142,
                "provider_extras": {},
                "cost_usd": 0.0001,
                "rate_card_hash": "card",
            },
        ]

    local_path = tmp_path / "trajectory.jsonl"
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=task_dir,
        trial_config=stub_trial_config(agent_name="litellm", agent_model=model),
        driver=FakeDriver(),
        agent=agent,
        verifier=_AlwaysPassVerifier(),  # type: ignore[arg-type]
        object_store=FakeObjectStore(),
        local_trajectory_path=local_path,
        llm_calls_fetcher=fetcher,
        verifier_driver_factory=FakeDriver,
    )

    result = await Trial(ctx=ctx).run()
    assert result.state.value == "succeeded"
    assert captured_calls == []

    llm_call_events = [
        json.loads(line)
        for line in local_path.read_text().splitlines()
        if json.loads(line).get("kind") == "llm_call"
    ]
    assert len(llm_call_events) == 1
    assert llm_call_events[0]["finish_reason"] == "stop"
    assert llm_call_events[0]["response"]["content"] == "done"


async def test_trial_skips_fetcher_when_none(tmp_path: Path) -> None:
    """Backwards-compat: a context without llm_calls_fetcher (v0.7 path)
    finalizes normally with no extra events injected."""
    sol = tmp_path / "task" / "solution"
    sol.mkdir(parents=True)
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    (sol.parent / "task.toml").write_text('schema_version = "1"\n')

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler(
        {
            "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
                return_code=0,
                stdout=b"hello\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.05,
            ),
        }
    )

    trial_id = uuid4()
    local_path = tmp_path / "trajectory.jsonl"
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=sol.parent,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=sol.parent, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),  # type: ignore[arg-type]
        object_store=FakeObjectStore(),
        local_trajectory_path=local_path,
        # No llm_calls_fetcher.
        verifier_driver_factory=FakeDriver,
    )

    trial = Trial(ctx=ctx)
    result = await trial.run()
    assert result.state.value == "succeeded"

    # JSONL has no llm_call events.
    lines = local_path.read_text().splitlines()
    assert all(json.loads(line).get("kind") != "llm_call" for line in lines)
