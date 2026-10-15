"""The shared trial pipeline carries task policy to the actual runtime."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.agent.oracle import OracleAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.fake import FakeDriver
from loom.models.result import FailureReason, TrialState
from loom.models.task import AgentDefaults
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import Trial, TrialContext
from tests._trial_config_defaults import stub_trial_config
from tests.integration.test_trial_runner import _AlwaysPassVerifier, _task_config


def context(tmp_path, agent, enabled):
    (tmp_path / "instruction.md").write_text("Inspect the fixture.")
    task = _task_config().model_copy(update={"agent": AgentDefaults(
        name="oracle", continue_until_timeout=enabled,
    )})
    return TrialContext(
        trial_id=uuid4(), team_id=uuid4(), task_config=task, task_checksum="0" * 64,
        task_dir=tmp_path, trial_config=stub_trial_config(), driver=FakeDriver(),
        agent=agent, verifier=_AlwaysPassVerifier(), object_store=FakeObjectStore(),
        local_trajectory_path=tmp_path / "trajectory.jsonl",
        verifier_driver_factory=FakeDriver,
    )


@pytest.mark.parametrize("enabled", [False, True])
async def test_pipeline_sets_task_policy_before_setup_and_attempt(tmp_path, enabled):
    agent = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="glm-5.2"), team_id=str(uuid4()),
        trial_id=uuid4(), cp_client=None, gateway_url="http://127.0.0.1:9000",
        continue_until_timeout=not enabled,
    )
    seen = []

    async def setup(**kwargs):
        seen.append(("setup", agent.continue_until_timeout))

    async def run(**kwargs):
        seen.append(("run", agent.continue_until_timeout))
        assert agent._attempt_deadline is not None

    agent.setup = AsyncMock(side_effect=setup)
    agent.run = AsyncMock(side_effect=run)
    await Trial(ctx=context(tmp_path, agent, enabled), state_patch=None).run()
    assert seen == [("setup", enabled), ("run", enabled)]


async def test_pipeline_rejects_unsupported_agent_before_starting_sandbox(tmp_path):
    ctx = context(tmp_path, OracleAgent(task_dir=tmp_path, trial_id=uuid4()), True)
    result = await Trial(ctx=ctx, state_patch=None).run()
    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TASK_COMPATIBILITY
    assert "continue_until_timeout requires the pinned Terminus-2 runtime" in result.failure_message
    assert ctx.driver.state == "constructed"


async def test_continuation_without_deadline_fails_before_token_or_harbor(tmp_path):
    cp = AsyncMock()
    agent = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="glm-5.2"), team_id=str(uuid4()),
        trial_id=uuid4(), cp_client=cp, gateway_url="http://127.0.0.1:9000",
        continue_until_timeout=True,
    )
    from loom.errors import AgentError

    with pytest.raises(AgentError, match="absolute attempt deadline"):
        await agent.run(instruction="Inspect", env=FakeDriver(), trajectory=AsyncMock(),
                        mcp=[], skills_dir=None, step_id="agent")
    cp.mint_step_token.assert_not_called()
