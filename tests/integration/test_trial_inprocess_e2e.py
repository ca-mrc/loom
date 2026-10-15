"""In-process E2E: Trial.run() with FakeDriver + OracleAgent + AlwaysPassVerifier.

Exercises the entire Plan 3 stack end-to-end against fakes.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest
from botocore.exceptions import ConnectionClosedError

from loom.agent.oracle import OracleAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.errors import AgentError
from loom.models.exec import ExecResult
from loom.models.result import FailureReason, TrialState
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierError, VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import Trial, TrialContext
from loom.trial.watchdog_cancellation import WatchdogCancellation, WatchdogTriggerReason
from tests._trial_config_defaults import stub_trial_config

pytestmark = pytest.mark.docker


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


class _MissingTestsVerifier:
    name = "missing-tests"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(
            rewards={},
            error=VerifierError(
                kind="missing_tests",
                message="pytest did not produce /loom/verifier/junit.xml",
            ),
        )


class _ScoredVerifierError:
    name = "scored-error"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(
            rewards={"valid": 0.0},
            error=VerifierError(kind="exec_failure", message="schema validation failed"),
        )


class _ScoredPassedZeroVerifier:
    """Mirrors #1186: verifier scores an unsolved workspace as passed=0.0."""

    name = "scored-zero"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 0.0})


class _HarborMissingAgent:
    """Model-backed agent that fails before any LLM call (#1186)."""

    mode = "in-box"
    name = "terminus-2"
    version = "1.0"
    supports_os = frozenset({"linux"})
    model = ModelSpec(provider="openai", name="glm-5.1", source="api")
    emits_gateway_llm_call_events = True

    async def setup(self, env) -> None:  # type: ignore[no-untyped-def]
        return None

    async def run(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        raise AgentError(
            "terminus-2 requires harbor@527d50d preinstalled in the worker image",
        )


class _FailingAtifObjectStore(FakeObjectStore):
    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        if key.endswith("/atif.json"):
            raise ConnectionClosedError(endpoint_url="http://127.0.0.1:19000")
        return await super().put_object(bucket=bucket, key=key, body=body)


@pytest.fixture
def hello_task(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    (d / "task.toml").write_text('schema_version = "1"\n')
    (d / "instruction.md").write_text("say hello\n")
    sol = d / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    return d


async def test_trial_run_happy_path(hello_task: Path, tmp_path: Path):
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
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
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )

    trial = Trial(ctx=ctx)
    result = await trial.run()

    assert result.state == TrialState.SUCCEEDED
    assert len(result.steps) == 1
    assert result.steps[0].verifier_result is not None
    assert result.steps[0].verifier_result.rewards["passed"] == 1.0
    assert result.trajectory_uri == ctx.trajectory_uri
    assert result.atif_uri is not None
    assert (ctx.trajectory_bucket, ctx.trajectory_key) in store.objects
    assert (
        ctx.trajectory_bucket,
        f"{ctx.team_id}/{ctx.trial_id}/atif.json",
    ) in store.objects


async def test_trial_run_records_finalize_failure_message(
    hello_task: Path,
    tmp_path: Path,
):
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
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
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=_FailingAtifObjectStore(),
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )

    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TRAJECTORY_FLUSH_FAILED
    assert result.failure_message is not None
    assert "finalize trajectory failed" in result.failure_message
    assert "ConnectionClosedError" in result.failure_message


async def test_trial_run_respects_environment_workdir(
    hello_task: Path,
    tmp_path: Path,
):
    class _WorkdirVerifier:
        name = "workdir"

        async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
            assert artifacts_dir == PurePosixPath("/app")
            return VerifierResult(rewards={"passed": 1.0})

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="alpine",
            workdir=PurePosixPath("/app"),
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler(
        {
            "chmod +x /app/solution/solve.sh && /app/solution/solve.sh": ExecResult(
                return_code=0,
                stdout=b"hello\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.05,
            ),
        }
    )
    driver = FakeDriver(exec_handler=handler)
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=driver,
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_WorkdirVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )

    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.SUCCEEDED
    assert PurePosixPath("/app/instruction.md") in driver.filesystem
    assert PurePosixPath("/workspace/instruction.md") not in driver.filesystem


async def test_trial_run_passes_task_sandbox_start_options(
    hello_task: Path,
    tmp_path: Path,
):
    class _CaptureStartOptionsDriver(FakeDriver):
        start_options: list[StartOptions | None]

        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            super().__init__(**kwargs)
            self.start_options = []

        async def start(self, *, options: StartOptions | None = None) -> None:
            self.start_options.append(options)
            await super().start(options=options)

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="alpine",
            environment={"BETA": "2", "ALPHA": "1"},
            extra_hosts={
                "example.com": "131.25.18.2",
                "archive.ubuntu.com": "162.242.195.82",
            },
            dns=["192.0.2.1"],
            tmpfs=["/root:size=100M,mode=755"],
        ),
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
    driver = _CaptureStartOptionsDriver(exec_handler=handler)
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=driver,
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        sandbox_extra_hosts=(("host.docker.internal", "host-gateway"),),
        container_cgroup_parent="/local-test.slice/worker.scope",
        verifier_driver_factory=FakeDriver,
    )

    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.SUCCEEDED
    assert len(driver.start_options) == 1
    start_options = driver.start_options[0]
    assert start_options is not None
    assert start_options.cgroup_parent == "/local-test.slice/worker.scope"
    assert start_options.environment == (("ALPHA", "1"), ("BETA", "2"))
    assert start_options.extra_hosts == (
        ("archive.ubuntu.com", "162.242.195.82"),
        ("example.com", "131.25.18.2"),
        ("host.docker.internal", "host-gateway"),
    )
    assert start_options.dns == ("192.0.2.1",)
    assert start_options.tmpfs == ("/root:size=100M,mode=755",)


async def test_trial_run_driver_start_failure_classified(
    hello_task: Path,
    tmp_path: Path,
):
    """Regression for Bug 1: a DriverError from start() used to escape
    Trial.run unhandled. Now classified into FailureReason.ENV_START_FAILURE
    and surfaced via TrialResult."""
    from loom.errors import DriverError
    from loom.models.result import FailureReason

    class _BoomDriver(FakeDriver):
        async def start(self, *, options: StartOptions | None = None) -> None:
            raise DriverError("simulated start failure")

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=_BoomDriver(),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()
    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.ENV_START_FAILURE


async def test_trial_run_state_patch_failure_doesnt_kill_trial(
    hello_task: Path,
    tmp_path: Path,
):
    """Regression for Bug 2: a state_patch HTTP error used to propagate out
    of Trial.run. Now logged + continued."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
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
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )

    async def _boom_patch(state: str, reason: str | None) -> None:
        raise RuntimeError(f"simulated PATCH 5xx for state={state}")

    result = await Trial(ctx=ctx, state_patch=_boom_patch).run()
    # Trial reached SUCCEEDED even though both PATCH calls raised.
    assert result.state == TrialState.SUCCEEDED


async def test_trial_run_cancellation_stashes_result(
    hello_task: Path,
    tmp_path: Path,
):
    """Regression for Bug 4: cancellation re-raises CancelledError, so the
    in-flight TrialResult used to be lost. Now stashed on trial.result so the
    caller can recover state=CANCELLED + partial steps + atif_uri."""
    import asyncio

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )

    class _SlowAgent:
        mode = "out-of-box"
        name = "slow"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_):  # type: ignore[no-untyped-def]
            await asyncio.sleep(60)  # cancelled out from under us

    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(),
        agent=_SlowAgent(),  # type: ignore[arg-type]
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    trial = Trial(ctx=ctx)
    task_handle = asyncio.create_task(trial.run())
    await asyncio.sleep(0.1)
    task_handle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task_handle
    # Bug 4 fix: result available via trial.result
    assert trial.result is not None
    assert trial.result.state == TrialState.CANCELLED


async def test_trial_run_watchdog_hard_deadline_records_agent_timeout(
    hello_task: Path,
    tmp_path: Path,
):
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle", timeout_sec=10.0),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )

    class _SlowAgent:
        mode = "out-of-box"
        name = "slow"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_):  # type: ignore[no-untyped-def]
            await asyncio.sleep(60)

    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(),
        agent=_SlowAgent(),  # type: ignore[arg-type]
        verifier=_AlwaysPassVerifier(),
        object_store=FakeObjectStore(),
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    trial = Trial(ctx=ctx)

    task_handle = asyncio.create_task(trial.run())
    await asyncio.sleep(0.1)
    task_handle.cancel(
        WatchdogCancellation(
            reason=WatchdogTriggerReason.HARD_DEADLINE,
            message="trial exceeded worker hard deadline",
            elapsed_sec=41.0,
            hard_deadline_sec=40.0,
        )
    )

    result = await task_handle

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.AGENT_TIMEOUT
    assert result.failure_message is not None
    assert "watchdog hard deadline" in result.failure_message
    assert "elapsed_sec=41" in result.failure_message
    assert "hard_deadline_sec=40" in result.failure_message


async def test_scored_verifier_cannot_override_attempt_timeout_evidence(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="slow", timeout_sec=0.01),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )

    class _SlowAgent:
        mode = "out-of-box"
        name = "slow"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
            await asyncio.sleep(60)

    store = FakeObjectStore()
    trial_id = uuid4()
    state_patches: list[tuple[str, str | None, str | None]] = []

    async def _record_state(
        state: str,
        reason: str | None,
        message: str | None,
    ) -> None:
        state_patches.append((state, reason, message))

    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(override_agent_timeout_sec=0.01),
        driver=FakeDriver(),
        agent=_SlowAgent(),  # type: ignore[arg-type]
        verifier=_AlwaysPassVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "deadline-events.jsonl",
        verifier_driver_factory=FakeDriver,
    )

    result = await Trial(ctx=ctx, state_patch=_record_state).run()

    events = [
        json.loads(line)
        for line in ctx.local_trajectory_path.read_text().splitlines()
    ]
    atif = json.loads(
        store.objects[
            (
                ctx.trajectory_bucket,
                f"{ctx.team_id}/{ctx.trial_id}/atif.json",
            )
        ]
    )
    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.AGENT_TIMEOUT
    assert result.steps[0].verifier_result is not None
    assert result.steps[0].verifier_result.rewards == {"passed": 1.0}
    assert events[-1]["kind"] == "trial_end"
    assert events[-1]["final_state"] == "failed"
    assert events[-1]["failure_reason"] == "agent_timeout"
    assert atif["metadata"]["final_state"] == "failed"
    assert atif["metadata"]["error"] == {"failure_reason": "agent_timeout"}
    assert state_patches[-1][:2] == ("failed", "agent_timeout")


async def test_trial_run_unscored_agent_error_marks_failed(
    hello_task: Path,
    tmp_path: Path,
):
    """Agent error without a usable verifier score remains a platform failure."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    # No matching command → falls through to default ExecResult(rc=0) → solve.sh exits 0.
    # Force agent error by passing a handler that returns non-zero.
    handler = command_table_handler(
        {
            "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
                return_code=42,
                stdout=b"",
                stderr=b"oops",
                truncated=False,
                duration_sec=0.01,
            ),
        }
    )
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_MissingTestsVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()
    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.AGENT_ERROR
    assert result.steps[0].error is not None
    assert result.steps[0].error.phase == "agent"


async def test_trial_run_scored_agent_error_stays_succeeded(
    hello_task: Path,
    tmp_path: Path,
):
    """Agent errors with explicit verifier rewards remain scored outcomes."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="structured"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler(
        {
            "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
                return_code=42,
                stdout=b"",
                stderr=b"oops",
                truncated=False,
                duration_sec=0.01,
            ),
        }
    )
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_ScoredVerifierError(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.SUCCEEDED
    assert result.failure_reason is None
    assert result.reward == {"valid": 0.0}
    assert result.steps[0].error is not None
    assert result.steps[0].error.phase == "agent"
    assert result.steps[0].verifier_result is not None
    assert result.steps[0].verifier_result.error is not None


async def test_trial_run_harbor_setup_error_not_succeeded_despite_reward(
    hello_task: Path,
    tmp_path: Path,
):
    """#1186: platform setup AgentError + verifier 0.0 must not count as succeeded."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="terminus-2"),
        verifier=VerifierDefaults(name="script"),
        steps=[StepConfig(name="main")],
    )
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(agent_name="terminus-2"),
        driver=FakeDriver(),
        agent=_HarborMissingAgent(),
        verifier=_ScoredPassedZeroVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TASK_COMPATIBILITY
    assert result.reward == {"passed": 0.0}
    assert result.steps[0].error is not None
    assert result.steps[0].error.phase == "agent"
    assert "Harbor pin" in (result.steps[0].error.message or "")


async def test_trial_run_empty_reward_verifier_error_marks_failed(
    hello_task: Path,
    tmp_path: Path,
):
    """Verifier infrastructure errors without rewards are platform failures."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
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
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_MissingTestsVerifier(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.VERIFIER_ERROR
    assert result.steps[0].error is None
    assert result.steps[0].verifier_result is not None
    assert result.steps[0].verifier_result.error is not None
    assert result.steps[0].verifier_result.error.kind == "missing_tests"


async def test_trial_run_scored_verifier_error_stays_succeeded(
    hello_task: Path,
    tmp_path: Path,
):
    """Verifier errors with an explicit reward remain scored outcomes."""
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="structured"),
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
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=hello_task, trial_id=trial_id),
        verifier=_ScoredVerifierError(),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=FakeDriver,
    )
    result = await Trial(ctx=ctx).run()

    assert result.state == TrialState.SUCCEEDED
    assert result.failure_reason is None
    assert result.reward == {"valid": 0.0}
    assert result.steps[0].verifier_result is not None
    assert result.steps[0].verifier_result.error is not None
