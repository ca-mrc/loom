import asyncio
import fnmatch
import shlex
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pytest

from loom.agent.oracle import OracleAgent
from loom.attempt_deadline import AttemptDeadline
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.networking import Public
from loom.models.result import StepResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.trajectory import EventKind
from loom.models.trial import BackoffSpec, RetryPolicy, RetryReason
from loom.models.verifier import VerifierResult
from loom.trajectory.attempt_guard import AttemptTrajectoryFencedError
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.attempt_supervisor import AttemptTimeoutDiagnostic
from loom.trial.step_runner import _isolated_verifier_start_options, run_step
from loom.trial.trial import TrialContext
from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
    materialize_workspace,
)
from tests._trial_config_defaults import stub_trial_config


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


class _TTLCapturingAgent:
    name = "ttl-capturing"
    step_token_ttl_sec = 1800
    observed_step_token_ttl_sec: int | None = None

    async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.observed_step_token_ttl_sec = self.step_token_ttl_sec


@pytest.fixture
async def context(tmp_path: Path) -> TrialContext:
    sol = tmp_path / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text("#!/bin/sh\necho ok\n")
    (sol / "solve.sh").chmod(0o755)

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
                stdout=b"ok\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.05,
            ),
        }
    )
    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    trial_id = uuid4()
    return TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=tmp_path,
        trial_config=stub_trial_config(),
        driver=driver,
        agent=OracleAgent(task_dir=tmp_path, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=FakeObjectStore(),
        local_trajectory_path=tmp_path / "events.jsonl",
        workspace_staging_policy=WorkspaceStagingPolicy.from_provenance(
            TB21_AGENT_WORKSPACE_POLICY,
        ),
        verifier_driver_factory=FakeDriver,
    )


def test_isolated_verifier_inherits_local_cgroup_parent(
    context: TrialContext,
) -> None:
    context.container_cgroup_parent = "/local-test.slice/worker.scope"

    options = _isolated_verifier_start_options(context)

    assert options.cgroup_parent == "/local-test.slice/worker.scope"


async def test_run_step_happy_path(context: TrialContext, tmp_path: Path):
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )
    assert isinstance(sr, StepResult)
    assert sr.verifier_result is not None
    assert sr.verifier_result.rewards["passed"] == 1.0
    assert sr.error is None

    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_START in kinds
    assert EventKind.STEP_END in kinds


async def test_run_step_applies_effective_timeout_before_agent_run(
    context: TrialContext,
) -> None:
    agent = _TTLCapturingAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        override_agent_timeout_sec=9000.0,
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert result.error is None
    assert agent.observed_step_token_ttl_sec == 9301


@pytest.mark.parametrize("wrapped", [False, True])
async def test_run_step_records_agent_error(context: TrialContext, tmp_path: Path, wrapped: bool):
    """If the agent raises AgentError, run_step records it as a phase=agent
    StepError and still emits step_end."""
    from loom.errors import AgentError

    class ContextLengthExceededError(Exception):
        pass

    class _BoomAgent:
        mode = "out-of-box"
        name = "boom"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_):  # type: ignore[no-untyped-def]
            if wrapped:
                try:
                    raise ContextLengthExceededError
                except ContextLengthExceededError as exc:
                    raise AgentError("") from exc
            raise AgentError("boom")

    context.agent = _BoomAgent()  # type: ignore[assignment]
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )
    assert sr.error is not None
    assert sr.error.phase == "agent"
    assert sr.error.reason == "exception"
    assert sr.error.exception_type == ("ContextLengthExceededError" if wrapped else "AgentError")
    assert sr.error.message == ("ContextLengthExceededError" if wrapped else "boom")
    assert StepResult.model_validate_json(sr.model_dump_json()).error == sr.error
    # step_end still fires.
    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_END in kinds


async def test_gateway_deadline_response_uses_timeout_supervisor_path(
    context: TrialContext,
) -> None:
    """A signed Gateway cutoff is the same timeout as the local deadline."""
    from loom.errors import AgentError

    class _GatewayDeadlineAgent:
        mode = "out-of-box"
        name = "gateway-deadline"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
            raise AgentError(
                "gateway 504: {'detail': {'code': 'agent_timeout', "
                "'reason': 'attempt_deadline_reached'}}"
            )

    context.agent = _GatewayDeadlineAgent()  # type: ignore[assignment]
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    events = list(TrajectoryReader(context.local_trajectory_path).iter_all())
    assert result.error is not None
    assert result.error.reason == "timeout"
    assert sum(event.kind == EventKind.AGENT_TIMEOUT for event in events) == 1


async def test_gateway_deadline_retry_records_timeout_before_retry(
    context: TrialContext,
) -> None:
    from loom.errors import AgentError

    class _RetryGatewayDeadlineAgent:
        mode = "out-of-box"
        name = "retry-gateway-deadline"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.calls = 0

        async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                raise AgentError(
                    "gateway 504: {'detail': {'code': 'agent_timeout', "
                    "'reason': 'attempt_deadline_reached'}}"
                )

    agent = _RetryGatewayDeadlineAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        skip_verifier=True,
        retry=RetryPolicy(
            max_attempts=2,
            retry_on=frozenset({RetryReason.AGENT_TIMEOUT}),
            backoff=BackoffSpec(base_sec=0.001, max_sec=0.001, jitter=0),
        ),
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    events = list(TrajectoryReader(context.local_trajectory_path).iter_all())
    timeout_index = next(
        index for index, event in enumerate(events) if event.kind == EventKind.AGENT_TIMEOUT
    )
    retry_index = next(
        index for index, event in enumerate(events) if event.kind == EventKind.AGENT_RETRY
    )
    assert result.error is None
    assert agent.calls == 2
    assert timeout_index < retry_index
    assert sum(event.kind == EventKind.AGENT_TIMEOUT for event in events) == 1


async def test_run_step_retries_retryable_gateway_failure(context: TrialContext):
    """A transient gateway 503 during agent.run should consume retry budget
    and continue the same step before the trial is marked failed."""

    class _FlakyGatewayAgent:
        mode = "out-of-box"
        name = "flaky-gateway"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.calls = 0

        async def run(self, **_):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                request = httpx.Request(
                    "POST",
                    "http://loom-llm-gateway:9100/v1/chat/completions",
                )
                response = httpx.Response(503, request=request)
                raise httpx.HTTPStatusError(
                    "HTTP 503",
                    request=request,
                    response=response,
                )

    agent = _FlakyGatewayAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        retry=RetryPolicy(
            max_attempts=2,
            retry_on=frozenset({RetryReason.GATEWAY_ERROR}),
            backoff=BackoffSpec(base_sec=0.001, max_sec=0.001, jitter=0),
        )
    )

    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert sr.error is None
    assert agent.calls == 2
    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.AGENT_RETRY in kinds


async def test_run_step_retries_textual_provider_transport_disconnect(
    context: TrialContext,
):
    """Subprocess agents can only surface some provider/gateway transport
    disconnects as text. These should consume retry budget like typed gateway
    transport exceptions instead of becoming immediate agent failures."""
    from loom.errors import AgentError

    class _FlakySubprocessAgent:
        mode = "out-of-box"
        name = "codex"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.calls = 0

        async def run(self, **_):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                raise AgentError(
                    "codex exited rc=1 on step main; stderr: "
                    "Server disconnected without sending a response."
                )

    agent = _FlakySubprocessAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        retry=RetryPolicy(
            max_attempts=2,
            backoff=BackoffSpec(base_sec=0.001, max_sec=0.001, jitter=0),
        )
    )

    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert sr.error is None
    assert agent.calls == 2
    reader = TrajectoryReader(context.local_trajectory_path)
    retry_events = [e for e in reader.iter_all() if e.kind == EventKind.AGENT_RETRY]
    assert len(retry_events) == 1
    assert retry_events[0].failure_reason == "provider_transport_disconnect"


async def test_run_step_records_verifier_exception(context: TrialContext):
    """Regression for Bug 3: previously only TimeoutError was caught around
    the verifier. A VerifierError used to escape step_runner entirely,
    bypassing per-step error tracking and the StepEndEvent emission."""
    from loom.errors import VerifierError

    class _BoomVerifier:
        name = "boom"

        async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
            raise VerifierError("simulated")

    context.verifier = _BoomVerifier()  # type: ignore[assignment]
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )
    assert sr.error is not None
    assert sr.error.phase == "verifier"
    assert sr.error.reason == "exception"
    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_END in kinds


async def test_run_step_records_verifier_timeout_as_structured_result(
    context: TrialContext,
):
    class _HungVerifier:
        name = "hung"

        async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
            import asyncio

            await asyncio.sleep(1)

    context.verifier = _HungVerifier()  # type: ignore[assignment]
    context.trial_config = stub_trial_config(override_verifier_timeout_sec=0.01)
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert sr.error is not None
    assert sr.error.phase == "verifier"
    assert sr.error.reason == "timeout"
    assert sr.verifier_result is not None
    assert sr.verifier_result.error is not None
    assert sr.verifier_result.error.kind == "timeout"
    assert sr.verifier_result.error.detail["timeout_sec"] == 0.01
    assert sr.verifier_result.error.detail["step_name"] == "main"
    # #377: verifier-timeout evidence now includes elapsed_sec and a
    # best-effort in-sandbox probe so operators can distinguish
    # "stuck in a wait" from "task genuinely too slow" from "harness
    # bug" without a rerun.
    assert "elapsed_sec" in sr.verifier_result.error.detail
    assert sr.verifier_result.error.detail["elapsed_sec"] >= 0.0
    assert "post_mortem_probe" in sr.verifier_result.error.detail
    assert isinstance(sr.verifier_result.error.detail["post_mortem_probe"], str)


async def test_run_step_collects_verifier_required_artifacts_after_verifier(
    context: TrialContext,
):
    class _WritesRequiredArtifactVerifier:
        name = "script"

        async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
            env.filesystem[artifacts_dir / "design_parameters.brand"] = b"brand\n"
            return VerifierResult(rewards={"score": 1.0})

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            pattern = shlex.split(cmd)[3]
            matches = [
                path.as_posix().encode()
                for path in sorted(context.driver.filesystem)
                if fnmatch.fnmatch(path.as_posix(), pattern)
            ]
            return ExecResult(
                return_code=0,
                stdout=b"\x00".join(matches) + (b"\x00" if matches else b""),
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.01,
        )

    context.trial_config = stub_trial_config(verifier_env_mode="shared")
    context.driver.exec_handler = handler
    context.verifier = _WritesRequiredArtifactVerifier()  # type: ignore[assignment]
    step = StepConfig(
        name="main",
        artifacts=[],
        required_artifacts=["design_parameters.brand"],
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        sr = await run_step(
            ctx=context,
            step=step,
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert sr.error is None
    assert [artifact.key for artifact in sr.artifacts] == [
        f"{context.team_id}/{context.trial_id}/main/design_parameters.brand",
    ]
    assert (
        "artifacts",
        f"{context.team_id}/{context.trial_id}/main/design_parameters.brand",
    ) in context.object_store.objects


async def test_run_step_marks_missing_required_artifact_invalid(
    context: TrialContext,
):
    step = StepConfig(
        name="main",
        artifacts=[],
        required_artifacts=["missing-required.asset"],
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        sr = await run_step(
            ctx=context,
            step=step,
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert sr.error is not None
    assert sr.error.phase == "artifacts"
    assert sr.error.reason == "missing_artifacts"
    assert "missing-required.asset" in sr.error.message


async def test_run_step_skip_verifier(context: TrialContext):
    """trial_config.skip_verifier=True omits the verifier phase entirely."""
    context.trial_config = stub_trial_config(skip_verifier=True)
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )
    assert sr.verifier_result is None


async def test_private_verifier_uses_fresh_driver_without_exposing_agent_workspace(
    context: TrialContext,
) -> None:
    """A background agent process must never observe verifier-only files.

    The observer is deliberately activated after public staging and before the
    verifier phase.  A late private upload to the agent's container therefore
    fails this regression test; a fresh verifier driver passes it.
    """

    class _ObservingDriver(FakeDriver):
        def __init__(self) -> None:
            super().__init__()
            self.background_observer_active = False
            self.private_paths_observed: list[PurePosixPath] = []

        async def upload(self, src: Path, dst: PurePosixPath) -> None:
            if self.background_observer_active and (
                "/solution/" in dst.as_posix()
                or "/tests/" in dst.as_posix()
                or "/verifier/" in dst.as_posix()
                or dst.name == "upstream-task.toml"
            ):
                self.private_paths_observed.append(dst)
            await super().upload(src, dst)

        async def exec(self, cmd, **kwargs):  # type: ignore[no-untyped-def]
            if cmd.startswith("find "):
                paths = [
                    path.as_posix().encode()
                    for path in sorted(self.filesystem)
                    if path.is_relative_to(PurePosixPath("/workspace"))
                ]
                return ExecResult(
                    return_code=0,
                    stdout=b"\x00".join(paths) + (b"\x00" if paths else b""),
                    stderr=b"",
                    truncated=False,
                    duration_sec=0,
                )
            return await super().exec(cmd, **kwargs)

    class _BackgroundAgent:
        mode = "out-of-box"
        name = "background-agent"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, *, env, **_kwargs):  # type: ignore[no-untyped-def]
            env.background_observer_active = True
            env.filesystem[PurePosixPath("/workspace/agent-output.txt")] = b"answer"

    class _VerifierThatCapturesItsDriver:
        name = "pass"

        def __init__(self) -> None:
            self.env: FakeDriver | None = None

        async def verify(self, *, env, **_kwargs):  # type: ignore[no-untyped-def]
            self.env = env
            assert PurePosixPath("/workspace/verifier/run.sh") in env.filesystem
            assert PurePosixPath("/workspace/solution/solve.sh") in env.filesystem
            assert PurePosixPath("/workspace/agent-output.txt") in env.filesystem
            return VerifierResult(rewards={"passed": 1.0})

    task_dir = context.task_dir
    (task_dir / "instruction.md").write_text("Solve it\n")
    (task_dir / "tests").mkdir()
    (task_dir / "tests" / "private-test.sh").write_text("private\n")
    (task_dir / "verifier").mkdir()
    (task_dir / "verifier" / "run.sh").write_text("#!/bin/sh\n")
    (task_dir / "upstream-task.toml").write_text("private upstream\n")

    agent_driver = _ObservingDriver()
    await agent_driver.start(options=StartOptions())
    verifier_driver = _ObservingDriver()
    context.driver = agent_driver
    context.agent = _BackgroundAgent()  # type: ignore[assignment]
    context.workspace_staging_policy = WorkspaceStagingPolicy.from_provenance(
        TB21_AGENT_WORKSPACE_POLICY,
    )
    context.verifier_driver_factory = lambda: verifier_driver  # type: ignore[attr-defined]
    verifier = _VerifierThatCapturesItsDriver()
    context.verifier = verifier  # type: ignore[assignment]

    await materialize_workspace(
        driver=agent_driver,
        task_dir=task_dir,
        dst=PurePosixPath("/workspace"),
        policy=context.workspace_staging_policy,
        phase="agent",
    )
    assert agent_driver.private_paths_observed == []
    assert PurePosixPath("/workspace/verifier/run.sh") not in agent_driver.filesystem

    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    assert result.error is None
    assert verifier.env is verifier_driver
    assert verifier_driver.state == "stopped"
    assert agent_driver.private_paths_observed == []
    assert PurePosixPath("/workspace/verifier/run.sh") not in agent_driver.filesystem


async def test_private_verifier_driver_is_stopped_when_step_is_cancelled(
    context: TrialContext,
) -> None:
    entered_verifier = asyncio.Event()
    stop_started = asyncio.Event()
    allow_stop = asyncio.Event()

    class _HandoffDriver(FakeDriver):
        async def exec(self, cmd, **kwargs):  # type: ignore[no-untyped-def]
            if cmd.startswith("find "):
                paths = [
                    path.as_posix().encode()
                    for path in sorted(self.filesystem)
                    if path.is_relative_to(PurePosixPath("/workspace"))
                ]
                return ExecResult(
                    return_code=0,
                    stdout=b"\x00".join(paths) + (b"\x00" if paths else b""),
                    stderr=b"",
                    truncated=False,
                    duration_sec=0,
                )
            return await super().exec(cmd, **kwargs)

        async def stop(self, *, delete: bool = True) -> None:
            stop_started.set()
            await allow_stop.wait()
            await super().stop(delete=delete)

    class _BlockingVerifier:
        name = "blocking"

        async def verify(self, **_kwargs):  # type: ignore[no-untyped-def]
            entered_verifier.set()
            await asyncio.Future()

    task_dir = context.task_dir
    (task_dir / "tests").mkdir()
    (task_dir / "tests" / "private-test.sh").write_text("private\n")
    (task_dir / "verifier").mkdir()
    (task_dir / "verifier" / "run.sh").write_text("#!/bin/sh\n")
    (task_dir / "upstream-task.toml").write_text("private upstream\n")
    verifier_driver = _HandoffDriver()
    context.workspace_staging_policy = WorkspaceStagingPolicy.from_provenance(
        TB21_AGENT_WORKSPACE_POLICY,
    )
    context.verifier_driver_factory = lambda: verifier_driver
    context.verifier = _BlockingVerifier()  # type: ignore[assignment]

    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        running = asyncio.create_task(
            run_step(
                ctx=context,
                step=context.task_config.steps[0],
                trajectory=writer,
                baseline_policy=Public(),
            )
        )
        await asyncio.wait_for(entered_verifier.wait(), timeout=2)
        running.cancel()
        await asyncio.wait_for(stop_started.wait(), timeout=2)
        await asyncio.sleep(0)
        assert not running.done(), "cancellation must wait for verifier teardown"
        allow_stop.set()
        with pytest.raises(asyncio.CancelledError):
            await running

    assert verifier_driver.state == "stopped"


async def test_agent_deadline_emits_one_diagnostic_and_does_not_retry_by_default(
    context: TrialContext,
) -> None:
    class _DeadlineAgent:
        mode = "out-of-box"
        name = "deadline"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.deadlines: list[AttemptDeadline] = []
            self.close_calls = 0
            self.late_write_rejected = False

        def begin_attempt(self, deadline: AttemptDeadline) -> None:
            self.deadlines.append(deadline)

        async def aclose_attempt(self) -> None:
            self.close_calls += 1

        async def run(self, *, trajectory, **_kwargs):  # type: ignore[no-untyped-def]
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                with pytest.raises(AttemptTrajectoryFencedError):
                    await trajectory.write_raw_dict({"kind": "late"})
                self.late_write_rejected = True
                raise

    agent = _DeadlineAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        override_agent_timeout_sec=0.01,
        skip_verifier=True,
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    events = list(TrajectoryReader(context.local_trajectory_path).iter_all())
    timeout_events = [event for event in events if event.kind == EventKind.AGENT_TIMEOUT]
    retry_events = [event for event in events if event.kind == EventKind.AGENT_RETRY]
    assert result.error is not None
    assert result.error.phase == "agent"
    assert result.error.reason == "timeout"
    assert len(timeout_events) == 1
    assert timeout_events[0].configured_timeout_sec == 0.01
    assert timeout_events[0].task_stopped is True
    assert retry_events == []
    assert len(agent.deadlines) == 1
    assert agent.close_calls == 1
    assert agent.late_write_rejected


async def test_explicit_agent_timeout_retry_gets_a_new_deadline(
    context: TrialContext,
) -> None:
    class _RetryDeadlineAgent:
        mode = "out-of-box"
        name = "retry-deadline"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.calls = 0
            self.deadlines: list[AttemptDeadline] = []
            self.close_calls = 0

        def begin_attempt(self, deadline: AttemptDeadline) -> None:
            self.deadlines.append(deadline)

        async def aclose_attempt(self) -> None:
            self.close_calls += 1

        async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            deadline = self.deadlines[-1]
            await deadline.record_step_token_grant(
                agent_attempt_id=deadline.agent_attempt_id,
                step_jwt_id=uuid4(),
            )
            if self.calls == 1:
                await asyncio.Future()

    agent = _RetryDeadlineAgent()
    context.agent = agent  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        override_agent_timeout_sec=0.01,
        skip_verifier=True,
        retry=RetryPolicy(
            max_attempts=2,
            retry_on=frozenset({RetryReason.AGENT_TIMEOUT}),
            backoff=BackoffSpec(base_sec=0.001, max_sec=0.001, jitter=0),
        ),
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    events = list(TrajectoryReader(context.local_trajectory_path).iter_all())
    assert result.error is None
    assert agent.calls == 2
    assert len(agent.deadlines) == 2
    assert agent.deadlines[0] is not agent.deadlines[1]
    starts = [event for event in events if event.kind == EventKind.AGENT_ATTEMPT_START]
    timeouts = [event for event in events if event.kind == EventKind.AGENT_TIMEOUT]
    assert len(starts) == 2
    assert starts[0].agent_attempt_id == agent.deadlines[0].agent_attempt_id
    assert starts[1].agent_attempt_id == agent.deadlines[1].agent_attempt_id
    assert starts[0].agent_attempt_id != starts[1].agent_attempt_id
    assert timeouts[0].agent_attempt_id == starts[0].agent_attempt_id
    grants = [event for event in events if event.kind == EventKind.AGENT_STEP_TOKEN_GRANT]
    assert len(grants) == 2
    assert [event.agent_attempt_id for event in grants] == [
        event.agent_attempt_id for event in starts
    ]
    assert grants[0].step_jwt_id != grants[1].step_jwt_id
    assert agent.close_calls == 2
    assert sum(event.kind == EventKind.AGENT_TIMEOUT for event in events) == 1
    assert sum(event.kind == EventKind.AGENT_RETRY for event in events) == 1


async def test_cancellation_resistant_agent_skips_verifier_and_artifacts(
    context: TrialContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _NeverRunAgent:
        mode = "out-of-box"
        name = "unhealthy-deadline"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("fake supervisor must not call agent")

    class _RecordingVerifier:
        name = "recording"

        def __init__(self) -> None:
            self.calls = 0

        async def verify(self, **_kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            return VerifierResult(rewards={"passed": 1.0})

    async def fake_supervise(*, agent, **_kwargs):  # type: ignore[no-untyped-def]
        agent._loom_worker_unhealthy = True
        return AttemptTimeoutDiagnostic(
            configured_timeout_sec=0.01,
            elapsed_monotonic_sec=0.03,
            cancellation_drain_sec=0.02,
            transport_close_required=True,
            task_stopped=False,
        )

    agent = _NeverRunAgent()
    verifier = _RecordingVerifier()
    context.agent = agent  # type: ignore[assignment]
    context.verifier = verifier  # type: ignore[assignment]
    context.trial_config = stub_trial_config(
        override_agent_timeout_sec=0.01,
        retry=RetryPolicy(
            max_attempts=2,
            retry_on=frozenset({RetryReason.AGENT_TIMEOUT}),
            backoff=BackoffSpec(base_sec=0.001, max_sec=0.001, jitter=0),
        ),
    )
    monkeypatch.setattr(
        "loom.trial.step_runner.supervise_agent_attempt",
        fake_supervise,
    )
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket,
        key=context.trajectory_key,
        min_part_bytes=0,
    )

    async with writer:
        result = await run_step(
            ctx=context,
            step=context.task_config.steps[0],
            trajectory=writer,
            baseline_policy=Public(),
        )

    events = list(TrajectoryReader(context.local_trajectory_path).iter_all())
    assert result.error is not None
    assert result.error.reason == "timeout"
    assert result.verifier_result is None
    assert result.artifacts == []
    assert verifier.calls == 0
    assert sum(event.kind == EventKind.AGENT_TIMEOUT for event in events) == 1
    assert sum(event.kind == EventKind.AGENT_RETRY for event in events) == 0
    assert sum(event.kind == EventKind.STEP_END for event in events) == 1
