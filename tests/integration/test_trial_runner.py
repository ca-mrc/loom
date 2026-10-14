"""Integration: LocalTrialRunner wraps Trial.run() and propagates state via
the callback. Uses Plan 1+2+3 stack with fakes."""

from __future__ import annotations

import asyncio
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.oracle import OracleAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.result import FailureReason, TrialState
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    TaskSidecarConfig,
    VerifierDefaults,
)
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom.trial.workspace import TB21_AGENT_WORKSPACE_POLICY, WorkspaceStagingPolicy
from loom_worker.trial_runner import LocalTrialRunner
from tests._trial_config_defaults import stub_trial_config


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


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


def _task_config(
    *,
    artifacts: list[str] | None = None,
    sidecars: list[TaskSidecarConfig] | None = None,
    environment: dict[str, str] | None = None,
) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="alpine",
            sidecars=sidecars or [],
            environment=environment or {},
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main", artifacts=artifacts or [])],
    )


def _driver_factory(handler: Any) -> Any:
    return lambda: FakeDriver(exec_handler=handler)


def _docker_supporting_caps() -> Capabilities:
    return Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network", "allowlist"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto", "limit", "guarantee"]),
        supports_custom_network=True,
    )


async def test_runner_invokes_run_and_reports_states(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    state_calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        state_calls.append((state, failure_reason))
        return True

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
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED
    assert ("running", None) in state_calls
    assert ("succeeded", None) in state_calls


async def test_runner_scopes_trajectory_and_atif_to_claim_attempt(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    async def fake_state_patch(
        _state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        return True

    handler = command_table_handler(
        {
            "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": (
                ExecResult(
                    return_code=0,
                    stdout=b"hello\n",
                    stderr=b"",
                    truncated=False,
                    duration_sec=0.05,
                )
            ),
        }
    )
    trial_id = uuid4()
    team_id = uuid4()
    store = FakeObjectStore()
    root = tmp_path / "trajectories"
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=team_id,
        attempt_count=2,
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir,
            trial_id=trial_id,
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=store,
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=root,
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()

    prefix = f"{team_id}/{trial_id}/attempts/2"
    assert result.trajectory_uri == f"s3://trajectories/{prefix}/events.jsonl"
    assert result.atif_uri == f"s3://trajectories/{prefix}/atif.json"
    assert ("trajectories", f"{prefix}/events.jsonl") in store.objects
    assert ("trajectories", f"{prefix}/atif.json") in store.objects
    assert (root / f"{trial_id}.attempt-2.events.jsonl").is_file()


async def test_runner_uses_fresh_verifier_driver_for_private_workspace_policy(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    """The actual worker runner must supply the factory required by run_step."""

    class _HandoffDriver(FakeDriver):
        async def exec(self, cmd: str, **kwargs: object) -> ExecResult:
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

    class _PublicOutputAgent:
        mode = "out-of-box"
        # A user-controlled name is not sufficient to receive private assets.
        name = "oracle"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, *, env, **_kwargs):  # type: ignore[no-untyped-def]
            env.filesystem[PurePosixPath("/workspace/agent-output.txt")] = b"answer"

    class _CapturingVerifier:
        name = "capture"

        def __init__(self) -> None:
            self.env: FakeDriver | None = None

        async def verify(self, *, env, **_kwargs):  # type: ignore[no-untyped-def]
            self.env = env
            assert PurePosixPath("/workspace/verifier/run.sh") in env.filesystem
            assert PurePosixPath("/workspace/solution/solve.sh") in env.filesystem
            assert PurePosixPath("/workspace/agent-output.txt") in env.filesystem
            return VerifierResult(rewards={"passed": 1.0})

    (hello_task / "tests").mkdir()
    (hello_task / "tests" / "private-test.sh").write_text("private\n")
    (hello_task / "verifier").mkdir()
    (hello_task / "verifier" / "run.sh").write_text("#!/bin/sh\n")
    (hello_task / "upstream-task.toml").write_text("private upstream\n")
    drivers: list[_HandoffDriver] = []

    def driver_factory() -> _HandoffDriver:
        driver = _HandoffDriver()
        drivers.append(driver)
        return driver

    async def state_patch(
        _state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        return True

    trial_id = uuid4()
    verifier = _CapturingVerifier()
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=driver_factory,
        agent_factory=lambda *_args: _PublicOutputAgent(),  # type: ignore[return-value]
        verifier_factory=lambda: verifier,  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=state_patch,
        workspace_staging_policy=WorkspaceStagingPolicy.from_provenance(
            TB21_AGENT_WORKSPACE_POLICY,
        ),
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert len(drivers) == 2
    assert verifier.env is drivers[1]
    assert drivers[0].state == "stopped"
    assert drivers[1].state == "stopped"
    assert PurePosixPath("/workspace/verifier/run.sh") not in drivers[0].filesystem
    assert PurePosixPath("/workspace/solution/solve.sh") not in drivers[0].filesystem
    assert PurePosixPath("/workspace/tests/private-test.sh") not in drivers[0].filesystem


async def test_runner_grants_only_solution_to_trusted_oracle_runtime(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    """Canonical provenance + the concrete Oracle runtime unlock solution only."""

    class _HandoffDriver(FakeDriver):
        async def exec(self, cmd: str, **kwargs: object) -> ExecResult:
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

    class _Verifier:
        name = "capture"

        async def verify(self, *, env, **_kwargs):  # type: ignore[no-untyped-def]
            assert PurePosixPath("/workspace/solution/solve.sh") in env.filesystem
            assert PurePosixPath("/workspace/tests/private-test.sh") in env.filesystem
            assert PurePosixPath("/workspace/verifier/run.sh") in env.filesystem
            return VerifierResult(rewards={"passed": 1.0})

    (hello_task / "tests").mkdir()
    (hello_task / "tests" / "private-test.sh").write_text("private\n")
    (hello_task / "verifier").mkdir()
    (hello_task / "verifier" / "run.sh").write_text("#!/bin/sh\n")
    (hello_task / "upstream-task.toml").write_text("private upstream\n")
    drivers: list[_HandoffDriver] = []

    def driver_factory() -> _HandoffDriver:
        driver = _HandoffDriver(
            exec_handler=command_table_handler(
                {
                    "chmod +x /workspace/solution/solve.sh && "
                    "/workspace/solution/solve.sh": ExecResult(
                        return_code=0,
                        stdout=b"hello\n",
                        stderr=b"",
                        truncated=False,
                        duration_sec=0.01,
                    ),
                },
            ),
        )
        drivers.append(driver)
        return driver

    async def state_patch(
        _state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        return True

    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(agent_name="oracle", agent_model=None),
        driver_factory=driver_factory,
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir,
            trial_id=trial_id,
        ),
        verifier_factory=lambda: _Verifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=state_patch,
        workspace_staging_policy=WorkspaceStagingPolicy.from_provenance(
            TB21_AGENT_WORKSPACE_POLICY,
        ),
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert len(drivers) == 2
    assert PurePosixPath("/workspace/solution/solve.sh") in drivers[0].filesystem
    assert PurePosixPath("/workspace/tests/private-test.sh") not in drivers[0].filesystem
    assert PurePosixPath("/workspace/verifier/run.sh") not in drivers[0].filesystem
    assert PurePosixPath("/workspace/upstream-task.toml") not in drivers[0].filesystem


async def test_runner_starts_sidecars_and_uses_returned_network(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class CapturingDriver(FakeDriver):
        start_options: StartOptions | None = None

        async def start(self, *, options: StartOptions | None = None) -> None:
            events.append("driver-start")
            self.start_options = options
            await super().start(options=options)

        async def stop(self, *, delete: bool = True) -> None:
            events.append("driver-stop")
            await super().stop(delete=delete)

    class FakeSidecars:
        async def start(self, network_name: str | None = None) -> str:
            events.append(f"sidecars-start:{network_name}")
            return "loom-sidecar-network"

        async def stop(self) -> None:
            events.append("sidecars-stop")

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        return True

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
    driver = CapturingDriver(
        capabilities=_docker_supporting_caps(),
        exec_handler=handler,
    )
    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(
            sidecars=[TaskSidecarConfig(name="api", docker_image="api:latest")],
            environment={"API_URL": "http://api:8000"},
        ),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(verifier_env_mode="shared"),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        sidecar_runtime_factory=lambda: FakeSidecars(),
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert driver.start_options is not None
    assert driver.start_options.network == "loom-sidecar-network"
    assert driver.start_options.environment == (("API_URL", "http://api:8000"),)
    assert events == [
        "sidecars-start:None",
        "driver-start",
        "driver-stop",
        "sidecars-stop",
    ]


async def test_runner_swallows_state_patch_exception(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    """A transient PATCH failure must not crash the trial body. The runner
    logs the warning and lets Trial.run() continue."""
    calls: list[tuple[str, str | None]] = []

    async def flaky_patch(
        state: str,
        fr: str | None,
        failure_message: str | None = None,
    ) -> bool:
        calls.append((state, fr))
        if state == "running":
            raise RuntimeError("simulated network blip")
        return True

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
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=flaky_patch,
    )

    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED
    assert ("running", None) in calls
    assert ("succeeded", None) in calls


async def test_runner_marks_failed_when_trajectory_upload_cannot_start(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    """A missing trajectories bucket makes multipart upload creation fail.

    The worker-facing runner must convert that into a terminal failed
    state instead of letting the background task crash and leave the CP
    row stuck in running.
    """

    class MissingBucketStore(FakeObjectStore):
        async def create_multipart_upload(self, *, bucket: str, key: str):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

        async def put_object(self, *, bucket: str, key: str, body: bytes):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

    calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        calls.append((state, failure_reason))
        return True

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
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=MissingBucketStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TRAJECTORY_FLUSH_FAILED
    assert ("failed", FailureReason.TRAJECTORY_FLUSH_FAILED.value) in calls


async def test_runner_marks_failed_when_artifact_upload_fails(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    """Artifact persistence is part of platform success.

    The verifier may still return a reward from the sandbox workspace, but
    if a declared artifact cannot be persisted to object storage, the worker
    must not report the trial as succeeded.
    """

    class MissingArtifactBucketStore(FakeObjectStore):
        async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
            if bucket == "artifacts":
                raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")
            return await super().put_object(bucket=bucket, key=key, body=body)

    calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        calls.append((state, failure_reason))
        return True

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/result.txt\x00",
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0,
            stdout=b"hello\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.05,
        )

    trial_id = uuid4()
    driver = FakeDriver(exec_handler=handler)
    driver.filesystem[PurePosixPath("/workspace/result.txt")] = b"hello"
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(artifacts=["result.txt"]),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(verifier_env_mode="shared"),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=MissingArtifactBucketStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.ARTIFACT_UPLOAD_FAILED
    assert result.steps[0].error is not None
    assert result.steps[0].error.phase == "artifacts"
    assert ("failed", FailureReason.ARTIFACT_UPLOAD_FAILED.value) in calls
    assert ("succeeded", None) not in calls


async def test_runner_projects_successful_trial_outputs(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    state_calls: list[tuple[str, str | None]] = []
    projection_calls: list[dict[str, Any]] = []

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        state_calls.append((state, failure_reason))
        return True

    async def fake_output_projection(
        result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        projection_calls.append(
            {
                "result": result_payload,
                "trajectory_index": trajectory_index,
            }
        )
        return True

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/result.txt\x00",
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0,
            stdout=b"hello\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.05,
        )

    trial_id = uuid4()
    team_id = uuid4()
    driver = FakeDriver(exec_handler=handler)
    driver.filesystem[PurePosixPath("/workspace/result.txt")] = b"hello"
    class VersionedStore(FakeObjectStore):
        async def put_object_with_metadata(
            self,
            *,
            bucket: str,
            key: str,
            body: bytes,
            metadata=None,
            require_versioning: bool = False,
        ) -> object:
            assert metadata is None and not require_versioning
            uri = await super().put_object(bucket=bucket, key=key, body=body)
            version_id = (
                "artifact-version-789"
                if bucket == "artifacts"
                else "atif-version-456"
            )
            return SimpleNamespace(uri=uri, version_id=version_id)

        async def complete_multipart_upload_with_metadata(self, upload):  # type: ignore[no-untyped-def]
            uri = await super().complete_multipart_upload(upload)
            return SimpleNamespace(uri=uri, version_id="trajectory-version-123")

    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=team_id,
        task_config=_task_config(artifacts=["result.txt"]),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(verifier_env_mode="shared"),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=VersionedStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert ("succeeded", None) in state_calls
    assert len(projection_calls) == 1
    projection = projection_calls[0]
    assert projection["result"]["state"] == "succeeded"
    assert projection["result"]["aggregate_reward"] == 1.0
    assert projection["trajectory_index"]["trajectory_uri"] == result.trajectory_uri
    assert projection["trajectory_index"]["atif_uri"] == result.atif_uri
    assert projection["trajectory_index"]["trajectory_sha256"] == (
        result.trajectory_sha256
    )
    assert projection["trajectory_index"]["trajectory_size_bytes"] == (
        result.trajectory_size_bytes
    )
    assert projection["trajectory_index"]["atif_sha256"] == result.atif_sha256
    assert projection["trajectory_index"]["atif_size_bytes"] == (
        result.atif_size_bytes
    )
    assert projection["trajectory_index"]["trajectory_version_id"] == (
        "trajectory-version-123"
    )
    assert projection["trajectory_index"]["atif_version_id"] == (
        "atif-version-456"
    )
    assert isinstance(result.trajectory_sha256, str)
    assert len(result.trajectory_sha256) == 64
    assert result.trajectory_size_bytes is not None
    assert result.trajectory_size_bytes > 0
    assert isinstance(result.atif_sha256, str)
    assert len(result.atif_sha256) == 64
    assert result.atif_size_bytes is not None
    assert result.atif_size_bytes > 0
    assert projection["trajectory_index"]["artifacts"] == [
        {
            "step_name": "main",
            "bucket": "artifacts",
            "key": f"{team_id}/{trial_id}/main/result.txt",
            "size": 5,
            "content_hash": (
                "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e"
                "1b161e5c1fa7425e73043362938b9824"
            ),
            "version_id": "artifact-version-789",
            "share_status": "shared",
            "blocked_reason": None,
        }
    ]


async def test_runner_projects_failed_trial_outputs_before_terminal_state(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    projected: dict[str, Any] = {}

    class _FailingAgent:
        mode = "out-of-box"
        name = "failing"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_kwargs: Any) -> None:
            raise RuntimeError("intentional agent failure")

    async def fake_state_patch(
        state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        if state in {"succeeded", "failed", "cancelled"}:
            events.append(f"state:{state}")
        return True

    async def fake_output_projection(
        result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        events.append(f"projection:{result_payload['state']}")
        projected.update(trajectory_index)
        return True

    runner = LocalTrialRunner(
        trial_id=uuid4(),
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(agent_name="failing"),
        driver_factory=lambda: FakeDriver(),
        agent_factory=lambda *_args: _FailingAgent(),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert events == ["projection:failed", "state:failed"]
    assert projected["trajectory_uri"] == result.trajectory_uri
    assert projected["atif_uri"] == result.atif_uri
    assert projected["trajectory_sha256"] == result.trajectory_sha256
    assert projected["atif_sha256"] == result.atif_sha256


async def test_runner_projects_cancelled_trial_outputs_before_cancellation_propagates(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    agent_started = asyncio.Event()
    events: list[str] = []

    class _BlockingAgent:
        mode = "out-of-box"
        name = "blocking"
        version = "1"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_kwargs: Any) -> None:
            agent_started.set()
            await asyncio.Event().wait()

    async def fake_state_patch(
        state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        if state in {"succeeded", "failed", "cancelled"}:
            events.append(f"state:{state}")
        return True

    async def fake_output_projection(
        result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        events.append(f"projection:{result_payload['state']}")
        assert trajectory_index["trajectory_uri"] is not None
        assert trajectory_index["atif_uri"] is not None
        return True

    runner = LocalTrialRunner(
        trial_id=uuid4(),
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(agent_name="blocking"),
        driver_factory=lambda: FakeDriver(),
        agent_factory=lambda *_args: _BlockingAgent(),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    run_task = asyncio.create_task(runner.run())
    await agent_started.wait()
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert events == ["projection:cancelled", "state:cancelled"]


async def test_runner_does_not_report_success_when_output_projection_fails(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    state_calls: list[tuple[str, str | None, str | None]] = []
    projection_calls = 0

    async def fake_state_patch(
        state: str,
        failure_reason: str | None,
        failure_message: str | None = None,
    ) -> bool:
        state_calls.append((state, failure_reason, failure_message))
        return True

    async def fake_output_projection(
        result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        nonlocal projection_calls
        projection_calls += 1
        assert result_payload["aggregate_reward"] == 1.0
        assert trajectory_index["trajectory_uri"].endswith("/events.jsonl")
        return False

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/result.txt\x00",
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0,
            stdout=b"hello\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.05,
        )

    trial_id = uuid4()
    team_id = uuid4()
    driver = FakeDriver(exec_handler=handler)
    driver.filesystem[PurePosixPath("/workspace/result.txt")] = b"hello"
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=team_id,
        task_config=_task_config(artifacts=["result.txt"]),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(verifier_env_mode="shared"),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    result = await runner.run()

    assert projection_calls == 1
    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TRAJECTORY_FLUSH_FAILED
    assert not any(call[0] == "succeeded" for call in state_calls)
    assert not any(call[0] in {"succeeded", "failed", "cancelled"} for call in state_calls)


async def test_runner_does_not_project_uncommitted_trajectory_identity(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    projected: dict[str, Any] = {}

    class MissingBucketStore(FakeObjectStore):
        async def create_multipart_upload(self, *, bucket: str, key: str):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

        async def put_object(self, *, bucket: str, key: str, body: bytes):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

    async def fake_state_patch(
        _state: str,
        _failure_reason: str | None,
        _failure_message: str | None = None,
    ) -> bool:
        return True

    async def fake_output_projection(
        _result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        projected.update(trajectory_index)
        return True

    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=lambda: FakeDriver(),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=MissingBucketStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert projected["trajectory_uri"] is None
    assert projected["atif_uri"] is None


async def test_runner_logs_fenced_response(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
    caplog,
):
    async def fenced_patch(
        state: str,
        fr: str | None,
        failure_message: str | None = None,
    ) -> bool:
        return state != "running"  # Pretend `running` PATCH was fenced

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
    runner = LocalTrialRunner(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name: OracleAgent(
            task_dir=task_dir, trial_id=trial_id
        ),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fenced_patch,
    )

    import logging

    with caplog.at_level(logging.WARNING, logger="loom_worker.trial_runner"):
        await runner.run()
    assert any("state_patch_fenced" in m for m in caplog.messages)


async def test_trial_config_agent_and_model_drive_the_factory(  # type: ignore[no-untyped-def]
    hello_task,
    tmp_path: Path,
):
    """Plan 23: TrialConfig.agent_name + agent_model are required and
    used directly. The worker NEVER falls back to TaskConfig.agent.*
    for service-mode trials — every submission carries explicit
    agent + model identity."""
    from loom.models.types import ModelSpec

    captured: dict[str, Any] = {}

    def capture_factory(task_dir, _gw, model, name):  # type: ignore[no-untyped-def]
        captured["name"] = name
        captured["model"] = model
        return OracleAgent(task_dir=task_dir, trial_id=uuid4())

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
    explicit_model = ModelSpec(provider="anthropic", name="claude-opus-4-7")

    async def noop_patch(
        state: str,
        _fr: str | None,
        failure_message: str | None = None,
    ) -> bool:
        return True

    # Task says oracle/None; TrialConfig says claude-code/claude-opus-4-7
    # — the factory MUST see the TrialConfig values, not the task's.
    runner = LocalTrialRunner(
        trial_id=uuid4(),
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(
            agent_name="claude-code",
            agent_model=explicit_model,
        ),
        driver_factory=_driver_factory(handler),
        agent_factory=capture_factory,
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=noop_patch,
    )
    await runner.run()
    assert captured["name"] == "claude-code"
    assert captured["model"] == explicit_model


async def test_runner_applies_trial_request_params_to_agent(
    hello_task: Path,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    class RequestParamAgent:
        mode = "out-of-box"
        name = "litellm"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        def __init__(self) -> None:
            self.request_params: dict[str, Any] = {}

        async def run(self, **_: Any) -> None:
            captured["request_params"] = self.request_params

    def capture_factory(task_dir, _gw, model, name):  # type: ignore[no-untyped-def]
        return RequestParamAgent()

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

    async def noop_patch(
        state: str,
        _fr: str | None,
        failure_message: str | None = None,
    ) -> bool:
        return True

    runner = LocalTrialRunner(
        trial_id=uuid4(),
        team_id=uuid4(),
        task_config=_task_config(),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(
            agent_name="litellm",
            request_params={
                "temperature": 0,
                "top_p": 0.5,
                "seed": 1234,
                "messages": [{"role": "user", "content": "secret"}],
                "api_key": "sk-hidden",
                "extra_body": {"top_k": 40, "prompt": "secret"},
            },
        ),
        driver_factory=_driver_factory(handler),
        agent_factory=capture_factory,
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=noop_patch,
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert captured["request_params"] == {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {"top_k": 40},
    }
