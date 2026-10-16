"""LocalRunner produces a TrialResult for a solution-baseline oracle
trial against FakeDriver, and writes the trajectory to the local
object store + a friendly per-trial directory."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import pytest

from loom.driver.base import ExecHandle
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.models.exec import ExecResult
from loom.models.result import TrialState
from loom.models.task import TaskConfig
from loom.models.trial import MultiModelSwitchSpec, TrialConfig
from loom.models.types import ModelSpec
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner
from tests._trial_config_defaults import stub_trial_config


def _oracle_task_config(verifier: dict[str, Any]) -> TaskConfig:
    return TaskConfig.model_validate({
        "schema_version": "1",
        "task": {"id": "cli-test/echo", "name": "echo"},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": verifier,
        "steps": [{"name": "solve"}],
    })


def _runner(
    tmp_path: Path,
    verifier: dict[str, Any],
    driver: FakeDriver,
) -> LocalRunner:
    bucket_root = tmp_path / "store"
    bucket_root.mkdir()
    store = LocalDiskObjectStore(root=bucket_root)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\nexit 0\n")
    (task_dir / "solution" / "solve.sh").chmod(0o755)

    return LocalRunner(
        trial_id=uuid4(), team_id=uuid4(),
        task_config=_oracle_task_config(verifier),
        task_checksum="x" * 64,
        task_dir=task_dir,
        driver_factory=lambda: driver,
        output_dir=tmp_path / "runs",
        object_store=store,
        upstream_gateway_tokens={"anthropic": "x"},
        anthropic_client=None, openai_client=None, google_client=None,
        # These cases plant verifier evidence on one FakeDriver; a second
        # start() on that instance is rejected. Shared injects after refuse.
        trial_config=stub_trial_config(verifier_env_mode="shared"),
    )


_PASSING_JUNIT = b'<testsuites><testsuite><testcase name="smoke"/></testsuite></testsuites>'


@pytest.mark.asyncio
async def test_local_multimodel_fails_before_sandbox_start(tmp_path: Path) -> None:
    driver = FakeDriver()
    runner = _runner(tmp_path, {"name": "pytest"}, driver)
    model = ModelSpec(provider="anthropic", name="claude-model")
    runner.task_config = runner.task_config.model_copy(update={
        "agent": runner.task_config.agent.model_copy(update={"name": "terminus-2", "model": model}),
    })
    runner.trial_config = TrialConfig(
        agent_name="terminus-2", agent_model=model,
        multi_model=MultiModelSwitchSpec(enabled=True, secondary_model=model),
    )
    with pytest.raises(AgentError, match="multi_model"):
        await runner.run()
    assert driver.state == "constructed"


@pytest.mark.asyncio
async def test_local_runner_rejects_gateway_only_controls_for_direct_codex(tmp_path: Path) -> None:
    captured: list[dict[str, str]] = []

    def capture(argv: list[str], env_vars: dict[str, str], cwd: PurePosixPath,
                user: str | int | None) -> ExecHandle:
        captured.append(env_vars)
        raise AgentError("stop before process launch")

    driver = FakeDriver(streaming_handler=capture)
    runner = _runner(tmp_path, {"name": "pytest"}, driver)
    model = ModelSpec(provider="openai", name="gpt-model")
    runner.task_config = runner.task_config.model_copy(update={
        "agent": runner.task_config.agent.model_copy(update={"name": "codex", "model": model}),
    })
    runner.upstream_gateway_tokens = {"openai": "synthetic-direct-key"}
    runner.trial_config = TrialConfig(
        agent_name="codex", agent_model=model, request_params={"temperature": 0.2, "api_key": "ignored"},
    )
    with pytest.raises(AgentError, match="request_params"):
        await runner.run()
    assert not captured
    assert driver.state == "constructed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("verifier", "command", "user", "output_path", "output", "rewards"),
    [
        pytest.param(
            {"name": "pytest"},
            "cd /workspace/tests && pytest --junitxml=/loom/verifier/junit.xml --maxfail=0 -q",
            None,
            "/loom/verifier/junit.xml",
            _PASSING_JUNIT,
            {"passed": 1.0, "pytest_pass_rate": 1.0},
            id="pytest-defaults",
        ),
        pytest.param(
            {"name": "pytest", "args": {
                "tests_dir": "/workspace/custom-tests",
                "pytest_args": ["-k", "smoke", "-q"],
                "user": "grader",
            }},
            "cd /workspace/custom-tests && pytest --junitxml=/loom/verifier/junit.xml -k smoke -q",
            "grader",
            "/loom/verifier/junit.xml",
            _PASSING_JUNIT,
            {"passed": 1.0, "pytest_pass_rate": 1.0},
            id="pytest-configured",
        ),
        pytest.param(
            {"name": "script", "args": {
                "script_path": "/workspace/verifier/grade.sh", "user": "grader",
            }},
            "sh /workspace/verifier/grade.sh",
            "grader",
            "/loom/verifier/output.json",
            b'{"rewards":{"score":0.75},"checks":[]}',
            {"score": 0.75},
            id="script-configured",
        ),
    ],
)
async def test_local_runner_grades_with_task_verifier(
    tmp_path: Path,
    verifier: dict[str, Any],
    command: str,
    user: str | None,
    output_path: str,
    output: bytes,
    rewards: dict[str, float],
) -> None:
    driver = FakeDriver()

    def execute(
        cmd: str,
        actual_user: str | int | None,
        cwd: PurePosixPath | None,
        env: Mapping[str, str] | None,
    ) -> ExecResult:
        # Grading evidence exists only if the runner executes the configured
        # command as the configured user. Trial and verifier code stay real.
        if cmd == command and actual_user == user:
            driver.filesystem[PurePosixPath(output_path)] = output
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.01)

    driver.exec_handler = execute
    runner = _runner(tmp_path, verifier, driver)
    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED, result.failure_message
    assert result.reward == rewards
    assert driver.state == "stopped"
    traj_dir = tmp_path / "runs" / str(runner.trial_id)
    assert traj_dir.exists()
    assert (traj_dir / "events.jsonl").exists()
    lines = (traj_dir / "events.jsonl").read_text().splitlines()
    kinds = [json.loads(line)["kind"] for line in lines if line.strip()]
    assert "trial_start" in kinds
    assert "trial_end" in kinds


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("verifier", "error", "message"),
    [
        ({"name": "unknown"}, ValueError, "unknown verifier"),
        ({"name": "script"}, TypeError, "script_path"),
        ({"name": "pytest", "args": {"invalid_option": True}}, TypeError, "invalid_option"),
    ],
)
async def test_local_runner_rejects_invalid_verifier_before_start(
    tmp_path: Path,
    verifier: dict[str, Any],
    error: type[Exception],
    message: str,
) -> None:
    driver = FakeDriver()
    runner = _runner(tmp_path, verifier, driver)

    with pytest.raises(error, match=message):
        await runner.run()

    assert driver.state == "constructed"
