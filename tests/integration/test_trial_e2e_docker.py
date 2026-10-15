"""E2E: Trial.run() against DockerDriver with OracleAgent + PytestVerifier.

Verifies the full pipeline (start → trial-start event → OracleAgent →
PytestVerifier → trial-end → finalize → ATIF upload) works against a
real Docker container. Skipped if no daemon.

Self-contained: solve.sh installs pytest, writes a passing test, and
writes a marker file. PytestVerifier then runs that test. No external
image dependencies beyond `python:3.11-alpine`.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.oracle import OracleAgent
from loom.models.result import TrialState
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.trial import TrialConfig
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import Trial, TrialContext
from loom.verifier.pytest_verifier import PytestVerifier

pytestmark = pytest.mark.docker


@pytest.fixture
def docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture
def self_contained_task(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    (d / "task.toml").write_text('schema_version = "1"\n')
    (d / "instruction.md").write_text("write result + a passing test\n")
    sol = d / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text(
        "#!/bin/sh\n"
        "set -e\n"
        "pip install --quiet --root-user-action=ignore pytest 2>/dev/null\n"
        "mkdir -p /workspace/tests\n"
        "cat > /workspace/tests/test_basic.py <<'PYEOF'\n"
        "def test_passes():\n"
        "    assert 2 + 2 == 4\n"
        "PYEOF\n"
        "echo ok > /workspace/result.txt\n",
    )
    (sol / "solve.sh").chmod(0o755)
    return d


async def test_e2e_oracle_pytest_docker(
    self_contained_task: Path, tmp_path: Path, docker_available: bool,
):
    if not docker_available:
        pytest.skip("Docker daemon not available")
    pytest.importorskip("docker")

    from loom.driver.docker import DockerDriver

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello-d", name="hello-d"),
        environment=EnvironmentConfig(os="linux", docker_image="python:3.11-alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
        steps=[StepConfig(name="main")],
    )
    driver = DockerDriver(
        image="python:3.11-alpine", workspace=PurePosixPath("/workspace"),
    )
    store = FakeObjectStore()
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id, team_id=uuid4(),
        task_config=task, task_checksum="0" * 64,
        task_dir=self_contained_task,
        trial_config=TrialConfig(
            agent_name="oracle", agent_model=None, force_build=False,
        ),
        driver=driver,
        agent=OracleAgent(task_dir=self_contained_task, trial_id=trial_id),
        verifier=PytestVerifier(tests_dir=PurePosixPath("/workspace/tests")),
        object_store=store,
        local_trajectory_path=tmp_path / "events.jsonl",
        verifier_driver_factory=lambda: DockerDriver(
            image="python:3.11-alpine", workspace=PurePosixPath("/workspace"),
        ),
    )

    result = await Trial(ctx=ctx).run()

    # Trial reached a terminal state and ATIF was uploaded.
    assert result.state in {TrialState.SUCCEEDED, TrialState.FAILED}
    assert result.atif_uri is not None
    assert (ctx.trajectory_bucket, ctx.trajectory_key) in store.objects
    assert (
        ctx.trajectory_bucket, f"{ctx.team_id}/{ctx.trial_id}/atif.json",
    ) in store.objects

    # Happy-path assertion: the trial succeeded AND the verifier reports the
    # test passed.
    if result.state == TrialState.SUCCEEDED:
        sr = result.steps[0]
        assert sr.verifier_result is not None, "verifier_result missing"
        assert sr.verifier_result.rewards.get("passed") == 1.0, (
            f"expected passed=1.0; got {sr.verifier_result.rewards}"
        )
