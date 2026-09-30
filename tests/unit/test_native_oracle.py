"""#2054: Oracle runs on native execution through the private-sandbox
controller, with no model, and only Oracle's sandbox sees `solution/`."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.errors import AgentError
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.exec import ExecResult
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_deferred_verifier_plan,
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from loom.service_execution_oracle import (
    oracle_usage,
    parse_oracle_events,
    remove_oracle_solution,
    run_oracle,
)
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_events,
    validate_usage_accounting,
)
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _RUNTIME_IMAGE,
    _provenance,
    _task,
)
from tests.unit.test_service_execution_terminus_plan import _events, _inputs


def _oracle(**updates: object) -> TrialConfig:
    return TrialConfig.model_validate({"agent_name": "oracle", "agent_model": None, **updates})


def _plan(trial: TrialConfig | None = None):
    task, _, profile = _inputs()
    return task, compile_service_execution_plan(
        task=task, trial=trial or _oracle(), profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )


# --- admission --------------------------------------------------------------


def test_model_free_oracle_is_admitted_on_a_task_image() -> None:
    task, _, profile = _inputs()

    assert automatic_service_execution_rejections(task, _oracle(), source_provenance=_provenance()) == ()
    assert runtime_profile_rejections(task, _oracle(), profile) == ()


def test_oracle_with_model_fields_is_rejected() -> None:
    task, _, _ = _inputs()
    with_model = _oracle(agent_model=ModelSpec(provider="openai", name="gpt-5"))
    with_params = _oracle(request_params={"temperature": 0})

    for trial in (with_model, with_params):
        reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
        assert "oracle_model_forbidden" in reasons
        assert "api_model_required" not in reasons


def test_oracle_needs_a_task_image_not_the_runner_image() -> None:
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"docker_image": None})})

    reasons = automatic_service_execution_rejections(task, _oracle(), source_provenance=_provenance())

    assert "immutable_task_image_required" in reasons


def test_oracle_may_use_a_prepared_dockerfile_like_terminus() -> None:
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"docker_image": None, "dockerfile": PurePosixPath("Dockerfile")},
    )})

    reasons = automatic_service_execution_rejections(
        task, _oracle(), source_provenance=_provenance(), allow_task_image_preparation=True,
    )

    assert "immutable_task_image_required" not in reasons


def test_oracle_keeps_harness_only_limits() -> None:
    task, _, _ = _inputs()
    continuing = task.model_copy(update={"agent": task.agent.model_copy(update={"continue_until_timeout": True})})

    reasons = automatic_service_execution_rejections(continuing, _oracle(), source_provenance=_provenance())
    assert "agent_continuation_unsupported" in reasons
    assert runtime_profile_rejections(task, _oracle(agent_version="1.0"), _inputs()[2]) == (
        "agent_version_not_in_runtime_profile",
    )


# --- plan --------------------------------------------------------------------


def test_oracle_plan_runs_the_oracle_phase_in_a_private_sandbox() -> None:
    task, plan = _plan()

    assert plan.main.argv[4] == "oracle"
    assert json.loads(plan.main.environment["LOOM_TASK_TRIAL_JSON"])["agent_name"] == "oracle"
    assert "LOOM_TASK_MODEL" not in plan.main.environment
    assert plan.task_image_ref == task.environment.docker_image
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox"]
    assert all(s.private_sandbox for s in plan.sidecars)
    paths = {item.relative_path for item in plan.output_declarations}
    assert {"trajectory/events.jsonl", "accounting/usage.json", "artifacts/workspace.tar"} <= paths
    assert not any(path.startswith("artifacts/harbor/") for path in paths)


def test_oracle_preserves_separate_and_shared_verifier_modes() -> None:
    task, separate = _plan()
    assert separate.verifier_execution == "separate_execution"
    verifier = compile_deferred_verifier_plan(separate, task, verifier_timeout_seconds=60)
    assert verifier.main.argv[4] == "verify-sandbox"

    _, shared = _plan(_oracle(verifier_env_mode="shared"))
    assert shared.verifier_execution == "in_attempt"
    assert shared.in_place_verifier is True
    assert shared.verifier is not None and shared.verifier.argv[4] == "verify-sandbox"


def test_oracle_and_terminus_commands_differ() -> None:
    _, oracle = _plan()
    _, terminus = _plan(_inputs()[1])

    assert oracle.command_identity_sha256 != terminus.command_identity_sha256


# --- controller ----------------------------------------------------------------


class _Driver:
    def __init__(self, return_code: int = 0) -> None:
        self.return_code = return_code
        self.commands: list[tuple[str, PurePosixPath | None]] = []

    async def exec(self, cmd: str, *, cwd: PurePosixPath | None = None, **_: object) -> ExecResult:
        self.commands.append((cmd, cwd))
        return ExecResult(return_code=self.return_code, stdout=b"ok", stderr=b"", duration_sec=0.5)


def _task_dir(tmp_path: Path) -> Path:
    solve = tmp_path / "task" / "solution" / "solve.sh"
    solve.parent.mkdir(parents=True)
    solve.write_text("#!/bin/sh\necho done\n")
    return tmp_path / "task"


async def test_run_oracle_executes_solve_and_records_one_event(tmp_path: Path) -> None:
    driver, trial_id = _Driver(), uuid4()
    task, _, _ = _inputs()

    await run_oracle(
        driver=driver,  # type: ignore[arg-type]
        task_dir=_task_dir(tmp_path), workspace=tmp_path / "out", task_config=task,
        trial_config=_oracle(), trial_id=trial_id,
    )

    workdir = task.environment.workdir
    assert driver.commands == [(
        f"chmod +x {workdir}/solution/solve.sh && {workdir}/solution/solve.sh", workdir / "solution",
    )]
    events = parse_oracle_events((tmp_path / "out" / "trajectory.jsonl").read_bytes(), trial_id=trial_id)
    assert [(e.kind, e.step_id, e.seq, e.return_code) for e in events] == [("env_exec", "agent", 0, 0)]


async def test_run_oracle_fails_on_missing_solution_or_nonzero_exit(tmp_path: Path) -> None:
    task, _, _ = _inputs()
    (tmp_path / "empty").mkdir()
    with pytest.raises(AgentError, match="requires"):
        await run_oracle(
            driver=_Driver(),  # type: ignore[arg-type]
            task_dir=tmp_path / "empty", workspace=tmp_path / "a", task_config=task,
            trial_config=_oracle(), trial_id=uuid4(),
        )
    with pytest.raises(AgentError, match="rc=3"):
        await run_oracle(
            driver=_Driver(return_code=3),  # type: ignore[arg-type]
            task_dir=_task_dir(tmp_path), workspace=tmp_path / "b", task_config=task,
            trial_config=_oracle(), trial_id=uuid4(),
        )


async def test_run_oracle_refuses_a_model(tmp_path: Path) -> None:
    task, _, _ = _inputs()
    with pytest.raises(AgentError, match="no model"):
        await run_oracle(
            driver=_Driver(),  # type: ignore[arg-type]
            task_dir=_task_dir(tmp_path), workspace=tmp_path / "out", task_config=task,
            trial_config=_oracle(agent_model=ModelSpec(provider="openai", name="gpt-5")),
            trial_id=uuid4(),
        )


async def test_solution_is_removed_from_the_sandbox() -> None:
    driver = _Driver()

    await remove_oracle_solution(driver, PurePosixPath("/app"))  # type: ignore[arg-type]

    assert driver.commands == [("rm -rf -- /app/solution", PurePosixPath("/app"))]
    with pytest.raises(AgentError, match="could not be removed"):
        await remove_oracle_solution(_Driver(return_code=1), PurePosixPath("/app"))  # type: ignore[arg-type]


def test_controller_rejects_a_phase_for_another_agent(tmp_path: Path, monkeypatch) -> None:
    from loom.service_execution_sandbox_task import ServiceExecutionTaskError, main

    (tmp_path / "task.toml").write_text(_task_toml())
    monkeypatch.setenv("LOOM_TASK_TRIAL_JSON", _oracle().model_dump_json())
    monkeypatch.setattr("sys.argv", ["x", "terminus-2", "--workspace", str(tmp_path)])

    with pytest.raises(ServiceExecutionTaskError, match="does not match"):
        main()


def _task_toml() -> str:
    return """schema_version = "1"
[task]
id = "task-1"
name = "t"
[environment]
os = "linux"
cpu_arch = "x86_64"
gpu_vendor = "none"
network_policies_supported = ["gateway-only"]
[environment.baseline_network_policy]
kind = "gateway-only"
[agent]
name = "oracle"
[verifier]
name = "script"
[verifier.args]
script_path = "verifier/check.sh"
[[steps]]
name = "main"
instruction_file = "instruction.md"
"""


# --- materialization -----------------------------------------------------------


def _trace(trial_id):
    from loom.models.trajectory import EnvExecEvent

    event = EnvExecEvent(
        emitted_at=datetime.now(UTC), trial_id=trial_id, step_id="agent", seq=0,
        cmd="solve", user=None, cwd="/app/solution", return_code=0, stdout_bytes=2,
        stderr_bytes=0, truncated=False, duration_sec=0.5,
    )
    return event, event.model_dump_json().encode() + b"\n"


def _result(event, status: str = "succeeded") -> ExecutionRuntimeResultV1:
    task, _, _ = _inputs()
    return ExecutionRuntimeResultV1.model_validate({
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": task.environment.docker_image, "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "3" * 64,
        "execution_class_id": "linux-amd64-cpu-pod-v1", "status": status,
        "started_at": event.emitted_at, "finished_at": event.emitted_at,
        "phases": [], "outputs": [], "verifier_rewards": {"passed": 1} if status == "succeeded" else None,
        "partial_evidence": status != "succeeded",
    })


def test_oracle_usage_is_known_zero_and_bound_to_a_real_solver_run() -> None:
    _, body = _trace(uuid4())
    usage = json.dumps(oracle_usage()).encode()

    validate_usage_accounting(trace_body=body, usage_body=usage, trial_config=_oracle())
    with pytest.raises(MaterializationIntegrityError, match="trajectory_invalid"):
        validate_usage_accounting(trace_body=b"", usage_body=usage, trial_config=_oracle())
    drift = json.dumps({**oracle_usage(), "call_count": 1}).encode()
    with pytest.raises(MaterializationIntegrityError, match="usage_output_identity_drift"):
        validate_usage_accounting(trace_body=body, usage_body=drift, trial_config=_oracle())


def test_oracle_trace_may_only_hold_solver_execution() -> None:
    _, _, terminus_events = _events()
    body = b"\n".join(e.model_dump_json().encode() for e in terminus_events) + b"\n"

    with pytest.raises(ValueError, match="solver execution"):
        parse_oracle_events(body)
    _, oracle_body = _trace(uuid4())
    with pytest.raises(ValueError, match="another Trial"):
        parse_oracle_events(oracle_body, trial_id=uuid4())


def test_canonical_oracle_events_carry_the_solver_run() -> None:
    trial_id = uuid4()
    event, body = _trace(trial_id)
    task, _, _ = _inputs()

    canonical = build_canonical_events(
        trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_oracle(),
        runtime_result=_result(event), trace_body=body, verifier_body=b'{"rewards":{"passed":1}}',
        gateway_calls=[],
    )

    assert canonical[2].kind == "env_exec"
    assert canonical[3].summary == {"llm_calls": 0.0}
    assert canonical[-1].final_state == "succeeded"
    assert [e.seq for e in canonical] == list(range(len(canonical)))


def test_gateway_calls_during_oracle_fail_materialization() -> None:
    trial_id = uuid4()
    event, body = _trace(trial_id)
    task, _, _ = _inputs()

    with pytest.raises(MaterializationIntegrityError, match="oracle_model_calls_present"):
        build_canonical_events(
            trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_oracle(),
            runtime_result=_result(event), trace_body=body, verifier_body=b'{"rewards":{"passed":1}}',
            gateway_calls=[{"gateway_request_id": "x"}],
        )


def test_failed_oracle_keeps_an_empty_trace_honest() -> None:
    trial_id = uuid4()
    event, _ = _trace(trial_id)
    task, _, _ = _inputs()

    canonical = build_canonical_events(
        trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_oracle(),
        runtime_result=_result(event, "task_error"), trace_body=b"", verifier_body=None,
        gateway_calls=[],
    )

    assert canonical[-1].final_state == "failed"


# --- workspace acceptance fixture ------------------------------------------------


def _workspace_fixture(tmp_path: Path) -> tuple[Path, TaskConfig]:
    import io
    import tarfile
    import tomllib

    from loom.agent_model_acceptance_taskset import build_workspace_taskset

    build_workspace_taskset(output_dir=tmp_path / "ts")
    with tarfile.open(fileobj=io.BytesIO((tmp_path / "ts" / "bundle.tar.gz").read_bytes())) as archive:
        archive.extractall(tmp_path / "bundle", filter="data")
    root = tmp_path / "bundle" / "tasks" / "workspace-csv-summary"
    return root, TaskConfig.model_validate(tomllib.loads((root / "task.toml").read_text()))


@pytest.mark.parametrize("agent_name", ["oracle", "terminus-2"])
def test_workspace_fixture_is_admitted_with_a_prepared_image(tmp_path: Path, agent_name: str) -> None:
    from loom.agent_model_acceptance_taskset import WORKSPACE_TASK_ID

    _, task = _workspace_fixture(tmp_path)
    trial = _oracle() if agent_name == "oracle" else _inputs()[1]

    assert task.task.id == WORKSPACE_TASK_ID
    assert task.steps[0].required_artifacts == ["summarize.py", "reports/totals.json"]
    assert automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance(), allow_task_image_preparation=True,
    ) == ()


def test_workspace_fixture_hides_solution_and_verifier_from_model_agents(tmp_path: Path) -> None:
    from fnmatch import fnmatchcase

    from loom.service_execution_sandbox_task import _POLICY, _agent_input_exclusions

    root, task = _workspace_fixture(tmp_path)
    files = [path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()]
    staged = {
        name for name in files
        if not _POLICY.is_private(PurePosixPath(name))
        and not any(fnmatchcase(name, pattern) for pattern in _agent_input_exclusions(task))
    }

    assert staged == {"task.toml", "instruction.md", "data/items.csv"}


def test_workspace_reference_solution_passes_its_verifier(tmp_path: Path) -> None:
    import subprocess

    root, _ = _workspace_fixture(tmp_path)

    def verify(name: str) -> dict[str, float]:
        out = tmp_path / f"{name}.json"
        subprocess.run(["/bin/sh", "verifier/check.sh"], cwd=root, check=True,
                       env={"LOOM_VERIFIER_OUTPUT": str(out), "PATH": "/usr/bin:/bin"})
        return json.loads(out.read_text())["rewards"]

    assert verify("before") == {"report": 0.0, "reproduced": 0.0}
    subprocess.run(["/bin/sh", str(root / "solution" / "solve.sh")], cwd=root / "solution", check=True)
    assert verify("after") == {"report": 1.0, "reproduced": 1.0}
    (root / "reports" / "totals.json").write_text('{"apple": 9.0, "pear": 8}')
    assert verify("float") == {"report": 0.0, "reproduced": 1.0}
