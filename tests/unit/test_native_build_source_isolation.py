"""Native task image build inputs must not reappear as agent runtime inputs."""

import tomllib
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.models.exec import ExecResult
from loom.models.task import TaskConfig, TaskSidecarConfig
from loom.service_execution_sandbox_task import (
    _POLICY,
    _agent_input_exclusions,
    main,
    run_agent,
    run_verifier,
)
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task
from loom.trial.workspace import materialize_workspace
from tests.unit.test_service_execution_sandbox_task import Sandbox
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("context", ["environment", "docker/build-inputs", "docker/build[private]"])
async def test_dedicated_build_context_stays_out_of_agent_with_private_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, context: str,
):
    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "docker_image": None, "dockerfile": PurePosixPath(context) / "Dockerfile",
        "docker_build_context": PurePosixPath(context),
    })})
    for name, content in {
        "instruction.md": "Produce answer.txt", "runtime.txt": "ordinary runtime asset",
        "tests/check.py": "trusted assertion", "verifier/check.sh": "trusted verifier",
        f"{context}/Dockerfile": "FROM image\nCOPY setup.sh /app/\nRUN /app/setup.sh && rm /app/setup.sh\n",
        f"{context}/setup.sh": "private fixture generation",
        f"{context}/tests/check.py": "duplicate trusted assertion",
        "Dockerfile.notes.txt": "ordinary similarly named asset",
    }.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    agent, verifier = Sandbox(), Sandbox()
    agent.filesystem[PurePosixPath("/app/repo/.git/HEAD")] = b"image-provided git data"
    agent.filesystem[PurePosixPath("/app/fixture.txt")] = b"baked fixture"
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver",
                        lambda role, task, trial: agent if role == "task-sandbox" else verifier)

    async def identity(gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        prefix = f"/app/{context}/"
        assert not any(str(path).startswith(prefix) for path in agent.filesystem)
        assert agent.filesystem[PurePosixPath("/app/runtime.txt")] == b"ordinary runtime asset"
        assert agent.filesystem[PurePosixPath("/app/Dockerfile.notes.txt")] == b"ordinary similarly named asset"
        assert PurePosixPath("/app/tests/check.py") not in agent.filesystem
        agent.filesystem[PurePosixPath("/app/answer.txt")] = b"42"
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    await run_agent(tmp_path, task, trial)

    def check(cmd, user, cwd, env):
        assert cwd == PurePosixPath("/app") and user is None
        assert verifier.filesystem[PurePosixPath("/app/tests/check.py")] == b"trusted assertion"
        assert verifier.filesystem[PurePosixPath("/app/answer.txt")] == b"42"
        assert verifier.filesystem[PurePosixPath("/app/fixture.txt")] == b"baked fixture"
        assert verifier.filesystem[PurePosixPath("/app/repo/.git/HEAD")] == b"image-provided git data"
        assert not any(str(path).startswith(f"/app/{context}/") for path in verifier.filesystem)
        verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    verifier.exec_handler = check
    await run_verifier(tmp_path, task, trial)
    assert (tmp_path / context / "setup.sh").read_text() == "private fixture generation"


@pytest.mark.parametrize("kind", ["root-context", "default-context", "image-only"])
async def test_ambiguous_root_and_image_only_assets_are_preserved(tmp_path: Path, kind: str):
    task, _, _ = _inputs()
    if kind != "image-only":
        task = task.model_copy(update={"environment": task.environment.model_copy(update={
            "docker_image": None, "dockerfile": PurePosixPath("Dockerfile"),
            "docker_build_context": PurePosixPath(".") if kind == "root-context" else None,
        })})
    for name in ("Dockerfile", "runtime.txt", "environment/Dockerfile", "environment/runtime.txt", "tests/check.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    driver = Sandbox()
    await driver.start()
    await materialize_workspace(driver=driver, task_dir=tmp_path, dst=PurePosixPath("/app"),
                                policy=_POLICY, excluded_paths=_agent_input_exclusions(task))
    await driver.stop()
    assert PurePosixPath("/app/runtime.txt") in driver.filesystem
    assert PurePosixPath("/app/environment/runtime.txt") in driver.filesystem
    assert PurePosixPath("/app/environment/Dockerfile") in driver.filesystem
    assert (PurePosixPath("/app/Dockerfile") in driver.filesystem) == (kind == "image-only")
    assert PurePosixPath("/app/tests/check.py") not in driver.filesystem


def test_native_entrypoint_uses_frozen_build_config_after_image_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    original = '''schema_version = "1"
[task]
id = "test/build-input-isolation"
name = "Build input isolation"
[environment]
os = "linux"
cpu_arch = "x86_64"
dockerfile = "environment/Dockerfile"
docker_build_context = "environment"
workdir = "/app"
[agent]
name = "terminus-2"
[verifier]
name = "script"
[verifier.args]
script_path = "verifier/check.sh"
[[steps]]
name = "main"
instruction_file = "instruction.md"
'''
    task = TaskConfig.model_validate(tomllib.loads(original))
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="a" * 64, cpu_arch="x86_64", task_checksum="b" * 64,
        task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance={},
        registry_images={"task": "registry.example/task@sha256:" + "c" * 64},
    )
    resolved = resolve_prepared_task(task, grant)
    assert resolved.environment.dockerfile is None
    assert resolved.environment.docker_build_context is None
    (tmp_path / "task.toml").write_text(original)
    (tmp_path / "instruction.md").write_text("Produce an answer")
    (tmp_path / "environment/tests").mkdir(parents=True)
    (tmp_path / "environment/Dockerfile").write_text("FROM image")
    (tmp_path / "environment/tests/check.py").write_text("must stay private")
    driver = Sandbox()
    monkeypatch.setenv("LOOM_TASK_TRIAL_JSON", _inputs()[1].model_dump_json())
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr("sys.argv", ["native-task", "terminus-2", "--workspace", str(tmp_path)])
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver", lambda role, task, trial: driver)

    async def identity(gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert kwargs["task_config"].environment.docker_build_context == PurePosixPath("environment")
        assert not any(str(path).startswith("/app/environment/") for path in driver.filesystem)
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    main()
    assert driver.quiesced
    assert (tmp_path / "task.toml").read_text() == original


@pytest.mark.parametrize("context", ["fixtures/server", "fixtures/server[private]"])
async def test_fixture_build_source_never_reaches_agent(tmp_path: Path, context: str) -> None:
    task, _, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "sidecars": [TaskSidecarConfig(
            name="server", dockerfile=PurePosixPath("docker/fixture.Dockerfile"),
            docker_build_context=PurePosixPath(context), command=["python3", "/server.py"],
        )],
    })})
    for name, content in {
        f"{context}/server.py": "fixture response contains the answer",
        f"{context}/subdir/secret.txt": "build-only source",
        "docker/fixture.Dockerfile": "FROM python\nCOPY . /fixture/\n",
        "fixtures/runtime.txt": "ordinary task input",
        "docker/notes.txt": "ordinary task input",
        "tests/check.py": "private grading",
    }.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    driver = Sandbox()
    await driver.start()
    try:
        await materialize_workspace(driver=driver, task_dir=tmp_path, dst=PurePosixPath("/app"),
                                    policy=_POLICY, excluded_paths=_agent_input_exclusions(task))
    finally:
        await driver.stop()
    assert not any(str(path).startswith(f"/app/{context}/") for path in driver.filesystem)
    assert PurePosixPath("/app/docker/fixture.Dockerfile") not in driver.filesystem
    assert PurePosixPath("/app/tests/check.py") not in driver.filesystem
    assert driver.filesystem[PurePosixPath("/app/fixtures/runtime.txt")] == b"ordinary task input"
    assert driver.filesystem[PurePosixPath("/app/docker/notes.txt")] == b"ordinary task input"
    assert (tmp_path / context / "server.py").read_text() == "fixture response contains the answer"
