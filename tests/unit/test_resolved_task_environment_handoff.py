"""The frozen resolved OCI environment reaches every trusted controller phase."""

from pathlib import PurePosixPath
from uuid import uuid4

import pytest
import tomli_w

from loom.execution_runtime_contract import RuntimeHandoffInputV1
from loom.hosted_harness import HARNESS_SETUP_PHASE
from loom.models.task import EnvironmentConfig
from loom.service_execution_materialization import (
    compile_deferred_verifier_plan,
    compile_service_execution_plan,
)
from loom.task_image_materialization import TaskImageExecutionGrantV1
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_KEY = "LOOM_TASK_ENVIRONMENT_JSON"


def _prepared(*, explicit=False, oversized=False):
    task, trial, profile = _inputs()
    environment = task.environment.model_copy(update={
        "dockerfile": PurePosixPath("environment/Dockerfile"), "docker_image": None,
        "user": "root" if explicit else "image", "image_workdir": True, "image_entrypoint": True,
        "environment": {"HOME": "/custom/root", "KEPT": "authored"} if explicit else {},
    })
    task = task.model_copy(update={"environment": environment})
    image = profile.task_image_ref
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="1" * 64, cpu_arch="x86_64", task_checksum=_REVISION.removeprefix("sha256:"),
        task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance=_provenance(),
        registry_images={"task": image}, image_runtime_metadata={"task": {
            "image_ref": image, "user": "worker", "uid": 1001, "gid": 1002, "home": "/home/worker",
            "workdir": "/app/resolved", "entrypoint": ["/entrypoint.sh"],
            "environment": {"PASSWORD": "diagnostic-fixture-value", "KEPT": "image",
                            **({"LARGE": "x" * 4000} if oversized else {})},
        }},
    )
    profile = profile.model_copy(update={"supports_task_identity": True})
    return task, trial, profile, grant


def _compile(task, trial, profile, grant):
    return compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION, task_image_grant=grant)


@pytest.mark.parametrize("explicit", [False, True])
def test_plan_handoff_and_deferred_verifier_share_frozen_environment(explicit):
    task, trial, profile, grant = _prepared(explicit=explicit)
    original = task.model_dump_json()
    plan = _compile(task, trial, profile, grant)
    resolved = EnvironmentConfig.model_validate_json(plan.main.environment[_KEY])
    assert str(resolved.workdir) == "/app/resolved"
    assert resolved.user == ("root" if explicit else "1001:1002")
    assert resolved.environment["HOME"] == ("/custom/root" if explicit else "/home/worker")
    assert resolved.entrypoint == ("/entrypoint.sh",)
    assert resolved.environment["PASSWORD"] == "diagnostic-fixture-value"
    assert "PASSWORD" not in plan.main.environment
    assert next(s for s in plan.sidecars if s.private_sandbox).environment["PASSWORD"] == "diagnostic-fixture-value"
    for phase in (*plan.setup, *((plan.verifier,) if plan.verifier else ())):
        assert phase.environment[_KEY] == plan.main.environment[_KEY]
    deferred = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256="sha256:" + "a" * 64, file_count=1, total_bytes=1))
    assert deferred.main.environment[_KEY] == plan.main.environment[_KEY]
    sandbox = next(s for s in deferred.sidecars if s.private_sandbox)
    assert sandbox.identity.run_as_user == (0 if explicit else 1001)
    assert sandbox.environment["PASSWORD"] == "diagnostic-fixture-value"
    assert task.model_dump_json() == original


def test_deferred_verifier_keeps_explicit_verifier_identity_override():
    task, trial, profile, grant = _prepared()
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"user": "root"})})
    grant = grant.model_copy(update={"task_config": task.model_dump(mode="json")})
    plan = _compile(task, trial, profile, grant)
    deferred = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256="sha256:" + "a" * 64, file_count=1, total_bytes=1))
    assert next(s for s in plan.sidecars if s.private_sandbox).identity.run_as_user == 1001
    assert next(s for s in deferred.sidecars if s.private_sandbox).identity.run_as_user == 0


def test_legacy_plan_has_no_new_environment_contract():
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert _KEY not in plan.main.environment
    assert all(_KEY not in phase.environment for phase in plan.setup)


@pytest.mark.parametrize("raw", ["{broken", '{"os":"linux","unknown":"private-value"}',
    '{"os":"linux","user":"image"}', '{"os":"linux","environment":{"LOOM_GATEWAY_TOKEN":"private-value"}}'])
def test_invalid_environment_handoff_is_sanitized(raw):
    from loom.task_environment_handoff import decode_resolved_task_environment
    with pytest.raises(ValueError, match=r"^resolved_task_environment_invalid$") as caught:
        decode_resolved_task_environment(raw)
    assert caught.value.__cause__ is None


def test_private_fixture_environment_is_preserved_in_resolved_handoff():
    from loom.task_environment_handoff import decode_resolved_task_environment

    environment = decode_resolved_task_environment(
        '{"os":"linux","environment":{"API_KEY":"fixture-only"}}')
    assert environment.environment == {"API_KEY": "fixture-only"}


@pytest.mark.parametrize("phase", [HARNESS_SETUP_PHASE, "terminus-2", "verify-sandbox"])
def test_real_phase_entrypoint_uses_resolved_environment_and_keeps_source_unchanged(tmp_path, monkeypatch, phase):
    from loom import service_execution_sandbox_task as module

    task, trial, profile, grant = _prepared()
    plan = _compile(task, trial, profile, grant)
    source = tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)).encode()
    (tmp_path / "task.toml").write_bytes(source)
    monkeypatch.setenv("LOOM_TASK_TRIAL_JSON", trial.model_dump_json())
    from loom.task_image_materialization import resolve_prepared_task
    resolved = resolve_prepared_task(task, grant).environment
    monkeypatch.setenv(_KEY, plan.main.environment[_KEY])
    monkeypatch.setattr("sys.argv", ["sandbox-task", phase, "--workspace", str(tmp_path)])
    observed = []
    async def capture(workspace, received, received_trial, **kwargs):
        observed.append(received)
        if phase == "terminus-2":
            assert kwargs["original_task"].environment.dockerfile == task.environment.dockerfile
            assert kwargs["original_task"].environment.user == "image"
    target = "run_setup" if phase == HARNESS_SETUP_PHASE else "run_verifier" if phase == "verify-sandbox" else "run_agent"
    monkeypatch.setattr(module, target, capture)
    module.main()
    assert observed[0].environment == resolved
    assert observed[0].steps == task.steps
    assert (tmp_path / "task.toml").read_bytes() == source


@pytest.mark.asyncio
async def test_resolved_agent_upload_excludes_frozen_original_build_inputs(tmp_path, monkeypatch):
    from loom import service_execution_sandbox_task as module
    from loom.task_image_materialization import resolve_prepared_task
    from tests.unit.test_service_execution_sandbox_task import Sandbox

    task, trial, _, grant = _prepared()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "docker_build_context": PurePosixPath("environment"),
    })})
    grant = grant.model_copy(update={"task_config": task.model_dump(mode="json")})
    resolved = resolve_prepared_task(task, grant)
    assert resolved.environment.docker_build_context is None
    for name, contents in {"instruction.md": "work", "runtime.txt": "runtime input",
            "environment/Dockerfile": "FROM image", "environment/private-fixture": "build-only",
            "tests/test.py": "private grading", "solution/solve.sh": "private solution"}.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
    driver = Sandbox()
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    async def identity(_):
        return uuid4(), uuid4()
    async def terminus(**kwargs):
        assert kwargs["task_config"].environment.user == "1001:1002"
        assert kwargs["task_config"].environment.workdir == PurePosixPath("/app/resolved")
        assert not any(str(path).startswith("/app/resolved/environment/") for path in driver.filesystem)
        assert PurePosixPath("/app/resolved/tests/test.py") not in driver.filesystem
        assert PurePosixPath("/app/resolved/solution/solve.sh") not in driver.filesystem
        assert driver.filesystem[PurePosixPath("/app/resolved/runtime.txt")] == b"runtime input"
        # A task-controlled replacement cannot alter the original build hints
        # already held by the trusted controller.
        driver.filesystem[PurePosixPath("/app/resolved/task.toml")] = b'[environment]\ndocker_build_context="."\n'
        assert "environment/**" in module._agent_input_exclusions(task)
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    await module.run_agent(tmp_path, resolved, trial, original_task=task)
    assert (tmp_path / "environment/private-fixture").read_bytes() == b"build-only"


def test_resolved_environment_requires_qualified_current_controller(monkeypatch):
    from loom import service_execution_materialization as module
    task, trial, profile, grant = _prepared()
    monkeypatch.setattr(module, "controller_image_for_trial", lambda *_: "registry.example/old@sha256:" + "a" * 64)
    with pytest.raises(ValueError, match="resolved_task_environment_controller_unavailable"):
        _compile(task, trial, profile, grant)


def test_oversized_resolved_environment_fails_before_launch():
    task, trial, profile, grant = _prepared(oversized=True)
    with pytest.raises(ValueError, match="resolved_task_environment_too_large"):
        _compile(task, trial, profile, grant)


@pytest.mark.parametrize("flag", ["image_entrypoint", "image_workdir"])
def test_unprepared_image_defaults_require_metadata(flag):
    from loom.service_execution_materialization import automatic_service_execution_rejections
    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={flag: True})})
    assert "task_image_runtime_metadata_required" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())


@pytest.mark.parametrize("environment", [
    {"LOOM_CONTROL": "fixture"}, {"HTTP_PROXY": "fixture"}, {"HTTPS_PROXY": "fixture"},
    {"ALL_PROXY": "fixture"}, {"NO_PROXY": "fixture"}, {"KUBECONFIG": "fixture"},
    {"invalid-name": "fixture"}, {"FIXTURE": "nul\x00value"}, {"FIXTURE": "x" * 4097},
    {f"FIXTURE_{i}": "value" for i in range(63)},
], ids=["platform-control", "http-proxy", "https-proxy", "all-proxy", "no-proxy",
        "kubeconfig", "invalid-name", "nul", "overlong", "merged-entry-count"])
def test_prepared_task_admission_keeps_environment_bounds(environment):
    from loom.service_execution_materialization import automatic_service_execution_rejections
    from loom.task_image_materialization import resolve_prepared_task

    task, trial, _, grant = _prepared()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"environment": environment},
    )})
    grant = grant.model_copy(update={"task_config": task.model_dump(mode="json")})
    resolved = resolve_prepared_task(task, grant)
    assert "task_environment_invalid" in automatic_service_execution_rejections(
        resolved, trial, source_provenance=_provenance(),
    )
