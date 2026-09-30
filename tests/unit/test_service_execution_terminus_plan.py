from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import PurePosixPath
from uuid import uuid4

import pytest

from loom.execution_contract import VerifierTopology, workload_requirements_from_task
from loom.execution_runtime_contract import (
    ExecutionRuntimeResultV1,
    validate_runtime_plan_requirements,
)
from loom.models.trajectory import LLMCallEvent, Terminus2UserPromptEvent
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
)
from loom.service_execution_terminus_trace import parse_terminus_events, terminus_usage
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_events,
    validate_usage_accounting,
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _RUNTIME_IMAGE,
    _TASK_IMAGE,
    _profile,
    _provenance,
    _task,
    _trial,
)

_CONTROLLER = "registry.example/worker@sha256:" + "9" * 64


def _inputs():
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"workdir": PurePosixPath("/app")},
    )})
    trial = _trial().model_copy(update={"agent_name": "terminus-2"})
    profile = _profile().model_copy(update={
        "agent_image_ref": _CONTROLLER,
        "image_admission": signed_image_admission_bundle((_TASK_IMAGE, _RUNTIME_IMAGE, _CONTROLLER)),
    })
    return task, trial, profile


def test_terminus_plan_preserves_task_environment_and_has_fresh_private_verifier():
    task, trial, profile = _inputs()
    assert not automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.agent_image_ref == _CONTROLLER
    assert plan.task_image_ref == task.environment.docker_image
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox"]
    assert plan.verifier_execution == "separate_execution"
    assert plan.verifier is None
    assert plan.verifier_after_agent_timeout is False


def test_deferred_verifier_plan_restores_committed_workspace():
    from loom.service_execution_materialization import compile_deferred_verifier_plan

    task, trial, profile = _inputs()
    agent = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    agent = agent.model_copy(update={
        "main": agent.main.model_copy(update={
            "argv": (*agent.main.argv[:4], "future-workspace-agent", *agent.main.argv[5:]),
        }),
    })
    assert any(
        item.relative_path == "artifacts/workspace.tar" and item.required
        for item in agent.output_declarations
    )
    verifier = compile_deferred_verifier_plan(agent, task, verifier_timeout_seconds=120)
    assert verifier.execution_role == "verifier"
    assert verifier.verifier_execution == "skipped"
    assert [sidecar.role_name for sidecar in verifier.sidecars if sidecar.private_sandbox] == [
        "verifier-sandbox",
    ]
    assert verifier.main.argv == (
        "python", "-I", "-m", "loom.service_execution_sandbox_task",
        "verify-sandbox", "--workspace", "/workspace",
    )
    assert any(
        item.relative_path == "verifier/output.json" and item.required
        for item in verifier.output_declarations
    )


def test_verifier_restores_committed_workspace_archive(tmp_path):
    from loom.service_execution_sandbox_task import (
        ServiceExecutionTaskError,
        stage_committed_workspace_archive,
    )

    with pytest.raises(ServiceExecutionTaskError, match="handoff archive is missing"):
        stage_committed_workspace_archive(tmp_path)
    committed = tmp_path / "artifacts" / "workspace.tar"
    committed.parent.mkdir()
    committed.write_bytes(b"committed-workspace")
    staged = stage_committed_workspace_archive(tmp_path)
    assert staged == tmp_path / ".loom" / "workspace.tar"
    assert staged.read_bytes() == b"committed-workspace"


def test_shared_terminus_plan_omits_idle_verifier_sidecar():
    task, trial, profile = _inputs()
    trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    assert not automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox"]
    assert all(s.private_sandbox and s.image_ref == _TASK_IMAGE for s in plan.sidecars)
    assert plan.in_place_verifier is True
    assert plan.verifier_execution == "in_attempt"
    assert plan.main.argv[4] == "terminus-2"
    assert plan.verifier.argv[4] == "verify-sandbox"
    assert plan.main.argv[-2:] == plan.verifier.argv[-2:] == ("--workspace", "/workspace")
    assert plan.main.working_directory == plan.verifier.working_directory == "/app"
    assert plan.main.argv[:3] == plan.verifier.argv[:3] == ("python", "-I", "-m")
    assert json.loads(plan.main.environment["LOOM_TASK_TRIAL_JSON"])["agent_name"] == "terminus-2"
    paths = {item.relative_path for item in plan.output_declarations}
    assert not next(item for item in plan.output_declarations
                    if item.relative_path == "artifacts/answer.txt").required
    assert {"trajectory/events.jsonl", "artifacts/harbor/trajectory.json",
            "artifacts/workspace.tar", "accounting/usage.json"} <= paths


def test_workload_topology_matches_direct_completion_plan_with_separate_task_default():
    task = _task()
    trial = _trial()
    assert task.verifier.env_mode == "separate"
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=_profile(), source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    requirements = workload_requirements_from_task(task, trial)
    assert requirements.verifier_topology == VerifierTopology.IN_ATTEMPT
    assert plan.verifier_execution == "in_attempt"
    validate_runtime_plan_requirements(plan, requirements)


def test_workload_topology_matches_shared_terminus_trial_override():
    task, trial, profile = _inputs()
    assert task.verifier.env_mode == "separate"
    trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    requirements = workload_requirements_from_task(task, trial)
    assert requirements.verifier_topology == VerifierTopology.IN_ATTEMPT
    assert plan.verifier_execution == "in_attempt"
    validate_runtime_plan_requirements(plan, requirements)


def test_terminus_rejects_missing_controller_and_disabled_private_isolation():
    task, trial, _ = _inputs()
    with pytest.raises(ValueError, match="no Terminus controller"):
        compile_service_execution_plan(task=task, trial=trial, profile=_profile(),
            source_provenance=_provenance(), task_revision_sha256=_REVISION)
    reasons = automatic_service_execution_rejections(task, trial.model_copy(
        update={"workspace_staging_policy_name": "none"}), source_provenance=_provenance())
    assert "private_workspace_isolation_required" in reasons


def _events():
    _, trial, _ = _inputs()
    now = datetime.now(UTC)
    identity = uuid4()
    events = [Terminus2UserPromptEvent(emitted_at=now, trial_id=identity, step_id="agent", seq=0,
                                     prompt_id=str(uuid4()), harbor_step_id=1,
                                     message="Solve the original task")]
    events.append(LLMCallEvent(
        emitted_at=now, trial_id=identity, step_id="agent", seq=1,
        model=trial.agent_model, rate_card_hash="rate-v1", system_prompt=None,
        messages=[{"role": "user", "content": "Solve"}],
        response={"role": "assistant", "content": "done"}, finish_reason="stop",
        input_tokens=5, output_tokens=3, cached_input_tokens=0, cache_write_tokens=0,
        thinking_tokens=0, provider_extras={}, request_params=trial.request_params,
        cost_usd_snapshot=0.001, duration_sec=1, streamed=False, time_to_first_token_sec=None,
        gateway_request_id=str(uuid4()),
    ))
    return trial, identity, events


def test_typed_trace_keeps_native_events_and_real_call_accounting():
    trial, identity, events = _events()
    body = b"\n".join(e.model_dump_json().encode() for e in events) + b"\n"
    assert parse_terminus_events(body, trial=trial, trial_id=identity) == events
    usage = terminus_usage(events, trial)
    validate_usage_accounting(trace_body=body, usage_body=json.dumps(usage).encode(), trial_config=trial)
    result = ExecutionRuntimeResultV1.model_validate({
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": _TASK_IMAGE, "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "3" * 64,
        "execution_class_id": "linux-amd64-cpu-pod-v1", "status": "succeeded",
        "started_at": events[0].emitted_at, "finished_at": events[-1].emitted_at,
        "phases": [], "outputs": [], "verifier_rewards": {"passed": 0}, "partial_evidence": False,
    })
    lost = {**result.model_dump(), "status": "runtime_error", "partial_evidence": True,
            "failure_reason": "sandbox_lost"}
    assert ExecutionRuntimeResultV1.model_validate(lost).failure_reason == "sandbox_lost"
    with pytest.raises(ValueError, match="sandbox loss must remain a runtime failure"):
        ExecutionRuntimeResultV1.model_validate({
            **result.model_dump(), "failure_reason": "sandbox_lost",
        })
    canonical = build_canonical_events(
        trial_id=identity, task_id="task-1", task_config=_inputs()[0], trial_config=trial,
        runtime_result=result, trace_body=body, verifier_body=b'{"rewards":{"passed":0}}',
    )
    assert canonical[2].kind == "terminus2_user_prompt"
    assert canonical[3].gateway_request_id == events[1].gateway_request_id
    assert canonical[-1].final_state == "succeeded"
    assert canonical[-1].reward == {"passed": 0}
    assert [e.seq for e in canonical] == list(range(len(canonical)))
    usage["totals"]["input_tokens"] += 1
    with pytest.raises(MaterializationIntegrityError, match="usage_output_identity_drift"):
        validate_usage_accounting(trace_body=body, usage_body=json.dumps(usage).encode(),
                                  trial_config=trial)
    with pytest.raises(ValueError, match="another Trial"):
        parse_terminus_events(body, trial=trial, trial_id=uuid4())


def test_controller_resources_are_frozen_independently_of_large_task():
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1, runtime_pod_resources
    from loom.service_execution_materialization import (
        ControllerComputeResourcesV1,
        ServiceExecutionRuntimeProfileV1,
    )

    task, trial, profile = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "cpus": 8, "memory_mb": 16_384, "storage_mb": 10_240,
    })})
    old_profile = profile.model_dump(mode="json")
    old_profile.pop("controller_resources")
    legacy = ServiceExecutionRuntimeProfileV1.model_validate(old_profile)
    def compile(value):
        return compile_service_execution_plan(
            task=task, trial=trial, profile=value, source_provenance=_provenance(),
            task_revision_sha256=_REVISION,
        )
    old_plan = compile(legacy)
    assert "controller_resources" not in old_plan.canonical_payload()
    assert ExecutionRuntimePlanV1.model_validate(old_plan.canonical_payload()).canonical_payload() == old_plan.canonical_payload()
    assert runtime_pod_resources(old_plan).cpu_millis == 16_000
    profile = profile.model_copy(update={
        "controller_resources": ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=2048),
    })
    # Submission persists the profile before the scheduler compiles the plan.
    frozen_profile = ServiceExecutionRuntimeProfileV1.model_validate_json(profile.model_dump_json())
    plan = compile(frozen_profile)
    frozen = plan.canonical_payload()
    assert plan.task_resources.cpu_millis == 8000
    assert plan.controller_resources.cpu_millis == 1000
    assert plan.controller_resources.memory_mib == 2048
    assert plan.controller_resources.ephemeral_storage_mib == plan.workspace_mib == 10_240
    assert all(s.resources == plan.task_resources for s in plan.sidecars)
    assert runtime_pod_resources(plan).cpu_millis == 9_000
    assert runtime_pod_resources(plan).memory_mib == 18_432
    assert runtime_pod_resources(plan).ephemeral_storage_mib == 20_480
    changed_profile = profile.model_copy(update={
        "controller_resources": ControllerComputeResourcesV1(cpu_millis=2000, memory_mib=4096),
    })
    assert compile(changed_profile).controller_resources.cpu_millis == 2000
    assert compile(frozen_profile).controller_resources.cpu_millis == 1000
    assert ExecutionRuntimePlanV1.model_validate(frozen).controller_resources.cpu_millis == 1000


@pytest.mark.parametrize("mutation,reason", [
    ("missing_controller", "isolated attempt controller"),
    ("missing_verifier", "isolated attempt controller"),
    ("smaller_sandbox", "preserve task and verifier"),
    ("smaller_storage", "preserve task-derived storage"),
])
def test_controller_sizing_cannot_weaken_task_contract(mutation, reason):
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1
    from loom.service_execution_materialization import ControllerComputeResourcesV1

    task, trial, profile = _inputs()
    profile = profile.model_copy(update={
        "controller_resources": ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=2048),
    })
    payload = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    ).canonical_payload()
    if mutation == "missing_controller":
        payload["agent_image_ref"] = None
    elif mutation == "missing_verifier":
        payload["verifier_execution"] = "in_attempt"
        verifier = dict(payload["main"])
        verifier["role"] = "verifier"
        payload["verifier"] = verifier
        payload["in_place_verifier"] = False
    elif mutation == "smaller_sandbox":
        payload["sidecars"][0]["resources"]["cpu_millis"] = 1
    else:
        payload["controller_resources"]["ephemeral_storage_mib"] = 1
    with pytest.raises(ValueError, match=reason if mutation != "missing_controller" else "agent image reference"):
        ExecutionRuntimePlanV1.model_validate(payload)


def test_direct_completion_keeps_task_resources_with_controller_profile():
    from loom.service_execution_materialization import ControllerComputeResourcesV1

    task, trial, profile = _task(), _trial(), _profile()
    profile = profile.model_copy(update={
        "controller_resources": ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=2048),
    })
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.controller_resources is None
    assert plan.execution_resources == plan.task_resources
    assert "controller_resources" not in plan.canonical_payload()
