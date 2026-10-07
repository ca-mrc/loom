"""#2296: the task-sandbox planner is harness-neutral platform code.

Behaviour parity for Terminus-2 and Oracle is pinned by
`test_hosted_harness_plan_parity.py`; these tests cover the planner boundary,
its composition with network policy and guest isolation, and that every
migrated plan stays consistent with workload requirements, allocation and
Kubernetes rendering.
"""

from __future__ import annotations

import inspect
import json
import re

import pytest
from pydantic import ValidationError

import loom.hosted_harness as hosted
import loom.task_sandbox_planner as planner
from loom.execution_contract import VerifierTopology, workload_requirements_from_task
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    RuntimeHandoffInputV1,
    validate_runtime_plan_requirements,
)
from loom.hosted_harness import SANDBOX_CONTROLLER_MODULE, HostedHarnessSpec, NativeOutput
from loom.models.networking import NetworkPolicy, PublicWeb
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    compile_deferred_verifier_plan,
    service_execution_input_binding,
)
from loom.task_sandbox_planner import TaskSandboxPlanRequest, compile_task_sandbox_plan
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_guest_execution_materialization import _guest_inputs
from tests.unit.test_hosted_harness_plan_parity import _CASES
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_TEST_HARNESS = HostedHarnessSpec(
    name="test-only-harness", execution_kind="workspace", controller_module=SANDBOX_CONTROLLER_MODULE,
    controller_phase="test-only-harness", controller_image="harness-controller", model="required",
    gateway_protocol="openai-chat-completions", trace_format="terminus",
    required_driver_capabilities=frozenset({"exec", "upload", "download"}),
    native_outputs=(NativeOutput("agent/native.jsonl", "artifacts/native.jsonl", "agent_native", True),),
)


@pytest.fixture(autouse=True)
def _registered(monkeypatch: pytest.MonkeyPatch) -> None:
    # Workload requirements resolve topology through the registry, as in production.
    monkeypatch.setattr(hosted, "HOSTED_HARNESSES", hosted._index(
        (*{spec.name: spec for spec in hosted.HOSTED_HARNESSES.values()}.values(), _TEST_HARNESS),
    ))


def _request(*, guest: bool = False, shared: bool = False, policy: NetworkPolicy | None = None):
    task, _, profile = _guest_inputs("nested_docker") if guest else _inputs()
    trial = TrialConfig(
        agent_name="test-only-harness", agent_model=ModelSpec(provider="openai", name="gpt-5"),
        **({"verifier_env_mode": "shared"} if shared else {}),
    )
    if policy is not None:
        profile = profile.model_copy(update={"supports_task_web_egress": True})
    binding = service_execution_input_binding(_provenance())
    assert binding is not None and profile.agent_image_ref is not None
    return task, trial, TaskSandboxPlanRequest(
        spec=_TEST_HARNESS, task=task, trial=trial, profile=profile, binding=binding,
        task_revision_sha256=_REVISION, command_identity="sha256:" + "4" * 64,
        output_paths=("answer.txt",),
        effective_network_policy=policy or task.environment.baseline_network_policy,
        controller_image=profile.agent_image_ref,
    )


def test_planner_source_has_no_harness_name_branches() -> None:
    source = inspect.getsource(planner)
    code = re.sub(r'"""[\s\S]*?"""|#[^\n]*', "", source)

    assert not re.search(r"terminus|oracle|direct.completion|litellm|openhands|codex", code, re.I)
    assert "agent_name" not in code
    assert "hosted_harness(" not in code  # the caller resolves the spec


def test_execution_contract_has_no_harness_name_branches() -> None:
    import loom.execution_contract as contract

    code = re.sub(r'"""[\s\S]*?"""|#[^\n]*', "", inspect.getsource(contract))

    assert not re.search(r"terminus|oracle|direct.completion|litellm|openhands|codex", code, re.I)
    # Agent names are only ever registry lookup keys.
    assert set(re.findall(r"[\w.(]*agent(?:_|\.)name\)?", code)) == {
        "is_workspace_harness(trial.agent_name)", "hosted_harness(task.agent.name)",
    }


def _task_only_requirements(agent: str, env_mode: str):
    from tests.unit.test_service_execution_terminus_plan import _inputs

    task, _, _ = _inputs()
    return workload_requirements_from_task(task.model_copy(update={
        "agent": task.agent.model_copy(update={"name": agent}),
        "verifier": task.verifier.model_copy(update={"env_mode": env_mode}),
    }))


def test_task_only_requirements_keep_their_stored_digests() -> None:
    # Recorded from `dev` before the projection moved to the spec (#2296);
    # stored requirement comparisons depend on these exact bytes.
    import hashlib

    def digest(agent: str, env_mode: str) -> str:
        return hashlib.sha256(_task_only_requirements(agent, env_mode).model_dump_json().encode()).hexdigest()

    assert digest("terminus-2", "separate") == "cfe3a0053e9f4662c64da598d4183455247e5091ef97187d759ff443fc5e14ee"
    assert digest("terminus-2", "shared") == "a7f8aa3f3a598383c79729a5d3532b844fcce3017b3e5673c73eb7cb536774bd"


@pytest.mark.parametrize("agent", [*sorted(hosted.HOSTED_HARNESSES), "no-such-agent"])
@pytest.mark.parametrize("env_mode", ["separate", "shared"])
def test_task_only_separate_verifier_projection_is_terminus_only(agent: str, env_mode: str) -> None:
    # The historical rule was `task.agent.name == "terminus-2"`; every other
    # declared harness, including later workspace harnesses, keeps in-attempt.
    topology = _task_only_requirements(agent, env_mode).verifier_topology
    historical = agent == "terminus-2" and env_mode == "separate"
    assert topology == (VerifierTopology.SEPARATE_EXECUTION if historical else VerifierTopology.IN_ATTEMPT)
    assert {spec.name for spec in hosted.HOSTED_HARNESSES.values() if spec.task_declared_separate_verifier} == {
        "terminus-2",
    }


@pytest.mark.parametrize("name", ["direct-completion", "oracle-shared", "terminus-2-separate"])
def test_many_artifact_paths_compile_without_oversized_environment(name: str) -> None:
    from loom.service_execution_materialization import compile_service_execution_plan

    task, trial, profile = _CASES[name]()
    paths = [f"out/part-{index:04}.json" for index in range(515)]
    task = task.model_copy(update={"steps": [task.steps[0].model_copy(update={
        "artifacts": paths, "required_artifacts": [paths[-1]],
    })]})
    profile = profile.model_copy(update={"supports_task_artifact_inputs": True})

    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )

    assert {"artifacts/" + path for path in paths} <= {o.relative_path for o in plan.output_declarations}
    assert all(len(value.encode("utf-8")) <= 4096 for value in plan.main.environment.values())
    assert plan.main.environment["LOOM_TASK_ARTIFACTS_FROM_INPUT"] == "1"
    assert "LOOM_TASK_ARTIFACTS_JSON" not in plan.main.environment
    assert task.steps[0].artifacts == paths


@pytest.mark.parametrize("name", ["direct-completion", "oracle-shared", "terminus-2-separate"])
@pytest.mark.parametrize("encoded_bytes", [4096, 4097])
@pytest.mark.parametrize("supports_inputs", [False, True])
def test_artifact_transport_respects_frozen_profile_and_environment_boundary(
    name: str, encoded_bytes: int, supports_inputs: bool,
) -> None:
    from loom.service_execution_materialization import compile_service_execution_plan

    task, trial, profile = _CASES[name]()
    # Direct-completion uses compact JSON; the sandbox's legacy encoding uses
    # spaces. Size the fixture against each existing wire format.
    separators = (",", ":") if name == "direct-completion" else (", ", ": ")
    paths = [f"out/{index:04}" for index in range(339)]
    paths[-1] += "a" * (encoded_bytes - len(json.dumps(paths, separators=separators)))
    encoded = json.dumps(paths, separators=separators)
    assert len(encoded.encode("utf-8")) == encoded_bytes
    task = task.model_copy(update={"steps": [task.steps[0].model_copy(update={
        "artifacts": paths, "required_artifacts": [],
    })]})
    profile = profile.model_copy(update={"supports_task_artifact_inputs": supports_inputs})
    arguments = dict(task=task, trial=trial, profile=profile,
                     source_provenance=_provenance(), task_revision_sha256=_REVISION)
    if encoded_bytes > 4096 and not supports_inputs:
        with pytest.raises(ValidationError, match="process environment value is invalid"):
            compile_service_execution_plan(**arguments)
        return
    plan = compile_service_execution_plan(**arguments)
    if encoded_bytes > 4096:
        assert plan.main.environment["LOOM_TASK_ARTIFACTS_FROM_INPUT"] == "1"
        assert "LOOM_TASK_ARTIFACTS_JSON" not in plan.main.environment
    else:
        assert plan.main.environment["LOOM_TASK_ARTIFACTS_JSON"] == encoded
        assert "LOOM_TASK_ARTIFACTS_FROM_INPUT" not in plan.main.environment


@pytest.mark.parametrize("shared", [False, True])
def test_test_only_harness_compiles_through_the_planner(shared: bool) -> None:
    task, trial, request = _request(shared=shared)

    plan = compile_task_sandbox_plan(request)

    assert plan.main.argv[3:5] == (SANDBOX_CONTROLLER_MODULE, "test-only-harness")
    assert plan.agent_image_ref == request.controller_image
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox"]
    assert plan.in_place_verifier is shared
    assert plan.verifier_execution == ("in_attempt" if shared else "separate_execution")
    paths = {item.relative_path for item in plan.output_declarations}
    assert {"artifacts/native.jsonl", "artifacts/answer.txt", "artifacts/workspace.tar"} <= paths
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(task, trial))


def test_separate_mode_emits_the_existing_deferred_verifier_contract() -> None:
    task, _, request = _request()
    plan = compile_task_sandbox_plan(request)

    verifier = compile_deferred_verifier_plan(
        plan, task, verifier_timeout_seconds=60,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256="sha256:" + "5" * 64, file_count=1, total_bytes=1),
    )

    assert verifier.execution_role == "verifier"
    assert verifier.main.argv[4] == "verify-sandbox"
    assert [s.role_name for s in verifier.sidecars if s.private_sandbox] == ["verifier-sandbox"]
    assert verifier.main.argv[3] == plan.main.argv[3]  # the attempt's trusted controller module
    assert {o.relative_path for o in verifier.output_declarations} <= {
        "diagnostics/verifier-exception.json", "verifier/output.json", "artifacts/verifier/ctrf.json",
    }


def test_guest_extension_keeps_two_colocated_guests_for_any_workspace_harness() -> None:
    task, trial, request = _request(guest=True)

    plan = compile_task_sandbox_plan(request)

    assert plan.execution_class_id.startswith("linux-amd64-cpu-guest")
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox", "verifier-sandbox"]
    assert all(s.guest_execution is not None for s in plan.sidecars)
    # Separate grading stays colocated for guests; not #2212's deferred lifecycle.
    assert plan.verifier_execution == "in_attempt" and plan.in_place_verifier is False
    assert plan.controller_resources is not None
    assert plan.controller_resources.ephemeral_storage_mib == (
        task.environment.storage_mb + plan.runtime_volume_mib
    )
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(task, trial))


def test_planner_attaches_the_frozen_network_contract() -> None:
    policy = PublicWeb()
    task, _, request = _request(policy=policy)

    plan = compile_task_sandbox_plan(request)

    assert plan.effective_network_policy == policy
    assert plan.task_egress is not None
    assert plan.output_declarations[0].relative_path == "diagnostics/task-egress.jsonl"
    assert '"public-web"' in plan.main.environment["LOOM_EFFECTIVE_NETWORK_POLICY_JSON"]
    # The planner uses the resolved policy, not the task baseline.
    assert task.environment.baseline_network_policy.kind == "gateway-only"


@pytest.mark.parametrize("name", [name for name in _CASES if not name.startswith(("direct", "litellm"))])
def test_migrated_plans_stay_consistent_with_requirements_allocation_and_rendering(name: str) -> None:
    from loom.service_execution_materialization import compile_service_execution_plan

    task, trial, profile = _CASES[name]()
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    requirements = workload_requirements_from_task(task, trial)
    validate_runtime_plan_requirements(plan, requirements)
    lease = _lease()
    allocated = allocate_node_resources(plan, target_id=lease.target_id, usable_node=ContainerResourcesV1(
        cpu_millis=16_000, memory_mib=240 * 1024, ephemeral_storage_mib=512 * 1024,
    ))
    lease.execution_class_id = allocated.execution_class_id
    lease.runtime_contract_json = allocated.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = requirements.model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)

    job = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))

    sandboxes = [c["name"] for c in job["spec"]["template"]["spec"]["initContainers"] if "sandbox" in c["name"]]
    assert len(sandboxes) == sum(1 for s in plan.sidecars if s.private_sandbox)
    expected = (VerifierTopology.SEPARATE_EXECUTION if plan.verifier_execution == "separate_execution"
                else VerifierTopology.IN_ATTEMPT)
    assert requirements.verifier_topology == expected
