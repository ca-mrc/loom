"""Guest launch authority comes from deployment readiness and frozen task requirements."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from loom.execution_contract import workload_requirements_from_task
from loom.execution_requirements import GUEST_EXECUTION_CAPABILITIES
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    runtime_pod_resources,
    validate_runtime_plan_requirements,
)
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    ControllerComputeResourcesV1,
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_service.execution_admission import admit_execution_backend
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_prepared_image import _prepared
from tests.unit.test_service_execution_terminus_plan import _inputs


def _guest_inputs(*capabilities):
    task, trial, profile = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"].update(user="root", execution_requirements={
        "capabilities": capabilities or ["nested_docker"],
    })
    raw["verifier"]["user"] = "root"
    profile = ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"), "guest_runtime": "qemu-tcg-v1",
        "supports_task_identity": True, "runtime_volume_mib": 1024,
    })
    return TaskConfig.model_validate(raw), trial, profile


def _compile(task, trial, profile, **kwargs):
    return compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION, **kwargs)


@pytest.mark.parametrize("web", [False, True])
def test_guest_opt_in_preserves_primary_class_and_selects_separate_plan_class(web):
    task, trial, profile = _guest_inputs("singularity_mounts", "nested_docker")
    if web:
        profile = ServiceExecutionRuntimeProfileV1.model_validate({
            **profile.model_dump(mode="json"), "supports_task_web_egress": True,
            "execution_class_id": "linux-amd64-cpu-web-pod-v1",
        })
    assert profile.execution_class_id == (
        "linux-amd64-cpu-web-pod-v1" if web else "linux-amd64-cpu-pod-v1")
    assert "nested_docker_unqualified" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())
    assert not automatic_service_execution_rejections(task, trial,
        source_provenance=_provenance(), supported_capabilities=GUEST_EXECUTION_CAPABILITIES)
    plan = _compile(task, trial, profile)
    assert plan.execution_class_id == (
        "linux-amd64-cpu-guest-web-v1" if web else "linux-amd64-cpu-guest-v1")
    for sidecar in plan.canonical_payload()["sidecars"]:
        assert sidecar["guest_execution"] == {
            "schema_version": "loom.guest-execution.v1", "runtime": "qemu-tcg-v1",
            "capabilities": ["nested_docker", "singularity_mounts"],
        }
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(task))
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


def test_ordinary_plan_and_profile_omit_guest_extensions_even_when_deployment_ready():
    task, trial, profile = _inputs()
    assert "guest_runtime" not in profile.model_dump(mode="json")
    old = _compile(task, trial, profile)
    ready = ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"), "guest_runtime": "qemu-tcg-v1",
    })
    assert _compile(task, trial, ready).canonical_payload() == old.canonical_payload()
    assert all("guest_execution" not in sidecar for sidecar in old.canonical_payload()["sidecars"])


@pytest.mark.parametrize("change,reason", [
    ({"guest_runtime": None}, "guest_runtime_unavailable"),
    ({"supports_task_identity": False}, "task_identity_runtime_unavailable"),
    ({"runtime_volume_mib": 1023}, "guest_runtime_volume_too_small"),
])
def test_profile_readiness_cannot_be_granted_by_task_or_prepared_image(change, reason):
    task, trial, profile = _guest_inputs()
    profile = profile.model_copy(update=change)
    assert reason in runtime_profile_rejections(task, trial, profile)
    with pytest.raises(ValueError):
        _compile(task, trial, profile)
    prepared_task, _, _, grant = _prepared()
    raw = task.model_dump(mode="json")
    raw["environment"].update(docker_image=None, dockerfile=prepared_task.environment.dockerfile)
    task = TaskConfig.model_validate(raw)
    grant = grant.model_copy(update={"task_config": task.model_dump(mode="json")})
    with pytest.raises(ValueError, match=reason if change.get("guest_runtime", "ready") else "unqualified"):
        _compile(task, trial, profile, task_image_grant=grant)


@pytest.mark.parametrize("section,change,reason", [
    ("environment", {"user": "agent"}, "guest_root_identity_required"),
    ("verifier", {"user": None}, "guest_root_identity_required"),
    ("verifier", {"user": "1001:1001"}, "guest_root_identity_required"),
    ("environment", {"cpus": 0.5}, "guest_cpu_limit_too_small"),
    ("environment", {"memory_mb": 511}, "guest_memory_limit_too_small"),
    ("environment", {"storage_mb": 159}, "guest_ephemeral_storage_limit_too_small"),
])
def test_guest_prerequisites_are_checked_before_admission(section, change, reason):
    task, trial, _ = _guest_inputs()
    raw = task.model_dump(mode="json")
    raw[section].update(change)
    reasons = automatic_service_execution_rejections(TaskConfig.model_validate(raw), trial,
        source_provenance=_provenance(), supported_capabilities=GUEST_EXECUTION_CAPABILITIES)
    assert reason in reasons


def test_guest_requires_terminus_and_external_requirements_remain_rejected():
    task, trial, profile = _guest_inputs("nested_docker", "external_cluster")
    reasons = automatic_service_execution_rejections(task, trial.model_copy(update={
        "agent_name": "direct-completion"}), source_provenance=_provenance(),
        supported_capabilities=GUEST_EXECUTION_CAPABILITIES)
    assert {"guest_private_sandboxes_required", "external_cluster_unqualified"} <= set(reasons)
    with pytest.raises(ValueError, match="external_cluster_unqualified"):
        _compile(task, trial, profile)


def test_guest_rejects_unqualified_fixture_networking():
    from tests.unit.test_task_fixtures import _fixture

    task, trial, _ = _guest_inputs()
    raw = task.model_dump(mode="json")
    raw["environment"].update(docker_image=None, dockerfile="environment/Dockerfile",
                              docker_build_context="environment", sidecars=[_fixture()])
    reasons = automatic_service_execution_rejections(TaskConfig.model_validate(raw), trial,
        source_provenance=_provenance(), allow_task_image_preparation=True,
        supported_capabilities=GUEST_EXECUTION_CAPABILITIES)
    assert "guest_sidecars_unsupported" in reasons


@pytest.mark.parametrize("explicit_controller", [False, True])
@pytest.mark.parametrize("request_override", [False, True])
def test_guest_payload_storage_is_reserved_in_controller_and_node_share(explicit_controller, request_override):
    task, trial, profile = _guest_inputs()
    if explicit_controller:
        profile = profile.model_copy(update={
            "controller_resources": ControllerComputeResourcesV1(cpu_millis=500, memory_mib=512),
        })
    if request_override:
        profile = ServiceExecutionRuntimeProfileV1.model_validate({
            **profile.model_dump(mode="json"), "task_resource_requests": {
                "guest": {"task_revision_sha256": _REVISION, "requests": {
                    "controller": {"cpu_millis": 100, "memory_mib": 128, "ephemeral_storage_mib": 128},
                }},
            },
        })
    plan = _compile(task, trial, profile, task_id="guest")
    assert plan.controller_resources.ephemeral_storage_mib == task.environment.storage_mb + 1024
    expected_request = (128 if request_override else task.environment.storage_mb) + 1024
    assert plan.container_request("execution").ephemeral_storage_mib == expected_request
    assert runtime_pod_resources(plan).ephemeral_storage_mib == expected_request + 2 * task.environment.storage_mb
    # A node that fits only the task allocations cannot hide the shared payload.
    with pytest.raises(ValueError, match="exceeds_node_allocatable"):
        allocate_node_resources(plan, target_id="too-small", usable_node=ContainerResourcesV1(
            cpu_millis=4000, memory_mib=8192,
            ephemeral_storage_mib=expected_request + 2 * task.environment.storage_mb - 1,
        ))


@pytest.mark.parametrize("damage", [
    "ordinary_class", "missing_guest", "one_guest", "empty_caps", "external_cap",
    "nonroot", "short_volume", "wrong_socket", "wrong_probe", "small_resources",
    "duplicate_caps", "unsorted_caps", "long_timeout", "unreserved_payload", "noncanonical_timeout",
])
def test_guest_runtime_shape_rejects_partial_or_unsafe_launches(damage):
    task, trial, profile = _guest_inputs()
    raw = _compile(task, trial, profile).canonical_payload()
    if damage == "ordinary_class":
        raw["execution_class_id"] = profile.execution_class_id
    elif damage in {"missing_guest", "one_guest"}:
        for sidecar in raw["sidecars"][:2 if damage == "missing_guest" else 1]:
            sidecar.pop("guest_execution")
    elif damage in {"empty_caps", "external_cap"}:
        raw["sidecars"][0]["guest_execution"]["capabilities"] = (
            [] if damage == "empty_caps" else ["external_cluster"])
    elif damage == "nonroot":
        raw["sidecars"][0]["identity"]["run_as_user"] = 65532
    elif damage == "short_volume":
        raw["runtime_volume_mib"] = 32
    elif damage == "wrong_socket":
        raw["sidecars"][0]["argv"][2] = "/tmp/foreign.sock"
    elif damage == "wrong_probe":
        raw["sidecars"][0]["startup_probe"]["argv"][2] = "/tmp/foreign.sock"
    elif damage in {"duplicate_caps", "unsorted_caps"}:
        raw["sidecars"][0]["guest_execution"]["capabilities"] = [
            "nested_docker", "nested_docker" if damage == "duplicate_caps" else "isolated_kernel_settings"]
    elif damage in {"long_timeout", "noncanonical_timeout"}:
        raw["sidecars"][0]["argv"][-1] = "86401" if damage == "long_timeout" else "0900"
    elif damage == "unreserved_payload":
        raw["controller_resources"] = None
    else:
        raw["sidecars"][0]["resources"]["memory_mib"] = 511
    with pytest.raises(ValueError):
        ExecutionRuntimePlanV1.model_validate(raw)


@pytest.mark.parametrize("damage", ["caps", "isolation", "prerequisite", "host_flag"])
def test_runtime_binding_rejects_drift_from_frozen_workload(damage):
    task, trial, profile = _guest_inputs()
    plan = _compile(task, trial, profile)
    raw = workload_requirements_from_task(task).model_dump(mode="json")
    if damage == "caps":
        raw["execution_requirements"]["capabilities"] = ["isolated_kernel_settings"]
    elif damage == "isolation":
        raw["isolation_level"] = "shared_kernel"
    elif damage == "host_flag":
        raw["host_network"] = True
    else:
        raw["execution_requirements"]["prerequisites"] = [{"name": "missing", "kind": "fixture"}]
    with pytest.raises(ValueError):
        validate_runtime_plan_requirements(plan,
            type(workload_requirements_from_task(task)).model_validate(raw))


@pytest.mark.parametrize("capability", ["nested_docker", "isolated_kernel_settings"])
@pytest.mark.parametrize("allocated", [False, True])
def test_renderer_keeps_guest_state_private_bounded_and_host_unprivileged(capability, allocated):
    task, trial, profile = _guest_inputs(capability)
    plan = _compile(task, trial, profile)
    lease = _lease()
    if allocated:
        plan = allocate_node_resources(plan, target_id=lease.target_id, usable_node=ContainerResourcesV1(
            cpu_millis=16_000, memory_mib=240 * 1024, ephemeral_storage_mib=512 * 1024,
        ))
    lease.execution_class_id = plan.execution_class_id
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    pod = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]
    volumes = {item["name"]: item for item in pod["volumes"]}
    total = runtime_pod_resources(plan)
    assert total.cpu_millis == plan.execution_resources.cpu_millis + plan.task_resources.cpu_millis * 2
    assert total.ephemeral_storage_mib == (
        plan.execution_resources.ephemeral_storage_mib + plan.task_resources.ephemeral_storage_mib * 2)
    for container in pod["initContainers"][1:]:
        role = container["name"]
        assert container["command"] == [
            "/loom/runtime/guest/bin/loom-guest-runtime", "--payload", "/loom/runtime/guest",
            "--root", "/", "--state", "/loom/guest-state/incarnation",
            "--socket", f"/loom/sandboxes/{role}/sandbox.sock",
            "--memory-mib", str(plan.task_resources.memory_mib),
            "--storage-mib", str(plan.task_resources.ephemeral_storage_mib - 32),
            "--cpu-millis", str(plan.task_resources.cpu_millis),
            "--max-transfer-bytes", str(plan.max_artifact_bytes),
            "--exec-timeout-seconds", plan.sidecars[0].argv[-1],
            *(["--nested-docker"] if capability == "nested_docker" else []),
        ]
        assert container["image"] == plan.task_image_ref
        assert container["securityContext"] == {
            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "runAsNonRoot": False, "runAsUser": 0, "runAsGroup": 0,
            "capabilities": {"drop": ["ALL"], "add": ["DAC_OVERRIDE"]},
        }
        assert {"name": "runtime", "mountPath": "/loom/runtime", "readOnly": True} in container["volumeMounts"]
        assert {"name": f"{role}-guest-state", "mountPath": "/loom/guest-state"} in container["volumeMounts"]
        assert volumes[f"{role}-guest-state"]["emptyDir"] == {
            "sizeLimit": f"{plan.task_resources.ephemeral_storage_mib}Mi"}
        assert container["startupProbe"]["exec"]["command"] == [
            "/loom/bin/loom-sandbox-runtime", "--check-socket", f"/loom/sandboxes/{role}/sandbox.sock"]
        assert container["startupProbe"]["failureThreshold"] == 60
        mounted = {item["name"] for item in container["volumeMounts"]}
        assert mounted == {"runtime", f"{role}-socket", f"{role}-guest-state"}
    assert all("guest-state" not in mount["name"] for container in [
        pod["initContainers"][0], *pod["containers"]] for mount in container["volumeMounts"])


@pytest.mark.parametrize("ready", [False, True])
async def test_service_admission_uses_deployment_opt_in(monkeypatch, ready):
    task, trial, profile = _guest_inputs()
    if not ready:
        profile = profile.model_copy(update={"guest_runtime": None})
    result = Mock()
    result.all.return_value = [("guest", task.model_dump(mode="json"), _provenance())]
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    monkeypatch.setattr("loom_service.execution_admission.get_service_execution_backend_pools",
        AsyncMock(return_value=[SimpleNamespace(pool_name="nebius-cpu")]))
    args = dict(backend="nebius", task_ids=["guest"], trial_config=trial.model_dump(mode="json"),
        combinations=[], runtime_profile_json=profile.model_dump_json(), resolve_versions=False)
    if ready:
        assert await admit_execution_backend(session, **args) == profile
    else:
        with pytest.raises(HTTPException, match="nested_docker_unqualified"):
            await admit_execution_backend(session, **args)
