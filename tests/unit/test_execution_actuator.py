from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import (
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    ProcessPhaseV1,
    SidecarContainerV1,
)
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.contracts import ActuatorContractError
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.support.execution_image_admission import signed_image_admission_bundle


def _lease() -> ServiceExecutionLease:
    now = datetime.now(UTC)
    requirements = WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=1500,
        memory_mib=2048,
        ephemeral_storage_mib=4096,
        isolation_level=IsolationLevel.SHARED_KERNEL,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref="registry.example/task@sha256:" + "a" * 64,
        sidecar_count=0,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )
    runtime_image_ref = "registry.example/runtime@sha256:" + "b" * 64
    runtime = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_role="attempt",
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=requirements.image_ref or "",
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256="sha256:" + "c" * 64,
        image_admission=signed_image_admission_bundle(
            (requirements.image_ref or "", runtime_image_ref), now=now
        ),
        task_resources=ContainerResourcesV1(
            cpu_millis=1500,
            memory_mib=2048,
            ephemeral_storage_mib=4096,
        ),
        workspace_mib=4096,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
    )
    requirements_json = requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        execution_role="attempt",
        parent_lease_id=None,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="nebius-eu-north1-staging",
        routing_generation=1,
        selected_pool_id="nebius-cpu",
        routing_reason="admin_target_binding",
        routing_decision_sha256="sha256:" + "d" * 64,
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json,
        runtime_contract_sha256=canonical_digest(runtime_json),
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name="loom-nebius-staging",
        job_name="loom-123456789abc-a1-g1",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=10),
    )


def test_job_renderer_is_deterministic_restricted_and_lease_scoped() -> None:
    lease = _lease()
    now = datetime.now(UTC)
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        runtime_class_name="loom-sandbox",
    )

    first = render_execution_job(lease, target=target, now=now)
    second = render_execution_job(lease, target=target, now=now)

    assert first == second
    assert first["metadata"]["name"] == lease.job_name
    assert (
        first["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"]
        == "execution-unit"
    )
    assert (
        first["spec"]["template"]["metadata"]["annotations"][
            "cluster-autoscaler.kubernetes.io/safe-to-evict"
        ]
        == "false"
    )
    assert first["spec"]["backoffLimit"] == 0
    pod = first["spec"]["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["runtimeClassName"] == "loom-sandbox"
    assert pod["serviceAccountName"] == "loom-execution-attempt"
    assert "serviceAccountToken" not in str(pod)
    assert pod["initContainers"][0]["name"] == "runtime-materializer"
    container = pod["containers"][0]
    assert container["image"].endswith("@sha256:" + "a" * 64)
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": False,
        "runAsNonRoot": True,
    }
    assert container["resources"]["limits"] == {
        "cpu": "1500m",
        "memory": "2048Mi",
        "ephemeral-storage": "4096Mi",
    }
    assert container["terminationMessagePath"] == "/loom/output/termination-message"
    runtime_env = {item["name"]: item["value"] for item in container["env"]}
    assert runtime_env == {
        "LOOM_EXECUTION_BROKER_URL": (
            "http://loom-llm-gateway.loom.svc.cluster.local:9100/internal/service-execution"
        ),
        "LOOM_EXECUTION_GENERATION": "1",
        "LOOM_EXECUTION_LEASE_ID": str(lease.id),
        "LOOM_EXECUTION_ROLE": "attempt",
    }
    assert "TOKEN" not in str(runtime_env)
    assert "API_KEY" not in str(runtime_env)
    assert container["volumeMounts"][0] == {
        "name": "runtime",
        "mountPath": "/loom/runtime",
        "readOnly": True,
    }
    assert not ({"privileged", "hostNetwork", "hostPID", "hostIPC"} & pod.keys())
    assert "hostPath" not in str(pod)


@pytest.mark.parametrize(("setup_count", "execution_role", "expected_mib"), [
    (0, "attempt", 44), (2, "attempt", 84), (32, "attempt", 684), (0, "verifier", 24),
])
def test_output_volume_fits_every_phase_and_atomic_result_metadata(
    setup_count: int, execution_role: str, expected_mib: int,
) -> None:
    lease = _lease()
    assert lease.runtime_contract_json is not None
    payload = {
        **lease.runtime_contract_json,
        "setup": [ProcessPhaseV1(role="setup", argv=("/bin/true",),
            working_directory="/workspace", timeout_seconds=1)] * setup_count,
        "max_artifact_bytes": 1024 * 1024,
    }
    if execution_role == "verifier":
        lease.execution_role = "verifier"
        payload.update(execution_role="verifier", main=payload["verifier"],
            verifier=None, verifier_execution="skipped")
    plan = ExecutionRuntimePlanV1.model_validate(payload)
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)

    pod = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]

    # Each phase can save two 10 MiB streams, in addition to the 1 MiB
    # artifact bundle, two 1 MiB copies during result replacement and 4 KiB
    # for the termination summary, rounded upward to whole MiB.
    output = next(volume for volume in pod["volumes"] if volume["name"] == "output")
    assert output["emptyDir"]["sizeLimit"] == f"{expected_mib}Mi"
    assert pod["containers"][0]["resources"]["limits"]["ephemeral-storage"] == "4096Mi"


def test_output_volume_preserves_independent_aggregate_storage_limit() -> None:
    lease = _lease()
    assert lease.runtime_contract_json is not None
    plan = ExecutionRuntimePlanV1.model_validate({
        **lease.runtime_contract_json, "max_artifact_bytes": 4096 * 1024 * 1024,
    })
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)

    pod = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]

    # Existing plans may declare per-volume caps larger than their aggregate
    # Pod budget. Correcting one cap must not change admitted resources.
    output = next(volume for volume in pod["volumes"] if volume["name"] == "output")
    assert output["emptyDir"]["sizeLimit"] == "4139Mi"
    assert pod["containers"][0]["resources"]["limits"]["ephemeral-storage"] == "4096Mi"
    assert pod["containers"][0]["resources"]["requests"]["ephemeral-storage"] == "4096Mi"


def test_job_renderer_uses_default_runtime_when_no_runtime_class_is_configured() -> None:
    lease = _lease()
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
    )

    pod = render_execution_job(lease, target=target)["spec"]["template"]["spec"]

    assert "runtimeClassName" not in pod
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}


def test_job_renderer_applies_target_node_placement() -> None:
    lease = _lease()
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        node_selector={"loom.nebius/node-role": "execution"},
        tolerations=(
            {
                "key": "loom.nebius/execution",
                "operator": "Equal",
                "value": "true",
                "effect": "NoSchedule",
            },
        ),
    )

    pod = render_execution_job(lease, target=target)["spec"]["template"]["spec"]

    assert pod["nodeSelector"] == {"loom.nebius/node-role": "execution"}
    assert pod["tolerations"] == [
        {
            "key": "loom.nebius/execution",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }
    ]


def test_job_renderer_rejects_mutable_image_missing_limits_and_wrong_scope() -> None:
    lease = _lease()
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        runtime_class_name="loom-sandbox",
    )
    lease.runtime_contract_json = None
    lease.runtime_contract_sha256 = None
    with pytest.raises(ActuatorContractError, match="no immutable runtime contract"):
        render_execution_job(lease, target=target)

    lease = _lease()
    lease.workload_requirements_json = {
        **lease.workload_requirements_json,
        "image_ref": "registry.example/task:latest",
    }
    with pytest.raises(ActuatorContractError, match="immutable"):
        render_execution_job(lease, target=target)

    lease = _lease()
    lease.workload_requirements_json = {
        **lease.workload_requirements_json,
        "memory_mib": None,
    }
    with pytest.raises(ActuatorContractError, match="memory_mib"):
        render_execution_job(lease, target=target)

    with pytest.raises(ActuatorContractError, match="namespace"):
        render_execution_job(
            lease,
            target=ExecutionTargetRuntime(
                target_id="nebius-eu-north1-staging",
                namespace="foreign",
                runtime_class_name="loom-sandbox",
            ),
        )


@pytest.mark.parametrize("audience", [None, "loom-execution"])
def test_job_renderer_maps_ordered_native_sidecar_and_bounded_volumes(audience: str | None) -> None:
    lease = _lease()
    assert lease.runtime_contract_json is not None
    plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    sidecar = SidecarContainerV1(
        role_name="database",
        image_ref="registry.example/database@sha256:" + "d" * 64,
        argv=("/bin/database",),
        resources=ContainerResourcesV1(
            cpu_millis=250,
            memory_mib=256,
            ephemeral_storage_mib=128,
        ),
        startup_probe=ProbeV1(kind="tcp", port=5432),
        readiness_probe=ProbeV1(kind="tcp", port=5432),
    )
    runtime_json = ExecutionRuntimePlanV1.model_validate(
        {
            **plan.canonical_payload(),
            "sidecars": [sidecar.model_dump(mode="json")],
            "image_admission": signed_image_admission_bundle(
                (plan.task_image_ref, plan.runtime_image_ref, sidecar.image_ref)
            ).model_dump(mode="json"),
        }
    ).canonical_payload()
    lease.runtime_contract_json = runtime_json
    lease.runtime_contract_sha256 = canonical_digest(runtime_json)
    lease.workload_requirements_json = {
        **lease.workload_requirements_json,
        "sidecar_count": 1,
    }
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        runtime_class_name="loom-sandbox",
        pod_identity_audience=audience,
    )

    manifest = render_execution_job(lease, target=target)
    pod = manifest["spec"]["template"]["spec"]
    assert [item["name"] for item in pod["initContainers"]] == [
        "runtime-materializer",
        "database",
    ]
    rendered = pod["initContainers"][1]
    assert rendered["restartPolicy"] == "Always"
    assert rendered["startupProbe"]["tcpSocket"] == {"port": 5432}
    expected_volumes = {"runtime", "workspace", "output"}
    if audience is not None:
        expected_volumes.add("execution-identity")
        identity = next(item for item in pod["volumes"] if item["name"] == "execution-identity")
        assert identity["projected"] == {
            "defaultMode": 0o440,
            "sources": [
                {
                    "serviceAccountToken": {
                        "audience": audience,
                        "expirationSeconds": 3600,
                        "path": "token",
                    }
                }
            ],
        }
        execution = pod["containers"][0]
        assert {
            "name": "LOOM_EXECUTION_POD_TOKEN_FILE",
            "value": "/var/run/secrets/loom-execution/token",
        } in execution["env"]
        assert {
            "name": "execution-identity",
            "mountPath": "/var/run/secrets/loom-execution",
            "readOnly": True,
        } in execution["volumeMounts"]
        assert all("subPath" not in mount for mount in execution["volumeMounts"])
    else:
        assert "LOOM_EXECUTION_POD_TOKEN_FILE" not in str(pod)
    assert pod["automountServiceAccountToken"] is False
    assert all(
        mount["name"] != "execution-identity"
        for container in pod["initContainers"]
        for mount in container["volumeMounts"]
    )
    assert {item["name"] for item in pod["volumes"]} == expected_volumes


@pytest.mark.parametrize("independent_controller", [False, True])
def test_private_sandboxes_have_disjoint_filesystems_and_full_resource_request(
    independent_controller: bool,
) -> None:
    from loom.execution_runtime_contract import runtime_pod_resources

    lease = _lease()
    plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    agent_image = "registry.example/agent@sha256:" + "e" * 64
    sidecars = []
    for role in ("task-sandbox", "verifier-sandbox"):
        probe = ProbeV1(
            kind="exec",
            argv=(
                "/loom/bin/loom-sandbox-runtime",
                "--check-socket",
                f"/loom/sandboxes/{role}/sandbox.sock",
            ),
        )
        sidecars.append(
            SidecarContainerV1(
                role_name=role,
                private_sandbox=True,
                image_ref=plan.task_image_ref,
                argv=(
                    "/loom/bin/loom-sandbox-runtime",
                    "--socket",
                    f"/loom/sandboxes/{role}/sandbox.sock",
                ),
                resources=plan.task_resources,
                startup_probe=probe,
                readiness_probe=probe,
            )
        )
    plan = ExecutionRuntimePlanV1.model_validate(
        {
            **plan.canonical_payload(),
            "agent_image_ref": agent_image,
            "sidecars": [s.model_dump(mode="json") for s in sidecars],
            "image_admission": signed_image_admission_bundle(
                (plan.task_image_ref, plan.runtime_image_ref, agent_image)
            ),
        }
    )
    if independent_controller:
        plan = ExecutionRuntimePlanV1.model_validate({
            **plan.canonical_payload(),
            "controller_resources": plan.task_resources.model_copy(
                update={"cpu_millis": 500, "memory_mib": 512}
            ),
        })
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    assert lease.workload_requirements_json["sidecar_count"] == 0
    pod = render_execution_job(
        lease,
        target=ExecutionTargetRuntime(
            target_id=lease.target_id,
            namespace=lease.namespace_name,
            pod_identity_audience="loom-execution",
        ),
    )["spec"]["template"]["spec"]
    assert pod["shareProcessNamespace"] is False
    assert pod["automountServiceAccountToken"] is False
    execution = pod["containers"][0]
    assert execution["image"] == agent_image
    for sidecar in pod["initContainers"][1:]:
        mounts = sidecar["volumeMounts"]
        assert {m["name"] for m in mounts} == {"runtime", f"{sidecar['name']}-socket"}
        binary = next(m for m in mounts if m["name"] == "runtime")
        assert binary == {
            "name": "runtime",
            "mountPath": "/loom/bin/loom-sandbox-runtime",
            "subPath": "loom-sandbox-runtime",
            "readOnly": True,
        }
        for filename in ("hosts", "resolv.conf"):
            network_file = next(
                (m for m in mounts if m["mountPath"] == f"/etc/{filename}"), None,
            )
            assert network_file == {
                "name": f"{sidecar['name']}-socket",
                "mountPath": f"/etc/{filename}",
                "subPath": f"network/{filename}",
            }
        assert {
            "name": f"{sidecar['name']}-socket",
            "mountPath": f"/loom/sandboxes/{sidecar['name']}",
        } in pod["initContainers"][0]["volumeMounts"]
        assert not any(m["mountPath"].startswith("/etc/") for m in execution["volumeMounts"])
        assert sidecar["securityContext"]["readOnlyRootFilesystem"] is False
        assert sidecar["securityContext"]["runAsNonRoot"] is True
        assert sidecar["securityContext"]["capabilities"]["drop"] == ["ALL"]
        assert sidecar["restartPolicy"] == "Always"
        assert (
            len([m for m in execution["volumeMounts"] if m["name"] == f"{sidecar['name']}-socket"])
            == 1
        )
    resources = runtime_pod_resources(plan)
    assert resources.cpu_millis == (
        plan.execution_resources.cpu_millis + 2 * plan.task_resources.cpu_millis
    )
    assert resources.memory_mib == (
        plan.execution_resources.memory_mib + 2 * plan.task_resources.memory_mib
    )
    assert (
        sum(
            int(c["resources"]["requests"]["cpu"][:-1])
            for c in [execution, *pod["initContainers"][1:]]
        )
        == resources.cpu_millis
    )
    assert (
        sum(
            int(c["resources"]["requests"]["memory"][:-2])
            for c in [execution, *pod["initContainers"][1:]]
        )
        == resources.memory_mib
    )
