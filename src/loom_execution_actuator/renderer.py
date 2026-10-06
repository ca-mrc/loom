from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import GUEST_LAUNCHER_STORAGE_MIB, WorkloadRequirementsV1
from loom.execution_image_admission import (
    ImageAdmissionError,
    validate_execution_image_admission_bundle,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    RuntimeComposition,
    SidecarContainerV1,
    validate_runtime_plan_requirements,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.sandbox_identity import ROOT_INSTALL_CAPABILITIES
from loom_execution_actuator.contracts import ActuatorContractError

_DIGEST_IMAGE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class ExecutionTargetRuntime:
    target_id: str
    namespace: str
    runtime_class_name: str | None = None
    node_selector: dict[str, str] | None = None
    tolerations: tuple[dict[str, str], ...] = ()
    service_account_name: str = "loom-execution-attempt"
    pod_identity_audience: str | None = None
    credential_broker_url: str = (
        "http://loom-llm-gateway.loom.svc.cluster.local:9100/internal/service-execution"
    )

    def __post_init__(self) -> None:
        if not self.target_id or not self.namespace:
            raise ValueError("target and namespace are required")
        if self.pod_identity_audience is not None and (
            not self.pod_identity_audience.strip()
            or self.pod_identity_audience != self.pod_identity_audience.strip()
        ):
            raise ValueError("Pod identity audience must be nonempty")
        if not self.credential_broker_url.startswith(("http://", "https://")):
            raise ValueError("credential broker URL must be HTTP(S)")


def _required_positive(requirements: dict[str, Any], key: str) -> int:
    value = requirements.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ActuatorContractError(f"execution requirement {key} must be positive")
    return value


def _resources(
    *, cpu_millis: int, memory_mib: int, storage_mib: int,
    request: ContainerResourcesV1 | None = None,
) -> dict[str, Any]:
    values = {
        "cpu": f"{cpu_millis}m",
        "memory": f"{memory_mib}Mi",
        "ephemeral-storage": f"{storage_mib}Mi",
    }
    requested = values.copy() if request is None else {
        "cpu": f"{request.cpu_millis}m",
        "memory": f"{request.memory_mib}Mi",
        "ephemeral-storage": f"{request.ephemeral_storage_mib}Mi",
    }
    return {"requests": requested, "limits": values.copy()}


def _security_context(*, read_only_root: bool = True) -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": read_only_root,
        "runAsNonRoot": True,
    }


def _probe(value: ProbeV1) -> dict[str, Any]:
    result: dict[str, Any] = {
        "timeoutSeconds": value.timeout_seconds,
        "periodSeconds": value.period_seconds,
        "failureThreshold": value.failure_threshold,
    }
    if value.initial_delay_seconds:
        result["initialDelaySeconds"] = value.initial_delay_seconds
    if value.kind == "http":
        result["httpGet"] = {"port": value.port, "path": value.path}
    elif value.kind == "tcp":
        result["tcpSocket"] = {"port": value.port}
    else:
        result["exec"] = {"command": list(value.argv)}
    return result


def _sidecar(
    value: SidecarContainerV1, *, request: ContainerResourcesV1 | None = None,
    max_artifact_bytes: int | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": value.role_name,
        "image": value.image_ref,
        "imagePullPolicy": "IfNotPresent",
        "command": list(value.argv),
        "env": [{"name": name, "value": item} for name, item in sorted(value.environment.items())],
        "resources": _resources(
            cpu_millis=value.resources.cpu_millis,
            memory_mib=value.resources.memory_mib,
            storage_mib=value.resources.ephemeral_storage_mib,
            request=request,
        ),
        "restartPolicy": "Always",
        "startupProbe": _probe(value.startup_probe),
        "readinessProbe": _probe(value.readiness_probe),
        "securityContext": _security_context(),
        "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}],
    }
    if value.task_fixture:
        result["volumeMounts"] = []
        result["securityContext"].update(runAsUser=65532, runAsGroup=65532)
    elif value.private_sandbox:
        result["securityContext"] = _security_context(read_only_root=False)
        if value.identity is not None:
            identity = value.identity
            result["securityContext"].update(
                runAsUser=identity.run_as_user, runAsGroup=identity.run_as_group,
                runAsNonRoot=identity.run_as_user != 0,
            )
            if identity.run_as_user == 0:
                result["securityContext"]["capabilities"]["add"] = list(ROOT_INSTALL_CAPABILITIES)
            result["env"].append({"name": "HOME", "value": identity.home})
        result["volumeMounts"] = [
            {
                "name": f"{value.role_name}-socket",
                "mountPath": f"/loom/sandboxes/{value.role_name}",
            },
            {
                "name": "runtime",
                "mountPath": "/loom/bin/loom-sandbox-runtime",
                "subPath": "loom-sandbox-runtime",
                "readOnly": True,
            },
            *[
                {
                    "name": f"{value.role_name}-socket",
                    "mountPath": f"/etc/{filename}",
                    "subPath": f"network/{filename}",
                }
                for filename in ("hosts", "resolv.conf")
            ],
        ]
        if value.guest_execution is not None:
            if max_artifact_bytes is None:
                raise ActuatorContractError("guest execution requires a frozen transfer limit")
            result["securityContext"]["readOnlyRootFilesystem"] = True
            result["securityContext"]["capabilities"]["add"] = ["DAC_OVERRIDE"]
            result["volumeMounts"].extend([
                {"name": "runtime", "mountPath": "/loom/runtime", "readOnly": True},
                {"name": f"{value.role_name}-guest-state", "mountPath": "/loom/guest-state"},
            ])
            result["command"] = [
                "/loom/runtime/guest/bin/loom-guest-runtime", "--payload", "/loom/runtime/guest",
                "--root", "/", "--state", "/loom/guest-state/incarnation",
                "--socket", f"/loom/sandboxes/{value.role_name}/sandbox.sock",
                "--memory-mib", str(value.resources.memory_mib),
                "--storage-mib", str(value.resources.ephemeral_storage_mib - GUEST_LAUNCHER_STORAGE_MIB),
                "--cpu-millis", str(value.resources.cpu_millis),
                "--max-transfer-bytes", str(max_artifact_bytes),
                "--exec-timeout-seconds", value.argv[-1],
            ]
            if "nested_docker" in value.guest_execution.capabilities:
                result["command"].append("--nested-docker")
    return result


def _runtime_plan(lease: ServiceExecutionLease) -> ExecutionRuntimePlanV1:
    if lease.runtime_contract_json is None or lease.runtime_contract_sha256 is None:
        raise ActuatorContractError("execution lease has no immutable runtime contract")
    if canonical_digest(lease.runtime_contract_json) != lease.runtime_contract_sha256:
        raise ActuatorContractError("execution runtime contract digest does not match lease")
    try:
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        requirements = WorkloadRequirementsV1.model_validate(lease.workload_requirements_json)
        validate_runtime_plan_requirements(plan, requirements)
    except (ValidationError, ValueError) as exc:
        raise ActuatorContractError(f"invalid execution runtime contract: {exc}") from exc
    if plan.execution_class_id != lease.execution_class_id:
        raise ActuatorContractError("runtime contract execution class does not match lease")
    if plan.execution_role != lease.execution_role:
        raise ActuatorContractError("runtime contract execution role does not match lease")
    if plan.composition != RuntimeComposition.INIT_PAYLOAD:
        raise ActuatorContractError("runtime composition is not supported by this actuator")
    return plan


def render_execution_job(
    lease: ServiceExecutionLease,
    *,
    target: ExecutionTargetRuntime,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Render one immutable Pod-native execution unit from its durable lease."""

    if lease.target_id != target.target_id:
        raise ActuatorContractError("lease target does not match actuator target")
    if lease.namespace_name != target.namespace:
        raise ActuatorContractError("lease namespace does not match actuator namespace")
    requirements = lease.workload_requirements_json
    image_ref = requirements.get("image_ref")
    if not isinstance(image_ref, str) or not _DIGEST_IMAGE.fullmatch(image_ref):
        raise ActuatorContractError("execution image must be immutable by sha256 digest")
    _required_positive(requirements, "cpu_millis")
    _required_positive(requirements, "memory_mib")
    _required_positive(requirements, "ephemeral_storage_mib")
    plan = _runtime_plan(lease)
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        validate_execution_image_admission_bundle(
            plan.image_admission,
            required_image_refs=plan.published_image_refs(),
            now=current_time,
        )
    except ImageAdmissionError as exc:
        raise ActuatorContractError(str(exc)) from exc
    active_deadline = int((lease.deadline_at - current_time).total_seconds())
    if active_deadline <= 0:
        raise ActuatorContractError("execution deadline has expired")
    target_label = canonical_digest(
        {"schema_version": "loom.target-label.v1", "target_id": target.target_id},
        persisted=False,
    ).removeprefix("sha256:")[:32]
    labels = {
        "app.kubernetes.io/managed-by": "loom-execution-actuator",
        "app.kubernetes.io/component": "execution-unit",
        "loom.openai.com/lease-id": str(lease.id),
        "loom.openai.com/generation": str(lease.resource_generation),
        "loom.openai.com/target": target_label,
    }
    annotations = {
        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
        "loom.openai.com/schema-version": "loom.execution-job.v1",
        "loom.openai.com/target-id": target.target_id,
        "loom.openai.com/execution-unit-key": str(lease.execution_unit_key),
        "loom.openai.com/workload-requirements-sha256": lease.workload_requirements_sha256,
        "loom.openai.com/runtime-contract-sha256": lease.runtime_contract_sha256,
        "loom.openai.com/candidate-sha": plan.candidate_sha,
        "loom.openai.com/task-revision-sha256": plan.task_revision_sha256,
        "loom.openai.com/command-identity-sha256": plan.command_identity_sha256,
        "loom.openai.com/execution-role": plan.execution_role,
        "loom.openai.com/image-admission-sha256": canonical_digest(
            plan.image_admission.model_dump(mode="json")
        ),
    }
    if lease.parent_lease_id is not None:
        annotations["loom.openai.com/parent-lease-id"] = str(lease.parent_lease_id)
    annotations["loom.openai.com/container-roles"] = ",".join(
        [
            "execution",
            plan.main.role,
            *[sidecar.role_name for sidecar in plan.sidecars],
            *(["verifier"] if plan.verifier is not None else []),
        ]
    )
    resources = _resources(
        cpu_millis=plan.execution_resources.cpu_millis,
        memory_mib=plan.execution_resources.memory_mib,
        storage_mib=plan.execution_resources.ephemeral_storage_mib,
        request=plan.container_request("execution"),
    )
    plan_payload = canonical_document(plan.canonical_payload())
    encoded_plan = base64.urlsafe_b64encode(plan_payload).rstrip(b"=").decode("ascii")
    runtime_mount = {"name": "runtime", "mountPath": "/loom/runtime"}
    execution_runtime_mount = {**runtime_mount, "readOnly": True}
    workspace_mount = {"name": "workspace", "mountPath": "/workspace"}
    output_mount = {"name": "output", "mountPath": "/loom/output"}
    init_containers = [
        {
            "name": "runtime-materializer",
            "image": plan.runtime_image_ref,
            "imagePullPolicy": "IfNotPresent",
            "command": [
                "/loom-execution-runtime",
                "materialize",
                "--encoded-plan",
                encoded_plan,
            ],
            "resources": _resources(cpu_millis=50, memory_mib=64, storage_mib=32),
            "securityContext": _security_context(),
            "volumeMounts": [runtime_mount],
        },
        *[_sidecar(sidecar, request=plan.container_request(sidecar.role_name),
                   max_artifact_bytes=plan.max_artifact_bytes)
          for sidecar in plan.sidecars],
    ]
    output_mib = max(
        1,
        (plan.max_artifact_bytes + 2 * plan.max_log_bytes_per_stream + 1_048_575) // 1_048_576,
    )
    job: dict[str, Any] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": lease.job_name,
            "namespace": target.namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "activeDeadlineSeconds": active_deadline,
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "shareProcessNamespace": False,
                    "enableServiceLinks": False,
                    "serviceAccountName": target.service_account_name,
                    "runtimeClassName": target.runtime_class_name,
                    "nodeSelector": dict(target.node_selector or {}),
                    "tolerations": [dict(item) for item in target.tolerations],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": plan.run_as_user,
                        "runAsGroup": plan.run_as_group,
                        "fsGroup": plan.fs_group,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "terminationGracePeriodSeconds": plan.termination_grace_seconds,
                    "volumes": [
                        {
                            "name": "runtime",
                            "emptyDir": {"sizeLimit": f"{plan.runtime_volume_mib}Mi"},
                        },
                        {
                            "name": "workspace",
                            "emptyDir": {"sizeLimit": f"{plan.workspace_mib}Mi"},
                        },
                        {"name": "output", "emptyDir": {"sizeLimit": f"{output_mib}Mi"}},
                    ],
                    "initContainers": init_containers,
                    "containers": [
                        {
                            "name": "execution",
                            "image": plan.agent_image_ref or plan.task_image_ref,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/loom/runtime/loom-execution-runtime"],
                            "args": [
                                "--plan",
                                "/loom/runtime/execution-plan.json",
                                "--workspace",
                                "/workspace",
                                "--output-root",
                                "/loom/output",
                            ],
                            "resources": resources,
                            "securityContext": _security_context(read_only_root=False),
                            "volumeMounts": [
                                execution_runtime_mount,
                                workspace_mount,
                                output_mount,
                            ],
                            "terminationMessagePath": "/loom/output/termination-message",
                            "terminationMessagePolicy": "File",
                            "env": [
                                {"name": "LOOM_EXECUTION_LEASE_ID", "value": str(lease.id)},
                                {
                                    "name": "LOOM_EXECUTION_GENERATION",
                                    "value": str(lease.generation),
                                },
                                {
                                    "name": "LOOM_EXECUTION_ROLE",
                                    "value": lease.execution_role,
                                },
                                {
                                    "name": "LOOM_EXECUTION_BROKER_URL",
                                    "value": target.credential_broker_url,
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }
    pod = job["spec"]["template"]["spec"]
    for sidecar in plan.sidecars:
        if sidecar.guest_execution is not None:
            pod["volumes"].append({
                "name": f"{sidecar.role_name}-guest-state",
                "emptyDir": {"sizeLimit": f"{sidecar.resources.ephemeral_storage_mib}Mi"},
            })
        if sidecar.private_sandbox:
            name = f"{sidecar.role_name}-socket"
            pod["volumes"].append({"name": name, "emptyDir": {"sizeLimit": "1Mi"}})
            # Seed independent copies before an untrusted process starts. The
            # kubelet's implicit network files can otherwise be shared by Pod
            # containers, even when their image filesystems are separate.
            pod["initContainers"][0]["volumeMounts"].append(
                {"name": name, "mountPath": f"/loom/sandboxes/{sidecar.role_name}"}
            )
            pod["containers"][0]["volumeMounts"].append(
                {"name": name, "mountPath": f"/loom/sandboxes/{sidecar.role_name}"}
            )
    if target.pod_identity_audience is not None:
        pod = job["spec"]["template"]["spec"]
        pod["volumes"].append(
            {
                "name": "execution-identity",
                "projected": {
                    "defaultMode": 0o440,
                    "sources": [
                        {
                            "serviceAccountToken": {
                                "audience": target.pod_identity_audience,
                                "expirationSeconds": 3600,
                                "path": "token",
                            }
                        }
                    ],
                },
            }
        )
        execution = pod["containers"][0]
        execution["volumeMounts"].append(
            {
                "name": "execution-identity",
                "mountPath": "/var/run/secrets/loom-execution",
                "readOnly": True,
            }
        )
        execution["env"].append(
            {
                "name": "LOOM_EXECUTION_POD_TOKEN_FILE",
                "value": "/var/run/secrets/loom-execution/token",
            }
        )
    if target.runtime_class_name is None:
        del job["spec"]["template"]["spec"]["runtimeClassName"]
    if not target.node_selector:
        del job["spec"]["template"]["spec"]["nodeSelector"]
    if not target.tolerations:
        del job["spec"]["template"]["spec"]["tolerations"]
    return job


__all__ = ["ExecutionTargetRuntime", "render_execution_job"]
