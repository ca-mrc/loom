"""Shared reservation identity, absolute deadline and accounting for native Jobs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_execution_capacity_collector.kubernetes import rendered_pod_resources


@dataclass(frozen=True)
class PreparedPoolNativeBuild:
    request_sha256: str
    resources: ResourceTotals
    pod_slots: int
    namespace_uid: UUID
    lease_epoch: int
    configmap: dict[str, Any]
    job: dict[str, Any]


def bind_native_build(*, configmap: dict[str, Any], job: dict[str, Any], reservation_id: UUID,
                      target: ExecutionTargetRuntime, namespace_uid: UUID, lease_epoch: int,
                      deadline_at: datetime, request_sha256: str,
                      runtime_class_overhead: ResourceTotals | None) -> PreparedPoolNativeBuild:
    """Wrap freshly rendered documents; keep workload-specific claims untouched."""
    # Each reservation owns a unique Job even after an unstarted reselection.
    name = f"loom-pool-{reservation_id.hex}"
    for document in (configmap, job):
        document["metadata"]["name"] = name
    for metadata in (configmap["metadata"], job["metadata"], job["spec"]["template"]["metadata"]):
        metadata["labels"]["app.kubernetes.io/managed-by"] = "loom-pool-gateway"
        metadata.setdefault("annotations", {})["loom.openai.com/target-id"] = target.target_id
    pod = job["spec"]["template"]["spec"]
    # activeDeadlineSeconds starts at Job startup, not original admission.
    # Every phase therefore enforces the retained absolute cutoff as PID1.
    pod["volumes"].append({"name": "deadline-runtime", "emptyDir": {"sizeLimit": "8Mi"}})
    for phase in [*pod["initContainers"], *pod["containers"]]:
        runtime = "/usr/local/bin/loom-build-deadline"
        extra: list[str] = []
        if phase["name"] in {"prepare", "build"}:
            phase["volumeMounts"].append({"name": "deadline-runtime", "mountPath": "/loom/deadline-runtime",
                                         "readOnly": phase["name"] == "build"})
        if phase["name"] == "prepare":
            extra.append("--install-runtime")
        elif phase["name"] == "build":
            runtime = "/loom/deadline-runtime/loom-build-deadline"
        phase["command"] = [runtime, "--deadline-at", deadline_at.isoformat(), *extra, "--", *phase["command"]]
    for volume in pod["volumes"]:
        if volume["name"] == "claim":
            volume["configMap"]["name"] = name
    if (job["spec"]["parallelism"] != 1 or job["spec"]["completions"] != 1
            or pod["nodeSelector"] != target.node_selector
            or not pod["nodeSelector"].get("nebius.com/node-group-id")
            or (target.runtime_class_name is None) != (runtime_class_overhead is None)):
        raise ValueError("pool_build_physical_profile_mismatch")
    accounting = dict(pod)
    if runtime_class_overhead is not None:
        overhead = runtime_class_overhead
        accounting["overhead"] = {"cpu": f"{overhead.cpu_millis}m", "memory": f"{overhead.memory_mib}Mi",
                                  "ephemeral-storage": f"{overhead.storage_mib}Mi"}
    return PreparedPoolNativeBuild(request_sha256=request_sha256, resources=rendered_pod_resources(accounting),
        pod_slots=1, namespace_uid=namespace_uid, lease_epoch=lease_epoch, configmap=configmap, job=job)
