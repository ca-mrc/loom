"""Actual cgroup isolation using the same frozen/rendered container allocation."""

import os
import time

import pytest

from loom.execution_failure_diagnosis import execution_failure_diagnosis
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import ContainerResourcesV1, ExecutionRuntimePlanV1
from loom_execution_actuator.renderer import _resources
from loom_service.diagnosis import build_trial_diagnosis
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_execution_resource_allocation import _plan

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires an explicitly enabled disposable Kubernetes test",
)


def test_node_share_memory_is_enforced_and_oom_has_kernel_evidence(capsys):
    # Scale memory down for a cheap local check; production 4 -> 7 GiB is also
    # checked through the full renderer. This is not the original paid task.
    _, _, plan = _plan(64)
    payload = plan.canonical_payload()
    payload["controller_resources"]["memory_mib"] = 32
    plan = ExecutionRuntimePlanV1.model_validate(payload)
    allocated = allocate_node_resources(plan, target_id="local-probe", usable_node=ContainerResourcesV1(
        cpu_millis=16_000, memory_mib=16 * 224, ephemeral_storage_mib=512 * 1024,
    ))
    assert allocated.task_resources.memory_mib == 96
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        core.create_namespaced_service_account("default", {
            "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "memory-probe"},
        })
        results = {}
        for name, memory in (("declared", 64), ("allocated", allocated.task_resources.memory_mib)):
            resources = _resources(cpu_millis=1000, memory_mib=memory, storage_mib=128)
            core.create_namespaced_pod("default", {
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": name},
                "spec": {"restartPolicy": "Never", "serviceAccountName": "memory-probe", "automountServiceAccountToken": False,
                    "containers": [{"name": "task-sandbox", "image": "python:3.11-slim",
                        "resources": resources, "command": ["python", "-c",
                            "import time; x=bytearray(80*1024*1024); print(len(x), flush=True); time.sleep(1)"],
                    }]},
            })
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and len(results) < 2:
            for name in ("declared", "allocated"):
                pod = core.read_namespaced_pod(name, "default")
                if pod.status.phase in {"Failed", "Succeeded"}:
                    results[name] = pod
            if len(results) < 2:
                time.sleep(1)
        assert len(results) == 2, {p.metadata.name: p.status.to_dict() for p in core.list_namespaced_pod("default").items}
        assert results["allocated"].status.phase == "Succeeded"
        assert results["allocated"].spec.containers[0].resources.requests["memory"] == "96Mi"
        assert results["allocated"].spec.containers[0].resources.limits["memory"] == "96Mi"
        ending = results["declared"].status.container_statuses[0].state.terminated
        assert ending.reason == "OOMKilled" and ending.exit_code == 137
        event = {"ordinal": 1, "payload": {
            "normalized_state": "oom_killed", "job_uid": "local-probe",
            "pod_uid": results["declared"].metadata.uid, "reason": "SandboxTerminated",
            "container_diagnostics": [{"name": "task-sandbox", "restart_count": 0,
                "current_termination": {"reason": ending.reason, "exit_code": ending.exit_code,
                    "started_at": ending.started_at.isoformat(), "finished_at": ending.finished_at.isoformat()}}],
        }}
        diagnosis = execution_failure_diagnosis([event], plan=plan, job_uid="local-probe",
                                                pod_uid=results["declared"].metadata.uid)
        assert diagnosis is not None and diagnosis["memory_limit_mib"] == 64
        report = build_trial_diagnosis({"entity": {"type": "trial", "id": "local-probe"},
            "failure": {"reason_code": "trial.oom_killed", "platform_outcome": "failed"},
            "execution_failure": diagnosis})
        from loom_cli.eval_cmd import _print_diagnosis_report
        _print_diagnosis_report(report)
        assert "OOMKilled" in capsys.readouterr().out
    finally:
        container.stop()
