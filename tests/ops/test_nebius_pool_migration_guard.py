"""Fixed guard transport qualifies the actual controller lineage before exec."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_migration import migration_request


@pytest.fixture
def guard_runtime(monkeypatch, tmp_path):
    import subprocess

    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    request = migration_request()
    target = request.guards[0]
    controller = copy.deepcopy(target.controller)
    controller["metadata"]["generation"] = 1
    controller["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
        "updatedReplicas": 1, "availableReplicas": 1}
    replica = {"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {
        "name": "loom-control-plane-abc", "namespace": target.namespace, "uid": str(uuid4()),
        "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "loom-control-plane",
            "uid": controller["metadata"]["uid"], "controller": True}]}, "spec": {
            "replicas": 1, "template": copy.deepcopy(controller["spec"]["template"])}}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "loom-control-plane-abc-def",
        "namespace": target.namespace, "uid": str(uuid4()), "labels": {"app": "loom-control-plane", "pod-template-hash": "abc"},
        "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": replica["metadata"]["name"],
            "uid": replica["metadata"]["uid"], "controller": True}]}, "spec": copy.deepcopy(controller["spec"]["template"]["spec"]),
        "status": {"phase": "Running", "containerStatuses": [{"name": "loom-control-plane", "ready": True}]}}
    kubeconfig = tmp_path / "config"
    kubeconfig.write_text("private-config")
    kubeconfig.chmod(0o600)
    state = SimpleNamespace(request=request, target=target, controller=controller, replica=replica, pod=pod, kubeconfig=kubeconfig,
        calls=[], status="held", raw=None, extra_pod=False, namespace_drift=False, continuation=False,
        final_drift=False, executed=False)

    def run(command, **kwargs):
        state.calls.append(command)
        assert kwargs["timeout"] == 40 and kwargs["check"] is False
        assert command[:3] == ["/usr/bin/kubectl", "--kubeconfig", str(kubeconfig)]
        position = next(index for index, item in enumerate(command) if item in {"get", "exec"})
        args = command[position:]
        if args[0] == "exec":
            assert args == ["exec", "-n", target.namespace, "pod/" + pod["metadata"]["name"], "-c", "loom-control-plane", "--",
                "python", "-m", "loom.nebius_rollout_guard", args[-5], "--owner", str(request.registration.spec.operation_id),
                "--candidate", request.registration.candidate["candidate_sha"]]
            state.executed = True
            return SimpleNamespace(returncode=0, stdout=state.raw if state.raw is not None else json.dumps({"status": state.status}).encode())
        if args[1] == "namespace":
            name = args[2]
            expected = request.registration.binding.kube_system_uid if name == "kube-system" else str(target.namespace_uid)
            value = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name,
                "uid": str(uuid4()) if state.namespace_drift else expected}}
        elif args[1] == "deployment":
            value = state.controller
        elif args[1] == "replicaset":
            value = state.replica
        else:
            assert args[1] == "pods" and "app=loom-control-plane" in args
            value = {"apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "1",
                "continue": "next" if state.continuation else ""}, "items": [state.pod] * (2 if state.extra_pod else 1)}
            if state.executed and state.final_drift:
                value = copy.deepcopy(value)
                value["items"][0]["metadata"]["uid"] = str(uuid4())
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode())

    monkeypatch.setattr(subprocess, "run", run)
    api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path("/usr/bin/kubectl"))
    return api, state


@pytest.mark.parametrize("action,status", [("observe", "held"), ("acquire", "acquired"), ("acquire", "skipped_busy")])
def test_guard_uses_only_fixed_command_for_exact_running_controller(guard_runtime, action, status):
    api, state = guard_runtime
    state.status = status
    assert api.guard(state.target, action) == {"status": status}
    assert sum("exec" in row for row in state.calls) == 1


@pytest.mark.parametrize("damage", ["namespace", "controller", "template", "owner", "replica_owner", "extra_pod",
    "continuation", "terminating", "not_ready", "release", "target", "config", "final_drift", "bad_report"])
def test_guard_refuses_drift_foreign_lineage_and_unapproved_commands(guard_runtime, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = guard_runtime
    action, target = "observe", state.target
    if damage == "namespace":
        state.namespace_drift = True
    elif damage == "controller":
        state.controller["metadata"]["uid"] = str(uuid4())
    elif damage == "template":
        state.pod["spec"]["containers"][0]["image"] = "foreign:latest"
    elif damage == "owner":
        state.pod["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif damage == "replica_owner":
        state.replica["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif damage in {"extra_pod", "continuation", "final_drift"}:
        setattr(state, damage, True)
    elif damage == "terminating":
        state.pod["metadata"]["deletionTimestamp"] = "2026-09-30T00:00:00Z"
    elif damage == "not_ready":
        state.pod["status"]["containerStatuses"][0]["ready"] = False
    elif damage == "release":
        action = "release"
    elif damage == "target":
        target = replace(target, namespace="foreign")
    elif damage == "config":
        state.kubeconfig.write_text("changed-private-config")
    elif damage == "bad_report":
        state.raw = b"private-marker"
    with pytest.raises(PoolMigrationError) as error:
        api.guard(target, action)
    assert "private-marker" not in str(error.value)
    if damage not in {"final_drift", "bad_report"}:
        assert not state.executed
