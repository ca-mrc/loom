"""Failed installed retirement must be distinguishable from transport failure."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops import nebius_management_preflight as preflight
from tests.ops.test_nebius_management_preflight import Cluster


class RetirementCluster(Cluster):
    namespace = "loom-nebius-management"
    job_name = "loom-retirement-6ab141b5cd71"
    container_name = "loom-platform-migrate-840d0a1e211b"

    def __init__(self):
        super().__init__()
        self.raw = 'private-logs\n{"status":"retirement_blocked","detail":"private-secret"}\n'
        self.job = {"metadata": {"uid": "job-uid"}, "status": {
            "conditions": [{"type": "Failed", "status": "True"}]}}
        self.pod = {"metadata": {"namespace": self.namespace, "name": self.job_name + "-h4t2c", "uid": "pod-uid",
            "labels": {"loom.nebius/retirement": self.job_name},
            "ownerReferences": [{"kind": "Job", "name": self.job_name, "uid": "job-uid", "controller": True}]},
            "spec": {"containers": [{"name": self.container_name, "command": [
                "python", "-m", "loom_service.environment_management.retirement"]}]},
            "status": {"phase": "Failed", "containerStatuses": [{"name": self.container_name,
                "state": {"terminated": {"exitCode": 1, "signal": 0, "reason": "Error", "message": "private-secret"}}}]}}
        self.lists["pods"].append(self.pod)

    def get(self, kind, name, namespace):
        if kind == "job":
            self.calls.append(("get", kind, name, namespace))
            assert (name, namespace) == (self.job_name, self.namespace)
            return copy.deepcopy(self.job)
        return super().get(kind, name, namespace)

    def run(self, *args, **kwargs):
        if args[0] == "logs":
            self.calls.append(args)
            assert args == ("logs", self.pod["metadata"]["name"], "-n", self.namespace, "-c", self.container_name,
                            "--tail=50", "--limit-bytes=16384")
            assert kwargs == {"timeout": 40}
            return self.raw
        return super().run(*args, **kwargs)


def inspect(cluster):
    return preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")


def test_inspection_exposes_failed_retirement_identity_and_safe_termination_not_secret_messages():
    cluster = RetirementCluster()
    result = inspect(cluster)
    assert result["failed_retirement_jobs"] == [{"namespace": cluster.namespace, "job": cluster.job_name,
        "job_uid": "job-uid", "pod": cluster.pod["metadata"]["name"], "pod_uid": "pod-uid",
        "container": cluster.container_name, "termination": {"exit_code": 1, "signal": 0, "reason": "Error"},
        "diagnostic": {"status": "retirement_blocked"}, "registry_probe": {
            "status": "unavailable", "stage": "configuration_identity", "error_type": "KeyError"}}]
    assert "private-" not in json.dumps(result)
    assert all(call[0] in {"get", "config", "logs"} for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["namespace", "owner", "label", "command", "uid", "running_job"])
def test_unqualified_retirement_pods_never_trigger_log_reads(mutation):
    cluster = RetirementCluster()
    if mutation == "namespace":
        cluster.pod["metadata"]["namespace"] = "foreign"
    elif mutation == "owner":
        cluster.pod["metadata"]["ownerReferences"][0]["controller"] = False
    elif mutation == "label":
        cluster.pod["metadata"]["labels"]["loom.nebius/retirement"] = "foreign"
    elif mutation == "command":
        cluster.pod["spec"]["containers"][0]["command"] = ["private-command"]
    elif mutation == "uid":
        cluster.job["metadata"]["uid"] = "replacement"
    else:
        cluster.job["status"] = {"active": 1}
    result = inspect(cluster)
    assert result["failed_retirement_jobs"] == []
    assert not any(call[0] == "logs" for call in cluster.calls)


@pytest.mark.parametrize("raw,reason,exit_code,diagnostic,termination", [
    ("private-secret", "OOMKilled", 137, {"status": "unavailable"}, {"reason": "OOMKilled", "exit_code": 137, "signal": 0}),
    ('{"status":"private-secret"}', "private-secret", "private-secret", {"status": "unavailable"}, {"reason": "Other", "signal": 0}),
    ('ModuleNotFoundError: private-module', "Error", 1, {"error_type": "ModuleNotFoundError"}, {"reason": "Error", "exit_code": 1, "signal": 0}),
])
def test_retirement_diagnostics_export_only_allowlisted_values(raw, reason, exit_code, diagnostic, termination):
    cluster = RetirementCluster()
    cluster.raw = raw
    cluster.pod["status"]["containerStatuses"][0]["state"]["terminated"].update(reason=reason, exitCode=exit_code)
    result = inspect(cluster)
    failure, = result["failed_retirement_jobs"]
    assert failure["diagnostic"] == diagnostic
    assert failure["termination"] == termination
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("stale", [False, True])
def test_retirement_inspection_bounds_both_job_and_log_requests(stale):
    cluster = RetirementCluster()
    cluster.lists["pods"].extend(copy.deepcopy(cluster.pod) for _ in range(8))
    if stale:
        cluster.job["metadata"]["uid"] = "replacement"
    result = inspect(cluster)
    assert len([call for call in cluster.calls if call[:2] == ("get", "job")]) == 3
    assert len([call for call in cluster.calls if call[0] == "logs"]) == (0 if stale else 3)
    assert len(result["failed_retirement_jobs"]) == (0 if stale else 3)


def test_unavailable_retirement_logs_preserve_safe_termination_evidence():
    class Unreadable(RetirementCluster):
        def run(self, *args, **kwargs):
            if args[0] == "logs":
                raise RuntimeError("private-transport-secret")
            return super().run(*args, **kwargs)

    result = inspect(Unreadable())
    assert result["status"] == "observed"
    failure, = result["failed_retirement_jobs"]
    assert failure["diagnostic"] == {"status": "unavailable"}
    assert failure["termination"]["exit_code"] == 1
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("raw", ['[]', '{"status":[]}', '[' * 4000 + ']' * 4000,
    '{"status":"retirement_blocked"}\n' + 'x' * 16384])
def test_malformed_or_outside_bounded_tail_is_not_retirement_evidence(raw):
    cluster = RetirementCluster()
    cluster.raw = raw
    assert inspect(cluster)["failed_retirement_jobs"][0]["diagnostic"] == {"status": "unavailable"}
