"""Recovery adds one narrowly selected DNS path without rewriting old authority."""
from __future__ import annotations

import copy
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.ops.test_nebius_management_retirement import setup_request as setup_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def test_recovery_adds_only_a_new_bound_job_and_its_dns_permission(retirement_request):
    from scripts.ops.nebius_management_retirement import retirement_documents
    from scripts.ops.nebius_management_retirement_recovery import recovery_documents

    request, _ = retirement_request
    original = copy.deepcopy(retirement_documents(request))
    original_job, = [doc for doc in original["job"].values() if doc["kind"] == "Job"]
    uid = str(uuid4())
    result = recovery_documents(request, original_job_uid=uid)
    assert result == recovery_documents(request, original_job_uid=uid)
    assert retirement_documents(request) == original
    assert set(result) == {"network", "job"}
    job, = result["job"].values()
    policy, = result["network"].values()
    assert job["kind"] == "Job" and policy["kind"] == "NetworkPolicy"
    assert job["metadata"]["name"].startswith("loom-retirement-recovery-")
    assert job["metadata"]["name"] != original_job["metadata"]["name"]
    assert job["metadata"]["annotations"] == {"loom.nebius/recovery-of-job-uid": uid}
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["activeDeadlineSeconds"] == 1800
    assert "ttlSecondsAfterFinished" not in job["spec"]
    original_labels = original_job["spec"]["template"]["metadata"]["labels"]
    labels = job["spec"]["template"]["metadata"]["labels"]
    assert labels == {**original_labels, "loom.nebius/retirement-recovery": job["metadata"]["name"]}
    wanted = copy.deepcopy(original_job["spec"]["template"]["spec"])
    actual = copy.deepcopy(job["spec"]["template"]["spec"])
    command = actual["containers"][0].pop("command")
    wanted["containers"][0].pop("command")
    assert actual == wanted
    assert command[:2] == ["python", "-c"] and len(command) == 3
    assert policy["spec"] == {
        "podSelector": {"matchLabels": {"loom.nebius/management-installation": request.binding.installation_id,
            "loom.nebius/retirement-recovery": job["metadata"]["name"]}},
        "policyTypes": ["Egress"], "egress": [{
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                    "podSelector": {"matchExpressions": [{"key": "k8s-app", "operator": "In",
                        "values": ["kube-dns", "coredns"]}]}}],
            "ports": [{"protocol": "TCP", "port": 53}, {"protocol": "UDP", "port": 53}],
        }],
    }
    assert recovery_documents(request, original_job_uid=str(uuid4())) != result


@pytest.mark.parametrize("uid", ["", "not-a-uid", "00000000-0000-0000-0000-000000000000"])
def test_recovery_requires_original_job_uid(retirement_request, uid):
    from scripts.ops.nebius_management_retirement_recovery import recovery_documents

    with pytest.raises(ValueError):
        recovery_documents(retirement_request[0], original_job_uid=uid)
