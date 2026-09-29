"""Recovery adds one narrowly selected DNS path without rewriting old authority."""
from __future__ import annotations

import copy
import json
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.ops.test_nebius_management_retirement import setup_request as setup_request
from tests.ops.test_nebius_management_stage import PhaseAPI
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


@pytest.fixture
def staging(retirement_request, tmp_path):
    request, _ = retirement_request
    return dict(request=request, original_job_uid=str(uuid4()), api=PhaseAPI(request.binding),
        state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def test_recovery_intent_precedes_dns_and_job_and_replay_never_recreates(staging):
    from scripts.ops.nebius_management_retirement_recovery import stage_recovery

    api = staging["api"]
    create = api.create_resource

    def checked_create(doc):
        assert (staging["anchor_dir"] / (staging["request"].binding.installation_id + ".json")).is_file()
        record = json.loads((staging["state_dir"] / "resources/stage.json").read_bytes())
        item = next(row for row in record["resources"].values() if row["desired"] == doc)
        assert item["status"] == "create_intent" and item["uid"] is None
        if doc["kind"] == "Job":
            assert [row["kind"] for row in api.resources.values()] == ["NetworkPolicy"]
        create(doc)

    api.create_resource = checked_create
    first = stage_recovery(**staging)
    assert len(api.creates) == 2
    frozen = copy.deepcopy(api.resources)
    assert stage_recovery(**staging) == first
    assert api.resources == frozen and len(api.creates) == 2


@pytest.mark.parametrize("damage", ["anchor", "progress", "journal", "missing_job", "replaced_job", "changed_dns"])
def test_recovery_evidence_loss_never_adopts_or_recreates(staging, damage):
    from scripts.ops.nebius_management_retirement_recovery import stage_recovery
    from scripts.ops.nebius_management_stage import ManagementStageError

    stage_recovery(**staging)
    api, state = staging["api"], staging["state_dir"]
    if damage == "anchor":
        (staging["anchor_dir"] / (staging["request"].binding.installation_id + ".json")).unlink()
    elif damage == "progress":
        (state / "recovery.json").unlink()
    elif damage == "journal":
        (state / "resources/stage.json").unlink()
    else:
        key = next(key for key, doc in api.resources.items() if doc["kind"] == "Job")
        if damage == "missing_job":
            del api.resources[key]
        elif damage == "replaced_job":
            api.resources[key]["metadata"]["uid"] = str(uuid4())
        else:
            policy = next(doc for doc in api.resources.values() if doc["kind"] == "NetworkPolicy")
            policy["spec"]["podSelector"] = {}
    before = copy.deepcopy(api.resources)
    with pytest.raises(ManagementStageError):
        stage_recovery(**staging)
    assert len(api.creates) == 2 and api.resources == before
