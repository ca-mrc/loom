"""Protected retirement renders no provisioning or shared service authority."""
from __future__ import annotations

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import ROOT
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def retirement_request(setup_request):
    from scripts.ops.nebius_management_retirement import RetirementInstallRequest

    from loom.nebius_environment_contract import new_environment_registration
    from loom_service.environment_management.retirement import RetirementTarget

    setup, api = setup_request
    row = new_environment_registration(foundation_from(setup.deployment.installation.foundation.platform_config),
        environment_id=uuid4(), incarnation=uuid4(), owner_user_id=uuid4(), owner_team_id=uuid4(), slug="alice-retirement")
    row = type(row).model_validate(row.model_dump() | {"deployment_generation": 2, "desired_state": "destroyed"})
    target = RetirementTarget(operation_id=uuid4(), source_operation_id=uuid4(), registration=row,
        namespace_uids={name: uuid4() for name in row.namespaces})
    return RetirementInstallRequest(binding=setup.binding, deployment=setup.deployment,
        candidate=setup.candidate, profile=setup.profile, targets=(target,), repo_root=ROOT), api


def test_retirement_has_exact_namespace_permissions_and_no_shared_service_identity(retirement_request):
    from scripts.ops.nebius_management_retirement import retirement_documents

    request, _ = retirement_request
    phases = retirement_documents(request)
    documents = [doc for phase in phases.values() for doc in phase.values()]
    job, = [doc for doc in documents if doc["kind"] == "Job"]
    pod = job["spec"]["template"]["spec"]
    assert "app" not in job["spec"]["template"]["metadata"]["labels"]
    assert pod["serviceAccountName"].startswith("loom-retirement-")
    assert pod["restartPolicy"] == "Never" and job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 1800
    container, = pod["containers"]
    assert container["command"] == ["python", "-m", "loom_service.environment_management.retirement"]
    assert container["env"] == [{"name": "LOOM_RETIREMENT_DB_URL", "valueFrom": {
        "secretKeyRef": {"name": "loom-platform-db", "key": "service-url"}}}]
    assert "@sha256:" in container["image"]
    assert {volume["secret"]["secretName"] for volume in pod["volumes"] if "secret" in volume} == {"loom-platform-db"}
    assert not any(doc["kind"] in {"Secret", "Namespace", "PersistentVolumeClaim", "Deployment", "Service"} for doc in documents)
    roles = [doc for doc in documents if doc["kind"] == "Role"]
    assert {doc["metadata"]["namespace"] for doc in roles} == set(request.targets[0].namespace_uids)
    for role in roles:
        for rule in role["rules"]:
            assert "*" not in [*rule["verbs"], *rule["resources"], *rule["apiGroups"]]
            assert not {"secrets", "persistentvolumeclaims", "namespaces", "roles", "rolebindings"} & set(rule["resources"])
            if "delete" in rule["verbs"]:
                assert rule["resources"] == ["pods"]
            if "pods" in rule["resources"]:
                assert "create" not in rule["verbs"]
    cluster_roles = [doc for doc in documents if doc["kind"] == "ClusterRole"]
    assert len(cluster_roles) == 1
    assert cluster_roles[0]["rules"] == [{"apiGroups": [""], "resources": ["namespaces"],
        "resourceNames": sorted(request.targets[0].namespace_uids), "verbs": ["get"]}]


@pytest.mark.parametrize("endpoint,port", [("https://api.cluster.test", 443), ("https://api.cluster.test:6443", 6443)])
def test_retirement_egress_reaches_the_bound_api_port(retirement_request, endpoint, port):
    from scripts.ops.nebius_management_retirement import retirement_documents

    request, _ = retirement_request
    raw = request.deployment.model_dump(mode="json")
    config = json.loads(raw["installation"]["foundation"]["platform_config_json"])
    config["kubernetes_api_server"] = endpoint
    raw["installation"]["foundation"]["platform_config_json"] = json.dumps(config)
    raw["installation"]["applications"]["runtime"]["kubernetes"]["endpoint"] = endpoint
    request = replace(request, deployment=type(request.deployment).model_validate(raw))
    documents = retirement_documents(request)
    policy, = [doc for doc in documents["network"].values() if "Egress" in doc["spec"]["policyTypes"]]
    api_rules = [rule for rule in policy["spec"]["egress"] if "to" not in rule]
    assert api_rules == [{"ports": [{"protocol": "TCP", "port": port}]}]


def test_retirement_stages_replay_without_new_jobs_or_rewriting_shared_objects(retirement_request, tmp_path):
    from scripts.ops.nebius_management_retirement import retirement_ready, stage_retirement

    request, api = retirement_request
    api.key = lambda doc: doc["kind"] + ":" + doc["metadata"].get("namespace", "-") + ":" + doc["metadata"]["name"]
    for phase in ("permissions", "network", "job"):
        args = dict(request=request, phase=phase, api=api, state_dir=tmp_path / phase)
        first = stage_retirement(**args)
        assert stage_retirement(**args) == first
    count = len(api.creates)
    args = dict(request=request, api=api, state_dir=tmp_path / "job")
    assert retirement_ready(**args) is False
    job, = [doc for doc in api.resources.values() if doc["kind"] == "Job"]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    assert retirement_ready(**args) is True
    assert len(api.creates) == count
    job["metadata"]["uid"] = str(uuid4())
    with pytest.raises(RuntimeError, match="identity"):
        retirement_ready(**args)


def test_retirement_rejects_foreign_cluster_before_staging(retirement_request, tmp_path):
    from scripts.ops.nebius_management_retirement import stage_retirement

    request, api = retirement_request
    target, = request.targets
    target = target.model_copy(update={"registration": target.registration.model_copy(update={"cluster_id": "cluster-foreign"})})
    with pytest.raises(RuntimeError, match="binding"):
        stage_retirement(request=replace(request, targets=(target,)), phase="permissions", api=api, state_dir=tmp_path / "state")
    assert not api.creates


def test_connected_retirement_requires_job_completion_and_preserves_lost_stage_evidence(retirement_request, tmp_path):
    from contextlib import nullcontext

    from scripts.ops.nebius_management_retirement import install_retirement

    request, api = retirement_request
    api.key = lambda doc: doc["kind"] + ":" + doc["metadata"].get("namespace", "-") + ":" + doc["metadata"]["name"]
    args = dict(request=request, resources=lambda phase: nullcontext(api), state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")
    assert install_retirement(**args)["status"] == "pending"
    count = len(api.creates)
    job, = [doc for doc in api.resources.values() if doc["kind"] == "Job"]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    assert install_retirement(**args)["status"] == "management_retired"
    assert install_retirement(**args)["status"] == "management_retired"
    assert len(api.creates) == count
    (tmp_path / "state/job/stage.json").unlink()
    with pytest.raises(RuntimeError, match="recovery"):
        install_retirement(**args)
    assert len(api.creates) == count
