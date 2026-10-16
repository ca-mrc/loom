"""Fixed management authority persists once and cannot adopt or expand grants."""
from __future__ import annotations

import copy
import json
import ssl
from uuid import uuid4

import httpx
import pytest
from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI

from loom.nebius_management_authority import ManagementNamespaceAuthority


@pytest.fixture
def inputs():
    from scripts.ops.nebius_management_material import ManagementBinding

    authority = ManagementNamespaceAuthority(installation_id=uuid4(), namespace="loom-nebius-management")
    binding = ManagementBinding(str(authority.installation_id), authority.namespace, str(uuid4()), str(uuid4()))
    return authority, binding, PhaseAPI(binding)


def stage(inputs, state):
    from scripts.ops.nebius_management_authority_stage import stage_management_authority

    authority, binding, api = inputs
    return stage_management_authority(authority=authority, binding=binding, api=api, state_dir=state)


def test_fixed_authority_preserves_uids_on_replay(inputs, tmp_path):
    first = stage(inputs, tmp_path / "state")
    assert stage(inputs, tmp_path / "state") == first
    assert len(inputs[2].creates) == 9
    assert inputs[2].creates[-1].endswith("-bootstrap")
    assert first["status"] == "management_authority_staged"
    assert len(first["resource_uids"]) == 9
    assert "rules" not in json.dumps(first)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_authority_unknown_create_never_repeats(inputs, tmp_path, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].failure = failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ManagementStageError, match="unresolved"):
                stage(inputs, tmp_path / "state")
        assert len(inputs[2].creates) == 1
    else:
        first = stage(inputs, tmp_path / "state")
        assert stage(inputs, tmp_path / "state") == first
        assert len(inputs[2].creates) == 9


def test_authority_preflights_late_collision_before_any_grant(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    name = "ClusterRoleBinding:" + inputs[0].name + "-bootstrap"
    inputs[2].resources[name] = {"metadata": {"uid": str(uuid4())}}
    with pytest.raises(ManagementStageError, match="untracked"):
        stage(inputs, tmp_path / "state")
    assert not inputs[2].creates


def test_recovery_preflights_every_resource_before_resuming_writes(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / "state"
    original = inputs[2].verify_identity

    def fail_after_preparation(binding):
        original(binding)
        if (state / "stage.json").exists():
            raise RuntimeError("interruption before first create")

    inputs[2].verify_identity = fail_after_preparation
    with pytest.raises(ManagementStageError):
        stage(inputs, state)
    assert not inputs[2].creates
    inputs[2].verify_identity = original
    key = "ClusterRoleBinding:" + inputs[0].name + "-bootstrap"
    inputs[2].resources[key] = {"metadata": {"uid": str(uuid4())}}
    with pytest.raises(ManagementStageError, match="untracked"):
        stage(inputs, state)
    assert not inputs[2].creates


@pytest.mark.parametrize("change", ["missing", "uid", "grant"])
def test_authority_drift_cannot_be_repaired_implicitly(inputs, tmp_path, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    stage(inputs, tmp_path / "state")
    key = "ClusterRole:" + inputs[0].name + "-bootstrap"
    if change == "missing":
        del inputs[2].resources[key]
    elif change == "uid":
        inputs[2].resources[key]["metadata"]["uid"] = str(uuid4())
    else:
        inputs[2].resources[key]["rules"][0]["verbs"].append("delete")
    with pytest.raises(ManagementStageError):
        stage(inputs, tmp_path / "state")
    assert len(inputs[2].creates) == 9


def test_authority_identity_must_match_owned_namespace(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs = (inputs[0].model_copy(update={"installation_id": uuid4()}), *inputs[1:])
    with pytest.raises(ManagementStageError, match="binding"):
        stage(inputs, tmp_path / "state")
    assert not inputs[2].creates


def test_authority_readiness_requires_current_checked_policies_without_writes(inputs, tmp_path):
    from scripts.ops.nebius_management_authority_stage import management_authority_ready
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / "state"
    stage(inputs, state)
    args = dict(authority=inputs[0], binding=inputs[1], api=inputs[2], state_dir=state)
    assert management_authority_ready(**args) is False
    for doc in inputs[2].resources.values():
        if doc["kind"] == "ValidatingAdmissionPolicy":
            doc["metadata"]["generation"] = 2
            doc["status"] = {"observedGeneration": 2, "typeChecking": {"expressionWarnings": []}}
    assert management_authority_ready(**args) is True
    policy = next(doc for doc in inputs[2].resources.values() if doc["kind"] == "ValidatingAdmissionPolicy")
    policy["status"]["observedGeneration"] = 1
    assert management_authority_ready(**args) is False
    policy["status"]["observedGeneration"] = 2
    policy["status"]["typeChecking"]["expressionWarnings"] = [{"warning": "invalid expression"}]
    with pytest.raises(ManagementStageError, match="policy"):
        management_authority_ready(**args)
    assert len(inputs[2].creates) == 9


def test_https_authority_rejects_arbitrary_grants_before_network(inputs):
    from scripts.ops.nebius_management_authority_stage import HTTPSManagementAuthorityAPI
    from scripts.ops.nebius_management_stage import ManagementStageError

    authority, binding, _ = inputs
    from loom.nebius_management_authority import render_namespace_authority

    attempts = []
    with HTTPSManagementAuthorityAPI(authority=authority, binding=binding,
                                    api_server="https://cluster.example", ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(lambda request: attempts.append(request)))
        document = copy.deepcopy(render_namespace_authority(authority)[-1])
        document["roleRef"]["name"] = "cluster-admin"
        document["metadata"]["annotations"] = {"loom.nebius/management-stage-operation": str(uuid4())}
        with pytest.raises(ManagementStageError, match="scope"):
            api.create_resource(document)
    assert not attempts


@pytest.mark.parametrize("mutation", ["selector", "match_policy", "param", "role"])
def test_defaulting_cannot_narrow_policy_or_expand_authority(inputs, tmp_path, mutation):
    from scripts.ops.nebius_management_stage import ManagementStageError

    def change(doc):
        if doc["kind"] == "ValidatingAdmissionPolicy":
            if mutation == "selector":
                doc["spec"]["matchConstraints"]["namespaceSelector"] = {"matchLabels": {"bypass": "yes"}}
            elif mutation == "match_policy":
                doc["spec"]["matchConstraints"]["matchPolicy"] = "Exact"
            elif mutation == "param":
                doc["spec"]["paramKind"] = {"apiVersion": "v1", "kind": "ConfigMap"}
        if doc["kind"] == "ClusterRole" and mutation == "role":
            doc["aggregationRule"] = {"clusterRoleSelectors": [{}]}

    inputs[2].default_change = change
    with pytest.raises(ManagementStageError, match="defaulting"):
        stage(inputs, tmp_path / "state")
    assert not inputs[2].creates
