"""Deployment identity claims must agree with an explicitly constrained target."""

from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import NebiusPlatformError, build_platform
from tests.support.nebius_platform import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def test_identity_profile_cannot_claim_readiness_in_restricted_namespace(platform_inputs):
    config, candidate, profile = platform_inputs
    profile["supports_task_identity"] = True
    with pytest.raises(NebiusPlatformError, match=r"identity.*policy"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_identity_policy_is_target_bound_and_can_prepare_without_enabling_execution(platform_inputs):
    config, candidate, profile = platform_inputs
    config["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    namespace = next(doc for doc in files["00-namespaces.yaml"]
                     if doc["metadata"]["name"] == config["execution_namespace"])
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "baseline"
    policy, binding = files["00-task-identity-policy.yaml"]
    assert policy["kind"] == "ValidatingAdmissionPolicy"
    assert policy["spec"]["failurePolicy"] == "Fail"
    assert binding["spec"]["validationActions"] == ["Deny"]
    assert binding["spec"]["matchResources"]["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": config["execution_namespace"],
    }
    different = deepcopy(config)
    different["task_identity_policy"]["target_id"] = "another-target"
    with pytest.raises(NebiusPlatformError, match=r"identity.*target"):
        build_platform(different, candidate, profile, {}, repo_root=ROOT)


def test_deployment_requires_exact_policy_artifact_before_namespace_exception(platform_inputs, tmp_path):
    from scripts.ops.deploy_nebius_platform import DeploymentError, load_render

    from loom.nebius_platform_render import write_platform

    config, candidate, profile = platform_inputs
    config["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    write_platform(files, config, candidate, tmp_path)
    load_render(tmp_path)
    policy_file = tmp_path / "00-task-identity-policy.yaml"
    original = policy_file.read_text()
    policy_file.write_text(original.replace("failurePolicy: Fail", "failurePolicy: Ignore"))
    with pytest.raises(DeploymentError, match="policy differs"):
        load_render(tmp_path)
    policy_file.unlink()
    with pytest.raises(DeploymentError, match="policy artifact is missing"):
        load_render(tmp_path)
