"""A guest target shares physical capacity, never collectors or broad root authority."""
import json
from copy import deepcopy
from pathlib import Path

import pytest

from loom.execution_contract import ExecutionClassV1, ExecutionTopologyV1
from loom.nebius_platform_render import NebiusPlatformError, build_platform, write_platform
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401

ROOT = Path(__file__).resolve().parents[2]


def guest_inputs(inputs):
    config, candidate, profile = deepcopy(inputs)
    config["guest_execution_target"] = {"target_id": "nebius-guest-fixture"}
    config["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }
    profile.update(guest_runtime="qemu-tcg-v1", guest_runtime_volume_mib=1024,
                   guest_max_artifact_bytes=6 * 1024**3, supports_task_identity=True)
    return config, candidate, profile


def test_guest_render_has_independent_catalog_and_actuator_with_one_physical_owner(platform_inputs, tmp_path):  # noqa: F811
    from scripts.ops.deploy_nebius_platform import load_render

    config, candidate, profile = guest_inputs(platform_inputs)
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    cm = next(doc for doc in files["10-config-network.yaml"] if doc["kind"] == "ConfigMap"
              and doc["metadata"]["name"] == "loom-platform-config")
    primary = json.loads(cm["data"]["catalog.json"])
    guest = json.loads(cm["data"]["guest-catalog.json"])
    klass = ExecutionClassV1.model_validate(guest["execution_class"])
    topology = ExecutionTopologyV1.model_validate(guest["topology"])
    assert klass.class_id == topology.execution_class_id == "linux-amd64-cpu-guest-v1"
    assert primary["execution_class"]["class_id"] == "linux-amd64-cpu-pod-v1"
    target, = topology.targets
    assert target.target_id == "nebius-guest-fixture"
    assert target.capacity_owner_target_id == config["target_id"]
    assert target.namespace_name == config["execution_namespace"]
    assert target.health_check_id == target.target_id
    docs = files["60-execution.yaml"]
    deployments = [doc for doc in docs if doc["kind"] == "Deployment"]
    assert len(deployments) == 2
    guest_actuator = next(doc for doc in deployments if doc["metadata"]["name"] == "nebius-guest-fixture-actuator")
    env = {item["name"]: item.get("value") for item in guest_actuator["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] == target.target_id
    assert "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER" not in env
    assert len([doc for doc in docs if doc["kind"] == "CronJob"]) == 1
    assert len([doc for doc in docs if doc["kind"] == "ResourceQuota"]) == 1
    assert len([doc for doc in files["00-task-identity-policy.yaml"] if doc["kind"] == "ValidatingAdmissionPolicy"]) == 1
    write_platform(files, config, candidate, tmp_path)
    _, observed_config, _ = load_render(tmp_path)
    assert observed_config["guest_execution_target"] == config["guest_execution_target"]


def test_guest_actuator_retains_the_published_image_and_shared_credentials(platform_inputs):  # noqa: F811
    config, candidate, profile = guest_inputs(platform_inputs)
    image = "cr.eu-north1.nebius.cloud/project/loom-execution-actuator@sha256:" + "a" * 64
    candidate["images"]["execution_actuator"]["image_ref"] = image
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    deployments = [row for row in files["60-execution.yaml"] if row["kind"] == "Deployment"]
    ordinary, guest = (row["spec"]["template"]["spec"] for row in deployments)
    assert guest["containers"][0]["image"] == ordinary["containers"][0]["image"] == image
    assert guest["serviceAccountName"] == ordinary["serviceAccountName"]
    assert guest["volumes"] == ordinary["volumes"]
    ordinary_secrets = [entry for entry in ordinary["containers"][0]["env"] if "valueFrom" in entry]
    assert [entry for entry in guest["containers"][0]["env"] if "valueFrom" in entry] == ordinary_secrets


@pytest.mark.parametrize("damage", ["no-target", "same-target", "actuator-collision", "wrong-scope", "no-policy", "unknown-runtime"])
def test_guest_readiness_rejects_incomplete_or_unbound_deployment(platform_inputs, damage):  # noqa: F811
    config, candidate, profile = guest_inputs(platform_inputs)
    if damage == "no-target":
        del config["guest_execution_target"]
    elif damage == "same-target":
        config["guest_execution_target"]["target_id"] = config["target_id"]
    elif damage == "actuator-collision":
        config["guest_execution_target"]["target_id"] = "loom-execution"
    elif damage == "wrong-scope":
        config["guest_execution_target"]["namespace"] = "foreign"
    elif damage == "no-policy":
        del config["task_identity_policy"]
    else:
        profile["guest_runtime"] = "host-docker"
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_emulated_auth_adds_distinct_target_without_rebinding_existing_guest(platform_inputs, tmp_path):  # noqa: F811
    from scripts.ops.deploy_nebius_platform import load_render

    from loom_control_plane.execution_capacity_targets import validate_capacity_owner

    config, candidate, profile = guest_inputs(platform_inputs)
    original = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    config["emulated_auth_execution_target"] = {"target_id": "nebius-auth-fixture"}
    profile["supports_emulated_pkcs11"] = True
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    data = files["10-config-network.yaml"][0]["data"]
    assert data["guest-catalog.json"] == original["10-config-network.yaml"][0]["data"]["guest-catalog.json"]
    auth = json.loads(data["emulated-auth-catalog.json"])
    target, = ExecutionTopologyV1.model_validate(auth["topology"]).targets
    assert target.target_id == "nebius-auth-fixture"
    assert target.execution_class_id == "linux-amd64-cpu-guest-auth-v1"
    owner, = ExecutionTopologyV1.model_validate(json.loads(data["catalog.json"])["topology"]).targets
    validate_capacity_owner(target, owner)
    deployments = [row for row in files["60-execution.yaml"] if row["kind"] == "Deployment"]
    assert {row["metadata"]["name"] for row in deployments} == {
        "loom-execution-actuator", "nebius-guest-fixture-actuator", "nebius-auth-fixture-actuator",
    }
    auth_pod = next(row for row in deployments if row["metadata"]["name"] == "nebius-auth-fixture-actuator")["spec"]["template"]["spec"]
    env = {entry["name"]: entry.get("value") for entry in auth_pod["containers"][0]["env"]}
    assert env["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] == "nebius-auth-fixture"
    assert "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER" not in env
    assert len([row for row in files["60-execution.yaml"] if row["kind"] == "CronJob"]) == 1
    policy = next(row for row in files["10-config-network.yaml"] if row["kind"] == "NetworkPolicy" and row["metadata"]["name"] == "postgres-private")
    peers = policy["spec"]["ingress"][0]["from"]
    assert any(row.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/name") == "nebius-auth-fixture-actuator" for row in peers)
    write_platform(files, config, candidate, tmp_path)
    _, readback, _ = load_render(tmp_path)
    assert readback["emulated_auth_execution_target"] == config["emulated_auth_execution_target"]


@pytest.mark.parametrize("damage", ["missing-target", "missing-readiness", "missing-guest", "same-owner", "same-guest", "bad-fields"])
def test_emulated_auth_readiness_requires_exact_distinct_target(platform_inputs, damage):  # noqa: F811
    config, candidate, profile = guest_inputs(platform_inputs)
    config["emulated_auth_execution_target"] = {"target_id": "nebius-auth-fixture"}
    profile["supports_emulated_pkcs11"] = True
    if damage == "missing-target":
        del config["emulated_auth_execution_target"]
    elif damage == "missing-readiness":
        del profile["supports_emulated_pkcs11"]
    elif damage == "missing-guest":
        del config["guest_execution_target"]
    elif damage == "same-owner":
        config["emulated_auth_execution_target"]["target_id"] = config["target_id"]
    elif damage == "same-guest":
        config["emulated_auth_execution_target"]["target_id"] = "nebius-guest-fixture"
    else:
        config["emulated_auth_execution_target"]["device"] = "/dev/card"
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)
