from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from scripts.ops import deploy_nebius_platform as deploy

from loom.nebius_platform_render import (
    NebiusPlatformError,
    build_platform,
    build_regional_execution,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

ROOT = Path(__file__).resolve().parents[2]


def test_pinned_candidate_archive_owns_all_renderer_dependency_imports() -> None:
    from scripts.ops.nebius_idle_rollout import candidate_manifests

    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    with candidate_manifests(sha) as source:
        assert (source / "pyproject.toml").is_file()
        assert (source / "uv.lock").is_file()
        process = subprocess.run(
            [sys.executable, "-c", "from pathlib import Path; import loom.nebius_platform_render as r; "
             "import loom_execution_actuator.task_image_settings as b; import loom_llm_gateway.task_egress as e; "
             "assert all(Path(m.__file__).resolve().is_relative_to(Path.cwd().resolve()) for m in (r, b, e))"],
            cwd=source, env={**os.environ, "PYTHONPATH": str(source / "src")},
            capture_output=True, text=True, check=False,
        )
        assert process.returncode == 0, process.stderr


def builder() -> dict:
    return {"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
            "cache_bucket": "loom-shared-build-cache", "max_concurrent": 2,
            "cpu_millis": 8000, "memory_mib": 8192, "ephemeral_storage_mib": 65536,
            "archive_secret_name": "loom-task-image-archive-writer"}


def test_regional_builder_is_opt_in_and_uses_canonical_shared_storage(regional_inputs: tuple) -> None:
    config, candidate, profile = regional_inputs
    config["task_image_builder"] = builder()
    target = config["regional_execution_targets"][0]
    default = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    assert not any(doc["metadata"].get("namespace", "").endswith("-build") for doc in default)
    target["task_image_builder"] = builder()
    original = deepcopy(config)
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    deployment = next(doc for doc in files["60-execution.yaml"]
                      if doc["kind"] == "Deployment" and doc["metadata"]["name"] == target["target_id"] + "-actuator")
    env = {row["name"]: row.get("value") for row in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    settings = json.loads(env["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"])
    build_ns = target["execution_namespace"] + "-build"
    assert settings["namespace"] == build_ns
    assert settings["storage_endpoint"] == config["storage_endpoint"]
    assert settings["storage_region"] == config["region"]
    assert settings["source_bucket"] == config["buckets"]["artifacts"]
    assert settings["registry_repository"] == builder()["registry_repository"]
    assert deployment["metadata"]["namespace"] == config["execution_namespace"]
    namespaces = {doc["metadata"]["name"]: doc["metadata"]["labels"] for doc in remote if doc["kind"] == "Namespace"}
    assert namespaces[build_ns]["pod-security.kubernetes.io/enforce"] == "privileged"
    assert namespaces[target["execution_namespace"]]["pod-security.kubernetes.io/enforce"] == "restricted"
    quota = next(doc for doc in remote if doc["kind"] == "ResourceQuota" and doc["metadata"]["namespace"] == build_ns)
    assert quota["spec"]["hard"]["requests.cpu"] == "16000m"
    role = next(doc for doc in remote if doc["kind"] == "Role" and doc["metadata"]["namespace"] == build_ns)
    assert not any("secrets" in rule["resources"] for rule in role["rules"])
    binding = next(doc for doc in remote if doc["kind"] == "RoleBinding" and doc["metadata"]["namespace"] == build_ns)
    assert binding["subjects"] == [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": target["service_account_ids"]["actuator"]}]
    assert not any(doc["kind"] in {"Secret", "Deployment", "CronJob"} for doc in remote)
    assert config == original


@pytest.mark.parametrize("policy", [None, "environment-local-capacity-balanced", "invalid"])
def test_placement_policy_matches_catalog_and_control_plane(platform_inputs: tuple, policy: str | None) -> None:
    config, candidate, profile = platform_inputs
    if policy:
        config["execution_placement_policy"] = policy
    if policy == "invalid":
        with pytest.raises(NebiusPlatformError, match="execution_placement_policy"):
            build_platform(config, candidate, profile, {}, repo_root=ROOT)
        return
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    cm = next(doc for doc in files["10-config-network.yaml"] if doc["kind"] == "ConfigMap")
    expected = policy or "environment-local-health-first"
    assert json.loads(cm["data"]["catalog.json"])["topology"]["placement_policy"] == expected
    cp = next(doc for doc in files["40-services.yaml"] if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-control-plane")
    assert {row["name"]: row.get("value") for row in cp["spec"]["template"]["spec"]["containers"][0]["env"]}["LOOM_CP_SERVICE_EXECUTION_PLACEMENT_POLICY"] == expected


@pytest.mark.parametrize("failure", [None, "secret", "permission", "scope"])
def test_regional_builder_preflight_checks_remote_keys_and_each_namespace(
    regional_inputs: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str | None,
) -> None:
    config, candidate, _profile = regional_inputs
    target = config["regional_execution_targets"][0]
    target["task_image_builder"] = builder()
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    if failure == "scope":
        next(row for row in remote if row["kind"] == "Role")["metadata"]["namespace"] = "unreviewed-build"
    (tmp_path / (target["target_id"] + ".yaml")).write_text(yaml.safe_dump_all(remote))
    view = {"clusters": [{"name": "west", "cluster": {"server": target["kubernetes_api_server"]}}],
            "contexts": [{"name": "west-operator", "context": {"cluster": "west"}}]}
    primary = SimpleNamespace(kubeconfig=tmp_path / "operator", local_executable=None, run=lambda *args: json.dumps(view))
    commands = []

    class Remote:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, *args):
            commands.append(args)
            if args[:2] == ("get", "secret"):
                assert args[args.index("-n") + 1] == target["execution_namespace"] + "-build"
                return "access-key\ncredentials.json" if failure == "secret" else "access-key\nsecret-key\ncredentials.json"
            assert args[:2] == ("auth", "can-i")
            if failure == "permission" and args[-1] == target["execution_namespace"] + "-build":
                return "no"
            return "yes"

    monkeypatch.setattr(deploy, "Kubectl", Remote)
    monkeypatch.setattr(deploy, "verify_cluster_identity", lambda *_args: None)
    if failure:
        with pytest.raises(deploy.DeploymentError, match={"secret": "required secret keys", "permission": "operator", "scope": "scope"}[failure]):
            deploy.preflight_regional_rollout(primary, config, tmp_path)
    else:
        assert set(deploy.preflight_regional_rollout(primary, config, tmp_path)) == {target["target_id"]}
        namespaces = {row[-1] for row in commands if row[:2] == ("auth", "can-i") and "-n" in row}
        assert namespaces == {target["execution_namespace"], target["execution_namespace"] + "-build", target["execution_namespace"] + "-observations"}
        assert {row[2] for row in commands if row[:2] == ("get", "secret")} == {
            "loom-task-build-source", "loom-task-build-cache", "loom-task-build-registry", "loom-task-image-archive-writer"}
    assert not any(row[0] in {"apply", "patch", "create"} for row in commands)
