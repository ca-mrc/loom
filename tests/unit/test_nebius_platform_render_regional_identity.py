from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import (
    NebiusPlatformError,
    build_platform,
    build_regional_execution,
)
from loom.nebius_task_identity_policy import identity_policy_documents
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

ROOT = Path(__file__).resolve().parents[2]


def current_regions(inputs: tuple) -> tuple:
    config, candidate, profile = inputs
    config["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }
    target = config["regional_execution_targets"][0]
    target.update(target_id="nebius-eu-west1-web-pod", execution_namespace="loom-nebius-eu-west1-web")
    target["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": target["target_id"],
        "execution_namespace": target["execution_namespace"],
    }
    config["task_egress"] = {"protected_cidrs": ["8.8.4.4/32"]}
    profile.update(supports_task_identity=True, supports_task_web_egress=True,
                   supports_task_artifact_inputs=True, resource_allocation_policy="node-share-v1",
                   execution_class_id="linux-amd64-cpu-web-pod-v1")
    return config, candidate, profile


def test_current_regional_identity_is_independent_and_preserves_web_runtime(regional_inputs: tuple) -> None:
    config, candidate, profile = current_regions(regional_inputs)
    target = config["regional_execution_targets"][0]
    original = deepcopy(config)
    primary = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    namespace = next(doc for doc in remote if doc["kind"] == "Namespace")
    assert namespace["metadata"]["name"] == target["execution_namespace"]
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "baseline"
    policies = [doc for doc in remote if doc["kind"].startswith("ValidatingAdmissionPolicy")]
    assert policies == identity_policy_documents(target["execution_namespace"], target["target_id"])
    assert primary["00-task-identity-policy.yaml"] == identity_policy_documents(
        config["execution_namespace"], config["target_id"])
    cm = next(doc for doc in primary["10-config-network.yaml"] if doc["kind"] == "ConfigMap")
    published = json.loads(cm["data"]["profile.json"])
    for capability in ("supports_task_identity", "supports_task_web_egress", "supports_task_artifact_inputs"):
        assert published[capability] is True
    catalog = json.loads(cm["data"]["catalog.json"])
    secondary = next(row for row in catalog["topology"]["targets"] if row["target_id"] == target["target_id"])
    assert secondary["execution_class_id"] == "linux-amd64-cpu-web-pod-v1"
    assert secondary["pod_identity_audience"] == "loom-execution"
    assert secondary["service_account_name"] == "loom-execution-attempt"
    assert secondary["health_role"] == "secondary"
    assert not {"Deployment", "CronJob", "Secret", "StatefulSet"} & {doc["kind"] for doc in remote}
    assert config == original


def test_current_regional_identity_requires_its_own_exact_policy(regional_inputs: tuple) -> None:
    config, candidate, profile = current_regions(regional_inputs)
    target = config["regional_execution_targets"][0]
    target.pop("task_identity_policy")
    with pytest.raises(NebiusPlatformError, match=r"regional.*identity.*policy"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)
    target["task_identity_policy"] = deepcopy(config["task_identity_policy"])
    with pytest.raises((NebiusPlatformError, ValueError), match=r"identity.*target"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_primary_guest_siblings_are_not_projected_into_regional_pod_target(regional_inputs: tuple) -> None:
    config, candidate, profile = current_regions(regional_inputs)
    config["guest_execution_target"] = {"target_id": "nebius-north-guest"}
    config["emulated_auth_execution_target"] = {"target_id": "nebius-north-guest-auth"}
    profile.update(guest_runtime="qemu-tcg-v1", supports_emulated_pkcs11=True)
    primary = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    target = config["regional_execution_targets"][0]
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    assert "guest-catalog.json" in next(doc for doc in primary["10-config-network.yaml"] if doc["kind"] == "ConfigMap")["data"]
    policies = [doc for doc in remote if doc["kind"].startswith("ValidatingAdmissionPolicy")]
    assert policies == identity_policy_documents(target["execution_namespace"], target["target_id"])
    assert "nebius-north-guest" not in json.dumps(remote)


@pytest.mark.parametrize("tamper", [False, True])
def test_regional_policy_install_keeps_restricted_until_same_admission_probes(
    regional_inputs: tuple, tmp_path: Path, tamper: bool,
) -> None:
    import yaml
    from scripts.ops.deploy_nebius_platform import (
        DeploymentError,
        TaskIdentityPolicyDeniedError,
        install_regional_task_identity_policy,
    )

    config, candidate, _profile = current_regions(regional_inputs)
    target = config["regional_execution_targets"][0]
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    if tamper:
        next(doc for doc in remote if doc["kind"] == "ValidatingAdmissionPolicy")["spec"]["failurePolicy"] = "Ignore"
    (tmp_path / (target["target_id"] + ".yaml")).write_text(yaml.safe_dump_all(remote))

    class Kube:
        def __init__(self) -> None:
            self.applied: list[list[dict]] = []
            self.probes = 0

        def run(self, *args: str) -> None:
            documents = list(yaml.safe_load_all(Path(args[args.index("-f") + 1]).read_text()))
            if "--dry-run=server" in args:
                self.probes += 1
                if documents[0]["spec"]["containers"][0]["securityContext"].get("capabilities", {}).get("add"):
                    raise TaskIdentityPolicyDeniedError(target["execution_namespace"] + "-private-root-v1")
            else:
                self.applied.append(documents)

        def get(self, *_args: str) -> dict:
            return {"metadata": {"generation": 1}, "status": {"observedGeneration": 1}}

    kube = Kube()
    if tamper:
        with pytest.raises(DeploymentError, match="regional identity policy differs"):
            install_regional_task_identity_policy(kube, config, tmp_path, target["target_id"], tmp_path / "snapshot")
        assert kube.applied == [] and kube.probes == 0
    else:
        install_regional_task_identity_policy(kube, config, tmp_path, target["target_id"], tmp_path / "snapshot")
        assert kube.applied[0][0]["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
        assert kube.applied[0][0]["metadata"]["name"] == target["execution_namespace"]
        assert kube.applied[1] == identity_policy_documents(target["execution_namespace"], target["target_id"])
        assert kube.probes == 2


@pytest.mark.parametrize("matching_contexts,permission", [(0, True), (2, True), (1, False), (1, True)])
def test_regional_preflight_uses_unique_existing_operator_context_before_any_mutation(
    regional_inputs: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    matching_contexts: int, permission: bool,
) -> None:
    from types import SimpleNamespace

    import yaml
    from scripts.ops import deploy_nebius_platform as deploy

    config, candidate, _profile = current_regions(regional_inputs)
    target = config["regional_execution_targets"][0]
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    (tmp_path / (target["target_id"] + ".yaml")).write_text(yaml.safe_dump_all(remote))
    view = {"clusters": [{"name": "west", "cluster": {"server": target["kubernetes_api_server"]}}],
            "contexts": [{"name": "operator-west-" + str(index), "context": {"cluster": "west"}}
                         for index in range(matching_contexts)]}
    primary = SimpleNamespace(kubeconfig=tmp_path / "existing-operator-config", local_executable=None,
                              run=lambda *args: json.dumps(view))
    commands = []
    checks = []

    class Remote:
        def __init__(self, kubeconfig: Path, *, context: str, local_executable: Path | None) -> None:
            assert kubeconfig == primary.kubeconfig and context == "operator-west-0"

        def run(self, *args: str) -> str:
            commands.append(args)
            assert args[:2] == ("auth", "can-i")
            return "yes" if permission else "no"

    monkeypatch.setattr(deploy, "Kubectl", Remote)
    monkeypatch.setattr(deploy, "verify_cluster_identity", lambda _kube, observed, expected:
                        checks.append((observed["cluster_id"], expected)))
    if matching_contexts != 1 or not permission:
        with pytest.raises(deploy.DeploymentError, match="operator"):
            deploy.preflight_regional_rollout(primary, config, tmp_path)
        assert not any(command[0] in {"apply", "patch", "create"} for command in commands)
    else:
        selected = deploy.preflight_regional_rollout(primary, config, tmp_path)
        assert set(selected) == {target["target_id"]}
        assert selected[target["target_id"]][1] == remote
        assert checks == [(target["cluster_id"], target["cluster_id"])]


def test_regional_candidate_renderer_keeps_remote_output_separate(
    regional_inputs: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_idle_rollout as rollout

    config, _candidate, _profile = current_regions(regional_inputs)

    @contextmanager
    def source(_sha: str):
        yield tmp_path

    commands = []
    monkeypatch.setattr(rollout, "candidate_manifests", source)
    monkeypatch.setattr(rollout.subprocess, "run", lambda command, **_kwargs:
                        commands.append(command) or SimpleNamespace(returncode=0))
    args = SimpleNamespace(candidate="a" * 40, publication_dir=tmp_path, render_dir=tmp_path / "primary",
                           regional_render_dir=tmp_path / "remote")
    rollout.render_candidate(args, config, "{}")
    assert commands[0][commands[0].index("--regional-output") + 1] == str(args.regional_render_dir)
