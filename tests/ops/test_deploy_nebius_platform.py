from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import deploy_nebius_platform as deploy
from scripts.ops import nebius_candidate as candidate
from tests.unit.test_nebius_platform_render import platform_inputs, regional_inputs  # noqa: F401

from loom.nebius_platform_render import build_platform, write_platform


@pytest.mark.parametrize("status,accepted", [
    ({"observedGeneration": 6}, True),
    ({"observedGeneration": 6, "typeChecking": {}}, True),
    ({"observedGeneration": 5}, False),
    ({"observedGeneration": 6, "typeChecking": {
        "expressionWarnings": [{"warning": "undefined field"}],
    }}, False),
])
def test_policy_observation_requires_current_generation_without_warnings(tmp_path, monkeypatch, status, accepted):
    """A clean warning-recovery readback may omit empty typeChecking."""
    probes = []

    class PolicyKube:
        def get(self, kind, name, namespace):
            assert kind == "validatingadmissionpolicy"
            return {"metadata": {"generation": 6}, "status": status}

        def run(self, *command):
            assert command[0] == "apply"
            if "--dry-run=server" in command:
                pod = yaml.safe_load(Path(command[command.index("-f") + 1]).read_text())
                capabilities = pod["spec"]["containers"][0]["securityContext"]["capabilities"]
                probes.append(capabilities.get("add", []))
                if probes[-1]:
                    raise deploy.TaskIdentityPolicyDeniedError("execution-private-root-v1")
            return ""

    ticks = iter(range(0, 1000, 31))
    monkeypatch.setattr(deploy.time, "monotonic", lambda: next(ticks))
    if accepted:
        deploy.install_task_identity_policy(PolicyKube(), {"execution_namespace": "execution"}, tmp_path)
        assert probes == [[], ["NET_BIND_SERVICE"]]
    else:
        with pytest.raises(deploy.DeploymentError, match=r"not observed|type-checking warnings"):
            deploy.install_task_identity_policy(PolicyKube(), {"execution_namespace": "execution"}, tmp_path)
        assert probes == []


@pytest.mark.parametrize("failure", ["converges", "permanent", "foreign-policy", "transport"])
def test_positive_admission_probe_waits_only_for_own_policy_convergence(tmp_path, monkeypatch, failure):
    """Type-check status can advance before the admission evaluator's cache."""
    probes = []

    class PolicyKube:
        def get(self, kind, name, namespace):
            return {"metadata": {"generation": 6}, "status": {"observedGeneration": 6}}

        def run(self, *command):
            if "--dry-run=server" not in command:
                return ""
            pod = yaml.safe_load(Path(command[command.index("-f") + 1]).read_text())
            extra = pod["spec"]["containers"][0]["securityContext"]["capabilities"].get("add", [])
            probes.append(extra)
            if extra or failure == "permanent" or len(probes) == 1:
                if failure == "transport":
                    raise deploy.DeploymentError("kubectl apply failed: Forbidden")
                policy = "foreign-private-root-v1" if failure == "foreign-policy" else "execution-private-root-v1"
                raise deploy.TaskIdentityPolicyDeniedError(policy)
            return ""

    ticks = iter([0, 1, 31])
    monkeypatch.setattr(deploy.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(deploy.time, "sleep", lambda _: None)
    if failure == "converges":
        deploy.install_task_identity_policy(PolicyKube(), {"execution_namespace": "execution"}, tmp_path)
        assert probes == [[], [], ["NET_BIND_SERVICE"]]
    else:
        with pytest.raises(deploy.DeploymentError):
            deploy.install_task_identity_policy(PolicyKube(), {"execution_namespace": "execution"}, tmp_path)
        assert probes == ([[], []] if failure == "permanent" else [[]])


@pytest.mark.parametrize("enabled,selector", [
    (True, None), (True, {"app": "loom-web"}), (False, {"app": "loom-shared-ingress"}),
])
def test_application_rollout_cannot_perform_or_revert_ingress_cutover(rendered, enabled, selector):
    _, config, manifest, files = rendered
    config["shared_ingress_enabled"] = enabled
    kube = FakeKubectl(config, files)
    if selector is not None:
        kube.objects["service", "loom-web"] = {"spec": {"selector": selector}}
    with pytest.raises(deploy.DeploymentError, match="ingress"):
        deploy.preflight(kube, manifest, config, files, config["cluster_id"])
    assert not any(command[0] in {"apply", "patch", "delete", "exec"} for command in kube.commands)


@pytest.mark.parametrize("ready", [False, True])
def test_shared_ingress_rollout_requires_ready_existing_controller(rendered, ready):
    _, config, manifest, files = rendered
    config["shared_ingress_enabled"] = True
    kube = FakeKubectl(config, files)
    kube.objects["service", "loom-web"] = {"spec": {"selector": {"app": "loom-shared-ingress"}}}
    kube.objects["deployment", "loom-shared-ingress"] = {
        "metadata": {"generation": 2}, "spec": {"replicas": 1},
        "status": {"observedGeneration": 2 if ready else 1, "availableReplicas": 1, "updatedReplicas": 1},
    }
    if ready:
        assert deploy.preflight(kube, manifest, config, files, config["cluster_id"])["database_exists"] is False
    else:
        with pytest.raises(deploy.DeploymentError, match="ingress"):
            deploy.preflight(kube, manifest, config, files, config["cluster_id"])


def test_ingress_cutover_between_preflight_and_lock_cannot_be_reverted(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True

    class InterleavedCutover(FakeKubectl):
        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if command[0] == "exec" and "acquire" in command:
                # Another protected operation finished before this lock was acquired.
                self.objects["service", "loom-web"] = {"spec": {"selector": {"app": "loom-shared-ingress"}}}
            return result

    kube = InterleavedCutover(config, files, database=True)
    kube.objects["service", "loom-web"] = {"spec": {"selector": {"app": "loom-web"}}}
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="ingress"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "patch", "delete", "create"} for command in kube.commands)
    assert kube.objects["service", "loom-web"]["spec"]["selector"] == {"app": "loom-shared-ingress"}
    assert any(command[0] == "exec" and "release" in command for command in kube.commands)


@pytest.mark.parametrize("process,setting", [
    ("loom-control-plane", "LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON"),
    ("loom-execution-actuator", "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"),
    ("loom-service", "LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"),
])
def test_ordinary_rollout_cannot_strip_global_pool_binding_or_restore_local_writers(rendered, process, setting):
    _, config, manifest, files = rendered
    kube = FakeKubectl(config, files)
    # Even malformed/incomplete global wiring must not be overwritten by the
    # old standalone renderer. The protected successor owns its recovery.
    kube.objects["deployment", process] = {"spec": {"template": {"spec": {
        "containers": [{"env": [{"name": setting, "value": "incomplete-global-binding"}]}]}}}}
    with pytest.raises(deploy.DeploymentError, match=r"pool.*protected"):
        deploy.preflight(kube, manifest, config, files, config["cluster_id"])
    assert not any(command[0] in {"apply", "patch", "delete", "exec", "create"} for command in kube.commands)


def test_pool_cutover_between_preflight_and_idle_guard_cannot_restore_local_writer(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True

    class InterleavedPoolCutover(FakeKubectl):
        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if command[0] == "exec" and "acquire" in command:
                self.objects["deployment", "loom-execution-actuator"] = {"spec": {"template": {"spec": {
                    "containers": [{"env": [{"name": "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL", "value": "retained-global-binding"}]}]}}}}
            return result

    kube = InterleavedPoolCutover(config, files, database=True)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match=r"pool.*protected"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "patch", "delete", "create"} for command in kube.commands)
    assert any(command[0] == "exec" and "release" in command for command in kube.commands)


def test_completed_legacy_rollback_allows_ordinary_rollout(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    operation = "11111111-1111-4111-8111-111111111111"
    uid = "22222222-2222-4222-8222-222222222222"
    kube = FakeKubectl(config, files, database=True)
    kube.objects["deployment", "loom-control-plane"] = {"metadata": {
        "uid": uid, "annotations": {"loom.nebius/pool-retirement-operation": operation}},
        "spec": {"replicas": 1}}
    calls = []
    def completion(selected):
        calls.append(selected)
        return {"operation_id": operation, "outcome": "legacy", "workloads": {
            "Deployment:" + config["namespace"] + ":loom-control-plane": uid}}
    monkeypatch.setattr(kube, "legacy_pool_completion", completion, raising=False)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    assert calls == [operation]


@pytest.mark.parametrize("failure", ["incomplete", "global", "wrong-operation", "wrong-uid", "new-operation-after-guard",
    "uid-after-guard", "global-after-guard", "locked"])
def test_legacy_completion_does_not_bypass_runtime_or_guard_boundaries(rendered, monkeypatch, failure):
    args, config, _, files = rendered
    args.apply = True
    operation = "11111111-1111-4111-8111-111111111111"
    uid = "22222222-2222-4222-8222-222222222222"
    other = "33333333-3333-4333-8333-333333333333"
    class Interleaved(FakeKubectl):
        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if guard_action(command) == "acquire":
                current = self.objects["deployment", "loom-control-plane"]
                if failure == "new-operation-after-guard":
                    current["metadata"]["annotations"]["loom.nebius/pool-retirement-operation"] = other
                elif failure == "uid-after-guard":
                    current["metadata"]["uid"] = other
                elif failure == "global-after-guard":
                    current["spec"]["template"] = {"spec": {"containers": [{"env": [
                        {"name": "LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON", "value": "global"}]}]}}
            return result
    kube = Interleaved(config, files, database=True)
    kube.objects["deployment", "loom-control-plane"] = {"metadata": {"uid": uid,
        "annotations": {"loom.nebius/pool-retirement-operation": operation}}, "spec": {"replicas": 1}}
    calls = []
    def proof(selected):
        calls.append(selected)
        if failure == "incomplete":
            raise deploy.DeploymentError("legacy pool completion unavailable")
        return {"operation_id": other if failure == "wrong-operation" else operation,
            "outcome": "global" if failure == "global" else "legacy", "workloads": {
                "Deployment:" + config["namespace"] + ":loom-control-plane": other if failure == "wrong-uid" else uid}}
    monkeypatch.setattr(kube, "legacy_pool_completion", proof)
    if failure == "locked":
        kube.guard_identity = ("pool-recovery:" + operation, "a" * 40)
        assert deploy.deploy(args, kube=kube)["status"] == "skipped_locked"
    else:
        with pytest.raises(deploy.DeploymentError, match="pool"):
            deploy.deploy(args, kube=kube)
    assert calls == [operation]
    assert not any(command[0] in {"apply", "patch", "delete", "create"} for command in kube.commands)


def test_standalone_deployer_rejects_managed_child(request, tmp_path):
    from tests.unit.test_nebius_environment_render import rendered

    inputs = request.getfixturevalue("platform_inputs")
    result = rendered(inputs)
    write_platform(result.files, result.config, inputs[1], tmp_path)
    with pytest.raises(deploy.DeploymentError, match="managed"):
        deploy.load_render(tmp_path)


def test_on_demand_build_secret_preflight_and_namespace(
    request: pytest.FixtureRequest, tmp_path: Path
) -> None:
    config, candidate, profile = request.getfixturevalue("platform_inputs")
    config["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
        "cache_bucket": config["buckets"]["artifacts"],
    }
    files = build_platform(config, candidate, profile, {}, repo_root=deploy.ROOT)
    write_platform(files, config, candidate, tmp_path)
    _, observed, _ = deploy.load_render(tmp_path)
    assert observed["task_image_builder"] == config["task_image_builder"]
    requirements = deploy.secret_requirements(files, config)
    namespace = config["execution_namespace"] + "-build"
    assert requirements[namespace, "loom-task-build-source"] == {"access-key", "secret-key"}
    assert requirements[namespace, "loom-task-build-registry"] == {"credentials.json"}
    assert requirements[namespace, "loom-task-build-cache"] == {"access-key", "secret-key"}


@pytest.mark.parametrize("cache_enabled", [False, True])
@pytest.mark.parametrize("egress_enabled", [False, True])
def test_native_build_render_preflight_does_not_import_service_dependencies(
    request: pytest.FixtureRequest, tmp_path: Path, cache_enabled: bool, egress_enabled: bool
) -> None:
    config, release, profile = request.getfixturevalue("platform_inputs")
    config["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
        **({"cache_bucket": config["buckets"]["artifacts"]} if cache_enabled else {}),
    }
    if egress_enabled:
        config["task_egress"] = {"protected_cidrs": ["198.51.100.0/24"]}
        profile["supports_task_web_egress"] = True
        profile["execution_class_id"] = "linux-amd64-cpu-web-pod-v1"
    config["task_resource_requests"] = {"local/measured-task": {
        "task_revision_sha256": "sha256:" + "d" * 64,
        "requests": {"controller": {
            "cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 100,
        }},
    }}
    inputs = tmp_path / "inputs.json"
    inputs.write_text(json.dumps([config, release, profile]))
    # A fresh process prevents imports already loaded by pytest from masking
    # accidental controller/DB dependencies in the offline operator path.
    result = subprocess.run(
        [sys.executable, "-I", "-c", """
import importlib.abc
import json
import sys
from pathlib import Path
root, inputs, output = map(Path, sys.argv[1:])
sys.path[:0] = [str(root), str(root / "src")]
class NoServiceDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"sqlalchemy", "asyncpg", "kubernetes", "nebius"} or fullname in {
            "loom.db", "loom_control_plane", "loom_execution_actuator.task_image_controller",
            "loom_execution_actuator.task_image_renderer", "loom_execution_actuator.renderer",
        }:
            raise ModuleNotFoundError("offline rendering imported " + fullname)
sys.meta_path.insert(0, NoServiceDependencies())
from loom.nebius_platform_render import build_platform, write_platform
from scripts.ops.deploy_nebius_platform import load_render, secret_requirements
from loom_execution_actuator.config import ExecutionActuatorSettings
config, release, profile = json.loads(inputs.read_text())
files = build_platform(config, release, profile, {}, repo_root=root)
write_platform(files, config, release, output)
identity, observed, loaded = load_render(output)
default_requests = observed.pop("default_task_resource_requests")
assert sum(role["cpu_millis"] for role in default_requests.values()) == 1000
assert sum(role["memory_mib"] for role in default_requests.values()) == 2048
assert sum(role["ephemeral_storage_mib"] for role in default_requests.values()) == 2048
assert observed == config
assert loaded == files
assert identity["candidate_sha"] == release["candidate_sha"]
required = secret_requirements(loaded, observed)
namespace = config["execution_namespace"] + "-build"
assert required[namespace, "loom-task-build-source"] == {"access-key", "secret-key"}
assert required[namespace, "loom-task-build-registry"] == {"credentials.json"}
assert ((namespace, "loom-task-build-cache") in required) == ("cache_bucket" in config["task_image_builder"])
""", str(deploy.ROOT), str(inputs), str(tmp_path / "render")],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture
def rendered(tmp_path: Path, request: pytest.FixtureRequest) -> tuple[argparse.Namespace, dict, dict, dict]:
    key = Ed25519PrivateKey.generate()
    signer = tmp_path / "signer.pem"
    signer.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    signer.chmod(0o600)
    trust = {
        "schema_version": 1,
        "keys": [
            {
                "signing_key_id": "publisher",
                "public_key_base64": base64.b64encode(
                    key.public_key().public_bytes(
                        serialization.Encoding.Raw, serialization.PublicFormat.Raw
                    )
                ).decode(),
            }
        ],
    }
    keyring = tmp_path / "trust.json"
    keyring.write_text(json.dumps(trust))
    record = {
        "schema_version": "loom.nebius-candidate.v1",
        "repository": candidate.REPOSITORY,
        "source_ref": candidate.SOURCE_REF,
        "candidate_sha": "a" * 40,
        "workflow_path": candidate.WORKFLOW,
        "run_id": 1,
        "registry_prefix": "cr.eu-north1.nebius.cloud/test",
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "policy_sha256": "sha256:" + "d" * 64,
        "images": {},
    }
    for component, name in candidate.COMPONENTS.items():
        record["images"][component] = {
            "image_ref": record["registry_prefix"] + "/" + name + "@sha256:" + "e" * 64,
            "source_sha": record["candidate_sha"],
            "platform": "linux/amd64",
            "sbom_sha256": "sha256:" + "f" * 64,
            "vulnerability_report_sha256": "sha256:" + "0" * 64,
            "highest_vulnerability_severity": "none",
        }
    release, profile = candidate.create_candidate(
        record, signing_key=signer, signing_key_id="publisher", keyring_json=json.dumps(trust)
    )
    config = json.loads(
        (deploy.ROOT / "deploy/nebius/integration.platform.json.example").read_text()
    )
    config["environment"] = getattr(request, "param", "development")
    for name in (
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "public_allocation_id",
    ):
        config[name] = "example-id"
    config["quota_parent_id"] = "tenant-test"
    config["cluster_id"] = "mk8scluster-test"
    config["kubernetes_api_server"] = "https://api.cluster.test"
    config["execution_price"]["vcpu_microusd_per_hour"] = 1000
    config["execution_price"]["source_version"] = "test-fixture"
    for name in ("postgres_image", "backup_image"):
        config[name] = record["registry_prefix"] + "/postgres@sha256:" + "1" * 64
    config["buckets"] = {name: "loom-integration-" + name for name in config["buckets"]}
    files = build_platform(config, release, profile, trust, repo_root=deploy.ROOT)
    output = tmp_path / "render"
    manifest = write_platform(files, config, release, output)
    args = argparse.Namespace(
        render_dir=output,
        kubeconfig=tmp_path / "kubeconfig",
        evidence_dir=tmp_path / "evidence",
        expected_cluster_id=config["cluster_id"],
        apply=False,
        retry_failed_jobs=False,
    )
    return args, config, manifest, files


@pytest.mark.parametrize("kind", ["ClusterRole", "ClusterRoleBinding"])
def test_usage_cluster_resources_must_belong_to_target(rendered, kind):
    args, config, _, files = rendered
    resource = next(
        row for row in files["60-execution.yaml"]
        if row["kind"] == kind
        and row["metadata"]["name"] == config["execution_namespace"] + "-actuator-usage"
    )
    deploy.load_render(args.render_dir)
    resource["metadata"]["name"] = "different-environment-actuator-usage"
    (args.render_dir / "60-execution.yaml").write_text(
        yaml.safe_dump_all(files["60-execution.yaml"])
    )
    with pytest.raises(deploy.DeploymentError, match="does not belong"):
        deploy.load_render(args.render_dir)


def guard_action(command):
    if "loom.nebius_rollout_guard" in command:
        return command[command.index("loom.nebius_rollout_guard") + 1]
    if "psql" in command and "FROM public.nebius_rollout_guard" in command[-1]:
        return "observe"
    return None


class FakeKubectl(deploy.Kubectl):
    def __init__(self, config: dict, files: dict, *, database: bool = False):
        self.config = config
        self.files = files
        self.commands: list[tuple[str, ...]] = []
        self.objects: dict[tuple[str, str], dict] = {}
        self.secrets = deploy.secret_requirements(files, config)
        self.fail_backup = False
        self.wrong_server = False
        self.schema_ready = True
        self.schema_current = True
        self.version_table = True
        self.guard_identity = None
        if database:
            self.objects["statefulset", "loom-postgres"] = {"metadata": {"name": "loom-postgres"}}
            self.objects["cronjob", "loom-platform-backup"] = files["80-backup.yaml"][0]

    def get(self, kind: str, name: str, namespace: str) -> dict:
        self.commands.append(("get", kind, name, namespace))
        return self.objects.get((kind, name), {})

    def run(self, *args: str, timeout: int = 90) -> str:
        self.commands.append(args)
        if args[0] == "exec":
            if "psql" in args:
                if guard_action(args) == "observe":
                    import re
                    owner = re.search(r"owner = '([^']+)'", args[-1])[1]
                    candidate = re.search(r"candidate_sha = '([^']+)'", args[-1])[1]
                    status = "open" if self.guard_identity is None else (
                        "held" if self.guard_identity == (owner, candidate) else "skipped_locked")
                    return json.dumps({"status": status})
                if "migration_ready())" in args[-1]:
                    return json.dumps(self.schema_ready)
                from loom.db.schema_startup import service_schema_head
                if "version_num FROM public.alembic_version" in args[-1]:
                    return json.dumps(service_schema_head() if self.schema_current else "previous")
                return json.dumps({"version_table": self.version_table,
                                   "access_schema": True, "access_guard": True})
            action = args[args.index("loom.nebius_rollout_guard") + 1]
            identity = (args[args.index("--owner") + 1], args[args.index("--candidate") + 1])
            if action == "observe":
                status = "open" if self.guard_identity is None else (
                    "held" if self.guard_identity == identity else "skipped_locked"
                )
            elif action == "acquire":
                status = "skipped_locked" if self.guard_identity is not None else "acquired"
                if status == "acquired":
                    self.guard_identity = identity
            else:
                assert self.guard_identity == identity
                self.guard_identity = None
                status = "released"
            return json.dumps({"status": status})
        if args[:2] == ("config", "view"):
            return json.dumps(
                {
                    "clusters": [
                        {
                            "name": "nebius-cluster-test",
                            "cluster": {
                                "server": "https://wrong.test"
                                if self.wrong_server
                                else self.config["kubernetes_api_server"],
                                "certificate-authority-data": "redacted",
                            },
                        }
                    ]
                }
            )
        if args[:2] == ("get", "nodes"):
            return json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": "computeinstance-test",
                                "labels": {"loom.nebius/node-role": "system"},
                            },
                            "spec": {"providerID": "nebius://computeinstance-test"},
                        }
                    ]
                }
            )
        if args[:2] == ("get", "secret"):
            assert "go-template=" in args[-1]
            assert r'{{"\n"}}' in args[-1]
            return "\n".join(sorted(self.secrets[(args[4], args[2])]))
        if args[0] == "apply":
            for obj in yaml.safe_load_all(Path(args[2]).read_text()):
                self.objects[obj["kind"].lower(), obj["metadata"]["name"]] = obj
                if obj["kind"] == "Deployment":
                    replicas = obj["spec"].get("replicas", 1)
                    obj["metadata"]["generation"] = 1
                    obj["status"] = {"observedGeneration": 1, "updatedReplicas": replicas,
                                     "availableReplicas": replicas, "replicas": replicas}
                if obj["kind"] == "Job":
                    obj["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
        if args[:2] == ("create", "job"):
            name = args[2]
            condition = (
                "Failed" if self.fail_backup and name.startswith("loom-predeploy-") else "Complete"
            )
            self.objects["job", name] = {
                "metadata": {"name": name},
                "status": {"conditions": [{"type": condition, "status": "True"}]},
            }
        if args[:2] == ("delete", "job"):
            self.objects.pop(("job", args[2]))
        return ""


def _current_primary(kube, config, files, target_id):
    """An installed ConfigMap, independent of the proposed render."""
    import copy

    current = copy.deepcopy(next(row for row in files["10-config-network.yaml"]
                                if row["kind"] == "ConfigMap"))
    environment = {**config, "target_id": target_id}
    current["data"]["environment.json"] = json.dumps(environment)
    kube.objects["configmap", "loom-platform-config"] = current


@pytest.mark.parametrize("rendered", ["development", "staging"], indirect=True)
@pytest.mark.parametrize("retire", [False, True])
@pytest.mark.parametrize("apply", [False, True])
def test_environment_reclassification_rejected_before_any_cluster_write(rendered, monkeypatch, retire, apply):
    args, config, _, files = rendered
    args.apply = apply
    args.retire_target = "previous-primary" if retire else None
    kube = FakeKubectl(config, files, database=True)
    previous = {**config, "environment": "staging" if config["environment"] == "development" else "development"}
    _current_primary(kube, previous, files, args.retire_target or config["target_id"])
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="environment reclassification"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "exec", "create", "delete"} for command in kube.commands)


def test_environment_reclassification_rechecked_after_guard_acquisition(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    _current_primary(kube, config, files, config["target_id"])
    original_guard = deploy.rollout_guard

    def interleave(_kube, _ns, action, _owner, _candidate):
        if action == "acquire":
            _current_primary(kube, {**config, "environment": "staging"}, files, config["target_id"])
        return original_guard(_kube, _ns, action, _owner, _candidate)

    monkeypatch.setattr(deploy, "rollout_guard", interleave)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="environment reclassification"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "create", "delete"} for command in kube.commands)
    assert kube.guard_identity is None


@pytest.mark.parametrize("retire_target", [None, "foreign-target"])
def test_primary_target_change_requires_exact_explicit_retirement(rendered, monkeypatch, retire_target):
    # Without this fence, an immutable-class replacement leaves the old target
    # eligible for dispatch after its actuator has moved to the new identity.
    args, config, _, files = rendered
    args.apply = True
    args.retire_target = retire_target
    kube = FakeKubectl(config, files, database=True)
    _current_primary(kube, config, files, "previous-primary")
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="target"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "exec", "create", "delete"} for command in kube.commands)


@pytest.mark.parametrize("installed", [False, True])
def test_retirement_cannot_name_the_destination_or_fresh_install(rendered, monkeypatch, installed):
    args, config, _, files = rendered
    args.apply = True
    args.retire_target = config["target_id"]
    kube = FakeKubectl(config, files, database=installed)
    if installed:
        _current_primary(kube, config, files, config["target_id"])
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="target"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "exec", "create", "delete"} for command in kube.commands)


@pytest.mark.parametrize("guest_field", ["guest_execution_target", "emulated_auth_execution_target"])
@pytest.mark.parametrize("change", ["remove-guest", "rename-guest", "replace-owner"])
def test_guest_target_replacement_requires_separate_retirement_protocol(rendered, change, guest_field):
    _, config, _, _ = rendered
    previous = {**config, guest_field: {"target_id": "guest-original"}}
    proposed = {**previous}
    retire = None
    if change == "remove-guest":
        proposed.pop(guest_field)
    elif change == "rename-guest":
        proposed[guest_field] = {"target_id": "guest-replacement"}
    else:
        proposed["target_id"] = "owner-replacement"
        retire = previous["target_id"]
    with pytest.raises(deploy.DeploymentError, match="guest"):
        deploy.validate_target_replacement(
            {"data": {"environment.json": json.dumps(previous)}}, proposed, retire,
        )


def test_guest_target_can_be_added_and_retained_without_replacing_owner(rendered):
    _, config, _, _ = rendered
    proposed = {**config, "guest_execution_target": {"target_id": "guest-original"}}
    for previous in (config, proposed):
        deploy.validate_target_replacement(
            {"data": {"environment.json": json.dumps(previous)}}, proposed, None,
        )


@pytest.mark.parametrize("changed", ["target_id", "execution_namespace", "region"])
def test_target_replacement_rechecks_its_source_after_lock(rendered, monkeypatch, changed):
    args, config, _, files = rendered
    args.apply = True
    args.retire_target = "previous-primary"
    kube = FakeKubectl(config, files, database=True)
    _current_primary(kube, config, files, "previous-primary")
    original_guard = deploy.rollout_guard

    def interleave(_kube, _ns, action, _owner, _candidate):
        if action == "acquire":
            current = kube.objects["configmap", "loom-platform-config"]["data"]
            environment = json.loads(current["environment.json"])
            environment[changed] = "concurrent-change"
            current["environment.json"] = json.dumps(environment)
        return original_guard(_kube, _ns, action, _owner, _candidate)

    monkeypatch.setattr(deploy, "rollout_guard", interleave)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="target"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "create", "delete"} for command in kube.commands)


@pytest.mark.parametrize("changed", ["cluster_id", "namespace", "execution_namespace", "environment",
                                     "regional_execution_targets"])
def test_target_replacement_rejects_foreign_or_regional_source(rendered, changed):
    args, config, _, files = rendered
    args.retire_target = "previous-primary"
    kube = FakeKubectl(config, files, database=True)
    _current_primary(kube, config, files, "previous-primary")
    current = kube.objects["configmap", "loom-platform-config"]["data"]
    environment = json.loads(current["environment.json"])
    environment[changed] = [{}] if changed == "regional_execution_targets" else "foreign"
    current["environment.json"] = json.dumps(environment)
    with pytest.raises(deploy.DeploymentError, match="target"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "exec", "create", "delete"} for command in kube.commands)


@pytest.mark.parametrize("status", ["skipped_busy", "skipped_locked", "planned"])
def test_target_replacement_plan_and_unavailable_guard_do_not_retire(rendered, monkeypatch, status):
    args, config, _, files = rendered
    args.apply = status != "planned"
    args.retire_target = "previous-primary"
    kube = FakeKubectl(config, files, database=True)
    _current_primary(kube, config, files, "previous-primary")
    calls = []

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return {"status": status}

    monkeypatch.setattr(deploy, "rollout_guard", guard)
    assert deploy.deploy(args, kube=kube)["status"] == status
    assert calls == ([] if status == "planned" else ["acquire"])
    assert not any(command[0] in {"apply", "exec", "create", "delete"} for command in kube.commands)


@pytest.mark.parametrize("failure", [None, "freshness", "backup", "retire", "apply", "target-readback"])
def test_target_retirement_is_guarded_and_ambiguous_failure_stays_paused(rendered, monkeypatch, failure):
    args, config, _, files = rendered
    args.apply = True
    args.retire_target = "previous-primary"
    calls = []

    class ReplacingKubectl(FakeKubectl):
        def run(self, *command, timeout=90):
            if command[:2] == ("create", "job"):
                calls.append("backup")
            if command[0] == "exec" and "-c" in command and "psql" not in command:
                action, previous, destination = command[-3:]
                calls.append(action)
                assert previous == "previous-primary" and destination == config["target_id"]
                if (failure, action) in {("freshness", "validate"), ("retire", "retire"),
                                        ("target-readback", "verify")}:
                    raise RuntimeError("remote connection lost after possible commit")
                return json.dumps({"previous_target_id": previous, "target_id": destination, "status": action})
            if command[0] == "apply":
                calls.append("apply")
                if failure == "apply":
                    raise RuntimeError("apply failed")
            return super().run(*command, timeout=timeout)

    kube = ReplacingKubectl(config, files, database=True)
    kube.fail_backup = failure == "backup"
    _current_primary(kube, config, files, "previous-primary")
    original_guard = deploy.rollout_guard

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return original_guard(_kube, _ns, action, _owner, _candidate)

    monkeypatch.setattr(deploy, "rollout_guard", guard)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    if failure:
        with pytest.raises((RuntimeError, deploy.DeploymentError)):
            deploy.deploy(args, kube=kube)
        evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
        assert evidence["dispatch_paused"] == (failure not in {"freshness", "backup"})
    else:
        result = deploy.deploy(args, kube=kube)
        assert result["status"] == "complete"
        assert result["target_replacement"] == {
            "previous_target_id": "previous-primary", "target_id": config["target_id"],
            "previous_target_retired": True, "active_target_verified": True,
        }
    if failure == "freshness":
        assert calls == ["acquire", "validate", "observe", "release", "observe"]
    elif failure == "backup":
        assert calls == ["acquire", "validate", "backup", "observe", "release", "observe"]
    else:
        assert calls[:4] == ["acquire", "validate", "backup", "retire"]
        assert ("release" in calls) == (failure is None)
        if failure != "retire":
            assert "apply" in calls[4:]
        if failure is None:
            assert calls[-2:] == ["verify", "release"]


@pytest.mark.parametrize("response_mode", ["ok", "reused-target", "wrong-target", "http-error", "redirect",
                                          "inactive-destination", "active-previous"])
def test_retirement_program_calls_admin_api_without_exposing_credentials(tmp_path, response_mode):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.error import HTTPError

    secret = tmp_path / "secret.toml"
    token = "local-fixture-admin-token"
    secret.write_text('[admin]\ntoken = "' + token + '"\n')
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/admin/execution-capacity/status"
            assert self.headers["Authorization"] == "Bearer " + token
            rows = [{"target_id": "previous-primary", "desired_state": "active"}]
            if response_mode == "reused-target":
                rows.append({"target_id": "next-primary", "desired_state": "retired"})
            elif response_mode in {"inactive-destination", "active-previous"}:
                rows[0]["desired_state"] = "active" if response_mode == "active-previous" else "retired"
                rows.append({"target_id": "next-primary",
                             "desired_state": "active" if response_mode == "active-previous" else "retired"})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"targets": rows}).encode())

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.path, self.headers["Authorization"], body))
            self.send_response({"http-error": 403, "redirect": 307}.get(response_mode, 200))
            if response_mode == "redirect":
                self.send_header("Location", "/credential-leak")
            self.end_headers()
            self.wfile.write(json.dumps({
                "target_id": "foreign" if response_mode == "wrong-target" else "previous-primary",
                **{key: body[key] for key in ("desired_state", "observed_state", "health_status")},
                "health_observed_at": body["observed_at"],
                "untrusted_extra": token,
            }).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    namespace = {"__name__": "retirement_fixture"}
    try:
        exec(deploy.TARGET_RETIRE_PROGRAM, namespace)
        def invoke():
            return namespace["target_action"](
                "verify" if response_mode in {"inactive-destination", "active-previous"} else "retire",
                "previous-primary", "next-primary", secret_file=secret,
                origin=f"http://127.0.0.1:{server.server_port}",
            )
        if response_mode == "ok":
            result = invoke()
            assert result == {"previous_target_id": "previous-primary", "target_id": "next-primary",
                              "status": "retire"}
            assert token not in json.dumps(result)
        else:
            with pytest.raises(HTTPError if response_mode in {"http-error", "redirect"} else ValueError):
                invoke()
        if response_mode in {"reused-target", "inactive-destination", "active-previous"}:
            assert requests == []
            return
        assert len(requests) == 1
        path, authorization, body = requests[0]
        assert path == "/admin/service-execution/targets/previous-primary/health"
        assert authorization == "Bearer " + token
        assert body == {"desired_state": "retired", "observed_state": "retired",
                        "health_status": "unhealthy", "observed_at": body["observed_at"],
                        "error_code": "target_replaced"}
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_reviewed_render_read_only_plan(rendered: tuple) -> None:
    args, config, _, files = rendered
    kube = FakeKubectl(config, files)
    result = deploy.deploy(args, kube=kube)
    assert result["status"] == "planned"
    assert all(command[0] in {"get", "config"} for command in kube.commands)
    evidence = next(iter(args.evidence_dir.glob("*.json"))).read_text()
    assert "api.cluster.test" not in evidence
    assert "certificate-authority-data" not in evidence


def test_reviewed_yaml_can_be_tuned_without_rehash_or_git_checkout(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, _, files = rendered
    path = args.render_dir / "40-services.yaml"
    rows = list(yaml.safe_load_all(path.read_text()))
    service = next(row for row in rows if row["kind"] == "Deployment")
    service["spec"]["replicas"] = 2
    path.write_text(yaml.safe_dump_all(rows, sort_keys=False))
    (args.render_dir / "README.md").write_text("Reviewed development settings")
    args.apply = True
    kube = FakeKubectl(config, files)
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)

    def no_checkout_gate(*args, **kwargs):
        pytest.fail("deployment must not depend on operator Git checkout state")

    monkeypatch.setattr(deploy.subprocess, "run", no_checkout_gate)
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    assert kube.objects["deployment", service["metadata"]["name"]]["spec"]["replicas"] == 2


def test_foreign_namespace_fails_before_mutation(rendered: tuple) -> None:
    args, config, _, files = rendered
    path = args.render_dir / "40-services.yaml"
    path.write_text(path.read_text().replace(config["namespace"], "other-namespace"))
    kube = FakeKubectl(config, files)
    with pytest.raises(deploy.DeploymentError, match="namespace"):
        deploy.deploy(args, kube=kube)
    assert kube.commands == []


def test_wrong_cluster_and_missing_secret_fail_before_mutation(rendered: tuple) -> None:
    args, config, _, files = rendered
    kube = FakeKubectl(config, files)
    kube.wrong_server = True
    with pytest.raises(deploy.DeploymentError, match="API server"):
        deploy.deploy(args, kube=kube)
    kube.wrong_server = False
    kube.secrets[(config["namespace"], "loom-platform-db")] = set()
    with pytest.raises(deploy.DeploymentError, match="secret keys"):
        deploy.deploy(args, kube=kube)
    assert all(command[0] in {"get", "config"} for command in kube.commands)


def test_failed_upgrade_backup_prevents_all_apply(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    kube.fail_backup = True
    with pytest.raises(deploy.DeploymentError, match=r"loom-predeploy-.*Failed"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "delete"} for command in kube.commands)
    result = json.loads(next(iter(args.evidence_dir.glob("*.json"))).read_text())
    assert result["phases"][-1]["name"] == "pre-upgrade-backup"
    assert result["status"] == "failed"


@pytest.mark.parametrize("rendered", ["development", "staging"], indirect=True)
def test_fresh_apply_and_completed_jobs_are_idempotent(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files)
    smoke: list[tuple[str, str]] = []
    monkeypatch.setattr(deploy, "public_smoke", lambda origin, environment: smoke.append((origin, environment)))
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    applies = [Path(command[2]).name for command in kube.commands if command[0] == "apply"]
    assert (
        applies.index("80-backup.yaml")
        < applies.index("30-migrate.yaml")
        < applies.index("40-services.yaml")
    )
    kube.commands.clear()
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    applies = [Path(command[2]).name for command in kube.commands if command[0] == "apply"]
    assert "30-migrate.yaml" not in applies and "50-configure.yaml" not in applies
    assert not any(command[0] == "delete" for command in kube.commands)
    assert smoke == [("https://" + config["public_host"], config["environment"])] * 2


def test_failed_candidate_job_requires_explicit_retry(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    name = files["30-migrate.yaml"][0]["metadata"]["name"]
    kube.objects["job", name] = {"status": {"conditions": [{"type": "Failed", "status": "True"}]}}
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="explicitly retry"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] == "delete" for command in kube.commands)
    args.retry_failed_jobs = True
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    assert (
        "delete",
        "job",
        name,
        "-n",
        config["namespace"],
        "--cascade=foreground",
        "--wait=true",
    ) in kube.commands


def test_kubectl_error_retains_api_reason_without_secret_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        deploy.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="Error from server (Forbidden): token=secret https://private.test",
        ),
    )
    with pytest.raises(deploy.DeploymentError, match="Forbidden") as error:
        deploy.Kubectl(tmp_path / "config").run("apply", "-f", "test.yaml")
    assert "secret" not in str(error.value) and "private" not in str(error.value)


@pytest.mark.parametrize("condition", ["Complete", "Failed", "Pending"])
def test_job_wait_returns_on_failure_or_completion_and_preserves_timeout(
    monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    reads = 0
    now = 0.0

    def get(*args: str) -> dict:
        nonlocal reads
        reads += 1
        # First read is still pending, then the controller records its terminal
        # condition. Failed jobs must not wait out the full completion timeout.
        observed = "Pending" if reads == 1 else condition
        return {"status": {"conditions": [{"type": observed, "status": "True"}]}}

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    monkeypatch.setattr(deploy.time, "monotonic", lambda: now)
    monkeypatch.setattr(deploy.time, "sleep", sleep)
    kube = SimpleNamespace(get=get)
    if condition == "Complete":
        deploy.wait_for_job(kube, "test-migration", "test-namespace", 20)
    else:
        reason = "Failed condition" if condition == "Failed" else "timed out"
        with pytest.raises(deploy.DeploymentError, match=reason):
            deploy.wait_for_job(kube, "test-migration", "test-namespace", 20)
    assert now == (20 if condition == "Pending" else 5)
    assert reads == (5 if condition == "Pending" else 2)


def test_regional_deploy_waits_for_each_primary_actuator_and_checks_secret_keys(
    rendered: tuple, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, _, _, _ = rendered
    config, candidate, profile = request.getfixturevalue("regional_inputs")
    config["cluster_id"] = "nebius-cluster-test"
    args.expected_cluster_id = config["cluster_id"]
    files = build_platform(config, candidate, profile, {}, repo_root=deploy.ROOT)
    write_platform(files, config, candidate, args.render_dir)
    args.apply = True
    kube = FakeKubectl(config, files)
    monkeypatch.setattr(deploy, "public_smoke", lambda *_args: None)
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    target_id = config["regional_execution_targets"][0]["target_id"]
    rollouts = [command[2] for command in kube.commands if command[:2] == ("rollout", "status")]
    assert "deployment/loom-execution-actuator" in rollouts
    assert "deployment/" + target_id + "-actuator" in rollouts
    for role in ("actuator", "collector", "gateway"):
        namespace = config["namespace"] if role == "gateway" else config["execution_namespace"]
        assert kube.secrets[namespace, target_id + "-" + role + "-kubernetes"] == {
            "ca.crt",
            "credentials.json",
        }
    assert not any(target_id + "-collector-nebius" in name for _, name in kube.secrets)


@pytest.mark.parametrize("status", ["skipped_busy", "skipped_locked"])
def test_busy_upgrade_exits_without_backup_apply_or_resume(rendered, monkeypatch, status):
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    calls = []

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return {"status": status}

    monkeypatch.setattr(deploy, "rollout_guard", guard)
    assert deploy.deploy(args, kube=kube)["status"] == status
    assert calls == ["acquire"]
    assert not any(command[0] in {"apply", "create", "delete"} for command in kube.commands)


def test_failed_health_retains_pause_and_success_resumes_after_readback(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    calls = []
    original_guard = deploy.rollout_guard

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return original_guard(_kube, _ns, action, _owner, _candidate)

    def health(*_):
        raise deploy.DeploymentError("unhealthy")

    monkeypatch.setattr(deploy, "rollout_guard", guard)
    monkeypatch.setattr(deploy, "public_smoke", health)
    with pytest.raises(deploy.DeploymentError, match="unhealthy"):
        deploy.deploy(args, kube=FakeKubectl(config, files, database=True))
    assert calls == ["acquire", "observe"]
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["dispatch_paused"] is True
    calls.clear()
    monkeypatch.setattr(deploy, "public_smoke", lambda *_: calls.append("health"))
    original = deploy.verify_deployed_images

    def readback(*args):
        original(*args)
        calls.append("readback")

    monkeypatch.setattr(deploy, "verify_deployed_images", readback)
    assert deploy.deploy(args, kube=FakeKubectl(config, files, database=True))["status"] == "complete"
    assert calls == ["acquire", "health", "readback", "release"]


def test_remote_apply_streams_manifest_and_keeps_ssh_host_verification(tmp_path, monkeypatch):
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "deploy@gateway.example")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/known_hosts")
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("kind: ConfigMap\n")
    calls = []

    def command(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout="applied", stderr="")

    monkeypatch.setattr(deploy.subprocess, "run", command)
    assert deploy.Kubectl(Path("/remote/kubeconfig")).run("apply", "-f", str(manifest)) == "applied"
    argv, kwargs = calls[0]
    assert "StrictHostKeyChecking=yes" in argv
    assert argv[-1].endswith("apply -f -")
    assert kwargs["input"] == manifest.read_text()


def test_active_application_access_blocks_before_backup_or_apply(rendered):
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    kube.schema_current = False
    kube.schema_ready = False
    with pytest.raises(deploy.DeploymentError, match='application_database_access_active'):
        deploy.deploy(args, kube=kube)
    assert not any(c[0] in {'create', 'apply', 'delete'} for c in kube.commands)
    evidence = json.loads(next(args.evidence_dir.glob('*.json')).read_text())
    assert evidence['reason_code'] == 'application_database_access_active'
    assert evidence['dispatch_paused'] is False


def test_current_schema_does_not_require_revoking_personal_access(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    kube.schema_ready = False
    monkeypatch.setattr(deploy, 'public_smoke', lambda *_: None)
    assert deploy.deploy(args, kube=kube)['status'] == 'complete'
    assert not any('migration_ready())' in c[-1] for c in kube.commands if 'psql' in c)


@pytest.mark.parametrize('status', ['held', 'open', 'skipped_locked'])
def test_resume_requires_saved_owner_and_candidate_without_reacquiring(rendered, monkeypatch, status):
    args, config, _, files = rendered
    args.apply = True
    args.resume_guard_owner = 'rollout-' + '1' * 32
    args.retry_failed_jobs = True
    calls = []
    def guard(_kube, _ns, action, owner, candidate):
        assert owner == args.resume_guard_owner
        assert candidate == 'a' * 40
        calls.append(action)
        return {'status': status if action == 'observe' else 'released'}
    monkeypatch.setattr(deploy, 'rollout_guard', guard)
    monkeypatch.setattr(deploy, 'public_smoke', lambda *_: None)
    if status == 'held':
        assert deploy.deploy(args, kube=FakeKubectl(config, files, database=True))['status'] == 'complete'
        assert calls == ['observe', 'release']
    else:
        with pytest.raises(deploy.DeploymentError, match='original candidate'):
            deploy.deploy(args, kube=FakeKubectl(config, files, database=True))
        assert calls == ['observe', 'observe']


def test_owned_recovery_backup_failure_preserves_existing_pause(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    args.resume_guard_owner = 'rollout-' + '1' * 32
    calls = []
    monkeypatch.setattr(deploy, 'rollout_guard', lambda _k, _n, action, _o, _c: calls.append(action) or {'status': 'held'})
    kube = FakeKubectl(config, files, database=True)
    kube.fail_backup = True
    with pytest.raises(deploy.DeploymentError):
        deploy.deploy(args, kube=kube)
    assert calls == ['observe', 'observe']
    evidence = json.loads(next(args.evidence_dir.glob('*.json')).read_text())
    assert evidence['dispatch_paused'] is True


def test_partial_first_install_without_version_table_can_resume(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    kube.version_table = False
    monkeypatch.setattr(deploy, "public_smoke", lambda *_: None)
    result = deploy.deploy(args, kube=kube)
    assert result["status"] == "complete"
    assert result["preflight"]["migration_needed"] is True
    assert not any("version_num FROM public.alembic_version" in c[-1] for c in kube.commands if "psql" in c)


@pytest.mark.parametrize("lost_action", ["acquire", "release"])
def test_committed_guard_response_loss_reconciles_persisted_pause(rendered, lost_action):
    args, config, _, files = rendered
    args.apply = True

    class ResponseLossKubectl(FakeKubectl):
        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if "loom.nebius_rollout_guard" in command and lost_action in command:
                raise deploy.DeploymentError("guard response lost after commit")
            return result

    kube = ResponseLossKubectl(config, files, database=True)
    # A pre-mutation failure exercises cleanup of an existing reservation.
    kube.fail_backup = lost_action == "release"
    with pytest.raises(deploy.DeploymentError):
        deploy.deploy(args, kube=kube)
    actions = [guard_action(c) for c in kube.commands if guard_action(c)]
    assert actions == ["acquire", "observe", "release", "observe"]
    assert kube.guard_identity is None
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["dispatch_paused"] is False
    assert evidence["guard_observation"] == {"status": "open"}
    assert not any(c[0] == "apply" for c in kube.commands)


@pytest.mark.parametrize("observation", ["unavailable", "skipped_locked"])
def test_lost_acquire_response_never_releases_unconfirmed_owner(rendered, observation):
    args, config, _, files = rendered
    args.apply = True

    class ResponseLossKubectl(FakeKubectl):
        def run(self, *command, timeout=90):
            if guard_action(command) == "observe":
                self.commands.append(command)
                if observation == "unavailable":
                    raise deploy.DeploymentError("private transport error")
                return json.dumps({"status": "skipped_locked"})
            result = super().run(*command, timeout=timeout)
            if "loom.nebius_rollout_guard" in command and "acquire" in command:
                if observation == "skipped_locked":
                    self.guard_identity = ("foreign-owner", "b" * 40)
                raise deploy.DeploymentError("guard response lost after commit")
            return result

    kube = ResponseLossKubectl(config, files, database=True)
    with pytest.raises(deploy.DeploymentError, match="guard response lost"):
        deploy.deploy(args, kube=kube)
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["dispatch_paused"] is True
    assert evidence["guard_observation"] == {"status": observation}
    assert not any("release" in c for c in kube.commands)
    assert not any(c[0] in {"apply", "create", "delete"} for c in kube.commands)
    assert "private transport error" not in json.dumps(evidence)


def test_recovery_preflight_failure_preserves_saved_owner_and_pause(rendered):
    args, config, manifest, files = rendered
    args.apply = True
    args.resume_guard_owner = "rollout-" + "1" * 32
    kube = FakeKubectl(config, files, database=True)
    kube.guard_identity = (args.resume_guard_owner, manifest["candidate_sha"])
    kube.wrong_server = True
    with pytest.raises(deploy.DeploymentError):
        deploy.deploy(args, kube=kube)
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["guard_owner"] == args.resume_guard_owner
    assert evidence["candidate_sha"] == manifest["candidate_sha"]
    assert evidence["dispatch_paused"] is True
    # Failed cluster validation cannot authorize even an observation there.
    assert evidence["guard_observation"] == {"status": "unavailable"}
    assert not any("loom.nebius_rollout_guard" in c for c in kube.commands)


@pytest.mark.parametrize("repeat_loss", [False, True])
def test_recovery_observe_response_loss_preserves_inherited_pause(rendered, repeat_loss):
    args, config, manifest, files = rendered
    args.apply = True
    args.resume_guard_owner = "rollout-" + "1" * 32

    class ResponseLossKubectl(FakeKubectl):
        observations = 0

        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if guard_action(command) == "observe":
                self.observations += 1
                if repeat_loss or self.observations == 1:
                    raise deploy.DeploymentError("guard observation response lost")
            return result

    kube = ResponseLossKubectl(config, files, database=True)
    kube.guard_identity = (args.resume_guard_owner, manifest["candidate_sha"])
    with pytest.raises(deploy.DeploymentError, match="observation response lost"):
        deploy.deploy(args, kube=kube)
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["guard_owner"] == args.resume_guard_owner
    assert evidence["dispatch_paused"] is True
    assert evidence["guard_observation"] == {"status": "unavailable" if repeat_loss else "held"}
    assert not any("release" in c or "acquire" in c for c in kube.commands)
    assert not any(c[0] in {"apply", "create", "delete"} for c in kube.commands)


def test_completed_rollout_release_response_loss_reads_open_database(rendered, monkeypatch):
    args, config, _, files = rendered
    args.apply = True
    monkeypatch.setattr(deploy, "public_smoke", lambda *_: None)

    class ResponseLossKubectl(FakeKubectl):
        def run(self, *command, timeout=90):
            result = super().run(*command, timeout=timeout)
            if "loom.nebius_rollout_guard" in command and "release" in command:
                raise deploy.DeploymentError("release response lost after commit")
            return result

    kube = ResponseLossKubectl(config, files, database=True)
    with pytest.raises(deploy.DeploymentError, match="release response lost"):
        deploy.deploy(args, kube=kube)
    evidence = json.loads(next(args.evidence_dir.glob("*.json")).read_text())
    assert evidence["dispatch_paused"] is False
    assert evidence["guard_observation"] == {"status": "open"}
    assert kube.guard_identity is None
    actions = [guard_action(c) for c in kube.commands if guard_action(c)]
    assert actions == ["acquire", "release", "observe"]


def test_recovery_observes_persisted_database_without_control_plane_pod(rendered):
    _, config, manifest, files = rendered
    kube = FakeKubectl(config, files, database=True)
    owner = "rollout-" + "9" * 32
    kube.guard_identity = (owner, manifest["candidate_sha"])
    assert deploy.rollout_guard(kube, config["namespace"], "observe", owner, manifest["candidate_sha"]) == {"status": "held"}
    command = kube.commands[-1]
    assert "statefulset/loom-postgres" in command and "psql" in command
    assert "BEGIN READ ONLY" in command[-1]
    assert "deployment/loom-control-plane" not in command
