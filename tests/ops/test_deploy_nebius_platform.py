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
def rendered(tmp_path: Path) -> tuple[argparse.Namespace, dict, dict, dict]:
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


class FakeKubectl(deploy.Kubectl):
    def __init__(self, config: dict, files: dict, *, database: bool = False):
        self.config = config
        self.files = files
        self.commands: list[tuple[str, ...]] = []
        self.objects: dict[tuple[str, str], dict] = {}
        self.secrets = deploy.secret_requirements(files, config)
        self.fail_backup = False
        self.wrong_server = False
        if database:
            self.objects["statefulset", "loom-postgres"] = {"metadata": {"name": "loom-postgres"}}
            self.objects["cronjob", "loom-platform-backup"] = files["80-backup.yaml"][0]

    def get(self, kind: str, name: str, namespace: str) -> dict:
        self.commands.append(("get", kind, name, namespace))
        return self.objects.get((kind, name), {})

    def run(self, *args: str, timeout: int = 90) -> str:
        self.commands.append(args)
        if args[0] == "exec":
            return json.dumps({"status": "released" if "release" in args else "acquired"})
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


@pytest.mark.parametrize("change", ["remove-guest", "rename-guest", "replace-owner"])
def test_guest_target_replacement_requires_separate_retirement_protocol(rendered, change):
    _, config, _, _ = rendered
    previous = {**config, "guest_execution_target": {"target_id": "guest-original"}}
    proposed = {**previous}
    retire = None
    if change == "remove-guest":
        proposed.pop("guest_execution_target")
    elif change == "rename-guest":
        proposed["guest_execution_target"] = {"target_id": "guest-replacement"}
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

    def interleave(_kube, _ns, action, _owner, _candidate):
        if action == "acquire":
            current = kube.objects["configmap", "loom-platform-config"]["data"]
            environment = json.loads(current["environment.json"])
            environment[changed] = "concurrent-change"
            current["environment.json"] = json.dumps(environment)
        return {"status": "acquired" if action == "acquire" else "released"}

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
            if command[0] == "exec" and "-c" in command:
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

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return {"status": "acquired" if action == "acquire" else "released"}

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
        assert calls == ["acquire", "validate", "release"]
    elif failure == "backup":
        assert calls == ["acquire", "validate", "backup", "release"]
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


def test_fresh_apply_and_completed_jobs_are_idempotent(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, _, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files)
    smoke: list[str] = []
    monkeypatch.setattr(deploy, "public_smoke", lambda origin, environment: smoke.append(origin))
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
    assert len(smoke) == 2


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

    def guard(_kube, _ns, action, _owner, _candidate):
        calls.append(action)
        return {"status": "acquired" if action == "acquire" else "released"}

    def health(*_):
        raise deploy.DeploymentError("unhealthy")

    monkeypatch.setattr(deploy, "rollout_guard", guard)
    monkeypatch.setattr(deploy, "public_smoke", health)
    with pytest.raises(deploy.DeploymentError, match="unhealthy"):
        deploy.deploy(args, kube=FakeKubectl(config, files, database=True))
    assert calls == ["acquire"]
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
