"""Exercise root-policy admission against a disposable API, without model calls."""

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from loom.nebius_platform_render import build_platform
from loom.nebius_task_identity_policy import identity_namespace_labels, identity_policy_documents
from loom_execution_actuator.renderer import ExecutionTargetRuntime, _sidecar, render_execution_job
from tests.integration.test_execution_actuator_k3s import _lease, _load_client, _start_k3s
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_task_sandbox_identity import _identity_task


def _pod(namespace: str, *, user: str = "root", home: str | None = None, deferred: bool = False) -> dict:
    from loom.execution_runtime_contract import RuntimeHandoffInputV1
    from loom.service_execution_materialization import (
        compile_deferred_verifier_plan,
        compile_service_execution_plan,
    )

    lease = _lease(namespace)
    job = render_execution_job(lease, target=ExecutionTargetRuntime(target_id=lease.target_id, namespace=namespace))
    template = job["spec"]["template"]
    task, trial, profile = _identity_task(user, home)
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile.model_copy(update={"supports_task_identity": True}),
        source_provenance=_provenance(), task_revision_sha256="sha256:" + "c" * 64,
    )
    if deferred:
        plan = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120,
            handoff_input=RuntimeHandoffInputV1(
                manifest_sha256="sha256:" + "d" * 64, file_count=1, total_bytes=10,
            ))
    template["spec"]["initContainers"].extend(_sidecar(sidecar) for sidecar in plan.sidecars)
    template["spec"]["volumes"].extend(
        {"name": sidecar.role_name + "-socket", "emptyDir": {}} for sidecar in plan.sidecars
    )
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {
        **template["metadata"], "name": "identity-admission", "namespace": namespace,
    }, "spec": template["spec"]}


@pytest.mark.timeout(240)
def test_private_root_policy_accepts_only_the_constrained_pod_shape(tmp_path: Path, platform_inputs):  # noqa: F811
    import json
    import os
    import subprocess
    import time

    if os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1":
        pytest.skip("set LOOM_RUN_DISPOSABLE_K3S=1 for actual isolated admission checks")
    namespace = "loom-identity-policy-test"
    container = _start_k3s()

    def apply(documents, *, dry_run=False):
        path = tmp_path / "policy-probe.yaml"
        path.write_text(yaml.safe_dump_all(documents))
        subprocess.run(["docker", "cp", str(path), container.get_wrapped_container().id + ":/tmp/probe.yaml"],
                       check=True, capture_output=True)
        return container.exec(["kubectl", "apply", "-f", "/tmp/probe.yaml", *(["--dry-run=server"] if dry_run else [])])

    try:
        _load_client(container)
        result = apply([
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": namespace, "labels": {"pod-security.kubernetes.io/enforce": "restricted"},
            }},
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
                "name": "loom-execution-attempt", "namespace": namespace,
            }, "automountServiceAccountToken": False},
        ])
        assert result.exit_code == 0, result.output.decode()
        # Match the deployed execution quota: even admission-only dry runs
        # must declare the resource requests enforced by this namespace.
        config, candidate, profile = platform_inputs
        files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        quota = deepcopy(next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "ResourceQuota"))
        quota["metadata"]["namespace"] = namespace
        quota["spec"]["hard"]["requests.ephemeral-storage"] = "16Gi"
        result = apply([quota])
        assert result.exit_code == 0, result.output.decode()
        pod = _pod(namespace)
        assert apply([pod], dry_run=True).exit_code != 0  # Existing default refuses root.
        policies = identity_policy_documents(namespace, "disposable-k3s")
        from scripts.ops.deploy_nebius_platform import Kubectl, install_task_identity_policy

        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}",
        ))
        kubeconfig.chmod(0o600)
        (tmp_path / "00-task-identity-policy.yaml").write_text(yaml.safe_dump_all(policies))
        install_task_identity_policy(Kubectl(kubeconfig), {"execution_namespace": namespace}, tmp_path)
        # Retain the default execution quota for the ordinary workload shapes;
        # bootstrap above additionally exercises the supported storage override.
        quota["spec"]["hard"].pop("requests.ephemeral-storage")
        result = apply([quota])
        assert result.exit_code == 0, result.output.decode()
        assert apply([pod], dry_run=True).exit_code != 0  # Policy setup retains restricted PSS.
        result = apply([{"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": namespace, "labels": identity_namespace_labels(),
        }}])
        assert result.exit_code == 0, result.output.decode()
        for _ in range(60):
            result = apply([pod], dry_run=True)
            if result.exit_code == 0:
                break
            time.sleep(0.2)
        assert result.exit_code == 0, result.output.decode()
        result = container.exec(["kubectl", "get", "validatingadmissionpolicy", policies[0]["metadata"]["name"], "-o", "json"])
        assert not json.loads(result.output).get("status", {}).get("typeChecking", {}).get("expressionWarnings")
        nonroot = deepcopy(pod)
        for sandbox in nonroot["spec"]["initContainers"][1:]:
            sandbox["securityContext"].update(runAsNonRoot=True)
            sandbox["securityContext"].pop("runAsUser")
            sandbox["securityContext"].pop("runAsGroup")
            sandbox["securityContext"]["capabilities"].pop("add")
        assert apply([nonroot], dry_run=True).exit_code == 0

        # Admission must accept the real platform controller/collector Pod
        # templates, including security inherited from their Pod defaults.
        config, candidate, profile = platform_inputs
        config["task_identity_policy"] = {
            "mode": "private-root-v1", "target_id": config["target_id"],
            "execution_namespace": config["execution_namespace"],
        }
        files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        platform_pods = []
        for document in files["60-execution.yaml"]:
            if document["kind"] == "ServiceAccount":
                account = deepcopy(document)
                account["metadata"]["namespace"] = namespace
                result = apply([account])
                assert result.exit_code == 0, result.output.decode()
            if document["kind"] == "Deployment":
                template = document["spec"]["template"]
            elif document["kind"] == "CronJob":
                template = document["spec"]["jobTemplate"]["spec"]["template"]
            else:
                continue
            platform_pods.append({"apiVersion": "v1", "kind": "Pod", "metadata": {
                **deepcopy(template.get("metadata", {})), "namespace": namespace,
                "name": document["metadata"]["name"] + "-policy-check",
            }, "spec": deepcopy(template["spec"])})
        assert len(platform_pods) == 2
        admitted_shapes = [
            *platform_pods, _pod(namespace, user="0:1001", home="/root"),
            _pod(namespace, deferred=True), _pod(namespace, user="1001:1002", home="/home/miles", deferred=True),
        ]
        failures = []
        for admitted in admitted_shapes:
            result = apply([admitted], dry_run=True)
            if result.exit_code != 0:
                failures.append(result.output.decode())
        assert not failures, "\n".join(failures)

        from tests.unit.test_task_fixtures import _plan as fixture_plan

        fixture_pod = deepcopy(pod)
        fixture_pod["spec"]["initContainers"].insert(1, _sidecar(fixture_plan().sidecars[0]))
        result = apply([fixture_pod], dry_run=True)
        assert result.exit_code == 0, result.output.decode()
        fixture_mutations = [
            lambda p, c: c.update(volumeMounts=[{"name": "workspace", "mountPath": "/workspace"}]),
            lambda p, c: c["securityContext"].update(runAsUser=1000),
            lambda p, c: c["securityContext"].update(runAsGroup=0),
            lambda p, c: c["securityContext"].update(readOnlyRootFilesystem=False),
            lambda p, c: c["securityContext"]["capabilities"].update(add=["NET_BIND_SERVICE"]),
            lambda p, c: c.update(envFrom=[{"secretRef": {"name": "controller-token"}}]),
            lambda p, c: c.update(env=[{"name": "HOST", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}]),
            lambda p, c: c.update(lifecycle={"postStart": {"exec": {"command": ["true"]}}}),
            lambda p, c: [c.pop(key) for key in ("restartPolicy", "startupProbe", "readinessProbe")],
            lambda p, c: p["spec"].update(hostAliases=[{"ip": "127.0.0.1", "hostnames": ["fixture.example"]}]),
        ]
        for mutate in fixture_mutations:
            bad = deepcopy(fixture_pod)
            mutate(bad, bad["spec"]["initContainers"][1])
            result = apply([bad], dry_run=True)
            assert result.exit_code != 0, "unsafe fixture variation was admitted"
            assert "private-root-v1" in result.output.decode()
        regular_fixture = deepcopy(fixture_pod)
        fixture_container = regular_fixture["spec"]["initContainers"].pop(1)
        fixture_container.pop("restartPolicy")
        regular_fixture["spec"]["containers"].append(fixture_container)
        result = apply([regular_fixture], dry_run=True)
        assert result.exit_code != 0, "regular-container fixture was admitted"
        assert "private-root-v1" in result.output.decode()

        for platform_pod in platform_pods:
            for section in ("containers", "initContainers"):
                for index in range(len(platform_pod["spec"].get(section, []))):
                    for override in ({"runAsNonRoot": False}, {"runAsUser": 0}):
                        bad = deepcopy(platform_pod)
                        bad["spec"][section][index]["securityContext"].update(override)
                        result = apply([bad], dry_run=True)
                        assert result.exit_code != 0, "unsafe inherited-security override was admitted"
                        assert "private-root-v1" in result.output.decode()
        mutations = [
            lambda p: p["spec"]["containers"][0]["securityContext"].update(runAsUser=0, runAsNonRoot=False),
            lambda p: p["spec"]["initContainers"][1]["securityContext"]["capabilities"]["add"].append("SYS_ADMIN"),
            lambda p: p["spec"]["initContainers"][1]["securityContext"].update(allowPrivilegeEscalation=True),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"][0].update(name="workspace"),
            lambda p: p["spec"]["initContainers"][1].update(envFrom=[{"secretRef": {"name": "controller-token"}}]),
            lambda p: p["spec"].update(hostPID=True),
            lambda p: p["spec"].update(shareProcessNamespace=True),
            lambda p: p["spec"]["initContainers"][1].update(command=["/bin/sh"]),
            lambda p: p["spec"]["initContainers"][0]["securityContext"]["capabilities"].update(add=["NET_BIND_SERVICE"]),
            lambda p: p["metadata"]["annotations"].update({"loom.openai.com/target-id": "other-target"}),
            lambda p: p["spec"]["volumes"][0].update(emptyDir=None, hostPath={"path": "/"}),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"][2].update(name="verifier-sandbox-socket"),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"][2].update(subPath="sandbox.sock"),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"][2].update(mountPath="/etc/shadow"),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"].pop(),
        ]
        for mutate in mutations:
            bad = deepcopy(pod)
            mutate(bad)
            result = apply([bad], dry_run=True)
            assert result.exit_code != 0, "unsafe variation was admitted"
        # A policy bound to this namespace must not affect another namespace.
        result = apply([
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace + "-other"}},
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
                "name": "outside-policy-probe", "namespace": namespace + "-other",
            }},
        ])
        assert result.exit_code == 0
        other = deepcopy(pod)
        other["metadata"]["namespace"] = namespace + "-other"
        other["spec"]["serviceAccountName"] = "outside-policy-probe"
        other["spec"]["containers"][0]["securityContext"].update(runAsUser=0, runAsNonRoot=False)
        assert apply([other], dry_run=True).exit_code == 0

        # Ephemeral containers are admitted on a separate subresource. Park a
        # Pod on a nonexistent node so this test never pulls an image or runs it.
        parked = deepcopy(pod)
        parked["spec"]["nodeName"] = "loom-admission-nonexistent-node"
        assert apply([parked]).exit_code == 0
        from kubernetes import client
        from kubernetes.client.exceptions import ApiException

        core = client.CoreV1Api()
        ephemeral = {"name": "diagnostic", "image": "invalid.local/admission-only:unused", "securityContext": {
            "runAsNonRoot": True, "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
        }}
        core.patch_namespaced_pod_ephemeralcontainers(
            parked["metadata"]["name"], namespace, {"spec": {"ephemeralContainers": [ephemeral]}}, dry_run="All",
        )
        ephemeral["securityContext"].update(runAsUser=0, runAsNonRoot=False)
        with pytest.raises(ApiException) as rejected:
            core.patch_namespaced_pod_ephemeralcontainers(
                parked["metadata"]["name"], namespace, {"spec": {"ephemeralContainers": [ephemeral]}}, dry_run="All",
            )
        assert rejected.value.status in {403, 422}
        assert "private-root-v1" in rejected.value.body
    finally:
        container.stop()


def _guest_pod(namespace: str, *, emulated_auth: bool = False) -> dict:
    from loom.execution_contract import workload_requirements_from_task
    from loom.pipeline.keys import canonical_digest
    from tests.unit.test_guest_execution_materialization import _compile, _guest_inputs

    task, trial, profile = _guest_inputs(*(["emulated_pkcs11_authentication"] if emulated_auth else []))
    if emulated_auth:
        profile = profile.model_copy(update={"supports_emulated_pkcs11": True})
    plan = _compile(task, trial, profile)
    lease = _lease(namespace)
    lease.target_id = "disposable-auth-guest" if emulated_auth else "disposable-guest"
    lease.execution_class_id = plan.execution_class_id
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    template = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=namespace,
    ))["spec"]["template"]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {
        **template["metadata"], "name": "guest-admission", "namespace": namespace,
    }, "spec": template["spec"]}


@pytest.mark.timeout(240)
@pytest.mark.parametrize("emulated_auth", [False, True])
def test_single_policy_allows_bound_guest_and_native_shapes_only(tmp_path: Path, emulated_auth: bool):
    import json
    import os
    import subprocess
    import time

    if os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1":
        pytest.skip("set LOOM_RUN_DISPOSABLE_K3S=1 for actual isolated admission checks")
    namespace = "loom-guest-policy-test"
    policies = identity_policy_documents(
        namespace, "disposable-k3s", guest_target_id="disposable-guest",
        emulated_auth_target_id="disposable-auth-guest" if emulated_auth else None,
    )
    container = _start_k3s()
    _load_client(container)

    def apply(documents, *, dry_run=False):
        path = tmp_path / "guest-policy.yaml"
        path.write_text(yaml.safe_dump_all(documents))
        subprocess.run(["docker", "cp", str(path), container.get_wrapped_container().id + ":/tmp/guest-policy.yaml"],
                       check=True, capture_output=True)
        return container.exec(["kubectl", "apply", "-f", "/tmp/guest-policy.yaml", *(["--dry-run=server"] if dry_run else [])])

    try:
        result = apply([
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": namespace, "labels": identity_namespace_labels(),
            }},
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
                "name": "loom-execution-attempt", "namespace": namespace,
            }, "automountServiceAccountToken": False},
            *policies,
        ])
        assert result.exit_code == 0, result.output.decode()
        # Admission can evaluate a new policy before its asynchronous static
        # type-checking status is published. Deployment requires that status.
        deadline = time.monotonic() + 30
        while True:
            observed = container.exec([
                "kubectl", "get", "validatingadmissionpolicy",
                policies[0]["metadata"]["name"], "-o", "json",
            ])
            assert observed.exit_code == 0, observed.output.decode()
            policy = json.loads(observed.output)
            status = policy.get("status", {})
            if (status.get("observedGeneration") == policy["metadata"]["generation"]
                    and status.get("observedGeneration", 0) > 0):
                break
            assert time.monotonic() < deadline, "policy type checking did not finish"
            time.sleep(0.2)
        assert not status.get("typeChecking", {}).get("expressionWarnings"), status
        guest = _guest_pod(namespace, emulated_auth=emulated_auth)
        for _ in range(60):
            result = apply([guest], dry_run=True)
            if result.exit_code == 0:
                break
            time.sleep(0.2)
        assert result.exit_code == 0, result.output.decode()
        assert apply([_pod(namespace)], dry_run=True).exit_code == 0
        for damage in ("owner-target", "foreign-target", "host-cap", "writable-root", "foreign-state", "host-volume", "unbounded-state", "memory-state", "shell", "probe", "host-sysctl"):
            changed = deepcopy(guest)
            sidecar = next(c for c in changed["spec"]["initContainers"] if c["name"] == "task-sandbox")
            if damage.endswith("target"):
                changed["metadata"]["annotations"]["loom.openai.com/target-id"] = (
                    "disposable-k3s" if damage == "owner-target" else "foreign")
            elif damage == "host-cap":
                sidecar["securityContext"]["capabilities"]["add"].append("SYS_ADMIN")
            elif damage == "writable-root":
                sidecar["securityContext"]["readOnlyRootFilesystem"] = False
            elif damage == "foreign-state":
                sidecar["volumeMounts"][-1]["name"] = "verifier-sandbox-guest-state"
            elif damage == "host-volume":
                volume = next(v for v in changed["spec"]["volumes"] if v["name"] == "task-sandbox-guest-state")
                volume.pop("emptyDir")
                volume["hostPath"] = {"path": "/"}
            elif damage in {"unbounded-state", "memory-state"}:
                volume = next(v for v in changed["spec"]["volumes"] if v["name"] == "task-sandbox-guest-state")
                if damage == "unbounded-state":
                    volume["emptyDir"].pop("sizeLimit")
                else:
                    volume["emptyDir"]["medium"] = "Memory"
            elif damage == "shell":
                sidecar["command"] = ["/bin/sh", "-c", "sleep 1000"]
            elif damage == "probe":
                sidecar["startupProbe"]["exec"]["command"] = ["/bin/sh", "-c", "true"]
            else:
                changed["spec"]["securityContext"]["sysctls"] = [{"name": "kernel.core_pattern", "value": "x"}]
            result = apply([changed], dry_run=True)
            assert result.exit_code != 0, damage
        # A warning followed by a clean update can omit the now-empty
        # typeChecking parent from the server-side-applied status. Exercise
        # actual recovery through the same gate used by protected rollout.
        broken = deepcopy(policies)
        broken[0]["spec"]["validations"].append({
            "expression": "has(object.spec.undefinedField)",
            "message": "Deliberate type-checking failure for recovery qualification.",
        })
        assert apply(broken).exit_code == 0
        deadline = time.monotonic() + 30
        while True:
            observed = container.exec([
                "kubectl", "get", "validatingadmissionpolicy",
                policies[0]["metadata"]["name"], "-o", "json",
            ])
            policy = json.loads(observed.output)
            status = policy.get("status", {})
            if status.get("observedGeneration") == policy["metadata"]["generation"]:
                assert status.get("typeChecking", {}).get("expressionWarnings")
                break
            assert time.monotonic() < deadline, "deliberate warning was not observed"
            time.sleep(0.2)
        from scripts.ops.deploy_nebius_platform import Kubectl, install_task_identity_policy

        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}",
        ))
        subprocess.run(["kubectl", "--kubeconfig", str(kubeconfig), "config", "rename-context", "default", "loom-rollout"],
                       check=True, capture_output=True)
        (tmp_path / "00-task-identity-policy.yaml").write_text(yaml.safe_dump_all(policies))
        install_task_identity_policy(Kubectl(kubeconfig), {"execution_namespace": namespace}, tmp_path)
    finally:
        container.stop()
