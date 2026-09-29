"""Run protected dispatch, journals and HTTP adapter together against fixed API rows."""
from __future__ import annotations

import copy
import json
import ssl
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_retirement_diagnostic_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_management_retirement_diagnostic_entry import application_material as application_material
from tests.ops.test_nebius_management_retirement_diagnostic_entry import checks as checks
from tests.ops.test_nebius_management_retirement_diagnostic_entry import cloud as cloud
from tests.ops.test_nebius_management_retirement_diagnostic_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_retirement_diagnostic_entry import installation as installation
from tests.ops.test_nebius_management_retirement_diagnostic_entry import management_inputs as management_inputs
from tests.ops.test_nebius_management_retirement_diagnostic_entry import material as material
from tests.ops.test_nebius_management_retirement_diagnostic_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_management_retirement_diagnostic_entry import private_diagnostic as private_diagnostic
from tests.ops.test_nebius_management_retirement_diagnostic_entry import private_retirement as private_retirement
from tests.ops.test_nebius_management_retirement_diagnostic_entry import private_upgrade as private_upgrade
from tests.ops.test_nebius_management_retirement_diagnostic_entry import retirement_request as retirement_request
from tests.ops.test_nebius_management_retirement_diagnostic_entry import setup_request as setup_request


def resource_path(doc):
    plural = {"Namespace": "namespaces", "ServiceAccount": "serviceaccounts", "ConfigMap": "configmaps",
        "ClusterRole": "clusterroles", "ClusterRoleBinding": "clusterrolebindings", "Role": "roles", "RoleBinding": "rolebindings",
        "NetworkPolicy": "networkpolicies", "Job": "jobs", "Deployment": "deployments",
        "ValidatingAdmissionPolicy": "validatingadmissionpolicies", "ValidatingAdmissionPolicyBinding": "validatingadmissionpolicybindings"}[doc["kind"]]
    prefix = "/api/v1" if doc["apiVersion"] == "v1" else "/apis/" + doc["apiVersion"]
    return prefix + ("/namespaces/" + doc["metadata"]["namespace"] if doc["metadata"].get("namespace") else "") + "/" + plural + "/" + doc["metadata"]["name"]


@pytest.fixture
def live(private_diagnostic, monkeypatch):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops.nebius_management_retirement_diagnostic_entry import load_diagnostic_inputs

    metadata, _, fake = private_diagnostic
    context = load_diagnostic_inputs(metadata)
    original = context.retirement
    binding = original.request.binding
    ns = binding.namespace
    rows = {resource_path(doc): copy.deepcopy(doc) for doc in fake.resources.values()}
    rows[resource_path(original.active_management)] = copy.deepcopy(original.active_management)
    for name, uid in (("kube-system", binding.kube_system_uid), (ns, binding.namespace_uid)):
        rows["/api/v1/namespaces/" + name] = {"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": name, "uid": uid, "labels": {"loom.nebius/management-installation": binding.installation_id,
                "pod-security.kubernetes.io/enforce": "restricted"}}}
    for doc in original.legacy_fence:
        rows[resource_path(doc)] = copy.deepcopy(doc) | {"status": {"observedGeneration": 1, "typeChecking": {}}}
    for target in original.request.targets:
        for name, uid in target.namespace_uids.items():
            rows["/api/v1/namespaces/" + name] = {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": str(uid), "labels": {"loom.nebius/environment-id": str(target.registration.environment_id),
                    "loom.nebius/incarnation": str(target.registration.incarnation)}}}
    state = SimpleNamespace(metadata=metadata, context=context, rows=rows, fake=fake, calls=[], pods=[], log=b"", pod_readback=None)
    pods_path = "/api/v1/namespaces/" + ns + "/pods"

    def http(request):
        assert request.headers["Authorization"] == "Bearer fixture-operator"
        path = request.url.path
        state.calls.append((request.method, str(request.url)))
        if request.method == "POST":
            assert path == "/apis/batch/v1/namespaces/" + ns + "/jobs"
            doc = json.loads(request.content)
            assert doc["metadata"]["name"].startswith("loom-retirement-probe-")
            if request.url.params.get("dryRun") == "All":
                result = fake.default_resource(doc)
            else:
                fake.create_resource(doc)
                result = fake.get_resource(doc)
                rows[resource_path(result)] = result
            return httpx.Response(201, json=result)
        assert request.method == "GET"
        if path == pods_path:
            if request.url.params.get("labelSelector"):
                assert request.url.params["limit"] == "2"
            # Kubernetes typed lists omit item TypeMeta, unlike individual GETs.
            items = [{k: v for k, v in pod.items() if k not in {"apiVersion", "kind"}} for pod in state.pods]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items})
        if path.startswith(pods_path + "/"):
            if path.endswith("/log"):
                assert request.url.params["limitBytes"] == "16384"
                assert request.url.params.get("previous", "false") == "false"
                return httpx.Response(200, content=state.log)
            return httpx.Response(200, json=state.pod_readback or state.pods[0])
        return httpx.Response(200, json=rows[path]) if path in rows else httpx.Response(404)

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(http))

    async def operator(connection):
        assert connection == original.original_inputs.operator_connection
        return ssl.create_default_context(), "fixture-operator"

    monkeypatch.setattr(entry, "_operator_transport", operator)
    path = Path(metadata["inputs_path"]).parent / "operation.json"
    path.write_text(json.dumps(metadata))
    path.chmod(0o600)
    state.operation_path = path
    return state


def complete(live):
    job, = [doc for doc in live.rows.values() if doc.get("kind") == "Job" and doc["metadata"]["name"].startswith("loom-retirement-probe-")]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    name, uid, ns = (job["metadata"][key] for key in ("name", "uid", "namespace"))
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name + "-fixture", "namespace": ns,
        "uid": str(uuid4()), "labels": copy.deepcopy(job["spec"]["template"]["metadata"]["labels"]),
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": name, "uid": uid, "controller": True}]},
        "spec": copy.deepcopy(job["spec"]["template"]["spec"]), "status": {"phase": "Succeeded", "containerStatuses": [
            {"name": c["name"], "restartCount": 0, "state": {"terminated": {"exitCode": 0}}} for c in job["spec"]["template"]["spec"]["containers"]]}}
    live.pods = [pod]
    report = {"schema": "loom.nebius-retirement-startup-probe.v1", "status": "observed", "stage": "complete",
        "checks": ["database_binding", "kubernetes_ca", "kubernetes_token", "database", "kubernetes"], "operations": [
            {"operation_id": str(target.operation_id), "phase": "pending", "runner_epoch": 0, "lease_present": False,
                "error_present": False, "resource_count": 3, "effects_started": False} for target in live.context.retirement.request.targets]}
    live.log = json.dumps(report).encode() + b"\n"
    return report


def invoke(live, capsys, action):
    from scripts.ops.nebius_management_entry import main

    code = main(str(live.operation_path), action)
    captured = capsys.readouterr()
    assert "private-" not in captured.out + captured.err
    return code, json.loads(captured.out)


def test_protected_diagnostic_dispatch_preserves_old_job_and_never_repeats_create(live, capsys):
    frozen = copy.deepcopy(live.rows)
    code, report = invoke(live, capsys, "preflight")
    assert code == 0 and report["status"] == "preflight_qualified"
    assert not any(method == "POST" for method, _ in live.calls)
    assert not Path(live.metadata["state_dir"]).exists()
    code, report = invoke(live, capsys, "install")
    assert code == 0 and report["status"] == "pending" and report["phase"] == "retirement-diagnostic"
    expected = complete(live)
    code, report = invoke(live, capsys, "install")
    assert code == 0 and report["status"] == "retirement_diagnostic_observed" and report["probe"] == expected
    assert invoke(live, capsys, "install")[1] == report
    assert all(live.rows[path] == doc for path, doc in frozen.items())
    assert len([url for method, url in live.calls if method == "POST" and "dryRun=" not in url]) == 1


def test_changed_original_state_prevents_any_diagnostic_mutation(live, capsys):
    baseline = copy.deepcopy(live.rows)
    original_job = next(path for path, doc in baseline.items() if doc.get("kind") == "Job")
    original_cm = next(path for path, doc in baseline.items() if doc.get("kind") == "ConfigMap")
    for change in ("uid", "settings", "not_failed", "still_active", "manager", "namespace"):
        live.rows.clear()
        live.rows.update(copy.deepcopy(baseline))
        if change == "uid":
            live.rows[original_job]["metadata"]["uid"] = str(uuid4())
        elif change == "settings":
            live.rows[original_cm]["data"]["retirement.json"] = "private-change"
        elif change == "not_failed":
            live.rows[original_job]["status"] = {}
        elif change == "still_active":
            live.rows[original_job]["status"]["active"] = 1
        elif change == "manager":
            live.rows[resource_path(live.context.retirement.active_management)]["metadata"]["uid"] = str(uuid4())
        else:
            name = next(iter(live.context.retirement.request.targets[0].namespace_uids))
            live.rows["/api/v1/namespaces/" + name]["metadata"]["uid"] = str(uuid4())
        code, report = invoke(live, capsys, "install")
        assert code == 0 and report["status"] == "blocked" and report["stage"] == "diagnostic_original", change
        assert not any(method == "POST" for method, _ in live.calls), change
        assert not Path(live.metadata["state_dir"]).exists(), change


def test_diagnostic_readback_rejects_ambiguous_foreign_or_changed_pod_and_private_logs(live, capsys):
    assert invoke(live, capsys, "install")[1]["status"] == "pending"
    for change in ("duplicate", "owner", "command", "restart", "sidecar", "raw_log", "oversized", "wrong_operation", "replaced_after_log"):
        complete(live)
        live.pod_readback = None
        if change == "duplicate":
            live.pods.append(copy.deepcopy(live.pods[0]))
        elif change == "owner":
            live.pods[0]["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
        elif change == "command":
            live.pods[0]["spec"]["containers"][0]["command"] = ["private-command"]
        elif change == "restart":
            live.pods[0]["status"]["containerStatuses"][0]["restartCount"] = 1
        elif change == "sidecar":
            live.pods[0]["spec"]["containers"].append({"name": "private-sidecar", "image": "foreign"})
        elif change == "raw_log":
            live.log = b"private-traceback\n" + live.log
        elif change == "oversized":
            live.log = b"x" * 16385
        elif change == "wrong_operation":
            result = json.loads(live.log)
            result["operations"][0]["operation_id"] = str(uuid4())
            live.log = json.dumps(result).encode()
        else:
            live.pod_readback = copy.deepcopy(live.pods[0])
            live.pod_readback["metadata"]["uid"] = str(uuid4())
        code, report = invoke(live, capsys, "install")
        assert code == 0 and report["status"] == "blocked", change
        assert report["stage"] in {"diagnostic_pod", "diagnostic_log", "diagnostic_readback"}, change
    assert len([url for method, url in live.calls if method == "POST" and "dryRun=" not in url]) == 1
