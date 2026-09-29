"""Recovery uses frozen original receipts, current DNS and one exact new Job."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_gateway import (
    recovery_operation,
    recovery_report,
    startup_report,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    application_material as application_material,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import checks as checks
from tests.ops.test_nebius_management_retirement_diagnostic_live import cloud as cloud
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    complete,
    invoke,
    resource_path,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_retirement_diagnostic_live import installation as installation
from tests.ops.test_nebius_management_retirement_diagnostic_live import live as live
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import material as material
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    private_diagnostic as private_diagnostic,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    private_retirement as private_retirement,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    retirement_request as retirement_request,
)
from tests.ops.test_nebius_management_retirement_diagnostic_live import (
    setup_request as setup_request,
)


@pytest.fixture
def recovery(live, capsys, tmp_path, monkeypatch):
    assert invoke(live, capsys, "install")[1]["status"] == "pending"
    complete(live)
    live.log = json.dumps({"schema": "loom.nebius-retirement-startup-probe.v1", "status": "unavailable",
        "stage": "database", "checks": ["database_binding", "kubernetes_ca", "kubernetes_token"],
        "error_type": "OperationalError", "http_status": None, "operations": []}).encode()
    assert invoke(live, capsys, "install")[1]["status"] == "retirement_diagnostic_observed"
    original = live.metadata
    dns_uid = str(uuid4())
    live.rows["/api/v1/namespaces/kube-system/services/coredns"] = {
        "apiVersion": "v1", "kind": "Service", "metadata": {"name": "coredns", "namespace": "kube-system", "uid": dns_uid},
        "spec": {"type": "ClusterIP", "clusterIP": "10.43.0.10", "selector": {"k8s-app": "coredns"},
            "ports": [{"port": 53, "targetPort": 53, "protocol": protocol} for protocol in ("TCP", "UDP")]}}
    metadata = recovery_operation(tmp_path) | {key: original[key] for key in ("candidate", "installation_id", "namespace")}
    inputs = {"schema_version": "loom.nebius-management-retirement-recovery-private-inputs.v1",
        "diagnostic_operation": original, "diagnostic_journal_sha256": hashlib.sha256(
            (Path(original["state_dir"]) / "job/stage.json").read_bytes()).hexdigest(), "dns_service_uid": dns_uid}
    path = Path(metadata["inputs_path"])
    path.parent.mkdir(mode=0o700)
    path.write_text(json.dumps(inputs))
    path.chmod(0o600)
    metadata["inputs_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    live.inputs = inputs
    live.metadata = metadata
    live.operation_path = path.parent / "operation.json"
    live.operation_path.write_text(json.dumps(metadata))
    live.operation_path.chmod(0o600)
    live.recovery_pods, live.recovery_log = [], b""
    live.calls.clear()
    original_http = live.http

    def http(request):
        body = json.loads(request.content) if request.method == "POST" else {}
        if body.get("metadata", {}).get("name", "").startswith("loom-retirement-recovery-"):
            live.calls.append((request.method, str(request.url)))
            assert body["kind"] in {"Job", "NetworkPolicy"}
            expected_path = resource_path(body).rsplit("/", 1)[0]
            assert request.url.path == expected_path
            if request.url.params.get("dryRun") == "All":
                result = live.fake.default_resource(body)
            else:
                live.fake.create_resource(body)
                result = live.fake.get_resource(body)
                live.rows[resource_path(result)] = result
            return httpx.Response(201, json=result)
        if request.method == "GET" and live.recovery_pods:
            pod = live.recovery_pods[0]
            base = "/api/v1/namespaces/" + metadata["namespace"] + "/pods"
            if request.url.path == base and request.url.params.get("labelSelector") == (
                    "batch.kubernetes.io/controller-uid=" + pod["metadata"]["ownerReferences"][0]["uid"]):
                return httpx.Response(200, json={"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": live.recovery_pods})
            if request.url.path == base + "/" + pod["metadata"]["name"] + "/log":
                return httpx.Response(200, content=live.recovery_log)
            if request.url.path == base + "/" + pod["metadata"]["name"]:
                return httpx.Response(200, json=pod)
        return original_http(request)

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(http))
    return live


def complete_recovery(live):
    job, = [doc for doc in live.rows.values() if doc.get("kind") == "Job"
        and doc["metadata"]["name"].startswith("loom-retirement-recovery-")]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    pod = copy.deepcopy(live.pods[0])
    pod["metadata"].update(name=job["metadata"]["name"] + "-fixture", uid=str(uuid4()),
        labels=copy.deepcopy(job["spec"]["template"]["metadata"]["labels"]))
    pod["metadata"]["ownerReferences"][0].update(name=job["metadata"]["name"], uid=job["metadata"]["uid"])
    pod["spec"] = copy.deepcopy(job["spec"]["template"]["spec"])
    live.recovery_pods = [pod]
    report = recovery_report()
    targets = live.context.retirement.request.targets
    report["startup"]["operations"] = [startup_report()["operations"][0] | {"operation_id": str(target.operation_id)} for target in targets]
    report["operations"] = [recovery_report()["operations"][0] | {"operation_id": str(target.operation_id)} for target in targets]
    live.recovery_log = json.dumps(report).encode()
    return report


@pytest.mark.parametrize("extra_selector", [{}, {"loom.test/dns-instance": "native"}])
def test_connected_recovery_preserves_old_evidence_and_requires_release_report(recovery, capsys, extra_selector):
    recovery.rows["/api/v1/namespaces/kube-system/services/coredns"]["spec"]["selector"].update(extra_selector)
    frozen = copy.deepcopy(recovery.rows)
    assert invoke(recovery, capsys, "preflight")[1]["status"] == "preflight_qualified"
    assert not Path(recovery.metadata["state_dir"]).exists()
    assert not any(method == "POST" for method, _ in recovery.calls)
    assert invoke(recovery, capsys, "install")[1]["phase"] == "retirement-recovery"
    report = complete_recovery(recovery)
    result = invoke(recovery, capsys, "install")[1]
    assert result["status"] == "retirement_recovered" and result["recovery"] == report
    assert invoke(recovery, capsys, "install")[1] == result
    assert all(recovery.rows[key] == value for key, value in frozen.items())
    assert len([url for method, url in recovery.calls if method == "POST" and "dryRun=" not in url]) == 2


@pytest.mark.parametrize("selector", [
    {}, {"loom.test/dns-instance": "native"},
    {"k8s-app": "kube-dns", "loom.test/dns-instance": "native"}, None, [],
])
def test_additional_dns_selectors_cannot_replace_required_native_selector(recovery, capsys, selector):
    recovery.rows["/api/v1/namespaces/kube-system/services/coredns"]["spec"]["selector"] = selector
    code, result = invoke(recovery, capsys, "install")
    assert code == 0 and result["status"] == "blocked" and result["stage"] == "recovery_dns"
    assert not any(method == "POST" for method, _ in recovery.calls)
    assert not Path(recovery.metadata["state_dir"]).exists()


@pytest.mark.parametrize("damage", ["dns_uid", "dns_selector", "dns_deleting", "diagnostic_success", "diagnostic_uid",
    "original_active", "manager_uid", "legacy_pod", "fence", "target_namespace"])
def test_changed_recovery_preconditions_block_before_any_write(recovery, capsys, damage):
    dns = recovery.rows["/api/v1/namespaces/kube-system/services/coredns"]
    if damage == "dns_uid":
        dns["metadata"]["uid"] = str(uuid4())
    elif damage == "dns_selector":
        dns["spec"]["selector"] = {"k8s-app": "kube-dns"}
    elif damage == "dns_deleting":
        dns["metadata"]["deletionTimestamp"] = "2026-09-29T00:00:00Z"
    elif damage == "diagnostic_success":
        complete(recovery)
    elif damage == "diagnostic_uid":
        job = next(doc for doc in recovery.rows.values() if doc.get("kind") == "Job" and doc["metadata"]["name"].startswith("loom-retirement-probe-"))
        job["metadata"]["uid"] = str(uuid4())
    elif damage == "original_active":
        job = next(doc for doc in recovery.rows.values() if doc.get("kind") == "Job" and "-probe-" not in doc["metadata"]["name"])
        job["status"]["active"] = 1
    elif damage == "manager_uid":
        recovery.rows[resource_path(recovery.context.retirement.active_management)]["metadata"]["uid"] = str(uuid4())
    elif damage == "legacy_pod":
        recovery.pods[0]["spec"]["serviceAccountName"] = "loom-management-provisioner"
    elif damage == "fence":
        del recovery.rows[resource_path(recovery.context.retirement.legacy_fence[0])]
    else:
        name = next(iter(recovery.context.retirement.request.targets[0].namespace_uids))
        recovery.rows["/api/v1/namespaces/" + name]["metadata"]["uid"] = str(uuid4())
    code, result = invoke(recovery, capsys, "install")
    assert code == 0 and result["status"] == "blocked"
    assert result["stage"] == ("recovery_dns" if damage.startswith("dns_") else "recovery_original")
    assert not any(method == "POST" for method, _ in recovery.calls)
    assert not Path(recovery.metadata["state_dir"]).exists()


@pytest.mark.parametrize("damage", ["journal", "anchor", "candidate", "unknown_input", "zero_dns_uid"])
def test_private_recovery_changes_fail_before_transport(recovery, capsys, damage):
    inputs = recovery.inputs
    old = inputs["diagnostic_operation"]
    if damage == "journal":
        path = Path(old["state_dir"]) / "job/stage.json"
        path.write_bytes(path.read_bytes() + b" ")
    elif damage == "anchor":
        (Path(old["anchor_dir"]) / (old["installation_id"] + ".json")).unlink()
    elif damage == "candidate":
        old["candidate"] = "e" * 40
    elif damage == "unknown_input":
        inputs["command"] = "private-command"
    else:
        inputs["dns_service_uid"] = "00000000-0000-0000-0000-000000000000"
    path = Path(recovery.metadata["inputs_path"])
    path.write_text(json.dumps(inputs))
    recovery.metadata["inputs_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    recovery.operation_path.write_text(json.dumps(recovery.metadata))
    assert invoke(recovery, capsys, "install")[0] == 1
    assert not recovery.calls


def test_runtime_failure_is_closed_and_never_retried(recovery, capsys):
    assert invoke(recovery, capsys, "install")[1]["status"] == "pending"
    report = complete_recovery(recovery) | {"status": "blocked", "stage": "retirement", "error_type": "ManagementError", "operations": []}
    recovery.recovery_log = json.dumps(report).encode()
    result = invoke(recovery, capsys, "install")[1]
    assert result["status"] == "blocked" and result["recovery"] == report
    assert invoke(recovery, capsys, "install")[1] == result
    assert len([url for method, url in recovery.calls if method == "POST" and "dryRun=" not in url]) == 2


def test_dns_named_container_ports_do_not_change_native_selector_authority(recovery, capsys):
    service = recovery.rows["/api/v1/namespaces/kube-system/services/coredns"]
    for port in service["spec"]["ports"]:
        port["targetPort"] = "dns-" + port["protocol"].lower()
    assert invoke(recovery, capsys, "preflight")[1]["status"] == "preflight_qualified"
    assert not any(method == "POST" for method, _ in recovery.calls)
