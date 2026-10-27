"""Protected evidence must refer to the installed subject and exact backup Job."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


@pytest.fixture
def evidence(installation):
    from scripts.ops.nebius_management_evidence import HTTPSManagementEvidenceAPI
    from scripts.ops.nebius_management_install import render_installation
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    rendered = render_installation(request)
    job = copy.deepcopy(rendered.files["85-backup-verify.yaml"][0])
    job["metadata"]["uid"] = str(uuid4())
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": job["metadata"]["name"] + "-abcde", "namespace": binding.namespace, "uid": str(uuid4()),
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": job["metadata"]["name"],
                             "uid": job["metadata"]["uid"], "controller": True}],
        "labels": {"batch.kubernetes.io/controller-uid": job["metadata"]["uid"]},
    }, "spec": copy.deepcopy(job["spec"]["template"]["spec"]), "status": {"phase": "Succeeded"}}
    pod["status"]["containerStatuses"] = [{"name": "loom-platform-backup", "restartCount": 0,
                                             "state": {"terminated": {"exitCode": 0}}}]
    pod["status"]["initContainerStatuses"] = [{"name": "pg-dump", "restartCount": 0,
                                                 "state": {"terminated": {"exitCode": 0}}}]
    account = {"apiVersion": "v1", "kind": "ServiceAccount", "automountServiceAccountToken": False,
               "metadata": {"name": "loom-management-provisioner", "namespace": binding.namespace,
                            "uid": str(uuid4()), "labels": {"loom.nebius/management-installation": binding.installation_id}}}
    values = {"pod": pod, "job": job, "account": account, "calls": [], "fault": None,
              "listed_type": {"apiVersion": "v1", "kind": "Pod"},
              "list_type": {"apiVersion": "v1", "kind": "PodList"}}
    report = {"backup_key": binding.namespace + "/2026/09/24/180000-" + "a" * 12 + ".dump",
              "sha256": "a" * 64, "bytes": 12}

    def handler(req):
        values["calls"].append(req)
        path = req.url.path
        if path.endswith("/namespaces/kube-system"):
            return httpx.Response(200, json={"kind": "Namespace", "metadata": {
                "name": "kube-system", "uid": binding.kube_system_uid}})
        if path.endswith("/namespaces/" + binding.namespace):
            return httpx.Response(200, json={"kind": "Namespace", "metadata": {
                "name": binding.namespace, "uid": binding.namespace_uid,
                "labels": {"loom.nebius/management-installation": binding.installation_id,
                           "pod-security.kubernetes.io/enforce": "restricted"}}})
        if path.endswith("/serviceaccounts/loom-management-provisioner/token"):
            assert req.method == "POST"
            if values["fault"] == "lost_token":
                raise httpx.ReadTimeout("private token response")
            if values["fault"] == "replaced_account":
                values["account"]["metadata"]["uid"] = str(uuid4())
            return httpx.Response(201, json={"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                "status": {"token": "private-runtime-token", "expirationTimestamp":
                           (datetime.now(UTC) + timedelta(minutes=10)).isoformat()}})
        if path.endswith("/serviceaccounts/loom-management-provisioner"):
            return httpx.Response(200, json=values["account"])
        if path.endswith("/jobs/" + job["metadata"]["name"]):
            return httpx.Response(200, json=values["job"])
        if path.endswith("/pods"):
            assert req.url.params["labelSelector"] == "batch.kubernetes.io/controller-uid=" + job["metadata"]["uid"]
            listed = {key: value for key, value in values["pod"].items() if key not in {"apiVersion", "kind"}}
            return httpx.Response(200, json={**values["list_type"], "metadata": {},
                                            "items": [{**listed, **values["listed_type"]}]})
        if path.endswith("/log"):
            assert req.url.params["container"] == "loom-platform-backup"
            if values["fault"] == "replaced_pod":
                values["pod"]["metadata"]["uid"] = str(uuid4())
            return httpx.Response(200, content=values.get("log", (json.dumps(report) + "\n").encode()))
        if path.endswith("/pods/" + pod["metadata"]["name"]):
            return httpx.Response(200, json=values["pod"])
        raise AssertionError((req.method, str(req.url)))

    with HTTPSManagementEvidenceAPI(binding=binding, rendered=rendered, api_server="https://cluster.example.com",
                                    ssl_context=ssl.create_default_context(), token="operator-token") as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handler))
        yield api, values, report


def test_short_lived_token_is_for_exact_recorded_runtime_account(evidence):
    api, values, _ = evidence
    token = api.runtime_token(service_account_uid=values["account"]["metadata"]["uid"])
    assert token == "private-runtime-token"
    writes = [req for req in values["calls"] if req.method != "GET"]
    assert len(writes) == 1
    assert json.loads(writes[0].content) == {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                                          "spec": {"audiences": [], "expirationSeconds": 600}}


@pytest.mark.parametrize("fault", ["lost_token", "replaced_account", "foreign_account"])
def test_runtime_token_does_not_retry_or_accept_account_replacement(evidence, fault):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, _ = evidence
    uid = values["account"]["metadata"]["uid"]
    values["fault"] = fault
    if fault == "foreign_account":
        values["account"]["metadata"]["labels"] = {}
    with pytest.raises(ManagementInstallError) as error:
        api.runtime_token(service_account_uid=uid)
    assert "private" not in str(error.value)
    writes = [req for req in values["calls"] if req.method != "GET"]
    assert len(writes) == (0 if fault == "foreign_account" else 1)


def test_backup_report_is_from_exact_completed_job_pod_and_uploader(evidence):
    api, values, report = evidence
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report
    assert all(req.method == "GET" for req in values["calls"])


def test_backup_report_accepts_real_successful_uploader_entrypoint(evidence, tmp_path, monkeypatch, capsys):
    from loom import nebius_platform_bootstrap as bootstrap

    api, values, _ = evidence
    dump = tmp_path / "loom.dump"
    dump.write_bytes(b"PGDMP-test-backup")
    digest = hashlib.sha256(dump.read_bytes()).hexdigest()
    uploaded = []

    class Storage:
        def upload_file(self, filename, bucket, key, **kwargs):
            assert filename == str(dump) and bucket == "dedicated-backups"
            assert kwargs == {"ExtraArgs": {"Metadata": {"sha256": digest}}}
            uploaded.append(key)

        def head_object(self, **kwargs):
            assert kwargs == {"Bucket": "dedicated-backups", "Key": uploaded[0]}
            return {"ContentLength": 17, "Metadata": {"Sha256": digest}}

    config = tmp_path / "config.json"
    config.write_text(json.dumps({"namespace": api.binding.namespace, "region": "eu-north1",
        "storage_endpoint": "https://storage.example.com", "buckets": {"backup": "dedicated-backups"}}))
    monkeypatch.setattr(bootstrap, "Path", lambda path: dump if path == "/backup/loom.dump" else Path(path))
    monkeypatch.setattr(bootstrap.boto3, "client", lambda *args, **kwargs: Storage())
    monkeypatch.setattr(sys, "argv", ["bootstrap", "backup"])
    monkeypatch.setenv("LOOM_PLATFORM_CONFIG", str(config))
    monkeypatch.setenv("LOOM_BACKUP_ACCESS_KEY", "fixture-access-key")
    monkeypatch.setenv("LOOM_BACKUP_SECRET_KEY", "fixture-secret-key")
    assert bootstrap.main() == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    values["log"] = captured.out.encode()
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == {
        "backup_key": uploaded[0], "sha256": digest, "bytes": 17,
    }


@pytest.mark.parametrize("suffix", ["Nebius platform backup complete\n", "Nebius platform backup complete\r\n"])
def test_backup_report_accepts_exact_cli_success_trailer(evidence, suffix):
    api, values, report = evidence
    values["log"] = (json.dumps(report) + "\n" + suffix).encode()
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report


@pytest.mark.parametrize("extra", ["arbitrary private text", "Nebius platform configure complete",
    "Nebius platform backup complete\nextra", "Nebius platform backup complete\nNebius platform backup complete",
    '{"backup_key":"second-report"}', ""])
def test_backup_report_rejects_arbitrary_or_duplicate_trailers(evidence, extra):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, report = evidence
    values["log"] = (json.dumps(report) + "\n" + extra + "\n").encode() if extra else b""
    with pytest.raises(ManagementInstallError) as caught:
        api.backup_report(job_uid=values["job"]["metadata"]["uid"])
    assert caught.value.stage == "backup_log"


@pytest.mark.parametrize("omitted", [("apiVersion",), ("kind",), ("apiVersion", "kind")])
def test_backup_evidence_accepts_typed_list_items_without_type_meta(evidence, omitted):
    api, values, report = evidence
    for key in omitted:
        values["listed_type"].pop(key)
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report
    assert all(request.method == "GET" for request in values["calls"])


@pytest.mark.parametrize("target,field,value", [
    ("list_type", "apiVersion", "apps/v1"), ("list_type", "apiVersion", None),
    ("list_type", "kind", "SecretList"), ("list_type", "kind", None),
    ("listed_type", "apiVersion", "apps/v1"), ("listed_type", "apiVersion", None),
    ("listed_type", "kind", "Secret"), ("listed_type", "kind", None),
])
def test_backup_evidence_rejects_explicit_conflicting_collection_or_item_type(evidence, target, field, value):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, _ = evidence
    values[target][field] = value
    with pytest.raises(ManagementInstallError):
        api.backup_report(job_uid=values["job"]["metadata"]["uid"])
    assert not any(request.url.path.endswith("/log") for request in values["calls"])


def test_backup_pod_accepts_kubernetes_equivalent_resource_quantities(evidence):
    api, values, report = evidence
    values["pod"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "0.1"
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report


@pytest.mark.parametrize("seconds", [300, 0])
def test_backup_pod_qualifies_only_standard_admission_tolerations(evidence, seconds):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, report = evidence
    values["pod"]["spec"]["tolerations"].extend([
        {"key": "node.kubernetes.io/" + key, "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": seconds}
        for key in ("not-ready", "unreachable")
    ])
    if seconds == 300:
        assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report
    else:
        with pytest.raises(ManagementInstallError):
            api.backup_report(job_uid=values["job"]["metadata"]["uid"])


@pytest.mark.parametrize("fault,stage", [("owner", "backup_pod_identity"), ("image", "backup_pod_template"),
    ("failed", "backup_job"), ("replaced_pod", "backup_readback"), ("restarted", "backup_pod_status"),
    ("pod_phase", "backup_pod_status")])
def test_wrong_or_changed_backup_execution_never_supplies_object_proof(evidence, fault, stage):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, _ = evidence
    values["fault"] = fault
    if fault == "owner":
        values["pod"]["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif fault == "image":
        values["pod"]["spec"]["containers"][0]["image"] = "foreign:latest"
    elif fault == "failed":
        values["job"]["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
    elif fault == "restarted":
        values["pod"]["status"]["containerStatuses"][0]["restartCount"] = 1
    elif fault == "pod_phase":
        values["pod"]["status"]["phase"] = "Running"
    with pytest.raises(ManagementInstallError) as error:
        api.backup_report(job_uid=values["job"]["metadata"]["uid"])
    assert error.value.stage == stage
    if fault != "replaced_pod":
        assert not any(req.url.path.endswith("/log") for req in values["calls"])
