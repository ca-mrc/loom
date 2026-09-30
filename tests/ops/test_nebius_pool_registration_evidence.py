"""A recorded Job is not registration proof without its exact successful runtime."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import rfc8785
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_registration import request


@pytest.fixture
def registration_live(tmp_path):
    from scripts.ops.nebius_pool_registration import (
        HTTPSPoolRegistrationAPI,
        stage_pool_registration,
    )

    current = request()
    fake = PhaseAPI(current.binding)
    stage_pool_registration(request=current, api=fake, state_dir=tmp_path)
    job = next(row for row in fake.resources.values() if row["kind"] == "Job")
    config = next(row for row in fake.resources.values() if row["kind"] == "ConfigMap")
    name, uid = job["metadata"]["name"], job["metadata"]["uid"]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        **copy.deepcopy(job["spec"]["template"]["metadata"]), "namespace": current.binding.namespace,
        "name": name + "-abc", "uid": str(uuid4()), "ownerReferences": [{"apiVersion": "batch/v1",
            "kind": "Job", "name": name, "uid": uid, "controller": True, "blockOwnerDeletion": True}]},
        "spec": copy.deepcopy(job["spec"]["template"]["spec"]), "status": {"phase": "Succeeded",
            "containerStatuses": [{"name": "register", "restartCount": 0, "state": {"terminated": {"exitCode": 0}}}]}}
    checksum = hashlib.sha256(rfc8785.dumps(current.spec.model_dump(mode="json")) + b"\n").hexdigest()
    report = {"schema_version": "loom.pool-installation-receipt.v1", "operation_id": str(current.spec.operation_id),
        "pool_id": str(current.spec.pool_id), "installation_sha256": checksum, "mode": "closed", "participants": 3, "machines": 5}
    state = SimpleNamespace(request=current, state_dir=tmp_path, job=job, config=config, pod=pod, report=report,
        log=None, calls=[], extra_pod=False, continuation=False, namespace_drift=False, final_drift=False)

    def respond(message):
        state.calls.append(message)
        assert message.method == "GET"
        path = message.url.path
        binding = current.binding
        if path in ("/api/v1/namespaces/kube-system", "/api/v1/namespaces/" + binding.namespace):
            namespace = path.rsplit("/", 1)[-1]
            identity = binding.kube_system_uid if namespace == "kube-system" else binding.namespace_uid
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": namespace, "uid": str(uuid4()) if state.namespace_drift else identity,
                "labels": {"loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        if "/configmaps/" in path:
            return httpx.Response(200, json=state.config)
        if "/jobs/" in path:
            return httpx.Response(200, json=state.job)
        base = "/api/v1/namespaces/" + binding.namespace + "/pods"
        if path == base:
            assert dict(message.url.params) == {"labelSelector": "batch.kubernetes.io/controller-uid=" + uid, "limit": "2"}
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "PodList",
                "metadata": {"continue": "next" if state.continuation else ""}, "items": [state.pod] * (2 if state.extra_pod else 1)})
        if path.endswith("/log"):
            assert dict(message.url.params) == {"container": "register", "limitBytes": "16384", "timestamps": "false"}
            return httpx.Response(200, content=state.log if state.log is not None else json.dumps(state.report).encode())
        assert path == base + "/" + state.pod["metadata"]["name"]
        value = copy.deepcopy(state.pod)
        if state.final_drift:
            value["metadata"]["uid"] = str(uuid4())
        return httpx.Response(200, json=value)

    api = HTTPSPoolRegistrationAPI(request=current, api_server="https://cluster.example", ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
    return api, state


def test_successful_registration_is_bound_to_actual_job_pod_and_committed_receipt(registration_live):
    api, state = registration_live
    with api:
        proof = api.registration_report(state.state_dir)
        assert proof == {"job_uid": state.job["metadata"]["uid"], "pod_uid": state.pod["metadata"]["uid"],
            "registration": state.report}
        assert api.registration_report(state.state_dir) == proof


@pytest.mark.parametrize("damage", ["pending", "failed", "replaced_job", "config", "extra_pod", "continuation",
    "namespace_drift", "owner", "image", "mount", "privileged", "restart", "nonzero", "report", "checksum",
    "operation", "pool", "mode", "counts", "bool_count", "log_secret", "log_oversize", "trailer", "final_drift", "lost_state"])
def test_unqualified_registration_never_becomes_activation_evidence(registration_live, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = registration_live
    if damage == "pending":
        state.job["status"] = {}
    elif damage == "failed":
        state.job["status"]["conditions"] = [{"type": "Failed", "status": "True"}]
    elif damage == "replaced_job":
        state.job["metadata"]["uid"] = str(uuid4())
    elif damage == "config":
        state.config["data"]["installation.json"] = "{}"
    elif damage in {"extra_pod", "continuation", "namespace_drift", "final_drift"}:
        setattr(state, damage, True)
    elif damage == "owner":
        state.pod["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif damage in {"image", "mount", "privileged"}:
        container = state.pod["spec"]["containers"][0]
        if damage == "image":
            container["image"] = "foreign:latest"
        elif damage == "mount":
            state.pod["spec"]["volumes"].append({"name": "foreign", "hostPath": {"path": "/"}})
        else:
            container["securityContext"]["privileged"] = True
    elif damage in {"restart", "nonzero"}:
        container = state.pod["status"]["containerStatuses"][0]
        if damage == "restart":
            container["restartCount"] = 1
        else:
            container["state"]["terminated"]["exitCode"] = 1
    elif damage in {"report", "checksum", "operation", "pool", "mode", "counts", "bool_count"}:
        key, value = {"report": ("private_plan", "private-marker"), "checksum": ("installation_sha256", "sha256:" + "0" * 64),
            "operation": ("operation_id", str(uuid4())), "pool": ("pool_id", str(uuid4())), "mode": ("mode", "global"),
            "counts": ("participants", 2), "bool_count": ("participants", True)}[damage]
        state.report[key] = value
    elif damage == "log_secret":
        state.log = b"private-marker"
    elif damage == "log_oversize":
        state.log = b" " * 16385
    elif damage == "trailer":
        state.log = json.dumps(state.report).encode() + b"\nprivate-marker"
    elif damage == "lost_state":
        (state.state_dir / "stage.json").unlink()
    with api:
        if damage == "pending":
            assert api.registration_report(state.state_dir) is None
        else:
            with pytest.raises(ManagementStageError) as error:
                api.registration_report(state.state_dir)
            assert "private-marker" not in str(error.value)
