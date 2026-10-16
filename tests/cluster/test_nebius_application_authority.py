"""Actual Kubernetes admission/RBAC for application-only lifecycle authority."""
from __future__ import annotations

import base64
import copy
import os
import ssl
import time
from uuid import uuid4

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_authority import authority_for
from tests.unit.test_nebius_application_render import inputs, named

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


def _can_create_application_secret(http):
    response = http.post("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", json={
        "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
        "spec": {"resourceAttributes": {"namespace": "loom-dev-alice", "group": "", "resource": "secrets", "verb": "create"}},
    })
    assert response.status_code == 201, response.status_code
    status = response.json()["status"]
    assert not status.get("evaluationError"), "application access review failed"
    assert type(status.get("allowed")) is bool
    return status["allowed"]


def _exercise_pod_fence(http, core, authority, registration, rendered):
    from kubernetes.client.exceptions import ApiException

    from loom.nebius_application_authority import application_pod_fence

    fence = application_pod_fence(authority, registration, operation_id=uuid4())
    ns = registration.application_namespace
    path = "/api/v1/namespaces/" + ns + "/resourcequotas"
    invalid = copy.deepcopy(fence)
    invalid["metadata"]["name"] = "arbitrary-quota"
    deadline = time.monotonic() + 20
    while True:
        observed = http.post(path + "?dryRun=All", json=invalid)
        if observed.status_code == 403 and "application pod-fence boundary" in observed.text:
            break
        assert observed.status_code in (201, 403), observed.text
        assert time.monotonic() < deadline, "Pod-fence admission not effective"
        time.sleep(0.1)
    for change in ("name", "pods", "scopes", "selector", "extra-resource", "owner", "generation", "nil-operation"):
        invalid = copy.deepcopy(fence)
        if change == "name":
            invalid["metadata"]["name"] = "arbitrary-quota"
        elif change == "pods":
            invalid["spec"]["hard"]["pods"] = "1"
        elif change == "scopes":
            invalid["spec"]["scopes"] = ["BestEffort"]
        elif change == "selector":
            invalid["spec"]["scopeSelector"] = {"matchExpressions": [{"scopeName": "BestEffort", "operator": "Exists"}]}
        elif change == "extra-resource":
            invalid["spec"]["hard"]["requests.cpu"] = "1"
        elif change == "owner":
            invalid["metadata"]["labels"]["loom.nebius/application-id"] = str(uuid4())
        elif change == "generation":
            invalid["metadata"]["annotations"]["loom.nebius/deployment-generation"] = "0"
        else:
            invalid["metadata"]["annotations"]["loom.nebius/operation-id"] = "00000000-0000-0000-0000-000000000000"
        denied = http.post(path + "?dryRun=All", json=invalid)
        assert denied.status_code == 403 and "application pod-fence boundary" in denied.text, denied.text
    assert http.post("/api/v1/namespaces/loom-dev-foreign/resourcequotas", json=fence).status_code == 403
    created = http.post(path, json=fence)
    assert created.status_code == 201, created.text
    resource = path + "/loom-application-retired"
    assert http.get(path + "/arbitrary-quota").status_code == 403
    assert http.delete(path + "/arbitrary-quota").status_code == 403
    # Scope changes are already immutable at the API layer; CREATE above proves
    # our policy denies scoped quotas. Exercise mutable UPDATE fields here.
    for patch in ({"spec": {"hard": {"pods": "1"}}},
                  {"metadata": {"labels": {"loom.nebius/incarnation": str(uuid4())}}}):
        denied = http.patch(resource, json=patch, headers={"Content-Type": "application/merge-patch+json"})
        assert denied.status_code == 403 and "application pod-fence boundary" in denied.text, denied.text
    deadline = time.monotonic() + 20
    while True:
        observed = http.get(resource)
        assert observed.status_code == 200, observed.text
        if observed.json().get("status", {}).get("hard", {}).get("pods") == "0":
            break
        assert time.monotonic() < deadline, "zero-Pod quota not reconciled"
        time.sleep(0.1)
    # Same generation cannot be replaced by another operation; newer may supersede.
    changed = http.patch(resource, json={"metadata": {"annotations": {"loom.nebius/operation-id": str(uuid4())}}},
                         headers={"Content-Type": "application/merge-patch+json"})
    assert changed.status_code == 403, changed.text
    annotations = {"loom.nebius/deployment-generation": "2", "loom.nebius/operation-id": str(uuid4())}
    changed = http.patch(resource, json={"metadata": {"annotations": annotations}},
                         headers={"Content-Type": "application/merge-patch+json"})
    assert changed.status_code == 200, changed.text
    replay = http.patch(resource, json={"metadata": {"annotations": annotations}},
                        headers={"Content-Type": "application/merge-patch+json"})
    assert replay.status_code == 200, replay.text
    denied = http.patch(resource, json={"metadata": {"annotations": fence["metadata"]["annotations"]}},
                        headers={"Content-Type": "application/merge-patch+json"})
    assert denied.status_code == 403, denied.text
    # Use operator authority to isolate ResourceQuota from manager RBAC denial.
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "fence-probe"},
           "spec": copy.deepcopy(named(rendered, "Deployment", "loom-service")["spec"]["template"]["spec"])}
    deadline = time.monotonic() + 20
    while True:
        try:
            core.create_namespaced_pod(ns, pod, dry_run="All")
        except ApiException as rejected:
            assert rejected.status == 403 and "exceeded quota: loom-application-retired" in rejected.body, rejected.body
            break
        assert time.monotonic() < deadline, "zero-Pod quota not enforced"
        time.sleep(0.1)
    # Neither a replaced UID nor the earlier generation's version can reopen it.
    for stale in ({"uid": str(uuid4())}, {
        "uid": created.json()["metadata"]["uid"], "resourceVersion": created.json()["metadata"]["resourceVersion"],
    }):
        wrong = http.request("DELETE", resource, json={"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": stale})
        assert wrong.status_code == 409, wrong.text
    metadata = http.get(resource).json()["metadata"]
    removed = http.request("DELETE", resource, json={"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {
        "uid": metadata["uid"], "resourceVersion": metadata["resourceVersion"],
    }})
    assert removed.status_code == 200, removed.text
    # Negative cache observation is read-only/dry-run, never a real Pod write.
    deadline = time.monotonic() + 20
    while True:
        try:
            core.create_namespaced_pod(ns, pod, dry_run="All")
            break
        except ApiException as exc:
            assert exc.status == 403 and "exceeded quota: loom-application-retired" in exc.body, exc.body
            assert time.monotonic() < deadline, "removed quota still enforced"
            time.sleep(0.1)


@pytest.mark.timeout(180)
def test_application_manager_can_manage_apps_but_not_shared_or_legacy_resources(platform_inputs):
    from kubernetes import client, utils

    from loom.nebius_application_authority import (
        application_namespace_binding,
        render_application_authority,
    )
    from loom.nebius_application_render import render_application
    from loom.nebius_management_authority import (
        ManagementNamespaceAuthority,
        render_namespace_authority,
    )

    values = inputs(platform_inputs)
    authority = authority_for(values[2])
    rendered = render_application(*values, authority=authority)
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        core.create_namespace({"metadata": {"name": authority.namespace}})
        core.create_namespaced_service_account(authority.namespace, {"metadata": {"name": "loom-application-provisioner"}})
        legacy = ManagementNamespaceAuthority(installation_id=authority.installation_id, namespace=authority.namespace)
        # Coexisting legacy policies must not be broadened or bypassed by app authority.
        for doc in render_namespace_authority(legacy) + render_application_authority(authority):
            utils.create_from_dict(core.api_client, doc)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        policies = [doc["metadata"]["name"] for doc in render_application_authority(authority)
                    if doc["kind"] == "ValidatingAdmissionPolicy"]
        deadline = time.monotonic() + 20
        while True:
            observed = [admission.read_validating_admission_policy(name) for name in policies]
            if all(item.status and item.status.type_checking for item in observed):
                warnings = {item.metadata.name: [line for warning in item.status.type_checking.expression_warnings
                                                 for line in warning.warning.splitlines() if line.startswith("ERROR:")]
                            for item in observed
                            if item.status.type_checking.expression_warnings}
                assert not warnings, repr(warnings)
                break
            assert time.monotonic() < deadline, "application policies were not type-checked"
            time.sleep(0.1)
        token = core.create_namespaced_service_account_token("loom-application-provisioner", authority.namespace,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[], expiration_seconds=600))).status.token
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        own = named(rendered, "Namespace", "loom-dev-alice")
        core.create_namespace({"metadata": {"name": "loom-dev-foreign"}})
        core.create_namespaced_secret("loom-dev-foreign", {"metadata": {"name": "private"}, "stringData": {"data": "foreign"}})
        old = copy.deepcopy(own)
        old["metadata"]["name"] = "loom-dev-legacy"
        old["metadata"]["labels"].pop("loom.nebius/application-installation")
        old["metadata"]["labels"]["loom.nebius/namespace-installation"] = str(authority.installation_id)
        old["metadata"]["labels"]["loom.nebius/environment-id"] = "10000000-0000-4000-8000-000000000001"
        core.create_namespace(old)
        with httpx.Client(base_url=endpoint, verify=trust, headers={"Authorization": "Bearer " + token},
                          trust_env=False, follow_redirects=False, timeout=20) as http:
            invalid = copy.deepcopy(own)
            invalid["metadata"]["name"] = "foreign-created"
            deadline = time.monotonic() + 20
            while True:
                result = http.post("/api/v1/namespaces?dryRun=All", json=invalid)
                if result.status_code == 403 and "application namespaces boundary" in result.text:
                    break
                assert result.status_code in (201, 403), result.text
                assert time.monotonic() < deadline, "application namespace admission not effective"
                time.sleep(0.1)
            for name in ("loom-dev", "loom-staging", "loom-prod", "loom-dev-shared", "loom-run-deadbeef", authority.shared_namespace):
                invalid = copy.deepcopy(own)
                invalid["metadata"]["name"] = name
                denied = http.post("/api/v1/namespaces?dryRun=All", json=invalid)
                assert denied.status_code == 403, denied.text
            for key, value in (
                ("loom.nebius/application-installation", "foreign"), ("loom.nebius/data-environment-id", "foreign"),
                ("loom.nebius/application-id", "00000000-0000-0000-0000-000000000000"),
                ("loom.nebius/incarnation", "invalid"), ("pod-security.kubernetes.io/enforce", "privileged"),
                ("loom.nebius/environment-id", "10000000-0000-4000-8000-000000000001"),
                ("loom.nebius/namespace-installation", str(authority.installation_id)),
            ):
                invalid = copy.deepcopy(own)
                invalid["metadata"]["labels"][key] = value
                denied = http.post("/api/v1/namespaces?dryRun=All", json=invalid)
                assert denied.status_code == 403, denied.text
            invalid = copy.deepcopy(own)
            del invalid["metadata"]["labels"]
            assert http.post("/api/v1/namespaces?dryRun=All", json=invalid).status_code == 403
            created = http.post("/api/v1/namespaces", json=own)
            assert created.status_code == 201, created.text
            for namespace in ("loom-dev-foreign", "loom-dev-legacy", authority.namespace):
                denied = http.post(f"/apis/rbac.authorization.k8s.io/v1/namespaces/{namespace}/rolebindings",
                                   json=application_namespace_binding(authority, namespace))
                assert denied.status_code == 403, denied.text
            path = "/apis/rbac.authorization.k8s.io/v1/namespaces/loom-dev-alice/rolebindings"
            role = named(rendered, "RoleBinding", authority.name)
            for change in ("subject", "role", "extra-subject"):
                invalid = copy.deepcopy(role)
                if change == "subject":
                    invalid["subjects"][0]["namespace"] = "loom-dev-alice"
                elif change == "role":
                    invalid["roleRef"]["name"] = legacy.name + "-resources"
                else:
                    invalid["subjects"].append({"kind": "ServiceAccount", "name": "loom-platform", "namespace": "loom-dev-alice"})
                denied = http.post(path, json=invalid)
                assert denied.status_code == 403, denied.text
            # The bootstrap role alone must not authorize app Secret writes.
            assert not _can_create_application_secret(http)
            created = http.post(path, json=role)
            assert created.status_code == 201, created.text
            # RoleBinding persistence is not authorizer-cache propagation.
            # Observe effective permission; never retry a real Secret write.
            deadline = time.monotonic() + 20
            while not _can_create_application_secret(http):
                assert time.monotonic() < deadline, "application resource RBAC did not propagate"
                time.sleep(0.1)
            local = "/api/v1/namespaces/loom-dev-alice"
            secret = {"metadata": {"name": "loom-application-db"}, "stringData": {"url": "test-only"}}
            created = http.post(local + "/secrets", json=secret)
            assert created.status_code == 201, created.json().get("message", "no Kubernetes status message")
            assert http.get(local + "/secrets/loom-application-db").status_code == 200
            assert http.get("/api/v1/secrets").status_code == 403
            assert http.get("/api/v1/namespaces/loom-dev-foreign/secrets/private").status_code == 403
            assert http.patch(local, json={"metadata": {"labels": {"unsafe": "true"}}},
                              headers={"Content-Type": "application/merge-patch+json"}).status_code == 403
            assert http.delete(local).status_code == 403
            for resource, api, body in (
                ("persistentvolumeclaims", "/api/v1", {"metadata": {"name": "forbidden"}}),
                ("jobs", "/apis/batch/v1", {"metadata": {"name": "forbidden"}}),
                ("roles", "/apis/rbac.authorization.k8s.io/v1", {"metadata": {"name": "forbidden"}}),
                ("pods", "/api/v1", {"metadata": {"name": "forbidden"}}),
            ):
                assert http.post(api + "/namespaces/loom-dev-alice/" + resource, json=body).status_code == 403
            service_account = named(rendered, "ServiceAccount", "loom-platform")
            assert http.post(local + "/serviceaccounts", json=service_account).status_code == 201
            assert http.post(local + "/serviceaccounts/loom-platform/token", json={
                "apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest", "spec": {"audiences": []},
            }).status_code == 403
            legacy_token = {"apiVersion": "v1", "kind": "Secret", "metadata": {
                "name": "legacy-token", "annotations": {"kubernetes.io/service-account.name": "loom-platform"},
            }, "type": "kubernetes.io/service-account-token"}
            denied = http.post(local + "/secrets?dryRun=All", json=legacy_token)
            assert denied.status_code == 403, denied.status_code
            # An operator-created old token must not be mutable into another
            # issuance by the manager either. No token is printed or consumed.
            core.create_namespaced_secret("loom-dev-alice", legacy_token)
            denied = http.patch(local + "/secrets/legacy-token?dryRun=All",
                                json={"metadata": {"annotations": {"probe": "changed"}}},
                                headers={"Content-Type": "application/merge-patch+json"})
            assert denied.status_code == 403, denied.status_code
            deployments = "/apis/apps/v1/namespaces/loom-dev-alice/deployments"
            for name in ("loom-service", "loom-web"):
                doc = named(rendered, "Deployment", name)
                accepted = http.post(deployments + "?dryRun=All", json=doc)
                assert accepted.status_code == 201, accepted.text
            # Real lifecycle writes, no application Pods, image pulls or shared data.
            stopped = copy.deepcopy(named(rendered, "Deployment", "loom-service"))
            stopped["spec"]["replicas"] = 0
            assert http.post(deployments, json=stopped).status_code == 201
            changed = http.patch(deployments + "/loom-service", json={"metadata": {"annotations": {"test": "updated"}}},
                                 headers={"Content-Type": "application/merge-patch+json"})
            assert changed.status_code == 200, changed.text
            assert http.delete(deployments + "/loom-service").status_code == 200
            assert http.delete(local + "/secrets/loom-application-db").status_code == 200
            _exercise_pod_fence(http, core, authority, values[0], rendered)
        assert not core.list_namespaced_pod("loom-dev-alice").items
    finally:
        container.stop()
