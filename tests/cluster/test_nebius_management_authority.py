"""Real API authorization/admission fences the manager away from foreign data."""
from __future__ import annotations

import base64
import copy
import os
import ssl
import time
from dataclasses import replace

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_management_bootstrap_and_owned_permissions_are_enforced_by_actual_api(tmp_path, installation):
    from kubernetes import client
    from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
    from scripts.ops.nebius_management_authority_stage import (
        HTTPSManagementAuthorityAPI,
        management_authority_ready,
        stage_management_authority,
    )
    from scripts.ops.nebius_management_bootstrap import (
        BootstrapBinding,
        HTTPSBootstrapAPI,
        bootstrap_management,
    )
    from scripts.ops.nebius_management_evidence import (
        HTTPSManagementEvidenceAPI,
        _matches_backup_template,
    )
    from scripts.ops.nebius_management_install import ManagementInstallError, render_installation
    from scripts.ops.nebius_management_live import HTTPSManagementInstallationAPI
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import stage_management_resources

    from loom.nebius_management_authority import (
        ManagementNamespaceAuthority,
        namespace_binding,
        render_namespace_authority,
    )

    authority = ManagementNamespaceAuthority(
        installation_id="30000000-0000-4000-8000-000000000001", namespace="loom-nebius-management",
    )
    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        operator_trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        user = config["users"][0]["user"]
        certificate, key = tmp_path / "client.crt", tmp_path / "client.key"
        certificate.write_bytes(base64.b64decode(user["client-certificate-data"]))
        key.write_bytes(base64.b64decode(user["client-key-data"]))
        key.chmod(0o600)
        operator_trust.load_cert_chain(certificate, key)
        bootstrap = BootstrapBinding(str(authority.installation_id), authority.namespace,
                                     core.read_namespace("kube-system").metadata.uid)
        with HTTPSBootstrapAPI(binding=bootstrap, api_server=endpoint, ssl_context=operator_trust) as api:
            receipt = bootstrap_management(binding=bootstrap, api=api, state_dir=tmp_path / "bootstrap")
        binding = ManagementBinding(bootstrap.installation_id, bootstrap.namespace,
                                    receipt["namespace_uid"], bootstrap.kube_system_uid)
        core.create_namespace({"metadata": {"name": "loom-dev-foreign"}})
        core.create_namespaced_secret("loom-dev-foreign", {"metadata": {"name": "private"}, "stringData": {"value": "foreign"}})
        class ExternalPrerequisites:
            def preflight(self, request, rendered):
                raise AssertionError("cloud/route qualification is outside this disposable RBAC test")

            def public_route(self, request):
                raise AssertionError("no public route in disposable RBAC test")

        request = replace(installation[0], binding=bootstrap)
        live = HTTPSManagementInstallationAPI(request=request, api_server=endpoint, ssl_context=operator_trust,
            runtime_ca_pem=base64.b64decode(config["clusters"][0]["cluster"]["certificate-authority-data"]).decode(),
            checks=ExternalPrerequisites())
        with live.resources(binding, "config") as api:
            stage_management_resources(rendered=live.rendered, phase="10-config-network.yaml", binding=binding,
                                       api=api, state_dir=tmp_path / "config")
        # Real Pod-only admission defaults differ from Job-template defaults.
        # Dry-runs prove compatibility without claiming a database backup ran.
        batch = client.BatchV1Api(core.api_client)
        job = core.api_client.sanitize_for_serialization(batch.create_namespaced_job(
            binding.namespace, live.rendered.files["85-backup-verify.yaml"][0], dry_run="All"))
        template = job["spec"]["template"]["spec"]
        admitted = core.api_client.sanitize_for_serialization(core.create_namespaced_pod(binding.namespace, {
            "apiVersion": "v1", "kind": "Pod", "metadata": {"generateName": "backup-evidence-"}, "spec": template,
        }, dry_run="All"))
        assert len(admitted["spec"]["tolerations"]) > len(template["tolerations"])
        assert _matches_backup_template(admitted["spec"], template)
        # The evidence reader uses raw HTTPS, not SDK objects that may fill
        # TypeMeta. Characterize the actual list-versus-GET wire contract.
        # A nonexistent node prevents image pulls or workload execution.
        wire_pod = core.create_namespaced_pod(binding.namespace, {
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "backup-wire-shape"},
            "spec": {**template, "nodeName": "loom-wire-format-only"},
        })
        with httpx.Client(base_url=endpoint, verify=operator_trust, trust_env=False, timeout=20) as http:
            collection = http.get("/api/v1/namespaces/" + binding.namespace + "/pods",
                                  params={"fieldSelector": "metadata.name=backup-wire-shape"}).json()
            individual = http.get("/api/v1/namespaces/" + binding.namespace + "/pods/backup-wire-shape").json()
        assert (collection["apiVersion"], collection["kind"]) == ("v1", "PodList")
        assert len(collection["items"]) == 1
        listed = collection["items"][0]
        assert "apiVersion" not in listed and "kind" not in listed
        assert (individual["apiVersion"], individual["kind"]) == ("v1", "Pod")
        assert listed["metadata"]["uid"] == individual["metadata"]["uid"] == wire_pod.metadata.uid
        assert listed["spec"] == individual["spec"]
        with HTTPSManagementAuthorityAPI(authority=authority, binding=binding,
                                        api_server=endpoint, ssl_context=operator_trust) as api:
            arguments = dict(authority=authority, binding=binding, api=api, state_dir=tmp_path / "authority")
            first = stage_management_authority(**arguments)
            assert stage_management_authority(**arguments) == first
            assert len(first["resource_uids"]) == 9
        policies = [doc["metadata"]["name"] for doc in render_namespace_authority(authority)
                    if doc["kind"] == "ValidatingAdmissionPolicy"]
        deadline = time.monotonic() + 20
        while True:
            observed = [admission.read_validating_admission_policy(name) for name in policies]
            if all(item.status and item.status.type_checking for item in observed):
                assert all(not item.status.type_checking.expression_warnings for item in observed)
                break
            assert time.monotonic() < deadline, "admission policy was not type checked"
            time.sleep(0.1)
        with HTTPSManagementAuthorityAPI(authority=authority, binding=binding,
                                        api_server=endpoint, ssl_context=operator_trust) as api:
            assert management_authority_ready(authority=authority, binding=binding, api=api, state_dir=tmp_path / "authority")
        account_uid = core.read_namespaced_service_account("loom-management-provisioner", authority.namespace).metadata.uid
        with HTTPSManagementEvidenceAPI(binding=binding, rendered=render_installation(installation[0]),
                                        api_server=endpoint, ssl_context=operator_trust) as api:
            token = api.runtime_token(service_account_uid=account_uid)
        with HTTPSManagementAuthorityProbe(authority=authority, service_account_uid=account_uid,
                                          api_server=endpoint, ssl_context=trust, token=token) as probe:
            deadline = time.monotonic() + 20
            while not probe.qualify():
                assert time.monotonic() < deadline, "runtime authority did not qualify"
                time.sleep(0.1)
        # Exercise the connected caller, including the journal-bound account and
        # a fresh trust-only context. Reusing operator mTLS would fail its actual
        # SelfSubjectReview instead of silently qualifying cluster-admin.
        deadline = time.monotonic() + 20
        while True:
            try:
                live.qualify_authority(binding, tmp_path / "authority")
                break
            except ManagementInstallError as exc:
                assert str(exc) == "management authority propagation pending"
                assert time.monotonic() < deadline
                time.sleep(0.1)
        with httpx.Client(base_url=endpoint, verify=trust, headers={"Authorization": "Bearer " + token},
                          trust_env=False, follow_redirects=False, timeout=20) as http:
            own = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice", "labels": {
                "loom.nebius/namespace-installation": str(authority.installation_id),
                "loom.nebius/environment-id": "10000000-0000-4000-8000-000000000001",
                "loom.nebius/incarnation": "20000000-0000-4000-8000-000000000001",
                "pod-security.kubernetes.io/enforce": "restricted",
            }}}
            # Negative dry-run establishes policy-cache readiness without ever
            # creating an invalid namespace during admission propagation.
            invalid = copy.deepcopy(own)
            invalid["metadata"]["name"] = "foreign-created"
            deadline = time.monotonic() + 20
            while True:
                result = http.post("/api/v1/namespaces?dryRun=All", json=invalid)
                if result.status_code == 403 and "management namespace boundary" in result.text:
                    break
                assert result.status_code in (201, 403), result.text
                assert time.monotonic() < deadline, "namespace admission did not become effective"
                time.sleep(0.1)
            for key, value in (("loom.nebius/namespace-installation", "foreign"),
                               ("pod-security.kubernetes.io/enforce", "privileged"),
                               ("loom.nebius/environment-id", "invalid"),
                               ("loom.nebius/incarnation", "00000000-0000-0000-0000-000000000000")):
                invalid = copy.deepcopy(own)
                invalid["metadata"]["labels"][key] = value
                assert http.post("/api/v1/namespaces", json=invalid).status_code == 403
            invalid = copy.deepcopy(own)
            del invalid["metadata"]["labels"]
            assert http.post("/api/v1/namespaces", json=invalid).status_code == 403
            assert http.post("/api/v1/namespaces", json=own).status_code == 201
            bindings = "/apis/rbac.authorization.k8s.io/v1/namespaces/loom-dev-alice/rolebindings"
            binding = namespace_binding(authority, "loom-dev-alice")
            foreign = namespace_binding(authority, "loom-dev-foreign")
            assert http.post(bindings.replace("loom-dev-alice", "loom-dev-foreign"), json=foreign).status_code == 403
            for field in ("subject", "role"):
                invalid = copy.deepcopy(binding)
                if field == "subject":
                    invalid["subjects"][0]["namespace"] = "loom-dev-alice"
                else:
                    invalid["roleRef"]["name"] = "cluster-admin"
                denied = http.post(bindings, json=invalid)
                assert denied.status_code == 403
                if field == "subject":
                    assert "management namespace binding boundary" in denied.text
            assert http.post(bindings, json=binding).status_code == 201
            for name in ("loom-run-20000000000040008000000000000001", "loom-run-20000000000040008000000000000001-build"):
                run = copy.deepcopy(own)
                run["metadata"]["name"] = name
                assert http.post("/api/v1/namespaces", json=run).status_code == 201
                assert http.post(bindings.replace("loom-dev-alice", name), json=namespace_binding(authority, name)).status_code == 201
            secrets = "/api/v1/namespaces/loom-dev-alice/secrets"
            assert http.post(secrets, json={"metadata": {"name": "own"}, "stringData": {"value": "own"}}).status_code == 201
            assert http.get(secrets + "/own").status_code == 200
            assert http.get("/api/v1/namespaces/loom-dev-foreign/secrets/private").status_code == 403
            assert http.get("/api/v1/secrets").status_code == 403
            assert http.patch("/api/v1/namespaces/loom-dev-foreign", json={"metadata": {"labels": own["metadata"]["labels"]}},
                              headers={"Content-Type": "application/merge-patch+json"}).status_code == 403
            assert http.delete("/api/v1/namespaces/loom-dev-alice/persistentvolumeclaims/data").status_code == 403
            roles = bindings.replace("rolebindings", "roles")
            role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                "metadata": {"name": "loom-execution-observer", "namespace": "loom-dev-alice"},
                "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch"]},
                          {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]}]}
            excessive = copy.deepcopy(role)
            excessive["rules"].append({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]})
            denied = http.post(roles, json=excessive)
            assert denied.status_code == 403
            assert "management observer role boundary" in denied.text
            assert http.post(roles, json=role).status_code == 201
            observer = copy.deepcopy(binding)
            observer["metadata"]["name"] = "loom-execution-observer"
            observer["roleRef"].update(kind="Role", name="loom-execution-observer")
            observer["subjects"] = [{"kind": "ServiceAccount", "namespace": "loom-dev-alice", "name": "loom-execution-actuator"}]
            assert http.post(bindings, json=observer).status_code == 201
            core.create_namespaced_service_account("loom-dev-alice", {"metadata": {"name": "loom-execution-actuator"}})
            child_token = core.create_namespaced_service_account_token("loom-execution-actuator", "loom-dev-alice",
                client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[], expiration_seconds=600))).status.token
            child_headers = {"Authorization": "Bearer " + child_token}
            assert http.get(secrets + "/own", headers=child_headers).status_code == 403
            assert http.get("/apis/batch/v1/namespaces/loom-dev-alice/jobs", headers=child_headers).status_code == 200
            role["metadata"]["name"] = "escalation"
            role["rules"] = [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}]
            assert http.post(roles, json=role).status_code == 403
            # Retained cleanup patches routing rather than deleting the Ingress.
            ingress_path = "/apis/networking.k8s.io/v1/namespaces/loom-dev-alice/ingresses"
            ingress = {"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
                "metadata": {"name": "route", "namespace": "loom-dev-alice"},
                "spec": {"defaultBackend": {"service": {"name": "service", "port": {"number": 8090}}}}}
            assert http.post(ingress_path, json=ingress).status_code == 201
            assert http.patch(ingress_path + "/route", json=[{
                "op": "replace", "path": "/spec/defaultBackend/service/name", "value": "loom-retained-alice",
            }], headers={"Content-Type": "application/json-patch+json"}).status_code == 200
            assert http.delete(ingress_path + "/route").status_code == 403
    finally:
        container.stop()
