"""Live adapter composes fixed stages and actual evidence, not readiness flags."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
import tomllib
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_evidence import evidence as evidence
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_install import run
from tests.ops.test_nebius_management_proofs import Objects
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


class Checks:
    def __init__(self):
        self.calls = []

    def preflight(self, request, rendered):
        self.calls.append("preflight")

    def public_route(self, request):
        self.calls.append("route")


def make_live(request, checks):
    from scripts.ops.nebius_management_live import HTTPSManagementInstallationAPI

    return HTTPSManagementInstallationAPI(request=request, api_server="https://cluster.example.com",
        ssl_context=ssl.create_default_context(), runtime_ca_pem=None,
        token="operator-token", checks=checks)


def test_live_adapter_only_selects_fixed_phase_resource_transports(installation):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    api = make_live(request, Checks())
    expected = {"config": {"ConfigMap", "NetworkPolicy", "ServiceAccount"}, "authority": {
        "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "ClusterRole", "ClusterRoleBinding"},
        "supplied": {"Secret"}, "database": {"Service", "StatefulSet"}, "migration": {"Job"},
        "backup": {"Job"}, "schedule": {"CronJob"}, "service": {"Service", "Deployment"}, "public": {"Ingress"}}
    for phase, kinds in expected.items():
        with api.resources(binding, phase) as transport:
            assert {doc["kind"] for doc in transport.documents.values()} == kinds
    with pytest.raises(ManagementInstallError):
        api.resources(binding, "arbitrary.yaml")
    wrong = copy.copy(binding)
    object.__setattr__(wrong, "installation_id", str(uuid4()))
    with pytest.raises(ManagementInstallError):
        api.resources(wrong, "supplied")


def test_live_preflight_checks_actual_cluster_before_other_prerequisites(installation, monkeypatch):
    from scripts.ops.nebius_management_bootstrap import HTTPSBootstrapAPI
    from scripts.ops.nebius_management_install import ManagementInstallError, render_installation

    request, _ = installation
    checks = Checks()
    api = make_live(request, checks)
    real = HTTPSBootstrapAPI.__init__

    def setup(self, **kwargs):
        real(self, **kwargs)
        self.client.close()
        self.client = httpx.Client(base_url=self.api_server, transport=httpx.MockTransport(lambda req:
            httpx.Response(200, json={"kind": "Namespace", "metadata": {"name": "kube-system", "uid": str(uuid4())}})))

    monkeypatch.setattr(HTTPSBootstrapAPI, "__init__", setup)
    with pytest.raises(ManagementInstallError):
        api.preflight(request, render_installation(request))
    assert checks.calls == []
    assert api.diagnostic_stage == "cluster_identity"


def test_successful_live_preflight_clears_prior_failure_stage(installation, monkeypatch):
    from scripts.ops.nebius_management_bootstrap import HTTPSBootstrapAPI
    from scripts.ops.nebius_management_install import ManagementInstallError, render_installation

    request, _ = installation
    api = make_live(request, Checks())
    status = 403
    real = HTTPSBootstrapAPI.__init__
    def setup(self, **kwargs):
        real(self, **kwargs)
        self.client.close()
        self.client = httpx.Client(base_url=self.api_server, transport=httpx.MockTransport(lambda req:
            httpx.Response(status, json={"kind": "Namespace", "metadata": {
                "name": "kube-system", "uid": request.binding.kube_system_uid}})))
    monkeypatch.setattr(HTTPSBootstrapAPI, "__init__", setup)
    with pytest.raises(ManagementInstallError):
        api.preflight(request, render_installation(request))
    assert api.diagnostic_stage == "cluster_identity"
    status = 200
    api.preflight(request, render_installation(request))
    assert api.diagnostic_stage is None


def test_missing_authority_journal_cannot_issue_token_or_restage_policy(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    api = make_live(request, Checks())
    with pytest.raises(ManagementInstallError):
        api.qualify_authority(binding, tmp_path / "missing")
    assert not (tmp_path / "missing").exists()


def test_live_backup_reads_exact_job_output_and_explicit_backup_identity(installation, evidence, monkeypatch):
    import boto3
    from botocore.hooks import HierarchicalEmitter
    from scripts.ops import nebius_management_live as live

    request, _ = installation
    transport, values, report = evidence
    payload = b"PGDMP" + b"a-test-dump"
    checksum = hashlib.sha256(payload).hexdigest()
    report.update(sha256=checksum, bytes=len(payload))
    report["backup_key"] = report["backup_key"].replace("a" * 12, checksum[:12])
    objects = Objects(payload, checksum)
    objects.close = lambda: None
    objects.meta = SimpleNamespace(events=HierarchicalEmitter())
    clients = []

    def s3(name, **kwargs):
        clients.append((name, kwargs))
        return objects

    monkeypatch.setattr(boto3, "client", s3)
    monkeypatch.setattr(live, "HTTPSManagementEvidenceAPI", lambda **kwargs: transport)
    api = make_live(request, Checks())
    proof = api.verify_backup(transport.binding, api.rendered, values["job"]["metadata"]["uid"])
    assert proof["sha256"] == checksum and proof["bytes"] == len(payload)
    assert clients[0][0] == "s3"
    options = clients[0][1]
    assert options["aws_access_key_id"] == request.material["loom-platform-storage"]["backup-access-key"]
    assert options["aws_secret_access_key"] == request.material["loom-platform-storage"]["backup-secret-key"]
    assert options["config"].retries == {"total_max_attempts": 1, "mode": "standard"}
    assert options["config"].proxies == {}


def test_public_verification_requires_recorded_route_before_opening_admin_material(installation, tmp_path, monkeypatch):
    from scripts.ops import nebius_management_live as live
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    checks = Checks()
    api = make_live(request, checks)
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    calls = []
    monkeypatch.setattr(live, "ManagementPublicProbe", lambda **kwargs: calls.append("unsafe"), raising=False)
    with pytest.raises(ManagementInstallError):
        api.verify_public(binding, api.rendered, tmp_path / "missing" / "material")
    assert calls == [] and checks.calls == []


def test_live_public_auth_uses_only_retained_manager_admin_after_route_checks(installation, tmp_path, monkeypatch):
    from scripts.ops import nebius_management_live as live
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    request, external = installation
    for kind in ("StatefulSet", "Job", "Job", "Deployment"):
        run(installation, tmp_path)
        external.complete(kind)
    result = run(installation, tmp_path)
    material_dir = tmp_path / "installation" / "bootstrap" / "material"
    retained = json.loads((material_dir / "material.json").read_text())
    expected = tomllib.loads(retained["material"]["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
    received = []
    checks = Checks()

    def public_probe(**kwargs):
        assert checks.calls[-1] == "route"
        probe = ManagementPublicProbe(**kwargs)
        probe.client.close()

        def handle(req):
            auth = req.headers.get("authorization")
            received.append(auth)
            if req.url.path.endswith("/health/ready"):
                return httpx.Response(200, json={"status": "ready", "mode": "management", "postgres": "ready", "provisioner": "ready"})
            if req.url.path == "/api/v1/tasks":
                return httpx.Response(404)
            return httpx.Response(200, json={"items": []}) if auth == "Bearer " + expected else httpx.Response(401)

        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        return probe

    monkeypatch.setattr(live, "ManagementPublicProbe", public_probe, raising=False)
    api = make_live(request, checks)
    monkeypatch.setattr(api, "resources", external.resources)
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                result["namespace_uid"], request.binding.kube_system_uid)
    api.verify_public(binding, api.rendered, material_dir)
    assert received.count("Bearer " + expected) == 1
    assert "Bearer operator-token" not in received
