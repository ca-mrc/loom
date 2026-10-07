"""Connected fresh-manager transports select only dev-scoped application stages."""
from __future__ import annotations

import json
import ssl
import tomllib
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_install import run, to_admission
from tests.ops.test_nebius_management_live import Checks
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import application_management_inputs as application_management_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def make_live(request, checks):
    from scripts.ops.nebius_development_management_live import HTTPSDevelopmentManagementAPI

    return HTTPSDevelopmentManagementAPI(request=request,
        api_server=request.deployment.installation.foundation.platform_config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), runtime_ca_pem=None, token='operator-token', checks=checks)


def bound(request):
    from scripts.ops.nebius_management_material import ManagementBinding

    return ManagementBinding(request.binding.installation_id, request.binding.namespace,
        str(uuid4()), request.binding.kube_system_uid)


def test_connected_resources_exclude_legacy_cloud_and_authority(installation):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, _ = installation
    api, binding = make_live(request, Checks()), bound(request)
    with api.resources(binding, 'supplied') as transport:
        assert {doc['metadata']['name'] for doc in transport.documents.values()} == {
            'loom-management-publications', 'loom-platform-storage'}
    with api.resources(binding, 'config') as transport:
        assert len(transport.documents) == 5
        assert {doc['metadata']['namespace'] for doc in transport.documents.values()} == {'loom-nebius-management-dev'}
    for phase in ('authority', 'arbitrary.yaml', 'application-retirement'):
        with pytest.raises(ManagementInstallError):
            api.resources(binding, phase)
    with pytest.raises(ManagementInstallError):
        api.resources(replace(binding, namespace='loom-nebius-management'), 'database')


def test_application_transports_bind_exact_both_namespace_identities(installation):
    from scripts.ops.nebius_development_management_install import _setup
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, _ = installation
    api, binding = make_live(request, Checks()), bound(request)
    setup = _setup(request, binding)
    for phase in ('config', 'admission', 'permissions', 'network', 'material', 'database'):
        with api.application_resources(setup, phase) as transport:
            assert transport.shared_namespace == 'loom-dev'
            assert transport.shared_namespace_uid == request.shared_namespace_uid
            assert {doc['metadata'].get('namespace') for doc in transport.documents.values()} <= {
                None, 'loom-dev', 'loom-nebius-management-dev'}
    for change in (replace(setup, shared_namespace_uid=str(uuid4())),
                   replace(setup, material=replace(setup.material, manager_password='n' * 48))):
        with pytest.raises(ManagementInstallError):
            api.application_resources(change, 'material')
    for phase in ('retirement', 'migration'):
        with pytest.raises(ManagementInstallError):
            api.application_resources(setup, phase)


def test_missing_setup_history_cannot_issue_runtime_token(installation, tmp_path, monkeypatch):
    from scripts.ops.nebius_application_setup import HTTPSApplicationSetupAPI
    from scripts.ops.nebius_development_management_install import _setup
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, _ = installation
    api, binding = make_live(request, Checks()), bound(request)
    calls = []
    monkeypatch.setattr(HTTPSApplicationSetupAPI, '_request', lambda *args, **kwargs: calls.append(args))
    with pytest.raises(ManagementInstallError):
        api.qualify_application(_setup(request, binding), tmp_path / 'missing')
    assert not calls
    assert not (tmp_path / 'missing').exists()


@pytest.mark.parametrize('worker', ['application_provisioner', 'provisioner'])
def test_public_readiness_requires_application_worker_and_own_retained_admin(installation, tmp_path, monkeypatch, worker):
    from scripts.ops import nebius_management_live as live
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    request, external = installation
    to_admission(installation, tmp_path)
    external.admit()
    for kind in ('Job', 'Job', 'Deployment'):
        run(installation, tmp_path)
        external.complete(kind)
    result = run(installation, tmp_path)
    material_dir = tmp_path / 'state/bootstrap/material'
    retained = json.loads((material_dir / 'material.json').read_text())
    expected = tomllib.loads(retained['material']['loom-admin-secret']['secrets.toml'])['admin']['token']
    checks, received = Checks(), []

    def public_probe(**kwargs):
        assert checks.calls[-1] == 'route'
        probe = ManagementPublicProbe(**kwargs)
        probe.client.close()

        def handle(req):
            auth = req.headers.get('authorization')
            received.append(auth)
            if req.url.path.endswith('/health/ready'):
                return httpx.Response(200, json={'status': 'ready', 'mode': 'management', 'postgres': 'ready', worker: 'ready'})
            if req.url.path == '/api/v1/tasks':
                return httpx.Response(404)
            return httpx.Response(200, json={'items': []}) if auth == 'Bearer ' + expected else httpx.Response(401)

        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        return probe

    monkeypatch.setattr(live, 'ManagementPublicProbe', public_probe)
    api = make_live(request, checks)
    monkeypatch.setattr(api, 'resources', external.resources)
    binding = replace(bound(request), namespace_uid=result['namespace_uid'])
    if worker == 'provisioner':
        with pytest.raises(ManagementInstallError):
            api.verify_public(binding, api.rendered, material_dir)
    else:
        api.verify_public(binding, api.rendered, material_dir)
        assert received.count('Bearer ' + expected) == 1
    assert 'Bearer operator-token' not in received
