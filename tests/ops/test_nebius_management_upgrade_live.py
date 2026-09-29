"""Connected upgrade uses retained identities and actual runtime-subject probes."""
from __future__ import annotations

import json
import ssl
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade import run as run_upgrade
from tests.ops.test_nebius_management_upgrade import to_retirement
from tests.ops.test_nebius_management_upgrade import upgrade as upgrade
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class Checks:
    def __init__(self):
        self.calls = []

    def preflight(self, request):
        self.calls.append('prerequisites')

    def public_route(self, request):
        self.calls.append('public_route')


@pytest.fixture
def connected(upgrade, monkeypatch):
    from scripts.ops.nebius_management_material import _documents
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from scripts.ops.nebius_management_upgrade_live import HTTPSManagementUpgradeAPI

    request, stored = upgrade
    binding = request.setup.binding
    bundle = json.loads((request.original_state / 'bootstrap/material/material.json').read_text())
    generated = _documents(bundle['material'], binding, bundle['operation_id'])
    for name, doc in generated.items():
        doc['metadata']['uid'] = bundle['resources'][name]['uid']
    namespaces = {binding.namespace: {'kind': 'Namespace', 'metadata': {'name': binding.namespace,
        'uid': binding.namespace_uid, 'labels': {'loom.nebius/management-installation': binding.installation_id,
            'pod-security.kubernetes.io/enforce': 'restricted'}}},
        'kube-system': {'kind': 'Namespace', 'metadata': {'name': 'kube-system', 'uid': binding.kube_system_uid}},
        request.setup.deployment.installation.applications.shared.platform_namespace: {'kind': 'Namespace', 'metadata': {
            'name': request.setup.deployment.installation.applications.shared.platform_namespace,
            'uid': request.setup.shared_namespace_uid}}}
    state = {'fault': None, 'calls': [], 'namespaces': namespaces, 'generated': generated, 'contexts': [], 'tokens': []}
    kinds = {'configmaps': 'ConfigMap', 'networkpolicies': 'NetworkPolicy', 'serviceaccounts': 'ServiceAccount',
        'services': 'Service', 'deployments': 'Deployment', 'statefulsets': 'StatefulSet', 'jobs': 'Job',
        'cronjobs': 'CronJob', 'secrets': 'Secret', 'ingresses': 'Ingress', 'persistentvolumeclaims': 'PersistentVolumeClaim',
        'persistentvolumes': 'PersistentVolume', 'validatingadmissionpolicies': 'ValidatingAdmissionPolicy',
        'validatingadmissionpolicybindings': 'ValidatingAdmissionPolicyBinding', 'clusterroles': 'ClusterRole',
        'clusterrolebindings': 'ClusterRoleBinding', 'roles': 'Role', 'rolebindings': 'RoleBinding'}

    def handler(message):
        state['calls'].append(message)
        path = message.url.path
        parts = path.split('/')
        if message.method == 'GET':
            if parts[-2] == 'namespaces':
                doc = namespaces.get(parts[-1])
            else:
                kind = kinds[parts[-2]]
                doc = (generated.get(parts[-1]) if kind == 'Secret' and parts[-1] in generated
                    else stored.store.resources.get(kind + ':' + parts[-1]))
                if kind == 'Deployment' and parts[-1] == 'loom-service':
                    doc = stored.switch.document
            return httpx.Response(200, json=doc) if doc else httpx.Response(404)
        assert path == '/api/v1/namespaces/' + binding.namespace + '/serviceaccounts/loom-application-provisioner/token'
        assert json.loads(message.content) == {'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest',
            'spec': {'audiences': [], 'expirationSeconds': 600}}
        if state['fault'] == 'lost_token':
            raise httpx.ReadTimeout('private-token-outcome')
        if state['fault'] == 'account_replaced':
            stored.store.resources['ServiceAccount:loom-application-provisioner']['metadata']['uid'] = str(uuid4())
        return httpx.Response(201, json={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest', 'status': {
            'token': 'application-only-token', 'expirationTimestamp': (datetime.now(UTC) + timedelta(minutes=10)).isoformat()}})

    real = ManagementKubernetesTransport.__init__

    def transport(self, **kwargs):
        real(self, **kwargs)
        state['contexts'].append(kwargs['ssl_context'])
        state['tokens'].append(kwargs.get('token'))
        self.client.close()
        self.client = httpx.Client(base_url=self.api_server, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(ManagementKubernetesTransport, '__init__', transport)
    checks = Checks()
    trust = ssl.create_default_context()
    with HTTPSManagementUpgradeAPI(request=request, api_server=request.setup.deployment.installation.applications.runtime.kubernetes.endpoint,
        ssl_context=trust, token='operator-only-token', runtime_ca_pem=None, checks=checks) as api:
        yield api, state, checks, trust


def test_upgrade_live_preflight_is_read_only_and_preserves_retained_database_and_secrets(upgrade, connected):
    request, _ = upgrade
    api, state, checks, _ = connected
    api.preflight(request)
    assert checks.calls == ['prerequisites']
    assert state['calls'] and all(row.method == 'GET' for row in state['calls'])
    assert any('/persistentvolumeclaims/' in row.url.path for row in state['calls'])
    assert any('/persistentvolumes/' in row.url.path for row in state['calls'])
    assert any(row.url.path.endswith('/secrets/loom-admin-secret') for row in state['calls'])


@pytest.mark.parametrize('drift', ['shared_namespace', 'secret', 'volume', 'database'])
def test_upgrade_live_drift_blocks_before_prerequisites_or_writes(upgrade, connected, drift):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    request, stored = upgrade
    api, state, checks, _ = connected
    if drift == 'shared_namespace':
        state['namespaces'][request.setup.deployment.installation.applications.shared.platform_namespace]['metadata']['uid'] = str(uuid4())
    elif drift == 'secret':
        state['generated']['loom-platform-db']['metadata']['uid'] = str(uuid4())
    elif drift == 'volume':
        volume = next(row for row in stored.store.resources.values() if row['kind'] == 'PersistentVolume')
        volume['spec']['csi']['volumeHandle'] = 'different-disk'
    else:
        stored.store.resources['StatefulSet:loom-postgres']['status']['readyReplicas'] = 0
    with pytest.raises(ManagementUpgradeError):
        api.preflight(request)
    assert not checks.calls and all(row.method == 'GET' for row in state['calls'])


@pytest.mark.parametrize('fault', [None, 'lost_token', 'account_replaced', 'missing_policy'])
def test_upgrade_live_uses_short_lived_new_subject_with_trust_only_tls(upgrade, connected, tmp_path, monkeypatch, fault):
    from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, stored = upgrade
    to_retirement(upgrade, tmp_path)
    api, state, _, operator_trust = connected
    state['fault'] = fault
    if fault == 'missing_policy':
        (tmp_path / 'upgrade/admission/stage.json').unlink()
    observed = []

    def subject(self):
        observed.append(self)
        assert self.service_account_uid == stored.store.resources['ServiceAccount:loom-application-provisioner']['metadata']['uid']
        assert self.authority == request.setup.deployment.installation.applications.authority
        return True

    monkeypatch.setattr(HTTPSManagementAuthorityProbe, 'qualify', subject)
    if fault:
        with pytest.raises(ManagementStageError):
            api.qualify_authority(request.setup, tmp_path / 'upgrade')
        assert not observed
    else:
        assert api.qualify_authority(request.setup, tmp_path / 'upgrade') is True
        assert len(observed) == 1
        assert state['contexts'][-1] is not operator_trust
        assert state['tokens'][-1] == 'application-only-token'
    writes = [row for row in state['calls'] if row.method != 'GET']
    assert len(writes) == (0 if fault == 'missing_policy' else 1)
    assert all(row.url.path.endswith('/serviceaccounts/loom-application-provisioner/token') for row in writes)


def test_upgrade_live_rejects_foreign_stage_request_before_connecting(upgrade, connected):
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, _ = upgrade
    api, state, _, _ = connected
    wrong = replace(request.setup, shared_namespace_uid=str(uuid4()))
    for selected, phase in ((wrong, 'config'), (request.setup, 'arbitrary')):
        with pytest.raises(ManagementStageError):
            api.resources(selected, phase)
    assert not state['calls']


@pytest.mark.parametrize('fault', [None, 'stale_generation', 'wrong_image', 'public_auth'])
def test_upgrade_live_requires_exact_new_deployment_and_application_public_auth(upgrade, connected, tmp_path, monkeypatch, fault):
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    request, stored = upgrade
    to_retirement(upgrade, tmp_path)
    stored.switch.processes = False
    assert run_upgrade(upgrade, tmp_path)['phase'] == 'migration'
    stored.complete('loom-management-migrate-')
    stored.public_ready = True
    assert run_upgrade(upgrade, tmp_path)['status'] == 'management_upgraded'
    deployment = stored.switch.document
    deployment['status'] = {'observedGeneration': deployment['metadata']['generation'],
        'replicas': 1, 'readyReplicas': 1, 'updatedReplicas': 1, 'availableReplicas': 1}
    if fault == 'stale_generation':
        deployment['status']['observedGeneration'] -= 1
    elif fault == 'wrong_image':
        deployment['spec']['template']['spec']['containers'][0]['image'] = 'unqualified/image:latest'
    proofs = []

    def public(self, *, admin_token):
        assert self.runtime == 'applications'
        assert admin_token
        proofs.append(self.api_server)
        if fault == 'public_auth':
            raise RuntimeError('private-auth-failure')

    monkeypatch.setattr(ManagementPublicProbe, 'verify', public)
    api, state, checks, _ = connected
    if fault == 'stale_generation':
        assert api.verify_public(request, tmp_path / 'upgrade') is False
    elif fault:
        with pytest.raises(ManagementUpgradeError):
            api.verify_public(request, tmp_path / 'upgrade')
    else:
        assert api.verify_public(request, tmp_path / 'upgrade') is True
    assert len(proofs) == (0 if fault in {'stale_generation', 'wrong_image'} else 1)
    assert checks.calls == ([] if fault in {'stale_generation', 'wrong_image'} else ['public_route'])
    assert all(row.method == 'GET' for row in state['calls'])
