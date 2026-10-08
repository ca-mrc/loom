"""Shared HTTPS is opt-in, dev-only and additive to retained private workloads."""
from __future__ import annotations

import json
from dataclasses import replace

import pytest
from tests.ops.test_nebius_development_management_install import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_install import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_install import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_install import material as material
from tests.ops.test_nebius_development_management_install import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_install import run, to_admission
from tests.ops.test_nebius_development_management_install import tls_material as tls_material


def public_request(request, host=None):
    raw = request.deployment.model_dump(mode='json')
    foundation = raw['installation']['foundation']
    config = json.loads(foundation['platform_config_json'])
    config['public_host'] = host or 'shared.' + foundation['public_dns_zone']
    foundation['platform_config_json'] = json.dumps(config)
    return replace(request, deployment=type(request.deployment).model_validate(raw), shared_public_route=True)


def test_shared_public_phase_is_last_additive_dev_only_and_replayable(installation, tmp_path):
    request, api = installation
    request = public_request(request)
    installation = request, api
    to_admission(installation, tmp_path)
    api.admit()
    assert run(installation, tmp_path)['phase'] == 'application-database'
    api.complete('Job')
    assert run(installation, tmp_path)['phase'] == 'backup'
    api.complete('Job')
    assert run(installation, tmp_path)['phase'] == 'service'
    assert not any(doc['kind'] == 'Ingress' for doc in api.store.resources.values())
    api.complete('Deployment')
    result = run(installation, tmp_path)
    assert result['status'] == 'development_management_installed'
    route = api.store.resources['Ingress:loom-development']
    assert route['metadata']['namespace'] == 'loom-dev'
    host = request.deployment.installation.foundation.platform_config['public_host']
    assert route['spec'] == {'ingressClassName': 'loom-shared', 'tls': [{'hosts': [host]}],
        'rules': [{'host': host, 'http': {'paths': [
            {'path': '/api', 'pathType': 'Prefix', 'backend': {'service': {'name': 'loom-service', 'port': {'number': 8090}}}},
            {'path': '/', 'pathType': 'Prefix', 'backend': {'service': {'name': 'loom-web', 'port': {'number': 8080}}}},
        ]}}]}
    policy = api.store.resources['NetworkPolicy:loom-development-public']
    assert policy['metadata']['namespace'] == 'loom-dev'
    assert policy['spec']['podSelector'] == {'matchExpressions': [
        {'key': 'app', 'operator': 'In', 'values': ['loom-service', 'loom-web']}]}
    foundation = request.deployment.installation.foundation
    assert policy['spec']['ingress'] == [{'from': [{
        'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': foundation.ingress_namespace}},
        'podSelector': {'matchLabels': {'app.kubernetes.io/name': foundation.ingress_controller_label}},
    }], 'ports': [{'protocol': 'TCP', 'port': 8080}, {'protocol': 'TCP', 'port': 8090}]}]
    assert api.store.creates[-2:] == ['NetworkPolicy:loom-development-public', 'Ingress:loom-development']
    parent = json.loads((tmp_path / 'state/installation.json').read_text())
    assert parent['phases']['application-development-public']['status'] == 'complete'
    creates = list(api.store.creates)
    assert run(installation, tmp_path) == result
    assert api.store.creates == creates


@pytest.mark.parametrize('host', ['dev.example.com', 'deep.shared.dev.example.com', 'staging.example.net'])
def test_uncovered_shared_hostname_rejected_before_bootstrap(installation, tmp_path, host):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    request = public_request(request, host)
    with pytest.raises(ManagementInstallError):
        run((request, api), tmp_path)
    assert api.store is None
    assert not (tmp_path / 'anchor').exists()


def test_private_installation_cannot_be_replayed_as_public(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    public = public_request(request)
    private = replace(public, shared_public_route=False)
    assert run((private, api), tmp_path)['phase'] == 'database'
    creates = list(api.store.creates)
    with pytest.raises(ManagementInstallError, match='recovery'):
        run((public, api), tmp_path)
    assert api.store.creates == creates


def test_only_fixed_dev_resources_are_exposed_to_application_setup(installation):
    from scripts.ops.nebius_application_setup import _documents
    from scripts.ops.nebius_development_management_install import _setup
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import ManagementStageError

    request = public_request(installation[0])
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
        request.shared_namespace_uid, request.binding.kube_system_uid)
    setup = _setup(request, binding)
    docs = _documents(setup, 'development-public')
    assert set(docs) == {'Ingress:loom-dev:loom-development', 'NetworkPolicy:loom-dev:loom-development-public'}
    with pytest.raises(ManagementStageError):
        _documents(_setup(replace(request, shared_public_route=False), binding), 'development-public')


@pytest.mark.parametrize('change', ['extra-route', 'extra-peer', 'annotation'])
def test_public_defaulting_cannot_expand_the_fixed_route(installation, tmp_path, change):
    from scripts.ops.nebius_application_setup import stage_application_setup
    from scripts.ops.nebius_development_management_install import _setup
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import ManagementStageError
    from tests.ops.test_nebius_management_stage import PhaseAPI

    request = public_request(installation[0])
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
        request.shared_namespace_uid, request.binding.kube_system_uid)
    api = PhaseAPI(binding)
    def injected(doc):
        if change == 'extra-route' and doc['kind'] == 'Ingress':
            doc['spec']['rules'].append({'host': 'foreign.example.com', 'http': doc['spec']['rules'][0]['http']})
        elif change == 'extra-peer' and doc['kind'] == 'NetworkPolicy':
            doc['spec']['ingress'][0]['from'].append({'namespaceSelector': {}})
        elif change == 'annotation':
            doc['metadata']['annotations']['foreign.example/controller-override'] = 'true'
    api.default_change = injected
    with pytest.raises(ManagementStageError):
        stage_application_setup(request=_setup(request, binding), phase='development-public', api=api, state_dir=tmp_path / 'public')
    assert not api.creates
