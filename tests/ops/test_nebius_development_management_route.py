"""Fresh manager routing reads shared infrastructure without staging activation."""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def route(installation, monkeypatch):
    from scripts.ops import nebius_development_management_route as module

    from loom.nebius_platform_render import digest

    request = installation[0]
    raw = request.deployment.model_dump(mode='json')
    raw['installation']['foundation']['ingress_controller_label'] = 'loom-shared-ingress'
    request = replace(request, deployment=type(request.deployment).model_validate(raw))
    foundation = request.deployment.installation.foundation
    ns = foundation.ingress_namespace
    def obj(api, kind, name, namespace=None, **parts):
        return {'apiVersion': api, 'kind': kind, 'metadata': {'name': name, 'uid': str(uuid4()),
            **({'namespace': namespace} if namespace else {})}, **parts}
    resources = {
        'namespace': obj('v1', 'Namespace', ns),
        'service': obj('v1', 'Service', 'loom-web', ns, spec={
            'type': 'LoadBalancer', 'selector': {'app': 'loom-shared-ingress'},
            'ports': [{'port': 443, 'targetPort': 8443, 'protocol': 'TCP'}]},
            status={'loadBalancer': {'ingress': [{'ip': '8.8.8.8'}]}}),
        'controller': obj('apps/v1', 'Deployment', 'loom-shared-ingress', ns,
            spec={'replicas': 1, 'template': {'metadata': {'labels': {'app': 'loom-shared-ingress',
                'app.kubernetes.io/name': 'loom-shared-ingress'}}, 'spec': {'volumes': [
                    {'name': 'config', 'configMap': {'name': 'loom-shared-ingress'}}]}}},
            status={'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'updatedReplicas': 1, 'availableReplicas': 1}),
        'config': obj('v1', 'ConfigMap', 'loom-shared-ingress', ns, data={
            'traefik.json': json.dumps({'providers': {'kubernetesIngress': {'ingressClass': 'loom-shared'}}}),
            'routes.yaml': json.dumps({'tcp': {'routers': {'standalone': {'rule': 'HostSNI(`staging.example.com`)'}}}})}),
        'ingress_class': obj('networking.k8s.io/v1', 'IngressClass', 'loom-shared',
            spec={'controller': 'traefik.io/ingress-controller'})}
    resources['controller']['metadata']['generation'] = 1
    settings = module.DevelopmentManagementRouteSettings(
        **{key + '_uid': value['metadata']['uid'] for key, value in resources.items()},
        service_spec_digest=digest(resources['service']['spec']),
        controller_spec_digest=digest(resources['controller']['spec']),
        config_data_digest=digest(resources['config']['data']))
    calls, dns_names, tls_names, ingresses = [], [], [], []
    def handle(message):
        assert message.method == 'GET'
        calls.append(message.url.path)
        paths = {
            '/api/v1/namespaces/' + ns: resources['namespace'],
            '/api/v1/namespaces/kube-system': {'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': 'kube-system', 'uid': request.binding.kube_system_uid}},
            '/api/v1/namespaces/' + ns + '/services/loom-web': resources['service'],
            '/api/v1/namespaces/' + ns + '/configmaps/loom-shared-ingress': resources['config'],
            '/apis/apps/v1/namespaces/' + ns + '/deployments/loom-shared-ingress': resources['controller'],
            '/apis/networking.k8s.io/v1/ingressclasses/loom-shared': resources['ingress_class'],
            '/apis/networking.k8s.io/v1/ingresses': {'apiVersion': 'networking.k8s.io/v1', 'kind': 'IngressList',
                'metadata': {'resourceVersion': '6'}, 'items': ingresses}}
        return httpx.Response(200, json=paths[message.url.path])
    api = module.HTTPSDevelopmentManagementRoute(settings=settings,
        api_server=foundation.platform_config['kubernetes_api_server'], ssl_context=ssl.create_default_context(), token='operator')
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handle))
    monkeypatch.setattr(module, 'qualify_dns_address', lambda host, address: dns_names.append((host, address)))
    monkeypatch.setattr(module, 'qualify_tls_address', lambda host, address, fingerprint=None: tls_names.append((host, address, fingerprint)))
    with api:
        yield api, request, resources, calls, dns_names, tls_names, ingresses


def test_initial_preflight_needs_no_manager_tls_or_staging_application_config(route):
    api, request, _, calls, dns_names, tls_names, _ = route
    api.preflight(request)
    assert request.deployment.public_host in {host for host, _ in dns_names}
    assert len(tls_names) == 1 and tls_names[0][0].endswith('.' + request.deployment.installation.foundation.public_dns_zone)
    assert tls_names[0][2] is None  # Existing wildcard renewal need not match an old fingerprint.
    assert all('loom-platform-config' not in path and '/secrets/' not in path for path in calls)


def test_final_route_requires_matching_new_manager_certificate(route):
    api, request, _, _, _, tls_names, ingresses = route
    ingresses.append(own_route(request))
    api.verify_public(request)
    assert tls_names[-1][:2] == (request.deployment.public_host, '8.8.8.8')
    assert len(tls_names[-1][2]) == 64


def test_renewal_route_uses_retained_tls_and_needs_no_old_leaf_or_wildcard(route, monkeypatch):
    from scripts.ops import nebius_development_management_route as module

    api, request, _, _, dns_names, tls_names, ingresses = route
    expected = own_route(request)
    expected['metadata']['uid'] = str(uuid4())
    expected['spec']['tls'][0]['secretName'] = 'loom-management-tls-successor'
    ingresses.append(copy.deepcopy(expected))
    def expired(*args, **kwargs):
        raise AssertionError('renewal must not validate the predecessor or probe wildcard TLS')
    monkeypatch.setattr(module.certificates, 'validate_management_certificate', expired)
    monkeypatch.setattr(module, 'qualify_tls_address', expired)
    assert api.qualify_retained(deployment=request.deployment,
        kube_system_uid=request.binding.kube_system_uid, expected=expected) == '8.8.8.8'
    assert dns_names == [(request.deployment.public_host, '8.8.8.8')]
    assert not tls_names


@pytest.mark.parametrize('change', ['uid', 'annotation', 'deleting'])
def test_renewal_route_requires_retained_identity_and_metadata(route, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _, dns_names, _, ingresses = route
    expected = own_route(request)
    expected['metadata']['uid'] = str(uuid4())
    row = copy.deepcopy(expected)
    if change == 'uid':
        row['metadata']['uid'] = str(uuid4())
    elif change == 'annotation':
        row['metadata'].setdefault('annotations', {})['foreign'] = 'true'
    else:
        row['metadata']['deletionTimestamp'] = '2026-10-07T00:00:00Z'
    ingresses.append(row)
    with pytest.raises(ManagementInstallError):
        api.qualify_retained(deployment=request.deployment,
            kube_system_uid=request.binding.kube_system_uid, expected=expected)
    assert not dns_names


def test_renewal_accepts_explicit_fresh_controller_pin_without_changing_controller(route):
    from loom.nebius_platform_render import digest

    api, request, resources, calls, _, _, ingresses = route
    expected = own_route(request)
    expected['metadata']['uid'] = str(uuid4())
    ingresses.append(expected)
    resources['controller']['spec']['template']['spec']['volumes'].append(
        {'name': 'tls', 'secret': {'secretName': 'new-default-certificate'}})
    api.settings = api.settings.model_copy(update={
        'controller_spec_digest': digest(resources['controller']['spec'])})
    assert api.qualify_retained(deployment=request.deployment,
        kube_system_uid=request.binding.kube_system_uid, expected=expected) == '8.8.8.8'
    assert not any('/secrets/' in path for path in calls)


def own_route(request):
    from loom_service.environment_management.deployment import render_management

    return render_management(request.deployment, candidate=request.candidate, profile=request.profile,
        repo_root=Path(__file__).resolve().parents[2]).files['70-public.yaml'][0]


def test_shared_route_is_qualified_without_controller_or_secret_writes(route, monkeypatch):
    from scripts.ops import nebius_development_management_route as module
    from scripts.ops.nebius_development_public import render_development_public
    from tests.ops.test_nebius_development_public import public_request

    api, request, _, calls, dns_names, tls_names, ingresses = route
    request = public_request(request)
    host = request.deployment.installation.foundation.platform_config['public_host']
    api.preflight(request)
    assert (host, '8.8.8.8') in dns_names
    assert (host, '8.8.8.8', None) in tls_names
    ingresses.extend([own_route(request), render_development_public(request.deployment)[1]])
    probes = []
    monkeypatch.setattr(module, 'probe_shared_development', lambda **kwargs: probes.append(kwargs))
    api.verify_public(request, shared_candidate='b' * 40)
    assert probes == [{'address': '8.8.8.8', 'port': 443, 'hostname': host, 'candidate': 'b' * 40}]
    assert not any('/secrets/' in path for path in calls)


@pytest.mark.parametrize('conflict', ['exact', 'wildcard', 'passthrough', 'wrong-backend'])
def test_shared_hostname_conflicts_fail_before_public_connections(route, conflict):
    from scripts.ops.nebius_development_public import render_development_public
    from scripts.ops.nebius_management_install import ManagementInstallError
    from tests.ops.test_nebius_development_public import public_request

    from loom.nebius_platform_render import digest

    api, request, resources, _, dns_names, tls_names, ingresses = route
    request = public_request(request)
    host = request.deployment.installation.foundation.platform_config['public_host']
    if conflict == 'passthrough':
        data = resources['config']['data']
        data['routes.yaml'] = json.dumps({'tcp': {'routers': {'dev': {'rule': 'HostSNI(`' + host + '`)'}}}})
        api.settings = api.settings.model_copy(update={'config_data_digest': digest(data)})
    elif conflict == 'wrong-backend':
        row = render_development_public(request.deployment)[1]
        row['spec']['rules'][0]['http']['paths'][0]['backend']['service']['name'] = 'foreign'
        ingresses.append(row)
    else:
        ingresses.append({'apiVersion': 'networking.k8s.io/v1', 'kind': 'Ingress',
            'metadata': {'name': 'foreign', 'namespace': 'foreign'},
            'spec': {'rules': [{'host': host if conflict == 'exact' else '*.' + host.partition('.')[2]}]}})
    with pytest.raises(ManagementInstallError):
        api.preflight(request)
    assert not dns_names and not tls_names


def test_shared_final_proof_requires_ingress_and_retained_foundation_candidate(route):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from tests.ops.test_nebius_development_public import public_request

    api, request, _, _, _, _, ingresses = route
    request = public_request(request)
    ingresses.append(own_route(request))
    with pytest.raises(ManagementInstallError):
        api.verify_public(request, shared_candidate='b' * 40)
    with pytest.raises(ManagementInstallError):
        api.verify_public(request)


@pytest.mark.parametrize('change', ['namespace_uid', 'service_uid', 'controller_uid', 'config', 'class', 'selector', 'not_ready', 'address'])
def test_route_drift_blocks_before_dns_or_public_connection(route, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, resources, _, dns_names, tls_names, _ = route
    if change.endswith('_uid'):
        resources[change.removesuffix('_uid')]['metadata']['uid'] = str(uuid4())
    elif change == 'config':
        resources['config']['data']['routes.yaml'] = '{}'
    elif change == 'class':
        resources['ingress_class']['spec']['controller'] = 'other-controller'
    elif change == 'selector':
        resources['service']['spec']['selector'] = {'app': 'loom-web'}
    elif change == 'not_ready':
        resources['controller']['status']['readyReplicas'] = 0
    else:
        resources['service']['status']['loadBalancer']['ingress'] = [{'hostname': 'foreign.test'}]
    with pytest.raises(ManagementInstallError):
        api.preflight(request)
    assert not dns_names and not tls_names


@pytest.mark.parametrize('wildcard', [False, True])
def test_competing_host_blocks_even_if_ingress_class_differs(route, wildcard):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _, dns_names, _, ingresses = route
    host = request.deployment.public_host
    ingresses.append({'apiVersion': 'networking.k8s.io/v1', 'kind': 'Ingress',
        'metadata': {'name': 'foreign', 'namespace': 'foreign'},
        'spec': {'ingressClassName': 'other', 'rules': [{'host': '*.' + host.partition('.')[2] if wildcard else host}]}})
    with pytest.raises(ManagementInstallError):
        api.preflight(request)
    assert not dns_names


def test_own_route_must_use_exact_tls_and_backend_before_final_probe(route):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _, _, tls_names, ingresses = route
    ingresses.append(copy.deepcopy(own_route(request)))
    api.preflight(request)
    ingresses[0]['spec']['tls'][0]['secretName'] = 'foreign-secret'
    tls_names.clear()
    with pytest.raises(ManagementInstallError):
        api.verify_public(request)
    assert not tls_names


def test_final_route_without_ingress_cannot_report_ready(route):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _, dns_names, tls_names, _ = route
    with pytest.raises(ManagementInstallError):
        api.verify_public(request)
    assert not dns_names and not tls_names


def test_shared_controller_change_during_public_probe_is_not_accepted(route, monkeypatch):
    from scripts.ops import nebius_development_management_route as module
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, resources, _, _, _, _ = route
    def changed(*args, **kwargs):
        resources['controller']['metadata']['uid'] = str(uuid4())
    monkeypatch.setattr(module, 'qualify_tls_address', changed)
    with pytest.raises(ManagementInstallError):
        api.preflight(request)


@pytest.mark.parametrize('change', ['passthrough', 'namespace_filter'])
def test_pinned_but_incompatible_controller_configuration_does_not_qualify(route, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    from loom.nebius_platform_render import digest

    api, request, resources, _, dns_names, _, _ = route
    data = resources['config']['data']
    if change == 'passthrough':
        data['routes.yaml'] = json.dumps({'tcp': {'routers': {'standalone': {
            'rule': 'HostSNI(`' + request.deployment.public_host + '`)'}}}})
    else:
        data['traefik.json'] = json.dumps({'providers': {'kubernetesIngress': {
            'ingressClass': 'loom-shared', 'namespaces': ['staging-only']}}})
    api.settings = api.settings.model_copy(update={'config_data_digest': digest(data)})
    with pytest.raises(ManagementInstallError):
        api.preflight(request)
    assert not dns_names


@pytest.mark.parametrize('change', [None, 'alias', 'extra_address', 'ipv6'])
def test_dns_qualification_checks_normal_resolution_before_credentials(monkeypatch, change):
    from types import SimpleNamespace

    import dns.name
    from scripts.ops import nebius_development_management_route as module
    from scripts.ops.nebius_management_install import ManagementInstallError

    class Answer(list):
        canonical_name = dns.name.from_text('foreign.test' if change == 'alias' else 'manage.example.com')
    calls = []
    def resolve(host, kind, **kwargs):
        calls.append((host, kind))
        assert kwargs == {'lifetime': 10, 'search': False, 'raise_on_no_answer': False}
        addresses = ['8.8.8.8'] + (['1.1.1.1'] if change == 'extra_address' else [])
        return Answer(addresses if kind == 'A' else (['::1'] if change == 'ipv6' else []))
    monkeypatch.setattr(module.dns.resolver, 'Resolver', lambda: SimpleNamespace(resolve=resolve))
    if change:
        with pytest.raises(ManagementInstallError):
            module.qualify_dns_address('manage.example.com', '8.8.8.8')
    else:
        module.qualify_dns_address('manage.example.com', '8.8.8.8')
        assert calls == [('manage.example.com', 'A'), ('manage.example.com', 'AAAA')]
