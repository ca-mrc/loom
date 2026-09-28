"""Authority qualification must use the authenticated runtime subject, not operator identity."""
from __future__ import annotations

import json
import ssl
from uuid import uuid4

import httpx
import pytest

from loom.nebius_management_authority import ManagementNamespaceAuthority


@pytest.fixture
def probe():
    from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe

    authority = ManagementNamespaceAuthority(installation_id=uuid4(), namespace="loom-nebius-management")
    uid = str(uuid4())
    return authority, uid, HTTPSManagementAuthorityProbe(authority=authority, service_account_uid=uid,
        api_server="https://cluster.example", ssl_context=ssl.create_default_context(), token="runtime-only-token")


def responder(authority, uid, requests, *, change=None):
    def handle(request):
        requests.append(request)
        doc = json.loads(request.content)
        path = request.url.path
        if path.endswith("selfsubjectreviews"):
            value = {"status": {"userInfo": {"username": "system:serviceaccount:" + authority.namespace + ":loom-management-provisioner",
                "uid": uid, "groups": ["system:serviceaccounts", "system:serviceaccounts:" + authority.namespace, "system:authenticated"]}}}
            if change == "operator":
                value["status"]["userInfo"]["username"] = "operator"
            if change == "replaced_account":
                value["status"]["userInfo"]["uid"] = str(uuid4())
            return httpx.Response(201, json=value)
        if path.endswith("selfsubjectaccessreviews"):
            attrs = doc["spec"]["resourceAttributes"]
            allowed = (attrs["verb"], attrs["resource"]) in {("create", "namespaces"), ("get", "namespaces"), ("create", "rolebindings"), ("bind", "clusterroles")}
            if change == "global_secrets" and attrs["resource"] == "secrets":
                allowed = True
            return httpx.Response(201, json={"status": {"allowed": allowed}})
        assert request.url.params["dryRun"] == "All"
        assert request.method == "POST"
        if path == "/api/v1/namespaces":
            if doc["metadata"]["name"].startswith("loom-dev-"):
                return httpx.Response(201, json=doc)
            message = "management namespace boundary"
            suffix = "namespaces"
        else:
            assert path.endswith("/rolebindings")
            message = "management namespace binding boundary"
            suffix = "bindings"
        if change == "policy_absent":
            return httpx.Response(201, json=doc)
        if change == "unrelated_denial":
            message = "denied by unrelated permission"
        return httpx.Response(403, json={"kind": "Status", "reason": "Forbidden", "code": 403,
            "message": "ValidatingAdmissionPolicy '" + authority.name + "-" + suffix + "': " + message})
    return handle


def test_probe_checks_real_subject_dry_run_admission_and_forbidden_privileges(probe):
    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests)))
    with api:
        assert api.qualify() is True
    assert any(r.url.path.endswith("selfsubjectreviews") for r in requests)
    assert any(r.url.path.endswith("selfsubjectaccessreviews") for r in requests)
    assert all(r.url.params.get("dryRun") == "All" for r in requests if r.url.path.endswith(("namespaces", "rolebindings")))


@pytest.mark.parametrize("change", ["operator", "replaced_account", "global_secrets", "unrelated_denial"])
def test_wrong_subject_or_excess_privilege_cannot_qualify(probe, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests, change=change)))
    with api, pytest.raises(ManagementStageError):
        api.qualify()
    if change in {"operator", "replaced_account"}:
        assert len(requests) == 1


def test_unpropagated_admission_returns_pending_without_a_persisted_probe(probe):
    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests, change="policy_absent")))
    with api:
        assert api.qualify() is False
    assert all(r.url.params.get("dryRun") == "All" for r in requests if r.url.path.endswith(("namespaces", "rolebindings")))


@pytest.mark.parametrize('change', [None, 'operator', 'legacy_account', 'shared_write', 'shared_secret_read',
    'missing_observer', 'policy_absent', 'unrelated_denial'])
def test_application_subject_probe_qualifies_shared_reads_but_not_legacy_or_shared_authority(change):
    from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
    from scripts.ops.nebius_management_stage import ManagementStageError

    from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1

    authority = ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace='loom-nebius-management',
        cluster_id='cluster-fixture', data_environment_id=uuid4(), shared_namespace='loom-nebius-platform')
    uid, requests = str(uuid4()), []

    def handle(request):
        requests.append(request)
        doc = json.loads(request.content)
        path = request.url.path
        if path.endswith('selfsubjectreviews'):
            name = 'operator' if change == 'operator' else 'system:serviceaccount:loom-nebius-management:' + (
                'loom-management-provisioner' if change == 'legacy_account' else 'loom-application-provisioner')
            return httpx.Response(201, json={'status': {'userInfo': {'username': name, 'uid': uid, 'groups': [
                'system:serviceaccounts', 'system:serviceaccounts:loom-nebius-management', 'system:authenticated']}}})
        if path.endswith('selfsubjectaccessreviews'):
            attrs = doc['spec']['resourceAttributes']
            namespace = attrs.get('namespace')
            allowed = not namespace and (attrs['verb'], attrs['resource']) in {
                ('create', 'namespaces'), ('get', 'namespaces'), ('create', 'rolebindings'), ('bind', 'clusterroles')}
            if namespace == authority.shared_namespace:
                allowed = (attrs['resource'] == 'networkpolicies' and attrs['verb'] == 'get'
                    and attrs.get('name') in {authority.name + '-' + suffix for suffix in ('postgres', 'control-plane', 'gateway')}
                    and change != 'missing_observer')
                if (attrs['resource'], attrs['verb']) == ('secrets', 'create') and change == 'shared_write':
                    allowed = True
                if (attrs['resource'], attrs['verb']) == ('secrets', 'get') and change == 'shared_secret_read':
                    allowed = True
            return httpx.Response(201, json={'status': {'allowed': allowed}})
        assert request.method == 'POST' and request.url.params['dryRun'] == 'All'
        if path == '/api/v1/namespaces':
            labels = doc['metadata']['labels']
            owned = (doc['metadata']['name'].startswith('loom-dev-')
                and labels.get('loom.nebius/application-installation') == str(authority.installation_id)
                and labels.get('loom.nebius/data-environment-id') == str(authority.data_environment_id)
                and 'loom.nebius/environment-id' not in labels and 'loom.nebius/namespace-installation' not in labels)
            if owned:
                return httpx.Response(201, json=doc)
            suffix = 'namespaces'
        else:
            assert path.endswith('/rolebindings')
            assert doc['subjects'][0]['name'] == 'loom-application-provisioner'
            suffix = 'bindings'
        if change == 'policy_absent':
            return httpx.Response(201, json=doc)
        message = ('unrelated rejection' if change == 'unrelated_denial'
            else authority.name + '-' + suffix + ': application ' + suffix + ' boundary')
        return httpx.Response(403, json={'kind': 'Status', 'reason': 'Forbidden', 'code': 403, 'message': message})

    with HTTPSManagementAuthorityProbe(authority=authority, service_account_uid=uid, api_server='https://cluster.example',
                                      ssl_context=ssl.create_default_context(), token='application-token') as api:
        api.client.close()
        api.client = httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(handle))
        if change in {'operator', 'legacy_account', 'shared_write', 'shared_secret_read', 'unrelated_denial'}:
            with pytest.raises(ManagementStageError):
                api.qualify()
        else:
            assert api.qualify() is (change is None)
    if change in {'operator', 'legacy_account'}:
        assert len(requests) == 1
    if change is None:
        namespaces = [json.loads(row.content) for row in requests if row.url.path == '/api/v1/namespaces']
        assert any(row['metadata']['name'] == authority.shared_namespace for row in namespaces)
        assert any('loom.nebius/namespace-installation' in row['metadata']['labels'] for row in namespaces)
        scoped = [json.loads(row.content)['spec']['resourceAttributes'] for row in requests
            if row.url.path.endswith('selfsubjectaccessreviews')]
        assert {row.get('name') for row in scoped if row.get('namespace') == authority.shared_namespace
            and row['resource'] == 'networkpolicies' and row['verb'] == 'get'} >= {
                authority.name + '-' + purpose for purpose in ('postgres', 'control-plane', 'gateway')}
