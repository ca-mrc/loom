"""Concrete schema transport preserves phase authority and exact HTTPS effects."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
import ssl
from contextlib import ExitStack, nullcontext
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from httpx import Client as HTTPClient
from tests.ops.test_nebius_development_schema_projection import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_schema_projection import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_schema_projection import (
    connected as connected,
)
from tests.ops.test_nebius_development_schema_projection import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    entry as entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_schema_projection import (
    installation as installation,
)
from tests.ops.test_nebius_development_schema_projection import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_schema_projection import (
    live as live,
)
from tests.ops.test_nebius_development_schema_projection import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    material as material,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_schema_projection import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    publication as publication,
)
from tests.ops.test_nebius_development_schema_projection import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_schema_projection import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    retained as retained,
)
from tests.ops.test_nebius_development_schema_projection import (
    route as route,
)
from tests.ops.test_nebius_development_schema_projection import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_schema_projection import (
    successor_inputs as successor_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_development_schema_upgrade import (
    SchemaParentAPI,
)
from tests.ops.test_nebius_development_schema_upgrade import (
    schema_retained as schema_retained,
)


def module():
    name = 'scripts.ops.nebius_development_schema_live'
    assert importlib.util.find_spec(name), 'concrete schema transport is missing'
    return importlib.import_module(name)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_phase_inventory_uses_recorded_successors_after_replacement(schema_retained):
    from scripts.ops import nebius_development_schema_upgrade as schema
    live = module()
    fake = SchemaParentAPI(schema_retained)
    payload = b'PGDMP' + b'streamed exact checkpoint'
    checksum = hashlib.sha256(payload).hexdigest()
    original_report = fake.report
    def report(phase, record):
        proof = original_report(phase, record)
        if phase.endswith('-backup'):
            proof.update(sha256=checksum, bytes=len(payload),
                key=proof['key'].split('/')[0] + '/2026/10/20/120000-' + checksum[:12] + '.dump')
        return proof
    fake.report = report
    result = schema.run_development_schema_upgrade(retained=schema_retained, api=fake, action='apply')
    assert result['status'] == 'complete'
    record = schema._record(schema_retained)
    options = live.schema_inventory_options(schema_retained, record)
    from scripts.ops.nebius_management_stage import _canonical_quantities
    for key, desired in schema_retained.plan.targets.items():
        actual, = options[key]
        assert _canonical_quantities(actual)['spec'] == _canonical_quantities(desired)['spec']
        assert actual['metadata']['uid'] == schema_retained.plan.originals[key]['metadata']['uid']
    key = next(iter(schema_retained.plan.targets))
    assert options[key][0]['spec'] != schema_retained.plan.originals[key]['spec']
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    api.retained, api.request = schema_retained, schema_retained.runtime.request
    api.frame, api.prechild = copy.deepcopy(record), None
    api._private_inputs = lambda: None
    api._database = lambda namespace: {}
    api._census = lambda observed, current: None
    api._closed = lambda current: None
    api.inspect = lambda **kwargs: pytest.fail('completed successors must not call original-only inspect')
    resources = {key: copy.deepcopy(choices[0]) for key, choices in options.items()}
    api.manager = SimpleNamespace(binding=fake.store.binding)
    api._request = lambda method, path: next(copy.deepcopy(row) for key, row in resources.items()
        if path == resource_path(live, row))
    observed = api._observe(record)
    assert observed == resources
    policy = live._PolicyProof(api, observed)
    assert len(policy.build_policy) == 2
    assert policy.build_profile.target.namespace == schema_retained.projection.participant.build_namespace.name
    from tests.ops.test_nebius_management_proofs import Objects
    for mode in ('shared', 'manager'):
        phase = mode + '-backup'
        staged = schema._staged_resources(schema_retained, record, phase)
        job, = (copy.deepcopy(row) for row in staged.values() if row['kind'] == 'Job')
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
        namespace = job['metadata']['namespace']
        template = job['spec']['template']
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': namespace,
            'name': job['metadata']['name'] + '-abc', 'uid': str(uuid4()), 'labels': template['metadata']['labels'],
            'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
                'uid': job['metadata']['uid'], 'controller': True}]}, 'spec': template['spec'],
            'status': {'phase': 'Succeeded', **{status: [{'name': row['name'], 'restartCount': 0,
                'state': {'terminated': {'exitCode': 0}}} for row in template['spec'].get(field, [])]
                for field, status in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses'))}}}
        receipt = record['phases'][phase]['proof']
        uploader = dict(backup_key=receipt['key'], sha256=checksum, bytes=len(payload))
        staged['Job:' + namespace + ':' + job['metadata']['name']] = job
        api._recorded = lambda selected, current, staged=staged: copy.deepcopy(staged)
        calls = []
        def transport(request, pod=pod, uploader=uploader, calls=calls):
            calls.append(request)
            if request.url.path.endswith('/log'):
                return httpx.Response(200, json=uploader)
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'items': [pod]}
                if request.url.path.endswith('/pods') else pod)
        objects = Objects(payload, checksum)
        api._backup_client = lambda objects=objects: nullcontext(objects)
        with HTTPClient(base_url='https://kubernetes.example', transport=httpx.MockTransport(transport)) as client:
            api.client = client
            api._request = lambda method, path: client.get(path).json()
            assert api.report(phase, record) == receipt
        assert all('/namespaces/' + namespace + '/' in call.url.path for call in calls)
        assert [name for name, args in objects.calls] == ['head', 'get', 'head']
        assert objects.body.closed


def resource_path(live, row):
    version, resource, cluster = live._READ_PATHS[row['kind']]
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + ('' if cluster else
        '/namespaces/' + row['metadata']['namespace']) + '/' + resource + '/' + row['metadata']['name']


def test_configmap_transition_cas_changes_data_and_never_retries_conflict():
    live = module()
    original = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {
        'name': 'loom-platform-config', 'namespace': 'loom-dev', 'uid': str(uuid4()), 'resourceVersion': '7'},
        'data': {'profile.json': 'old'}}
    from scripts.ops.nebius_ingress_stage import _snapshot
    target = _snapshot(original)
    target['data']['profile.json'] = 'approved'
    key = 'ConfigMap:loom-dev:loom-platform-config'
    calls = []
    def route(request):
        calls.append(request)
        return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'reason': 'Conflict', 'code': 409})
    with httpx.Client(transport=httpx.MockTransport(route), base_url='https://kubernetes.example') as http:
        parent = SimpleNamespace(client=http, _child_qualify=lambda *args, **kwargs: None)
        adapter = live.SchemaWorkloads(parent, 'policy', {key: original}, {key: target})
        assert adapter.patch_workload(key, original, target) is False
    assert len(calls) == 1 and calls[0].method == 'PATCH'
    assert calls[0].url.path == '/api/v1/namespaces/loom-dev/configmaps/loom-platform-config'
    operations = json.loads(calls[0].content)
    assert {'op': 'test', 'path': '/metadata/uid', 'value': original['metadata']['uid']} in operations
    assert {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '7'} in operations
    assert {'op': 'test', 'path': '/data', 'value': original['data']} in operations
    assert {'op': 'replace', 'path': '/data', 'value': target['data']} in operations
    assert not any(row['path'] == '/spec' for row in operations)


@pytest.mark.parametrize('phase', ['shared-probe', 'manager-upgrade', 'shared-inspect'])
def test_database_query_is_rejected_while_any_schema_job_is_active(phase):
    live = module()
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    api.frame = {'phases': {phase: {'status': 'started'}}}
    api._private_inputs = lambda: None
    api._database = lambda namespace: pytest.fail('active schema Job must not share a SQL session')
    with pytest.raises(ValueError):
        api._sql('loom-dev', 'SELECT 1')


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_https_stage_dispatch_qualifies_local_prechild_then_requires_persisted_child(schema_retained, monkeypatch):
    from scripts.ops import nebius_development_schema_upgrade as schema
    from scripts.ops.nebius_ingress_stage import _key
    live = module()
    fake = SchemaParentAPI(schema_retained)
    fake.pending = 'shared-probe'
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    api.retained = schema_retained
    api.frame = api.external_frame = api.prechild = None
    api.external_checked = False
    api.manager = SimpleNamespace(binding=fake.store.binding)
    api.api_server = 'https://kubernetes.example'
    api.context, api.token = ssl.create_default_context(), 'explicit-test-token'
    api.children = ExitStack()
    api._private_inputs = lambda: None
    api.inspect = lambda **kwargs: pytest.fail('original-only inventory must not be used')
    api._database = lambda namespace: {}
    api._census = lambda observed, record: None
    api._closed = lambda record: None
    api._capacity = lambda: None
    async def external(record):
        pass
    api._external = external
    calls, prechildren = [], []
    for key, options in live.schema_inventory_options(schema_retained, None).items():
        fake.store.resources.setdefault(key, copy.deepcopy(options[0]))
    def handle(request):
        calls.append(request)
        if request.method == 'GET':
            matches = [row for row in fake.store.resources.values() if request.url.path == resource_path(live, row)]
            if not matches:
                return httpx.Response(404)
            assert len(matches) == 1
            return httpx.Response(200, json=matches[0])
        assert request.method == 'POST'
        document = json.loads(request.content)
        if request.url.params.get('dryRun') == 'All':
            prechildren.append(api.prechild)
            return httpx.Response(201, json=fake.store.default_resource(document))
        fake.store.create_resource(document)
        return httpx.Response(201, json=fake.store.resources[_key(document)])
    def route(request):
        try:
            return handle(request)
        except Exception as error:
            pytest.fail('mock Kubernetes transport failed: ' + repr(error))
    clients = ExitStack()
    def client():
        return clients.enter_context(HTTPClient(transport=httpx.MockTransport(route), base_url=api.api_server))
    api.client = client()
    original_init = live.SchemaResources.__init__
    def resources_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.client.close()
        self.client = client()
    monkeypatch.setattr(live.SchemaResources, '__init__', resources_init)
    fake.qualify = api.qualify
    fake.resources = api.resources
    try:
        result = schema.run_development_schema_upgrade(retained=schema_retained, api=fake, action='apply')
        assert result['phase'] == 'shared-probe'
        assert prechildren and set(prechildren) == {'shared-probe'}
        child = schema._child(schema_retained, 'shared-probe')
        assert child.is_file()
        api.frame = None
        assert api._current_record() == schema._record(schema_retained)
        saved = child.read_bytes()
        child.unlink()
        try:
            with pytest.raises(ValueError, match='child_missing'):
                api._current_record()
        finally:
            from scripts.ops import nebius_certificates as state
            state._write_private(child, saved)
    finally:
        api.children.close()
        clients.close()
    assert any(call.method == 'POST' and not call.url.query for call in calls)


@pytest.mark.parametrize('kind', ['Job', 'Pod', 'Deployment', 'HorizontalPodAutoscaler'])
def test_complete_namespace_census_rejects_unaccounted_writers(kind):
    live = module()
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    api.retained = SimpleNamespace(projection=SimpleNamespace(participant=SimpleNamespace(
        execution_namespace=SimpleNamespace(name='execution'), build_namespace=SimpleNamespace(name='build'))))
    api.manager = SimpleNamespace(binding=SimpleNamespace(namespace='loom-nebius-management-dev'))
    def collection(namespace, version, selected, resource):
        rows = []
        if namespace == 'loom-dev' and selected == kind:
            rows.append({'apiVersion': version, 'kind': kind, 'metadata': {
                'namespace': namespace, 'name': 'foreign-writer', 'uid': str(uuid4())}, 'spec': {}})
        return {'items': rows}
    api._collection = collection
    with pytest.raises(ValueError, match='unaccounted'):
        api._census({}, None)


def test_owned_pod_still_rejects_changed_code_and_privilege():
    live = module()
    wanted = {'containers': [{'name': 'service', 'image': 'service@sha256:' + 'a' * 64,
        'securityContext': {'allowPrivilegeEscalation': False}}], 'automountServiceAccountToken': False}
    assert live.HTTPSDevelopmentSchemaAPI._template(wanted, wanted, pod=True)
    for change in ('sidecar', 'command', 'security'):
        actual = copy.deepcopy(wanted)
        if change == 'sidecar':
            actual['containers'].append({'name': 'extra-writer', 'image': 'arbitrary'})
        elif change == 'command':
            actual['containers'][0]['command'] = ['sh', '-c', 'untracked']
        else:
            actual['containers'][0]['securityContext']['privileged'] = True
        assert not live.HTTPSDevelopmentSchemaAPI._template(actual, wanted, pod=True)


def test_completed_replay_rechecks_current_readiness():
    live = module()
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    record = {'phases': {'verify': {'status': 'complete'}}}
    retained = object()
    api.retained = retained
    api.external_checked, api.external_frame = True, copy.deepcopy(record)
    api._private_inputs = lambda: None
    api._current_record = lambda: copy.deepcopy(record)
    api._observe = lambda current: {}
    calls = []
    api.verify = lambda current: calls.append(copy.deepcopy(current)) or False
    with pytest.raises(ValueError, match='completed_successor_not_ready'):
        api.qualify(retained, record)
    assert calls == [record]


def test_final_verification_rejects_fresh_database_drift_despite_completed_job(monkeypatch):
    live = module()
    from scripts.ops import nebius_development_schema_upgrade as schema
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    record = {'phases': {phase: {'status': 'complete', 'proof': {'historical': 'qualified'}} for phase in schema.PHASES}}
    api.retained = SimpleNamespace(plan=SimpleNamespace(targets={}), projection=SimpleNamespace(catalog_request={}))
    api._current_record = lambda: record
    api._observe = lambda current: {}
    api.report = lambda phase, current: current['phases'][phase]['proof']
    api.catalog_observe = lambda: {}
    async def external(current):
        pass
    api._external = external
    monkeypatch.setattr(live, '_PolicyProof', lambda *args: SimpleNamespace(policy_ready=lambda: True))
    monkeypatch.setattr(live, 'qualify_closed_development_catalog_report', lambda *args, **kwargs: 'complete')
    calls = []
    def fresh(phase, current):
        calls.append(phase)
        raise ValueError('fresh_database_grants_changed')
    api._fresh_database = fresh
    with pytest.raises(ValueError, match='fresh_database_grants_changed'):
        api.verify(record)
    assert calls == ['shared-inspect']


def test_started_backup_reserves_capacity_until_its_uploaded_object_is_qualified(tmp_path, monkeypatch):
    live = module()
    api = object.__new__(live.HTTPSDevelopmentSchemaAPI)
    api.request = SimpleNamespace(database=SimpleNamespace(foundation=SimpleNamespace(inputs=SimpleNamespace(
        config={'postgres_storage_gi': 12}))))
    api.retained = SimpleNamespace(projection=SimpleNamespace(before_manager=SimpleNamespace(postgres_storage_gi=2)))
    record = {'phases': {'shared-backup': {'status': 'started'}, 'manager-backup': {'status': 'complete'}}}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {
        'name': 'checkpoint', 'namespace': 'loom-dev', 'uid': str(uuid4())}, 'spec': {'template': {'spec': {}}}}
    path = tmp_path / 'stage.json'
    path.write_text('{}')
    monkeypatch.setattr(live.schema, '_child', lambda retained, phase: path)
    monkeypatch.setattr(live.schema, '_staged_resources', lambda *args, **kwargs: {'job': job})
    api._get = lambda selected: copy.deepcopy(job)
    api.report = lambda *args: pytest.fail('unfinished upload cannot release reserved capacity')
    assert api._remaining_backup_bytes(record) == 12 * 1024**3
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}]}
    api.report = lambda *args: None
    with pytest.raises(ValueError, match='upload_unconfirmed'):
        api._remaining_backup_bytes(record)
    api.report = lambda *args: {'job_uid': job['metadata']['uid'], 'key': 'qualified checkpoint'}
    assert api._remaining_backup_bytes(record) == 1


@pytest.mark.parametrize('changed', [False, True])
def test_connected_transport_binds_captured_operator_files_before_yield(tmp_path, monkeypatch, changed):
    from scripts.ops import nebius_certificates as state
    live = module()
    ca, credentials = tmp_path / 'ca.pem', tmp_path / 'operator.json'
    files = {ca: b'trust', credentials: b'explicit credentials'}
    for path, raw in files.items():
        state._write_private(path, raw)
    connection = SimpleNamespace(ca_file=ca, credentials_file=credentials, endpoint='https://explicit.example')
    retained = SimpleNamespace(inputs=SimpleNamespace(connection=connection), files=files)
    context = ssl.create_default_context()
    calls = []
    async def transport(selected):
        assert selected is connection
        calls.append('resolve')
        if changed:
            credentials.write_bytes(b'changed credentials')
        return context, 'explicit-token'
    def construct(**kwargs):
        assert kwargs == dict(retained=retained, ssl_context=context, token='explicit-token', unchanged=unchanged)
        calls.append('construct')
        return nullcontext('connected')
    def unchanged():
        calls.append('unchanged')
    monkeypatch.setattr(live, '_transport', transport)
    monkeypatch.setattr(live, 'HTTPSDevelopmentSchemaAPI', construct)
    if changed:
        with pytest.raises(ValueError, match='history_changed'):
            with live.connected_schema_api(retained, unchanged=unchanged):
                pytest.fail('changed credentials cannot yield a connected adapter')
        assert 'construct' not in calls
    else:
        with live.connected_schema_api(retained, unchanged=unchanged) as api:
            assert api == 'connected'
        assert calls.index('unchanged') < calls.index('resolve') < calls.index('construct')
