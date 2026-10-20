"""Shared handoff parent lifecycle regressions."""
from __future__ import annotations

from tests.ops.test_nebius_shared_pool_handoff import (
    ROOT as ROOT,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    Path as Path,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    SharedPhaseAPI as SharedPhaseAPI,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _key as _key,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_material as application_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    cloud as cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    connected as connected,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    copy as copy,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    dev_pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    entry as entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    handoff as handoff,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    hashlib as hashlib,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    installation as installation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    inventory as inventory,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    live as live,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    material as material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    preflight as preflight,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publication as publication,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    published_source as published_source,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytest as pytest,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytestmark as pytestmark,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    retained as retained,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    route as route,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_guard_transport as shared_guard_transport,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_operation as shared_operation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_runtime as shared_runtime,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_history as staging_history,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_observation as staging_observation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    uuid4 as uuid4,
)


def test_restored_collector_uses_completed_current_lineage_and_bound_publication(shared_operation, tmp_path, monkeypatch):
    """Real source attachment/lineage; only Kubernetes and the separately tested SQL read are doubled."""
    import importlib.util
    import json

    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER

    name = 'scripts.ops.nebius_shared_pool_recovery_collector'
    assert importlib.util.find_spec(name) is not None, 'restored collector publication has no completed-job consumer'
    module = __import__(name, fromlist=['observe_restored_staging_collector'])
    _, bindings = shared_operation
    attachment = bindings.plan.staging.attachment
    collector, = (copy.deepcopy(row) for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')
    controller = copy.deepcopy(attachment.guard.controller)
    for row in (collector, controller):
        row['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
    namespace = collector['metadata']['namespace']
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': 'collector-100', 'namespace': namespace,
        'uid': str(uuid4()), 'resourceVersion': '401', 'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'CronJob',
            'name': collector['metadata']['name'], 'uid': collector['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(collector['spec']['jobTemplate']['spec']),
        'status': {'succeeded': 1, 'completionTime': '2026-10-10T20:00:00Z', 'conditions': [{'type': 'Complete', 'status': 'True'}]}}
    generated = {prefix + key: value for prefix in ('', 'batch.kubernetes.io/') for key, value in (
        ('controller-uid', job['metadata']['uid']), ('job-name', job['metadata']['name']))}
    job['spec'].update(selector={'matchLabels': {'batch.kubernetes.io/controller-uid': job['metadata']['uid']}},
        completions=1, parallelism=1, completionMode='NonIndexed', manualSelector=False,
        podReplacementPolicy='TerminatingOrFailed', suspend=False)
    job['spec']['template']['metadata']['labels'].update(generated)
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'collector-100-abcde', 'namespace': namespace,
        'uid': str(uuid4()), 'resourceVersion': '402', **copy.deepcopy(job['spec']['template']['metadata']),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
            'uid': job['metadata']['uid'], 'controller': True}]}, 'spec': copy.deepcopy(job['spec']['template']['spec']),
        'status': {'phase': 'Succeeded'}}
    pod['spec']['nodeName'] = 'qualified-platform-node'
    pod['metadata']['labels']['topology.kubernetes.io/region'] = attachment.config['region']
    pod['spec'].setdefault('tolerations', []).extend({'key': 'node.kubernetes.io/' + key,
        'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300} for key in ('not-ready', 'unreachable'))
    for field, statuses in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
        pod['status'][statuses] = [{'name': row['name'], 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}
            for row in pod['spec'].get(field, [])]
    documents = {_key(row): copy.deepcopy(row) for row in attachment.resources}
    documents[_key(collector)] = collector
    for volume in collector['spec']['jobTemplate']['spec']['template']['spec']['volumes']:
        for source in volume.get('projected', {}).get('sources', []):
            if 'secret' in source:
                ref = source['secret']
                documents['Secret:' + namespace + ':' + ref['name']] = {'apiVersion': 'v1', 'kind': 'Secret',
                    'metadata': {'name': ref['name'], 'namespace': namespace, 'uid': str(uuid4()), 'resourceVersion': '403'},
                    'data': {item['key']: 'cHJpdmF0ZS10ZXN0' for item in ref['items']}}
    listing = {'jobs': {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '404'}, 'items': [job]},
        'pods': {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '405'}, 'items': [pod]}}
    receipt = {'id': str(uuid4()), 'created': True, 'target_id': attachment.config['target_id'],
        'source': 'nebius-kubernetes-capacity-collector', 'source_version': 'current-provider-kubernetes-digest',
        'observed_at': '2026-10-10T20:00:00Z', 'provider_capacity_state': 'insufficient',
        'autoscaler_state': 'ready', 'observation_sha256': 'sha256:' + 'a' * 64}
    kubeconfig = tmp_path / 'collector-kubeconfig'
    kubeconfig.write_text('private-test-config')
    kubeconfig.chmod(0o600)
    api = KubectlPoolGuardAPI(request=bindings.migration, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
    mode, calls, publications = {'case': None}, [], []

    def get(kind, name, selected=None):
        calls.append(('get', kind, name, selected))
        if kind == 'namespace' and name == 'kube-system':
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': bindings.migration.registration.binding.kube_system_uid}}
        wanted_kind = {'configmap': 'ConfigMap', 'secret': 'Secret', 'cronjob': 'CronJob',
            'deployment': 'Deployment', 'namespace': 'Namespace'}[kind]
        return copy.deepcopy(documents[wanted_kind + ':' + (selected or '-') + ':' + name])

    def run(args):
        calls.append(tuple(args))
        if args[0] == 'logs':
            assert args == ['logs', '-n', namespace, 'pod/' + pod['metadata']['name'], '-c', 'collector',
                '--limit-bytes=16384', '--timestamps=false']
            if mode['case'] == 'late_secret':
                next(row for row in documents.values() if row['kind'] == 'Secret')['metadata']['resourceVersion'] = 'changed'
            if mode['case'] == 'late_binding':
                attachment.config['region'] = 'changed'
            return copy.deepcopy(receipt)
        assert args[:2] == ['get', '--raw']
        kind = 'jobs' if args[2] == '/apis/batch/v1/namespaces/' + namespace + '/jobs?limit=1000' else 'pods'
        assert args[2] == ('/apis/batch/v1' if kind == 'jobs' else '/api/v1') + '/namespaces/' + namespace + '/' + kind + '?limit=1000'
        return copy.deepcopy(listing[kind])

    def publication(target, *, expected, publication):
        from scripts.ops.nebius_management_switch import _stable

        assert target == attachment.guard and _stable(expected) == _stable(controller)
        assert publication['receipt'] == receipt
        assert publication['source'] == 'nebius-kubernetes-capacity-collector'
        assert publication['node_group_id'] == bindings.migration.registration.spec.node_group_id
        assert publication['quota_identities'] == {key: list(value) for key, value in bindings.migration.registration.spec.quota_identities.items()}
        assert {row['target_id'] for row in publication['targets']} == {row.target_id for row in attachment.participant.targets}
        publications.append(copy.deepcopy(publication))
        if mode['case'] == 'late_pod':
            listing['pods']['items'][0]['status']['containerStatuses'][0]['restartCount'] = 1
        return mode['case'] != 'stale'

    monkeypatch.setattr(api, '_get', get)
    monkeypatch.setattr(api, '_run', run)
    monkeypatch.setattr(api, 'observe_collector_publication', publication)

    def observe():
        try:
            return module.observe_restored_staging_collector(bindings=bindings, guards=api, expected=collector, controller=controller)
        except ValueError as error:
            import traceback

            frames, cause = [], error
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('restored staging collector unqualified: ' + ', '.join(frames)) from None

    assert observe() is True
    assert len(publications) == 1
    baseline = copy.deepcopy((documents, listing, attachment.config))
    for case in ('missing', 'running', 'failed', 'old_template', 'stale', 'foreign_job', 'foreign_pod', 'duplicate',
            'truncated', 'pod_image', 'region', 'sidecar', 'config', 'missing_secret_key', 'empty_secret_key',
            'late_secret', 'late_pod', 'late_binding', 'suspended'):
        documents.clear()
        documents.update(copy.deepcopy(baseline[0]))
        listing.clear()
        listing.update(copy.deepcopy(baseline[1]))
        attachment.config.clear()
        attachment.config.update(copy.deepcopy(baseline[2]))
        mode['case'] = case
        if case == 'missing':
            listing['jobs']['items'], listing['pods']['items'] = [], []
        elif case in {'running', 'failed'}:
            listing['jobs']['items'][0]['status'] = {'active': 1} if case == 'running' else {
                'failed': 1, 'conditions': [{'type': 'Failed', 'status': 'True'}]}
        elif case == 'old_template':
            listing['jobs']['items'][0]['spec']['template']['spec']['containers'][0]['image'] = 'old/image'
        elif case in {'foreign_job', 'foreign_pod'}:
            listing['jobs' if case == 'foreign_job' else 'pods']['items'][0]['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
        elif case == 'duplicate':
            listing['pods']['items'].append(copy.deepcopy(listing['pods']['items'][0]))
        elif case == 'truncated':
            listing['pods']['metadata']['continue'] = 'more'
        elif case == 'pod_image':
            listing['pods']['items'][0]['spec']['containers'][0]['image'] = 'other/image'
        elif case == 'region':
            listing['pods']['items'][0]['metadata']['labels']['topology.kubernetes.io/region'] = 'wrong-region'
        elif case == 'sidecar':
            listing['pods']['items'][0]['spec']['containers'].append({'name': 'foreign', 'image': 'other/image'})
        elif case == 'config':
            documents['ConfigMap:' + namespace + ':loom-execution-capacity-collector']['data']['EXTRA_OVERRIDE'] = 'changed'
        elif case in {'missing_secret_key', 'empty_secret_key'}:
            secret = next(row for row in documents.values() if row['kind'] == 'Secret')
            secret['data'] = ({'unrelated': 'dW5yZWxhdGVk'} if case == 'missing_secret_key'
                else dict.fromkeys(secret['data'], ''))
        elif case == 'suspended':
            documents[_key(collector)]['spec']['suspend'] = True
        before = len(publications)
        if case in {'missing', 'running', 'failed', 'old_template', 'stale'}:
            assert observe() is False, case
        else:
            with pytest.raises(ValueError, match='restored staging collector unqualified'):
                observe()
        if case not in {'stale', 'late_pod', 'late_secret', 'late_binding'}:
            assert len(publications) == before, case
    assert all(row[0] in {'get', 'logs'} for row in calls)
    assert 'private-' not in json.dumps(publications)

    # The actual recovery callback connects collector health and process/SQL
    # challenges without supplying its own stop, restart or reopen authority.
    from scripts.ops.nebius_ingress_stage import _snapshot

    assert hasattr(module, 'SharedRecoveryRuntimeReadiness'), 'restored health is not connected to the restart callback'
    documents.clear()
    documents.update(copy.deepcopy(baseline[0]))
    listing.clear()
    listing.update(copy.deepcopy(baseline[1]))
    attachment.config.clear()
    attachment.config.update(copy.deepcopy(baseline[2]))
    mode['case'] = None
    controller_pod = {'metadata': {'uid': str(uuid4())}, 'status': {'containerStatuses': [{'restartCount': 0}]}}
    database_pod = {'metadata': {'uid': str(uuid4())}, 'status': {'containerStatuses': [{'restartCount': 0}]}}
    monkeypatch.setattr(api, '_database', lambda *_: copy.deepcopy(database_pod))
    monkeypatch.setattr(api, '_runtime', lambda *_, **kwargs: copy.deepcopy(controller_pod))
    readiness = module.SharedRecoveryRuntimeReadiness(bindings=bindings, guards=api)

    def callback(captured):
        desired = _snapshot(captured)
        desired['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        before = copy.deepcopy(desired)
        before['metadata']['uid'] = captured['metadata']['uid']
        before['spec']['suspend' if captured['kind'] == 'CronJob' else 'replicas'] = True if captured['kind'] == 'CronJob' else 0
        current = copy.deepcopy(desired)
        current['metadata']['uid'] = captured['metadata']['uid']
        return readiness(bindings.plan.development.runtime.request, 'recovery-restart', before, desired, current)

    assert callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')) is True
    mode['case'] = 'stale'
    assert callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')) is False
    original_publication = api.observe_collector_publication
    def late_database(*args, **kwargs):
        result = original_publication(*args, **kwargs)
        database_pod['status']['containerStatuses'][0]['restartCount'] += 1
        return result
    monkeypatch.setattr(api, 'observe_collector_publication', late_database)
    for pending in (None, 'stale'):
        mode['case'] = pending
        with pytest.raises(ValueError, match='restored staging runtime unqualified'):
            callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob'))
    monkeypatch.setattr(api, 'observe_collector_publication', original_publication)
    probes = []
    def probe(name, target, *, original, expected, **kwargs):
        assert target == attachment.guard and original == bindings.plan.staging.originals[_key(original)]
        assert expected['spec'] == original['spec']
        if name == mode['case']:
            raise ValueError('test-only failed process probe')
        probes.append((name, _key(original)))
    for method in ('database', 'legacy_settings', 'telemetry'):
        monkeypatch.setattr(api, 'qualify_runtime_' + method, lambda *args, method=method, **kwargs: probe(method, *args, **kwargs))
    for captured in bindings.plan.staging.originals.values():
        if captured['kind'] != 'Deployment' or captured['spec']['replicas'] == 0:
            continue
        mode['case'], probes[:] = None, []
        assert callback(captured) is True
        names = ['database', 'legacy_settings']
        if captured['metadata']['namespace'] == namespace:
            names.append('telemetry')
        assert probes == [(name, _key(captured)) for name in names]
        for name in names:
            mode['case'] = name
            with pytest.raises(ValueError, match='restored staging runtime unqualified'):
                callback(captured)


@pytest.mark.timeout(420)
def test_connected_shared_parent_gates_real_guard_acquisition_and_unknown_replies(shared_operation, shared_guard_transport,
        tmp_path, monkeypatch):
    """Concrete parent and real child/SQL binding; separately covered observation adapters are boundaries here."""
    import importlib.util
    import ssl
    from types import SimpleNamespace

    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_api'
    assert importlib.util.find_spec(name) is not None, 'concrete shared-pool parent is missing'
    module = __import__(name, fromlist=['HTTPSSharedPoolOperationAPI'])
    args, bindings = shared_operation
    guard, model = shared_guard_transport
    operator = tmp_path / 'operator-connection'
    operator.write_bytes(b'private-test-operator')
    operator.chmod(0o600)
    failures, seen = set(), set()

    def checked(gate, **options):
        seen.add(gate)
        if gate in failures:
            raise ValueError('simulated ' + gate + ' rejection')
        if 'bindings' in options:
            assert options['bindings'] == bindings
        if gate == 'writers':
            return module.SharedPoolWriterObservation(bindings.plan, '501', {}, {}, bindings.input_digest)
        return SimpleNamespace(plan=bindings.plan, bindings=bindings, pending_workloads=(),
            pool_state='closed', guard_states={target.namespace: 'held' if target.namespace in model.held else 'open'
                for target in bindings.migration.guards}, input_digest=bindings.input_digest, authority={})

    async def publication(**options):
        assert options['request'] == args['runtime']
        checked('publication')

    async def staging_publication(**options):
        assert options['request'] == args['runtime'] and options['staging'] == bindings.plan.staging
        checked('staging-publication')

    async def provider(**options):
        assert options['operator_credentials'] == operator and options['runtime'] == args['runtime']
        return checked('provider', **options)

    monkeypatch.setattr(module, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(module, 'qualify_staging_publication', staging_publication, raising=False)
    monkeypatch.setattr(module, 'observe_shared_pool_provider', provider)
    monkeypatch.setattr(module, 'observe_shared_pool_writers', lambda **options: checked('writers', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_writers', lambda **options: checked('writers', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_processes', lambda **options: checked('processes', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_processes', lambda **options: checked('processes', **options))
    # Check absence through the real parent's HTTP reader, without a live API.
    import httpx

    def handle(request):
        assert request.method == 'GET'
        return httpx.Response(404)

    context = ssl.create_default_context(cadata=args['runtime'].database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    kwargs = dict(bindings=bindings, observation=args['observation'], repo_root=ROOT,
        api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=context, token='operator-test', operator_credentials=operator,
        private_files={operator: operator.read_bytes()})

    def connect():
        connection = module.HTTPSSharedPoolOperationAPI(**kwargs)
        connection.client.close()
        connection.client = type(connection.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
        # Teach the external kubectl fixture the parent's freshly generated
        # private connection. Keep the actual parent-owned guard adapter intact.
        guard.prefix = list(connection.guards.prefix)
        return connection

    def run(api):
        try:
            return parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('connected parent failure at ' + ', '.join(frames)) from None

    with connect() as api:
        model.busy = bindings.migration.guards[0].namespace
        for failed in ('publication', 'staging-publication', 'provider', 'writers', 'processes'):
            failures.add(failed)
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
            failures.clear()
            assert not model.acquires and not model.held
        result = run(api)
        assert result['status'] == 'pending' and result['phase'] == 'forward-guards'
        assert seen == {'publication', 'staging-publication', 'provider', 'writers', 'processes'}
        assert model.acquires == [bindings.migration.guards[0].namespace]
        manager = args['runtime'].database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        # Intent with no response must not be redispatched by this parent.
        model.busy, model.uncertain = None, bindings.migration.guards[0].namespace
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        attempts = list(model.acquires)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert model.acquires == attempts
        for changed_phase in ('forward-drain', 'recovery-fence', '../forward-guards'):
            with pytest.raises(ValueError):
                api.advance(changed_phase, bindings, state / changed_phase)
        assert model.acquires == attempts
    # Reconstructing the concrete transport keeps the uncertain intent intact.
    with connect() as api:
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert model.acquires == attempts


# The real retained-installation fixture takes ~27s before this connected
# completion/replay matrix runs. Give it a bounded lifecycle budget rather
# than the 60s unit-test default (CI run 38167845683 exhausted that total).
@pytest.mark.timeout(120)
def test_connected_shared_parent_forward_completion_requires_current_health(shared_operation, tmp_path, monkeypatch):
    """Real final child and parent; preceding effects and component observation boundaries are fixtures."""
    import ssl
    from types import SimpleNamespace

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_api as module
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    args, bindings = shared_operation
    assert hasattr(module, 'SharedPoolPhaseEvidence'), 'completion validation still depends on live operator authority'
    historical = module.SharedPoolPhaseEvidence(bindings=bindings)
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    predecessor = SharedPhaseAPI(parent)
    predecessor.prepare_failure = ('forward-activate', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**args, bindings=bindings, api=predecessor, action='install')
    # Represent the already tested opening's completed remote effect, keeping
    # its actual semantic receipt and parent checksum validation in this test.
    activation = state / 'forward-activate'
    module.activation.prepare_shared_pool_activation(bindings=bindings, state_dir=activation)
    opened = _read(activation / 'activation.json')
    opened['opening'] = 'opened'
    for row in opened['guards'].values():
        row['release'] = 'released'
    private_state._atomic_json(activation / 'activation.json', opened)
    frame = _read(state / 'operation.json')
    frame['forward']['forward-activate'].update(status='complete', files=parent._files(activation))
    private_state._atomic_json(state / 'operation.json', frame)
    operator = tmp_path / 'forward-operator'
    operator.write_bytes(b'private-forward-test')
    operator.chmod(0o600)
    mode = {'damage': None}

    async def publication(**options):
        assert options['request'] == args['runtime']

    async def provider(**options):
        assert options['runtime'] == args['runtime']
        return SimpleNamespace(plan=bindings.plan)

    def processes(**options):
        assert options['bindings'] == bindings and options['state_dir'] == state
        if mode['damage'] == 'unhealthy':
            raise ValueError('simulated failed current process challenge')
        if mode['damage'] == 'receipt':
            path = state / 'forward-verify/verify.json'
            path.write_bytes(path.read_bytes() + b'\n')
        return module.SharedPoolForwardProcessObservation(bindings,
            'closed' if mode['damage'] == 'closed' else 'global',
            {target.namespace: 'held' if mode['damage'] == 'held' else 'open' for target in bindings.migration.guards},
            'revoked', ('Deployment:loom-dev:loom-service',) if mode['damage'] == 'pending' else (), {}, bindings.input_digest)

    monkeypatch.setattr(module, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(module, 'observe_shared_pool_provider', provider)
    monkeypatch.setattr(module, 'observe_shared_pool_forward_writers', lambda **_: module.SharedPoolWriterObservation(
        bindings.plan, '501', {}, {}, bindings.input_digest))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_processes', processes)

    context = ssl.create_default_context(cadata=args['runtime'].database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    with module.HTTPSSharedPoolOperationAPI(bindings=bindings, observation=args['observation'], repo_root=ROOT,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
            token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}) as api:
        registry = api._registry()
        for phase in parent.FORWARD_PHASES[:-2]:
            def validate(*, state_dir, complete, bindings, prepared=False):
                if prepared:
                    predecessor.validate_prepared_phase(state_dir.name, state_dir)
                else:
                    predecessor.validate_phase(state_dir.name, state_dir, complete=complete)
            registry[phase] = ((lambda **_: pytest.fail('completed predecessor must not be prepared again'), validate),)
        monkeypatch.setattr(api, '_registry', lambda: registry)
        # Effective permissions are exercised with real rule reviews in the
        # connected recovery test; this test owns final phase/health composition.
        monkeypatch.setattr(api, '_permissions', lambda *_, **__: None)
        child = state / 'forward-verify'
        api.prepare('forward-verify', bindings, child)
        api.prepare('forward-verify', bindings, child)  # Interrupted local preparation is resumable.
        assert _read(child / 'verify.json')['status'] == 'prepared'
        frame['forward']['forward-verify']['status'] = 'started'
        private_state._atomic_json(state / 'operation.json', frame)
        with pytest.raises(ValueError):
            api.validate_prepared_phase('forward-verify', child)  # Started authority cannot masquerade as unstarted.
        raw = (child / 'verify.json').read_bytes()
        for damage in ('pending', 'closed', 'held', 'unhealthy', 'receipt'):
            mode['damage'] = damage
            with pytest.raises(ValueError):
                api.advance('forward-verify', bindings, child)
            assert _read(child / 'verify.json')['status'] == 'prepared'
            (child / 'verify.json').write_bytes(raw)
        mode['damage'] = None
        assert api.advance('forward-verify', bindings, child)
        assert _read(child / 'verify.json')['status'] == 'observed'
        historical.validate_phase('forward-verify', child, complete=True)
        final_raw = (child / 'verify.json').read_bytes()
        private_state._atomic_json(child / 'verify.json', {**_read(child / 'verify.json'), 'status': 'prepared'})
        with pytest.raises(ValueError):
            historical.validate_phase('forward-verify', child, complete=True)
        (child / 'verify.json').write_bytes(final_raw)
        with pytest.raises(ValueError):
            historical.validate_phase('forward-verify', state / 'foreign', complete=True)
        result = parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert result['status'] == 'shared_pool_handoff_complete'
        assert result['admission_open'] and result['writer_migration_complete']
        retained = {path: path.read_bytes() for path in state.rglob('*') if path.is_file()}
        assert parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='preflight')['admission_open']
        mode['damage'] = 'unhealthy'
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='preflight')
        for phase in api.supported_phases:
            with pytest.raises(ValueError):
                api.advance(phase, bindings, state / phase)
        assert all(path.read_bytes() == value for path, value in retained.items())


@pytest.mark.timeout(420)
def test_shared_guard_child_retains_busy_and_unknown_acquisitions_without_registering_again(shared_operation, shared_guard_transport):
    import importlib.util

    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    name = 'scripts.ops.nebius_shared_pool_guards'
    assert importlib.util.find_spec(name) is not None, 'concrete shared guard child is missing'
    module = __import__(name, fromlist=['advance_shared_pool_guards'])
    args, bindings = shared_operation
    guards, model = shared_guard_transport

    class ConnectedGuardAPI(SharedPhaseAPI):
        guard_qualification_hook = None

        def qualify(self, bound, record, *, recovering):
            super().qualify(bound, record, recovering=recovering)
            if self.guard_qualification_hook is not None:
                self.guard_qualification_hook()

        def prepare(self, phase, bound, state):
            if phase == 'forward-guards':
                module.prepare_shared_pool_guards(bindings=bound, state_dir=state)
            else:
                super().prepare(phase, bound, state)

        def validate_prepared_phase(self, phase, state):
            if phase == 'forward-guards':
                record = module.validate_shared_pool_guards(bindings=bindings, state_dir=state, complete=False)
                assert set(record['guards'].values()) == {'prepared'}
            else:
                super().validate_prepared_phase(phase, state)

        def validate_phase(self, phase, state, *, complete):
            if phase == 'forward-guards':
                module.validate_shared_pool_guards(bindings=bindings, state_dir=state, complete=complete)
            else:
                super().validate_phase(phase, state, complete=complete)

        def advance(self, phase, bound, state):
            if phase == 'forward-guards':
                return module.advance_shared_pool_guards(bindings=bound, api=guards, state_dir=state,
                    qualify=lambda: self.qualify(bound, _read(state.parent / 'operation.json'), recovering=False))
            return super().advance(phase, bound, state)

    api = ConnectedGuardAPI(parent)
    api.block = 'forward-drain'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    first, second = (target.namespace for target in bindings.migration.guards)
    model.busy = second
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    assert model.held == {first} and model.acquires == [first, second]
    assert api.events == []
    # Disappearing evidence during a pre-dispatch qualification must not be
    # recreated by a later save. This is a separate controlled fault scenario.
    manager = args['runtime'].database.manager.retained.request.retained
    child = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff/forward-guards/guards.json'
    checkpoint = child.read_bytes()
    second_id = str(bindings.migration.guards[1].participant_id)
    def lose_guard_receipt():
        if child.exists() and _read(child)['guards'][second_id] == 'acquire_intent':
            api.guard_qualification_hook = None
            child.unlink()  # Only this test's temporary child journal.
    api.guard_qualification_hook = lose_guard_receipt
    model.busy = None
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.acquires == [first, second] and model.held == {first}
    assert not child.exists()
    # Restore the controlled fixture for the independent unknown-reply scenario;
    # production never rewinds an acquisition intent this way.
    child.write_bytes(checkpoint)
    child.chmod(0o600)
    model.busy, model.uncertain = None, second
    for _ in range(2):
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
    assert model.acquires == [first, second, second]
    assert api.events == []
    # A late committed acquisition is observed, not dispatched again.
    model.held.add(second)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-drain'
    assert model.acquires == [first, second, second]
    before = list(model.acquires)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-drain'
    assert model.acquires == before


@pytest.mark.timeout(420)
@pytest.mark.parametrize('direction', ['forward', 'recovery'])
def test_shared_parent_resumes_before_and_after_each_local_child_preparation(shared_operation, direction):
    import json

    from scripts.ops import nebius_shared_pool_operation as module

    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    phases, final = module.FORWARD_PHASES, 'shared_pool_handoff_complete'
    if direction == 'recovery':
        api.block = 'forward-wire'
        assert module.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        kwargs['action'] = 'recover'
        phases, final = module.RECOVERY_PHASES, 'staging_restored_dev_closed'
    prefix = list(api.events)
    for index, phase in enumerate(phases):
        for boundary in ('before', 'directory', 'after'):
            api.prepare_failure = (phase, boundary)
            with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
                module.run_shared_pool_operation(**kwargs)
            record = json.loads((state / 'operation.json').read_bytes())
            assert record[direction][phase]['status'] == 'prepared'
            assert (state / phase / 'receipt.json').exists() is (boundary == 'after')
            assert api.events == prefix + list(phases[:index])
    api.prepare_failure = None
    assert module.run_shared_pool_operation(**kwargs)['status'] == final
    assert api.events == prefix + list(phases)


@pytest.mark.timeout(420)
def test_shared_parent_anchors_all_phases_and_never_replays_completed_effects(shared_operation):
    import importlib.util
    import json

    name = 'scripts.ops.nebius_shared_pool_operation'
    assert importlib.util.find_spec(name) is not None, 'shared handoff phase controller is missing'
    module = __import__(name, fromlist=['run_shared_pool_operation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    marker = Path(manager.operation['anchor_dir']) / 'shared-pool-handoff.json'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    before = {path: path.read_bytes() for path in bindings.plan.history}
    assert module.run_shared_pool_operation(**kwargs, action='preflight')['status'] == 'shared_pool_preflight_qualified'
    assert api.events == [] and not state.exists() and not marker.exists()
    phases = api.supported_phases
    api.supported_phases -= {'recovery-restore-workloads'}
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    assert api.events == [] and not state.exists() and not marker.exists()
    api.supported_phases = phases
    api.uncertain = 'forward-wire'
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    journal = json.loads((state / 'operation.json').read_bytes())
    assert journal['forward']['forward-wire']['status'] == 'started'
    assert journal['forward']['forward-start']['status'] == 'prepared'
    assert marker.exists()
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'shared_pool_handoff_complete'
    assert api.events == [
        'forward-guards', 'forward-drain', 'forward-stop', 'forward-roles', 'forward-database',
        'forward-material', 'forward-wire', 'forward-start', 'forward-build-policy',
        'forward-gateway', 'forward-activate', 'forward-verify',
    ]
    writes = list(api.events)
    preflights = api.preflights
    assert module.run_shared_pool_operation(**kwargs, action='install')['admission_open'] is True
    qualified = module.run_shared_pool_operation(**kwargs, action='preflight')
    assert qualified['status'] == 'shared_pool_preflight_qualified'
    assert qualified['admission_open'] is True and qualified['writer_migration_complete'] is True
    assert api.events == writes and api.preflights == preflights
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='recover')
    child = state / 'forward-wire/receipt.json'
    raw = child.read_bytes()
    try:
        child.write_bytes(b'{}')
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            module.run_shared_pool_operation(**kwargs, action='install')
        assert api.events == writes
    finally:
        child.write_bytes(raw)
        child.chmod(0o600)
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.timeout(420)
def test_normal_release_predecessor_requires_completed_shared_history_without_live_effects(shared_operation):
    import hashlib

    from scripts.ops import nebius_shared_pool_operation as module

    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    anchor = Path(manager.operation['anchor_dir'])
    marker = anchor / 'shared-pool-handoff.json'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    assert not state.exists() and not marker.exists() and not api.events

    api.block = 'forward-wire'
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'pending'
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    api.block = None
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'shared_pool_handoff_complete'
    effects = (list(api.events), list(api.qualifications), api.preflights)
    proof = module.load_completed_shared_pool_operation(**kwargs)
    assert proof.migration == bindings.migration and proof.state_dir == state
    assert bindings.plan.history.items() <= proof.history.items()
    assert proof.history[marker] == hashlib.sha256(marker.read_bytes()).hexdigest()
    assert proof.history[state / 'operation.json'] == hashlib.sha256((state / 'operation.json').read_bytes()).hexdigest()
    for phase in module.FORWARD_PHASES:
        receipt = state / phase / 'receipt.json'
        assert proof.history[receipt] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert module.load_completed_shared_pool_operation(**kwargs) == proof
    assert effects == (api.events, api.qualifications, api.preflights)

    # Consume the actual completed registration in the normal-release renderer.
    # Kubernetes current state is projected from this handoff's real targets;
    # completion history alone is not presented as an observation of live Pods.
    from scripts.ops.nebius_pool_participant_release import prepare_staging_pool_release

    from loom.nebius_platform_render import build_platform

    attachment = bindings.plan.staging.attachment
    previous_files = build_platform(attachment.config, proof.migration.registration.candidate,
        attachment.runtime_profile.model_dump(mode='json'), proof.migration.registration.spec.profiles.image_admission_keyring,
        repo_root=ROOT)
    current = {_key(row): copy.deepcopy(row) for rows in previous_files.values() for row in rows}
    for row in current.values():
        row['metadata'].update(uid=str(uuid4()), resourceVersion='899')
    # Handoff observation covers selected processes/authority, not the complete
    # rollout inventory (notably ServiceAccounts). Preserve its known identities
    # within this synthetic full Kubernetes readback.
    current.update({_key(row): copy.deepcopy(row) for row in attachment.resources})
    for key, desired in {**bindings.plan.staging_targets, **bindings.plan.staging_role_targets}.items():
        observed = current[key]
        current[key] = copy.deepcopy(desired)
        current[key]['metadata'].update(uid=observed['metadata']['uid'], resourceVersion='900')
        if current[key]['kind'] == 'Deployment':
            current[key]['spec']['replicas'] = 1
    candidate = copy.deepcopy(proof.migration.registration.candidate)
    candidate['candidate_sha'] = 'd' * 40
    runtime_profile = attachment.runtime_profile.model_copy(update={'candidate_sha': candidate['candidate_sha']})
    release = prepare_staging_pool_release(previous=proof.migration, current=tuple(current.values()),
        config=attachment.config, candidate=candidate, runtime_profile=runtime_profile, operation_id=uuid4(),
        management_origin='https://' + args['runtime'].database.manager.deployment.public_host, repo_root=ROOT)
    assert release.refresh.before == proof.migration.registration.spec
    assert release.refresh.after.policy_revision == proof.migration.registration.spec.policy_revision + 1
    before_dev, = (row for row in proof.migration.registration.spec.participants if row.environment_class == 'development')
    after_dev, = (row for row in release.refresh.after.participants if row.environment_class == 'development')
    assert after_dev == before_dev

    for path in (marker, state / 'operation.json', state / 'forward-wire' / 'receipt.json'):
        original = path.read_bytes()
        try:
            path.write_bytes(b'{}')
            with pytest.raises(ValueError, match='completed shared pool history unavailable'):
                module.load_completed_shared_pool_operation(**kwargs)
        finally:
            path.write_bytes(original)
    recovery = anchor / 'shared-pool-recovery.json'
    recovery.write_bytes(b'{}')
    recovery.chmod(0o600)
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    assert effects == (api.events, api.qualifications, api.preflights)


@pytest.mark.timeout(420)
def test_shared_parent_recovery_freezes_forward_history_and_is_terminal(shared_operation):
    import importlib.util
    import json

    name = 'scripts.ops.nebius_shared_pool_operation'
    assert importlib.util.find_spec(name) is not None, 'shared handoff recovery controller is missing'
    module = __import__(name, fromlist=['run_shared_pool_operation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    anchor = Path(manager.operation['anchor_dir'])
    api = SharedPhaseAPI(module)
    api.block = 'forward-wire'
    kwargs = {**args, 'bindings': bindings, 'api': api}
    before = {path: path.read_bytes() for path in bindings.plan.history}
    result = module.run_shared_pool_operation(**kwargs, action='install')
    assert result['status'] == 'pending' and result['phase'] == 'forward-wire'
    forward = json.loads((state / 'operation.json').read_bytes())['forward']
    api.uncertain = 'recovery-restore-workloads'
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='recover')
    assert (anchor / 'shared-pool-recovery.json').is_file()
    assert json.loads((state / 'operation.json').read_bytes())['forward'] == forward
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    # Cancelling must freeze the pending child's bytes too, not only the
    # completed-child hashes. This altered receipt is individually well formed.
    pending = state / 'forward-wire/receipt.json'
    raw = pending.read_bytes()
    writes = list(api.events)
    try:
        pending.write_text(json.dumps({'phase': 'forward-wire', 'status': 'applied'}))
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            module.run_shared_pool_operation(**kwargs, action='recover')
        assert api.events == writes
    finally:
        pending.write_bytes(raw)
        pending.chmod(0o600)
    result = module.run_shared_pool_operation(**kwargs, action='recover')
    assert result['status'] == 'staging_restored_dev_closed' and result['admission_open'] is False
    assert result['writer_migration_complete'] is False
    assert [phase for phase in api.events if phase.startswith('recovery-')] == [
        'recovery-fence', 'recovery-drain', 'recovery-stop', 'recovery-revoke',
        'recovery-restore-workloads', 'recovery-restore-roles', 'recovery-restart',
        'recovery-reopen', 'recovery-verify',
    ]
    writes = list(api.events)
    assert module.run_shared_pool_operation(**kwargs, action='recover')['status'] == 'staging_restored_dev_closed'
    assert api.events == writes
    assert json.loads((state / 'operation.json').read_bytes())['forward'] == forward
    assert all(path.read_bytes() == raw for path, raw in before.items())
