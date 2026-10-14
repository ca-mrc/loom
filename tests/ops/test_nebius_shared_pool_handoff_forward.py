"""Shared handoff forward lifecycle regressions."""
from __future__ import annotations

from tests.ops.test_nebius_shared_pool_handoff import (
    ROOT as ROOT,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    UUID as UUID,
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


@pytest.mark.timeout(420)
@pytest.mark.parametrize('connected_parent', [False, True], ids=['adapter', 'parent'])
def test_shared_parent_connects_current_staging_workload_transitions_without_replaying_unknown_patch(
        shared_operation, connected_parent, tmp_path, monkeypatch):
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as concrete
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_workloads'
    assert importlib.util.find_spec(name) is not None, 'shared current-staging workload child is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingWorkloads'])
    args, bindings = shared_operation
    runtime = args['runtime']
    original = copy.deepcopy(bindings.plan.staging.originals)
    rows, documents, writes = copy.deepcopy(original), {}, []
    mode = {'lose_reply': True, 'lose_read': False, 'delete_before_write': True, 'deleted_receipt': None}
    phases = {'forward-stop', 'forward-wire', 'forward-start'}

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            namespace = meta['namespace']
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'),
                    ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + namespace + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    refresh()

    def handle(message):
        path = message.url.path
        assert path in documents, 'workload child must not operate on other resources'
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated first readback loss')
            return httpx.Response(200, json=documents[path])
        assert message.method == 'PATCH'
        current = copy.deepcopy(documents[path])
        key = _key(current)
        changes = json.loads(message.content)
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': original[key]['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': current['spec']},
        ]
        for change in changes:
            target, field = (current['metadata'], change['path'].rsplit('/', 1)[-1]) if change['path'].startswith('/metadata/') else (current, 'spec')
            if change['op'] == 'test':
                assert target[field] == change['value']
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if message.url.params.get('dryRun') == 'All':
            return httpx.Response(200, json=current)
        assert not message.url.query
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        current['metadata']['generation'] += 1
        rows[key] = current
        writes.append(key)
        refresh()
        if mode['lose_reply']:
            mode['lose_reply'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated lost committed PATCH response')
        return httpx.Response(200, json=documents[path])

    with ExitStack() as stack:
        connection = None
        class ConnectedWorkloadsAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, state):
                if phase in phases:
                    if connection is not None:
                        connection.prepare(phase, bound, state)
                    else:
                        module.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=state)
                else:
                    super().prepare(phase, bound, state)

            def validate_prepared_phase(self, phase, state):
                if phase in phases:
                    if connection is not None:
                        connection.validate_prepared_phase(phase, state)
                    else:
                        module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state,
                            complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, state)

            def validate_phase(self, phase, state, *, complete):
                if phase in phases:
                    if connection is not None:
                        connection.validate_phase(phase, state, complete=complete)
                    else:
                        module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state, complete=complete)
                else:
                    super().validate_phase(phase, state, complete=complete)

            def advance(self, phase, bound, state):
                if phase not in phases:
                    return super().advance(phase, bound, state)
                if connection is not None:
                    return connection.advance(phase, bound, state)
                def qualify_runtime(request, child_phase, writing):
                    assert request == runtime and child_phase == phase
                    if writing and mode['delete_before_write']:
                        mode['delete_before_write'] = False
                        path = state / 'transition.json'
                        mode['deleted_receipt'] = path.read_bytes()
                        path.unlink()  # Fault injection in this test's own temporary child history.
                api = stack.enter_context(module.HTTPSSharedStagingWorkloads(bindings=bound, phase=phase,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify_runtime, observe_ready=lambda *_: True))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_staging_workloads(bindings=bound, phase=phase, api=api, state_dir=state)

        api = ConnectedWorkloadsAPI(parent)
        if connected_parent:
            # Keep the actual connected parent, child construction, phase
            # preparation/validation, CAS dispatch and readiness adapter. Only
            # earlier effects and separately exercised external observations
            # are boundaries in this workload-route regression.
            operator = tmp_path / 'workload-operator'
            operator.write_bytes(b'private-workload-test')
            operator.chmod(0o600)
            context = ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
            connection = stack.enter_context(concrete.HTTPSSharedPoolOperationAPI(bindings=bindings,
                observation=args['observation'], repo_root=ROOT,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
                token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}))
            registry = connection._registry()
            for phase in parent.FORWARD_PHASES:
                if phase not in phases:
                    def validate(*, state_dir, complete, bindings, prepared=False):
                        if prepared:
                            api.validate_prepared_phase(state_dir.name, state_dir)
                            return {'guards': {'fixture': 'prepared'}}
                        api.validate_phase(state_dir.name, state_dir, complete=complete)
                    registry[phase] = ((lambda **_: pytest.fail('earlier phase preparation is a fixture boundary'), validate),)
            monkeypatch.setattr(connection, '_registry', lambda: registry)
            monkeypatch.setattr(connection, '_permissions', lambda *_, **__: None)

            async def publication(**options):
                assert options['request'] == runtime

            async def provider(**options):
                assert options['runtime'] == runtime
                return SimpleNamespace(plan=bindings.plan)

            def processes(**options):
                assert options['bindings'] == bindings and options['state_dir'] == connection.shared_state
                return concrete.SharedPoolForwardProcessObservation(bindings, 'closed', {}, 'revoked', (), {}, bindings.input_digest)

            monkeypatch.setattr(concrete, 'qualify_runtime_publication', publication)
            monkeypatch.setattr(concrete, 'observe_shared_pool_provider', provider)
            monkeypatch.setattr(concrete, 'observe_shared_pool_forward_writers', lambda **_: concrete.SharedPoolWriterObservation(
                bindings.plan, '501', {}, {}, bindings.input_digest))
            monkeypatch.setattr(concrete, 'observe_shared_pool_forward_processes', processes)
            original_qualify = connection._child_qualify

            def qualify(request, phase, writing):
                original_qualify(request, phase, writing)
                if writing and mode['delete_before_write']:
                    mode['delete_before_write'] = False
                    path = connection.shared_state / phase / 'transition.json'
                    mode['deleted_receipt'] = path.read_bytes()
                    path.unlink()

            monkeypatch.setattr(connection, '_child_qualify', qualify)
            child_type = module.HTTPSSharedStagingWorkloads

            def child(**options):
                result = child_type(**options)
                client_type = type(result.client)
                result.client.close()
                result.client = client_type(base_url=result.api_server, transport=httpx.MockTransport(handle))
                return result

            monkeypatch.setattr(module, 'HTTPSSharedStagingWorkloads', child)
        api.block = 'forward-build-policy'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        manager = runtime.database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == [], 'lost child evidence must prevent a mutating PATCH'
        missing = state / 'forward-stop/transition.json'
        assert not missing.exists() and mode['deleted_receipt'] is not None
        missing.write_bytes(mode['deleted_receipt'])
        missing.chmod(0o600)
        with pytest.raises(ValueError, match='shared pool operation unconfirmed') as interrupted:
            parent.run_shared_pool_operation(**kwargs)
        if len(writes) != 1:
            import traceback

            frames, cause = [], interrupted.value
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('workload phase failed before intended transport fault: ' + ', '.join(frames), pytrace=False)
        record = json.loads((state / 'operation.json').read_bytes())
        assert record['forward']['forward-stop']['status'] == 'started'
        choices = module.shared_forward_staging_options(bindings=bindings, state_dir=state)
        assert len(choices[writes[0]]) == 2  # Both original and committed target, never a guessed outcome.
        assert tuple(_stable(row) for row in choices[writes[0]]) == (
            _stable(original[writes[0]]), _stable(bindings.plan.staging.stopped[writes[0]]))
        with pytest.raises(ValueError):
            module.HTTPSSharedStagingWorkloads(bindings=bindings, phase='forward-wire',
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=lambda *_: True)
        assert len(writes) == 1  # A later child cannot jump the incomplete stop phase.
        result = parent.run_shared_pool_operation(**kwargs)
        assert result['status'] == 'pending' and result['phase'] == 'forward-build-policy'
        deployments = {key for key, row in original.items() if row['kind'] == 'Deployment'}
        collector, = (key for key, row in original.items() if row['kind'] == 'CronJob')
        assert writes.count(collector) == 1  # Remains stopped; do not patch or start it again.
        assert set(writes) == set(original)
        assert all(writes.count(key) == 3 for key in deployments)
        assert rows[collector]['spec']['suspend'] is True
        for key in deployments:
            wanted = copy.deepcopy(bindings.plan.staging_targets[key])
            wanted['spec']['replicas'] = 1
            assert _stable(rows[key]) == _stable(wanted)
            assert rows[key]['metadata']['uid'] == original[key]['metadata']['uid']
        choices = module.shared_forward_staging_options(bindings=bindings, state_dir=state)
        assert all(len(options) == 1 and _stable(options[0]) == _stable(rows[key]) for key, options in choices.items())
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
        assert writes == before


@pytest.mark.timeout(420)
def test_shared_drain_checks_both_databases_and_retains_qualified_paged_backlog(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """Use real guards/history validators; saved drain is never current authority."""
    import importlib.util
    import json
    import shutil
    import ssl
    import subprocess
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
    from scripts.ops.nebius_pool_migration_guard import pool_cutover_readiness_sql
    from scripts.ops.nebius_pool_origin_history import pool_management_history_sql
    from scripts.ops.nebius_pool_startup_database import pool_startup_closed_sql
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    from loom.nebius_pool_priority import PoolWorkOriginV1
    from loom_service.pool_management.capacity import digest

    name = 'scripts.ops.nebius_shared_pool_drain'
    assert importlib.util.find_spec(name) is not None, 'shared real drain observation is missing'
    module = __import__(name, fromlist=['observe_closed_shared_pool_drain'])
    args, bindings = shared_operation
    runtime = args['runtime']
    guards, model = shared_guard_transport
    spec = bindings.migration.registration.spec
    model.held.update(target.namespace for target in bindings.migration.guards)
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    history_reports, read_history = {}, []
    backlog = {}
    for target in bindings.migration.guards:
        participant, = (row for row in spec.participants if row.participant_id == target.participant_id)
        count = 129 if participant.environment_class == 'development' else 1
        origins = tuple(PoolWorkOriginV1(data_environment_id=participant.environment_id,
            submission_id=UUID(int=index + 1), kind='environment', application=None) for index in range(count))
        rows = [{'key': 'batch:' + str(origin.submission_id), 'origin': origin.model_dump(mode='json'),
            'source_matches': True} for origin in origins]
        backlog[str(participant.participant_id)] = count
        logical_pool, = (row['value'] for row in target.controller['spec']['template']['spec']['containers'][0]['env']
            if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID')
        for offset in range(0, count, 128):
            after = rows[offset - 1]['key'] if offset else None
            query = pool_cutover_readiness_sql(owner=str(spec.operation_id),
                candidate=bindings.migration.registration.candidate['candidate_sha'], logical_pool_id=logical_pool, after=after)
            model.readiness_pages[target.namespace, query] = {'status': 'observed', 'schema_revision': '0176', 'rows': rows[offset:offset + 128]}
            page = origins[offset:offset + 128]
            history_reports[pool_management_history_sql(page)] = {'schema': 'loom.pool-management-history.v1',
                'schema_revision': '0176', 'read_only': True, 'rows': [
                    {'ordinal': index, 'origin': origin.model_dump(mode='json'), 'application': None, 'operation': None}
                    for index, origin in enumerate(page, start=1)]}
    _, documents, handler = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
    populate_database_backends(documents)
    monkeypatch.setattr(shutil, 'which', lambda _: '/usr/bin/kubectl')
    guard_run = subprocess.run
    mode = {'history_invalid': True, 'reopen_guard': False}
    closed_reads = []
    with HTTPSDevelopmentRuntimeProbes(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='private-test-kubernetes-token') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(handler))

        def run(argv, **kwargs):
            if argv[:len(guards.prefix)] == guards.prefix:
                return guard_run(argv, **kwargs)
            assert argv[:len(probes.prefix)] == probes.prefix
            command = argv[len(probes.prefix):]
            assert command[:17] == ['exec', '-n', manager.binding.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres',
                '--', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
            query = command[-1]
            if query == pool_startup_closed_sql(spec):
                closed_reads.append(query)
                value = {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'qualified': True}
            else:
                assert query in history_reports, 'only fixed read-only origin history belongs on the manager DB'
                read_history.append(query)
                value = copy.deepcopy(history_reports[query])
                if mode['history_invalid']:
                    value['rows'][0]['origin']['data_environment_id'] = str(uuid4())
                if mode['reopen_guard']:
                    mode['reopen_guard'] = False
                    model.held.remove(bindings.migration.guards[0].namespace)
            payload = json.dumps(value, indent=32).encode()
            if len(value.get('rows', [])) == 128:
                assert 65536 < len(payload) < 4 * 1024**2  # Legal JSON history exceeds the small process-probe cap.
            return SimpleNamespace(returncode=0, stdout=payload, stderr=b'')

        monkeypatch.setattr(subprocess, 'run', run)

        class ConnectedDrainAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, child):
                if phase == 'forward-drain':
                    module.prepare_shared_pool_drain(bindings=bound, state_dir=child)
                else:
                    super().prepare(phase, bound, child)

            def validate_prepared_phase(self, phase, child):
                if phase == 'forward-drain':
                    module.validate_shared_pool_drain(bindings=bindings, state_dir=child, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, child)

            def validate_phase(self, phase, child, *, complete):
                if phase == 'forward-drain':
                    module.validate_shared_pool_drain(bindings=bindings, state_dir=child, complete=complete)
                else:
                    super().validate_phase(phase, child, complete=complete)

            def advance(self, phase, bound, child):
                if phase != 'forward-drain':
                    return super().advance(phase, bound, child)
                return module.advance_shared_pool_drain(bindings=bound, guards=guards, development=probes,
                    state_dir=child, qualify=lambda: None)

        api = ConnectedDrainAPI(parent)
        api.block = 'forward-stop'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert read_history and closed_reads
        mode['history_invalid'] = False
        mode['reopen_guard'] = True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        model.held.update(target.namespace for target in bindings.migration.guards)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'
        record = json.loads((state / 'forward-drain/drain.json').read_bytes())
        assert record['pending_origins'] == backlog
        assert set(model.readiness_calls) == {target.namespace for target in bindings.migration.guards}
        assert set(read_history) == set(history_reports)  # Includes the 129th queued source, not only page one.
        before = (state / 'forward-drain/drain.json').read_bytes()
        for target in bindings.migration.guards:
            model.readiness_failure = target.namespace
            with pytest.raises(ValueError):
                module.observe_closed_shared_pool_drain(bindings=bindings, guards=guards, development=probes,
                    state_dir=state, qualify=lambda: None)
        assert (state / 'forward-drain/drain.json').read_bytes() == before
        assert model.runtime_stages == model.acquires == []


@pytest.mark.timeout(420)
def test_shared_material_child_preserves_dev_and_never_replays_uncertain_creates(shared_operation):
    """Real fixed stage engine/HTTPS transport, with only remote effects doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot

    name = 'scripts.ops.nebius_shared_pool_material'
    assert importlib.util.find_spec(name) is not None, 'shared staging material child is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingMaterial'])
    args, bindings = shared_operation
    runtime = args['runtime']
    fixed = {**bindings.plan.staging_material, **bindings.plan.staging_reader_authority}
    assert sorted(row['kind'] for row in fixed.values()) == ['ClusterRole', 'ClusterRoleBinding', 'Secret', 'Secret']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    directory = state / 'forward-material'
    receipt, initialized, dispatched = (directory / filename for filename in ('stage.json', 'material.json', 'dispatch.json'))
    stored, posts, defaults = {}, [], []
    mode = {'prepare_loss': True, 'default_loss': True, 'erase_stage': True,
        'saved_stage': None, 'reply_loss': True, 'read_loss': False}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def handle(message):
        if message.method == 'GET':
            assert message.url.path in paths, 'material stage must never read or rewrite installed dev/manager resources'
            key = paths[message.url.path]
            if mode['read_loss']:
                mode['read_loss'] = False
                raise httpx.ReadError('simulated lost create readback')
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        document = json.loads(message.content)
        key = _key(document)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        actual = copy.deepcopy(document)
        actual['metadata'].update(uid=str(uuid4()), resourceVersion='1')
        if message.url.params.get('dryRun') == 'All':
            defaults.append(key)
            if mode['default_loss']:
                mode['default_loss'] = False
                raise httpx.ReadError('simulated lost effect-free default preview')
            return httpx.Response(201, json=actual)
        assert not message.url.query and key not in stored
        assert json.loads(initialized.read_bytes())['dispatch'] is True and dispatched.is_file()
        assert json.loads(receipt.read_bytes())['resources'][key]['status'] == 'create_intent'
        posts.append(key)
        stored[key] = actual
        if mode['reply_loss']:
            mode['reply_loss'], mode['read_loss'] = False, True
            raise httpx.ReadError('simulated lost committed create reply')
        return httpx.Response(201, json=actual)

    with ExitStack() as stack:
        class ConnectedMaterialAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, child):
                if phase == 'forward-material':
                    module.prepare_shared_staging_material(bindings=bound, state_dir=child)
                    if mode['prepare_loss']:
                        mode['prepare_loss'] = False
                        raise TimeoutError('simulated interruption after local preparation')
                else:
                    super().prepare(phase, bound, child)

            def validate_prepared_phase(self, phase, child):
                if phase == 'forward-material':
                    module.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, child)

            def validate_phase(self, phase, child, *, complete):
                if phase == 'forward-material':
                    module.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=complete)
                else:
                    super().validate_phase(phase, child, complete=complete)

            def transport(self, bound):
                def qualify_runtime(request, phase, writing):
                    assert request == runtime and phase == 'forward-material'
                    if writing and mode['erase_stage'] and json.loads(initialized.read_bytes())['dispatch']:
                        mode['erase_stage'] = False
                        mode['saved_stage'] = receipt.read_bytes()
                        receipt.unlink()  # Fault injection in this test's private child evidence.
                api = stack.enter_context(module.HTTPSSharedStagingMaterial(bindings=bound,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify_runtime))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return api

            def advance(self, phase, bound, child):
                if phase != 'forward-material':
                    return super().advance(phase, bound, child)
                return module.advance_shared_staging_material(bindings=bound, api=self.transport(bound), state_dir=child)

        api = ConnectedMaterialAPI(parent)
        api.block = 'forward-wire'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert initialized.is_file() and not receipt.exists() and not dispatched.exists()
        assert defaults == posts == []  # Preparing the child cannot even invoke a dry-run.
        first = next(iter(fixed))
        stored[first] = copy.deepcopy(fixed[first])
        stored[first]['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert defaults == posts == []  # Existing material is never adopted.
        stored.clear()  # Reset only this controlled test transport's foreign-resource scenario.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert defaults == [first] and posts == [] and not receipt.exists()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == [] and not receipt.exists() and mode['saved_stage'] is not None
        assert json.loads(initialized.read_bytes())['dispatch'] is True and dispatched.is_file()
        receipt.write_bytes(mode['saved_stage'])
        receipt.chmod(0o600)
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == []  # A saved create intent without its object cannot be replayed.
        # Rewind only this controlled pre-effect fixture for the independent
        # lost-response scenario. Production cannot reset dispatch history.
        prepared = json.loads(mode['saved_stage'])
        for item in prepared['resources'].values():
            item['status'] = 'prepared'
        receipt.write_text(json.dumps(prepared))
        preparing = json.loads(initialized.read_bytes())
        preparing['dispatch'] = False
        initialized.write_text(json.dumps(preparing))
        dispatched.unlink()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == [first] and set(stored) == {first}
        for path in (initialized, dispatched, receipt):
            content = path.read_bytes()
            path.unlink()
            with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
                parent.run_shared_pool_operation(**kwargs)
            assert posts == [first]
            path.write_bytes(content)
            path.chmod(0o600)
        with pytest.raises(ValueError):
            foreign = next(row for row in bindings.plan.development.runtime.resources.values() if row['kind'] == 'Secret')
            api.transport(bindings).create_resource(foreign)
        assert posts == [first]
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        assert posts == list(fixed) and set(stored) == set(fixed)
        for key, row in stored.items():
            actual = _snapshot(row)
            actual['metadata']['annotations'].pop('loom.nebius/management-stage-operation')
            if not actual['metadata']['annotations']:
                actual['metadata'].pop('annotations')
            assert actual == fixed[key]
        reader, = (row for row in stored.values() if row['kind'] == 'ClusterRole')
        assert reader['rules'][0]['verbs'] == ['get'] and reader['rules'][0]['resources'] == ['namespaces']
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        assert posts == list(fixed)


@pytest.mark.timeout(420)
def test_shared_database_child_stages_only_staging_and_reads_unknown_commits(shared_operation, shared_guard_transport):
    """Real bound DB adapter and parent; no replay of dev's completed SQL stage."""
    import importlib.util
    import json

    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_database'
    assert importlib.util.find_spec(name) is not None, 'shared staging database child is missing'
    module = __import__(name, fromlist=['advance_shared_staging_database'])
    args, bindings = shared_operation
    database, model = shared_guard_transport
    model.held.update(target.namespace for target in bindings.migration.guards)
    staging = bindings.plan.staging.attachment.guard
    dev, = (target for target in bindings.migration.guards if target.participant_id != staging.participant_id)
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    receipt = state / 'forward-database/database.json'
    failure = {'delete': True, 'saved': None}
    # The DB-bound path must still work with the captured controller stopped.
    controller = model.documents['/apis/apps/v1/namespaces/' + staging.namespace + '/deployments/loom-control-plane']
    controller['spec']['replicas'] = 0

    class ConnectedDatabaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            if phase == 'forward-database':
                module.prepare_shared_staging_database(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-database':
                module.validate_shared_staging_database(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-database':
                module.validate_shared_staging_database(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase != 'forward-database':
                return super().advance(phase, bound, directory)

            def qualify():
                if failure['delete'] and json.loads(receipt.read_bytes())['status'] == 'intent':
                    failure['delete'] = False
                    failure['saved'] = receipt.read_bytes()
                    receipt.unlink()  # This test's private fault injection, never production repair.

            return module.advance_shared_staging_database(bindings=bound, api=database, state_dir=directory, qualify=qualify)

    api = ConnectedDatabaseAPI(parent)
    api.block = 'forward-material'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert not receipt.exists() and model.runtime_stages == [] and failure['saved'] is not None
    receipt.write_bytes(failure['saved'])
    receipt.chmod(0o600)
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.runtime_stages == []  # Missing outcome of saved intent never permits another stage.
    # Controlled fixture rewind to exercise an independent unknown-commit fault.
    # Production cannot reset an uncertain intent or recreate missing evidence.
    prepared = json.loads(failure['saved'])
    prepared['status'] = 'prepared'
    receipt.write_text(json.dumps(prepared))
    model.runtime_uncertain = True
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.runtime_stages == [staging.namespace]
    assert json.loads(receipt.read_bytes())['status'] == 'intent'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    assert model.runtime_stages == [staging.namespace] and set(model.runtime_reads) == {staging.namespace}
    assert json.loads(receipt.read_bytes())['status'] == 'staged'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    assert model.runtime_stages == [staging.namespace]
    assert dev.namespace not in model.runtime_roles
    assert model.acquires == []  # No registration, guard replay/release, or controller exec.


@pytest.mark.timeout(420)
def test_shared_parent_restricts_current_roles_once_and_checks_foreign_effective_grants(shared_operation):
    """Catch repeated unknown PATCHes and false fencing from only two Role reads."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_management_switch import _stable

    name = 'scripts.ops.nebius_shared_pool_roles'
    assert importlib.util.find_spec(name) is not None, 'connected shared Role fencing is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingRoles'])
    args, bindings = shared_operation
    originals = bindings.plan.staging_role_originals
    roles = copy.deepcopy(originals)
    writes, attempts, reviews, qualifications = [], [], [], []
    mode = {'delete_receipt': True, 'delete_at_intent': False, 'saved_receipt': None, 'reject': True, 'lose_reply': True,
        'lose_read': False, 'extra_grant': True, 'incomplete': False, 'change_binding': False}
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    receipt = state / 'forward-roles/roles.json'
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    rolebindings = [copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] == 'RoleBinding']
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'name': 'foreign-reader', 'namespace': 'outside-pool', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'subjects': [{'kind': 'Group', 'apiGroup': 'rbac.authorization.k8s.io', 'name': 'system:authenticated'}],
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign-reader'}}
    rolebindings.append(foreign)
    original_bindings = copy.deepcopy(rolebindings)

    def handle(message):
        path = message.url.path
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            assert namespace in namespaces
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1',
                    'labels': {'loom.nebius/management-installation': identity.installation_id,
                        'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path in ('/apis/rbac.authorization.k8s.io/v1/rolebindings',
                '/apis/rbac.authorization.k8s.io/v1/clusterrolebindings'):
            assert message.method == 'GET' and 'Impersonate-User' not in message.headers
            kind = 'RoleBinding' if path.endswith('/rolebindings') else 'ClusterRoleBinding'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': kind + 'List', 'metadata': {'resourceVersion': '71'},
                'items': copy.deepcopy(rolebindings) if kind == 'RoleBinding' else []})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            account = message.headers['Impersonate-User']
            account_namespace = account.split(':')[2]
            assert set(message.headers.get_list('Impersonate-Group', split_commas=True)) == {
                'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'}
            namespace = json.loads(message.content)['spec']['namespace']
            reviews.append((account, namespace))
            rules = [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'list', 'watch']}]
            if namespace == 'outside-pool' and mode['extra_grant']:
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']})
            if mode['change_binding']:
                mode['change_binding'] = False
                foreign['metadata']['resourceVersion'] = '2'
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1',
                'kind': 'SelfSubjectRulesReview', 'spec': {}, 'status': {'incomplete': mode['incomplete'],
                    'resourceRules': rules, 'nonResourceRules': []}})
        assert 'Impersonate-User' not in message.headers
        parts = path.split('/')
        assert parts[:5] == ['', 'apis', 'rbac.authorization.k8s.io', 'v1', 'namespaces']
        key = 'Role:' + parts[5] + ':' + parts[7]
        assert parts[6] == 'roles' and key in originals
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated lost first Role readback')
            return httpx.Response(200, json=roles[key])
        assert message.method == 'PATCH' and not message.url.query
        changes = json.loads(message.content)
        current = copy.deepcopy(roles[key])
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': originals[key]['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/rules', 'value': current['rules']},
        ]
        assert [(row['op'], row['path']) for row in changes[3:]] == [
            ('add', '/metadata/annotations'), ('replace', '/rules')]
        assert json.loads(receipt.read_bytes())['roles'][key] == 'intent'
        attempts.append(key)
        if mode['reject']:
            mode['reject'] = False
            return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status',
                'status': 'Failure', 'code': 409, 'reason': 'Conflict'})
        current['metadata']['annotations'] = changes[3]['value']
        current['rules'] = changes[4]['value']
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        roles[key] = current
        writes.append(key)
        if mode['lose_reply']:
            mode['lose_reply'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated lost committed Role PATCH reply')
        return httpx.Response(200, json=current)

    with ExitStack() as stack:
        class ConnectedRolesAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, directory):
                if phase == 'forward-roles':
                    module.prepare_shared_staging_roles(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-roles':
                    module.validate_shared_staging_roles(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-roles':
                    module.validate_shared_staging_roles(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase != 'forward-roles':
                    return super().advance(phase, bound, directory)

                def qualify(current, writing):
                    assert current == bindings
                    qualifications.append(writing)
                    if writing and (mode['delete_receipt'] or (mode['delete_at_intent']
                            and 'intent' in json.loads(receipt.read_bytes())['roles'].values())):
                        mode['delete_receipt'], mode['delete_at_intent'] = False, False
                        mode['saved_receipt'] = receipt.read_bytes()
                        receipt.unlink()  # Fault injection only in this test's private history.

                api = stack.enter_context(module.HTTPSSharedStagingRoles(bindings=bound,
                    api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_staging_roles(bindings=bound, api=api, state_dir=directory)

        api = ConnectedRolesAPI(parent)
        api.block = 'forward-database'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts and not receipt.exists() and mode['saved_receipt'] is not None
        # Restore this controlled pre-dispatch fixture to test other faults.
        # Production never recreates lost started history.
        prepared_checkpoint = mode['saved_receipt']
        receipt.write_bytes(prepared_checkpoint)
        receipt.chmod(0o600)
        mode['delete_at_intent'] = True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts and not receipt.exists()
        receipt.write_bytes(mode['saved_receipt'])
        receipt.chmod(0o600)
        # Even with restored intent, an unobserved effect is never redispatched.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts
        receipt.write_bytes(prepared_checkpoint)  # Controlled fixture rewind only.
        namespaces['kube-system'] = str(uuid4())
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts
        namespaces['kube-system'] = identity.kube_system_uid
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
        assert len(attempts) == 1 and writes == []  # Definite rejection permits a later retry.
        first = sorted(roles)[0]
        roles[first]['rules'].append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert len(attempts) == 1 and writes == []
        roles[first]['rules'].pop()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert len(writes) == 1
        options = module.shared_forward_role_options(bindings=bindings, state_dir=state)
        assert len(options[writes[0]]) == 2  # Unknown commit retains both exact possibilities.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert set(writes) == set(originals) and len(writes) == 2
        assert len(attempts) == 3  # No second dispatch of the unknown committed PATCH.
        assert any(namespace == 'outside-pool' for _, namespace in reviews)
        assert rolebindings == original_bindings
        mode['extra_grant'], mode['incomplete'] = False, True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        mode['incomplete'], mode['change_binding'] = False, True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
        assert len(attempts) == 3 and any(qualifications)
        expected_subjects = set()
        for row in bindings.plan.staging.originals.values():
            pod = (row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob'
                else row['spec']['template']['spec'])
            expected_subjects.add('system:serviceaccount:' + row['metadata']['namespace'] + ':' + pod.get('serviceAccountName', 'default'))
        assert {subject for subject, namespace in reviews if namespace == 'outside-pool'} == expected_subjects
        assert all(_stable(roles[key]) == _stable(bindings.plan.staging_role_targets[key]) for key in originals)
        assert all(roles[key]['metadata']['uid'] == originals[key]['metadata']['uid'] for key in originals)
        assert all(len(choices) == 1 for choices in module.shared_forward_role_options(bindings=bindings, state_dir=state).values())
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
        assert len(attempts) == 3


@pytest.mark.timeout(420)
def test_shared_build_activation_requires_effective_policy_and_never_repeats_unknown_patch(
        shared_operation, shared_runtime, completed_pool, handoff):
    """Missing policy enforcement must never expose the rootless-build exception."""
    import importlib
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_build_activation'
    assert importlib.util.find_spec(name) is not None, 'shared build activation is not connected'
    module = importlib.import_module(name)
    args, bindings = shared_operation
    plan = bindings.plan.development
    original = plan.build_namespace_original
    namespace_path = '/api/v1/namespaces/' + original['metadata']['name']
    documents = {namespace_path: copy.deepcopy(original)}
    documents[namespace_path]['metadata']['resourceVersion'] = '801'
    for row in plan.build_policy.values():
        plural = 'validatingadmissionpolicies' if row['kind'] == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
        path = '/apis/admissionregistration.k8s.io/v1/' + plural + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
        documents[path]['metadata'].update(generation=1, resourceVersion='802')
        if row['kind'] == 'ValidatingAdmissionPolicy':
            documents[path]['status'] = {'observedGeneration': 1, 'typeChecking': {'expressionWarnings': []}}
            policy_path = path
    registration = bindings.migration.registration.binding
    documents['/api/v1/namespaces/kube-system'] = {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': 'kube-system', 'uid': registration.kube_system_uid}}
    calls, patches = [], []
    mode = {'deny': False, 'response_lost': True, 'conflict': False}

    def handler(request):
        calls.append((request.method, request.url.path, dict(request.url.params)))
        if request.method == 'GET':
            assert not request.url.params
            return httpx.Response(200, json=copy.deepcopy(documents[request.url.path]))
        if request.method == 'POST':
            assert request.url.path == namespace_path + '/pods' and dict(request.url.params) == {'dryRun': 'All'}
            pod = json.loads(request.content)
            assert pod['kind'] == 'Pod' and pod['metadata']['namespace'] == original['metadata']['name']
            assert pod['spec']['automountServiceAccountToken'] is False
            if mode.get('flap_before_patch'):
                mode['probe_count'] = mode.get('probe_count', 0) + 1
            if not mode['deny'] or (mode.get('flap_before_patch') and mode['probe_count'] == 2):
                return httpx.Response(201, json=pod)
            policy = documents[policy_path]['metadata']['name']
            reason = 'Only sequential prepare, build and publish phases are allowed.'
            message = 'denied by policy ' + (policy if mode['deny'] != 'foreign' else 'foreign-policy') + ': ' + reason
            return httpx.Response(403, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                'reason': 'Forbidden', 'code': 403, 'message': message})
        assert request.method == 'PATCH' and request.url.path == namespace_path and not request.url.params
        body = json.loads(request.content)
        current = documents[namespace_path]
        assert body[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': original['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/metadata/labels', 'value': original['metadata']['labels']},
        ]
        assert all(item['path'] in {'/metadata/uid', '/metadata/resourceVersion', '/metadata/labels',
            '/metadata/annotations'} for item in body)
        patches.append(body)
        if mode['conflict']:
            mode['conflict'] = False
            return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                'reason': 'Conflict', 'code': 409})
        for patch in body:
            if patch['op'] in {'add', 'replace'}:
                current['metadata'][patch['path'].split('/')[-1]] = copy.deepcopy(patch['value'])
        current['metadata']['resourceVersion'] = '803'
        if mode.get('lose_policy_after_patch'):
            mode['deny'] = False
        if mode['response_lost']:
            mode['response_lost'] = False
            raise httpx.ReadTimeout('simulated response loss after namespace commit')
        return httpx.Response(200, json=copy.deepcopy(current))

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, state):
            if phase == 'forward-build-policy':
                module.prepare_shared_build_activation(bindings=bound, state_dir=state)
            else:
                super().prepare(phase, bound, state)

        def validate_prepared_phase(self, phase, state):
            if phase == 'forward-build-policy':
                module.validate_shared_build_activation(bindings=bindings, state_dir=state, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, state)

        def validate_phase(self, phase, state, *, complete):
            if phase == 'forward-build-policy':
                module.validate_shared_build_activation(bindings=bindings, state_dir=state, complete=complete)
            else:
                super().validate_phase(phase, state, complete=complete)

        def advance(self, phase, bound, state):
            if phase != 'forward-build-policy':
                return super().advance(phase, bound, state)
            with module.HTTPSSharedBuildActivation(bindings=bound,
                    api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=lambda current, writing: None) as api:
                api.client.close()
                api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(handler))
                return module.advance_shared_build_activation(bindings=bound, api=api, state_dir=state)

    api = API(parent)
    api.block = 'forward-gateway'
    kwargs = dict(**args, bindings=bindings, api=api, action='install')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == [] and documents[namespace_path]['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'
    mode['deny'] = 'foreign'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert patches == []
    mode['deny'] = True
    for damage in ('warning', 'uid', 'selector'):
        saved = copy.deepcopy(documents[policy_path])
        if damage == 'warning':
            documents[policy_path]['status']['typeChecking']['expressionWarnings'] = [{'warning': 'invalid'}]
        elif damage == 'uid':
            documents[policy_path]['metadata']['uid'] = str(uuid4())
        else:
            documents[policy_path]['spec']['matchConditions'] = [{'name': 'skip', 'expression': 'false'}]
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        documents[policy_path] = saved
        assert patches == []
    documents[policy_path]['status']['observedGeneration'] = 0
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == []
    documents[policy_path]['status']['observedGeneration'] = 1
    mode['flap_before_patch'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == []  # Known loss of enforcement before dispatch remains a safe wait.
    mode['flap_before_patch'] = False
    mode['conflict'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert len(patches) == 1
    mode['lose_policy_after_patch'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert len(patches) == 2  # Completion requires fresh enforcement after the PATCH too.
    mode['deny'] = True
    api.prepare_failure = ('forward-gateway', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert len(patches) == 2  # Rejected CAS retried; lost committed response was read back.
    assert documents[namespace_path]['metadata']['labels'] == plan.build_namespace_target['metadata']['labels']
    assert any(method == 'POST' for method, _, _ in calls)
    assert not any(method in {'DELETE', 'PUT'} for method, _, _ in calls)

    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory

    # The original installer remains strict. Only the anchored shared handoff
    # may recognize this exact successor; arbitrary label changes still fail.
    runtime = args['runtime']
    _, inventory, read = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
    inventory[namespace_path] = copy.deepcopy(documents[namespace_path])
    with HTTPSDevelopmentRuntimeObserver(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir)
        assert observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        inventory[namespace_path]['metadata']['labels']['pod-security.kubernetes.io/warn'] = 'privileged'
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        inventory[namespace_path] = copy.deepcopy(documents[namespace_path])
        receipt = Path(runtime.database.manager.retained.request.retained.operation['state_dir']).parent / 'shared-pool-handoff/forward-build-policy/build-policy.json'
        saved = receipt.read_bytes()
        receipt.unlink()
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        receipt.write_bytes(saved)
        receipt.chmod(0o600)
    from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    populate_runtime_pods(inventory)
    with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
        component, deployment, pod = probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)
        assert component == 'controller' and deployment['metadata']['name'] == 'loom-control-plane'
        assert pod['status']['phase'] == 'Running'
    api.prepare_failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    assert len(patches) == 2


@pytest.mark.timeout(600)
def test_shared_gateway_stages_exact_grants_then_starts_once_under_closed_parent(
        shared_operation, completed_pool, shared_runtime, handoff):
    """A grant or PATCH reply loss must not start a second writer or replay effects."""
    import importlib
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_build_activation as build_child
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import (
        populate_runtime_pods,
        runtime_http_inventory,
    )

    name = 'scripts.ops.nebius_shared_pool_gateway'
    assert importlib.util.find_spec(name) is not None, 'shared gateway child is missing'
    module = importlib.import_module(name)
    args, bindings = shared_operation
    runtime, plan = args['runtime'], bindings.plan.development
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'forward-gateway'
    fixed = plan.authority
    resources = {}
    gateway = copy.deepcopy(plan.gateway_original)
    gateway['metadata'].update(resourceVersion='901', generation=1)
    gateway_path = runtime_resource_path(gateway) + '/' + gateway['metadata']['name']
    resource_paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}
    creates, patches, reviews, challenges = [], [], [], []
    documents = {}
    mode = {'lose_create': True, 'lose_patch': True, 'lose_read': False,
        'ready': False, 'extra_permission': False, 'runtime_ready': False, 'prepare_loss': True}

    def refresh():
        namespace = gateway['metadata']['namespace']
        gateway['status'] = {'observedGeneration': gateway['metadata']['generation'],
            **{field: int(mode['ready'] and gateway['spec']['replicas'] == 1)
                for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
        documents.clear()
        documents[gateway_path] = copy.deepcopy(gateway)
        for version, plural, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod')):
            path = ('/api/v1' if version == 'v1' else '/apis/' + version) + '/namespaces/' + namespace + '/' + plural
            documents[path] = {'apiVersion': version, 'kind': kind + 'List',
                'metadata': {'resourceVersion': '902'}, 'items': []}
        if mode['ready']:
            populate_runtime_pods(documents)

    refresh()

    def handle(request):
        path = request.url.path
        if request.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated committed effect readback loss')
            if path in resource_paths:
                key = resource_paths[path]
                return httpx.Response(200, json=copy.deepcopy(resources[key])) if key in resources else httpx.Response(404)
            if path in documents:
                return httpx.Response(200, json=copy.deepcopy(documents[path]))
            collection = path.rsplit('/', 1)[-1]
            assert path == '/apis/rbac.authorization.k8s.io/v1/' + collection
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[collection]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '903'},
                'items': [copy.deepcopy(row) for row in resources.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert request.method == 'POST' and not request.url.query
            assert request.headers['Impersonate-User'] == 'system:serviceaccount:loom-nebius-management-dev:loom-pool-gateway'
            namespace = json.loads(request.content)['spec']['namespace']
            reviews.append(namespace)
            rules = [copy.deepcopy(rule) for row in resources.values()
                if row['kind'] == 'ClusterRole' or (row['kind'] == 'Role' and row['metadata']['namespace'] == namespace)
                for rule in row['rules']]
            if mode['extra_permission']:
                rules.append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {},
                'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': [], 'evaluationError': ''}})
        if request.method == 'POST':
            row = json.loads(request.content)
            key = _key(row)
            assert key in fixed and path == runtime_resource_path(fixed[key])
            row['metadata'].update(uid=str(uuid4()), resourceVersion='904')
            if request.url.params.get('dryRun') == 'All':
                return httpx.Response(201, json=row)
            assert not request.url.query and key not in resources
            assert json.loads((child / 'gateway.json').read_bytes())['dispatch'] is True
            assert json.loads((child / 'authority/stage.json').read_bytes())['resources'][key]['status'] == 'create_intent'
            assert gateway['spec']['replicas'] == 0
            creates.append(key)
            resources[key] = row
            if mode['lose_create']:
                mode['lose_create'], mode['lose_read'] = False, True
                raise httpx.ReadError('simulated committed grant response loss')
            return httpx.Response(201, json=row)
        assert request.method == 'PATCH' and path == gateway_path
        assert set(resources) == set(fixed) and reviews, 'gateway cannot start before effective fixed grants'
        changed = copy.deepcopy(gateway)
        changes = json.loads(request.content)
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': gateway['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': gateway['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': gateway['spec']},
        ]
        for change in changes:
            target, field = ((changed['metadata'], change['path'].rsplit('/', 1)[-1])
                if change['path'].startswith('/metadata/') else (changed, 'spec'))
            if change['op'] == 'test':
                assert target[field] == change['value']
            elif change['op'] == 'remove':
                target.pop(field)
            else:
                target[field] = change['value']
        if request.url.params.get('dryRun') == 'All':
            return httpx.Response(200, json=changed)
        assert not request.url.query
        assert json.loads((child / 'start/transition.json').read_bytes())['resources'][_key(gateway)]['status'] == 'intent'
        patches.append(_key(gateway))
        changed['metadata'].update(resourceVersion='905', generation=2)
        gateway.clear()
        gateway.update(changed)
        refresh()
        if mode['lose_patch']:
            mode['lose_patch'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated committed gateway response loss')
        return httpx.Response(200, json=copy.deepcopy(gateway))

    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            def prepare(self, phase, bound, directory):
                if phase == 'forward-activate':
                    return activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
                if phase == 'forward-build-policy':
                    return build_child.prepare_shared_build_activation(bindings=bound, state_dir=directory)
                if phase != 'forward-gateway':
                    return super().prepare(phase, bound, directory)
                module.prepare_shared_gateway(bindings=bound, state_dir=directory)
                if mode['prepare_loss']:
                    mode['prepare_loss'] = False
                    raise TimeoutError('simulated interruption after effect-free child preparation')

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-activate':
                    activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-activate':
                    activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-activate':
                    return False
                if phase == 'forward-build-policy':
                    # Only this preceding phase's remote effect is doubled;
                    # gateway and inventory readers consume its real receipt.
                    record = build_child.validate_shared_build_activation(bindings=bound, state_dir=directory, complete=False)
                    private_state._atomic_json(directory / 'build-policy.json', {**record, 'status': 'activated'})
                    return True
                if phase != 'forward-gateway':
                    return super().advance(phase, bound, directory)
                def ready(request, selected, original, expected, actual):
                    assert request == runtime and selected == 'forward-gateway'
                    assert _key(original) == _key(actual) == _key(plan.gateway_original)
                    challenges.append(actual['metadata']['uid'])
                    return mode['runtime_ready']
                transport = stack.enter_context(module.HTTPSSharedGateway(bindings=bound,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=ready))
                transport.client.close()
                transport.client = type(completed_pool[3].api.client)(base_url=transport.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_gateway(bindings=bound, api=transport, state_dir=directory)

        api = API(parent)
        api.block = 'forward-activate'
        kwargs = dict(**args, bindings=bindings, api=api, action='install')
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == patches == []
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == [next(iter(fixed))] and patches == []
        for path in (child / 'gateway.json', child / 'authority/stage.json', child / 'authority/dispatch.json', child / 'start/transition.json'):
            saved = path.read_bytes()
            path.unlink()
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**kwargs)
            assert len(creates) == 1 and patches == []
            path.write_bytes(saved)
            path.chmod(0o600)
        mode['extra_permission'] = True
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == list(fixed) and patches == []
        mode['extra_permission'] = False
        with pytest.raises(ValueError) as startup_error:
            parent.run_shared_pool_operation(**kwargs)
        if not patches:
            import traceback

            frames, cause = [], startup_error.value
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('gateway failed before intended PATCH fault: ' + ', '.join(frames), pytrace=False)
        assert patches == [_key(gateway)]
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
        assert challenges == []  # No process challenge before actual rollout readiness.
        mode['ready'] = True
        refresh()
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
        assert challenges and patches == [_key(gateway)]
        reads, inventory, read = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
        namespace = copy.deepcopy(plan.build_namespace_target)
        namespace['metadata']['uid'] = plan.build_namespace_original['metadata']['uid']
        inventory['/api/v1/namespaces/' + namespace['metadata']['name']] = namespace
        inventory[gateway_path] = copy.deepcopy(gateway)
        with HTTPSDevelopmentRuntimeObserver(request=runtime,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                ssl_context=ssl.create_default_context()) as observer:
            for connection in (observer, observer.manager, observer.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir)  # Ordinary installation remains strict.
            observed = observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert observed[_key(gateway)] == gateway
            for damage in ('uid', 'image'):
                changed = inventory[gateway_path]
                if damage == 'uid':
                    changed['metadata']['uid'] = str(uuid4())
                else:
                    changed['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
                with pytest.raises(ValueError):
                    observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
                inventory[gateway_path] = copy.deepcopy(gateway)
            receipt = child / 'start/transition.json'
            saved = receipt.read_bytes()
            receipt.unlink()
            before = len(reads)
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert len(reads) == before
            receipt.write_bytes(saved)
            receipt.chmod(0o600)
            changed_during_read = []

            def changing_read(message):
                result = read(message)
                if not changed_during_read:
                    # Same parsed record, different retained bytes: semantic
                    # equality alone must not hide a concurrent journal write.
                    receipt.write_bytes(saved + b'\n')
                    changed_during_read.append(True)
                return result

            for connection in (observer, observer.manager, observer.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                    transport=httpx.MockTransport(changing_read))
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert changed_during_read
            receipt.write_bytes(saved)
            receipt.chmod(0o600)
        from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes

        populate_runtime_pods(inventory)
        with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
                ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            component, _, pod = probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)
            assert component == 'controller' and pod['status']['phase'] == 'Running'
        assert all(message.method == 'GET' for message in reads)
        mode['runtime_ready'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
        assert patches == [_key(gateway)] and creates == list(fixed)
        assert gateway['spec']['replicas'] == 1 and _stable(gateway) == _stable(plan.gateway_target)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
        assert patches == [_key(gateway)] and creates == list(fixed)
        assert {namespace for participant in bindings.migration.registration.spec.participants
            for namespace in (participant.execution_namespace.name, participant.build_namespace.name)} <= set(reviews)
        activation_path = child.parent / 'forward-activate/activation.json'
        activation_bytes = activation_path.read_bytes()
        with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
                ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            assert probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)[0] == 'controller'
            activation_path.unlink()
            before = len(reads)
            with pytest.raises(ValueError):
                probes.inspect(state_dir=plan.runtime.state_dir)
            assert len(reads) == before
            activation_path.write_bytes(activation_bytes)
            activation_path.chmod(0o600)
            changed = []

            def change_activation_during_read(message):
                response = read(message)
                if not changed:
                    activation_path.write_bytes(activation_bytes + b'\n')
                    changed.append(True)
                return response

            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                    transport=httpx.MockTransport(change_activation_during_read))
            with pytest.raises(ValueError):
                probes.inspect(state_dir=plan.runtime.state_dir)
            assert changed
            activation_path.write_bytes(activation_bytes)
            activation_path.chmod(0o600)


@pytest.mark.timeout(180)
def test_shared_activation_binds_own_parent_and_never_replays_unknown_writes(shared_operation):
    """Real phase journals; only preceding phases and remote transactions doubled.

    This checks shared-parent authority and one-dispatch ordering, not installed
    runtime readiness or the concrete database transport's SQL qualification.
    """
    import importlib.util
    import json

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_activation'
    assert importlib.util.find_spec(name) is not None, 'shared-parent activation connection is missing'
    module = __import__(name, fromlist=['advance_shared_pool_activation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'forward-activate'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    effects = []

    class Remote:
        migration = bindings.migration
        mode = 'closed'
        guards = dict.fromkeys(roster, 'held')
        failure = ('open', None, 'before')
        qualified = False
        damage = None
        ready = True

        def verify_retained(self):
            if not self.qualified:
                raise ValueError('simulated current authority loss')

        def qualify_shared_state(self):
            self.verify_retained()
            if not self.ready:
                raise ValueError('simulated unhealthy shared runtime')

        def read_workload(self, key):
            pytest.fail('shared activation must not reconstruct old startup workloads')

        def qualify_runtime(self):
            self.verify_retained()
            assert self.mode == 'closed' and set(self.guards.values()) == {'held'}

        def pool_state(self):
            if self.damage == 'receipt-bytes':
                path = child / 'activation.json'
                path.write_bytes(path.read_bytes() + b'\n')
            elif self.damage == 'receipt-loss':
                (child / 'activation.json').unlink()
            return self.mode

        def guard_state(self, participant):
            return self.guards[participant]

        def _write(self, operation, participant=None):
            record = json.loads((child / 'activation.json').read_bytes())
            frame = json.loads((state / 'operation.json').read_bytes())
            assert frame['forward']['forward-activate']['status'] == 'started'
            assert not (Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json').exists()
            if operation == 'open':
                assert record['opening'] == 'intent'
                assert self.mode == 'closed' and set(self.guards.values()) == {'held'}
            else:
                assert operation == 'release'
                assert self.mode == 'global' and record['opening'] == 'opened'
                assert record['guards'][participant] == {'release': 'intent', 'fence': 'prepared'}
                assert self.guards[participant] == 'held'
            effects.append((operation, participant))
            if self.failure == (operation, participant, 'before'):
                raise TimeoutError('simulated lost transaction before acknowledgement')
            if operation == 'open':
                self.mode = 'global'
            else:
                self.guards[participant] = 'open'
            if self.failure == (operation, participant, 'after'):
                raise TimeoutError('simulated committed transaction reply loss')

        def open_pool(self):
            self._write('open')

        def release_guard(self, participant):
            self._write('release', participant)

        def fence_pool(self):
            pytest.fail('forward child cannot acquire recovery authority')

        def fence_guard(self, participant):
            pytest.fail('forward child cannot acquire recovery authority')

    remote = Remote()

    class API(SharedPhaseAPI):
        interrupt_preparation = True

        def prepare(self, phase, bound, directory):
            if phase == 'forward-activate':
                module.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
                if self.interrupt_preparation:
                    self.interrupt_preparation = False
                    raise TimeoutError('simulated loss after local preparation')
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-activate':
                module.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-activate':
                module.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-activate':
                return module.advance_shared_pool_activation(bindings=bound, api=remote, state_dir=directory)
            return super().advance(phase, bound, directory)

    api = API(parent)
    api.block = 'forward-verify'
    kwargs = dict(**args, bindings=bindings, api=api, action='install')
    with pytest.raises(FileNotFoundError):
        module.prepare_shared_pool_activation(bindings=bindings, state_dir=child)
    assert not child.exists()
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    assert json.loads((child / 'activation.json').read_bytes())['opening'] == 'prepared'
    assert json.loads((state / 'operation.json').read_bytes())['forward']['forward-activate']['status'] == 'prepared'
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('closed',)
    assert view.guard_states == {target.namespace: ('held',) for target in bindings.migration.guards}
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []  # Preparation resumed, but live qualification failed.
    remote.qualified = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]  # An unknown opening is never replayed.
    assert module.shared_forward_activation_view(bindings=bindings, state_dir=state).pool_states == ('closed', 'global')
    remote.migration = bindings.plan.staging.legacy.context.request.fencing.retirement.migration
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Never observe or write through another installation's transport.
    remote.migration = bindings.migration
    retained = (child / 'activation.json').read_bytes()
    for damage in ('receipt-bytes', 'receipt-loss'):
        remote.damage = damage
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert effects == [('open', None)]
        (child / 'activation.json').write_bytes(retained)
        (child / 'activation.json').chmod(0o600)
    remote.damage = None
    for loss in ('missing', 'roster', 'released-before-opening', 'recovery'):
        record = json.loads(retained)
        if loss == 'missing':
            (child / 'activation.json').unlink()
        else:
            if loss == 'roster':
                del record['guards'][roster[0]]
            elif loss == 'released-before-opening':
                record['guards'][roster[0]]['release'] = 'released'
            else:
                record['cancellation'] = 'intent'
            private_state._atomic_json(child / 'activation.json', record)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert effects == [('open', None)]
        (child / 'activation.json').write_bytes(retained)
        (child / 'activation.json').chmod(0o600)
    remote.mode = 'global'  # A later read settles the first opening.
    remote.qualified = False
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Global alone is insufficient authority.
    remote.qualified = True
    remote.ready = False
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Valid history alone cannot release unhealthy runtimes.
    remote.ready = True
    remote.failure = ('release', roster[0], 'before')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None), ('release', roster[0])]
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('global',)
    assert view.guard_states == {target.namespace: (('held', 'open') if str(target.participant_id) == roster[0] else ('held',))
        for target in bindings.migration.guards}
    remote.guards[roster[0]] = 'open'
    remote.failure = ('release', roster[1], 'after')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    assert set(remote.guards.values()) == {'open'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    module.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=True)
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('global',) and view.guard_states == {target.namespace: ('open',) for target in bindings.migration.guards}
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'shared_pool_handoff_complete'
    assert module.shared_forward_activation_view(bindings=bindings, state_dir=state).frame['outcome'] == 'shared_pool_handoff_complete'
