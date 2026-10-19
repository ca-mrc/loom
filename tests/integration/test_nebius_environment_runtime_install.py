"""Closed delivery consumes real enrollment and source evidence before staging."""
from __future__ import annotations

import asyncio
import copy
import importlib
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_administration_dispatch import _load

from loom_service.pool_management.administration import administer
from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_nebius_enrollment_issuer import prepared
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.ops.test_nebius_development_build_cloud import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud
from tests.ops.test_nebius_development_runtime_readiness import running as running
from tests.support.nebius_phase_api import PhaseAPI
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication


@pytest.mark.parametrize('committed_enrollment', [False, True])
@pytest.mark.timeout(180)
async def test_installation_stages_only_enrolled_owner_closed_and_recovers_exact_effects(
        sessions, tmp_path, platform_inputs, publication, publisher_cloud, committed_enrollment, running, monkeypatch):
    name = 'scripts.ops.nebius_environment_runtime_install'
    assert importlib.util.find_spec(name), 'protected independent runtime installer is missing'
    module = importlib.import_module(name)
    cloud = publisher_cloud
    inputs = copy.deepcopy(platform_inputs)
    inputs[0].update(project_id=cloud.scope['project_id'], quota_parent_id=cloud.scope['tenant_id'], region=cloud.scope['region'])
    async with prepared(sessions, tmp_path, inputs, publication) as (issuer, policy, clients, remote, api):
        profiles = policy.profiles.model_dump(mode='json')
        profiles['task_images'][0]['settings']['registry_repository'] = cloud.repositories[0]
        # This fixture has no authorized cache identity; it tests canonical
        # source + registry delivery, not arbitrary shared-cache credentials.
        profiles['task_images'][0]['settings'].update(cache_bucket=None, cache_secret_name=None)
        profiles = PoolProfileCatalog.model_validate(profiles)
        token = 'runtime-participant-' + 'a' * 40
        import hashlib
        machines = tuple(row.model_copy(update={'token_sha256': hashlib.sha256(token.encode()).hexdigest()}) for row in policy.machines)
        policy = replace(policy, profiles=profiles, machines=machines)
        enrollment = await issuer.issue_enrollment(policy=policy, operation_id=uuid4(), **clients)
        job = _load(policy.host, enrollment)
        if committed_enrollment:
            async with sessions.begin() as session:
                await administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')
        class Resources(PhaseAPI):
            key = staticmethod(_key)

        resources = Resources(policy.host.runtime.binding)
        resources.failure = 'after'  # Every lost create reply must be read back, not retried.
        @contextmanager
        def resource_connection(documents, namespaces, qualify):
            assert set(namespaces) == {'app-red', 'work-red', 'build-red', policy.host.runtime.binding.namespace}
            qualify()
            yield resources
            qualify()
        kwargs = dict(policy=policy, enrollment=enrollment, operation_id=uuid4(), token=token,
            registry_scope=DevelopmentRegistryCloudScope.model_validate(cloud.scope), registry_credential=cloud.credential,
            registry_sdk=None, registry_clients=cloud.clients, resource_connection=resource_connection, **clients)
        if not committed_enrollment:
            with pytest.raises(ValueError, match='environment_runtime_install_unqualified'):
                await module.stage_environment_runtime(**kwargs)
            assert not resources.creates
            return
        result = await module.stage_environment_runtime(**kwargs)
        assert result['status'] == 'runtime_staged_closed'
        assert result['participant_id'] == str(policy.participant.participant_id)
        assert result['admission_open'] is False
        retained = module.load_staged_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        assert retained.plan['workloads']['service']['metadata']['namespace'] == 'app-red'
        assert retained.plan['workloads']['control_plane']['spec']['replicas'] == 0
        assert all(doc['metadata'].get('uid') for doc in retained.resources.values())
        assert retained.plan['foundation_identities']['k8s:Namespace:-:work-red'] == str(policy.participant.execution_namespace.uid)
        originals = copy.deepcopy(remote.writes)
        creates = list(resources.creates)
        assert await module.stage_environment_runtime(**kwargs) == result
        assert resources.creates == creates and remote.writes == originals
        deployments = [doc for doc in resources.resources.values() if doc['kind'] == 'Deployment']
        assert len(deployments) == 1 and deployments[0]['spec']['replicas'] == 0
        assert deployments[0]['metadata']['namespace'] == 'work-red'
        assert not any(doc['kind'] in {'Namespace', 'ResourceQuota', 'Job', 'CronJob'} for doc in resources.resources.values())
        # An expired read-only observation is replaceable, but the original
        # enrollment/mutation authority and runtime create intents are not.
        api.pending = True
        assert await module.stage_environment_runtime(**kwargs) is None
        observations = policy.host.state_root / ('runtime-' + str(kwargs['operation_id'])) / 'observations'
        previous = json.loads((observations / 'current.json').read_text())
        inspection = observations / previous['operation_id'] / 'inspection.json'
        pending = json.loads(inspection.read_text())
        pending['authorization']['issued_at'] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
        pending['authorization']['expires_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        inspection.write_text(json.dumps(pending))
        api.pending = False
        assert await module.stage_environment_runtime(**kwargs) == result
        assert json.loads((observations / 'current.json').read_text())['operation_id'] != previous['operation_id']
        assert _load(policy.host, enrollment) == job
        assert resources.creates == creates and remote.writes == originals
        phase = policy.host.state_root / ('runtime-' + str(kwargs['operation_id'])) / 'material' / 'stage.json'
        saved = phase.read_bytes()
        phase.unlink()
        with pytest.raises(ValueError, match='environment_runtime_retained_unqualified'):
            module.load_staged_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        with pytest.raises(ValueError, match='environment_runtime_install_unqualified'):
            await module.stage_environment_runtime(**kwargs)
        assert resources.creates == creates
        phase.write_bytes(saved)
        phase.chmod(0o600)

        # Continue through the actual startup journal, not a second enrollment
        # or restaging operation. Only remote controller/HTTP effects are fake.
        startup = importlib.import_module('scripts.ops.nebius_environment_runtime_start')
        from scripts.ops.nebius_environment_runtime_start import runtime_resource_path

        from tests.ops.test_nebius_development_runtime_transition import RuntimeAPI

        for doc in resources.resources.values():
            remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']] = copy.deepcopy(doc)
        changes = []
        reject = True

        class Workloads(RuntimeAPI):
            def patch_workload(self, key, before, desired):
                try:
                    return super().patch_workload(key, before, desired)
                finally:
                    actual = self.rows[key]
                    remote.kubernetes[runtime_resource_path(actual) + '/' + actual['metadata']['name']] = actual
                    changes.append(key)

        @contextmanager
        def workload_connection(before, targets, namespaces, qualify):
            api = Workloads({key: remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']]
                for key, doc in before.items()})
            api.ready, api.preview_rejected, api.failure = True, reject, 'after'
            api.qualify = qualify
            yield api

        inputs = dict(policy=policy, operation_id=kwargs['operation_id'], workload_connection=workload_connection, **clients)
        selected_path = '/apis/apps/v1/namespaces/app-red/deployments/loom-service'
        original_service = copy.deepcopy(remote.kubernetes[selected_path])
        reads = 0
        def drift_after_foundation_read(request):
            nonlocal reads
            if request.method == 'GET' and request.url.path == selected_path:
                reads += 1
                if reads == 3:
                    remote.kubernetes[selected_path]['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
            return remote.http(request)
        async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(drift_after_foundation_read)) as changed:
            from loom_service.environment_management.kubernetes_provider import (
                KubernetesEnvironmentProvider,
            )

            with pytest.raises(ValueError, match='environment_runtime_start_unqualified'):
                await startup.start_environment_runtime(**{**inputs, 'kubernetes': KubernetesEnvironmentProvider(changed)})
        assert changes == [], 'same-UID drift cannot become a newly authorized original'
        remote.kubernetes[selected_path] = original_service
        assert (await startup.start_environment_runtime(**inputs))['status'] == 'runtime_start_pending'
        assert changes == []
        reject = False
        started = await startup.start_environment_runtime(**inputs)
        assert started['status'] == 'runtime_started_closed'
        assert started['admission_open'] is False
        assert len(changes) == 8
        assert all(key.split(':')[1] in {'app-red', 'work-red'} for key in changes)
        assert await startup.start_environment_runtime(**inputs) == started
        assert len(changes) == 8, 'completed phases must not stop or replace the new runtime again'
        for name in ('loom-service', 'loom-control-plane'):
            assert remote.kubernetes[f'/apis/apps/v1/namespaces/app-red/deployments/{name}']['spec']['replicas'] == 1
        quota = remote.kubernetes['/api/v1/namespaces/work-red/resourcequotas/loom-execution-disabled']
        assert quota['spec']['hard'] == {'pods': '1', 'count/jobs.batch': '0'}
        assert remote.kubernetes['/api/v1/namespaces/build-red/resourcequotas/loom-execution-disabled']['spec']['hard'] == {'pods': '0'}
        # Read-only successors must come from all anchored completed phases,
        # not a caller template or merely the presence of running workloads.
        assert hasattr(startup, 'load_started_environment_runtime'), 'completed startup evidence reader missing'
        current = startup.load_started_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        assert current.resources['Deployment:app-red:loom-service']['spec']['replicas'] == 1
        assert current.resources['Deployment:work-red:loom-execution-actuator']['spec']['replicas'] == 1
        assert current.resources['ResourceQuota:work-red:loom-execution-disabled']['spec']['hard']['count/jobs.batch'] == '0'
        child = retained.state_dir / 'start' / 'replace' / 'transition.json'
        saved = child.read_bytes()
        child.unlink()
        with pytest.raises(ValueError, match='environment_runtime_started_unqualified'):
            startup.load_started_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        child.write_bytes(saved)
        child.chmod(0o600)
        assert startup.load_started_environment_runtime(policy=policy, operation_id=kwargs['operation_id']).resources == current.resources
        assert len(changes) == 8

        # Continue to actual process probes. Only kubectl/remote processes are
        # doubled here; fixed commands also run against real settings in ops tests.
        from scripts.ops.nebius_environment_runtime_probes import EnvironmentRuntimeProbes
        from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
        from scripts.ops.nebius_pool_runtime_settings import BOUND_POOL_SETTINGS_COMMAND

        readiness_name = 'scripts.ops.nebius_environment_runtime_readiness'
        assert importlib.util.find_spec(readiness_name), 'connected runtime observation missing'
        readiness = importlib.import_module(readiness_name)
        for doc in current.resources.values():
            if doc['kind'] != 'Deployment':
                continue
            doc = remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']]
            namespace, name = doc['metadata']['namespace'], doc['metadata']['name']
            doc['metadata'].setdefault('generation', 1)
            doc['status'] = {'observedGeneration': doc['metadata']['generation'],
                'replicas': 1, 'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}
            labels = {**doc['spec']['template']['metadata']['labels'], 'pod-template-hash': 'abc123'}
            replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': name + '-abc123',
                'namespace': namespace, 'uid': str(uuid4()), 'generation': 1, 'resourceVersion': '9', 'labels': labels,
                'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': name,
                    'uid': doc['metadata']['uid'], 'controller': True}]},
                'spec': {'replicas': 1, 'selector': {'matchLabels': {**doc['spec']['selector']['matchLabels'], 'pod-template-hash': 'abc123'}},
                    'template': copy.deepcopy(doc['spec']['template'])},
                'status': {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
            replica['spec']['template']['metadata']['labels'] = labels
            pod = copy.deepcopy(doc['spec']['template'])
            pod.update(apiVersion='v1', kind='Pod')
            pod['metadata'].update(name=name + '-abc123-pod', namespace=namespace, uid=str(uuid4()), labels=labels,
                ownerReferences=[{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
                    'uid': replica['metadata']['uid'], 'controller': True}])
            pod['status'] = {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                'containerStatuses': [{'name': row['name'], 'ready': True, 'restartCount': 0, 'state': {'running': {}}}
                    for row in pod['spec']['containers']],
                'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
                    for row in pod['spec'].get('initContainers', [])]}
            for version, kind, suffix, item in (('apps/v1', 'ReplicaSet', 'replicasets', replica), ('v1', 'Pod', 'pods', pod)):
                path = ('/api/v1' if version == 'v1' else '/apis/apps/v1') + '/namespaces/' + namespace + '/' + suffix
                remote.kubernetes.setdefault(path, {'apiVersion': version, 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '9'}, 'items': []})['items'].append(item)

        def runtime_http(request):
            response = remote.http(request)
            selector = request.url.params.get('labelSelector')
            if selector and response.status_code == 200 and response.json().get('kind') == 'PodList':
                value = response.json()
                key, expected = selector.split('=')
                value['items'] = [row for row in value['items'] if row['metadata'].get('labels', {}).get(key) == expected]
                return httpx.Response(200, json=value)
            return response

        commands = []
        reject_process = False
        late_restart = False
        @contextmanager
        def probe_connection(qualify, region):
            def read(path):
                qualify()
                response = runtime_http(httpx.Request('GET', 'https://api.cluster.test' + path))
                assert response.status_code == 200
                return response.json()
            def execute(pod, container, command):
                assert pod['metadata']['namespace'] in {'app-red', 'work-red'}
                assert command[2] in {BOUND_POOL_SETTINGS_COMMAND, _BOUND_DATABASE_COMMAND}
                assert command[3] in {'service', 'controller', 'actuator'}
                assert 'private-' not in ' '.join(command)
                commands.append(command[3])
                if late_restart and command[3] == 'service':
                    actuator_pod, = remote.kubernetes['/api/v1/namespaces/work-red/pods']['items']
                    actuator_pod['status']['containerStatuses'][0]['restartCount'] += 1
                return {} if reject_process else {'status': 'qualified'}
            yield EnvironmentRuntimeProbes(read=read, execute=execute, qualify=qualify, region=region)

        async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(runtime_http)) as http:
            observe_inputs = {**clients, 'kubernetes': KubernetesEnvironmentProvider(http),
                'policy': policy, 'operation_id': kwargs['operation_id'], 'probe_connection': probe_connection}
            observed = await readiness.observe_environment_runtime(**observe_inputs)
            assert observed['status'] == 'runtime_processes_verified_closed' and observed['admission_open'] is False
            assert observed['participant_id'] == str(policy.participant.participant_id)
            assert sorted(commands) == ['actuator', 'actuator', 'controller', 'controller', 'service', 'service']
            assert len(changes) == 8
            reject_process = True
            with pytest.raises(ValueError, match='environment_runtime_readiness_unqualified'):
                await readiness.observe_environment_runtime(**observe_inputs)
            assert len(changes) == 8

            reject_process, late_restart = False, False
            assert hasattr(readiness, 'observe_environment_pre_activation'), 'connected platform/environment readiness missing'
            from tests.integration.test_nebius_platform_runtime_readiness import (
                prepared_platform_runtime,
            )

            platform_dir = tmp_path / 'platform'
            platform_dir.mkdir(mode=0o700)
            platform = await prepared_platform_runtime(sessions, platform_dir, running, existing=(api.platform_input, policy.host))
            late_application_restart = False
            @contextmanager
            def platform_probe_connection(qualify, region):
                def execute(pod, container, command):
                    if late_application_restart and command[3] == 'gateway':
                        actuator_pod, = remote.kubernetes['/api/v1/namespaces/work-red/pods']['items']
                        actuator_pod['status']['containerStatuses'][0]['restartCount'] += 1
                    return platform.probes.execute(pod, container, command)
                yield EnvironmentRuntimeProbes(read=platform.probes.read, execute=execute, qualify=qualify, region=region)
            final_inputs = {**observe_inputs, 'platform_policy': platform.policy, 'platform_probe_connection': platform_probe_connection}
            ready = await readiness.observe_environment_pre_activation(**final_inputs)
            assert ready['status'] == 'preactivation_runtime_verified_closed'
            assert ready['admission_open'] is False and ready['participant_id'] == str(policy.participant.participant_id)
            assert ready['platform']['platform_operation_id'] == str(platform.operation.operation_id)
            assert api.request.request.require_capacity is True
            assert api.request.request.platform_authorization_id == platform.policy.authority.authorization_id
            assert len(changes) == 8, 'preactivation readback must not open quotas or restart readers'
            late_application_restart = True
            with pytest.raises(ValueError, match='environment_preactivation_unqualified'):
                await readiness.observe_environment_pre_activation(**final_inputs)
            assert len(changes) == 8
            reject_process, late_restart = False, True
            # A later component's probe must not leave the earlier actuator's
            # successful, but superseded, process evidence in the ready bundle.
            with pytest.raises(ValueError, match='environment_runtime_readiness_unqualified'):
                await readiness.observe_environment_runtime(**observe_inputs)
            assert len(changes) == 8

            # Keep the same real enrollment/start/process/platform chain. Opening
            # selected Kubernetes boundaries never grants participant admission.
            reject_process = late_restart = late_application_restart = False
            boundaries = importlib.import_module('scripts.ops.nebius_environment_runtime_boundaries')
            assert hasattr(boundaries, 'prepare_environment_boundaries'), 'connected selected-boundary preparation missing'
            allow_quotas = False
            inventory_effects = []
            class Boundaries(Workloads):
                def policy_ready(self):
                    return allow_quotas or remote.kubernetes['/api/v1/namespaces/build-red']['metadata']['labels'][
                        'pod-security.kubernetes.io/enforce'] == 'restricted'
                def check_inventory(self):
                    # Actual inventory transport is separately covered at the
                    # HTTP boundary; inject remote drift during this later IO.
                    if inventory_effects:
                        inventory_effects.pop(0)()
            @contextmanager
            def boundary_connection(before, targets, namespaces, qualify):
                transport = Boundaries({key: remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']]
                    for key, doc in before.items()})
                transport.ready, transport.failure = True, 'after'
                transport.preview_rejected = not allow_quotas and any(doc['kind'] == 'ResourceQuota' for doc in targets.values())
                transport.qualify = qualify
                yield transport
            boundary_inputs = {**final_inputs, 'boundary_connection': boundary_connection}
            pending = await boundaries.prepare_environment_boundaries(**boundary_inputs)
            assert pending['status'] == 'runtime_boundaries_pending' and pending['admission_open'] is False
            assert pending['phase'] == 'build-psa', 'failed post-CAS policy proof must prevent every quota release'
            assert len(changes) == 9 and changes[-1] == 'Namespace:-:build-red'
            assert remote.kubernetes['/api/v1/namespaces/build-red']['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'privileged'
            assert remote.kubernetes['/api/v1/namespaces/build-red/resourcequotas/loom-execution-disabled']['spec']['hard'] == {'pods': '0'}
            # Anchored partial successors must be observable on recovery without
            # rewriting original startup or foundation evidence.
            allow_quotas = True
            opened = await boundaries.prepare_environment_boundaries(**boundary_inputs)
            assert opened['status'] == 'runtime_boundaries_prepared_closed' and opened['admission_open'] is False
            assert len(changes) == 11
            assert set(changes[-2:]) == {'ResourceQuota:work-red:loom-execution-disabled', 'ResourceQuota:build-red:loom-execution-disabled'}
            for space in ('work-red', 'build-red'):
                assert remote.kubernetes[f'/api/v1/namespaces/{space}/resourcequotas/loom-execution-disabled']['spec'] == {}
            assert (await boundaries.prepare_environment_boundaries(**boundary_inputs))['status'] == 'runtime_boundaries_prepared_closed'
            assert len(changes) == 11, 'lost-reply and completed recovery must never repeat PATCH'
            assert startup.load_started_environment_runtime(policy=policy, operation_id=kwargs['operation_id']).resources == current.resources
            child = retained.state_dir / 'boundaries' / 'build-psa' / 'transition.json'
            saved = child.read_bytes()
            child.unlink()
            with pytest.raises(ValueError, match='environment_boundary_history_unqualified'):
                boundaries.load_environment_boundary_progress(policy=policy, started=current)
            child.write_bytes(saved)
            child.chmod(0o600)
            assert boundaries.load_environment_boundary_progress(policy=policy, started=current).cursor == {
                'phases': {'build-psa': 'complete', 'worker-quota': 'complete', 'build-quota': 'complete'}}
            activation_name = 'scripts.ops.nebius_environment_activation_issuer'
            assert importlib.util.find_spec(activation_name), 'scoped readiness/activation issuer missing'
            activation = importlib.import_module(activation_name)
            from scripts.ops.nebius_pool_administration_dispatch import dispatch_administration

            from tests.integration.test_nebius_pool_administration_dispatch import command

            # Issuance must not retain formerly healthy processes or resources
            # when they change during the later native-policy/inventory checks.
            actuator_pod, = remote.kubernetes['/api/v1/namespaces/work-red/pods']['items']
            platform_pod = platform.collections['pods']['items'][0]
            platform_database = platform.live['Secret:' + policy.host.runtime.binding.namespace + ':' + policy.host.runtime.database_secret]
            for target, field, value in (
                    (actuator_pod['status']['containerStatuses'][0], 'restartCount', 99),
                    (platform_pod['status']['containerStatuses'][0], 'restartCount', 99),
                    (platform_database['data'], 'service-url', 'Zm9yZWlnbg==')):
                original = target[field]
                inventory_effects.append(lambda target=target, field=field, value=value: target.__setitem__(field, value))
                transition_id = uuid4()
                with pytest.raises(ValueError, match='environment_activation_issuance_unqualified'):
                    await activation.issue_environment_activation(**final_inputs, transition_id=transition_id,
                        action='ready', predecessor=None, boundary_connection=boundary_connection)
                assert not inventory_effects
                assert not (retained.state_dir / ('activation-' + str(transition_id)) / 'issued.json').exists()
                target[field] = original

            predecessor = None
            issued = []
            class MutationAPI(PhaseAPI):
                def administration_report(self, state_dir):
                    return {'job_uid': str(uuid4()), 'pod_uid': str(uuid4()), 'receipt': self.committed}
            for action, expected_state in (('ready', 'ready'), ('activate', 'active')):
                transition_id = uuid4()
                issuing = dict(**final_inputs, transition_id=transition_id, action=action, predecessor=predecessor,
                    boundary_connection=boundary_connection)
                if action == 'ready':
                    allow_quotas = False
                    assert await activation.issue_environment_activation(**issuing) is None
                    assert not (retained.state_dir / ('activation-' + str(transition_id)) / 'issued.json').exists()
                    allow_quotas = True
                reference = await activation.issue_environment_activation(**issuing)
                assert reference.participant_id == policy.participant.participant_id
                operation = _load(policy.host, reference)
                assert operation.request.expected_head == (enrollment.operation_id if predecessor is None else predecessor.operation_id)
                assert operation.request.action == action and operation.request.readiness is not None
                async with sessions.begin() as session:
                    committed = await administer(session, operation.request.model_dump_json().encode(), operation.authorization, action='apply')
                mutation_api = MutationAPI(policy.host.runtime.binding)
                mutation_api.committed = committed
                @contextmanager
                def mutation_connection(request, operation=operation, mutation_api=mutation_api):
                    assert request.request == operation.request and request.authorization == operation.authorization
                    yield mutation_api
                applied = await asyncio.to_thread(dispatch_administration, command(reference, 'apply'),
                    action='apply', host=policy.host, connect=mutation_connection)
                assert applied['status'] == 'complete'
                assert applied['proof']['receipt']['receipt']['state'] == expected_state
                assert (await asyncio.to_thread(dispatch_administration, command(reference, 'recover'),
                    action='recover', host=policy.host, connect=mutation_connection))['status'] == 'complete'
                assert len(mutation_api.creates) == 2
                assert await activation.issue_environment_activation(**issuing) == reference
                issued.append((issuing, reference))
                predecessor = reference
            # The earlier ready intent stays immutable even after activation.
            assert await activation.issue_environment_activation(**issued[0][0]) == issued[0][1]
            assert len(changes) == 11, 'issuing admission operations cannot modify Kubernetes boundaries again'
            issuance_path = retained.state_dir / ('activation-' + str(issued[0][0]['transition_id'])) / 'issued.json'
            saved = issuance_path.read_bytes()
            class Later(datetime):
                @classmethod
                def now(cls, tz=None):
                    return datetime.now(tz) + timedelta(hours=2)
            with monkeypatch.context() as clock:
                clock.setattr(activation, 'datetime', Later)
                assert await activation.issue_environment_activation(**issued[0][0]) == issued[0][1]
            assert issuance_path.read_bytes() == saved, 'historical grant recovery must not renew issuance or readiness'
            damaged = json.loads(saved)
            damaged['request']['expected_head'] = str(uuid4())
            issuance_path.write_text(json.dumps(damaged))
            with pytest.raises(ValueError, match='environment_activation_issuance_unqualified'):
                await activation.issue_environment_activation(**issued[0][0])
            issuance_path.write_bytes(saved)
