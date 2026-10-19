"""Fixed generic Jobs run the real administration process against disposable SQL."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
import ssl
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from tests.integration.test_nebius_pool_administration import administration, authorization
from tests.integration.test_nebius_pool_participant_lifecycle import setup
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.nebius_phase_api import PhaseAPI


def jobs():
    name = 'scripts.ops.nebius_pool_administration_job'
    assert importlib.util.find_spec(name), 'generic protected administration Job adapter is missing'
    return importlib.import_module(name)


async def inputs(sessions):
    module = jobs()
    _, spec, _, _, _, change = await setup(sessions)
    selected = spec.participants[2]
    operation = await change(selected, 'suspend', apply=False)
    grant = authorization(administration(), selected, operation)
    binding = module.PlatformNamespaceBindingV2(installation_id=str(selected.installation_id),
        namespace='platform-services-east', namespace_uid=str(uuid4()), kube_system_uid=str(uuid4()))
    runtime = module.PoolAdministrationRuntimeV2(binding=binding,
        service_image='registry.example/loom-service@sha256:' + 'e' * 64,
        database_secret='platform-ledger-credentials', database_url_key='admin-url', database_ca_key='ca.crt',
        node_selector={'platform.example/role': 'control'}, tolerations=(), region='eu-north1')
    return module.PoolAdministrationJobV2(runtime=runtime, request=operation, authorization=grant,
        action='apply'), selected, change


async def test_fixed_render_executes_scoped_operation_and_fresh_status_without_legacy_source(sessions, tmp_path):
    module = jobs()
    current, selected, change = await inputs(sessions)
    api = PhaseAPI(current.runtime.binding)
    reports = []
    for action in ('apply', 'status'):
        item = replace(current, action=action, observation_id=uuid4() if action == 'status' else None)
        documents = module.administration_documents(item)
        config, = (row for row in documents.values() if row['kind'] == 'ConfigMap')
        job, = (row for row in documents.values() if row['kind'] == 'Job')
        pod = job['spec']['template']['spec']
        container, = pod['containers']
        assert pod['nodeSelector'] == {'platform.example/role': 'control'}
        assert pod['automountServiceAccountToken'] is False
        assert job['spec']['backoffLimit'] == 0
        assert {row['metadata']['namespace'] for row in documents.values()} == {'platform-services-east'}
        env = dict(os.environ)
        for name, content in config['data'].items():
            (tmp_path / name).write_text(content)
        for row in container['env']:
            if 'valueFrom' in row:
                assert row['valueFrom'] == {'secretKeyRef': {'name': 'platform-ledger-credentials', 'key': 'admin-url'}}
                env[row['name']] = sessions.kw['bind'].url.render_as_string(hide_password=False)
            elif row['value'].startswith('/var/run/loom-pool-administration/'):
                env[row['name']] = str(tmp_path / row['value'].rsplit('/', 1)[-1])
            else:
                env[row['name']] = row['value']
        assert env['LOOM_POOL_ADMINISTRATION_AUTHORIZATION_SHA256'] == hashlib.sha256(
            (tmp_path / 'authorization.json').read_bytes()).hexdigest()
        command = container['command']
        assert command == ['python', '-m', 'loom_service.pool_management.administration']
        completed = subprocess.run([sys.executable, *command[1:]], env=env, capture_output=True, text=True, timeout=30)
        assert completed.returncode == 0, completed.stdout
        result = json.loads(completed.stdout)
        module.validate_administration_report(item, result)
        assert result['receipt']['state'] == result['current']['state'] == 'suspended'
        reports.append(result)
        first = module.stage_administration_job(request=item, api=api, state_dir=tmp_path / 'journal')
        assert module.stage_administration_job(request=item, api=api, state_dir=tmp_path / 'journal') == first
    assert len(api.creates) == 3, 'apply and a fresh read share one exact immutable input'
    await change(selected, 'resume', expected_head=current.request.operation_id)
    async with sessions() as session:
        assert (await session.get(NebiusPoolParticipantLifecycle, selected.participant_id)).state == 'active'
    # The old report remains historical; it is never relabelled as current readiness.
    assert reports[0]['current']['state'] == 'suspended'


@pytest.mark.parametrize('failure', ['before', 'after'])
async def test_uncertain_create_retains_intent_and_never_resubmits(sessions, tmp_path, failure):
    module = jobs()
    current, _, _ = await inputs(sessions)

    class API(PhaseAPI):
        def create_resource(self, doc):
            self.failure = failure if doc['kind'] == 'Job' else None
            super().create_resource(doc)

    api = API(current.runtime.binding)
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ValueError, match='preserve'):
                module.stage_administration_job(request=current, api=api, state_dir=tmp_path)
        else:
            module.stage_administration_job(request=current, api=api, state_dir=tmp_path)
    assert len(api.creates) == 2


@pytest.mark.parametrize('damage', ['grant', 'installation', 'status_id', 'apply_id', 'image', 'namespace', 'report_target', 'report_extra'])
async def test_changed_input_or_private_report_is_refused(sessions, damage):
    module = jobs()
    current, _, _ = await inputs(sessions)
    if damage == 'grant':
        current = replace(current, authorization=current.authorization.model_copy(update={'request_sha256': 'f' * 64}))
    elif damage == 'installation':
        current = replace(current, runtime=replace(current.runtime,
            binding=replace(current.runtime.binding, installation_id=str(uuid4()))))
    elif damage == 'status_id':
        current = replace(current, action='status')
    elif damage == 'apply_id':
        current = replace(current, observation_id=uuid4())
    elif damage == 'image':
        current = replace(current, runtime=replace(current.runtime, service_image='image:latest'))
    elif damage == 'namespace':
        with pytest.raises(ValueError):
            replace(current.runtime.binding, namespace='../foreign')
        return
    else:
        report = {'schema_version': 'loom.pool-administration-result.v2',
            'authorization_id': str(current.authorization.authorization_id),
            'request_sha256': current.authorization.request_sha256, 'receipt': None, 'current': None}
        if damage == 'report_target':
            report['authorization_id'] = str(uuid4())
        else:
            report['private'] = 'must-not-leak'
        with pytest.raises(ValueError):
            module.validate_administration_report(current, report)
        return
    with pytest.raises(ValueError):
        module.administration_documents(current)


@pytest.mark.parametrize('damage', ['state', 'binding', 'generation', 'current_absent'])
async def test_report_must_describe_the_exact_completed_transition(sessions, damage):
    module = jobs()
    current, _, _ = await inputs(sessions)
    async with sessions.begin() as session:
        report = await administration().administer(session, current.request.model_dump_json().encode(),
            current.authorization, action='apply')
    if damage == 'state':
        report['receipt']['state'] = 'active'
    elif damage == 'binding':
        report['receipt']['binding_sha256'] = 'f' * 64
    elif damage == 'generation':
        report['receipt']['generation'] = True
    else:
        report['current'] = None
    with pytest.raises(ValueError):
        module.validate_administration_report(current, report)


async def test_https_readback_requires_recorded_job_pod_and_database_commit(sessions, tmp_path):
    module = jobs()
    current, _, _ = await inputs(sessions)
    binding = current.runtime.binding
    external = PhaseAPI(binding)
    module.stage_administration_job(request=current, api=external, state_dir=tmp_path)
    job, = (row for row in external.resources.values() if row['kind'] == 'Job')
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    uid, name = job['metadata']['uid'], job['metadata']['name']
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {**copy.deepcopy(job['spec']['template']['metadata']),
        'namespace': binding.namespace, 'name': name + '-abc', 'uid': str(uuid4()),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': uid, 'controller': True}]},
        'spec': copy.deepcopy(job['spec']['template']['spec']), 'status': {'phase': 'Succeeded',
            'containerStatuses': [{'name': 'register', 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}]}}
    async with sessions.begin() as session:
        report = await administration().administer(session, current.request.model_dump_json().encode(),
            current.authorization, action='apply')

    def respond(message):
        assert message.method == 'GET', 'commit readback may not mutate or retry effects'
        path = message.url.path
        if path in {'/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + binding.namespace}:
            ns = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': ns, 'uid': binding.kube_system_uid if ns == 'kube-system' else binding.namespace_uid,
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if '/configmaps/' in path or '/jobs/' in path:
            kind = 'ConfigMap' if '/configmaps/' in path else 'Job'
            return httpx.Response(200, json=external.resources[kind + ':' + path.rsplit('/', 1)[-1]])
        if path.endswith('/pods'):
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {}, 'items': [pod]})
        if path.endswith('/log'):
            return httpx.Response(200, json=report)
        return httpx.Response(200, json=pod)

    api = module.HTTPSPoolAdministrationJobAPI(request=current, api_server='https://cluster.example', ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(respond))
    with api:
        assert api.administration_report(tmp_path) == {'job_uid': uid, 'pod_uid': pod['metadata']['uid'], 'receipt': report}
        pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
        with pytest.raises(RuntimeError):
            api.administration_report(tmp_path)
