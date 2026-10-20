"""Generic process qualification executes the actual image settings readers."""
from __future__ import annotations

import copy
import importlib
import json
import os
import ssl
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import certifi
import pytest
from tests.ops.test_nebius_development_runtime_readiness import running as running
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_runtime_settings import settings_case as settings_case
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


@pytest.mark.parametrize('settings_case', ['controller', 'actuator', 'service', 'manager', 'gateway'], indirect=True)
@pytest.mark.parametrize('damage', [None, 'settings', 'database', 'restart', 'template', 'missing_reply'])
def test_generic_process_probe_binds_actual_configuration_and_current_process(settings_case, running, tmp_path, damage):
    module = 'scripts.ops.nebius_environment_runtime_probes'
    assert importlib.util.find_spec(module), 'independent runtime process qualification missing'
    probes = importlib.import_module(module)
    component, target, environment, options, _, _, _ = settings_case
    current, collections, replica, pod = running
    current['metadata'].update(name=target['metadata']['name'], namespace='app-blue')
    current['spec'] = copy.deepcopy(target['spec'])
    current['spec']['replicas'] = 1
    labels = {**current['spec']['template']['metadata']['labels'], 'pod-template-hash': 'abc123'}
    replica['metadata'].update(namespace='app-blue', labels=labels)
    replica['metadata']['ownerReferences'][0].update(name=current['metadata']['name'])
    replica['spec'].update(selector={'matchLabels': {**current['spec']['selector']['matchLabels'], 'pod-template-hash': 'abc123'}},
        template=copy.deepcopy(current['spec']['template']))
    replica['spec']['template']['metadata']['labels'] = labels
    pod['metadata'].update(copy.deepcopy(current['spec']['template']['metadata']))
    pod['metadata'].update(namespace='app-blue', labels=labels)
    pod['spec'] = copy.deepcopy(current['spec']['template']['spec'])
    pod['status']['containerStatuses'][0]['name'] = pod['spec']['containers'][0]['name']
    pod['status']['initContainerStatuses'] = [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
        for row in pod['spec'].get('initContainers', [])]
    wanted = copy.deepcopy(current)
    db_url = 'postgresql+psycopg://reader:private-db-marker@loom-postgres.app-blue.svc:5432/loom'
    db_variable = {'controller': 'LOOM_CP_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL',
        'service': 'LOOM_SVC_DB_URL', 'manager': 'LOOM_SVC_DB_URL', 'gateway': 'LOOM_POOL_GATEWAY_DB_URL'}[component]
    environment[db_variable] = db_url
    if damage == 'settings':
        if component == 'service':
            environment['LOOM_SVC_SERVICE_MODE'] = 'management'
        elif component == 'manager':
            environment['LOOM_SVC_SERVICE_MODE'] = 'application'
        elif component == 'gateway':
            environment['LOOM_POOL_GATEWAY_MACHINE_ID'] = str(uuid4())
        else:
            variable = 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON' if component == 'controller' else 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'
            pool = json.loads(environment[variable])
            pool['management_origin'] = 'https://foreign.example.com'
            environment[variable] = json.dumps(pool)
    elif damage == 'database':
        environment[db_variable] = db_url.replace('app-blue.svc', 'staging.svc')
    elif damage == 'template':
        pod['spec']['containers'][0]['image'] = 'foreign/image'
    reads, executions = [], []

    def read(path):
        reads.append(path)
        assert '/namespaces/app-blue/' in path
        if '/deployments/' in path:
            return copy.deepcopy(current)
        return copy.deepcopy(collections['sets' if '/replicasets?' in path else 'pods'])

    def execute(actual_pod, container, command):
        assert actual_pod['metadata']['uid'] == pod['metadata']['uid']
        assert container == current['spec']['template']['spec']['containers'][0]['name']
        assert command[0:2] == ['python', '-c'] and len(command) == 6
        executions.append(command)
        result = subprocess.run([sys.executable, *command[1:]], env=environment, cwd=tmp_path,
            capture_output=True, timeout=30, check=False)
        if result.returncode or result.stderr:
            raise ValueError('remote process refused')
        if damage == 'restart':
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        return {} if damage == 'missing_reply' else json.loads(result.stdout)

    subject = probes.EnvironmentRuntimeProbes(read=read, execute=execute, qualify=lambda: None, region='eu-north1')
    def observe():
        return subject.qualify_process(workload=wanted, component=component, database_namespace='app-blue',
            database_url=db_url, **options)
    if damage is None:
        result = observe()
        assert result['pod_uid'] == pod['metadata']['uid']
        assert result['deployment_uid'] == current['metadata']['uid']
        assert result['component'] == component and len(executions) == 2
        assert 'private-' not in json.dumps(result)
        subject.recheck_process(workload=wanted, proof=result)
        # Name reuse never makes another process the retained one.
        current['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError, match='environment_runtime_process_unqualified'):
            observe()
    else:
        with pytest.raises(ValueError, match='environment_runtime_process_unqualified') as error:
            observe()
        assert 'private-' not in str(error.value)
    assert reads


@pytest.mark.parametrize('damage', [None, 'command', 'connection', 'error', 'oversized'])
def test_fixed_process_transport_uses_only_private_explicit_connection_and_cleans_it(monkeypatch, damage):
    from scripts.ops import nebius_environment_runtime_readiness as readiness
    from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND

    monkeypatch.setattr(readiness.shutil, 'which', lambda name, path: '/usr/bin/kubectl')
    executed = []
    def run(command, **kwargs):
        executed.append(command)
        assert kwargs['env'] == {'PATH': os.defpath, 'LANG': 'C.UTF-8'}
        assert kwargs['timeout'] == 40 and kwargs['check'] is False
        config_path = Path(command[command.index('--kubeconfig') + 1])
        assert config_path.stat().st_mode & 0o777 == 0o600
        config = json.loads(config_path.read_text())
        assert config['clusters'][0]['cluster']['server'] == 'https://selected.cluster.test'
        assert config['users'][0]['user'] == {'token': 'private-transport-fixture'}
        assert len(config['clusters']) == len(config['users']) == len(config['contexts']) == 1
        assert command[command.index('exec') + 1:command.index('--')] == ['-n', 'work-blue', 'pod/reader-pod', '-c', 'reader']
        return SimpleNamespace(returncode=1 if damage == 'error' else 0,
            stderr=b'private-failure-marker' if damage == 'error' else b'',
            stdout=b'x' * 65537 if damage == 'oversized' else b'{"status":"qualified"}')
    monkeypatch.setattr(readiness.subprocess, 'run', run)
    transport = readiness.FixedProcessTransport(endpoint='https://selected.cluster.test', trust=ssl.create_default_context(cafile=certifi.where()),
        token='private-transport-fixture', qualify=lambda: None)
    directory = transport.path.parent
    try:
        command = ['python', '-c', _BOUND_DATABASE_COMMAND, 'actuator', 'a' * 64, 'b' * 64]
        if damage == 'command':
            command[2] = 'print("arbitrary command")'
        elif damage == 'connection':
            transport.path.write_text('{}')
        pod = {'metadata': {'name': 'reader-pod', 'namespace': 'work-blue', 'uid': str(uuid4())}}
        if damage is None:
            assert transport.execute(pod, 'reader', command) == {'status': 'qualified'}
            assert len(executed) == 1
        else:
            with pytest.raises(ValueError, match='environment_runtime_probe_transport_unqualified') as error:
                transport.execute(pod, 'reader', command)
            assert 'private-' not in str(error.value)
            if damage in {'command', 'connection'}:
                assert not executed
    finally:
        transport.close()
    assert not directory.exists()
