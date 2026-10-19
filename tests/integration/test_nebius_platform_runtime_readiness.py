"""Platform readiness binds real committed authority and current runtime reads."""
from __future__ import annotations

import base64
import copy
import importlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from scripts.ops.nebius_environment_runtime_probes import EnvironmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
from scripts.ops.nebius_pool_administration_dispatch import (
    PoolAdministrationHostV2,
    PoolAdministrationScopeV2,
    retain_authorized_operation,
)
from scripts.ops.nebius_pool_administration_job import PoolAdministrationRuntimeV2
from sqlalchemy import text

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolLifecycleOperation
from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_service.pool_management.administration import (
    PoolAdministrationAuthorizationV2,
    administer,
)
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection
from tests.integration.test_nebius_pool_platform_lifecycle import apply, prepare
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.ops.test_nebius_development_runtime_readiness import running as running


@pytest.mark.parametrize('damage', [None, 'unpinned_authority', 'database', 'namespace', 'restart', 'late-secret'])
async def test_platform_runtime_uses_its_own_authority_without_environment_history(sessions, tmp_path, running, damage):
    name = 'scripts.ops.nebius_platform_runtime_readiness'
    assert importlib.util.find_spec(name), 'independent platform readiness consumer missing'
    subject = importlib.import_module(name)
    _, spec, installed, authority, capture, opening = await prepare(sessions)
    await capture()
    operation = opening(installed.operation_id, authority)
    committed = await apply(sessions, operation)
    await capture()
    async with sessions() as session:
        audit = await session.get(NebiusPoolLifecycleOperation, UUID(committed['authorization_id']))
        grant = PoolAdministrationAuthorizationV2.model_validate(audit.request_json['authorization'])
    namespace = 'platform-east'
    binding = PlatformNamespaceBindingV2(installation_id=str(spec.installation_id), namespace=namespace,
        namespace_uid=str(uuid4()), kube_system_uid=str(uuid4()))
    image = 'registry.example/service@sha256:' + 'e' * 64
    host = PoolAdministrationHostV2(runtime=PoolAdministrationRuntimeV2(binding=binding, service_image=image,
        database_secret='platform-db', database_url_key='admin-url', database_ca_key='ca.crt',
        node_selector={'example.com/role': 'system'}, tolerations=(), region='eu-north1'),
        scope=PoolAdministrationScopeV2(installation_id=spec.installation_id, pool_id=spec.pool_id, authority='platform'),
        state_root=tmp_path / 'operations', anchor_root=tmp_path / 'anchors',
        connection=NebiusKubernetesConnection(endpoint='https://cluster.example', ca_file=tmp_path / 'ca.pem',
            credentials_file=tmp_path / 'credentials.json'))
    reference = retain_authorized_operation(host=host, request=operation, authorization=grant, evidence_sha256='a' * 64)
    request = PoolInspectionV2(schema_version='loom.pool-inspection.v2', operation_id=uuid4(),
        installation_id=spec.installation_id, pool_id=spec.pool_id, require_capacity=True,
        platform_authorization_id=grant.authorization_id)
    now = datetime.now(UTC)
    read_grant = grant.model_copy(update={'authorization_id': uuid4(), 'request_sha256': digest(request.model_dump(mode='json')),
        'issued_at': now, 'expires_at': now + timedelta(minutes=5)})
    async with sessions.begin() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        snapshot = qualify_inspection(await administer(session, request.model_dump_json().encode(), read_grant, action='status'),
            request, read_grant.authorization_id)

    resources, live, collections = {}, {}, {}
    def add(kind, name, **extra):
        row = {'apiVersion': 'v1', 'kind': kind, 'metadata': {'name': name, 'namespace': namespace, 'uid': str(uuid4())}, **extra}
        resources[_key(row)] = row
        return row
    url = f'postgresql+psycopg://reader:private-fixture@loom-postgres.{namespace}.svc:5432/loom'
    add('Secret', 'platform-db', data={key: base64.b64encode(url.encode()).decode() for key in ('service-url', 'admin-url')})
    gateway_machine, = (row for row in spec.machines if row.role == 'gateway')
    add('Secret', 'gateway-token', data={'token': base64.b64encode(b'private-gateway-fixture').decode()})
    catalog = add('ConfigMap', 'platform-profiles', data={'profiles.json': spec.profiles.model_dump_json()})
    commands = []
    for component in ('manager', 'gateway'):
        controller, _, replica, pod = copy.deepcopy(running)
        labels = {'app': component}
        controller['metadata'].update(name=component, namespace=namespace, uid=str(uuid4()))
        controller['spec']['selector']['matchLabels'] = labels
        template = controller['spec']['template']
        template['metadata']['labels'] = labels
        container, = template['spec']['containers']
        container.update(name=component, image=image)
        if component == 'gateway':
            container.update(command=['python', '-m', 'loom_service.pool_management'],
                ports=[{'name': 'health', 'containerPort': 9120}],
                readinessProbe={'httpGet': {'path': '/readyz', 'port': 'health'}})
        values = {'LOOM_SVC_SERVICE_MODE': 'management', 'LOOM_SVC_POOL_PROFILES_FILE': '/profiles/profiles.json'} if component == 'manager' else {
            'LOOM_POOL_GATEWAY_POOL_ID': str(spec.pool_id), 'LOOM_POOL_GATEWAY_INSTALLATION_ID': str(spec.installation_id),
            'LOOM_POOL_GATEWAY_MACHINE_ID': str(gateway_machine.machine_id), 'LOOM_POOL_GATEWAY_ADMISSION_EPOCH': str(spec.admission_epoch),
            'LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE': '/token/token', 'LOOM_POOL_GATEWAY_KUBERNETES': json.dumps({
                'kind': 'projected_service_account', 'endpoint': host.connection.endpoint,
                'ca_file': '/kube/ca.crt', 'token_file': '/kube/token'})}
        container['env'] = [{'name': key, 'value': value} for key, value in values.items()]
        container['env'].append({'name': 'LOOM_SVC_DB_URL' if component == 'manager' else 'LOOM_POOL_GATEWAY_DB_URL',
            'valueFrom': {'secretKeyRef': {'name': 'platform-db', 'key': 'service-url'}}})
        hashed = {**labels, 'pod-template-hash': 'abc123'}
        replica['metadata'].update(name=component + '-rs', namespace=namespace, uid=str(uuid4()), labels=hashed,
            ownerReferences=[{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': component,
                'uid': controller['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}])
        replica['spec'].update(selector={'matchLabels': hashed}, template=copy.deepcopy(template))
        replica['spec']['template']['metadata']['labels'] = hashed
        pod['metadata'].update(name=component + '-pod', namespace=namespace, uid=str(uuid4()), labels=hashed,
            ownerReferences=[{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
                'uid': replica['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}])
        pod['spec'] = copy.deepcopy(template['spec'])
        pod['status']['containerStatuses'][0]['name'] = component
        resources[_key(controller)] = controller
        for kind, suffix, row in (('ReplicaSet', 'replicasets', replica), ('Pod', 'pods', pod)):
            collections.setdefault(suffix, {'apiVersion': 'v1' if kind == 'Pod' else 'apps/v1',
                'kind': kind + 'List', 'metadata': {'resourceVersion': '9'}, 'items': []})['items'].append(row)
    live = copy.deepcopy(resources)
    gateway_after_commands = 0
    def read(path):
        nonlocal gateway_after_commands
        if path.endswith('/deployments/gateway') and len(commands) == 4:
            gateway_after_commands += 1
            if damage == 'late-secret' and gateway_after_commands == 4:
                live['Secret:' + namespace + ':platform-db']['data']['service-url'] = base64.b64encode(
                    url.replace(namespace, 'foreign').encode()).decode()
        if path in {'/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + namespace}:
            selected = path.rsplit('/', 1)[1]
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': selected,
                'uid': str(uuid4()) if damage == 'namespace' else (binding.kube_system_uid if selected == 'kube-system' else binding.namespace_uid)}}
        if '?' in path:
            return copy.deepcopy(collections[path.split('?')[0].rsplit('/', 1)[1]])
        kind = {'deployments': 'Deployment', 'secrets': 'Secret', 'configmaps': 'ConfigMap'}[path.split('/')[-2]]
        return copy.deepcopy(live[kind + ':' + namespace + ':' + path.rsplit('/', 1)[1]])
    def execute(pod, container, command):
        commands.append(command[3])
        if damage == 'restart' and command[3] == 'gateway':
            collections['pods']['items'][0]['status']['containerStatuses'][0]['restartCount'] += 1
        return {'status': 'qualified'}
    probes = EnvironmentRuntimeProbes(read=read, execute=execute, qualify=lambda: None, region='eu-north1')
    policy = subject.PlatformRuntimePolicyV2(host=host, authority=reference, resources=resources,
        manager_key='Deployment:' + namespace + ':manager', gateway_key='Deployment:' + namespace + ':gateway',
        gateway_token_key='Secret:' + namespace + ':gateway-token', catalog_key=_key(catalog))
    if damage == 'database':
        live['Secret:' + namespace + ':platform-db']['data']['service-url'] = base64.b64encode(url.replace(namespace, 'foreign').encode()).decode()
    elif damage == 'unpinned_authority':
        (host.anchor_root / (str(reference.authorization_id) + '.json')).unlink()
    if damage:
        with pytest.raises(ValueError, match='platform_runtime_unqualified'):
            subject.observe_platform_runtime(policy=policy, snapshot=snapshot, probes=probes)
    else:
        result = subject.observe_platform_runtime(policy=policy, snapshot=snapshot, probes=probes)
        assert result['status'] == 'platform_runtime_verified'
        assert result['platform_operation_id'] == str(operation.operation_id)
        assert [row['component'] for row in result['processes']] == ['manager', 'gateway']
        assert commands == ['manager', 'service', 'gateway', 'gateway']
        assert 'private-' not in json.dumps(result)
