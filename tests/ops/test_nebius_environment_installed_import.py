"""Installed legacy snapshots use real import/readback/release consumers."""
from __future__ import annotations

import base64
import copy
import json
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_environment_foundation import reconcile_foundation
from scripts.ops.nebius_environment_import_cloud import ImportedCloudScopeV2
from scripts.ops.nebius_environment_installed_import import (
    ImportedEnvironmentRuntimeV2,
    InstalledLegacyFoundationV2,
    load_imported_environment_runtime,
    prepare_installed_legacy_foundation_import,
)
from scripts.ops.nebius_environment_legacy_foundation import NebiusFoundationCloud
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_environment_release import release_inputs as release_inputs
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_environment_foundation import ROOT, declaration
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2, render_foundation
from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.environment_management.credentials import generate_material
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError


def installed_source(platform_inputs, publication, cloud, *, kind='development', wanted=None):
    wanted = wanted or declaration(platform_inputs, kind=kind, backups=kind == 'production')
    config = wanted.platform_config
    config.update(project_id=cloud.config['project_id'], quota_parent_id=cloud.config['quota_parent_id'], region=cloud.config['region'])
    wanted = EnvironmentFoundationDeclarationV2.model_validate(wanted.model_dump() | {'platform_config_json': json.dumps(config)})
    subjects = {}
    for purpose, name in (('canonical', 'data'), ('source', 'source'), *((('backup', 'backup'),) if kind == 'production' else ())):
        if name == 'backup':
            for prefix in ('serviceaccount-', 'group-', 'aws-', 'bucket-'):
                cls, body = cloud.rows[prefix + 'source']
                cloud.rows[prefix + name] = cls, json.loads(json.dumps(body).replace('source', 'backup'))
            cloud.groups['serviceaccount-backup'] = ['group-backup']
            cloud.permits['group-backup'] = []
            cloud.material.update({'backup-access-key': 'aws-backup', 'backup-secret-key': 'private-backup-secret'})
        bucket_name = config['buckets']['artifacts' if purpose == 'canonical' else purpose]
        cloud.rows['bucket-' + name][1]['metadata']['name'] = bucket_name
        subjects[purpose] = dict(account_id='serviceaccount-' + name, group_id='group-' + name,
            key_id='accesskey-' + name, buckets={'bucket-' + name: bucket_name})
    scope = ImportedCloudScopeV2(tenant_id=cloud.scope['tenant_id'], region=cloud.scope['region'],
        compute_project_id=cloud.scope['compute_project_id'], object_project_id=cloud.scope['object_project_id'],
        database_disk_id='computedisk-test', subjects=subjects)
    foundation = render_foundation(wanted, platform_inputs[1], profile=platform_inputs[2], keyring={}, repo_root=ROOT)
    resources = {_key(doc): copy.deepcopy(doc) for group in foundation.files.values() for doc in group}
    material = generate_material(namespace=wanted.registration.application_namespace, tls_secret_name=config['db_tls_secret_name'])
    material['loom-platform-storage'] = cloud.material
    for name, value in material.items():
        doc = dict(apiVersion='v1', kind='Secret', metadata=dict(name=name, namespace=wanted.registration.application_namespace),
            type='Opaque', data={key: base64.b64encode(raw.encode()).decode() for key, raw in value.items()})
        resources[_key(doc)] = doc
    for doc in resources.values():
        doc['metadata']['uid'] = str(uuid4())
    pvc = resources['PersistentVolumeClaim:' + wanted.registration.application_namespace + ':data-loom-postgres-0']
    pvc['spec']['volumeName'] = 'pv-existing-data'
    volume = dict(apiVersion='v1', kind='PersistentVolume', metadata=dict(name='pv-existing-data', uid=str(uuid4())),
        spec=dict(claimRef=dict(namespace=wanted.registration.application_namespace, name=pvc['metadata']['name'],
            uid=pvc['metadata']['uid']), csi=dict(driver='disk.csi.nebius.ai', volumeHandle='computedisk-test')))
    return InstalledLegacyFoundationV2(schema_version='loom.installed-legacy-foundation.v2', declaration=wanted,
        publication=ProtectedPublication.model_validate(publication[0]), cloud_scope=scope, resources=resources, volume=volume)


class Readback:
    def __init__(self, source):
        self.rows, self.calls = {}, []
        from scripts.ops.nebius_development_runtime_live import runtime_resource_path

        from loom_service.environment_management.provider import ProvisioningContext
        from loom_service.environment_management.registry import OperationLease
        row = source.declaration.registration
        context = ProvisioningContext(OperationLease(uuid4(), row.environment_id, 1, 1, uuid4()),
            row.model_dump(mode='json'), source.declaration.platform_config, {})
        for doc in source.resources.values():
            if doc['kind'] in {'ClusterRole', 'ClusterRoleBinding', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
                path = runtime_resource_path(doc) + '/' + doc['metadata']['name']
            else:
                # This helper is pure and uses no client transport.
                _, path = KubernetesEnvironmentProvider._path(None, context, doc)
            self.rows[path] = copy.deepcopy(doc)
            if doc['kind'] == 'PersistentVolumeClaim':
                self.rows[path]['status'] = {'phase': 'Bound'}
        self.rows['/api/v1/persistentvolumes/' + source.volume['metadata']['name']] = copy.deepcopy(source.volume) | {'status': {'phase': 'Bound'}}
        self.rows['/api/v1/namespaces/kube-system'] = dict(apiVersion='v1', kind='Namespace',
            metadata=dict(name='kube-system', uid=str(source.declaration.kube_system_uid)))

    def http(self, request):
        self.calls.append((request.method, request.url.path))
        assert request.method == 'GET', 'import may not create, relabel, rotate or restart anything'
        row = self.rows.get(request.url.path)
        return httpx.Response(200, json=row) if row is not None else httpx.Response(404)


async def observe(directory, reference, source, cloud, remote, **kwargs):
    async with httpx.AsyncClient(base_url=source.declaration.platform_config['kubernetes_api_server'], transport=httpx.MockTransport(remote.http)) as http:
        return await reconcile_foundation(state_dir=directory, reference=reference,
            kubernetes=KubernetesEnvironmentProvider(http), cloud=NebiusFoundationCloud(None, clients={}, legacy_clients=cloud.clients), **kwargs)


@pytest.mark.parametrize('kind', ['development', 'staging', 'production'])
async def test_installed_import_reads_exact_credentials_data_and_cloud_for_all_classes(tmp_path, platform_inputs, publication, cloud, kind):
    source = installed_source(platform_inputs, publication, cloud, kind=kind)
    directory, operation_id = tmp_path / 'imported', uuid4()
    reference = prepare_installed_legacy_foundation_import(state_dir=directory, source=source, operation_id=operation_id,
        candidate=platform_inputs[1], profile=platform_inputs[2])
    remote = Readback(source)
    peer = '/api/v1/namespaces/peer-same-class'
    remote.rows[peer] = dict(apiVersion='v1', kind='Namespace', metadata=dict(name='peer-same-class', uid=str(uuid4())))
    original = copy.deepcopy(remote.rows)
    result = await observe(directory, reference, source, cloud, remote)
    assert result.phase == 'legacy_bindings_verified' and result.execution_enabled is None
    assert result.namespace_uids == {name: source.resources['Namespace:-:' + name]['metadata']['uid'] for name in source.declaration.registration.namespaces}
    assert remote.rows == original and ('GET', peer) not in remote.calls
    assert ('get', 'aws-backup') in cloud.calls if kind == 'production' else ('get', 'aws-backup') not in cloud.calls
    material = json.loads((directory / 'material.json').read_text())
    assert material['loom-platform-storage'] == cloud.material
    assert prepare_installed_legacy_foundation_import(state_dir=directory, source=source, operation_id=operation_id,
        candidate=platform_inputs[1], profile=platform_inputs[2]) == reference
    assert await observe(directory, reference, source, cloud, remote)


@pytest.mark.parametrize('damage', ['secret', 'pvc', 'pv', 'namespace', 'cloud-key', 'cloud-project', 'cloud-disk', 'extra-bucket', 'profile-authority'])
async def test_installed_import_refuses_replaced_or_expanded_authority(tmp_path, platform_inputs, publication, cloud, damage):
    source = installed_source(platform_inputs, publication, cloud)
    directory = tmp_path / 'imported'
    reference = prepare_installed_legacy_foundation_import(state_dir=directory, source=source, operation_id=uuid4(),
        candidate=platform_inputs[1], profile=platform_inputs[2])
    remote = Readback(source)
    await observe(directory, reference, source, cloud, remote)
    options = {}
    if damage in {'secret', 'pvc', 'pv', 'namespace'}:
        kind = dict(secret='Secret', pvc='PersistentVolumeClaim', pv='PersistentVolume', namespace='Namespace')[damage]
        next(doc for doc in remote.rows.values() if doc['kind'] == kind)['metadata']['uid'] = str(uuid4())
    elif damage == 'cloud-key':
        cloud.rows['aws-data'][1]['spec']['account']['service_account']['id'] = 'serviceaccount-other'
    elif damage == 'cloud-project':
        cloud.rows['serviceaccount-data'][1]['metadata']['parent_id'] = 'project-other'
    elif damage == 'cloud-disk':
        cloud.rows['computedisk-test'][1]['metadata']['parent_id'] = 'project-other'
    elif damage == 'extra-bucket':
        cls, doc = cloud.rows['bucket-data']
        doc = copy.deepcopy(doc)
        doc['metadata'].update(id='bucket-peer', name='peer-data')
        cloud.rows['bucket-peer'] = cls, doc
    else:
        key = 'ConfigMap:app-red:loom-platform-config'
        doc = copy.deepcopy(source.resources[key])
        doc['data']['config.json'] = '{}'
        options['_runtime_successors'] = {'k8s:' + key: (doc,)}
    with pytest.raises(ProviderBlockedError, match='legacy_foundation_observation_unqualified'):
        await observe(directory, reference, source, cloud, remote, **options)


@pytest.mark.parametrize('kind', ['development', 'staging', 'production'])
async def test_imported_runtime_is_a_release_predecessor_without_reenrollment(tmp_path, platform_inputs, publication, cloud, runtime_request, release_inputs, kind):
    from scripts.ops.nebius_environment_release_contracts import release_projection
    from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
    from scripts.ops.nebius_pool_administration_dispatch import (
        PoolAdministrationHostV2,
        PoolAdministrationScopeV2,
    )
    from scripts.ops.nebius_pool_administration_job import PoolAdministrationRuntimeV2
    from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2

    from loom.nebius_kubernetes import NebiusKubernetesConnection
    from loom.nebius_pool_contract import PoolParticipantV1

    inputs = (platform_inputs[0], release_inputs['previous'].candidate, release_inputs['previous'].profile)
    wanted = runtime_request['declaration']
    body, config = wanted.model_dump(mode='json'), wanted.platform_config
    body['registration']['kind'] = config['registration']['kind'] = config['environment'] = kind
    body.update(platform_config_json=json.dumps(config), backups_enabled=kind == 'production')
    wanted = EnvironmentFoundationDeclarationV2.model_validate(body)
    participant = PoolParticipantV1.model_validate(runtime_request['participant'].model_dump() | {
        'environment_class': kind, 'legacy_catalog_environment': None})
    release_inputs = copy.deepcopy(release_inputs)
    release_inputs['participant'] = participant
    for doc in release_inputs['resources'].values():
        if doc['kind'] == 'Deployment':
            for setting in doc['spec']['template']['spec']['containers'][0].get('env', []):
                if setting['name'] in {'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'}:
                    value = json.loads(setting['value'])
                    value['participant'] = participant.model_dump(mode='json')
                    value['environment'] = participant.catalog_environment
                    setting['value'] = json.dumps(value)
    source = installed_source(inputs, publication, cloud, wanted=wanted, kind=kind)
    resources = copy.deepcopy(source.resources)
    resources.update(release_inputs['resources'])
    for namespace in (participant.execution_namespace, participant.build_namespace):
        resources['Namespace:-:' + namespace.name]['metadata']['uid'] = str(namespace.uid)
    runtime = ImportedEnvironmentRuntimeV2(operation_id=uuid4(), participant=participant, profiles=runtime_request['profiles'],
        application_publication=source.publication, active_head=uuid4())
    source = source.model_copy(update={'resources': resources, 'runtime': runtime})
    directory = tmp_path / 'imported'
    reference = prepare_installed_legacy_foundation_import(state_dir=directory, source=source, operation_id=uuid4(),
        candidate=inputs[1], profile=inputs[2])
    remote = Readback(source)
    await observe(directory, reference, source, cloud, remote)
    binding = PlatformNamespaceBindingV2(installation_id=str(participant.installation_id), namespace='platform-services',
        namespace_uid=str(uuid4()), kube_system_uid=str(source.declaration.kube_system_uid))
    admin_runtime = PoolAdministrationRuntimeV2(binding=binding, service_image=inputs[1]['images']['service']['image_ref'],
        database_secret='platform-ledger', database_url_key='admin-url', database_ca_key='ca.crt',
        node_selector={'platform.example/role': 'control'}, tolerations=(), region=source.declaration.platform_config['region'])
    host = PoolAdministrationHostV2(runtime=admin_runtime, scope=PoolAdministrationScopeV2(installation_id=participant.installation_id,
        pool_id=participant.pool_id, participant_id=participant.participant_id, environment_id=participant.environment_id,
        incarnation=participant.incarnation, authority=participant.environment_class), state_root=tmp_path / 'operations',
        anchor_root=tmp_path / 'anchors', connection=NebiusKubernetesConnection(endpoint='https://api.cluster.test',
            ca_file=tmp_path / 'ca.pem', credentials_file=tmp_path / 'credentials.json'))
    policy = PoolEnrollmentPolicyV2(host=host, management_origin='https://platform.example.com', foundation_dir=directory,
        foundation=reference, application_publication=source.publication, manager_publication=source.publication,
        participant=participant, profiles=runtime.profiles, machines=(runtime_request['machine'],))
    adopted = load_imported_environment_runtime(policy=policy, operation_id=runtime.operation_id)
    assert adopted.staged.plan['expected_head'] == str(runtime.active_head)
    assert not (host.state_root / ('runtime-' + str(runtime.operation_id))).exists()
    release, targets = release_projection(**{**release_inputs, 'resources': adopted.resources})
    assert release.participant.binding_revision == participant.binding_revision + 1
    assert all(doc['metadata']['uid'] == resources[key]['metadata']['uid'] for key, doc in targets.items())
    for key, doc in targets.items():
        path = next(path for path, original in remote.rows.items() if _key(original) == key)
        remote.rows[path] = copy.deepcopy(doc)
    # Exact anchored release successors keep foundational data/credentials while
    # allowing only the publication profile and native-policy image literals.
    await observe(directory, reference, source, cloud, remote,
        _runtime_successors={'k8s:' + key: (doc,) for key, doc in targets.items()})
    with pytest.raises(ValueError, match='environment_import_runtime_policy_invalid'):
        load_imported_environment_runtime(policy=replace(policy, participant=participant.model_copy(update={'binding_revision': 2})),
            operation_id=runtime.operation_id)
