"""The fresh manager reserves application Pods, never a database per owner."""
from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import ssl
import zipfile
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_route import route as route
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_prerequisites import published_request
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_candidate_catalog import github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def capacity_checks(route, inventory, tmp_path):
    from scripts.ops.nebius_development_management_prerequisites import (
        HTTPSDevelopmentManagementPrerequisites,
    )

    request = route[1]
    credential = tmp_path / 'operator.json'
    credential.write_text('{}')
    credential.chmod(0o600)
    # This test owns only inventory sizing, not private input preparation. Actual
    # retained handoff and provider/publication checks have their own coverage.
    api = HTTPSDevelopmentManagementPrerequisites(settings=SimpleNamespace(),
        operator_cloud_credentials=credential, api_server=route[0].api_server,
        ssl_context=ssl.create_default_context(), token='operator')
    inventory['nodes'][0]['status']['allocatable'] = {'cpu': '128', 'memory': '256Gi',
        'ephemeral-storage': '512Gi', 'pods': '1000'}
    calls = []
    def handle(message):
        assert message.method == 'GET'
        resource = message.url.path.split('/')[-1]
        calls.append(resource)
        kinds = {'nodes': ('v1', 'Node'), 'pods': ('v1', 'Pod'), 'persistentvolumeclaims': ('v1', 'PersistentVolumeClaim'),
            'persistentvolumes': ('v1', 'PersistentVolume'), 'horizontalpodautoscalers': ('autoscaling/v2', 'HorizontalPodAutoscaler'),
            'replicationcontrollers': ('v1', 'ReplicationController'), 'deployments': ('apps/v1', 'Deployment'),
            'statefulsets': ('apps/v1', 'StatefulSet'), 'replicasets': ('apps/v1', 'ReplicaSet'),
            'daemonsets': ('apps/v1', 'DaemonSet'), 'jobs': ('batch/v1', 'Job'), 'cronjobs': ('batch/v1', 'CronJob')}
        version, kind = kinds[resource]
        return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
            'metadata': {'resourceVersion': '7'}, 'items': inventory.get(resource, []) if resource != 'pods' else []})
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handle))
    with api:
        yield api, request, inventory, calls


def test_fresh_manager_capacity_has_one_database_and_only_api_web_owner_reserve(capacity_checks, monkeypatch):
    from scripts.ops import nebius_development_management_prerequisites as module
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_capacity import qualify_platform_capacity

    api, request, _, calls = capacity_checks
    observed = []
    def fit(**values):
        observed.append(values)
        return qualify_platform_capacity(**values)
    monkeypatch.setattr(module, 'qualify_platform_capacity', fit)
    assert api.platform_capacity(request, render_installation(request)) == 10 * 1024
    values, = observed
    assert values['reserve'].storage_mib == 0 and values['reserve_pods'] > 0
    assert [(row['metadata']['namespace'], row['metadata']['name']) for row in values['planned']
        if row['kind'] == 'StatefulSet'] == [('loom-nebius-management-dev', 'loom-postgres')]
    assert 'persistentvolumeclaims' in calls


def test_capacity_without_an_eligible_platform_node_fails(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    api, request, inventory, _ = capacity_checks
    inventory['nodes'] = []
    with pytest.raises(ManagementCapacityError):
        api.platform_capacity(request, render_installation(request))


def test_other_pending_claims_still_charge_provider_headroom(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation

    api, request, inventory, _ = capacity_checks
    inventory['persistentvolumeclaims'] = [{'metadata': {'namespace': 'foreign', 'name': 'pending'},
        'spec': {'resources': {'requests': {'storage': '3Gi'}}}, 'status': {'phase': 'Pending'}}]
    assert api.platform_capacity(request, render_installation(request)) == 13 * 1024


def test_live_manager_claim_is_not_reserved_twice(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation

    api, request, inventory, _ = capacity_checks
    rendered = render_installation(request)
    stateful = next(row for row in rendered.files['20-database.yaml'] if row['kind'] == 'StatefulSet')
    inventory['statefulsets'] = [copy.deepcopy(stateful)]
    inventory['statefulsets'][0]['metadata']['uid'] = '20000000-0000-4000-8000-000000000001'
    inventory['persistentvolumeclaims'] = [{'metadata': {'namespace': request.binding.namespace, 'name': 'data-loom-postgres-0'},
        'spec': {'resources': {'requests': {'storage': '10Gi'}}}, 'status': {'phase': 'Bound', 'capacity': {'storage': '10Gi'}}}]
    assert api.platform_capacity(request, rendered) == 0


@pytest.mark.parametrize('blocked', [None, 'foundation', 'capacity', 'provider', 'backup', 'route'])
def test_connected_preflight_orders_every_barrier_and_never_masks_failure(capacity_checks, monkeypatch, blocked):
    from scripts.ops import nebius_development_management_prerequisites as module
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _ = capacity_checks
    rendered, retained, events = render_installation(request), object(), []
    def event(name):
        events.append(name)
        if blocked == name:
            raise RuntimeError('private-provider-payload')
    def foundation(selected):
        assert selected == request
        event('foundation')
        return retained
    def capacity(selected, manifests):
        assert selected == request and manifests == rendered
        event('capacity')
        return 10 * 1024
    async def provider(selected, stored, pending):
        assert selected == request and stored is retained and pending == 10 * 1024
        event('provider')
    def route(selected, *, installed):
        assert selected == request and not installed
        event('route')
    @contextmanager
    def backup(selected):
        assert selected == request
        def listing(**kwargs):
            assert kwargs == {'Bucket': request.deployment.backup_bucket, 'MaxKeys': 1}
            event('backup')
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}
        yield SimpleNamespace(list_objects_v2=listing)
    monkeypatch.setattr(api, 'foundation', foundation)
    monkeypatch.setattr(api, 'platform_capacity', capacity)
    monkeypatch.setattr(api, 'provider_and_publication', provider)
    monkeypatch.setattr(api, '_route', route)
    monkeypatch.setattr(module, 'backup_client', backup)
    sequence = ['foundation', 'capacity', 'provider', 'backup', 'route', 'foundation']
    if blocked is None:
        api.preflight(request, rendered)
        assert events == sequence
    else:
        with pytest.raises(ManagementInstallError) as error:
            api.preflight(request, rendered)
        assert 'private-provider-payload' not in str(error.value)
        assert events == sequence[:sequence.index(blocked) + 1]


@pytest.fixture
def provider_checks(capacity_checks, cloud, monkeypatch):
    import nebius.sdk
    from nebius.api.nebius.quotas import v1 as quotas
    from scripts.ops import nebius_development_management_prerequisites as module
    from scripts.ops.nebius_application_cloud_scope import ApplicationCloudScope
    from scripts.ops.nebius_development_cloud import DevelopmentCloudScope
    from scripts.ops.nebius_management_cloud_scope import ManagementBackupScope

    api, request, _, _ = capacity_checks
    config = request.deployment.installation.foundation.platform_config
    app = request.deployment.installation.applications
    raw = {**cloud.scope, 'tenant_id': config['quota_parent_id'], 'region': config['region'],
        'compute_project_id': config['project_id'], 'object_project_id': 'project-independent-data',
        'data_group_id': app.storage.data_group_id, 'source_group_id': app.storage.source_group_id,
        'data_buckets': {f'bucket-data-{i}': name for i, name in enumerate(sorted({config['buckets'][key]
            for key in ('artifacts', 'trajectories')}))}, 'source_bucket_name': config['buckets']['source']}
    scope = DevelopmentCloudScope.model_validate(raw)
    api.settings = SimpleNamespace(cloud=ApplicationCloudScope(
        tenant_id=scope.tenant_id, region=scope.region, shared_project_id=scope.object_project_id,
        provisioning_project_id=app.storage.project_id, provisioning_account_id='account-manager',
        provisioning_group_id='group-manager', provisioning_key_id='key-manager', membership_group_id='group-membership',
        data_group_id=scope.data_group_id, source_group_id=scope.source_group_id),
        backup=ManagementBackupScope(tenant_id=scope.tenant_id, region=scope.region,
            backup_project_id='project-backup', backup_account_id='account-backup', backup_group_id='group-backup',
            backup_bucket_id='bucket-backup', backup_key_id='key-backup'),
        backup_quota_name='storage-size', backup_quota_unit='byte')
    retained = SimpleNamespace(inputs=SimpleNamespace(config=config, settings=SimpleNamespace(cloud=scope)),
        phases={'supplied': {'resources': {'Secret:loom-platform-storage': {'desired': {'data': {
            key: base64.b64encode(value.encode()).decode() for key, value in cloud.material.items()}}}}}})
    events, change = [], {}
    async def publications(selected, http):
        assert selected == request
        events.append('publication')
    async def development(**kwargs):
        assert kwargs['scope'] == scope and kwargs['material'] == cloud.material
        assert kwargs['config'] == config and kwargs['pending_storage_mib'] == 10240
        events.append('development')
    async def application(**kwargs):
        assert kwargs['scope'].shared_project_id == 'project-independent-data'
        assert kwargs['data_buckets'] == scope.data_buckets
        assert kwargs['source_bucket'] == (scope.source_bucket_id, scope.source_bucket_name)
        assert kwargs['credentials_json'] == request.application_material.cloud_credentials_json
        events.append('application')
    async def backup(**kwargs):
        assert kwargs['material'] == request.material['loom-platform-storage']
        assert kwargs['bucket_name'] == request.deployment.backup_bucket
        assert kwargs['backup_bytes'] == 10 * 1024**3
        events.append('backup')
    async def quota(message, **kwargs):
        assert kwargs == {'timeout': 30, 'retries': 0}
        assert message.name == 'storage-size' and message.parent_id == scope.tenant_id and message.region == scope.region
        events.append('quota')
        row = {'metadata': {'id': 'quota-backup', 'parent_id': scope.tenant_id, 'name': 'storage-size'},
            'spec': {'limit': str(20 * 1024**3), 'region': scope.region}, 'status': {
                'state': 'STATE_ACTIVE', 'usage_state': 'USAGE_STATE_USED', 'usage': str(10 * 1024**3),
                'service': 'storage', 'unit': 'byte'}}
        if change.get('quota'):
            row['spec']['limit'] = '1'
        if change.get('identity'):
            api.operator_cloud_credentials.write_text('{"changed":true}')
        return quotas.QuotaAllowance.from_json(json.dumps(row))
    class SDK:
        def __init__(self, **kwargs):
            assert kwargs['credentials_file_name'] == str(api.operator_cloud_credentials)
            events.append('sdk')
        async def close(self):
            events.append('close')
    monkeypatch.setattr(api, 'publications', publications)
    monkeypatch.setattr(module, 'qualify_development_cloud', development)
    monkeypatch.setattr(module, 'qualify_application_cloud', application)
    monkeypatch.setattr(module, 'qualify_backup_material', backup)
    monkeypatch.setattr(nebius.sdk, 'SDK', SDK)
    monkeypatch.setattr(quotas, 'QuotaAllowanceServiceClient', lambda sdk: SimpleNamespace(get_by_name=quota))
    return api, request, retained, events, change


async def test_provider_preflight_binds_object_project_and_qualifies_all_three_authorities(provider_checks):
    api, request, retained, events, _ = provider_checks
    await api.provider_and_publication(request, retained, 10240)
    assert events == ['publication', 'sdk', 'development', 'application', 'backup', 'quota', 'close']


@pytest.mark.parametrize('drift', ['compute-project', 'data-group', 'backup-account', 'backup-bucket', 'quota', 'identity'])
async def test_provider_preflight_rejects_cross_binding_and_late_quota_or_identity_drift(provider_checks, drift):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, retained, events, change = provider_checks
    if drift == 'compute-project':
        api.settings.cloud = api.settings.cloud.model_copy(update={'shared_project_id': retained.inputs.config['project_id']})
    elif drift == 'data-group':
        api.settings.cloud = api.settings.cloud.model_copy(update={'data_group_id': 'group-other'})
    elif drift == 'backup-account':
        api.settings.backup = api.settings.backup.model_copy(update={'backup_account_id': api.settings.cloud.provisioning_account_id})
    elif drift == 'backup-bucket':
        api.settings.backup = api.settings.backup.model_copy(update={'backup_bucket_id': retained.inputs.settings.cloud.source_bucket_id})
    else:
        change[drift] = True
    with pytest.raises(ManagementInstallError):
        await api.provider_and_publication(request, retained, 10240)
    if drift not in {'quota', 'identity'}:
        assert 'sdk' not in events
    else:
        assert events[-1] == 'close'


@pytest.mark.parametrize('drift', [None, 'source', 'image', 'schema', 'failed-check', 'expired-artifact'])
async def test_manager_and_personal_release_use_authenticated_publication_bytes(capacity_checks, publication, drift):
    from scripts.ops.nebius_management_install import ManagementInstallError

    from loom_service.application_management.installation import ApplicationInstallation
    from loom_service.environment_management.registry import ManagementError

    api, request, _, _ = capacity_checks
    reference, responses, payload, keyring, candidate = copy.deepcopy(publication)
    candidate['source_archive_sha256'] = 'sha256:' + 'f' * 64
    buffer = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(payload)) as source, zipfile.ZipFile(buffer, 'w') as target:
        for name in source.namelist():
            target.writestr(name, json.dumps(candidate).encode() if name == 'candidate.json' else source.read(name))
    payload = buffer.getvalue()
    reference['artifact_sha256'] = 'sha256:' + hashlib.sha256(payload).hexdigest()
    responses['actions/artifacts/123']['digest'] = reference['artifact_sha256']
    request = published_request((request, None), (reference, responses, payload, keyring, candidate))
    app = request.deployment.installation.applications.model_dump(mode='json')
    release = {'release_id': str(reference['candidate_id']), 'source_digest': 'sha256:' + 'f' * 64,
        'schema_revision': app['shared']['schema_revision'],
        'service_image_ref': candidate['images']['service']['image_ref'], 'web_image_ref': candidate['images']['web']['image_ref']}
    if drift == 'source':
        release['source_digest'] = 'sha256:' + 'e' * 64
    elif drift == 'image':
        release['web_image_ref'] = release['web_image_ref'].replace('@sha256:', '-other@sha256:')
    elif drift == 'schema':
        release['schema_revision'] = 'incompatible'
    elif drift == 'failed-check':
        responses['commits/' + 'b' * 40 + '/check-runs']['check_runs'][0]['conclusion'] = 'failure'
    elif drift == 'expired-artifact':
        responses['actions/artifacts/123']['expired'] = True
    app['releases'] = [release]
    deployment = request.deployment.model_copy(update={'installation': request.deployment.installation.model_copy(
        update={'applications': ApplicationInstallation.model_validate(app)})})
    request = replace(request, deployment=deployment)
    api.settings = SimpleNamespace(candidate_id=reference['candidate_id'])
    async with httpx.AsyncClient(transport=github_transport(responses, payload), trust_env=False) as http:
        if drift is None:
            await api.publications(request, http)
        else:
            with pytest.raises((ManagementInstallError, ManagementError)):
                await api.publications(request, http)
