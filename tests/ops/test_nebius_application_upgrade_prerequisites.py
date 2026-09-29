"""Installed upgrade checks the shared app footprint and retained data inputs."""
from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import ssl
import zipfile
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_install import installation as installation
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
def shared_checks(setup_request, application_material, inventory, tmp_path):
    from scripts.ops.nebius_application_upgrade_prerequisites import (
        ApplicationUpgradePrerequisites,
        UpgradePrerequisiteSettings,
    )
    from scripts.ops.nebius_management_prerequisites import HTTPSManagementPrerequisites

    setup = replace(setup_request[0], material=application_material)
    application = setup.deployment.installation.applications
    config = setup.deployment.installation.foundation.platform_config
    namespace = application.shared.platform_namespace
    uids = {key: str(uuid4()) for key in ('config', 'database', 'auth', 'service')}
    def obj(kind, name, key, **values):
        return {'apiVersion': 'apps/v1' if kind == 'Deployment' else 'v1', 'kind': kind,
            'metadata': {'name': name, 'namespace': namespace, 'uid': uids[key]}, **values}
    rows = {
        'loom-platform-config': obj('ConfigMap', 'loom-platform-config', 'config', data={
            'environment.json': json.dumps(config), 'profile.json': application.shared.runtime_profile_json,
            'keyring.json': json.dumps(setup.deployment.installation.keyring)}),
        'loom-platform-db': obj('Secret', 'loom-platform-db', 'database', data={key: base64.b64encode(value.encode()).decode()
            for key, value in {'ca.crt': application_material.ca_pem,
                'admin-url': f'postgresql://postgres:private-admin@loom-postgres.{namespace}.svc:5432/loom?sslmode=verify-full&sslrootcert=/var/run/loom-db/ca.crt'}.items()}),
        'loom-platform-auth': obj('Secret', 'loom-platform-auth', 'auth', data={
            'secret-store-master-key': base64.b64encode(application_material.secret_store_master_keys.encode()).decode()}),
        'loom-service': obj('Deployment', 'loom-service', 'service', spec={'replicas': 1, 'template': {'spec': {'containers': [{
            'name': 'loom-service', 'env': [{'name': 'LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON',
                'value': application.shared.runtime_profile_json}]}]}}})}
    base = HTTPSManagementPrerequisites(settings=SimpleNamespace(), ingress=SimpleNamespace(),
        certificate_config={}, ingress_state=tmp_path, operator_cloud_credentials=tmp_path / 'operator.json',
        api_server=application.runtime.kubernetes.endpoint, ssl_context=ssl.create_default_context())
    base.client.close()
    calls = []
    def http(request):
        calls.append(request)
        assert request.method == 'GET'
        name = request.url.path.split('/')[-1]
        return httpx.Response(200, json=rows[name])
    base.client = httpx.Client(base_url=base.api_server, transport=httpx.MockTransport(http))
    settings = UpgradePrerequisiteSettings(candidate_id=uuid4(), shared_config_uid=uids['config'],
        shared_database_uid=uids['database'], shared_auth_uid=uids['auth'], shared_service_uid=uids['service'],
        bucket_ids={key: 'bucket-' + key for key in ('artifacts', 'trajectories', 'source')}, cloud={
            'tenant_id': config['quota_parent_id'], 'region': config['region'],
            'provisioning_project_id': application.storage.project_id,
            'provisioning_account_id': 'account-manager', 'provisioning_key_id': 'key-manager',
            'provisioning_group_id': 'group-manager', 'membership_group_id': 'group-membership',
            'shared_project_id': config['project_id'], 'data_group_id': application.storage.data_group_id,
            'source_group_id': application.storage.source_group_id})
    # Only shared checks use these fields; retained original history is covered
    # by the composed upgrade tests before this concrete prerequisite is called.
    request = SimpleNamespace(setup=setup, original=SimpleNamespace(material={'loom-management-publications': {'token': 'private-token'}}))
    client = ApplicationUpgradePrerequisites(base=base, settings=settings)
    yield client, request, rows, calls, inventory
    base.client.close()


def test_shared_material_matches_actual_development_consumers_without_writes(shared_checks):
    client, request, _, calls, _ = shared_checks
    client.shared_material(request)
    assert len(calls) == 4 and all('/namespaces/loom-nebius-platform/' in str(call.url) for call in calls)


@pytest.mark.parametrize('drift', ['namespace', 'uid', 'ca', 'keyring', 'database', 'environment', 'profile', 'consumer_profile', 'guest_target'])
def test_shared_drift_cannot_become_application_credentials(shared_checks, drift):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    client, request, rows, _, _ = shared_checks
    if drift in {'namespace', 'uid'}:
        rows['loom-platform-db']['metadata'][drift] = str(uuid4())
    elif drift in {'ca', 'database'}:
        rows['loom-platform-db']['data']['ca.crt' if drift == 'ca' else 'admin-url'] = base64.b64encode(b'private-invalid').decode()
    elif drift == 'keyring':
        rows['loom-platform-auth']['data']['secret-store-master-key'] = base64.b64encode(b'private-other').decode()
    elif drift == 'consumer_profile':
        rows['loom-service']['spec']['template']['spec']['containers'][0]['env'][0]['value'] = '{}'
    elif drift == 'guest_target':
        config = json.loads(rows['loom-platform-config']['data']['environment.json'])
        config['guest_execution_target'] = {'target_id': 'nebius-guest-not-selected'}
        rows['loom-platform-config']['data']['environment.json'] = json.dumps(config)
    else:
        rows['loom-platform-config']['data'][{'environment': 'environment.json', 'profile': 'profile.json'}[drift]] = '{}'
    with pytest.raises(ManagementPrerequisiteError) as error:
        client.shared_material(request)
    assert 'private-' not in str(error.value)


def test_upgrade_fit_reserves_only_personal_api_web_and_no_new_persistent_storage(shared_checks, monkeypatch):
    from scripts.ops import nebius_application_upgrade_prerequisites as target
    from scripts.ops.nebius_management_capacity import qualify_platform_capacity

    client, request, _, _, inventory = shared_checks
    inventory['nodes'][0]['status']['allocatable'] = {'cpu': '128', 'memory': '256Gi', 'ephemeral-storage': '512Gi', 'pods': '1000'}
    client.base.inventory = lambda api, resource, kind: copy.deepcopy(inventory['nodes'] if kind == 'Node' else [])
    observed = []
    def fit(**values):
        observed.append(values)
        return qualify_platform_capacity(**values)
    monkeypatch.setattr(target, 'qualify_platform_capacity', fit)
    client.platform_capacity(request)
    values, = observed
    assert values['reserve'].storage_mib == 0
    assert values['reserve_pods'] > 0
    assert not any(row['kind'] == 'StatefulSet' for row in values['planned'])
    assert {row['kind'] for row in values['planned']} == {'Deployment', 'Job'}
    assert {row['metadata']['namespace'] for row in values['planned']} == {
        request.setup.binding.namespace, request.setup.deployment.installation.applications.shared.platform_namespace}


def test_upgrade_fit_cannot_report_capacity_without_an_eligible_platform_node(shared_checks):
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    client, request, _, _, _ = shared_checks
    client.base.inventory = lambda *args: []
    with pytest.raises(ManagementCapacityError):
        client.platform_capacity(request)


@pytest.mark.parametrize('failure', [None, 'shared_material', 'platform_capacity', 'provider', 'public_route'])
def test_connected_preflight_requires_every_proof_in_order(shared_checks, monkeypatch, failure):
    from scripts.ops.nebius_management_install import ManagementInstallRequest
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    client, request, _, _, _ = shared_checks
    setup = request.setup
    request.original = ManagementInstallRequest(binding=setup.binding, deployment=setup.deployment,
        candidate=setup.candidate, profile=setup.profile, material=request.original.material)
    calls = []
    def check(name):
        def perform(selected):
            calls.append(name)
            if name == failure:
                raise RuntimeError('private-provider-diagnostic')
        return perform
    async def provider(selected):
        check('provider')(selected)
    monkeypatch.setattr(client.base, 'foundation', check('foundation'))
    monkeypatch.setattr(client, 'shared_material', check('shared_material'))
    monkeypatch.setattr(client, 'platform_capacity', check('platform_capacity'))
    monkeypatch.setattr(client, 'provider_and_publication', provider, raising=False)
    monkeypatch.setattr(client.base, 'public_route', check('public_route'))
    sequence = ['foundation', 'shared_material', 'platform_capacity', 'provider', 'public_route']
    if failure:
        with pytest.raises(ManagementPrerequisiteError) as error:
            client.preflight(request)
        assert 'private-provider' not in str(error.value)
        assert calls == sequence[:sequence.index(failure) + 1]
    else:
        client.preflight(request)
        assert calls == sequence and client.diagnostic_stage is None


@pytest.mark.parametrize('change', [None, 'missing_source', 'wrong_source', 'wrong_image', 'wrong_schema'])
async def test_upgrade_releases_require_real_source_and_image_publication(installation, publication,
        application_management_inputs, change):
    from scripts.ops.nebius_application_upgrade_prerequisites import ApplicationUpgradePrerequisites
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    from loom_service.application_management.installation import ApplicationInstallation

    reference, responses, payload, keyring, document = copy.deepcopy(publication)
    document['source_archive_sha256'] = 'sha256:' + 'f' * 64
    if change == 'missing_source':
        document.pop('source_archive_sha256')
    buffer = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(payload)) as source, zipfile.ZipFile(buffer, 'w') as target:
        for name in source.namelist():
            target.writestr(name, json.dumps(document).encode() if name == 'candidate.json' else source.read(name))
    payload = buffer.getvalue()
    reference['artifact_sha256'] = 'sha256:' + hashlib.sha256(payload).hexdigest()
    responses['actions/artifacts/123']['digest'] = reference['artifact_sha256']
    selected = (reference, responses, payload, keyring, document)
    old = published_request(installation, selected)
    application = copy.deepcopy(application_management_inputs[0]['installation']['applications'])
    release = {'release_id': str(reference['candidate_id']), 'source_digest': 'sha256:' + 'f' * 64,
        'schema_revision': application['shared']['schema_revision'],
        'service_image_ref': document['images']['service']['image_ref'],
        'web_image_ref': document['images']['web']['image_ref']}
    if change == 'wrong_source':
        release['source_digest'] = 'sha256:' + 'e' * 64
    elif change == 'wrong_image':
        release['web_image_ref'] = release['web_image_ref'].replace('@sha256:', '-other@sha256:')
    elif change == 'wrong_schema':
        release['schema_revision'] = 'incompatible'
    application['releases'] = [release]
    deployment = old.deployment.model_copy(update={'installation': old.deployment.installation.model_copy(update={
        'provider_runtime': None, 'applications': ApplicationInstallation.model_validate(application)})})
    request = SimpleNamespace(original=old, setup=SimpleNamespace(deployment=deployment, candidate=old.candidate, profile=old.profile))
    checks = ApplicationUpgradePrerequisites(base=None, settings=SimpleNamespace(candidate_id=reference['candidate_id']))
    async with httpx.AsyncClient(transport=github_transport(responses, payload), trust_env=False) as http:
        if change is None:
            await checks.publications(request, http)
        else:
            with pytest.raises(ManagementPrerequisiteError):
                await checks.publications(request, http)
