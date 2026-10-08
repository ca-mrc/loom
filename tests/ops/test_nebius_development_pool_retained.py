"""Runtime successors consume closed pool evidence without replaying installation."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import json
import ssl
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from tests.ops.test_nebius_development_build_cloud import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud
from tests.ops.test_nebius_development_management_foundation import (
    development_inputs as original_development_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_management_foundation import entry as entry
from tests.ops.test_nebius_development_management_foundation import handoff as handoff
from tests.ops.test_nebius_development_management_foundation import live as live
from tests.ops.test_nebius_development_management_foundation import preflight as preflight
from tests.ops.test_nebius_development_management_foundation import publication as publication
from tests.ops.test_nebius_development_management_foundation import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_management_foundation import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_pool_install import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_install import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_pool_install import (
    connected as connected,
)
from tests.ops.test_nebius_development_pool_install import (
    installation as installation,
)
from tests.ops.test_nebius_development_pool_install import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_pool_install import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    manager_entry as original_manager_entry,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_install import (
    material as material,
)
from tests.ops.test_nebius_development_pool_install import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_inputs as original_pool_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_install import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    retained as retained,
)
from tests.ops.test_nebius_development_pool_install import (
    route as route,
)
from tests.ops.test_nebius_development_pool_install import (
    tls_material as tls_material,
)


def module():
    name = 'scripts.ops.nebius_development_pool_retained'
    if importlib.util.find_spec(name) is None:
        pytest.fail('read-only completed development pool loader is missing')
    return importlib.import_module(name)


@pytest.fixture
def development_inputs(request):
    config, candidate, profile = request.getfixturevalue('original_development_inputs')
    # The public-manager fixture selects this host. Its completed foundation
    # must have reserved that same endpoint, not a different earlier hostname.
    config['public_host'] = 'shared.dev.example.com'
    return config, candidate, profile


@pytest.fixture
def manager_entry(request):
    mode = getattr(request, 'param', None)
    foundation = request.getfixturevalue('handoff') if isinstance(mode, str) and mode.startswith('foundation') else None
    original = request.getfixturevalue('original_manager_entry')
    if foundation is not None:
        from dataclasses import asdict

        from tests.ops.test_nebius_development_management_install import DevelopmentAPI
        from tests.ops.test_nebius_development_source_intake import save_inputs

        reference, manager, _, _, _ = foundation
        operation, payload, path, _ = original
        payload.update(binding=asdict(manager.binding), shared_namespace_uid=manager.shared_namespace_uid,
            deployment=manager.deployment.model_dump(mode='json'))
        applications = payload['deployment']['installation']['applications']
        schema = '0159' if mode == 'foundation-old-schema' else '0174'
        applications['shared']['schema_revision'] = schema
        for release in applications['releases']:
            release['schema_revision'] = schema
        if mode == 'foundation-wrong-namespace':
            payload['shared_namespace_uid'] = 'af74765d-efdb-4700-9aef-46d2b56d0e37'
        payload['prerequisites']['foundation'] = reference
        for name in ('ca_pem', 'secret_store_master_keys'):
            if mode != 'foundation-wrong-' + name:
                Path(payload['application_files'][name]).write_text(getattr(manager.application_material, name))
        if mode.startswith('foundation-runtime'):
            from scripts.ops.nebius_development_management_foundation import (
                RetainedDevelopmentReference,
                load_retained_foundation,
            )

            inputs = load_retained_foundation(RetainedDevelopmentReference.model_validate(reference)).inputs
            payload.update(candidate=copy.deepcopy(inputs.candidate), profile=copy.deepcopy(inputs.profile))
            payload['deployment']['installation']['keyring'] = copy.deepcopy(inputs.keyring)
            payload['deployment']['installation']['registry_prefix'] = inputs.candidate['registry_prefix']
            operation.update(candidate=inputs.candidate['candidate_sha'], source_sha=inputs.candidate['candidate_sha'])
            (Path(path).parent / 'development-management-source.json').write_text(inputs.settings.preflight.source.model_dump_json())
        save_inputs(operation, payload, path)
        api = DevelopmentAPI(manager.binding)
        api.shared_uid = payload['shared_namespace_uid']
        return operation, payload, path, api
    if getattr(request, 'param', False):
        from tests.ops.test_nebius_development_source_intake import source_inputs

        return source_inputs(original)
    return original


@pytest.fixture
def pool_inputs(request, retained):
    reference, value, tokens = request.getfixturevalue('original_pool_inputs')
    mode = request.node.callspec.params.get('manager_entry')
    if isinstance(mode, str) and mode.startswith('foundation-runtime'):
        manager = retained[3]
        config = manager.deployment.installation.foundation.platform_config
        value['profiles']['image_admission_keyring'] = manager.deployment.installation.keyring
        participant, = value['participants']
        for target in participant['targets']:
            if 'trial' in target['workload_kinds']:
                target['target_id'] = config['target_id']
        for profile in value['profiles']['execution']:
            profile['runtime']['target_id'] = config['target_id']
            profile['runtime']['credential_broker_url'] = 'http://loom-llm-gateway.loom-dev.svc.cluster.local:9100/internal/service-execution'
            profile['runtime']['service_account_name'] = 'loom-execution-attempt'
        for profile in value['profiles']['task_images']:
            profile['target']['target_id'] = config['target_id']
        if mode == 'foundation-runtime-build':
            labels = {'loom.nebius/node-os': 'linux', 'loom.nebius/node-arch': 'amd64'}
            value['node_selector'].update(labels)
            for kind, key in (('execution', 'runtime'), ('task_images', 'target'), ('application_images', 'target')):
                for profile in value['profiles'].get(kind, []):
                    profile[key]['node_selector'].update(labels)
        if mode.startswith('foundation-runtime-build-material'):
            for identity in value['quota_identities'].values():
                identity[0] = config['quota_parent_id']
            for kind, purpose in (('task_images', 'task'), ('application_images', 'application')):
                for profile in value['profiles'][kind]:
                    profile['settings'].update(source_secret_name='loom-build-' + purpose + '-source',
                        registry_secret_name='loom-build-registry', registry_auth_kind='nebius',
                        registry_repository='cr.eu-north1.nebius.cloud/builds/' + purpose + '-images',
                        cache_secret_name=None, cache_bucket=None)
                    if mode.endswith('cache'):
                        profile['settings'].update(cache_secret_name='loom-build-cache', cache_bucket='loom-native-cache')
                    if mode.endswith('collision'):
                        profile['settings']['source_secret_name'] = 'loom-build-shared-source'
        if mode == 'foundation-runtime-bad-keyring':
            value['profiles']['image_admission_keyring'] = {'schema_version': 1, 'keys': []}
        elif mode == 'foundation-runtime-bad-broker':
            value['profiles']['execution'][0]['runtime']['credential_broker_url'] = 'http://loom-llm-gateway.loom-staging.svc.cluster.local:9100/internal/service-execution'
        elif mode == 'foundation-runtime-bad-account':
            value['profiles']['execution'][0]['runtime']['service_account_name'] = 'default'
        elif mode == 'foundation-runtime-bad-target':
            for target in participant['targets']:
                if 'trial' in target['workload_kinds']:
                    target['target_id'] = 'unrelated-target'
            for profile in value['profiles']['execution']:
                profile['runtime']['target_id'] = 'unrelated-target'
            for profile in value['profiles']['task_images']:
                profile['target']['target_id'] = 'unrelated-target'
    return reference, value, tokens


@pytest.fixture
def completed_pool(pool_entry, capsys):
    entry, operation, inputs, path, connected = pool_entry
    connected.server.complete = True
    assert entry.main(str(path), 'install') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_pool_installed_closed'
    return operation, inputs, path, connected


def reference(loader, completed_pool):
    path = completed_pool[2]
    return loader.RetainedDevelopmentPoolReference(operation_path=path,
        operation_sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def publication_inputs(completed_pool):
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource

    from loom_service.environment_management.candidates import ProtectedPublication
    from loom_service.environment_management.manager import CandidateBundle

    inputs = completed_pool[3].server.request.retained.inputs
    candidate, profile = copy.deepcopy(inputs.candidate), copy.deepcopy(inputs.profile)
    candidate.update(candidate_sha='e' * 40, source_archive_sha256='sha256:' + '5' * 64, run_id=12345)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '6' * 64
    profile.update(candidate_sha=candidate['candidate_sha'], task_image_ref=candidate['images']['service']['image_ref'])
    source = PreparedDevelopmentSource(source_sha=candidate['candidate_sha'], source_archive_sha256=candidate['source_archive_sha256'])
    selected = ProtectedPublication(candidate_id=UUID('47f27bd6-bfd9-4ae8-a585-82916a848c85'),
        source_sha=source.source_sha, run_id=12345, run_attempt=1, artifact_id=23456,
        artifact_sha256='sha256:' + '7' * 64, pull_request=2399)
    return source, selected, CandidateBundle(selected.candidate_id, candidate, profile)


def runtime_publication(completed_pool):
    from scripts.ops import nebius_development_runtime_render as runtime

    if not hasattr(runtime, 'DevelopmentRuntimePublication'):
        pytest.fail('source-bound development runtime publication is missing')
    return runtime.DevelopmentRuntimePublication(*publication_inputs(completed_pool))


def test_completed_pool_load_is_readonly_and_keeps_actual_identities(completed_pool, monkeypatch):
    from scripts.ops import nebius_development_pool_install, nebius_development_pool_intent

    loader = module()
    operation, _, _, connected = completed_pool
    before = len(connected.server.calls)
    monkeypatch.setattr(nebius_development_pool_install, 'install_development_pool',
        lambda **_: pytest.fail('installer replayed'))
    for name in ('namespace_documents', 'delivery_documents'):
        monkeypatch.setattr(nebius_development_pool_intent, name,
            lambda *_: pytest.fail('historical runtime rerendered'))
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.operation == operation
    participant, = value.request.registration.spec.participants
    for namespace in (participant.execution_namespace, participant.build_namespace):
        assert str(namespace.uid) == connected.server.namespaces[namespace.name]['metadata']['uid']
    deployment, = [row for row in value.resources.values() if row['kind'] == 'Deployment']
    assert deployment['spec']['replicas'] == 0
    assert len(connected.server.calls) == before
    assert all(path.read_bytes() == raw for path, raw in value.files.items())
    assert not any(token in repr(value) for token in connected.intent.tokens.values())


def test_retired_operator_files_do_not_invalidate_completed_pool(completed_pool):
    loader = module()
    _, inputs, _, connected = completed_pool
    for name in ('ca_file', 'credentials_file'):
        Path(inputs['operator_connection'][name]).unlink()
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.request.registration.spec.pool_id == connected.server.request.registration.spec.pool_id


@pytest.mark.parametrize('manager_entry', [False, True], indirect=True, ids=['new-source', 'retained-source'])
def test_manager_runtime_consumes_closed_catalog_and_retains_source_identity(completed_pool):
    name = 'scripts.ops.nebius_development_runtime_render'
    if importlib.util.find_spec(name) is None:
        pytest.fail('retained development manager runtime preparation is missing')
    publication = runtime_publication(completed_pool)
    prepared = importlib.import_module(name).prepare_manager_runtime(reference(module(), completed_pool), publication=publication)
    retained = prepared.retained
    spec = retained.request.registration.spec
    before = retained.request.retained.inputs.deployment
    assert prepared.deployment.pool_catalog_operation_id == spec.operation_id
    builder, = [row for row in spec.machines if row.workload_scope == 'application_builder']
    assert prepared.deployment.application_builder_machine_id == builder.machine_id
    assert prepared.delivery.deployment['spec']['replicas'] == 0
    assert prepared.delivery.deployment['spec']['template']['spec']['containers'][0]['image'] == publication.bundle.candidate['images']['service']['image_ref']
    assert prepared.original['spec']['template']['spec']['containers'][0]['image'] != publication.bundle.candidate['images']['service']['image_ref']
    assert prepared.original['metadata']['uid'] == completed_pool[3].server.store.resources['Deployment:loom-service']['metadata']['uid']
    old_volumes = {row['name']: row for row in prepared.original['spec']['template']['spec']['volumes']}
    volumes = {row['name']: row for row in prepared.delivery.deployment['spec']['template']['spec']['volumes']}
    for name in ('management-cloud', 'application-shared', 'db-ca'):
        assert volumes[name] == old_volumes[name]
    assert 'pool-profiles' in volumes
    configuration, = [row for row in prepared.delivery.configuration if row['kind'] == 'ConfigMap']
    config = json.loads(configuration['data']['installation.json'])
    assert config['applications']['runtime']['build']['binding']['pool_id'] == str(spec.pool_id)
    source = volumes['application-source-credentials']['secret']['secretName']
    assert source == prepared.delivery.source_secret_name
    if before.installation.applications.runtime.source_upload is not None:
        assert source == old_volumes['application-source-credentials']['secret']['secretName']
        assert prepared.requires_source_material is False
    else:
        assert prepared.requires_source_material is True
    assert prepared.deployment.installation.applications.shared == before.installation.applications.shared


@pytest.mark.parametrize('damage', ['source', 'archive', 'profile', 'selection', 'run'])
def test_runtime_rejects_mixed_source_publication_before_delivery(completed_pool, damage):
    from dataclasses import replace

    from scripts.ops import nebius_development_runtime_render as runtime

    target = runtime_publication(completed_pool)
    if damage == 'source':
        target = replace(target, source=target.source.model_copy(update={'source_sha': 'f' * 40}))
    elif damage == 'archive':
        target.bundle.candidate['source_archive_sha256'] = 'sha256:' + 'f' * 64
    elif damage == 'profile':
        target.bundle.profile['candidate_sha'] = 'f' * 40
    elif damage == 'selection':
        target = replace(target, publication=target.publication.model_copy(update={'candidate_id': UUID('47f27bd6-bfd9-4ae8-a585-82916a848c86')}))
    else:
        target.bundle.candidate['run_id'] += 1
    calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development manager runtime unqualified'):
        runtime.prepare_manager_runtime(reference(module(), completed_pool), publication=target)
    assert len(completed_pool[3].server.calls) == calls


def database_runtime(completed_pool, **changes):
    name = 'scripts.ops.nebius_development_runtime_setup'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database delivery is missing')
    arguments = dict(publication=runtime_publication(completed_pool),
        operation_id=UUID('aecc7407-b7b8-4c38-8d1f-bca5dca9840f'),
        actuator_password='runtime-actuator-' + 'p' * 40, batch_runner_token='loom_br_' + 'r' * 64)
    arguments.update(changes)
    return importlib.import_module(name).prepare_database_runtime(reference(module(), completed_pool), **arguments)


def build_material(request, publisher_cloud, **changes):
    from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope

    name = 'scripts.ops.nebius_development_build_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed native development build material delivery is missing')
    scope = DevelopmentRegistryCloudScope.model_validate({**publisher_cloud.scope,
        'project_id': request.foundation.inputs.config['project_id']})
    arguments = dict(registry_scope=scope, registry_credential=publisher_cloud.credential, cache_material=None)
    arguments.update(changes)
    return importlib.import_module(name).prepare_build_runtime(request, **arguments)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material',
    'foundation-runtime-build-material-cache'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_build_material_keeps_task_application_cache_and_publisher_credentials_separate(completed_pool, publisher_cloud):
    request = database_runtime(completed_pool)
    profiles = request.manager.retained.request.registration.spec.profiles
    cache = {'access-key': 'cache-test-access', 'secret-key': 'cache-test-secret'} if profiles.task_images[0].settings.cache_bucket else None
    documents = build_material(request, publisher_cloud, cache_material=cache)
    assert {row['kind'] for row in documents} == {'Secret', 'ServiceAccount', 'NetworkPolicy'}
    assert all(row['metadata']['namespace'] == 'loom-nebius-dev-execution-build' for row in documents)
    secrets = {row['metadata']['name']: row for row in documents if row['kind'] == 'Secret'}
    assert set(secrets) == {'loom-build-task-source', 'loom-build-application-source', 'loom-build-registry'} | (
        {'loom-build-cache'} if cache else set())
    retained = request.foundation.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
    assert retained['access-key'] != retained['source-access-key']
    for purpose, prefix in (('task', ''), ('application', 'source-')):
        assert secrets['loom-build-' + purpose + '-source']['data'] == {
            key: retained[prefix + key] for key in ('access-key', 'secret-key')}
    assert secrets['loom-build-registry']['data'] == {
        'credentials.json': base64.b64encode(publisher_cloud.credential).decode()}
    if cache:
        assert secrets['loom-build-cache']['data'] == {key: base64.b64encode(value.encode()).decode() for key, value in cache.items()}
    assert all(row['immutable'] is True and row['type'] == 'Opaque' for row in secrets.values())
    accounts = [row for row in documents if row['kind'] == 'ServiceAccount']
    assert {row['metadata']['name'] for row in accounts} == {
        profile.target.service_account_name for profile in (*profiles.task_images, *profiles.application_images)}
    assert all(row['automountServiceAccountToken'] is False for row in accounts)
    network, = [row for row in documents if row['kind'] == 'NetworkPolicy']
    assert network['spec']['ingress'] == [] and set(network['spec']['policyTypes']) == {'Ingress', 'Egress'}
    public = network['spec']['egress'][1]
    assert public['ports'] == [{'protocol': 'TCP', 'port': 80}, {'protocol': 'TCP', 'port': 443}]
    assert {'10.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12', '192.168.0.0/16'} <= set(public['to'][0]['ipBlock']['except'])


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-collision',
    'foundation-runtime-build-material-cache'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_build_material_rejects_colliding_authorities_or_missing_cache_key(completed_pool, publisher_cloud):
    with pytest.raises(ValueError, match='development build runtime unqualified'):
        build_material(database_runtime(completed_pool), publisher_cloud)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('damage', ['changed-request', 'unneeded-cache', 'wrong-registry'])
def test_build_material_refuses_unbound_input_without_emitting_documents(completed_pool, publisher_cloud, damage):
    request = database_runtime(completed_pool)
    changes = {}
    if damage == 'changed-request':
        request.database[0]['data']['setup.json'] = '{}'
    elif damage == 'unneeded-cache':
        changes['cache_material'] = {'access-key': 'unused-access', 'secret-key': 'unused-secret'}
    else:
        publisher_cloud.scope['registry_fqdn'] = 'cr.eu-north1.nebius.cloud/foreign'
    with pytest.raises(ValueError, match='development build runtime unqualified'):
        build_material(request, publisher_cloud, **changes)


def runtime_install_request(completed_pool, publisher_cloud):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
    from scripts.ops.nebius_development_collector_cloud import DevelopmentCollectorCloudScope

    name = 'scripts.ops.nebius_development_runtime_install'
    if importlib.util.find_spec(name) is None:
        pytest.fail('connected development runtime installation is missing')
    runtime = importlib.import_module(name)
    request = database_runtime(completed_pool)
    project = request.foundation.inputs.config['project_id']
    registry = DevelopmentRegistryCloudScope.model_validate({**publisher_cloud.scope, 'project_id': project})
    observer = DevelopmentCollectorCloudScope(tenant_id='tenant-test', region='eu-north1', project_id=project,
        account_id='serviceaccount-observer', group_id='group-observer', key_id='authpublickey-observer')
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode()
    credential = json.dumps({'subject-credentials': {'alg': 'RS256', 'private-key': private,
        'kid': observer.key_id, 'iss': observer.account_id, 'sub': observer.account_id}}).encode()
    return runtime, runtime.DevelopmentRuntimeInstallRequest(database=request, collector_scope=observer,
        collector_credential=credential, registry_scope=registry, registry_credential=publisher_cloud.credential)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_parent_freezes_complete_fixed_inventory_before_writes(completed_pool, publisher_cloud):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan = runtime.prepare_runtime_install(request)
    assert set(plan.fixed) == {'database', 'material', 'authority', 'isolation', 'workloads', 'catalog'}
    keys = [key for rows in plan.fixed.values() for key in rows]
    assert len(keys) == len(set(keys))
    assert set(plan.originals) == set(plan.stopped) == set(plan.targets) == {
        'Deployment:loom-nebius-management-dev:loom-service', 'Deployment:loom-dev:loom-service',
        'Deployment:loom-dev:loom-control-plane'}
    assert all(row['metadata']['uid'] for row in plan.originals.values())
    assert all(row['spec']['replicas'] == 0 for rows in (plan.stopped, plan.targets) for row in rows.values())
    assert {row['kind'] for row in plan.fixed['workloads'].values()} == {'Deployment', 'CronJob'}
    for row in plan.fixed['workloads'].values():
        assert row['spec'].get('replicas', 0) == 0 and row['spec'].get('suspend', True) is True
    assert {row['kind'] for row in plan.fixed['isolation'].values()} == {
        'NetworkPolicy', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}
    for row in plan.fixed['authority'].values():
        for rule in row.get('rules', []):
            assert set(rule['verbs']) <= {'get', 'list', 'watch'}
    source = request.database.manager.delivery.source_secret_name
    document = plan.fixed['material']['Secret:loom-nebius-management-dev:' + source]
    assert set(document['data']) == {'credentials.json'}
    material = json.loads(base64.b64decode(document['data']['credentials.json'], validate=True))
    assert set(material) == {'access-key', 'secret-key'}
    retained = request.database.foundation.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
    assert material['access-key'] == base64.b64decode(retained['source-access-key']).decode()
    assert runtime.prepare_runtime_install(request) == plan
    assert len(plan.input_digest) == 71 and plan.input_digest.startswith('sha256:')


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('damage', ['shared-account', 'shared-key', 'shared-group', 'shared-private-key', 'changed-database'])
def test_runtime_parent_rejects_shared_cloud_authority_or_changed_predecessor(completed_pool, publisher_cloud, damage):
    from dataclasses import replace

    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    if damage == 'changed-database':
        request.database.database[0]['data']['setup.json'] = '{}'
    elif damage == 'shared-private-key':
        credential = json.loads(request.collector_credential)
        credential['subject-credentials']['private-key'] = json.loads(request.registry_credential)['subject-credentials']['private-key']
        request = replace(request, collector_credential=json.dumps(credential).encode())
    else:
        field = {'shared-account': 'account_id', 'shared-key': 'key_id', 'shared-group': 'group_id'}[damage]
        scope = request.collector_scope.model_copy(update={field: getattr(request.registry_scope, field)})
        credential = json.loads(request.collector_credential)
        subject = credential['subject-credentials']
        subject.update(sub=scope.account_id, iss=scope.account_id, kid=scope.key_id)
        request = replace(request, collector_scope=scope, collector_credential=json.dumps(credential).encode())
    with pytest.raises(ValueError, match='development runtime installation intent unqualified'):
        runtime.prepare_runtime_install(request)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_delivery_keeps_old_data_identity_and_separates_credentials(completed_pool):
    from sqlalchemy.engine import make_url

    calls = len(completed_pool[3].server.calls)
    prepared = database_runtime(completed_pool)
    manager, foundation = prepared.manager, prepared.foundation
    shared, worker = prepared.material
    config, job = prepared.database
    participant, = manager.retained.request.registration.spec.participants
    assert shared['metadata']['namespace'] == job['metadata']['namespace'] == config['metadata']['namespace'] == 'loom-dev'
    assert worker['metadata']['namespace'] == participant.execution_namespace.name
    assert shared['immutable'] is worker['immutable'] is config['immutable'] is True
    assert set(shared['data']) == {'actuator-password', 'batch-runner-token'}
    assert set(worker['data']) == {'actuator-url', 'ca.crt'}
    material = {key: base64.b64decode(value).decode() for key, value in worker['data'].items()}
    database = make_url(material['actuator-url'])
    assert (database.username, database.password, database.host, database.database) == (
        'loom_actuator', 'runtime-actuator-' + 'p' * 40, 'loom-postgres.loom-dev.svc', 'loom')
    assert dict(database.query) == {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}
    original = foundation.bootstrap['material']['loom-platform-db']
    assert material['ca.crt'] == original['ca.crt']
    assert original['postgres-password'] not in repr(material)
    assert json.loads(config['data']['setup.json']) == {'namespace': 'loom-dev',
        'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'schema_revision': '0174'}
    pod = job['spec']['template']['spec']
    container, = pod['containers']
    assert container['command'] == ['python', '-m', 'loom.nebius_development_runtime_database']
    assert container['image'] == manager.publication.bundle.candidate['images']['service']['image_ref']
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never' and job['spec']['backoffLimit'] == 0
    assert not pod.get('initContainers')
    assert container['securityContext']['allowPrivilegeEscalation'] is False
    assert container['securityContext']['capabilities']['drop'] == ['ALL']
    env = {row['name']: row for row in container['env']}
    assert set(env) == {'LOOM_DEVELOPMENT_RUNTIME_CONFIG', 'LOOM_DB_URL', 'LOOM_DB_ACTUATOR_PASSWORD', 'LOOM_BATCH_RUNNER_TOKEN'}
    assert env['LOOM_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'admin-url'}
    for setting, key in [('LOOM_DB_ACTUATOR_PASSWORD', 'actuator-password'), ('LOOM_BATCH_RUNNER_TOKEN', 'batch-runner-token')]:
        assert env[setting]['valueFrom']['secretKeyRef'] == {'name': shared['metadata']['name'], 'key': key}
    assert {row['name'] for row in container['volumeMounts']} == {'db-ca', 'runtime-setup'}
    assert {row['name'] for row in pod['volumes']} == {'db-ca', 'runtime-setup'}
    ca, = [row for row in pod['volumes'] if row['name'] == 'db-ca']
    assert ca['secret'] == {'secretName': 'loom-platform-db', 'defaultMode': 0o440,
        'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}
    assert len(completed_pool[3].server.calls) == calls
    assert all(path.read_bytes() == raw for path, raw in foundation.files.items())


@pytest.mark.parametrize('changes', [{'operation_id': UUID(int=0)},
    {'actuator_password': 'too-short'}, {'batch_runner_token': 'wrong-kind-' + 'r' * 64}])
def test_runtime_database_delivery_refuses_invalid_material(completed_pool, changes):
    # This protects the pre-write boundary, not merely the eventual SQL failure.
    name = 'scripts.ops.nebius_development_runtime_setup'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database delivery is missing')
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool, **changes)


@pytest.mark.parametrize('manager_entry', ['foundation-old-schema', 'foundation-wrong-namespace',
    'foundation-wrong-ca_pem', 'foundation-wrong-secret_store_master_keys'], indirect=True)
def test_runtime_database_delivery_rejects_manager_foundation_mismatch(completed_pool):
    calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool)
    assert len(completed_pool[3].server.calls) == calls


def runtime_database_stage(request, api, state):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'stage_database_runtime'):
        pytest.fail('fixed development runtime database stage is missing')
    return setup.stage_database_runtime(request=request, api=api, state_dir=state)


def runtime_database_api(request):
    from tests.ops.test_nebius_management_stage import PhaseAPI

    class NamespacedAPI(PhaseAPI):
        @staticmethod
        def key(doc):
            return ':'.join((doc['kind'], doc['metadata']['namespace'], doc['metadata']['name']))

    return NamespacedAPI(request.manager.retained.request.retained.binding)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_retains_four_exact_creates_and_replays(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    first = runtime_database_stage(request, api, state)
    before = copy.deepcopy(api.resources)
    assert runtime_database_stage(request, api, state) == first
    assert api.resources == before
    name = 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f'
    assert api.creates == ['Secret:loom-dev:' + name, 'Secret:loom-nebius-dev-execution:' + name,
        'ConfigMap:loom-dev:' + name, 'Job:loom-dev:' + name]
    assert first['status'] == 'management_phase_staged'
    assert first['phase'] == 'development-runtime-database'
    assert len(first['resource_uids']) == 4
    assert 'runtime-actuator-' not in json.dumps(first)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_runtime_database_stage_never_retries_uncertain_create(completed_pool, tmp_path, failure):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    api.failure = failure
    if failure == 'after':
        receipt = runtime_database_stage(request, api, state)
        assert runtime_database_stage(request, api, state) == receipt
        assert len(api.creates) == 4
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development runtime database stage unqualified'):
                runtime_database_stage(request, api, state)
        assert len(api.creates) == 1


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_refuses_caller_modified_job_before_writes(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    request.database[1]['spec']['template']['spec']['containers'][0]['command'] = ['sh', '-c', 'echo unapproved']
    with pytest.raises(ValueError, match='development runtime database stage unqualified'):
        runtime_database_stage(request, api, state)
    assert not api.creates and not state.exists()


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_proof_binds_job_operation_role_and_token(completed_pool, tmp_path):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'validate_database_runtime_proof'):
        pytest.fail('fixed development runtime database receipt validation is missing')
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    runtime_database_stage(request, api, state)
    job, = [row for row in api.resources.values() if row['kind'] == 'Job']
    proof = {'job_uid': job['metadata']['uid'], 'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b',
        'database': {'status': 'development_runtime_database_installed',
            'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'role': 'loom_actuator',
            'role_oid': 17000, 'token_sha256': hashlib.sha256(('loom_br_' + 'r' * 64).encode()).hexdigest()}}
    assert setup.validate_database_runtime_proof(request, state, proof) is None
    for field, value in [('status', 'pending'), ('operation_id', '27d6a159-4df3-4bbf-8722-897b2b3c619b'),
            ('role', 'postgres'), ('role_oid', True), ('role_oid', 0), ('role_oid', 2**32),
            ('token_sha256', '0' * 64), ('extra', 'not-allowed')]:
        changed = copy.deepcopy(proof)
        changed['database'][field] = value
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, changed)
    for field, value in [('job_uid', proof['pod_uid']), ('pod_uid', str(UUID(int=0))), ('extra', True)]:
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, {**proof, field: value})


@pytest.fixture
def runtime_database_live(completed_pool, handoff, monkeypatch):
    from types import SimpleNamespace

    request = database_runtime(completed_pool)
    store = runtime_database_api(request)
    foundation, pool = handoff[2], completed_pool[3].server
    original = pool.handle
    state = SimpleNamespace(request=request, store=store, foundation=foundation, pool=pool,
        calls=[], complete=False, pod=None, report=None, on_log=None,
        job_name='loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f')
    kinds = {'secrets': 'Secret', 'configmaps': 'ConfigMap', 'serviceaccounts': 'ServiceAccount',
        'services': 'Service', 'networkpolicies': 'NetworkPolicy', 'statefulsets': 'StatefulSet',
        'deployments': 'Deployment', 'jobs': 'Job', 'persistentvolumeclaims': 'PersistentVolumeClaim',
        'persistentvolumes': 'PersistentVolume'}

    def handle(message):
        path, method = message.url.path, message.method
        state.calls.append(message)
        name = path.rsplit('/', 1)[-1]
        if method == 'POST':
            document = json.loads(message.content)
            assert document['metadata']['name'] == state.job_name
            assert document['kind'] in {'Secret', 'ConfigMap', 'Job'}
            if message.url.params.get('dryRun') == 'All':
                return httpx.Response(201, json=store.default_resource(document))
            try:
                store.create_resource(document)
            except OSError:
                raise httpx.ReadTimeout('response lost') from None
            return httpx.Response(201, json=store.get_resource(document))
        assert method == 'GET'
        if '/namespaces/loom-dev/pods' in path:
            job, = [row for row in store.resources.values() if row['kind'] == 'Job']
            if state.pod is None:
                state.pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
                    **copy.deepcopy(job['spec']['template']['metadata']), 'namespace': 'loom-dev',
                    'name': job['metadata']['name'] + '-abc', 'uid': str(uuid4()),
                    'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'controller': True,
                        'name': job['metadata']['name'], 'uid': job['metadata']['uid']}]},
                    'spec': copy.deepcopy(job['spec']['template']['spec']), 'status': {'phase': 'Succeeded',
                        'containerStatuses': [{'name': job['spec']['template']['spec']['containers'][0]['name'],
                            'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}],
                        'initContainerStatuses': [{'name': row['name'], 'restartCount': 0,
                            'state': {'terminated': {'exitCode': 0}}}
                            for row in job['spec']['template']['spec'].get('initContainers', [])]}}
            if name == 'pods':
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {}, 'items': [state.pod]})
            if name == 'log':
                if state.on_log:
                    state.on_log()
                report = state.report or {'status': 'development_runtime_database_installed',
                    'operation_id': str(request.operation_id), 'role': 'loom_actuator', 'role_oid': 17000,
                    'token_sha256': hashlib.sha256(('loom_br_' + 'r' * 64).encode()).hexdigest()}
                return httpx.Response(200, json=report)
            assert name == state.pod['metadata']['name']
            return httpx.Response(200, json=state.pod)
        if name == state.job_name:
            collection = path.split('/')[-2]
            namespace = path.split('/namespaces/', 1)[1].split('/', 1)[0]
            value = store.resources.get(':'.join((kinds[collection], namespace, name)))
            value = copy.deepcopy(value)
            if value is not None and value['kind'] == 'Job' and state.complete:
                value['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
            return httpx.Response(404) if value is None else httpx.Response(200, json=value)
        if path == '/api/v1/namespaces/loom-dev':
            return httpx.Response(200, json=foundation.bootstrap.namespace)
        if '/namespaces/loom-dev/' in path or '/persistentvolumes/' in path:
            kind = kinds[path.split('/')[-2]]
            value = foundation.bootstrap.secrets.get(name) if kind == 'Secret' else None
            if value is None:
                value = foundation.stage.resources.get(kind + ':' + name)
            if value is not None:
                return httpx.Response(200, json=value)
        return original(message)

    monkeypatch.setattr(pool, 'handle', handle)
    return state


def runtime_database_https(live, **options):
    name = 'scripts.ops.nebius_development_runtime_database_live'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database HTTPS adapter is missing')
    return importlib.import_module(name).HTTPSDevelopmentRuntimeDatabaseAPI(request=live.request,
        api_server=live.request.foundation.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(), **options)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_waits_for_bound_sql_receipt_and_replays(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'runtime-database'
    with runtime_database_https(live) as api:
        runtime_database_stage(live.request, api, state)
        assert api.database_report(state) is None
        live.complete = True
        proof = api.database_report(state)
        assert proof['database']['role'] == 'loom_actuator'
        assert proof['database']['operation_id'] == 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f'
        before = list(live.store.creates)
        runtime_database_stage(live.request, api, state)
        assert api.database_report(state) == proof
        assert live.store.creates == before and len(before) == 4


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('damage', ['database', 'execution-namespace'])
def test_runtime_database_https_rejects_live_identity_drift_before_writes(runtime_database_live, tmp_path, damage):
    live = runtime_database_live
    if damage == 'database':
        live.foundation.bootstrap.secrets['loom-platform-db']['metadata']['uid'] = str(uuid4())
    else:
        live.pool.namespaces['loom-nebius-dev-execution']['metadata']['uid'] = str(uuid4())
    with runtime_database_https(live) as api, pytest.raises(ValueError, match='development runtime database stage unqualified'):
        runtime_database_stage(live.request, api, tmp_path / 'runtime-database')
    assert not live.store.creates
    assert all(message.method == 'GET' for message in live.calls)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_rejects_receipt_and_late_drift(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'runtime-database'
    with runtime_database_https(live) as api:
        runtime_database_stage(live.request, api, state)
        live.complete = True
        first = api.database_report(state)
        live.report = {**first['database'], 'role': 'postgres'}
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        live.report = None
        live.pod['status']['containerStatuses'][0]['restartCount'] = 1
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        live.pod['status']['containerStatuses'][0]['restartCount'] = 0
        def drift():
            live.foundation.bootstrap.secrets['loom-platform-db']['metadata']['uid'] = str(uuid4())
        live.on_log = drift
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        assert len(live.store.creates) == 4


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_private_inputs_cannot_shadow_retained_evidence(runtime_database_live):
    live = runtime_database_live
    path = live.request.reference.operation_path
    changed = path.read_bytes() + b' '
    with runtime_database_https(live, private_files={path: changed}) as api:
        path.write_bytes(changed)
        with pytest.raises(ValueError, match='development runtime database prerequisites unqualified'):
            api.verify_identity(api.binding)
    assert not live.store.creates


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_shared_runtime_wires_real_predecessors_without_changing_artifact_provenance(completed_pool):
    name = 'scripts.ops.nebius_development_shared_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh shared development runtime preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_shared_runtime(database)
    service, controller = value.targets['service'], value.targets['control_plane']
    original = value.original
    for key, target, component in [('service', service, 'service'), ('control_plane', controller, 'control_plane')]:
        assert target['metadata']['uid'] == original[key]['metadata']['uid']
        assert target['metadata']['namespace'] == 'loom-dev'
        assert target['spec']['replicas'] == 0
        assert target['spec']['strategy'] == {'type': 'Recreate'}
        assert target['spec']['template']['spec']['containers'][0]['image'] == database.manager.publication.bundle.candidate['images'][component]['image_ref']
        old = {row['name']: row for row in original[key]['spec']['template']['spec']['volumes']}
        current = {row['name']: row for row in target['spec']['template']['spec']['volumes']}
        assert all(current[key] == row for key, row in old.items())
    api_env = {row['name']: row for row in service['spec']['template']['spec']['containers'][0]['env']}
    cp_env = {row['name']: row for row in controller['spec']['template']['spec']['containers'][0]['env']}
    assert api_env['LOOM_SVC_SERVICE_MODE']['value'] == 'application'
    assert api_env['LOOM_SVC_BATCH_RUNNER_CP_TOKEN']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f', 'key': 'batch-runner-token'}
    original_profile = database.manager.retained.request.retained.inputs.profile
    profile = json.loads(api_env['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value'])
    assert profile['candidate_sha'] == original_profile['candidate_sha']
    assert profile['candidate_sha'] != database.manager.publication.source.source_sha
    assert profile['runtime_image_ref'] == original_profile['runtime_image_ref']
    assert profile['image_admission'] == original_profile['image_admission']
    source = json.loads(api_env['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])
    assert source['kind'] == 'environment'
    assert source['data_environment_id'] == str(database.manager.deployment.installation.applications.shared.data_environment_id)
    pool = json.loads(cp_env['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
    assert pool['environment'] == 'development'
    assert pool['logical_pool_id'] == 'nebius-cpu'
    assert pool['management_origin'] == 'https://manage.example.com'
    assert pool['bearer_token_file'] == '/var/run/loom-pool-token/token'
    assert cp_env['LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED']['value'] == 'true'
    assert cp_env['LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED']['value'] == 'true'
    assert len(completed_pool[3].server.calls) == before_calls


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-bad-keyring', 'foundation-runtime-bad-target', 'foundation-runtime-bad-broker'], indirect=True)
def test_shared_runtime_rejects_incompatible_closed_catalog_before_writes(completed_pool):
    name = 'scripts.ops.nebius_development_shared_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh shared development runtime preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development shared runtime unqualified'):
        importlib.import_module(name).prepare_shared_runtime(database)
    assert len(completed_pool[3].server.calls) == before_calls


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_fresh_actuator_uses_catalog_and_only_read_authority(completed_pool, monkeypatch):
    name = 'scripts.ops.nebius_development_actuator_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development actuator preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_actuator_runtime(database)
    spec = database.manager.retained.request.registration.spec
    participant, = spec.participants
    deployment = value.deployment
    assert 'uid' not in deployment['metadata']
    assert deployment['metadata']['namespace'] == participant.execution_namespace.name
    assert deployment['spec']['replicas'] == 0
    assert deployment['spec']['strategy'] == {'type': 'Recreate'}
    pod = deployment['spec']['template']['spec']
    container, = pod['containers']
    assert container['image'] == database.manager.publication.bundle.candidate['images']['execution_actuator']['image_ref']
    settings = {row['name']: row for row in container['env']}
    assert settings['LOOM_EXECUTION_ACTUATOR_DB_URL']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f', 'key': 'actuator-url'}
    assert settings['LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL']['value'] == 'http://loom-llm-gateway.loom-dev.svc.cluster.local:9100/internal/service-execution'
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL']['value'])['participant'] == participant.model_dump(mode='json')
    build, = spec.profiles.task_images
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER']['value']) == build.settings.model_dump(mode='json')
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR']['value']) == spec.profiles.execution[0].runtime.node_selector
    assert pod['nodeSelector'] == {'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}
    assert pod['securityContext']['runAsUser'] == 65532
    assert all(row['image'] == database.manager.publication.bundle.candidate['images']['service']['image_ref'] for row in pod['initContainers'])
    namespaces = {doc['metadata']['namespace'] for doc in value.authority if doc['kind'] == 'Role'}
    assert namespaces == {participant.execution_namespace.name, participant.build_namespace.name}
    rules = [rule for doc in value.authority for rule in doc.get('rules', [])]
    assert rules and all(set(rule['verbs']) <= {'get', 'list'} for rule in rules)
    assert {'apiGroups': [''], 'resources': ['nodes', 'nodes/stats'], 'verbs': ['get']} in rules
    assert not any('nodes/proxy' in rule['resources'] for rule in rules)
    assert all(doc.get('metadata', {}).get('namespace') != 'loom-staging' for doc in value.authority)
    from loom_execution_actuator.config import ExecutionActuatorSettings
    for row in container['env']:
        if 'value' in row:
            monkeypatch.setenv(row['name'], row['value'])
    parsed = ExecutionActuatorSettings(db_url='postgresql+psycopg://loom_actuator:test@db/loom', controller_id='dev-actuator-test')
    assert parsed.global_pool.participant == participant
    assert parsed.task_image_builder == build.settings
    assert parsed.service_account_name == 'loom-execution-attempt'
    assert len(completed_pool[3].server.calls) == before_calls


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-bad-account'], indirect=True)
def test_fresh_actuator_rejects_foreign_task_service_account(completed_pool):
    from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
    database = database_runtime(completed_pool)
    with pytest.raises(ValueError, match='development actuator runtime unqualified'):
        prepare_actuator_runtime(database)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_job_binds_original_target_and_new_code_without_database_authority(completed_pool):
    name = 'scripts.ops.nebius_development_catalog_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development catalog Job preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_catalog_runtime(database)
    config, job = value.documents
    request = json.loads(config['data']['catalog.json'])
    execution, = database.manager.retained.request.registration.spec.profiles.execution
    assert request['execution_class'] == execution.execution_class.model_dump(mode='json')
    assert request['operation_id'] == str(database.operation_id)
    target, = request['topology']['targets']
    assert target['target_id'] == database.foundation.inputs.config['target_id']
    assert target['cluster_scope_id'] == database.foundation.inputs.config['cluster_scope_id']
    assert target['namespace_name'] == 'loom-nebius-dev-execution'
    assert target['environment'] == 'development'
    assert target['service_account_name'] == 'loom-execution-attempt'
    assert config['immutable'] is True
    assert config['metadata']['namespace'] == job['metadata']['namespace'] == 'loom-dev'
    assert job['spec']['backoffLimit'] == 0
    pod = job['spec']['template']['spec']
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never'
    container, = pod['containers']
    assert container['image'] == database.manager.publication.bundle.candidate['images']['service']['image_ref']
    assert container['command'] == ['python', '-m', 'loom.nebius_development_catalog']
    assert container['env'] == [{'name': 'LOOM_DEVELOPMENT_RUNTIME_CATALOG_CONFIG', 'value': '/var/run/loom-runtime-catalog/catalog.json'}]
    volumes = {row['name']: row for row in pod['volumes']}
    assert set(volumes) == {'runtime-catalog', 'admin-source', 'admin-owned'}
    assert volumes['admin-source']['secret']['secretName'] == 'loom-admin-secret'
    assert volumes['admin-source']['secret']['items'] == [{'key': 'secrets.toml', 'path': 'secrets.toml'}]
    assert volumes['admin-owned']['emptyDir']['medium'] == 'Memory'
    assert volumes['runtime-catalog']['configMap']['name'] == config['metadata']['name']
    initializer, = pod['initContainers']
    assert initializer['name'] == 'prepare-admin-secret'
    assert initializer['image'] == container['image']
    assert initializer['securityContext']['allowPrivilegeEscalation'] is False
    assert pod['securityContext']['runAsUser'] == 1000
    assert all(row['readOnly'] is True for row in container['volumeMounts'])
    assert len(completed_pool[3].server.calls) == before_calls


def catalog_runtime_stage(request, api, state):
    from scripts.ops import nebius_development_catalog_runtime as catalog

    if not hasattr(catalog, 'stage_catalog_runtime'):
        pytest.fail('fixed development catalog stage is missing')
    return catalog.stage_catalog_runtime(request=request, api=api, state_dir=state)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_stage_creates_only_fixed_config_and_job_and_replays(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    first = catalog_runtime_stage(request, api, state)
    before = copy.deepcopy(api.resources)
    assert catalog_runtime_stage(request, api, state) == first
    assert api.resources == before
    name = 'loom-dev-catalog-aecc7407b7b84c388d1fbca5dca9840f'
    assert api.creates == ['ConfigMap:loom-dev:' + name, 'Job:loom-dev:' + name]
    assert first['phase'] == 'development-runtime-catalog'
    assert first['status'] == 'management_phase_staged'
    assert len(first['resource_uids']) == 2


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_catalog_stage_never_retries_uncertain_create(completed_pool, tmp_path, failure):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    api.failure = failure
    if failure == 'after':
        receipt = catalog_runtime_stage(request, api, state)
        assert catalog_runtime_stage(request, api, state) == receipt
        assert len(api.creates) == 2
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development catalog stage unqualified'):
                catalog_runtime_stage(request, api, state)
        assert len(api.creates) == 1


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_stage_refuses_modified_runtime_request_before_writes(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    request.database[1]['spec']['template']['spec']['serviceAccountName'] = 'foreign-authority'
    with pytest.raises(ValueError, match='development catalog stage unqualified'):
        catalog_runtime_stage(request, api, state)
    assert not api.creates and not state.exists()


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_proof_binds_exact_job_request_and_operation(completed_pool, tmp_path):
    import rfc8785
    from scripts.ops import nebius_development_catalog_runtime as catalog

    if not hasattr(catalog, 'validate_catalog_runtime_proof'):
        pytest.fail('fixed development catalog proof validation is missing')
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    catalog_runtime_stage(request, api, state)
    config, = [row for row in api.resources.values() if row['kind'] == 'ConfigMap']
    job, = [row for row in api.resources.values() if row['kind'] == 'Job']
    raw = json.loads(config['data']['catalog.json'])
    canonical = rfc8785.dumps(raw) + b'\n'
    proof = {'job_uid': job['metadata']['uid'], 'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b',
        'catalog': {'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f',
            'target_id': raw['topology']['targets'][0]['target_id'],
            'catalog_sha256': 'sha256:' + hashlib.sha256(canonical).hexdigest()}}
    assert catalog.validate_catalog_runtime_proof(request, state, proof) is None
    for field, value in [('operation_id', str(uuid4())), ('target_id', 'staging'),
            ('catalog_sha256', '0' * 64), ('extra', True)]:
        changed = copy.deepcopy(proof)
        changed['catalog'][field] = value
        with pytest.raises(ValueError, match='development catalog receipt unqualified'):
            catalog.validate_catalog_runtime_proof(request, state, changed)
    for field, value in [('job_uid', proof['pod_uid']), ('pod_uid', str(UUID(int=0))), ('extra', True)]:
        with pytest.raises(ValueError, match='development catalog receipt unqualified'):
            catalog.validate_catalog_runtime_proof(request, state, {**proof, field: value})
    journal = state / 'stage.json'
    saved = json.loads(journal.read_bytes())
    for row in saved['resources'].values():
        row['status'] = 'prepared'
    journal.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match='development catalog receipt unqualified'):
        catalog.validate_catalog_runtime_proof(request, state, proof)


def runtime_catalog_https(live):
    from scripts.ops import nebius_development_catalog_runtime as catalog

    name = 'scripts.ops.nebius_development_catalog_live'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development catalog HTTPS adapter is missing')
    live.job_name = 'loom-dev-catalog-aecc7407b7b84c388d1fbca5dca9840f'
    config, _ = catalog.prepare_catalog_runtime(live.request).documents
    from loom.pipeline.keys import canonical_digest

    raw = json.loads(config['data']['catalog.json'])
    live.report = {'operation_id': str(live.request.operation_id),
        'target_id': raw['topology']['targets'][0]['target_id'], 'catalog_sha256': canonical_digest(raw)}
    live.catalog_checks = 0
    live.catalog_allowed = True

    def qualify(request):
        assert request == live.request
        live.catalog_checks += 1
        if not live.catalog_allowed:
            raise ValueError('runtime phase not qualified')
        # Child transport qualification is composed with the existing real
        # history/live reader. The new closed CP's transition and phase ordering
        # are the connected runtime parent's responsibility, not this fixture.
        with runtime_database_https(live) as original:
            original.verify_identity(original.binding)

    return importlib.import_module(name).HTTPSDevelopmentCatalogAPI(request=live.request,
        api_server=live.request.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), qualify_runtime=qualify)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_verifies_fixed_job_initializer_and_receipt(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        catalog_runtime_stage(live.request, api, state)
        assert api.catalog_report(state) is None
        live.complete = True
        proof = api.catalog_report(state)
        assert proof['catalog'] == live.report
        assert live.pod['status']['initContainerStatuses'][0]['name'] == 'prepare-admin-secret'
        assert api.catalog_report(state) == proof
        assert len(live.store.creates) == 2
        assert live.catalog_checks > 2


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_requires_parent_qualification_and_rejects_extra_authority(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        live.catalog_allowed = False
        with pytest.raises(ValueError, match='development catalog stage unqualified'):
            catalog_runtime_stage(live.request, api, state)
        assert not live.store.creates
        live.catalog_allowed = True
        foreign = copy.deepcopy(live.request.database[1])
        with pytest.raises(ValueError, match='resource outside fixed development runtime'):
            api.create_resource(foreign)
        assert not live.store.creates


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_late_prerequisite_loss_cannot_report_completion(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        catalog_runtime_stage(live.request, api, state)
        live.complete = True
        live.on_log = lambda: setattr(live, 'catalog_allowed', False)
        with pytest.raises(ValueError, match='development catalog execution unqualified'):
            api.catalog_report(state)
        assert len(live.store.creates) == 2


@pytest.mark.parametrize('damage', ['anchor', 'parent', 'phase', 'incomplete', 'inputs', 'receipt'])
def test_missing_or_changed_completion_cannot_become_successor_authority(completed_pool, damage):
    loader = module()
    _, _, _, connected = completed_pool
    selector = reference(loader, completed_pool)
    original = Path(connected.server.request.retained.operation['state_dir']).parent
    state = original / 'pool-installation'
    parent_path = state / 'installation.json'
    parent = json.loads(parent_path.read_bytes())
    if damage == 'anchor':
        (Path(connected.server.request.retained.operation['anchor_dir']) / 'pool-installation.json').unlink()
    elif damage == 'parent':
        parent_path.unlink()
    elif damage == 'phase':
        (state / 'material/stage.json').unlink()
    elif damage == 'incomplete':
        parent['phases']['workload'] = {'status': 'started', 'sha256': None}
        parent_path.write_text(json.dumps(parent))
    elif damage == 'inputs':
        inputs_path = Path(completed_pool[0]['inputs_path'])
        inputs_path.write_bytes(inputs_path.read_bytes() + b' ')
    else:
        registration = original / 'pool-registration/registration.json'
        value = json.loads(registration.read_bytes())
        value['proof']['registration']['mode'] = 'global'
        registration.write_text(json.dumps(value))
        parent['phases']['registration']['sha256'] = hashlib.sha256(registration.read_bytes()).hexdigest()
        parent_path.write_text(json.dumps(parent))
    before = len(connected.server.calls)
    with pytest.raises(ValueError, match='retained development pool') as caught:
        loader.load_retained_pool(selector)
    assert len(connected.server.calls) == before
    assert not any(token in str(caught.value) for token in connected.intent.tokens.values())


def network_runtime(request):
    name = 'scripts.ops.nebius_development_network_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development runtime network delivery is missing')
    return importlib.import_module(name).prepare_network_runtime(request)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_network_scopes_database_and_gateway_without_control_or_build_access(completed_pool):
    request = database_runtime(completed_pool)
    documents = network_runtime(request)
    policies = {row['metadata']['name']: row for row in documents}
    assert set(policies) == {'loom-execution-attempt-default-deny', 'loom-execution-attempt-egress',
        'development-runtime-postgres', 'development-runtime-gateway'}
    assert all(row['kind'] == 'NetworkPolicy' for row in documents)
    spec = request.manager.retained.request.registration.spec
    for name, app, port, selector in (
        ('postgres', 'loom-postgres', 5432, {'app.kubernetes.io/name': 'loom-execution-actuator'}),
        ('gateway', 'loom-llm-gateway', 9100, {'app.kubernetes.io/component': 'execution-unit'}),
    ):
        row = policies['development-runtime-' + name]
        assert row['metadata']['namespace'] == 'loom-dev'
        assert row['spec'] == {'podSelector': {'matchLabels': {'app': app}}, 'policyTypes': ['Ingress'],
            'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {
                'kubernetes.io/metadata.name': 'loom-nebius-dev-execution',
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/pool': str(spec.pool_id)}}, 'podSelector': {'matchLabels': selector}}],
                'ports': [{'protocol': 'TCP', 'port': port}]}]}
    deny = policies['loom-execution-attempt-default-deny']
    assert deny['spec']['ingress'] == deny['spec']['egress'] == []
    assert deny['metadata']['namespace'] == 'loom-nebius-dev-execution'
    egress = policies['loom-execution-attempt-egress']
    assert egress['metadata']['namespace'] == 'loom-nebius-dev-execution'
    assert egress['spec']['podSelector'] == {'matchLabels': {'app.kubernetes.io/component': 'execution-unit'}}
    dns, gateway = egress['spec']['egress']
    assert dns['to'][0]['namespaceSelector']['matchLabels'] == {'kubernetes.io/metadata.name': 'kube-system'}
    assert dns['ports'] == [{'protocol': 'UDP', 'port': 53}, {'protocol': 'TCP', 'port': 53}]
    assert gateway == {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'loom-dev'}},
        'podSelector': {'matchLabels': {'app': 'loom-llm-gateway'}}}], 'ports': [{'protocol': 'TCP', 'port': 9100}]}


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_network_refuses_changed_foundation_without_emitting_policy(completed_pool):
    request = database_runtime(completed_pool)
    request.foundation.inputs.config['namespace'] = 'loom-nebius-staging'
    with pytest.raises(ValueError, match='development runtime network unqualified'):
        network_runtime(request)


def build_policy(request):
    value = importlib.import_module('scripts.ops.nebius_development_build_policy')
    if not hasattr(value, 'prepare_build_policy'):
        pytest.fail('closed development build admission request binding is missing')
    return value.prepare_build_policy(request)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_build_policy_is_bound_to_retained_namespace_without_psa_change(completed_pool):
    request = database_runtime(completed_pool)
    policies = build_policy(request)
    policy, binding = policies
    assert policy['kind'] == 'ValidatingAdmissionPolicy' and policy['spec']['failurePolicy'] == 'Fail'
    assert binding['kind'] == 'ValidatingAdmissionPolicyBinding'
    assert binding['spec']['validationActions'] == ['Deny']
    assert binding['spec']['matchResources'] == {'namespaceSelector': {
        'matchLabels': {'kubernetes.io/metadata.name': 'loom-nebius-dev-execution-build'}}}
    assert policy['metadata']['labels']['loom.nebius/development-runtime-operation'] == str(request.operation_id)
    assert all('namespace' not in row['metadata'] for row in policies)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_build_policy_rejects_changed_retained_foundation(completed_pool):
    request = database_runtime(completed_pool)
    request.foundation.inputs.config['namespace'] = 'loom-nebius-staging'
    with pytest.raises(ValueError, match='development build policy unqualified'):
        build_policy(request)
