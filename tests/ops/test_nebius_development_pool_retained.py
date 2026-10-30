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
        schema = ('0159' if mode == 'foundation-old-schema'
            else '0175' if mode == 'foundation-runtime-build-material-split-schema' else '0176')
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
            if mode == 'foundation-runtime-build-material-split-schema':
                # The manager has its own database; its migration head is not
                # the head of the retained shared-dev foundation/application DB.
                # Publication authenticity is covered separately over HTTPS.
                payload['candidate']['service_schema_head'] = '0176'
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
            # Match the native SSD template used by observer preflight.
            value['quota_identities']['storage'][3] = 'compute.disk.size.network-ssd'
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
    candidate.setdefault('service_schema_head', '0176')
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


def runtime_install_request(completed_pool, publisher_cloud, **changes):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
    from scripts.ops.nebius_development_collector_cloud import DevelopmentCollectorCloudScope

    name = 'scripts.ops.nebius_development_runtime_install'
    if importlib.util.find_spec(name) is None:
        pytest.fail('connected development runtime installation is missing')
    runtime = importlib.import_module(name)
    request = database_runtime(completed_pool, **changes)
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


def foundation_publication(completed_pool):
    from scripts.ops.nebius_development_management_foundation import load_retained_foundation
    from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication

    from loom_service.environment_management.manager import CandidateBundle

    retained = module().load_retained_pool(reference(module(), completed_pool))
    foundation = load_retained_foundation(retained.request.retained.inputs.prerequisites.foundation)
    inputs = foundation.inputs
    selected = inputs.settings.preflight.publication
    return DevelopmentRuntimePublication(inputs.settings.preflight.source, selected,
        CandidateBundle(selected.candidate_id, inputs.candidate, inputs.profile))


@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_keeps_separate_database_publications_and_immutable_application_schema(
        completed_pool, publisher_cloud, published_source):
    """A newer manager must not force its schema or image onto shared dev."""
    manager = runtime_publication(completed_pool)
    data = foundation_publication(completed_pool)
    assert manager.bundle.candidate['service_schema_head'] == '0176'
    assert data.bundle.candidate['service_schema_head'] == '0175'
    runtime, request = runtime_install_request(completed_pool, publisher_cloud,
        publication=manager, data_publication=data)
    plan = runtime.prepare_runtime_install(request)
    job = next(row for row in plan.fixed['database'].values() if row['kind'] == 'Job')
    config = next(row for row in plan.fixed['database'].values() if row['kind'] == 'ConfigMap')
    assert json.loads(config['data']['setup.json'])['schema_revision'] == '0175'
    assert job['spec']['template']['spec']['containers'][0]['image'] == data.bundle.candidate['images']['service']['image_ref']
    for component, name in [('service', 'loom-service'), ('control_plane', 'loom-control-plane')]:
        target = plan.targets['Deployment:loom-dev:' + name]
        assert target['spec']['template']['spec']['containers'][0]['image'] == data.bundle.candidate['images'][component]['image_ref']
        assert target['spec']['replicas'] == 0
    management = plan.targets['Deployment:loom-nebius-management-dev:loom-service']
    assert management['spec']['template']['spec']['containers'][0]['image'] == manager.bundle.candidate['images']['service']['image_ref']
    actuator = next(row for row in plan.fixed['workloads'].values()
        if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-execution-actuator')
    assert actuator['spec']['template']['spec']['containers'][0]['image'] == data.bundle.candidate['images']['execution_actuator']['image_ref']
    application = request.database.manager.deployment.installation.applications
    assert application.shared.schema_revision == '0175'
    assert application.runtime.build.binding.recipe.schema_revision == '0175'
    assert all(row.schema_revision == '0175' for row in application.releases)
    assert all(path.read_bytes() == raw for path, raw in request.database.foundation.files.items())


@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_rejects_manager_image_for_an_older_shared_database(completed_pool, published_source):
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool)


@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_split_runtime_refuses_schema_or_source_drift(completed_pool, published_source):
    from dataclasses import replace

    for damage in ('data-schema', 'unknown-schema', 'missing-schema', 'manager-schema', 'source'):
        manager, data = runtime_publication(completed_pool), foundation_publication(completed_pool)
        if damage == 'data-schema':
            data.bundle.candidate['service_schema_head'] = '0176'
        elif damage == 'unknown-schema':
            data.bundle.candidate['service_schema_head'] = '0159'
        elif damage == 'missing-schema':
            data.bundle.candidate.pop('service_schema_head')
        elif damage == 'manager-schema':
            manager.bundle.candidate['service_schema_head'] = '0175'
        else:
            data = replace(data, source=data.source.model_copy(update={'source_sha': 'f' * 40}))
        with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
            database_runtime(completed_pool, publication=manager, data_publication=data)


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
@pytest.mark.parametrize('connected', [False, True], indirect=True, ids=['dev-first', 'staging-first'])
def test_common_catalog_runtime_preparation_and_resume_leave_staging_untouched(completed_pool, publisher_cloud):
    runtime, request, api, _, _ = parent_install_fixture(completed_pool, publisher_cloud)
    spec = request.database.manager.retained.request.registration.spec
    dev, = [row for row in spec.participants if row.environment_class == 'development']
    staging, = [row for row in spec.participants if row.environment_class == 'staging']
    plan = runtime.prepare_runtime_install(request)
    documents = [doc for phase in plan.fixed.values() for doc in phase.values()] + list(plan.targets.values())
    assert {doc['metadata'].get('namespace') for doc in documents} <= {
        None, 'loom-dev', 'loom-nebius-management-dev', dev.execution_namespace.name, dev.build_namespace.name}
    for doc in documents:
        if doc['kind'] in {'RoleBinding', 'ClusterRoleBinding'}:
            assert {row.get('namespace') for row in doc['subjects']} <= {
                dev.execution_namespace.name, 'loom-nebius-management-dev'}
    policy, = [doc for doc in documents if doc['kind'] == 'ValidatingAdmissionPolicy']
    assert staging.build_namespace.name not in json.dumps(policy)
    assert 'retained-staging-artifacts' not in json.dumps(policy)
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_database'
    api.database_complete = api.catalog_complete = True
    result = runtime.install_development_runtime(request=request, api=api, execute=True)
    assert result['status'] == 'development_runtime_installed_closed'
    assert result['admission_open'] is False and result['writer_migration_complete'] is False
    created, patches = list(api.store.creates), list(api.transition.patches)
    assert runtime.install_development_runtime(request=request, api=api, execute=True) == result
    assert api.store.creates == created and api.transition.patches == patches
    assert all(doc['metadata'].get('namespace') not in {
        staging.execution_namespace.name, staging.build_namespace.name, 'loom-nebius-platform'}
        for doc in api.store.resources.values())


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


class RuntimeParentAPI:
    def __init__(self, request, plan):
        from scripts.ops.nebius_ingress_stage import _key
        from tests.ops.test_nebius_development_runtime_transition import RuntimeAPI
        from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI

        self.request, self.plan = request, plan
        self.store = PhaseAPI(request.database.manager.retained.request.retained.binding)
        self.store.key = _key
        self.store.resources.update(copy.deepcopy(plan.originals))
        for row in self.store.resources.values():
            row['metadata']['resourceVersion'] = '1'
        self.transition = RuntimeAPI({})
        self.transition.rows = self.store.resources
        self.transition.ready = True
        self.database_complete = False
        self.catalog_complete = False
        self.visits = []
        self.inspections = 0

    def qualify(self, *, plan, state_dir, record):
        assert plan == self.plan
        assert record['input_digest'] == plan.input_digest
        journal = state_dir / 'installation.json'
        if journal.exists():
            assert record == json.loads(journal.read_bytes())

    def resources(self, phase):
        self.visits.append(phase)
        return self.store

    def workloads(self, phase):
        self.visits.append(phase)
        return self.transition

    def report(self, phase, state):
        if not getattr(self, phase + '_complete'):
            return None
        document = next(row for row in self.plan.fixed[phase].values() if row['kind'] == 'Job')
        key = self.store.key(document)
        if phase == 'database':
            payload = {'status': 'development_runtime_database_installed',
                'operation_id': str(self.request.database.operation_id), 'role': 'loom_actuator',
                'role_oid': 17000, 'token_sha256': hashlib.sha256(('loom_br_' + 'r' * 64).encode()).hexdigest()}
        else:
            from loom.pipeline.keys import canonical_digest

            config = next(row for row in self.plan.fixed[phase].values() if row['kind'] == 'ConfigMap')
            raw = json.loads(config['data']['catalog.json'])
            payload = {'operation_id': str(self.request.database.operation_id),
                'target_id': raw['topology']['targets'][0]['target_id'], 'catalog_sha256': canonical_digest(raw)}
        return {'job_uid': self.store.resources[key]['metadata']['uid'],
            'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b', phase: payload}

    def inspect_runtime(self, *, plan, state_dir, record):
        self.qualify(plan=plan, state_dir=state_dir, record=record)
        self.inspections += 1


def parent_install_fixture(completed_pool, publisher_cloud):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    if not hasattr(runtime, 'install_development_runtime'):
        pytest.fail('connected anchored development runtime parent is missing')
    plan = runtime.prepare_runtime_install(request)
    api = RuntimeParentAPI(request, plan)
    manager = request.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'runtime-installation'
    anchor = Path(manager.operation['anchor_dir']) / 'runtime-installation.json'
    return runtime, request, api, state, anchor


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_parent_orders_sql_catalog_start_and_replays_read_only(completed_pool, publisher_cloud):
    runtime, request, api, state, anchor = parent_install_fixture(completed_pool, publisher_cloud)
    def run(execute=True):
        return runtime.install_development_runtime(request=request, api=api, execute=execute)
    assert run(False)['status'] == 'development_runtime_preflight_qualified'
    assert not state.exists() and not anchor.exists() and not api.store.creates
    assert run()['status'] == 'pending_database'
    assert len(api.store.creates) == 4 and not api.transition.patches
    assert anchor.is_file()
    api.database_complete = True
    assert run()['status'] == 'pending_catalog'
    assert api.store.resources['Deployment:loom-dev:loom-service']['spec']['replicas'] == 0
    assert api.store.resources['Deployment:loom-dev:loom-control-plane']['spec']['replicas'] == 1
    assert api.store.resources['Deployment:loom-nebius-management-dev:loom-service']['spec']['replicas'] == 1
    assert all(row['spec'].get('replicas', 0) == 0 and row['spec'].get('suspend', True)
        for key, row in api.store.resources.items() if key in api.plan.fixed['workloads'])
    api.catalog_complete = True
    result = run()
    assert result['status'] == 'development_runtime_installed_closed'
    assert result['admission_open'] is False and result['writer_migration_complete'] is False
    assert api.store.resources['Deployment:loom-dev:loom-service']['spec']['replicas'] == 1
    saved = json.loads((state / 'installation.json').read_bytes())
    assert all(row['status'] == 'complete' for row in saved['phases'].values())
    for key in api.plan.fixed['workloads']:
        spec = api.store.resources[key]['spec']
        assert spec.get('replicas', 1) == 1 and spec.get('suspend', False) is False
    before = (list(api.store.creates), list(api.transition.patches), list(api.visits))
    assert run() == result
    assert (api.store.creates, api.transition.patches, api.visits) == before
    assert api.inspections >= 1


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_parent_resumes_definitively_rejected_preview_without_lost_evidence(completed_pool, publisher_cloud):
    runtime, request, api, _, _ = parent_install_fixture(completed_pool, publisher_cloud)
    api.database_complete = True
    api.transition.preview_rejected = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_stop'
    assert not api.transition.patches
    api.transition.preview_rejected = False
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_catalog'
    api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('damage', ['lost-child', 'lost-parent', 'changed-child', 'phase-jump'])
def test_runtime_parent_refuses_lost_or_rewritten_evidence_before_further_writes(completed_pool, publisher_cloud, damage):
    runtime, request, api, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    def run():
        return runtime.install_development_runtime(request=request, api=api, execute=True)
    assert run()['status'] == 'pending_database'
    before = list(api.store.creates)
    if damage == 'lost-child':
        (state / 'database/stage.json').unlink()
    elif damage == 'lost-parent':
        (state / 'installation.json').unlink()
    elif damage == 'changed-child':
        api.database_complete = True
        assert run()['status'] == 'pending_catalog'
        before = list(api.store.creates)
        path = state / 'database/stage.json'
        path.write_bytes(path.read_bytes() + b' ')
    else:
        path = state / 'installation.json'
        record = json.loads(path.read_bytes())
        record['phases']['start']['status'] = 'started'
        path.write_text(json.dumps(record))
    patches = list(api.transition.patches)
    with pytest.raises(ValueError, match='development runtime installation unqualified'):
        run()
    assert api.store.creates == before and api.transition.patches == patches


def runtime_workload_options(runtime, request, state):
    if not hasattr(runtime, 'runtime_workload_options'):
        pytest.fail('phase-aware development runtime workload observation is missing')
    return runtime.runtime_workload_options(request=request, state_dir=state)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_live_options_follow_anchored_successors_not_original_installation(completed_pool, publisher_cloud):
    from scripts.ops.nebius_management_switch import _stable

    runtime, request, api, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_database'
    choices = runtime_workload_options(runtime, request, state)
    assert choices == {key: (row,) for key, row in api.plan.originals.items()}
    api.database_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_catalog'
    choices = runtime_workload_options(runtime, request, state)
    assert len(choices) == 5
    for key, (expected,) in choices.items():
        assert _stable(expected) == _stable(api.store.resources[key])
        assert expected['metadata']['uid'] == api.store.resources[key]['metadata']['uid']
    api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    choices = runtime_workload_options(runtime, request, state)
    for key, (expected,) in choices.items():
        assert _stable(expected) == _stable(api.store.resources[key])
        assert expected['spec'].get('replicas', 1) == 1 and expected['spec'].get('suspend', False) is False
    # A live observer must not accept a changed child hidden under a completed parent.
    path = state / 'control/transition.json'
    path.write_bytes(path.read_bytes() + b' ')
    with pytest.raises(ValueError, match='development runtime workload history unqualified'):
        runtime_workload_options(runtime, request, state)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_live_options_preserve_both_sides_of_uncertain_patch_without_retry(completed_pool, publisher_cloud):
    runtime, request, api, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    api.database_complete = True
    api.transition.failure = 'before'
    with pytest.raises(ValueError, match='development runtime installation unqualified'):
        runtime.install_development_runtime(request=request, api=api, execute=True)
    choices = runtime_workload_options(runtime, request, state)
    key = api.transition.patches[0]
    before, after = choices[key]
    assert before['spec']['replicas'] == 1 and after['spec']['replicas'] == 0
    assert before['metadata']['uid'] == after['metadata']['uid'] == api.plan.originals[key]['metadata']['uid']
    patches = list(api.transition.patches)
    assert runtime_workload_options(runtime, request, state) == choices
    assert api.transition.patches == patches


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_prechild_window_cannot_hide_existing_history_or_another_phase(completed_pool, publisher_cloud):
    runtime, request, api, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_database'
    for phase in ('database', 'stop', 'unrecognised'):
        with pytest.raises(ValueError, match='development runtime workload history unqualified'):
            runtime.runtime_workload_options(request=request, state_dir=state, _prechild_phase=phase)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('input_version', ['v1', 'v2'])
def test_runtime_private_entry_binds_source_history_and_separate_material_before_connection(
        completed_pool, publisher_cloud, monkeypatch, input_version):
    from scripts.ops import nebius_certificates as private_state

    extra = {'data_publication': foundation_publication(completed_pool)} if input_version == 'v2' else {}
    _, request = runtime_install_request(completed_pool, publisher_cloud, **extra)
    name = 'scripts.ops.nebius_development_runtime_entry'
    if importlib.util.find_spec(name) is None:
        pytest.fail('protected development runtime entry is missing')
    entry = importlib.import_module(name)
    manager = request.database.manager.retained.request.retained
    owner = Path(manager.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-development-runtime' / manager.binding.installation_id / str(request.database.operation_id)
    root.mkdir(mode=0o700, parents=True)
    material = {}
    for field, raw in (('collector_credential_file', request.collector_credential),
            ('registry_credential_file', request.registry_credential),
            ('actuator_password_file', b'runtime-actuator-' + b'p' * 40),
            ('batch_runner_token_file', b'loom_br_' + b'r' * 64)):
        path = root / (field + '.private')
        private_state._write_private(path, raw)
        material[field] = str(path)
    source = root / 'development-runtime-source.json'
    installer_source = request.database.manager.publication.source.model_dump(mode='json')
    if input_version == 'v2':
        installer_source = {'source_sha': 'd' * 40, 'source_archive_sha256': 'sha256:' + 'e' * 64}
    private_state._atomic_json(source, installer_source)
    monkeypatch.setattr(entry, 'SOURCE_RECORD', source)
    value = {'schema_version': 'loom.nebius-development-runtime-inputs.' + input_version,
        'retained': request.database.reference.model_dump(mode='json'),
        'publication': request.database.manager.publication.publication.model_dump(mode='json'),
        'candidate': request.database.manager.publication.bundle.candidate,
        'profile': request.database.manager.publication.bundle.profile,
        'collector_scope': request.collector_scope.model_dump(mode='json'),
        'registry_scope': request.registry_scope.model_dump(mode='json'),
        'operator_connection': completed_pool[1]['operator_connection'], **material}
    if input_version == 'v2':
        data = extra['data_publication']
        value.update(publication_source=request.database.manager.publication.source.model_dump(mode='json'),
            data_runtime={'source': data.source.model_dump(mode='json'),
                'publication': data.publication.model_dump(mode='json'),
                'candidate': data.bundle.candidate, 'profile': data.bundle.profile})
    path = root / 'inputs.json'
    private_state._atomic_json(path, value)
    operation = {'schema': 'loom.nebius-development-runtime-operation.v1',
        'source_sha': installer_source['source_sha'],
        'installation_id': manager.binding.installation_id, 'namespace': manager.binding.namespace,
        'operation_id': str(request.database.operation_id), 'inputs_path': str(path),
        'inputs_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    _, actual, files = entry.load_inputs(operation)
    assert actual == request and all(item.read_bytes() == raw for item, raw in files.items())
    assert {path, source, *(Path(item) for item in material.values())} <= files.keys()
    assert request.collector_credential.decode() not in repr(actual)
    # The successor must consume v2 history after installer credentials retire;
    # it must not reinterpret it as the old single-source format.
    if input_version == 'v2':
        from scripts.ops.nebius_development_runtime_install import (
            install_development_runtime,
            prepare_runtime_install,
        )
        from scripts.ops.nebius_shared_pool_entry import RetainedRuntimeReference, _retained_runtime

        parent = RuntimeParentAPI(request, prepare_runtime_install(request))
        parent.database_complete = parent.catalog_complete = True
        assert install_development_runtime(request=request, api=parent, execute=True)['status'] == 'development_runtime_installed_closed'
        operation_path = root / 'operation.json'
        private_state._atomic_json(operation_path, operation)
        retained_reference = RetainedRuntimeReference(operation_path=operation_path,
            operation_sha256=hashlib.sha256(operation_path.read_bytes()).hexdigest())
        assert _retained_runtime(retained_reference, {}) == request
    for damage in ('source', 'staging', 'history', 'material'):
        changed = copy.deepcopy(operation)
        if damage == 'source':
            changed['source_sha'] = 'f' * 40
        elif damage == 'staging':
            changed['namespace'] = 'loom-staging'
        elif damage == 'history':
            edited = copy.deepcopy(value)
            edited['retained']['operation_sha256'] = 'f' * 64
            private_state._atomic_json(path, edited)
            changed['inputs_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            private_state._atomic_json(path, value)
            changed['inputs_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            credential = Path(material['collector_credential_file'])
            private_state._atomic_json(credential, json.loads(request.registry_credential))
        with pytest.raises(ValueError, match='development runtime private inputs unqualified'):
            entry.load_inputs(changed)


def runtime_http_inventory(request, parent, pool, foundation):
    calls, documents = [], {}
    plurals = {'Secret': 'secrets', 'ConfigMap': 'configmaps', 'Namespace': 'namespaces',
        'ServiceAccount': 'serviceaccounts', 'Service': 'services', 'StatefulSet': 'statefulsets',
        'Deployment': 'deployments', 'Job': 'jobs', 'CronJob': 'cronjobs', 'NetworkPolicy': 'networkpolicies',
        'Role': 'roles', 'RoleBinding': 'rolebindings', 'ClusterRole': 'clusterroles', 'ClusterRoleBinding': 'clusterrolebindings',
        'Ingress': 'ingresses', 'ResourceQuota': 'resourcequotas', 'LimitRange': 'limitranges',
        'PersistentVolume': 'persistentvolumes', 'PersistentVolumeClaim': 'persistentvolumeclaims',
        'ValidatingAdmissionPolicy': 'validatingadmissionpolicies', 'ValidatingAdmissionPolicyBinding': 'validatingadmissionpolicybindings'}
    for row in (*pool.store.resources.values(), *foundation.stage.resources.values(),
            *foundation.bootstrap.secrets.values(), foundation.bootstrap.namespace, *pool.namespaces.values(),
            *request.database.manager.retained.resources.values(), *parent.store.resources.values()):
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        namespace = row['metadata'].get('namespace')
        path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plurals[row['kind']] + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
    def handle(message):
        calls.append(message)
        assert message.method == 'GET' and '/namespaces/loom-staging/' not in message.url.path
        if message.url.path in documents:
            return httpx.Response(200, json=documents[message.url.path])
        if message.url.path in {'/api/v1/namespaces/kube-system', '/api/v1/namespaces/loom-nebius-management-dev'}:
            return pool.handle(message)
        return httpx.Response(404)
    return calls, documents, handle


def protected_runtime_request(completed_pool, publisher_cloud, published_source):
    from dataclasses import replace

    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
    from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication

    from loom_service.environment_management.candidates import ProtectedPublication
    from loom_service.environment_management.manager import CandidateBundle

    _, request = runtime_install_request(completed_pool, publisher_cloud)
    original = request.database.manager.retained.request.retained.inputs
    selected = ProtectedPublication.model_validate(published_source.reference)
    publication = DevelopmentRuntimePublication(
        PreparedDevelopmentSource(source_sha=selected.source_sha,
            source_archive_sha256=published_source.candidate['source_archive_sha256']),
        selected, CandidateBundle(selected.candidate_id, published_source.candidate, original.profile))
    return replace(request, database=database_runtime(completed_pool, publication=publication))


def runtime_github_transport(published_source):
    from tests.unit.test_nebius_candidate_catalog import github_transport

    transport = github_transport(published_source.responses, published_source.payload)
    async def public_read(message):
        if message.url.host == 'api.github.com':
            assert message.headers['Authorization'] == 'Bearer scoped-publication-test-token'
            message.headers['Authorization'] = 'Bearer test-github-secret'
        return await transport.handle_async_request(message)
    return httpx.MockTransport(public_read)




def runtime_cloud_transport(request, publisher_cloud, monkeypatch, tmp_path):
    from types import SimpleNamespace

    from cryptography.hazmat.primitives import serialization
    from nebius.api.nebius.compute import v1 as compute
    from nebius.api.nebius.iam import v1 as iam
    from nebius.api.nebius.mk8s import v1 as mk8s
    from nebius.api.nebius.quotas import v1 as quotas
    from nebius.api.nebius.registry import v1 as registry
    from nebius.sdk import SDK
    from scripts.ops import nebius_certificates as private_state
    from tests.unit.test_execution_capacity_collector import (
        _enum,
        _node_group_spec,
        _platform_client,
        _quota,
    )

    cloud, scope = publisher_cloud, request.collector_scope
    config = request.database.foundation.inputs.config
    spec = request.database.manager.retained.request.registration.spec
    cloud.rows[scope.project_id] = cloud.rows.pop('project-children')
    cloud.rows[scope.project_id][1]['metadata']['id'] = scope.project_id
    for _, row in cloud.rows.values():
        if row['metadata'].get('parent_id') == 'project-children':
            row['metadata']['parent_id'] = scope.project_id
    key = serialization.load_pem_private_key(json.loads(request.collector_credential)[
        'subject-credentials']['private-key'].encode(), password=None)
    cloud.rows[scope.account_id] = (iam.ServiceAccount, {'metadata': {'id': scope.account_id,
        'parent_id': scope.project_id}, 'status': {'active': True}})
    cloud.rows[scope.group_id] = (iam.Group, {'metadata': {'id': scope.group_id, 'parent_id': scope.tenant_id}})
    cloud.rows[scope.key_id] = (iam.AuthPublicKey, {'metadata': {'id': scope.key_id, 'parent_id': scope.project_id},
        'spec': {'account': {'service_account': {'id': scope.account_id}}, 'data': key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()}, 'status': {'state': 'ACTIVE'}})
    cloud.groups[scope.account_id] = [scope.group_id]
    cloud.permits[scope.group_id] = [{'metadata': {'id': 'permit-observer', 'parent_id': scope.group_id},
        'spec': {'resource_id': scope.tenant_id, 'role': 'viewer'}}]
    operator = tmp_path / 'runtime-operator.json'
    private_state._write_private(operator, b'{"operator":"private-test"}')
    sessions, reads, closed = [], [], []
    mode = {'damage': None}

    def sdk_init(self, *, credentials_file_name, **kwargs):
        self.path = Path(credentials_file_name)
        self.identity = 'operator' if self.path == operator else json.loads(self.path.read_bytes())['subject-credentials']['sub']
        assert self.identity in {'operator', scope.account_id}
        assert self.path.stat().st_mode & 0o077 == 0
        sessions.append(self)
    async def sdk_close(self):
        closed.append(self)
    monkeypatch.setattr(SDK, '__init__', sdk_init)
    monkeypatch.setattr(SDK, 'close', sdk_close)
    for cls, name in [('ProjectServiceClient', 'projects'), ('ServiceAccountServiceClient', 'accounts'),
            ('GroupServiceClient', 'groups'), ('GroupMembershipServiceClient', 'memberships'),
            ('AccessPermitServiceClient', 'permits'), ('AuthPublicKeyServiceClient', 'public_keys')]:
        def client(sdk, name=name):
            assert sdk.identity == 'operator'
            return cloud.clients[name]
        monkeypatch.setattr(iam, cls, client)
    monkeypatch.setattr(registry, 'RegistryServiceClient', lambda sdk: cloud.clients['registries'])

    async def allowance(sdk, selection, **kwargs):
        assert sdk.identity == scope.account_id and selection.parent_id == scope.tenant_id
        reads.append(('quotas', sdk.identity))
        amounts = {'nodes': 20, 'vcpu': 80, 'memory': 160 * 1024**3, 'storage': 2000 * 1024**3}
        return SimpleNamespace(items=[_quota(mode.get('storage_quota', identity[3]) if name == 'storage' else identity[3], identity[4], amounts[name],
            amounts[name] if mode['damage'] == 'exhausted' else 0, 1)
            for name, identity in spec.quota_identities.items()], next_page_token='')
    async def group(sdk, selection, **kwargs):
        assert sdk.identity == scope.account_id and selection.id == spec.node_group_id
        reads.append(('nodegroup', sdk.identity))
        native = _node_group_spec()
        if mode.get('storage_quota') == 'compute.disk.size.network-ssd-non-replicated':
            native.template.boot_disk = mk8s.DiskSpec(size_gibibytes=80,
                type=mk8s.DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED)
        if mode.get('native_disk_type') is not None:
            native.template.boot_disk = SimpleNamespace(size_gibibytes=80, type=_enum(mode['native_disk_type']))
        return SimpleNamespace(metadata=SimpleNamespace(id=spec.node_group_id, resource_version=1,
            parent_id='foreign-cluster' if mode['damage'] == 'cluster' else config['cluster_id']),
            spec=native, status=SimpleNamespace(
                state=_enum('DELETING' if mode['damage'] == 'deleting' else 'RUNNING'), node_count=0,
                target_node_count=1 if mode['damage'] == 'scaling' else 0, ready_node_count=0,
                reconciling=mode['damage'] == 'scaling', events=[]))
    monkeypatch.setattr(quotas, 'QuotaAllowanceServiceClient',
        lambda sdk: SimpleNamespace(list=lambda *args, **kwargs: allowance(sdk, *args, **kwargs)))
    monkeypatch.setattr(mk8s, 'NodeGroupServiceClient',
        lambda sdk: SimpleNamespace(get=lambda *args, **kwargs: group(sdk, *args, **kwargs)))
    monkeypatch.setattr(compute, 'PlatformServiceClient', lambda sdk: _platform_client())
    return operator, sessions, reads, closed, mode






@pytest.fixture(params=[
    ('nebius_development_runtime_observation', 'HTTPSDevelopmentRuntimeObserver'),
    ('nebius_development_runtime_live', 'HTTPSDevelopmentRuntimeResources'),
    ('nebius_development_runtime_live', 'HTTPSDevelopmentRuntimeWorkloads'),
])
def runtime_private_reader(request, tmp_path):
    from types import SimpleNamespace

    module, name = request.param
    cls = getattr(importlib.import_module('scripts.ops.' + module), name)
    reader = object.__new__(cls)
    if name == 'HTTPSDevelopmentRuntimeObserver':
        # Match the constructor's original-catalog selection for this file guard.
        reader.preactivation = reader._preactivation_origin = None
        reader.schema_successor = reader._schema_origin = None
        reader.storage = reader._storage_origin = None
    files = {}
    for name, raw in (('manager', b'manager'), ('foundation', b'foundation'), ('operator', b'')):
        path = tmp_path / name
        path.write_bytes(raw)
        path.chmod(0o600)
        files[name] = {path: raw}
    reader.request = SimpleNamespace(database=SimpleNamespace(
        manager=SimpleNamespace(retained=SimpleNamespace(files=files['manager'])),
        foundation=SimpleNamespace(files=files['foundation'])))
    reader.private_files = files['operator']
    return reader, files












def populate_runtime_pods(documents):
    """API-shaped running controller collections, without replacing production checks."""
    for current in list(documents.values()):
        if current['kind'] != 'Deployment' or current['spec']['replicas'] != 1:
            continue
        generation = current['metadata'].setdefault('generation', 1)
        current['status'] = {'observedGeneration': generation, 'replicas': 1, 'updatedReplicas': 1,
            'readyReplicas': 1, 'availableReplicas': 1}
        namespace, name = current['metadata']['namespace'], current['metadata']['name']
        template = copy.deepcopy(current['spec']['template'])
        labels = {**template['metadata']['labels'], 'pod-template-hash': 'a12b34'}
        template['metadata']['labels'] = labels
        replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {
            'name': name + '-a12b34', 'namespace': namespace, 'uid': str(uuid4()), 'generation': 1,
            'labels': labels, 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment',
                'name': name, 'uid': current['metadata']['uid'], 'controller': True}]},
            'spec': {'replicas': 1, 'selector': {'matchLabels': {
                **current['spec']['selector']['matchLabels'], 'pod-template-hash': 'a12b34'}}, 'template': copy.deepcopy(template)},
            'status': {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
        pod = {'apiVersion': 'v1', 'kind': 'Pod', **copy.deepcopy(template), 'metadata': {
            **template['metadata'], 'name': name + '-a12b34-test', 'namespace': namespace, 'uid': str(uuid4()),
            'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
                'uid': replica['metadata']['uid'], 'controller': True}]},
            'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                'containerStatuses': [{'name': row['name'], 'ready': True, 'restartCount': 0,
                    'state': {'running': {'startedAt': '2026-10-09T00:00:00Z'}}} for row in template['spec']['containers']],
                'initContainerStatuses': [{'name': row['name'], 'restartCount': 0,
                    'state': {'terminated': {'exitCode': 0}}} for row in template['spec'].get('initContainers', [])]}}
        for version, plural, kind, row in (('apps/v1', 'replicasets', 'ReplicaSet', replica), ('v1', 'pods', 'Pod', pod)):
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            path = prefix + '/namespaces/' + namespace + '/' + plural
            documents.setdefault(path, {'apiVersion': version, 'kind': kind + 'List',
                'metadata': {'resourceVersion': '1'}, 'items': []})['items'].append(row)


def populate_database_backends(documents):
    for database in list(documents.values()):
        if database['kind'] != 'StatefulSet' or database['metadata']['name'] != 'loom-postgres':
            continue
        namespace = database['metadata']['namespace']
        database['metadata']['generation'] = 1
        database['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1,
            'currentReplicas': 1, 'updatedReplicas': 1, 'currentRevision': 'db-rev', 'updateRevision': 'db-rev'}
        service = documents['/api/v1/namespaces/' + namespace + '/services/loom-postgres']
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'loom-postgres-0', 'namespace': namespace,
            'uid': str(uuid4()), 'labels': {'app': 'loom-postgres', 'controller-revision-hash': 'db-rev'},
            'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': 'loom-postgres',
                'uid': database['metadata']['uid'], 'controller': True}]},
            'spec': copy.deepcopy(database['spec']['template']['spec']), 'status': {'phase': 'Running',
                'podIP': '10.20.0.2', 'podIPs': [{'ip': '10.20.0.2'}],
                'containerStatuses': [{'name': 'loom-postgres', 'ready': True, 'restartCount': 0}]}}
        pod['spec'].setdefault('volumes', []).append({'name': 'data',
            'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
        documents.setdefault('/api/v1/namespaces/' + namespace + '/pods', {'apiVersion': 'v1', 'kind': 'PodList',
            'metadata': {'resourceVersion': '1'}, 'items': []})['items'].append(pod)
        documents['/apis/discovery.k8s.io/v1/namespaces/' + namespace + '/endpointslices'] = {
            'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSliceList', 'metadata': {'resourceVersion': '1'},
            'items': [{'metadata': {'namespace': namespace, 'name': 'loom-postgres-test', 'uid': str(uuid4()),
                'labels': {'kubernetes.io/service-name': 'loom-postgres'}, 'ownerReferences': [
                    {'apiVersion': 'v1', 'kind': 'Service', 'name': 'loom-postgres',
                        'uid': service['metadata']['uid'], 'controller': True}]},
                'addressType': 'IPv4', 'ports': [{'name': service['spec']['ports'][0]['name'], 'port': 5432}],
                'endpoints': [{'addresses': ['10.20.0.2'], 'conditions': {'ready': True}, 'targetRef': {
                    'kind': 'Pod', 'namespace': namespace, 'name': 'loom-postgres-0', 'uid': pod['metadata']['uid']}}]}]}




def runtime_resources_api(request, phase, qualifier):
    name = 'scripts.ops.nebius_development_runtime_live'
    if importlib.util.find_spec(name) is None:
        pytest.fail('phase-bound development runtime HTTPS resources are missing')
    return importlib.import_module(name).HTTPSDevelopmentRuntimeResources(
        request=request, phase=phase, api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), token='operator-test', qualify_runtime=qualifier)






def runtime_workloads_api(request, qualifier, readiness):
    name = 'scripts.ops.nebius_development_runtime_live'
    module = importlib.import_module(name)
    if not hasattr(module, 'HTTPSDevelopmentRuntimeWorkloads'):
        pytest.fail('phase-bound development runtime workload HTTPS is missing')
    return module.HTTPSDevelopmentRuntimeWorkloads(request=request, phase='stop',
        api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), token='operator-test',
        qualify_runtime=qualifier, observe_ready=readiness)
















def runtime_database_stage(request, api, state):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'stage_database_runtime'):
        pytest.fail('fixed development runtime database stage is missing')
    return setup.stage_database_runtime(request=request, api=api, state_dir=state)


def runtime_database_api(request):
    from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI

    class NamespacedAPI(PhaseAPI):
        @staticmethod
        def key(doc):
            return ':'.join((doc['kind'], doc['metadata']['namespace'], doc['metadata']['name']))

    return NamespacedAPI(request.manager.retained.request.retained.binding)










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




















def catalog_runtime_stage(request, api, state):
    from scripts.ops import nebius_development_catalog_runtime as catalog

    if not hasattr(catalog, 'stage_catalog_runtime'):
        pytest.fail('fixed development catalog stage is missing')
    return catalog.stage_catalog_runtime(request=request, api=api, state_dir=state)










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










def network_runtime(request):
    name = 'scripts.ops.nebius_development_network_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development runtime network delivery is missing')
    return importlib.import_module(name).prepare_network_runtime(request)






def build_policy(request):
    value = importlib.import_module('scripts.ops.nebius_development_build_policy')
    if not hasattr(value, 'prepare_build_policy'):
        pytest.fail('closed development build admission request binding is missing')
    return value.prepare_build_policy(request)


@pytest.fixture
def system_kubectl(monkeypatch, tmp_path):
    """Mount the trusted tool directories without depending on the host install.

    These tests double the subprocess transport. Keep real executable lookup,
    explicit trusted search paths, and the untrusted/missing-tool refusal cases.
    """
    from scripts.ops import nebius_development_runtime_probes as module

    system = tmp_path / 'local-bin'
    untrusted = tmp_path / 'untrusted-bin'
    empty = tmp_path / 'empty-bin'
    for directory in (system, untrusted, empty):
        directory.mkdir()
    for directory in (system, untrusted):
        executable = directory / 'kubectl'
        executable.write_text('#!/bin/sh\nexit 0\n')
        executable.chmod(0o700)
    real_which = module.shutil.which

    def mounted_which(command, mode=module.os.F_OK | module.os.X_OK, path=None):
        # Map system directories into the test sandbox; retain real executable
        # lookup and any untrusted ambient directory selected by the caller.
        mounts = {'/usr/local/bin': str(system), '/bin': str(empty), '/usr/bin': str(empty)}
        search = module.os.environ['PATH'] if path is None else path
        return real_which(command, mode=mode, path=module.os.pathsep.join(
            mounts.get(directory, directory) for directory in search.split(module.os.pathsep)))

    monkeypatch.setattr(module.shutil, 'which', mounted_which)
    return system, untrusted



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('domain', ['manager', 'data'])
async def test_runtime_publication_resolves_protected_bytes_and_rejects_changed_bundle(
        completed_pool, publisher_cloud, published_source, domain):
    from dataclasses import replace

    from httpx._client import AsyncClient
    from scripts.ops import nebius_development_runtime_external as module

    request = protected_runtime_request(completed_pool, publisher_cloud, published_source)
    if domain == 'data':
        request = replace(request, database=replace(request.database,
            data_publication=copy.deepcopy(request.database.manager.publication)))
        baseline = '5781389317608cedf95be68590505c96257aa6c7'
        comparison = {'base_commit': {'sha': baseline}, 'merge_base_commit': {'sha': baseline},
            'ahead_by': 10, 'behind_by': 0, 'status': 'ahead'}
        published_source.responses['compare/' + baseline + '...' + published_source.reference['source_sha']] = comparison
    transport = runtime_github_transport(published_source)
    # Real GitHub approval, digest and signature validation; only HTTPS is replaced.
    async with AsyncClient(transport=transport) as http:
        await module.qualify_runtime_publication(request=request, http=http)
        target = request.database.data_publication if domain == 'data' else request.database.manager.publication
        original_image = target.bundle.candidate['images']['service']['image_ref']
        target.bundle.candidate['images']['service']['image_ref'] += '-changed'
        with pytest.raises(ValueError, match='development runtime publication unqualified'):
            await module.qualify_runtime_publication(request=request, http=http)
        if domain == 'data':
            target.bundle.candidate['images']['service']['image_ref'] = original_image
            comparison.update(status='behind', ahead_by=0, behind_by=1,
                merge_base_commit={'sha': published_source.reference['source_sha']})
            with pytest.raises(ValueError, match='development runtime publication unqualified'):
                await module.qualify_runtime_publication(request=request, http=http)
    # Same immutable selection cannot become approved merely because its local
    # candidate was once resolved: a now-failed protected check is re-read.
    request = protected_runtime_request(completed_pool, publisher_cloud, published_source)
    checks = published_source.responses['commits/' + 'b' * 40 + '/check-runs']['check_runs']
    checks[0]['conclusion'] = 'failure'
    async with AsyncClient(transport=transport) as http:
        with pytest.raises(ValueError, match='development runtime publication unqualified') as error:
            await module.qualify_runtime_publication(request=request, http=http)
    assert 'test-github-secret' not in str(error.value)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('connected', [None, True], indirect=True, ids=['dev-only', 'common-catalog'])
async def test_runtime_cloud_uses_observer_credentials_and_rejects_authority_or_pool_drift(
        completed_pool, publisher_cloud, monkeypatch, tmp_path):
    from scripts.ops import nebius_development_runtime_external as module

    _, request = runtime_install_request(completed_pool, publisher_cloud)
    operator, sessions, reads, closed, mode = runtime_cloud_transport(request, publisher_cloud, monkeypatch, tmp_path)
    cloud, scope = publisher_cloud, request.collector_scope
    await module.qualify_runtime_cloud(request=request, operator_credentials=operator)
    assert reads == [('quotas', scope.account_id), ('nodegroup', scope.account_id)]
    assert sessions == closed and not sessions[-1].path.exists()
    assert ('get', request.registry_scope.registry_id) in cloud.calls
    for damage in ('collector-editor', 'registry-admin', 'cluster'):
        mode['damage'] = damage
        cloud.permits[scope.group_id][0]['spec']['role'] = 'editor' if damage == 'collector-editor' else 'viewer'
        cloud.permits[request.registry_scope.group_id][0]['spec']['role'] = 'admin' if damage == 'registry-admin' else 'editor'
        with pytest.raises(ValueError, match='development runtime cloud unqualified') as error:
            await module.qualify_runtime_cloud(request=request, operator_credentials=operator)
        assert 'PRIVATE KEY' not in str(error.value) and 'private-test' not in str(error.value)
        assert sessions == closed
    # Native disk migration invalidates the old quota binding. Only an explicit
    # storage-only successor may qualify it, using the same observer credentials.
    from scripts.ops.nebius_pool_storage_binding import ClosedPoolStorageRefresh

    cloud.permits[scope.group_id][0]['spec']['role'] = 'viewer'
    cloud.permits[request.registry_scope.group_id][0]['spec']['role'] = 'editor'
    mode.update(damage=None, storage_quota='compute.disk.size.network-ssd-non-replicated')
    with pytest.raises(ValueError, match='development runtime cloud unqualified'):
        await module.qualify_runtime_cloud(request=request, operator_credentials=operator)
    before = request.database.manager.retained.request.registration.spec
    after = before.model_dump(mode='json')
    after['policy_revision'] += 1
    after['quota_identities']['storage'][3] = mode['storage_quota']
    storage = ClosedPoolStorageRefresh(schema_version='loom.closed-pool-storage-refresh.v1', operation_id=uuid4(),
        target_disk_type='NETWORK_SSD_NON_REPLICATED', before=before, after=after)
    snapshot = await module.qualify_runtime_cloud(request=request, operator_credentials=operator, storage=storage)
    assert snapshot.node_group.id == before.node_group_id
    assert snapshot.quota_resources['storage'].name == 'compute.disk.size.network-ssd-non-replicated'
    assert reads[-2:] == [('quotas', scope.account_id), ('nodegroup', scope.account_id)]
    assert sessions == closed and not sessions[-1].path.exists()
    assert request.database.manager.retained.request.registration.spec == before
    for native_disk_type in ('UNSPECIFIED', 'UNRECOGNIZED_NATIVE_DISK', 'NETWORK_SSD'):
        mode['native_disk_type'] = native_disk_type
        with pytest.raises(ValueError, match='development runtime cloud unqualified'):
            await module.qualify_runtime_cloud(request=request, operator_credentials=operator, storage=storage)
        assert sessions == closed and not sessions[-1].path.exists()



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.timeout(300)  # Complete parent with real history/crypto, HTTP boundary only.
def test_concrete_runtime_parent_qualifies_before_writes_and_resumes_pending_sql(
        completed_pool, publisher_cloud, published_source, handoff, monkeypatch, tmp_path, system_kubectl):
    import hmac
    from types import SimpleNamespace

    from httpx._client import AsyncClient, Client
    from pydantic import PostgresDsn
    from scripts.ops import nebius_development_runtime_install as runtime
    from scripts.ops import nebius_development_runtime_probes as probes
    from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )

    from loom_service.pool_management.capacity import digest

    name = 'scripts.ops.nebius_development_runtime_api'
    if importlib.util.find_spec(name) is None:
        pytest.fail('concrete phase-aware runtime parent is missing')
    module = importlib.import_module(name)
    request = protected_runtime_request(completed_pool, publisher_cloud, published_source)
    plan = runtime.prepare_runtime_install(request)
    parent = RuntimeParentAPI(request, plan)
    manager = request.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'runtime-installation'
    operator, _, cloud_reads, _, _ = runtime_cloud_transport(request, publisher_cloud, monkeypatch, tmp_path)
    calls, documents, get = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    populate_runtime_pods(documents)
    populate_database_backends(documents)
    writes, executions, logs = [], [], {}
    for row in documents.values():
        if row['kind'] in {'Deployment', 'CronJob'}:
            row['metadata'].setdefault('resourceVersion', '1')
            row['metadata'].setdefault('generation', 1)
    def reconcile(current):
        namespace, name = current['metadata']['namespace'], current['metadata']['name']
        for plural, version, kind in (('pods', 'v1', 'Pod'), ('replicasets', 'apps/v1', 'ReplicaSet')):
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            path = prefix + '/namespaces/' + namespace + '/' + plural
            listing = documents.setdefault(path, {'apiVersion': version, 'kind': kind + 'List',
                'metadata': {'resourceVersion': '1'}, 'items': []})
            listing['items'] = [row for row in listing['items'] if not row['metadata']['name'].startswith(name + '-a12b34')]
        count = current['spec']['replicas']
        current['status'] = {'observedGeneration': current['metadata']['generation'], **{
            field: count for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
        if count:
            additions = {'current': current}
            populate_runtime_pods(additions)
            for path, row in additions.items():
                if path != 'current':
                    documents[path]['items'].extend(row['items'])
    def handle(message):
        if message.method == 'GET':
            if message.url.path in logs:
                return httpx.Response(200, content=json.dumps(logs[message.url.path]).encode())
            if message.url.path.endswith('/pods') and message.url.params.get('labelSelector'):
                listing = copy.deepcopy(documents[message.url.path])
                label, value = message.url.params['labelSelector'].split('=', 1)
                listing['items'] = [row for row in listing['items'] if row['metadata']['labels'].get(label) == value]
                return httpx.Response(200, json=listing)
            return get(message)
        if message.method == 'PATCH':
            current = copy.deepcopy(documents[message.url.path])
            for change in json.loads(message.content):
                target, field = (current['metadata'], change['path'].rsplit('/', 1)[-1]) if change['path'].startswith('/metadata/') else (current, 'spec')
                if change['op'] == 'test':
                    assert target[field] == change['value']
                elif change['op'] == 'remove':
                    del target[field]
                else:
                    target[field] = change['value']
            if not message.url.query:
                writes.append(current)
                current['metadata']['generation'] += 1
                current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
                documents[message.url.path] = current
                if current['kind'] == 'Deployment':
                    reconcile(current)
            return httpx.Response(200, json=current)
        assert message.method == 'POST' and 'staging' not in message.url.path
        row = json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            return httpx.Response(201, json=parent.store.default_resource(row))
        writes.append(row)
        parent.store.create_resource(row)
        actual = parent.store.get_resource(row)
        if actual['kind'] in {'Deployment', 'CronJob'}:
            actual['metadata'].update(resourceVersion='1', generation=1)
            if actual['kind'] == 'Deployment':
                reconcile(actual)
        documents[message.url.path + '/' + row['metadata']['name']] = actual
        return httpx.Response(201, json=actual)
    monkeypatch.setattr(httpx, 'Client', lambda **kwargs: Client(**{**kwargs, 'transport': httpx.MockTransport(handle)}))
    monkeypatch.setattr(httpx, 'AsyncClient', lambda **kwargs: AsyncClient(**kwargs,
        transport=runtime_github_transport(published_source)))
    spec = request.database.manager.retained.request.registration.spec
    def execute(argv, **kwargs):
        executions.append(argv)
        if argv[-1].startswith('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;'):
            return SimpleNamespace(returncode=0, stderr=b'', stdout=json.dumps({
                'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
                'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'qualified': True}).encode())
        command = argv[argv.index('exec'):]
        component, nonce, response = command[10:]
        namespace, pod_name = command[2], command[3].removeprefix('pod/')
        actual = documents['/apis/apps/v1/namespaces/' + namespace + '/deployments/' + pod_name.removesuffix('-a12b34-test')]
        if command[9] == BOUND_POOL_SETTINGS_COMMAND:
            participant, = spec.participants
            machine, = (row for row in spec.machines if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
            value = expected_pool_runtime_settings(component, actual,
                token_sha256=machine.token_sha256 if component in {'controller', 'actuator'} else None,
                catalog_sha256=hashlib.sha256(spec.profiles.model_dump_json().encode()).hexdigest() if component == 'manager' else None)
            payload = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
        else:
            assert command[9] == _BOUND_DATABASE_COMMAND
            variable = {'controller': 'LOOM_CP_DB_URL', 'service': 'LOOM_SVC_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
            entry, = (row for row in actual['spec']['template']['spec']['containers'][0]['env'] if row['name'] == variable)
            reference = entry['valueFrom']['secretKeyRef']
            secret = documents['/api/v1/namespaces/' + namespace + '/secrets/' + reference['name']]
            url = base64.b64decode(secret['data'][reference['key']]).decode()
            payload = (url if component == 'actuator' else str(PostgresDsn(url))).encode()
        assert hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest() == response
        return SimpleNamespace(returncode=0, stderr=b'', stdout=json.dumps({
            'status': 'qualified'}).encode())
    monkeypatch.setattr(probes.subprocess, 'run', execute)
    arguments = dict(request=request, api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
        token='runtime-operator', operator_credentials=operator, private_files={operator: operator.read_bytes()})
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        assert runtime.install_development_runtime(request=request, api=api, execute=False)['status'] == 'development_runtime_preflight_qualified'
        assert calls and cloud_reads and executions and not writes and not state.exists()
        selected = documents['/api/v1/namespaces/loom-dev']
        original_uid = selected['metadata']['uid']
        selected['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError, match='development runtime installation unqualified'):
            runtime.install_development_runtime(request=request, api=api, execute=True)
        assert not writes and not state.exists()
        selected['metadata']['uid'] = original_uid
        assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_database'
        assert len(writes) == 4 and all(row['kind'] in {'Secret', 'ConfigMap', 'Job'} for row in writes)
        # Bound real history validation work per child boundary. Repeated full
        # reconstructions here multiply across every workload GET and PATCH.
        validations = []
        validate_child = runtime._validate_runtime_child
        def counted_validation(request, plan, state, phase, item):
            validations.append(phase)
            return validate_child(request, plan, state, phase, item)
        with monkeypatch.context() as counted:
            counted.setattr(runtime, '_validate_runtime_child', counted_validation)
            api._child_qualify(request, 'database', False)
        assert validations == ['database', 'database'], 'validate history once before and once after live reads'
        assert len(writes) == 4
    # A new process can observe the pending Job but must not repeat any writes.
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_database'
        assert len(writes) == 4
    path = state / 'database/stage.json'
    preserved = path.with_name('stage-preserved.json')
    path.rename(preserved)
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        with pytest.raises(ValueError, match='development runtime installation unqualified'):
            runtime.install_development_runtime(request=request, api=api, execute=True)
    assert len(writes) == 4, 'a fresh process cannot adopt lost child history'
    assert not path.exists()
    preserved.rename(path)
    def complete_job(phase):
        selected, = (row for row in parent.plan.fixed[phase].values() if row['kind'] == 'Job')
        job = documents['/apis/batch/v1/namespaces/loom-dev/jobs/' + selected['metadata']['name']]
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
        uid, name = job['metadata']['uid'], job['metadata']['name']
        pod = {'apiVersion': 'v1', 'kind': 'Pod', **copy.deepcopy(job['spec']['template'])}
        pod['metadata'].update(name=name + '-test', namespace='loom-dev', uid=str(uuid4()), ownerReferences=[{
            'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': uid, 'controller': True}])
        pod['metadata']['labels']['batch.kubernetes.io/controller-uid'] = uid
        pod['status'] = {'phase': 'Succeeded', **{states: [{'name': row['name'], 'restartCount': 0,
            'state': {'terminated': {'exitCode': 0}}} for row in pod['spec'].get(containers, [])]
            for containers, states in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses'))}}
        path = '/api/v1/namespaces/loom-dev/pods'
        documents[path]['items'].append(pod)
        documents[path + '/' + name + '-test'] = pod
        setattr(parent, phase + '_complete', True)
        logs[path + '/' + name + '-test/log'] = parent.report(phase, state / phase)[phase]
    complete_job('database')
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'pending_catalog'
    complete_job('catalog')
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        result = runtime.install_development_runtime(request=request, api=api, execute=True)
        assert result['status'] == 'development_runtime_installed_closed'
        assert result['pool_id'] == str(spec.pool_id)
        assert result['admission_open'] is result['writer_migration_complete'] is False
    before = len(writes)
    with module.HTTPSDevelopmentRuntimeAPI(**arguments) as api:
        assert runtime.install_development_runtime(request=request, api=api, execute=True) == result
    assert len(writes) == before and len(executions) > 4
    assert all('staging' not in row['metadata'].get('namespace', '') for row in writes)



def test_runtime_private_input_reads_are_bounded_by_retained_material(runtime_private_reader, monkeypatch):
    from scripts.ops import nebius_certificates

    reader, files = runtime_private_reader
    calls = []
    read = nebius_certificates._private_read
    def measured(path, *, limit):
        calls.append((path, limit))
        return read(path, limit=limit)
    monkeypatch.setattr(nebius_certificates, '_private_read', measured)
    reader._private_inputs()
    # The reader already adds one byte to detect growth. Repeated guards must
    # not allocate the 4 MiB maximum for each tiny retained journal or key.
    assert calls == [(path, len(raw)) for group in files.values() for path, raw in group.items()]



@pytest.mark.parametrize('change', ['same-length', 'growth', 'truncated', 'mode', 'symlink', 'hardlink', 'oversize'])
def test_runtime_private_input_rechecks_reject_drift(runtime_private_reader, tmp_path, change):
    from scripts.ops.nebius_certificates import CertificateError

    reader, files = runtime_private_reader
    reader._private_inputs()
    path, raw = next(iter(files['manager'].items()))
    if change == 'same-length':
        path.write_bytes(b'x' * len(raw))
    elif change == 'growth':
        path.write_bytes(raw + b'x')
    elif change == 'truncated':
        path.write_bytes(raw[:-1])
    elif change == 'mode':
        path.chmod(0o644)
    elif change == 'symlink':
        target = tmp_path / 'retained'
        path.rename(target)
        path.symlink_to(target)
    elif change == 'hardlink':
        (tmp_path / 'linked').hardlink_to(path)
    else:
        raw = b'x' * (4 * 1024**2 + 1)
        path.write_bytes(raw)
        files['manager'][path] = raw
    with pytest.raises((ValueError, OSError, CertificateError)):
        reader._private_inputs()



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('connected', [None, True], indirect=True, ids=['dev-only', 'common-catalog'])
def test_runtime_observer_checks_retained_data_and_successors_without_replaying_originals(completed_pool, publisher_cloud, handoff, monkeypatch):
    runtime, request, parent, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    parent.database_complete = True
    assert runtime.install_development_runtime(request=request, api=parent, execute=True)['status'] == 'pending_catalog'
    name = 'scripts.ops.nebius_development_runtime_observation'
    if importlib.util.find_spec(name) is None:
        pytest.fail('phase-aware HTTPS runtime observation is missing')
    module = importlib.import_module(name)
    foundation, pool = handoff[2], completed_pool[3].server
    calls, documents, handle = runtime_http_inventory(request, parent, pool, foundation)
    with module.HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
        result = observer.inspect(state_dir=state)
        assert len(result) == 5
        assert result['Deployment:loom-dev:loom-control-plane']['spec']['replicas'] == 1
        assert result['Deployment:loom-dev:loom-service']['spec']['replicas'] == 0
        # The observer has frozen its source-derived plan. Local proof checks
        # must re-read/validate child bytes, not recursively re-render old installs.
        from scripts.ops import nebius_development_runtime_setup
        monkeypatch.setattr(nebius_development_runtime_setup, 'database_runtime_documents',
            lambda *_: pytest.fail('read-only observation re-rendered the frozen plan'))
        assert observer.inspect(state_dir=state) == result
        # A child callback's parent frame must match before any live reads.
        before = len(calls)
        with pytest.raises(ValueError, match='development runtime live inventory unqualified'):
            observer.inspect(state_dir=state, _expected_record={})
        assert len(calls) == before
        # A valid active journal can change without changing parent metadata or
        # workload choices. Compare its bytes across the live observation too.
        child = runtime._child_path(state, 'catalog')
        original_child = child.read_bytes()
        get = observer._get
        def change_child_during_read(document):
            actual = get(document)
            child.write_bytes(original_child + b'\n')
            return actual
        with monkeypatch.context() as changed:
            changed.setattr(observer, '_get', change_child_during_read)
            with pytest.raises(ValueError, match='development runtime live inventory unqualified'):
                observer.inspect(state_dir=state)
        child.write_bytes(original_child)
        assert observer.inspect(state_dir=state) == result
        for path, field in (('/api/v1/namespaces/loom-dev/secrets/loom-platform-auth', 'data'),
                ('/apis/apps/v1/namespaces/loom-dev/deployments/loom-control-plane', 'uid'),
                ('/apis/apps/v1/namespaces/loom-dev/deployments/loom-control-plane', 'original'),
                ('/api/v1/namespaces/loom-dev', 'uid'),
                ('/api/v1/namespaces/loom-nebius-management-dev/persistentvolumeclaims/data-loom-postgres-0', 'uid')):
            original = copy.deepcopy(documents[path])
            if field == 'data':
                documents[path]['data']['secret-store-master-key'] = 'Y2hhbmdlZA=='
            elif field == 'original':
                documents[path]['spec'] = copy.deepcopy(parent.plan.originals['Deployment:loom-dev:loom-control-plane']['spec'])
            else:
                documents[path]['metadata']['uid'] = str(uuid4())
            with pytest.raises(ValueError, match='development runtime live inventory unqualified'):
                observer.inspect(state_dir=state)
            documents[path] = original
    assert calls and all(row.method == 'GET' for row in calls)
