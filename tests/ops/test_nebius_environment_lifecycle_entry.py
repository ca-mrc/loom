"""Installed lifecycle input authority, independent of runner-selected paths."""
from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication


def private(path, body):
    raw = json.dumps(body, sort_keys=True).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    return hashlib.sha256(raw).hexdigest()


def configuration(tmp_path):
    scope = dict(installation_id=str(uuid4()), pool_id=str(uuid4()), participant_id=str(uuid4()),
        environment_id=str(uuid4()), incarnation=str(uuid4()), authority='development')
    operation = str(uuid4())
    inputs = tmp_path / 'inputs.json'
    checksum = private(inputs, {'schema_version': 'loom.environment-lifecycle-inputs.v2'})
    body = dict(schema_version='loom.environment-lifecycle-configuration.v2', scope=scope,
        operations={operation: dict(kind='activate', inputs_path=str(inputs), inputs_sha256=checksum)})
    path = tmp_path / 'configuration.json'
    sha = private(path, body)
    return path, sha, body, operation, inputs


def test_private_configuration_binds_exact_scope_operation_and_bytes(tmp_path):
    name = 'scripts.ops.nebius_environment_lifecycle_entry'
    assert importlib.util.find_spec(name), 'installed lifecycle entry missing'
    entry = importlib.import_module(name)
    path, sha, body, operation, inputs = configuration(tmp_path)
    config = entry.load_lifecycle_configuration(path, sha)
    request = dict(schema_version='loom.environment-lifecycle-command.v2', action='prepare',
        operation_id=operation, scope=body['scope'])
    selected = entry.select_operation(config, request)
    assert selected.kind == 'activate'
    assert selected.inputs_path == inputs
    for changed in ({**request, 'operation_id': str(uuid4())},
                    {**request, 'scope': {**request['scope'], 'participant_id': str(uuid4())}},
                    {**request, 'inputs_path': '/tmp/untrusted'}):
        with pytest.raises(ValueError):
            entry.select_operation(config, changed)
    inputs.write_text('{}')
    with pytest.raises(ValueError):
        entry.load_lifecycle_configuration(path, sha)


@pytest.mark.parametrize('change', ['symlink', 'permissions', 'duplicate', 'nil', 'hash'])
def test_lifecycle_configuration_rejects_ambiguous_or_mutable_authority(tmp_path, change):
    name = 'scripts.ops.nebius_environment_lifecycle_entry'
    assert importlib.util.find_spec(name), 'installed lifecycle entry missing'
    entry = importlib.import_module(name)
    path, sha, body, operation, inputs = configuration(tmp_path)
    if change == 'symlink':
        link = tmp_path / 'link.json'
        link.symlink_to(inputs)
        body['operations'][operation]['inputs_path'] = str(link)
        sha = private(path, body)
    elif change == 'permissions':
        path.chmod(0o644)
    elif change == 'duplicate':
        raw = path.read_text().replace('"scope":', '"scope": {}, "scope":')
        path.write_text(raw)
        sha = hashlib.sha256(raw.encode()).hexdigest()
    elif change == 'nil':
        body['operations']['00000000-0000-0000-0000-000000000000'] = body['operations'].pop(operation)
        sha = private(path, body)
    else:
        sha = '0' * 64
    with pytest.raises(ValueError):
        entry.load_lifecycle_configuration(path, sha)


def test_lifecycle_prepare_cannot_accept_a_caller_proof_or_unknown_input(tmp_path):
    module_name = 'scripts.ops.nebius_environment_lifecycle_inputs'
    assert importlib.util.find_spec(module_name), 'typed lifecycle private inputs missing'
    inputs = importlib.import_module(module_name)
    with pytest.raises(ValueError):
        inputs.LifecycleInputsV2.model_validate({'schema_version': 'loom.environment-lifecycle-inputs.v2',
            'readiness': {'valid_until': '2099-01-01T00:00:00Z'}, 'run_command': ['sh', '-c', 'true']})


def test_lifecycle_executor_is_installed_with_entry():
    name = 'scripts.ops.nebius_environment_lifecycle'
    assert importlib.util.find_spec(name), 'connected lifecycle executor missing'
    module = importlib.import_module(name)
    assert callable(module.execute_lifecycle)


async def test_installed_provision_precedes_namespace_uids_and_recovers_exact_effects(tmp_path, platform_inputs, publication, monkeypatch):
    from scripts.ops import nebius_environment_lifecycle as lifecycle
    from scripts.ops.nebius_environment_lifecycle_entry import (
        LifecycleCommandV2,
        load_lifecycle_configuration,
    )
    from scripts.ops.nebius_environment_lifecycle_inputs import LifecycleClients
    from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
    from scripts.ops.nebius_pool_administration_job import PoolAdministrationRuntimeV2
    from tests.ops.test_nebius_environment_foundation import Remote
    from tests.unit.test_nebius_candidate_catalog import github_transport
    from tests.unit.test_nebius_environment_foundation import declaration

    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import (
        GitHubCandidateCatalog,
        ProtectedPublication,
    )
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderRetryError

    reference, responses, payload, keyring, candidate = publication
    publication_ref = ProtectedPublication.model_validate(reference)
    async with httpx.AsyncClient(transport=github_transport(responses, payload)) as github:
        catalog = GitHubCandidateCatalog(github, token='test-github-secret', publications=[publication_ref],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        bundle = await catalog.resolve(publication_ref.candidate_id)
        wanted = declaration((platform_inputs[0], bundle.candidate, bundle.profile))
        row = wanted.registration
        scope = dict(installation_id=str(row.installation_id), pool_id=str(row.pool_id), participant_id=str(row.participant_id),
            environment_id=str(row.environment_id), incarnation=str(row.incarnation), authority=row.kind)
        runtime = PoolAdministrationRuntimeV2(binding=PlatformNamespaceBindingV2(installation_id=str(row.installation_id),
            namespace='platform-services-east', namespace_uid=str(uuid4()), kube_system_uid=str(wanted.kube_system_uid)),
            service_image=candidate['images']['service']['image_ref'], database_secret='platform-ledger-credentials',
            database_url_key='admin-url', database_ca_key='ca.crt', node_selector={'platform.example/role': 'control'},
            tolerations=(), region='eu-north1')
        github_file, cloud_file = tmp_path / 'github', tmp_path / 'cloud'
        inputs = dict(schema_version='loom.environment-foundation-inputs.v2',
            host=dict(runtime=asdict(runtime), scope=scope, state_root=str(tmp_path / 'operations'),
                anchor_root=str(tmp_path / 'anchors'), connection=dict(endpoint='https://api.cluster.test',
                    ca_file=str(tmp_path / 'ca.pem'), credentials_file=str(tmp_path / 'kube.json'))),
            foundation_dir=str(tmp_path / 'foundation'), declaration=wanted.model_dump(mode='json'),
            application_publication=publication_ref.model_dump(mode='json'), image_admission_keyring=json.loads(keyring),
            github_token=dict(path=str(github_file), sha256=private(github_file, {})),
            cloud_credentials=dict(path=str(cloud_file), sha256=private(cloud_file, {})), registry_prefix=catalog.registry_prefix)
        inputs_path, config_path = tmp_path / 'provision.json', tmp_path / 'configuration.json'
        config_sha = private(config_path, dict(schema_version='loom.environment-lifecycle-configuration.v2', scope=scope,
            operations={str(wanted.operation_id): dict(kind='provision', inputs_path=str(inputs_path), inputs_sha256=private(inputs_path, inputs))}))
        config = load_lifecycle_configuration(config_path, config_sha)
        remote = Remote(tmp_path / 'foundation')
        async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as kube:
            @asynccontextmanager
            async def connected(inputs, policy, unchanged):
                assert policy is None, 'foundation must not require nonexistent participant namespace UIDs'
                yield LifecycleClients(catalog=catalog, kubernetes=KubernetesEnvironmentProvider(kube), cloud=remote,
                    public_http=kube, connect=lambda request: pytest.fail('provision cannot enroll'), registry_sdk=None)
            monkeypatch.setattr(lifecycle, 'lifecycle_clients', connected)
            request = LifecycleCommandV2.model_validate(dict(schema_version='loom.environment-lifecycle-command.v2',
                action='prepare', scope=scope, operation_id=str(wanted.operation_id)))
            async def execute(action):
                return await lifecycle.execute_lifecycle(configuration=config, request=request.model_copy(update={'action': action}),
                    operation=config.operations[wanted.operation_id], unchanged=lambda: load_lifecycle_configuration(config_path, config_sha))
            assert (await execute('prepare'))['status'] == 'prepared'
            assert not remote.writes
            with pytest.raises(ValueError, match='recovery_missing'):
                await execute('recover')
            remote.lose = 'cloud'
            with pytest.raises(ProviderRetryError):
                await execute('apply')
            completed = await execute('recover')
            assert completed['status'] == 'complete' and completed['admission_open'] is False
            writes = list(remote.writes)
            assert (await execute('status'))['status'] == 'observed'
            assert (await execute('recover'))['status'] == 'complete'
            assert remote.writes == writes
            assert set(json.loads((tmp_path / 'foundation' / 'observed.json').read_text())['namespace_uids']) == {
                row.application_namespace, row.execution_namespace, row.build_namespace}
            intent_path = tmp_path / 'operations' / ('lifecycle-operation-' + str(wanted.operation_id)) / 'intent.json'
            retained = json.loads(intent_path.read_text())
            private(intent_path, retained | {'inputs_sha256': '0' * 64})
            with pytest.raises(ValueError, match='environment_lifecycle_history_changed'):
                await execute('status')
            private(intent_path, retained)
            anchor_path = tmp_path / 'anchors' / ('lifecycle-' + str(wanted.operation_id) + '.json')
            private(anchor_path, {'intent_sha256': '0' * 64})
            with pytest.raises(ValueError, match='environment_lifecycle_history_changed'):
                await execute('status')
            assert remote.writes == writes


@pytest.mark.parametrize('mode,participant_state,opened', [('global', 'active', True), ('closed', 'active', False),
    ('global', 'suspended', False)])
async def test_status_includes_platform_closure(tmp_path, monkeypatch, mode, participant_state, opened):
    from scripts.ops import nebius_environment_lifecycle as lifecycle
    from scripts.ops.nebius_environment_lifecycle_entry import (
        LifecycleCommandV2,
        load_lifecycle_configuration,
    )

    path, sha, body, operation_id, _ = configuration(tmp_path)
    config = load_lifecycle_configuration(path, sha)
    request = LifecycleCommandV2.model_validate(dict(schema_version='loom.environment-lifecycle-command.v2',
        action='status', scope=body['scope'], operation_id=operation_id))
    snapshot = SimpleNamespace(pool=SimpleNamespace(mode=mode), participant=SimpleNamespace(head=SimpleNamespace(state=participant_state)))
    monkeypatch.setattr(lifecycle, '_observe', lambda *args: {})
    monkeypatch.setattr(lifecycle, 'inspection', lambda raw: snapshot)
    policy = SimpleNamespace(host=SimpleNamespace(state_root=tmp_path / 'state', anchor_root=tmp_path / 'anchors'),
        participant=SimpleNamespace(participant_id=request.scope.participant_id))
    result = await lifecycle.execute_connected_lifecycle(request=request, operation=config.operations[request.operation_id],
        inputs=SimpleNamespace(current_release_id=None), policy=policy, clients=SimpleNamespace(connect=None), unchanged=lambda: None)
    assert result['status'] == 'observed' and result['admission_open'] is opened
