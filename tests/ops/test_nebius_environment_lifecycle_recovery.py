"""Installed recovery selects retained runtime history before issuing new phases."""
from __future__ import annotations

import copy
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from scripts.ops import nebius_environment_lifecycle as lifecycle
from scripts.ops.nebius_environment_installed_import import (
    ImportedEnvironmentRuntimeV2,
    load_imported_environment_runtime,
    prepare_installed_legacy_foundation_import,
)
from scripts.ops.nebius_environment_lifecycle_entry import (
    LifecycleCommandV2,
    load_lifecycle_configuration,
)
from scripts.ops.nebius_environment_lifecycle_inputs import LifecycleClients
from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
from scripts.ops.nebius_pool_administration_dispatch import (
    PoolAdministrationHostV2,
    PoolAdministrationScopeV2,
    retain_authorized_operation,
)
from scripts.ops.nebius_pool_administration_job import PoolAdministrationRuntimeV2
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2, _grant, _policy
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_environment_installed_import import Readback, installed_source, observe
from tests.ops.test_nebius_environment_lifecycle_entry import private
from tests.ops.test_nebius_environment_release import release_inputs as release_inputs
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantEnrollmentV2,
    PoolParticipantOperationV2,
    PoolParticipantReadinessV2,
)


@pytest.fixture
async def imported_runtime(tmp_path, platform_inputs, publication, cloud, runtime_request, release_inputs):
    source = installed_source((platform_inputs[0], release_inputs['previous'].candidate, release_inputs['previous'].profile),
        publication, cloud, wanted=runtime_request['declaration'])
    participant = runtime_request['participant']
    resources = copy.deepcopy(source.resources) | release_inputs['resources']
    for namespace in (participant.execution_namespace, participant.build_namespace):
        resources['Namespace:-:' + namespace.name]['metadata']['uid'] = str(namespace.uid)
    runtime = ImportedEnvironmentRuntimeV2(operation_id=uuid4(), participant=participant, profiles=runtime_request['profiles'],
        application_publication=source.publication, active_head=uuid4())
    source = source.model_copy(update={'resources': resources, 'runtime': runtime})
    directory = tmp_path / 'imported'
    reference = prepare_installed_legacy_foundation_import(state_dir=directory, source=source, operation_id=uuid4(),
        candidate=release_inputs['previous'].candidate, profile=release_inputs['previous'].profile)
    await observe(directory, reference, source, cloud, Readback(source))
    binding = PlatformNamespaceBindingV2(installation_id=str(participant.installation_id), namespace='platform-services',
        namespace_uid=str(uuid4()), kube_system_uid=str(source.declaration.kube_system_uid))
    runtime_policy = PoolAdministrationRuntimeV2(binding=binding,
        service_image=release_inputs['previous'].candidate['images']['service']['image_ref'], database_secret='platform-ledger',
        database_url_key='admin-url', database_ca_key='ca.crt', node_selector={'platform.example/role': 'control'},
        tolerations=(), region=source.declaration.platform_config['region'])
    host = PoolAdministrationHostV2(runtime=runtime_policy, scope=PoolAdministrationScopeV2(
        **{name: getattr(participant, name) for name in ('installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation')},
        authority=participant.environment_class), state_root=tmp_path / 'operations', anchor_root=tmp_path / 'anchors',
        connection=NebiusKubernetesConnection(endpoint='https://api.cluster.test', ca_file=tmp_path / 'ca.pem',
            credentials_file=tmp_path / 'credentials.json'))
    policy = PoolEnrollmentPolicyV2(host=host, management_origin='https://platform.example.com', foundation_dir=directory,
        foundation=reference, application_publication=source.publication, manager_publication=source.publication,
        participant=participant, profiles=runtime.profiles, machines=(runtime_request['machine'],))
    return policy, runtime


def readiness(policy):
    now = datetime.now(UTC)
    return PoolParticipantReadinessV2(observed_at=now, valid_until=now + timedelta(minutes=4),
        pool_authority_sha256='a' * 64, participant_binding_sha256=digest(policy.participant.model_dump(mode='json')),
        catalog_sha256=digest({'participant': policy.participant.model_dump(mode='json'), 'profiles': policy.profiles.model_dump(mode='json')}),
        observer_machine_id=uuid4(), gateway_machine_id=uuid4(), observer_credential_epoch=1, gateway_credential_epoch=1,
        capabilities=('physical-v1', 'registry-v1', 'scoped-lifecycle-v1'))


def retain_predecessor(policy, action, expected_head):
    if action == 'enroll':
        request = PoolParticipantEnrollmentV2(schema_version='loom.pool-participant-enrollment.v2', operation_id=uuid4(),
            pool_authority_sha256='a' * 64, foundation_sha256=policy.foundation.plan_sha256,
            participant=policy.participant, profiles=policy.profiles, machines=policy.machines)
    else:
        request = PoolParticipantOperationV2(schema_version='loom.pool-participant-operation.v2', operation_id=uuid4(),
            pool_id=policy.participant.pool_id, participant_id=policy.participant.participant_id, incarnation=policy.participant.incarnation,
            expected_head=expected_head, expected_binding_sha256=digest(policy.participant.model_dump(mode='json')),
            pool_authority_sha256='a' * 64, action=action, readiness=readiness(policy) if action == 'ready' else None)
    now = datetime.now(UTC)
    grant = _grant(policy, request, uuid4(), issued_at=now, expires_at=now + timedelta(minutes=4))
    return retain_authorized_operation(host=policy.host, request=request, authorization=grant, evidence_sha256='b' * 64)


async def execute(tmp_path, monkeypatch, policy, runtime_id, predecessor, kind, clients, *, enrollment_id=None):
    platform_host = _policy(policy)['host']
    platform_host['scope'] = dict(installation_id=str(policy.participant.installation_id), pool_id=str(policy.participant.pool_id),
        authority='platform')
    platform_authority = dict(schema_version='loom.pool-administration-reference.v2', **platform_host['scope'],
        operation_id=str(uuid4()), authorization_id=str(uuid4()), request_sha256='a' * 64, document_sha256='b' * 64)
    github, cloud_file = tmp_path / 'github.json', tmp_path / 'cloud.json'
    inputs = dict(schema_version='loom.environment-lifecycle-inputs.v2', enrollment=_policy(policy),
        github_token=dict(path=str(github), sha256=private(github, {})),
        cloud_credentials=dict(path=str(cloud_file), sha256=private(cloud_file, {})), registry_prefix='registry.example',
        runtime_operation_id=str(runtime_id), enrollment_operation_id=str(enrollment_id or uuid4()),
        predecessor=predecessor.model_dump(mode='json'), platform=dict(host=platform_host, authority=platform_authority,
            resources={}, manager_key='manager', gateway_key='gateway', gateway_token_key='token', catalog_key='catalog'))
    input_path, config_path = tmp_path / 'inputs.json', tmp_path / 'configuration.json'
    operation_id = uuid4()
    config_sha = private(config_path, dict(schema_version='loom.environment-lifecycle-configuration.v2',
        scope=policy.host.scope.model_dump(mode='json'), operations={str(operation_id): dict(kind=kind,
            inputs_path=str(input_path), inputs_sha256=private(input_path, inputs))}))
    config = load_lifecycle_configuration(config_path, config_sha)
    request = LifecycleCommandV2(schema_version='loom.environment-lifecycle-command.v2', action='apply',
        scope=policy.host.scope, operation_id=operation_id)
    @asynccontextmanager
    async def connected(inputs, selected, unchanged):
        assert selected == policy
        yield clients
    monkeypatch.setattr(lifecycle, 'lifecycle_clients', connected)
    return await lifecycle.execute_lifecycle(configuration=config, request=request, operation=config.operations[operation_id],
        unchanged=lambda: load_lifecycle_configuration(config_path, config_sha))


@pytest.mark.parametrize('changed_head,policy_ready', [(False, True), (True, True), (False, False)])
async def test_imported_baseline_resume_uses_real_retained_import_without_fresh_install_journals(
        tmp_path, monkeypatch, imported_runtime, changed_head, policy_ready):
    from scripts.ops import nebius_environment_release_readiness as release_readiness
    from scripts.ops import nebius_environment_runtime_start as startup

    policy, runtime = imported_runtime
    predecessor = retain_predecessor(policy, 'suspend', runtime.active_head)
    adopted = load_imported_environment_runtime(policy=policy, operation_id=runtime.operation_id)
    head = uuid4() if changed_head else predecessor.operation_id
    snapshot = SimpleNamespace(participant=SimpleNamespace(binding=policy.participant, profiles=policy.profiles,
        head=SimpleNamespace(operation_id=head, state='suspended')))
    monkeypatch.setattr(lifecycle, '_observe', lambda *args, **kwargs: {})
    monkeypatch.setattr(lifecycle, 'inspection', lambda raw: snapshot)
    monkeypatch.setattr(startup, 'load_started_environment_runtime', lambda **kwargs: pytest.fail('imports have no fresh-install history'))
    calls = []
    proof = readiness(policy)
    async def qualify(**kwargs):
        assert kwargs['resources'] == adopted.resources
        assert kwargs['foundation_identities'] == adopted.staged.plan['foundation_identities']
        assert kwargs['snapshot'] is snapshot
        kwargs['qualify']()
        calls.append('readiness')
        return {'readiness': proof.model_dump(mode='json')}
    async def transition(**kwargs):
        assert kwargs['policy'] == policy and kwargs['readiness'] == proof
        assert kwargs['expected_head'] == predecessor.operation_id and kwargs['action'] == 'resume'
        calls.append('resume')
        return dict(status='complete', admission_open=True)
    def native_policy(**kwargs):
        assert kwargs['resources'] == adopted.resources
        kwargs['qualify']()
        calls.append('native-policy')
        return policy_ready
    monkeypatch.setattr(lifecycle, 'resume_build_policy_ready', native_policy)
    monkeypatch.setattr(release_readiness, 'release_readiness', qualify)
    monkeypatch.setattr(lifecycle, 'transition_participant', transition)
    clients = LifecycleClients(None, None, None, None, None, None)
    if changed_head:
        with pytest.raises(ValueError, match='environment_resume_predecessor_changed'):
            await execute(tmp_path, monkeypatch, policy, runtime.operation_id, predecessor, 'resume', clients)
        assert calls == []
    else:
        result = await execute(tmp_path, monkeypatch, policy, runtime.operation_id, predecessor, 'resume', clients)
        assert result['status'] == ('complete' if policy_ready else 'pending')
        assert result['admission_open'] is policy_ready
        assert calls == ['readiness', 'native-policy', *(['resume'] if policy_ready else [])]
    assert not (policy.host.state_root / ('runtime-' + str(runtime.operation_id))).exists()


@pytest.mark.parametrize('wrong_enrollment', [False, True])
async def test_new_activation_from_ready_reuses_started_runtime_without_reenrollment_or_restage(
        tmp_path, monkeypatch, imported_runtime, wrong_enrollment):
    from scripts.ops import nebius_environment_runtime_start as startup

    policy, runtime = imported_runtime
    enrollment = retain_predecessor(policy, 'enroll', None)
    predecessor = retain_predecessor(policy, 'ready', uuid4() if wrong_enrollment else enrollment.operation_id)
    retained = SimpleNamespace(staged=SimpleNamespace(plan={'enrollment': enrollment.model_dump(mode='json')}))
    def loaded(**kwargs):
        assert kwargs == dict(policy=policy, operation_id=runtime.operation_id)
        return retained
    monkeypatch.setattr(startup, 'load_started_environment_runtime', loaded)
    for name in ('issue_enrollment', 'stage_environment_runtime', 'start_environment_runtime'):
        monkeypatch.setattr(lifecycle, name, lambda **kwargs: pytest.fail('ready predecessor must reuse completed runtime'))
    calls = []
    async def activate(**kwargs):
        assert kwargs['predecessor'] == predecessor and kwargs['operation_id'] == runtime.operation_id
        assert kwargs['activation_id'] not in {predecessor.operation_id, enrollment.operation_id, runtime.operation_id}
        calls.append(kwargs['activation_id'])
        return dict(status='environment_active', admission_open=True)
    monkeypatch.setattr(lifecycle, 'activate_environment_runtime', activate)
    clients = LifecycleClients(None, None, None, None, None, None)
    if wrong_enrollment:
        with pytest.raises(ValueError, match='environment_lifecycle_ready_predecessor_unqualified'):
            await execute(tmp_path, monkeypatch, policy, runtime.operation_id, predecessor, 'activate', clients,
                enrollment_id=enrollment.operation_id)
        assert calls == []
    else:
        result = await execute(tmp_path, monkeypatch, policy, runtime.operation_id, predecessor, 'activate', clients,
            enrollment_id=enrollment.operation_id)
        assert result['status'] == 'complete' and result['admission_open'] is True
        cursor = json.loads((policy.host.state_root / ('lifecycle-operation-' + str(calls[0])) / 'cursor.json').read_text())
        assert cursor == {'enroll': enrollment.model_dump(mode='json'), 'stage': True, 'start': True}
