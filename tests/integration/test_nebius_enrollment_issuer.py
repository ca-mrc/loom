"""Real publication/foundation/SQL qualification precedes closed enrollment grants."""
from __future__ import annotations

import copy
import importlib
import json
import os
import subprocess
import sys
import tomllib
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from uuid import UUID, uuid4, uuid5

import httpx
import pytest

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.administration import administer
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_nebius_pool_participant_lifecycle import platform
from tests.integration.test_nebius_pool_profile_registry import catalog as profile_catalog
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.ops.test_nebius_environment_foundation import Remote, run
from tests.ops.test_nebius_foundation_readiness import database_controller
from tests.support.nebius_phase_api import PhaseAPI
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import HEAD, github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_environment_foundation import ROOT, declaration


def issuer():
    name = 'scripts.ops.nebius_pool_enrollment_issuer'
    assert importlib.util.find_spec(name), 'protected publication/foundation enrollment issuer is missing'
    return importlib.import_module(name)


@asynccontextmanager
async def prepared(sessions, tmp_path, platform_inputs, publication):
    subject = issuer()
    from scripts.ops import nebius_environment_foundation as foundations
    from scripts.ops import nebius_pool_administration_dispatch as dispatch
    from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
    from scripts.ops.nebius_pool_administration_job import (
        PoolAdministrationRuntimeV2,
        administration_documents,
    )

    _, spec, _, _, _, _ = await platform(sessions)
    selected = spec.participants[2]
    reference, responses, payload, keyring, candidate = publication
    pinned = ProtectedPublication.model_validate(reference)
    async with httpx.AsyncClient(transport=github_transport(responses, payload)) as github:
        catalog = GitHubCandidateCatalog(github, token='test-github-secret', publications=[pinned],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        bundle = await catalog.resolve(pinned.candidate_id)
        config = copy.deepcopy(platform_inputs[0])
        config.update(cluster_id=spec.cluster_id, execution_node_group_id=spec.node_group_id)
        wanted = declaration((config, bundle.candidate, bundle.profile))
        identity = wanted.registration.model_copy(update={name: getattr(selected, name)
            for name in ('installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation')})
        identity = identity.model_copy(update={'target_id': selected.targets[0].target_id})
        config = wanted.platform_config
        config.update(registration=identity.model_dump(mode='json'), target_id=identity.target_id)
        wanted = EnvironmentFoundationDeclarationV2.model_validate(wanted.model_dump() | {
            'registration': identity.model_dump(), 'platform_config_json': json.dumps(config)})
        state = tmp_path / 'foundation'
        foundation = foundations.prepare_foundation(state_dir=state, declaration=wanted,
            candidate=bundle.candidate, profile=bundle.profile, keyring=json.loads(keyring), repo_root=ROOT)
        remote = Remote(state)
        observed = await run(state, foundation, remote)
        database_controller(remote)
        selected = PoolParticipantV1.model_validate(selected.model_dump() | {
            'execution_namespace': {'name': identity.execution_namespace, 'uid': observed.namespace_uids[identity.execution_namespace]},
            'build_namespace': {'name': identity.build_namespace, 'uid': observed.namespace_uids[identity.build_namespace]}})
        profiles = profile_catalog(spec, spec.participants[2]).model_dump(mode='json')
        profiles['image_admission_keyring'] = json.loads(keyring)
        for row in profiles['execution']:
            row.update(candidate_sha=candidate['candidate_sha'], runtime_image_ref=bundle.profile['runtime_image_ref'],
                runtime_binary_sha256=bundle.profile['runtime_binary_sha256'])
            row['runtime']['namespace'] = selected.execution_namespace.name
        for row in profiles['task_images']:
            row['target']['namespace'] = row['settings']['namespace'] = selected.build_namespace.name
            row['settings']['service_image'] = candidate['images']['service']['image_ref']
        runtime = PoolAdministrationRuntimeV2(binding=PlatformNamespaceBindingV2(
            installation_id=str(selected.installation_id), namespace='platform-services-east', namespace_uid=str(uuid4()),
            kube_system_uid=str(wanted.kube_system_uid)), service_image=candidate['images']['service']['image_ref'],
            database_secret='platform-ledger-credentials', database_url_key='admin-url', database_ca_key='ca.crt',
            node_selector={'platform.example/role': 'control'}, tolerations=(), region='eu-north1')
        host = dispatch.PoolAdministrationHostV2(runtime=runtime,
            scope=dispatch.PoolAdministrationScopeV2(installation_id=selected.installation_id, pool_id=selected.pool_id,
                participant_id=selected.participant_id, environment_id=selected.environment_id, incarnation=selected.incarnation,
                authority=selected.environment_class), state_root=tmp_path / 'operations', anchor_root=tmp_path / 'anchors',
            connection=NebiusKubernetesConnection(endpoint='https://api.cluster.test', ca_file=tmp_path / 'ca.pem',
                credentials_file=tmp_path / 'credentials.json'))
        policy = subject.PoolEnrollmentPolicyV2(host=host, foundation_dir=state, foundation=foundation,
            application_publication=pinned, manager_publication=pinned, participant=selected,
            profiles=PoolProfileCatalog.model_validate(profiles),
            machines=tuple(row for row in spec.machines if row.participant_id == selected.participant_id))

        class API(PhaseAPI):
            request = None
            pending = False

            def administration_report(self, state_dir):
                if self.pending:
                    return None
                assert self.request.action == 'status', 'issuer must not apply before issuing a grant'
                documents = administration_documents(self.request)
                config = next(row for row in documents.values() if row['kind'] == 'ConfigMap')
                job = next(row for row in documents.values() if row['kind'] == 'Job')
                process = tmp_path / 'process'
                process.mkdir(exist_ok=True)
                for name, value in config['data'].items():
                    (process / name).write_text(value)
                env = dict(os.environ)
                for item in job['spec']['template']['spec']['containers'][0]['env']:
                    if 'valueFrom' in item:
                        env[item['name']] = sessions.kw['bind'].url.render_as_string(hide_password=False)
                    elif item['value'].startswith('/var/run/loom-pool-administration/'):
                        env[item['name']] = str(process / item['value'].rsplit('/', 1)[-1])
                    else:
                        env[item['name']] = item['value']
                result = subprocess.run([sys.executable, '-m', 'loom_service.pool_management.administration'],
                    env=env, capture_output=True, text=True, timeout=30)
                assert result.returncode == 0, result.stdout + result.stderr
                return {'job_uid': str(uuid4()), 'pod_uid': str(uuid4()), 'receipt': json.loads(result.stdout)}

        api = API(runtime.binding)

        @contextmanager
        def connect(request):
            api.request = request
            yield api

        token = tomllib.loads(json.loads((state / 'material.json').read_text())['loom-admin-secret']['secrets.toml'])['admin']['token']

        def public(request):
            assert request.headers['Authorization'] == 'Bearer ' + token
            return httpx.Response(200, json={'status': 'ready', 'postgres': 'ready', 'object_store': 'ready',
                'environment': 'development', 'namespace': 'app-red', 'blockers': []})

        async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as kube, \
                httpx.AsyncClient(transport=httpx.MockTransport(public)) as public_http:
            yield subject, policy, dict(catalog=catalog, kubernetes=KubernetesEnvironmentProvider(kube),
                cloud=remote, public_http=public_http, connect=connect), remote, api


async def test_qualified_publication_and_observed_foundation_reach_real_closed_enrollment(sessions, tmp_path, platform_inputs, publication):
    from scripts.ops.nebius_pool_administration_dispatch import _load

    async with prepared(sessions, tmp_path, platform_inputs, publication) as (module, policy, inputs, remote, api):
        operation_id = uuid4()
        original_writes = copy.deepcopy(remote.writes)
        reference = await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs)
        assert reference is not None
        job = _load(policy.host, reference)
        assert job.request.participant == policy.participant
        assert job.request.foundation_sha256 != policy.foundation.plan_sha256
        async with sessions.begin() as session:
            result = await administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')
        assert result['receipt']['state'] == 'enrolled'
        assert result['receipt']['participant_id'] == str(policy.participant.participant_id)
        assert await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs) == reference
        assert len(api.creates) == 2 and remote.writes == original_writes


@pytest.mark.parametrize('damage', ['failed_ci', 'manager_image', 'host_class', 'namespace_uid', 'cluster_uid',
    'profile_image', 'physical_pool', 'publication_binding'])
async def test_unqualified_evidence_never_retains_an_apply_grant(sessions, tmp_path, platform_inputs, publication, damage):
    async with prepared(sessions, tmp_path, platform_inputs, publication) as (module, policy, inputs, remote, _api):
        if damage == 'failed_ci':
            publication[1][f'commits/{HEAD}/check-runs']['check_runs'][0]['conclusion'] = 'failure'
        elif damage == 'manager_image':
            policy = replace(policy, host=replace(policy.host, runtime=replace(policy.host.runtime,
                service_image='registry.example/foreign@sha256:' + 'f' * 64)))
        elif damage == 'host_class':
            policy = replace(policy, host=replace(policy.host, scope=policy.host.scope.model_copy(update={'authority': 'production'})))
        elif damage == 'namespace_uid':
            remote.kubernetes['/api/v1/namespaces/work-red']['metadata']['uid'] = str(uuid4())
        elif damage == 'cluster_uid':
            policy = replace(policy, host=replace(policy.host, runtime=replace(policy.host.runtime,
                binding=replace(policy.host.runtime.binding, kube_system_uid=str(uuid4())))))
        elif damage == 'profile_image':
            profiles = policy.profiles.model_dump(mode='json')
            profiles['task_images'][0]['settings']['service_image'] = 'registry.example/foreign@sha256:' + 'f' * 64
            policy = replace(policy, profiles=PoolProfileCatalog.model_validate(profiles))
        elif damage == 'physical_pool':
            policy = replace(policy, participant=policy.participant.model_copy(update={'admission_epoch': 99}))
        else:
            policy = replace(policy, application_publication=policy.application_publication.model_copy(update={'candidate_id': UUID(int=42)}))
        with pytest.raises(ValueError, match='enrollment'):
            await module.issue_enrollment(policy=policy, operation_id=uuid4(), **inputs)
        assert not policy.host.anchor_root.exists() or not list(policy.host.anchor_root.iterdir())


async def test_unfinished_inspection_remains_pending_and_recovers_original_job(sessions, tmp_path, platform_inputs, publication):
    async with prepared(sessions, tmp_path, platform_inputs, publication) as (module, policy, inputs, _remote, api):
        operation_id = uuid4()
        api.pending = True
        assert await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs) is None
        assert not policy.host.anchor_root.exists()
        api.pending = False
        assert await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs) is not None
        assert len(api.creates) == 2, 'recovery must not make another inspection Job'


async def test_published_runtime_cannot_gain_unpublished_execution_capabilities(sessions, tmp_path, platform_inputs, publication):
    async with prepared(sessions, tmp_path, platform_inputs, publication) as (module, policy, inputs, _remote, _api):
        profiles = policy.profiles.model_dump(mode='json')
        profiles['execution'][0]['execution_class']['supports_task_web_egress'] = True
        # Use a valid independently constructed capability class: image identity
        # alone must not authorize capabilities absent from its publication.
        from loom.execution_contract import nebius_cpu_execution_class

        foreign = nebius_cpu_execution_class(supports_task_web_egress=True)
        profiles['execution'][0].update(execution_class=foreign.model_dump(mode='json'), execution_class_id=foreign.class_id)
        policy = replace(policy, profiles=PoolProfileCatalog.model_validate(profiles))
        with pytest.raises(ValueError, match='enrollment'):
            await module.issue_enrollment(policy=policy, operation_id=uuid4(), **inputs)
        assert not policy.host.anchor_root.exists()


async def test_lost_reply_before_grant_retention_recovers_only_the_original_qualified_intent(
        sessions, tmp_path, platform_inputs, publication, monkeypatch):
    async with prepared(sessions, tmp_path, platform_inputs, publication) as (module, policy, inputs, _remote, _api):
        operation_id = uuid4()
        original = module.retain_authorized_operation

        def lose_reply(**kwargs):
            raise OSError('injected-before-retention')

        monkeypatch.setattr(module, 'retain_authorized_operation', lose_reply)
        with pytest.raises(ValueError, match='enrollment'):
            await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs)
        monkeypatch.setattr(module, 'retain_authorized_operation', original)
        issued = policy.host.state_root / ('enrollment-' + str(operation_id)) / 'issued.json'
        retained = json.loads(issued.read_text())
        retained['request']['operation_id'] = str(uuid4())
        retained['authorization']['request_sha256'] = digest(retained['request'])
        # A private journal must not silently retarget a prepared operation after
        # a lost reply, even if the substituted grant is internally consistent.
        assert retained['authorization']['authorization_id'] == str(uuid5(operation_id, 'enroll-authorization'))
        issued.write_text(json.dumps(retained))
        with pytest.raises(ValueError, match='enrollment'):
            await module.issue_enrollment(policy=policy, operation_id=operation_id, **inputs)
        assert not policy.host.anchor_root.exists()
