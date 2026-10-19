"""Closed delivery consumes real enrollment and source evidence before staging."""
from __future__ import annotations

import copy
import importlib
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_administration_dispatch import _load

from loom_service.pool_management.administration import administer
from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_nebius_enrollment_issuer import prepared
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.ops.test_nebius_development_build_cloud import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud
from tests.support.nebius_phase_api import PhaseAPI
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication


@pytest.mark.parametrize('committed_enrollment', [False, True])
async def test_installation_stages_only_enrolled_owner_closed_and_recovers_exact_effects(
        sessions, tmp_path, platform_inputs, publication, publisher_cloud, committed_enrollment):
    name = 'scripts.ops.nebius_environment_runtime_install'
    assert importlib.util.find_spec(name), 'protected independent runtime installer is missing'
    module = importlib.import_module(name)
    cloud = publisher_cloud
    inputs = copy.deepcopy(platform_inputs)
    inputs[0].update(project_id=cloud.scope['project_id'], quota_parent_id=cloud.scope['tenant_id'], region=cloud.scope['region'])
    async with prepared(sessions, tmp_path, inputs, publication) as (issuer, policy, clients, remote, api):
        profiles = policy.profiles.model_dump(mode='json')
        profiles['task_images'][0]['settings']['registry_repository'] = cloud.repositories[0]
        # This fixture has no authorized cache identity; it tests canonical
        # source + registry delivery, not arbitrary shared-cache credentials.
        profiles['task_images'][0]['settings'].update(cache_bucket=None, cache_secret_name=None)
        profiles = PoolProfileCatalog.model_validate(profiles)
        token = 'runtime-participant-' + 'a' * 40
        import hashlib
        machines = tuple(row.model_copy(update={'token_sha256': hashlib.sha256(token.encode()).hexdigest()}) for row in policy.machines)
        policy = replace(policy, profiles=profiles, machines=machines)
        enrollment = await issuer.issue_enrollment(policy=policy, operation_id=uuid4(), **clients)
        job = _load(policy.host, enrollment)
        if committed_enrollment:
            async with sessions.begin() as session:
                await administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')
        class Resources(PhaseAPI):
            key = staticmethod(_key)

        resources = Resources(policy.host.runtime.binding)
        resources.failure = 'after'  # Every lost create reply must be read back, not retried.
        @contextmanager
        def resource_connection(documents, namespaces, qualify):
            assert set(namespaces) == {'app-red', 'work-red', 'build-red', policy.host.runtime.binding.namespace}
            qualify()
            yield resources
            qualify()
        kwargs = dict(policy=policy, enrollment=enrollment, operation_id=uuid4(), token=token,
            registry_scope=DevelopmentRegistryCloudScope.model_validate(cloud.scope), registry_credential=cloud.credential,
            registry_sdk=None, registry_clients=cloud.clients, resource_connection=resource_connection, **clients)
        if not committed_enrollment:
            with pytest.raises(ValueError, match='environment_runtime_install_unqualified'):
                await module.stage_environment_runtime(**kwargs)
            assert not resources.creates
            return
        result = await module.stage_environment_runtime(**kwargs)
        assert result['status'] == 'runtime_staged_closed'
        assert result['participant_id'] == str(policy.participant.participant_id)
        assert result['admission_open'] is False
        retained = module.load_staged_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        assert retained.plan['workloads']['service']['metadata']['namespace'] == 'app-red'
        assert retained.plan['workloads']['control_plane']['spec']['replicas'] == 0
        assert all(doc['metadata'].get('uid') for doc in retained.resources.values())
        assert retained.plan['foundation_identities']['k8s:Namespace:-:work-red'] == str(policy.participant.execution_namespace.uid)
        originals = copy.deepcopy(remote.writes)
        creates = list(resources.creates)
        assert await module.stage_environment_runtime(**kwargs) == result
        assert resources.creates == creates and remote.writes == originals
        deployments = [doc for doc in resources.resources.values() if doc['kind'] == 'Deployment']
        assert len(deployments) == 1 and deployments[0]['spec']['replicas'] == 0
        assert deployments[0]['metadata']['namespace'] == 'work-red'
        assert not any(doc['kind'] in {'Namespace', 'ResourceQuota', 'Job', 'CronJob'} for doc in resources.resources.values())
        # An expired read-only observation is replaceable, but the original
        # enrollment/mutation authority and runtime create intents are not.
        api.pending = True
        assert await module.stage_environment_runtime(**kwargs) is None
        observations = policy.host.state_root / ('runtime-' + str(kwargs['operation_id'])) / 'observations'
        previous = json.loads((observations / 'current.json').read_text())
        inspection = observations / previous['operation_id'] / 'inspection.json'
        pending = json.loads(inspection.read_text())
        pending['authorization']['issued_at'] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
        pending['authorization']['expires_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        inspection.write_text(json.dumps(pending))
        api.pending = False
        assert await module.stage_environment_runtime(**kwargs) == result
        assert json.loads((observations / 'current.json').read_text())['operation_id'] != previous['operation_id']
        assert _load(policy.host, enrollment) == job
        assert resources.creates == creates and remote.writes == originals
        phase = policy.host.state_root / ('runtime-' + str(kwargs['operation_id'])) / 'material' / 'stage.json'
        saved = phase.read_bytes()
        phase.unlink()
        with pytest.raises(ValueError, match='environment_runtime_retained_unqualified'):
            module.load_staged_environment_runtime(policy=policy, operation_id=kwargs['operation_id'])
        with pytest.raises(ValueError, match='environment_runtime_install_unqualified'):
            await module.stage_environment_runtime(**kwargs)
        assert resources.creates == creates
        phase.write_bytes(saved)
        phase.chmod(0o600)

        # Continue through the actual startup journal, not a second enrollment
        # or restaging operation. Only remote controller/HTTP effects are fake.
        startup = importlib.import_module('scripts.ops.nebius_environment_runtime_start')
        from scripts.ops.nebius_environment_runtime_start import runtime_resource_path

        from tests.ops.test_nebius_development_runtime_transition import RuntimeAPI

        for doc in resources.resources.values():
            remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']] = copy.deepcopy(doc)
        changes = []
        reject = True

        class Workloads(RuntimeAPI):
            def patch_workload(self, key, before, desired):
                try:
                    return super().patch_workload(key, before, desired)
                finally:
                    actual = self.rows[key]
                    remote.kubernetes[runtime_resource_path(actual) + '/' + actual['metadata']['name']] = actual
                    changes.append(key)

        @contextmanager
        def workload_connection(before, targets, namespaces, qualify):
            api = Workloads({key: remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']]
                for key, doc in before.items()})
            api.ready, api.preview_rejected, api.failure = True, reject, 'after'
            api.qualify = qualify
            yield api

        inputs = dict(policy=policy, operation_id=kwargs['operation_id'], workload_connection=workload_connection, **clients)
        selected_path = '/apis/apps/v1/namespaces/app-red/deployments/loom-service'
        original_service = copy.deepcopy(remote.kubernetes[selected_path])
        reads = 0
        def drift_after_foundation_read(request):
            nonlocal reads
            if request.method == 'GET' and request.url.path == selected_path:
                reads += 1
                if reads == 3:
                    remote.kubernetes[selected_path]['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
            return remote.http(request)
        async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(drift_after_foundation_read)) as changed:
            from loom_service.environment_management.kubernetes_provider import (
                KubernetesEnvironmentProvider,
            )

            with pytest.raises(ValueError, match='environment_runtime_start_unqualified'):
                await startup.start_environment_runtime(**{**inputs, 'kubernetes': KubernetesEnvironmentProvider(changed)})
        assert changes == [], 'same-UID drift cannot become a newly authorized original'
        remote.kubernetes[selected_path] = original_service
        assert (await startup.start_environment_runtime(**inputs))['status'] == 'runtime_start_pending'
        assert changes == []
        reject = False
        started = await startup.start_environment_runtime(**inputs)
        assert started['status'] == 'runtime_started_closed'
        assert started['admission_open'] is False
        assert len(changes) == 8
        assert all(key.split(':')[1] in {'app-red', 'work-red'} for key in changes)
        assert await startup.start_environment_runtime(**inputs) == started
        assert len(changes) == 8, 'completed phases must not stop or replace the new runtime again'
        for name in ('loom-service', 'loom-control-plane'):
            assert remote.kubernetes[f'/apis/apps/v1/namespaces/app-red/deployments/{name}']['spec']['replicas'] == 1
        quota = remote.kubernetes['/api/v1/namespaces/work-red/resourcequotas/loom-execution-disabled']
        assert quota['spec']['hard'] == {'pods': '1', 'count/jobs.batch': '0'}
        assert remote.kubernetes['/api/v1/namespaces/build-red/resourcequotas/loom-execution-disabled']['spec']['hard'] == {'pods': '0'}
