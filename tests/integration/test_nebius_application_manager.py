"""Real owner API/registry controls personal applications, never legacy stacks."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import httpx
import pytest

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_completion import evidence
from tests.integration.test_nebius_application_completion import stopped_context as stopped_context
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def manager(registry, platform_inputs, *, plan=None, releases=None, authority=None):
    from loom.nebius_application_contract import ApplicationReleaseV1, SharedDevelopmentBindingV1
    from loom_service.application_management.manager import ApplicationManager

    _, release, shared, foundation = inputs(platform_inputs)
    if plan is not None:
        release = ApplicationReleaseV1.model_validate(plan['release'])
        shared = SharedDevelopmentBindingV1.model_validate(plan['shared'])
    authority = authority or ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace='loom-nebius-management',
        cluster_id=shared.cluster_id, data_environment_id=shared.data_environment_id, shared_namespace=shared.platform_namespace)
    return ApplicationManager(registry, foundation=foundation, shared=shared, authority=authority,
        releases=(release,) if releases is None else releases), release


async def test_manager_replay_and_stop_do_not_depend_on_available_release_catalog(applications, platform_inputs):
    from loom.nebius_application_contract import (
        ApplicationCreateRequestV1,
        ApplicationOperationRequestV1,
    )

    registry, _, (alice, bob), _, _, _ = applications
    service, release = manager(registry, platform_inputs)
    payload = ApplicationCreateRequestV1(slug='alice', release_id=release.release_id)
    operation = await service.create(alice, payload, idempotency_key='create')
    status = await registry.status(operation.application_id, principal=alice)
    assert status.operation == operation and status.registration.owner_user_id == alice.user_id
    unavailable, _ = manager(registry, platform_inputs, releases=())
    assert await unavailable.create(alice, payload, idempotency_key='create') == operation
    with pytest.raises(ManagementError, match='application_release_unavailable'):
        await unavailable.create(alice, payload, idempotency_key='different')
    request = ApplicationOperationRequestV1(action='suspend', expected_generation=1)
    with pytest.raises(ManagementError, match='application_forbidden'):
        await unavailable.transition(bob, operation.application_id, request, idempotency_key='stolen')
    stopped = await unavailable.transition(alice, operation.application_id, request, idempotency_key='stop')
    assert stopped.action == 'suspend' and stopped.deployment_generation == 2
    assert await unavailable.transition(alice, operation.application_id, request, idempotency_key='stop') == stopped
    assert (await registry.status(operation.application_id, principal=alice)).operation == stopped


async def test_update_from_actual_ready_completion_preserves_identity_and_freezes_new_images(ready_context, platform_inputs):
    from loom.nebius_application_contract import ApplicationOperationRequestV1

    coordinator, registry, _, alice, lease, _, _, _ = ready_context
    plan = await registry.frozen_plan(lease)
    await coordinator.start(lease)
    _, original = manager(registry, platform_inputs, plan=plan)
    release = original.model_copy(update={'release_id': uuid4(), 'source_digest': 'sha256:' + '1' * 64,
        'service_image_ref': 'cr.eu-north1.nebius.cloud/test/feature@sha256:' + '2' * 64})
    service, _ = manager(registry, platform_inputs, plan=plan, releases=(release,), authority=coordinator.runtime.authority)
    request = ApplicationOperationRequestV1(action='update', expected_generation=1, release_id=release.release_id)
    operation = await service.transition(replace(alice, scopes=['submit']), lease.application_id,
        request, idempotency_key='update')
    current = await registry.claim(operation.operation_id)
    new_plan = await registry.frozen_plan(current)
    assert new_plan['registration']['incarnation'] == plan['registration']['incarnation']
    assert new_plan['registration']['application_namespace'] == 'loom-dev-alice'
    assert new_plan['registration']['deployment_generation'] == new_plan['registration']['access_generation'] == 2
    assert new_plan['shared'] == plan['shared'] and new_plan['release']['service_image_ref'] == release.service_image_ref
    unavailable, _ = manager(registry, platform_inputs, plan=plan, releases=())
    replay = await unavailable.transition(alice, lease.application_id, request, idempotency_key='update')
    assert replay.operation_id == operation.operation_id and replay.phase == 'running'
    with pytest.raises(ManagementError, match='application_generation_conflict'):
        await service.transition(alice, lease.application_id, request, idempotency_key='stale')


async def test_resume_uses_same_release_after_real_stopped_completion(stopped_context, platform_inputs):
    from loom.nebius_application_contract import ApplicationOperationRequestV1

    registry, _, alice, lease, runtime, *_ = stopped_context
    plan = await registry.frozen_plan(lease)
    await registry.complete_stopped(lease, await evidence(stopped_context))
    service, release = manager(registry, platform_inputs, plan=plan, authority=runtime.authority)
    operation = await service.transition(alice, lease.application_id,
        ApplicationOperationRequestV1(action='resume', expected_generation=2), idempotency_key='resume')
    assert operation.action == 'resume' and operation.deployment_generation == operation.access_generation == 3
    current = await registry.claim(operation.operation_id)
    assert (await registry.frozen_plan(current))['registration']['release_id'] == str(release.release_id)


async def test_management_api_authenticates_owner_and_never_exposes_private_plan(applications, platform_inputs,
        isolated_migration_postgres_url):
    from loom.db.schema import TeamMembership, User
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.password_auth import hash_password

    registry, factory, (alice, bob), _, _, _ = applications
    service, release = manager(registry, platform_inputs)
    async with factory.begin() as session:
        for principal, name in ((alice, 'alice'), (bob, 'bob')):
            user = await session.get(User, principal.user_id)
            user.password_hash = hash_password(name + '-owner-passphrase')
            session.add(TeamMembership(user_id=principal.user_id, team_id=principal.team_id, role='owner'))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url=isolated_migration_postgres_url, auth_local_http=False, public_base_url='https://management.example.com'))
    async with app.router.lifespan_context(app):
        app.state.application_manager = service
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example.com') as a, \
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example.com') as b:
            assert (await a.get('/api/v1/applications')).status_code == 401
            for client, name in ((a, 'alice'), (b, 'bob')):
                login = await client.post('/api/v1/auth/login', json={'username': name, 'password': name + '-owner-passphrase'})
                assert login.status_code == 200
                client.headers['X-Loom-CSRF'] = login.json()['csrf_token']
            payload = {'slug': 'alice', 'release_id': str(release.release_id)}
            result = await a.post('/api/v1/applications', json=payload, headers={'Idempotency-Key': 'api'})
            assert result.status_code == 202, result.text
            application_id, operation_id = result.json()['application_id'], result.json()['operation_id']
            assert (await a.post('/api/v1/applications', json=payload, headers={'Idempotency-Key': 'api'})).json() == result.json()
            status = await a.get(f'/api/v1/applications/{application_id}')
            assert set(status.json()) == {'registration', 'operation'}
            assert status.json()['registration']['owner_user_id'] == str(alice.user_id)
            assert status.json()['operation']['phase'] == 'pending'
            assert (await b.get(f'/api/v1/applications/{application_id}')).status_code == 403
            assert (await b.get(f'/api/v1/application-operations/{operation_id}')).status_code == 403
            assert (await b.get('/api/v1/applications')).json() == {'items': []}
            assert (await a.post('/api/v1/applications', json=payload | {'namespace': 'loom-prod'},
                headers={'Idempotency-Key': 'unsafe'})).status_code == 422
            request = {'action': 'suspend', 'expected_generation': 1}
            path = f'/api/v1/applications/{application_id}/operations'
            assert (await b.post(path, json=request, headers={'Idempotency-Key': 'stolen'})).status_code == 403
            stop = await a.post(path, json=request, headers={'Idempotency-Key': 'stop'})
            assert stop.status_code == 202 and stop.json()['deployment_generation'] == 2
            assert (await a.get('/api/v1/applications')).json()['items'][0]['desired_state'] == 'suspended'
            assert (await b.post(f'/api/v1/application-operations/{operation_id}/retry')).status_code == 403


async def test_application_routes_are_management_only():
    from fastapi import FastAPI

    from loom_service.app import register_api_routes

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://personal.example.com') as client:
        assert (await client.get('/api/v1/applications')).status_code == 404
