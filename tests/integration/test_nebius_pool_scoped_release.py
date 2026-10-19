"""A selected environment releases independently, including retained build work."""
from __future__ import annotations

import copy
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.nebius_pool_contract import PoolActivationV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_observation_continuity import workload
from tests.integration.test_nebius_pool_participant_http import action
from tests.integration.test_nebius_pool_participant_lifecycle import setup
from tests.integration.test_nebius_pool_profile_registry import catalog
from tests.integration.test_nebius_pool_registry import machine
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_task_image_render import build_inputs


def replacement(spec, selected):
    original = catalog(spec, selected)
    mapping = {row.profile_id: uuid4() for row in selected.targets}
    following = selected.model_copy(update={'binding_revision': selected.binding_revision + 1,
        'targets': tuple(row.model_copy(update={'profile_id': mapping[row.profile_id]}) for row in selected.targets)})
    data = original.model_dump(mode='json')
    for family in ('execution', 'task_images', 'application_images'):
        for row in data.get(family, []):
            row['profile_id'] = str(mapping[UUID(row['profile_id'])])
            if family == 'execution':
                row['candidate_sha'] = '2' * 40
            else:
                row['settings']['service_image'] = 'registry.example/service@sha256:' + 'd' * 64
                if family == 'application_images':
                    row['recipe']['trusted_image_ref'] = row['settings']['service_image']
    return following, data


def request_for(selected, kind):
    if kind == 'trial':
        return workload(selected)
    _, body, _ = build_inputs()
    body.update(pool_id=selected.pool_id, participant_revision=selected.binding_revision,
        admission_epoch=selected.admission_epoch, target_id=selected.targets[0].target_id,
        origin=workload(selected).origin.model_dump(mode='json'))
    body['key']['participant_id'] = selected.participant_id
    return PoolTaskImagePrepareV1.model_validate(body)


@pytest.mark.parametrize('kind', ['trial', 'task_image_build'])
async def test_release_keeps_waiting_original_profiles_and_peer_work_dispatchable(sessions, kind):
    _, spec, app, headers, observe, change = await setup(sessions)
    selected, peer = spec.participants[2], spec.participants[1]
    following, profiles = replacement(spec, selected)
    old_request = request_for(selected, kind)
    ready = (await change(selected, 'resume', apply=False)).readiness.model_dump(mode='json')
    ready.update(participant_binding_sha256=digest(following.model_dump(mode='json')),
        catalog_sha256=digest({'participant': following.model_dump(mode='json'), 'profiles': profiles}))
    async with sessions() as session:
        before = (await session.get(NebiusPoolBinding, spec.pool_id)).binding_sha256
        peer_before = copy.deepcopy((await session.get(NebiusPoolParticipant, peer.participant_id)).binding_json)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        path = f'/internal/pools/v1/{spec.pool_id}'
        await observe(full=True)
        queued = await http.post(path + '/prepare', json=old_request.model_dump(mode='json'), headers=headers(selected))
        assert queued.status_code == 200 and queued.json()['phase'] == 'waiting'
        await observe()
        peer_request = workload(peer)
        admitted = await http.post(path + '/prepare', json=peer_request.model_dump(mode='json'), headers=headers(peer))
        assert admitted.status_code == 200 and admitted.json()['phase'] == 'reserved'
        begin = await change(selected, 'begin_release', replacement={'participant': following.model_dump(mode='json'),
            'profiles': profiles, 'publication_sha256': '3' * 64})
        assert begin['state'] == 'suspended'
        committed = await change(selected, 'commit_release', readiness=ready)
        assert committed['state'] == 'ready'
        await change(following, 'activate', readiness=ready)
        resumed = await http.post(path + '/prepare', json=old_request.model_dump(mode='json'), headers=headers(selected))
        assert resumed.status_code == 200, resumed.text
        assert resumed.json()['phase'] == 'reserved'
        for owner, request in ((peer, peer_request), (following, old_request)):
            activated = await http.post(path + '/activate', json=PoolActivationV1(action=action(request),
                not_after=request.deadline_at).model_dump(mode='json'), headers=headers(owner))
            assert activated.status_code == 200 and activated.json()['phase'] == 'create_intent', activated.text
    gateway = await machine(sessions, spec.pool_id, role='gateway')
    journal = PoolGatewayJournal(sessions)
    async with sessions() as session:
        rows = (await session.scalars(select(NebiusPoolRequest))).all()
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).binding_sha256 == before
        assert (await session.get(NebiusPoolParticipant, peer.participant_id)).binding_json == peer_before
    for row in rows:
        if row.plan_json['configmap'] is not None:
            auxiliary = await journal.prepare_create(gateway, row.request_id, kind='ConfigMap')
            await journal.dispatch_create(gateway, auxiliary.effect_id)
            await journal.observe_create(gateway, auxiliary.effect_id, uid=uuid4(), resource_version='11')
        effect = await journal.prepare_create(gateway, row.request_id, kind='Job')
        assert (await journal.dispatch_create(gateway, effect.effect_id)).phase == 'dispatched'
        if row.participant_id == selected.participant_id and kind == 'task_image_build':
            assert effect.document['spec']['template']['spec']['initContainers'][0]['image'] == 'registry.example/service@sha256:' + 'a' * 64


async def test_release_refuses_incompatible_changes_before_suspending_selected_environment(sessions):
    _, spec, _, _, _, change = await setup(sessions)
    selected = spec.participants[2]
    following, profiles = replacement(spec, selected)
    profiles['task_images'][0]['settings']['source_secret_name'] = 'new-secret-not-retained'
    with pytest.raises(ValueError, match='compatib'):
        await change(selected, 'begin_release', replacement={'participant': following.model_dump(mode='json'),
            'profiles': profiles, 'publication_sha256': '3' * 64})
    from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle

    async with sessions() as session:
        assert (await session.get(NebiusPoolParticipantLifecycle, selected.participant_id)).state == 'active'


async def test_release_commit_waits_for_charged_work_without_releasing_it(sessions):
    _, spec, app, headers, _, change = await setup(sessions)
    selected = spec.participants[2]
    following, profiles = replacement(spec, selected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=workload(selected).model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 200 and response.json()['phase'] == 'reserved'
    readiness = (await change(selected, 'resume', apply=False)).readiness
    await change(selected, 'begin_release', replacement={'participant': following.model_dump(mode='json'),
        'profiles': profiles, 'publication_sha256': '3' * 64})
    with pytest.raises(ValueError, match='outstanding'):
        await change(selected, 'commit_release', readiness=readiness)
    async with sessions() as session:
        assert (await session.scalar(select(NebiusPoolRequest))).phase == 'reserved'


async def test_personal_application_builder_retains_exact_source_and_recipe_across_release(environment_registry, application_build_inputs):
    from loom.application_source_upload import ApplicationSourceUploadBindingV1
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch
    from loom_service.application_management.source_registry import ApplicationSourceRegistry
    from tests.integration.test_nebius_application_build_registry import build_registry, verified

    _, sessions, (owner, _), _ = environment_registry
    _, spec, app, headers, observe, change = await setup(sessions, application_recipe=application_build_inputs[0].recipe)
    selected = spec.participants[2]
    profile, = catalog(spec, selected).application_images
    claim = application_build_inputs[0].model_copy(update={'recipe': profile.recipe,
        'source_bucket': profile.settings.source_bucket, 'storage_endpoint': profile.settings.storage_endpoint,
        'storage_region': profile.settings.storage_region, 'registry_repository': profile.settings.registry_repository})
    sources = ApplicationSourceRegistry(sessions, binding=ApplicationSourceUploadBindingV1(
        installation_id=selected.installation_id, data_environment_id=selected.environment_id,
        cluster_id=spec.cluster_id, source_bucket=claim.source_bucket))
    source = await verified(sources, owner)
    builds = build_registry(sessions, sources, claim, pool_id=spec.pool_id, participant_id=selected.participant_id,
        profile_id=profile.profile_id, target_id=profile.target.target_id,
        admission_epoch=selected.admission_epoch, participant_revision=selected.binding_revision)
    build = await builds.create(principal=owner, upload_id=source.upload_id, idempotency_key='retained-source')
    request = await ApplicationBuildDispatch(builds, request_lifetime_seconds=600).freeze(build.build_id, attempt=1)
    following, profiles = replacement(spec, selected)
    readiness = (await change(selected, 'resume', apply=False)).readiness.model_dump(mode='json')
    readiness.update(participant_binding_sha256=digest(following.model_dump(mode='json')),
        catalog_sha256=digest({'participant': following.model_dump(mode='json'), 'profiles': profiles}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        path = f'/internal/pools/v1/{spec.pool_id}'
        await observe(full=True)
        response = await http.post(path + '/prepare', json=request.model_dump(mode='json'),
            headers=headers(selected, 'application_builder'))
        assert response.status_code == 200 and response.json()['phase'] == 'waiting', response.text
        await change(selected, 'begin_release', replacement={'participant': following.model_dump(mode='json'),
            'profiles': profiles, 'publication_sha256': '3' * 64})
        await change(selected, 'commit_release', readiness=readiness)
        await observe()
        await change(following, 'activate', readiness=readiness)
        response = await http.post(path + '/prepare', json=request.model_dump(mode='json'),
            headers=headers(selected, 'application_builder'))
        assert response.status_code == 200 and response.json()['phase'] == 'reserved', response.text
        activated = await http.post(path + '/activate', json=PoolActivationV1(action=action(request),
            not_after=request.deadline_at).model_dump(mode='json'), headers=headers(selected, 'application_builder'))
        assert activated.status_code == 200 and activated.json()['phase'] == 'create_intent', activated.text
    async with sessions() as session:
        row = await session.scalar(select(NebiusPoolRequest))
        assert row.priority == 3
        assert row.plan_json['job']['metadata']['labels']['loom.application-build-id'] == str(build.build_id)
        assert row.plan_json['job']['spec']['template']['spec']['initContainers'][0]['image'] == profile.recipe.trusted_image_ref
