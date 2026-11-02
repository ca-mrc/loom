"""Normal persistent cache controls do not change physical execution authority."""
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from loom.db.nebius_cache_policy_schema import NebiusCacheRuntimePolicy
from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom_service.pool_management.cache_policy import (
    PoolCachePolicyV1,
    effective_task_profile,
)
from loom_service.pool_management.cache_policy import cache_policy as _cache_policy
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_profile_registry import setup
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


async def additional_pool(sessions, installation_id, *, endpoint=None, cache_bucket=None):
    from sqlalchemy import update

    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.installation import PoolInstallation, register_installation
    from loom_service.pool_management.profile_registry import store_participant_catalog
    from tests.integration.test_nebius_pool_installation import installation
    from tests.integration.test_nebius_pool_profile_registry import catalog

    raw, _ = installation(('development',))
    raw.update(pool_id=str(uuid4()), installation_id=str(installation_id), cluster_id='other-cluster', node_group_id='other-group')
    raw['node_selector']['nebius.com/node-group-id'] = 'other-group'
    for row in raw['participants']:
        row.update(pool_id=raw['pool_id'], installation_id=raw['installation_id'])
    for family, runtime in [('execution', 'runtime'), ('task_images', 'target')]:
        for row in raw['profiles'][family]:
            row[runtime]['node_selector'] = dict(raw['node_selector'])
            if family == 'task_images' and endpoint is not None:
                row['settings'].update(storage_endpoint=endpoint, cache_bucket=cache_bucket)
    spec = PoolInstallation.model_validate(raw)
    async with sessions.begin() as session:
        await register_installation(session, spec)
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        binding = dict(pool.binding_json, observation_binding='physical-v1', profile_source='registry-v1')
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(
            policy_revision=pool.policy_revision+1, binding_json=binding, binding_sha256=digest(binding)))
        for participant in spec.participants:
            await store_participant_catalog(session, participant, catalog(spec, participant))
    return spec


def request(spec, **values):
    return PoolCachePolicyV1(operation_id=uuid4(), installation_id=spec.installation_id,
        pool_id=spec.pool_id, profile_id=spec.profiles.task_images[0].profile_id, **values)


async def cache_policy(*args, **kwargs):
    kwargs.setdefault('trusted_reader_image', 'registry.example/service@sha256:'+'a'*64)
    return await _cache_policy(*args, **kwargs)


async def test_normal_admin_policy_is_independent_persistent_inspectable_and_replayable(sessions):
    from loom_service.pool_management.administration import (
        PoolAdministrationAuthorizationV2,
        administer,
    )
    from loom_service.pool_management.capacity import digest

    spec, _, _, _ = await setup(sessions)
    operation = request(spec, cache_total_mib=65536, cache_gc_policy='budget-pressure', reuse_task_cache_for_verifier=True)
    async with sessions() as session:
        issued_at = await session.scalar(select(func.clock_timestamp()))
    grant = PoolAdministrationAuthorizationV2(schema_version='loom.pool-administration-authorization.v2',
        authorization_id=uuid4(), installation_id=spec.installation_id, pool_id=spec.pool_id, authority='platform',
        request_sha256=digest(operation.model_dump(mode='json')), issued_at=issued_at,
        expires_at=issued_at+timedelta(minutes=5))
    async with sessions.begin() as session:
        first = await administer(session, operation.model_dump_json().encode(), grant, action='apply',
            trusted_reader_image=spec.profiles.task_images[0].settings.service_image)
    async with sessions.begin() as session:
        replayed = await administer(session, operation.model_dump_json().encode(), grant, action='apply',
            trusted_reader_image=spec.profiles.task_images[0].settings.service_image)
        assert replayed['receipt'] == first['receipt']
    async with sessions() as session:
        inspected = await cache_policy(session, request(spec), apply=False)
        assert inspected['current']['cache_total_mib'] == 65536
        assert inspected['current']['reuse_task_cache_for_verifier'] is True
        assert inspected['current']['policy_source'] == 'management-db'
        assert inspected['current']['registered_scope_profiles'] == 3
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        assert pool.admission_epoch == spec.admission_epoch and pool.policy_revision == spec.policy_revision+1
        profile = spec.profiles.profiles().task_images[operation.profile_id]
        effective = await effective_task_profile(session, installation_id=spec.installation_id, profile=profile)
        assert profile.settings.cache_total_mib == 4096
        assert effective.settings.cache_total_mib == 65536
        assert effective.settings.reuse_task_cache_for_verifier is True
        assert effective.profile_id == profile.profile_id and effective.target == profile.target
        assert pool.binding_json['profile_catalog_sha256'] == digest(spec.profiles.model_dump(mode='json'))
    import json
    output = json.dumps(first)
    assert 'storage.example' not in output and 'cache_bucket' not in output


async def test_new_native_activation_freezes_policy_and_old_plan_replays_without_current_override(sessions):
    import json

    import httpx

    from loom.db.nebius_pool_schema import NebiusPoolRequest
    from tests.integration.test_nebius_pool_control import action
    from tests.integration.test_nebius_pool_observation_continuity import workload
    from tests.unit.test_nebius_pool_task_image_render import build_inputs

    spec, app, headers, _ = await setup(sessions)
    participant = spec.participants[0]
    _, body, _ = build_inputs()
    body.update(pool_id=str(spec.pool_id), admission_epoch=participant.admission_epoch,
        participant_revision=participant.binding_revision, origin=workload(participant).origin.model_dump(mode='json'))
    body['key']['participant_id'] = str(participant.participant_id)
    from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
    build = PoolTaskImagePrepareV1.model_validate(body)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example') as client:
        endpoint = f'/internal/pools/v1/{spec.pool_id}'
        admitted = await client.post(endpoint+'/prepare', json=build.model_dump(mode='json'), headers=headers(participant))
        assert admitted.status_code == 200 and admitted.json()['phase'] == 'reserved'
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match='build_drain_required'):
                await cache_policy(session, request(spec, cache_gc_policy='budget-pressure'), apply=True)
        async with sessions.begin() as session:
            await cache_policy(session, request(spec, cache_total_mib=65536, reuse_task_cache_for_verifier=True), apply=True)
        activation = action(build, activation=True)
        active = await client.post(endpoint+'/activate', json=activation.model_dump(mode='json'), headers=headers(participant))
        assert active.status_code == 200
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, active.json()['reservation_id'])
            frozen = row.plan_json
            claim = json.loads(frozen['configmap']['data']['claim.json'])
            assert claim['cache_total_mib'] == 65536 and claim['reuse_task_cache_for_verifier'] is True
            assert row.request_json == build.model_dump(mode='json'), 'actor raw body/hash remains unchanged'
        async with sessions.begin() as session:
            await cache_policy(session, request(spec, cache_total_mib=131072), apply=True)
        replay = await client.post(endpoint+'/activate', json=activation.model_dump(mode='json'), headers=headers(participant))
        assert replay.json() == active.json()
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, active.json()['reservation_id'])
            assert row.plan_json == frozen
            inspected = await cache_policy(session, request(spec), apply=False)
            assert inspected['current']['cache_total_mib'] == 131072
            assert inspected['current']['frozen_claims'][0]['cache_total_mib'] == 65536


async def test_cache_owner_scope_cannot_be_reused_by_another_installation(sessions):
    spec, _, _, _ = await setup(sessions)
    async with sessions.begin() as session:
        await cache_policy(session, request(spec, cache_total_mib=65536), apply=True)
    profile = spec.profiles.profiles().task_images[spec.profiles.task_images[0].profile_id]
    async with sessions() as session:
        with pytest.raises(ValueError, match='owner_conflict'):
            await effective_task_profile(session, installation_id=uuid4(), profile=profile)


async def test_read_only_inspection_does_not_create_policy_or_journal(sessions):
    spec, _, _, _ = await setup(sessions)
    async with sessions() as session:
        result = await cache_policy(session, request(spec), apply=False)
        assert result['current']['cache_total_mib'] == 4096
        assert result['current']['policy_source'] == 'profile-default'
        assert await session.scalar(select(NebiusCacheRuntimePolicy)) is None
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match='read_only'):
            await cache_policy(session, request(spec), apply=True)


@pytest.mark.parametrize('trusted', [None, 'registry.example/service@sha256:'+'f'*64])
async def test_future_old_or_unknown_reader_is_rejected_before_policy_write(sessions, trusted):
    spec, _, _, _ = await setup(sessions)
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match='future_reader_unqualified'):
            await _cache_policy(session, request(spec, cache_total_mib=65536, cache_gc_policy='budget-pressure'),
                apply=True, trusted_reader_image=trusted)
        assert await session.scalar(select(NebiusCacheRuntimePolicy)) is None


@pytest.mark.parametrize('same_owner', [False, True])
async def test_physical_cache_scope_is_not_partitioned_by_pool_id(sessions, same_owner):
    spec, _, _, _ = await setup(sessions)
    other = await additional_pool(sessions, spec.installation_id if same_owner else uuid4())
    if not same_owner:
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match='owner_conflict'):
                await cache_policy(session, request(spec, cache_total_mib=65536), apply=True)
        return
    async with sessions.begin() as session:
        result = await cache_policy(session, request(spec, cache_total_mib=65536), apply=True)
        assert result['current']['registered_scope_pools'] == 2
    async with sessions.begin() as session:
        await cache_policy(session, request(other, cache_total_mib=131072), apply=True)
    async with sessions() as session:
        result = await cache_policy(session, request(spec), apply=False)
        assert result['current']['cache_total_mib'] == 131072
        assert len((await session.scalars(select(NebiusCacheRuntimePolicy))).all()) == 1


async def test_pressure_counts_queued_app_without_pool_request_and_retry_freezes_new_policy(environment_registry, build_inputs):
    from loom_service.application_management.build_journal import ApplicationBuildJournal
    from tests.integration.test_nebius_application_build_registry import build_registry, verified
    from tests.integration.test_nebius_application_source_upload import upload_registry

    _, factory, (owner, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, owner)
    registry = build_registry(factory, sources, build_inputs[0], cache_bucket='shared-cache')
    binding = registry.binding
    spec = await additional_pool(factory, binding.source.installation_id,
        endpoint=binding.storage_endpoint, cache_bucket=binding.cache_bucket)
    first = await registry.create(principal=owner, upload_id=source.upload_id, idempotency_key='queued-reader')
    async with factory() as session:
        inspected = await cache_policy(session, request(spec), apply=False)
        assert inspected['current']['unsettled_application_attempts'] == 1
        assert inspected['current']['active_build_requests'] == 0
    async with factory.begin() as session:
        with pytest.raises(ValueError, match='build_drain_required'):
            await cache_policy(session, request(spec, cache_total_mib=65536, cache_gc_policy='budget-pressure'), apply=True)
    # Local fixture uses the ordinary cancellation state machine, not SQL repair.
    # The production control never cancels readers; its caller waits for drain.
    await registry.cancel(first.build_id, principal=owner, attempt=1)
    journal = ApplicationBuildJournal(registry)
    lease = await journal.claim(first.build_id, attempt=1)
    await journal.cancelled_unstarted(lease, None)
    async with factory.begin() as session:
        await cache_policy(session, request(spec, cache_total_mib=65536, cache_gc_policy='budget-pressure'), apply=True)
    await registry.retry(first.build_id, principal=owner, attempt=1)
    frozen = await journal.dispatch.freeze(first.build_id, attempt=2)
    assert frozen.build.cache_total_mib == 65536 and frozen.build.cache_gc_policy == 'budget-pressure'
