"""The operator's installed readonly drain probe uses real frozen SQL history."""
import asyncio
import json
import sys
from datetime import UTC, datetime

import pytest
from scripts.ops import nebius_execution_tuning as tuning

from loom.db.schema import TaskImageMaterializationAttempt
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.integration.test_nebius_task_image_capacity import _native
from tests.integration.test_nebius_task_image_capacity import native_setup as native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows as _cleanup_service_execution_test_rows,
)
from tests.integration.test_service_execution_leases import _seed_ready_trial
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@pytest.mark.parametrize('condition,expected', [
    ('old-image', 1), ('old-budget', 1), ('compatible', 0), ('released', 0), ('different-cache', 0),
])
@pytest.mark.asyncio
async def test_real_frozen_reader_probe_distinguishes_scope_version_and_release(
    native_setup, postgres_url, monkeypatch, capsys, condition, expected,
):
    sessions, owned = native_setup
    current_image = 'registry.example/service@sha256:' + 'a' * 64
    now = datetime.now(UTC)
    scope = {'endpoint':'https://cache.example','region':'eu-north1','bucket':'build-cache',
             'image':current_image,'budget':65536}
    claim = {'storage_endpoint':scope['endpoint'],'storage_region':scope['region'],
             'cache_bucket':'different-cache' if condition=='different-cache' else scope['bucket'],
             'cache_total_mib':4096 if condition=='old-budget' else 65536}
    image = 'registry.example/old-service@sha256:' + 'b' * 64 if condition=='old-image' else current_image
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt_id, _ = await _native(session, owned, pair, now)
        row = await session.get(TaskImageMaterializationAttempt, attempt_id)
        row.native_build = {**row.native_build,
            'configmap':{'data':{'claim.json':json.dumps(claim)}},
            'job':{'spec':{'template':{'spec':{'initContainers':[{'name':'prepare','image':image}]}}}}}
        if condition=='released':
            row.native_build = {**row.native_build,'capacity_reserved_at':now.isoformat(),
                                 'capacity_released_at':now.isoformat()}
    monkeypatch.setenv('LOOM_CP_DB_URL', postgres_url)
    monkeypatch.delenv('LOOM_CP_DB_URL_POOL', raising=False)
    monkeypatch.setenv('LOOM_CP_MINIO_ACCESS_KEY', 'test-access')
    monkeypatch.setenv('LOOM_CP_MINIO_SECRET_KEY', 'test-secret')
    monkeypatch.setattr(sys, 'argv', ['cache-reader-probe',json.dumps([scope])])
    await asyncio.to_thread(exec, compile(tuning._CACHE_READER_STATE,'installed-cache-reader-probe','exec'), {'__name__':'reader_probe'})
    result = json.loads(capsys.readouterr().out)
    assert result == {'native_incompatible':expected,'application_incompatible':0,'management_owned':0}


@pytest.mark.parametrize('condition,expected', [('old-image',1),('old-budget',1),('compatible',0),('different-cache',0)])
@pytest.mark.asyncio
async def test_real_app_queued_reader_is_included_without_waiting_for_a_pod(
    environment_registry, isolated_migration_postgres_url, build_inputs, monkeypatch, capsys, condition, expected,
):
    from tests.integration.test_nebius_application_build_registry import build_registry, verified
    from tests.integration.test_nebius_application_source_upload import upload_registry

    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    image = 'registry.example/service@sha256:' + 'a' * 64
    claim = build_inputs[0]
    recipe = claim.recipe.model_copy(update={'trusted_image_ref':
        'registry.example/old-service@sha256:' + 'e' * 64 if condition=='old-image' else image})
    claim = claim.model_copy(update={'recipe':recipe})
    options = {} if condition=='old-budget' else {'cache_total_mib':65536}
    registry = build_registry(factory, sources, claim,
        cache_bucket='different-cache' if condition=='different-cache' else 'build-cache', **options)
    result = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key='old-cache-reader')
    assert result.attempt == 1
    scope = {'endpoint':claim.storage_endpoint,'region':claim.storage_region,'bucket':'build-cache',
             'image':image,'budget':65536}
    monkeypatch.setenv('LOOM_CP_DB_URL',isolated_migration_postgres_url)
    monkeypatch.delenv('LOOM_CP_DB_URL_POOL',raising=False)
    monkeypatch.setenv('LOOM_CP_MINIO_ACCESS_KEY','test-access')
    monkeypatch.setenv('LOOM_CP_MINIO_SECRET_KEY','test-secret')
    monkeypatch.setattr(sys,'argv',['cache-reader-probe',json.dumps([scope])])
    await asyncio.to_thread(exec,compile(tuning._CACHE_READER_STATE,'installed-app-cache-reader-probe','exec'),{'__name__':'reader_probe'})
    assert json.loads(capsys.readouterr().out) == {'native_incompatible':0,'application_incompatible':expected,'management_owned':0}


@pytest.mark.parametrize('owner', ['current-profile', 'persisted-policy'])
@pytest.mark.asyncio
async def test_real_future_management_scope_rejected_even_without_running_builds(
    sessions, isolated_migration_postgres_url, monkeypatch, capsys, owner,
):
    from uuid import uuid4

    from loom.db.nebius_cache_policy_schema import NebiusCacheRuntimePolicy
    from tests.integration.test_nebius_pool_profile_registry import setup

    if owner == 'current-profile':
        spec, _, _, _ = await setup(sessions)
        settings = spec.profiles.task_images[0].settings
        endpoint, region, bucket = settings.storage_endpoint, settings.storage_region, settings.cache_bucket
    else:
        endpoint, region, bucket = 'https://owned-cache.example', 'eu-north1', 'managed-cache'
        async with sessions.begin() as session:
            session.add(NebiusCacheRuntimePolicy(storage_endpoint=endpoint, storage_region=region,
                cache_bucket=bucket, installation_id=uuid4(), cache_total_mib=65536,
                cache_gc_policy='budget-pressure', reuse_task_cache_for_verifier=False))
    scope = dict(endpoint=endpoint, region=region, bucket=bucket,
                 image='registry.example/service@sha256:' + 'a' * 64, budget=65536)
    monkeypatch.setenv('LOOM_CP_DB_URL', isolated_migration_postgres_url)
    monkeypatch.delenv('LOOM_CP_DB_URL_POOL', raising=False)
    monkeypatch.setenv('LOOM_CP_MINIO_ACCESS_KEY', 'test-access')
    monkeypatch.setenv('LOOM_CP_MINIO_SECRET_KEY', 'test-secret')
    monkeypatch.setattr(sys, 'argv', ['cache-reader-probe', json.dumps([scope])])
    await asyncio.to_thread(exec, compile(tuning._CACHE_READER_STATE, 'installed-future-cache-reader-probe', 'exec'), {'__name__':'reader_probe'})
    assert json.loads(capsys.readouterr().out) == {
        'native_incompatible':0, 'application_incompatible':0, 'management_owned':1}
