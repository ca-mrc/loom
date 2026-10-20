"""Closed schema catalog CAS includes configured builds and preserves neighbors."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import psycopg
import pytest
from scripts.ops.nebius_development_schema_catalog import (
    closed_development_catalog_state_sql,
    qualify_closed_development_catalog_report,
    refresh_closed_development_catalog_sql,
)
from scripts.ops.nebius_development_schema_projection import (
    ClosedDevelopmentCatalogRefresh,
    _pool_successor,
)
from sqlalchemy import func, select, update

from loom.auth import AuthContext
from loom.db.nebius_application_build_schema import NebiusApplicationBuildAttempt
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom.db.schema import Team, User
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.integration.test_nebius_pool_participant_refresh import reserved, snapshot
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@pytest.fixture
def catalog_request(build_inputs):
    config, _ = installation(('development', 'staging'))
    config, _, _ = add_application_builder(config, build_inputs[0].recipe.model_copy(update={'schema_revision': '0175'}))
    config['profiles']['application_images'][0]['settings'].update(
        registry_repository=build_inputs[0].registry_repository, storage_endpoint=build_inputs[0].storage_endpoint)
    before = PoolInstallation.model_validate(config)
    execution, task = before.profiles.execution[0], before.profiles.task_images[0]
    source = ServiceExecutionRuntimeProfileV1(candidate_sha=execution.candidate_sha,
        execution_class_id=execution.execution_class_id, task_image_ref=task.settings.service_image,
        runtime_image_ref=execution.runtime_image_ref, runtime_binary_sha256=execution.runtime_binary_sha256,
        image_admission=signed_image_admission_bundle((task.settings.service_image, execution.runtime_image_ref)))
    target = source.model_copy(update={'candidate_sha': '2' * 40})
    arguments = dict(before=before, participant_id=before.participants[0].participant_id,
        source_schema_revision='0175', target_schema_revision='0178', source_profile=source, runtime_profile=target)
    after = _pool_successor(**arguments, operation_id=uuid4())
    return ClosedDevelopmentCatalogRefresh(**arguments, after=after, manager_schema_revision='0178')


@pytest.fixture
async def installed_catalog(sessions, catalog_request):
    async with sessions.begin() as session:
        await register_installation(session, catalog_request.before)
    return catalog_request, sessions.kw['bind'].url.render_as_string(hide_password=False)


async def test_closed_builder_catalog_commits_once_and_preserves_neighbor_and_credentials(sessions, installed_catalog):
    request, url = installed_catalog
    before = await snapshot(sessions)
    report = await asyncio.to_thread(read_sql, url, closed_development_catalog_state_sql(request))
    assert qualify_closed_development_catalog_report(request, report, read_only=True) == 'ready'
    applied = await asyncio.to_thread(read_sql, url, refresh_closed_development_catalog_sql(request))
    assert qualify_closed_development_catalog_report(request, applied, read_only=False) == 'complete'
    after = await snapshot(sessions)
    for table in ('nebius_pool_machines', 'nebius_pool_machine_credentials', 'tokens', 'nebius_pool_requests'):
        assert before[table] == after[table]
    pool, = after['nebius_pool_bindings']
    assert pool['mode'] == 'closed' and pool['admission_epoch'] == request.before.admission_epoch
    assert pool['binding_json']['profile_catalog_sha256'] == digest(request.after.profiles.model_dump(mode='json'))
    neighbor_id = request.before.participants[1].participant_id
    assert [row for row in before['nebius_pool_participants'] if row['participant_id'] == neighbor_id] == [
        row for row in after['nebius_pool_participants'] if row['participant_id'] == neighbor_id]
    # A committed-but-unreported write is reconciled by a fresh exact read.
    observed = await asyncio.to_thread(read_sql, url, closed_development_catalog_state_sql(request))
    assert qualify_closed_development_catalog_report(request, observed, read_only=True) == 'complete'
    with pytest.raises(psycopg.Error):
        await asyncio.to_thread(read_sql, url, refresh_closed_development_catalog_sql(request))
    assert await snapshot(sessions) == after


@pytest.mark.parametrize('damage', ['global', 'pool-work', 'queued-build', 'active-application', 'wrong-manager-schema'])
async def test_closed_schema_catalog_rejects_work_and_schema_drift_without_mutation(sessions, installed_catalog, damage):
    request, url = installed_catalog
    queued = None
    if damage == 'global':
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.before.pool_id).values(mode='global'))
    elif damage == 'pool-work':
        await reserved(sessions, request, request.before.participants[0], phase='waiting')
    elif damage == 'wrong-manager-schema':
        request = request.model_copy(update={'manager_schema_revision': '0176'})
    elif damage == 'active-application':
        from tests.integration.test_nebius_application_registry import application
        async with sessions.begin() as session:
            await session.run_sync(lambda sync: application(sync.connection(),
                data_environment_id=request.before.participants[0].environment_id))
    else:
        from loom.application_image_build import ApplicationImageBuildBindingV1
        from loom.application_source_upload import ApplicationSourceUploadBindingV1
        from loom_service.application_management.build_registry import ApplicationBuildRegistry
        from loom_service.application_management.source_registry import ApplicationSourceRegistry
        from tests.integration.test_nebius_application_source_upload import intent

        user_id, team_id = uuid4(), uuid4()
        async with sessions.begin() as session:
            session.add(User(id=user_id, username='schema-owner', username_normalized='schema-owner', status='active'))
            session.add(Team(id=team_id, name='schema-owner'))
        owner = AuthContext(token_hash=b'', type='user', scopes=['read:own', 'submit'], team_id=team_id,
            user_id=user_id, expires_at=None, auth_kind='session')
        participant = request.before.participants[0]
        profile, = request.before.profiles.application_images
        source = ApplicationSourceUploadBindingV1(installation_id=request.before.installation_id,
            data_environment_id=participant.environment_id, cluster_id=request.before.cluster_id,
            source_bucket=profile.settings.source_bucket)
        sources = ApplicationSourceRegistry(sessions, binding=source)
        upload = await sources.create(principal=owner, request=intent(), idempotency_key='schema-source')
        await sources.complete(upload.upload_id, principal=owner)
        builder = ApplicationBuildRegistry(sessions, binding=ApplicationImageBuildBindingV1(source=source,
            recipe=profile.recipe, pool_id=request.before.pool_id, participant_id=participant.participant_id,
            profile_id=profile.profile_id, target_id=profile.target.target_id,
            admission_epoch=participant.admission_epoch, participant_revision=participant.binding_revision,
            storage_endpoint=profile.settings.storage_endpoint, storage_region=profile.settings.storage_region,
            cache_bucket=profile.settings.cache_bucket, registry_repository=profile.settings.registry_repository))
        queued = await builder.create(principal=owner, upload_id=upload.upload_id, idempotency_key='schema-build')
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0
    before = await snapshot(sessions)
    with pytest.raises(psycopg.Error):
        await asyncio.to_thread(read_sql, url, refresh_closed_development_catalog_sql(request))
    assert await snapshot(sessions) == before
    if queued is not None:
        async with sessions() as session:
            row = await session.get(NebiusApplicationBuildAttempt, (queued.build_id, queued.attempt))
            assert row.phase == 'queued' and row.pool_request_json is None


@pytest.mark.parametrize('damage', ['neighbor', 'recipe', 'image', 'credential', 'quota', 'namespace'])
def test_closed_schema_catalog_rejects_valid_but_unrelated_contract_changes(catalog_request, damage):
    value = catalog_request.model_dump(mode='json')
    after = value['after']
    if damage == 'neighbor':
        after['profiles']['execution'][1]['runtime_binary_sha256'] = 'sha256:' + 'd' * 64
    elif damage == 'recipe':
        after['profiles']['application_images'][0]['recipe']['schema_revision'] = '0179'
    elif damage == 'image':
        after['profiles']['task_images'][0]['settings']['service_image'] = 'registry.example/service@sha256:' + 'f' * 64
    elif damage == 'credential':
        after['machines'][0]['credential_epoch'] += 1
    elif damage == 'quota':
        after['admission']['build_concurrency_limit'] += 1
    else:
        after['participants'][0]['build_namespace']['uid'] = str(uuid4())
    PoolInstallation.model_validate(after)
    with pytest.raises(ValueError):
        ClosedDevelopmentCatalogRefresh.model_validate(value)
