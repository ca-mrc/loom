"""Owner build intent and frozen source survive concurrency and manager restart."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_source_upload import intent, upload_registry
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


def build_registry(factory, sources, claim, **changes):
    from loom.application_image_build import ApplicationImageBuildBindingV1
    from loom_service.application_management.build_registry import ApplicationBuildRegistry

    binding = ApplicationImageBuildBindingV1.model_validate({
        "source": sources.binding.model_dump(), "recipe": claim.recipe.model_dump(),
        "storage_endpoint": claim.storage_endpoint, "storage_region": claim.storage_region,
        "cache_bucket": None, "registry_repository": claim.registry_repository,
        "pool_id": uuid4(), "participant_id": uuid4(), "profile_id": uuid4(),
        "target_id": "shared", "admission_epoch": 2, "participant_revision": 1,
    } | changes)
    return ApplicationBuildRegistry(factory, binding=binding)


async def verified(sources, owner, key="source"):
    source = await sources.create(principal=owner, request=intent(), idempotency_key=key)
    # This is the trusted verifier boundary, never an owner completion endpoint.
    return await sources.complete(source.upload_id, principal=owner)


@pytest.mark.parametrize('explicit', [False, True])
async def test_cache_policy_survives_database_restart_without_rewriting_historical_claims(
    environment_registry, build_inputs, explicit,
):
    from loom.db.nebius_application_build_schema import (
        NebiusApplicationBuild,
        NebiusApplicationBuildAttempt,
    )
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
    from loom_service.application_management.build_registry import retained_build_claim

    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    options = {'cache_total_mib': 65536, 'cache_gc_policy': 'budget-pressure'} if explicit else {}
    registry = build_registry(factory, sources, build_inputs[0], **options)
    first = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key='cache-policy')
    async with factory() as session:
        row = await session.get(NebiusApplicationBuild, first.build_id)
        attempt = await session.get(NebiusApplicationBuildAttempt, (first.build_id, 1))
        before = dict(attempt.claim_json)
        source_row = await session.get(NebiusApplicationSourceUpload, source.upload_id)
        _, retained = retained_build_claim(row, attempt, source_row)
        assert retained.model_dump(mode='json') == before
        for field in ('cache_total_mib', 'cache_gc_policy'):
            assert (field in before) is explicit
            assert (field in row.binding_json) is explicit
    # New current policy never changes the retained raw claim/status proof.
    changed = build_registry(factory, sources, build_inputs[0], cache_total_mib=65536, cache_gc_policy='budget-pressure')
    assert await changed.status(first.build_id, principal=alice) == first
    async with factory() as session:
        attempt = await session.get(NebiusApplicationBuildAttempt, (first.build_id, 1))
        assert attempt.claim_json == before


async def test_independent_policy_freezes_each_new_app_attempt_without_rewriting_prior_history(environment_registry, build_inputs):
    from loom.db.nebius_application_build_schema import NebiusApplicationBuildAttempt
    from loom.db.nebius_cache_policy_schema import NebiusCacheRuntimePolicy
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch
    from loom_service.application_management.build_journal import ApplicationBuildJournal

    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    registry = build_registry(factory, sources, build_inputs[0], cache_bucket='shared-cache')
    binding = registry.binding
    scope = (binding.storage_endpoint, binding.storage_region, binding.cache_bucket)
    async with factory.begin() as session:
        session.add(NebiusCacheRuntimePolicy(storage_endpoint=scope[0], storage_region=scope[1], cache_bucket=scope[2],
            installation_id=binding.source.installation_id, cache_total_mib=65536,
            cache_gc_policy='legacy-seven-day', reuse_task_cache_for_verifier=True))
    first = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key='runtime-cache')
    frozen = await ApplicationBuildDispatch(registry).freeze(first.build_id, attempt=1)
    assert frozen.build.cache_total_mib == 65536
    assert 'reuse_task_cache_for_verifier' not in frozen.build.model_dump()
    async with factory() as session:
        attempt = await session.get(NebiusApplicationBuildAttempt, (first.build_id, 1))
        old = dict(attempt.claim_json)
    # Natural cancellation of an unstarted disposable attempt enables ordinary
    # retry; no existing claim or source/recipe is rewritten by a policy change.
    await registry.cancel(first.build_id, principal=alice, attempt=1)
    journal = ApplicationBuildJournal(registry)
    lease = await journal.claim(first.build_id, attempt=1)
    # Request was frozen but never admitted: give the normal cancellation path
    # its exact cancelled-unstarted receipt rather than editing SQL state.
    from loom.nebius_pool_contract import PoolReceiptV1
    from loom.pipeline.keys import canonical_digest
    receipt = PoolReceiptV1(reservation_id=uuid4(), pool_id=frozen.pool_id, request_key=frozen.key,
        admission_epoch=frozen.admission_epoch, request_sha256=canonical_digest(frozen.model_dump(mode='json')).removeprefix('sha256:'),
        phase='cancelled_unstarted', plan_sha256=None, job_uid=None, cleanup_observation_id=None)
    await journal.cancelled_unstarted(lease, receipt)
    async with factory.begin() as session:
        policy = await session.get(NebiusCacheRuntimePolicy, scope)
        policy.cache_total_mib = 131072
    second = await registry.retry(first.build_id, principal=alice, attempt=1)
    latest = await ApplicationBuildDispatch(registry).freeze(first.build_id, attempt=2)
    assert second.attempt == 2 and latest.build.cache_total_mib == 131072
    assert latest.build.recipe == frozen.build.recipe and latest.build.source == frozen.build.source
    assert await registry.status(first.build_id, principal=alice) == second
    async with factory() as session:
        prior = await session.get(NebiusApplicationBuildAttempt, (first.build_id, 1))
        assert prior.claim_json == old


@pytest.mark.parametrize('environment_registry', ['0176'], indirect=True)
async def test_unenabled_legacy_app_history_does_not_select_or_insert_new_policy_column(environment_registry, build_inputs):
    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    registry = build_registry(factory, sources, build_inputs[0])
    created = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key='legacy-cache-shape')
    assert await registry.status(created.build_id, principal=alice) == created


async def test_concurrent_build_intents_freeze_one_attempt_and_replay_after_restart(environment_registry, build_inputs):
    from loom.db.nebius_application_build_schema import (
        NebiusApplicationBuild,
        NebiusApplicationBuildAttempt,
    )
    from loom_service.application_management.build_registry import ApplicationBuildRegistry

    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    registry = build_registry(factory, sources, build_inputs[0])
    values = await asyncio.gather(*[registry.create(principal=alice, upload_id=source.upload_id,
        idempotency_key="build-one") for _ in range(3)])
    first = values[0]
    assert values == [first] * 3 and first.phase == "queued" and first.attempt == 1
    assert first.desired_state == "running" and first.source_digest == source.source_digest
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationBuild)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationBuildAttempt)) == 1
        row = await session.get(NebiusApplicationBuildAttempt, (first.build_id, 1))
        assert row.claim_json["build_id"] == str(first.build_id)
        assert row.claim_json["upload_id"] == str(source.upload_id)
        assert row.claim_json["owner_user_id"] == str(alice.user_id)
        assert row.claim_json["source"]["source_digest"] == source.source_digest
        assert row.claim_json["recipe"] == registry.binding.recipe.model_dump(mode="json")
    # Catalog changes are not permission to rerender an already-frozen request.
    binding = registry.binding.model_copy(update={"recipe": registry.binding.recipe.model_copy(update={"snapshotter": "native"})})
    restarted = ApplicationBuildRegistry(factory, binding=binding)
    assert await restarted.create(principal=alice, upload_id=source.upload_id, idempotency_key="build-one") == first
    assert await restarted.status(first.build_id, principal=alice) == first


async def test_unverified_source_never_creates_build_and_different_replay_conflicts(environment_registry, build_inputs):
    from loom.db.nebius_application_build_schema import NebiusApplicationBuild

    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await sources.create(principal=alice, request=intent(), idempotency_key="not-verified")
    registry = build_registry(factory, sources, build_inputs[0])
    with pytest.raises(ManagementError, match="application_source_not_verified"):
        await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key="build")
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationBuild)) == 0
    await sources.complete(source.upload_id, principal=alice)
    first = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key="build")
    another = await verified(sources, alice, "another")
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, upload_id=another.upload_id, idempotency_key="build")
    assert await registry.status(first.build_id, principal=alice) == first


@pytest.mark.parametrize("damage", ["owner", "team", "installation", "data", "cluster", "read_only"])
async def test_build_source_and_status_are_bound_to_owner_team_installation(environment_registry, build_inputs, damage):
    from loom_service.application_management.build_registry import ApplicationBuildRegistry

    _, factory, (alice, bob), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    registry = build_registry(factory, sources, build_inputs[0])
    first = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key="private")
    owner, reader = alice, registry
    if damage == "owner":
        owner = bob
    elif damage == "team":
        owner = replace(alice, team_id=uuid4())
    elif damage == "read_only":
        owner = replace(alice, scopes=["read:own"])
    else:
        field = {"installation": "installation_id", "data": "data_environment_id", "cluster": "cluster_id"}[damage]
        binding = registry.binding.model_copy(update={"source": registry.binding.source.model_copy(
            update={field: "another-cluster" if damage == "cluster" else uuid4()})})
        reader = ApplicationBuildRegistry(factory, binding=binding)
    with pytest.raises(ManagementError):
        await reader.create(principal=owner, upload_id=source.upload_id, idempotency_key="other")
    if damage != "read_only":
        with pytest.raises(ManagementError):
            await reader.status(first.build_id, principal=owner)
    assert await registry.status(first.build_id, principal=alice) == first


async def test_equal_source_different_owners_has_distinct_builds_without_duplicate_source_identity(environment_registry, build_inputs):
    _, factory, (alice, bob), _ = environment_registry
    sources = upload_registry(factory)
    one, two = await asyncio.gather(verified(sources, alice), verified(sources, bob))
    registry = build_registry(factory, sources, build_inputs[0])
    a, b = await asyncio.gather(*[registry.create(principal=owner, upload_id=upload.upload_id,
        idempotency_key="same-key") for owner, upload in ((alice, one), (bob, two))])
    assert a.build_id != b.build_id and a.upload_id != b.upload_id
    assert a.source_digest == b.source_digest and a.recipe_digest == b.recipe_digest


@pytest.mark.parametrize("mutation", ["build_delete", "attempt_delete", "claim", "upload", "owner", "binding"])
async def test_database_retains_build_identity_and_attempt_inputs(environment_registry, build_inputs, mutation):
    _, factory, (alice, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, alice)
    registry = build_registry(factory, sources, build_inputs[0])
    first = await registry.create(principal=alice, upload_id=source.upload_id, idempotency_key="retained")
    sql = {
        "build_delete": "DELETE FROM nebius_application_builds WHERE build_id=:id",
        "attempt_delete": "DELETE FROM nebius_application_build_attempts WHERE build_id=:id",
        "claim": "UPDATE nebius_application_build_attempts SET claim_json='{}'::jsonb WHERE build_id=:id",
        "upload": "UPDATE nebius_application_builds SET upload_id=:other WHERE build_id=:id",
        "owner": "UPDATE nebius_application_builds SET owner_user_id=:other WHERE build_id=:id",
        "binding": "UPDATE nebius_application_builds SET binding_json='{}'::jsonb WHERE build_id=:id",
    }[mutation]
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(text(sql), {"id": first.build_id, "other": uuid4()})
    assert await registry.status(first.build_id, principal=alice) == first
