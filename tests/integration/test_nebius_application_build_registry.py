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
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


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
