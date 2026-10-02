"""Owner source intents persist through real PostgreSQL transactions/restarts."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def upload_registry(factory, **changes):
    from loom.application_source_upload import ApplicationSourceUploadBindingV1
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    binding = ApplicationSourceUploadBindingV1(
        installation_id=uuid4(), data_environment_id=uuid4(), cluster_id="cluster-1",
        source_bucket="shared-source", **changes,
    )
    return ApplicationSourceRegistry(factory, binding=binding)


def intent(**changes):
    from loom.application_source_upload import ApplicationSourceUploadRequestV1

    return ApplicationSourceUploadRequestV1.model_validate({
        "source_digest": "sha256:" + "a" * 64, "archive_sha256": "b" * 64,
        "archive_size_bytes": 10240, "base_commit": "c" * 40,
    } | changes)


async def test_concurrent_owner_upload_replay_survives_registry_restart(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    replies = await asyncio.gather(*[
        registry.create(principal=alice, request=intent(), idempotency_key="source-1") for _ in range(3)
    ])
    first = replies[0]
    assert all(reply == first for reply in replies)
    assert first.phase == "awaiting_source"
    assert first.source_digest == "sha256:" + "a" * 64
    assert first.archive_sha256 == "b" * 64
    assert first.base_commit == "c" * 40
    restored = ApplicationSourceRegistry(factory, binding=registry.binding)
    assert await restored.status(first.upload_id, principal=alice) == first
    assert await restored.create(principal=alice, request=intent(), idempotency_key="source-1") == first
    other = await registry.create(principal=bob, request=intent(), idempotency_key="source-1")
    assert other.upload_id != first.upload_id
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationSourceUpload)) == 2
        rows = list(await session.scalars(select(NebiusApplicationSourceUpload)))
        assert {row.object_key for row in rows} == {"application-sources/v1/sha256/" + "b" * 64 + ".tar"}


@pytest.mark.parametrize("change", [
    {"source_digest": "sha256:" + "d" * 64}, {"archive_sha256": "e" * 64},
    {"archive_size_bytes": 20480}, {"base_commit": None},
])
async def test_upload_idempotency_refuses_changed_source_intent(environment_registry, change):
    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="same-key")
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, request=intent(**change), idempotency_key="same-key")
    assert await registry.status(first.upload_id, principal=alice) == first


async def test_upload_owner_team_installation_and_mutation_scope_are_enforced(environment_registry):
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    for stranger in (bob, replace(alice, team_id=uuid4())):
        with pytest.raises(ManagementError, match="application_source_forbidden"):
            await registry.status(first.upload_id, principal=stranger)
        with pytest.raises(ManagementError, match="application_source_forbidden"):
            await registry.complete(first.upload_id, principal=stranger)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=replace(alice, team_id=uuid4()), request=intent(), idempotency_key="source")
    read_only = replace(alice, scopes=["read:own"])
    assert await registry.status(first.upload_id, principal=read_only) == first
    with pytest.raises(ManagementError, match="environment_scope_required"):
        await registry.complete(first.upload_id, principal=read_only)
    alternate = ApplicationSourceRegistry(factory, binding=registry.binding.model_copy(update={"installation_id": uuid4()}))
    with pytest.raises(ManagementError, match="application_source_forbidden"):
        await alternate.status(first.upload_id, principal=alice)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await alternate.create(principal=alice, request=intent(), idempotency_key="source")


async def test_verified_source_receipt_is_idempotent_and_not_a_ready_build(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    replies = await asyncio.gather(*[registry.complete(first.upload_id, principal=alice) for _ in range(3)])
    assert all(reply == replies[0] for reply in replies)
    assert replies[0].phase == "source_verified"
    assert set(replies[0].model_dump()) == {
        "schema_version", "upload_id", "source_digest", "archive_sha256", "archive_size_bytes",
        "base_commit", "phase", "expires_at",
    }
    async with factory() as session:
        row = await session.get(NebiusApplicationSourceUpload, first.upload_id)
        assert row.verified_at >= row.created_at
        assert row.expires_at - row.created_at == timedelta(seconds=3600)


async def test_upload_database_retains_immutable_identity_and_verified_receipt(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    for change in ({"archive_sha256": "d" * 64}, {"object_key": "other"},
                   {"owner_user_id": uuid4()}, {"expires_at": first.expires_at + timedelta(seconds=1)}):
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(update(NebiusApplicationSourceUpload).where(
                    NebiusApplicationSourceUpload.upload_id == first.upload_id).values(**change))
    verified = await registry.complete(first.upload_id, principal=alice)
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(update(NebiusApplicationSourceUpload).where(
                NebiusApplicationSourceUpload.upload_id == first.upload_id
            ).values(phase="awaiting_source", verified_at=None))
    assert await registry.status(first.upload_id, principal=alice) == verified
    async with factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0173"
