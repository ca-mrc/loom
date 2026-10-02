"""Real retained application requests survive restart before any pool HTTP."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_build_registry import build_registry, verified
from tests.integration.test_nebius_application_source_upload import upload_registry
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def queued(environment_registry, build_inputs):
    _, factory, (owner, _), _ = environment_registry
    sources = upload_registry(factory)
    source = await verified(sources, owner)
    registry = build_registry(factory, sources, build_inputs[0])
    build = await registry.create(principal=owner, upload_id=source.upload_id, idempotency_key="build")
    return registry, build


async def test_concurrent_freeze_commits_one_exact_request_before_restart(environment_registry, build_inputs):
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch

    registry, build = await queued(environment_registry, build_inputs)
    dispatch = ApplicationBuildDispatch(registry, request_lifetime_seconds=1800)
    requests = await asyncio.gather(*[dispatch.freeze(build.build_id, attempt=1) for _ in range(3)])
    request = requests[0]
    assert requests == [request] * 3
    assert request.key.local_work_id == build.build_id and request.key.generation == 1
    assert request.key.workload_kind == "application_image_build"
    assert request.key.participant_id == registry.binding.participant_id
    assert request.pool_id == registry.binding.pool_id and request.target_id == registry.binding.target_id
    assert request.origin.kind == "personal_build" and request.origin.application is None
    assert request.origin.submission_id == build.build_id
    assert request.build.source.source_digest == build.source_digest
    restarted = ApplicationBuildDispatch(registry, request_lifetime_seconds=3600)
    assert await restarted.freeze(build.build_id, attempt=1) == request
    # A separate committed transaction observes the exact bytes before HTTP can start.
    async with registry.session_factory() as session:
        row = await session.get(NebiusApplicationBuildAttempt, (build.build_id, 1))
        assert row.pool_request_json == request.model_dump(mode="json")
        now = await session.scalar(select(func.clock_timestamp()))
        assert 1700 < (request.deadline_at - now).total_seconds() <= 1800


async def test_cancelled_unsubmitted_build_cannot_freeze_new_demand(environment_registry, build_inputs):
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch

    registry, build = await queued(environment_registry, build_inputs)
    async with registry.session_factory.begin() as session:
        await session.execute(update(NebiusApplicationBuild).where(NebiusApplicationBuild.build_id == build.build_id)
            .values(desired_state="cancelled"))
    with pytest.raises(ManagementError, match="application_build_not_dispatchable"):
        await ApplicationBuildDispatch(registry).freeze(build.build_id, attempt=1)
    async with registry.session_factory() as session:
        assert (await session.get(NebiusApplicationBuildAttempt, (build.build_id, 1))).pool_request_json is None


async def test_cancelled_submitted_build_retains_exact_request_for_reconciliation(environment_registry, build_inputs):
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch

    registry, build = await queued(environment_registry, build_inputs)
    dispatch = ApplicationBuildDispatch(registry)
    request = await dispatch.freeze(build.build_id, attempt=1)
    async with registry.session_factory.begin() as session:
        await session.execute(update(NebiusApplicationBuild).where(NebiusApplicationBuild.build_id == build.build_id)
            .values(desired_state="cancelled"))
    assert await dispatch.freeze(build.build_id, attempt=1) == request


@pytest.mark.parametrize("damage", ["unknown", "attempt", "installation", "data", "cluster"])
async def test_dispatch_does_not_cross_attempt_or_installation_boundary(environment_registry, build_inputs, damage):
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch
    from loom_service.application_management.build_registry import ApplicationBuildRegistry

    registry, build = await queued(environment_registry, build_inputs)
    identity, attempt = build.build_id, 1
    if damage == "unknown":
        identity = uuid4()
    elif damage == "attempt":
        attempt = 2
    else:
        field = {"installation": "installation_id", "data": "data_environment_id", "cluster": "cluster_id"}[damage]
        source = registry.binding.source.model_copy(update={field: "foreign" if damage == "cluster" else uuid4()})
        registry = ApplicationBuildRegistry(registry.session_factory, binding=registry.binding.model_copy(update={"source": source}))
    with pytest.raises(ManagementError):
        await ApplicationBuildDispatch(registry).freeze(identity, attempt=attempt)


@pytest.mark.parametrize("mutation", ["deadline", "clear", "hash"])
async def test_sql_cannot_replace_or_erase_frozen_dispatch(environment_registry, build_inputs, mutation):
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch

    registry, build = await queued(environment_registry, build_inputs)
    dispatch = ApplicationBuildDispatch(registry)
    request = await dispatch.freeze(build.build_id, attempt=1)
    fields = {
        "deadline": "pool_request_json=jsonb_set(pool_request_json, '{deadline_at}', '\"2099-01-01T00:00:00Z\"')",
        "clear": "pool_request_json=NULL, pool_request_sha256=NULL",
        "hash": "pool_request_sha256=repeat('f',64)",
    }[mutation]
    with pytest.raises(IntegrityError):
        async with registry.session_factory.begin() as session:
            await session.execute(text("UPDATE nebius_application_build_attempts SET " + fields + " WHERE build_id=:id"),
                {"id": build.build_id})
    assert await dispatch.freeze(build.build_id, attempt=1) == request
