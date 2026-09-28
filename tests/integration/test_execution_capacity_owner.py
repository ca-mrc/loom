"""One physical authority for independently fenced ordinary and guest targets."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionTarget
from loom.execution_contract import ExecutionTargetV1, nebius_guest_execution_class
from loom_control_plane.service_execution import ServiceExecutionConflict, persist_execution_catalog
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared fixture
    _seed_ready_trial,
)


def _alias(owner, **changes):
    return ExecutionTargetV1.model_validate({
        **owner.model_dump(mode="json"), "target_id": owner.target_id + "-guest",
        "health_check_id": owner.health_check_id + "-guest",
        "execution_class_id": nebius_guest_execution_class().class_id,
        "capacity_owner_target_id": owner.target_id, **changes,
    })


async def test_catalog_alias_keeps_health_and_intent_independent(postgres_url):
    from loom_control_plane.execution_capacity_targets import resolve_capacity_targets

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _, owner = await _seed_ready_trial(session, now=datetime.now(UTC))
            alias = _alias(owner)
            await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(alias,))
            row = await session.get(ServiceExecutionTarget, alias.target_id)
            assert (row.desired_state, row.observed_state, row.health_status) == ("disabled", "unknown", "unknown")
            group = await resolve_capacity_targets(session, alias.target_id)
            assert group.owner.id == owner.target_id
            assert group.target_ids == frozenset({owner.target_id, alias.target_id})
            assert group.scope.model_dump(mode="json") == {
                "schema_version": "loom.execution-capacity-target-scope.v1",
                "owner_target_id": owner.target_id, "namespace_name": owner.namespace_name,
                "target_ids": sorted([owner.target_id, alias.target_id]),
            }
            same = await resolve_capacity_targets(session, owner.target_id)
            assert same.scope == group.scope
            # Registration is idempotent and cannot silently move ownership.
            await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(alias,))
            with pytest.raises(ServiceExecutionConflict):
                await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(),
                    targets=(_alias(owner, capacity_owner_target_id="missing-target"),))
    finally:
        await engine.dispose()


@pytest.mark.parametrize("change", [
    {"capacity_owner_target_id": "missing-target"},
    {"namespace_name": "foreign-namespace"}, {"environment": "production"},
    {"cluster_scope_id": "foreign-cluster"}, {"logical_pool_id": "foreign-pool"},
    {"region": "eu-west1"}, {"failure_domain": "foreign-domain"},
    {"execution_class_id": "linux-amd64-cpu-guest-web-v1"},
])
async def test_catalog_alias_rejects_unqualified_owner_scope(postgres_url, change):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _, owner = await _seed_ready_trial(session, now=datetime.now(UTC))
            alias = _alias(owner, **change)
            with pytest.raises(ServiceExecutionConflict):
                await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(
                    supports_task_web_egress=alias.execution_class_id.endswith("guest-web-v1")), targets=(alias,))
    finally:
        await engine.dispose()


async def test_catalog_rejects_capacity_owner_chains(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _, owner = await _seed_ready_trial(session, now=datetime.now(UTC))
            first = _alias(owner)
            await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(first,))
            nested = _alias(owner, target_id="guest-" + uuid4().hex,
                            capacity_owner_target_id=first.target_id)
            with pytest.raises(ServiceExecutionConflict):
                await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(nested,))
    finally:
        await engine.dispose()


async def test_catalog_rejects_uncollectable_family_size_before_registration(postgres_url):
    from loom_control_plane.execution_capacity_targets import resolve_capacity_targets

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _, owner = await _seed_ready_trial(session, now=datetime.now(UTC))
            members = tuple(_alias(owner, target_id=f"{owner.target_id}-guest-{index}",
                health_check_id=f"{owner.health_check_id}-guest-{index}") for index in range(63))
            await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=members)
            assert len((await resolve_capacity_targets(session, owner.target_id)).scope.target_ids) == 64
            excess = _alias(owner, target_id=owner.target_id + "-excess")
            with pytest.raises(ServiceExecutionConflict, match="64"):
                await persist_execution_catalog(session, execution_class=nebius_guest_execution_class(), targets=(excess,))
            assert await session.get(ServiceExecutionTarget, excess.target_id) is None
    finally:
        await engine.dispose()
