"""A dedicated builder identity cannot borrow ordinary participant authority."""
from __future__ import annotations

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_schema import NebiusPoolMachine, NebiusPoolRequest
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_application_build_admission import (
    prepare_application,
    setup_application_pool,
)
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_registry import machine, prepare
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize("operation", ["prepare", "status", "activate", "cancel"])
async def test_environment_machine_cannot_prepare_or_control_personal_build(environment_registry, build_inputs, operation):
    from loom_service.pool_management.control import PoolControlError
    from loom_service.pool_management.registry import PoolAdmissionError

    factory, principals, apps, profiles, _, _, _, _ = await setup_application_pool(environment_registry, build_inputs)
    builder, request = principals[0], apps[0]
    ordinary = await machine(factory, builder.pool_id, builder.participant_id)
    if operation == "prepare":
        with pytest.raises(PoolAdmissionError):
            await prepare_application(factory, ordinary, request, profiles)
        async with factory() as session:
            assert list(await session.scalars(select(NebiusPoolRequest))) == []
    else:
        granted = await prepare_application(factory, builder, request, profiles)
        with pytest.raises(PoolControlError):
            await operate(factory, ordinary, action(request, activation=operation == "activate"),
                profiles=profiles, operation=operation)
        assert await operate(factory, builder, action(request), operation="status") == granted


@pytest.mark.parametrize("operation", ["prepare", "status", "activate", "cancel"])
async def test_builder_cannot_prepare_or_control_ordinary_execution(environment_registry, build_inputs, operation):
    from loom_service.pool_management.control import PoolControlError
    from loom_service.pool_management.registry import PoolAdmissionError

    factory, principals, _, profiles, _, executions, _, _ = await setup_application_pool(environment_registry, build_inputs)
    builder, request = principals[0], executions[0]
    ordinary = await machine(factory, builder.pool_id, builder.participant_id)
    if operation == "prepare":
        with pytest.raises(PoolAdmissionError):
            await prepare(factory, builder, request, profiles)
        async with factory() as session:
            assert list(await session.scalars(select(NebiusPoolRequest))) == []
    else:
        granted = await prepare(factory, ordinary, request, profiles)
        with pytest.raises(PoolControlError):
            await operate(factory, builder, action(request, activation=operation == "activate"),
                profiles=profiles, operation=operation)
        assert await operate(factory, ordinary, action(request), operation="status") == granted


async def test_builder_scope_cannot_change_when_rotating_a_credential(environment_registry, build_inputs):
    factory, principals, _, _, _, _, _, _ = await setup_application_pool(environment_registry, build_inputs)
    builder = principals[0]
    with pytest.raises(DBAPIError, match="machine identity is immutable"):
        async with factory.begin() as session:
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == builder.machine_id)
                .values(workload_scope="environment", credential_epoch=builder.credential_epoch + 1))
    async with factory() as session:
        row = await session.get(NebiusPoolMachine, builder.machine_id)
        assert row.workload_scope == "application_builder" and row.credential_epoch == builder.credential_epoch


async def test_builder_cannot_gain_observer_identity_or_ordinary_node_allocation(environment_registry, build_inputs):
    from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1
    from loom_service.pool_management.allocation import node_allocation
    from loom_service.pool_management.auth import PoolAuthenticationError

    factory, principals, _, _, _, executions, _, _ = await setup_application_pool(environment_registry, build_inputs)
    builder, request = principals[0], executions[0]
    with pytest.raises(DBAPIError):
        await machine(factory, builder.pool_id, workload_scope="application_builder")
    allocation = PoolNodeAllocationRequestV1(pool_id=builder.pool_id, participant_id=builder.participant_id,
        admission_epoch=request.admission_epoch, participant_revision=request.participant_revision,
        target_id=request.target_id)
    with pytest.raises(PoolAuthenticationError):
        async with factory.begin() as session:
            await node_allocation(session, builder, allocation)
