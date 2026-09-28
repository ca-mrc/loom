"""Actual source membership and shared enrollment across TWO migrated databases."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from loom.db.schema import Team, TeamMembership, User
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import setup
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def enrollment(applications, platform_inputs, database_access, shared_ca):
    result = await setup(applications, platform_inputs, database_access, shared_ca)
    async with result[2]() as session:
        source_name = await session.scalar(text("SELECT current_database()"))
    assert source_name != database_access[0].info.dbname
    return result


@pytest.mark.parametrize("role", ["owner", "member", "viewer"])
async def test_current_projection_does_not_infer_team_owner_from_application_owner(enrollment, role):
    from loom_service.application_management.identity import ApplicationOwnerProjection

    _, registry, factory, alice, _, lease, _, _ = enrollment
    async with factory.begin() as session:
        member = await session.get(TeamMembership, (alice.team_id, alice.user_id))
        member.role = role
        user = await session.get(User, alice.user_id)
        user.display_name = "Current source profile"
        user.is_platform_admin = True
    value = await ApplicationOwnerProjection(registry).read(lease)
    assert value.role == role
    assert value.display_name == "Current source profile"
    assert set(value.model_dump()) == {"user_id", "team_id", "username", "username_normalized", "display_name", "team_name", "role"}


@pytest.mark.parametrize("change", ["missing_membership", "disabled_user", "disabled_team", "pending_user"])
async def test_source_eligibility_is_checked_from_current_rows(enrollment, change):
    from loom_service.application_management.identity import ApplicationOwnerProjection

    _, registry, factory, alice, _, lease, _, _ = enrollment
    async with factory.begin() as session:
        if change == "missing_membership":
            await session.delete(await session.get(TeamMembership, (alice.team_id, alice.user_id)))
        elif change == "disabled_team":
            await session.execute(text("UPDATE teams SET disabled_at=now() WHERE id=:id"), {"id": alice.team_id})
        elif change == "disabled_user":
            await session.execute(text("UPDATE users SET disabled_at=now() WHERE id=:id"), {"id": alice.user_id})
        else:
            user = await session.get(User, alice.user_id)
            user.status = "pending_setup"
    with pytest.raises(ProviderBlockedError, match="application_principal_unavailable"):
        await ApplicationOwnerProjection(registry).read(lease)


async def test_projection_fences_expired_and_taken_over_operation_lease(enrollment):
    from loom_service.application_management.identity import ApplicationOwnerProjection

    _, registry, factory, _, _, lease, _, _ = enrollment
    await expire(factory, lease)
    projection = ApplicationOwnerProjection(registry)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await projection.read(lease)
    replacement = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await projection.read(lease)
    assert (await projection.read(replacement)).role == "member"


async def test_prepare_enrolls_current_member_after_sql_before_delivering_iam(enrollment, database_access):
    provider, _, factory, alice, _, lease, cloud, _ = enrollment
    async with factory.begin() as session:
        user = await session.get(User, alice.user_id)
        user.password_hash, user.is_platform_admin = "management-only-password", True
        user.display_name = "Latest name"
        team = await session.get(Team, alice.team_id)
        team.name = "Latest team"
    material = await provider.prepare(lease)
    admin = database_access[0]
    assert admin.execute("SELECT u.display_name,u.password_hash,u.is_platform_admin,t.name,m.role FROM public.users u JOIN public.team_memberships m ON m.user_id=u.id JOIN public.teams t ON t.id=m.team_id WHERE u.id=%s", (alice.user_id,)).fetchone() == ("Latest name", None, False, "Latest team", "member")
    assert len(cloud.mutations) == 4
    assert await provider.prepare(lease) == material


async def test_enrollment_failure_keeps_material_and_reservation_without_data_membership(enrollment, database_access):
    provider, registry, factory, alice, _, lease, cloud, _ = enrollment
    async with factory.begin() as session:
        await session.delete(await session.get(TeamMembership, (alice.team_id, alice.user_id)))
    with pytest.raises(ProviderBlockedError, match="application_principal_unavailable"):
        await provider.prepare(lease)
    original = await registry.load_material(lease)
    assert original
    assert len(cloud.mutations) == 2
    admin = database_access[0]
    assert admin.execute("SELECT count(*) FROM loom_application_access.generations WHERE application_id=%s", (lease.application_id,)).fetchone() == (1,)
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (alice.user_id,)).fetchone() == (0,)
    async with factory.begin() as session:
        assert await session.scalar(text("SELECT cpu_millis>0 FROM nebius_application_reservations WHERE application_id=:id"), {"id": lease.application_id})
        session.add(TeamMembership(user_id=alice.user_id, team_id=alice.team_id, role="viewer"))
    assert await provider.prepare(lease) == original
    assert admin.execute("SELECT role FROM public.team_memberships WHERE user_id=%s", (alice.user_id,)).fetchone() == ("viewer",)


async def test_retry_preserves_shared_administrator_role_edit(enrollment, database_access):
    provider, _, _, alice, _, lease, _, _ = enrollment
    material = await provider.prepare(lease)
    admin = database_access[0]
    admin.execute("UPDATE public.team_memberships SET role='viewer' WHERE user_id=%s", (alice.user_id,))
    assert await provider.prepare(lease) == material
    assert admin.execute("SELECT role FROM public.team_memberships WHERE user_id=%s", (alice.user_id,)).fetchone() == ("viewer",)


@pytest.mark.parametrize("change", ["role", "lease"])
async def test_source_or_lease_change_during_external_enrollment_prevents_iam(enrollment, monkeypatch, change):
    provider, registry, factory, alice, _, lease, cloud, _ = enrollment
    original = provider.database.enroll

    async def changed(*args, **kwargs):
        result = await original(*args, **kwargs)
        if change == "lease":
            await expire(factory, lease)
            assert await registry.claim(lease.operation_id) is not None
        else:
            async with factory.begin() as session:
                member = await session.get(TeamMembership, (alice.team_id, alice.user_id))
                member.role = "viewer"
        return result

    monkeypatch.setattr(provider.database, "enroll", changed)
    expected = ManagementError if change == "lease" else ProviderRetryError
    with pytest.raises(expected):
        await provider.prepare(lease)
    assert len(cloud.mutations) == 2
