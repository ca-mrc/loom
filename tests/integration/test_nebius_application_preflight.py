"""Live startup access qualification against separate management/shared databases."""
from __future__ import annotations

import hashlib

import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.db.schema import User
from loom.db.schema_startup import service_schema_head
from loom_service.environment_management.provider import ProviderBlockedError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import (
    db_bundle,
    setup,
)
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def prepared_access(applications, platform_inputs, database_access, shared_ca):
    context = await setup(applications, platform_inputs, database_access, shared_ca)
    material = await context[0].prepare(context[5])
    return context, material


async def test_qualification_preserves_material_shared_profile_and_actual_role(prepared_access, database_access):
    (provider, registry, _, _, row, lease, cloud, _), material = prepared_access
    admin = database_access[0]
    admin.execute("UPDATE public.team_memberships SET role='viewer' WHERE user_id=%s AND team_id=%s", (row.owner_user_id, row.owner_team_id))
    admin.execute("UPDATE public.users SET display_name='Shared profile',password_hash='retained' WHERE id=%s", (row.owner_user_id,))
    before = admin.execute("SELECT (SELECT count(*) FROM public.users),(SELECT count(*) FROM public.teams)").fetchone()
    proof = await provider.qualify(lease)
    assert proof.identity.operation_id == lease.operation_id and proof.identity.data_environment_id == row.data_environment_id
    assert proof.schema_revision == service_schema_head() and proof.database_role == f"lap_{row.incarnation.hex}_g1"
    assert (proof.user_id, proof.team_id, proof.membership_role) == (row.owner_user_id, row.owner_team_id, "viewer")
    assert proof.access_key_sha256 == hashlib.sha256(b"test-access-key").hexdigest()
    assert "test-access-key" not in proof.model_dump_json() and "test-private-key" not in proof.model_dump_json()
    assert make_url(db_bundle(material)["url"]).password not in proof.model_dump_json()
    assert await registry.load_material(lease) == material
    assert len(cloud.mutations) == 4
    assert admin.execute("SELECT (SELECT count(*) FROM public.users),(SELECT count(*) FROM public.teams)").fetchone() == before
    assert admin.execute("SELECT display_name,password_hash FROM public.users WHERE id=%s", (row.owner_user_id,)).fetchone() == ("Shared profile", "retained")


async def test_qualification_cannot_initialize_unprepared_access(applications, platform_inputs, database_access, shared_ca):
    provider, registry, _, _, _, lease, cloud, _ = await setup(applications, platform_inputs, database_access, shared_ca)
    with pytest.raises((ManagementError, ProviderBlockedError)):
        await provider.qualify(lease)
    assert cloud.mutations == []
    assert await registry.cloud_history(lease) == []
    assert database_access[0].execute("SELECT count(*) FROM loom_application_access.generations").fetchone() == (0,)


async def test_qualification_does_not_finish_interrupted_membership_creation(
    applications, platform_inputs, database_access, shared_ca, monkeypatch,
):
    provider, _, _, _, _, lease, cloud, _ = await setup(applications, platform_inputs, database_access, shared_ca)
    create = provider.cloud.create

    async def interrupt(current, key, binding):
        if key == "data":
            raise InterruptedError("before membership intent")
        return await create(current, key, binding)

    with monkeypatch.context() as patch:
        patch.setattr(provider.cloud, "create", interrupt)
        with pytest.raises(InterruptedError):
            await provider.prepare(lease)
    with pytest.raises(ProviderBlockedError, match="application_access_not_prepared"):
        await provider.qualify(lease)
    assert len(cloud.mutations) == 2


@pytest.mark.parametrize("damage", ["schema", "source-disabled", "shared-disabled", "membership-removed", "cloud-deleted", "sql-retired", "changed-key"])
async def test_qualification_rejects_current_access_drift(prepared_access, database_access, monkeypatch, damage):
    (provider, _, factory, _, row, lease, cloud, _), _ = prepared_access
    admin = database_access[0]
    if damage == "schema":
        admin.execute("UPDATE public.alembic_version SET version_num='changed'")
    elif damage == "source-disabled":
        async with factory.begin() as session:
            user = await session.get(User, row.owner_user_id)
            user.status = "disabled"
    elif damage == "shared-disabled":
        admin.execute("UPDATE public.users SET disabled_at=now() WHERE id=%s", (row.owner_user_id,))
    elif damage == "membership-removed":
        admin.execute("DELETE FROM public.team_memberships WHERE user_id=%s AND team_id=%s", (row.owner_user_id, row.owner_team_id))
    elif damage == "cloud-deleted":
        cloud.resources.pop("resource-3")
    elif damage == "sql-retired":
        await provider.database.revoke(lease, 1)
    else:
        async def changed(identity):
            return {"access-key": "changed", "secret-key": "changed"}
        monkeypatch.setattr(cloud, "access_key_secret", changed)
    with pytest.raises(ProviderBlockedError):
        await provider.qualify(lease)
    assert len(cloud.mutations) == 4


@pytest.mark.parametrize("privilege,target", [
    ("SELECT", "TABLE public.shared_records"), ("INSERT", "TABLE public.shared_records"),
    ("UPDATE", "TABLE public.shared_records"), ("DELETE", "TABLE public.shared_records"),
    ("USAGE", "SEQUENCE public.shared_records_id_seq"), ("SELECT", "SEQUENCE public.shared_records_id_seq"),
])
async def test_qualification_requires_every_runtime_privilege_without_repair(prepared_access, database_access, privilege, target):
    (provider, _, _, _, row, lease, cloud, _), _ = prepared_access
    admin = database_access[0]
    runtime = "loom_app_runtime_" + row.data_environment_id.hex
    admin.execute(sql.SQL("REVOKE {} ON {} FROM {}").format(sql.SQL(privilege), sql.SQL(target), sql.Identifier(runtime)))
    with pytest.raises(ProviderBlockedError, match="application_database_runtime_grants"):
        await provider.qualify(lease)
    function = "has_sequence_privilege" if target.startswith("SEQUENCE") else "has_table_privilege"
    assert admin.execute(sql.SQL("SELECT pg_catalog.{}(%s,%s,%s)").format(sql.Identifier(function)),
                         (runtime, target.split()[1], privilege)).fetchone() == (False,)
    assert len(cloud.mutations) == 4


async def test_qualification_cannot_return_evidence_after_database_await_supersession(prepared_access, monkeypatch):
    (provider, registry, _, alice, row, lease, cloud, _), _ = prepared_access
    qualify = provider.database.qualify

    async def stop_after_read(*args, **kwargs):
        result = await qualify(*args, **kwargs)
        await registry.transition(row.application_id, principal=alice, idempotency_key="stop",
            action="suspend", expected_generation=1)
        return result

    monkeypatch.setattr(provider.database, "qualify", stop_after_read)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await provider.qualify(lease)
    assert len(cloud.mutations) == 4
