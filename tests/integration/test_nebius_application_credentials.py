"""Compose actual management material and shared SQL access, with cloud I/O controlled."""
from __future__ import annotations

import base64
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy.engine import make_url

from loom.nebius_application_render import render_application
from loom_service.application_management.cloud_provider import ApplicationCloudProvider
from loom_service.environment_management.credentials import generate_material
from loom_service.environment_management.provider import ProviderBlockedError
from tests.integration.test_nebius_application_cloud_provider import Cloud
from tests.integration.test_nebius_application_database import access_postgres as access_postgres
from tests.integration.test_nebius_application_database import database_access as database_access
from tests.integration.test_nebius_application_database import login
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture(scope="module")
def shared_ca():
    return generate_material(namespace="loom-dev", tls_secret_name="test-tls")["loom-platform-db"]["ca.crt"]


async def setup(applications, platform_inputs, database_access, shared_ca, *, slug="alice", cloud=None):
    from loom_service.application_management.credentials import (
        ApplicationCredentialProvider,
        SharedApplicationCredentials,
    )
    from loom_service.application_management.database import AsyncApplicationDatabaseAccess

    registry, factory, (alice, _), _, _, _ = applications
    admin, manager_url, _, data_id = database_access
    row, release, shared, foundation = inputs(platform_inputs, slug)
    row = row.model_copy(update={"owner_user_id": alice.user_id, "owner_team_id": alice.team_id,
                                 "data_environment_id": data_id})
    shared = shared.model_copy(update={"data_environment_id": data_id})
    prepared = render_application(row, release, shared, foundation)
    operation = await registry.create(principal=alice, idempotency_key=slug,
                                      prepared=prepared, release=release, shared=shared)
    lease = await registry.claim(operation.operation_id)
    cloud = cloud or Cloud()
    config = SharedApplicationCredentials(data_environment_id=data_id, ca_pem=shared_ca,
        secret_store_master_keys=base64.b64encode(b"s" * 32).decode(), database_name=make_url(manager_url).database)
    provider = ApplicationCredentialProvider(registry, ApplicationCloudProvider(registry, cloud),
        AsyncApplicationDatabaseAccess(manager_url, data_id), storage_binding={
            "data_environment_id": str(data_id), "project_id": "application-project",
            "data_group_id": "data-group", "source_group_id": "source-group"}, shared=config)
    return provider, registry, factory, alice, row, lease, cloud, config


def db_bundle(material):
    return next(value for name, value in material.items() if name.startswith("loom-application-db-"))


async def test_prepare_commits_real_revocable_login_and_reuses_shared_material(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, factory, alice, row, lease, cloud, shared = await setup(
        applications, platform_inputs, database_access, shared_ca)
    material = await provider.prepare(lease)
    db = db_bundle(material)
    url = make_url(db["url"])
    assert url.username == f"lap_{row.incarnation.hex}_g1"
    assert len(url.password) == 64
    assert url.host == f"loom-postgres.{(await registry.frozen_plan(lease))['shared']['platform_namespace']}.svc"
    assert dict(url.query) == {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}
    assert db["ca.crt"] == shared_ca
    auth = material[f"loom-application-auth-{row.incarnation.hex}-g1"]
    assert auth == {"secret-store-master-keys": shared.secret_store_master_keys}
    assert all("admin" not in name and "backup" not in name for name in material)
    with login(database_access[1], url.username, url.password) as connection:
        connection.execute("INSERT INTO shared_records(value) VALUES ('personal API')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("CREATE TABLE forbidden(id integer)")
    assert await provider.prepare(lease) == material
    await expire(factory, lease)
    current = await registry.claim(lease.operation_id)
    assert await provider.prepare(current) == material
    assert len(cloud.mutations) == 4
    assert url.password not in (await registry.get_operation(lease.operation_id, principal=alice)).model_dump_json()


async def test_sql_failure_after_material_commit_retries_same_password_without_new_iam(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, _, _, _, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    admin = database_access[0]
    admin.execute("GRANT CREATE ON SCHEMA public TO PUBLIC")
    with pytest.raises(ProviderBlockedError, match="application_database_public_privileges"):
        await provider.prepare(lease)
    original = await registry.load_material(lease)
    assert admin.execute("SELECT count(*) FROM loom_application_access.generations").fetchone()[0] == 0
    assert len(cloud.mutations) == 2
    admin.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    assert await provider.prepare(lease) == original
    assert len(cloud.mutations) == 4


async def test_stop_retires_database_generation_without_breaking_sibling(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, _, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    first = make_url(db_bundle(await provider.prepare(lease))["url"])
    sibling, _, _, _, _, sibling_lease, _, _ = await setup(
        applications, platform_inputs, database_access, shared_ca, slug="alice-second", cloud=cloud)
    second = make_url(db_bundle(await sibling.prepare(sibling_lease))["url"])
    with login(database_access[1], first.username, first.password) as old_connection, \
            login(database_access[1], second.username, second.password) as other:
        stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
            idempotency_key="stop", expected_generation=1)
        current = await registry.claim(stopped.operation_id)
        await provider.retire_database(current)
        with pytest.raises(psycopg.OperationalError):
            old_connection.execute("SELECT 1")
        assert other.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(psycopg.OperationalError):
            login(database_access[1], first.username, first.password)
        with pytest.raises(ProviderBlockedError, match="application_database_retired"):
            await provider.database.grant(lease, first.password)
    assert len(cloud.mutations) == 8  # SQL-only retirement does not claim S3 denial.


@pytest.mark.parametrize("damage", ["data", "ca", "keyring"])
async def test_changed_shared_material_is_not_silently_delivered_on_replay(
    applications, platform_inputs, database_access, shared_ca, damage,
):
    provider, _, _, _, _, lease, cloud, config = await setup(
        applications, platform_inputs, database_access, shared_ca)
    await provider.prepare(lease)
    changes = {"data_environment_id": uuid4()} if damage == "data" else {
        "ca_pem": "not-a-certificate"} if damage == "ca" else {
        "secret_store_master_keys": base64.b64encode(b"different-shared-key".ljust(32, b"x")).decode()}
    provider.shared = replace(config, **changes)
    with pytest.raises(ProviderBlockedError):
        await provider.prepare(lease)
    assert len(cloud.mutations) == 4
