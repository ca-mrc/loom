"""Compose actual management material and shared SQL access, with cloud I/O controlled."""
from __future__ import annotations

import base64
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import psycopg
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy.engine import make_url

from loom.nebius_application_render import render_application
from loom_service.application_management.cloud_provider import ApplicationCloudProvider
from loom_service.environment_management.credentials import generate_material
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_cloud_provider import Cloud
from tests.integration.test_nebius_application_database import access_postgres as access_postgres
from tests.integration.test_nebius_application_database import database_access as database_access
from tests.integration.test_nebius_application_database import login
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_kubernetes import KubernetesAPI
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
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
    _, manager_url, _, data_id = database_access
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
    with pytest.raises(ProviderBlockedError, match="application_database_role_identity"):
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


async def test_stop_retires_only_owned_cloud_generation_and_requires_data_plane_denial(
    applications, platform_inputs, database_access, shared_ca,
):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    provider, registry, _, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    await provider.prepare(lease)
    sibling, _, _, _, _, sibling_lease, _, _ = await setup(
        applications, platform_inputs, database_access, shared_ca, slug="alice-second", cloud=cloud)
    await sibling.prepare(sibling_lease)
    sibling_ids = set(cloud.resources) - {"resource-1", "resource-2", "resource-3", "resource-4"}
    stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
        idempotency_key="stop", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    original = await registry.frozen_plan(current, operation_id=lease.operation_id)
    env = next(doc for docs in original["files"].values() for doc in docs
               if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")["spec"]["template"]["spec"]["containers"][0]["env"]
    endpoint = next(entry["value"] for entry in env if entry["name"] == "LOOM_SVC_MINIO_ENDPOINT")
    code = "AccessDenied"
    requests = []

    def rejection(request):
        requests.append(request)
        return httpx.Response(403, text=f"<Error><Code>{code}</Code></Error>")

    async with httpx.AsyncClient(base_url=endpoint, transport=httpx.MockTransport(rejection)) as http:
        verifier = ApplicationObjectAccessVerifier(http)
        with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
            await provider.retire_cloud(current, verifier)
        assert set(cloud.resources) == sibling_ids
        assert [entry[1:3] for entry in cloud.mutations[8:]] == [
            ("membership", "resource-4"), ("membership", "resource-3"),
            ("access_key", "resource-2"), ("service_account", "resource-1")]
        code = "InvalidAccessKeyId"
        await provider.retire_cloud(current, verifier)
        destroyed = await registry.transition(row.application_id, principal=alice, action="destroy_retained",
            idempotency_key="destroy", expected_generation=2)
        latest = await registry.claim(destroyed.operation_id)
        await provider.retire_cloud(latest, verifier)
    assert len(cloud.mutations) == 12 and len(requests) == 3
    assert all("Credential=test-access-key/" in request.headers["Authorization"] for request in requests)


async def test_stop_does_not_dispatch_prepared_cloud_creation(
    applications, platform_inputs, database_access, shared_ca,
):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    provider, registry, _, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    await registry.prepare_cloud_create(lease, "account", provider.storage.model_dump(mode="json"))
    stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
        idempotency_key="stop", expected_generation=1)
    current = await registry.claim(stopped.operation_id)

    def unexpected(request):
        raise AssertionError("unsent account has no delivered object key to probe")

    async with httpx.AsyncClient(base_url="https://storage.test", transport=httpx.MockTransport(unexpected)) as http:
        await provider.retire_cloud(current, ApplicationObjectAccessVerifier(http))
    assert cloud.mutations == []


async def test_active_generation_created_during_retirement_scan_is_not_retired(
    applications, platform_inputs, database_access, shared_ca, monkeypatch,
):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    provider, registry, _, _, _, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    history = registry.cloud_history
    first = True

    async def peer_prepares(current):
        nonlocal first
        result = await history(current)
        if first:
            first = False
            await provider.prepare(current)
        return result

    monkeypatch.setattr(registry, "cloud_history", peer_prepares)
    async with httpx.AsyncClient(base_url="https://storage.test") as http:
        await provider.retire_cloud(lease, ApplicationObjectAccessVerifier(http))
    assert len(cloud.mutations) == 4 and all(entry[0] == "create" for entry in cloud.mutations)


async def test_cloud_retirement_recovers_lost_delete_across_destroy_without_resending(
    applications, platform_inputs, database_access, shared_ca,
):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    provider, registry, _, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    await provider.prepare(lease)
    stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
        idempotency_key="stop", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    original = await registry.frozen_plan(current)
    env = next(doc for docs in original["files"].values() for doc in docs
               if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")["spec"]["template"]["spec"]["containers"][0]["env"]
    endpoint = next(entry["value"] for entry in env if entry["name"] == "LOOM_SVC_MINIO_ENDPOINT")
    requests = []

    def rejection(request):
        requests.append(request)
        return httpx.Response(403, text="<Error><Code>InvalidAccessKeyId</Code></Error>")

    async with httpx.AsyncClient(base_url=endpoint, transport=httpx.MockTransport(rejection)) as http:
        verifier = ApplicationObjectAccessVerifier(http)
        cloud.delay_delete = True
        with pytest.raises(ProviderRetryError):
            await provider.retire_cloud(current, verifier)
        destroyed = await registry.transition(row.application_id, principal=alice, action="destroy_retained",
            idempotency_key="destroy", expected_generation=2)
        latest = await registry.claim(destroyed.operation_id)
        cloud.delay_delete = False
        with pytest.raises(ProviderWaitingError):
            await provider.retire_cloud(latest, verifier)
        assert len(cloud.mutations) == 5 and requests == []
        del cloud.resources["resource-4"]  # The single original DELETE takes effect late.
        await provider.retire_cloud(latest, verifier)
    assert cloud.resources == {} and len(cloud.mutations) == 8 and len(requests) == 1


async def test_permissionless_undelivered_key_retires_without_invented_probe_material(
    applications, platform_inputs, database_access, shared_ca,
):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    provider, registry, _, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    for key in ("account", "key"):
        await provider.cloud.create(lease, key, provider.storage.model_dump(mode="json"))
    stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
        idempotency_key="stop", expected_generation=1)
    current = await registry.claim(stopped.operation_id)

    def unexpected(request):
        raise AssertionError("a never-delivered permissionless key needs no fabricated material")

    async with httpx.AsyncClient(base_url="https://storage.test", transport=httpx.MockTransport(unexpected)) as http:
        await provider.retire_cloud(current, ApplicationObjectAccessVerifier(http))
    assert cloud.resources == {} and len(cloud.mutations) == 4


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


async def test_duplicate_ca_extensions_are_bounded_before_any_access_grant(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, _, _, _, _, lease, cloud, config = await setup(
        applications, platform_inputs, database_access, shared_ca)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "malformed-test-ca")])
    now = datetime.now(UTC)
    builder = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(1).not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True))
    # Only the malformed fixture bypasses the builder's duplicate-extension guard.
    builder._extensions.append(builder._extensions[0])
    pem = builder.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM).decode()
    provider.shared = replace(config, ca_pem=pem)
    with pytest.raises(ProviderBlockedError, match="application_shared_credentials_invalid"):
        await provider.prepare(lease)
    assert cloud.mutations == []
    assert database_access[0].execute("SELECT count(*) FROM loom_application_access.generations").fetchone()[0] == 0


async def test_retirement_does_not_need_a_deliverable_ca_or_keyring(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, _, alice, row, lease, _, config = await setup(
        applications, platform_inputs, database_access, shared_ca)
    url = make_url(db_bundle(await provider.prepare(lease))["url"])
    provider.shared = replace(config, ca_pem="retired-ca", secret_store_master_keys="retired-keyring")
    stopped = await registry.transition(row.application_id, principal=alice, action="suspend",
        idempotency_key="stop", expected_generation=1)
    await provider.retire_database(await registry.claim(stopped.operation_id))
    with pytest.raises(psycopg.OperationalError):
        login(database_access[1], url.username, url.password)


async def test_invalid_persisted_url_is_bounded_before_any_access_grant(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, _, _, row, lease, cloud, config = await setup(
        applications, platform_inputs, database_access, shared_ca)
    await registry.ensure_material(lease, lambda _: {
        f"loom-application-db-{row.incarnation.hex}-g1": {"url": "invalid-private-url", "ca.crt": shared_ca},
        f"loom-application-storage-{row.incarnation.hex}-g1": {"access-key": "x", "secret-key": "y"},
        f"loom-application-auth-{row.incarnation.hex}-g1": {"secret-store-master-keys": config.secret_store_master_keys},
    })
    with pytest.raises(ProviderBlockedError, match="application_credential_material_conflict") as error:
        await provider.prepare(lease)
    assert "invalid-private-url" not in str(error.value)
    assert len(cloud.mutations) == 2
    assert database_access[0].execute("SELECT count(*) FROM loom_application_access.generations").fetchone()[0] == 0


async def test_lease_loss_during_key_read_prevents_credential_commit_and_grants(
    applications, platform_inputs, database_access, shared_ca,
):
    provider, registry, factory, _, _, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    read = cloud.access_key_secret

    async def delayed(identity):
        value = await read(identity)
        await expire(factory, lease)
        return value

    cloud.access_key_secret = delayed
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await provider.prepare(lease)
    current = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match="application_material_missing"):
        await registry.load_material(current)
    assert len(cloud.mutations) == 2
    assert database_access[0].execute("SELECT count(*) FROM loom_application_access.generations").fetchone()[0] == 0


async def test_generation_secret_delivery_matches_renderer_and_never_reposts_after_timeout(
    applications, platform_inputs, database_access, shared_ca,
):
    from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
    from loom_service.environment_management.provider import ProviderWaitingError

    provider, registry, _, _, _, lease, _, _ = await setup(
        applications, platform_inputs, database_access, shared_ca)
    plan = await registry.frozen_plan(lease)
    documents = [doc for group in plan["files"].values() for doc in group]
    namespace = next(doc for doc in documents if doc["kind"] == "Namespace")
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        kubernetes = ApplicationKubernetesProvider(registry, http)
        await kubernetes.create(lease, "namespace", namespace)
        api.lose_response = True
        with pytest.raises(ProviderWaitingError):
            await provider.deliver(lease, kubernetes)
        assert len(api.mutations) == 2
        api.lose_response = False
        await provider.deliver(lease, kubernetes)
        await provider.deliver(lease, kubernetes)
    assert len(api.mutations) == 4  # Namespace and three immutable generation Secrets.
    material = await registry.load_material(lease)
    secrets_by_name = {doc["metadata"]["name"]: doc for doc in api.objects.values() if doc["kind"] == "Secret"}
    assert set(secrets_by_name) == set(material)
    for name, values in material.items():
        secret = secrets_by_name[name]
        assert secret["immutable"] is True and secret["type"] == "Opaque"
        assert secret["metadata"]["namespace"] == namespace["metadata"]["name"]
        assert {key: base64.b64decode(value).decode() for key, value in secret["data"].items()} == values
    deployment = next(doc for doc in documents if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
    for variable in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
        if "valueFrom" in variable:
            reference = variable["valueFrom"]["secretKeyRef"]
            assert reference["key"] in material[reference["name"]]
    password = make_url(db_bundle(material)["url"]).password
    assert password not in repr(await registry.effect_history(lease))
