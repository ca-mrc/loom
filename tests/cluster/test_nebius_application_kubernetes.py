"""Actual disposable Kubernetes requests driven by the PostgreSQL effect journal."""
from __future__ import annotations

import asyncio
import copy
import os
import ssl
import time

import httpx
import pytest

from loom_service.application_management.kubernetes import (
    ApplicationKubernetesProvider,
    KubernetesEffectRejectedError,
)
from loom_service.environment_management.provider import ProviderWaitingError
from tests.integration.conftest import (
    isolated_migration_postgres_url as isolated_migration_postgres_url,
)
from tests.integration.conftest import (
    migration_template_postgres_url as migration_template_postgres_url,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.integration.test_nebius_application_credentials import setup as credential_setup
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_database import access_postgres as access_postgres
from tests.integration.test_nebius_application_database import database_access as database_access
from tests.integration.test_nebius_application_effects import started
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_journal_drives_real_create_preconditioned_patch_and_delete(applications):
    from kubernetes import client

    registry, _, _, plan, _, lease = await started(applications)
    # Disposable startup is outside the operation: renew before the first effect.
    container = await asyncio.to_thread(_start_k3s)
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        write_statuses = []
        async def response_status(response):
            if response.request.method != "GET":
                write_statuses.append((response.request.method, response.status_code))
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     event_hooks={"response": [response_status]}) as http:
            provider = ApplicationKubernetesProvider(registry, http)
            await registry.renew(lease, lease_seconds=180)
            namespace = await provider.create(lease, "namespace", named(plan["prepared"], "Namespace", "loom-dev-alice"))
            assert namespace.phase == "observed"
            document = copy.deepcopy(named(plan["prepared"], "Deployment", "loom-service"))
            document["spec"]["replicas"] = 0  # Never pull/run application images.
            created = await provider.create(lease, "api", document)
            deployments = client.AppsV1Api(core.api_client)
            deadline = time.monotonic() + 20
            while True:
                current = await asyncio.to_thread(deployments.read_namespaced_deployment, "loom-service", "loom-dev-alice")
                if (current.status.observed_generation or 0) >= current.metadata.generation:
                    break
                assert time.monotonic() < deadline, "disposable Deployment controller did not observe zero replicas"
                await asyncio.sleep(0.1)
            patched = await provider.patch_spec(lease, "confirm-stop", document,
                uid=created.observed_uid, resource_version=current.metadata.resource_version)
            assert patched.observed_uid == created.observed_uid and patched.phase == "observed"
            # The immutable request version is retained even if Kubernetes changes
            # status later. Any conflicting write remains unresolved, never resent.
            target = dict(api_version="apps/v1", kind="Deployment", namespace="loom-dev-alice", name="loom-service",
                          uid=patched.observed_uid, resource_version=patched.observed_resource_version)
            deadline = time.monotonic() + 20
            attempt = 0
            while True:
                try:
                    deleted = await provider.delete(lease, f"delete-api-{attempt}", **target)
                    break
                except KubernetesEffectRejectedError as exc:
                    assert exc.status_code == 409 and attempt < 3
                    current = await asyncio.to_thread(deployments.read_namespaced_deployment, "loom-service", "loom-dev-alice")
                    assert current.metadata.uid == patched.observed_uid
                    target["resource_version"] = current.metadata.resource_version
                    attempt += 1  # Only definitive rejection permits a new key.
                except ProviderWaitingError:
                    assert time.monotonic() < deadline, f"exact retirement did not reconcile; write status codes: {write_statuses}"
                    await asyncio.sleep(0.1)
            assert deleted.phase == "observed" and deleted.observed_resource_version is None
            history = await registry.effect_history(lease)
            assert len(history) == 4 + attempt
            assert sum(effect.phase == "rejected" for effect in history) == attempt
            assert (await asyncio.to_thread(core.list_namespaced_pod, "loom-dev-alice")).items == []
    finally:
        await asyncio.to_thread(container.stop)


@pytest.mark.timeout(180)
async def test_stop_records_predecessor_create_after_lost_real_response(applications):
    registry, _, alice, plan, _, lease = await started(applications)
    await registry.renew(lease, lease_seconds=180)
    container = await asyncio.to_thread(_start_k3s)
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        writes = []

        async def lose_deployment_reply(response):
            if response.request.method != "GET":
                writes.append((response.request.method, response.status_code))
            if response.request.method == "POST" and response.request.url.path.endswith("/deployments"):
                assert response.status_code == 201
                raise httpx.ReadTimeout("simulated response loss after real Kubernetes CREATE")

        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     event_hooks={"response": [lose_deployment_reply]}) as http:
            provider = ApplicationKubernetesProvider(registry, http)
            await registry.renew(lease, lease_seconds=180)
            await provider.create(lease, "namespace", named(plan["prepared"], "Namespace", "loom-dev-alice"))
            document = copy.deepcopy(named(plan["prepared"], "Deployment", "loom-service"))
            document["spec"]["replicas"] = 0
            with pytest.raises(ProviderWaitingError):
                await provider.create(lease, "api", document)
            stopped = await registry.transition(lease.application_id, principal=alice, action="suspend",
                idempotency_key="stop", expected_generation=1)
            current = await registry.claim(stopped.operation_id, lease_seconds=180)
            result = await provider.reconcile(current, lease.operation_id, "api", document=document)
            assert result.phase == "observed" and result.operation_id == lease.operation_id
            assert result.observed_uid and result.observed_resource_version
            assert writes == [("POST", 201), ("POST", 201)]
            assert (await asyncio.to_thread(core.list_namespaced_pod, "loom-dev-alice")).items == []
    finally:
        await asyncio.to_thread(container.stop)


@pytest.mark.timeout(180)
async def test_composed_credentials_create_only_immutable_generation_secrets(
    applications, platform_inputs, database_access, shared_ca,
):
    from kubernetes.client.exceptions import ApiException

    credentials, registry, _, _, row, lease, _, _ = await credential_setup(
        applications, platform_inputs, database_access, shared_ca)
    await registry.renew(lease, lease_seconds=180)
    container = await asyncio.to_thread(_start_k3s)
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False) as http:
            kubernetes = ApplicationKubernetesProvider(registry, http)
            await registry.renew(lease, lease_seconds=180)
            plan = await registry.frozen_plan(lease)
            namespace = next(doc for docs in plan["files"].values() for doc in docs if doc["kind"] == "Namespace")
            await kubernetes.create(lease, "namespace", namespace)
            await credentials.deliver(lease, kubernetes)
            await credentials.deliver(lease, kubernetes)
        material = await registry.load_material(lease)
        actual = (await asyncio.to_thread(core.list_namespaced_secret, row.application_namespace)).items
        assert {secret.metadata.name for secret in actual} == set(material)
        for secret in actual:
            assert secret.immutable is True
            assert set(secret.data) == set(material[secret.metadata.name])
        with pytest.raises(ApiException) as failure:
            await asyncio.to_thread(core.patch_namespaced_secret, actual[0].metadata.name,
                                   row.application_namespace, {"data": {"injected": "eA=="}})
        assert failure.value.status == 422
        assert (await asyncio.to_thread(core.list_namespaced_pod, row.application_namespace)).items == []
        assert len(await registry.effect_history(lease)) == 4
    finally:
        await asyncio.to_thread(container.stop)
