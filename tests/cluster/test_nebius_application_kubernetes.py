"""Actual disposable Kubernetes requests driven by the PostgreSQL effect journal."""
from __future__ import annotations

import asyncio
import base64
import copy
import os
import ssl
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4

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
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import setup as credential_setup
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_effects import started
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_material import material as fixture_material
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import runtime_inputs
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(240)
@pytest.mark.parametrize("running", [False, True])
async def test_runtime_retires_routes_and_waits_for_real_deployment_controllers(applications, platform_inputs, running):
    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    tag = "docker.io/library/loom-application-retirement:" + uuid4().hex
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        image = None
        if running:
            platform = await asyncio.to_thread(_docker_platform)
            await asyncio.to_thread(_build_image, tag=tag,
                dockerfile="tests/fixtures/execution_runtime_fixture/Dockerfile", platform=platform)
            with tempfile.TemporaryDirectory(prefix="loom-application-retirement-") as temporary:
                image = await asyncio.to_thread(_import_image, container, tag=tag, root=Path(temporary), ordinal=1)
        registry, authority, lease, alice, rendered = await runtime_inputs(
            applications, platform_inputs, fixture_image=image)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        requests = []

        async def record_request(request):
            requests.append((request.method, request.url.path))

        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     event_hooks={"request": [record_request]}) as http:
            kubernetes = ApplicationKubernetesProvider(registry, http)
            runtime = ApplicationRuntimeProvider(registry, kubernetes, authority=authority)
            await registry.renew(lease, lease_seconds=180)
            await kubernetes.create(lease, "namespace", named(rendered, "Namespace", "loom-dev-alice"))
            await kubernetes.create(lease, "account", named(rendered, "ServiceAccount", "loom-platform"))
            deadline = time.monotonic() + 30
            while not running:
                try:
                    await runtime.close_admission(lease)
                    break
                except ProviderWaitingError:
                    assert time.monotonic() < deadline
                    await asyncio.sleep(0.1)
            # Default images never run. The second case starts harmless imported
            # fixtures before the gate, proving real controller-owned Pod exit.
            for docs in rendered.files.values():
                for doc in docs:
                    if doc["kind"] in {"Deployment", "Service", "Ingress"}:
                        await kubernetes.create(lease, doc["kind"] + ":" + doc["metadata"]["name"], doc)
            if running:
                deadline = time.monotonic() + 60
                while True:
                    pods = (await asyncio.to_thread(core.list_namespaced_pod, "loom-dev-alice")).items
                    if len(pods) == 2 and all(pod.status.phase == "Running" for pod in pods):
                        assert all(pod.metadata.owner_references[0].kind == "ReplicaSet" for pod in pods)
                        break
                    assert time.monotonic() < deadline, "fixture Deployment Pods did not start"
                    await asyncio.sleep(0.2)
            stopped = await registry.transition(lease.application_id, principal=alice,
                idempotency_key="stop", action="suspend", expected_generation=1)
            current = await registry.claim(stopped.operation_id)
            deadline = time.monotonic() + 30
            while True:
                try:
                    await runtime.stop_workloads(current)
                    break
                except ProviderWaitingError:
                    assert time.monotonic() < deadline, "personal process retirement did not converge"
                    await asyncio.sleep(0.1)
            for name in ("loom-service", "loom-web"):
                response = await http.get("/apis/apps/v1/namespaces/loom-dev-alice/deployments/" + name)
                assert response.status_code == 200
                deployment = response.json()
                assert deployment["spec"]["replicas"] == 0
                assert deployment["status"]["observedGeneration"] >= deployment["metadata"]["generation"]
            assert (await asyncio.to_thread(core.list_namespaced_service, "loom-dev-alice")).items == []
            assert (await asyncio.to_thread(core.list_namespaced_pod, "loom-dev-alice")).items == []
            assert (await http.get("/apis/networking.k8s.io/v1/namespaces/loom-dev-alice/ingresses/loom-web")).status_code == 404
            assert not any(method == "DELETE" and "/pods/" in path for method, path in requests)
            await runtime.stop_workloads(current)
    finally:
        await asyncio.to_thread(container.stop)
        if running:
            await asyncio.to_thread(subprocess.run, ["docker", "image", "rm", tag], capture_output=True, check=False)


@pytest.mark.timeout(180)
@pytest.mark.parametrize("active", [False, True])
async def test_application_preparation_and_stop_use_only_protected_manager_authority(applications, platform_inputs, active):
    from kubernetes import client, utils

    from loom.nebius_application_authority import render_application_authority
    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    registry, authority, lease, alice, _ = await runtime_inputs(applications, platform_inputs)
    if not active:
        stopped = await registry.transition(lease.application_id, principal=alice, action='suspend',
            idempotency_key='early-stop', expected_generation=1)
        lease = await registry.claim(stopped.operation_id, lease_seconds=300)
    else:
        await registry.renew(lease, lease_seconds=300)
    container = await asyncio.to_thread(_start_k3s)
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        await asyncio.to_thread(core.create_namespace, {'metadata': {'name': authority.namespace}})
        await asyncio.to_thread(core.create_namespaced_service_account, authority.namespace,
            {'metadata': {'name': 'loom-application-provisioner'}})
        documents = render_application_authority(authority)
        for document in documents:
            await asyncio.to_thread(utils.create_from_dict, core.api_client, document)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        deadline = time.monotonic() + 25
        for document in documents:
            if document['kind'] != 'ValidatingAdmissionPolicy':
                continue
            while True:
                policy = await asyncio.to_thread(admission.read_validating_admission_policy, document['metadata']['name'])
                if policy.status and policy.status.type_checking:
                    break
                assert time.monotonic() < deadline, 'protected admission policy not ready'
                await asyncio.sleep(0.1)
        issued = await asyncio.to_thread(core.create_namespaced_service_account_token,
            'loom-application-provisioner', authority.namespace, client.AuthenticationV1TokenRequest(
                spec=client.V1TokenRequestSpec(audiences=[])))
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        # No admin client certificate: only the real protected manager token.
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     headers={'Authorization': 'Bearer ' + issued.status.token}) as http:
            async def allowed(resource):
                response = await http.post('/apis/authorization.k8s.io/v1/selfsubjectaccessreviews', json={
                    'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectAccessReview',
                    'spec': {'resourceAttributes': {'namespace': 'loom-dev-alice', 'group': '',
                                                    'resource': resource, 'verb': 'create'}}})
                assert response.status_code == 201
                return response.json()['status']['allowed'] is True
            deadline = time.monotonic() + 25
            while not await allowed('namespaces'):
                assert time.monotonic() < deadline, 'bootstrap authorization not ready'
                await asyncio.sleep(0.1)
            assert not await allowed('resourcequotas')
            provider = ApplicationRuntimeProvider(registry, ApplicationKubernetesProvider(registry, http), authority=authority)
            deadline = time.monotonic() + 30
            while True:
                try:
                    if active:
                        await provider.prepare_static(lease)
                    else:
                        proof = await provider.stop_workloads(lease)
                        assert proof.deployments == ()
                    break
                except ProviderWaitingError:
                    assert time.monotonic() < deadline, 'protected preparation/early stop did not converge'
                    await asyncio.sleep(0.1)
            assert await allowed('resourcequotas')
            effects = await registry.effect_history(lease)
            expected = [
                ('Namespace', 'create'), ('RoleBinding', 'create'), ('ResourceQuota', 'create')]
            if active:
                expected += [('ServiceAccount', 'create'), *[('NetworkPolicy', 'create')] * 4]
                await provider.prepare_static(lease)
            assert [(item.intent.kind, item.intent.action) for item in effects] == expected
            assert all(item.phase == 'observed' for item in effects)
            assert (await asyncio.to_thread(core.list_namespaced_pod, 'loom-dev-alice')).items == []
            assert (await http.get('/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service')).status_code == 404
            assert (await http.get('/apis/networking.k8s.io/v1/namespaces/loom-dev-alice/ingresses/loom-web')).status_code == 404
            assert (await http.get('/api/v1/namespaces/loom-dev/services')).status_code == 403
            assert (await http.delete('/api/v1/namespaces/loom-dev-alice')).status_code == 403
            if active:
                from loom.nebius_application_authority import render_application_shared_observer
                from loom.nebius_application_network import application_shared_network_policies

                shared_ns = authority.shared_namespace
                await asyncio.to_thread(core.create_namespace, {'metadata': {'name': shared_ns}})
                shared_policies = application_shared_network_policies(authority)
                observer = render_application_shared_observer(authority)
                network_path = f'/apis/networking.k8s.io/v1/namespaces/{shared_ns}/networkpolicies'
                first_path = network_path + '/' + shared_policies[0]['metadata']['name']
                assert (await http.get(first_path)).status_code == 403
                for doc in observer:
                    collection = 'roles' if doc['kind'] == 'Role' else 'rolebindings'
                    denied = await http.post(f'/apis/rbac.authorization.k8s.io/v1/namespaces/{shared_ns}/{collection}', json=doc)
                    assert denied.status_code == 403, (collection, denied.text)  # No Secret/request credential content.
                    if doc['kind'] == 'Role':
                        # A binding to a missing Role returns404 during RBAC
                        # escalation checking, before admission can reject it.
                        # Install just the Role to test the actual binding denial.
                        await asyncio.to_thread(utils.create_from_dict, core.api_client, doc)
                for doc in [*shared_policies, observer[1]]:
                    await asyncio.to_thread(utils.create_from_dict, core.api_client, doc)
                deadline = time.monotonic() + 20
                while (await http.get(first_path)).status_code != 200:
                    assert time.monotonic() < deadline, 'named shared observation did not become available'
                    await asyncio.sleep(0.1)
                shared_proof = await provider.read_shared_network(lease)
                assert {item.name for item in shared_proof} == {doc['metadata']['name'] for doc in shared_policies}
                assert all(item.uid and item.resource_version for item in shared_proof)
                for path in (network_path, network_path + '/arbitrary',
                             '/apis/networking.k8s.io/v1/namespaces/loom-dev-foreign/networkpolicies/' + shared_proof[0].name,
                             f'/api/v1/namespaces/{shared_ns}/secrets/private'):
                    assert (await http.get(path)).status_code == 403
                assert (await http.post(network_path, json=shared_policies[0])).status_code == 403
                assert (await http.patch(first_path, json={'spec': {'ingress': [{}]}},
                    headers={'Content-Type': 'application/merge-patch+json'})).status_code == 403
                assert (await http.delete(first_path)).status_code == 403
                bundles = await registry.ensure_material(lease, fixture_material)
                for index, (name, values) in enumerate(bundles.items()):
                    await provider.kubernetes.create(lease, f'credential:{index}', {
                        'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'immutable': True,
                        'metadata': {'name': name, 'namespace': 'loom-dev-alice'},
                        'data': {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}})
                prepared = await provider.read_prepared(lease)
                assert len(prepared.resources) == 8
                assert set(bundles) <= {item.name for item in prepared.resources}
                assert (await asyncio.to_thread(core.list_namespaced_pod, 'loom-dev-alice')).items == []
    finally:
        await asyncio.to_thread(container.stop)


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
async def test_runtime_closes_real_pod_admission_and_advances_fence(applications, platform_inputs):
    from kubernetes.client.exceptions import ApiException

    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    registry, authority, lease, alice, rendered = await runtime_inputs(applications, platform_inputs)
    await registry.renew(lease, lease_seconds=180)
    container = await asyncio.to_thread(_start_k3s)
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False) as http:
            kubernetes = ApplicationKubernetesProvider(registry, http)
            runtime = ApplicationRuntimeProvider(registry, kubernetes, authority=authority)
            await registry.renew(lease, lease_seconds=180)
            await kubernetes.create(lease, "namespace", named(rendered, "Namespace", "loom-dev-alice"))
            await kubernetes.create(lease, "account", named(rendered, "ServiceAccount", "loom-platform"))
            deadline = time.monotonic() + 20
            while True:
                try:
                    await runtime.close_admission(lease)
                    break
                except ProviderWaitingError:
                    assert time.monotonic() < deadline, "quota controller did not confirm the fixed gate"
                    await asyncio.sleep(0.1)
            deployment = named(rendered, "Deployment", "loom-service")
            pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "gate-probe"},
                   "spec": deployment["spec"]["template"]["spec"]}
            # Dry run exercises admission without starting a process even if a
            # broken gate unexpectedly accepts the request.
            with pytest.raises(ApiException) as denied:
                await asyncio.to_thread(core.create_namespaced_pod, "loom-dev-alice", pod, dry_run="All")
            assert denied.value.status == 403 and "exceeded quota: loom-application-retired" in denied.value.body
            before = await asyncio.to_thread(core.read_namespaced_resource_quota, "loom-application-retired", "loom-dev-alice")
            stopped = await registry.transition(lease.application_id, principal=alice, action="suspend",
                idempotency_key="stop", expected_generation=1)
            current = await registry.claim(stopped.operation_id, lease_seconds=180)
            await runtime.close_admission(current)
            after = await asyncio.to_thread(core.read_namespaced_resource_quota, "loom-application-retired", "loom-dev-alice")
            assert after.metadata.uid == before.metadata.uid
            assert after.metadata.annotations["loom.nebius/deployment-generation"] == "2"
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
