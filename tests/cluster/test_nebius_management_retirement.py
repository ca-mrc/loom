"""Real API defaulting and actual retirement-SA authority, never live Kubernetes."""
from __future__ import annotations

import asyncio
import base64
import copy
import json
import os
import ssl
import time
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
import yaml

from tests.integration.conftest import (
    isolated_migration_postgres_url as isolated_migration_postgres_url,
)
from tests.integration.conftest import (
    migration_template_postgres_url as migration_template_postgres_url,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_environment_retirement import prepare_retirement
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1", reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_retirement_real_defaulting_and_scoped_service_account(
    retirement_request, environment_registry, isolated_migration_postgres_url, tmp_path, monkeypatch,
):
    from kubernetes import client, utils
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_retirement import (
        retirement_documents,
        retirement_ready,
        stage_retirement,
    )
    from scripts.ops.nebius_management_retirement_entry import HTTPSRetirementStageAPI
    from scripts.ops.nebius_management_stage import ManagementStageError
    from sqlalchemy.engine import make_url

    from loom.db.nebius_environment_schema import NebiusPlatformReservation
    from loom_service.application_management.deployment import render_application_setup
    from loom_service.environment_management.deployment import render_management
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.retirement import (
        RetirementSettings,
        RetirementTarget,
        reconcile_retirement,
        retirement_database_url,
        run_retirement,
    )

    request = retirement_request[0]
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        ca = base64.b64decode(config["clusters"][0]["cluster"]["certificate-authority-data"]).decode()
        trust = ssl.create_default_context(cadata=ca)
        certificate, key = tmp_path / "client.crt", tmp_path / "client.key"
        certificate.write_bytes(base64.b64decode(config["users"][0]["user"]["client-certificate-data"]))
        key.write_bytes(base64.b64decode(config["users"][0]["user"]["client-key-data"]))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        management = core.create_namespace({"metadata": {"name": request.binding.namespace, "labels": {
            "loom.nebius/management-installation": request.binding.installation_id,
            "pod-security.kubernetes.io/enforce": "restricted"}}})
        binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
            management.metadata.uid, core.read_namespace("kube-system").metadata.uid)
        async with httpx.AsyncClient(base_url=endpoint, verify=trust, trust_env=False, timeout=10) as operator:
            target = RetirementTarget.model_validate(await prepare_retirement(environment_registry, KubernetesEnvironmentProvider(operator)))
        request = replace(request, binding=binding, targets=(target,))
        registry, factory, (alice, bob), prepare = environment_registry
        foreign = await registry.create(principal=bob, idempotency_key="foreign", prepared=prepare("bob", bob))
        own = target.registration.application_namespace
        pvc = core.create_namespaced_persistent_volume_claim(own, {"metadata": {"name": "retained-data"},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Mi"}}}})
        async with factory() as session:
            original_storage = (await session.get(NebiusPlatformReservation, target.registration.environment_id)).storage_mib
        rendered = render_management(request.deployment, candidate=request.candidate, profile=request.profile, repo_root=request.repo_root)
        management_deployment = next(doc for doc in rendered.files["40-services.yaml"] if doc["kind"] == "Deployment")
        actual = client.AppsV1Api(core.api_client).create_namespaced_deployment(binding.namespace, management_deployment)
        active = core.api_client.sanitize_for_serialization(actual)
        setup = SimpleNamespace(deployment=request.deployment, candidate=request.candidate, profile=request.profile)
        context = SimpleNamespace(request=request, upgrade=SimpleNamespace(setup=setup), active_management=active,
            original_inputs=SimpleNamespace(operator_connection=SimpleNamespace(endpoint=endpoint)))
        fences = render_application_setup(request.deployment, candidate=request.candidate, profile=request.profile,
            repo_root=request.repo_root)["retirement"]
        for doc in fences:
            utils.create_from_dict(core.api_client, copy.deepcopy(doc))
        with httpx.Client(base_url=endpoint, verify=trust, trust_env=False, timeout=10) as operator:
            context.legacy_fence = []
            for doc in fences:
                plural = "validatingadmissionpolicies" if doc["kind"] == "ValidatingAdmissionPolicy" else "validatingadmissionpolicybindings"
                response = operator.get("/apis/admissionregistration.k8s.io/v1/" + plural + "/" + doc["metadata"]["name"])
                response.raise_for_status()
                context.legacy_fence.append(response.json())
        deadline = time.monotonic() + 30
        with HTTPSRetirementStageAPI(context=context, phase="permissions", ssl_context=trust, token=None) as api:
            while True:
                try:
                    api.verify_identity(binding)
                    break
                except ManagementStageError as exc:
                    assert str(exc) == "legacy management admission fence is not ready" and time.monotonic() < deadline
                    time.sleep(0.1)
        for phase in ("permissions", "network", "job"):
            with HTTPSRetirementStageAPI(context=context, phase=phase, ssl_context=trust, token=None) as api:
                args = dict(request=request, phase=phase, api=api, state_dir=tmp_path / phase)
                first = stage_retirement(**args)
                assert stage_retirement(**args) == first
                if phase == "job":
                    assert retirement_ready(request=request, api=api, state_dir=tmp_path / phase) is False
        docs = retirement_documents(request)
        account = next(doc["metadata"]["name"] for doc in docs["permissions"].values() if doc["kind"] == "ServiceAccount")
        token = core.create_namespaced_service_account_token(account, binding.namespace,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[], expiration_seconds=600))).status.token
        # Trust-only TLS plus a real projected-subject token: operator mTLS must
        # not accidentally authorize these negative assertions.
        with httpx.Client(base_url=endpoint, verify=ssl.create_default_context(cadata=ca),
                headers={"Authorization": "Bearer " + token}, trust_env=False, timeout=10) as subject:
            for namespace, group, resource, verb, name, allowed in (
                (own, "apps", "deployments", "patch", "loom-service", True),
                (own, "batch", "jobs", "create", "", True),
                (own, "", "pods", "delete", "terminal", True),
                (None, "", "namespaces", "get", own, True),
                (own, "", "pods", "create", "", False),
                (own, "", "secrets", "get", "loom-platform-db", False),
                (own, "", "persistentvolumeclaims", "delete", "data-loom-postgres-0", False),
                (binding.namespace, "apps", "deployments", "patch", "loom-service", False),
                (None, "", "namespaces", "create", "", False),
                (None, "", "namespaces", "get", "foreign-owner", False),
            ):
                response = subject.post("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", json={
                    "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview", "spec": {
                        "resourceAttributes": {"group": group, "resource": resource, "verb": verb,
                            **({"namespace": namespace} if namespace else {}), **({"name": name} if name else {})}}})
                assert response.status_code == 201
                assert response.json()["status"]["allowed"] is allowed, (namespace, resource, verb)
        async with httpx.AsyncClient(base_url=endpoint, verify=ssl.create_default_context(cadata=ca),
                headers={"Authorization": "Bearer " + token}, trust_env=False, timeout=10) as runtime:
            settings_doc = next(doc for doc in docs["job"].values() if doc["kind"] == "ConfigMap")
            settings = RetirementSettings.model_validate(json.loads(settings_doc["data"]["retirement.json"]))
            projected_ca, projected_token = tmp_path / "projected-ca.crt", tmp_path / "projected-token"
            projected_ca.write_text(ca)
            projected_token.write_text(token)
            projected_token.chmod(0o440)
            # Retarget only disposable transport addresses/mounts. Exercise the
            # actual startup, projected-token auth and pre-claim qualification.
            settings = settings.model_copy(update={"kubernetes": settings.kubernetes.model_copy(update={
                "endpoint": endpoint, "ca_file": projected_ca, "token_file": projected_token,
            })})
            database_url = (f"postgresql://loom_service:fixture@loom-postgres.{binding.namespace}.svc:5432/loom"
                "?sslmode=verify-full&sslrootcert=/var/run/loom-db/ca.crt")

            def disposable_database(value, namespace):
                assert value == database_url and namespace == binding.namespace
                retirement_database_url(value, namespace)
                return make_url(isolated_migration_postgres_url)

            monkeypatch.setattr("loom_service.environment_management.retirement.retirement_database_url", disposable_database)
            async with asyncio.timeout(75):
                await run_retirement(settings, database_url)
            operation = await registry.get_operation(target.operation_id, principal=alice)
            assert operation.phase == "completed", operation.error_code
            assert await reconcile_retirement(factory, target, KubernetesEnvironmentProvider(runtime)) == "completed"
        assert (await registry.get_operation(foreign.operation_id, principal=bob)).phase == "pending"
        async with factory() as session:
            reservation = await session.get(NebiusPlatformReservation, target.registration.environment_id)
            assert (reservation.cpu_millis, reservation.memory_mib, reservation.ephemeral_storage_mib) == (0, 0, 0)
            assert reservation.storage_mib == original_storage > 0
        assert core.read_namespaced_persistent_volume_claim("retained-data", own).metadata.uid == pvc.metadata.uid
        for name, uid in target.namespace_uids.items():
            assert core.read_namespace(name).metadata.uid == str(uid)
            assert core.read_namespaced_resource_quota("loom-environment-retained", name).status.hard["pods"] == "0"
            assert not core.list_namespaced_pod(name).items
    finally:
        container.stop()
