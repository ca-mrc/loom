"""Actual API admission/defaulting for retained controller tombstones."""

from __future__ import annotations

import asyncio
import os
import ssl
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from loom.nebius_environment_render import render_environment
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderWaitingError,
    ProvisioningContext,
)
from loom_service.environment_management.registry import OperationLease
from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup
from loom_service.environment_management.retained_destroy import EnvironmentRetainedDestroy
from loom_service.environment_management.steps import ProvisioningStep, creation_steps
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import foundation_from, registration_for

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires the disposable Kubernetes lane")


def _differences(actual, expected, path=""):
    """Test-only diagnostics over synthetic manifests; never a runtime log."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return [difference for key, value in expected.items()
                for difference in _differences(actual.get(key), value, path + "/" + key)]
    if isinstance(expected, list) and isinstance(actual, list) and len(expected) == len(actual):
        return [difference for index, (a, e) in enumerate(zip(actual, expected, strict=True))
                for difference in _differences(a, e, path + "/" + str(index))]
    return [] if actual == expected or (actual is None and expected == []) else [(path, actual, expected)]


async def test_retained_stop_uses_real_json_patch_and_preserves_pvc_identity(platform_inputs):
    foundation = foundation_from(platform_inputs[0])
    row = registration_for(foundation, "alice")
    rendered = render_environment(row, platform_inputs[1], foundation, profile=platform_inputs[2], keyring={},
                                  repo_root=Path(__file__).resolve().parents[2])
    ctx = ProvisioningContext(OperationLease(uuid4(), row.environment_id, 1, 1, uuid4()),
                              row.model_dump(mode="json"), rendered.config, {})
    container = await asyncio.to_thread(_start_k3s)
    try:
        client, _, _ = await asyncio.to_thread(_load_client, container)
        config = client.Configuration.get_default_copy()
        tls = ssl.create_default_context(cafile=config.ssl_ca_cert)
        tls.load_cert_chain(config.cert_file, config.key_file)
        async with httpx.AsyncClient(base_url=config.host, verify=tls, trust_env=False, timeout=30) as http:
            provider = KubernetesEnvironmentProvider(http)
            cleanup = RetainedKubernetesCleanup(provider)
            resources = [step for step in creation_steps(rendered) if step.kind == "kubernetes"]
            for step in resources:
                try:
                    ctx.identities[step.key] = await provider.apply(ctx, step)
                except ProviderBlockedError:
                    _, path = provider._path(ctx, step.payload)
                    actual = (await http.get(path)).json()
                    pytest.fail(f"{step.key}: {_differences(actual, provider._expected(ctx, step))}")
            pvc_path = f"/api/v1/namespaces/{row.application_namespace}/persistentvolumeclaims"
            async with asyncio.timeout(30):
                while not (pvcs := (await http.get(pvc_path)).json()["items"]):
                    await asyncio.sleep(0.2)
            before = {pvc["metadata"]["name"]: pvc["metadata"]["uid"] for pvc in pvcs}
            cleanup_id = uuid4()
            for step in resources:
                if step.payload["kind"] not in {"Deployment", "StatefulSet", "Job", "CronJob", "Ingress"}:
                    continue
                original_uid = ctx.identities[step.key]
                assert await cleanup.stop(ctx, step, cleanup_id=cleanup_id) == original_uid
                assert await cleanup.stop(ctx, step, cleanup_id=cleanup_id) == original_uid
                with pytest.raises(ProviderBlockedError):
                    await provider.apply(ctx, step)  # Stale create cannot restart it.
            after = {pvc["metadata"]["name"]: pvc["metadata"]["uid"] for pvc in (await http.get(pvc_path)).json()["items"]}
            assert after == before
            destroy = EnvironmentRetainedDestroy(SimpleNamespace(), provider, SimpleNamespace(), SimpleNamespace())
            current = replace(ctx, lease=replace(ctx.lease, operation_id=cleanup_id, deployment_generation=2),
                              action="destroy_retained", source=ctx)
            async with asyncio.timeout(45):
                while True:
                    try:
                        await destroy._close_pod_admission(current, ctx, row.application_namespace, require_idle=True)
                        break
                    except ProviderWaitingError:
                        await asyncio.sleep(0.2)
            deployment = next(step.payload for step in resources if step.payload["kind"] == "Deployment")
            late = await http.post(f"/api/v1/namespaces/{row.application_namespace}/pods", json={
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "delayed-controller-pod"},
                "spec": deployment["spec"]["template"]["spec"],
            })
            assert late.status_code == 403 and "quota" in late.json()["message"].lower(), late.text

            # A scoped quota can report hard=used=0 while admitting ordinary
            # Pods. It must never serve as retained cleanup's admission proof.
            scoped = replace(current, registration={**current.registration, "application_namespace": "loom-dev-scoped"},
                             identities={})
            ns_step = ProvisioningStep("k8s:Namespace:-:loom-dev-scoped", "kubernetes", {
                "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-scoped"},
            })
            scoped.identities[ns_step.key] = await provider.apply(scoped, ns_step)
            quota_step = ProvisioningStep("retain:quota:loom-dev-scoped", "kubernetes", {
                "apiVersion": "v1", "kind": "ResourceQuota", "metadata": {
                    "name": "loom-environment-retained", "namespace": "loom-dev-scoped",
                }, "spec": {"hard": {"pods": "0"}},
            })
            quota = provider._expected(scoped, quota_step)
            quota["spec"]["scopes"] = ["Terminating"]
            quotas_path = "/api/v1/namespaces/loom-dev-scoped/resourcequotas"
            created = await http.post(quotas_path, json=quota)
            assert created.status_code == 201, created.text
            async with asyncio.timeout(30):
                while True:
                    observed = (await http.get(quotas_path + "/loom-environment-retained")).json()
                    if observed.get("status") == {"hard": {"pods": "0"}, "used": {"pods": "0"}}:
                        break
                    await asyncio.sleep(0.2)
            with pytest.raises(ProviderBlockedError, match="kubernetes_resource_identity_conflict"):
                await destroy._close_pod_admission(scoped, scoped, "loom-dev-scoped", require_idle=True)
            ordinary = await http.post("/api/v1/namespaces/loom-dev-scoped/pods", json={
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "uncovered-by-scoped-quota"},
                "spec": {"schedulerName": "not-installed", "containers": [{"name": "test", "image": "test.invalid/never-pulled"}]},
            })
            assert ordinary.status_code == 201, ordinary.text
    finally:
        await asyncio.to_thread(container.stop)
