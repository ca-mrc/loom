"""Actual CNI enforcement of personal API egress and shared-side admission."""
from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from tests.cluster.test_nebius_retirement_startup_probe import _native_dns_labels
from tests.cluster.test_nebius_shared_ingress import _add_failure_diagnostics, _run
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _pod_probe,
    _start_k3s,
    _wait_for_allowed_peer,
    _wait_for_pod,
    _wait_for_policy_programming,
)
from tests.unit.test_nebius_application_authority import authority_for
from tests.unit.test_nebius_application_render import inputs, named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.parametrize("dns_label", ["kube-dns", "coredns"])
@pytest.mark.timeout(300)
async def test_shared_data_accepts_managed_apis_but_not_other_namespaces_or_web(platform_inputs, dns_label):
    from kubernetes import client, utils

    from loom.nebius_application_network import render_application_shared_access
    from loom.nebius_application_render import render_application

    values = inputs(platform_inputs)
    shared, foundation = values[2:]
    authority = authority_for(shared)
    policies = render_application_shared_access(authority, shared, foundation)
    fixture_tag = "docker.io/library/loom-personal-network:" + uuid4().hex
    container = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        await asyncio.to_thread(_build_image, tag=fixture_tag,
            dockerfile="tests/fixtures/execution_runtime_fixture/Dockerfile", platform=platform)
        with tempfile.TemporaryDirectory(prefix="loom-personal-network-") as temporary:
            # Testcontainers sees the host's multi-TB filesystem; a percentage
            # floor can evict tiny fixture Pods with hundreds of GiB still free.
            container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
            _, core, _ = await asyncio.to_thread(_load_client, container)
            api = client.ApiClient()
            if dns_label == "coredns":
                await asyncio.to_thread(_run, container, "kubectl", "wait", "--for=create", "deployment/coredns",
                                        "-n", "kube-system", "--timeout=60s")
                await asyncio.to_thread(_native_dns_labels, container, core)
            image = await asyncio.to_thread(_import_image, container, tag=fixture_tag, root=Path(temporary), ordinal=1)
            data_ns = shared.platform_namespace
            await asyncio.to_thread(core.create_namespace, {"metadata": {"name": data_ns}})
            await asyncio.to_thread(core.create_namespaced_service_account, data_ns,
                {"metadata": {"name": "network-fixture"}, "automountServiceAccountToken": False})
            policy_names = {doc["spec"]["podSelector"]["matchLabels"]["app"]: doc["metadata"]["name"] for doc in policies}
            for doc in [{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                         "metadata": {"name": "shared-deny", "namespace": data_ns},
                         "spec": {"podSelector": {}, "policyTypes": ["Ingress"], "ingress": []}}, *policies]:
                await asyncio.to_thread(utils.create_from_dict, api, doc)

            def pod(name, namespace, app, command):
                return {"apiVersion": "v1", "kind": "Pod", "metadata": {
                    "name": name, "namespace": namespace, "labels": {"app": app}}, "spec": {
                        "restartPolicy": "Never", "automountServiceAccountToken": False,
                        "serviceAccountName": "network-fixture",
                        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000,
                                            "seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [{"name": "fixture", "image": image, "imagePullPolicy": "IfNotPresent",
                            "command": ["/fixture", *command], "securityContext": {
                                "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
                                "readOnlyRootFilesystem": True}}],
                    }}

            targets = (("postgres", "loom-postgres", 5432), ("control", "loom-control-plane", 8080),
                       ("gateway", "loom-llm-gateway", 9100), ("unrelated", "other-service", 8090))
            servers = {}
            for name, app, port in targets:
                await asyncio.to_thread(core.create_namespaced_pod, data_ns, pod(name, data_ns, app, ["server", str(port)]))
                servers[name] = await asyncio.to_thread(_wait_for_pod, core, data_ns, name)
                await asyncio.to_thread(core.create_namespaced_service, data_ns, {
                    "metadata": {"name": app}, "spec": {"selector": {"app": app},
                        "ports": [{"port": port, "targetPort": port, "protocol": "TCP"}]},
                })

            # Policies already exist before these independent developers arrive.
            clients = {}
            client_policies = {}
            for slug, change in (("alice", None), ("bob", None), ("eve", None),
                                 ("foreign", "installation"), ("other-data", "data"), ("unlabeled", "none")):
                app_values = inputs(platform_inputs, slug)
                rendered = render_application(*app_values, authority=authority)
                namespace = named(rendered, "Namespace", "loom-dev-" + slug)
                labels = namespace["metadata"]["labels"]
                if change == "installation":
                    labels["loom.nebius/application-installation"] = str(uuid4())
                elif change == "data":
                    labels["loom.nebius/data-environment-id"] = str(uuid4())
                elif change == "none":
                    namespace["metadata"]["labels"] = {}
                ns = namespace["metadata"]["name"]
                await asyncio.to_thread(core.create_namespace, namespace)
                await asyncio.to_thread(core.create_namespaced_service_account, ns,
                    {"metadata": {"name": "network-fixture"}, "automountServiceAccountToken": False})
                for doc in rendered.files["10-network.yaml"]:
                    if doc["kind"] == "NetworkPolicy":
                        await asyncio.to_thread(utils.create_from_dict, api, doc)
                await asyncio.to_thread(core.create_namespaced_pod, ns, pod("api", ns, "loom-service", ["idle"]))
                api_pod = await asyncio.to_thread(_wait_for_pod, core, ns, "api")
                client_policies[slug] = (api_pod.status.pod_ip, ("default-deny", "public-api", "application-egress"))
                clients[slug] = (ns, "api")
            await asyncio.to_thread(core.create_namespaced_pod, "loom-dev-alice", pod("web", "loom-dev-alice", "loom-web", ["idle"]))
            web_pod = await asyncio.to_thread(_wait_for_pod, core, "loom-dev-alice", "web")
            client_policies["web"] = (web_pod.status.pod_ip, ("default-deny", "public-web"))
            clients["web"] = ("loom-dev-alice", "web")

            await _wait_for_policy_programming(container, {
                name: (servers[name].status.pod_ip, ("shared-deny", policy_names[app]) if app in policy_names else ("shared-deny",))
                for name, app, _ in targets
            })
            await _wait_for_policy_programming(container, client_policies)
            for name, app, port in targets:
                # A blocked connection only proves isolation if its server works.
                await _wait_for_allowed_peer(core, data_ns, name, f"http://127.0.0.1:{port}")
                url = f"http://{servers[name].status.pod_ip}:{port}"
                service_url = f"http://{app}.{data_ns}.svc.cluster.local:{port}"
                # An unrestricted shared-side Pod proves DNS/endpoints work
                # independently of the personal API's egress policy.
                await _wait_for_allowed_peer(core, data_ns, name, service_url)
                for slug, (ns, client_name) in clients.items():
                    if slug in {"alice", "bob", "eve"} and name != "unrelated":
                        await _wait_for_allowed_peer(core, ns, client_name, url)
                        await _wait_for_allowed_peer(core, ns, client_name, service_url)
                    else:
                        denied = await asyncio.to_thread(_pod_probe, core, ns, client_name, url)
                        assert ("exit:1 reason:network " in denied or "exit:1 reason:timeout " in denied), (
                            f"expected network denial: {slug} -> {name}: {denied}")
    except (Exception, pytest.fail.Exception) as exc:
        if container is not None:
            # Preserve sandbox/CNI/image-startup evidence before disposable
            # teardown. This adds no retries and never reads Secret contents.
            await asyncio.to_thread(_add_failure_diagnostics, container, shared.platform_namespace, exc)
        raise
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
        await asyncio.to_thread(subprocess.run, ["docker", "image", "rm", "--force", fixture_tag],
                                capture_output=True, check=False)
