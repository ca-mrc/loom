"""Qualify private TLS publication against an explicitly disposable API."""
from __future__ import annotations

import http.client
import json
import os
import shutil
import socket
import ssl
import subprocess
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from scripts.ops.nebius_certificates import _bind_installation
from scripts.ops.nebius_ingress_gateway import (
    IngressError,
    KubectlControllerAPI,
    TLSBinding,
    deliver_tls,
    qualify_controller,
    switch_controller_certificate,
)

from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress
from tests.cluster.test_nebius_shared_ingress import (
    PYTHON,
    SERVER,
    TRAEFIK,
    _backend,
    _certificate,
    _run,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s, _wait_for_pod
from tests.ops import test_nebius_certificates as certificate_material
from tests.ops.test_nebius_certificates import installation, material, publish
from tests.ops.test_nebius_ingress_gateway import inputs as inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_shared_ingress import ingress_input as ingress_input

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires an explicitly disposable Kubernetes API")


@pytest.fixture(autouse=True)
def live_certificate_clock(monkeypatch):
    # OpenSSL observes real time; do not let a frozen unit-test certificate
    # expire and break this long-lived transport/rotation qualification.
    monkeypatch.setattr(certificate_material, "NOW", datetime.now(UTC))


@pytest.mark.timeout(150)
def test_immutable_tls_delivery_replays_and_rotates_without_replacing_old_secret(tmp_path, monkeypatch):
    from kubernetes import client

    working = tmp_path / "working"
    working.mkdir(mode=0o700)
    monkeypatch.chdir(working)
    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        namespace = "loom-tls-" + uuid4().hex[:8]
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        config = json.loads(installation(tmp_path).read_text())
        root = Path(config["state_dir"])
        chain, key, roots = material()
        publish(root, chain, key, roots)
        _bind_installation(root, config)
        binding = TLSBinding(
            installation_id=str(uuid4()), certificate_installation_id=config["installation_id"],
            namespace=namespace, namespace_uid=ns.metadata.uid,
            kube_system_uid=core.read_namespace("kube-system").metadata.uid,
            child_domain=config["child_domain"], management_host=config["management_host"],
        )
        # Only execution is adapted for the container's private kubectl; real
        # production namespace binding, JSON readback and Secret creation run.
        class ContainerAPI(KubectlControllerAPI):
            def _run(self, arguments, *, payload=None):
                result = subprocess.run(
                    ["docker", "exec", "-i", container.get_wrapped_container().id,
                     "kubectl", "--request-timeout=30s", *arguments],
                    input=payload, capture_output=True, timeout=40, check=False,
                )
                if result.returncode:
                    raise IngressError("disposable Kubernetes operation failed")
                return result.stdout

        # Adapter validates private input ownership; container uses its own config.
        kubeconfig = tmp_path / "private-kubeconfig"
        kubeconfig.write_bytes(b"disposable container owns actual configuration")
        kubeconfig.chmod(0o600)
        api = ContainerAPI(kubeconfig, binding=binding, executable=Path("/bin/kubectl"))
        first = deliver_tls(config, binding=binding, api=api, roots=roots, now=certificate_material.NOW)
        assert deliver_tls(config, binding=binding, api=api, roots=roots, now=certificate_material.NOW) == first
        old = core.read_namespaced_secret(first["secret_name"], namespace)
        assert old.immutable and old.metadata.uid == first["secret_uid"]
        core.create_namespaced_service_account(namespace, client.V1ServiceAccount(
            metadata=client.V1ObjectMeta(name="tls-fixture"), automount_service_account_token=False))
        server = '''import http.server, ssl
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
server = http.server.HTTPServer(("0.0.0.0", 8443), http.server.BaseHTTPRequestHandler)
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
'''
        core.create_namespaced_pod(namespace, client.V1Pod(
            metadata=client.V1ObjectMeta(name="ingress"),
            spec=client.V1PodSpec(
                restart_policy="Never", service_account_name="tls-fixture", automount_service_account_token=False,
                volumes=[client.V1Volume(name="tls", secret=client.V1SecretVolumeSource(secret_name=first["secret_name"]))],
                containers=[client.V1Container(
                    name="tls", image=PYTHON, command=["python", "-c", server],
                    volume_mounts=[client.V1VolumeMount(name="tls", mount_path="/tls", read_only=True)],
                    readiness_probe=client.V1Probe(tcp_socket=client.V1TCPSocketAction(port=8443), period_seconds=1),
                )],
            ),
        ))
        ready = _wait_for_pod(core, namespace, "ingress")
        raw = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert raw.exit_code == 0
        kubeconfig.write_text(raw.output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}"))
        executable = shutil.which("kubectl")
        assert executable is not None, "disposable TLS transport test requires kubectl"
        probe = KubectlControllerAPI(kubeconfig, binding=binding, executable=Path(executable))
        context = ssl.create_default_context(cadata=roots[0].public_bytes(serialization.Encoding.PEM).decode())
        with monkeypatch.context() as trusted:
            trusted.setattr(ssl, "create_default_context", lambda: context)
            assert probe.probe_tls(namespace, "ingress", ready.metadata.uid, binding.management_host) == first["fingerprint_sha256"]
            with pytest.raises(IngressError, match="identity differs"):
                probe.probe_tls(namespace, "ingress", str(uuid4()), binding.management_host)
        with pytest.raises(client.ApiException) as error:
            core.patch_namespaced_secret(first["secret_name"], namespace, {"data": {"tls.key": "Zm9yZWlnbg=="}})
        assert error.value.status == 422
        new_chain, new_key, new_roots = material()
        publish(root, new_chain, new_key, new_roots)
        second = deliver_tls(config, binding=binding, api=api, roots=new_roots, now=certificate_material.NOW)
        assert second["secret_uid"] != first["secret_uid"]
        assert core.read_namespaced_secret(first["secret_name"], namespace).data == old.data
        assert len(core.list_namespaced_secret(namespace).items) == 2
        apps = client.AppsV1Api()
        apps.create_namespaced_deployment(namespace, {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "loom-shared-ingress", "namespace": namespace,
                         "labels": {"loom.nebius/ingress-installation-id": binding.installation_id}},
            "spec": {"replicas": 0, "selector": {"matchLabels": {"app": "loom-shared-ingress"}},
                     "template": {"metadata": {"labels": {"app": "loom-shared-ingress"}}, "spec": {
                         "containers": [{"name": "controller", "image": PYTHON}],
                         "volumes": [{"name": "tls", "secret": {"secretName": first["secret_name"]}}],
                     }}},
        })
        observed = api.get_deployment(namespace, "loom-shared-ingress")
        wrong_uid = {**observed, "metadata": {**observed["metadata"], "uid": str(uuid4())}}
        with pytest.raises(IngressError):
            api.switch_controller_tls(wrong_uid, second["secret_name"])
        apps.patch_namespaced_deployment("loom-shared-ingress", namespace,
                                        {"metadata": {"annotations": {"concurrent-change": "retained"}}})
        with pytest.raises(IngressError):
            api.switch_controller_tls(observed, second["secret_name"])
        current = api.get_deployment(namespace, "loom-shared-ingress")
        assert current["spec"]["template"]["spec"]["volumes"][0]["secret"]["secretName"] == first["secret_name"]
        api.switch_controller_tls(current, second["secret_name"])
        switched = api.get_deployment(namespace, "loom-shared-ingress")
        assert switched["metadata"]["uid"] == current["metadata"]["uid"]
        assert switched["metadata"]["generation"] == current["metadata"]["generation"] + 1
        assert switched["metadata"]["annotations"]["concurrent-change"] == "retained"
        assert switched["spec"]["template"]["spec"]["volumes"][0]["secret"]["secretName"] == second["secret_name"]
        assert switched["spec"]["template"]["spec"]["containers"] == current["spec"]["template"]["spec"]["containers"]
        core.delete_namespaced_secret(second["secret_name"], namespace)
        with pytest.raises(IngressError, match="unresolved"):
            deliver_tls(config, binding=binding, api=api, roots=new_roots, now=certificate_material.NOW)
        assert len(core.list_namespaced_secret(namespace).items) == 1
        assert not (working / ".kube").exists()
        assert (tmp_path / ".loom-ingress-kubectl-cache").stat().st_mode & 0o077 == 0
    finally:
        container.stop()


@pytest.mark.timeout(240)
def test_real_traefik_rotation_qualifies_fresh_pods_and_preserves_previous_secret(inputs, ingress_input, tmp_path, monkeypatch):
    """Local image alias is not live registry mirroring or public cutover evidence."""
    config, fixture_binding, _fake, roots, _selected, root = inputs
    namespace = ingress_input["foundation"]["ingress_namespace"]
    container = _start_k3s(node_name="loom-tls-rotation", ephemeral_storage_floor="2Gi")
    try:
        from kubernetes import client
        _, core, _ = _load_client(container)
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        binding = replace(fixture_binding, namespace=namespace, namespace_uid=ns.metadata.uid,
                          kube_system_uid=core.read_namespace("kube-system").metadata.uid)
        ident = container.get_wrapped_container().id
        # Docker's classic archive can rebuild the manifest with a different
        # digest. Pull the pinned manifest in containerd itself; this preserves
        # the qualified registry identity on both local and CI Docker backends.
        _run(container, "ctr", "images", "pull", TRAEFIK)
        _run(container, "ctr", "images", "pull", PYTHON)
        image = "cr.eu-north1.nebius.cloud/test/traefik@" + TRAEFIK.split("@", 1)[1]
        _run(container, "ctr", "images", "tag", TRAEFIK, image)
        assert image in _run(container, "ctr", "images", "ls", "-q").splitlines()
        _run(container, "kubectl", "wait", "--for=create", "node/loom-tls-rotation", "--timeout=60s")
        _run(container, "kubectl", "label", "nodes", "--all", "loom.nebius/node-role=system", "loom.nebius/platform=integration")
        kubeconfig = tmp_path / "rotation-kubeconfig"
        raw = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert raw.exit_code == 0
        kubeconfig.write_text(raw.output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}"))
        kubeconfig.chmod(0o600)
        executable = shutil.which("kubectl")
        assert executable is not None
        api = KubectlControllerAPI(kubeconfig, binding=binding, executable=Path(executable))
        first = deliver_tls(config, binding=binding, api=api, roots=roots, now=certificate_material.NOW)
        ingress_input.update(installation_id=binding.installation_id, tls_secret_name=first["secret_name"], image=image)
        ingress_input["foundation"]["public_dns_zone"] = binding.child_domain
        installed = SharedIngressInstallation.model_validate(ingress_input)
        docs = render_shared_ingress(installed)
        old_host = installed.foundation.platform_config["public_host"]
        old_tls = _certificate([old_host])
        docs += [
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "default", "namespace": namespace}},
            {"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls", "stringData": old_tls,
             "metadata": {"name": "legacy-tls", "namespace": namespace}},
            {"apiVersion": "v1", "kind": "ConfigMap", "data": {"server.py": SERVER},
             "metadata": {"name": "backend-code", "namespace": namespace}},
            _backend(namespace, "loom-web", 8443, tls=True),
            {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "public", "namespace": namespace},
             "spec": {"type": "NodePort", "selector": {"app": "loom-shared-ingress"},
                      "ports": [{"port": 443, "targetPort": 8443, "nodePort": 30443}]}},
        ]
        _run(container, "kubectl", "apply", "-f", "-", payload=yaml.safe_dump_all(docs))
        _run(container, "kubectl", "rollout", "status", "deployment/loom-shared-ingress", "-n", namespace,
             "--timeout=90s", timeout=100)
        deployment = api.get_deployment(namespace, "loom-shared-ingress")
        _run(container, "kubectl", "rollout", "status", "deployment/coredns", "-n", "kube-system", "--timeout=60s", timeout=70)
        _run(container, "kubectl", "wait", "pod/loom-web", "-n", namespace, "--for=condition=Ready", "--timeout=60s", timeout=70)
        networks = json.loads(subprocess.check_output(["docker", "inspect", ident]))[0]["NetworkSettings"]["Networks"]
        addresses = [value["IPAddress"] for value in networks.values() if value.get("IPAddress")]
        assert len(addresses) == 1
        legacy_secret = core.read_namespaced_secret("legacy-tls", namespace)
        public = core.read_namespaced_service("public", namespace)

        def legacy_proof():
            trust = ssl.create_default_context(cadata=old_tls["tls.crt"])
            deadline = time.monotonic() + 30
            while True:
                try:
                    with socket.create_connection((addresses[0], 30443), timeout=5) as connection:
                        with trust.wrap_socket(connection, server_hostname=old_host) as secured:
                            secured.sendall(f"GET / HTTP/1.1\r\nHost: {old_host}\r\nConnection: close\r\n\r\n".encode())
                            response = http.client.HTTPResponse(secured)
                            response.begin()
                            assert (response.status, response.read()) == (200, (namespace + ":8443").encode())
                    break
                except (OSError, AssertionError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.5)
            trust.set_alpn_protocols(["acme-tls/1"])
            with socket.create_connection((addresses[0], 30443), timeout=5) as connection:
                with trust.wrap_socket(connection, server_hostname=old_host) as secured:
                    assert secured.selected_alpn_protocol() == "acme-tls/1"

        def qualified(receipt, trusted_roots):
            trust = ssl.create_default_context(cadata=trusted_roots[0].public_bytes(serialization.Encoding.PEM).decode())
            deadline = time.monotonic() + 30
            with monkeypatch.context() as local_trust:
                local_trust.setattr(ssl, "create_default_context", lambda: trust)
                while True:
                    try:
                        return qualify_controller(binding=binding, api=api, deployment_uid=deployment["metadata"]["uid"],
                                                  image=image, tls_receipt=receipt)
                    except IngressError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.5)

        first_proof = qualified(first, roots)
        legacy_proof()
        old_secret = core.read_namespaced_secret(first["secret_name"], namespace)
        chain, key, next_roots = material()
        publish(root, chain, key, next_roots)
        second = deliver_tls(config, binding=binding, api=api, roots=next_roots, now=certificate_material.NOW)
        switch = switch_controller_certificate(config, binding=binding, api=api, tls_receipt=second,
            deployment_uid=deployment["metadata"]["uid"], roots=next_roots, now=certificate_material.NOW)
        assert switch_controller_certificate(config, binding=binding, api=api, tls_receipt=second,
            deployment_uid=deployment["metadata"]["uid"], roots=next_roots, now=certificate_material.NOW) == switch
        _run(container, "kubectl", "rollout", "status", "deployment/loom-shared-ingress", "-n", namespace,
             "--timeout=90s", timeout=100)
        second_proof = qualified(second, next_roots)
        legacy_proof()
        assert first_proof["deployment_uid"] == second_proof["deployment_uid"]
        assert first_proof["pod_uids"] != second_proof["pod_uids"]
        assert first_proof["fingerprint_sha256"] != second_proof["fingerprint_sha256"]
        assert second_proof["fingerprint_sha256"] == second["fingerprint_sha256"]
        retained = core.read_namespaced_secret(first["secret_name"], namespace)
        assert retained.metadata.uid == old_secret.metadata.uid and retained.data == old_secret.data
        retained_legacy = core.read_namespaced_secret("legacy-tls", namespace)
        assert retained_legacy.metadata.uid == legacy_secret.metadata.uid and retained_legacy.data == legacy_secret.data
        retained_public = core.read_namespaced_service("public", namespace)
        assert retained_public.metadata.uid == public.metadata.uid and retained_public.spec == public.spec
    finally:
        container.stop()
