"""Real API defaulting/replay for initial ingress; no live route activation."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_stage import KubectlStageAPI, StageError, stage_controller

from loom.nebius_shared_ingress import SharedIngressInstallation
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_shared_ingress import ingress_input as ingress_input

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires an explicitly disposable Kubernetes API")


@pytest.mark.timeout(180)
def test_initial_stage_qualifies_real_defaults_and_preserves_public_service(ingress_input, tmp_path):
    from kubernetes import client

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        installation = SharedIngressInstallation.model_validate(ingress_input)
        namespace = installation.foundation.ingress_namespace
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        public = core.create_namespaced_service(namespace, {
            "apiVersion": "v1", "kind": "Service", "metadata": {"name": "loom-public"},
            "spec": {"type": "NodePort", "selector": {"app": "loom-web"},
                     "ports": [{"port": 443, "targetPort": 8443}]},
        })
        binding = TLSBinding(
            installation_id=str(installation.installation_id), certificate_installation_id=str(uuid4()),
            namespace=namespace, namespace_uid=ns.metadata.uid,
            kube_system_uid=core.read_namespace("kube-system").metadata.uid,
            child_domain=installation.foundation.public_dns_zone, management_host="management.other.test",
        )
        raw = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert raw.exit_code == 0
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(raw.output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}"))
        kubeconfig.chmod(0o600)
        executable = shutil.which("kubectl")
        assert executable is not None
        api = KubectlStageAPI(kubeconfig, binding=binding, executable=Path(executable), installation=installation)
        args = {"binding": binding, "api": api, "state_dir": tmp_path / "stage"}
        receipt = stage_controller(installation, **args)
        assert receipt["status"] == "controller_staged" and len(receipt["resource_uids"]) == 8
        assert stage_controller(installation, **args) == receipt
        after = core.read_namespaced_service("loom-public", namespace)
        assert after.metadata.uid == public.metadata.uid and after.spec.to_dict() == public.spec.to_dict()
        origin = core.read_namespaced_service("loom-web-origin", namespace)
        assert origin.spec.type == "ClusterIP" and origin.spec.cluster_ip
        # Do not schedule any workload: the fixture node deliberately lacks the
        # protected system-node selector. This test is resource staging only.
        journal = json.loads(next((tmp_path / "stage").glob("*.json")).read_text())
        assert journal["resources"][f"Service:{namespace}:loom-web-origin"]["observed"]["spec"]["clusterIP"] == origin.spec.cluster_ip
        client.AppsV1Api().patch_namespaced_deployment("loom-shared-ingress", namespace, {
            "spec": {"template": {"spec": {"hostNetwork": True}}},
        })
        with pytest.raises(StageError, match="differs"):
            stage_controller(installation, **args)
        assert core.read_namespaced_service("loom-web-origin", namespace).metadata.uid == origin.metadata.uid
    finally:
        container.stop()
