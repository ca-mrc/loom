"""Real API defaulting/readback for each fixed management manifest phase."""
from __future__ import annotations

import base64
import os
import ssl
import time
from uuid import uuid4

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_all_fixed_management_phases_preserve_real_defaulting_and_uids(tmp_path, management_inputs, installation, checks):
    from scripts.ops.nebius_management_bootstrap import (
        BootstrapBinding,
        HTTPSBootstrapAPI,
        bootstrap_management,
    )
    from scripts.ops.nebius_management_install import render_installation
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_prerequisites import (
        HTTPSManagementPrerequisites,
        ManagementPrerequisiteError,
    )
    from scripts.ops.nebius_management_stage import (
        HTTPSManagementStageAPI,
        ManagementStageError,
        management_phase_ready,
        stage_management_resources,
    )
    from scripts.ops.nebius_management_storage import (
        prepare_management_storage,
        verify_management_storage,
    )

    rendered = render_installation(installation[0])
    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        user = config["users"][0]["user"]
        certificate, key = tmp_path / "client.crt", tmp_path / "client.key"
        certificate.write_bytes(base64.b64decode(user["client-certificate-data"]))
        key.write_bytes(base64.b64decode(user["client-key-data"]))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        check = checks[0]
        with HTTPSManagementPrerequisites(settings=check.settings, ingress=check.ingress,
                certificate_config={}, ingress_state=tmp_path / "no-public-route", operator_cloud_credentials=tmp_path / "no-cloud-key",
                api_server=endpoint, ssl_context=trust) as inventory:
            deadline = time.monotonic() + 20
            while True:
                try:
                    nodes = inventory.inventory("v1", "nodes", "Node")
                except ManagementPrerequisiteError:
                    raw = inventory._request("GET", "/api/v1/nodes?limit=100")
                    # Test-only diagnostics: just type metadata, never a Node or
                    # Pod payload, operator configuration or Secret contents.
                    pytest.fail(str({"type": (raw.get("apiVersion"), raw.get("kind")),
                        "version": raw.get("metadata", {}).get("resourceVersion"),
                        "item_types": [(row.get("apiVersion"), row.get("kind")) for row in raw.get("items", [])]}))
                if nodes:
                    break
                assert time.monotonic() < deadline, "disposable node did not register"
                time.sleep(0.1)
            assert len(nodes) == 1 and nodes[0]["metadata"]["uid"]
            assert inventory.inventory("apps/v1", "deployments", "Deployment") == []
            assert inventory.inventory("batch/v1", "cronjobs", "CronJob") == []
            assert inventory.inventory("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler") == []
        bootstrap = BootstrapBinding(management_inputs[0]["installation_id"],
                                     "loom-nebius-management", core.read_namespace("kube-system").metadata.uid)
        with HTTPSBootstrapAPI(binding=bootstrap, api_server=endpoint, ssl_context=trust) as api:
            receipt = bootstrap_management(binding=bootstrap, api=api, state_dir=tmp_path / "bootstrap")
        binding = ManagementBinding(bootstrap.installation_id, bootstrap.namespace,
                                    receipt["namespace_uid"], bootstrap.kube_system_uid)
        for phase in ("10-config-network.yaml", "20-database.yaml", "30-migrate.yaml",
                      "40-services.yaml", "80-backup.yaml", "70-public.yaml"):
            with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase=phase,
                                         api_server=endpoint, ssl_context=trust) as api:
                arguments = dict(rendered=rendered, phase=phase, binding=binding, api=api, state_dir=tmp_path / phase)
                if phase == "20-database.yaml":
                    prepare_management_storage(rendered=rendered, binding=binding, api=api, state_dir=tmp_path / phase)
                first = stage_management_resources(**arguments)
                assert stage_management_resources(**arguments) == first
                assert all(first["resource_uids"].values())
                if phase in {"20-database.yaml", "30-migrate.yaml", "40-services.yaml"}:
                    # This disposable API has no integration platform node or
                    # runtime images. Creating resources cannot prove readiness.
                    assert management_phase_ready(**arguments) is False
        # The StatefulSet controller creates the retained claim even though this
        # test has no eligible platform node. Bind a test-only static CSI volume:
        # this proves real PVC/PV API ownership/readback, not a database mount.
        deadline = time.monotonic() + 20
        while True:
            try:
                claim = core.read_namespaced_persistent_volume_claim("data-loom-postgres-0", binding.namespace)
                break
            except Exception as exc:
                assert getattr(exc, "status", None) == 404 and time.monotonic() < deadline
                time.sleep(0.1)
        pv_name = "management-evidence-" + uuid4().hex
        volume = core.create_persistent_volume({"apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": pv_name}, "spec": {"capacity": {"storage": "10Gi"}, "accessModes": ["ReadWriteOnce"],
                "storageClassName": claim.spec.storage_class_name, "persistentVolumeReclaimPolicy": "Retain",
                "claimRef": {"name": claim.metadata.name, "namespace": binding.namespace, "uid": claim.metadata.uid},
                "csi": {"driver": "test.csi.example.com", "volumeHandle": "disposable-no-real-disk"}}})
        with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase="20-database.yaml",
                                     api_server=endpoint, ssl_context=trust) as api:
            while True:
                claim = api.get_database_claim()
                pv = api.get_database_volume()
                if claim and pv and claim.get("status", {}).get("phase") == pv.get("status", {}).get("phase") == "Bound":
                    break
                assert time.monotonic() < deadline, "test CSI claim did not bind"
                time.sleep(0.1)
            args = dict(rendered=rendered, binding=binding, api=api, state_dir=tmp_path / "20-database.yaml",
                        evidence_dir=tmp_path / "storage")
            receipt = verify_management_storage(**args)
            assert receipt["pvc_uid"] == claim["metadata"]["uid"]
            assert receipt["pv_uid"] == volume.metadata.uid
            assert verify_management_storage(**args) == receipt
        service = core.read_namespaced_service("loom-postgres", binding.namespace)
        assert service.spec.cluster_ip and service.spec.cluster_ip != "None"
        core.patch_namespace(binding.namespace, {"metadata": {"labels": {
            "pod-security.kubernetes.io/enforce": "privileged",
        }}})
        with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase="10-config-network.yaml",
                                     api_server=endpoint, ssl_context=trust) as api:
            with pytest.raises(ManagementStageError):
                stage_management_resources(rendered=rendered, phase="10-config-network.yaml", binding=binding,
                                           api=api, state_dir=tmp_path / "new-state")
        assert core.read_namespace(binding.namespace).metadata.uid == binding.namespace_uid
    finally:
        container.stop()
