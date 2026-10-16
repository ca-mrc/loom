"""Compose real delivery, staging, controller proof and cutover against a fake wire."""
from __future__ import annotations

import copy
import importlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_image import DIGEST
from tests.ops import test_nebius_ingress_cutover as routing
from tests.ops import test_nebius_ingress_stage as staging
from tests.ops.test_nebius_ingress_gateway import inputs as inputs
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.support.nebius_platform import platform_inputs as platform_inputs

from loom.nebius_environment_contract import FoundationBinding


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_operation")


@pytest.fixture
def installation(inputs, platform_inputs, inventory, tmp_path):
    cert, binding, tls, roots, selected, _root = inputs
    config, _candidate, profile = platform_inputs
    binding = replace(binding, namespace=config["namespace"])
    tls.binding = binding
    inventory["nodes"][0]["status"]["allocatable"].update(cpu="1100m", memory="1152Mi", **{"ephemeral-storage": "2176Mi"})

    class API(routing.API):
        def __init__(self):
            super().__init__()
            self.binding, self.image, self.candidate = binding, "cr.eu-north1.nebius.cloud/test/loom-shared-ingress@" + DIGEST, "a" * 40
            for row in (self.service, self.config):
                row["metadata"]["namespace"] = binding.namespace
            self.config["data"]["environment.json"] = json.dumps(config)
            self.config["data"]["profile.json"] = json.dumps({**profile, "candidate_sha": self.candidate})
            self.stage = staging.API()
            self.rs_uid, self.pod_uid = str(uuid4()), str(uuid4())
            self.legacy_ok = True

        def foundation(self):
            live = json.loads(self.config["data"]["environment.json"])
            return FoundationBinding(platform_config_json=json.dumps({**live, "shared_ingress_enabled": False}, sort_keys=True),
                                     public_dns_zone=binding.child_domain, ingress_class_name="loom-shared",
                                     ingress_namespace=binding.namespace, ingress_controller_label="loom-shared-ingress")

        def capacity(self):
            pods = inventory["pods"] + self.list_controller_pods(binding.namespace)
            return module().qualify_capacity(nodes=inventory["nodes"], pods=pods)

        def staging(self, installation):
            return self.stage

        def verify_identity(self, selected_binding):
            tls.verify_identity(selected_binding)

        def get_secret(self, namespace, name):
            return tls.get_secret(namespace, name)

        def create_secret(self, document):
            tls.create_secret(document)

        def get_deployment(self, namespace, name):
            rows = [copy.deepcopy(d) for d in self.stage.resources.values() if d["kind"] == "Deployment"]
            if not rows:
                return None
            row = rows[0]
            row["metadata"]["generation"] = 1
            row["status"] = {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1}
            return row

        def list_controller_replicasets(self, namespace):
            deployment = self.get_deployment(namespace, "loom-shared-ingress")
            return [] if deployment is None else [{"metadata": {"uid": self.rs_uid, "ownerReferences": [
                {"apiVersion": "apps/v1", "kind": "Deployment", "name": "loom-shared-ingress",
                 "uid": deployment["metadata"]["uid"], "controller": True}]}}]

        def list_controller_pods(self, namespace):
            deployment = self.get_deployment(namespace, "loom-shared-ingress")
            if deployment is None:
                return []
            return [{"metadata": {"name": "ingress-pod", "namespace": binding.namespace, "uid": self.pod_uid,
                                  "resourceVersion": "1", "labels": {"app": "loom-shared-ingress"},
                                  "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "ingress-rs",
                                                       "uid": self.rs_uid, "controller": True}]},
                     "spec": {**deployment["spec"]["template"]["spec"], "nodeName": "computeinstance-test"},
                     "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}]

        def get_pod(self, namespace, name):
            return self.list_controller_pods(namespace)[0]

        def probe_tls(self, namespace, name, uid, server_name):
            return selected["fingerprint_sha256"]

        def probe_legacy_pod(self, pod):
            if not self.legacy_ok:
                raise RuntimeError("private legacy probe failure")

        def probe_public(self, receipt):
            self.public_probe()

        def probe_original_backend(self, origin):
            actual = next(d for d in self.stage.resources.values() if d["kind"] == "Service")
            assert actual["metadata"]["uid"] == origin["uid"]

    from tests.ops.test_nebius_certificates import NOW

    return {"api": API(), "certificate_config": cert, "state_dir": tmp_path / "ingress",
            "now": NOW, "roots": roots}, tls, inventory


def test_connected_install_delivers_tls_stages_and_cuts_over_without_recreating_public_service(installation):
    args, tls, _inventory = installation
    api = args["api"]
    service_uid = api.service["metadata"]["uid"]
    result = module().install_ingress(**args)
    assert result["status"] == "complete" and result["service_uid"] == service_uid
    assert len(api.stage.creates) == 8 and tls.creates == 1
    assert api.service["spec"]["selector"] == {"app": "loom-shared-ingress"}
    assert api.writes == ["acquire", "service", "config", "release"]
    assert module().install_ingress(**args) == result
    assert len(api.stage.creates) == 8 and tls.creates == 1
    assert api.writes == ["acquire", "service", "config", "release"]


def test_capacity_failure_precedes_even_private_secret_delivery(installation):
    args, tls, inventory = installation
    inventory["nodes"][0]["status"]["allocatable"]["cpu"] = "100m"
    with pytest.raises(module().OperationError):
        module().install_ingress(**args)
    assert not tls.creates and not args["api"].stage.creates and not args["api"].writes


def test_legacy_staging_probe_failure_cannot_acquire_guard_or_switch_public_route(installation):
    args, tls, _inventory = installation
    args["api"].legacy_ok = False
    with pytest.raises(module().OperationError):
        module().install_ingress(**args)
    assert tls.creates == 1 and len(args["api"].stage.creates) == 8
    assert args["api"].writes == []
    assert args["api"].service["spec"]["selector"] == {"app": "loom-web"}


def test_interrupted_public_probe_can_resume_same_installation_without_repeating_staging(installation):
    args, tls, _inventory = installation
    api = args["api"]
    api.probe_ok = False
    with pytest.raises(module().OperationError):
        module().install_ingress(**args)
    assert api.owner is not None and api.writes == ["acquire", "service"]
    api.probe_ok = True
    assert module().install_ingress(**args)["status"] == "complete"
    assert api.owner is None and api.writes == ["acquire", "service", "config", "release"]
    assert tls.creates == 1 and len(api.stage.creates) == 8


def test_connected_recovery_needs_neither_new_controller_health_nor_tls_redelivery(installation):
    args, tls, _inventory = installation
    api = args["api"]
    api.probe_ok = False
    with pytest.raises(module().OperationError):
        module().install_ingress(**args)
    api.legacy_ok = False  # The new controller is unhealthy; original backend is not.
    tls.secrets.clear()
    result = module().rollback_ingress(api=api, state_dir=args["state_dir"])
    assert result["status"] == "rolled_back" and api.owner is None
    assert api.service["spec"]["selector"] == {"app": "loom-web"}
    assert tls.creates == 1 and len(api.stage.creates) == 8
    assert api.writes == ["acquire", "service", "restore_service", "release"]


def test_missing_origin_ownership_journal_cannot_authorize_connected_recovery(installation):
    args, _tls, _inventory = installation
    api = args["api"]
    api.probe_ok = False
    with pytest.raises(module().OperationError):
        module().install_ingress(**args)
    (args["state_dir"] / "stage" / (api.binding.installation_id + ".json")).unlink()
    with pytest.raises(module().OperationError):
        module().rollback_ingress(api=api, state_dir=args["state_dir"])
    assert api.owner is not None and api.writes == ["acquire", "service"]
