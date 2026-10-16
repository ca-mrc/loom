"""Initial ingress staging must never adopt, overwrite or retry unknown writes."""
from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_shared_ingress import ingress_input as ingress_input

from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_stage")


def key(document):
    return document["kind"] + ":" + document["metadata"].get("namespace", "-") + ":" + document["metadata"]["name"]


class API:
    def __init__(self):
        self.resources = {}
        self.creates = []
        self.identity_matches = True
        self.failure = None
        self.change = None

    def verify_identity(self, binding):
        if not self.identity_matches:
            raise RuntimeError("private cluster identity mismatch")

    def get_resource(self, desired):
        return copy.deepcopy(self.resources.get(key(desired)))

    def default_resource(self, desired):
        result = copy.deepcopy(desired)
        result["metadata"].update(uid=str(uuid4()), resourceVersion="1", creationTimestamp=None)
        if result["kind"] == "Deployment":
            result["spec"].update(progressDeadlineSeconds=600, revisionHistoryLimit=10)
        if result["kind"] == "Service":
            result["spec"].update(clusterIP="10.0.0.99", clusterIPs=["10.0.0.99"], ipFamilies=["IPv4"])
        result["status"] = {}
        return result

    def create_resource(self, desired):
        self.creates.append(key(desired))
        if self.failure == "before":
            raise TimeoutError("private API error")
        result = self.default_resource(desired)
        if result["kind"] == "Service":
            result["spec"].update(clusterIP="10.0.0.10", clusterIPs=["10.0.0.10"])
        if result["kind"] == "Deployment":
            result["metadata"].setdefault("annotations", {})["deployment.kubernetes.io/revision"] = "1"
        if self.change is not None:
            self.change(result)
        self.resources[key(desired)] = result
        if self.failure == "after":
            raise TimeoutError("private API error")


@pytest.fixture
def staging(ingress_input, tmp_path):
    from scripts.ops.nebius_ingress_gateway import TLSBinding

    installation = SharedIngressInstallation.model_validate(ingress_input)
    binding = TLSBinding(
        installation_id=str(installation.installation_id), certificate_installation_id=str(uuid4()),
        namespace=installation.foundation.ingress_namespace, namespace_uid=str(uuid4()), kube_system_uid=str(uuid4()),
        child_domain=installation.foundation.public_dns_zone, management_host="management.other.test",
    )
    return {"installation": installation, "binding": binding, "api": API(), "state_dir": tmp_path / "stage"}


def test_initial_stage_records_all_resource_uids_and_replays_without_writes(staging):
    implementation, args = module(), staging
    result = implementation.stage_controller(**args)
    assert result["status"] == "controller_staged"
    assert len(result["resource_uids"]) == len(args["api"].creates) == 8
    assert result == implementation.stage_controller(**args)
    assert len(args["api"].creates) == 8
    assert {d["kind"] for d in args["api"].resources.values()} == {
        "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "IngressClass", "ConfigMap", "Service", "NetworkPolicy", "Deployment",
    }
    assert all(d["spec"].get("type", "ClusterIP") == "ClusterIP" for d in args["api"].resources.values() if d["kind"] == "Service")
    assert all(path.stat().st_mode & 0o077 == 0 for path in args["state_dir"].rglob("*.json"))


def test_exact_looking_untracked_object_is_never_adopted(staging):
    implementation, args = module(), staging
    doc = render_shared_ingress(args["installation"])[0]
    args["api"].resources[key(doc)] = args["api"].default_resource(doc)
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)
    assert not args["api"].creates


@pytest.mark.parametrize("failure", ["before", "after"])
def test_stage_resolves_unknown_create_only_by_readback(staging, failure):
    implementation, args = module(), staging
    api = args["api"]
    api.failure = failure
    for _ in range(2):
        if failure == "before":
            with pytest.raises(implementation.StageError) as error:
                implementation.stage_controller(**args)
            assert "private" not in str(error.value)
        else:
            assert implementation.stage_controller(**args)["status"] == "controller_staged"
    assert len(api.creates) == (1 if failure == "before" else 8)


@pytest.mark.parametrize("change", ["uid", "owner", "deleting", "missing", "service-allocation", "extra-rbac", "extra-pod-field"])
def test_stage_replay_blocks_changed_identity_configuration_or_allocation(staging, change):
    implementation, args = module(), staging
    implementation.stage_controller(**args)
    api = args["api"]
    kind = "Service" if change == "service-allocation" else "ClusterRole" if change == "extra-rbac" else "Deployment"
    document = next(d for d in api.resources.values() if d["kind"] == kind)
    if change == "uid":
        document["metadata"]["uid"] = str(uuid4())
    elif change == "owner":
        document["metadata"]["labels"] = {}
    elif change == "deleting":
        document["metadata"]["deletionTimestamp"] = "2026-09-23T20:00:00Z"
    elif change == "missing":
        del api.resources[key(document)]
    elif change == "service-allocation":
        document["spec"].update(clusterIP="10.0.0.11", clusterIPs=["10.0.0.11"])
    elif change == "extra-rbac":
        document["rules"].append({"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]})
    else:
        document["spec"]["template"]["spec"]["hostNetwork"] = True
    before = copy.deepcopy(api.resources)
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)
    assert api.resources == before and len(api.creates) == 8


def test_stage_checks_actual_create_not_just_the_dry_run(staging):
    implementation, args = module(), staging

    def add_privilege(document):
        if document["kind"] == "Deployment":
            document["spec"]["template"]["spec"]["hostNetwork"] = True

    args["api"].change = add_privilege
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)


@pytest.mark.parametrize("change", ["owner", "rbac", "host-network", "pod-security"])
def test_server_defaulting_cannot_change_requested_authority(staging, monkeypatch, change):
    implementation, args = module(), staging
    api = args["api"]
    original = api.default_resource

    def defaults(desired):
        result = original(desired)
        if change == "owner":
            result["metadata"]["labels"]["loom.nebius/ingress-installation-id"] = str(uuid4())
        elif change == "rbac" and result["kind"] == "ClusterRole":
            result["rules"].append({"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]})
        elif result["kind"] == "Deployment":
            pod = result["spec"]["template"]["spec"]
            if change == "host-network":
                pod["hostNetwork"] = True
            elif change == "pod-security":
                pod["containers"][0]["securityContext"]["privileged"] = True
        return result

    monkeypatch.setattr(api, "default_resource", defaults)
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)
    assert not api.creates


def test_stage_keeps_intent_if_namespace_identity_changes_after_create(staging):
    implementation, args = module(), staging
    api = args["api"]
    api.change = lambda document: setattr(api, "identity_matches", False)
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)
    assert len(api.creates) == 1
    documents = [json.loads(path.read_text()) for path in args["state_dir"].glob("*.json")]
    assert documents and all(document["status"] != "controller_staged" for document in documents)


def test_changed_render_cannot_reinterpret_recorded_stage(staging):
    implementation, args = module(), staging
    implementation.stage_controller(**args)
    updated = args["installation"].model_dump(mode="json")
    updated["image"] = updated["image"].replace("d" * 64, "e" * 64)
    args["installation"] = SharedIngressInstallation.model_validate(updated)
    with pytest.raises(implementation.StageError):
        implementation.stage_controller(**args)
    assert len(args["api"].creates) == 8


@pytest.mark.parametrize("change", [None, "verb", "namespace", "name", "operation"])
def test_staging_transport_accepts_only_the_fixed_render_with_operation_identity(staging, tmp_path, monkeypatch, change):
    implementation, args = module(), staging
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("private fixture")
    kubeconfig.chmod(0o600)
    api = implementation.KubectlStageAPI(kubeconfig, binding=args["binding"], executable=Path("/usr/bin/kubectl"),
                                         installation=args["installation"])
    desired = next(d for d in render_shared_ingress(args["installation"]) if d["kind"] == "ClusterRole")
    desired["metadata"]["annotations"] = {"loom.nebius/ingress-stage-id": str(uuid4())}
    if change == "verb":
        desired["rules"][0]["verbs"].append("delete")
    elif change == "namespace":
        desired["metadata"]["namespace"] = "foreign"
    elif change == "name":
        desired["metadata"]["name"] = "foreign"
    elif change == "operation":
        desired["metadata"]["annotations"]["loom.nebius/ingress-stage-id"] = "not-an-identity"
    commands = []
    monkeypatch.setattr(api, "verify_identity", lambda binding: None)

    def run(argv, *, payload=None):
        commands.append((argv, payload))
        return json.dumps(desired).encode() if "--dry-run=server" in argv else b"created"

    monkeypatch.setattr(api, "_run", run)
    if change is None:
        assert api.default_resource(desired) == desired
        api.create_resource(desired)
        assert [command for command, _ in commands] == [
            ["create", "--dry-run=server", "-f", "-", "-o", "json"],
            ["create", "-f", "-", "-o", "name"],
        ]
        assert all(json.loads(payload) == desired for _, payload in commands)
    else:
        for action in (api.default_resource, api.create_resource, api.get_resource):
            with pytest.raises(implementation.StageError):
                action(desired)
        assert not commands
