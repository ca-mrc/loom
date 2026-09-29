"""Retirement entry binds the completed upgrade and never resumes provisioning."""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_gateway import retirement_operation
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade import UpgradeAPI
from tests.ops.test_nebius_management_upgrade_entry import private_upgrade as private_upgrade
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def private_retirement(private_upgrade, retirement_request, tmp_path):
    from scripts.ops.nebius_management_entry import load_upgrade_inputs
    from scripts.ops.nebius_management_upgrade import upgrade_management

    upgrade_meta, _, _, installed = private_upgrade
    _, upgrade, _, _ = load_upgrade_inputs(upgrade_meta)
    api = UpgradeAPI(installed, upgrade.setup)
    api.authority, api.public_ready, api.switch.processes = True, True, False
    for _ in range(8):
        result = upgrade_management(request=upgrade, api=api, state_dir=Path(upgrade_meta["state_dir"]),
            anchor_dir=Path(upgrade_meta["anchor_dir"]))
        if result["status"] == "management_upgraded":
            break
        api.complete("loom")
    assert result["status"] == "management_upgraded"
    root = tmp_path / "nebius-management/retirement"
    root.mkdir(mode=0o700)
    metadata = retirement_operation(tmp_path) | {key: upgrade_meta[key] for key in ("installation_id", "namespace", "candidate")}
    metadata["source_sha"] = metadata["candidate"]
    request = replace(retirement_request[0], binding=upgrade.setup.binding, deployment=upgrade.setup.deployment,
        candidate=upgrade.setup.candidate, profile=upgrade.setup.profile)
    # The private-entry fixture qualifies a different concrete cluster from the
    # generic rendering fixture; bind this legacy environment to that cluster.
    target, = request.targets
    target = type(target).model_validate(target.model_dump(mode="json") | {"registration": {
        **target.registration.model_dump(mode="json"), "cluster_id": request.deployment.installation.foundation.platform_config["cluster_id"],
    }})
    request = replace(request, targets=(target,))
    state = Path(upgrade_meta["state_dir"])
    payload = {"schema_version": "loom.nebius-management-retirement-private-inputs.v1",
        "upgrade_operation": upgrade_meta, "upgrade_state_sha256": hashlib.sha256((state / "upgrade.json").read_bytes()).hexdigest(),
        "switch_sha256": hashlib.sha256((state / "switch/switch.json").read_bytes()).hexdigest(),
        "deployment": request.deployment.model_dump(mode="json"), "candidate": request.candidate, "profile": request.profile,
        "candidate_id": str(uuid4()), "targets": [target.model_dump(mode="json") for target in request.targets]}
    inputs = Path(metadata["inputs_path"])
    inputs.write_text(json.dumps(payload))
    inputs.chmod(0o600)
    metadata["inputs_sha256"] = hashlib.sha256(inputs.read_bytes()).hexdigest()
    path = root / "operation.json"
    path.write_text(json.dumps(metadata))
    path.chmod(0o600)
    return metadata, payload, path, api


def test_retirement_loader_qualifies_retained_upgrade_without_rewriting_it(private_retirement):
    from scripts.ops.nebius_management_retirement_entry import load_retirement_inputs

    metadata, payload, _, _ = private_retirement
    original = Path(payload["upgrade_operation"]["state_dir"])
    before = {path: path.read_bytes() for path in original.rglob("*.json")}
    context = load_retirement_inputs(metadata)
    assert context.request.deployment.installation.provider_runtime is None
    assert len(context.request.targets) == 1
    assert context.active_management["spec"]["template"]["spec"]["serviceAccountName"] == "loom-application-provisioner"
    assert not Path(metadata["state_dir"]).exists()
    assert all(path.read_bytes() == data for path, data in before.items())


def test_protected_entry_runs_only_fixed_retirement_and_reports_completed_job(private_retirement, monkeypatch, capsys):
    from contextlib import contextmanager, nullcontext

    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_retirement_entry as retirement
    from tests.ops.test_nebius_management_stage import PhaseAPI

    metadata, _, path, _ = private_retirement
    context = retirement.load_retirement_inputs(metadata)
    api = PhaseAPI(context.request.binding)
    api.key = lambda doc: doc["kind"] + ":" + doc["metadata"].get("namespace", "-") + ":" + doc["metadata"]["name"]
    @contextmanager
    def connected(selected):
        assert selected.request == context.request
        yield lambda phase: nullcontext(api)
    monkeypatch.setattr(retirement, "connected_retirement", connected)
    monkeypatch.setattr(entry, "connected_api", lambda *args: pytest.fail("legacy bootstrap selected"))
    monkeypatch.setattr(entry, "connected_upgrade_api", lambda *args: pytest.fail("upgrade replay selected"))
    assert entry.main(str(path), "preflight") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "preflight_qualified"
    assert not api.creates
    assert entry.main(str(path), "install") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "pending"
    job, = [doc for doc in api.resources.values() if doc["kind"] == "Job"]
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    assert entry.main(str(path), "install") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "management_retired"


@pytest.mark.parametrize("damage", [None, "namespace", "legacy_pod", "manager", "fence",
    "fence_uid", "fence_condition", "fence_selector", "binding_selector"])
def test_live_adapter_reads_fences_before_any_mutation(private_retirement, damage):
    import copy
    import ssl

    import httpx
    from scripts.ops.nebius_management_retirement_entry import (
        HTTPSRetirementStageAPI,
        load_retirement_inputs,
    )

    metadata, payload, _, _ = private_retirement
    context = load_retirement_inputs(metadata)
    binding = context.request.binding
    api = HTTPSRetirementStageAPI(context=context, phase="permissions", ssl_context=ssl.create_default_context(), token="fixture-token")
    rows = {
        "/api/v1/namespaces/kube-system": {"kind": "Namespace", "metadata": {"name": "kube-system", "uid": binding.kube_system_uid}},
        "/api/v1/namespaces/" + binding.namespace: {"kind": "Namespace", "metadata": {
            "name": binding.namespace, "uid": binding.namespace_uid, "labels": {
                "loom.nebius/management-installation": binding.installation_id, "pod-security.kubernetes.io/enforce": "restricted"}}},
        "/apis/apps/v1/namespaces/" + binding.namespace + "/deployments/loom-service": copy.deepcopy(context.active_management),
        "/api/v1/namespaces/" + binding.namespace + "/pods": {"items": [], "metadata": {}},
    }
    journal = json.loads(Path(payload["upgrade_operation"]["state_dir"], "retirement/stage.json").read_bytes())
    for item in journal["resources"].values():
        doc = copy.deepcopy(item["observed"])
        plural = "validatingadmissionpolicies" if doc["kind"] == "ValidatingAdmissionPolicy" else "validatingadmissionpolicybindings"
        doc["metadata"]["uid"] = item["uid"]
        doc["metadata"]["generation"] = 1
        doc["status"] = {"observedGeneration": 1, "typeChecking": {}}
        rows["/apis/admissionregistration.k8s.io/v1/" + plural + "/" + doc["metadata"]["name"]] = doc
    for target in context.request.targets:
        for name, uid in target.namespace_uids.items():
            rows["/api/v1/namespaces/" + name] = {"kind": "Namespace", "metadata": {"name": name, "uid": str(uid), "labels": {
                "loom.nebius/environment-id": str(target.registration.environment_id), "loom.nebius/incarnation": str(target.registration.incarnation)}}}
    if damage == "namespace":
        rows["/api/v1/namespaces/" + next(iter(api.namespaces))]["metadata"]["uid"] = str(uuid4())
    elif damage == "legacy_pod":
        rows["/api/v1/namespaces/" + binding.namespace + "/pods"]["items"].append({"spec": {"serviceAccountName": "loom-management-provisioner"}})
    elif damage == "manager":
        rows["/apis/apps/v1/namespaces/" + binding.namespace + "/deployments/loom-service"]["metadata"]["uid"] = str(uuid4())
    elif damage == "fence":
        rows.pop(next(path for path in rows if "/validatingadmissionpolicies/" in path))
    elif damage in {"fence_uid", "fence_condition", "fence_selector"}:
        policy = next(row for path, row in rows.items() if "/validatingadmissionpolicies/" in path)
        if damage == "fence_uid":
            policy["metadata"]["uid"] = str(uuid4())
        elif damage == "fence_condition":
            policy["spec"]["matchConditions"] = [{"name": "skip-all", "expression": "false"}]
        else:
            policy["spec"]["matchConstraints"]["namespaceSelector"] = {"matchLabels": {"bypass": "true"}}
    elif damage == "binding_selector":
        policy_binding = next(row for path, row in rows.items() if "/validatingadmissionpolicybindings/" in path)
        policy_binding["spec"]["matchResources"] = {"namespaceSelector": {"matchLabels": {"bypass": "true"}}}
    def transport(request):
        assert request.method == "GET", "unqualified preflight mutated Kubernetes"
        return httpx.Response(200, json=rows[request.url.path]) if request.url.path in rows else httpx.Response(404)
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(transport))
    def check():
        api.verify_identity(binding)
        api.verify_namespaces()
    with api:
        if damage is None:
            check()
        else:
            with pytest.raises(RuntimeError):
                check()


@pytest.mark.parametrize("damage", ["hash", "unfinished", "switch", "deployment", "candidate", "upgrade_path"])
def test_bad_retirement_input_cannot_reach_connection(private_retirement, monkeypatch, capsys, damage):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_retirement_entry as retirement

    metadata, payload, path, _ = private_retirement
    if damage == "hash":
        payload["upgrade_state_sha256"] = "0" * 64
    elif damage == "unfinished":
        state = Path(payload["upgrade_operation"]["state_dir"], "upgrade.json")
        record = json.loads(state.read_bytes())
        record["activation_started"] = False
        state.write_text(json.dumps(record))
        payload["upgrade_state_sha256"] = hashlib.sha256(state.read_bytes()).hexdigest()
    elif damage == "switch":
        payload["switch_sha256"] = "0" * 64
    elif damage == "deployment":
        payload["deployment"]["postgres_storage_gi"] += 10
    elif damage == "candidate":
        metadata["candidate"] = "0" * 40
    else:
        payload["upgrade_operation"]["inputs_path"] = str(path.parent / "inputs.json")
    inputs = Path(metadata["inputs_path"])
    inputs.write_text(json.dumps(payload))
    metadata["inputs_sha256"] = hashlib.sha256(inputs.read_bytes()).hexdigest()
    path.write_text(json.dumps(metadata))
    monkeypatch.setattr(retirement, "connected_retirement", lambda *args: pytest.fail("unqualified connection"))
    assert entry.main(str(path), "install") == 1
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"
