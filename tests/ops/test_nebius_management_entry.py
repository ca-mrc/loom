"""Private inputs stay on the gateway; public actions drive the actual installer."""
from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import contextmanager
from dataclasses import asdict

import pytest
from tests.ops.test_nebius_ingress_bootstrap import configuration
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_gateway import operation
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_management_entry")


@pytest.fixture
def entry_inputs(tmp_path, checks, installation):
    client, request = checks
    metadata = operation(tmp_path)
    metadata.update(installation_id=request.binding.installation_id, namespace=request.binding.namespace,
                    candidate=request.candidate["candidate_sha"])
    root = tmp_path / "nebius-management"
    root.mkdir(mode=0o700)
    def private(name, content):
        path = root / name
        path.write_text(content)
        path.chmod(0o600)
        return str(path)
    materials = {name: {key: private(name + "-" + key, value) for key, value in values.items()}
                 for name, values in request.material.items()}
    ingress = configuration(tmp_path)
    ingress["candidate"] = "1" * 40  # Historical installation journal is not rewritten.
    config = request.deployment.installation.foundation.platform_config
    config["cluster_id"] = "mk8scluster-e00fixture"
    ingress["api_server"] = config["kubernetes_api_server"]
    ingress["cluster_id"] = config["cluster_id"]
    connection = {"endpoint": config["kubernetes_api_server"], "ca_file": private("operator-ca.pem", "fixture-ca"),
                  "credentials_file": private("operator.json", "fixture-operator-credential")}
    payload = {"schema_version": "loom.nebius-management-private-inputs.v1", "binding": asdict(request.binding),
        "deployment": request.deployment.model_dump(mode="json"), "candidate": request.candidate, "profile": request.profile,
        "prerequisites": client.settings.model_dump(mode="json"), "operator_connection": connection,
        "operator_cloud_credentials": connection["credentials_file"], "material_files": materials,
        "ingress_config": private("ingress.json", json.dumps(ingress)), "foundation_candidate": "2" * 40}
    payload["deployment"]["installation"]["foundation"]["platform_config_json"] = json.dumps(config)
    private("inputs.json", json.dumps(payload))
    metadata["inputs_sha256"] = hashlib.sha256((root / "inputs.json").read_bytes()).hexdigest()
    path = private("operation.json", json.dumps(metadata))
    return metadata, payload, path, installation[1]


def test_private_inputs_load_exact_material_and_preserve_historical_ingress(entry_inputs):
    metadata, payload, _, _ = entry_inputs
    selected, request, ingress = module().load_inputs(metadata)
    assert selected.foundation_candidate == "2" * 40 and ingress["candidate"] == "1" * 40
    assert request.candidate["candidate_sha"] == metadata["candidate"]
    assert request.material["loom-management-publications"]["token"] == "scoped-publication-test-token"
    assert request.deployment.model_dump(mode="json") == payload["deployment"]


@pytest.mark.parametrize("change", ["hash", "candidate", "alias", "extra", "symlink", "public_file"])
def test_unqualified_private_inputs_fail_before_connection_or_state(entry_inputs, change, monkeypatch, capsys):
    from pathlib import Path
    metadata, payload, path, _ = entry_inputs
    if change == "hash":
        metadata["inputs_sha256"] = "0" * 64
    elif change == "candidate":
        metadata["candidate"] = "0" * 40
    elif change == "alias":
        payload["material_files"]["loom-management-cloud"]["credentials.json"] = payload["operator_connection"]["credentials_file"]
    elif change == "extra":
        payload["shell"] = "kubectl apply"
    elif change == "symlink":
        original = Path(payload["material_files"]["loom-management-publications"]["token"])
        link = original.with_suffix(".link")
        link.symlink_to(original)
        payload["material_files"]["loom-management-publications"]["token"] = str(link)
    else:
        Path(payload["material_files"]["loom-management-publications"]["token"]).chmod(0o644)
    Path(metadata["inputs_path"]).write_text(json.dumps(payload))
    if change != "hash":
        metadata["inputs_sha256"] = hashlib.sha256(Path(metadata["inputs_path"]).read_bytes()).hexdigest()
    Path(path).write_text(json.dumps(metadata))
    calls = []
    monkeypatch.setattr(module(), "connected_api", lambda *args: calls.append(args))
    assert module().main(path, "install") == 1
    assert calls == [] and not Path(metadata["state_dir"]).exists()
    output = capsys.readouterr().out
    assert json.loads(output)["status"] == "blocked" and "fixture-operator" not in output


def test_preflight_is_read_only_and_install_advances_only_to_readiness_barrier(entry_inputs, monkeypatch, capsys):
    from pathlib import Path
    metadata, _, path, api = entry_inputs
    calls = []
    @contextmanager
    def connect(inputs, request, ingress):
        calls.append(inputs.foundation_candidate)
        yield api
    monkeypatch.setattr(module(), "connected_api", connect)
    assert module().main(path, "preflight") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "preflight_qualified"
    assert api.events == ["preflight"] and not Path(metadata["state_dir"]).exists()
    assert module().main(path, "install") == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "pending" and report["phase"] == "database"
    assert report["candidate"] == metadata["candidate"]
    assert not any(doc["kind"] == "Deployment" for doc in api.store.resources.values())
    writes = len(api.store.creates)
    assert module().main(path, "install") == 0
    assert len(api.store.creates) == writes
    assert calls == ["2" * 40] * 3


def test_tooling_import_qualification_never_opens_private_inputs(entry_inputs, monkeypatch, capsys):
    _, _, path, _ = entry_inputs
    monkeypatch.setattr(module(), "load_inputs", lambda *args: pytest.fail("private inputs opened"))
    assert module().main(path, "qualify") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "tooling_qualified"


def test_unknown_action_never_opens_private_inputs(entry_inputs, monkeypatch):
    _, _, path, _ = entry_inputs
    monkeypatch.setattr(module(), "load_inputs", lambda *args: pytest.fail("private inputs opened"))
    assert module().main(path, "shell") == 1


@pytest.mark.parametrize("stage,expected", [("backup_pod_template", "backup_pod_template"),
    ("recovery", "recovery"), ("private-secret", "operation")])
def test_install_error_stage_survives_fixed_transport_without_exception_text(entry_inputs, monkeypatch, capsys,
                                                                           stage, expected):
    from scripts.ops.nebius_management_install import ManagementInstallError

    _, _, path, api = entry_inputs
    @contextmanager
    def connect(*args):
        yield api
    monkeypatch.setattr(module(), "connected_api", connect)
    def fail(**kwargs):
        raise ManagementInstallError("private-secret", stage=stage)
    monkeypatch.setattr(module(), "install_management", fail)
    assert module().main(path, "install") == 0
    result = capsys.readouterr().out
    assert json.loads(result)["stage"] == expected
    assert "private-secret" not in result


@pytest.mark.parametrize("action", ["preflight", "install"])
@pytest.mark.parametrize("stage,prerequisite,expected", [
    ("cluster_identity", None, "cluster_identity"),
    ("prerequisites", "storage_class", "storage_class"),
    ("backup_readback", None, "backup_readback"),
    ("backup_object", None, "backup_object"),
    ("install_service", None, "install_service"),
    (None, "storage_class", "operation"),
    ("private-secret", "private-secret", "operation"),
])
def test_bound_failure_exports_only_current_allowlisted_stage(entry_inputs, monkeypatch, capsys,
                                                              action, stage, prerequisite, expected):
    from pathlib import Path
    from types import SimpleNamespace

    metadata, _, path, api = entry_inputs
    api.diagnostic_stage = stage
    api.checks = SimpleNamespace(diagnostic_stage=prerequisite)
    def fail(*args):
        raise RuntimeError("private-secret raw-provider-response")
    api.preflight = fail
    @contextmanager
    def connect(*args):
        yield api
    monkeypatch.setattr(module(), "connected_api", connect)
    # A valid failure report must survive the private transport. The rollout
    # CLI, tested separately, treats blocked as nonzero rather than readiness.
    assert module().main(path, action) == 0
    output = capsys.readouterr().out
    report = json.loads(output)
    assert report == {"status": "blocked", "stage": expected,
        **{key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
    assert "private-secret" not in output and "raw-provider-response" not in output
    assert not Path(metadata["state_dir"]).exists()
