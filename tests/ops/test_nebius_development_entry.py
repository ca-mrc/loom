"""The private entrypoint binds one source/input set and drives the installer."""
from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest
from tests.ops.test_nebius_candidate import source_checkout as source_checkout
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_live import live as live
from tests.ops.test_nebius_development_preflight import preflight as preflight
from tests.ops.test_nebius_development_preflight import published_source as published_source
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_development_foundation import development_inputs as development_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_development_entry")


@pytest.fixture
def entry(live, tmp_path, monkeypatch):
    root = tmp_path / "nebius-development" / live.request.bootstrap.installation_id
    root.mkdir(mode=0o700, parents=True)

    def private(path, value):
        path.write_text(value)
        path.chmod(0o600)
        return str(path)

    selection = asdict(live.request.selection)
    materials = {name: private(root / name, value) for name, value in selection.pop("storage").items()}
    payload = {"schema_version": "loom.nebius-development-private-inputs.v1", "binding": asdict(live.request.bootstrap),
        **selection, "settings": live.api.settings.model_dump(mode="json"), "storage_files": materials,
        "operator_connection": {"endpoint": live.api.api_server,
            "ca_file": private(root / "operator-ca.pem", "test-ca"),
            "credentials_file": str(live.credentials)}}
    raw = json.dumps(payload)
    operation = {"schema": "loom.nebius-development-operation.v1", "namespace": "loom-dev",
        "installation_id": live.request.bootstrap.installation_id,
        "source_sha": selection["candidate"]["candidate_sha"], "candidate": selection["candidate"]["candidate_sha"],
        "inputs_path": private(root / "inputs.json", raw), "inputs_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "state_dir": str(root / "state"), "anchor_dir": str(tmp_path / "nebius-development-anchors" / root.name)}
    (tmp_path / "nebius-development-anchors").mkdir(mode=0o700)
    operation_path = private(root / "operation.json", json.dumps(operation))
    source_path = Path(private(root / "development-source.json", live.api.settings.preflight.source.model_dump_json()))
    monkeypatch.setattr(module(), "SOURCE_RECORD", source_path)
    return operation, payload, operation_path, live


def test_entry_loads_one_exact_source_without_delivering_operator_credentials(entry):
    operation, _, _, live = entry
    inputs, request, files = module().load_inputs(operation)
    assert request == live.request
    assert inputs.binding.namespace == "loom-dev"
    assert set(request.selection.storage) == {"access-key", "secret-key", "source-access-key", "source-secret-key"}
    assert live.credentials in files and Path(operation["inputs_path"]) in files
    assert all("operator-test" not in value for value in request.selection.storage.values())


@pytest.mark.parametrize("change", ["hash", "source", "namespace", "path", "credential-alias", "extra", "public", "symlink"])
def test_private_input_mismatch_cannot_open_connection_or_write_state(entry, change, monkeypatch, capsys):
    operation, payload, path, live = entry
    if change == "hash":
        operation["inputs_sha256"] = "0" * 64
    elif change == "source":
        payload["settings"]["preflight"]["source"]["source_archive_sha256"] = "sha256:" + "0" * 64
    elif change == "namespace":
        operation["namespace"] = "loom-nebius-platform"
    elif change == "path":
        operation["state_dir"] = str(Path(operation["inputs_path"]).parent / "staging")
    elif change == "credential-alias":
        payload["storage_files"]["secret-key"] = str(live.credentials)
    elif change == "extra":
        payload["ready"] = True
    elif change == "public":
        Path(payload["storage_files"]["secret-key"]).chmod(0o644)
    else:
        original = Path(payload["storage_files"]["secret-key"])
        alias = original.with_suffix(".link")
        alias.symlink_to(original)
        payload["storage_files"]["secret-key"] = str(alias)
    raw = json.dumps(payload)
    Path(operation["inputs_path"]).write_text(raw)
    if change != "hash":
        operation["inputs_sha256"] = hashlib.sha256(raw.encode()).hexdigest()
    Path(path).write_text(json.dumps(operation))
    monkeypatch.setattr(module(), "connected_api", lambda *args, **kwargs: pytest.fail("connection opened"))
    assert module().main(path, "install") == 1
    report = capsys.readouterr().out
    assert json.loads(report) == {"status": "blocked", "stage": "inputs"}
    assert "secret" not in report and not Path(operation["state_dir"]).exists()


def test_private_entry_composes_real_installer_pending_and_resume_without_retry(entry, monkeypatch, capsys):
    from tests.ops.test_nebius_development_install import InstallationAPI

    operation, _, path, live = entry
    api = InstallationAPI(live.request)
    monkeypatch.setattr(module(), "connected_api", lambda *args, **kwargs: api)
    assert module().main(path, "preflight") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "development_preflight_qualified"
    assert not Path(operation["state_dir"]).exists()
    assert module().main(path, "install") == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "pending" and report["phase"] == "storage"
    assert report["candidate"] == operation["candidate"]
    writes = len(api.bootstrap.creates) + len(api.stage.creates)
    assert module().main(path, "install") == 0
    assert json.loads(capsys.readouterr().out) == report
    assert len(api.bootstrap.creates) + len(api.stage.creates) == writes


def test_unknown_command_does_not_read_private_files(monkeypatch, capsys):
    monkeypatch.setattr(module(), "load_inputs", lambda *args: pytest.fail("private inputs read"))
    assert module().main("/not-an-operation", "rollback-staging") == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "stage": "operation"}
