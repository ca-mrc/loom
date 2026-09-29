"""Diagnostic input authority is bound to the preserved original create receipts."""
from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from pathlib import Path

import pytest
from tests.ops.test_nebius_management_gateway import diagnostic_operation
from tests.ops.test_nebius_management_retirement_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_management_retirement_entry import (
    application_material as application_material,
)
from tests.ops.test_nebius_management_retirement_entry import checks as checks
from tests.ops.test_nebius_management_retirement_entry import cloud as cloud
from tests.ops.test_nebius_management_retirement_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_retirement_entry import installation as installation
from tests.ops.test_nebius_management_retirement_entry import management_inputs as management_inputs
from tests.ops.test_nebius_management_retirement_entry import material as material
from tests.ops.test_nebius_management_retirement_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_management_retirement_entry import (
    private_retirement as private_retirement,
)
from tests.ops.test_nebius_management_retirement_entry import private_upgrade as private_upgrade
from tests.ops.test_nebius_management_retirement_entry import (
    retirement_request as retirement_request,
)
from tests.ops.test_nebius_management_retirement_entry import setup_request as setup_request


@pytest.fixture
def private_diagnostic(private_retirement, tmp_path):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_retirement import install_retirement
    from scripts.ops.nebius_management_retirement_entry import load_retirement_inputs
    from tests.ops.test_nebius_management_stage import PhaseAPI

    original, _, _, _ = private_retirement
    context = load_retirement_inputs(original)
    api = PhaseAPI(context.request.binding)
    api.key = _key
    install_retirement(request=context.request, resources=lambda phase: nullcontext(api),
        state_dir=Path(original["state_dir"]), anchor_dir=Path(original["anchor_dir"]))
    job, = [doc for doc in api.resources.values() if doc["kind"] == "Job"]
    job["status"] = {"conditions": [{"type": "Failed", "status": "True"}], "failed": 1}
    metadata = diagnostic_operation(tmp_path) | {key: original[key] for key in ("candidate", "installation_id", "namespace")}
    state = Path(original["state_dir"])
    payload = {"schema_version": "loom.nebius-management-retirement-diagnostic-private-inputs.v1",
        "retirement_operation": original,
        "retirement_state_sha256": hashlib.sha256((state / "retirement.json").read_bytes()).hexdigest(),
        "retirement_journal_sha256": {phase: hashlib.sha256((state / phase / "stage.json").read_bytes()).hexdigest()
            for phase in ("permissions", "network", "job")}}
    path = Path(metadata["inputs_path"])
    path.parent.mkdir(mode=0o700)
    path.write_text(json.dumps(payload))
    path.chmod(0o600)
    metadata["inputs_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return metadata, payload, api


def test_diagnostic_loader_preserves_original_inputs_and_journals(private_diagnostic):
    from scripts.ops.nebius_management_retirement_diagnostic_entry import load_diagnostic_inputs

    metadata, payload, _ = private_diagnostic
    original_root = Path(payload["retirement_operation"]["inputs_path"]).parent
    frozen = {p: p.read_bytes() for p in original_root.rglob("*.json")}
    context = load_diagnostic_inputs(metadata)
    assert context.retirement.request.candidate["candidate_sha"] == metadata["candidate"]
    assert metadata["source_sha"] != metadata["candidate"]
    assert set(context.receipts) == {"permissions", "network", "job"}
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    assert not Path(metadata["state_dir"]).exists()


@pytest.mark.parametrize("damage", ["journal_bytes", "state_bytes", "anchor_missing", "incomplete_receipt", "original_candidate", "original_path"])
def test_diagnostic_rejects_changed_original_authority_before_network(private_diagnostic, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_management_retirement_diagnostic_entry import load_diagnostic_inputs

    metadata, payload, _ = private_diagnostic
    old = payload["retirement_operation"]
    state = Path(old["state_dir"])
    if damage == "journal_bytes":
        with (state / "job/stage.json").open("ab") as stream:
            stream.write(b" ")
    elif damage == "state_bytes":
        with (state / "retirement.json").open("ab") as stream:
            stream.write(b" ")
    elif damage == "anchor_missing":
        (Path(old["anchor_dir"]) / (old["installation_id"] + ".json")).unlink()
    elif damage == "incomplete_receipt":
        path = state / "job/stage.json"
        record = json.loads(path.read_bytes())
        next(iter(record["resources"].values()))["status"] = "create_intent"
        path.write_text(json.dumps(record))
        payload["retirement_journal_sha256"]["job"] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif damage == "original_candidate":
        metadata["candidate"] = "d" * 40
    else:
        old["inputs_path"] = str(Path(metadata["inputs_path"]))
    path = Path(metadata["inputs_path"])
    path.write_text(json.dumps(payload))
    metadata["inputs_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(EntryError):
        load_diagnostic_inputs(metadata)
    assert not Path(metadata["state_dir"]).exists()
