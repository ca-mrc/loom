"""Diagnostic creation has its own receipt and cannot replace the failed Job."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def test_diagnostic_preserves_runtime_mounts_identity_and_policy_selectors(retirement_request):
    from scripts.ops.nebius_management_retirement import retirement_documents
    from scripts.ops.nebius_management_retirement_diagnostic import diagnostic_documents

    request, _ = retirement_request
    original, = [doc for doc in retirement_documents(request)["job"].values() if doc["kind"] == "Job"]
    frozen = copy.deepcopy(original)
    docs = diagnostic_documents(request)
    diagnostic, = docs.values()
    assert diagnostic["kind"] == "Job" and diagnostic["metadata"]["name"] != original["metadata"]["name"]
    assert diagnostic["spec"]["backoffLimit"] == 0 and diagnostic["spec"]["activeDeadlineSeconds"] == 240
    assert "ttlSecondsAfterFinished" not in diagnostic["spec"]
    before, after = original["spec"]["template"], diagnostic["spec"]["template"]
    assert before["metadata"] == after["metadata"]  # Same network-policy selectors.
    before_pod, after_pod = copy.deepcopy(before["spec"]), copy.deepcopy(after["spec"])
    command = after_pod["containers"][0].pop("command")
    before_pod["containers"][0].pop("command")
    assert before_pod == after_pod
    assert command[:2] == ["python", "-c"] and len(command) == 3
    assert command[2] == (Path(__file__).parents[2] / "scripts/ops/nebius_retirement_startup_probe.py").read_text()
    assert original == frozen


@pytest.fixture
def staging(retirement_request, tmp_path):
    request, _ = retirement_request
    api = PhaseAPI(request.binding)
    return dict(request=request, api=api, state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def test_diagnostic_create_intent_precedes_write_and_replay_never_recreates(staging):
    from scripts.ops.nebius_management_retirement_diagnostic import stage_diagnostic

    api = staging["api"]
    original_create = api.create_resource

    def create(doc):
        assert (staging["anchor_dir"] / (staging["request"].binding.installation_id + ".json")).is_file()
        record = json.loads((staging["state_dir"] / "job/stage.json").read_bytes())
        item, = record["resources"].values()
        assert item["status"] == "create_intent" and item["uid"] is None
        original_create(doc)

    api.create_resource = create
    first = stage_diagnostic(**staging)
    assert len(api.creates) == 1
    frozen = copy.deepcopy(api.resources)
    assert stage_diagnostic(**staging) == first
    assert api.resources == frozen and len(api.creates) == 1


@pytest.mark.parametrize("damage", ["lost_anchor", "lost_progress", "lost_journal", "missing_job", "replaced_job", "changed_job"])
def test_diagnostic_evidence_loss_or_replacement_never_retries(staging, damage):
    from scripts.ops.nebius_management_retirement_diagnostic import stage_diagnostic
    from scripts.ops.nebius_management_stage import ManagementStageError

    stage_diagnostic(**staging)
    api, state = staging["api"], staging["state_dir"]
    if damage == "lost_anchor":
        (staging["anchor_dir"] / (staging["request"].binding.installation_id + ".json")).unlink()
    elif damage == "lost_progress":
        (state / "diagnostic.json").unlink()
    elif damage == "lost_journal":
        (state / "job/stage.json").unlink()
    elif damage == "missing_job":
        api.resources.clear()
    elif damage == "replaced_job":
        next(iter(api.resources.values()))["metadata"]["uid"] = str(uuid4())
    else:
        next(iter(api.resources.values()))["spec"]["template"]["spec"]["containers"][0]["command"] = ["python", "-m", "foreign"]
    before = copy.deepcopy(api.resources)
    with pytest.raises(ManagementStageError):
        stage_diagnostic(**staging)
    assert len(api.creates) == 1 and api.resources == before


def test_foreign_diagnostic_name_is_not_adopted_or_replaced(staging):
    from scripts.ops.nebius_management_retirement_diagnostic import (
        diagnostic_documents,
        stage_diagnostic,
    )
    from scripts.ops.nebius_management_stage import ManagementStageError

    api = staging["api"]
    doc, = diagnostic_documents(staging["request"]).values()
    foreign = copy.deepcopy(doc)
    foreign["metadata"]["uid"] = str(uuid4())
    api.resources[api.key(doc)] = foreign
    with pytest.raises(ManagementStageError):
        stage_diagnostic(**staging)
    assert not api.creates and api.resources[api.key(doc)] == foreign
