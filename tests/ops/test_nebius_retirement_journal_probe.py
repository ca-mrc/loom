"""Original create receipts, not timestamp resolution, bind diagnostic targets."""
from __future__ import annotations

import copy
import json
import shlex
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key, _snapshot
from scripts.ops.nebius_management_retirement import stage_retirement
from tests.ops.test_nebius_retirement_registry_inspection import RegistryCluster, inspect
from tests.ops.test_nebius_retirement_registry_inspection import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_retirement_registry_inspection import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_retirement_registry_inspection import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_retirement_registry_inspection import (
    retirement_request as retirement_request,
)
from tests.ops.test_nebius_retirement_registry_inspection import setup_request as setup_request

from loom.nebius_platform_render import digest


@pytest.fixture
def receipt(retirement_request, tmp_path):
    request, api = retirement_request
    state = tmp_path / "nebius-management" / "retirement" / "state"
    state.mkdir(parents=True, mode=0o700)
    stage_retirement(request=request, phase="job", api=api, state_dir=state / "job")
    expected = {"binding": asdict(request.binding), "resources": {
        _key(doc): {"uid": doc["metadata"]["uid"], "snapshot": digest(_snapshot(doc))}
        for doc in api.resources.values()}}
    return state, expected


@pytest.mark.parametrize("mutation", [None, "uid", "snapshot", "binding", "uncommitted", "missing", "public_file"])
def test_fixed_journal_probe_matches_real_create_receipts_without_writes(receipt, mutation):
    from scripts.ops import nebius_retirement_journal_probe as probe

    state, expected = receipt
    path = state / "job" / "stage.json"
    if mutation == "uid":
        next(iter(expected["resources"].values()))["uid"] = str(uuid4())
    elif mutation == "snapshot":
        next(iter(expected["resources"].values()))["snapshot"] = "sha256:" + "0" * 64
    elif mutation == "binding":
        expected["binding"]["namespace_uid"] = str(uuid4())
    elif mutation == "uncommitted":
        record = json.loads(path.read_text())
        next(iter(record["resources"].values()))["status"] = "create_intent"
        path.write_text(json.dumps(record))
    elif mutation == "missing":
        path.unlink()
    elif mutation == "public_file":
        path.chmod(0o644)
    before = {str(file): file.read_bytes() for file in state.rglob("*") if file.is_file()}
    result = subprocess.run([sys.executable, "-c", Path(probe.__file__).read_text(), str(state), json.dumps(expected)],
                            capture_output=True, text=True, timeout=10, check=True)
    assert json.loads(result.stdout)["status"] == ("matched" if mutation is None else "unavailable")
    assert not result.stderr
    assert {str(file): file.read_bytes() for file in state.rglob("*") if file.is_file()} == before
    assert "retirement.json" not in result.stdout and len(result.stdout) < 256


@pytest.mark.parametrize("replacement", [False, True])
def test_protected_inspection_binds_equal_timestamp_config_to_original_journal(
    receipt, retirement_request, monkeypatch, replacement,
):
    state, _ = receipt
    request, api = retirement_request
    cluster = RegistryCluster(request)
    cluster.job = copy.deepcopy(next(doc for doc in api.resources.values() if doc["kind"] == "Job"))
    cluster.cm = copy.deepcopy(next(doc for doc in api.resources.values() if doc["kind"] == "ConfigMap"))
    cluster.job["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
    for doc in (cluster.job, cluster.cm):
        doc["metadata"]["creationTimestamp"] = "2026-09-28T00:00:00Z"
    if replacement:
        cluster.cm["metadata"]["uid"] = str(uuid4())
    cluster.pod["metadata"]["ownerReferences"][0]["uid"] = cluster.job["metadata"]["uid"]
    cluster.lists["namespaces"] += [{"metadata": {"name": name, "uid": uid}} for name, uid in (
        (request.binding.namespace, request.binding.namespace_uid), ("kube-system", request.binding.kube_system_uid))]
    operation = {"schema": "loom.nebius-management-retirement-operation.v1", "source_sha": "a" * 40,
        "candidate": "a" * 40, "installation_id": request.binding.installation_id, "namespace": request.binding.namespace,
        "state_dir": str(state), "anchor_dir": str(state.parent / "anchor"),
        "inputs_path": str(state.parent / "inputs.json"), "inputs_sha256": "a" * 64}
    monkeypatch.setenv("NEBIUS_MANAGEMENT_OPERATION_JSON", json.dumps(operation))
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "operator@fixture")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/hosts")
    original = subprocess.run

    def ssh(command, *, input, capture_output, text, timeout, check):
        assert command[0] == "ssh" and command[-2] == "operator@fixture"
        arguments = shlex.split(command[-1])
        assert arguments[:2] == ["python3", "-"] and arguments[2] == str(state)
        return original([sys.executable, "-c", input, *arguments[2:]], capture_output=True,
                        text=True, timeout=10, check=True)

    monkeypatch.setattr(subprocess, "run", ssh)
    report = inspect(cluster)["failed_retirement_jobs"][0]["registry_probe"]
    assert report["status"] == ("unavailable" if replacement else "observed")
    assert cluster.executions == (0 if replacement else 1)
