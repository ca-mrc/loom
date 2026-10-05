"""Closed registration precedes retained, non-retrying old-controller retirement."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_migration import MigrationAPI
from tests.ops.test_nebius_pool_migration import run as close_pool
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def retirement_inputs(runtime_inputs, platform_inputs):
    from scripts.ops.nebius_pool_retirement import PoolRetirementRequest

    from loom.nebius_platform_render import build_platform

    migration, actuators, _, _ = runtime_inputs
    collectors = []
    config, candidate, profile = copy.deepcopy(platform_inputs)
    for participant in migration.registration.spec.participants:
        guard, = [row for row in migration.guards if row.participant_id == participant.participant_id]
        config.update(namespace=guard.namespace, execution_namespace=participant.execution_namespace.name)
        docs = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        cron, = [row for row in docs["60-execution.yaml"] if row["kind"] == "CronJob"]
        cron["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        collectors.append(cron)
    return PoolRetirementRequest(migration=migration, actuators=tuple(actuators.values()), collectors=tuple(collectors))


class API:
    def __init__(self, request):
        from scripts.ops.nebius_pool_retirement import retirement_documents

        self.request = request
        self.documents = copy.deepcopy(retirement_documents(request))
        self.patches = []
        self.held = True
        self.failure = None
        self.busy = set()

    def verify_guards(self):
        if not self.held:
            raise ValueError("private-marker")

    def read(self, key):
        return copy.deepcopy(self.documents[key])

    def stop(self, key, before):
        from scripts.ops.nebius_pool_retirement import stopped_document

        assert before == self.documents[key]
        self.patches.append(key)
        if self.failure == "conflict":
            return False
        if self.failure == "before":
            raise OSError("private-marker")
        desired = stopped_document(self.request, key)
        desired["metadata"].update(uid=before["metadata"]["uid"], resourceVersion=str(int(before["metadata"]["resourceVersion"]) + 1))
        self.documents[key] = desired
        if self.failure == "after":
            raise OSError("private-marker")
        return True

    def drained(self, key):
        return key not in self.busy


def initialize(request, tmp_path):
    assert close_pool(request.migration, MigrationAPI(request.migration), tmp_path)["status"] == "pool_registered_closed"
    return API(request)


def retire(request, api, tmp_path):
    from scripts.ops.nebius_pool_retirement import retire_pool_workloads

    return retire_pool_workloads(request=request, api=api, state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def test_stopped_roster_qualifies_once_and_returns_detached_exact_templates(retirement_inputs, monkeypatch):
    from scripts.ops import nebius_pool_retirement as retirement

    request = retirement_inputs
    originals = (*request.collectors, *request.actuators, *(row.controller for row in request.migration.guards))
    expected = {}
    for original in originals:
        value = copy.deepcopy(original)
        for field in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "selfLink"):
            value["metadata"].pop(field, None)
        value.pop("status", None)
        value["metadata"].setdefault("annotations", {})["loom.nebius/pool-retirement-operation"] = str(
            request.migration.registration.spec.operation_id)
        value["spec"]["suspend" if value["kind"] == "CronJob" else "replicas"] = True if value["kind"] == "CronJob" else 0
        expected[value["kind"] + ":" + value["metadata"]["namespace"] + ":" + value["metadata"]["name"]] = value
    qualified = retirement.retirement_documents
    calls = []
    def qualify(value):
        calls.append(value)
        return qualified(value)
    monkeypatch.setattr(retirement, "retirement_documents", qualify)
    result = retirement.stopped_documents(request)
    assert result == expected
    assert len(calls) == 1  # Real roster validation, not one full scan per output.
    first = next(iter(result.values()))
    first["spec"]["template" if first["kind"] == "Deployment" else "jobTemplate"]["private-mutation"] = True
    assert all("private-mutation" not in repr(row) for row in originals)
    assert retirement.stopped_documents(request) == expected


def test_exact_old_workloads_stop_without_changing_templates_or_opening_admission(retirement_inputs, tmp_path):
    request = retirement_inputs
    api = initialize(request, tmp_path)
    originals = copy.deepcopy(api.documents)
    result = retire(request, api, tmp_path)
    assert result["status"] == "old_pool_workloads_retired" and result["writer_migration_complete"] is False
    assert retire(request, api, tmp_path) == result
    assert len(api.patches) == 9
    assert [originals[key]["kind"] for key in api.patches[:3]] == ["CronJob"] * 3
    for key, original in originals.items():
        current = api.documents[key]
        assert current["metadata"]["uid"] == original["metadata"]["uid"]
        if current["kind"] == "CronJob":
            assert current["spec"]["suspend"] is True
            assert current["spec"]["jobTemplate"] == original["spec"]["jobTemplate"]
        else:
            assert current["spec"]["replicas"] == 0
            assert current["spec"]["template"] == original["spec"]["template"]
    assert (tmp_path / "state/retirement.json").stat().st_mode & 0o777 == 0o600


def test_guest_sibling_is_retired_and_drained_without_duplicate_participant(guest_runtime_inputs, retirement_inputs, tmp_path):
    request, actuators, _, _, guest = guest_runtime_inputs
    retirement = replace(retirement_inputs, migration=request, actuators=(*actuators.values(), guest))
    api = initialize(retirement, tmp_path)
    key = "Deployment:loom-nebius-exec-0:nebius-guest-fixture-actuator"
    api.busy.add(key)
    assert retire(retirement, api, tmp_path)["status"] == "pending_drain"
    assert api.documents[key]["spec"]["replicas"] == 0
    assert api.documents[key]["spec"]["template"] == guest["spec"]["template"]
    api.busy.clear()
    assert retire(retirement, api, tmp_path)["status"] == "old_pool_workloads_retired"
    assert len(api.patches) == 10
    assert retire(retirement, api, tmp_path)["status"] == "old_pool_workloads_retired"
    assert len(api.patches) == 10
    assert len(json.loads((tmp_path / "state/migration.json").read_text())["guards"]) == 3
    # Even a previously drained sibling must be checked again on replay.
    api.busy.add(key)
    assert retire(retirement, api, tmp_path)["status"] == "pending_drain"


def test_retirement_cannot_omit_registered_guest(guest_runtime_inputs, retirement_inputs):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    request, actuators, _, _, _ = guest_runtime_inputs
    retirement = replace(retirement_inputs, migration=request, actuators=tuple(actuators.values()))
    with pytest.raises(ValueError):
        retirement_documents(retirement)


@pytest.mark.parametrize("failure", ["before", "after", "conflict"])
def test_retirement_preserves_uncertain_updates_and_retries_only_definite_conflicts(retirement_inputs, tmp_path, failure):
    api = initialize(retirement_inputs, tmp_path)
    api.failure = failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError) as error:
                retire(retirement_inputs, api, tmp_path)
            assert "private-marker" not in str(error.value)
        assert len(api.patches) == 1
    elif failure == "after":
        assert retire(retirement_inputs, api, tmp_path)["status"] == "old_pool_workloads_retired"
        assert retire(retirement_inputs, api, tmp_path)["status"] == "old_pool_workloads_retired"
        assert len(api.patches) == 9
    else:
        assert retire(retirement_inputs, api, tmp_path)["status"] == "pending_retirement"
        api.failure = None
        assert retire(retirement_inputs, api, tmp_path)["status"] == "old_pool_workloads_retired"
        assert len(api.patches) == 10


def test_retirement_waits_for_drain_and_requalifies_earlier_stopped_workloads(retirement_inputs, tmp_path):
    api = initialize(retirement_inputs, tmp_path)
    key = next(iter(api.documents))
    api.busy.add(key)
    assert retire(retirement_inputs, api, tmp_path)["status"] == "pending_drain"
    assert len(api.patches) == 1
    api.busy.clear()
    assert retire(retirement_inputs, api, tmp_path)["status"] == "old_pool_workloads_retired"
    api.documents[key]["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError):
        retire(retirement_inputs, api, tmp_path)
    assert len(api.patches) == 9


@pytest.mark.parametrize("damage", ["registration", "guards", "closure_anchor", "original", "journal", "retirement_anchor"])
def test_missing_or_changed_evidence_cannot_restart_retirement(retirement_inputs, tmp_path, damage):
    api = initialize(retirement_inputs, tmp_path)
    if damage in {"journal", "retirement_anchor"}:
        retire(retirement_inputs, api, tmp_path)
        if damage == "journal":
            (tmp_path / "state/retirement.json").unlink()
        else:
            (tmp_path / "anchor" / (str(retirement_inputs.migration.registration.spec.operation_id) + "-retirement.json")).unlink()
    elif damage == "guards":
        api.held = False
    elif damage == "closure_anchor":
        (tmp_path / "anchor" / (str(retirement_inputs.migration.registration.spec.operation_id) + ".json")).unlink()
    elif damage == "original":
        next(iter(api.documents.values()))["metadata"]["uid"] = str(uuid4())
    else:
        state = tmp_path / "state/migration.json"
        record = json.loads(state.read_text())
        record["registration"]["proof"] = None
        state.write_text(json.dumps(record))
    count = len(api.patches)
    with pytest.raises(ValueError):
        retire(retirement_inputs, api, tmp_path)
    assert len(api.patches) == count


@pytest.mark.parametrize("damage", ["missing", "extra", "foreign_namespace", "wrong_kind", "wrong_name"])
def test_retirement_cannot_expand_beyond_registered_pool_controllers(retirement_inputs, tmp_path, damage):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    request = copy.deepcopy(retirement_inputs)
    if damage == "missing":
        request = replace(request, actuators=request.actuators[:-1])
    elif damage == "extra":
        request = replace(request, collectors=(*request.collectors, request.collectors[0]))
    elif damage == "foreign_namespace":
        request.actuators[0]["metadata"]["namespace"] = "foreign"
    elif damage == "wrong_kind":
        request.actuators[0]["kind"] = "StatefulSet"
    else:
        request.collectors[0]["metadata"]["name"] = "database-backup"
    with pytest.raises(ValueError):
        retirement_documents(request)
