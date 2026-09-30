"""Restrict the recorded participant roles only after observed process drain."""
from __future__ import annotations

import copy
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_runtime import participant_readonly_roles
from tests.ops.test_nebius_pool_retirement import initialize
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def fencing_inputs(retirement_inputs):
    from scripts.ops.nebius_pool_role_fencing import PoolRoleFenceRequest

    roles = [copy.deepcopy(row) for row in participant_readonly_roles(request=retirement_inputs.migration) if row["kind"] == "Role"]
    for role in roles:
        role["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        role["rules"] = [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "create", "delete"]}]
    return PoolRoleFenceRequest(retirement=retirement_inputs, originals=tuple(roles))


class Roles:
    def __init__(self, request, retirement):
        self.retirement = retirement
        self.roles = {_key(row): copy.deepcopy(row) for row in request.originals}
        self.patches = []
        self.failure = None

    def read_role(self, key):
        return copy.deepcopy(self.roles[key])

    def restrict_role(self, key, before, desired):
        assert before == self.roles[key]
        self.patches.append(key)
        if self.failure == "conflict":
            return False
        if self.failure == "before":
            raise OSError("private-marker")
        result = copy.deepcopy(desired)
        result["metadata"].update(uid=before["metadata"]["uid"], resourceVersion="2")
        self.roles[key] = result
        if self.failure == "after":
            raise OSError("private-marker")
        return True


def run(request, api, tmp_path):
    from scripts.ops.nebius_pool_role_fencing import fence_pool_roles

    return fence_pool_roles(request=request, api=api, state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def test_six_retained_roles_become_readers_only_after_all_old_processes_drain(fencing_inputs, tmp_path):
    request = fencing_inputs
    retirement = initialize(request.retirement, tmp_path)
    retirement.busy.add(next(iter(retirement.documents)))
    api = Roles(request, retirement)
    assert run(request, api, tmp_path)["status"] == "pending_drain"
    assert api.patches == []
    retirement.busy.clear()
    result = run(request, api, tmp_path)
    assert result["status"] == "participant_roles_restricted" and result["writer_migration_complete"] is False
    assert len(api.patches) == 6 and len(retirement.patches) == 9
    for original in request.originals:
        current = api.roles[_key(original)]
        assert current["metadata"]["uid"] == original["metadata"]["uid"]
        assert all(set(rule["verbs"]) <= {"get", "list"} for rule in current["rules"])
        job_rule, = [rule for rule in current["rules"] if rule["resources"] == ["jobs"]]
        assert job_rule["verbs"] == ["get"]
    assert run(request, api, tmp_path) == result
    assert len(api.patches) == 6 and len(retirement.patches) == 9


def test_changed_original_role_is_rejected_before_stopping_any_controller(fencing_inputs, tmp_path):
    api = Roles(fencing_inputs, initialize(fencing_inputs.retirement, tmp_path))
    next(iter(api.roles.values()))["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError):
        run(fencing_inputs, api, tmp_path)
    assert api.patches == [] and api.retirement.patches == []


@pytest.mark.parametrize("failure", ["before", "after", "conflict"])
def test_role_fence_recovers_only_confirmed_effects_without_repeating_unknown_updates(fencing_inputs, tmp_path, failure):
    api = Roles(fencing_inputs, initialize(fencing_inputs.retirement, tmp_path))
    api.failure = failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError) as error:
                run(fencing_inputs, api, tmp_path)
            assert "private-marker" not in str(error.value)
        assert len(api.patches) == 1
    elif failure == "after":
        assert run(fencing_inputs, api, tmp_path)["status"] == "participant_roles_restricted"
        assert len(api.patches) == 6
    else:
        assert run(fencing_inputs, api, tmp_path)["status"] == "pending_role_fence"
        api.failure = None
        assert run(fencing_inputs, api, tmp_path)["status"] == "participant_roles_restricted"
        assert len(api.patches) == 7


@pytest.mark.parametrize("damage", ["role_uid", "rules", "controller", "journal", "anchor", "retirement_journal"])
def test_role_fence_replay_refuses_changed_authority_or_missing_evidence(fencing_inputs, tmp_path, damage):
    request = fencing_inputs
    api = Roles(request, initialize(request.retirement, tmp_path))
    run(request, api, tmp_path)
    if damage == "role_uid":
        next(iter(api.roles.values()))["metadata"]["uid"] = str(uuid4())
    elif damage == "rules":
        next(iter(api.roles.values()))["rules"][0]["verbs"].append("create")
    elif damage == "controller":
        next(row for row in api.retirement.documents.values() if row["kind"] == "Deployment")["spec"]["replicas"] = 1
    elif damage in {"journal", "retirement_journal"}:
        (tmp_path / "state" / ("role-fencing.json" if damage == "journal" else "retirement.json")).unlink()
    else:
        operation = request.retirement.migration.registration.spec.operation_id
        (tmp_path / "anchor" / (str(operation) + "-role-fencing.json")).unlink()
    with pytest.raises(ValueError):
        run(request, api, tmp_path)
    assert len(api.patches) == 6


@pytest.mark.parametrize("damage", ["missing", "extra", "kind", "name", "namespace"])
def test_role_fence_cannot_target_unrecorded_roles(fencing_inputs, tmp_path, damage):
    request = copy.deepcopy(fencing_inputs)
    api = Roles(request, initialize(request.retirement, tmp_path))
    if damage == "missing":
        request = replace(request, originals=request.originals[:-1])
    elif damage == "extra":
        request = replace(request, originals=(*request.originals, request.originals[0]))
    else:
        row = request.originals[0]
        if damage == "kind":
            row["kind"] = "ClusterRole"
        else:
            row["metadata"][damage] = "foreign"
    with pytest.raises(ValueError):
        run(request, api, tmp_path)
    assert api.patches == [] and api.retirement.patches == []
