"""The connected cutover stages closed runtimes without replaying old writers."""
from __future__ import annotations

import copy
import hashlib
from dataclasses import replace

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_collector_runtime import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_material import MaterialAPI
from tests.ops.test_nebius_pool_migration import MigrationAPI
from tests.ops.test_nebius_pool_retirement import API as RETIREMENT_API
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_fencing import Roles
from tests.ops.test_nebius_pool_role_fencing import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_runtime import desired_profile
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def cutover_inputs(collector_inputs, retirement_inputs, fencing_inputs, runtime_inputs):
    from scripts.ops.nebius_pool_cutover import PoolCutoverRequest

    from loom_service.pool_management.installation import PoolInstallation

    migration, collector, configmap = collector_inputs
    _, _, services, manager = runtime_inputs
    config = migration.registration.spec.model_dump(mode="json")
    tokens = {row.machine_id: "cutover_" + row.machine_id.hex for row in migration.registration.spec.machines}
    for row in config["machines"]:
        row["token_sha256"] = hashlib.sha256(tokens[next(key for key in tokens if str(key) == row["machine_id"])].encode()).hexdigest()
    migration = replace(migration, registration=replace(migration.registration, spec=PoolInstallation.model_validate(config)))
    retirement = replace(retirement_inputs, migration=migration,
        collectors=tuple(collector if row["metadata"]["namespace"] == collector["metadata"]["namespace"] else row
            for row in retirement_inputs.collectors))
    fencing = replace(fencing_inputs, retirement=retirement)
    request = PoolCutoverRequest(fencing=fencing, manager=manager, services=tuple(services.values()),
        collector_config=configmap, profiles={key: desired_profile(migration, doc) for key, doc in services.items()},
        management_origin="https://manage.example.com", kubernetes_endpoint="https://kubernetes.default.svc")
    return request, tokens


class CutoverAPI:
    """Only external I/O is doubled; child journals/renderers remain real."""

    def __init__(self, request):
        self.request = request
        migration = request.fencing.retirement.migration
        self.migration = MigrationAPI(migration)
        self.retirement = RETIREMENT_API(request.fencing.retirement)
        self.fencing = Roles(request.fencing, self.retirement)
        self.resources = MaterialAPI(migration.registration.binding)
        self.documents = self.retirement.documents
        for doc in (request.manager, *request.services):
            self.documents[_key(doc)] = copy.deepcopy(doc)
        self.patches = []
        self.events = []
        self.busy = set()
        self.unqualified_queue = False
        self.active_application_access = False
        self.failure = None
        self.fail_key = None
        self.unqualified_preflight = False

    def preflight(self, request):
        assert request == self.request
        if self.unqualified_preflight:
            raise ValueError("private-marker")

    def qualify_quiescence(self):
        self.events.append("quiescence")
        if self.unqualified_queue or self.active_application_access:
            raise ValueError("private-marker")

    def qualify_runtime_access(self, action):
        assert action in {"stage", "observe"}
        self.events.append("acl-" + action)

    def read_workload(self, key):
        return copy.deepcopy(self.documents[key])

    def patch_workload(self, key, before, desired):
        assert before == self.documents[key]
        self.patches.append(key)
        self.events.append("patch:" + key)
        if self.failure == "conflict" and key == self.fail_key:
            return False
        if self.failure == "before" and key == self.fail_key:
            raise OSError("private-marker")
        value = copy.deepcopy(desired)
        value["metadata"].update(uid=before["metadata"]["uid"], resourceVersion=str(int(before["metadata"]["resourceVersion"]) + 1))
        self.documents[key] = value
        if self.failure == "after" and key == self.fail_key:
            raise OSError("private-marker")
        return True

    def drained_workload(self, key, desired):
        assert self.documents[key]["spec"] == desired["spec"]
        return key not in self.busy


def run(request, tokens, api, tmp_path):
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover

    return stage_pool_cutover(request=request, tokens=tokens, api=api,
        state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "cutover-anchor")


def test_connected_parent_freezes_producers_retires_fences_and_stages_only_closed_runtime(cutover_inputs, tmp_path):
    from loom.nebius_pool_settings import PoolRuntimeSettings

    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    result = run(request, tokens, api, tmp_path)
    assert result["status"] == "pool_runtime_staged_closed"
    assert result["writer_migration_complete"] is False
    producer_keys = [_key(request.manager), *(_key(row) for row in request.services)]
    assert api.patches[:4] == producer_keys
    assert api.events.index("quiescence") > api.events.index("patch:" + producer_keys[-1])
    assert len(api.retirement.patches) == 9
    assert len(api.fencing.patches) == 6
    assert api.events.count("acl-stage") == 1
    assert len(api.migration.guards) == 3
    for document in api.documents.values():
        if document["kind"] == "Deployment":
            assert document["spec"]["replicas"] == 0
        else:
            assert document["spec"]["suspend"] is True
        if document["metadata"]["name"] == "loom-control-plane":
            container, = document["spec"]["template"]["spec"]["containers"]
            settings = {row["name"]: row for row in container["env"]}
            runtime = PoolRuntimeSettings.model_validate_json(settings["LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON"]["value"])
            assert runtime.management_origin == "https://manage.example.com"
    gateway = api.resources.resources["Deployment:loom-nebius-management:loom-pool-gateway"]
    assert gateway["spec"]["replicas"] == 0
    serialized = (tmp_path / "cutover/cutover.json").read_text()
    assert all(token not in serialized for token in tokens.values())


def test_recovery_qualifies_wired_templates_instead_of_replaying_original_retirement(cutover_inputs, tmp_path):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    first = run(request, tokens, api, tmp_path)
    before = (len(api.patches), len(api.retirement.patches), len(api.fencing.patches), len(api.resources.creates))
    assert run(request, tokens, api, tmp_path) == first
    assert (len(api.patches), len(api.retirement.patches), len(api.fencing.patches), len(api.resources.creates)) == before
    assert api.events.count("acl-stage") == 1
    assert api.events.count("acl-observe") >= 2


@pytest.mark.parametrize("boundary", ["producer_drain", "queue_origin", "application_access", "publication"])
def test_unqualified_producer_or_schema_boundary_cannot_retire_writers_or_issue_material(cutover_inputs, tmp_path, boundary):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    if boundary == "producer_drain":
        api.busy.add(_key(request.services[-1]))
        assert run(request, tokens, api, tmp_path)["status"] == "pending_producer_drain"
    else:
        api.unqualified_queue = boundary == "queue_origin"
        api.active_application_access = boundary == "application_access"
        api.unqualified_preflight = boundary == "publication"
        with pytest.raises(ValueError):
            run(request, tokens, api, tmp_path)
    assert not api.retirement.patches and not api.fencing.patches and not api.resources.creates
    assert not api.migration.guards
    if boundary == "publication":
        assert not api.patches


@pytest.mark.parametrize("failure", ["before", "after", "conflict"])
def test_uncertain_runtime_patch_is_observed_not_repeated(cutover_inputs, tmp_path, failure):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    key = _key(request.fencing.retirement.migration.guards[0].controller)
    api.fail_key, api.failure = key, failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError) as error:
                run(request, tokens, api, tmp_path)
            assert "private-marker" not in str(error.value)
        assert api.patches.count(key) == 1
    elif failure == "after":
        assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
        assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
        assert api.patches.count(key) == 1
    else:
        assert run(request, tokens, api, tmp_path)["status"] == "pending_runtime_update"
        api.failure = None
        assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
        assert api.patches.count(key) == 2
    assert len(api.migration.guards) == 3


@pytest.mark.parametrize("damage", ["effective_grant", "role", "workload", "guard", "lost_parent", "lost_anchor", "lost_child"])
def test_runtime_recovery_rejects_lost_evidence_and_authority_or_identity_drift(cutover_inputs, tmp_path, damage):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    run(request, tokens, api, tmp_path)
    before = (len(api.patches), len(api.retirement.patches), len(api.fencing.patches), len(api.resources.creates))
    if damage == "effective_grant":
        api.fencing.extra_authority = True
    elif damage == "role":
        next(iter(api.fencing.roles.values()))["rules"].append({"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create"]})
    elif damage == "workload":
        api.documents[_key(request.manager)]["spec"]["replicas"] = 1
    elif damage == "guard":
        api.migration.guards.clear()
    elif damage == "lost_parent":
        (tmp_path / "cutover/cutover.json").unlink()
    elif damage == "lost_anchor":
        next((tmp_path / "cutover-anchor").glob("*-cutover.json")).unlink()
    else:
        (tmp_path / "cutover/writers/role-fencing.json").unlink()
    with pytest.raises(ValueError):
        run(request, tokens, api, tmp_path)
    assert (len(api.patches), len(api.retirement.patches), len(api.fencing.patches), len(api.resources.creates)) == before


def test_runtime_renderer_rejects_an_incomplete_shared_service_roster_before_any_mutation(cutover_inputs, tmp_path):
    request, tokens = cutover_inputs
    request = replace(request, services=request.services[:-1])
    api = CutoverAPI(request)
    with pytest.raises(ValueError):
        run(request, tokens, api, tmp_path)
    assert not api.patches and not api.retirement.patches and not api.resources.creates
