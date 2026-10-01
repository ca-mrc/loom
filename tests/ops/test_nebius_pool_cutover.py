"""The connected cutover stages closed runtimes without replaying old writers."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
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
        self.acl_staged = set()
        self.acl_failure = None
        self.acl_fail_participant = None

    def preflight(self, request):
        assert request == self.request
        if self.unqualified_preflight:
            raise ValueError("private-marker")

    def qualify_quiescence(self):
        self.events.append("quiescence")
        if self.unqualified_queue or self.active_application_access:
            raise ValueError("private-marker")

    def qualify_runtime_access(self, participant_id, action):
        assert action in {"stage", "observe"}
        self.events.append("acl-" + action + ":" + str(participant_id))
        if action == "stage":
            if self.acl_failure == "before" and participant_id == self.acl_fail_participant:
                raise OSError("private-marker")
            self.acl_staged.add(participant_id)
            if self.acl_failure == "after" and participant_id == self.acl_fail_participant:
                raise OSError("private-marker")
        else:
            assert participant_id in self.acl_staged

    def read_workload(self, key):
        return copy.deepcopy(self.documents[key])

    def preview_workload(self, key, before, desired):
        assert before == self.documents[key]
        return copy.deepcopy(desired)

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
        from scripts.ops.nebius_management_switch import _matches

        assert _matches(self.documents[key], desired, self.documents[key]["metadata"]["uid"])
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
    assert len([row for row in api.events if row.startswith("acl-stage:")]) == 3
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
    assert len([row for row in api.events if row.startswith("acl-stage:")]) == 3
    assert len([row for row in api.events if row.startswith("acl-observe:")]) >= 6


@pytest.mark.parametrize("failure", ["before", "after"])
def test_each_participant_acl_intent_is_recovered_independently_without_repeating_sql(cutover_inputs, tmp_path, failure):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    participant = request.fencing.retirement.migration.guards[1].participant_id
    api.acl_fail_participant, api.acl_failure = participant, failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError):
                run(request, tokens, api, tmp_path)
        assert not api.resources.creates
    else:
        assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
        assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
        assert len(api.acl_staged) == 3
    assert api.events.count("acl-stage:" + str(participant)) == 1


def test_partial_runtime_recovery_rechecks_every_frozen_producer_before_another_patch(cutover_inputs, tmp_path):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    api.fail_key = _key(request.fencing.retirement.migration.guards[0].controller)
    api.failure = "conflict"
    assert run(request, tokens, api, tmp_path)["status"] == "pending_runtime_update"
    api.failure = None
    api.documents[_key(request.services[-1])]["spec"]["replicas"] = 1
    before = len(api.patches)
    with pytest.raises(ValueError):
        run(request, tokens, api, tmp_path)
    assert len(api.patches) == before


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


@pytest.mark.parametrize("resource", ["gateway", "machine", "catalog"])
def test_resources_changed_during_runtime_replacement_cannot_qualify_closed_completion(cutover_inputs, tmp_path, monkeypatch, resource):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    patch = api.patch_workload
    creates = []

    def change_staged_resource(key, before, desired):
        result = patch(key, before, desired)
        if desired["kind"] == "CronJob":
            creates.append(len(api.resources.creates))
            if resource == "gateway":
                api.resources.resources["Deployment:loom-nebius-management:loom-pool-gateway"]["spec"]["replicas"] = 1
            elif resource == "machine":
                secret = next(row for row in api.resources.resources.values() if row["kind"] == "Secret")
                secret["data"]["token"] = "Zm9yZWlnbg=="
            else:
                catalog = next(row for row in api.resources.resources.values() if row["kind"] == "ConfigMap" and "profiles.json" in row["data"])
                catalog["data"]["profiles.json"] = "{}"
        return result

    monkeypatch.setattr(api, "patch_workload", change_staged_resource)
    with pytest.raises(ValueError):
        run(request, tokens, api, tmp_path)
    assert creates == [len(api.resources.creates)]  # No repair/overwrite of drift.


@pytest.mark.parametrize("damage", [None, "namespace", "uid", "running", "foreign_template", "redirect"])
def test_fixed_https_runtime_patch_binds_uid_namespace_and_disabled_target(cutover_inputs, damage):
    from scripts.ops.nebius_pool_cutover import cutover_documents
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
    from scripts.ops.nebius_pool_retirement import stopped_document

    request, tokens = cutover_inputs
    migration = request.fencing.retirement.migration
    guard = migration.guards[0]
    key = _key(guard.controller)
    before = stopped_document(request.fencing.retirement, key)
    before["metadata"].update(uid=guard.controller["metadata"]["uid"], resourceVersion="2")
    desired = cutover_documents(request)["runtime"][key]
    namespaces = {migration.registration.binding.namespace: migration.registration.binding.namespace_uid,
        "kube-system": migration.registration.binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace)}}
    calls = []

    def respond(message):
        calls.append(message)
        if message.method == "GET" and message.url.path.startswith("/api/v1/namespaces/"):
            name = message.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": str(uuid4()) if damage == "namespace" and name == guard.namespace else namespaces[name],
                "labels": {"loom.nebius/management-installation": migration.registration.binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        assert message.method == "PATCH"
        assert message.url.path == "/apis/apps/v1/namespaces/" + guard.namespace + "/deployments/loom-control-plane"
        if damage == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.example/"})
        patch = json.loads(message.content)
        assert patch[:3] == [{"op": "test", "path": "/metadata/uid", "value": guard.controller["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "2"},
            {"op": "test", "path": "/spec", "value": before["spec"]}]
        value = copy.deepcopy(desired)
        value["metadata"].update(uid=guard.controller["metadata"]["uid"], resourceVersion="3")
        return httpx.Response(200, json=value)

    external = CutoverAPI(request)
    external.migration.guards = {row.participant_id: str(migration.registration.spec.operation_id) for row in migration.guards}
    with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=external.migration,
            guards=SimpleNamespace(request=migration, guard=external.migration.guard), checks=external, api_server="https://cluster.example",
            ssl_context=ssl.create_default_context()) as api:
        api.fencing.verify_readonly = external.fencing.verify_readonly
        api.client.close()
        api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond), follow_redirects=False)
        if damage == "uid":
            before["metadata"]["uid"] = str(uuid4())
        elif damage == "running":
            desired["spec"]["replicas"] = 1
        elif damage == "foreign_template":
            desired["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign.example/unreviewed:latest"
        if damage:
            with pytest.raises(ValueError):
                api.patch_workload(key, before, desired)
        else:
            assert api.patch_workload(key, before, desired) is True
        patches = [row for row in calls if row.method == "PATCH"]
        assert len(patches) == (1 if damage in {None, "redirect"} else 0)


def test_https_resource_stage_routes_only_fixed_catalog_secrets_and_gateway_authority(cutover_inputs, tmp_path):
    from scripts.ops.nebius_management_stage import _MARKER
    from scripts.ops.nebius_pool_cutover import cutover_documents
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI

    request, tokens = cutover_inputs
    migration = request.fencing.retirement.migration
    binding = migration.registration.binding
    namespaces = {binding.namespace: binding.namespace_uid, "kube-system": binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace)}}
    calls = []

    def respond(message):
        calls.append(message)
        name = message.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": name, "uid": namespaces[name], "labels": {"loom.nebius/management-installation": binding.installation_id,
                "pod-security.kubernetes.io/enforce": "restricted"}}})

    external = CutoverAPI(request)
    with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=external.migration,
            guards=SimpleNamespace(request=migration), checks=external, api_server="https://cluster.example",
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
        documents = cutover_documents(request)
        cluster_role = next(row for row in documents["authority"] if row["kind"] == "ClusterRole")
        collector = next(row for row in documents["configuration"] if row["metadata"]["namespace"] != binding.namespace)
        for doc, want in ((cluster_role, "/apis/rbac.authorization.k8s.io/v1/clusterroles"),
                (collector, "/api/v1/namespaces/" + collector["metadata"]["namespace"] + "/configmaps")):
            assert api._approved(doc) == want
            marked = copy.deepcopy(doc)
            marked["metadata"].setdefault("annotations", {})[_MARKER] = str(uuid4())
            assert api._approved(marked, writing=True) == want
            marked["metadata"]["name"] = "foreign-resource"
            with pytest.raises(ValueError):
                api.create_resource(marked)
        assert not calls  # Rejected before even an operator request.


@pytest.mark.parametrize('damage', [None, 'schema', 'origin_history', 'unknown_origin'])
def test_https_quiescence_requires_bound_database_pages_and_registered_origin_history(cutover_inputs, monkeypatch, damage):
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI

    from loom.nebius_pool_priority import PoolWorkOriginV1

    request, tokens = cutover_inputs
    migration = request.fencing.retirement.migration
    calls = []
    external = CutoverAPI(request)

    def database_page(target, *, after):
        calls.append(('database', target.participant_id, after))
        assert target in migration.guards and after is None
        participant = next(row for row in migration.registration.spec.participants
            if row.participant_id == target.participant_id)
        return {'status': 'observed', 'schema_revision': '0171' if damage == 'schema' else '0172', 'rows': [{
            'key': 'batch:' + str(participant.participant_id), 'source_matches': True,
            'origin': None if damage == 'unknown_origin' else {
                'schema_version': 'loom.pool-work-origin.v1', 'data_environment_id': str(participant.environment_id),
                'submission_id': str(participant.participant_id), 'kind': 'environment', 'application': None}}]}

    def registered_origins(target, origins):
        assert target in migration.guards
        assert len(origins) == 1 and isinstance(origins[0], PoolWorkOriginV1)
        calls.append(('history', target.participant_id))
        if damage == 'origin_history':
            raise ValueError('private-marker')

    external.qualify_pending_origins = registered_origins
    guards = SimpleNamespace(request=migration, cutover_readiness_page=database_page)
    with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=external.migration,
            guards=guards, checks=external, api_server='https://cluster.example',
            ssl_context=ssl.create_default_context()) as api:
        monkeypatch.setattr(api, '_scope', lambda: None)
        if damage:
            with pytest.raises(ValueError):
                api.qualify_quiescence()
            assert external.events == []
        else:
            api.qualify_quiescence()
            assert calls == [item for row in migration.guards for item in
                [('database', row.participant_id, None), ('history', row.participant_id)]]
            assert external.events == ['quiescence']
