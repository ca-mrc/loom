"""The connected cutover stages closed runtimes without replaying old writers."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

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


def platform_writer_authority(kube_system_uid):
    from scripts.ops.nebius_pool_platform_authority import PoolPlatformAuthority

    resources = []
    for name, subjects, rules in (
        ("cluster-admin", [{"kind": "Group", "name": "system:masters", "apiGroup": "rbac.authorization.k8s.io"}],
            [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}, {"nonResourceURLs": ["*"], "verbs": ["*"]}]),
        ("system:controller:job-controller", [{"kind": "ServiceAccount", "name": "job-controller", "namespace": "kube-system"}],
            [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "patch", "update", "watch"]},
                {"apiGroups": ["batch"], "resources": ["jobs/status"], "verbs": ["update"]},
                {"apiGroups": ["batch"], "resources": ["jobs/finalizers"], "verbs": ["update"]},
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "delete", "list", "patch", "watch"]},
                {"apiGroups": ["", "events.k8s.io"], "resources": ["events"], "verbs": ["create", "patch", "update"]}]),
    ):
        for kind in ("ClusterRole", "ClusterRoleBinding"):
            document = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": kind,
                "metadata": {"name": name, "uid": str(uuid4()), "resourceVersion": "1",
                    "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                    "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"}}}
            if kind == "ClusterRole":
                document["rules"] = rules
            else:
                document.update(subjects=subjects,
                    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name})
            resources.append(document)
    return PoolPlatformAuthority(schema_version="loom.pool-platform-authority.v1", kube_system_uid=kube_system_uid,
        resources=tuple(resources))


def test_cutover_projection_reuses_only_exact_inputs_and_detaches_outputs(cutover_inputs, monkeypatch):
    from scripts.ops import nebius_pool_cutover as cutover

    request, _ = cutover_inputs
    render = cutover.wire_manager
    calls = []

    def counted(**kwargs):
        calls.append(None)
        return render(**kwargs)

    monkeypatch.setattr(cutover, "wire_manager", counted)
    first = cutover.cutover_documents(request)
    original = copy.deepcopy(first)
    key = _key(request.manager)
    first["runtime"][key]["spec"]["replicas"] = 100
    assert cutover.cutover_documents(request) == original
    assert len(calls) == 1

    request.manager["spec"]["template"]["metadata"].setdefault("annotations", {})["example.com/revision"] = "next"
    changed = cutover.cutover_documents(request)
    assert changed["runtime"][key]["spec"]["template"]["metadata"]["annotations"]["example.com/revision"] == "next"
    assert len(calls) == 2
    # Equality alone would alias True and 1, bypassing the original validator.
    request.manager["spec"]["replicas"] = True
    with pytest.raises(ValueError):
        cutover.cutover_documents(request)


def test_pure_projection_never_reuses_mutated_model_inputs(cutover_inputs):
    from scripts.ops.nebius_pool_cutover import cutover_documents

    request, _ = cutover_inputs
    cutover_documents(request)
    changed = replace(request, collector_credential=request.collector_credential.model_copy(update={"uid": UUID(int=0)}))
    with pytest.raises(ValueError):
        cutover_documents(changed)


def test_reused_cutover_projection_rechecks_current_image_admission_clock(cutover_inputs, monkeypatch):
    from datetime import datetime, timedelta

    from scripts.ops.nebius_pool_cutover import cutover_documents

    from loom import execution_image_admission as admission

    request, _ = cutover_inputs
    cutover_documents(request)
    before_issuance = min(row.statement.issued_at for profile in request.profiles.values()
        for row in profile.image_admission.admissions) - timedelta(hours=1)

    class EarlierClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return before_issuance

    monkeypatch.setattr(admission, "datetime", EarlierClock)
    with pytest.raises(ValueError):
        cutover_documents(request)


@pytest.fixture
def cutover_inputs(collector_inputs, retirement_inputs, fencing_inputs, runtime_inputs):
    from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
    from scripts.ops.nebius_pool_runtime import PoolCollectorCredential

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
        management_origin="https://manage.example.com", kubernetes_endpoint="https://kubernetes.default.svc",
        collector_credential=PoolCollectorCredential(uid=uuid4(), resource_version='27',
            sha256=hashlib.sha256(b'{"private":"collector-marker"}').hexdigest()),
        platform_authority=platform_writer_authority(migration.registration.binding.kube_system_uid))
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

    def qualify_binding(self, migration, manager):
        assert migration == self.request.fencing.retirement.migration and manager == self.request.manager

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


def test_connected_cutover_preserves_closed_dormant_consumers_and_rejects_respawn(cutover_inputs, tmp_path):
    from scripts.ops.nebius_pool_cutover import cutover_documents, retained_cutover_workloads
    from tests.ops.test_nebius_pool_dormant import dormant_consumer

    request, tokens = cutover_inputs
    consumer = dormant_consumer(request.fencing.retirement)
    request = replace(request, fencing=replace(request.fencing,
        retirement=replace(request.fencing.retirement, dormant_consumers=(consumer,))))
    api = CutoverAPI(request)
    result = run(request, tokens, api, tmp_path)
    assert result["status"] == "pool_runtime_staged_closed"
    assert len(api.retirement.patches) == 11 and len(api.migration.guards) == 3
    runtime = cutover_documents(request)["runtime"]
    retained = retained_cutover_workloads(request, state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "cutover-anchor")
    for original in (consumer.actuator, consumer.collector):
        key = _key(original)
        field = "template" if original["kind"] == "Deployment" else "jobTemplate"
        assert key not in runtime and key in retained
        assert api.documents[key]["spec"][field] == original["spec"][field]
    before = (len(api.patches), len(api.retirement.patches), len(api.resources.creates))
    assert run(request, tokens, api, tmp_path) == result
    api.documents[_key(consumer.collector)]["spec"]["suspend"] = False
    with pytest.raises(ValueError):
        run(request, tokens, api, tmp_path)
    assert (len(api.patches), len(api.retirement.patches), len(api.resources.creates)) == before


@pytest.mark.parametrize('field,value', [('uid', UUID('aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa')),
    ('resource_version', '28'), ('sha256', 'e' * 64)])
def test_cutover_recovery_cannot_replace_the_qualified_collector_credential(cutover_inputs, tmp_path, field, value):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    run(request, tokens, api, tmp_path)
    before = (len(api.patches), len(api.resources.creates))
    altered = replace(request, collector_credential=request.collector_credential.model_copy(update={field: value}))
    api.request = altered  # The anchored parent, not the transport double, must reject replay drift.
    with pytest.raises(ValueError):
        run(altered, tokens, api, tmp_path)
    assert (len(api.patches), len(api.resources.creates)) == before


@pytest.mark.parametrize("boundary", ["initial", "producer", "retirement", "runtime", "complete"])
def test_runtime_preflight_derives_only_original_or_journal_qualified_workloads(cutover_inputs, tmp_path, boundary):
    from scripts.ops.nebius_management_switch import _matches
    from scripts.ops.nebius_pool_cutover import retained_cutover_workloads

    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    if boundary == "producer":
        api.busy.add(_key(request.services[0]))
    elif boundary == "retirement":
        api.retirement.busy.add(_key(request.fencing.retirement.actuators[0]))
    elif boundary == "runtime":
        api.fail_key = _key(request.fencing.retirement.migration.guards[0].controller)
        api.failure = "conflict"
    if boundary != "initial":
        run(request, tokens, api, tmp_path)
    qualified = retained_cutover_workloads(request, state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "cutover-anchor")
    assert set(qualified) == set(api.documents)
    for key, document in qualified.items():
        assert _matches(api.documents[key], document, api.documents[key]["metadata"]["uid"])
    assert (tmp_path / "cutover/cutover.json").exists() is (boundary != "initial")


@pytest.mark.parametrize("damage", ["missing_anchor", "wrong_anchor", "missing_parent", "missing_retirement",
    "changed_retirement", "unrecorded_runtime", "changed_expected"])
def test_runtime_preflight_never_treats_a_state_file_as_recovery_authority(cutover_inputs, tmp_path, damage):
    from scripts.ops.nebius_pool_cutover import retained_cutover_workloads

    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    run(request, tokens, api, tmp_path)
    parent = tmp_path / "cutover/cutover.json"
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    marker = tmp_path / "cutover-anchor" / (operation + "-cutover.json")
    if damage == "missing_anchor":
        marker.unlink()
    elif damage == "wrong_anchor":
        marker.write_text('{}')
    elif damage == "missing_parent":
        parent.unlink()
    elif damage == "missing_retirement":
        (tmp_path / "cutover/writer-anchor" / (operation + "-retirement.json")).unlink()
    elif damage == "changed_retirement":
        (tmp_path / "cutover/writers/retirement.json").write_text('{}')
    else:
        saved = json.loads(parent.read_text())
        if damage == "unrecorded_runtime":
            saved['fenced'] = None
        else:
            saved['runtime'][_key(request.manager)]['expected']['spec']['replicas'] = 1
        parent.write_text(json.dumps(saved))
    with pytest.raises(ValueError):
        retained_cutover_workloads(request, state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "cutover-anchor")


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


@pytest.mark.parametrize("boundary", ["producer", "runtime"])
def test_definite_preview_conflict_returns_pending_without_a_write_or_intent(
        cutover_inputs, tmp_path, boundary):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    preview = api.preview_workload
    key = _key(request.manager if boundary == "producer" else request.fencing.retirement.migration.guards[0].controller)
    calls = []

    def conflict(actual_key, before, desired):
        if actual_key == key:
            calls.append(actual_key)
            return None  # Validated dry-run API rejection, not an uncertain write.
        return preview(actual_key, before, desired)

    api.preview_workload = conflict
    for index in range(2):
        result = run(request, tokens, api, tmp_path)
        assert result["status"] == "pending_" + boundary + "_update"
        assert len(calls) == index + 1
        assert key not in api.patches
        record = json.loads((tmp_path / "cutover/cutover.json").read_text())
        assert record["producers" if boundary == "producer" else "runtime"][key] == {"phase": "prepared", "expected": None}
    api.preview_workload = preview
    assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
    assert api.patches.count(key) == (2 if boundary == "producer" else 1)


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


@pytest.mark.parametrize("damage", [None, "namespace", "uid", "running", "foreign_template", "redirect",
    "preview_conflict", "preview_invalid", "preview_malformed", "preview_timeout"])
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
        if damage in {"preview_conflict", "preview_invalid", "preview_malformed", "preview_timeout"}:
            assert dict(message.url.params) == {"dryRun": "All"}
            if damage == "preview_timeout":
                raise httpx.ReadTimeout("test-only-marker")
            code = 409 if damage == "preview_conflict" else 422
            return httpx.Response(code, json={"apiVersion": "v1", "kind": "Status", "status": "Failure",
                "code": code, "reason": "Conflict" if code == 409 or damage == "preview_malformed" else "Invalid"})
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
            guards=SimpleNamespace(request=migration, guard=external.migration.guard), checks=external, history=external, api_server="https://cluster.example",
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
        if damage in {"preview_conflict", "preview_invalid"}:
            assert api.preview_workload(key, before, desired) is None
        elif damage in {"preview_malformed", "preview_timeout"}:
            with pytest.raises(ValueError):
                api.preview_workload(key, before, desired)
        elif damage:
            with pytest.raises(ValueError):
                api.patch_workload(key, before, desired)
        else:
            assert api.patch_workload(key, before, desired) is True
        patches = [row for row in calls if row.method == "PATCH"]
        assert len(patches) == (1 if damage in {None, "redirect", "preview_conflict", "preview_invalid",
            "preview_malformed", "preview_timeout"} else 0)


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
            guards=SimpleNamespace(request=migration), checks=external, history=external, api_server="https://cluster.example",
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
        return {'status': 'observed', 'schema_revision': '0171' if damage == 'schema' else '0174', 'rows': [{
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
            guards=guards, checks=external, history=external, api_server='https://cluster.example',
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


def test_https_cutover_refuses_a_history_reader_bound_to_another_manager(cutover_inputs):
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI

    request, tokens = cutover_inputs
    external = CutoverAPI(request)
    migration = request.fencing.retirement.migration

    def reject_binding(actual, manager):
        assert actual == migration and manager == request.manager
        raise ValueError('misbound management history')

    history = SimpleNamespace(qualify_binding=reject_binding, qualify_pending_origins=lambda *_: None)
    with pytest.raises(ValueError, match='misbound'):
        with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=external.migration,
            guards=SimpleNamespace(request=migration), checks=external, history=history,
            api_server='https://cluster.example', ssl_context=ssl.create_default_context()):
            pass
    assert external.events == [] and external.patches == []


@pytest.fixture
def cutover_binding_inventory(cutover_inputs):
    """Complete external RBAC collections, including unrelated native authority."""
    from scripts.ops.nebius_pool_runtime import participant_readonly_roles

    request, _ = cutover_inputs
    migration = request.fencing.retirement.migration
    inventories = {"roles": list(copy.deepcopy(request.fencing.originals)),
        "clusterroles": [], "rolebindings": [], "clusterrolebindings": []}
    for document in participant_readonly_roles(request=migration):
        if document["kind"] != "RoleBinding":
            continue
        document["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        inventories["rolebindings"].append(document)
    for row in request.platform_authority.resources:
        inventories["clusterroles" if row["kind"] == "ClusterRole" else "clusterrolebindings"].append(copy.deepcopy(row))
    inventories["clusterroles"].append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": "system:discovery", "uid": str(uuid4()), "resourceVersion": "1"},
        "rules": [{"nonResourceURLs": ["/api", "/apis", "/version"], "verbs": ["get"]},
            {"apiGroups": ["authorization.k8s.io"], "resources": ["selfsubjectrulesreviews"], "verbs": ["create"]}]})
    inventories["clusterrolebindings"].append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": "system:discovery", "uid": str(uuid4()), "resourceVersion": "1"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:discovery"},
        "subjects": [{"kind": "Group", "name": "system:authenticated", "apiGroup": "rbac.authorization.k8s.io"}]})
    return inventories


def writer_workload_inventory(request, *, originals=None):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    rows = {resource: [] for resource in ("deployments", "replicasets", "statefulsets", "daemonsets",
        "replicationcontrollers", "cronjobs", "jobs", "pods")}
    if originals is None:
        originals = (*retirement_documents(request.fencing.retirement).values(), request.manager, *request.services)
    for document in originals:
        rows["cronjobs" if document["kind"] == "CronJob" else "deployments"].append(copy.deepcopy(document))
    return rows


def writer_descendant(parent, kind, *, name=None):
    """A typed Kubernetes owner chain, not a label-based permission exemption."""
    template = (parent["spec"]["jobTemplate"]["spec"]["template"] if parent["kind"] == "CronJob"
        else parent["spec"]["template"])
    return {"apiVersion": "v1" if kind == "Pod" else "batch/v1" if kind == "Job" else "apps/v1", "kind": kind,
        "metadata": {"name": name or parent["metadata"]["name"] + "-child", "namespace": parent["metadata"]["namespace"],
            "uid": str(uuid4()), "resourceVersion": "1", "ownerReferences": [{"apiVersion": parent["apiVersion"],
                "kind": parent["kind"], "name": parent["metadata"]["name"], "uid": parent["metadata"]["uid"],
                "controller": True, "blockOwnerDeletion": True}]},
        "spec": copy.deepcopy(template["spec"]) if kind == "Pod" else {"template": copy.deepcopy(template)}}


def binding_preflight(request, tokens, inventories, *, page_mode=None, qualified_hook=None, journal=None,
                      workloads=None, workload_page_mode=None, database_read=None, history_read=None):
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI

    migration = request.fencing.retirement.migration
    namespaces = {migration.registration.binding.namespace: migration.registration.binding.namespace_uid,
        "kube-system": migration.registration.binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}}
    kinds = {"roles": "Role", "clusterroles": "ClusterRole", "rolebindings": "RoleBinding",
        "clusterrolebindings": "ClusterRoleBinding"}
    workload_kinds = {"deployments": "Deployment", "replicasets": "ReplicaSet", "statefulsets": "StatefulSet",
        "daemonsets": "DaemonSet", "replicationcontrollers": "ReplicationController", "cronjobs": "CronJob",
        "jobs": "Job", "pods": "Pod"}
    workloads = writer_workload_inventory(request) if workloads is None else workloads
    calls = []

    def respond(message):
        calls.append(message)
        assert message.method == "GET", "inventory qualification must not persist any mutation"
        if message.url.path.startswith("/api/v1/namespaces/"):
            name = message.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": namespaces[name], "labels": {
                    "loom.nebius/management-installation": migration.registration.binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        resource = message.url.path.rsplit("/", 1)[-1]
        version = ("rbac.authorization.k8s.io/v1" if resource in kinds else
            "v1" if resource in {"pods", "replicationcontrollers"} else
            "batch/v1" if resource in {"jobs", "cronjobs"} else "apps/v1")
        assert message.url.path == ("/api/" if version == "v1" else "/apis/") + version + "/" + resource
        continuation = message.url.params.get("continue")
        assert dict(message.url.params) == {"limit": "100", **({"continue": "next"} if continuation else {}),
            **({"resourceVersion": "7", "resourceVersionMatch": "Exact"} if resource != "roles" and not continuation else {})}
        metadata = {"resourceVersion": "7"}
        entries = inventories[resource] if resource in kinds else workloads[resource]
        if page_mode and resource == "rolebindings":
            if not continuation:
                metadata["continue"], entries = "next", entries[:1]
            else:
                entries = entries[1:]
                if page_mode == "changed_version":
                    metadata["resourceVersion"] = "8"
                elif page_mode == "repeated_token":
                    metadata["continue"] = "next"
        if workload_page_mode and resource == "pods":
            if not continuation:
                metadata["continue"], entries = "next", entries[:1]
            else:
                entries = entries[1:]
                if workload_page_mode == "changed_version":
                    metadata["resourceVersion"] = "8"
                elif workload_page_mode == "repeated_token":
                    metadata["continue"] = "next"
        return httpx.Response(200, json={"apiVersion": version,
            "kind": (kinds | workload_kinds)[resource] + "List", "metadata": metadata, "items": entries})

    external = CutoverAPI(request)
    def empty_page(target, *, after):
        assert target in migration.guards and after is None
        return {"status": "observed", "schema_revision": "0174", "rows": []}
    def empty_history(target, origins):
        assert target in migration.guards and origins == ()
    external.qualify_pending_origins = history_read or empty_history
    if qualified_hook is not None:
        def checked_preflight(actual):
            assert actual == request
            qualified_hook()
        external.preflight = checked_preflight
    with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=external.migration,
            guards=SimpleNamespace(request=migration, cutover_readiness_page=database_read or empty_page), checks=external, history=external,
            api_server="https://cluster.example", ssl_context=ssl.create_default_context(),
            state_dir=journal[0] if journal else None, anchor_dir=journal[1] if journal else None) as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(respond))
        api.preflight(request)
    return calls


@pytest.mark.parametrize("damage", [None, "schema", "active_access", "unknown_origin", "history"])
def test_preflight_qualifies_every_database_before_producer_downtime(cutover_inputs, cutover_binding_inventory, damage):
    request, tokens = cutover_inputs
    migration = request.fencing.retirement.migration
    seen = []
    def page(target, *, after):
        assert target in migration.guards and after is None
        seen.append(("database", target.participant_id))
        if damage == "active_access":
            raise ValueError("pool cutover application access active")
        rows = [{"key": "batch:" + str(uuid4()), "source_matches": True, "origin": None}] if damage == "unknown_origin" else []
        return {"status": "observed", "schema_revision": "0171" if damage == "schema" else "0174", "rows": rows}
    def history(target, origins):
        assert target in migration.guards and origins == ()
        seen.append(("history", target.participant_id))
        if damage == "history":
            raise ValueError("pool management history schema unqualified")
    if damage:
        with pytest.raises(ValueError):
            binding_preflight(request, tokens, cutover_binding_inventory, database_read=page, history_read=history)
    else:
        binding_preflight(request, tokens, cutover_binding_inventory, database_read=page, history_read=history)
        assert seen == [(kind, row.participant_id) for row in migration.guards for kind in ("database", "history")]


@pytest.mark.parametrize("damage", ["foreign_subject", "extra_named_grant", "cluster_group",
    "cross_namespace_grant", "unresolved", "role_drift", "duplicate", "missing_role", "missing_binding",
    "foreign_scoped_writer", "namespace_group", "serviceaccount_user", "aggregated_reader"])
def test_connected_cutover_rejects_unqualified_retained_writer_bindings_before_downtime(
        cutover_inputs, cutover_binding_inventory, damage):
    request, tokens = cutover_inputs
    rows = cutover_binding_inventory
    binding = rows["rolebindings"][0]
    if damage == "foreign_subject":
        binding["subjects"].append({"kind": "ServiceAccount", "name": "foreign-writer", "namespace": "foreign"})
    elif damage in {"extra_named_grant", "cross_namespace_grant", "foreign_scoped_writer"}:
        extra = copy.deepcopy(binding)
        extra["metadata"].update(name="unexpected", uid=str(uuid4()))
        if damage == "cross_namespace_grant":
            extra["metadata"]["namespace"] = "foreign"
        elif damage == "foreign_scoped_writer":
            extra["subjects"] = [{"kind": "ServiceAccount", "namespace": "foreign", "name": "unknown-writer"}]
        extra["roleRef"].update(kind="ClusterRole", name="unexpected-named-writer")
        rows["rolebindings"].append(extra)
        rows["clusterroles"].append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
            "metadata": {"name": "unexpected-named-writer", "uid": str(uuid4()), "resourceVersion": "1"},
            "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["patch"],
                "resourceNames": ["one-named-job"]}]})
    elif damage in {"cluster_group", "namespace_group", "serviceaccount_user"}:
        subject = binding["subjects"][0]
        kind, name = "Group", "system:serviceaccounts"
        if damage == "namespace_group":
            name += ":" + subject["namespace"]
        elif damage == "serviceaccount_user":
            kind, name = "User", "system:serviceaccount:" + subject["namespace"] + ":" + subject["name"]
        extra = copy.deepcopy(rows["clusterrolebindings"][0])
        extra["metadata"].update(name="unexpected-subject-grant", uid=str(uuid4()))
        extra["subjects"] = [{"kind": kind, "name": name, "apiGroup": "rbac.authorization.k8s.io"}]
        rows["clusterrolebindings"].append(extra)
    elif damage == "unresolved":
        binding["roleRef"]["name"] = "missing"
    elif damage == "role_drift":
        rows["roles"][0]["rules"][0]["verbs"].append("patch")
    elif damage == "duplicate":
        rows["roles"].append(copy.deepcopy(rows["roles"][0]))
    elif damage == "missing_role":
        rows["roles"].pop(0)
    elif damage == "aggregated_reader":
        rows["clusterroles"][-1]["aggregationRule"] = {"clusterRoleSelectors": [{"matchLabels": {"foreign": "true"}}]}
    else:
        rows["rolebindings"].pop(0)
    with pytest.raises(ValueError, match=r"pool.*binding"):
        binding_preflight(request, tokens, rows)


@pytest.mark.parametrize("page_mode", ["complete", "late_foreign", "changed_version", "repeated_token"])
def test_connected_writer_binding_inventory_cannot_qualify_only_the_first_page(
        cutover_inputs, cutover_binding_inventory, page_mode):
    request, tokens = cutover_inputs
    if page_mode == "late_foreign":
        cutover_binding_inventory["rolebindings"][-1]["subjects"].append({
            "kind": "ServiceAccount", "namespace": "foreign", "name": "late-writer"})
    if page_mode == "complete":
        binding_preflight(request, tokens, cutover_binding_inventory, page_mode=page_mode)
    else:
        with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory, page_mode=page_mode)


@pytest.mark.parametrize("restricted", ["first", "all"])
def test_binding_preflight_accepts_exact_reader_roles_during_journaled_parent_recovery(
        cutover_inputs, cutover_binding_inventory, restricted):
    from scripts.ops.nebius_pool_role_fencing import role_fence_documents

    request, tokens = cutover_inputs
    targets = role_fence_documents(request.fencing)
    rows = cutover_binding_inventory["roles"]
    for index, original in enumerate(rows):
        if index == 0 or restricted == "all":
            rows[index] = {**copy.deepcopy(targets[_key(original)]), "metadata": {
                **copy.deepcopy(targets[_key(original)]["metadata"]), "uid": original["metadata"]["uid"], "resourceVersion": "2"}}
    binding_preflight(request, tokens, cutover_binding_inventory)


def test_connected_binding_preflight_preserves_unrelated_native_and_operator_authority(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs
    calls = binding_preflight(request, tokens, cutover_binding_inventory)
    assert {row.url.path.rsplit("/", 1)[-1] for row in calls
        if row.url.path.startswith("/apis/rbac.authorization.k8s.io/v1/")} == {
            "roles", "clusterroles", "rolebindings", "clusterrolebindings"}


@pytest.mark.parametrize("scope,verbs,named,rejected", [
    ("execution", ["create"], False, True),
    ("build", ["patch"], True, True),
    ("cluster", ["update"], False, True),
    ("execution", ["delete"], True, True),
    ("execution", ["deletecollection"], False, True),
    ("execution", ["*"], False, True),
    ("execution", ["get", "list", "watch"], False, False),
    ("foreign", ["create", "patch"], False, False),
])
def test_foreign_cronjob_permissions_cannot_bypass_the_pool_job_writer_boundary(
        cutover_inputs, cutover_binding_inventory, scope, verbs, named, rejected):
    request, tokens = cutover_inputs
    participant = request.fencing.retirement.migration.registration.spec.participants[0]
    namespace = {"execution": participant.execution_namespace.name,
        "build": participant.build_namespace.name, "cluster": None, "foreign": "unrelated"}[scope]
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": "foreign-cron-producer", "uid": str(uuid4()), "resourceVersion": "1"},
        "rules": [{"apiGroups": ["batch"], "resources": ["cronjobs"], "verbs": verbs,
            **({"resourceNames": ["retained-schedule"]} if named else {})}]}
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding" if namespace is None else "RoleBinding",
        "metadata": {"name": "foreign-cron-producer", "uid": str(uuid4()), "resourceVersion": "1",
            **({"namespace": namespace} if namespace is not None else {})},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "foreign-cron-producer"},
        "subjects": [{"kind": "ServiceAccount", "namespace": "unrelated", "name": "cron-producer"}]}
    cutover_binding_inventory["clusterroles"].append(role)
    cutover_binding_inventory["clusterrolebindings" if namespace is None else "rolebindings"].append(binding)
    if rejected:
        with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory)
    else:
        binding_preflight(request, tokens, cutover_binding_inventory)


@pytest.mark.parametrize("scope,suspended,rejected", [
    ("execution", False, True), ("execution", True, True),
    ("build", False, True), ("build", True, True),
    ("foreign", False, False), ("foreign", True, False),
])
def test_existing_foreign_cronjob_cannot_create_pool_jobs_through_native_controller(
        cutover_inputs, cutover_binding_inventory, scope, suspended, rejected):
    request, tokens = cutover_inputs
    participant = request.fencing.retirement.migration.registration.spec.participants[0]
    namespace = {"execution": participant.execution_namespace.name,
        "build": participant.build_namespace.name, "foreign": "unrelated"}[scope]
    inventories = writer_workload_inventory(request)
    producer = copy.deepcopy(request.fencing.retirement.collectors[0])
    producer["metadata"].update(name="unregistered-cron-producer", namespace=namespace, uid=str(uuid4()))
    producer["spec"]["suspend"] = suspended
    producer["spec"]["jobTemplate"]["spec"]["template"]["spec"]["serviceAccountName"] = "unregistered-account"
    inventories["cronjobs"].append(producer)
    before = copy.deepcopy(inventories)
    if rejected:
        with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory, workloads=inventories)
    else:
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=inventories)
    assert inventories == before  # Never adopt, suspend or delete unknown producers.


@pytest.mark.parametrize("declared", [False, True])
def test_writer_inventory_requires_explicit_dormant_roster(cutover_inputs, cutover_binding_inventory, declared):
    from tests.ops.test_nebius_pool_dormant import dormant_consumer

    request, tokens = cutover_inputs
    consumer = dormant_consumer(request.fencing.retirement)
    workloads = writer_workload_inventory(request)
    workloads["deployments"].append(copy.deepcopy(consumer.actuator))
    workloads["cronjobs"].append(copy.deepcopy(consumer.collector))
    before = copy.deepcopy(workloads)
    if declared:
        request = replace(request, fencing=replace(request.fencing,
            retirement=replace(request.fencing.retirement, dormant_consumers=(consumer,))))
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=workloads)
    else:
        with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory, workloads=workloads)
    assert workloads == before


@pytest.mark.parametrize("boundary", ["foreign_owner", "management_producer"])
def test_retained_binding_gate_does_not_adopt_or_reduce_unrelated_authority(
        cutover_inputs, cutover_binding_inventory, boundary):
    request, tokens = cutover_inputs
    rows = cutover_binding_inventory
    if boundary == "foreign_owner":
        rows["clusterroles"][0]["metadata"]["ownerReferences"] = [{"apiVersion": "v1", "kind": "ConfigMap",
            "name": "foreign-controller", "uid": str(uuid4()), "controller": True}]
    else:
        namespace = request.manager["metadata"]["namespace"]
        account = request.manager["spec"]["template"]["spec"]["serviceAccountName"]
        rows["roles"].append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
            "metadata": {"name": "management", "namespace": namespace, "uid": str(uuid4()), "resourceVersion": "1"},
            "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create", "delete"]}]})
        rows["rolebindings"].append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
            "metadata": {"name": "management", "namespace": namespace, "uid": str(uuid4()), "resourceVersion": "1"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "management"},
            "subjects": [{"kind": "ServiceAccount", "namespace": namespace, "name": account}]})
    if boundary == "foreign_owner":
        with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
            binding_preflight(request, tokens, rows)
    else:
        binding_preflight(request, tokens, rows)


def test_binding_drift_during_other_preflight_checks_cannot_reach_producer_downtime(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs

    def add_foreign_subject():
        cutover_binding_inventory["rolebindings"][0]["subjects"].append({
            "kind": "ServiceAccount", "namespace": "foreign", "name": "racing-writer"})

    with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory, qualified_hook=add_foreign_subject)


@pytest.mark.parametrize("damage", [None, "unrecorded", "anchor", "role_uid", "foreign_subject", "receipt", "intent",
    "intent_extra_rules", "intent_extra_subject", "intent_aggregation"])
def test_gateway_writer_permissions_require_actual_retained_cutover_stage_proof(
        cutover_inputs, cutover_binding_inventory, tmp_path, damage):
    from scripts.ops.nebius_pool_cutover import cutover_documents

    request, tokens = cutover_inputs
    external = CutoverAPI(request)
    assert run(request, tokens, external, tmp_path)["status"] == "pool_runtime_staged_closed"
    rows = cutover_binding_inventory
    rows["roles"] = list(copy.deepcopy(external.fencing.roles).values())
    resources = {"Role": "roles", "RoleBinding": "rolebindings",
        "ClusterRole": "clusterroles", "ClusterRoleBinding": "clusterrolebindings"}
    for document in cutover_documents(request)["authority"]:
        rows[resources[document["kind"]]].append(copy.deepcopy(external.resources.resources[_key(document)]))
    journal = (tmp_path / "cutover", tmp_path / "cutover-anchor")
    if damage == "unrecorded":
        journal = None
    elif damage == "anchor":
        marker, = (tmp_path / "cutover-anchor").glob("*-cutover.json")
        payload = json.loads(marker.read_text())
        payload["contract_sha256"] = "sha256:" + "0" * 64
        marker.write_text(json.dumps(payload))
    elif damage == "role_uid":
        rows["roles"][-1]["metadata"]["uid"] = str(uuid4())
    elif damage == "foreign_subject":
        rows["rolebindings"][-1]["subjects"].append({"kind": "ServiceAccount", "namespace": "foreign", "name": "forged"})
    elif damage in {"receipt", "intent", "intent_extra_rules", "intent_extra_subject", "intent_aggregation"}:
        parent = tmp_path / "cutover" / "cutover.json"
        path = tmp_path / "cutover" / "authority" / "stage.json"
        record, stage = json.loads(parent.read_text()), json.loads(path.read_text())
        kind = "ClusterRole" if damage == "intent_aggregation" else "Role" if damage == "intent_extra_rules" else "RoleBinding"
        key, item = next((key, value) for key, value in stage["resources"].items() if key.startswith(kind + ":"))
        item.update(status="create_intent", uid=None, observed=None)
        if damage in {"intent_extra_rules", "intent_extra_subject"}:
            actual = next(row for row in rows[resources[kind]] if _key(row) == key)
            field, extra = ("rules", {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}) if kind == "Role" else (
                "subjects", {"kind": "ServiceAccount", "namespace": "foreign", "name": "forged-successor"})
            actual[field].append(copy.deepcopy(extra))
            item["expected"][field].append(copy.deepcopy(extra))
        elif damage == "intent_aggregation":
            actual = next(row for row in rows[resources[kind]] if _key(row) == key)
            actual["aggregationRule"] = item["expected"]["aggregationRule"] = {
                "clusterRoleSelectors": [{"matchLabels": {"foreign": "true"}}]}
        path.write_text(json.dumps(stage))
        if damage != "receipt":
            # Simulate interruption after the API CREATE, before its UID receipt.
            record["phases"]["authority"] = None
            parent.write_text(json.dumps(record))
    if damage in {None, "intent"}:
        before = {path: path.read_bytes() for path in (tmp_path / "cutover").rglob("*.json")}
        binding_preflight(request, tokens, rows, journal=journal,
            workloads=writer_workload_inventory(request, originals=external.documents.values()))
        assert {path: path.read_bytes() for path in before} == before
    else:
        with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
            binding_preflight(request, tokens, rows, journal=journal)


@pytest.mark.parametrize("resource,kind", [("deployments", "Deployment"), ("replicasets", "ReplicaSet"),
    ("statefulsets", "StatefulSet"), ("daemonsets", "DaemonSet"), ("replicationcontrollers", "ReplicationController"),
    ("cronjobs", "CronJob"), ("jobs", "Job"), ("pods", "Pod")])
def test_retiring_writer_identity_cannot_be_shared_with_unregistered_workload(
        cutover_inputs, cutover_binding_inventory, resource, kind):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    original = request.fencing.retirement.actuators[0]
    template = copy.deepcopy(original["spec"]["template"])
    spec = {"template": template, "replicas": 0}
    if kind == "CronJob":
        spec = {"suspend": True, "jobTemplate": {"spec": {"template": template}}}
    elif kind == "Pod":
        spec = template["spec"]
    elif kind == "Job":
        spec = {"suspend": True, "template": template}
    rows[resource].append({"apiVersion": "v1" if kind in {"Pod", "ReplicationController"}
        else "batch/v1" if kind in {"Job", "CronJob"} else "apps/v1", "kind": kind,
        "metadata": {"name": "unregistered", "namespace": original["metadata"]["namespace"],
            "uid": str(uuid4()), "resourceVersion": "1"}, "spec": spec,
        "status": {"phase": "Succeeded"}})
    with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("damage", [None, "owner_uid", "owner_name", "owner_kind", "owner_version", "not_controller",
    "second_owner", "foreign_namespace", "wrong_account", "missing_parent", "cycle", "direct_pod"])
def test_only_complete_typed_retained_writer_descendant_lineage_qualifies(
        cutover_inputs, cutover_binding_inventory, damage):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    replica = writer_descendant(request.fencing.retirement.actuators[0], "ReplicaSet")
    pod = writer_descendant(replica, "Pod")
    rows["replicasets"].append(replica)
    rows["pods"].append(pod)
    owner = pod["metadata"]["ownerReferences"][0]
    if damage == "owner_uid":
        owner["uid"] = str(uuid4())
    elif damage == "owner_name":
        owner["name"] = "different-name"
    elif damage == "owner_kind":
        owner["kind"] = "Job"
    elif damage == "owner_version":
        owner["apiVersion"] = "batch/v1"
    elif damage == "not_controller":
        owner["controller"] = False
    elif damage == "second_owner":
        pod["metadata"]["ownerReferences"].append(copy.deepcopy(owner))
    elif damage == "foreign_namespace":
        replica["metadata"]["namespace"] = "foreign"
    elif damage == "wrong_account":
        replica["spec"]["template"]["spec"]["serviceAccountName"] = "foreign-account"
    elif damage == "missing_parent":
        rows["replicasets"].clear()
    elif damage == "cycle":
        replica["metadata"]["ownerReferences"] = copy.deepcopy(pod["metadata"]["ownerReferences"])
    elif damage == "direct_pod":
        pod["metadata"]["ownerReferences"] = copy.deepcopy(replica["metadata"]["ownerReferences"])
    if damage is None:
        calls = binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)
        assert {row.url.path.rsplit("/", 1)[-1] for row in calls} >= set(rows)
        assert all("fieldSelector" not in row.url.params for row in calls if row.url.path == "/api/v1/pods")
    else:
        with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


def test_retained_collector_job_history_is_inventoried_without_requiring_deletion(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    job = writer_descendant(request.fencing.retirement.collectors[0], "Job")
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
    pod = writer_descendant(job, "Pod")
    pod["status"] = {"phase": "Succeeded"}
    rows["jobs"].append(job)
    rows["pods"].append(pod)
    binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("damage", ["missing_root", "replaced_root", "drifted_root", "unanchored_stopped_root",
    "duplicate_uid", "duplicate_key", "invalid_account"])
def test_writer_workload_inventory_requires_exact_retained_roots_and_unique_objects(
        cutover_inputs, cutover_binding_inventory, damage):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    original = rows["deployments"][0]
    if damage == "missing_root":
        rows["deployments"].pop(0)
    elif damage == "replaced_root":
        original["metadata"]["uid"] = str(uuid4())
    elif damage == "drifted_root":
        original["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign.invalid/image:latest"
    elif damage == "unanchored_stopped_root":
        original["spec"]["replicas"] = 0
    elif damage in {"duplicate_uid", "duplicate_key"}:
        duplicate = copy.deepcopy(original)
        duplicate["metadata"]["name" if damage == "duplicate_uid" else "uid"] = "duplicate" if damage == "duplicate_uid" else str(uuid4())
        rows["deployments"].append(duplicate)
    else:
        pod = writer_descendant(writer_descendant(original, "ReplicaSet"), "Pod")
        pod["spec"]["serviceAccountName"] = []
        rows["pods"].append(pod)
    with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("mode", ["complete", "late_foreign", "changed_version", "repeated_token"])
def test_writer_workload_inventory_cannot_ignore_terminal_pods_or_later_pages(
        cutover_inputs, cutover_binding_inventory, mode):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    replica = writer_descendant(request.fencing.retirement.actuators[0], "ReplicaSet")
    rows["replicasets"].append(replica)
    for index in range(2):
        pod = writer_descendant(replica, "Pod", name=f"historical-{index}")
        pod["status"] = {"phase": "Succeeded"}
        rows["pods"].append(pod)
    if mode == "late_foreign":
        rows["pods"][-1]["metadata"].pop("ownerReferences")
    if mode == "complete":
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows, workload_page_mode=mode)
    else:
        with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
            binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows, workload_page_mode=mode)


def test_retained_identity_workload_drift_during_preflight_is_rejected(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)

    def unexpected_controller():
        extra = copy.deepcopy(request.fencing.retirement.actuators[0])
        extra["metadata"].update(name="concurrent-writer", uid=str(uuid4()))
        extra["spec"]["replicas"] = 0
        rows["deployments"].append(extra)

    with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows, qualified_hook=unexpected_controller)


def test_writer_workload_inventory_preserves_foreign_identities_even_with_equal_account_names(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    extra = copy.deepcopy(request.fencing.retirement.actuators[0])
    extra["metadata"].update(namespace="unrelated-physical-pool", uid=str(uuid4()))
    rows["deployments"].append(extra)
    binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("template", [{}, {"template": None}])
def test_foreign_replication_controller_without_a_pod_template_is_not_a_writer_consumer(
        cutover_inputs, cutover_binding_inventory, template):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    # A legal adopt-only ReplicationController has no Pod-creation identity.
    # Any Pods it adopts are still covered by the complete Pod collection.
    rows["replicationcontrollers"].append({"apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"namespace": "foreign", "name": "adopt-only", "uid": str(uuid4()), "resourceVersion": "1"},
        "spec": {"selector": {"legacy": "retained"}, "replicas": 0, **template}})
    binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("subject", [
    {"kind": "ServiceAccount", "namespace": "foreign", "name": "unregistered-controller"},
    {"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "unregistered-automation"},
    {"kind": "Group", "apiGroup": "rbac.authorization.k8s.io", "name": "unregistered-writers"},
])
def test_unregistered_cluster_wide_job_writer_cannot_bypass_participant_inventory(
        cutover_inputs, cutover_binding_inventory, subject):
    request, tokens = cutover_inputs
    extra = copy.deepcopy(cutover_binding_inventory["clusterrolebindings"][0])
    extra["metadata"].update(name="unregistered-cluster-writer", uid=str(uuid4()))
    extra["subjects"] = [subject]
    cutover_binding_inventory["clusterrolebindings"].append(extra)
    with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory)


def test_native_controller_identity_cannot_be_borrowed_by_an_in_cluster_workload(
        cutover_inputs, cutover_binding_inventory):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    borrowed = copy.deepcopy(request.fencing.retirement.actuators[0])
    borrowed["metadata"].update(namespace="kube-system", name="borrowed-controller", uid=str(uuid4()))
    borrowed["spec"]["replicas"] = 0
    borrowed["spec"]["template"]["spec"]["serviceAccountName"] = "job-controller"
    rows["deployments"].append(borrowed)
    with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)


@pytest.mark.parametrize("damage", ["missing_authority", "cluster", "role_uid", "binding_uid", "rules",
    "subjects", "role_ref", "missing_role", "missing_binding", "alias_binding", "aggregation"])
def test_platform_authority_requires_retained_live_objects_not_only_native_names(
        cutover_inputs, cutover_binding_inventory, damage):
    request, tokens = cutover_inputs
    rows = cutover_binding_inventory
    binding_preflight(request, tokens, rows)
    role, binding = rows["clusterroles"][1], rows["clusterrolebindings"][1]
    if damage == "missing_authority":
        request = replace(request, platform_authority=None)
    elif damage == "cluster":
        request = replace(request, platform_authority=request.platform_authority.model_copy(update={"kube_system_uid": uuid4()}))
    elif damage == "role_uid":
        role["metadata"]["uid"] = str(uuid4())
    elif damage == "binding_uid":
        binding["metadata"]["uid"] = str(uuid4())
    elif damage == "rules":
        role["rules"][0]["verbs"].append("create")
    elif damage == "subjects":
        binding["subjects"].append({"kind": "ServiceAccount", "name": "unexpected", "namespace": "kube-system"})
    elif damage == "role_ref":
        binding["roleRef"]["name"] = "cluster-admin"
    elif damage == "missing_role":
        rows["clusterroles"].remove(role)
    elif damage == "missing_binding":
        rows["clusterrolebindings"].remove(binding)
    elif damage == "alias_binding":
        extra = copy.deepcopy(binding)
        extra["metadata"].update(name="native-looking-alias", uid=str(uuid4()))
        rows["clusterrolebindings"].append(extra)
    else:
        role["aggregationRule"] = {"clusterRoleSelectors": [{"matchLabels": {"foreign": "true"}}]}
    with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
        binding_preflight(request, tokens, rows)


@pytest.mark.parametrize("damage", ["native_create", "native_wildcard", "native_secrets", "native_aggregation",
    "native_subject", "native_role_ref", "bootstrap_label", "admin_subject", "admin_role_ref", "admin_alias",
    "unreferenced_role", "duplicate_uid", "owned", "deleting", "zero_cluster"])
def test_platform_input_rejects_privilege_broadening_even_when_retained_as_an_original(cutover_inputs, damage):
    from scripts.ops.nebius_pool_platform_authority import PoolPlatformAuthority

    request, _ = cutover_inputs
    payload = request.platform_authority.model_dump(mode="json")
    PoolPlatformAuthority.model_validate(payload)
    admin, admin_binding, role, binding = payload["resources"]
    if damage == "native_create":
        role["rules"][0]["verbs"].append("create")
    elif damage == "native_wildcard":
        role["rules"][0]["verbs"] = ["*"]
    elif damage == "native_secrets":
        role["rules"].append({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]})
    elif damage == "native_aggregation":
        role["aggregationRule"] = {"clusterRoleSelectors": [{"matchLabels": {"foreign": "true"}}]}
    elif damage == "native_subject":
        binding["subjects"][0]["namespace"] = "foreign"
    elif damage == "native_role_ref":
        binding["roleRef"]["name"] = "cluster-admin"
    elif damage == "bootstrap_label":
        role["metadata"]["labels"].clear()
    elif damage == "admin_subject":
        admin_binding["subjects"][0]["name"] = "system:authenticated"
    elif damage == "admin_role_ref":
        admin_binding["roleRef"]["name"] = role["metadata"]["name"]
    elif damage == "admin_alias":
        admin_binding["metadata"]["name"] = "foreign-admin"
    elif damage == "unreferenced_role":
        payload["resources"].remove(binding)
    elif damage == "duplicate_uid":
        role["metadata"]["uid"] = admin["metadata"]["uid"]
    elif damage == "owned":
        role["metadata"]["ownerReferences"] = [{"apiVersion": "v1", "kind": "ConfigMap", "name": "foreign", "uid": str(uuid4())}]
    elif damage == "deleting":
        role["metadata"]["deletionTimestamp"] = "2026-10-01T00:00:00Z"
    else:
        payload["kube_system_uid"] = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(ValueError, match="pool_platform_authority_unqualified"):
        PoolPlatformAuthority.model_validate(payload)


def test_cutover_replay_cannot_replace_its_retained_platform_authority(cutover_inputs, tmp_path):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    assert run(request, tokens, api, tmp_path)["status"] == "pool_runtime_staged_closed"
    authority = request.platform_authority.model_copy(deep=True)
    authority.resources[0]["metadata"]["uid"] = str(uuid4())
    changed = replace(request, platform_authority=authority)
    api = CutoverAPI(changed)
    with pytest.raises(ValueError):
        run(changed, tokens, api, tmp_path)
    assert not api.patches and not api.retirement.patches and not api.resources.creates
