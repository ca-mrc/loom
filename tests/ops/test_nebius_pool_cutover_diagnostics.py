"""Disposable cutover diagnostics expose provenance, never workload contents."""
from __future__ import annotations

import copy
import json
import subprocess
from types import SimpleNamespace

import httpx
import pytest
from tests.cluster import pool_cutover_diagnostics as diagnostics
from tests.ops.test_nebius_pool_cutover import (
    CutoverAPI,
    binding_preflight,
    run,
    writer_descendant,
    writer_workload_inventory,
)
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover import runtime_inputs as runtime_inputs

PRIVATE = "do-not-print-token-or-manifest"
FOREIGN_UID = "01234567-89ab-4cde-8fab-0123456789ab"


def read_report(capsys):
    output = capsys.readouterr().out
    assert PRIVATE not in output
    assert "private-marker" not in output
    return json.loads(output.removeprefix("disposable cutover failure: "))


@pytest.mark.parametrize(("owners", "kind", "count"), [
    (None, "absent", None), ([], "list", 0),
    ({"credential": PRIVATE}, "dict", None), (PRIVATE, "str", None),
    ([{"credential": PRIVATE}, {"spec": PRIVATE}], "list", 2),
])
def test_failed_inventory_reports_actual_owner_shape_without_new_reads_or_contents(
        cutover_inputs, cutover_binding_inventory, capsys, monkeypatch, owners, kind, count):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    foreign = copy.deepcopy(request.fencing.retirement.actuators[0])
    foreign["metadata"].update(name="unregistered-writer-consumer", uid=FOREIGN_UID,
        annotations={"secret": PRIVATE})
    if owners is not None:
        foreign["metadata"]["ownerReferences"] = owners
    foreign["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "SECRET", "value": PRIVATE})
    rows["deployments"].append(foreign)
    caught = None

    def forbidden_read(*args, **kwargs):
        pytest.fail("diagnostics must use the failed snapshot, never fresh HTTP reads")

    with pytest.raises(ValueError) as result:
        with diagnostics.observe_cutover_stage(4):
            try:
                binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)
            except ValueError as error:
                caught = error
                monkeypatch.setattr(httpx.Client, "send", forbidden_read)
                raise
    assert result.value is caught
    report = read_report(capsys)
    assert report["invocation"] == 4
    assert report["snapshot_resource_version"] == "7"
    workload = report["workload"]
    assert workload["kind"] == "Deployment"
    assert workload["name"] == "unregistered-writer-consumer"
    assert workload["uid"] == FOREIGN_UID
    assert workload["namespace"] == foreign["metadata"]["namespace"]
    assert workload["service_account"] == foreign["spec"]["template"]["spec"]["serviceAccountName"]
    assert workload["owner_references_type"] == kind
    assert workload["owner_references_count"] == count
    assert any(row["function"] == "qualify_retained_writer_workloads" for row in report["locations"])
    assert all(token not in json.dumps(report) for token in tokens.values())


def test_failed_pod_ancestry_comes_from_same_snapshot(cutover_inputs, cutover_binding_inventory, capsys):
    request, tokens = cutover_inputs
    rows = writer_workload_inventory(request)
    parent = request.fencing.retirement.actuators[0]
    replica = writer_descendant(parent, "ReplicaSet")
    pod = writer_descendant(replica, "Pod")
    pod["metadata"]["ownerReferences"][0].update(controller=False, private=PRIVATE)
    rows["replicasets"].append(replica)
    rows["pods"].append(pod)
    with pytest.raises(ValueError), diagnostics.observe_cutover_stage(2):
        binding_preflight(request, tokens, cutover_binding_inventory, workloads=rows)
    report = read_report(capsys)
    assert report["workload"]["uid"] == pod["metadata"]["uid"]
    assert [row["uid"] for row in report["ancestry"]] == [
        pod["metadata"]["uid"], replica["metadata"]["uid"], parent["metadata"]["uid"]]
    assert [row["retained_root"] for row in report["ancestry"]] == [False, False, True]
    assert report["ancestry"][0]["owners"][0]["controller"] is False


def test_unexpected_preflight_rejection_retains_hidden_binding_location(
        cutover_inputs, cutover_binding_inventory, capsys):
    request, tokens = cutover_inputs
    inventory = copy.deepcopy(cutover_binding_inventory)
    inventory["rolebindings"][0]["roleRef"]["name"] = PRIVATE
    # Match the connected test's nested error-stage assertion: pytest's
    # AssertionError must retain the original, deliberately hidden failure.
    with pytest.raises(AssertionError), diagnostics.observe_cutover_stage(0):
        with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
            binding_preflight(request, tokens, inventory)
    report = read_report(capsys)
    assert report["snapshot_resource_version"] == "7"
    assert any(row["function"] == "qualify_pool_writer_bindings" for row in report["locations"])
    assert any(row["function"] == "qualify_retained_writer_bindings" for row in report["locations"])
    assert all(token not in json.dumps(report) for token in tokens.values())


def test_stage_failure_reports_only_known_journal_phases(cutover_inputs, tmp_path, capsys):
    request, tokens = cutover_inputs
    api = CutoverAPI(request)
    api.unqualified_queue = True
    with pytest.raises(ValueError), diagnostics.observe_cutover_stage(3):
        run(request, tokens, api, tmp_path)
    report = read_report(capsys)
    assert report["journal"] == {"record": "present", "producers": ["stopped"],
        "runtime": ["prepared"], "runtime_access": ["prepared"], "stages": []}
    assert report["snapshot_resource_version"] is None
    assert all(token not in json.dumps(report) for token in tokens.values())
    assert diagnostics.journal_phases({"producers": {PRIVATE: {"phase": PRIVATE}},
        "runtime": {PRIVATE: {"phase": "intent", "expected": {"secret": PRIVATE}}},
        "runtime_access": {PRIVATE: PRIVATE}, "phases": {PRIVATE: PRIVATE, "material": PRIVATE}}) == {
            "record": "present", "producers": [], "runtime": ["intent"],
            "runtime_access": [], "stages": ["material"]}


def test_diagnostic_failure_never_replaces_original_exception(monkeypatch, capsys):
    original = ValueError(PRIVATE)

    def broken(*args, **kwargs):
        raise RuntimeError(PRIVATE)

    monkeypatch.setattr(diagnostics, "cutover_failure_report", broken)
    with pytest.raises(ValueError) as result:
        with diagnostics.observe_cutover_stage(1):
            raise original
    assert result.value is original
    assert result.value.__context__ is None
    assert PRIVATE not in capsys.readouterr().out
    with diagnostics.observe_cutover_stage(2):
        pass
    assert capsys.readouterr().out == ""


def test_unrelated_exception_never_exposes_type_name_message_or_locals(capsys):
    private_exception = type(PRIVATE, (ValueError,), {})
    private_manifest = {"tokens": PRIVATE, "spec": {"credential": PRIVATE}}
    with pytest.raises(ValueError), diagnostics.observe_cutover_stage(7):
        raise private_exception(private_manifest)
    report = read_report(capsys)
    assert report == {"invocation": 7, "locations": [], "snapshot_resource_version": None,
        "journal": {"record": "unavailable"}, "workload": None, "ancestry": []}


def test_bootstrap_diagnostics_project_actual_models_and_hide_private_reader_fields(monkeypatch):
    from kubernetes import client

    roles = client.V1ClusterRoleList(metadata=client.V1ListMeta(resource_version='31'), items=[
        client.V1ClusterRole(metadata=client.V1ObjectMeta(name='clustercidrs-node', annotations={'token': PRIVATE}),
            rules=[client.V1PolicyRule(verbs=['get'], resources=[PRIVATE])])])
    bindings = client.V1ClusterRoleBindingList(metadata=client.V1ListMeta(resource_version='31'), items=[
        client.V1ClusterRoleBinding(metadata=client.V1ObjectMeta(name='clustercidrs-node'),
            role_ref=client.V1RoleRef(api_group='rbac.authorization.k8s.io', kind='ClusterRole', name='clustercidrs-node'),
            subjects=[client.RbacV1Subject(kind='User', name=PRIVATE)]),
        client.V1ClusterRoleBinding(metadata=client.V1ObjectMeta(name='system:k3s-controller'),
            role_ref=client.V1RoleRef(api_group=PRIVATE, kind='ClusterRole', name=PRIVATE))])
    addon = {'metadata': {'generation': 3, 'annotations': {'token': PRIVATE}}, 'spec': {'source': PRIVATE},
        'status': {'observedGeneration': 2, 'private': PRIVATE, 'conditions': [
            {'type': 'Ready', 'status': 'False', 'message': PRIVATE}, {'type': PRIVATE, 'status': PRIVATE}]}}
    events = {'items': [{'involvedObject': {'kind': 'Addon', 'name': 'rolebindings'},
        'reason': reason, 'count': index, 'message': PRIVATE, 'source': {'host': PRIVATE}}
        for index, reason in enumerate(('ApplyingManifest', 'AppliedManifest', 'ApplyManifestFailed', PRIVATE))]}
    calls = []

    def read(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, json.dumps(addon if len(calls) == 1 else events), PRIVATE)

    monkeypatch.setattr(diagnostics.subprocess, 'run', read)
    error = AssertionError('bootstrap unresolved')
    diagnostics.add_bootstrap_rbac_diagnostics(SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id='fixture')),
        roles, bindings, error)
    note, = error.__notes__
    assert PRIVATE not in note
    report = json.loads(note.removeprefix('disposable bootstrap RBAC: '))
    assert report == {'roles_resource_version': '31', 'bindings_resource_version': '31',
        'targets': {'clustercidrs-node': {'role_present': True, 'binding_present': True,
            'binding_role_ref_present': True}, 'system:k3s-controller': {'role_present': False,
            'binding_present': True, 'binding_role_ref_present': False}},
        'addon': {'generation': 3, 'observed_generation': 2,
            'conditions': [{'type': 'Ready', 'status': 'False'}, {'type': None, 'status': None}]},
        'events': [{'reason': 'ApplyingManifest', 'count': 0}, {'reason': 'AppliedManifest', 'count': 1},
            {'reason': 'ApplyManifestFailed', 'count': 2}]}
    assert len(calls) == 2
    for command, kwargs in calls:
        assert command[:8] == ['docker', 'exec', 'fixture', 'kubectl', '--request-timeout=2s', '-n', 'kube-system', 'get']
        assert kwargs == {'capture_output': True, 'text': True, 'timeout': 3}


@pytest.mark.parametrize('failure', ['exception', 'returncode'])
def test_bootstrap_reader_failures_keep_original_error_and_bounded_single_reads(monkeypatch, failure):
    calls = []

    def broken(command, **kwargs):
        calls.append((command, kwargs))
        if failure == 'returncode':
            return subprocess.CompletedProcess(command, 7, PRIVATE, PRIVATE)
        if len(calls) == 1:
            raise subprocess.TimeoutExpired(PRIVATE, 3, output=PRIVATE, stderr=PRIVATE)
        raise RuntimeError(PRIVATE)

    monkeypatch.setattr(diagnostics.subprocess, 'run', broken)
    cluster = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id='fixture'))
    rows = SimpleNamespace(metadata=SimpleNamespace(resource_version=PRIVATE), items=[])
    original = AssertionError('bootstrap unresolved')
    with pytest.raises(AssertionError) as caught:
        try:
            raise original
        except AssertionError as error:
            diagnostics.add_bootstrap_rbac_diagnostics(cluster, rows, rows, error)
            raise
    assert caught.value is original and original.__context__ is None
    note, = original.__notes__
    assert PRIVATE not in note
    report = json.loads(note.removeprefix('disposable bootstrap RBAC: '))
    assert report['roles_resource_version'] is report['bindings_resource_version'] is None
    assert report['addon'] == ({'error': 'TimeoutExpired'} if failure == 'exception' else {'returncode': 7})
    assert report['events'] == ({'error': 'RuntimeError'} if failure == 'exception' else {'returncode': 7})
    assert len(calls) == 2
    assert all('--request-timeout=2s' in command and kwargs['timeout'] == 3 for command, kwargs in calls)
