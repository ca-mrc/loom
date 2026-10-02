"""Restrict the recorded participant roles only after observed process drain."""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx
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
        self.extra_authority = False

    def verify_readonly(self):
        if self.extra_authority:
            raise ValueError("private-marker")

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


@pytest.mark.parametrize("replay", [False, True])
def test_extra_effective_grant_blocks_fencing_even_when_recorded_roles_are_readonly(fencing_inputs, tmp_path, replay):
    api = Roles(fencing_inputs, initialize(fencing_inputs.retirement, tmp_path))
    if replay:
        run(fencing_inputs, api, tmp_path)
    api.extra_authority = True
    with pytest.raises(ValueError) as error:
        run(fencing_inputs, api, tmp_path)
    assert "private-marker" not in str(error.value)
    assert len(api.patches) == 6
    api.extra_authority = False
    assert run(fencing_inputs, api, tmp_path)["status"] == "participant_roles_restricted"
    assert len(api.patches) == 6  # Recovery observes; it does not repeat role writes.


@pytest.mark.parametrize("account", ["system:masters", "", None])
def test_malformed_retained_review_subject_is_rejected_before_any_downtime(fencing_inputs, tmp_path, account):
    request = copy.deepcopy(fencing_inputs)
    request.retirement.collectors[0]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["serviceAccountName"] = account
    api = Roles(request, initialize(request.retirement, tmp_path))
    with pytest.raises(ValueError):
        run(request, api, tmp_path)
    assert api.patches == [] and api.retirement.patches == []


def rules_review(namespace="loom-nebius-exec-0"):
    return {"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview",
        "spec": {"namespace": namespace}, "status": {"incomplete": False,
            "resourceRules": [
                {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get"]},
                {"apiGroups": [""], "resources": ["nodes", "pods"], "verbs": ["get", "list"]},
                {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get"], "resourceNames": [namespace]},
                {"apiGroups": ["authorization.k8s.io"], "resources": ["selfsubjectaccessreviews", "selfsubjectrulesreviews"], "verbs": ["create"]},
                {"apiGroups": ["authentication.k8s.io"], "resources": ["selfsubjectreviews"], "verbs": ["create"]}],
            "nonResourceRules": [{"verbs": ["get"], "nonResourceURLs": ["/api", "/apis/*", "/version"]}]}}


@pytest.mark.parametrize("echo_spec", [False, True])
def test_complete_reader_rules_include_named_reads_and_standard_self_inspection(echo_spec):
    from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules

    review = rules_review()
    if not echo_spec:
        review["spec"] = {}  # Actual Kubernetes response does not echo the request.
    qualify_pool_reader_rules(review, namespace="loom-nebius-exec-0")


def test_actual_retained_collector_rules_remain_qualified_readers(platform_inputs):
    from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules

    from loom.nebius_platform_render import build_platform

    config, candidate, profile = platform_inputs
    documents = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
    role, = [row for rows in documents.values() for row in rows if row["kind"] == "ClusterRole"
        and row["metadata"]["name"] == config["execution_namespace"] + "-collector"]
    review = rules_review()
    review["status"]["resourceRules"].extend(role["rules"])
    qualify_pool_reader_rules(review, namespace="loom-nebius-exec-0")


def test_rendered_actuator_telemetry_is_reader_only_without_node_proxy(platform_inputs):
    from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules

    from loom.nebius_platform_render import build_platform

    config, candidate, profile = platform_inputs
    documents = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
    role, = [row for rows in documents.values() for row in rows if row["kind"] == "ClusterRole"
        and row["metadata"]["name"] == config["execution_namespace"] + "-actuator-usage"]
    review = rules_review()
    review["status"]["resourceRules"].extend(role["rules"])
    qualify_pool_reader_rules(review, namespace="loom-nebius-exec-0")


@pytest.mark.parametrize("damage", ["write", "named_write", "pod_create", "deployment", "exec", "secret",
    "impersonation", "node_proxy", "token", "wildcard_resource", "wildcard_group", "wildcard_verb", "nonresource_write", "nonresource_unknown",
    "incomplete", "missing_incomplete", "false_string", "evaluation_error", "namespace", "kind", "missing_rules", "malformed_rule"])
def test_uncertain_rules_or_direct_and_indirect_writer_authority_cannot_qualify(damage):
    from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules

    review = rules_review()
    rules = review["status"]["resourceRules"]
    extras = {
        "write": {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create"]},
        "named_write": {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["patch"], "resourceNames": ["retained-job"]},
        "pod_create": {"apiGroups": [""], "resources": ["pods"], "verbs": ["create"]},
        "deployment": {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["patch"]},
        "exec": {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get"]},
        "secret": {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]},
        "impersonation": {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["impersonate"]},
        "node_proxy": {"apiGroups": [""], "resources": ["nodes/proxy"], "verbs": ["get"]},
        "token": {"apiGroups": [""], "resources": ["serviceaccounts/token"], "verbs": ["create"]},
        "wildcard_resource": {"apiGroups": [""], "resources": ["*"], "verbs": ["get"]},
        "wildcard_group": {"apiGroups": ["*"], "resources": ["pods"], "verbs": ["get"]},
        "wildcard_verb": {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["*"]},
        "malformed_rule": {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": "get"},
    }
    if damage in extras:
        rules.append(extras[damage])
    elif damage == "nonresource_write":
        review["status"]["nonResourceRules"][0]["verbs"] = ["post"]
    elif damage == "nonresource_unknown":
        review["status"]["nonResourceRules"][0]["nonResourceURLs"] = ["*"]
    elif damage in {"incomplete", "false_string"}:
        review["status"]["incomplete"] = True if damage == "incomplete" else "false"
    elif damage == "missing_incomplete":
        del review["status"]["incomplete"]
    elif damage == "evaluation_error":
        review["status"]["evaluationError"] = "private-marker"
    elif damage == "namespace":
        review["spec"]["namespace"] = "foreign"
    elif damage == "kind":
        review["kind"] = "SubjectAccessReview"
    else:
        del review["status"]["resourceRules"]
    with pytest.raises(ValueError) as error:
        qualify_pool_reader_rules(review, namespace="loom-nebius-exec-0")
    assert "private-marker" not in str(error.value)


@pytest.mark.parametrize(("failure", "dormant"), [
    (None, False), ("grant", False), ("incomplete", False), ("transport", False),
    ("oversized", False), ("namespace", False), (None, True), ("dormant_grant", True),
])
def test_https_reviews_only_retained_identities_with_real_groups_and_never_retries(fencing_inputs, failure, dormant):
    from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI
    from scripts.ops.nebius_pool_role_fencing_live import HTTPSPoolRoleFenceAPI
    from tests.ops.test_nebius_pool_retirement_live import Guards

    request = fencing_inputs
    if dormant:
        from tests.ops.test_nebius_pool_dormant import dormant_consumer

        request = replace(request, retirement=replace(request.retirement,
            dormant_consumers=(dormant_consumer(request.retirement),)))
    binding = request.retirement.migration.registration.binding
    namespaces = {binding.namespace: binding.namespace_uid,
        **{row.namespace: str(row.namespace_uid) for row in request.retirement.migration.guards},
        **{ns.name: str(ns.uid) for row in request.retirement.migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}}
    identities = {f"system:serviceaccount:loom-nebius-{component}-{index}:{account}"
        for index in range(3) for component, account in (
            ("platform", "loom-platform"), ("exec", "loom-execution-actuator"), ("exec", "loom-execution-capacity-collector"))}
    if dormant:
        identities.update({"system:serviceaccount:loom-nebius-exec-0:nebius-retained-remote-actuator",
            "system:serviceaccount:loom-nebius-exec-0:nebius-retained-remote-collector"})
    observed = []

    def respond(message):
        if message.method == "GET":
            name = message.url.path.removeprefix("/api/v1/namespaces/")
            assert "Impersonate-User" not in message.headers
            uid = binding.kube_system_uid if name == "kube-system" else namespaces[name]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": str(uuid4()) if failure == "namespace" else uid,
                "resourceVersion": "1", "labels": {"loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        assert message.method == "POST" and message.url.path == "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews"
        subject = message.headers["Impersonate-User"]
        assert subject in identities
        account_namespace = subject.split(":")[2]
        assert set(message.headers.get_list("Impersonate-Group")) == {
            "system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + account_namespace}
        body = json.loads(message.content)
        namespace = body["spec"]["namespace"]
        assert namespace in namespaces and body == {"apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectRulesReview", "spec": {"namespace": namespace}}
        observed.append((subject, namespace))
        review = rules_review(namespace)
        review["spec"] = {}
        if failure == "grant" or (failure == "dormant_grant" and subject.endswith(":nebius-retained-remote-actuator")):
            review["status"]["resourceRules"].append({"apiGroups": ["batch"], "resources": ["jobs"],
                "verbs": ["patch"], "resourceNames": ["old-job"]})
        elif failure == "incomplete":
            review["status"]["incomplete"] = True
        elif failure == "transport":
            raise httpx.ReadError("private-marker")
        elif failure == "oversized":
            return httpx.Response(201, content=b" " * (4 * 1024**2 + 1))
        return httpx.Response(201, json=review)

    with HTTPSPoolRetirementAPI(request=request.retirement, guards=Guards(request.retirement),
            api_server="https://cluster.example", ssl_context=ssl.create_default_context()) as retirement:
        retirement.client.close()
        retirement.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
        with HTTPSPoolRoleFenceAPI(request=request, retirement=retirement,
                api_server="https://cluster.example", ssl_context=ssl.create_default_context()) as api:
            api.client.close()
            api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
            if failure:
                with pytest.raises(ValueError) as error:
                    api.verify_readonly()
                assert "private-marker" not in str(error.value)
                if failure == "dormant_grant":
                    assert len(observed) > 1
                    assert observed[-1][0] == "system:serviceaccount:loom-nebius-exec-0:nebius-retained-remote-actuator"
                else:
                    assert len(observed) == (0 if failure == "namespace" else 1)
            else:
                api.verify_readonly()
                assert set(observed) == {(subject, namespace) for subject in identities for namespace in namespaces}
                assert len(observed) == len(identities) * len(namespaces)


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
