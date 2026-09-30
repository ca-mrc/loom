"""Installed migration discovery must expose references, not credential payloads."""
from __future__ import annotations

import json

import pytest
from scripts.ops import nebius_management_preflight as preflight
from tests.ops.test_nebius_management_preflight import Cluster


def metadata(name, namespace=None):
    return {"name": name, "uid": name + "-uid", "resourceVersion": "21",
            **({"namespace": namespace} if namespace else {})}


class Installed(Cluster):
    def __init__(self):
        super().__init__()
        self.maps = {("execution", "collector-config"): {"metadata": metadata("collector-config", "execution"), "data": {
            "LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID": "native-cpu",
            "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID": "mk8snodegroup-test",
            "LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_BEARER_TOKEN": "private-cm-token"}}}
        for name, target in (("standard-actuator", "native-cpu"), ("guest-actuator", "guest-cpu")):
            self.lists["deployments"].append({"metadata": metadata(name, "execution"), "spec": {
                "replicas": 1, "template": {"spec": {"serviceAccountName": "actuator", "containers": [{
                    "name": "actuator", "env": [
                        {"name": "LOOM_EXECUTION_ACTUATOR_TARGET_ID", "value": target},
                        {"name": "LOOM_EXECUTION_ACTUATOR_NAMESPACE", "value": "execution"},
                        {"name": "LOOM_EXECUTION_ACTUATOR_DB_URL", "valueFrom": {
                            "secretKeyRef": {"name": "database", "key": "actuator-url"}}},
                        {"name": "LOOM_EXECUTION_ACTUATOR_PRIVATE_TOKEN", "value": "private-token"}],
                    "command": ["private-command"], "args": ["private-argument"],
                }]}}}})
        self.lists["deployments"].append({"metadata": metadata("control", "platform"), "spec": {
            "replicas": 1, "template": {"spec": {"containers": [{"name": "control", "env": [
                {"name": "LOOM_CP_DB_URL", "value": "postgresql://user:private-password@db/dev"},
                {"name": "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT", "value": "development"}],
            }]}}}})
        self.lists["cronjobs"] = [{"metadata": metadata("collector", "execution"), "spec": {
            "suspend": False, "jobTemplate": {"spec": {"template": {"spec": {"serviceAccountName": "collector", "containers": [{
                "name": "collector", "envFrom": [{"configMapRef": {"name": "collector-config"}}],
            }]}}}}}}]
        self.lists["roles"] = [{"metadata": metadata("writer", "execution"), "rules": [
            {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "create", "delete"]}]}]
        self.lists["rolebindings"] = [{"metadata": metadata("writer", "execution"), "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "writer"},
            "subjects": [{"kind": "ServiceAccount", "name": "actuator", "namespace": "execution"}]}]

    def get(self, kind, name, namespace):
        if (namespace, name) in self.maps:
            self.calls.append(("get", kind, name, namespace))
            assert kind == "configmap"
            return self.maps[namespace, name]
        return super().get(kind, name, namespace)


def observe(cluster):
    return preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")["controller_inventory"]


def test_discovers_guest_target_shared_database_reference_and_collector_group_without_secret_reads():
    cluster = Installed()
    report = observe(cluster)
    controllers = {row["name"]: row for row in report["controllers"]}
    for name, target in (("standard-actuator", "native-cpu"), ("guest-actuator", "guest-cpu")):
        row = controllers[name]
        assert row["kind"] == "Deployment" and row["replicas"] == 1
        assert row["service_account"] == "actuator"
        assert row["containers"][0]["settings"]["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] == {"value": target}
        assert row["containers"][0]["settings"]["LOOM_EXECUTION_ACTUATOR_DB_URL"] == {
            "secret_ref": {"namespace": "execution", "name": "database", "key": "actuator-url"}}
    assert controllers["control"]["containers"][0]["settings"]["LOOM_CP_DB_URL"] == {"value_withheld": True}
    assert controllers["collector"]["containers"][0]["settings"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID"] == {
        "value": "mk8snodegroup-test"}
    assert report["job_write_bindings"] == [{"name": "writer", "namespace": "execution", "uid": "writer-uid",
        "resource_version": "21", "kind": "RoleBinding", "role": {"kind": "Role", "name": "writer", "uid": "writer-uid"},
        "subjects": [{"kind": "ServiceAccount", "name": "actuator", "namespace": "execution"}],
        "rules": [{"verbs": ["create", "delete"], "resource_names": []}]}]
    assert "private-" not in json.dumps(report)
    assert all(call[0] in {"get", "config"} and call[1] != "secret" for call in cluster.calls)
    assert "resolved_database_identity" in report["unverified"]


def test_reports_group_cluster_grants_and_unresolved_roles_instead_of_claiming_exclusive_writer():
    cluster = Installed()
    cluster.lists["clusterroles"] = [{"metadata": metadata("broad"), "rules": [
        {"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}]}]
    cluster.lists["clusterrolebindings"] = [{"metadata": metadata("broad"), "roleRef": {
        "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "broad"},
        "subjects": [{"kind": "Group", "name": "system:serviceaccounts:execution"}]}]
    cluster.lists["rolebindings"].append({"metadata": metadata("missing", "execution"), "roleRef": {
        "apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "missing"}, "subjects": []})
    report = observe(cluster)
    broad, = [row for row in report["job_write_bindings"] if row["name"] == "broad"]
    assert broad["kind"] == "ClusterRoleBinding" and "namespace" not in broad
    assert broad["subjects"] == [{"kind": "Group", "name": "system:serviceaccounts:execution"}]
    assert broad["rules"][0]["verbs"] == ["create", "delete", "deletecollection", "patch", "update"]
    assert report["unresolved_bindings"] == [{"name": "missing", "namespace": "execution", "uid": "missing-uid",
        "resource_version": "21", "kind": "RoleBinding"}]
    assert "effective_writer_fencing" in report["unverified"]


def test_job_status_and_read_only_grants_are_not_reported_as_job_writers():
    cluster = Installed()
    cluster.lists["roles"][0]["rules"] = [
        {"apiGroups": ["batch"], "resources": ["jobs/status"], "verbs": ["update"]},
        {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch"]}]
    assert observe(cluster)["job_write_bindings"] == []


@pytest.mark.parametrize("order,explicit,expected", [
    ("config-secret", False, {"value_withheld": True}),
    ("secret-config", False, {"value": "native-cpu"}),
    ("config-secret", True, {"value": "explicit-target"}),
])
def test_unread_secret_env_from_never_preserves_a_shadowed_config_target(order, explicit, expected):
    cluster = Installed()
    pod = cluster.lists["cronjobs"][0]["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    container = pod["containers"][0]
    source = container["envFrom"][0]
    secret = {"secretRef": {"name": "collector-secrets"}}
    container["envFrom"] = [source, secret] if order == "config-secret" else [secret, source]
    name = "LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID"
    if explicit:
        container["env"] = [{"name": name, "value": "explicit-target"}]
    observed = next(row for row in observe(cluster)["controllers"] if row["name"] == "collector")["containers"][0]
    assert observed["settings"][name] == expected
    assert observed["unresolved_secret_env_from"] == [{"namespace": "execution", "name": "collector-secrets", "prefix": ""}]


def test_cluster_role_reused_by_role_binding_keeps_its_namespace_scope():
    cluster = Installed()
    cluster.lists["clusterroles"] = [{"metadata": metadata("common"), "rules": [
        {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create"]}]}]
    cluster.lists["rolebindings"][0]["roleRef"].update(kind="ClusterRole", name="common")
    writer, = observe(cluster)["job_write_bindings"]
    assert writer["kind"] == "RoleBinding" and writer["namespace"] == "execution"
    assert writer["role"] == {"kind": "ClusterRole", "name": "common", "uid": "common-uid"}
    assert writer["rules"] == [{"verbs": ["create"], "resource_names": []}]


@pytest.mark.parametrize("resource", ["deployments", "cronjobs", "roles", "rolebindings", "clusterroles", "clusterrolebindings"])
def test_missing_controller_or_permission_list_is_never_an_empty_success(resource):
    class Broken(Installed):
        def run(self, *args, **kwargs):
            if args[:2] == ("get", resource):
                return json.dumps({"kind": "Status", "message": "private-api-error"})
            return super().run(*args, **kwargs)

    with pytest.raises(preflight.DeploymentError):
        observe(Broken())


def test_partial_paginated_list_is_not_a_complete_writer_inventory():
    class Partial(Installed):
        def run(self, *args, **kwargs):
            result = json.loads(super().run(*args, **kwargs))
            if args[:2] == ("get", "rolebindings"):
                result["metadata"] = {"continue": "private-page-token"}
            return json.dumps(result)

    with pytest.raises(preflight.DeploymentError):
        observe(Partial())
