"""Failed refresh inspection is bounded, read-only and credential-free."""
from __future__ import annotations

import base64
import copy
import json
from uuid import uuid4

import pytest
from scripts.ops import nebius_management_preflight as preflight
from tests.ops.test_nebius_management_preflight import Cluster
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_refresh_resources import (
    documents,
)
from tests.ops.test_nebius_management_refresh_resources import (
    resources_request as resources_request,
)
from tests.unit.test_nebius_management_refresh_probe import db_url
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    management_inputs as management_inputs,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

from loom.nebius_management_refresh_probe import RefreshProbeSettings


class FailedRefresh(Cluster):
    def __init__(self, request, *, driver="postgresql"):
        super().__init__()
        self.cm, self.job = documents(request, "shared-probe")
        namespace = self.config["namespace"]
        assert self.job["metadata"]["namespace"] == namespace
        for doc in (self.cm, self.job):
            doc["metadata"].update(uid=str(uuid4()), resourceVersion="42")
        self.job["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
        template = copy.deepcopy(self.job["spec"]["template"])
        self.pod = {**template, "metadata": {**template["metadata"], "namespace": namespace,
            "name": self.job["metadata"]["name"] + "-abcde", "uid": str(uuid4()),
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "controller": True,
                "name": self.job["metadata"]["name"], "uid": self.job["metadata"]["uid"]}]},
            "status": {"phase": "Failed", "containerStatuses": [{
                "name": template["spec"]["containers"][0]["name"],
                "state": {"terminated": {"exitCode": 1, "reason": "Error", "message": "private-error"}},
            }]}}
        self.lists["pods"].append(self.pod)
        settings = RefreshProbeSettings.model_validate_json(self.cm["data"]["probe.json"])
        self.url = db_url(settings).set(drivername=driver)
        self.raw_log = 'private-prefix\n{"schema":"loom.nebius-management-refresh-probe.v1","status":"unqualified","secret":"private-log"}'
        self.broken_secret = False

    def get(self, kind, name, namespace):
        for wanted_kind, doc in (("job", self.job), ("configmap", self.cm)):
            if (kind, name, namespace) == (wanted_kind, doc["metadata"]["name"], doc["metadata"]["namespace"]):
                self.calls.append(("get", kind, name, namespace))
                return copy.deepcopy(doc)
        if (kind, name, namespace) == ("secret", "loom-platform-db", "loom-nebius-platform"):
            self.calls.append(("get", kind, name, namespace))
            if self.broken_secret:
                raise ValueError("private-credential-error")
            return {"metadata": {"name": name, "namespace": namespace}, "data": {
                "service-url": base64.b64encode(self.url.render_as_string(hide_password=False).encode()).decode(),
                "admin-url": "private-admin-url"}}
        return super().get(kind, name, namespace)

    def run(self, *args, **kwargs):
        if args[0] == "logs":
            self.calls.append(args)
            assert args == ("logs", self.pod["metadata"]["name"], "-n", "loom-nebius-platform", "-c",
                self.pod["spec"]["containers"][0]["name"], "--tail=50", "--limit-bytes=16384")
            assert kwargs == {"timeout": 40}
            return self.raw_log
        return super().run(*args, **kwargs)


def inspect(cluster):
    return preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")


@pytest.mark.parametrize("driver,raw,normalized", [("postgresql", True, False), ("postgresql+psycopg", False, True)])
def test_inspection_distinguishes_url_driver_without_exposing_credentials(resources_request, driver, raw, normalized):
    cluster = FailedRefresh(resources_request, driver=driver)
    result = inspect(cluster)
    item, = result["failed_refresh_probes"]
    assert item["job_uid"] == cluster.job["metadata"]["uid"]
    assert item["pod_uid"] == cluster.pod["metadata"]["uid"]
    assert item["termination"] == {"exit_code": 1, "reason": "Error"}
    assert item["diagnostic"] == {"status": "unqualified"}
    assert item["current_url"] == {"status": "observed_current", "checks": {
        "raw_postgresql_driver": raw, "psycopg_driver": normalized, "service_role": True,
        "password_present": True, "namespace_host": True, "port": True, "database": True, "tls_query": True,
    }}
    encoded = json.dumps(result)
    assert "private-" not in encoded and "not-a-real-password" not in encoded
    assert all(call[0] in {"get", "logs", "config"} for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["owner", "job_uid", "marker", "command", "namespace", "not_failed"])
def test_unbound_job_never_reads_credentials_or_logs(resources_request, mutation):
    cluster = FailedRefresh(resources_request)
    if mutation == "owner":
        cluster.pod["metadata"]["ownerReferences"][0]["controller"] = False
    elif mutation == "job_uid":
        cluster.job["metadata"]["uid"] = str(uuid4())
    elif mutation == "marker":
        cluster.job["metadata"]["annotations"]["loom.nebius/management-refresh-id"] = str(uuid4())
    elif mutation == "command":
        cluster.pod["spec"]["containers"][0]["command"].append("arbitrary")
    elif mutation == "namespace":
        cluster.pod["metadata"]["namespace"] = "foreign-namespace"
    else:
        cluster.job["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
    assert inspect(cluster)["failed_refresh_probes"] == []
    assert not any(call[0] == "logs" or call[:2] == ("get", "secret") for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["mutable", "settings_namespace", "secret_ref", "mount", "oversize", "unreadable"])
def test_unqualified_url_material_does_not_hide_failed_job(resources_request, mutation):
    cluster = FailedRefresh(resources_request)
    if mutation == "mutable":
        cluster.cm["immutable"] = False
    elif mutation == "settings_namespace":
        settings = json.loads(cluster.cm["data"]["probe.json"])
        settings["shared"]["platform_namespace"] = "foreign-namespace"
        cluster.cm["data"]["probe.json"] = json.dumps(settings)
    elif mutation == "secret_ref":
        for pod_spec in (cluster.job["spec"]["template"]["spec"], cluster.pod["spec"]):
            pod_spec["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"]["name"] = "foreign-secret"
    elif mutation == "mount":
        cluster.job["spec"]["template"]["spec"]["volumes"] = []
    elif mutation == "oversize":
        cluster.cm["data"]["probe.json"] = " " * 262145
    else:
        cluster.broken_secret = True
    item, = inspect(cluster)["failed_refresh_probes"]
    assert item["diagnostic"] == {"status": "unqualified"}
    assert item["current_url"] == {"status": "unavailable"}
    if mutation != "unreadable":
        assert not any(call[:2] == ("get", "secret") for call in cluster.calls)


@pytest.mark.parametrize("log,expected", [
    ("ModuleNotFoundError: private-module", {"error_type": "ModuleNotFoundError"}),
    ('{"schema":"foreign","status":"unqualified"}', {"status": "unavailable"}),
    ('{"schema":"loom.nebius-management-refresh-probe.v1","status":"qualified"}', {"status": "unavailable"}),
    ('{"schema":"loom.nebius-management-refresh-probe.v1","status":[]}', {"status": "unavailable"}),
    ("private-traceback", {"status": "unavailable"}),
])
def test_logs_are_closed_projection_not_raw_output(resources_request, log, expected):
    cluster = FailedRefresh(resources_request)
    cluster.raw_log = log
    item, = inspect(cluster)["failed_refresh_probes"]
    assert item["diagnostic"] == expected
    assert "private" not in json.dumps(item)


def test_at_most_three_failed_probes_are_inspected(resources_request):
    cluster = FailedRefresh(resources_request)
    cluster.lists["pods"] = [copy.deepcopy(cluster.pod) for _ in range(8)]
    assert len(inspect(cluster)["failed_refresh_probes"]) == 3
    assert len([call for call in cluster.calls if call[0] == "logs"]) == 3
