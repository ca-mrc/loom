"""Exact image CAS and HTTPS transport matrix, independently shardable."""
from __future__ import annotations

import copy
import json

import pytest
from tests.ops.test_nebius_pool_manager_image_history import (
    ImageAPI,
    entry,
    selected_image_case,
    switch,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    checks as checks,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    historical_cutover as historical_cutover,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    image_repair_case as image_repair_case,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    installation as installation,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    material as material,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    prepared_repair as prepared_repair,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_manager_image_history import (
    runtime_inputs as runtime_inputs,
)


@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
@pytest.mark.parametrize("phase", ["isolate", "stop", "template", "start"])
@pytest.mark.parametrize("loss", ["before", "after"])
def test_manager_image_switch_reconciles_lost_cas_without_duplicate_write(image_repair_case, phase, loss, target):
    image_repair_case = selected_image_case(image_repair_case, target)
    api = ImageAPI(image_repair_case)
    api.failure = (phase, loss)
    result = switch(image_repair_case, api)
    if loss == "before":
        assert result["status"] == "pending_manager_image_outcome"
        calls = list(api.calls)
        assert switch(image_repair_case, api)["status"] == "pending_manager_image_outcome"
        assert api.calls == calls
        api.deliver()
    assert switch(image_repair_case, api)["status"] == "pool_manager_image_repaired_closed"
    assert api.calls == ["isolate", "stop", "template", "start"]
    assert all(row["phase"] == "applied" for row in entry(image_repair_case).record["phases"].values())



@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
@pytest.mark.parametrize("loss", [None, "before", "after"])
def test_https_image_switch_sends_only_exact_uid_version_metadata_spec_cas(image_repair_case, loss, target):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from scripts.ops.nebius_pool_manager_image_live import HTTPSPoolManagerImageAPI
    from scripts.ops.nebius_pool_runtime_image import runtime_image_template

    image_repair_case = selected_image_case(image_repair_case, target)
    context, remote, state, anchor, binding = image_repair_case
    original = entry(image_repair_case).documents[0]
    key = _key(original)
    cron = original["kind"] == "CronJob"
    prefix = "/apis/batch/v1/namespaces/" if cron else "/apis/apps/v1/namespaces/"
    workload = prefix + original["metadata"]["namespace"] + ("/cronjobs/" if cron else "/deployments/") + original["metadata"]["name"]
    template_path = "/spec/jobTemplate/spec/template" if cron else "/spec/template"
    field = "suspend" if cron else "replicas"
    component = "execution_actuator" if cron else "service"
    writes = []

    def respond(message):
        path = message.url.path
        if message.method == "GET":
            if path == workload:
                actual = remote.startup.documents[key]
                actual["metadata"]["generation"] = 1
                actual["status"] = {} if cron else {"observedGeneration": 1, "replicas": actual["spec"]["replicas"]}
                return httpx.Response(200, json=actual)
            assert path.endswith("/jobs" if cron else "/replicasets") or path.endswith("/pods")
            return httpx.Response(200, json={"apiVersion": "v1" if path.endswith("/pods") else "batch/v1" if cron else "apps/v1",
                "kind": "PodList" if path.endswith("/pods") else "JobList" if cron else "ReplicaSetList",
                "metadata": {"resourceVersion": "100"}, "items": []})
        assert message.method == "PATCH" and path == workload
        before = remote.read_workload(key)
        patches = json.loads(message.content)
        assert patches[:4] == [
            {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/metadata", "value": before["metadata"]},
            {"op": "test", "path": "/spec", "value": before["spec"]}]
        change, desired = patches[4], copy.deepcopy(before)
        if change["path"] == template_path:
            phase = "template"
            assert before["spec"][field] == (True if cron else 0)
            expected = copy.deepcopy(runtime_image_template(before))
            for container in (*expected["spec"]["containers"], *expected["spec"]["initContainers"]):
                container["image"] = binding.candidate["images"][component]["image_ref"]
            assert change["value"] == expected
            if cron:
                desired["spec"]["jobTemplate"]["spec"]["template"] = expected
            else:
                desired["spec"]["template"] = expected
        elif change["path"] == "/metadata/annotations":
            phase = "isolate"
            assert change["op"] == "add" and len(patches) == 5
            desired["metadata"]["annotations"] = {**before["metadata"].get("annotations", {}),
                "loom.nebius/manager-image-repair": str(binding.operation_id)}
            assert change["value"] == desired["metadata"]["annotations"]
        else:
            assert change["path"] == "/spec/" + field
            phase = "stop" if change["value"] == (True if cron else 0) else "start"
            desired["spec"][field] = change["value"]
        if phase == "start":
            assert patches[5:] == [{"op": "remove", "path": "/metadata/annotations/loom.nebius~1manager-image-repair"}]
            del desired["metadata"]["annotations"]["loom.nebius/manager-image-repair"]
        elif phase != "isolate":
            assert len(patches) == 5 and change["op"] == "replace"
        if message.url.params:
            assert dict(message.url.params) == {"dryRun": "All"}
        else:
            assert entry(image_repair_case).record["phases"][phase]["phase"] == "intent"
            writes.append(phase)
            if phase == "template" and loss == "before":
                raise httpx.ReadTimeout("synthetic lost request")
            desired["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 1)
            remote.startup.documents[key] = desired
            if phase == "template" and loss == "after":
                raise httpx.ReadTimeout("synthetic lost response")
        return httpx.Response(200, json=desired)

    class TransportOnlyImage(HTTPSPoolManagerImageAPI):
        def qualify_closed(self):
            self._qualify_binding()
            remote.qualify_closed()

    with httpx.Client(base_url="https://kubernetes.invalid", transport=httpx.MockTransport(respond)) as client:
        parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor,
            client=client, _scope=lambda: None, error_type=ValueError)
        parent._request = lambda method, path, **kwargs: ManagementKubernetesTransport._request(parent, method, path, **kwargs)
        api = TransportOnlyImage(parent=parent, binding=binding)
        result = switch(image_repair_case, api)
        assert result["status"] == ("pending_manager_image_outcome" if loss == "before" else "pool_manager_image_repaired_closed")
        assert switch(image_repair_case, api) == result
        assert writes == (["isolate", "stop", "template"] if loss == "before" else ["isolate", "stop", "template", "start"])

