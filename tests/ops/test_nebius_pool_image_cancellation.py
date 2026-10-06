"""Complete image cancellation/fencing matrix, independently shardable."""
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
@pytest.mark.parametrize("late_commit", [False, True])
# Targeted cases requalify two image entries through all successor shutdowns.
@pytest.mark.timeout(180)
def test_cancellation_fences_image_cas_before_successor_shutdown(image_repair_case, phase, late_commit, target):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    image_repair_case = selected_image_case(image_repair_case, target)
    context, remote, state, anchor, _ = image_repair_case
    key = _key(entry(image_repair_case).documents[0])
    image_api = ImageAPI(image_repair_case)
    image_api.failure = (phase, "before")
    assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    old_version = image_api.pending[0]["metadata"]["resourceVersion"]
    if late_commit:
        image_api.deliver()
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)["status"] == "pool_activation_cancelled"
    assert fence_pool_startup(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)["status"] == "startup_writes_fenced"
    assert api.read_workload(key)["metadata"]["resourceVersion"] != old_version
    assert api.fence_calls == ([] if late_commit else [key])
    assert stop_pool_successors(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)["status"] == "pool_successors_stopped"
    with pytest.raises(ValueError):
        switch(image_repair_case, image_api)



@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
def test_https_cancellation_fences_image_intent_not_completed_source_repair(image_repair_case, target):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_startup import closed_startup_documents
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    image_repair_case = selected_image_case(image_repair_case, target)
    context, remote, state, anchor, _ = image_repair_case
    original = entry(image_repair_case).documents[0]
    key = _key(original)
    path = ("/cronjobs/" if original["kind"] == "CronJob" else "/deployments/") + original["metadata"]["name"]
    image_api = ImageAPI(image_repair_case)
    image_api.failure = ("template", "before")
    assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)["status"] == "pool_activation_cancelled"
    writes = []

    def respond(message):
        before = remote.read_workload(key)
        patches = json.loads(message.content)
        assert message.method == "PATCH" and message.url.path == path
        assert patches[:4] == [
            {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/metadata", "value": before["metadata"]},
            {"op": "test", "path": "/spec", "value": before["spec"]}]
        assert len(patches) == 5 and patches[-1]["path"] == "/metadata/annotations"
        desired = copy.deepcopy(before)
        desired["metadata"]["annotations"] = patches[-1]["value"]
        if not message.url.params:
            writes.append(patches)
            desired["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 1)
            remote.startup.documents[key] = desired
        return httpx.Response(200, json=desired)

    with httpx.Client(base_url="https://kubernetes.invalid", transport=httpx.MockTransport(respond)) as client:
        closed, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
        adapter = SimpleNamespace(request=context.request, state=state, anchor=anchor, closed=closed, targets=targets,
            parent=SimpleNamespace(client=client), _scope=lambda: None, _path=lambda key: path,
            verify_retained=api.verify_retained, pool_state=api.pool_state, guard_state=api.guard_state)
        api.preview_startup_fence = lambda key, before, desired: (copy.deepcopy(desired)
            if HTTPSPoolActivationAPI._startup_fence_patch(adapter, key, before, desired, preview=True) else None)
        api.fence_startup = lambda key, before, desired: HTTPSPoolActivationAPI._startup_fence_patch(
            adapter, key, before, desired, preview=False)
        assert fence_pool_startup(request=context.request, api=api, state_dir=state,
            anchor_dir=anchor)["status"] == "startup_writes_fenced"
        assert len(writes) == 1

