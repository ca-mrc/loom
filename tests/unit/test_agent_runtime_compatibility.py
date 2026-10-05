"""Published bridge compatibility, including immutable historical snapshots."""

from pathlib import Path

import httpx
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverError
from loom.service_execution_materialization import (
    compile_service_execution_plan,
    freeze_agent_runtime_releases,
    runtime_profile_rejections,
)
from tests.support.agent_runtime import release
from tests.support.strict_health_published_bridge import StrictHealthPublishedBridge
from tests.unit.test_service_execution_materialization import _profile, _provenance, _task, _trial
from tests.unit.test_service_sandbox_driver import driver_for

OLD_BRIDGE = "44dbda72dff90fde5c29b094227db6c5ee03389b"


@pytest.mark.asyncio
@pytest.mark.parametrize("health", [
    {"ready": True, "instance_id": "a" * 32},
    {"ready": False, "instance_id": "a" * 32},
    {"instance_id": "a" * 32},
    {"ready": 1, "instance_id": "a" * 32},
    [],
])
async def test_exact_published_start_failure_and_current_bridge_health(
    monkeypatch, tmp_path: Path, health,
):
    # Exercise the archived start method and current method with the same HTTP
    # response. Current Unix-socket RPC and Go incarnation tests cover transport.
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda **kwargs: httpx.MockTransport(
        lambda request: httpx.Response(200, json=health),
    ))
    driver = driver_for(tmp_path / "rpc.sock")
    with pytest.raises(DriverError, match="readiness response invalid"):
        await StrictHealthPublishedBridge.start(driver)
    assert driver._client is None
    if isinstance(health, dict) and health.get("ready") is True:
        await ServiceSandboxDriver.start(driver)
        await driver.stop()
    else:
        with pytest.raises(DriverError, match="readiness response invalid"):
            await ServiceSandboxDriver.start(driver)
        assert driver._client is None


def test_catalog_marks_the_exact_old_bridge_unavailable_without_changing_its_binding():
    runtime = release("harbor-0.18.0-nebius-44dbda72-b").model_copy(
        update={"publisher_source_revision": OLD_BRIDGE},
    )
    original = runtime.model_dump(mode="json")
    metadata = runtime.public_metadata()
    assert metadata["readiness_status"] == "unavailable"
    assert runtime.agent_version in metadata["readiness_message"]
    assert "instance_id" in metadata["readiness_message"]
    assert "choose a compatible published version" in metadata["readiness_message"]
    assert runtime.model_dump(mode="json") == original
    assert "agent_image_ref" not in metadata
    assert release().public_metadata()["readiness_status"] == "ready"


def test_frozen_old_bridge_is_rejected_before_plan_compilation():
    runtime = release().model_copy(update={"publisher_source_revision": OLD_BRIDGE})
    profile = freeze_agent_runtime_releases(_profile(), (runtime,))
    trial = _trial().model_copy(update={
        "agent_name": "terminus-2", "agent_version": runtime.agent_version,
    })
    assert runtime_profile_rejections(_task(), trial, profile) == (
        "agent_runtime_bridge_incompatible",
    )
    with pytest.raises(ValueError, match="agent_runtime_bridge_incompatible"):
        compile_service_execution_plan(
            task=_task(), trial=trial, profile=profile,
            task_revision_sha256="sha256:" + "c" * 64, source_provenance=_provenance(),
        )


def test_independent_compatible_version_still_selects_its_exact_image():
    runtime = release("independently-published")
    profile = freeze_agent_runtime_releases(_profile(), (runtime,))
    trial = _trial().model_copy(update={
        "agent_name": "terminus-2", "agent_version": runtime.agent_version,
    })
    assert runtime_profile_rejections(_task(), trial, profile) == ()
    plan = compile_service_execution_plan(
        task=_task(), trial=trial, profile=profile,
        task_revision_sha256="sha256:" + "c" * 64, source_provenance=_provenance(),
    )
    assert plan.agent_image_ref == runtime.agent_image_ref
    assert profile.agent_image_ref is None
