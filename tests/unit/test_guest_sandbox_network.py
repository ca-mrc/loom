"""Guest commands retain the same authorized proxy across the VM boundary."""

import pytest

from loom.models.task import TaskConfig
from loom.service_execution_sandbox_task import sandbox_driver
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("guest", [False, True])
def test_task_proxy_address_follows_sandbox_network_namespace(monkeypatch, guest):
    task, _, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["baseline_network_policy"] = {
        "kind": "web-allowlist", "destinations": [{"host": "example.org", "protocol": "https"}],
    }
    if guest:
        raw["environment"]["execution_requirements"] = {"capabilities": ["nested_docker"]}
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:18791")
    driver = sandbox_driver("task-sandbox", TaskConfig.model_validate(raw))
    expected = "http://10.0.2.2:18791" if guest else "http://127.0.0.1:18791"
    assert driver._command_environment["HTTP_PROXY"] == expected
    assert driver._command_environment["https_proxy"] == expected


@pytest.mark.parametrize("limit", [str(6 * 1024**3), "0", "invalid", str(11 * 1024**3), ""])
def test_guest_transfer_limit_requires_bounded_runtime_authority(monkeypatch, limit):
    from loom.service_execution_task import ServiceExecutionTaskError

    task, _, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["execution_requirements"] = {"capabilities": ["singularity_mounts"]}
    monkeypatch.setenv("LOOM_SANDBOX_MAX_TRANSFER_BYTES", limit)
    if limit == str(6 * 1024**3):
        assert sandbox_driver("task-sandbox", TaskConfig.model_validate(raw))._max_transfer == int(limit)
    else:
        with pytest.raises(ServiceExecutionTaskError, match="guest_transfer_limit_invalid"):
            sandbox_driver("task-sandbox", TaskConfig.model_validate(raw))
