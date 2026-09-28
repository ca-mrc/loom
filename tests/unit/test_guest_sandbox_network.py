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
