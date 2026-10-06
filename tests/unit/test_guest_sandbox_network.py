"""Guest commands retain the same authorized proxy across the VM boundary."""

import json

import pytest

from loom.models.task import TaskConfig
from loom.service_execution_sandbox_task import sandbox_driver
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("guest", [None, "declared", "forced"])
def test_task_proxy_address_follows_sandbox_network_namespace(monkeypatch, guest):
    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["baseline_network_policy"] = {
        "kind": "web-allowlist", "destinations": [{"host": "example.org", "protocol": "https"}],
    }
    if guest == "declared":
        raw["environment"]["execution_requirements"] = {"capabilities": ["nested_docker"]}
    if guest == "forced":
        # A plain guest declares no capabilities; the Trial's isolation selects it.
        trial = trial.model_copy(update={"isolation": "guest"})
    monkeypatch.setenv("LOOM_SANDBOX_MAX_TRANSFER_BYTES", str(6 * 1024**3))
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:18791")
    monkeypatch.setenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", json.dumps(
        {"kind": "web-allowlist", "destinations": [{"host": "example.org", "protocol": "https"}]},
    ))
    driver = sandbox_driver("task-sandbox", TaskConfig.model_validate(raw), trial)
    expected = "http://10.0.2.2:18791" if guest else "http://127.0.0.1:18791"
    assert driver._command_environment["HTTP_PROXY"] == expected
    assert driver._command_environment["https_proxy"] == expected
    # Pod-loopback services, including the model broker, bypass the proxy.
    bypass = "localhost,127.0.0.1,::1" + (",10.0.2.2" if guest else "")
    assert driver._command_environment["NO_PROXY"] == driver._command_environment["no_proxy"] == bypass
    assert driver._max_transfer == (6 * 1024**3 if guest else 256 * 1024 * 1024)


@pytest.mark.parametrize("limit", [str(6 * 1024**3), "0", "invalid", str(11 * 1024**3), ""])
def test_guest_transfer_limit_requires_bounded_runtime_authority(monkeypatch, limit):
    from loom.service_execution_task import ServiceExecutionTaskError

    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["execution_requirements"] = {"capabilities": ["singularity_mounts"]}
    monkeypatch.setenv("LOOM_SANDBOX_MAX_TRANSFER_BYTES", limit)
    monkeypatch.setenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", '{"kind":"gateway-only"}')
    if limit == str(6 * 1024**3):
        assert sandbox_driver("task-sandbox", TaskConfig.model_validate(raw), trial)._max_transfer == int(limit)
    else:
        with pytest.raises(ServiceExecutionTaskError, match="guest_transfer_limit_invalid"):
            sandbox_driver("task-sandbox", TaskConfig.model_validate(raw), trial)


@pytest.mark.parametrize(
    ("value", "reason"),
    [(None, "effective_network_policy_unavailable"), ("{}", "effective_network_policy_invalid")],
)
def test_sandbox_requires_valid_frozen_network_policy(monkeypatch, value, reason):
    from loom.service_execution_task import ServiceExecutionTaskError

    task, trial, _ = _inputs()
    if value is None:
        monkeypatch.delenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", raising=False)
    else:
        monkeypatch.setenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", value)
    with pytest.raises(ServiceExecutionTaskError, match=reason):
        sandbox_driver("task-sandbox", task, trial)


@pytest.mark.parametrize("guest", [False, True])
def test_installed_agent_reaches_the_broker_from_its_sandbox(monkeypatch, guest):
    from loom.hosted_harness import CODEX
    from loom.service_execution_sandbox_task import installed_agent_model_environment

    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:18790")
    environment = installed_agent_model_environment(
        CODEX, base_url_env="OPENAI_BASE_URL", api_key_env="OPENAI_API_KEY", guest=guest,
    )
    host = "10.0.2.2" if guest else "127.0.0.1"
    assert environment == {"OPENAI_BASE_URL": f"http://{host}:18790/v1", "OPENAI_API_KEY": "loom_workload_proxy"}


def test_guest_broker_requires_ipv4_loopback(monkeypatch):
    from loom.hosted_harness import CODEX
    from loom.service_execution_sandbox_task import installed_agent_model_environment
    from loom.service_execution_task import ServiceExecutionTaskError

    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://[::1]:18790")
    with pytest.raises(ServiceExecutionTaskError, match="loopback broker"):
        installed_agent_model_environment(CODEX, base_url_env="A", api_key_env="B", guest=True)


@pytest.mark.parametrize("guest", [False, True])
def test_setup_install_reaches_the_proxy_from_its_sandbox(monkeypatch, guest):
    from loom.service_execution_sandbox_task import _setup_proxy_environment

    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:18791")
    host = "10.0.2.2" if guest else "127.0.0.1"
    assert _setup_proxy_environment(guest)["https_proxy"] == f"http://{host}:18791"
    # The controller itself always uses its own loopback.
    assert _setup_proxy_environment()["https_proxy"] == "http://127.0.0.1:18791"
