"""#2310: how an installed agent in the task sandbox reaches the model, and
that no credential reaches the sandbox.

The full authority chain:
- the agent gets only the loopback broker URL and a fixed placeholder key;
- the broker forwards only canonical model routes, only in the agent phase,
  replacing caller credentials with the Pod's workload token (Go tests in
  `cmd/loom-execution-runtime`: model_access_test.go, model_phase_authority_test.go);
- that token is bound to the Trial's Provider Connection, so a caller-supplied
  connection header cannot switch it (Gateway facade tests);
- the rendered task sandbox mounts no identity token and receives no secrets.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.hosted_harness import ORACLE, TERMINUS_2
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import compile_service_execution_plan
from loom.service_execution_sandbox_task import (
    WORKLOAD_PROXY_API_KEY,
    installed_agent_model_environment,
)
from loom.service_execution_task import ServiceExecutionTaskError
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_BROKER = Path(__file__).resolve().parents[2] / "cmd/loom-execution-runtime/broker.go"


def test_installed_agent_gets_only_the_loopback_broker_and_a_placeholder(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:43111")

    environment = installed_agent_model_environment(
        TERMINUS_2, base_url_env="OPENAI_BASE_URL", api_key_env="OPENAI_API_KEY",
    )

    assert environment == {"OPENAI_BASE_URL": "http://127.0.0.1:43111/v1", "OPENAI_API_KEY": "loom_workload_proxy"}


def test_placeholder_matches_the_broker() -> None:
    assert f'"OPENAI_API_KEY":         "{WORKLOAD_PROXY_API_KEY}"' in _BROKER.read_text()


@pytest.mark.parametrize("gateway", [
    "", "https://127.0.0.1:43111", "http://gateway.loom.svc:9100", "http://127.0.0.1", "http://127.0.0.1:43111/openai",
])
def test_refuses_anything_but_the_loopback_broker(monkeypatch, gateway: str) -> None:
    monkeypatch.setenv("LOOM_GATEWAY_URL", gateway)

    with pytest.raises(ServiceExecutionTaskError, match="loopback broker"):
        installed_agent_model_environment(TERMINUS_2, base_url_env="B", api_key_env="K")


def test_model_free_harness_gets_no_model_access(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:43111")

    with pytest.raises(ServiceExecutionTaskError, match="does not use a model"):
        installed_agent_model_environment(ORACLE, base_url_env="B", api_key_env="K")


def test_rendered_task_sandbox_holds_no_credentials() -> None:
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION,
    )
    lease = _lease()
    lease.execution_class_id = plan.execution_class_id
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task, trial).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    pod = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name, pod_identity_audience="loom-execution",
    ))["spec"]["template"]["spec"]

    assert pod["automountServiceAccountToken"] is False
    sandboxes = [c for c in pod["initContainers"] if c["name"] in {"task-sandbox", "verifier-sandbox"}]
    assert sandboxes
    identity_mounts = {"execution-identity"}
    for sandbox in sandboxes:
        assert not identity_mounts & {m["name"] for m in sandbox["volumeMounts"]}
        assert not any(m["mountPath"].startswith("/var/run/secrets") for m in sandbox["volumeMounts"])
        assert not any(re.search(r"TOKEN|KEY|SECRET|PASSWORD|CREDENTIAL", e["name"]) for e in sandbox["env"])
        assert not any("valueFrom" in e for e in sandbox["env"])
        assert "envFrom" not in sandbox
    # Only the trusted execution container receives the Pod identity token.
    execution = pod["containers"][0]
    assert "execution-identity" in {m["name"] for m in execution["volumeMounts"]}
