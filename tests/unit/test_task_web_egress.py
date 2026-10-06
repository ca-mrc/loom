from __future__ import annotations

import socket
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import (
    TASK_EGRESS_OUTPUT,
    RuntimeHandoffInputV1,
    validate_runtime_plan_requirements,
)
from loom.models.networking import PublicWeb, WebAllowlist, WebDestination
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_deferred_verifier_plan,
    compile_service_execution_plan,
)
from loom_llm_gateway.task_egress import EgressDeniedError, resolve_destination
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def policy() -> WebAllowlist:
    return WebAllowlist(destinations=(WebDestination(host="registry.npmjs.org", protocol="https"),))


def test_egress_policy_bound_to_plan_and_runtime_capability() -> None:
    task, trial, profile = _inputs()
    assert "task_egress" not in workload_requirements_from_task(task).model_dump(mode="json")
    kwargs = dict(task=task, trial=trial, profile=profile, source_provenance=_provenance(),
                  task_revision_sha256=_REVISION)
    assert "task_egress" not in compile_service_execution_plan(**kwargs).canonical_payload()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "baseline_network_policy": policy(), "network_policies_supported": frozenset({"web-allowlist"}),
    })})
    kwargs["task"] = task
    with pytest.raises(ValueError, match="task_egress_runtime_unavailable"):
        compile_service_execution_plan(**kwargs)
    kwargs["profile"] = profile.model_copy(update={
        "supports_task_web_egress": True,
        "execution_class_id": "linux-amd64-cpu-web-pod-v1",
    })
    plan = compile_service_execution_plan(**kwargs)
    requirements = workload_requirements_from_task(task, trial)
    assert plan.task_egress == requirements.task_egress == policy()
    assert TASK_EGRESS_OUTPUT in plan.output_declarations
    validate_runtime_plan_requirements(plan, requirements)
    with pytest.raises(ValueError, match="network"):
        validate_runtime_plan_requirements(plan.model_copy(update={"task_egress": None}), requirements)


def test_trial_override_is_frozen_across_agent_and_deferred_verifier_plans() -> None:
    task, trial, profile = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "network_policies_supported": frozenset({"gateway-only", "public-web"}),
    })})
    trial = trial.model_copy(update={"baseline_network_policy_override": PublicWeb()})
    profile = profile.model_copy(update={
        "supports_task_web_egress": True,
        "execution_class_id": "linux-amd64-cpu-web-pod-v1",
    })

    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert task.environment.baseline_network_policy.kind == "gateway-only"
    assert plan.effective_network_policy == PublicWeb()
    assert plan.task_egress == PublicWeb()
    assert plan.main.environment["LOOM_EFFECTIVE_NETWORK_POLICY_JSON"] == '{"kind":"public-web"}'
    assert workload_requirements_from_task(task, trial).effective_network_policy == PublicWeb()

    drifted = plan.model_dump(mode="json")
    drifted["main"]["environment"]["LOOM_EFFECTIVE_NETWORK_POLICY_JSON"] = (
        '{"kind":"gateway-only"}'
    )
    with pytest.raises(ValidationError, match="differs from the plan"):
        type(plan).model_validate(drifted)

    verifier = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120, handoff_input=_HANDOFF)
    assert verifier.effective_network_policy == plan.effective_network_policy
    assert verifier.main.environment == plan.main.environment

    shared_trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    shared = compile_service_execution_plan(
        task=task, trial=shared_trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert shared.verifier is not None
    assert shared.verifier.environment == shared.main.environment
    assert shared.effective_network_policy == PublicWeb()


def test_unsupported_override_and_response_only_web_topology_are_rejected() -> None:
    task, trial, _ = _inputs()
    override = PublicWeb()
    unsupported = trial.model_copy(update={"baseline_network_policy_override": override})
    assert "network_policy_override_not_supported" in automatic_service_execution_rejections(
        task, unsupported, source_provenance=_provenance(),
    )

    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "network_policies_supported": frozenset({"gateway-only", "public-web"}),
    })})
    response_only = unsupported.model_copy(update={"agent_name": "direct-completion"})
    assert "task_network_consumer_unavailable" in automatic_service_execution_rejections(
        task, response_only, source_provenance=_provenance(),
    )


@pytest.mark.parametrize("host", ["*.example.org", "localhost", "127.0.0.1", "[::1]", "EXAMPLE.org", "example.org.",
    "user@example.org", "example.org:443", "a.svc.cluster.local", "example.org/path", "0177.0.0.1"])
def test_destination_rejects_ambiguous_or_internal_hosts(host: str) -> None:
    with pytest.raises(ValidationError):
        WebDestination(host=host, protocol="https")


@pytest.mark.parametrize("address", ["127.0.0.1", "10.1.2.3", "169.254.169.254", "100.64.0.1", "::1",
    "::ffff:127.0.0.1", "64:ff9b::a00:1", "2001:db8::1", "224.0.0.1", "192.0.2.1", "4000::1", "fec0::1", "::8.8.8.8", "192.88.99.1", "2001:4860:4860::8888%lo"])
async def test_dns_mixed_answer_fails_closed(address: str) -> None:
    resolver = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
        (socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))])
    with pytest.raises(EgressDeniedError, match="destination_address_denied"):
        await resolve_destination(WebDestination(host="example.org", protocol="https"), (), resolver=resolver)


async def test_dns_is_pinned_and_deployment_protected_addresses_are_denied() -> None:
    resolver = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))])
    destination = WebDestination(host="example.org", protocol="https")
    assert await resolve_destination(destination, (), resolver=resolver) == ("8.8.8.8",)
    with pytest.raises(EgressDeniedError, match="destination_address_denied"):
        await resolve_destination(destination, ("8.8.8.8/32",), resolver=resolver)


async def test_command_receives_proxy_environment_without_changing_model_gateway(monkeypatch) -> None:
    from unittest.mock import Mock

    from loom.service_execution_sandbox_task import sandbox_driver

    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "baseline_network_policy": policy(),
    })})
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:12345")
    monkeypatch.setenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", policy().model_dump_json())
    driver = sandbox_driver("task-sandbox", task, trial)
    response = Mock()
    response.json.return_value = {"return_code": 0, "stdout": "", "stderr": "", "duration_sec": 0}
    driver._request = AsyncMock(return_value=response)
    await driver.exec("npm install extra-version@1.0.0", env={"CUSTOM": "value"})
    env = driver._request.call_args.kwargs["json"]["env"]
    assert env["http_proxy"] == env["HTTPS_PROXY"] == "http://127.0.0.1:12345"
    assert env["NO_PROXY"] == "localhost,127.0.0.1,::1"
    assert env["CUSTOM"] == "value"
    assert "OPENAI_BASE_URL" not in env


def test_class_capability_must_explicitly_admit_web_egress() -> None:
    from loom.execution_contract import (
        NEBIUS_CPU_EXECUTION_CLASS_V1,
        NEBIUS_CPU_WEB_EXECUTION_CLASS_V1,
        evaluate_execution_admission,
    )

    task, _, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"baseline_network_policy": policy()})})
    requirements = workload_requirements_from_task(task)
    legacy = NEBIUS_CPU_EXECUTION_CLASS_V1
    assert "supports_task_web_egress" not in legacy.model_dump(mode="json")
    assert "task_web_egress_unsupported" in {reason.code for reason in evaluate_execution_admission(requirements, legacy).reasons}
    decision = evaluate_execution_admission(requirements, NEBIUS_CPU_WEB_EXECUTION_CLASS_V1)
    assert decision.compatible
    assert decision.execution_class_id != legacy.class_id


_HANDOFF = RuntimeHandoffInputV1(
    manifest_sha256="sha256:" + "d" * 64, file_count=1, total_bytes=10,
)
