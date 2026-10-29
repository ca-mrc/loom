"""Prepared OCI task ENV and independent verifier audit policy compose safely."""
from __future__ import annotations

import json

import pytest

from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT, RuntimeHandoffInputV1
from loom.models.networking import GatewayOnly, NoNetwork, PublicWeb, WebAllowlist, WebDestination
from loom.models.task import EnvironmentConfig
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import compile_service_execution_plan
from loom.task_environment_handoff import RESOLVED_TASK_ENVIRONMENT, RESOLVED_VERIFIER_ENVIRONMENT
from loom.task_image_materialization import TaskImageExecutionGrantV1
from loom.task_sandbox_planner import compile_deferred_verifier_plan
from tests.unit.test_independent_verifier_environment import independent_inputs, verifier_grant
from tests.unit.test_service_execution_materialization import _REVISION, _provenance


@pytest.mark.parametrize("harness", ["oracle", "terminus-2"])
@pytest.mark.parametrize("agent_policy,verifier_policy", [
    (PublicWeb(), NoNetwork()),
    (PublicWeb(), GatewayOnly()),
    (NoNetwork(), PublicWeb()),
    (NoNetwork(), WebAllowlist(destinations=(WebDestination(host="example.com", protocol="https"),))),
], ids=["public-agent-private-verifier", "public-agent-gateway-verifier",
        "private-agent-public-verifier", "private-agent-allowlist-verifier"])
def test_prepared_environment_admission_preserves_independent_audit_and_parent(
    harness, agent_policy, verifier_policy,
):
    task, trial, profile = independent_inputs()
    if harness == "oracle":
        trial = TrialConfig(agent_name="oracle", agent_model=None)
    task = task.model_copy(update={
        "environment": task.environment.model_copy(update={
            "user": "image", "image_workdir": True, "image_entrypoint": True,
            "environment": {"KEPT": "agent-authored"}, "baseline_network_policy": agent_policy,
        }),
        "verifier": task.verifier.model_copy(update={
            "environment": task.verifier.environment.model_copy(update={
                "environment": {"ONLY_VERIFIER": "yes", "KEPT": "verifier-authored"},
                "baseline_network_policy": verifier_policy,
            }),
        }),
    })
    profile = profile.model_copy(update={"supports_task_identity": True, "supports_task_web_egress": True})
    raw_grant = verifier_grant(task, profile).model_dump(mode="json")
    raw_grant["image_runtime_metadata"]["task"] = {
        "image_ref": raw_grant["registry_images"]["task"], "user": "agent", "uid": 1001,
        "gid": 1002, "home": "/home/agent", "workdir": "/app/agent", "entrypoint": ["/agent-entry"],
        "environment": {"PASSWORD": "agent-fixture", "KEPT": "agent-image"},
    }
    raw_grant["image_runtime_metadata"]["verifier"]["environment"].update(
        PASSWORD="verifier-fixture", KEPT="verifier-image",
    )
    grant = TaskImageExecutionGrantV1.model_validate(raw_grant)
    original_task = task.model_dump_json()
    parent = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, task_image_grant=grant,
        source_provenance=_provenance(), task_revision_sha256=_REVISION,
    )
    original_parent = parent.model_dump_json()
    task_environment = EnvironmentConfig.model_validate_json(parent.main.environment[RESOLVED_TASK_ENVIRONMENT])
    verifier_environment = EnvironmentConfig.model_validate_json(parent.main.environment[RESOLVED_VERIFIER_ENVIRONMENT])
    assert task_environment.environment["PASSWORD"] == "agent-fixture"
    assert task_environment.environment["KEPT"] == "agent-authored"
    assert verifier_environment.environment["PASSWORD"] == "verifier-fixture"
    assert verifier_environment.environment["KEPT"] == "verifier-authored"
    assert all("PASSWORD" not in phase.environment for phase in (*parent.setup, parent.main))

    deferred = compile_deferred_verifier_plan(
        parent, task, verifier_timeout_seconds=120,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256="sha256:" + "a" * 64, file_count=1, total_bytes=1),
    )
    sandbox, = [sidecar for sidecar in deferred.sidecars if sidecar.private_sandbox]
    assert sandbox.environment["PASSWORD"] == "verifier-fixture"
    assert sandbox.environment["KEPT"] == "verifier-authored"
    assert sandbox.identity.run_as_user == 1002
    assert deferred.task_image_ref == grant.registry_images["verifier"]
    assert "PASSWORD" not in deferred.main.environment
    assert deferred.effective_network_policy == verifier_policy
    assert json.loads(deferred.main.environment["LOOM_EFFECTIVE_NETWORK_POLICY_JSON"]) == verifier_policy.model_dump(mode="json")
    proxy_runs = deferred.task_egress is not None or deferred.setup_egress is not None
    assert (TASK_EGRESS_OUTPUT in deferred.output_declarations) == proxy_runs
    reward, = [item for item in deferred.output_declarations if item.relative_path == "verifier/output.json"]
    assert reward.required
    assert parent.effective_network_policy == agent_policy
    assert parent.model_dump_json() == original_parent
    assert task.model_dump_json() == original_task
