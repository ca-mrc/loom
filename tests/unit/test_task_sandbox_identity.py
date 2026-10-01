"""Task identity is confined to private sandboxes and deployment opt-in."""

import json

import pytest

from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT, ExecutionRuntimePlanV1
from loom.models.task import TaskConfig
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from loom_execution_actuator.renderer import _sidecar
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _identity_task(user, home=None, verifier_user=None):
    task, trial, profile = _inputs()
    payload = task.model_dump(mode="json")
    payload["environment"]["user"] = user
    if home is not None:
        payload["environment"]["environment"] = {"HOME": home}
    payload["verifier"]["user"] = verifier_user
    return TaskConfig.model_validate(payload), trial, profile


@pytest.mark.parametrize("user,home,uid,gid", [
    ("root", None, 0, 0), (0, "/root/custom", 0, 0),
    ("1001:1002", "/home/miles", 1001, 1002),
])
def test_identity_reaches_private_containers_without_changing_controller(user, home, uid, gid):
    task, trial, profile = _identity_task(user, home)
    assert not automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert "task_identity_runtime_unavailable" in runtime_profile_rejections(task, trial, profile)
    profile = profile.model_copy(update={"supports_task_identity": True})
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256="sha256:" + "c" * 64,
    )
    assert (plan.run_as_user, plan.run_as_group) == (65532, 65532)
    for sandbox in plan.sidecars:
        assert sandbox.identity.model_dump() == {
            "run_as_user": uid, "run_as_group": gid, "home": home or "/root",
        }
        container = _sidecar(sandbox)
        security = container["securityContext"]
        assert (security["runAsUser"], security["runAsGroup"]) == (uid, gid)
        assert security["runAsNonRoot"] is (uid != 0)
        assert security["allowPrivilegeEscalation"] is False
        assert security["capabilities"]["drop"] == ["ALL"]
        assert security["capabilities"].get("add", []) == (
            ["CHOWN", "DAC_OVERRIDE", "FOWNER", "SETUID", "SETGID", "KILL"] if uid == 0 else []
        )
        assert {"name": "HOME", "value": home or "/root"} in container["env"]
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


def test_default_identity_preserves_legacy_plan_bytes_and_security():
    task, trial, profile = _inputs()
    assert "supports_task_identity" not in profile.model_dump(mode="json")
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256="sha256:" + "c" * 64,
    )
    for sandbox in plan.canonical_payload()["sidecars"]:
        assert "identity" not in sandbox
    for sandbox in plan.sidecars:
        assert _sidecar(sandbox)["securityContext"] == {
            "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
            "readOnlyRootFilesystem": False, "runAsNonRoot": True,
        }


@pytest.mark.parametrize("user,home", [
    ("miles", "/home/miles"), (1001, "/home/miles"), ("1001:1002", None),
    ("root", "/root/../tests"), ("root", "/loom/private"), ("root", ""),
])
def test_unsupported_or_ambiguous_identity_is_rejected_before_execution(user, home):
    task, trial, _ = _identity_task(user, home)
    assert "unsupported_task_identity" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance(),
    )


def test_verifier_identity_is_preserved_separately():
    task, trial, profile = _identity_task("1001:1002", "/home/miles", verifier_user="root")
    profile = profile.model_copy(update={"supports_task_identity": True})
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256="sha256:" + "c" * 64,
    )
    assert [item.identity.run_as_user for item in plan.sidecars] == [1001]
    from loom.service_execution_materialization import compile_deferred_verifier_plan
    verifier = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120)
    assert verifier.sidecars[-1].identity.run_as_user == 0


def test_identity_cannot_apply_to_an_ordinary_sidecar_or_the_controller():
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256="sha256:" + "c" * 64,
    )
    raw = plan.canonical_payload()
    raw["sidecars"][0].update(role_name="database", private_sandbox=False,
                             identity={"run_as_user": 0, "run_as_group": 0, "home": "/root"})
    with pytest.raises(ValueError, match="private sandbox"):
        ExecutionRuntimePlanV1.model_validate(raw)
    raw = plan.canonical_payload()
    raw["run_as_user"] = 0
    with pytest.raises(ValueError):
        ExecutionRuntimePlanV1.model_validate(raw)


@pytest.mark.parametrize("declaration", ["template_identity", "task_identity", "lifecycle", "mutable_paths", "template_egress"])
def test_explicit_template_cannot_bypass_automatic_capability_readiness(declaration):
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256="sha256:" + "c" * 64,
    )
    template = plan.canonical_payload()
    del template["task_revision_sha256"]
    payload = task.model_dump(mode="json")
    payload["service_execution"] = {"logical_pool_id": profile.logical_pool_id, "runtime_template": template}
    if declaration == "template_identity":
        template["sidecars"][0]["identity"] = {"run_as_user": 0, "run_as_group": 0, "home": "/root"}
    elif declaration == "task_identity":
        payload["environment"]["user"] = "root"
    elif declaration == "lifecycle":
        payload["environment"]["service_lifecycle"] = {"readiness": {"command": "/bin/true"}}
    elif declaration == "mutable_paths":
        payload["environment"]["mutable_paths"] = ["/data"]
    else:
        policy = {"kind": "web-allowlist", "destinations": [{"host": "example.org", "protocol": "https"}]}
        template["task_egress"] = policy
        template["effective_network_policy"] = policy
        frozen = json.dumps(policy, sort_keys=True, separators=(",", ":"))
        for phase in (template["main"], template.get("verifier")):
            if phase is not None:
                phase["environment"]["LOOM_EFFECTIVE_NETWORK_POLICY_JSON"] = frozen
        template["output_declarations"].append(TASK_EGRESS_OUTPUT.model_dump(mode="json"))
    with pytest.raises(ValueError, match="automatic native execution"):
        TaskConfig.model_validate(payload)
