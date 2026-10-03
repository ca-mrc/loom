from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    ProcessPhaseV1,
    RuntimeOutputDeclarationV1,
    SidecarContainerV1,
    VerifierExecution,
)
from tests.support.execution_image_admission import signed_image_admission_bundle

_IMAGE = "registry.example/runtime@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/loom-runtime@sha256:" + "b" * 64
_RESOURCES = ContainerResourcesV1(
    cpu_millis=1000,
    memory_mib=1024,
    ephemeral_storage_mib=2048,
)


def _phase(role: str) -> ProcessPhaseV1:
    return ProcessPhaseV1(
        role=role,
        argv=("/bin/loom-task", role),
        working_directory="/workspace",
        timeout_seconds=60,
        environment={"LOOM_PHASE": role},
    )


def _plan(**updates: object) -> ExecutionRuntimePlanV1:
    values: dict[str, object] = {
        "candidate_sha": "1" * 40,
        "task_revision_sha256": "sha256:" + "2" * 64,
        "command_identity_sha256": "sha256:" + "3" * 64,
        "execution_role": "attempt",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "composition": "init_payload",
        "task_image_ref": _IMAGE,
        "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "image_admission": signed_image_admission_bundle((_IMAGE, _RUNTIME_IMAGE)),
        "task_resources": _RESOURCES,
        "workspace_mib": 4096,
        "runtime_volume_mib": 128,
        "setup": (_phase("setup"),),
        "main": _phase("agent"),
        "verifier_execution": "in_attempt",
        "verifier": _phase("verifier"),
    }
    values.update(updates)
    return ExecutionRuntimePlanV1.model_validate(values)


def test_runtime_plan_is_immutable_bounded_and_digest_pinned() -> None:
    plan = _plan()
    assert plan.schema_version == "loom.execution-runtime-plan.v1"
    assert plan.canonical_payload()["task_image_ref"] == _IMAGE
    with pytest.raises(ValidationError, match="digest-pinned"):
        _plan(task_image_ref="registry.example/task:latest")
    with pytest.raises(ValidationError, match="forbidden name"):
        ProcessPhaseV1.model_validate(
            {
                **_phase("agent").model_dump(mode="json"),
                "environment": {"API_TOKEN": "x"},
            }
        )


@pytest.mark.parametrize("value", ["a" * 4096, "é" * 2048])
def test_phase_environment_retains_utf8_value_bound(value: str) -> None:
    raw = _phase("agent").model_dump()
    ProcessPhaseV1.model_validate({**raw, "environment": {"LOOM_VALUE": value}})
    with pytest.raises(ValidationError, match="process environment value is invalid"):
        ProcessPhaseV1.model_validate({**raw, "environment": {"LOOM_VALUE": value + "a"}})


@pytest.mark.parametrize("directory", ["/app", "/workspace", "/workspace/task"])
def test_phase_accepts_trusted_controller_or_task_workspace(directory: str) -> None:
    ProcessPhaseV1.model_validate({**_phase("agent").model_dump(), "working_directory": directory})


@pytest.mark.parametrize("directory", [
    "/app/task", "/appevil", "/workspaceevil", "/tmp", "/app/.", "/workspace/../app",
])
def test_phase_rejects_other_working_directories(directory: str) -> None:
    with pytest.raises(ValidationError):
        ProcessPhaseV1.model_validate({**_phase("agent").model_dump(), "working_directory": directory})


def test_sidecar_order_and_probes_are_explicit() -> None:
    first = SidecarContainerV1(
        role_name="database",
        image_ref=_IMAGE,
        argv=("/bin/database",),
        resources=_RESOURCES,
        startup_probe=ProbeV1(kind="tcp", port=5432),
        readiness_probe=ProbeV1(kind="tcp", port=5432),
    )
    second = SidecarContainerV1(
        role_name="api",
        image_ref=_IMAGE,
        argv=("/bin/api",),
        resources=_RESOURCES,
        startup_probe=ProbeV1(kind="http", port=8080, path="/healthz"),
        readiness_probe=ProbeV1(kind="http", port=8080, path="/readyz"),
        depends_on=("database",),
    )
    assert [item.role_name for item in _plan(sidecars=(first, second)).sidecars] == [
        "database",
        "api",
    ]
    with pytest.raises(ValidationError, match="earlier sidecars"):
        _plan(sidecars=(second, first))
    with pytest.raises(ValidationError, match="reserved container role"):
        _plan(
            sidecars=(
                SidecarContainerV1(
                    **{
                        **first.model_dump(mode="python"),
                        "role_name": "verifier",
                    }
                ),
            )
        )


def test_verifier_topology_is_fail_closed() -> None:
    separate = _plan(
        verifier_execution=VerifierExecution.SEPARATE_EXECUTION,
        verifier=None,
    )
    assert separate.verifier is None
    with pytest.raises(ValidationError, match="requires a verifier"):
        _plan(verifier=None)
    with pytest.raises(ValidationError, match="cannot run"):
        _plan(verifier_execution="skipped")

    verifier_unit = _plan(
        execution_role="verifier",
        main=_phase("verifier"),
        verifier_execution="skipped",
        verifier=None,
    )
    assert verifier_unit.main.role == "verifier"
    with pytest.raises(ValidationError, match="verifier main phase"):
        _plan(
            execution_role="verifier",
            verifier_execution="skipped",
            verifier=None,
        )


def test_complete_output_declarations_are_confined_and_unique() -> None:
    declaration = RuntimeOutputDeclarationV1(
        source_path="answer.txt",
        relative_path="artifacts/answer.txt",
        kind="task_artifact",
        required=True,
    )
    assert _plan(output_declarations=(declaration,)).output_declarations == (declaration,)
    with pytest.raises(ValidationError, match="must be unique"):
        _plan(output_declarations=(declaration, declaration))
    with pytest.raises(ValidationError, match="confined relative"):
        RuntimeOutputDeclarationV1(
            source_path="../answer.txt",
            relative_path="artifacts/answer.txt",
            kind="task_artifact",
            required=True,
        )
    with pytest.raises(ValidationError, match="unknown bundle namespace"):
        RuntimeOutputDeclarationV1(
            source_path="answer.txt",
            relative_path="unscoped/answer.txt",
            kind="task_artifact",
            required=True,
        )


def _timeout_verifier_plan() -> ExecutionRuntimePlanV1:
    probe = ProbeV1(kind="exec", argv=("/bin/true",))
    sandboxes = tuple(
        SidecarContainerV1(
            role_name=name,
            image_ref=_IMAGE,
            argv=("/bin/true",),
            resources=_RESOURCES,
            startup_probe=probe,
            readiness_probe=probe,
            private_sandbox=True,
        )
        for name in ("task-sandbox", "verifier-sandbox")
    )
    return _plan(
        agent_image_ref=_IMAGE,
        sidecars=sandboxes,
        verifier_after_agent_timeout=True,
    )


def test_timeout_verification_opt_in_round_trip_and_legacy_payload() -> None:
    legacy = _plan()
    payload = legacy.canonical_payload()
    assert "verifier_after_agent_timeout" not in payload
    assert ExecutionRuntimePlanV1.model_validate(
        {**payload, "verifier_after_agent_timeout": False}
    ).canonical_payload() == payload
    assert not ExecutionRuntimePlanV1.model_validate(payload).verifier_after_agent_timeout

    enabled = _timeout_verifier_plan()
    payload = enabled.canonical_payload()
    assert payload["verifier_after_agent_timeout"] is True
    loaded = ExecutionRuntimePlanV1.model_validate(payload)
    assert loaded.verifier_after_agent_timeout
    assert loaded.main.timeout_seconds == enabled.main.timeout_seconds
    assert loaded.verifier == enabled.verifier


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"composition": "precomposed"}, "isolated attempt controller"),
        ({"agent_image_ref": None}, "agent image reference"),
        ({"sidecars": []}, "isolated attempt controller"),
        ({"verifier_execution": "skipped", "verifier": None}, "in-attempt verifier"),
        ({"verifier_execution": "separate_execution", "verifier": None}, "in-attempt verifier"),
        ({"verifier": None}, "requires a verifier phase"),
        (
            {
                "execution_role": "verifier",
                "main": _phase("verifier").model_dump(mode="json"),
                "verifier_execution": "skipped",
                "verifier": None,
            },
            "isolated attempt controller",
        ),
    ],
)
def test_timeout_verification_rejects_unsafe_topology(
    changes: dict[str, object], reason: str
) -> None:
    payload = _timeout_verifier_plan().canonical_payload()
    payload.update(changes)
    with pytest.raises(ValidationError, match=reason):
        ExecutionRuntimePlanV1.model_validate(payload)


@pytest.mark.parametrize("missing_role", ["task-sandbox", "verifier-sandbox"])
def test_timeout_verification_requires_both_private_sandboxes(missing_role: str) -> None:
    payload = _timeout_verifier_plan().canonical_payload()
    payload["sidecars"] = [
        item for item in payload["sidecars"] if item["role_name"] != missing_role
    ]
    with pytest.raises(ValidationError, match="isolated attempt controller"):
        ExecutionRuntimePlanV1.model_validate(payload)

    payload = _timeout_verifier_plan().canonical_payload()
    for item in payload["sidecars"]:
        if item["role_name"] == missing_role:
            item["private_sandbox"] = False
    with pytest.raises(ValidationError, match="private mounts"):
        ExecutionRuntimePlanV1.model_validate(payload)
