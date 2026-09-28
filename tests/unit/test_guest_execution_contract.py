"""Guest authority stays separate from immutable shared-host privileges."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys

import pytest
from pydantic import ValidationError

from loom import execution_contract as contract
from loom.execution_requirements import (
    TaskExecutionRequirementsV1,
    execution_requirement_diagnostics,
)
from loom.models.task import TaskConfig

_GUEST_CAPABILITIES = (
    "nested_docker",
    "singularity_mounts",
    "isolated_kernel_settings",
)


def _task(*capabilities: str, **environment: object) -> TaskConfig:
    return TaskConfig.model_validate(
        {
            "task": {"id": "guest-contract", "name": "Guest contract"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "docker_image": "registry.example/task@sha256:" + "a" * 64,
                "baseline_network_policy": {"kind": "gateway-only"},
                "cpus": 1,
                "memory_mb": 512,
                "storage_mb": 160,
                "execution_requirements": {"capabilities": capabilities},
                **environment,
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "script"},
        }
    )


@pytest.mark.parametrize(
    ("execution_class", "digest"),
    [
        (
            contract.NEBIUS_CPU_EXECUTION_CLASS_V1,
            "59806870a3a53f1a9ab8c387f06a3f5b49548cd883ffc790b92351a34cc85f6b",
        ),
        (
            contract.NEBIUS_CPU_WEB_EXECUTION_CLASS_V1,
            "9aec5da55f67dec445bd4035a0cfb3bd68f27e026dc1210bdb3e6129e3691be6",
        ),
    ],
)
def test_old_class_canonical_bytes_are_unchanged(
    execution_class: contract.ExecutionClassV1, digest: str
) -> None:
    # Recorded before adding the extension: persisted class identities cannot drift.
    assert hashlib.sha256(execution_class.model_dump_json().encode()).hexdigest() == digest
    explicit_null = contract.ExecutionClassV1.model_validate(
        {**execution_class.model_dump(mode="json"), "guest_execution": None}
    )
    assert explicit_null.model_dump_json() == execution_class.model_dump_json()


@pytest.mark.parametrize("web", [False, True])
def test_guest_catalog_identity_admits_declared_capabilities_without_host_flags(web: bool) -> None:
    execution_class = contract.nebius_guest_execution_class(supports_task_web_egress=web)
    assert execution_class.class_id == (
        "linux-amd64-cpu-guest-web-v1" if web else "linux-amd64-cpu-guest-v1"
    )
    assert execution_class.isolation_level == "dedicated_guest_kernel"
    assert execution_class.supports_task_web_egress is web
    assert execution_class.guest_execution is not None
    assert execution_class.guest_execution.schema_version == "loom.guest-execution-class.v1"
    assert execution_class.guest_execution.runtime == "qemu-tcg-v1"
    assert execution_class.guest_execution.supported_capabilities == frozenset(_GUEST_CAPABILITIES)
    requirements = contract.workload_requirements_from_task(_task(*_GUEST_CAPABILITIES))
    assert contract.evaluate_execution_admission(requirements, execution_class).compatible


@pytest.mark.parametrize("capability", _GUEST_CAPABILITIES)
def test_guest_projection_retains_declaration_without_requesting_trusted_host_privileges(
    capability: str,
) -> None:
    task = _task(capability)
    requirements = contract.workload_requirements_from_task(task)
    assert requirements.isolation_level == "dedicated_guest_kernel"
    assert requirements.execution_requirements == task.environment.execution_requirements
    for field in (
        "privileged", "host_path", "host_network", "nested_containers",
        "host_devices", "host_specialized",
    ):
        assert getattr(requirements, field) is False
    shared = contract.evaluate_execution_admission(
        requirements, contract.NEBIUS_CPU_EXECUTION_CLASS_V1
    )
    assert {reason.code for reason in shared.reasons} >= {
        f"{capability}_unqualified", "isolation_level_unsupported",
    }


def test_plain_workload_projection_and_class_selection_stay_shared_kernel() -> None:
    requirements = contract.workload_requirements_from_task(_task())
    assert requirements.isolation_level == "shared_kernel"
    assert contract.evaluate_execution_admission(
        requirements, contract.nebius_cpu_execution_class()
    ).compatible
    guest = contract.evaluate_execution_admission(requirements, contract.nebius_guest_execution_class())
    assert {reason.code for reason in guest.reasons} == {"isolation_level_unsupported"}


@pytest.mark.parametrize(
    "flag",
    [
        "permits_privileged", "permits_host_path", "permits_host_network",
        "permits_nested_containers", "permits_host_devices",
    ],
)
def test_guest_class_cannot_enable_any_trusted_host_escape(flag: str) -> None:
    raw = contract.nebius_guest_execution_class().model_dump(mode="json")
    with pytest.raises(ValidationError, match="host-escape"):
        contract.ExecutionClassV1.model_validate({**raw, flag: True})


@pytest.mark.parametrize(
    "updates",
    [
        {"guest_execution": None},
        {"isolation_level": "shared_kernel"},
        {"isolation_level": "sandboxed_runtime"},
        {"isolation_level": "dedicated_ephemeral_node"},
        {"cpu_architecture": "arm64"},
        {"gpu_vendor": "nvidia"},
    ],
)
def test_guest_class_requires_exact_isolation_and_linux_x86_cpu_shape(
    updates: dict[str, object],
) -> None:
    raw = contract.nebius_guest_execution_class().model_dump(mode="json")
    with pytest.raises(ValidationError, match="guest"):
        contract.ExecutionClassV1.model_validate({**raw, **updates})


@pytest.mark.parametrize(
    "updates",
    [
        {"schema_version": "loom.guest-execution-class.v0"},
        {"runtime": "host-docker"},
        {"supported_capabilities": ["privileged"]},
        {"supported_capabilities": ["external_cluster"]},
        {"supported_capabilities": ["pkcs11_authentication"]},
        {"supported_capabilities": ["dpdk_networking"]},
        {"host_socket": "/var/run/docker.sock"},
    ],
)
def test_guest_extension_rejects_unknown_versions_runtimes_and_external_authority(
    updates: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        contract.GuestExecutionClassV1.model_validate(
            {"supported_capabilities": list(_GUEST_CAPABILITIES), **updates}
        )


def test_guest_capability_serialization_is_stable_across_hash_seeds() -> None:
    script = (
        "from loom.execution_contract import nebius_guest_execution_class; "
        "print(nebius_guest_execution_class().model_dump_json())"
    )
    documents = [
        subprocess.check_output(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            text=True,
        )
        for seed in ("0", "5", "8")
    ]
    assert len(set(documents)) == 1


def test_admission_suppresses_only_capabilities_declared_by_the_guest_class() -> None:
    raw = contract.nebius_guest_execution_class().model_dump(mode="json")
    raw["guest_execution"]["supported_capabilities"] = ["nested_docker"]
    execution_class = contract.ExecutionClassV1.model_validate(raw)
    requirements = contract.workload_requirements_from_task(_task(*_GUEST_CAPABILITIES))
    decision = contract.evaluate_execution_admission(requirements, execution_class)
    assert {reason.code for reason in decision.reasons} == {
        "singularity_mounts_unqualified", "isolated_kernel_settings_unqualified",
    }


def test_guest_diagnostics_do_not_qualify_prerequisites_or_external_capabilities() -> None:
    requirements = TaskExecutionRequirementsV1.model_validate(
        {
            "capabilities": [*_GUEST_CAPABILITIES, "external_cluster", "pkcs11_authentication", "dpdk_networking"],
            "prerequisites": [
                {"name": "cluster", "kind": "endpoint"},
                {"name": "auth", "kind": "managed_secret", "reference": "k8s-secret://team/auth"},
                {"name": "card", "kind": "device", "reference": "loom://devices/card"},
                {"name": "app", "kind": "fixture", "reference": "loom://fixtures/app"},
            ],
        }
    )
    assert len(execution_requirement_diagnostics(requirements)) == 10
    diagnostics = execution_requirement_diagnostics(
        requirements, supported_capabilities=frozenset(_GUEST_CAPABILITIES)
    )
    assert {item.code for item in diagnostics} == {
        "external_cluster_unqualified", "pkcs11_authentication_unqualified",
        "dpdk_networking_unqualified", "execution_prerequisite_missing",
        "execution_prerequisite_unverified",
    }
    assert {item.field for item in diagnostics if item.category == "execution_prerequisite"} == {
        "prerequisites.cluster", "prerequisites.auth", "prerequisites.card", "prerequisites.app",
    }


@pytest.mark.parametrize("capability", ["external_cluster", "pkcs11_authentication", "dpdk_networking"])
def test_guest_admission_keeps_external_capabilities_and_prerequisites_rejected(capability: str) -> None:
    requirements = contract.workload_requirements_from_task(
        _task(
            execution_requirements={
                "capabilities": ["nested_docker", capability],
                "prerequisites": [{"name": "source", "kind": "fixture", "reference": "loom://fixtures/source"}],
            }
        )
    )
    decision = contract.evaluate_execution_admission(requirements, contract.nebius_guest_execution_class())
    assert not decision.compatible
    assert {reason.code for reason in decision.reasons} >= {
        f"{capability}_unqualified", "execution_prerequisite_unverified",
    }


@pytest.mark.parametrize(
    ("field", "value", "reason", "minimum"),
    [
        ("cpus", 0.999, "guest_cpu_limit_too_small", "1000"),
        ("memory_mb", 511, "guest_memory_limit_too_small", "512"),
        ("storage_mb", 159, "guest_ephemeral_storage_limit_too_small", "160"),
    ],
)
def test_guest_minimum_resources_are_enforced_with_actionable_limits(
    field: str, value: int | float, reason: str, minimum: str,
) -> None:
    requirements = contract.workload_requirements_from_task(_task("nested_docker", **{field: value}))
    decision = contract.evaluate_execution_admission(requirements, contract.nebius_guest_execution_class())
    assert not decision.compatible
    rejection = next(item for item in decision.reasons if item.code == reason)
    assert minimum in rejection.message
    assert field in rejection.message


@pytest.mark.parametrize("legacy_flag", ["nested_containers", "host_specialized"])
def test_old_frozen_requirements_parse_without_silent_guest_upgrade(legacy_flag: str) -> None:
    raw = contract.workload_requirements_from_task(_task()).model_dump(mode="json")
    raw.update(
        isolation_level="shared_kernel",
        execution_requirements={"capabilities": ["nested_docker"], "prerequisites": []},
        **{legacy_flag: True},
    )
    requirements = contract.WorkloadRequirementsV1.model_validate(raw)
    assert requirements.model_dump(mode="json") == raw
    decision = contract.evaluate_execution_admission(requirements, contract.nebius_guest_execution_class())
    assert {reason.code for reason in decision.reasons} >= {
        "isolation_level_unsupported", f"{legacy_flag}_unsupported",
    }
