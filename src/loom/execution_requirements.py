"""Explicit task requirements; declarations never grant runtime authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ExecutionCapability = Literal[
    "nested_docker", "singularity_mounts", "isolated_kernel_settings",
    "external_cluster", "pkcs11_authentication", "dpdk_networking",
]
GuestExecutionCapability = Literal[
    "nested_docker", "singularity_mounts", "isolated_kernel_settings",
]
GUEST_EXECUTION_CAPABILITIES: frozenset[GuestExecutionCapability] = frozenset({
    "nested_docker", "singularity_mounts", "isolated_kernel_settings",
})


class ExecutionPrerequisiteV1(BaseModel):
    """An unresolved dependency, with an optional opaque inventory reference."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,79}$")
    kind: Literal["endpoint", "managed_secret", "device", "fixture"]
    reference: str | None = Field(
        default=None, max_length=512,
        pattern=r"^(?:loom|k8s-secret)://[A-Za-z0-9._/@:-]+$",
    )


class TaskExecutionRequirementsV1(BaseModel):
    """Bounded capability and prerequisite declarations retained at admission."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    capabilities: tuple[ExecutionCapability, ...] = Field(default=(), max_length=6)
    prerequisites: tuple[ExecutionPrerequisiteV1, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def _unique_declarations(self) -> TaskExecutionRequirementsV1:
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("execution capabilities must be unique")
        names = [item.name for item in self.prerequisites]
        if len(set(names)) != len(names):
            raise ValueError("execution prerequisite names must be unique")
        return self


@dataclass(frozen=True)
class ExecutionRequirementDiagnostic:
    code: str
    field: str
    category: Literal["runtime_capability", "execution_prerequisite"]
    reason: str
    action: str


_CAPABILITY_ACTIONS: dict[ExecutionCapability, tuple[str, str]] = {
    "nested_docker": (
        "Nested Docker execution has no qualified service runtime.",
        "Qualify an isolated, trial-owned daemon, build cache and teardown; never mount a trusted host socket.",
    ),
    "singularity_mounts": (
        "Singularity bootstrap and mount operations have no qualified service runtime.",
        "Qualify the declared Singularity version, image format, mounts, resource limits and artifact transfer in an isolated runtime.",
    ),
    "isolated_kernel_settings": (
        "Trial-specific kernel settings have no qualified isolated service runtime.",
        "Qualify a runtime that isolates the required kernel settings and restores state without changing the shared host.",
    ),
    "external_cluster": (
        "Access to a task-owned external cluster has no qualified service contract.",
        "Declare the owned endpoint and managed authentication references, then qualify lifecycle, egress and cleanup.",
    ),
    "pkcs11_authentication": (
        "PKCS#11 or emulated smartcard authentication has no qualified service contract.",
        "Declare the actual authentication fixture or device and qualify agent/socket forwarding without bypassing authentication.",
    ),
    "dpdk_networking": (
        "DPDK device and traffic-generation execution has no qualified service runtime.",
        "Qualify owned network devices, hugepages, driver binding, traffic isolation and cleanup in a dedicated runtime.",
    ),
}


def execution_requirement_diagnostics(
    requirements: TaskExecutionRequirementsV1 | None,
    *,
    supported_capabilities: frozenset[GuestExecutionCapability] = frozenset(),
) -> tuple[ExecutionRequirementDiagnostic, ...]:
    """Compare declarations with explicit class support; leave prerequisites unresolved.

    References are recorded, never resolved here. Omitting class support retains
    the fail-closed behavior. Task data cannot select or authorize a new class,
    and a capability match is not evidence of deployed runtime readiness.
    """
    if requirements is None:
        return ()
    diagnostics = []
    for capability in requirements.capabilities:
        if capability in supported_capabilities:
            continue
        reason, action = _CAPABILITY_ACTIONS[capability]
        diagnostics.append(ExecutionRequirementDiagnostic(
            code=f"{capability}_unqualified", field=f"capabilities.{capability}",
            category="runtime_capability", reason=reason, action=action,
        ))
    for prerequisite in requirements.prerequisites:
        missing = prerequisite.reference is None
        diagnostics.append(ExecutionRequirementDiagnostic(
            code="execution_prerequisite_missing" if missing else "execution_prerequisite_unverified",
            field=f"prerequisites.{prerequisite.name}", category="execution_prerequisite",
            reason=(f"The {prerequisite.kind} prerequisite {prerequisite.name!r} has "
                    + ("no inventory reference." if missing else "an unverified inventory reference.")),
            action="Provide an owned prerequisite through the managed inventory and qualify its binding; never embed credentials in task files.",
        ))
    return tuple(diagnostics)
