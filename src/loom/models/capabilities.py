"""Capabilities + RequiredCapabilities — what workers offer and trials demand
(spec §2.3, §3.1.1). Scalar fields match the §2.6 SQL claim query semantics
exactly."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from loom.models.types import (
    OS,
    CPUArch,
    GPUVendor,
    NetworkPolicyKind,
    RequiredCPUArch,
    ResourceMode,
    SandboxBackend,
)


class Capabilities(BaseModel):
    """A single backend configuration's offered capabilities."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: SandboxBackend = "docker"
    os: OS
    cpu_arch: CPUArch = "x86_64"
    gpu_vendor: GPUVendor
    network_policies: frozenset[NetworkPolicyKind]
    dynamic_network_policy: bool
    mounted_fs: bool
    resource_modes: frozenset[ResourceMode]
    # GPU types this backend can attach to a single sandbox. Empty set ⇒
    # backend cannot attach a GPU even if gpu_vendor != "none". For Docker
    # and Docker this is always empty (no GPU passthrough). For Modal it
    # lists the modal-supported GPU strings.
    gpu_types: frozenset[str] = frozenset()
    # Phase B (#78 / #188): can the backend honor `StartOptions.network`
    # (attach the sandbox container to a specific docker network)?
    # True for DockerDriver. False for Modal (cloud sandboxes
    # don't expose docker networking). When the worker has sandbox
    # isolation on AND this is False, the trial fails loudly with
    # CapabilityError rather than silently degrading.
    supports_custom_network: bool = False
    terminus2_model_switch: bool = False
    mainland_proxy: bool = Field(default=False, exclude_if=lambda value: not value)


class RequiredCapabilities(BaseModel):
    """What a trial requires. Persisted as `trials.requires_caps` JSONB."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: SandboxBackend = "docker"
    os: OS
    cpu_arch: RequiredCPUArch = "x86_64"
    gpu_vendor: GPUVendor
    network_policies: frozenset[NetworkPolicyKind]
    terminus2_model_switch: bool = False
    # Preserve existing persisted capability JSON when the option is off.
    mainland_proxy: bool = Field(default=False, exclude_if=lambda value: not value)

    def satisfied_by(self, caps: Capabilities) -> bool:
        """True iff `caps` offers everything this requires."""
        return (
            self.backend == caps.backend
            and self.os == caps.os
            and (self.cpu_arch == "any" or self.cpu_arch == caps.cpu_arch)
            and self.gpu_vendor == caps.gpu_vendor
            and self.network_policies <= caps.network_policies
            and (not self.terminus2_model_switch or caps.terminus2_model_switch)
            and (not self.mainland_proxy or caps.mainland_proxy)
        )
