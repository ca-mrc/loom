"""Derive a trial's RequiredCapabilities from task + agent + verifier configs.

Spec §3.1.1. Submitters do not provide requires_caps directly — the Control
Plane derives them on POST /trials.
"""

from __future__ import annotations

from loom.models.capabilities import RequiredCapabilities
from loom.models.networking import (
    Allowlist,
    GatewayOnly,
    NetworkPolicy,
    NoNetwork,
    Public,
    PublicWeb,
    WebAllowlist,
)
from loom.models.task import TaskConfig
from loom.models.types import NetworkPolicyKind


def derive_requires_caps(task: TaskConfig) -> RequiredCapabilities:
    """Union all network policies the trial may need across its steps."""
    needed: set[NetworkPolicyKind] = set()

    baseline = task.environment.baseline_network_policy
    needed.add(_kind(baseline))

    for step in task.steps:
        if step.network is None:
            continue
        if step.network.agent_phase is not None:
            needed.add(_kind(step.network.agent_phase))
        if step.network.verifier_phase is not None:
            needed.add(_kind(step.network.verifier_phase))

    return RequiredCapabilities(
        os=task.environment.os,
        cpu_arch=task.environment.cpu_arch,
        gpu_vendor=task.environment.gpu_vendor,
        network_policies=frozenset(needed),
    )


def _kind(policy: NetworkPolicy) -> NetworkPolicyKind:
    if isinstance(policy, Public):
        return "public"
    if isinstance(policy, NoNetwork):
        return "no-network"
    if isinstance(policy, GatewayOnly):
        return "gateway-only"
    if isinstance(policy, Allowlist):
        return "allowlist"
    if isinstance(policy, WebAllowlist):
        return "web-allowlist"
    if isinstance(policy, PublicWeb):
        return "public-web"
    raise ValueError(f"unknown NetworkPolicy: {policy!r}")
