"""Advanced execution config: one document selecting all four hosted axes (#2314).

The document is input sugar. It maps onto the existing ``TrialConfig`` fields
and never forms a parallel config model; the server validates the resulting
combination as a whole.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import AgentVersion, IsolationSelection, VerifierEnvMode

EXECUTION_SELECTION_SCHEMA_VERSION = "loom.execution-selection.v1"


class _Strict(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HarnessSelectionV1(_Strict):
    name: str = Field(min_length=1)
    version: AgentVersion | None = None


class NetworkPolicySelectionV1(_Strict):
    mode: Literal["gateway-only", "web-allowlist", "public-web"]
    allow: tuple[str, ...] = ()


class ExecutionSelectionV1(_Strict):
    """Omitted fields keep that axis's default path."""

    schema_version: Literal["loom.execution-selection.v1"]
    harness: HarnessSelectionV1 | None = None
    network_policy: NetworkPolicySelectionV1 | None = None
    verification: VerifierEnvMode | None = None
    isolation: IsolationSelection | None = None


def execution_selection_json_schema() -> dict[str, Any]:
    return ExecutionSelectionV1.model_json_schema()


def execution_selection_readback(trial_config: Any, plan: Any | None) -> dict[str, Any]:
    """What a trial asked for on each axis, beside what its frozen attempt plan ran."""

    from loom.execution_runtime_contract import VerifierExecution

    config = trial_config if isinstance(trial_config, dict) else {}
    requested = {
        "harness": {"name": config.get("agent_name"), "version": config.get("agent_version")},
        "network_policy": config.get("baseline_network_policy_override"),
        "verification": config.get("verifier_env_mode"),
        "isolation": config.get("isolation") or "auto",
    }
    if plan is None:
        return {"requested": requested, "effective": None}
    verification = {
        VerifierExecution.IN_ATTEMPT: "shared",
        VerifierExecution.SEPARATE_EXECUTION: "separate",
        VerifierExecution.SKIPPED: "skipped",
    }[plan.verifier_execution]
    return {
        "requested": requested,
        "effective": {
            "execution_class_id": plan.execution_class_id,
            "network_policy": (
                plan.effective_network_policy.model_dump(mode="json")
                if plan.effective_network_policy is not None else None
            ),
            "verification": verification,
            "fresh_sandbox_grading": verification == "separate",
            "isolation": (
                "guest" if any(sidecar.guest_execution is not None for sidecar in plan.sidecars)
                else "container"
            ),
        },
    }


def resolved_execution_selection(task: TaskConfig, trial: TrialConfig) -> dict[str, Any]:
    """The four axes a trial resolves to before compile; ``None`` network means unsupported."""

    from loom.execution_contract import effective_guest_capabilities
    from loom.models.networking import (
        UnsupportedNetworkPolicyOverrideError,
        resolve_effective_network_policy,
    )
    from loom.verifier_runtime import resolve_verifier_env_mode

    try:
        policy: dict[str, Any] | None = resolve_effective_network_policy(
            baseline=task.environment.baseline_network_policy,
            supported=task.environment.network_policies_supported,
            override=trial.baseline_network_policy_override,
        ).model_dump(mode="json")
    except UnsupportedNetworkPolicyOverrideError:
        policy = None
    return {
        "harness": {"name": trial.agent_name, "version": trial.agent_version},
        "network_policy": policy,
        "verification": resolve_verifier_env_mode(task, trial),
        "isolation": "guest" if effective_guest_capabilities(task, trial) is not None else "container",
    }
