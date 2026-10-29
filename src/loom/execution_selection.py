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
from loom.models.types import AgentVersion, IsolationSelection

EXECUTION_SELECTION_SCHEMA_VERSION = "loom.execution-selection.v1"
ExecutionVerificationSelection = Literal["shared", "separate", "skipped"]


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
    verification: ExecutionVerificationSelection | None = None
    isolation: IsolationSelection | None = None


def execution_selection_json_schema() -> dict[str, Any]:
    return ExecutionSelectionV1.model_json_schema()


def requested_verification(trial_config: Any) -> ExecutionVerificationSelection | None:
    """The verification axis the trial asked for. Skip is stored as ``skip_verifier``."""

    config = trial_config if isinstance(trial_config, dict) else {}
    if config.get("skip_verifier") is True:
        return "skipped"
    mode = config.get("verifier_env_mode")
    if mode == "shared":
        return "shared"
    if mode == "separate":
        return "separate"
    return None


def apply_verification_selection(
    trial_config: dict[str, Any],
    verification: str | None,
    *,
    purpose: str,
) -> str | None:
    """Map the verification axis onto ``TrialConfig`` fields. Returns a CLI error or None."""

    if verification == "skipped":
        if purpose != "trajectory_generation":
            return "verification skipped is only allowed with --purpose trajectory_generation."
        trial_config["skip_verifier"] = True
        return None
    if verification in {"shared", "separate"}:
        trial_config["verifier_env_mode"] = verification
        return None
    return None


def reported_verification(
    verifier_execution: str | None, *, in_place_verifier: bool,
) -> tuple[str | None, bool]:
    """Map a frozen attempt plan onto the verification axis.

    ``in_attempt`` is the pod that grades, not the axis. Shared grading runs
    inside the task sandbox (``in_place_verifier``). A fresh verifier sandbox
    that stays in that pod — guest separate, or a verifier kept beside retained
    services — is still separate. A deferred child execution is separate too.
    """

    if verifier_execution == "skipped":
        return "skipped", False
    fresh_sandbox = verifier_execution == "separate_execution" or (
        verifier_execution == "in_attempt" and not in_place_verifier
    )
    if fresh_sandbox:
        return "separate", True
    if verifier_execution == "in_attempt":
        return "shared", False
    return verifier_execution, False


def execution_selection_readback(trial_config: Any, plan: Any | None) -> dict[str, Any]:
    """What a trial asked for on each axis, beside what its frozen attempt plan ran."""

    config = trial_config if isinstance(trial_config, dict) else {}
    agent_name = config.get("agent_name")
    requested = {
        "harness": {"name": agent_name, "version": config.get("agent_version")} if agent_name else None,
        "network_policy": config.get("baseline_network_policy_override"),
        "verification": requested_verification(config),
        "isolation": config.get("isolation") or "auto",
    }
    if "mainland_proxy" in config:
        requested["mainland_proxy"] = config["mainland_proxy"]
    if plan is None:
        return {"requested": requested, "effective": None}
    verification, fresh_sandbox_grading = reported_verification(
        plan.verifier_execution, in_place_verifier=plan.in_place_verifier,
    )
    readback: dict[str, Any] = {
        "requested": requested,
        "effective": {
            "execution_class_id": plan.execution_class_id,
            "network_policy": (
                plan.effective_network_policy.model_dump(mode="json")
                if plan.effective_network_policy is not None else None
            ),
            "verification": verification,
            "fresh_sandbox_grading": fresh_sandbox_grading,
            "isolation": (
                "guest" if any(sidecar.guest_execution is not None for sidecar in plan.sidecars)
                else "container"
            ),
        },
    }
    mainland_profile = getattr(plan, "mainland_proxy_profile", None)
    if mainland_profile is not None:
        readback["effective"]["mainland_proxy"] = {
            "enabled": True,
            "provider_version": mainland_profile.provider_version,
            "scope": "simulated mainland tool egress",
        }
    return readback


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
            override=trial.baseline_network_policy_override,
        ).model_dump(mode="json")
    except UnsupportedNetworkPolicyOverrideError:
        policy = None
    return {
        "harness": {"name": trial.agent_name, "version": trial.agent_version},
        "network_policy": policy,
        "verification": (
            "skipped" if trial.skip_verifier else resolve_verifier_env_mode(task, trial)
        ),
        "isolation": "guest" if effective_guest_capabilities(task, trial) is not None else "container",
    }
