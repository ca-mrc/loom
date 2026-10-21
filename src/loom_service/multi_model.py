"""Batch/trial validation helpers for mid-trajectory model switch (#1380)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import ValidationError

from loom.db.schema import ProviderConnection
from loom.models.trial import MultiModelSwitchSpec, materialize_multi_model_switch_episode
from loom.models.types import ModelSpec

_FORBIDDEN_CLIENT_INTERNALS = frozenset({"seed", "prng_version", "k1", "k2"})


def parse_multi_model(
    trial_config: dict[str, Any],
) -> MultiModelSwitchSpec | None:
    raw = trial_config.get("multi_model")
    if raw is None:
        return None
    if isinstance(raw, dict) and _FORBIDDEN_CLIENT_INTERNALS.intersection(raw):
        raise ValueError(
            "trial_config.multi_model must not include resolved seed internals "
            f"({sorted(_FORBIDDEN_CLIENT_INTERNALS)})",
        )
    return MultiModelSwitchSpec.model_validate(raw)


def validate_multi_model_for_batch(
    *,
    trial_config: dict[str, Any],
    agent_name: str | None,
    agent_model: ModelSpec | None,
    provider_connection_id: UUID | None,
    provider_connection: ProviderConnection | None,
    context: str = "trial_config",
) -> str | None:
    """Return an error detail string, or None when valid / disabled."""
    try:
        spec = parse_multi_model(trial_config)
    except (ValidationError, ValueError) as exc:
        return f"{context}.multi_model is invalid: {exc}"
    if spec is None or not spec.enabled:
        return None
    if agent_name != "terminus-2":
        return (
            f"{context}: multi_model.enabled requires agent_name "
            f"'terminus-2' (got {agent_name!r})"
        )
    if agent_model is None:
        return f"{context}: multi_model.enabled requires a non-null agent_model"
    if spec.secondary_model is None:
        return f"{context}: multi_model.secondary_model is required when enabled"
    if provider_connection_id is None:
        return (
            f"{context}: multi_model.enabled requires provider_connection_id "
            "(same BYO connection for primary and secondary models)"
        )
    if agent_model.provider != spec.secondary_model.provider:
        return (
            f"{context}: multi_model.secondary_model.provider "
            f"({spec.secondary_model.provider!r}) must match "
            f"agent_model.provider ({agent_model.provider!r})"
        )
    if agent_model.source != spec.secondary_model.source:
        return (
            f"{context}: multi_model.secondary_model.source "
            f"({spec.secondary_model.source!r}) must match "
            f"agent_model.source ({agent_model.source!r})"
        )
    if provider_connection is not None:
        allowed = provider_connection.allowed_models
        if allowed:
            for label, name in (
                ("agent_model", agent_model.name),
                ("multi_model.secondary_model", spec.secondary_model.name),
            ):
                if name not in allowed:
                    return (
                        f"{context}: {label} name {name!r} is not in "
                        "provider_connection.allowed_models"
                    )
        failed = getattr(provider_connection, "last_preflight_status", None)
        if failed == "failed":
            return (
                f"{context}: provider_connection preflight failed; "
                "refusing multi_model.enabled"
            )
    return None


def apply_plan_mode(
    trial_config: dict[str, Any],
    *,
    mode: str | None,
) -> dict[str, Any]:
    """inherit keeps the mix plan; resample redraws K1 or a new beta seed."""
    out = dict(trial_config)
    resolved = mode or out.get("model_switch_plan_mode") or "inherit"
    out["model_switch_plan_mode"] = resolved
    raw = out.get("multi_model")
    if not isinstance(raw, dict) or not raw.get("enabled"):
        return apply_multi_model_materialization(out)
    if resolved == "resample":
        mm = dict(raw)
        if raw.get("policy") in {"beta_mixture", "student_to_teacher_turns"}:
            # A frozen server seed would otherwise survive into clone-config
            # and rerun-failed. Drop it so materialize draws a new one.
            mm.pop("mix_seed", None)
        else:
            mm.pop("switch_episode", None)
            mm.pop("return_switch_episode", None)
        out["multi_model"] = mm
    return apply_multi_model_materialization(out)


def apply_multi_model_materialization(trial_config: dict[str, Any]) -> dict[str, Any]:
    """Copy trial_config and persist a concrete switch_episode when needed."""
    out = dict(trial_config)
    raw = out.get("multi_model")
    if raw is None:
        return out
    materialized = materialize_multi_model_switch_episode(raw)
    if materialized is not None:
        out["multi_model"] = materialized
    return out


def usage_by_role(rows: list[Any]) -> dict[str, Any]:
    """Aggregate llm_calls by student/teacher for API/SPA."""
    out: dict[str, dict[str, Any]] = {
        "student": {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0},
        "teacher": {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0},
        "uncorrelated": {
            "calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0,
        },
    }
    for row in rows:
        role = getattr(row, "role", None) or "uncorrelated"
        if role not in out:
            role = "uncorrelated"
        bucket = out[role]
        bucket["calls"] += 1
        bucket["input_tokens"] += int(getattr(row, "input_tokens", 0) or 0)
        bucket["output_tokens"] += int(getattr(row, "output_tokens", 0) or 0)
        cost = getattr(row, "cost_usd", 0) or 0
        bucket["cost_usd"] += float(cost)
    return out
