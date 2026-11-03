"""Pydantic models for Batch submission (Plan 28 PR-3).

A Batch carries a shared `trial_config` (every TrialConfig knob
except agent / model / n_per_task — those live on the
Combinations) plus a list of Combinations. Each Combination is one
(agent, model, n_per_task) tuple.

Backward compat: when `combinations` is empty, the Batch is
single-combination — agent_name / agent_model / n_per_task live on
trial_config + Batch.n_per_task as they did before this PR.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from loom.models.types import AgentVersion, ModelSpec


class Combination(BaseModel):
    """One (agent, model, n_per_task) tuple within a Batch.

    Harbor calls this `组合` (AgentModelCombination). Each
    Combination × Task × sample produces one Trial.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_name: str = Field(min_length=1)
    agent_version: AgentVersion | None = None
    # Required: the rule "every trial states which model" from PR-E
    # applies per-Combination. `null` is allowed when the agent
    # doesn't call an LLM (oracle, in-box runtimes), but the field
    # must be present.
    agent_model: ModelSpec | None
    claude_code_mode: Literal["native", "chat"] | None = Field(default=None, exclude_if=lambda value: value is None)

    # Per-Combination sample count. 1..100 (same bounds as Batch.n_per_task).
    n_per_task: int = Field(default=1, ge=1, le=100)

    # Display label. Unique within the Batch (route validates).
    # Falls back to a derived value `"{agent_name}"` or
    # `"{agent_name}/{provider}/{name}"` if None.
    label: str | None = Field(default=None, max_length=200)

    # Optional provider route for this Combination. When omitted, the
    # Batch-level provider_connection_id / provider_model_id remains the
    # backward-compatible default.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None
