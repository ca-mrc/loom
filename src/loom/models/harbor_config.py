"""Harbor Terminus-2 controls supplied on a batch (#2667).

Constructor fields are passed to ``Terminus2(...)``. Completion fields are
merged into ``request_params`` and then into Harbor ``llm_kwargs``. The split
matches Harbor 0.18: ``temperature`` binds to ``LiteLLM._temperature``, and the
other completion fields are spread into every ``litellm.acompletion`` call.
``drop_params=True`` keeps that set for an OpenAI-compatible endpoint.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ReasoningEffort = Literal[
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "default",
]

# Standard chat parameters Harbor forwards for an ``openai/`` model.
# Provider-specific knobs (top_k, min_p, mirostat, and similar) are omitted:
# LiteLLM drops them for this provider prefix.
_COMPLETION_FIELDS = (
    "temperature",
    "top_p",
    "max_tokens",
    "max_completion_tokens",
    "seed",
    "stop",
    "frequency_penalty",
    "presence_penalty",
    "reasoning_effort",
)


class HarborConfig(BaseModel):
    """Run controls for one Terminus-2 batch.

    ``agent_timeout_sec`` is the Loom attempt deadline (Harbor's
    ``--agent-timeout``), not a Terminus constructor argument.
    ``max_input_tokens`` becomes ``model_info`` for Harbor's local context
    accounting. The episode loop reads that value only when summarization
    is on.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_turns: int | None = Field(default=None, ge=1, le=1_000_000)
    agent_timeout_sec: float | None = Field(default=None, gt=0)
    max_input_tokens: int | None = Field(default=None, ge=1)
    enable_summarize: bool | None = None
    proactive_summarization_threshold: int | None = Field(default=None, ge=0)
    max_thinking_tokens: int | None = Field(default=None, ge=1024)
    reasoning_effort: ReasoningEffort | None = None

    temperature: float | None = Field(default=None, ge=0)
    top_p: float | None = Field(default=None, ge=0, le=1)
    max_tokens: int | None = Field(default=None, ge=1)
    max_completion_tokens: int | None = Field(default=None, ge=1)
    seed: int | None = None
    stop: str | list[str] | None = None
    frequency_penalty: float | None = Field(default=None, ge=-2, le=2)
    presence_penalty: float | None = Field(default=None, ge=-2, le=2)

    @model_validator(mode="after")
    def _one_output_limit(self) -> HarborConfig:
        if self.max_tokens is not None and self.max_completion_tokens is not None:
            raise ValueError("harbor_config accepts max_tokens or max_completion_tokens, not both")
        return self

    def completion_parameters(self) -> dict[str, Any]:
        """Sampling controls that belong in Harbor ``llm_kwargs``."""
        values: dict[str, Any] = {}
        for key in _COMPLETION_FIELDS:
            value = getattr(self, key)
            if value is not None:
                values[key] = value
        return values


def apply_harbor_config_dict(trial_config: dict[str, Any]) -> dict[str, Any]:
    """Copy Harbor controls onto the trial fields the rest of Loom already reads.

    ``agent_timeout_sec`` becomes ``override_agent_timeout_sec``. Completion
    fields are merged into ``request_params``. Constructor-only fields stay
    on ``harbor_config`` for the Terminus runtime.
    """
    raw = trial_config.get("harbor_config")
    if raw is None:
        return trial_config
    parsed = HarborConfig.model_validate(raw)
    out = dict(trial_config)
    out["harbor_config"] = parsed.model_dump(mode="json", exclude_none=True)
    if parsed.agent_timeout_sec is not None:
        existing = out.get("override_agent_timeout_sec")
        if existing is not None and float(existing) != parsed.agent_timeout_sec:
            raise ValueError(
                "harbor_config.agent_timeout_sec conflicts with override_agent_timeout_sec",
            )
        out["override_agent_timeout_sec"] = parsed.agent_timeout_sec
    params = dict(out.get("request_params") or {})
    for key, value in parsed.completion_parameters().items():
        current = params.get(key)
        if current is not None and current != value:
            raise ValueError(f"harbor_config.{key} conflicts with request_params.{key}")
        params[key] = value
    if params:
        out["request_params"] = params
    return out
