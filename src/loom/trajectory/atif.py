"""ATIF v1.7 schema models + project_to_atif (spec §4.8).

Schema is intentionally compact: trajectory → metadata + steps[]. Each step
records whether an LLM call happened, the messages exchanged, metrics, and
reasoning content.
"""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.models.trajectory import (
    AgentThoughtEvent,
    ChatMessage,
    LLMCallEvent,
    StepStartEvent,
    ToolUseEvent,
    TrajectoryEvent,
    TrialEndEvent,
    TrialErrorEvent,
    TrialStartEvent,
    VerifierEndEvent,
    VerifierStartEvent,
)
from loom.trajectory.llm_call_events import llm_call_diagnostic_counts

# Bumped whenever project_to_atif's logic changes such that a re-projection
# of the same events would no longer be byte-identical. Mixed into the
# deterministic trajectory_id hash so re-runs after a logic change are
# distinguishable from prior runs.
PROJECTION_VERSION = "2"


class AtifMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    task_id: str
    agent_name: str
    agent_version: str
    final_state: Literal["succeeded", "failed", "cancelled"] | None = None
    reward: dict[str, float] | None = None
    error: dict[str, Any] | None = None
    verifier_name: str | None = None
    verifier_rewards: dict[str, float] | None = None


class AtifStepMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    cache_write_tokens: int
    thinking_tokens: int
    cost_usd: float


class AtifStep(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_id: str
    llm_call_count: int = Field(ge=0)
    is_copied_context: bool
    messages: list[dict[str, Any]] | None = None
    reasoning_content: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    metrics: AtifStepMetrics | None = None

    @model_validator(mode="after")
    def _consistency(self) -> AtifStep:
        if self.llm_call_count == 0:
            for field_name in ("messages", "reasoning_content", "metrics"):
                if getattr(self, field_name) is not None:
                    raise ValueError(
                        f"llm_call_count == 0 → `{field_name}` must be absent",
                    )
        if self.llm_call_count > 0 and self.metrics is None:
            raise ValueError("llm_call_count > 0 → `metrics` required")
        return self


class AtifTrajectory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["1.7"] = "1.7"
    trajectory_id: str
    session_id: str
    metadata: AtifMetadata
    steps: list[AtifStep]


def project_to_atif(
    events: Iterable[TrajectoryEvent],
    *,
    task_id: str,
    agent_name: str,
    agent_version: str,
) -> AtifTrajectory:
    """Pure transform: Loom event log → ATIF v1.7 document (spec §4.8).

    Deterministic: same input + same projection version → byte-identical
    output (modulo dict ordering). Re-runnable for ATIF schema bumps.

    Raises ValueError if no TrialStartEvent is encountered — the trial_id
    is the canonical session_id and we don't invent one.
    """
    step_order: list[str] = []
    step_calls: dict[str, list[LLMCallEvent]] = {}
    step_tools: dict[str, list[ToolUseEvent]] = {}
    step_thoughts: dict[str, list[AgentThoughtEvent]] = {}

    metadata_fields: dict[str, Any] = {
        "task_id": task_id,
        "agent_name": agent_name,
        "agent_version": agent_version,
    }
    trial_id: UUID | None = None

    def _bucket(step_id: str) -> None:
        if step_id not in step_calls:
            step_order.append(step_id)
            step_calls[step_id] = []
            step_tools[step_id] = []
            step_thoughts[step_id] = []

    for event in events:
        if isinstance(event, TrialStartEvent):
            trial_id = event.trial_id
            if event.tool_network_profile is not None:
                metadata_fields["tool_network_profile"] = event.tool_network_profile
        elif isinstance(event, StepStartEvent):
            _bucket(event.step_id)
        elif isinstance(event, LLMCallEvent):
            _bucket(event.step_id)
            step_calls[event.step_id].append(event)
        elif isinstance(event, ToolUseEvent):
            _bucket(event.step_id)
            step_tools[event.step_id].append(event)
        elif isinstance(event, AgentThoughtEvent):
            _bucket(event.step_id)
            step_thoughts[event.step_id].append(event)
        elif isinstance(event, VerifierStartEvent):
            # Last verifier_name wins if multiple verifier passes.
            metadata_fields["verifier_name"] = event.verifier_name
        elif isinstance(event, VerifierEndEvent):
            metadata_fields["verifier_rewards"] = dict(event.result.rewards)
        elif isinstance(event, TrialEndEvent):
            metadata_fields["final_state"] = event.final_state
            if event.reward is not None:
                metadata_fields["reward"] = dict(event.reward)
            if event.failure_reason is not None:
                metadata_fields["error"] = {"failure_reason": event.failure_reason}
        elif isinstance(event, TrialErrorEvent):
            metadata_fields["error"] = {
                "type": event.error_type,
                "message": event.message,
            }

    if trial_id is None:
        raise ValueError(
            "project_to_atif: no TrialStartEvent in event log — refusing to "
            "invent a session_id. Truncated/corrupt trajectory?",
        )

    steps = [
        _project_step(
            step_id,
            step_calls.get(step_id, []),
            step_tools.get(step_id, []),
            step_thoughts.get(step_id, []),
        )
        for step_id in step_order
    ]

    session_id = str(trial_id)
    # Deterministic: derived from session_id + projection version. Same
    # input → same trajectory_id, every run.
    trajectory_id = sha256(
        f"loom-atif/{PROJECTION_VERSION}/{session_id}".encode(),
    ).hexdigest()

    return AtifTrajectory(
        trajectory_id=trajectory_id,
        session_id=session_id,
        metadata=AtifMetadata(**metadata_fields),
        steps=steps,
    )


def _project_step(
    step_id: str,
    calls: list[LLMCallEvent],
    tools: list[ToolUseEvent],
    thoughts: list[AgentThoughtEvent],
) -> AtifStep:
    n = len(calls)
    tool_calls: list[dict[str, Any]] | None = (
        [
            {"name": t.tool_name, "args": t.args, "result": t.result, "error": t.error}
            for t in tools
        ]
        if tools
        else None
    )

    if n == 0:
        # llm_call_count == 0: messages/metrics/reasoning must be absent.
        return AtifStep(
            step_id=step_id,
            llm_call_count=0,
            is_copied_context=False,
            tool_calls=tool_calls,
        )

    metrics = AtifStepMetrics(
        input_tokens=sum(c.input_tokens for c in calls),
        output_tokens=sum(c.output_tokens for c in calls),
        cached_input_tokens=sum(c.cached_input_tokens for c in calls),
        cache_write_tokens=sum(c.cache_write_tokens for c in calls),
        thinking_tokens=sum(c.thinking_tokens for c in calls),
        cost_usd=sum(c.cost_usd_snapshot for c in calls),
        **llm_call_diagnostic_counts(calls),
    )

    last_call = calls[-1]
    chat_messages: list[ChatMessage] = []
    if last_call.system_prompt:
        chat_messages.append(ChatMessage(role="system", content=last_call.system_prompt))
    chat_messages.extend(last_call.messages)
    chat_messages.append(last_call.response)
    messages = [m.model_dump() for m in chat_messages]

    reasoning = "\n---\n".join(t.content for t in thoughts) if thoughts else None

    return AtifStep(
        step_id=step_id,
        llm_call_count=n,
        is_copied_context=False,
        messages=messages,
        reasoning_content=reasoning,
        tool_calls=tool_calls,
        metrics=metrics,
    )
