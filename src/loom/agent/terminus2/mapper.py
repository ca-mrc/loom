"""Terminus2TrajectoryMapper — derive Harbor-compatible ATIF steps from typed events (#745)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from loom.agent.terminus2.agent_message import format_agent_message, parse_agent_message
from loom.models.trajectory import (
    EventKind,
    LLMCallEvent,
    Terminus2CommandEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
    TrajectoryEvent,
)


class Terminus2TrajectoryMapper:
    """Project native terminus2 typed events into per-turn ATIF-like steps."""

    @staticmethod
    def project_to_atif(
        events: Iterable[TrajectoryEvent],
        *,
        task_id: str,
        agent_name: str,
        agent_version: str,
    ) -> dict[str, Any]:
        turns: dict[str, dict[str, Any]] = {}
        commands_by_turn: dict[str, list[Terminus2CommandEvent]] = {}
        observations_by_turn: dict[str, Terminus2TerminalObservationEvent] = {}
        llm_by_gateway: dict[str, LLMCallEvent] = {}
        user_prompts: list[Terminus2UserPromptEvent] = []
        switches: list[dict[str, Any]] = []

        for event in events:
            if event.kind == EventKind.LLM_CALL:
                llm_by_gateway[event.gateway_request_id] = event
            elif event.kind == EventKind.TERMINUS2_USER_PROMPT:
                user_prompts.append(event)
            elif event.kind == EventKind.TERMINUS2_TURN:
                turns[event.turn_id] = {
                    "turn_id": event.turn_id,
                    "turn_index": event.turn_index,
                    "gateway_request_id": event.gateway_request_id,
                    "parse_state": event.parse_state,
                    "completion_state": event.completion_state,
                    "analysis": event.analysis,
                    "plan": event.plan,
                    "raw_response_excerpt": event.raw_response_excerpt,
                    "reasoning_content": event.reasoning_content,
                    "harbor_step_id": event.harbor_step_id,
                }
            elif event.kind == EventKind.TERMINUS2_COMMAND:
                commands_by_turn.setdefault(event.turn_id, []).append(event)
            elif event.kind == EventKind.TERMINUS2_TERMINAL_OBSERVATION:
                observations_by_turn[event.turn_id] = event
            elif event.kind == EventKind.TERMINUS2_MODEL_SWITCH:
                switches.append(
                    {
                        "kind": "applied",
                        "switch_episode": event.switch_episode,
                        "from_role": event.from_role,
                        "to_role": event.to_role,
                        "from_model": event.from_model.model_dump(mode="json"),
                        "to_model": event.to_model.model_dump(mode="json"),
                    },
                )

        numbered: list[tuple[int, dict[str, Any]]] = []
        for prompt in sorted(user_prompts, key=lambda p: p.harbor_step_id):
            numbered.append(
                (
                    prompt.harbor_step_id,
                    {
                        "step_id": str(prompt.harbor_step_id),
                        "source": "user",
                        "message": prompt.message,
                        "is_initial_prompt": prompt.is_initial,
                        "prompt_id": prompt.prompt_id,
                    },
                ),
            )

        has_user_prompt = bool(user_prompts)
        for turn_id, turn in sorted(
            turns.items(),
            key=lambda item: item[1]["turn_index"],
        ):
            gw_id = turn["gateway_request_id"]
            llm = llm_by_gateway.get(gw_id)
            obs = observations_by_turn.get(turn_id)
            cmds = sorted(commands_by_turn.get(turn_id, []), key=lambda c: c.index)
            tool_calls = [
                {
                    "tool_call_id": cmd.command_id,
                    "function_name": "bash_command",
                    "arguments": {
                        "keystrokes": cmd.keystrokes,
                        "duration": cmd.duration_sec,
                    },
                }
                for cmd in cmds
            ]
            if turn["completion_state"] in ("pending_confirm", "complete"):
                tool_calls.append(
                    {
                        "tool_call_id": f"{turn_id}_task_complete",
                        "function_name": "mark_task_complete",
                        "arguments": {},
                    },
                )
            analysis = str(turn.get("analysis") or "")
            plan = str(turn.get("plan") or "")
            if not analysis and not plan:
                excerpt = str(turn.get("raw_response_excerpt") or "")
                if excerpt:
                    analysis, plan = parse_agent_message(excerpt)

            harbor_step_id = turn.get("harbor_step_id")
            if isinstance(harbor_step_id, int):
                step_number = harbor_step_id
            elif has_user_prompt:
                step_number = int(turn["turn_index"]) + 2
            else:
                step_number = int(turn["turn_index"]) + 1

            step: dict[str, Any] = {
                "step_id": str(step_number),
                "source": "agent",
                "turn_id": turn_id,
                "parse_state": turn["parse_state"],
                "completion_state": turn["completion_state"],
                "tool_calls": tool_calls or None,
                "observation": obs.text if obs else None,
                "observation_metadata": (
                    {
                        "capture_source": obs.capture_source,
                        "is_aggregate": obs.is_aggregate,
                        "completeness": obs.completeness,
                        "content_hash": obs.content_hash,
                    }
                    if obs
                    else None
                ),
            }
            if analysis:
                step["analysis"] = analysis
            if plan:
                step["plan"] = plan
            if analysis or plan:
                step["message"] = format_agent_message(analysis=analysis, plan=plan)
            elif turn.get("raw_response_excerpt"):
                step["message"] = str(turn["raw_response_excerpt"])
            reasoning = str(turn.get("reasoning_content") or "")
            # Always emit the ATIF field. Do not invent CoT from analysis —
            # analysis/plan are separate Terminus fields; true thinking only
            # comes from Harbor/provider ``reasoning_content``.
            step["reasoning_content"] = reasoning or None
            if llm is not None:
                step["metrics"] = {
                    "input_tokens": llm.input_tokens,
                    "output_tokens": llm.output_tokens,
                    "cached_input_tokens": llm.cached_input_tokens,
                    "cache_write_tokens": llm.cache_write_tokens,
                    "thinking_tokens": llm.thinking_tokens,
                    "cost_usd": llm.cost_usd_snapshot,
                }
                step["gateway_request_id"] = gw_id
                step["requested_model"] = llm.requested_model or llm.model.name
                step["response_model"] = llm.response_model
                if llm.role:
                    step["role"] = llm.role
            numbered.append((step_number, step))

        numbered.sort(key=lambda item: item[0])
        return {
            "schema_version": "harbor-tb2-v2-projection",
            "task_id": task_id,
            "agent_name": agent_name,
            "agent_version": agent_version,
            "steps": [step for _, step in numbered],
            "model_switches": switches,
        }

    @staticmethod
    def enrich_from_native(
        trajectory: dict[str, Any],
        native: dict[str, Any] | None,
        *,
        reasoning_by_gateway: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Backfill initial user prompt + reasoning_content from native/provider.

        Used when older ``loom_trajectory.jsonl`` trails lack typed user-prompt
        events or when Harbor/provider carried ``reasoning_content`` that the
        bridge did not yet emit.
        """
        steps = list(trajectory.get("steps") or [])
        native_steps = (
            native.get("steps") if isinstance(native, dict) else None
        ) or []
        has_user = any(
            isinstance(step, dict) and step.get("source") == "user" for step in steps
        )
        if not has_user:
            for native_step in native_steps:
                if (
                    isinstance(native_step, dict)
                    and native_step.get("source") == "user"
                    and isinstance(native_step.get("message"), str)
                    and native_step["message"]
                ):
                    steps.insert(
                        0,
                        {
                            "step_id": str(native_step.get("step_id") or 1),
                            "source": "user",
                            "message": native_step["message"],
                            "is_initial_prompt": True,
                        },
                    )
                    break

        agent_steps = [
            step for step in steps if isinstance(step, dict) and step.get("source") == "agent"
        ]
        native_agents = [
            step
            for step in native_steps
            if isinstance(step, dict) and step.get("source") == "agent"
        ]
        for index, step in enumerate(agent_steps):
            if not step.get("reasoning_content"):
                if reasoning_by_gateway:
                    gw = step.get("gateway_request_id")
                    if isinstance(gw, str) and reasoning_by_gateway.get(gw):
                        step["reasoning_content"] = reasoning_by_gateway[gw]
                if not step.get("reasoning_content") and index < len(native_agents):
                    native_rc = native_agents[index].get("reasoning_content")
                    if isinstance(native_rc, str) and native_rc:
                        step["reasoning_content"] = native_rc
            if "reasoning_content" not in step:
                step["reasoning_content"] = None

        # Keep Harbor-like numbering once a leading user prompt exists.
        if any(s.get("source") == "user" for s in steps if isinstance(s, dict)):
            for index, step in enumerate(steps, start=1):
                if isinstance(step, dict):
                    step["step_id"] = str(index)

        trajectory = dict(trajectory)
        trajectory["steps"] = steps
        return trajectory

    @staticmethod
    def validate_turn_joins(
        events: Iterable[TrajectoryEvent], *, allow_final_unobserved_turn: bool = False,
    ) -> list[str]:
        """Return validation errors for #745/#746 join invariants."""
        errors: list[str] = []
        turns: list[Terminus2TurnEvent] = []
        llm_ids: set[str] = set()
        obs_turns: set[str] = set()

        for event in events:
            if event.kind == EventKind.LLM_CALL:
                llm_ids.add(event.gateway_request_id)
            elif event.kind == EventKind.TERMINUS2_TURN:
                turns.append(event)
            elif event.kind == EventKind.TERMINUS2_TERMINAL_OBSERVATION:
                obs_turns.add(event.turn_id)

        final_turn_id = turns[-1].turn_id if allow_final_unobserved_turn and turns else None
        for turn in turns:
            if turn.parse_state != "ok":
                continue
            if turn.gateway_request_id not in llm_ids:
                errors.append(
                    f"turn {turn.turn_id}: missing LLMCallEvent for "
                    f"gateway_request_id={turn.gateway_request_id}",
                )
            if turn.turn_id not in obs_turns and turn.turn_id != final_turn_id:
                errors.append(
                    f"turn {turn.turn_id}: missing terminus2_terminal_observation",
                )
        return errors
