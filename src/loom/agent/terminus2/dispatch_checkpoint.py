"""Persist Harbor's parsed current turn before command execution begins."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from loom.errors import AgentError


def install_dispatch_checkpoint(
    agent: Any,
    *,
    record: Callable[[dict[str, Any], int], Awaitable[None]],
    require_active: Callable[[], None],
) -> None:
    """Hook the pinned Harbor parse/dispatch boundary, instance locally.

    The upstream agent writes its Step only after ``_execute_commands``. A
    shell-exit exception in that call otherwise erases the current model turn.
    Neither a failed persistence nor an expired/fenced attempt may send keys.
    """
    original = getattr(agent, "_handle_llm_interaction", None)
    execute = getattr(agent, "_execute_commands", None)
    if not callable(original) or not callable(execute):
        raise AgentError("Harbor is missing the parsed-command checkpoint boundary")

    async def handle(chat: Any, *args: Any, **kwargs: Any) -> Any:
        before = (chat.total_input_tokens, chat.total_output_tokens, chat.total_cache_tokens)
        result = await original(chat, *args, **kwargs)
        commands, complete, feedback, analysis, plan, response = result
        if feedback and "ERROR:" in feedback:
            return result  # Parse errors are recorded by Harbor and never dispatched.
        require_active()
        episode = int(agent._n_episodes)
        calls = [{"tool_call_id": f"call_{episode - 1}_{i + 1}",
                  "function_name": "bash_command",
                  "arguments": {"keystrokes": cmd.keystrokes, "duration": cmd.duration_sec}}
                 for i, cmd in enumerate(commands)]
        if complete:
            calls.append({"function_name": "mark_task_complete", "arguments": {}})
        step = {
            "step_id": len(agent._trajectory_steps) + 1,
            "source": "agent",
            "message": f"Analysis: {analysis or ''}\nPlan: {plan or ''}",
            "reasoning_content": getattr(response, "reasoning_content", "") or "",
            "metrics": {
                "prompt_tokens": chat.total_input_tokens - before[0],
                "completion_tokens": chat.total_output_tokens - before[1],
                "cached_tokens": chat.total_cache_tokens - before[2],
            },
            "tool_calls": calls,
        }
        await record(step, episode)
        require_active()
        return result

    async def execute_with_fence(*args: Any, **kwargs: Any) -> Any:
        require_active()
        return await execute(*args, **kwargs)

    agent._handle_llm_interaction = handle
    agent._execute_commands = execute_with_fence
