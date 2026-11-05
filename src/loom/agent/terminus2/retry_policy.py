"""Gateway transport retry ownership for the pinned Harbor adapter (#2805)."""

from __future__ import annotations

from types import MethodType
from typing import Any

from loom.agent.terminus2.model_switch import LoomRoleRouter


def _single_attempt(owner: Any, name: str) -> None:
    method = getattr(owner, name, None)
    original = getattr(method, "__wrapped__", None)
    if callable(getattr(method, "retry_with", None)) and callable(original):
        # Tenacity's wrapper is bypassed only on this instance. Keep Harbor's
        # query implementation, including its context/summarization strategies.
        setattr(owner, name, MethodType(original, owner))


def install_gateway_retry_policy(agent: Any) -> None:
    """Let the Gateway own retries, without multiplying them in its client.

    Pinned Harbor wraps both LiteLLM.call and Terminus2._query_llm in three
    attempts; OpenAI adds another three. Summary calls reuse the same delegate
    and kwargs. Apply after teacher construction so both delegates participate.
    Direct-provider runs retain Harbor/SDK behavior. Remove this adapter when
    Harbor exposes equivalent per-instance transport retry ownership.
    """
    agent._llm_call_kwargs.update(max_retries=0, num_retries=0)
    _single_attempt(agent, "_query_llm")
    llm = getattr(agent, "_llm", None)
    delegates = (llm.student, llm.teacher) if isinstance(llm, LoomRoleRouter) else (llm,)
    for delegate in delegates:
        _single_attempt(delegate, "call")
