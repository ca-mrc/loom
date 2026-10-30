"""Wrap LiteLLM acompletion + parse the response into a Loom-shaped struct.

LiteLLM normalises across providers but provider-specific usage fields still
leak through. We parse them into typed counters + a `provider_extras` dict
of named int counters (spec §4.4.1 — provider_extras is `dict[str, int]`,
not opaque).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import litellm

from loom.llm_sdk_clients import ModelSDKClients


@dataclass(frozen=True)
class ParsedResponse:
    response_content: str
    finish_reason: str
    input_tokens: int
    cached_input_tokens: int
    cache_write_tokens: int
    output_tokens: int
    thinking_tokens: int
    provider_extras: dict[str, int]
    raw_response: dict[str, Any]


_KNOWN_USAGE_KEYS = frozenset({
    "prompt_tokens", "completion_tokens", "total_tokens", "thinking_tokens",
    # Anthropic
    "cache_creation_input_tokens", "cache_read_input_tokens",
})


def parse_litellm_response(
    response: dict[str, Any], *, provider: str,
) -> ParsedResponse:
    choice = response["choices"][0]
    msg = choice["message"]
    content = msg.get("content", "")
    if content is None:
        # Tool-only responses may omit text without omitting the tool calls.
        content = ""
    elif not isinstance(content, str):
        # Multimodal content list — collapse to text fragments
        parts = [
            p.get("text", "") for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        ]
        content = "".join(parts)

    usage = response.get("usage", {}) or {}
    input_tokens = int(usage.get("prompt_tokens", 0) or 0)
    output_tokens = int(usage.get("completion_tokens", 0) or 0)
    thinking_tokens = int(usage.get("thinking_tokens", 0) or 0)

    cached_input_tokens = int(usage.get("cache_read_input_tokens", 0) or 0)
    cache_write_tokens = int(usage.get("cache_creation_input_tokens", 0) or 0)

    provider_extras: dict[str, int] = {}
    for k, v in usage.items():
        if k in _KNOWN_USAGE_KEYS:
            continue
        if isinstance(v, int):
            provider_extras[k] = v
    # Surface the cache counters in provider_extras too so downstream tools
    # that want to inspect named counters don't have to know we already
    # mapped them into typed fields.
    if "cache_creation_input_tokens" in usage:
        provider_extras["cache_creation_input_tokens"] = int(
            usage["cache_creation_input_tokens"],
        )
    if "cache_read_input_tokens" in usage:
        provider_extras["cache_read_input_tokens"] = int(
            usage["cache_read_input_tokens"],
        )

    return ParsedResponse(
        response_content=content,
        finish_reason=choice.get("finish_reason", "stop"),
        input_tokens=input_tokens,
        cached_input_tokens=cached_input_tokens,
        cache_write_tokens=cache_write_tokens,
        output_tokens=output_tokens,
        thinking_tokens=thinking_tokens,
        provider_extras=provider_extras,
        raw_response=response,
    )


async def acompletion(
    *,
    model: str,
    messages: list[dict[str, Any]],
    api_key: str | None = None,
    timeout: float = 120.0,
    model_clients: ModelSDKClients | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Forward to litellm.acompletion. Thin wrapper so we can stub it in tests."""
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "timeout": timeout,
    }
    if api_key:
        kwargs["api_key"] = api_key
    kwargs.update(extra)
    clients = model_clients or ModelSDKClients(timeout_sec=timeout)
    try:
        kwargs["client"] = clients.client_for(
            model=model, api_key=api_key, api_base=extra.get("api_base"),
        )
        response = await litellm.acompletion(**kwargs)
        if hasattr(response, "model_dump"):
            return response.model_dump()  # type: ignore[no-any-return]
        return dict(response)
    finally:
        if model_clients is None:
            await clients.aclose()
