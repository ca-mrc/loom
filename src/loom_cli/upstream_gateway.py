"""UpstreamDirectGatewayClient — implements LLMGatewayClient against
upstream provider SDKs directly. Used by the stateless `loom run` CLI
where a real Loom Gateway service isn't available.

Cost is computed locally from `~/.config/loom/rate-cards.toml`. The
returned `GatewayCallResponse` matches the same dataclass the
Worker/Trial expect, so Trial.run() doesn't know it's running outside
the service stack.

Implements:
  - anthropic dialect → anthropic.AsyncAnthropic.messages.create
  - openai dialect    → openai.AsyncOpenAI.chat.completions.create
  - google dialect    → google.genai.Client.aio.models.generate_content
  - local dialect     → openai.AsyncOpenAI client against a
                        user-registered local OpenAI-compatible server
                        (vLLM, ollama, llama.cpp, lm-studio). Model
                        spec form: `local/<server>/<model_id>`.
                        See LoomConfig.local_providers + `loom config
                        set local.<server>.base_url URL`.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any

from loom.agent.gateway_client import GatewayCallRequest, GatewayCallResponse
from loom.models.trajectory import ChatMessage
from loom_cli.config import LocalProvider
from loom_cli.rate_cards import (
    RateCardTable,
    compute_cost_usd,
    load_rate_cards,
    lookup_entry,
    seed_default_if_missing,
)


@dataclass
class UpstreamDirectGatewayClient:
    """Stateless LLM Gateway implementation. Instantiate once per `loom run`."""

    anthropic_client: Any
    openai_client: Any
    google_client: Any
    tokens: dict[str, str]
    local_providers: dict[str, LocalProvider] = field(default_factory=dict)
    _rate_cards: RateCardTable | None = None
    # Per-server openai.AsyncOpenAI client cache so we don't reconstruct
    # one per call when the same local server is hit repeatedly across a
    # multi-turn agent loop.
    _local_clients: dict[str, Any] = field(default_factory=dict)

    def _table(self) -> RateCardTable:
        if self._rate_cards is None:
            seed_default_if_missing()
            self._rate_cards = load_rate_cards()
        return self._rate_cards

    def _rate_card_hash(self) -> str:
        text = repr(tuple(
            (e.provider, e.model, e.input_per_mtok, e.output_per_mtok,
             e.cache_read_per_mtok, e.cache_write_per_mtok)
            for e in self._table().entries
        ))
        return hashlib.sha256(text.encode()).hexdigest()

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        provider = request.model.provider
        if provider == "anthropic":
            return await self._call_anthropic(request)
        if provider == "openai":
            return await self._call_openai(request)
        if provider == "google":
            return await self._call_google(request)
        if provider == "local":
            return await self._call_local(request)
        raise ValueError(f"unsupported provider for upstream-direct: {provider!r}")

    async def _call_anthropic(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("anthropic"):
            raise ValueError(
                "no anthropic API key — run `loom config set token.anthropic <key>` "
                "or export ANTHROPIC_API_KEY",
            )
        msgs = [{"role": m.role, "content": m.content} for m in request.messages]
        kwargs: dict[str, Any] = {
            "model": request.model.name,
            "messages": msgs,
            "max_tokens": 4096,
        }
        if request.system_prompt is not None:
            kwargs["system"] = request.system_prompt
        t0 = time.monotonic()
        raw = await self.anthropic_client.messages.create(**kwargs)
        dur = time.monotonic() - t0

        text_parts: list[str] = []
        for block in raw.content:
            if getattr(block, "type", None) == "text":
                text_parts.append(block.text)
        text = "".join(text_parts)

        usage = raw.usage
        in_tok = int(getattr(usage, "input_tokens", 0))
        out_tok = int(getattr(usage, "output_tokens", 0))
        cache_w = int(getattr(usage, "cache_creation_input_tokens", 0) or 0)
        cache_r = int(getattr(usage, "cache_read_input_tokens", 0) or 0)

        entry = lookup_entry(
            self._table(), provider="anthropic", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cache_r, cache_write_tokens=cache_w,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cache_r,
            cache_write_tokens=cache_w,
            output_tokens=out_tok,
            thinking_tokens=0,
            provider_extras={
                "cache_read_input_tokens": cache_r,
                "cache_creation_input_tokens": cache_w,
            },
            cost_usd=cost,
            finish_reason="stop" if str(getattr(raw, "stop_reason", "")) in {
                "end_turn", "stop_sequence",
            } else str(getattr(raw, "stop_reason", "unknown")),
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )

    async def _call_openai(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("openai"):
            raise ValueError(
                "no openai API key — run `loom config set token.openai <key>` "
                "or export OPENAI_API_KEY",
            )
        msgs: list[dict[str, Any]] = []
        if request.system_prompt is not None:
            msgs.append({"role": "system", "content": request.system_prompt})
        for m in request.messages:
            msgs.append({"role": m.role, "content": m.content})
        kwargs: dict[str, Any] = {"model": request.model.name, "messages": msgs}
        t0 = time.monotonic()
        raw = await self.openai_client.chat.completions.create(**kwargs)
        dur = time.monotonic() - t0

        choice = raw.choices[0]
        text = choice.message.content or ""
        finish = str(choice.finish_reason or "stop")

        usage = raw.usage
        in_tok = int(getattr(usage, "prompt_tokens", 0))
        out_tok = int(getattr(usage, "completion_tokens", 0))
        details = getattr(usage, "prompt_tokens_details", None)
        cached = int(getattr(details, "cached_tokens", 0) or 0) if details else 0

        entry = lookup_entry(
            self._table(), provider="openai", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cached, cache_write_tokens=0,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cached,
            cache_write_tokens=0,
            output_tokens=out_tok,
            thinking_tokens=0,
            provider_extras={"cached_tokens": cached},
            cost_usd=cost,
            finish_reason=finish,
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )

    async def _call_google(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("google"):
            raise ValueError(
                "no google API key — run `loom config set token.google <key>` "
                "or export GOOGLE_API_KEY",
            )
        contents = [
            {"role": "user" if m.role == "user" else "model",
             "parts": [{"text": m.content}]}
            for m in request.messages
        ]
        t0 = time.monotonic()
        raw = await self.google_client.aio.models.generate_content(
            model=request.model.name,
            contents=contents,
            config={"system_instruction": request.system_prompt},
        )
        dur = time.monotonic() - t0

        candidates = raw.candidates or []
        cand = candidates[0] if candidates else None
        content = getattr(cand, "content", None)
        parts = getattr(content, "parts", None) or []
        text = "".join(
            part.text for part in parts
            if isinstance(part.text, str) and not part.thought
        )
        reason = getattr(cand, "finish_reason", None)
        if cand is None:
            reason = getattr(raw.prompt_feedback, "block_reason", None)
        # The supported SDK exposes string enums, not the legacy numeric enum.
        reason_value = str(getattr(reason, "value", reason) or "")
        if reason_value == "STOP":
            finish = "stop"
        elif reason_value == "MAX_TOKENS":
            finish = "length"
        elif reason_value in {
            "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII",
            "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION",
        }:
            finish = "content_filter"
        elif reason_value in {"", "FINISH_REASON_UNSPECIFIED", "BLOCK_REASON_UNSPECIFIED"}:
            finish = "unknown"
        else:
            finish = reason_value.lower()

        usage = raw.usage_metadata
        in_tok = int(getattr(usage, "prompt_token_count", 0) or 0)
        out_tok = int(getattr(usage, "candidates_token_count", 0) or 0)
        cached = int(getattr(usage, "cached_content_token_count", 0) or 0)
        thinking = int(getattr(usage, "thoughts_token_count", 0) or 0)

        entry = lookup_entry(
            self._table(), provider="google", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cached, cache_write_tokens=0,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cached,
            cache_write_tokens=0,
            output_tokens=out_tok,
            thinking_tokens=thinking,
            provider_extras={
                "cachedContentTokenCount": cached,
                "thoughtsTokenCount": thinking,
            },
            cost_usd=cost,
            finish_reason=finish,
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )

    def _get_local_client(self, server_name: str) -> Any:
        """Return a cached `openai.AsyncOpenAI` client aimed at the
        named local server's `base_url`. Lazy-imports `openai` so the
        local path doesn't force-load the OpenAI SDK on calls that
        only use anthropic / google."""
        cached = self._local_clients.get(server_name)
        if cached is not None:
            return cached
        provider = self.local_providers.get(server_name)
        if provider is None:
            registered = sorted(self.local_providers.keys()) or ["(none)"]
            raise ValueError(
                f"local provider {server_name!r} not registered. "
                f"Run `loom config set local.{server_name}.base_url URL` "
                f"to register it (URL is the OpenAI-compatible endpoint, "
                f"typically ending in /v1). Currently registered: "
                f"{registered}.",
            )
        try:
            import openai
        except ImportError as exc:  # pragma: no cover — dep declared in pyproject
            raise RuntimeError(
                "openai SDK not installed but a local provider was requested. "
                "`pip install openai` to fix.",
            ) from exc
        # Local servers without auth still expect *some* string. vLLM
        # without `--api-key` accepts any value; ollama ignores it.
        # Use "EMPTY" as the universal placeholder when the user didn't
        # configure an api_key.
        api_key = provider.api_key or "EMPTY"
        client = openai.AsyncOpenAI(base_url=provider.base_url, api_key=api_key)
        self._local_clients[server_name] = client
        return client

    async def _call_local(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        # request.model.name is "<server>/<model_id>"; split on the first
        # slash to get the registered server name + the model id the
        # local server publishes via /v1/models.
        full = request.model.name
        if "/" not in full:
            raise ValueError(
                f"local model spec must be `local/<server>/<model>`; "
                f"got `local/{full}` (missing server / model split).",
            )
        server_name, model_id = full.split("/", 1)
        client = self._get_local_client(server_name)

        msgs: list[dict[str, Any]] = []
        if request.system_prompt is not None:
            msgs.append({"role": "system", "content": request.system_prompt})
        for m in request.messages:
            msgs.append({"role": m.role, "content": m.content})
        t0 = time.monotonic()
        raw = await client.chat.completions.create(model=model_id, messages=msgs)
        dur = time.monotonic() - t0

        choice = raw.choices[0]
        text = choice.message.content or ""
        finish = str(choice.finish_reason or "stop")

        usage = raw.usage
        in_tok = int(getattr(usage, "prompt_tokens", 0) or 0)
        out_tok = int(getattr(usage, "completion_tokens", 0) or 0)
        # NOTE: many "reasoning" models served locally (e.g. DeepSeek-R1
        # via vLLM) emit `reasoning_content` and a `completion_tokens`
        # breakdown that includes a reasoning-only sub-count. The
        # OpenAI SDK surfaces it as `usage.completion_tokens_details.
        # reasoning_tokens`. TODO: thread that into `thinking_tokens`
        # the same way the anthropic and gemini paths do for their
        # native counters — gap matches the existing _call_openai
        # path (also hard-codes thinking_tokens=0).

        # Cost: $0 by default for local. Allow override via a rate-card
        # entry keyed (provider=f"local:{server}", model=model_id) — for
        # users who want internal cost-tracking against their own GPU
        # rates.
        entry = lookup_entry(
            self._table(),
            provider=f"local:{server_name}",
            model=model_id,
            default_zero=True,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=0, cache_write_tokens=0,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=0,
            cache_write_tokens=0,
            output_tokens=out_tok,
            thinking_tokens=0,
            # No provider-specific counters (provider_extras is typed
            # dict[str, int]). The dispatched-against server is
            # encoded in the gateway_request_id prefix below.
            provider_extras={},
            cost_usd=cost,
            finish_reason=finish,
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-local-{int(t0 * 1000)}",
        )
