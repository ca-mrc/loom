"""Real HTTP-backed LLMGatewayClient. Workers use this in production."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from loom.agent.gateway_client import (
    GatewayCallRequest,
    GatewayCallResponse,
)
from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.llm_http_client import create_model_http_client
from loom.models.trajectory import ChatMessage
from loom.request_params import sanitize_request_extras


@dataclass
class HttpLLMGatewayClient:
    """Talks HTTP to the Loom LLM Gateway. Constructed once per worker."""

    base_url: str
    token: str
    timeout_sec: float = 120.0
    attempt_deadline: AttemptDeadline | None = None
    # Optional pre-built client for tests (so ASGITransport / mocks can be injected).
    _client: httpx.AsyncClient | None = None
    _owns_client: bool = False

    def for_attempt(
        self,
        deadline: AttemptDeadline,
        *,
        token: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> HttpLLMGatewayClient:
        """Create an attempt-bound client with an independently closable transport.

        Passing ``client`` is useful for an injected test transport. Without
        one, the returned client owns a new transport so a future attempt
        supervisor can interrupt blocked I/O without closing the worker-wide
        Gateway client.
        """

        if client is None:
            client = create_model_http_client(
                base_url=self.base_url,
                timeout_sec=self.timeout_sec,
            )
            owns_client = True
        else:
            owns_client = False
        return HttpLLMGatewayClient(
            base_url=self.base_url,
            token=self.token if token is None else token,
            timeout_sec=self.timeout_sec,
            attempt_deadline=deadline,
            _client=client,
            _owns_client=owns_client,
        )

    def with_token(self, token: str) -> HttpLLMGatewayClient:
        """Share this client's transport and deadline under a scoped bearer."""

        return HttpLLMGatewayClient(
            base_url=self.base_url,
            token=token,
            timeout_sec=self.timeout_sec,
            attempt_deadline=self.attempt_deadline,
            _client=self._client,
        )

    async def aclose(self) -> None:
        """Close only a transport owned by this client."""

        if self._owns_client and self._client is not None:
            await self._client.aclose()

    def _request_budget_sec(self) -> float:
        if self.attempt_deadline is None:
            return self.timeout_sec
        return min(self.timeout_sec, self.attempt_deadline.require_remaining())

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        body: dict[str, Any] = {
            "model": request.model.to_gateway_model_string(),
            "messages": [
                m.model_dump(exclude_none=True) for m in request.messages
            ],
            "loom": {
                "team_id": request.team_id,
                "trial_id": request.trial_id,
                "step_id": request.step_id,
            },
        }
        if request.system_prompt is not None:
            # OpenAI wire format: system prompt is the first chat message.
            body["messages"] = (
                [{"role": "system", "content": request.system_prompt}]
                + body["messages"]
            )
        if request.tools is not None:
            body["tools"] = [t.model_dump() for t in request.tools]
        if request.tool_choice is not None:
            body["tool_choice"] = request.tool_choice
        if request.model.tier:
            body["loom"]["tier"] = request.model.tier
        if request.model.region:
            body["loom"]["region"] = request.model.region
        body.update(sanitize_request_extras(request.request_params))
        if (
            request.model.max_output_tokens is not None
            and "max_tokens" not in body
            and "max_completion_tokens" not in body
            and "max_output_tokens" not in body
        ):
            body["max_tokens"] = request.model.max_output_tokens
        # #178: forward BYO provider connection id so the gateway can
        # decrypt + use the team's stored credential.
        if request.provider_connection_id:
            body["loom"]["provider_connection_id"] = (
                request.provider_connection_id
            )

        # Read the absolute deadline immediately before transport dispatch. A
        # response that arrives after the boundary is discarded below.
        request_budget_sec = (
            None if self.attempt_deadline is None else self._request_budget_sec()
        )
        client = self._client or create_model_http_client(
            base_url=self.base_url,
            timeout_sec=self.timeout_sec,
        )
        try:
            if self.attempt_deadline is None:
                r = await client.post(
                    "/v1/chat/completions",
                    headers={"Authorization": f"Bearer {self.token}"},
                    json=body,
                )
            else:
                assert request_budget_sec is not None
                request_timeout = httpx.Timeout(request_budget_sec)
                try:
                    async with asyncio.timeout(request_budget_sec):
                        r = await client.post(
                            "/v1/chat/completions",
                            headers={"Authorization": f"Bearer {self.token}"},
                            json=body,
                            timeout=request_timeout,
                        )
                except (TimeoutError, httpx.TimeoutException) as exc:
                    if self.attempt_deadline.reached:
                        raise AttemptDeadlineExceededError(
                            "agent attempt deadline reached during gateway request",
                        ) from exc
                    raise
                if self.attempt_deadline.reached:
                    raise AttemptDeadlineExceededError(
                        "agent attempt deadline reached before gateway response",
                    )
            r.raise_for_status()
            payload = r.json()
        finally:
            if self._client is None:
                await client.aclose()

        loom_block = payload["loom"]
        return GatewayCallResponse(
            response=ChatMessage(**payload["choices"][0]["message"]),
            input_tokens=loom_block["input_tokens"],
            cached_input_tokens=loom_block["cached_input_tokens"],
            cache_write_tokens=loom_block["cache_write_tokens"],
            output_tokens=loom_block["output_tokens"],
            thinking_tokens=loom_block["thinking_tokens"],
            provider_extras=dict(loom_block["provider_extras"]),
            cost_usd=loom_block["cost_usd"],
            finish_reason=loom_block["finish_reason"],
            duration_sec=loom_block["duration_sec"],
            streamed=loom_block["streamed"],
            time_to_first_token_sec=loom_block.get("time_to_first_token_sec"),
            rate_card_hash=loom_block["rate_card_hash"],
            gateway_request_id=loom_block["gateway_request_id"],
        )
