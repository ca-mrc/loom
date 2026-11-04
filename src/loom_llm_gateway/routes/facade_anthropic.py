"""Connection-bound Messages facade for native Anthropic or explicit Chat translation.

The sandbox supplies only a step JWT. Connection ownership, the current lease,
selected model and frozen Claude mode are checked before provider dispatch.
The legacy platform-key /v1/messages route remains independent.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import StreamingResponse

from loom.anthropic_chat_compat import AnthropicCompatibilityError, anthropic_to_chat
from loom.anthropic_protocol import anthropic_api_base_url
from loom.provider_connection_routes import ROUTE_PROVIDER_TYPES, ProviderConnectionRouting
from loom_llm_gateway.execution_attempt_dispatch import authorize_anthropic_trial_route
from loom_llm_gateway.routes._facade_common import (
    decrypt_facade_api_key,
    resolve_facade_connection,
    resolve_provider_connection_id,
    verify_facade_auth,
)
from loom_llm_gateway.routes.anthropic_dispatch import MessagesDispatch

router = APIRouter()


@router.post("/anthropic/v1/messages", response_model=None)
async def anthropic_messages_facade(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
    x_loom_provider_connection_id: str | None = Header(default=None, alias="x-loom-provider-connection-id"),
) -> dict[str, Any] | StreamingResponse:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session, authorization or (f"Bearer {x_api_key}" if x_api_key else None),
            settings.step_jwt_signing_key.get_secret_value(), request=request,
        )
    assert ctx.team_id is not None
    connection_id = resolve_provider_connection_id(ctx, x_loom_provider_connection_id)
    if not isinstance(payload.get("model"), str) or not payload["model"]:
        raise HTTPException(status_code=400, detail="`model` is required")
    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session, connection_id, ctx.team_id,
            supported_types=frozenset({"anthropic", "openai-compatible", "custom"}),
            dialect_label="/anthropic/v1/messages",
        )
        routing = ProviderConnectionRouting.from_storage(row.provider_type, row.base_url, row.alternate_route)
        alternate = routing.alternate
        if alternate and (not alternate.supports_model(payload["model"])
                          or (row.allowed_models is not None and payload["model"] not in row.allowed_models)):
            alternate = None
        identity, native = await authorize_anthropic_trial_route(
            session, ctx, model=payload["model"], connection_type=row.provider_type,
            alternate_type=ROUTE_PROVIDER_TYPES[alternate.protocol] if alternate else None,
        )
        base_url = alternate.base_url if alternate and native != (row.provider_type == "anthropic") else row.base_url
        try:
            upstream_payload = payload if native else anthropic_to_chat(payload)
            url = (anthropic_api_base_url(base_url) + "/messages" if native
                   else base_url.rstrip("/") + "/chat/completions")
        except (AnthropicCompatibilityError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        api_key = await decrypt_facade_api_key(session, row)
    headers = {"content-type": "application/json"}
    if native:
        headers.update({"x-api-key": api_key, "anthropic-version": request.headers.get("anthropic-version", "2023-06-01")})
        if beta := request.headers.get("anthropic-beta"):
            headers["anthropic-beta"] = beta
    else:
        headers["authorization"] = f"Bearer {api_key}"
    if accept := request.headers.get("accept"):
        headers["accept"] = accept
    upstream = await request.app.state.egress_client_pool.get(connection_id)
    dispatch = MessagesDispatch(request, ctx, row, identity, payload, upstream_payload, native, api_key, url, headers, upstream)
    return await dispatch.stream() if payload.get("stream") else await dispatch.complete()
