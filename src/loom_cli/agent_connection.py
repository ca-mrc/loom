"""Resolve local agent connections without mixing provider wire dialects."""

from __future__ import annotations

from urllib.parse import urlsplit

from loom.agent.direct_provider import DirectProviderConnection
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient

_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "anthropic": "https://api.anthropic.com",
    "google": "https://generativelanguage.googleapis.com",
}
_PROVIDERS_BY_DIALECT = {
    "anthropic": {"anthropic"},
    "gemini": {"google"},
    "openai_chat": {"openai", "local"},
    "openai_responses": {"openai", "local"},
}


def resolve_agent_connection(
    gateway: UpstreamDirectGatewayClient,
    model: ModelSpec,
    *,
    dialect: str | None = None,
) -> DirectProviderConnection:
    if dialect is not None and model.provider not in _PROVIDERS_BY_DIALECT.get(dialect, set()):
        raise AgentError(f"provider {model.provider!r} is incompatible with agent dialect {dialect!r}")
    if model.provider == "local":
        server, separator, name = model.name.partition("/")
        if not separator or not name:
            raise AgentError("local model requires local/<server>/<model>")
        local = gateway.local_providers.get(server)
        if local is None:
            raise AgentError(f"local provider {server!r} is not registered")
        url = urlsplit(local.base_url)
        if (url.scheme not in {"http", "https"} or not url.hostname or url.username
                or url.password or url.query or url.fragment):
            raise AgentError("local provider base_url must be an HTTP(S) endpoint without credentials or query")
        return DirectProviderConnection(
            model=model.model_copy(update={"provider": "openai", "name": name}),
            api_key=local.api_key or "EMPTY", base_url=local.base_url,
        )
    if model.provider not in _BASE_URLS:
        raise AgentError(f"unsupported direct provider {model.provider!r}")
    key = gateway.tokens.get(model.provider)
    if not key:
        raise AgentError(f"no {model.provider} API key; configure token.{model.provider} or its API-key environment variable")
    # Gemini CLI accepts a host root; LiteLLM instead treats an explicit base
    # as already versioned. Let native Terminus select its own /v1beta default.
    base_url = None if dialect is None and model.provider == "google" else _BASE_URLS[model.provider]
    return DirectProviderConnection(
        model=model, api_key=key, base_url=base_url,
        api_key_env="ANTHROPIC_API_KEY" if model.provider == "anthropic" else None,
    )
