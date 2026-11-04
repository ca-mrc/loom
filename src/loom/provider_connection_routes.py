"""Wire routes under one provider identity and credential."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError

ProviderWireRoute = Literal["openai_chat", "anthropic_messages"]
OPENAI_SHAPED_PROVIDER_TYPES = frozenset({"openai-compatible", "custom"})
ROUTE_PROVIDER_TYPES = {
    "openai_chat": "openai-compatible",
    "anthropic_messages": "anthropic",
}


class AlternateProviderRoute(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol: ProviderWireRoute
    base_url: str
    verified_models: dict[str, bool] = Field(default_factory=dict)

    def supports_model(self, model: str | None) -> bool:
        return model is not None and self.verified_models.get(model) is True


def primary_protocol(provider_type: str) -> ProviderWireRoute | None:
    if provider_type in OPENAI_SHAPED_PROVIDER_TYPES:
        return "openai_chat"
    if provider_type == "anthropic":
        return "anthropic_messages"
    return None


def same_provider_origin(left: str, right: str) -> bool:
    def origin(url: str) -> tuple[str, str | None, int | None]:
        parts = urlsplit(url)
        port = parts.port
        if port is None:
            port = {"https": 443, "http": 80}.get(parts.scheme)
        return parts.scheme, parts.hostname, port

    return origin(left) == origin(right)


@dataclass(frozen=True)
class ProviderConnectionRouting:
    primary_type: str
    alternate: AlternateProviderRoute | None = None

    @classmethod
    def from_storage(
        cls, primary_type: str, primary_url: str, alternate: Mapping[str, Any] | None,
    ) -> ProviderConnectionRouting:
        if alternate is None:
            return cls(primary_type)
        try:
            route = AlternateProviderRoute.model_validate(alternate)
            protocol = primary_protocol(primary_type)
            if protocol is not None and protocol != route.protocol and same_provider_origin(primary_url, route.base_url):
                return cls(primary_type, route)
        except (ValidationError, ValueError):
            # Invalid optional metadata cannot change a legacy route.
            pass
        return cls(primary_type)

    def verified_alternate(self, protocol: ProviderWireRoute, model: str | None) -> AlternateProviderRoute | None:
        if self.alternate and self.alternate.protocol == protocol and self.alternate.supports_model(model):
            return self.alternate
        return None
