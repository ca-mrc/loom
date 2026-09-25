"""NetworkPolicy tagged union for declared task networking requirements.

Spec §4.2 (Supporting types).
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _BasePolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Public(_BasePolicy):
    kind: Literal["public"] = "public"


class NoNetwork(_BasePolicy):
    kind: Literal["no-network"] = "no-network"


class GatewayOnly(_BasePolicy):
    kind: Literal["gateway-only"] = "gateway-only"


class Allowlist(_BasePolicy):
    kind: Literal["allowlist"] = "allowlist"
    domains: tuple[str, ...] = Field(..., min_length=1)
    cidrs: tuple[str, ...] = ()


class WebDestination(_BasePolicy):
    """Exact public DNS destination; HTTPS uses CONNECT, HTTP uses forwarding."""

    host: str = Field(min_length=3, max_length=253)
    protocol: Literal["http", "https"]

    @field_validator("host")
    @classmethod
    def canonical_public_hostname(cls, value: str) -> str:
        labels = value.split(".")
        if (len(labels) < 2 or not re.fullmatch(r"[a-z]{2,63}", labels[-1])
            or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                   for label in labels)
            or labels[-1] in {"local", "internal", "localhost", "test", "invalid"}
            or value.endswith(".cluster.local")):
            raise ValueError("web destination requires a canonical public DNS hostname")
        return value

    @property
    def port(self) -> int:
        return 443 if self.protocol == "https" else 80


class WebAllowlist(_BasePolicy):
    kind: Literal["web-allowlist"] = "web-allowlist"
    destinations: tuple[WebDestination, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def canonical_destinations(self) -> WebAllowlist:
        pairs = [(item.host, item.protocol) for item in self.destinations]
        if pairs != sorted(set(pairs)):
            raise ValueError("web destinations must be unique and sorted by host/protocol")
        return self


class PublicWeb(_BasePolicy):
    """Public HTTP and HTTPS through the gateway dialer.

    This is not Docker ``Public``. The execution pod still has no route to
    the public internet; the gateway dials port 80 or 443 after the public
    address checks.
    """

    kind: Literal["public-web"] = "public-web"


NetworkPolicy = Annotated[
    Public | NoNetwork | GatewayOnly | Allowlist | WebAllowlist | PublicWeb,
    Field(discriminator="kind"),
]

TaskHttpEgress = Annotated[WebAllowlist | PublicWeb, Field(discriminator="kind")]


def hosted_http_egress(policy: NetworkPolicy) -> WebAllowlist | PublicWeb | None:
    """Return the dialer policy for a hosted task, or None when downloads stay off."""
    if isinstance(policy, (WebAllowlist, PublicWeb)):
        return policy
    return None
