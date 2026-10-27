"""Task metadata supported by Harbor's native Terminus prompt contract."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

from loom.models.mcp import MCPConnection


def terminus_task_metadata_rejections(mcp: Sequence[MCPConnection]) -> tuple[str, ...]:
    """Reject fields native Terminus cannot convey; never silently discard them."""
    reasons = []
    for index, server in enumerate(mcp):
        prefix = f"environment.mcp_servers.{index}"
        if server.transport not in {"stdio", "sse", "http"}:
            reasons.append(f"{prefix}.transport: terminus_mcp_transport_unsupported")
        if server.command and not server.command[0].strip():
            reasons.append(f"{prefix}.command: terminus_mcp_command_required")
        if server.env:
            reasons.append(f"{prefix}.env: terminus_mcp_environment_unsupported")
        if server.headers:
            reasons.append(f"{prefix}.headers: terminus_mcp_headers_unsupported")
        if server.url and urlsplit(server.url).username is not None:
            reasons.append(f"{prefix}.url: terminus_mcp_url_credentials_unsupported")
    return tuple(reasons)


def harbor_mcp_documents(mcp: Sequence[MCPConnection]) -> list[dict[str, Any]]:
    reasons = terminus_task_metadata_rejections(mcp)
    if reasons:
        raise ValueError("; ".join(reasons))
    return [
        {
            "name": server.name,
            "transport": "streamable-http" if server.transport == "http" else server.transport,
            **({"command": server.command[0], "args": server.command[1:]}
               if server.command else {"url": server.url}),
        }
        for server in mcp
    ]
