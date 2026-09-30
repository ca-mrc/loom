"""Agent catalog — GET /api/v1/agents.

Returns the union of built-in agent names + registered launcher
adapters so the SPA can populate a dropdown rather than ask the user
to type a free-form name.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from loom.agent_runtime_registry import list_agent_runtimes
from loom.service_execution_backend import local_execution_enabled
from loom_service.agent_catalog import AgentEntry, list_agents, native_execution_error
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


def _catalog_item(agent: AgentEntry, versions: list[dict[str, str]]) -> dict[str, Any]:
    item = {**agent.to_dict(), "versions": versions}
    # Hosted deployments execute natively on Nebius. Report what that path can
    # actually run, rather than the runtime contract alone (#2054).
    reason = None if local_execution_enabled() else native_execution_error(agent.name)
    if reason is not None:
        item.update(
            service_mode_ready=False,
            readiness_status="unavailable",
            readiness_message=reason,
        )
    return item


@router.get("/agents")
async def list_agents_route(sc: SessionAndCtx) -> dict[str, Any]:
    session, _ = sc
    versions: dict[str, list[dict[str, str]]] = {}
    for release in await list_agent_runtimes(session):
        versions.setdefault(release.agent_name, []).append(release.public_metadata())
    return {"items": [
        _catalog_item(agent, versions.get(agent.name, []))
        for agent in list_agents()
    ]}
