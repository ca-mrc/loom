"""CLI-local agents use explicit direct connections without Control Plane grants."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom_cli.agent_connection import resolve_agent_connection
from loom_cli.terminus_ledger import LocalHarborLedger
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient
from loom_worker.trial_runner import AgentFactory


def build_agent_factory(
    *,
    team_id: UUID,
    trial_id: UUID,
) -> AgentFactory:
    """Return a factory matching `loom_worker.trial_runner.AgentFactory`.

    Routes:
      - "oracle"  -> OracleAgent
      - "direct-completion" / legacy "litellm" -> LiteLLMAgent (requires model)
      - any other -> SubprocessAgent wrapping loom_launcher.get_adapter(name)
                     (requires model; AgentError if name is unknown)
    """

    def make(
        task_dir: Path,
        gateway: LLMGatewayClient,
        model: ModelSpec | None,
        agent_name: str,
    ) -> AgentRuntime:
        agent: AgentRuntime
        if agent_name == "oracle":
            agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
        elif agent_name in {"direct-completion", "litellm"}:
            if model is None:
                raise AgentError(
                    "direct-completion agent requires task.agent.model to be set",
                )
            agent = LiteLLMAgent(  # type: ignore[assignment]
                model=model,
                gateway=gateway,
                team_id=str(team_id),
                trial_id=trial_id,
            )
        elif agent_name == "terminus-2":
            if model is None:
                raise AgentError(
                    "terminus-2 agent requires task.agent.model to be set",
                )
            if not isinstance(gateway, UpstreamDirectGatewayClient):
                raise AgentError("local Terminus requires an upstream-direct provider client")
            connection = resolve_agent_connection(gateway, model)
            agent = LoomTerminus2Runtime(  # type: ignore[assignment]
                model=model,
                team_id=str(team_id),
                trial_id=trial_id,
                cp_client=None,
                gateway_url="",
                direct_provider=connection,
                direct_ledger=LocalHarborLedger(trial_id=trial_id, model=model, connection=connection),
            )
        else:
            from loom_launcher import get_adapter

            from loom.agent.subprocess import SubprocessAgent

            adapter = get_adapter(agent_name)
            if adapter is None:
                raise AgentError(
                    f"unknown agent {agent_name!r} — not a built-in runtime "
                    f"and not registered in loom-launcher",
                )
            if model is None:
                raise AgentError(
                    f"{agent_name} requires task.agent.model to be set",
                )
            if not isinstance(gateway, UpstreamDirectGatewayClient):
                raise AgentError("local subprocess requires an upstream-direct provider client")
            agent = SubprocessAgent(  # type: ignore[assignment]
                adapter=adapter,
                model=model,
                cp_client=None,
                gateway_url="",
                team_id=team_id,
                trial_id=trial_id,
                direct_provider=resolve_agent_connection(gateway, model, dialect=adapter.endpoint_dialect),
            )
        return agent

    return make
