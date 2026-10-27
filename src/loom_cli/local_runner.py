"""Stateless wrapper around `Trial.run()` for the CLI.

Constructs a TrialContext against:
  - LocalDiskObjectStore (trajectory + ATIF land on local disk)
  - UpstreamDirectGatewayClient (no Gateway service required)
  - CLI agent factory (oracle / direct-completion / launcher subprocess)
  - Task-configured verifier (the same selection and arguments as the worker)
  - A no-op state_patch_callback (no Control Plane)

After Trial.run() finishes, the trajectory + ATIF docs are already in
the object store; we also copy events.jsonl + atif.json to
`<output_dir>/<trial_id>/` for friendly user-facing paths.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

from loom.agent.subprocess import SubprocessAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.result import TrialResult
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.trajectory.storage import ObjectStore
from loom.trial.trial import Trial, TrialContext
from loom.verifier.factory import task_verifier_factory
from loom_cli.agent_factory import build_agent_factory
from loom_cli.config import LocalProvider
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient

logger = logging.getLogger(__name__)


@dataclass
class LocalRunner:
    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path

    driver_factory: Callable[[], Driver]
    output_dir: Path
    object_store: ObjectStore

    upstream_gateway_tokens: dict[str, str]
    anthropic_client: object
    openai_client: object
    google_client: object
    local_providers: dict[str, LocalProvider] = field(default_factory=dict)

    trial_config: TrialConfig | None = None

    async def run(self) -> TrialResult:
        # TrialConfig requires agent_name + agent_model. In CLI mode (no API
        # submission), task.toml is the source of truth
        # — we copy the task's agent identity onto a fresh TrialConfig so
        # `loom run X` keeps working without the user having to pass
        # --agent / --model when the task already specifies them.
        cfg = self.trial_config or TrialConfig(
            agent_name=self.task_config.agent.name,
            agent_model=self.task_config.agent.model,
        )
        if cfg.multi_model is not None and cfg.multi_model.enabled:
            raise AgentError("local execution does not support multi_model sessions")
        gateway = UpstreamDirectGatewayClient(
            anthropic_client=self.anthropic_client,
            openai_client=self.openai_client,
            google_client=self.google_client,
            tokens=self.upstream_gateway_tokens,
            local_providers=self.local_providers,
        )
        agent_factory = build_agent_factory(
            team_id=self.team_id, trial_id=self.trial_id,
        )
        agent = agent_factory(
            self.task_dir, gateway,
            self.task_config.agent.model,
            self.task_config.agent.name,
        )
        if isinstance(agent, SubprocessAgent) and cfg.request_params:
            raise AgentError("local subprocess request_params require an agent-native configuration; gateway controls are unsupported")
        if isinstance(agent, LoomTerminus2Runtime):
            agent.request_params = dict(cfg.request_params)
            agent.harbor_config = cfg.harbor_config
        verifier = task_verifier_factory(self.task_config)()
        driver = self.driver_factory()

        trial_dir = self.output_dir / str(self.trial_id)
        trial_dir.mkdir(parents=True, exist_ok=True)

        ctx = TrialContext(
            trial_id=self.trial_id, team_id=self.team_id,
            task_config=self.task_config, task_checksum=self.task_checksum,
            task_dir=self.task_dir, trial_config=cfg,
            driver=driver, agent=agent, verifier=verifier,
            object_store=self.object_store,
            local_trajectory_path=trial_dir / "events.jsonl",
            llm_calls_fetcher=None,
            # Separate verification always gets a fresh driver from the same
            # factory the CLI used for the agent sandbox (#2418).
            verifier_driver_factory=self.driver_factory,
        )
        trial = Trial(ctx=ctx, state_patch=None)
        result = await trial.run()
        try:
            atif_bytes = await self.object_store.get_object(
                bucket="trajectories",
                key=f"{self.team_id}/{self.trial_id}/atif.json",
            )
            (trial_dir / "atif.json").write_bytes(atif_bytes)
        except Exception:
            logger.warning("could not copy atif.json out of object store")
        return result
