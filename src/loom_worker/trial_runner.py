"""Per-trial runner — builds the TrialContext and invokes Trial.run().

Plan 3 owns Trial.run() itself; this is the worker-side wrapper that wires
it to a real Driver/Agent/Verifier and a state PATCH callback that hits
the Control Plane.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.local_vllm_client import LocalVLLMGatewayClient
from loom.driver.base import Driver
from loom.models.result import FailureReason, TrialResult, TrialState
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.terminal_result_semantics import aggregate_reward_scalar
from loom.trajectory.cp_event_sink import CpEventSink
from loom.trajectory.object_identity import TrajectoryObjectIdentity
from loom.trajectory.storage import ObjectStore
from loom.trial.trial import Trial, TrialContext
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.verifier.base import Verifier
from loom_worker.jwt_rotator import JWTRotator
from loom_worker.sandbox_network import (
    SandboxBridge,
    SandboxNetworkAllocator,
    create_sandbox_bridge,
    teardown_sandbox_bridge,
)
from loom_worker.sandbox_singleton import SandboxSingletonManager
from loom_worker.task_sidecars import DockerTaskSidecarRuntime
from loom_worker.vllm_registry import WorkerVLLMRegistry
from loom_worker.worker_health import WorkerUnhealthyError

logger = logging.getLogger(__name__)

_TERMINAL_TRIAL_STATES = frozenset({TrialState.SUCCEEDED, TrialState.FAILED, TrialState.CANCELLED})
_TERMINAL_TRIAL_STATE_VALUES = frozenset(state.value for state in _TERMINAL_TRIAL_STATES)


# (state, failure_reason, failure_message) → bool: True if the Control Plane
# accepted the transition, False if the worker has lost its claim (fenced).
StatePatchCallback = Callable[[str, str | None, str | None], Awaitable[bool]]
OutputProjectionCallback = Callable[[dict[str, object], dict[str, object]], Awaitable[bool]]

# Factory signature: (task_dir, gateway, model, agent_name) → AgentRuntime.
# agent_name is read from task_config.agent.name; the factory routes:
#   "oracle"             → OracleAgent
#   "direct-completion"  → LiteLLMAgent (legacy alias: "litellm")
#   <launcher adapter>   → SubprocessAgent wrapping the adapter
AgentFactory = Callable[
    [Path, LLMGatewayClient, "ModelSpec | None", str],
    AgentRuntime,
]


class TaskSidecarRuntime(Protocol):
    async def start(self, network_name: str | None = None) -> str: ...

    async def stop(self) -> None: ...


TaskSidecarRuntimeFactory = Callable[[], TaskSidecarRuntime]


@dataclass
class LocalTrialRunner:
    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path
    trial_config: TrialConfig

    driver_factory: Callable[[], Driver]
    agent_factory: AgentFactory
    verifier_factory: Callable[[], Verifier]

    object_store: ObjectStore
    gateway_client: LLMGatewayClient

    local_trajectory_root: Path
    state_patch_callback: StatePatchCallback
    attempt_count: int | None = None
    # Must match control-plane + loom-service bucket settings for the
    # environment. Defaults preserve local/dev behavior.
    trajectory_bucket: str = "trajectories"
    artifacts_bucket: str = "artifacts"
    output_projection_callback: OutputProjectionCallback | None = None
    # Plan 9/11 amendment A11.1: optional fetcher the worker plumbs
    # through to Trial via TrialContext.llm_calls_fetcher. None means
    # no llm_calls injection at finalize (legacy v0.7 behavior).
    llm_calls_fetcher: Callable[[UUID], Awaitable[list[dict[str, object]]]] | None = None
    # PR-E: worker-spawned vLLM registry. Optional — when None, any
    # trial requesting `ModelSpec.source=hf, hf_execution=local-vllm`
    # surfaces an AgentError instead of silently routing elsewhere.
    vllm_registry: WorkerVLLMRegistry | None = None
    # #188 / Phase B: optional sandbox-isolation hooks. When BOTH are
    # set, the runner creates a per-trial bridge before driver.start
    # and attaches the singleton to it once the driver is up. None →
    # legacy behavior (driver attaches to default docker network).
    sandbox_allocator: SandboxNetworkAllocator | None = None
    sandbox_singleton: SandboxSingletonManager | None = None
    # Phase D: JWT mint callback + per-trial bind-mount root. When
    # `sandbox_mint_token` is set AND the bridge/singleton are
    # configured, the runner builds a JWTRotator around the trial
    # lifecycle: initial-token write + atomic rotation every
    # `sandbox_step_jwt_ttl_sec / 2`. `sandbox_secrets_root` is the
    # parent dir under which `<trial_id>/run/loom/` is created and
    # bind-mounted at `/run/loom/` in the sandbox.
    sandbox_mint_token: Callable[[UUID], Awaitable[str]] | None = None
    sandbox_secrets_root: Path | None = None
    sandbox_step_jwt_ttl_sec: int = 600
    sandbox_extra_hosts: tuple[tuple[str, str], ...] = ()
    # #672 PR-3: bind-mount tuples for the family-run state directory.
    # Populated by the worker main loop after prepare_family_state_mount
    # downloads the shared state tarball; each entry is
    # (host_path, container_path, mode) — e.g. ("/tmp/foo",
    # "/root/.skills", "rw"). Appended alongside the JWT rotator mount
    # inside :meth:`run`.
    family_state_volumes: tuple[tuple[str, str, str], ...] = ()
    workspace_staging_policy: WorkspaceStagingPolicy | None = None
    sidecar_runtime_factory: TaskSidecarRuntimeFactory | None = None
    # #5 Slice 3b: optional CP-side event sink. When the worker is
    # configured to dual-write trajectory events into Postgres
    # `trial_events`, main_loop constructs one per trial and passes
    # it here; the runner forwards it onto TrialContext for
    # Trial.run → TrajectoryWriter to mirror events through.
    cp_event_sink: CpEventSink | None = None
    model_switch_plan: dict[str, Any] | None = None
    # Per-container resource caps for trial and setup-sidecar containers.
    # main_loop populates these from WorkerSettings.container_*; 0 is unset.
    container_cpus: float = 0.0
    container_memory_mib: int = 0
    container_pids: int = 0
    container_cgroup_parent: str | None = None
    runtime_identity_labels: tuple[tuple[str, str], ...] = ()
    # Trusted image consumers supply an online, bounded, one-use authority call.
    # None preserves the existing non-V2 runner. It is not an offline grant.
    start_authorization: Callable[[], Awaitable[bool]] | None = None

    def __post_init__(self) -> None:
        self._active_agent: AgentRuntime | None = None
        self._start_authorization_attempted = False

    async def interrupt_attempt(self) -> None:
        """Best-effort stop of attempt-owned agent resources on ownership revoke.

        Used by the cancellation watchdog (#1491) so blocked LLM / tmux work
        cannot hold the RunnerPool seat after CP reclaim or cancel.
        """
        agent = self._active_agent
        if agent is None:
            return
        close = getattr(agent, "aclose_attempt", None)
        if close is None:
            return
        await close()

    async def run(self) -> TrialResult:
        if self.start_authorization is not None:
            # Set before the first await: concurrent calls, a lost response and
            # cancellation must not retry possibly committed one-use authority.
            if self._start_authorization_attempted:
                raise RuntimeError("trial start authorization already attempted")
            self._start_authorization_attempted = True
            if await self.start_authorization() is not True:
                raise RuntimeError("trial start authorization refused")
        # The gate precedes factories, bridges, sidecars, tokens and vLLM launch.
        # On denial there are no runner-owned resources that need cleanup.
        from loom.errors import DriverError
        from loom.task_runtime_compatibility import task_runtime_rejections

        rejections = task_runtime_rejections(self.task_config, agent_name=self.trial_config.agent_name)
        if rejections:
            raise DriverError("Unsupported task requirements: " + "; ".join(rejections))
        driver = self.driver_factory()

        # #188 / Phase B: per-trial sandbox bridge + singleton attach.
        # Only when BOTH allocator AND singleton are provided (the
        # worker main_loop wires both when LOOM_WORKER_SANDBOX_ISOLATION
        # is on AND the singleton started cleanly). Fails closed: if
        # the driver can't honor a custom network, raise rather than
        # silently start the trial without isolation.
        sandbox_bridge: SandboxBridge | None = None
        on_driver_started_cb: Callable[[], Awaitable[None]] | None = None
        if self.sandbox_allocator is not None and self.sandbox_singleton is not None:
            if not driver.capabilities.supports_custom_network:
                raise RuntimeError(
                    "sandbox isolation enabled but driver "
                    f"{type(driver).__name__} does not support "
                    "StartOptions.network; pair isolation with a "
                    "DockerDriver-backed worker",
                )
            sandbox_bridge = await create_sandbox_bridge(
                trial_id=self.trial_id,
                allocator=self.sandbox_allocator,
            )

            async def _attach_singleton() -> None:
                assert sandbox_bridge is not None
                assert self.sandbox_singleton is not None
                await self.sandbox_singleton.attach_to_bridge(sandbox_bridge)

            on_driver_started_cb = _attach_singleton

        sidecar_runtime: TaskSidecarRuntime | None = None
        if self.task_config.environment.sidecars:
            if not driver.capabilities.supports_custom_network:
                raise RuntimeError(
                    "task sidecars require a driver that supports "
                    "StartOptions.network; pair sidecar tasks with a "
                    "DockerDriver-backed worker",
                )
            sidecar_runtime = (
                self.sidecar_runtime_factory()
                if self.sidecar_runtime_factory is not None
                else DockerTaskSidecarRuntime(
                    task_config=self.task_config,
                    task_dir=self.task_dir,
                    task_checksum=self.task_checksum,
                    trial_id=self.trial_id,
                    container_cpus=self.container_cpus,
                    container_memory_mib=self.container_memory_mib,
                    container_pids=self.container_pids,
                    container_cgroup_parent=self.container_cgroup_parent,
                    runtime_identity_labels=self.runtime_identity_labels,
                )
            )

        # Phase D: per-trial JWT rotator. Built when isolation is on
        # AND mint+secrets-root are configured. The rotator writes
        # the initial JWT BEFORE the driver starts so the bind-mounted
        # file is already populated by the time the container sees it.
        sandbox_volumes: tuple[tuple[str, str, str], ...] = ()
        jwt_rotator: JWTRotator | None = None
        if (
            sandbox_bridge is not None
            and self.sandbox_mint_token is not None
            and self.sandbox_secrets_root is not None
        ):
            jwt_dir = self.sandbox_secrets_root / str(self.trial_id) / "run" / "loom"
            jwt_rotator = JWTRotator(
                trial_id=self.trial_id,
                jwt_dir=jwt_dir,
                mint_callback=self.sandbox_mint_token,
                expiry_sec=self.sandbox_step_jwt_ttl_sec,
            )
            # Bind-mount the dir read-only at /run/loom/ inside the
            # sandbox. The rotator owns the file on the HOST side; the
            # sandbox sees atomic-replaced contents.
            sandbox_volumes = ((str(jwt_dir), "/run/loom", "ro"),)

        # #672 PR-3: append the family-run state volumes so the sandbox
        # sees the shared skills directory alongside any JWT rotator
        # mount. Cleanup of the host-side staging dir happens in the
        # worker main loop's finally block.
        if self.family_state_volumes:
            sandbox_volumes = sandbox_volumes + self.family_state_volumes

        # Plan 23: agent + model live on TrialConfig and are required.
        # TaskConfig.agent.* is no longer consulted for service-mode
        # trials — every submission specifies which agent + model run.
        # PR-E: when the model targets a worker-spawned vLLM, swap the
        # gateway for a direct LocalVLLMGatewayClient before handing
        # it to the agent factory. The vLLM registry caches across
        # trials so successive trials of the same model reuse one
        # subprocess instead of paying the 1-3 min startup each time.
        effective_gateway = await self._resolve_gateway()
        agent = self.agent_factory(
            self.task_dir,
            effective_gateway,
            self.trial_config.agent_model,
            self.trial_config.agent_name,
        )
        self._active_agent = agent
        # #184: if the direct-completion runtime opts in, surface the task's
        # declared artifact paths so it can write the LLM's final
        # response somewhere the verifier can grade. Duck-typed so
        # Protocol-respecting agents that write their own artifacts
        # (oracle, claude-code, opencode, …) are unaffected.
        if hasattr(agent, "artifact_paths"):
            agent.artifact_paths = [a for step in self.task_config.steps for a in step.artifacts]
        if hasattr(agent, "request_params"):
            agent.request_params = dict(self.trial_config.request_params)
        if hasattr(agent, "multi_model"):
            agent.multi_model = self.trial_config.multi_model
        if hasattr(agent, "model_switch_plan"):
            agent.model_switch_plan = self.model_switch_plan
        verifier = self.verifier_factory()

        trajectory_identity = TrajectoryObjectIdentity(
            bucket=self.trajectory_bucket,
            team_id=self.team_id,
            trial_id=self.trial_id,
            attempt_count=self.attempt_count,
        )
        ctx = TrialContext(
            trial_id=self.trial_id,
            team_id=self.team_id,
            task_config=self.task_config,
            task_checksum=self.task_checksum,
            task_dir=self.task_dir,
            trial_config=self.trial_config,
            driver=driver,
            agent=agent,
            verifier=verifier,
            object_store=self.object_store,
            local_trajectory_path=trajectory_identity.local_path(
                self.local_trajectory_root,
            ),
            attempt_count=self.attempt_count,
            trajectory_bucket=self.trajectory_bucket,
            artifacts_bucket=self.artifacts_bucket,
            llm_calls_fetcher=self.llm_calls_fetcher,
            sandbox_network=(sandbox_bridge.name if sandbox_bridge is not None else None),
            on_driver_started=on_driver_started_cb,
            sandbox_volumes=sandbox_volumes,
            sandbox_extra_hosts=self.sandbox_extra_hosts,
            workspace_staging_policy=self.workspace_staging_policy,
            # Only provenance-gated private-workspace tasks receive a second
            # Driver. The existing image-specific factory is safe to reuse:
            # it creates a fresh instance for the verifier lifecycle, while
            # ordinary tasks retain the legacy single-driver behavior.
            verifier_driver_factory=(
                self.driver_factory if self.workspace_staging_policy is not None else None
            ),
            cp_event_sink=self.cp_event_sink,
            # #896: forward per-container caps into the trial container.
            container_cpus=self.container_cpus,
            container_memory_mib=self.container_memory_mib,
            container_pids=self.container_pids,
            container_cgroup_parent=self.container_cgroup_parent,
            runtime_identity_labels=self.runtime_identity_labels,
        )

        deferred_terminal_patch: tuple[str, str | None, str | None] | None = None

        async def _send_state_patch(
            state: str,
            fr: str | None,
            fm: str | None = None,
        ) -> bool:
            # Trial expects an `Awaitable[None]` callback. We adapt our
            # bool-returning callback by logging when the Control Plane
            # rejects with a fence (False) and swallowing other errors so a
            # transient PATCH failure doesn't crash the trial body.
            try:
                ok = await self.state_patch_callback(state, fr, fm)
                if not ok:
                    logger.warning(
                        "state_patch_fenced trial=%s state=%s — worker lost claim",
                        self.trial_id,
                        state,
                    )
                    return False
                return True
            except Exception as exc:
                logger.warning(
                    "state_patch_error trial=%s state=%s err=%s",
                    self.trial_id,
                    state,
                    exc,
                )
                return False

        async def _patch(state: str, fr: str | None, fm: str | None = None) -> None:
            nonlocal deferred_terminal_patch
            if (
                state in _TERMINAL_TRIAL_STATE_VALUES
                and self.output_projection_callback is not None
            ):
                deferred_terminal_patch = (state, fr, fm)
                return
            await _send_state_patch(state, fr, fm)

        async def _project_then_report_terminal(result: TrialResult) -> bool:
            projection_ok = await self._patch_output_projection(result)
            if not projection_ok:
                if result.state == TrialState.SUCCEEDED:
                    result.state = TrialState.FAILED
                    result.failure_reason = FailureReason.TRAJECTORY_FLUSH_FAILED
                    result.failure_message = (
                        result.failure_message
                        or "successful trial output projection was not accepted "
                        "by the control plane"
                    )
                    result.finished_at = datetime.now(UTC)
                return False
            if deferred_terminal_patch is not None:
                await _send_state_patch(*deferred_terminal_patch)
            return True

        trial = Trial(ctx=ctx, state_patch=_patch)
        # Phase D: when a rotator is configured, wrap the trial body
        # in its async context. `__aenter__` writes the initial JWT
        # (so the container's first read sees a valid token) and
        # spawns the rotation task; `__aexit__` cancels the task.
        # The bind-mount path is already in ctx.sandbox_volumes so
        # driver.start picks it up.
        if jwt_rotator is not None:
            await jwt_rotator.__aenter__()
        try:
            if sidecar_runtime is not None:
                ctx.sandbox_network = await sidecar_runtime.start(
                    network_name=(sandbox_bridge.name if sandbox_bridge is not None else None),
                )
            result = await trial.run()
            if result.state in _TERMINAL_TRIAL_STATES:
                await _project_then_report_terminal(result)
            if getattr(agent, "_loom_worker_unhealthy", False):
                raise WorkerUnhealthyError(
                    "agent attempt ignored cancellation beyond the bounded drain",
                )
            return result
        except asyncio.CancelledError:
            cancelled_result = trial.result
            if cancelled_result is not None and cancelled_result.state == TrialState.CANCELLED:
                await asyncio.shield(_project_then_report_terminal(cancelled_result))
            raise
        except WorkerUnhealthyError:
            raise
        except Exception:
            logger.exception("trial_runner_uncaught_exception trial=%s", self.trial_id)
            if trial.result is None:
                raise
            result = trial.result
            if result.state != TrialState.CANCELLED:
                result.state = TrialState.FAILED
                result.failure_reason = (
                    result.failure_reason or FailureReason.TRAJECTORY_FLUSH_FAILED
                )
                result.finished_at = datetime.now(UTC)
                await _patch(
                    result.state.value,
                    result.failure_reason.value if result.failure_reason else None,
                    result.failure_message,
                )
                await _project_then_report_terminal(result)
                return result
            raise
        finally:
            self._active_agent = None
            if sidecar_runtime is not None:
                try:
                    await sidecar_runtime.stop()
                except Exception:
                    logger.exception(
                        "task_sidecar_teardown_failed trial=%s",
                        self.trial_id,
                    )
            # Phase D: stop the rotator BEFORE bridge teardown so the
            # rotation task isn't competing with the bind-mount source
            # dir going away.
            if jwt_rotator is not None:
                try:
                    await jwt_rotator.__aexit__(None, None, None)
                except Exception:
                    logger.exception(
                        "jwt_rotator_teardown_failed trial=%s",
                        self.trial_id,
                    )
            # #188: release the per-trial bridge. Runs unconditionally
            # — even if Trial.run raised — so a half-started trial
            # doesn't leak the /24. Singleton's network endpoint is
            # cleaned up by docker network rm.
            if sandbox_bridge is not None and self.sandbox_allocator is not None:
                try:
                    await teardown_sandbox_bridge(
                        bridge=sandbox_bridge,
                        allocator=self.sandbox_allocator,
                    )
                except Exception:
                    logger.exception(
                        "sandbox_bridge_teardown_failed trial=%s name=%s",
                        self.trial_id,
                        sandbox_bridge.name,
                    )

    async def _patch_output_projection(self, result: TrialResult) -> bool:
        if self.output_projection_callback is None:
            return True
        result_payload = _build_result_payload(result)
        trajectory_index = _build_trajectory_index(result)
        try:
            ok = await self.output_projection_callback(
                result_payload,
                trajectory_index,
            )
            if not ok:
                logger.warning(
                    "output_projection_patch_fenced trial=%s — worker lost claim",
                    self.trial_id,
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "output_projection_patch_error trial=%s err=%s",
                self.trial_id,
                exc,
            )
            return False

    async def _resolve_gateway(self) -> LLMGatewayClient:
        """Pick the gateway client for this trial.

        Default: the worker's `gateway_client` (HTTP to the LLM Gateway
        service). When the trial's model selects worker-spawned vLLM,
        substitute a `LocalVLLMGatewayClient` pointed at the registry's
        cached subprocess URL — bypasses the gateway since the vLLM
        runs on this same worker host.
        """
        model = self.trial_config.agent_model
        if model is None:
            return self.gateway_client
        if model.source != "hf" or model.hf_execution != "local-vllm":
            return self.gateway_client
        if self.vllm_registry is None:
            from loom.errors import AgentError

            raise AgentError(
                "trial requests source=hf, hf_execution=local-vllm but "
                "this worker has no vllm_registry configured. Set up a "
                "worker with `pip install loom[vllm]` or pick a different "
                "model source.",
            )
        handle = await self.vllm_registry.get_or_launch(model.name)
        return LocalVLLMGatewayClient(base_url=handle.base_url)


def _build_result_payload(result: TrialResult) -> dict[str, object]:
    payload: dict[str, object] = result.model_dump(mode="json")
    payload["aggregate_reward"] = aggregate_reward_scalar(result.reward)
    payload.setdefault("cost_usd", 0.0)
    return payload


def _build_trajectory_index(result: TrialResult) -> dict[str, object]:
    artifacts = [
        artifact.model_dump(mode="json") for step in result.steps for artifact in step.artifacts
    ]
    return {
        "schema_version": "1",
        "trial_id": str(result.id),
        "team_id": str(result.team_id),
        "task_id": result.task_id,
        "trajectory_uri": result.trajectory_uri,
        "trajectory_sha256": result.trajectory_sha256,
        "trajectory_size_bytes": result.trajectory_size_bytes,
        "trajectory_version_id": result.trajectory_version_id,
        "atif_uri": result.atif_uri,
        "atif_sha256": result.atif_sha256,
        "atif_size_bytes": result.atif_size_bytes,
        "atif_version_id": result.atif_version_id,
        "atif_schema_version": result.atif_schema_version,
        "artifacts": artifacts,
    }
