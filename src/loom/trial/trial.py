"""Trial composition + run() body (spec §2.5 + §3.3)."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from loom.agent.base import AgentRuntime, InBoxAgentRuntime
from loom.agent.oracle import OracleAgent
from loom.driver.base import Driver, StartOptions
from loom.errors import (
    AgentContinuationError,
    classify_failure,
    classify_failure_message,
    is_platform_setup_agent_failure,
)
from loom.models.networking import NetworkPolicy
from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepError,
    TrialResult,
    TrialState,
)
from loom.models.task import TaskConfig
from loom.models.trajectory import (
    TrialCancelledEvent,
    TrialEndEvent,
    TrialErrorEvent,
    TrialStartEvent,
)
from loom.models.trial import TrialConfig
from loom.security.redaction import redact_text
from loom.trajectory.cp_event_sink import CpEventSink
from loom.trajectory.llm_call_events import llm_call_row_to_event
from loom.trajectory.object_identity import TrajectoryObjectIdentity
from loom.trajectory.storage import ObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.finalize import finalize_trajectory_with_metadata
from loom.trial.step_runner import run_step
from loom.trial.watchdog_cancellation import (
    WatchdogTriggerReason,
    extract_watchdog_cancellation,
    watchdog_timeout_failure_message,
)
from loom.trial.workspace import WorkspaceStagingPolicy, materialize_workspace
from loom.verifier.base import Verifier

logger = logging.getLogger(__name__)

_FINALIZE_TIMEOUT_SEC = 60.0
_STATE_PATCH_TIMEOUT_SEC = 15.0

StatePatchCallback = Callable[[str, str | None, str | None], Awaitable[None]]


@dataclass
class TrialContext:
    """Everything a Trial needs to run. Constructed by the worker; the Trial
    itself just executes against this bundle."""

    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path
    trial_config: TrialConfig
    driver: Driver
    agent: AgentRuntime
    verifier: Verifier
    object_store: ObjectStore
    local_trajectory_path: Path
    attempt_count: int | None = None
    trajectory_bucket: str = "trajectories"
    artifacts_bucket: str = "artifacts"
    # Plan 9/11 amendment A11.1: optional callback the worker uses to
    # fetch llm_calls rows from the Control Plane at finalize time and
    # project each into an LLMCallEvent before ATIF runs. If None, no
    # rows are injected (legacy v0.7 behavior — agents that bypassed
    # the Gateway have no llm_calls).
    llm_calls_fetcher: Callable[[UUID], Awaitable[list[dict[str, Any]]]] | None = None
    # #188 / Phase B: sandbox-isolation hooks. When the worker has
    # `LOOM_WORKER_SANDBOX_ISOLATION=1`, these are populated:
    # - `sandbox_network` is the per-trial bridge name passed to
    #   `StartOptions.network` so the container attaches there at
    #   create time.
    # - `on_driver_started` runs RIGHT AFTER `driver.start()`
    #   succeeds. The worker uses it to attach the
    #   `loom-llm-gateway-sandbox` singleton (PR-B2 #221) to the
    #   bridge — without that hop, the sandbox has no reachable
    #   outbound endpoint.
    # Both default None → pre-Phase-B behavior unchanged.
    sandbox_network: str | None = None
    on_driver_started: Callable[[], Awaitable[None]] | None = None
    # Phase D PR-D1: bind-mounts the driver applies at create time.
    # When sandbox isolation is on, the worker passes the rotator's
    # jwt_dir here mounted at `/run/loom/`. Empty tuple → no mounts
    # (unchanged behavior).
    sandbox_volumes: tuple[tuple[str, str, str], ...] = ()
    # Optional sandbox host aliases. Used for sandbox-facing gateway
    # endpoints on Linux Docker, where `host.docker.internal` is not
    # present unless the container is started with host-gateway mapping.
    sandbox_extra_hosts: tuple[tuple[str, str], ...] = ()
    # Profile-task provenance can require private verifier assets to remain
    # outside the normal agent workspace.  Such tasks must receive a fresh
    # verifier-only driver; a delayed upload to the agent driver is unsafe
    # because background processes can survive the agent phase.
    workspace_staging_policy: WorkspaceStagingPolicy | None = None
    verifier_driver_factory: Callable[[], Driver] | None = None
    # #5 Slice 3b: optional CP-side event sink. When set, every event
    # appended through the TrajectoryWriter is mirrored to the
    # `trial_events` table via the worker's HttpControlPlaneClient.
    # MinIO remains authoritative until Slice 3c flips the SSE reader
    # to Postgres; sink failures are logged + swallowed here.
    cp_event_sink: CpEventSink | None = None
    # Per-container resource caps; 0 leaves the corresponding Docker limit unset.
    # Forwarded through StartOptions when creating trial containers.
    container_cpus: float = 0.0
    container_memory_mib: int = 0
    container_pids: int = 0
    container_cgroup_parent: str | None = None
    runtime_identity_labels: tuple[tuple[str, str], ...] = ()

    @property
    def task_id(self) -> str:
        return self.task_config.task.id

    @property
    def trajectory_key(self) -> str:
        return self.trajectory_identity.events_key

    @property
    def trajectory_uri(self) -> str:
        return self.trajectory_identity.events_uri

    @property
    def trajectory_identity(self) -> TrajectoryObjectIdentity:
        return TrajectoryObjectIdentity(
            bucket=self.trajectory_bucket,
            team_id=self.team_id,
            trial_id=self.trial_id,
            attempt_count=self.attempt_count,
        )


@dataclass
class Trial:
    """One trial's lifecycle. Wraps a TrialContext and executes run().

    The state PATCH callback is optional; in Plan 6 the worker provides one
    that hits the Control Plane. In tests / in-process runs it's None, and
    the resulting state lives only on TrialResult.
    """

    ctx: TrialContext
    state_patch: StatePatchCallback | None = None
    # Bug 4 fix: stash the TrialResult here so callers can recover it after
    # a CancelledError propagates out of run(). Populated by run() once the
    # in-flight result skeleton has been built.
    result: TrialResult | None = None

    async def run(self) -> TrialResult:
        result = TrialResult(
            id=self.ctx.trial_id,
            task_id=self.ctx.task_id,
            task_checksum=self.ctx.task_checksum,
            team_id=self.ctx.team_id,
            agent=AgentInfo(
                name=self.ctx.agent.name,
                version=self.ctx.agent.version,
                mode=self.ctx.agent.mode,
                model=self.ctx.agent.model,
            ),
            config=self.ctx.trial_config,
            state=TrialState.RUNNING,
            started_at=datetime.now(UTC),
        )
        self.result = result
        if self.state_patch is not None:
            # Bug 2 fix: PATCH errors must not propagate out of Trial.run.
            # Plan 6's worker has its own reclaim/retry — losing one heartbeat
            # is recoverable; raising here orphans the local state machine.
            try:
                await self.state_patch("running", None, None)
            except Exception:
                logger.exception("state PATCH (running) failed; continuing")

        from loom.models.networking import resolve_effective_network_policy

        baseline: NetworkPolicy = resolve_effective_network_policy(
            baseline=self.ctx.task_config.environment.baseline_network_policy,
            supported=self.ctx.task_config.environment.network_policies_supported,
            override=self.ctx.trial_config.baseline_network_policy_override,
        )

        writer = TrajectoryWriter(
            local_path=self.ctx.local_trajectory_path,
            store=self.ctx.object_store,
            bucket=self.ctx.trajectory_bucket,
            key=self.ctx.trajectory_key,
            cp_event_sink=self.ctx.cp_event_sink,
        )

        cancelled = False
        pending_llm_rows: list[dict[str, Any]] | None = None
        seq = _SeqCounter()

        try:
            async with writer:
                driver_started = False
                try:
                    from loom.agent.terminus2.runtime import LoomTerminus2Runtime
                    from loom.errors import DriverError
                    from loom.task_runtime_compatibility import task_runtime_rejections

                    rejections = task_runtime_rejections(self.ctx.task_config, agent_name=self.ctx.agent.name)
                    if rejections:
                        raise DriverError("Unsupported task requirements: " + "; ".join(rejections))

                    if isinstance(self.ctx.agent, LoomTerminus2Runtime):
                        self.ctx.agent.continue_until_timeout = self.ctx.task_config.agent.continue_until_timeout
                    elif self.ctx.task_config.agent.continue_until_timeout:
                        raise AgentContinuationError("continue_until_timeout requires the Terminus-2 runtime")
                    # Bug 1 fix: driver.start() + TrialStartEvent must land
                    # inside the broad-Exception catch so ENV_START_FAILURE
                    # (DriverError) becomes a classified TrialResult instead
                    # of propagating raw out of Trial.run.
                    await self.ctx.driver.start(
                        options=StartOptions(
                            force_build=self.ctx.trial_config.force_build,
                            network=self.ctx.sandbox_network,
                            volumes=self.ctx.sandbox_volumes,
                            environment=tuple(
                                sorted(
                                    self.ctx.task_config.environment.environment.items(),
                                )
                            ),
                            extra_hosts=_merge_extra_hosts(
                                self.ctx.task_config.environment.extra_hosts,
                                self.ctx.sandbox_extra_hosts,
                            ),
                            dns=tuple(self.ctx.task_config.environment.dns),
                            tmpfs=tuple(self.ctx.task_config.environment.tmpfs),
                            labels=tuple(
                                sorted(
                                    {
                                        **dict(self.ctx.runtime_identity_labels),
                                        "loom.trial-container": "true",
                                        "loom.trial_id": str(self.ctx.trial_id),
                                        "loom.team_id": str(self.ctx.team_id),
                                        "loom.task_id": self.ctx.task_id,
                                    }.items()
                                )
                            ),
                            cpus=self.ctx.task_config.environment.cpus,
                            memory_mb=self.ctx.task_config.environment.memory_mb,
                            storage_mb=self.ctx.task_config.environment.storage_mb,
                            gpus=self.ctx.task_config.environment.gpus,
                            # #896: per-container caps (0 = unbounded default).
                            container_cpus=self.ctx.container_cpus,
                            container_memory_mib=self.ctx.container_memory_mib,
                            container_pids=self.ctx.container_pids,
                            cgroup_parent=self.ctx.container_cgroup_parent,
                        )
                    )
                    driver_started = True
                    # #188 / Phase B: attach the per-node singleton
                    # to this trial's bridge AFTER container creation.
                    # Failure to attach is a hard error — the sandbox
                    # has no other reachable endpoint, so trying to
                    # run the agent would just hang.
                    if self.ctx.on_driver_started is not None:
                        await self.ctx.on_driver_started()
                    await writer.append(
                        TrialStartEvent(
                            emitted_at=datetime.now(UTC),
                            trial_id=self.ctx.trial_id,
                            step_id="__trial__",
                            seq=seq.next(),
                            task_id=self.ctx.task_id,
                            agent_name=self.ctx.agent.name,
                            agent_mode=self.ctx.agent.mode,
                        )
                    )
                    # #186: upload the materialized task bundle into the
                    # sandbox so the agent sees instructions / scaffolding
                    # AND the verifier sees grading tests under the task
                    # workdir. Mirrors what oracle_runner does for the
                    # verify CLI. Skips `task.toml` (host-side metadata).
                    workdir = self.ctx.task_config.environment.workdir
                    await materialize_workspace(
                        driver=self.ctx.driver,
                        task_dir=self.ctx.task_dir,
                        dst=workdir,
                        policy=self.ctx.workspace_staging_policy,
                        phase="agent",
                        trusted_private_paths=(
                            self.ctx.workspace_staging_policy.trusted_oracle_paths
                            if (
                                self.ctx.workspace_staging_policy is not None
                                and self.ctx.trial_config.agent_name == "oracle"
                                and isinstance(self.ctx.agent, OracleAgent)
                            )
                            else ()
                        ),
                    )
                    if hasattr(self.ctx.agent, "workdir"):
                        self.ctx.agent.workdir = workdir

                    if isinstance(self.ctx.agent, InBoxAgentRuntime):
                        await self.ctx.agent.setup(env=self.ctx.driver)

                    for step in self.ctx.task_config.steps:
                        sr = await run_step(
                            ctx=self.ctx,
                            step=step,
                            trajectory=writer,
                            baseline_policy=baseline,
                        )
                        result.steps.append(sr)
                    result.reward = _aggregate(self.ctx, result)
                    # #1186: LLM evidence must be known before terminal-state
                    # selection so model-backed + 0-call platform failures are
                    # not promoted to succeeded by the scored-verifier carve-out.
                    (
                        llm_call_count,
                        pending_llm_rows,
                    ) = await self._resolve_llm_call_count(writer)
                    terminal_failure = _first_terminal_step_failure(
                        result,
                        llm_call_count=llm_call_count,
                    )
                    if terminal_failure is not None:
                        result.state = TrialState.FAILED
                        result.failure_reason = terminal_failure[0]
                        result.failure_message = terminal_failure[1]
                    else:
                        result.state = TrialState.SUCCEEDED
                except asyncio.CancelledError as exc:
                    watchdog_cancellation = extract_watchdog_cancellation(exc)
                    if (
                        watchdog_cancellation is not None
                        and watchdog_cancellation.reason == WatchdogTriggerReason.HARD_DEADLINE
                    ):
                        result.state = TrialState.FAILED
                        result.failure_reason = FailureReason.AGENT_TIMEOUT
                        result.failure_message = watchdog_timeout_failure_message(
                            watchdog_cancellation,
                        )
                    else:
                        cancelled = True
                        result.state = TrialState.CANCELLED
                        try:
                            await writer.append(
                                TrialCancelledEvent(
                                    emitted_at=datetime.now(UTC),
                                    trial_id=self.ctx.trial_id,
                                    step_id="__trial__",
                                    seq=seq.next(),
                                    cancellation_requested_at=datetime.now(UTC),
                                    observed_at=datetime.now(UTC),
                                )
                            )
                        except Exception:
                            logger.exception("failed to append TrialCancelledEvent")
                    if not cancelled:
                        try:
                            await writer.append(
                                TrialErrorEvent(
                                    emitted_at=datetime.now(UTC),
                                    trial_id=self.ctx.trial_id,
                                    step_id="__trial__",
                                    seq=seq.next(),
                                    error_type="WatchdogHardDeadline",
                                    message=result.failure_message or "",
                                    traceback=None,
                                )
                            )
                        except Exception:
                            logger.exception("failed to append TrialErrorEvent")
                except Exception as exc:
                    result.state = TrialState.FAILED
                    reason, message = classify_failure(exc)
                    result.failure_reason = reason
                    result.failure_message = message
                    try:
                        await writer.append(
                            TrialErrorEvent(
                                emitted_at=datetime.now(UTC),
                                trial_id=self.ctx.trial_id,
                                step_id="__trial__",
                                seq=seq.next(),
                                error_type=type(exc).__name__,
                                message=str(exc),
                                traceback=_format_tb(exc),
                            )
                        )
                    except Exception:
                        # Early-start failure leaves the writer with no
                        # TrialStartEvent; appending an error event can
                        # itself fail. Don't mask the real failure.
                        logger.exception("failed to append TrialErrorEvent")
                finally:
                    if driver_started:
                        await asyncio.shield(
                            self.ctx.driver.stop(delete=self.ctx.trial_config.delete_env),
                        )
                # Plan 9/11 amendment A11.1 / #1186: append any CP llm_calls
                # rows fetched before terminal-state selection (or fetch now
                # if the agent path skipped early resolve — e.g. CancelledError
                # before steps completed). Keep these before TrialEndEvent so
                # terminal state remains the last event in the canonical log.
                try:
                    if pending_llm_rows is not None:
                        await self._append_llm_call_rows(writer, pending_llm_rows)
                    elif (
                        self.ctx.llm_calls_fetcher is not None
                        and not _agent_emits_gateway_llm_call_events(self.ctx.agent)
                    ):
                        await self._append_llm_call_events(writer)
                except Exception:
                    logger.exception(
                        "failed to fetch llm_calls; trajectory will be "
                        "finalized without LLMCallEvents",
                    )

                try:
                    await writer.append(
                        TrialEndEvent(
                            emitted_at=datetime.now(UTC),
                            trial_id=self.ctx.trial_id,
                            step_id="__trial__",
                            seq=seq.next(),
                            final_state=result.state.value,
                            reward=result.reward,
                            failure_reason=(
                                result.failure_reason.value if result.failure_reason else None
                            ),
                        )
                    )
                except Exception:
                    logger.exception("failed to append TrialEndEvent")
        finally:
            try:
                trajectory_body = self.ctx.local_trajectory_path.read_bytes()
                result.trajectory_uri = self.ctx.trajectory_uri if writer.remote_committed else None
                result.trajectory_sha256 = hashlib.sha256(trajectory_body).hexdigest()
                result.trajectory_size_bytes = len(trajectory_body)
                result.trajectory_version_id = writer.remote_version_id
                finalized = await asyncio.wait_for(
                    asyncio.shield(
                        finalize_trajectory_with_metadata(
                            local_path=self.ctx.local_trajectory_path,
                            store=self.ctx.object_store,
                            team_id=str(self.ctx.team_id),
                            trial_id=str(self.ctx.trial_id),
                            task_id=self.ctx.task_id,
                            agent_name=self.ctx.agent.name,
                            agent_version=self.ctx.agent.version,
                            bucket=self.ctx.trajectory_bucket,
                            attempt_count=self.ctx.attempt_count,
                        )
                    ),
                    timeout=_FINALIZE_TIMEOUT_SEC,
                )
                result.atif_uri = finalized.uri
                result.atif_sha256 = finalized.sha256
                result.atif_size_bytes = finalized.size_bytes
                result.atif_version_id = finalized.version_id
                result.atif_schema_version = "1.7"
            except (Exception, TimeoutError) as exc:
                if result.state == TrialState.SUCCEEDED:
                    result.state = TrialState.FAILED
                    result.failure_reason = (
                        result.failure_reason or FailureReason.TRAJECTORY_FLUSH_FAILED
                    )
                    result.failure_message = result.failure_message or _exception_message(
                        "finalize trajectory failed",
                        exc,
                    )

            result.finished_at = datetime.now(UTC)

            if self.state_patch is not None:
                # Bug 2 fix: catch general exceptions too — TimeoutError was
                # the only original except clause, leaving HTTP errors,
                # connection drops, etc. to propagate out of Trial.run.
                try:
                    await asyncio.wait_for(
                        asyncio.shield(
                            self.state_patch(
                                result.state.value,
                                result.failure_reason.value if result.failure_reason else None,
                                result.failure_message,
                            )
                        ),
                        timeout=_STATE_PATCH_TIMEOUT_SEC,
                    )
                except TimeoutError:
                    logger.warning(
                        "state PATCH timed out; crash detector will reclaim",
                    )
                except Exception:
                    logger.exception(
                        "state PATCH (terminal) failed; crash detector will reclaim",
                    )

            if cancelled:
                raise asyncio.CancelledError

        return result

    async def _resolve_llm_call_count(
        self,
        writer: TrajectoryWriter,
    ) -> tuple[int, list[dict[str, Any]] | None]:
        """Return (llm_call_count, pending_rows_to_append_or_None) for #1186.

        Gateway-emitting agents (terminus-2) already wrote LLMCallEvents to
        the trajectory writer. Other agents fetch CP ``llm_calls`` rows here
        so terminal-state selection can see the count; append happens later
        (before TrialEndEvent) via ``_append_llm_call_rows``.
        """
        if _agent_emits_gateway_llm_call_events(self.ctx.agent):
            return writer.llm_call_event_count, None
        if self.ctx.llm_calls_fetcher is None:
            return 0, None
        try:
            rows = await self.ctx.llm_calls_fetcher(self.ctx.trial_id)
        except Exception:
            logger.exception(
                "failed to fetch llm_calls before terminal-state selection; "
                "treating llm_call_count as 0",
            )
            return 0, None
        return len(rows), rows

    async def _append_llm_call_rows(
        self,
        writer: TrajectoryWriter,
        rows: list[dict[str, Any]],
    ) -> None:
        for row in rows:
            await writer.append(
                llm_call_row_to_event(row, trial_id=self.ctx.trial_id, seq=0)
            )

    async def _append_llm_call_events(self, writer: TrajectoryWriter) -> None:
        """Plan 9/11 A11.1: query CP for llm_calls rows + project each
        into a valid LLMCallEvent on the local JSONL.

        Cost-attribution facts (tokens, model, rate_card_hash) come
        straight from the row. Conversation content (system_prompt,
        messages, response, finish_reason) is set to placeholders
        because the Gateway doesn't persist message bodies — the
        adapter's `capture_events()` is the source of truth for
        message-level semantics, this projection is the source of
        truth for billing.
        """
        assert self.ctx.llm_calls_fetcher is not None
        rows = await self.ctx.llm_calls_fetcher(self.ctx.trial_id)
        await self._append_llm_call_rows(writer, rows)


def _agent_emits_gateway_llm_call_events(agent: AgentRuntime) -> bool:
    return bool(getattr(agent, "emits_gateway_llm_call_events", False))


def _aggregate(ctx: TrialContext, result: TrialResult) -> dict[str, float] | None:
    strategy = ctx.task_config.multi_step.reward_strategy if ctx.task_config.multi_step else "mean"
    rewards = [s.verifier_result.rewards for s in result.steps if s.verifier_result is not None]
    if not rewards:
        return None
    keys: set[str] = set()
    for r in rewards:
        keys.update(r.keys())
    if not keys:
        return None
    if strategy == "final":
        return dict(rewards[-1])
    if strategy == "min":
        return {k: min(r.get(k, 0.0) for r in rewards) for k in keys}
    return {k: sum(r.get(k, 0.0) for r in rewards) / len(rewards) for k in keys}


def _merge_extra_hosts(
    task_extra_hosts: dict[str, str],
    sandbox_extra_hosts: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    return (
        *tuple(sorted(task_extra_hosts.items())),
        *sandbox_extra_hosts,
    )


def _first_terminal_step_failure(
    result: TrialResult,
    *,
    llm_call_count: int = 0,
) -> tuple[FailureReason, str | None] | None:
    model_backed = result.agent.model is not None
    for step in result.steps:
        verifier_result = step.verifier_result
        if step.error is not None:
            canonical_failure = _failure_from_step_error(step.error)
            # Coding benchmark agents can exit non-zero after producing code; if
            # the verifier still produced a numeric score, keep the platform run
            # successful and let the reward carry model/agent correctness.
            # #1186: do NOT suppress platform/setup agent failures, or
            # model-backed agents that never reached the gateway/provider.
            suppress_scored_agent_error = (
                step.error.phase == "agent"
                and canonical_failure[0] != FailureReason.AGENT_TIMEOUT
                and verifier_result is not None
                and bool(verifier_result.rewards)
                and not is_platform_setup_agent_failure(step.error.message)
                and not (model_backed and llm_call_count == 0)
            )
            if not suppress_scored_agent_error:
                return canonical_failure
        if (
            verifier_result is not None
            and verifier_result.error is not None
            and not verifier_result.rewards
        ):
            if verifier_result.error.kind == "timeout":
                return FailureReason.VERIFIER_TIMEOUT, verifier_result.error.message
            return FailureReason.VERIFIER_ERROR, verifier_result.error.message
    return None


def _failure_from_step_error(error: StepError) -> tuple[FailureReason, str | None]:
    if error.phase == "agent":
        if error.reason == "exception":
            text_result = classify_failure_message(error.message)
            if text_result is not None:
                return text_result
        reason = (
            FailureReason.AGENT_TIMEOUT if error.reason == "timeout" else FailureReason.AGENT_ERROR
        )
        return reason, error.message
    if error.phase == "verifier":
        reason = (
            FailureReason.VERIFIER_TIMEOUT
            if error.reason == "timeout"
            else FailureReason.VERIFIER_ERROR
        )
        return reason, error.message
    if error.phase == "artifacts":
        if error.reason == "missing_artifacts":
            return FailureReason.MISSING_REQUIRED_ARTIFACTS, error.message
        return FailureReason.ARTIFACT_UPLOAD_FAILED, error.message
    return FailureReason.INTERNAL_ERROR, error.message


def _exception_message(prefix: str, exc: BaseException) -> str:
    message = f"{prefix}: {type(exc).__name__}: {exc}"
    return redact_text(message, limit=1000).strip()


def _format_tb(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


class _SeqCounter:
    """Monotonic per-trial sequence counter used by Trial.run()."""

    def __init__(self) -> None:
        self._n = 0

    def next(self) -> int:
        v = self._n
        self._n += 1
        return v
