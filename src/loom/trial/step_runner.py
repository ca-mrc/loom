"""_run_step — per-step body (spec §3.4).

Phase order: prepare (skipped in v1; setup.sh lives in Plan 7) → agent →
verifier → artifact collection. Errors are recorded as StepError and the loop
continues to the next phase. Provenance-gated private workspaces use a fresh
verifier driver; ordinary tasks preserve the legacy single-driver path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import posixpath
import time
from datetime import UTC, datetime
from math import ceil
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from uuid import UUID

from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.driver.base import Driver, StartOptions
from loom.errors import AgentError, classify_failure, classify_failure_message, exception_info
from loom.models.networking import NetworkPolicy
from loom.models.result import ArtifactRef, FailureReason, StepError, StepResult
from loom.models.task import StepConfig
from loom.models.trajectory import (
    AgentAttemptStartEvent,
    AgentRetryEvent,
    AgentStepTokenGrantEvent,
    AgentTimeoutEvent,
    StepEndEvent,
    StepStartEvent,
)
from loom.models.trial import RetryPolicy, RetryReason
from loom.models.verifier import VerifierError, VerifierResult
from loom.retry import next_attempt_at
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.artifacts import ArtifactCollector
from loom.trial.attempt_supervisor import supervise_agent_attempt
from loom.trial.phase_network import phase_network
from loom.trial.stale_running import effective_agent_timeout_sec
from loom.trial.workspace import materialize_workspace, refuse_planted_private_paths
from loom.trial.workspace_snapshot import handoff_workspace_snapshot
from loom.verifier_runtime import resolve_verifier_env_mode

if TYPE_CHECKING:
    from loom.trial.trial import TrialContext

logger = logging.getLogger(__name__)


# JWT NumericDate claims have whole-second precision while an attempt deadline
# retains sub-second precision.  Reserve the required five minutes plus one
# encoding second so the signed token cannot end just before the attempt.
_STEP_JWT_TTL_BUFFER_SEC = 301


def _apply_step_token_ttl(agent: object, effective_agent_timeout_sec: float) -> None:
    """Keep a subprocess agent's static step JWT valid for the resolved step.

    ``effective_agent_timeout_sec`` is the value produced by
    ``_resolve_agent_timeout`` and therefore already includes task defaults,
    step overrides, trial overrides, and the trial multiplier.
    """
    if not hasattr(agent, "step_token_ttl_sec"):
        return
    agent.step_token_ttl_sec = ceil(effective_agent_timeout_sec) + _STEP_JWT_TTL_BUFFER_SEC


class _VerifierDriverLease:
    """Idempotent ownership of the private verifier driver lifecycle."""

    def __init__(self) -> None:
        self.driver: Driver | None = None

    async def close(self, *, delete: bool) -> None:
        driver = self.driver
        if driver is None:
            return
        self.driver = None
        await driver.stop(delete=delete)


async def run_step(
    *,
    ctx: TrialContext,
    step: StepConfig,
    trajectory: TrajectoryWriter,
    baseline_policy: NetworkPolicy,
) -> StepResult:
    lease = _VerifierDriverLease()
    try:
        return await _run_step_impl(
            ctx=ctx,
            step=step,
            trajectory=trajectory,
            baseline_policy=baseline_policy,
            verifier_driver_lease=lease,
        )
    finally:
        # Cancellation and watchdog paths bypass ordinary result assembly.
        # The lease makes this cleanup idempotent with the normal path below.
        try:
            await _close_verifier_driver(
                lease,
                delete=ctx.trial_config.delete_env,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to stop isolated verifier driver")


async def _run_step_impl(
    *,
    ctx: TrialContext,
    step: StepConfig,
    trajectory: TrajectoryWriter,
    baseline_policy: NetworkPolicy,
    verifier_driver_lease: _VerifierDriverLease,
) -> StepResult:
    sr_started = datetime.now(UTC)
    sr_error: StepError | None = None

    instruction = _resolve_instruction(ctx, step)
    seq = _SeqCounter()
    workdir = ctx.task_config.environment.workdir

    await trajectory.append(
        StepStartEvent(
            emitted_at=datetime.now(UTC),
            trial_id=ctx.trial_id,
            step_id=step.name,
            seq=seq.next(),
            instruction_excerpt=instruction[:200],
        )
    )

    if getattr(ctx.agent, "_loom_worker_unhealthy", False):
        return await _finish_unhealthy_step(
            ctx=ctx,
            step=step,
            trajectory=trajectory,
            seq=seq,
            started_at=sr_started,
            error=StepError(
                phase="agent",
                reason="timeout",
                message="prior agent attempt remained active after cancellation drain",
                occurred_at=datetime.now(UTC),
            ),
        )

    # Agent phase ──────────────────────────────────────────────────────────
    agent_timeout = _resolve_agent_timeout(ctx, step)
    _apply_step_token_ttl(ctx.agent, agent_timeout)
    agent_phase: NetworkPolicy = (
        step.network.agent_phase if step.network and step.network.agent_phase else baseline_policy
    )
    sr_error = await _run_agent_with_retry(
        ctx=ctx,
        step=step,
        trajectory=trajectory,
        baseline_policy=baseline_policy,
        agent_phase=agent_phase,
        agent_timeout=agent_timeout,
        instruction=instruction,
        seq=seq,
    )

    # A cancellation-resistant agent still owns and may mutate its workspace.
    # Preserve the platform-owned timeout/step-end trajectory, but do not start
    # verifier or artifact reads against that concurrently changing state. The
    # worker layer observes this flag after terminal projection and restarts.
    if getattr(ctx.agent, "_loom_worker_unhealthy", False):
        return await _finish_unhealthy_step(
            ctx=ctx,
            step=step,
            trajectory=trajectory,
            seq=seq,
            started_at=sr_started,
            error=sr_error,
        )

    artifacts_uri: str | None = None
    artifacts: list[ArtifactRef] = []

    # Verifier phase ───────────────────────────────────────────────────────
    verifier_result = None
    verifier_env: Driver = ctx.driver
    isolated_verifier_driver: Driver | None = None
    if not ctx.trial_config.skip_verifier:
        verifier_timeout = _resolve_verifier_timeout(ctx, step)
        verifier_phase: NetworkPolicy = (
            step.network.verifier_phase
            if step.network and step.network.verifier_phase
            else baseline_policy
        )
        verifier_started = time.monotonic()
        try:
            in_place = resolve_verifier_env_mode(ctx.task_config, ctx.trial_config) == "shared"
            if ctx.workspace_staging_policy is not None and in_place:
                await refuse_planted_private_paths(ctx.driver, workdir)
                await materialize_workspace(
                    driver=ctx.driver,
                    task_dir=ctx.task_dir,
                    dst=workdir,
                    policy=ctx.workspace_staging_policy,
                    phase="verifier",
                )
            elif ctx.workspace_staging_policy is not None:
                factory = ctx.verifier_driver_factory
                if factory is None:
                    raise RuntimeError(
                        "private workspace staging requires a fresh verifier driver",
                    )
                isolated_verifier_driver = factory()
                verifier_driver_lease.driver = isolated_verifier_driver
                verifier_env = isolated_verifier_driver
                await verifier_env.start(options=_isolated_verifier_start_options(ctx))
                # The verifier driver receives a fresh public bundle plus its
                # private verifier-only files.  We then copy only non-private
                # agent workspace files across the process boundary; an agent
                # cannot create a lookalike verifier path that overwrites the
                # trusted verifier asset.
                await materialize_workspace(
                    driver=verifier_env,
                    task_dir=ctx.task_dir,
                    dst=workdir,
                    policy=ctx.workspace_staging_policy,
                    phase="agent",
                )
                await materialize_workspace(
                    driver=verifier_env,
                    task_dir=ctx.task_dir,
                    dst=workdir,
                    policy=ctx.workspace_staging_policy,
                    phase="verifier",
                )
                await _handoff_agent_workspace(
                    agent_driver=ctx.driver,
                    verifier_driver=verifier_env,
                    workdir=workdir,
                    policy=ctx.workspace_staging_policy,
                    preserve_acls=ctx.task_config.environment.preserve_acls,
                )
            async with phase_network(
                verifier_env,
                baseline=baseline_policy,
                phase=verifier_phase,
            ):
                reader = TrajectoryReader(ctx.local_trajectory_path)
                verifier_result = await asyncio.wait_for(
                    ctx.verifier.verify(
                        task=ctx.task_config,
                        env=verifier_env,
                        artifacts_dir=workdir,
                        trajectory=reader,
                    ),
                    timeout=verifier_timeout,
                )
        except TimeoutError:
            elapsed_sec = time.monotonic() - verifier_started
            message = f"verifier exceeded {verifier_timeout}s"
            # #377: run a best-effort post-mortem probe against the sandbox
            # while the container is still up so operators can distinguish
            # "verifier stuck in a wait" from "task genuinely too slow" from
            # "harness bug" without a rerun. The probe is non-mutating and
            # bounded by its own short timeout so we never let it turn a
            # timeout into a hang.
            probe_output = await _post_mortem_verifier_probe(verifier_env)
            verifier_result = VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="timeout",
                    message=message,
                    detail={
                        "timeout_sec": verifier_timeout,
                        "elapsed_sec": elapsed_sec,
                        "step_name": step.name,
                        "verifier_name": getattr(ctx.verifier, "name", None),
                        "post_mortem_probe": probe_output,
                    },
                ),
            )
            if sr_error is None:
                sr_error = StepError(
                    phase="verifier",
                    reason="timeout",
                    message=message,
                    occurred_at=datetime.now(UTC),
                )
        except Exception as exc:
            # Bug 3 fix: previously only TimeoutError was caught. A
            # VerifierError (registry mismatch) or driver failure mid-verify
            # would escape step_runner entirely, bypassing per-step error
            # tracking and step_end emission. Mirror the agent-phase pattern.
            if sr_error is None:
                sr_error = StepError(
                    phase="verifier",
                    reason="exception",
                    message=str(exc),
                    occurred_at=datetime.now(UTC),
                )

    # Artifact collection ──────────────────────────────────────────────────
    # Preserve the final workspace state, after the verifier has inspected the
    # same files. This keeps verifier-required outputs from being invisible
    # when they are not part of the generic artifact glob list.
    collector = ArtifactCollector(
        store=ctx.object_store,
        bucket=ctx.artifacts_bucket,
        team_id=str(ctx.team_id),
        trial_id=str(ctx.trial_id),
        step_name=step.name,
        local_root=ctx.local_trajectory_path.parent / "artifacts" / step.name,
        workspace_root=workdir,
    )
    try:
        collection = await collector.collect(
            env=verifier_env,
            patterns=_artifact_patterns(ctx, step),
            required_patterns=list(step.required_artifacts),
            platform_patterns=_verifier_artifact_patterns(ctx, verifier_result),
        )
        artifacts_uri = collection.prefix
        artifacts = [
            ArtifactRef(
                step_name=step.name,
                bucket=artifact.bucket,
                key=artifact.key,
                size=artifact.size,
                content_hash=artifact.content_hash,
                version_id=artifact.version_id,
                share_status=artifact.share_status,
                blocked_reason=artifact.blocked_reason,
            )
            for artifact in collection.artifacts
        ]
        if collection.missing_required and sr_error is None:
            missing = ", ".join(collection.missing_required)
            sr_error = StepError(
                phase="artifacts",
                reason="missing_artifacts",
                message=(
                    f"missing verifier-required artifacts for step {step.name}: "
                    f"{missing}; write these files under {workdir.as_posix()} "
                    "before verifier exit, or rerun because this trial's "
                    "artifact evidence is incomplete"
                ),
                occurred_at=datetime.now(UTC),
            )
    except Exception as exc:
        if sr_error is None:
            sr_error = StepError(
                phase="artifacts",
                reason="exception",
                message=str(exc),
                occurred_at=datetime.now(UTC),
            )

    if isolated_verifier_driver is not None:
        try:
            await _close_verifier_driver(
                verifier_driver_lease,
                delete=ctx.trial_config.delete_env,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if sr_error is None:
                sr_error = StepError(
                    phase="verifier",
                    reason="cleanup",
                    message=f"verifier driver cleanup failed: {exc}",
                    occurred_at=datetime.now(UTC),
                )

    sr_finished = datetime.now(UTC)
    step_result = StepResult(
        step_name=step.name,
        started_at=sr_started,
        finished_at=sr_finished,
        verifier_result=verifier_result,
        error=sr_error,
        artifacts_uri=artifacts_uri,
        artifacts=artifacts,
    )

    await trajectory.append(
        StepEndEvent(
            emitted_at=datetime.now(UTC),
            trial_id=ctx.trial_id,
            step_id=step.name,
            seq=seq.next(),
            summary=verifier_result.rewards if verifier_result else None,
            error_phase=sr_error.phase if sr_error else None,
        )
    )
    return step_result


class _SeqCounter:
    def __init__(self) -> None:
        self._n = 0

    def next(self) -> int:
        v = self._n
        self._n += 1
        return v


async def _finish_unhealthy_step(
    *,
    ctx: TrialContext,
    step: StepConfig,
    trajectory: TrajectoryWriter,
    seq: _SeqCounter,
    started_at: datetime,
    error: StepError | None,
) -> StepResult:
    """Terminalize a step without reading a workspace still owned by an agent."""

    result = StepResult(
        step_name=step.name,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        verifier_result=None,
        error=error,
        artifacts_uri=None,
        artifacts=[],
    )
    await trajectory.append(
        StepEndEvent(
            emitted_at=datetime.now(UTC),
            trial_id=ctx.trial_id,
            step_id=step.name,
            seq=seq.next(),
            summary=None,
            error_phase=error.phase if error else "agent",
        )
    )
    return result


def _isolated_verifier_start_options(ctx: TrialContext) -> StartOptions:
    """Options for the verifier-only container.

    It shares only task runtime networking/configuration. Agent credentials,
    family state, and JWT bind mounts deliberately stay on the agent driver.
    """
    return StartOptions(
        force_build=ctx.trial_config.force_build,
        network=ctx.sandbox_network,
        environment=tuple(sorted(ctx.task_config.environment.environment.items())),
        extra_hosts=tuple(
            sorted(
                {
                    *ctx.task_config.environment.extra_hosts.items(),
                    *ctx.sandbox_extra_hosts,
                },
            ),
        ),
        dns=tuple(ctx.task_config.environment.dns),
        tmpfs=tuple(ctx.task_config.environment.tmpfs),
        cpus=ctx.task_config.environment.cpus,
        memory_mb=ctx.task_config.environment.memory_mb,
        storage_mb=ctx.task_config.environment.storage_mb,
        gpus=ctx.task_config.environment.gpus,
        labels=tuple(
            sorted(
                {
                    **dict(ctx.runtime_identity_labels),
                    "loom.trial-container": "true",
                    "loom.driver-role": "verifier",
                    "loom.trial_id": str(ctx.trial_id),
                    "loom.team_id": str(ctx.team_id),
                    "loom.task_id": ctx.task_id,
                }.items(),
            ),
        ),
        container_cpus=ctx.container_cpus,
        container_memory_mib=ctx.container_memory_mib,
        container_pids=ctx.container_pids,
        cgroup_parent=ctx.container_cgroup_parent,
    )


async def _handoff_agent_workspace(
    *,
    agent_driver: Driver,
    verifier_driver: Driver,
    workdir: PurePosixPath,
    policy: object,
    preserve_acls: bool = False,
) -> None:
    """Snapshot the public agent workspace into the fresh verifier driver."""
    from loom.trial.workspace import WorkspaceStagingPolicy

    if not isinstance(policy, WorkspaceStagingPolicy):
        raise TypeError("private verifier handoff requires WorkspaceStagingPolicy")
    await handoff_workspace_snapshot(
        agent_driver=agent_driver,
        verifier_driver=verifier_driver,
        workdir=workdir,
        policy=policy,
        preserve_acls=preserve_acls,
    )


async def _close_verifier_driver(
    lease: _VerifierDriverLease,
    *,
    delete: bool,
) -> None:
    """Finish verifier teardown before propagating caller cancellation.

    ``asyncio.shield`` alone returns immediately when its caller is cancelled,
    leaving the protected stop task detached.  Retain and wait for the cleanup
    task so sidecar/network teardown cannot race a still-running verifier.
    """

    cleanup = asyncio.create_task(lease.close(delete=delete))
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            await asyncio.shield(cleanup)
            break
        except asyncio.CancelledError as exc:
            if cleanup.cancelled():
                raise
            cancellation = exc
            continue
    try:
        cleanup.result()
    except Exception:
        if cancellation is not None:
            logger.exception("isolated verifier cleanup failed during cancellation")
        else:
            raise
    if cancellation is not None:
        raise cancellation


def _artifact_patterns(ctx: TrialContext, step: StepConfig) -> list[str]:
    # ``.loom/verifier`` is platform-owned. Ignore task-authored patterns in
    # that namespace, including broad globs and normalized traversal aliases;
    # only the exact names selected below may reach ArtifactCollector.
    patterns = [pattern for pattern in step.artifacts if not _is_reserved_verifier_pattern(pattern)]
    # `openhands` is the legacy alias of `openhands-sdk` (#2054); same runner.
    if ctx.agent.name in {"terminus-2", "openhands-sdk", "openhands"}:
        loom_agent = ".loom/agent/**"
        if loom_agent not in patterns:
            patterns.append(loom_agent)
    return patterns


_VERIFIER_PLATFORM_PATHS = frozenset(
    {
        ".loom/verifier/script.log",
        ".loom/verifier/script.log.meta.json",
        ".loom/verifier/output.json",
        ".loom/verifier/pytest.log",
        ".loom/verifier/pytest.log.meta.json",
        ".loom/verifier/pytest-install.log",
        ".loom/verifier/pytest-install.log.meta.json",
        ".loom/verifier/junit.xml",
    }
)


def _verifier_artifact_patterns(
    ctx: TrialContext,
    verifier_result: VerifierResult | None,
) -> list[str]:
    """Exact trusted names collected through ArtifactCollector's platform lane."""
    patterns: list[str] = []
    if ctx.agent.name == "terminus-2":
        patterns.extend(
            [
                ".loom/verifier/pytest.log",
                ".loom/verifier/pytest.log.meta.json",
            ]
        )
    structured = verifier_result.structured if verifier_result is not None else None
    if not isinstance(structured, dict):
        return patterns
    audit = structured.get("loom_verifier_audit")
    if not isinstance(audit, dict):
        return patterns
    artifacts = audit.get("artifacts")
    if not isinstance(artifacts, list):
        return patterns
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        path = artifact.get("path")
        if isinstance(path, str) and path in _VERIFIER_PLATFORM_PATHS:
            if path not in patterns:
                patterns.append(path)
    return patterns


def _is_reserved_verifier_pattern(pattern: str) -> bool:
    normalized = posixpath.normpath(pattern).lstrip("/")
    return normalized == ".loom/verifier" or normalized.startswith(".loom/verifier/")


def _resolve_instruction(ctx: TrialContext, step: StepConfig) -> str:
    step_dir = ctx.task_dir / "steps" / step.name
    candidate = step_dir / step.instruction_file
    if not candidate.is_file():
        candidate = ctx.task_dir / step.instruction_file
    if candidate.is_file():
        return candidate.read_text(encoding="utf-8")
    return ""


_POST_MORTEM_PROBE_TIMEOUT_SEC = 10.0


async def _post_mortem_verifier_probe(driver: object) -> str:
    """Best-effort post-mortem probe when the verifier times out (#377).

    Emits a single non-mutating shell that captures:

    * ``ps`` snapshot so the operator can see what verifier/agent
      processes are still running inside the sandbox.
    * A listing of the standard verifier output directory (``/loom/verifier``)
      to reveal whether the script wrote anything before the timeout hit.
    * ``uptime`` so the operator can eyeball load / stall.

    The probe runs with its own short wall-clock timeout so a hung
    sandbox can't turn a verifier timeout into a step-runner hang. Any
    failure is folded into the returned string so evidence is always a
    single text field.
    """
    probe_cmd = (
        "echo -- UPTIME ; uptime 2>&1 || true ; "
        "echo -- PS ; ps aux 2>&1 | head -50 || true ; "
        "echo -- LOOM_VERIFIER_DIR ; "
        "ls -la /loom/verifier 2>&1 || true"
    )
    exec_ = getattr(driver, "exec", None)
    if exec_ is None:
        return "post-mortem probe unavailable: driver has no .exec()"
    try:
        async with asyncio.timeout(_POST_MORTEM_PROBE_TIMEOUT_SEC):
            result = await exec_(probe_cmd, user="root")
    except (TimeoutError, asyncio.CancelledError):
        return (
            f"post-mortem probe timed out after "
            f"{_POST_MORTEM_PROBE_TIMEOUT_SEC:.0f}s (sandbox likely wedged)"
        )
    except Exception as exc:  # pragma: no cover - defensive
        return f"post-mortem probe raised {type(exc).__name__}: {exc}"

    stdout = getattr(result, "stdout", b"") or b""
    stderr = getattr(result, "stderr", b"") or b""
    parts: list[str] = []
    if stdout:
        with contextlib.suppress(Exception):
            parts.append(stdout.decode("utf-8", errors="replace"))
    if stderr:
        with contextlib.suppress(Exception):
            parts.append("-- STDERR --\n" + stderr.decode("utf-8", errors="replace"))
    return "\n".join(parts) or "(no probe output)"


def _resolve_agent_timeout(ctx: TrialContext, step: StepConfig) -> float:
    timeout = effective_agent_timeout_sec(
        task_config=ctx.task_config,
        trial_config=ctx.trial_config,
        step_config=step,
    )
    if timeout is None:  # TaskConfig validation guarantees a positive default.
        raise ValueError("agent timeout could not be resolved")
    return timeout


def _resolve_verifier_timeout(ctx: TrialContext, step: StepConfig) -> float:
    base = ctx.task_config.verifier.timeout_sec
    if step.verifier and step.verifier.timeout_sec is not None:
        base = step.verifier.timeout_sec
    if ctx.trial_config.override_verifier_timeout_sec is not None:
        base = ctx.trial_config.override_verifier_timeout_sec
    return base * ctx.trial_config.verifier_timeout_multiplier


async def _run_agent_with_retry(
    *,
    ctx: TrialContext,
    step: StepConfig,
    trajectory: TrajectoryWriter,
    baseline_policy: NetworkPolicy,
    agent_phase: NetworkPolicy,
    agent_timeout: float,
    instruction: str,
    seq: _SeqCounter,
) -> StepError | None:
    policy = ctx.trial_config.retry
    attempt = 1
    while True:
        try:

            async def run_attempt(
                guarded_trajectory: TrajectoryWriter,
            ) -> None:
                try:
                    if ctx.task_config.environment.preserve_acls:
                        from loom.trial.workspace_acls import require_acl_support

                        await require_acl_support(ctx.driver, ctx.task_config.environment.workdir)
                    async with phase_network(
                        ctx.driver,
                        baseline=baseline_policy,
                        phase=agent_phase,
                    ):
                        await ctx.agent.run(
                            instruction=instruction,
                            env=ctx.driver,
                            trajectory=guarded_trajectory,
                            mcp=[],
                            skills_dir=None,
                            step_id=step.name,
                        )
                except AgentError as exc:
                    text_result = classify_failure_message(str(exc))
                    if text_result is not None and text_result[0] == FailureReason.AGENT_TIMEOUT:
                        raise AttemptDeadlineExceededError(text_result[1] or str(exc)) from exc
                    raise
                except Exception as exc:
                    failure_reason, failure_message = classify_failure(exc)
                    if failure_reason == FailureReason.AGENT_TIMEOUT:
                        raise AttemptDeadlineExceededError(failure_message or str(exc)) from exc
                    raise

            async def record_attempt_started(
                deadline: AttemptDeadline,
                attempt_trajectory: TrajectoryWriter,
            ) -> None:
                assert deadline.agent_attempt_id is not None
                await attempt_trajectory.append(
                    AgentAttemptStartEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=ctx.trial_id,
                        step_id=step.name,
                        seq=seq.next(),
                        agent_attempt_id=deadline.agent_attempt_id,
                        attempt_deadline_wall_clock=deadline.wall_deadline,
                    )
                )

            async def record_step_token_grant(
                agent_attempt_id: UUID,
                step_jwt_id: UUID,
                attempt_trajectory: TrajectoryWriter,
            ) -> None:
                await attempt_trajectory.append(
                    AgentStepTokenGrantEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=ctx.trial_id,
                        step_id=step.name,
                        seq=seq.next(),
                        agent_attempt_id=agent_attempt_id,
                        step_jwt_id=step_jwt_id,
                    )
                )

            timeout_diagnostic = await supervise_agent_attempt(
                agent=ctx.agent,
                configured_timeout_sec=agent_timeout,
                trajectory=trajectory,
                run=run_attempt,
                on_attempt_started=record_attempt_started,
                on_step_token_grant=record_step_token_grant,
            )
            if timeout_diagnostic is None:
                return None

            message = f"agent run exceeded {agent_timeout}s"
            await trajectory.append(
                AgentTimeoutEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=ctx.trial_id,
                    step_id=step.name,
                    seq=seq.next(),
                    configured_timeout_sec=(timeout_diagnostic.configured_timeout_sec),
                    elapsed_monotonic_sec=(timeout_diagnostic.elapsed_monotonic_sec),
                    cancellation_drain_sec=(timeout_diagnostic.cancellation_drain_sec),
                    transport_close_required=(timeout_diagnostic.transport_close_required),
                    task_stopped=timeout_diagnostic.task_stopped,
                    agent_attempt_id=timeout_diagnostic.agent_attempt_id,
                )
            )
            if timeout_diagnostic.task_stopped and await _maybe_retry_agent_failure(
                policy=policy,
                retry_reason=RetryReason.AGENT_TIMEOUT,
                failure_message=message,
                attempt=attempt,
                trajectory=trajectory,
                ctx=ctx,
                step=step,
                seq=seq,
            ):
                attempt += 1
                continue
            return StepError(
                phase="agent",
                reason="timeout",
                message=message,
                occurred_at=datetime.now(UTC),
            )
        except TimeoutError:
            message = f"agent run exceeded {agent_timeout}s"
            if await _maybe_retry_agent_failure(
                policy=policy,
                retry_reason=RetryReason.AGENT_TIMEOUT,
                failure_message=message,
                attempt=attempt,
                trajectory=trajectory,
                ctx=ctx,
                step=step,
                seq=seq,
            ):
                attempt += 1
                continue
            return StepError(
                phase="agent",
                reason="timeout",
                message=message,
                occurred_at=datetime.now(UTC),
            )
        except AgentError as exc:
            info = exception_info(exc)
            text_result = classify_failure_message(str(exc))
            if text_result is not None:
                failure_reason, failure_message = text_result
                retry_reason = _retry_reason_for_failure(failure_reason)
                if retry_reason is not None and await _maybe_retry_agent_failure(
                    policy=policy,
                    retry_reason=retry_reason,
                    failure_message=failure_message,
                    attempt=attempt,
                    trajectory=trajectory,
                    ctx=ctx,
                    step=step,
                    seq=seq,
                ):
                    attempt += 1
                    continue
                return StepError(
                    phase="agent",
                    reason="exception",
                    message=failure_message or info.exception_message,
                    exception_type=info.exception_type,
                    occurred_at=datetime.now(UTC),
                )
            return StepError(
                phase="agent",
                reason="exception",
                message=info.exception_message,
                exception_type=info.exception_type,
                occurred_at=datetime.now(UTC),
            )
        except Exception as exc:
            failure_reason, failure_message = classify_failure(exc)
            retry_reason = _retry_reason_for_failure(failure_reason)
            if retry_reason is not None and await _maybe_retry_agent_failure(
                policy=policy,
                retry_reason=retry_reason,
                failure_message=failure_message,
                attempt=attempt,
                trajectory=trajectory,
                ctx=ctx,
                step=step,
                seq=seq,
            ):
                attempt += 1
                continue
            raise


def _retry_reason_for_failure(reason: FailureReason) -> RetryReason | None:
    if reason == FailureReason.GATEWAY_ERROR:
        return RetryReason.GATEWAY_ERROR
    if reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT:
        return RetryReason.PROVIDER_TRANSPORT_DISCONNECT
    if reason == FailureReason.STEP_CREDENTIAL_INVALID_OR_EXPIRED:
        return RetryReason.STEP_CREDENTIAL_INVALID_OR_EXPIRED
    if reason == FailureReason.STEP_CREDENTIAL_SCOPE_INVALID:
        return RetryReason.STEP_CREDENTIAL_SCOPE_INVALID
    if reason == FailureReason.ENV_START_FAILURE:
        return RetryReason.ENV_START_FAILURE
    if reason == FailureReason.AGENT_TIMEOUT:
        return RetryReason.AGENT_TIMEOUT
    if reason == FailureReason.VERIFIER_TIMEOUT:
        return RetryReason.VERIFIER_TIMEOUT
    if reason == FailureReason.TRAJECTORY_FLUSH_FAILED:
        return RetryReason.TRAJECTORY_FLUSH_FAILED
    return None


async def _maybe_retry_agent_failure(
    *,
    policy: RetryPolicy,
    retry_reason: RetryReason,
    failure_message: str | None,
    attempt: int,
    trajectory: TrajectoryWriter,
    ctx: TrialContext,
    step: StepConfig,
    seq: _SeqCounter,
) -> bool:
    if attempt >= policy.max_attempts:
        return False
    if retry_reason not in policy.retry_on:
        return False

    now = datetime.now(UTC)
    retry_at = next_attempt_at(
        attempt_count=attempt,
        backoff=policy.backoff,
        now=now,
    )
    delay = max(0.0, (retry_at - now).total_seconds())
    await trajectory.append(
        AgentRetryEvent(
            emitted_at=datetime.now(UTC),
            trial_id=ctx.trial_id,
            step_id=step.name,
            seq=seq.next(),
            attempt=attempt,
            max_attempts=policy.max_attempts,
            failure_reason=retry_reason.value,
            failure_message=failure_message,
            retry_after_sec=delay,
        )
    )
    await asyncio.sleep(delay)
    return True
