"""Orchestrator main loop (#672 PR-2).

Every ``settings.family_orchestrator_poll_sec`` seconds:

1. ``SELECT ... FROM batch_family_state WHERE state = 'adapting'``
   ``AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())``
   ``ORDER BY updated_at FOR UPDATE SKIP LOCKED LIMIT 1``.
2. For each picked row: load the batch's resolved
   ``family_run_spec``, find the just-completed trial for the current
   sequence position, call ``adapter.evolve`` under
   ``family_adapter_call_timeout_sec``, and either apply
   ``NextFamilyState`` (bump index → ``pending``/``done``) or feed the
   exception into ``failure_policy.on_adapter_failure`` and apply the
   resulting ``FailureAction``.

The picker + writer are exposed as pure ``async`` functions so tests
can drive one iteration with a fake DB session or a testcontainer
Postgres.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.family_run.orchestration import (
    NextFamilyState,
    apply_advance_decision,
)
from loom.family_run.registry import resolve_plugin
from loom.family_run.spec import (
    AdvanceDecision,
    FailureAction,
    ResolvedFamilyRunSpec,
)

logger = logging.getLogger(__name__)


class _TrialCancellationClient(Protocol):
    async def cancel_trial(self, trial_id: str) -> None: ...


_PICK_SQL = text("""
SELECT batch_id,
       family_key,
       task_sequence,
       current_index,
       state_uri,
       attempt_count,
       next_attempt_at
  FROM batch_family_state
 WHERE state = 'adapting'
   AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
 ORDER BY updated_at
 FOR UPDATE SKIP LOCKED
 LIMIT 1
""")

_PICK_CANCELLATION_SQL = text("""
SELECT cancellation.batch_id,
       cancellation.family_key,
       cancellation.state,
       cancellation.task_sequence,
       cancellation.current_index
  FROM public.batch_family_state AS cancellation
 WHERE cancellation.state = 'cancelling'
    OR (
      cancellation.state = 'aborted'
      AND EXISTS (
        SELECT 1
          FROM public.trials AS pending_trial
         WHERE pending_trial.batch_id = cancellation.batch_id
           AND pending_trial.family_key = cancellation.family_key
           AND pending_trial.state IN ('queued', 'protected-pending')
      )
    )
 ORDER BY cancellation.updated_at
 FOR UPDATE OF cancellation SKIP LOCKED
 LIMIT 1
""")

_LOAD_BATCH_SPEC_SQL = text("""
SELECT family_run_spec,
       team_id
  FROM batches
 WHERE id = (:batch_id)::uuid
""")

_LOAD_TRIAL_SQL = text("""
SELECT id,
       task_id,
       state,
       result,
       attempt_count,
       trajectory_index->>'trajectory_uri' AS trajectory_uri
  FROM trials
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
   AND task_id = (:task_id)::text
   AND state IN ('succeeded', 'failed', 'cancelled')
 ORDER BY finished_at DESC NULLS LAST
 LIMIT 1
""")

_UPDATE_SUCCESS_SQL = text("""
UPDATE batch_family_state
   SET state = (:new_state)::text,
       current_index = (:new_current_index)::int,
       attempt_count = 0,
       state_uri = COALESCE((:new_state_uri)::text, state_uri),
       last_error = NULL,
       next_attempt_at = NULL,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
""")

_UPDATE_RETRY_SQL = text("""
UPDATE batch_family_state
   SET attempt_count = attempt_count + 1,
       next_attempt_at = (:next_attempt_at)::timestamptz,
       last_error = (:last_error)::text,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
""")

_UPDATE_STALLED_SQL = text("""
UPDATE batch_family_state
   SET state = 'stalled',
       last_error = (:last_error)::text,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
""")

_UPDATE_ABORTED_SQL = text("""
UPDATE batch_family_state
   SET state = 'aborted',
       last_error = (:last_error)::text,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
""")

_LOAD_REMAINING_TRIAL_IDS_SQL = text("""
SELECT pending_trial.id
  FROM public.trials AS pending_trial
 WHERE pending_trial.batch_id = (:batch_id)::uuid
   AND pending_trial.family_key = (:family_key)::text
   AND pending_trial.state IN ('queued', 'protected-pending')
 ORDER BY pending_trial.id
""")

_COMPLETE_CANCELLATION_SQL = text("""
UPDATE public.batch_family_state
   SET state = (:final_state)::text,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
   AND state = 'cancelling'
""")


@dataclass(frozen=True)
class _PickedFamily:
    batch_id: UUID
    family_key: str
    task_sequence: list[str]
    current_index: int
    state_uri: str | None
    attempt_count: int


@dataclass(frozen=True)
class _PickedCancellation:
    batch_id: UUID
    family_key: str
    state: str
    task_sequence: tuple[str, ...]
    current_index: int


@dataclass
class _TrialShim:
    id: UUID
    task_id: str
    state: str
    reward: float | None
    attempt_count: int


@dataclass
class _FamilyShim:
    batch_id: UUID
    family_key: str
    task_sequence: list[str]
    current_index: int
    attempt_count: int


def _build_state_backend(spec: ResolvedFamilyRunSpec, ctx: OrchestratorContext) -> Any:
    """Materialise the batch's configured state backend plugin.

    ``ctx.state_backend_factory`` overrides the entry-point-based
    default; tests inject an in-memory backend that way. The default
    resolves via ``loom.family.state_backends`` entry point and
    forwards the ObjectStore/bucket from the orchestrator context to
    S3ArtifactsStateBackend.
    """
    if ctx.state_backend_factory is not None:
        return ctx.state_backend_factory(spec)
    plugin = resolve_plugin("loom.family.state_backends", spec.state_backend)
    # S3ArtifactsStateBackend takes (store, bucket) at __init__ but
    # the default entry-point registration instantiates with zero
    # args. Poke the store + bucket in post-hoc when available.
    if hasattr(plugin, "store") and ctx.object_store is not None:
        plugin.store = ctx.object_store
    if hasattr(plugin, "bucket") and ctx.artifacts_bucket is not None:
        plugin.bucket = ctx.artifacts_bucket
    return plugin


@dataclass
class OrchestratorContext:
    """Runtime dependencies shared across iterations.

    Tests supply fakes; production wires up an httpx-backed gateway
    client + boto3-shaped ObjectStore inside ``run()``.
    """

    session_factory: async_sessionmaker[AsyncSession]
    gateway: Any = None
    cancellation_client: _TrialCancellationClient | None = None
    object_store: Any = None
    artifacts_bucket: str | None = None
    state_backend_factory: Any = None
    settings_default_model: str | None = None
    adapter_call_timeout_sec: float = 300.0
    poll_sec: float = 5.0


async def run(ctx: OrchestratorContext, *, stop_event: asyncio.Event | None = None) -> None:
    """Long-running loop. Exits cleanly when ``stop_event.is_set()``.

    Never re-raises exceptions from a single iteration - one bad
    family row must not take down the whole service.
    """
    stop = stop_event or asyncio.Event()
    while not stop.is_set():
        try:
            picked = await run_once(ctx)
        except Exception:
            logger.exception("family_orchestrator_iteration_error")
            picked = None
        if not picked:
            try:
                await asyncio.wait_for(stop.wait(), timeout=ctx.poll_sec)
            except TimeoutError:
                continue


async def run_once(ctx: OrchestratorContext) -> bool:
    """Run exactly one polling iteration. Returns True when a family
    row was picked + processed, False when the queue was empty (poll
    would sleep next)."""
    async with ctx.session_factory() as session:
        cancellation_row = (
            await session.execute(_PICK_CANCELLATION_SQL)
        ).mappings().one_or_none()
        if cancellation_row is not None:
            cancellation = _PickedCancellation(
                batch_id=UUID(str(cancellation_row["batch_id"])),
                family_key=str(cancellation_row["family_key"]),
                state=str(cancellation_row["state"]),
                task_sequence=tuple(cancellation_row["task_sequence"]),
                current_index=int(cancellation_row["current_index"]),
            )
            pending_rows = (
                await session.execute(
                    _LOAD_REMAINING_TRIAL_IDS_SQL,
                    {
                        "batch_id": cancellation.batch_id,
                        "family_key": cancellation.family_key,
                    },
                )
            ).mappings().all()
            trial_ids = tuple(UUID(str(item["id"])) for item in pending_rows)
            await session.commit()
            await _cancel_trials(ctx, trial_ids)
            if cancellation.state == "cancelling":
                final_state = (
                    "done"
                    if cancellation.current_index >= len(cancellation.task_sequence)
                    else "pending"
                )
                async with ctx.session_factory() as completion_session:
                    await completion_session.execute(
                        _COMPLETE_CANCELLATION_SQL,
                        {
                            "batch_id": cancellation.batch_id,
                            "family_key": cancellation.family_key,
                            "final_state": final_state,
                        },
                    )
                    await completion_session.commit()
            return True

        row = (await session.execute(_PICK_SQL)).mappings().one_or_none()
        if row is None:
            await session.commit()
            return False
        picked = _PickedFamily(
            batch_id=row["batch_id"],
            family_key=row["family_key"],
            task_sequence=list(row["task_sequence"]),
            current_index=row["current_index"],
            state_uri=row["state_uri"],
            attempt_count=row["attempt_count"],
        )
        spec_row = (await session.execute(
            _LOAD_BATCH_SPEC_SQL, {"batch_id": picked.batch_id},
        )).mappings().one_or_none()
        if spec_row is None or spec_row["family_run_spec"] is None:
            logger.warning(
                "family_orchestrator_missing_spec batch_id=%s family_key=%s",
                picked.batch_id, picked.family_key,
            )
            await session.execute(_UPDATE_STALLED_SQL, {
                "batch_id": picked.batch_id,
                "family_key": picked.family_key,
                "last_error": "batches.family_run_spec is NULL",
            })
            await session.commit()
            return True
        spec = ResolvedFamilyRunSpec.model_validate(spec_row["family_run_spec"])
        # Sequence position of the just-completed trial equals the
        # family's current_index at ADAPTING time.
        just_completed_task_id = picked.task_sequence[picked.current_index]
        trial_row = (await session.execute(_LOAD_TRIAL_SQL, {
            "batch_id": picked.batch_id,
            "family_key": picked.family_key,
            "task_id": just_completed_task_id,
        })).mappings().one_or_none()
        if trial_row is None:
            logger.warning(
                "family_orchestrator_missing_trial batch=%s family=%s task=%s",
                picked.batch_id, picked.family_key, just_completed_task_id,
            )
            await session.execute(_UPDATE_STALLED_SQL, {
                "batch_id": picked.batch_id,
                "family_key": picked.family_key,
                "last_error": (
                    "no terminal trial for task "
                    f"{just_completed_task_id!r}"
                ),
            })
            await session.commit()
            return True

        adapter = resolve_plugin("loom.family.adapters", spec.adapter)
        state_backend = _build_state_backend(spec, ctx)
        family_shim = _FamilyShim(
            batch_id=picked.batch_id,
            family_key=picked.family_key,
            task_sequence=picked.task_sequence,
            current_index=picked.current_index,
            attempt_count=picked.attempt_count,
        )
        trial_shim = _TrialShim(
            id=trial_row["id"],
            task_id=trial_row["task_id"],
            state=trial_row["state"],
            reward=_reward_from_result(trial_row["result"]),
            attempt_count=trial_row["attempt_count"] or 1,
        )
        adapter_params = dict(spec.adapter.params)
        # The persisted Batch row, not user-controlled plugin params, is the
        # authority for delegated provider access and usage attribution.
        adapter_params["team_id"] = str(spec_row["team_id"])
        if ctx.gateway is not None and "gateway" not in adapter_params:
            adapter_params["gateway"] = ctx.gateway
        if (
            ctx.settings_default_model is not None
            and "settings_default_model" not in adapter_params
        ):
            adapter_params["settings_default_model"] = ctx.settings_default_model
        if "call_timeout_sec" not in adapter_params:
            adapter_params["call_timeout_sec"] = ctx.adapter_call_timeout_sec
        if "trajectory_uri" not in adapter_params and trial_row.get("trajectory_uri"):
            adapter_params["trajectory_uri"] = trial_row["trajectory_uri"]

        try:
            new_state_uri = await asyncio.wait_for(
                adapter.evolve(
                    trial=trial_shim,
                    family=family_shim,
                    state_uri=picked.state_uri or "",
                    backend=state_backend,
                    params=adapter_params,
                ),
                timeout=ctx.adapter_call_timeout_sec,
            )
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
                raise
            logger.warning(
                "family_orchestrator_adapter_failed batch=%s family=%s err=%s",
                picked.batch_id, picked.family_key, exc,
            )
            policy = resolve_plugin(
                "loom.family.failure_policies", spec.failure_policy,
            )
            action = policy.on_adapter_failure(
                family=family_shim,
                exception=exc,
                params=spec.failure_policy.params,
            )
            cancellation_trial_ids = await _apply_failure_action(
                session,
                picked=picked,
                action=action,
                spec=spec,
                exception=exc,
            )
            await session.commit()
            await _cancel_trials(ctx, cancellation_trial_ids)
            return True

        # Success: apply ADVANCE decision, then bump index / mark done.
        advance = apply_advance_decision(family_shim, AdvanceDecision.ADVANCE)
        bumped_index = family_shim.current_index + 1
        if bumped_index >= len(family_shim.task_sequence):
            new_state = "done"
        else:
            new_state = "pending"
        await session.execute(_UPDATE_SUCCESS_SQL, {
            "batch_id": picked.batch_id,
            "family_key": picked.family_key,
            "new_state": new_state,
            "new_current_index": bumped_index,
            "new_state_uri": new_state_uri,
        })
        # Reference `advance` so it's part of the audit trail - the
        # variable itself confirms the pure state-machine result the
        # writer just materialised.
        del advance
        await session.commit()
        return True


async def _apply_failure_action(
    session: AsyncSession,
    *,
    picked: _PickedFamily,
    action: FailureAction,
    spec: ResolvedFamilyRunSpec,
    exception: BaseException,
) -> tuple[UUID, ...]:
    if action.kind == "retry_with_backoff":
        next_at = datetime.now(UTC) + timedelta(
            seconds=action.backoff_sec or 30.0,
        )
        await session.execute(_UPDATE_RETRY_SQL, {
            "batch_id": picked.batch_id,
            "family_key": picked.family_key,
            "next_attempt_at": next_at,
            "last_error": _short_repr(exception),
        })
        return ()
    if action.kind == "skip_and_advance":
        bumped_index = picked.current_index + 1
        if bumped_index >= len(picked.task_sequence):
            new_state = "done"
        else:
            new_state = "pending"
        await session.execute(_UPDATE_SUCCESS_SQL, {
            "batch_id": picked.batch_id,
            "family_key": picked.family_key,
            "new_state": new_state,
            "new_current_index": bumped_index,
            "new_state_uri": None,
        })
        return ()
    if action.kind == "abort_family":
        # StallFamilyPolicy's terminal action arrives as abort_family;
        # spec says the operator-recoverable state is 'stalled'.
        # abort_family policy semantics -> hard abort.
        # Distinguish by inspecting the policy name.
        if spec.failure_policy.name == "stall_family":
            await session.execute(_UPDATE_STALLED_SQL, {
                "batch_id": picked.batch_id,
                "family_key": picked.family_key,
                "last_error": _short_repr(exception),
            })
        else:
            await session.execute(_UPDATE_ABORTED_SQL, {
                "batch_id": picked.batch_id,
                "family_key": picked.family_key,
                "last_error": _short_repr(exception),
            })
            rows = (
                await session.execute(
                    _LOAD_REMAINING_TRIAL_IDS_SQL,
                    {
                        "batch_id": picked.batch_id,
                        "family_key": picked.family_key,
                    },
                )
            ).mappings().all()
            return tuple(UUID(str(row["id"])) for row in rows)
        return ()
    raise ValueError(f"unknown FailureAction.kind: {action.kind!r}")


async def _cancel_trials(
    ctx: OrchestratorContext,
    trial_ids: tuple[UUID, ...],
) -> None:
    if not trial_ids:
        return
    if ctx.cancellation_client is None:
        raise RuntimeError("family trial cancellation client is unavailable")
    for trial_id in trial_ids:
        await ctx.cancellation_client.cancel_trial(str(trial_id))


def _reward_from_result(result: Any) -> float | None:
    if not isinstance(result, dict):
        return None
    reward = result.get("reward")
    if isinstance(reward, (int, float)):
        return float(reward)
    return None


def _short_repr(exc: BaseException) -> str:
    text_str = f"{type(exc).__name__}: {exc}"
    return text_str[:500]


# Referenced by NextFamilyState import for downstream verification.
__all__ = ["NextFamilyState", "OrchestratorContext", "run", "run_once"]
