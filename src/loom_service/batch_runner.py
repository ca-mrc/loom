"""Batch runner background loop (spec §7 / Plan 19 Task 5, renamed
in Plan 28).

The runner is a single asyncio task spawned from the service lifespan.
On each tick it:

1. `SELECT FOR UPDATE SKIP LOCKED` rows from `batches` where
   `state IN ('submitted', 'running')`. SKIP LOCKED makes concurrent
   instances safe — each replica processes a disjoint slice.
2. For each batch, resolves the `task_filter` into the live task
   list, subtracts task_ids already submitted under this batch,
   and POSTs the remainder to Control Plane. Each submission carries
   an `idempotency_key = "{batch_id}::{task_id}::{sample_idx}"` so
   re-running the loop (or a CP retry) never produces duplicate
   trial rows.
3. Recomputes the batch's state from current trial counts and
   advances the row.

`next_batch_state` is split out so the state machine is
unit-testable without standing up Postgres.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    Batch,
    BatchFamilyState,
    LlmCall,
    Task,
    Trial,
    Worker,
)
from loom_service.task_config_validation import (
    expected_trial_count,
    split_valid_task_configs,
)
from loom_service.task_filter import resolve_task_filter
from loom_service.usage_accounting import hard_budget_exceeded_diagnostic

logger = logging.getLogger(__name__)

_TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})
_IN_FLIGHT: frozenset[str] = frozenset(
    {"queued", "protected-pending", "claimed", "running"}
)
_NON_RETRYABLE_SUBMIT_STATUSES: frozenset[int] = frozenset({400, 403, 404, 409, 422})
_MAX_SUBMIT_ERROR_DETAIL_LEN = 500


@dataclass(frozen=True)
class _SubmitResult:
    ok: bool
    retryable: bool
    error: dict[str, Any] | None = None


@dataclass(frozen=True)
class _HardBudgetCancellation:
    batch_id: UUID
    trial_ids: tuple[UUID, ...]
    diagnostic: dict[str, Any]
    consumed: Decimal
    budget: Decimal


@dataclass(frozen=True)
class PendingUnit:
    task_id: str
    combination_idx: int | None
    trial_config: dict[str, Any]
    sample_idx: int
    required_worker_pool: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None
    # #672 PR-3: populated when the parent batch opted into family-run
    # mode; the batch_runner looks the key up from batch_family_state
    # by task_id at fanout time. The CP submit endpoint persists it
    # onto ``trials.family_key`` so the scheduler's claim query can
    # gate the trial on family sequence position.
    family_key: str | None = None
    inherit_model_switch_plan_from_trial_id: UUID | None = None


def next_batch_state(
    *,
    current: str,
    expected: int,
    counts: Mapping[str, int],
) -> str:
    """Pure state-transition function.

    Inputs:
    - current: existing batch.state
    - expected: batch.expected_trial_count (materialized at create)
    - counts: {trial_state: count_of_trials_in_that_state}

    Outputs the new state. Rules:
    - cancelled is absorbing.
    - all expected trials terminal + 0 in flight → finished.
    - any trial submitted (in-flight or terminal) → running.
    - otherwise stay in current.
    """
    if current == "cancelled":
        return "cancelled"
    terminal_count = sum(counts.get(k, 0) for k in _TERMINAL)
    in_flight = sum(counts.get(k, 0) for k in _IN_FLIGHT)
    if expected > 0 and terminal_count >= expected and in_flight == 0:
        return "finished"
    if in_flight > 0 or terminal_count > 0:
        return "running"
    return current


def _idempotency_key(
    batch_id: UUID,
    task_id: str,
    sample_idx: int,
    combination_idx: int | None = None,
    required_worker_pool: str | None = None,
) -> str:
    """Stable, inspectable key — operators can grep for it in logs.

    Plan 28 PR-3 grew the key when the batch has Combinations:
    single-combination batches keep the 3-segment
    `{batch}::{task}::{sample}` shape (preserves in-flight keys);
    multi-combination batches use 4 segments,
    `{batch}::{task}::{combination}::{sample}`.
    """
    if combination_idx is None:
        base = f"{batch_id}::{task_id}::{sample_idx}"
    else:
        base = f"{batch_id}::{task_id}::{combination_idx}::{sample_idx}"
    if required_worker_pool:
        return f"{base}::pool::{required_worker_pool}"
    return base


def _single_line_excerpt(text: str) -> str:
    cleaned = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
    return cleaned[:_MAX_SUBMIT_ERROR_DETAIL_LEN]


def _response_detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return _single_line_excerpt(resp.text)
    if isinstance(body, dict):
        detail = body.get("detail")
        if isinstance(detail, str):
            return _single_line_excerpt(detail)
        if detail is not None:
            return _single_line_excerpt(str(detail))
    return _single_line_excerpt(resp.text)


def _fanout_errors(batch: Batch) -> list[dict[str, Any]]:
    return [item for item in (batch.fanout_errors or []) if isinstance(item, dict)]


def _fanout_error_keys(errors: list[dict[str, Any]]) -> set[str]:
    return {str(item["idempotency_key"]) for item in errors if item.get("idempotency_key")}


def _materialize_trial_config(
    shared: dict[str, Any], combination: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the per-trial config for one Combination.

    Shared trial_config supplies the common knobs (timeouts, retry,
    skip_verifier, etc.); the Combination supplies agent_name +
    agent_model. The merge clobbers `agent_name` / `agent_model` on
    the shared config (route forbids them when combinations is
    non-empty, but defense in depth).
    """
    from loom_service.multi_model import apply_plan_mode

    if combination is None:
        return apply_plan_mode(shared, mode=shared.get("model_switch_plan_mode"))
    import copy as _copy
    out: dict[str, Any] = _copy.deepcopy(shared)
    out["agent_name"] = combination["agent_name"]
    if combination.get("agent_version") is not None:
        out["agent_version"] = combination["agent_version"]
    else:
        out.pop("agent_version", None)
    out["agent_model"] = combination.get("agent_model")
    return apply_plan_mode(out, mode=out.get("model_switch_plan_mode"))


def _coerce_uuid(value: object) -> UUID | None:
    if value is None or value == "":
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _effective_provider_fields(
    batch: Batch,
    combination: Mapping[str, Any] | None,
) -> tuple[UUID | None, str | None]:
    # Batches submitted since #2054 store each combination's resolved pair;
    # the field-by-field inheritance below only serves older batches.
    conn_id = batch.provider_connection_id
    model_id = batch.provider_model_id
    if combination is not None:
        if combination.get("agent_model") is None:
            # No-model agents (oracle) never run on a provider route.
            return None, None
        conn_id = _coerce_uuid(
            combination.get("provider_connection_id"),
        ) or conn_id
        raw_model_id = combination.get("provider_model_id")
        if isinstance(raw_model_id, str) and raw_model_id:
            model_id = raw_model_id
    return conn_id, model_id


def _batch_required_worker_pools(batch: Batch) -> list[str]:
    values = batch.required_worker_pools or []
    pools: list[str] = []
    seen: set[str] = set()
    for raw in values:
        pool = str(raw).strip()
        if not pool or pool in seen:
            continue
        seen.add(pool)
        pools.append(pool)
    return pools


def _task_cpu_arch(config: object) -> str:
    if not isinstance(config, Mapping):
        return "x86_64"
    environment = config.get("environment")
    if not isinstance(environment, Mapping):
        return "x86_64"
    raw = environment.get("cpu_arch", "x86_64")
    if not isinstance(raw, str):
        return "x86_64"
    arch = raw.strip()
    return arch or "x86_64"


def _worker_capability_cpu_arch(capability: object) -> str:
    if not isinstance(capability, Mapping):
        return "x86_64"
    raw = capability.get("cpu_arch", "x86_64")
    if not isinstance(raw, str):
        return "x86_64"
    arch = raw.strip()
    return arch or "x86_64"


async def _known_pool_cpu_arches(
    session: AsyncSession,
    pool_names: list[str],
) -> dict[str, tuple[str, ...]]:
    if not pool_names:
        return {}
    arches_by_pool: dict[str, set[str]] = {pool: set() for pool in pool_names}
    worker_rows = (
        await session.execute(
            select(Worker.pool_name, Worker.capabilities).where(
                Worker.pool_name.in_(pool_names),
                Worker.status == "active",
                Worker.drain_state == "active",
            ),
        )
    ).all()
    for pool_name, capabilities in worker_rows:
        if not isinstance(pool_name, str) or pool_name not in arches_by_pool:
            continue
        if not isinstance(capabilities, list):
            continue
        for capability in capabilities:
            arches_by_pool[pool_name].add(_worker_capability_cpu_arch(capability))

    return {
        pool: tuple(sorted(arches))
        for pool, arches in arches_by_pool.items()
        if arches
    }


async def _task_cpu_arches(
    session: AsyncSession,
    task_ids: list[str],
) -> dict[str, str]:
    rows = (
        await session.execute(
            select(Task.id, Task.config).where(Task.id.in_(task_ids)),
        )
    ).all()
    return {str(task_id): _task_cpu_arch(config) for task_id, config in rows}


def _cpu_arch_compatible(task_cpu_arch: str, pool_cpu_arches: tuple[str, ...]) -> bool:
    return (
        task_cpu_arch == "any"
        or "any" in pool_cpu_arches
        or task_cpu_arch in pool_cpu_arches
    )


def _group_task_cpu_arches(
    task_ids: list[str],
    task_cpu_arches: Mapping[str, str],
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for task_id in task_ids:
        arch = task_cpu_arches.get(task_id, "x86_64")
        grouped.setdefault(arch, []).append(task_id)
    return {arch: grouped[arch] for arch in sorted(grouped)}


def _coverage_incompatible_error(
    batch: Batch,
    *,
    task_id: str,
    sample_idx: int,
    combination_idx: int | None,
    required_worker_pool: str,
    pool_cpu_arches: tuple[str, ...],
    task_cpu_arches: Mapping[str, str],
    task_ids: list[str],
) -> dict[str, Any]:
    key = _idempotency_key(
        batch.id,
        task_id,
        sample_idx,
        combination_idx=combination_idx,
        required_worker_pool=required_worker_pool,
    )
    task_arch_summary = _group_task_cpu_arches(task_ids, task_cpu_arches)
    detail = (
        f"required_worker_pool {required_worker_pool!r} advertises "
        f"cpu_arch {list(pool_cpu_arches)!r}, but selected tasks require "
        f"cpu_arch {task_arch_summary!r}; refusing to submit unclaimable "
        "coverage trial"
    )
    return {
        "task_id": task_id,
        "sample_idx": sample_idx,
        "combination_idx": combination_idx,
        "idempotency_key": key,
        "status_code": 400,
        "reason": "required_worker_pool_incompatible",
        "required_worker_pool": required_worker_pool,
        "pool_cpu_arches": list(pool_cpu_arches),
        "task_cpu_arches": task_arch_summary,
        "detail": detail,
        "seen_at": datetime.now(UTC).isoformat(),
    }


async def _coverage_pending_units(
    session: AsyncSession,
    batch: Batch,
    *,
    task_ids: list[str],
    shared_config: dict[str, Any],
    failed_fanout_keys: set[str],
    existing_idempotency_keys: set[str],
) -> tuple[list[PendingUnit], list[dict[str, Any]]]:
    required_pools = _batch_required_worker_pools(batch)
    if not required_pools or not task_ids:
        return [], []

    combination_idx: int | None = None
    cfg = shared_config
    base_sample_idx = int(batch.n_per_task)
    if batch.combinations:
        combination_idx = 0
        cfg = _materialize_trial_config(shared_config, batch.combinations[0])
        base_sample_idx = int(batch.combinations[0].get("n_per_task", 1))
    provider_connection_id, provider_model_id = _effective_provider_fields(
        batch,
        batch.combinations[0] if batch.combinations else None,
    )

    task_cpu_arches = await _task_cpu_arches(session, task_ids)
    pool_cpu_arches_by_name = await _known_pool_cpu_arches(session, required_pools)
    units: list[PendingUnit] = []
    errors: list[dict[str, Any]] = []
    for offset, pool in enumerate(required_pools):
        sample_idx = base_sample_idx + offset
        pool_cpu_arches = pool_cpu_arches_by_name.get(pool)
        task_id = task_ids[0]
        if pool_cpu_arches:
            compatible_task_id = next(
                (
                    candidate
                    for candidate in task_ids
                    if _cpu_arch_compatible(
                        task_cpu_arches.get(candidate, "x86_64"),
                        pool_cpu_arches,
                    )
                ),
                None,
            )
            if compatible_task_id is None:
                key = _idempotency_key(
                    batch.id,
                    task_id,
                    sample_idx,
                    combination_idx=combination_idx,
                    required_worker_pool=pool,
                )
                if key in failed_fanout_keys or key in existing_idempotency_keys:
                    continue
                errors.append(
                    _coverage_incompatible_error(
                        batch,
                        task_id=task_id,
                        sample_idx=sample_idx,
                        combination_idx=combination_idx,
                        required_worker_pool=pool,
                        pool_cpu_arches=pool_cpu_arches,
                        task_cpu_arches=task_cpu_arches,
                        task_ids=task_ids,
                    ),
                )
                continue
            task_id = compatible_task_id
        key = _idempotency_key(
            batch.id,
            task_id,
            sample_idx,
            combination_idx=combination_idx,
            required_worker_pool=pool,
        )
        if key in failed_fanout_keys or key in existing_idempotency_keys:
            continue
        units.append(
            PendingUnit(
                task_id=task_id,
                combination_idx=combination_idx,
                trial_config=cfg,
                sample_idx=sample_idx,
                required_worker_pool=pool,
                provider_connection_id=provider_connection_id,
                provider_model_id=provider_model_id,
            ),
        )
    return units, errors


def _with_family_key(
    unit: PendingUnit, family_key: str | None,
) -> PendingUnit:
    """Return a copy of ``unit`` with ``family_key`` set. #672 PR-3."""
    if family_key is None:
        return unit
    return PendingUnit(
        task_id=unit.task_id,
        combination_idx=unit.combination_idx,
        trial_config=unit.trial_config,
        sample_idx=unit.sample_idx,
        required_worker_pool=unit.required_worker_pool,
        provider_connection_id=unit.provider_connection_id,
        provider_model_id=unit.provider_model_id,
        family_key=family_key,
        inherit_model_switch_plan_from_trial_id=(
            unit.inherit_model_switch_plan_from_trial_id
        ),
    )


async def _load_family_key_map(
    session: AsyncSession, batch_id: UUID,
) -> dict[str, str]:
    """Return ``task_id -> family_key`` from the batch's seeded family
    state (#672 PR-3).

    Empty dict when the batch is not in family-run mode — the fanout
    path treats it as ``.get(task_id) -> None``.
    """
    rows = (await session.execute(
        select(
            BatchFamilyState.family_key,
            BatchFamilyState.task_sequence,
        ).where(BatchFamilyState.batch_id == batch_id),
    )).all()
    out: dict[str, str] = {}
    for family_key, task_sequence in rows:
        for task_id in (task_sequence or []):
            out[str(task_id)] = str(family_key)
    return out


def _rerun_targets(batch: Batch) -> list[dict[str, Any]]:
    return [item for item in (batch.rerun_targets or []) if isinstance(item, dict)]


async def _pending_rerun_units(
    session: AsyncSession,
    batch: Batch,
    targets: list[dict[str, Any]],
    failed_fanout_keys: set[str],
) -> list[PendingUnit]:
    target_task_ids = sorted({str(t.get("task_id")) for t in targets if t.get("task_id")})
    valid_task_ids, invalid_tasks = await split_valid_task_configs(
        session, target_task_ids,
    )
    valid_task_id_set = set(valid_task_ids)
    if invalid_tasks:
        logger.warning(
            "rerun batch %s skipped %d invalid target task configs: %s",
            batch.id,
            len(invalid_tasks),
            [item.task_id for item in invalid_tasks],
        )

    existing = {
        (row[0], int(row[1]), int(row[2]))
        for row in (await session.execute(
            select(Trial.task_id, Trial.combination_idx, Trial.sample_idx)
            .where(Trial.batch_id == batch.id),
        )).all()
    }
    shared_config = dict(batch.trial_config)
    pending: list[PendingUnit] = []
    for target in targets:
        task_id = str(target.get("task_id") or "")
        if not task_id or task_id not in valid_task_id_set:
            continue
        sample_idx = int(target.get("sample_idx") or 0)
        combination_idx = int(target.get("combination_idx") or 0)
        key = _idempotency_key(
            batch.id,
            task_id,
            sample_idx,
            combination_idx=combination_idx,
        )
        if key in failed_fanout_keys:
            continue
        if (task_id, combination_idx, sample_idx) in existing:
            continue
        combination = None
        if batch.combinations:
            if combination_idx < 0 or combination_idx >= len(batch.combinations):
                logger.warning(
                    "rerun batch %s target has out-of-range combination_idx=%s",
                    batch.id,
                    combination_idx,
                )
                continue
            combination = batch.combinations[combination_idx]
        cfg = _materialize_trial_config(shared_config, combination)
        provider_connection_id, provider_model_id = _effective_provider_fields(
            batch,
            combination,
        )
        inherit_from = None
        raw_origin = target.get("original_trial_id")
        if raw_origin:
            inherit_from = _coerce_uuid(raw_origin)
        pending.append(
            PendingUnit(
                task_id=task_id,
                combination_idx=combination_idx,
                trial_config=cfg,
                sample_idx=sample_idx,
                provider_connection_id=provider_connection_id,
                provider_model_id=provider_model_id,
                inherit_model_switch_plan_from_trial_id=inherit_from,
            ),
        )
    return pending


async def _submit_one(
    http_client: httpx.AsyncClient,
    *,
    authorization: str | None,
    batch_id: UUID,
    task_id: str,
    sample_idx: int,
    trial_config: dict[str, Any],
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    combination_idx: int | None = None,
    required_worker_pool: str | None = None,
    family_key: str | None = None,
    inherit_model_switch_plan_from_trial_id: UUID | None = None,
) -> _SubmitResult:
    idempotency_key = _idempotency_key(
        batch_id,
        task_id,
        sample_idx,
        combination_idx=combination_idx,
        required_worker_pool=required_worker_pool,
    )
    payload: dict[str, Any] = {
        "task_id": task_id,
        "config": trial_config,
        "batch_id": str(batch_id),
        "sample_idx": sample_idx,
        "idempotency_key": idempotency_key,
    }
    if combination_idx is not None:
        payload["combination_idx"] = combination_idx
    if required_worker_pool is not None:
        payload["required_worker_pool"] = required_worker_pool
    if provider_connection_id is not None:
        payload["provider_connection_id"] = str(provider_connection_id)
    if provider_model_id is not None:
        payload["provider_model_id"] = provider_model_id
    if family_key is not None:
        payload["family_key"] = family_key
    if inherit_model_switch_plan_from_trial_id is not None:
        payload["inherit_model_switch_plan_from_trial_id"] = str(
            inherit_model_switch_plan_from_trial_id,
        )
    headers: dict[str, str] = {}
    if authorization:
        headers["Authorization"] = authorization
    try:
        resp = await http_client.post(
            "/trials", json=payload, headers=headers,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "batch %s task %s submit error: %s",
            batch_id, task_id, exc,
        )
        return _SubmitResult(ok=False, retryable=True)
    if resp.status_code >= 400:
        detail = _response_detail(resp)
        logger.warning(
            "batch %s task %s submit failed: %s %s",
            batch_id, task_id, resp.status_code, resp.text,
        )
        retryable = resp.status_code not in _NON_RETRYABLE_SUBMIT_STATUSES
        return _SubmitResult(
            ok=False,
            retryable=retryable,
            error=None if retryable else {
                "task_id": task_id,
                "sample_idx": sample_idx,
                "combination_idx": combination_idx,
                "idempotency_key": idempotency_key,
                "status_code": resp.status_code,
                "detail": detail,
                "seen_at": datetime.now(UTC).isoformat(),
            },
        )
    return _SubmitResult(ok=True, retryable=False)


def _compute_result_status(
    terminal_states: list[str],
) -> str:
    """Outcome classifier for a finished batch.

    Per the spec: succeeded / partial_failed / all_failed.
    Cancelled is set elsewhere (lifecycle override).

    A trial's platform outcome is its terminal state. Reward is model/evaluator
    outcome data and can be absent or zero without making a completed trial a
    platform failure.
    """
    succeeded = sum(1 for state in terminal_states if state == "succeeded")
    failed = len(terminal_states) - succeeded
    if failed == 0 and succeeded > 0:
        return "succeeded"
    if succeeded == 0:
        return "all_failed"
    return "partial_failed"


async def _advance_batch_state(
    session: AsyncSession, batch: Batch,
) -> None:
    state_rows = (await session.execute(
        select(Trial.state).where(Trial.batch_id == batch.id),
    )).scalars().all()
    counts: dict[str, int] = {}
    for st in state_rows:
        counts[str(st)] = counts.get(str(st), 0) + 1
    new_state = next_batch_state(
        current=batch.state,
        expected=batch.expected_trial_count,
        counts=counts,
    )

    values: dict[str, Any] = {}
    if new_state != batch.state:
        values["state"] = new_state
        if new_state in ("finished", "cancelled"):
            values["finished_at"] = datetime.now(UTC)
        if new_state == "cancelled":
            values["result_status"] = "cancelled"
        elif new_state == "finished":
            computed = _compute_result_status([str(st) for st in state_rows])
            if batch.result_status == "partial_failed" and (
                computed == "succeeded"
            ):
                values["result_status"] = "partial_failed"
            else:
                values["result_status"] = computed

    if values:
        await session.execute(
            update(Batch).where(Batch.id == batch.id).values(**values),
        )


async def _cancel_batch_for_hard_budget(
    session: AsyncSession,
    batch: Batch,
) -> _HardBudgetCancellation | None:
    if batch.state == "cancelled":
        return None
    if str(getattr(batch, "budget_policy", "none") or "none") != "hard":
        return None
    if batch.budget_usd is None:
        return None

    row = (
        await session.execute(
            select(
                func.coalesce(func.sum(LlmCall.cost_usd), 0).label("cost_usd"),
                func.count(LlmCall.id).label("llm_calls_count"),
            )
            .join(Trial, Trial.id == LlmCall.trial_id)
            .where(Trial.batch_id == batch.id),
        )
    ).one()
    if int(row.llm_calls_count or 0) == 0:
        return None

    consumed = Decimal(str(row.cost_usd or 0))
    budget = Decimal(str(batch.budget_usd))
    if consumed <= budget:
        return None

    diagnostic = hard_budget_exceeded_diagnostic(
        batch_id=batch.id,
        budget_usd=float(budget),
        estimated_cost_usd=float(consumed),
    )
    trial_ids = tuple(
        (
            await session.execute(
                select(Trial.id).where(
                    Trial.batch_id == batch.id,
                    Trial.state.in_(list(_IN_FLIGHT)),
                ),
            )
        )
        .scalars()
        .all()
    )
    return _HardBudgetCancellation(
        batch_id=batch.id,
        trial_ids=trial_ids,
        diagnostic=diagnostic,
        consumed=consumed,
        budget=budget,
    )


async def _request_hard_budget_cancellation(
    http_client: httpx.AsyncClient,
    cancellation: _HardBudgetCancellation,
    *,
    authorization: str | None,
) -> None:
    headers = {"Authorization": authorization} if authorization else {}
    for trial_id in cancellation.trial_ids:
        try:
            response = await http_client.post(
                f"/trials/{trial_id}/cancel",
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise RuntimeError("hard-budget trial cancellation is unavailable") from exc
        if response.status_code not in {200, 409}:
            raise RuntimeError("hard-budget trial cancellation was rejected")


async def _finalize_hard_budget_cancellation(
    session: AsyncSession,
    cancellation: _HardBudgetCancellation,
) -> None:
    batch = (
        await session.execute(
            select(Batch).where(Batch.id == cancellation.batch_id).with_for_update(),
        )
    ).scalar_one_or_none()
    if batch is None or batch.state == "cancelled":
        return
    diagnostics = [
        item for item in (batch.budget_diagnostics or []) if isinstance(item, dict)
    ]
    await session.execute(
        update(Batch)
        .where(Batch.id == cancellation.batch_id)
        .values(
            state="cancelled",
            result_status="cancelled",
            finished_at=datetime.now(UTC),
            budget_diagnostics=[*diagnostics, cancellation.diagnostic],
        ),
    )
    logger.warning(
        "batch %s cancelled after hard budget exceeded: cost=%s budget=%s",
        cancellation.batch_id,
        cancellation.consumed,
        cancellation.budget,
    )


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    batch_size: int,
    submit_rate_per_sec: int,
    cp_authorization: str | None = None,
) -> None:
    """Process all non-terminal batches once. Safe to call from a loop.

    The locking strategy splits each tick into three short transactions:

    1. SELECT … FOR UPDATE SKIP LOCKED the batch rows to claim them
       for this runner instance, materialize the pending task list,
       COMMIT (release the lock).
    2. HTTP fanout to Control Plane. No DB locks held — trial INSERTs
       on the CP side need a key-share lock on the parent batch row,
       which would deadlock against a held FOR UPDATE.
    3. Re-open a transaction, advance each batch's state from the
       current trial counts.

    Concurrent runner safety: the idempotency_key
    `{batch_id}::{task_id}::{sample_idx}` is the cross-process dedupe
    key — a second runner that picks up the same batch mid-tick
    (after we released our SKIP-LOCKED claim) submits the same
    payloads, and the CP's ON CONFLICT DO NOTHING on the partial
    unique index collapses the duplicates.

    `cp_authorization` is the bearer token the runner sends upstream to
    Control Plane — in production this is a service-owned token with
    `submit` scope.
    """
    delay = 1.0 / max(submit_rate_per_sec, 1)

    # Phase 1: pick + materialize work, then release the lock.
    # Pending unit is (task_id, combination_idx, sample_idx). For
    # single-combination batches combination_idx is None — the
    # idempotency key shape matches the 3-segment form that already
    # exists in the DB. For multi-combination, combination_idx is
    # 0..len(combinations)-1.
    #
    # Each work item is (batch_id, pending units). A PendingUnit carries the
    # effective provider route for that task/combination/sample.
    work: list[tuple[UUID, list[PendingUnit]]] = []
    hard_budget_cancellations: list[_HardBudgetCancellation] = []
    fanout_errors_by_batch: dict[UUID, list[dict[str, Any]]] = {}
    async with session_factory() as s:
        batches_to_process = (await s.execute(
            select(Batch)
            .where(Batch.state.in_(["submitted", "running"]))
            .with_for_update(skip_locked=True),
        )).scalars().all()
        for b in batches_to_process:
            hard_budget_cancellation = await _cancel_batch_for_hard_budget(s, b)
            if hard_budget_cancellation is not None:
                hard_budget_cancellations.append(hard_budget_cancellation)
                continue
            existing_fanout_errors = _fanout_errors(b)
            failed_fanout_keys = _fanout_error_keys(existing_fanout_errors)
            targets = _rerun_targets(b)
            if targets:
                rerun_pending_units = await _pending_rerun_units(
                    s, b, targets, failed_fanout_keys,
                )
                family_key_map = await _load_family_key_map(s, b.id)
                if family_key_map:
                    rerun_pending_units = [
                        _with_family_key(u, family_key_map.get(u.task_id))
                        for u in rerun_pending_units
                    ]
                work.append((b.id, rerun_pending_units))
                continue
            task_ids = (
                list(b.resolved_task_ids)
                if b.resolved_task_ids is not None
                else await resolve_task_filter(
                    s,
                    b.task_filter,
                    team_id=b.team_id,
                    require_runnable=False,
                )
            )
            task_ids, invalid_tasks = await split_valid_task_configs(
                s, task_ids,
            )
            if invalid_tasks:
                adjusted_expected = max(
                    0,
                    expected_trial_count(
                        task_count=len(task_ids),
                        n_per_task=b.n_per_task,
                        combinations=b.combinations,
                    )
                    + (len(_batch_required_worker_pools(b)) if task_ids else 0)
                    - len(failed_fanout_keys),
                )
                values: dict[str, Any] = {
                    "expected_trial_count": adjusted_expected,
                }
                if adjusted_expected == 0:
                    values.update({
                        "state": "finished",
                        "result_status": "all_failed",
                        "finished_at": datetime.now(UTC),
                    })
                elif b.result_status is None:
                    values["result_status"] = "partial_failed"
                if (
                    b.expected_trial_count != adjusted_expected
                    or any(
                        getattr(b, key) != value
                        for key, value in values.items()
                        if hasattr(b, key)
                    )
                ):
                    await s.execute(
                        update(Batch).where(Batch.id == b.id).values(**values),
                    )
                logger.warning(
                    "batch %s skipped %d invalid task configs: %s",
                    b.id,
                    len(invalid_tasks),
                    [item.task_id for item in invalid_tasks],
                )
                b.expected_trial_count = adjusted_expected
                if adjusted_expected == 0:
                    continue
                if b.result_status is None:
                    b.result_status = "partial_failed"
            if b.combinations:
                # Multi-combination: existing key is
                # (task_id, combination_idx, sample_idx).
                existing_multi = {
                    (row[0], row[1], row[2])
                    for row in (await s.execute(
                        select(
                            Trial.task_id,
                            Trial.combination_idx,
                            Trial.sample_idx,
                        ).where(Trial.batch_id == b.id),
                    )).all()
                }
                pending_units: list[PendingUnit] = []
                shared_config = dict(b.trial_config)
                existing_idempotency_keys = {
                    str(row[0])
                    for row in (await s.execute(
                        select(Trial.idempotency_key).where(
                            Trial.batch_id == b.id,
                            Trial.idempotency_key.is_not(None),
                        ),
                    )).all()
                }
                for c_idx, combo in enumerate(b.combinations):
                    combo_config = _materialize_trial_config(
                        shared_config, combo,
                    )
                    provider_connection_id, provider_model_id = (
                        _effective_provider_fields(b, combo)
                    )
                    n = int(combo.get("n_per_task", 1))
                    for t in task_ids:
                        for s_idx in range(n):
                            key = _idempotency_key(
                                b.id, t, s_idx, combination_idx=c_idx,
                            )
                            if key in failed_fanout_keys:
                                continue
                            if (t, c_idx, s_idx) in existing_multi:
                                continue
                            pending_units.append(
                                PendingUnit(
                                    task_id=t,
                                    combination_idx=c_idx,
                                    trial_config=combo_config,
                                    sample_idx=s_idx,
                                    provider_connection_id=provider_connection_id,
                                    provider_model_id=provider_model_id,
                                ),
                            )
                coverage_units, coverage_errors = await _coverage_pending_units(
                    s,
                    b,
                    task_ids=task_ids,
                    shared_config=shared_config,
                    failed_fanout_keys=failed_fanout_keys,
                    existing_idempotency_keys=existing_idempotency_keys,
                )
                pending_units.extend(coverage_units)
                if coverage_errors:
                    fanout_errors_by_batch.setdefault(b.id, []).extend(
                        coverage_errors,
                    )
                work.append((b.id, pending_units))
            else:
                # Single-combination: keep the 2-tuple key shape and
                # the None combination_idx so the resulting
                # idempotency_key uses the 3-segment format.
                existing_single = {
                    (row[0], row[1])
                    for row in (await s.execute(
                        select(Trial.task_id, Trial.sample_idx).where(
                            Trial.batch_id == b.id,
                        ),
                    )).all()
                }
                existing_idempotency_keys = {
                    str(row[0])
                    for row in (await s.execute(
                        select(Trial.idempotency_key).where(
                            Trial.batch_id == b.id,
                            Trial.idempotency_key.is_not(None),
                        ),
                    )).all()
                }
                pending_units = []
                cfg = dict(b.trial_config)
                provider_connection_id, provider_model_id = _effective_provider_fields(
                    b,
                    None,
                )
                for t in task_ids:
                    for s_idx in range(b.n_per_task):
                        key = _idempotency_key(b.id, t, s_idx)
                        if key in failed_fanout_keys:
                            continue
                        if (t, s_idx) in existing_single:
                            continue
                        pending_units.append(
                            PendingUnit(
                                task_id=t,
                                combination_idx=None,
                                trial_config=cfg,
                                sample_idx=s_idx,
                                provider_connection_id=provider_connection_id,
                                provider_model_id=provider_model_id,
                            ),
                        )
                coverage_units, coverage_errors = await _coverage_pending_units(
                    s,
                    b,
                    task_ids=task_ids,
                    shared_config=cfg,
                    failed_fanout_keys=failed_fanout_keys,
                    existing_idempotency_keys=existing_idempotency_keys,
                )
                pending_units.extend(coverage_units)
                if coverage_errors:
                    fanout_errors_by_batch.setdefault(b.id, []).extend(
                        coverage_errors,
                    )
                family_key_map = await _load_family_key_map(s, b.id)
                if family_key_map:
                    pending_units = [
                        _with_family_key(u, family_key_map.get(u.task_id))
                        for u in pending_units
                    ]
                work.append((b.id, pending_units))
        await s.commit()

    # Phase 2: cancellation and fanout HTTP calls. No DB locks.
    for cancellation in hard_budget_cancellations:
        await _request_hard_budget_cancellation(
            http_client,
            cancellation,
            authorization=cp_authorization,
        )
        async with session_factory() as s:
            await _finalize_hard_budget_cancellation(s, cancellation)
            await s.commit()

    for batch_id, pending_units in work:
        for chunk_start in range(0, len(pending_units), batch_size):
            chunk = pending_units[chunk_start:chunk_start + batch_size]
            for unit in chunk:
                submit_result = await _submit_one(
                    http_client,
                    authorization=cp_authorization,
                    batch_id=batch_id,
                    task_id=unit.task_id,
                    sample_idx=unit.sample_idx,
                    trial_config=unit.trial_config,
                    provider_connection_id=unit.provider_connection_id,
                    provider_model_id=unit.provider_model_id,
                    combination_idx=unit.combination_idx,
                    required_worker_pool=unit.required_worker_pool,
                    family_key=unit.family_key,
                    inherit_model_switch_plan_from_trial_id=(
                        unit.inherit_model_switch_plan_from_trial_id
                    ),
                )
                if (
                    not submit_result.ok
                    and not submit_result.retryable
                    and submit_result.error is not None
                ):
                    fanout_errors_by_batch.setdefault(batch_id, []).append(
                        submit_result.error,
                    )
                await asyncio.sleep(delay)

    if fanout_errors_by_batch:
        async with session_factory() as s:
            for batch_id, new_errors in fanout_errors_by_batch.items():
                row = (await s.execute(
                    select(Batch)
                    .where(Batch.id == batch_id)
                    .with_for_update(),
                )).scalar_one_or_none()
                if row is None or row.state == "cancelled":
                    continue
                existing_errors = _fanout_errors(row)
                seen_keys = _fanout_error_keys(existing_errors)
                added: list[dict[str, Any]] = []
                for error in new_errors:
                    error_key_raw = error.get("idempotency_key")
                    if not error_key_raw:
                        continue
                    error_key = str(error_key_raw)
                    if error_key in seen_keys:
                        continue
                    seen_keys.add(error_key)
                    added.append(error)
                if not added:
                    continue
                adjusted_expected = max(
                    0,
                    int(row.expected_trial_count) - len(added),
                )
                update_values: dict[str, Any] = {
                    "fanout_errors": existing_errors + added,
                    "expected_trial_count": adjusted_expected,
                }
                if adjusted_expected == 0:
                    update_values.update({
                        "state": "finished",
                        "result_status": "all_failed",
                        "finished_at": datetime.now(UTC),
                    })
                elif row.result_status is None:
                    update_values["result_status"] = "partial_failed"
                await s.execute(
                    update(Batch).where(Batch.id == batch_id).values(**update_values),
                )
            await s.commit()

    # Phase 3: advance state for the batches we processed.
    async with session_factory() as s:
        for batch_id, _ in work:
            row = (await s.execute(
                select(Batch).where(Batch.id == batch_id),
            )).scalar_one_or_none()
            if row is None:
                continue
            await _advance_batch_state(s, row)
        await s.commit()


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    batch_size: int,
    submit_rate_per_sec: int,
    poll_interval_sec: int,
    cp_authorization: str | None = None,
) -> None:
    """Forever-loop entrypoint for the service lifespan.

    If `cp_authorization` is None the loop logs ONE warning and then
    skips submitting (still ticks the poll). Without a token every
    CP submit would 401 — better to surface the misconfig once than
    to spam the CP with failed POSTs."""
    warned_missing_token = False
    while True:
        try:
            if cp_authorization is None:
                if not warned_missing_token:
                    logger.warning(
                        "batch_runner has no CP token "
                        "(LOOM_SVC_BATCH_RUNNER_CP_TOKEN unset); "
                        "batches will queue but not fan out",
                    )
                    warned_missing_token = True
            else:
                await run_once(
                    session_factory=session_factory,
                    http_client=http_client,
                    batch_size=batch_size,
                    submit_rate_per_sec=submit_rate_per_sec,
                    cp_authorization=cp_authorization,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("batch_runner iteration failed")
        await asyncio.sleep(poll_interval_sec)
