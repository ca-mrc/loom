"""Shared execution admission and immutable task-resource snapshots.

Batch creation, failed-case reruns and Run Library clone/reuse call these
operations with their own runtime policy. Callers own authorization and the
transaction; admission never commits or imports route modules.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, NoReturn

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select

from loom.agent_runtime_registry import resolve_agent_runtimes
from loom.db.schema import Task
from loom.execution_selection import resolved_execution_selection
from loom.hosted_harness import harnesses_supporting, hosted_harness
from loom.models.batch import Combination
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_backend import (
    NEBIUS_BACKEND,
    NEBIUS_LOGICAL_POOL_ID,
    local_execution_enabled,
)
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    TaskExecutionResourceRequestsV1,
    execution_selection_rejections,
    freeze_agent_runtime_releases,
    load_service_execution_runtime_profile,
    validate_task_resource_requests,
)
from loom.verifier_runtime import apply_legacy_verifier_default
from loom_service.metrics import SUBMISSION_REJECTS_TOTAL
from loom_service.worker_backends import (
    get_active_backends,
    get_service_execution_backend_pools,
    runtime_environment,
)


def reject_submission(
    *,
    reason: str,
    status_code: int,
    detail: Any,
) -> NoReturn:
    SUBMISSION_REJECTS_TOTAL.labels(reason=reason).inc()
    raise HTTPException(status_code=status_code, detail=detail)



def reject_unsupported_hosted_backend(backend: str) -> None:
    """Reject any explicit non-Nebius backend outside disposable local execution.

    Never reinterprets the request as Nebius: the caller is told to omit it.
    """
    if backend != NEBIUS_BACKEND and not local_execution_enabled():
        reject_submission(
            reason="unsupported_hosted_backend",
            status_code=400,
            detail={
                "reason": "unsupported_hosted_backend",
                "backend": backend,
                "message": (
                    "Hosted execution supports Nebius only. "
                    "Omit `backend` and resubmit."
                ),
            },
        )



async def freeze_task_resource_requests(
    session: Any,
    *,
    backend: str,
    task_ids: Sequence[str],
    trial_config: dict[str, Any],
    combinations: Sequence[Combination | dict[str, Any]],
    profile: ServiceExecutionRuntimeProfileV1 | None,
    overrides: dict[str, TaskExecutionResourceRequestsV1],
) -> ServiceExecutionRuntimeProfileV1 | None:
    """Freeze selected deployment policy plus explicit overrides at submission.

    The same API resolves browser and CLI submissions. The environment baseline
    applies to newly selected tasks; measured and explicit overrides take priority.
    Every resolved request is frozen against the selected task's current revision.
    """
    if backend != NEBIUS_BACKEND or profile is None:
        if overrides:
            raise HTTPException(status_code=400, detail="task_resource_requests requires native Nebius execution")
        return profile
    if not set(overrides).issubset(task_ids):
        raise HTTPException(status_code=400, detail="task_resource_requests contains an unselected task")
    selections = [
        {**trial_config, "agent_name": item.agent_name, "agent_version": item.agent_version,
         "agent_model": item.agent_model.model_dump(mode="json") if item.agent_model is not None else None}
        for raw in combinations
        for item in (raw if isinstance(raw, Combination) else Combination.model_validate(raw),)
    ] or [trial_config]
    # Measured resource requests are a harness feature, not a topology (#2288).
    requests_supported = all(
        (spec := hosted_harness(item.get("agent_name"))) is not None
        and spec.supports("task_resource_requests")
        for item in selections
    )
    if overrides and not requests_supported:
        raise HTTPException(
            status_code=400,
            detail="task_resource_requests supports only " + ", ".join(harnesses_supporting("task_resource_requests")),
        )
    requests = {
        task_id: entry for task_id, entry in profile.task_resource_requests.items()
        if task_id in task_ids and requests_supported
    }
    requests.update(overrides)
    # Node-share profiles resolve memory from target capacity at admission.
    # The retired fixed template must not reject small source declarations first.
    baseline = (profile.default_task_resource_requests
                if requests_supported and profile.resource_allocation_policy is None else None)
    selected_ids = set(task_ids) if baseline is not None else set(requests)
    if not selected_ids:
        return profile.model_copy(update={"task_resource_requests": {}})
    rows = (await session.execute(
        select(
            Task.id, Task.checksum, Task.config, Task.source_provenance,
            Task.legacy_separate_verifier_checksum,
        ).where(Task.id.in_(list(selected_ids))),
    )).all()
    if {str(row[0]) for row in rows} != selected_ids:
        raise HTTPException(status_code=400, detail="task_resource_requests task is missing")
    try:
        trials = [TrialConfig.model_validate(item) for item in selections]
        for task_id, checksum, raw_task, provenance, legacy_checksum in rows:
            task = TaskConfig.model_validate(raw_task)
            if task.service_execution is not None:
                if str(task_id) in requests:
                    raise ValueError("task_resource_requests requires automatic native execution")
                continue
            revision = "sha256:" + checksum.removeprefix("sha256:")
            if str(task_id) not in requests:
                assert baseline is not None
                requests[str(task_id)] = TaskExecutionResourceRequestsV1(
                    task_revision_sha256=revision, requests=baseline,
                )
            for trial in trials:
                try:
                    validate_task_resource_requests(
                        task=task,
                        trial=apply_legacy_verifier_default(
                            task, trial, task_checksum=checksum,
                            legacy_separate_verifier_checksum=legacy_checksum,
                            source_provenance=provenance or {},
                        ),
                        profile=profile,
                        task_revision_sha256=revision,
                        override=requests[str(task_id)],
                    )
                except ValueError as exc:
                    raise ValueError(f"{task_id}: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return profile.model_copy(update={"task_resource_requests": requests})



def submission_trials(
    trial_config: dict[str, Any], combinations: Sequence[Combination | dict[str, Any]],
) -> tuple[TrialConfig, ...]:
    """One trial per combination (or the bare trial config); raises ``ValidationError``."""
    if not combinations:
        return (TrialConfig.model_validate(trial_config),)
    return tuple(
        TrialConfig.model_validate(
            {
                **trial_config,
                "agent_name": combination.agent_name,
                "agent_version": combination.agent_version,
                "agent_model": (
                    combination.agent_model.model_dump(mode="json")
                    if combination.agent_model is not None
                    else None
                ),
            }
        )
        for raw_combination in combinations
        for combination in (
            raw_combination
            if isinstance(raw_combination, Combination)
            else Combination.model_validate(raw_combination),
        )
    )


async def preview_execution_selection(
    session: Any,
    *,
    task_ids: Sequence[str],
    trial_config: dict[str, Any],
    combinations: Sequence[Combination | dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per task, the axes each submitted trial resolves to; nothing is written."""
    trials = submission_trials(trial_config, combinations)
    rows = (
        await session.execute(
            select(
                Task.id, Task.config, Task.source_provenance, Task.checksum,
                Task.legacy_separate_verifier_checksum,
            ).where(Task.id.in_(list(task_ids))),
        )
    ).all()
    by_id = {str(row[0]): row for row in rows}
    preview: list[dict[str, Any]] = []
    for task_id in task_ids:
        row = by_id.get(task_id)
        if row is None:
            continue
        _, config, provenance, checksum, legacy_checksum = row
        task = TaskConfig.model_validate(config)
        preview.append({
            "task_id": task_id,
            "trials": [
                resolved_execution_selection(
                    task,
                    apply_legacy_verifier_default(
                        task, trial, task_checksum=checksum,
                        legacy_separate_verifier_checksum=legacy_checksum,
                        source_provenance=dict(provenance or {}),
                    ),
                )
                for trial in trials
            ],
        })
    return preview


async def admit_execution_backend(
    session: Any,
    *,
    backend: str,
    task_ids: Sequence[str],
    trial_config: dict[str, Any],
    combinations: Sequence[Combination | dict[str, Any]],
    runtime_profile_json: str,
    resolve_versions: bool = True,
    automatic_only: bool = False,
) -> ServiceExecutionRuntimeProfileV1 | None:
    """Require a native target, or a worker in explicit local development."""
    reject_unsupported_hosted_backend(backend)
    selection_configs = [
        combo.model_dump(mode="json") if isinstance(combo, Combination) else combo
        for combo in combinations
    ] or [trial_config]
    selections = [
        (str(item.get("agent_name", "")), item.get("agent_version"))
        for item in selection_configs
    ]
    if any(version is not None for _, version in selections) and backend != NEBIUS_BACKEND:
        raise HTTPException(status_code=400, detail="agent_version requires the native Nebius backend")
    task_rows = (
        await session.execute(
            select(
                Task.id, Task.config, Task.source_provenance, Task.checksum,
                Task.legacy_separate_verifier_checksum,
            ).where(Task.id.in_(list(task_ids))),
        )
    ).all()
    configs_by_id = {
        str(task_id): (
            TaskConfig.model_validate(config), dict(source_provenance or {}),
            checksum, legacy_checksum,
        )
        for task_id, config, source_provenance, checksum, legacy_checksum in task_rows
    }
    if backend == NEBIUS_BACKEND:
        parsed_trials: tuple[TrialConfig, ...] | None = None
        parsed_trial_error = False
        profile = load_service_execution_runtime_profile(runtime_profile_json)
        if profile is not None and resolve_versions:
            try:
                releases = await resolve_agent_runtimes(session, selections)
                profile = freeze_agent_runtime_releases(profile, releases)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        incompatible_task_ids: list[str] = []
        rejection_reasons: dict[str, list[str]] = {}
        automatic_profile_used = False
        for task_id in task_ids:
            task_entry = configs_by_id.get(task_id)
            task_config = task_entry[0] if task_entry is not None else None
            provenance = task_entry[1] if task_entry is not None else {}
            binding = task_config.service_execution if task_config is not None else None
            reasons: tuple[str, ...] = ()
            if binding is not None and automatic_only:
                raise HTTPException(
                    status_code=400,
                    detail="current runtime rerun requires automatic native execution for every task",
                )
            if binding is not None and any(version is not None for _, version in selections):
                raise HTTPException(status_code=400, detail="agent_version requires automatic native execution")
            if binding is None and task_config is not None:
                automatic_profile_used = True
                if parsed_trials is None:
                    try:
                        parsed_trials = submission_trials(trial_config, combinations)
                    except ValidationError:
                        parsed_trials = ()
                        parsed_trial_error = True
                if parsed_trial_error:
                    reasons = ("automatic_trial_config_invalid",)
                else:
                    assert task_entry is not None
                    effective_trials = tuple(
                        apply_legacy_verifier_default(
                            task_config, parsed_trial, task_checksum=task_entry[2],
                            legacy_separate_verifier_checksum=task_entry[3],
                            source_provenance=provenance,
                        )
                        for parsed_trial in parsed_trials
                    )
                    reasons = tuple(
                        dict.fromkeys(
                            reason
                            for parsed_trial in effective_trials
                            for reason in execution_selection_rejections(
                                task_config,
                                parsed_trial,
                                profile,
                                source_provenance=provenance,
                                allow_task_image_preparation=True,
                            )
                        )
                    )
                if profile is None:
                    reasons = (*reasons, "runtime_profile_unavailable")
            if (
                task_config is None
                or (binding is not None and binding.logical_pool_id != NEBIUS_LOGICAL_POOL_ID)
                or reasons
            ):
                incompatible_task_ids.append(task_id)
                if reasons:
                    rejection_reasons[task_id] = list(dict.fromkeys(reasons))
        if incompatible_task_ids:
            reject_submission(
                reason="nebius_task_incompatible",
                status_code=400,
                detail={
                    "reason": "nebius_task_incompatible",
                    "backend": NEBIUS_BACKEND,
                    "logical_pool_id": NEBIUS_LOGICAL_POOL_ID,
                    "task_ids": incompatible_task_ids,
                    "rejection_reasons": rejection_reasons,
                },
            )
        service_pools = await get_service_execution_backend_pools(session)
        if any(pool.pool_name == NEBIUS_LOGICAL_POOL_ID for pool in service_pools):
            return profile if automatic_profile_used else None
        reject_submission(
            reason="nebius_target_unavailable",
            status_code=400,
            detail=(
                "backend 'nebius' has no fresh healthy active target in "
                f"environment {runtime_environment()!r}"
            ),
        )

    active_backends = await get_active_backends(session)
    if backend in active_backends:
        return None
    available_str = ", ".join(sorted(active_backends)) or "(none — no active workers)"
    reject_submission(
        reason="no_workers", status_code=400,
        detail=(f"no active worker advertises backend {backend!r}. "
                f"Currently available: {available_str}. Start a local worker."),
    )
