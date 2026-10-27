"""One-command delivery bundle export for completed batch families (#390).

Shared bundle contracts and errors are re-exported for existing Python consumers.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from sqlalchemy import select

from loom.auth import AuthContext
from loom.data_lifecycle_registry import (
    bind_existing_batch_lifecycle_authority,
    ensure_artifact_lifecycle_authority,
    register_lifecycle_object,
)
from loom.db.schema import Artifact, Batch, LlmCall, Task, Trial, TrialResourceUsage
from loom.models.trajectory import TrialEndEvent
from loom.resource_usage_store import resource_usage_response, row_to_report
from loom.terminal_result_semantics import (
    explicit_skip_verifier,
    has_numeric_reward,
    is_scored_agent_timeout,
    terminal_result_conflicts,
)
from loom.trajectory.object_identity import (
    TrajectoryObjectFilename,
    resolve_trajectory_object_key,
)
from loom_service.delivery_export_errors import (
    CorruptDeliveryObjectsError as CorruptDeliveryObjectsError,
)
from loom_service.delivery_export_errors import DeliveryExportError as DeliveryExportError
from loom_service.delivery_export_errors import (
    InvalidDeliveryBatchFamilyError as InvalidDeliveryBatchFamilyError,
)
from loom_service.delivery_export_errors import (
    MissingDeliveryObjectsError as MissingDeliveryObjectsError,
)
from loom_service.delivery_export_errors import (
    TerminalStateMismatchError as TerminalStateMismatchError,
)
from loom_service.delivery_export_errors import (
    UnreadableDeliveryObjectsError as UnreadableDeliveryObjectsError,
)
from loom_service.delivery_export_errors import (
    UnresolvedDeliveryTrialsError as UnresolvedDeliveryTrialsError,
)
from loom_service.delivery_export_openhands import (
    build_per_trial_openhands_bundle,
)
from loom_service.delivery_export_tb2_v2 import (
    build_per_trial_v2_bundle,
    parse_trajectory_events,
    resolve_verifier_artifacts,
)
from loom_service.trial_bundles import CanonicalTrialBundle as CanonicalTrialBundle
from loom_service.trial_bundles import CanonicalTrialBundleFile as CanonicalTrialBundleFile
from loom_service.trial_bundles import CanonicalTrialIdentity as CanonicalTrialIdentity
from loom_service.trial_bundles import ObjectRef as ObjectRef
from loom_service.trial_bundles import canonical_bundle_for_trial as canonical_bundle_for_trial
from loom_service.trial_bundles import (
    canonical_bundle_from_artifact as canonical_bundle_from_artifact,
)
from loom_service.trial_bundles import (
    canonical_trial_bundle_manifest as canonical_trial_bundle_manifest,
)
from loom_service.trial_bundles import has_archive_path_traversal

SELECTION_RULE = "highest_priority_deliverable_by_task_sample_combination"
EXPLICIT_TRIAL_IDS_SELECTION_RULE = "explicit_trial_ids"
SCHEMA_VERSION = "1"
TERMINAL_BATCH_STATES = {"finished", "cancelled"}
PAYLOAD_CHECKSUMS_FILE = "checksums/SHA256SUMS"
DEFAULT_ARCHIVE_SPOOL_MAX_BYTES = 64 * 1024 * 1024
DeliveryExportMode = Literal[
    "lightweight",
    "raw-harbor",
    "raw-harbor-tb2-v1",
    "raw-harbor-tb2-v2",
    "openhands-export",
]


@dataclass(frozen=True)
class SelectedTrial:
    trial: Trial
    batch: Batch
    priority: int
    selection_source: str
    trajectory: ObjectRef
    atif: ObjectRef
    reward: float | None

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (
            str(self.trial.task_id),
            int(self.trial.sample_idx),
            int(self.trial.combination_idx),
        )


@dataclass(frozen=True)
class ArchiveBuildResult:
    body: Any
    sha256: str
    size_bytes: int


def _json_bytes(data: Any) -> bytes:
    return (
        json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode()


def _public_json_bytes(data: Any) -> bytes:
    return (json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def _safe_slug(value: str, *, max_len: int = 96) -> str:
    out = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("-")
    slug = "".join(out).strip("-._") or "task"
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug[:max_len].rstrip("-._") or "task"


def _task_archive_relpath(source_key: str, prefix: str) -> str | None:
    rel = source_key[len(prefix) :].lstrip("/")
    if not rel or has_archive_path_traversal(rel):
        return None
    return rel


def _extract_reward(result: dict[str, Any] | None) -> float | None:
    if not isinstance(result, dict):
        return None
    for key in ("aggregate_reward", "reward", "score"):
        raw = result.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


def _reward_key(reward: float | None) -> str:
    if reward is None:
        return "null"
    if reward.is_integer():
        return f"{reward:.1f}"
    return format(reward, ".12g")


def _trial_coordinate(trial: Trial) -> tuple[str, int, int]:
    return (
        str(trial.task_id),
        int(trial.sample_idx),
        int(trial.combination_idx),
    )


def _object_error_code(exc: ClientError) -> str | None:
    code = exc.response.get("Error", {}).get("Code")
    return code if isinstance(code, str) else None


MISSING_OBJECT_ERROR_CODES = {"NoSuchBucket", "NoSuchKey", "404"}
UNREADABLE_OBJECT_ERROR_CODES = {"AccessDenied", "Forbidden", "403"}


def _object_error_detail(
    ref: ObjectRef,
    *,
    operation: str,
    exc: ClientError,
) -> dict[str, str]:
    detail = ref.as_dict()
    detail["operation"] = operation
    detail["error_code"] = _object_error_code(exc) or "unknown"
    return detail


def _parse_s3_uri(uri: str) -> tuple[str, str] | None:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        return None
    key = parsed.path.lstrip("/")
    if not key:
        return None
    return parsed.netloc, key


def _object_ref_for_trial(
    trial: Trial,
    *,
    kind: str,
    trajectories_bucket: str,
) -> ObjectRef:
    index = trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    uri_key = "trajectory_uri" if kind == "trajectory" else "atif_uri"
    filename: TrajectoryObjectFilename = "events.jsonl" if kind == "trajectory" else "atif.json"
    try:
        key = resolve_trajectory_object_key(
            uri=index.get(uri_key),
            expected_bucket=trajectories_bucket,
            team_id=trial.team_id,
            trial_id=trial.id,
            filename=filename,
        )
    except ValueError as exc:
        raise InvalidDeliveryBatchFamilyError(
            {
                "message": str(exc),
                "trial_id": str(trial.id),
                "object_kind": kind,
            }
        ) from exc
    return ObjectRef(
        kind=kind,
        trial_id=trial.id,
        bucket=trajectories_bucket,
        key=key,
    )


def _model_info(batch: Batch) -> tuple[str | None, str | None]:
    config = batch.trial_config if isinstance(batch.trial_config, dict) else {}
    model = config.get("agent_model")
    if isinstance(model, dict):
        provider = model.get("provider")
        name = model.get("name")
        return (
            str(provider) if provider is not None else None,
            str(name) if name is not None else batch.provider_model_id,
        )
    if batch.provider_model_id:
        return None, batch.provider_model_id
    return None, None


async def _load_batch(session: Any, batch_id: UUID) -> Batch:
    batch = (await session.execute(select(Batch).where(Batch.id == batch_id))).scalar_one_or_none()
    if batch is None:
        raise InvalidDeliveryBatchFamilyError({"message": "batch not found"})
    return cast(Batch, batch)


async def _auto_supplemental_batches(session: Any, main_batch_id: UUID) -> list[Batch]:
    seen = {main_batch_id}
    frontier = [main_batch_id]
    out: list[Batch] = []
    while frontier:
        rows = list(
            (
                await session.execute(
                    select(Batch)
                    .where(Batch.rerun_of_batch_id.in_(frontier))
                    .order_by(Batch.created_at.asc(), Batch.id.asc()),
                )
            )
            .scalars()
            .all()
        )
        frontier = []
        for batch in rows:
            if batch.id in seen:
                continue
            seen.add(batch.id)
            frontier.append(batch.id)
            out.append(batch)
    return out


async def _load_batch_family(
    session: Any,
    main_batch_id: UUID,
    supplemental_batch_ids: list[UUID] | None,
) -> tuple[Batch, list[Batch]]:
    main = await _load_batch(session, main_batch_id)
    if supplemental_batch_ids is None:
        supplements = await _auto_supplemental_batches(session, main_batch_id)
    else:
        supplements = []
        for batch_id in supplemental_batch_ids:
            if batch_id == main_batch_id:
                raise InvalidDeliveryBatchFamilyError(
                    {"message": "supplemental_batch_ids must not include the main batch"}
                )
            supplements.append(await _load_batch(session, batch_id))
    for batch in supplements:
        if batch.team_id != main.team_id:
            raise InvalidDeliveryBatchFamilyError(
                {
                    "message": "all supplemental batches must belong to the main batch team",
                    "batch_id": str(batch.id),
                }
            )
    for batch in [main, *supplements]:
        if str(batch.state) not in TERMINAL_BATCH_STATES:
            raise InvalidDeliveryBatchFamilyError(
                {
                    "message": "batch family must be terminal before delivery export",
                    "batch_id": str(batch.id),
                    "state": str(batch.state),
                }
            )
    if supplemental_batch_ids is not None:
        allowed_parents = {main.id}
        for batch in supplements:
            if batch.rerun_of_batch_id not in allowed_parents:
                raise InvalidDeliveryBatchFamilyError(
                    {
                        "message": (
                            "supplemental batches must be linked rerun descendants "
                            "of the main batch in priority order"
                        ),
                        "batch_id": str(batch.id),
                        "rerun_of_batch_id": (
                            str(batch.rerun_of_batch_id)
                            if batch.rerun_of_batch_id is not None
                            else None
                        ),
                    }
                )
            allowed_parents.add(batch.id)
    return main, supplements


async def _trials_for_batches(
    session: Any,
    batch_ids: list[UUID],
) -> dict[UUID, list[Trial]]:
    rows = list(
        (
            await session.execute(
                select(Trial)
                .where(Trial.batch_id.in_(batch_ids))
                .order_by(
                    Trial.task_id.asc(),
                    Trial.sample_idx.asc(),
                    Trial.combination_idx.asc(),
                    Trial.submitted_at.asc(),
                    Trial.id.asc(),
                ),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[Trial]] = {batch_id: [] for batch_id in batch_ids}
    for trial in rows:
        if trial.batch_id is not None:
            out.setdefault(trial.batch_id, []).append(trial)
    return out


async def _tasks_for_selected(
    session: Any,
    selected: list[SelectedTrial],
) -> dict[str, Task]:
    task_ids = sorted({item.trial.task_id for item in selected})
    if not task_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(Task).where(Task.id.in_(task_ids)),
            )
        )
        .scalars()
        .all()
    )
    return {task.id: task for task in rows}


async def _llm_calls_for_selected(
    session: Any,
    selected: list[SelectedTrial],
) -> dict[UUID, list[LlmCall]]:
    trial_ids = [item.trial.id for item in selected]
    if not trial_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(LlmCall)
                .where(LlmCall.trial_id.in_(trial_ids))
                .order_by(LlmCall.trial_id.asc(), LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[LlmCall]] = {trial_id: [] for trial_id in trial_ids}
    for call in rows:
        out.setdefault(call.trial_id, []).append(call)
    return out


async def _resource_usage_for_selected(
    session: Any,
    selected: list[SelectedTrial],
) -> dict[UUID, list[TrialResourceUsage]]:
    trial_ids = [item.trial.id for item in selected]
    if not trial_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(TrialResourceUsage)
                .where(TrialResourceUsage.trial_id.in_(trial_ids))
                .order_by(
                    TrialResourceUsage.trial_id.asc(),
                    TrialResourceUsage.attempt_count.asc(),
                    TrialResourceUsage.container_role.asc(),
                    TrialResourceUsage.role_name.asc(),
                )
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[TrialResourceUsage]] = {trial_id: [] for trial_id in trial_ids}
    for row in rows:
        out.setdefault(row.trial_id, []).append(row)
    return out


async def _canonical_bundles_for_selected(
    session: Any,
    selected: list[SelectedTrial],
) -> dict[UUID, CanonicalTrialBundle]:
    selected_by_id = {item.trial.id: item for item in selected}
    if not selected_by_id:
        return {}
    artifacts = list(
        (
            await session.execute(
                select(Artifact)
                .where(
                    Artifact.trial_id.in_(selected_by_id),
                    Artifact.control_producer_kind == "service_execution",
                )
                .order_by(Artifact.trial_id.asc(), Artifact.created_at.asc()),
            )
        )
        .scalars()
        .all()
    )
    bundles: dict[UUID, CanonicalTrialBundle] = {}
    for artifact in artifacts:
        if artifact.trial_id is None:
            continue
        item = selected_by_id[artifact.trial_id]
        bundle = canonical_bundle_from_artifact(artifact, trial=item.trial)
        if bundle is None:
            continue
        if artifact.trial_id in bundles:
            raise InvalidDeliveryBatchFamilyError(
                {"message": "multiple canonical Trial bundles match one selected attempt"}
            )
        bundles[artifact.trial_id] = bundle
    return bundles


def _select_trials(
    *,
    main: Batch,
    supplements: list[Batch],
    trials_by_batch: dict[UUID, list[Trial]],
    trajectories_bucket: str,
) -> list[SelectedTrial]:
    batches = [main, *supplements]
    priority_by_batch = {batch.id: index for index, batch in enumerate(batches)}
    main_keys = {_trial_coordinate(trial) for trial in trials_by_batch.get(main.id, [])}
    if not main_keys:
        raise UnresolvedDeliveryTrialsError(
            {"message": "main batch has no trials", "unresolved_trials": []}
        )

    selected_by_key: dict[tuple[str, int, int], SelectedTrial] = {}
    unresolved: list[dict[str, Any]] = []
    all_by_key: dict[tuple[str, int, int], list[Trial]] = {key: [] for key in main_keys}
    for batch in batches:
        for trial in trials_by_batch.get(batch.id, []):
            key = _trial_coordinate(trial)
            if key in all_by_key:
                all_by_key[key].append(trial)
            if key not in main_keys or not _is_delivery_eligible(trial):
                continue
            priority = priority_by_batch[batch.id]
            current = selected_by_key.get(key)
            if current is not None and current.priority > priority:
                continue
            if current is not None and current.priority == priority:
                current_submitted = current.trial.submitted_at or datetime.min.replace(tzinfo=UTC)
                trial_submitted = trial.submitted_at or datetime.min.replace(tzinfo=UTC)
                if (trial_submitted, str(trial.id)) <= (current_submitted, str(current.trial.id)):
                    continue
            selected_by_key[key] = _selected_trial_for(
                trial=trial,
                batch=batch,
                priority=priority,
                trajectories_bucket=trajectories_bucket,
            )

    for key in sorted(main_keys):
        if key in selected_by_key:
            continue
        candidates = all_by_key.get(key, [])
        latest = candidates[-1] if candidates else None
        unresolved.append(
            {
                "task_id": key[0],
                "sample_idx": key[1],
                "combination_idx": key[2],
                "latest_trial_id": str(latest.id) if latest is not None else None,
                "latest_state": str(latest.state) if latest is not None else None,
                "latest_failure_reason": latest.failure_reason if latest is not None else None,
            }
        )
    if unresolved:
        raise UnresolvedDeliveryTrialsError(
            {
                "message": "batch family has attempts without complete delivery evidence",
                "unresolved_trials": unresolved,
            }
        )
    selected = [
        selected_by_key[key]
        for key in sorted(selected_by_key, key=lambda item: (item[0], item[1], item[2]))
    ]
    _assert_selected_terminal_consistency(selected)
    return selected


def _is_delivery_eligible(trial: Trial) -> bool:
    return str(trial.state) == "succeeded" or is_scored_agent_timeout(
        state=str(trial.state),
        result=trial.result,
        failure_reason=trial.failure_reason,
    )


def _selected_trial_for(
    *,
    trial: Trial,
    batch: Batch,
    priority: int,
    trajectories_bucket: str,
) -> SelectedTrial:
    return SelectedTrial(
        trial=trial,
        batch=batch,
        priority=priority,
        selection_source="main" if priority == 0 else "supplemental",
        trajectory=_object_ref_for_trial(
            trial,
            kind="trajectory",
            trajectories_bucket=trajectories_bucket,
        ),
        atif=_object_ref_for_trial(
            trial,
            kind="atif",
            trajectories_bucket=trajectories_bucket,
        ),
        reward=_extract_reward(trial.result),
    )


def _assert_selected_terminal_consistency(selected: list[SelectedTrial]) -> None:
    inconsistent: list[dict[str, Any]] = []
    for item in selected:
        trial = item.trial
        conflicts = terminal_result_conflicts(
            state=str(trial.state),
            result=trial.result,
            failure_reason=trial.failure_reason,
            config=trial.config,
        )
        if conflicts:
            inconsistent.append(
                {
                    "trial_id": str(trial.id),
                    "batch_id": str(item.batch.id),
                    "task_id": trial.task_id,
                    "sample_idx": int(trial.sample_idx),
                    "combination_idx": int(trial.combination_idx),
                    "conflicts": conflicts,
                }
            )
    if inconsistent:
        raise TerminalStateMismatchError(
            {
                "message": "selected delivery trials have contradictory terminal semantics",
                "inconsistent_trials": inconsistent,
            }
        )


def _select_trials_by_ids(
    *,
    main: Batch,
    supplements: list[Batch],
    trials_by_batch: dict[UUID, list[Trial]],
    trial_ids: list[UUID],
    trajectories_bucket: str,
) -> tuple[list[SelectedTrial], dict[str, Any]]:
    """Select explicit trial IDs from the authorized batch family.

    Sibling main coordinates need not resolve. Selected trials must still be
    delivery-eligible and terminal-consistent.
    """
    if not trial_ids:
        raise InvalidDeliveryBatchFamilyError(
            {
                "message": "selection.trial_ids must be a non-empty list when provided",
            }
        )
    if len(set(trial_ids)) != len(trial_ids):
        raise InvalidDeliveryBatchFamilyError(
            {
                "message": "selection.trial_ids must not contain duplicates",
            }
        )

    batches = [main, *supplements]
    batch_by_id = {batch.id: batch for batch in batches}
    priority_by_batch = {batch.id: index for index, batch in enumerate(batches)}
    trials_by_id: dict[UUID, Trial] = {}
    for batch in batches:
        for trial in trials_by_batch.get(batch.id, []):
            trials_by_id[trial.id] = trial

    missing = [str(trial_id) for trial_id in trial_ids if trial_id not in trials_by_id]
    if missing:
        raise InvalidDeliveryBatchFamilyError(
            {
                "message": "selected trial ids are not in the authorized batch family",
                "unknown_trial_ids": missing,
            }
        )

    ineligible: list[dict[str, Any]] = []
    selected: list[SelectedTrial] = []
    for trial_id in trial_ids:
        trial = trials_by_id[trial_id]
        batch_id = trial.batch_id
        if batch_id is None or batch_id not in batch_by_id:
            raise InvalidDeliveryBatchFamilyError(
                {
                    "message": "selected trial ids are not in the authorized batch family",
                    "unknown_trial_ids": [str(trial.id)],
                }
            )
        batch = batch_by_id[batch_id]
        if not _is_delivery_eligible(trial):
            ineligible.append(
                {
                    "trial_id": str(trial.id),
                    "batch_id": str(batch.id),
                    "task_id": trial.task_id,
                    "sample_idx": int(trial.sample_idx),
                    "combination_idx": int(trial.combination_idx),
                    "state": str(trial.state),
                    "failure_reason": trial.failure_reason,
                }
            )
            continue
        selected.append(
            _selected_trial_for(
                trial=trial,
                batch=batch,
                priority=priority_by_batch[batch.id],
                trajectories_bucket=trajectories_bucket,
            )
        )
    if ineligible:
        raise UnresolvedDeliveryTrialsError(
            {
                "message": (
                    "explicitly selected trials are not eligible for delivery export"
                ),
                "ineligible_trials": ineligible,
            }
        )
    _assert_selected_terminal_consistency(selected)

    main_keys = {_trial_coordinate(trial) for trial in trials_by_batch.get(main.id, [])}
    selected_keys = {_trial_coordinate(item.trial) for item in selected}
    skipped_coordinates = [
        {
            "task_id": key[0],
            "sample_idx": key[1],
            "combination_idx": key[2],
        }
        for key in sorted(main_keys - selected_keys)
    ]
    selection_meta = {
        "selection_rule": EXPLICIT_TRIAL_IDS_SELECTION_RULE,
        "requested_trial_ids": [str(trial_id) for trial_id in trial_ids],
        "selected_trial_ids": [str(item.trial.id) for item in selected],
        "skipped_coordinates_count": len(skipped_coordinates),
        "skipped_coordinates": skipped_coordinates,
    }
    return selected, selection_meta


def _head_delivery_objects(
    client: Any,
    selected: list[SelectedTrial],
    canonical_bundles: dict[UUID, CanonicalTrialBundle] | None = None,
) -> dict[str, Any]:
    missing: list[dict[str, str]] = []
    unreadable: list[dict[str, str]] = []
    checked = 0
    for item in selected:
        bundle = (canonical_bundles or {}).get(item.trial.id)
        bundle_refs = tuple(file.ref for file in bundle.files) if bundle is not None else ()
        for ref in (item.trajectory, item.atif, *bundle_refs):
            checked += 1
            try:
                client.head_object(Bucket=ref.bucket, Key=ref.key)
            except ClientError as exc:
                code = _object_error_code(exc)
                if code in MISSING_OBJECT_ERROR_CODES:
                    missing.append(ref.as_dict())
                    continue
                if code in UNREADABLE_OBJECT_ERROR_CODES:
                    unreadable.append(
                        _object_error_detail(ref, operation="HeadObject", exc=exc),
                    )
                    continue
                raise
    if missing:
        raise MissingDeliveryObjectsError(
            {
                "message": "delivery bundle cannot be prepared because objects are missing",
                "missing_objects": missing,
            }
        )
    if unreadable:
        raise UnreadableDeliveryObjectsError(
            {
                "message": "delivery bundle cannot be prepared because objects are unreadable",
                "unreadable_objects": unreadable,
            }
        )
    return {"checked": checked, "missing": []}


def _get_object_bytes(client: Any, ref: ObjectRef) -> bytes:
    try:
        obj = client.get_object(Bucket=ref.bucket, Key=ref.key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in MISSING_OBJECT_ERROR_CODES:
            raise MissingDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became missing during archive assembly"
                    ),
                    "missing_objects": [ref.as_dict()],
                }
            ) from exc
        if code in UNREADABLE_OBJECT_ERROR_CODES:
            raise UnreadableDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became unreadable during archive assembly"
                    ),
                    "unreadable_objects": [
                        _object_error_detail(ref, operation="GetObject", exc=exc),
                    ],
                }
            ) from exc
        raise
    body = obj["Body"]
    try:
        return bytes(body.read())
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _get_object_stream(
    client: Any,
    ref: ObjectRef,
) -> tuple[Any, int]:
    try:
        obj = client.get_object(Bucket=ref.bucket, Key=ref.key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in MISSING_OBJECT_ERROR_CODES:
            raise MissingDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became missing during archive assembly"
                    ),
                    "missing_objects": [ref.as_dict()],
                }
            ) from exc
        if code in UNREADABLE_OBJECT_ERROR_CODES:
            raise UnreadableDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became unreadable during archive assembly"
                    ),
                    "unreadable_objects": [
                        _object_error_detail(ref, operation="GetObject", exc=exc),
                    ],
                }
            ) from exc
        raise
    body = obj["Body"]
    raw_size = obj.get("ContentLength")
    try:
        size = int(raw_size)
    except (TypeError, ValueError) as exc:
        close = getattr(body, "close", None)
        if callable(close):
            close()
        raise UnreadableDeliveryObjectsError(
            {
                "message": "delivery object is missing ContentLength for streamed export",
                "unreadable_objects": [
                    {
                        **ref.as_dict(),
                        "operation": "GetObject",
                        "error_code": "MissingContentLength",
                    }
                ],
            }
        ) from exc
    return body, size


def _get_s3_object_stream(
    client: Any,
    *,
    bucket: str,
    key: str,
    kind: str,
) -> tuple[Any, int]:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in MISSING_OBJECT_ERROR_CODES:
            raise MissingDeliveryObjectsError(
                {
                    "message": "raw delivery export object disappeared during archive assembly",
                    "missing_objects": [{"kind": kind, "bucket": bucket, "key": key}],
                }
            ) from exc
        if code in UNREADABLE_OBJECT_ERROR_CODES:
            raise UnreadableDeliveryObjectsError(
                {
                    "message": "raw delivery export object is unreadable",
                    "unreadable_objects": [
                        {
                            "kind": kind,
                            "bucket": bucket,
                            "key": key,
                            "operation": "GetObject",
                            "error_code": code or "unknown",
                        }
                    ],
                }
            ) from exc
        raise
    body = obj["Body"]
    try:
        size = int(obj.get("ContentLength"))
    except (TypeError, ValueError) as exc:
        close = getattr(body, "close", None)
        if callable(close):
            close()
        raise UnreadableDeliveryObjectsError(
            {
                "message": "raw delivery export object is missing ContentLength",
                "unreadable_objects": [
                    {
                        "kind": kind,
                        "bucket": bucket,
                        "key": key,
                        "operation": "GetObject",
                        "error_code": "MissingContentLength",
                    }
                ],
            }
        ) from exc
    return body, size


def _list_s3_prefix_objects(
    client: Any,
    *,
    bucket: str,
    prefix: str,
) -> list[dict[str, Any]]:
    if not prefix:
        return []
    objects_attr = getattr(client, "objects", None)
    if isinstance(objects_attr, dict):
        rows = []
        for (candidate_bucket, key), body in objects_attr.items():
            if candidate_bucket == bucket and isinstance(key, str) and key.startswith(prefix):
                rows.append({"Key": key, "Size": len(body)})
        return sorted(rows, key=lambda row: str(row["Key"]))

    paginator = getattr(client, "get_paginator", None)
    if callable(paginator):
        pages = paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        out: list[dict[str, Any]] = []
        for page in pages:
            for item in page.get("Contents", []) or []:
                key = item.get("Key")
                if isinstance(key, str):
                    out.append({"Key": key, "Size": int(item.get("Size") or 0)})
        return sorted(out, key=lambda row: str(row["Key"]))

    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    out = []
    for item in response.get("Contents", []) or []:
        key = item.get("Key")
        if isinstance(key, str):
            out.append({"Key": key, "Size": int(item.get("Size") or 0)})
    return sorted(out, key=lambda row: str(row["Key"]))


def _ledger_rows(selected: list[SelectedTrial]) -> list[dict[str, Any]]:
    rows = []
    for index, item in enumerate(selected, start=1):
        task_slug = _safe_slug(item.trial.task_id)
        prefix = f"{index:05d}-{task_slug}-{item.trial.id}"
        rows.append(
            {
                "task_id": item.trial.task_id,
                "sample_idx": int(item.trial.sample_idx),
                "combination_idx": int(item.trial.combination_idx),
                "selected_trial_id": str(item.trial.id),
                "selected_batch_id": str(item.batch.id),
                "selection_priority": item.priority,
                "selection_source": item.selection_source,
                "state": item.trial.state,
                "failure_reason": item.trial.failure_reason,
                "reward": item.reward,
                "trajectory_bucket": item.trajectory.bucket,
                "trajectory_key": item.trajectory.key,
                "atif_bucket": item.atif.bucket,
                "atif_key": item.atif.key,
                "trajectory_file": f"trajectories/{prefix}-events.jsonl",
                "atif_file": f"atif/{prefix}-atif.json",
            }
        )
    return rows


def _csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    fieldnames = [
        "task_id",
        "sample_idx",
        "combination_idx",
        "selected_trial_id",
        "selected_batch_id",
        "selection_priority",
        "selection_source",
        "state",
        "failure_reason",
        "reward",
        "trajectory_bucket",
        "trajectory_key",
        "atif_bucket",
        "atif_key",
        "trajectory_file",
        "atif_file",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


def _summary(
    *,
    main: Batch,
    supplements: list[Batch],
    selected: list[SelectedTrial],
    object_validation: dict[str, Any],
    mode: DeliveryExportMode = "lightweight",
    extra_object_counts: dict[str, int] | None = None,
    selection_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider, model = _model_info(main)
    reward_distribution: dict[str, int] = {}
    source_counts = {str(batch.id): 0 for batch in [main, *supplements]}
    for item in selected:
        reward_distribution[_reward_key(item.reward)] = (
            reward_distribution.get(_reward_key(item.reward), 0) + 1
        )
        source_counts[str(item.batch.id)] = source_counts.get(str(item.batch.id), 0) + 1
    object_counts = {
        "atif": len(selected),
        "trajectory": len(selected),
    }
    if extra_object_counts:
        object_counts.update(extra_object_counts)
    selection_rule = (
        str(selection_meta["selection_rule"])
        if selection_meta and selection_meta.get("selection_rule")
        else SELECTION_RULE
    )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "status": "ready",
        "mode": mode,
        "selection_rule": selection_rule,
        "batch_family": {
            "main_batch_id": str(main.id),
            "supplemental_batch_ids": [str(batch.id) for batch in supplements],
        },
        "task_count": len({item.trial.task_id for item in selected}),
        "trial_count": len(selected),
        "source_counts": source_counts,
        "reward_distribution": dict(sorted(reward_distribution.items())),
        "model_provider": provider,
        "model_name": model,
        "object_counts": object_counts,
        "object_validation": object_validation,
        "created_at": datetime.now(UTC).isoformat(),
    }
    if selection_meta is not None:
        summary["selection"] = {
            "rule": selection_rule,
            "requested_trial_ids": list(selection_meta.get("requested_trial_ids") or []),
            "selected_trial_ids": list(selection_meta.get("selected_trial_ids") or []),
            "skipped_coordinates_count": int(
                selection_meta.get("skipped_coordinates_count") or 0
            ),
            "skipped_coordinates": list(selection_meta.get("skipped_coordinates") or []),
        }
    if _is_raw_harbor_mode(mode):
        summary["layout"] = _raw_harbor_layout()
    if _is_tb2_v1_profile(mode):
        summary["export_profile"] = {
            "name": "raw-harbor-tb2",
            "version": "1",
            "source_of_truth": "provider_logs",
            "audit_spine": "loom_trajectory.jsonl",
        }
    elif _is_tb2_v2_profile(mode):
        summary["export_profile"] = {
            "name": "raw-harbor-tb2",
            "version": "2",
            "source_of_truth": "harbor-checkpoint-bridge",
            "audit_spine": "loom_trajectory.jsonl",
            "model_input_trajectory": "model_input_trajectory.json",
            "execution_trajectory": "trajectory.json",
            "terminal_transcript": "terminal_transcript.jsonl",
        }
    elif _is_openhands_export_profile(mode):
        summary["export_profile"] = {
            "name": "openhands-export",
            "version": "1",
            "source_of_truth": "native/openhands_sdk_events.json",
            "audit_spine": "loom_trajectory.jsonl",
            "model_input_trajectory": "model_input_trajectory.json",
            "execution_trajectory": "trajectory.json",
        }
    return summary


class _HashingWriter:
    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.sha256 = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self.sha256.update(data)
        return int(self._wrapped.write(data))

    def flush(self) -> None:
        flush = getattr(self._wrapped, "flush", None)
        if callable(flush):
            flush()


class _HashingReader:
    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.sha256 = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        data = self._wrapped.read(size)
        if data:
            self.sha256.update(data)
        return cast(bytes, data)


def _tar_info(name: str, size: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = size
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o644
    return info


def _add_tar_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> str:
    info = _tar_info(name, len(data))
    tar.addfile(info, io.BytesIO(data))
    return hashlib.sha256(data).hexdigest()


def _add_tar_stream(
    tar: tarfile.TarFile,
    name: str,
    body: Any,
    size: int,
) -> str:
    reader = _HashingReader(body)
    try:
        tar.addfile(_tar_info(name, size), reader)
        return reader.sha256.hexdigest()
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _payload_checksums_from_entries(entries: list[tuple[str, str]]) -> bytes:
    return "".join(
        f"{digest}  {name}\n" for name, digest in sorted(entries, key=lambda item: item[0])
    ).encode()


def _raw_log_for_call(call: LlmCall) -> dict[str, Any] | None:
    extras = call.provider_extras if isinstance(call.provider_extras, dict) else {}
    raw = extras.get("_loom_raw_provider_log")
    if isinstance(raw, dict):
        return raw
    return None


def _message_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                parts.append(part["text"])
            elif isinstance(part, str):
                parts.append(part)
        if parts:
            return "".join(parts)
    if content is None:
        return ""
    try:
        return json.dumps(content, ensure_ascii=False)
    except TypeError:
        return str(content)


def _first_or_last_raw_message(
    raw_log: dict[str, Any],
    *,
    role: str,
    last: bool = False,
) -> dict[str, Any] | None:
    request = raw_log.get("request")
    if not isinstance(request, dict):
        return None
    body = request.get("body")
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
        return None
    candidates = [
        item for item in body["messages"] if isinstance(item, dict) and item.get("role") == role
    ]
    if not candidates:
        return None
    return candidates[-1] if last else candidates[0]


def _assistant_message_from_raw_log(raw_log: dict[str, Any]) -> dict[str, Any] | None:
    response = raw_log.get("response")
    if not isinstance(response, dict):
        return None
    body = response.get("body")
    if not isinstance(body, dict):
        return None
    choices = body.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if isinstance(message, dict) and isinstance(message.get("role"), str):
                msg = {"role": message["role"]}
                if "content" in message:
                    msg["content"] = message["content"]
                if "reasoning_content" in message:
                    msg["reasoning_content"] = message["reasoning_content"]
                if "tool_calls" in message:
                    msg["tool_calls"] = message["tool_calls"]
                return msg
    content = body.get("content")
    if isinstance(content, list):
        text = "".join(
            str(part.get("text"))
            for part in content
            if isinstance(part, dict) and isinstance(part.get("text"), str)
        )
        if text:
            return {"role": "assistant", "content": text}
    return None


def _normalize_tb2_command(command: Any) -> dict[str, Any] | None:
    if not isinstance(command, dict):
        return None
    keystrokes = command.get("keystrokes")
    if not isinstance(keystrokes, str):
        cmd = command.get("cmd") or command.get("command")
        if isinstance(cmd, str):
            keystrokes = cmd
    out: dict[str, Any] = {}
    if isinstance(keystrokes, str):
        out["keystrokes"] = keystrokes
    duration = command.get("duration")
    if duration is None:
        duration = command.get("timeout_sec")
    if duration is not None:
        out["duration"] = duration
    return out or None


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_tb2_action_payload(payload: Any) -> dict[str, Any] | None:
    data = _parse_json_object(payload)
    if data is None:
        return None
    has_tb2 = any(key in data for key in ("analysis", "plan", "duration", "task_complete"))
    has_loom = any(
        key in data
        for key in (
            "state_analysis",
            "explanation",
            "timeout_sec",
            "is_task_complete",
        )
    )
    commands_raw = data.get("commands")
    has_commands = isinstance(commands_raw, list)
    if not has_tb2 and not has_loom and not has_commands:
        return None

    commands: list[dict[str, Any]] = []
    if isinstance(commands_raw, list):
        for command in commands_raw:
            normalized = _normalize_tb2_command(command)
            if normalized is not None:
                commands.append(normalized)

    raw_task_complete = (
        data.get("task_complete")
        if "task_complete" in data
        else data.get("is_task_complete", False)
    )
    normalized_action: dict[str, Any] = {
        "analysis": data.get("analysis") or data.get("state_analysis") or "",
        "plan": data.get("plan") or data.get("explanation") or "",
        "commands": commands,
        "task_complete": bool(raw_task_complete),
    }
    return normalized_action


def _normalize_tb2_assistant_content(content: Any) -> Any:
    normalized = _normalize_tb2_action_payload(content)
    if normalized is None:
        return content
    return json.dumps(normalized, ensure_ascii=False)


def _messages_from_raw_log(
    raw_log: dict[str, Any],
    *,
    normalize_tb2: bool = False,
) -> list[dict[str, Any]]:
    request = raw_log.get("request")
    messages: list[dict[str, Any]] = []
    if isinstance(request, dict):
        body = request.get("body")
        if isinstance(body, dict) and isinstance(body.get("messages"), list):
            for item in body["messages"]:
                if isinstance(item, dict) and isinstance(item.get("role"), str):
                    msg: dict[str, Any] = {"role": item["role"]}
                    if "content" in item:
                        content = item["content"]
                        msg["content"] = (
                            _normalize_tb2_assistant_content(content)
                            if normalize_tb2 and item["role"] == "assistant"
                            else content
                        )
                    messages.append(msg)
    assistant = _assistant_message_from_raw_log(raw_log)
    if assistant is not None:
        msg = {"role": assistant["role"]}
        if "content" in assistant:
            content = assistant["content"]
            msg["content"] = _normalize_tb2_assistant_content(content) if normalize_tb2 else content
        if isinstance(assistant.get("reasoning_content"), str):
            msg["reasoning_content"] = assistant["reasoning_content"]
        messages.append(msg)
    return messages


def _call_timestamp(call: LlmCall) -> str | None:
    captured_at = getattr(call, "captured_at", None)
    if isinstance(captured_at, datetime):
        return captured_at.isoformat()
    return None


def _agent_name_for_trial(trial: Trial) -> str:
    config = trial.config if isinstance(trial.config, dict) else {}
    raw = config.get("agent_name")
    return str(raw) if raw else "unknown"


def _model_name_for_trial(item: SelectedTrial, calls: list[LlmCall]) -> str | None:
    for call in calls:
        if call.model:
            return call.model
    if item.trial.provider_model_id:
        return item.trial.provider_model_id
    if item.batch.provider_model_id:
        return item.batch.provider_model_id
    return None


def _tb2_tool_calls(action: dict[str, Any], *, call_index: int) -> list[dict[str, Any]]:
    commands = action.get("commands")
    if not isinstance(commands, list):
        return []
    out: list[dict[str, Any]] = []
    for command_index, command in enumerate(commands, start=1):
        if not isinstance(command, dict):
            continue
        out.append(
            {
                "tool_call_id": f"call-{call_index}-{command_index}",
                "function_name": "bash_command",
                "arguments": command,
            }
        )
    return out


def _tb2_agent_message(action: dict[str, Any]) -> str:
    analysis = str(action.get("analysis") or "")
    plan = str(action.get("plan") or "")
    return f"Analysis: {analysis}\nPlan: {plan}"


def _tb2_observation_after_call(
    calls: list[LlmCall],
    *,
    current_index: int,
) -> dict[str, list[dict[str, str]]]:
    next_index = current_index + 1
    if next_index >= len(calls):
        return {"results": []}
    raw_log = _raw_log_for_call(calls[next_index])
    if raw_log is None:
        return {"results": []}
    message = _first_or_last_raw_message(raw_log, role="user", last=True)
    if message is None:
        return {"results": []}
    content = _message_content_text(message.get("content"))
    if not content:
        return {"results": []}
    return {"results": [{"content": content}]}


def _raw_harbor_trajectory(item: SelectedTrial, calls: list[LlmCall]) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    for zero_index, call in enumerate(calls):
        call_index = zero_index + 1
        raw_log = _raw_log_for_call(call)
        if raw_log is None:
            continue
        user_message = _first_or_last_raw_message(raw_log, role="user", last=True)
        timestamp = _call_timestamp(call)
        if user_message is not None:
            user_step: dict[str, Any] = {
                "step_id": f"{call.step_id}:user",
                "source": "user",
                "message": _message_content_text(user_message.get("content")),
            }
            if timestamp is not None:
                user_step["timestamp"] = timestamp
            steps.append(user_step)

        assistant = _assistant_message_from_raw_log(raw_log)
        if assistant is None:
            continue
        content = assistant.get("content")
        action = _normalize_tb2_action_payload(content)
        agent_step: dict[str, Any] = {
            "step_id": f"{call.step_id}:assistant",
            "source": "agent",
            "model_name": call.model,
            "message": (
                _tb2_agent_message(action) if action is not None else _message_content_text(content)
            ),
            "observation": _tb2_observation_after_call(
                calls,
                current_index=zero_index,
            ),
            "metrics": {
                "input_tokens": int(call.input_tokens or 0),
                "output_tokens": int(call.output_tokens or 0),
                "cost_usd": float(call.cost_usd or 0),
                "rate_card_hash": call.rate_card_hash,
            },
        }
        if timestamp is not None:
            agent_step["timestamp"] = timestamp
        if isinstance(assistant.get("reasoning_content"), str):
            agent_step["reasoning_content"] = assistant["reasoning_content"]
        if action is not None:
            tool_calls = _tb2_tool_calls(action, call_index=call_index)
            if tool_calls:
                agent_step["tool_calls"] = tool_calls
        elif isinstance(assistant.get("tool_calls"), list):
            agent_step["tool_calls"] = assistant["tool_calls"]
        steps.append(agent_step)

    return {
        "schema_version": "ATIF-v1.7",
        "session_id": str(item.trial.id),
        "agent": {
            "name": _agent_name_for_trial(item.trial),
            "model_name": _model_name_for_trial(item, calls),
            "version": None,
            "extra": {},
        },
        "steps": steps,
        "final_metrics": _raw_metrics(item, calls),
    }


def _raw_harbor_layout() -> dict[str, Any]:
    return {
        "top_level_manifests": ["manifest.json", "summary.json"],
        "task_bundles": "task_bundles/<task_id>/...",
        "agent_runs": "agent_runs/<task_id>/<trial_id>/...",
        "derived": "derived/sft_messages.jsonl",
    }


def _raw_harbor_object_counts(
    *,
    client: Any,
    tasks_by_id: dict[str, Task],
    llm_calls_by_trial: dict[UUID, list[LlmCall]],
) -> dict[str, int]:
    provider_logs = 0
    for calls in llm_calls_by_trial.values():
        provider_logs += sum(1 for call in calls if _raw_log_for_call(call) is not None)

    task_bundle_files = 0
    for task in tasks_by_id.values():
        parsed = _parse_s3_uri(task.source or "")
        if parsed is None:
            continue
        bucket, prefix = parsed
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"
        for obj in _list_s3_prefix_objects(client, bucket=bucket, prefix=prefix):
            key = str(obj["Key"])
            if _task_archive_relpath(key, prefix) is not None:
                task_bundle_files += 1
    return {
        "provider_logs": provider_logs,
        "task_bundle_files": task_bundle_files,
    }


def _raw_execution_result(item: SelectedTrial) -> dict[str, Any]:
    trial = item.trial
    return {
        "schema_version": "1",
        "trial_id": str(trial.id),
        "task_id": trial.task_id,
        "batch_id": str(item.batch.id),
        "state": trial.state,
        "failure_reason": trial.failure_reason,
        "failure_message": trial.failure_message,
        "reward": item.reward,
        "result": trial.result,
        "started_at": trial.started_at.isoformat() if trial.started_at else None,
        "finished_at": trial.finished_at.isoformat() if trial.finished_at else None,
    }


def _raw_metrics(item: SelectedTrial, calls: list[LlmCall]) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "trial_id": str(item.trial.id),
        "task_id": item.trial.task_id,
        "reward": item.reward,
        "llm_calls_count": len(calls),
        "total_prompt_tokens": sum(int(call.input_tokens or 0) for call in calls),
        "total_completion_tokens": sum(int(call.output_tokens or 0) for call in calls),
        "selection_source": item.selection_source,
        "selection_priority": item.priority,
    }


def _raw_verifier_output(item: SelectedTrial) -> dict[str, Any]:
    result = item.trial.result if isinstance(item.trial.result, dict) else {}
    config = item.trial.config if isinstance(item.trial.config, dict) else {}
    return {
        "schema_version": "1",
        "trial_id": str(item.trial.id),
        "task_id": item.trial.task_id,
        "reward": item.reward,
        "skip_verifier": config.get("skip_verifier") is True,
        "verifier_output": result.get("verifier_output") or result,
    }


def _raw_agent_native_note(item: SelectedTrial) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "trial_id": str(item.trial.id),
        "status": "documented_equivalent",
        "path": "loom_trajectory.jsonl",
        "description": (
            "Loom stores the persisted agent-native event stream as typed "
            "trajectory JSONL. Raw Harbor exports keep that stream as audit "
            "evidence and reconstruct trajectory.json from provider logs."
        ),
    }


def _is_raw_harbor_mode(mode: DeliveryExportMode) -> bool:
    return mode in {
        "raw-harbor",
        "raw-harbor-tb2-v1",
        "raw-harbor-tb2-v2",
        "openhands-export",
    }


def _is_tb2_v1_profile(mode: DeliveryExportMode) -> bool:
    return mode == "raw-harbor-tb2-v1"


def _is_tb2_v2_profile(mode: DeliveryExportMode) -> bool:
    return mode == "raw-harbor-tb2-v2"


def _is_openhands_export_profile(mode: DeliveryExportMode) -> bool:
    return mode == "openhands-export"


def _is_tb2_profile(mode: DeliveryExportMode) -> bool:
    return mode in {"raw-harbor-tb2-v1", "raw-harbor-tb2-v2"}


def _raw_provider_log_path(item: SelectedTrial, index: int) -> str:
    return f"provider_logs/{item.trial.task_id}/{item.trial.id}/{index:05d}.json"


def _raw_agent_run_path(item: SelectedTrial, filename: str) -> str:
    return f"agent_runs/{item.trial.task_id}/{item.trial.id}/{filename}"


def _build_archive(
    *,
    client: Any,
    manifest: dict[str, Any],
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    selected: list[SelectedTrial],
    mode: DeliveryExportMode,
    trajectory_events_by_trial: dict[UUID, list[Any]] | None = None,
    tasks_by_id: dict[str, Task] | None = None,
    llm_calls_by_trial: dict[UUID, list[LlmCall]] | None = None,
    resource_usage_by_trial: dict[UUID, list[TrialResourceUsage]] | None = None,
    canonical_bundles: dict[UUID, CanonicalTrialBundle] | None = None,
    artifacts_bucket: str = "artifacts",
    spool_max_bytes: int = DEFAULT_ARCHIVE_SPOOL_MAX_BYTES,
) -> ArchiveBuildResult:
    spool = tempfile.SpooledTemporaryFile(max_size=spool_max_bytes, mode="w+b")
    hashing_spool = _HashingWriter(spool)
    checksums: list[tuple[str, str]] = []

    def add_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> None:
        checksums.append((name, _add_tar_bytes(tar, name, data)))

    def add_stream(tar: tarfile.TarFile, name: str, body: Any, size: int) -> str:
        digest = _add_tar_stream(tar, name, body, size)
        checksums.append((name, digest))
        return digest

    def add_ref(tar: tarfile.TarFile, name: str, ref: ObjectRef) -> None:
        body, size = _get_object_stream(client, ref)
        add_stream(tar, name, body, size)

    def add_bundle_file(
        tar: tarfile.TarFile,
        name: str,
        file: CanonicalTrialBundleFile,
    ) -> None:
        body, size = _get_object_stream(client, file.ref)
        if size != file.size_bytes:
            close = getattr(body, "close", None)
            if callable(close):
                close()
            raise CorruptDeliveryObjectsError(
                {
                    "message": "canonical Trial bundle size changed before export",
                    "object": file.ref.as_dict(),
                }
            )
        digest = _add_tar_stream(tar, name, body, size)
        if digest != file.sha256.removeprefix("sha256:"):
            raise CorruptDeliveryObjectsError(
                {
                    "message": "canonical Trial bundle digest changed before export",
                    "object": file.ref.as_dict(),
                }
            )
        checksums.append((name, digest))

    with gzip.GzipFile(fileobj=hashing_spool, mode="wb", mtime=0, filename="") as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            add_bytes(tar, "manifest.json", _public_json_bytes(manifest))
            add_bytes(tar, "summary.json", _public_json_bytes(summary))
            add_bytes(tar, "ledger/trials.jsonl", b"".join(_json_bytes(row) for row in rows))
            add_bytes(tar, "ledger/trials.csv", _csv_bytes(rows))
            usage_rows = resource_usage_by_trial or {}
            add_bytes(
                tar,
                "ledger/resource_usage.jsonl",
                b"".join(
                    _json_bytes(row_to_report(row).model_dump(mode="json"))
                    for item in selected
                    for row in usage_rows.get(item.trial.id, [])
                ),
            )
            for row, item in zip(rows, selected, strict=True):
                add_ref(tar, str(row["trajectory_file"]), item.trajectory)
                add_ref(tar, str(row["atif_file"]), item.atif)
                bundle = (canonical_bundles or {}).get(item.trial.id)
                if bundle is None:
                    continue
                bundle_prefix = (
                    f"trial_bundles/{_safe_slug(bundle.task_id)}/{bundle.trial_id}"
                )
                add_bytes(
                    tar,
                    f"{bundle_prefix}/bundle.json",
                    _public_json_bytes(
                        {
                            "schema_version": "loom.canonical-trial-bundle-export.v1",
                            "artifact_id": str(bundle.artifact_id),
                            "trial_id": str(bundle.trial_id),
                            "task_id": bundle.task_id,
                            "attempt": bundle.attempt,
                            "manifest_sha256": bundle.manifest_sha256,
                            "content_sha256": bundle.content_sha256,
                            "files": [
                                {
                                    "relative_path": file.relative_path,
                                    "size_bytes": file.size_bytes,
                                    "sha256": file.sha256,
                                    "media_type": file.media_type,
                                }
                                for file in bundle.files
                            ],
                        }
                    ),
                )
                for file in bundle.files:
                    add_bundle_file(
                        tar,
                        f"{bundle_prefix}/{file.relative_path}",
                        file,
                    )
            if _is_raw_harbor_mode(mode):
                _add_raw_harbor_entries(
                    tar=tar,
                    client=client,
                    add_bytes=add_bytes,
                    add_ref=add_ref,
                    add_stream=add_stream,
                    selected=selected,
                    trajectory_events_by_trial=trajectory_events_by_trial or {},
                    tasks_by_id=tasks_by_id or {},
                    llm_calls_by_trial=llm_calls_by_trial or {},
                    resource_usage_by_trial=usage_rows,
                    mode=mode,
                    artifacts_bucket=artifacts_bucket,
                )
            add_bytes(tar, PAYLOAD_CHECKSUMS_FILE, _payload_checksums_from_entries(checksums))
    size_bytes = int(spool.tell())
    spool.seek(0)
    return ArchiveBuildResult(
        body=spool,
        sha256=hashing_spool.sha256.hexdigest(),
        size_bytes=size_bytes,
    )


def build_canonical_trial_bundle_archive(
    *,
    client: Any,
    bundle: CanonicalTrialBundle,
    spool_max_bytes: int = DEFAULT_ARCHIVE_SPOOL_MAX_BYTES,
) -> ArchiveBuildResult:
    spool = tempfile.SpooledTemporaryFile(max_size=spool_max_bytes, mode="w+b")
    hashing_spool = _HashingWriter(spool)
    checksums: list[tuple[str, str]] = []

    try:
        with gzip.GzipFile(fileobj=hashing_spool, mode="wb", mtime=0, filename="") as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                manifest_bytes = _public_json_bytes(canonical_trial_bundle_manifest(bundle))
                checksums.append(
                    ("bundle.json", _add_tar_bytes(tar, "bundle.json", manifest_bytes))
                )
                for file in bundle.files:
                    body, size = _get_object_stream(client, file.ref)
                    if size != file.size_bytes:
                        close = getattr(body, "close", None)
                        if callable(close):
                            close()
                        raise CorruptDeliveryObjectsError(
                            {
                                "message": "canonical Trial bundle size changed before download",
                                "object": file.ref.as_dict(),
                            }
                        )
                    digest = _add_tar_stream(tar, file.relative_path, body, size)
                    if digest != file.sha256.removeprefix("sha256:"):
                        raise CorruptDeliveryObjectsError(
                            {
                                "message": "canonical Trial bundle digest changed before download",
                                "object": file.ref.as_dict(),
                            }
                        )
                    checksums.append((file.relative_path, digest))
                _add_tar_bytes(
                    tar,
                    PAYLOAD_CHECKSUMS_FILE,
                    _payload_checksums_from_entries(checksums),
                )
    except Exception:
        spool.close()
        raise
    size_bytes = int(spool.tell())
    spool.seek(0)
    return ArchiveBuildResult(
        body=spool,
        sha256=hashing_spool.sha256.hexdigest(),
        size_bytes=size_bytes,
    )


def _load_trajectory_events(client: Any, ref: ObjectRef) -> list[Any]:
    body, _size = _get_object_stream(client, ref)
    try:
        return parse_trajectory_events(body)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _validate_typed_terminal_evidence(
    *,
    item: SelectedTrial,
    events: list[Any],
) -> None:
    terminal_events = [event for event in events if isinstance(event, TrialEndEvent)]
    conflicts: list[dict[str, Any]] = []
    if len(terminal_events) != 1:
        conflicts.append(
            {
                "field": "trajectory.trial_end_count",
                "expected": 1,
                "actual": len(terminal_events),
            }
        )
    else:
        terminal = terminal_events[0]
        if terminal.final_state != str(item.trial.state):
            conflicts.append(
                {
                    "field": "trajectory.trial_end.final_state",
                    "expected": str(item.trial.state),
                    "actual": terminal.final_state,
                }
            )
        expected_failure = getattr(item.trial, "failure_reason", None)
        if terminal.failure_reason != expected_failure:
            conflicts.append(
                {
                    "field": "trajectory.trial_end.failure_reason",
                    "expected": expected_failure,
                    "actual": terminal.failure_reason,
                }
            )
        if not has_numeric_reward(terminal.reward) and not (
            str(item.trial.state) == "succeeded" and explicit_skip_verifier(item.trial.config)
        ):
            conflicts.append(
                {
                    "field": "trajectory.trial_end.reward",
                    "expected": "numeric reward or config.skip_verifier=true",
                    "actual": terminal.reward,
                }
            )
        result = item.trial.result
        if isinstance(result, dict) and "reward" in result and result["reward"] != terminal.reward:
            conflicts.append(
                {
                    "field": "trajectory.trial_end.reward",
                    "expected": result["reward"],
                    "actual": terminal.reward,
                }
            )
    if conflicts:
        raise TerminalStateMismatchError(
            {
                "message": "typed trajectory terminal evidence disagrees with the trial row",
                "inconsistent_trials": [
                    {
                        "trial_id": str(item.trial.id),
                        "batch_id": str(item.batch.id),
                        "task_id": item.trial.task_id,
                        "conflicts": conflicts,
                    }
                ],
            }
        )


def _validate_atif_terminal_evidence(*, client: Any, item: SelectedTrial) -> None:
    body = _get_object_bytes(client, item.atif)
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TerminalStateMismatchError(
            {
                "message": "ATIF terminal evidence is not valid JSON",
                "inconsistent_trials": [
                    {
                        "trial_id": str(item.trial.id),
                        "batch_id": str(item.batch.id),
                        "task_id": item.trial.task_id,
                        "conflicts": [
                            {
                                "field": "atif",
                                "expected": "JSON object",
                                "actual": "invalid_json",
                            }
                        ],
                    }
                ],
            }
        ) from exc
    metadata = document.get("metadata") if isinstance(document, dict) else None
    conflicts: list[dict[str, Any]] = []
    if not isinstance(metadata, dict):
        conflicts.append(
            {"field": "atif.metadata", "expected": "object", "actual": type(metadata).__name__}
        )
    else:
        final_state = metadata.get("final_state")
        if final_state != str(item.trial.state):
            conflicts.append(
                {
                    "field": "atif.metadata.final_state",
                    "expected": str(item.trial.state),
                    "actual": final_state,
                }
            )
        error = metadata.get("error")
        failure_reason = error.get("failure_reason") if isinstance(error, dict) else None
        expected_failure = getattr(item.trial, "failure_reason", None)
        if failure_reason != expected_failure:
            conflicts.append(
                {
                    "field": "atif.metadata.error.failure_reason",
                    "expected": expected_failure,
                    "actual": failure_reason,
                }
            )
        reward = metadata.get("reward")
        if not has_numeric_reward(reward) and not (
            str(item.trial.state) == "succeeded" and explicit_skip_verifier(item.trial.config)
        ):
            conflicts.append(
                {
                    "field": "atif.metadata.reward",
                    "expected": "numeric reward or config.skip_verifier=true",
                    "actual": reward,
                }
            )
        result = item.trial.result
        if isinstance(result, dict) and "reward" in result and result["reward"] != reward:
            conflicts.append(
                {
                    "field": "atif.metadata.reward",
                    "expected": result["reward"],
                    "actual": reward,
                }
            )
    if conflicts:
        raise TerminalStateMismatchError(
            {
                "message": "ATIF terminal evidence disagrees with the trial row",
                "inconsistent_trials": [
                    {
                        "trial_id": str(item.trial.id),
                        "batch_id": str(item.batch.id),
                        "task_id": item.trial.task_id,
                        "conflicts": conflicts,
                    }
                ],
            }
        )


def _validate_selected_terminal_evidence(
    *,
    client: Any,
    selected: list[SelectedTrial],
) -> dict[UUID, list[Any]]:
    events_by_trial: dict[UUID, list[Any]] = {}
    for item in selected:
        events = _load_trajectory_events(client, item.trajectory)
        _validate_typed_terminal_evidence(item=item, events=events)
        _validate_atif_terminal_evidence(client=client, item=item)
        events_by_trial[item.trial.id] = events
    return events_by_trial


def _add_raw_harbor_entries(
    *,
    tar: tarfile.TarFile,
    client: Any,
    add_bytes: Any,
    add_ref: Any,
    add_stream: Any,
    selected: list[SelectedTrial],
    trajectory_events_by_trial: dict[UUID, list[Any]],
    tasks_by_id: dict[str, Task],
    llm_calls_by_trial: dict[UUID, list[LlmCall]],
    resource_usage_by_trial: dict[UUID, list[TrialResourceUsage]],
    mode: DeliveryExportMode,
    artifacts_bucket: str,
) -> None:
    tb2_v1_profile = _is_tb2_v1_profile(mode)
    tb2_v2_profile = _is_tb2_v2_profile(mode)
    openhands_profile = _is_openhands_export_profile(mode)
    tb2_profile = _is_tb2_profile(mode)
    provider_logs: list[dict[str, Any]] = []
    sft_rows: list[dict[str, Any]] = []
    task_bundle_files: list[dict[str, Any]] = []

    for item in selected:
        calls = llm_calls_by_trial.get(item.trial.id, [])
        trial_provider_logs: list[dict[str, Any]] = []
        for index, call in enumerate(calls, start=1):
            raw_log = _raw_log_for_call(call)
            if raw_log is None:
                continue
            path = _raw_provider_log_path(item, index)
            add_bytes(tar, path, _public_json_bytes(raw_log))
            manifest_row = {
                "archive_path": path,
                "trial_id": str(item.trial.id),
                "task_id": item.trial.task_id,
                "llm_call_id": str(call.id),
                "dialect": call.dialect,
                "model": call.model,
                "ref": raw_log.get("ref"),
            }
            provider_logs.append(manifest_row)
            trial_provider_logs.append(manifest_row)
            if not tb2_v2_profile and not openhands_profile:
                messages = _messages_from_raw_log(raw_log, normalize_tb2=tb2_profile)
                if messages:
                    sft_rows.append(
                        {
                            "trial_id": str(item.trial.id),
                            "task_id": item.trial.task_id,
                            "llm_call_id": str(call.id),
                            "reward": item.reward,
                            "reward_positive": (
                                bool(item.reward > 0) if item.reward is not None else None
                            ),
                            "source": "provider_logs",
                            "selection_source": item.selection_source,
                            "messages": messages,
                        }
                    )

        add_bytes(
            tar,
            _raw_agent_run_path(item, "execution_result.json"),
            _public_json_bytes(_raw_execution_result(item)),
        )
        add_bytes(
            tar,
            _raw_agent_run_path(item, "metrics.json"),
            _public_json_bytes(_raw_metrics(item, calls)),
        )
        add_bytes(
            tar,
            _raw_agent_run_path(item, "resource_usage.json"),
            _public_json_bytes(
                resource_usage_response(resource_usage_by_trial.get(item.trial.id, [])),
            ),
        )
        add_bytes(
            tar,
            _raw_agent_run_path(item, "verifier_output.json"),
            _public_json_bytes(_raw_verifier_output(item)),
        )

        trajectory_artifacts: list[dict[str, str]]
        if openhands_profile:
            events = trajectory_events_by_trial[item.trial.id]
            openhands_bundle = build_per_trial_openhands_bundle(
                trial=item.trial,
                events=events,
                calls=calls,
                client=client,
                artifacts_bucket=artifacts_bucket,
                messages_from_raw_log=_messages_from_raw_log,
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "trajectory.json"),
                _public_json_bytes(openhands_bundle.execution_trajectory),
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "model_input_trajectory.json"),
                _public_json_bytes(openhands_bundle.model_input_trajectory),
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "export_provenance.json"),
                _public_json_bytes(openhands_bundle.export_provenance),
            )
            add_ref(
                tar,
                _raw_agent_run_path(item, "loom_trajectory.jsonl"),
                item.trajectory,
            )
            for archive_path, data in openhands_bundle.native_artifacts.items():
                add_bytes(
                    tar,
                    _raw_agent_run_path(item, archive_path),
                    data,
                )
            trajectory_artifacts = list(openhands_bundle.artifact_manifest_entries)
        elif tb2_v2_profile:
            events = trajectory_events_by_trial[item.trial.id]
            tb2_v2_bundle = build_per_trial_v2_bundle(
                trial=item.trial,
                batch=item.batch,
                events=events,
                calls=calls,
                client=client,
                artifacts_bucket=artifacts_bucket,
                messages_from_raw_log=_messages_from_raw_log,
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "trajectory.json"),
                _public_json_bytes(tb2_v2_bundle.execution_trajectory),
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "model_input_trajectory.json"),
                _public_json_bytes(tb2_v2_bundle.model_input_trajectory),
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "terminal_transcript.jsonl"),
                tb2_v2_bundle.terminal_transcript,
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "export_provenance.json"),
                _public_json_bytes(tb2_v2_bundle.export_provenance),
            )
            add_ref(
                tar,
                _raw_agent_run_path(item, "loom_trajectory.jsonl"),
                item.trajectory,
            )
            for archive_path, data in tb2_v2_bundle.native_artifacts.items():
                add_bytes(
                    tar,
                    _raw_agent_run_path(item, archive_path),
                    data,
                )
            trajectory_artifacts = list(tb2_v2_bundle.artifact_manifest_entries)
        elif tb2_v1_profile:
            add_bytes(
                tar,
                _raw_agent_run_path(item, "trajectory.json"),
                _public_json_bytes(_raw_harbor_trajectory(item, calls)),
            )
            add_bytes(
                tar,
                _raw_agent_run_path(item, "agent_native_trajectory.json"),
                _public_json_bytes(_raw_agent_native_note(item)),
            )
            add_ref(tar, _raw_agent_run_path(item, "loom_trajectory.jsonl"), item.trajectory)
            trajectory_artifacts = [
                {"kind": "trajectory", "path": "trajectory.json"},
                {"kind": "agent_native_trajectory", "path": "loom_trajectory.jsonl"},
            ]
        else:
            add_ref(tar, _raw_agent_run_path(item, "trajectory.jsonl"), item.trajectory)
            trajectory_artifacts = [
                {"kind": "agent_native_trajectory", "path": "trajectory.jsonl"},
            ]

        add_ref(tar, _raw_agent_run_path(item, "atif.json"), item.atif)

        verifier_artifacts = resolve_verifier_artifacts(
            item.trial,
            client=client,
            artifacts_bucket=artifacts_bucket,
        )
        verifier_manifest_entries: list[dict[str, Any]] = []
        for artifact in verifier_artifacts:
            add_bytes(
                tar,
                _raw_agent_run_path(item, artifact.archive_path),
                artifact.data,
            )
            verifier_manifest_entries.append(
                {
                    "kind": "verifier_artifact",
                    "path": artifact.archive_path,
                    "content_hash": artifact.content_hash,
                    "size_bytes": artifact.size_bytes,
                    "truncated": artifact.truncated,
                    "share_status": artifact.share_status,
                    "blocked_reason": artifact.blocked_reason,
                    "step_name": artifact.step_name,
                    "source_key": artifact.source_key,
                }
            )

        artifact_manifest = {
            "schema_version": "1",
            "trial_id": str(item.trial.id),
            "task_id": item.trial.task_id,
            "artifacts": [
                {"kind": "execution_result", "path": "execution_result.json"},
                {"kind": "metrics", "path": "metrics.json"},
                {"kind": "resource_usage", "path": "resource_usage.json"},
                {"kind": "verifier_output", "path": "verifier_output.json"},
                *trajectory_artifacts,
                {"kind": "atif", "path": "atif.json"},
                {"kind": "provider_logs_manifest", "path": "provider_logs_manifest.json"},
                *verifier_manifest_entries,
            ],
        }
        add_bytes(
            tar,
            _raw_agent_run_path(item, "artifact_manifest.json"),
            _public_json_bytes(artifact_manifest),
        )
        add_bytes(
            tar,
            _raw_agent_run_path(item, "provider_logs_manifest.json"),
            _public_json_bytes(
                {
                    "schema_version": "1",
                    "trial_id": str(item.trial.id),
                    "task_id": item.trial.task_id,
                    "logs": trial_provider_logs,
                }
            ),
        )

    for task_id, task in sorted(tasks_by_id.items()):
        parsed = _parse_s3_uri(task.source or "")
        if parsed is None:
            continue
        bucket, prefix = parsed
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"
        for obj in _list_s3_prefix_objects(client, bucket=bucket, prefix=prefix):
            key = str(obj["Key"])
            rel = _task_archive_relpath(key, prefix)
            if rel is None:
                continue
            body, size = _get_s3_object_stream(
                client,
                bucket=bucket,
                key=key,
                kind="task_bundle",
            )
            archive_path = f"task_bundles/{task_id}/{rel}"
            digest = add_stream(tar, archive_path, body, size)
            task_bundle_files.append(
                {
                    "task_id": task_id,
                    "archive_path": archive_path,
                    "bucket": bucket,
                    "key": key,
                    "sha256": digest,
                    "size_bytes": size,
                }
            )

    add_bytes(
        tar,
        "provider_logs/manifest.json",
        _public_json_bytes(
            {
                "schema_version": "1",
                "logs": provider_logs,
            }
        ),
    )
    add_bytes(
        tar,
        "task_bundles/manifest.json",
        _public_json_bytes(
            {
                "schema_version": "1",
                "files": task_bundle_files,
            }
        ),
    )
    if not tb2_v2_profile and not openhands_profile:
        add_bytes(
            tar,
            "derived/sft_messages.jsonl",
            b"".join(_json_bytes(row) for row in sft_rows),
        )


def _archive_filename(batch: Batch, *, mode: DeliveryExportMode) -> str:
    suffix_by_mode = {
        "lightweight": "delivery",
        "raw-harbor": "raw-harbor",
        "raw-harbor-tb2-v1": "raw-harbor-tb2-v1",
        "raw-harbor-tb2-v2": "raw-harbor-tb2-v2",
        "openhands-export": "openhands-export",
    }
    suffix = suffix_by_mode[mode]
    return f"{_safe_slug(batch.name, max_len=120)}-{suffix}.tar.gz"


def _artifact_response(
    *,
    artifact: Artifact,
    batch_id: UUID,
    manifest: dict[str, Any],
    sha256: str,
    public_base_url: str | None = None,
) -> dict[str, Any]:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    filename = str(storage.get("filename") or artifact.name)
    download_path = f"/api/v1/batches/{batch_id}/delivery-export/{artifact.id}/download"
    return {
        "id": str(artifact.id),
        "status": "ready",
        "archive_filename": filename,
        "sha256": sha256,
        "download_url": _public_download_url(public_base_url, download_path),
        "manifest": manifest,
        "object_validation": manifest.get("object_validation", {"checked": 0, "missing": []}),
        "storage": {
            "bucket": storage.get("bucket"),
            "key": storage.get("key"),
            "size_bytes": storage.get("size_bytes"),
        },
        "created_at": artifact.created_at.isoformat() if artifact.created_at else None,
    }


def _public_download_url(public_base_url: str | None, path: str) -> str:
    if public_base_url and public_base_url.strip():
        return f"{public_base_url.strip().rstrip('/')}{path}"
    return path


async def latest_delivery_export(
    session: Any,
    *,
    batch_id: UUID,
    public_base_url: str | None = None,
) -> dict[str, Any]:
    artifacts = list(
        (
            await session.execute(
                select(Artifact)
                .where(
                    Artifact.batch_id == batch_id,
                    Artifact.artifact_type == "trajectory_bundle",
                )
                .order_by(Artifact.created_at.desc(), Artifact.id.desc()),
            )
        )
        .scalars()
        .all()
    )
    for artifact in artifacts:
        metadata = (
            artifact.artifact_metadata if isinstance(artifact.artifact_metadata, dict) else {}
        )
        delivery = metadata.get("delivery_export")
        if not isinstance(delivery, dict):
            continue
        manifest = delivery.get("manifest")
        if not isinstance(manifest, dict):
            continue
        sha256 = str(delivery.get("sha256") or artifact.content_hash).removeprefix("sha256:")
        return _artifact_response(
            artifact=artifact,
            batch_id=batch_id,
            manifest=manifest,
            sha256=sha256,
            public_base_url=public_base_url,
        )
    return {"status": "not_ready", "reason": "no_delivery_export"}


async def load_delivery_artifact(
    session: Any,
    *,
    batch_id: UUID,
    artifact_id: UUID,
) -> Artifact:
    artifact = (
        await session.execute(
            select(Artifact).where(
                Artifact.id == artifact_id,
                Artifact.batch_id == batch_id,
                Artifact.artifact_type == "trajectory_bundle",
            )
        )
    ).scalar_one_or_none()
    if artifact is None:
        raise InvalidDeliveryBatchFamilyError({"message": "delivery export not found"})
    return cast(Artifact, artifact)


async def create_delivery_export(
    session: Any,
    *,
    minio_client: Any,
    settings: Any,
    ctx: AuthContext,
    main_batch_id: UUID,
    supplemental_batch_ids: list[UUID] | None,
    mode: DeliveryExportMode = "lightweight",
    trial_ids: list[UUID] | None = None,
    public_base_url: str | None = None,
) -> dict[str, Any]:
    main, supplements = await _load_batch_family(session, main_batch_id, supplemental_batch_ids)
    await bind_existing_batch_lifecycle_authority(
        session,
        batch_id=main.id,
        expected_team_id=main.team_id,
    )
    batch_ids = [main.id, *[batch.id for batch in supplements]]
    trials_by_batch = await _trials_for_batches(session, batch_ids)
    selection_meta: dict[str, Any] | None = None
    if trial_ids is not None:
        selected, selection_meta = _select_trials_by_ids(
            main=main,
            supplements=supplements,
            trials_by_batch=trials_by_batch,
            trial_ids=trial_ids,
            trajectories_bucket=settings.trajectories_bucket,
        )
    else:
        selected = _select_trials(
            main=main,
            supplements=supplements,
            trials_by_batch=trials_by_batch,
            trajectories_bucket=settings.trajectories_bucket,
        )
    canonical_bundles = await _canonical_bundles_for_selected(session, selected)
    # Scored deadlines are native attempts. Their reward alone is insufficient:
    # require the committed canonical bundle before exporting their evidence.
    missing_bundles = [
        {"trial_id": str(item.trial.id), "kind": "trial_bundle"}
        for item in selected
        if str(item.trial.state) != "succeeded" and item.trial.id not in canonical_bundles
    ]
    if missing_bundles:
        raise MissingDeliveryObjectsError(
            {
                "message": "scored timeout canonical bundle is unavailable",
                "missing_objects": missing_bundles,
            }
        )
    object_validation = _head_delivery_objects(
        minio_client,
        selected,
        canonical_bundles,
    )
    trajectory_events_by_trial = _validate_selected_terminal_evidence(
        client=minio_client,
        selected=selected,
    )
    tasks_by_id: dict[str, Task] = {}
    llm_calls_by_trial: dict[UUID, list[LlmCall]] = {}
    resource_usage_by_trial = await _resource_usage_for_selected(session, selected)
    extra_object_counts: dict[str, int] = {}
    if canonical_bundles:
        extra_object_counts.update(
            {
                "trial_bundles": len(canonical_bundles),
                "trial_bundle_files": sum(
                    len(bundle.files) for bundle in canonical_bundles.values()
                ),
            }
        )
    if _is_raw_harbor_mode(mode):
        tasks_by_id = await _tasks_for_selected(session, selected)
        llm_calls_by_trial = await _llm_calls_for_selected(session, selected)
        extra_object_counts.update(
            _raw_harbor_object_counts(
                client=minio_client,
                tasks_by_id=tasks_by_id,
                llm_calls_by_trial=llm_calls_by_trial,
            )
        )
    rows = _ledger_rows(selected)
    summary = _summary(
        main=main,
        supplements=supplements,
        selected=selected,
        object_validation=object_validation,
        mode=mode,
        extra_object_counts=extra_object_counts,
        selection_meta=selection_meta,
    )
    archive_manifest = dict(summary)
    archive_manifest["payload_checksums"] = {
        "algorithm": "sha256",
        "file": PAYLOAD_CHECKSUMS_FILE,
        "scope": f"archive payload files excluding {PAYLOAD_CHECKSUMS_FILE}",
    }
    archive = _build_archive(
        client=minio_client,
        manifest=archive_manifest,
        summary=summary,
        rows=rows,
        selected=selected,
        mode=mode,
        trajectory_events_by_trial=trajectory_events_by_trial,
        tasks_by_id=tasks_by_id,
        llm_calls_by_trial=llm_calls_by_trial,
        resource_usage_by_trial=resource_usage_by_trial,
        canonical_bundles=canonical_bundles,
        artifacts_bucket=settings.artifacts_bucket,
    )
    sha256 = archive.sha256
    manifest = dict(archive_manifest)
    manifest["archive_sha256"] = sha256
    summary["archive_sha256"] = sha256

    artifact_id = uuid4()
    artifact_created_at = datetime.now(UTC)
    lifecycle_authority_id = await ensure_artifact_lifecycle_authority(
        session,
        artifact_id=artifact_id,
        team_id=main.team_id,
        created_at=artifact_created_at,
    )
    filename = _archive_filename(main, mode=mode)
    storage_key = f"delivery-exports/{main.team_id}/{main.id}/{artifact_id}/{filename}"
    archive_result = minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=storage_key,
        Body=archive.body,
    )
    close = getattr(archive.body, "close", None)
    if callable(close):
        close()
    checksum_key = f"{storage_key}.sha256"
    checksum_body = f"{sha256}  {filename}\n".encode()
    checksum_result = minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=checksum_key,
        Body=checksum_body,
    )
    archive_version = archive_result.get("VersionId")
    if not isinstance(archive_version, str) or not archive_version:
        archive_version = None
    checksum_version = checksum_result.get("VersionId")
    if not isinstance(checksum_version, str) or not checksum_version:
        checksum_version = None
    await register_lifecycle_object(
        session,
        authority_id=lifecycle_authority_id,
        bucket=settings.artifacts_bucket,
        object_key=storage_key,
        version_id=archive_version,
        content_sha256=sha256,
        size_bytes=archive.size_bytes,
        created_at=artifact_created_at,
    )
    await register_lifecycle_object(
        session,
        authority_id=lifecycle_authority_id,
        bucket=settings.artifacts_bucket,
        object_key=checksum_key,
        version_id=checksum_version,
        content_sha256=hashlib.sha256(checksum_body).hexdigest(),
        size_bytes=len(checksum_body),
        created_at=artifact_created_at,
    )
    source_batch_ids = [str(batch_id) for batch_id in batch_ids]
    artifact = Artifact(
        id=artifact_id,
        artifact_type="trajectory_bundle",
        artifact_schema_version=SCHEMA_VERSION,
        name=filename,
        team_id=main.team_id,
        batch_id=main.id,
        trial_id=None,
        created_by={
            "kind": "delivery_export",
            "token_prefix": ctx.token_hash.hex()[:8] if ctx.token_hash else None,
            "user_id": str(ctx.user_id) if ctx.user_id else None,
        },
        content_hash=f"sha256:{sha256}",
        storage={
            "backend": "object_store",
            "bucket": settings.artifacts_bucket,
            "key": storage_key,
            "filename": filename,
            "media_type": "application/gzip",
            "size_bytes": archive.size_bytes,
            "sha256_key": f"{storage_key}.sha256",
        },
        visibility="team",
        share_status="shared",
        redaction_state="not_required",
        safety_state="safe",
        blocked_reason=None,
        retention={"policy": "keep_forever", "reason": "delivery_export"},
        provenance={
            "relation": "delivery_export",
            "selection_rule": (
                selection_meta["selection_rule"]
                if selection_meta is not None
                else SELECTION_RULE
            ),
            "source_batch_ids": source_batch_ids,
            "selected_trial_ids": [str(item.trial.id) for item in selected],
            **(
                {
                    "requested_trial_ids": list(
                        selection_meta.get("requested_trial_ids") or []
                    ),
                    "skipped_coordinates_count": int(
                        selection_meta.get("skipped_coordinates_count") or 0
                    ),
                }
                if selection_meta is not None
                else {}
            ),
        },
        artifact_metadata={
            "delivery_export": {
                "status": "ready",
                "mode": mode,
                "sha256": sha256,
                "manifest": manifest,
                "task_count": manifest["task_count"],
                "trial_count": manifest["trial_count"],
                "object_counts": manifest["object_counts"],
                "object_validation": object_validation,
                "ledger_rows": len(rows),
            }
        },
        created_at=artifact_created_at,
        lifecycle_authority_id=lifecycle_authority_id,
    )
    session.add(artifact)
    await session.commit()
    await session.refresh(artifact)
    return _artifact_response(
        artifact=artifact,
        batch_id=main.id,
        manifest=manifest,
        sha256=sha256,
        public_base_url=public_base_url,
    )


def artifact_storage_for_download(artifact: Artifact) -> tuple[str, str, str]:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    bucket = storage.get("bucket")
    key = storage.get("key")
    filename = storage.get("filename") or artifact.name
    if not isinstance(bucket, str) or not isinstance(key, str):
        raise InvalidDeliveryBatchFamilyError({"message": "delivery export storage missing"})
    return bucket, key, str(Path(str(filename)).name or artifact.name)
