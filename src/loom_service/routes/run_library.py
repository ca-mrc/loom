"""Org-wide Run Library for completed shared work (#336)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, column, false, func, literal, or_, select, true
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from loom.data_lifecycle_registry import ensure_batch_lifecycle_authority
from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    Batch,
    LlmCall,
    PipelineRun,
    ServiceExecutionLease,
    Team,
    Trial,
)
from loom.security.redaction import redact_mapping, redact_text
from loom.service_execution_backend import NEBIUS_BACKEND, local_execution_enabled
from loom_service import wire_responses as wire
from loom_service.agent_catalog import native_selections_error, selection_agents
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)
from loom_service.combination_summary import combination_summary_for_batch
from loom_service.debug_evidence import build_batch_debug_evidence
from loom_service.delivery_export_errors import DeliveryExportError
from loom_service.dependencies import SessionAndCtx
from loom_service.diagnosis import build_batch_diagnosis, trial_failure_records
from loom_service.effective_combination import (
    DerivedRoutes,
    ProviderRouteError,
    derive_stored_routes,
)
from loom_service.execution_admission import (
    admit_execution_backend,
    freeze_task_resource_requests,
)
from loom_service.failure_taxonomy import is_replaceable_by_successful_supplemental
from loom_service.monitor_filters import apply_batch_monitor_filters
from loom_service.multi_model import apply_plan_mode
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.pool_submission import submission_origin
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.public_links import public_url_for
from loom_service.routes.object_downloads import stream_object_response
from loom_service.submission_compat import validate_submission_agent_task_compatibility
from loom_service.task_config_validation import expected_trial_count
from loom_service.task_filter import resolve_task_filter_with_diagnostics
from loom_service.trial_bundles import CanonicalTrialBundleFile, canonical_bundle_from_artifact
from loom_service.usage_accounting import empty_usage_projection, usage_by_batch_ids

router = APIRouter()

_ORG_VISIBLE_BATCH_STATES = frozenset({"finished", "cancelled"})
_ORG_VISIBLE_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_ARTIFACT_GROUPS = (
    "reports",
    "trajectories",
    "reusable_outputs",
    "logs_diagnostics",
    "raw_diagnostics",
)
_ARTIFACT_TYPE_GROUPS = {
    "atif_projection": "reports",
    "metric_table": "reports",
    "trajectory": "trajectories",
    "trajectory_bundle": "trajectories",
    "completion_set": "reusable_outputs",
    "task_set": "reusable_outputs",
    "task_split": "reusable_outputs",
    "skill_markdown": "reusable_outputs",
    "workflow_spec": "reusable_outputs",
    "verifier_replay": "logs_diagnostics",
    "debug_bundle": "raw_diagnostics",
    "evidence_bundle": "reusable_outputs",
    "training_data_export": "reusable_outputs",
}
_DOWNLOAD_REDACTION_STATES = frozenset({"not_required", "redacted"})
_TRIAL_SUMMARY_STATES = (
    "queued",
    "claimed",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)
_BATCH_LIST_ARTIFACT_SUMMARY_PER_BATCH_LIMIT = 100
_BATCH_DETAIL_ARTIFACT_PREVIEW_LIMIT = 200


class _CloneConfigRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None
    model_switch_plan_mode: Literal["inherit", "resample"] = "inherit"


class _ReuseArtifactRequest(BaseModel):
    key: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


class _VisibilityPatch(BaseModel):
    visibility: str
    share_status: str


@dataclass(frozen=True)
class _RunLibraryTrialProjection:
    id: UUID
    team_id: UUID
    batch_id: UUID | None
    task_id: str
    config: dict[str, Any]
    state: str
    failure_reason: str | None
    failure_message: str | None
    result: dict[str, Any] | None
    started_at: datetime | None
    attempt_count: int
    sample_idx: int
    combination_idx: int
    provider_connection_id: UUID | None
    provider_model_id: str | None
    worker_id: UUID | None


def _is_owner_or_admin(ctx: Any, team_id: UUID) -> bool:
    return is_admin(ctx) or ctx.team_id == team_id


def _batch_is_org_visible(batch: Batch) -> bool:
    return (
        batch.visibility == "org"
        and batch.share_status == "shared"
        and batch.state in _ORG_VISIBLE_BATCH_STATES
    )


def _trial_is_org_visible(
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    if batch is not None:
        return _batch_is_org_visible(batch) and trial.state in _ORG_VISIBLE_TRIAL_STATES

    trial_shared = (
        trial.visibility == "org"
        and trial.share_status == "shared"
        and trial.state in _ORG_VISIBLE_TRIAL_STATES
    )
    return trial_shared


def _require_nebius_source_backend(backend: str | None, *, action: str) -> None:
    """Refuse to derive a new submission from a batch on a retired backend.

    Nebius is the only supported hosted backend. Historical batches keep their
    original backend for display, but a new batch cannot inherit it, and it
    cannot be silently relabelled as Nebius: that would skip the Nebius
    admission and runtime-profile freeze that ordinary submission performs.
    Disposable local execution (LOOM_LOCAL_EXECUTION=1) keeps worker backends.
    """
    if backend != NEBIUS_BACKEND and not local_execution_enabled():
        raise HTTPException(
            status_code=400,
            detail=(
                f"source batch used backend {backend!r}, which is no longer "
                f"supported; Loom hosted execution is Nebius-only. Submit a "
                f"new batch instead of {action}."
            ),
        )


def _can_read_batch(ctx: Any, batch: Batch) -> bool:
    return _is_owner_or_admin(ctx, batch.team_id) or _batch_is_org_visible(batch)


def _can_read_trial(
    ctx: Any,
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    return _is_owner_or_admin(ctx, trial.team_id) or _trial_is_org_visible(
        trial,
        batch,
    )


def _artifact_role(item: dict[str, Any]) -> str:
    raw = item.get("role") or item.get("artifact_role")
    if isinstance(raw, str):
        normalized = raw.strip().lower().replace("-", "_")
        aliases = {
            "report": "reports",
            "reports": "reports",
            "atif": "reports",
            "trajectory": "trajectories",
            "trajectories": "trajectories",
            "output": "reusable_outputs",
            "outputs": "reusable_outputs",
            "reusable_output": "reusable_outputs",
            "reusable_outputs": "reusable_outputs",
            "log": "logs_diagnostics",
            "logs": "logs_diagnostics",
            "diagnostic": "logs_diagnostics",
            "diagnostics": "logs_diagnostics",
            "logs_diagnostics": "logs_diagnostics",
            "raw": "raw_diagnostics",
            "raw_diagnostic": "raw_diagnostics",
            "raw_diagnostics": "raw_diagnostics",
            "internal_diagnostics": "raw_diagnostics",
        }
        role = aliases.get(normalized)
        if role is not None:
            return role

    key = item.get("key")
    key_text = key.lower() if isinstance(key, str) else ""
    if key_text.endswith("atif.json") or "report" in key_text:
        return "reports"
    if key_text.endswith("events.jsonl") or "trajectory" in key_text:
        return "trajectories"
    if "debug" in key_text or "raw" in key_text or "internal" in key_text:
        return "raw_diagnostics"
    if "log" in key_text or "diagnostic" in key_text:
        return "logs_diagnostics"
    return "reusable_outputs"


def _artifact_bucket(item: dict[str, Any], default_bucket: str) -> str:
    bucket = item.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return default_bucket
    return bucket


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _artifact_type_label(artifact_type: str) -> str:
    return artifact_type.replace("_", " ").capitalize()


def _artifact_group_for_type(artifact_type: str) -> str:
    return _ARTIFACT_TYPE_GROUPS.get(artifact_type, "reusable_outputs")


def _artifact_storage_key(artifact: Artifact) -> str | None:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    key = storage.get("key")
    return key if isinstance(key, str) and key else None


def _artifact_storage_bucket(
    artifact: Artifact,
    default_bucket: str,
) -> str:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    bucket = storage.get("bucket")
    return bucket if isinstance(bucket, str) and bucket else default_bucket


def _artifact_storage_size(artifact: Artifact) -> int:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    raw = storage.get("size_bytes") or storage.get("size")
    try:
        return max(int(raw), 0) if raw is not None else 0
    except (TypeError, ValueError):
        return 0


def _artifact_source(artifact: Artifact) -> dict[str, str | None]:
    created_by = artifact.created_by if isinstance(artifact.created_by, dict) else {}
    kind = created_by.get("kind")
    if not isinstance(kind, str) or not kind:
        kind = "trial" if artifact.trial_id is not None else "batch"
    return {
        "kind": kind,
        "batch_id": str(artifact.batch_id) if artifact.batch_id else None,
        "trial_id": str(artifact.trial_id) if artifact.trial_id else None,
    }


def _safe_artifact_blocked_reason(artifact: Artifact) -> str:
    if artifact.blocked_reason:
        return redact_text(artifact.blocked_reason)
    if artifact.safety_state not in {"safe", "unknown"}:
        return "blocked by artifact safety policy"
    if artifact.redaction_state not in _DOWNLOAD_REDACTION_STATES:
        return "blocked by artifact redaction policy"
    return "blocked by artifact sharing policy"


def _artifact_parent_visible(
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    if batch is not None:
        return _batch_is_org_visible(batch)
    if trial is not None:
        return _trial_is_org_visible(trial)
    return artifact.visibility == "org"


def _artifact_content_allowed(
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    return (
        artifact.visibility == "org"
        and artifact.share_status == "shared"
        and artifact.safety_state == "safe"
        and artifact.redaction_state in _DOWNLOAD_REDACTION_STATES
        and _artifact_parent_visible(artifact, batch=batch, trial=trial)
    )


def _artifact_reuse_allowed(
    ctx: Any,
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    # Internal verification permits reuse within the producing team; it does
    # not certify the content for cross-team sharing or bypass blocked content.
    if artifact.blocked_reason:
        return False
    if (
        ctx.team_id == artifact.team_id
        and artifact.safety_state in {"safe", "verified_internal"}
        and artifact.share_status in {"pending_scan", "shared"}
        and artifact.redaction_state in {"pending", "not_required", "redacted"}
    ):
        return True
    return _artifact_content_allowed(artifact, batch=batch, trial=trial)


def _canonical_artifact_files(
    artifact: Artifact, trial: Trial | None,
) -> tuple[CanonicalTrialBundleFile, ...]:
    if trial is None:
        return ()
    try:
        bundle = canonical_bundle_from_artifact(artifact, trial=trial)
    except DeliveryExportError:
        return ()
    # Raw source evidence remains available through the canonical bundle
    # download; only canonical output files are offered as reusable inputs.
    if bundle is None:
        return ()
    return tuple(item for item in bundle.files if item.relative_path.startswith("files/"))


def _artifact_metadata_visible(
    ctx: Any,
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    return _is_owner_or_admin(ctx, artifact.team_id) or (
        artifact.visibility == "org"
        and artifact.share_status == "shared"
        and _artifact_parent_visible(artifact, batch=batch, trial=trial)
    )


def _artifact_metadata_visibility_predicate(
    ctx: Any,
    *,
    artifact_model: Any = Artifact,
    batch_model: Any | None = None,
    trial_model: Any | None = None,
) -> Any:
    """SQL equivalent of ``_artifact_metadata_visible`` for one parent kind."""

    if is_admin(ctx):
        return true()
    if batch_model is not None:
        parent_visible = and_(
            batch_model.visibility == "org",
            batch_model.share_status == "shared",
            batch_model.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
        )
    elif trial_model is not None:
        parent_visible = and_(
            trial_model.visibility == "org",
            trial_model.share_status == "shared",
            trial_model.state.in_(sorted(_ORG_VISIBLE_TRIAL_STATES)),
        )
    else:
        parent_visible = artifact_model.visibility == "org"

    shared_metadata = and_(
        artifact_model.visibility == "org",
        artifact_model.share_status == "shared",
        parent_visible,
    )
    if ctx.team_id is None:
        return shared_metadata
    return or_(artifact_model.team_id == ctx.team_id, shared_metadata)


def _joined_artifact_metadata_visibility_predicate(ctx: Any) -> Any:
    """Apply parent-priority semantics to the artifact library outer joins."""

    if is_admin(ctx):
        return true()
    return or_(
        and_(
            Batch.id.is_not(None),
            _artifact_metadata_visibility_predicate(ctx, batch_model=Batch),
        ),
        and_(
            Batch.id.is_(None),
            Trial.id.is_not(None),
            _artifact_metadata_visibility_predicate(ctx, trial_model=Trial),
        ),
        and_(
            Batch.id.is_(None),
            Trial.id.is_(None),
            _artifact_metadata_visibility_predicate(ctx),
        ),
    )


def _artifact_items(
    trajectory_index: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not trajectory_index:
        return []
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    return [item for item in artifacts if isinstance(item, dict)]


def _find_artifact(
    trajectory_index: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    for item in _artifact_items(trajectory_index):
        if item.get("key") == key:
            return item
    return None


def _share_status(item: dict[str, Any]) -> str:
    status = item.get("share_status")
    if status in {"pending_scan", "shared", "blocked"}:
        return str(status)
    return "pending_scan"


def _blocked_reason(item: dict[str, Any]) -> str:
    reason = item.get("blocked_reason")
    if isinstance(reason, str) and reason.strip():
        return redact_text(reason)
    return "blocked by artifact sharing policy"


async def _typed_artifacts_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, list[Artifact]]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(Artifact)
                .where(Artifact.trial_id.in_(trial_ids))
                .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[Artifact]] = {trial.id: [] for trial in trials}
    for artifact in rows:
        if artifact.trial_id is not None:
            out.setdefault(artifact.trial_id, []).append(artifact)
    return out


async def _parents_for_artifacts(
    session: Any,
    artifacts: Sequence[Artifact],
) -> dict[UUID, list[dict[str, Any]]]:
    artifact_ids = [artifact.id for artifact in artifacts]
    if not artifact_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(ArtifactLineageEdge)
                .where(ArtifactLineageEdge.child_artifact_id.in_(artifact_ids))
                .order_by(
                    ArtifactLineageEdge.created_at.asc(),
                    ArtifactLineageEdge.id.asc(),
                ),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[dict[str, Any]]] = {artifact.id: [] for artifact in artifacts}
    for edge in rows:
        out.setdefault(edge.child_artifact_id, []).append(
            {
                "artifact_id": (
                    str(edge.parent_artifact_id) if edge.parent_artifact_id is not None else None
                ),
                "relation": edge.relation,
                "metadata": edge.edge_metadata,
            }
        )
    return out


def _serialize_typed_artifact(
    request: Request | None,
    artifact: Artifact,
    owner_team: Team,
    *,
    ctx: Any,
    batch: Batch | None = None,
    trial: Trial | None = None,
    parents: list[dict[str, Any]] | None = None,
    bundle_file: CanonicalTrialBundleFile | None = None,
) -> dict[str, Any] | None:
    key = bundle_file.ref.key if bundle_file else _artifact_storage_key(artifact)
    if key is None:
        return None
    role = _artifact_group_for_type(artifact.artifact_type)
    owner_or_admin = _is_owner_or_admin(ctx, artifact.team_id)
    content_allowed = _artifact_content_allowed(
        artifact,
        batch=batch,
        trial=trial,
    )
    full_metadata = owner_or_admin or content_allowed
    reuse_allowed = _artifact_reuse_allowed(
        ctx, artifact, batch=batch, trial=trial,
    )
    can_download = (
        artifact.trial_id is not None
        and request is not None
        and (owner_or_admin or content_allowed)
    )
    entry: dict[str, Any] = {
        "id": str(artifact.id),
        "trial_id": str(artifact.trial_id) if artifact.trial_id else None,
        "key": key if full_metadata else f"redacted-artifact:{artifact.id}",
        "size": (
            (bundle_file.size_bytes if bundle_file else _artifact_storage_size(artifact))
            if full_metadata else 0
        ),
        "role": role,
        "artifact_type": artifact.artifact_type,
        "artifact_type_label": _artifact_type_label(artifact.artifact_type),
        "artifact_schema_version": artifact.artifact_schema_version,
        "owner_team": {"id": str(owner_team.id), "name": owner_team.name},
        "source": _artifact_source(artifact),
        "share_status": artifact.share_status,
        "safety_state": artifact.safety_state,
        "redaction_state": artifact.redaction_state,
        "can_reuse": artifact.trial_id is not None and reuse_allowed,
        "blocked_reason": None if reuse_allowed else _safe_artifact_blocked_reason(artifact),
        "content_hash": artifact.content_hash if full_metadata else None,
        "storage": artifact.storage if full_metadata else None,
        "provenance": artifact.provenance if full_metadata else {},
        "metadata": {
            key: value for key, value in (artifact.artifact_metadata or {}).items()
            if not (artifact.control_producer_kind == "service_execution"
                    and key == "execution_event_payloads")
        } if full_metadata else {},
        "parents": (parents or []) if full_metadata else [],
    }
    if bundle_file is not None and full_metadata:
        entry.update({
            "relative_path": bundle_file.relative_path,
            "content_hash": bundle_file.sha256,
            "storage": {
                "bucket": bundle_file.ref.bucket,
                "key": bundle_file.ref.key,
                "media_type": bundle_file.media_type,
                "size_bytes": bundle_file.size_bytes,
            },
        })
    if can_download and request is not None:
        entry["download_url"] = str(
            public_url_for(
                request,
                "download_run_library_artifact",
                trial_id=str(artifact.trial_id),
            ).include_query_params(key=key),
        )
    else:
        entry["download_url"] = None
    return entry


def _serialize_typed_artifacts(
    request: Request | None,
    artifact: Artifact,
    owner_team: Team,
    *,
    ctx: Any,
    batch: Batch | None,
    trial: Trial | None,
    parents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not _artifact_metadata_visible(ctx, artifact, batch=batch, trial=trial):
        return []
    if artifact.pipeline_run_id is not None and artifact.pipeline_stage_run_id is not None:
        # Pipeline bundles use an upload manifest, not a Trial storage key.
        # Represent the bundle itself; its detail route authorizes file access.
        return [{
            "id": str(artifact.id), "trial_id": None,
            "key": artifact.name, "relative_path": artifact.name,
            "size": artifact.stored_size_bytes or 0,
            "role": _artifact_group_for_type(artifact.artifact_type),
            "artifact_type": artifact.artifact_type,
            "owner_team": {"id": str(owner_team.id), "name": owner_team.name},
            "share_status": artifact.share_status,
            "safety_state": artifact.safety_state,
            "redaction_state": artifact.redaction_state,
            "can_reuse": False, "download_url": None,
        }]
    files: Sequence[CanonicalTrialBundleFile | None] = (
        _canonical_artifact_files(artifact, trial)
        if _artifact_storage_key(artifact) is None else (None,)
    )
    entries = []
    for item in files:
        entry = _serialize_typed_artifact(
            request, artifact, owner_team, ctx=ctx, batch=batch, trial=trial,
            parents=parents, bundle_file=item,
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _legacy_artifact_type(item: dict[str, Any]) -> str:
    raw = item.get("artifact_type")
    if isinstance(raw, str) and raw:
        return raw
    role = _artifact_role(item)
    if role == "reports":
        return "atif_projection"
    if role == "trajectories":
        return "trajectory"
    if role in {"logs_diagnostics", "raw_diagnostics"}:
        return "debug_bundle"
    return "evidence_bundle"


def _legacy_safety_state(item: dict[str, Any]) -> str:
    status = _share_status(item)
    if status == "shared":
        return "safe"
    if status == "blocked":
        return "unsafe"
    return "unknown"


def _legacy_redaction_state(item: dict[str, Any]) -> str:
    status = _share_status(item)
    if status == "shared":
        return "not_required"
    if status == "blocked":
        return "blocked"
    return "pending"


def _serialize_legacy_artifact(
    request: Request,
    trial: Trial,
    owner_team: Team,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    key = item.get("key")
    if not isinstance(key, str) or not key:
        return None
    size = item.get("size")
    try:
        size_int = int(size) if size is not None else 0
    except (TypeError, ValueError):
        size_int = 0
    role = _artifact_role(item)
    status = _share_status(item)
    artifact_type = _legacy_artifact_type(item)
    return {
        "trial_id": str(trial.id),
        "key": key,
        "size": max(size_int, 0),
        "role": role,
        "artifact_type": artifact_type,
        "artifact_type_label": _artifact_type_label(artifact_type),
        "artifact_schema_version": "1.0",
        "owner_team": {"id": str(owner_team.id), "name": owner_team.name},
        "source": {
            "kind": "trial",
            "batch_id": str(trial.batch_id) if trial.batch_id else None,
            "trial_id": str(trial.id),
        },
        "share_status": status,
        "safety_state": _legacy_safety_state(item),
        "redaction_state": _legacy_redaction_state(item),
        "blocked_reason": (_blocked_reason(item) if status == "blocked" else None),
        "content_hash": item.get("content_hash") or "pending:legacy-unhashed",
        "storage": {
            "backend": "object_store",
            "bucket": _artifact_bucket(item, "artifacts"),
            "key": key,
            "media_type": item.get("media_type") or "application/octet-stream",
            "size_bytes": max(size_int, 0),
        },
        "provenance": {
            "batch_id": str(trial.batch_id) if trial.batch_id else None,
            "trial_id": str(trial.id),
            "source_trial_ids": [str(trial.id)],
            "relation": "produced_from",
        },
        "metadata": {
            "legacy_role": item.get("role") or item.get("artifact_role"),
            "step_name": item.get("step_name"),
        },
        "parents": [],
        "download_url": str(
            public_url_for(
                request,
                "download_run_library_artifact",
                trial_id=str(trial.id),
            ).include_query_params(key=key),
        ),
    }


def _empty_artifact_summary() -> dict[str, int]:
    return {role: 0 for role in _ARTIFACT_GROUPS}


async def _batch_detail_artifact_preview(
    session: Any,
    batch_id: UUID,
    trials: Sequence[Any],
) -> tuple[dict[UUID, list[Artifact]], dict[str, int], bool]:
    typed_by_trial: dict[UUID, list[Artifact]] = {trial.id: [] for trial in trials}
    summary = _empty_artifact_summary()
    per_batch_limit = max(int(_BATCH_DETAIL_ARTIFACT_PREVIEW_LIMIT), 0)
    if per_batch_limit == 0:
        return typed_by_trial, summary, True

    trial_ids = [trial.id for trial in trials]
    rows = list(
        (
            await session.execute(
                select(Artifact)
                .where(
                    or_(
                        Artifact.batch_id == batch_id,
                        Artifact.trial_id.in_(trial_ids),
                    )
                )
                .order_by(Artifact.created_at.asc(), Artifact.id.asc())
                .limit(per_batch_limit + 1),
            )
        )
        .scalars()
        .all()
    )
    truncated = len(rows) > per_batch_limit
    for artifact in rows[:per_batch_limit]:
        summary[_artifact_group_for_type(artifact.artifact_type)] += 1
        if artifact.trial_id is not None:
            typed_by_trial.setdefault(artifact.trial_id, []).append(artifact)
    return typed_by_trial, summary, truncated


async def _batch_trial_bundles(
    request: Request,
    session: Any,
    trials: Sequence[_RunLibraryTrialProjection],
) -> list[dict[str, Any]]:
    if not trials:
        return []
    leases = list(
        (
            await session.execute(
                select(ServiceExecutionLease)
                .where(
                    ServiceExecutionLease.trial_id.in_([trial.id for trial in trials]),
                    ServiceExecutionLease.execution_role == "attempt",
                )
                .order_by(ServiceExecutionLease.trial_id, ServiceExecutionLease.attempt.desc())
            )
        )
        .scalars()
        .all()
    )
    newest_lease: dict[UUID, ServiceExecutionLease] = {}
    for lease in leases:
        newest_lease.setdefault(lease.trial_id, lease)
    artifacts = list(
        (
            await session.execute(
                select(Artifact).where(
                    Artifact.trial_id.in_([trial.id for trial in trials]),
                    Artifact.control_producer_kind == "service_execution",
                )
            )
        )
        .scalars()
        .all()
    )
    artifacts_by_trial: dict[UUID, list[Artifact]] = {}
    for artifact in artifacts:
        if artifact.trial_id is not None:
            artifacts_by_trial.setdefault(artifact.trial_id, []).append(artifact)
    out: list[dict[str, Any]] = []
    for trial in trials:
        lease = newest_lease.get(trial.id)
        if lease is None:
            continue
        bundle = None
        integrity_error = False
        try:
            matches = [
                parsed
                for artifact in artifacts_by_trial.get(trial.id, [])
                if (
                    parsed := canonical_bundle_from_artifact(
                        artifact,
                        trial=trial,
                    )
                )
                is not None
            ]
            if len(matches) == 1:
                bundle = matches[0]
            elif len(matches) > 1:
                integrity_error = True
        except DeliveryExportError:
            integrity_error = True
        ready = bundle is not None and lease.materialization_state == "committed"
        out.append(
            {
                "trial_id": str(trial.id),
                "task_id": trial.task_id,
                "trial_state": trial.state,
                "materialization_state": (
                    "invalid_integrity" if integrity_error else lease.materialization_state
                ),
                "attempts": lease.materialization_attempts,
                "canonical_ready": ready,
                "file_count": len(bundle.files) if ready and bundle else 0,
                "size_bytes": (
                    sum(item.size_bytes for item in bundle.files)
                    if ready and bundle
                    else 0
                ),
                "manifest_sha256": bundle.manifest_sha256 if ready and bundle else None,
                "content_sha256": bundle.content_sha256 if ready and bundle else None,
                "download_url": (
                    str(
                        public_url_for(
                            request,
                            "download_trial_bundle",
                            trial_id=str(trial.id),
                        )
                    )
                    if ready
                    else None
                ),
            }
        )
    return out


def _artifact_inventory(
    request: Request,
    ctx: Any,
    trials: Sequence[Any],
    batch: Batch,
    owner_team: Team,
    typed_by_trial: dict[UUID, list[Artifact]],
    parents_by_artifact: dict[UUID, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {role: [] for role in _ARTIFACT_GROUPS}
    for trial in trials:
        typed = typed_by_trial.get(trial.id) or []
        if typed:
            for artifact in typed:
                entries = _serialize_typed_artifacts(
                    request,
                    artifact,
                    owner_team,
                    ctx=ctx,
                    batch=batch,
                    trial=trial,
                    parents=parents_by_artifact.get(artifact.id, []),
                )
                for entry in entries:
                    grouped[entry["role"]].append(entry)
            continue
        for item in _artifact_items(getattr(trial, "trajectory_index", None)):
            legacy_entry = _serialize_legacy_artifact(request, trial, owner_team, item)
            if legacy_entry is not None:
                grouped[legacy_entry["role"]].append(legacy_entry)
    return grouped


def _trial_summary(trials: Sequence[Any]) -> dict[str, int]:
    summary = _empty_trial_summary()
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _empty_trial_summary() -> dict[str, int]:
    return {state: 0 for state in _TRIAL_SUMMARY_STATES}


def _rollup_result(result: dict[str, Any] | None) -> tuple[float | None, float]:
    if not result:
        return None, 0.0
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        reward_f = float(reward) if reward is not None else None
    except (TypeError, ValueError):
        reward_f = None
    try:
        cost_f = float(result.get("cost_usd", 0) or 0)
    except (TypeError, ValueError):
        cost_f = 0.0
    return reward_f, cost_f


def _trial_rollup(trials: Sequence[Any]) -> tuple[float | None, float]:
    reward_sum = 0.0
    reward_count = 0
    cost_total = 0.0
    for trial in trials:
        if trial.state not in _ORG_VISIBLE_TRIAL_STATES:
            continue
        reward, cost = _rollup_result(trial.result)
        cost_total += cost
        if reward is not None:
            reward_sum += reward
            reward_count += 1
    return (
        reward_sum / reward_count if reward_count else None,
        cost_total,
    )


def _artifact_filter_active(filters: dict[str, Any]) -> bool:
    return any(value is not None for value in filters.values())


def _typed_artifact_matches_filters(
    artifact: Artifact,
    filters: dict[str, Any],
) -> bool:
    artifact_type = filters.get("artifact_type")
    if artifact_type is not None and artifact_type not in {
        artifact.artifact_type,
        _artifact_group_for_type(artifact.artifact_type),
    }:
        return False
    owner_team_id = filters.get("owner_team_id")
    if owner_team_id is not None and artifact.team_id != owner_team_id:
        return False
    source_trial_id = filters.get("source_trial_id")
    provenance = artifact.provenance if isinstance(artifact.provenance, dict) else {}
    source_trial_ids = provenance.get("source_trial_ids")
    if source_trial_id is not None:
        if artifact.trial_id != source_trial_id and (
            not isinstance(source_trial_ids, list)
            or str(source_trial_id) not in {str(item) for item in source_trial_ids}
        ):
            return False
    source_batch_id = filters.get("source_batch_id")
    if source_batch_id is not None:
        if artifact.batch_id != source_batch_id and str(provenance.get("batch_id")) != str(
            source_batch_id
        ):
            return False
    safety_state = filters.get("safety_state")
    if safety_state is not None and artifact.safety_state != safety_state:
        return False
    provenance_relation = filters.get("provenance_relation")
    if provenance_relation is not None and provenance.get("relation") != provenance_relation:
        return False
    return True


def _typed_artifact_filter_predicates(filters: dict[str, Any]) -> list[Any]:
    predicates: list[Any] = []
    artifact_type = filters.get("artifact_type")
    if artifact_type is not None:
        if artifact_type in _ARTIFACT_GROUPS:
            matching_types = tuple(
                item_type
                for item_type, group in _ARTIFACT_TYPE_GROUPS.items()
                if group == artifact_type
            )
            group_predicate: Any = Artifact.artifact_type.in_(matching_types)
            if artifact_type == "reusable_outputs":
                group_predicate = or_(
                    group_predicate,
                    Artifact.artifact_type.not_in(tuple(_ARTIFACT_TYPE_GROUPS)),
                )
            predicates.append(group_predicate)
        else:
            predicates.append(Artifact.artifact_type == artifact_type)

    owner_team_id = filters.get("owner_team_id")
    if owner_team_id is not None:
        predicates.append(Artifact.team_id == owner_team_id)
    source_trial_id = filters.get("source_trial_id")
    if source_trial_id is not None:
        predicates.append(
            or_(
                Artifact.trial_id == source_trial_id,
                Artifact.provenance.contains(
                    {"source_trial_ids": [str(source_trial_id)]},
                ),
            )
        )
    source_batch_id = filters.get("source_batch_id")
    if source_batch_id is not None:
        predicates.append(
            or_(
                Artifact.batch_id == source_batch_id,
                Artifact.provenance.contains(
                    {"batch_id": str(source_batch_id)},
                ),
            )
        )
    safety_state = filters.get("safety_state")
    if safety_state is not None:
        predicates.append(Artifact.safety_state == safety_state)
    provenance_relation = filters.get("provenance_relation")
    if provenance_relation is not None:
        predicates.append(
            Artifact.provenance.contains({"relation": provenance_relation}),
        )
    return predicates


def _legacy_artifact_role_expression(item: Any) -> Any:
    raw_role = func.lower(
        func.replace(
            func.btrim(
                func.coalesce(
                    func.nullif(item["role"].astext, ""),
                    item["artifact_role"].astext,
                    "",
                )
            ),
            "-",
            "_",
        )
    )
    key_text = func.lower(func.coalesce(item["key"].astext, ""))
    return case(
        (raw_role.in_(("report", "reports", "atif")), "reports"),
        (raw_role.in_(("trajectory", "trajectories")), "trajectories"),
        (
            raw_role.in_(
                ("output", "outputs", "reusable_output", "reusable_outputs"),
            ),
            "reusable_outputs",
        ),
        (
            raw_role.in_(
                (
                    "log",
                    "logs",
                    "diagnostic",
                    "diagnostics",
                    "logs_diagnostics",
                ),
            ),
            "logs_diagnostics",
        ),
        (
            raw_role.in_(
                (
                    "raw",
                    "raw_diagnostic",
                    "raw_diagnostics",
                    "internal_diagnostics",
                ),
            ),
            "raw_diagnostics",
        ),
        (or_(key_text.like("%atif.json"), key_text.like("%report%")), "reports"),
        (
            or_(key_text.like("%events.jsonl"), key_text.like("%trajectory%")),
            "trajectories",
        ),
        (
            or_(
                key_text.like("%debug%"),
                key_text.like("%raw%"),
                key_text.like("%internal%"),
            ),
            "raw_diagnostics",
        ),
        (
            or_(key_text.like("%log%"), key_text.like("%diagnostic%")),
            "logs_diagnostics",
        ),
        else_="reusable_outputs",
    )


def _legacy_artifact_filter_predicates(
    item: Any,
    filters: dict[str, Any],
) -> tuple[list[Any], Any]:
    role = _legacy_artifact_role_expression(item)
    explicit_type = case(
        (
            func.jsonb_typeof(item["artifact_type"]) == "string",
            func.nullif(item["artifact_type"].astext, ""),
        ),
        else_=None,
    )
    artifact_type_expression = case(
        (explicit_type.is_not(None), explicit_type),
        (role == "reports", "atif_projection"),
        (role == "trajectories", "trajectory"),
        (role.in_(("logs_diagnostics", "raw_diagnostics")), "debug_bundle"),
        else_="evidence_bundle",
    )
    raw_share_status = item["share_status"].astext
    share_status = case(
        (
            raw_share_status.in_(("pending_scan", "shared", "blocked")),
            raw_share_status,
        ),
        else_="pending_scan",
    )
    safety_state_expression = case(
        (share_status == "shared", "safe"),
        (share_status == "blocked", "unsafe"),
        else_="unknown",
    )

    predicates: list[Any] = []
    artifact_type = filters.get("artifact_type")
    if artifact_type is not None:
        predicates.append(
            or_(
                artifact_type_expression == artifact_type,
                role == artifact_type,
            )
        )
    owner_team_id = filters.get("owner_team_id")
    if owner_team_id is not None:
        predicates.append(Trial.team_id == owner_team_id)
    source_trial_id = filters.get("source_trial_id")
    if source_trial_id is not None:
        predicates.append(Trial.id == source_trial_id)
    source_batch_id = filters.get("source_batch_id")
    if source_batch_id is not None:
        predicates.append(Trial.batch_id == source_batch_id)
    safety_state = filters.get("safety_state")
    if safety_state is not None:
        predicates.append(safety_state_expression == safety_state)
    provenance_relation = filters.get("provenance_relation")
    if provenance_relation is not None:
        predicates.append(
            true() if provenance_relation == "produced_from" else false(),
        )
    return predicates, share_status


def _batch_artifact_filter_predicate(
    ctx: Any,
    filters: dict[str, Any],
) -> Any:
    """Filter batches with one database-level artifact existence predicate."""

    typed_trial = aliased(Trial, name="artifact_filter_trial")
    typed_parent = or_(
        and_(
            Artifact.batch_id == Batch.id,
            _artifact_metadata_visibility_predicate(ctx, batch_model=Batch),
        ),
        and_(
            Artifact.batch_id.is_(None),
            typed_trial.batch_id == Batch.id,
            _artifact_metadata_visibility_predicate(ctx, trial_model=typed_trial),
        ),
    )
    typed_match = (
        select(Artifact.id)
        .select_from(Artifact)
        .outerjoin(typed_trial, typed_trial.id == Artifact.trial_id)
        .where(typed_parent, *_typed_artifact_filter_predicates(filters))
        .correlate(Batch)
        .exists()
    )

    legacy_array = case(
        (
            func.jsonb_typeof(Trial.trajectory_index["artifacts"]) == "array",
            Trial.trajectory_index["artifacts"],
        ),
        else_=literal([], type_=JSONB),
    )
    legacy_items = (
        func.jsonb_array_elements(legacy_array)
        .table_valued(column("value", JSONB))
        .alias("legacy_filter_artifact")
    )
    legacy_item = legacy_items.c.value
    legacy_predicates, legacy_share_status = _legacy_artifact_filter_predicates(
        legacy_item,
        filters,
    )
    legacy_visible: Any
    if is_admin(ctx):
        legacy_visible = true()
    else:
        owner_visible = Trial.team_id == ctx.team_id if ctx.team_id is not None else false()
        legacy_visible = or_(
            owner_visible,
            and_(
                Batch.visibility == "org",
                Batch.share_status == "shared",
                Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
                Trial.state.in_(sorted(_ORG_VISIBLE_TRIAL_STATES)),
                legacy_share_status == "shared",
            ),
        )
    legacy_item_match = (
        select(literal(1))
        .select_from(legacy_items)
        .where(
            func.jsonb_typeof(legacy_item) == "object",
            legacy_visible,
            *legacy_predicates,
        )
        .correlate(Batch, Trial)
        .exists()
    )
    typed_for_trial = aliased(Artifact, name="typed_artifact_for_legacy_trial")
    no_typed_registry_entry = ~(
        select(typed_for_trial.id)
        .where(typed_for_trial.trial_id == Trial.id)
        .correlate(Trial)
        .exists()
    )
    legacy_match = (
        select(Trial.id)
        .where(
            Trial.batch_id == Batch.id,
            no_typed_registry_entry,
            legacy_item_match,
        )
        .correlate(Batch)
        .exists()
    )
    return or_(typed_match, legacy_match)


async def _batch_trials(session: Any, batch_id: UUID) -> list[Trial]:
    return list(
        (
            await session.execute(
                select(Trial).where(Trial.batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )


async def _batch_trial_projections(
    session: Any,
    batch_id: UUID,
) -> list[_RunLibraryTrialProjection]:
    return await _batch_trial_projections_for_batch_ids(session, [batch_id])


async def _batch_trial_projections_for_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> list[_RunLibraryTrialProjection]:
    if not batch_ids:
        return []
    rows = (
        await session.execute(
            select(
                Trial.id,
                Trial.team_id,
                Trial.batch_id,
                Trial.task_id,
                Trial.config,
                Trial.state,
                Trial.failure_reason,
                Trial.failure_message,
                Trial.result,
                Trial.started_at,
                Trial.attempt_count,
                Trial.sample_idx,
                Trial.combination_idx,
                Trial.provider_connection_id,
                Trial.provider_model_id,
                Trial.worker_id,
            )
            .where(Trial.batch_id.in_(list(batch_ids)))
            .order_by(Trial.submitted_at.asc(), Trial.id.asc()),
        )
    ).all()
    return [
        _RunLibraryTrialProjection(
            id=row.id,
            team_id=row.team_id,
            batch_id=row.batch_id,
            task_id=row.task_id,
            config=row.config,
            state=row.state,
            failure_reason=row.failure_reason,
            failure_message=row.failure_message,
            result=row.result,
            started_at=row.started_at,
            attempt_count=row.attempt_count,
            sample_idx=row.sample_idx,
            combination_idx=row.combination_idx,
            provider_connection_id=row.provider_connection_id,
            provider_model_id=row.provider_model_id,
            worker_id=row.worker_id,
        )
        for row in rows
    ]


def _trial_key(trial: Any) -> tuple[str, int, int]:
    return (
        str(trial.task_id),
        int(getattr(trial, "sample_idx", 0) or 0),
        int(getattr(trial, "combination_idx", 0) or 0),
    )


def _effective_trials(
    original_trials: Sequence[Any],
    rerun_trials: Sequence[Any],
) -> list[Any]:
    original_by_key = {_trial_key(trial): trial for trial in original_trials}
    effective = dict(original_by_key)
    for trial in rerun_trials:
        if str(trial.state) != "succeeded":
            continue
        key = _trial_key(trial)
        original = original_by_key.get(key)
        if original is None or not is_replaceable_by_successful_supplemental(original):
            continue
        effective[key] = trial
    return list(effective.values())


async def _visible_rerun_batch_ids(session: Any, batch_id: UUID) -> list[UUID]:
    return [
        row.id
        for row in (
            await session.execute(
                select(Batch.id).where(
                    Batch.rerun_of_batch_id == batch_id,
                    Batch.visibility == "org",
                    Batch.share_status == "shared",
                    Batch.state.in_(_ORG_VISIBLE_BATCH_STATES),
                ),
            )
        ).all()
    ]


async def _llm_calls_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> list[LlmCall]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return []
    return list(
        (
            await session.execute(
                select(LlmCall)
                .where(LlmCall.trial_id.in_(trial_ids))
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )


def _serialize_batch_base(batch: Batch, owner_team: Team) -> dict[str, Any]:
    return {
        "id": str(batch.id),
        "team_id": str(batch.team_id),
        "owner_team": {
            "id": str(owner_team.id),
            "name": owner_team.name,
        },
        "name": batch.name,
        "description": batch.description,
        "task_filter": batch.task_filter,
        "trial_config": batch.trial_config,
        "backend": batch.backend,
        "combinations": batch.combinations,
        "provider_connection_id": (
            str(batch.provider_connection_id) if batch.provider_connection_id else None
        ),
        "provider_model_id": batch.provider_model_id,
        "state": batch.state,
        "result_status": batch.result_status,
        "visibility": batch.visibility,
        "share_status": batch.share_status,
        "source_provenance": batch.source_provenance,
        "resolved_task_ids": batch.resolved_task_ids,
        "expected_trial_count": batch.expected_trial_count,
        "created_by_token_prefix": batch.created_by_token_prefix,
        "created_at": batch.created_at.isoformat(),
        "finished_at": batch.finished_at.isoformat() if batch.finished_at else None,
    }


async def _serialize_batch(
    request: Request,
    session: Any,
    ctx: Any,
    batch: Batch,
    owner_team: Team,
    *,
    include_inventory: bool = False,
    include_debug: bool = False,
) -> dict[str, Any]:
    trials = await _batch_trial_projections(session, batch.id)
    (
        typed_by_trial,
        artifact_summary,
        artifact_summary_truncated,
    ) = await _batch_detail_artifact_preview(session, batch.id, trials)
    reward, cost = _trial_rollup(trials)
    combination_summary = await combination_summary_for_batch(
        session,
        combinations=batch.combinations,
        trials=trials,
        expected_trial_count=batch.expected_trial_count,
        required_worker_pool_count=len(batch.required_worker_pools or []),
        fanout_errors=batch.fanout_errors,
    )
    visible_rerun_batch_ids = await _visible_rerun_batch_ids(session, batch.id)
    rerun_trials = await _batch_trial_projections_for_batch_ids(
        session,
        visible_rerun_batch_ids,
    )
    effective_trials = _effective_trials(trials, rerun_trials)
    effective_combination_summary = await combination_summary_for_batch(
        session,
        combinations=batch.combinations,
        trials=effective_trials,
        expected_trial_count=batch.expected_trial_count,
        required_worker_pool_count=len(batch.required_worker_pools or []),
        fanout_errors=batch.fanout_errors,
    )
    out = {
        **_serialize_batch_base(batch, owner_team),
        "trial_summary": _trial_summary(trials),
        "aggregate_reward": reward,
        "total_cost_usd": cost,
        "combination_summary": combination_summary,
        "effective_combination_summary": effective_combination_summary,
        "artifact_summary": artifact_summary,
        "artifact_summary_truncated": artifact_summary_truncated,
    }
    usage = await usage_by_batch_ids(session, [batch.id])
    out.update(usage.get(batch.id, empty_usage_projection()))
    if include_debug:
        llm_calls = await _llm_calls_for_trials(session, trials)
        debug_evidence = build_batch_debug_evidence(
            batch,
            trials=trials,
            llm_calls=llm_calls,
        )
        out["debug_evidence"] = debug_evidence
        out["diagnosis"] = build_batch_diagnosis(
            debug_evidence,
            trial_failures=trial_failure_records(trials),
        )
    if include_inventory:
        typed_artifacts = [
            artifact for artifacts in typed_by_trial.values() for artifact in artifacts
        ]
        parents_by_artifact = await _parents_for_artifacts(session, typed_artifacts)
        out["artifact_inventory"] = _artifact_inventory(
            request,
            ctx,
            trials,
            batch,
            owner_team,
            typed_by_trial,
            parents_by_artifact,
        )
        out["artifact_inventory_truncated"] = artifact_summary_truncated
        out["trial_bundles"] = (
            await _batch_trial_bundles(
                request,
                session,
                trials,
            )
            if _is_owner_or_admin(ctx, batch.team_id)
            else []
        )
    return out


async def _batch_list_trial_rollups(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, tuple[dict[str, int], float | None, float]]:
    summaries = {batch_id: _empty_trial_summary() for batch_id in batch_ids}
    reward_sums = {batch_id: 0.0 for batch_id in batch_ids}
    reward_counts = {batch_id: 0 for batch_id in batch_ids}
    cost_totals = {batch_id: 0.0 for batch_id in batch_ids}
    if not batch_ids:
        return {}

    rows = (
        await session.execute(
            select(Trial.batch_id, Trial.state, Trial.result).where(
                Trial.batch_id.in_(batch_ids),
            ),
        )
    ).all()
    for batch_id, state, result in rows:
        if batch_id is None:
            continue
        summary = summaries.setdefault(batch_id, _empty_trial_summary())
        state_text = str(state)
        summary[state_text] = summary.get(state_text, 0) + 1
        if state_text not in _ORG_VISIBLE_TRIAL_STATES:
            continue
        reward, cost = _rollup_result(result)
        cost_totals[batch_id] = cost_totals.get(batch_id, 0.0) + cost
        if reward is not None:
            reward_sums[batch_id] = reward_sums.get(batch_id, 0.0) + reward
            reward_counts[batch_id] = reward_counts.get(batch_id, 0) + 1

    return {
        batch_id: (
            summaries.get(batch_id, _empty_trial_summary()),
            (
                reward_sums.get(batch_id, 0.0) / reward_counts[batch_id]
                if reward_counts.get(batch_id, 0)
                else None
            ),
            cost_totals.get(batch_id, 0.0),
        )
        for batch_id in batch_ids
    }


async def _batch_list_artifact_summaries(
    session: Any,
    ctx: Any,
    batch_ids: Sequence[UUID],
) -> tuple[dict[UUID, dict[str, int]], set[UUID]]:
    """Load capped, caller-visible typed summaries in one lateral query."""

    summaries = {batch_id: _empty_artifact_summary() for batch_id in batch_ids}
    truncated: set[UUID] = set()
    if not batch_ids:
        return summaries, truncated

    per_batch_limit = max(int(_BATCH_LIST_ARTIFACT_SUMMARY_PER_BATCH_LIMIT), 0)
    artifact_trial = aliased(Trial, name="artifact_summary_trial")
    direct_parent = and_(
        Artifact.batch_id == Batch.id,
        _artifact_metadata_visibility_predicate(ctx, batch_model=Batch),
    )
    trial_parent = and_(
        Artifact.batch_id.is_(None),
        artifact_trial.batch_id == Batch.id,
        _artifact_metadata_visibility_predicate(ctx, trial_model=artifact_trial),
    )
    visible_artifacts = (
        select(
            Artifact.artifact_type.label("artifact_type"),
            Artifact.created_at.label("created_at"),
            Artifact.id.label("artifact_id"),
        )
        .select_from(Artifact)
        .outerjoin(artifact_trial, artifact_trial.id == Artifact.trial_id)
        .where(or_(direct_parent, trial_parent))
        .order_by(Artifact.created_at.asc(), Artifact.id.asc())
        .limit(per_batch_limit + 1)
        .correlate(Batch)
        .lateral("visible_batch_artifacts")
    )
    rows = (
        await session.execute(
            select(
                Batch.id,
                visible_artifacts.c.artifact_type,
                visible_artifacts.c.created_at,
                visible_artifacts.c.artifact_id,
            )
            .select_from(Batch)
            .join(visible_artifacts, true())
            .where(Batch.id.in_(batch_ids))
            .order_by(
                Batch.id.asc(),
                visible_artifacts.c.created_at.asc(),
                visible_artifacts.c.artifact_id.asc(),
            ),
        )
    ).all()

    seen = {batch_id: 0 for batch_id in batch_ids}
    for batch_id, artifact_type, _created_at, _artifact_id in rows:
        if batch_id not in summaries:
            continue
        seen[batch_id] += 1
        if seen[batch_id] > per_batch_limit:
            truncated.add(batch_id)
            continue
        summary = summaries.setdefault(batch_id, _empty_artifact_summary())
        summary[_artifact_group_for_type(str(artifact_type))] += 1
    return summaries, truncated


def _serialize_batch_list_item(
    batch: Batch,
    owner_team: Team,
    trial_rollup: tuple[dict[str, int], float | None, float],
    artifact_summary: dict[str, int],
    artifact_summary_truncated: bool = False,
) -> dict[str, Any]:
    trial_summary, reward, cost = trial_rollup
    return {
        **_serialize_batch_base(batch, owner_team),
        "trial_summary": trial_summary,
        "aggregate_reward": reward,
        "total_cost_usd": cost,
        "artifact_summary": artifact_summary,
        "artifact_summary_truncated": artifact_summary_truncated,
    }


async def _load_batch_with_team(
    session: Any,
    batch_id: UUID,
) -> tuple[Batch, Team]:
    row = (
        await session.execute(
            select(Batch, Team).join(Team, Team.id == Batch.team_id).where(Batch.id == batch_id),
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="batch not found")
    batch, team = row
    return batch, team


async def _load_trial_with_batch(
    session: Any,
    trial_id: UUID,
) -> tuple[Trial, Batch | None]:
    trial = (
        await session.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    batch: Batch | None = None
    if trial.batch_id is not None:
        batch = (
            await session.execute(
                select(Batch).where(Batch.id == trial.batch_id),
            )
        ).scalar_one_or_none()
    return trial, batch


async def _typed_artifact_for_trial_key(
    session: Any,
    trial: Trial,
    key: str,
) -> tuple[Artifact, CanonicalTrialBundleFile | None] | None:
    rows = cast(
        list[Artifact],
        list(
            (
                await session.execute(
                    select(Artifact)
                    .where(Artifact.trial_id == trial.id)
                    .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
                )
            )
            .scalars()
            .all()
        ),
    )
    for artifact in rows:
        if _artifact_storage_key(artifact) == key:
            return artifact, None
        for item in _canonical_artifact_files(artifact, trial):
            if item.ref.key == key:
                return artifact, item
    return None


def _apply_read_filter(
    stmt: Any,
    *,
    ctx: Any,
    scope: str,
    team_id: UUID | None,
) -> Any:
    if team_id is not None:
        if _is_owner_or_admin(ctx, team_id):
            return stmt.where(Batch.team_id == team_id)
        return stmt.where(
            and_(
                Batch.team_id == team_id,
                Batch.visibility == "org",
                Batch.share_status == "shared",
                Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
            ),
        )

    if scope == "all":
        if is_admin(ctx):
            return stmt
        return stmt.where(
            or_(
                Batch.team_id == ctx.team_id,
                and_(
                    Batch.visibility == "org",
                    Batch.share_status == "shared",
                    Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
                ),
            ),
        )

    if ctx.team_id is None:
        return stmt
    return stmt.where(Batch.team_id == ctx.team_id)


def _batch_after_cursor(cursor: Cursor) -> Any:
    """Return the strict keyset predicate for the Run Library ordering."""

    return or_(
        Batch.created_at < cursor.submitted_at,
        and_(
            Batch.created_at == cursor.submitted_at,
            Batch.id < cursor.id,
        ),
    )


@router.get("/run-library/batches", response_model=wire.RunLibraryBatchList, response_model_exclude_unset=True)
async def list_run_library_batches(
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    visibility: Annotated[str | None, Query(pattern="^(team|org|private)$")] = None,
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")

    stmt = (
        select(Batch, Team)
        .join(Team, Team.id == Batch.team_id)
        .order_by(Batch.created_at.desc(), Batch.id.desc())
    )
    stmt = _apply_read_filter(stmt, ctx=ctx, scope=scope, team_id=team_id)
    stmt = apply_batch_monitor_filters(
        stmt,
        target_team=None,
        q=q,
        benchmark_id=benchmark_id,
        agent_name=agent_name or agent,
        model_provider=model_provider,
        model_name=model_name or model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
    if visibility:
        stmt = stmt.where(Batch.visibility == visibility)
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(_batch_after_cursor(cur))
    artifact_filters = {
        "artifact_type": artifact_type,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    artifact_filtering = _artifact_filter_active(artifact_filters)
    if artifact_filtering:
        stmt = stmt.where(_batch_artifact_filter_predicate(ctx, artifact_filters))
    rows = [(batch, team) for batch, team in (await session.execute(stmt.limit(limit + 1))).all()]

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    serialized: list[dict[str, Any]] = []
    batch_ids = [batch.id for batch, _team in page_rows]
    trial_rollups = await _batch_list_trial_rollups(session, batch_ids)
    usage = await usage_by_batch_ids(session, batch_ids)
    artifact_summaries, truncated_artifact_summaries = await _batch_list_artifact_summaries(
        session, ctx, batch_ids
    )
    for batch, team in page_rows:
        item = _serialize_batch_list_item(
            batch,
            team,
            trial_rollups.get(
                batch.id,
                (_empty_trial_summary(), None, 0.0),
            ),
            artifact_summaries.get(batch.id, _empty_artifact_summary()),
            batch.id in truncated_artifact_summaries,
        )
        item.update(usage.get(batch.id, empty_usage_projection()))
        serialized.append(item)

    next_cursor: str | None = None
    if has_more and page_rows:
        last_batch = page_rows[-1][0]
        next_cursor = encode_cursor(
            Cursor(submitted_at=last_batch.created_at, id=last_batch.id),
        )
    return {"items": serialized, "next_cursor": next_cursor}


async def _artifact_rows_for_library(
    session: Any,
    ctx: Any,
    *,
    request: Request | None,
    scope: str,
    artifact_filters: dict[str, Any],
    safe_content_only: bool,
    limit: int,
    cursor: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    stmt = (
        select(Artifact, Team, Batch, Trial)
        .join(Team, Team.id == Artifact.team_id)
        .outerjoin(Batch, Batch.id == Artifact.batch_id)
        .outerjoin(Trial, Trial.id == Artifact.trial_id)
        .order_by(Artifact.created_at.desc(), Artifact.id.desc())
    )
    owner_team_id = artifact_filters.get("owner_team_id")
    if owner_team_id is not None:
        stmt = stmt.where(Artifact.team_id == owner_team_id)

    artifact_type = artifact_filters.get("artifact_type")
    if artifact_type in _ARTIFACT_TYPE_GROUPS:
        stmt = stmt.where(Artifact.artifact_type == artifact_type)
    # Source filters may match execution columns or provenance JSON, so they
    # stay in the Python filter below instead of narrowing SQL too early.
    safety_state = artifact_filters.get("safety_state")
    if safety_state is not None:
        stmt = stmt.where(Artifact.safety_state == safety_state)
    if artifact_filters.get("producer_kind") == "pipeline":
        stmt = stmt.where(
            Artifact.producer_kind.in_(("container", "platform", "checkpoint")),
            Artifact.pipeline_run_id.is_not(None),
            Artifact.pipeline_stage_run_id.is_not(None),
        )
        pipeline_recipe = artifact_filters.get("pipeline_recipe")
        pipeline_result = artifact_filters.get("pipeline_result")
        run_filter = select(PipelineRun.id)
        if pipeline_recipe:
            try:
                recipe_name, recipe_version = str(pipeline_recipe).rsplit("@", 1)
                run_filter = run_filter.where(
                    PipelineRun.recipe_name == recipe_name,
                    PipelineRun.recipe_version == int(recipe_version),
                )
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=422, detail="invalid pipeline_recipe") from exc
        if pipeline_result:
            run_filter = run_filter.where(PipelineRun.result == pipeline_result)
        stmt = stmt.where(Artifact.pipeline_run_id.in_(run_filter))

    if not is_admin(ctx):
        restricted_runs = select(PipelineRun.id).where(
            PipelineRun.team_id == ctx.team_id,
        )
        if getattr(ctx, "role", None) != "owner":
            restricted_runs = restricted_runs.where(
                PipelineRun.created_by_user_id == getattr(ctx, "user_id", None),
            ) if getattr(ctx, "user_id", None) is not None else restricted_runs.where(false())
        stmt = stmt.where(or_(
            Artifact.pipeline_run_id.is_(None),
            Artifact.access_class.is_(None),
            Artifact.access_class.in_(("sanitized_audit", "team_runtime")),
            Artifact.pipeline_run_id.in_(restricted_runs),
        ))

    if scope != "all":
        if ctx.team_id is None:
            return [], None
        stmt = stmt.where(Artifact.team_id == ctx.team_id)
    elif not is_admin(ctx):
        stmt = stmt.where(_joined_artifact_metadata_visibility_predicate(ctx))

    if cursor:
        try:
            after = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(or_(
            Artifact.created_at < after.submitted_at,
            and_(Artifact.created_at == after.submitted_at, Artifact.id < after.id),
        ))

    # Scan bounded database pages because provenance and content authorization
    # also filter in Python. Look ahead after filtering, never before it.
    selected: list[tuple[Artifact, Team, Batch | None, Trial | None]] = []
    scan_stmt = stmt
    scan_size = max(limit + 1, 200)
    while len(selected) <= limit:
        rows = list((await session.execute(scan_stmt.limit(scan_size))).all())
        for artifact, owner_team, batch, trial in rows:
            if not _artifact_metadata_visible(ctx, artifact, batch=batch, trial=trial):
                continue
            if not _typed_artifact_matches_filters(artifact, artifact_filters):
                continue
            if safe_content_only and not _artifact_content_allowed(
                artifact, batch=batch, trial=trial,
            ):
                continue
            selected.append((artifact, owner_team, batch, trial))
            if len(selected) > limit:
                break
        if len(selected) > limit or len(rows) < scan_size:
            break
        last = rows[-1][0]
        scan_stmt = stmt.where(or_(
            Artifact.created_at < last.created_at,
            and_(Artifact.created_at == last.created_at, Artifact.id < last.id),
        ))
    next_cursor = None
    if len(selected) > limit:
        selected = selected[:limit]
        last = selected[-1][0]
        next_cursor = encode_cursor(Cursor(submitted_at=last.created_at, id=last.id))

    parents_by_artifact = await _parents_for_artifacts(
        session,
        [artifact for artifact, _owner_team, _batch, _trial in selected],
    )
    out: list[dict[str, Any]] = []
    pipeline_ids = (
        {
            artifact.pipeline_run_id
            for artifact, _owner_team, _batch, _trial in selected
            if artifact.pipeline_run_id is not None
        }
        if artifact_filters.get("producer_kind") == "pipeline"
        else set()
    )
    pipeline_runs = {}
    if pipeline_ids:
        pipeline_runs = {
            item.id: item
            for item in list(
                (
                    await session.execute(
                        select(PipelineRun).where(PipelineRun.id.in_(pipeline_ids))
                    )
                ).scalars()
            )
        }
    for artifact, owner_team, batch, trial in selected:
        items = _serialize_typed_artifacts(
            request,
            artifact,
            owner_team,
            ctx=ctx,
            batch=batch,
            trial=trial,
            parents=parents_by_artifact.get(artifact.id, []),
        )
        for item in items:
            pipeline_run = pipeline_runs.get(artifact.pipeline_run_id)
            if artifact_filters.get("producer_kind") == "pipeline" and pipeline_run is not None:
                item["pipeline"] = {
                    "run_id": str(pipeline_run.id),
                    "stage_run_id": str(artifact.pipeline_stage_run_id),
                    "recipe": f"{pipeline_run.recipe_name}@{pipeline_run.recipe_version}",
                    "result": pipeline_run.result,
                }
            out.append(redact_mapping(item) if safe_content_only else item)
    return out, next_cursor


@router.get("/run-library/artifacts")
async def list_run_library_artifacts(
    request: Request,
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    producer_kind: Annotated[str | None, Query(pattern="^pipeline$")] = None,
    pipeline_recipe: Annotated[str | None, Query()] = None,
    pipeline_result: Annotated[str | None, Query()] = None,
    team_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=500)] = 200,
    cursor: Annotated[str | None, Query()] = None,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    filters = {
        "artifact_type": artifact_type,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
        "producer_kind": producer_kind,
        "pipeline_recipe": pipeline_recipe,
        "pipeline_result": pipeline_result,
        "owner_team_id": team_id or owner_team_id,
    }
    stmt_items, next_cursor = await _artifact_rows_for_library(
        session,
        ctx,
        request=request,
        scope=scope,
        artifact_filters=filters,
        safe_content_only=False,
        limit=limit,
        cursor=cursor,
    )
    return {"items": stmt_items, "next_cursor": next_cursor}


@router.get("/run-library/artifacts/export")
async def export_run_library_artifacts(
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    format: Annotated[str, Query(pattern="^(json|jsonl)$")] = "jsonl",
    limit: Annotated[int, Query(gt=0, le=5000)] = 1000,
) -> Response:
    session, ctx = sc
    require_scope(ctx, "read:own")
    filters = {
        "artifact_type": artifact_type,
        "owner_team_id": owner_team_id,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    items, _next_cursor = await _artifact_rows_for_library(
        session,
        ctx,
        request=None,
        scope=scope,
        artifact_filters=filters,
        safe_content_only=True,
        limit=limit,
    )
    if format == "json":
        return Response(
            content=json.dumps({"items": items}, separators=(",", ":")),
            media_type="application/json",
        )
    content = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in items)
    return Response(content=content, media_type="application/x-ndjson")


@router.get("/run-library/batches/{batch_id}", response_model=wire.RunLibraryBatchDetail, response_model_exclude_unset=True)
async def get_run_library_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    include_debug: Annotated[
        bool,
        Query(
            description=(
                "Include heavyweight debug_evidence and diagnosis payloads. "
                "Defaults to false so Run Library detail stays bounded."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    batch, team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, batch):
        raise HTTPException(status_code=403, detail="batch is not shared")
    return await _serialize_batch(
        request,
        session,
        ctx,
        batch,
        team,
        include_inventory=True,
        include_debug=include_debug,
    )


@router.patch("/run-library/batches/{batch_id}/visibility")
async def update_run_library_batch_visibility(
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _VisibilityPatch,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    if payload.visibility not in {"team", "org", "private"}:
        raise HTTPException(status_code=400, detail="invalid visibility")
    if payload.share_status not in {"pending_scan", "shared", "blocked"}:
        raise HTTPException(status_code=400, detail="invalid share_status")
    batch, _team = await _load_batch_with_team(session, batch_id)
    require_team_or_admin(ctx, batch.team_id)
    batch.visibility = payload.visibility
    batch.share_status = payload.share_status
    await session.commit()
    await session.refresh(batch)
    return {
        "batch_id": str(batch.id),
        "visibility": batch.visibility,
        "share_status": batch.share_status,
    }


def _current_retry_defaults(settings: Any) -> dict[str, Any]:
    """Materialize the current deployment RetryPolicy defaults."""
    return {
        "max_attempts": settings.trial_retry_default_max_attempts,
        "retry_on": sorted(settings.trial_retry_default_retry_on),
        "backoff": {
            "base_sec": settings.trial_retry_default_backoff_base_sec,
            "max_sec": settings.trial_retry_default_backoff_max_sec,
            "multiplier": settings.trial_retry_default_backoff_multiplier,
            "jitter": settings.trial_retry_default_backoff_jitter,
        },
    }


def _retry_default_snapshot_mismatch(
    source_trial_config: dict[str, Any],
    settings: Any,
) -> dict[str, Any] | None:
    """#401 PR-3: warn on clone when the source batch's explicit RetryPolicy
    diverges from the current deployment defaults. Returns None when the
    source had no explicit `retry` (clones will inherit current defaults at
    submit) or when the source policy already matches."""
    source_retry = source_trial_config.get("retry")
    if not isinstance(source_retry, dict):
        return None
    current = _current_retry_defaults(settings)
    source_backoff = source_retry.get("backoff") or {}
    source_norm = {
        "max_attempts": source_retry.get(
            "max_attempts",
            current["max_attempts"],
        ),
        "retry_on": sorted(source_retry.get("retry_on", current["retry_on"])),
        "backoff": {
            "base_sec": source_backoff.get(
                "base_sec",
                current["backoff"]["base_sec"],
            ),
            "max_sec": source_backoff.get(
                "max_sec",
                current["backoff"]["max_sec"],
            ),
            "multiplier": source_backoff.get(
                "multiplier",
                current["backoff"]["multiplier"],
            ),
            "jitter": source_backoff.get(
                "jitter",
                current["backoff"]["jitter"],
            ),
        },
    }
    if source_norm == current:
        return None
    return {"source": source_norm, "current": current}


async def _resolve_new_batch_snapshot(
    session: AsyncSession,
    *,
    task_filter: dict[str, Any],
    team_id: UUID,
) -> tuple[list[str], list[dict[str, str]]]:
    """Resolve and lifecycle-check tasks before a derived batch is committed."""

    result = await resolve_task_filter_with_diagnostics(
        session,
        task_filter,
        team_id=team_id,
        require_runnable=True,
    )
    if not result.task_ids:
        raise HTTPException(status_code=400, detail="task filter matched zero runnable tasks")
    return list(result.task_ids), list(result.benchmark_selection_provenance)


async def _freeze_derived_runtime_profile(
    request: Request,
    session: AsyncSession,
    *,
    team_id: UUID,
    backend: str,
    task_ids: list[str],
    trial_config: dict[str, Any],
    combinations: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """A derived batch is a new submission under current deployment policy.

    Resolve current harness releases and resource defaults through the ordinary
    admission path; never copy a historical runtime snapshot into a new batch.
    """
    await validate_submission_agent_task_compatibility(
        session, team_id=team_id, task_ids=task_ids,
        combinations=combinations, trial_config=trial_config,
    )
    profile = await admit_execution_backend(
        session, backend=backend, task_ids=task_ids,
        trial_config=trial_config, combinations=combinations,
        runtime_profile_json=request.app.state.settings.service_execution_runtime_profile_json,
    )
    profile = await freeze_task_resource_requests(
        session, backend=backend, task_ids=task_ids,
        trial_config=trial_config, combinations=combinations, profile=profile, overrides={},
    )
    return profile.model_dump(mode="json") if profile is not None else None



async def _derive_new_batch_routes(
    session: Any,
    *,
    team_id: UUID,
    trial_config: dict[str, Any],
    combinations: list[dict[str, Any]],
    source_connection_id: UUID | None,
    source_model_id: str | None,
    replacement_connection_id: UUID | None,
    replacement_model_id: str | None,
    action: str,
    backend: str | None,
) -> DerivedRoutes:
    """A clone or artifact reuse creates new trials, so its stored
    selections go through the fresh-submission contract (#2054), and every
    model-backed selection runs on the caller's selected connection rather
    than the source's. Only the resulting connections are authorized."""
    native_err = native_selections_error(backend, selection_agents(trial_config, combinations))
    if native_err is not None:
        raise HTTPException(status_code=400, detail=f"cannot {action}: {native_err}")
    try:
        routes = derive_stored_routes(
            trial_config=trial_config,
            combinations=combinations,
            source_connection_id=source_connection_id,
            source_model_id=source_model_id,
            replacement_connection_id=replacement_connection_id,
            replacement_model_id=replacement_model_id,
            replace_connections=True,
        )
    except ProviderRouteError as exc:
        raise HTTPException(status_code=400, detail=f"cannot {action}: {exc}") from exc
    for connection_id in sorted(routes.connection_ids, key=str):
        await validate_provider_connection(
            session,
            connection_id,
            team_id=team_id,
            agent_submission=True,
        )
    return routes


@router.post("/run-library/batches/{batch_id}/clone-config", status_code=201, response_model=wire.CloneRunLibraryBatchResult, response_model_exclude_unset=True)
async def clone_run_library_batch_config(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _CloneConfigRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    source, _team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, source):
        raise HTTPException(status_code=403, detail="batch is not shared")
    _require_nebius_source_backend(source.backend, action="cloning its config")
    routes = await _derive_new_batch_routes(
        session,
        team_id=ctx.team_id,
        trial_config=source.trial_config,
        combinations=list(source.combinations or []),
        source_connection_id=source.provider_connection_id,
        source_model_id=source.provider_model_id,
        replacement_connection_id=payload.provider_connection_id,
        replacement_model_id=payload.provider_model_id,
        action="clone",
        backend=source.backend,
    )

    task_filter = dict(source.task_filter)
    explicit_task_ids = task_filter.get("task_ids")
    if isinstance(explicit_task_ids, (list, tuple)) and all(
        isinstance(task_id, str) for task_id in explicit_task_ids
    ):
        await validate_submission_agent_task_compatibility(
            session,
            team_id=ctx.team_id,
            task_ids=explicit_task_ids,
            combinations=list(source.combinations or []),
            trial_config=source.trial_config,
        )
    resolved_task_ids, benchmark_provenance = await _resolve_new_batch_snapshot(
        session,
        task_filter=task_filter,
        team_id=ctx.team_id,
    )
    combinations = routes.combinations
    trial_config = apply_plan_mode(dict(source.trial_config), mode=payload.model_switch_plan_mode)
    runtime_profile = await _freeze_derived_runtime_profile(
        request, session, team_id=ctx.team_id, backend=source.backend,
        task_ids=resolved_task_ids, combinations=combinations, trial_config=trial_config,
    )
    # #1109: user clone must not re-inject operator pool-coverage trials.
    required_worker_pools: list[str] = []
    expected = expected_trial_count(
        task_count=len(resolved_task_ids),
        n_per_task=source.n_per_task,
        combinations=combinations,
    )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    provenance: list[dict[str, Any]] = [
        {
            "kind": "cloned_batch_config",
            "source_batch_id": str(source.id),
            "source_team_id": str(source.team_id),
            "source_visibility": source.visibility,
        },
        *benchmark_provenance,
    ]
    clone_id = uuid4()
    clone_created_at = datetime.now(UTC)
    clone_lifecycle_authority_id = await ensure_batch_lifecycle_authority(
        session,
        batch_id=clone_id,
        team_id=ctx.team_id,
        created_at=clone_created_at,
    )
    clone = Batch(
        id=clone_id,
        pool_origin=submission_origin(request.app.state.settings, clone_id),
        purpose=source.purpose,
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description or (f"Cloned config from shared batch {source.id}."),
        task_filter=task_filter,
        resolved_task_ids=resolved_task_ids,
        trial_config=trial_config,
        service_execution_runtime_profile=runtime_profile,
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
        expected_trial_count=expected,
        n_per_task=source.n_per_task,
        backend=source.backend,
        combinations=combinations,
        required_worker_pools=required_worker_pools,
        provider_connection_id=routes.batch_connection_id,
        provider_model_id=routes.batch_model_id,
        source_provenance=provenance,
        created_at=clone_created_at,
        lifecycle_authority_id=clone_lifecycle_authority_id,
    )
    session.add(clone)
    await session.commit()
    await session.refresh(clone)
    mismatch = _retry_default_snapshot_mismatch(
        clone.trial_config,
        request.app.state.settings,
    )
    return {
        "batch_id": str(clone.id),
        "cloned_from_batch_id": str(source.id),
        "provider_connection_id": (
            str(clone.provider_connection_id) if clone.provider_connection_id else None
        ),
        "provider_model_id": clone.provider_model_id,
        "source_provenance": clone.source_provenance,
        "state": clone.state,
        "created_at": clone.created_at.isoformat(),
        "retry_default_snapshot_mismatch": mismatch,
    }


@router.get("/run-library/trials/{trial_id}/artifacts/download")
async def download_run_library_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    key: Annotated[str, Query(min_length=1)],
) -> StreamingResponse:
    settings = request.app.state.settings
    session, ctx = sc
    require_scope(ctx, "read:own")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    match = await _typed_artifact_for_trial_key(session, trial, key)
    if match is not None:
        typed_artifact, bundle_file = match
        if not (
            _is_owner_or_admin(ctx, typed_artifact.team_id)
            or _artifact_content_allowed(
                typed_artifact,
                batch=batch,
                trial=trial,
            )
        ):
            raise HTTPException(
                status_code=403,
                detail=_safe_artifact_blocked_reason(typed_artifact),
            )
        return stream_object_response(
            client=request.app.state.minio_client,
            bucket=(bundle_file.ref.bucket if bundle_file else _artifact_storage_bucket(
                typed_artifact, settings.artifacts_bucket,
            )),
            key=key,
            filename=_artifact_filename(key),
            artifact_kind="artifact",
        )
    artifact = _find_artifact(trial.trajectory_index, key)
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=_artifact_bucket(artifact, settings.artifacts_bucket),
        key=key,
        filename=_artifact_filename(key),
        artifact_kind="artifact",
    )


@router.post("/run-library/trials/{trial_id}/artifacts/reuse", status_code=201, response_model=wire.ReuseRunLibraryArtifactResult, response_model_exclude_unset=True)
async def reuse_run_library_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    payload: _ReuseArtifactRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    _require_nebius_source_backend(
        batch.backend if batch is not None else None,
        action="reusing its artifact",
    )
    match = await _typed_artifact_for_trial_key(session, trial, payload.key)
    typed_artifact, bundle_file = match if match else (None, None)
    artifact = _find_artifact(trial.trajectory_index, payload.key)
    if typed_artifact is None and artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if typed_artifact is not None and not _artifact_reuse_allowed(
        ctx, typed_artifact,
        batch=batch,
        trial=trial,
    ):
        raise HTTPException(
            status_code=403,
            detail=_safe_artifact_blocked_reason(typed_artifact),
        )
    if typed_artifact is None and artifact is not None and _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    routes = await _derive_new_batch_routes(
        session,
        team_id=ctx.team_id,
        trial_config=dict(batch.trial_config) if batch is not None else dict(trial.config),
        combinations=list(batch.combinations or []) if batch is not None else [],
        source_connection_id=(
            batch.provider_connection_id if batch is not None else trial.provider_connection_id
        ),
        source_model_id=trial.provider_model_id or (batch.provider_model_id if batch else None),
        replacement_connection_id=payload.provider_connection_id,
        replacement_model_id=payload.provider_model_id,
        action="reuse",
        backend=batch.backend if batch is not None else None,
    )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    role = (
        _artifact_group_for_type(typed_artifact.artifact_type)
        if typed_artifact is not None
        else _artifact_role(artifact or {})
    )
    source_batch_id = str(batch.id) if batch is not None else None
    provenance_item: dict[str, Any] = {
        "kind": "reused_artifact",
        "relation": "reused_as_input",
        "source_batch_id": source_batch_id,
        "source_trial_id": str(trial.id),
        "source_team_id": str(trial.team_id),
        "source_artifact_key": payload.key,
        "source_artifact_role": role,
    }
    if typed_artifact is not None:
        provenance_item.update(
            {
                "source_artifact_id": str(typed_artifact.id),
                "source_artifact_type": typed_artifact.artifact_type,
                "source_artifact_schema_version": (typed_artifact.artifact_schema_version),
                "source_content_hash": typed_artifact.content_hash,
                "source_visibility": typed_artifact.visibility,
                "source_share_status": typed_artifact.share_status,
                "source_safety_state": typed_artifact.safety_state,
                "source_redaction_state": typed_artifact.redaction_state,
            }
        )
    if bundle_file is not None:
        provenance_item.update({
            "source_artifact_relative_path": bundle_file.relative_path,
            "source_file_sha256": bundle_file.sha256,
        })
    provenance = [provenance_item]
    task_filter = (
        dict(batch.task_filter)
        if batch is not None
        else {"subset_kind": "explicit", "task_ids": [trial.task_id]}
    )
    trial_config = dict(batch.trial_config) if batch is not None else dict(trial.config)
    resolved_task_ids, benchmark_provenance = await _resolve_new_batch_snapshot(
        session,
        task_filter=task_filter,
        team_id=ctx.team_id,
    )
    combinations = routes.combinations
    backend = batch.backend if batch else "docker"
    runtime_profile = await _freeze_derived_runtime_profile(
        request, session, team_id=ctx.team_id, backend=backend,
        task_ids=resolved_task_ids, combinations=combinations, trial_config=trial_config,
    )
    # #1109: user artifact reuse must not re-inject operator pool-coverage.
    required_worker_pools: list[str] = []
    n_per_task = batch.n_per_task if batch else 1
    expected = expected_trial_count(
        task_count=len(resolved_task_ids),
        n_per_task=n_per_task,
        combinations=combinations,
    )
    provenance.extend(benchmark_provenance)
    derived_id = uuid4()
    derived_created_at = datetime.now(UTC)
    derived_lifecycle_authority_id = await ensure_batch_lifecycle_authority(
        session,
        batch_id=derived_id,
        team_id=ctx.team_id,
        created_at=derived_created_at,
    )
    derived = Batch(
        id=derived_id,
        pool_origin=submission_origin(request.app.state.settings, derived_id),
        purpose="trajectory_generation",
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description
        or (f"Reuses shared artifact {payload.key} from trial {trial.id}."),
        task_filter=task_filter,
        resolved_task_ids=resolved_task_ids,
        trial_config=trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
        expected_trial_count=expected,
        n_per_task=n_per_task,
        backend=backend,
        service_execution_runtime_profile=runtime_profile,
        combinations=combinations,
        required_worker_pools=required_worker_pools,
        provider_connection_id=routes.batch_connection_id,
        provider_model_id=routes.batch_model_id,
        source_provenance=provenance,
        created_at=derived_created_at,
        lifecycle_authority_id=derived_lifecycle_authority_id,
    )
    session.add(derived)
    await session.commit()
    await session.refresh(derived)
    return {
        "batch_id": str(derived.id),
        "source_artifact": {
            **(
                {
                    "id": str(typed_artifact.id),
                    "artifact_type": typed_artifact.artifact_type,
                    "artifact_schema_version": typed_artifact.artifact_schema_version,
                    "content_hash": typed_artifact.content_hash,
                }
                if typed_artifact is not None
                else {}
            ),
            "trial_id": str(trial.id),
            "key": payload.key,
            "role": role,
        },
        "source_provenance": derived.source_provenance,
        "state": derived.state,
        "created_at": derived.created_at.isoformat(),
    }
