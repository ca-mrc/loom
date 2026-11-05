"""Batch-family delivery bundle export routes (#390)."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from loom.auth import AuthContext
from loom.db.schema import Batch
from loom_service import wire_responses as wire
from loom_service.auth_guards import require_scope, require_team_or_admin
from loom_service.delivery_export import (
    artifact_storage_for_download,
    create_delivery_export,
    latest_delivery_export,
    load_delivery_artifact,
)
from loom_service.delivery_export_errors import DeliveryExportError
from loom_service.delivery_export_openhands import OpenHandsExportError
from loom_service.delivery_export_tb2_v2 import Tb2V2ExportError
from loom_service.dependencies import SessionAndCtx
from loom_service.public_links import public_base_url
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter()


class _DeliveryExportSelection(BaseModel):
    trial_ids: list[UUID] | None = Field(
        default=None,
        description=(
            "Optional explicit trial UUID list within the authorized batch family. "
            "When set, only these trials are packed and unresolved sibling "
            "coordinates are skipped. Selected trials must still be delivery-"
            "eligible and pass object/TB2 validation."
        ),
    )
    eligible_only: bool = Field(
        default=False,
        description=(
            "Pack every delivery-eligible attempt in the batch family and "
            "record coordinates that are not eligible. Mutually exclusive "
            "with trial_ids. Omit both for the full-family export that fails "
            "closed when any coordinate is unresolved."
        ),
    )

    @model_validator(mode="after")
    def _exclusive_selection(self) -> "_DeliveryExportSelection":
        if self.eligible_only and self.trial_ids:
            raise ValueError(
                "selection.eligible_only cannot be combined with selection.trial_ids"
            )
        return self


class _DeliveryExportRequest(BaseModel):
    mode: Literal[
        "lightweight",
        "raw-harbor",
        "raw-harbor-tb2-v1",
        "raw-harbor-tb2-v2",
        "openhands-export",
    ] = Field(
        default="lightweight",
        description=(
            "Export mode. `lightweight` preserves the #390 ledger, ATIF, and "
            "trajectory bundle. `raw-harbor` adds Derek-style raw provider "
            "logs, task bundle inputs, agent-run artifacts, and derived SFT JSONL. "
            "`raw-harbor-tb2-v1` adds the versioned TB2-facing delivery profile "
            "while preserving Loom-native audit artifacts. "
            "`raw-harbor-tb2-v2` projects execution from typed terminus2 events "
            "and native Harbor artifacts. "
            "`openhands-export` projects execution from native OpenHands SDK "
            "events and provider logs."
        ),
    )
    supplemental_batch_ids: list[UUID] | None = Field(
        default=None,
        description=(
            "Optional explicit supplemental rerun priority order. "
            "When omitted, linked rerun descendants are used by created_at/id order."
        ),
    )
    selection: _DeliveryExportSelection | None = Field(
        default=None,
        description=(
            "Optional subset selection. Omit for the default full-family "
            "coordinate resolution that fails closed on unresolved attempts."
        ),
    )


async def _load_authorized_batch(
    sc: SessionAndCtx,
    batch_id: UUID,
) -> tuple[Batch, AuthContext]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    batch = (
        await session.execute(select(Batch).where(Batch.id == batch_id))
    ).scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    require_team_or_admin(ctx, batch.team_id)
    return batch, ctx


@router.get("/batches/{batch_id}/delivery-export", response_model=wire.DeliveryExport, response_model_exclude_unset=True)
async def get_batch_delivery_export(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, object]:
    batch, _ctx = await _load_authorized_batch(sc, batch_id)
    session, _ = sc
    return await latest_delivery_export(
        session,
        batch_id=batch.id,
        public_base_url=public_base_url(request),
    )


@router.post("/batches/{batch_id}/delivery-export", status_code=201, response_model=wire.DeliveryExport, response_model_exclude_unset=True)
async def create_batch_delivery_export(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _DeliveryExportRequest | None = None,
) -> dict[str, object]:
    batch, ctx = await _load_authorized_batch(sc, batch_id)
    require_scope(ctx, "submit")
    session, _ = sc
    mode = payload.mode if payload is not None else "lightweight"
    try:
        return await create_delivery_export(
            session,
            minio_client=request.app.state.minio_client,
            settings=request.app.state.settings,
            ctx=ctx,
            main_batch_id=batch.id,
            supplemental_batch_ids=(
                payload.supplemental_batch_ids if payload is not None else None
            ),
            mode=mode,
            trial_ids=(
                list(payload.selection.trial_ids)
                if (
                    payload is not None
                    and payload.selection is not None
                    and payload.selection.trial_ids is not None
                )
                else None
            ),
            eligible_only=(
                payload is not None
                and payload.selection is not None
                and payload.selection.eligible_only
            ),
            public_base_url=public_base_url(request),
        )
    except (DeliveryExportError, Tb2V2ExportError, OpenHandsExportError) as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/batches/{batch_id}/delivery-export/{artifact_id}/download")
async def download_delivery_export(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    artifact_id: UUID,
) -> StreamingResponse:
    batch, _ctx = await _load_authorized_batch(sc, batch_id)
    session, _ = sc
    try:
        artifact = await load_delivery_artifact(
            session,
            batch_id=batch.id,
            artifact_id=artifact_id,
        )
        bucket, key, filename = artifact_storage_for_download(artifact)
    except (DeliveryExportError, Tb2V2ExportError, OpenHandsExportError) as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=bucket,
        key=key,
        filename=filename,
        artifact_kind="trajectory_bundle",
        media_type="application/gzip",
    )
