"""Operator and future-actuator surfaces for durable service execution state."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from loom.auth import AuthContext, is_admin, verify_bearer_token
from loom.db.schema import (
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionLeaseHistory,
    Trial,
)
from loom.execution_contract import (
    ExecutionClassV1,
    ExecutionTopologyV1,
    WorkloadRequirementsV1,
)
from loom.execution_image_admission import ImageAdmissionError, ImageAdmissionKeyring
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    acknowledge_execution_command,
    claim_execution_commands,
    enqueue_execution_transition,
    execution_lease_projection,
    persist_execution_catalog,
    record_execution_event,
    reserve_trial_execution,
    set_execution_target_health,
)

router = APIRouter()


class _StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CatalogBody(_StrictBody):
    execution_class: ExecutionClassV1
    topology: ExecutionTopologyV1


class TargetHealthBody(_StrictBody):
    desired_state: str
    observed_state: str
    health_status: str
    observed_at: datetime
    error_code: str | None = Field(default=None, max_length=120)


class ReservationBody(_StrictBody):
    request_id: UUID
    trial_id: UUID
    execution_class_id: str
    target_id: str
    requirements: WorkloadRequirementsV1
    runtime_contract: ExecutionRuntimePlanV1
    parent_lease_id: UUID | None = None
    deadline_at: datetime


class TransitionBody(_StrictBody):
    expected_generation: int = Field(gt=0)
    desired_state: str
    payload: dict[str, Any] = Field(default_factory=dict)


class CommandClaimBody(_StrictBody):
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    consumer_id: str = Field(min_length=1, max_length=120)
    limit: int = Field(default=20, ge=1, le=100)
    lease_seconds: int = Field(default=60, ge=5, le=300)


class CommandAckBody(_StrictBody):
    consumer_id: str = Field(min_length=1, max_length=120)
    acknowledgement: dict[str, Any]


class ExecutionEventBody(_StrictBody):
    generation: int = Field(gt=0)
    ordinal: int = Field(gt=0)
    event_kind: str
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=180)
    payload: dict[str, Any]
    observed_at: datetime


async def _auth(request: Request, authorization: str | None) -> AuthContext:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(request.app.state, "admin_secret_verifier", None),
        )
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    return ctx


async def _admin(request: Request, authorization: str | None) -> AuthContext:
    ctx = await _auth(request, authorization)
    if not is_admin(ctx):
        raise HTTPException(status_code=403, detail="platform admin required")
    return ctx


def _conflict(exc: ServiceExecutionConflict) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail="execution_generation_fenced"
        if isinstance(exc, ServiceExecutionFenceError)
        else str(exc),
    )


@router.post("/admin/service-execution/catalog")
async def put_execution_catalog(
    body: CatalogBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    if body.topology.execution_class_id != body.execution_class.class_id:
        raise HTTPException(status_code=409, detail="topology execution class mismatch")
    async with request.app.state.session_factory() as session:
        try:
            await persist_execution_catalog(
                session,
                execution_class=body.execution_class,
                targets=body.topology.targets,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    return {
        "execution_class_id": body.execution_class.class_id,
        "logical_pool_id": body.topology.logical_pool_id,
        "target_ids": [target.target_id for target in body.topology.targets],
    }


@router.post("/admin/service-execution/targets/{target_id}/health")
async def update_execution_target_health(
    target_id: str,
    body: TargetHealthBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    async with request.app.state.session_factory() as session:
        try:
            target = await set_execution_target_health(
                session,
                target_id=target_id,
                desired_state=body.desired_state,
                observed_state=body.observed_state,
                health_status=body.health_status,
                observed_at=body.observed_at,
                error_code=body.error_code,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    assert target.health_observed_at is not None
    return {
        "target_id": target.id,
        "desired_state": target.desired_state,
        "observed_state": target.observed_state,
        "health_status": target.health_status,
        "health_observed_at": target.health_observed_at.isoformat(),
    }


@router.post("/admin/service-execution/reservations", status_code=201)
async def create_execution_reservation(
    body: ReservationBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    if getattr(request.app.state.settings, "global_pool", None) is not None:
        raise HTTPException(status_code=409, detail="global execution requires queued admission")
    try:
        image_admission_keyring = ImageAdmissionKeyring.from_json(
            request.app.state.settings.execution_image_admission_public_keys_json
        )
    except ImageAdmissionError as exc:
        raise HTTPException(
            status_code=503,
            detail="execution image admission trust is unavailable",
        ) from exc
    async with request.app.state.session_factory() as session:
        try:
            lease = await reserve_trial_execution(
                session,
                request_id=body.request_id,
                trial_id=body.trial_id,
                execution_class_id=body.execution_class_id,
                target_id=body.target_id,
                requirements=body.requirements,
                runtime_contract=body.runtime_contract,
                image_admission_keyring=image_admission_keyring,
                parent_lease_id=body.parent_lease_id,
                deadline_at=body.deadline_at,
            )
            await session.commit()
            await session.refresh(lease)
        except ExecutionProvisioningBlockedError as exc:
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail=exc.reason,
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    return execution_lease_projection(lease)


@router.post("/admin/service-execution/leases/{lease_id}/transitions")
async def create_execution_transition(
    lease_id: UUID,
    body: TransitionBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    async with request.app.state.session_factory() as session:
        try:
            command = await enqueue_execution_transition(
                session,
                lease_id=lease_id,
                expected_generation=body.expected_generation,
                desired_state=body.desired_state,
                payload=body.payload,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    return {
        "command_id": str(command.id),
        "lease_id": str(command.lease_id),
        "generation": command.generation,
        "command_type": command.command_type,
        "idempotency_key": command.idempotency_key,
        "state": command.state,
    }


@router.post("/admin/service-execution/commands/claim")
async def claim_commands(
    body: CommandClaimBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    async with request.app.state.session_factory() as session:
        try:
            commands = await claim_execution_commands(
                session,
                consumer_id=body.consumer_id,
                target_id=body.target_id,
                limit=body.limit,
                lease_seconds=body.lease_seconds,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    return {
        "items": [
            {
                "command_id": str(item.id),
                "lease_id": str(item.lease_id),
                "generation": item.generation,
                "sequence": item.sequence,
                "command_type": item.command_type,
                "idempotency_key": item.idempotency_key,
                "payload": item.payload,
                "delivery_count": item.delivery_count,
                "claim_expires_at": item.claim_expires_at.isoformat(),
            }
            for item in commands
        ]
    }


@router.post("/admin/service-execution/commands/{command_id}/ack")
async def acknowledge_command(
    command_id: UUID,
    body: CommandAckBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    async with request.app.state.session_factory() as session:
        try:
            command = await acknowledge_execution_command(
                session,
                command_id=command_id,
                consumer_id=body.consumer_id,
                acknowledgement=body.acknowledgement,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    assert command.acknowledged_at is not None
    return {
        "command_id": str(command.id),
        "state": command.state,
        "acknowledged_at": command.acknowledged_at.isoformat(),
        "acknowledgement_sha256": command.acknowledgement_sha256,
    }


@router.post("/admin/service-execution/leases/{lease_id}/events", status_code=201)
async def ingest_execution_event(
    lease_id: UUID,
    body: ExecutionEventBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _admin(request, authorization)
    async with request.app.state.session_factory() as session:
        try:
            event, duplicate = await record_execution_event(
                session,
                lease_id=lease_id,
                generation=body.generation,
                ordinal=body.ordinal,
                event_kind=body.event_kind,
                payload=body.payload,
                observed_at=body.observed_at,
                idempotency_key=body.idempotency_key,
            )
            await session.commit()
        except ServiceExecutionConflict as exc:
            await session.rollback()
            raise _conflict(exc) from exc
    return {
        "event_id": str(event.id),
        "lease_id": str(event.lease_id),
        "generation": event.generation,
        "ordinal": event.ordinal,
        "duplicate": duplicate,
    }


@router.get("/service-execution/trials/{trial_id}")
async def get_trial_execution(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _auth(request, authorization)
    async with request.app.state.session_factory() as session:
        trial = await session.get(Trial, trial_id)
        if trial is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if not is_admin(ctx) and (ctx.team_id is None or ctx.team_id != trial.team_id):
            raise HTTPException(status_code=404, detail="trial not found")
        lease = (
            await session.execute(
                select(ServiceExecutionLease)
                .where(
                    ServiceExecutionLease.trial_id == trial_id,
                    ServiceExecutionLease.execution_role == "attempt",
                )
                .order_by(ServiceExecutionLease.attempt.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if lease is None:
            return {"trial_id": str(trial_id), "execution": None}
        verifier = (
            await session.execute(
                select(ServiceExecutionLease).where(
                    ServiceExecutionLease.parent_lease_id == lease.id,
                    ServiceExecutionLease.execution_role == "verifier",
                )
                .order_by(ServiceExecutionLease.verifier_retry.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        commands = (
            (
                await session.execute(
                    select(ServiceExecutionCommand)
                    .where(ServiceExecutionCommand.lease_id == lease.id)
                    .order_by(
                        ServiceExecutionCommand.generation,
                        ServiceExecutionCommand.sequence,
                    )
                )
            )
            .scalars()
            .all()
        )
        events = (
            (
                await session.execute(
                    select(ServiceExecutionEvent)
                    .where(ServiceExecutionEvent.lease_id == lease.id)
                    .order_by(ServiceExecutionEvent.generation, ServiceExecutionEvent.ordinal)
                    .limit(500)
                )
            )
            .scalars()
            .all()
        )
        history = (
            (
                await session.execute(
                    select(ServiceExecutionLeaseHistory)
                    .where(ServiceExecutionLeaseHistory.lease_id == lease.id)
                    .order_by(ServiceExecutionLeaseHistory.transition_ordinal)
                    .limit(500)
                )
            )
            .scalars()
            .all()
        )
    return {
        "trial_id": str(trial_id),
        "execution": execution_lease_projection(lease),
        "verifier_execution": (
            execution_lease_projection(verifier) if verifier is not None else None
        ),
        "commands": [
            {
                "command_id": str(command.id),
                "generation": command.generation,
                "sequence": command.sequence,
                "command_type": command.command_type,
                "state": command.state,
                "delivery_count": command.delivery_count,
                "created_at": command.created_at.isoformat(),
                "acknowledged_at": (
                    command.acknowledged_at.isoformat() if command.acknowledged_at else None
                ),
            }
            for command in commands
        ],
        "events": [
            {
                "event_id": str(event.id),
                "generation": event.generation,
                "ordinal": event.ordinal,
                "event_kind": event.event_kind,
                "observed_at": event.observed_at.isoformat(),
                "accepted_at": event.accepted_at.isoformat(),
            }
            for event in events
        ],
        "history": [
            {
                "transition_ordinal": item.transition_ordinal,
                "generation": item.generation,
                "desired_state": item.desired_state,
                "observed_state": item.observed_state,
                "cleanup_state": item.cleanup_state,
                "changed_at": item.changed_at.isoformat(),
            }
            for item in history
        ],
    }
