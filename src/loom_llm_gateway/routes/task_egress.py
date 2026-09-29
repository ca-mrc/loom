"""Authenticated task-only egress; policy authority is the frozen execution lease."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, WebSocket
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.models.networking import WebAllowlist, WebDestination
from loom.pipeline.keys import canonical_digest
from loom_control_plane.service_execution_output import ServiceExecutionPeerV1
from loom_llm_gateway.drain import ensure_drain_state
from loom_llm_gateway.routes.service_execution import _authorize
from loom_llm_gateway.task_egress import (
    EgressDeniedError,
    TaskEgressRuntime,
    connect_destination,
    relay,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.websocket("/internal/service-execution/task-egress")
async def task_egress(websocket: WebSocket) -> None:
    runtime = getattr(websocket.app.state, "task_egress", None)
    drain = ensure_drain_state(websocket.app)
    lease_key = ""
    acquired = False
    writer = None
    outcome = "task_egress_denied"
    await websocket.accept()
    await drain.enter()
    try:
        if not isinstance(runtime, TaskEgressRuntime) or (await drain.snapshot())[1]:
            raise EgressDeniedError("task_egress_unavailable")
        if websocket.query_params:
            raise EgressDeniedError("task_egress_request_invalid")
        identity = ServiceExecutionPeerV1(
            lease_id=websocket.headers.get("x-loom-execution-lease-id", ""),
            generation=websocket.headers.get("x-loom-execution-generation", ""),
            execution_role=websocket.headers.get("x-loom-execution-role", ""),
        )
        lease = await _authorize(websocket, identity, purpose="token")
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        if (canonical_digest(plan.canonical_payload()) != lease.runtime_contract_sha256
            or websocket.headers.get("x-loom-runtime-contract-sha256") != lease.runtime_contract_sha256):
            raise EgressDeniedError("task_egress_contract_mismatch")
        if plan.task_egress is None:
            raise EgressDeniedError("task_egress_not_declared")
        lease_key = str(lease.id)
        runtime.acquire(lease_key)
        acquired = True
        async with asyncio.timeout(5):
            text = await websocket.receive_text()
        if len(text) > 1024:
            raise EgressDeniedError("task_egress_request_invalid")
        destination = WebDestination.model_validate(json.loads(text))
        if (isinstance(plan.task_egress, WebAllowlist)
                and destination not in plan.task_egress.destinations):
            raise EgressDeniedError("task_egress_destination_denied")
        phase_deadline = datetime.fromisoformat(websocket.headers.get("x-loom-phase-deadline", ""))
        if phase_deadline.tzinfo is None:
            raise EgressDeniedError("task_egress_request_invalid")
        deadline = min(lease.deadline_at, phase_deadline)
        if deadline <= datetime.now(UTC):
            raise EgressDeniedError("task_egress_deadline")
        async with asyncio.timeout(min(15, (deadline - datetime.now(UTC)).total_seconds())):
            reader, writer = await connect_destination(destination, runtime.config.protected_cidrs)
        await websocket.send_json({"status": "ready"})

        async def reauthorize() -> None:
            await _authorize(websocket, identity, purpose="token")
            if (await drain.snapshot())[1]:
                raise EgressDeniedError("task_egress_draining")

        await relay(websocket, reader, writer, deadline=deadline, reauthorize=reauthorize)
        outcome = "task_egress_completed"
    except EgressDeniedError as exc:
        outcome = str(exc)
    except HTTPException:
        outcome = "task_egress_identity_rejected"
    except (ValidationError, ValueError, KeyError, TypeError):
        outcome = "task_egress_request_invalid"
    except TimeoutError:
        outcome = "task_egress_timeout"
    except (WebSocketDisconnect, OSError):
        outcome = "task_egress_transport_closed"
    finally:
        if writer is not None:
            writer.close()
        if acquired:
            assert isinstance(runtime, TaskEgressRuntime)
            runtime.release(lease_key)
        await drain.leave()
        logger.info("task_egress outcome=%s lease=%s", outcome, lease_key)
        try:
            await websocket.close(code=1000 if outcome == "task_egress_completed" else 1008, reason=outcome)
        except (RuntimeError, OSError):
            pass
