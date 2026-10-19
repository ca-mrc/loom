"""Non-billing, request-correlated observations around actual upstream I/O.

An admission is permission to attempt transport, not proof the provider saw it.
Unknown observations remain queryable as ``admitted`` / ``pending`` after a
process failure. Nothing here creates a billable call or an execution event.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar
from uuid import UUID, uuid4

from fastapi import Request
from sqlalchemy import insert, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from loom.auth import AuthContext
from loom.db.schema import GatewayDispatchReceipt
from loom_llm_gateway.attempt_deadline import AttemptDeadlineReachedError, GatewayAttemptDeadline

logger = logging.getLogger(__name__)
T = TypeVar("T")
Purpose = Literal["model_call", "capability_probe", "adapter_call"]
ProviderOutcome = Literal[
    "response_received",
    "stream_completed",
    "deadline",
    "cancelled",
    "transport_error",
    "not_dispatched",
]
GatewayOutcome = Literal["completed", "error", "cancelled", "deadline"]
_STATE_KEY = "_loom_dispatch_audit"
_ACTIVE_RECEIPT: ContextVar[UUID | None] = ContextVar("gateway_dispatch_receipt", default=None)


def dispatch_request_headers(headers: dict[str, str]) -> dict[str, str]:
    """Attach only the opaque, committed HTTP dispatch ID, never execution claims.

    Evaluate inside the actual transport callable: each retry has its own ID.
    Incoming caller trace headers are not authority and are never used here.
    """
    receipt = _ACTIVE_RECEIPT.get()
    if receipt is None:
        return dict(headers)
    return {**headers, "X-Request-ID": str(receipt)}


# Audit cleanup never borrows the attempt's provider-execution budget. The
# bounded inline await creates no detached task and cannot swallow cancellation.
_AUDIT_TIMEOUT_SECONDS = 1.0
_DIALECTS = frozenset(
    {
        "openai",
        "anthropic",
        "google",
        "chat",
        "messages",
        "responses",
        "gemini",
        "facade_openai",
        "facade_anthropic",
        "facade_google",
        "chat_byo",
        "chat_litellm",
        "facade_openai_responses",
        "facade_openai_chat_compat",
        "anthropic_stream",
        "facade_anthropic_stream",
    }
)


@dataclass
class _RequestAudit:
    request_id: UUID = field(default_factory=uuid4)
    next_ordinal: int = 0
    receipt_ids: list[UUID] = field(default_factory=list)
    session_factory: async_sessionmaker[AsyncSession] | None = None


class DispatchAudit:
    """One route/provider binding; retries share the HTTP request identity."""

    def __init__(
        self,
        state: _RequestAudit,
        ctx: AuthContext,
        dialect: str,
        provider_connection_id: UUID | None,
        purpose: Purpose,
    ) -> None:
        if dialect not in _DIALECTS or purpose not in {
            "model_call",
            "capability_probe",
            "adapter_call",
        }:
            raise ValueError("invalid dispatch audit classification")
        if ctx.team_id is None or ctx.step_id is None or ctx.token_subject is None:
            raise ValueError("dispatch audit requires authenticated step subject")
        self.state = state
        self.ctx = ctx
        self.dialect = dialect
        # This is the route's resolved transport, not merely a JWT claim.
        # Explicit None denotes the platform endpoint even if a token carries
        # a connection used by a different supported dialect.
        self.provider_connection_id = provider_connection_id
        self.purpose = purpose

    async def _admit(self, attempt: int) -> UUID:
        if attempt < 1:
            raise ValueError("dispatch attempt must be positive")
        self.state.next_ordinal += 1
        receipt_id = uuid4()
        # No await occurs before reserving the ordinal, including concurrent
        # sends within the same incoming request.
        values = dict(
            id=receipt_id,
            request_id=self.state.request_id,
            dispatch_ordinal=self.state.next_ordinal,
            attempt=attempt,
            team_id=self.ctx.team_id,
            trial_id=self.ctx.trial_id,
            execution_attempt_id=self.ctx.execution_attempt_id,
            step_id=self.ctx.step_id,
            step_jwt_id=self.ctx.step_jwt_id,
            agent_attempt_id=getattr(self.ctx, "agent_attempt_id", None),
            provider_connection_id=self.provider_connection_id,
            dialect=self.dialect,
            purpose=self.purpose,
            attempt_deadline_wall_clock=self.ctx.attempt_deadline_wall_clock,
        )
        assert self.state.session_factory is not None
        try:
            async with asyncio.timeout(_AUDIT_TIMEOUT_SECONDS):
                async with self.state.session_factory() as session:
                    await session.execute(insert(GatewayDispatchReceipt).values(**values))
                    await session.commit()
        except asyncio.CancelledError:
            logger.warning(
                "gateway_dispatch_admission_cancelled_commit_unknown request_id=%s receipt_id=%s",
                self.state.request_id,
                receipt_id,
            )
            raise
        except Exception as exc:
            # Only fixed categories are safe: exception text, driver details,
            # SQL binds and tracebacks must never enter these logs.
            category = (
                "timeout" if isinstance(exc, TimeoutError)
                else "database" if isinstance(exc, SQLAlchemyError)
                else "other"
            )
            logger.error(
                "gateway_dispatch_admission_failed request_id=%s error_category=%s",
                self.state.request_id, category,
            )
            raise DispatchAuditUnavailableError("gateway dispatch audit unavailable") from None
        self.state.receipt_ids.append(receipt_id)
        return receipt_id

    async def _finish(
        self, receipt_id: UUID, outcome: ProviderOutcome, response: Any = None
    ) -> None:
        await _persist_observation(
            self.state,
            update(GatewayDispatchReceipt)
            .where(
                GatewayDispatchReceipt.id == receipt_id,
                GatewayDispatchReceipt.provider_outcome == "admitted",
            )
            .values(
                provider_outcome=outcome,
                provider_observed_at=datetime.now(UTC),
                provider_http_status=_http_status(response),
            ),
            observation="provider",
        )

    async def send(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        deadline: GatewayAttemptDeadline | None,
        attempt: int = 1,
    ) -> T:
        if deadline is not None:
            deadline.require_remaining()
        receipt_id = await self._admit(attempt)
        dispatched = False
        returned = False
        observed_response = None

        async def invoke() -> T:
            nonlocal dispatched, returned, observed_response
            token = _ACTIVE_RECEIPT.set(receipt_id)
            try:
                dispatched = True
                observed_response = await operation()
                returned = True
                return observed_response
            finally:
                _ACTIVE_RECEIPT.reset(token)

        try:
            # Recheck after the durable commit. The timeout encloses only
            # provider I/O, never the audit terminal transaction.
            response = await deadline.run(invoke) if deadline is not None else await invoke()
        except AttemptDeadlineReachedError:
            await self._finish(
                receipt_id,
                "response_received"
                if returned
                else ("deadline" if dispatched else "not_dispatched"),
                observed_response,
            )
            raise
        except asyncio.CancelledError:
            await self._finish(receipt_id, "cancelled" if dispatched else "not_dispatched")
            raise
        except Exception:
            await self._finish(receipt_id, "transport_error" if dispatched else "not_dispatched")
            raise
        await self._finish(receipt_id, "response_received", response)
        return response

    @asynccontextmanager
    async def stream(
        self,
        operation: Callable[[], AbstractAsyncContextManager[T]],
        *,
        deadline: GatewayAttemptDeadline | None,
        attempt: int = 1,
        completed: Callable[[], bool] | None = None,
    ) -> AsyncIterator[T]:
        if deadline is not None:
            deadline.require_remaining()
        receipt_id = await self._admit(attempt)
        dispatched = False
        response = None
        entered = False
        exited = False
        cm = None

        def provider_outcome(failure: ProviderOutcome) -> ProviderOutcome:
            # A route marks this only after real provider EOF, before any
            # usage write or downstream rendering. Those later failures must
            # never erase the completed upstream observation.
            return "stream_completed" if completed is not None and completed() else failure

        try:
            if deadline is not None:
                deadline.require_remaining()
            cm = operation()

            async def enter() -> T:
                nonlocal dispatched, entered, response
                dispatched = True
                response = await cm.__aenter__()
                entered = True
                return response

            yielded_response = await deadline.run(enter) if deadline is not None else await enter()
            try:
                yield yielded_response
            except BaseException as exc:
                # Preserve the original failure even if provider cleanup fails.
                try:
                    async with asyncio.timeout(_AUDIT_TIMEOUT_SECONDS):
                        exited = True
                        await cm.__aexit__(type(exc), exc, exc.__traceback__)
                except Exception:
                    pass
                raise
            else:
                async with asyncio.timeout(_AUDIT_TIMEOUT_SECONDS):
                    exited = True
                    await cm.__aexit__(None, None, None)
        except AttemptDeadlineReachedError:
            await self._finish(
                receipt_id,
                provider_outcome("deadline" if dispatched else "not_dispatched"),
                response,
            )
            raise
        except asyncio.CancelledError:
            await self._finish(
                receipt_id,
                provider_outcome(
                    "not_dispatched"
                    if not dispatched
                    else (
                        "deadline"
                        if deadline is not None and deadline.deadline_observed
                        else "cancelled"
                    )
                ),
                response,
            )
            raise
        except BaseException:
            await self._finish(
                receipt_id,
                provider_outcome(
                    "not_dispatched"
                    if not dispatched
                    else (
                        "deadline"
                        if deadline is not None and deadline.deadline_observed
                        else "transport_error"
                    )
                ),
                response,
            )
            raise
        finally:
            # deadline.run also checks the clock after __aenter__ returns. A
            # response obtained exactly at that boundary still needs closing.
            if entered and not exited and cm is not None:
                try:
                    async with asyncio.timeout(_AUDIT_TIMEOUT_SECONDS):
                        await cm.__aexit__(None, None, None)
                except Exception:
                    logger.warning(
                        "gateway_dispatch_stream_cleanup_failed request_id=%s",
                        self.state.request_id,
                    )
        await self._finish(
            receipt_id,
            "stream_completed" if completed is None else provider_outcome("response_received"),
            response,
        )


class DispatchAuditUnavailableError(RuntimeError):
    """Admission failed closed before provider I/O; contains no driver details."""


def audit_for_request(
    request: Request,
    ctx: AuthContext,
    dialect: str,
    provider_connection_id: UUID | None = None,
    purpose: Purpose = "model_call",
) -> DispatchAudit:
    state = request.scope.setdefault(_STATE_KEY, _RequestAudit())
    if not isinstance(state, _RequestAudit):
        raise RuntimeError("invalid dispatch audit request state")
    state.session_factory = request.app.state.session_factory
    return DispatchAudit(state, ctx, dialect, provider_connection_id, purpose)


def request_dispatch_audit(
    request: Request,
    dialect: str,
    provider_connection_id: UUID | None = None,
    purpose: Purpose = "model_call",
) -> DispatchAudit | None:
    ctx = request.scope.get("_loom_dispatch_auth")
    if not isinstance(ctx, AuthContext) or ctx.token_subject is None:
        return None
    # Pipeline uses its existing reservation/settlement authority, not a second
    # ordinary-Trial audit path. Never fabricate Trial identity for DB bearers.
    if ctx.execution_attempt_id is not None:
        return None
    return audit_for_request(request, ctx, dialect, provider_connection_id, purpose)


async def _persist_observation(
    state: _RequestAudit, statement: Any, *, observation: Literal["provider", "gateway"],
) -> None:
    assert state.session_factory is not None
    phase = "session"
    try:
        async with asyncio.timeout(_AUDIT_TIMEOUT_SECONDS):
            async with state.session_factory() as session:
                phase = "execute"
                await session.execute(statement)
                phase = "commit"
                await session.commit()
                phase = "session_exit"
    except asyncio.CancelledError:
        # A repeated cancellation may interrupt cleanup. Keep the durable
        # admitted/pending row instead of suppressing cancellation or spawning
        # an unbounded background finalizer.
        logger.warning("gateway_dispatch_observation_cancelled request_id=%s", state.request_id)
        raise
    except Exception as exc:
        # Match admission's fixed categories; driver details, SQL binds and
        # tracebacks can contain private data. Phase identifies the unfinished
        # await, not its physical cause (execute includes connection checkout).
        category = (
            "timeout" if isinstance(exc, TimeoutError)
            else "database" if isinstance(exc, SQLAlchemyError)
            else "other"
        )
        logger.error(
            "gateway_dispatch_observation_failed request_id=%s observation=%s "
            "phase=%s error_category=%s",
            state.request_id, observation, phase, category,
        )


def _http_status(response: Any) -> int | None:
    status = getattr(response, "status_code", None)
    return status if type(status) is int and 100 <= status <= 599 else None


class DispatchAuditMiddleware:
    """Observe final HTTP completion separately from upstream observations."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status: int | None = None
        completed = False
        outcome: GatewayOutcome = "error"

        async def observe(message: Message) -> None:
            nonlocal status, completed
            await send(message)
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                completed = True

        try:
            await self.app(scope, receive, observe)
            if completed:
                outcome = "completed"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            state = scope.get(_STATE_KEY)
            if isinstance(state, _RequestAudit) and state.receipt_ids:
                bound_deadline = scope.get("state", {}).get("loom_gateway_attempt_deadline")
                if getattr(bound_deadline, "deadline_observed", False):
                    outcome = "deadline"
                await _persist_observation(
                    state,
                    update(GatewayDispatchReceipt)
                    .where(
                        GatewayDispatchReceipt.request_id == state.request_id,
                        GatewayDispatchReceipt.gateway_outcome == "pending",
                    )
                    .values(
                        gateway_outcome=outcome,
                        gateway_http_status=status,
                        gateway_observed_at=datetime.now(UTC),
                    ),
                    observation="gateway",
                )
