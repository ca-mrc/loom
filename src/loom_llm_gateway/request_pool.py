"""Opt-in request routing; selection and durable dispatch admission are atomic.

The signed connection remains the logical route. Only this trusted hosted,
stateless Chat path may resolve it to another authorized physical connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import httpx
from fastapi import HTTPException, Request
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    GatewayDispatchReceipt,
    ProviderConnection,
    ProviderConnectionTimeoutHealth,
    ProviderConnectionTimeoutObservation,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolBinding,
    ProviderRequestPoolMember,
    Trial,
)
from loom.provider_model_preflight import preflight_failure_kind
from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline
from loom_llm_gateway.dispatch_audit import DispatchAudit, request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import (
    admitted_service_execution_models,
    authorize_trial_execution_dispatch,
)
from loom_llm_gateway.retry import RetryBudgetExceededError
from loom_llm_gateway.routes._facade_common import (
    decrypt_facade_api_key,
    resolve_facade_connection,
)

logger = logging.getLogger(__name__)


class PoolMemberSwitchedError(Exception):
    """The trial binding moved after an upstream timeout; do not send again."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


_OPENAI_TYPES = frozenset({"openai-compatible", "custom"})
_STATEFUL_FIELDS = frozenset(
    {
        "previous_response_id",
        "conversation",
        "conversation_id",
        "thread_id",
        "assistant_id",
        "file_id",
        "file_ids",
        "vector_store_ids",
        "attachments",
        "store",
        "background",
    }
)


def validate_pool_payload(payload: dict[str, Any]) -> None:
    """Complete client-owned history and ordinary client-side function tools only."""
    extra = payload.get("extra_body")
    if isinstance(extra, dict):
        if "extra_body" in extra:
            raise HTTPException(400, "request pool does not support nested extra_body")
        validate_pool_payload({**payload, **extra, "extra_body": None})
    if payload.get("stream") or _STATEFUL_FIELDS.intersection(payload):
        raise HTTPException(400, "request pool does not support streaming or server-side state")
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise HTTPException(400, "request pool requires complete messages")
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in {
            "system",
            "developer",
            "user",
            "assistant",
            "tool",
            "function",
        }:
            raise HTTPException(400, "request pool requires complete messages")
        content = message.get("content")
        if isinstance(content, list):
            for part in content:
                # Hosted files/tools can be scoped to the original account.
                if not isinstance(part, dict) or part.get("type") != "text":
                    raise HTTPException(400, "request pool does not support account-scoped content")
    tools = payload.get("tools", [])
    if not isinstance(tools, list) or any(
        not isinstance(tool, dict) or tool.get("type") != "function" for tool in tools
    ):
        raise HTTPException(400, "request pool supports client-side function tools only")


async def prepare_request_pool(
    request: Request,
    ctx: AuthContext,
    connection_id: UUID,
    payload: dict[str, Any],
) -> PoolDispatchAudit | None:
    async with request.app.state.session_factory() as session:
        enabled = await session.scalar(
            select(ProviderRequestPool.enabled).where(
                ProviderRequestPool.connection_id == connection_id,
            )
        )
        if not enabled:
            return None
        if ctx.service_execution_lease_id is None or ctx.trial_id is None:
            raise HTTPException(400, "request pool requires a hosted Terminus2 execution")
        config = await session.scalar(select(Trial.config).where(Trial.id == ctx.trial_id))
        if not isinstance(config, dict) or config.get("agent_name") != "terminus-2":
            raise HTTPException(400, "request pool requires a hosted Terminus2 execution")
        validate_pool_payload(payload)
        model_names = {name for _, name in admitted_service_execution_models(config)}
        if not model_names or payload["model"] not in model_names:
            raise HTTPException(403, "request pool model is not admitted for this execution")
    base = request_dispatch_audit(request, "facade_openai", connection_id)
    if base is None:
        raise HTTPException(403, "request pool requires authenticated dispatch auditing")
    return PoolDispatchAudit(base, model_names)


class PoolDispatchAudit(DispatchAudit):
    """A per-request selector; secrets and selected auth never live on shared clients."""

    def __init__(self, base: DispatchAudit, model_names: set[str]) -> None:
        super().__init__(
            base.state, base.ctx, base.dialect, base.provider_connection_id, base.purpose
        )
        assert base.provider_connection_id is not None
        self.pool_id = base.provider_connection_id
        self.model_names = model_names
        self.row: ProviderConnection | None = None
        self.api_key = ""
        self.attempt_timeout_sec = 20.0
        self.cooldown_sec = 30.0
        self.quota_group = ""
        self.last_attempt = 1
        self.failover_reason: str | None = None
        self.possible_duplicate_inference = False
        self._keys: list[str] = []

    async def _prepare_dispatch(self, session: AsyncSession, values: dict[str, Any]) -> None:
        pool = await session.scalar(
            select(ProviderRequestPool)
            .where(
                ProviderRequestPool.connection_id == self.pool_id,
            )
            .with_for_update()
        )
        if pool is None or not pool.enabled:
            raise HTTPException(409, "request pool changed; retry the request")
        await authorize_trial_execution_dispatch(session, self.ctx)
        assert self.ctx.team_id is not None
        await resolve_facade_connection(
            session,
            self.pool_id,
            self.ctx.team_id,
            supported_types=_OPENAI_TYPES,
            dialect_label="request pool",
            allow_request_pool=True,
        )
        required = set(pool.required_models)
        if not self.model_names.issubset(required):
            raise HTTPException(409, "request pool does not support every admitted execution model")
        members = list(
            (
                await session.scalars(
                    select(ProviderRequestPoolMember)
                    .where(
                        ProviderRequestPoolMember.pool_connection_id == self.pool_id,
                    )
                    .order_by(ProviderRequestPoolMember.ordinal)
                )
            ).all()
        )
        now = datetime.now(UTC)
        suppressed = await self._suppressed_members(session, members, now)
        assert self.ctx.trial_id is not None
        binding = await session.get(
            ProviderRequestPoolBinding, (self.pool_id, self.ctx.trial_id)
        )
        selected = await self._select_member(
            session, pool, members, binding, now, suppressed, required,
        )
        if selected is None:
            raise HTTPException(
                429,
                "provider_request_pool_unavailable",
                headers={"Retry-After": "1"},
            )
        member, row, key = selected
        self.row, self.api_key = row, key
        self.quota_group = member.quota_group
        self.last_attempt = values["attempt"]
        self._keys.append(key)
        self.attempt_timeout_sec = pool.attempt_timeout_sec
        self.cooldown_sec = pool.cooldown_sec
        values.update(
            provider_connection_id=row.id,
            pool_connection_id=self.pool_id,
            failover_reason=self.failover_reason,
        )

    async def _suppressed_members(
        self,
        session: AsyncSession,
        members: list[ProviderRequestPoolMember],
        now: datetime,
    ) -> set[UUID]:
        ids = [member.connection_id for member in members]
        if not ids:
            return set()
        rows = list(
            (
                await session.scalars(
                    select(ProviderConnectionTimeoutHealth).where(
                        ProviderConnectionTimeoutHealth.connection_id.in_(ids),
                    )
                )
            ).all()
        )
        return {
            row.connection_id
            for row in rows
            if row.suppressed_until is not None and row.suppressed_until > now
        }

    async def _eligible(
        self,
        session: AsyncSession,
        member: ProviderRequestPoolMember,
        now: datetime,
        suppressed: set[UUID],
        required: set[str],
    ) -> tuple[ProviderConnection, str] | None:
        if (
            not member.enabled
            or member.connection_id in suppressed
            or (member.cooldown_until is not None and member.cooldown_until > now)
        ):
            return None
        assert self.ctx.team_id is not None
        try:
            row = await resolve_facade_connection(
                session,
                member.connection_id,
                self.ctx.team_id,
                supported_types=_OPENAI_TYPES,
                dialect_label="request pool",
                allow_request_pool=member.connection_id == self.pool_id,
            )
        except HTTPException:
            return None
        if row.status != "valid" or (
            row.allowed_models and not required.issubset(row.allowed_models)
        ):
            return None
        cached = list(
            (
                await session.scalars(
                    select(ProviderModelCache).where(
                        ProviderModelCache.provider_connection_id == row.id,
                        ProviderModelCache.visible.is_(True),
                        ProviderModelCache.upstream_present.is_(True),
                        ProviderModelCache.model_id.in_(required),
                    )
                )
            ).all()
        )
        if {model.model_id for model in cached} != required or any(
            preflight_failure_kind(
                model.last_preflight_status,
                model.last_preflight_error_code,
                model.last_preflight_http_status,
            )
            == "rejected"
            for model in cached
        ):
            return None
        return row, await decrypt_facade_api_key(session, row)

    async def _select_member(
        self,
        session: AsyncSession,
        pool: ProviderRequestPool,
        members: list[ProviderRequestPoolMember],
        binding: ProviderRequestPoolBinding | None,
        now: datetime,
        suppressed: set[UUID],
        required: set[str],
    ) -> tuple[ProviderRequestPoolMember, ProviderConnection, str] | None:
        if not members:
            return None
        if binding is not None:
            current = next(
                (member for member in members if member.connection_id == binding.member_connection_id),
                None,
            )
            if current is not None:
                found = await self._eligible(session, current, now, suppressed, required)
                if found is not None:
                    return current, found[0], found[1]
        start = pool.next_member
        if binding is not None:
            for index, member in enumerate(members):
                if member.connection_id == binding.member_connection_id:
                    start = index + 1
                    break
        for offset in range(len(members)):
            index = (start + offset) % len(members)
            member = members[index]
            found = await self._eligible(session, member, now, suppressed, required)
            if found is None:
                continue
            assert self.ctx.trial_id is not None
            if binding is None:
                session.add(
                    ProviderRequestPoolBinding(
                        pool_connection_id=self.pool_id,
                        trial_id=self.ctx.trial_id,
                        member_connection_id=member.connection_id,
                        updated_at=now,
                    )
                )
                pool.next_member = (index + 1) % len(members)
            else:
                binding.member_connection_id = member.connection_id
                binding.updated_at = now
            return member, found[0], found[1]
        return None

    def response_retryable(self, response: httpx.Response) -> bool:
        # Same-member retries only. A real upstream timeout switches the binding
        # and returns 504 instead of starting another generation here.
        return response.status_code in {408, 429, 500, 502, 503}

    async def _switch_after_timeout(self, reason: str) -> None:
        assert self.row is not None and self.state.session_factory is not None
        assert self.ctx.trial_id is not None
        self.failover_reason = reason
        self.possible_duplicate_inference = True
        if not self.state.receipt_ids:
            return
        receipt_id = self.state.receipt_ids[-1]
        try:
            async with asyncio.timeout(1.0), self.state.session_factory() as session:
                pool = await session.scalar(
                    select(ProviderRequestPool)
                    .where(ProviderRequestPool.connection_id == self.pool_id)
                    .with_for_update()
                )
                if pool is None or not pool.enabled:
                    return
                await authorize_trial_execution_dispatch(session, self.ctx)
                now = datetime.now(UTC)
                await session.execute(
                    pg_insert(ProviderConnectionTimeoutObservation)
                    .values(
                        receipt_id=receipt_id,
                        connection_id=self.row.id,
                        observed_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["receipt_id"])
                )
                health = await session.get(ProviderConnectionTimeoutHealth, self.row.id)
                if health is None:
                    health = ProviderConnectionTimeoutHealth(connection_id=self.row.id)
                    session.add(health)
                    await session.flush()
                window_start = now - timedelta(seconds=pool.timeout_window_sec)
                count_stmt = (
                    select(func.count())
                    .select_from(ProviderConnectionTimeoutObservation)
                    .where(
                        ProviderConnectionTimeoutObservation.connection_id == self.row.id,
                        ProviderConnectionTimeoutObservation.observed_at >= window_start,
                    )
                )
                if health.suppression_started_at is not None:
                    count_stmt = count_stmt.where(
                        ProviderConnectionTimeoutObservation.observed_at
                        > health.suppression_started_at
                    )
                observed = int(await session.scalar(count_stmt) or 0)
                suppressed_now = (
                    health.suppressed_until is not None and health.suppressed_until > now
                )
                if observed >= pool.timeout_burst_count and not suppressed_now:
                    health.suppressed_until = now + timedelta(seconds=pool.cooldown_sec)
                    health.suppression_started_at = now
                await session.execute(
                    update(GatewayDispatchReceipt)
                    .where(GatewayDispatchReceipt.id == receipt_id)
                    .values(failover_reason=reason)
                )
                await self._rebind_after_timeout(session, pool, now)
                await session.commit()
        except HTTPException:
            raise
        except Exception:
            logger.warning(
                "request_pool_timeout_switch_unavailable pool_connection_id=%s", self.pool_id,
            )

    async def _rebind_after_timeout(
        self, session: AsyncSession, pool: ProviderRequestPool, now: datetime,
    ) -> None:
        assert self.row is not None and self.ctx.trial_id is not None
        members = list(
            (
                await session.scalars(
                    select(ProviderRequestPoolMember)
                    .where(ProviderRequestPoolMember.pool_connection_id == self.pool_id)
                    .order_by(ProviderRequestPoolMember.ordinal)
                )
            ).all()
        )
        suppressed = await self._suppressed_members(session, members, now)
        suppressed.add(self.row.id)
        required = set(pool.required_models)
        start = 0
        timed_out_group = self.quota_group
        for index, member in enumerate(members):
            if member.connection_id == self.row.id:
                start = index + 1
                timed_out_group = member.quota_group
                break
        spare: ProviderRequestPoolMember | None = None
        for offset in range(len(members)):
            member = members[(start + offset) % len(members)]
            if member.connection_id == self.row.id or member.quota_group == timed_out_group:
                continue
            if await self._eligible(session, member, now, suppressed, required) is None:
                continue
            spare = member
            break
        if spare is None:
            return
        binding = await session.get(
            ProviderRequestPoolBinding, (self.pool_id, self.ctx.trial_id)
        )
        if binding is None:
            session.add(
                ProviderRequestPoolBinding(
                    pool_connection_id=self.pool_id,
                    trial_id=self.ctx.trial_id,
                    member_connection_id=spare.connection_id,
                    updated_at=now,
                )
            )
            return
        binding.member_connection_id = spare.connection_id
        binding.updated_at = now

    async def _note_member_success(self) -> None:
        if self.row is None or self.state.session_factory is None:
            return
        try:
            async with asyncio.timeout(1.0), self.state.session_factory() as session:
                health = await session.get(ProviderConnectionTimeoutHealth, self.row.id)
                now = datetime.now(UTC)
                if (
                    health is not None
                    and health.suppressed_until is not None
                    and health.suppressed_until > now
                ):
                    health.suppressed_until = None
                    await session.commit()
        except Exception:
            logger.warning(
                "request_pool_timeout_probe_unavailable pool_connection_id=%s", self.pool_id,
            )

    async def send(
        self,
        operation: Any,
        *,
        deadline: GatewayAttemptDeadline | None,
        attempt: int = 1,
    ) -> Any:
        try:
            response = await super().send(operation, deadline=deadline, attempt=attempt)
        except RetryBudgetExceededError:
            # Gateway policy expiry is not a member health observation and
            # must not suppress a provider or rotate the sticky binding.
            raise
        except httpx.TimeoutException:
            await self._switch_after_timeout("upstream_timeout")
            raise PoolMemberSwitchedError("upstream_timeout") from None
        if response.status_code == 504:
            await self._switch_after_timeout("upstream_504")
            raise PoolMemberSwitchedError("upstream_504") from None
        if response.status_code < 400:
            await self._note_member_success()
        return response

    def redact(self, text: str) -> str:
        for key in self._keys:
            text = text.replace(key, "[REDACTED]")
        return text

    def attribution(self) -> dict[str, Any]:
        return {
            "pool_connection_id": str(self.pool_id),
            "provider_connection_id": str(self.row.id) if self.row else None,
            "failover_reason": self.failover_reason,
            "possible_duplicate_inference": self.possible_duplicate_inference,
        }

    def redact_log(self, log: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.redact(json.dumps(log))))
